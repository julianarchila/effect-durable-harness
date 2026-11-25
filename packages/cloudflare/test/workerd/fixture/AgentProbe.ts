import * as AgentTool from "@effect-durable-harness/core/AgentTool";
import * as Extension from "@effect-durable-harness/core/Extension";
import { AgentStorage } from "@effect-durable-harness/core/AgentStorage";
import * as Doc from "@effect-durable-harness/core/Doc";
import * as Harness from "@effect-durable-harness/core/Harness";
import { LiveDoc } from "@effect-durable-harness/core/Live";
import { ROOT_CONVERSATION_ID, TaskId } from "@effect-durable-harness/core/Ids";
import type { Message } from "@effect-durable-harness/core/Message";
import type { TaskRecord } from "@effect-durable-harness/core/Records";
import * as Task from "@effect-durable-harness/core/Task";
import { Tx } from "@effect-durable-harness/core/Tx";
import * as FauxModel from "@effect-durable-harness/core/testing/FauxModel";
import * as Cloudflare from "@alchemy.run/cloudflare";
import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Predicate from "effect/Predicate";
import * as Random from "effect/Random";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { commitWrites } from "@effect-durable-harness/core/testing/CommitWrites";
import * as ToolSearch from "@effect-durable-harness/core/ToolSearch";
import * as Host from "../../../src/Host.ts";
import * as RecordedSpans from "./RecordedSpans.ts";

/** A latest, initial-fork conversation document, which a deferred commit may hold. */
const NoteDoc = Doc.make({
  kind: "probe.note",
  version: 1,
  scope: "conversation",
  history: "latest",
  fork: "initial",
  schema: Schema.Struct({ text: Schema.String }),
  initial: () => ({ text: "" }),
  checkpointWhen: () => false,
});

/** A checkpoint's phase, when it has one: generations and tool calls checkpoint `{ phase, ... }`. */
const decodePhase = Schema.decodeUnknownOption(Schema.Struct({ phase: Schema.String }));

/** How far ahead the alarm stays while there is active work: short, so the tests recover in about a second. */
export const KEEP_ALIVE_MS = 1_000;

/** The fake model's reference, configured on the root conversation. */
export const FAUX = { provider: "faux", modelId: "faux-1" } as const;

/**
 * Which object instance is running: a fresh one after every restart. Tasks put it in their results, so a test
 * can tell whether the instance that finished the work started before the test's next call (woken by the alarm)
 * or because of it.
 */
export class Instance extends Context.Service<
  Instance,
  { readonly id: string; readonly startedAt: number }
>()("@effect-durable-harness/cloudflare/test/workerd/fixture/AgentProbe/Instance") {}

/** What a probe task returns. */
export const ProbeResult = Schema.Struct({
  instance: Schema.String,
  instanceStartedAt: Schema.Number,
  at: Schema.Number,
});

/** What `probe.hold` returns: where its wait began and ended, its restarts then, and its child's outcome. */
export const HoldResult = Schema.Struct({
  ...ProbeResult.fields,
  waitedIn: Schema.String,
  waitedFrom: Schema.Number,
  restarts: Schema.Number,
  child: Schema.String,
});

/** Sleeps until `at` with `sleepUntil`: no invocation, so only the deadline holds the alarm. */
const Sleep = Task.make({
  name: "probe.sleep",
  version: 1,
  input: Schema.Struct({ at: Schema.Number }),
  checkpoint: Schema.Union([
    Schema.Struct({ phase: Schema.Literal("sleep") }),
    Schema.Struct({ phase: Schema.Literal("woke") }),
  ]),
  result: ProbeResult,
  initial: () => ({ phase: "sleep" }),
  phases: {
    sleep: (task, runtime) => runtime.sleepUntil(task.input.at, { phase: "woke" }),
    woke: (_task, runtime) =>
      Effect.gen(function* () {
        const result = yield* resultNow;
        yield* runtime.commit(() =>
          Effect.succeed({
            status: "terminal",
            outcome: { status: "completed", result },
          } as const),
        );
      }),
  },
  abort: (_task, runtime) =>
    runtime.commit(() =>
      Effect.succeed({ status: "terminal", outcome: { status: "aborted" } } as const),
    ),
});

/**
 * Waits in memory, inside its phase, for a `probe.sleep` child it creates with `abandonOnRestart`: active work with no
 * I/O and no timer pending, which only the host's hold keeps in memory. A restart in between is visible twice: the
 * next open abandons the child (`aborted`), and the rerun phase starts in another instance with a restart counted.
 */
const Hold = Task.make({
  name: "probe.hold",
  version: 1,
  input: Schema.Struct({ ms: Schema.Number }),
  checkpoint: Schema.Union([
    Schema.Struct({ phase: Schema.Literal("spawn") }),
    Schema.Struct({ phase: Schema.Literal("wait"), child: Schema.Number }),
  ]),
  result: HoldResult,
  initial: () => ({ phase: "spawn" }),
  phases: {
    spawn: (task, runtime) =>
      Effect.gen(function* () {
        const at = (yield* Clock.currentTimeMillis) + task.input.ms;
        yield* runtime.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            const child = yield* tx.createTask(
              Sleep,
              { at },
              { ownership: { kind: "task", taskId: task.id }, abandonOnRestart: true },
            );
            return Task.next({ phase: "wait" as const, child });
          }),
        );
      }),
    wait: (task, runtime) =>
      Effect.gen(function* () {
        const waitedIn = (yield* Instance).id;
        const waitedFrom = yield* Clock.currentTimeMillis;
        // SAFETY: the checkpoint stores the ID `createTask` returned.
        const child = yield* runtime.awaitTask(TaskId.make(task.state.checkpoint.child));
        const result = {
          ...(yield* resultNow),
          waitedIn,
          waitedFrom,
          restarts: task.restarts ?? 0,
          child: child.state.outcome.status,
        };
        yield* runtime.commit(() =>
          Effect.succeed({ status: "terminal", outcome: { status: "completed", result } } as const),
        );
      }),
  },
  abort: (_task, runtime) =>
    runtime.commit(() =>
      Effect.succeed({ status: "terminal", outcome: { status: "aborted" } } as const),
    ),
});

/**
 * Opens a TCP socket from its phase, which runs on the object's fibers after the call that created the task has
 * returned: the case of a database driver called from a tool or a task. It writes `ping`, reads the echo
 * and closes the socket, all inside the phase.
 */
const Socket = Task.make({
  name: "probe.socket",
  version: 1,
  input: Schema.Struct({ hostname: Schema.String, port: Schema.Number }),
  checkpoint: Schema.Struct({ phase: Schema.Literal("connect") }),
  result: Schema.String,
  initial: () => ({ phase: "connect" }),
  phases: {
    connect: (task, runtime) =>
      Effect.gen(function* () {
        const echo = yield* Effect.tryPromise({
          try: async () => {
            // Imported where it runs: the test process loads this module on Node, which has no such module.
            const { connect } = await import("cloudflare:sockets");
            const socket = connect({ hostname: task.input.hostname, port: task.input.port });
            const writer = socket.writable.getWriter();
            // A line: some echo servers answer only whole lines.
            await writer.write(new TextEncoder().encode("ping\n"));
            const reader = socket.readable.getReader();
            const { value } = await reader.read();
            await socket.close();
            return new TextDecoder().decode(value).trim();
          },
          catch: (cause) => new SocketFailed({ message: String(cause) }),
        });
        yield* runtime.commit(() =>
          Effect.succeed({
            status: "terminal",
            outcome: { status: "completed", result: echo },
          } as const),
        );
      }),
  },
  abort: (_task, runtime) =>
    runtime.commit(() =>
      Effect.succeed({ status: "terminal", outcome: { status: "aborted" } } as const),
    ),
});

class SocketFailed extends Schema.TaggedError<SocketFailed>()("SocketFailed", {
  message: Schema.String,
}) {}

/**
 * Works in `chunks` chunks of `chunkMs` of plain `Effect.sleep` (active work, not `sleepUntil`), committing its
 * progress after each one, so a restart resumes from the last chunk.
 */
const Busy = Task.make({
  name: "probe.busy",
  version: 1,
  input: Schema.Struct({ chunks: Schema.Number, chunkMs: Schema.Number }),
  checkpoint: Schema.Struct({ phase: Schema.Literal("run"), done: Schema.Number }),
  result: ProbeResult,
  initial: () => ({ phase: "run", done: 0 }),
});

/** `Busy`'s handlers, as a Layer. */
const BusyLive = Busy.toLayer({
  phases: {
    run: (task, runtime) =>
      Effect.gen(function* () {
        let done = task.state.checkpoint.done;
        while (done < task.input.chunks) {
          yield* Effect.sleep(Duration.millis(task.input.chunkMs));
          done++;
          const next = done;
          yield* runtime.commit(() =>
            Effect.succeed({
              status: "running",
              checkpoint: { phase: "run", done: next },
            } as const),
          );
        }
        const result = yield* resultNow;
        yield* runtime.commit(() =>
          Effect.succeed({
            status: "terminal",
            outcome: { status: "completed", result },
          } as const),
        );
      }),
  },
  abort: (_task, runtime) =>
    runtime.commit(() =>
      Effect.succeed({ status: "terminal", outcome: { status: "aborted" } } as const),
    ),
});

const resultNow = Effect.gen(function* () {
  const instance = yield* Instance;
  return {
    instance: instance.id,
    instanceStartedAt: instance.startedAt,
    at: yield* Clock.currentTimeMillis,
  };
});

/** A tool that takes `ms` and is safe to run again after a restart. */
const Slow = AgentTool.make("slow", {
  description: "Waits for a while.",
  parameters: Schema.Struct({ ms: Schema.Number }),
  replay: "safe",
});

/** `Slow`'s implementation, as a Layer. */
const SlowLive = Slow.toLayer(({ ms }) =>
  Effect.sleep(Duration.millis(ms)).pipe(
    Effect.as({ output: [{ type: "text" as const, text: `waited ${ms} ms` }] }),
  ),
);

/** Parks until `ms` from its first run (`AgentTool.sleep`): no invocation while it sleeps. Tools call it, the model not. */
const Nap = AgentTool.make("nap", {
  description: "Sleeps without holding the object.",
  parameters: Schema.Struct({ ms: Schema.Number }),
  callers: ["tools"],
  replay: "safe",
  execute: ({ ms }, api) =>
    api.wake === undefined
      ? Effect.map(Clock.currentTimeMillis, (now) => AgentTool.sleep({ until: now + ms }))
      : Effect.succeed(AgentTool.text("woke")),
});

/** What `nest` answers: where its wait on `nap` began and ended, and what `nap` answered. */
export const NestResult = Schema.Struct({
  ...ProbeResult.fields,
  waitedIn: Schema.String,
  waitedFrom: Schema.Number,
  nested: Schema.String,
});

/**
 * Calls `nap` and waits for it in memory (`executeTool` with `wait: true`) while `nap` is parked: the caller's
 * invocation stays occupied with no I/O and no timer of its own. Not replay-safe, so a restart ends it `interrupted`
 * instead of answering.
 */
const Nest = AgentTool.make("nest", {
  description: "Waits for a nested nap.",
  parameters: Schema.Struct({ ms: Schema.Number }),
  execute: ({ ms }, api) =>
    Effect.gen(function* () {
      const waitedIn = (yield* Instance).id;
      const waitedFrom = yield* Clock.currentTimeMillis;
      const nested = yield* api.executeTool("nap", { ms }, { wait: true }).pipe(Effect.orDie);
      const result = {
        ...(yield* resultNow),
        waitedIn,
        waitedFrom,
        nested: Predicate.isString(nested.structuredOutput) ? nested.structuredOutput : "error",
      };
      return AgentTool.text(encodeNestResult(result));
    }),
});

const encodeNestResult = Schema.encodeSync(Schema.fromJsonString(NestResult));

export const ProbeExtension = Extension.make({
  name: "probe",
  tasks: [Sleep, Busy, Socket, Hold],
  tools: [Slow, Nap, Nest],
});

/**
 * A deferred tool described in Chinese, written without spaces: `tool_search` finds it for a query such as `问题`
 * only when `Intl.Segmenter` splits the description into words inside the object.
 */
const IssueSearch = AgentTool.make("repo_issue_search", {
  description: "搜索仓库问题",
  parameters: Schema.Struct({}),
  deferred: true,
  execute: () => Effect.succeed(AgentTool.text("searched")),
});

/** `tool_search` and a deferred tool for it to find. */
const SearchExtension = Extension.make({
  name: "probe-search",
  tools: [IssueSearch, ToolSearch.tool],
});

const textOf = (message: Message | undefined): string => {
  if (message === undefined || message.role === "system") return "";
  if (Predicate.isString(message.content)) return message.content;
  return message.content.map((part) => (part.type === "text" ? part.text : "")).join("");
};

/**
 * The fake model answers from the request alone, so it behaves the same after a restart: `tool:<ms>` calls `slow`,
 * `nest:<ms>` calls `nest`,
 * `search:<query>` calls `tool_search`, a tool result is answered with `done`, and anything else is echoed with padding that, at 40 tokens per second,
 * streams for about three seconds.
 */
export const fakeModel = Layer.unwrap(
  Effect.sync(() => {
    const faux = FauxModel.make({ tokensPerSecond: 40 });
    const respond: FauxModel.FauxResponseFactory = (request) =>
      Effect.sync(() => {
        faux.appendResponses([respond]);
        const last = request.messages.at(-1);
        if (last?.role === "toolResult") return FauxModel.fauxAssistantMessage("done");
        const text = textOf(last);
        const search = /^search:(.+)$/.exec(text);
        if (search !== null) {
          return FauxModel.fauxAssistantMessage(
            [FauxModel.fauxToolCall(ToolSearch.TOOL_NAME, { query: search[1] ?? "" })],
            { stopReason: "toolUse" },
          );
        }
        const nest = /^nest:(\d+)$/.exec(text);
        if (nest !== null) {
          return FauxModel.fauxAssistantMessage(
            [FauxModel.fauxToolCall("nest", { ms: Number(nest[1]) })],
            { stopReason: "toolUse" },
          );
        }
        const tool = /^tool:(\d+)$/.exec(text);
        if (tool !== null) {
          return FauxModel.fauxAssistantMessage(
            [FauxModel.fauxToolCall("slow", { ms: Number(tool[1]) })],
            { stopReason: "toolUse" },
          );
        }
        return FauxModel.fauxAssistantMessage(`echo: ${text}. ${"and so on ".repeat(30)}`);
      });
    faux.setResponses([respond]);
    return faux.layer;
  }),
);

const instance = Layer.effect(
  Instance,
  Effect.gen(function* () {
    return {
      id: String(yield* Random.nextIntBetween(0, 1_000_000_000)),
      startedAt: yield* Clock.currentTimeMillis,
    };
  }),
);

/** A task as the tests see it. */
export interface TaskView {
  readonly status: string;
  readonly result: typeof ProbeResult.Type | null;
  readonly endedAt: number | null;
}

/**
 * The per-instance part of the agent's services: the fake model, and `Instance`, which the probe's handlers read and
 * which must change with every restart. Handlers that read it are built with the agent, per instance; the others are
 * provided to the object's Layer and built once per isolate.
 */
const perInstance = Layer.mergeAll(fakeModel, instance);

/** The methods every probe object has, from the instance and the services it holds. */
const probeMethodsLive = (host: Host.Instance<Schema.Json, Instance>) =>
  Effect.gen(function* () {
    return yield* probeMethods(
      host.harness,
      yield* AgentStorage,
      yield* Instance,
      yield* Cloudflare.DurableObjectState,
    );
  }).pipe(Effect.provideContext(host.context));

/** Which isolate built the object's Layer: provided from outside the object, so built once per isolate. */
export class Isolate extends Context.Service<Isolate, { readonly id: string }>()(
  "@effect-durable-harness/cloudflare/test/workerd/fixture/AgentProbe/Isolate",
) {}

const IsolateLive = Layer.effect(
  Isolate,
  Effect.map(Random.nextIntBetween(0, 1_000_000_000), (id) => ({ id: String(id) })),
);

/** A KV namespace a task handler binds in its Layer, at init: Alchemy sees the binding when it plans the Worker. */
const ProbeKV = Cloudflare.KV.Namespace("ProbeKV");

/** Writes `value` under `key` in `ProbeKV` and reads it back. */
const KvTask = Task.make({
  name: "probe.kv",
  version: 1,
  input: Schema.Struct({ key: Schema.String, value: Schema.String }),
  checkpoint: Schema.Struct({ phase: Schema.Literal("write") }),
  result: Schema.String,
  initial: () => ({ phase: "write" }),
});

/** `KvTask`'s handlers: the Layer requests the binding, so it is built where Alchemy can see it. */
const KvTaskLive = KvTask.toLayer(
  Effect.gen(function* () {
    const kv = yield* Cloudflare.KV.ReadWriteNamespace(ProbeKV);
    return {
      phases: {
        write: (task, runtime) =>
          Effect.gen(function* () {
            yield* Host.withRuntimeContext(kv.put(task.input.key, task.input.value));
            const read = yield* Host.withRuntimeContext(kv.get(task.input.key));
            yield* runtime.commit(() =>
              Effect.succeed({
                status: "terminal",
                outcome: { status: "completed", result: read ?? "" },
              } as const),
            );
          }),
      },
      abort: (_task, runtime) =>
        runtime.commit(() =>
          Effect.succeed({ status: "terminal", outcome: { status: "aborted" } } as const),
        ),
    };
  }),
);

const KvExtension = Extension.make({ name: "probe-kv", tasks: [KvTask] });

/**
 * `AgentProbe`'s methods: the common ones, the isolate's ID and the KV task. `Isolate` is built once per isolate, so
 * the object's outer constructor yields it and passes it in.
 */
const agentProbeMethods = (
  host: Host.Instance<Schema.Json, Instance>,
  isolate: Isolate["Service"],
) =>
  Effect.gen(function* () {
    const methods = yield* probeMethodsLive(host);
    const harness = host.harness;
    return {
      ...methods,
      /** The isolate that built the object's Layer. */
      isolate: () => Effect.succeed(isolate.id),
      /** The spans the events' tracers ended in this isolate (`RecordedSpans`). */
      recordedSpans: () => Effect.sync(RecordedSpans.recordedSpans),
      /** Starts a `probe.kv` task; `outcome` reads its outcome once terminal. */
      startKv: (key: string, value: string) =>
        harness
          .commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              return yield* tx.createTask(
                KvTask,
                { key, value },
                { conversationId: ROOT_CONVERSATION_ID, ownership: { kind: "conversation" } },
              );
            }),
          )
          .pipe(Effect.map(Number), Effect.orDie),
    };
  });

/** The methods the test Worker routes to. */
const probeMethods = (
  harness: Harness.Harness["Service"],
  agentStorage: AgentStorage["Service"],
  instance: Instance["Service"],
  state: Cloudflare.DurableObjectState["Service"],
) =>
  Effect.gen(function* () {
    const storage = state.raw.storage;
    const root = yield* harness.ensureRoot({ agent: { model: FAUX } }).pipe(Effect.orDie);

    const create = (kind: "probe.sleep" | "probe.busy", input: Schema.Json) =>
      root
        .commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            return kind === "probe.sleep"
              ? yield* tx.createTask(
                  Sleep,
                  Schema.decodeUnknownSync(Sleep.definition.input)(input),
                  {
                    ownership: { kind: "conversation" },
                  },
                )
              : yield* tx.createTask(Busy, Schema.decodeUnknownSync(Busy.definition.input)(input), {
                  ownership: { kind: "conversation" },
                });
          }),
        )
        .pipe(Effect.orDie);

    return {
      /** The instance, its alarm and the kernel's wake. */
      state: () =>
        Effect.gen(function* () {
          const wake = yield* harness.wake.pipe(Effect.orDie);
          const inspection = yield* harness.inspect.pipe(Effect.orDie);
          return {
            instance: instance.id,
            instanceStartedAt: instance.startedAt,
            now: yield* Clock.currentTimeMillis,
            alarm: yield* Effect.promise(() => storage.getAlarm()),
            nextWakeAt: Option.getOrNull(wake.nextWakeAt),
            activeWork: wake.activeWork,
            scheduling: inspection.scheduling,
            running: inspection.tasks
              .filter((task) => task.state.kind === "running")
              .map((task) => task.record.kind),
            /** Each running task's checkpoint phase, as `kind:phase`, for a test that must crash a given phase. */
            phases: inspection.tasks
              .filter((task) => task.state.kind === "running")
              .map((task) => {
                const checkpoint =
                  task.record.state.status === "running" ? task.record.state.checkpoint : undefined;
                const phase = Option.match(decodePhase(checkpoint), {
                  onNone: () => "",
                  onSome: (decoded) => decoded.phase,
                });
                return `${task.record.kind}:${phase}`;
              }),
          };
        }),
      /** Starts a `probe.socket` task; `outcome` reads its outcome once terminal. */
      startSocket: (hostname: string, port: number) =>
        root
          .commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              return yield* tx.createTask(
                Socket,
                { hostname, port },
                { ownership: { kind: "conversation" } },
              );
            }),
          )
          .pipe(Effect.map(Number), Effect.orDie),
      outcome: (id: number) =>
        harness.getTask(TaskId.make(id)).pipe(
          Effect.orDie,
          Effect.map((record) => {
            if (Option.isNone(record)) return null;
            const state = record.value.state;
            return state.status === "terminal" ? state.outcome : null;
          }),
        ),
      /** Starts a `probe.hold` whose child sleeps `ms`; `holdResult` reads its result once completed. */
      startHold: (ms: number) =>
        root
          .commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              return yield* tx.createTask(Hold, { ms }, { ownership: { kind: "conversation" } });
            }),
          )
          .pipe(Effect.map(Number), Effect.orDie),
      holdResult: (id: number) =>
        harness.getTask(TaskId.make(id)).pipe(
          Effect.orDie,
          Effect.map((record) => {
            if (Option.isNone(record)) return null;
            const state = record.value.state;
            return state.status === "terminal" && state.outcome.status === "completed"
              ? Schema.decodeUnknownSync(HoldResult)(state.outcome.result)
              : {
                  status: state.status,
                  outcome: state.status === "terminal" ? state.outcome : null,
                };
          }),
        ),
      startTask: (kind: "probe.sleep" | "probe.busy", input: Schema.Json) =>
        Effect.map(create(kind, input), Number),
      task: (id: number) =>
        harness.getTask(TaskId.make(id)).pipe(
          Effect.orDie,
          Effect.map((record): TaskView | null => {
            if (Option.isNone(record)) return null;
            const state = record.value.state;
            const result =
              state.status === "terminal" && state.outcome.status === "completed"
                ? Schema.decodeUnknownSync(ProbeResult)(state.outcome.result)
                : null;
            return { status: state.status, result, endedAt: record.value.endedAt ?? null };
          }),
        ),
      submit: (text: string, requestId: string) =>
        root.submit({ type: "input", content: text, requestId }).pipe(
          Effect.map((submission) => Number(submission.id)),
          Effect.orDie,
        ),
      /** Submits an image of `bytes` bytes: over Durable Object SQLite's value limit, the commit fails. */
      submitImage: (bytes: number, requestId: string) =>
        root
          .submit({
            type: "input",
            requestId,
            content: [
              { type: "text", text: "look" },
              { type: "image", mimeType: "image/png", data: "A".repeat(bytes) },
            ],
          })
          .pipe(
            Effect.map((submission) => Number(submission.id)),
            Effect.orDie,
          ),
      /** The transcript's model messages, as role and text. */
      transcript: () =>
        Stream.runCollect(root.entries({ order: "ascending" })).pipe(
          Effect.orDie,
          Effect.map((entries) =>
            Array.from(entries).flatMap((entry) =>
              (entry.model ?? []).flatMap((message) =>
                message.role === "system" ? [] : [{ role: message.role, text: textOf(message) }],
              ),
            ),
          ),
        ),
      /** Resets the object, as an eviction or a crash does; `sync` first makes the last commit durable. */
      crash: (sync: boolean) =>
        Effect.gen(function* () {
          if (sync) yield* Effect.promise(() => storage.sync());
          yield* state.abort("probe: crash");
        }),
      /**
       * Commits a runnable task straight to storage, bypassing the Session, so nothing in memory knows about it:
       * only the alarm the commit armed can run it. Answers the alarm right after the commit.
       */
      orphanTask: (input: { readonly chunks: number; readonly chunkMs: number }) =>
        Effect.gen(function* () {
          const id = TaskId.make(yield* agentStorage.mintId.pipe(Effect.orDie));
          const record: TaskRecord = {
            id,
            conversationId: ROOT_CONVERSATION_ID,
            kind: Busy.definition.name,
            version: Busy.definition.version,
            input,
            background: false,
            abortRequested: false,
            state: { status: "pending", checkpoint: { phase: "run", done: 0 } },
          };
          yield* commitWrites(agentStorage, [{ type: "task", value: record }]).pipe(Effect.orDie);
          return { id: Number(id), alarm: yield* Effect.promise(() => storage.getAlarm()) };
        }),
      /**
       * Sets the root's note in a deferred commit held up to `withinMs`; the note is created first, by an
       * immediate commit, since a creation is never held.
       */
      holdNote: (text: string, withinMs: number) =>
        Effect.gen(function* () {
          yield* root.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              yield* tx.doc(NoteDoc, ROOT_CONVERSATION_ID);
            }),
          );
          yield* root.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              (yield* tx.doc(NoteDoc, ROOT_CONVERSATION_ID)).text = text;
            }),
            { persistence: { kind: "deferred", withinMs } },
          );
        }).pipe(Effect.orDie),
      /**
       * The length of the partial answer memory holds in the root's `core.live`, read in a commit that writes
       * nothing: no view is mounted, and what is held stays held.
       */
      partialLength: () =>
        root
          .commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              const message = (yield* tx.doc(LiveDoc, ROOT_CONVERSATION_ID)).generation?.message;
              const block = message?.content.find((part) => part.type === "text");
              return block?.type === "text" ? block.text.length : 0;
            }),
          )
          .pipe(Effect.orDie),
      /**
       * The root's note in memory and in storage, read past the Session. Memory is read in a commit that writes
       * nothing, which leaves what is held held.
       */
      note: () =>
        Effect.gen(function* () {
          const memory = yield* root
            .commit(
              Effect.gen(function* () {
                const tx = yield* Tx;
                return (yield* tx.doc(NoteDoc, ROOT_CONVERSATION_ID)).text;
              }),
            )
            .pipe(Effect.orDie);
          const record = yield* agentStorage
            .findDocument(
              {
                kind: NoteDoc.definition.kind,
                scope: { kind: "conversation", conversationId: ROOT_CONVERSATION_ID },
              },
              "current",
            )
            .pipe(Effect.orDie);
          const stored = Option.isNone(record)
            ? Option.none()
            : yield* agentStorage.document(record.value.id, "current").pipe(Effect.orDie);
          return {
            memory,
            stored: Option.getOrNull(
              Option.map(stored, (document) => String(document.value["text"] ?? "")),
            ),
          };
        }),
      /** Makes every later entry write fail, until `heal`. */
      poison: () =>
        Effect.sync(() => {
          storage.sql.exec("CREATE TABLE IF NOT EXISTS probe_poison (on_ INTEGER)");
          storage.sql.exec("INSERT INTO probe_poison VALUES (1)");
          storage.sql.exec(
            "CREATE TRIGGER IF NOT EXISTS probe_poison_entries BEFORE INSERT ON durable_entries WHEN (SELECT COUNT(*) FROM probe_poison) > 0 BEGIN SELECT RAISE(ABORT, 'probe: poisoned'); END",
          );
        }),
      heal: () => Effect.sync(() => storage.sql.exec("DELETE FROM probe_poison")),
    };
  });

/** `AgentProbe`'s methods, as other Workers see them. */
export type AgentProbeMethods = Effect.Success<ReturnType<typeof agentProbeMethods>>;

/** The object under test, in Alchemy's modular form: the class is its contract; `AgentProbeLive` implements it. */
export class AgentProbe extends Cloudflare.DurableObject<AgentProbe, AgentProbeMethods>()(
  "AgentProbe",
  { errors: Host.errors },
) {}

/**
 * The host serves the agent over this object's SQLite. The handlers that read `Instance` are built with the
 * agent, per instance; `KvTaskLive` and `Isolate` are provided to the object's Layer and built once per isolate, at
 * init, where Alchemy sees the KV binding.
 */
export const AgentProbeLive = AgentProbe.make(
  Effect.gen(function* () {
    const agent = yield* Host.agent(
      // Progress is stored right after each commit (`persistWithinMs: 0`), so the restart tests' crash keeps the
      // partial a cut request converts.
      Harness.layer({
        extensions: [ProbeExtension, KvExtension, SearchExtension],
        settings: () => ({ progress: { persistWithinMs: 0 } }),
      }).pipe(Layer.provide(Layer.mergeAll(BusyLive, SlowLive)), Layer.provideMerge(perInstance)),
      { storage: { keepAliveMs: KEEP_ALIVE_MS } },
    );
    const isolate = yield* Isolate;
    // Alchemy's two-phase constructor: the inner Effect is the instance's.
    // @effect-diagnostics-next-line returnEffectInGen:off
    return Effect.gen(function* () {
      const host = yield* agent.start;
      return host.methods(yield* agentProbeMethods(host, isolate));
    });
  }),
).pipe(
  Layer.provide(KvTaskLive),
  Layer.provide(Layer.mergeAll(IsolateLive, Cloudflare.KV.ReadWriteNamespaceBinding)),
);

/** The same object with the default keep-alive (30 s), in the inline form, for the long deployed keep-alive run. */
export class KeepAliveProbe extends Cloudflare.DurableObject<KeepAliveProbe>()(
  "KeepAliveProbe",
  Effect.map(
    Host.agent(
      Harness.layer({ extensions: [ProbeExtension] }).pipe(
        Layer.provide(Layer.mergeAll(BusyLive, SlowLive)),
        Layer.provideMerge(perInstance),
      ),
    ),
    (agent) =>
      Effect.gen(function* () {
        const host = yield* agent.start;
        return host.methods(yield* probeMethodsLive(host));
      }),
  ),
) {}

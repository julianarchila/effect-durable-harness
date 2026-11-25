import * as Rivet from "@alchemy.run/rivet";
import * as AgentTool from "@effect-durable-harness/core/AgentTool";
import { AgentStorage } from "@effect-durable-harness/core/AgentStorage";
import * as Extension from "@effect-durable-harness/core/Extension";
import * as Harness from "@effect-durable-harness/core/Harness";
import { ROOT_CONVERSATION_ID, TaskId } from "@effect-durable-harness/core/Ids";
import type { Message } from "@effect-durable-harness/core/Message";
import type { TaskRecord } from "@effect-durable-harness/core/Records";
import * as Task from "@effect-durable-harness/core/Task";
import { Tx } from "@effect-durable-harness/core/Tx";
import { commitWrites } from "@effect-durable-harness/core/testing/CommitWrites";
import * as FauxModel from "@effect-durable-harness/core/testing/FauxModel";
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
import * as Config from "effect/Config";
import * as Doc from "@effect-durable-harness/core/Doc";
import * as Host from "../../src/Host.ts";

/**
 * The host on Rivet: the agent of the workerd suite's `AgentProbe` (`packages/cloudflare/test/workerd/fixture`), cut
 * to what the Rivet suite asserts: the alarm, recovery after a crash and after the actor sleeps, a model turn, the
 * session socket, and waits in memory that outlast RivetKit's idle sleep.
 */

/** How far ahead the alarm stays while there is active work: short, so the tests recover within seconds. */
export const KEEP_ALIVE_MS = 2_000;

/** The fake model's reference, configured on the root conversation. */
export const FAUX = { provider: "faux", modelId: "faux-1" } as const;

/** Which activation is running, and on which node: a fresh one after every crash or sleep. */
export class Instance extends Context.Service<
  Instance,
  { readonly id: string; readonly startedAt: number; readonly node: string }
>()("@effect-durable-harness/rivet/test/fixture/Probe/Instance") {}

/** A conversation document the commit benchmark writes. */
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

/** What a probe task returns. */
export const ProbeResult = Schema.Struct({
  instance: Schema.String,
  instanceStartedAt: Schema.Number,
  at: Schema.Number,
});

const resultNow = Effect.gen(function* () {
  const instance = yield* Instance;
  return {
    instance: instance.id,
    instanceStartedAt: instance.startedAt,
    at: yield* Clock.currentTimeMillis,
  };
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
});

const SleepLive = Sleep.toLayer({
  phases: {
    sleep: (task, runtime) => runtime.sleepUntil(task.input.at, { phase: "woke" }),
    woke: (_task, runtime) =>
      Effect.gen(function* () {
        const result = yield* resultNow;
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

/** Works in `chunks` chunks of `chunkMs`, committing after each one, so a restart resumes from the last chunk. */
const Busy = Task.make({
  name: "probe.busy",
  version: 1,
  input: Schema.Struct({ chunks: Schema.Number, chunkMs: Schema.Number }),
  checkpoint: Schema.Struct({ phase: Schema.Literal("run"), done: Schema.Number }),
  result: ProbeResult,
  initial: () => ({ phase: "run", done: 0 }),
});

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
 * Waits in memory, inside its phase, for a `probe.sleep` child it creates with `abandonOnRestart`: active work with no
 * inbound event, which only the host's hold keeps in one activation. A new activation in between is visible twice:
 * its open abandons the child (`aborted`), and the rerun phase starts in another instance with a restart counted.
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
});

const HoldLive = Hold.toLayer({
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

const encodeNestResult = Schema.encodeSync(Schema.fromJsonString(NestResult));

/**
 * Calls `nap` and waits for it in memory (`executeTool` with `wait: true`) while `nap` is parked: the caller's
 * invocation stays occupied with no inbound event. Not replay-safe, so a new activation ends it `interrupted` instead
 * of answering.
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

const ProbeExtension = Extension.make({
  name: "probe",
  tasks: [Sleep, Busy, Hold],
  tools: [Nap, Nest],
});

const textOf = (message: Message | undefined): string => {
  if (message === undefined || message.role === "system") return "";
  if (Predicate.isString(message.content)) return message.content;
  return message.content.map((part) => (part.type === "text" ? part.text : "")).join("");
};

/**
 * The fake model answers from the request alone: `nest:<ms>` calls `nest`, a tool result is answered with `done`, and
 * anything else is echoed.
 */
const fakeModel = Layer.unwrap(
  Effect.sync(() => {
    const faux = FauxModel.make({ tokensPerSecond: 200 });
    const respond: FauxModel.FauxResponseFactory = (request) =>
      Effect.sync(() => {
        faux.appendResponses([respond]);
        const last = request.messages.at(-1);
        if (last?.role === "toolResult") return FauxModel.fauxAssistantMessage("done");
        const nest = /^nest:(\d+)$/.exec(textOf(last));
        if (nest !== null) {
          return FauxModel.fauxAssistantMessage(
            [FauxModel.fauxToolCall("nest", { ms: Number(nest[1]) })],
            { stopReason: "toolUse" },
          );
        }
        return FauxModel.fauxAssistantMessage(`echo: ${textOf(last)}`);
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
      // Fly sets it on every Machine; elsewhere (the local swarm) it is empty.
      node: yield* Config.String("FLY_MACHINE_ID").pipe(Config.withDefault("")),
    };
  }),
);

/** A task as the tests see it. */
export interface TaskView {
  readonly status: string;
  readonly result: typeof ProbeResult.Type | null;
}

/** A session socket connection: who opened it, from `?username=`. */
export const User = Schema.Struct({ username: Schema.String });

const probeMethods = (host: Host.Instance<typeof User.Type, Instance>) =>
  Effect.gen(function* () {
    const harness = host.harness;
    const storage = yield* AgentStorage;
    const self = yield* Instance;
    const state = yield* Rivet.DurableObjectState;
    const root = yield* harness.ensureRoot({ agent: { model: FAUX } }).pipe(Effect.orDie);
    return {
      /** The activation, its alarm and the kernel's wake. */
      state: () =>
        Effect.gen(function* () {
          const wake = yield* harness.wake.pipe(Effect.orDie);
          return {
            instance: self.id,
            instanceStartedAt: self.startedAt,
            node: self.node,
            now: yield* Clock.currentTimeMillis,
            alarm: yield* state.storage.getAlarm(),
            nextWakeAt: Option.getOrNull(wake.nextWakeAt),
            activeWork: wake.activeWork,
            sockets: (yield* state.getWebSockets()).length,
          };
        }),
      startTask: (kind: "probe.sleep" | "probe.busy", input: Schema.Json) =>
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
                : yield* tx.createTask(
                    Busy,
                    Schema.decodeUnknownSync(Busy.definition.input)(input),
                    {
                      ownership: { kind: "conversation" },
                    },
                  );
            }),
          )
          .pipe(Effect.map(Number), Effect.orDie),
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
            const taskState = record.value.state;
            return taskState.status === "terminal" && taskState.outcome.status === "completed"
              ? Schema.decodeUnknownSync(HoldResult)(taskState.outcome.result)
              : null;
          }),
        ),
      task: (id: number) =>
        harness.getTask(TaskId.make(id)).pipe(
          Effect.orDie,
          Effect.map((record): TaskView | null => {
            if (Option.isNone(record)) return null;
            const taskState = record.value.state;
            const result =
              taskState.status === "terminal" && taskState.outcome.status === "completed"
                ? Schema.decodeUnknownSync(ProbeResult)(taskState.outcome.result)
                : null;
            return { status: taskState.status, result };
          }),
        ),
      submit: (text: string, requestId: string) =>
        root.submit({ type: "input", content: text, requestId }).pipe(
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
      /** Resets the activation, as a crash does: the next call starts a fresh one from storage. */
      crash: () => host.abort("probe: crash"),
      /**
       * Commits a runnable task straight to storage, bypassing the Session, so nothing in memory knows about it:
       * only the alarm the commit armed can run it. Answers the alarm right after the commit.
       */
      orphanTask: (chunks: number, chunkMs: number) =>
        Effect.gen(function* () {
          const id = TaskId.make(yield* storage.mintId.pipe(Effect.orDie));
          const record: TaskRecord = {
            id,
            conversationId: ROOT_CONVERSATION_ID,
            kind: Busy.definition.name,
            version: Busy.definition.version,
            input: { chunks, chunkMs },
            background: false,
            abortRequested: false,
            state: { status: "pending", checkpoint: { phase: "run", done: 0 } },
          };
          yield* commitWrites(storage, [{ type: "task", value: record }]).pipe(Effect.orDie);
          return { id: Number(id), alarm: yield* state.storage.getAlarm() };
        }),
      /**
       * Commit latency: `sequential` commits one after the other on the root, then `parallel` fibers of `perFiber`
       * commits each, on conversations of their own (where group commit applies). Answers milliseconds.
       */
      bench: (sequential: number, parallel: number, perFiber: number) =>
        Effect.gen(function* () {
          const write = (conversation: typeof root, text: string) =>
            conversation
              .commit(
                Effect.gen(function* () {
                  const tx = yield* Tx;
                  (yield* tx.doc(NoteDoc, conversation.id)).text = text;
                }),
              )
              .pipe(Effect.orDie);
          yield* write(root, "warm");
          const sequentialStart = yield* Clock.currentTimeMillis;
          for (let index = 0; index < sequential; index++) yield* write(root, `s${index}`);
          const sequentialMs = (yield* Clock.currentTimeMillis) - sequentialStart;
          const conversations = yield* Effect.forEach(
            Array.from({ length: parallel }, (_, index) => index),
            () =>
              harness
                .createConversation({ agent: { model: FAUX }, ownership: { kind: "ownerless" } })
                .pipe(Effect.orDie),
          );
          yield* Effect.forEach(conversations, (conversation) => write(conversation, "warm"), {
            concurrency: "unbounded",
          });
          const parallelStart = yield* Clock.currentTimeMillis;
          yield* Effect.forEach(
            conversations,
            (conversation) =>
              Effect.forEach(
                Array.from({ length: perFiber }, (_, index) => index),
                (index) => write(conversation, `p${index}`),
                { discard: true },
              ),
            { concurrency: "unbounded", discard: true },
          );
          const parallelMs = (yield* Clock.currentTimeMillis) - parallelStart;
          return {
            perCommitMs: sequentialMs / sequential,
            parallelCommitsPerSecond: (parallel * perFiber * 1000) / Math.max(parallelMs, 1),
          };
        }),
      /** Sends an application event to every session socket connection; answers how many it reached. */
      broadcast: (text: string) => host.socket.send(() => true, { text }),
    };
  }).pipe(Effect.provideContext(host.context));

/** `Probe`'s methods, as callers see them. */
export type ProbeMethods = Effect.Success<ReturnType<typeof probeMethods>>;

/** The object under test, declared without its code: each target implements it on its pool (`swarm/`, `cloud/`). */
export class Probe extends Rivet.DurableObject<Probe, ProbeMethods>()("Probe") {}

/** `Probe`'s implementation, before the pool a target provides. */
export const probe = Effect.gen(function* () {
  const agent = yield* Host.agent(
    Harness.layer({
      extensions: [ProbeExtension],
      settings: () => ({ progress: { persistWithinMs: 0 } }),
    }).pipe(
      Layer.provide(Layer.mergeAll(BusyLive, SleepLive, HoldLive)),
      Layer.provideMerge(Layer.mergeAll(fakeModel, instance)),
    ),
    {
      storage: { keepAliveMs: KEEP_ALIVE_MS },
      socket: Host.socket({
        connection: User,
        accept: (request) =>
          Effect.succeed({
            username:
              new URL(request.url, "http://object").searchParams.get("username") ?? "anonymous",
          }),
      }),
    },
  );
  // @effect-diagnostics-next-line returnEffectInGen:off
  return Effect.gen(function* () {
    const host = yield* agent.start;
    return host.methods<ProbeMethods>(yield* probeMethods(host));
  });
});

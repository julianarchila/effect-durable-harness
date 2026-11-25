// Nested tool calls (`ToolContext.executeTool`) across a restart, on every backend.
//
// - A closed and reopened storage is a fresh storage of the backend that every Harness of a case opens in turn
//   (`freshInner`), each behind a `killable` controlled storage; closing a Harness writes back the tasks it
//   stopped.
// - `addTool` is an extension `tool:<name>` of `openChat`, `addHooks` one holding the hooks, and installed tasks
//   an extension holding them.
// - A task waiting for its abort is a handler that never returns until its fiber is interrupted; waiting for a
//   submission advances the `TestClock`; `waitFor` polls until a condition holds.
// - A `core.live` document change carries no `kind`, so the case that reads it takes a non-null value as a change.
// - "orphans an abandoned task whose definition is missing once an abort is requested": the reopened Harness is
//   opened without the task's extension.
//
// Skipped, since the registry does not change at runtime:
// - "keeps an abandoned task whose definition is missing waiting until it is installed, then runs its abort"
import { assert, it } from "@effect/vitest";
import * as Context from "effect/Context";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as AgentTool from "../src/AgentTool.ts";
import { AgentStorage } from "../src/AgentStorage.ts";
import type {
  ExecuteToolError,
  NestedToolExecutionResult,
  ToolExecutionResult,
} from "../src/AgentTool.ts";
import { ToolResultEntry } from "../src/Entry.ts";
import * as Extension from "../src/Extension.ts";
import * as Hook from "../src/Hook.ts";
import type { SubmissionId, TaskId } from "../src/Ids.ts";
import { LiveDoc } from "../src/Live.ts";
import type { AssistantMessage, ToolResultMessage } from "../src/Message.ts";
import type { EntryRecord, TaskRecord } from "../src/Records.ts";
import type { Submission } from "../src/Submission.ts";
import * as Task from "../src/Task.ts";
import { fauxAssistantMessage, fauxText, fauxToolCall } from "../src/testing/FauxModel.ts";
import { Tx } from "../src/Tx.ts";
import { allEntries, type ChatSetup, chatSetup, openChat, waitFor } from "./ChatSupport.ts";
import { killable, type TestHarness } from "./HarnessSupport.ts";
import { nestedResultOf, nestedResults, viewHistory } from "./LiveSupport.ts";
import { type Backend, describeEachBackend, run } from "./SessionSupport.ts";

type Inner = AgentStorage["Service"];

const Parameters = Schema.Struct({ text: Schema.optionalKey(Schema.String) });
type Args = typeof Parameters.Type;
type Extra = Omit<
  AgentTool.MakeOptions<Args, Schema.Json, never, never>,
  "description" | "parameters" | "execute"
>;

/** A tool named `name` with an empty parameter schema. */
const tool = (
  name: string,
  execute: (
    args: Args,
    api: AgentTool.Call,
  ) => Effect.Effect<ToolExecutionResult, ExecuteToolError | Schema.SchemaError>,
  extra: Extra = {},
) => AgentTool.make(name, { description: name, parameters: Parameters, execute, ...extra });

const toolExtension = (agentTool: AgentTool.Any) =>
  Extension.make({ name: `tool:${agentTool.name}`, tools: [agentTool] });

const text = (value: string) => ({ type: "text" as const, text: value });

const call = (name: string, id = "c1"): AssistantMessage =>
  fauxAssistantMessage([fauxToolCall(name, {}, { id })], { stopReason: "toolUse" });

const DONE = fauxAssistantMessage([fauxText("done")]);

/** A fresh storage of the backend, alive for the test's scope. */
const freshInner = (backend: Backend) =>
  Effect.gen(function* () {
    const context = yield* Layer.buildWithMemoMap(
      backend.layer,
      yield* Layer.makeMemoMap,
      yield* Effect.scope,
    );
    return Context.get(context, AgentStorage);
  });

/** A chat over `inner`, without starting scheduling. */
const open = (inner: Inner, setup: ChatSetup, extensions: ReadonlyArray<Extension.Any>) =>
  Effect.gen(function* () {
    const crashable = yield* killable(inner);
    return yield* openChat(crashable.storage, setup, { extensions });
  });

/** Runs `effect` to its end while the `TestClock` advances. */
const finished = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  Effect.gen(function* () {
    const fiber = yield* Effect.forkChild(effect, { startImmediately: true });
    yield* waitFor(Effect.sync(() => fiber.pollUnsafe() !== undefined));
    return yield* Fiber.join(fiber);
  });

const settle = (submission: Submission) => finished(submission.await);

/** Waits for the submission `id` to settle. */
const settled = (h: TestHarness, id: SubmissionId) =>
  Effect.flatMap(h.harness.submission(id), (found) => settle(Option.getOrThrow(found)));

const NestedInput = Schema.Struct({
  kind: Schema.Literal("nested"),
  call: Schema.Struct({ name: Schema.String }),
});

/** The nested tool tasks, in ID order. */
const nestedTasks = (h: TestHarness) =>
  h.harness
    .commit(
      Effect.gen(function* () {
        const tx = yield* Tx;
        return (yield* tx.scanTasks({ kind: "core.tool" }, 100)).items;
      }),
    )
    .pipe(
      Effect.map(
        (tasks): ReadonlyArray<TaskRecord> =>
          tasks.filter((task) => Schema.is(NestedInput)(task.input)),
      ),
    );

const results = (entries: ReadonlyArray<EntryRecord>): Array<ToolResultMessage> =>
  entries.flatMap((entry) => {
    const message = entry.model?.[0];
    return ToolResultEntry.is(entry) && message?.role === "toolResult" ? [message] : [];
  });

const codes = (result: NestedToolExecutionResult | undefined): Array<string> =>
  (result?.diagnostics ?? []).flatMap((diagnostic) =>
    diagnostic.code === undefined ? [] : [diagnostic.code],
  );

const outcomeOf = (
  record: TaskRecord | Task.TypedTaskRecord<Schema.Json, Schema.Json, unknown> | undefined,
) => (record?.state.status === "terminal" ? record.state.outcome : undefined);

const taskOf = (h: TestHarness, id: TaskId) =>
  h.harness.getTask(id).pipe(Effect.map(Option.getOrUndefined));

const Work = Schema.Struct({ phase: Schema.Literal("work") });

/**
 * Worker tasks: one `work` phase that never returns until its invocation is cancelled, and an abort handler
 * that records the abort reason and ends `aborted`.
 */
const worker = (
  name: string,
  record: { readonly phases?: Array<string>; readonly aborts?: Array<string | undefined> },
  started: Deferred.Deferred<void>,
) =>
  Task.make({
    name,
    version: 1,
    input: Schema.Null,
    checkpoint: Work,
    result: Schema.Null,
    initial: () => ({ phase: "work" }),
    phases: {
      work: () =>
        Effect.gen(function* () {
          record.phases?.push("work");
          yield* Deferred.succeed(started, undefined);
          return yield* Effect.never;
        }),
    },
    abort: (task, runtime) =>
      Effect.gen(function* () {
        record.aborts?.push(task.abortReason);
        if (record.phases !== undefined && record.aborts === undefined) {
          record.phases.push(`abort ${task.abortReason}`);
        }
        yield* runtime.aborted;
      }),
  });

/** The task `spawn` created, once it did. */
interface Created {
  id?: TaskId | undefined;
}

/** A `spawn` tool: creates `task` owned by the call, and waits for it. */
const spawn = (task: Task.Task<null, { readonly phase: "work" }, null, never>, created: Created) =>
  tool("spawn", (_args, api) =>
    Effect.gen(function* () {
      const id = yield* api.createTask(task, null, {
        ownership: { kind: "task", taskId: api.taskId },
      });
      created.id = id;
      yield* api.awaitTask(id);
      return {};
    }).pipe(Effect.orDie),
  );

/** A `batch` tool that makes one nested call of `name`. */
const batchOf = (name: string) =>
  tool("batch", (_args, api) => Effect.as(api.executeTool(name, {}), {}));

const awaited = (deferred: Deferred.Deferred<void>) => waitFor(Deferred.isDone(deferred));

describeEachBackend("nested tool calls across a restart", (backend) => {
  it.effect("abandons a replay-unsafe caller's unfinished nested calls before they run again", () =>
    run(backend)(
      Effect.gen(function* () {
        const inner = yield* freshInner(backend);
        const setup = chatSetup();
        const runs = { started: 0, gated: 0 };
        const running = Deferred.makeUnsafe<void>();
        const gated = Deferred.makeUnsafe<void>();
        // Replay-safe, so only the abandonment keeps it from rerunning.
        const started = tool(
          "started",
          (_args, api) =>
            Effect.gen(function* () {
              runs.started++;
              yield* api.output("partial\n");
              yield* api.details({ step: 1 });
              yield* Deferred.succeed(running, undefined);
              return yield* Effect.never;
            }).pipe(Effect.orDie),
          { replay: "safe" },
        );
        const gate = tool("gated", () =>
          Effect.sync(() => {
            runs.gated++;
            return {};
          }),
        );
        // Holds `gated` in its `call` phase, before intent, until the Harness closes.
        const hooks = Hook.tool({
          beforeTool: (hookCall) =>
            hookCall.name !== "gated"
              ? Effect.succeed(undefined)
              : Deferred.succeed(gated, undefined).pipe(Effect.andThen(Effect.never)),
        });
        const batch = tool("batch", (_args, api) =>
          Effect.as(
            Effect.all([api.executeTool("started", {}), api.executeTool("gated", {})], {
              concurrency: "unbounded",
            }),
            {},
          ),
        );
        const extensions = [
          toolExtension(started),
          toolExtension(gate),
          Extension.make({ name: "hooks", hooks: [hooks] }),
          toolExtension(batch),
        ];
        setup.faux.setResponses([call("batch"), DONE]);
        let opened = yield* open(inner, setup, extensions);
        const id = (yield* opened.root.submit({ type: "input", content: "go" })).id;
        yield* awaited(running);
        yield* awaited(gated);
        yield* opened.harness.close;

        opened = yield* open(inner, setup, extensions);
        const history = yield* viewHistory(opened.root);
        const nested = yield* nestedResults(opened.harness);
        yield* opened.harness.harness.resume;
        assert.strictEqual((yield* settled(opened.harness, id)).status, "done");
        yield* finished(opened.harness.harness.awaitIdle);
        history.detach();
        assert.deepStrictEqual(runs, { started: 1, gated: 0 });
        const [first, second] = yield* nestedTasks(opened.harness);
        for (const task of [first, second]) {
          assert.include(task, {
            abandonOnRestart: true,
            abortRequested: true,
            abortReason: "restart",
          });
          assert.strictEqual(outcomeOf(task)?.status, "aborted");
        }
        const end = (callId: string) => nestedResultOf(history.live(), nested, callId);
        assert.deepStrictEqual(codes(end("c1/1")), ["interrupted"]);
        assert.include(end("c1/1"), { isError: true });
        assert.deepStrictEqual(end("c1/1")?.details, { step: 1 });
        assert.notProperty(end("c1/1"), "structuredOutput");
        assert.deepStrictEqual(codes(end("c1/2")), ["abandoned"]);
        // The caller reports the interruption.
        const [result] = results(yield* allEntries(opened.root));
        assert.include(result, { toolCallId: "c1", isError: true });
        const live = yield* opened.harness.session.snapshot(LiveDoc, opened.root.id);
        assert.deepStrictEqual(Option.getOrUndefined(live), {});
        yield* opened.harness.close;
      }),
    ),
  );

  it.effect("abandons the work an abandoned nested call owns, which never runs a phase again", () =>
    run(backend)(
      Effect.gen(function* () {
        const inner = yield* freshInner(backend);
        const setup = chatSetup();
        const phases: Array<string> = [];
        const aborts: Array<string | undefined> = [];
        const started = Deferred.makeUnsafe<void>();
        const Worker = worker("test.worker", { phases, aborts }, started);
        const created: Created = {};
        const extensions = [
          Extension.make({ name: "worker", tasks: [Worker] }),
          toolExtension(spawn(Worker, created)),
          toolExtension(batchOf("spawn")),
        ];
        setup.faux.setResponses([call("batch"), DONE]);
        let opened = yield* open(inner, setup, extensions);
        const id = (yield* opened.root.submit({ type: "input", content: "go" })).id;
        yield* awaited(started);
        yield* opened.harness.close;

        opened = yield* open(inner, setup, extensions);
        yield* opened.harness.harness.resume;
        assert.strictEqual((yield* settled(opened.harness, id)).status, "done");
        yield* finished(opened.harness.harness.awaitIdle);
        assert.deepStrictEqual(phases, ["work"]);
        // The cascade passed the restart reason on.
        assert.deepStrictEqual(aborts, ["restart"]);
        assert.strictEqual(
          outcomeOf(yield* taskOf(opened.harness, created.id!))?.status,
          "aborted",
        );
        yield* opened.harness.close;
      }),
    ),
  );

  // Needs the worker's extension uninstalled and installed again while the Harness runs; the registry is static.
  it.effect.skip(
    "keeps an abandoned task whose definition is missing waiting until it is installed, then runs its abort",
    () => Effect.void,
  );

  it.effect(
    "orphans an abandoned task whose definition is missing once an abort is requested",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const inner = yield* freshInner(backend);
          const setup = chatSetup();
          const started = Deferred.makeUnsafe<void>();
          const Worker = worker("test.gone", {}, started);
          const created: Created = {};
          const tools = [toolExtension(spawn(Worker, created)), toolExtension(batchOf("spawn"))];
          setup.faux.setResponses([call("batch"), DONE]);
          let opened = yield* open(inner, setup, [
            Extension.make({ name: "gone", tasks: [Worker] }),
            ...tools,
          ]);
          const id = (yield* opened.root.submit({ type: "input", content: "go" })).id;
          yield* awaited(started);
          yield* opened.harness.close;

          // The reopened Harness is opened without the worker's extension.
          opened = yield* open(inner, setup, tools);
          yield* opened.harness.harness.resume;
          const h = opened.harness;
          yield* waitFor(
            Effect.map(taskOf(h, created.id!), (record) => record?.abortReason === "restart"),
          );
          yield* finished(opened.root.abort());
          const record = yield* taskOf(h, created.id!);
          assert.deepStrictEqual(outcomeOf(record), { status: "orphaned", reason: "missing_task" });
          assert.strictEqual((yield* settled(h, id)).status, "unanswered");
          yield* h.close;
        }),
      ),
  );

  it.effect("marks nothing when the reopened Harness never starts scheduling", () =>
    run(backend)(
      Effect.gen(function* () {
        const inner = yield* freshInner(backend);
        const setup = chatSetup();
        const running = Deferred.makeUnsafe<void>();
        const hang = tool("hang", () =>
          Deferred.succeed(running, undefined).pipe(Effect.andThen(Effect.never)),
        );
        const extensions = [toolExtension(hang), toolExtension(batchOf("hang"))];
        setup.faux.setResponses([call("batch"), DONE]);
        let opened = yield* open(inner, setup, extensions);
        yield* opened.root.submit({ type: "input", content: "go" });
        yield* awaited(running);
        yield* opened.harness.close;

        // An inspection-only open: no resume, no submission.
        opened = yield* open(inner, setup, extensions);
        const inspected = yield* opened.harness.harness.inspect;
        assert.strictEqual(inspected.scheduling, "paused");
        yield* opened.harness.close;

        opened = yield* open(inner, setup, extensions);
        const [nested] = yield* nestedTasks(opened.harness);
        assert.include(nested, { abandonOnRestart: true, abortRequested: false });
        assert.strictEqual(nested?.state.status, "pending");
        yield* opened.harness.close;
      }),
    ),
  );

  it.effect(
    "lets a replay-safe caller's unfinished nested calls recover and reattaches to them by key",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const inner = yield* freshInner(backend);
          const setup = chatSetup();
          const runs = { safe: 0, unsafe: 0, caller: 0 };
          const running = {
            safe: Deferred.makeUnsafe<void>(),
            unsafe: Deferred.makeUnsafe<void>(),
          };
          const safe = tool(
            "safe",
            () =>
              Effect.gen(function* () {
                runs.safe++;
                if (runs.safe === 1) {
                  yield* Deferred.succeed(running.safe, undefined);
                  return yield* Effect.never;
                }
                return { output: [text(`safe run ${runs.safe}`)] };
              }),
            { replay: "safe" },
          );
          const unsafe = tool("unsafe", () =>
            Effect.gen(function* () {
              runs.unsafe++;
              yield* Deferred.succeed(running.unsafe, undefined);
              return yield* Effect.never;
            }),
          );
          let got: ReadonlyArray<NestedToolExecutionResult> = [];
          const batch = tool(
            "batch",
            (_args, api) =>
              Effect.gen(function* () {
                runs.caller++;
                got = yield* Effect.all(
                  [api.executeTool("safe", {}), api.executeTool("unsafe", {})],
                  {
                    concurrency: "unbounded",
                  },
                );
                return {};
              }),
            { replay: "safe" },
          );
          const extensions = [toolExtension(safe), toolExtension(unsafe), toolExtension(batch)];
          setup.faux.setResponses([call("batch"), DONE]);
          let opened = yield* open(inner, setup, extensions);
          const id = (yield* opened.root.submit({ type: "input", content: "go" })).id;
          yield* awaited(running.safe);
          yield* awaited(running.unsafe);
          yield* opened.harness.close;

          opened = yield* open(inner, setup, extensions);
          yield* opened.harness.harness.resume;
          assert.strictEqual((yield* settled(opened.harness, id)).status, "done");
          assert.deepStrictEqual(runs, { safe: 2, unsafe: 1, caller: 2 });
          // Default keys follow call order, so the rerun found both calls.
          const tasks = yield* nestedTasks(opened.harness);
          assert.strictEqual(tasks.length, 2);
          for (const task of tasks) {
            assert.isUndefined(task.abandonOnRestart);
            assert.isUndefined(task.abortReason);
          }
          assert.strictEqual(got[0]?.structuredOutput, "safe run 2");
          assert.deepStrictEqual(codes(got[1]), ["interrupted"]);
          yield* opened.harness.close;
        }),
      ),
  );

  it.effect(
    "clears the slots of nested calls an earlier attempt finished when the rerun makes none",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const inner = yield* freshInner(backend);
          const setup = chatSetup();
          const done = Deferred.makeUnsafe<void>();
          let attempts = 0;
          const quick = tool("quick", () => Effect.succeed({ output: [text("ok")] }));
          const caller = tool(
            "caller",
            (_args, api) =>
              Effect.gen(function* () {
                attempts++;
                if (attempts === 1) {
                  yield* api.executeTool("quick", {});
                  yield* Deferred.succeed(done, undefined);
                  return yield* Effect.never;
                }
                return { output: [text("done")] };
              }),
            { replay: "safe" },
          );
          const extensions = [toolExtension(quick), toolExtension(caller)];
          setup.faux.setResponses([call("caller"), DONE]);
          let opened = yield* open(inner, setup, extensions);
          const id = (yield* opened.root.submit({ type: "input", content: "go" })).id;
          yield* awaited(done);
          // The finished nested call's slot is still listed, below its running caller.
          const before = yield* opened.harness.session.snapshot(LiveDoc, opened.root.id);
          assert.strictEqual(Option.getOrUndefined(before)?.nestedTools?.length, 1);
          yield* opened.harness.close;

          opened = yield* open(inner, setup, extensions);
          let slotsAfterCaller: number | undefined;
          yield* opened.harness.kernel.observeCommits({
            advance: (publication) => {
              for (const change of publication.changes) {
                if (change.type !== "task" || change.value.kind !== "core.tool") continue;
                const input = change.value.input;
                const model = Schema.is(Schema.Struct({ kind: Schema.Literal("model") }))(input);
                if (!model || change.value.state.status !== "terminal") continue;
                const live = publication.changes.find(
                  (each) => each.type === "document" && each.record.kind === "core.live",
                );
                // A document change has no `kind`: a non-null value is a change.
                const value =
                  live?.type === "document" && live.value !== null
                    ? Schema.decodeUnknownOption(
                        Schema.Struct({
                          nestedTools: Schema.optionalKey(Schema.Array(Schema.Json)),
                        }),
                      )(live.value)
                    : Option.none();
                slotsAfterCaller = Option.isSome(value)
                  ? (value.value.nestedTools?.length ?? 0)
                  : undefined;
              }
            },
            end: () => {},
          });
          yield* opened.harness.harness.resume;
          assert.strictEqual((yield* settled(opened.harness, id)).status, "done");
          assert.strictEqual(attempts, 2);
          // The caller's terminal commit removed the slot, though the rerun made no nested call.
          assert.strictEqual(slotsAfterCaller, 0);
          yield* opened.harness.close;
        }),
      ),
  );

  it.effect(
    "aborts a nested call an earlier attempt of a replay-safe caller left running and the rerun never made again",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const inner = yield* freshInner(backend);
          const setup = chatSetup();
          const running = Deferred.makeUnsafe<void>();
          let attempts = 0;
          const slow = tool(
            "slow",
            () => Deferred.succeed(running, undefined).pipe(Effect.andThen(Effect.never)),
            { replay: "safe" },
          );
          const caller = tool(
            "caller",
            (_args, api) =>
              Effect.gen(function* () {
                attempts++;
                // Only the first attempt calls `slow`; the rerun after the restart returns at once.
                if (attempts === 1) yield* api.executeTool("slow", {}, { key: "slow" });
                return { output: [text("done")] };
              }),
            { replay: "safe" },
          );
          const extensions = [toolExtension(slow), toolExtension(caller)];
          setup.faux.setResponses([call("caller"), DONE]);
          let opened = yield* open(inner, setup, extensions);
          const id = (yield* opened.root.submit({ type: "input", content: "go" })).id;
          yield* awaited(running);
          yield* opened.harness.close;

          opened = yield* open(inner, setup, extensions);
          yield* opened.harness.harness.resume;
          assert.strictEqual((yield* settled(opened.harness, id)).status, "done");
          assert.strictEqual(attempts, 2);
          // The rerun's settlement found the leftover call among the tasks it owns and aborted it.
          const [leftover] = yield* nestedTasks(opened.harness);
          assert.strictEqual(outcomeOf(leftover)?.status, "aborted");
          yield* opened.harness.close;
        }),
      ),
  );

  it.effect(
    "reattaches a replay-safe caller by default and explicit keys, and rejects a rerun that calls differently",
    () =>
      run(backend)(
        Effect.gen(function* () {
          for (const order of ["same", "swapped"] as const) {
            const inner = yield* freshInner(backend);
            const setup = chatSetup();
            const runs: Array<string> = [];
            const echo = tool("echo", (args) =>
              Effect.sync(() => {
                runs.push(args.text ?? "");
                return { output: [text(`echo ${args.text}`)] };
              }),
            );
            const blocked = Deferred.makeUnsafe<void>();
            let callerRuns = 0;
            let seen: Array<string> = [];
            const batch = tool(
              "batch",
              (_args, api) =>
                Effect.gen(function* () {
                  callerRuns++;
                  const first = callerRuns === 2 && order === "swapped" ? "b" : "a";
                  const one = yield* api.executeTool("echo", { text: first });
                  const named = yield* api.executeTool("echo", { text: "named" }, { key: "named" });
                  if (callerRuns === 1) {
                    yield* Deferred.succeed(blocked, undefined);
                    return yield* Effect.never;
                  }
                  seen = [one, named].map((result) => String(result.structuredOutput));
                  return {};
                }),
              { replay: "safe" },
            );
            const extensions = [toolExtension(echo), toolExtension(batch)];
            setup.faux.setResponses([call("batch"), DONE]);
            let opened = yield* open(inner, setup, extensions);
            const id = (yield* opened.root.submit({ type: "input", content: "go" })).id;
            yield* awaited(blocked);
            yield* opened.harness.close;

            opened = yield* open(inner, setup, extensions);
            yield* opened.harness.harness.resume;
            assert.strictEqual((yield* settled(opened.harness, id)).status, "done");
            const [result] = results(yield* allEntries(opened.root));
            if (order === "same") {
              assert.deepStrictEqual(runs, ["a", "named"]);
              assert.deepStrictEqual(seen, ["echo a", "echo named"]);
              assert.strictEqual(result!.isError, false);
            } else {
              // Key 1 was made with other arguments: the rerun fails instead of reusing a different call's result.
              assert.deepStrictEqual(runs, ["a", "named"]);
              assert.strictEqual(result!.isError, true);
              assert.include(
                // The text items hold the message.
                result!.content.map((item) => (item.type === "text" ? item.text : "")).join("|"),
                "Nested call c1/1 was already made with another tool",
              );
            }
            yield* opened.harness.close;
          }
        }),
      ),
  );

  it.effect("abandons the child tasks a replay-unsafe tool created and awaited in memory", () =>
    run(backend)(
      Effect.gen(function* () {
        const inner = yield* freshInner(backend);
        const setup = chatSetup();
        const phases: Array<string> = [];
        const started = Deferred.makeUnsafe<void>();
        const Worker = worker("test.direct-worker", { phases }, started);
        const work = tool("work", (_args, api) =>
          Effect.gen(function* () {
            const id = yield* api.createTask(Worker, null, {
              ownership: { kind: "task", taskId: api.taskId },
            });
            yield* api.awaitTask(id);
            return {};
          }).pipe(Effect.orDie),
        );
        const extensions = [
          Extension.make({ name: "direct-worker", tasks: [Worker] }),
          toolExtension(work),
        ];
        setup.faux.setResponses([call("work"), DONE]);
        let opened = yield* open(inner, setup, extensions);
        const id = (yield* opened.root.submit({ type: "input", content: "go" })).id;
        yield* awaited(started);
        yield* opened.harness.close;

        opened = yield* open(inner, setup, extensions);
        yield* opened.harness.harness.resume;
        assert.strictEqual((yield* settled(opened.harness, id)).status, "done");
        assert.deepStrictEqual(phases, ["work", "abort restart"]);
        yield* opened.harness.close;
      }),
    ),
  );

  // A replay-unsafe tool cannot run again after a restart, so the host must not let the object hibernate under
  // its in-memory wait: the call's invocation is active work while the task it awaits sleeps.
  it.effect("reports a replay-unsafe tool awaiting a sleeping task in memory as active work", () =>
    run(backend)(
      Effect.gen(function* () {
        const inner = yield* freshInner(backend);
        const setup = chatSetup();
        const Sleeper = Task.make({
          name: "test.direct-sleeper",
          version: 1,
          input: Schema.Null,
          checkpoint: Schema.Union([
            Schema.Struct({ phase: Schema.Literal("sleep") }),
            Schema.Struct({ phase: Schema.Literal("done") }),
          ]),
          result: Schema.Null,
          initial: () => ({ phase: "sleep" }),
          phases: {
            sleep: (_task, runtime) => runtime.sleepUntil(86_400_000, { phase: "done" }),
            done: (_task, runtime) => runtime.complete(null),
          },
          abort: () => Effect.void,
        });
        let child: TaskId | undefined;
        const work = tool("work", (_args, api) =>
          Effect.gen(function* () {
            // Kept across a restart: the wait itself is all a restart would lose.
            child = yield* api.createTask(Sleeper, null, {
              ownership: { kind: "task", taskId: api.taskId },
              abandonOnRestart: false,
            });
            yield* api.awaitTask(child);
            return {};
          }).pipe(Effect.orDie),
        );
        const extensions = [
          Extension.make({ name: "direct-sleeper", tasks: [Sleeper] }),
          toolExtension(work),
        ];
        setup.faux.setResponses([call("work"), DONE]);
        const opened = yield* open(inner, setup, extensions);
        yield* opened.root.submit({ type: "input", content: "go" });
        yield* waitFor(
          Effect.map(
            opened.harness.harness.wake,
            (wake) => Option.getOrUndefined(wake.nextWakeAt) === 86_400_000,
          ),
        );
        assert.strictEqual((yield* taskOf(opened.harness, child!))?.state.status, "sleeping");
        assert.deepStrictEqual(yield* opened.harness.harness.wake, {
          nextWakeAt: Option.some(86_400_000),
          activeWork: true,
        });
        yield* opened.harness.close;
      }),
    ),
  );

  it.effect(
    "leaves background work under an abandoned nested call running, and settles without it",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const inner = yield* freshInner(backend);
          const setup = chatSetup();
          const started = Deferred.makeUnsafe<void>();
          const Background = worker("test.background", {}, started);
          let background: TaskId | undefined;
          const spawnTool = tool("spawn", (_args, api) =>
            Effect.gen(function* () {
              // A conversation the call owns, with background work in it, as a persistent subagent has.
              background = yield* api.commit(
                Effect.gen(function* () {
                  const tx = yield* Tx;
                  const child = yield* tx.createConversation({
                    ownership: { kind: "task", taskId: api.taskId },
                  });
                  return yield* tx.createTask(Background, null, {
                    conversationId: child.id,
                    ownership: { kind: "conversation" },
                    background: true,
                  });
                }),
              );
              return yield* Effect.never;
            }).pipe(Effect.orDie),
          );
          const extensions = [
            Extension.make({ name: "background", tasks: [Background] }),
            toolExtension(spawnTool),
            toolExtension(batchOf("spawn")),
          ];
          setup.faux.setResponses([call("batch"), DONE]);
          let opened = yield* open(inner, setup, extensions);
          const id = (yield* opened.root.submit({ type: "input", content: "go" })).id;
          yield* awaited(started);
          yield* opened.harness.close;

          opened = yield* open(inner, setup, extensions);
          yield* opened.harness.harness.resume;
          assert.strictEqual((yield* settled(opened.harness, id)).status, "done");
          const [spawned] = yield* nestedTasks(opened.harness);
          assert.strictEqual(spawned?.abortReason, "restart");
          assert.strictEqual(spawned?.state.status, "terminal");
          const record = yield* taskOf(opened.harness, background!);
          assert.strictEqual(record?.abortRequested, false);
          assert.strictEqual(record?.state.status, "running");
          yield* finished(opened.harness.harness.abortTask(background!));
          yield* opened.harness.close;
        }),
      ),
  );
});

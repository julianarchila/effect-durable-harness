// Hooks that start tasks: `afterTools` tasks that end the run or answer it with a steer, and `onYield` tasks that
// decide whether an answer stands while the run's inputs stay open.
//
// Notes:
// - Extensions are the static extensions of `openChat`. A crash is `HarnessSupport.killable`: a killed Harness
//   writes nothing more, and the next one opens over the same storage, on every backend.
// - A decider's steer is admitted through its conversation handle in its terminal commit.
// - The invalid continuations are one test each, each over its own storage.
// - "At the restart limit" writes the generation `running` with `restarts: 5` with `commitWrites` after a close,
//   as a dead process leaves it; "when the scheduler ends the generation" stages the cleanup with the internal
//   `settleSchedulerOutcome` inside a commit.
// - The last suite covers combinations: several `onYield` tasks, a continuation with a malformed optional
//   key, `afterTools` tasks of two extensions, and an abort and the restart limit in the `afterTools` phase.
import { assert, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as AgentTool from "../src/AgentTool.ts";
import { AgentStorage } from "../src/AgentStorage.ts";
import * as Extension from "../src/Extension.ts";
import type { Conversation } from "../src/Harness.ts";
import * as Hook from "../src/Hook.ts";
import type { EntryId, TaskId } from "../src/Ids.ts";
import { settleSchedulerOutcome } from "../src/internal/live.ts";
import { LiveDoc, type LiveRun } from "../src/Live.ts";
import * as Signal from "../src/Signal.ts";
import type { TaskRecord } from "../src/Records.ts";
import type { SettledSubmission, Submission } from "../src/Submission.ts";
import * as Task from "../src/Task.ts";
import { commitWrites } from "../src/testing/CommitWrites.ts";
import { fauxAssistantMessage, fauxText, fauxToolCall } from "../src/testing/FauxModel.ts";
import { allEntries, type ChatSetup, chatSetup, openChat, textOf, waitFor } from "./ChatSupport.ts";
import { abortedWith, completed, killable, type TestHarness } from "./HarnessSupport.ts";
import { controlledStorage, describeEachBackend, run } from "./SessionSupport.ts";

const CALL = fauxAssistantMessage([fauxToolCall("noop", {}, { id: "c1" })], {
  stopReason: "toolUse",
});

/** What a hook task completes with, an optional steer it submits in its terminal commit, and a gate it waits for. */
interface Plan {
  readonly result: Schema.Json;
  readonly steer?: string | undefined;
  readonly gate?: Deferred.Deferred<void> | undefined;
}

/** What the deciders saw: the answer each read from `run.yield`, and how often the hook was asked. */
interface Seen {
  readonly yields: Array<EntryId | undefined>;
  asked: number;
}

const DecideCheckpoint = Schema.Struct({ phase: Schema.Literal("decide") });

/** A task that waits for its plan's gate, submits its steer, and completes with its plan's result. */
const deciderTask = (plan: Plan, seen: Seen, name = "test.decider") =>
  Task.make({
    name,
    version: 1,
    input: Schema.Null,
    checkpoint: DecideCheckpoint,
    result: Schema.Json,
    initial: () => ({ phase: "decide" }),
    phases: {
      decide: (_task, runtime) =>
        Effect.gen(function* () {
          if (plan.gate !== undefined) yield* Deferred.await(plan.gate);
          // The answer a yield task decides about is in `run.yield`.
          const live = Option.getOrUndefined(
            yield* runtime.snapshot(LiveDoc, runtime.conversationId),
          );
          seen.yields.push(live?.run?.yield?.answer);
          const handle = Option.getOrThrow(yield* runtime.conversation(runtime.conversationId));
          yield* runtime.commit(
            Effect.gen(function* () {
              if (plan.steer !== undefined) {
                yield* handle.admit({ type: "input", content: plan.steer, whenBusy: "steer" });
              }
              return completed(plan.result);
            }),
          );
        }),
    },
    abort: (_task, runtime) => runtime.commit(Effect.succeed(abortedWith("test"))),
  });

const noop = AgentTool.make("noop", {
  description: "noop",
  parameters: Schema.Struct({}),
  execute: () => Effect.succeed({}),
});

/** The deciders extension: `noop`, the decider, and a hook that asks for one decider the first time only. */
const setupWith = (hookName: "afterTools" | "onYield", plan: Plan) => {
  const setup = chatSetup();
  const seen: Seen = { yields: [], asked: 0 };
  const Decider = deciderTask(plan, seen);
  const handler = () =>
    Effect.sync(() => {
      seen.asked++;
      return seen.asked === 1 ? { tasks: [Hook.hookTask(Decider, null)] } : undefined;
    });
  const extension = Extension.make({
    name: "deciders",
    tools: [noop],
    tasks: [Decider],
    hooks: [
      Hook.generation(hookName === "afterTools" ? { afterTools: handler } : { onYield: handler }),
    ],
  });
  return { setup, seen, extension };
};

const open = (setup: ChatSetup, extensions: ReadonlyArray<Extension.Any>) =>
  Effect.gen(function* () {
    const opened = yield* openChat(yield* controlledStorage, setup, { extensions });
    yield* opened.harness.harness.resume;
    return opened;
  });

/** A chat over `inner`; `kill` simulates a crash: nothing more lands. */
const openKillable = (
  inner: AgentStorage["Service"],
  setup: ChatSetup,
  extensions: ReadonlyArray<Extension.Any>,
) =>
  Effect.gen(function* () {
    const crashable = yield* killable(inner);
    const opened = yield* openChat(crashable.storage, setup, { extensions });
    yield* opened.harness.harness.resume;
    return { ...opened, kill: crashable.kill.pipe(Effect.andThen(opened.harness.close)) };
  });

/** Runs `effect` to its end while the `TestClock` advances. */
const finished = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  Effect.gen(function* () {
    const fiber = yield* Effect.forkChild(effect, { startImmediately: true });
    yield* waitFor(Effect.sync(() => fiber.pollUnsafe() !== undefined));
    return yield* Fiber.join(fiber);
  });

const settle = (submission: Submission) => finished(submission.await);

const texts = (root: Conversation) =>
  Effect.map(allEntries(root), (entries) =>
    entries.flatMap((entry) => {
      const text = textOf(entry.model?.[0]);
      return text === undefined ? [] : [text];
    }),
  );

const live = (h: TestHarness, root: Conversation) =>
  Effect.map(h.session.snapshot(LiveDoc, root.id), Option.getOrUndefined);

/** The run, once its `onYield` tasks decide. */
const yielding = (h: TestHarness, root: Conversation) =>
  Effect.gen(function* () {
    let run: LiveRun | undefined;
    yield* waitFor(
      Effect.map(live(h, root), (state) => {
        run = state?.run;
        return run?.yield !== undefined;
      }),
    );
    return run!;
  });

const answerOf = (settled: SettledSubmission) =>
  settled.status === "done" && "answer" in settled ? settled.answer : undefined;

describeEachBackend("afterTools tasks", (backend) => {
  it.effect(
    "end the run with { terminate: true }, as a round whose calls all asked to terminate",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const { setup, extension } = setupWith("afterTools", { result: { terminate: true } });
          setup.faux.setResponses([CALL]);
          const { harness, root } = yield* open(setup, [extension]);
          const settled = yield* settle(yield* root.submit({ type: "input", content: "go" }));
          assert.strictEqual(settled.status, "done");
          // Answered with the tool-calling answer; no further request.
          const assistant = (yield* allEntries(root)).find(
            (entry) => entry.kind === "core.assistant",
          );
          assert.strictEqual(answerOf(settled), assistant?.id);
          assert.strictEqual(setup.faux.state.callCount, 1);
          yield* harness.close;
        }),
      ),
  );

  it.effect("answer the run with a steer the next request sees", () =>
    run(backend)(
      Effect.gen(function* () {
        const { setup, extension } = setupWith("afterTools", {
          result: null,
          steer: "also check the tests",
        });
        setup.faux.setResponses([CALL, fauxAssistantMessage([fauxText("checked")])]);
        const { harness, root } = yield* open(setup, [extension]);
        const settled = yield* settle(yield* root.submit({ type: "input", content: "go" }));
        assert.strictEqual(settled.status, "done");
        assert.deepStrictEqual(yield* texts(root), ["go", "also check the tests", "checked"]);
        yield* harness.close;
      }),
    ),
  );

  it.effect("report a result that is no AfterToolsDecision and go on", () =>
    run(backend)(
      Effect.gen(function* () {
        const { setup, extension } = setupWith("afterTools", { result: { continue: "x" } });
        setup.faux.setResponses([CALL, fauxAssistantMessage([fauxText("done")])]);
        const { harness, root } = yield* open(setup, [extension]);
        const settled = yield* settle(yield* root.submit({ type: "input", content: "go" }));
        assert.strictEqual(settled.status, "done");
        assert.isTrue(
          setup.reports.some(
            (report) => report.kind === "extension_error" && report.extension === "deciders",
          ),
        );
        assert.strictEqual(setup.faux.state.callCount, 2);
        yield* harness.close;
      }),
    ),
  );
});

describeEachBackend("onYield tasks", (backend) => {
  it.effect("keep the inputs open while they decide, and continue the run with { continue }", () =>
    run(backend)(
      Effect.gen(function* () {
        const gate = Deferred.makeUnsafe<void>();
        const { setup, seen, extension } = setupWith("onYield", {
          result: { continue: "review: add a test" },
          gate,
        });
        setup.faux.setResponses([
          fauxAssistantMessage([fauxText("first")]),
          fauxAssistantMessage([fauxText("second")]),
        ]);
        const { harness, root } = yield* open(setup, [extension]);
        const submission = yield* root.submit({ type: "input", content: "go" });
        yield* yielding(harness, root);
        // The answer is appended, the input not yet answered.
        assert.strictEqual((yield* submission.status).status, "placed");
        yield* Deferred.succeed(gate, undefined);
        const settled = yield* settle(submission);
        assert.deepStrictEqual(yield* texts(root), ["go", "first", "review: add a test", "second"]);
        const entries = yield* allEntries(root);
        assert.strictEqual(answerOf(settled), entries.at(-1)?.id);
        const first = entries.find((entry) => textOf(entry.model?.[0]) === "first")?.id;
        assert.deepStrictEqual(seen.yields, [first]);
        yield* harness.close;
      }),
    ),
  );

  const invalid: ReadonlyArray<Schema.Json> = [
    5,
    [],
    [{ type: "text" }],
    [{ type: "image", data: "x" }],
  ];
  for (const input of invalid) {
    it.effect(
      `ignore a continuation that holds no valid user input (${JSON.stringify(input)})`,
      () =>
        run(backend)(
          Effect.gen(function* () {
            const { setup, extension } = setupWith("onYield", { result: { continue: input } });
            setup.faux.setResponses([fauxAssistantMessage([fauxText("first")])]);
            const { harness, root } = yield* open(setup, [extension]);
            const settled = yield* settle(yield* root.submit({ type: "input", content: "go" }));
            assert.strictEqual(settled.status, "done");
            assert.deepStrictEqual(yield* texts(root), ["go", "first"]);
            yield* harness.close;
          }),
        ),
    );
  }

  it.effect("let an answer that stands run its afterRun tasks next, without run.yield", () =>
    run(backend)(
      Effect.gen(function* () {
        const { setup, extension } = setupWith("onYield", { result: null });
        const asked: Array<unknown> = [];
        const afterRun = Extension.make({
          name: "after-run",
          hooks: [
            Hook.generation({
              afterRun: (_run, api) =>
                Effect.gen(function* () {
                  const state = yield* api.snapshot(LiveDoc, api.conversationId);
                  asked.push(Option.getOrUndefined(state)?.run?.yield);
                  return undefined;
                }).pipe(Effect.orDie),
            }),
          ],
        });
        setup.faux.setResponses([fauxAssistantMessage([fauxText("first")])]);
        const { harness, root } = yield* open(setup, [extension, afterRun]);
        const settled = yield* settle(yield* root.submit({ type: "input", content: "go" }));
        assert.strictEqual(settled.status, "done");
        yield* finished(root.awaitIdle);
        assert.deepStrictEqual(asked, [undefined]);
        yield* harness.close;
      }),
    ),
  );

  it.effect("let the answer stand without a continuation", () =>
    run(backend)(
      Effect.gen(function* () {
        const { setup, extension } = setupWith("onYield", { result: null });
        setup.faux.setResponses([fauxAssistantMessage([fauxText("first")])]);
        const { harness, root } = yield* open(setup, [extension]);
        const settled = yield* settle(yield* root.submit({ type: "input", content: "go" }));
        assert.strictEqual(settled.status, "done");
        assert.deepStrictEqual(yield* texts(root), ["go", "first"]);
        assert.isUndefined((yield* live(harness, root))?.run);
        yield* harness.close;
      }),
    ),
  );

  it.effect("lose their continuation to a user message queued meanwhile", () =>
    run(backend)(
      Effect.gen(function* () {
        const gate = Deferred.makeUnsafe<void>();
        const { setup, extension } = setupWith("onYield", {
          result: { continue: "review: add a test" },
          gate,
        });
        setup.faux.setResponses([
          fauxAssistantMessage([fauxText("first")]),
          fauxAssistantMessage([fauxText("to you")]),
        ]);
        const { harness, root } = yield* open(setup, [extension]);
        const submission = yield* root.submit({ type: "input", content: "go" });
        yield* yielding(harness, root);
        const queued = yield* root.submit({
          type: "input",
          content: "a question",
          whenBusy: "followUp",
        });
        yield* Deferred.succeed(gate, undefined);
        assert.strictEqual((yield* settle(submission)).status, "done");
        assert.strictEqual((yield* settle(queued)).status, "done");
        assert.deepStrictEqual(yield* texts(root), ["go", "first", "a question", "to you"]);
        yield* harness.close;
      }),
    ),
  );

  it.effect(
    "settle the inputs done with the answer when the run is aborted while they decide",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const gate = Deferred.makeUnsafe<void>();
          const { setup, extension } = setupWith("onYield", {
            result: { continue: "never" },
            gate,
          });
          setup.faux.setResponses([fauxAssistantMessage([fauxText("first")])]);
          const { harness, root } = yield* open(setup, [extension]);
          const submission = yield* root.submit({ type: "input", content: "go" });
          const run = yield* yielding(harness, root);
          yield* finished(root.abort());
          const settled = yield* settle(submission);
          assert.strictEqual(settled.status, "done");
          assert.strictEqual(answerOf(settled), run.yield?.answer);
          const state = yield* live(harness, root);
          assert.isUndefined(state?.run);
          assert.isUndefined(state?.runFailure);
          yield* harness.close;
        }),
      ),
  );
});

/** Delivered to a parking decider to let it complete. */
const Go = Signal.make("test.go", { version: 1, schema: Schema.Struct({}) });
const ParkCheckpoint = Schema.Union([
  Schema.Struct({ phase: Schema.Literal("park") }),
  Schema.Struct({ phase: Schema.Literal("done") }),
]);

/** A decider that parks on `Go` until a delivery, then completes with `result`. */
const parkingDecider = (result: Schema.Json) =>
  Task.make({
    name: "test.parking-decider",
    version: 1,
    input: Schema.Null,
    checkpoint: ParkCheckpoint,
    result: Schema.Json,
    initial: () => ({ phase: "park" }),
    phases: {
      park: (_task, runtime) =>
        runtime.sleep({ checkpoint: { phase: "done" }, signals: [Go.key("go")] }),
      done: (_task, runtime) => runtime.commit(Effect.succeed(completed(result))),
    },
    abort: (_task, runtime) => runtime.commit(Effect.succeed(abortedWith("test"))),
  });

describeEachBackend("hook task recovery", (backend) => {
  it.effect(
    "asks the afterTools hooks again after a crash before their tasks exist, not after",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const inner = yield* AgentStorage;
          const setup = chatSetup();
          const Decider = parkingDecider(null);
          const gate = Deferred.makeUnsafe<void>();
          let asked = 0;
          const extensions = [
            Extension.make({
              name: "deciders",
              tools: [noop],
              tasks: [Decider],
              hooks: [
                Hook.generation({
                  afterTools: () =>
                    Effect.gen(function* () {
                      asked++;
                      // The first process dies while its hook runs.
                      if (asked === 1) yield* Deferred.await(gate);
                      return { tasks: [Hook.hookTask(Decider, null)] };
                    }),
                }),
              ],
            }),
          ];
          setup.faux.setResponses([CALL, fauxAssistantMessage([fauxText("done")])]);
          const crashed = yield* openKillable(inner, setup, extensions);
          const id = (yield* crashed.root.submit({ type: "input", content: "go" })).id;
          yield* waitFor(Effect.sync(() => asked === 1));
          yield* crashed.kill;
          const next = yield* openKillable(inner, setup, extensions);
          // Asked again: nothing was created before the crash.
          yield* waitFor(Effect.sync(() => asked === 2));
          let decider: TaskId | undefined;
          yield* waitFor(
            Effect.gen(function* () {
              const tasks = (yield* next.harness.harness.inspect).tasks;
              decider = tasks.find((task) => task.record.kind === "test.parking-decider")?.record
                .id;
              if (decider === undefined) return false;
              const record = Option.getOrUndefined(yield* next.harness.harness.getTask(decider));
              return record?.state.status === "sleeping";
            }),
          );
          // With the task waiting, the next process resumes the wait without asking.
          yield* next.harness.close;
          const third = yield* openKillable(inner, setup, extensions);
          yield* third.harness.harness.signal(decider!, Go, "go", {});
          const submission = Option.getOrThrow(yield* third.harness.harness.submission(id));
          assert.strictEqual((yield* settle(submission)).status, "done");
          assert.strictEqual(asked, 2);
          yield* third.harness.close;
        }),
      ),
  );

  it.effect(
    "settles a yield's inputs done with its answer at the restart limit, without a runFailure",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const inner = yield* AgentStorage;
          const setup = chatSetup();
          const Decider = parkingDecider({ continue: "never" });
          const extensions = [
            Extension.make({
              name: "deciders",
              tasks: [Decider],
              hooks: [
                Hook.generation({
                  onYield: () => Effect.succeed({ tasks: [Hook.hookTask(Decider, null)] }),
                }),
              ],
            }),
          ];
          setup.faux.setResponses([fauxAssistantMessage([fauxText("first")])]);
          let opened = yield* openKillable(inner, setup, extensions);
          const id = (yield* opened.root.submit({ type: "input", content: "go" })).id;
          const held = yield* yielding(opened.harness, opened.root);
          yield* opened.harness.close;
          // Cut off in `yield` as often as the limit allows: as a dead process leaves it, `running` with its count.
          const record = Option.getOrThrow(yield* inner.task(held.taskId));
          assert.strictEqual(record.state.status, "waiting");
          if (record.state.status !== "waiting") return;
          yield* commitWrites(inner, [
            {
              type: "task",
              value: {
                ...record,
                state: { status: "running", checkpoint: record.state.checkpoint },
                restarts: 5,
              },
            },
          ]);
          opened = yield* openKillable(inner, setup, extensions);
          const submission = Option.getOrThrow(yield* opened.harness.harness.submission(id));
          const settled = yield* settle(submission);
          assert.strictEqual(answerOf(settled), held.yield?.answer);
          const state = yield* live(opened.harness, opened.root);
          assert.deepStrictEqual([state?.run, state?.runFailure], [undefined, undefined]);
          yield* opened.harness.close;
        }),
      ),
  );

  it.effect(
    "settles a yield's inputs done with its answer when the scheduler ends the generation",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const setup = chatSetup();
          const Decider = parkingDecider({ continue: "never" });
          const extension = Extension.make({
            name: "deciders",
            tasks: [Decider],
            hooks: [
              Hook.generation({
                onYield: () => Effect.succeed({ tasks: [Hook.hookTask(Decider, null)] }),
              }),
            ],
          });
          setup.faux.setResponses([fauxAssistantMessage([fauxText("first")])]);
          const { harness, root } = yield* open(setup, [extension]);
          const submission = yield* root.submit({ type: "input", content: "go" });
          const held = yield* yielding(harness, root);
          const storage = yield* AgentStorage;
          const generation = Option.getOrThrow(yield* storage.task(held.taskId));
          // The Harness cleanup the scheduler stages with a `faulted` outcome it writes.
          yield* harness.harness.commit(
            settleSchedulerOutcome(generation, {
              status: "faulted",
              error: { message: "broken" },
            }),
          );
          const settled = yield* settle(submission);
          assert.strictEqual(answerOf(settled), held.yield?.answer);
          assert.isUndefined((yield* live(harness, root))?.runFailure);
          yield* harness.close;
        }),
      ),
  );
});

/** The run's generation once it waits in `phase`, read from storage. */
const generationIn = (h: TestHarness, root: Conversation, phase: string) =>
  Effect.gen(function* () {
    const storage = yield* AgentStorage;
    let record: TaskRecord | undefined;
    yield* waitFor(
      Effect.gen(function* () {
        const taskId = (yield* live(h, root))?.run?.taskId;
        if (taskId === undefined) return false;
        record = Option.getOrUndefined(yield* storage.task(taskId));
        const state = record?.state;
        return state?.status === "waiting" && Schema.is(PhaseOf(phase))(state.checkpoint);
      }),
    );
    return record!;
  });

const PhaseOf = (phase: string) => Schema.Struct({ phase: Schema.Literal(phase) });

/** The tasks a generation waiting on hook tasks waits on. */
const HookWait = Schema.Struct({ tasks: Schema.Array(Schema.Number) });

const yieldTo = (continuation: Schema.Json) => {
  const { setup, extension } = setupWith("onYield", { result: { continue: continuation } });
  setup.faux.setResponses([
    fauxAssistantMessage([fauxText("first")]),
    fauxAssistantMessage([fauxText("second")]),
  ]);
  return { setup, extension };
};

describeEachBackend("hook tasks, combined", (backend) => {
  it.effect("continue with a text item whose malformed optional key is dropped", () =>
    run(backend)(
      Effect.gen(function* () {
        const { setup, extension } = yieldTo([
          { type: "text", text: "review: add a test", textSignature: 5 },
        ]);
        const { harness, root } = yield* open(setup, [extension]);
        const settled = yield* settle(yield* root.submit({ type: "input", content: "go" }));
        assert.strictEqual(settled.status, "done");
        assert.deepStrictEqual(yield* texts(root), ["go", "first", "review: add a test", "second"]);
        const user = (yield* allEntries(root)).find(
          (entry) => textOf(entry.model?.[0]) === "review: add a test",
        );
        const message = user?.model?.[0];
        assert.deepStrictEqual(message?.role === "user" ? message.content : undefined, [
          { type: "text", text: "review: add a test" },
        ]);
        yield* harness.close;
      }),
    ),
  );

  it.effect("take the continuation of the first onYield task that has one, in creation order", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        const seen: Seen = { yields: [], asked: 0 };
        const Stands = deciderTask({ result: null }, seen, "test.stands");
        const Again = deciderTask({ result: { continue: "again" } }, seen, "test.again");
        const extension = Extension.make({
          name: "deciders",
          tasks: [Stands, Again],
          hooks: [
            Hook.generation({
              onYield: () =>
                Effect.sync(() => {
                  seen.asked++;
                  return seen.asked === 1
                    ? { tasks: [Hook.hookTask(Stands, null), Hook.hookTask(Again, null)] }
                    : undefined;
                }),
            }),
          ],
        });
        setup.faux.setResponses([
          fauxAssistantMessage([fauxText("first")]),
          fauxAssistantMessage([fauxText("second")]),
        ]);
        const { harness, root } = yield* open(setup, [extension]);
        const settled = yield* settle(yield* root.submit({ type: "input", content: "go" }));
        assert.strictEqual(settled.status, "done");
        assert.deepStrictEqual(yield* texts(root), ["go", "first", "again", "second"]);
        yield* harness.close;
      }),
    ),
  );

  it.effect("create the afterTools tasks of every extension, in extension order", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        const seen: Seen = { yields: [], asked: 0 };
        const gate = Deferred.makeUnsafe<void>();
        const extensionWith = (name: string, tools: ReadonlyArray<AgentTool.Any>) => {
          const Decider = deciderTask({ result: null, gate }, seen, `test.${name}`);
          return Extension.make({
            name,
            tools,
            tasks: [Decider],
            hooks: [
              Hook.generation({
                afterTools: () => Effect.succeed({ tasks: [Hook.hookTask(Decider, null)] }),
              }),
            ],
          });
        };
        setup.faux.setResponses([CALL, fauxAssistantMessage([fauxText("done")])]);
        const { harness, root } = yield* open(setup, [
          extensionWith("first", [noop]),
          extensionWith("second", []),
        ]);
        const submission = yield* root.submit({ type: "input", content: "go" });
        const generation = yield* generationIn(harness, root, "afterTools");
        const storage = yield* AgentStorage;
        const kinds: Array<string | undefined> = [];
        const waited = generation.state.status === "waiting" ? generation.state.checkpoint : null;
        for (const id of Schema.decodeUnknownSync(HookWait)(waited).tasks) {
          // SAFETY: a task ID listed in the generation's checkpoint.
          const record = yield* storage.task(id as TaskId);
          kinds.push(Option.getOrUndefined(record)?.kind);
        }
        assert.deepStrictEqual(kinds, ["test.first", "test.second"]);
        yield* Deferred.succeed(gate, undefined);
        assert.strictEqual((yield* settle(submission)).status, "done");
        yield* harness.close;
      }),
    ),
  );

  it.effect("end the run aborted when it is aborted while waiting in afterTools", () =>
    run(backend)(
      Effect.gen(function* () {
        const gate = Deferred.makeUnsafe<void>();
        const { setup, extension } = setupWith("afterTools", { result: null, gate });
        setup.faux.setResponses([CALL]);
        const { harness, root } = yield* open(setup, [extension]);
        const submission = yield* root.submit({ type: "input", content: "go" });
        yield* generationIn(harness, root, "afterTools");
        yield* finished(root.abort());
        const settled = yield* settle(submission);
        assert.deepInclude(settled, { status: "unanswered", reason: "aborted" });
        const state = yield* live(harness, root);
        assert.isUndefined(state?.run);
        assert.strictEqual(state?.runFailure?.reason, "aborted");
        assert.strictEqual(setup.faux.state.callCount, 1);
        yield* harness.close;
      }),
    ),
  );

  it.effect(
    "end the run with restart_limit when afterTools is cut off as often as the limit allows",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const inner = yield* AgentStorage;
          const setup = chatSetup();
          const Decider = parkingDecider(null);
          const extensions = [
            Extension.make({
              name: "deciders",
              tools: [noop],
              tasks: [Decider],
              hooks: [
                Hook.generation({
                  afterTools: () => Effect.succeed({ tasks: [Hook.hookTask(Decider, null)] }),
                }),
              ],
            }),
          ];
          setup.faux.setResponses([CALL]);
          let opened = yield* openKillable(inner, setup, extensions);
          const id = (yield* opened.root.submit({ type: "input", content: "go" })).id;
          const record = yield* generationIn(opened.harness, opened.root, "afterTools");
          yield* opened.harness.close;
          if (record.state.status !== "waiting") return assert.fail("not waiting");
          yield* commitWrites(inner, [
            {
              type: "task",
              value: {
                ...record,
                state: { status: "running", checkpoint: record.state.checkpoint },
                restarts: 5,
              },
            },
          ]);
          opened = yield* openKillable(inner, setup, extensions);
          const submission = Option.getOrThrow(yield* opened.harness.harness.submission(id));
          const settled = yield* settle(submission);
          assert.deepInclude(settled, { status: "unanswered", reason: "restart_limit" });
          assert.strictEqual(
            (yield* live(opened.harness, opened.root))?.runFailure?.reason,
            "restart_limit",
          );
          yield* opened.harness.close;
        }),
      ),
  );
});

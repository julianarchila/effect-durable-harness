// Tests of the scheduler's races. Uninterruptible is not atomic: a fiber can yield between two steps. These tests run
// the Harness's fibers with a scheduler that yields every few operations and start the racing call at every turn, as
// `SessionKernel.test.ts` does.
import { assert, it } from "@effect/vitest";
import * as Cause from "effect/Cause";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Exit from "effect/Exit";
import * as Option from "effect/Option";
import * as Scheduler from "effect/Scheduler";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as TestClock from "effect/testing/TestClock";
import * as AgentTool from "../src/AgentTool.ts";
import * as Doc from "../src/Doc.ts";
import * as Extension from "../src/Extension.ts";
import { LiveDoc, type LiveState } from "../src/Live.ts";
import { CommitRejected } from "../src/SessionError.ts";
import type { ConversationId, TaskId } from "../src/Ids.ts";
import * as Task from "../src/Task.ts";
import { fauxAssistantMessage, fauxText, fauxToolCall } from "../src/testing/FauxModel.ts";
import { Tx } from "../src/Tx.ts";
import { type ChatSetup, chatSetup, openChat, waitFor } from "./ChatSupport.ts";
import { eventually } from "./HarnessSupport.ts";
import { abortedWith, completed, openHarness, type TestHarness } from "./HarnessSupport.ts";
import {
  type ControlledStorage,
  controlledStorage,
  describeEachBackend,
  flush,
  run,
} from "./SessionSupport.ts";
import { serialTimeout } from "./Timeouts.ts";

// Effect's scheduler makes no progress below three operations between yields.
const OPS = [3, 4, 5, 8];
const TURNS = 12;

interface Log {
  readonly events: Array<string>;
  gate: Deferred.Deferred<void>;
}
const log: Log = { events: [], gate: Deferred.makeUnsafe() };

/** Two phases around a gate; its abort handler aborts. */
const Work = Task.make({
  name: "test.kernel.work",
  version: 1,
  input: Schema.Null,
  checkpoint: Schema.Union([
    Schema.Struct({ phase: Schema.Literal("one") }),
    Schema.Struct({ phase: Schema.Literal("two") }),
  ]),
  result: Schema.Null,
  initial: () => ({ phase: "one" }),
  phases: {
    one: (_task, runtime) =>
      Effect.gen(function* () {
        log.events.push("one");
        yield* Deferred.await(log.gate);
        yield* runtime.commit(() =>
          Effect.succeed({ status: "running", checkpoint: { phase: "two" } } as const),
        );
      }),
    two: (_task, runtime) =>
      Effect.gen(function* () {
        log.events.push("two");
        yield* runtime.commit(() => Effect.succeed(completed(null)));
      }),
  },
  abort: (_task, runtime) =>
    Effect.gen(function* () {
      log.events.push("abort");
      yield* runtime.commit(() => Effect.succeed(abortedWith("test")));
    }),
});

/** Holds its owner open until released; its abort handler aborts. */
const Child = Task.make({
  name: "test.kernel.child",
  version: 1,
  input: Schema.Null,
  checkpoint: Schema.Struct({ phase: Schema.Literal("hold") }),
  result: Schema.Null,
  initial: () => ({ phase: "hold" }),
  phases: {
    hold: (_task, runtime) =>
      Effect.gen(function* () {
        log.events.push("child");
        yield* Deferred.await(log.gate);
        yield* runtime.commit(() => Effect.succeed(completed(null)));
      }),
  },
  abort: (_task, runtime) => runtime.commit(() => Effect.succeed(abortedWith("child"))),
});

const Notes = Doc.make({
  checkpointWhen: () => false,
  kind: "test.kernel.notes",
  version: 1,
  scope: "session",
  schema: Schema.Struct({ text: Schema.String }),
  initial: () => ({ text: "" }),
});

/** Runtime calls a handler starts and interrupts after `turns` scheduler turns, then completes. */
const Interrupting = Task.make({
  name: "test.kernel.interrupting",
  version: 1,
  input: Schema.Struct({
    call: Schema.Literals(["watchDoc", "awaitTask"]),
    turns: Schema.Number,
    other: Schema.Number,
  }),
  checkpoint: Schema.Struct({ phase: Schema.Literal("run") }),
  result: Schema.Null,
  initial: () => ({ phase: "run" }),
  phases: {
    run: (task, runtime) =>
      Effect.gen(function* () {
        const outer = yield* Scope.make();
        const call =
          task.input.call === "watchDoc"
            ? Scope.provide(runtime.watchDoc(Notes), outer).pipe(Effect.asVoid)
            : // SAFETY: the input names a task created by the test.
              runtime.awaitTask(task.input.other as TaskId).pipe(Effect.asVoid);
        const fiber = yield* Effect.forkChild(call, { startImmediately: true });
        for (let turn = 0; turn < task.input.turns; turn++) yield* Effect.yieldNow;
        yield* Fiber.interrupt(fiber);
        yield* Scope.close(outer, Exit.void);
        // Still working: what the interrupted call registered must be gone by now.
        log.events.push("working");
        yield* Deferred.await(released);
        yield* runtime.commit(() => Effect.succeed(completed(null)));
      }),
  },
  abort: (_task, runtime) => runtime.commit(() => Effect.succeed(abortedWith("test"))),
});

let released = Deferred.makeUnsafe<void>();

/** Leaves a timer armed when it completes (a phase no longer sleeps in memory). */
let timerFired = false;
const LeavesTimer = Task.make({
  name: "test.kernel.leaves-timer",
  version: 1,
  input: Schema.Null,
  checkpoint: Schema.Struct({ phase: Schema.Literal("run") }),
  result: Schema.Null,
  initial: () => ({ phase: "run" }),
  phases: {
    run: (_task, runtime) =>
      Effect.gen(function* () {
        yield* runtime.setTimer(
          Effect.sync(() => {
            timerFired = true;
          }),
          "60 seconds",
        );
        yield* runtime.commit(() => Effect.succeed(completed(null)));
      }),
  },
  abort: (_task, runtime) => runtime.commit(() => Effect.succeed(abortedWith("test"))),
});

/** Leaves its progress commit to a fiber that commits after `turns` turns, and returns at once. */
const Racing = Task.make({
  name: "test.kernel.racing",
  version: 1,
  input: Schema.Struct({ turns: Schema.Number }),
  checkpoint: Schema.Union([
    Schema.Struct({ phase: Schema.Literal("one") }),
    Schema.Struct({ phase: Schema.Literal("two") }),
  ]),
  result: Schema.Null,
  initial: () => ({ phase: "one" }),
  phases: {
    one: (task, runtime) =>
      Effect.gen(function* () {
        racingCommit = yield* Effect.forkDetach(
          Effect.gen(function* () {
            for (let turn = 0; turn < task.input.turns; turn++) yield* Effect.yieldNow;
            yield* runtime.commit(() =>
              Effect.succeed({ status: "running", checkpoint: { phase: "two" } } as const),
            );
          }),
          { startImmediately: true },
        );
      }),
    two: (_task, runtime) => runtime.commit(() => Effect.succeed(completed(null))),
  },
  abort: (_task, runtime) => runtime.commit(() => Effect.succeed(abortedWith("test"))),
});
let racingCommit: Fiber.Fiber<void, unknown> | undefined;

/**
 * Interrupts an `agent()` wait after `turns` turns, then resolves the agent again in the same phase and records
 * whether the first wait ended interrupted or resolved, and whether both returned the same agent.
 */
const AgentRacing = Task.make({
  name: "test.kernel.agent-racing",
  version: 1,
  input: Schema.Struct({ turns: Schema.Number }),
  checkpoint: Schema.Struct({ phase: Schema.Literal("run") }),
  result: Schema.Null,
  initial: () => ({ phase: "run" }),
  phases: {
    run: (task, runtime) =>
      Effect.gen(function* () {
        const caller = yield* Effect.forkChild(runtime.agent, { startImmediately: true });
        for (let turn = 0; turn < task.input.turns; turn++) yield* Effect.yieldNow;
        yield* Fiber.interrupt(caller);
        const first = yield* Fiber.await(caller);
        const agent = yield* runtime.agent;
        agentOutcomes.push(
          Exit.isSuccess(first)
            ? first.value === agent
              ? "resolved, same"
              : "resolved, different"
            : Cause.hasInterruptsOnly(first.cause)
              ? `interrupted, ${agent.thinkingLevel}`
              : `failed: ${Cause.pretty(first.cause)}`,
        );
        yield* runtime.commit(() => Effect.succeed(completed(null)));
      }),
  },
  abort: (_task, runtime) => runtime.commit(() => Effect.succeed(abortedWith("test"))),
});
const agentOutcomes: Array<string> = [];

/**
 * Creates a conversation it owns and leaves a submit through its invocation-bound handle to a fiber that submits
 * after `turns` turns; then waits for its signal.
 */
const Submitter = Task.make({
  name: "test.kernel.submitter",
  version: 1,
  input: Schema.Struct({ turns: Schema.Number }),
  checkpoint: Schema.Struct({ phase: Schema.Literal("run") }),
  result: Schema.Null,
  initial: () => ({ phase: "run" }),
  phases: {
    run: (task, runtime) =>
      Effect.gen(function* () {
        const child = yield* runtime
          .commit(() =>
            Effect.gen(function* () {
              const tx = yield* Tx;
              submitterChild = (yield* tx.createConversation({
                ownership: { kind: "task", taskId: runtime.taskId },
              })).id;
            }),
          )
          .pipe(Effect.andThen(Effect.sync(() => submitterChild!)));
        const handle = Option.getOrThrow(yield* runtime.conversation(child));
        boundSubmit = yield* Effect.forkDetach(
          Effect.gen(function* () {
            for (let turn = 0; turn < task.input.turns; turn++) yield* Effect.yieldNow;
            yield* handle.submit({ type: "input", content: "late" });
          }),
          { startImmediately: true },
        );
        return yield* Effect.never;
      }),
  },
  abort: (_task, runtime) => runtime.commit(() => Effect.succeed(abortedWith("test"))),
});
let submitterChild: ConversationId | undefined;
let boundSubmit: Fiber.Fiber<void, unknown> | undefined;

const tasks = [Work, Child, Interrupting, LeavesTimer, Racing, AgentRacing, Submitter];

/** A Harness whose fibers yield every `ops` operations. */
const openYielding = (storage: ControlledStorage, ops: number) =>
  openHarness(storage, tasks).pipe(Effect.provideService(Scheduler.MaxOpsBeforeYield, ops));

const createWork = (h: TestHarness) =>
  Effect.gen(function* () {
    const root = yield* h.harness.ensureRoot();
    return yield* root.commit(
      Effect.gen(function* () {
        const tx = yield* Tx;
        return yield* tx.createTask(Work, null, { ownership: { kind: "conversation" } });
      }),
    );
  });

const statusOf = (h: TestHarness, id: TaskId) =>
  h.harness.getTask(id).pipe(Effect.map((record) => Option.getOrThrow(record).state));

const reset = () => {
  log.events.length = 0;
  log.gate = Deferred.makeUnsafe();
};

describeEachBackend("scheduler races", (backend) => {
  it.effect("an abort racing the reservation ends the task aborted, with one abort handler", () =>
    run(backend)(
      Effect.gen(function* () {
        for (const ops of OPS) {
          for (let turns = 0; turns < TURNS; turns++) {
            reset();
            const h = yield* openYielding(yield* controlledStorage, ops);
            const id = yield* createWork(h);
            yield* h.harness.resume;
            const aborting = yield* Effect.forkChild(
              Effect.gen(function* () {
                for (let turn = 0; turn < turns; turn++) yield* Effect.yieldNow;
                return yield* h.harness.abortTask(id);
              }),
              { startImmediately: true },
            );
            const result = yield* Fiber.join(aborting);
            assert.strictEqual(result._tag, "Marked");
            const settled = yield* h.harness.awaitTask(id);
            assert.deepStrictEqual(
              settled.state.outcome,
              { status: "aborted", reason: "test" },
              `ops ${ops}, turns ${turns}`,
            );
            assert.strictEqual(log.events.filter((event) => event === "abort").length, 1);
            // Phase one waits on a gate the test never opens, so the run either never started or was
            // interrupted in phase one: the abort handler decides the outcome either way.
            yield* h.close;
          }
        }
      }),
    ),
  );

  it.effect(
    "a close racing the reservation and the step starts no phase and writes no outcome",
    () =>
      run(backend)(
        Effect.gen(function* () {
          for (const ops of OPS) {
            for (let turns = 0; turns < TURNS; turns++) {
              reset();
              const storage = yield* controlledStorage;
              const h = yield* openYielding(storage, ops);
              const id = yield* createWork(h);
              // Phase one passes its gate at once, so the race also covers the step before phase two.
              yield* Deferred.succeed(log.gate, undefined);
              yield* h.harness.resume;
              const closing = yield* Effect.forkChild(
                Effect.gen(function* () {
                  for (let turn = 0; turn < turns; turn++) yield* Effect.yieldNow;
                  yield* h.close;
                }),
                { startImmediately: true },
              );
              yield* Fiber.join(closing);
              const events = log.events.length;
              const commits = storage.commits.length;
              yield* flush;
              // No invocation code runs once close resolved, and nothing commits after it.
              assert.strictEqual(log.events.length, events, `ops ${ops}, turns ${turns}`);
              assert.strictEqual(storage.commits.length, commits, `ops ${ops}, turns ${turns}`);
              // Close wrote no fault: the task is live, or finished by its own phase two.
              const reopened = yield* openHarness(storage, tasks);
              const state = yield* statusOf(reopened, id);
              if (state.status === "terminal") {
                assert.deepStrictEqual(state.outcome, { status: "completed", result: null });
              }
              yield* reopened.harness.resume;
              const settled = yield* reopened.harness.awaitTask(id);
              assert.deepStrictEqual(settled.state.outcome, { status: "completed", result: null });
              yield* reopened.close;
            }
          }
        }),
      ),
  );

  it.effect("work created below an owner racing its abort mark is aborted or refused", () =>
    run(backend)(
      Effect.gen(function* () {
        for (const ops of OPS) {
          for (let turns = 0; turns < TURNS; turns++) {
            reset();
            const h = yield* openYielding(yield* controlledStorage, ops);
            const owner = yield* createWork(h);
            yield* h.harness.resume;
            const creating = yield* Effect.forkChild(
              Effect.gen(function* () {
                for (let turn = 0; turn < turns; turn++) yield* Effect.yieldNow;
                return yield* h.harness.commit(
                  Effect.gen(function* () {
                    const tx = yield* Tx;
                    return yield* tx.createTask(Child, null, {
                      ownership: { kind: "task", taskId: owner },
                    });
                  }),
                );
              }).pipe(Effect.exit),
              { startImmediately: true },
            );
            yield* h.harness.abortTask(owner);
            const created = yield* Fiber.join(creating);
            const settled = yield* h.harness.awaitTask(owner);
            assert.deepStrictEqual(settled.state.outcome, { status: "aborted", reason: "test" });
            if (created._tag === "Success") {
              // Created before the mark: the cascade aborted it, and its owner waited for it.
              const child = yield* h.harness.awaitTask(created.value);
              assert.strictEqual(
                child.state.outcome.status,
                "aborted",
                `ops ${ops}, turns ${turns}`,
              );
              assert.isTrue((child.endedAt ?? 0) <= (settled.endedAt ?? 0));
            } else {
              // Created after the mark: the commit is refused because its owner is abort-marked.
              const error = Cause.squash(created.cause);
              assert.isTrue(
                Schema.is(CommitRejected)(error) && error.reason._tag === "InvalidOwner",
                `ops ${ops}, turns ${turns}`,
              );
            }
            yield* h.close;
          }
        }
      }),
    ),
  );

  for (const call of ["watchDoc", "awaitTask"] as const) {
    it.effect(`a ${call} interrupted at any point leaves nothing registered behind`, () =>
      run(backend)(
        Effect.gen(function* () {
          for (const ops of [3, 5]) {
            reset();
            const h = yield* openYielding(yield* controlledStorage, ops);
            yield* h.harness.commit(
              Effect.gen(function* () {
                const tx = yield* Tx;
                yield* tx.doc(Notes);
              }),
            );
            // A task that stays live for the whole sweep, for `awaitTask` to wait on.
            const other = yield* createWork(h);
            yield* h.harness.resume;
            yield* Deferred.succeed(log.gate, undefined);
            yield* h.harness.awaitTask(other);
            log.gate = Deferred.makeUnsafe<void>();
            const held = yield* createWork(h);
            for (let turns = 0; turns < 60; turns++) {
              log.events.length = 0;
              const root = yield* h.harness.ensureRoot();
              const id = yield* root.commit(
                Effect.gen(function* () {
                  const tx = yield* Tx;
                  return yield* tx.createTask(
                    Interrupting,
                    { call, turns, other: held },
                    { ownership: { kind: "conversation" } },
                  );
                }),
              );
              yield* eventually(Effect.sync(() => log.events.includes("working")));
              const detail = `${call}, ops ${ops}, turns ${turns}`;
              // The interrupted call left no pin, deadline or waiter: the invocation is plain work.
              assert.strictEqual(h.kernel.documentCacheSizes.attached, 0, detail);
              const wake = h.scheduler.wake();
              assert.isTrue(wake.activeWork, detail);
              assert.isTrue(Option.isNone(wake.nextWakeAt), detail);
              assert.strictEqual(h.scheduler.waiterCount(), 0, detail);
              const release = released;
              released = Deferred.makeUnsafe<void>();
              yield* Deferred.succeed(release, undefined);
              yield* h.harness.awaitTask(id);
            }
            yield* Deferred.succeed(log.gate, undefined);
            yield* h.close;
          }
        }),
      ),
    );
  }

  // The phase's agent is a memo created in the step that first asks for it; a caller interrupted at any point
  // cancels only its own wait.
  it.effect("an agent() wait interrupted at any point leaves the phase's resolution intact", () =>
    run(backend)(
      Effect.gen(function* () {
        for (const ops of OPS) {
          const h = yield* openYielding(yield* controlledStorage, ops);
          const root = yield* h.harness.ensureRoot();
          yield* h.harness.resume;
          agentOutcomes.length = 0;
          for (let turns = 0; turns < 30; turns++) {
            const id = yield* root.commit(
              Effect.gen(function* () {
                const tx = yield* Tx;
                return yield* tx.createTask(
                  AgentRacing,
                  { turns },
                  { ownership: { kind: "conversation" } },
                );
              }),
            );
            const settled = yield* h.harness.awaitTask(id);
            assert.deepStrictEqual(
              settled.state.outcome,
              { status: "completed", result: null },
              `ops ${ops}, turns ${turns}`,
            );
          }
          // Every second call resolved the phase's agent; the sweep reaches both sides of the race.
          assert.deepStrictEqual(
            [...new Set(agentOutcomes)].toSorted(),
            ["interrupted, off", "resolved, same"],
            `ops ${ops}`,
          );
          yield* h.close;
        }
      }),
    ),
  );

  // A run invocation's signal is aborted in the commit listener that sees its abort mark, and a handle operation
  // checks that signal first on the line: a submit through a bound handle is admitted before the mark or refused,
  // never admitted after it.
  it.effect(
    "a bound handle submit racing the abort mark is admitted before it or refused",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const outcomes = new Set<string>();
          for (const ops of OPS) {
            // Up to 90 turns: since group commit the mark reaches the invocation at its publication, off the line,
            // a few dozen turns after the abort's commit was admitted.
            for (let turns = 0; turns < 90; turns += 3) {
              submitterChild = undefined;
              boundSubmit = undefined;
              const storage = yield* controlledStorage;
              const h = yield* openYielding(storage, ops);
              const root = yield* h.harness.ensureRoot();
              const id = yield* root.commit(
                Effect.gen(function* () {
                  const tx = yield* Tx;
                  return yield* tx.createTask(
                    Submitter,
                    { turns },
                    { ownership: { kind: "conversation" } },
                  );
                }),
              );
              yield* h.harness.resume;
              yield* eventually(Effect.sync(() => boundSubmit !== undefined));
              yield* h.harness.abortTask(id);
              yield* h.harness.awaitTask(id);
              const submitted = yield* Fiber.await(boundSubmit!);
              const detail = `ops ${ops}, turns ${turns}`;
              const commitOf = (
                found: (write: (typeof storage.commits)[number][number]) => boolean,
              ) => storage.commits.findIndex((writes) => writes.some(found));
              const admitted = commitOf(
                (write) =>
                  write.type === "submission" && write.value.conversationId === submitterChild,
              );
              const marked = commitOf(
                (write) =>
                  write.type === "task" && write.value.id === id && write.value.abortRequested,
              );
              assert.isAbove(marked, -1, detail);
              if (Exit.isSuccess(submitted)) {
                // Durable when its caller has it. With group commit, the check runs when the job starts on the line,
                // which can be after the mark was admitted and before it was published, so the admission may be stored
                // after the mark.
                assert.isAbove(admitted, -1, detail);
                outcomes.add("admitted");
              } else {
                assert.strictEqual(admitted, -1, detail);
                outcomes.add("refused");
              }
              yield* h.close;
            }
          }
          // The sweep reaches both sides of the race.
          assert.deepStrictEqual([...outcomes].toSorted(), ["admitted", "refused"]);
        }),
      ),
    // A sweep of 4 scheduler settings by 30 delays, each with a Harness of its own.
    serialTimeout(2_461),
  );

  // A commit's admission loads the ownership chain above the work it creates (`admitting`). Its caller
  // interrupted at any step leaves the line free and the Session healthy, and work it committed runs.
  // The reservation is the decision its region checked. A pass whose probe ran clean
  // before a publication made a task runnable must not reserve it from a record a later unpublished part changes.
  it.effect("a reservation keeps the abort mark a commit admitted before it writes", () =>
    run(backend)(
      Effect.gen(function* () {
        for (const ops of OPS) {
          for (let lead = 0; lead < 2 * TURNS; lead++) {
            reset();
            const storage = yield* controlledStorage;
            const h = yield* openYielding(storage, ops);
            const root = yield* h.harness.ensureRoot();
            // A waiter that waits for a child: runnable only once the child's end is published.
            const { child, waiter } = yield* root.commit(
              Effect.gen(function* () {
                const tx = yield* Tx;
                const child = yield* tx.createTask(Child, null, {
                  ownership: { kind: "conversation" },
                });
                const waiter = yield* tx.createTask(Work, null, {
                  ownership: { kind: "conversation" },
                });
                return { child, waiter };
              }),
            );
            yield* h.kernel.commitWith((tx) =>
              Effect.gen(function* () {
                const record = Option.getOrThrow(yield* tx.service.task(waiter));
                yield* tx.setTask({
                  ...record,
                  state: {
                    status: "waiting",
                    checkpoint: { phase: "one" },
                    on: [child],
                    policy: "allSettled",
                  },
                });
              }),
            );
            yield* h.harness.resume;
            yield* eventually(Effect.sync(() => log.events.includes("child")));
            // One commit in storage, so the child's end and the waiter's abort mark go in the next call.
            // The child's end is stored alone, in a call held in storage; the abort mark waits for the next one.
            const first = yield* storage.holdCommits;
            yield* Deferred.succeed(log.gate, undefined);
            yield* first.entered;
            const second = yield* storage.holdCommits;
            const aborting = yield* Effect.forkChild(h.harness.abortTask(waiter), {
              startImmediately: true,
            });
            yield* eventually(Effect.sync(() => h.kernel.lineSizes.unpublished > 1));
            // A pass starts behind the mark, before the waiter is runnable; the child's end is published `lead`
            // turns later, anywhere in the pass.
            yield* h.harness.resume;
            for (let turn = 0; turn < lead; turn++) yield* Effect.yieldNow;
            yield* first.release;
            yield* second.entered;
            yield* second.release;
            assert.strictEqual((yield* Fiber.join(aborting))._tag, "Marked");
            const settled = yield* h.harness.awaitTask(waiter);
            assert.deepStrictEqual(
              settled.state.outcome,
              { status: "aborted", reason: "test" },
              `ops ${ops}, lead ${lead}`,
            );
            yield* h.close;
          }
        }
      }),
    ),
  );

  it.effect(
    "a creation below a forgotten chain interrupted during its admission commits whole or not at all",
    () =>
      run(backend)(
        Effect.gen(function* () {
          for (const ops of OPS) {
            for (let turns = 0; turns < TURNS; turns++) {
              reset();
              yield* Deferred.succeed(log.gate, undefined);
              const h = yield* openYielding(yield* controlledStorage, ops);
              const root = yield* h.harness.ensureRoot();
              const tree = yield* root.commit(
                Effect.gen(function* () {
                  const tx = yield* Tx;
                  const p = yield* tx.createTask(Work, null, {
                    ownership: { kind: "conversation" },
                  });
                  const c = yield* tx.createConversation({
                    ownership: { kind: "task", taskId: p },
                  });
                  return { p, c: c.id };
                }),
              );
              yield* h.harness.resume;
              // P ends and the sweep forgets C's chain.
              yield* h.harness.awaitTask(tree.p);
              const c = Option.getOrThrow(yield* h.harness.conversation(tree.c));
              const creating = yield* Effect.forkChild(
                c.commit(
                  Effect.gen(function* () {
                    const tx = yield* Tx;
                    return yield* tx.createTask(Work, null, {
                      ownership: { kind: "conversation" },
                    });
                  }),
                ),
                { startImmediately: true },
              );
              for (let turn = 0; turn < turns; turn++) yield* Effect.yieldNow;
              yield* Fiber.interrupt(creating);
              const created = yield* Fiber.await(creating);
              // The line is free and the Session healthy.
              yield* createWork(h);
              assert.isTrue(Option.isNone(h.kernel.failure), `ops ${ops}, turns ${turns}`);
              if (Exit.isSuccess(created)) {
                assert.strictEqual(
                  (yield* h.harness.awaitTask(created.value)).state.outcome.status,
                  "completed",
                );
              }
              yield* h.harness.awaitIdle;
              yield* h.close;
            }
          }
        }),
      ),
  );

  it.effect("a timer the handler left armed ends with its invocation", () =>
    run(backend)(
      Effect.gen(function* () {
        const h = yield* openHarness(yield* controlledStorage, tasks);
        const root = yield* h.harness.ensureRoot();
        const id = yield* root.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            return yield* tx.createTask(LeavesTimer, null, { ownership: { kind: "conversation" } });
          }),
        );
        yield* h.harness.resume;
        yield* h.harness.awaitTask(id);
        yield* flush;
        // The invocation's end disarms its timers.
        yield* TestClock.adjust("1 minute");
        yield* flush;
        assert.isFalse(timerFired);
        yield* h.close;
      }),
    ),
  );

  it.effect("a runtime commit racing its step either counts or is refused", () =>
    run(backend)(
      Effect.gen(function* () {
        for (const ops of OPS) {
          const outcomes = new Set<string>();
          for (let turns = 0; turns < 40; turns++) {
            const h = yield* openYielding(yield* controlledStorage, ops);
            const root = yield* h.harness.ensureRoot();
            const id = yield* root.commit(
              Effect.gen(function* () {
                const tx = yield* Tx;
                return yield* tx.createTask(
                  Racing,
                  { turns },
                  { ownership: { kind: "conversation" } },
                );
              }),
            );
            yield* h.harness.resume;
            const settled = yield* h.harness.awaitTask(id);
            const committed = yield* Fiber.await(racingCommit!);
            const detail = `ops ${ops}, turns ${turns}`;
            // A commit queued before the step counts as progress; one that reaches the
            // line after it is refused, and the phase made no progress.
            if (Exit.isSuccess(committed)) {
              assert.deepStrictEqual(
                settled.state.outcome,
                { status: "completed", result: null },
                detail,
              );
            } else {
              assert.strictEqual(settled.state.outcome.status, "faulted", detail);
            }
            outcomes.add(settled.state.outcome.status);
            yield* h.close;
          }
          // The sweep reaches both sides of the race.
          assert.deepStrictEqual([...outcomes].toSorted(), ["completed", "faulted"], `ops ${ops}`);
        }
      }),
    ),
  );
});

// A handler-local throttle is stopped, its commit in flight joined, before the handler's terminal commit. These
// sweeps abort a generation and a tool at every point of their throttled output, under a scheduler that yields
// every few operations, and check that no throttled commit is published after the task's terminal commit.
// After an abort mark the kernel itself refuses the task's commits, so the abort sweeps check the outcome, not the
// throttle's stop. The retry sweep does: the generation task keeps committing after a retried attempt, and a
// throttle left running with its pending partial lands it after the classification. The units of `Progress`
// (`HarnessOutput.test.ts`) check the tool side's stop.

/** One publication: whether it ended the observed task, and the `core.live` value it published. */
interface Published {
  readonly terminal: boolean;
  readonly live: LiveState | undefined;
}

/**
 * Every publication from now on, in order; `taskId()` names the task whose terminal commit is marked. The
 * Session's own observers, which see deferred progress when memory adopts it, not when it is stored.
 */
const observe = (h: TestHarness, taskId: () => TaskId | undefined) =>
  Effect.gen(function* () {
    const lives: Array<Published> = [];
    yield* h.kernel.followCommits((publication) => {
      let terminal = false;
      let live: LiveState | undefined;
      for (const change of publication.changes) {
        if (
          change.type === "task" &&
          change.value.id === taskId() &&
          change.value.state.status === "terminal"
        ) {
          terminal = true;
        }
        if (
          change.type === "document" &&
          change.record.kind === "core.live" &&
          change.value !== null
        ) {
          // SAFETY: a published `core.live` value, which its schema checked when it was written.
          live = change.value as LiveState;
        }
      }
      lives.push({ terminal, live });
    });
    return lives;
  });

/** Lets `effect` finish while the `TestClock` advances, then runs out every timer left. */
const settleWithClock = <A, E>(effect: Effect.Effect<A, E>) =>
  Effect.gen(function* () {
    const fiber = yield* Effect.forkChild(effect, { startImmediately: true });
    yield* waitFor(Effect.sync(() => fiber.pollUnsafe() !== undefined));
    const result = yield* Fiber.join(fiber);
    yield* TestClock.adjust("1 second");
    yield* flush;
    return result;
  });

/** Advances the `TestClock` by `millis` 1 ms steps, letting work run after each, then yields `turns` times. */
const advance = (millis: number, turns: number) =>
  Effect.gen(function* () {
    for (let step = 0; step < millis; step++) {
      yield* TestClock.adjust("1 millis");
      yield* flush;
    }
    for (let turn = 0; turn < turns; turn++) yield* Effect.yieldNow;
  });

/** `openChat` with Harness fibers that yield every `ops` operations. */
const openYieldingChat = (
  storage: ControlledStorage,
  setup: ChatSetup,
  ops: number,
  extensions: ReadonlyArray<Extension.Any> = [],
) =>
  openChat(storage, setup, { extensions }).pipe(
    Effect.provideService(Scheduler.MaxOpsBeforeYield, ops),
  );

/** The running output a publication shows in the slot of call `c1`. */
const slotOutput = (published: Published) =>
  published.live?.tools?.find((slot) => slot.callId === "c1")?.output;

describeEachBackend("throttles racing an abort", (backend) => {
  it.effect(
    "an abort at any point of a request stream lands no partial after the outcome",
    () =>
      Effect.gen(function* () {
        const reached = new Set<string>();
        for (const ops of [3, 5]) {
          for (let millis = 0; millis < 36; millis++) {
            for (const turns of [0, 3]) {
              yield* run(backend)(
                Effect.gen(function* () {
                  // One token a millisecond and a partial commit at most every 3 ms: the stream outlasts the sweep.
                  const setup = chatSetup({ tokensPerSecond: 1000, tokenSize: { min: 1, max: 1 } });
                  setup.settings.progress = { partialIntervalMs: 3 };
                  setup.faux.setResponses([fauxAssistantMessage("w".repeat(60))]);
                  const { harness: h, root } = yield* openYieldingChat(
                    yield* controlledStorage,
                    setup,
                    ops,
                  );
                  let taskId: TaskId | undefined;
                  const lives = yield* observe(h, () => taskId);
                  yield* h.harness.resume;
                  yield* root.submit({ type: "input", content: "hi" });
                  taskId = Option.getOrThrow(yield* h.session.snapshot(LiveDoc, root.id)).run
                    ?.taskId;
                  assert.isDefined(taskId);
                  yield* advance(millis, turns);
                  yield* h.harness.abortTask(taskId!);
                  yield* settleWithClock(h.harness.awaitTask(taskId!));
                  const detail = `ops ${ops}, ${millis} ms, turns ${turns}`;
                  const terminal = lives.findIndex((published) => published.terminal);
                  assert.isAbove(terminal, -1, detail);
                  // The outcome commit clears the partial; nothing writes one back afterwards.
                  for (const published of lives.slice(terminal)) {
                    assert.isUndefined(published.live?.generation, detail);
                  }
                  const partials = lives
                    .slice(0, terminal)
                    .filter(
                      (published) => published.live?.generation?.message !== undefined,
                    ).length;
                  reached.add(partials > 0 ? "after partials" : "before any partial");
                  yield* h.close;
                }),
              );
            }
          }
        }
        // The sweep reaches both sides: aborts before the first partial and aborts while partials commit.
        assert.deepStrictEqual([...reached].toSorted(), ["after partials", "before any partial"]);
      }),
    // An exhaustive sweep, like the one below.
    serialTimeout(3_694),
  );

  it.effect(
    "a retried attempt lands no partial of its own after its classification",
    () =>
      Effect.gen(function* () {
        const reached = new Set<string>();
        for (const ops of [3, 4, 5]) {
          for (let length = 1; length <= 12; length++) {
            yield* run(backend)(
              Effect.gen(function* () {
                // The first attempt streams `length` tokens, one a millisecond, and ends with a retryable error;
                // the generation task keeps committing, so only the throttle's own stop keeps a stale partial out.
                const setup = chatSetup({ tokensPerSecond: 1000, tokenSize: { min: 1, max: 1 } });
                setup.settings.progress = { partialIntervalMs: 3 };
                setup.settings.retry = { enabled: true, maxRetries: 1, baseDelayMs: 1 };
                setup.faux.setResponses([
                  fauxAssistantMessage("w".repeat(length), {
                    stopReason: "error",
                    errorMessage: "503 Service Unavailable",
                  }),
                  fauxAssistantMessage("done"),
                ]);
                const { harness: h, root } = yield* openYieldingChat(
                  yield* controlledStorage,
                  setup,
                  ops,
                );
                let taskId: TaskId | undefined;
                const lives = yield* observe(h, () => taskId);
                yield* h.harness.resume;
                const submission = yield* root.submit({ type: "input", content: "hi" });
                taskId = Option.getOrThrow(yield* h.session.snapshot(LiveDoc, root.id)).run?.taskId;
                assert.strictEqual((yield* settleWithClock(submission.await)).status, "done");
                const detail = `ops ${ops}, length ${length}`;
                // The classification of the first attempt clears its partial and records the retry.
                const retried = lives.findIndex(
                  (published) => published.live?.generation?.retry !== undefined,
                );
                assert.isAbove(retried, -1, detail);
                for (const published of lives.slice(retried)) {
                  const generation = published.live?.generation;
                  assert.isFalse(
                    generation?.attempt === 1 && generation.message !== undefined,
                    detail,
                  );
                }
                const partials = lives
                  .slice(0, retried)
                  .filter((published) => published.live?.generation?.message !== undefined).length;
                reached.add(partials > 0 ? "after partials" : "before any partial");
                yield* h.close;
              }),
            );
          }
        }
        assert.include([...reached], "after partials");
      }),
    serialTimeout(1_563),
  );

  it.effect(
    "a tool aborted at any point of its throttled output lands no progress after its result",
    () =>
      Effect.gen(function* () {
        const reached = new Set<string>();
        const looper = AgentTool.make("looper", {
          description: "Writes a line every millisecond until it is aborted",
          parameters: Schema.Struct({}),
          execute: () =>
            Effect.gen(function* () {
              const api = yield* AgentTool.ToolContext;
              for (let line = 0; ; line++) {
                yield* api.output(`${line}\n`);
                yield* Effect.sleep("1 millis");
              }
            }),
        });
        for (const ops of [3, 5]) {
          for (let millis = 0; millis < 24; millis++) {
            for (const turns of [0, 3]) {
              yield* run(backend)(
                Effect.gen(function* () {
                  const setup = chatSetup();
                  setup.settings.progress = { outputIntervalMs: 3 };
                  setup.faux.setResponses([
                    fauxAssistantMessage([fauxToolCall("looper", {}, { id: "c1" })], {
                      stopReason: "toolUse",
                    }),
                    fauxAssistantMessage([fauxText("done")]),
                  ]);
                  const { harness: h, root } = yield* openYieldingChat(
                    yield* controlledStorage,
                    setup,
                    ops,
                    [Extension.make({ name: "tool:looper", tools: [looper] })],
                  );
                  let taskId: TaskId | undefined;
                  const lives = yield* observe(h, () => taskId);
                  yield* h.harness.resume;
                  const submission = yield* root.submit({ type: "input", content: "hi" });
                  // The tool task starts without the clock: the first request needs no time.
                  for (let step = 0; step < 100 && taskId === undefined; step++) {
                    yield* flush;
                    const live = Option.getOrUndefined(yield* h.session.snapshot(LiveDoc, root.id));
                    taskId = live?.tools?.[0]?.taskId;
                  }
                  assert.isDefined(taskId);
                  yield* advance(millis, turns);
                  yield* h.harness.abortTask(taskId!);
                  yield* settleWithClock(submission.await);
                  const detail = `ops ${ops}, ${millis} ms, turns ${turns}`;
                  const terminal = lives.findIndex((published) => published.terminal);
                  assert.isAbove(terminal, -1, detail);
                  // The terminal commit clears the slot's running output; no progress commit writes it back.
                  for (const published of lives.slice(terminal)) {
                    assert.isUndefined(slotOutput(published), detail);
                  }
                  const progress = lives
                    .slice(0, terminal)
                    .filter((published) => slotOutput(published) !== undefined);
                  reached.add(progress.length > 0 ? "after progress" : "before any progress");
                  yield* h.close;
                }),
              );
            }
          }
        }
        // The sweep reaches aborts while progress commits; the first flush starts in the tool's first step.
        assert.include([...reached], "after progress");
      }),
    // An exhaustive sweep: about 5 s alone on sqlite, more under the full suite's load.
    serialTimeout(5_208),
  );
});

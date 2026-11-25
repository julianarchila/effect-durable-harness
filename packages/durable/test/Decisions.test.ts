import { assert, describe, it } from "@effect/vitest";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { ConversationId, TaskId } from "../src/Ids.ts";
import type { TaskRecord, TaskState } from "../src/Records.ts";
import * as Task from "../src/Task.ts";
import * as Decisions from "../src/internal/decisions.ts";
import * as Ownership from "../src/internal/ownership.ts";

// Tests of the scheduler's decisions: each decision is a pure function of the committed state, so these check
// "given this state, this plan" without a Harness or fibers. The Harness suites cover the same rules through the
// scheduler.

const conversation = ConversationId.make(1);
const pending: TaskState = { status: "pending", checkpoint: null };
const running: TaskState = { status: "running", checkpoint: { phase: "run" } };
const noOverlay = (): Ownership.Overlay => ({ tasks: new Map(), edges: new Map() });

interface TaskOptions {
  readonly kind?: string | undefined;
  readonly version?: number | undefined;
  readonly owner?: number | undefined;
  readonly background?: boolean | undefined;
  readonly abortRequested?: boolean | undefined;
  readonly abortReason?: "restart" | undefined;
  readonly state?: TaskState | undefined;
  readonly restarts?: number | undefined;
}

const task = (id: number, options: TaskOptions = {}): TaskRecord => {
  const base = {
    id: TaskId.make(id),
    conversationId: conversation,
    kind: options.kind ?? "test",
    version: options.version ?? 1,
    input: null,
    background: options.background ?? false,
    abortRequested: options.abortRequested ?? false,
  };
  const owned = options.owner === undefined ? base : { ...base, owner: TaskId.make(options.owner) };
  const reasoned =
    options.abortReason === undefined ? owned : { ...owned, abortReason: options.abortReason };
  const counted =
    options.restarts === undefined ? reasoned : { ...reasoned, restarts: options.restarts };
  // SAFETY: a record with one of the task states; the decisions read its fields, not its schema.
  return { ...counted, state: options.state ?? pending } as TaskRecord;
};

const sleeping = (until: number): TaskState => ({
  status: "sleeping",
  checkpoint: { phase: "run" },
  until,
});

const completing = (status: "completed" | "faulted"): TaskState =>
  status === "completed"
    ? { status: "completing", outcome: { status: "completed", result: null } }
    : { status: "completing", outcome: { status: "faulted", error: { message: "boom" } } };

const Probe = Task.make({
  name: "test",
  version: 2,
  input: Schema.Null,
  checkpoint: Schema.Struct({ phase: Schema.Literal("run") }),
  result: Schema.Null,
  initial: () => ({ phase: "run" }),
  phases: { run: () => Effect.void },
  abort: () => Effect.void,
  migrate: (_input, _checkpoint, from) => {
    if (from === 0) throw new Error("cannot migrate");
    return { input: null, checkpoint: { phase: "run" } as const };
  },
});

const definitions = (): Decisions.Definitions => ({
  tasks: new Map([["test", Probe]]),
  failedMigrations: new Map(),
});

const indexWith = (records: ReadonlyArray<TaskRecord>) => {
  const index = Ownership.make({ onDropQueued: () => {} });
  index.setEdge(conversation, null);
  for (const record of records) index.track(undefined, record);
  return index;
};

/** A pass over `candidates` at time 1000 with the default restart limit, where only the built-in kind limits itself. */
const inputs = (
  candidates: ReadonlyArray<TaskId>,
  invoked: (id: TaskId) => boolean = () => false,
  restartLimit = 5,
): Decisions.ReserveInputs => ({
  invoked,
  candidates,
  now: 1_000,
  restartLimit: () => restartLimit,
  limitsRestarts: (kind) => kind === "core.tool",
});

const writes = (actions: ReadonlyArray<Decisions.Action>) =>
  actions.flatMap((action) =>
    action._tag === "Write" ? [`${action.record.id}:${action.record.state.status}`] : [],
  );

const settles = (actions: ReadonlyArray<Decisions.Action>) =>
  actions.flatMap((action) => (action._tag === "Settle" ? [action.record.id] : []));

const sleep = (until: number) =>
  ({ status: "sleeping", checkpoint: { phase: "next" }, until }) as const;

// SAFETY: a running record, as built.
const runningRecord = (record: TaskRecord) => record as Decisions.RunningRecord;

describe("restarts and sleeps", () => {
  it("starts the count over on a new state, but not on the same checkpoint committed again", () => {
    const current = task(1, { state: running, restarts: 2 });
    assert.strictEqual(Decisions.committedState(current, running).restarts, 2);
    const next: TaskState = { status: "running", checkpoint: { phase: "run", step: 2 } };
    assert.isUndefined(Decisions.committedState(current, next).restarts);
    const asleep: TaskState = { status: "sleeping", checkpoint: { phase: "run" }, until: 5 };
    assert.isUndefined(Decisions.committedState(current, asleep).restarts);
  });

  it("drops the count with an abort mark and an outcome", () => {
    const current = task(1, { state: running, restarts: 2 });
    assert.isUndefined(Decisions.withAbortMark(current).restarts);
    assert.isUndefined(
      Decisions.withState(current, { status: "terminal", outcome: { status: "aborted" } }).restarts,
    );
  });

  it("refuses a sleep until something that is not a time, and a sleep in an abort handler", () => {
    const current = task(7, { state: running });
    assert.isUndefined(Decisions.invalidSleep("run", current, sleep(0)));
    assert.strictEqual(
      Decisions.invalidSleep("run", current, sleep(Number.NaN)),
      "Task 7 cannot sleep until NaN: not a time in milliseconds",
    );
    assert.strictEqual(
      Decisions.invalidSleep("run", current, sleep(-1)),
      "Task 7 cannot sleep until -1: not a time in milliseconds",
    );
    assert.strictEqual(
      Decisions.invalidSleep("run", current, sleep(Number.POSITIVE_INFINITY)),
      "Task 7 cannot sleep until Infinity: not a time in milliseconds",
    );
    assert.strictEqual(
      Decisions.invalidSleep("abort", current, sleep(10)),
      "Abort handler of task 7 cannot sleep",
    );
  });
});

describe("step decisions (the precedence rules)", () => {
  it("continues into the first phase, and ends an abort-marked task", () => {
    const index = indexWith([task(1, { state: running })]);
    const current = runningRecord(task(1, { state: running }));
    assert.deepStrictEqual(Decisions.decideStep(index, current, undefined), Decisions.Continue);
    const marked = runningRecord(task(1, { state: running, abortRequested: true }));
    assert.deepStrictEqual(Decisions.decideStep(index, marked, undefined), Decisions.End);
  });

  it("ends work below an owner with cancellation intent and asks for its cascade", () => {
    const index = indexWith([
      task(1, { abortRequested: true }),
      task(2, { owner: 1, state: running }),
    ]);
    const current = runningRecord(task(2, { owner: 1, state: running }));
    assert.deepStrictEqual(Decisions.decideStep(index, current, undefined), {
      _tag: "End",
      cascade: true,
    });
  });

  it("faults an uncaught failure and a phase without durable progress", () => {
    const index = indexWith([task(1, { state: running })]);
    const current = runningRecord(task(1, { state: running }));
    const failure = Cause.die(new Error("boom"));
    const failed = Decisions.decideStep(index, current, { checkpoint: { phase: "run" }, failure });
    // The failure itself goes with the fault, to its `task_faulted` report.
    assert.deepStrictEqual(failed, { _tag: "Fault", message: "boom", cause: failure });
    const stuck = Decisions.decideStep(index, current, { checkpoint: { phase: "run" } });
    assert.strictEqual(stuck._tag, "Fault");
    const progressed = Decisions.decideStep(index, current, { checkpoint: { phase: "before" } });
    assert.deepStrictEqual(progressed, Decisions.Continue);
  });
});

describe("reservation", () => {
  it("starts a ready task, migrating it, and drops waiting and invoked ones", () => {
    const records = [
      task(1),
      task(2, { state: running }),
      task(3, {
        state: { status: "waiting", checkpoint: null, on: [TaskId.make(1)], policy: "allSettled" },
      }),
    ];
    const index = indexWith(records);
    const plan = Decisions.planReserve(
      index,
      definitions(),
      inputs([TaskId.make(1), TaskId.make(2), TaskId.make(3)], (id) => id === TaskId.make(2)),
      noOverlay(),
    );
    assert.deepStrictEqual(
      plan.start.map((start) => [start.record.id, start.mode]),
      [[TaskId.make(1), "run"]],
    );
    assert.deepStrictEqual(writes(plan.actions), ["1:running"]);
    assert.deepStrictEqual(plan.dropped, [TaskId.make(1), TaskId.make(2), TaskId.make(3)]);
    assert.isFalse(plan.cascade);
  });

  it("orphans an abort-marked task no definition can take, but waits for an abandoned one", () => {
    const index = indexWith([
      task(1, { kind: "gone", abortRequested: true }),
      task(2, { kind: "gone", abortRequested: true, abortReason: "restart" }),
      task(3, { kind: "gone" }),
    ]);
    const plan = Decisions.planReserve(
      index,
      definitions(),
      inputs([TaskId.make(1), TaskId.make(2), TaskId.make(3)]),
      noOverlay(),
    );
    assert.deepStrictEqual(writes(plan.actions), ["1:terminal"]);
    assert.deepStrictEqual(settles(plan.actions), [TaskId.make(1)]);
    assert.deepStrictEqual(plan.start, []);
    // Blocked tasks stay candidates for when a definition appears.
    assert.deepStrictEqual(plan.dropped, []);
  });

  it("holds work below a cancelling owner for its cascade", () => {
    const index = indexWith([task(1, { abortRequested: true }), task(2, { owner: 1 })]);
    const plan = Decisions.planReserve(index, definitions(), inputs([TaskId.make(2)]), noOverlay());
    assert.isTrue(plan.cascade);
    assert.deepStrictEqual(plan.start, []);
  });

  it("drops a sleep not yet due, and reserves a due one or one an abort mark ends", () => {
    const index = indexWith([
      task(1, { version: 2, state: sleeping(2_000) }),
      task(2, { version: 2, state: sleeping(1_000) }),
      task(3, { version: 2, state: sleeping(2_000), abortRequested: true }),
    ]);
    const plan = Decisions.planReserve(
      index,
      definitions(),
      inputs([TaskId.make(1), TaskId.make(2), TaskId.make(3)]),
      noOverlay(),
    );
    assert.deepStrictEqual(
      plan.start.map((start) => [start.record.id, start.mode]),
      [
        [TaskId.make(2), "run"],
        [TaskId.make(3), "abort"],
      ],
    );
    assert.deepStrictEqual(writes(plan.actions), ["2:running", "3:running"]);
    assert.deepStrictEqual(plan.dropped, [TaskId.make(1), TaskId.make(2), TaskId.make(3)]);
  });

  it("faults an extension's task and an abort handler at the restart limit, not a built-in run", () => {
    const index = indexWith([
      task(1, { version: 2, restarts: 5 }),
      task(2, { version: 2, restarts: 4 }),
      task(3, { kind: "core.tool", restarts: 5 }),
      task(4, { version: 2, restarts: 5, abortRequested: true }),
    ]);
    const plan = Decisions.planReserve(
      index,
      {
        ...definitions(),
        tasks: new Map([
          ["test", Probe],
          ["core.tool", Probe],
        ]),
      },
      inputs([TaskId.make(1), TaskId.make(2), TaskId.make(3), TaskId.make(4)]),
      noOverlay(),
    );
    assert.deepStrictEqual(
      plan.cutOff.map((cut) => cut.message),
      [
        "Task 1 (test) was cut off 5 times without progress",
        "Abort handler of task 4 (test) was cut off 5 times",
      ],
    );
    assert.deepStrictEqual(settles(plan.actions), [TaskId.make(1), TaskId.make(4)]);
    // The built-in kind is reserved: its phases end it themselves.
    assert.deepStrictEqual(
      plan.start.map((start) => start.record.id),
      [TaskId.make(2), TaskId.make(3)],
    );
  });

  it("returns a failed migration for the scheduler to record, and blocks the task", () => {
    const index = indexWith([task(1, { version: 0 })]);
    const plan = Decisions.planReserve(index, definitions(), inputs([TaskId.make(1)]), noOverlay());
    assert.deepStrictEqual(
      plan.failures.map((failure) => failure.id),
      [TaskId.make(1)],
    );
    assert.deepStrictEqual(plan.start, []);
  });
});

describe("reconcile", () => {
  it("marks live foreground work below an owner with intent, passing a restart reason on", () => {
    const index = indexWith([
      task(1, { abortRequested: true, abortReason: "restart" }),
      task(2, { owner: 1 }),
      task(3, { owner: 1, background: true }),
    ]);
    const plan = Decisions.planReconcile(index, [], []);
    assert.deepStrictEqual(
      plan.marks.map((record) => [record.id, record.abortReason]),
      [[TaskId.make(2), "restart"]],
    );
  });

  it("marks the other members of a failFast wait whose member failed", () => {
    const failedMember = task(2, { owner: 1, state: completing("faulted") });
    const waiter = task(1, {
      state: {
        status: "waiting",
        checkpoint: null,
        on: [TaskId.make(2), TaskId.make(3)],
        policy: "failFast",
      },
    });
    const index = indexWith([waiter, failedMember, task(3, { owner: 1 })]);
    const plan = Decisions.planReconcile(index, [waiter], []);
    assert.deepStrictEqual(
      plan.marks.map((record) => record.id),
      [TaskId.make(3)],
    );
  });

  it("finalizes held outcomes in rounds, up the chain, settling scheduler outcomes", () => {
    const index = indexWith([
      task(1, { state: completing("faulted") }),
      task(2, { owner: 1, state: completing("completed") }),
    ]);
    const actions = Decisions.planFinalize(index, [TaskId.make(2), TaskId.make(1)], noOverlay());
    assert.deepStrictEqual(writes(actions), ["2:terminal", "1:terminal"]);
    assert.deepStrictEqual(settles(actions), [TaskId.make(1)]);
  });
});

describe("waits and aborts", () => {
  it("finds the path of a wait cycle", () => {
    const index = indexWith([
      task(1, { state: running }),
      task(2, {
        state: { status: "waiting", checkpoint: null, on: [TaskId.make(1)], policy: "allSettled" },
      }),
    ]);
    assert.deepStrictEqual(
      Decisions.waitCycle(index, TaskId.make(1), [TaskId.make(2)], noOverlay()),
      [TaskId.make(2), TaskId.make(1)],
    );
    assert.isUndefined(Decisions.waitCycle(index, TaskId.make(2), [TaskId.make(1)], noOverlay()));
  });

  it("marks a task and joins its run, orphans one nothing can take, and leaves a terminal one", () => {
    const index = indexWith([task(1, { state: running }), task(2, { kind: "gone" })]);
    const marked = Decisions.planAbort(
      index,
      definitions(),
      task(1, { state: running }),
      "run",
      false,
      noOverlay(),
    );
    assert.strictEqual(marked._tag, "Marked");
    if (marked._tag === "Marked") {
      assert.isTrue(marked.joinRun);
      assert.deepStrictEqual(writes(marked.actions), ["1:running"]);
      assert.isTrue(marked.actions[0]?._tag === "Write" && marked.actions[0].record.abortRequested);
    }
    const orphaned = Decisions.planAbort(
      index,
      definitions(),
      task(2, { kind: "gone" }),
      undefined,
      false,
      noOverlay(),
    );
    assert.strictEqual(orphaned._tag, "Marked");
    if (orphaned._tag === "Marked")
      assert.deepStrictEqual(writes(orphaned.actions), ["2:terminal"]);
    const ended = task(3, {
      state: { status: "terminal", outcome: { status: "completed", result: null } },
    });
    assert.deepStrictEqual(
      Decisions.planAbort(index, definitions(), ended, undefined, false, noOverlay()),
      { _tag: "AlreadyTerminal" },
    );
  });

  it("aborts a conversation's foreground work, crossing background work only when asked", () => {
    const index = indexWith([task(1), task(2, { background: true }), task(3, { owner: 2 })]);
    const foreground = Decisions.planConversationAbort(index, conversation, false, [conversation]);
    assert.deepStrictEqual(foreground.reached, [TaskId.make(1)]);
    assert.deepStrictEqual(foreground.withdraw, [conversation]);
    const all = Decisions.planConversationAbort(index, conversation, true, []);
    assert.sameMembers([...all.reached], [TaskId.make(1), TaskId.make(2), TaskId.make(3)]);
  });
});

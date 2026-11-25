// Tasks that cannot run, the restart limit of an extension's task, and the host's wake around them, on every
// backend.
//
// - A crash is `Killable.kill` (`HarnessSupport.killable`), and the killed Harness is then closed, which settles
//   at once on its failed storage; a reopen is a second Harness over the same storage of the backend. The host's
//   wake is a `SessionWakeups`: active is active work or work due now.
// - A phase that waits for its interruption is `Effect.never`.
// - Extensions are static: a change of extensions is a reopen with other extensions. A task graph watch's `start`
//   is a consumer fiber that pulls every frame (`followGraph`).
// - Reports' causes fail with `Report.TaskFaulted`.
// - "keeps the host's wake active until open's recovery pass has landed": the commit of the recovery pass
//   fails with `failNextCommit`; every wake the host heard says work is due.
import { assert, it } from "@effect/vitest";
import * as Cause from "effect/Cause";
import * as Clock from "effect/Clock";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { AgentStorage } from "../src/AgentStorage.ts";
import * as Doc from "../src/Doc.ts";
import type { TaskId } from "../src/Ids.ts";
import type { TaskRecord } from "../src/Records.ts";
import { TaskFaulted } from "../src/Report.ts";
import { SessionWakeups, type Wake } from "../src/SessionWakeups.ts";
import * as Task from "../src/Task.ts";
import { Tx } from "../src/Tx.ts";
import {
  completed,
  eventually,
  killable,
  openHarness,
  type TestHarness,
} from "./HarnessSupport.ts";
import { controlledStorage, describeEachBackend, flush, run } from "./SessionSupport.ts";
import { commitWrites } from "../src/testing/CommitWrites.ts";

/** A started task graph watch keeps its value current. */
const followGraph = (h: TestHarness) =>
  Effect.gen(function* () {
    const graph = yield* h.harness.watchTaskGraph;
    yield* Effect.forkChild(Stream.runDrain(graph.frames), { startImmediately: true });
    return graph;
  });

type Inner = AgentStorage["Service"];

const Run = Schema.Struct({ phase: Schema.Literal("run") });

/** A task that hangs in its phase while `crashing`, as one killed every time would, and completes otherwise. */
const crashLoop = (
  state: { crashing: boolean; started: number },
  abort?: Task.TaskHandler<null, { readonly phase: "run" }, { readonly phase: "run" }, null, never>,
) =>
  Task.make({
    name: "test.crash-loop",
    version: 1,
    input: Schema.Null,
    checkpoint: Run,
    result: Schema.Null,
    initial: () => ({ phase: "run" }),
    phases: {
      run: (_task, runtime) =>
        Effect.suspend(() => {
          state.started++;
          return state.crashing ? Effect.never : runtime.complete(null);
        }),
    },
    abort: abort ?? (() => Effect.void),
  });

/** The `durable.parked` document as an older Harness stored it: the tasks it reported parked in memory. */
const LegacyParked = Doc.make({
  kind: "durable.parked",
  version: 1,
  scope: "session",
  schema: Schema.Struct({ tasks: Schema.Array(Schema.Number) }),
  initial: () => ({ tasks: [] }),
  checkpointWhen: () => true,
});

interface Settings {
  restartLimit?: number | undefined;
}

/** A Harness over `inner` whose host hears every wake; `kill()` crashes it. */
const openWith = (inner: Inner, tasks: ReadonlyArray<Task.Any>, settings: Settings = {}) =>
  Effect.gen(function* () {
    const wakes: Array<Wake> = [];
    const crashable = yield* killable(inner);
    const h = yield* openHarness(crashable.storage, tasks, { settings: () => settings }).pipe(
      Effect.provideService(SessionWakeups, {
        schedule: (wake) =>
          Effect.sync(() => {
            wakes.push(wake);
          }),
      }),
    );
    return { h, wakes, kill: crashable.kill.pipe(Effect.andThen(h.close)) };
  });

const create = <S extends { readonly phase: string }>(
  h: TestHarness,
  task: Task.Task<null, S, null, never>,
  options: { readonly abandonOnRestart?: boolean } = {},
) =>
  Effect.gen(function* () {
    const root = yield* h.harness.ensureRoot();
    return yield* root.commit(
      Effect.gen(function* () {
        const tx = yield* Tx;
        return yield* tx.createTask(task, null, {
          ownership: { kind: "conversation" },
          abandonOnRestart: options.abandonOnRestart,
        });
      }),
    );
  });

const recordOf = (h: TestHarness, id: TaskId) =>
  h.harness.getTask(id).pipe(Effect.map(Option.getOrUndefined));

/** Whether the wake is active: active work, or work due now. */
const active = (wake: Wake, now: number) =>
  wake.activeWork ||
  Option.match(wake.nextWakeAt, { onNone: () => false, onSome: (at) => at <= now });

const activeNow = (h: TestHarness) =>
  Effect.gen(function* () {
    const now = yield* Clock.currentTimeMillis;
    return active(yield* h.harness.wake, now);
  });

const faults = (h: TestHarness) =>
  h.reports.flatMap((report) => {
    const error = Option.getOrUndefined(Cause.findErrorOption(report.cause));
    return Schema.is(TaskFaulted)(error) ? [error] : [];
  });

describeEachBackend("recovery at open", (backend) => {
  it.effect("keeps the host's wake active until open's recovery pass has landed", () =>
    run(backend)(
      Effect.gen(function* () {
        const inner = yield* AgentStorage;
        const Held = Task.make({
          name: "test.held",
          version: 1,
          input: Schema.Null,
          checkpoint: Run,
          result: Schema.Null,
          initial: () => ({ phase: "run" }),
          phases: { run: () => Effect.void },
          abort: () => Effect.void,
        });
        const first = yield* openWith(inner, [Held]);
        const id = yield* create(first.h, Held);
        yield* first.h.close;
        // A crash left the task holding its outcome: the next open's recovery pass makes it terminal.
        const record = Option.getOrThrow(yield* inner.task(id));
        const completing: TaskRecord = {
          id: record.id,
          conversationId: record.conversationId,
          kind: record.kind,
          version: record.version,
          input: record.input,
          background: record.background,
          abortRequested: record.abortRequested,
          state: { status: "completing", outcome: { status: "completed", result: null } },
        };
        yield* commitWrites(inner, [{ type: "task", value: completing }]);

        // That pass's commit fails, as on a platform reset.
        const storage = yield* controlledStorage.pipe(Effect.provideService(AgentStorage, inner));
        storage.failNextCommit("reset");
        const wakes: Array<Wake> = [];
        const h = yield* openHarness(storage, [Held]).pipe(
          Effect.provideService(SessionWakeups, {
            schedule: (wake) =>
              Effect.sync(() => {
                wakes.push(wake);
              }),
          }),
        );
        assert.strictEqual((yield* h.harness.closed).reason, "failed");
        yield* flush;
        // The host was told work is underway, and never that it ended: its recovery alarm stays.
        const now = yield* Clock.currentTimeMillis;
        assert.isAbove(wakes.length, 0);
        assert.isTrue(wakes.every((wake) => active(wake, now)));
      }),
    ),
  );
});

describeEachBackend("tasks that cannot run", (backend) => {
  it.effect(
    "faults an extension's task cut off restartLimit times in a row instead of running it again",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const inner = yield* AgentStorage;
          const state = { crashing: true, started: 0 };
          const Loop = crashLoop(state);
          let opened = yield* openWith(inner, [Loop], { restartLimit: 3 });
          const root = yield* opened.h.harness.ensureRoot();
          const id = yield* create(opened.h, Loop);
          // Each open runs the phase, and each crash cuts it off: three restarts.
          for (let attempt = 1; attempt <= 3; attempt++) {
            yield* opened.h.harness.resume;
            yield* eventually(Effect.sync(() => state.started === attempt));
            yield* opened.kill; // a crash: a close would not count
            opened = yield* openWith(inner, [Loop], { restartLimit: 3 });
          }
          const h = opened.h;
          assert.strictEqual((yield* recordOf(h, id))?.restarts, 3);
          // Nothing is parked: the task is ready, and the host is asked to run, which ends it.
          const inspection = yield* h.harness.inspect;
          assert.deepStrictEqual(inspection.tasks.find((task) => task.record.id === id)?.state, {
            kind: "pending",
            migrates: false,
          });
          assert.isTrue(yield* activeNow(h));
          yield* h.harness.resume;
          const settled = yield* h.harness.awaitTask(id);
          assert.deepStrictEqual(settled.state.outcome, {
            status: "faulted",
            error: {
              message: `Task ${id} (test.crash-loop) was cut off 3 times without progress`,
              detail: { reason: "restart_limit" },
            },
          });
          assert.strictEqual(state.started, 3);
          yield* eventually(Effect.sync(() => faults(h).length > 0));
          assert.deepStrictEqual(
            faults(h).map((fault) => ({
              taskId: fault.taskId,
              kind: fault.kind,
              conversationId: fault.conversationId,
              reason: fault.reason,
            })),
            [
              {
                taskId: id,
                kind: "test.crash-loop",
                conversationId: root.id,
                reason: "restart_limit",
              },
            ],
          );
          // The report says what it is about. `openHarness` installs the tasks as `tasks`.
          assert.deepStrictEqual(
            h.reports
              .filter((report) => report.kind === "task_faulted")
              .map(({ cause: _cause, ...info }) => info),
            [
              {
                kind: "task_faulted",
                task: { id, kind: "test.crash-loop", conversationId: root.id },
                extension: "tasks",
                reason: "restart_limit",
              },
            ],
          );
          yield* h.close;
        }),
      ),
  );

  it.effect("counts no restart for a task a close stopped", () =>
    run(backend)(
      Effect.gen(function* () {
        const inner = yield* AgentStorage;
        const state = { crashing: true, started: 0 };
        const Loop = crashLoop(state);
        let opened = yield* openWith(inner, [Loop], { restartLimit: 1 });
        const id = yield* create(opened.h, Loop);
        yield* opened.h.harness.resume;
        yield* eventually(Effect.sync(() => state.started === 1));
        yield* opened.h.close;
        opened = yield* openWith(inner, [Loop], { restartLimit: 1 });
        const record = yield* recordOf(opened.h, id);
        assert.strictEqual(record?.state.status, "pending");
        assert.isUndefined(record?.restarts);
        state.crashing = false;
        assert.strictEqual(
          (yield* opened.h.harness.awaitTask(id)).state.outcome.status,
          "completed",
        );
        yield* opened.h.close;
      }),
    ),
  );

  // An older Harness stored the tasks it reported parked in `durable.parked`, and did not count their restart. The
  // document is ignored now: a task it names that is still `running` at open was cut off, and counts.
  it.effect(
    "ignores a stored durable.parked document and counts the restart of a task it names",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const inner = yield* AgentStorage;
          const state = { crashing: true, started: 0 };
          const Loop = crashLoop(state);
          let opened = yield* openWith(inner, [Loop]);
          const id = yield* create(opened.h, Loop);
          yield* opened.h.harness.resume;
          yield* eventually(Effect.sync(() => state.started === 1));
          yield* opened.h.harness.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              (yield* tx.doc(LegacyParked)).tasks = [id];
            }),
          );
          yield* opened.kill;
          opened = yield* openWith(inner, [Loop]);
          assert.strictEqual((yield* recordOf(opened.h, id))?.restarts, 1);
          // The document is left as it was, and the task runs again.
          assert.deepStrictEqual(
            yield* opened.h.harness.commit(
              Effect.gen(function* () {
                const tx = yield* Tx;
                return [...(yield* tx.doc(LegacyParked)).tasks];
              }),
            ),
            [id],
          );
          state.crashing = false;
          yield* opened.h.harness.resume;
          assert.strictEqual(
            (yield* opened.h.harness.awaitTask(id)).state.outcome.status,
            "completed",
          );
          yield* opened.h.close;
        }),
      ),
  );

  it.effect("keeps an abort mark admitted before close in the record close writes back", () =>
    run(backend)(
      Effect.gen(function* () {
        const inner = yield* AgentStorage;
        const state = { crashing: true, started: 0 };
        const Loop = crashLoop(state);
        const { h } = yield* openWith(inner, [Loop]);
        const id = yield* create(h, Loop);
        yield* h.harness.resume;
        yield* eventually(Effect.sync(() => state.started === 1));
        // Hold the line; the abort mark is admitted behind it, then close begins.
        const gate = yield* Deferred.make<void>();
        const entered = yield* Deferred.make<void>();
        const holding = yield* Effect.forkChild(
          h.harness.commit(
            Deferred.succeed(entered, undefined).pipe(Effect.andThen(Deferred.await(gate))),
          ),
          { startImmediately: true },
        );
        yield* Deferred.await(entered);
        const aborting = yield* Effect.forkChild(h.harness.abortTask(id), {
          startImmediately: true,
        });
        const closing = yield* Effect.forkChild(h.close, { startImmediately: true });
        // The invocation ends while the line is held; its write-back waits behind the abort mark.
        yield* flush;
        yield* Deferred.succeed(gate, undefined);
        yield* Fiber.join(holding);
        assert.deepStrictEqual(yield* Fiber.join(aborting), { _tag: "Marked" });
        yield* Fiber.join(closing);
        const record = Option.getOrThrow(yield* inner.task(id));
        assert.isTrue(record.abortRequested);
        assert.strictEqual(record.state.status, "pending");
      }),
    ),
  );

  it.effect("faults a task whose abort handler is cut off restartLimit times", () =>
    run(backend)(
      Effect.gen(function* () {
        const inner = yield* AgentStorage;
        let aborts = 0;
        const Stuck = Task.make({
          name: "test.stuck-abort",
          version: 1,
          input: Schema.Null,
          checkpoint: Run,
          result: Schema.Null,
          initial: () => ({ phase: "run" }),
          phases: { run: () => Effect.never },
          abort: () =>
            Effect.suspend(() => {
              aborts++;
              return Effect.never;
            }),
        });
        let opened = yield* openWith(inner, [Stuck], { restartLimit: 1 });
        const id = yield* create(opened.h, Stuck);
        yield* opened.h.harness.resume;
        yield* Effect.forkChild(opened.h.harness.abortTask(id), { startImmediately: true });
        yield* eventually(Effect.sync(() => aborts === 1));
        yield* opened.kill; // a crash in the abort handler
        opened = yield* openWith(inner, [Stuck], { restartLimit: 1 });
        const settled = yield* opened.h.harness.awaitTask(id);
        assert.strictEqual(settled.state.outcome.status, "faulted");
        assert.deepInclude(
          settled.state.outcome.status === "faulted" ? settled.state.outcome.error : undefined,
          { detail: { reason: "restart_limit" } },
        );
        // The receipt keeps no restart count.
        assert.isUndefined(settled.restarts);
        assert.strictEqual(aborts, 1);
        yield* eventually(Effect.sync(() => faults(opened.h).length > 0));
        assert.deepInclude(faults(opened.h)[0], { taskId: id, reason: "restart_limit" });
        yield* opened.h.close;
      }),
    ),
  );

  it.effect("reads restartLimit when it reserves the task: a raised one runs it again", () =>
    run(backend)(
      Effect.gen(function* () {
        const inner = yield* AgentStorage;
        const state = { crashing: true, started: 0 };
        const Loop = crashLoop(state);
        const settings: Settings = { restartLimit: 1 };
        let opened = yield* openWith(inner, [Loop], settings);
        const id = yield* create(opened.h, Loop);
        yield* opened.h.harness.resume;
        yield* eventually(Effect.sync(() => state.started === 1));
        yield* opened.kill; // a crash: a close would not count
        opened = yield* openWith(inner, [Loop], settings);
        assert.strictEqual((yield* recordOf(opened.h, id))?.restarts, 1);
        state.crashing = false;
        // The host's live settings change before the task is reserved; it runs instead of ending.
        settings.restartLimit = Number.POSITIVE_INFINITY;
        yield* opened.h.harness.resume;
        assert.strictEqual(
          (yield* opened.h.harness.awaitTask(id)).state.outcome.status,
          "completed",
        );
        assert.strictEqual(state.started, 2);
        yield* opened.h.close;
      }),
    ),
  );

  it.effect("runs the abort handler of a task at the limit", () =>
    run(backend)(
      Effect.gen(function* () {
        const inner = yield* AgentStorage;
        const state = { crashing: true, started: 0 };
        const aborted: Array<TaskId> = [];
        const Loop = crashLoop(state, (task, runtime) =>
          Effect.suspend(() => {
            aborted.push(task.id);
            return runtime.aborted;
          }),
        );
        let opened = yield* openWith(inner, [Loop], { restartLimit: 1 });
        const id = yield* create(opened.h, Loop);
        yield* opened.h.harness.resume;
        yield* eventually(Effect.sync(() => state.started === 1));
        yield* opened.kill; // a crash: a close would not count
        opened = yield* openWith(inner, [Loop], { restartLimit: 1 });
        yield* opened.h.harness.abortTask(id);
        assert.strictEqual((yield* opened.h.harness.awaitTask(id)).state.outcome.status, "aborted");
        assert.deepStrictEqual(aborted, [id]);
        assert.strictEqual(state.started, 1);
        yield* opened.h.close;
      }),
    ),
  );

  it.effect("does not count the same checkpoint committed again as progress", () =>
    run(backend)(
      Effect.gen(function* () {
        const inner = yield* AgentStorage;
        let started = 0;
        const Intent = Task.make({
          name: "test.intent",
          version: 1,
          input: Schema.Null,
          checkpoint: Run,
          result: Schema.Null,
          initial: () => ({ phase: "run" }),
          phases: {
            run: (task, runtime) =>
              Effect.gen(function* () {
                started++;
                // Intent before an effect, then the effect never returns: the checkpoint is unchanged.
                yield* runtime.next(task.state.checkpoint);
                return yield* Effect.never;
              }),
          },
          abort: () => Effect.void,
        });
        let opened = yield* openWith(inner, [Intent], { restartLimit: 2 });
        const id = yield* create(opened.h, Intent);
        for (let attempt = 1; attempt <= 2; attempt++) {
          yield* opened.h.harness.resume;
          yield* eventually(Effect.sync(() => started === attempt));
          yield* eventually(
            recordOf(opened.h, id).pipe(Effect.map((record) => record?.state.status === "running")),
          );
          yield* opened.kill; // a crash: a close would not count
          opened = yield* openWith(inner, [Intent], { restartLimit: 2 });
        }
        assert.strictEqual((yield* recordOf(opened.h, id))?.restarts, 2);
        yield* opened.h.harness.resume;
        const outcome = (yield* opened.h.harness.awaitTask(id)).state.outcome;
        assert.strictEqual(outcome.status, "faulted");
        assert.deepInclude(outcome.status === "faulted" ? outcome.error : undefined, {
          detail: { reason: "restart_limit" },
        });
        assert.strictEqual(started, 2);
        yield* opened.h.close;
      }),
    ),
  );

  it.effect("starts the count over when the task makes progress", () =>
    run(backend)(
      Effect.gen(function* () {
        const inner = yield* AgentStorage;
        let crashing = true;
        let started = 0;
        const Steps = Task.make({
          name: "test.steps",
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
              Effect.suspend(() => {
                started++;
                return crashing ? Effect.never : runtime.next({ phase: "two" });
              }),
            two: () =>
              Effect.suspend(() => {
                started++;
                return Effect.never;
              }),
          },
          abort: () => Effect.void,
        });
        let opened = yield* openWith(inner, [Steps], { restartLimit: 2 });
        const id = yield* create(opened.h, Steps);
        yield* opened.h.harness.resume;
        yield* eventually(Effect.sync(() => started === 1));
        yield* opened.kill; // a crash: a close would not count
        opened = yield* openWith(inner, [Steps], { restartLimit: 2 });
        assert.strictEqual((yield* recordOf(opened.h, id))?.restarts, 1);
        // Progress: phase one commits phase two, which resets the count.
        crashing = false;
        yield* opened.h.harness.resume;
        yield* eventually(Effect.sync(() => started === 3));
        assert.isUndefined((yield* recordOf(opened.h, id))?.restarts);
        yield* opened.h.close;
      }),
    ),
  );

  // An invocation that waits in memory for a task is active work, so the host must not let the object hibernate
  // under it: losing it is a crash like any other, and counts toward `restartLimit`.
  it.effect(
    "counts a restart for a task cut off while it waits in memory, up to restartLimit",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const inner = yield* AgentStorage;
          let waits = 0;
          const Sleeper = Task.make({
            name: "test.sleeper",
            version: 1,
            input: Schema.Null,
            checkpoint: Schema.Union([
              Schema.Struct({ phase: Schema.Literal("run") }),
              Schema.Struct({ phase: Schema.Literal("done") }),
            ]),
            result: Schema.Null,
            initial: () => ({ phase: "run" }),
            phases: {
              run: (_task, runtime) => runtime.sleepUntil(60_000, { phase: "done" }),
              done: (_task, runtime) => runtime.complete(null),
            },
            abort: () => Effect.void,
          });
          const Waiter = Task.make({
            name: "test.waiter",
            version: 1,
            input: Schema.Null,
            checkpoint: Schema.Union([
              Schema.Struct({ phase: Schema.Literal("spawn") }),
              Schema.Struct({ phase: Schema.Literal("wait"), child: Schema.Number }),
            ]),
            result: Schema.Null,
            initial: () => ({ phase: "spawn" }),
            phases: {
              spawn: (task, runtime) =>
                runtime.commit(
                  Effect.gen(function* () {
                    const tx = yield* Tx;
                    const child = yield* tx.createTask(Sleeper, null, {
                      ownership: { kind: "task", taskId: task.id },
                    });
                    return Task.next({ phase: "wait" as const, child });
                  }),
                ),
              wait: (task, runtime) =>
                Effect.gen(function* () {
                  waits++;
                  // SAFETY: the checkpoint stores the ID `createTask` returned.
                  yield* runtime.awaitTask(task.state.checkpoint.child as TaskId);
                  yield* runtime.complete(null);
                }),
            },
            abort: () => Effect.void,
          });
          const tasks = [Waiter, Sleeper];
          let opened = yield* openWith(inner, tasks, { restartLimit: 2 });
          const id = yield* create(opened.h, Waiter);
          // Each open runs the wait again, and the host hears active work; each eviction cuts it off.
          for (let attempt = 1; attempt <= 2; attempt++) {
            yield* opened.h.harness.resume;
            yield* eventually(Effect.sync(() => waits === attempt));
            const wakes = opened.wakes;
            yield* eventually(
              Effect.sync(() => {
                const last = wakes.at(-1);
                return (
                  last !== undefined &&
                  last.activeWork &&
                  Option.getOrUndefined(last.nextWakeAt) === 60_000
                );
              }),
            );
            yield* opened.kill; // evicted under active work: a crash
            opened = yield* openWith(inner, tasks, { restartLimit: 2 });
            assert.strictEqual((yield* recordOf(opened.h, id))?.restarts, attempt);
          }
          yield* opened.h.harness.resume;
          const settled = yield* opened.h.harness.awaitTask(id);
          assert.deepStrictEqual(settled.state.outcome, {
            status: "faulted",
            error: {
              message: `Task ${id} (test.waiter) was cut off 2 times without progress`,
              detail: { reason: "restart_limit" },
            },
          });
          assert.strictEqual(waits, 2);
          yield* opened.h.close;
        }),
      ),
  );

  it.effect("tells the graph and the host when a migration fails", () =>
    run(backend)(
      Effect.gen(function* () {
        const inner = yield* AgentStorage;
        const definition = (
          version: number,
          migrate?: Task.TaskMigration<null, { readonly phase: "run" }>,
        ) =>
          Task.make({
            name: "test.migrating",
            version,
            input: Schema.Null,
            checkpoint: Run,
            result: Schema.Null,
            initial: () => ({ phase: "run" }),
            phases: { run: () => Effect.void },
            abort: () => Effect.void,
            migrate,
          });
        const V1 = definition(1);
        const V2 = definition(2, () => {
          throw new Error("cannot migrate");
        });
        let opened = yield* openWith(inner, [V1]);
        // Created, never run: scheduling stays off.
        const id = yield* create(opened.h, V1);
        yield* opened.h.close;
        opened = yield* openWith(inner, [V2]);
        const graph = yield* followGraph(opened.h);
        yield* opened.h.harness.resume;
        yield* eventually(
          Effect.sync(() => graph.value.tasks[String(id)]?.blocked === "migration_failed"),
        );
        yield* eventually(activeNow(opened.h).pipe(Effect.map((isActive) => !isActive)));
        assert.deepStrictEqual(yield* opened.h.harness.wake, {
          nextWakeAt: Option.none(),
          activeWork: false,
        });
        const now = yield* Clock.currentTimeMillis;
        yield* eventually(
          Effect.sync(() => opened.wakes.length > 0 && !active(opened.wakes.at(-1)!, now)),
        );
        yield* opened.h.close;
      }),
    ),
  );

  it.effect("shows an abandoned task waiting for its definition as blocked", () =>
    run(backend)(
      Effect.gen(function* () {
        const inner = yield* AgentStorage;
        const Abandoned = Task.make({
          name: "test.abandoned",
          version: 1,
          input: Schema.Null,
          checkpoint: Run,
          result: Schema.Null,
          initial: () => ({ phase: "run" }),
          phases: { run: () => Effect.void },
          abort: () => Effect.void,
        });
        let opened = yield* openWith(inner, [Abandoned]);
        const id = yield* create(opened.h, Abandoned, { abandonOnRestart: true });
        yield* opened.h.close;
        // The next open marks it `restart`; without its definition, it waits for one to clean up.
        opened = yield* openWith(inner, []);
        const graph = yield* followGraph(opened.h);
        yield* opened.h.harness.resume;
        yield* eventually(
          recordOf(opened.h, id).pipe(Effect.map((record) => record?.abortReason === "restart")),
        );
        yield* eventually(
          Effect.sync(() => graph.value.tasks[String(id)]?.blocked === "missing_task"),
        );
        yield* opened.h.close;
      }),
    ),
  );

  it.effect(
    "shows a task without a definition as blocked, and wakes the host once a registry change unblocks it",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const inner = yield* AgentStorage;
          const Missing = Task.make({
            name: "test.missing",
            version: 1,
            input: Schema.Null,
            checkpoint: Run,
            result: Schema.Null,
            initial: () => ({ phase: "run" }),
            phases: { run: (_task, runtime) => runtime.commit(Effect.succeed(completed(null))) },
            abort: () => Effect.void,
          });
          let opened = yield* openWith(inner, []);
          const id = yield* create(opened.h, Missing);
          yield* opened.h.harness.resume;
          yield* eventually(
            opened.h.harness.inspect.pipe(
              Effect.map((inspection) => {
                const state = inspection.tasks.find((task) => task.record.id === id)?.state;
                return state?.kind === "blocked" && state.reason === "missing_task";
              }),
            ),
          );
          yield* eventually(activeNow(opened.h).pipe(Effect.map((isActive) => !isActive)));
          yield* opened.h.close;
          // The registry change is a reopen with the definition: the host hears it can run work now.
          opened = yield* openWith(inner, [Missing]);
          const now = yield* Clock.currentTimeMillis;
          yield* eventually(Effect.sync(() => opened.wakes.some((wake) => active(wake, now))));
          yield* opened.h.harness.resume;
          assert.strictEqual(
            (yield* opened.h.harness.awaitTask(id)).state.outcome.status,
            "completed",
          );
          yield* opened.h.close;
        }),
      ),
  );
});

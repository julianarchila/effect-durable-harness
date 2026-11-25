// Durable sleeps in tasks, on every backend.
//
// Notes:
// - The settable `now` is the `TestClock`. A host's `SessionWakeups` is told every wake, and its `Wake`
//   (`{ nextWakeAt, activeWork }`) is simplified with `simpleWake` into `{ active, at? }`: active is active work or
//   work due now, `at` a deadline not yet due.
// - Without host wakeups, the scheduler keeps a `Clock` sleep for the earliest sleep, which
//   `scheduler.sleepTimerAt()` shows, and the `TestClock` fires.
// - A reopened SQLite file is a second Harness over the same storage.
// - `runtime.setTimer` returns the timer's fiber instead of a disarm function; a late host fire cannot happen,
//   since the invocation's end interrupts the fiber, so that case checks the timer never fires.
// - A task graph watch's `start` is a consumer fiber that pulls every frame.
// - "runs a due sleep at any scheduling pass, before its timer fires": the unrelated change is a task
//   created in another conversation, as the registry is static; the host's wakeups keep the scheduler
//   from arming a timer that would fire first.
//
// Not covered: a foreground timer the host declines; there is no `HarnessOptions.setTimer`, timers are
// `Clock` sleeps.
import { assert, it } from "@effect/vitest";
import * as Clock from "effect/Clock";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import type { TaskId } from "../src/Ids.ts";
import { SessionWakeups, type Wake } from "../src/SessionWakeups.ts";
import * as Task from "../src/Task.ts";
import { Tx } from "../src/Tx.ts";
import {
  abortedWith,
  eventually,
  openHarness,
  type OpenHarnessOptions,
  type TestHarness,
} from "./HarnessSupport.ts";
import {
  type ControlledStorage,
  controlledStorage,
  describeEachBackend,
  failureMessage,
  flush,
  run,
} from "./SessionSupport.ts";

/** A simplified view of a `Wake` at the clock's `now`. */
interface SimpleWake {
  readonly active: boolean;
  readonly at?: number;
}

const simpleWake = (wake: Wake, now: number): SimpleWake => {
  const next = Option.getOrUndefined(wake.nextWakeAt);
  const active = wake.activeWork || (next !== undefined && next <= now);
  return next !== undefined && next > now ? { active, at: next } : { active };
};

const NapState = Schema.Union([
  Schema.Struct({ phase: Schema.Literal("start") }),
  Schema.Struct({ phase: Schema.Literal("after"), slept: Schema.Number }),
]);

/** Sleeps `delay` ms on the Harness clock, then completes with the time it resumed at. */
const napTask = (delay: number, runs: { after: number } = { after: 0 }) =>
  Task.make({
    name: "test.nap",
    version: 1,
    input: Schema.Null,
    checkpoint: NapState,
    result: Schema.Number,
    initial: () => ({ phase: "start" }),
    phases: {
      start: (_task, runtime) =>
        Effect.gen(function* () {
          const now = yield* runtime.now;
          yield* runtime.sleepUntil(now + delay, { phase: "after", slept: delay });
        }),
      after: (_task, runtime) =>
        Effect.gen(function* () {
          runs.after++;
          const resumed = yield* runtime.now;
          yield* runtime.complete(resumed);
        }),
    },
    abort: (_task, runtime) => runtime.commit(Effect.succeed(abortedWith("test"))),
  });

type Nap = ReturnType<typeof napTask>;

const startNap = (h: TestHarness, task: Nap) =>
  Effect.gen(function* () {
    const root = yield* h.harness.ensureRoot();
    return yield* root.commit(
      Effect.gen(function* () {
        const tx = yield* Tx;
        return yield* tx.createTask(task, null, { ownership: { kind: "conversation" } });
      }),
    );
  });

const stateOf = (h: TestHarness, id: TaskId) =>
  h.harness.getTask(id).pipe(Effect.map((record) => Option.getOrUndefined(record)?.state));

const statusIs = (h: TestHarness, id: TaskId, status: string) =>
  eventually(stateOf(h, id).pipe(Effect.map((state) => state?.status === status)));

const wakeNow = (h: TestHarness) =>
  Effect.gen(function* () {
    const now = yield* Clock.currentTimeMillis;
    return simpleWake(yield* h.harness.wake, now);
  });

/** A Harness whose host is told every wake in `wakes`. */
const openWithHost = (
  storage: ControlledStorage,
  tasks: ReadonlyArray<Task.Any>,
  wakes: Array<Wake> = [],
  options: OpenHarnessOptions = {},
) =>
  openHarness(storage, tasks, options).pipe(
    Effect.provideService(SessionWakeups, {
      schedule: (wake) =>
        Effect.sync(() => {
          wakes.push(wake);
        }),
    }),
  );

describeEachBackend("sleeping tasks", (backend) => {
  it.effect("sleeps without an invocation and resumes at its checkpoint once due", () =>
    run(backend)(
      Effect.gen(function* () {
        yield* TestClock.setTime(1_000);
        const wakes: Array<Wake> = [];
        const runs = { after: 0 };
        const Nap = napTask(1_000, runs);
        const h = yield* openWithHost(yield* controlledStorage, [Nap], wakes);
        const graph = yield* h.harness.watchTaskGraph;
        // A started watch keeps its value current.
        yield* Effect.forkChild(Stream.runDrain(graph.frames), { startImmediately: true });
        const id = yield* startNap(h, Nap);
        yield* h.harness.resume;
        yield* statusIs(h, id, "sleeping");
        assert.deepStrictEqual(yield* stateOf(h, id), {
          status: "sleeping",
          checkpoint: { phase: "after", slept: 1_000 },
          until: 2_000,
        });
        const inspection = yield* h.harness.inspect;
        assert.deepStrictEqual(inspection.tasks.find((task) => task.record.id === id)?.state, {
          kind: "sleeping",
          until: 2_000,
        });
        yield* flush;
        assert.deepStrictEqual(graph.value.tasks[String(id)]?.state, {
          status: "sleeping",
          phase: "after",
          until: 2_000,
        });
        yield* eventually(wakeNow(h).pipe(Effect.map((wake) => !wake.active)));
        assert.deepStrictEqual(yield* wakeNow(h), { active: false, at: 2_000 });
        yield* flush;
        assert.deepStrictEqual(simpleWake(wakes.at(-1)!, 1_000), { active: false, at: 2_000 });

        // Early and repeated wakes run nothing.
        yield* TestClock.setTime(1_999);
        yield* h.harness.resume;
        yield* h.harness.resume;
        yield* flush;
        assert.strictEqual(runs.after, 0);
        assert.strictEqual((yield* stateOf(h, id))?.status, "sleeping");

        yield* TestClock.setTime(2_000);
        yield* h.harness.resume;
        yield* h.harness.resume;
        const receipt = yield* h.harness.awaitTask(id);
        assert.deepStrictEqual(receipt.state.outcome, { status: "completed", result: 2_000 });
        assert.strictEqual(runs.after, 1);
        yield* eventually(wakeNow(h).pipe(Effect.map((wake) => !wake.active)));
        assert.deepStrictEqual(yield* wakeNow(h), { active: false });
        yield* eventually(
          Effect.sync(() => {
            const last = wakes.at(-1)!;
            return !last.activeWork && Option.isNone(last.nextWakeAt);
          }),
        );
        // Only changes: idle at open, due, running, asleep until 2000, due and running again, idle.
        for (let index = 1; index < wakes.length; index++) {
          assert.notDeepEqual(wakes[index], wakes[index - 1]);
        }
        assert.isTrue(wakes.some((wake) => wake.activeWork));
        assert.isTrue(
          wakes.some(
            (wake) => !wake.activeWork && Option.getOrUndefined(wake.nextWakeAt) === 2_000,
          ),
        );
        yield* h.close;
      }),
    ),
  );

  it.effect("keeps a sleep across reopen", () =>
    run(backend)(
      Effect.gen(function* () {
        yield* TestClock.setTime(1_000);
        const Nap = napTask(60_000);
        const storage = yield* controlledStorage;
        let h = yield* openWithHost(storage, [Nap]);
        const id = yield* startNap(h, Nap);
        yield* h.harness.resume;
        yield* statusIs(h, id, "sleeping");
        yield* h.close;

        h = yield* openWithHost(storage, [Nap]);
        // Known from storage before scheduling starts, once open's recovery pass is done.
        yield* eventually(wakeNow(h).pipe(Effect.map((wake) => !wake.active)));
        assert.deepStrictEqual(yield* wakeNow(h), { active: false, at: 61_000 });
        yield* h.harness.resume;
        yield* flush;
        assert.strictEqual((yield* stateOf(h, id))?.status, "sleeping");
        yield* h.close;

        yield* TestClock.setTime(61_000);
        h = yield* openWithHost(storage, [Nap]);
        assert.deepStrictEqual(yield* wakeNow(h), { active: true });
        yield* h.harness.resume;
        assert.deepStrictEqual((yield* h.harness.awaitTask(id)).state.outcome, {
          status: "completed",
          result: 61_000,
        });
        yield* h.close;
      }),
    ),
  );

  it.effect(
    "resumes a due sleep with its timer without onWake, with background timers declined",
    () =>
      run(backend)(
        Effect.gen(function* () {
          yield* TestClock.setTime(1_000);
          const Nap = napTask(500);
          const h = yield* openHarness(yield* controlledStorage, [Nap]);
          const id = yield* startNap(h, Nap);
          yield* h.harness.resume;
          yield* eventually(Effect.sync(() => h.scheduler.sleepTimerAt() === 1_500));
          assert.strictEqual((yield* stateOf(h, id))?.status, "sleeping");
          yield* TestClock.adjust(500);
          yield* statusIs(h, id, "terminal");
          assert.deepStrictEqual(yield* stateOf(h, id), {
            status: "terminal",
            outcome: { status: "completed", result: 1_500 },
          });
          yield* h.close;
        }),
      ),
  );

  it.effect("runs a due sleep at any scheduling pass, before its timer fires", () =>
    run(backend)(
      Effect.gen(function* () {
        yield* TestClock.setTime(1_000);
        const Nap = napTask(500);
        const Other = napTask(60_000);
        const h = yield* openWithHost(yield* controlledStorage, [Nap]);
        const id = yield* startNap(h, Nap);
        yield* h.harness.resume;
        yield* statusIs(h, id, "sleeping");
        // The deadline passes, and an unrelated change starts a pass; no timer fires.
        yield* TestClock.setTime(1_500);
        const conversation = yield* h.harness.createConversation({
          ownership: { kind: "ownerless" },
        });
        yield* conversation.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            yield* tx.createTask(Other, null, { ownership: { kind: "conversation" } });
          }),
        );
        yield* statusIs(h, id, "terminal");
        assert.deepStrictEqual(yield* stateOf(h, id), {
          status: "terminal",
          outcome: { status: "completed", result: 1_500 },
        });
        yield* h.close;
      }),
    ),
  );

  it.effect("disarms the sleep timer when the sleep is aborted and when the Harness closes", () =>
    run(backend)(
      Effect.gen(function* () {
        yield* TestClock.setTime(1_000);
        const Short = napTask(500);
        const h = yield* openHarness(yield* controlledStorage, [Short]);
        const aborted = yield* startNap(h, Short);
        yield* h.harness.resume;
        yield* eventually(Effect.sync(() => h.scheduler.sleepTimerAt() === 1_500));
        yield* h.harness.abortTask(aborted);
        yield* h.harness.awaitTask(aborted);
        yield* eventually(Effect.sync(() => h.scheduler.sleepTimerAt() === undefined));
        const kept = yield* startNap(h, Short);
        yield* statusIs(h, kept, "sleeping");
        yield* eventually(Effect.sync(() => h.scheduler.sleepTimerAt() === 1_500));
        yield* h.close;
        assert.isUndefined(h.scheduler.sleepTimerAt());
      }),
    ),
  );

  it.effect("disarms an invocation's timers when it ends, and a late fire runs nothing", () =>
    run(backend)(
      Effect.gen(function* () {
        const fired: Array<string> = [];
        let captured: Task.TaskRuntime<null, { readonly phase: "run" }, null> | undefined;
        const Timed = Task.make({
          name: "test.timed",
          version: 1,
          input: Schema.Null,
          checkpoint: Schema.Struct({ phase: Schema.Literal("run") }),
          result: Schema.Null,
          initial: () => ({ phase: "run" }),
          phases: {
            run: (_task, runtime) =>
              Effect.gen(function* () {
                captured = runtime;
                yield* runtime.setTimer(
                  Effect.sync(() => {
                    fired.push("late");
                  }),
                  "60 seconds",
                );
                yield* runtime.complete(null);
              }),
          },
          abort: () => Effect.void,
        });
        const h = yield* openHarness(yield* controlledStorage, [Timed]);
        const root = yield* h.harness.ensureRoot();
        const id = yield* root.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            return yield* tx.createTask(Timed, null, { ownership: { kind: "conversation" } });
          }),
        );
        yield* h.harness.resume;
        yield* h.harness.awaitTask(id);
        yield* flush;
        // Armed after the invocation ended: nothing fires.
        yield* captured!.setTimer(
          Effect.sync(() => {
            fired.push("after end");
          }),
          "1 second",
        );
        yield* TestClock.adjust("60 seconds");
        yield* flush;
        assert.deepStrictEqual(fired, []);
        yield* h.close;
      }),
    ),
  );

  it.effect("arms no timer for sleeps with onWake", () =>
    run(backend)(
      Effect.gen(function* () {
        yield* TestClock.setTime(1_000);
        const Nap = napTask(500);
        const h = yield* openWithHost(yield* controlledStorage, [Nap]);
        const id = yield* startNap(h, Nap);
        yield* h.harness.resume;
        yield* statusIs(h, id, "sleeping");
        yield* flush;
        assert.isUndefined(h.scheduler.sleepTimerAt());
        yield* TestClock.setTime(1_500);
        assert.deepStrictEqual(yield* wakeNow(h), { active: true });
        yield* h.harness.resume;
        assert.strictEqual((yield* h.harness.awaitTask(id)).state.outcome.status, "completed");
        yield* h.close;
      }),
    ),
  );

  it.effect("counts neither a wait on a sleeping task nor an outcome held over one as active", () =>
    run(backend)(
      Effect.gen(function* () {
        const Child = napTask(500);
        const Parent = Task.make({
          name: "test.parent",
          version: 1,
          input: Schema.Struct({ hold: Schema.Boolean }),
          checkpoint: Schema.Union([
            Schema.Struct({ phase: Schema.Literal("start") }),
            Schema.Struct({ phase: Schema.Literal("joined"), child: Schema.Number }),
          ]),
          result: Schema.Null,
          initial: () => ({ phase: "start" }),
          phases: {
            start: (task, runtime) =>
              runtime.commit(
                Effect.gen(function* () {
                  const tx = yield* Tx;
                  const child = yield* tx.createTask(Child, null, {
                    ownership: { kind: "task", taskId: task.id },
                  });
                  // Held: completes next, holding its outcome over the sleeping child. Otherwise waits for it.
                  return task.input.hold
                    ? Task.next({ phase: "joined" as const, child })
                    : Task.waitFor([child], {
                        checkpoint: { phase: "joined" as const, child },
                        policy: "allSettled",
                      });
                }),
              ),
            joined: (_task, runtime) => runtime.complete(null),
          },
          abort: () => Effect.void,
        });
        for (const hold of [false, true]) {
          yield* TestClock.setTime(1_000);
          const h = yield* openWithHost(yield* controlledStorage, [Parent, Child]);
          const root = yield* h.harness.ensureRoot();
          const id = yield* root.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              return yield* tx.createTask(
                Parent,
                { hold },
                { ownership: { kind: "conversation" } },
              );
            }),
          );
          yield* h.harness.resume;
          yield* statusIs(h, id, hold ? "completing" : "waiting");
          yield* eventually(wakeNow(h).pipe(Effect.map((wake) => wake.at === 1_500)));
          yield* eventually(wakeNow(h).pipe(Effect.map((wake) => !wake.active)));
          assert.deepStrictEqual(yield* wakeNow(h), { active: false, at: 1_500 });
          yield* h.close;
        }
      }),
    ),
  );

  it.effect("runs the abort handler of a sleeping task without waiting for its time", () =>
    run(backend)(
      Effect.gen(function* () {
        yield* TestClock.setTime(1_000);
        const Nap = napTask(60_000);
        const h = yield* openWithHost(yield* controlledStorage, [Nap]);
        const id = yield* startNap(h, Nap);
        yield* h.harness.resume;
        yield* statusIs(h, id, "sleeping");
        yield* h.harness.abortTask(id);
        assert.deepStrictEqual((yield* h.harness.awaitTask(id)).state.outcome, {
          status: "aborted",
          reason: "test",
        });
        yield* h.close;
      }),
    ),
  );

  it.effect(
    "rejects a sleep until something that is not a time, and a sleep in an abort handler",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const errors: Array<string> = [];
          const Invalid = Task.make({
            name: "test.invalid-sleep",
            version: 1,
            input: Schema.Null,
            checkpoint: Schema.Union([
              Schema.Struct({ phase: Schema.Literal("run") }),
              Schema.Struct({ phase: Schema.Literal("next") }),
            ]),
            result: Schema.Null,
            initial: () => ({ phase: "run" }),
            phases: {
              run: (_task, runtime) =>
                Effect.gen(function* () {
                  for (const until of [Number.NaN, -1, Number.POSITIVE_INFINITY]) {
                    const error = yield* Effect.flip(runtime.sleepUntil(until, { phase: "next" }));
                    errors.push(error.message);
                  }
                  yield* runtime.complete(null);
                }),
              next: () => Effect.void,
            },
            abort: () => Effect.void,
          });
          const Aborting = Task.make({
            name: "test.sleep-in-abort",
            version: 1,
            input: Schema.Null,
            checkpoint: Schema.Struct({ phase: Schema.Literal("run") }),
            result: Schema.Null,
            initial: () => ({ phase: "run" }),
            phases: { run: () => Effect.never },
            abort: (_task, runtime) =>
              Effect.gen(function* () {
                const now = yield* runtime.now;
                errors.push(yield* failureMessage(runtime.sleepUntil(now + 10, { phase: "run" })));
                yield* runtime.commit(Effect.succeed(abortedWith("test")));
              }),
          });
          const h = yield* openHarness(yield* controlledStorage, [Invalid, Aborting]);
          const root = yield* h.harness.ensureRoot();
          const id = yield* root.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              return yield* tx.createTask(Invalid, null, { ownership: { kind: "conversation" } });
            }),
          );
          const aborting = yield* root.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              return yield* tx.createTask(Aborting, null, { ownership: { kind: "conversation" } });
            }),
          );
          yield* h.harness.resume;
          assert.strictEqual((yield* h.harness.awaitTask(id)).state.outcome.status, "completed");
          yield* h.harness.abortTask(aborting);
          assert.strictEqual(
            (yield* h.harness.awaitTask(aborting)).state.outcome.status,
            "aborted",
          );
          assert.deepStrictEqual(errors.slice(0, 3), [
            `Task ${id} cannot sleep until NaN: not a time in milliseconds`,
            `Task ${id} cannot sleep until -1: not a time in milliseconds`,
            `Task ${id} cannot sleep until Infinity: not a time in milliseconds`,
          ]);
          assert.include(errors[3], `Abort handler of task ${aborting} cannot sleep`);
          yield* h.close;
        }),
      ),
  );

  it.effect("does not count a task without a definition as active", () =>
    run(backend)(
      Effect.gen(function* () {
        const Missing = napTask(10);
        const h = yield* openHarness(yield* controlledStorage, []);
        const root = yield* h.harness.ensureRoot();
        // Created through a token the registry does not hold: nothing can run it.
        yield* root.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            yield* tx.createTask(Missing, null, { ownership: { kind: "conversation" } });
          }),
        );
        yield* eventually(wakeNow(h).pipe(Effect.map((wake) => !wake.active)));
        assert.deepStrictEqual(yield* wakeNow(h), { active: false });
        yield* h.harness.resume;
        yield* flush;
        yield* eventually(wakeNow(h).pipe(Effect.map((wake) => !wake.active)));
        assert.deepStrictEqual(yield* wakeNow(h), { active: false });
        yield* h.close;
      }),
    ),
  );

  it.effect("reports a throwing onWake and offers the same wake again at the next pass", () =>
    run(backend)(
      Effect.gen(function* () {
        yield* TestClock.setTime(1_000);
        const wakes: Array<Wake> = [];
        let failing = true;
        const failed = yield* Deferred.make<void>();
        const Nap = napTask(1_000);
        const h = yield* openHarness(yield* controlledStorage, [Nap]).pipe(
          Effect.provideService(SessionWakeups, {
            schedule: (wake) =>
              Effect.suspend(() => {
                wakes.push(wake);
                if (failing && Option.isSome(wake.nextWakeAt) && wake.nextWakeAt.value === 2_000) {
                  return Deferred.succeed(failed, undefined).pipe(
                    Effect.andThen(Effect.die(new Error("alarm failed"))),
                  );
                }
                return Effect.void;
              }),
          }),
        );
        const id = yield* startNap(h, Nap);
        yield* h.harness.resume;
        yield* Deferred.await(failed);
        yield* eventually(
          Effect.sync(() =>
            h.reports.some(
              (report) =>
                report.kind === "host_error" &&
                report.callback === "onWake" &&
                String(report.cause).includes("alarm failed"),
            ),
          ),
        );
        // The host recovers; an unrelated change offers the same, undelivered wake again.
        failing = false;
        const offered = wakes.length;
        yield* h.harness.createConversation({ ownership: { kind: "ownerless" } });
        yield* eventually(Effect.sync(() => wakes.length > offered));
        assert.deepStrictEqual(simpleWake(wakes.at(-1)!, 1_000), { active: false, at: 2_000 });
        yield* TestClock.setTime(2_000);
        yield* h.harness.resume;
        yield* h.harness.awaitTask(id);
        yield* eventually(
          Effect.sync(() => {
            const last = wakes.at(-1)!;
            return !last.activeWork && Option.isNone(last.nextWakeAt);
          }),
        );
        yield* h.close;
      }),
    ),
  );
});

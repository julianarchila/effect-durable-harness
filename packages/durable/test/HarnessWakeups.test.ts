// Tests of `Harness.wake` and `SessionWakeups`. `HarnessSleep.test.ts` covers `wake()` and the host's wakeups
// through sleeping tasks; these cases follow the same semantics: a sleep ends its phase and holds no invocation.
import { assert, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as TestClock from "effect/testing/TestClock";
import type { TaskId } from "../src/Ids.ts";
import { SessionWakeups, type Wake } from "../src/SessionWakeups.ts";
import * as Task from "../src/Task.ts";
import { Tx } from "../src/Tx.ts";
import { completed, eventually, openHarness, type TestHarness } from "./HarnessSupport.ts";
import { controlledStorage, describeEachBackend, flush, run } from "./SessionSupport.ts";

const gates = new Map<string, Deferred.Deferred<void>>();
const gate = (name: string) => {
  const existing = gates.get(name);
  if (existing !== undefined) return existing;
  const created = Deferred.makeUnsafe<void>();
  gates.set(name, created);
  return created;
};

/** Holds until its gate opens, then completes. */
const Hold = Task.make({
  name: "test.wake.hold",
  version: 1,
  input: Schema.Struct({ name: Schema.String }),
  checkpoint: Schema.Struct({ phase: Schema.Literal("hold") }),
  result: Schema.Null,
  initial: () => ({ phase: "hold" }),
  phases: {
    hold: (task, runtime) =>
      Deferred.await(gate(task.input.name)).pipe(
        Effect.andThen(runtime.commit(() => Effect.succeed(completed(null)))),
      ),
  },
  abort: (_task, runtime) =>
    runtime.commit(() =>
      Effect.succeed({ status: "terminal", outcome: { status: "aborted" } } as const),
    ),
});

/** The phase ends asleep until the deadline, and the next phase runs once it is due. */
const FollowUp = Task.make({
  name: "test.wake.follow-up",
  version: 1,
  input: Schema.Struct({ at: Schema.Number }),
  checkpoint: Schema.Union([
    Schema.Struct({ phase: Schema.Literal("sleep") }),
    Schema.Struct({ phase: Schema.Literal("due") }),
  ]),
  result: Schema.Number,
  initial: () => ({ phase: "sleep" }),
  phases: {
    sleep: (task, runtime) =>
      Effect.gen(function* () {
        sleeps.push(task.input.at);
        yield* runtime.sleepUntil(task.input.at, { phase: "due" });
      }),
    due: (_task, runtime) =>
      Effect.gen(function* () {
        const now = yield* runtime.now;
        yield* runtime.commit(() => Effect.succeed(completed(now)));
      }),
  },
  abort: (_task, runtime) =>
    runtime.commit(() =>
      Effect.succeed({ status: "terminal", outcome: { status: "aborted" } } as const),
    ),
});
const sleeps: Array<number> = [];

/** Waits in memory for a child it creates, which sleeps; `abandon` creates it with `abandonOnRestart`. */
const Parent = Task.make({
  name: "test.wake.parent",
  version: 1,
  input: Schema.Struct({ at: Schema.Number, abandon: Schema.Boolean }),
  checkpoint: Schema.Union([
    Schema.Struct({ phase: Schema.Literal("spawn") }),
    Schema.Struct({ phase: Schema.Literal("wait"), child: Schema.Number }),
  ]),
  result: Schema.Null,
  initial: () => ({ phase: "spawn" }),
  phases: {
    spawn: (task, runtime) =>
      runtime.commit(() =>
        Effect.gen(function* () {
          const tx = yield* Tx;
          const child = yield* tx.createTask(
            FollowUp,
            { at: task.input.at },
            { ownership: { kind: "conversation" }, abandonOnRestart: task.input.abandon },
          );
          return { status: "running", checkpoint: { phase: "wait", child } } as const;
        }),
      ),
    wait: (task, runtime) =>
      Effect.gen(function* () {
        // SAFETY: the checkpoint stores the ID `createTask` returned.
        yield* runtime.awaitTask(task.state.checkpoint.child as TaskId);
        yield* runtime.commit(() => Effect.succeed(completed(null)));
      }),
  },
  abort: (_task, runtime) =>
    runtime.commit(() =>
      Effect.succeed({ status: "terminal", outcome: { status: "aborted" } } as const),
    ),
});

/** Created with `abandonOnRestart`: creates a `Parent` it owns and waits for it durably, holding no invocation. */
const Owner = Task.make({
  name: "test.wake.owner",
  version: 1,
  input: Schema.Struct({ at: Schema.Number }),
  checkpoint: Schema.Union([
    Schema.Struct({ phase: Schema.Literal("spawn") }),
    Schema.Struct({ phase: Schema.Literal("done") }),
  ]),
  result: Schema.Null,
  initial: () => ({ phase: "spawn" }),
  phases: {
    spawn: (task, runtime) =>
      runtime.commit(() =>
        Effect.gen(function* () {
          const tx = yield* Tx;
          const child = yield* tx.createTask(
            Parent,
            { at: task.input.at, abandon: false },
            { ownership: { kind: "task", taskId: task.id } },
          );
          return Task.waitFor([child], { checkpoint: { phase: "done" }, policy: "allSettled" });
        }),
      ),
    done: (_task, runtime) => runtime.complete(null),
  },
  abort: (_task, runtime) =>
    runtime.commit(() =>
      Effect.succeed({ status: "terminal", outcome: { status: "aborted" } } as const),
    ),
});

const tasks = [Hold, FollowUp, Parent, Owner];

const create = <I extends Schema.Json, S extends { readonly phase: string }, R>(
  h: TestHarness,
  task: Task.Task<I, S, R, never>,
  input: I,
) =>
  Effect.gen(function* () {
    const root = yield* h.harness.ensureRoot();
    return yield* root.commit(
      Effect.gen(function* () {
        const tx = yield* Tx;
        return yield* tx.createTask(task, input, { ownership: { kind: "conversation" } });
      }),
    );
  });

const wakeOf = (h: TestHarness) => h.harness.wake;

/** Waits until the wake settles at `expected`: a reconcile the last commit scheduled is due work too. */
const settlesAt = (h: TestHarness, expected: Wake) =>
  eventually(
    wakeOf(h).pipe(
      Effect.map(
        (wake) =>
          wake.activeWork === expected.activeWork &&
          Option.getOrUndefined(wake.nextWakeAt) === Option.getOrUndefined(expected.nextWakeAt),
      ),
    ),
  ).pipe(
    Effect.andThen(wakeOf(h)),
    Effect.map((wake) => assert.deepStrictEqual(wake, expected)),
  );

const noWake: Wake = { nextWakeAt: Option.none(), activeWork: false };

describeEachBackend("Harness wakeups", (backend) => {
  it.effect("reports no wake for an idle Harness", () =>
    run(backend)(
      Effect.gen(function* () {
        const h = yield* openHarness(yield* controlledStorage, tasks);
        yield* h.harness.resume;
        yield* settlesAt(h, noWake);
      }),
    ),
  );

  it.effect("reports now while a task is ready to reserve, and active work while it runs", () =>
    run(backend)(
      Effect.gen(function* () {
        gates.clear();
        yield* TestClock.setTime(1_000);
        const h = yield* openHarness(yield* controlledStorage, tasks);
        const id = yield* create(h, Hold, { name: "ready" });
        // Paused: the task is ready, so the host must wake the Session now.
        assert.deepStrictEqual(yield* wakeOf(h), {
          nextWakeAt: Option.some(1_000),
          activeWork: false,
        });
        yield* h.harness.resume;
        yield* eventually(
          wakeOf(h).pipe(Effect.map((wake) => wake.activeWork && Option.isNone(wake.nextWakeAt))),
        );
        yield* Deferred.succeed(gate("ready"), undefined);
        yield* h.harness.awaitTask(id);
        yield* settlesAt(h, noWake);
      }),
    ),
  );

  it.effect("reports a sleeping task's deadline without active work", () =>
    run(backend)(
      Effect.gen(function* () {
        sleeps.length = 0;
        const scheduled: Array<Wake> = [];
        const h = yield* openHarness(yield* controlledStorage, tasks).pipe(
          Effect.provideService(SessionWakeups, {
            schedule: (wake) => Effect.sync(() => scheduled.push(wake)),
          }),
        );
        const id = yield* create(h, FollowUp, { at: 60_000 });
        yield* h.harness.resume;
        yield* eventually(Effect.sync(() => sleeps.length === 1));
        const expected: Wake = { nextWakeAt: Option.some(60_000), activeWork: false };
        yield* settlesAt(h, expected);
        yield* flush;
        // The host was told in order; the last report is the current wake.
        assert.deepStrictEqual(scheduled.at(-1), expected);
        yield* TestClock.adjust(60_000);
        // With a host's wakeups the kernel arms no timer: the host's alarm resumes it.
        yield* flush;
        assert.strictEqual(sleeps.length, 1);
        yield* h.harness.resume;
        const settled = yield* h.harness.awaitTask(id);
        assert.deepStrictEqual(settled.state.outcome, { status: "completed", result: 60_000 });
        yield* eventually(
          Effect.sync(() => {
            const last = scheduled.at(-1)!;
            return Option.isNone(last.nextWakeAt) && !last.activeWork;
          }),
        );
        assert.deepStrictEqual(scheduled.at(-1), noWake);
      }),
    ),
  );

  // An invocation is active work while it waits in memory for a task, whether a restart would abort that task
  // or not: losing it is a crash, which reruns its phase and counts a restart.
  for (const abandon of [false, true]) {
    it.effect(
      `counts an invocation waiting in memory for a task as active work${abandon ? " (abandonOnRestart)" : ""}`,
      () =>
        run(backend)(
          Effect.gen(function* () {
            sleeps.length = 0;
            const h = yield* openHarness(yield* controlledStorage, tasks);
            yield* create(h, Parent, { at: 60_000, abandon });
            yield* h.harness.resume;
            yield* eventually(Effect.sync(() => sleeps.length === 1));
            yield* settlesAt(h, { nextWakeAt: Option.some(60_000), activeWork: true });
          }),
        ),
    );
  }

  it.effect("counts an in-memory wait below an abandonOnRestart owner as active work", () =>
    run(backend)(
      Effect.gen(function* () {
        sleeps.length = 0;
        const h = yield* openHarness(yield* controlledStorage, tasks);
        const root = yield* h.harness.ensureRoot();
        yield* root.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            return yield* tx.createTask(
              Owner,
              { at: 60_000 },
              { ownership: { kind: "conversation" }, abandonOnRestart: true },
            );
          }),
        );
        yield* h.harness.resume;
        yield* eventually(Effect.sync(() => sleeps.length === 1));
        // The owner waits durably; its child waits in memory for a sleeping grandchild. The next open would
        // abort the owner's tree, so losing the child's invocation is not free.
        yield* settlesAt(h, { nextWakeAt: Option.some(60_000), activeWork: true });
      }),
    ),
  );

  // A deferred follow-up: the eviction is a close and the alarm is the deadline the wake reports. A sleep
  // holds no invocation, so the reopened Session knows the deadline from storage.
  it.effect(
    "resumes a deferred follow-up after an eviction once the clock reaches its deadline",
    () =>
      run(backend)(
        Effect.gen(function* () {
          sleeps.length = 0;
          const storage = yield* controlledStorage;
          const first = yield* openHarness(storage, tasks);
          const id = yield* create(first, FollowUp, { at: 86_400_000 });
          yield* first.harness.resume;
          yield* eventually(Effect.sync(() => sleeps.length === 1));
          yield* settlesAt(first, { nextWakeAt: Option.some(86_400_000), activeWork: false });
          // Evicted while sleeping: the task is `sleeping` in storage, with its deadline.
          yield* first.close;
          const early = yield* openHarness(storage, tasks);
          yield* settlesAt(early, { nextWakeAt: Option.some(86_400_000), activeWork: false });
          yield* early.close;

          yield* TestClock.adjust(86_400_000);
          const reopened = yield* openHarness(storage, tasks);
          // The alarm woke a new process: the sleep is due now.
          assert.isTrue(Option.isSome((yield* wakeOf(reopened)).nextWakeAt));
          yield* reopened.harness.resume;
          const settled = yield* reopened.harness.awaitTask(id);
          assert.deepStrictEqual(settled.state.outcome, {
            status: "completed",
            result: 86_400_000,
          });
          // The next phase ran from the sleep's checkpoint; the phase that slept did not run again.
          assert.deepStrictEqual(sleeps, [86_400_000]);
        }),
      ),
  );

  // A sleep holds no invocation, so a sleeping `abandonOnRestart` task is not active work: only its deadline
  // is reported.
  it.effect("reports only the deadline of a sleeping task a restart would abort", () =>
    run(backend)(
      Effect.gen(function* () {
        sleeps.length = 0;
        const h = yield* openHarness(yield* controlledStorage, tasks);
        const root = yield* h.harness.ensureRoot();
        yield* root.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            return yield* tx.createTask(
              FollowUp,
              { at: 60_000 },
              { ownership: { kind: "conversation" }, abandonOnRestart: true },
            );
          }),
        );
        yield* h.harness.resume;
        yield* eventually(Effect.sync(() => sleeps.length === 1));
        yield* settlesAt(h, { nextWakeAt: Option.some(60_000), activeWork: false });
      }),
    ),
  );

  it.effect("tells the host again a wake whose report failed", () =>
    run(backend)(
      Effect.gen(function* () {
        sleeps.length = 0;
        const scheduled: Array<Wake> = [];
        let failures = 0;
        const h = yield* openHarness(yield* controlledStorage, tasks).pipe(
          Effect.provideService(SessionWakeups, {
            schedule: (wake) =>
              Effect.suspend(() => {
                // The host cannot arm the deadline's alarm once.
                if (Option.getOrUndefined(wake.nextWakeAt) === 60_000 && failures === 0) {
                  failures++;
                  return Effect.die(new Error("alarm unavailable"));
                }
                scheduled.push(wake);
                return Effect.void;
              }),
          }),
        );
        yield* create(h, FollowUp, { at: 60_000 });
        yield* h.harness.resume;
        yield* eventually(Effect.sync(() => sleeps.length === 1 && failures === 1));
        // Any later change of state makes the kernel tell the host the same wake again.
        yield* h.harness.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            yield* tx.createConversation({ ownership: { kind: "ownerless" } });
          }),
        );
        yield* eventually(
          Effect.sync(
            () => Option.getOrUndefined(scheduled.at(-1)?.nextWakeAt ?? Option.none()) === 60_000,
          ),
        );
        assert.deepStrictEqual(scheduled.at(-1), {
          nextWakeAt: Option.some(60_000),
          activeWork: false,
        });
      }),
    ),
  );
});

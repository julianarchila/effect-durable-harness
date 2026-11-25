// Recovery of tasks across a crash or a reopen, on every backend.
//
// A reopen is a new Harness over the same controlled storage. The registry is static, so a registry change
// becomes a reopen with other extensions.
//
// Not covered, since a static registry has no runtime registration, uninstall or definition handover:
// - "orphans a marked task whose definition disappeared while its run was active": the definition cannot
//   disappear while a Harness runs.
// - "definition handover": "hands over at the next phase boundary to a same-version replacement without
//   overlap", "keeps the memos across a handover", "hands over to a newer version with a migration",
//   "hands over to a newer version whose migration fails and leaves the task blocked", "keeps running under
//   the old definition when the replacement is missing or cannot take the task", "rejects a runtime commit
//   of the old invocation queued behind its handover commit", "preserves an abort mark that races the
//   handover commit; the new definition aborts".
import { assert, it } from "@effect/vitest";
import * as Cause from "effect/Cause";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as TestClock from "effect/testing/TestClock";
import * as Doc from "../src/Doc.ts";
import type { TaskId } from "../src/Ids.ts";
import * as Task from "../src/Task.ts";
import { Tx } from "../src/Tx.ts";
import {
  abortedWith,
  assertNoCallsAfter,
  completed,
  eventually,
  openHarness,
  settled,
  type TestHarness,
} from "./HarnessSupport.ts";
import {
  type ControlledStorage,
  controlledStorage,
  describeEachBackend,
  failureMessage,
  flush,
  recordCalls,
  run,
} from "./SessionSupport.ts";

const createIn = <S extends { readonly phase: string }, R, Req>(
  harness: TestHarness,
  task: Task.Task<null, S, R, Req>,
) =>
  Effect.gen(function* () {
    const root = yield* harness.harness.ensureRoot();
    return yield* root.commit(
      Effect.gen(function* () {
        const tx = yield* Tx;
        return yield* tx.createTask(task, null, { ownership: { kind: "conversation" } });
      }),
    );
  });

/** The task's record, or undefined. */
const getTask = <R>(harness: TestHarness, id: TaskId<R>) =>
  harness.harness.getTask(id).pipe(Effect.map(Option.getOrUndefined));

/**
 * A crashed Harness is abandoned without close: it opens in a scope nobody closes, since closing it
 * would join a handler that ignores its signal forever.
 */
const openAbandoned = (storage: ControlledStorage, tasks: ReadonlyArray<Task.Any>) =>
  Effect.gen(function* () {
    const scope = yield* Scope.make();
    return yield* openHarness(storage, tasks).pipe(Scope.provide(scope));
  });

/** Fake external service whose operations are idempotent by request key. */
class TransferService {
  readonly applied = new Map<string, number>();
  calls = 0;

  apply(key: string, amount: number): number {
    this.calls++;
    if (!this.applied.has(key)) this.applied.set(key, amount * 10);
    return this.applied.get(key) ?? 0;
  }
}

const TransferState = Schema.Union([
  Schema.Struct({ phase: Schema.Literal("prepare") }),
  Schema.Struct({ phase: Schema.Literal("apply"), key: Schema.String }),
]);

/**
 * Intent/effect/outcome task: `prepare` commits the intent, `apply` performs the effect and commits the outcome.
 * While `interrupt.first` is set, the first `apply` blocks after the effect until the invocation is signalled.
 */
const transferTask = (service: TransferService, interrupt: { first: boolean }) =>
  Task.make({
    name: "test.transfer",
    version: 1,
    input: Schema.Struct({ amount: Schema.Number }),
    checkpoint: TransferState,
    result: Schema.Struct({ receipt: Schema.Number }),
    initial: () => ({ phase: "prepare" }),
    phases: {
      prepare: (task, runtime) =>
        Effect.gen(function* () {
          yield* runtime.memo("requested", task.input.amount);
          yield* runtime.commit(() =>
            Effect.succeed({
              status: "running",
              checkpoint: { phase: "apply", key: `transfer-${task.id}` },
            } as const),
          );
        }),
      apply: (task, runtime) =>
        Effect.gen(function* () {
          const receipt = service.apply(task.state.checkpoint.key, task.input.amount);
          if (interrupt.first) {
            interrupt.first = false;
            // Blocks until the invocation is interrupted.
            return yield* Effect.never;
          }
          yield* runtime.commit(() => Effect.succeed(completed({ receipt })));
        }),
    },
    abort: (_task, runtime) =>
      runtime.commit(() =>
        Effect.succeed({ status: "terminal", outcome: { status: "aborted" } } as const),
      ),
  });

const Step = Schema.Struct({ phase: Schema.Literal("run") });
type Step = typeof Step.Type;
const RefinedStep = Schema.Struct({
  phase: Schema.Literal("run"),
  note: Schema.optionalKey(Schema.NonEmptyString),
});

/** A versioned one-phase task that completes with `result`, optionally migrating older records. */
const versioned = (version: number, result: string, migrate?: Task.TaskMigration<null, Step>) =>
  Task.make({
    name: "test.versioned",
    version,
    input: Schema.Null,
    checkpoint: Step,
    result: Schema.String,
    initial: () => ({ phase: "run" }),
    phases: { run: (_task, runtime) => runtime.commit(() => Effect.succeed(completed(result))) },
    abort: (_task, runtime) => runtime.commit(() => Effect.succeed(abortedWith(result))),
    migrate,
  });

describeEachBackend("task recovery", (backend) => {
  it.effect(
    "resumes an intent/effect/outcome task interrupted after its intent across close and reopen",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const storage = yield* controlledStorage;
          const service = new TransferService();
          const Transfer = transferTask(service, { first: true });

          const first = yield* openHarness(storage, [Transfer]);
          const root = yield* first.harness.ensureRoot();
          const id = yield* root.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              return yield* tx.createTask(
                Transfer,
                { amount: 7 },
                { ownership: { kind: "conversation" } },
              );
            }),
          );
          yield* first.harness.resume;
          yield* eventually(Effect.sync(() => service.calls === 1));
          yield* first.close;

          const second = yield* openHarness(storage, [Transfer]);
          // Open reconciled `running` to `pending` and kept the checkpoint and memos; nothing ran yet.
          assert.containSubset(yield* getTask(second, id), {
            abortRequested: false,
            memos: { requested: 7 },
            state: { status: "pending", checkpoint: { phase: "apply", key: `transfer-${id}` } },
          });
          assert.strictEqual(service.calls, 1);
          yield* second.harness.resume;
          const receipt = yield* second.harness.awaitTask(id);
          assert.deepStrictEqual(receipt.state.outcome, {
            status: "completed",
            result: { receipt: 70 },
          });
          assert.strictEqual(service.calls, 2);
          assert.strictEqual(service.applied.size, 1);
          yield* second.close;

          const third = yield* openHarness(storage, [Transfer]);
          assert.deepStrictEqual(yield* getTask(third, id), receipt);
          yield* third.close;
        }),
      ),
  );

  it.effect(
    "keeps a task's startedAt across close and reopen and stamps endedAt when it settles",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const storage = yield* controlledStorage;
          const service = new TransferService();
          const Transfer = transferTask(service, { first: true });
          // The Harness reads `Clock`, here the TestClock.
          yield* TestClock.setTime(2_000);

          const first = yield* openHarness(storage, [Transfer]);
          const root = yield* first.harness.ensureRoot();
          const id = yield* root.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              return yield* tx.createTask(
                Transfer,
                { amount: 7 },
                { ownership: { kind: "conversation" } },
              );
            }),
          );
          yield* first.harness.resume;
          yield* eventually(Effect.sync(() => service.calls === 1));
          yield* TestClock.setTime(3_000);
          yield* first.close;

          yield* TestClock.setTime(9_000);
          const second = yield* openHarness(storage, [Transfer]);
          const reopened = yield* getTask(second, id);
          assert.deepStrictEqual(
            [reopened?.state.status, reopened?.startedAt, reopened?.endedAt],
            ["pending", 2_000, undefined],
          );
          yield* second.harness.resume;
          const receipt = yield* second.harness.awaitTask(id);
          assert.deepStrictEqual([receipt.startedAt, receipt.endedAt], [2_000, 9_000]);
          yield* second.close;
        }),
      ),
  );

  it.effect("resumes abort work after close at every direct-task abort stage", () =>
    run(backend)(
      Effect.gen(function* () {
        const storage = yield* controlledStorage;
        const log: Array<string> = [];
        const abortGate = { block: true };
        const abortReached = yield* Deferred.make<void>();
        const runRelease = yield* Deferred.make<void>();
        const Abortable = Task.make({
          name: "test.abortable",
          version: 1,
          input: Schema.Null,
          checkpoint: Step,
          result: Schema.Null,
          initial: () => ({ phase: "run" }),
          phases: {
            run: () =>
              Effect.gen(function* () {
                log.push("run");
                // Ignores the abort signal until released, so the mark is durable while the run is active.
                yield* Effect.uninterruptible(Deferred.await(runRelease));
              }),
          },
          abort: (_task, runtime) =>
            Effect.gen(function* () {
              log.push("abort");
              if (abortGate.block) {
                yield* Deferred.succeed(abortReached, undefined);
                // Blocks until the invocation is interrupted.
                return yield* Effect.never;
              }
              yield* runtime.commit(() => Effect.succeed(abortedWith("stop")));
            }),
        });

        // Stage 1: the mark is committed while the run invocation is still active.
        let opened = yield* openHarness(storage, [Abortable]);
        const id = yield* createIn(opened, Abortable);
        yield* opened.harness.resume;
        yield* eventually(Effect.sync(() => log.length === 1));
        const aborting = yield* Effect.forkChild(opened.harness.abortTask(id));
        const stage1 = opened;
        yield* eventually(
          getTask(stage1, id).pipe(Effect.map((record) => record?.abortRequested === true)),
        );
        const closing = yield* Effect.forkChild(opened.close);
        // Let the forked close seal before the run is released.
        yield* flush;
        yield* Deferred.succeed(runRelease, undefined);
        yield* Fiber.join(closing);
        assert.deepStrictEqual(yield* Fiber.join(aborting), { _tag: "Marked" });
        assert.deepStrictEqual(log, ["run"]);

        // Stage 2: reopen dispatches the abort invocation, never the run; close while it is active.
        opened = yield* openHarness(storage, [Abortable]);
        assert.containSubset(yield* getTask(opened, id), {
          abortRequested: true,
          state: { status: "pending" },
        });
        yield* opened.harness.resume;
        yield* Deferred.await(abortReached);
        yield* opened.close;
        assert.deepStrictEqual(log, ["run", "abort"]);

        // Stage 3: a fresh abort invocation settles the task.
        abortGate.block = false;
        opened = yield* openHarness(storage, [Abortable]);
        yield* opened.harness.resume;
        assert.deepStrictEqual((yield* opened.harness.awaitTask(id)).state.outcome, {
          status: "aborted",
          reason: "stop",
        });
        yield* opened.close;
        assert.deepStrictEqual(log, ["run", "abort", "abort"]);

        // Stage 4: the terminal receipt survives reopen and nothing runs again.
        opened = yield* openHarness(storage, [Abortable]);
        yield* opened.harness.resume;
        yield* flush;
        assert.deepStrictEqual(yield* opened.harness.abortTask(id), { _tag: "AlreadyTerminal" });
        yield* opened.close;
        assert.deepStrictEqual(log, ["run", "abort", "abort"]);
      }),
    ),
  );
});

/**
 * Crash simulation: the crashed Harness is abandoned without close, its held storage commit never lands, and its
 * blocked handlers never return. A new Harness then opens the same storage.
 */
type Log = Array<string>;

/** A task whose run ignores its signal and whose abort handler blocks while `blockAbort` is set. */
const crashTask = (log: Log, options: { readonly blockAbort: boolean }) =>
  Task.make({
    name: "test.crash",
    version: 1,
    input: Schema.Null,
    checkpoint: Step,
    result: Schema.Null,
    initial: () => ({ phase: "run" }),
    phases: {
      run: () =>
        Effect.gen(function* () {
          log.push("run");
          // Never returns and ignores its signal.
          return yield* Effect.uninterruptible(Effect.never);
        }),
    },
    abort: (_task, runtime) =>
      Effect.gen(function* () {
        log.push("abort");
        if (options.blockAbort) return yield* Effect.uninterruptible(Effect.never);
        yield* runtime.commit(() => Effect.succeed(abortedWith("recovered")));
      }),
  });

// A crash drops the held commit: it dies without reaching storage, so the crashed Session fails. The
// crashed Harness is never closed. The recovered Harness is abandoned too, since its run may ignore its
// signal forever.
const recover = (storage: ControlledStorage, log: Log) =>
  Effect.gen(function* () {
    yield* storage.crash;
    const harness = yield* openAbandoned(storage, [crashTask(log, { blockAbort: false })]);
    const page = yield* harness.harness.commit(
      Effect.gen(function* () {
        const tx = yield* Tx;
        return yield* tx.scanTasks({ kind: "test.crash" }, 1);
      }),
    );
    const found = page.items[0];
    assert.isDefined(found);
    return { harness, id: found.id };
  });

const crashedRun = (log: Log) =>
  Effect.gen(function* () {
    const storage = yield* controlledStorage;
    const harness = yield* openAbandoned(storage, [crashTask(log, { blockAbort: true })]);
    const id = yield* createIn(harness, crashTask(log, { blockAbort: true }));
    yield* harness.harness.resume;
    yield* eventually(Effect.sync(() => log.length === 1));
    return { storage, harness, id };
  });

describeEachBackend("task crash recovery", (backend) => {
  it.effect("crash while the mark commit is in storage: the run resumes", () =>
    run(backend)(
      Effect.gen(function* () {
        const log: Log = [];
        const { storage, harness, id } = yield* crashedRun(log);
        const held = yield* storage.holdCommits;
        yield* Effect.forkChild(harness.harness.abortTask(id));
        yield* held.entered;
        const recovered = yield* recover(storage, log);
        assert.containSubset(yield* getTask(recovered.harness, id), {
          abortRequested: false,
          state: { status: "pending" },
        });
        yield* recovered.harness.harness.resume;
        yield* eventually(Effect.sync(() => log.length === 2));
        assert.deepStrictEqual(log, ["run", "run"]);
      }),
    ),
  );

  it.effect("crash after the mark before the run joins: only the abort handler runs", () =>
    run(backend)(
      Effect.gen(function* () {
        const log: Log = [];
        const { storage, harness, id } = yield* crashedRun(log);
        // The run ignores its signal, so abortTask never finishes joining it.
        let joined = false;
        yield* Effect.forkChild(
          harness.harness.abortTask(id).pipe(
            Effect.tap(() =>
              Effect.sync(() => {
                joined = true;
              }),
            ),
          ),
        );
        yield* eventually(
          getTask(harness, id).pipe(Effect.map((record) => record?.abortRequested === true)),
        );
        yield* flush;
        assert.isFalse(joined);
        const recovered = yield* recover(storage, log);
        assert.containSubset(yield* getTask(recovered.harness, id), {
          abortRequested: true,
          state: { status: "pending" },
        });
        yield* recovered.harness.harness.resume;
        assert.deepStrictEqual((yield* recovered.harness.harness.awaitTask(id)).state.outcome, {
          status: "aborted",
          reason: "recovered",
        });
        assert.deepStrictEqual(log, ["run", "abort"]);
        yield* recovered.harness.close;
      }),
    ),
  );

  it.effect("crash while the abort handler runs: a fresh abort invocation settles the task", () =>
    run(backend)(
      Effect.gen(function* () {
        const log: Log = [];
        const storage = yield* controlledStorage;
        const Crash = crashTask(log, { blockAbort: true });
        const harness = yield* openAbandoned(storage, [Crash]);
        const id = yield* createIn(harness, Crash);
        yield* harness.harness.abortTask(id);
        yield* harness.harness.resume;
        yield* eventually(Effect.sync(() => log.length === 1));
        assert.deepStrictEqual(log, ["abort"]);
        assert.containSubset(yield* getTask(harness, id), { state: { status: "running" } });
        const recovered = yield* recover(storage, log);
        yield* recovered.harness.harness.resume;
        assert.deepStrictEqual((yield* recovered.harness.harness.awaitTask(id)).state.outcome, {
          status: "aborted",
          reason: "recovered",
        });
        assert.deepStrictEqual(log, ["abort", "abort"]);
        yield* recovered.harness.close;
      }),
    ),
  );

  it.effect("crash while the abort outcome is in storage: the abort handler runs again", () =>
    run(backend)(
      Effect.gen(function* () {
        const log: Log = [];
        const storage = yield* controlledStorage;
        const reached = yield* Deferred.make<void>();
        const proceed = yield* Deferred.make<void>();
        const Crash = Task.make({
          name: "test.crash",
          version: 1,
          input: Schema.Null,
          checkpoint: Step,
          result: Schema.Null,
          initial: () => ({ phase: "run" }),
          phases: { run: () => Effect.void },
          abort: (_task, runtime) =>
            Effect.gen(function* () {
              log.push("abort");
              yield* Deferred.succeed(reached, undefined);
              yield* Deferred.await(proceed);
              yield* runtime.commit(() => Effect.succeed(abortedWith("lost")));
            }),
        });
        const harness = yield* openAbandoned(storage, [Crash]);
        const id = yield* createIn(harness, Crash);
        yield* harness.harness.abortTask(id);
        yield* harness.harness.resume;
        yield* Deferred.await(reached);
        const held = yield* storage.holdCommits;
        yield* Deferred.succeed(proceed, undefined);
        yield* held.entered;
        const recovered = yield* recover(storage, log);
        yield* recovered.harness.harness.resume;
        assert.deepStrictEqual((yield* recovered.harness.harness.awaitTask(id)).state.outcome, {
          status: "aborted",
          reason: "recovered",
        });
        assert.deepStrictEqual(log, ["abort", "abort"]);
        yield* recovered.harness.close;
      }),
    ),
  );

  it.effect("crash after the terminal outcome: nothing runs again", () =>
    run(backend)(
      Effect.gen(function* () {
        const log: Log = [];
        const storage = yield* controlledStorage;
        const Crash = crashTask(log, { blockAbort: false });
        const harness = yield* openAbandoned(storage, [Crash]);
        const id = yield* createIn(harness, Crash);
        yield* harness.harness.abortTask(id);
        yield* harness.harness.resume;
        yield* harness.harness.awaitTask(id);
        const recovered = yield* recover(storage, log);
        yield* recovered.harness.harness.resume;
        yield* flush;
        assert.deepStrictEqual(log, ["abort"]);
        assert.deepStrictEqual((yield* getTask(recovered.harness, id))?.state, {
          status: "terminal",
          outcome: { status: "aborted", reason: "recovered" },
        });
        yield* recovered.harness.close;
      }),
    ),
  );

  it.effect("crash while the reservation commit is in storage: the task is still pending", () =>
    run(backend)(
      Effect.gen(function* () {
        const log: Log = [];
        const storage = yield* controlledStorage;
        const Crash = crashTask(log, { blockAbort: false });
        const harness = yield* openAbandoned(storage, [Crash]);
        const id = yield* createIn(harness, Crash);
        const held = yield* storage.holdCommits;
        yield* harness.harness.resume;
        yield* held.entered;
        const recovered = yield* recover(storage, log);
        assert.strictEqual((yield* getTask(recovered.harness, id))?.state.status, "pending");
        assert.deepStrictEqual(log, []);
        yield* recovered.harness.harness.abortTask(id);
        yield* recovered.harness.harness.resume;
        yield* recovered.harness.harness.awaitTask(id);
        assert.deepStrictEqual(log, ["abort"]);
        yield* recovered.harness.close;
      }),
    ),
  );
});

describeEachBackend("blocked tasks", (backend) => {
  it.effect(
    "keeps a task with a missing definition pending, and live for idle waits, until registration",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const storage = yield* controlledStorage;
          const V1 = versioned(1, "v1");
          const harness = yield* openHarness(storage, []);
          const id = yield* createIn(harness, V1);
          yield* harness.harness.resume;
          yield* flush;
          assert.strictEqual((yield* getTask(harness, id))?.state.status, "pending");
          const idle = yield* Effect.forkChild(harness.harness.awaitIdle);
          yield* flush;
          // The registry is static, so registration is a reopen with the definition. The blocked task kept the
          // idle wait open until then; close rejects the wait.
          assert.isFalse(yield* settled(idle));
          yield* harness.close;
          assert.strictEqual((yield* Effect.flip(Fiber.join(idle)))._tag, "SessionClosed");
          const registered = yield* openHarness(storage, [V1]);
          yield* registered.harness.resume;
          yield* registered.harness.awaitIdle;
          assert.deepStrictEqual((yield* getTask(registered, id))?.state, {
            status: "terminal",
            outcome: { status: "completed", result: "v1" },
          });
          yield* registered.close;
        }),
      ),
  );

  it.effect(
    "keeps a task stored by a newer version pending until a fitting definition is registered",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const storage = yield* controlledStorage;
          const harness = yield* openHarness(storage, [versioned(1, "old")]);
          const id = yield* createIn(harness, versioned(2, "new"));
          yield* harness.harness.resume;
          yield* flush;
          assert.strictEqual((yield* getTask(harness, id))?.state.status, "pending");
          // With a static registry the fitting definition arrives with a reopen.
          yield* harness.close;
          const replaced = yield* openHarness(storage, [versioned(2, "new")]);
          assert.deepStrictEqual((yield* replaced.harness.awaitTask(id)).state.outcome, {
            status: "completed",
            result: "new",
          });
          yield* replaced.close;
        }),
      ),
  );

  it.effect(
    "migrates at reservation, leaves the record unchanged when migration fails, and retries only for a new definition",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const storage = yield* controlledStorage;
          let opened = yield* openHarness(storage, []);
          const id = yield* createIn(opened, versioned(1, "v1"));
          yield* opened.close;

          let failures = 0;
          opened = yield* openHarness(storage, [
            versioned(2, "v2", () => {
              failures++;
              throw new Error("cannot migrate");
            }),
          ]);
          yield* opened.harness.resume;
          const failing = opened;
          yield* eventually(Effect.sync(() => failing.reports.length === 1));
          // A static registry has no installs to wake the scheduler without a new definition, so only the
          // settle remains.
          yield* flush;
          assert.strictEqual(failures, 1);
          assert.lengthOf(opened.reports, 1);
          assert.strictEqual(opened.reports[0]!.kind, "migration_failed");
          assert.include(Cause.pretty(opened.reports[0]!.cause), "cannot migrate");
          assert.containSubset(yield* getTask(opened, id), {
            version: 1,
            state: { status: "pending", checkpoint: { phase: "run" } },
          });

          const migrations: Array<number> = [];
          // The new definition replaces the old one with a reopen.
          yield* opened.close;
          opened = yield* openHarness(storage, [
            versioned(2, "v2", (_input, checkpoint, fromVersion) => {
              migrations.push(fromVersion);
              // SAFETY: the stored v1 checkpoint is a `Step`; the scheduler also checks it against the schema.
              return { input: null, checkpoint: checkpoint as Step };
            }),
          ]);
          const receipt = yield* opened.harness.awaitTask(id);
          assert.containSubset(receipt, {
            version: 2,
            state: { outcome: { status: "completed", result: "v2" } },
          });
          assert.deepStrictEqual(migrations, [1]);
          yield* opened.close;
        }),
      ),
  );

  it.effect("blocks an older record whose newer definition has no migration", () =>
    run(backend)(
      Effect.gen(function* () {
        const storage = yield* controlledStorage;
        let opened = yield* openHarness(storage, []);
        const id = yield* createIn(opened, versioned(1, "v1"));
        yield* opened.close;
        opened = yield* openHarness(storage, [versioned(2, "v2")]);
        yield* opened.harness.resume;
        const blocked = opened;
        yield* eventually(Effect.sync(() => blocked.reports.length === 1));
        assert.include(Cause.pretty(opened.reports[0]!.cause), "has no migration from 1");
        assert.containSubset(yield* getTask(opened, id), {
          version: 1,
          state: { status: "pending" },
        });
        yield* opened.close;
      }),
    ),
  );

  // The new version's schemas refine the old ones, so a result of the right type can still be rejected.
  it.effect(
    "blocks a task migration_failed when its migration produces an input or a checkpoint its schemas reject",
    () =>
      Effect.gen(function* () {
        const refined = (migrate: Task.TaskMigration<string | null, typeof RefinedStep.Type>) =>
          Task.make({
            name: "test.versioned",
            version: 2,
            input: Schema.NullOr(Schema.NonEmptyString),
            checkpoint: RefinedStep,
            result: Schema.String,
            initial: () => ({ phase: "run" }),
            phases: {
              run: (_task, runtime) => runtime.commit(() => Effect.succeed(completed("v2"))),
            },
            abort: (_task, runtime) => runtime.commit(() => Effect.succeed(abortedWith("v2"))),
            migrate,
          });
        const cases = [
          {
            migrate: () => ({ input: "", checkpoint: { phase: "run" } as const }),
            message: "migration produced an input its schema rejects",
          },
          {
            migrate: () => ({ input: null, checkpoint: { phase: "run", note: "" } as const }),
            message: "migration produced a checkpoint its schema rejects",
          },
        ];
        for (const each of cases) {
          yield* run(backend)(
            Effect.gen(function* () {
              const storage = yield* controlledStorage;
              let opened = yield* openHarness(storage, []);
              const id = yield* createIn(opened, versioned(1, "v1"));
              yield* opened.close;

              opened = yield* openHarness(storage, [refined(each.migrate)]);
              yield* opened.harness.resume;
              const blocked = opened;
              yield* eventually(Effect.sync(() => blocked.reports.length === 1));
              assert.strictEqual(opened.reports[0]!.kind, "migration_failed");
              assert.include(Cause.pretty(opened.reports[0]!.cause), each.message);
              const inspected = (yield* opened.harness.inspect).tasks.find(
                (entry) => entry.record.id === id,
              );
              assert.containSubset(inspected?.state, {
                kind: "blocked",
                reason: "migration_failed",
              });
              assert.containSubset(yield* getTask(opened, id), {
                version: 1,
                input: null,
                state: { status: "pending", checkpoint: { phase: "run" } },
              });
              yield* opened.close;
            }),
          );
        }
      }),
  );

  it.effect("settles an aborted blocked task as orphaned and retires its documents", () =>
    run(backend)(
      Effect.gen(function* () {
        const Scratch = Doc.make({
          checkpointWhen: () => false,
          kind: "test.orphan-scratch",
          version: 1,
          scope: "task",
          schema: Schema.Struct({ n: Schema.Number }),
          initial: () => ({ n: 0 }),
        });
        const storage = yield* controlledStorage;
        const harness = yield* openHarness(storage, []);
        const root = yield* harness.harness.ensureRoot();
        const id = yield* root.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            const created = yield* tx.createTask(versioned(1, "x"), null, {
              ownership: { kind: "conversation" },
            });
            (yield* tx.doc(Scratch, created)).n = 1;
            return created;
          }),
        );
        // Before resume: the marking commit settles the blocked task directly.
        assert.deepStrictEqual(yield* harness.harness.abortTask(id), { _tag: "Marked" });
        assert.deepStrictEqual((yield* getTask(harness, id))?.state, {
          status: "terminal",
          outcome: { status: "orphaned", reason: "missing_task" },
        });
        assert.isTrue(Option.isNone(yield* harness.session.snapshot(Scratch, id)));
        yield* harness.close;
      }),
    ),
  );

  it.effect(
    "orphans a reopened abort-marked task without a definition once scheduling resumes",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const storage = yield* controlledStorage;
          let opened = yield* openHarness(storage, [versioned(1, "x")]);
          const id = yield* createIn(opened, versioned(1, "x"));
          assert.deepStrictEqual(yield* opened.harness.abortTask(id), { _tag: "Marked" });
          yield* opened.close;

          opened = yield* openHarness(storage, []);
          assert.containSubset(yield* getTask(opened, id), {
            abortRequested: true,
            state: { status: "pending" },
          });
          const idle = yield* Effect.forkChild(opened.harness.awaitIdle);
          yield* opened.harness.resume;
          yield* Fiber.join(idle);
          assert.deepStrictEqual((yield* getTask(opened, id))?.state, {
            status: "terminal",
            outcome: { status: "orphaned", reason: "missing_task" },
          });
          yield* opened.close;
        }),
      ),
  );
});

describeEachBackend("Harness open", (backend) => {
  // A static registry has no subscription to release, and the Session does not close storage: a failed reconciliation
  // commit fails the layer build, and the Session it opened ends, so no storage call follows the failure.
  it.effect("releases its registry subscription and closes the Session when open fails", () =>
    run(backend)(
      Effect.gen(function* () {
        const storage = yield* controlledStorage;
        const log: Array<string> = [];
        const Stuck = Task.make({
          name: "test.stuck",
          version: 1,
          input: Schema.Null,
          checkpoint: Step,
          result: Schema.Null,
          initial: () => ({ phase: "run" }),
          phases: {
            run: () =>
              Effect.gen(function* () {
                log.push("run");
                // Never returns and ignores its signal.
                return yield* Effect.uninterruptible(Effect.never);
              }),
          },
          abort: () => Effect.void,
        });
        // Leave a running task behind so open has a reconciliation commit to fail. This Harness is never
        // closed: its run ignores its signal, so it is abandoned.
        const first = yield* openAbandoned(storage, [Stuck]);
        yield* createIn(first, Stuck);
        yield* first.harness.resume;
        yield* eventually(Effect.sync(() => log.length === 1));

        const calls = recordCalls(storage);
        storage.failNextCommit("disk full");
        assert.include(yield* failureMessage(Effect.scoped(openHarness(storage, []))), "disk full");
        yield* assertNoCallsAfter(calls, calls.length);
      }),
    ),
  );
});

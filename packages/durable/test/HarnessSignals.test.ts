// Signals and the wakes of parked tasks: delivery results, sleeps on signals and deadlines, `any` waits, kept wakes.
//
// Notes:
// - Signals are declared with a `Schema` codec, so a value the schema does not admit and a value that is not JSON
//   are both `invalid`; schemas that admit non-JSON values cannot be written, since the codec's encoding is JSON.
// - `readSignal` returns an `Option`; a value that cannot be read fails with `InvalidSignal`. "fails reading a value
//   whose migrate throws" covers a migration that throws.
// - A reopened process is a second Harness over the same storage, on every backend.
// - The settable `now` is the `TestClock`; `wake()` is simplified as in `HarnessSleep.test.ts`.
// - "is active in wake() once a task in on is terminal": the registry is static, so the children are left without
//   a definition by reopening with only `Parent`.
import { assert, it } from "@effect/vitest";
import * as Clock from "effect/Clock";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import { ROOT_CONVERSATION_ID, TaskId } from "../src/Ids.ts";
import { type TaskRecord, WakeReason } from "../src/Records.ts";
import { SessionWakeups, type Wake } from "../src/SessionWakeups.ts";
import * as Signal from "../src/Signal.ts";
import * as Task from "../src/Task.ts";
import { Tx } from "../src/Tx.ts";
import { abortedWith, eventually, openHarness, type TestHarness } from "./HarnessSupport.ts";
import {
  type ControlledStorage,
  controlledStorage,
  describeEachBackend,
  flush,
  run,
} from "./SessionSupport.ts";

/** A JSON value passed where `T` is expected, as a value from outside may be. */
const outside = <T>(value: Schema.Json): T =>
  // SAFETY: deliberately unchecked: a delivery must refuse what its schema rejects.
  value as T;

const AnswerValue = Schema.Struct({ text: Schema.String });
const Answer = Signal.make("test.answer", { version: 1, schema: AnswerValue });

const SignalStateOf = <T, I extends Schema.Json>(value: Schema.Codec<T, I>) =>
  Schema.Union([Schema.Struct({ value }), Schema.Struct({ expired: Schema.Literal(true) })]);

const AskInput = Schema.Struct({
  keys: Schema.Array(Schema.String),
  until: Schema.optionalKey(Schema.Number),
});
const AskState = Schema.Union([
  Schema.Struct({ phase: Schema.Literal("ask") }),
  Schema.Struct({ phase: Schema.Literal("answered") }),
]);
const AskResult = Schema.Struct({
  wake: Schema.optionalKey(WakeReason),
  states: Schema.Array(Schema.NullOr(SignalStateOf(AnswerValue))),
});

/** Sleeps on `Answer` under `keys` (and `until`), then completes with its wake and every key's state (`null`: none). */
const Ask = Task.make({
  name: "test.ask",
  version: 1,
  input: AskInput,
  checkpoint: AskState,
  result: AskResult,
  initial: () => ({ phase: "ask" }),
  phases: {
    ask: (task, runtime) =>
      runtime.sleep({
        checkpoint: { phase: "answered" },
        signals: task.input.keys.map((key) => Answer.key(key)),
        until: task.input.until,
      }),
    answered: (task, runtime) =>
      Effect.gen(function* () {
        const states: Array<Signal.SignalState<{ readonly text: string }> | null> = [];
        for (const key of task.input.keys) {
          states.push(Option.getOrNull(yield* runtime.readSignal(Answer, key)));
        }
        const wake = task.state.wake;
        yield* runtime.complete(wake === undefined ? { states } : { wake, states });
      }),
  },
  abort: (_task, runtime) => runtime.commit(Effect.succeed(abortedWith("test"))),
});

const start = (h: TestHarness, input: typeof AskInput.Type) =>
  Effect.gen(function* () {
    const root = yield* h.harness.ensureRoot();
    return yield* root.commit(
      Effect.gen(function* () {
        const tx = yield* Tx;
        return yield* tx.createTask(Ask, input, { ownership: { kind: "conversation" } });
      }),
    );
  });

const createIn = <I extends Schema.Json, S extends { readonly phase: string }, R>(
  h: TestHarness,
  task: Task.Task<I, S, R, never>,
  input: NoInfer<I>,
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

const stateOf = (h: TestHarness, id: TaskId) =>
  h.harness.getTask(id).pipe(Effect.map((record) => Option.getOrUndefined(record)?.state));

const statusIs = (h: TestHarness, id: TaskId, status: string) =>
  eventually(stateOf(h, id).pipe(Effect.map((state) => state?.status === status)));

/** The wake of a `pending` or `running` state. */
const wakeOf = (state: TaskRecord["state"] | undefined): WakeReason | undefined =>
  state !== undefined && (state.status === "pending" || state.status === "running")
    ? state.wake
    : undefined;

/** `Wake` at the clock's `now`, as `{ active, at? }`. */
const wakeNow = (h: TestHarness) =>
  Effect.gen(function* () {
    const now = yield* Clock.currentTimeMillis;
    const wake = yield* h.harness.wake;
    const next = Option.getOrUndefined(wake.nextWakeAt);
    const active = wake.activeWork || (next !== undefined && next <= now);
    return next !== undefined && next > now ? { active, at: next } : { active };
  });

const inactive = (h: TestHarness) =>
  eventually(wakeNow(h).pipe(Effect.map((wake) => !wake.active)));

/** A Harness whose host is told every wake, so the scheduler arms no timer of its own. */
const openWithHost = (storage: ControlledStorage, tasks: ReadonlyArray<Task.Any>) =>
  openHarness(storage, tasks).pipe(
    Effect.provideService(SessionWakeups, { schedule: (_wake: Wake) => Effect.void }),
  );

describeEachBackend("signals", (backend) => {
  it.effect("rejects names with a slash, bad versions, and empty keys", () =>
    Effect.sync(() => {
      assert.throws(
        () => Signal.make("a/b", { version: 1, schema: Schema.String }),
        /without "\/"/,
      );
      assert.throws(
        () => Signal.make("a", { version: 0, schema: Schema.String }),
        /positive integer/,
      );
      assert.throws(() => Answer.key(""), /non-empty/);
      assert.deepStrictEqual(Answer.key("k"), { signal: "test.answer", key: "k" });
    }),
  );

  it.effect("answers not_found, invalid, delivered, duplicate, and closed", () =>
    run(backend)(
      Effect.gen(function* () {
        const h = yield* openHarness(yield* controlledStorage, [Ask]);
        const id = yield* start(h, { keys: ["a"] });
        assert.strictEqual(
          yield* h.harness.signal(TaskId.make(999_999), Answer, "a", { text: "x" }),
          "not_found",
        );
        assert.strictEqual(
          yield* h.harness.signal(id, Answer, "a", outside<{ readonly text: string }>({ text: 1 })),
          "invalid",
        );
        // The schema admits it, but NaN is not JSON.
        const Loose = Signal.make("test.loose", {
          version: 1,
          schema: Schema.Struct({ text: Schema.String, n: Schema.Number }),
        });
        assert.strictEqual(
          yield* h.harness.signal(id, Loose, "a", { text: "x", n: Number.NaN }),
          "invalid",
        );
        assert.strictEqual(yield* h.harness.signal(id, Answer, "a", { text: "yes" }), "delivered");
        assert.strictEqual(
          yield* h.harness.signal(id, Answer, "a", { text: "again" }),
          "duplicate",
        );
        const receipt = yield* h.harness.awaitTask(id);
        assert.strictEqual(receipt.state.outcome.status, "completed");
        assert.strictEqual(yield* h.harness.signal(id, Answer, "b", { text: "late" }), "closed");
        yield* h.close;
      }),
    ),
  );

  it.effect("answers closed for an abort-marked task", () =>
    run(backend)(
      Effect.gen(function* () {
        const h = yield* openHarness(yield* controlledStorage, [Ask]);
        const id = yield* start(h, { keys: ["a"] });
        yield* h.harness.abortTask(id);
        assert.strictEqual(yield* h.harness.signal(id, Answer, "a", { text: "yes" }), "closed");
        yield* h.close;
      }),
    ),
  );

  it.effect("decides deliveries started together in one commit against each other", () =>
    run(backend)(
      Effect.gen(function* () {
        const h = yield* openHarness(yield* controlledStorage, [Ask]);
        const id = yield* start(h, { keys: ["a", "b"] });
        const results = yield* h.harness.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            return yield* Effect.all(
              [
                tx.signal(id, Answer, "a", { text: "first" }),
                tx.signal(id, Answer, "a", { text: "second" }),
                tx.signal(id, Answer, "b", { text: "bee" }),
              ],
              { concurrency: "unbounded" },
            );
          }),
        );
        assert.deepStrictEqual(results, ["delivered", "duplicate", "delivered"]);
        const receipt = yield* h.harness.awaitTask(id);
        assert.deepStrictEqual(receipt.state.outcome, {
          status: "completed",
          result: {
            wake: { reason: "signal", signal: "test.answer", key: "a" },
            states: [{ value: { text: "first" } }, { value: { text: "bee" } }],
          },
        });
        yield* h.close;
      }),
    ),
  );

  it.effect("reads the task, so it fails with ReadAfterWrite after a table write", () =>
    run(backend)(
      Effect.gen(function* () {
        const h = yield* openHarness(yield* controlledStorage, [Ask]);
        const id = yield* start(h, { keys: ["a"] });
        const error = yield* Effect.flip(
          h.harness.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              yield* tx.signal(id, Answer, "a", { text: "one" });
              yield* tx.signal(id, Answer, "b", { text: "two" });
            }),
          ),
        );
        assert.strictEqual(error._tag, "ReadAfterWrite");
        yield* h.close;
      }),
    ),
  );

  it.effect("keeps a value delivered before the sleep, which then wakes at once", () =>
    run(backend)(
      Effect.gen(function* () {
        const h = yield* openHarness(yield* controlledStorage, [Ask]);
        const id = yield* start(h, { keys: ["a"] });
        // Not running yet: the value waits in the task.
        assert.strictEqual(
          yield* h.harness.signal(id, Answer, "a", { text: "early" }),
          "delivered",
        );
        const receipt = yield* h.harness.awaitTask(id);
        assert.deepStrictEqual(receipt.state.outcome, {
          status: "completed",
          result: {
            wake: { reason: "signal", signal: "test.answer", key: "a" },
            states: [{ value: { text: "early" } }],
          },
        });
        yield* h.close;
      }),
    ),
  );

  it.effect(
    "holds no invocation while it waits, and wakes on the first listed key holding a value",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const h = yield* openWithHost(yield* controlledStorage, [Ask]);
          const graph = yield* h.harness.watchTaskGraph;
          yield* Effect.forkChild(Stream.runDrain(graph.frames), { startImmediately: true });
          const id = yield* start(h, { keys: ["a", "b"] });
          yield* h.harness.resume;
          yield* statusIs(h, id, "sleeping");
          yield* inactive(h);
          yield* flush;
          assert.deepStrictEqual(graph.value.tasks[String(id)]?.state, {
            status: "sleeping",
            phase: "answered",
            signals: [Answer.key("a"), Answer.key("b")],
          });
          // Without `until` there is no deadline to wake for.
          assert.deepStrictEqual(yield* wakeNow(h), { active: false });
          const inspection = yield* h.harness.inspect;
          assert.deepStrictEqual(inspection.tasks.find((task) => task.record.id === id)?.state, {
            kind: "sleeping",
          });
          assert.strictEqual(
            yield* h.harness.signal(id, Answer, "b", { text: "bee" }),
            "delivered",
          );
          const receipt = yield* h.harness.awaitTask(id);
          assert.deepStrictEqual(receipt.state.outcome, {
            status: "completed",
            result: {
              wake: { reason: "signal", signal: "test.answer", key: "b" },
              states: [null, { value: { text: "bee" } }],
            },
          });
          yield* h.close;
        }),
      ),
  );

  it.effect("expires its keys at the deadline, so a late delivery is closed", () =>
    run(backend)(
      Effect.gen(function* () {
        yield* TestClock.setTime(1_000);
        const h = yield* openWithHost(yield* controlledStorage, [Ask]);
        const id = yield* start(h, { keys: ["a"], until: 2_000 });
        yield* h.harness.resume;
        yield* statusIs(h, id, "sleeping");
        yield* inactive(h);
        assert.deepStrictEqual(yield* wakeNow(h), { active: false, at: 2_000 });
        yield* TestClock.setTime(2_000);
        yield* h.harness.resume;
        const receipt = yield* h.harness.awaitTask(id);
        assert.deepStrictEqual(receipt.state.outcome, {
          status: "completed",
          result: { wake: { reason: "expired" }, states: [{ expired: true }] },
        });
        assert.strictEqual(yield* h.harness.signal(id, Answer, "a", { text: "late" }), "closed");
        yield* h.close;
      }),
    ),
  );

  it.effect(
    "answers a delivery after the deadline closed and expires the key, before the sleep is reserved",
    () =>
      run(backend)(
        Effect.gen(function* () {
          yield* TestClock.setTime(1_000);
          const h = yield* openWithHost(yield* controlledStorage, [Ask]);
          const id = yield* start(h, { keys: ["a"], until: 2_000 });
          yield* h.harness.resume;
          yield* statusIs(h, id, "sleeping");
          // Due, but no pass ran since: the delivery itself finds the deadline passed.
          yield* TestClock.setTime(2_500);
          assert.strictEqual(yield* h.harness.signal(id, Answer, "a", { text: "late" }), "closed");
          const receipt = yield* h.harness.awaitTask(id);
          assert.deepStrictEqual(receipt.state.outcome, {
            status: "completed",
            result: { wake: { reason: "expired" }, states: [{ expired: true }] },
          });
          yield* h.close;
        }),
      ),
  );

  it.effect(
    "rejects a sleep on a key that already woke a wait, and one without until or signals",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const outcomes: Array<string> = [];
          const Twice = Task.make({
            name: "test.twice",
            version: 1,
            input: Schema.Struct({ signals: Schema.Boolean }),
            checkpoint: Schema.Union([
              Schema.Struct({ phase: Schema.Literal("first") }),
              Schema.Struct({ phase: Schema.Literal("second") }),
            ]),
            result: Schema.Null,
            initial: () => ({ phase: "first" }),
            phases: {
              first: (task, runtime) =>
                runtime.sleep({
                  checkpoint: { phase: "second" },
                  signals: task.input.signals ? [Answer.key("a")] : undefined,
                }),
              second: (_task, runtime) =>
                Effect.gen(function* () {
                  const error = yield* Effect.flip(
                    runtime.sleep({ checkpoint: { phase: "first" }, signals: [Answer.key("a")] }),
                  );
                  outcomes.push(error.message);
                  yield* runtime.complete(null);
                }),
            },
            abort: (_task, runtime) => runtime.commit(Effect.succeed(abortedWith("test"))),
          });
          const h = yield* openHarness(yield* controlledStorage, [Twice]);
          const consumed = yield* createIn(h, Twice, { signals: true });
          yield* h.harness.signal(consumed, Answer, "a", { text: "once" });
          yield* h.harness.awaitTask(consumed);
          assert.deepStrictEqual(outcomes, [
            `Task ${consumed} cannot sleep on signal test.answer key a: it already woke a wait`,
          ]);
          const empty = yield* createIn(h, Twice, { signals: false });
          const receipt = yield* h.harness.awaitTask(empty);
          const outcome = receipt.state.outcome;
          assert.strictEqual(outcome.status, "faulted");
          assert.include(
            outcome.status === "faulted" ? outcome.error.message : "",
            `Task ${empty} cannot sleep without until or signals`,
          );
          yield* h.close;
        }),
      ),
  );

  it.effect(
    "wakes on the first listed key in order, and keeps the other key's value for a later sleep",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const Wakes = Schema.Array(Schema.NullOr(WakeReason));
          const Two = Task.make({
            name: "test.two",
            version: 1,
            input: Schema.Null,
            checkpoint: Schema.Union([
              Schema.Struct({ phase: Schema.Literal("first") }),
              Schema.Struct({ phase: Schema.Literal("second"), wakes: Wakes }),
              Schema.Struct({ phase: Schema.Literal("done"), wakes: Wakes }),
            ]),
            result: Wakes,
            initial: () => ({ phase: "first" }),
            phases: {
              first: (_task, runtime) =>
                runtime.sleep({
                  checkpoint: { phase: "second", wakes: [] },
                  signals: [Answer.key("a"), Answer.key("b")],
                }),
              second: (task, runtime) =>
                runtime.sleep({
                  checkpoint: { phase: "done", wakes: [task.state.wake ?? null] },
                  signals: [Answer.key("b")],
                }),
              done: (task, runtime) =>
                runtime.complete([...task.state.checkpoint.wakes, task.state.wake ?? null]),
            },
            abort: (_task, runtime) => runtime.commit(Effect.succeed(abortedWith("test"))),
          });
          const h = yield* openHarness(yield* controlledStorage, [Two]);
          const id = yield* createIn(h, Two, null);
          // Both before the first sleep; `b` is delivered first, but `a` is listed first.
          const deliver = (key: string, text: string) =>
            h.harness.commit(
              Effect.gen(function* () {
                const tx = yield* Tx;
                return yield* tx.signal(id, Answer, key, { text });
              }),
            );
          yield* deliver("b", "bee");
          yield* deliver("a", "ay");
          const receipt = yield* h.harness.awaitTask(id);
          assert.deepStrictEqual(receipt.state.outcome, {
            status: "completed",
            result: [
              { reason: "signal", signal: "test.answer", key: "a" },
              { reason: "signal", signal: "test.answer", key: "b" },
            ],
          });
          yield* h.close;
        }),
      ),
  );

  it.effect("rejects a sleep on a key that expired", () =>
    run(backend)(
      Effect.gen(function* () {
        yield* TestClock.setTime(1_000);
        const errors: Array<string> = [];
        const Again = Task.make({
          name: "test.again",
          version: 1,
          input: Schema.Null,
          checkpoint: Schema.Union([
            Schema.Struct({ phase: Schema.Literal("first") }),
            Schema.Struct({ phase: Schema.Literal("second") }),
          ]),
          result: Schema.Null,
          initial: () => ({ phase: "first" }),
          phases: {
            first: (_task, runtime) =>
              runtime.sleep({
                checkpoint: { phase: "second" },
                signals: [Answer.key("a")],
                until: 2_000,
              }),
            second: (_task, runtime) =>
              Effect.gen(function* () {
                const error = yield* Effect.flip(
                  runtime.sleep({ checkpoint: { phase: "first" }, signals: [Answer.key("a")] }),
                );
                errors.push(error.message);
                yield* runtime.complete(null);
              }),
          },
          abort: (_task, runtime) => runtime.commit(Effect.succeed(abortedWith("test"))),
        });
        const h = yield* openWithHost(yield* controlledStorage, [Again]);
        const id = yield* createIn(h, Again, null);
        yield* h.harness.resume;
        yield* statusIs(h, id, "sleeping");
        yield* TestClock.setTime(2_000);
        yield* h.harness.resume;
        yield* h.harness.awaitTask(id);
        assert.strictEqual(errors.length, 1);
        assert.include(
          errors[0],
          `Task ${id} cannot sleep on signal test.answer key a: it expired`,
        );
        yield* h.close;
      }),
    ),
  );

  it.effect("ends a sleep on signals at once when aborted", () =>
    run(backend)(
      Effect.gen(function* () {
        const h = yield* openHarness(yield* controlledStorage, [Ask]);
        const id = yield* start(h, { keys: ["a"] });
        yield* h.harness.resume;
        yield* statusIs(h, id, "sleeping");
        yield* h.harness.abortTask(id);
        const receipt = yield* h.harness.awaitTask(id);
        assert.deepStrictEqual(receipt.state.outcome, { status: "aborted", reason: "test" });
        yield* h.close;
      }),
    ),
  );

  it.effect("fails reading a value of a newer version, or of an older one without migrate", () =>
    run(backend)(
      Effect.gen(function* () {
        const Count = Schema.Struct({ n: Schema.Number });
        const V1 = Signal.make("test.versioned", { version: 1, schema: Count });
        const V2 = Signal.make("test.versioned", { version: 2, schema: Count });
        const errors: Array<string> = [];
        const Read = Task.make({
          name: "test.read-versions",
          version: 1,
          input: Schema.Struct({ token: Schema.Literals([1, 2]) }),
          checkpoint: Schema.Struct({ phase: Schema.Literal("read") }),
          result: Schema.Null,
          initial: () => ({ phase: "read" }),
          phases: {
            read: (task, runtime) =>
              Effect.gen(function* () {
                yield* runtime
                  .readSignal(task.input.token === 1 ? V1 : V2, "k")
                  .pipe(
                    Effect.catchTag("InvalidSignal", (error) =>
                      Effect.sync(() => errors.push(error.message)),
                    ),
                  );
                yield* runtime.complete(null);
              }),
          },
          abort: (_task, runtime) => runtime.commit(Effect.succeed(abortedWith("test"))),
        });
        const h = yield* openHarness(yield* controlledStorage, [Read]);
        const newer = yield* createIn(h, Read, { token: 1 });
        yield* h.harness.signal(newer, V2, "k", { n: 2 });
        const older = yield* createIn(h, Read, { token: 2 });
        yield* h.harness.signal(older, V1, "k", { n: 1 });
        yield* h.harness.awaitTask(newer);
        yield* h.harness.awaitTask(older);
        assert.deepStrictEqual(errors, [
          "Signal test.versioned value has version 2, newer than 1",
          "Signal test.versioned has no migration from version 1",
        ]);
        yield* h.close;
      }),
    ),
  );

  it.effect("migrates a value stored by an older version when it is read", () =>
    run(backend)(
      Effect.gen(function* () {
        const Old = Signal.make("test.count", {
          version: 1,
          schema: Schema.Struct({ n: Schema.String }),
        });
        const New = Signal.make("test.count", {
          version: 2,
          schema: Schema.Struct({ n: Schema.Number }),
          migrate: (value) => ({
            // SAFETY: version 1 stored `{ n: string }`.
            n: Number((value as { readonly n: string }).n),
          }),
        });
        const reads: Array<unknown> = [];
        const Read = Task.make({
          name: "test.read",
          version: 1,
          input: Schema.Null,
          checkpoint: Schema.Struct({ phase: Schema.Literal("read") }),
          result: Schema.Null,
          initial: () => ({ phase: "read" }),
          phases: {
            read: (_task, runtime) =>
              Effect.gen(function* () {
                reads.push(Option.getOrUndefined(yield* runtime.readSignal(New, "k")));
                yield* runtime.complete(null);
              }),
          },
          abort: (_task, runtime) => runtime.commit(Effect.succeed(abortedWith("test"))),
        });
        const h = yield* openHarness(yield* controlledStorage, [Read]);
        const id = yield* createIn(h, Read, null);
        assert.strictEqual(yield* h.harness.signal(id, Old, "k", { n: "42" }), "delivered");
        yield* h.harness.awaitTask(id);
        assert.deepStrictEqual(reads, [{ value: { n: 42 } }]);
        yield* h.close;
      }),
    ),
  );

  it.effect("fails reading a value whose migrate throws", () =>
    run(backend)(
      Effect.gen(function* () {
        const Count = Schema.Struct({ n: Schema.Number });
        const Old = Signal.make("test.throwing", { version: 1, schema: Count });
        const New = Signal.make("test.throwing", {
          version: 2,
          schema: Count,
          migrate: () => {
            throw new Error("unexpected version 1 value");
          },
        });
        const errors: Array<string> = [];
        const Read = Task.make({
          name: "test.read-throwing",
          version: 1,
          input: Schema.Null,
          checkpoint: Schema.Struct({ phase: Schema.Literal("read") }),
          result: Schema.Null,
          initial: () => ({ phase: "read" }),
          phases: {
            read: (_task, runtime) =>
              Effect.gen(function* () {
                yield* runtime
                  .readSignal(New, "k")
                  .pipe(
                    Effect.catchTag("InvalidSignal", (error) =>
                      Effect.sync(() => errors.push(error.message)),
                    ),
                  );
                yield* runtime.complete(null);
              }),
          },
          abort: (_task, runtime) => runtime.commit(Effect.succeed(abortedWith("test"))),
        });
        const h = yield* openHarness(yield* controlledStorage, [Read]);
        const id = yield* createIn(h, Read, null);
        yield* h.harness.signal(id, Old, "k", { n: 1 });
        yield* h.harness.awaitTask(id);
        assert.deepStrictEqual(errors, ["Signal test.throwing migration from version 1 failed"]);
        yield* h.close;
      }),
    ),
  );

  it.effect("composes a signal with the task's own state change in one commit", () =>
    run(backend)(
      Effect.gen(function* () {
        const results: Array<Signal.SignalResult> = [];
        const Self = Task.make({
          name: "test.self",
          version: 1,
          input: Schema.Null,
          checkpoint: Schema.Union([
            Schema.Struct({ phase: Schema.Literal("start") }),
            Schema.Struct({ phase: Schema.Literal("woke") }),
          ]),
          result: Schema.NullOr(WakeReason),
          initial: () => ({ phase: "start" }),
          phases: {
            start: (_task, runtime) =>
              runtime.sleep(
                { checkpoint: { phase: "woke" }, signals: [Answer.key("self")] },
                Effect.gen(function* () {
                  const tx = yield* Tx;
                  results.push(yield* tx.signal(runtime.taskId, Answer, "self", { text: "me" }));
                }),
              ),
            woke: (task, runtime) => runtime.complete(task.state.wake ?? null),
          },
          abort: (_task, runtime) => runtime.commit(Effect.succeed(abortedWith("test"))),
        });
        const h = yield* openHarness(yield* controlledStorage, [Self]);
        const id = yield* createIn(h, Self, null);
        const receipt = yield* h.harness.awaitTask(id);
        assert.deepStrictEqual(results, ["delivered"]);
        assert.deepStrictEqual(receipt.state.outcome, {
          status: "completed",
          result: { reason: "signal", signal: "test.answer", key: "self" },
        });
        yield* h.close;
      }),
    ),
  );

  /** Waits `any` on two `Ask` children; once woken, aborts the one still live and completes with its wake. */
  const Parent = Task.make({
    name: "test.any",
    version: 1,
    input: Schema.Null,
    checkpoint: Schema.Union([
      Schema.Struct({ phase: Schema.Literal("spawn") }),
      Schema.Struct({ phase: Schema.Literal("joined"), on: Schema.Array(TaskId) }),
    ]),
    result: Schema.NullOr(WakeReason),
    initial: () => ({ phase: "spawn" }),
    phases: {
      spawn: (_task, runtime) =>
        runtime.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            const own = { ownership: { kind: "task", taskId: runtime.taskId } } as const;
            const on = [
              yield* tx.createTask(Ask, { keys: ["a"] }, own),
              yield* tx.createTask(Ask, { keys: ["a"] }, own),
            ];
            return Task.waitFor(on, { checkpoint: { phase: "joined", on }, policy: "any" });
          }),
        ),
      joined: (task, runtime) =>
        Effect.gen(function* () {
          const wake = task.state.wake;
          for (const id of task.state.checkpoint.on) yield* runtime.abortOwned(TaskId.make(id));
          yield* runtime.complete(wake ?? null);
        }),
    },
    abort: (_task, runtime) => runtime.commit(Effect.succeed(abortedWith("test"))),
  });

  /** Starts `Parent` and waits until it waits on its two children, both asleep; returns its ID and theirs. */
  const startParent = (h: TestHarness) =>
    Effect.gen(function* () {
      const id = yield* createIn(h, Parent, null);
      yield* h.harness.resume;
      yield* statusIs(h, id, "waiting");
      const state = yield* stateOf(h, id);
      const on = state?.status === "waiting" ? state.on : [];
      const [first, second] = on;
      assert.isDefined(first);
      assert.isDefined(second);
      yield* statusIs(h, first!, "sleeping");
      yield* statusIs(h, second!, "sleeping");
      return { id, first: first!, second: second! };
    });

  it.effect(
    "resumes an any wait once one task in on is terminal, with the ended ones as its wake",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const h = yield* openHarness(yield* controlledStorage, [Ask, Parent]);
          const { id, first, second } = yield* startParent(h);
          yield* h.harness.signal(second, Answer, "a", { text: "done" });
          const receipt = yield* h.harness.awaitTask(id);
          assert.deepStrictEqual(receipt.state.outcome, {
            status: "completed",
            result: { reason: "tasks", ended: [second] },
          });
          const firstState = yield* stateOf(h, first);
          assert.strictEqual(
            firstState?.status === "terminal" ? firstState.outcome.status : undefined,
            "aborted",
          );
          yield* h.close;
        }),
      ),
  );

  it.effect(
    "is active in wake() once a task of an any wait is terminal, not while all are live",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const storage = yield* controlledStorage;
          let h = yield* openHarness(storage, [Ask, Parent]);
          const { id, first } = yield* startParent(h);
          yield* h.close;
          // Reopened paused, without `Ask`: the children are blocked, and only the waiter can become active.
          h = yield* openWithHost(storage, [Parent]);
          yield* inactive(h);
          assert.deepStrictEqual(yield* wakeNow(h), { active: false });
          // Aborting a blocked child settles it `orphaned` at once, without scheduling.
          yield* h.harness.abortTask(first);
          assert.strictEqual((yield* stateOf(h, first))?.status, "terminal");
          yield* eventually(wakeNow(h).pipe(Effect.map((wake) => wake.active)));
          yield* h.harness.resume;
          const receipt = yield* h.harness.awaitTask(id);
          assert.deepStrictEqual(receipt.state.outcome, {
            status: "completed",
            result: { reason: "tasks", ended: [first] },
          });
          yield* h.close;
        }),
      ),
  );

  it.effect("keeps a sleep on signals asleep across reopen, active once a value is delivered", () =>
    run(backend)(
      Effect.gen(function* () {
        const storage = yield* controlledStorage;
        let h = yield* openWithHost(storage, [Ask]);
        const id = yield* start(h, { keys: ["a"] });
        yield* h.harness.resume;
        yield* statusIs(h, id, "sleeping");
        yield* h.close;
        h = yield* openWithHost(storage, [Ask]);
        // Scheduling is paused: once open's recovery pass landed, the sleep needs no process.
        yield* inactive(h);
        assert.deepStrictEqual(yield* wakeNow(h), { active: false });
        // A commit of its own delivers without enabling scheduling: the task can run now.
        yield* h.harness.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            return yield* tx.signal(id, Answer, "a", { text: "later" });
          }),
        );
        yield* eventually(wakeNow(h).pipe(Effect.map((wake) => wake.active)));
        yield* h.harness.resume;
        const receipt = yield* h.harness.awaitTask(id);
        const outcome = receipt.state.outcome;
        assert.deepStrictEqual(outcome.status === "completed" ? outcome.result.states : [], [
          { value: { text: "later" } },
        ]);
        yield* h.close;
      }),
    ),
  );

  it.effect("counts no progress for a woken task that commits its checkpoint again", () =>
    run(backend)(
      Effect.gen(function* () {
        const restarts: Array<number | undefined> = [];
        const wakes: Array<WakeReason | undefined> = [];
        const Again = Task.make({
          name: "test.recommit",
          version: 1,
          input: Schema.Null,
          checkpoint: Schema.Union([
            Schema.Struct({ phase: Schema.Literal("woke") }),
            Schema.Struct({ phase: Schema.Literal("next") }),
          ]),
          result: Schema.Null,
          initial: () => ({ phase: "woke" }),
          phases: {
            woke: (_task, runtime) =>
              Effect.gen(function* () {
                yield* runtime.next({ phase: "woke" });
                const record = Option.getOrUndefined(yield* runtime.getTask(runtime.taskId));
                restarts.push(record?.restarts);
                wakes.push(wakeOf(record?.state));
                yield* runtime.next({ phase: "next" });
              }),
            next: (_task, runtime) =>
              Effect.gen(function* () {
                const record = Option.getOrUndefined(yield* runtime.getTask(runtime.taskId));
                restarts.push(record?.restarts);
                yield* runtime.complete(null);
              }),
          },
          abort: (_task, runtime) => runtime.commit(Effect.succeed(abortedWith("test"))),
        });
        const storage = yield* controlledStorage;
        const first = yield* openHarness(storage, [Again]);
        yield* first.harness.ensureRoot();
        yield* first.close;
        // A woken task cut off by a crash: `running` with its wake, as a dead process left it.
        const id = TaskId.make(yield* storage.mintId);
        const record: TaskRecord = {
          id,
          conversationId: ROOT_CONVERSATION_ID,
          kind: "test.recommit",
          version: 1,
          input: null,
          background: false,
          abortRequested: false,
          state: { status: "running", checkpoint: { phase: "woke" }, wake: { reason: "time" } },
        };
        yield* storage.commit([[{ type: "task", value: record }]], () => {});
        const h = yield* openHarness(storage, [Again]);
        assert.deepStrictEqual(yield* stateOf(h, id), {
          status: "pending",
          checkpoint: { phase: "woke" },
          wake: { reason: "time" },
        });
        yield* h.harness.awaitTask(id);
        // The same checkpoint again keeps the count and the wake; the next one starts the count over.
        assert.deepStrictEqual(restarts, [1, undefined]);
        assert.deepStrictEqual(wakes, [{ reason: "time" }]);
        yield* h.close;
      }),
    ),
  );

  it.effect(
    "keeps the wake through a commit of the same checkpoint and a reopen, and drops it with the next checkpoint",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const seen: Array<WakeReason | undefined> = [];
          let runs = 0;
          const Keep = Task.make({
            name: "test.keep",
            version: 1,
            input: Schema.Null,
            checkpoint: Schema.Union([
              Schema.Struct({ phase: Schema.Literal("sleep") }),
              Schema.Struct({ phase: Schema.Literal("woke") }),
              Schema.Struct({ phase: Schema.Literal("after") }),
            ]),
            result: Schema.Null,
            initial: () => ({ phase: "sleep" }),
            phases: {
              sleep: (_task, runtime) =>
                runtime.sleep({ checkpoint: { phase: "woke" }, signals: [Answer.key("k")] }),
              woke: (task, runtime) =>
                Effect.gen(function* () {
                  seen.push(task.state.wake);
                  // Intent: the same checkpoint again keeps the wake.
                  yield* runtime.next({ phase: "woke" });
                  seen.push(
                    wakeOf(Option.getOrUndefined(yield* runtime.getTask(runtime.taskId))?.state),
                  );
                  // The first run is stopped by the close.
                  if (runs++ === 0) return yield* Effect.never;
                  yield* runtime.next({ phase: "after" });
                }),
              after: (_task, runtime) =>
                Effect.gen(function* () {
                  seen.push(
                    wakeOf(Option.getOrUndefined(yield* runtime.getTask(runtime.taskId))?.state),
                  );
                  yield* runtime.complete(null);
                }),
            },
            abort: (_task, runtime) => runtime.commit(Effect.succeed(abortedWith("test"))),
          });
          const storage = yield* controlledStorage;
          let h = yield* openHarness(storage, [Keep]);
          const id = yield* createIn(h, Keep, null);
          yield* h.harness.signal(id, Answer, "k", { text: "go" });
          yield* eventually(Effect.sync(() => seen.length === 2));
          const wake = { reason: "signal", signal: "test.answer", key: "k" } as const;
          assert.deepStrictEqual(yield* stateOf(h, id), {
            status: "running",
            checkpoint: { phase: "woke" },
            wake,
          });
          // Close stops the phase and writes the task back as pending, with its wake.
          yield* h.close;
          assert.deepStrictEqual(seen, [wake, wake]);
          h = yield* openHarness(storage, [Keep]);
          assert.deepStrictEqual(yield* stateOf(h, id), {
            status: "pending",
            checkpoint: { phase: "woke" },
            wake,
          });
          yield* h.harness.awaitTask(id);
          assert.deepStrictEqual(seen, [wake, wake, wake, wake, undefined]);
          yield* h.close;
        }),
      ),
  );
});

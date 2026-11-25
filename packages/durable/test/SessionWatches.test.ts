// Session watches, on every backend. A watch's frames are a single-consumer
// stream: a listener is a consumer fiber over `frames`, and one pull is one
// listener call, folding what is pending; `stop()` closes the watch's scope, and
// `closed` is how the stream ends (empty on Session close, `ended`).
// Interrupting the committing fiber does not interrupt delivery. The three
// retention cases check with `WeakRef`s and a collection.
import { setFlagsFromString } from "node:v8";
import { runInNewContext } from "node:vm";
import { assert, it } from "@effect/vitest";
import * as Delta from "@effect-durable-harness/delta/Delta";
import * as Context from "effect/Context";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import type { WatchFrame } from "../src/Doc.ts";
import { AgentStorage } from "../src/AgentStorage.ts";
import * as Doc from "../src/Doc.ts";
import { IdleTimers } from "../src/IdleTimers.ts";
import type { ConversationId } from "../src/Ids.ts";
import type { SessionKernel } from "../src/internal/session.ts";
import { Tx } from "../src/Tx.ts";
import {
  acquire,
  createConversation,
  describeEachBackend,
  documentChanges,
  flush,
  openSession,
  openTestSession,
  run,
} from "./SessionSupport.ts";

const State = Schema.Struct({
  value: Schema.Number,
  items: Schema.Array(Schema.String),
  retained: Schema.Struct({ label: Schema.String }),
});
type State = typeof State.Type;

const Text = Schema.Struct({ text: Schema.String });
type Text = typeof Text.Type;

const StateDoc = Doc.make({
  checkpointWhen: () => false,
  kind: "watch.state",
  version: 1,
  scope: "session",
  schema: State,
  initial: () => ({ value: 0, items: ["a", "b"], retained: { label: "stable" } }),
});

const KeysDoc = Doc.make({
  checkpointWhen: () => false,
  kind: "watch.keys",
  version: 1,
  scope: "session",
  schema: Schema.Struct({ keys: Schema.Record(Schema.String, Schema.Number) }),
  initial: () => ({ keys: {} }),
});

const createState = Effect.gen(function* () {
  const harness = yield* openTestSession();
  yield* harness.session.commit(
    Effect.gen(function* () {
      const tx = yield* Tx;
      yield* tx.doc(StateDoc);
    }),
  );
  return harness;
});

const setValue = (session: SessionKernel, value: number) =>
  session.commit(
    Effect.gen(function* () {
      const tx = yield* Tx;
      (yield* tx.doc(StateDoc)).value = value;
    }),
  );

const retire = (session: SessionKernel) =>
  session.commit(
    Effect.gen(function* () {
      const tx = yield* Tx;
      yield* tx.retireDoc(StateDoc);
    }),
  );

describeEachBackend("Session document watches", (backend) => {
  it.effect("never creates an absent document", () =>
    run(backend)(
      Effect.gen(function* () {
        const { session, storage } = yield* openTestSession();
        const commits = storage.commits.length;
        assert.isTrue(Option.isNone(yield* session.watchDoc(StateDoc)));
        assert.lengthOf(storage.commits, commits);
        assert.strictEqual(storage.mintCount, 0);
      }),
    ),
  );

  it.effect(
    "keeps the acquisition revision until start, then delivers the commits since in one call",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const { session, publications } = yield* createState;
          const { value: watch, release } = yield* acquire(session.watchDoc(StateDoc));
          const initial = watch.value;
          yield* setValue(session, 1);
          const firstPublished = documentChanges(publications.at(-1)!)[0]!;
          yield* setValue(session, 2);
          const secondPublished = documentChanges(publications.at(-1)!)[0]!;
          assert.strictEqual(watch.value, initial);

          const deliveries: Array<WatchFrame<State>> = [];
          const consumer = yield* watch.frames.pipe(
            Stream.tap((frame) => Effect.sync(() => assert.strictEqual(watch.value, frame.value))),
            Stream.runForEach((frame) => Effect.sync(() => deliveries.push(frame))),
            Effect.forkChild,
          );
          yield* flush;
          // Both commits set `value`: folded, the second set replaces the first.
          assert.deepStrictEqual(firstPublished.ops, [["s", ["value"], 1]]);
          assert.deepStrictEqual(deliveries, [
            { value: secondPublished.value!, ops: secondPublished.ops, retired: false },
          ]);
          assert.strictEqual(deliveries[0]!.value, secondPublished.value);
          assert.strictEqual(initial.value, 0);

          // A listener that keeps up gets each commit's ops as published.
          yield* setValue(session, 3);
          yield* flush;
          const thirdPublished = documentChanges(publications.at(-1)!)[0]!;
          assert.lengthOf(deliveries, 2);
          assert.strictEqual(deliveries[1]!.value, thirdPublished.value);
          assert.strictEqual(deliveries[1]!.ops, thirdPublished.ops);
          yield* release;
          yield* Fiber.interrupt(consumer);
        }),
      ),
  );

  it.effect(
    "serializes callbacks and folds the commits made while one is in flight into the next",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const { session } = yield* createState;
          const { value: watch, release } = yield* acquire(session.watchDoc(StateDoc));
          const entered = yield* Deferred.make<void>();
          const proceed = yield* Deferred.make<void>();
          const values: Array<number> = [];
          const batches: Array<Delta.Delta> = [];
          let active = 0;
          let maxActive = 0;
          const consumer = yield* watch.frames.pipe(
            Stream.take(2),
            Stream.runForEach((frame) =>
              Effect.gen(function* () {
                active++;
                maxActive = Math.max(maxActive, active);
                values.push(frame.value.value);
                batches.push(frame.ops);
                if (values.length === 1) {
                  yield* Deferred.succeed(entered, undefined);
                  yield* Deferred.await(proceed);
                }
                active--;
              }),
            ),
            Effect.forkChild,
          );
          yield* setValue(session, 1);
          yield* Deferred.await(entered);
          for (let value = 2; value <= 20; value++) yield* setValue(session, value);
          assert.deepStrictEqual(values, [1]);
          yield* Deferred.succeed(proceed, undefined);
          yield* Fiber.join(consumer);
          assert.strictEqual(maxActive, 1);
          assert.deepStrictEqual(values, [1, 20]);
          assert.deepStrictEqual(batches[1], [["s", ["value"], 20]]);
          yield* release;
        }),
      ),
  );

  it.effect("allows a listener to initiate a later Session commit", () =>
    run(backend)(
      Effect.gen(function* () {
        const { session } = yield* createState;
        const { value: watch, release } = yield* acquire(session.watchDoc(StateDoc));
        const values: Array<number> = [];
        const consumer = yield* watch.frames.pipe(
          Stream.take(2),
          Stream.runForEach((frame) =>
            Effect.gen(function* () {
              const current = frame.value.value;
              values.push(current);
              if (current === 1) yield* setValue(session, 2);
            }),
          ),
          Effect.forkChild,
        );
        yield* setValue(session, 1);
        yield* Fiber.join(consumer);
        assert.deepStrictEqual(values, [1, 2]);
        yield* release;
      }),
    ),
  );

  it.effect("folds 101 pending commits of one field into one set", () =>
    run(backend)(
      Effect.gen(function* () {
        const { session } = yield* createState;
        const { value: watch, release } = yield* acquire(session.watchDoc(StateDoc));
        for (let value = 1; value <= 101; value++) yield* setValue(session, value);
        const [frame] = yield* watch.frames.pipe(Stream.take(1), Stream.runCollect);
        assert.strictEqual(frame!.value.value, 101);
        assert.deepStrictEqual(frame!.ops, [["s", ["value"], 101]]);
        yield* release;
      }),
    ),
  );

  it.effect("replaces the root once more than 10,000 ops are pending after folding", () =>
    run(backend)(
      Effect.gen(function* () {
        const { session } = yield* openTestSession();
        yield* session.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            yield* tx.doc(KeysDoc);
          }),
        );
        const { value: watch, release } = yield* acquire(session.watchDoc(KeysDoc));
        // Sets of distinct keys, in commits small enough to stay ops: 4,000 + 4,000 + 2,001 pending ops.
        for (const [from, to] of [
          [0, 4_000],
          [4_000, 8_000],
          [8_000, 10_001],
        ] as const) {
          yield* session.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              const state = yield* tx.doc(KeysDoc);
              for (let index = from; index < to; index++) state.keys[`k${index}`] = index;
            }),
          );
        }
        const [frame] = yield* watch.frames.pipe(Stream.take(1), Stream.runCollect);
        assert.deepStrictEqual(frame, {
          value: watch.value,
          ops: [["r", watch.value]],
          retired: false,
        });
        assert.lengthOf(Object.keys(watch.value.keys), 10_001);
        yield* release;
      }),
    ),
  );

  it.effect("never folds the in-flight frame into the next call", () =>
    run(backend)(
      Effect.gen(function* () {
        const { session } = yield* createState;
        const { value: watch, release } = yield* acquire(session.watchDoc(StateDoc));
        const entered = yield* Deferred.make<void>();
        const proceed = yield* Deferred.make<void>();
        const deliveries: Array<WatchFrame<State>> = [];
        const consumer = yield* watch.frames.pipe(
          Stream.take(2),
          Stream.runForEach((frame) =>
            Effect.gen(function* () {
              deliveries.push(frame);
              if (deliveries.length === 1) {
                yield* Deferred.succeed(entered, undefined);
                yield* Deferred.await(proceed);
              }
            }),
          ),
          Effect.forkChild,
        );
        yield* setValue(session, 1);
        yield* Deferred.await(entered);
        for (let value = 2; value <= 102; value++) yield* setValue(session, value);
        yield* Deferred.succeed(proceed, undefined);
        yield* Fiber.join(consumer);
        assert.strictEqual(deliveries[0]!.value.value, 1);
        assert.deepStrictEqual(
          { value: deliveries[1]!.value.value, ops: deliveries[1]!.ops },
          { value: 102, ops: [["s", ["value"], 102]] },
        );
        assert.lengthOf(deliveries, 2);
        yield* flush;
        // Nothing else was pending.
        yield* release;
      }),
    ),
  );

  it.effect(
    "folds a retirement into the pending commits' call, which reports it with the last value, and goes on",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const { session } = yield* createState;
          const { value: watch, release } = yield* acquire(session.watchDoc(StateDoc));
          for (let value = 1; value <= 100; value++) yield* setValue(session, value);
          const last = Option.getOrThrow(yield* session.snapshot(StateDoc));
          yield* retire(session);
          const [first] = yield* watch.frames.pipe(Stream.take(1), Stream.runCollect);
          assert.deepStrictEqual(first, {
            value: last,
            ops: [["s", ["value"], 100]],
            retired: true,
          });
          assert.strictEqual(first!.value, last);
          assert.strictEqual(watch.value, last);
          yield* setValue(session, 7);
          const [second] = yield* watch.frames.pipe(Stream.take(1), Stream.runCollect);
          assert.deepStrictEqual(second, {
            value: { ...last, value: 7 },
            ops: [["r", { ...last, value: 7 }]],
            retired: false,
          });
          yield* release;
        }),
      ),
  );

  it.effect("delivers replayable structural no-op commits instead of suppressing them", () =>
    run(backend)(
      Effect.gen(function* () {
        const { session } = yield* createState;
        const { value: watch, release } = yield* acquire(session.watchDoc(StateDoc));
        const initial = watch.value;
        yield* session.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            const state = yield* tx.doc(StateDoc);
            const first = state.items.shift()!;
            state.items.unshift(first);
          }),
        );
        const newest = Option.getOrThrow(yield* session.snapshot(StateDoc));
        assert.notStrictEqual(newest, initial);
        assert.deepStrictEqual(newest, initial);
        const [frame] = yield* watch.frames.pipe(Stream.take(1), Stream.runCollect);
        assert.strictEqual(frame!.value, newest);
        assert.isAbove(frame!.ops.length, 0);
        yield* release;
      }),
    ),
  );

  it.effect("interrupting the committing fiber does not interrupt delivery", () =>
    run(backend)(
      Effect.gen(function* () {
        const { session } = yield* createState;
        const { value: watch, release } = yield* acquire(session.watchDoc(StateDoc));
        const entered = yield* Deferred.make<void>();
        const proceed = yield* Deferred.make<void>();
        const consumer = yield* watch.frames.pipe(
          Stream.take(1),
          Stream.runForEach(() =>
            Deferred.succeed(entered, undefined).pipe(Effect.andThen(Deferred.await(proceed))),
          ),
          Effect.forkChild,
        );
        const committed = yield* Deferred.make<void>();
        const producer = yield* setValue(session, 1).pipe(
          Effect.andThen(Deferred.succeed(committed, undefined)),
          Effect.andThen(Effect.never),
          Effect.forkChild,
        );
        yield* Deferred.await(committed);
        yield* Deferred.await(entered);
        yield* Fiber.interrupt(producer);
        yield* Deferred.succeed(proceed, undefined);
        // The delivery finishes in the consumer's fiber, untouched by the producer's interruption.
        yield* Fiber.join(consumer);
        yield* release;
      }),
    ),
  );

  it.effect("keeps earlier immutable revisions stable", () =>
    run(backend)(
      Effect.gen(function* () {
        const { session } = yield* createState;
        const { value: watch, release } = yield* acquire(session.watchDoc(StateDoc));
        const initial = watch.value;
        yield* setValue(session, 1);
        const [frame] = yield* watch.frames.pipe(Stream.take(1), Stream.runCollect);
        assert.strictEqual(frame!.value, watch.value);
        assert.notStrictEqual(frame!.value, initial);
        assert.strictEqual(frame!.value.retained, initial.retained);
        assert.strictEqual(initial.value, 0);
        yield* release;
      }),
    ),
  );

  it.effect(
    "reports a change and a retirement in one commit as the retirement, with the value it was given before",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const { session, storage } = yield* createState;
          const { value: watch, release } = yield* acquire(session.watchDoc(StateDoc));
          const before = watch.value;
          yield* session.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              (yield* tx.doc(StateDoc)).value = 5;
              yield* tx.retireDoc(StateDoc);
            }),
          );
          // Storage got the change, then the retirement; the commit publishes the retirement only.
          assert.deepStrictEqual(
            storage.commits.at(-1)!.map((write) => write.type),
            ["document.change", "document.retire"],
          );
          const [frame] = yield* watch.frames.pipe(Stream.take(1), Stream.runCollect);
          assert.deepStrictEqual(frame, { value: before, ops: [], retired: true });
          assert.strictEqual(frame!.value, before);
          yield* release;
        }),
      ),
  );

  it.effect(
    "follows its address across a retirement and a recreation in one commit, in one call",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const { session } = yield* createState;
          const { value: watch, release } = yield* acquire(session.watchDoc(StateDoc));
          const changes: Array<WatchFrame<State>> = [];
          const consumer = yield* watch.frames.pipe(
            Stream.runForEach((frame) => Effect.sync(() => changes.push(frame))),
            Effect.forkChild,
          );
          yield* session.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              yield* tx.retireDoc(StateDoc);
              (yield* tx.doc(StateDoc)).value = 10;
            }),
          );
          yield* flush;
          const recreated = Option.getOrThrow(yield* session.snapshot(StateDoc));
          assert.lengthOf(changes, 1);
          assert.deepStrictEqual(changes[0], {
            value: recreated,
            ops: [["r", recreated]],
            retired: true,
          });
          yield* setValue(session, 11);
          yield* flush;
          assert.deepStrictEqual(changes[1], {
            value: watch.value,
            ops: [["s", ["value"], 11]],
            retired: false,
          });
          assert.strictEqual(watch.value.value, 11);
          // The watch goes on: its stream has not ended.
          assert.isUndefined(consumer.pollUnsafe());
          yield* release;
          yield* Fiber.join(consumer);
        }),
      ),
  );

  it.effect("Session close discards retirement buffered before start", () =>
    run(backend)(
      Effect.gen(function* () {
        const { session } = yield* createState;
        const { value: watch } = yield* acquire(session.watchDoc(StateDoc));
        const baseline = watch.value;
        yield* retire(session);
        yield* session.close;
        // `closed` settles `ended`: the stream ends with no frame.
        assert.deepStrictEqual(yield* Stream.runCollect(watch.frames), []);
        assert.strictEqual(watch.value, baseline);
      }),
    ),
  );

  it.effect("Session close discards retirement behind an in-flight callback", () =>
    run(backend)(
      Effect.gen(function* () {
        const { session } = yield* createState;
        const { value: watch } = yield* acquire(session.watchDoc(StateDoc));
        const entered = yield* Deferred.make<void>();
        const proceed = yield* Deferred.make<void>();
        const values: Array<number> = [];
        const consumer = yield* watch.frames.pipe(
          Stream.runForEach((frame) =>
            Effect.gen(function* () {
              values.push(frame.value.value);
              yield* Deferred.succeed(entered, undefined);
              yield* Deferred.await(proceed);
            }),
          ),
          Effect.forkChild,
        );
        yield* setValue(session, 1);
        yield* Deferred.await(entered);
        yield* retire(session);
        yield* session.close;
        yield* Deferred.succeed(proceed, undefined);
        yield* Fiber.join(consumer);
        assert.deepStrictEqual(values, [1]);
      }),
    ),
  );

  it.effect("closing the scope twice is harmless and a stopped watch yields no frames", () =>
    run(backend)(
      Effect.gen(function* () {
        const { session } = yield* createState;
        const started = yield* acquire(session.watchDoc(StateDoc));
        yield* started.release;
        yield* started.release;
        yield* setValue(session, 1);
        assert.deepStrictEqual(yield* Stream.runCollect(started.value.frames), []);
      }),
    ),
  );

  it.effect("cancels acquisition without leaking a registered watch", () =>
    run(backend)(
      Effect.gen(function* () {
        const { session, storage } = yield* createState;
        yield* session.unloadDocuments;
        const gate = yield* storage.holdFindDocument;
        const acquisition = yield* acquire(session.watchDoc(StateDoc)).pipe(Effect.forkChild);
        yield* gate.entered;
        // A storage call runs to completion once started, so the interruption takes effect after the read.
        const interrupting = yield* Effect.forkChild(Fiber.interrupt(acquisition), {
          startImmediately: true,
        });
        yield* gate.release;
        yield* Fiber.join(interrupting);
        assert.isTrue((yield* Fiber.await(acquisition))._tag === "Failure");
        assert.strictEqual(session.documentCacheSizes.attached, 0);
        yield* session.close;
      }),
    ),
  );

  it.effect("cancels future delivery without aborting an in-flight callback", () =>
    run(backend)(
      Effect.gen(function* () {
        const { session } = yield* createState;
        const { value: watch, release } = yield* acquire(session.watchDoc(StateDoc));
        const entered = yield* Deferred.make<void>();
        const proceed = yield* Deferred.make<void>();
        let finished = false;
        const consumer = yield* watch.frames.pipe(
          Stream.runForEach(() =>
            Effect.gen(function* () {
              yield* Deferred.succeed(entered, undefined);
              yield* Deferred.await(proceed);
              finished = true;
            }),
          ),
          Effect.forkChild,
        );
        yield* setValue(session, 1);
        yield* Deferred.await(entered);
        yield* release;
        yield* setValue(session, 2);
        yield* Deferred.succeed(proceed, undefined);
        // The in-flight callback finishes; no frame follows the stop.
        yield* Fiber.join(consumer);
        assert.isTrue(finished);
      }),
    ),
  );

  it.effect("Session close stops future delivery without joining an in-flight callback", () =>
    run(backend)(
      Effect.gen(function* () {
        const { session } = yield* createState;
        const { value: watch } = yield* acquire(session.watchDoc(StateDoc));
        const entered = yield* Deferred.make<void>();
        const proceed = yield* Deferred.make<void>();
        const consumer = yield* watch.frames.pipe(
          Stream.runForEach(() =>
            Deferred.succeed(entered, undefined).pipe(Effect.andThen(Deferred.await(proceed))),
          ),
          Effect.forkChild,
        );
        yield* setValue(session, 1);
        yield* Deferred.await(entered);
        // Close completes while the callback is still running.
        yield* session.close;
        assert.deepStrictEqual(yield* session.closed, { reason: "closed" });
        yield* Deferred.succeed(proceed, undefined);
        yield* Fiber.join(consumer);
      }),
    ),
  );

  it.effect("settles listener failure on only the affected watch", () =>
    run(backend)(
      Effect.gen(function* () {
        const { session } = yield* createState;
        const { value: failed } = yield* acquire(session.watchDoc(StateDoc));
        const { value: healthy, release } = yield* acquire(session.watchDoc(StateDoc));
        const failing = yield* failed.frames.pipe(
          Stream.runForEach(() => Effect.fail("listener failed")),
          Effect.flip,
          Effect.forkChild,
        );
        const healthyConsumer = yield* healthy.frames.pipe(
          Stream.take(1),
          Stream.runCollect,
          Effect.forkChild,
        );
        yield* setValue(session, 1);
        assert.strictEqual(yield* Fiber.join(failing), "listener failed");
        assert.lengthOf(yield* Fiber.join(healthyConsumer), 1);
        yield* release;
      }),
    ),
  );

  it.effect("hydrates migration without writing and observes the later exact edit", () =>
    run(backend)(
      Effect.gen(function* () {
        const Old = Doc.make({
          checkpointWhen: () => false,
          kind: "watch.migration",
          version: 1,
          scope: "session",
          schema: Schema.Struct({ value: Schema.Number }),
          initial: () => ({ value: 3 }),
        });
        const Current = Doc.make({
          checkpointWhen: () => false,
          kind: "watch.migration",
          version: 2,
          scope: "session",
          schema: Schema.Struct({ value: Schema.Number, migrated: Schema.Boolean }),
          initial: () => ({ value: 0, migrated: false }),
          migrate: (value) => ({ value: Number(value["value"]), migrated: true }),
        });
        const { session, storage } = yield* openTestSession();
        yield* session.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            yield* tx.doc(Old);
          }),
        );
        yield* session.unloadDocuments;
        const commits = storage.commits.length;
        const { value: watch, release } = yield* acquire(session.watchDoc(Current));
        assert.deepStrictEqual(watch.value, { value: 3, migrated: true });
        assert.lengthOf(storage.commits, commits);
        const values: Array<number> = [];
        const consumer = yield* watch.frames.pipe(
          Stream.runForEach((frame) => Effect.sync(() => values.push(frame.value.value))),
          Effect.forkChild,
        );
        yield* session.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            yield* tx.doc(Current);
          }),
        );
        yield* flush;
        assert.deepStrictEqual(values, []);
        yield* session.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            (yield* tx.doc(Current)).value = 4;
          }),
        );
        yield* flush;
        assert.deepStrictEqual(values, [4]);
        yield* release;
        yield* Fiber.join(consumer);
      }),
    ),
  );

  it.effect("can replay every delivered exact operation batch from the acquisition revision", () =>
    run(backend)(
      Effect.gen(function* () {
        const { session } = yield* createState;
        const { value: watch, release } = yield* acquire(session.watchDoc(StateDoc));
        let replica: Readonly<State> = watch.value;
        const consumer = yield* watch.frames.pipe(
          Stream.take(1),
          Stream.runForEach((frame) =>
            Effect.sync(() => {
              replica = Delta.applyImmutableUnsafe(replica, frame.ops);
              assert.deepStrictEqual(replica, frame.value);
            }),
          ),
          Effect.forkChild,
        );
        yield* session.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            const state = yield* tx.doc(StateDoc);
            state.value = 3;
            state.items.splice(0, 1);
          }),
        );
        yield* Fiber.join(consumer);
        assert.deepStrictEqual(replica, watch.value);
        yield* release;
      }),
    ),
  );

  it.effect("continues after the tracker cache unloads", () =>
    run(backend)(
      Effect.gen(function* () {
        const { session, storage } = yield* createState;
        const { value: watch, release } = yield* acquire(session.watchDoc(StateDoc));
        const baseline = watch.value;
        const reads = storage.documentReadCount;
        yield* session.unloadDocuments;
        const reloaded = Option.getOrThrow(yield* session.snapshot(StateDoc));
        assert.deepStrictEqual(reloaded, baseline);
        assert.notStrictEqual(reloaded, baseline);
        assert.isAbove(storage.documentReadCount, reads);
        const consumer = yield* watch.frames.pipe(
          Stream.take(1),
          Stream.runCollect,
          Effect.forkChild,
        );
        yield* setValue(session, 7);
        yield* Fiber.join(consumer);
        assert.strictEqual(watch.value.value, 7);
        yield* release;
      }),
    ),
  );

  it.effect("ignores the same kind in another conversation and under another family key", () =>
    run(backend)(
      Effect.gen(function* () {
        const Notes = Doc.make({
          checkpointWhen: () => false,
          kind: "watch.scoped-notes",
          version: 1,
          scope: "conversation",
          history: "latest",
          fork: "initial",
          schema: Text,
          initial: () => ({ text: "" }),
        });
        const Members = Doc.family({
          checkpointWhen: () => false,
          kind: "watch.members",
          version: 1,
          family: true,
          scope: "session",
          schema: Text,
          initial: (_seed: null) => ({ text: "" }),
        });
        const { session } = yield* openTestSession();
        const watched = yield* createConversation(session);
        const other = yield* createConversation(session);
        const write = (conversationId: ConversationId, key: string, text: string) =>
          session.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              (yield* tx.doc(Notes, conversationId)).text = text;
              (yield* tx.doc(Members, key, null)).text = text;
            }),
          );
        yield* write(watched, "watched", "a");
        const notes = yield* acquire(session.watchDoc(Notes, watched));
        const member = yield* acquire(session.watchDoc(Members, "watched"));
        const changes: Array<WatchFrame<Text>> = [];
        const consumers = yield* Effect.forEach([notes.value, member.value], (watch) =>
          watch.frames.pipe(
            Stream.runForEach((frame) => Effect.sync(() => changes.push(frame))),
            Effect.forkChild,
          ),
        );
        // The same kinds at other addresses: created, changed, retired, and created again.
        yield* write(other, "other", "other");
        yield* session.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            yield* tx.retireDoc(Notes, other);
            yield* tx.retireDoc(Members, "other");
          }),
        );
        yield* write(other, "other", "again");
        yield* flush;
        assert.deepStrictEqual(changes, []);
        assert.strictEqual(session.documentCacheSizes.attached, 2);
        yield* notes.release;
        yield* member.release;
        yield* Effect.forEach(consumers, Fiber.join);
      }),
    ),
  );

  it.effect("takes the version of a recreation written by a newer definition, then its ops", () =>
    run(backend)(
      Effect.gen(function* () {
        const Old = Doc.make({
          checkpointWhen: () => false,
          kind: "watch.recreated-version",
          version: 1,
          scope: "session",
          schema: Schema.Struct({ value: Schema.Number }),
          initial: () => ({ value: 1 }),
        });
        const Current = Doc.make({
          checkpointWhen: () => false,
          kind: "watch.recreated-version",
          version: 2,
          scope: "session",
          schema: Schema.Struct({ value: Schema.Number, migrated: Schema.Boolean }),
          initial: () => ({ value: 0, migrated: false }),
          migrate: (value) => ({ value: Number(value["value"]), migrated: true }),
        });
        const { session } = yield* openTestSession();
        yield* session.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            yield* tx.doc(Old);
          }),
        );
        const { value: watch, release } = yield* acquire(session.watchDoc(Old));
        const changes: Array<WatchFrame<unknown>> = [];
        const consumer = yield* watch.frames.pipe(
          Stream.runForEach((frame) => Effect.sync(() => changes.push(frame))),
          Effect.forkChild,
        );
        yield* session.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            yield* tx.retireDoc(Current);
            (yield* tx.doc(Current)).value = 5;
          }),
        );
        yield* flush;
        yield* session.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            (yield* tx.doc(Current)).value = 6;
          }),
        );
        yield* flush;
        assert.deepStrictEqual(changes, [
          {
            value: { value: 5, migrated: false },
            ops: [["r", { value: 5, migrated: false }]],
            retired: true,
          },
          { value: { value: 6, migrated: false }, ops: [["s", ["value"], 6]], retired: false },
        ]);
        yield* release;
        yield* Fiber.join(consumer);
      }),
    ),
  );

  // On `sqlite-node` the case fails outside effect-durable-harness: `@effect/sql-sqlite-node`'s statement cache keeps the fiber
  // that prepared a statement, and that fiber's stack frame keeps the arguments of the traced call that ran it,
  // the commit's writes (found with a heap snapshot). The kernel keeps nothing of the earlier incarnation.
  it.effect.skipIf(backend.name === "sqlite")(
    "keeps no earlier incarnation alive after a retirement and a recreation, while it goes on",
    () =>
      run(backend)(
        Effect.gen(function* () {
          /** Watch the document, retire and recreate it, and deliver the new value; `drop` then unloads the cache. */
          const follow = (session: SessionKernel, drop: boolean) =>
            Effect.gen(function* () {
              yield* session.commit(
                Effect.gen(function* () {
                  const tx = yield* Tx;
                  (yield* tx.doc(StateDoc)).retained = { label: "first incarnation" };
                }),
              );
              const watched = yield* acquire(session.watchDoc(StateDoc));
              const first = new WeakRef(watched.value.value.retained);
              const consumer = yield* watched.value.frames.pipe(Stream.runDrain, Effect.forkChild);
              yield* session.commit(
                Effect.gen(function* () {
                  const tx = yield* Tx;
                  yield* tx.retireDoc(StateDoc);
                  (yield* tx.doc(StateDoc)).value = 2;
                }),
              );
              yield* flush;
              if (drop) yield* session.unloadDocuments;
              return { watched, first, consumer };
            });
          // Retention 0 evicts the retired incarnation at once; the default keeps the cache until it is unloaded.
          for (const [retention, drop] of [
            [Duration.zero, false],
            [Duration.minutes(10), true],
          ] as const) {
            // A plain Session: the test Session's recorders keep every published value.
            const session = yield* openSession(yield* AgentStorage, {
              documentRetention: () => retention,
            });
            const { watched, first, consumer } = yield* follow(session, drop);
            yield* collect;
            assert.strictEqual(watched.value.value.value, 2);
            assert.deepStrictEqual(watched.value.value.retained, { label: "stable" });
            assert.isUndefined(first.deref(), `retention ${Duration.toMillis(retention)}`);
            yield* watched.release;
            yield* Fiber.join(consumer);
          }
        }),
      ),
  );

  it.effect(
    "keeps a watch of a retired conversation document until it is stopped, then nothing of it",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const Notes = Doc.make({
            checkpointWhen: () => false,
            kind: "watch.notes",
            version: 1,
            scope: "conversation",
            history: "latest",
            fork: "initial",
            schema: Text,
            initial: () => ({ text: "" }),
          });
          const { session } = yield* openTestSession();
          const conversationId = yield* createConversation(session);
          /** A consumed watch of a document that is then retired; `stop` says whether to stop it first. */
          const watchRetired = (stop: boolean) =>
            Effect.gen(function* () {
              yield* session.commit(
                Effect.gen(function* () {
                  const tx = yield* Tx;
                  (yield* tx.doc(Notes, conversationId)).text = "kept";
                }),
              );
              const watched = yield* acquire(session.watchDoc(Notes, conversationId));
              const consumer = yield* watched.value.frames.pipe(Stream.runDrain, Effect.forkChild);
              yield* session.commit(
                Effect.gen(function* () {
                  const tx = yield* Tx;
                  yield* tx.retireDoc(Notes, conversationId);
                }),
              );
              yield* flush;
              if (stop) {
                yield* watched.release;
                yield* Fiber.join(consumer);
              }
              return { watch: new WeakRef(watched.value), release: watched.release };
            });
          // A watch follows its address, so a forgotten one stays until it is stopped or the Session ends.
          const forgotten = yield* watchRetired(false);
          yield* collect;
          assert.isDefined(forgotten.watch.deref());
          yield* forgotten.release;

          const stopped = yield* watchRetired(true);
          yield* collect;
          assert.isUndefined(stopped.watch.deref());
        }),
      ),
  );

  it.effect(
    "retains neither its Session nor its listener once stopped, while still referenced",
    () =>
      run(backend)(
        Effect.gen(function* () {
          /**
           * A watch of a Session over a storage nothing else references; `stop` says whether to stop it before
           * returning. The Session's scope is dropped, never closed.
           */
          const watchAlone = (stop: boolean) =>
            Effect.gen(function* () {
              const scope = yield* Scope.make();
              const storage = Context.get(
                yield* Layer.build(Layer.fresh(backend.layer)).pipe(Scope.provide(scope)),
                AgentStorage,
              );
              // No eviction timer, so nothing but the watch can keep the Session alive.
              const session = yield* openSession(storage).pipe(
                Scope.provide(scope),
                Effect.provideService(IdleTimers, { enabled: false }),
              );
              yield* session.commit(
                Effect.gen(function* () {
                  const tx = yield* Tx;
                  yield* tx.doc(StateDoc);
                }),
              );
              const watched = yield* acquire(session.watchDoc(StateDoc));
              // A closure per watch, so its collection is this watch's.
              // oxlint-disable-next-line unicorn/consistent-function-scoping
              const listener = (_frame: WatchFrame<State>) => Effect.void;
              const consumer = yield* watched.value.frames.pipe(
                Stream.runForEach(listener),
                Effect.forkChild,
              );
              yield* setValue(session, 1);
              yield* flush;
              if (stop) {
                yield* watched.release;
                yield* Fiber.join(consumer);
              }
              if (stop) {
                return {
                  watch: watched.value,
                  session: new WeakRef(storage),
                  listener: new WeakRef(listener),
                };
              }
              return {
                watch: watched.value,
                release: watched.release,
                close: Scope.close(scope, Exit.void),
                // The Session kernel is a record of functions over the Session's state, so
                // the state its closures share is followed through the storage they hold.
                session: new WeakRef(storage),
                listener: new WeakRef(listener),
              };
            });

          // Control: a running watch keeps its Session, so the collection below is real.
          const running = yield* watchAlone(false);
          yield* collect;
          assert.isDefined(running.session.deref());
          yield* running.release!;
          yield* running.close!;

          const stopped = yield* watchAlone(true);
          yield* collect;
          assert.isUndefined(stopped.session.deref());
          assert.isUndefined(stopped.listener.deref());
          // What it delivered last stays readable.
          assert.strictEqual(stopped.watch.value.value, 1);
        }),
      ),
  );
});

/** A few macrotask turns, each followed by a full collection. */
const collect = Effect.promise(async () => {
  setFlagsFromString("--expose-gc");
  // SAFETY: `gc` is the collector the flag exposes in a new context.
  const gc = runInNewContext("gc") as () => void;
  for (let round = 0; round < 5; round++) {
    await new Promise((resolve) => setImmediate(resolve));
    gc();
  }
});

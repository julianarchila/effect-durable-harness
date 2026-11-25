// The Harness's synchronous observers (`Conversation.observe` on the handle, `observeDoc()` of a task document) and
// task document watches, which end with their task, on every backend.
//
// - Frames are `WatchFrame`s and an end is an `ObserverEnd` (`SessionObservers.test.ts`).
// - "advance a graph observer with a blocked refresh, outside any commit": the registry is static, so the reason
//   changes with a failed migration: a task of version 1 is reopened under a version 2 whose migration throws.
// - A watch's `start(listener)` is a consumer fiber over `frames`, and `closed` is how that stream ends. A
//   consumer ends only once its callback returns, so "a watch busy in an earlier call…" checks that the stream
//   fails once the busy call returns, delivering no retirement. "a watch never started…" checks that a stream
//   started after the close yields nothing.
// - Retention is followed through the Harness's kernel, a record of functions over its state, with `IdleTimers`
//   off (a pending eviction sleep is a GC root). A watch's release lives in the scope that acquired it, so the
//   cases that check what a watch keeps close that scope; "keeps no task document watch alive…" follows the
//   consumer's listener, which is what a running effect-durable-harness watch keeps alive. The cases that check the Harness is
//   collected are skipped on `sqlite-node`, whose statement cache keeps it (`SessionWatches`).
import { setFlagsFromString } from "node:v8";
import { runInNewContext } from "node:vm";
import { assert, it } from "@effect/vitest";
import * as Cause from "effect/Cause";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as Doc from "../src/Doc.ts";
import type { WatchFrame } from "../src/Doc.ts";
import type { Conversation } from "../src/Harness.ts";
import type { TaskId } from "../src/Ids.ts";
import { IdleTimers } from "../src/IdleTimers.ts";
import type { Observer, ObserverEnd } from "../src/Observer.ts";
import * as Task from "../src/Task.ts";
import { Tx } from "../src/Tx.ts";
import type { TaskGraph } from "../src/TaskGraph.ts";
import type { ConversationView } from "../src/View.ts";
import { completed, eventually, holdLine, joined, openHarness, settled } from "./HarnessSupport.ts";
import { controlledStorage, describeEachBackend, flush, run } from "./SessionSupport.ts";

const Lines = Schema.Struct({ lines: Schema.Array(Schema.String) });
type Lines = typeof Lines.Type;

const Progress = Doc.make({
  checkpointWhen: () => false,
  kind: "test.observers.progress",
  version: 1,
  scope: "task",
  schema: Lines,
  initial: () => ({ lines: [] }),
});

/**
 * A task that writes its progress document, signals `written`, waits for `gate`, then completes, which retires the
 * document. It takes two phases, since a phase's commit returns the next state.
 */
const writer = (gate: Deferred.Deferred<void>, written: Deferred.Deferred<void>) =>
  Task.make({
    name: "test.observers.writer",
    version: 1,
    input: Schema.Null,
    checkpoint: Schema.Union([
      Schema.Struct({ phase: Schema.Literal("run") }),
      Schema.Struct({ phase: Schema.Literal("finish") }),
    ]),
    result: Schema.Null,
    initial: () => ({ phase: "run" }),
    phases: {
      run: (task, runtime) =>
        runtime.commit(() =>
          Effect.gen(function* () {
            const tx = yield* Tx;
            (yield* tx.doc(Progress, task.id)).lines.push("started");
            return { status: "running", checkpoint: { phase: "finish" } } as const;
          }),
        ),
      finish: (_task, runtime) =>
        Effect.gen(function* () {
          yield* Deferred.succeed(written, undefined);
          yield* Deferred.await(gate);
          yield* runtime.commit(() => Effect.succeed(completed(null)));
        }),
    },
    abort: () => Effect.void,
  });

type Call<T> = { readonly advance: WatchFrame<T> } | { readonly end: ObserverEnd };

/** An observer recording each call. */
const recorder = <T>() => {
  const calls: Array<Call<T>> = [];
  const observer: Observer<T> = {
    advance: (call) => {
      calls.push({ advance: call });
    },
    end: (end) => {
      calls.push({ end });
    },
  };
  return { calls, observer };
};

const appendNote = (root: Conversation) =>
  root.commit(
    Effect.gen(function* () {
      const tx = yield* Tx;
      return yield* tx.appendEntry(root.id, { kind: "note" });
    }),
  );

/** A Harness with an empty registry and its root conversation. */
const openRoot = Effect.gen(function* () {
  const storage = yield* controlledStorage;
  const h = yield* openHarness(storage, []);
  const root = yield* h.harness.ensureRoot();
  return { ...h, root };
});

/** A Harness with a writer task started in the root conversation and its progress document written. */
const openWriter = Effect.gen(function* () {
  const storage = yield* controlledStorage;
  const gate = yield* Deferred.make<void>();
  const written = yield* Deferred.make<void>();
  const h = yield* openHarness(storage, [writer(gate, written)]);
  const root = yield* h.harness.ensureRoot();
  const id: TaskId = yield* root.commit(
    Effect.gen(function* () {
      const tx = yield* Tx;
      return yield* tx.createTask(writer(gate, written), null, {
        ownership: { kind: "conversation" },
      });
    }),
  );
  yield* h.harness.resume;
  yield* Deferred.await(written);
  return { ...h, root, id, finish: Deferred.succeed(gate, undefined) };
});

/** A one-phase task whose migration always throws. */
const migrating = (version: number) =>
  Task.make({
    name: "test.observers.migrating",
    version,
    input: Schema.Null,
    checkpoint: Schema.Struct({ phase: Schema.Literal("run") }),
    result: Schema.Null,
    initial: () => ({ phase: "run" }),
    phases: {
      run: (_task, runtime) => runtime.commit(() => Effect.succeed(completed(null))),
    },
    abort: () => Effect.void,
    migrate: () => {
      throw new Error("cannot migrate");
    },
  });

/** The single end a recorder got, checked to be a failure; its error. */
const failure = (end: ObserverEnd | undefined) => {
  assert.strictEqual(end?._tag, "Failed");
  return end?._tag === "Failed" ? end.error : undefined;
};

const endOf = <T>(call: Call<T> | undefined) =>
  call !== undefined && "end" in call ? call.end : undefined;

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

describeEachBackend("observeConversation and observeTaskGraph", (backend) => {
  it.effect(
    "start at the current value and advance at the publication with the same change a watch gets",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const h = yield* openRoot;
          const root = h.root;
          const view = recorder<ConversationView>();
          const viewObservation = yield* root.observe(view.observer);
          const graph = recorder<TaskGraph>();
          const graphObservation = yield* h.harness.observeTaskGraph(graph.observer);
          assert.strictEqual(viewObservation.value.conversation.id, root.id);
          assert.deepStrictEqual(graphObservation.value, { tasks: {} });
          const watchScope = yield* Scope.make();
          const watch = yield* root.watch().pipe(Scope.provide(watchScope));
          const watched: Array<WatchFrame<ConversationView>> = [];
          yield* Effect.forkChild(
            Stream.runForEach(watch.frames, (frame) => Effect.sync(() => watched.push(frame))),
            { startImmediately: true },
          );
          const entry = yield* appendNote(root);
          assert.lengthOf(view.calls, 1);
          const [call] = view.calls;
          const advance = call !== undefined && "advance" in call ? call.advance : undefined;
          assert.isFalse(advance?.retired);
          assert.strictEqual(advance?.value.entries.at(-1)?.id, entry.id);
          yield* flush;
          // The watch got the same change.
          assert.deepStrictEqual(watched, [advance]);
          // No task changed: the graph got nothing.
          assert.deepStrictEqual(graph.calls, []);
          viewObservation.detach();
          yield* appendNote(root);
          assert.lengthOf(view.calls, 1);
          yield* Scope.close(watchScope, Exit.void);
          const graphWatch = yield* h.harness.watchTaskGraph;
          yield* h.close;
          // A Session close fails an observer (it may follow the next Session); a watch ends `ended`.
          assert.lengthOf(graph.calls, 1);
          assert.strictEqual(failure(endOf(graph.calls[0]))?._tag, "SessionClosed");
          // The close settles `ended`: the frame stream ends with no frame.
          assert.deepStrictEqual(Array.from(yield* Stream.runCollect(graphWatch.frames)), []);
        }),
      ),
  );

  it.effect("end with the error that failed the Harness", () =>
    run(backend)(
      Effect.gen(function* () {
        const h = yield* openRoot;
        const graph = recorder<TaskGraph>();
        yield* h.harness.observeTaskGraph(graph.observer);
        h.storage.failNextCommit("disk gone");
        assert.isTrue(Exit.isFailure(yield* Effect.exit(appendNote(h.root))));
        yield* h.harness.closed;
        assert.lengthOf(graph.calls, 1);
        assert.strictEqual(failure(endOf(graph.calls[0]))?._tag, "SessionFailed");
      }),
    ),
  );

  it.effect(
    "report a throwing observer, detach it and end it with the error, and keep the view going",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const h = yield* openRoot;
          const root = h.root;
          const bug = new Error("observer bug");
          const ends: Array<ObserverEnd> = [];
          yield* root.observe({
            advance: () => {
              throw bug;
            },
            end: (end) => ends.push(end),
          });
          const healthy = recorder<ConversationView>();
          yield* root.observe(healthy.observer);
          yield* appendNote(root);
          yield* appendNote(root);
          yield* eventually(Effect.sync(() => h.reports.length > 0));
          assert.deepStrictEqual(
            h.reports.map((report) => Cause.squash(report.cause)),
            [bug],
          );
          assert.deepStrictEqual(
            h.reports.map((report) => report.kind),
            ["listener_error"],
          );
          assert.deepStrictEqual(ends, [{ _tag: "ListenerError", error: bug }]);
          assert.lengthOf(healthy.calls, 2);
          yield* h.close;
          assert.lengthOf(ends, 1);
          assert.deepStrictEqual(yield* h.harness.closed, { reason: "closed" });
        }),
      ),
  );

  it.effect("reject, never throw, on a closed Harness", () =>
    run(backend)(
      Effect.gen(function* () {
        const h = yield* openRoot;
        yield* h.close;
        const view = yield* Effect.flip(h.root.observe(recorder<ConversationView>().observer));
        const graph = yield* Effect.flip(
          h.harness.observeTaskGraph(recorder<TaskGraph>().observer),
        );
        assert.strictEqual(view._tag, "SessionClosed");
        assert.strictEqual(view.message, "Session is closed");
        assert.strictEqual(graph._tag, "SessionClosed");
        assert.strictEqual(graph.message, "Session is closed");
      }),
    ),
  );

  it.effect("reject an attachment cancelled while it waits on the line, and attach nothing", () =>
    run(backend)(
      Effect.gen(function* () {
        const h = yield* openRoot;
        const root = h.root;
        const view = recorder<ConversationView>();
        const graph = recorder<TaskGraph>();
        // A commit holds the line, so both attachments wait behind it.
        const held = yield* holdLine(h);
        yield* held.entered;
        const attachingView = yield* Effect.forkChild(root.observe(view.observer), {
          startImmediately: true,
        });
        const attachingGraph = yield* Effect.forkChild(h.harness.observeTaskGraph(graph.observer), {
          startImmediately: true,
        });
        yield* Fiber.interrupt(attachingView);
        yield* Fiber.interrupt(attachingGraph);
        yield* held.release;
        assert.isTrue(Exit.hasInterrupts(yield* Fiber.await(attachingView)));
        assert.isTrue(Exit.hasInterrupts(yield* Fiber.await(attachingGraph)));
        yield* appendNote(root);
        yield* h.close;
        assert.deepStrictEqual(view.calls, []);
        assert.deepStrictEqual(graph.calls, []);
      }),
    ),
  );

  it.effect("advance a graph observer with a blocked refresh, outside any commit", () =>
    run(backend)(
      Effect.gen(function* () {
        const storage = yield* controlledStorage;
        const first = yield* openHarness(storage, [migrating(1)]);
        const root = yield* first.harness.ensureRoot();
        // Scheduling is not resumed: the task stays pending.
        const id: TaskId = yield* root.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            return yield* tx.createTask(migrating(1), null, {
              ownership: { kind: "conversation" },
            });
          }),
        );
        yield* first.close;
        const h = yield* openHarness(storage, [migrating(2)]);
        const graph = recorder<TaskGraph>();
        const observation = yield* h.harness.observeTaskGraph(graph.observer);
        const key = String(id);
        // A migration is declared: nothing blocks the task until it is tried.
        assert.isUndefined(observation.value.tasks[key]?.blocked);
        yield* h.harness.resume;
        yield* eventually(Effect.sync(() => graph.calls.length === 1));
        // The failed migration re-derives the reason on the line, with no commit.
        const [call] = graph.calls;
        const advance = call !== undefined && "advance" in call ? call.advance : undefined;
        assert.deepStrictEqual(advance?.ops, [
          ["s", ["tasks", key, "blocked"], "migration_failed"],
        ]);
        assert.isFalse(advance?.retired);
        assert.strictEqual(advance?.value.tasks[key]?.blocked, "migration_failed");
        assert.deepStrictEqual(
          h.reports.map((report) => report.kind),
          ["migration_failed"],
        );
        observation.detach();
        yield* h.close;
      }),
    ),
  );

  it.effect("keep no detached observer alive", () =>
    run(backend)(
      Effect.gen(function* () {
        const h = yield* openRoot;
        const root = h.root;
        /** Attach observers only the Harness references; `detach` says whether to detach them before returning. */
        const attach = (detach: boolean) =>
          Effect.gen(function* () {
            const view: Observer<ConversationView> = { advance: () => {}, end: () => {} };
            const graph: Observer<TaskGraph> = { advance: () => {}, end: () => {} };
            const observations = [
              yield* root.observe(view),
              yield* h.harness.observeTaskGraph(graph),
            ];
            if (detach) for (const observation of observations) observation.detach();
            return { observations, view: new WeakRef(view), graph: new WeakRef(graph) };
          });
        // Control: attached observers stay, so the collection below is real.
        const attached = yield* attach(false);
        yield* collect;
        assert.isDefined(attached.view.deref());
        assert.isDefined(attached.graph.deref());
        for (const observation of attached.observations) observation.detach();

        const detached = yield* attach(true);
        yield* collect;
        assert.isUndefined(detached.view.deref());
        assert.isUndefined(detached.graph.deref());
        yield* h.close;
      }),
    ),
  );
});

describeEachBackend("task documents", (backend) => {
  it.effect(
    "a watch delivers the retirement of the commit that ends its task, then ends `ended`",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const h = yield* openWriter;
          const watch = Option.getOrThrow(yield* h.session.watchDoc(Progress, h.id));
          assert.deepStrictEqual(watch.value, { lines: ["started"] });
          const closed = yield* Effect.forkChild(Stream.runCollect(watch.frames), {
            startImmediately: true,
          });
          yield* h.finish;
          yield* h.harness.awaitTask(h.id);
          const changes = yield* joined(closed);
          assert.deepStrictEqual(changes, [
            { value: { lines: ["started"] }, ops: [], retired: true },
          ]);
          assert.strictEqual(changes[0]?.value, watch.value);
          // The task is terminal: its address is absent for good.
          assert.isTrue(Option.isNone(yield* h.session.watchDoc(Progress, h.id)));
          yield* h.close;
        }).pipe(Effect.scoped),
      ),
  );

  it.effect(
    "an observer gets the retirement, then its end, at the publication of the commit that ends the task",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const h = yield* openWriter;
          const progress = recorder<Lines>();
          yield* h.session.observeDoc(Progress, h.id, progress.observer);
          yield* h.finish;
          yield* h.harness.awaitTask(h.id);
          assert.deepStrictEqual(progress.calls, [
            { advance: { value: { lines: ["started"] }, ops: [], retired: true } },
            { end: { _tag: "Ended" } },
          ]);
          yield* h.close;
          assert.lengthOf(progress.calls, 2);
        }),
      ),
  );

  // Adapted: an effect-durable-harness watch is a stream its consumer pulls, so what a running watch keeps alive is its consumer's
  // listener; once the task ended, the Session ends the stream and keeps nothing of it.
  it.effect("keeps no task document watch alive once its task ended", () =>
    run(backend)(
      Effect.gen(function* () {
        const h = yield* openWriter;
        let ended = false;
        /** A started watch of the task's document, which only the Harness references. */
        const start = Effect.gen(function* () {
          const watch = Option.getOrThrow(yield* h.session.watchDoc(Progress, h.id));
          // oxlint-disable-next-line unicorn/consistent-function-scoping -- a closure per watch, to collect it
          const listener = (_change: WatchFrame<Lines>) => Effect.void;
          yield* Effect.forkChild(
            Stream.runForEach(watch.frames, listener).pipe(
              Effect.ensuring(
                Effect.sync(() => {
                  ended = true;
                }),
              ),
            ),
            { startImmediately: true },
          );
          return new WeakRef(listener);
        });
        const watch = yield* start;
        // Control: the watch stays while its task runs, so the collection below is real.
        yield* collect;
        assert.isDefined(watch.deref());
        yield* h.finish;
        yield* h.harness.awaitTask(h.id);
        yield* eventually(Effect.sync(() => ended));
        yield* collect;
        assert.isUndefined(watch.deref());
        // Nothing stays pinned for the ended watch.
        assert.strictEqual(h.kernel.documentCacheSizes.attached, 0);
        yield* h.close;
      }),
    ),
  );

  // The acquisition's scope holds a watch's release until it closes, so it is closed here once the Harness is.
  // Skipped on `sqlite-node`: its statement cache keeps a closed Harness (`SessionWatches`).
  it.effect.skipIf(backend.name === "sqlite")(
    "a watch never started when its task ends still ends with the Harness's close, keeping nothing of it",
    () =>
      run(backend)(
        Effect.gen(function* () {
          /** Acquire a task document's watch, never start it, end its task, and close the Harness. */
          const acquire = Effect.gen(function* () {
            const h = yield* openWriter;
            const watch = Option.getOrThrow(yield* h.session.watchDoc(Progress, h.id));
            yield* h.finish;
            yield* h.harness.awaitTask(h.id);
            yield* flush;
            yield* h.close;
            return { watch, harness: new WeakRef(h.kernel) };
          }).pipe(Effect.scoped, Effect.provideService(IdleTimers, { enabled: false }));
          const { watch, harness } = yield* acquire;
          // Closed at the Harness's close, the pending retirement dropped, as for every watch.
          assert.deepStrictEqual(yield* Stream.runCollect(watch.frames), []);
          yield* collect;
          assert.isUndefined(harness.deref());
        }),
      ),
  );

  it.effect("a watch never started when its task ends fails with the Harness", () =>
    run(backend)(
      Effect.gen(function* () {
        const h = yield* openWriter;
        const watch = Option.getOrThrow(yield* h.session.watchDoc(Progress, h.id));
        yield* h.finish;
        yield* h.harness.awaitTask(h.id);
        h.storage.failNextCommit("disk gone");
        assert.isTrue(Exit.isFailure(yield* Effect.exit(appendNote(h.root))));
        const error = yield* Effect.flip(Stream.runCollect(watch.frames));
        assert.strictEqual(error._tag, "SessionFailed");
      }).pipe(Effect.scoped),
    ),
  );

  it.effect(
    "a watch busy in an earlier call when its task ends fails with the Harness before its retirement call",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const h = yield* openWriter;
          const watch = Option.getOrThrow(yield* h.session.watchDoc(Progress, h.id));
          const entered = yield* Deferred.make<void>();
          const release = yield* Deferred.make<void>();
          const values: Array<Lines> = [];
          const closed = yield* Effect.forkChild(
            Stream.runForEach(watch.frames, (frame) =>
              Effect.gen(function* () {
                values.push(frame.value);
                yield* Deferred.succeed(entered, undefined);
                yield* Deferred.await(release);
              }),
            ),
            { startImmediately: true },
          );
          yield* h.harness.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              (yield* tx.doc(Progress, h.id)).lines.push("more");
            }),
          );
          yield* Deferred.await(entered);
          // The task ends while the listener is busy: its retirement waits behind the running call.
          yield* h.finish;
          yield* h.harness.awaitTask(h.id);
          h.storage.failNextCommit("disk gone");
          assert.isTrue(Exit.isFailure(yield* Effect.exit(appendNote(h.root))));
          yield* Deferred.succeed(release, undefined);
          const exit = yield* Fiber.await(closed);
          assert.isTrue(Exit.isFailure(exit));
          assert.deepStrictEqual(values, [{ lines: ["started", "more"] }]);
        }).pipe(Effect.scoped),
      ),
  );

  it.effect(
    "a watch whose retirement call runs when its task ends ends only after that call returns",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const h = yield* openWriter;
          const watch = Option.getOrThrow(yield* h.session.watchDoc(Progress, h.id));
          const entered = yield* Deferred.make<void>();
          const release = yield* Deferred.make<void>();
          let returned = false;
          const closed = yield* Effect.forkChild(
            Stream.runForEach(watch.frames, () =>
              Effect.gen(function* () {
                yield* Deferred.succeed(entered, undefined);
                yield* Deferred.await(release);
                returned = true;
              }),
            ),
            { startImmediately: true },
          );
          // An explicit retirement first: its call runs while the task ends, and nothing more is pending.
          yield* h.harness.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              yield* tx.retireDoc(Progress, h.id);
            }),
          );
          yield* Deferred.await(entered);
          yield* h.finish;
          yield* h.harness.awaitTask(h.id);
          assert.isFalse(yield* settled(closed));
          yield* Deferred.succeed(release, undefined);
          yield* joined(closed);
          assert.isTrue(returned);
          yield* h.close;
        }).pipe(Effect.scoped),
      ),
  );

  // Skipped on `sqlite-node`, as the case above.
  it.effect.skipIf(backend.name === "sqlite")(
    "hands listeners changes that keep nothing of the Harness",
    () =>
      run(backend)(
        Effect.gen(function* () {
          /** Watch a task document until its task ends, keeping every change, then close the Harness. */
          const acquire = Effect.gen(function* () {
            const h = yield* openWriter;
            const watch = Option.getOrThrow(yield* h.session.watchDoc(Progress, h.id));
            const changes: Array<WatchFrame<Lines>> = [];
            const closed = yield* Effect.forkChild(
              Stream.runForEach(watch.frames, (change) => Effect.sync(() => changes.push(change))),
              { startImmediately: true },
            );
            yield* h.harness.commit(
              Effect.gen(function* () {
                const tx = yield* Tx;
                (yield* tx.doc(Progress, h.id)).lines.push("more");
              }),
            );
            yield* flush;
            yield* h.finish;
            yield* h.harness.awaitTask(h.id);
            yield* joined(closed);
            yield* h.close;
            return { changes, harness: new WeakRef(h.kernel) };
          }).pipe(Effect.scoped, Effect.provideService(IdleTimers, { enabled: false }));
          const { changes, harness } = yield* acquire;
          assert.lengthOf(changes, 2);
          for (const change of changes) {
            assert.deepStrictEqual(Object.keys(change), ["value", "ops", "retired"]);
            assert.deepStrictEqual(structuredClone(change), change);
          }
          yield* collect;
          assert.isUndefined(harness.deref());
        }),
      ),
  );
});

// How the Harness fails after a failed Storage call, on every backend.
// `failNextCommit(message)` fails the next commit with a `StorageFailure`; a one-shot
// `intercept` hook fails or holds any other call.
// A task waiting for its signal blocks on `Effect.never` and sees
// the signal as its interruption. Reports are the Causes
// passed to `onReport`, delivered on a fiber of their own, so a check of
// `reports` lets that fiber run first.
//
// A context read scans its range off the Session line with a page limit above 1: a `scanEntries`
// intercept with a limit above 1 is that scan, held or failed once.
//
// Not covered here:
// - Failing when Storage cannot close: the Session does not close storage; the storage layer's own
//   scope does.
// - Exempting a read its caller cancelled: a storage call is never interrupted, so there is no
//   exemption for a cancelled read (`guard` in src/internal/session.ts).
// - Containing foreign thenables or promises: there are no promises to contain.
// - A throwing clock: `Clock` cannot throw.
// - A throwing watch listener, which ends that watch: covered by `SessionWatches` (a watch is a
//   stream whose consumer's failure ends it).
import { assert, it } from "@effect/vitest";
import * as Cause from "effect/Cause";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { Backend, Rejected, StorageFailure } from "../src/AgentStorage.ts";
import type { WatchFrame } from "../src/Doc.ts";
import type { Conversation, Harness, HarnessError } from "../src/Harness.ts";
import { ConversationId, TaskId } from "../src/Ids.ts";
import { LiveDoc } from "../src/Live.ts";
import { type ReadOnlySession, SessionFailed } from "../src/SessionError.ts";
import * as Task from "../src/Task.ts";
import { Tx } from "../src/Tx.ts";
import {
  awaited,
  eventually,
  joined,
  openHarness,
  settled,
  type OpenHarnessOptions,
} from "./HarnessSupport.ts";
import {
  acquire,
  type ControlledStorage,
  controlledStorage,
  describeEachBackend,
  flush,
  run,
} from "./SessionSupport.ts";
import type { Report } from "../src/Report.ts";

/** Waits for its signal, records that it saw it, and ends `aborted` through its abort handler. */
const signals: Array<string> = [];
const Waiting = Task.make({
  name: "test.waiting",
  version: 1,
  input: Schema.Null,
  checkpoint: Schema.Struct({ phase: Schema.Literal("work") }),
  result: Schema.Null,
  initial: () => ({ phase: "work" }),
  phases: {
    // The invocation's signal is the interruption of its handler's fiber.
    work: () =>
      Effect.never.pipe(
        Effect.onInterrupt(() =>
          Effect.sync(() => {
            signals.push("aborted");
          }),
        ),
      ),
  },
  abort: (_task, runtime) =>
    runtime.commit(() =>
      Effect.succeed({ status: "terminal", outcome: { status: "aborted" } } as const),
    ),
});

/** A listener that throws. */
const asyncThrow = () => {
  throw new Error("async listener");
};

/** The error a failed Storage call returns. */
const diskGone = () =>
  new StorageFailure({ reason: new Backend({ message: "disk gone", cause: "disk gone" }) });

const started = (storage: ControlledStorage, options?: OpenHarnessOptions) =>
  Effect.gen(function* () {
    signals.length = 0;
    const h = yield* openHarness(storage, [Waiting], options);
    const root = yield* h.harness.ensureRoot();
    return { h, root };
  });

const appendNote = (root: Conversation) =>
  root.commit(
    Effect.gen(function* () {
      const tx = yield* Tx;
      return yield* tx.appendEntry(root.id, { kind: "note" });
    }),
  );

const createWaiting = (root: Conversation) =>
  root.commit(
    Effect.gen(function* () {
      const tx = yield* Tx;
      return yield* tx.createTask(Waiting, null, { ownership: { kind: "conversation" } });
    }),
  );

/** Lets the scheduler run until the task is `running`, for a bounded number of turns. */
const running = (harness: Harness["Service"], id: TaskId) =>
  eventually(
    harness
      .getTask(id)
      .pipe(Effect.map((found) => Option.isSome(found) && found.value.state.status === "running")),
  );

/** How a watch's frames end, as a close reason. */
const watchEnd = <T>(frames: Stream.Stream<WatchFrame<T>, SessionFailed>) =>
  Stream.runDrain(frames).pipe(
    Effect.as<{ readonly reason: string; readonly error?: unknown }>({ reason: "stopped" }),
    Effect.catchTag("SessionFailed", (error) =>
      Effect.succeed({ reason: "session_failed", error: error.cause }),
    ),
  );

/** Any tagged error a call fails with. */
interface Tagged {
  readonly _tag: string;
}

const isSessionFailed = Schema.is(SessionFailed);

/** Asserts the error is a `SessionFailed` with `cause`. */
const assertSessionFailed = (error: Tagged, cause: Error) => {
  assert.strictEqual(error._tag, "SessionFailed");
  assert.strictEqual(isSessionFailed(error) ? error.cause : undefined, cause);
};

/** The errors the reports carry, once pending reports had their turn. */
const reported = (reports: ReadonlyArray<Report>) =>
  flush.pipe(Effect.as(reports.map((report) => Cause.squash(report.cause))));

describeEachBackend("a failed Storage call", (backend) => {
  it.effect(
    "ends everything with the first error: the call, later calls, waits, watches, streams, running tasks",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const storage = yield* controlledStorage;
          const { h, root } = yield* started(storage);
          const id = yield* createWaiting(root);
          yield* h.harness.resume;
          yield* running(h.harness, id);
          const task = yield* Effect.forkChild(h.harness.awaitTask(id));
          const idle = yield* Effect.forkChild(h.harness.awaitIdle);
          const watch = yield* acquire(h.session.watchDoc(LiveDoc, root.id));
          const watchClosed = yield* Effect.forkChild(watchEnd(watch.value.frames));
          yield* flush;

          storage.failNextCommit("disk gone");
          // The call that hit it gets the storage error itself.
          const failure = yield* Effect.flip(appendNote(root));
          assert.instanceOf(failure, StorageFailure);
          assert.strictEqual(failure.message, "disk gone");

          assertSessionFailed(yield* Effect.flip(joined(task)), failure);
          assertSessionFailed(yield* Effect.flip(joined(idle)), failure);
          assert.deepStrictEqual(yield* joined(watchClosed), {
            reason: "session_failed",
            error: failure,
          });
          // Every later call, and a wait that starts now.
          assertSessionFailed(yield* Effect.flip(h.harness.getTask(id)), failure);
          assertSessionFailed(yield* Effect.flip(h.harness.awaitIdle), failure);
          assertSessionFailed(
            yield* Effect.flip(root.submit({ type: "input", content: "x" })),
            failure,
          );
          assert.deepStrictEqual(yield* h.harness.closed, { reason: "failed", error: failure });
          assert.deepStrictEqual(signals, ["aborted"]);
          assert.deepStrictEqual(yield* reported(h.reports), [failure]);
          yield* h.close;
        }),
      ),
  );

  it.effect("fails it from a read off the Session line, as a context read makes", () =>
    run(backend)(
      Effect.gen(function* () {
        const storage = yield* controlledStorage;
        const { h, root } = yield* started(storage);
        yield* appendNote(root);
        const error = diskGone();
        storage.intercept.scanEntries = (args, next) => {
          if (args[1] <= 1) return next;
          storage.intercept.scanEntries = undefined;
          return Effect.fail(error);
        };
        assert.strictEqual(yield* Effect.flip(root.context()), error);
        assert.deepStrictEqual(yield* h.harness.closed, { reason: "failed", error });
        assert.deepStrictEqual(yield* reported(h.reports), [error]);
        yield* h.close;
      }),
    ),
  );

  it.effect(
    "ends Storage calls underway with SessionFailed, and closes once they have settled",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const storage = yield* controlledStorage;
          const { h, root } = yield* started(storage);
          const release = yield* Deferred.make<void>();
          // A failed check still releases the held read, whose storage call is uninterruptible.
          yield* Effect.addFinalizer(() => Deferred.succeed(release, undefined));
          yield* appendNote(root);
          const entered = yield* Deferred.make<void>();
          storage.intercept.scanEntries = (args, next) =>
            args[1] <= 1
              ? next
              : Deferred.succeed(entered, undefined).pipe(
                  Effect.andThen(Deferred.await(release)),
                  Effect.andThen(next),
                );
          const reading = yield* Effect.forkChild(root.context());
          yield* awaited(entered);
          // Another call fails the Session while the context read is underway.
          const error = diskGone();
          storage.intercept.mintId = () => {
            storage.intercept.mintId = undefined;
            return Effect.fail(error);
          };
          assert.strictEqual(yield* Effect.flip(appendNote(root)), error);
          const closed = yield* Effect.forkChild(h.harness.closed);
          // Pending fibers get their turns.
          assert.isFalse(yield* settled(closed));
          // The read succeeds at the backend, yet its caller gets the failure: nothing it read is used.
          yield* Deferred.succeed(release, undefined);
          assertSessionFailed(yield* Effect.flip(joined(reading)), error);
          assert.deepStrictEqual(yield* joined(closed), { reason: "failed", error });
          yield* h.close;
        }),
      ),
  );

  // The Session does not close storage, so the
  // backend close is the Harness's scope close, recorded as "close" once `h.close` returns. It must wait for
  // the paged read underway, and the read must start no next page once close began.
  it.effect(
    "closes the backend only after a paged read underway, which cannot start another page",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const pages: Array<string> = [];
          const firstPage = yield* Deferred.make<void>();
          const releaseFirst = yield* Deferred.make<void>();
          // A failed check still releases the held page, whose storage call is uninterruptible.
          yield* Effect.addFinalizer(() => Deferred.succeed(releaseFirst, undefined));
          const storage = yield* controlledStorage;
          storage.intercept.scanEntries = (args, next) =>
            Effect.suspend(() => {
              if (args[1] <= 1) return next;
              pages.push(args[2] === undefined ? "first" : "next");
              if (args[2] !== undefined) return next;
              return Deferred.succeed(firstPage, undefined).pipe(
                Effect.andThen(Deferred.await(releaseFirst)),
                Effect.andThen(next),
              );
            });
          const { h, root } = yield* started(storage);
          // More entries than one page of the context read.
          yield* root.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              for (let n = 0; n < 300; n++) yield* tx.appendEntry(root.id, { kind: "note" });
            }),
          );
          const reading = yield* Effect.forkChild(root.context());
          yield* awaited(firstPage);
          const closing = yield* Effect.forkChild(
            h.close.pipe(
              Effect.andThen(
                Effect.sync(() => {
                  pages.push("close");
                }),
              ),
            ),
            { startImmediately: true },
          );
          // The backend closes after the page: close has not finished while the page is held.
          assert.isFalse(yield* settled(closing));
          yield* Deferred.succeed(releaseFirst, undefined);
          const failure = yield* Effect.flip(joined(reading));
          assert.include(failure.message, "is closed");
          yield* joined(closing);
          assert.deepStrictEqual(pages, ["first", "close"]);
          assert.deepStrictEqual(yield* h.harness.closed, { reason: "closed" });
        }),
      ),
  );

  it.effect("rejects the Harness's own calls with SessionFailed after a failure", () =>
    run(backend)(
      Effect.gen(function* () {
        const storage = yield* controlledStorage;
        const { h, root } = yield* started(storage);
        storage.failNextCommit("disk gone");
        const failure = yield* Effect.flip(appendNote(root));
        assert.instanceOf(failure, StorageFailure);
        assertSessionFailed(yield* Effect.flip(h.harness.resume), failure);
        assertSessionFailed(yield* Effect.flip(h.harness.ensureRoot()), failure);
        assertSessionFailed(yield* Effect.flip(h.harness.conversation(root.id)), failure);
        assertSessionFailed(
          yield* Effect.flip(h.harness.createConversation({ ownership: { kind: "ownerless" } })),
          failure,
        );
        yield* h.close;
      }),
    ),
  );

  it.effect("ends what a task holds with session_failed, not as cancelled by its signal", () =>
    run(backend)(
      Effect.gen(function* () {
        const ends: Array<unknown> = [];
        const watching = yield* Deferred.make<void>();
        const Watcher = Task.make({
          name: "test.watcher",
          version: 1,
          input: Schema.Null,
          checkpoint: Schema.Struct({ phase: Schema.Literal("work") }),
          result: Schema.Null,
          initial: () => ({ phase: "work" }),
          phases: {
            // The watch's frames are drained and the idle wait runs on a fiber of its own, each mapped
            // to how it ended.
            work: (_task, runtime) =>
              Effect.scoped(
                Effect.gen(function* () {
                  const doc = Option.getOrThrow(
                    yield* runtime.watchDoc(LiveDoc, runtime.conversationId),
                  );
                  const handle = Option.getOrThrow(
                    yield* runtime.conversation(runtime.conversationId),
                  );
                  const idle = yield* handle.awaitIdle.pipe(
                    Effect.as("resolved"),
                    Effect.catch((error) => Effect.succeed(error._tag)),
                    Effect.forkScoped,
                  );
                  yield* Deferred.succeed(watching, undefined);
                  const docEnd = yield* watchEnd(doc.frames);
                  ends.push(docEnd, yield* Fiber.join(idle));
                }),
              ),
          },
          abort: () => Effect.void,
        });
        const storage = yield* controlledStorage;
        const opened = yield* openHarness(storage, [Watcher]);
        const root = yield* opened.harness.ensureRoot();
        yield* root.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            yield* tx.createTask(Watcher, null, { ownership: { kind: "conversation" } });
          }),
        );
        yield* opened.harness.resume;
        yield* awaited(watching);
        storage.failNextCommit("disk gone");
        const failure = yield* Effect.flip(appendNote(root));
        assert.instanceOf(failure, StorageFailure);
        yield* opened.harness.closed;
        assert.deepStrictEqual(ends, [
          { reason: "session_failed", error: failure },
          "SessionFailed",
        ]);
        yield* opened.close;
      }),
    ),
  );

  it.effect("fails it from a failed ID mint", () =>
    run(backend)(
      Effect.gen(function* () {
        const storage = yield* controlledStorage;
        const { h, root } = yield* started(storage);
        const error = diskGone();
        storage.intercept.mintId = () => {
          storage.intercept.mintId = undefined;
          return Effect.fail(error);
        };
        assert.strictEqual(yield* Effect.flip(appendNote(root)), error);
        assert.deepStrictEqual(yield* h.harness.closed, { reason: "failed", error });
        yield* h.close;
      }),
    ),
  );

  it.effect("does not commit what a callback did after it caught a failed read", () =>
    run(backend)(
      Effect.gen(function* () {
        const storage = yield* controlledStorage;
        const { h, root } = yield* started(storage);
        const written = storage.admittedCommits.length;
        const error = diskGone();
        storage.intercept.task = () => {
          storage.intercept.task = undefined;
          return Effect.fail(error);
        };
        const rejected = yield* Effect.flip(
          root.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              yield* Effect.ignore(tx.task(TaskId.make(1)));
              yield* tx.appendEntry(root.id, { kind: "note" });
            }),
          ),
        );
        assertSessionFailed(rejected, error);
        assert.strictEqual(storage.admittedCommits.length, written);
        yield* h.close;
      }),
    ),
  );

  // A commit cannot fail with `StorageRequestError` here; its type allows only
  // `StorageFailure`, and an invalid batch is its `Rejected` reason (AgentStorage.ts).
  it.effect("fails it for a StorageRequestError from a commit, whose effect is unknown", () =>
    run(backend)(
      Effect.gen(function* () {
        const storage = yield* controlledStorage;
        const { h, root } = yield* started(storage);
        const rejection = new StorageFailure({ reason: new Rejected({ message: "bad batch" }) });
        storage.intercept.commit = () => {
          storage.intercept.commit = undefined;
          return Effect.fail(rejection);
        };
        assert.strictEqual(yield* Effect.flip(appendNote(root)), rejection);
        assert.deepStrictEqual(yield* h.harness.closed, { reason: "failed", error: rejection });
        yield* h.close;
      }),
    ),
  );

  // `tx.scanEntries` and `snapshotAsOf` only: `Conversation.entries` is a Stream without cursors
  // (Harness.ts), so it cannot be given an invalid one.
  it.effect("fails only the call for an invalid request", () =>
    run(backend)(
      Effect.gen(function* () {
        const { h, root } = yield* started(yield* controlledStorage);
        const entry = yield* appendNote(root);
        const requests: ReadonlyArray<Effect.Effect<void, Tagged>> = [
          root
            .commit(
              Effect.gen(function* () {
                const tx = yield* Tx;
                return yield* tx.scanEntries({ conversationId: ConversationId.make(999_999) }, 10);
              }),
            )
            .pipe(Effect.asVoid),
          // `core.live` keeps no history.
          // SAFETY: the overloads accept only rewindable documents; the test asks for one that is not.
          Effect.asVoid(h.session.snapshotAsOf(LiveDoc as never, root.id, entry.id)),
        ];
        for (const request of requests) {
          assert.strictEqual((yield* Effect.flip(request))._tag, "StorageRequestError");
        }
        yield* appendNote(root);
        const ended = yield* Effect.forkChild(h.harness.closed);
        yield* flush;
        assert.isUndefined(ended.pollUnsafe());
        assert.deepStrictEqual(yield* reported(h.reports), []);
        yield* h.close;
      }),
    ),
  );

  it.effect(
    "fails it for a throw in its own commit listeners, which would leave memory behind storage",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const { h, root } = yield* started(yield* controlledStorage);
          const bug = new Error("listener bug");
          yield* h.kernel.followCommits(() => {
            throw bug;
          });
          // The commit itself is durable and resolves; the Session fails behind it.
          yield* appendNote(root);
          assert.deepStrictEqual(yield* h.harness.closed, { reason: "failed", error: bug });
          assert.deepStrictEqual(yield* reported(h.reports), [bug]);
          yield* h.close;
        }),
      ),
  );

  // Adapted: a host close is closing the Harness's scope (`h.close`).
  it.effect("fails it for a storage error during a host close, before the backend closes", () =>
    run(backend)(
      Effect.gen(function* () {
        const storage = yield* controlledStorage;
        const { h, root } = yield* started(storage);
        const held = yield* storage.holdCommits;
        storage.failNextCommit("disk gone");
        const committing = yield* Effect.forkChild(appendNote(root));
        yield* held.entered;
        // Started at once, so close is underway before the commit settles.
        const closing = yield* Effect.forkChild(h.close, { startImmediately: true });
        yield* held.release;
        const failure = yield* Effect.flip(joined(committing));
        assert.instanceOf(failure, StorageFailure);
        yield* joined(closing);
        assert.deepStrictEqual(yield* h.harness.closed, { reason: "failed", error: failure });
        assert.deepStrictEqual(yield* reported(h.reports), [failure]);
      }),
    ),
  );

  it.effect(
    "ends an abort that joins a run ignoring its signal, and a reentrant report finds the Session failed",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const release = yield* Deferred.make<void>();
          const entered = yield* Deferred.make<void>();
          // The handler waits for something its signal cannot cancel: the wait is uninterruptible.
          const Stubborn = Task.make({
            name: "test.stubborn",
            version: 1,
            input: Schema.Null,
            checkpoint: Schema.Struct({ phase: Schema.Literal("work") }),
            result: Schema.Null,
            initial: () => ({ phase: "work" }),
            phases: {
              work: () =>
                Deferred.succeed(entered, undefined).pipe(
                  Effect.andThen(Deferred.await(release)),
                  Effect.uninterruptible,
                ),
            },
            abort: () => Effect.void,
          });
          const storage = yield* controlledStorage;
          let harness: Harness["Service"] | undefined;
          // Reports are delivered on a fiber of their own, so the report records the wait's end.
          // The first report wins.
          const reentered = yield* Deferred.make<HarnessError | ReadOnlySession | undefined>();
          const h = yield* openHarness(storage, [Stubborn], {
            onReport: () =>
              harness === undefined
                ? Effect.void
                : harness.awaitIdle.pipe(
                    Effect.as(undefined),
                    Effect.catch((error) => Effect.succeed(error)),
                    Effect.flatMap((ended) => Deferred.succeed(reentered, ended)),
                    Effect.asVoid,
                  ),
          });
          harness = h.harness;
          // A failed check still lets the stubborn run end, so the Harness's scope can close.
          yield* Effect.addFinalizer(() => Deferred.succeed(release, undefined));
          const root = yield* h.harness.ensureRoot();
          const id = yield* root.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              return yield* tx.createTask(Stubborn, null, { ownership: { kind: "conversation" } });
            }),
          );
          yield* h.harness.resume;
          yield* awaited(entered);
          const held = yield* storage.holdCommits;
          const aborting = yield* Effect.forkChild(h.harness.abortTask(id));
          yield* held.entered;
          yield* held.release;
          // The abort mark commits and the abort joins the run, which ignores its signal; then the Session fails.
          yield* flush;
          storage.failNextCommit("disk gone");
          const failure = yield* Effect.flip(appendNote(root));
          assert.instanceOf(failure, StorageFailure);
          assertSessionFailed(yield* Effect.flip(joined(aborting)), failure);
          assertSessionFailed((yield* awaited(reentered)) ?? { _tag: "resolved" }, failure);
          yield* Deferred.succeed(release, undefined);
          yield* h.close;
        }),
      ),
  );
});

describeEachBackend("listener and report errors", (backend) => {
  // Adapted: listeners are synchronous and `onReport` returns an Effect, so there is no promise to
  // reject. A throwing listener is reported; an `onReport` that dies is dropped, and the Harness keeps
  // working and closes cleanly.
  it.effect(
    "reports a listener or onReport whose promise rejects, without an unhandled rejection",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const storage = yield* controlledStorage;
          const { h, root } = yield* started(storage);
          yield* h.kernel.observeCommits({ advance: asyncThrow, end: () => {} });
          yield* h.kernel.subscribeClose(asyncThrow);
          yield* appendNote(root);
          yield* h.close;
          assert.deepStrictEqual((yield* reported(h.reports)).map(String), [
            "Error: async listener",
            "Error: async listener",
          ]);

          // The first Harness is closed, so this one reopens the same storage.
          const rejecting = yield* openHarness(storage, [], {
            onReport: () => Effect.die(new Error("async listener")),
          });
          yield* rejecting.kernel.observeCommits({
            advance: () => {
              throw new Error("sync listener");
            },
            end: () => {},
          });
          const again = yield* rejecting.harness.ensureRoot();
          yield* appendNote(again);
          yield* rejecting.close;
          assert.deepStrictEqual(yield* rejecting.harness.closed, { reason: "closed" });
        }),
      ),
  );

  it.effect(
    "reports a throwing commit or close listener and runs the others; the commit stands",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const { h, root } = yield* started(yield* controlledStorage);
          const seen: Array<string> = [];
          yield* h.kernel.observeCommits({
            advance: () => {
              throw new Error("commit listener");
            },
            end: () => {},
          });
          yield* h.kernel.observeCommits({
            advance: () => {
              seen.push("commit");
            },
            end: () => {},
          });
          yield* h.kernel.subscribeClose(() => {
            throw new Error("close listener");
          });
          yield* h.kernel.subscribeClose(() => {
            seen.push("close");
          });
          yield* appendNote(root);
          yield* h.close;
          assert.deepStrictEqual(seen, ["commit", "close"]);
          assert.deepStrictEqual((yield* reported(h.reports)).map(String), [
            "Error: commit listener",
            "Error: close listener",
          ]);
        }),
      ),
  );

  // The throwing observer is one of the Harness's internal view mounts (`h.views.attach`), released with
  // the test's scope; the throw reaches `onReport`. The other observer is a watch, whose frames end
  // without a failure.
  it.effect("keeps feeding the other observers of a view when one throws", () =>
    run(backend)(
      Effect.gen(function* () {
        const { h, root } = yield* started(yield* controlledStorage);
        const failure = new Error("observer broke");
        yield* h.views.attach(root.id, undefined, () => ({
          advance: () => {
            throw failure;
          },
          closeSession: () => {},
        }));
        const watch = yield* acquire(root.watch().pipe(Effect.map(Option.some)));
        yield* appendNote(root);
        const seen = yield* watch.value.frames.pipe(Stream.take(1), Stream.runCollect);
        assert.lengthOf(seen, 1);
        // Reports are delivered on a fiber of their own: wait, bounded, until the one expected arrived.
        yield* eventually(Effect.sync(() => h.reports.length > 0));
        assert.deepStrictEqual(yield* reported(h.reports), [failure]);
        yield* h.close;
        assert.deepStrictEqual(yield* watch.value.frames.pipe(Stream.runCollect), []);
      }),
    ),
  );

  // The task part only. A handler that dies with the value faults with its message; the
  // throwing watch consumer is covered by `SessionWatches`.
  it.effect(
    "faults only the task, and ends only the watch, for a thrown value that cannot be formatted",
    () =>
      run(backend)(
        Effect.gen(function* () {
          // SAFETY: `Object.create(null)` returns an object without a prototype.
          const unformattable = Object.create(null) as object;
          const Throws = Task.make({
            name: "test.throws-unformattable",
            version: 1,
            input: Schema.Null,
            checkpoint: Schema.Struct({ phase: Schema.Literal("work") }),
            result: Schema.Null,
            initial: () => ({ phase: "work" }),
            phases: {
              work: () => Effect.die(unformattable),
            },
            abort: () => Effect.void,
          });
          const opened = yield* openHarness(yield* controlledStorage, [Throws]);
          const root = yield* opened.harness.ensureRoot();
          const id = yield* root.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              return yield* tx.createTask(Throws, null, { ownership: { kind: "conversation" } });
            }),
          );
          yield* opened.harness.resume;
          const settled = yield* joined(yield* Effect.forkChild(opened.harness.awaitTask(id)));
          assert.deepStrictEqual(settled.state.outcome, {
            status: "faulted",
            error: { message: "[object Object]" },
          });
          yield* appendNote(root);
          yield* opened.close;
        }),
      ),
  );

  // The onReport part only; `Clock` cannot throw. A throwing commit listener makes the reports, and
  // the test checks every one reached `onReport`.
  it.effect(
    "keeps working when onReport throws, and falls back to Date.now when the clock throws",
    () =>
      run(backend)(
        Effect.gen(function* () {
          let thrown = 0;
          let delivered = 0;
          const { h, root } = yield* started(yield* controlledStorage, {
            onReport: () => {
              delivered++;
              throw new Error("report broke");
            },
          });
          yield* h.kernel.observeCommits({
            advance: () => {
              thrown++;
              throw new Error("listener broke");
            },
            end: () => {},
          });
          const id = yield* createWaiting(root);
          yield* h.harness.resume;
          yield* running(h.harness, id);
          const found = Option.getOrThrow(yield* h.harness.getTask(id));
          assert.isNumber(found.startedAt);
          yield* h.harness.abortTask(id);
          const settled = yield* joined(yield* Effect.forkChild(h.harness.awaitTask(id)));
          assert.strictEqual(settled.state.outcome.status, "aborted");
          yield* h.close;
          // A throwing observer is reported once, then detached.
          assert.strictEqual(thrown, 1);
          assert.strictEqual(delivered, thrown);
        }),
      ),
  );
});

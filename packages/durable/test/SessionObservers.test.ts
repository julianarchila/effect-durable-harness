// Session observers, on every backend: `Session.observeDoc()`, the synchronous observer of one document address
// that watches are built on, and `Session.observeCommits()`.
//
// - A frame is a `WatchFrame` (`{ value, ops, retired }`) and an end is an `ObserverEnd` tagged by `_tag`. A closed
//   Session ends with `SessionClosed`, and the error that failed the Session is the `SessionFailed` that wraps it.
// - The reporting Session is a kernel whose `report` records. Storage never calls back into the Session during
//   `commit`, only hands receipts, so every call is outside it.
// - A cancelled attachment context is an interrupted attaching fiber; no value is `None`.
// - The scope ID is typed, and address resolution has no runtime check for a missing one, as for `watchDoc`, so
//   only the closed Session case is checked.
// - An async observer returns an Effect that fails, which the Session runs.
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
import * as Stream from "effect/Stream";
import type * as Tracer from "effect/Tracer";
import { AgentStorage } from "../src/AgentStorage.ts";
import * as Doc from "../src/Doc.ts";
import type { WatchFrame } from "../src/Doc.ts";
import type { ConversationId } from "../src/Ids.ts";
import * as internal from "../src/internal/session.ts";
import type { SessionKernel } from "../src/internal/session.ts";
import type { CommitObserver, Observation, Observer, ObserverEnd } from "../src/Observer.ts";
import type { ReportInfo } from "../src/Report.ts";
import { Tx } from "../src/Tx.ts";
import {
  acquire,
  controlledStorage,
  createConversation,
  describeEachBackend,
  flush,
  openSession,
  run,
} from "./SessionSupport.ts";

const Counter = Schema.Struct({ count: Schema.Number });
type Counter = typeof Counter.Type;

const SessionCounter = Doc.make({
  checkpointWhen: () => false,
  kind: "observe.session-counter",
  version: 1,
  scope: "session",
  schema: Counter,
  initial: () => ({ count: 0 }),
});

/** A latest, initial-fork conversation document, the kind a deferred commit may hold. */
const ConversationCounter = Doc.make({
  checkpointWhen: () => false,
  kind: "observe.conversation-counter",
  version: 1,
  scope: "conversation",
  history: "latest",
  fork: "initial",
  schema: Counter,
  initial: () => ({ count: 0 }),
});

type Call<T> = { readonly advance: WatchFrame<T> } | { readonly end: ObserverEnd };

/** An observer that records each call. */
const recorder = <T = Counter>() => {
  const calls: Array<Call<T>> = [];
  const observer: Observer<T> = {
    advance: (change) => {
      calls.push({ advance: change });
    },
    end: (end) => {
      calls.push({ end });
    },
  };
  return { calls, observer };
};

/** A Session kernel over a controlled storage that keeps its reports. */
const open = Effect.gen(function* () {
  const storage = yield* controlledStorage;
  const reports: Array<{ readonly error: unknown; readonly info: ReportInfo }> = [];
  /** The span each report went on, by report. */
  const spans = new Map<unknown, Tracer.AnySpan | undefined>();
  const session = yield* internal
    .make({
      report: (cause, info, on) => {
        reports.push({ error: Cause.squash(cause), info });
        spans.set(Cause.squash(cause), on);
      },
    })
    .pipe(Effect.provideService(AgentStorage, storage));
  return { storage, session, reports, spans };
});

const add = (session: SessionKernel, by = 1) =>
  session.commit(
    Effect.gen(function* () {
      const tx = yield* Tx;
      (yield* tx.doc(SessionCounter)).count += by;
    }),
  );

const setConversationCount = (
  session: SessionKernel,
  conversationId: ConversationId,
  count: number,
) =>
  session.commit(
    Effect.gen(function* () {
      const tx = yield* Tx;
      (yield* tx.doc(ConversationCounter, conversationId)).count = count;
    }),
  );

/** The ended observer's end, checked to be a failure, and its error's tag. */
const failedWith = (end: ObserverEnd | undefined) => {
  assert.strictEqual(end?._tag, "Failed");
  return end?._tag === "Failed" ? end.error : undefined;
};

/** The end of a recorder's single call. */
const onlyEnd = <T>(calls: ReadonlyArray<Call<T>>): ObserverEnd | undefined => {
  assert.lengthOf(calls, 1);
  const [call] = calls;
  return call !== undefined && "end" in call ? call.end : undefined;
};

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

describeEachBackend("observeDoc", (backend) => {
  it.effect("returns undefined for an absent address and attaches nothing", () =>
    run(backend)(
      Effect.gen(function* () {
        const { session } = yield* open;
        const { calls, observer } = recorder();
        assert.isTrue(Option.isNone(yield* session.observeDoc(SessionCounter, observer)));
        yield* add(session);
        assert.deepStrictEqual(calls, []);
        assert.strictEqual(session.documentCacheSizes.attached, 0);
        yield* session.close;
      }),
    ),
  );

  it.effect(
    "starts at the committed value and advances at the publication of each stored commit with its ops",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const { session } = yield* open;
          yield* add(session);
          const { calls, observer } = recorder();
          const observation = Option.getOrThrow(
            yield* session.observeDoc(SessionCounter, observer),
          );
          assert.deepStrictEqual(observation.value, { count: 1 });
          yield* add(session, 2);
          assert.deepStrictEqual(calls, [
            { advance: { value: { count: 3 }, ops: [["s", ["count"], 3]], retired: false } },
          ]);
          observation.detach();
          yield* session.close;
        }),
      ),
  );

  it.effect("advances when memory adopts a held commit, before anything is stored", () =>
    run(backend)(
      Effect.gen(function* () {
        const { storage, session } = yield* open;
        const conversationId = yield* createConversation(session);
        yield* setConversationCount(session, conversationId, 0);
        const { calls, observer } = recorder();
        const observation = Option.getOrThrow(
          yield* session.observeDoc(ConversationCounter, conversationId, observer),
        );
        const stored = storage.commits.length;
        yield* session.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            (yield* tx.doc(ConversationCounter, conversationId)).count = 5;
          }),
          { persistence: { kind: "deferred", withinMs: Number.POSITIVE_INFINITY } },
        );
        assert.lengthOf(storage.commits, stored);
        assert.deepStrictEqual(calls, [
          { advance: { value: { count: 5 }, ops: [["s", ["count"], 5]], retired: false } },
        ]);
        observation.detach();
        yield* session.close;
      }),
    ),
  );

  // A held commit's caller span travels to its adoption, as a stored commit's does to its
  // publication, so a throw the guard reports there goes on it.
  it.effect("reports a throw at a held commit's adoption on that commit's caller span", () =>
    run(backend)(
      Effect.gen(function* () {
        const { session, reports, spans } = yield* open;
        const conversationId = yield* createConversation(session);
        yield* setConversationCount(session, conversationId, 0);
        const bug = new Error("observer bug");
        const observation = Option.getOrThrow(
          yield* session.observeDoc(ConversationCounter, conversationId, {
            advance: () => {
              throw bug;
            },
            end: () => {},
          }),
        );
        const caller = yield* session
          .commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              (yield* tx.doc(ConversationCounter, conversationId)).count = 5;
            }),
            { persistence: { kind: "deferred", withinMs: Number.POSITIVE_INFINITY } },
          )
          .pipe(Effect.andThen(Effect.currentParentSpan), Effect.withSpan("test.caller"));
        const report = reports.find((each) => each.error === bug);
        assert.strictEqual(report?.info.kind, "listener_error");
        assert.strictEqual(spans.get(bug)?.spanId, caller.spanId);
        observation.detach();
        yield* session.close;
      }),
    ),
  );

  it.effect(
    "follows a conversation document's address: a retirement with the last value, then the new incarnation",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const { session } = yield* open;
          const conversationId = yield* createConversation(session);
          yield* setConversationCount(session, conversationId, 2);
          const { calls, observer } = recorder();
          const observation = Option.getOrThrow(
            yield* session.observeDoc(ConversationCounter, conversationId, observer),
          );
          yield* session.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              yield* tx.retireDoc(ConversationCounter, conversationId);
            }),
          );
          // The address is absent now: a new attachment finds nothing, the existing one goes on.
          assert.isTrue(
            Option.isNone(
              yield* session.observeDoc(ConversationCounter, conversationId, recorder().observer),
            ),
          );
          yield* setConversationCount(session, conversationId, 9);
          assert.deepStrictEqual(calls, [
            { advance: { value: observation.value, ops: [], retired: true } },
            { advance: { value: { count: 9 }, ops: [["r", { count: 9 }]], retired: false } },
          ]);
          const [first] = calls;
          assert.strictEqual(
            first !== undefined && "advance" in first ? first.advance.value : undefined,
            observation.value,
          );
          observation.detach();
          yield* session.close;
        }),
      ),
  );

  it.effect("gets nothing after detach, which is idempotent and unpins the document", () =>
    run(backend)(
      Effect.gen(function* () {
        const { session } = yield* open;
        yield* add(session);
        const { calls, observer } = recorder();
        const observation = Option.getOrThrow(yield* session.observeDoc(SessionCounter, observer));
        assert.strictEqual(session.documentCacheSizes.attached, 1);
        observation.detach();
        observation.detach();
        assert.strictEqual(session.documentCacheSizes.attached, 0);
        yield* add(session);
        yield* session.close;
        assert.deepStrictEqual(calls, []);
      }),
    ),
  );

  it.effect(
    "ends once when the Session closes, `failed` (S1: an adapter follows the next Session), a watch `ended`",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const { session } = yield* open;
          yield* add(session);
          const { calls, observer } = recorder();
          yield* session.observeDoc(SessionCounter, observer);
          const watched = yield* acquire(session.watchDoc(SessionCounter));
          const frames = yield* Effect.forkChild(Stream.runCollect(watched.value.frames), {
            startImmediately: true,
          });
          yield* session.close;
          assert.strictEqual(failedWith(onlyEnd(calls))?._tag, "SessionClosed");
          // A watch's stream ends without a failure: `ended`.
          assert.deepStrictEqual(yield* Fiber.join(frames), []);
        }),
      ),
  );

  it.effect(
    "ends each close's observers with an error of that close, not one shared by every Session",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const errors: Array<ObserverEnd> = [];
          for (let i = 0; i < 2; i++) {
            const { session } = yield* open;
            yield* add(session);
            yield* session.observeDoc(SessionCounter, {
              advance: () => {},
              end: (end) => errors.push(end),
            });
            yield* session.close;
          }
          assert.lengthOf(errors, 2);
          const closes = errors.map(failedWith);
          for (const error of closes) assert.strictEqual(error?._tag, "SessionClosed");
          assert.notStrictEqual(closes[0], closes[1]);
        }),
      ),
  );

  it.effect("ends with the error that failed the Session", () =>
    run(backend)(
      Effect.gen(function* () {
        const { storage, session } = yield* open;
        yield* add(session);
        const { calls, observer } = recorder();
        yield* session.observeDoc(SessionCounter, observer);
        storage.failNextCommit("disk gone");
        assert.isTrue(Exit.isFailure(yield* Effect.exit(add(session))));
        yield* session.closed;
        const error = failedWith(onlyEnd(calls));
        assert.strictEqual(error?._tag, "SessionFailed");
        assert.include(String(error?._tag === "SessionFailed" ? error.cause : ""), "disk gone");
      }),
    ),
  );

  it.effect(
    "reports a throwing observer, detaches it and ends it with the error; the commit and other observers go on",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const { session, reports } = yield* open;
          yield* add(session);
          const bug = new Error("observer bug");
          const ends: Array<ObserverEnd> = [];
          yield* session.observeDoc(SessionCounter, {
            advance: () => {
              throw bug;
            },
            end: (end) => ends.push(end),
          });
          const healthy = recorder();
          yield* session.observeDoc(SessionCounter, healthy.observer);
          yield* add(session);
          yield* add(session);
          assert.deepStrictEqual(reports, [{ error: bug, info: { kind: "listener_error" } }]);
          assert.deepStrictEqual(ends, [{ _tag: "ListenerError", error: bug }]);
          assert.lengthOf(healthy.calls, 2);
          assert.deepStrictEqual(Option.getOrUndefined(yield* session.snapshot(SessionCounter)), {
            count: 3,
          });
          yield* session.close;
          // Detached at its throw: the close does not end it again.
          assert.lengthOf(ends, 1);
          assert.isTrue(Option.isNone(session.failure));
        }),
      ),
  );

  it.effect(
    "attaches atomically on the line: a commit queued before the attachment is in its value",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const { session } = yield* open;
          yield* add(session);
          const { calls, observer } = recorder();
          const committing = yield* Effect.forkChild(add(session, 10), { startImmediately: true });
          const observation = Option.getOrThrow(
            yield* session.observeDoc(SessionCounter, observer),
          );
          yield* Fiber.join(committing);
          assert.deepStrictEqual(observation.value, { count: 11 });
          assert.deepStrictEqual(calls, []);
          observation.detach();
          yield* session.close;
        }),
      ),
  );

  // The attaching fiber is interrupted while
  // it waits behind a commit that holds the line.
  it.effect("rejects an attachment whose context is cancelled, and attaches nothing", () =>
    run(backend)(
      Effect.gen(function* () {
        const { session } = yield* open;
        yield* add(session);
        const entered = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        const holding = yield* Effect.forkChild(
          session.commit(
            Deferred.succeed(entered, undefined).pipe(Effect.andThen(Deferred.await(release))),
          ),
        );
        yield* Deferred.await(entered);
        const attaching = yield* Effect.forkChild(
          session.observeDoc(SessionCounter, recorder().observer),
          { startImmediately: true },
        );
        yield* Fiber.interrupt(attaching);
        yield* Deferred.succeed(release, undefined);
        yield* Fiber.join(holding);
        assert.isTrue(Exit.hasInterrupts(yield* Fiber.await(attaching)));
        yield* flush;
        assert.strictEqual(session.documentCacheSizes.attached, 0);
        yield* session.close;
      }),
    ),
  );

  it.effect("keeps no detached observer alive", () =>
    run(backend)(
      Effect.gen(function* () {
        const session = yield* openSession(yield* AgentStorage);
        yield* add(session);
        /** Attach an observer only the Session references; `detach` says whether to detach it before returning. */
        const attach = (detach: boolean) =>
          Effect.gen(function* () {
            const observer: Observer<Counter> = { advance: () => {}, end: () => {} };
            const observation = Option.getOrThrow(
              yield* session.observeDoc(SessionCounter, observer),
            );
            if (detach) observation.detach();
            return { observation, observer: new WeakRef(observer) };
          });
        // Control: an attached observer stays, so the collection below is real.
        const attached = yield* attach(false);
        yield* collect;
        assert.isDefined(attached.observer.deref());
        attached.observation.detach();

        const detached = yield* attach(true);
        yield* collect;
        assert.isUndefined(detached.observer.deref());
        yield* flush;
        yield* session.close;
      }),
    ),
  );

  it.effect("rejects, never throws, on a closed Session or a missing scope ID", () =>
    run(backend)(
      Effect.gen(function* () {
        const { session } = yield* open;
        yield* session.close;
        const closed = yield* Effect.flip(session.observeDoc(SessionCounter, recorder().observer));
        assert.strictEqual(closed._tag, "SessionClosed");
        assert.strictEqual(closed.message, "Session is closed");
      }),
    ),
  );

  // Skipped on `sqlite-node`, as `SessionWatches`' "keeps no earlier incarnation alive…": the driver's statement
  // cache keeps the fiber that prepared a statement, and its traced frame the commit's writes.
  it.effect.skipIf(backend.name === "sqlite")(
    "keeps no value of a document retired for good, while still attached",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const session = yield* openSession(yield* AgentStorage);
          const conversationId = yield* createConversation(session);
          yield* session.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              yield* tx.doc(ConversationCounter, conversationId);
            }),
          );
          /** Attach an observer that keeps nothing, change the document, and retire it: only a WeakRef to its last value. */
          const retire = Effect.gen(function* () {
            let last: WeakRef<object> | undefined;
            const observation = Option.getOrThrow(
              yield* session.observeDoc(ConversationCounter, conversationId, {
                advance: (change) => {
                  if (!change.retired) last = new WeakRef(change.value);
                },
                end: () => {},
              }),
            );
            yield* setConversationCount(session, conversationId, 7);
            yield* session.commit(
              Effect.gen(function* () {
                const tx = yield* Tx;
                yield* tx.retireDoc(ConversationCounter, conversationId);
              }),
            );
            yield* session.unloadDocuments;
            // SAFETY: the change above advanced the observer.
            return { observation, last: last! };
          });
          const { observation, last } = yield* retire;
          yield* collect;
          assert.isUndefined(last.deref());
          observation.detach();
          yield* session.close;
        }),
      ),
  );
});

describeEachBackend("observeDoc throwing asynchronously", (backend) => {
  it.effect(
    "reports an observer whose call returns a rejected promise, then detaches it and ends it `listener_error`",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const { session, reports } = yield* open;
          yield* add(session);
          const bug = new Error("async observer bug");
          const ends: Array<ObserverEnd> = [];
          let calls = 0;
          // An async observer: the Effect its call returns fails after the call.
          const observer: Observer<Counter> = {
            advance: () => {
              calls++;
              return Effect.fail(bug);
            },
            end: (end) => ends.push(end),
          };
          yield* session.observeDoc(SessionCounter, observer);
          yield* add(session);
          yield* flush;
          yield* add(session);
          assert.strictEqual(calls, 1);
          assert.deepStrictEqual(reports, [{ error: bug, info: { kind: "listener_error" } }]);
          assert.deepStrictEqual(ends, [{ _tag: "ListenerError", error: bug }]);
          yield* session.close;
          assert.lengthOf(ends, 1);
        }),
      ),
  );
});

/** A commit observer recording each seq and its end. */
const commits = () => {
  const calls: Array<{ readonly seq: number } | { readonly end: ObserverEnd }> = [];
  const observer: CommitObserver = {
    advance: (publication) => calls.push({ seq: publication.seq }),
    end: (end) => calls.push({ end }),
  };
  return { calls, observer };
};

describeEachBackend("observeCommits", (backend) => {
  it.effect(
    "gets every stored commit, then `failed` once the Session closed (S1), after its last publication",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const { session } = yield* open;
          const { calls, observer } = commits();
          yield* session.observeCommits(observer);
          yield* add(session);
          yield* add(session);
          yield* session.close;
          assert.lengthOf(calls, 3);
          assert.isTrue(calls.slice(0, 2).every((call) => "seq" in call));
          const last = calls[2];
          assert.strictEqual(
            failedWith(last !== undefined && "end" in last ? last.end : undefined)?._tag,
            "SessionClosed",
          );
        }),
      ),
  );

  it.effect("ends `failed` with the error that failed the Session", () =>
    run(backend)(
      Effect.gen(function* () {
        const { storage, session } = yield* open;
        const { calls, observer } = commits();
        yield* session.observeCommits(observer);
        storage.failNextCommit("disk gone");
        assert.isTrue(Exit.isFailure(yield* Effect.exit(add(session))));
        yield* session.closed;
        assert.lengthOf(calls, 1);
        const [call] = calls;
        assert.strictEqual(
          failedWith(call !== undefined && "end" in call ? call.end : undefined)?._tag,
          "SessionFailed",
        );
      }),
    ),
  );

  it.effect("gets nothing after detach, which is idempotent, not even its end", () =>
    run(backend)(
      Effect.gen(function* () {
        const { session } = yield* open;
        const { calls, observer } = commits();
        const observation = yield* session.observeCommits(observer);
        yield* add(session);
        yield* flush;
        observation.detach();
        observation.detach();
        yield* add(session);
        yield* session.close;
        assert.lengthOf(calls, 1);
      }),
    ),
  );

  it.effect(
    "reports a throwing observer, detaches it and ends it `listener_error`; the others go on",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const { session, reports } = yield* open;
          const bug = new Error("commit observer bug");
          const ends: Array<ObserverEnd> = [];
          let thrown = 0;
          yield* session.observeCommits({
            advance: () => {
              thrown++;
              throw bug;
            },
            end: (end) => ends.push(end),
          });
          const healthy = commits();
          yield* session.observeCommits(healthy.observer);
          yield* add(session);
          yield* add(session);
          yield* flush;
          assert.strictEqual(thrown, 1);
          assert.deepStrictEqual(reports, [{ error: bug, info: { kind: "listener_error" } }]);
          assert.deepStrictEqual(ends, [{ _tag: "ListenerError", error: bug }]);
          assert.lengthOf(healthy.calls, 2);
          yield* session.close;
          assert.lengthOf(ends, 1);
        }),
      ),
  );
});

describeEachBackend("observeDoc detached during a commit", (backend) => {
  /** A Session with a conversation counter at 1, and the commit that retires it and creates it again at 2. */
  const retiring = Effect.gen(function* () {
    const opened = yield* open;
    const session = opened.session;
    const conversationId = yield* createConversation(session);
    yield* setConversationCount(session, conversationId, 1);
    const recreate = session.commit(
      Effect.gen(function* () {
        const tx = yield* Tx;
        yield* tx.retireDoc(ConversationCounter, conversationId);
        (yield* tx.doc(ConversationCounter, conversationId)).count = 2;
      }),
    );
    return { ...opened, conversationId, recreate };
  });

  it.effect(
    "gets nothing more of the commit once it throws on the retirement, and pins nothing for the recreation",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const { session, reports, conversationId, recreate } = yield* retiring;
          const bug = new Error("observer bug");
          const changes: Array<WatchFrame<Counter>> = [];
          const ends: Array<ObserverEnd> = [];
          const observation = Option.getOrThrow(
            yield* session.observeDoc(ConversationCounter, conversationId, {
              advance: (change) => {
                changes.push(change);
                throw bug;
              },
              end: (end) => ends.push(end),
            }),
          );
          yield* recreate;
          assert.deepStrictEqual(changes, [{ value: observation.value, ops: [], retired: true }]);
          assert.deepStrictEqual(ends, [{ _tag: "ListenerError", error: bug }]);
          assert.deepStrictEqual(reports, [{ error: bug, info: { kind: "listener_error" } }]);
          assert.strictEqual(session.documentCacheSizes.attached, 0);
          observation.detach();
          assert.strictEqual(session.documentCacheSizes.attached, 0);
          yield* session.close;
        }),
      ),
  );

  it.effect(
    "gets nothing more of the commit once it detaches in the retirement call, and pins nothing",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const { session, conversationId, recreate } = yield* retiring;
          const changes: Array<WatchFrame<Counter>> = [];
          const ends: Array<ObserverEnd> = [];
          let observation: Observation<Counter> | undefined;
          observation = Option.getOrUndefined(
            yield* session.observeDoc(ConversationCounter, conversationId, {
              advance: (change) => {
                changes.push(change);
                observation?.detach();
              },
              end: (end) => ends.push(end),
            }),
          );
          yield* recreate;
          assert.deepStrictEqual(changes, [{ value: observation!.value, ops: [], retired: true }]);
          assert.strictEqual(session.documentCacheSizes.attached, 0);
          yield* session.close;
          assert.deepStrictEqual(ends, []);
        }),
      ),
  );

  it.effect(
    "gets nothing of the commit when another observer's call detached it first, and pins nothing",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const { session, conversationId, recreate } = yield* retiring;
          let second: Observation<Counter> | undefined;
          const first = Option.getOrThrow(
            yield* session.observeDoc(ConversationCounter, conversationId, {
              advance: () => second?.detach(),
              end: () => {},
            }),
          );
          const detached = recorder();
          second = Option.getOrUndefined(
            yield* session.observeDoc(ConversationCounter, conversationId, detached.observer),
          );
          yield* recreate;
          assert.deepStrictEqual(detached.calls, []);
          // Only the first observer pins the new incarnation; its detach leaves nothing pinned.
          first.detach();
          assert.strictEqual(session.documentCacheSizes.attached, 0);
          yield* session.close;
        }),
      ),
  );
});

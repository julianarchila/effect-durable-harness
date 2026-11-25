// Kernel tests of Effect-specific paths (an interrupted commit caller, an
// interrupted acquisition) and of the Session's end after a storage failure.
import { assert, it } from "@effect/vitest";
import type * as Tracker from "@effect-durable-harness/delta/Tracker";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as Scheduler from "effect/Scheduler";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import { TestClock } from "effect/testing";
import * as Doc from "../src/Doc.ts";
import type { Transaction } from "../src/internal/transaction.ts";
import * as Task from "../src/Task.ts";
import type { CommitRejected } from "../src/SessionError.ts";
import { Tx, type TxDocError } from "../src/Tx.ts";
import * as RecordCache from "../src/internal/recordCache.ts";
import * as MemoryAgentStorage from "../src/MemoryAgentStorage.ts";
import { ConversationId, TaskId } from "../src/Ids.ts";
import type { TaskRecord } from "../src/Records.ts";
import { AgentStorage, StorageRequestError } from "../src/AgentStorage.ts";
import {
  acquire,
  controlledStorage,
  createConversation,
  describeEachBackend,
  failureMessage,
  flush,
  openSession,
  openTestSession,
  run,
} from "./SessionSupport.ts";

const Counter = Schema.Struct({ count: Schema.Number });
const CounterDoc = Doc.make({
  checkpointWhen: () => false,
  kind: "kernel.counter",
  version: 1,
  scope: "session",
  schema: Counter,
  initial: () => ({ count: 0 }),
});

const OtherDoc = Doc.make({
  checkpointWhen: () => false,
  kind: "kernel.other-counter",
  version: 1,
  scope: "session",
  schema: Counter,
  initial: () => ({ count: 0 }),
});

const WorkTask = Task.make({
  name: "kernel.work",
  version: 1,
  input: Schema.Null,
  checkpoint: Schema.Struct({ phase: Schema.Literal("start") }),
  result: Schema.Null,
  initial: () => ({ phase: "start" }),
  phases: { start: () => Effect.void },
  abort: () => Effect.void,
});

/** The record a cached read and a publication race on, at `version`. */
const raceRecord = (version: number): TaskRecord => ({
  id: TaskId.make(7),
  conversationId: ConversationId.make(1),
  kind: "kernel.race",
  version,
  input: null,
  state: { status: "running", checkpoint: { phase: "start" } },
  background: false,
  abortRequested: false,
});

const bump = Effect.gen(function* () {
  const tx = yield* Tx;
  (yield* tx.doc(CounterDoc)).count++;
});

describeEachBackend("Session kernel", (backend) => {
  it.effect("an interrupted commit caller seals the transaction and revokes its drafts", () =>
    run(backend)(
      Effect.gen(function* () {
        const { session, storage } = yield* openTestSession();
        const inside = yield* Deferred.make<void>();
        let draft: Tracker.Draft<typeof Counter.Type> | undefined;
        let captured: Transaction | undefined;
        const commit = yield* session
          .commitWith((tx) =>
            Effect.gen(function* () {
              captured = tx;
              draft = yield* tx.service.doc(CounterDoc);
              yield* Deferred.succeed(inside, undefined);
              // oxlint-disable-next-line durable/no-external-in-commit -- the test parks the change to interrupt its caller mid-commit.
              return yield* Effect.never;
            }),
          )
          .pipe(Effect.forkChild);
        yield* Deferred.await(inside);
        yield* Fiber.interrupt(commit);
        assert.throws(() => {
          draft!.count = 1;
        });
        assert.include(
          yield* failureMessage(captured!.service.doc(CounterDoc)),
          "Transaction has settled",
        );
        assert.lengthOf(storage.commits, 0);
        // The line is free again.
        yield* session.commit(bump);
        assert.deepStrictEqual(Option.getOrUndefined(yield* session.snapshot(CounterDoc)), {
          count: 1,
        });
      }),
    ),
  );

  it.effect("a storage failure settles `closed` without closing the scope", () =>
    run(backend)(
      Effect.gen(function* () {
        const { session, storage } = yield* openTestSession();
        yield* session.commit(bump);
        storage.failNextCommit("disk vanished");
        const failure = yield* Effect.flip(session.commit(bump));
        const end = yield* session.closed;
        assert.isTrue(end.reason === "failed" && end.error === failure);
        assert.strictEqual(session.documentCacheSizes.cached, 0);
      }),
    ),
  );

  it.effect("a defect from storage commit fails the Session", () =>
    run(backend)(
      Effect.gen(function* () {
        const { session, storage } = yield* openTestSession();
        storage.dieNextCommit("connection lost");
        assert.include(yield* failureMessage(session.commit(bump)), "connection lost");
        assert.strictEqual((yield* Effect.flip(session.commit(bump)))._tag, "SessionFailed");
        assert.strictEqual((yield* session.closed).reason, "failed");
      }),
    ),
  );

  it.effect("watch consumers run after every commit listener", () =>
    run(backend)(
      Effect.gen(function* () {
        const { session } = yield* openTestSession();
        yield* session.commit(bump);
        const order: Array<string> = [];
        const watch = yield* acquire(session.watchDoc(CounterDoc));
        yield* watch.value.frames.pipe(
          Stream.runForEach(() => Effect.sync(() => order.push("watch"))),
          Effect.forkChild,
        );
        yield* flush;
        // A host listener runs after the Session's own listeners, which feed the watch.
        yield* session.observeCommits({
          advance: () => {
            order.push("listener");
          },
          end: () => {},
        });
        yield* session.commit(bump);
        yield* flush;
        assert.deepStrictEqual(order, ["listener", "watch"]);
        yield* watch.release;
      }),
    ),
  );

  it.effect("a failed watch consumer stops the watch and releases its pin", () =>
    run(backend)(
      Effect.gen(function* () {
        const { session } = yield* openTestSession();
        yield* session.commit(bump);
        const watch = yield* acquire(session.watchDoc(CounterDoc));
        assert.strictEqual(session.documentCacheSizes.attached, 1);
        const consumer = yield* watch.value.frames.pipe(
          Stream.runForEach(() => Effect.fail("listener failed")),
          Effect.flip,
          Effect.forkChild,
        );
        yield* session.commit(bump);
        assert.strictEqual(yield* Fiber.join(consumer), "listener failed");
        // The acquiring scope is still open, but the watch is gone.
        assert.strictEqual(session.documentCacheSizes.attached, 0);
        yield* watch.release;
      }),
    ),
  );

  it.effect("a defect from a storage read fails the Session", () =>
    run(backend)(
      Effect.gen(function* () {
        const storage = yield* AgentStorage;
        const session = yield* openSession({
          ...storage,
          findDocument: () => Effect.die(new Error("read invariant broken")),
        });
        assert.include(
          yield* failureMessage(session.snapshot(CounterDoc)),
          "read invariant broken",
        );
        assert.isTrue(Option.isSome(session.failure));
        assert.strictEqual((yield* session.closed).reason, "failed");
      }),
    ),
  );

  it.effect("document addresses stay distinct whatever their kinds and keys contain", () =>
    run(backend)(
      Effect.gen(function* () {
        const { session } = yield* openTestSession();
        const family = Doc.family({
          checkpointWhen: () => false,
          kind: "a",
          family: true,
          scope: "session",
          version: 1,
          schema: Counter,
          initial: (_seed: null) => ({ count: 0 }),
        });
        const singleton = Doc.make({
          checkpointWhen: () => false,
          kind: "a\u0000session\u0000-\u0000kx",
          scope: "session",
          version: 1,
          schema: Counter,
          initial: () => ({ count: 0 }),
        });
        const key = "x\u0000session\u0000-\u0000s";
        yield* session.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            (yield* tx.doc(family, key, null)).count = 1;
            (yield* tx.doc(singleton)).count = 2;
          }),
        );
        assert.deepStrictEqual(Option.getOrUndefined(yield* session.snapshot(family, key)), {
          count: 1,
        });
        assert.deepStrictEqual(Option.getOrUndefined(yield* session.snapshot(singleton)), {
          count: 2,
        });
      }),
    ),
  );

  it.effect("a watch consumer sees the end of its stream after every close listener", () =>
    run(backend)(
      Effect.gen(function* () {
        const { session } = yield* openTestSession();
        yield* session.commit(bump);
        const order: Array<string> = [];
        const watch = yield* acquire(session.watchDoc(CounterDoc));
        const consumer = yield* watch.value.frames.pipe(
          Stream.runDrain,
          Effect.andThen(Effect.sync(() => order.push("watch ended"))),
          Effect.forkChild,
        );
        yield* flush;
        yield* session.subscribeClose(() => {
          order.push("close listener");
        });
        yield* session.close;
        yield* Fiber.join(consumer);
        assert.deepStrictEqual(order, ["close listener", "watch ended"]);
      }),
    ),
  );

  it.effect(
    "a watch consumer that ends early leaves the watch and its buffer to the next one",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const { session } = yield* openTestSession();
          yield* session.commit(bump);
          const watch = yield* acquire(session.watchDoc(CounterDoc));
          yield* session.commit(bump);
          const first = yield* watch.value.frames.pipe(Stream.take(1), Stream.runCollect);
          // Pending between the two consumers: the next one takes it.
          yield* session.commit(bump);
          const second = yield* watch.value.frames.pipe(Stream.take(1), Stream.runCollect);
          assert.deepStrictEqual(
            [...first, ...second].map((frame) => frame.value.count),
            [2, 3],
          );
          assert.strictEqual(session.documentCacheSizes.attached, 1);
          yield* watch.release;
        }),
      ),
  );

  it.effect("a storage defect keeps its original error as the Session's failure", () =>
    run(backend)(
      Effect.gen(function* () {
        const storage = yield* AgentStorage;
        const defect = new Error("read invariant broken");
        const session = yield* openSession({ ...storage, findDocument: () => Effect.die(defect) });
        yield* Effect.exit(session.snapshot(CounterDoc));
        const end = yield* session.closed;
        assert.isTrue(end.reason === "failed" && end.error === defect);
        // A call after the failure gets `SessionFailed`, whatever the storage would do.
        assert.strictEqual(
          (yield* Effect.flip(session.snapshot(CounterDoc)))._tag,
          "SessionFailed",
        );
      }),
    ),
  );

  it.effect("a storage method that throws before returning its Effect fails the Session", () =>
    run(backend)(
      Effect.gen(function* () {
        const storage = yield* AgentStorage;
        const session = yield* openSession({
          ...storage,
          findDocument: () => {
            throw new Error("threw synchronously");
          },
        });
        assert.include(yield* failureMessage(session.snapshot(CounterDoc)), "threw synchronously");
        assert.isTrue(Option.isSome(session.failure));
      }),
    ),
  );

  it.effect("an interruption from storage commit fails the Session", () =>
    run(backend)(
      Effect.gen(function* () {
        const storage = yield* AgentStorage;
        let commits = 0;
        const session = yield* openSession({
          ...storage,
          commit: (batches, receive) =>
            storage
              .commit(batches, receive)
              .pipe(
                Effect.andThen((result) =>
                  ++commits === 2 ? Effect.interrupt : Effect.succeed(result),
                ),
              ),
        });
        yield* session.commit(bump);
        // The caller gets the Session's failure, an error it can handle, not an interruption it never asked for.
        assert.strictEqual((yield* Effect.flip(session.commit(bump)))._tag, "SessionFailed");
        // Whether the batch committed is unknown, so the Session must not keep serving its memory.
        assert.isTrue(Option.isSome(session.failure));
        assert.strictEqual((yield* Effect.flip(session.commit(bump)))._tag, "SessionFailed");
      }),
    ),
  );

  // A `Tx` operation runs to completion once started: interrupting its fiber takes
  // effect when it finishes. The next cases interrupt operations held in storage and check that nothing
  // is left half done.

  it.effect("an interrupted acquisition still completes and is memoized", () =>
    run(backend)(
      Effect.gen(function* () {
        const { session, storage } = yield* openTestSession();
        const gate = yield* storage.holdFindDocument;
        yield* session.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            const first = yield* Effect.forkChild(tx.doc(CounterDoc), { startImmediately: true });
            yield* gate.entered;
            const interrupting = yield* Effect.forkChild(Fiber.interrupt(first), {
              startImmediately: true,
            });
            // The interruption waits for the acquisition, which waits for storage.
            assert.isUndefined(interrupting.pollUnsafe());
            yield* gate.release;
            yield* Fiber.join(interrupting);
            (yield* tx.doc(CounterDoc)).count = 5;
          }),
        );
        assert.deepStrictEqual(Option.getOrUndefined(yield* session.snapshot(CounterDoc)), {
          count: 5,
        });
        assert.strictEqual(storage.mintCount, 1);
      }),
    ),
  );

  it.effect(
    "an interrupted retirement still completes, so a later acquisition creates the replacement",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const { session, storage } = yield* openTestSession();
          yield* session.commit(bump);
          yield* session.unloadDocuments;
          const gate = yield* storage.holdFindDocument;
          yield* session.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              const retiring = yield* Effect.forkChild(tx.retireDoc(CounterDoc), {
                startImmediately: true,
              });
              yield* gate.entered;
              const interrupting = yield* Effect.forkChild(Fiber.interrupt(retiring), {
                startImmediately: true,
              });
              yield* gate.release;
              yield* Fiber.join(interrupting);
              (yield* tx.doc(CounterDoc)).count = 7;
            }),
          );
          const writes = storage.commits.at(-1)!;
          assert.lengthOf(
            writes.filter((write) => write.type === "document.retire"),
            1,
          );
          assert.deepStrictEqual(Option.getOrUndefined(yield* session.snapshot(CounterDoc)), {
            count: 7,
          });
        }),
      ),
  );

  it.effect(
    "an acquisition that waits on an interrupted one shares its draft and its retirement",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const { session, storage } = yield* openTestSession();
          yield* session.commit(bump);
          yield* session.unloadDocuments;
          const gate = yield* storage.holdFindDocument;
          yield* session.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              const first = yield* Effect.forkChild(tx.doc(CounterDoc), { startImmediately: true });
              yield* gate.entered;
              const second = yield* Effect.forkChild(tx.doc(CounterDoc), {
                startImmediately: true,
              });
              const retiring = yield* Effect.forkChild(tx.retireDoc(CounterDoc), {
                startImmediately: true,
              });
              const interrupting = yield* Effect.forkChild(Fiber.interrupt(first), {
                startImmediately: true,
              });
              yield* gate.release;
              yield* Fiber.join(interrupting);
              (yield* Fiber.join(second)).count = 99;
              yield* Fiber.join(retiring);
            }),
          );
          // The second `doc()` came before `retireDoc`, so it edits the incarnation that is retired.
          assert.isTrue(Option.isNone(yield* session.snapshot(CounterDoc)));
        }),
      ),
  );

  it.effect("a retirement error the callback catches does not reject the commit", () =>
    run(backend)(
      Effect.gen(function* () {
        const { session } = yield* openTestSession();
        const Latest = Doc.make({
          checkpointWhen: () => false,
          kind: "kernel.notes",
          version: 1,
          scope: "conversation",
          history: "latest",
          fork: "initial",
          schema: Counter,
          initial: () => ({ count: 0 }),
        });
        const Rewindable = Doc.make({
          checkpointWhen: () => false,
          kind: "kernel.notes",
          version: 1,
          scope: "conversation",
          history: "rewindable",
          fork: "asOf",
          schema: Counter,
          initial: () => ({ count: 0 }),
        });
        const conversationId = yield* createConversation(session);
        yield* session.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            yield* tx.doc(Latest, conversationId);
          }),
        );
        yield* session.unloadDocuments;
        yield* session.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            const retired = yield* Effect.flip(tx.retireDoc(Rewindable, conversationId));
            assert.strictEqual(retired._tag, "DocumentAccessError");
            yield* bump;
          }),
        );
        // The other write commits: the failed lookup staged nothing.
        assert.deepStrictEqual(Option.getOrUndefined(yield* session.snapshot(CounterDoc)), {
          count: 1,
        });
        assert.isTrue(Option.isSome(yield* session.snapshot(Latest, conversationId)));
      }),
    ),
  );

  // A scheduler that yields every few operations lets an interruption land at any step of a `Tx`
  // operation's start; sweeping the number of turns before it covers each of them.
  it.effect(
    "an interruption at any step of a retirement or an acquisition leaves nothing half done",
    () =>
      run(backend)(
        Effect.gen(function* () {
          for (let turns = 0; turns < 12; turns++) {
            const { session, storage } = yield* openTestSession();
            yield* session.commit(bump);
            yield* session.unloadDocuments;
            yield* session.commit(
              Effect.gen(function* () {
                const tx = yield* Tx;
                const retiring = yield* Effect.forkChild(
                  tx
                    .retireDoc(CounterDoc)
                    .pipe(Effect.provideService(Scheduler.MaxOpsBeforeYield, 3)),
                  { startImmediately: true },
                );
                const acquiring = yield* Effect.forkChild(
                  tx.doc(OtherDoc).pipe(Effect.provideService(Scheduler.MaxOpsBeforeYield, 3)),
                  { startImmediately: true },
                );
                for (let turn = 0; turn < turns; turn++) yield* Effect.yieldNow;
                yield* Fiber.interrupt(retiring);
                yield* Fiber.interrupt(acquiring);
                (yield* tx.doc(CounterDoc)).count = 7;
                (yield* tx.doc(OtherDoc)).count = 3;
              }),
            );
            assert.isTrue(Option.isNone(session.failure));
            const retires = storage.commits
              .at(-1)!
              .filter((write) => write.type === "document.retire");
            assert.isAtMost(retires.length, 1);
            assert.deepStrictEqual(Option.getOrUndefined(yield* session.snapshot(OtherDoc)), {
              count: 3,
            });
            yield* session.close;
          }
        }),
      ),
  );

  it.effect("an interruption from storage mintId fails the Session", () =>
    run(backend)(
      Effect.gen(function* () {
        const storage = yield* AgentStorage;
        const session = yield* openSession({ ...storage, mintId: Effect.interrupt });
        assert.strictEqual((yield* Effect.flip(session.commit(bump)))._tag, "SessionFailed");
        assert.isTrue(Option.isSome(session.failure));
      }),
    ),
  );

  it.effect("a caller interrupted at any step of taking the line does not hold it", () =>
    run(backend)(
      Effect.gen(function* () {
        for (const ops of [3, 4]) {
          for (const kind of ["commit", "snapshot"] as const) {
            for (let turns = 0; turns < 16; turns++) {
              const { session } = yield* openTestSession();
              yield* session.commit(bump);
              yield* session.unloadDocuments;
              const call = kind === "commit" ? session.commit(bump) : session.snapshot(CounterDoc);
              const caller = yield* Effect.forkChild(
                call.pipe(Effect.provideService(Scheduler.MaxOpsBeforeYield, ops)),
                { startImmediately: true },
              );
              for (let turn = 0; turn < turns; turn++) yield* Effect.yieldNow;
              yield* Fiber.interrupt(caller);
              // The next commit and close get the line.
              const next = yield* Effect.forkChild(session.commit(bump), {
                startImmediately: true,
              });
              yield* flush;
              assert.isDefined(next.pollUnsafe(), `line held: ${kind}, ops ${ops}, turns ${turns}`);
              yield* session.close;
            }
          }
        }
      }),
    ),
  );

  // The Session's failure is checked right before `AgentStorage.commit`. A failure
  // from another fiber at any step of a commit means no storage call starts after it.
  it.effect("no storage commit starts after another fiber failed the Session", () =>
    run(backend)(
      Effect.gen(function* () {
        for (const ops of [3, 4, 5, 8]) {
          for (let turns = 0; turns < 24; turns++) {
            const base = yield* AgentStorage;
            // Counts the calls the Session starts: the function call, made in the step that checks the failure.
            let calls = 0;
            const session = yield* openSession({
              ...base,
              commit: (batches, receive) => {
                calls++;
                return base.commit(batches, receive);
              },
            });
            yield* session.commit(bump);
            const before = calls;
            const committing = yield* Effect.forkChild(
              session.commit(bump).pipe(Effect.provideService(Scheduler.MaxOpsBeforeYield, ops)),
              { startImmediately: true },
            );
            for (let turn = 0; turn < turns; turn++) yield* Effect.yieldNow;
            session.fail(new Error("failed elsewhere"));
            const atFailure = calls;
            yield* Fiber.await(committing);
            assert.strictEqual(calls, atFailure, `ops ${ops}, turns ${turns}`);
            assert.isAtMost(atFailure - before, 1);
            yield* session.closed;
          }
        }
      }),
    ),
  );

  // A read and a publication of the same record race at every yield point. The read's end, its
  // version check and its keep are one step, so a publication that lands after the read returned
  // and before it kept its result is never overwritten by the older value.
  it.effect(
    "a record publication racing a cached read at any yield point is never overwritten",
    () =>
      Effect.gen(function* () {
        const base = yield* AgentStorage;
        for (const ops of [3, 4, 5, 8]) {
          for (let turns = 0; turns < 24; turns++) {
            // A backend that serves what is stored now, as a real one does.
            let stored = raceRecord(1);
            const cache = RecordCache.make({
              ...base,
              task: () => Effect.sync(() => Option.some(stored)),
            });
            const reader = yield* Effect.forkChild(
              cache.storage
                .task(TaskId.make(7))
                .pipe(Effect.provideService(Scheduler.MaxOpsBeforeYield, ops)),
              { startImmediately: true },
            );
            for (let turn = 0; turn < turns; turn++) yield* Effect.yieldNow;
            stored = raceRecord(2);
            cache.published([{ type: "task", value: stored }]);
            yield* Fiber.join(reader);
            const kept = Option.getOrUndefined(yield* cache.storage.task(TaskId.make(7)));
            assert.strictEqual(kept?.version, 2, `ops ${ops}, turns ${turns}`);
            assert.strictEqual(cache.readsInFlight(), 0);
          }
        }
      }).pipe(Effect.provide(MemoryAgentStorage.layer)),
  );

  // With group commit a caller waits for its delivery off the line, behind the commits before it. An
  // interruption at any step, while an earlier call is in flight, leaves the line free and every later outcome
  // delivered, and the line state returns to zero.
  it.effect(
    "a commit caller interrupted at any step behind a call in flight leaves the line state at zero",
    () =>
      run(backend)(
        Effect.gen(function* () {
          for (const ops of [3, 4, 5, 8]) {
            for (let turns = 0; turns < 16; turns++) {
              const { session, storage } = yield* openTestSession();
              yield* session.commit(bump);
              const held = yield* storage.holdCommits;
              const first = yield* Effect.forkChild(session.commit(bump), {
                startImmediately: true,
              });
              yield* held.entered;
              const caller = yield* Effect.forkChild(
                session.commit(bump).pipe(Effect.provideService(Scheduler.MaxOpsBeforeYield, ops)),
                { startImmediately: true },
              );
              for (let turn = 0; turn < turns; turn++) yield* Effect.yieldNow;
              // A settlement runs to its end once admitted, so the interruption may wait for
              // a gate the held call keeps closed: it is joined after the release.
              const interrupting = yield* Effect.forkChild(Fiber.interrupt(caller), {
                startImmediately: true,
              });
              // A later commit is delivered once the call in flight is released.
              const later = yield* Effect.forkChild(session.commit(bump), {
                startImmediately: true,
              });
              yield* held.release;
              yield* Fiber.join(interrupting);
              yield* Fiber.join(first);
              yield* Fiber.join(later);
              for (let turn = 0; turn < 50 && session.lineSizes.outstanding > 0; turn++) {
                yield* Effect.yieldNow;
              }
              assert.deepStrictEqual(
                session.lineSizes,
                { outstanding: 0, unpublished: 0, reads: 0 },
                `ops ${ops}, turns ${turns}`,
              );
              assert.isTrue(Option.isNone(session.failure));
              yield* session.close;
            }
          }
        }),
      ),
  );

  // The record cache registers a read in flight and releases it when the read ends; an interruption
  // of its caller at any step must not leave it registered, or the versions it keeps would grow forever.
  it.effect("a cached record read interrupted at any step leaves no read in flight", () =>
    run(backend)(
      Effect.gen(function* () {
        for (const ops of [3, 4, 5, 8]) {
          for (let turns = 0; turns < 12; turns++) {
            const storage = yield* controlledStorage;
            const held = yield* Deferred.make<void>();
            storage.intercept.task = (_id, next) => Deferred.await(held).pipe(Effect.andThen(next));
            const session = yield* openSession(storage);
            const conversationId = yield* createConversation(session);
            const id = yield* session.commit(
              Effect.gen(function* () {
                const tx = yield* Tx;
                return yield* tx.createTask(WorkTask, null, {
                  ownership: { kind: "conversation" },
                  conversationId,
                });
              }),
            );
            // A reopened Session has not cached it: the read goes to storage and waits there.
            yield* session.close;
            const reopened = yield* openSession(storage);
            const reader = yield* Effect.forkChild(
              reopened.storage
                .task(id)
                .pipe(Effect.provideService(Scheduler.MaxOpsBeforeYield, ops)),
              { startImmediately: true },
            );
            for (let turn = 0; turn < turns; turn++) yield* Effect.yieldNow;
            // A storage read runs to completion (see `guard`): the interruption waits for it.
            const interrupting = yield* Effect.forkChild(Fiber.interrupt(reader), {
              startImmediately: true,
            });
            yield* Deferred.succeed(held, undefined);
            yield* Fiber.join(interrupting);
            assert.strictEqual(
              reopened.readCacheSizes.readsInFlight,
              0,
              `ops ${ops}, turns ${turns}`,
            );
            yield* reopened.close;
          }
        }
      }),
    ),
  );

  // `offLine` counts a read close waits for; an interruption anywhere between taking the count
  // and releasing it must not leave it counted, or close would wait forever.
  it.effect("an off-line read interrupted at any step does not hold close", () =>
    run(backend)(
      Effect.gen(function* () {
        for (const ops of [3, 4, 5, 8]) {
          for (let turns = 0; turns < 16; turns++) {
            const { session } = yield* openTestSession();
            const read = session.offLine(
              Effect.forEach(Array.from({ length: 4 }), () => Effect.yieldNow, { discard: true }),
            );
            const reader = yield* Effect.forkChild(
              read.pipe(Effect.provideService(Scheduler.MaxOpsBeforeYield, ops)),
              { startImmediately: true },
            );
            for (let turn = 0; turn < turns; turn++) yield* Effect.yieldNow;
            yield* Fiber.interrupt(reader);
            const closing = yield* Effect.forkChild(session.close, { startImmediately: true });
            yield* flush;
            assert.isDefined(closing.pollUnsafe(), `close held: ops ${ops}, turns ${turns}`);
          }
        }
      }),
    ),
  );

  it.effect("a read failure still fails the Session when its caller was interrupted", () =>
    run(backend)(
      Effect.gen(function* () {
        const storage = yield* AgentStorage;
        const reading = yield* Deferred.make<void>();
        const proceed = yield* Deferred.make<void>();
        const session = yield* openSession({
          ...storage,
          findDocument: () =>
            Deferred.succeed(reading, undefined).pipe(
              Effect.andThen(Deferred.await(proceed)),
              Effect.andThen(Effect.die(new Error("disk vanished"))),
            ),
        });
        const caller = yield* Effect.forkChild(session.snapshot(CounterDoc), {
          startImmediately: true,
        });
        yield* Deferred.await(reading);
        const interrupting = yield* Effect.forkChild(Fiber.interrupt(caller), {
          startImmediately: true,
        });
        yield* Deferred.succeed(proceed, undefined);
        yield* Fiber.join(interrupting);
        assert.isTrue(Option.isSome(session.failure));
      }),
    ),
  );

  it.effect("a forked setTask that succeeds is always part of the commit", () =>
    run(backend)(
      Effect.gen(function* () {
        const { session, storage } = yield* openTestSession();
        const conversationId = yield* createConversation(session);
        const taskId = yield* session.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            return yield* tx.createTask(WorkTask, null, {
              ownership: { kind: "conversation" },
              conversationId,
            });
          }),
        );
        for (let attempt = 0; attempt < 8; attempt++) {
          let forked: Fiber.Fiber<void, CommitRejected> | undefined;
          const committed = yield* session
            .commitWith((tx) =>
              Effect.gen(function* () {
                const task = Option.getOrThrow(yield* tx.service.task(taskId));
                forked = yield* Effect.forkChild(
                  tx
                    .setTask({
                      ...task,
                      state: { status: "running", checkpoint: { phase: "start" } },
                    })
                    .pipe(Effect.provideService(Scheduler.MaxOpsBeforeYield, 3)),
                  { startImmediately: true },
                );
                for (let turn = 0; turn < attempt; turn++) yield* Effect.yieldNow;
              }),
            )
            .pipe(Effect.exit);
          const staged = yield* Fiber.await(forked!);
          const status = Option.getOrThrow(yield* storage.task(taskId)).state.status;
          // The write is part of the commit; or it started too late and was rejected; or it was still
          // pending when the callback returned, so the commit was rejected. Never a committed
          // transaction, a successful `setTask` and a lost write.
          if (committed._tag === "Success" && staged._tag === "Success")
            assert.strictEqual(status, "running");
          else assert.strictEqual(status, "pending");
          if (status === "running") return;
        }
        assert.fail("setTask never made it into a commit");
      }),
    ),
  );

  it.effect("a retention setting that fails counts as the default", () =>
    run(backend)(
      Effect.gen(function* () {
        const { session } = yield* openTestSession({
          documentRetention: () => {
            throw new Error("settings broke");
          },
        });
        yield* session.commit(bump);
        assert.deepStrictEqual(Option.getOrUndefined(yield* session.snapshot(CounterDoc)), {
          count: 1,
        });
        assert.isTrue(Option.isNone(session.failure));
      }),
    ),
  );

  it.effect("a forked document edit that succeeds is always part of the commit", () =>
    run(backend)(
      Effect.gen(function* () {
        const { session } = yield* openTestSession();
        for (let attempt = 0; attempt < 8; attempt++) {
          const before = Option.getOrUndefined(yield* session.snapshot(CounterDoc))?.count ?? 0;
          let forked: Fiber.Fiber<void, TxDocError> | undefined;
          const committed = yield* session
            .commit(
              Effect.gen(function* () {
                forked = yield* Effect.forkChild(
                  bump.pipe(Effect.provideService(Scheduler.MaxOpsBeforeYield, 3)),
                  { startImmediately: true },
                );
                for (let turn = 0; turn < attempt; turn++) yield* Effect.yieldNow;
              }),
            )
            .pipe(Effect.exit);
          const edited = yield* Fiber.await(forked!);
          const after = Option.getOrUndefined(yield* session.snapshot(CounterDoc))?.count ?? 0;
          if (committed._tag === "Success" && edited._tag === "Success")
            assert.strictEqual(after, before + 1);
          else assert.strictEqual(after, before);
        }
      }),
    ),
  );

  it.effect("an unexpected error from storage commit fails the Session", () =>
    run(backend)(
      Effect.gen(function* () {
        const storage = yield* AgentStorage;
        const unexpected = new StorageRequestError({ message: "not a commit error" });
        const session = yield* openSession({
          ...storage,
          // SAFETY: deliberately breaks the backend's type, as a faulty backend could.
          commit: () => Effect.fail(unexpected) as never,
        });
        assert.strictEqual((yield* Effect.flip(session.commit(bump)))._tag, "SessionFailed");
        // The Session keeps the backend's own error.
        const end = yield* session.closed;
        assert.isTrue(end.reason === "failed" && end.error === unexpected);
      }),
    ),
  );

  it.effect("an observer acquisition interrupted after it attached releases at once", () =>
    run(backend)(
      Effect.gen(function* () {
        const { session } = yield* openTestSession();
        yield* session.commit(bump);
        const scope = yield* Scope.make();
        for (let turns = 0; turns < 40; turns++) {
          for (const open of [session.watchDoc(CounterDoc).pipe(Effect.asVoid)]) {
            const acquiring = yield* Effect.forkChild(
              Scope.provide(open, scope).pipe(
                Effect.provideService(Scheduler.MaxOpsBeforeYield, 3),
              ),
              { startImmediately: true },
            );
            for (let turn = 0; turn < turns; turn++) yield* Effect.yieldNow;
            yield* Fiber.interrupt(acquiring);
            const exit = yield* Fiber.await(acquiring);
            if (exit._tag === "Failure") {
              assert.strictEqual(session.documentCacheSizes.attached, 0, `turns ${turns}`);
            }
          }
        }
        yield* Scope.close(scope, Exit.void);
      }),
    ),
  );

  it.effect("a terminal setTask is never overwritten by a concurrent one", () =>
    run(backend)(
      Effect.gen(function* () {
        const { session, storage } = yield* openTestSession();
        const conversationId = yield* createConversation(session);
        for (let turns = 0; turns < 10; turns++) {
          const taskId = yield* session.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              return yield* tx.createTask(WorkTask, null, {
                ownership: { kind: "conversation" },
                conversationId,
              });
            }),
          );
          yield* session.commitWith((tx) =>
            Effect.gen(function* () {
              const task = Option.getOrThrow(yield* tx.service.task(taskId));
              const running = yield* Effect.forkChild(
                tx
                  .setTask({
                    ...task,
                    state: { status: "running", checkpoint: { phase: "start" } },
                  })
                  .pipe(Effect.provideService(Scheduler.MaxOpsBeforeYield, 3)),
                { startImmediately: true },
              );
              for (let turn = 0; turn < turns; turn++) yield* Effect.yieldNow;
              yield* tx.setTask({
                ...task,
                state: { status: "terminal", outcome: { status: "completed", result: null } },
              });
              yield* Fiber.await(running);
            }),
          );
          assert.strictEqual(
            Option.getOrThrow(yield* storage.task(taskId)).state.status,
            "terminal",
          );
        }
      }),
    ),
  );

  it.effect("an interrupted run of a reused observer Effect leaves the other runs' observers", () =>
    run(backend)(
      Effect.gen(function* () {
        const { session, storage } = yield* openTestSession();
        yield* session.commit(bump);
        const open = session.watchDoc(CounterDoc);
        const first = yield* acquire(open);
        const gate = yield* storage.holdCommits;
        const holding = yield* Effect.forkChild(session.commit(bump), { startImmediately: true });
        yield* gate.entered;
        const second = yield* Effect.forkChild(Effect.scoped(open), { startImmediately: true });
        yield* Fiber.interrupt(second);
        yield* gate.release;
        yield* Fiber.join(holding);
        assert.strictEqual(session.documentCacheSizes.attached, 1);
        const [frame] = yield* first.value.frames.pipe(Stream.take(1), Stream.runCollect);
        assert.deepStrictEqual(frame!.value, { count: 2 });
        yield* first.release;
      }),
    ),
  );

  it.effect("a released pin evicts on time without another Session operation", () =>
    run(backend)(
      Effect.gen(function* () {
        const { session } = yield* openTestSession({
          documentRetention: () => Duration.zero,
        });
        yield* session.commit(bump);
        const watch = yield* acquire(session.watchDoc(CounterDoc));
        const consumer = yield* watch.value.frames.pipe(
          Stream.runForEach(() => Effect.fail("listener failed")),
          Effect.flip,
          Effect.forkChild,
        );
        yield* session.commit(bump);
        yield* Fiber.join(consumer);
        // The failed consumer stopped the watch and released its pin; nothing else runs on the Session.
        yield* flush;
        assert.strictEqual(session.documentCacheSizes.cached, 0);
        yield* watch.release;
      }),
    ),
  );

  it.effect("no operation reaches storage after close completes, wherever its fiber yields", () =>
    run(backend)(
      Effect.gen(function* () {
        const storage = yield* AgentStorage;
        for (const kind of ["commit", "snapshot"] as const) {
          for (let turns = 0; turns < 16; turns++) {
            let closed = false;
            const late: Array<string> = [];
            const session = yield* openSession({
              ...storage,
              commit: (batches, receive) =>
                Effect.suspend(() => {
                  if (closed) late.push("commit");
                  return storage.commit(batches, receive);
                }),
              findDocument: (address, at) =>
                Effect.suspend(() => {
                  if (closed) late.push("findDocument");
                  return storage.findDocument(address, at);
                }),
            });
            const call = kind === "commit" ? session.commit(bump) : session.snapshot(CounterDoc);
            const caller = yield* Effect.forkChild(
              call.pipe(Effect.provideService(Scheduler.MaxOpsBeforeYield, 3)),
              { startImmediately: true },
            );
            for (let turn = 0; turn < turns; turn++) yield* Effect.yieldNow;
            yield* session.close;
            closed = true;
            yield* Fiber.await(caller);
            assert.deepStrictEqual(late, [], `${kind}, turns ${turns}`);
          }
        }
      }),
    ),
  );

  it.effect("an interrupted sweep scheduling does not disable eviction", () =>
    run(backend)(
      Effect.gen(function* () {
        for (let turns = 0; turns < 16; turns++) {
          const retention = { value: Duration.infinity };
          const { session } = yield* openTestSession({ documentRetention: () => retention.value });
          yield* session.commit(bump);
          retention.value = Duration.millis(100);
          // Past the touch granule (a tenth of the retention), so the next use reschedules and evicts
          // promptly once the retention is shortened.
          yield* TestClock.adjust(Duration.millis(50));
          const reading = yield* Effect.forkChild(
            session
              .snapshot(CounterDoc)
              .pipe(Effect.provideService(Scheduler.MaxOpsBeforeYield, 3)),
            { startImmediately: true },
          );
          for (let turn = 0; turn < turns; turn++) yield* Effect.yieldNow;
          yield* Fiber.interrupt(reading);
          yield* session.snapshot(CounterDoc);
          yield* TestClock.adjust(Duration.millis(1_000));
          // The sweep fiber may inherit the slow scheduler of the fiber that forked it: wait, bounded.
          for (let attempt = 0; attempt < 200 && session.documentCacheSizes.cached > 0; attempt++) {
            yield* session.readOnLine(Effect.void);
            yield* Effect.yieldNow;
          }
          assert.strictEqual(session.documentCacheSizes.cached, 0, `turns ${turns}`);
          yield* session.close;
        }
      }),
    ),
  );

  it.effect("a close listener registered while close begins is refused or notified", () =>
    run(backend)(
      Effect.gen(function* () {
        for (let turns = 0; turns < 12; turns++) {
          const { session } = yield* openTestSession();
          let notified = 0;
          const registering = yield* Effect.forkChild(
            session
              .subscribeClose(() => {
                notified++;
              })
              .pipe(Effect.provideService(Scheduler.MaxOpsBeforeYield, 3)),
            { startImmediately: true },
          );
          for (let turn = 0; turn < turns; turn++) yield* Effect.yieldNow;
          yield* session.close;
          const registered = yield* Fiber.await(registering);
          if (registered._tag === "Success") assert.strictEqual(notified, 1, `turns ${turns}`);
        }
      }),
    ),
  );

  it.effect("a migration's receiver is its definition", () =>
    run(backend)(
      Effect.gen(function* () {
        const { session } = yield* openTestSession();
        yield* session.commit(bump);
        const Current = Doc.make({
          checkpointWhen: () => false,
          kind: "kernel.counter",
          version: 2,
          scope: "session",
          schema: Schema.Struct({ count: Schema.Number, labels: Schema.Array(Schema.String) }),
          initial: () => ({ count: 0, labels: ["initial"] }),
          migrate(value) {
            // `this` is typed before `T` is inferred, so the initial value reads as plain JSON here.
            return {
              count: Number(value["count"]),
              labels: [String(this.initial()["labels"]), "migrated"],
            };
          },
        });
        assert.deepStrictEqual(Option.getOrUndefined(yield* session.snapshot(Current)), {
          count: 1,
          labels: ["initial", "migrated"],
        });
      }),
    ),
  );

  it.effect("a long queue of jobs that never suspend all run, and close completes", () =>
    run(backend)(
      Effect.gen(function* () {
        const { session } = yield* openTestSession();
        const release = yield* Deferred.make<void>();
        const holding = yield* Effect.forkChild(session.readOnLine(Deferred.await(release)), {
          startImmediately: true,
        });
        let ran = 0;
        const queued = yield* Effect.forEach(Array.from({ length: 10_000 }), () =>
          Effect.forkChild(
            session.readOnLine(
              Effect.sync(() => {
                ran++;
              }),
            ),
            { startImmediately: true },
          ),
        );
        yield* Deferred.succeed(release, undefined);
        yield* Fiber.join(holding);
        yield* Effect.forEach(queued, (fiber) => Fiber.join(fiber), { discard: true });
        assert.strictEqual(ran, 10_000);
        yield* session.close;
      }),
    ),
  );

  it.effect("a duplicate acquisition that awaits a memo commits once its producer settles", () =>
    run(backend)(
      Effect.gen(function* () {
        // The producer may yield anywhere, so the order of its bookkeeping must not depend on timing.
        for (const ops of [3, 4, 5, 6, 7, 8, 2048]) {
          const { session, storage } = yield* openTestSession();
          const gate = yield* storage.holdFindDocument;
          const waiting = yield* Deferred.make<void>();
          const commit = yield* Effect.forkChild(
            session.commit(
              Effect.gen(function* () {
                const tx = yield* Tx;
                yield* Effect.forkChild(
                  tx.doc(CounterDoc).pipe(Effect.provideService(Scheduler.MaxOpsBeforeYield, ops)),
                  { startImmediately: true },
                );
                yield* Deferred.succeed(waiting, undefined);
                (yield* tx.doc(CounterDoc)).count = 4;
              }),
            ),
            { startImmediately: true },
          );
          yield* gate.entered;
          yield* Deferred.await(waiting);
          yield* gate.release;
          yield* Fiber.join(commit);
          assert.deepStrictEqual(Option.getOrUndefined(yield* session.snapshot(CounterDoc)), {
            count: 4,
          });
          yield* session.close;
        }
      }),
    ),
  );
});

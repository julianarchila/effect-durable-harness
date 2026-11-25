// Group commit on the Session line, on every backend: batching, gates, outcome fences, publication, failures per
// part, backpressure, bounds, and a randomized interleaving against an oracle.
//
// - `batchStorage` wraps the `AgentStorage` in context, with controls: held calls, recorded calls, a refusal by
//   predicate, and a rejection of one call.
// - A refusal and a rejection are typed `StorageFailure`s (`AgentStorage.commit`).
// - Random latency is a random number of scheduler turns, so the case is deterministic under `TestClock`.
// - The Session's own observers are `followCommits`; `ObservedCommit` kinds (`stored`, `memory`) come with
//   deferred commits.
//
// Skipped, each named in place: the cases that need module mocking to make adoption or publication throw, and
// those for a storage's own publication scope (`StoreStorage`), which no storage here has.
import { assert, it } from "@effect/vitest";
import * as Cause from "effect/Cause";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import {
  AgentStorage,
  Backend,
  type CommitResult,
  StorageFailure,
  type StorageWrite,
} from "../src/AgentStorage.ts";
import * as Doc from "../src/Doc.ts";
import { type ConversationId, type EntryId, TaskId } from "../src/Ids.ts";
import type { ObservedCommit, SessionKernel } from "../src/internal/session.ts";
import type { CommitOptions } from "../src/Session.ts";
import * as Task from "../src/Task.ts";
import { Tx } from "../src/Tx.ts";
import { settled } from "./HarnessSupport.ts";
import { describeEachBackend, flush, openSession, run } from "./SessionSupport.ts";

/** A latest, initial-fork conversation document: one key per conversation. */
const Note = Doc.make({
  checkpointWhen: () => false,
  kind: "gc.note",
  version: 1,
  scope: "conversation",
  history: "latest",
  fork: "initial",
  schema: Schema.Struct({ n: Schema.Number }),
  initial: () => ({ n: 0 }),
});

const Shared = Doc.make({
  checkpointWhen: () => false,
  kind: "gc.shared",
  version: 1,
  scope: "session",
  schema: Schema.Struct({ n: Schema.Number }),
  initial: () => ({ n: 0 }),
});

/** A task whose documents a commit that ends it retires. */
const WorkTask = Task.make({
  name: "gc.work",
  version: 1,
  input: Schema.Null,
  checkpoint: Schema.Struct({ phase: Schema.Literal("start") }),
  result: Schema.Null,
  initial: () => ({ phase: "start" }),
  phases: { start: () => Effect.void },
  abort: () => Effect.void,
});

const Progress = Doc.make({
  checkpointWhen: () => false,
  kind: "gc.progress",
  version: 1,
  scope: "task",
  schema: Schema.Struct({ n: Schema.Number }),
  initial: () => ({ n: 0 }),
});

const backendFailure = (message: string) =>
  new StorageFailure({ reason: new Backend({ message, cause: message }) });

/** The controls of `batchStorage`. */
interface BatchControls {
  readonly calls: Array<ReadonlyArray<ReadonlyArray<StorageWrite>>>;
  /** Refuse the next batch this matches, with `refusal`. */
  refuse: ((writes: ReadonlyArray<StorageWrite>) => boolean) | undefined;
  refusal: StorageFailure;
  /** Fail the call with this index after its batches are stored, with `rejection`. */
  rejectCall: number | undefined;
  rejection: StorageFailure;
  /** Delay each call by up to this many scheduler turns. */
  latency: (() => number) | undefined;
  /** Fail the next `task` read with this error. */
  failTaskRead: StorageFailure | undefined;
  /** Holds the next call in flight until released. */
  readonly hold: Effect.Effect<{
    readonly entered: Effect.Effect<void>;
    readonly release: Effect.Effect<void>;
  }>;
}

/**
 * The storage in context, whose `commit` calls are recorded and can be held in flight, and whose batches can be
 * refused (failed before adoption) or whose calls can fail after their receipts.
 */
const batchStorage = Effect.gen(function* () {
  const storage = yield* AgentStorage;
  const calls: Array<ReadonlyArray<ReadonlyArray<StorageWrite>>> = [];
  let held:
    | { readonly entered: Deferred.Deferred<void>; readonly gate: Deferred.Deferred<void> }
    | undefined;
  const controls: BatchControls = {
    calls,
    /** Refuse the next batch this matches, with `refusal`. */
    refuse: undefined,
    refusal: backendFailure("refused"),
    /** Fail the call with this index after its batches are stored, with `rejection`. */
    rejectCall: undefined,
    rejection: backendFailure("connection lost"),
    /** Delay each call by up to this many scheduler turns. */
    latency: undefined,
    /** Fail the next `task` read with this error. */
    failTaskRead: undefined,
    /** Holds the next call in flight until released. */
    hold: Effect.sync(() => {
      const next = { entered: Deferred.makeUnsafe<void>(), gate: Deferred.makeUnsafe<void>() };
      held = next;
      return {
        entered: Deferred.await(next.entered),
        release: Deferred.succeed(next.gate, undefined).pipe(Effect.asVoid),
      };
    }),
  };
  const wrapped: AgentStorage["Service"] = {
    ...storage,
    task: (id) =>
      Effect.suspend(() => {
        const failure = controls.failTaskRead;
        if (failure === undefined) return storage.task(id);
        controls.failTaskRead = undefined;
        return Effect.fail(failure);
      }),
    commit: (batches, receive) =>
      Effect.gen(function* () {
        const call = calls.length;
        calls.push(batches);
        const current = held;
        held = undefined;
        if (current !== undefined) {
          yield* Deferred.succeed(current.entered, undefined);
          yield* Deferred.await(current.gate);
        }
        const delay = controls.latency?.() ?? 0;
        for (let turn = 0; turn < delay; turn++) yield* Effect.yieldNow;
        // The batches before the refused one are stored; it and the ones after are not.
        const at = batches.findIndex((writes) => {
          if (controls.refuse?.(writes) !== true) return false;
          controls.refuse = undefined;
          return true;
        });
        let result: CommitResult;
        if (at < 0) result = yield* storage.commit(batches, receive);
        else {
          const stored = yield* storage.commit(batches.slice(0, at), receive);
          result = stored.stored < at ? stored : { stored: at, refused: controls.refusal };
        }
        if (controls.rejectCall === call) return yield* controls.rejection;
        return result;
      }),
  };
  return { storage: wrapped, controls };
});

/** A Session over the batch storage with `conversations` conversations, and its own observers' publications. */
const open = (conversations: number) =>
  Effect.gen(function* () {
    const { storage, controls } = yield* batchStorage;
    const session = yield* openSession(storage, { documentRetention: () => Duration.seconds(600) });
    const ids = yield* session.commit(
      Effect.gen(function* () {
        const tx = yield* Tx;
        const created: Array<ConversationId> = [];
        for (let n = 0; n < conversations; n++) {
          created.push((yield* tx.createConversation({ ownership: { kind: "ownerless" } })).id);
        }
        return created;
      }),
    );
    const published: Array<ObservedCommit> = [];
    yield* session.followCommits((publication) => {
      published.push(publication);
    });
    return { storage, controls, session, ids, published };
  });

/** Increments conversation `id`'s note; returns the value it read. */
const bump = (session: SessionKernel, id: ConversationId, options?: CommitOptions) =>
  session.commit(
    Effect.gen(function* () {
      const tx = yield* Tx;
      const note = yield* tx.doc(Note, id);
      return note.n++;
    }),
    options,
  );

const DEFERRED: CommitOptions = { persistence: { kind: "deferred" } };

const append = (session: SessionKernel, id: ConversationId, kind: string) =>
  session.commit(
    Effect.gen(function* () {
      const tx = yield* Tx;
      return yield* tx.appendEntry(id, { kind });
    }),
  );

/** Any of the tagged errors a Session operation fails with. */
type Tagged = { readonly _tag: string };

/** One read of a conversation's entries through the Session. */
type Reader = (session: SessionKernel, id: ConversationId) => Effect.Effect<unknown, Tagged>;

/** Runs `f` with the transaction of the running commit callback. */
const withTx = <A, E>(f: (tx: Tx["Service"]) => Effect.Effect<A, E, Tx>) =>
  Effect.gen(function* () {
    const tx = yield* Tx;
    return yield* f(tx);
  });

/** Starts `effect` at once on a fiber of its own. */
const start = <A, E>(effect: Effect.Effect<A, E>) =>
  Effect.forkChild(effect, { startImmediately: true });

/** Records the order in which `fibers` settle, by name; a failure as `name!`. */
const settleOrder = (fibers: Record<string, Fiber.Fiber<unknown, unknown>>) =>
  Effect.gen(function* () {
    const order: Array<string> = [];
    for (const [name, fiber] of Object.entries(fibers)) {
      yield* Effect.forkChild(
        Effect.flatMap(Fiber.await(fiber), (exit) =>
          Effect.sync(() => order.push(Exit.isSuccess(exit) ? name : `${name}!`)),
        ),
        { startImmediately: true },
      );
    }
    return order;
  });

/** Lets forked fibers run until `check` holds, for a bounded number of turns. */
const eventually = (check: () => boolean) =>
  Effect.gen(function* () {
    for (let attempt = 0; attempt < 400 && !check(); attempt++) yield* Effect.yieldNow;
    assert.isTrue(check(), "condition not reached");
  });

/** The note values a reopened Session over `storage` reads, per conversation. */
const notes = (storage: AgentStorage["Service"], ids: ReadonlyArray<ConversationId>) =>
  Effect.gen(function* () {
    const reopened = yield* openSession(storage);
    const values: Array<number> = [];
    for (const id of ids) {
      values.push(Option.getOrUndefined(yield* reopened.snapshot(Note, id))?.n ?? 0);
    }
    yield* reopened.close;
    return values;
  });

/** The kinds of the entries `published` holds, in order. */
const entryKinds = (published: ReadonlyArray<ObservedCommit>) =>
  published.flatMap((publication) =>
    publication.changes.flatMap((change) => (change.type === "entry" ? [change.value.kind] : [])),
  );

/** A fiber's failure, or `undefined` when it succeeded. */
const failureOf = <A, E>(fiber: Fiber.Fiber<A, E>) =>
  Effect.map(Fiber.await(fiber), (exit) =>
    Exit.isFailure(exit) ? Option.getOrUndefined(Exit.findErrorOption(exit)) : undefined,
  );

const closedReason = (session: SessionKernel) => Effect.map(session.closed, (end) => end);

describeEachBackend("group commit: batching", (backend) => {
  it.effect(
    "stores the commits admitted while a call is in flight in one call, each with its seq, published in order",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const { controls, session, ids, published } = yield* open(3);
          // SAFETY: `open(n)` creates `n` conversations.
          const [a, b, c] = ids as [ConversationId, ConversationId, ConversationId];
          const held = yield* controls.hold;
          const first = yield* start(bump(session, a));
          yield* held.entered;
          const ran: Array<string> = [];
          const note = (id: ConversationId, name: string) =>
            session.commit(
              Effect.gen(function* () {
                const tx = yield* Tx;
                (yield* tx.doc(Note, id)).n++;
                ran.push(name);
              }),
            );
          const second = yield* start(note(b, "b"));
          const third = yield* start(note(c, "c"));
          // Their callbacks run while the first commit is in flight: they touch nothing it writes.
          yield* eventually(() => ran.length === 2);
          assert.strictEqual(published.length, 0);
          const order = yield* settleOrder({ a: first, b: second, c: third });
          yield* held.release;
          yield* Fiber.join(first);
          yield* Fiber.join(second);
          yield* Fiber.join(third);
          yield* flush;
          assert.deepStrictEqual(order, ["a", "b", "c"]);
          assert.deepStrictEqual(
            controls.calls.slice(-2).map((call) => call.length),
            [1, 2],
          );
          const seqs = published.flatMap((publication) =>
            publication.kind === "stored" ? [publication.seq] : [],
          );
          assert.deepStrictEqual(
            seqs,
            seqs.toSorted((x, y) => x - y),
          );
          assert.strictEqual(new Set(seqs).size, 3);
          yield* session.close;
        }),
      ),
  );
});

describeEachBackend("group commit: gates", (backend) => {
  it.effect(
    "makes a read of an entry ID leaked by an unpublished commit wait for its publication",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const { controls, session, ids } = yield* open(1);
          const held = yield* controls.hold;
          let leaked: EntryId | undefined;
          const appending = yield* start(
            session.commit(
              Effect.gen(function* () {
                const tx = yield* Tx;
                leaked = (yield* tx.appendEntry(ids[0]!, { kind: "note" })).id;
              }),
            ),
          );
          yield* held.entered;
          const reading = yield* start(
            session.commit(
              Effect.gen(function* () {
                const tx = yield* Tx;
                return yield* tx.entry(leaked!);
              }),
            ),
          );
          assert.isFalse(yield* settled(reading));
          yield* held.release;
          yield* Fiber.join(appending);
          assert.strictEqual(Option.getOrUndefined(yield* Fiber.join(reading))?.id, leaked);
          yield* session.close;
        }),
      ),
  );

  it.effect(
    "makes a job wait for an unpublished creation at a document address instead of creating it again",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const { controls, session } = yield* open(0);
          const held = yield* controls.hold;
          const first = yield* start(
            session.commit(
              Effect.gen(function* () {
                const tx = yield* Tx;
                (yield* tx.doc(Shared)).n++;
              }),
            ),
          );
          yield* held.entered;
          const second = yield* start(
            session.commit(
              Effect.gen(function* () {
                const tx = yield* Tx;
                const shared = yield* tx.doc(Shared);
                shared.n++;
                return shared.n;
              }),
            ),
          );
          assert.isFalse(yield* settled(second));
          yield* held.release;
          yield* Fiber.join(first);
          assert.strictEqual(yield* Fiber.join(second), 2);
          const creations = controls.calls
            .flat(2)
            .filter(
              (write) => write.type === "document.create" && write.record.kind === "gc.shared",
            );
          assert.lengthOf(creations, 1);
          yield* session.close;
        }),
      ),
  );

  it.effect("makes a write to a conversation an unpublished commit creates wait for it", () =>
    run(backend)(
      Effect.gen(function* () {
        const { controls, session } = yield* open(0);
        const held = yield* controls.hold;
        let leaked: ConversationId | undefined;
        const creating = yield* start(
          session.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              leaked = (yield* tx.createConversation({ ownership: { kind: "ownerless" } })).id;
            }),
          ),
        );
        yield* held.entered;
        const appending = yield* start(
          session.commit(
            Effect.suspend(() => withTx((tx) => tx.appendEntry(leaked!, { kind: "note" }))),
          ),
        );
        assert.isFalse(yield* settled(appending));
        yield* held.release;
        yield* Fiber.join(creating);
        assert.strictEqual((yield* Fiber.join(appending)).conversationId, leaked);
        yield* session.close;
      }),
    ),
  );

  // The task's document-scope key is touched before the Session's index of its documents is read.
  it.effect(
    "makes a commit that ends a task wait for a document of it an unpublished commit creates",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const { controls, session, ids } = yield* open(1);
          const taskId = yield* session.commit(
            withTx((tx) =>
              tx.createTask(WorkTask, null, {
                ownership: { kind: "conversation" },
                conversationId: ids[0]!,
              }),
            ),
          );
          const held = yield* controls.hold;
          const creating = yield* start(
            session.commit(
              Effect.gen(function* () {
                const tx = yield* Tx;
                (yield* tx.doc(Progress, taskId)).n = 1;
              }),
            ),
          );
          yield* held.entered;
          const ending = yield* start(
            session.commitWith((tx) =>
              Effect.gen(function* () {
                const task = Option.getOrThrow(yield* tx.service.task(taskId));
                yield* tx.setTask({
                  ...task,
                  state: { status: "terminal", outcome: { status: "completed", result: null } },
                });
              }),
            ),
          );
          yield* flush;
          yield* held.release;
          yield* Fiber.join(creating);
          yield* Fiber.join(ending);
          const current = yield* session.readOnLine(
            session.storage.scanDocuments({ scope: { kind: "task", taskId }, at: "current" }, 10),
          );
          assert.deepStrictEqual(current.items, []);
          yield* session.close;
        }),
      ),
  );

  // `Tx` has no `latestEntryId`; the Session's own helper is covered through
  // `latestHeadMarker`, the transaction's scan, and the line's reads.
  it.effect(
    "makes each read of the entries of a conversation an unpublished commit creates wait for its creation",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const reads = {
            latestHeadMarker: (session, id) =>
              session.commit(
                withTx((tx) => Effect.map(tx.latestHeadMarker(id), Option.getOrUndefined)),
              ),
            "tx.scanEntries": (session, id) =>
              session.commit(
                withTx((tx) =>
                  Effect.map(tx.scanEntries({ conversationId: id }, 1), (page) => page.items),
                ),
              ),
            "line scanEntries": (session, id) =>
              session.readOnLine(
                Effect.map(
                  session.storage.scanEntries({ conversationId: id }, 1),
                  (page) => page.items,
                ),
              ),
            "line findLatestHeadMarker": (session, id) =>
              session.readOnLine(
                Effect.map(session.storage.findLatestHeadMarker(id), Option.getOrUndefined),
              ),
          } satisfies Record<string, Reader>;
          for (const [name, read] of Object.entries(reads)) {
            const { controls, session } = yield* open(0);
            const held = yield* controls.hold;
            let leaked: ConversationId | undefined;
            const creating = yield* start(
              session.commit(
                Effect.gen(function* () {
                  const tx = yield* Tx;
                  leaked = (yield* tx.createConversation({ ownership: { kind: "ownerless" } })).id;
                }),
              ),
            );
            yield* held.entered;
            const reader: Reader = read;
            const reading = yield* start(Effect.suspend(() => reader(session, leaked!)));
            yield* flush;
            yield* held.release;
            yield* Fiber.join(creating);
            const exit = yield* Fiber.await(reading);
            assert.isTrue(Exit.isSuccess(exit), name);
            if (Exit.isSuccess(exit)) {
              assert.deepStrictEqual(exit.value, Array.isArray(exit.value) ? [] : undefined, name);
            }
            yield* session.close;
          }
        }),
      ),
  );

  it.effect("makes a scan that cannot be keyed wait for every unpublished commit", () =>
    run(backend)(
      Effect.gen(function* () {
        const { controls, session } = yield* open(0);
        const held = yield* controls.hold;
        const creating = yield* start(
          session.commit(
            withTx((tx) =>
              Effect.map(tx.createConversation({ ownership: { kind: "ownerless" } }), (c) => c.id),
            ),
          ),
        );
        yield* held.entered;
        const scanning = yield* start(
          session.commit(
            withTx((tx) =>
              Effect.map(tx.scanConversations({}, 10), (page) => page.items.map((c) => c.id)),
            ),
          ),
        );
        assert.isFalse(yield* settled(scanning));
        yield* held.release;
        const created = yield* Fiber.join(creating);
        assert.include(yield* Fiber.join(scanning), created);
        yield* session.close;
      }),
    ),
  );

  it.effect("makes a line read after an un-awaited commit see it", () =>
    run(backend)(
      Effect.gen(function* () {
        const { controls, session, ids } = yield* open(1);
        const held = yield* controls.hold;
        let leaked: EntryId | undefined;
        yield* start(
          session.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              leaked = (yield* tx.appendEntry(ids[0]!, { kind: "note" })).id;
            }),
          ),
        );
        yield* held.entered;
        const reading = yield* start(
          session.readOnLine(Effect.suspend(() => session.storage.entry(leaked!))),
        );
        yield* flush;
        yield* held.release;
        assert.strictEqual(Option.getOrUndefined(yield* Fiber.join(reading))?.entry.id, leaked);
        yield* session.close;
      }),
    ),
  );

  it.effect(
    "makes a reader of a document whose held changes an unpublished commit stores wait, and store them once",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const { storage, controls, session, ids } = yield* open(1);
          const id = ids[0]!;
          yield* bump(session, id);
          yield* bump(session, id, DEFERRED);
          // The first commit only reads the held document: storing its held changes is its write of it.
          const held = yield* controls.hold;
          const storing = yield* start(
            session.commit(
              withTx((tx) =>
                Effect.gen(function* () {
                  yield* tx.doc(Note, id);
                  yield* tx.appendEntry(id, { kind: "a" });
                }),
              ),
            ),
          );
          yield* held.entered;
          let read = false;
          const reading = yield* start(
            session.commit(
              withTx((tx) =>
                Effect.gen(function* () {
                  yield* tx.doc(Note, id);
                  read = true;
                  yield* tx.appendEntry(id, { kind: "b" });
                }),
              ),
            ),
          );
          yield* flush;
          assert.isFalse(read);
          yield* held.release;
          yield* Fiber.join(storing);
          yield* Fiber.join(reading);
          const changes = controls.calls
            .slice(-2)
            .map((call) => call.flat().filter((write) => write.type === "document.change"));
          assert.deepStrictEqual(
            changes.map((writes) => writes.length),
            [1, 0],
          );
          const record = Option.getOrThrow(
            yield* storage.findDocument(
              { kind: Note.definition.kind, scope: { kind: "conversation", conversationId: id } },
              "current",
            ),
          );
          const stored = Option.getOrThrow(yield* storage.document(record.id, "current"));
          assert.strictEqual(stored.deltasSinceBase, 1);
          yield* session.close;
        }),
      ),
  );

  it.effect(
    "makes a reader of a document held by a deferred commit behind a call wait for it, and store it",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const { storage, controls, session, ids, published } = yield* open(2);
          // SAFETY: `open(n)` creates `n` conversations.
          const [a, b] = ids as [ConversationId, ConversationId];
          yield* bump(session, a);
          const held = yield* controls.hold;
          const first = yield* start(bump(session, b));
          yield* held.entered;
          const deferredCommit = yield* start(
            session.commit(
              withTx((tx) =>
                Effect.map(tx.doc(Note, a), (note) => {
                  note.n = 5;
                }),
              ),
              DEFERRED,
            ),
          );
          const reader = yield* start(bump(session, a));
          yield* flush;
          // Nothing is published while the first commit is in flight, the deferred one neither.
          assert.lengthOf(published, 1);
          yield* held.release;
          yield* Fiber.join(first);
          yield* Fiber.join(deferredCommit);
          assert.strictEqual(yield* Fiber.join(reader), 5);
          // The reader's own batch stored what the deferred commit held.
          const stored = controls.calls.at(-1)!.flat();
          assert.lengthOf(
            stored.filter((write) => write.type === "document.change"),
            1,
          );
          yield* session.close;
          assert.deepStrictEqual(yield* notes(storage, [a, b]), [6, 1]);
        }),
      ),
  );
});

describeEachBackend("group commit: outcomes", (backend) => {
  it.effect(
    "delivers a read, a commit without writes, a callback error, and a deferred commit after an earlier commit",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const { controls, session, ids } = yield* open(2);
          const held = yield* controls.hold;
          const first = yield* start(bump(session, ids[0]!));
          yield* held.entered;
          const order = yield* settleOrder({
            first,
            read: yield* start(session.readOnLine(Effect.succeed("read"))),
            none: yield* start(session.commit(Effect.succeed("none"))),
            error: yield* start(session.commit(Effect.fail(backendFailure("own")))),
            deferred: yield* start(bump(session, ids[1]!, DEFERRED)),
          });
          yield* flush;
          assert.deepStrictEqual(order, []);
          yield* held.release;
          yield* eventually(() => order.length === 5);
          // A callback's own error affects only its job.
          assert.deepStrictEqual(order, ["first", "read", "none", "error!", "deferred"]);
          yield* session.close;
        }),
      ),
  );

  it.effect(
    "makes every outcome after a fatal failure SessionFailed, and keeps the earlier ones",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const { storage, controls, session, ids } = yield* open(4);
          // SAFETY: `open(n)` creates `n` conversations.
          const [a, b, c, d] = ids as [
            ConversationId,
            ConversationId,
            ConversationId,
            ConversationId,
          ];
          const held = yield* controls.hold;
          const first = yield* start(bump(session, a));
          yield* held.entered;
          const stored = yield* start(bump(session, b));
          const refused = yield* start(append(session, c, "refuse"));
          const after = yield* start(bump(session, d));
          const read = yield* start(session.readOnLine(Effect.succeed("read")));
          controls.refuse = (writes) =>
            writes.some((write) => write.type === "entry" && write.value.kind === "refuse");
          // Every callback has run, so the three are admitted while the first call is in flight.
          yield* flush;
          yield* held.release;
          assert.strictEqual(yield* Fiber.join(first), 0);
          assert.strictEqual(yield* Fiber.join(stored), 0);
          assert.strictEqual(yield* failureOf(refused), controls.refusal);
          assert.strictEqual((yield* failureOf(after))?._tag, "SessionFailed");
          assert.strictEqual((yield* failureOf(read))?._tag, "SessionFailed");
          assert.deepStrictEqual(yield* closedReason(session), {
            reason: "failed",
            error: controls.refusal,
          });
          // One call stored the first two of the three batches it got; the refused one and the one after are not.
          assert.lengthOf(controls.calls.at(-1)!, 3);
          assert.deepStrictEqual(yield* notes(storage, [a, b, d]), [1, 1, 0]);
        }),
      ),
  );
});

describeEachBackend("group commit: failures per part", (backend) => {
  it.effect(
    "publishes the parts received before a call rejects, then rejects them with its error",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const { storage, controls, session, ids, published } = yield* open(3);
          // SAFETY: `open(n)` creates `n` conversations.
          const [a, b, c] = ids as [ConversationId, ConversationId, ConversationId];
          const held = yield* controls.hold;
          const first = yield* start(bump(session, a));
          yield* held.entered;
          const second = yield* start(bump(session, b));
          const third = yield* start(bump(session, c));
          controls.rejectCall = controls.calls.length;
          yield* flush;
          yield* held.release;
          yield* Fiber.join(first);
          assert.strictEqual(yield* failureOf(second), controls.rejection);
          assert.strictEqual(yield* failureOf(third), controls.rejection);
          // Durable, so published.
          assert.lengthOf(published, 3);
          assert.deepStrictEqual(yield* closedReason(session), {
            reason: "failed",
            error: controls.rejection,
          });
          assert.deepStrictEqual(yield* notes(storage, [a, b, c]), [1, 1, 1]);
        }),
      ),
  );

  it.effect(
    "fails every job after the first part of a call that rejects, except its other parts, with their storage error",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const { controls, session, ids, published } = yield* open(1);
          const id = ids[0]!;
          const held = yield* controls.hold;
          const first = yield* start(append(session, id, "a"));
          yield* held.entered;
          // Between the two parts of the failing call, a job without writes.
          const before = yield* start(append(session, id, "b"));
          const between = yield* start(session.commit(Effect.succeed("no write")));
          const after = yield* start(append(session, id, "d"));
          yield* flush;
          controls.rejectCall = controls.calls.length;
          yield* held.release;
          assert.isTrue(Exit.isSuccess(yield* Fiber.await(first)));
          assert.strictEqual(yield* failureOf(before), controls.rejection);
          assert.strictEqual((yield* failureOf(between))?._tag, "SessionFailed");
          assert.strictEqual(yield* failureOf(after), controls.rejection);
          // Both parts were received, so they were published before they failed.
          assert.lengthOf(controls.calls.at(-1)!, 2);
          assert.lengthOf(published, 3);
          assert.deepStrictEqual(yield* closedReason(session), {
            reason: "failed",
            error: controls.rejection,
          });
        }),
      ),
  );

  it.effect(
    "reports SessionFailed with the first error for a call that rejects after another failure failed the Session",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const { controls, session, ids, published } = yield* open(1);
          const id = ids[0]!;
          const held = yield* controls.hold;
          const inFlight = yield* start(append(session, id, "a"));
          yield* held.entered;
          // A later job's read fails first, and fails the Session; then the call in flight fails with another.
          const first = backendFailure("first failure: read");
          controls.failTaskRead = first;
          const reading = yield* start(session.commit(withTx((tx) => tx.task(TaskId.make(999)))));
          yield* flush;
          assert.isTrue(Option.isSome(session.failure));
          controls.rejectCall = controls.calls.length - 1;
          yield* held.release;
          assert.strictEqual(yield* failureOf(reading), first);
          const outcome = yield* failureOf(inFlight);
          assert.strictEqual(outcome?._tag, "SessionFailed");
          assert.strictEqual(outcome?._tag === "SessionFailed" ? outcome.cause : undefined, first);
          // Nothing of the call is published once the Session failed.
          assert.deepStrictEqual(published, []);
          assert.deepStrictEqual(yield* closedReason(session), { reason: "failed", error: first });
        }),
      ),
  );

  it.effect("rejects every part of a call with its rejection", () =>
    run(backend)(
      Effect.gen(function* () {
        const { controls, session, ids } = yield* open(3);
        // SAFETY: `open(n)` creates `n` conversations.
        const [a, b, c] = ids as [ConversationId, ConversationId, ConversationId];
        const held = yield* controls.hold;
        const first = yield* start(bump(session, a));
        yield* held.entered;
        const parts = [yield* start(bump(session, b)), yield* start(bump(session, c))];
        yield* flush;
        controls.rejection = backendFailure("rejected value");
        controls.rejectCall = controls.calls.length;
        yield* held.release;
        yield* Fiber.join(first);
        for (const part of parts) assert.strictEqual(yield* failureOf(part), controls.rejection);
        assert.deepStrictEqual(yield* closedReason(session), {
          reason: "failed",
          error: controls.rejection,
        });
      }),
    ),
  );

  it.effect(
    "rejects a refused part with its refusal, and the parts after it with SessionFailed",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const { controls, session, ids } = yield* open(3);
          // SAFETY: `open(n)` creates `n` conversations.
          const [a, b, c] = ids as [ConversationId, ConversationId, ConversationId];
          const held = yield* controls.hold;
          const first = yield* start(bump(session, a));
          yield* held.entered;
          const refused = yield* start(append(session, b, "refuse"));
          const after = yield* start(bump(session, c));
          yield* flush;
          controls.refusal = backendFailure("refused value");
          controls.refuse = (writes) =>
            writes.some((write) => write.type === "entry" && write.value.kind === "refuse");
          yield* held.release;
          yield* Fiber.join(first);
          assert.strictEqual(yield* failureOf(refused), controls.refusal);
          assert.strictEqual((yield* failureOf(after))?._tag, "SessionFailed");
          assert.deepStrictEqual(yield* closedReason(session), {
            reason: "failed",
            error: controls.refusal,
          });
        }),
      ),
  );

  it.skip("leaves the parts after one whose adoption throws unpublished, rejected with SessionFailed (spies on adopt)", () => {});
});

describeEachBackend("group commit: a publication that throws", (backend) => {
  // Publication fails the Session before it checks whether a job was poisoned, so a later job whose outcome was
  // known while the commit was stored is delivered as `SessionFailed`.
  it.effect("fails a read that completed behind a commit whose own observer throws", () =>
    run(backend)(
      Effect.gen(function* () {
        const { controls, session, ids } = yield* open(1);
        let throwing = false;
        yield* session.followCommits(() => {
          if (throwing) throw new Error("observer broke");
        });
        const held = yield* controls.hold;
        throwing = true;
        const writing = yield* start(bump(session, ids[0]!));
        yield* held.entered;
        const reading = yield* start(session.readOnLine(Effect.succeed("read")));
        yield* flush;
        yield* held.release;
        const written = yield* Fiber.await(writing);
        const read = yield* Fiber.await(reading);
        assert.isTrue(Exit.isSuccess(written));
        assert.isTrue(Exit.isFailure(read));
        if (Exit.isFailure(read)) assert.include(String(Cause.squash(read.cause)), "SessionFailed");
        assert.isTrue(Option.isSome(session.failure));
      }),
    ),
  );

  it.skip("fails the Session for a deferred commit memory cannot adopt, and still closes (spies on adoptHeld)", () => {});
  it.skip("settles a deferred commit whose adoption throws, alone, and closes (spies on adoptHeld)", () => {});
  it.skip("fails the Session for a throw in the read caches' write-through, and leaves the later parts unpublished (spies on the cache)", () => {});
});

describeEachBackend("group commit: deferred commits", (backend) => {
  it.effect("publishes a deferred commit only once the commit before it is published", () =>
    run(backend)(
      Effect.gen(function* () {
        const { controls, session, ids, published } = yield* open(2);
        // SAFETY: `open(n)` creates `n` conversations.
        const [a, b] = ids as [ConversationId, ConversationId];
        // Only a change of an existing document is held.
        yield* bump(session, b);
        published.length = 0;
        const held = yield* controls.hold;
        const first = yield* start(bump(session, a));
        yield* held.entered;
        const later = yield* start(bump(session, b, DEFERRED));
        yield* flush;
        assert.lengthOf(published, 0);
        yield* held.release;
        yield* Fiber.join(first);
        yield* Fiber.join(later);
        assert.deepStrictEqual(
          published.map((publication) => publication.kind),
          ["stored", "memory"],
        );
        yield* session.close;
      }),
    ),
  );

  it.effect("never publishes a deferred commit behind a commit that fails", () =>
    run(backend)(
      Effect.gen(function* () {
        const { controls, session, ids, published } = yield* open(2);
        // SAFETY: `open(n)` creates `n` conversations.
        const [a, b] = ids as [ConversationId, ConversationId];
        yield* bump(session, b);
        published.length = 0;
        const held = yield* controls.hold;
        const first = yield* start(append(session, a, "refuse"));
        yield* held.entered;
        const later = yield* start(bump(session, b, DEFERRED));
        controls.refuse = (writes) =>
          writes.some((write) => write.type === "entry" && write.value.kind === "refuse");
        yield* held.release;
        assert.strictEqual(yield* failureOf(first), controls.refusal);
        assert.strictEqual((yield* failureOf(later))?._tag, "SessionFailed");
        assert.deepStrictEqual(published, []);
      }),
    ),
  );
});

describeEachBackend("group commit: publication", (backend) => {
  // Two parts are a low-priority yield apart.
  it.effect(
    "publishes the parts of one call a macrotask apart: what one resolves runs before the next is published",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const { controls, session, ids, published } = yield* open(3);
          // SAFETY: `open(n)` creates `n` conversations.
          const [a, b, c] = ids as [ConversationId, ConversationId, ConversationId];
          const held = yield* controls.hold;
          const first = yield* start(bump(session, a));
          yield* held.entered;
          let seenAtSecond: number | undefined;
          const second = yield* start(
            bump(session, b).pipe(
              Effect.tap(() =>
                Effect.sync(() => {
                  seenAtSecond = published.length;
                }),
              ),
            ),
          );
          const third = yield* start(bump(session, c));
          yield* flush;
          yield* held.release;
          yield* Fiber.join(first);
          yield* Fiber.join(second);
          yield* Fiber.join(third);
          assert.lengthOf(controls.calls.at(-1)!, 2);
          assert.strictEqual(seenAtSecond, 2);
          assert.lengthOf(published, 3);
          yield* session.close;
        }),
      ),
  );
});

describeEachBackend("group commit: the storage's publication scope", () => {
  it.skip("publishes each commit inside the storage's scope, the next only once one it runs later ran (StoreStorage)", () => {});
  it.skip("reports a scope that throws instead of running the publication, and publishes the commit (StoreStorage)", () => {});
});

describeEachBackend("group commit: a failure between two publications", (backend) => {
  it.effect(
    "publishes no part of a call after a failure that a continuation of an earlier part caused",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const { controls, session, ids, published } = yield* open(1);
          const id = ids[0]!;
          const held = yield* controls.hold;
          const lead = yield* start(append(session, id, "lead"));
          yield* held.entered;
          const read = backendFailure("read failed between publications");
          // What `one` resolves runs in the pause before `two` is published: a read there fails the Session.
          const one = yield* start(
            append(session, id, "one").pipe(
              Effect.tap(() =>
                Effect.sync(() => {
                  controls.failTaskRead = read;
                }),
              ),
              Effect.andThen(session.commit(withTx((tx) => tx.task(TaskId.make(999))))),
            ),
          );
          const two = yield* start(append(session, id, "two"));
          yield* flush;
          yield* held.release;
          yield* Fiber.await(lead);
          yield* Fiber.await(one);
          const outcome = yield* failureOf(two);
          assert.deepStrictEqual(entryKinds(published), ["lead", "one"]);
          assert.strictEqual(outcome?._tag, "SessionFailed");
          assert.strictEqual(outcome?._tag === "SessionFailed" ? outcome.cause : undefined, read);
        }),
      ),
  );

  it.effect(
    "publishes no part of a call after the Session fails in a continuation of an earlier part",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const { controls, session, ids, published } = yield* open(1);
          const id = ids[0]!;
          const held = yield* controls.hold;
          const lead = yield* start(append(session, id, "lead"));
          yield* held.entered;
          const failure = new Error("failed between publications");
          const one = yield* start(
            append(session, id, "one").pipe(
              Effect.tap(() => Effect.sync(() => session.fail(failure))),
            ),
          );
          const two = yield* start(append(session, id, "two"));
          yield* flush;
          yield* held.release;
          yield* Fiber.await(lead);
          yield* Fiber.await(one);
          const outcome = yield* failureOf(two);
          assert.deepStrictEqual(entryKinds(published), ["lead", "one"]);
          assert.strictEqual(outcome?._tag, "SessionFailed");
        }),
      ),
  );

  it.skip("checks the failure when a storage's scope runs a publication it deferred, not when it was asked to (StoreStorage)", () => {});
});

const NoteV2 = Doc.make({
  checkpointWhen: () => false,
  kind: "gc.note",
  version: 2,
  scope: "conversation",
  history: "latest",
  fork: "initial",
  schema: Schema.Struct({ n: Schema.Number, migrated: Schema.Literal(true) }),
  initial: () => ({ n: 0, migrated: true as const }),
  // SAFETY: `migrate()` gets version 1's value.
  migrate: (value) => ({ ...(value as { readonly n: number }), migrated: true as const }),
});

describeEachBackend("group commit: upkeep through the writer", (backend) => {
  it.effect("stores held changes for an unload after the call in flight", () =>
    run(backend)(
      Effect.gen(function* () {
        const { storage, controls, session, ids } = yield* open(2);
        // SAFETY: `open(n)` creates `n` conversations.
        const [a, b] = ids as [ConversationId, ConversationId];
        yield* bump(session, a);
        yield* bump(session, b, DEFERRED);
        const held = yield* controls.hold;
        const first = yield* start(bump(session, a));
        yield* held.entered;
        const unloading = yield* start(session.unloadDocuments);
        assert.isFalse(yield* settled(unloading));
        yield* held.release;
        yield* Fiber.join(first);
        yield* Fiber.join(unloading);
        const last = controls.calls.at(-1)!.flat();
        assert.deepStrictEqual(
          last.map((write) => write.type),
          ["document.change"],
        );
        assert.deepStrictEqual(yield* session.snapshot(Note, b), Option.some({ n: 1 }));
        yield* session.close;
        assert.deepStrictEqual(yield* notes(storage, [a, b]), [2, 1]);
      }),
    ),
  );

  it.effect(
    "stores held changes for a version reload inside a callback after the call in flight",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const { controls, session, ids } = yield* open(2);
          // SAFETY: `open(n)` creates `n` conversations.
          const [a, b] = ids as [ConversationId, ConversationId];
          yield* bump(session, a);
          yield* bump(session, b);
          yield* bump(session, b, DEFERRED);
          const held = yield* controls.hold;
          const first = yield* start(bump(session, a));
          yield* held.entered;
          const reloading = yield* start(
            session.commit(withTx((tx) => Effect.map(tx.doc(NoteV2, b), (note) => note.n))),
          );
          assert.isFalse(yield* settled(reloading));
          yield* held.release;
          yield* Fiber.join(first);
          // The reload found what memory held, stored first.
          assert.strictEqual(yield* Fiber.join(reloading), 2);
          yield* session.close;
        }),
      ),
  );

  it.effect("stores held changes at their deadline after the call in flight", () =>
    run(backend)(
      Effect.gen(function* () {
        const { controls, session, ids } = yield* open(2);
        // SAFETY: `open(n)` creates `n` conversations.
        const [a, b] = ids as [ConversationId, ConversationId];
        yield* bump(session, a);
        const held = yield* controls.hold;
        const first = yield* start(bump(session, a));
        yield* held.entered;
        const later = yield* start(
          bump(session, b, { persistence: { kind: "deferred", withinMs: 0 } }),
        );
        yield* flush;
        const calls = controls.calls.length;
        yield* held.release;
        yield* Fiber.join(first);
        yield* Fiber.join(later);
        yield* eventually(() => controls.calls.length === calls + 1);
        assert.deepStrictEqual(
          controls.calls
            .at(-1)!
            .flat()
            .map((write) => write.type),
          ["document.create"],
        );
        yield* session.close;
      }),
    ),
  );

  it.effect("drains the calls in flight and stores held changes when it closes", () =>
    run(backend)(
      Effect.gen(function* () {
        const { storage, controls, session, ids } = yield* open(2);
        // SAFETY: `open(n)` creates `n` conversations.
        const [a, b] = ids as [ConversationId, ConversationId];
        const held = yield* controls.hold;
        const first = yield* start(bump(session, a));
        yield* held.entered;
        const later = yield* start(bump(session, b, DEFERRED));
        yield* flush;
        const closing = yield* start(session.close);
        yield* held.release;
        yield* Fiber.join(first);
        yield* Fiber.join(later);
        yield* Fiber.join(closing);
        assert.deepStrictEqual(yield* session.closed, { reason: "closed" });
        assert.deepStrictEqual(yield* notes(storage, [a, b]), [1, 1]);
      }),
    ),
  );
});

describeEachBackend("group commit: backpressure and bounds", (backend) => {
  it.effect(
    "starts no job past 256 without a delivered outcome, and returns its line state to zero when idle",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const { controls, session, ids } = yield* open(2);
          // SAFETY: `open(n)` creates `n` conversations.
          const [a, b] = ids as [ConversationId, ConversationId];
          const held = yield* controls.hold;
          const first = yield* start(bump(session, a));
          yield* held.entered;
          let ran = 0;
          const appends: Array<Fiber.Fiber<void, Tagged>> = [];
          for (let n = 0; n < 300; n++) {
            appends.push(
              yield* start(
                session.commit(
                  Effect.gen(function* () {
                    ran++;
                    const tx = yield* Tx;
                    yield* tx.appendEntry(b, { kind: "note" });
                  }),
                ),
              ),
            );
          }
          yield* eventually(() => ran === 255);
          yield* flush;
          assert.strictEqual(ran, 255);
          assert.strictEqual(session.lineSizes.outstanding, 256);
          yield* held.release;
          yield* Fiber.join(first);
          for (const fiber of appends) yield* Fiber.join(fiber);
          assert.strictEqual(ran, 300);
          yield* eventually(() => session.lineSizes.outstanding === 0);
          assert.deepStrictEqual(session.lineSizes, { outstanding: 0, unpublished: 0, reads: 0 });
          yield* session.close;
        }),
      ),
    30_000,
  );
});

/** A small deterministic generator, so a failing seed reproduces. */
const random = (seed: number) => {
  let state = seed;
  return () => {
    state = (state * 1_103_515_245 + 12_345) % 2_147_483_648;
    return state / 2_147_483_648;
  };
};

describeEachBackend("group commit: randomized interleavings", (backend) => {
  for (const seed of [1, 7, 42, 1_234, 99_991]) {
    it.effect(
      `stores, publishes, and reads only what an order of the commits implies (seed ${seed})`,
      () =>
        run(backend)(
          Effect.gen(function* () {
            const next = random(seed);
            const { storage, controls, session, ids } = yield* open(4);
            const log = ids[0]!;
            controls.latency = () => Math.floor(next() * 3);
            const refuseAt = seed % 2 === 0 ? Math.floor(next() * 60) : -1;
            controls.refuse = (writes) =>
              writes.some(
                (write) =>
                  write.type === "entry" &&
                  // SAFETY: every entry this case writes carries `{ index }`.
                  (write.value.data as { readonly index?: number } | undefined)?.index === refuseAt,
              );
            // Per commit, in line order: the notes it increments and the values it read.
            const plans: Array<ReadonlyArray<ConversationId>> = [];
            const reads: Array<Array<number> | undefined> = [];
            const outcomes: Array<Fiber.Fiber<void, Tagged>> = [];
            for (let index = 0; index < 60; index++) {
              const touched = ids.filter(() => next() < 0.4);
              plans.push(touched);
              const outcome = yield* start(
                session.commit(
                  Effect.gen(function* () {
                    const tx = yield* Tx;
                    const values: Array<number> = [];
                    for (const id of touched) values.push((yield* tx.doc(Note, id)).n++);
                    reads[index] = values;
                    yield* tx.appendEntry(log, { kind: "log", data: { index } });
                  }),
                ),
              );
              outcomes.push(outcome);
              const pause = next();
              if (pause < 0.3) yield* Fiber.await(outcome);
              else if (pause < 0.5) yield* flush;
            }
            const results: Array<boolean> = [];
            for (const outcome of outcomes)
              results.push(Exit.isSuccess(yield* Fiber.await(outcome)));
            if (refuseAt >= 0) yield* session.closed;
            else yield* session.close;
            // Outcomes match storage: a commit resolved exactly when its log entry is stored.
            const reopened = yield* openSession(storage);
            const logged = new Set<number>();
            const entries = yield* reopened.commit(
              withTx((tx) => tx.scanEntries({ conversationId: log }, 1_000)),
            );
            for (const entry of entries.items) {
              // SAFETY: every log entry this case writes carries `{ index }`.
              logged.add((entry.data as { readonly index: number }).index);
            }
            for (const [index, ok] of results.entries()) {
              assert.strictEqual(logged.has(index), ok, `commit ${index}`);
            }
            // Each callback read every earlier stored increment of what it touched, and nothing else.
            for (const [index, values] of reads.entries()) {
              if (values === undefined) continue;
              for (const [position, id] of plans[index]!.entries()) {
                const expected = plans
                  .slice(0, index)
                  .filter((touched, earlier) => results[earlier] && touched.includes(id));
                assert.strictEqual(
                  values[position],
                  expected.length,
                  `commit ${index} read of ${id}`,
                );
              }
            }
            // Storage equals the stored commits replayed in order.
            for (const id of ids) {
              const increments = plans.filter(
                (touched, index) => results[index] && touched.includes(id),
              ).length;
              assert.strictEqual(
                Option.getOrUndefined(yield* reopened.snapshot(Note, id))?.n ?? 0,
                increments,
              );
            }
            assert.deepStrictEqual(session.lineSizes, { outstanding: 0, unpublished: 0, reads: 0 });
            yield* reopened.close;
          }),
        ),
      30_000,
    );
  }
});

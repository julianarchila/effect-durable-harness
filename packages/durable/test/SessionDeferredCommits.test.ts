// Deferred commits, on every backend: what a deferred commit (`CommitPersistence`) holds in memory, what stores
// it, and what storage and each kind of listener then sees.
//
// - Time is `TestClock` and the kernel's `heldDeadline`: the hold loop sleeps on `Clock` (see
//   `internal/session.ts`), so `armed()` is the deadline it sleeps until and `fire()` advances the clock to it.
// - Listeners are the kernel's `observeCommits()` and `followCommits()`, `followCommits` with `ObservedCommit`.
// - A `withinMs` that is not zero or more milliseconds dies with a `TypeError`.
// - The Session does not close its storage, so a reopen is another Session over the same storage.
// - A throw in `checkpointWhen` is a defect of the definition: the commit dies.
//
// "retains a held revision only until it is stored" runs on memory only: see the case.
import { setFlagsFromString } from "node:v8";
import { runInNewContext } from "node:vm";
import { assert, it } from "@effect/vitest";
import * as Delta from "@effect-durable-harness/delta/Delta";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import { AgentStorage, type DocumentAddress, type StorageWrite } from "../src/AgentStorage.ts";
import * as Doc from "../src/Doc.ts";
import type { ConversationId, DocumentId } from "../src/Ids.ts";
import * as internal from "../src/internal/session.ts";
import type { ObservedCommit, SessionKernel } from "../src/internal/session.ts";
import type {
  CommitChange,
  CommitOptions,
  CommitPublication,
  SessionOptions,
} from "../src/Session.ts";
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
import * as Duration from "effect/Duration";
import type { ControlledStorage } from "./SessionSupport.ts";

const Text = Schema.Struct({
  text: Schema.String,
  base: Schema.optionalKey(Schema.Boolean),
  explode: Schema.optionalKey(Schema.Boolean),
  keys: Schema.optionalKey(Schema.Record(Schema.String, Schema.Number)),
});
type Text = typeof Text.Type;

/** A latest, initial-fork conversation document, the kind a deferred commit may hold. */
const TextDoc = Doc.make({
  kind: "test.deferred.text",
  version: 1,
  scope: "conversation",
  history: "latest",
  fork: "initial",
  schema: Text,
  initial: () => ({ text: "" }),
  checkpointWhen: (value) => {
    if (value.explode === true) throw new Error("checkpoint failed");
    return value.base === true;
  },
});

/** `TextDoc` at version 2, which a reload migrates to. */
const TextDocV2 = Doc.make({
  kind: "test.deferred.text",
  version: 2,
  scope: "conversation",
  history: "latest",
  fork: "initial",
  schema: Schema.Struct({ ...Text.fields, migrated: Schema.Literal(true) }),
  initial: () => ({ text: "", migrated: true as const }),
  // SAFETY: `migrate()` gets the older version's value: version 1's `Text`.
  migrate: (value) => ({ ...(value as Text), migrated: true as const }),
  checkpointWhen: () => false,
});

const RewindableDoc = Doc.make({
  kind: "test.deferred.rewindable",
  version: 1,
  scope: "conversation",
  history: "rewindable",
  fork: "asOf",
  schema: Text,
  initial: () => ({ text: "" }),
  checkpointWhen: () => false,
});

const CurrentForkDoc = Doc.make({
  kind: "test.deferred.current-fork",
  version: 1,
  scope: "conversation",
  history: "latest",
  fork: "current",
  schema: Text,
  initial: () => ({ text: "" }),
  checkpointWhen: () => false,
});

const SessionTextDoc = Doc.make({
  kind: "test.deferred.session",
  version: 1,
  scope: "session",
  schema: Text,
  initial: () => ({ text: "" }),
  checkpointWhen: () => false,
});

/** `MAX_PENDING_OPS`: held operations past which a store writes the whole value. */
const MAX_PENDING_OPS = 10_000;

const DEFERRED: CommitOptions = { persistence: { kind: "deferred" } };

const IMMEDIATE: CommitOptions = { persistence: { kind: "immediate" } };

const within = (withinMs: number): CommitOptions => ({
  persistence: { kind: "deferred", withinMs },
});

const address = (conversationId: ConversationId): DocumentAddress => ({
  kind: TextDoc.definition.kind,
  scope: { kind: "conversation", conversationId },
});

interface OpenOptions {
  readonly retentionMs?: number;
  readonly persistWithinMs?: number;
}

/**
 * A Session over controlled storage, recording what `observeCommits` and the Session's own
 * observers (`followCommits()`) get, with one `TextDoc` created.
 */
const open = (options: OpenOptions = {}) =>
  Effect.gen(function* () {
    const storage = yield* controlledStorage;
    const retention = Duration.millis(options.retentionMs ?? 600_000);
    const within = options.persistWithinMs;
    const sessionOptions: SessionOptions =
      within === undefined
        ? { documentRetention: () => retention }
        : { documentRetention: () => retention, persistWithin: () => Duration.millis(within) };
    const session = yield* openSession(storage, sessionOptions);
    const stored: Array<CommitPublication> = [];
    yield* session.observeCommits({
      advance: (publication) => {
        stored.push(publication);
      },
      end: () => {},
    });
    const observed: Array<ObservedCommit> = [];
    yield* session.followCommits((publication) => {
      observed.push(publication);
    });
    const conversationId = yield* createConversation(session);
    yield* session.commit(withTx((tx) => tx.doc(TextDoc, conversationId)));
    const recordId = Option.getOrThrow(
      yield* storage.findDocument(address(conversationId), "current"),
    ).id;
    return { storage, session, stored, observed, conversationId, recordId };
  }).pipe(Effect.orDie);

const withTx = <A, E>(body: (tx: Tx["Service"]) => Effect.Effect<A, E>) =>
  Effect.gen(function* () {
    const tx = yield* Tx;
    return yield* body(tx);
  });

const append = (
  session: SessionKernel,
  conversationId: ConversationId,
  text: string,
  options: CommitOptions = DEFERRED,
) =>
  session.commit(
    withTx((tx) =>
      Effect.map(tx.doc(TextDoc, conversationId), (draft) => {
        draft.text += text;
      }),
    ),
    options,
  );

/** Delay of the armed deadline from now; empty when none is armed. */
const armed = (session: SessionKernel) =>
  Effect.gen(function* () {
    const deadline = session.heldDeadline;
    if (deadline === undefined) return [];
    return [deadline - session.now()];
  });

/** Fires the armed deadline, if any, and lets the line run the store it queued. */
const fire = (session: SessionKernel) =>
  Effect.gen(function* () {
    const deadline = session.heldDeadline;
    if (deadline !== undefined) yield* TestClock.adjust(Math.max(0, deadline - session.now()));
    yield* flush;
    yield* session.readOnLine(Effect.void);
    yield* flush;
  });

/** Batches admitted to storage from here on. */
const mark = (storage: ControlledStorage) => {
  const from = storage.commits.length;
  return () => storage.commits.slice(from);
};

const changesOf = (batches: ReadonlyArray<ReadonlyArray<StorageWrite>>) =>
  batches
    .flat()
    .filter(
      (write): write is Extract<StorageWrite, { readonly type: "document.change" }> =>
        write.type === "document.change",
    );

const storedValue = (storage: ControlledStorage, id: DocumentId) =>
  Effect.map(storage.document(id, "current"), (found) =>
    Option.getOrUndefined(Option.map(found, (document) => document.value)),
  );

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

/** Holds more than `MAX_PENDING_OPS` operations: deferred commits of one set per key. */
const overflow = (session: SessionKernel, conversationId: ConversationId) =>
  Effect.gen(function* () {
    yield* session.commit(
      withTx((tx) =>
        Effect.map(tx.doc(TextDoc, conversationId), (draft) => {
          draft.keys = {};
        }),
      ),
    );
    for (let from = 0; from <= MAX_PENDING_OPS; from += 4_000) {
      yield* session.commit(
        withTx((tx) =>
          Effect.map(tx.doc(TextDoc, conversationId), (draft) => {
            // SAFETY: set by the commit above.
            const keys = draft.keys!;
            for (let index = from; index < from + 4_000; index++) keys[`k${index}`] = index;
          }),
        ),
        DEFERRED,
      );
    }
  });

describeEachBackend("holding a deferred commit", (backend) => {
  it.effect(
    "adopts a commit that only changes an existing latest, initial-fork conversation document in memory, unstored",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const { session, storage, stored, observed, conversationId, recordId } = yield* open();
          const admitted = storage.commits.length;
          const publications = stored.length;
          yield* append(session, conversationId, "a");
          yield* append(session, conversationId, "b");
          assert.strictEqual(storage.commits.length, admitted);
          assert.strictEqual(stored.length, publications);
          assert.deepStrictEqual(
            yield* session.snapshot(TextDoc, conversationId),
            Option.some({ text: "ab" }),
          );
          assert.deepStrictEqual(yield* storedValue(storage, recordId), { text: "" });
          assert.strictEqual(session.documentCacheSizes.held, 1);
          assert.deepStrictEqual(
            observed.slice(-2).map((publication) => publication.kind),
            ["memory", "memory"],
          );
        }),
      ),
  );

  it.effect("stores a deferred commit at once when it writes anything else", () =>
    run(backend)(
      Effect.gen(function* () {
        const { session, storage, conversationId } = yield* open();
        const stores = <E>(change: Effect.Effect<unknown, E, Tx>) =>
          Effect.gen(function* () {
            const admitted = storage.commits.length;
            yield* session.commit(change, DEFERRED);
            return storage.commits.length > admitted;
          });
        // An entry next to the held document's change.
        assert.isTrue(
          yield* stores(
            withTx((tx) =>
              Effect.gen(function* () {
                (yield* tx.doc(TextDoc, conversationId)).text += "x";
                yield* tx.appendEntry(conversationId, { kind: "test.note" });
              }),
            ),
          ),
        );
        // Creation, then other document semantics.
        assert.isTrue(yield* stores(withTx((tx) => tx.doc(RewindableDoc, conversationId))));
        assert.isTrue(
          yield* stores(
            withTx((tx) =>
              Effect.map(tx.doc(RewindableDoc, conversationId), (draft) => {
                draft.text = "r";
              }),
            ),
          ),
        );
        assert.isTrue(yield* stores(withTx((tx) => tx.doc(CurrentForkDoc, conversationId))));
        assert.isTrue(
          yield* stores(
            withTx((tx) =>
              Effect.map(tx.doc(CurrentForkDoc, conversationId), (draft) => {
                draft.text = "c";
              }),
            ),
          ),
        );
        assert.isTrue(yield* stores(withTx((tx) => tx.doc(SessionTextDoc))));
        assert.isTrue(
          yield* stores(
            withTx((tx) =>
              Effect.map(tx.doc(SessionTextDoc), (draft) => {
                draft.text = "s";
              }),
            ),
          ),
        );
        // A held document next to an immediate one: the whole commit is stored.
        assert.isTrue(
          yield* stores(
            withTx((tx) =>
              Effect.gen(function* () {
                (yield* tx.doc(TextDoc, conversationId)).text += "y";
                (yield* tx.doc(SessionTextDoc)).text += "s";
              }),
            ),
          ),
        );
        // Retirement.
        assert.isTrue(yield* stores(withTx((tx) => tx.retireDoc(CurrentForkDoc, conversationId))));
        assert.strictEqual(session.documentCacheSizes.held, 0);
        // The control: the same change alone is held.
        assert.isFalse(
          yield* stores(
            withTx((tx) =>
              Effect.map(tx.doc(TextDoc, conversationId), (draft) => {
                draft.text += "z";
              }),
            ),
          ),
        );
      }),
    ),
  );

  it.effect("stores a deferred commit that migrates its document at once, as a base", () =>
    run(backend)(
      Effect.gen(function* () {
        const { session, storage, conversationId } = yield* open();
        const batches = mark(storage);
        yield* session.commit(
          withTx((tx) =>
            Effect.map(tx.doc(TextDocV2, conversationId), (draft) => {
              draft.text = "v2";
            }),
          ),
          DEFERRED,
        );
        assert.deepStrictEqual(
          changesOf(batches()).map((write) => write.content.kind),
          ["base"],
        );
        assert.strictEqual(session.documentCacheSizes.held, 0);
      }),
    ),
  );

  it.effect("rejects a deferred commit on a read-only Session like any commit with writes", () =>
    run(backend)(
      Effect.gen(function* () {
        const { storage, conversationId } = yield* open();
        const viewer = yield* internal
          .make({ readOnly: true })
          .pipe(Effect.provideService(AgentStorage, storage));
        const error = yield* Effect.flip(append(viewer, conversationId, "a"));
        assert.strictEqual(error._tag, "ReadOnlySession");
      }),
    ),
  );

  it.effect("rejects a withinMs that is not a number of milliseconds, 0 or more", () =>
    run(backend)(
      Effect.gen(function* () {
        const { session, conversationId } = yield* open();
        for (const withinMs of [-1, Number.NaN]) {
          const exit = yield* Effect.exit(append(session, conversationId, "a", within(withinMs)));
          assert.isTrue(Exit.isFailure(exit));
          if (Exit.isFailure(exit)) {
            const defect = exit.cause.reasons.find((reason) => reason._tag === "Die");
            assert.instanceOf(defect?._tag === "Die" ? defect.defect : undefined, TypeError);
          }
        }
        yield* append(session, conversationId, "a", IMMEDIATE);
      }),
    ),
  );
});

describeEachBackend("storing held changes", (backend) => {
  it.effect("stores them first, folded with its own, in a commit that writes the document", () =>
    run(backend)(
      Effect.gen(function* () {
        const { session, storage, conversationId, recordId } = yield* open();
        yield* append(session, conversationId, "a");
        yield* append(session, conversationId, "b");
        const batches = mark(storage);
        yield* append(session, conversationId, "c", IMMEDIATE);
        assert.lengthOf(batches(), 1);
        assert.deepStrictEqual(
          changesOf(batches()).map((write) => write.content),
          [{ version: 1, kind: "delta", ops: [["a", ["text"], "abc"]] }],
        );
        assert.deepStrictEqual(yield* storedValue(storage, recordId), { text: "abc" });
        assert.strictEqual(session.documentCacheSizes.held, 0);
      }),
    ),
  );

  it.effect("stores them in a commit that only reads the document, with its other writes", () =>
    run(backend)(
      Effect.gen(function* () {
        const { session, storage, conversationId, recordId } = yield* open();
        yield* append(session, conversationId, "held");
        const batches = mark(storage);
        yield* session.commit(
          withTx((tx) =>
            Effect.gen(function* () {
              const text = (yield* tx.doc(TextDoc, conversationId)).text;
              yield* tx.appendEntry(conversationId, { kind: "test.derived", data: text });
            }),
          ),
        );
        assert.lengthOf(batches(), 1);
        assert.deepStrictEqual(
          batches()[0]!
            .map((write) => write.type)
            .toSorted(),
          ["document.change", "entry"],
        );
        assert.deepStrictEqual(yield* storedValue(storage, recordId), { text: "held" });
      }),
    ),
  );

  it.effect(
    "leaves them held through commits that neither read nor write the document, or write nothing",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const { session, storage, conversationId } = yield* open();
          yield* append(session, conversationId, "held");
          const batches = mark(storage);
          yield* session.commit(
            withTx((tx) => tx.appendEntry(conversationId, { kind: "test.unrelated" })),
          );
          yield* session.commit(withTx((tx) => tx.doc(TextDoc, conversationId)));
          assert.deepStrictEqual(changesOf(batches()), []);
          assert.strictEqual(session.documentCacheSizes.held, 1);
        }),
      ),
  );

  it.effect("keeps them when a commit that reads the document fails before Storage", () =>
    run(backend)(
      Effect.gen(function* () {
        const { session, storage, conversationId, recordId } = yield* open();
        yield* append(session, conversationId, "held");
        const failed = yield* Effect.flip(
          session.commit(
            withTx((tx) =>
              Effect.gen(function* () {
                (yield* tx.doc(TextDoc, conversationId)).text += "!";
                yield* tx.appendEntry(conversationId, { kind: "test.note" });
                return yield* Effect.fail("callback failed" as const);
              }),
            ),
          ),
        );
        assert.strictEqual(failed, "callback failed");
        // Its checkpoint predicate throws: settlement fails after every other step.
        const exploded = yield* Effect.exit(
          session.commit(
            withTx((tx) =>
              Effect.map(tx.doc(TextDoc, conversationId), (draft) => {
                draft.explode = true;
              }),
            ),
          ),
        );
        assert.isTrue(Exit.hasDies(exploded));
        assert.strictEqual(session.documentCacheSizes.held, 1);
        yield* fire(session);
        assert.deepStrictEqual(yield* storedValue(storage, recordId), { text: "held" });
      }),
    ),
  );

  it.effect("stores every held document in one transaction when the earliest deadline comes", () =>
    run(backend)(
      Effect.gen(function* () {
        const { session, storage, stored, conversationId } = yield* open();
        const other = yield* createConversation(session);
        yield* session.commit(withTx((tx) => tx.doc(TextDoc, other)));
        yield* append(session, conversationId, "one");
        yield* append(session, other, "two");
        assert.deepStrictEqual(yield* armed(session), [1_000]);
        const publications = stored.length;
        const batches = mark(storage);
        yield* fire(session);
        assert.lengthOf(batches(), 1);
        assert.lengthOf(changesOf(batches()), 2);
        assert.strictEqual(stored.length, publications + 1);
        assert.strictEqual(session.documentCacheSizes.held, 0);
        assert.deepStrictEqual(yield* armed(session), []);
        const otherId = Option.getOrThrow(
          yield* storage.findDocument(address(other), "current"),
        ).id;
        assert.deepStrictEqual(yield* storedValue(storage, otherId), { text: "two" });
      }),
    ),
  );

  it.effect(
    "arms the earliest deadline of the held commits, each with its withinMs or the default",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const { session, conversationId } = yield* open({ persistWithinMs: 2_000 });
          yield* append(session, conversationId, "a");
          assert.deepStrictEqual(yield* armed(session), [2_000]);
          yield* TestClock.adjust(500);
          // Earlier: the timer moves up.
          yield* append(session, conversationId, "b", within(100));
          assert.deepStrictEqual(yield* armed(session), [100]);
          // Later: the deadline stays.
          yield* append(session, conversationId, "c", within(60_000));
          assert.deepStrictEqual(yield* armed(session), [100]);
        }),
      ),
  );

  it.effect(
    "arms no timer for an infinite window, and moves the timer on when a commit stores the held document",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const { session, conversationId } = yield* open();
          yield* append(session, conversationId, "a", within(Number.POSITIVE_INFINITY));
          assert.deepStrictEqual(yield* armed(session), []);
          assert.strictEqual(session.documentCacheSizes.held, 1);
          yield* append(session, conversationId, "b");
          assert.deepStrictEqual(yield* armed(session), [1_000]);
          yield* append(session, conversationId, "c", IMMEDIATE);
          assert.deepStrictEqual(yield* armed(session), []);
          assert.strictEqual(session.documentCacheSizes.held, 0);
        }),
      ),
  );

  it.effect(
    "stores them at close, after the commits admitted before it, and fails the Session when that store fails",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const { session, storage, conversationId, recordId } = yield* open();
          yield* append(session, conversationId, "held");
          yield* session.close;
          assert.deepStrictEqual(yield* storedValue(storage, recordId), { text: "held" });
          assert.deepStrictEqual(yield* session.closed, { reason: "closed" });
        }),
      ).pipe(
        Effect.andThen(
          run(backend)(
            Effect.gen(function* () {
              const failing = yield* open();
              yield* append(failing.session, failing.conversationId, "lost");
              failing.storage.failNextCommit("disk gone");
              yield* failing.session.close;
              const end = yield* failing.session.closed;
              assert.strictEqual(end.reason, "failed");
              assert.deepStrictEqual(yield* storedValue(failing.storage, failing.recordId), {
                text: "",
              });
            }),
          ),
        ),
      ),
  );

  it.effect("stores nothing once the Session failed", () =>
    run(backend)(
      Effect.gen(function* () {
        const { session, storage, conversationId, recordId } = yield* open();
        yield* append(session, conversationId, "lost");
        const batches = mark(storage);
        session.fail(new Error("crash"));
        yield* session.closed;
        assert.deepStrictEqual(batches(), []);
        assert.deepStrictEqual(yield* storedValue(storage, recordId), { text: "" });
      }),
    ),
  );

  it.effect("stores them before unloadDocuments() drops the trackers", () =>
    run(backend)(
      Effect.gen(function* () {
        const { session, storage, conversationId, recordId } = yield* open();
        yield* append(session, conversationId, "held");
        yield* session.unloadDocuments;
        assert.deepStrictEqual(yield* storedValue(storage, recordId), { text: "held" });
        assert.strictEqual(session.documentCacheSizes.cached, 0);
        assert.strictEqual(session.documentCacheSizes.held, 0);
        assert.deepStrictEqual(
          yield* session.snapshot(TextDoc, conversationId),
          Option.some({ text: "held" }),
        );
      }),
    ),
  );

  it.effect("stores them before a token of another definition version reloads the document", () =>
    run(backend)(
      Effect.gen(function* () {
        const { session, storage, conversationId, recordId } = yield* open();
        yield* append(session, conversationId, "held");
        const batches = mark(storage);
        assert.deepStrictEqual(
          yield* session.snapshot(TextDocV2, conversationId),
          Option.some({ text: "held", migrated: true as const }),
        );
        assert.deepStrictEqual(
          changesOf(batches()).map((write) => write.content.kind),
          ["delta"],
        );
        assert.deepStrictEqual(yield* storedValue(storage, recordId), { text: "held" });
        // The same inside a commit, which then migrates with a base.
        yield* session.commit(withTx((tx) => tx.doc(TextDoc, conversationId)));
        yield* append(session, conversationId, "+");
        yield* session.commit(
          withTx((tx) =>
            Effect.map(tx.doc(TextDocV2, conversationId), (draft) => {
              draft.text += "v2";
            }),
          ),
        );
        assert.deepStrictEqual(yield* storedValue(storage, recordId), {
          text: "held+v2",
          migrated: true,
        });
      }),
    ),
  );

  it.effect("stores them before the document's retirement, in the retiring commit", () =>
    run(backend)(
      Effect.gen(function* () {
        const { session, storage, conversationId } = yield* open();
        yield* append(session, conversationId, "held");
        const batches = mark(storage);
        yield* session.commit(withTx((tx) => tx.retireDoc(TextDoc, conversationId)));
        assert.deepStrictEqual(
          batches()[0]!.map((write) => write.type),
          ["document.change", "document.retire"],
        );
        assert.deepStrictEqual(changesOf(batches())[0]!.content, {
          version: 1,
          kind: "delta",
          ops: [["a", ["text"], "held"]],
        });
        assert.strictEqual(session.documentCacheSizes.held, 0);
        assert.isTrue(Option.isNone(yield* session.snapshot(TextDoc, conversationId)));
      }),
    ),
  );
});

describeEachBackend("bases and overflow", (backend) => {
  it.effect(
    "stores a base when a held commit selected one, and counts the store as one delta otherwise",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const { session, storage, conversationId } = yield* open();
          const batches = mark(storage);
          yield* session.commit(
            withTx((tx) =>
              Effect.map(tx.doc(TextDoc, conversationId), (draft) => {
                draft.text = "x";
                draft.base = true;
              }),
            ),
            DEFERRED,
          );
          yield* append(session, conversationId, "y");
          yield* fire(session);
          assert.deepStrictEqual(
            changesOf(batches()).map((write) => write.content),
            [{ version: 1, kind: "base", value: { text: "xy", base: true } }],
          );
        }),
      ),
  );

  it.effect("stores the whole value once more than MAX_PENDING_OPS operations are held", () =>
    run(backend)(
      Effect.gen(function* () {
        const { session, storage, stored, conversationId } = yield* open();
        yield* overflow(session, conversationId);
        const batches = mark(storage);
        yield* fire(session);
        const value = Option.getOrThrow(yield* session.snapshot(TextDoc, conversationId));
        assert.deepStrictEqual(
          changesOf(batches()).map((write) => write.content),
          [{ version: 1, kind: "base", value }],
        );
        // A durable listener replaces its copy with the stored value.
        const last = stored.at(-1)!.changes;
        assert.lengthOf(last, 1);
        assert.deepStrictEqual(last[0]!.type === "document" && last[0]!.ops, [["r", value]]);
      }),
    ),
  );

  it.effect("stores a base of the commit's own value when it stores an overflowed hold", () =>
    run(backend)(
      Effect.gen(function* () {
        const { session, storage, conversationId } = yield* open();
        yield* overflow(session, conversationId);
        yield* append(session, conversationId, "a");
        const batches = mark(storage);
        yield* append(session, conversationId, "b", IMMEDIATE);
        const value = Option.getOrThrow(yield* session.snapshot(TextDoc, conversationId));
        assert.strictEqual(value.text, "ab");
        assert.deepStrictEqual(
          changesOf(batches()).map((write) => write.content),
          [{ version: 1, kind: "base", value }],
        );
      }),
    ),
  );
});

describeEachBackend("publications", (backend) => {
  it.effect("give observeCommits() exactly what was stored, and nothing for a held commit", () =>
    run(backend)(
      Effect.gen(function* () {
        const { session, storage, conversationId, recordId, stored } = yield* open();
        const from = storage.commits.length;
        const publications = stored.length;
        yield* append(session, conversationId, "a");
        yield* append(session, conversationId, "b");
        yield* append(session, conversationId, "c", IMMEDIATE);
        yield* append(session, conversationId, "d");
        yield* fire(session);
        yield* append(session, conversationId, "e");
        yield* session.commit(
          withTx((tx) =>
            Effect.gen(function* () {
              yield* tx.doc(TextDoc, conversationId);
              yield* tx.appendEntry(conversationId, { kind: "test.note" });
            }),
          ),
        );
        yield* append(session, conversationId, "f");
        yield* session.close;
        const batches = storage.commits.slice(from);
        const published = stored.slice(publications);
        assert.lengthOf(published, batches.length);
        // A replica built from the publications alone always equals what storage holds.
        let fromStorage: Schema.Json = { text: "" };
        let fromPublications: Schema.Json = { text: "" };
        for (const [index, batch] of batches.entries()) {
          const write = changesOf([batch]).find((candidate) => candidate.id === recordId)!;
          const change = published[index]!.changes.find(
            (candidate): candidate is Extract<CommitChange, { readonly type: "document" }> =>
              candidate.type === "document" &&
              candidate.value !== null &&
              candidate.record.id === recordId,
          )!;
          fromStorage =
            write.content.kind === "base"
              ? write.content.value
              : Result.getOrThrow(Delta.applyImmutable(fromStorage, write.content.ops));
          fromPublications = Result.getOrThrow(Delta.applyImmutable(fromPublications, change.ops));
          assert.deepStrictEqual(fromPublications, fromStorage);
          assert.deepStrictEqual(change.value, fromStorage);
        }
        assert.deepStrictEqual(fromStorage, { text: "abcdef" });
        assert.deepStrictEqual(
          published.map((publication) =>
            publication.changes.map((change) => change.type).toSorted(),
          ),
          [["document"], ["document"], ["document", "entry"], ["document"]],
        );
      }),
    ),
  );

  it.effect("show every held commit to watches and the Session's own observers, once", () =>
    run(backend)(
      Effect.gen(function* () {
        const { session, conversationId, observed } = yield* open();
        const { value: watch, release } = yield* acquire(session.watchDoc(TextDoc, conversationId));
        const values: Array<string> = [];
        const consumer = yield* watch.frames.pipe(
          Stream.runForEach((frame) => Effect.sync(() => values.push(frame.value.text))),
          Effect.forkChild,
        );
        yield* flush;
        const from = observed.length;
        for (const text of ["a", "b", "c"]) {
          yield* append(session, conversationId, text);
          yield* flush;
        }
        yield* append(session, conversationId, "d", IMMEDIATE);
        yield* flush;
        yield* fire(session);
        assert.deepStrictEqual(values, ["a", "ab", "abc", "abcd"]);
        const seen = observed.slice(from);
        assert.deepStrictEqual(
          seen.map((publication) => publication.kind),
          ["memory", "memory", "memory", "stored"],
        );
        // The storing commit shows its own operations; the held ones were shown already. The deadline showed
        // nothing.
        assert.deepStrictEqual(
          seen.map((publication) =>
            publication.changes.map((change) => change.type === "document" && change.ops),
          ),
          [
            [[["a", ["text"], "a"]]],
            [[["a", ["text"], "b"]]],
            [[["a", ["text"], "c"]]],
            [[["a", ["text"], "d"]]],
          ],
        );
        yield* release;
        yield* Fiber.interrupt(consumer);
      }),
    ),
  );
});

describeEachBackend("pinning", (backend) => {
  it.effect(
    "keeps a held document cached with retention 0 until it is stored, then lets it go",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const { session, conversationId } = yield* open({ retentionMs: 0 });
          yield* session.readOnLine(Effect.void);
          yield* flush;
          assert.deepInclude(session.documentCacheSizes, { cached: 0, held: 0, attached: 0 });
          yield* append(session, conversationId, "held");
          yield* session.readOnLine(Effect.void);
          yield* flush;
          assert.deepInclude(session.documentCacheSizes, { cached: 1, held: 1, attached: 1 });
          yield* fire(session);
          yield* session.commit(Effect.void);
          yield* flush;
          assert.deepInclude(session.documentCacheSizes, { cached: 0, held: 0, attached: 0 });
          assert.deepStrictEqual(
            yield* session.snapshot(TextDoc, conversationId),
            Option.some({ text: "held" }),
          );
        }),
      ),
  );
});

describeEachBackend("pinning a held revision", (backend) => {
  // Memory only: on `sqlite-node`, `effect/sql`'s prepared-statement cache keeps the fiber that first prepared each
  // statement, whose stack frame holds that commit's publication, so the first store's revision stays reachable
  // through it (one per statement, whatever the Session does). The pinning case above checks the unpin there.
  const test = backend.name === "sqlite" ? it.effect.skip : it.effect;
  test("retains a held revision only until it is stored", () =>
    run(backend)(
      Effect.gen(function* () {
        const { session, conversationId, stored, observed } = yield* open({ retentionMs: 0 });
        yield* append(session, conversationId, "x".repeat(1_000));
        const revision = new WeakRef(
          Option.getOrThrow(yield* session.snapshot(TextDoc, conversationId)),
        );
        yield* collect;
        assert.isDefined(revision.deref());
        yield* fire(session);
        yield* session.commit(Effect.void);
        // The recorded publications would keep it.
        stored.length = 0;
        observed.length = 0;
        yield* flush;
        yield* collect;
        yield* flush;
        yield* collect;
        assert.isUndefined(revision.deref());
      }),
    ));
});

describeEachBackend("crash", (backend) => {
  it.effect(
    "leaves Storage at the last stored state, which memory had, and nothing held after reopen",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const { session, storage, conversationId, recordId } = yield* open();
          const states: Array<string> = [];
          yield* session.followCommits((publication) => {
            for (const change of publication.changes) {
              if (
                change.type === "document" &&
                change.value !== null &&
                change.record.id === recordId
              ) {
                // SAFETY: publications carry untyped document values; this one is `TextDoc`'s.
                states.push((change.value as Text).text);
              }
            }
          });
          yield* append(session, conversationId, "a");
          yield* fire(session);
          yield* append(session, conversationId, "b");
          yield* append(session, conversationId, "c");
          session.fail(new Error("crash"));
          yield* session.closed;
          const reopened = yield* openSession(storage);
          const value = Option.getOrThrow(yield* reopened.snapshot(TextDoc, conversationId));
          assert.deepStrictEqual(value, { text: "a" });
          assert.include(states, value.text);
          assert.strictEqual(reopened.documentCacheSizes.held, 0);
        }),
      ),
  );
});

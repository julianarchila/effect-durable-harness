// Session documents, on every backend.
//
// A "pending acquisition" is one forked with `startImmediately` that the
// callback does not join; close is the kernel's `close`. Checks match the
// error message and add the error tag where one exists.
import { assert, it } from "@effect/vitest";
import type * as Tracker from "@effect-durable-harness/delta/Tracker";
import * as Data from "effect/Data";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Doc from "../src/Doc.ts";
import { type ConversationId, TaskId } from "../src/Ids.ts";
import type { TaskRecord } from "../src/Records.ts";
import { DocumentAccessError } from "../src/SessionError.ts";
import { Tx, type TxDocError } from "../src/Tx.ts";
import {
  createConversation,
  describeEachBackend,
  documentChanges,
  failureMessage,
  flush,
  openTestSession,
  run,
  type TestSession,
} from "./SessionSupport.ts";

class CallbackFailed extends Data.TaggedError("CallbackFailed")<{}> {
  override get message() {
    return "callback failed";
  }
}

const Live = Schema.Struct({
  message: Schema.optionalKey(Schema.String),
  items: Schema.Array(Schema.String),
  nested: Schema.Struct({ count: Schema.Number }),
  other: Schema.Struct({ label: Schema.String }),
});
type Live = typeof Live.Type;

let liveInitCount = 0;
const LiveDoc = Doc.make({
  checkpointWhen: () => false,
  kind: "test.live",
  version: 1,
  scope: "conversation",
  history: "latest",
  fork: "initial",
  schema: Live,
  initial: () => {
    liveInitCount++;
    return { items: [], nested: { count: 0 }, other: { label: "x" } };
  },
});

const RewindableLiveDoc = Doc.make({
  checkpointWhen: () => false,
  kind: "test.live",
  version: 1,
  scope: "conversation",
  history: "rewindable",
  fork: "asOf",
  schema: Live,
  initial: () => ({ items: [], nested: { count: 0 }, other: { label: "x" } }),
});

const LiveDocV2 = Doc.make({
  checkpointWhen: () => false,
  kind: "test.live",
  version: 2,
  scope: "conversation",
  history: "latest",
  fork: "initial",
  schema: Live,
  initial: () => ({ items: [], nested: { count: 0 }, other: { label: "x" } }),
});

const CounterDoc = Doc.make({
  checkpointWhen: () => false,
  kind: "test.counter",
  version: 1,
  scope: "session",
  schema: Schema.Struct({ count: Schema.Number }),
  initial: () => ({ count: 0 }),
});

const seeds: Array<string> = [];
const MemberDoc = Doc.family({
  checkpointWhen: () => false,
  kind: "test.member",
  version: 1,
  family: true,
  scope: "session",
  schema: Schema.Struct({ seed: Schema.String, hits: Schema.Number }),
  initial: (seed: string) => {
    seeds.push(seed);
    return { seed, hits: 0 };
  },
});

const setupLive = Effect.gen(function* () {
  const harness = yield* openTestSession();
  const conversationId = yield* createConversation(harness.session);
  yield* harness.session.commit(
    Effect.gen(function* () {
      const tx = yield* Tx;
      const live = yield* tx.doc(LiveDoc, conversationId);
      live.items.push("a", "b");
    }),
  );
  return { ...harness, conversationId };
});

const snapshot = (harness: TestSession, conversationId: ConversationId) =>
  harness.session.snapshot(LiveDoc, conversationId).pipe(Effect.map(Option.getOrUndefined));

const setMessage = (harness: TestSession, conversationId: ConversationId, message: string) =>
  harness.session.commit(
    Effect.gen(function* () {
      const tx = yield* Tx;
      (yield* tx.doc(LiveDoc, conversationId)).message = message;
    }),
  );

/** Runs `effect`, which must fail with a `DocumentAccessError` for a value its schema rejects. */
const failsInvalidValue = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  Effect.gen(function* () {
    const error = yield* Effect.flip(effect);
    assert.instanceOf(error, DocumentAccessError);
    assert.strictEqual(error.reason._tag, "InvalidValue");
    assert.include(error.message, "does not match the schema of version");
  });

describeEachBackend("Session document transactions", (backend) => {
  it.effect("creates an initial base on first access and adopts it after Storage success", () =>
    run(backend)(
      Effect.gen(function* () {
        const harness = yield* openTestSession();
        const { session, storage, publications } = harness;
        const conversationId = yield* createConversation(session);
        yield* setMessage(harness, conversationId, "hello");
        const writes = storage.commits.at(-1)!;
        assert.lengthOf(writes, 1);
        assert.deepInclude(writes[0], { type: "document.create" });
        const create = writes[0]!;
        if (create.type !== "document.create") throw new Error("expected a create");
        assert.deepInclude(create.record, {
          kind: "test.live",
          scope: { kind: "conversation", conversationId },
          history: "latest",
          fork: "initial",
        });
        assert.deepStrictEqual(create.content, {
          kind: "base",
          version: 1,
          value: { message: "hello", items: [], nested: { count: 0 }, other: { label: "x" } },
        });
        const value = yield* snapshot(harness, conversationId);
        assert.deepStrictEqual(value, {
          message: "hello",
          items: [],
          nested: { count: 0 },
          other: { label: "x" },
        });
        const publication = publications.at(-1)!;
        const published = documentChanges(publication)[0]!;
        assert.strictEqual(published.record.createdAt, publication.seq);
        assert.strictEqual(published.value, value);
        assert.strictEqual(published.conversationId, conversationId);
        const admitted = storage.admittedCommits
          .at(-1)!
          .find((write) => write.type === "document.create")!;
        if (admitted.type !== "document.create") throw new Error("expected a create");
        assert.strictEqual(admitted.content.value, published.value);
      }),
    ),
  );

  it.effect("never creates on snapshot and returns undefined when absent", () =>
    run(backend)(
      Effect.gen(function* () {
        const harness = yield* openTestSession();
        const { session, storage } = harness;
        const conversationId = yield* createConversation(session);
        const before = storage.commits.length;
        assert.isTrue(Option.isNone(yield* session.snapshot(LiveDoc, conversationId)));
        assert.isTrue(Option.isNone(yield* session.snapshot(CounterDoc)));
        assert.isTrue(Option.isNone(yield* session.snapshot(MemberDoc, "k")));
        assert.strictEqual(storage.commits.length, before);
        assert.strictEqual(storage.mintCount, 1);
      }),
    ),
  );

  it.effect("returns shared immutable snapshots and keeps prior revisions stable", () =>
    run(backend)(
      Effect.gen(function* () {
        const harness = yield* setupLive;
        const { session, conversationId } = harness;
        const first = (yield* snapshot(harness, conversationId))!;
        assert.strictEqual(yield* snapshot(harness, conversationId), first);
        yield* session.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            (yield* tx.doc(LiveDoc, conversationId)).nested.count = 1;
          }),
        );
        const second = (yield* snapshot(harness, conversationId))!;
        assert.notStrictEqual(second, first);
        assert.strictEqual(first.nested.count, 0);
        assert.strictEqual(second.nested.count, 1);
        // Unchanged subtrees are structurally shared between immutable revisions.
        assert.strictEqual(second.items, first.items);
        assert.strictEqual(second.other, first.other);
      }),
    ),
  );

  it.effect(
    "adopts by pointer swap and shares operation payloads with the published revision",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const harness = yield* setupLive;
          const { session, storage, publications, conversationId } = harness;
          yield* session.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              const live = yield* tx.doc(LiveDoc, conversationId);
              live.other = { label: "y" };
              live.items.push("c");
            }),
          );
          const value = (yield* snapshot(harness, conversationId))!;
          const published = documentChanges(publications.at(-1)!)[0]!;
          assert.strictEqual(published.value, value);
          const admitted = storage.admittedCommits
            .at(-1)!
            .find((write) => write.type === "document.change")!;
          if (admitted.type !== "document.change" || admitted.content.kind !== "delta") {
            throw new Error("Expected delta");
          }
          assert.strictEqual(published.ops, admitted.content.ops);
          const set = published.ops.find((op) => op[0] === "s");
          assert.deepStrictEqual(set, ["s", ["other"], { label: "y" }]);
          // Trusted immutability: the Session makes no second copy of operation payloads.
          assert.strictEqual(set![2], value.other);
        }),
      ),
  );

  it.effect("copies assigned values per placement", () =>
    run(backend)(
      Effect.gen(function* () {
        const harness = yield* setupLive;
        const { session, conversationId } = harness;
        const value = { label: "shared" };
        yield* session.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            const live = yield* tx.doc(LiveDoc, conversationId);
            live.other = value;
            // SAFETY: the draft accepts any JSON property; the schema allows the extra one on load.
            (live as Tracker.Draft<Live> & { copy?: { label: string } }).copy = value;
            value.label = "mutated";
          }),
        );
        // SAFETY: the commit above placed `copy` next to the schema's fields.
        const stored = (yield* snapshot(harness, conversationId)) as Live & {
          copy: { label: string };
        };
        assert.deepStrictEqual(stored.other, { label: "shared" });
        assert.deepStrictEqual(stored.copy, { label: "shared" });
        assert.notStrictEqual(stored.copy, stored.other);
        assert.notStrictEqual(stored.other, value);
      }),
    ),
  );

  it.effect("suppresses writes and publications for empty batches", () =>
    run(backend)(
      Effect.gen(function* () {
        const { session, storage, publications, conversationId } = yield* setupLive;
        const commits = storage.commits.length;
        const published = publications.length;
        yield* session.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            const live = yield* tx.doc(LiveDoc, conversationId);
            live.nested.count = 0;
            live.items.push("z");
            live.items.pop();
          }),
        );
        assert.strictEqual(storage.commits.length, commits);
        assert.strictEqual(publications.length, published);
      }),
    ),
  );

  it.effect("writes and publishes replayable nonempty structural no-ops", () =>
    run(backend)(
      Effect.gen(function* () {
        const harness = yield* setupLive;
        const { session, storage, publications, conversationId } = harness;
        const before = (yield* snapshot(harness, conversationId))!;
        yield* session.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            const live = yield* tx.doc(LiveDoc, conversationId);
            const first = live.items.shift()!;
            live.items.unshift(first);
          }),
        );
        const write = storage.commits.at(-1)![0]!;
        assert.strictEqual(write.type, "document.change");
        if (write.type === "document.change") assert.strictEqual(write.content.kind, "delta");
        const after = (yield* snapshot(harness, conversationId))!;
        assert.deepStrictEqual(after, before);
        assert.notStrictEqual(after, before);
        assert.isAbove(documentChanges(publications.at(-1)!)[0]!.ops.length, 0);
      }),
    ),
  );

  it.effect("revokes escaped drafts when the callback settles", () =>
    run(backend)(
      Effect.gen(function* () {
        const harness = yield* setupLive;
        const { session, conversationId } = harness;
        let escaped: Tracker.Draft<Live> | undefined;
        let items: Array<string> | undefined;
        yield* session.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            escaped = yield* tx.doc(LiveDoc, conversationId);
            items = escaped.items;
            escaped.message = "inside";
          }),
        );
        assert.throws(() => escaped!.message);
        assert.throws(() => {
          escaped!.message = "outside";
        });
        assert.throws(() => items!.length);
        assert.throws(() => items!.push("outside"));
        assert.strictEqual((yield* snapshot(harness, conversationId))!.message, "inside");

        const returned = yield* session.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            return yield* tx.doc(LiveDoc, conversationId);
          }),
        );
        assert.throws(() => returned.message);
      }),
    ),
  );

  it.effect("aborts every change when the callback fails", () =>
    run(backend)(
      Effect.gen(function* () {
        const harness = yield* setupLive;
        const { session, storage, conversationId } = harness;
        const before = (yield* snapshot(harness, conversationId))!;
        const commits = storage.commits.length;
        const message = yield* failureMessage(
          session.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              const live = yield* tx.doc(LiveDoc, conversationId);
              const counter = yield* tx.doc(CounterDoc);
              live.message = "lost";
              counter.count = 5;
              return yield* new CallbackFailed();
            }),
          ),
        );
        assert.include(message, "callback failed");
        assert.strictEqual(storage.commits.length, commits);
        assert.strictEqual(yield* snapshot(harness, conversationId), before);
        assert.isTrue(Option.isNone(yield* session.snapshot(CounterDoc)));
        yield* setMessage(harness, conversationId, "kept");
        assert.strictEqual((yield* snapshot(harness, conversationId))!.message, "kept");
      }),
    ),
  );

  it.effect("memoizes concurrent duplicate acquisition and initializes once", () =>
    run(backend)(
      Effect.gen(function* () {
        const { session, storage } = yield* openTestSession();
        const conversationId = yield* createConversation(session);
        const initCount = liveInitCount;
        const mints = storage.mintCount;
        yield* session.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            const [first, second] = yield* Effect.all(
              [tx.doc(LiveDoc, conversationId), tx.doc(LiveDoc, conversationId)],
              { concurrency: "unbounded" },
            );
            assert.strictEqual(first, second);
            assert.strictEqual(yield* tx.doc(LiveDoc, conversationId), first);
            first.message = "once";
          }),
        );
        assert.strictEqual(liveInitCount, initCount + 1);
        assert.strictEqual(storage.mintCount, mints + 1);
        assert.lengthOf(
          storage.commits.at(-1)!.filter((write) => write.type === "document.create"),
          1,
        );
      }),
    ),
  );

  it.effect("uses the first family seed and ignores seeds for existing members", () =>
    run(backend)(
      Effect.gen(function* () {
        const { session } = yield* openTestSession();
        seeds.length = 0;
        yield* session.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            const first = yield* tx.doc(MemberDoc, "k", "first");
            const second = yield* tx.doc(MemberDoc, "k", "second");
            assert.strictEqual(second, first);
            first.hits++;
          }),
        );
        yield* session.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            (yield* tx.doc(MemberDoc, "k", "third")).hits++;
            (yield* tx.doc(MemberDoc, "other", "fourth")).hits++;
          }),
        );
        assert.deepStrictEqual(seeds, ["first", "fourth"]);
        assert.deepStrictEqual(Option.getOrUndefined(yield* session.snapshot(MemberDoc, "k")), {
          seed: "first",
          hits: 2,
        });
        assert.deepStrictEqual(Option.getOrUndefined(yield* session.snapshot(MemberDoc, "other")), {
          seed: "fourth",
          hits: 1,
        });
      }),
    ),
  );

  it.effect("rejects a callback that succeeds with a pending acquisition and drains it", () =>
    run(backend)(
      Effect.gen(function* () {
        const harness = yield* setupLive;
        const { session, storage, conversationId } = harness;
        yield* session.unloadDocuments;
        const gate = yield* storage.holdFindDocument;
        // A fiber resumes inline when a `Deferred` completes, so the test waits for the callback to return
        // and lets the commit reach settlement before it opens the gate.
        const returned = yield* Deferred.make<void>();
        const commits = storage.commits.length;
        let pending: Fiber.Fiber<Tracker.Draft<Live>, TxDocError> | undefined;
        const commit = yield* session
          .commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              pending = yield* Effect.forkChild(tx.doc(LiveDoc, conversationId), {
                startImmediately: true,
              });
              yield* Deferred.succeed(returned, undefined);
            }),
          )
          .pipe(Effect.forkChild);
        yield* gate.entered;
        yield* Deferred.await(returned);
        yield* flush;
        // The line stays held until the late acquisition settles.
        assert.isUndefined(commit.pollUnsafe());
        yield* gate.release;
        assert.include(yield* failureMessage(Fiber.join(commit)), "pending Tx operations");
        assert.include(yield* failureMessage(Fiber.join(pending!)), "Transaction has settled");
        assert.strictEqual(storage.commits.length, commits);
        yield* setMessage(harness, conversationId, "after");
        assert.strictEqual((yield* snapshot(harness, conversationId))!.message, "after");
      }),
    ),
  );

  it.effect(
    "does not initialize or mint for an absent acquisition that finishes after settlement",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const { session, storage } = yield* openTestSession();
          let initialized = 0;
          const LateDoc = Doc.make({
            checkpointWhen: () => false,
            kind: "test.late",
            version: 1,
            scope: "session",
            schema: Schema.Struct({ count: Schema.Number }),
            initial: () => {
              initialized++;
              return { count: 0 };
            },
          });
          const gate = yield* storage.holdFindDocument;
          // A fiber resumes inline when a `Deferred` completes, so the test waits for the callback to return
          // and lets the commit reach settlement before it opens the gate.
          const returned = yield* Deferred.make<void>();
          const mints = storage.mintCount;
          let pending:
            | Fiber.Fiber<Tracker.Draft<{ readonly count: number }>, TxDocError>
            | undefined;
          const commit = yield* session
            .commit(
              Effect.gen(function* () {
                const tx = yield* Tx;
                pending = yield* Effect.forkChild(tx.doc(LateDoc), { startImmediately: true });
                yield* Deferred.succeed(returned, undefined);
              }),
            )
            .pipe(Effect.forkChild);
          yield* gate.entered;
          yield* Deferred.await(returned);
          yield* flush;
          yield* gate.release;
          assert.include(yield* failureMessage(Fiber.join(commit)), "pending Tx operations");
          assert.include(yield* failureMessage(Fiber.join(pending!)), "Transaction has settled");
          assert.strictEqual(initialized, 0);
          assert.strictEqual(storage.mintCount, mints);
        }),
      ),
  );

  it.effect("rejects with the callback error when it fails with a pending acquisition", () =>
    run(backend)(
      Effect.gen(function* () {
        const { session, storage, conversationId } = yield* setupLive;
        yield* session.unloadDocuments;
        const gate = yield* storage.holdFindDocument;
        // A fiber resumes inline when a `Deferred` completes, so the test waits for the callback to return
        // and lets the commit reach settlement before it opens the gate.
        const returned = yield* Deferred.make<void>();
        let pending: Fiber.Fiber<Tracker.Draft<Live>, TxDocError> | undefined;
        const commit = yield* session
          .commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              pending = yield* Effect.forkChild(tx.doc(LiveDoc, conversationId), {
                startImmediately: true,
              });
              yield* Deferred.succeed(returned, undefined);
              return yield* new CallbackFailed();
            }),
          )
          .pipe(Effect.forkChild);
        yield* gate.entered;
        yield* Deferred.await(returned);
        yield* flush;
        yield* gate.release;
        assert.include(yield* failureMessage(Fiber.join(commit)), "callback failed");
        assert.include(yield* failureMessage(Fiber.join(pending!)), "Transaction has settled");
      }),
    ),
  );

  it.effect("rejects Tx use after the callback settles", () =>
    run(backend)(
      Effect.gen(function* () {
        const { session, conversationId } = yield* setupLive;
        let captured: Parameters<Parameters<typeof session.commitWith>[0]>[0] | undefined;
        yield* session.commitWith((tx) =>
          Effect.sync(() => {
            captured = tx;
          }),
        );
        const tx = captured!;
        assert.include(
          yield* failureMessage(tx.service.doc(LiveDoc, conversationId)),
          "Transaction has settled",
        );
        assert.include(
          yield* failureMessage(tx.service.conversation(conversationId)),
          "Transaction has settled",
        );
        // SAFETY: the record is never read; a settled transaction rejects before looking at it.
        assert.include(
          yield* failureMessage(tx.setTask({} as TaskRecord)),
          "Transaction has settled",
        );
      }),
    ),
  );

  it.effect("rejects tokens whose semantics or version disagree with the stored incarnation", () =>
    run(backend)(
      Effect.gen(function* () {
        const { session, conversationId } = yield* setupLive;
        const semantics = yield* Effect.flip(session.snapshot(RewindableLiveDoc, conversationId));
        assert.strictEqual(semantics._tag, "DocumentAccessError");
        assert.include(semantics.message, "does not match the supplied definition semantics");
        assert.include(
          yield* failureMessage(
            session.commit(
              Effect.gen(function* () {
                const tx = yield* Tx;
                yield* tx.doc(RewindableLiveDoc, conversationId);
              }),
            ),
          ),
          "does not match the supplied definition semantics",
        );
        assert.include(
          yield* failureMessage(
            session.commit(
              Effect.gen(function* () {
                const tx = yield* Tx;
                yield* tx.doc(LiveDocV2, conversationId);
              }),
            ),
          ),
          "requires migration from version 1",
        );
      }),
    ),
  );

  // A deploy that changes a definition's schema without a new version: the stored value no longer matches it.
  it.effect(
    "fails a cold load, a fork copy and a migration whose value the schema rejects with InvalidValue",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const { session } = yield* openTestSession();
          const definition = {
            checkpointWhen: () => false,
            kind: "test.checked",
            scope: "conversation",
            history: "rewindable",
            fork: "asOf",
          } as const;
          const Wide = Doc.make({
            ...definition,
            version: 1,
            schema: Schema.Struct({ count: Schema.Number }),
            initial: () => ({ count: 1 }),
          });
          const Narrow = Doc.make({
            ...definition,
            version: 1,
            schema: Schema.Struct({ count: Schema.String }),
            initial: () => ({ count: "1" }),
          });
          const Migrated = Doc.make({
            ...definition,
            version: 2,
            schema: Schema.Struct({ label: Schema.NonEmptyString }),
            initial: () => ({ label: "none" }),
            // A string, but not the non-empty one the schema requires.
            migrate: () => ({ label: "" }),
          });
          const conversationId = yield* createConversation(session);
          const at = yield* session.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              yield* tx.doc(Wide, conversationId);
              return (yield* tx.appendEntry(conversationId, { kind: "point" })).id;
            }),
          );

          yield* session.unloadDocuments;
          yield* failsInvalidValue(session.snapshot(Narrow, conversationId));

          yield* session.unloadDocuments;
          yield* failsInvalidValue(
            session.commit(
              Effect.gen(function* () {
                const tx = yield* Tx;
                const child = yield* tx.forkConversation(conversationId, at, {
                  ownership: { kind: "ownerless" },
                });
                yield* tx.doc(Narrow, child.id);
              }),
            ),
          );

          yield* session.unloadDocuments;
          yield* failsInvalidValue(session.snapshot(Migrated, conversationId));
          // The stored value is left as it was.
          assert.deepStrictEqual(
            yield* session.snapshot(Wide, conversationId),
            Option.some({ count: 1 }),
          );
        }),
      ),
  );

  it.effect(
    "rejects non-JSON initializer values and draft placements before Storage admission",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const harness = yield* openTestSession();
          const { session, storage } = harness;
          const conversationId = yield* createConversation(session);
          const DateDoc = Doc.make({
            checkpointWhen: () => false,
            kind: "test.date",
            version: 1,
            scope: "session",
            // `Schema.Any` lets the initializer return a class instance, to check the initializer copy.
            schema: Schema.Struct({ at: Schema.Any }),
            initial: () => ({ at: new Map() }),
          });
          assert.include(
            yield* failureMessage(
              session.commit(
                Effect.gen(function* () {
                  const tx = yield* Tx;
                  yield* tx.doc(DateDoc);
                }),
              ),
            ),
            "strict JSON",
          );
          const commits = storage.commits.length;
          // A placement that is not JSON throws inside the callback: a defect that rolls the commit back.
          assert.include(
            yield* failureMessage(
              session.commit(
                Effect.gen(function* () {
                  const tx = yield* Tx;
                  const live = yield* tx.doc(LiveDoc, conversationId);
                  // Deliberately not JSON, to check the placement copy.
                  live.items.push(undefined!);
                }),
              ),
            ),
            "strict JSON",
          );
          assert.strictEqual(storage.commits.length, commits);
          assert.isUndefined(yield* snapshot(harness, conversationId));
        }),
      ),
  );

  it.effect("rolls back prepared documents when batch assembly fails", () =>
    run(backend)(
      Effect.gen(function* () {
        const harness = yield* setupLive;
        const { session, storage, conversationId } = harness;
        yield* session.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            (yield* tx.doc(CounterDoc)).count = 1;
          }),
        );
        const live = yield* snapshot(harness, conversationId);
        const counter = Option.getOrUndefined(yield* session.snapshot(CounterDoc));
        const commits = storage.commits.length;
        assert.include(
          yield* failureMessage(
            session.commitWith((tx) =>
              Effect.gen(function* () {
                const service = yield* Tx;
                (yield* service.doc(LiveDoc, conversationId)).message = "lost";
                (yield* service.doc(CounterDoc)).count = 2;
                // Replacing a missing task fails during assembly, after every change was prepared.
                yield* tx.setTask({
                  id: TaskId.make(999),
                  conversationId,
                  kind: "missing",
                  version: 1,
                  input: null,
                  background: false,
                  abortRequested: false,
                  state: { status: "pending", checkpoint: { phase: "start" } },
                });
              }),
            ),
          ),
          "Task 999 does not exist",
        );
        assert.strictEqual(storage.commits.length, commits);
        assert.strictEqual(yield* snapshot(harness, conversationId), live);
        assert.strictEqual(Option.getOrUndefined(yield* session.snapshot(CounterDoc)), counter);
        yield* session.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            (yield* tx.doc(LiveDoc, conversationId)).message = "next";
            (yield* tx.doc(CounterDoc)).count = 3;
          }),
        );
        assert.strictEqual(Option.getOrUndefined(yield* session.snapshot(CounterDoc))!.count, 3);
      }),
    ),
  );

  it.effect("fails the Session after a Storage failure and publishes nothing", () =>
    run(backend)(
      Effect.gen(function* () {
        const harness = yield* setupLive;
        const { session, storage, publications, conversationId } = harness;
        const before = (yield* snapshot(harness, conversationId))!;
        const published = publications.length;
        storage.failNextCommit("disk vanished");
        const failure = yield* Effect.flip(setMessage(harness, conversationId, "uncertain"));
        assert.strictEqual(failure._tag, "StorageFailure");
        assert.strictEqual(failure.message, "disk vanished");
        assert.strictEqual(publications.length, published);
        assert.isUndefined(before.message);
        const read = yield* Effect.flip(session.snapshot(LiveDoc, conversationId));
        assert.strictEqual(read._tag, "SessionFailed");
        const later = yield* Effect.flip(session.commit(Effect.void));
        assert.strictEqual(later._tag, "SessionFailed");
        yield* session.close;
        const end = yield* session.closed;
        assert.isTrue(end.reason === "failed" && end.error === failure);
      }),
    ),
  );

  it.effect("keeps the previous revision unchanged through Storage settlement", () =>
    run(backend)(
      Effect.gen(function* () {
        const harness = yield* setupLive;
        const { session, storage, conversationId } = harness;
        const before = (yield* snapshot(harness, conversationId))!;
        const copy = structuredClone(before);
        const gate = yield* storage.holdCommits;
        const commit = yield* session
          .commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              const live = yield* tx.doc(LiveDoc, conversationId);
              live.items.push("c");
              live.nested.count = 9;
            }),
          )
          .pipe(Effect.forkChild);
        yield* gate.entered;
        assert.strictEqual(yield* snapshot(harness, conversationId), before);
        assert.deepStrictEqual(before, copy);
        yield* gate.release;
        yield* Fiber.join(commit);
        const after = (yield* snapshot(harness, conversationId))!;
        assert.deepStrictEqual(after.items, ["a", "b", "c"]);
        assert.deepStrictEqual(before, copy);
      }),
    ),
  );

  it.effect("retires documents and creates a new incarnation at the same address", () =>
    run(backend)(
      Effect.gen(function* () {
        const harness = yield* setupLive;
        const { session, storage, publications, conversationId } = harness;
        const oldId = documentChanges(publications.at(-1)!)[0]!.record.id;
        yield* session.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            const live = yield* tx.doc(LiveDoc, conversationId);
            live.message = "final";
            yield* tx.retireDoc(LiveDoc, conversationId);
            const replacement = yield* tx.doc(LiveDoc, conversationId);
            assert.notStrictEqual(replacement, live);
            replacement.message = "new";
          }),
        );
        const writes = storage.commits.at(-1)!;
        assert.lengthOf(writes, 3);
        assert.isTrue(
          writes.some((write) => write.type === "document.change" && write.id === oldId),
        );
        assert.deepInclude(writes, { type: "document.retire", id: oldId });
        assert.isTrue(
          writes.some(
            (write) => write.type === "document.create" && write.record.kind === "test.live",
          ),
        );
        const publication = publications.at(-1)!;
        const [retired, created] = documentChanges(publication);
        assert.strictEqual(retired!.record.id, oldId);
        assert.isNull(retired!.value);
        assert.deepStrictEqual(retired!.ops, []);
        assert.strictEqual(retired!.record.retiredAt, publication.seq);
        assert.notStrictEqual(created!.record.id, oldId);
        assert.strictEqual(created!.record.createdAt, publication.seq);
        assert.deepInclude(created!.value, { message: "new", items: [] });
        assert.deepStrictEqual(created!.ops, []);
        assert.strictEqual(yield* snapshot(harness, conversationId), created!.value);

        yield* session.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            yield* tx.retireDoc(LiveDoc, conversationId);
          }),
        );
        assert.isUndefined(yield* snapshot(harness, conversationId));
        yield* session.unloadDocuments;
        assert.isUndefined(yield* snapshot(harness, conversationId));
        // Retiring an absent address is a no-op.
        const commits = storage.commits.length;
        yield* session.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            yield* tx.retireDoc(LiveDoc, conversationId);
          }),
        );
        assert.strictEqual(storage.commits.length, commits);
      }),
    ),
  );

  it.effect("retires without acquisition and recreates both existing and absent addresses", () =>
    run(backend)(
      Effect.gen(function* () {
        const { session, storage, publications, conversationId } = yield* setupLive;
        const oldId = documentChanges(publications.at(-1)!)[0]!.record.id;
        yield* session.unloadDocuments;
        yield* session.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            const [live] = yield* Effect.all(
              [tx.retireDoc(LiveDoc, conversationId), tx.doc(LiveDoc, conversationId)],
              { concurrency: "unbounded" },
            ).pipe(Effect.map(([, replacement]) => [replacement] as const));
            live.message = "replacement";
          }),
        );
        const existingWrites = storage.commits.at(-1)!;
        assert.lengthOf(existingWrites, 2);
        assert.deepInclude(existingWrites, { type: "document.retire", id: oldId });
        assert.isTrue(
          existingWrites.some(
            (write) => write.type === "document.create" && write.record.kind === "test.live",
          ),
        );

        yield* session.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            const [, member] = yield* Effect.all(
              [tx.retireDoc(MemberDoc, "absent"), tx.doc(MemberDoc, "absent", "seed")],
              { concurrency: "unbounded" },
            );
            member.hits = 1;
          }),
        );
        const absentWrites = storage.commits.at(-1)!;
        assert.lengthOf(
          absentWrites.filter((write) => write.type === "document.retire"),
          0,
        );
        assert.lengthOf(
          absentWrites.filter((write) => write.type === "document.create"),
          1,
        );
        assert.deepStrictEqual(
          Option.getOrUndefined(yield* session.snapshot(MemberDoc, "absent")),
          {
            seed: "seed",
            hits: 1,
          },
        );
      }),
    ),
  );

  it.effect("retires the existing incarnation when retirement races a pending acquisition", () =>
    run(backend)(
      Effect.gen(function* () {
        const harness = yield* setupLive;
        const { session, storage, publications, conversationId } = harness;
        const oldId = documentChanges(publications.at(-1)!)[0]!.record.id;
        yield* session.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            const acquired = yield* Effect.forkChild(tx.doc(LiveDoc, conversationId), {
              startImmediately: true,
            });
            const retired = yield* Effect.forkChild(tx.retireDoc(LiveDoc, conversationId), {
              startImmediately: true,
            });
            (yield* Fiber.join(acquired)).message = "final";
            yield* Fiber.join(retired);
          }),
        );
        const firstWrites = storage.commits.at(-1)!;
        assert.lengthOf(firstWrites, 2);
        assert.isTrue(
          firstWrites.some(
            (write) =>
              write.type === "document.change" &&
              write.id === oldId &&
              write.content.kind === "delta",
          ),
        );
        assert.deepInclude(firstWrites, { type: "document.retire", id: oldId });
        assert.isUndefined(yield* snapshot(harness, conversationId));

        yield* setMessage(harness, conversationId, "second");
        const secondId = documentChanges(publications.at(-1)!)[0]!.record.id;
        yield* session.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            const acquired = yield* Effect.forkChild(tx.doc(LiveDoc, conversationId), {
              startImmediately: true,
            });
            const retired = yield* Effect.forkChild(tx.retireDoc(LiveDoc, conversationId), {
              startImmediately: true,
            });
            const recreated = yield* Effect.forkChild(tx.doc(LiveDoc, conversationId), {
              startImmediately: true,
            });
            const old = yield* Fiber.join(acquired);
            const fresh = yield* Fiber.join(recreated);
            yield* Fiber.join(retired);
            assert.notStrictEqual(fresh, old);
            fresh.message = "third";
          }),
        );
        const writes = storage.commits.at(-1)!;
        assert.lengthOf(writes, 2);
        assert.deepInclude(writes, { type: "document.retire", id: secondId });
        assert.isTrue(
          writes.some(
            (write) => write.type === "document.create" && write.record.kind === "test.live",
          ),
        );
        assert.strictEqual((yield* snapshot(harness, conversationId))!.message, "third");
      }),
    ),
  );

  it.effect("reloads an unloaded document from Storage", () =>
    run(backend)(
      Effect.gen(function* () {
        const harness = yield* setupLive;
        const { session, conversationId } = harness;
        const loaded = (yield* snapshot(harness, conversationId))!;
        yield* session.unloadDocuments;
        const reloaded = (yield* snapshot(harness, conversationId))!;
        assert.notStrictEqual(reloaded, loaded);
        assert.deepStrictEqual(reloaded, loaded);
        yield* session.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            (yield* tx.doc(LiveDoc, conversationId)).items.push("c");
          }),
        );
        yield* session.unloadDocuments;
        assert.deepStrictEqual((yield* snapshot(harness, conversationId))!.items, ["a", "b", "c"]);
      }),
    ),
  );

  it.effect("delivers complete publications synchronously after adoption", () =>
    run(backend)(
      Effect.gen(function* () {
        const harness = yield* setupLive;
        const { session, publications, conversationId } = harness;
        const published = publications.length;
        let delivered = 0;
        const observation = yield* session.observeCommits({
          advance: () => {
            delivered++;
          },
          end: () => {},
        });
        const result = yield* session.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            (yield* tx.doc(LiveDoc, conversationId)).message = "m";
            return "done";
          }),
        );
        assert.strictEqual(result, "done");
        // The listener gets no commit `Context`.
        assert.strictEqual(delivered, 1);
        assert.strictEqual(publications.length, published + 1);
        observation.detach();
      }),
    ),
  );

  it.effect("publishes close synchronously and supports unsubscription", () =>
    run(backend)(
      Effect.gen(function* () {
        const { session } = yield* openTestSession();
        const calls: Array<string> = [];
        yield* session.subscribeClose(() => calls.push("active"));
        const unsubscribe = yield* session.subscribeClose(() => calls.push("removed"));
        unsubscribe();
        yield* session.close;
        assert.deepStrictEqual(calls, ["active"]);
      }),
    ),
  );

  it.effect("settles admitted commits before close and rejects later admission", () =>
    run(backend)(
      Effect.gen(function* () {
        const harness = yield* setupLive;
        const { session, storage, conversationId } = harness;
        const gate = yield* storage.holdCommits;
        const commit = yield* setMessage(harness, conversationId, "admitted").pipe(
          Effect.forkChild,
        );
        yield* gate.entered;
        const queued = yield* setMessage(harness, conversationId, "queued").pipe(
          Effect.forkChild({ startImmediately: true }),
        );
        const admittedSnapshot = yield* session
          .snapshot(MemberDoc, "absent")
          .pipe(Effect.forkChild({ startImmediately: true }));
        const closed = yield* session.close.pipe(Effect.forkChild({ startImmediately: true }));
        assert.strictEqual((yield* Effect.flip(session.commit(Effect.void)))._tag, "SessionClosed");
        assert.include(yield* failureMessage(session.snapshot(LiveDoc, conversationId)), "closed");
        yield* gate.release;
        yield* Fiber.join(commit);
        yield* Fiber.join(queued);
        assert.isTrue(Option.isNone(yield* Fiber.join(admittedSnapshot)));
        yield* Fiber.join(closed);
        assert.deepStrictEqual(yield* session.closed, { reason: "closed" });
        // Storage is not closed by the Session: its own layer closes it, after the Session.
      }),
    ),
  );
});

// The Session takes a commit's durability receipt inside `AgentStorage.commit`, adopts once storage settled,
// and publishes before it fails.
//
// The transaction is not a class and the tests use no module mocking, so each case checks what adoption or
// discarding leaves observable: the publications, the snapshot, and what a reopened Session reads.
//
// "fails the Session with its own adoption error, which never reaches Storage": the error is raised while the
// Session reads the commit's writes, right before it adopts the commit, in the same step and with the same
// outcome as an `adopt` that throws. The storage, once it resolved, replaces the first write of the batch it was
// given (the Session's own array) with a getter that throws.
import { assert, it } from "@effect/vitest";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import { AgentStorage, Backend, StorageFailure } from "../src/AgentStorage.ts";
import * as Doc from "../src/Doc.ts";
import type { ConversationId, Seq } from "../src/Ids.ts";
import type { ObservedCommit } from "../src/internal/session.ts";
import type { CommitOptions, CommitPublication } from "../src/Session.ts";
import { Tx } from "../src/Tx.ts";
import {
  createConversation,
  describeEachBackend,
  documentChanges,
  failureMessage,
  openSession,
  run,
} from "./SessionSupport.ts";

const CounterDoc = Doc.make({
  checkpointWhen: () => false,
  kind: "adoption.counter",
  version: 1,
  scope: "session",
  schema: Schema.Struct({ count: Schema.Number }),
  initial: () => ({ count: 0 }),
});

const bump = Effect.gen(function* () {
  const tx = yield* Tx;
  (yield* tx.doc(CounterDoc)).count++;
});

/** A latest, initial-fork conversation document, the kind a deferred commit may hold. */
const NoteDoc = Doc.make({
  checkpointWhen: () => false,
  kind: "adoption.note",
  version: 1,
  scope: "conversation",
  history: "latest",
  fork: "initial",
  schema: Schema.Struct({
    a: Schema.optionalKey(Schema.Number),
    b: Schema.optionalKey(Schema.Number),
  }),
  initial: () => ({}),
});

const HELD: CommitOptions = {
  persistence: { kind: "deferred", withinMs: Number.POSITIVE_INFINITY },
};

/** Sets one field of conversation `id`'s note. */
const setNote = (id: ConversationId, field: "a" | "b", value: number) =>
  Effect.gen(function* () {
    const tx = yield* Tx;
    (yield* tx.doc(NoteDoc, id))[field] = value;
  });

/** A conversation with its note created. */
const withNote = Effect.gen(function* () {
  const opened = yield* open;
  const conversationId = yield* createConversation(opened.session);
  yield* opened.session.commit(
    Effect.gen(function* () {
      const tx = yield* Tx;
      yield* tx.doc(NoteDoc, conversationId);
    }),
  );
  return { ...opened, conversationId };
});

const reclaimFailed = new StorageFailure({
  reason: new Backend({ message: "reclaim failed", cause: "reclaim failed" }),
});

type Fault = "reject_after_adopt" | "skip_adopt" | "reject_before_adopt" | Error;

/**
 * The storage in context, recording each receipt and its own resolution in `order`; its next commit misbehaves
 * as `fault` says.
 */
const faultyStorage = Effect.gen(function* () {
  const storage = yield* AgentStorage;
  const order: Array<string> = [];
  let fault: Fault | undefined;
  let adopting = false;
  const faulty: AgentStorage["Service"] = {
    ...storage,
    commit: (batches, receive) =>
      Effect.suspend(() => {
        const current = fault;
        fault = undefined;
        if (current === "reject_before_adopt") return Effect.fail(reclaimFailed);
        if (current === "skip_adopt") return storage.commit(batches, () => {});
        return storage
          .commit(batches, (index, seq) => {
            order.push("adopt");
            adopting = true;
            receive(index, seq);
            adopting = false;
            order.push("adopted");
          })
          .pipe(
            Effect.tap(() =>
              Effect.sync(() => {
                order.push("resolve");
                // The Session reads its writes again before adopting: an error of its own there.
                if (current instanceof Error) {
                  Object.defineProperty(batches[0], 0, {
                    get() {
                      throw current;
                    },
                  });
                }
              }),
            ),
            Effect.flatMap((result) =>
              current === "reject_after_adopt"
                ? Effect.fail(reclaimFailed)
                : Effect.succeed(result),
            ),
          );
      }),
  };
  return {
    storage: faulty,
    order,
    fail: (next: Fault) => {
      fault = next;
    },
    get adopting() {
      return adopting;
    },
  };
});

/** A Session over the faulty storage, and a reader of the counter through a fresh Session over it. */
const open = Effect.gen(function* () {
  const faulty = yield* faultyStorage;
  const session = yield* openSession(faulty.storage);
  const reopenedCount = Effect.gen(function* () {
    const reopened = yield* openSession(faulty.storage);
    const value = yield* reopened.snapshot(CounterDoc);
    yield* reopened.close;
    return Option.map(value, (counter) => counter.count);
  });
  return { faulty, session, reopenedCount };
});

describeEachBackend("Session adoption after Storage stored a commit", (backend) => {
  it.effect(
    "takes the durability receipt inside the commit, and adopts once Storage resolved",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const { faulty, session } = yield* open;
          const adopted: Array<Seq> = [];
          yield* session.followCommits((publication) => {
            faulty.order.push("published");
            if (publication.kind === "stored") adopted.push(publication.seq);
          });
          yield* session.commit(bump);
          assert.deepStrictEqual(faulty.order, ["adopt", "adopted", "resolve", "published"]);
          assert.strictEqual(adopted.length, 1);
          assert.deepStrictEqual(yield* session.snapshot(CounterDoc), Option.some({ count: 1 }));
          yield* session.close;
        }),
      ),
  );

  it.effect("fails the Session without discarding when Storage rejects after adoption", () =>
    run(backend)(
      Effect.gen(function* () {
        const { faulty, session, reopenedCount } = yield* open;
        yield* session.commit(bump);
        const published: Array<Seq> = [];
        yield* session.followCommits((publication) => {
          if (publication.kind === "stored") published.push(publication.seq);
        });
        faulty.fail("reject_after_adopt");
        assert.strictEqual(yield* Effect.flip(session.commit(bump)), reclaimFailed);
        // Not discarded: memory adopted the commit and published it.
        assert.strictEqual(published.length, 1);
        assert.deepStrictEqual(yield* session.closed, { reason: "failed", error: reclaimFailed });
        assert.deepStrictEqual(yield* reopenedCount, Option.some(2));
      }),
    ),
  );

  it.effect("discards the staged changes when Storage rejects before adoption", () =>
    run(backend)(
      Effect.gen(function* () {
        const { faulty, session, reopenedCount } = yield* open;
        yield* session.commit(bump);
        const published: Array<Seq> = [];
        yield* session.followCommits((publication) => {
          if (publication.kind === "stored") published.push(publication.seq);
        });
        faulty.fail("reject_before_adopt");
        assert.strictEqual(yield* Effect.flip(session.commit(bump)), reclaimFailed);
        assert.deepStrictEqual(published, []);
        assert.strictEqual((yield* session.closed).reason, "failed");
        assert.deepStrictEqual(yield* reopenedCount, Option.some(1));
      }),
    ),
  );

  it.effect("fails the Session with its own adoption error, which never reaches Storage", () =>
    run(backend)(
      Effect.gen(function* () {
        const { faulty, session } = yield* open;
        yield* session.commit(bump);
        const failure = new Error("adoption failed");
        faulty.fail(failure);
        const exit = yield* Effect.exit(session.commit(bump));
        assert.isTrue(Exit.isFailure(exit) && Cause.squash(exit.cause) === failure);
        // Storage saw a normal adoption and resolved.
        assert.deepStrictEqual(faulty.order.slice(-3), ["adopt", "adopted", "resolve"]);
        assert.deepStrictEqual(yield* session.closed, { reason: "failed", error: failure });
      }),
    ),
  );

  it.effect("fails the Session when Storage resolves without adopting", () =>
    run(backend)(
      Effect.gen(function* () {
        const { faulty, session } = yield* open;
        faulty.fail("skip_adopt");
        assert.include(yield* failureMessage(session.commit(bump)), "without adopting");
        assert.strictEqual((yield* session.closed).reason, "failed");
      }),
    ),
  );
});

describeEachBackend("publication after storage", (backend) => {
  // Both run in the publication's synchronous step, own observers first, and both after storage resolved.
  it.effect("runs its own observers once Storage resolved, and host listeners after them", () =>
    run(backend)(
      Effect.gen(function* () {
        const { faulty, session } = yield* open;
        yield* session.followCommits(() => {
          faulty.order.push(`observed adopting=${faulty.adopting}`);
        });
        yield* session.observeCommits({
          advance: () => {
            faulty.order.push(`host adopting=${faulty.adopting}`);
          },
          end: () => {},
        });
        yield* session.commit(bump);
        assert.deepStrictEqual(faulty.order, [
          "adopt",
          "adopted",
          "resolve",
          "observed adopting=false",
          "host adopting=false",
        ]);
        yield* session.close;
      }),
    ),
  );

  it.effect(
    "calls the host listeners registered when its dispatch runs, and none for a commit published without one",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const { session } = yield* open;
          const calls: Array<string> = [];
          const runSync = Effect.runSyncWith(yield* Effect.context<never>());
          /** Attaches a host listener from inside a publication, where the caller cannot yield. */
          const attach = (
            target: typeof session,
            advance: (publication: CommitPublication) => void,
          ) => runSync(target.observeCommits({ advance, end: () => {} }));
          const removed = yield* session.observeCommits({
            advance: () => {
              calls.push("removed");
            },
            end: () => {},
          });
          yield* session.observeCommits({
            advance: () => {
              calls.push("kept");
            },
            end: () => {},
          });
          // During the publication, after the dispatch was decided.
          let changed = false;
          yield* session.followCommits(() => {
            if (changed) return;
            changed = true;
            removed.detach();
            attach(session, () => {
              calls.push("added");
            });
          });
          yield* session.commit(bump);
          assert.deepStrictEqual(calls, ["kept", "added"]);
          // A Session with no host listener at the publication dispatches to none, so one added during it misses it.
          const bare = yield* open;
          const late: Array<Seq> = [];
          let added = false;
          yield* bare.session.followCommits(() => {
            if (added) return;
            added = true;
            attach(bare.session, (publication) => {
              late.push(publication.seq);
            });
          });
          yield* bare.session.commit(bump);
          assert.deepStrictEqual(late, []);
          yield* bare.session.commit(bump);
          assert.lengthOf(late, 1);
          yield* session.close;
          yield* bare.session.close;
        }),
      ),
  );

  it.effect(
    "fails the Session after Storage returned for a throw of its own observers; the others and hosts got the commit",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const { faulty, session, reopenedCount } = yield* open;
          const bug = new Error("observer bug");
          yield* session.followCommits(() => {
            faulty.order.push("observed");
            throw bug;
          });
          yield* session.followCommits(() => {
            faulty.order.push("observed after");
          });
          yield* session.observeCommits({
            advance: () => {
              faulty.order.push(`host failed=${Option.isSome(session.failure)}`);
            },
            end: () => {},
          });
          yield* session.subscribeClose(() => {
            faulty.order.push("close");
          });
          yield* session.commit(bump);
          // Published once Storage resolved; the Session failed only after the hosts got the commit.
          assert.deepStrictEqual(faulty.order, [
            "adopt",
            "adopted",
            "resolve",
            "observed",
            "observed after",
            "host failed=false",
            "close",
          ]);
          assert.deepStrictEqual(yield* session.closed, { reason: "failed", error: bug });
          assert.deepStrictEqual(yield* reopenedCount, Option.some(1));
        }),
      ),
  );

  it.effect(
    "publishes a commit that Storage rejects after adopting it, then fails the Session",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const { faulty, session, reopenedCount } = yield* open;
          yield* session.commit(bump);
          const stored: Array<Seq> = [];
          yield* session.followCommits(() => {
            faulty.order.push("observed");
          });
          yield* session.observeCommits({
            advance: (publication) => {
              faulty.order.push("host");
              stored.push(publication.seq);
            },
            end: () => {},
          });
          faulty.order.length = 0;
          faulty.fail("reject_after_adopt");
          assert.strictEqual(yield* Effect.flip(session.commit(bump)), reclaimFailed);
          assert.deepStrictEqual(faulty.order, ["adopt", "adopted", "resolve", "observed", "host"]);
          assert.strictEqual(stored.length, 1);
          assert.strictEqual((yield* session.closed).reason, "failed");
          assert.deepStrictEqual(yield* reopenedCount, Option.some(2));
        }),
      ),
  );

  it.effect("keeps what host listeners get: exactly what was stored, held changes included", () =>
    run(backend)(
      Effect.gen(function* () {
        const { faulty, session, conversationId } = yield* withNote;
        const observed: Array<ObservedCommit> = [];
        const stored: Array<CommitPublication> = [];
        yield* session.followCommits((publication) => {
          faulty.order.push("observed");
          observed.push(publication);
        });
        yield* session.observeCommits({
          advance: (publication) => {
            faulty.order.push("host");
            stored.push(publication);
          },
          end: () => {},
        });
        faulty.order.length = 0;
        yield* session.commit(setNote(conversationId, "a", 1), HELD);
        // Held in memory: observed at once, nothing stored.
        assert.deepStrictEqual(faulty.order, ["observed"]);
        yield* session.commit(setNote(conversationId, "b", 2));
        assert.deepStrictEqual(faulty.order, [
          "observed",
          "adopt",
          "adopted",
          "resolve",
          "observed",
          "host",
        ]);
        const own = observed[1]!;
        assert.deepStrictEqual(
          own.changes.map((change) => change.type === "document" && change.ops),
          [[["s", ["b"], 2]]],
        );
        assert.lengthOf(stored, 1);
        assert.deepStrictEqual(
          documentChanges(stored[0]!).map((change) => change.ops),
          [
            [
              ["s", ["a"], 1],
              ["s", ["b"], 2],
            ],
          ],
        );
        assert.strictEqual(own.kind === "stored" ? own.seq : undefined, stored[0]!.seq);
        yield* session.close;
      }),
    ),
  );

  it.effect("publishes a store of held changes to host listeners once Storage resolved", () =>
    run(backend)(
      Effect.gen(function* () {
        const { faulty, session, conversationId } = yield* withNote;
        yield* session.observeCommits({
          advance: () => {
            faulty.order.push(`host adopting=${faulty.adopting}`);
          },
          end: () => {},
        });
        yield* session.commit(setNote(conversationId, "a", 1), HELD);
        faulty.order.length = 0;
        yield* session.unloadDocuments;
        assert.deepStrictEqual(faulty.order, [
          "adopt",
          "adopted",
          "resolve",
          "host adopting=false",
        ]);
        yield* session.close;
      }),
    ),
  );

  it.effect(
    "publishes a store of held changes that Storage rejects after adopting it, then fails the Session",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const { faulty, session, conversationId } = yield* withNote;
          const stored: Array<ReadonlyArray<unknown>> = [];
          yield* session.observeCommits({
            advance: (publication) => {
              stored.push(documentChanges(publication).map((change) => change.ops));
            },
            end: () => {},
          });
          yield* session.commit(setNote(conversationId, "a", 1), HELD);
          faulty.order.length = 0;
          faulty.fail("reject_after_adopt");
          assert.strictEqual(yield* Effect.flip(session.unloadDocuments), reclaimFailed);
          assert.deepStrictEqual(faulty.order, ["adopt", "adopted", "resolve"]);
          assert.deepStrictEqual(stored, [[[["s", ["a"], 1]]]]);
          const end = yield* session.closed;
          assert.deepStrictEqual(end, { reason: "failed", error: reclaimFailed });
        }),
      ),
  );
});

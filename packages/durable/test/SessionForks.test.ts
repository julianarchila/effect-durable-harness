// Conversation forks, on every backend: kernel cases over `tx.forkConversation`.
//
// - Documents carry a `Schema`; values entering from a copy are checked against it.
// - A rejected commit is an Effect failure: its rendered cause holds the message (`failureMessage`).
// - After a failed Session the backend is read directly: the Session does not close storage.
// - "copies every family member across storage scan pages" creates its 260 members one after the other.
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import type { StorageWrite } from "../src/AgentStorage.ts";
import * as Doc from "../src/Doc.ts";
import type { ConversationId, EntryId } from "../src/Ids.ts";
import type { CommitPublication, DocumentCommitChange } from "../src/Session.ts";
import * as Task from "../src/Task.ts";
import { Tx } from "../src/Tx.ts";
import {
  createConversation,
  describeEachBackend,
  documentChanges,
  failureMessage,
  flush,
  openTestSession,
  run,
} from "./SessionSupport.ts";

const documentCreates = (writes: ReadonlyArray<StorageWrite>) =>
  writes.filter((write) => write.type === "document.create");

const documentCopies = (writes: ReadonlyArray<StorageWrite>) =>
  writes.filter(
    (write): write is Extract<StorageWrite, { readonly type: "document.copy" }> =>
      write.type === "document.copy",
  );

const documentCopyChanges = (publication: CommitPublication) =>
  publication.changes.filter(
    (change): change is Extract<DocumentCommitChange, { readonly type: "document.copy" }> =>
      change.type === "document.copy",
  );

const Value = Schema.Struct({ value: Schema.String });

const ownerless = { ownership: { kind: "ownerless" } } as const;

const fork = (parentId: ConversationId, at: EntryId) =>
  Effect.gen(function* () {
    const tx = yield* Tx;
    return yield* tx.forkConversation(parentId, at, ownerless);
  });

const some = <A>(value: Option.Option<A>) => Option.getOrUndefined(value);

describeEachBackend("Session conversation document forks", (backend) => {
  it.effect(
    "copies as-of and current singleton and family bases while leaving initial documents absent",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const AsOf = Doc.make({
            checkpointWhen: () => false,
            kind: "fork.policies.as-of",
            version: 1,
            scope: "conversation",
            history: "rewindable",
            fork: "asOf",
            schema: Value,
            initial: () => ({ value: "as-initial" }),
          });
          const Current = Doc.make({
            checkpointWhen: () => false,
            kind: "fork.policies.current",
            version: 1,
            scope: "conversation",
            history: "latest",
            fork: "current",
            schema: Value,
            initial: () => ({ value: "current-initial" }),
          });
          const Initial = Doc.make({
            checkpointWhen: () => false,
            kind: "fork.policies.initial",
            version: 1,
            scope: "conversation",
            history: "latest",
            fork: "initial",
            schema: Value,
            initial: () => ({ value: "fresh" }),
          });
          const AsOfFamily = Doc.family({
            checkpointWhen: () => false,
            kind: "fork.policies.as-of-family",
            version: 1,
            family: true,
            scope: "conversation",
            history: "rewindable",
            fork: "asOf",
            schema: Value,
            initial: (seed: string) => ({ value: seed }),
          });
          const CurrentFamily = Doc.family({
            checkpointWhen: () => false,
            kind: "fork.policies.current-family",
            version: 1,
            family: true,
            scope: "conversation",
            history: "latest",
            fork: "current",
            schema: Value,
            initial: (seed: string) => ({ value: seed }),
          });
          const { session, storage, publications } = yield* openTestSession();
          const parentId = yield* createConversation(session);
          const forkAt = yield* session.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              const at = (yield* tx.appendEntry(parentId, { kind: "fork-point" })).id;
              (yield* tx.doc(AsOf, parentId)).value = "as-at-fork";
              (yield* tx.doc(Current, parentId)).value = "current-at-fork";
              (yield* tx.doc(Initial, parentId)).value = "parent-only";
              (yield* tx.doc(AsOfFamily, parentId, "a", "unused")).value = "family-as-a";
              (yield* tx.doc(AsOfFamily, parentId, "b", "unused")).value = "family-as-b";
              (yield* tx.doc(CurrentFamily, parentId, "a", "unused")).value = "family-current-a";
              return at;
            }),
          );
          yield* session.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              (yield* tx.doc(AsOf, parentId)).value = "as-after-fork";
              (yield* tx.doc(Current, parentId)).value = "current-when-copied";
              (yield* tx.doc(AsOfFamily, parentId, "a", "unused")).value = "family-as-after";
              (yield* tx.doc(CurrentFamily, parentId, "a", "unused")).value =
                "family-current-when-copied";
            }),
          );
          yield* flush;
          const documentReads = storage.documentReadCount;

          const child = yield* session.commit(fork(parentId, forkAt));
          yield* flush;
          assert.strictEqual(storage.documentReadCount, documentReads);

          assert.deepStrictEqual(some(yield* session.snapshot(AsOf, child.id)), {
            value: "as-at-fork",
          });
          assert.deepStrictEqual(some(yield* session.snapshot(Current, child.id)), {
            value: "current-when-copied",
          });
          assert.isUndefined(some(yield* session.snapshot(Initial, child.id)));
          assert.deepStrictEqual(some(yield* session.snapshot(AsOfFamily, child.id, "a")), {
            value: "family-as-a",
          });
          assert.deepStrictEqual(some(yield* session.snapshot(AsOfFamily, child.id, "b")), {
            value: "family-as-b",
          });
          assert.deepStrictEqual(some(yield* session.snapshot(CurrentFamily, child.id, "a")), {
            value: "family-current-when-copied",
          });

          const admitted = storage.admittedCommits.at(-1)!;
          const copies = documentCopies(admitted);
          assert.lengthOf(copies, 5);
          assert.isTrue(copies.every((write) => write.record.scope.kind === "conversation"));
          assert.isTrue(
            copies.every(
              (write) =>
                write.record.scope.kind === "conversation" &&
                write.record.scope.conversationId === child.id,
            ),
          );
          const publication = publications.at(-1)!;
          const copied = documentCopyChanges(publication);
          assert.lengthOf(copied, 5);
          for (const change of copied) {
            assert.strictEqual(change.record.createdAt, publication.seq);
            assert.strictEqual(change.conversationId, child.id);
            const write = copies.find((candidate) => candidate.record.id === change.record.id)!;
            assert.deepStrictEqual(change.source, write.source);
          }

          const parentRecord = yield* storage.findDocument(
            {
              kind: AsOf.definition.kind,
              scope: { kind: "conversation", conversationId: parentId },
            },
            "current",
          );
          const childRecord = yield* storage.findDocument(
            {
              kind: AsOf.definition.kind,
              scope: { kind: "conversation", conversationId: child.id },
            },
            "current",
          );
          assert.notStrictEqual(
            Option.getOrThrow(childRecord).id,
            Option.getOrThrow(parentRecord).id,
          );

          yield* session.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              (yield* tx.doc(AsOf, child.id)).value = "child-independent";
            }),
          );
          assert.deepStrictEqual(some(yield* session.snapshot(AsOf, parentId)), {
            value: "as-after-fork",
          });
          assert.deepStrictEqual(some(yield* session.snapshot(AsOf, child.id)), {
            value: "child-independent",
          });
          yield* session.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              (yield* tx.doc(Initial, child.id)).value = "child-created";
            }),
          );
          assert.deepStrictEqual(some(yield* session.snapshot(Initial, child.id)), {
            value: "child-created",
          });
        }),
      ),
  );

  it.effect(
    "uses final document state from the fork entry commit while excluding later same-commit entries",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const SameCommit = Doc.make({
            checkpointWhen: () => false,
            kind: "fork.same-commit",
            version: 1,
            scope: "conversation",
            history: "rewindable",
            fork: "asOf",
            schema: Value,
            initial: () => ({ value: "initial" }),
          });
          const { session } = yield* openTestSession();
          const parentId = yield* createConversation(session);
          const { forkAt, excluded } = yield* session.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              const included = (yield* tx.appendEntry(parentId, { kind: "included" })).id;
              (yield* tx.doc(SameCommit, parentId)).value = "final-state-of-commit";
              const after = (yield* tx.appendEntry(parentId, { kind: "excluded" })).id;
              return { forkAt: included, excluded: after };
            }),
          );
          const child = yield* session.commit(fork(parentId, forkAt));
          assert.deepStrictEqual(some(yield* session.snapshot(SameCommit, child.id)), {
            value: "final-state-of-commit",
          });
          const visible = yield* session.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              return yield* tx.scanEntries({ conversationId: child.id }, 10);
            }),
          );
          const ids = visible.items.map((entry) => entry.id);
          assert.include(ids, forkAt);
          assert.notInclude(ids, excluded);
        }),
      ),
  );

  it.effect(
    "selects the entry-owning ancestor for as-of copies and the immediate parent for current copies",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const AsOf = Doc.make({
            checkpointWhen: () => false,
            kind: "fork.ancestry.as-of",
            version: 1,
            scope: "conversation",
            history: "rewindable",
            fork: "asOf",
            schema: Value,
            initial: () => ({ value: "initial" }),
          });
          const Current = Doc.make({
            checkpointWhen: () => false,
            kind: "fork.ancestry.current",
            version: 1,
            scope: "conversation",
            history: "latest",
            fork: "current",
            schema: Value,
            initial: () => ({ value: "initial" }),
          });
          const { session } = yield* openTestSession();
          const rootId = yield* createConversation(session);
          const inherited = yield* session.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              const at = (yield* tx.appendEntry(rootId, { kind: "root" })).id;
              (yield* tx.doc(AsOf, rootId)).value = "root-at-entry";
              (yield* tx.doc(Current, rootId)).value = "root-current";
              return at;
            }),
          );
          const parent = yield* session.commit(fork(rootId, inherited));
          const parentEntry = yield* session.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              const at = (yield* tx.appendEntry(parent.id, { kind: "parent" })).id;
              (yield* tx.doc(AsOf, parent.id)).value = "parent-at-own-entry";
              (yield* tx.doc(Current, parent.id)).value = "parent-current";
              return at;
            }),
          );

          const inheritedFork = yield* session.commit(fork(parent.id, inherited));
          assert.deepStrictEqual(some(yield* session.snapshot(AsOf, inheritedFork.id)), {
            value: "root-at-entry",
          });
          assert.deepStrictEqual(some(yield* session.snapshot(Current, inheritedFork.id)), {
            value: "parent-current",
          });

          const ownEntryFork = yield* session.commit(fork(parent.id, parentEntry));
          assert.deepStrictEqual(some(yield* session.snapshot(AsOf, ownEntryFork.id)), {
            value: "parent-at-own-entry",
          });
          assert.deepStrictEqual(some(yield* session.snapshot(Current, ownEntryFork.id)), {
            value: "parent-current",
          });
        }),
      ),
  );

  it.effect(
    "copies the stored value and version without consulting migration definitions or migrated caches",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const V1 = Doc.make({
            checkpointWhen: () => false,
            kind: "fork.stored-version",
            version: 1,
            scope: "conversation",
            history: "rewindable",
            fork: "asOf",
            schema: Schema.Struct({ count: Schema.Number }),
            initial: () => ({ count: 1 }),
          });
          let migrations = 0;
          const V3 = Doc.make({
            checkpointWhen: () => false,
            kind: "fork.stored-version",
            version: 3,
            scope: "conversation",
            history: "rewindable",
            fork: "asOf",
            schema: Schema.Struct({ count: Schema.Number, migrated: Schema.Boolean }),
            initial: () => ({ count: 0, migrated: false }),
            migrate: (value, fromVersion) => {
              assert.strictEqual(fromVersion, 1);
              migrations++;
              return { count: Number(value["count"]), migrated: true };
            },
          });
          const { session, storage } = yield* openTestSession();
          const parentId = yield* createConversation(session);
          const forkAt = yield* session.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              const at = (yield* tx.appendEntry(parentId, { kind: "point" })).id;
              yield* tx.doc(V1, parentId);
              return at;
            }),
          );
          yield* session.unloadDocuments;
          assert.deepStrictEqual(some(yield* session.snapshot(V3, parentId)), {
            count: 1,
            migrated: true,
          });
          assert.strictEqual(migrations, 1);

          const child = yield* session.commit(fork(parentId, forkAt));
          assert.strictEqual(migrations, 1);
          const childRecord = Option.getOrThrow(
            yield* storage.findDocument(
              {
                kind: V1.definition.kind,
                scope: { kind: "conversation", conversationId: child.id },
              },
              "current",
            ),
          );
          const childStored = Option.getOrThrow(yield* storage.document(childRecord.id, "current"));
          assert.strictEqual(childStored.version, 1);
          assert.deepStrictEqual(childStored.value, { count: 1 });
          assert.deepStrictEqual(some(yield* session.snapshot(V3, child.id)), {
            count: 1,
            migrated: true,
          });
          assert.strictEqual(migrations, 2);
        }),
      ),
  );

  it.effect("coalesces a typed migration and override into the copied creation base", () =>
    run(backend)(
      Effect.gen(function* () {
        const V1 = Doc.make({
          checkpointWhen: () => false,
          kind: "fork.override",
          version: 1,
          scope: "conversation",
          history: "rewindable",
          fork: "asOf",
          schema: Schema.Struct({ count: Schema.Number }),
          initial: () => ({ count: 2 }),
        });
        let checkpoints = 0;
        const V2 = Doc.make({
          kind: "fork.override",
          version: 2,
          scope: "conversation",
          history: "rewindable",
          fork: "asOf",
          schema: Schema.Struct({ count: Schema.Number, migrated: Schema.Boolean }),
          initial: () => ({ count: 0, migrated: false }),
          migrate: (value) => ({ count: Number(value["count"]), migrated: true }),
          checkpointWhen: () => {
            checkpoints++;
            return false;
          },
        });
        const { session, storage, publications } = yield* openTestSession();
        const parentId = yield* createConversation(session);
        const forkAt = yield* session.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            const at = (yield* tx.appendEntry(parentId, { kind: "point" })).id;
            yield* tx.doc(V1, parentId);
            return at;
          }),
        );

        const documentReads = storage.documentReadCount;
        const child = yield* session.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            const created = yield* tx.forkConversation(parentId, forkAt, ownerless);
            const value = yield* tx.doc(V2, created.id);
            value.count = 9;
            return created;
          }),
        );
        yield* flush;
        assert.strictEqual(storage.documentReadCount, documentReads + 1);
        const creates = documentCreates(storage.admittedCommits.at(-1)!);
        assert.lengthOf(creates, 1);
        assert.deepInclude(creates[0]!.content, {
          kind: "base",
          version: 2,
          value: { count: 9, migrated: true },
        });
        assert.strictEqual(checkpoints, 0);
        const changes = documentChanges(publications.at(-1)!);
        assert.lengthOf(changes, 1);
        assert.strictEqual(changes[0]!.version, 2);
        assert.deepStrictEqual(some(yield* session.snapshot(V2, child.id)), {
          count: 9,
          migrated: true,
        });
        const parentRecord = Option.getOrThrow(
          yield* storage.findDocument(
            {
              kind: V1.definition.kind,
              scope: { kind: "conversation", conversationId: parentId },
            },
            "current",
          ),
        );
        assert.strictEqual(
          Option.getOrThrow(yield* storage.document(parentRecord.id, "current")).version,
          1,
        );
      }),
    ),
  );

  it.effect("copies the incarnation alive at the fork point across retirement and recreation", () =>
    run(backend)(
      Effect.gen(function* () {
        const Incarnations = Doc.make({
          checkpointWhen: () => false,
          kind: "fork.incarnations",
          version: 1,
          scope: "conversation",
          history: "rewindable",
          fork: "asOf",
          schema: Value,
          initial: () => ({ value: "initial" }),
        });
        const { session } = yield* openTestSession();
        const parentId = yield* createConversation(session);
        const oldAt = yield* session.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            const at = (yield* tx.appendEntry(parentId, { kind: "old" })).id;
            (yield* tx.doc(Incarnations, parentId)).value = "old";
            return at;
          }),
        );
        const retiredAt = yield* session.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            const at = (yield* tx.appendEntry(parentId, { kind: "retired" })).id;
            yield* tx.retireDoc(Incarnations, parentId);
            return at;
          }),
        );
        const newAt = yield* session.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            const at = (yield* tx.appendEntry(parentId, { kind: "new" })).id;
            (yield* tx.doc(Incarnations, parentId)).value = "new";
            return at;
          }),
        );

        const oldChild = yield* session.commit(fork(parentId, oldAt));
        const emptyChild = yield* session.commit(fork(parentId, retiredAt));
        const newChild = yield* session.commit(fork(parentId, newAt));
        assert.deepStrictEqual(some(yield* session.snapshot(Incarnations, oldChild.id)), {
          value: "old",
        });
        assert.isUndefined(some(yield* session.snapshot(Incarnations, emptyChild.id)));
        assert.deepStrictEqual(some(yield* session.snapshot(Incarnations, newChild.id)), {
          value: "new",
        });
      }),
    ),
  );

  it.effect("rejects invisible fork points before admission and remains usable", () =>
    run(backend)(
      Effect.gen(function* () {
        const { session, storage, publications } = yield* openTestSession();
        const rootId = yield* createConversation(session);
        const { visible, hidden } = yield* session.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            return {
              visible: (yield* tx.appendEntry(rootId, { kind: "visible" })).id,
              hidden: (yield* tx.appendEntry(rootId, { kind: "hidden" })).id,
            };
          }),
        );
        const parent = yield* session.commit(fork(rootId, visible));
        yield* flush;
        const commits = storage.commits.length;
        const published = publications.length;
        assert.include(
          yield* failureMessage(session.commit(fork(parent.id, hidden))),
          `Entry ${hidden} is not visible`,
        );
        yield* flush;
        assert.lengthOf(storage.commits, commits);
        assert.lengthOf(publications, published);
        const independent = yield* session.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            return yield* tx.createConversation(ownerless);
          }),
        );
        assert.deepStrictEqual(some(yield* storage.conversation(independent.id)), independent);
      }),
    ),
  );

  it.effect(
    "rejects duplicate as-of and current selections for one child address before admission",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const AsOf = Doc.make({
            checkpointWhen: () => false,
            kind: "fork.duplicate-policy",
            version: 1,
            scope: "conversation",
            history: "rewindable",
            fork: "asOf",
            schema: Value,
            initial: () => ({ value: "as-of" }),
          });
          const Current = Doc.make({
            checkpointWhen: () => false,
            kind: "fork.duplicate-policy",
            version: 1,
            scope: "conversation",
            history: "latest",
            fork: "current",
            schema: Value,
            initial: () => ({ value: "current" }),
          });
          const { session, storage } = yield* openTestSession();
          const parentId = yield* createConversation(session);
          const oldAt = yield* session.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              const at = (yield* tx.appendEntry(parentId, { kind: "old" })).id;
              yield* tx.doc(AsOf, parentId);
              return at;
            }),
          );
          yield* session.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              yield* tx.retireDoc(AsOf, parentId);
            }),
          );
          yield* session.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              yield* tx.doc(Current, parentId);
            }),
          );
          const commits = storage.commits.length;
          assert.include(
            yield* failureMessage(session.commit(fork(parentId, oldAt))),
            "Fork selects multiple source documents",
          );
          assert.lengthOf(storage.commits, commits);
          const independent = yield* session.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              return yield* tx.createConversation(ownerless);
            }),
          );
          assert.deepStrictEqual(some(yield* storage.conversation(independent.id)), independent);
        }),
      ),
  );

  it.effect("rejects current and as-of source writes in the fork transaction", () =>
    run(backend)(
      Effect.gen(function* () {
        const Current = Doc.make({
          checkpointWhen: () => false,
          kind: "fork.same-transaction-current",
          version: 1,
          scope: "conversation",
          history: "latest",
          fork: "current",
          schema: Value,
          initial: () => ({ value: "committed" }),
        });
        const AsOf = Doc.make({
          checkpointWhen: () => false,
          kind: "fork.same-transaction-as-of",
          version: 1,
          scope: "conversation",
          history: "rewindable",
          fork: "asOf",
          schema: Value,
          initial: () => ({ value: "committed" }),
        });
        const { session, storage } = yield* openTestSession();
        const parentId = yield* createConversation(session);
        const forkAt = yield* session.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            const at = (yield* tx.appendEntry(parentId, { kind: "point" })).id;
            yield* tx.doc(Current, parentId);
            yield* tx.doc(AsOf, parentId);
            return at;
          }),
        );
        const commits = storage.commits.length;
        assert.include(
          yield* failureMessage(
            session.commit(
              Effect.gen(function* () {
                const tx = yield* Tx;
                (yield* tx.doc(Current, parentId)).value = "before-fork";
                yield* tx.forkConversation(parentId, forkAt, ownerless);
              }),
            ),
          ),
          "Cannot change fork source document",
        );
        assert.include(
          yield* failureMessage(
            session.commit(
              Effect.gen(function* () {
                const tx = yield* Tx;
                yield* tx.forkConversation(parentId, forkAt, ownerless);
                (yield* tx.doc(Current, parentId)).value = "after-fork";
              }),
            ),
          ),
          "Cannot change fork source document",
        );
        assert.include(
          yield* failureMessage(
            session.commit(
              Effect.gen(function* () {
                const tx = yield* Tx;
                (yield* tx.doc(AsOf, parentId)).value = "as-of-write";
                yield* tx.forkConversation(parentId, forkAt, ownerless);
              }),
            ),
          ),
          "Cannot change fork source document",
        );
        assert.lengthOf(storage.commits, commits);
        assert.deepStrictEqual(some(yield* session.snapshot(Current, parentId)), {
          value: "committed",
        });
        const child = yield* session.commit(fork(parentId, forkAt));
        assert.deepStrictEqual(some(yield* session.snapshot(Current, child.id)), {
          value: "committed",
        });
      }),
    ),
  );

  it.effect("rolls every copied base back when later pre-admission assembly fails", () =>
    run(backend)(
      Effect.gen(function* () {
        const Copied = Doc.make({
          checkpointWhen: () => false,
          kind: "fork.rollback.copied",
          version: 1,
          scope: "conversation",
          history: "rewindable",
          fork: "asOf",
          schema: Value,
          initial: () => ({ value: "copied" }),
        });
        let failCheckpoint = true;
        const Failure = Doc.make({
          kind: "fork.rollback.failure",
          version: 1,
          scope: "session",
          schema: Schema.Struct({ count: Schema.Number }),
          initial: () => ({ count: 0 }),
          checkpointWhen: () => {
            if (failCheckpoint) throw new Error("checkpoint failed");
            return false;
          },
        });
        const { session, storage, publications } = yield* openTestSession();
        const parentId = yield* createConversation(session);
        const forkAt = yield* session.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            const at = (yield* tx.appendEntry(parentId, { kind: "point" })).id;
            yield* tx.doc(Copied, parentId);
            yield* tx.doc(Failure);
            return at;
          }),
        );
        yield* flush;
        const commits = storage.commits.length;
        const published = publications.length;
        let childId: ConversationId | undefined;
        assert.include(
          yield* failureMessage(
            session.commit(
              Effect.gen(function* () {
                const tx = yield* Tx;
                childId = (yield* tx.forkConversation(parentId, forkAt, ownerless)).id;
                (yield* tx.doc(Failure)).count = 1;
              }),
            ),
          ),
          "checkpoint failed",
        );
        yield* flush;
        assert.lengthOf(storage.commits, commits);
        assert.lengthOf(publications, published);
        assert.isTrue(Option.isNone(yield* storage.conversation(childId!)));
        failCheckpoint = false;
        yield* session.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            (yield* tx.doc(Failure)).count = 2;
          }),
        );
        assert.deepStrictEqual(some(yield* session.snapshot(Failure)), { count: 2 });
        assert.deepStrictEqual(some(yield* session.snapshot(Copied, parentId)), {
          value: "copied",
        });
      }),
    ),
  );

  it.effect("fails the Session on a rejected fork commit, which leaves no effect", () =>
    run(backend)(
      Effect.gen(function* () {
        const Rejected = Doc.make({
          checkpointWhen: () => false,
          kind: "fork.storage-rejected",
          version: 1,
          scope: "conversation",
          history: "rewindable",
          fork: "asOf",
          schema: Value,
          initial: () => ({ value: "source" }),
        });
        const { session, storage } = yield* openTestSession();
        const parentId = yield* createConversation(session);
        const forkAt = yield* session.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            const at = (yield* tx.appendEntry(parentId, { kind: "point" })).id;
            yield* tx.doc(Rejected, parentId);
            return at;
          }),
        );
        let rejectedChildId: ConversationId | undefined;
        storage.failNextCommit("copy rejected");
        const rejection = yield* Effect.flip(
          session.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              rejectedChildId = (yield* tx.forkConversation(parentId, forkAt, ownerless)).id;
            }),
          ),
        );
        assert.strictEqual(rejection._tag, "StorageFailure");
        assert.strictEqual(rejection.message, "copy rejected");
        const failed = yield* Effect.flip(
          session.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              yield* tx.createConversation(ownerless);
            }),
          ),
        );
        assert.strictEqual(failed._tag, "SessionFailed");
        assert.strictEqual(failed._tag === "SessionFailed" ? failed.cause : undefined, rejection);
        // The failed Session leaves its storage open; it holds nothing of the batch.
        const end = yield* session.closed;
        assert.isTrue(end.reason === "failed" && end.error === rejection);
        assert.isTrue(Option.isNone(yield* storage.conversation(rejectedChildId!)));
      }),
    ),
  );

  it.effect("retires a copied document and can recreate the address in the fork transaction", () =>
    run(backend)(
      Effect.gen(function* () {
        const Recreated = Doc.make({
          checkpointWhen: () => false,
          kind: "fork.retire-recreate",
          version: 1,
          scope: "conversation",
          history: "rewindable",
          fork: "asOf",
          schema: Value,
          initial: () => ({ value: "fresh" }),
        });
        const { session, storage } = yield* openTestSession();
        const parentId = yield* createConversation(session);
        const forkAt = yield* session.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            const at = (yield* tx.appendEntry(parentId, { kind: "point" })).id;
            (yield* tx.doc(Recreated, parentId)).value = "copied";
            return at;
          }),
        );
        const child = yield* session.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            const created = yield* tx.forkConversation(parentId, forkAt, ownerless);
            yield* tx.retireDoc(Recreated, created.id);
            (yield* tx.doc(Recreated, created.id)).value = "replacement";
            return created;
          }),
        );
        const writes = storage.commits.at(-1)!;
        assert.lengthOf(
          writes.filter((write) => write.type === "document.copy"),
          1,
        );
        assert.lengthOf(
          writes.filter((write) => write.type === "document.create"),
          1,
        );
        assert.lengthOf(
          writes.filter((write) => write.type === "document.retire"),
          1,
        );
        assert.deepStrictEqual(some(yield* session.snapshot(Recreated, child.id)), {
          value: "replacement",
        });
      }),
    ),
  );

  it.effect(
    "copies only conversation documents, leaving Session and task documents in their original scopes",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const Copied = Doc.make({
            checkpointWhen: () => false,
            kind: "fork.scope.conversation",
            version: 1,
            scope: "conversation",
            history: "latest",
            fork: "current",
            schema: Value,
            initial: () => ({ value: "conversation" }),
          });
          const SessionOnly = Doc.make({
            checkpointWhen: () => false,
            kind: "fork.scope.session",
            version: 1,
            scope: "session",
            schema: Value,
            initial: () => ({ value: "session" }),
          });
          const TaskOnly = Doc.make({
            checkpointWhen: () => false,
            kind: "fork.scope.task",
            version: 1,
            scope: "task",
            schema: Value,
            initial: () => ({ value: "task" }),
          });
          const Work = Task.make({
            name: "fork.scope.work",
            version: 1,
            input: Schema.Null,
            checkpoint: Schema.Struct({ phase: Schema.Literal("start") }),
            result: Schema.Null,
            initial: () => ({ phase: "start" }),
            phases: { start: () => Effect.void },
            abort: () => Effect.void,
          });
          const { session, storage, publications } = yield* openTestSession();
          const parentId = yield* createConversation(session);
          const { forkAt, taskId } = yield* session.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              const at = (yield* tx.appendEntry(parentId, { kind: "point" })).id;
              const task = yield* tx.createTask(Work, null, {
                ownership: { kind: "conversation" },
                conversationId: parentId,
              });
              yield* tx.doc(Copied, parentId);
              yield* tx.doc(SessionOnly);
              yield* tx.doc(TaskOnly, task);
              return { forkAt: at, taskId: task };
            }),
          );
          const child = yield* session.commit(fork(parentId, forkAt));
          yield* flush;
          const copies = documentCopies(storage.commits.at(-1)!);
          assert.deepStrictEqual(
            copies.map((write) => write.record.kind),
            [Copied.definition.kind],
          );
          assert.deepStrictEqual(
            documentCopyChanges(publications.at(-1)!).map((change) => change.record.kind),
            [Copied.definition.kind],
          );
          assert.deepStrictEqual(some(yield* session.snapshot(Copied, child.id)), {
            value: "conversation",
          });
          assert.deepStrictEqual(some(yield* session.snapshot(SessionOnly)), { value: "session" });
          assert.deepStrictEqual(some(yield* session.snapshot(TaskOnly, taskId)), {
            value: "task",
          });
        }),
      ),
  );

  it.effect("copies every family member across storage scan pages", () =>
    run(backend)(
      Effect.gen(function* () {
        const Family = Doc.family({
          checkpointWhen: () => false,
          kind: "fork.pagination",
          version: 1,
          family: true,
          scope: "conversation",
          history: "latest",
          fork: "current",
          schema: Schema.Struct({ value: Schema.Number }),
          initial: (seed: number) => ({ value: seed }),
        });
        const { session, storage } = yield* openTestSession();
        const parentId = yield* createConversation(session);
        const forkAt = yield* session.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            return (yield* tx.appendEntry(parentId, { kind: "point" })).id;
          }),
        );
        yield* session.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            for (let index = 0; index < 260; index++) {
              yield* tx.doc(Family, parentId, `member-${index}`, index);
            }
          }),
        );
        const child = yield* session.commit(fork(parentId, forkAt));
        assert.lengthOf(documentCopies(storage.commits.at(-1)!), 260);
        assert.deepStrictEqual(some(yield* session.snapshot(Family, child.id, "member-0")), {
          value: 0,
        });
        assert.deepStrictEqual(some(yield* session.snapshot(Family, child.id, "member-259")), {
          value: 259,
        });
      }),
    ),
  );
});

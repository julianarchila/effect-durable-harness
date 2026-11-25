import * as SqliteClient from "@effect/sql-sqlite-node/SqliteClient";
import { assert, describe, it } from "@effect/vitest";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Scope from "effect/Scope";
import {
  AgentStorage,
  type StorageFailure,
  type StorageRequestError,
} from "../src/AgentStorage.ts";
import { DocumentId, EntryId, ROOT_CONVERSATION_ID } from "../src/Ids.ts";
import * as SqlAgentStorage from "../src/SqlAgentStorage.ts";
import { commitWrites } from "../src/testing/CommitWrites.ts";

/** `_tag` of a storage error, and the `reason._tag` of a `StorageFailure`. */
const errorTags = (error: StorageFailure | StorageRequestError) =>
  error._tag === "StorageFailure" ? [error._tag, error.reason._tag] : [error._tag];

// Settlement of SQL storage operations. Driver-level concerns (prepared
// statements, transaction handles, rollback errors, operation queueing) belong
// to `SqlClient` and are not tested here.

/** A fresh storage over a fresh `:memory:` database; `close` closes the storage's scope. */
const open = Effect.gen(function* () {
  const scope = yield* Scope.make();
  yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void));
  const context = yield* Layer.buildWithScope(
    SqlAgentStorage.layer().pipe(
      Layer.provide(Layer.orDie(SqliteClient.layer({ filename: ":memory:" }))),
    ),
    scope,
  );
  return { storage: Context.get(context, AgentStorage), close: Scope.close(scope, Exit.void) };
});

describe("SqlAgentStorage settlement", () => {
  it.effect("runs a read issued after a commit against the committed state", () =>
    Effect.gen(function* () {
      const { storage } = yield* open;
      const commit = yield* Effect.forkChild(
        commitWrites(storage, [{ type: "conversation", value: { id: ROOT_CONVERSATION_ID } }]),
      );
      const read = yield* Effect.forkChild(storage.conversation(ROOT_CONVERSATION_ID));
      yield* Fiber.join(commit);
      assert.deepStrictEqual(yield* Fiber.join(read), Option.some({ id: ROOT_CONVERSATION_ID }));
    }).pipe(Effect.scoped),
  );

  it.effect("lets admitted multi-query reads finish before storage closes", () =>
    Effect.gen(function* () {
      const { storage, close } = yield* open;
      const entryId = EntryId.make(2);
      yield* commitWrites(storage, [
        { type: "conversation", value: { id: ROOT_CONVERSATION_ID } },
        {
          type: "entry",
          value: { id: entryId, conversationId: ROOT_CONVERSATION_ID, kind: "probe" },
        },
      ]);
      const scan = yield* Effect.forkChild(
        storage.scanEntries({ conversationId: ROOT_CONVERSATION_ID }, 10),
      );
      const entry = yield* Effect.forkChild(storage.visibleEntry(ROOT_CONVERSATION_ID, entryId));
      const head = yield* Effect.forkChild(storage.findLatestHeadMarker(ROOT_CONVERSATION_ID));
      const closing = yield* Effect.forkChild(close);
      assert.deepStrictEqual(
        (yield* Fiber.join(scan)).items.map((item) => item.id),
        [entryId],
      );
      assert.deepStrictEqual(
        Option.map(yield* Fiber.join(entry), (found) => found.entry.kind),
        Option.some("probe"),
      );
      assert.deepStrictEqual(yield* Fiber.join(head), Option.none());
      yield* Fiber.join(closing);
      const error = yield* Effect.flip(
        storage.scanEntries({ conversationId: ROOT_CONVERSATION_ID }, 10),
      );
      assert.deepStrictEqual(errorTags(error), ["StorageFailure", "Closed"]);
      assert.include(error.message, "closed");
    }).pipe(Effect.scoped),
  );

  it.effect("adopts IDs a commit claims only after the commit succeeds", () =>
    Effect.gen(function* () {
      const { storage } = yield* open;
      yield* commitWrites(storage, [{ type: "conversation", value: { id: ROOT_CONVERSATION_ID } }]);
      assert.strictEqual(yield* storage.mintId, 2);
      // Rejected: the conversation already exists. Its entry ID 200 must not be adopted.
      yield* Effect.flip(
        commitWrites(storage, [
          {
            type: "entry",
            value: {
              id: EntryId.make(200),
              conversationId: ROOT_CONVERSATION_ID,
              kind: "rejected",
            },
          },
          { type: "conversation", value: { id: ROOT_CONVERSATION_ID } },
        ]),
      );
      assert.strictEqual(yield* storage.mintId, 3);
      assert.deepStrictEqual(yield* storage.entry(EntryId.make(200)), Option.none());
      yield* commitWrites(storage, [
        {
          type: "entry",
          value: { id: EntryId.make(100), conversationId: ROOT_CONVERSATION_ID, kind: "settled" },
        },
      ]);
      assert.strictEqual(yield* storage.mintId, 101);
    }).pipe(Effect.scoped),
  );

  it.effect("does not roll minted IDs back when a commit fails", () =>
    Effect.gen(function* () {
      const { storage } = yield* open;
      const minted = yield* storage.mintId;
      yield* Effect.flip(
        commitWrites(storage, [
          {
            type: "entry",
            value: { id: EntryId.make(minted), conversationId: ROOT_CONVERSATION_ID, kind: "x" },
          },
          {
            type: "entry",
            value: { id: EntryId.make(minted), conversationId: ROOT_CONVERSATION_ID, kind: "x" },
          },
        ]),
      );
      assert.strictEqual(yield* storage.mintId, minted + 1);
    }).pipe(Effect.scoped),
  );

  it.effect("reads a document from one committed state while a commit replaces its base", () =>
    Effect.gen(function* () {
      const id = DocumentId.make(5);
      // Each yield count starts the commit at a different point of the read's queries.
      for (let yields = 0; yields < 16; yields++) {
        yield* Effect.scoped(
          Effect.gen(function* () {
            const { storage } = yield* open;
            yield* commitWrites(storage, [
              {
                type: "document.create",
                record: { id, kind: "replaced", scope: { kind: "session" } },
                content: { kind: "base", version: 1, value: { value: 1 } },
              },
            ]);
            const read = yield* Effect.forkChild(storage.document(id, "current"));
            for (let index = 0; index < yields; index++) yield* Effect.yieldNow;
            const replace = yield* Effect.forkChild(
              commitWrites(storage, [
                {
                  type: "document.change",
                  id,
                  content: { kind: "base", version: 1, value: { value: 2 } },
                },
              ]),
            );
            const stored = yield* Fiber.join(read);
            yield* Fiber.join(replace);
            assert.deepInclude(
              [{ value: 1 }, { value: 2 }],
              Option.getOrUndefined(Option.map(stored, (document) => document.value)),
            );
          }),
        );
      }
    }),
  );

  it.effect("finishes an admitted commit when its caller is interrupted", () =>
    Effect.gen(function* () {
      const { storage } = yield* open;
      const commit = yield* Effect.forkChild(
        commitWrites(storage, [{ type: "conversation", value: { id: ROOT_CONVERSATION_ID } }]),
      );
      yield* Effect.yieldNow;
      yield* Fiber.interrupt(commit);
      assert.deepStrictEqual(
        yield* storage.conversation(ROOT_CONVERSATION_ID),
        Option.some({ id: ROOT_CONVERSATION_ID }),
      );
    }).pipe(Effect.scoped),
  );
});

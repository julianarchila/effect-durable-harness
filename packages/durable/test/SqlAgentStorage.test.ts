import * as NodeFileSystem from "@effect/platform-node/NodeFileSystem";
import * as SqliteClient from "@effect/sql-sqlite-node/SqliteClient";
import { assert, describe, it } from "@effect/vitest";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as SqlClient from "effect/sql/SqlClient";
import {
  AgentStorage,
  type StorageFailure,
  type StorageRequestError,
  type StorageWrite,
} from "../src/AgentStorage.ts";
import { ConversationId, DocumentId, EntryId, ROOT_CONVERSATION_ID, TaskId } from "../src/Ids.ts";
import type { DocumentCreate, EntryRecord, TaskRecord } from "../src/Records.ts";
import * as SqlAgentStorage from "../src/SqlAgentStorage.ts";
import * as AgentStorageConformance from "../src/testing/AgentStorageConformance.ts";
import { commitWrites } from "../src/testing/CommitWrites.ts";

/** `_tag` of a storage error, and the `reason._tag` of a `StorageFailure`. */
const errorTags = (error: StorageFailure | StorageRequestError) =>
  error._tag === "StorageFailure" ? [error._tag, error.reason._tag] : [error._tag];

// `SqlAgentStorage` on `@effect/sql-sqlite-node`. WAL and checkpoints belong to
// the `SqlClient` layer, not to storage, so they are not tested here.

// Since effect 4.0.1 the client layer fails with `SqlError` when it cannot open the database; a test that
// cannot open SQLite is broken setup, so it dies.
const memoryClient = Layer.orDie(SqliteClient.layer({ filename: ":memory:" }));

describe("SqlAgentStorage conformance", () => {
  const storage = AgentStorageConformance.layer(
    SqlAgentStorage.layer().pipe(Layer.provide(memoryClient)),
  );
  for (const conformanceCase of AgentStorageConformance.cases) {
    it.effect(conformanceCase.name, () => conformanceCase.run.pipe(Effect.provide(storage)));
  }
});

/**
 * Storage that rebuilds the `SqlAgentStorage` layer over the same `SqlClient`
 * after every commit, as if reopening the file. Its only in-memory state is the next ID, so every read after a
 * commit must come from the database.
 */
const reopening = Effect.gen(function* () {
  const sql = yield* Layer.build(memoryClient);
  const build = Effect.gen(function* () {
    const scope = yield* Scope.make();
    const context = yield* Layer.buildWithScope(SqlAgentStorage.layer(), scope).pipe(
      Effect.provideContext(sql),
    );
    return { scope, storage: Context.get(context, AgentStorage) };
  });
  let current = yield* build;
  let closed = false;
  yield* Effect.addFinalizer(() =>
    Effect.suspend(() => {
      closed = true;
      return Scope.close(current.scope, Exit.void);
    }),
  );
  // Once closed, a commit reaches the closed storage instead of reopening it.
  const reopen = Effect.suspend(() =>
    closed
      ? Effect.void
      : Effect.gen(function* () {
          yield* Scope.close(current.scope, Exit.void);
          current = yield* build;
        }),
  );
  const storage: AgentStorage["Service"] = {
    commit: (batches, receive) =>
      Effect.suspend(() => current.storage.commit(batches, receive)).pipe(
        Effect.exit,
        Effect.tap(() => reopen),
        Effect.flatten,
      ),
    mintId: Effect.suspend(() => current.storage.mintId),
    conversation: (id) => Effect.suspend(() => current.storage.conversation(id)),
    scanConversations: (query, limit, cursor) =>
      Effect.suspend(() => current.storage.scanConversations(query, limit, cursor)),
    entry: (id) => Effect.suspend(() => current.storage.entry(id)),
    visibleEntry: (conversationId, id) =>
      Effect.suspend(() => current.storage.visibleEntry(conversationId, id)),
    findLatestHeadMarker: (conversationId, atOrBefore) =>
      Effect.suspend(() => current.storage.findLatestHeadMarker(conversationId, atOrBefore)),
    scanEntries: (query, limit, cursor) =>
      Effect.suspend(() => current.storage.scanEntries(query, limit, cursor)),
    task: (id) => Effect.suspend(() => current.storage.task(id)),
    scanTasks: (query, limit, cursor) =>
      Effect.suspend(() => current.storage.scanTasks(query, limit, cursor)),
    submission: (id) => Effect.suspend(() => current.storage.submission(id)),
    scanSubmissions: (query, limit, cursor) =>
      Effect.suspend(() => current.storage.scanSubmissions(query, limit, cursor)),
    submissionByRequest: (conversationId, requestId) =>
      Effect.suspend(() => current.storage.submissionByRequest(conversationId, requestId)),
    findDocument: (address, at) => Effect.suspend(() => current.storage.findDocument(address, at)),
    document: (id, at) => Effect.suspend(() => current.storage.document(id, at)),
    scanDocuments: (query, limit, cursor) =>
      Effect.suspend(() => current.storage.scanDocuments(query, limit, cursor)),
  };
  return storage;
});

describe("SqlAgentStorage conformance across reopen", () => {
  const storage = Layer.succeed(AgentStorageConformance.ConformanceStorage, { open: reopening });
  for (const conformanceCase of AgentStorageConformance.cases) {
    it.effect(conformanceCase.name, () => conformanceCase.run.pipe(Effect.provide(storage)));
  }
});

// -----------------------------------------------------------------------------
// file-backed storage
// -----------------------------------------------------------------------------

const databasePath = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem;
  const directory = yield* fs.makeTempDirectoryScoped({ prefix: "durable-sqlite-" });
  return `${directory}/storage.sqlite`;
});

/** Opens a file-backed client and, unless `raw`, storage over it; `close` closes both. */
const open = (filename: string) =>
  Effect.gen(function* () {
    const scope = yield* Scope.make();
    yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void));
    const context = yield* Layer.buildWithScope(
      SqlAgentStorage.layer().pipe(
        Layer.provideMerge(Layer.orDie(SqliteClient.layer({ filename }))),
      ),
      scope,
    );
    return {
      storage: Context.get(context, AgentStorage),
      sql: Context.get(context, SqlClient.SqlClient),
      close: Scope.close(scope, Exit.void),
    };
  });

const openRaw = (filename: string) =>
  Effect.gen(function* () {
    const scope = yield* Scope.make();
    yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void));
    const context = yield* Layer.buildWithScope(
      Layer.orDie(SqliteClient.layer({ filename })),
      scope,
    );
    return { sql: Context.get(context, SqlClient.SqlClient), close: Scope.close(scope, Exit.void) };
  });

const CountRow = Schema.Struct({ value: Schema.Number });

const scalar = (sql: SqlClient.SqlClient, statement: string, params: ReadonlyArray<number> = []) =>
  sql.unsafe(statement, params).pipe(
    Effect.flatMap((rows) => Schema.decodeUnknownEffect(CountRow)(rows[0])),
    Effect.map((row) => row.value),
  );

const entry = (id: EntryId, data?: Schema.Json): EntryRecord =>
  data === undefined
    ? { id, conversationId: ROOT_CONVERSATION_ID, kind: "message" }
    : { id, conversationId: ROOT_CONVERSATION_ID, kind: "message", data };

const pendingTask = (id: TaskId): TaskRecord => ({
  id,
  conversationId: ROOT_CONVERSATION_ID,
  kind: "test.task",
  version: 1,
  input: null,
  state: { status: "pending", checkpoint: { phase: "ready" } },
  background: false,
  abortRequested: false,
});

const createRoot = (storage: AgentStorage["Service"]) =>
  commitWrites(storage, [{ type: "conversation", value: { id: ROOT_CONVERSATION_ID } }]);

const revisionCount = (sql: SqlClient.SqlClient, documentId: DocumentId) =>
  scalar(sql, "SELECT count(*) AS value FROM durable_document_revisions WHERE document_id = ?", [
    documentId,
  ]);

const scoped = <A, E>(
  name: string,
  test: () => Effect.Effect<A, E, Scope.Scope | FileSystem.FileSystem>,
) => it.live(name, () => Effect.scoped(test()).pipe(Effect.provide(NodeFileSystem.layer)));

describe("SqlAgentStorage", () => {
  scoped("persists records, sequence allocation, and global ID allocation across reopen", () =>
    Effect.gen(function* () {
      const path = yield* databasePath;
      const first = yield* open(path);
      assert.strictEqual(yield* createRoot(first.storage), 1);
      const entryId = EntryId.make(yield* first.storage.mintId);
      assert.strictEqual(
        yield* commitWrites(first.storage, [{ type: "entry", value: entry(entryId) }]),
        2,
      );
      yield* first.close;

      const reopened = yield* open(path);
      assert.deepStrictEqual(
        yield* reopened.storage.entry(entryId),
        Option.some({ entry: entry(entryId), commitSeq: 2 }),
      );
      assert.strictEqual(yield* reopened.storage.mintId, entryId + 1);
      const taskId = TaskId.make(yield* reopened.storage.mintId);
      assert.strictEqual(
        yield* commitWrites(reopened.storage, [{ type: "task", value: pendingTask(taskId) }]),
        3,
      );
    }),
  );

  scoped("rejects persisted metadata corruption on reopen", () =>
    Effect.gen(function* () {
      const path = yield* databasePath;
      yield* (yield* open(path)).close;
      const raw = yield* openRaw(path);
      yield* raw.sql.unsafe("DELETE FROM durable_metadata");
      yield* raw.close;
      const error = yield* Effect.flip(open(path));
      assert.deepStrictEqual(errorTags(error), ["StorageFailure", "Corrupted"]);
      assert.include(error.message, "Durable SQLite metadata is missing");
    }),
  );

  scoped("rejects a document whose required base is missing", () =>
    Effect.gen(function* () {
      const { storage, sql } = yield* open(yield* databasePath);
      yield* createRoot(storage);
      const id = DocumentId.make(yield* storage.mintId);
      yield* commitWrites(storage, [
        {
          type: "document.create",
          record: { id, kind: "corrupt", scope: { kind: "session" } },
          content: { kind: "base", version: 1, value: { retained: true } },
        },
      ]);
      yield* sql.unsafe("DELETE FROM durable_document_revisions WHERE document_id = ?", [id]);
      const error = yield* Effect.flip(storage.document(id, "current"));
      assert.deepStrictEqual(errorTags(error), ["StorageFailure", "Corrupted"]);
      assert.strictEqual(error.message, `Document ${id} is missing a required base`);
    }),
  );

  scoped(
    "replays detached root replacements and follow-up edits while rejecting corrupt operations",
    () =>
      Effect.gen(function* () {
        const { storage, sql } = yield* open(yield* databasePath);
        yield* createRoot(storage);
        const id = DocumentId.make(yield* storage.mintId);
        const change = (ops: Extract<StorageWrite, { type: "document.change" }>["content"]) =>
          commitWrites(storage, [{ type: "document.change", id, content: ops }]);
        yield* commitWrites(storage, [
          {
            type: "document.create",
            record: { id, kind: "replay", scope: { kind: "session" } },
            content: { kind: "base", version: 1, value: { nested: { value: 1 }, rows: [] } },
          },
        ]);
        yield* change({
          kind: "delta",
          version: 1,
          ops: [["r", { nested: { value: 2 }, rows: [{ id: 1 }] }]],
        });
        yield* change({
          kind: "delta",
          version: 1,
          ops: [
            ["s", ["nested", "value"], 3],
            ["p", ["rows"], 1, 0, [{ id: 2 }]],
            ["m", ["rows"], [1, 0]],
          ],
        });
        yield* change({ kind: "delta", version: 1, ops: [["s", ["nested", "value"], 4]] });

        const expected = { nested: { value: 4 }, rows: [{ id: 2 }, { id: 1 }] };
        const first = Option.getOrThrow(yield* storage.document(id, "current"));
        assert.deepStrictEqual(first.value, expected);
        Object.assign(first.value, { nested: { value: 99 } });
        assert.deepStrictEqual(
          Option.getOrThrow(yield* storage.document(id, "current")).value,
          expected,
        );

        yield* sql.unsafe(
          `UPDATE durable_document_revisions SET content = ? WHERE document_id = ? AND seq =
          (SELECT max(seq) FROM durable_document_revisions WHERE document_id = ?)`,
          ['[["unknown"]]', id, id],
        );
        const error = yield* Effect.flip(storage.document(id, "current"));
        assert.deepStrictEqual(errorTags(error), ["StorageFailure", "Corrupted"]);
        assert.include(error.message, "unknown op verb");
      }),
  );

  scoped("rolls SQL rows and sequence allocation back as one transaction", () =>
    Effect.gen(function* () {
      const { storage, sql } = yield* open(yield* databasePath);
      yield* createRoot(storage);
      const transientId = EntryId.make(yield* storage.mintId);
      const transientTaskId = TaskId.make(yield* storage.mintId);
      const circular: Array<Schema.Json> = [];
      circular.push(circular);
      const error = yield* Effect.flip(
        commitWrites(storage, [
          { type: "entry", value: entry(transientId) },
          {
            type: "task",
            // A circular input JSON cannot encode: rejected mid-transaction.
            value: { ...pendingTask(transientTaskId), input: circular },
          },
        ]),
      );
      assert.include(error.message, "circular structure");
      assert.deepStrictEqual(yield* storage.entry(transientId), Option.none());
      assert.deepStrictEqual(yield* storage.task(transientTaskId), Option.none());
      const committedId = EntryId.make(yield* storage.mintId);
      assert.strictEqual(
        yield* commitWrites(storage, [{ type: "entry", value: entry(committedId) }]),
        2,
      );
      assert.strictEqual(yield* scalar(sql, "SELECT count(*) AS value FROM durable_entries"), 1);
      assert.strictEqual(
        yield* scalar(sql, "SELECT next_seq AS value FROM durable_metadata WHERE singleton = 1"),
        3,
      );
    }),
  );

  scoped("reconstructs recent and ancient rewindable points after reopen", () =>
    Effect.gen(function* () {
      const path = yield* databasePath;
      const first = yield* open(path);
      const storage = first.storage;
      yield* createRoot(storage);
      const id = DocumentId.make(yield* storage.mintId);
      const record = {
        id,
        kind: "history",
        scope: { kind: "conversation", conversationId: ROOT_CONVERSATION_ID },
        history: "rewindable",
        fork: "asOf",
      } satisfies DocumentCreate;
      const createdAt = yield* commitWrites(storage, [
        {
          type: "document.create",
          record,
          content: { kind: "base", version: 1, value: { count: 0 } },
        },
      ]);
      let ancientAt = createdAt;
      let recentAt = createdAt;
      for (let count = 1; count <= 40; count++) {
        recentAt = yield* commitWrites(storage, [
          {
            type: "document.change",
            id,
            content:
              count === 20
                ? { kind: "base", version: 1, value: { count } }
                : { kind: "delta", version: 1, ops: [["s", ["count"], count]] },
          },
        ]);
        if (count === 5) ancientAt = recentAt;
      }
      yield* first.close;
      const reopened = yield* open(path);
      assert.deepStrictEqual(
        Option.getOrThrow(yield* reopened.storage.document(id, ancientAt)).value,
        {
          count: 5,
        },
      );
      assert.deepStrictEqual(
        Option.getOrThrow(yield* reopened.storage.document(id, recentAt)).value,
        {
          count: 40,
        },
      );
    }),
  );

  scoped(
    "uses indexes for exact addresses, exact scopes, entry history, and document revision tails",
    () =>
      Effect.gen(function* () {
        const { sql } = yield* open(yield* databasePath);
        const PlanRow = Schema.Struct({ detail: Schema.String });
        const plan = (statement: string, params: ReadonlyArray<string | number>) =>
          sql.unsafe(`EXPLAIN QUERY PLAN ${statement}`, params).pipe(
            Effect.flatMap((rows) => Schema.decodeUnknownEffect(Schema.Array(PlanRow))(rows)),
            Effect.map((rows) => rows.map((row) => row.detail).join("\n")),
          );
        const details = [
          yield* plan(
            `SELECT record FROM durable_documents
            WHERE kind = ? AND scope_kind = ? AND owner_id = ? AND family = ? AND key_value = ?
            AND retired_at IS NULL ORDER BY created_at DESC LIMIT 1`,
            ["kind", "session", 0, 0, ""],
          ),
          yield* plan(
            `SELECT record FROM durable_documents
            WHERE kind = ? AND scope_kind = ? AND owner_id = ? AND family = ? AND key_value = ?
            AND created_at <= ? AND (retired_at IS NULL OR retired_at > ?)
            ORDER BY created_at DESC LIMIT 1`,
            ["kind", "conversation", 1, 0, "", 10, 10],
          ),
          yield* plan(
            `SELECT record FROM durable_documents
            WHERE scope_kind = ? AND owner_id = ? AND kind = ? AND id > ? ORDER BY id LIMIT ?`,
            ["task", 1, "kind", 0, 10],
          ),
          yield* plan(
            `SELECT record FROM durable_entries
            WHERE conversation_id = ? AND id <= ? ORDER BY id DESC LIMIT ?`,
            [1, 10, 10],
          ),
          yield* plan(
            `SELECT record FROM durable_entries
            WHERE conversation_id = ? AND head IS NOT NULL AND id <= ? ORDER BY id DESC LIMIT 1`,
            [1, 10],
          ),
          yield* plan(
            "SELECT record FROM durable_tasks WHERE status = ? AND id > ? ORDER BY id LIMIT ?",
            ["pending", 0, 10],
          ),
          yield* plan(
            `SELECT seq, kind, version, content FROM durable_document_revisions
            WHERE document_id = ? AND kind = 'base' AND seq <= ? ORDER BY seq DESC LIMIT 1`,
            [1, 10],
          ),
          yield* plan(
            `SELECT seq, kind, version, content FROM durable_document_revisions
            WHERE document_id = ? AND seq > ? AND seq <= ? ORDER BY seq`,
            [1, 5, 10],
          ),
        ];
        assert.include(details[0], "durable_documents_by_address");
        assert.include(details[1], "durable_documents_by_address");
        assert.include(details[2], "durable_documents_by_scope_kind");
        assert.include(details[3], "durable_entries_by_conversation");
        assert.include(details[4], "durable_entry_heads_by_conversation");
        assert.include(details[5], "durable_tasks_by_status");
        assert.include(details[6], "durable_document_revisions_by_kind");
        assert.include(details[7], "sqlite_autoindex_durable_document_revisions_1");
        for (const detail of details.slice(0, 3))
          assert.notInclude(detail, "SCAN durable_documents");
        assert.notInclude(details[7], "SCAN durable_document_revisions");
      }),
  );

  scoped("reclaims current-only revisions only after a base or retirement", () =>
    Effect.gen(function* () {
      const { storage, sql } = yield* open(yield* databasePath);
      yield* createRoot(storage);
      const id = DocumentId.make(yield* storage.mintId);
      yield* commitWrites(storage, [
        {
          type: "document.create",
          record: { id, kind: "latest", scope: { kind: "session" } },
          content: { kind: "base", version: 1, value: { count: 0 } },
        },
      ]);
      for (let count = 1; count <= 10; count++) {
        yield* commitWrites(storage, [
          {
            type: "document.change",
            id,
            content: { kind: "delta", version: 1, ops: [["s", ["count"], count]] },
          },
        ]);
      }
      assert.strictEqual(yield* revisionCount(sql, id), 11);
      const ContentRow = Schema.Struct({ content: Schema.String });
      const [latest] = yield* sql
        .unsafe(
          "SELECT content FROM durable_document_revisions WHERE document_id = ? AND kind = 'delta' ORDER BY seq DESC LIMIT 1",
          [id],
        )
        .pipe(Effect.flatMap(Schema.decodeUnknownEffect(Schema.Array(ContentRow))));
      assert.deepStrictEqual(
        yield* Schema.decodeUnknownEffect(Schema.fromJsonString(Schema.Json))(latest?.content),
        [["s", ["count"], 10]],
      );
      yield* commitWrites(storage, [
        {
          type: "document.change",
          id,
          content: { kind: "base", version: 1, value: { count: 11 } },
        },
      ]);
      assert.strictEqual(yield* revisionCount(sql, id), 1);
      yield* commitWrites(storage, [
        {
          type: "document.change",
          id,
          content: { kind: "delta", version: 1, ops: [["r", { count: 12 }]] },
        },
      ]);
      assert.strictEqual(yield* revisionCount(sql, id), 2);
      yield* commitWrites(storage, [{ type: "document.retire", id }]);
      assert.strictEqual(yield* revisionCount(sql, id), 0);
    }),
  );

  scoped("reuses pages released by current-only checkpoints", () =>
    Effect.gen(function* () {
      const { storage, sql } = yield* open(yield* databasePath);
      yield* createRoot(storage);
      const id = DocumentId.make(yield* storage.mintId);
      const large = "x".repeat(512 * 1024);
      yield* commitWrites(storage, [
        {
          type: "document.create",
          record: { id, kind: "reuse", scope: { kind: "session" } },
          content: { kind: "base", version: 1, value: { text: large } },
        },
      ]);
      yield* commitWrites(storage, [
        {
          type: "document.change",
          id,
          content: { kind: "base", version: 1, value: { text: "small" } },
        },
      ]);
      const pagesAfterDelete = yield* scalar(
        sql,
        "SELECT page_count AS value FROM pragma_page_count()",
      );
      const freeAfterDelete = yield* scalar(
        sql,
        "SELECT freelist_count AS value FROM pragma_freelist_count()",
      );
      assert.isAbove(freeAfterDelete, 0);
      yield* commitWrites(storage, [
        {
          type: "document.change",
          id,
          content: { kind: "base", version: 1, value: { text: large } },
        },
      ]);
      const pagesAfterReuse = yield* scalar(
        sql,
        "SELECT page_count AS value FROM pragma_page_count()",
      );
      const freeAfterReuse = yield* scalar(
        sql,
        "SELECT freelist_count AS value FROM pragma_freelist_count()",
      );
      assert.isAtMost(pagesAfterReuse, pagesAfterDelete + 2);
      assert.isBelow(freeAfterReuse, freeAfterDelete);
    }),
  );

  scoped("keeps representative row and document storage bounded", () =>
    Effect.gen(function* () {
      const path = yield* databasePath;
      const { storage, close } = yield* open(path);
      yield* createRoot(storage);
      for (let index = 0; index < 100; index++) {
        const id = EntryId.make(yield* storage.mintId);
        yield* commitWrites(storage, [
          { type: "entry", value: entry(id, { index, text: "x".repeat(1_024) }) },
        ]);
      }
      const documentId = DocumentId.make(yield* storage.mintId);
      yield* commitWrites(storage, [
        {
          type: "document.create",
          record: {
            id: documentId,
            kind: "size.history",
            scope: { kind: "conversation", conversationId: ROOT_CONVERSATION_ID },
            history: "rewindable",
            fork: "asOf",
          },
          content: { kind: "base", version: 1, value: { count: 0 } },
        },
      ]);
      for (let count = 1; count <= 100; count++) {
        yield* commitWrites(storage, [
          {
            type: "document.change",
            id: documentId,
            content: { kind: "delta", version: 1, ops: [["s", ["count"], count]] },
          },
        ]);
      }
      yield* close;
      const fs = yield* FileSystem.FileSystem;
      assert.isBelow(Number((yield* fs.stat(path)).size), 1024 * 1024);
    }),
  );

  scoped("isolates two prefixes in one database", () =>
    Effect.gen(function* () {
      const path = yield* databasePath;
      const raw = yield* openRaw(path);
      const build = (prefix: string) =>
        Layer.build(SqlAgentStorage.layer({ prefix })).pipe(
          Effect.provideService(SqlClient.SqlClient, raw.sql),
          Effect.map(Context.get(AgentStorage)),
        );
      const first = yield* build("first_");
      const second = yield* build("second_");
      yield* createRoot(first);
      assert.deepStrictEqual(
        yield* first.conversation(ROOT_CONVERSATION_ID),
        Option.some({ id: ROOT_CONVERSATION_ID }),
      );
      assert.deepStrictEqual(yield* second.conversation(ROOT_CONVERSATION_ID), Option.none());
      assert.deepStrictEqual(yield* second.conversation(ConversationId.make(1)), Option.none());
    }),
  );

  scoped("rejects a prefix that is not an SQL identifier", () =>
    Effect.gen(function* () {
      const raw = yield* openRaw(yield* databasePath);
      const error = yield* Effect.flip(
        Layer.build(SqlAgentStorage.layer({ prefix: "1; DROP TABLE x; --" })).pipe(
          Effect.provideService(SqlClient.SqlClient, raw.sql),
        ),
      );
      assert.deepStrictEqual(errorTags(error), ["StorageFailure", "Rejected"]);
    }),
  );

  // SQLite reserves `sqlite_` names, in any case.
  scoped("rejects a prefix SQLite reserves", () =>
    Effect.gen(function* () {
      const raw = yield* openRaw(yield* databasePath);
      for (const prefix of ["sqlite_", "SQLite_durable_"]) {
        const error = yield* Effect.flip(
          Layer.build(SqlAgentStorage.layer({ prefix })).pipe(
            Effect.provideService(SqlClient.SqlClient, raw.sql),
          ),
        );
        assert.deepStrictEqual(errorTags(error), ["StorageFailure", "Rejected"]);
      }
    }),
  );
});

// Batches. Every database failure is a `SqlError`, which the storage maps to the `Backend` reason, so one way of
// failing covers them all: the check's read hits a missing table.
/** Hides the record type index, so a check's read of it fails in the database; `restore` brings it back. */
const breakChecks = (sql: SqlClient.SqlClient) => ({
  break: sql.unsafe("ALTER TABLE durable_record_ids RENAME TO durable_record_ids_hidden"),
  restore: sql.unsafe("ALTER TABLE durable_record_ids_hidden RENAME TO durable_record_ids"),
});

describe("SqlAgentStorage batches", () => {
  scoped(
    "rejects the whole call, the batches before it included, when a later batch's check read fails",
    () =>
      Effect.gen(function* () {
        const { storage, sql } = yield* open(yield* databasePath);
        yield* createRoot(storage);
        const documentId = DocumentId.make(yield* storage.mintId);
        yield* commitWrites(storage, [
          {
            type: "document.create",
            record: { id: documentId, kind: "batch", scope: { kind: "session" } },
            content: { kind: "base", version: 1, value: { count: 0 } },
          },
        ]);
        const entryId = EntryId.make(yield* storage.mintId);
        const checks = breakChecks(sql);
        yield* checks.break;
        const receipts: Array<number> = [];
        // The first batch claims no ID, so its checks pass and it is written; the second's check read fails.
        const outcome = yield* Effect.exit(
          storage.commit(
            [
              [
                {
                  type: "document.change",
                  id: documentId,
                  content: { kind: "delta", version: 1, ops: [["s", ["count"], 1]] },
                },
              ],
              [{ type: "entry", value: entry(entryId) }],
            ],
            (index) => receipts.push(index),
          ),
        );
        yield* checks.restore;
        assert.isTrue(Exit.isFailure(outcome));
        if (Exit.isFailure(outcome)) {
          const error = Exit.findErrorOption(outcome);
          assert.deepStrictEqual(
            Option.map(error, errorTags),
            Option.some(["StorageFailure", "Backend"]),
          );
        }
        assert.deepStrictEqual(receipts, []);
        assert.deepStrictEqual(
          Option.map(yield* storage.document(documentId, "current"), (stored) => stored.value),
          Option.some({ count: 0 }),
        );
        assert.deepStrictEqual(yield* storage.entry(entryId), Option.none());
      }),
  );

  scoped(
    "rejects a batch call whose check read fails in the database, and refuses only for its own checks",
    () =>
      Effect.gen(function* () {
        const { storage, sql } = yield* open(yield* databasePath);
        yield* createRoot(storage);
        const first = EntryId.make(yield* storage.mintId);
        const second = EntryId.make(yield* storage.mintId);
        const batches: Array<ReadonlyArray<StorageWrite>> = [first, second].map((id) => [
          { type: "entry", value: entry(id) },
        ]);
        const checks = breakChecks(sql);
        yield* checks.break;
        const receipts: Array<number> = [];
        const outcome = yield* Effect.exit(
          storage.commit(batches, (index) => receipts.push(index)),
        );
        yield* checks.restore;
        // Unknown outcome, rolled back whole: nothing received, nothing stored.
        assert.isTrue(Exit.isFailure(outcome));
        assert.deepStrictEqual(receipts, []);
        assert.deepStrictEqual(yield* storage.entry(first), Option.none());
        // The storage's own check still refuses, keeping the batches before it.
        const [firstBatch] = batches;
        if (firstBatch === undefined) return assert.fail("Expected a first batch");
        const refused = yield* storage.commit(
          [firstBatch, [{ type: "conversation", value: { id: ROOT_CONVERSATION_ID } }]],
          (index) => receipts.push(index),
        );
        assert.strictEqual(refused.stored, 1);
        assert.include(refused.refused?.message, "already belongs to conversation");
        assert.deepStrictEqual(receipts, [0]);
        assert.isTrue(Option.isSome(yield* storage.entry(first)));
      }),
  );
});

// A batch whose own checks throw without a failed database call is refused, keeping the batches before it; only
// a database failure fails the call.
describe("SqlAgentStorage batches, own checks", () => {
  scoped(
    "refuses a batch whose own check dies on a corrupt row, and keeps the batches before it",
    () =>
      Effect.gen(function* () {
        const { storage, sql } = yield* open(yield* databasePath);
        yield* createRoot(storage);
        const documentId = DocumentId.make(yield* storage.mintId);
        yield* commitWrites(storage, [
          {
            type: "document.create",
            record: { id: documentId, kind: "corrupt", scope: { kind: "session" } },
            content: { kind: "base", version: 1, value: { n: 0 } },
          },
        ]);
        // The document's stored record is JSON `null`: the check that reads it back throws.
        yield* sql.unsafe("UPDATE durable_documents SET record = 'null' WHERE id = ?", [
          documentId,
        ]);
        const first = EntryId.make(yield* storage.mintId);
        const receipts: Array<number> = [];
        const result = yield* storage.commit(
          [
            [{ type: "entry", value: entry(first) }],
            [
              {
                type: "document.change",
                id: documentId,
                content: { kind: "delta", version: 1, ops: [["s", ["n"], 1]] },
              },
            ],
          ],
          (index) => receipts.push(index),
        );
        assert.strictEqual(result.stored, 1);
        assert.deepStrictEqual(
          result.refused === undefined ? undefined : errorTags(result.refused),
          ["StorageFailure", "Corrupted"],
        );
        assert.deepStrictEqual(receipts, [0]);
        assert.isTrue(Option.isSome(yield* storage.entry(first)));
      }),
  );
});

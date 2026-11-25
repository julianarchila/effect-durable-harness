/**
 * SQLite schema of `SqlAgentStorage` and its migrator.
 *
 * @internal
 */
import * as Effect from "effect/Effect";
import * as Result from "effect/Result";
import * as SqlClient from "effect/sql/SqlClient";
import type { SqlError } from "effect/sql/SqlError";
import { Backend, StorageFailure } from "../AgentStorage.ts";
import { corrupted, rejected } from "./commit.ts";

/** One immutable, versioned step of the schema history. */
export interface SqlMigration {
  readonly version: number;
  readonly statements: ReadonlyArray<string>;
}

const identifier = /^[A-Za-z_][A-Za-z0-9_]*$/;

/**
 * A prefix is an SQL identifier (letters, digits and underscores, not starting with a digit) that does not start with
 * SQLite's reserved `sqlite_`, in any case.
 */
export const validatePrefix = (prefix: string): Result.Result<string, StorageFailure> =>
  !identifier.test(prefix)
    ? Result.fail(rejected(`Invalid SqlAgentStorage table prefix: ${JSON.stringify(prefix)}`))
    : prefix.toLowerCase().startsWith("sqlite_")
      ? Result.fail(rejected(`SqlAgentStorage table prefix ${JSON.stringify(prefix)} is reserved`))
      : Result.succeed(prefix);

// Every table and index name carries the layer's prefix. The default prefix is
// `durable_`, not none, because storage shares a Durable Object's SQLite with
// the consumer; the schema and metadata tables are `<prefix>schema` and
// `<prefix>metadata`.
/** Prefixed table names. */
export const tables = (prefix: string) => ({
  schema: `${prefix}schema`,
  metadata: `${prefix}metadata`,
  recordIds: `${prefix}record_ids`,
  conversations: `${prefix}conversations`,
  entries: `${prefix}entries`,
  tasks: `${prefix}tasks`,
  submissions: `${prefix}submissions`,
  documents: `${prefix}documents`,
  revisions: `${prefix}document_revisions`,
});

// next_id is TEXT because node:sqlite rejects INTEGER results outside JavaScript's safe integer range.
const initialSchema = (prefix: string): ReadonlyArray<string> => {
  const t = tables(prefix);
  return [
    `CREATE TABLE ${t.metadata} (
      singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
      next_id TEXT NOT NULL,
      next_seq INTEGER NOT NULL
    ) STRICT`,
    `INSERT INTO ${t.metadata} (singleton, next_id, next_seq) VALUES (1, '2', 1)`,
    `CREATE TABLE ${t.recordIds} (
      id INTEGER PRIMARY KEY,
      record_type TEXT NOT NULL CHECK (record_type IN ('conversation', 'entry', 'task', 'submission', 'document'))
    ) STRICT`,
    `CREATE TABLE ${t.conversations} (
      id INTEGER PRIMARY KEY,
      owner_conversation_id INTEGER,
      owner_task_id INTEGER,
      record TEXT NOT NULL CHECK (json_valid(record))
    ) STRICT`,
    `CREATE INDEX ${prefix}conversations_by_owner_conversation ON ${t.conversations} (owner_conversation_id, id)`,
    `CREATE INDEX ${prefix}conversations_by_owner_task ON ${t.conversations} (owner_task_id, id)`,
    `CREATE TABLE ${t.entries} (
      id INTEGER PRIMARY KEY,
      conversation_id INTEGER NOT NULL,
      head INTEGER,
      commit_seq INTEGER NOT NULL,
      record TEXT NOT NULL CHECK (json_valid(record))
    ) STRICT`,
    `CREATE INDEX ${prefix}entries_by_conversation ON ${t.entries} (conversation_id, id DESC)`,
    `CREATE INDEX ${prefix}entry_heads_by_conversation ON ${t.entries} (conversation_id, id DESC) WHERE head IS NOT NULL`,
    `CREATE TABLE ${t.tasks} (
      id INTEGER PRIMARY KEY,
      conversation_id INTEGER NOT NULL,
      kind TEXT NOT NULL,
      status TEXT NOT NULL CHECK (status IN ('pending', 'running', 'waiting', 'completing', 'terminal')),
      abort_requested INTEGER NOT NULL CHECK (abort_requested IN (0, 1)),
      background INTEGER NOT NULL CHECK (background IN (0, 1)),
      record TEXT NOT NULL CHECK (json_valid(record))
    ) STRICT`,
    `CREATE INDEX ${prefix}tasks_by_status ON ${t.tasks} (status, id)`,
    `CREATE INDEX ${prefix}tasks_by_conversation ON ${t.tasks} (conversation_id, id)`,
    `CREATE INDEX ${prefix}tasks_by_kind ON ${t.tasks} (kind, id)`,
    `CREATE INDEX ${prefix}tasks_by_abort_requested ON ${t.tasks} (abort_requested, id)`,
    `CREATE INDEX ${prefix}tasks_by_background ON ${t.tasks} (background, id)`,
    `CREATE TABLE ${t.submissions} (
      id INTEGER PRIMARY KEY,
      conversation_id INTEGER NOT NULL,
      request_id TEXT,
      status TEXT NOT NULL CHECK (status IN ('queued', 'placed', 'done', 'unanswered')),
      record TEXT NOT NULL CHECK (json_valid(record))
    ) STRICT`,
    `CREATE INDEX ${prefix}submissions_by_request ON ${t.submissions} (conversation_id, request_id)`,
    `CREATE INDEX ${prefix}submissions_by_conversation ON ${t.submissions} (conversation_id, id)`,
    `CREATE INDEX ${prefix}submissions_by_status ON ${t.submissions} (status, id)`,
    `CREATE TABLE ${t.documents} (
      id INTEGER PRIMARY KEY,
      kind TEXT NOT NULL,
      family INTEGER NOT NULL CHECK (family IN (0, 1)),
      key_value TEXT NOT NULL,
      scope_kind TEXT NOT NULL CHECK (scope_kind IN ('session', 'conversation', 'task')),
      owner_id INTEGER NOT NULL,
      created_at INTEGER NOT NULL,
      retired_at INTEGER,
      record TEXT NOT NULL CHECK (json_valid(record))
    ) STRICT`,
    `CREATE INDEX ${prefix}documents_by_address
      ON ${t.documents} (kind, scope_kind, owner_id, family, key_value, created_at DESC, retired_at)`,
    `CREATE INDEX ${prefix}documents_by_scope ON ${t.documents} (scope_kind, owner_id, id)`,
    `CREATE INDEX ${prefix}documents_by_scope_kind ON ${t.documents} (scope_kind, owner_id, kind, id)`,
    `CREATE TABLE ${t.revisions} (
      document_id INTEGER NOT NULL,
      seq INTEGER NOT NULL,
      kind TEXT NOT NULL CHECK (kind IN ('base', 'delta')),
      version INTEGER NOT NULL,
      content TEXT NOT NULL CHECK (json_valid(content)),
      PRIMARY KEY (document_id, seq)
    ) STRICT`,
    `CREATE INDEX ${prefix}document_revisions_by_kind ON ${t.revisions} (document_id, kind, seq DESC)`,
  ];
};

/**
 * Version 2 adds the `sleeping` task status. SQLite cannot change a CHECK
 * constraint in place, so the table is rebuilt, with its indexes.
 */
const sleepingStatus = (prefix: string): ReadonlyArray<string> => {
  const t = tables(prefix);
  const rebuilt = `${t.tasks}_v2`;
  return [
    `CREATE TABLE ${rebuilt} (
      id INTEGER PRIMARY KEY,
      conversation_id INTEGER NOT NULL,
      kind TEXT NOT NULL,
      status TEXT NOT NULL CHECK (status IN ('pending', 'running', 'waiting', 'sleeping', 'completing', 'terminal')),
      abort_requested INTEGER NOT NULL CHECK (abort_requested IN (0, 1)),
      background INTEGER NOT NULL CHECK (background IN (0, 1)),
      record TEXT NOT NULL CHECK (json_valid(record))
    ) STRICT`,
    `INSERT INTO ${rebuilt} (id, conversation_id, kind, status, abort_requested, background, record)
      SELECT id, conversation_id, kind, status, abort_requested, background, record FROM ${t.tasks}`,
    `DROP TABLE ${t.tasks}`,
    `ALTER TABLE ${rebuilt} RENAME TO ${t.tasks}`,
    `CREATE INDEX ${prefix}tasks_by_status ON ${t.tasks} (status, id)`,
    `CREATE INDEX ${prefix}tasks_by_conversation ON ${t.tasks} (conversation_id, id)`,
    `CREATE INDEX ${prefix}tasks_by_kind ON ${t.tasks} (kind, id)`,
    `CREATE INDEX ${prefix}tasks_by_abort_requested ON ${t.tasks} (abort_requested, id)`,
    `CREATE INDEX ${prefix}tasks_by_background ON ${t.tasks} (background, id)`,
  ];
};

/** Immutable, ordered schema history for `prefix`. Append new migrations after the initial schema ships. */
export const migrations = (prefix: string): ReadonlyArray<SqlMigration> => [
  { version: 1, statements: initialSchema(prefix) },
  { version: 2, statements: sleepingStatus(prefix) },
];

export const currentSchemaVersion = 2;

/** Maps a driver failure to `StorageFailure`, once, at the storage boundary. */
export const backend = (error: SqlError): StorageFailure =>
  new StorageFailure({ reason: new Backend({ message: error.message, cause: error }) });

// An own migrator instead of `effect/sql/Migrator`, because that one creates its
// table outside the migration transaction (`Migrator.ts:312`), swallows `Locked`
// and never rejects a newer schema; the bootstrap must be atomic and a newer
// schema rejected.
/** Apply every pending migration, and the bootstrap of the schema table, in one transaction. */
export const applyMigrations = (
  prefix: string,
  pending: ReadonlyArray<SqlMigration>,
): Effect.Effect<void, StorageFailure, SqlClient.SqlClient> =>
  Effect.gen(function* () {
    for (const [index, migration] of pending.entries()) {
      if (migration.version !== index + 1) {
        return yield* rejected(
          "Durable SQLite migrations must have contiguous versions starting at 1",
        );
      }
    }
    const client = yield* SqlClient.SqlClient;
    const sql = client.withoutTransforms();
    const schema = tables(prefix).schema;
    yield* sql.withTransaction(
      Effect.gen(function* () {
        yield* sql.unsafe(`CREATE TABLE IF NOT EXISTS ${schema} (
          singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
          version INTEGER NOT NULL CHECK (version >= 0)
        ) STRICT`);
        yield* sql.unsafe(`INSERT OR IGNORE INTO ${schema} (singleton, version) VALUES (1, 0)`);
        const [row] = yield* sql.unsafe<{ readonly version: number }>(
          `SELECT version FROM ${schema} WHERE singleton = 1`,
        );
        if (row === undefined) return yield* corrupted("Durable SQLite schema metadata is missing");
        const supported = pending.at(-1)?.version ?? 0;
        if (row.version > supported) {
          return yield* corrupted(
            `Durable SQLite schema version ${row.version} is newer than supported version ${supported}`,
          );
        }
        for (const migration of pending) {
          if (migration.version <= row.version) continue;
          for (const statement of migration.statements) yield* sql.unsafe(statement);
          yield* sql.unsafe(`UPDATE ${schema} SET version = ? WHERE singleton = 1`, [
            migration.version,
          ]);
        }
      }),
    );
  }).pipe(Effect.catchTag("SqlError", (error) => Effect.fail(backend(error))));

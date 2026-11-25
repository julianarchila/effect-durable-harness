import * as NodeFileSystem from "@effect/platform-node/NodeFileSystem";
import * as SqliteClient from "@effect/sql-sqlite-node/SqliteClient";
import { assert, describe, it } from "@effect/vitest";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Scope from "effect/Scope";
import * as SqlClient from "effect/sql/SqlClient";
import {
  AgentStorage,
  type StorageFailure,
  type StorageRequestError,
} from "../src/AgentStorage.ts";
import { EntryId, ROOT_CONVERSATION_ID } from "../src/Ids.ts";
import * as SqlSchema from "../src/internal/sqlSchema.ts";
import * as SqlAgentStorage from "../src/SqlAgentStorage.ts";
import { commitWrites } from "../src/testing/CommitWrites.ts";

/** `_tag` of a storage error, and the `reason._tag` of a `StorageFailure`. */
const errorTags = (error: StorageFailure | StorageRequestError) =>
  error._tag === "StorageFailure" ? [error._tag, error.reason._tag] : [error._tag];

// The SQL storage migrator, with prefixed table names (see `src/internal/sqlSchema.ts`).

const prefix = "durable_";

const databasePath = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem;
  const directory = yield* fs.makeTempDirectoryScoped({ prefix: "durable-migrations-" });
  return `${directory}/storage.sqlite`;
});

const openClient = (filename: string) =>
  Effect.gen(function* () {
    const scope = yield* Scope.make();
    yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void));
    const context = yield* Layer.buildWithScope(
      Layer.orDie(SqliteClient.layer({ filename })),
      scope,
    );
    return { sql: Context.get(context, SqlClient.SqlClient), close: Scope.close(scope, Exit.void) };
  });

const openStorage = (sql: SqlClient.SqlClient) =>
  Layer.build(SqlAgentStorage.layer()).pipe(
    Effect.provideService(SqlClient.SqlClient, sql),
    Effect.map(Context.get(AgentStorage)),
  );

const rows = (sql: SqlClient.SqlClient, statement: string) => sql.unsafe(statement);

const migrate = (sql: SqlClient.SqlClient, migrations?: ReadonlyArray<SqlSchema.SqlMigration>) =>
  SqlSchema.applyMigrations(prefix, migrations ?? SqlSchema.migrations(prefix)).pipe(
    Effect.provideService(SqlClient.SqlClient, sql),
  );

const scoped = <A, E>(
  name: string,
  test: () => Effect.Effect<A, E, Scope.Scope | FileSystem.FileSystem>,
) => it.live(name, () => Effect.scoped(test()).pipe(Effect.provide(NodeFileSystem.layer)));

describe("SqlAgentStorage migrations", () => {
  scoped("creates the current schema and can be applied repeatedly", () =>
    Effect.gen(function* () {
      const { sql } = yield* openClient(yield* databasePath);
      yield* migrate(sql);
      yield* migrate(sql);
      assert.deepStrictEqual(
        yield* rows(sql, "SELECT version FROM durable_schema WHERE singleton = 1"),
        [{ version: SqlSchema.currentSchemaVersion }],
      );
      assert.deepStrictEqual(
        yield* rows(sql, "SELECT next_id, next_seq FROM durable_metadata WHERE singleton = 1"),
        [{ next_id: "2", next_seq: 1 }],
      );
    }),
  );

  scoped(
    "migrates a version 1 database to store sleeping tasks, keeping its tasks and indexes",
    () =>
      Effect.gen(function* () {
        const path = yield* databasePath;
        const first = yield* openClient(path);
        yield* migrate(first.sql, SqlSchema.migrations(prefix).slice(0, 1));
        // A waiting task as version 1 stored it.
        const record =
          '{"id":5,"conversationId":1,"kind":"test.task","version":1,"input":null,"background":false,' +
          '"abortRequested":false,"state":{"status":"waiting","checkpoint":{"phase":"run"},"on":[],"policy":"allSettled"}}';
        yield* first.sql.unsafe(
          "INSERT INTO durable_tasks (id, conversation_id, kind, status, abort_requested, background, record) VALUES (5, 1, 'test.task', 'waiting', 0, 0, ?)",
          [record],
        );
        const sleeping =
          "INSERT INTO durable_tasks (id, conversation_id, kind, status, abort_requested, background, record) VALUES (6, 1, 'test.task', 'sleeping', 0, 0, '{}')";
        yield* Effect.flip(first.sql.unsafe(sleeping));
        yield* first.close;

        const { sql } = yield* openClient(path);
        yield* openStorage(sql);
        assert.deepStrictEqual(
          yield* rows(sql, "SELECT version FROM durable_schema WHERE singleton = 1"),
          [{ version: 2 }],
        );
        assert.deepStrictEqual(
          yield* rows(sql, "SELECT status, record FROM durable_tasks WHERE id = 5"),
          [{ status: "waiting", record }],
        );
        yield* sql.unsafe(sleeping);
        assert.deepStrictEqual(
          yield* rows(
            sql,
            "SELECT name FROM sqlite_schema WHERE type = 'index' AND tbl_name = 'durable_tasks' ORDER BY name",
          ),
          [
            { name: "durable_tasks_by_abort_requested" },
            { name: "durable_tasks_by_background" },
            { name: "durable_tasks_by_conversation" },
            { name: "durable_tasks_by_kind" },
            { name: "durable_tasks_by_status" },
          ],
        );
        assert.deepStrictEqual(
          yield* rows(
            sql,
            "SELECT count(*) AS count FROM sqlite_schema WHERE name = 'durable_tasks_v2'",
          ),
          [{ count: 0 }],
        );
      }),
  );

  scoped("rejects a database newer than the portable core", () =>
    Effect.gen(function* () {
      const { sql } = yield* openClient(yield* databasePath);
      yield* migrate(sql);
      yield* sql.unsafe("UPDATE durable_schema SET version = ? WHERE singleton = 1", [
        SqlSchema.currentSchemaVersion + 1,
      ]);
      const error = yield* Effect.flip(openStorage(sql));
      assert.deepStrictEqual(errorTags(error), ["StorageFailure", "Corrupted"]);
      assert.include(error.message, "is newer than supported version");
    }),
  );

  scoped("rolls initial bootstrap and every pending migration back together", () =>
    Effect.gen(function* () {
      const { sql } = yield* openClient(yield* databasePath);
      const firstMigration: SqlSchema.SqlMigration = {
        version: 1,
        statements: [
          "CREATE TABLE migration_first (value TEXT) STRICT",
          "INSERT INTO migration_first (value) VALUES ('retained')",
        ],
      };
      const failed = yield* Effect.flip(
        migrate(sql, [
          firstMigration,
          {
            version: 2,
            statements: ["CREATE TABLE migration_second (value TEXT) STRICT", "THIS IS NOT SQL"],
          },
        ]),
      );
      assert.deepStrictEqual(errorTags(failed), ["StorageFailure", "Backend"]);
      assert.deepStrictEqual(
        yield* rows(
          sql,
          "SELECT count(*) AS count FROM sqlite_schema WHERE name IN ('durable_schema', 'migration_first', 'migration_second')",
        ),
        [{ count: 0 }],
      );

      yield* migrate(sql, [
        firstMigration,
        { version: 2, statements: ["CREATE TABLE migration_second (value TEXT) STRICT"] },
      ]);
      assert.deepStrictEqual(
        yield* rows(sql, "SELECT version FROM durable_schema WHERE singleton = 1"),
        [{ version: 2 }],
      );
      assert.deepStrictEqual(yield* rows(sql, "SELECT value FROM migration_first"), [
        { value: "retained" },
      ]);
    }),
  );

  scoped("rolls a failed migration back and preserves stored data for a successful retry", () =>
    Effect.gen(function* () {
      const path = yield* databasePath;
      const first = yield* openClient(path);
      const storage = yield* openStorage(first.sql);
      yield* commitWrites(storage, [
        { type: "conversation", value: { id: ROOT_CONVERSATION_ID } },
        {
          type: "entry",
          value: {
            id: EntryId.make(2),
            conversationId: ROOT_CONVERSATION_ID,
            kind: "retained",
            data: { retained: true },
          },
        },
      ]);
      yield* first.close;

      const { sql } = yield* openClient(path);
      const nextVersion = SqlSchema.currentSchemaVersion + 1;
      const failed = yield* Effect.flip(
        migrate(sql, [
          ...SqlSchema.migrations(prefix),
          {
            version: nextVersion,
            statements: ["CREATE TABLE migration_probe (value TEXT) STRICT", "THIS IS NOT SQL"],
          },
        ]),
      );
      assert.deepStrictEqual(errorTags(failed), ["StorageFailure", "Backend"]);
      assert.deepStrictEqual(
        yield* rows(sql, "SELECT version FROM durable_schema WHERE singleton = 1"),
        [{ version: SqlSchema.currentSchemaVersion }],
      );
      assert.deepStrictEqual(
        yield* rows(
          sql,
          "SELECT count(*) AS count FROM sqlite_schema WHERE type = 'table' AND name = 'migration_probe'",
        ),
        [{ count: 0 }],
      );

      yield* migrate(sql, [
        ...SqlSchema.migrations(prefix),
        { version: nextVersion, statements: ["CREATE TABLE migration_probe (value TEXT) STRICT"] },
      ]);
      assert.deepStrictEqual(
        yield* rows(sql, "SELECT version FROM durable_schema WHERE singleton = 1"),
        [{ version: nextVersion }],
      );
      assert.deepStrictEqual(
        yield* rows(sql, "SELECT record, commit_seq FROM durable_entries WHERE id = 2"),
        [
          {
            record: '{"id":2,"conversationId":1,"kind":"retained","data":{"retained":true}}',
            commit_seq: 1,
          },
        ],
      );
      assert.deepStrictEqual(
        yield* rows(sql, "SELECT next_id, next_seq FROM durable_metadata WHERE singleton = 1"),
        [{ next_id: "3", next_seq: 2 }],
      );
    }),
  );

  scoped("rejects migrations whose versions are not contiguous from 1", () =>
    Effect.gen(function* () {
      const { sql } = yield* openClient(yield* databasePath);
      const error = yield* Effect.flip(migrate(sql, [{ version: 2, statements: [] }]));
      assert.deepStrictEqual(errorTags(error), ["StorageFailure", "Rejected"]);
      assert.include(error.message, "contiguous versions starting at 1");
    }),
  );
});

/**
 * `AgentStorage` on `effect/sql`'s `SqlClient`, SQLite dialect. It runs
 * on Durable Object SQLite (`@effect/sql-sqlite-do`) and Node SQLite
 * (`@effect/sql-sqlite-node`).
 *
 * @since 0.0.0
 */
// There is no database facade, because `SqlClient` already provides it: queueing
// and rollback come from the driver (`sqlite-do`'s connection semaphore and
// `storage.transaction`, `sqlite-node`'s `BEGIN IMMEDIATE`).
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Result from "effect/Result";
import * as SqlClient from "effect/sql/SqlClient";
import { isSqlError } from "effect/sql/SqlError";
import {
  AgentStorage,
  type CommittedEntry,
  type ConversationQuery,
  type Cursor,
  type DocumentAddress,
  type DocumentContent,
  type DocumentPoint,
  type DocumentQuery,
  type EntryQuery,
  type HeadMarker,
  type Page,
  StorageFailure,
  StorageRequestError,
  type StorageWrite,
  type StoredDocument,
  type SubmissionQuery,
  type TaskQuery,
} from "./AgentStorage.ts";
import {
  type ConversationId,
  type DocumentId,
  type EntryId,
  Seq,
  type SubmissionId,
  type TaskId,
} from "./Ids.ts";
import {
  candidateNextId,
  checkDocumentActions,
  checkGlobalIds,
  copyContent,
  corrupted,
  type DocumentAction,
  isAliveAt,
  isCurrentOnly,
  mintFrom,
  prepareDocumentActions,
  rejected,
  replay,
  scopeOwner,
  type TableName,
} from "./internal/commit.ts";
import { openGate } from "./internal/gate.ts";
import { page, type ScanStart, scanStart } from "./internal/scan.ts";
import {
  applyMigrations,
  backend,
  migrations,
  tables,
  validatePrefix,
} from "./internal/sqlSchema.ts";
import type {
  ConversationRecord,
  DocumentCreate,
  DocumentRecord,
  EntryRecord,
  SubmissionRecord,
  TaskRecord,
} from "./Records.ts";

/**
 * Options of `SqlAgentStorage.layer`.
 *
 * @category models
 * @since 0.0.0
 */
export interface SqlAgentStorageOptions {
  /** Prefix of every table and index: an SQL identifier, not starting with `sqlite_`. Default `"durable_"`. */
  readonly prefix?: string | undefined;
}

type SqlValue = string | number | null;

interface JsonRow {
  readonly record: string;
}
interface EntryJsonRow {
  readonly record: string;
  readonly commit_seq: number;
}
interface RevisionRow {
  readonly seq: number;
  readonly kind: DocumentContent["kind"];
  readonly version: number;
  readonly content: string;
}
interface MetadataRow {
  readonly next_id: string;
  readonly next_seq: number;
}

// Rows are parsed, not decoded with Schema, because storage trusts its own
// writes and decoding every read costs time and could mishandle `__proto__`
// keys.
const parse = <A>(text: string): A => JSON.parse(text);

const stringify = <A>(value: A): Result.Result<string, StorageFailure> => {
  try {
    return Result.succeed(JSON.stringify(value));
  } catch (cause) {
    return Result.fail(rejected(cause instanceof Error ? cause.message : String(cause), cause));
  }
};

/** JSON text of a write; a value JSON cannot encode, such as a circular one, rejects the batch. */
const encode = <A>(value: A): Effect.Effect<string, StorageFailure> =>
  Effect.fromResult(stringify(value));

// Some SQLite bindings replace lone UTF-16 surrogates. JSON encoding keeps indexed identities lossless.
const encodeIndexed = (value: string): string => JSON.stringify(value);

const addressParts = (address: DocumentAddress | DocumentCreate | DocumentRecord) => ({
  kind: encodeIndexed(address.kind),
  scopeKind: address.scope.kind,
  ownerId: scopeOwner(address.scope),
  family: address.key === undefined ? 0 : 1,
  keyValue: encodeIndexed(address.key ?? ""),
});

/** The ID condition, its parameter and the `ORDER BY` direction of a table scan. */
const scanSql = (start: ScanStart) =>
  start.order === "ascending"
    ? { clause: "id > ?", param: start.after ?? -1, direction: "ASC" }
    : { clause: "id < ?", param: start.after ?? Number.MAX_SAFE_INTEGER, direction: "DESC" };

const filter = <A>(clause: string, value: A | undefined, bind: (value: A) => SqlValue) =>
  value === undefined ? [] : [[clause, bind(value)] as const];
const same = <A extends SqlValue>(value: A): SqlValue => value;
const flag = (value: boolean): SqlValue => (value ? 1 : 0);

const unknownConversation = (id: ConversationId) =>
  new StorageRequestError({ message: `Unknown conversation: ${id}` });

const make = Effect.fnUntraced(function* (options: SqlAgentStorageOptions) {
  const prefix = yield* Effect.fromResult(validatePrefix(options.prefix ?? "durable_"));
  yield* applyMigrations(prefix, migrations(prefix));
  const client = yield* SqlClient.SqlClient;
  // SQL runs without the client's name transforms, so a consumer's transforms
  // cannot break the raw column names.
  const sql = client.withoutTransforms();
  const t = tables(prefix);

  const all = <A extends object>(text: string, params: ReadonlyArray<SqlValue> = []) =>
    sql
      .unsafe<A>(text, params)
      .pipe(Effect.catchTag("SqlError", (error) => Effect.fail(backend(error))));
  const transaction = <A, E>(effect: Effect.Effect<A, E>) =>
    sql
      .withTransaction(effect)
      .pipe(Effect.catchIf(isSqlError, (error) => Effect.fail(backend(error))));
  const first = <A extends object>(text: string, params: ReadonlyArray<SqlValue> = []) =>
    Effect.map(all<A>(text, params), (rows) => Option.fromUndefinedOr(rows[0]));

  const readMetadata = Effect.flatMap(
    first<MetadataRow>(`SELECT next_id, next_seq FROM ${t.metadata} WHERE singleton = 1`),
    Option.match({
      onNone: () => Effect.fail(corrupted("Durable SQLite metadata is missing")),
      onSome: Effect.succeed,
    }),
  );

  const metadata = yield* readMetadata;
  let nextId = Number(metadata.next_id);
  const gate = yield* openGate("SqlAgentStorage");
  yield* Effect.addFinalizer(() => gate.close);

  const readConversation = (id: number) =>
    Effect.map(
      first<JsonRow>(`SELECT record FROM ${t.conversations} WHERE id = ?`, [id]),
      Option.map((row) => parse<ConversationRecord>(row.record)),
    );

  const requireConversation = (id: ConversationId) =>
    Effect.flatMap(
      readConversation(id),
      Option.match({
        onNone: () => Effect.fail(unknownConversation(id)),
        onSome: Effect.succeed,
      }),
    );

  /** Materialize one incarnation; the caller runs it in a transaction so every query sees one state. */
  const materialize = Effect.fnUntraced(function* (id: DocumentId, at: DocumentPoint) {
    const row = yield* first<JsonRow>(`SELECT record FROM ${t.documents} WHERE id = ?`, [id]);
    if (Option.isNone(row)) return Option.none<StoredDocument>();
    const record = parse<DocumentRecord>(row.value.record);
    if (at !== "current" && isCurrentOnly(record)) {
      return yield* new StorageRequestError({
        message: `Document ${id} does not retain historical content`,
      });
    }
    if (!isAliveAt(record, at)) return Option.none<StoredDocument>();
    const upper = at === "current" ? Number.MAX_SAFE_INTEGER : at;
    const base = yield* first<RevisionRow>(
      `SELECT seq, kind, version, content FROM ${t.revisions}
        WHERE document_id = ? AND kind = 'base' AND seq <= ? ORDER BY seq DESC LIMIT 1`,
      [id, upper],
    );
    if (Option.isNone(base))
      return yield* corrupted(`Document ${id} is missing a required base`, id);
    const tail = yield* all<RevisionRow>(
      `SELECT seq, kind, version, content FROM ${t.revisions}
        WHERE document_id = ? AND seq > ? AND seq <= ? ORDER BY seq`,
      [id, base.value.seq, upper],
    );
    const value = yield* Effect.fromResult(
      replay(
        id,
        { version: base.value.version, value: parse(base.value.content) },
        tail.map((revision) => ({
          kind: revision.kind,
          version: revision.version,
          ops: revision.kind === "delta" ? parse(revision.content) : [],
        })),
      ),
    );
    return Option.some<StoredDocument>({
      record,
      version: base.value.version,
      value,
      deltasSinceBase: tail.length,
    });
  });

  const claimId = (id: number, table: TableName) =>
    all(`INSERT OR IGNORE INTO ${t.recordIds} (id, record_type) VALUES (?, ?)`, [id, table]);

  const applyTableWrite = Effect.fnUntraced(function* (write: StorageWrite, seq: Seq) {
    switch (write.type) {
      case "conversation": {
        const value = write.value;
        yield* claimId(value.id, "conversation");
        yield* all(
          `INSERT INTO ${t.conversations} (id, owner_conversation_id, owner_task_id, record) VALUES (?, ?, ?, ?)`,
          [
            value.id,
            value.owner?.conversationId ?? null,
            value.owner?.taskId ?? null,
            yield* encode(value),
          ],
        );
        return;
      }
      case "entry": {
        const value = write.value;
        yield* claimId(value.id, "entry");
        yield* all(
          `INSERT INTO ${t.entries} (id, conversation_id, head, commit_seq, record) VALUES (?, ?, ?, ?, ?)`,
          [value.id, value.conversationId, value.head ?? null, seq, yield* encode(value)],
        );
        return;
      }
      case "task": {
        const value = write.value;
        yield* claimId(value.id, "task");
        yield* all(
          `INSERT INTO ${t.tasks} (id, conversation_id, kind, status, abort_requested, background, record)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET conversation_id = excluded.conversation_id, kind = excluded.kind,
            status = excluded.status, abort_requested = excluded.abort_requested,
            background = excluded.background, record = excluded.record`,
          [
            value.id,
            value.conversationId,
            encodeIndexed(value.kind),
            value.state.status,
            value.abortRequested ? 1 : 0,
            value.background ? 1 : 0,
            yield* encode(value),
          ],
        );
        return;
      }
      case "submission": {
        const value = write.value;
        yield* claimId(value.id, "submission");
        yield* all(
          `INSERT INTO ${t.submissions} (id, conversation_id, request_id, status, record) VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET conversation_id = excluded.conversation_id,
            request_id = excluded.request_id, status = excluded.status, record = excluded.record`,
          [
            value.id,
            value.conversationId,
            value.requestId === undefined ? null : encodeIndexed(value.requestId),
            value.status,
            yield* encode(value),
          ],
        );
        return;
      }
      case "document.create":
      case "document.copy":
      case "document.change":
      case "document.retire":
        return;
    }
  });

  const applyDocumentActions = Effect.fnUntraced(function* (
    actions: ReadonlyMap<DocumentId, DocumentAction>,
    seq: Seq,
  ) {
    for (const [id, action] of actions) {
      let record: DocumentRecord;
      if (action.create !== undefined) {
        record = action.retire
          ? { ...action.create, createdAt: seq, retiredAt: seq }
          : { ...action.create, createdAt: seq };
        const parts = addressParts(record);
        yield* claimId(id, "document");
        yield* all(
          `INSERT INTO ${t.documents}
            (id, kind, family, key_value, scope_kind, owner_id, created_at, retired_at, record)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
          [
            id,
            parts.kind,
            parts.family,
            parts.keyValue,
            parts.scopeKind,
            parts.ownerId,
            seq,
            action.retire ? seq : null,
            yield* encode(record),
          ],
        );
      } else {
        const row = yield* first<JsonRow>(`SELECT record FROM ${t.documents} WHERE id = ?`, [id]);
        if (Option.isNone(row)) return yield* rejected(`Unknown document: ${id}`);
        record = parse<DocumentRecord>(row.value.record);
      }

      const content = action.content;
      if (content !== undefined) {
        if (content.kind === "base" && isCurrentOnly(record)) {
          yield* all(`DELETE FROM ${t.revisions} WHERE document_id = ?`, [id]);
        }
        yield* all(
          `INSERT INTO ${t.revisions} (document_id, seq, kind, version, content) VALUES (?, ?, ?, ?, ?)`,
          [
            id,
            seq,
            content.kind,
            content.version,
            yield* encode(content.kind === "base" ? content.value : content.ops),
          ],
        );
      }

      if (action.retire) {
        if (action.create === undefined) {
          record = { ...record, retiredAt: seq };
          yield* all(`UPDATE ${t.documents} SET retired_at = ?, record = ? WHERE id = ?`, [
            seq,
            yield* encode(record),
            id,
          ]);
        }
        if (isCurrentOnly(record))
          yield* all(`DELETE FROM ${t.revisions} WHERE document_id = ?`, [id]);
      }
    }
  });

  /**
   * The storage's own checks of one batch, copy resolution included, before it writes anything, against the
   * state the earlier batches of the call left.
   */
  const checkBatch = Effect.fnUntraced(function* (
    writes: ReadonlyArray<StorageWrite>,
    actions: Map<DocumentId, DocumentAction>,
  ) {
    yield* checkGlobalIds(writes, (id) =>
      Effect.map(
        first<{ readonly record_type: TableName }>(
          `SELECT record_type FROM ${t.recordIds} WHERE id = ?`,
          [id],
        ),
        (row) => Option.getOrUndefined(row)?.record_type,
      ),
    );
    // A copy reads pre-batch committed state, whatever the order of the writes.
    for (const [id, action] of actions) {
      if (action.copy === undefined || action.create === undefined) continue;
      const stored = yield* materialize(action.copy.id, action.copy.at).pipe(
        Effect.catchTag("StorageRequestError", (error) =>
          Effect.fail(rejected(`Document copy ${id} was rejected: ${error.message}`)),
        ),
      );
      action.content = yield* Effect.fromResult(
        copyContent(id, action.copy, stored, action.create),
      );
    }
    yield* checkDocumentActions(actions, {
      existing: (id) =>
        Effect.gen(function* () {
          const row = yield* first<JsonRow>(`SELECT record FROM ${t.documents} WHERE id = ?`, [id]);
          if (Option.isNone(row)) return Option.none();
          const last = yield* first<{ readonly version: number }>(
            `SELECT version FROM ${t.revisions} WHERE document_id = ? ORDER BY seq DESC LIMIT 1`,
            [id],
          );
          return Option.some({
            record: parse<DocumentRecord>(row.value.record),
            lastVersion: Option.getOrUndefined(last)?.version,
          });
        }),
      hasCurrent: (record) => {
        const parts = addressParts(record);
        return Effect.map(
          first(
            `SELECT id FROM ${t.documents}
            WHERE kind = ? AND scope_kind = ? AND owner_id = ? AND family = ? AND key_value = ?
            AND retired_at IS NULL LIMIT 1`,
            [parts.kind, parts.scopeKind, parts.ownerId, parts.family, parts.keyValue],
          ),
          Option.isSome,
        );
      },
    });
  });

  // `commit` runs inside `sql.withTransaction`, because a host can then wrap it in an outer transaction; a nested transaction is a savepoint. Such a
  // host holds the receipts until its own transaction commits. The Durable Object
  // hosts once set their alarm that way; now they arm it before the commit
  // instead (`@effect-durable-harness/host`, `internal/commit.ts`).
  //
  // The batches of one call go in one transaction, each
  // with its own seq, checked before it writes anything; the first one refused
  // ends the call, and the transaction commits the batches before it. No
  // savepoints, which Durable Objects do not allow. Only the storage's own checks
  // refuse: a failure of the database (`Backend`) is an unknown outcome, so it
  // fails the call and rolls back every batch with the transaction. The database's
  // failures are the `Backend` reason, so the reason tells them apart.
  const commit = Effect.fn("AgentStorage.commit")(
    function* (
      batches: ReadonlyArray<ReadonlyArray<StorageWrite>>,
      receive: (index: number, seq: Seq) => void,
    ) {
      const seqs: Array<Seq> = [];
      let refused: StorageFailure | undefined;
      let candidate = nextId;
      yield* transaction(
        Effect.gen(function* () {
          const metadata = yield* readMetadata;
          let next = metadata.next_seq;
          for (const writes of batches) {
            const checked = yield* Effect.exit(
              Effect.gen(function* () {
                const actions = yield* Effect.fromResult(prepareDocumentActions(writes));
                yield* checkBatch(writes, actions);
                return actions;
              }),
            );
            if (Exit.isFailure(checked)) {
              const failure = Exit.findErrorOption(checked);
              // A failure of the database is an unknown outcome: it fails the call, and the transaction rolls back
              // every batch. Anything else the checks end with, a defect of their own included (a check
              // that throws without a failed database call), refuses this batch and keeps the earlier ones.
              if (Option.isSome(failure) && failure.value.reason._tag === "Backend") {
                return yield* failure.value;
              }
              if (Exit.hasInterrupts(checked) && Option.isNone(failure)) {
                return yield* Effect.failCause(checked.cause);
              }
              refused = Option.isSome(failure)
                ? failure.value
                : corrupted(
                    `A check of the batch failed: ${String(Cause.squash(checked.cause))}`,
                    undefined,
                    Cause.squash(checked.cause),
                  );
              break;
            }
            const actionsOf = checked.value;
            const seq = Seq.make(next++);
            for (const write of writes) yield* applyTableWrite(write, seq);
            yield* applyDocumentActions(actionsOf, seq);
            seqs.push(seq);
            candidate = candidateNextId(candidate, writes);
          }
          if (seqs.length === 0) return;
          yield* all(`UPDATE ${t.metadata} SET next_id = ?, next_seq = ? WHERE singleton = 1`, [
            String(Math.max(Number(metadata.next_id), candidate)),
            next,
          ]);
        }),
      );
      // IDs a commit claims are adopted only after it succeeds (`sqlite-facade.test.ts:171-223`).
      nextId = Math.max(nextId, candidate);
      // The transaction returned: the stored batches are durable.
      for (const [index, seq] of seqs.entries()) receive(index, seq);
      return refused === undefined ? { stored: seqs.length } : { stored: seqs.length, refused };
    },
    gate.admit,
    Effect.uninterruptible,
  );

  const readEntry = Effect.fnUntraced(function* (id: number) {
    const row = yield* first<EntryJsonRow>(
      `SELECT record, commit_seq FROM ${t.entries} WHERE id = ?`,
      [id],
    );
    return Option.map(
      row,
      (found): CommittedEntry => ({
        entry: parse<EntryRecord>(found.record),
        commitSeq: Seq.make(found.commit_seq),
      }),
    );
  });

  const visibleEntry = Effect.fn("AgentStorage.visibleEntry")(function* (
    conversationId: ConversationId,
    id: number,
  ) {
    let conversation = yield* requireConversation(conversationId);
    const found = yield* readEntry(id);
    if (Option.isNone(found)) return found;
    let upperEntryId = Number.POSITIVE_INFINITY;
    while (conversation.id !== found.value.entry.conversationId) {
      if (conversation.parent === undefined) return Option.none<CommittedEntry>();
      upperEntryId = Math.min(upperEntryId, conversation.parent.at);
      const parent = yield* readConversation(conversation.parent.conversationId);
      if (Option.isNone(parent)) return Option.none<CommittedEntry>();
      conversation = parent.value;
    }
    return found.value.entry.id > upperEntryId ? Option.none<CommittedEntry>() : found;
  }, gate.admit);

  const findLatestHeadMarker = Effect.fn("AgentStorage.findLatestHeadMarker")(function* (
    conversationId: ConversationId,
    atOrBefore?: EntryId | undefined,
  ) {
    let conversation = yield* requireConversation(conversationId);
    let upper: number | undefined = atOrBefore;
    while (true) {
      const row =
        upper === undefined
          ? yield* first<JsonRow>(
              `SELECT record FROM ${t.entries} WHERE conversation_id = ? AND head IS NOT NULL ORDER BY id DESC LIMIT 1`,
              [conversation.id],
            )
          : yield* first<JsonRow>(
              `SELECT record FROM ${t.entries} WHERE conversation_id = ? AND head IS NOT NULL AND id <= ? ORDER BY id DESC LIMIT 1`,
              [conversation.id, upper],
            );
      if (Option.isSome(row)) return Option.some(parse<HeadMarker>(row.value.record));
      if (conversation.parent === undefined) return Option.none<HeadMarker>();
      upper =
        upper === undefined ? conversation.parent.at : Math.min(upper, conversation.parent.at);
      const parent = yield* readConversation(conversation.parent.conversationId);
      if (Option.isNone(parent)) return Option.none<HeadMarker>();
      conversation = parent.value;
    }
  }, gate.admit);

  const entryRange = (
    conversationId: number,
    lower: number | undefined,
    upper: number | undefined,
  ) => {
    const clauses = ["conversation_id = ?"];
    const params: Array<SqlValue> = [conversationId];
    if (lower !== undefined) {
      clauses.push("id >= ?");
      params.push(lower);
    }
    if (upper !== undefined) {
      clauses.push("id <= ?");
      params.push(upper);
    }
    return { where: clauses.join(" AND "), params };
  };

  const scanEntries = Effect.fn("AgentStorage.scanEntries")(function* (
    query: EntryQuery,
    limit: number,
    cursor?: Cursor,
  ) {
    const start = yield* Effect.fromResult(scanStart(query.order, cursor, "descending"));
    const after = start.after;
    let conversation = yield* requireConversation(query.conversationId);
    const values: Array<EntryRecord> = [];
    if (start.order === "descending") {
      let upper: number | undefined = query.maxEntryId;
      if (after !== undefined) upper = Math.min(upper ?? Number.MAX_SAFE_INTEGER, after - 1);
      while (true) {
        const range = entryRange(conversation.id, query.minEntryId, upper);
        const rows = yield* all<JsonRow>(
          `SELECT record FROM ${t.entries} WHERE ${range.where} ORDER BY id DESC LIMIT ?`,
          [...range.params, limit + 1 - values.length],
        );
        values.push(...rows.map((row) => parse<EntryRecord>(row.record)));
        if (values.length > limit || conversation.parent === undefined) break;
        upper =
          upper === undefined ? conversation.parent.at : Math.min(upper, conversation.parent.at);
        if (query.minEntryId !== undefined && upper < query.minEntryId) break;
        const parent = yield* readConversation(conversation.parent.conversationId);
        if (Option.isNone(parent)) break;
        conversation = parent.value;
      }
      return page(values, limit, "descending");
    }
    // Oldest first: the fork chain's segments from the root conversation forward, each up to its fork point.
    const segments: Array<{ readonly conversationId: number; readonly upper: number | undefined }> =
      [];
    let upper: number | undefined = query.maxEntryId;
    while (true) {
      segments.push({ conversationId: conversation.id, upper });
      if (conversation.parent === undefined) break;
      upper =
        upper === undefined ? conversation.parent.at : Math.min(upper, conversation.parent.at);
      if (query.minEntryId !== undefined && upper < query.minEntryId) break;
      const parent = yield* readConversation(conversation.parent.conversationId);
      if (Option.isNone(parent)) break;
      conversation = parent.value;
    }
    let lower: number | undefined = query.minEntryId;
    if (after !== undefined) lower = Math.max(lower ?? after + 1, after + 1);
    for (const segment of segments.toReversed()) {
      const range = entryRange(segment.conversationId, lower, segment.upper);
      const rows = yield* all<JsonRow>(
        `SELECT record FROM ${t.entries} WHERE ${range.where} ORDER BY id ASC LIMIT ?`,
        [...range.params, limit + 1 - values.length],
      );
      values.push(...rows.map((row) => parse<EntryRecord>(row.record)));
      if (values.length > limit) break;
    }
    return page(values, limit, "ascending");
  }, gate.admit);

  /** One filtered table scan by ID. */
  const scanTable = Effect.fnUntraced(function* <A extends { readonly id: number }>(
    table: string,
    filters: ReadonlyArray<readonly [string, SqlValue]>,
    order: ScanStart["order"] | undefined,
    cursor: Cursor | undefined,
    limit: number,
  ) {
    const start = yield* Effect.fromResult(scanStart(order, cursor, "ascending"));
    const scan = scanSql(start);
    const clauses = [scan.clause, ...filters.map(([clause]) => clause)];
    const rows = yield* all<JsonRow>(
      `SELECT record FROM ${table} WHERE ${clauses.join(" AND ")} ORDER BY id ${scan.direction} LIMIT ?`,
      [scan.param, ...filters.map(([, value]) => value), limit + 1],
    );
    return page(
      rows.map((row) => parse<A>(row.record)),
      limit,
      start.order,
    ) satisfies Page<A>;
  });

  const readRecord = <A>(table: string, id: number) =>
    Effect.map(
      first<JsonRow>(`SELECT record FROM ${table} WHERE id = ?`, [id]),
      Option.map((row) => parse<A>(row.record)),
    );

  return AgentStorage.of({
    commit,

    // `mintId` is an Effect value, not a method, so it is traced with `withSpan`.
    mintId: Effect.gen(function* () {
      const id = yield* Effect.fromResult(mintFrom(nextId));
      nextId = id + 1;
      return id;
    }).pipe(gate.admit, Effect.withSpan("AgentStorage.mintId")),

    conversation: Effect.fn("AgentStorage.conversation")(function* (id: ConversationId) {
      return yield* readConversation(id);
    }, gate.admit),

    scanConversations: Effect.fn("AgentStorage.scanConversations")(function* (
      query: ConversationQuery,
      limit: number,
      cursor?: Cursor | undefined,
    ) {
      return yield* scanTable<ConversationRecord>(
        t.conversations,
        [
          ...filter("owner_conversation_id = ?", query.ownerConversationId, same),
          ...filter("owner_task_id = ?", query.ownerTaskId, same),
        ],
        query.order,
        cursor,
        limit,
      );
    }, gate.admit),

    entry: Effect.fn("AgentStorage.entry")(function* (id: EntryId) {
      return yield* readEntry(id);
    }, gate.admit),

    visibleEntry,
    findLatestHeadMarker,
    scanEntries,

    task: Effect.fn("AgentStorage.task")(function* (id: TaskId) {
      return yield* readRecord<TaskRecord>(t.tasks, id);
    }, gate.admit),

    scanTasks: Effect.fn("AgentStorage.scanTasks")(function* (
      query: TaskQuery,
      limit: number,
      cursor?: Cursor | undefined,
    ) {
      return yield* scanTable<TaskRecord>(
        t.tasks,
        [
          ...filter("conversation_id = ?", query.conversationId, same),
          ...filter("kind = ?", query.kind, encodeIndexed),
          ...filter("status = ?", query.status, same),
          ...filter("abort_requested = ?", query.abortRequested, flag),
          ...filter("background = ?", query.background, flag),
        ],
        query.order,
        cursor,
        limit,
      );
    }, gate.admit),

    submission: Effect.fn("AgentStorage.submission")(function* (id: SubmissionId) {
      return yield* readRecord<SubmissionRecord>(t.submissions, id);
    }, gate.admit),

    scanSubmissions: Effect.fn("AgentStorage.scanSubmissions")(function* (
      query: SubmissionQuery,
      limit: number,
      cursor?: Cursor | undefined,
    ) {
      return yield* scanTable<SubmissionRecord>(
        t.submissions,
        [
          ...filter("conversation_id = ?", query.conversationId, same),
          ...filter("status = ?", query.status, same),
        ],
        query.order,
        cursor,
        limit,
      );
    }, gate.admit),

    submissionByRequest: Effect.fn("AgentStorage.submissionByRequest")(function* (
      conversationId: ConversationId,
      requestId: string,
    ) {
      const row = yield* first<JsonRow>(
        `SELECT record FROM ${t.submissions} WHERE conversation_id = ? AND request_id = ?`,
        [conversationId, encodeIndexed(requestId)],
      );
      return Option.map(row, (found) => parse<SubmissionRecord>(found.record));
    }, gate.admit),

    findDocument: Effect.fn("AgentStorage.findDocument")(function* (
      address: DocumentAddress,
      at: DocumentPoint,
    ) {
      const parts = addressParts(address);
      const params: Array<SqlValue> = [
        parts.kind,
        parts.scopeKind,
        parts.ownerId,
        parts.family,
        parts.keyValue,
      ];
      const lifetime =
        at === "current"
          ? "retired_at IS NULL"
          : "created_at <= ? AND (retired_at IS NULL OR retired_at > ?)";
      if (at !== "current") params.push(at, at);
      const row = yield* first<JsonRow>(
        `SELECT record FROM ${t.documents}
          WHERE kind = ? AND scope_kind = ? AND owner_id = ? AND family = ? AND key_value = ?
          AND ${lifetime} ORDER BY created_at DESC LIMIT 1`,
        params,
      );
      return Option.map(row, (found) => parse<DocumentRecord>(found.record));
    }, gate.admit),

    document: Effect.fn("AgentStorage.document")(function* (id: DocumentId, at: DocumentPoint) {
      // The record and revision queries must observe one
      // committed state; a commit between them can replace the base.
      return yield* transaction(materialize(id, at));
    }, gate.admit),

    scanDocuments: Effect.fn("AgentStorage.scanDocuments")(function* (
      query: DocumentQuery,
      limit: number,
      cursor?: Cursor | undefined,
    ) {
      const start = yield* Effect.fromResult(scanStart("ascending", cursor, "ascending"));
      const clauses = ["scope_kind = ?", "owner_id = ?", "id > ?"];
      const params: Array<SqlValue> = [
        query.scope.kind,
        scopeOwner(query.scope),
        start.after ?? -1,
      ];
      if (query.kind !== undefined) {
        clauses.push("kind = ?");
        params.push(encodeIndexed(query.kind));
      }
      if (query.at === "current") {
        clauses.push("retired_at IS NULL");
      } else {
        clauses.push("created_at <= ?", "(retired_at IS NULL OR retired_at > ?)");
        params.push(query.at, query.at);
      }
      const rows = yield* all<JsonRow>(
        `SELECT record FROM ${t.documents} WHERE ${clauses.join(" AND ")} ORDER BY id LIMIT ?`,
        [...params, limit + 1],
      );
      return page(
        rows.map((row) => parse<DocumentRecord>(row.record)),
        limit,
        "ascending",
      );
    }, gate.admit),
  });
});

/**
 * `AgentStorage` over the `SqlClient` in context. Building the layer applies
 * pending migrations atomically; closing its scope refuses new calls and waits
 * for admitted ones. The client belongs to
 * the caller, which can share its database with other tables through
 * `prefix`.
 *
 * @category layers
 * @since 0.0.0
 */
export const layer = (
  options: SqlAgentStorageOptions = {},
): Layer.Layer<AgentStorage, StorageFailure, SqlClient.SqlClient> =>
  Layer.effect(AgentStorage, make(options));

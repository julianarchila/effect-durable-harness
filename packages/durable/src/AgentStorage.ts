/**
 * The storage contract every Session sits on: `commit` of batches of records
 * and document commands, each atomic with its own seq, global ID minting, and
 * reads.
 *
 * @since 0.0.0
 */
import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";
import type * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import {
  type ConversationId,
  DocumentId,
  type EntryId,
  Seq,
  type SubmissionId,
  type TaskId,
} from "./Ids.ts";
import {
  ConversationRecord,
  DeltaOperations,
  DocumentCreate,
  type DocumentRecord,
  type DocumentScope,
  EntryRecord,
  SubmissionRecord,
  type SubmissionStatus,
  TaskRecord,
  type TaskStatus,
} from "./Records.ts";

// -----------------------------------------------------------------------------
// errors
// -----------------------------------------------------------------------------

/**
 * Reason of a `StorageFailure`: the batch breaks a storage invariant, such as
 * an ID written twice or a second current incarnation at one address.
 *
 * @category errors
 * @since 0.0.0
 */
export class Rejected extends Schema.TaggedError<Rejected>()("Rejected", {
  message: Schema.String,
  cause: Schema.optional(Schema.Defect()),
}) {}

/**
 * Reason of a `StorageFailure`: persisted state cannot be read back, such as
 * missing metadata, a document without its base, a delta tail that crosses a
 * version, or a schema newer than this code supports.
 *
 * @category errors
 * @since 0.0.0
 */
export class Corrupted extends Schema.TaggedError<Corrupted>()("Corrupted", {
  message: Schema.String,
  /** The document whose stored revisions cannot be replayed, when one can be named. */
  documentId: Schema.optional(DocumentId),
  cause: Schema.optional(Schema.Defect()),
}) {}

/**
 * Reason of a `StorageFailure`: the storage layer's scope has closed, so every
 * later operation fails.
 *
 * @category errors
 * @since 0.0.0
 */
export class Closed extends Schema.TaggedError<Closed>()("Closed", {
  message: Schema.String,
}) {}

/**
 * Reason of a `StorageFailure`: no safe integer ID is left to mint.
 *
 * @category errors
 * @since 0.0.0
 */
export class IdSpaceExhausted extends Schema.TaggedError<IdSpaceExhausted>()("IdSpaceExhausted", {
  message: Schema.String,
}) {}

/**
 * Reason of a `StorageFailure`: the database underneath failed. `cause` is the
 * driver's error, such as an `SqlError`.
 *
 * @category errors
 * @since 0.0.0
 */
export class Backend extends Schema.TaggedError<Backend>()("Backend", {
  message: Schema.String,
  cause: Schema.Defect(),
}) {}

/**
 * The reasons a storage operation can fail.
 *
 * @category errors
 * @since 0.0.0
 */
export type StorageFailureReason = Rejected | Corrupted | Closed | IdSpaceExhausted | Backend;

/**
 * A storage operation failed. It is fatal to the Session that owns the
 * storage, which observes it and becomes
 * `failed`. Handle one reason with `Effect.catchReason`.
 *
 * @category errors
 * @since 0.0.0
 */
export class StorageFailure extends Schema.TaggedError<StorageFailure>()("StorageFailure", {
  reason: Schema.Union([Rejected, Corrupted, Closed, IdSpaceExhausted, Backend]),
}) {
  override readonly cause = this.reason;

  override get message(): string {
    return this.reason.message;
  }
}

/**
 * A read rejected an invalid request with no durable effect: an unknown
 * conversation, a foreign or malformed cursor, or history a document does not
 * keep. Unlike `StorageFailure`, it fails only that read,
 * so callers handle it on its own with `Effect.catchTag`.
 *
 * @category errors
 * @since 0.0.0
 */
export class StorageRequestError extends Schema.TaggedError<StorageRequestError>()(
  "StorageRequestError",
  { message: Schema.String },
) {}

// -----------------------------------------------------------------------------
// writes
// -----------------------------------------------------------------------------

/**
 * A complete document checkpoint at a definition version.
 *
 * @category writes
 * @since 0.0.0
 */
export const BaseContent = Schema.Struct({
  version: Schema.Number,
  kind: Schema.Literal("base"),
  value: Schema.JsonObject,
});

/**
 * @category writes
 * @since 0.0.0
 */
export type BaseContent = typeof BaseContent.Type;

/**
 * A delta operation batch applied to the newest stored value. It keeps the
 * stored version: a version change must be a base.
 *
 * @category writes
 * @since 0.0.0
 */
export const DeltaContent = Schema.Struct({
  version: Schema.Number,
  kind: Schema.Literal("delta"),
  ops: DeltaOperations,
});

/**
 * @category writes
 * @since 0.0.0
 */
export type DeltaContent = typeof DeltaContent.Type;

/**
 * The content of a document change: a checkpoint or an operation batch, as
 * the owning Session selects.
 *
 * @category writes
 * @since 0.0.0
 */
export const DocumentContent = Schema.Union([BaseContent, DeltaContent]);

/**
 * @category writes
 * @since 0.0.0
 */
export type DocumentContent = typeof DocumentContent.Type;

/**
 * The point of a document read: the current state, or the state after one
 * commit sequence.
 *
 * @category writes
 * @since 0.0.0
 */
export const DocumentPoint = Schema.Union([Seq, Schema.Literal("current")]);

/**
 * @category writes
 * @since 0.0.0
 */
export type DocumentPoint = typeof DocumentPoint.Type;

/**
 * The exact persisted source of a definition-free document copy.
 *
 * @category writes
 * @since 0.0.0
 */
export const DocumentCopySource = Schema.Struct({ id: DocumentId, at: DocumentPoint });

/**
 * @category writes
 * @since 0.0.0
 */
export type DocumentCopySource = typeof DocumentCopySource.Type;

/**
 * One record or document mutation in an atomic commit, told apart by `type`.
 *
 * @category writes
 * @since 0.0.0
 */
export const StorageWrite = Schema.Union([
  Schema.Struct({ type: Schema.Literal("conversation"), value: ConversationRecord }),
  Schema.Struct({ type: Schema.Literal("entry"), value: EntryRecord }),
  Schema.Struct({ type: Schema.Literal("task"), value: TaskRecord }),
  Schema.Struct({ type: Schema.Literal("submission"), value: SubmissionRecord }),
  Schema.Struct({
    type: Schema.Literal("document.create"),
    record: DocumentCreate,
    content: BaseContent,
  }),
  Schema.Struct({
    type: Schema.Literal("document.copy"),
    record: DocumentCreate,
    source: DocumentCopySource,
  }),
  Schema.Struct({
    type: Schema.Literal("document.change"),
    id: DocumentId,
    content: DocumentContent,
  }),
  Schema.Struct({ type: Schema.Literal("document.retire"), id: DocumentId }),
]);

/**
 * @category writes
 * @since 0.0.0
 */
export type StorageWrite = typeof StorageWrite.Type;

// -----------------------------------------------------------------------------
// reads
// -----------------------------------------------------------------------------

/**
 * ID order of a scan: `ascending` is oldest first.
 *
 * @category reads
 * @since 0.0.0
 */
export type ScanOrder = "ascending" | "descending";

/**
 * Backend-owned JSON continuation state that callers only round-trip to the
 * same scan. It carries the scan's order; the built-in backends use
 * `{ after, order }`.
 *
 * @category reads
 * @since 0.0.0
 */
export type Cursor = Schema.JsonObject;

/**
 * One ordered scan result and its continuation, absent on the last page.
 *
 * @category reads
 * @since 0.0.0
 */
// `next` is an `Option` instead of an optional field, because effect-durable-harness's lookups return `Option`
// for absence.
export interface Page<A> {
  readonly items: ReadonlyArray<A>;
  readonly next: Option.Option<Cursor>;
}

/**
 * Filters for a conversation scan; every filter given must match.
 *
 * @category reads
 * @since 0.0.0
 */
export interface ConversationQuery {
  readonly ownerConversationId?: ConversationId | undefined;
  readonly ownerTaskId?: TaskId | undefined;
  /** Default `ascending`; with a cursor, the cursor's order. */
  readonly order?: ScanOrder | undefined;
}

/**
 * Inclusive ID bounds for a scan of one conversation's fork-aware history.
 *
 * @category reads
 * @since 0.0.0
 */
export interface EntryQuery {
  readonly conversationId: ConversationId;
  readonly minEntryId?: EntryId | undefined;
  readonly maxEntryId?: EntryId | undefined;
  /** Default `descending`; with a cursor, the cursor's order. */
  readonly order?: ScanOrder | undefined;
}

/**
 * Filters for a task scan; every filter given must match.
 *
 * @category reads
 * @since 0.0.0
 */
export interface TaskQuery {
  readonly conversationId?: ConversationId | undefined;
  readonly kind?: string | undefined;
  readonly status?: TaskStatus | undefined;
  readonly abortRequested?: boolean | undefined;
  readonly background?: boolean | undefined;
  /** Default `ascending`; with a cursor, the cursor's order. */
  readonly order?: ScanOrder | undefined;
}

/**
 * Filters for a submission scan; every filter given must match.
 *
 * @category reads
 * @since 0.0.0
 */
export interface SubmissionQuery {
  readonly conversationId?: ConversationId | undefined;
  readonly status?: SubmissionStatus | undefined;
  /** Default `ascending`; with a cursor, the cursor's order. */
  readonly order?: ScanOrder | undefined;
}

/**
 * The exact logical identity of a singleton document or one family member.
 *
 * @category reads
 * @since 0.0.0
 */
export interface DocumentAddress {
  readonly kind: string;
  readonly scope: DocumentScope;
  /** Absent selects the singleton; present selects one family member. */
  readonly key?: string | undefined;
}

/**
 * A scan of the document incarnations alive in one exact scope at one point,
 * by ascending ID.
 *
 * @category reads
 * @since 0.0.0
 */
export interface DocumentQuery {
  readonly scope: DocumentScope;
  readonly at: DocumentPoint;
  readonly kind?: string | undefined;
}

/**
 * A detached materialized document value and its stored definition version at
 * one point.
 *
 * @category reads
 * @since 0.0.0
 */
export interface StoredDocument {
  readonly record: DocumentRecord;
  readonly version: number;
  readonly value: Schema.JsonObject;
  /** Deltas replayed after the selected base to materialize `value`. */
  readonly deltasSinceBase: number;
}

/**
 * One entry and the sequence of the commit that persisted it.
 *
 * @category reads
 * @since 0.0.0
 */
export interface CommittedEntry {
  readonly entry: EntryRecord;
  readonly commitSeq: Seq;
}

/**
 * An entry whose `head` is set: it marks where the active context starts.
 *
 * @category reads
 * @since 0.0.0
 */
export type HeadMarker = EntryRecord & { readonly head: EntryId };

type Read<A> = Effect.Effect<A, StorageFailure | StorageRequestError>;

/**
 * What `AgentStorage.commit` stored: the number of durable batches, from the
 * first, and why the next one was refused, if one was.
 *
 * A refusal is one of the storage's own checks of a batch (global IDs,
 * document commands, copy resolution), run before that batch writes anything.
 * Any other failure is an unknown outcome, and the call fails instead.
 *
 * @category models
 * @since 0.0.0
 */
export interface CommitResult {
  readonly stored: number;
  // `refused` is the typed `StorageFailure` the check returned, because effect-durable-harness's checks fail
  // with typed reasons (`Rejected`, `Corrupted`).
  readonly refused?: StorageFailure | undefined;
}

// -----------------------------------------------------------------------------
// service
// -----------------------------------------------------------------------------

/**
 * The atomic persistence boundary for Session records. Provide
 * it with `MemoryAgentStorage.layer` or `SqlAgentStorage.layer`.
 *
 * Storage trusts the owning Session to supply valid records, references,
 * ancestry and transitions. It enforces atomicity, global ID ownership,
 * write-once conversations, entries and documents, document record
 * consistency and detached values. The Session serializes commits, so storage
 * adds no commit mutex of its own.
 *
 * @category services
 * @since 0.0.0
 */
export class AgentStorage extends Context.Service<
  AgentStorage,
  {
    // No cancellation parameter on any method and no `close()`, because cancellation is fiber interruption and
    // closing is the end of the layer's `Scope`.
    /**
     * Persists `batches` in order, each as one atomic commit with its own seq, checked against the state the
     * earlier ones left, in as few round trips as the backend can.
     * Stops at the first batch the storage's own checks refuse: the batches before it are durable, it and the
     * ones after are not. Calls `receive(index, seq)` once per durable batch, in order and synchronously, after
     * all of them are durable and before the call settles. Fails for an unknown outcome (a lost connection, a
     * failed statement or `COMMIT`); `receive` calls made before the failure still stand. Uninterruptible once
     * admitted.
     */
    readonly commit: (
      batches: ReadonlyArray<ReadonlyArray<StorageWrite>>,
      receive: (index: number, seq: Seq) => void,
    ) => Effect.Effect<CommitResult, StorageFailure>;
    /** Returns an ID above every ID minted or stored before, also across reopen. */
    readonly mintId: Effect.Effect<number, StorageFailure>;
    /** Returns the conversation with this ID, if committed. */
    readonly conversation: (id: ConversationId) => Read<Option.Option<ConversationRecord>>;
    /** Scans conversations by ID in `query.order` (default ascending). */
    readonly scanConversations: (
      query: ConversationQuery,
      limit: number,
      cursor?: Cursor | undefined,
    ) => Read<Page<ConversationRecord>>;
    // `entry` and `visibleEntry` are separate methods rather than overloads told apart with `typeof`, which
    // `no-runtime-typeof` forbids here.
    /** Returns one entry by global ID and the sequence of the commit that persisted it. */
    readonly entry: (id: EntryId) => Read<Option.Option<CommittedEntry>>;
    /** Returns one entry only when it is visible through the conversation's ancestry. */
    readonly visibleEntry: (
      conversationId: ConversationId,
      id: EntryId,
    ) => Read<Option.Option<CommittedEntry>>;
    /** Returns the newest visible entry with a `head` at or below the optional inclusive cutoff. */
    readonly findLatestHeadMarker: (
      conversationId: ConversationId,
      atOrBefore?: EntryId | undefined,
    ) => Read<Option.Option<HeadMarker>>;
    /** Scans the inclusive visible range in `query.order` (default descending). */
    readonly scanEntries: (
      query: EntryQuery,
      limit: number,
      cursor?: Cursor | undefined,
    ) => Read<Page<EntryRecord>>;
    /** Returns the latest complete record of the task, if committed. */
    readonly task: (id: TaskId) => Read<Option.Option<TaskRecord>>;
    /** Scans tasks by ID in `query.order` (default ascending). */
    readonly scanTasks: (
      query: TaskQuery,
      limit: number,
      cursor?: Cursor | undefined,
    ) => Read<Page<TaskRecord>>;
    /** Returns the latest complete record of the submission, if committed. */
    readonly submission: (id: SubmissionId) => Read<Option.Option<SubmissionRecord>>;
    /** Scans submissions by ID in `query.order` (default ascending). */
    readonly scanSubmissions: (
      query: SubmissionQuery,
      limit: number,
      cursor?: Cursor | undefined,
    ) => Read<Page<SubmissionRecord>>;
    /** Returns the submission with this conversation-scoped deduplication key, if any. */
    readonly submissionByRequest: (
      conversationId: ConversationId,
      requestId: string,
    ) => Read<Option.Option<SubmissionRecord>>;
    /** Returns the incarnation occupying one exact logical address at the point. */
    readonly findDocument: (
      address: DocumentAddress,
      at: DocumentPoint,
    ) => Read<Option.Option<DocumentRecord>>;
    /** Materializes one incarnation at the point, never following a replacement. */
    readonly document: (id: DocumentId, at: DocumentPoint) => Read<Option.Option<StoredDocument>>;
    /** Scans the incarnations alive in one exact scope at the point, by ascending ID. */
    readonly scanDocuments: (
      query: DocumentQuery,
      limit: number,
      cursor?: Cursor | undefined,
    ) => Read<Page<DocumentRecord>>;
  }
>()("@effect-durable-harness/core/AgentStorage") {}

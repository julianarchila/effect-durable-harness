/**
 * The transaction of one Session commit. `Tx` exists
 * only while a commit callback runs: the callback's only requirement is `Tx`,
 * so it cannot reach a model, a tool or the network.
 *
 * Table reads are allowed only before the first table write
 * (`ReadAfterWrite`). Document access stays available after it, and reads
 * the transaction's own writes. Every draft `doc` returns is revoked when the
 * callback settles.
 *
 * @since 0.0.0
 */
import type * as Tracker from "@effect-durable-harness/delta/Tracker";
import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";
import type * as Option from "effect/Option";
import type * as Schema from "effect/Schema";
import type {
  ConversationQuery,
  Cursor,
  EntryQuery,
  HeadMarker,
  Page,
  StorageFailure,
  StorageRequestError,
  TaskQuery,
} from "./AgentStorage.ts";
import type {
  ConversationDoc,
  ConversationDocFamily,
  SessionDoc,
  SessionDocFamily,
  TaskDoc,
  TaskDocFamily,
} from "./Doc.ts";
import type { EntryDraft, EntryKind, TypedEntry, TypedEntryDraft } from "./Entry.ts";
import type { ConversationId, EntryId, SubmissionId, TaskId } from "./Ids.ts";
import type { ConversationRecord, EntryRecord, SubmissionRecord, TaskRecord } from "./Records.ts";
import type {
  CommitRejected,
  DocumentAccessError,
  ReadAfterWrite,
  SessionFailed,
} from "./SessionError.ts";
import type { Signal, SignalResult } from "./Signal.ts";
import type { Task, TaskOptions } from "./Task.ts";

/**
 * Ownership a caller selects explicitly whenever a conversation is created.
 *
 * @category models
 * @since 0.0.0
 */
export type ConversationOwnership =
  | { readonly kind: "ownerless" }
  | { readonly kind: "task"; readonly taskId: TaskId };

/**
 * Options of `tx.createConversation` and `tx.forkConversation`.
 *
 * @category models
 * @since 0.0.0
 */
export interface ConversationCreateOptions {
  readonly ownership: ConversationOwnership;
}

/**
 * The fields of a new submission record; the Session assigns `id`.
 *
 * @category models
 * @since 0.0.0
 */
export type SubmissionCreate = SubmissionRecord extends infer R
  ? R extends SubmissionRecord
    ? Omit<R, "id">
    : never
  : never;

/**
 * A terminal status staged for a submission; identity, type and entry come
 * from its current record.
 *
 * @category models
 * @since 0.0.0
 */
export type SubmissionSettlement =
  | { readonly status: "done"; readonly answer: EntryId }
  | {
      readonly status: "unanswered";
      readonly reason: string;
      readonly detail?: Schema.Json | undefined;
    };

/**
 * Errors of a `Tx` table read.
 *
 * @category errors
 * @since 0.0.0
 */
export type TxReadError =
  | ReadAfterWrite
  | CommitRejected
  | StorageRequestError
  | StorageFailure
  | SessionFailed;

/**
 * Errors of a `Tx` table write.
 *
 * @category errors
 * @since 0.0.0
 */
export type TxWriteError = CommitRejected | StorageRequestError | StorageFailure | SessionFailed;

/**
 * Errors of a `Tx` document access.
 *
 * @category errors
 * @since 0.0.0
 */
export type TxDocError = TxWriteError | DocumentAccessError;

/**
 * `tx.doc`: get-or-create the document at one address and return its draft.
 * One overload per scope and family shape.
 *
 * @category models
 * @since 0.0.0
 */
export interface DocAcquire {
  <T>(doc: SessionDoc<T>): Effect.Effect<Tracker.Draft<T>, TxDocError>;
  <T>(
    doc: ConversationDoc<T>,
    conversationId: ConversationId,
  ): Effect.Effect<Tracker.Draft<T>, TxDocError>;
  <T>(doc: TaskDoc<T>, taskId: TaskId): Effect.Effect<Tracker.Draft<T>, TxDocError>;
  <T, I>(
    doc: SessionDocFamily<T, I>,
    key: string,
    seed: NoInfer<I>,
  ): Effect.Effect<Tracker.Draft<T>, TxDocError>;
  <T, I>(
    doc: ConversationDocFamily<T, I>,
    conversationId: ConversationId,
    key: string,
    seed: NoInfer<I>,
  ): Effect.Effect<Tracker.Draft<T>, TxDocError>;
  <T, I>(
    doc: TaskDocFamily<T, I>,
    taskId: TaskId,
    key: string,
    seed: NoInfer<I>,
  ): Effect.Effect<Tracker.Draft<T>, TxDocError>;
}

/**
 * `tx.retireDoc`: retire the document at one address, if it exists, without
 * creating it.
 *
 * @category models
 * @since 0.0.0
 */
export interface DocRetire {
  <T>(doc: SessionDoc<T>): Effect.Effect<void, TxDocError>;
  <T>(doc: ConversationDoc<T>, conversationId: ConversationId): Effect.Effect<void, TxDocError>;
  <T>(doc: TaskDoc<T>, taskId: TaskId): Effect.Effect<void, TxDocError>;
  <T, I>(doc: SessionDocFamily<T, I>, key: string): Effect.Effect<void, TxDocError>;
  <T, I>(
    doc: ConversationDocFamily<T, I>,
    conversationId: ConversationId,
    key: string,
  ): Effect.Effect<void, TxDocError>;
  <T, I>(doc: TaskDocFamily<T, I>, taskId: TaskId, key: string): Effect.Effect<void, TxDocError>;
}

/**
 * `tx.entry`: one committed entry by ID, optionally narrowed to a kind.
 *
 * @category models
 * @since 0.0.0
 */
export interface EntryRead {
  (id: EntryId): Effect.Effect<Option.Option<EntryRecord>, TxReadError>;
  <D extends Schema.Json>(
    kind: EntryKind<D>,
    id: EntryId,
  ): Effect.Effect<Option.Option<TypedEntry<D>>, TxReadError>;
}

/**
 * `tx.appendEntry`: append one entry, raw or of a typed kind.
 *
 * @category models
 * @since 0.0.0
 */
export interface EntryAppend {
  (conversationId: ConversationId, draft: EntryDraft): Effect.Effect<EntryRecord, TxWriteError>;
  <D extends Schema.Json>(
    kind: EntryKind<D>,
    conversationId: ConversationId,
    draft: TypedEntryDraft<NoInfer<D>>,
  ): Effect.Effect<TypedEntry<D>, TxWriteError>;
}

/**
 * The transaction of the running commit callback.
 *
 * @category services
 * @since 0.0.0
 */
// Lookups return `Option`.
export class Tx extends Context.Service<
  Tx,
  {
    /** Committed conversation; a table read. */
    readonly conversation: (
      id: ConversationId,
    ) => Effect.Effect<Option.Option<ConversationRecord>, TxReadError>;
    /** Committed entry by global ID, none when absent or of another kind; a table read. */
    readonly entry: EntryRead;
    /** Committed task record; a table read. */
    readonly task: (id: TaskId) => Effect.Effect<Option.Option<TaskRecord>, TxReadError>;
    /** One page of committed conversations; a table read. */
    readonly scanConversations: (
      query: ConversationQuery,
      limit: number,
      cursor?: Cursor | undefined,
    ) => Effect.Effect<Page<ConversationRecord>, TxReadError>;
    /** One page of a conversation's committed fork-aware history; a table read. */
    readonly scanEntries: (
      query: EntryQuery,
      limit: number,
      cursor?: Cursor | undefined,
    ) => Effect.Effect<Page<EntryRecord>, TxReadError>;
    /** One page of committed tasks; a table read. */
    readonly scanTasks: (
      query: TaskQuery,
      limit: number,
      cursor?: Cursor | undefined,
    ) => Effect.Effect<Page<TaskRecord>, TxReadError>;
    /** Newest visible committed entry of the conversation that carries a `head`; a table read. */
    readonly latestHeadMarker: (
      conversationId: ConversationId,
    ) => Effect.Effect<Option.Option<HeadMarker>, TxReadError>;
    /** Committed submission by ID; a table read. */
    readonly submission: (
      id: SubmissionId,
    ) => Effect.Effect<Option.Option<SubmissionRecord>, TxReadError>;
    /** Committed submission with a conversation-scoped request ID; a table read. */
    readonly submissionByRequest: (
      conversationId: ConversationId,
      requestId: string,
    ) => Effect.Effect<Option.Option<SubmissionRecord>, TxReadError>;
    /** Stages a new conversation with a minted ID; returns an inert record. */
    readonly createConversation: (
      options: ConversationCreateOptions,
    ) => Effect.Effect<ConversationRecord, TxDocError>;
    /** Stages a conversation that inherits `parent`'s history through the visible entry `at`. */
    readonly forkConversation: (
      parent: ConversationId,
      at: EntryId,
      options: ConversationCreateOptions,
    ) => Effect.Effect<ConversationRecord, TxDocError>;
    /** Stages an immutable entry with a minted ID. */
    readonly appendEntry: EntryAppend;
    /**
     * Stages a raw submission record with a minted ID. No admission rules apply: no busy check, no inbox,
     * no placement; use `Conversation.submit` unless the caller implements admission itself.
     */
    readonly createSubmission: (
      create: SubmissionCreate,
    ) => Effect.Effect<SubmissionRecord, TxWriteError>;
    /**
     * Settles a queued or placed submission; only a placed input or continue can be answered, and a settled one stays
     * as it is. Resolved during assembly against this transaction's latest record of it, so it is not a
     * table read and works after table writes.
     */
    readonly settleSubmission: (
      id: SubmissionId,
      settlement: SubmissionSettlement,
    ) => Effect.Effect<void, CommitRejected>;
    /** Places a queued submission at `entry`: an input becomes `placed`, a write `done`. Resolved as `settleSubmission`. */
    readonly placeSubmission: (
      id: SubmissionId,
      entry: EntryId,
    ) => Effect.Effect<void, CommitRejected>;
    /**
     * Delivers `value` to task `taskId` under `key`: stored in the task until a sleep on the key wakes on it,
     * also when the task does not sleep on it yet. A table read, so it fails with `ReadAfterWrite` after the
     * commit's first table write; the result is provisional until the commit is stored. Checks nothing about
     * the caller.
     */
    readonly signal: <T>(
      taskId: TaskId,
      signal: Signal<T>,
      key: string,
      value: T,
    ) => Effect.Effect<SignalResult, TxReadError>;
    /** Stages a pending task with a minted ID and its definition's first checkpoint. */
    readonly createTask: <I extends Schema.Json, S extends { readonly phase: string }, R, Req>(
      task: Task<I, S, R, Req>,
      input: NoInfer<I>,
      options: TaskOptions,
    ) => Effect.Effect<TaskId<R>, TxWriteError>;
    /** Get-or-create one document and return its draft; memoized per address in this transaction. */
    readonly doc: DocAcquire;
    /** Retire one document without creating it; an acquired draft's final content is written first. */
    readonly retireDoc: DocRetire;
  }
>()("@effect-durable-harness/core/Tx") {}

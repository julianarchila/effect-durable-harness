/**
 * The errors of a Session and its transactions.
 *
 * Misuses of a transaction or a document token are grouped by boundary:
 * `CommitRejected` for what rolls a commit back before storage
 * admission, `DocumentAccessError` for typed document access, plus
 * `ReadAfterWrite`, `SessionFailed` and `SessionClosed`.
 *
 * @since 0.0.0
 */
import * as Schema from "effect/Schema";
import { ConversationId } from "./Ids.ts";

// -----------------------------------------------------------------------------
// session lifetime
// -----------------------------------------------------------------------------

/**
 * The Session failed, so nothing runs or commits any more. It fails when a
 * storage operation fails (other than an invalid read) or a commit cannot be
 * adopted in memory; `cause` is that first error. Build a new Session over the
 * same storage: it recovers from what was committed.
 *
 * @category errors
 * @since 0.0.0
 */
export class SessionFailed extends Schema.TaggedError<SessionFailed>()("SessionFailed", {
  cause: Schema.Defect(),
}) {
  override get message(): string {
    return "Session failed after a storage error; close and reopen it";
  }
}

/**
 * The Session is closing or closed: it admits no new commit, read, state or
 * watch.
 *
 * @category errors
 * @since 0.0.0
 */
export class SessionClosed extends Schema.TaggedError<SessionClosed>()("SessionClosed", {}) {
  override get message(): string {
    return "Session is closed";
  }
}

/**
 * The Harness was opened read-only (`HarnessOptions.readOnly`): a commit with writes, or a call that would
 * run tasks, was refused. Nothing was written, and the Harness stays usable for reads.
 *
 * @category errors
 * @since 0.0.0
 */
export class ReadOnlySession extends Schema.TaggedError<ReadOnlySession>()("ReadOnlySession", {
  /** What was refused: `a commit with writes`, `running tasks`. */
  what: Schema.String,
}) {
  override get message(): string {
    return `Read-only Harness: ${this.what} refused`;
  }
}

// -----------------------------------------------------------------------------
// transactions
// -----------------------------------------------------------------------------

/**
 * A transaction read a table after its first table write. Read every required
 * row before writing.
 *
 * @category errors
 * @since 0.0.0
 */
export class ReadAfterWrite extends Schema.TaggedError<ReadAfterWrite>()("ReadAfterWrite", {
  /** The `Tx` method that read, or a qualified operation such as `Conversation.admit`. */
  method: Schema.String,
}) {
  override get message(): string {
    const operation = this.method.includes(".") ? this.method : `Tx.${this.method}`;
    return `${operation}() cannot read tables after the first table write`;
  }
}

/**
 * Reason of a `CommitRejected`: the transaction was used after its callback
 * settled, or the callback settled while one of its operations was pending.
 *
 * @category errors
 * @since 0.0.0
 */
export class Settled extends Schema.TaggedError<Settled>()("Settled", {
  message: Schema.String,
}) {}

/**
 * Reason of a `CommitRejected`: a referenced conversation, entry, task or
 * submission does not exist.
 *
 * @category errors
 * @since 0.0.0
 */
export class NotFound extends Schema.TaggedError<NotFound>()("NotFound", {
  message: Schema.String,
}) {}

/**
 * Reason of a `CommitRejected`: new owned work names an owner that cannot
 * own it: missing, `completing`, terminal or abort-marked, or a child task
 * that is background or lives in another conversation.
 *
 * @category errors
 * @since 0.0.0
 */
export class InvalidOwner extends Schema.TaggedError<InvalidOwner>()("InvalidOwner", {
  message: Schema.String,
}) {}

/**
 * Reason of a `CommitRejected`: a task write breaks the task record rules,
 * such as changing conversation, replacing a terminal task, or using a task
 * after its terminal candidate.
 *
 * @category errors
 * @since 0.0.0
 */
export class TaskConflict extends Schema.TaggedError<TaskConflict>()("TaskConflict", {
  message: Schema.String,
}) {}

/**
 * Reason of a `CommitRejected`: a submission change breaks the submission
 * lifecycle, such as placing a submission that is not queued or answering one
 * that is not a placed input or continue.
 *
 * @category errors
 * @since 0.0.0
 */
export class SubmissionConflict extends Schema.TaggedError<SubmissionConflict>()(
  "SubmissionConflict",
  { message: Schema.String },
) {}

/**
 * Reason of a `CommitRejected`: a value entering kernel ownership is not
 * strict JSON.
 *
 * @category errors
 * @since 0.0.0
 */
export class InvalidJson extends Schema.TaggedError<InvalidJson>()("InvalidJson", {
  message: Schema.String,
}) {}

/**
 * Reason of a `CommitRejected`: a fork cannot select one unambiguous stored
 * source.
 *
 * @category errors
 * @since 0.0.0
 */
export class ForkConflict extends Schema.TaggedError<ForkConflict>()("ForkConflict", {
  message: Schema.String,
}) {}

/**
 * The reasons a commit is rejected before storage admission.
 *
 * @category errors
 * @since 0.0.0
 */
export type CommitRejectedReason =
  | Settled
  | NotFound
  | InvalidOwner
  | TaskConflict
  | SubmissionConflict
  | InvalidJson
  | ForkConflict;

/**
 * A commit was rejected before storage admission: nothing was written or
 * published, and the Session stays healthy. Handle one reason with
 * `Effect.catchReason`.
 *
 * @category errors
 * @since 0.0.0
 */
export class CommitRejected extends Schema.TaggedError<CommitRejected>()("CommitRejected", {
  reason: Schema.Union([
    Settled,
    NotFound,
    InvalidOwner,
    TaskConflict,
    SubmissionConflict,
    InvalidJson,
    ForkConflict,
  ]),
}) {
  override readonly cause = this.reason;

  override get message(): string {
    return this.reason.message;
  }
}

// -----------------------------------------------------------------------------
// documents
// -----------------------------------------------------------------------------

/**
 * Reason of a `DocumentAccessError`: the token's scope, history or fork
 * disagrees with the stored incarnation.
 *
 * @category errors
 * @since 0.0.0
 */
export class SemanticsMismatch extends Schema.TaggedError<SemanticsMismatch>()(
  "SemanticsMismatch",
  { message: Schema.String },
) {}

/**
 * Reason of a `DocumentAccessError`: the stored version is newer than the
 * token's.
 *
 * @category errors
 * @since 0.0.0
 */
export class NewerVersion extends Schema.TaggedError<NewerVersion>()("NewerVersion", {
  message: Schema.String,
}) {}

/**
 * Reason of a `DocumentAccessError`: the stored version is older than the
 * token's and the token has no `migrate`.
 *
 * @category errors
 * @since 0.0.0
 */
export class MigrationRequired extends Schema.TaggedError<MigrationRequired>()(
  "MigrationRequired",
  { message: Schema.String },
) {}

/**
 * Reason of a `DocumentAccessError`: a value from outside the tracker (a
 * stored value, a fork copy or a migration result) is not strict JSON or does
 * not match the definition's schema.
 *
 * @category errors
 * @since 0.0.0
 */
export class InvalidValue extends Schema.TaggedError<InvalidValue>()("InvalidValue", {
  message: Schema.String,
}) {}

/**
 * Reason of a `DocumentAccessError`: a historical read names an entry that
 * the conversation cannot see.
 *
 * @category errors
 * @since 0.0.0
 */
export class NotVisible extends Schema.TaggedError<NotVisible>()("NotVisible", {
  message: Schema.String,
}) {}

/**
 * The reasons a typed document access fails.
 *
 * @category errors
 * @since 0.0.0
 */
export type DocumentAccessErrorReason =
  | SemanticsMismatch
  | NewerVersion
  | MigrationRequired
  | InvalidValue
  | NotVisible
  | NotFound;

/**
 * A typed document access failed: in a read (`snapshot`,
 * `watchDoc`, `snapshotAsOf`, `doc.require`) or in `tx.doc`. It fails only that
 * access; the Session stays healthy. `NotFound` comes only from `doc.require`.
 *
 * @category errors
 * @since 0.0.0
 */
export class DocumentAccessError extends Schema.TaggedError<DocumentAccessError>()(
  "DocumentAccessError",
  {
    reason: Schema.Union([
      SemanticsMismatch,
      NewerVersion,
      MigrationRequired,
      InvalidValue,
      NotVisible,
      NotFound,
    ]),
  },
) {
  override readonly cause = this.reason;

  override get message(): string {
    return this.reason.message;
  }
}

// -----------------------------------------------------------------------------
// conversations
// -----------------------------------------------------------------------------

/**
 * A conversation a caller required does not exist (`requireConversation`).
 *
 * @category errors
 * @since 0.0.0
 */
export class ConversationNotFound extends Schema.TaggedError<ConversationNotFound>()(
  "ConversationNotFound",
  { conversationId: ConversationId },
) {
  override get message(): string {
    return `Conversation ${this.conversationId} does not exist`;
  }
}

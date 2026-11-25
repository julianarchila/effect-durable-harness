/**
 * Submissions: durably admitted user input that may start a run,
 * or a passive entry write. Admission is one commit; a `requestId` makes a
 * resubmission return the existing submission. A busy conversation queues
 * submissions in `core.inbox`, and the run's boundaries place them.
 *
 * @since 0.0.0
 */
import type * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import type { EntryDraft } from "./Entry.ts";
import type { HarnessError } from "./Harness.ts";
import { ConversationId, type SubmissionId } from "./Ids.ts";
import type { InputEntry } from "./Inbox.ts";
import type { UserContent } from "./Message.ts";
import type { SubmissionNotify, SubmissionRecord } from "./Records.ts";
import type { ReadOnlySession } from "./SessionError.ts";
import type { TaskError } from "./TaskError.ts";
import type { TxDocError, TxReadError } from "./Tx.ts";

/**
 * User input that may start a run.
 *
 * @category models
 * @since 0.0.0
 */
export interface InputSubmissionDraft {
  /** Deduplicates within the conversation before any write. */
  readonly requestId?: string | undefined;
  /**
   * When the submission settles, delivers `Signal.SubmissionSettled` to this task under `key`, in the
   * settling commit; a task that refuses it (ended, gone) fails nothing.
   */
  readonly notify?: SubmissionNotify | undefined;
  readonly type: "input";
  readonly content: UserContent;
  /** What a busy conversation does with it; default `followUp`. */
  readonly whenBusy?: "steer" | "followUp" | "reject" | undefined;
  /**
   * What an idle conversation does with it: `"reject"` fails with `ConversationIdle` and writes nothing, so
   * the input never starts a run; default, it is admitted.
   */
  readonly whenIdle?: "reject" | undefined;
  /** Place the input as this entry; default `core.user` without data. The kind must not start with `core.`. */
  readonly entry?: InputEntry | undefined;
}

/**
 * A passive entry write.
 *
 * @category models
 * @since 0.0.0
 */
export interface WriteSubmissionDraft {
  readonly requestId?: string | undefined;
  /**
   * When the submission settles, delivers `Signal.SubmissionSettled` to this task under `key`, in the
   * settling commit; a task that refuses it (ended, gone) fails nothing.
   */
  readonly notify?: SubmissionNotify | undefined;
  readonly type: "write";
  readonly entry: EntryDraft;
}

/**
 * A run on the context as it is, without a user message, for example after a
 * run that ended without an answer (`core.live.runFailure`). Only an idle
 * conversation admits one; a busy one fails with `ConversationBusy`.
 *
 * @category models
 * @since 0.0.0
 */
export interface ContinueSubmissionDraft {
  readonly requestId?: string | undefined;
  /**
   * When the submission settles, delivers `Signal.SubmissionSettled` to this task under `key`, in the
   * settling commit; a task that refuses it (ended, gone) fails nothing.
   */
  readonly notify?: SubmissionNotify | undefined;
  readonly type: "continue";
}

/**
 * What a host submits.
 *
 * @category models
 * @since 0.0.0
 */
export type SubmissionDraft = InputSubmissionDraft | WriteSubmissionDraft | ContinueSubmissionDraft;

/**
 * Errors of an admission inside the caller's commit (`Conversation.admit`):
 * the admission's own rejections, and the `Tx` errors of its reads and
 * writes. `ReadAfterWrite` when it comes after the commit's first table
 * write, another admission's included.
 *
 * @category errors
 * @since 0.0.0
 */
export type AdmitError =
  | ConversationBusy
  | ConversationIdle
  | SubmissionError
  | TxReadError
  | TxDocError
  | ReadOnlySession;

/**
 * A submission record once it is `done` or `unanswered`.
 *
 * @category models
 * @since 0.0.0
 */
export type SettledSubmission = Extract<
  SubmissionRecord,
  { readonly status: "done" | "unanswered" }
>;

/**
 * What `withdraw` did: withdrew a queued submission, or found it placed or
 * settled.
 *
 * @category models
 * @since 0.0.0
 */
export type WithdrawResult =
  | { readonly _tag: "Withdrawn" }
  | { readonly _tag: "AlreadyPlaced" }
  | { readonly _tag: "AlreadySettled" };

/**
 * What `Harness.withdrawSubmission` did; `NotFound` for a submission unknown
 * or of another conversation.
 *
 * @category models
 * @since 0.0.0
 */
export type WithdrawSubmissionResult = WithdrawResult | { readonly _tag: "NotFound" };

/**
 * A handle of one admitted submission.
 *
 * @category models
 * @since 0.0.0
 */
export interface Submission {
  readonly id: SubmissionId;
  // `TaskError` (`InvocationEnded`) only for a submission made through a handle bound to a task invocation.
  /** The committed record; read-only. */
  readonly status: Effect.Effect<SubmissionRecord, HarnessError | SubmissionError | TaskError>;
  /**
   * Waits until it is `done` or `unanswered`; interrupting the wait cancels only the wait. Enables
   * scheduling.
   */
  readonly await: Effect.Effect<
    SettledSubmission,
    HarnessError | SubmissionError | TaskError | ReadOnlySession
  >;
  /** Withdraws a still-queued submission and removes its inbox item. */
  readonly withdraw: Effect.Effect<
    WithdrawResult,
    HarnessError | SubmissionError | TaskError | ReadOnlySession
  >;
}

// -----------------------------------------------------------------------------
// errors
// -----------------------------------------------------------------------------

/**
 * A busy conversation rejected input submitted with `whenBusy: "reject"`, or
 * a continue; nothing was written.
 *
 * @category errors
 * @since 0.0.0
 */
export class ConversationBusy extends Schema.TaggedError<ConversationBusy>()("ConversationBusy", {
  conversationId: ConversationId,
}) {
  override get message(): string {
    return `Conversation ${this.conversationId} is busy`;
  }
}

/**
 * An idle conversation rejected input submitted with `whenIdle: "reject"`;
 * nothing was written.
 *
 * @category errors
 * @since 0.0.0
 */
export class ConversationIdle extends Schema.TaggedError<ConversationIdle>()("ConversationIdle", {
  conversationId: ConversationId,
}) {
  override get message(): string {
    return `Conversation ${this.conversationId} is idle`;
  }
}

/**
 * Reason of a `SubmissionError`: no submission with this ID.
 *
 * @category errors
 * @since 0.0.0
 */
export class SubmissionNotFound extends Schema.TaggedError<SubmissionNotFound>()(
  "SubmissionNotFound",
  { message: Schema.String },
) {}

/**
 * Reason of a `SubmissionError`: the request ID already identifies a
 * submission of the other type.
 *
 * @category errors
 * @since 0.0.0
 */
export class RequestIdMismatch extends Schema.TaggedError<RequestIdMismatch>()(
  "RequestIdMismatch",
  { message: Schema.String },
) {}

/**
 * Reason of a `SubmissionError`: an input entry's kind is empty or starts
 * with `core.`.
 *
 * @category errors
 * @since 0.0.0
 */
export class InvalidInputEntry extends Schema.TaggedError<InvalidInputEntry>()(
  "InvalidInputEntry",
  { message: Schema.String },
) {}

/**
 * A submission call failed for a reason the caller handles; the Session stays
 * healthy and nothing was written.
 *
 * @category errors
 * @since 0.0.0
 */
export class SubmissionError extends Schema.TaggedError<SubmissionError>()("SubmissionError", {
  reason: Schema.Union([SubmissionNotFound, RequestIdMismatch, InvalidInputEntry]),
}) {
  override readonly cause = this.reason;

  override get message(): string {
    return this.reason.message;
  }
}

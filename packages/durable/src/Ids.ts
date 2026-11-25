/**
 * Record identities and commit sequences. IDs and sequences are
 * ordinary numbers in memory, JSON and SQLite; the brands only narrow
 * TypeScript types, and `make` applies one at a trusted
 * creation or decoding boundary.
 *
 * @since 0.0.0
 */
import * as Predicate from "effect/Predicate";
import * as Schema from "effect/Schema";

/**
 * Identity of one conversation, a transcript scope.
 *
 * @category ids
 * @since 0.0.0
 */
export const ConversationId = Schema.Number.pipe(Schema.brand("durable/ConversationId"));
/**
 * The type of `ConversationId`.
 *
 * @category ids
 * @since 0.0.0
 */
export type ConversationId = typeof ConversationId.Type;

/**
 * Identity of one immutable transcript entry.
 *
 * @category ids
 * @since 0.0.0
 */
export const EntryId = Schema.Number.pipe(Schema.brand("durable/EntryId"));
/**
 * The type of `EntryId`.
 *
 * @category ids
 * @since 0.0.0
 */
export type EntryId = typeof EntryId.Type;

/**
 * Identity of one durable task.
 *
 * @category ids
 * @since 0.0.0
 */
export const TaskId = Schema.Number.pipe(Schema.brand("durable/TaskId"));
declare const TaskResult: unique symbol;

/**
 * The type of `TaskId`. `R` is the task's result type, an erased brand that
 * exists only at the type level: `tx.createTask` returns `TaskId<R>` and
 * typed waits infer `R` from it; result-agnostic operations take
 * `TaskId<unknown>`, which every task ID is assignable to.
 *
 * @category ids
 * @since 0.0.0
 */
export type TaskId<R = unknown> = typeof TaskId.Type & { readonly [TaskResult]?: R };

/**
 * Identity of one admitted submission.
 *
 * @category ids
 * @since 0.0.0
 */
export const SubmissionId = Schema.Number.pipe(Schema.brand("durable/SubmissionId"));
/**
 * The type of `SubmissionId`.
 *
 * @category ids
 * @since 0.0.0
 */
export type SubmissionId = typeof SubmissionId.Type;

/**
 * Identity of one create-to-retire document incarnation; never reused.
 *
 * @category ids
 * @since 0.0.0
 */
export const DocumentId = Schema.Number.pipe(Schema.brand("durable/DocumentId"));
/**
 * The type of `DocumentId`.
 *
 * @category ids
 * @since 0.0.0
 */
export type DocumentId = typeof DocumentId.Type;

/**
 * Strictly increasing sequence of one atomic storage commit; gaps are allowed.
 *
 * @category ids
 * @since 0.0.0
 */
export const Seq = Schema.Number.pipe(Schema.brand("durable/Seq"));
/**
 * The type of `Seq`.
 *
 * @category ids
 * @since 0.0.0
 */
export type Seq = typeof Seq.Type;

/**
 * The root conversation always uses this reserved ID; the first minted ID is 2.
 *
 * @category ids
 * @since 0.0.0
 */
export const ROOT_CONVERSATION_ID: ConversationId = ConversationId.make(1);

// Document values and task checkpoints are JSON codecs whose type is their encoding (Doc.make, Task.make), which
// a brand is not: its encoding is a plain number. These schemas check the same number and keep the branded type.
const isId = (input: unknown): input is number =>
  Predicate.isNumber(input) && Number.isInteger(input) && input >= 0;

/**
 * A `TaskId` inside a document value or a checkpoint.
 *
 * @category schemas
 * @since 0.0.0
 */
export const TaskIdValue = Schema.declare((input): input is TaskId => isId(input), {
  identifier: "TaskId",
});

/**
 * An `EntryId` inside a document value or a checkpoint.
 *
 * @category schemas
 * @since 0.0.0
 */
export const EntryIdValue = Schema.declare((input): input is EntryId => isId(input), {
  identifier: "EntryId",
});

/**
 * A `SubmissionId` inside a document value or a checkpoint.
 *
 * @category schemas
 * @since 0.0.0
 */
export const SubmissionIdValue = Schema.declare((input): input is SubmissionId => isId(input), {
  identifier: "SubmissionId",
});

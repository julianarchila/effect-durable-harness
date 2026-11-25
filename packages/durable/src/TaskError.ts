/**
 * Errors of task operations. One error with a `reason`; a missing task reuses `SessionError.NotFound`, and closing and
 * failure reuse `SessionClosed` and `SessionFailed`.
 *
 * @since 0.0.0
 */
import * as Schema from "effect/Schema";
import { NotFound } from "./SessionError.ts";

/**
 * Reason of a `TaskError`: a runtime operation ran after its invocation ended
 *
 * @category errors
 * @since 0.0.0
 */
export class InvocationEnded extends Schema.TaggedError<InvocationEnded>()("InvocationEnded", {
  message: Schema.String,
}) {}

/**
 * Reason of a `TaskError`: a runtime commit found its task terminal,
 * `completing` or `waiting`.
 *
 * @category errors
 * @since 0.0.0
 */
export class NotRunning extends Schema.TaggedError<NotRunning>()("NotRunning", {
  message: Schema.String,
}) {}

/**
 * Reason of a `TaskError`: a run invocation tried to commit after its task's
 * durable abort mark.
 *
 * @category errors
 * @since 0.0.0
 */
export class AbortMarked extends Schema.TaggedError<AbortMarked>()("AbortMarked", {
  message: Schema.String,
}) {}

/**
 * Reason of a `TaskError`: a `waiting` state names a missing task, waits
 * `failFast` on a task its waiter does not own, closes a wait cycle, or comes
 * from an abort handler.
 *
 * @category errors
 * @since 0.0.0
 */
export class InvalidWait extends Schema.TaggedError<InvalidWait>()("InvalidWait", {
  message: Schema.String,
}) {}

/**
 * Reason of a `TaskError`: `abortOwned` named a task the invocation's task
 * does not own.
 *
 * @category errors
 * @since 0.0.0
 */
export class NotOwned extends Schema.TaggedError<NotOwned>()("NotOwned", {
  message: Schema.String,
}) {}

/**
 * Reason of a `TaskError`: `outcomes` named a task that is not terminal.
 *
 * @category errors
 * @since 0.0.0
 */
export class NotTerminal extends Schema.TaggedError<NotTerminal>()("NotTerminal", {
  message: Schema.String,
}) {}

/**
 * Reason of a `TaskError`: a `sleeping` state names an `until` that is not a
 * time, or comes from an abort handler.
 *
 * @category errors
 * @since 0.0.0
 */
export class InvalidSleep extends Schema.TaggedError<InvalidSleep>()("InvalidSleep", {
  message: Schema.String,
}) {}

/**
 * The reasons of a `TaskError`.
 *
 * @category errors
 * @since 0.0.0
 */
export type TaskErrorReason =
  | NotFound
  | InvocationEnded
  | NotRunning
  | AbortMarked
  | InvalidWait
  | InvalidSleep
  | NotOwned
  | NotTerminal;

/**
 * A task operation was refused. Nothing was written; the Session stays
 * healthy. Handle one reason with `Effect.catchReason`.
 *
 * @category errors
 * @since 0.0.0
 */
export class TaskError extends Schema.TaggedError<TaskError>()("TaskError", {
  reason: Schema.Union([
    NotFound,
    InvocationEnded,
    NotRunning,
    AbortMarked,
    InvalidWait,
    InvalidSleep,
    NotOwned,
    NotTerminal,
  ]),
}) {
  override readonly cause = this.reason;

  override get message(): string {
    return this.reason.message;
  }
}

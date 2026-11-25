/**
 * What the Harness reports to `HarnessOptions.onReport` . A report is a failure that fails no
 * caller: what it is about, tagged by `kind`, and the
 * `Cause` of the failure. A host tells a failed Session (`storage_failed`) from a faulted task
 * (`task_faulted`), and a crash loop (`task_interrupted`) from a throwing hook (`extension_error`).
 *
 * @since 0.0.0
 */
import type * as Cause from "effect/Cause";
import * as Schema from "effect/Schema";
import { ConversationId, TaskId } from "./Ids.ts";

/**
 * The task a report is about.
 *
 * @category models
 * @since 0.0.0
 */
export interface ReportTask {
  readonly id: TaskId;
  /** The task's registered name. */
  readonly kind: string;
  readonly conversationId: ConversationId;
}

/**
 * A host callback whose failure is a `host_error`, by its option name: `settings` is
 * `HarnessOptions.settings`, `env` is `HarnessOptions.env` while preparing a request, `onWake` the
 * `SessionWakeups` service's `schedule`, `models` the `Models` service's `cancelDeferred` (a failed cancellation
 * of a deferred response), and `telemetry` a telemetry call made while a commit is published, which must not
 * fail it.
 *
 * @category models
 * @since 0.0.0
 */
// No `now`, `registry` or `setTimer`: time comes from `Clock` and the registry is static, so none of them is a
// host callback that can throw. `models` is only a deferred response's
// cancellation: the other `Models` calls never fail (their failures are error messages).
export type HostCallback = "settings" | "env" | "onWake" | "models" | "telemetry";

/**
 * What a report is about, by `kind`. `extension`, where present, names the extension
 * whose code failed; for a task's own code, the one that installed the task's definition. Built-in tasks
 * have none.
 *
 * - `storage_failed`: the failure failed the Session, which closes.
 * - `task_faulted`: the task ended `faulted`: its phase or abort handler failed (`threw`), or it was cut off
 *   `settings.restartLimit` times (`restart_limit`).
 * - `task_interrupted`: crashes ended the task (`TaskInterrupted`).
 * - `migration_failed`: the task's stored state could not be migrated; it is blocked.
 * - `task_outdated`: a running task kept running under a definition that was replaced. Never reported
 *   under the static registry; the kind exists for hosts that switch over every kind.
 * - `task_error`: the task's own `report`, or a failure in running it.
 * - `extension_error`: a hook, a section or a wrapper failed, or a tool returned invalid structured output
 *   (`AgentTool.InvalidStructuredOutput`); with the task that ran it, or, outside a task,
 *   the conversation whose agent was resolved.
 * - `host_error`: a host callback failed; with the task that called it, if one did.
 * - `listener_error`: a commit, close, view or document listener threw.
 *
 * @category models
 * @since 0.0.0
 */
export type ReportInfo =
  | { readonly kind: "storage_failed" }
  | {
      readonly kind: "task_faulted";
      readonly task: ReportTask;
      readonly extension?: string | undefined;
      readonly reason: "threw" | "restart_limit";
    }
  | {
      readonly kind: "task_interrupted";
      readonly task: ReportTask;
      readonly extension?: string | undefined;
      readonly reason: "restart_limit" | "interrupted";
      readonly restarts: number;
    }
  | {
      readonly kind: "migration_failed";
      readonly task: ReportTask;
      readonly extension?: string | undefined;
    }
  | {
      readonly kind: "task_outdated";
      readonly task: ReportTask;
      readonly extension?: string | undefined;
      readonly reason: "missing_task" | "task_too_old" | "migration_failed";
    }
  | {
      readonly kind: "task_error";
      readonly task: ReportTask;
      readonly extension?: string | undefined;
    }
  | {
      readonly kind: "extension_error";
      readonly extension?: string | undefined;
      readonly task?: ReportTask | undefined;
      readonly conversationId?: ConversationId | undefined;
    }
  | {
      readonly kind: "host_error";
      readonly callback: HostCallback;
      readonly task?: ReportTask | undefined;
    }
  | { readonly kind: "listener_error" };

/**
 * One report to `HarnessOptions.onReport`: what it is about and the failure.
 *
 * @category models
 * @since 0.0.0
 */
// `cause` is the failure as Effect keeps it.
export type Report = ReportInfo & { readonly cause: Cause.Cause<unknown> };

/**
 * Whose code a task's `report(cause, from)` is about when not the task's own.
 *
 * @category models
 * @since 0.0.0
 */
export type ReportFrom =
  | { readonly kind: "extension"; readonly extension?: string | undefined }
  | { readonly kind: "host"; readonly callback: HostCallback };

/**
 * The failure of a `task_interrupted` report. Crashes ended a task: it ended itself after being cut off `settings.restartLimit` times in a row
 * (`restart_limit`: a tool call, a generation), or a generation's run ended because the host kept stopping
 * while the model answered (`interrupted`). `restarts` is the task's count when it ended; a host keeps it as
 * a crash-loop signal.
 *
 * @category reports
 * @since 0.0.0
 */
export class TaskInterrupted extends Schema.TaggedError<TaskInterrupted>()("TaskInterrupted", {
  taskId: TaskId,
  kind: Schema.String,
  conversationId: ConversationId,
  reason: Schema.Literals(["restart_limit", "interrupted"]),
  restarts: Schema.Number,
  message: Schema.String,
}) {}

/**
 * The failure of a `task_faulted` report with reason `restart_limit`. The scheduler ended a task `faulted` because it was cut off `settings.restartLimit` times: a task of an
 * extension's kind in its phases, or any task in its abort handler.
 *
 * @category reports
 * @since 0.0.0
 */
export class TaskFaulted extends Schema.TaggedError<TaskFaulted>()("TaskFaulted", {
  taskId: TaskId,
  kind: Schema.String,
  conversationId: ConversationId,
  reason: Schema.Literal("restart_limit"),
  message: Schema.String,
}) {}

// The reporter the kernel's components share: a failure that
// fails no caller and what it is about.
import type * as Cause from "effect/Cause";
import type * as Tracer from "effect/Tracer";
import type { ReportInfo, ReportTask } from "../Report.ts";
import type { ConversationId, TaskId } from "../Ids.ts";

/**
 * `on`: the span the report's telemetry goes on when it is not the reporting fiber's: a commit's publication runs on the writer's fiber, its listeners' reports belong to its caller.
 */
export type Reporter = (
  cause: Cause.Cause<unknown>,
  info: ReportInfo,
  on?: Tracer.AnySpan | undefined,
) => void;

/** The report info of a failing commit listener. */
export const LISTENER: ReportInfo = { kind: "listener_error" };

/** The task a report is about. */
export const reportTask = (task: {
  readonly taskId: TaskId;
  readonly kind: string;
  readonly conversationId: ConversationId;
}): ReportTask => ({ id: task.taskId, kind: task.kind, conversationId: task.conversationId });

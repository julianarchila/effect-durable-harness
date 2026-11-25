// Run control and tool slots in `core.live`, and the Harness cleanup of an outcome the scheduler writes itself.
// `appendAssistant` and `convertPartial` live here, since the scheduler's cleanup converts partials too and the
// generation task imports this module.
import type * as Tracker from "@effect-durable-harness/delta/Tracker";
import * as Effect from "effect/Effect";
import type * as Schema from "effect/Schema";
import { AssistantEntry, type TypedEntry } from "../Entry.ts";
import type { ConversationId, EntryId, TaskId } from "../Ids.ts";
import {
  type CompactionStatus,
  LiveDoc,
  type LiveState,
  type NestedToolSlot,
  type RunFailureReason,
  type ToolSlot,
} from "../Live.ts";
import type { AssistantMessage } from "../Message.ts";
import type { TaskRecord } from "../Records.ts";
import type { CommitOptions } from "../Session.ts";
import type { CommitRejected } from "../SessionError.ts";
import { Tx, type SubmissionSettlement, type TxDocError } from "../Tx.ts";
import { interruptionOf, type SchedulerOutcome } from "./decisions.ts";
import { recordUsage } from "./usage.ts";

type LiveDraft = Tracker.Draft<LiveState>;
type SlotDraft = Tracker.Draft<ToolSlot> | Tracker.Draft<NestedToolSlot>;

/** Built-in task kinds that can own `core.live.run`. */
const RUN_TASK_KINDS: ReadonlySet<string> = new Set(["core.generation"]);
const TOOL_TASK_KIND = "core.tool";
const COMPACTION_TASK_KIND = "core.compaction";

/**
 * How a run ends: with an answer, or without one, settling its inputs
 * `unanswered` and recording why.
 */
export type RunEnd =
  | { readonly status: "done"; readonly answer: EntryId }
  | {
      readonly status: "unanswered";
      readonly reason: RunFailureReason;
      readonly detail?: string | undefined;
      /** The inputs' settlement; default `unanswered` with `reason` and `detail`. */
      readonly settlement?: SubmissionSettlement | undefined;
    };

/**
 * Ends the run `taskId` owns: settles each of its inputs, removes `run`, and,
 * without an answer, records `runFailure`, unless the run answered already and waits for its after-run tasks
 * (`run.afterRun`), whose inputs are settled. Always removes `generation`, `tools` and `nestedTools`, whose
 * presentation belongs to the ending run.
 */
/**
 * How the progress throttles commit a partial answer, running tool
 * output, a slot's running status and a generation's attempt into `core.live`: deferred, so storage gets them
 * together, at the next commit that reads or writes `core.live` or after `settings.progress.persistWithinMs`. Every
 * commit that consumes or clears streamed fields reads `core.live`, so it stores them with its own changes.
 */
export const PROGRESS_COMMIT: CommitOptions = { persistence: { kind: "deferred" } };

export const endRun = (
  live: LiveDraft,
  taskId: TaskId,
  end: RunEnd,
): Effect.Effect<void, CommitRejected, Tx> =>
  Effect.gen(function* () {
    const tx = yield* Tx;
    const run = live.run;
    if (run?.taskId === taskId) {
      const inputs = [...run.inputs];
      const first = run.first;
      if (end.status === "done") {
        for (const id of inputs) yield* tx.settleSubmission(id, end);
      } else if (run.afterRun === undefined) {
        const failure: NonNullable<LiveDraft["runFailure"]> = {
          taskId,
          inputs,
          reason: end.reason,
        };
        if (end.detail !== undefined) failure.detail = end.detail;
        if (first !== undefined) failure.first = first;
        const settlement: SubmissionSettlement =
          end.settlement ?? unansweredSettlement(end.reason, end.detail);
        for (const id of inputs) yield* tx.settleSubmission(id, settlement);
        live.runFailure = failure;
      }
      delete live.run;
    }
    delete live.generation;
    delete live.tools;
    delete live.nestedTools;
  });

/** The default settlement of a failed run's inputs: `unanswered` with its reason and detail. */
const unansweredSettlement = (reason: string, detail: string | undefined): SubmissionSettlement =>
  detail === undefined
    ? { status: "unanswered", reason }
    : { status: "unanswered", reason, detail };

/**
 * The slot of tool task `taskId`, model-issued or nested, if `core.live` still lists it. Nested slots are found by
 * binary search: a tool may make thousands of nested calls, each of which looks up its slot in several commits,
 * and every element a search reads costs a node in the draft's tracker. Their task IDs ascend: each slot is
 * appended in the commit that creates its task, task IDs ascend in commit order, and removal keeps the order.
 */
export function toolSlot(live: LiveDraft, taskId: TaskId): SlotDraft | undefined;
export function toolSlot(live: LiveState, taskId: TaskId): ToolSlot | NestedToolSlot | undefined;
export function toolSlot(live: LiveState, taskId: TaskId): ToolSlot | NestedToolSlot | undefined {
  const slot = live.tools?.find((each) => each.taskId === taskId);
  const nested = live.nestedTools;
  if (slot !== undefined || nested === undefined) return slot;
  let low = 0;
  let high = nested.length - 1;
  while (low <= high) {
    const middle = (low + high) >>> 1;
    const candidate = nested[middle]!;
    if (candidate.taskId === taskId) return candidate;
    if (candidate.taskId < taskId) low = middle + 1;
    else high = middle - 1;
  }
  return undefined;
}

/** Whether a slot is a nested call's. */
export const isNestedSlot = (slot: SlotDraft): slot is Tracker.Draft<NestedToolSlot> =>
  "parentTaskId" in slot;

/**
 * Removes the nested slots below tool task `taskId`,
 * transitively; the list holds parents before their children.
 */
export const removeNestedSlots = (live: LiveDraft, taskId: TaskId): void => {
  const nested = live.nestedTools;
  if (nested === undefined) return;
  const removed = new Set<TaskId>([taskId]);
  for (const slot of nested) if (removed.has(slot.parentTaskId)) removed.add(slot.taskId);
  for (let index = nested.length - 1; index >= 0; index--) {
    if (removed.has(nested[index]!.parentTaskId)) nested.splice(index, 1);
  }
  if (nested.length === 0) delete live.nestedTools;
};

/**
 * Marks a slot done and clears its running output; the caller sets a
 * model-issued call's result `entry`, or a nested call's `summary`. Neither is set when the tool task faulted or
 * was orphaned.
 */
export const finishSlot = (slot: SlotDraft): void => {
  slot.status = "done";
  clearProgress(slot);
};

/** Removes what a tool published while running. */
export const clearProgress = (slot: SlotDraft): void => {
  clearOutput(slot);
  delete slot.details;
  delete slot.diagnostics;
};

/** Removes a slot's running output; its details and diagnostics stay, as across the runs of a parked call. */
export const clearOutput = (slot: SlotDraft): void => {
  delete slot.output;
  delete slot.droppedBytes;
  delete slot.droppedLines;
};

/** A compaction created in this commit; statuses stay in ID order. */
export const addCompactionStatus = (live: LiveDraft, status: CompactionStatus): void => {
  live.compactions ??= [];
  live.compactions.push(status);
};

/** The status of compaction task `taskId`, if listed. */
export const compactionStatus = (
  live: LiveDraft,
  taskId: TaskId,
): Tracker.Draft<CompactionStatus> | undefined =>
  live.compactions?.find((status) => status.taskId === taskId);

/** Removes the status of `taskId`, and the list once empty. */
export const removeCompactionStatus = (live: LiveDraft, taskId: TaskId): void => {
  const statuses = live.compactions;
  if (statuses === undefined) return;
  const index = statuses.findIndex((status) => status.taskId === taskId);
  if (index >= 0) statuses.splice(index, 1);
  if (statuses.length === 0) delete live.compactions;
};

/**
 * Removes the status of `taskId` and records that it failed with `error`, replacing an older failure.
 */
export const failCompactionStatus = (live: LiveDraft, taskId: TaskId, error: string): void => {
  const status = compactionStatus(live, taskId);
  if (status !== undefined) {
    live.compactionFailure = { taskId: status.taskId, trigger: status.trigger, error };
  }
  removeCompactionStatus(live, taskId);
};

/**
 * Appends a provider result and adds its usage to `core.usage` in the same commit. Every built-in writer of
 * assistant entries goes through here, so the ledger stays complete.
 */
export const appendAssistant = (
  conversationId: ConversationId,
  message: AssistantMessage,
): Effect.Effect<TypedEntry<never>, TxDocError, Tx> =>
  Effect.gen(function* () {
    const tx = yield* Tx;
    yield* recordUsage(
      conversationId,
      "models",
      `${message.provider}/${message.model}`,
      message.usage,
    );
    return yield* tx.appendEntry(AssistantEntry, conversationId, { model: [message] });
  });

/**
 * A committed partial of an interrupted attempt becomes an aborted assistant entry, so the transcript keeps what
 * the model produced and `core.usage` counts its spend.
 */
export const convertPartial = (
  live: LiveDraft,
  conversationId: ConversationId,
): Effect.Effect<void, TxDocError, Tx> =>
  Effect.gen(function* () {
    const partial = live.generation?.message;
    if (partial === undefined) return;
    yield* appendAssistant(conversationId, { ...partial, stopReason: "aborted" });
  });

/**
 * Harness cleanup for an outcome the scheduler writes itself (`faulted` or `orphaned`). A run task ends its run,
 * converting a committed partial; a tool task's slot is marked done without an entry, and context derivation
 * synthesizes the missing result, and the nested slots below it are removed; a compaction task's status is
 * removed and its failure recorded. Other kinds are ignored, so it never creates `core.live` elsewhere.
 */
export const settleSchedulerOutcome = (
  record: TaskRecord,
  outcome: SchedulerOutcome,
): Effect.Effect<void, TxDocError, Tx> =>
  Effect.gen(function* () {
    const tx = yield* Tx;
    if (record.kind === TOOL_TASK_KIND) {
      const live = yield* tx.doc(LiveDoc, record.conversationId);
      const slot = toolSlot(live, record.id);
      if (slot !== undefined) finishSlot(slot);
      removeNestedSlots(live, record.id);
      return;
    }
    // Only an abort handler is faulted for the restart limit here (the built-in
    // phases end themselves): the task was aborted, and keeps that meaning. Its run ends `aborted`, with the
    // limit as detail, and without converting its partial, which may be what cut the handler off; an aborted
    // compaction is no failure.
    const cutOff = outcome.status === "faulted" && restartLimitDetail(outcome.error.detail);
    if (record.kind === COMPACTION_TASK_KIND) {
      const live = yield* tx.doc(LiveDoc, record.conversationId);
      if (cutOff) removeCompactionStatus(live, record.id);
      else {
        failCompactionStatus(
          live,
          record.id,
          outcome.status === "faulted" ? outcome.error.message : outcome.reason,
        );
      }
      return;
    }
    if (!RUN_TASK_KINDS.has(record.kind)) return;
    const live = yield* tx.doc(LiveDoc, record.conversationId);
    if (live.run?.taskId !== record.id) return;
    // The run answered and waited for its `onYield` tasks: it ends with its answer, without a failure.
    const answered = live.run.yield?.answer;
    if (answered !== undefined) {
      return yield* endRun(live, record.id, { status: "done", answer: answered });
    }
    if (!cutOff) yield* convertPartial(live, record.conversationId);
    // The run's failure names the orphan reason as detail; the inputs keep it as reason.
    yield* endRun(
      live,
      record.id,
      outcome.status === "faulted"
        ? {
            status: "unanswered",
            reason: cutOff ? "aborted" : "faulted",
            detail: outcome.error.message,
          }
        : {
            status: "unanswered",
            reason: "orphaned",
            detail: outcome.reason,
            settlement: { status: "unanswered", reason: outcome.reason },
          },
    );
  });

// ─── The restart limit ───────────────────────────

/**
 * Built-in task kinds that end themselves once cut off `settings.restartLimit` times (`TaskRecord.restarts`); the
 * scheduler faults a task of any other kind instead.
 */
export const LIMITS_RESTARTS: ReadonlySet<string> = new Set([
  ...RUN_TASK_KINDS,
  TOOL_TASK_KIND,
  COMPACTION_TASK_KIND,
]);

/** Whether `task` was cut off `restartLimit` times in a row, so its phase ends it instead of running. */
export const cutOffTooOften = (
  task: { readonly restarts?: number | undefined },
  restartLimit: number,
): boolean => task.restarts !== undefined && task.restarts >= restartLimit;

/**
 * Whether a task at the limit may still do the cleanup it does when it ends, such as converting a partial: only at
 * the limit. Past it, that cleanup may be what cut the task off, so the task ends without it.
 */
export const salvageable = (
  task: { readonly restarts?: number | undefined },
  restartLimit: number,
): boolean => (task.restarts ?? 0) <= restartLimit;

/** How often a task was cut off, for a message: "once", or "3 times in a row". */
export const cutOffCount = (task: { readonly restarts?: number | undefined }): string => {
  const restarts = task.restarts ?? 0;
  return restarts === 1 ? "once" : `${restarts} times in a row`;
};

/** Whether a failure's detail is the restart limit's (`{ reason: "restart_limit" }`). */
const restartLimitDetail = (detail: Schema.Json | undefined): boolean =>
  interruptionOf(detail) === "restart_limit";

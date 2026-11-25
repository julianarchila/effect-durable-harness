/**
 * The built-in live document, `core.live`: run control and
 * the presentation of the work in flight. `run` is present exactly while the
 * conversation is busy and names the task that settles the run's inputs; the
 * rest is what a client shows: the partial of the current generation attempt
 * and the slots of the current tool round.
 *
 * Every terminal path of a run task removes `run`, `generation`, `tools` and
 * `nestedTools` in the commit that settles the run's inputs, and a run that
 * ends without an answer records why in `runFailure`.
 *
 * @since 0.0.0
 */
import * as Predicate from "effect/Predicate";
import * as Schema from "effect/Schema";
import { ToolDiagnostic } from "./AgentTool.ts";
import { CompactionReason } from "./Compaction.ts";
import * as Doc from "./Doc.ts";
import { EntryIdValue, SubmissionIdValue, TaskIdValue } from "./Ids.ts";
import { AssistantMessage, Usage } from "./Message.ts";

/**
 * Presentation of one tool call of the current round.
 *
 * @category models
 * @since 0.0.0
 */
export const ToolSlot = Schema.Struct({
  callId: Schema.String,
  name: Schema.String,
  status: Schema.Literals(["pending", "running", "sleeping", "waiting", "done"]),
  /** Retained running output, while running. */
  output: Schema.optionalKey(Schema.String),
  droppedBytes: Schema.optionalKey(Schema.Number),
  droppedLines: Schema.optionalKey(Schema.Number),
  /** Last `details` value, while running or parked; kept across the runs of a parked call. */
  details: Schema.optionalKey(Schema.Json),
  /** Diagnostics recorded through `ToolContext.diagnostic`, while running or parked; kept across its runs. */
  diagnostics: Schema.optionalKey(Schema.Array(ToolDiagnostic)),
  /** Absent for a call not started yet (sequential round). */
  taskId: Schema.optionalKey(TaskIdValue),
  /**
   * A call to a tool its request did not offer: its tool task resolves nothing and settles `tool_unavailable`.
   */
  unoffered: Schema.optionalKey(Schema.Literal(true)),
  /** Result entry once done; absent when the tool task faulted or was orphaned. */
  entry: Schema.optionalKey(EntryIdValue),
});
/**
 * The type of `ToolSlot`.
 *
 * @category models
 * @since 0.0.0
 */
export type ToolSlot = typeof ToolSlot.Type;

/**
 * How a nested call ended, for a status line: an error flag, its execution time, its
 * spend, and up to 500 characters of error text.
 *
 * @category models
 * @since 0.0.0
 */
export const NestedToolSummary = Schema.Struct({
  isError: Schema.Boolean,
  durationMs: Schema.optionalKey(Schema.Number),
  usage: Schema.optionalKey(Usage),
  error: Schema.optionalKey(Schema.String),
});
/**
 * The type of `NestedToolSummary`.
 *
 * @category models
 * @since 0.0.0
 */
export type NestedToolSummary = typeof NestedToolSummary.Type;

/**
 * Presentation of one nested call: a call a running tool made through `ToolContext.executeTool`.
 * `callId` is `<parentCallId>/<key>`, unique because keys contain no `/`. Slots are related by
 * task ID: `taskId` is the call's tool task, `parentTaskId` the calling one, model-issued or nested.
 *
 * @category models
 * @since 0.0.0
 */
export const NestedToolSlot = Schema.Struct({
  callId: Schema.String,
  name: Schema.String,
  status: Schema.Literals(["pending", "running", "sleeping", "waiting", "done"]),
  /** Retained running output, while running. */
  output: Schema.optionalKey(Schema.String),
  droppedBytes: Schema.optionalKey(Schema.Number),
  droppedLines: Schema.optionalKey(Schema.Number),
  /** Last `details` value, while running or parked; kept across the runs of a parked call. */
  details: Schema.optionalKey(Schema.Json),
  /** Diagnostics recorded through `ToolContext.diagnostic`, while running or parked; kept across its runs. */
  diagnostics: Schema.optionalKey(Schema.Array(ToolDiagnostic)),
  parentCallId: Schema.String,
  parentTaskId: TaskIdValue,
  taskId: TaskIdValue,
  /**
   * What the call was made with, replaced by the arguments it runs with (after repair, `beforeTool` and
   * decoding) once it starts executing. A model-issued call's arguments are in its assistant entry instead.
   */
  arguments: Schema.JsonObject,
  /** Once done: how the call ended. The result itself is in the caller's `NestedCallDoc`. */
  summary: Schema.optionalKey(NestedToolSummary),
});
/**
 * The type of `NestedToolSlot`.
 *
 * @category models
 * @since 0.0.0
 */
export type NestedToolSlot = typeof NestedToolSlot.Type;

/**
 * Why a run ended without an answer:
 * - `model_error`: the model's response failed, after any retries, or a blocking or overflow compaction it needed
 *   failed;
 * - `no_model`: no model is configured, or the configured one is not available;
 * - `aborted`: the run was aborted;
 * - `reset`: a queued reset started a new context at the post-tools boundary, before an answer (a tool's
 *   handoff continues the run);
 * - `faulted`: the run's task failed where it cannot recover, or storage rejected its commit;
 * - `orphaned`: the run's task was aborted while no registered definition could take it;
 * - `interrupted`: the host stopped while the model was answering, more often than `settings.retry` retries;
 * - `restart_limit`: the run's generation was cut off `settings.restartLimit` times in a row;
 * - `nothing_to_answer`: the context did not end in a user message or tool results, so no request was sent;
 * - `turn_limit`: a tool round reached the conversation's `maxTurns`; a continue resumes on its results.
 *
 * @category models
 * @since 0.0.0
 */
export const RunFailureReason = Schema.Literals([
  "model_error",
  "no_model",
  "aborted",
  "reset",
  "faulted",
  "orphaned",
  "interrupted",
  "restart_limit",
  "nothing_to_answer",
  "turn_limit",
]);
/**
 * The type of `RunFailureReason`.
 *
 * @category models
 * @since 0.0.0
 */
export type RunFailureReason = typeof RunFailureReason.Type;

/**
 * The newest run that ended without an answer.
 *
 * @category models
 * @since 0.0.0
 */
export const RunFailure = Schema.Struct({
  /** The task that ended the run: the run's task at its end. */
  taskId: TaskIdValue,
  /** The run's inputs, settled `unanswered`. */
  inputs: Schema.Array(SubmissionIdValue),
  reason: RunFailureReason,
  /**
   * `model_error`: the error text; `no_model`: which model is missing; `faulted`: the error message;
   * `orphaned`: why the task could not run; `turn_limit`: the tool rounds the run took, `"1 turn"` or
   * `"N turns"`.
   */
  detail: Schema.optionalKey(Schema.String),
  /** The failed run's `run.first`, so a continue covers its history too. */
  first: Schema.optionalKey(EntryIdValue),
});
/**
 * The type of `RunFailure`.
 *
 * @category models
 * @since 0.0.0
 */
export type RunFailure = typeof RunFailure.Type;

/**
 * Run control.
 *
 * @category models
 * @since 0.0.0
 */
export const LiveRun = Schema.Struct({
  /** The task that settles the run's inputs: its generation, handed from one to the next. */
  taskId: TaskIdValue,
  /** The run's placed inputs, settled when it ends; `[]` once it answered and runs only its `afterRun` tasks. */
  inputs: Schema.Array(SubmissionIdValue),
  /** Tool rounds so far, which `AgentState.maxTurns` bounds; an `onYield` continuation starts the count over. */
  turns: Schema.Number,
  /**
   * The run's first entry: the first one the boundary that started it placed, a continue's the failed run's
   * it continues; otherwise its first tool-calling answer.
   */
  first: Schema.optionalKey(EntryIdValue),
  /**
   * The run answered and waits for the tasks its `afterRun` hooks asked for, listed once created; its inputs are settled `done` already.
   */
  afterRun: Schema.optionalKey(
    Schema.Struct({ tasks: Schema.optionalKey(Schema.Array(TaskIdValue)) }),
  ),
  /**
   * The tasks an `onYield` hook asked for decide whether the answer stands: the answer's entry, which the tasks
   * read, and the tasks. The inputs stay open; every exit but a continuation settles them `done` with this answer.
   */
  yield: Schema.optionalKey(
    Schema.Struct({ answer: EntryIdValue, tasks: Schema.Array(TaskIdValue) }),
  ),
});
/**
 * The type of `LiveRun`.
 *
 * @category models
 * @since 0.0.0
 */
export type LiveRun = typeof LiveRun.Type;

/**
 * Presentation of one live compaction task.
 *
 * @category models
 * @since 0.0.0
 */
export const CompactionStatus = Schema.Struct({
  taskId: TaskIdValue,
  /** What started it: `compact()`, a threshold, or a context overflow. */
  trigger: CompactionReason,
  /** Whether a generation waits for it: a compaction the generation owns. */
  blocking: Schema.Boolean,
  attempt: Schema.Number,
  /** Durable backoff before the next summarization attempt. */
  retry: Schema.optionalKey(Schema.Struct({ at: Schema.Number, error: Schema.String })),
});
/**
 * The type of `CompactionStatus`.
 *
 * @category models
 * @since 0.0.0
 */
export type CompactionStatus = typeof CompactionStatus.Type;

/**
 * The newest compaction that failed since a compaction last produced a summary.
 *
 * @category models
 * @since 0.0.0
 */
export const CompactionFailure = Schema.Struct({
  taskId: TaskIdValue,
  /** What started the compaction that failed. */
  trigger: CompactionReason,
  /** The failed or faulted outcome's error message, or why the scheduler orphaned the task. */
  error: Schema.String,
});
/**
 * The type of `CompactionFailure`.
 *
 * @category models
 * @since 0.0.0
 */
export type CompactionFailure = typeof CompactionFailure.Type;

/**
 * The value of `core.live`.
 *
 * @category models
 * @since 0.0.0
 */
export const LiveState = Schema.Struct({
  /** Run control; present exactly while busy. */
  run: Schema.optionalKey(LiveRun),
  /** Presentation of the current generation attempt. */
  generation: Schema.optionalKey(
    Schema.Struct({
      attempt: Schema.Number,
      /** Committed throttled partial of the in-flight response. */
      message: Schema.optionalKey(AssistantMessage),
      /** Durable backoff before the next attempt. */
      retry: Schema.optionalKey(Schema.Struct({ at: Schema.Number, error: Schema.String })),
      /** Provider-side deferred response being polled. */
      deferred: Schema.optionalKey(Schema.Struct({ pollAt: Schema.Number })),
    }),
  ),
  /** The current tool round in call order. */
  tools: Schema.optionalKey(Schema.Array(ToolSlot)),
  /**
   * Nested calls of running tool calls, in creation order, so a parent precedes its children and task IDs
   * ascend. A call's nested slots are removed when the call settles.
   */
  nestedTools: Schema.optionalKey(Schema.Array(NestedToolSlot)),
  /** Live compaction tasks in task ID order; absent when none. */
  compactions: Schema.optionalKey(Schema.Array(CompactionStatus)),
  /**
   * The newest compaction failure, so a client can show it after the task ended. A compaction that produces a
   * summary removes it; one that is aborted or finds nothing to compact leaves it.
   */
  compactionFailure: Schema.optionalKey(CompactionFailure),
  /**
   * Why the newest run ended without an answer, so a client can show it after the run ended. Set in the
   * commit that ends the run; removed when a later commit starts a run. A run started in the same commit, by
   * `afterFailedRun: "continue"` or after a reset, keeps it.
   */
  runFailure: Schema.optionalKey(RunFailure),
});
/**
 * The type of `LiveState`.
 *
 * @category models
 * @since 0.0.0
 */
export type LiveState = typeof LiveState.Type;

/**
 * The `core.live` document. A complete base whenever nothing runs: no
 * generation and no running tool slot, model-issued or nested.
 *
 * @category documents
 * @since 0.0.0
 */
export const LiveDoc = Doc.make({
  kind: "core.live",
  version: 3,
  scope: "conversation",
  history: "latest",
  fork: "initial",
  schema: LiveState,
  initial: () => ({}),
  // Version 1 named a compaction's trigger `reason`, in `compactions` and
  // `compactionFailure`. Versions 1 and 2 may lack `run.turns`: a run in flight across the upgrade counts its turns
  // from 0. Version 2 already names the trigger `trigger`, so renaming leaves it alone.
  migrate: (value) => migrateLive(value),
  checkpointWhen: (value) =>
    value.generation === undefined &&
    !(value.tools ?? []).some((slot) => slot.status === "running") &&
    !(value.nestedTools ?? []).some((slot) => slot.status === "running"),
});

const migrateLive = (value: Schema.JsonObject): LiveState => {
  const migrated = new Map(Object.entries(value));
  const run = value["run"];
  if (run !== undefined && isJsonObject(run)) {
    migrated.set("run", { ...run, turns: Predicate.isNumber(run["turns"]) ? run["turns"] : 0 });
  }
  const compactions = value["compactions"];
  if (Array.isArray(compactions)) migrated.set("compactions", compactions.map(renameReason));
  const failure = value["compactionFailure"];
  if (failure !== undefined) migrated.set("compactionFailure", renameReason(failure));
  // SAFETY: the Session checks a migrated value against `LiveState` before it is used (`Doc.make`'s `schema`).
  return Object.fromEntries(migrated) as LiveState;
};

/**
 * A copy of a version 1 compaction status or failure with `reason`
 * renamed `trigger`, its keys in their order. Anything else is left for the schema check.
 */
const renameReason = (value: Schema.Json): Schema.Json =>
  isJsonObject(value)
    ? Object.fromEntries(
        Object.entries(value).map(([key, field]): [string, Schema.Json] => [
          key === "reason" ? "trigger" : key,
          field,
        ]),
      )
    : value;

const isJsonObject = (value: Schema.Json): value is Schema.JsonObject =>
  Predicate.isObject(value) && !Array.isArray(value);

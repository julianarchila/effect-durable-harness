// The pure decisions of the generation task: how a terminal response is classified, whether a final answer
// continues the run, how a tool round starts and how it ends. Each runs in one synchronous step between the
// loads and the writes of its commit.
import type { ToolControl } from "../AgentTool.ts";
import type { TaskId } from "../Ids.ts";
import type { ToolSlot } from "../Live.ts";
import type { AssistantMessage, DeferredHandle, ToolCall } from "../Message.ts";
import type { RetryPolicy } from "../Settings.ts";
import { isContextOverflow, isRetryableAssistantError, retryDelayMs } from "./classify.ts";

/** What a terminal response leads to, before any setting is read. */
export type AnswerClassification =
  | { readonly _tag: "Tools"; readonly calls: ReadonlyArray<ToolCall> }
  | { readonly _tag: "Answer" }
  | { readonly _tag: "Error" };

/** What an error response leads to, under the retry policy read for it. */
export type ErrorClassification =
  | { readonly _tag: "Retry"; readonly until: number; readonly error: string }
  | { readonly _tag: "Fail"; readonly text: string };

/**
 * After the `deferred` branch that `classify` in `generation.ts` takes first (`nextPollAt`): a tool-calling answer starts a round; `stop`, `length` and a `toolUse` without calls are
 * answers; anything else, a `deferred` message without a handle included, is an error, which
 * `classifyError` decides with the settings read then (they are read only on that branch).
 */
export const classifyAnswer = (message: AssistantMessage): AnswerClassification => {
  const calls = message.content.filter(
    (content): content is ToolCall => content.type === "toolCall",
  );
  if (message.stopReason === "toolUse" && calls.length > 0) return { _tag: "Tools", calls };
  if (
    message.stopReason === "stop" ||
    message.stopReason === "length" ||
    message.stopReason === "toolUse"
  ) {
    return { _tag: "Answer" };
  }
  return { _tag: "Error" };
};

/**
 * After the overflow recovery that `classify` in `generation.ts` tries first
 * (compaction): a retryable error retries while the policy allows another attempt (`attempt <= maxRetries`);
 * anything else, an overflow included, fails. An overflow is never retried: only a compaction can make the next
 * request fit.
 */
export const classifyError = (
  message: AssistantMessage,
  retry: RetryPolicy,
  attempt: number,
  now: number,
): ErrorClassification => {
  const overflow = message.stopReason === "error" && isContextOverflow(message);
  const retryable =
    message.stopReason === "error" &&
    !overflow &&
    isRetryableAssistantError(message) &&
    retry.enabled &&
    attempt <= retry.maxRetries;
  if (retryable) {
    return {
      _tag: "Retry",
      until: now + retryDelayMs(retry, attempt),
      error: message.errorMessage ?? "",
    };
  }
  return {
    _tag: "Fail",
    text: message.errorMessage ?? `Model response ended with stop reason ${message.stopReason}`,
  };
};

/**
 * The host stopped while this attempt's request or poll was in flight, a failed attempt under the retry policy. It retries while the policy allows another attempt, after
 * its backoff; otherwise the run ends `interrupted`.
 */
export const classifyInterruption = (
  retry: RetryPolicy,
  attempt: number,
  now: number,
  polling: boolean,
): ErrorClassification => {
  // A poll's fetch is charged like a request, with its own text.
  const text = polling
    ? `The host stopped while the deferred answer was fetched, on attempt ${attempt}`
    : `The host stopped while the model was answering, on attempt ${attempt}`;
  if (retry.enabled && attempt <= retry.maxRetries) {
    return { _tag: "Retry", until: now + retryDelayMs(retry, attempt), error: text };
  }
  return { _tag: "Fail", text };
};

/** The poll delay of a handle without `pollAfterMs`. */
export const DEFAULT_POLL_AFTER_MS = 5000;

/**
 * When a deferred response is polled, `pollAfterMs` (default 5 s) from now, and
 * strictly after the poll that returned it still deferred, so a clock behind it never polls in a loop.
 */
export const nextPollAt = (
  handle: DeferredHandle,
  now: number,
  polledAt: number | undefined,
): number =>
  Math.max(
    now + (handle.pollAfterMs ?? DEFAULT_POLL_AFTER_MS),
    polledAt === undefined ? Number.NEGATIVE_INFINITY : polledAt + 1,
  );

/**
 * An `onYield` continuation keeps the run going only when the final
 * boundary selected no user item and no reset; otherwise the run ends and selected items start the next one.
 */
export const decideFinal = (
  selectedUsers: number,
  reset: boolean,
  continuation: boolean,
): "Continue" | "End" => (continuation && selectedUsers === 0 && !reset ? "Continue" : "End");

/**
 * What one call of a new tool round gets: its tool task now, or a wait in `pending` until its turn in a sequential
 * round. `unoffered` flags a call to a tool its request did not offer, whose task settles `tool_unavailable`.
 */
export type RoundCall =
  | { readonly _tag: "Pending"; readonly call: ToolCall; readonly unoffered: boolean }
  | { readonly _tag: "Task"; readonly call: ToolCall; readonly unoffered: boolean };

/**
 * Every call gets a tool task, in call order, only the first one now when the round is sequential; a call to a
 * tool the request did not offer is flagged.
 */
export const planToolRound = (
  calls: ReadonlyArray<ToolCall>,
  offered: ReadonlySet<string>,
  sequential: boolean,
): ReadonlyArray<RoundCall> =>
  calls.map((call, index): RoundCall => {
    const unoffered = !offered.has(call.name);
    return sequential && index > 0
      ? { _tag: "Pending", call, unoffered }
      : { _tag: "Task", call, unoffered };
  });

/** How a finished tool round ends. */
export interface RoundEnd {
  /** Every call of the round asked to terminate. */
  readonly terminate: boolean;
  /** Names to enable, in call order. */
  readonly added: ReadonlyArray<string>;
  /** The last handoff in call order. */
  readonly handoff: string | undefined;
}

/**
 * The round's controls, in call order, the slots' order. A
 * slot without a task (faulted or orphaned before it got one) has no control.
 */
export const decideRoundEnd = (
  slots: ReadonlyArray<ToolSlot>,
  controls: ReadonlyMap<TaskId, ToolControl | undefined>,
): RoundEnd => {
  const ordered = slots.map((slot) =>
    slot.taskId === undefined ? undefined : controls.get(slot.taskId),
  );
  const terminate = slots.length > 0 && ordered.every((control) => control?.terminate === true);
  const added = ordered.flatMap((control) => control?.addTools ?? []);
  const handoff = ordered.findLast((control) => control?.handoff !== undefined)?.handoff;
  return { terminate, added, handoff };
};

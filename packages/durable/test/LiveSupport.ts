// What tests of renderers' state read: every `core.live` value of a conversation, and the results of nested calls.
//
// Views come from the conversation handle's `observe`; nested results from the kernel's `followCommits` (the Session's
// own listeners, which see a deferred commit when memory adopts it). The task graph helpers live in
// `HarnessTaskGraph.test.ts`.
import * as Effect from "effect/Effect";
import {
  NestedCallDoc,
  type NestedCallState,
  type NestedToolExecutionResult,
} from "../src/AgentTool.ts";
import type { Conversation } from "../src/Harness.ts";
import type { TaskId } from "../src/Ids.ts";
import type { LiveState, NestedToolSlot, ToolSlot } from "../src/Live.ts";
import type { ConversationView } from "../src/View.ts";
import type { TestHarness } from "./HarnessSupport.ts";

/** A view's `core.live`, or `{}` when it is absent. */
// SAFETY: the mount holds `core.live` as its definition materializes it.
const liveOf = (view: ConversationView) => (view.docs["core.live"] ?? {}) as LiveState;

/**
 * Every view of `conversation` from now on, one per commit that changes it, unfolded: a
 * synchronous observer, so no transition is skipped. `live()` lists their `core.live` values.
 */
export const viewHistory = (conversation: Conversation) =>
  Effect.gen(function* () {
    const views: Array<ConversationView> = [];
    const observation = yield* conversation.observe({
      advance: (frame) => {
        views.push(frame.value);
      },
      end: () => {},
    });
    views.push(observation.value);
    return {
      views,
      live: (): ReadonlyArray<LiveState> => views.map(liveOf),
      detach: observation.detach,
    };
  });

/** A model-issued or nested slot, read through the fields of both. */
export type Slot = ToolSlot &
  Partial<Pick<NestedToolSlot, "parentCallId" | "parentTaskId" | "arguments" | "summary">>;

/** The slots of tool call `callId`, model-issued or nested, in each `core.live` value. */
export const slotsOf = (
  history: ReadonlyArray<LiveState>,
  callId: string,
): ReadonlyArray<Slot | undefined> =>
  history.map((live) =>
    [...(live.tools ?? []), ...(live.nestedTools ?? [])].find((slot) => slot.callId === callId),
  );

/** The index of the first value where call `callId` ended: its slot is done, or gone after it was listed. */
export const endedAt = (history: ReadonlyArray<LiveState>, callId: string): number => {
  const slots = slotsOf(history, callId);
  const listed = slots.findIndex((slot) => slot !== undefined);
  if (listed < 0) return -1;
  return slots.findIndex(
    (slot, index) => index >= listed && (slot === undefined || slot.status === "done"),
  );
};

/** The calls whose slot went `running`, in the order they started. */
export const startOrder = (history: ReadonlyArray<LiveState>): Array<string> => {
  const started: Array<string> = [];
  for (const live of history) {
    for (const slot of [...(live.tools ?? []), ...(live.nestedTools ?? [])]) {
      if (slot.status === "running" && !started.includes(slot.callId)) started.push(slot.callId);
    }
  }
  return started;
};

/**
 * The result of every nested call from now on, by its tool task, read from the `NestedCallDoc` its terminal
 * commit writes, the place a client reads a live caller's nested
 * results from.
 */
export const nestedResults = (h: TestHarness) =>
  Effect.gen(function* () {
    const results = new Map<TaskId, NestedToolExecutionResult>();
    yield* h.kernel.followCommits((publication) => {
      for (const change of publication.changes) {
        if (change.type !== "document" || change.record.kind !== NestedCallDoc.definition.kind)
          continue;
        if (change.value === null) continue;
        // SAFETY: a `NestedCallDoc` change carries its state.
        const state = change.value as NestedCallState;
        if (state.taskId !== undefined && state.result !== undefined) {
          results.set(state.taskId, state.result);
        }
      }
    });
    return results;
  });

/** The result of nested call `callId`, found by the task its slot names. */
export const nestedResultOf = (
  history: ReadonlyArray<LiveState>,
  results: ReadonlyMap<TaskId, NestedToolExecutionResult>,
  callId: string,
): NestedToolExecutionResult | undefined => {
  const slot = slotsOf(history, callId).find((each) => each?.taskId !== undefined);
  return slot?.taskId === undefined ? undefined : results.get(slot.taskId);
};

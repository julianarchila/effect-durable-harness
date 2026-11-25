/**
 * The task graph view: every live task of the Session as
 * one structural value, for UIs and debugging: what runs, what waits for what,
 * and which conversations each task owns. A client follows it with
 * `Harness.watchTaskGraph` or `Harness.observeTaskGraph`, with the same frames
 * as a document's.
 *
 * @since 0.0.0
 */
import type { DocumentWatch } from "./Doc.ts";
import type { BlockedReason } from "./Harness.ts";
import type { ConversationId, TaskId } from "./Ids.ts";
import type { JoinPolicy, SignalKeyRecord, TaskOutcome } from "./Records.ts";

/**
 * A live task's durable status without its checkpoint and outcome payloads.
 * `phase` is the checkpoint's phase.
 *
 * @category models
 * @since 0.0.0
 */
export type TaskGraphState =
  | { readonly status: "pending" | "running"; readonly phase: string }
  | {
      readonly status: "waiting";
      readonly phase: string;
      readonly on: ReadonlyArray<TaskId>;
      readonly policy: JoinPolicy;
    }
  /** Asleep until the Harness clock reaches `until`, or a value under one of `signals`. */
  | {
      readonly status: "sleeping";
      readonly phase: string;
      readonly until?: number;
      readonly signals?: ReadonlyArray<SignalKeyRecord>;
    }
  /** Outcome held until its ordinary owned work drains. */
  | { readonly status: "completing"; readonly outcome: TaskOutcome["status"] };

/**
 * One live task of the graph: the committed record
 * without its input, checkpoint payload, outcome payload and memos.
 * `running` is the durable status: open writes surviving `running` tasks back
 * to `pending`, so they show `pending` until the scheduler reserves them
 * again.
 *
 * @category models
 * @since 0.0.0
 */
// A type alias with exact optional keys, as the records' (`Records.ts`), so a node is a `Schema.Json` value the
// graph's ops carry without a cast.
export type TaskGraphNode = {
  readonly id: TaskId;
  readonly kind: string;
  readonly conversationId: ConversationId;
  /** Owner task; absent for a conversation-owned task. */
  readonly owner?: TaskId;
  readonly background: boolean;
  readonly abortRequested: boolean;
  readonly state: TaskGraphState;
  /**
   * Why it cannot run here, when it cannot: no definition of the Harness's extensions takes it. Follows failed
   * migrations, which change it without a commit.
   */
  readonly blocked?: BlockedReason;
  /** Conversations this task owns, in ID order; they only grow while it lives. */
  readonly conversations: ReadonlyArray<ConversationId>;
};

/**
 * Every live task of the Session. A node is present from the
 * commit that creates its task until the commit that makes it terminal.
 *
 * @category models
 * @since 0.0.0
 */
export type TaskGraph = {
  /** Every live task, keyed by its decimal ID. */
  readonly tasks: Readonly<Record<string, TaskGraphNode>>;
};

/**
 * The graph as a watch. The graph never retires, so
 * its frames never set `retired`.
 *
 * @category models
 * @since 0.0.0
 */
export type TaskGraphWatch = DocumentWatch<TaskGraph>;

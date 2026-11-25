// The agent's RPC (`Host.AgentRpc`): `Harness` and `Conversation` as methods of the object, over plain data.
import type { Harness, HarnessError } from "@effect-durable-harness/core/Harness";
import type { ConversationId, EntryId, SubmissionId } from "@effect-durable-harness/core/Ids";
import type { ConversationRecord, SubmissionRecord } from "@effect-durable-harness/core/Records";
import type { CommitError } from "@effect-durable-harness/core/Session";
import type {
  CommitRejected,
  DocumentAccessError,
  ReadOnlySession,
} from "@effect-durable-harness/core/SessionError";
import {
  type ConversationBusy,
  type ConversationIdle,
  type SettledSubmission,
  SubmissionError,
  type SubmissionDraft,
  SubmissionNotFound,
  type WithdrawSubmissionResult,
} from "@effect-durable-harness/core/Submission";
import type { TaskError } from "@effect-durable-harness/core/TaskError";
import type { TxDocError } from "@effect-durable-harness/core/Tx";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import { ConversationNotFound, type RequestConflict } from "./errors.ts";
import * as Receipts from "./receipts.ts";

/**
 * Options of `AgentRpc.createConversation` and `AgentRpc.fork`.
 *
 * @category models
 * @since 0.0.0
 */
export interface ConversationRequest {
  /** Makes the call idempotent: the same `requestId` returns the conversation the first call made. */
  readonly requestId?: string | undefined;
}

/**
 * The agent as RPC methods of the object, for other Workers: opt in with `Host.agentRpc(harness)` in the
 * object's methods. Every method takes and returns plain data, which crosses Alchemy's RPC bridge as JSON; its
 * failures are effect-durable-harness's tagged errors, revived on the caller's side when the class declares `errors: Host.errors`.
 *
 * @category models
 * @since 0.0.0
 */
export interface AgentRpc {
  /** The root conversation, created ownerless on first use (`Harness.ensureRoot`). */
  readonly ensureRoot: () => Effect.Effect<ConversationRecord, CommitError | DocumentAccessError>;
  /** Every conversation, in ID order (`Harness.conversations`). */
  readonly conversations: () => Effect.Effect<ReadonlyArray<ConversationRecord>, HarnessError>;
  /** Creates an ownerless top-level conversation; idempotent by `requestId`. */
  readonly createConversation: (
    options?: ConversationRequest,
  ) => Effect.Effect<ConversationRecord, CommitError | TxDocError | RequestConflict>;
  /** Forks a conversation through the visible entry `entryId`; idempotent by `requestId`. */
  readonly fork: (
    conversationId: ConversationId,
    entryId: EntryId,
    options?: ConversationRequest,
  ) => Effect.Effect<ConversationRecord, CommitError | TxDocError | RequestConflict>;
  /**
   * Admits input or a passive write in one commit and answers its record as committed. The draft's
   * `requestId` makes it idempotent: a resent draft answers the first submission.
   */
  readonly submit: (
    conversationId: ConversationId,
    draft: SubmissionDraft,
  ) => Effect.Effect<
    SubmissionRecord,
    | ConversationNotFound
    | ConversationBusy
    | ConversationIdle
    | SubmissionError
    | HarnessError
    | CommitRejected
    | TaskError
    | ReadOnlySession
  >;
  /** Waits until a submission is `done` or `unanswered`. */
  readonly awaitSubmission: (
    id: SubmissionId,
  ) => Effect.Effect<
    SettledSubmission,
    SubmissionError | HarnessError | TaskError | ReadOnlySession
  >;
  /** Withdraws a queued submission (`Harness.withdrawSubmission`). */
  readonly withdraw: (
    id: SubmissionId,
    conversationId?: ConversationId | undefined,
  ) => Effect.Effect<WithdrawSubmissionResult, HarnessError | ReadOnlySession>;
  /** Withdraws a conversation's queued inputs, aborts its work and waits until it is idle. */
  readonly abortConversation: (
    id: ConversationId,
  ) => Effect.Effect<void, ConversationNotFound | HarnessError | ReadOnlySession>;
}

export const make = (harness: Harness["Service"]): AgentRpc => {
  const requireConversation = (id: ConversationId) =>
    Effect.flatMap(harness.conversation(id), (found) =>
      Option.match(found, {
        onNone: () => Effect.fail(new ConversationNotFound({ conversationId: id })),
        onSome: Effect.succeed,
      }),
    );
  return {
    ensureRoot: Effect.fn("AgentRpc.ensureRoot")(function* () {
      const root = yield* harness.ensureRoot();
      return { id: root.id };
    }),
    conversations: Effect.fn("AgentRpc.conversations")(function* () {
      return Array.from(yield* Stream.runCollect(harness.conversations()));
    }),
    createConversation: Effect.fn("AgentRpc.createConversation")(function* (options) {
      return yield* Receipts.ensure(harness, options?.requestId, undefined);
    }),
    fork: Effect.fn("AgentRpc.fork")(function* (conversationId, entryId, options) {
      return yield* Receipts.ensure(harness, options?.requestId, { conversationId, entryId });
    }),
    submit: Effect.fn("AgentRpc.submit")(function* (conversationId, draft) {
      const conversation = yield* requireConversation(conversationId);
      const submission = yield* conversation.submit(draft);
      return yield* submission.status;
    }),
    awaitSubmission: Effect.fn("AgentRpc.awaitSubmission")(function* (id) {
      const submission = yield* harness.submission(id);
      if (Option.isNone(submission)) {
        return yield* new SubmissionError({
          reason: new SubmissionNotFound({ message: `Submission ${id} does not exist` }),
        });
      }
      return yield* submission.value.await;
    }),
    withdraw: Effect.fn("AgentRpc.withdraw")(function* (id, conversationId) {
      return yield* harness.withdrawSubmission(id, conversationId);
    }),
    abortConversation: Effect.fn("AgentRpc.abortConversation")(function* (id) {
      const conversation = yield* requireConversation(id);
      yield* conversation.abort();
    }),
  };
};

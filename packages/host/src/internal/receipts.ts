/**
 * Idempotent create and fork by request ID, for the session socket and the agent RPC. The kernel takes a `requestId`
 * on `submit` only, so the host keeps a receipt per request ID, written in the commit that creates the conversation:
 * a resent command finds it and returns the same conversation instead of making another (the chat demo's
 * `ConversationCommandReceipts`, moved here from `examples/cloudflare-chat`).
 */
import * as Doc from "@effect-durable-harness/core/Doc";
import type { Harness } from "@effect-durable-harness/core/Harness";
import { ConversationId, EntryId } from "@effect-durable-harness/core/Ids";
import type { ConversationRecord } from "@effect-durable-harness/core/Records";
import type { CommitError } from "@effect-durable-harness/core/Session";
import { Tx, type TxDocError } from "@effect-durable-harness/core/Tx";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { RequestConflict } from "./errors.ts";

const Receipts = Doc.family({
  checkpointWhen: () => false,
  kind: "durable.conversation-receipt",
  version: 1,
  scope: "session",
  family: true,
  schema: Schema.Struct({
    receipt: Schema.optionalKey(
      Schema.Struct({
        command: Schema.Literals(["create", "fork"]),
        /** What the fork was asked for; absent for a create. */
        from: Schema.optionalKey(
          Schema.Struct({ conversationId: Schema.Number, entryId: Schema.Number }),
        ),
        conversation: Schema.Struct({
          id: Schema.Number,
          parent: Schema.optionalKey(
            Schema.Struct({ conversationId: Schema.Number, at: Schema.Number }),
          ),
        }),
      }),
    ),
  }),
  initial: (_seed: null) => ({}),
});

/** Where a fork starts. */
export interface ForkPoint {
  readonly conversationId: ConversationId;
  readonly entryId: EntryId;
}

export type ReceiptError = CommitError | TxDocError | RequestConflict;

const ownerless = { ownership: { kind: "ownerless" } } as const;

/** Creates an ownerless conversation, or forks one, once per `requestId`. */
export const ensure = (
  harness: Harness["Service"],
  requestId: string | undefined,
  from: ForkPoint | undefined,
): Effect.Effect<ConversationRecord, ReceiptError> => {
  const make: Effect.Effect<ConversationRecord, TxDocError, Tx> = Effect.flatMap(Tx, (tx) =>
    from === undefined
      ? tx.createConversation(ownerless)
      : tx.forkConversation(from.conversationId, from.entryId, ownerless),
  );
  if (requestId === undefined) return harness.commit(make);
  return harness.commit(
    Effect.gen(function* () {
      const tx = yield* Tx;
      const doc = yield* tx.doc(Receipts, requestId, null);
      const existing = doc.receipt;
      if (existing !== undefined) {
        const same =
          existing.command === (from === undefined ? "create" : "fork") &&
          existing.from?.conversationId === from?.conversationId &&
          existing.from?.entryId === from?.entryId;
        if (!same) return yield* new RequestConflict({ requestId });
        return recordOf(existing.conversation);
      }
      const created = yield* make;
      doc.receipt = {
        command: from === undefined ? "create" : "fork",
        conversation:
          created.parent === undefined
            ? { id: created.id }
            : { id: created.id, parent: { ...created.parent } },
      };
      if (from !== undefined) {
        doc.receipt.from = { conversationId: from.conversationId, entryId: from.entryId };
      }
      return created;
    }),
  );
};

const recordOf = (stored: {
  readonly id: number;
  readonly parent?: { readonly conversationId: number; readonly at: number };
}): ConversationRecord =>
  stored.parent === undefined
    ? { id: ConversationId.make(stored.id) }
    : {
        id: ConversationId.make(stored.id),
        parent: {
          conversationId: ConversationId.make(stored.parent.conversationId),
          at: EntryId.make(stored.parent.at),
        },
      };

/**
 * A conversation's committed active transcript and the model context derived
 * from it: what `Conversation.context` returns
 * and what a request sends.
 *
 * @since 0.0.0
 */
import * as Schema from "effect/Schema";
import { ConversationId, EntryId } from "./Ids.ts";
import type { Message } from "./Message.ts";
import type { EntryRecord } from "./Records.ts";

/**
 * Raw active transcript and derived model context.
 *
 * @category models
 * @since 0.0.0
 */
export interface ContextView {
  /** Newest applicable head marker, if any. */
  readonly head: EntryRecord | undefined;
  /** Raw active entries: the head marker followed by non-head entries from its head through the tail. */
  readonly entries: ReadonlyArray<EntryRecord>;
  /** Per entry of `entries`, its model messages after edits and excluded stop reasons, before tool result ordering. */
  readonly contributions: ReadonlyArray<ReadonlyArray<Message>>;
  /** Model context for the next provider request. */
  readonly messages: ReadonlyArray<Message>;
}

/**
 * A context read named an entry that is not visible from the conversation;
 * the Session stays healthy.
 *
 * @category errors
 * @since 0.0.0
 */
export class EntryNotVisible extends Schema.TaggedError<EntryNotVisible>()("EntryNotVisible", {
  conversationId: ConversationId,
  entryId: EntryId,
}) {
  override get message(): string {
    return `Entry ${this.entryId} is not visible from conversation ${this.conversationId}`;
  }
}

/**
 * The conversation view: one structural value per
 * conversation, with a window over its transcript and its built-in
 * documents, advanced from every commit that changes them. A client follows
 * it with a watch (`Conversation.watch`), with the same folded frames as a
 * document's.
 *
 * @since 0.0.0
 */
import type * as Schema from "effect/Schema";
import type { DocumentWatch } from "./Doc.ts";
import type { EntryId } from "./Ids.ts";
import type { ConversationRecord, EntryRecord } from "./Records.ts";

/**
 * One conversation's view.
 *
 * @category models
 * @since 0.0.0
 */
export interface ConversationView {
  readonly conversation: ConversationRecord;
  /**
   * A window over the transcript, its fork-aware history (`Conversation.entries`), in entry ID order. Head
   * markers such as a reset sit where they were written; their `head` tells where the entries they
   * summarize end.
   */
  readonly entries: ReadonlyArray<EntryRecord>;
  /** ID of the entry just before the window; absent when the window starts the transcript. */
  readonly before?: EntryId | undefined;
  /** Built-in conversation documents keyed by kind; absent documents are absent. */
  readonly docs: Readonly<Record<string, Schema.JsonObject>>;
}

/**
 * Which entries a view's window holds.
 * The window starts with the newest `min` entries, takes appends, and once it
 * would hold more than `max`, drops its oldest in one splice down to `min`.
 * With `from`, it starts at `from` instead when at most `max` entries are at
 * or after it. Bounds that are not integers, or `max` below `min`, are a
 * `RangeError` defect.
 *
 * @category models
 * @since 0.0.0
 */
export interface ConversationWindow {
  /**
   * Where the first value's window starts, inclusive, such as right after the newest entry a client has
   * (`ConversationTranscript.resume`). It selects only that first value: every entry committed after the
   * attachment is appended. A `from` past the newest entry starts the window empty, with `before` the newest
   * entry.
   */
  readonly from?: EntryId | undefined;
  /** Default 50; an integer, at least 1. */
  readonly min?: number | undefined;
  /** Default `2 * min`; an integer, at least `min`. */
  readonly max?: number | undefined;
}

/**
 * The view as a watch. A view never retires, so its
 * frames never set `retired`.
 *
 * @category models
 * @since 0.0.0
 */
export type ConversationWatch = DocumentWatch<ConversationView>;

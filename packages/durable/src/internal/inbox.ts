// The `core.inbox` queue and its boundaries. A boundary loads what it reads before the commit's first table write
// (`prepareBoundary`), decides which items it selects in one pure step (`planBoundary`), and then applies the
// plan's writes in order. Whether a write is stale is decided as it is placed: a reset placed earlier at the same
// boundary moves the range's start to its own entry, whose ID exists only once it is appended.
import type * as Tracker from "@effect-durable-harness/delta/Tracker";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import { type EntryDraft, UserEntry } from "../Entry.ts";
import type { ConversationId, EntryId, SubmissionId } from "../Ids.ts";
import { InboxDoc, type InboxItem, type InputEntry } from "../Inbox.ts";
import type { UserContent } from "../Message.ts";
import type { EntryRecord } from "../Records.ts";
import type { QueueMode, Settings } from "../Settings.ts";
import { Tx, type TxDocError, type TxReadError, type TxWriteError } from "../Tx.ts";

/** The settings a boundary reads, on the Session line. */
export type QueueModes = Pick<Settings, "steeringMode" | "followUpMode">;

/** What a boundary reads before the commit's first table write, and the newest head it has seen so far. */
export interface Boundary {
  readonly conversationId: ConversationId;
  readonly inbox: Tracker.Draft<{ items: Array<InboxItem> }>;
  readonly steeringMode: QueueMode;
  readonly followUpMode: QueueMode;
  /** Start of the active range, the newest head marker's `head`; advanced by heads written in this commit. */
  head: EntryId | undefined;
}

/**
 * Selected user items, in ID order, the first entry the boundary placed, a write's or a user item's (a run it
 * starts begins there, `LiveRun.first`), and whether a `head: "self"` write (a reset) was placed.
 */
export interface BoundaryResult {
  readonly users: ReadonlyArray<SubmissionId>;
  readonly first: EntryId | undefined;
  readonly reset: boolean;
}

/** One boundary's decision: what it places, in order, and the inbox positions it removes. */
export interface BoundaryPlan {
  /** Selected writes in ID order; one stale when it is placed settles `unanswered` instead. */
  readonly writes: ReadonlyArray<number>;
  /** Selected user items in ID order. */
  readonly users: ReadonlyArray<number>;
  readonly reset: boolean;
  /** Positions to remove, highest first. */
  readonly removed: ReadonlyArray<number>;
}

/**
 * Reads what a boundary needs. Table reads must precede the commit's
 * first table write, so callers prepare the boundary at the start of their commit.
 */
export const prepareBoundary = (
  conversationId: ConversationId,
  modes: QueueModes,
): Effect.Effect<Boundary, TxReadError | TxDocError, Tx> =>
  Effect.gen(function* () {
    const tx = yield* Tx;
    const head = Option.getOrUndefined(yield* tx.latestHeadMarker(conversationId))?.head;
    const inbox = yield* tx.doc(InboxDoc, conversationId);
    return {
      conversationId,
      // SAFETY: the draft of `core.inbox`, whose value is `InboxState`; the boundary edits its items in place.
      inbox: inbox as Tracker.Draft<{ items: Array<InboxItem> }>,
      steeringMode: modes.steeringMode,
      followUpMode: modes.followUpMode,
      head,
    };
  });

/**
 * The decision of a boundary: every write, the first or all steers, and at `final` the
 * first or all follow-ups. A selected reset turns a `postTools` boundary into `final`. Writes are placed first
 * and user items after them, each in ID order.
 */
export const planBoundary = (
  items: ReadonlyArray<InboxItem>,
  modes: QueueModes,
  at: "postTools" | "final",
): BoundaryPlan => {
  const reset = hasQueuedReset(items);
  const final = at === "final" || reset;
  const pick = (mode: "steer" | "followUp", queueMode: QueueMode): Array<number> => {
    const indexes = items.flatMap((item, index) => (item.mode === mode ? [index] : []));
    return queueMode === "all" ? indexes : indexes.slice(0, 1);
  };
  const writes = items.flatMap((item, index) => (item.mode === "write" ? [index] : []));
  const users = [
    ...pick("steer", modes.steeringMode),
    ...(final ? pick("followUp", modes.followUpMode) : []),
  ].toSorted((a, b) => a - b);
  const removed = [...writes, ...users].toSorted((a, b) => b - a);
  return { writes, users, reset, removed };
};

/**
 * The writes of a boundary: places the plan's items and removes them positionally. A write whose head targets an
 * entry before the active range, including a range a write placed earlier at this boundary started, settles
 * `stale`. `appendEntry` copies the drafts' values, so the items are removed only afterwards.
 */
export const applyBoundary = (
  boundary: Boundary,
  at: "postTools" | "final",
  now: number,
): Effect.Effect<BoundaryResult, TxWriteError, Tx> =>
  Effect.gen(function* () {
    const tx = yield* Tx;
    const items = boundary.inbox.items;
    const plan = planBoundary(items, boundary, at);
    let first: EntryId | undefined;
    for (const index of plan.writes) {
      const item = items[index];
      if (item?.mode !== "write") continue;
      // SAFETY: a queued write stores its `EntryDraft` as plain JSON.
      const draft = item.entry as EntryDraft;
      if (isStale(boundary.head, draft)) {
        yield* tx.settleSubmission(item.id, { status: "unanswered", reason: "stale" });
        continue;
      }
      const entry = yield* tx.appendEntry(boundary.conversationId, draft);
      if (draft.head !== undefined) boundary.head = draft.head === "self" ? entry.id : draft.head;
      yield* tx.placeSubmission(item.id, entry.id);
      first ??= entry.id;
    }
    const placed: Array<SubmissionId> = [];
    for (const index of plan.users) {
      const item = items[index];
      if (item === undefined || item.mode === "write") continue;
      const entry = yield* appendInput(boundary.conversationId, item.content, item.entry, now);
      yield* tx.placeSubmission(item.id, entry.id);
      placed.push(item.id);
      first ??= entry.id;
    }
    for (const index of plan.removed) items.splice(index, 1);
    return { users: placed, first, reset: plan.reset };
  });

/**
 * The entry an input is placed as: its own kind and data, or `core.user`, with the user message of `content`.
 */
export const appendInput = (
  conversationId: ConversationId,
  content: UserContent,
  entry: InputEntry | undefined,
  now: number,
): Effect.Effect<EntryRecord, TxWriteError, Tx> =>
  Effect.gen(function* () {
    const tx = yield* Tx;
    const message = { role: "user", content, timestamp: now } as const;
    if (entry === undefined)
      return yield* tx.appendEntry(UserEntry, conversationId, { model: [message] });
    return yield* tx.appendEntry(conversationId, { ...pickInputEntry(entry), model: [message] });
  });

/** The kind and data of an input's entry, without other fields. */
export const pickInputEntry = (entry: InputEntry): InputEntry =>
  entry.data === undefined ? { kind: entry.kind } : { kind: entry.kind, data: entry.data };

/**
 * A write with no `model`, `head` or `edits` (present counts, even if
 * empty) changes nothing the model sees and needs no boundary.
 */
export const isDisplayOnly = (entry: EntryDraft): boolean =>
  entry.model === undefined && entry.head === undefined && entry.edits === undefined;

/** Whether a `head: "self"` write is queued. */
export const hasQueuedReset = (items: ReadonlyArray<InboxItem>): boolean =>
  items.some((item) => item.mode === "write" && item.entry.head === "self");

/**
 * Whether a head write targets an entry before the active range, which
 * starts at `start`, a boundary's head.
 */
export const isStale = (start: number | undefined, entry: Pick<EntryDraft, "head">): boolean =>
  entry.head !== undefined && entry.head !== "self" && start !== undefined && entry.head < start;

/** Removes a withdrawn submission's item; the caller settles it. */
export const removeInboxItem = (
  conversationId: ConversationId,
  id: SubmissionId,
): Effect.Effect<void, TxDocError, Tx> =>
  Effect.gen(function* () {
    const tx = yield* Tx;
    const items = (yield* tx.doc(InboxDoc, conversationId)).items;
    const index = items.findIndex((item) => item.id === id);
    if (index >= 0) items.splice(index, 1);
  });

/**
 * Withdraws every queued input of a conversation, as
 * `Conversation.abort`, cascades and `afterFailedRun: "withdraw"` do: each settles `unanswered` with `reason`;
 * queued writes stay for later placement.
 */
export const withdrawQueuedInputs = (
  conversationId: ConversationId,
  reason = "aborted",
): Effect.Effect<void, TxDocError, Tx> =>
  Effect.gen(function* () {
    const tx = yield* Tx;
    const items = (yield* tx.doc(InboxDoc, conversationId)).items;
    for (let index = items.length - 1; index >= 0; index--) {
      const item = items[index];
      if (item === undefined || item.mode === "write") continue;
      yield* tx.settleSubmission(item.id, { status: "unanswered", reason });
      items.splice(index, 1);
    }
  });

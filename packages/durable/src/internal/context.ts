/**
 * Context derivation: the pure part. Given the bounds of one committed context
 * range and the visible entries scanned within them, oldest first, these
 * functions derive the raw active entries and the model context.
 *
 * Reading storage needs the Session line, so it lives with the Harness's
 * context reads and their per-conversation cache: they capture the bounds on
 * the line, scan `rangeQuery(bounds)` off it, and call `deriveRange` or
 * `extendRange` here. `deriveContext` and `activeEntries` take the scanned
 * entries rather than the storage.
 *
 * Stored and returned values are not frozen; a caller that keeps a range
 * across reads hands out copies of its arrays.
 */
import type { ContextView } from "../ContextView.ts";
import type { EntryQuery } from "../AgentStorage.ts";
import type { EntryId } from "../Ids.ts";
import { deepFreeze } from "./freeze.ts";
import type { AssistantMessage, Message, ToolCall, ToolResultMessage } from "../Message.ts";
import type { ContextEdit, EntryRecord } from "../Records.ts";
import { SystemEntry } from "../Entry.ts";

// These assistant messages stay in the transcript but never reach a provider.
const EXCLUDED_STOP_REASONS: ReadonlySet<AssistantMessage["stopReason"]> = new Set([
  "aborted",
  "error",
  "deferred",
]);
const MISSING_RESULT_TEXT = "Tool result unavailable: history ends before this call completed.";

/** Head marker and newest visible entry that fix one committed context range. */
export interface ContextBounds {
  readonly head: (EntryRecord & { readonly head: EntryId }) | undefined;
  readonly tail: EntryId;
}

export type { ContextView };

/**
 * One context read: the visible entries it scanned, from the head marker's
 * head, or transcript start, through `bounds.tail`, oldest first, and the view
 * derived from them. Entries at or below the tail never change, so a later read
 * with the same head marker extends both.
 */
export interface ContextRange {
  readonly bounds: ContextBounds;
  readonly entries: ReadonlyArray<EntryRecord>;
  readonly view: ContextView;
  /** Targets of the edits in `entries`. */
  readonly edited: ReadonlySet<EntryId>;
  /**
   * `view.messages` from the contributed messages before the last assistant
   * message. Tool results are ordered within the messages up to the next
   * assistant message, so later entries cannot change these.
   */
  readonly settled: ReadonlyArray<Message>;
  /** Contributed messages from the last assistant message on, before tool result ordering. */
  readonly open: ReadonlyArray<Message>;
  /** The baseline's system message; see `findBaseline`. */
  readonly baseline: Baseline;
  /**
   * Whether the range reached the conversation's newest entry when it was read, so the entries of later
   * commits extend it (`advanceRange`); a range cut off at an earlier entry is not.
   */
  readonly current: boolean;
}

/**
 * A baseline's system message, `null` when the range cannot have one, or `undefined` while a later entry may
 * be one.
 */
export type Baseline = Message | null | undefined;

/** Messages split by `settle`. */
export interface Settled {
  readonly settled: ReadonlyArray<Message>;
  readonly open: ReadonlyArray<Message>;
}

/**
 * The view of the context within `bounds`, derived from the visible entries of
 * `rangeQuery(bounds)`, oldest first; empty without bounds.
 */
export const deriveContext = (
  bounds: ContextBounds | undefined,
  entries: ReadonlyArray<EntryRecord>,
): ContextView => (bounds === undefined ? emptyView() : deriveRange(bounds, entries, false).view);

/** The view of an empty transcript. */
export const emptyView = (): ContextView => ({
  head: undefined,
  entries: [],
  contributions: [],
  messages: [],
});

/** Derive the context range from the entries scanned within `bounds`. */
export const deriveRange = (
  bounds: ContextBounds,
  entries: ReadonlyArray<EntryRecord>,
  current: boolean,
): ContextRange => {
  // The newest edit per target wins. Edits of every entry in the range count,
  // including older head markers that `selectActive` drops.
  const edits = new Map<EntryId, ContextEdit>();
  for (const entry of entries) for (const edit of entry.edits ?? []) edits.set(edit.target, edit);
  const active = selectActive(bounds.head, entries);
  const contributions = active.map((entry) => contribute(entry, edits.get(entry.id)));
  const split = settle([], contributions.flat());
  const baseline = findBaseline(bounds.head, active);
  const messages = leadWithSystem([...split.settled, ...orderToolResults(split.open)], baseline);
  const view: ContextView = { head: bounds.head, entries: active, contributions, messages };
  return {
    bounds,
    entries,
    view,
    edited: new Set(edits.keys()),
    settled: split.settled,
    open: split.open,
    baseline,
    current,
  };
};

/**
 * `previous` extended by the entries `added` after its tail under the same
 * head marker: only their contributions and the open messages are derived. An
 * added edit can change an earlier entry, so it derives the whole range again.
 */
export const extendRange = (
  previous: ContextRange,
  bounds: ContextBounds,
  added: ReadonlyArray<EntryRecord>,
  current: boolean,
): ContextRange => {
  const entries = [...previous.entries, ...added];
  if (
    added.some(
      (entry) =>
        entry.edits !== undefined || entry.head !== undefined || previous.edited.has(entry.id),
    )
  ) {
    return deriveRange(bounds, entries, current);
  }
  const contributions = added.map((entry) => contribute(entry, undefined));
  const split = settle(previous.settled, [...previous.open, ...contributions.flat()]);
  const baseline =
    previous.baseline !== undefined ? previous.baseline : findBaseline(bounds.head, added);
  const view: ContextView = {
    head: bounds.head,
    entries: [...previous.view.entries, ...added],
    contributions: [...previous.view.contributions, ...contributions],
    messages: leadWithSystem([...split.settled, ...orderToolResults(split.open)], baseline),
  };
  return {
    bounds,
    entries,
    view,
    edited: previous.edited,
    settled: split.settled,
    open: split.open,
    baseline,
    current,
  };
};

/**
 * `range` with the entries one commit appended to its
 * conversation, in ID order, without reading storage: the conversation's own new entries are its newest, and
 * a new head marker's head never lies before the range's start. A range that is not `current` stays as it is;
 * the next read extends it from storage. `undefined` drops the range.
 */
export const advanceRange = (
  range: ContextRange,
  appended: ReadonlyArray<EntryRecord>,
): ContextRange | undefined => {
  const last = appended.at(-1);
  if (!range.current || last === undefined) return range;
  // The published entries are the objects `tx.appendEntry` returned,
  // so a caller that changes one must not change the kept range, nor a later view.
  for (const entry of appended) deepFreeze(entry);
  let head = range.bounds.head;
  for (const entry of appended) if (isHeadMarker(entry)) head = entry;
  const bounds: ContextBounds = { head, tail: last.id };
  // Without a head marker the head is unchanged; a changed head is one of the appended markers.
  if (head === undefined || head.id === range.bounds.head?.id) {
    return extendRange(range, bounds, appended, true);
  }
  const start = head.head;
  // Heads only move forward: compaction keeps a later entry, a reset starts at itself, an older head is stale.
  if (range.bounds.head !== undefined && start < range.bounds.head.head) return undefined;
  const entries = [...range.entries, ...appended].filter((entry) => entry.id >= start);
  return deriveRange(bounds, entries, true);
};

/**
 * Entry `id` of `range`, by binary search; a range holds its
 * entries in ID order, every one visible from its conversation.
 */
export const rangeEntry = (range: ContextRange, id: EntryId): EntryRecord | undefined => {
  const entries = range.entries;
  let low = 0;
  let high = entries.length - 1;
  while (low <= high) {
    const middle = (low + high) >>> 1;
    const entry = entries[middle];
    if (entry === undefined) return undefined;
    if (entry.id === id) return entry;
    if (entry.id < id) low = middle + 1;
    else high = middle - 1;
  }
  return undefined;
};

const isHeadMarker = (entry: EntryRecord): entry is EntryRecord & { readonly head: EntryId } =>
  entry.head !== undefined;

/**
 * The baseline among `entries`, oldest first: the system
 * message of the first `core.system` entry after the head marker, or of the transcript without one, when
 * generation wrote it with every section and tool in effect. After a head marker it always does, and omits
 * the retained `core.system` entries. Without one it does when it saw no system message before, which holds
 * unless an earlier entry carries one, even if an edit omits it later. An edit that targets the baseline
 * contributes other messages, or none, so none leads; a later entry does not take its place.
 */
export const findBaseline = (
  head: ContextBounds["head"],
  entries: ReadonlyArray<EntryRecord>,
): Baseline => {
  for (const entry of entries) {
    if (head !== undefined && entry.id <= head.id) continue;
    if (SystemEntry.is(entry)) {
      return entry.model?.find((message) => message.role === "system") ?? null;
    }
    if (head === undefined && carriesSystemMessage(entry)) return null;
  }
  return undefined;
};

/** Whether an entry's own messages, or the replacement messages of its edits, include a system message. */
const carriesSystemMessage = (entry: EntryRecord): boolean =>
  (entry.model?.some(isSystem) ?? false) ||
  (entry.edits?.some((edit) => edit.action === "replace" && edit.messages.some(isSystem)) ?? false);

const isSystem = (message: Message): boolean => message.role === "system";

/**
 * Move the first system message to the front when only user
 * messages precede it, or when it is the baseline's. A run's input is committed before generation renders
 * the system prompt, so a transcript starts with the input followed by the baseline, and the range after a
 * compaction with the kept history, assistant messages included. Providers treat only a leading system
 * message as the initial prompt and tool set; without it, a later tool change rewrites the request's tool
 * list and invalidates the whole prompt cache. Only the first system message moves, so replaying system
 * messages in order gives the same sections and tools. Any other system message, before an assistant
 * message, stays where it was committed: moving it would change what it means to the model.
 */
export const leadWithSystem = (
  messages: ReadonlyArray<Message>,
  baseline: Baseline,
): ReadonlyArray<Message> => {
  const index = messages.findIndex((message) => message.role === "system");
  const system = messages[index];
  if (index <= 0 || system === undefined) return messages;
  if (system !== baseline && messages.slice(0, index).some((message) => message.role !== "user")) {
    return messages;
  }
  return [system, ...messages.slice(0, index), ...messages.slice(index + 1)];
};

/**
 * One active entry's model messages after its edit and excluded stop reasons,
 * before tool result ordering.
 */
export const contribute = (
  entry: EntryRecord,
  edit: ContextEdit | undefined,
): ReadonlyArray<Message> => {
  if (edit?.action === "omit") return [];
  const replaced = edit?.action === "replace" ? edit.messages : undefined;
  const contributed = replaced ?? entry.model ?? [];
  return contributed.filter(
    (message) => message.role !== "assistant" || !EXCLUDED_STOP_REASONS.has(message.stopReason),
  );
};

/**
 * Move the ordered messages before the last assistant message of `open` to
 * `settled`. `orderToolResults` of a sequence equals the concatenation over its
 * parts when each later part starts with an assistant message.
 */
export const settle = (settled: ReadonlyArray<Message>, open: ReadonlyArray<Message>): Settled => {
  const last = open.findLastIndex((message) => message.role === "assistant");
  if (last <= 0) return { settled, open };
  return {
    settled: [...settled, ...orderToolResults(open.slice(0, last))],
    open: open.slice(last),
  };
};

/**
 * The raw active entries within `bounds`, without deriving model context, from
 * the visible entries of `rangeQuery(bounds)`, oldest first. The view uses it.
 */
export const activeEntries = (
  bounds: ContextBounds | undefined,
  entries: ReadonlyArray<EntryRecord>,
): ReadonlyArray<EntryRecord> => (bounds === undefined ? [] : selectActive(bounds.head, entries));

/**
 * The visible range of `bounds`, as an entry query: from the head marker's
 * head, or transcript start, through the tail.
 */
export const rangeQuery = (bounds: ContextBounds): Omit<EntryQuery, "conversationId"> =>
  bounds.head === undefined
    ? { maxEntryId: bounds.tail }
    : { minEntryId: bounds.head.head, maxEntryId: bounds.tail };

/**
 * The head marker followed by the
 * range's non-head entries, or the whole range without a marker.
 */
export const selectActive = (
  head: ContextBounds["head"],
  range: ReadonlyArray<EntryRecord>,
): ReadonlyArray<EntryRecord> =>
  head === undefined ? range : [head, ...range.filter((entry) => entry.head === undefined)];

/**
 * Place each assistant's tool
 * results directly after it in call order. Results are taken from the messages
 * before the next assistant; a missing result is synthesized and unmatched
 * results are dropped.
 */
export const orderToolResults = (messages: ReadonlyArray<Message>): Array<Message> => {
  const ordered: Array<Message> = [];
  messages.forEach((message, index) => {
    if (message.role === "toolResult") return;
    ordered.push(message);
    if (message.role !== "assistant") return;
    const calls = message.content.filter(
      (content): content is ToolCall => content.type === "toolCall",
    );
    if (calls.length === 0) return;
    const results = new Map<string, ToolResultMessage>();
    for (let next = index + 1; next < messages.length; next++) {
      const candidate = messages[next];
      if (candidate === undefined || candidate.role === "assistant") break;
      // The first result for a call wins; later duplicates are dropped.
      if (candidate.role === "toolResult" && !results.has(candidate.toolCallId)) {
        results.set(candidate.toolCallId, candidate);
      }
    }
    for (const call of calls)
      ordered.push(results.get(call.id) ?? missingResult(call, message.timestamp));
  });
  return ordered;
};

/** The error result synthesized for a call without one. */
export const missingResult = (call: ToolCall, timestamp: number): ToolResultMessage => ({
  role: "toolResult",
  toolCallId: call.id,
  toolName: call.name,
  content: [{ type: "text", text: MISSING_RESULT_TEXT }],
  isError: true,
  details: { reason: "missing_result" },
  timestamp,
});

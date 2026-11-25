/**
 * What a client has of one conversation: every entry it got from a view's
 * window or fetched, and the gaps between. A layer over any `ConversationView`, wherever it comes from (a
 * watch, or a replica of it on another host). It imports no kernel runtime, so
 * a browser bundle can use it.
 *
 * @since 0.0.0
 */
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import type { EntryQuery, Page } from "./AgentStorage.ts";
import type { EntryId } from "./Ids.ts";
import type { EntryRecord } from "./Records.ts";
import type { ConversationView, ConversationWindow } from "./View.ts";

/**
 * One row of a client's transcript.
 *
 * @category models
 * @since 0.0.0
 */
export type TranscriptItem =
  | { readonly kind: "entry"; readonly entry: EntryRecord }
  /** Entries strictly between `after` and `upTo` are missing: render "load more" and call `fillGap`. */
  | { readonly kind: "gap"; readonly after: EntryId; readonly upTo: EntryId }
  /** The transcript continues above: render "load earlier" and call `loadEarlier`. Always the first item. */
  | { readonly kind: "earlier" };

/**
 * A gap item.
 *
 * @category models
 * @since 0.0.0
 */
export type TranscriptGap = Extract<TranscriptItem, { readonly kind: "gap" }>;

/**
 * The conversation view as a renderer sees it: `items`, the client's whole
 * transcript, instead of the window.
 *
 * @category models
 * @since 0.0.0
 */
export interface TranscriptView {
  readonly conversation: ConversationView["conversation"];
  readonly docs: ConversationView["docs"];
  readonly items: ReadonlyArray<TranscriptItem>;
}

/**
 * Pages a conversation's entries, as `Conversation.entries` with these
 * bounds and order does: at most `limit` items, and `next` when more follow.
 *
 * @category models
 * @since 0.0.0
 */
// The loader is an Effect, cancelled by interruption. Only whether `next` is set matters, never the cursor.
export type EntryLoader<E = never> = (
  query: Omit<EntryQuery, "conversationId">,
  limit: number,
) => Effect.Effect<Page<EntryRecord>, E>;

/**
 * Options of `make`.
 *
 * @category models
 * @since 0.0.0
 */
export interface ConversationTranscriptOptions {
  /** Entries `loadEarlier` and `fillGap` load per call; default 50. */
  readonly pageSize?: number | undefined;
}

/**
 * A client's transcript of one conversation.
 *
 * @category models
 * @since 0.0.0
 */
export interface ConversationTranscript<E = never> {
  /** Takes one value of the view: its window's entries join the transcript. O(items); never fetches. */
  readonly merge: (view: ConversationView) => TranscriptView;
  /**
   * Loads the page before the oldest entry held; without anything earlier, the current view. Concurrent
   * calls share one load. Before the first merge it dies.
   */
  readonly loadEarlier: Effect.Effect<TranscriptView, E>;
  /**
   * Loads the next page of `gap`, shrinking it from its start, or removing it once nothing is missing. A gap
   * that shrank since loads the gap it became; one that is gone gives the current view. Concurrent calls for
   * one gap share one load.
   */
  readonly fillGap: (gap: TranscriptGap) => Effect.Effect<TranscriptView, E>;
  /** The ID the transcript reaches: its newest entry, or a later empty window's `before`. */
  readonly newest: () => EntryId | undefined;
  /** `{ from }` right after `newest`, `{}` for an empty transcript, undefined before the first merge. */
  readonly resume: () => ConversationWindow | undefined;
}

const EARLIER: TranscriptItem = Object.freeze({ kind: "earlier" });

type EntryItem = Extract<TranscriptItem, { readonly kind: "entry" }>;

/**
 * Makes an empty transcript over `load`. A merge, `loadEarlier` or `fillGap`
 * that adds nothing returns the previous `items` array, and with the same
 * `docs` and `conversation` the previous view itself. A load that fails, or
 * dies, is not kept: the next call loads again.
 *
 * **Gotchas**
 *
 * A page size that is not an integer of at least 1 throws a `RangeError`.
 *
 * @category constructors
 * @since 0.0.0
 */
// A constructor function over a closure, and `newest` and `resume` are functions. Shared loads are fibers
// forked with the caller's services, so the first caller's interruption does not cancel the load the others
// wait on.
export const make = <E = never>(
  load: EntryLoader<E>,
  options?: ConversationTranscriptOptions,
): ConversationTranscript<E> => {
  const pageSize = options?.pageSize ?? 50;
  if (!Number.isSafeInteger(pageSize) || pageSize < 1) {
    throw new RangeError(`Page size must be an integer >= 1, got ${pageSize}`);
  }
  /** Every entry held, in ID order, as items. */
  let entries: Array<EntryItem> = [];
  const ids = new Set<EntryId>();
  /** Gaps in ID order; each is an immutable item, replaced when it shrinks. */
  let gaps: Array<TranscriptGap> = [];
  let started = false;
  /** Exclusive lower bound of the known range: entries at or below it are not loaded. */
  let low = Number.NEGATIVE_INFINITY;
  /** Inclusive upper bound of the known range: the newest entry, or a later empty window's `before`. */
  let tail = Number.NEGATIVE_INFINITY;
  let items: ReadonlyArray<TranscriptItem> = [];
  let last: ConversationView | undefined;
  let current: TranscriptView | undefined;
  const loads = new Map<string, Fiber.Fiber<TranscriptView, E>>();

  /** The view with the current items; the previous one when nothing changed. */
  const result = (): TranscriptView => {
    // SAFETY: called only after a merge.
    const view = last!;
    if (
      current !== undefined &&
      current.items === items &&
      current.docs === view.docs &&
      current.conversation === view.conversation
    ) {
      return current;
    }
    current = { conversation: view.conversation, docs: view.docs, items };
    return current;
  };

  /** Items in ID order: "earlier" until the start is known, then entries with each gap before the entry after it. */
  const build = (): Array<TranscriptItem> => {
    const built: Array<TranscriptItem> = low === Number.NEGATIVE_INFINITY ? [] : [EARLIER];
    let next = 0;
    for (const item of entries) {
      while (next < gaps.length && gaps[next]!.after < item.entry.id) built.push(gaps[next++]!);
      built.push(item);
    }
    while (next < gaps.length) built.push(gaps[next++]!);
    return built;
  };

  /** The gaps without (`lo`, `hi`]; the same array when none changes, as when the range is empty. */
  const subtract = (lo: number, hi: number): Array<TranscriptGap> => {
    if (hi <= lo) return gaps;
    let changed = false;
    const kept: Array<TranscriptGap> = [];
    for (const gap of gaps) {
      // Missing: IDs in (after, upTo); known now: IDs in (lo, hi].
      if (hi <= gap.after || lo + 1 >= gap.upTo) {
        kept.push(gap);
        continue;
      }
      changed = true;
      if (lo > gap.after) {
        const firstKnown = entries.find((item) => item.entry.id > lo)?.entry.id ?? lo + 1;
        kept.push(gapOf(gap.after, Math.min(gap.upTo, firstKnown)));
      }
      if (hi + 1 < gap.upTo) kept.push(gapOf(hi, gap.upTo));
    }
    return changed ? kept : gaps;
  };

  /**
   * `cover`: records that every entry with an ID in (`lo`, `hi`] is known, `added` being those entries in
   * ID order: holds the new ones, shrinks the gaps they fill, extends the known range, and opens a gap where
   * the range does not continue it.
   */
  const cover = (lo: number, hi: number, added: ReadonlyArray<EntryRecord>): void => {
    const fresh = added.filter((entry) => !ids.has(entry.id));
    for (const entry of fresh) ids.add(entry.id);
    const held = entries;
    // New entries past every held one: the items grow at their end.
    const appended =
      fresh.length === 0 || held.length === 0 || fresh[0]!.id > held.at(-1)!.entry.id;
    const newItems = fresh.map((entry): EntryItem => ({ kind: "entry", entry }));
    if (newItems.length > 0) entries = appended ? held.concat(newItems) : mergeById(held, newItems);
    let rebuild = false;
    if (!started || (held.length === 0 && gaps.length === 0 && lo >= tail)) {
      // The first range, or one after an empty transcript: it is all the client knows.
      rebuild = !started || lo !== low;
      started = true;
      low = lo;
      tail = Math.max(tail, hi);
    } else if (lo > tail) {
      // Past the newest known ID: what lies between is missing.
      const upTo = added[0]?.id ?? lo + 1;
      gaps = [...gaps, gapOf(tail, upTo)];
      tail = hi;
      rebuild = true;
    } else if (hi !== Number.NEGATIVE_INFINITY && hi < low) {
      // Below the oldest known ID: what lies between is missing.
      const upTo = held.find((item) => item.entry.id > hi)?.entry.id ?? low + 1;
      gaps = [gapOf(hi, upTo), ...gaps];
      low = lo;
      rebuild = true;
    } else {
      const remaining = subtract(lo, hi);
      if (remaining !== gaps) {
        gaps = remaining;
        rebuild = true;
      }
      if (lo < low) {
        low = lo;
        rebuild = true;
      }
      if (hi > tail) tail = hi;
    }
    if (rebuild || !appended) items = build();
    else if (newItems.length > 0) items = items.concat(newItems);
  };

  /**
   * One load per `key`, shared by concurrent callers. The check and the registration are one synchronous
   * step; the load runs on its own fiber, after the caller's step, and is forgotten once it settles.
   */
  const shared = (key: string, run: Effect.Effect<TranscriptView, E>) =>
    Effect.withFiber((fiber) => {
      const running = loads.get(key);
      if (running !== undefined) return Fiber.join(running);
      // `runForkWith` starts the fiber at once: the yield puts the load after this step, so `forked` is
      // registered before a synchronous loader settles and the shared load is forgotten.
      const forked = Effect.runForkWith(fiber.context)(
        Effect.yieldNow.pipe(
          Effect.andThen(run),
          Effect.ensuring(
            Effect.sync(() => {
              if (loads.get(key) === forked) loads.delete(key);
            }),
          ),
        ),
      );
      loads.set(key, forked);
      return Fiber.join(forked);
    });

  const requireMerged = Effect.suspend(() =>
    last === undefined
      ? Effect.die(new Error("ConversationTranscript: merge a view first"))
      : Effect.void,
  );

  return {
    merge: (view) => {
      last = view;
      const hi = view.entries.at(-1)?.id ?? view.before ?? Number.NEGATIVE_INFINITY;
      cover(view.before ?? Number.NEGATIVE_INFINITY, hi, view.entries);
      return result();
    },
    loadEarlier: requireMerged.pipe(
      Effect.andThen(
        Effect.suspend(() => {
          if (low === Number.NEGATIVE_INFINITY) return Effect.succeed(result());
          const bound = low;
          return shared(
            "earlier",
            Effect.gen(function* () {
              const page = yield* load({ maxEntryId: asId(bound), order: "descending" }, pageSize);
              const loaded = page.items.toReversed();
              const oldest = loaded[0];
              // No next page, or an empty one: the transcript starts here.
              if (Option.isNone(page.next) || oldest === undefined) {
                cover(Number.NEGATIVE_INFINITY, bound, loaded);
              } else {
                cover(oldest.id - 1, bound, loaded);
              }
              return result();
            }),
          );
        }),
      ),
    ),
    fillGap: (gap) =>
      Effect.suspend(() => {
        const target =
          gaps.find((each) => each.after === gap.after && each.upTo === gap.upTo) ??
          gaps.find((each) => each.after < gap.upTo && gap.after < each.upTo);
        if (target === undefined) return requireMerged.pipe(Effect.map(result));
        // Keyed by the gap the call resolved to, so a stale descriptor and the current one share the load.
        return shared(
          `${target.after}:${target.upTo}`,
          Effect.gen(function* () {
            const page = yield* load(
              {
                minEntryId: asId(target.after + 1),
                maxEntryId: asId(target.upTo - 1),
                order: "ascending",
              },
              pageSize,
            );
            const newest = page.items.at(-1);
            // No next page, or an empty one: nothing else is missing.
            if (Option.isNone(page.next) || newest === undefined) {
              cover(target.after, target.upTo - 1, page.items);
            } else {
              cover(target.after, newest.id, page.items);
            }
            return result();
          }),
        );
      }),
    newest: () => (tail === Number.NEGATIVE_INFINITY ? undefined : asId(tail)),
    resume: () => {
      if (!started) return undefined;
      return tail === Number.NEGATIVE_INFINITY ? {} : { from: asId(tail + 1) };
    },
  };
};

// SAFETY: entry IDs are integers; the transcript only turns exclusive bounds into inclusive ones.
const asId = (id: number): EntryId => id as EntryId;

const gapOf = (after: number, upTo: number): TranscriptGap =>
  Object.freeze({ kind: "gap", after: asId(after), upTo: asId(upTo) });

/** Two ID-ordered item lists merged in one pass. */
const mergeById = (
  left: ReadonlyArray<EntryItem>,
  right: ReadonlyArray<EntryItem>,
): Array<EntryItem> => {
  const merged: Array<EntryItem> = [];
  let i = 0;
  let j = 0;
  while (i < left.length && j < right.length) {
    merged.push(left[i]!.entry.id < right[j]!.entry.id ? left[i++]! : right[j++]!);
  }
  while (i < left.length) merged.push(left[i++]!);
  while (j < right.length) merged.push(right[j++]!);
  return merged;
};

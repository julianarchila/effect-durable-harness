// The storage reads of a context read (`captureContextBounds`, `readContextFrom`, `scanRange`): a context read captures its bounds on the Session line and scans the entries off it, which entries at or
// below the tail allow, since they never change. The derivation itself is `internal/context.ts`.
import * as Json from "@effect-durable-harness/delta/Json";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import type { ContextView } from "../ContextView.ts";
import { EntryNotVisible } from "../ContextView.ts";
import { type ConversationId, EntryId } from "../Ids.ts";
import {
  type ContextBounds,
  type ContextRange,
  deriveRange,
  emptyView,
  extendRange,
  rangeEntry,
  rangeQuery,
} from "./context.ts";
import { scanAll } from "./scan.ts";
import type { SessionKernel } from "./session.ts";

const SCAN_PAGE_SIZE = 256;

/**
 * The newest visible entry, or `at`, and the head marker at or before it, from the Session's conversation tails
 * after their first read. Run it on the Session line.
 */
export const captureContextBounds = (
  kernel: SessionKernel,
  conversationId: ConversationId,
  at: EntryId | undefined,
  /** Set when `at` is known to be visible, as an entry of a range of the conversation is. */
  visible = false,
) =>
  Effect.gen(function* () {
    let tail: EntryId;
    if (at === undefined) {
      const newest = yield* kernel.latestEntryId(conversationId);
      if (newest === undefined) return undefined;
      tail = newest;
    } else {
      if (!visible && Option.isNone(yield* kernel.storage.visibleEntry(conversationId, at))) {
        return yield* new EntryNotVisible({ conversationId, entryId: at });
      }
      tail = at;
    }
    const head = yield* kernel.latestHeadMarker(conversationId, tail);
    return { head, tail } satisfies ContextBounds;
  });

/** The visible entries of a range, oldest first. */
const scanRange = (
  kernel: SessionKernel,
  conversationId: ConversationId,
  range: ReturnType<typeof rangeQuery>,
) =>
  scanAll((cursor) =>
    // Off the line: entries at or below the tail never change. Close waits for a page underway and refuses
    // the next one.
    kernel.offLine(
      kernel.offLineStorage.scanEntries(
        { conversationId, ...range, order: "ascending" },
        SCAN_PAGE_SIZE,
        cursor,
      ),
    ),
  );

/**
 * A context read that reuses `previous`, an earlier range of the
 * same conversation: with the same head marker, only entries after its tail are scanned. Returns the view, with
 * arrays of its own, and the range to pass to the next read.
 */
export const readContextFrom = (
  kernel: SessionKernel,
  conversationId: ConversationId,
  at: EntryId | undefined,
  previous: ContextRange | undefined,
) =>
  Effect.gen(function* () {
    const visible =
      at !== undefined && previous !== undefined && rangeEntry(previous, at) !== undefined;
    const bounds = yield* kernel.readOnLine(
      captureContextBounds(kernel, conversationId, at, visible),
    );
    if (bounds === undefined) {
      return { view: emptyView(), range: undefined } satisfies {
        readonly view: ContextView;
        readonly range: ContextRange | undefined;
      };
    }
    // An `at` inside the previous range is visible; a read to the newest entry
    // is current, so the scheduler advances it at each later commit.
    const current = at === undefined;
    let range: ContextRange;
    if (previous === undefined || previous.bounds.head?.id !== bounds.head?.id) {
      range = deriveRange(
        bounds,
        yield* scanRange(kernel, conversationId, rangeQuery(bounds)),
        current,
      );
    } else if (bounds.tail === previous.bounds.tail) {
      range = previous;
    } else if (bounds.tail < previous.bounds.tail) {
      range = deriveRange(
        bounds,
        previous.entries.filter((entry) => entry.id <= bounds.tail),
        false,
      );
    } else {
      const added = yield* scanRange(kernel, conversationId, {
        minEntryId: EntryId.make(previous.bounds.tail + 1),
        maxEntryId: bounds.tail,
      });
      range = extendRange(previous, bounds, added, current);
    }
    // Entries committed while the scan ran off the line are not in the range: it is current only if none was.
    if (range.current && kernel.knownLatestEntryId(conversationId) !== range.bounds.tail) {
      range = { ...range, current: false };
    }
    // The kept range is not frozen, so the view is a deep copy: a caller that changes it cannot change the kept
    // range, nor the next read.
    const view = copyView(range.view);
    return {
      view,
      range,
    };
  });

/** A deep copy of a view, record by record. */
const copyView = (view: ContextView): ContextView => ({
  head: view.head === undefined ? undefined : copyRecord(view.head),
  entries: view.entries.map(copyRecord),
  contributions: view.contributions.map((messages) => messages.map(copyRecord)),
  messages: view.messages.map(copyRecord),
});

// SAFETY: a strict-JSON copy of a record has its shape; records hold no undefined values.
const copyRecord = <A extends object>(record: A): A => Json.copy(record) as A;

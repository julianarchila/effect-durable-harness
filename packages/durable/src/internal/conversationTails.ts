// The newest entry and newest head marker of each recently used conversation, kept current from the Session's
// commits.
//
// The newest entry ID and newest head marker per conversation, for the Session that owns its storage: every
// entry is written by one of its commits, so `apply` keeps both current, and a value read from storage stays
// valid until the next entry of that conversation. Read only on the Session line, so no commit that appends to
// the conversation is unpublished and a value read from storage can be kept. The newest entry visible from a
// conversation is its own newest or, for a fork without entries of its own, the parent's entry at the fork
// point: IDs grow, so a conversation's own new entry is always the newest. Each map is bounded by a count, the
// least recently used conversation leaving first. Kept markers are frozen: entry records are immutable.
import * as Effect from "effect/Effect";
import type { HeadMarker, StorageWrite } from "../AgentStorage.ts";
import type { ConversationId, EntryId } from "../Ids.ts";
import type { EntryRecord } from "../Records.ts";
import { deepFreeze } from "./freeze.ts";

/** Conversations whose newest entry ID is kept: IDs only, so many. */
const LATEST_CAPACITY = 4096;

/** Conversations whose newest head marker is kept: whole entries (a compaction summary is one), so fewer. */
const MARKER_CAPACITY = 256;

export interface ConversationTails {
  /** The newest entry ID visible from `conversationId`, read with `read` when not known. */
  readonly latest: <E>(
    conversationId: ConversationId,
    read: Effect.Effect<EntryId | undefined, E>,
  ) => Effect.Effect<EntryId | undefined, E>;
  /**
   * The newest visible head marker at or before `atOrBefore` (any when `undefined`), read with `read` when not
   * known. The newest marker answers every cutoff at or after it, and no marker answers every cutoff.
   */
  readonly marker: <E>(
    conversationId: ConversationId,
    atOrBefore: EntryId | undefined,
    read: Effect.Effect<HeadMarker | undefined, E>,
  ) => Effect.Effect<HeadMarker | undefined, E>;
  /** The newest entry ID if known, without reading: `null` for none, `undefined` when unknown. */
  readonly peek: (conversationId: ConversationId) => EntryId | null | undefined;
  /** At a commit's publication: applies a stored batch. */
  readonly apply: (writes: ReadonlyArray<StorageWrite>) => void;
  /** How many conversations have their newest entry kept, for tests that bound memory. */
  readonly size: () => number;
  /** How many conversations have their newest head marker kept, for tests that bound memory. */
  readonly markers: () => number;
}

/** Makes empty tails. */
export const make = (): ConversationTails => {
  /** Newest entry ID per conversation, `null` for none. */
  const latest = new Map<ConversationId, EntryId | null>();
  /** Newest head marker per conversation, `null` for none. */
  const markers = new Map<ConversationId, HeadMarker | null>();
  return {
    latest: (conversationId, read) =>
      Effect.suspend(() => {
        const known = use(latest, conversationId);
        if (known !== undefined) return Effect.succeed(known ?? undefined);
        return Effect.tap(read, (found) =>
          Effect.sync(() => put(latest, conversationId, found ?? null, LATEST_CAPACITY)),
        );
      }),
    marker: (conversationId, atOrBefore, read) =>
      Effect.suspend(() => {
        const marker = use(markers, conversationId);
        if (marker === null) return Effect.succeed(undefined);
        if (marker !== undefined && (atOrBefore === undefined || atOrBefore >= marker.id)) {
          return Effect.succeed(marker);
        }
        const newest = latest.get(conversationId);
        return Effect.tap(read, (found) =>
          Effect.sync(() => {
            // Only a read that no newer entry can change: no cutoff, or one at or after the newest entry.
            if (atOrBefore === undefined || (newest !== undefined && (newest ?? 0) <= atOrBefore)) {
              put(
                markers,
                conversationId,
                found === undefined ? null : deepFreeze(found),
                MARKER_CAPACITY,
              );
            }
          }),
        );
      }),
    peek: (conversationId) => latest.get(conversationId),
    apply: (writes) => {
      for (const write of writes) {
        if (write.type === "conversation") {
          // A new root conversation sees no entries; a fork sees its parent's up to the fork point.
          if (write.value.parent !== undefined) continue;
          put(latest, write.value.id, null, LATEST_CAPACITY);
          put(markers, write.value.id, null, MARKER_CAPACITY);
        } else if (write.type === "entry") {
          const entry = write.value;
          put(latest, entry.conversationId, entry.id, LATEST_CAPACITY);
          if (isHeadMarker(entry)) {
            put(markers, entry.conversationId, deepFreeze(entry), MARKER_CAPACITY);
          }
        }
      }
    },
    size: () => latest.size,
    markers: () => markers.size,
  };
};

const isHeadMarker = (entry: EntryRecord): entry is HeadMarker => entry.head !== undefined;

/** `map`'s value for `key`, made the most recently used. */
const use = <K, V>(map: Map<K, V>, key: K): V | undefined => {
  const value = map.get(key);
  if (value === undefined) return undefined;
  map.delete(key);
  map.set(key, value);
  return value;
};

/** Sets `key` as the most recently used, dropping the least recently used beyond `capacity`. */
const put = <K, V>(map: Map<K, V>, key: K, value: V, capacity: number): void => {
  map.delete(key);
  map.set(key, value);
  if (map.size > capacity) {
    const oldest = map.keys().next();
    if (oldest.done !== true) map.delete(oldest.value);
  }
};

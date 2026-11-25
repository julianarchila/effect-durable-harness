// A write-through cache of hot record reads for the Session that owns its storage.
//
// Storage that serves hot record reads from memory: conversations, tasks and written submissions, each table
// bounded by a count, the least recently used record leaving first, except live tasks, kept until they end, and
// the ended tasks a task waits on, kept until it moved on from its wait. A task's record is read again at each
// of its state changes, and a waiter reads the outcomes of what it waited on once it resumes; both are bounded
// by live work, which the scheduler keeps in memory too. Every commit goes through the Session, and one process
// owns a storage, so the cache applies each batch as it is published (`published`), not at its durability
// receipt, and never goes stale. A read result is kept only when no commit published since the read started
// wrote its record, as the read may not observe that commit; versions are kept only for records with a read in
// flight. Cached values are frozen and shared. Entries, scans and documents pass through: the Session keeps
// documents and the newest entry per conversation itself, and kept context ranges hold the entries a run reads.
//
// It wraps `AgentStorage`'s service and adds `published` and `sizes`; the Session builds it (`session.ts`).
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Option from "effect/Option";
import type { AgentStorage, StorageWrite } from "../AgentStorage.ts";
import type { ConversationId, SubmissionId, TaskId } from "../Ids.ts";
import type { ConversationRecord, SubmissionRecord, TaskRecord } from "../Records.ts";
import { deepFreeze } from "./freeze.ts";

type Storage = AgentStorage["Service"];

/** Records kept per table, for tests that bound memory. */
export interface RecordCacheSizes {
  readonly conversations: number;
  readonly tasks: number;
  readonly liveTasks: number;
  readonly awaited: number;
  readonly waits: number;
  readonly submissions: number;
}

export interface RecordCache {
  /** The storage with its record reads served from the cache. */
  readonly storage: Storage;
  /** Applies a batch the Session stored, when it publishes it. Its records are frozen in place and kept as they are. */
  readonly published: (writes: ReadonlyArray<StorageWrite>) => void;
  readonly sizes: () => RecordCacheSizes;
  /** Records with a read in flight, for tests that bound memory. */
  readonly readsInFlight: () => number;
}

/** Wraps `storage` with an empty cache. */
export const make = (storage: Storage): RecordCache => {
  const conversations = lru<ConversationId, ConversationRecord>(4096);
  /** Live (non-terminal) task records, until their task ends. */
  const liveTasks = new Map<TaskId, TaskRecord>();
  /**
   * Tasks some task waits on, or waited on until its wait ended and it has not moved on since: how many such
   * waits name each, and its ended record once it ended.
   */
  const awaited = new Map<TaskId, { count: number; record?: TaskRecord | undefined }>();
  /** Per task that waits, or left its wait and has not changed since: what it waited on, and whether it left. */
  const waits = new Map<TaskId, { readonly on: ReadonlyArray<TaskId>; left: boolean }>();
  // Ended task records carry inputs and outcomes, such as a tool call's arguments, so fewer are kept.
  const tasks = lru<TaskId, TaskRecord>(256);
  const submissions = lru<SubmissionId, SubmissionRecord>(256);
  /** Per record with a read in flight: the reads, and how many published commits wrote it since the first began. */
  const reads = new Map<number, { count: number; version: number }>();

  /** Keeps a frozen task record: a live one until its task ends, an ended one in the bounded table. */
  const keepTask = (record: TaskRecord) => {
    if (record.state.status === "terminal") {
      liveTasks.delete(record.id);
      const held = awaited.get(record.id);
      if (held !== undefined) held.record = record;
      else tasks.set(record.id, record);
      return;
    }
    tasks.delete(record.id);
    liveTasks.set(record.id, record);
  };

  /** A wait on `on` is over: what no other wait names goes back to the bounded table. */
  const release = (on: ReadonlyArray<TaskId>) => {
    for (const id of on) {
      const held = awaited.get(id);
      if (held === undefined) continue;
      held.count--;
      if (held.count > 0) continue;
      awaited.delete(id);
      if (held.record !== undefined) tasks.set(id, held.record);
    }
  };

  /**
   * Follows a task's waits from its written record: a wait holds the ended records of its `on` until the task,
   * after leaving the wait, changes again (or ends), as the phase that resumes reads their outcomes before its
   * next commit.
   */
  const followWait = (record: TaskRecord) => {
    const wait = waits.get(record.id);
    if (record.state.status === "waiting") {
      if (wait !== undefined) release(wait.on);
      const on = record.state.on;
      waits.set(record.id, { on, left: false });
      for (const id of on) {
        let held = awaited.get(id);
        if (held === undefined) {
          // One that ended already moves over from the bounded table.
          const ended = tasks.get(id);
          tasks.delete(id);
          held = { count: 0, record: ended };
          awaited.set(id, held);
        }
        held.count++;
      }
      return;
    }
    if (wait === undefined) return;
    if (!wait.left && record.state.status !== "terminal") {
      wait.left = true;
      return;
    }
    waits.delete(record.id);
    release(wait.on);
  };

  /** A published commit wrote record `id`: a read of it in flight may have missed that. */
  const wrote = (id: number) => {
    const inFlight = reads.get(id);
    if (inFlight !== undefined) inFlight.version++;
  };

  /** A read's result, frozen; a found record is kept unless a commit published since the read began wrote it. */
  const keep = <A extends object, E>(
    id: number,
    read: Effect.Effect<Option.Option<A>, E>,
    store: (value: A) => void,
  ): Effect.Effect<Option.Option<A>, E> =>
    Effect.uninterruptibleMask((restore) => {
      let inFlight = reads.get(id);
      if (inFlight === undefined) {
        inFlight = { count: 0, version: 0 };
        reads.set(id, inFlight);
      }
      const registered = inFlight;
      registered.count++;
      const version = registered.version;
      // The read's end, the version check and the keep run in one synchronous
      // step: a publication of the record between them would find no read in flight, bump nothing, and be
      // overwritten by this older result.
      // Only the read is interruptible: an interrupted read still ends its registration here.
      return Effect.flatMap(Effect.exit(restore(read)), (exit) =>
        Effect.suspend(() => {
          registered.count--;
          if (registered.count === 0) reads.delete(id);
          if (Exit.isSuccess(exit) && Option.isSome(exit.value)) {
            const value = exit.value.value;
            deepFreeze(value);
            if (registered.version === version) store(value);
          }
          return exit;
        }),
      );
    });

  return {
    storage: {
      ...storage,
      conversation: (id) =>
        Effect.suspend(() => {
          const cached = conversations.get(id);
          if (cached !== undefined) return Effect.succeed(Option.some(cached));
          return keep(id, storage.conversation(id), (record) => conversations.set(id, record));
        }),
      task: (id) =>
        Effect.suspend(() => {
          const cached = liveTasks.get(id) ?? awaited.get(id)?.record ?? tasks.get(id);
          if (cached !== undefined) return Effect.succeed(Option.some(cached));
          return keep(id, storage.task(id), keepTask);
        }),
      // Cached as written only: this process writes every submission it settles, and a read one is usually
      // settled.
      submission: (id) =>
        Effect.suspend(() => {
          const cached = submissions.get(id);
          return cached === undefined
            ? storage.submission(id)
            : Effect.succeed(Option.some(cached));
        }),
    },
    published: (writes) => {
      for (const write of writes) {
        switch (write.type) {
          case "conversation":
            wrote(write.value.id);
            conversations.set(write.value.id, deepFreeze(write.value));
            break;
          case "task":
            wrote(write.value.id);
            followWait(write.value);
            keepTask(deepFreeze(write.value));
            break;
          case "submission":
            wrote(write.value.id);
            submissions.set(write.value.id, deepFreeze(write.value));
            break;
          default:
            break;
        }
      }
    },
    sizes: () => ({
      conversations: conversations.size(),
      tasks: tasks.size(),
      liveTasks: liveTasks.size,
      awaited: awaited.size,
      waits: waits.size,
      submissions: submissions.size(),
    }),
    readsInFlight: () => reads.size,
  };
};

/** A map bounded to `capacity` entries that drops the least recently used one. */
const lru = <K, V>(capacity: number) => {
  const map = new Map<K, V>();
  return {
    get: (key: K): V | undefined => {
      const value = map.get(key);
      if (value === undefined) return undefined;
      map.delete(key);
      map.set(key, value);
      return value;
    },
    set: (key: K, value: V): void => {
      map.delete(key);
      map.set(key, value);
      if (map.size > capacity) {
        const oldest = map.keys().next();
        if (oldest.done !== true) map.delete(oldest.value);
      }
    },
    delete: (key: K): void => {
      map.delete(key);
    },
    size: () => map.size,
  };
};

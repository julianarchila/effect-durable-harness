// The Session's cached reads of storage records (`internal/recordCache.ts`).
import { assert, describe, it } from "@effect/vitest";
import * as Context from "effect/Context";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import { AgentStorage, type StorageWrite } from "../src/AgentStorage.ts";
import { ConversationId, TaskId } from "../src/Ids.ts";
import * as RecordCache from "../src/internal/recordCache.ts";
import * as MemoryAgentStorage from "../src/MemoryAgentStorage.ts";
import type { LiveTaskRecord, TaskRecord } from "../src/Records.ts";
import { commitWrites } from "../src/testing/CommitWrites.ts";

const task = (id: number, status: "pending" | "running" = "pending"): LiveTaskRecord => ({
  id: TaskId.make(id),
  conversationId: ConversationId.make(1),
  kind: "test.task",
  version: 1,
  input: null,
  background: false,
  abortRequested: false,
  state: { status, checkpoint: { phase: "run" } },
});

const ended = (id: number): TaskRecord => ({
  ...task(id),
  state: { status: "terminal", outcome: { status: "completed", result: id } },
});

/**
 * Memory storage that counts its task reads, and can hold the next one until released, so a commit can land
 * while the read is in flight.
 */
const countingStorage = Effect.gen(function* () {
  const context = yield* Layer.build(MemoryAgentStorage.layer);
  const storage = Context.get(context, AgentStorage);
  let taskReads = 0;
  let hold: Deferred.Deferred<void> | undefined;
  const counting: AgentStorage["Service"] = {
    ...storage,
    task: (id) =>
      Effect.suspend(() => {
        taskReads++;
        const held = hold;
        hold = undefined;
        // The read observes storage as it is now; its result is returned once the hold is released.
        return held === undefined
          ? storage.task(id)
          : storage.task(id).pipe(Effect.tap(() => Deferred.await(held)));
      }),
  };
  return {
    storage: counting,
    get taskReads() {
      return taskReads;
    },
    /** Holds the next task read after it read storage, until the returned deferred completes. */
    holdNextRead: Effect.sync(() => {
      const held = Deferred.makeUnsafe<void>();
      hold = held;
      return held;
    }),
  };
});

/** Stores `writes` as one batch, then publishes it, as the Session does. */
const commit = (cache: RecordCache.RecordCache, writes: Array<StorageWrite>) =>
  commitWrites(cache.storage, writes).pipe(
    Effect.tap(() => Effect.sync(() => cache.published(writes))),
  );

const status = (cache: RecordCache.RecordCache, id: number) =>
  Effect.map(
    cache.storage.task(TaskId.make(id)),
    (found) => Option.getOrUndefined(found)?.state.status,
  );

const conversation: StorageWrite = { type: "conversation", value: { id: ConversationId.make(1) } };

describe("cached storage", () => {
  it.effect("serves a written record without a read and follows later writes", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const backend = yield* countingStorage;
        const cache = RecordCache.make(backend.storage);
        yield* commit(cache, [conversation]);
        yield* commit(cache, [{ type: "task", value: task(2) }]);
        assert.strictEqual(yield* status(cache, 2), "pending");
        yield* commit(cache, [{ type: "task", value: task(2, "running") }]);
        assert.strictEqual(yield* status(cache, 2), "running");
        assert.strictEqual(backend.taskReads, 0);
      }),
    ),
  );

  it.effect("keeps a read only when no commit overlapped it", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const backend = yield* countingStorage;
        yield* commitWrites(backend.storage, [conversation, { type: "task", value: task(2) }]);
        const cache = RecordCache.make(backend.storage);
        const held = yield* backend.holdNextRead;
        const overlapped = yield* Effect.forkChild(cache.storage.task(TaskId.make(2)), {
          startImmediately: true,
        });
        yield* commit(cache, [{ type: "task", value: task(2, "running") }]);
        yield* Deferred.succeed(held, undefined);
        yield* Fiber.join(overlapped);
        assert.strictEqual(yield* status(cache, 2), "running");
        yield* status(cache, 2);
        assert.strictEqual(backend.taskReads, 1);
      }),
    ),
  );

  it.effect("keeps a bounded number of ended tasks, and every live one until it ends", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const backend = yield* countingStorage;
        const cache = RecordCache.make(backend.storage);
        yield* commit(cache, [conversation]);
        for (let id = 2; id < 600; id++) yield* commit(cache, [{ type: "task", value: task(id) }]);
        // More live tasks than the ended table holds: each one's next change finds it without a read.
        assert.deepInclude(cache.sizes(), { tasks: 0, liveTasks: 598 });
        for (let id = 2; id < 600; id++) yield* cache.storage.task(TaskId.make(id));
        assert.strictEqual(backend.taskReads, 0);
        for (let id = 2; id < 600; id++) yield* commit(cache, [{ type: "task", value: ended(id) }]);
        assert.deepInclude(cache.sizes(), { tasks: 256, liveTasks: 0 });
      }),
    ),
  );

  it.effect("applies a batch stored with commit when it is published, not at its receipt", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const backend = yield* countingStorage;
        const cache = RecordCache.make(backend.storage);
        yield* commit(cache, [conversation, { type: "task", value: task(2) }]);
        const batch: Array<StorageWrite> = [{ type: "task", value: task(2, "running") }];
        const receipts: Array<number> = [];
        const result = yield* cache.storage.commit([batch], (index) => receipts.push(index));
        assert.deepStrictEqual(result, { stored: 1 });
        assert.deepStrictEqual(receipts, [0]);
        // Stored, not yet published: the cache still shows what was published.
        assert.strictEqual(yield* status(cache, 2), "pending");
        cache.published(batch);
        assert.strictEqual(yield* status(cache, 2), "running");
        assert.strictEqual(backend.taskReads, 0);
      }),
    ),
  );

  it.effect(
    "drops a read a publication of its record overlapped, and keeps versions only for reads in flight",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const backend = yield* countingStorage;
          yield* commitWrites(backend.storage, [
            conversation,
            { type: "task", value: task(2) },
            { type: "task", value: task(3) },
          ]);
          const cache = RecordCache.make(backend.storage);
          const firstHeld = yield* backend.holdNextRead;
          const overlapped = yield* Effect.forkChild(cache.storage.task(TaskId.make(2)), {
            startImmediately: true,
          });
          const secondHeld = yield* backend.holdNextRead;
          const untouched = yield* Effect.forkChild(cache.storage.task(TaskId.make(3)), {
            startImmediately: true,
          });
          assert.strictEqual(cache.readsInFlight(), 2);
          // A publication of another record does not drop the read of task 3.
          cache.published([{ type: "task", value: task(2, "running") }]);
          yield* Deferred.succeed(firstHeld, undefined);
          yield* Deferred.succeed(secondHeld, undefined);
          yield* Fiber.join(overlapped);
          yield* Fiber.join(untouched);
          assert.strictEqual(cache.readsInFlight(), 0);
          assert.strictEqual(yield* status(cache, 2), "running");
          assert.strictEqual(yield* status(cache, 3), "pending");
          assert.strictEqual(backend.taskReads, 2);
        }),
      ),
  );

  it.effect("keeps the ended tasks a task waits on until it moved on from its wait", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const backend = yield* countingStorage;
        const cache = RecordCache.make(backend.storage);
        yield* commit(cache, [conversation]);
        const waiting = (state: LiveTaskRecord["state"]): TaskRecord => ({ ...task(2), state });
        const on = [3, 4].map((id) => TaskId.make(id));
        const waitState: LiveTaskRecord["state"] = {
          status: "waiting",
          checkpoint: { phase: "read" },
          on,
          policy: "allSettled",
        };
        yield* commit(cache, [{ type: "task", value: waiting(waitState) }]);
        yield* commit(cache, [
          { type: "task", value: ended(3) },
          { type: "task", value: ended(4) },
        ]);
        // A burst of other ended tasks fills the bounded table.
        for (let id = 10; id < 300; id++)
          yield* commit(cache, [{ type: "task", value: ended(id) }]);
        assert.deepInclude(cache.sizes(), { awaited: 2, waits: 1 });
        // Resumed: still held for the phase that reads the outcomes.
        yield* commit(cache, [
          { type: "task", value: waiting({ status: "running", checkpoint: { phase: "read" } }) },
        ]);
        for (const id of on) assert.strictEqual(yield* status(cache, id), "terminal");
        assert.strictEqual(backend.taskReads, 0);
        assert.deepInclude(cache.sizes(), { awaited: 2, waits: 1 });
        // Its next change ends the hold: the records go back to the bounded table, and no wait is left.
        yield* commit(cache, [
          { type: "task", value: waiting({ status: "running", checkpoint: { phase: "next" } }) },
        ]);
        assert.deepInclude(cache.sizes(), { awaited: 0, waits: 0, tasks: 256 });
        // A wait that ends with its task leaves nothing either.
        yield* commit(cache, [{ type: "task", value: waiting(waitState) }]);
        assert.deepInclude(cache.sizes(), { awaited: 2, waits: 1 });
        yield* commit(cache, [{ type: "task", value: ended(2) }]);
        assert.deepInclude(cache.sizes(), { awaited: 0, waits: 0 });
      }),
    ),
  );

  it.effect("keeps the written record itself, frozen: no copy", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const backend = yield* countingStorage;
        const cache = RecordCache.make(backend.storage);
        const value = { id: ConversationId.make(1) };
        const record = task(2);
        cache.published([
          { type: "conversation", value },
          { type: "task", value: record },
        ]);
        assert.strictEqual(
          Option.getOrUndefined(yield* cache.storage.task(TaskId.make(2))),
          record,
        );
        assert.strictEqual(
          Option.getOrUndefined(yield* cache.storage.conversation(ConversationId.make(1))),
          value,
        );
        assert.isTrue(Object.isFrozen(record.state));
      }),
    ),
  );
});

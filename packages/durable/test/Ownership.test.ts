import { assert, describe, it } from "@effect/vitest";
import { ConversationId, TaskId } from "../src/Ids.ts";
import type { TaskRecord, TaskState } from "../src/Records.ts";
import * as Ownership from "../src/internal/ownership.ts";

// Tests of the scheduler's ownership index: the index is synchronous, so these drive it directly, without a
// Harness or fibers. The Harness suites cover it through the scheduler.

const conversation = ConversationId.make(1);
const pending: TaskState = { status: "pending", checkpoint: null };
const ended: TaskState = { status: "terminal", outcome: { status: "completed", result: null } };

interface TaskOptions {
  readonly owner?: number | undefined;
  readonly background?: boolean | undefined;
  readonly abortRequested?: boolean | undefined;
  readonly state?: TaskState | undefined;
  readonly conversationId?: ConversationId | undefined;
}

const task = (id: number, options: TaskOptions = {}): TaskRecord => {
  const base = {
    id: TaskId.make(id),
    conversationId: options.conversationId ?? conversation,
    kind: "test",
    version: 1,
    input: null,
    background: options.background ?? false,
    abortRequested: options.abortRequested ?? false,
  };
  const owned = options.owner === undefined ? base : { ...base, owner: TaskId.make(options.owner) };
  // SAFETY: a record with one of the task states; the index reads its fields, not its schema.
  return { ...owned, state: options.state ?? pending } as TaskRecord;
};

// SAFETY: the record ended; a settled record has no memos, and these test records carry none.
const terminal = (record: TaskRecord): TaskRecord => ({ ...record, state: ended }) as TaskRecord;

const setup = () => {
  let drops = 0;
  const index = Ownership.make({
    onDropQueued: () => {
      drops++;
    },
  });
  index.setEdge(conversation, null);
  return { index, drops: () => drops };
};

describe("ownership index", () => {
  it("lists live work under its conversation, which is busy until the work ends", () => {
    const { index } = setup();
    const root = task(1);
    index.track(undefined, root);
    assert.isTrue(index.live.has(root.id));
    assert.isTrue(index.runnable.has(root.id));
    assert.isFalse(index.idle(conversation));
    assert.isFalse(index.idle(undefined));
    index.track(root, terminal(root));
    assert.isFalse(index.live.has(root.id));
    assert.isTrue(index.idle(conversation));
    assert.isTrue(index.idle(undefined));
  });

  it("finds ordinary owned work without entering background work", () => {
    const { index } = setup();
    index.track(undefined, task(1));
    index.track(undefined, task(3, { owner: 1 }));
    index.track(undefined, task(2, { owner: 1, background: true }));
    index.track(undefined, task(4, { owner: 2 }));
    assert.deepStrictEqual(index.ownedLive(TaskId.make(1)), [TaskId.make(3)]);
    assert.isTrue(index.hasOwnedLive(TaskId.make(1)));
    assert.deepStrictEqual(
      index.liveOwnedBy(TaskId.make(1)).map((record) => record.id),
      [TaskId.make(2), TaskId.make(3)],
    );
  });

  it("sees a commit's staged child through an overlay", () => {
    const { index } = setup();
    index.track(undefined, task(1));
    const child = task(2, { owner: 1 });
    assert.isFalse(index.hasOwnedLive(TaskId.make(1)));
    const overlay: Ownership.Overlay = { tasks: new Map([[child.id, child]]), edges: new Map() };
    assert.isTrue(index.hasOwnedLive(TaskId.make(1), overlay));
  });

  it("makes the owner and the waiters of ended work candidates again", () => {
    const { index } = setup();
    const owner = task(1, { abortRequested: true });
    const child = task(2, { owner: 1 });
    const waiter = task(3, {
      state: { status: "waiting", checkpoint: null, on: [TaskId.make(2)], policy: "allSettled" },
    });
    for (const record of [owner, child, waiter]) index.track(undefined, record);
    // The scheduler drops a candidate that cannot run yet.
    index.runnable.delete(owner.id);
    index.runnable.delete(waiter.id);
    index.track(child, terminal(child));
    assert.isTrue(index.runnable.has(owner.id));
    assert.isTrue(index.runnable.has(waiter.id));
  });

  it("holds a completing owner for finalization once its owned work ends", () => {
    const { index } = setup();
    const owner = task(1, {
      state: { status: "completing", outcome: { status: "completed", result: null } },
    });
    const child = task(2, { owner: 1 });
    index.track(undefined, owner);
    index.track(undefined, child);
    assert.isFalse(index.runnable.has(owner.id));
    index.finalizeChecks.clear();
    index.track(child, terminal(child));
    assert.isTrue(index.finalizeChecks.has(owner.id));
  });

  it("cascades cancellation intent down to a background boundary", () => {
    const { index } = setup();
    index.track(undefined, task(1, { abortRequested: true }));
    index.track(undefined, task(2, { owner: 1 }));
    index.track(undefined, task(3, { owner: 1, background: true }));
    index.track(undefined, task(4, { owner: 3 }));
    assert.isTrue(index.intent.has(TaskId.make(1)));
    assert.isTrue(index.belowCancelled({ task: TaskId.make(1) }));
    assert.isTrue(index.belowCancelled({ task: TaskId.make(2) }));
    assert.isFalse(index.belowCancelled({ task: TaskId.make(3) }));
  });

  it("treats work whose chain is not loaded as unknown until it is", () => {
    const index = Ownership.make({ onDropQueued: () => {} });
    const other = ConversationId.make(2);
    const record = task(1, { conversationId: other });
    index.track(undefined, record);
    assert.isTrue(index.unloaded.has(record.id));
    assert.isUndefined(index.inScope({ conversation: other }, { roots: true }));
    assert.isFalse(index.idle(undefined));
    index.setEdge(other, null);
    index.markLoaded([record.id]);
    assert.isTrue(index.known(record.id));
    assert.isTrue(index.inScope({ conversation: other }, { roots: true }));
  });

  it("forgets every node once the work below it has ended and the sweep ran", () => {
    const { index, drops } = setup();
    const records = [task(1), task(2, { owner: 1 }), task(3, { owner: 2 })];
    for (const record of records) index.track(undefined, record);
    for (const record of records.toReversed()) index.track(record, terminal(record));
    assert.isAbove(drops(), 0);
    index.sweep();
    assert.deepStrictEqual(index.sizes(), {
      live: 0,
      settled: 0,
      edges: 0,
      below: 0,
      roots: 0,
      unloaded: 0,
      dropQueue: 0,
      pins: 0,
    });
  });

  it("walks an ownership chain deeper than the call stack", () => {
    const { index } = setup();
    const depth = 20_000;
    const records: Array<TaskRecord> = [];
    for (let id = 1; id <= depth; id++) {
      const record = task(id, id === 1 ? {} : { owner: id - 1 });
      records.push(record);
      index.track(undefined, record);
    }
    assert.strictEqual(index.ownedLive(TaskId.make(1)).length, depth - 1);
    assert.isFalse(index.idle(conversation));
    for (const record of records.toReversed()) index.track(record, terminal(record));
    index.sweep();
    assert.strictEqual(index.sizes().below, 0);
    assert.isTrue(index.idle(conversation));
  });
});

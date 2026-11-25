import { assert, describe, it } from "@effect/vitest";
import { EntryId, SubmissionId } from "../src/Ids.ts";
import type { EntryDraft } from "../src/Entry.ts";
import type { InboxItem } from "../src/Inbox.ts";
import { isStale, planBoundary, type QueueModes } from "../src/internal/inbox.ts";

// Tests of the boundary's decision: `planBoundary` is a pure function of the queued items, the queue modes and
// the boundary's kind; `isStale` decides each write as it is placed. The `HarnessInbox` suite covers the same
// rules through the Harness.

const one: QueueModes = { steeringMode: "one-at-a-time", followUpMode: "one-at-a-time" };
const all: QueueModes = { steeringMode: "all", followUpMode: "all" };

const steer = (id: number): InboxItem => ({
  id: SubmissionId.make(id),
  mode: "steer",
  content: "s",
});
const followUp = (id: number): InboxItem => ({
  id: SubmissionId.make(id),
  mode: "followUp",
  content: "f",
});
const write = (id: number, head?: number | "self"): InboxItem => ({
  id: SubmissionId.make(id),
  mode: "write",
  entry: head === undefined ? { kind: "note" } : { kind: "note", head },
});

/** A write's head, as `isStale` reads it. */
const head = (target?: number | "self"): Pick<EntryDraft, "head"> =>
  target === undefined ? {} : { head: target === "self" ? "self" : EntryId.make(target) };

describe("planBoundary", () => {
  describe("selection by mode", () => {
    const items = [steer(1), followUp(2), steer(3), followUp(4)];

    it("selects the first steer and no follow-up at postTools", () => {
      const plan = planBoundary(items, one, "postTools");
      assert.deepStrictEqual(plan.users, [0]);
      assert.isFalse(plan.reset);
    });

    it("selects every steer and no follow-up at postTools with all", () => {
      assert.deepStrictEqual(planBoundary(items, all, "postTools").users, [0, 2]);
    });

    it("selects the first steer and the first follow-up at final", () => {
      assert.deepStrictEqual(planBoundary(items, one, "final").users, [0, 1]);
    });

    it("selects every user item at final with all", () => {
      assert.deepStrictEqual(planBoundary(items, all, "final").users, [0, 1, 2, 3]);
    });

    it("reads each mode on its own", () => {
      const modes: QueueModes = { steeringMode: "one-at-a-time", followUpMode: "all" };
      assert.deepStrictEqual(planBoundary(items, modes, "final").users, [0, 1, 3]);
    });

    it("selects every write at both kinds", () => {
      const writes = [write(1), steer(2), write(3)];
      for (const at of ["postTools", "final"] as const) {
        assert.deepStrictEqual(planBoundary(writes, one, at).writes, [0, 2], at);
      }
    });
  });

  describe("a reset at postTools", () => {
    it("turns postTools into final: follow-ups are selected too", () => {
      const plan = planBoundary([followUp(1), write(2, "self"), steer(3)], one, "postTools");
      assert.isTrue(plan.reset);
      assert.deepStrictEqual(plan.users, [0, 2]);
    });

    it("is no reset without a head: self write", () => {
      const plan = planBoundary([followUp(1), write(2, 1)], one, "postTools");
      assert.isFalse(plan.reset);
      assert.deepStrictEqual(plan.users, []);
    });
  });

  describe("writes before users", () => {
    it("plans writes and users each in ID order, in two lists the apply runs writes first", () => {
      const plan = planBoundary([followUp(1), write(2), steer(3), write(4)], all, "final");
      assert.deepStrictEqual(plan.writes, [1, 3]);
      assert.deepStrictEqual(plan.users, [0, 2]);
    });
  });

  describe("stale heads", () => {
    it("finds a head before the active range stale and one at or after it not", () => {
      assert.deepStrictEqual(
        [4, 5, 6].map((target) => isStale(EntryId.make(5), head(target))),
        [true, false, false],
      );
    });

    it("never finds a head: self write or a write without a head stale", () => {
      assert.isFalse(isStale(EntryId.make(5), head("self")));
      assert.isFalse(isStale(EntryId.make(5), head()));
    });

    it("finds nothing stale before the first head marker", () => {
      assert.isFalse(isStale(undefined, head(1)));
    });

    it("never makes a user item stale", () => {
      const plan = planBoundary([write(1, "self"), followUp(2)], one, "final");
      assert.deepStrictEqual(plan.users, [1]);
    });
  });

  describe("positional removal", () => {
    it("removes the selected items, writes included, highest position first, and keeps the rest", () => {
      const plan = planBoundary(
        [followUp(1), write(2, 1), steer(3), followUp(4), steer(5)],
        one,
        "postTools",
      );
      // Every write, stale or placed, and the first steer go; the follow-ups and the second steer stay.
      assert.deepStrictEqual(plan.removed, [2, 1]);
    });

    it("removes nothing from an empty inbox", () => {
      assert.deepStrictEqual(planBoundary([], all, "final"), {
        writes: [],
        users: [],
        reset: false,
        removed: [],
      });
    });
  });
});

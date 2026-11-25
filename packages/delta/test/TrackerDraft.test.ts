import { describe, it } from "@effect/vitest";
import { assertTrue, deepStrictEqual, strictEqual } from "@effect/vitest/utils";
import * as Delta from "@effect-durable-harness/delta/Delta";
import * as Tracker from "@effect-durable-harness/delta/Tracker";
import type * as Schema from "effect/Schema";
import { throwsTypeError } from "./utils.ts";

describe("transactional overlay drafts", () => {
  it("copies only changed branches", () => {
    const tracker = Tracker.make({
      changed: { count: 1, sibling: { value: "kept" } },
      untouched: { value: 2 },
    });
    const base = tracker.value;
    const change = Tracker.beginChange(tracker);
    const first = change.state.changed;
    strictEqual(change.state.changed, first);
    change.state.changed.count = 3;
    const prepared = Tracker.prepare(change);
    assertTrue(prepared.value !== base);
    assertTrue(prepared.value.changed !== base.changed);
    strictEqual(prepared.value.changed.sibling, base.changed.sibling);
    strictEqual(prepared.value.untouched, base.untouched);
  });

  it("supports frozen bases, deletion, and native array mutators", () => {
    // An optional key, rather than `{ optional: string | undefined }`, keeps `delete` legal under
    // `strictNullChecks` + `exactOptionalPropertyTypes` without changing the data.
    const tracker = Tracker.make<{ optional?: string; values: Array<number> }>({
      optional: "remove",
      values: [3, 1, 2],
    });
    const change = Tracker.beginChange(tracker);
    delete change.state.optional;
    change.state.values.push(4);
    strictEqual(change.state.values.pop(), 4);
    change.state.values.unshift(0);
    strictEqual(change.state.values.shift(), 0);
    deepStrictEqual(change.state.values.splice(1, 1, 5, 4), [1]);
    change.state.values.sort((left, right) => left - right);
    change.state.values.reverse();
    change.state.values.fill(9, 1, 3);
    change.state.values.copyWithin(1, 0, 2);
    const prepared = Tracker.prepare(change);
    deepStrictEqual(prepared.value, { values: [5, 5, 9, 2] });
  });

  it("copies inserted draft values without aliasing their original reference", () => {
    const tracker = Tracker.make({ values: [{ value: 1 }, { value: 2 }] });
    const change = Tracker.beginChange(tracker);
    const held = change.state.values[0]!;
    change.state.values.unshift(held);
    held.value = 9;
    const prepared = Tracker.prepare(change);
    deepStrictEqual(prepared.value.values, [{ value: 1 }, { value: 9 }, { value: 2 }]);
    assertTrue(prepared.value.values[0] !== prepared.value.values[1]);
  });

  it("keeps comparator edits when sorting object drafts", () => {
    const tracker = Tracker.make({
      rows: [
        { rank: 2, comparisons: 0 },
        { rank: 1, comparisons: 0 },
      ],
    });
    const change = Tracker.beginChange(tracker);
    change.state.rows.sort((left, right) => {
      left.comparisons++;
      right.comparisons++;
      return left.rank - right.rank;
    });
    const prepared = Tracker.prepare(change);
    deepStrictEqual(
      prepared.value.rows.map((row) => row.rank),
      [1, 2],
    );
    deepStrictEqual(
      prepared.value.rows.map((row) => row.comparisons),
      [1, 1],
    );
  });

  it("drops writes through detached child references", () => {
    const tracker = Tracker.make({
      child: { value: "removed" },
      items: [{ value: "removed" }, { value: "kept" }],
    });
    const change = Tracker.beginChange(tracker);
    const child = change.state.child;
    const shifted = change.state.items.shift();
    delete (
      change.state as {
        child?: Tracker.Draft<{ value: string }>;
        items: Tracker.Draft<Array<{ value: string }>>;
      }
    ).child;
    child.value = "detached child";
    if (shifted !== undefined) shifted.value = "detached item";
    // The expected value omits the deleted `child` key, which the root type requires.
    deepStrictEqual<unknown>(Tracker.prepare(change).value, { items: [{ value: "kept" }] });
  });

  it.each(["unshift", "splice"] as const)(
    "inserts 100,000 items with %s without argument overflow",
    (method) => {
      const tracker = Tracker.make({ values: [-1] });
      const change = Tracker.beginChange(tracker);
      const items = Array.from({ length: 100_000 }, (_, value) => value);
      if (method === "unshift")
        Reflect.apply(change.state.values.unshift, change.state.values, items);
      else Reflect.apply(change.state.values.splice, change.state.values, [1, 0, ...items]);
      const prepared = Tracker.prepare(change);
      strictEqual(prepared.value.values.length, 100_001);
      strictEqual(prepared.value.values[method === "unshift" ? 0 : 1], 0);
      strictEqual(prepared.value.values[method === "unshift" ? 99_999 : 100_000], 99_999);
      deepStrictEqual(Delta.applyImmutableUnsafe(prepared.base, prepared.ops), prepared.value);
    },
  );

  it("rejects non-strict JSON placements without mutating the draft or base", () => {
    const tracker = Tracker.make({
      number: 0,
      payload: null as Schema.Json,
      values: [1, 2] as Array<Schema.Json>,
    });
    const change = Tracker.beginChange(tracker);
    throwsTypeError(() => {
      change.state.values[0] = undefined as unknown as Schema.Json;
    }, /strict JSON/);
    throwsTypeError(
      () => change.state.values.push({ nested: undefined } as unknown as Schema.Json),
      /strict JSON/,
    );
    throwsTypeError(() => {
      change.state.number = Number.NaN;
    }, /strict JSON/);
    throwsTypeError(() => {
      // @effect-diagnostics-next-line globalDate:off
      change.state.payload = new Date() as unknown as Schema.Json;
    }, /plain objects/);
    deepStrictEqual(change.state, tracker.value);
    Tracker.abort(change);
    deepStrictEqual(tracker.value, { number: 0, payload: null, values: [1, 2] });
  });

  it("rejects array holes without mutating the committed base", () => {
    const tracker = Tracker.make({ values: [1, 2] });
    const change = Tracker.beginChange(tracker);
    throwsTypeError(() => delete change.state.values[0], /dense|holes/);
    Tracker.abort(change);
    deepStrictEqual(tracker.value.values, [1, 2]);
  });
});

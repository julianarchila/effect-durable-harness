import { describe, it } from "@effect/vitest";
import { assertFalse, assertTrue, deepStrictEqual, strictEqual } from "@effect/vitest/utils";
import * as Delta from "@effect-durable-harness/delta/Delta";
import * as Tracker from "@effect-durable-harness/delta/Tracker";

describe("tracker ownership", () => {
  it("takes immutable ownership of the imported revision in O(1)", () => {
    const input = {
      point: { x: 3, y: 7, pressure: 0.1 },
      rows: [{ values: [0, false, null, "text", { n: 1 }] }],
    };
    const tracker = Tracker.make(input);
    strictEqual(tracker.value, input);
    strictEqual(tracker.value.point, input.point);
    strictEqual(tracker.value.rows[0]!.values[4], input.rows[0]!.values[4]);
    assertFalse(Object.isFrozen(tracker.value.rows[0]!.values));
  });

  it("preserves null prototypes in an alias-free owned root", () => {
    type Dictionary = { enabled: boolean; child: { n: number } };
    const dictionary = Object.assign(Object.create(null) as Dictionary, {
      enabled: true,
      child: { n: 1 },
    });
    const tracker = Tracker.make({
      dictionary,
      left: { nested: [{ n: 1 }] },
      right: { nested: [{ n: 1 }] },
    });
    strictEqual(Object.getPrototypeOf(tracker.value.dictionary), null);
    strictEqual(tracker.value.dictionary, dictionary);
    assertTrue(tracker.value.left !== tracker.value.right);
    assertTrue(tracker.value.left.nested[0] !== tracker.value.right.nested[0]);
  });

  it("does not traverse a trusted root while taking ownership", () => {
    let reads = 0;
    const input = Object.defineProperty({}, "untrustedAccessor", {
      enumerable: true,
      get() {
        reads += 1;
        throw new Error("root was traversed");
      },
    });
    const tracker = Tracker.make(input);
    strictEqual(tracker.value, input);
    strictEqual(reads, 0);
  });

  it("copies assigned and inserted values immediately", () => {
    const tracker = Tracker.make({ rows: [] as Array<{ nested: { value: number } }> });
    const assigned = { nested: { value: 1 } };
    const change = Tracker.beginChange(tracker);
    change.state.rows.push(assigned);
    assigned.nested.value = 9;
    strictEqual(change.state.rows[0]!.nested.value, 1);
    const prepared = Tracker.prepare(change);
    strictEqual(prepared.value.rows[0]!.nested.value, 1);
    const replica = Delta.applyUnsafe(structuredClone(prepared.base), prepared.ops);
    deepStrictEqual(replica, prepared.value);
  });
});

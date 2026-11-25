import { describe, it } from "@effect/vitest";
import { assertFalse, assertTrue, deepStrictEqual, strictEqual } from "@effect/vitest/utils";
import * as Tracker from "@effect-durable-harness/delta/Tracker";

describe("immutable revision ownership", () => {
  it("takes ownership of an alias-free mutable JSON root without freezing", () => {
    const input = { left: { value: 1 }, right: { value: 1 } };
    const tracker = Tracker.make(input);
    strictEqual(tracker.value, input);
    assertTrue(tracker.value.left !== tracker.value.right);
    assertFalse(Object.isFrozen(tracker.value));
    assertFalse(Object.isFrozen(tracker.value.left));
  });

  it("commits transaction copies in place while sharing unchanged branches", () => {
    const tracker = Tracker.make({ changed: { value: 1 }, retained: { value: 2 } });
    const base = tracker.value;
    const baseSnapshot = structuredClone(base);
    const change = Tracker.beginChange(tracker);
    change.state.changed.value = 3;
    const prepared = Tracker.prepare(change);
    assertTrue(prepared.value.changed !== base.changed);
    strictEqual(prepared.value.retained, base.retained);
    assertFalse(Object.isFrozen(prepared.value.changed));
    Tracker.adopt(tracker, prepared);
    deepStrictEqual(base, baseSnapshot);
    strictEqual(tracker.value, prepared.value);
  });

  it("makes repeated placements independent", () => {
    const tracker = Tracker.make({ values: [] as Array<{ value: number }> });
    const change = Tracker.beginChange(tracker);
    const shared = { value: 1 };
    change.state.values.push(shared, shared);
    const prepared = Tracker.prepare(change);
    assertTrue(prepared.value.values[0] !== prepared.value.values[1]);
  });
});

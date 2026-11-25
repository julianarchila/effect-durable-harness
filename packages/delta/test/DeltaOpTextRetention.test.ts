import { describe, it } from "@effect/vitest";
import { assertTrue, deepStrictEqual } from "@effect/vitest/utils";
import * as Delta from "@effect-durable-harness/delta/Delta";
import * as Tracker from "@effect-durable-harness/delta/Tracker";
import { getHeapStatistics, setFlagsFromString } from "node:v8";
import { runInNewContext } from "node:vm";

// #9265: an append op's text was a V8 sliced string of the whole new string, so every op kept a full copy of the
// string it was cut from alive: a delta chain or a replica built from N appends to a growing text held N copies.

const gc = (() => {
  setFlagsFromString("--expose-gc");
  return runInNewContext("gc") as () => void;
})();

/** Heap bytes `build` leaves reachable through its result, and the result. */
function retained<T>(build: () => T): { bytes: number; kept: T } {
  gc();
  const before = getHeapStatistics().used_heap_size;
  const kept = build();
  gc();
  return { bytes: getHeapStatistics().used_heap_size - before, kept };
}

const BASE = "x".repeat(100_000);
const COMMITS = 300;
const piece = (index: number) => `line ${index} of a long streamed text\n`;

describe("append op text", () => {
  it("tracker append ops do not retain the string they were cut from", () => {
    const { bytes } = retained(() => {
      const tracker = Tracker.make({ text: BASE });
      const ops: Array<Delta.Operation> = [];
      for (let index = 0; index < COMMITS; index++) {
        const change = Tracker.beginChange(tracker);
        // A rope, as a streaming writer builds it.
        change.state.text = tracker.value.text + piece(index);
        const prepared = Tracker.prepare(change);
        ops.push(...prepared.ops);
        Tracker.adopt(tracker, prepared);
      }
      assertTrue(ops.every((op) => op[0] === "a"));
      return { ops, value: tracker.value };
    });
    // One copy of the text plus the op texts: well under a megabyte. Sliced op texts held one copy per op (30 MB).
    assertTrue(bytes < 2_000_000, `retained ${bytes} bytes`);
  });

  it("diffRevisions append ops do not retain the string they were cut from", () => {
    const { bytes } = retained(() => {
      const ops: Array<Delta.Operation> = [];
      let text = BASE;
      for (let index = 0; index < COMMITS; index++) {
        const next = text + piece(index);
        ops.push(...Delta.diff({ text }, { text: next }));
        text = next;
      }
      assertTrue(ops.every((op) => op[0] === "a"));
      return { ops, text };
    });
    assertTrue(bytes < 2_000_000, `retained ${bytes} bytes`);
  });

  it("a replica that applies a long stream of appends retains the text once, not once per commit", () => {
    const { bytes, kept } = retained(() => {
      const tracker = Tracker.make({ text: BASE });
      // Like a watch, a remote replica or Durable's conversation view: a value kept current by applying ops.
      let replica: { text: string } = { text: BASE };
      for (let index = 0; index < COMMITS; index++) {
        const change = Tracker.beginChange(tracker);
        change.state.text = tracker.value.text + piece(index);
        const prepared = Tracker.prepare(change);
        Tracker.adopt(tracker, prepared);
        replica = Delta.applyImmutableUnsafe(replica, prepared.ops);
      }
      // Comparing here would flatten the replica's text and drop what it retains; compare after measuring.
      return { replica, tracker };
    });
    assertTrue(bytes < 2_000_000, `retained ${bytes} bytes`);
    deepStrictEqual(kept.replica, kept.tracker.value);
  });
});

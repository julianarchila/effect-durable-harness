import { describe, it } from "@effect/vitest";
import { assertTrue, deepStrictEqual, strictEqual } from "@effect/vitest/utils";
import * as Delta from "@effect-durable-harness/delta/Delta";
import * as Tracker from "@effect-durable-harness/delta/Tracker";
import { GCProfiler, getHeapStatistics, setFlagsFromString } from "node:v8";
import { runInNewContext } from "node:vm";
import { throwsMessage, throwsTypeError } from "./utils.ts";

// #9265: streaming writers appended to growing strings by assignment, and the tracker's prefix check copied the whole
// string on every change.

type Block = { type: "text"; text: string; signature?: string; tags: Array<string> };
type Doc = { title: string; content: Array<Block>; meta: { note: string } };

function doc(): Doc {
  return {
    title: "t",
    content: [{ type: "text", text: "hello", tags: ["a"] }],
    meta: { note: "n" },
  };
}

/** Run `mutate` in one change, adopt it, and return its ops; checks that replaying the ops gives the new value. */
function commit<T extends object>(
  tracker: Tracker.Tracker<T>,
  mutate: (draft: Tracker.Draft<T>) => void,
): Array<Delta.Operation> {
  const base = tracker.value;
  const change = Tracker.beginChange(tracker);
  mutate(change.state);
  const prepared = Tracker.prepare(change);
  deepStrictEqual(Delta.applyImmutableUnsafe(base, prepared.ops), prepared.value);
  Tracker.adopt(tracker, prepared);
  return [...prepared.ops];
}

describe("appendText", () => {
  it("records an append op and the appended value", () => {
    const tracker = Tracker.make(doc());
    const before = tracker.value;
    deepStrictEqual(
      commit(tracker, (draft) => Tracker.appendText(draft.content[0]!, "text", " world")),
      [["a", ["content", 0, "text"], " world"]],
    );
    strictEqual(tracker.value.content[0]!.text, "hello world");
    strictEqual(before.content[0]!.text, "hello");
    deepStrictEqual(
      commit(tracker, (draft) => Tracker.appendText(draft, "title", "!")),
      [["a", ["title"], "!"]],
    );
    deepStrictEqual(
      commit(tracker, (draft) => Tracker.appendText(draft.meta, "note", "ote")),
      [["a", ["meta", "note"], "ote"]],
    );
    deepStrictEqual(tracker.value, {
      title: "t!",
      content: [{ type: "text", text: "hello world", tags: ["a"] }],
      meta: { note: "note" },
    });
  });

  it("joins several appends to one key in one change into one op, next to other changes", () => {
    const tracker = Tracker.make(doc());
    const ops = commit(tracker, (draft) => {
      Tracker.appendText(draft.content[0]!, "text", " a");
      draft.title = "new";
      Tracker.appendText(draft.content[0]!, "text", " b");
      Tracker.appendText(draft.content[0]!, "text", "");
      Tracker.appendText(draft.content[0]!, "text", " c");
    });
    // Each op is checked for presence and the batch for length: the order of the two is not contractual.
    strictEqual(ops.length, 2);
    assertTrue(ops.some((op) => JSON.stringify(op) === '["a",["content",0,"text"]," a b c"]'));
    assertTrue(ops.some((op) => JSON.stringify(op) === '["s",["title"],"new"]'));
    strictEqual(tracker.value.content[0]!.text, "hello a b c");
  });

  it("falls back to the string comparison when the same key is also assigned or deleted", () => {
    const tracker = Tracker.make(doc());
    deepStrictEqual(
      commit(tracker, (draft) => {
        Tracker.appendText(draft.content[0]!, "text", " x");
        draft.content[0]!.text += " y";
      }),
      [["a", ["content", 0, "text"], " x y"]],
    );
    deepStrictEqual(
      commit(tracker, (draft) => {
        Tracker.appendText(draft.content[0]!, "text", " z");
        draft.content[0]!.text = "replaced";
      }),
      [["s", ["content", 0, "text"], "replaced"]],
    );
    deepStrictEqual(
      commit(tracker, (draft) => {
        draft.title = "assigned";
        Tracker.appendText(draft, "title", " then appended");
      }),
      [["s", ["title"], "assigned then appended"]],
    );
    deepStrictEqual(
      commit(tracker, (draft) => {
        Tracker.appendText(draft.meta, "note", "x");
        delete (draft.meta as { note?: string }).note;
      }),
      [["d", ["meta", "note"]]],
    );
  });

  it("appends inside a block the same change inserted or replaced", () => {
    const tracker = Tracker.make(doc());
    commit(tracker, (draft) => {
      draft.content.push({ type: "text", text: "new", tags: [] });
      Tracker.appendText(draft.content[1]!, "text", " block");
      draft.content[0] = { type: "text", text: "other", tags: [] };
      Tracker.appendText(draft.content[0]!, "text", " one");
    });
    deepStrictEqual(
      tracker.value.content.map((block) => block.text),
      ["other one", "new block"],
    );
  });

  it("rejects what a JavaScript caller can get wrong", () => {
    const tracker = Tracker.make(doc());
    // SAFETY: the test calls appendText as an untyped JavaScript caller would.
    const append = Tracker.appendText as unknown as (
      draft: unknown,
      key: string | number,
      text: string,
    ) => void;
    throwsTypeError(
      () => append({ text: "plain object" }, "text", "x"),
      /appendText\(\) needs a draft/,
    );
    const change = Tracker.beginChange(tracker);
    throwsTypeError(
      () => append(change.state, "content", "x"),
      /appendText\(\) needs a string at "content"/,
    );
    throwsTypeError(
      () => append(change.state, "missing", "x"),
      /appendText\(\) needs a string at "missing"/,
    );
    throwsTypeError(
      () => append(change.state.content, "0", "x"),
      /appendText\(\) needs a string at "0"/,
    );
    const block = change.state.content[0]!;
    Tracker.prepare(change);
    throwsMessage(() => Tracker.appendText(block, "text", "x"), /Cannot use a settled overlay/);
    const aborted = Tracker.beginChange(tracker);
    const state = aborted.state;
    Tracker.abort(aborted);
    throwsMessage(() => Tracker.appendText(state, "title", "x"), /Cannot use a settled overlay/);
  });

  it("accepts only keys whose type is exactly string", () => {
    type Keys = Tracker.StringKeys<Block>;
    const keys: Array<Keys> = ["text"];
    deepStrictEqual(keys, ["text"]);
    const tracker = Tracker.make(doc());
    const change = Tracker.beginChange(tracker);
    const block = change.state.content[0]!;
    // @ts-expect-error a string literal type
    Tracker.appendText(block, "type", "x");
    // @ts-expect-error an optional string
    throwsTypeError(() => Tracker.appendText(block, "signature", "x"));
    // @ts-expect-error an array
    throwsTypeError(() => Tracker.appendText(block, "tags", "x"));
    // @ts-expect-error array keys
    throwsTypeError(() => Tracker.appendText(change.state.content, 0, "x"));
    Tracker.abort(change);
  });

  it("keeps an op text that does not hold a larger string it was cut from alive", () => {
    setFlagsFromString("--expose-gc");
    const gc = runInNewContext("gc") as () => void;
    gc();
    const before = getHeapStatistics().used_heap_size;
    const tracker = Tracker.make(doc());
    const ops: Array<Delta.Operation> = [];
    for (let index = 0; index < 50; index++) {
      const source = `${index}`.repeat(100_000);
      ops.push(
        ...commit(tracker, (draft) =>
          Tracker.appendText(draft.content[0]!, "text", source.slice(0, 40)),
        ),
      );
    }
    gc();
    assertTrue(getHeapStatistics().used_heap_size - before < 1_000_000);
    strictEqual(ops.length, 50);
  });
});

describe("appendText cost", () => {
  /** Bytes `run` allocates, counting what collections during it freed. */
  function allocatedBytes(run: () => void): number {
    const profiler = new GCProfiler();
    profiler.start();
    const before = getHeapStatistics().used_heap_size;
    run();
    const after = getHeapStatistics().used_heap_size;
    let freed = 0;
    for (const collection of profiler.stop()?.statistics ?? []) {
      freed +=
        collection.beforeGC.heapStatistics.usedHeapSize -
        collection.afterGC.heapStatistics.usedHeapSize;
    }
    return after - before + freed;
  }

  /** One change per 40-character chunk, appended to one streamed text until it has `length` characters. */
  function stream(length: number): number {
    const tracker = Tracker.make(doc());
    const chunk = "0123456789abcdefghij0123456789abcdefghij";
    return allocatedBytes(() => {
      for (let streamed = 0; streamed < length; streamed += chunk.length) {
        const change = Tracker.beginChange(tracker);
        Tracker.appendText(change.state.content[0]!, "text", chunk);
        Tracker.adopt(tracker, Tracker.prepare(change));
      }
    });
  }

  it("costs O(chunk) per change: a 500 KB stream allocates linearly", () => {
    const small = stream(50_000);
    const large = stream(500_000);
    // Ten times the text: linear growth is about 10x. Measured about 10 KB per change, mostly the change itself.
    assertTrue(large / small < 20);
    assertTrue(large / 12_500 < 40_000);
    // The same stream through `draft.text += chunk` makes prepare() compare the whole string on every change: on
    // Node 24, 6 GB allocated and about 2 s for 500 KB, against 120 MB and under 0.1 s here. Not asserted.
  });
});

// Tests for Delta's pending queue: folding is invisible to a replica, the cap, ownership of received ops, cost, GC.
import { afterEach, describe, it, vi } from "@effect/vitest";
import { assertTrue, deepStrictEqual, strictEqual } from "@effect/vitest/utils";
import * as Delta from "@effect-durable-harness/delta/Delta";
import * as PendingOps from "@effect-durable-harness/delta/PendingOps";
import * as Tracker from "@effect-durable-harness/delta/Tracker";
import * as Wire from "@effect-durable-harness/delta/Wire";
import { setImmediate } from "node:timers/promises";
import { setFlagsFromString } from "node:v8";
import { runInNewContext } from "node:vm";

// `Schema.Json` is readonly, and the generators below build values in place.
type JsonValue = null | boolean | number | string | Array<JsonValue> | { [key: string]: JsonValue };
type Op = Delta.Operation;
type PendingTake = PendingOps.PendingTake;
const MAX_PENDING_OPS = PendingOps.maxPendingOps;

type JsonObject = { [key: string]: JsonValue };
type Container = JsonValue[] | JsonObject;

/** A seeded generator, so a failing sequence reproduces from its seed. */
function random(seed: number) {
  let state = seed >>> 0;
  const next = (): number => {
    state = (Math.imul(state, 1664525) + 1013904223) >>> 0;
    return state / 4294967296;
  };
  const int = (max: number): number => Math.floor(next() * max);
  const pick = <T>(items: readonly T[]): T => items[int(items.length)]!;
  // Surrogate halves and separators: an append may split a code point, and paths hold any string.
  const texts = [
    "a",
    "bc",
    " tok",
    "é",
    "😀",
    "\u2028",
    "\ud83d",
    "\ude00",
    "日本語",
    '"q"',
    "\\",
    "",
    "x".repeat(30),
  ];
  const keys = ["a", "b", "0", "1", "01", "a.b", "x y", "text", "list"];
  const text = (): string => pick(texts);
  const primitive = (): JsonValue => pick([() => int(100), text, () => next() < 0.5, () => null])();
  const value = (depth = 0): JsonValue => {
    const roll = next();
    if (depth > 2 || roll < 0.4) return primitive();
    if (roll < 0.7) return Array.from({ length: int(4) }, () => value(depth + 1));
    const object: JsonObject = {};
    for (let count = int(4); count >= 0; count--) object[pick(keys)] = value(depth + 1);
    return object;
  };
  const root = (): JsonObject => {
    const object: JsonObject = {};
    for (let count = int(4) + 1; count >= 0; count--) object[pick(keys)] = value();
    return object;
  };
  return { next, int, pick, text, primitive, value, root };
}

type Random = ReturnType<typeof random>;

/** Every (parent, key) slot under `node`, parents first. */
function slots(
  node: JsonValue,
  out: Array<[Container, string | number]> = [],
): Array<[Container, string | number]> {
  if (node === null || typeof node !== "object") return out;
  const keys: Array<string | number> = Array.isArray(node)
    ? node.map((_, index) => index)
    : Object.keys(node);
  for (const key of keys) {
    out.push([node, key]);
    slots((node as JsonObject)[key]!, out);
  }
  return out;
}

const read = (parent: Container, key: string | number): JsonValue => (parent as JsonObject)[key]!;

const write = (parent: Container, key: string | number, value: JsonValue): void => {
  (parent as JsonObject)[key] = value;
};

type Append = (parent: JsonObject, key: string, text: string) => void;

/** Appends through `appendText()`, which a draft records without comparing strings. */
const appendToDraft: Append = (parent, key, text) =>
  Tracker.appendText(parent as never, key as never, text);

const appendToValue: Append = (parent, key, text) => {
  parent[key] = `${parent[key] as string}${text}`;
};

/** One random edit anywhere in `draft`: string appends and trims, array splices and reorders, object edits. */
function mutateAnywhere(draft: JsonObject, rnd: Random, append: Append): void {
  const all = slots(draft);
  if (all.length === 0) {
    draft[rnd.pick(["a", "b"])] = rnd.value();
    return;
  }
  const [parent, key] = rnd.pick(all);
  const current = read(parent, key);
  const roll = rnd.next();
  if (typeof current === "string") {
    if (roll < 0.3 && !Array.isArray(parent)) append(parent, key as string, rnd.text());
    else if (roll < 0.6) write(parent, key, current + rnd.text());
    else if (roll < 0.8) write(parent, key, current.slice(Math.min(current.length, rnd.int(3))));
    else write(parent, key, rnd.primitive());
  } else if (Array.isArray(current)) {
    if (roll < 0.3) current.push(rnd.value());
    else if (roll < 0.5)
      current.splice(
        rnd.int(current.length + 1),
        rnd.int(2),
        ...(rnd.next() < 0.5 ? [rnd.value()] : []),
      );
    else if (roll < 0.65 && current.length > 0) current[rnd.int(current.length)] = rnd.value();
    else if (roll < 0.75 && current.length > 1) current.reverse();
    else if (roll < 0.85 && current.length > 1)
      current.sort((a, b) => JSON.stringify(a).localeCompare(JSON.stringify(b)));
    else if (roll < 0.95) write(parent, key, rnd.value());
    else current.length = 0;
  } else if (current !== null && typeof current === "object") {
    const keys = Object.keys(current);
    if (roll < 0.4) current[rnd.pick(["a", "b", "1", "x y"])] = rnd.value();
    else if (roll < 0.6 && keys.length > 0) delete current[rnd.pick(keys)];
    else write(parent, key, rnd.value());
  } else {
    write(parent, key, rnd.next() < 0.7 ? rnd.primitive() : rnd.value());
  }
}

/** Leaf edits of a list of elements interleaved with splices, pushes and reorders of the list. */
function mutateList(draft: JsonObject, rnd: Random): void {
  const list = draft.list;
  if (!Array.isArray(list)) {
    draft.list = [
      { x: "", n: 0 },
      { x: "", n: 0 },
      { x: "", n: 0 },
    ];
    return;
  }
  const roll = rnd.next();
  if (roll < 0.45 && list.length > 0) {
    const element = list[rnd.int(list.length)];
    if (element !== null && typeof element === "object" && !Array.isArray(element)) {
      if (rnd.next() < 0.3) Tracker.appendText(element as never, "x" as never, rnd.text());
      else if (rnd.next() < 0.5) element.x = `${element.x as string}${rnd.text()}`;
      else element.n = rnd.int(9);
    } else list[rnd.int(list.length)] = { x: "", n: 0 };
  } else if (roll < 0.6)
    list.splice(
      rnd.int(list.length + 1),
      rnd.int(2),
      ...(rnd.next() < 0.6 ? [{ x: rnd.text(), n: 0 }] : []),
    );
  else if (roll < 0.7) list.push({ x: "", n: 0 });
  else if (roll < 0.8 && list.length > 1) list.reverse();
  else if (roll < 0.9 && list.length > 0) list[rnd.int(list.length)] = rnd.primitive();
  else if (list.length > 0) list.splice(rnd.int(list.length), 1);
}

/** Edits of a root array: pushes, splices, reorders, element sets and shifts. */
function mutateRootArray(draft: JsonValue[], rnd: Random): void {
  switch (rnd.int(7)) {
    case 0:
      draft.push(rnd.value());
      break;
    case 1:
      draft.splice(rnd.int(draft.length + 1), rnd.int(4), rnd.value());
      break;
    case 2:
      draft.reverse();
      break;
    case 3:
      draft.sort((a, b) => JSON.stringify(a).localeCompare(JSON.stringify(b)));
      break;
    case 4:
      if (draft.length > 0) draft[rnd.int(draft.length)] = rnd.value();
      break;
    case 5:
      draft.shift();
      break;
    default:
      draft.splice(rnd.int(draft.length + 1), rnd.int(4));
  }
}

/** One sequence of commits from a value: the commits' op batches and the value after each. */
type Sequence = {
  readonly initial: JsonValue;
  readonly commits: Array<{ ops: readonly Op[]; value: JsonValue }>;
};

function trackedSequence<T extends object>(
  initial: T,
  commits: number,
  rnd: Random,
  mutate: (draft: T) => void,
  replace: () => T,
): Sequence {
  const tracker = Tracker.make(structuredClone(initial));
  const out: Sequence["commits"] = [];
  for (let index = 0; index < commits; index++) {
    let prepared: Tracker.Prepared<T>;
    if (rnd.next() < 0.03) prepared = Tracker.prepareReplace(tracker, replace());
    else {
      const change = Tracker.beginChange(tracker);
      for (let edits = 1 + rnd.int(4); edits > 0; edits--) mutate(change.state as T);
      prepared = Tracker.prepare(change);
    }
    Tracker.adopt(tracker, prepared);
    out.push({ ops: prepared.ops, value: tracker.value as JsonValue });
  }
  return { initial: initial as JsonValue, commits: out };
}

/** Diffs between values, each an edit of the previous one or a new value. */
function diffSequence(rnd: Random, commits: number): Sequence {
  const initial = rnd.value();
  const out: Sequence["commits"] = [];
  let value = initial;
  for (let index = 0; index < commits; index++) {
    const edited: JsonObject = { value: structuredClone(value) };
    for (let edits = 1 + rnd.int(3); edits > 0; edits--) mutateAnywhere(edited, rnd, appendToValue);
    const next = rnd.next() < 0.2 ? rnd.value() : edited.value!;
    out.push({ ops: Delta.diff(value, next), value: next });
    value = next;
  }
  return { initial, commits: out };
}

/**
 * Feed the sequence through one queue, taking at random points as a slow consumer would, and replay what was taken
 * on a replica, once as ops and once through Delta's encoder and decoder. Both must reach the final value; the received
 * ops must be untouched.
 */
function checkSequence(sequence: Sequence, rnd: Random): { ops: number; folded: number } {
  for (const commit of sequence.commits) {
    // Frozen: a queue that wrote into a received batch or op tuple throws here.
    for (const op of commit.ops) Object.freeze(op);
    Object.freeze(commit.ops);
  }
  const received = JSON.stringify(sequence.commits.map((commit) => commit.ops));
  const queue = PendingOps.make();
  const taken: Array<{ batch: PendingTake; value: JsonValue }> = [];
  for (const [index, commit] of sequence.commits.entries()) {
    PendingOps.push(queue, commit.ops);
    if (rnd.next() < 0.1 || index === sequence.commits.length - 1)
      taken.push({ batch: PendingOps.take(queue), value: commit.value });
  }
  const final = sequence.commits.length === 0 ? sequence.initial : sequence.commits.at(-1)!.value;
  let replica = structuredClone(sequence.initial);
  const encode = Wire.makeEncoder();
  const decode = Wire.makeDecoder();
  let decoded = Delta.applyImmutableUnsafe<JsonValue>(
    null,
    Wire.decodeUnsafe(decode, JSON.parse(JSON.stringify(Wire.encode(encode, [["r", replica]])))),
  );
  let folded = 0;
  for (const { batch, value } of taken) {
    const ops: readonly Op[] = batch._tag === "Ops" ? batch.ops : [["r", value]];
    folded += ops.length;
    replica = Delta.applyImmutableUnsafe(replica, ops);
    decoded = Delta.applyImmutableUnsafe(
      decoded,
      Wire.decodeUnsafe(decode, JSON.parse(JSON.stringify(Wire.encode(encode, ops)))),
    );
  }
  deepStrictEqual(replica, final);
  deepStrictEqual(decoded, final);
  strictEqual(JSON.stringify(sequence.commits.map((commit) => commit.ops)), received);
  return { ops: sequence.commits.reduce((sum, commit) => sum + commit.ops.length, 0), folded };
}

/** Room for a loaded CI runner: each fuzz test takes about a second locally. */
const FUZZ_TIMEOUT = 30_000;

describe("PendingOps folding", () => {
  it(
    "is invisible to a replica: tracker edits anywhere, appendText, splices and reorders",
    () => {
      const rnd = random(1);
      let ops = 0;
      let folded = 0;
      for (let run = 0; run < 800; run++) {
        const sequence = trackedSequence(
          rnd.root(),
          1 + rnd.int(30),
          rnd,
          (draft) => mutateAnywhere(draft, rnd, appendToDraft),
          rnd.root,
        );
        const result = checkSequence(sequence, rnd);
        ops += result.ops;
        folded += result.folded;
      }
      // The sequences fold: a fuzzer whose ops never merge proves nothing.
      assertTrue(folded < ops * 0.9);
    },
    FUZZ_TIMEOUT,
  );

  it(
    "is invisible to a replica: leaf edits of list elements between splices and reorders of the list",
    () => {
      const rnd = random(2);
      let ops = 0;
      let folded = 0;
      for (let run = 0; run < 800; run++) {
        const initial: JsonObject = {
          list: [
            { x: "a", n: 0 },
            { x: "b", n: 1 },
          ],
        };
        const sequence = trackedSequence(
          initial,
          5 + rnd.int(40),
          rnd,
          (draft) => mutateList(draft, rnd),
          () => ({ list: [] }),
        );
        const result = checkSequence(sequence, rnd);
        ops += result.ops;
        folded += result.folded;
      }
      assertTrue(folded < ops * 0.9);
    },
    FUZZ_TIMEOUT,
  );

  it(
    "is invisible to a replica: a root array",
    () => {
      const rnd = random(3);
      for (let run = 0; run < 300; run++) {
        const initial = Array.from({ length: rnd.int(6) }, () => rnd.value());
        const replace = () => Array.from({ length: rnd.int(6) }, () => rnd.value());
        checkSequence(
          trackedSequence(initial, 40, rnd, (draft) => mutateRootArray(draft, rnd), replace),
          rnd,
        );
      }
    },
    FUZZ_TIMEOUT,
  );

  it(
    "is invisible to a replica: diffRevisions between arbitrary values",
    () => {
      const rnd = random(4);
      for (let run = 0; run < 500; run++) checkSequence(diffSequence(rnd, 30), rnd);
    },
    FUZZ_TIMEOUT,
  );
});

/** Push each op as its own batch, take, and check the replay; returns the folded ops. */
function foldOneByOne(initial: JsonValue, ops: readonly Op[]): readonly Op[] {
  const queue = PendingOps.make();
  for (const op of ops) PendingOps.push(queue, [op]);
  const batch = PendingOps.take(queue);
  if (batch._tag !== "Ops") throw new Error("unexpected overflow");
  const expected = Delta.applyImmutableUnsafe(structuredClone(initial), ops);
  deepStrictEqual(Delta.applyImmutableUnsafe(structuredClone(initial), batch.ops), expected);
  const encode = Wire.makeEncoder();
  const decode = Wire.makeDecoder();
  const base = Delta.applyImmutableUnsafe(
    null,
    Wire.decodeUnsafe(decode, JSON.parse(JSON.stringify(Wire.encode(encode, [["r", initial]])))),
  );
  deepStrictEqual(
    Delta.applyImmutableUnsafe(
      base,
      Wire.decodeUnsafe(decode, JSON.parse(JSON.stringify(Wire.encode(encode, batch.ops)))),
    ),
    expected,
  );
  return batch.ops;
}

describe("PendingOps adversarial sequences", () => {
  it("a leaf set in an array element, a splice before it, the same path set again", () => {
    const ops: Op[] = [
      ["s", ["arr", 2, "x"], 1],
      ["p", ["arr"], 0, 0, [{ x: 9 }]],
      ["s", ["arr", 2, "x"], 2],
    ];
    deepStrictEqual(foldOneByOne({ arr: [{ x: 0 }, { x: 0 }, { x: 0 }] }, ops), ops);
    const appends: Op[] = [
      ["a", ["arr", 0, "x"], "1"],
      ["p", ["arr"], 0, 0, [{ x: "new" }]],
      ["a", ["arr", 0, "x"], "2"],
    ];
    deepStrictEqual(foldOneByOne({ arr: [{ x: "a" }, { x: "b" }] }, appends), appends);
  });

  it("`t` then `a`, and appends around a trim", () => {
    strictEqual(
      foldOneByOne({ s: "hello" }, [
        ["t", ["s"], 2],
        ["a", ["s"], "!"],
      ]).length,
      2,
    );
    strictEqual(
      foldOneByOne({ s: "hello" }, [
        ["a", ["s"], "AB"],
        ["t", ["s"], 3],
        ["a", ["s"], "C"],
      ]).length,
      3,
    );
  });

  it("an object `s` at a parent, then leaf sets below it", () => {
    strictEqual(
      foldOneByOne({ o: { a: 1 } }, [
        ["s", ["o", "a"], 2],
        ["s", ["o"], { a: 5, b: 6 }],
        ["s", ["o", "a"], 7],
      ]).length,
      3,
    );
    deepStrictEqual(
      foldOneByOne({ x: { s: "a" } }, [
        ["s", ["x"], { s: "b" }],
        ["a", ["x", "s"], "c"],
        ["s", ["x", "s"], "d"],
      ]),
      [
        ["s", ["x"], { s: "b" }],
        ["s", ["x", "s"], "d"],
      ],
    );
  });

  it("an ancestor primitive `s` between leaf ops below it", () => {
    deepStrictEqual(
      foldOneByOne({ o: { a: "x", b: "y" } }, [
        ["a", ["o", "a"], "1"],
        ["s", ["o", "b"], "q"],
        ["s", ["o"], "flat"],
        ["a", ["o"], "!"],
      ]),
      [
        ["a", ["o", "a"], "1"],
        ["s", ["o", "b"], "q"],
        ["s", ["o"], "flat!"],
      ],
    );
    deepStrictEqual(
      foldOneByOne({ a: { b: 0 } }, [
        ["s", ["a", "b"], 1],
        ["s", ["a"], null],
        ["s", ["a"], "x"],
      ]),
      [
        ["s", ["a", "b"], 1],
        ["s", ["a"], "x"],
      ],
    );
  });

  it("deletes, moves and splices shift what a path names", () => {
    strictEqual(
      foldOneByOne({ arr: ["a", "b", "c"] }, [
        ["s", ["arr", 2], "C"],
        ["d", ["arr", 0]],
        ["s", ["arr", 1], "Z"],
      ]).length,
      3,
    );
    strictEqual(
      foldOneByOne({ arr: [1, 2, 3] }, [
        ["s", ["arr", 0], 10],
        ["m", ["arr"], [2, 1, 0]],
        ["s", ["arr", 0], 20],
      ]).length,
      3,
    );
    strictEqual(
      foldOneByOne({ arr: [{ x: 0 }] }, [
        ["s", ["arr", 0, "x"], 1],
        ["p", ["arr"], 1, 0, [{ x: 0 }]],
        ["s", ["arr", 0, "x"], 2],
        ["s", ["arr", 1, "x"], 3],
      ]).length,
      4,
    );
  });

  it("keys of one parent never collide: `a.b` and `a`/`b`, `1` on an object and on an array", () => {
    foldOneByOne({ a: { b: "x" }, "a.b": "z" }, [
      ["a", ["a", "b"], "1"],
      ["s", ["a.b"], "y"],
      ["a", ["a", "b"], "2"],
    ]);
    foldOneByOne({ o: { "1": "a" }, arr: ["x", "y"] }, [
      ["s", ["o", "1"], "b"],
      ["s", ["arr", 1], "z"],
      ["s", ["o", "1"], "c"],
      ["s", ["arr", 1], "w"],
    ]);
  });

  it("an array set at the end appends, and a later set of that index folds into it", () => {
    deepStrictEqual(
      foldOneByOne({ arr: [1] }, [
        ["s", ["arr", 1], 2],
        ["s", ["arr", 0], 9],
        ["s", ["arr", 1], 3],
      ]),
      [
        ["s", ["arr", 1], 3],
        ["s", ["arr", 0], 9],
      ],
    );
  });

  it("`r` drops everything before it", () => {
    deepStrictEqual(
      foldOneByOne({ a: 1 }, [
        ["s", ["a"], 2],
        ["r", { b: "x" }],
        ["a", ["b"], "y"],
      ]),
      [
        ["r", { b: "x" }],
        ["a", ["b"], "y"],
      ],
    );
  });

  it("joins appends, folds appends into a string set, and keeps split surrogates", () => {
    deepStrictEqual(
      foldOneByOne({ s: "" }, [
        ["s", ["s"], "ab"],
        ["a", ["s"], "c"],
        ["s", ["t"], 1],
        ["a", ["s"], "d"],
      ]),
      [
        ["s", ["s"], "abcd"],
        ["s", ["t"], 1],
      ],
    );
    deepStrictEqual(
      foldOneByOne({ s: "" }, [
        ["a", ["s"], "\ud83d"],
        ["s", ["n"], 1],
        ["a", ["s"], "\ude00"],
        ["a", ["s"], "é"],
      ]),
      [
        ["a", ["s"], "😀é"],
        ["s", ["n"], 1],
      ],
    );
  });

  it("interleaved leaves, a structural op, then the same leaves again", () => {
    deepStrictEqual(
      foldOneByOne({ a: "", b: "", list: [0] }, [
        ["a", ["a"], "1"],
        ["a", ["b"], "1"],
        ["a", ["a"], "2"],
        ["p", ["list"], 1, 0, [1]],
        ["a", ["b"], "2"],
        ["a", ["a"], "3"],
      ]),
      [
        ["a", ["a"], "12"],
        ["a", ["b"], "1"],
        ["p", ["list"], 1, 0, [1]],
        ["a", ["b"], "2"],
        ["a", ["a"], "3"],
      ],
    );
  });
});

/** `count` set ops on distinct paths. */
const distinctSets = (count: number, from = 0): Op[] =>
  Array.from({ length: count }, (_, index): Op => ["s", ["k", from + index], index]);

describe("PendingOps cap", () => {
  it(`keeps ${MAX_PENDING_OPS} pending ops and replaces past them`, () => {
    const queue = PendingOps.make();
    PendingOps.push(queue, []);
    PendingOps.push(queue, distinctSets(MAX_PENDING_OPS));
    const exact = PendingOps.take(queue);
    strictEqual(exact._tag === "Ops" && exact.ops.length, MAX_PENDING_OPS);

    PendingOps.push(queue, distinctSets(MAX_PENDING_OPS / 2));
    PendingOps.push(queue, distinctSets(MAX_PENDING_OPS / 2, MAX_PENDING_OPS / 2));
    const full = PendingOps.take(queue);
    strictEqual(full._tag === "Ops" && full.ops.length, MAX_PENDING_OPS);

    PendingOps.push(queue, distinctSets(MAX_PENDING_OPS / 2));
    PendingOps.push(queue, distinctSets(MAX_PENDING_OPS / 2 + 1, MAX_PENDING_OPS / 2));
    deepStrictEqual(PendingOps.take(queue), { _tag: "Replace" });
  });

  it("counts ops after folding", () => {
    const queue = PendingOps.make();
    for (let index = 0; index < 3 * MAX_PENDING_OPS; index++)
      PendingOps.push(queue, [["a", ["text"], "x"]]);
    deepStrictEqual(PendingOps.take(queue), {
      _tag: "Ops",
      ops: [["a", ["text"], "x".repeat(3 * MAX_PENDING_OPS)]],
    });
  });

  it("accumulates nothing once replaced, until taken or a batch starting with `r`", () => {
    const queue = PendingOps.make();
    PendingOps.push(queue, distinctSets(1));
    PendingOps.push(queue, distinctSets(MAX_PENDING_OPS + 1));
    PendingOps.push(queue, distinctSets(3));
    deepStrictEqual(PendingOps.take(queue), { _tag: "Replace" });
    deepStrictEqual(PendingOps.take(queue), { _tag: "Ops", ops: [] });

    PendingOps.push(queue, distinctSets(1));
    PendingOps.push(queue, distinctSets(MAX_PENDING_OPS + 1));
    const fresh: readonly Op[] = [["r", { k: [0] }]];
    PendingOps.push(queue, fresh);
    const taken = PendingOps.take(queue);
    strictEqual(taken._tag === "Ops" && taken.ops, fresh);
    PendingOps.push(queue, distinctSets(1));
    PendingOps.push(queue, fresh);
    PendingOps.push(queue, [["s", ["n"], 1]]);
    deepStrictEqual(PendingOps.take(queue), { _tag: "Ops", ops: [...fresh, ["s", ["n"], 1]] });
  });

  it("hands one batch over as received, whatever its size", () => {
    const queue = PendingOps.make();
    const batch = distinctSets(MAX_PENDING_OPS + 1);
    PendingOps.push(queue, batch);
    const taken = PendingOps.take(queue);
    strictEqual(taken._tag === "Ops" && taken.ops, batch);
    PendingOps.push(queue, batch);
    PendingOps.push(queue, [["s", ["n"], 1]]);
    deepStrictEqual(PendingOps.take(queue), { _tag: "Replace" });
  });
});

describe("PendingOps ownership", () => {
  it("hands over a received batch by identity and never mutates batches or op tuples", () => {
    const queue = PendingOps.make();
    const first: readonly Op[] = Object.freeze([Object.freeze(["a", ["s"], "1"] as const)]);
    PendingOps.push(queue, first);
    const alone = PendingOps.take(queue);
    strictEqual(alone._tag === "Ops" && alone.ops, first);

    const second: readonly Op[] = Object.freeze([Object.freeze(["a", ["s"], "2"] as const)]);
    PendingOps.push(queue, first);
    PendingOps.push(queue, second);
    deepStrictEqual(PendingOps.take(queue), { _tag: "Ops", ops: [["a", ["s"], "12"]] });
    deepStrictEqual(first, [["a", ["s"], "1"]]);
    deepStrictEqual(second, [["a", ["s"], "2"]]);
  });
});

describe("PendingOps size", () => {
  it("counts pending ops after folding, the replacement once overflowed, and nothing once taken", () => {
    const queue = PendingOps.make();
    strictEqual(queue.size, 0);
    PendingOps.push(queue, []);
    strictEqual(queue.size, 0);
    PendingOps.push(queue, [
      ["a", ["text"], "a"],
      ["s", ["n"], 1],
    ]);
    strictEqual(queue.size, 2);
    PendingOps.push(queue, [
      ["a", ["text"], "b"],
      ["s", ["n"], 2],
      ["p", ["list"], 0, 0, [1]],
    ]);
    strictEqual(queue.size, 3);
    PendingOps.take(queue);
    strictEqual(queue.size, 0);

    PendingOps.push(queue, distinctSets(MAX_PENDING_OPS));
    strictEqual(queue.size, MAX_PENDING_OPS);
    PendingOps.push(queue, [["s", ["n"], 1]]);
    strictEqual(queue.size, 1);
    deepStrictEqual(PendingOps.take(queue), { _tag: "Replace" });
    strictEqual(queue.size, 0);
  });
});

describe("PendingOps cost", () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("computes one path key per leaf op once folding, none for one batch or structural ops", () => {
    const stringify = vi.spyOn(JSON, "stringify");
    const queue = PendingOps.make();
    const leaves: Op[] = [
      ["a", ["entries", 0, "text"], "a"],
      ["s", ["meta", "tokens"], 1],
    ];
    PendingOps.push(queue, leaves);
    PendingOps.take(queue);
    strictEqual(stringify.mock.calls.length, 0);

    PendingOps.push(queue, leaves);
    strictEqual(stringify.mock.calls.length, 0);
    PendingOps.push(queue, [
      ...leaves,
      ["p", ["entries"], 1, 0, [{ text: "" }]],
      ["s", ["meta"], { tokens: 0 }],
      ["d", ["x"]],
    ]);
    strictEqual(stringify.mock.calls.length, 4);
  });

  it("keeps one op per leaf path however many commits write it", () => {
    const queue = PendingOps.make();
    for (let index = 0; index < 50_000; index++) {
      PendingOps.push(queue, [
        ["a", ["entries", 0, "text"], "tok "],
        ["s", ["meta", "tokens"], index],
      ]);
    }
    const taken = PendingOps.take(queue);
    deepStrictEqual(taken._tag === "Ops" && taken.ops, [
      ["a", ["entries", 0, "text"], "tok ".repeat(50_000)],
      ["s", ["meta", "tokens"], 49_999],
    ]);
  });
});

const collect = (() => {
  setFlagsFromString("--expose-gc");
  const gc = runInNewContext("gc") as () => void;
  return async (): Promise<void> => {
    for (let round = 0; round < 5; round++) {
      // A macrotask turn from `node:timers/promises`, since global timers are linted out.
      await setImmediate();
      gc();
    }
  };
})();

describe("PendingOps references", () => {
  it("releases a folded-over op, and the index and everything pending once taken", async () => {
    const queue = PendingOps.make();
    const pending = (): { payload: WeakRef<object>; replaced: WeakRef<object> } => {
      const payload = { big: "x".repeat(1000) };
      const replaced: Op = ["s", ["n"], 1];
      PendingOps.push(queue, [replaced]);
      PendingOps.push(queue, [
        ["s", ["n"], 2],
        ["s", ["o"], payload],
      ]);
      return { payload: new WeakRef(payload), replaced: new WeakRef(replaced) };
    };
    const first = pending();
    await collect();
    // The set of `n` replaced the first one: only its newest op is held.
    strictEqual(first.replaced.deref(), undefined);
    assertTrue(first.payload.deref() !== undefined);
    PendingOps.take(queue);
    await collect();
    strictEqual(first.payload.deref(), undefined);

    // Taken while the folding state holds the index: nothing stays behind in the queue.
    const second = pending();
    PendingOps.push(queue, [["p", ["list"], 0, 0, [1]]]);
    PendingOps.take(queue);
    await collect();
    strictEqual(second.payload.deref(), undefined);
  });
});

import { describe, it } from "@effect/vitest";
import {
  assertFalse,
  assertTrue,
  deepStrictEqual,
  doesNotThrow,
  strictEqual,
} from "@effect/vitest/utils";
import * as Delta from "@effect-durable-harness/delta/Delta";
import * as Tracker from "@effect-durable-harness/delta/Tracker";
import * as Wire from "@effect-durable-harness/delta/Wire";
import * as Effect from "effect/Effect";
import * as Result from "effect/Result";
import type * as Schema from "effect/Schema";
import { throwsDeltaError, throwsMessage, throwsTypeError } from "./utils.ts";

describe("immutable tracker lifecycle", () => {
  it.effect(
    "keeps a draft alive across an asynchronous boundary and adopts only a prepared change",
    () =>
      Effect.gen(function* () {
        const input = { count: 1, nested: { text: "a" }, values: [1] };
        const tracker = Tracker.make(input);
        strictEqual(tracker.value, input);
        assertFalse(Object.isFrozen(tracker.value));
        assertFalse(Object.isFrozen(tracker.value.nested));

        const change = Tracker.beginChange(tracker);
        change.state.count = 2;
        yield* Effect.yieldNow;
        change.state.nested.text += "b";
        change.state.values.push(2);
        deepStrictEqual(tracker.value, input);

        const prepared = Tracker.prepare(change);
        strictEqual(prepared.base, tracker.value);
        deepStrictEqual(prepared.value, { count: 2, nested: { text: "ab" }, values: [1, 2] });
        deepStrictEqual(prepared.ops, [
          ["s", ["count"], 2],
          ["a", ["nested", "text"], "b"],
          ["p", ["values"], 1, 0, [2]],
        ]);
        throwsTypeError(() => change.state.count);
        deepStrictEqual(tracker.value, input);

        Tracker.adopt(tracker, prepared);
        strictEqual(tracker.value, prepared.value);
      }),
  );

  it("aborts and revokes without changing the committed value", () => {
    const tracker = Tracker.make({ child: { value: 1 } });
    const change = Tracker.beginChange(tracker);
    const child = change.state.child;
    child.value = 2;
    Tracker.abort(change);
    strictEqual(tracker.value.child.value, 1);
    throwsTypeError(() => child.value);
    doesNotThrow(() => Tracker.abort(change));
    throwsMessage(() => Tracker.prepare(change), /settled/);
    const next = Tracker.beginChange(tracker);
    Tracker.abort(next);
  });

  it("grows arrays with explicit nulls and revokes drafts after preparation", () => {
    const tracker = Tracker.make({ values: [1, 2] as Array<number | null> });
    const change = Tracker.beginChange(tracker);
    change.state.values.length = 4;
    const prepared = Tracker.prepare(change);
    deepStrictEqual(prepared.value.values, [1, 2, null, null]);
    throwsTypeError(() => change.state.values);
    Tracker.adopt(tracker, prepared);
  });

  it("normalizes a deep no-op to exact previous identity", () => {
    const tracker = Tracker.make({ value: { nested: [1, 2] } });
    const change = Tracker.beginChange(tracker);
    change.state.value = { nested: [1, 2] };
    const prepared = Tracker.prepare(change);
    strictEqual(prepared.value, prepared.base);
    deepStrictEqual(prepared.ops, []);
    Tracker.adopt(tracker, prepared);
    strictEqual(tracker.value, prepared.base);
  });

  it("takes immutable ownership of replacement input and applies no-op normalization", () => {
    const tracker = Tracker.make({ nested: { value: 1 } });
    const replacement = { nested: { value: 2 } };
    const prepared = Tracker.prepareReplace(tracker, replacement);
    strictEqual(prepared.value, replacement);
    deepStrictEqual(prepared.ops, [["r", replacement]]);
    Tracker.adopt(tracker, prepared);

    const noOp = Tracker.prepareReplace(tracker, { nested: { value: 2 } });
    strictEqual(noOp.value, tracker.value);
    deepStrictEqual(noOp.ops, []);
  });

  it("shares immutable operation placements with the prepared candidate", () => {
    const tracker = Tracker.make({ rows: [] as Array<{ id: number }> });
    const change = Tracker.beginChange(tracker);
    change.state.rows.push({ id: 1 });
    const prepared = Tracker.prepare(change);
    const splice = prepared.ops[0]!;
    if (splice[0] !== "p") throw new Error("expected splice");
    assertTrue(splice[4][0] === prepared.value.rows[0]);
    assertFalse(Object.isFrozen(prepared));
    assertFalse(Object.isFrozen(prepared.ops));
  });

  it("rejects foreign, stale, and repeated preparations while allowing competing changes", () => {
    const first = Tracker.make({ value: 0 });
    const second = Tracker.make({ value: 0 });
    const change = Tracker.beginChange(first);
    const competing = Tracker.beginChange(first);
    change.state.value = 1;
    competing.state.value = 2;
    const prepared = Tracker.prepare(change);
    const competingPrepared = Tracker.prepare(competing);
    throwsMessage(() => Tracker.adopt(second, prepared), /different tracker/);
    Tracker.adopt(first, prepared);
    throwsMessage(() => Tracker.adopt(first, prepared), /already been used/);
    throwsMessage(() => Tracker.adopt(first, competingPrepared), /stale/);

    const stale = Tracker.prepareReplace(first, { value: 2 });
    const winner = Tracker.prepareReplace(first, { value: 3 });
    Tracker.adopt(first, winner);
    throwsMessage(() => Tracker.adopt(first, stale), /stale/);
    throwsMessage(() => Tracker.adopt(first, stale), /stale/);
  });

  it("invalidates a prepared result when its change is aborted", () => {
    const tracker = Tracker.make({ value: 0 });
    const change = Tracker.beginChange(tracker);
    change.state.value = 1;
    const prepared = Tracker.prepare(change);
    Tracker.abort(change);
    throwsMessage(() => Tracker.adopt(tracker, prepared), /aborted/);
    doesNotThrow(() => Tracker.abort(change));
  });

  it("makes competing same-base preparations stale after adopting a no-op", () => {
    const tracker = Tracker.make({ value: { count: 1 } });
    const first = Tracker.prepareReplace(tracker, { value: { count: 1 } });
    const competing = Tracker.prepareReplace(tracker, { value: { count: 1 } });
    strictEqual(first.value, first.base);
    strictEqual(competing.value, competing.base);
    Tracker.adopt(tracker, first);
    throwsMessage(() => Tracker.adopt(tracker, first), /already been used/);
    throwsMessage(() => Tracker.adopt(tracker, competing), /stale/);
  });

  it("emits an owned root replacement without traversing large replacement input", () => {
    const rows = Array.from({ length: 10_000 }, (_, value) => ({ value, stable: { value } }));
    const tracker = Tracker.make({ rows });
    const replacement = { rows: tracker.value.rows.slice() };
    replacement.rows[5_000] = { value: -1, stable: replacement.rows[5_000]!.stable };
    const prepared = Tracker.prepareReplace(tracker, replacement);
    strictEqual(prepared.value, replacement);
    deepStrictEqual(prepared.ops, [["r", replacement]]);
    deepStrictEqual(Delta.applyImmutableUnsafe(prepared.base, prepared.ops), prepared.value);
  });
});

describe("canonical strings", () => {
  it("emits append and rolling-window operations", () => {
    const tracker = Tracker.make({ text: "abcdefgh" });
    let change = Tracker.beginChange(tracker);
    change.state.text += "ij";
    let prepared = Tracker.prepare(change);
    deepStrictEqual(prepared.ops, [["a", ["text"], "ij"]]);
    Tracker.adopt(tracker, prepared);

    change = Tracker.beginChange(tracker);
    change.state.text = `${change.state.text.slice(3)}xyz`;
    prepared = Tracker.prepare(change);
    deepStrictEqual(prepared.ops, [
      ["t", ["text"], 3],
      ["a", ["text"], "xyz"],
    ]);
  });

  it("finds bounded overlaps", () => {
    strictEqual(Delta.overlap("abcdefgh", "defghxyz", { scan: 65_536 }), 5);
    strictEqual(Delta.overlap("abcdef", "defghi", { scan: 0 }), 0);
  });
});

describe("operation application and validation", () => {
  it("applies mutable and immutable operations", () => {
    const operations: Array<Delta.Operation> = [
      ["a", ["text"], "b"],
      ["p", ["values"], 1, 1, [3, 4]],
      ["s", ["nested", "value"], 2],
    ];
    const base = { text: "a", values: [1, 2], nested: { value: 1 }, stable: { value: 9 } };
    const immutable = Delta.applyImmutableUnsafe(base, operations);
    deepStrictEqual(immutable, {
      text: "ab",
      values: [1, 3, 4],
      nested: { value: 2 },
      stable: { value: 9 },
    });
    deepStrictEqual(base, {
      text: "a",
      values: [1, 2],
      nested: { value: 1 },
      stable: { value: 9 },
    });
    strictEqual(immutable.stable, base.stable);
    deepStrictEqual(Delta.applyUnsafe(structuredClone(base), operations), immutable);
  });

  it("supports root replacement, root splice, and permutations", () => {
    const base: Array<Delta.Operation> = [["r", [1, 2, 3]]];
    assertTrue(Delta.isBase(base));
    let value = Delta.applyUnsafe<Array<number>>(undefined, base);
    value = Delta.applyUnsafe(value, [["p", [], 1, 1, [4]]]);
    value = Delta.applyUnsafe(value, [["m", [], [2, 0, 1]]]);
    deepStrictEqual(value, [3, 1, 4]);
  });

  it("rejects unsafe and malformed paths", () => {
    throwsDeltaError(
      () => Delta.applyUnsafe({}, [["s", ["constructor", "prototype", "x"], true]]),
      "UnsafePathError",
    );
    throwsDeltaError(
      () => Delta.applyUnsafe({ values: [1] }, [["s", ["values", 3], 2]]),
      "UnsafePathError",
    );
    throwsDeltaError(() => Delta.applyUnsafe({ value: 1 }, [["a", ["value"], "x"]]));
    throwsDeltaError(() => Delta.validateOperationUnsafe(["s", "value", 1]));
    throwsDeltaError(
      () => Delta.validateOperationUnsafe(["m", [], [0, 0]]),
      "InvalidOperationError",
      /bijection/,
    );
  });

  it("validates immutable operations before traversing the target", () => {
    let reads = 0;
    const target = Object.defineProperty({}, "trap", {
      enumerable: true,
      get() {
        reads += 1;
        return {};
      },
    });
    throwsDeltaError(
      () => Delta.applyImmutableUnsafe(target, [["s", "bad-path", 1] as never]),
      "InvalidOperationError",
      /path/,
    );
    strictEqual(reads, 0);
  });

  it("validates decoded and wire vocabularies separately", () => {
    doesNotThrow(() => Delta.validateOperationUnsafe(["s", ["value"], 1]));
    throwsDeltaError(() => Delta.validateOperationUnsafe(["s", 1]));
    doesNotThrow(() => Wire.validateOperationUnsafe(["s", 1]));
    doesNotThrow(() => Wire.validateOperationUnsafe(["#", 0, ["value"]]));
  });
});

describe("codec", () => {
  it("interns paths, omits adjacent paths, and round-trips", () => {
    const path = ["nested", "text"] as const;
    const encoder = Wire.makeEncoder();
    const decoder = Wire.makeDecoder();
    const first: Array<Delta.Operation> = [
      ["t", path, 1],
      ["a", path, "x"],
    ];
    deepStrictEqual(Wire.decodeUnsafe(decoder, Wire.encode(encoder, first)), first);
    const second: Array<Delta.Operation> = [["a", path, "y"]];
    const wire = Wire.encode(encoder, second);
    deepStrictEqual(wire, [
      ["#", 0, path],
      ["a", 0, "y"],
    ]);
    deepStrictEqual(Wire.decodeUnsafe(decoder, wire), second);
  });

  it("resets path dictionaries on a base", () => {
    const encoder = Wire.makeEncoder();
    const path = ["value"] as const;
    Wire.encode(encoder, [["s", path, 1]]);
    Wire.encode(encoder, [["s", path, 2]]);
    deepStrictEqual(Wire.encode(encoder, [["r", { value: 3 }]]), [["r", { value: 3 }]]);
    deepStrictEqual(Wire.encode(encoder, [["s", path, 4]]), [["s", path, 4]]);
  });

  it("rejects unresolved short forms and unsafe interned paths", () => {
    throwsDeltaError(() => Wire.decodeUnsafe(Wire.makeDecoder(), [["a", "x"]]));
    const wire = [
      ["#", 0, ["__proto__"]],
      ["s", 0, true],
    ];
    throwsDeltaError(() => Wire.decodeUnsafe(Wire.makeDecoder(), wire), "UnsafePathError");
  });

  it("omits an adjacent repeated path", () => {
    const encoder = Wire.makeEncoder();
    const path = ["value"] as const;
    deepStrictEqual(
      Wire.encode(encoder, [
        ["s", path, 1],
        ["s", path, 2],
      ]),
      [
        ["s", path, 1],
        ["s", 2],
      ],
    );
  });

  it("interns on second use rather than first", () => {
    const encoder = Wire.makeEncoder();
    const path = ["a", "deep"] as const;
    deepStrictEqual(Wire.encode(encoder, [["a", path, "1"]]), [["a", path, "1"]]);
    deepStrictEqual(Wire.encode(encoder, [["a", path, "2"]]), [
      ["#", 0, path],
      ["a", 0, "2"],
    ]);
  });

  it("does not collide paths containing null characters", () => {
    const operations: Array<Delta.Operation> = [
      ["s", ["a\u0000b"], 1],
      ["s", ["a", "b"], 2],
    ];
    deepStrictEqual(
      Wire.decodeUnsafe(Wire.makeDecoder(), Wire.encode(Wire.makeEncoder(), operations)),
      operations,
    );
  });

  it("clears decoder ids on a base batch", () => {
    const decoder = Wire.makeDecoder();
    Wire.decodeUnsafe(decoder, [
      ["#", 0, ["a"]],
      ["a", 0, "1"],
    ]);
    Wire.decodeUnsafe(decoder, [["r", { a: "" }]]);
    throwsDeltaError(() => Wire.decodeUnsafe(decoder, [["a", 0, "2"]]));
  });

  it("makes batches after a base self-contained", () => {
    const encoder = Wire.makeEncoder();
    const path = ["a", "deep"] as const;
    Wire.encode(encoder, [["a", path, "1"]]);
    Wire.encode(encoder, [["a", path, "2"]]);
    const base = Wire.encode(encoder, [["r", { a: { deep: "x" } }]]);
    const after = Wire.encode(encoder, [["a", path, "3"]]);
    deepStrictEqual(after, [["a", path, "3"]]);
    const decoder = Wire.makeDecoder();
    deepStrictEqual(Wire.decodeUnsafe(decoder, base), [["r", { a: { deep: "x" } }]]);
    deepStrictEqual(Wire.decodeUnsafe(decoder, after), [["a", path, "3"]]);
  });

  it("round-trips deterministic mixed operation streams", () => {
    const batches: Array<Array<Delta.Operation>> = Array.from({ length: 100 }, (_, index) => [
      ["s", ["rows", index, "value"], index],
      ["a", ["output"], String(index)],
      ["p", ["tail"], index, 0, [index]],
    ]);
    const encoder = Wire.makeEncoder();
    const decoder = Wire.makeDecoder();
    deepStrictEqual(
      batches.map((batch) => Wire.decodeUnsafe(decoder, Wire.encode(encoder, batch))),
      batches,
    );
  });
});

describe("immutable application ownership", () => {
  it("does not mutate a replacement payload targeted by a later operation", () => {
    const replacement = { nested: { value: 1 } };
    const next = Delta.applyImmutableUnsafe<{ nested: { value: number } }>(undefined, [
      ["r", replacement],
      ["s", ["nested", "value"], 2],
    ]);
    strictEqual(replacement.nested.value, 1);
    strictEqual(next.nested.value, 2);
  });

  it("adopts a mutable replacement payload rather than copying it", () => {
    const batch: Array<Delta.Operation> = [["r", { value: 0 }]];
    const first = Delta.applyUnsafe<{ value: number }>(undefined, batch);
    const second = Delta.applyUnsafe<{ value: number }>(undefined, batch);
    first.value = 1;
    strictEqual(second.value, 1);
  });
});

describe("path and prototype safety", () => {
  it("rejects constructor walks and forbidden interned paths", () => {
    throwsDeltaError(() =>
      Delta.applyUnsafe({}, JSON.parse('[["s",["constructor","prototype","gadget"],true]]')),
    );
    strictEqual(({} as Record<string, unknown>).gadget, undefined);
    const wire = [
      ["#", 0, ["__proto__", "w"]],
      ["s", 0, true],
    ];
    throwsDeltaError(() => Wire.decodeUnsafe(Wire.makeDecoder(), wire));
  });

  it("does not run inherited setters", () => {
    Object.defineProperty(Object.prototype, "trap", {
      set() {
        throw new Error("inherited setter ran");
      },
      configurable: true,
    });
    try {
      doesNotThrow(() => Delta.applyUnsafe({}, [["s", ["trap"], 1]]));
      deepStrictEqual(Delta.applyUnsafe({}, [["s", ["trap"], 1]]), { trap: 1 });
    } finally {
      delete (Object.prototype as Record<string, unknown>).trap;
    }
  });

  it("allows reserved names inside values without prototype pollution", () => {
    const value = JSON.parse('{"__proto__":{"z":1}}') as Schema.Json;
    const out = Delta.applyUnsafe<{ value?: object }>({}, [["s", ["value"], value]]);
    assertTrue(out.value !== undefined);
    assertTrue(Object.hasOwn(out.value, "__proto__"));
    strictEqual(({} as Record<string, unknown>).z, undefined);
  });

  it("imports own properties without invoking inherited setters", () => {
    const root = {} as Record<string, Schema.Json>;
    Object.defineProperty(root, "trap", {
      value: 1,
      writable: true,
      enumerable: true,
      configurable: true,
    });
    Object.defineProperty(Object.prototype, "trap", {
      set() {
        throw new Error("inherited setter ran");
      },
      configurable: true,
    });
    try {
      doesNotThrow(() => Tracker.make(root));
    } finally {
      delete (Object.prototype as Record<string, unknown>).trap;
    }

    let value: { values: Array<number> } | undefined;
    Object.defineProperty(Array.prototype, "0", {
      set() {
        throw new Error("inherited array setter ran");
      },
      configurable: true,
    });
    try {
      value = Tracker.make({ values: [1, 2] }).value;
    } finally {
      delete (Array.prototype as unknown as Record<number, unknown>)[0];
    }
    deepStrictEqual(value, { values: [1, 2] });
  });
});

describe("array index safety", () => {
  it("writes an existing index and appends exactly one past the end", () => {
    deepStrictEqual(Delta.applyUnsafe({ values: [1, 2, 3] }, [["s", ["values", 1], 9]]), {
      values: [1, 9, 3],
    });
    deepStrictEqual(Delta.applyUnsafe({ values: [1, 2, 3] }, [["s", ["values", 3], 9]]), {
      values: [1, 2, 3, 9],
    });
  });

  it("rejects gaps, huge indices, and string-spelled indices", () => {
    throwsDeltaError(() => Delta.applyUnsafe({ values: [1, 2, 3] }, [["s", ["values", 5], 9]]));
    throwsDeltaError(() =>
      Delta.applyUnsafe({ values: [] }, [["s", ["values", 4_294_967_290], 1]]),
    );
    throwsDeltaError(() => Delta.applyUnsafe({ values: [1] }, [["s", ["values", "0"], 9]]));
    throwsDeltaError(() => Delta.applyUnsafe({ values: ["a"] }, [["a", ["values", "0"], "b"]]));
  });

  it("allows explicit growth values and rejects deletion past the end", () => {
    deepStrictEqual(
      Delta.applyUnsafe({ values: [1] }, [["p", ["values"], 1, 0, [null, null, 9]]]),
      {
        values: [1, null, null, 9],
      },
    );
    throwsDeltaError(() => Delta.applyUnsafe({ values: [1] }, [["d", ["values", 1]]]));
  });

  it("applies large splice payloads without spreading them at once", () => {
    const items = new Array<Schema.Json>(300_000).fill(null);
    const result = Delta.applyUnsafe({ values: [] as Array<Schema.Json> }, [
      ["p", ["values"], 0, 0, items],
    ]);
    strictEqual(result.values.length, items.length);
  });
});

describe("operation structure safety", () => {
  it("rejects unknown verbs, malformed tuples, paths, and splice payloads", () => {
    throwsDeltaError(() => Delta.applyUnsafe({ value: 1 }, [["ZZZ", ["value"], 9] as never]));
    throwsDeltaError(() =>
      Delta.applyUnsafe({ values: [1] }, [["p", ["values"], 0, 0, "not-an-array"] as never]),
    );
    throwsDeltaError(() => Delta.applyUnsafe({ value: 1 }, [["s", "value", 9] as never]));
    throwsDeltaError(() => Delta.applyUnsafe({ value: 1 }, [{ op: "s" } as never]));
    throwsDeltaError(() => Delta.applyUnsafe({ value: 1 }, [null as never]));
  });

  it("rejects invalid append and truncation operations", () => {
    throwsDeltaError(() => Delta.applyUnsafe({ value: 1 }, [["a", ["missing"], "x"]]));
    throwsDeltaError(() => Delta.applyUnsafe({ value: 1 }, [["a", ["value"], "x"]]));
    throwsDeltaError(() => Delta.applyUnsafe({ value: "abc" }, [["t", ["value"], -1]]));
    throwsDeltaError(() => Wire.decodeUnsafe(Wire.makeDecoder(), [["t", ["value"], -1]]));
  });

  it("clamps splice removal past the end", () => {
    deepStrictEqual(Delta.applyUnsafe({ values: [1, 2] }, [["p", ["values"], 0, 1e9, []]]), {
      values: [],
    });
  });
});

describe("operation assertions", () => {
  it("accepts decoded operations and rejects wire-only forms", () => {
    for (const operation of [
      ["r", { value: 1 }],
      ["s", ["value"], 1],
      ["d", ["value"]],
      ["a", ["value"], "x"],
      ["t", ["value"], 2],
      ["p", ["value"], 0, 0, []],
      ["m", ["value"], [0]],
    ]) {
      doesNotThrow(() => Delta.validateOperationUnsafe(operation));
    }
    for (const wireOnly of [
      ["s", 1],
      ["d"],
      ["a", "x"],
      ["t", 2],
      ["p", 0, 0, []],
      ["#", 0, ["value"]],
      ["s", 0, 1],
    ]) {
      throwsDeltaError(() => Delta.validateOperationUnsafe(wireOnly));
      doesNotThrow(() => Wire.validateOperationUnsafe(wireOnly));
    }
  });

  it("does not recursively inspect operation payloads", () => {
    doesNotThrow(() => Delta.validateOperationUnsafe(["s", ["value"], new Map()]));
    // @effect-diagnostics-next-line globalDate:off
    doesNotThrow(() => Wire.validateOperationUnsafe(["r", new Date()]));
  });
});

describe("Result variants", () => {
  it("returns failures as values instead of throwing", () => {
    const applied = Delta.apply({ value: 1 }, [["a", ["value"], "x"]]);
    assertTrue(Result.isFailure(applied));
    strictEqual(applied.failure.reason._tag, "UnresolvablePathError");

    const unsafe = Delta.applyImmutable({}, [["s", ["__proto__", "x"], 1]]);
    assertTrue(Result.isFailure(unsafe));
    strictEqual(unsafe.failure.reason._tag, "UnsafePathError");

    const invalid = Delta.validateOperation(["s", "value", 1]);
    assertTrue(Result.isFailure(invalid));
    strictEqual(invalid.failure.reason._tag, "InvalidOperationError");

    const decoded = Wire.decode(Wire.makeDecoder(), [["a", 0, "x"]]);
    assertTrue(Result.isFailure(decoded));
    strictEqual(decoded.failure.reason._tag, "UnresolvablePathError");
  });

  it("returns successes as values", () => {
    deepStrictEqual(Delta.apply({ value: 1 }, [["s", ["value"], 2]]), Result.succeed({ value: 2 }));
    deepStrictEqual(
      Delta.applyImmutableBatches({ text: "a" }, [[["a", ["text"], "b"]], [["a", ["text"], "c"]]]),
      Result.succeed({ text: "abc" }),
    );
    deepStrictEqual(Delta.validatePath(["a", 0]), Result.succeed(["a", 0]));
    assertTrue(Result.isFailure(Delta.validatePath(["a", -1])));
  });
});

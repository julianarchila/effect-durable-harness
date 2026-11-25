import { describe, it } from "@effect/vitest";
import { assertFalse, deepStrictEqual, strictEqual, throws } from "@effect/vitest/utils";
import * as Delta from "@effect-durable-harness/delta/Delta";
import * as Tracker from "@effect-durable-harness/delta/Tracker";
import type * as Schema from "effect/Schema";
import { throwsDeltaError, throwsMessage } from "./utils.ts";

function freezeDeep<T>(value: T): T {
  if (value === null || typeof value !== "object" || Object.isFrozen(value)) return value;
  for (const child of Object.values(value)) freezeDeep(child);
  return Object.freeze(value);
}

function clone<T>(value: T): T {
  return JSON.parse(JSON.stringify(value)) as T;
}

describe("checked immutable operation application", () => {
  it("copies each touched container once while preserving input and payload ownership", () => {
    const shared = freezeDeep({ nested: { value: 1 } });
    const untouched = freezeDeep({ value: 9 });
    const rowPayload = freezeDeep({ id: 4, label: "placed" });
    const base = freezeDeep({
      text: "abcdef",
      stable: { value: 7 },
      branch: { value: 1 },
      copy: null as { value: number } | null,
      placed: null as typeof shared | null,
      untouched: null as typeof untouched | null,
      left: null as typeof shared | null,
      right: null as typeof shared | null,
      meta: { count: 0, obsolete: true },
      rows: [
        { id: 1, label: "one" },
        { id: 2, label: "two" },
        { id: 3, label: "three" },
      ],
    });
    const operations = freezeDeep([
      ["t", ["text"], 2],
      ["a", ["text"], "!"],
      ["s", ["meta", "count"], 1],
      ["s", ["meta", "count"], 2],
      ["d", ["meta", "obsolete"]],
      ["s", ["copy"], base.branch],
      ["s", ["copy", "value"], 2],
      ["s", ["placed"], shared],
      ["s", ["placed", "nested", "value"], 2],
      ["s", ["untouched"], untouched],
      ["s", ["left"], shared],
      ["s", ["right"], shared],
      ["s", ["left", "nested", "value"], 3],
      ["p", ["rows"], 1, 1, [rowPayload]],
      ["s", ["rows", 1, "label"], "edited"],
      ["m", ["rows"], [1, 0, 2]],
      ["s", ["rows", 0, "label"], "moved"],
    ] satisfies Array<Delta.Operation>);

    const result = Delta.applyImmutableUnsafe(base, operations);
    const mutableResult = Delta.applyUnsafe(clone(base), clone(operations));

    deepStrictEqual(result, mutableResult);
    strictEqual(result.text, "cdef!");
    strictEqual(result.stable, base.stable);
    strictEqual(result.untouched, untouched);
    assertFalse(result.copy === base.branch);
    deepStrictEqual(result.copy, { value: 2 });
    assertFalse(result.placed === shared);
    deepStrictEqual(result.placed, { nested: { value: 2 } });
    assertFalse(result.left === shared);
    strictEqual(result.right, shared);
    assertFalse(result.rows[0] === rowPayload);
    deepStrictEqual(result.rows[0], { id: 4, label: "moved" });
    strictEqual(base.branch.value, 1);
    strictEqual(shared.nested.value, 1);
    strictEqual(rowPayload.label, "placed");
  });

  it("protects root replacement payloads before later object and array edits", () => {
    const replacement = freezeDeep({ nested: { value: 1 }, values: [1, 2, 3] });
    const result = Delta.applyImmutableBatchesUnsafe<typeof replacement>(undefined, [
      [["r", replacement]],
      [["s", ["nested", "value"], 2]],
      [
        ["p", ["values"], 1, 1, [4, 5]],
        ["m", ["values"], [3, 0, 1, 2]],
      ],
    ]);
    deepStrictEqual(result, { nested: { value: 2 }, values: [3, 1, 4, 5] });
    deepStrictEqual(replacement, { nested: { value: 1 }, values: [1, 2, 3] });

    const array = freezeDeep([1, 2, 3]);
    const arrayResult = Delta.applyImmutableUnsafe<Array<number>>(array, [
      ["p", [], 1, 1, [4, 5]],
      ["m", [], [3, 0, 1, 2]],
      ["d", [1]],
    ]);
    deepStrictEqual(arrayResult, [3, 4, 5]);
    deepStrictEqual(array, [1, 2, 3]);
  });

  it("shares one private copy-on-write scope across batch partitions", () => {
    const base = freezeDeep({
      text: "abcdef",
      meta: { count: 0 },
      values: [
        { id: 1, value: 1 },
        { id: 2, value: 2 },
        { id: 3, value: 3 },
      ],
    });
    const batches = [
      [
        ["s", ["meta", "count"], 1],
        ["p", ["values"], 1, 1, [{ id: 4, value: 4 }]],
      ] satisfies Array<Delta.Operation>,
      [] satisfies Array<Delta.Operation>,
      [
        ["m", ["values"], [2, 0, 1]],
        ["s", ["values", 2, "value"], 40],
      ] satisfies Array<Delta.Operation>,
      [
        ["t", ["text"], 2],
        ["a", ["text"], "!"],
      ] satisfies Array<Delta.Operation>,
    ];

    const intermediate = Delta.applyImmutableUnsafe(base, batches[0]!);
    const intermediateSnapshot = clone(intermediate);
    let sequential = intermediate;
    for (let index = 1; index < batches.length; index++) {
      sequential = Delta.applyImmutableUnsafe(sequential, batches[index]!);
    }
    const streamed = Delta.applyImmutableBatchesUnsafe(base, batches);
    const flattened = Delta.applyImmutableUnsafe(base, batches.flat());

    deepStrictEqual(streamed, sequential);
    deepStrictEqual(streamed, flattened);
    deepStrictEqual(intermediate, intermediateSnapshot);
    strictEqual(base.meta.count, 0);
    strictEqual(base.values[1]!.id, 2);
  });

  it("handles tracker-produced batches across arbitrary revision boundaries", () => {
    type Document = {
      text: string;
      values: Array<{ id: number; score: number }>;
      revision: number;
    };
    const initial: Document = {
      text: "start",
      values: Array.from({ length: 8 }, (_, id) => ({ id, score: 0 })),
      revision: 0,
    };
    const tracker = Tracker.make(initial);
    const batches: Array<Delta.Delta> = [];
    for (let revision = 1; revision <= 40; revision++) {
      const change = Tracker.beginChange(tracker);
      change.state.revision = revision;
      change.state.text = `${change.state.text.slice(1)}${revision}`;
      switch (revision % 5) {
        case 0:
          change.state.values.reverse();
          break;
        case 1:
          change.state.values.push({ id: 100 + revision, score: revision });
          break;
        case 2:
          change.state.values.shift();
          break;
        case 3:
          change.state.values[revision % change.state.values.length]!.score = revision;
          break;
        default:
          change.state.values.splice(1, 1, { id: 200 + revision, score: revision });
      }
      const prepared = Tracker.prepare(change);
      batches.push(prepared.ops);
      Tracker.adopt(tracker, prepared);
    }

    deepStrictEqual(Delta.applyImmutableBatchesUnsafe(initial, batches), tracker.value);
  });

  it("does not expose partial application when validation or iteration fails", () => {
    const base = freezeDeep({ nested: { value: 1 } });
    const invalidBatches: ReadonlyArray<Delta.Delta> = [
      [["s", ["nested", "value"], 2]],
      [["s", ["constructor", "prototype", "polluted"], true]],
    ];
    throws(() => Delta.applyImmutableBatchesUnsafe(base, invalidBatches));
    strictEqual(base.nested.value, 1);
    strictEqual(({} as { polluted?: boolean }).polluted, undefined);

    function* throwingBatches(): Generator<Delta.Delta> {
      yield [["s", ["nested", "value"], 3]];
      throw new Error("revision stream failed");
    }
    throwsMessage(
      () => Delta.applyImmutableBatchesUnsafe(base, throwingBatches()),
      /revision stream failed/,
    );
    strictEqual(base.nested.value, 1);

    let advancedPastInvalid = false;
    let iteratorClosed = false;
    function* invalidOperationBatches(): Generator<Delta.Delta> {
      try {
        yield [["s", ["nested", "value"], 4]];
        yield [["s", ["__proto__", "polluted"], true]];
        advancedPastInvalid = true;
      } finally {
        iteratorClosed = true;
      }
    }
    throwsDeltaError(
      () => Delta.applyImmutableBatchesUnsafe(base, invalidOperationBatches()),
      "UnsafePathError",
    );
    strictEqual(advancedPastInvalid, false);
    strictEqual(iteratorClosed, true);
    strictEqual(base.nested.value, 1);

    let reads = 0;
    const target = Object.defineProperty({}, "trap", {
      enumerable: true,
      get() {
        reads++;
        return {};
      },
    });
    const malformed: Array<Delta.Operation> = [["s", "bad-path", 1] as never];
    throwsDeltaError(
      () => Delta.applyImmutableBatchesUnsafe(target, [malformed]),
      "InvalidOperationError",
      /path/,
    );
    strictEqual(reads, 0);

    throwsDeltaError(
      () => Delta.applyImmutableUnsafe({ values: [] }, [["s", ["values", "missing", "value"], 1]]),
      "UnresolvablePathError",
    );
    throwsDeltaError(
      () => Delta.applyImmutableUnsafe({ values: [{}] }, [["s", ["values", "0", "value"], 1]]),
      "UnsafePathError",
    );
  });

  it("allows one immutable batch to fan out without mutating shared payloads", () => {
    const payload = freezeDeep({ nested: { value: 1 } });
    const operations: Delta.Delta = [
      ["s", ["placed"], payload],
      ["s", ["placed", "nested", "value"], 2],
    ];
    const base = freezeDeep({ placed: null as typeof payload | null });
    const first = Delta.applyImmutableUnsafe(base, operations);
    const second = Delta.applyImmutableUnsafe(base, operations);
    deepStrictEqual(first, second);
    assertFalse(first === second);
    assertFalse(first.placed === second.placed);
    strictEqual(payload.nested.value, 1);
  });
});

describe("immutable container copies", () => {
  it("keep an own __proto__ member as data and the prototype of the copied object", () => {
    const plain = JSON.parse('{"__proto__":{"polluted":true},"n":1}') as Record<
      string,
      Schema.Json
    >;
    const bare = Object.assign(Object.create(null) as Record<string, Schema.Json>, { n: 1 });
    for (const base of [plain, bare]) {
      const checked = Delta.applyImmutableUnsafe(base, [["s", ["n"], 2]]);
      // Tracker revisions copy through the trusted path.
      const tracker = Tracker.make(base);
      const change = Tracker.beginChange(tracker);
      change.state.n = 2;
      Tracker.adopt(tracker, Tracker.prepare(change));
      for (const copy of [checked, tracker.value]) {
        assertFalse(copy === base);
        strictEqual(Object.getPrototypeOf(copy), Object.getPrototypeOf(base));
        deepStrictEqual(Object.keys(copy), Object.keys(base));
        strictEqual(copy.n, 2);
        if (base === plain)
          deepStrictEqual(Object.getOwnPropertyDescriptor(copy, "__proto__")?.value, {
            polluted: true,
          });
      }
    }
    strictEqual(({} as { polluted?: boolean }).polluted, undefined);
  });
});

describe("immutable application complexity", () => {
  it("appends to an array not yet copied in one copy, sharing everything else", () => {
    const entries = Array.from({ length: 1_000 }, (_, index) => ({ index }));
    const base = freezeDeep({ log: { entries, meta: { count: 1_000 } }, other: { value: 1 } });
    const result = Delta.applyImmutableUnsafe(base, [
      ["p", ["log", "entries"], 1_000, 0, [{ index: 1_000 }]],
      ["p", ["log", "entries"], 1_001, 0, [{ index: 1_001 }]],
    ]);
    strictEqual(result.log.entries.length, 1_002);
    deepStrictEqual(result.log.entries.slice(-2), [{ index: 1_000 }, { index: 1_001 }]);
    strictEqual(base.log.entries.length, 1_000);
    strictEqual(result.log.entries[0], entries[0]);
    strictEqual(result.log.meta, base.log.meta);
    strictEqual(result.other, base.other);
    assertFalse(result.log === base.log);

    const rootArray = freezeDeep([1, 2]);
    deepStrictEqual(Delta.applyImmutableUnsafe(rootArray, [["p", [], 2, 0, [3]]]), [1, 2, 3]);
    deepStrictEqual(rootArray, [1, 2]);
    // An appended item that is itself an array stays one element.
    deepStrictEqual(
      Delta.applyImmutableUnsafe<Schema.Json>(rootArray, [["p", [], 2, 0, [[3, 4]]]]),
      [1, 2, [3, 4]],
    );
    // Invalid paths fail with the general path's errors.
    const invalid: Array<[Delta.Operation, "UnresolvablePathError" | "UnsafePathError"]> = [
      [["p", ["values", "missing"], 0, 0, []], "UnresolvablePathError"],
      [["p", ["missing"], 0, 0, []], "UnresolvablePathError"],
      [["p", ["values", 0], 0, 0, []], "UnresolvablePathError"],
      [["p", ["__proto__"], 0, 0, []], "UnsafePathError"],
      [["p", ["list", "x"], 0, 0, []], "UnresolvablePathError"],
      [["p", ["text"], 0, 0, []], "UnresolvablePathError"],
      [["p", ["list", -1], 0, 0, []], "UnsafePathError"],
      [["p", ["list", 1.5], 0, 0, []], "UnsafePathError"],
      [["p", ["list", 0, "constructor"], 0, 0, []], "UnsafePathError"],
    ];
    const document = freezeDeep({ values: [] as Array<Schema.Json>, list: [[1]], text: "a" });
    for (const [op, error] of invalid) {
      // `throwsDeltaError` reports the tag mismatch for each failing op.
      throwsDeltaError(() => Delta.applyImmutableUnsafe(document, [op]), error);
    }
    // An insert that is not at the end takes the general path.
    deepStrictEqual(Delta.applyImmutableUnsafe(rootArray, [["p", [], 1, 0, [9]]]), [1, 9, 2]);
  });

  it("copies a wide object once rather than once per repeated write", () => {
    const width = 20_000;
    const base: Record<string, Schema.Json> = {};
    for (let index = 0; index < width; index++) base[`field${index}`] = index;
    const operations: Array<Delta.Operation> = Array.from(
      { length: 1_000 },
      (_, index): Delta.Operation => ["s", [`field${index}`], -index],
    );
    // A copy lists the object's keys; the proxy counts the listings.
    let shallowCopies = 0;
    const counted = new Proxy(base, {
      ownKeys(target) {
        shallowCopies++;
        return Reflect.ownKeys(target);
      },
    });
    const result = Delta.applyImmutableUnsafe(counted, operations);

    strictEqual(result.field999, -999);
    strictEqual(base.field999, 999);
    strictEqual(shallowCopies, 1);
  });
});

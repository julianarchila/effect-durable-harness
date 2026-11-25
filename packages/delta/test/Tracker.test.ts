import { assert, describe, it } from "@effect/vitest";
import { assertFalse, assertTrue, deepStrictEqual, strictEqual } from "@effect/vitest/utils";
import * as Delta from "@effect-durable-harness/delta/Delta";
import * as Tracker from "@effect-durable-harness/delta/Tracker";
import * as Wire from "@effect-durable-harness/delta/Wire";
import * as Effect from "effect/Effect";
import type * as Schema from "effect/Schema";
import { isProxy } from "node:util/types";
import { throwsMessage, throwsTypeError } from "./utils.ts";

const clone = <T>(value: T): T => JSON.parse(JSON.stringify(value)) as T;

function replay<T>(base: T, operations: Delta.Delta): T {
  return Delta.applyUnsafe(clone(base), clone(operations));
}

function expectAliasFree(value: unknown): void {
  const seen = new WeakMap<object, string>();
  const visit = (current: unknown, path: string): void => {
    if (current === null || typeof current !== "object") return;
    const previous = seen.get(current);
    if (previous !== undefined) throw new Error(`container at ${path} aliases ${previous}`);
    seen.set(current, path);
    if (Array.isArray(current)) {
      for (let index = 0; index < current.length; index++)
        visit(current[index], `${path}[${index}]`);
      return;
    }
    for (const key of Object.keys(current))
      visit((current as Record<string, unknown>)[key], `${path}.${key}`);
  };
  visit(value, "$root");
}

function settle<T extends object>(
  tracker: Tracker.Tracker<T>,
  mutate: (draft: Tracker.Draft<T>) => void,
): T {
  const baseRoot = tracker.value;
  const base = clone(baseRoot);
  const change = Tracker.beginChange(tracker);
  mutate(change.state);
  const prepared = Tracker.prepare(change);
  const candidate = clone(prepared.value);
  assertFalse(isProxy(prepared.value));
  deepStrictEqual(replay(base, prepared.ops), candidate);
  deepStrictEqual(baseRoot, base);
  Tracker.adopt(tracker, prepared);
  deepStrictEqual(baseRoot, base);
  strictEqual(tracker.value, prepared.value);
  deepStrictEqual(tracker.value, candidate);
  return candidate;
}

describe("delta tracker transactional overlay lifecycle", () => {
  it("materializes an immutable next revision and adopts it by pointer swap", () => {
    const initial = { count: 1, nested: { text: "a" }, values: [1] };
    const tracker = Tracker.make(initial);
    strictEqual(tracker.value, initial);
    const change = Tracker.beginChange(tracker);
    change.state.count = 2;
    change.state.nested.text += "b";
    change.state.values.push(2);
    deepStrictEqual(initial, { count: 1, nested: { text: "a" }, values: [1] });
    const prepared = Tracker.prepare(change);
    const next = prepared.value;
    strictEqual(prepared.baseRevision, 0);
    strictEqual(prepared.base, initial);
    assertTrue(next !== initial);
    assertFalse(isProxy(next));
    deepStrictEqual(next, { count: 2, nested: { text: "ab" }, values: [1, 2] });
    deepStrictEqual(prepared.ops, [
      ["s", ["count"], 2],
      ["a", ["nested", "text"], "b"],
      ["p", ["values"], 1, 0, [2]],
    ]);
    strictEqual(tracker.value, initial);
    Tracker.adopt(tracker, prepared);
    deepStrictEqual(initial, { count: 1, nested: { text: "a" }, values: [1] });
    strictEqual(tracker.value, next);
    strictEqual(prepared.value, next);
    strictEqual(prepared.base, initial);
    throwsTypeError(() => change.state.count);
  });

  it.effect("keeps a transaction live across await and seals it only at prepare", () =>
    Effect.gen(function* () {
      const tracker = Tracker.make({ left: 0, nested: { right: 0 } });
      const change = Tracker.beginChange(tracker);
      change.state.left = 1;
      yield* Effect.yieldNow;
      change.state.nested.right = 2;
      const prepared = Tracker.prepare(change);
      deepStrictEqual(prepared.value, { left: 1, nested: { right: 2 } });
      throwsTypeError(() => {
        change.state.left = 3;
      }, /settled/);
      Tracker.adopt(tracker, prepared);
    }),
  );

  it.effect(
    "allows Promise resolution to inspect a settled draft without reporting a false failure",
    () =>
      Effect.gen(function* () {
        // @effect-diagnostics-next-line preferSchemaOverJson:off
        const initial = JSON.parse('{"then":"document-value","value":1}') as {
          then: string;
          value: number;
        };
        const tracker = Tracker.make(initial);
        const change = Tracker.beginChange(tracker);
        const draft = change.state;
        strictEqual(draft.then, "document-value");
        draft.value = 2;
        const prepared = Tracker.prepare(change);

        // Native Promise resolution is the behavior under test: it reads `then` from the settled draft.
        const resolved = yield* Effect.promise(() => Promise.resolve(draft));
        strictEqual(resolved, draft);
        strictEqual(resolved.then, undefined);
        throwsTypeError(() => resolved.value, /settled/);

        Tracker.adopt(tracker, prepared);
        strictEqual(tracker.value.then, "document-value");
        strictEqual(tracker.value.value, 2);

        const largeTracker = Tracker.make({
          rows: Array.from({ length: 4_100 }, (_, value) => ({ value })),
        });
        const largeChange = Tracker.beginChange(largeTracker);
        const largeDraft = largeChange.state;
        for (const row of largeDraft.rows) assertTrue(row.value >= 0);
        largeDraft.rows[0]!.value = -1;
        const largePrepared = Tracker.prepare(largeChange);
        strictEqual(yield* Effect.promise(() => Promise.resolve(largeDraft)), largeDraft);
        throwsTypeError(() => largeDraft.rows, /settled/);
        Tracker.adopt(largeTracker, largePrepared);
      }),
  );

  it("aborts idempotently and revokes held descendants", () => {
    const tracker = Tracker.make({ child: { value: 1 } });
    const change = Tracker.beginChange(tracker);
    const child = change.state.child;
    child.value = 2;
    Tracker.abort(change);
    Tracker.abort(change);
    strictEqual(tracker.value.child.value, 1);
    throwsTypeError(() => child.value);
    throwsMessage(() => Tracker.prepare(change), /settled/);
  });

  it("allows competing contexts and invalidates loser views", () => {
    const tracker = Tracker.make({ value: 0 });
    const first = Tracker.beginChange(tracker);
    const second = Tracker.beginChange(tracker);
    first.state.value = 1;
    second.state.value = 2;
    const firstPrepared = Tracker.prepare(first);
    const secondPrepared = Tracker.prepare(second);
    const held = secondPrepared.value;
    Tracker.adopt(tracker, firstPrepared);
    strictEqual(tracker.value.value, 1);
    strictEqual(held.value, 2);
    throwsMessage(() => Tracker.adopt(tracker, secondPrepared), /stale/);
    throwsMessage(() => Tracker.adopt(tracker, firstPrepared), /already been used/);
  });

  it("invalidates competing open overlays without changing their base revision", () => {
    const initial = { rows: [{ value: 0 }, { value: 1 }] };
    const tracker = Tracker.make(initial);
    const staleChange = Tracker.beginChange(tracker);
    const held = staleChange.state.rows[1]!;
    held.value = 2;

    const winner = Tracker.beginChange(tracker);
    winner.state.rows[0]!.value = 3;
    const prepared = Tracker.prepare(winner);
    Tracker.adopt(tracker, prepared);

    deepStrictEqual(initial, { rows: [{ value: 0 }, { value: 1 }] });
    deepStrictEqual(tracker.value, { rows: [{ value: 3 }, { value: 1 }] });
    throwsTypeError(() => held.value, /settled/);
    throwsMessage(() => Tracker.prepare(staleChange), /settled/);
    Tracker.abort(staleChange);
  });

  it("adopts object edits as ordinary fast-layout immutable revisions", () => {
    const initial = { first: 1, second: 2 } as { first: number; second?: number; third?: number };
    const tracker = Tracker.make(initial);
    const change = Tracker.beginChange(tracker);
    change.state.first = 3;
    delete change.state.second;
    change.state.third = 4;
    const prepared = Tracker.prepare(change);
    Tracker.adopt(tracker, prepared);
    assertTrue(tracker.value !== initial);
    deepStrictEqual(initial, { first: 1, second: 2 });
    deepStrictEqual(tracker.value, { first: 3, third: 4 });
    deepStrictEqual(Object.getOwnPropertyDescriptor(tracker.value, "third"), {
      value: 4,
      writable: true,
      enumerable: true,
      configurable: true,
    });
  });

  it("supports adopt-then-publish ordering and retained publication reads", () => {
    const tracker = Tracker.make({ value: 0, nested: { count: 0 } });
    const change = Tracker.beginChange(tracker);
    change.state.value = 1;
    change.state.nested.count = 1;
    const prepared = Tracker.prepare(change);
    const operations = prepared.ops;
    Tracker.adopt(tracker, prepared);
    // Matches services/state.ts: adopt first, then inspect ops and publish prepared.value.
    strictEqual(prepared.ops, operations);
    const published = prepared.ops.length === 0 ? undefined : prepared.value;
    strictEqual(operations.length, 2);
    assertTrue(published !== undefined);
    strictEqual(published, tracker.value);
    deepStrictEqual(clone(published), { value: 1, nested: { count: 1 } });

    const next = Tracker.beginChange(tracker);
    next.state.value = 2;
    const nextPrepared = Tracker.prepare(next);
    Tracker.adopt(tracker, nextPrepared);
    // Retained immutable publications remain at their original revision.
    assertTrue(published !== tracker.value);
    deepStrictEqual(prepared.value, { value: 1, nested: { count: 1 } });
    deepStrictEqual(tracker.value, { value: 2, nested: { count: 1 } });
  });

  it("keeps replacement base/value structurally compatible and readable after adoption", () => {
    const original = { value: 0 };
    const replacement = { value: 1 };
    const tracker = Tracker.make(original);
    const prepared = Tracker.prepareReplace(tracker, replacement);
    strictEqual(prepared.base, original);
    strictEqual(prepared.value, replacement);
    Tracker.adopt(tracker, prepared);
    strictEqual(prepared.base, original);
    strictEqual(prepared.value, replacement);
    strictEqual(prepared.value, tracker.value);
  });

  it("keeps aborted and stale materialized candidates readable", () => {
    const tracker = Tracker.make({ value: 0 });
    const abortedChange = Tracker.beginChange(tracker);
    abortedChange.state.value = 1;
    const aborted = Tracker.prepare(abortedChange);
    const abortedView = aborted.value;
    Tracker.abort(aborted);
    strictEqual(abortedView.value, 1);
    strictEqual(aborted.value.value, 1);

    const loserChange = Tracker.beginChange(tracker);
    loserChange.state.value = 2;
    const loser = Tracker.prepare(loserChange);
    const loserView = loser.value;
    const winner = Tracker.prepareReplace(tracker, { value: 3 });
    Tracker.adopt(tracker, winner);
    strictEqual(loserView.value, 2);
    strictEqual(loser.value.value, 2);
  });

  it("lets a settled Change abort its prepared result without retaining its context", () => {
    const tracker = Tracker.make({ value: 0, nested: { value: 1 } });
    const change = Tracker.beginChange(tracker);
    change.state.value = 1;
    const prepared = Tracker.prepare(change);
    const operations = prepared.ops;
    Tracker.abort(change);
    Tracker.abort(change);
    strictEqual(prepared.ops, operations);
    deepStrictEqual(prepared.value, { value: 1, nested: { value: 1 } });
    throwsMessage(() => Tracker.adopt(tracker, prepared), /aborted/);
  });

  it("rejects foreign and aborted prepared values", () => {
    const first = Tracker.make({ value: 0 });
    const second = Tracker.make({ value: 0 });
    const prepared = Tracker.prepareReplace(first, { value: 1 });
    throwsMessage(() => Tracker.adopt(second, prepared), /different tracker/);
    Tracker.abort(prepared);
    Tracker.abort(prepared);
    throwsMessage(() => Tracker.adopt(first, prepared), /aborted/);
  });

  it("reads deleted own properties as absent", () => {
    Object.defineProperty(Object.prototype, "trackerInherited", {
      value: 7,
      writable: true,
      configurable: true,
    });
    try {
      const tracker = Tracker.make({ value: 1 } as { value?: number; trackerInherited?: number });
      const change = Tracker.beginChange(tracker);
      delete change.state.value;
      change.state.trackerInherited = 1;
      delete change.state.trackerInherited;
      strictEqual(change.state.value, undefined);
      strictEqual(change.state.trackerInherited, undefined);
      const prepared = Tracker.prepare(change);
      strictEqual(prepared.value.value, undefined);
      // Plain committed values expose inherited properties through ordinary
      // JavaScript reads; strict-JSON membership remains own-properties only.
      strictEqual(prepared.value.trackerInherited, 7);
      Tracker.adopt(tracker, prepared);
      deepStrictEqual(Object.keys(tracker.value), []);
    } finally {
      delete (Object.prototype as Record<string, unknown>).trackerInherited;
    }
  });

  it("detaches old handles for deeply equal object and array assignments before normalizing", () => {
    const initial = {
      object: { nested: { value: 1 } },
      array: [{ value: 1 }],
      rows: [{ value: 1 }],
    };
    const tracker = Tracker.make(initial);
    const change = Tracker.beginChange(tracker);
    const oldObject = change.state.object;
    const oldArray = change.state.array;
    const oldRow = change.state.rows[0]!;
    change.state.object = { nested: { value: 1 } };
    change.state.array = [{ value: 1 }];
    change.state.rows[0] = { value: 1 };
    assertTrue(change.state.object !== oldObject);
    assertTrue(change.state.array !== oldArray);
    assertTrue(change.state.rows[0] !== oldRow);
    oldObject.nested.value = 9;
    oldArray[0]!.value = 9;
    oldRow.value = 9;
    deepStrictEqual(change.state, {
      object: { nested: { value: 1 } },
      array: [{ value: 1 }],
      rows: [{ value: 1 }],
    });
    const prepared = Tracker.prepare(change);
    deepStrictEqual(prepared.ops, []);
    Tracker.adopt(tracker, prepared);
    strictEqual(tracker.value, initial);
    strictEqual(tracker.value.object, initial.object);
    strictEqual(tracker.value.array, initial.array);
    strictEqual(tracker.value.rows[0], initial.rows[0]);
  });

  it("normalizes a deeply equal replacement without changing committed identity", () => {
    const initial = { nested: { value: 1 }, rows: [1, 2, 3] };
    const tracker = Tracker.make(initial);
    const prepared = Tracker.prepareReplace(tracker, { nested: { value: 1 }, rows: [1, 2, 3] });
    deepStrictEqual(prepared.ops, []);
    Tracker.adopt(tracker, prepared);
    strictEqual(tracker.value, initial);
  });

  it("takes O(1) immutable ownership for replacement and its operation payload", () => {
    const tracker = Tracker.make({ value: 0, rows: [] as Array<{ value: number }> });
    const replacement = { value: 1, rows: [{ value: 2 }] };
    const prepared = Tracker.prepareReplace(tracker, replacement);
    strictEqual(prepared.base, tracker.value);
    strictEqual(prepared.value, replacement);
    const operations = prepared.ops;
    deepStrictEqual(operations, [["r", replacement]]);
    strictEqual((operations[0] as unknown as readonly ["r", typeof replacement])[1], replacement);
    Tracker.adopt(tracker, prepared);
    strictEqual(tracker.value, replacement);
  });
});

/** Runs `body` with `WeakRef` construction counted; returns the count. */
function countWeakRefs(body: () => void): number {
  const original = globalThis.WeakRef;
  let created = 0;
  globalThis.WeakRef = class<T extends WeakKey> extends original<T> {
    constructor(target: T) {
      super(target);
      created += 1;
    }
  };
  try {
    body();
  } finally {
    globalThis.WeakRef = original;
  }
  return created;
}

describe("delta tracker competing changes", () => {
  it("tracks one change at a time without a WeakRef", () => {
    const tracker = Tracker.make({ value: 0, child: { rows: [0] } });
    const created = countWeakRefs(() => {
      for (let index = 1; index <= 100; index++) {
        const change = Tracker.beginChange(tracker);
        change.state.value = index;
        change.state.child.rows.push(index);
        change.state.child = { rows: [index] };
        Tracker.adopt(tracker, Tracker.prepare(change));
        Tracker.abort(Tracker.beginChange(tracker));
        const aborted = Tracker.beginChange(tracker);
        aborted.state.value = -1;
        Tracker.abort(Tracker.prepare(aborted));
        Tracker.adopt(
          tracker,
          Tracker.prepareReplace(tracker, { value: index, child: { rows: [index] } }),
        );
      }
    });
    strictEqual(created, 0);
    deepStrictEqual(tracker.value, { value: 100, child: { rows: [100] } });
    strictEqual(tracker.revision, 200);
  });

  it("makes the older of two open drafts stale when the newer is adopted", () => {
    const tracker = Tracker.make({ value: 0, child: { value: 0 } });
    const older = Tracker.beginChange(tracker);
    const held = older.state.child;
    held.value = 1;
    const newer = Tracker.beginChange(tracker);
    newer.state.value = 2;
    Tracker.adopt(tracker, Tracker.prepare(newer));
    deepStrictEqual(tracker.value, { value: 2, child: { value: 0 } });
    throwsMessage(() => held.value, /settled/);
    throwsMessage(() => older.state, /settled/);
    throwsMessage(() => Tracker.prepare(older), /settled/);
    Tracker.abort(older);
  });

  it("makes the newer of two open drafts stale when the older is adopted", () => {
    const tracker = Tracker.make({ value: 0, child: { value: 0 } });
    const older = Tracker.beginChange(tracker);
    older.state.value = 1;
    const newer = Tracker.beginChange(tracker);
    const held = newer.state.child;
    held.value = 2;
    Tracker.adopt(tracker, Tracker.prepare(older));
    deepStrictEqual(tracker.value, { value: 1, child: { value: 0 } });
    throwsMessage(() => held.value, /settled/);
    throwsMessage(() => Tracker.prepare(newer), /settled/);
    // The tracker moved on: the next change is tracked as the only one again.
    const next = Tracker.beginChange(tracker);
    next.state.value = 3;
    Tracker.adopt(tracker, Tracker.prepare(next));
    deepStrictEqual(tracker.value, { value: 3, child: { value: 0 } });
  });

  it("makes every other of three drafts stale, open or prepared, whichever is adopted", () => {
    for (const winner of [0, 1, 2]) {
      const tracker = Tracker.make({ value: 0, rows: [{ value: 0 }] });
      const changes = [
        Tracker.beginChange(tracker),
        Tracker.beginChange(tracker),
        Tracker.beginChange(tracker),
      ];
      const held = changes.map((change) => change.state.rows[0]!);
      held.forEach((row, index) => {
        row.value = index + 1;
      });
      const prepared = Tracker.prepare(changes[(winner + 1) % 3]!);
      Tracker.adopt(tracker, Tracker.prepare(changes[winner]!));
      deepStrictEqual(tracker.value, { value: 0, rows: [{ value: winner + 1 }] });
      throwsMessage(() => Tracker.adopt(tracker, prepared), /stale/);
      const open = changes[(winner + 2) % 3]!;
      throwsMessage(() => held[(winner + 2) % 3]!.value, /settled/);
      throwsMessage(() => Tracker.prepare(open), /settled/);
      deepStrictEqual(prepared.value, { value: 0, rows: [{ value: ((winner + 1) % 3) + 1 }] });
    }
  });

  it("keeps an abandoned draft a competitor of the next change, through one WeakRef", () => {
    const tracker = Tracker.make({ value: 0, child: { value: 0 } });
    let abandonedChild: { value: number } | undefined;
    let abandoned: Tracker.Change<{ value: number; child: { value: number } }> | undefined;
    const created = countWeakRefs(() => {
      abandoned = Tracker.beginChange(tracker);
      abandonedChild = abandoned.state.child;
      abandonedChild.value = 1;
      const next = Tracker.beginChange(tracker);
      next.state.value = 2;
      Tracker.adopt(tracker, Tracker.prepare(next));
      const after = Tracker.beginChange(tracker);
      after.state.value = 3;
      Tracker.adopt(tracker, Tracker.prepare(after));
    });
    strictEqual(created, 1);
    deepStrictEqual(tracker.value, { value: 3, child: { value: 0 } });
    throwsMessage(() => abandonedChild!.value, /settled/);
    throwsMessage(() => Tracker.prepare(abandoned!), /settled/);
  });
});

describe("delta tracker policy view", () => {
  it("supports native reads, descriptors, keys, iteration, map, and JSON", () => {
    const tracker = Tracker.make({
      values: [1, 2, 3],
      object: { a: 1 } as { a: number; b?: number },
    });
    const change = Tracker.beginChange(tracker);
    change.state.values.splice(1, 1, 4, 5);
    change.state.object.b = 2;
    assertTrue(Array.isArray(change.state.values));
    strictEqual(change.state.values.length, 4);
    deepStrictEqual([...change.state.values], [1, 4, 5, 3]);
    deepStrictEqual(
      change.state.values.map((value) => value * 2),
      [2, 8, 10, 6],
    );
    deepStrictEqual(Object.keys(change.state.values), ["0", "1", "2", "3"]);
    strictEqual(Object.getOwnPropertyDescriptor(change.state.values, "2")?.value, 5);
    deepStrictEqual(Object.keys(change.state.object), ["a", "b"]);
    deepStrictEqual(JSON.parse(JSON.stringify(change.state)), {
      values: [1, 4, 5, 3],
      object: { a: 1, b: 2 },
    });
    Tracker.abort(change);
  });

  it("keeps document keys distinct from object proxy target fields", () => {
    const tracker = Tracker.make({
      object: {
        context: 1,
        base: 2,
        parent: 3,
        dirty: 4,
        target: 5,
        proxy: 6,
      },
    });
    const change = Tracker.beginChange(tracker);
    change.state.object.context = 7;
    change.state.object.proxy = 8;
    deepStrictEqual(Object.keys(change.state.object), [
      "context",
      "base",
      "parent",
      "dirty",
      "target",
      "proxy",
    ]);
    const prepared = Tracker.prepare(change);
    deepStrictEqual(prepared.value.object, {
      context: 7,
      base: 2,
      parent: 3,
      dirty: 4,
      target: 5,
      proxy: 8,
    });
    Tracker.adopt(tracker, prepared);
  });

  it("keeps native coercion side effects and string operation forms", () => {
    const tracker = Tracker.make({ text: "abcdefgh", values: [1, 2, 3], marker: 0 });
    const change = Tracker.beginChange(tracker);
    change.state.text = `${change.state.text.slice(3)}xyz`;
    change.state.values.splice(
      {
        valueOf() {
          change.state.marker = 1;
          return 1;
        },
      } as unknown as number,
      1,
      4,
    );
    const prepared = Tracker.prepare(change);
    deepStrictEqual(prepared.value, { text: "defghxyz", values: [1, 4, 3], marker: 1 });
    // `toContainEqual`: chai's `deepInclude` checks deep membership in an array.
    assert.deepInclude(prepared.ops, ["t", ["text"], 3]);
    assert.deepInclude(prepared.ops, ["a", ["text"], "xyz"]);
    deepStrictEqual(
      replay(
        tracker.value,
        Wire.decodeUnsafe(Wire.makeDecoder(), Wire.encode(Wire.makeEncoder(), prepared.ops)),
      ),
      prepared.value,
    );
    Tracker.adopt(tracker, prepared);
  });

  it("emits deletion and supports root arrays", () => {
    const objectTracker = Tracker.make({ keep: 1, remove: 2 } as { keep: number; remove?: number });
    settle(objectTracker, (draft) => {
      delete draft.remove;
    });
    deepStrictEqual(objectTracker.value, { keep: 1 });

    const arrayTracker = Tracker.make([1, 2, 3]);
    settle(arrayTracker, (draft) => {
      draft.reverse();
      draft.push(4);
    });
    deepStrictEqual(arrayTracker.value, [3, 2, 1, 4]);
  });

  it("preserves native object key order after delete and re-add", () => {
    const tracker = Tracker.make({ first: 1, second: 2 } as { first?: number; second: number });
    const change = Tracker.beginChange(tracker);
    delete change.state.first;
    change.state.first = 1;
    deepStrictEqual(Object.keys(change.state), ["second", "first"]);
    const prepared = Tracker.prepare(change);
    Tracker.adopt(tracker, prepared);
    deepStrictEqual(Object.keys(tracker.value), ["second", "first"]);
  });

  it("orders new integer-like object keys before strings for recipe-visible reads", () => {
    const tracker = Tracker.make({ object: { label: "x" } as Record<string, string>, first: "" });
    const base = clone(tracker.value);
    const change = Tracker.beginChange(tracker);
    change.state.object["2"] = "two";
    change.state.object["1"] = "one";
    deepStrictEqual(Object.keys(change.state.object), ["1", "2", "label"]);
    change.state.first = Object.keys(change.state.object)[0]!;
    const prepared = Tracker.prepare(change);
    strictEqual(prepared.value.first, "1");
    deepStrictEqual(Object.keys(prepared.value.object), ["1", "2", "label"]);
    deepStrictEqual(replay(base, prepared.ops), prepared.value);
    Tracker.adopt(tracker, prepared);
    strictEqual(tracker.value.first, "1");
    deepStrictEqual(Object.keys(tracker.value.object), ["1", "2", "label"]);
  });

  it("normalizes deeply equal container assignments before direct candidate materialization", () => {
    const initial = { child: { a: 1, b: 2 }, count: 0 };
    const tracker = Tracker.make(initial);
    const change = Tracker.beginChange(tracker);
    change.state.child = { b: 2, a: 1 };
    change.state.count = 1;
    const prepared = Tracker.prepare(change);
    const replayed = replay(initial, prepared.ops);
    deepStrictEqual(prepared.ops, [["s", ["count"], 1]]);
    deepStrictEqual(Object.keys(prepared.value.child), ["a", "b"]);
    deepStrictEqual(Object.keys(replayed.child), ["a", "b"]);
    deepStrictEqual(prepared.value, replayed);
    Tracker.adopt(tracker, prepared);
  });

  it("keeps native integer and string ordering across deletion and re-addition", () => {
    const tracker = Tracker.make({
      object: { 1: "one", 2: "two", first: "a", second: "b" } as Record<string, string>,
    });
    const change = Tracker.beginChange(tracker);
    delete change.state.object["2"];
    change.state.object["2"] = "two";
    delete change.state.object.first;
    change.state.object.first = "a";
    change.state.object["3"] = "three";
    change.state.object["0"] = "zero";
    deepStrictEqual(Object.keys(change.state.object), ["0", "1", "2", "3", "second", "first"]);
    const prepared = Tracker.prepare(change);
    deepStrictEqual(Object.keys(prepared.value.object), ["0", "1", "2", "3", "second", "first"]);
    Tracker.adopt(tracker, prepared);
    deepStrictEqual(Object.keys(tracker.value.object), ["0", "1", "2", "3", "second", "first"]);
  });

  it("orders integer-like keys on null-prototype objects", () => {
    const object = Object.create(null) as Record<string, string>;
    object.label = "x";
    const tracker = Tracker.make({ object });
    const base = clone(tracker.value);
    const change = Tracker.beginChange(tracker);
    change.state.object["2"] = "two";
    change.state.object["1"] = "one";
    deepStrictEqual(Object.keys(change.state.object), ["1", "2", "label"]);
    const prepared = Tracker.prepare(change);
    strictEqual(Object.getPrototypeOf(prepared.value.object), null);
    deepStrictEqual(Object.keys(prepared.value.object), ["1", "2", "label"]);
    // `toEqual` is prototype-insensitive: the replayed (JSON-cloned) base is a plain object while the
    // candidate keeps its null prototype, so use chai's `deepEqual` rather than Node's `deepStrictEqual`.
    assert.deepEqual(replay(base, prepared.ops), prepared.value);
    Tracker.adopt(tracker, prepared);
    strictEqual(Object.getPrototypeOf(tracker.value.object), null);
    deepStrictEqual(Object.keys(tracker.value.object), ["1", "2", "label"]);
  });

  it("forwards borrowed mutators to ordinary generic array receivers", () => {
    const tracker = Tracker.make({ values: [1, 2, 3] });
    const change = Tracker.beginChange(tracker);
    const receiver = [3, 1, 2];
    const push = change.state.values.push;
    const splice = change.state.values.splice;
    const sort = change.state.values.sort;
    const fill = change.state.values.fill;
    const copyWithin = change.state.values.copyWithin;
    strictEqual(Reflect.apply(push, receiver, [4]), 4);
    deepStrictEqual(Reflect.apply(splice, receiver, [1, 1, 5]), [1]);
    strictEqual(
      Reflect.apply(sort, receiver, [(left: number, right: number) => left - right]),
      receiver,
    );
    strictEqual(Reflect.apply(fill, receiver, [7, 1, 2]), receiver);
    strictEqual(Reflect.apply(copyWithin, receiver, [2, 0, 1]), receiver);
    deepStrictEqual(receiver, [2, 7, 2, 5]);
    deepStrictEqual(change.state.values, [1, 2, 3]);
    Tracker.abort(change);
  });

  it("matches native structural coercion ordering for splice, fill, and copyWithin", () => {
    const compare = (
      mutateNative: (values: Array<number>) => void,
      mutateDraft: (values: Array<number>) => void,
    ): void => {
      const native = [1, 2, 3];
      mutateNative(native);
      const tracker = Tracker.make({ values: [1, 2, 3] });
      settle(tracker, (draft) => mutateDraft(draft.values));
      deepStrictEqual(tracker.value.values, native);
    };
    compare(
      (values) => {
        values.splice(
          {
            valueOf() {
              values.push(4);
              return 1;
            },
          } as unknown as number,
          1,
          9,
        );
      },
      (values) => {
        values.splice(
          {
            valueOf() {
              values.push(4);
              return 1;
            },
          } as unknown as number,
          1,
          9,
        );
      },
    );
    compare(
      (values) => {
        values.fill(7, {
          valueOf() {
            values.push(4);
            return 1;
          },
        } as unknown as number);
      },
      (values) => {
        values.fill(7, {
          valueOf() {
            values.push(4);
            return 1;
          },
        } as unknown as number);
      },
    );
    compare(
      (values) => {
        values.copyWithin(
          {
            valueOf() {
              values.push(4);
              return 1;
            },
          } as unknown as number,
          0,
          2,
        );
      },
      (values) => {
        values.copyWithin(
          {
            valueOf() {
              values.push(4);
              return 1;
            },
          } as unknown as number,
          0,
          2,
        );
      },
    );
  });

  it("does not report inherited methods as own array properties", () => {
    const tracker = Tracker.make({ values: [1] });
    const change = Tracker.beginChange(tracker);
    strictEqual(Object.getOwnPropertyDescriptor(change.state.values, "map"), undefined);
    Tracker.abort(change);
  });

  it("keeps repeated identical writes instead of reverting their pending override", () => {
    const tracker = Tracker.make({ value: 0, values: [0] as Array<number | null> });
    settle(tracker, (draft) => {
      draft.value = 1;
      draft.value = 1;
      draft.values[0] = null;
      draft.values[0] = null;
      draft.values.push(2);
      draft.values[1] = null;
      draft.values[1] = null;
    });
    deepStrictEqual(tracker.value, { value: 1, values: [null, null] });
  });

  it("rejects array index gaps without mutation while allowing replacement and append", () => {
    const tracker = Tracker.make({ values: [1, 2] });
    const change = Tracker.beginChange(tracker);
    throwsTypeError(() => {
      change.state.values[3] = 4;
    }, /holes/);
    deepStrictEqual(change.state.values, [1, 2]);
    deepStrictEqual(tracker.value.values, [1, 2]);

    change.state.values[1] = 9;
    change.state.values[2] = 3;
    const prepared = Tracker.prepare(change);
    deepStrictEqual(prepared.value.values, [1, 9, 3]);
    deepStrictEqual(replay(tracker.value, prepared.ops), prepared.value);
    Tracker.adopt(tracker, prepared);
    deepStrictEqual(tracker.value.values, [1, 9, 3]);
  });

  it("supports optional deletion, explicit null length growth, shrinking, and default sort", () => {
    const tracker = Tracker.make({
      optional: "remove" as string | undefined,
      values: [3, 1, 2] as Array<number | null>,
    });
    settle(tracker, (draft) => {
      draft.optional = undefined;
      draft.values.length = 5;
      deepStrictEqual(draft.values, [3, 1, 2, null, null]);
      draft.values.length = 4;
      draft.values.sort();
      throwsTypeError(() => delete draft.values[0], /holes/);
    });
    deepStrictEqual(tracker.value, { values: [1, 2, 3, null] } as typeof tracker.value);
  });

  it("enumerates wide objects without duplicate keys", () => {
    const tracker = Tracker.make({ values: {} as Record<string, number> });
    const change = Tracker.beginChange(tracker);
    for (let index = 0; index < 20_000; index++) change.state.values[`field${index}`] = index;
    const keys = Object.keys(change.state.values);
    strictEqual(keys.length, 20_000);
    strictEqual(keys[0], "field0");
    strictEqual(keys.at(-1), "field19999");
    Tracker.abort(change);
  });

  it("does not dirty read-only traversals", () => {
    const tracker = Tracker.make({ nested: { rows: [{ value: 1 }] } });
    const change = Tracker.beginChange(tracker);
    strictEqual(change.state.nested.rows[0]!.value, 1);
    const prepared = Tracker.prepare(change);
    deepStrictEqual(prepared.ops, []);
    Tracker.adopt(tracker, prepared);
  });

  it("uses one proxy identity per accessed container", () => {
    const tracker = Tracker.make({ nested: { value: 1 }, rows: [{ value: 2 }] });
    const change = Tracker.beginChange(tracker);
    strictEqual(change.state.nested, change.state.nested);
    strictEqual(change.state.rows, change.state.rows);
    strictEqual(change.state.rows[0], change.state.rows[0]);
    Tracker.abort(change);
  });
});

describe("delta tracker by-value placements", () => {
  it("clones property, index, push, unshift, splice, fill, and copyWithin placements", () => {
    const tracker = Tracker.make({
      property: null as { value: number } | null,
      values: [{ value: 0 }, { value: 1 }, { value: 2 }],
    });
    const external = { value: 5 };
    const change = Tracker.beginChange(tracker);
    change.state.property = external;
    change.state.values[0] = external;
    change.state.values.push(external);
    change.state.values.unshift(external);
    change.state.values.splice(2, 0, external);
    external.value = 99;
    change.state.values.fill(change.state.values[0]!, 1, 3);
    change.state.values.copyWithin(3, 0, 2);
    const prepared = Tracker.prepare(change);
    const candidate = clone(prepared.value);
    deepStrictEqual(candidate.property, { value: 5 });
    deepStrictEqual(candidate.values.slice(0, 5), [
      { value: 5 },
      { value: 5 },
      { value: 5 },
      { value: 5 },
      { value: 5 },
    ]);
    Tracker.adopt(tracker, prepared);
  });

  it("rejects non-strict JSON placements before changing the draft", () => {
    const initial = {
      optional: "remove" as string | undefined,
      payload: null as Schema.Json,
      number: 0,
      values: [1, 2] as Array<Schema.Json>,
    };
    const tracker = Tracker.make(initial);
    const change = Tracker.beginChange(tracker);
    const undefinedValue = undefined as unknown as Schema.Json;

    throwsTypeError(() => {
      change.state.values[0] = undefinedValue;
    }, /strict JSON/);
    throwsTypeError(
      () => change.state.values.push({ staged: true }, undefinedValue),
      /strict JSON/,
    );
    throwsTypeError(() => change.state.values.unshift(undefinedValue), /strict JSON/);
    throwsTypeError(() => change.state.values.splice(1, 0, undefinedValue), /strict JSON/);
    throwsTypeError(() => change.state.values.fill(undefinedValue), /strict JSON/);
    throwsTypeError(() => {
      change.state.payload = { nested: undefined } as unknown as Schema.Json;
    }, /strict JSON/);
    throwsTypeError(() => {
      change.state.payload = { nested: () => 1 } as unknown as Schema.Json;
    }, /strict JSON/);
    throwsTypeError(() => {
      change.state.payload = { nested: Symbol("invalid") } as unknown as Schema.Json;
    }, /strict JSON/);
    throwsTypeError(() => {
      change.state.payload = { nested: 1n } as unknown as Schema.Json;
    }, /strict JSON/);
    throwsTypeError(() => {
      change.state.number = Number.NaN;
    }, /strict JSON/);
    throwsTypeError(() => {
      change.state.number = Number.POSITIVE_INFINITY;
    }, /strict JSON/);
    throwsTypeError(() => {
      // @effect-diagnostics-next-line globalDate:off
      change.state.payload = new Date() as unknown as Schema.Json;
    }, /plain objects/);

    const sparse: Array<Schema.Json> = new Array<Schema.Json>(1);
    throwsTypeError(() => {
      change.state.payload = sparse;
    }, /dense plain arrays/);
    const cyclic: { self?: unknown } = {};
    cyclic.self = cyclic;
    throwsTypeError(() => {
      change.state.payload = cyclic as Schema.Json;
    }, /cycles/);
    let accessorReads = 0;
    const accessor = Object.defineProperty({}, "value", {
      enumerable: true,
      get() {
        accessorReads += 1;
        return 1;
      },
    });
    throwsTypeError(() => {
      change.state.payload = accessor as Schema.Json;
    }, /data properties/);
    strictEqual(accessorReads, 0);

    deepStrictEqual(change.state, initial);
    change.state.optional = undefined;
    const dictionary = Object.create(null) as Record<string, Schema.Json>;
    dictionary.valid = true;
    change.state.payload = dictionary;
    change.state.values.push({ valid: true });
    const prepared = Tracker.prepare(change);
    // `toEqual` is prototype-insensitive: `payload` keeps its null prototype while the expected literal
    // and the replayed (JSON-cloned) value are plain objects, so use chai's `deepEqual`.
    assert.deepEqual<unknown>(prepared.value, {
      payload: { valid: true },
      number: 0,
      values: [1, 2, { valid: true }],
    });
    strictEqual(Object.getPrototypeOf(prepared.value.payload), null);
    assert.deepEqual(replay(initial, prepared.ops), prepared.value);
    Tracker.adopt(tracker, prepared);
  });

  it("expands repeated source aliases into independent placements", () => {
    const tracker = Tracker.make({
      left: null as { nested: { value: number } } | null,
      right: null as { nested: { value: number } } | null,
      rows: [] as Array<{ nested: { value: number } }>,
    });
    const shared = { nested: { value: 1 } };
    const change = Tracker.beginChange(tracker);
    change.state.left = shared;
    change.state.right = shared;
    change.state.rows.push(shared, shared);
    change.state.left.nested.value = 9;
    change.state.rows[0]!.nested.value = 8;
    const prepared = Tracker.prepare(change);
    deepStrictEqual(prepared.value, {
      left: { nested: { value: 9 } },
      right: { nested: { value: 1 } },
      rows: [{ nested: { value: 8 } }, { nested: { value: 1 } }],
    });
    assertTrue(prepared.value.left !== prepared.value.right);
    assertTrue(prepared.value.rows[0] !== prepared.value.rows[1]);
    deepStrictEqual(replay(tracker.value, prepared.ops), prepared.value);
    Tracker.adopt(tracker, prepared);
  });

  it("deep-clones draft-sourced placements within and across revisions", () => {
    const tracker = Tracker.make({
      a: { child: { value: 1 } },
      b: null as { child: { value: number } } | null,
      rows: [{ child: { value: 1 } }, { child: { value: 2 } }],
    });
    const first = Tracker.beginChange(tracker);
    first.state.b = first.state.a;
    first.state.b.child.value = 3;
    first.state.rows[1] = first.state.rows[0]!;
    const firstPrepared = Tracker.prepare(first);
    strictEqual(firstPrepared.value.a.child.value, 1);
    strictEqual(firstPrepared.value.b?.child.value, 3);
    assertTrue(firstPrepared.value.a !== firstPrepared.value.b);
    assertTrue(firstPrepared.value.a.child !== firstPrepared.value.b?.child);
    assertTrue(firstPrepared.value.rows[0] !== firstPrepared.value.rows[1]);
    assertTrue(firstPrepared.value.rows[0]!.child !== firstPrepared.value.rows[1]!.child);
    expectAliasFree(firstPrepared.value);
    Tracker.adopt(tracker, firstPrepared);

    const base = clone(tracker.value);
    const second = Tracker.beginChange(tracker);
    second.state.rows[1]!.child.value = 4;
    const secondPrepared = Tracker.prepare(second);
    deepStrictEqual(
      secondPrepared.value.rows.map((row) => row.child.value),
      [1, 4],
    );
    deepStrictEqual(replay(base, secondPrepared.ops), secondPrepared.value);
    expectAliasFree(secondPrepared.value);
    Tracker.adopt(tracker, secondPrepared);
  });

  it("distinguishes raw committed references from draft references", () => {
    const tracker = Tracker.make({
      source: { value: 1 },
      rawCopy: null as { value: number } | null,
      draftCopy: null as { value: number } | null,
    });
    const raw = tracker.value.source;
    const change = Tracker.beginChange(tracker);
    change.state.source.value = 2;
    change.state.rawCopy = raw;
    change.state.draftCopy = change.state.source;
    const prepared = Tracker.prepare(change);
    deepStrictEqual(prepared.value, {
      source: { value: 2 },
      rawCopy: { value: 1 },
      draftCopy: { value: 2 },
    });
    Tracker.adopt(tracker, prepared);
  });

  it("folds edits to introduced object and array subtrees into placement payloads", () => {
    const tracker = Tracker.make({
      nested: null as { rows: Array<{ value: number }> } | null,
      rows: [] as Array<{ values: Array<number> }>,
    });
    const change = Tracker.beginChange(tracker);
    change.state.nested = { rows: [{ value: 1 }] };
    change.state.nested.rows[0]!.value = 2;
    change.state.nested.rows.push({ value: 3 });
    change.state.rows.push({ values: [1] });
    change.state.rows[0]!.values.push(2);
    const prepared = Tracker.prepare(change);
    deepStrictEqual(prepared.ops, [
      ["s", ["nested"], { rows: [{ value: 2 }, { value: 3 }] }],
      ["p", ["rows"], 0, 0, [{ values: [1, 2] }]],
    ]);
    deepStrictEqual(replay(tracker.value, prepared.ops), prepared.value);
    Tracker.adopt(tracker, prepared);
  });

  it("folds pathological operation counts without payload-cost estimation", () => {
    const wide = Object.fromEntries(
      Array.from({ length: 5_000 }, (_, index) => [`field${index}`, 0]),
    ) as Record<string, number>;
    const wideTracker = Tracker.make(wide);
    const wideBase = clone(wideTracker.value);
    const wideChange = Tracker.beginChange(wideTracker);
    for (let index = 0; index < 5_000; index++) wideChange.state[`field${index}`] = 1;
    const widePrepared = Tracker.prepare(wideChange);
    strictEqual(widePrepared.ops.length, 1);
    strictEqual(widePrepared.ops[0]?.[0], "r");
    deepStrictEqual(replay(wideBase, widePrepared.ops), widePrepared.value);
    Tracker.adopt(wideTracker, widePrepared);

    const sparseTracker = Tracker.make({
      rows: Array.from({ length: 15_000 }, (_, value) => ({ value })),
    });
    const sparseBase = clone(sparseTracker.value);
    const sparseChange = Tracker.beginChange(sparseTracker);
    for (let index = 0; index < 15_000; index += 3)
      sparseChange.state.rows[index]!.value = -index - 1;
    const sparsePrepared = Tracker.prepare(sparseChange);
    strictEqual(sparsePrepared.ops.length, 1);
    strictEqual(sparsePrepared.ops[0]?.[0], "r");
    deepStrictEqual(replay(sparseBase, sparsePrepared.ops), sparsePrepared.value);
    Tracker.adopt(sparseTracker, sparsePrepared);
  }, 30_000);

  it("shares immutable operation placements with the materialized candidate", () => {
    const tracker = Tracker.make({ rows: [] as Array<{ value: number }> });
    const change = Tracker.beginChange(tracker);
    change.state.rows.push({ value: 1 });
    const prepared = Tracker.prepare(change);
    const operations = prepared.ops;
    const pending: Array<unknown> = [...operations];
    while (pending.length > 0) {
      const value = pending.pop();
      if (value === null || typeof value !== "object") continue;
      assertFalse(isProxy(value));
      pending.push(...Object.values(value));
    }
    const splice = operations[0];
    if (splice?.[0] !== "p") throw new Error("expected splice");
    strictEqual(splice[4][0], prepared.value.rows[0]);
    Tracker.adopt(tracker, prepared);
    strictEqual(tracker.value, prepared.value);
  });
});

describe("delta tracker piece arrays", () => {
  it("emits one insert for an append to a long array", () => {
    const tracker = Tracker.make({ values: Array.from({ length: 10_000 }, (_, index) => index) });
    const change = Tracker.beginChange(tracker);
    change.state.values.push(-1);
    const prepared = Tracker.prepare(change);
    deepStrictEqual(prepared.ops, [["p", ["values"], 10_000, 0, [-1]]]);
    Tracker.adopt(tracker, prepared);
    strictEqual(tracker.value.values.at(-1), -1);
    strictEqual(tracker.value.values.length, 10_001);
  });

  it("emits removals from the end backwards, then inserts, when kept elements keep their order", () => {
    const tracker = Tracker.make({ values: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9] });
    const values = settle(tracker, (draft) => {
      draft.values.shift(); // gap before the first kept element
      draft.values.splice(1, 2, 20); // gap between kept elements, with an insert
      draft.values.splice(5, 1); // another gap
      draft.values.pop(); // gap after the last kept element
      draft.values.push(30);
    });
    deepStrictEqual(values.values, [1, 20, 4, 5, 6, 8, 30]);
    const change = Tracker.beginChange(Tracker.make({ values: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9] }));
    change.state.values.shift();
    change.state.values.splice(1, 2, 20);
    change.state.values.splice(5, 1);
    change.state.values.pop();
    change.state.values.push(30);
    deepStrictEqual(Tracker.prepare(change).ops, [
      ["p", ["values"], 9, 1, []],
      ["p", ["values"], 7, 1, []],
      ["p", ["values"], 2, 2, []],
      ["p", ["values"], 0, 1, []],
      ["p", ["values"], 1, 0, [20]],
      ["p", ["values"], 6, 0, [30]],
    ]);
  });

  it("emits a permutation when kept elements change order", () => {
    for (const mutate of [
      (values: Array<number>) => values.reverse(),
      (values: Array<number>) =>
        values.sort((left, right) => (left % 2) - (right % 2) || left - right),
    ]) {
      const tracker = Tracker.make({ values: [0, 1, 2, 3, 4] });
      const expected = [0, 1, 2, 3, 4];
      mutate(expected);
      const change = Tracker.beginChange(tracker);
      mutate(change.state.values);
      const prepared = Tracker.prepare(change);
      assertTrue(prepared.ops.some((operation) => operation[0] === "m"));
      deepStrictEqual(replay(tracker.value, prepared.ops), { values: expected });
    }
  });

  it("keeps adoption independent from mutable detached permutation metadata", () => {
    const tracker = Tracker.make({ values: [3, 1, 2] });
    const change = Tracker.beginChange(tracker);
    change.state.values.sort((left, right) => left - right);
    const prepared = Tracker.prepare(change);
    const permutation = prepared.ops.find((operation) => operation[0] === "m");
    if (permutation?.[0] !== "m") throw new Error("expected permutation");
    // `Delta.Operation` is deeply readonly in effect-durable-harness; the test deliberately mutates the detached metadata.
    (permutation[2] as Array<number>).reverse();
    Tracker.adopt(tracker, prepared);
    deepStrictEqual(tracker.value.values, [1, 2, 3]);
  });

  it("matches native splice with zero or one argument", () => {
    const noArguments = Tracker.make({ values: [1, 2, 3] });
    const noArgumentsChange = Tracker.beginChange(noArguments);
    deepStrictEqual(
      Reflect.apply(noArgumentsChange.state.values.splice, noArgumentsChange.state.values, []),
      [],
    );
    const noArgumentsPrepared = Tracker.prepare(noArgumentsChange);
    deepStrictEqual(noArgumentsPrepared.value.values, [1, 2, 3]);
    deepStrictEqual(noArgumentsPrepared.ops, []);
    Tracker.adopt(noArguments, noArgumentsPrepared);

    const oneArgument = Tracker.make({ values: [1, 2, 3, 4] });
    const oneArgumentChange = Tracker.beginChange(oneArgument);
    deepStrictEqual(oneArgumentChange.state.values.splice(1), [2, 3, 4]);
    const oneArgumentPrepared = Tracker.prepare(oneArgumentChange);
    deepStrictEqual(oneArgumentPrepared.value.values, [1]);
    deepStrictEqual(replay(oneArgument.value, oneArgumentPrepared.ops), oneArgumentPrepared.value);
    Tracker.adopt(oneArgument, oneArgumentPrepared);

    const pastEnd = Tracker.make({ values: [1, 2, 3] });
    const pastEndChange = Tracker.beginChange(pastEnd);
    deepStrictEqual(pastEndChange.state.values.splice(3), []);
    const pastEndPrepared = Tracker.prepare(pastEndChange);
    deepStrictEqual(pastEndPrepared.value.values, [1, 2, 3]);
    Tracker.adopt(pastEnd, pastEndPrepared);
  });

  it("supports all structural mutators and native return values", () => {
    const tracker = Tracker.make({ values: [3, 1, 2] });
    const base = clone(tracker.value);
    const change = Tracker.beginChange(tracker);
    strictEqual(change.state.values.push(4), 4);
    strictEqual(change.state.values.pop(), 4);
    strictEqual(change.state.values.unshift(0), 4);
    strictEqual(change.state.values.shift(), 0);
    deepStrictEqual(change.state.values.splice(1, 1, 5, 4), [1]);
    strictEqual(
      change.state.values.sort((left, right) => left - right),
      change.state.values,
    );
    strictEqual(change.state.values.reverse(), change.state.values);
    change.state.values.fill(9, 1, 3);
    change.state.values.copyWithin(1, 0, 2);
    const prepared = Tracker.prepare(change);
    deepStrictEqual(prepared.value, { values: [5, 5, 9, 2] });
    deepStrictEqual(replay(base, prepared.ops), prepared.value);
    Tracker.adopt(tracker, prepared);
  });

  it("tracks held handles after reindex and suppresses detached writes", () => {
    const tracker = Tracker.make({ values: [{ value: "a" }, { value: "b" }, { value: "c" }] });
    const change = Tracker.beginChange(tracker);
    const held = change.state.values[1]!;
    change.state.values.unshift({ value: "front" });
    held.value = "moved";
    strictEqual(change.state.values[2]!.value, "moved");
    change.state.values.splice(2, 1);
    held.value = "detached";
    const prepared = Tracker.prepare(change);
    deepStrictEqual(prepared.value, {
      values: [{ value: "front" }, { value: "a" }, { value: "c" }],
    });
    deepStrictEqual(replay(tracker.value, prepared.ops), prepared.value);
    Tracker.adopt(tracker, prepared);
  });

  it("replays introduced, moved, then edited array descendants", () => {
    const tracker = Tracker.make({ values: [] as Array<{ id: number; nested: Array<number> }> });
    const base = clone(tracker.value);
    const change = Tracker.beginChange(tracker);
    change.state.values.push({ id: 1, nested: [1] });
    const held = change.state.values[0]!;
    change.state.values.unshift({ id: 0, nested: [] });
    held.nested.push(2);
    change.state.values.reverse();
    held.nested.push(3);
    const prepared = Tracker.prepare(change);
    deepStrictEqual(prepared.value, {
      values: [
        { id: 1, nested: [1, 2, 3] },
        { id: 0, nested: [] },
      ],
    });
    deepStrictEqual(replay(base, prepared.ops), prepared.value);
    Tracker.adopt(tracker, prepared);
  });

  it("combines movement and edits with permutation plus final paths", () => {
    const tracker = Tracker.make({
      values: [
        { rank: 3, edited: 0 },
        { rank: 1, edited: 0 },
        { rank: 2, edited: 0 },
      ],
    });
    const base = clone(tracker.value);
    const change = Tracker.beginChange(tracker);
    const held = change.state.values[0]!;
    change.state.values.sort((left, right) => left.rank - right.rank);
    held.edited = 1;
    const prepared = Tracker.prepare(change);
    deepStrictEqual(prepared.ops[0], ["m", ["values"], [1, 2, 0]]);
    deepStrictEqual(replay(base, prepared.ops), prepared.value);
    Tracker.adopt(tracker, prepared);
  });

  it("keeps comparator writes and structurally reentrant appends", () => {
    const tracker = Tracker.make({
      values: [
        { rank: 2, comparisons: 0 },
        { rank: 1, comparisons: 0 },
      ],
    });
    const base = clone(tracker.value);
    const change = Tracker.beginChange(tracker);
    let appended = false;
    change.state.values.sort((left, right) => {
      left.comparisons++;
      right.comparisons++;
      if (!appended) {
        appended = true;
        change.state.values[0] = { rank: 9, comparisons: 0 };
        change.state.values.push({ rank: 3, comparisons: 0 });
      }
      return left.rank - right.rank;
    });
    const prepared = Tracker.prepare(change);
    deepStrictEqual(
      prepared.value.values.map((value) => value.rank),
      [1, 2, 3],
    );
    deepStrictEqual(
      prepared.value.values.map((value) => value.comparisons),
      [1, 1, 0],
    );
    deepStrictEqual(replay(base, prepared.ops), prepared.value);
    Tracker.adopt(tracker, prepared);
  });

  it("normalizes duplicate entries created by structurally reentrant sort callbacks", () => {
    const tracker = Tracker.make({
      values: [
        { rank: 2, edited: 0, nested: { value: 0 } },
        { rank: 1, edited: 0, nested: { value: 0 } },
      ],
    });
    const base = clone(tracker.value);
    const change = Tracker.beginChange(tracker);
    let inserted = false;
    change.state.values.sort((left, right) => {
      left.edited = left.rank * 10;
      left.nested.value = left.rank * 100;
      right.edited = right.rank * 10;
      right.nested.value = right.rank * 100;
      if (!inserted) {
        inserted = true;
        change.state.values.unshift({ rank: 4, edited: 0, nested: { value: 0 } });
      }
      return left.rank - right.rank;
    });
    const prepared = Tracker.prepare(change);
    deepStrictEqual(
      prepared.value.values.map((value) => value.rank),
      [1, 2, 1],
    );
    deepStrictEqual(
      prepared.value.values.map((value) => value.edited),
      [10, 20, 10],
    );
    deepStrictEqual(
      prepared.value.values.map((value) => value.nested.value),
      [100, 200, 100],
    );
    assertTrue(prepared.value.values[0] !== prepared.value.values[2]);
    assertTrue(prepared.value.values[0]!.nested !== prepared.value.values[2]!.nested);
    deepStrictEqual(replay(base, prepared.ops), prepared.value);
    expectAliasFree(prepared.value);
    Tracker.adopt(tracker, prepared);
  });

  it("folds dense child and direct-index edits into one full-array operation", () => {
    const rowsTracker = Tracker.make({
      rows: Array.from({ length: 1_000 }, (_, value) => ({ value })),
    });
    const rowsBase = clone(rowsTracker.value);
    const rowsChange = Tracker.beginChange(rowsTracker);
    for (const row of rowsChange.state.rows) row.value += 1;
    const rowsPrepared = Tracker.prepare(rowsChange);
    strictEqual(rowsPrepared.ops.length, 1);
    strictEqual(rowsPrepared.ops[0]?.[0], "p");
    deepStrictEqual(replay(rowsBase, rowsPrepared.ops), rowsPrepared.value);
    Tracker.adopt(rowsTracker, rowsPrepared);

    const valuesTracker = Tracker.make({
      values: Array.from({ length: 1_000 }, (_, value) => value),
    });
    const valuesBase = clone(valuesTracker.value);
    const valuesChange = Tracker.beginChange(valuesTracker);
    for (let index = 0; index < 600; index++) valuesChange.state.values[index] = -index - 1;
    const valuesPrepared = Tracker.prepare(valuesChange);
    strictEqual(valuesPrepared.ops.length, 1);
    strictEqual(valuesPrepared.ops[0]?.[0], "p");
    deepStrictEqual(replay(valuesBase, valuesPrepared.ops), valuesPrepared.value);
    Tracker.adopt(valuesTracker, valuesPrepared);
  });

  it("folds only a deeply nested dense array region", () => {
    const tracker = Tracker.make({
      rows: Array.from({ length: 2_000 }, (_, value) => ({ nested: { value } })),
    });
    const base = clone(tracker.value);
    const change = Tracker.beginChange(tracker);
    change.state.rows[0]!.nested.value = -1;
    for (let index = 500; index < 1_000; index++) change.state.rows[index]!.nested.value = -index;
    const prepared = Tracker.prepare(change);
    const splice = prepared.ops.find((operation) => operation[0] === "p");
    deepStrictEqual(splice?.slice(0, 4), ["p", ["rows"], 500, 500]);
    if (splice?.[0] !== "p") throw new Error("expected regional splice");
    strictEqual(splice[4].length, 500);
    strictEqual(prepared.ops.length, 2);
    deepStrictEqual(replay(base, prepared.ops), prepared.value);
    Tracker.adopt(tracker, prepared);
  });

  it("does not cache emission paths for nested reserved-key folds covered by an outer region", () => {
    type Row = { flag: number; special?: { __proto__: { values: Array<number> } } };
    const rows: Array<Row> = Array.from({ length: 400 }, () => ({ flag: 0 }));
    rows[100]!.special = JSON.parse(
      `{"__proto__":{"values":[${Array.from({ length: 400 }, (_, value) => value).join(",")}]}}`,
    ) as NonNullable<Row["special"]>;
    const tracker = Tracker.make({ rows });
    const base = clone(tracker.value);
    const change = Tracker.beginChange(tracker);
    for (let index = 0; index < 256; index++) change.state.rows[index]!.flag = 1;
    for (let index = 0; index < 256; index++) {
      change.state.rows[100]!.special!.__proto__.values[index] = -index - 1;
    }
    const prepared = Tracker.prepare(change);
    strictEqual(prepared.ops.length, 1);
    deepStrictEqual(prepared.ops[0]?.slice(0, 4), ["p", ["rows"], 0, 256]);
    deepStrictEqual(replay(base, prepared.ops), prepared.value);
    Tracker.adopt(tracker, prepared);
  });

  it("suppresses nested structural and leaf operations covered by an outer dense region", () => {
    const tracker = Tracker.make({
      rows: Array.from({ length: 400 }, (_, index) => ({
        flag: 0,
        values: index === 100 ? Array.from({ length: 400 }, (_, value) => ({ value })) : [],
      })),
    });
    const base = clone(tracker.value);
    const change = Tracker.beginChange(tracker);
    for (let index = 0; index < 256; index++) change.state.rows[index]!.flag = 1;
    for (let index = 0; index < 256; index++)
      change.state.rows[100]!.values[index]!.value = -index - 1;
    change.state.rows[100]!.values.push({ value: 999 });
    const prepared = Tracker.prepare(change);
    strictEqual(prepared.ops.length, 1);
    deepStrictEqual(prepared.ops[0]?.slice(0, 4), ["p", ["rows"], 0, 256]);
    strictEqual(prepared.value.rows[100]!.values.length, 401);
    deepStrictEqual(replay(base, prepared.ops), prepared.value);
    Tracker.adopt(tracker, prepared);
  });

  it("emits multiple disjoint dense regions and preserves operations outside their boundaries", () => {
    const tracker = Tracker.make({
      values: Array.from({ length: 1_400 }, (_, value) => ({ value })),
    });
    const base = clone(tracker.value);
    const change = Tracker.beginChange(tracker);
    for (const index of [97, 358, 897, 1_158]) change.state.values[index]!.value = -index - 1;
    for (let index = 100; index < 356; index++) change.state.values[index]!.value = -index - 1;
    for (let index = 900; index < 1_156; index++) change.state.values[index]!.value = -index - 1;
    const prepared = Tracker.prepare(change);
    const splices = prepared.ops.filter((operation) => operation[0] === "p");
    deepStrictEqual(
      splices.map((operation) => operation.slice(0, 4)),
      [
        ["p", ["values"], 100, 256],
        ["p", ["values"], 900, 256],
      ],
    );
    for (const index of [97, 358, 897, 1_158]) {
      // `toContainEqual`: chai's `deepInclude` checks deep membership in an array.
      assert.deepInclude(prepared.ops, ["s", ["values", index, "value"], -index - 1]);
    }
    strictEqual(prepared.ops.length, 6);
    deepStrictEqual(replay(base, prepared.ops), prepared.value);
    Tracker.adopt(tracker, prepared);
  });

  it("normalizes a 20,000-operation queue transaction to two operations", () => {
    const tracker = Tracker.make({
      values: Array.from({ length: 20_000 }, (_, value) => ({ value })),
    });
    const base = clone(tracker.value);
    const change = Tracker.beginChange(tracker);
    for (let index = 0; index < 10_000; index++) {
      change.state.values.shift();
      change.state.values.push({ value: 20_000 + index });
    }
    const prepared = Tracker.prepare(change);
    strictEqual(prepared.ops.length, 2);
    deepStrictEqual(replay(base, prepared.ops), prepared.value);
    Tracker.adopt(tracker, prepared);
  });

  it("handles adversarial fragmentation and resolves held handles without piece scans", () => {
    const size = 20_000;
    const tracker = Tracker.make({
      values: Array.from({ length: size }, (_, value) => ({ value })),
    });
    const expected = clone(tracker.value);
    const base = clone(tracker.value);
    const change = Tracker.beginChange(tracker);
    const held = [1, 1_001, 5_001, 10_001, 15_001, 19_999].map(
      (index) => change.state.values[index]!,
    );
    for (let index = 0; index < size; index += 2) {
      change.state.values.splice(index, 1, { value: -index - 1 });
      expected.values.splice(index, 1, { value: -index - 1 });
    }
    for (const value of held) value.value += 100_000;
    for (const index of [1, 1_001, 5_001, 10_001, 15_001, 19_999])
      expected.values[index]!.value += 100_000;
    const prepared = Tracker.prepare(change);
    deepStrictEqual(prepared.value, expected);
    deepStrictEqual(replay(base, prepared.ops), prepared.value);
    Tracker.adopt(tracker, prepared);
  }, 30_000);

  it("normalizes restored overrides and cancelled structural edits to no operations", () => {
    const tracker = Tracker.make({ values: [1, 2, 3] });
    const change = Tracker.beginChange(tracker);
    change.state.values[1] = 9;
    change.state.values[1] = 2;
    change.state.values.reverse();
    change.state.values.reverse();
    change.state.values.push(4);
    strictEqual(change.state.values.pop(), 4);
    const prepared = Tracker.prepare(change);
    deepStrictEqual(prepared.value, { values: [1, 2, 3] });
    deepStrictEqual(prepared.ops, []);
    Tracker.adopt(tracker, prepared);
  });

  it("handles null sparse overrides without confusing absence", () => {
    const tracker = Tracker.make({
      values: Array.from({ length: 10_000 }, (_, value): number | null => value),
    });
    settle(tracker, (draft) => {
      draft.values[17] = null;
      draft.values[9_000] = null;
    });
    strictEqual(tracker.value.values[17], null);
    strictEqual(tracker.value.values[9_000], null);
  });

  it("supports self-overlapping fill and copyWithin by value", () => {
    const tracker = Tracker.make({ values: [{ n: 0 }, { n: 1 }, { n: 2 }, { n: 3 }] });
    settle(tracker, (draft) => {
      draft.values.copyWithin(1, 0, 3);
      draft.values.fill(draft.values[1]!, 0, 2);
      draft.values[0]!.n = 9;
    });
    deepStrictEqual(
      tracker.value.values.map((value) => value.n),
      [9, 0, 1, 2],
    );
    assertTrue(tracker.value.values[0] !== tracker.value.values[1]);
  });

  it.each(["unshift", "splice"] as const)(
    "inserts 100,000 items with %s",
    (method) => {
      const tracker = Tracker.make({ values: [-1] });
      const base = clone(tracker.value);
      const change = Tracker.beginChange(tracker);
      const items = Array.from({ length: 100_000 }, (_, value) => value);
      if (method === "unshift")
        Reflect.apply(change.state.values.unshift, change.state.values, items);
      else Reflect.apply(change.state.values.splice, change.state.values, [1, 0, ...items]);
      const prepared = Tracker.prepare(change);
      strictEqual(prepared.value.values.length, 100_001);
      strictEqual(prepared.value.values[method === "unshift" ? 99_999 : 100_000], 99_999);
      deepStrictEqual(replay(base, prepared.ops), prepared.value);
      Tracker.adopt(tracker, prepared);
    },
    30_000,
  );
});

describe("delta tracker randomized transactions", () => {
  type Document = {
    values: Array<{ id: number; score: number }>;
    text: string;
    meta: { revision: number; label?: string };
  };
  const random =
    (seed: number): (() => number) =>
    () => {
      seed = (seed + 0x6d2b79f5) | 0;
      let value = Math.imul(seed ^ (seed >>> 15), 1 | seed);
      value = (value + Math.imul(value ^ (value >>> 7), 61 | value)) ^ value;
      return ((value ^ (value >>> 14)) >>> 0) / 4_294_967_296;
    };
  const mutate = (
    document: Document | Tracker.Draft<Document>,
    choice: number,
    value: number,
  ): void => {
    const item = (): { id: number; score: number } => ({ id: value, score: value % 7 });
    switch (choice) {
      case 0:
        document.text += `-${value}`;
        break;
      case 1:
        document.values.push(item());
        break;
      case 2:
        document.values.unshift(item());
        break;
      case 3:
        if (document.values.length > 0) document.values.shift();
        break;
      case 4:
        if (document.values.length > 0) document.values.pop();
        break;
      case 5: {
        const index = document.values.length === 0 ? 0 : value % (document.values.length + 1);
        document.values.splice(index, document.values.length === 0 ? 0 : value % 2, item());
        break;
      }
      case 6:
        document.values.reverse();
        break;
      case 7:
        document.values.sort((left, right) => left.id - right.id);
        break;
      case 8:
        if (document.values.length > 0)
          document.values[value % document.values.length]!.score = value;
        break;
      case 9:
        document.meta.revision++;
        document.meta.label = `r-${value}`;
        break;
      default:
        delete document.meta.label;
    }
  };

  it("matches policy, detached replay, and adopted state after multi-operation transactions", () => {
    for (let seed = 1; seed <= 40; seed++) {
      const rng = random(seed);
      const initial: Document = {
        values: Array.from({ length: 4 }, (_, id) => ({ id, score: 0 })),
        text: "start",
        meta: { revision: 0 },
      };
      const tracker = Tracker.make(initial);
      const expected = clone(initial);
      for (let transaction = 0; transaction < 25; transaction++) {
        const baseRoot = tracker.value;
        const base = clone(baseRoot);
        const change = Tracker.beginChange(tracker);
        for (let operation = 0; operation < 5; operation++) {
          const choice = Math.floor(rng() * 11);
          const value = seed * 10_000 + transaction * 10 + operation;
          mutate(expected, choice, value);
          mutate(change.state, choice, value);
        }
        const prepared = Tracker.prepare(change);
        const policy = clone(prepared.value);
        assertFalse(isProxy(prepared.value));
        deepStrictEqual(
          baseRoot,
          base,
          `prepare changed base seed ${seed} transaction ${transaction}`,
        );
        deepStrictEqual(policy, expected, `policy seed ${seed} transaction ${transaction}`);
        deepStrictEqual(
          replay(base, prepared.ops),
          policy,
          `replay seed ${seed} transaction ${transaction}`,
        );
        Tracker.adopt(tracker, prepared);
        deepStrictEqual(
          baseRoot,
          base,
          `adopt changed base seed ${seed} transaction ${transaction}`,
        );
        strictEqual(tracker.value, prepared.value, `adopt seed ${seed} transaction ${transaction}`);
        deepStrictEqual(tracker.value, policy, `adopt seed ${seed} transaction ${transaction}`);
        expectAliasFree(tracker.value);
      }
    }
  }, 30_000);
});

describe("delta tracker object emission scaling", () => {
  it("orders many reverse-depth object edits through the bounded fallback", () => {
    type DeepNode = { value: number; next?: DeepNode };
    let initial: DeepNode = { value: 0 };
    for (let depth = 0; depth < 512; depth++) initial = { value: 0, next: initial };
    const tracker = Tracker.make(initial);
    const change = Tracker.beginChange(tracker);
    const nodes: Array<Tracker.Draft<DeepNode>> = [];
    let node: Tracker.Draft<DeepNode> | undefined = change.state;
    while (node !== undefined) {
      nodes.push(node);
      node = node.next;
    }
    for (let index = nodes.length - 1; index >= 0; index--) nodes[index]!.value = index + 1;
    const prepared = Tracker.prepare(change);
    strictEqual(prepared.ops.length, nodes.length);
    deepStrictEqual(replay(initial, prepared.ops), prepared.value);
    Tracker.adopt(tracker, prepared);
    expectAliasFree(tracker.value);
  });
});

describe("delta tracker security and storage-style cloning", () => {
  it("defines own properties without invoking inherited setters", () => {
    const tracker = Tracker.make({} as Record<string, Schema.Json>);
    Object.defineProperty(Object.prototype, "trap", {
      set() {
        throw new Error("inherited setter ran");
      },
      configurable: true,
    });
    try {
      settle(tracker, (draft) => {
        draft.trap = 1;
      });
      deepStrictEqual(tracker.value, { trap: 1 });
    } finally {
      delete (Object.prototype as Record<string, unknown>).trap;
    }
  });

  it("handles reserved own keys without prototype pollution", () => {
    const initial = JSON.parse('{"safe":{"__proto__":{"value":1}}}') as {
      safe: { __proto__: { value: number } };
    };
    const tracker = Tracker.make(initial);
    const change = Tracker.beginChange(tracker);
    change.state.safe.__proto__.value = 2;
    const prepared = Tracker.prepare(change);
    strictEqual(prepared.value.safe.__proto__.value, 2);
    deepStrictEqual(replay(tracker.value, prepared.ops), prepared.value);
    Tracker.adopt(tracker, prepared);
    strictEqual(({} as Record<string, unknown>).value, undefined);
  });

  it("supports a MemoryStorage-style recursive clone of Prepared.value", () => {
    const storageClone = <T>(value: T): T => {
      if (value === null || typeof value !== "object") return value;
      if (Array.isArray(value)) return value.map(storageClone) as T;
      const output: Record<string, unknown> = {};
      for (const key of Object.keys(value)) {
        Object.defineProperty(output, key, {
          value: storageClone((value as Record<string, unknown>)[key]),
          writable: true,
          enumerable: true,
          configurable: true,
        });
      }
      return output as T;
    };
    const tracker = Tracker.make({ rows: [{ value: 1 }] });
    const change = Tracker.beginChange(tracker);
    change.state.rows[0]!.value = 2;
    change.state.rows.push({ value: 3 });
    const prepared = Tracker.prepare(change);
    deepStrictEqual(storageClone(prepared.value), { rows: [{ value: 2 }, { value: 3 }] });
    Tracker.adopt(tracker, prepared);
  });
});

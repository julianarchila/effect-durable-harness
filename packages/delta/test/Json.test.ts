// The `copyJson` cases: `@effect-durable-harness/delta` exports only the copy.
import { assert, describe, it } from "@effect/vitest";
import { assertTrue, deepStrictEqual, strictEqual, throws } from "@effect/vitest/utils";
import * as Json from "@effect-durable-harness/delta/Json";

const throwsStrictJson = (thunk: () => void) =>
  throws(thunk, (error) => {
    assertTrue(error instanceof TypeError && /strict JSON/.test(error.message));
  });

describe("Json.copy", () => {
  it("copies strict JSON without retaining aliases", () => {
    const shared = { value: 1 };
    const input = { left: shared, right: shared };
    const copied = Json.copy(input) as typeof input;
    deepStrictEqual(copied, input);
    assert.notStrictEqual(copied, input);
    assert.notStrictEqual(copied.left, shared);
    assert.notStrictEqual(copied.right, shared);
    assert.notStrictEqual(copied.left, copied.right);
  });

  it("optionally omits undefined object properties without normalizing arrays", () => {
    const input = { kept: 1, omitted: undefined, nested: { omitted: undefined, kept: true } };
    throwsStrictJson(() => Json.copy(input));
    deepStrictEqual(Json.copy(input, { omitUndefinedProperties: true }), {
      kept: 1,
      nested: { kept: true },
    });
    throwsStrictJson(() => Json.copy([undefined], { omitUndefinedProperties: true }));
  });

  it("preserves null prototypes and own __proto__ data properties", () => {
    const input = Object.create(null) as Record<string, unknown>;
    Object.defineProperty(input, "__proto__", {
      value: { safe: true },
      enumerable: true,
      writable: true,
      configurable: true,
    });
    const copied = Json.copy(input) as Record<string, unknown>;
    strictEqual(Object.getPrototypeOf(copied), null);
    assertTrue(Object.hasOwn(copied, "__proto__"));
    deepStrictEqual(copied["__proto__"], { safe: true });
  });

  it("rejects cycles and non-strict container properties", () => {
    const cyclic: { self?: unknown } = {};
    cyclic.self = cyclic;
    const sparse: Array<unknown> = [];
    sparse[1] = 1;
    const extra = Object.assign([1], { extra: 2 });
    class ArraySubclass extends Array<unknown> {}
    const subclass = new ArraySubclass(1);
    const accessor = Object.defineProperty({}, "value", { enumerable: true, get: () => 1 });
    const hidden = Object.defineProperty({}, "value", { enumerable: false, value: 1 });
    const symbol = { [Symbol("value")]: 1 };
    for (const invalid of [cyclic, sparse, extra, subclass, accessor, hidden, symbol]) {
      throwsStrictJson(() => Json.copy(invalid));
    }
  });
});

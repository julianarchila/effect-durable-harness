import { assert, describe, it } from "@effect/vitest";
import * as Schema from "effect/Schema";
import { coerce, dropNullOptionals } from "../src/internal/coerce.ts";

// Coercion of tool arguments towards a JSON Schema: the three passes (optional nulls, conversion rules, primitive
// coercion by type), their order where they conflict, and the guarantees around them.

type JsonSchema = Schema.JsonObject;

const object = (
  properties: Schema.JsonObject,
  required: ReadonlyArray<string> = Object.keys(properties),
): JsonSchema => ({ type: "object", properties, required: [...required] });

/** The coerced `value` property of `{ value }` under an object schema with one required property `value`. */
const coerceValue = (schema: Schema.Json, value: Schema.Json): Schema.Json => {
  const coerced = coerce(object({ value: schema }), { value });
  assert.isTrue(Schema.is(Schema.JsonObject)(coerced));
  // SAFETY: checked above.
  return (coerced as Schema.JsonObject).value!;
};

describe("coerce", () => {
  it("never modifies the value", () => {
    const value = { count: "42", tags: "a" };
    const schema = object({
      count: { type: "number" },
      tags: { type: "array", items: { type: "string" } },
    });
    assert.deepStrictEqual(coerce(schema, value), { count: 42, tags: ["a"] });
    assert.deepStrictEqual(value, { count: "42", tags: "a" });
  });

  it("returns the value itself when nothing changes", () => {
    const value = { a: { b: [1, 2] }, c: "x" };
    const schema = object({
      a: object({ b: { type: "array", items: { type: "number" } } }),
      c: { type: "string" },
    });
    assert.strictEqual(coerce(schema, value), value);
  });

  it("copies only the changed paths", () => {
    const value = { a: { n: "1" }, b: { s: "x" } };
    const schema = object({
      a: object({ n: { type: "number" } }),
      b: object({ s: { type: "string" } }),
    });
    const coerced = coerce(schema, value);
    assert.deepStrictEqual(coerced, { a: { n: 1 }, b: { s: "x" } });
    // SAFETY: the root schema is an object, and so is the value.
    assert.strictEqual((coerced as Schema.JsonObject).b, value.b);
  });

  it("leaves values it cannot coerce for the decode to report", () => {
    for (const input of ["abc", "4x", {}, ["1", "2"]]) {
      assert.deepStrictEqual(coerceValue({ type: "integer" }, input), input);
    }
  });
});

describe("pass 1: optional nulls", () => {
  it("drops null in optional non-nullable properties, also nested", () => {
    const schema = object(
      {
        path: { type: "string" },
        offset: { type: "number" },
        nullable: { anyOf: [{ type: "string" }, { type: "null" }] },
        metadata: object({ enabled: { type: "boolean" } }, []),
      },
      ["path", "metadata"],
    );
    assert.deepStrictEqual(
      coerce(schema, {
        path: "file.txt",
        offset: null,
        nullable: null,
        metadata: { enabled: null },
      }),
      { path: "file.txt", nullable: null, metadata: {} },
    );
  });

  it("keeps a null whose property is a $ref (the reference may allow it)", () => {
    const schema = {
      type: "object",
      properties: { value: { $ref: "#/$defs/value" } },
      $defs: { value: { anyOf: [{ type: "number" }, { type: "null" }] } },
    };
    assert.deepStrictEqual(coerce(schema, { value: null }), { value: null });
  });

  it("walks array and tuple items", () => {
    const item = object({ a: { type: "string" }, b: { type: "integer" } }, ["a"]);
    assert.deepStrictEqual(coerce({ type: "array", items: item }, [{ a: "x", b: null }]), [
      { a: "x" },
    ]);
    assert.deepStrictEqual(coerce({ type: "array", prefixItems: [item] }, [{ a: "x", b: null }]), [
      { a: "x" },
    ]);
  });

  it("keeps a null under pattern and additional properties: their keys are not optional properties", () => {
    const schema = { type: "object", additionalProperties: { type: ["integer", "null"] } };
    assert.deepStrictEqual(coerce(schema, { n: null }), { n: null });
  });
});

describe("pass 2: conversion rules", () => {
  it("converts primitives by type", () => {
    assert.strictEqual(coerceValue({ type: "number" }, "42"), 42);
    assert.strictEqual(coerceValue({ type: "number" }, ""), 0);
    assert.strictEqual(coerceValue({ type: "number" }, "true"), 1);
    assert.strictEqual(coerceValue({ type: "integer" }, "3.5"), 3);
    assert.strictEqual(coerceValue({ type: "integer" }, 1.5), 1);
    assert.strictEqual(coerceValue({ type: "boolean" }, "1"), true);
    assert.strictEqual(coerceValue({ type: "boolean" }, "FALSE"), false);
    assert.strictEqual(coerceValue({ type: "null" }, "null"), null);
    assert.strictEqual(coerceValue({ type: "string" }, 42), "42");
  });

  it("wraps a single value into an array and converts the items", () => {
    assert.deepStrictEqual(coerceValue({ type: "array", items: { type: "number" } }, "1"), [1]);
    assert.deepStrictEqual(coerceValue({ type: "array", items: { type: "string" } }, null), [
      "null",
    ]);
  });

  it("converts tuple items in place and never wraps for a tuple", () => {
    const tuple = {
      type: "array",
      prefixItems: [{ type: "number" }, { type: "boolean" }],
      items: false,
    };
    assert.deepStrictEqual(coerceValue(tuple, ["1", "true"]), [1, true]);
    assert.strictEqual(coerceValue(tuple, "1"), "1");
  });

  it("converts const and enum values only when the result is a member", () => {
    assert.strictEqual(coerceValue({ const: 1 }, "1"), 1);
    assert.strictEqual(coerceValue({ const: 1 }, "2"), "2");
    assert.strictEqual(coerceValue({ enum: [1, 2, "hello"] }, "2"), 2);
    assert.strictEqual(coerceValue({ enum: [1, 2, "hello"] }, "world"), "world");
  });

  it("keeps a value one union branch accepts, else takes the first branch whose conversion the union accepts", () => {
    const union = { anyOf: [{ type: "number" }, { type: "null" }] };
    assert.strictEqual(coerceValue(union, null), null);
    assert.strictEqual(coerceValue(union, "42"), 42);
    assert.strictEqual(coerceValue(union, "null"), null);
    assert.strictEqual(
      coerceValue({ oneOf: [{ type: "boolean" }, { type: "number" }] }, "1"),
      true,
    );
  });

  it("converts allOf with each member in turn", () => {
    const schema = {
      allOf: [
        { type: "object", properties: { a: { type: "number" } } },
        { type: "object", properties: { b: { type: "boolean" } } },
      ],
    };
    assert.deepStrictEqual(coerce(schema, { a: "1", b: "true" }), { a: 1, b: true });
  });

  it("follows $ref", () => {
    const schema = {
      type: "object",
      properties: { item: { $ref: "#/$defs/item" } },
      $defs: { item: { type: "object", properties: { id: { type: "integer" } } } },
    };
    assert.deepStrictEqual(coerce(schema, { item: { id: "7" } }), { item: { id: 7 } });
  });

  it("leaves a value under a $ref that names nothing", () => {
    const schema = { type: "object", properties: { item: { $ref: "#/$defs/missing" } } };
    assert.deepStrictEqual(coerce(schema, { item: "7" }), { item: "7" });
  });

  it("converts patternProperties and additionalProperties", () => {
    const schema = {
      type: "object",
      properties: { a: { type: "number" } },
      patternProperties: { "^n_": { type: "number" } },
      additionalProperties: { type: "boolean" },
    };
    assert.deepStrictEqual(coerce(schema, { a: "1", n_x: "2", other: "true" }), {
      a: 1,
      n_x: 2,
      other: true,
    });
  });
});

describe("pass 3: primitive coercion by type", () => {
  it("coerces like Ajv's coerceTypes", () => {
    const cases: ReadonlyArray<readonly [Schema.JsonObject, Schema.Json, Schema.Json]> = [
      [{ type: "number" }, true, 1],
      [{ type: "number" }, null, 0],
      [{ type: "integer" }, "42", 42],
      [{ type: "boolean" }, "true", true],
      [{ type: "boolean" }, 0, false],
      [{ type: "string" }, true, "true"],
      [{ type: "null" }, "", null],
      [{ type: "null" }, 0, null],
      [{ type: "null" }, false, null],
    ];
    for (const [schema, input, expected] of cases) {
      assert.deepStrictEqual(coerceValue(schema, input), expected);
    }
  });

  it("leaves a value that matches one of a type array's types, else takes the first coercible type", () => {
    assert.strictEqual(coerceValue({ type: ["number", "string"] }, "1"), "1");
    assert.strictEqual(coerceValue({ type: ["boolean", "number"] }, "1"), 1);
    assert.strictEqual(coerceValue({ type: ["integer", "null"] }, "5"), 5);
  });

  it("accepts null for nullable array schemas with items", () => {
    assert.strictEqual(
      coerceValue({ type: ["array", "null"], items: { type: "string" } }, null),
      null,
    );
  });

  it("coerces nullable unions when the value matches no branch", () => {
    assert.strictEqual(coerceValue({ anyOf: [{ type: "number" }, { type: "null" }] }, "42"), 42);
    assert.strictEqual(coerceValue({ oneOf: [{ type: "number" }, { type: "null" }] }, null), null);
  });

  it("coerces tuple items by type", () => {
    const schema = {
      type: "array",
      prefixItems: [{ type: ["integer", "null"] }, { type: ["boolean", "null"] }],
    };
    assert.deepStrictEqual(coerce(schema, ["5", "true"]), [5, true]);
  });
});

describe("where the passes conflict, the conversion rules win", () => {
  it("turns null into 'null' for a required string (pass 3 alone would give '')", () => {
    assert.strictEqual(coerceValue({ type: "string" }, null), "null");
  });

  it("turns '1' and '0' into booleans (pass 3 takes only 'true', 'false', 1 and 0)", () => {
    assert.strictEqual(coerceValue({ type: "boolean" }, "0"), false);
  });

  it("truncates '42.1' for an integer (pass 3 rejects it)", () => {
    assert.strictEqual(coerceValue({ type: "integer" }, "42.1"), 42);
  });

  it("drops an optional null before converting it (pass 2 alone would give 'null')", () => {
    assert.deepStrictEqual(coerce(object({ name: { type: "string" } }, []), { name: null }), {});
  });
});

describe("recursive schemas", () => {
  const tree = {
    type: "object",
    properties: { t: { $ref: "#/$defs/Tree" } },
    $defs: {
      Tree: { anyOf: [{ type: "string" }, { type: "array", items: { $ref: "#/$defs/Tree" } }] },
    },
  };

  it("convert through a recursive union by its first accepting branch", () => {
    assert.deepStrictEqual(coerce(tree, { t: ["a", ["b"]] }), { t: ["a", ["b"]] });
    assert.deepStrictEqual(coerce(tree, { t: 5 }), { t: "5" });
    assert.deepStrictEqual(coerce(tree, { t: [5] }), { t: ["5"] });
  });

  it("leave a value a recursive schema cannot convert as it is", () => {
    assert.deepStrictEqual(coerce(tree, { t: { a: 1 } }), { t: { a: 1 } });
  });

  it("convert a scalar-or-array schema that refers to itself", () => {
    const schema = object({
      value: {
        anyOf: [{ type: "number" }, { type: "array", items: { $ref: "#/properties/value" } }],
      },
    });
    assert.deepStrictEqual(coerce(schema, { value: "42" }), { value: 42 });
    assert.deepStrictEqual(coerce(schema, { value: ["1", ["2"]] }), { value: [1, [2]] });
    assert.deepStrictEqual(coerce(schema, { value: {} }), { value: {} });
  });

  it("stop a root that refers to itself, and leave the input unchanged", () => {
    const schema = { anyOf: [{ type: "number" }, { type: "array", items: { $ref: "#" } }] };
    for (let i = 0; i < 20; i++) {
      assert.deepStrictEqual(coerce(schema, {}), {});
      assert.strictEqual(coerce(schema, "42"), 42);
      const input = [1, "2", ["3"]];
      assert.deepStrictEqual(coerce(schema, input), [1, 2, [3]]);
      assert.deepStrictEqual(input, [1, "2", ["3"]]);
    }
  });

  it("convert a deep value that every branch of a recursive union rejects, in linear time", () => {
    // Each object branch converts every child: without memoizing per call this is 2^depth work.
    const S = {
      anyOf: [
        { type: "object", additionalProperties: { $ref: "#/$defs/S" }, required: ["a"] },
        { type: "object", additionalProperties: { $ref: "#/$defs/S" }, required: ["b"] },
      ],
    };
    const schema = { type: "object", properties: { s: { $ref: "#/$defs/S" } }, $defs: { S } };
    let value: Schema.Json = { x: 1 };
    for (let i = 0; i < 40; i++) value = { c: value };
    assert.deepStrictEqual(coerce(schema, { s: value }), { s: value });
  });
});

describe("member keywords across the passes", () => {
  it("convert the properties a union node declares besides its branches", () => {
    const schema = {
      type: "object",
      properties: { a: { type: "integer" }, b: { type: "integer" } },
      anyOf: [{ required: ["a"] }, { required: ["b"] }],
    };
    assert.deepStrictEqual(coerce(schema, { a: "3.5" }), { a: 3 });
  });

  for (const keyword of ["anyOf", "oneOf"]) {
    it(`check ${keyword} candidates after the node's own properties convert`, () => {
      const schema = {
        type: "object",
        [keyword]: [
          {
            type: "object",
            properties: { a: { type: "array", items: { type: "integer" } } },
            required: ["a"],
          },
          { type: "object", properties: { b: { type: "boolean" } }, required: ["b"] },
        ],
        properties: { c: { type: "integer" } },
        required: ["c"],
      };
      const input = { a: "3", c: "2" };
      assert.deepStrictEqual(coerce(schema, input), { a: [3], c: 2 });
      assert.deepStrictEqual(input, { a: "3", c: "2" });
    });
  }
});

describe("the JSON Schema generated from a Schema", () => {
  it("coerces what the Schema then decodes", () => {
    const parameters = Schema.Struct({
      count: Schema.Number,
      limit: Schema.optionalKey(Schema.Int),
      tags: Schema.Array(Schema.String),
      flag: Schema.Boolean,
      mode: Schema.Literals(["fast", "slow"]),
      note: Schema.optionalKey(Schema.String),
      either: Schema.Union([Schema.Number, Schema.Boolean]),
    });
    const document = Schema.toJsonSchemaDocument(parameters);
    // SAFETY: a generated JSON Schema document is JSON.
    const jsonSchema = document.schema as Schema.JsonObject;
    const coerced = coerce(jsonSchema, {
      count: "42",
      limit: "3",
      tags: "a",
      flag: "true",
      mode: "fast",
      note: null,
      either: "7",
    });
    assert.deepStrictEqual(Schema.decodeUnknownSync(parameters)(coerced), {
      count: 42,
      limit: 3,
      tags: ["a"],
      flag: true,
      mode: "fast",
      either: 7,
    });
  });
});

describe("dropNullOptionals", () => {
  it("drops every null in a property that is not required, whether or not its schema accepts null", () => {
    const schema = object(
      {
        a: { anyOf: [{ type: "number" }, { type: "null" }] },
        b: { type: "null" },
        c: { anyOf: [{ type: "string" }, { type: "null" }] },
      },
      ["c"],
    );
    assert.deepStrictEqual(dropNullOptionals(schema, { a: null, b: null, c: null }), { c: null });
  });

  it("walks nested objects through union members, array items and references", () => {
    const inner = object({ x: { anyOf: [{ type: "string" }, { type: "null" }] } }, []);
    const schema = {
      type: "object",
      properties: {
        o: { anyOf: [inner, { type: "null" }] },
        list: { type: "array", items: inner },
        ref: { $ref: "#/$defs/inner" },
      },
      $defs: { inner },
    };
    assert.deepStrictEqual(
      dropNullOptionals(schema, {
        o: { x: null },
        list: [{ x: null }, { x: "y" }],
        ref: { x: null },
      }),
      { o: {}, list: [{}, { x: "y" }], ref: {} },
    );
  });

  it("returns the value itself when there is no optional null, and stops at a reference cycle", () => {
    const value = { a: 1 };
    assert.strictEqual(dropNullOptionals(object({ a: { type: "number" } }, []), value), value);
    const cycle = {
      $ref: "#/$defs/a",
      $defs: { a: { $ref: "#/$defs/b" }, b: { $ref: "#/$defs/a" } },
    };
    assert.strictEqual(dropNullOptionals(cycle, value), value);
  });
});

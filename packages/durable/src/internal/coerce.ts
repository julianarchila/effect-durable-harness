// Coercion of a model's tool arguments towards the JSON Schema the model was offered, before they are decoded.
// Models often send `"42"` for a number, a single value for an array, or `null` for an omitted optional property.
//
// Three passes, in this order:
// 1. Optional nulls: a `null` in an optional property whose schema rejects null is dropped, as if omitted.
// 2. Conversion rules keyed on JSON Schema structure (`$ref`, `const`, `enum`, `anyOf`/`oneOf`, `allOf`, `type`,
//    `items`/`prefixItems`, `properties`/`patternProperties`): `null` becomes "null" for a string, `"3.5"` becomes 3
//    for an integer, a single value becomes a one-item array, and a union takes the first branch whose conversion
//    passes.
// 3. Primitive coercion by `type` (also type arrays), as Ajv's `coerceTypes`: what pass 2 left, for example a type
//    array `["integer", "null"]`, or `""` for `null`.
// A value that cannot be coerced is left as it is; the decode then reports it.
//
// Every pass is copy-on-write: an object or array is copied only when one of its members changes, so an unchanged
// value comes back as the same reference and the input is never modified. All passes walk the same keywords for
// members: `properties`, then `patternProperties`, then `additionalProperties` for keys neither names; tuple items by
// `prefixItems` (or a draft-7 `items` array), the rest by `items`.
//
// Whether a value passes a schema node (to pick a union branch, or to tell whether an optional property accepts
// null) is a structural JSON Schema check of the common keywords; a keyword it does not know accepts.
import * as Predicate from "effect/Predicate";
import type * as Schema from "effect/Schema";
import { jsonEqual } from "./json.ts";

type Node = Schema.JsonObject;
type MutableObject = { [key: string]: Schema.Json };

/** `value` coerced towards `schema`: a copy along the changed paths only, or `value` itself when nothing changes. */
export const coerce = (schema: Schema.JsonObject, value: Schema.Json): Schema.Json => {
  const coercer = new Coercer(schema);
  return coercer.coerceTypes(
    coercer.convertRoot(coercer.dropOptionalNulls(value, schema, false)),
    schema,
  );
};

/**
 * `value` without a `null` in any property `schema` does not require, also nested and through local references,
 * whether or not the property's schema accepts null. A JSON Schema may accept `null` for a property whose decoder
 * accepts only its absence, as Effect generates `Schema.optional`; this is the value to decode when `value` itself
 * does not.
 */
export const dropNullOptionals = (schema: Schema.JsonObject, value: Schema.Json): Schema.Json =>
  new Coercer(schema).dropOptionalNulls(value, schema, true);

class Coercer {
  private readonly checker: Checker;
  /**
   * The (schema node, value) pairs pass 2 is converting through a re-entry point, as a stack. Conversion can enter
   * a node again with the same value only through a `$ref` or by wrapping a single value into an array: a recursive
   * schema that meets a value no branch accepts would convert it without end. A pair already on the stack
   * (primitives by value, objects by reference) leaves the value unchanged.
   */
  private readonly entered: Array<Schema.Json> = [];
  /** How often a re-entry was cut short; results computed across one are not memoized. */
  private cuts = 0;
  /**
   * Results of `$ref` targets by (node, value): a recursive union whose branches all convert the same children (and
   * all fail) would convert them once per branch, exponentially in the depth.
   */
  private readonly memo = new Map<Node, Map<Schema.Json, Schema.Json>>();
  private readonly root: Node;

  constructor(root: Node) {
    this.root = root;
    this.checker = new Checker(root);
  }

  convertRoot(value: Schema.Json): Schema.Json {
    return this.convert(this.root, value);
  }

  private passes(node: Schema.Json | undefined, value: Schema.Json): boolean {
    return node === undefined ? true : this.checker.check(node, value);
  }

  /** `node` converted for `value` at a re-entry point: unchanged when that pair is already being converted. */
  private reenter(node: Node, value: Schema.Json, memoize: boolean): Schema.Json {
    const entered = this.entered;
    for (let i = 0; i < entered.length; i += 2) {
      if (entered[i] === node && Object.is(entered[i + 1], value)) {
        this.cuts++;
        return value;
      }
    }
    const memo = memoize ? this.memo.get(node) : undefined;
    if (memo?.has(value)) return memo.get(value)!;
    const cuts = this.cuts;
    entered.push(node, value);
    const result = this.convert(node, value);
    entered.length -= 2;
    if (memoize && this.cuts === cuts) {
      const values = this.memo.get(node) ?? new Map<Schema.Json, Schema.Json>();
      this.memo.set(node, values);
      values.set(value, result);
    }
    return result;
  }

  // ─── Pass 1: optional nulls ───────────────────────────────────────────────

  /**
   * With `every`, every `null` in a property that is not required is dropped, and references are followed;
   * otherwise only one the property's schema rejects, and a property that is a reference keeps its `null`.
   */
  dropOptionalNulls(
    value: Schema.Json,
    node: Schema.Json | undefined,
    every: boolean,
  ): Schema.Json {
    const schema = every ? this.followRefs(node) : node;
    if (!isNode(schema)) return value;
    if (every) {
      // Any member may be the one that decodes: each drops what it does not require.
      for (const keyword of ["allOf", "anyOf", "oneOf"]) {
        const members = schema[keyword];
        if (!isJsonArray(members)) continue;
        for (const member of members) value = this.dropOptionalNulls(value, member, true);
      }
    }
    if (isJsonArray(value)) {
      return mapItems(value, (item, i) => {
        const itemSchema = itemSchemaAt(schema, i);
        return itemSchema === undefined ? item : this.dropOptionalNulls(item, itemSchema, every);
      });
    }
    if (!isJsonObject(value)) return value;
    const properties = isNode(schema.properties) ? schema.properties : {};
    const patterns = patternsOf(schema);
    const additional = isNode(schema.additionalProperties)
      ? schema.additionalProperties
      : undefined;
    if (patterns.length === 0 && additional === undefined && !isNode(schema.properties))
      return value;
    const required = new Set(isJsonArray(schema.required) ? schema.required : []);
    let out: MutableObject | undefined;
    const set = (key: string, next: Schema.Json) => {
      if (next === (out ?? value)[key]) return;
      out ??= { ...value };
      out[key] = next;
    };
    for (const [key, property] of Object.entries(properties)) {
      if (!Object.hasOwn(value, key)) continue;
      const isRef = isNode(property) && Predicate.isString(property.$ref);
      const drop = every || (!isRef && !this.passes(property, null));
      if (value[key] === null && !required.has(key) && drop) {
        out ??= { ...value };
        delete out[key];
        continue;
      }
      set(key, this.dropOptionalNulls(value[key]!, property, every));
    }
    // Values of pattern and additional properties are walked for their own optional nulls; the keys themselves
    // are not optional properties, so they stay.
    if (patterns.length > 0 || additional !== undefined) {
      for (const key of Object.keys(value)) {
        let matched = Object.hasOwn(properties, key);
        for (const [pattern, item] of patterns) {
          if (!pattern.test(key)) continue;
          matched = true;
          set(key, this.dropOptionalNulls((out ?? value)[key]!, item, every));
        }
        if (!matched && additional !== undefined)
          set(key, this.dropOptionalNulls(value[key]!, additional, every));
      }
    }
    return out ?? value;
  }

  /** The node a chain of references ends at; a chain longer than any sane schema's (a cycle) ends nowhere. */
  private followRefs(node: Schema.Json | undefined): Schema.Json | undefined {
    let current = node;
    for (let hops = 0; hops < 32; hops++) {
      if (!isNode(current) || !Predicate.isString(current.$ref)) return current;
      current = this.checker.resolve(current.$ref);
    }
    return undefined;
  }

  // ─── Pass 2: conversion rules ─────────────────────────────────────────────

  private convert(node: Schema.Json | undefined, value: Schema.Json): Schema.Json {
    if (!isNode(node)) return value;
    if (Predicate.isString(node.$ref)) {
      const target = this.checker.resolve(node.$ref);
      return isNode(target) ? this.reenter(target, value, true) : value;
    }
    return this.convertNode(node, value);
  }

  /** One schema node's conversion: its first keyword among `const`, `enum`, the unions, `allOf` and `type`. */
  private convertNode(node: Node, value: Schema.Json): Schema.Json {
    if (Object.hasOwn(node, "const")) return convertLiteral(node.const!, value);
    if (isJsonArray(node.enum)) return this.convertEnum(node, node.enum, value);
    // A union or intersection node may also declare members: they convert after the branches.
    if (isJsonArray(node.anyOf)) return this.convertUnion(node, node.anyOf, value);
    if (isJsonArray(node.oneOf)) return this.convertUnion(node, node.oneOf, value);
    if (isJsonArray(node.allOf)) {
      let next = value;
      for (const member of node.allOf) next = this.convert(member, next);
      return this.convertMembers(node, next);
    }
    switch (node.type) {
      case "string":
        return orValue(tryString(value), value);
      case "number":
        return orValue(tryNumber(value), value);
      case "integer": {
        const result = tryNumber(value);
        return result === undefined ? value : Math.trunc(result.value);
      }
      case "boolean":
        return orValue(tryBoolean(value), value);
      case "null":
        return orValue(tryNull(value), value);
      case "array":
        return this.convertArray(node, value);
      case "object":
        return this.convertObject(node, value);
      case undefined:
        if (node.properties !== undefined || node.patternProperties !== undefined) {
          return this.convertObject(node, value);
        }
        if (node.items !== undefined || node.prefixItems !== undefined)
          return this.convertArray(node, value);
        return value;
      default:
        return value;
    }
  }

  /** An enum is a union of its values: a member stays, else the first value the input converts to. */
  private convertEnum(node: Node, values: Schema.JsonArray, value: Schema.Json): Schema.Json {
    if (values.some((member) => member === value)) return value;
    for (const member of values) {
      const candidate = convertLiteral(member, value);
      if (candidate !== value && this.passes(node, candidate)) return candidate;
    }
    return value;
  }

  /**
   * A value one branch accepts stays; else the first branch's conversion that the whole union accepts. Branches are
   * converted in order and only until one is accepted, so a later recursive branch is not entered needlessly. A node
   * that also declares members converts them into each candidate before it is checked, so a branch is not
   * discarded for a sibling property that is converted after it.
   */
  private convertUnion(node: Node, branches: Schema.JsonArray, value: Schema.Json): Schema.Json {
    if (branches.some((branch) => this.passes(branch, value)))
      return this.convertMembers(node, value);
    for (const branch of branches) {
      const candidate = this.convertMembers(node, this.convert(branch, value));
      if (this.passes(node, candidate)) return candidate;
    }
    return this.convertMembers(node, value);
  }

  /** The object members of a union or intersection node, when it declares any. */
  private convertMembers(node: Node, value: Schema.Json): Schema.Json {
    const declares =
      node.properties !== undefined ||
      node.patternProperties !== undefined ||
      node.additionalProperties !== undefined;
    return declares ? this.convertObject(node, value) : value;
  }

  private convertArray(node: Node, value: Schema.Json): Schema.Json {
    const prefix = isJsonArray(node.items)
      ? node.items
      : isJsonArray(node.prefixItems)
        ? node.prefixItems
        : undefined;
    if (prefix !== undefined) {
      // A tuple converts its items and never wraps a single value.
      if (!isJsonArray(value)) return value;
      const rest = isJsonArray(node.items) ? undefined : node.items;
      return mapItems(value, (item, i) => {
        const schema = i < prefix.length ? prefix[i] : rest;
        return schema === undefined ? item : this.convert(schema, item);
      });
    }
    const items = node.items;
    if (isJsonArray(value)) {
      return items === undefined ? value : mapItems(value, (item) => this.convert(items, item));
    }
    // The wrapped single value is converted by the item schema with the same value: a re-entry point.
    return [isNode(items) ? this.reenter(items, value, false) : value];
  }

  private convertObject(node: Node, value: Schema.Json): Schema.Json {
    if (!isJsonObject(value)) return value;
    const properties = isNode(node.properties) ? node.properties : {};
    const patterns = patternsOf(node);
    const additional = isNode(node.additionalProperties) ? node.additionalProperties : undefined;
    let out: MutableObject | undefined;
    for (const key of Object.keys(value)) {
      const current: Schema.Json = value[key]!;
      let next: Schema.Json = current;
      let matched = Object.hasOwn(properties, key);
      if (matched) next = this.convert(properties[key], next);
      for (const [pattern, schema] of patterns) {
        if (!pattern.test(key)) continue;
        matched = true;
        next = this.convert(schema, next);
      }
      if (!matched && additional !== undefined) next = this.convert(additional, next);
      if (next !== current) {
        out ??= { ...value };
        out[key] = next;
      }
    }
    return out ?? value;
  }

  // ─── Pass 3: primitive coercion by type ───────────────────────────────────

  coerceTypes(value: Schema.Json, schema: Schema.Json | undefined): Schema.Json {
    if (!isNode(schema)) return value;
    let next = value;
    if (isJsonArray(schema.allOf))
      for (const member of schema.allOf) next = this.coerceTypes(next, member);
    if (isJsonArray(schema.anyOf)) next = this.coerceUnion(next, schema.anyOf);
    if (isJsonArray(schema.oneOf)) next = this.coerceUnion(next, schema.oneOf);
    const types = typesOf(schema);
    const matchesUnionMember = types.length > 1 && types.some((type) => matchesType(next, type));
    if (types.length > 0 && !matchesUnionMember) {
      for (const type of types) {
        const candidate = coercePrimitive(next, type);
        if (candidate !== next) {
          next = candidate;
          break;
        }
      }
    }
    if (types.includes("object") && isJsonObject(next)) next = this.coerceProperties(next, schema);
    if (types.includes("array") && isJsonArray(next)) next = this.coerceItems(next, schema);
    return next;
  }

  private coerceUnion(value: Schema.Json, branches: Schema.JsonArray): Schema.Json {
    for (const branch of branches) if (this.passes(branch, value)) return value;
    for (const branch of branches) {
      const coerced = this.coerceTypes(value, branch);
      if (this.passes(branch, coerced)) return coerced;
    }
    return value;
  }

  private coerceProperties(value: Schema.JsonObject, schema: Node): Schema.JsonObject {
    const properties = isNode(schema.properties) ? schema.properties : {};
    const patterns = patternsOf(schema);
    const additional = isNode(schema.additionalProperties)
      ? schema.additionalProperties
      : undefined;
    let out: MutableObject | undefined;
    const set = (key: string, next: Schema.Json) => {
      if (next === (out ?? value)[key]) return;
      out ??= { ...value };
      out[key] = next;
    };
    for (const [key, property] of Object.entries(properties)) {
      if (Object.hasOwn(value, key)) set(key, this.coerceTypes(value[key]!, property));
    }
    for (const key of Object.keys(value)) {
      let matched = Object.hasOwn(properties, key);
      for (const [pattern, item] of patterns) {
        if (!pattern.test(key)) continue;
        matched = true;
        set(key, this.coerceTypes((out ?? value)[key]!, item));
      }
      if (!matched && additional !== undefined) set(key, this.coerceTypes(value[key]!, additional));
    }
    return out ?? value;
  }

  private coerceItems(value: Schema.JsonArray, schema: Node): Schema.JsonArray {
    return mapItems(value, (item, i) => {
      const itemSchema = itemSchemaAt(schema, i);
      return isNode(itemSchema) ? this.coerceTypes(item, itemSchema) : item;
    });
  }
}

// ─── The structural check ───────────────────────────────────────────────────

/**
 * Checks values against the nodes of one root schema. Local references (`#`, `#/$defs/...`, any JSON Pointer into
 * the root) resolve; a reference that names nothing rejects only the values at its place. Keywords the check does
 * not know, `format` among them, accept.
 */
class Checker {
  /** The (node, value) pairs being checked through a `$ref`: a reference cycle that consumes no value rejects. */
  private readonly entered: Array<Schema.Json> = [];
  private readonly root: Node;

  constructor(root: Node) {
    this.root = root;
  }

  resolve(ref: string): Schema.Json | undefined {
    if (ref === "#") return this.root;
    if (!ref.startsWith("#/")) return undefined;
    let node: Schema.Json | undefined = this.root;
    for (const raw of ref.slice(2).split("/")) {
      const segment = decodeURIComponent(raw).replace(/~1/g, "/").replace(/~0/g, "~");
      if (isJsonArray(node)) node = node[Number(segment)];
      else if (isJsonObject(node) && Object.hasOwn(node, segment)) node = node[segment];
      else return undefined;
    }
    return node;
  }

  check(node: Schema.Json, value: Schema.Json): boolean {
    if (node === true) return true;
    if (node === false) return false;
    if (!isNode(node)) return true;
    if (Predicate.isString(node.$ref) && !this.checkRef(node.$ref, value)) return false;
    const types = typesOf(node);
    if (types.length > 0 && !types.some((type) => matchesType(value, type))) return false;
    if (Object.hasOwn(node, "const") && !jsonEqual(node.const!, value)) return false;
    if (isJsonArray(node.enum) && !node.enum.some((member) => jsonEqual(member, value)))
      return false;
    if (isJsonArray(node.allOf) && !node.allOf.every((member) => this.check(member, value)))
      return false;
    if (isJsonArray(node.anyOf) && !node.anyOf.some((member) => this.check(member, value)))
      return false;
    if (
      isJsonArray(node.oneOf) &&
      node.oneOf.filter((member) => this.check(member, value)).length !== 1
    )
      return false;
    if (node.not !== undefined && this.check(node.not, value)) return false;
    if (node.if !== undefined) {
      const branch = this.check(node.if, value) ? node.then : node.else;
      if (branch !== undefined && !this.check(branch, value)) return false;
    }
    if (Predicate.isNumber(value)) return checkNumber(node, value);
    if (Predicate.isString(value)) return checkString(node, value);
    if (isJsonArray(value)) return this.checkArray(node, value);
    if (isJsonObject(value)) return this.checkObject(node, value);
    return true;
  }

  private checkRef(ref: string, value: Schema.Json): boolean {
    const target = this.resolve(ref);
    if (target === undefined) return false;
    const entered = this.entered;
    for (let i = 0; i < entered.length; i += 2) {
      if (entered[i] === target && Object.is(entered[i + 1], value)) return false;
    }
    entered.push(target, value);
    const result = this.check(target, value);
    entered.length -= 2;
    return result;
  }

  private checkArray(node: Node, value: Schema.JsonArray): boolean {
    if (Predicate.isNumber(node.minItems) && value.length < node.minItems) return false;
    if (Predicate.isNumber(node.maxItems) && value.length > node.maxItems) return false;
    const prefix = isJsonArray(node.prefixItems)
      ? node.prefixItems
      : isJsonArray(node.items)
        ? node.items
        : [];
    const rest = isJsonArray(node.items) ? node.additionalItems : node.items;
    for (let i = 0; i < value.length; i++) {
      const schema = i < prefix.length ? prefix[i] : rest;
      if (schema !== undefined && !this.check(schema, value[i]!)) return false;
    }
    if (node.contains !== undefined && !value.some((item) => this.check(node.contains!, item)))
      return false;
    if (
      node.uniqueItems === true &&
      value.some((item, i) => value.slice(i + 1).some((other) => jsonEqual(item, other)))
    )
      return false;
    return true;
  }

  private checkObject(node: Node, value: Schema.JsonObject): boolean {
    const keys = Object.keys(value);
    if (Predicate.isNumber(node.minProperties) && keys.length < node.minProperties) return false;
    if (Predicate.isNumber(node.maxProperties) && keys.length > node.maxProperties) return false;
    if (isJsonArray(node.required)) {
      for (const key of node.required) {
        if (Predicate.isString(key) && !Object.hasOwn(value, key)) return false;
      }
    }
    const properties = isNode(node.properties) ? node.properties : {};
    const patterns = patternsOf(node);
    for (const key of keys) {
      const item = value[key]!;
      let matched = Object.hasOwn(properties, key);
      if (matched && !this.check(properties[key]!, item)) return false;
      for (const [pattern, schema] of patterns) {
        if (!pattern.test(key)) continue;
        matched = true;
        if (!this.check(schema, item)) return false;
      }
      if (!matched && node.additionalProperties !== undefined) {
        if (!this.check(node.additionalProperties, item)) return false;
      }
      if (node.propertyNames !== undefined && !this.check(node.propertyNames, key)) return false;
    }
    return true;
  }
}

const checkNumber = (node: Node, value: number): boolean =>
  !(Predicate.isNumber(node.minimum) && value < node.minimum) &&
  !(Predicate.isNumber(node.maximum) && value > node.maximum) &&
  !(Predicate.isNumber(node.exclusiveMinimum) && value <= node.exclusiveMinimum) &&
  !(Predicate.isNumber(node.exclusiveMaximum) && value >= node.exclusiveMaximum) &&
  !(
    Predicate.isNumber(node.multipleOf) &&
    node.multipleOf > 0 &&
    !Number.isInteger(value / node.multipleOf)
  );

const checkString = (node: Node, value: string): boolean => {
  // JSON Schema counts code points, not UTF-16 units.
  const length = [...value].length;
  if (Predicate.isNumber(node.minLength) && length < node.minLength) return false;
  if (Predicate.isNumber(node.maxLength) && length > node.maxLength) return false;
  if (Predicate.isString(node.pattern) && !patternOf(node.pattern).test(value)) return false;
  return true;
};

// ─── Shared helpers ─────────────────────────────────────────────────────────

const isNode = (value: Schema.Json | undefined): value is Node =>
  Predicate.isObject(value) && !Array.isArray(value);

const isJsonObject = (value: Schema.Json | undefined): value is Schema.JsonObject => isNode(value);

const isJsonArray = (value: Schema.Json | undefined): value is Schema.JsonArray =>
  Array.isArray(value);

/** Compiled patterns by source; an invalid pattern matches nothing. */
const patterns = new Map<string, RegExp>();

const patternOf = (source: string): RegExp => {
  const cached = patterns.get(source);
  if (cached !== undefined) return cached;
  const compiled = compilePattern(source);
  patterns.set(source, compiled);
  return compiled;
};

const compilePattern = (source: string): RegExp => {
  try {
    return new RegExp(source, "u");
  } catch {
    try {
      return new RegExp(source);
    } catch {
      return /(?!)/;
    }
  }
};

const patternsOf = (node: Node): ReadonlyArray<readonly [RegExp, Schema.Json]> =>
  isNode(node.patternProperties)
    ? Object.entries(node.patternProperties).map(
        ([pattern, item]) => [patternOf(pattern), item] as const,
      )
    : [];

/**
 * The schema of an array's item at `index`: a tuple's (`prefixItems`, or a draft-7 `items` array) for its prefix,
 * `items` after `prefixItems`, or `items` for every item. Undefined when nothing applies.
 */
const itemSchemaAt = (schema: Node, index: number): Schema.Json | undefined => {
  if (isJsonArray(schema.items)) return schema.items[index];
  if (isJsonArray(schema.prefixItems) && index < schema.prefixItems.length)
    return schema.prefixItems[index];
  return schema.items;
};

/** `items` mapped by `map`; the same array when every item maps to itself. */
const mapItems = (
  items: Schema.JsonArray,
  map: (item: Schema.Json, index: number) => Schema.Json,
): Schema.JsonArray => {
  let out: Array<Schema.Json> | undefined;
  for (let i = 0; i < items.length; i++) {
    const next = map(items[i]!, i);
    if (next === items[i]) continue;
    out ??= items.slice();
    out[i] = next;
  }
  return out ?? items;
};

const typesOf = (schema: Node): ReadonlyArray<string> => {
  if (Predicate.isString(schema.type)) return [schema.type];
  if (isJsonArray(schema.type)) return schema.type.filter(Predicate.isString);
  return [];
};

const matchesType = (value: Schema.Json, type: string): boolean => {
  switch (type) {
    case "number":
      return Predicate.isNumber(value) && Number.isFinite(value);
    case "integer":
      return Predicate.isNumber(value) && Number.isInteger(value);
    case "boolean":
      return Predicate.isBoolean(value);
    case "string":
      return Predicate.isString(value);
    case "null":
      return value === null;
    case "array":
      return isJsonArray(value);
    case "object":
      return isJsonObject(value);
    default:
      return false;
  }
};

// ─── Conversions of one value: a result, or undefined when there is none ────

type Converted<T> = { readonly value: T } | undefined;

const orValue = (result: Converted<Schema.Json>, value: Schema.Json): Schema.Json =>
  result === undefined ? value : result.value;

/** A literal (`const`, an enum value) taken from a value of another type that converts to exactly it. */
const convertLiteral = (literal: Schema.Json, value: Schema.Json): Schema.Json => {
  if (literal === value) return value;
  const result = Predicate.isString(literal)
    ? tryString(value)
    : Predicate.isNumber(literal)
      ? tryNumber(value)
      : Predicate.isBoolean(literal)
        ? tryBoolean(value)
        : undefined;
  return result !== undefined && result.value === literal ? result.value : value;
};

const tryString = (value: Schema.Json): Converted<string> => {
  if (Predicate.isBoolean(value) || isFiniteNumber(value)) return { value: String(value) };
  if (value === null) return { value: "null" };
  if (Predicate.isString(value)) return { value };
  return undefined;
};

const tryNumber = (value: Schema.Json): Converted<number> => {
  if (Predicate.isBoolean(value)) return { value: value ? 1 : 0 };
  if (isFiniteNumber(value)) return { value };
  if (value === null) return { value: 0 };
  if (!Predicate.isString(value)) return undefined;
  const coerced = +value;
  if (Number.isFinite(coerced)) return { value: coerced };
  const lowercase = value.toLowerCase();
  if (lowercase === "false") return { value: 0 };
  if (lowercase === "true") return { value: 1 };
  return integerOfString(value);
};

/** `12n`, or `12.5` truncated, within the safe integers. */
const integerOfString = (value: string): Converted<number> => {
  const digits = /^-?(0|[1-9]\d*)n$/.test(value)
    ? value.slice(0, -1)
    : /^-?(0|[1-9]\d*)\.\d+$/.test(value)
      ? value.split(".")[0]!
      : undefined;
  if (digits === undefined) return undefined;
  const big = BigInt(digits);
  return big <= BigInt(Number.MAX_SAFE_INTEGER) && big >= BigInt(Number.MIN_SAFE_INTEGER)
    ? { value: Number(big) }
    : undefined;
};

const tryBoolean = (value: Schema.Json): Converted<boolean> => {
  if (Predicate.isBoolean(value)) return { value };
  if (isFiniteNumber(value))
    return value === 0 ? { value: false } : value === 1 ? { value: true } : undefined;
  if (value === null) return { value: false };
  if (!Predicate.isString(value)) return undefined;
  const lowercase = value.toLowerCase();
  if (lowercase === "false" || value === "0") return { value: false };
  if (lowercase === "true" || value === "1") return { value: true };
  return undefined;
};

const tryNull = (value: Schema.Json): Converted<null> => {
  if (Predicate.isBoolean(value)) return value === false ? { value: null } : undefined;
  if (isFiniteNumber(value)) return value === 0 ? { value: null } : undefined;
  if (value === null) return { value: null };
  if (!Predicate.isString(value)) return undefined;
  const lowercase = value.toLowerCase();
  return lowercase === "undefined" || lowercase === "null" || value === "" || value === "0"
    ? { value: null }
    : undefined;
};

const isFiniteNumber = (value: Schema.Json): value is number =>
  Predicate.isNumber(value) && Number.isFinite(value);

const coercePrimitive = (value: Schema.Json, type: string): Schema.Json => {
  switch (type) {
    case "number":
    case "integer": {
      if (value === null) return 0;
      if (Predicate.isString(value) && value.trim() !== "") {
        const parsed = Number(value);
        if (type === "number" ? Number.isFinite(parsed) : Number.isInteger(parsed)) return parsed;
      }
      if (Predicate.isBoolean(value)) return value ? 1 : 0;
      return value;
    }
    case "boolean":
      if (value === null) return false;
      if (value === "true" || value === 1) return true;
      if (value === "false" || value === 0) return false;
      return value;
    case "string":
      if (value === null) return "";
      if (Predicate.isNumber(value) || Predicate.isBoolean(value)) return String(value);
      return value;
    case "null":
      return value === "" || value === 0 || value === false ? null : value;
    default:
      return value;
  }
};

/**
 * Strict-JSON copying for draft placements and for values a kernel takes ownership of.
 *
 * @since 0.1.0
 * @internal
 */

/**
 * Mutable strict JSON, as owned by the kernel while it builds a revision.
 *
 * @internal
 */
export type Json = null | boolean | number | string | Array<Json> | { [key: string]: Json };

const DATA_DESCRIPTOR: PropertyDescriptor = {
  value: undefined,
  writable: true,
  enumerable: true,
  configurable: true,
};

/**
 * Copies a value into an alias-free strict-JSON tree owned by the caller, throwing a `TypeError` for any
 * value that is not strict JSON. With `omitUndefinedProperties`, object properties whose value is
 * `undefined` are dropped; array elements never are.
 *
 * @internal
 */
export const copyJson = (value: unknown, omitUndefinedProperties = false): Json =>
  copy(value, undefined, omitUndefinedProperties);

const copy = (
  value: unknown,
  ancestors: Set<object> | undefined,
  omitUndefinedProperties: boolean,
): Json => {
  if (value === null || typeof value === "string" || typeof value === "boolean") return value;
  if (typeof value === "number") {
    if (Number.isFinite(value)) return value;
    throw new TypeError("Value contains a non-finite number and is not strict JSON");
  }
  if (typeof value !== "object") {
    throw new TypeError(`Value contains a non-JSON ${typeof value}; expected strict JSON`);
  }
  const active = ancestors ?? new Set<object>();
  if (active.has(value)) throw new TypeError("Value contains cycles and is not strict JSON");
  active.add(value);
  try {
    if (Array.isArray(value)) {
      // Indices plus `length` only: extra, symbol, and missing keys all change the count.
      if (
        Object.getPrototypeOf(value) !== Array.prototype ||
        Reflect.ownKeys(value).length !== value.length + 1
      ) {
        throw new TypeError("Value must contain strict JSON dense plain arrays");
      }
      const result = new Array<Json>(value.length);
      for (let index = 0; index < value.length; index++) {
        const descriptor = Object.getOwnPropertyDescriptor(value, index);
        if (descriptor === undefined || !descriptor.enumerable || !("value" in descriptor)) {
          throw new TypeError("Value must contain strict JSON enumerable indexed data properties");
        }
        defineData(result, String(index), copy(descriptor.value, active, omitUndefinedProperties));
      }
      return result;
    }
    const prototype = Object.getPrototypeOf(value);
    if (prototype !== Object.prototype && prototype !== null) {
      throw new TypeError("Value must contain strict JSON plain objects or arrays");
    }
    const result = Object.create(prototype) as Record<string, Json>;
    for (const key of Reflect.ownKeys(value)) {
      if (typeof key === "symbol")
        throw new TypeError("Value contains a symbol key and is not strict JSON");
      const descriptor = Object.getOwnPropertyDescriptor(value, key)!;
      if (!descriptor.enumerable || !("value" in descriptor)) {
        throw new TypeError("Value must contain strict JSON enumerable data properties");
      }
      if (descriptor.value === undefined && omitUndefinedProperties) continue;
      defineData(result, key, copy(descriptor.value, active, omitUndefinedProperties));
    }
    return result;
  } finally {
    active.delete(value);
  }
};

const defineData = (target: object, key: PropertyKey, value: Json): void => {
  DATA_DESCRIPTOR.value = value;
  Object.defineProperty(target, key, DATA_DESCRIPTOR);
  DATA_DESCRIPTOR.value = undefined;
};

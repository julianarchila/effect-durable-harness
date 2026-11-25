/**
 * Strict-JSON ownership: copy a caller's value into an alias-free tree before
 * a tracker or a store takes ownership of it. This is the same walk that
 * validates draft placements.
 *
 * @since 0.1.0
 */
import type * as Schema from "effect/Schema";
import * as internal from "./internal/json.ts";

/**
 * Options of {@link copy}.
 *
 * @category models
 * @since 0.1.0
 */
export interface CopyOptions {
  /** Drop object properties whose value is `undefined`, as `JSON.stringify` does; array elements are never dropped. */
  readonly omitUndefinedProperties?: boolean | undefined;
}

/**
 * Copies a value into an alias-free strict-JSON tree owned by the caller.
 *
 * **Details**
 *
 * Strict JSON is dense plain arrays, plain or null-prototype objects with
 * enumerable own data properties, strings, booleans, finite numbers and
 * `null`. Own `__proto__` data properties are preserved as data.
 *
 * **Gotchas**
 *
 * Throws a `TypeError` whose message says the value is not strict JSON for
 * `undefined`, non-finite numbers, functions, symbols, bigints, accessors,
 * symbol keys, sparse or extended arrays, class instances and cycles.
 *
 * @category constructors
 * @since 0.1.0
 */
export const copy = (value: unknown, options?: CopyOptions): Schema.Json =>
  internal.copyJson(value, options?.omitUndefinedProperties === true);

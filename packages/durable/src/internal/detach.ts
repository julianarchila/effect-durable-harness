/**
 * Detached copies of JSON values for the memory backend: a key such as
 * `__proto__` stays an own property instead of reaching the prototype.
 *
 * @internal
 */
import * as Predicate from "effect/Predicate";

const copy = <A>(value: A): A => {
  if (Array.isArray(value)) {
    // SAFETY: an array copied element by element has the type of the original.
    return value.map(copy) as A;
  }
  if (!Predicate.isObject(value)) return value;
  const nullPrototype = Object.getPrototypeOf(value) === null;
  const result: { [key: string]: A } = nullPrototype ? Object.create(null) : {};
  for (const [key, child] of Object.entries(value)) {
    Object.defineProperty(result, key, {
      value: copy(child),
      writable: true,
      enumerable: true,
      configurable: true,
    });
  }
  // SAFETY: a plain object copied key by key has the type of the original.
  return result as A;
};

/** A deep copy of a JSON-like value that shares no container with it. */
export const detach: <A>(value: A) => A = copy;

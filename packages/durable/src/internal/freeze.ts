// Deep freezing of records the Session shares: read results are frozen shared objects, so a caller that mutates
// one throws instead of changing what the next read returns.
import * as Predicate from "effect/Predicate";

/** Freezes `value` and everything it holds, in place, and returns it. Already frozen parts are left as they are. */
export const deepFreeze = <A>(value: A): A => {
  if (!Predicate.isObjectOrArray(value) || Object.isFrozen(value)) return value;
  for (const child of Object.values(value)) deepFreeze(child);
  Object.freeze(value);
  return value;
};

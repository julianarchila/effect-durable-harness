import {
  assertInstanceOf,
  assertMatch,
  assertTrue,
  strictEqual,
  throws,
} from "@effect/vitest/utils";
import * as Delta from "@effect-durable-harness/delta/Delta";

/**
 * Asserts that `thunk` throws a `DeltaError`, optionally with the given reason tag and a message matching
 * `pattern`.
 */
export const throwsDeltaError = (
  thunk: () => void,
  reason?: Delta.DeltaErrorReason["_tag"],
  pattern?: RegExp,
): void => {
  throws(thunk, (error) => {
    assertTrue(Delta.isDeltaError(error), `expected a DeltaError, got ${String(error)}`);
    if (reason !== undefined) strictEqual(error.reason._tag, reason);
    if (pattern !== undefined) assertMatch(error.message, pattern);
    return undefined;
  });
};

/**
 * Asserts that `thunk` throws a `TypeError`, optionally with a message matching `pattern`.
 */
export const throwsTypeError = (thunk: () => void, pattern?: RegExp): void => {
  throws(thunk, (error) => {
    assertInstanceOf(error, TypeError);
    if (pattern !== undefined) assertMatch(error.message, pattern);
    return undefined;
  });
};

/**
 * Asserts that `thunk` throws an `Error` whose message matches `pattern`.
 */
export const throwsMessage = (thunk: () => void, pattern: RegExp): void => {
  throws(thunk, (error) => {
    assertInstanceOf(error, Error);
    assertMatch(error.message, pattern);
    return undefined;
  });
};

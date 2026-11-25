/**
 * Signals: typed JSON values delivered to a live task from outside, kept in the
 * task until a wake uses them. A task sleeps on signal keys (`Task.sleep`), a
 * host delivers a value with `Harness.signal` or `Tx.signal`, and the woken code
 * reads it with `readSignal`.
 *
 * @since 0.0.0
 */
import * as Schema from "effect/Schema";
import { SettledSubmissionRecord } from "./Records.ts";

/**
 * A signal kind, built by `Signal.make`: values of type `T` delivered under
 * keys.
 *
 * @category models
 * @since 0.0.0
 */
export interface Signal<T> {
  /** Unique like a task or document kind; contains no `/`. */
  readonly name: string;
  /** Positive integer version of the value shape, stored with every value. */
  readonly version: number;
  /** The value's codec to JSON; a delivery it cannot encode is `invalid`. Values are stored encoded. */
  readonly schema: Schema.Codec<T, Schema.Json>;
  /** Converts a value stored, encoded, by an older version; pure. Its result is checked with `schema`. */
  readonly migrate?: ((value: Schema.Json, fromVersion: number) => NoInfer<T>) | undefined;
  /**
   * The key of one wait, as sleeps store it. Throws a `TypeError` for an empty key.
   */
  readonly key: (key: string) => SignalKey<T>;
}

/**
 * The key of one wait for a signal: the signal's name and the key. JSON, used
 * in sleeps and parks; `T` is a phantom type.
 *
 * @category models
 * @since 0.0.0
 */
export interface SignalKey<T> {
  readonly signal: string;
  readonly key: string;
  readonly "~value"?: T | undefined;
}

/**
 * A key's state: the value delivered under it, or expired because its sleep's
 * deadline came first.
 *
 * @category models
 * @since 0.0.0
 */
export type SignalState<T> = { readonly value: T } | { readonly expired: true };

/**
 * What a delivery did: `delivered` (durably accepted, not necessarily
 * consumed), `duplicate` (the key holds a value), `invalid` (not JSON, an empty
 * key, or not matching the schema), `closed` (the task ended, holds its
 * outcome, is abort-marked, or the key expired), `not_found` (no such task).
 *
 * @category models
 * @since 0.0.0
 */
export type SignalResult = "delivered" | "duplicate" | "invalid" | "closed" | "not_found";

/**
 * Options of `Signal.make`.
 *
 * @category constructors
 * @since 0.0.0
 */
export interface SignalOptions<T, I extends Schema.Json> {
  readonly version: number;
  readonly schema: Schema.Codec<T, I>;
  readonly migrate?: ((value: Schema.Json, fromVersion: number) => NoInfer<T>) | undefined;
}

/**
 * Defines a signal kind. `T` is inferred from `schema`, a codec whose encoding
 * is JSON: values are stored encoded and decoded when read.
 *
 * **Gotchas**
 *
 * Throws a `TypeError` for an empty name, a name with `/`, or a version that is
 * not a positive integer: a definition is a constant, so this fails when the
 * module loads.
 *
 * @category constructors
 * @since 0.0.0
 */
export const make = <T, I extends Schema.Json>(
  name: string,
  options: SignalOptions<T, I>,
): Signal<T> => {
  if (name === "" || name.includes("/")) {
    throw new TypeError(`Signal name "${name}" must be non-empty, without "/"`);
  }
  if (!Number.isSafeInteger(options.version) || options.version < 1) {
    throw new TypeError(`Signal ${name} version must be a positive integer`);
  }
  return {
    name,
    version: options.version,
    schema: options.schema,
    migrate: options.migrate,
    key: (key) => {
      if (key === "") throw new TypeError(`Signal ${name} key must be non-empty`);
      return { signal: name, key };
    },
  };
};

/**
 * Why a stored signal value cannot be read: its version is newer than the
 * token's, the token has no `migrate` for it, or the migrated value does not
 * match the schema.
 *
 * @category errors
 * @since 0.0.0
 */
export class InvalidSignal extends Schema.TaggedError<InvalidSignal>()("InvalidSignal", {
  signal: Schema.String,
  key: Schema.String,
  message: Schema.String,
}) {}

/**
 * Built in: delivered to `SubmissionDraft.notify` when the submission settles,
 * in the settling commit. Its value is the settled record, what
 * `Submission.await` resolves to.
 *
 * @category built-ins
 * @since 0.0.0
 */
export const SubmissionSettled: Signal<SettledSubmissionRecord> = make("core.submission-settled", {
  version: 1,
  schema: SettledSubmissionRecord,
});

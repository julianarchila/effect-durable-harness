/**
 * Exact operation batches over plain JSON. A `Delta` is an ordered batch of
 * `Operation` tuples: replace the root, set or delete a member, append to or
 * truncate a string, splice an array, or reorder one. The module validates
 * operations and paths, applies batches to mutable or immutable values,
 * computes a compact batch between two immutable revisions, and finds string
 * overlaps.
 *
 * @since 0.1.0
 */
import type * as Result from "effect/Result";
import type * as Schema from "effect/Schema";
import * as internal from "./internal/delta.ts";
import * as internalDiff from "./internal/diff.ts";
import type { DeltaError } from "./internal/errors.ts";

/**
 * One step of a path: an object key or a non-negative integer array index.
 *
 * @category models
 * @since 0.1.0
 */
export type Segment = string | number;

/**
 * A location inside a JSON value, from the root down.
 *
 * @category models
 * @since 0.1.0
 */
export type Path = ReadonlyArray<Segment>;

/**
 * A path with at least one segment, which therefore never addresses the root.
 *
 * @category models
 * @since 0.1.0
 */
export type NonEmptyPath = readonly [Segment, ...Array<Segment>];

/**
 * One decoded operation.
 *
 * **Details**
 *
 * - `["r", value]` replaces the complete value. It is the only operation that
 *   replaces the root.
 * - `["s", path, value]` sets an object property or array element.
 * - `["d", path]` deletes an object property or removes an array element.
 * - `["a", path, text]` appends to a string.
 * - `["t", path, count]` removes `count` UTF-16 code units from the front of a
 *   string.
 * - `["p", path, index, remove, items]` splices an array.
 * - `["m", path, permutation]` reorders an array:
 *   `next[i] = previous[permutation[i]]`.
 *
 * **Gotchas**
 *
 * Operation shape is not canonical: the same change may be expressed with
 * different tuples. Only the value a batch produces is contractual.
 *
 * @category models
 * @since 0.1.0
 */
export type Operation =
  | readonly ["r", Schema.Json]
  | readonly ["s", NonEmptyPath, Schema.Json]
  | readonly ["d", NonEmptyPath]
  | readonly ["a", NonEmptyPath, string]
  | readonly ["t", NonEmptyPath, number]
  | readonly ["p", Path, number, number, ReadonlyArray<Schema.Json>]
  | readonly ["m", Path, ReadonlyArray<number>];

/**
 * An ordered batch of operations that turns one revision into the next.
 *
 * @category models
 * @since 0.1.0
 */
export type Delta = ReadonlyArray<Operation>;

/**
 * Object keys that reach the prototype chain and are therefore rejected as path
 * segments: `__proto__`, `constructor`, and `prototype`.
 *
 * @category constants
 * @since 0.1.0
 */
export const reservedSegments: ReadonlySet<string> = internal.reservedSegments;

// -----------------------------------------------------------------------------
// errors
// -----------------------------------------------------------------------------

export {
  DeltaError,
  InvalidOperationError,
  isDeltaError,
  UnresolvablePathError,
  UnsafePathError,
} from "./internal/errors.ts";

export type { DeltaErrorReason } from "./internal/errors.ts";

// -----------------------------------------------------------------------------
// classification
// -----------------------------------------------------------------------------

/**
 * Returns `true` when the operation replaces the complete value.
 *
 * @category predicates
 * @since 0.1.0
 */
export const isReplace = (operation: { readonly 0: string }): boolean => operation[0] === "r";

/**
 * Returns `true` when a batch begins with a root replacement and is therefore
 * self-contained.
 *
 * **Details**
 *
 * A replacement only ever appears first in a batch, so this test is exact. It
 * works on decoded and encoded batches alike because `["r", value]` encodes to
 * itself.
 *
 * @category predicates
 * @since 0.1.0
 */
export const isBase = (operations: ReadonlyArray<{ readonly 0: string }>): boolean =>
  operations.length > 0 && operations[0]![0] === "r";

// -----------------------------------------------------------------------------
// validation
// -----------------------------------------------------------------------------

/**
 * Validates the verb, arity, path, and payload shape of one decoded operation.
 *
 * **Details**
 *
 * Paths must be inline arrays of safe segments; encoded short forms and path ids
 * are rejected. Payload values are not inspected.
 *
 * @see {@link validateOperationUnsafe} for the asserting variant
 *
 * @category validation
 * @since 0.1.0
 */
export const validateOperation = (u: unknown): Result.Result<Operation, DeltaError> =>
  internal.attempt(() => {
    internal.assertValidOperation(u);
    return u;
  });

/**
 * Asserts that a value is a valid decoded operation, throwing a
 * {@link DeltaError} otherwise.
 *
 * @see {@link validateOperation} for the `Result` variant
 *
 * @category unsafe
 * @since 0.1.0
 */
export const validateOperationUnsafe: (u: unknown) => asserts u is Operation =
  internal.assertValidOperation;

/**
 * Validates that every segment of a path is a safe key or a non-negative
 * integer index.
 *
 * @see {@link validatePathUnsafe} for the asserting variant
 *
 * @category validation
 * @since 0.1.0
 */
export const validatePath = (path: Path): Result.Result<Path, DeltaError> =>
  internal.attempt(() => {
    internal.assertSafePath(path);
    return path;
  });

/**
 * Asserts that every segment of a path is safe, throwing a {@link DeltaError}
 * otherwise.
 *
 * @see {@link validatePath} for the `Result` variant
 *
 * @category unsafe
 * @since 0.1.0
 */
export const validatePathUnsafe: (path: Path) => void = internal.assertSafePath;

// -----------------------------------------------------------------------------
// application
// -----------------------------------------------------------------------------

/**
 * Applies a batch to a mutable value in place and returns the resulting value.
 *
 * **Details**
 *
 * The result is returned because a root replacement cannot happen in place.
 * Every operation is validated before it is applied. Writes define own data
 * properties and never run inherited setters.
 *
 * **Gotchas**
 *
 * - The target is mutated and replacement payloads are adopted, not copied.
 *   Apply one in-memory batch to at most one mutable replica.
 * - On failure, operations before the failing one have already been applied.
 *
 * @see {@link applyUnsafe} for the throwing variant
 * @see {@link applyImmutable} for a non-mutating variant
 *
 * @category application
 * @since 0.1.0
 */
export const apply = <A>(target: A | undefined, operations: Delta): Result.Result<A, DeltaError> =>
  internal.attempt(() => internal.apply(target, operations));

/**
 * Applies a batch to a mutable value in place, throwing a {@link DeltaError} on
 * failure.
 *
 * @see {@link apply} for the `Result` variant
 *
 * @category unsafe
 * @since 0.1.0
 */
export const applyUnsafe: <A>(target: A | undefined, operations: Delta) => A = internal.apply;

/**
 * Applies a batch without mutating the previous value, sharing every container
 * the batch does not touch.
 *
 * **Gotchas**
 *
 * The input, the result, and the batch payloads are immutable by contract and
 * share containers. Mutating any of them corrupts the others.
 *
 * @see {@link applyImmutableUnsafe} for the throwing variant
 * @see {@link applyImmutableBatches} for replaying several batches at once
 *
 * @category application
 * @since 0.1.0
 */
export const applyImmutable = <A>(
  target: A | undefined,
  operations: Delta,
): Result.Result<A, DeltaError> =>
  internal.attempt(() => internal.applyImmutable(target, operations));

/**
 * Applies a batch without mutating the previous value, throwing a
 * {@link DeltaError} on failure.
 *
 * @see {@link applyImmutable} for the `Result` variant
 *
 * @category unsafe
 * @since 0.1.0
 */
export const applyImmutableUnsafe: <A>(target: A | undefined, operations: Delta) => A =
  internal.applyImmutable;

/**
 * Replays several ordered batches without mutating the starting value and
 * returns only the final result.
 *
 * **Details**
 *
 * One copy-on-write scope spans the whole replay, so a container copied for one
 * batch is reused by later ones and no intermediate revision is exposed.
 *
 * @see {@link applyImmutableBatchesUnsafe} for the throwing variant
 *
 * @category application
 * @since 0.1.0
 */
export const applyImmutableBatches = <A>(
  target: A | undefined,
  batches: Iterable<Delta>,
): Result.Result<A, DeltaError> =>
  internal.attempt(() => internal.applyImmutableBatches(target, batches));

/**
 * Replays several ordered batches immutably, throwing a {@link DeltaError} on
 * failure.
 *
 * @see {@link applyImmutableBatches} for the `Result` variant
 *
 * @category unsafe
 * @since 0.1.0
 */
export const applyImmutableBatchesUnsafe: <A>(
  target: A | undefined,
  batches: Iterable<Delta>,
) => A = internal.applyImmutableBatches;

// -----------------------------------------------------------------------------
// diffing
// -----------------------------------------------------------------------------

/**
 * Computes a compact batch that turns one immutable revision into another.
 *
 * **Details**
 *
 * - Identical references and deeply equal values produce no operation.
 * - Growing and sliding strings produce appends and front truncations.
 * - Array edits produce splices and permutations.
 * - When the batch would cost more than the target value, or exceeds the
 *   operation budget, it falls back to `[["r", after]]`.
 *
 * @category diffing
 * @since 0.1.0
 */
export const diff: (before: Schema.Json, after: Schema.Json) => Delta = internalDiff.diff;

/**
 * Returns `text.slice(start, end)` as a string of its own.
 *
 * **Details**
 *
 * V8 represents a slice of 13 or more characters as a view that keeps all of
 * `text` alive. An append op cut from a long, growing string would retain
 * that whole string for as long as the op lives: in a delta chain, a pending
 * queue, or a value built by applying the op. `diff` and the tracker cut
 * their append texts with this.
 *
 * @category diffing
 * @since 0.1.0
 */
export const ownedSlice: (text: string, start: number, end?: number) => string =
  internal.ownedSlice;

/**
 * Returns the length of the longest suffix of `before` that is also a prefix of
 * `after`, searching at most the last `scan` characters of `before`.
 *
 * **Details**
 *
 * The search tries a long head of `after` first and then a single character,
 * verifying each candidate exactly. It gives up after `maxCandidates`
 * candidates per head and returns `0`, which is never wrong, only less compact.
 *
 * @category diffing
 * @since 0.1.0
 */
export const overlap: (
  before: string,
  after: string,
  options: {
    readonly scan: number;
    readonly probe?: number | undefined;
    readonly maxCandidates?: number | undefined;
  },
) => number = internal.overlap;

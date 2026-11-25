/**
 * Immutable revision tracking with revocable drafts. A `Tracker` owns the
 * latest adopted revision of a JSON value. A `Change` opens a mutable draft
 * over that revision; preparing it revokes the draft and computes the next
 * immutable revision together with the exact `Delta` that produces it.
 * Adoption is a pointer swap that also makes every competing change stale.
 *
 * **Gotchas**
 *
 * Immutability is an ownership contract. Nothing is frozen or defensively
 * copied: the value passed to {@link make} or {@link prepareReplace}, every
 * adopted revision, every prepared value, and every operation payload must
 * never be mutated again. A violation is not detected and silently corrupts
 * state.
 *
 * @since 0.1.0
 */
import { dual } from "effect/Function";
import type { Pipeable } from "effect/Pipeable";
import type * as Delta from "./Delta.ts";
import * as internal from "./internal/tracker.ts";

const TypeId = internal.TypeId;

/**
 * A mutable, change-scoped view of a JSON value. Every `readonly` modifier is
 * removed, preserving tuple positions.
 *
 * @category models
 * @since 0.1.0
 */
export type Draft<A, Depth extends ReadonlyArray<unknown> = []> = Depth["length"] extends 8
  ? A
  : A extends null | boolean | number | string
    ? A
    : A extends (...args: ReadonlyArray<never>) => unknown
      ? A
      : A extends object
        ? { -readonly [K in keyof A]: Draft<A[K], [...Depth, unknown]> }
        : A;

/**
 * The keys of `A` whose value type is exactly `string`: required, and not a
 * string literal type. These are the keys {@link appendText} accepts.
 *
 * @category models
 * @since 0.1.0
 */
export type StringKeys<A> =
  A extends ReadonlyArray<unknown>
    ? never
    : {
        [Key in keyof A]: string extends A[Key] ? (A[Key] extends string ? Key : never) : never;
      }[keyof A];

/**
 * Owner of the latest adopted immutable revision of a JSON value.
 *
 * @category models
 * @since 0.1.0
 */
export interface Tracker<in out A extends object> extends Pipeable {
  readonly [TypeId]: typeof TypeId;
  /**
   * The latest adopted revision. Immutable by contract.
   *
   * @since 0.1.0
   */
  readonly value: A;
  /**
   * The number of adoptions so far, including no-op adoptions.
   *
   * @since 0.1.0
   */
  readonly revision: number;
}

/**
 * An open draft over one tracker revision. The draft is usable until the change
 * is prepared or aborted, or until another preparation of the same tracker is
 * adopted.
 *
 * @category models
 * @since 0.1.0
 */
export interface Change<in out A extends object> {
  /**
   * The mutable draft. Reading it after the change settles throws a
   * `TypeError`.
   *
   * @since 0.1.0
   */
  readonly state: Draft<A>;
}

/**
 * A candidate revision computed from a change or a replacement, ready to be
 * adopted by the tracker it came from.
 *
 * @category models
 * @since 0.1.0
 */
export interface Prepared<in out A extends object> {
  /**
   * The revision the change started from.
   *
   * @since 0.1.0
   */
  readonly base: A;
  /**
   * The candidate revision. It shares every untouched container with `base`.
   *
   * @since 0.1.0
   */
  readonly value: A;
  /**
   * The exact batch that turns `base` into `value`. Empty for a no-op, in which
   * case `value` is `base`.
   *
   * @since 0.1.0
   */
  readonly ops: Delta.Delta;
  /**
   * The tracker revision the change started from.
   *
   * @since 0.1.0
   */
  readonly baseRevision: number;
}

/**
 * Checks whether a value is a {@link Tracker}.
 *
 * @category guards
 * @since 0.1.0
 */
export const isTracker: (u: unknown) => u is Tracker<object> = internal.isTracker;

/**
 * Creates a tracker that takes ownership of an initial revision in O(1).
 *
 * **Gotchas**
 *
 * The root must be alias-free, acyclic strict JSON: dense plain arrays, plain or
 * null-prototype objects with enumerable own data properties, strings,
 * booleans, finite numbers, and `null`. It is trusted and never walked, so a
 * violation has unspecified behavior. The caller must not mutate it afterwards.
 *
 * @category constructors
 * @since 0.1.0
 */
export const make: <A extends object>(initial: A) => Tracker<A> = internal.make;

/**
 * Opens a change whose draft overlays the current revision without modifying
 * it.
 *
 * **Details**
 *
 * Several changes may be open at once. A draft may stay open across
 * asynchronous boundaries. Values placed into the draft are validated and
 * copied, so the caller keeps ownership of them.
 *
 * @category combinators
 * @since 0.1.0
 */
export const beginChange: <A extends object>(self: Tracker<A>) => Change<A> = internal.beginChange;

/**
 * Prepares a whole-root replacement.
 *
 * **Details**
 *
 * When the replacement is deeply equal to the current revision, the prepared
 * batch is empty and the current revision is kept. Otherwise the batch is
 * `[["r", value]]` and the prepared value is the replacement itself.
 *
 * @category combinators
 * @since 0.1.0
 */
export const prepareReplace: {
  <A extends object>(value: A): (self: Tracker<A>) => Prepared<A>;
  <A extends object>(self: Tracker<A>, value: A): Prepared<A>;
} = dual(2, internal.prepareReplace);

/**
 * Settles a change: revokes its draft and computes the candidate revision and
 * its exact batch.
 *
 * **Gotchas**
 *
 * Throws when the change is already settled or became stale because another
 * preparation was adopted. Preparation does not change the tracker.
 *
 * @category combinators
 * @since 0.1.0
 */
export const prepare: <A extends object>(self: Change<A>) => Prepared<A> = internal.prepare;

/**
 * Appends `text` to the string at `draft[key]` at O(text) cost.
 *
 * **Details**
 *
 * The change is recorded as one append op, and several appends to the same
 * key in one change join into one op, without comparing the old and new
 * strings. A plain `draft[key] += text` gives the same op, but preparing the
 * change then checks that the new string extends the old one, which reads the
 * whole string: O(length) per change. Streaming writers use this instead.
 *
 * **Gotchas**
 *
 * - A plain assignment or deletion of the key in the same change falls back
 *   to that comparison.
 * - Throws a `TypeError` when `draft` is not an open draft, or when the value
 *   at `key` is not a string.
 *
 * @category combinators
 * @since 0.1.0
 */
export const appendText: {
  <A extends object>(key: StringKeys<A>, text: string): (draft: Draft<A>) => void;
  <A extends object>(draft: Draft<A>, key: StringKeys<A>, text: string): void;
} = dual(3, internal.appendText);

/**
 * Discards a change or a prepared result. Idempotent.
 *
 * **Details**
 *
 * Aborting an open change revokes its draft. Aborting a change after it was
 * prepared, or aborting the prepared result, prevents its adoption.
 *
 * @category combinators
 * @since 0.1.0
 */
export const abort: <A extends object>(self: Change<A> | Prepared<A>) => void = internal.abort;

/**
 * Adopts a prepared result as the tracker's current revision.
 *
 * **Details**
 *
 * Adoption is a pointer swap with no diffing or allocation. It advances
 * `revision`, even for an empty batch, and makes every other open or prepared
 * change of the tracker stale.
 *
 * **Gotchas**
 *
 * Throws for a result that belongs to another tracker, was already adopted,
 * was aborted, or is stale.
 *
 * @category combinators
 * @since 0.1.0
 */
export const adopt: {
  <A extends object>(prepared: Prepared<A>): (self: Tracker<A>) => void;
  <A extends object>(self: Tracker<A>, prepared: Prepared<A>): void;
} = dual(2, internal.adopt);

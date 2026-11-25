/**
 * @since 0.1.0
 * @internal
 */
import * as Data from "effect/Data";
import type { Path, Segment } from "../Delta.ts";

const TypeId = "~@effect-durable-harness/delta/Delta/DeltaError";

/**
 * Reason for a {@link DeltaError} raised by an operation or path that does not
 * have the required shape.
 *
 * @category errors
 * @since 0.1.0
 */
export class InvalidOperationError extends Data.TaggedError("InvalidOperationError")<{
  readonly message: string;
}> {}

/**
 * Reason for a {@link DeltaError} raised by a path segment that could reach the
 * prototype chain, is not a non-negative integer index, or leaves a gap in an
 * array.
 *
 * @category errors
 * @since 0.1.0
 */
export class UnsafePathError extends Data.TaggedError("UnsafePathError")<{
  readonly segment: Segment;
}> {
  /**
   * Describes the rejected segment.
   *
   * @since 0.1.0
   */
  override get message(): string {
    return `unsafe path segment: ${String(this.segment)}`;
  }
}

/**
 * Reason for a {@link DeltaError} raised by a path, or an encoded path id, that
 * does not resolve in the target value or the decoder dictionary.
 *
 * @category errors
 * @since 0.1.0
 */
export class UnresolvablePathError extends Data.TaggedError("UnresolvablePathError")<{
  readonly path: Path | number;
}> {
  /**
   * Describes the unresolved path.
   *
   * @since 0.1.0
   */
  override get message(): string {
    return `unresolvable path: ${JSON.stringify(this.path)}`;
  }
}

/**
 * Union of the reasons a {@link DeltaError} can carry.
 *
 * @category errors
 * @since 0.1.0
 */
export type DeltaErrorReason = InvalidOperationError | UnsafePathError | UnresolvablePathError;

/**
 * Error raised when an operation, path, or encoded batch cannot be validated or
 * applied.
 *
 * **When to use**
 *
 * Use with `Effect.catchReason` or by switching on `reason._tag` to tell
 * malformed input from unsafe or unresolvable paths.
 *
 * @see {@link isDeltaError} for narrowing unknown values to this error type
 *
 * @category errors
 * @since 0.1.0
 */
export class DeltaError extends Data.TaggedError("DeltaError")<{
  readonly reason: DeltaErrorReason;
}> {
  readonly [TypeId]: typeof TypeId = TypeId;

  /**
   * The reason, exposed as the error cause.
   *
   * @since 0.1.0
   */
  override get cause(): DeltaErrorReason {
    return this.reason;
  }

  /**
   * Delegates to the reason's message.
   *
   * @since 0.1.0
   */
  override get message(): string {
    return this.reason.message;
  }
}

/**
 * Checks whether a value is a {@link DeltaError}.
 *
 * @category guards
 * @since 0.1.0
 */
export const isDeltaError = (u: unknown): u is DeltaError =>
  typeof u === "object" && u !== null && TypeId in u;

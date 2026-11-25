/**
 * Compact encoding of `Delta` batches for an ordered stream. An `Encoder`
 * interns paths that repeat across batches and omits a path that repeats the
 * previous operation's path within a batch; a matching `Decoder` restores the
 * decoded operations.
 *
 * **Gotchas**
 *
 * Use one encoder and one decoder per independent state stream. Every decoder
 * must observe exactly the batches of its encoder, in order, starting with a
 * base. A root replacement resets both dictionaries, so it is a recovery point:
 * after a decode or apply failure, discard the decoder and replica and resume
 * from a later base.
 *
 * @since 0.1.0
 */
import { dual } from "effect/Function";
import type * as Result from "effect/Result";
import type * as Schema from "effect/Schema";
import type * as Delta from "./Delta.ts";
import * as internalDelta from "./internal/delta.ts";
import type { DeltaError } from "./internal/errors.ts";
import * as internal from "./internal/wire.ts";

/**
 * A path written inline, or the id an encoder assigned to it on its second
 * use.
 *
 * @category models
 * @since 0.1.0
 */
export type PathRef<P extends Delta.Path = Delta.Path> = P | number;

/**
 * One encoded operation.
 *
 * **Details**
 *
 * The encoded vocabulary adds two compressions to `Delta.Operation`:
 *
 * - `["#", id, path]` defines an id, emitted on a path's second use, and a
 *   numeric `PathRef` references it.
 * - A shortened tuple, without its path, reuses the previous operation's path in
 *   the same batch; arity disambiguates it.
 *
 * `["r", value]` carries no path and encodes to itself.
 *
 * @category models
 * @since 0.1.0
 */
export type Operation =
  | readonly ["r", Schema.Json]
  | readonly ["s", PathRef<Delta.NonEmptyPath>, Schema.Json]
  | readonly ["s", Schema.Json]
  | readonly ["d", PathRef<Delta.NonEmptyPath>]
  | readonly ["d"]
  | readonly ["a", PathRef<Delta.NonEmptyPath>, string]
  | readonly ["a", string]
  | readonly ["t", PathRef<Delta.NonEmptyPath>, number]
  | readonly ["t", number]
  | readonly ["p", PathRef, number, number, ReadonlyArray<Schema.Json>]
  | readonly ["p", number, number, ReadonlyArray<Schema.Json>]
  | readonly ["m", PathRef, ReadonlyArray<number>]
  | readonly ["m", ReadonlyArray<number>]
  | readonly ["#", number, Delta.Path];

/**
 * Stateful encoder for one ordered stream of batches.
 *
 * @category models
 * @since 0.1.0
 */
export interface Encoder {
  readonly [internal.EncoderTypeId]: typeof internal.EncoderTypeId;
}

/**
 * Stateful decoder for one ordered stream of encoded batches.
 *
 * @category models
 * @since 0.1.0
 */
export interface Decoder {
  readonly [internal.DecoderTypeId]: typeof internal.DecoderTypeId;
}

/**
 * Creates an encoder with empty path dictionaries.
 *
 * @category constructors
 * @since 0.1.0
 */
export const makeEncoder: () => Encoder = internal.makeEncoder;

/**
 * Creates a decoder with empty path dictionaries.
 *
 * @category constructors
 * @since 0.1.0
 */
export const makeDecoder: () => Decoder = internal.makeDecoder;

/**
 * Encodes one batch, advancing the encoder's path dictionary.
 *
 * **Details**
 *
 * A path is written inline on first use and interned on its second use, since
 * a definition costs more than the path it replaces. Omission of a repeated
 * path is scoped to one batch; ids are the only state that spans batches.
 *
 * @category encoding
 * @since 0.1.0
 */
export const encode: {
  (operations: Delta.Delta): (self: Encoder) => Array<Operation>;
  (self: Encoder, operations: Delta.Delta): Array<Operation>;
} = dual(2, internal.encode);

/**
 * Validates and decodes one encoded batch, advancing the decoder's path
 * dictionary.
 *
 * **Gotchas**
 *
 * Payloads are not copied. Decoding one in-memory batch for two mutable
 * replicas makes them alias each other.
 *
 * @see {@link decodeUnsafe} for the throwing variant
 *
 * @category decoding
 * @since 0.1.0
 */
export const decode: {
  (
    wire: ReadonlyArray<unknown>,
  ): (self: Decoder) => Result.Result<Array<Delta.Operation>, DeltaError>;
  (self: Decoder, wire: ReadonlyArray<unknown>): Result.Result<Array<Delta.Operation>, DeltaError>;
} = dual(
  2,
  (
    self: Decoder,
    wire: ReadonlyArray<unknown>,
  ): Result.Result<Array<Delta.Operation>, DeltaError> =>
    internalDelta.attempt(() => internal.decode(self, wire)),
);

/**
 * Validates and decodes one encoded batch, throwing a `DeltaError` on failure.
 *
 * @see {@link decode} for the `Result` variant
 *
 * @category unsafe
 * @since 0.1.0
 */
export const decodeUnsafe: {
  (wire: ReadonlyArray<unknown>): (self: Decoder) => Array<Delta.Operation>;
  (self: Decoder, wire: ReadonlyArray<unknown>): Array<Delta.Operation>;
} = dual(2, internal.decode);

/**
 * Validates the verb, arity, path references, and payload shape of one encoded
 * operation. Payload values are not inspected.
 *
 * @see {@link validateOperationUnsafe} for the asserting variant
 *
 * @category validation
 * @since 0.1.0
 */
export const validateOperation = (u: unknown): Result.Result<Operation, DeltaError> =>
  internalDelta.attempt(() => {
    internal.assertValidOperation(u);
    return u;
  });

/**
 * Asserts that a value is a valid encoded operation, throwing a `DeltaError`
 * otherwise.
 *
 * @see {@link validateOperation} for the `Result` variant
 *
 * @category unsafe
 * @since 0.1.0
 */
export const validateOperationUnsafe: (u: unknown) => asserts u is Operation =
  internal.assertValidOperation;

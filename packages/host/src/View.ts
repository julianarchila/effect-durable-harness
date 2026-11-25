/**
 * What a conversation's view socket carries: a read-only stream of the conversation's
 * `watch`, as JSON text messages.
 *
 * The first message after the socket connects, and after every restart of the object, is a `snapshot` of the
 * conversation's view. Each later message is an `ops` batch of `@effect-durable-harness/delta` operations, wire-encoded with one
 * encoder per snapshot, that turns the previous value into the next. A client keeps one `Reader`: a snapshot
 * resets it.
 *
 * @since 0.0.0
 */
import * as Delta from "@effect-durable-harness/delta/Delta";
import * as Wire from "@effect-durable-harness/delta/Wire";
import type { ConversationView } from "@effect-durable-harness/core/View";
import * as Result from "effect/Result";

/**
 * One message of the view socket.
 *
 * @category models
 * @since 0.0.0
 */
export type ViewMessage =
  | { readonly type: "snapshot"; readonly value: ConversationView | null }
  | { readonly type: "ops"; readonly ops: ReadonlyArray<Wire.Operation> };

/**
 * A client's copy of a conversation view, kept current from the socket's messages.
 *
 * @category models
 * @since 0.0.0
 */
export interface Reader {
  /** The latest value; `null` before the first snapshot or for a retired conversation. */
  readonly value: () => ConversationView | null;
  /** Applies one message; fails on a malformed batch, after which the client should reconnect. */
  readonly apply: (
    message: ViewMessage,
  ) => Result.Result<ConversationView | null, Delta.DeltaError>;
}

/**
 * Makes a `Reader` with no value yet.
 *
 * @category constructors
 * @since 0.0.0
 */
export const makeReader = (): Reader => {
  let value: ConversationView | null = null;
  let decoder = Wire.makeDecoder();
  return {
    value: () => value,
    apply: (message) => {
      if (message.type === "snapshot") {
        decoder = Wire.makeDecoder();
        value = message.value;
        return Result.succeed(value);
      }
      return Wire.decode(decoder, message.ops).pipe(
        Result.flatMap((ops) => Delta.applyImmutable<ConversationView | null>(value, ops)),
        Result.map((next) => {
          value = next;
          return next;
        }),
      );
    },
  };
};

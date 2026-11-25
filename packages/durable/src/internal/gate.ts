/**
 * Admission of storage operations and the close that waits for them.
 *
 * Calls after close are refused, and admitted multi-query reads finish before
 * the database closes. Close is the layer's scope finalizer.
 *
 * @internal
 */
import * as Effect from "effect/Effect";
import * as Latch from "effect/Latch";
import { Closed, StorageFailure } from "../AgentStorage.ts";

export interface Gate {
  /** Run `operation` unless the storage is closed; close waits for it. */
  readonly admit: <A, E, R>(
    operation: Effect.Effect<A, E, R>,
  ) => Effect.Effect<A, E | StorageFailure, R>;
  /** Refuse new operations and wait for the admitted ones. */
  readonly close: Effect.Effect<void>;
}

export const openGate = (name: string): Effect.Effect<Gate> =>
  Effect.sync(() => {
    let closed = false;
    let admitted = 0;
    const drained = Latch.makeUnsafe(true);
    const release = Effect.sync(() => {
      admitted--;
      // oxlint-disable-next-line durable/no-inline-wakeup -- the end of the last admitted call, for close.
      if (admitted === 0) drained.openUnsafe();
    });
    return {
      admit: <A, E, R>(operation: Effect.Effect<A, E, R>) =>
        Effect.suspend((): Effect.Effect<A, E | StorageFailure, R> => {
          if (closed) {
            return Effect.fail(
              new StorageFailure({ reason: new Closed({ message: `${name} is closed` }) }),
            );
          }
          admitted++;
          if (admitted === 1) drained.closeUnsafe();
          return Effect.ensuring(operation, release);
        }),
      close: Effect.suspend(() => {
        closed = true;
        return drained.await;
      }),
    };
  });

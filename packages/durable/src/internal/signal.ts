// Wakeups that resume their waiters off the stack of whoever wakes them.
//
// A fiber waiting on a `Deferred` resumes inline, on the completer's stack, before `Deferred.doneUnsafe`
// returns (`effect/Deferred.ts:857-871`). So a waiter woken in the middle of a
// commit's publication would run before the later listeners, and a long chain
// of wakeups would grow the stack. Every wakeup that can land in the middle of
// another operation goes through these two primitives, which make the waiter
// yield first. A `done` that only marks the end of an invocation or of close,
// which a joiner awaits, stays inline: nothing else runs on that stack after it.
//
// The yield is not an ordering guarantee: what the waiter must not see half
// done is finished in the same synchronous step that wakes it (mutate, then
// signal).
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import type * as Fiber from "effect/Fiber";
import type * as Scope from "effect/Scope";

/**
 * Completes `deferred` with `effect`; each waiter yields before it runs `effect`, so it resumes off the
 * completer's stack. Returns whether this call completed it.
 */
export const doneAfterYield = <A, E>(
  deferred: Deferred.Deferred<A, E>,
  effect: Effect.Effect<A, E>,
): boolean => Deferred.doneUnsafe(deferred, Effect.andThen(Effect.yieldNow, effect));

/** A level-triggered signal one loop fiber waits for; firing it any number of times wakes it once. */
export interface Signal {
  /** Wakes the waiting loop after a yield; synchronous, so it can be fired from a commit listener. */
  readonly fire: () => void;
  /** Waits for the next fire, or returns at once if one came since the last wait. */
  readonly wait: Effect.Effect<void>;
}

/** Makes a signal. */
export const make = (): Signal => {
  let current = Deferred.makeUnsafe<void>();
  return {
    fire: () => {
      doneAfterYield(current, Effect.void);
    },
    wait: Effect.suspend(() => Deferred.await(current)).pipe(
      Effect.andThen(
        Effect.sync(() => {
          current = Deferred.makeUnsafe<void>();
        }),
      ),
    ),
  };
};

/**
 * Forks a fiber into `scope` that runs `body` after each wakeup of `signal`, forever. A
 * fire while `body` runs wakes it once more after it returns.
 */
export const forkLoop = (
  signal: Signal,
  body: Effect.Effect<void>,
  scope: Scope.Scope,
): Effect.Effect<Fiber.Fiber<never>> =>
  Effect.forkIn(Effect.forever(signal.wait.pipe(Effect.andThen(body))), scope);

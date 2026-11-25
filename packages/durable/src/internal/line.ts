// The Session's mutation line: a FIFO lock in call order. Jobs run on the caller's fiber, with its context
// and its interruption.
//
// `Semaphore` does not promise FIFO order: a release schedules the scan of its
// waiters, and a fiber that asks for a permit before that scan runs takes it
// first (`effect/Semaphore.ts:122-127`, `:262-276`). An actor would give FIFO
// order but run jobs on its own fiber.
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import { doneAfterYield } from "./signal.ts";

export interface Line {
  /**
   * Runs `job` once every job queued before it has finished. `refuse` is checked in the same synchronous
   * step that takes the line or queues for it, so a job is either queued before the condition holds or
   * refused with its error; no yield falls between the two.
   */
  readonly run: <A, E, R, X>(
    job: Effect.Effect<A, E, R>,
    refuse: () => X | undefined,
  ) => Effect.Effect<A, E | X, R>;
}

/** Makes an idle line. */
export const make = (): Line => {
  let locked = false;
  const turns: Array<Deferred.Deferred<void>> = [];
  const unlock = () => {
    const next = turns.shift();
    if (next === undefined) locked = false;
    // The next waiter would resume inline; the yield starts its job on a fresh stack, so a long queue of jobs
    // that never suspend cannot grow the stack until it overflows.
    else doneAfterYield(next, Effect.void);
  };
  return {
    // Taking the lock and registering its release run uninterruptibly, so an interruption cannot land
    // between them and leak the line; only the wait for a turn, and the job itself, are interruptible.
    run: <A, E, R, X>(job: Effect.Effect<A, E, R>, refuse: () => X | undefined) =>
      Effect.uninterruptibleMask((restore) => {
        const run = Effect.suspend(() => restore(job).pipe(Effect.ensuring(Effect.sync(unlock))));
        return Effect.suspend((): Effect.Effect<A, E | X, R> => {
          const refused = refuse();
          if (refused !== undefined) return Effect.fail(refused);
          if (!locked) {
            locked = true;
            return run;
          }
          const turn = Deferred.makeUnsafe<void>();
          turns.push(turn);
          return restore(Deferred.await(turn)).pipe(
            Effect.onInterrupt(() =>
              Effect.sync(() => {
                const index = turns.indexOf(turn);
                // Interrupted after the lock was handed over: pass it on.
                if (index === -1) unlock();
                else turns.splice(index, 1);
              }),
            ),
            Effect.andThen(run),
          );
        });
      }),
  };
};

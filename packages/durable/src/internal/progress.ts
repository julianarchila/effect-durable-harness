// Adaptive progress commits of a tool's output. The first change after an idle period commits at
// once; each commit then delays the next by at least `minIntervalMs` and by its
// written size at `PROGRESS_BYTES_PER_SECOND`. At most one commit is in flight;
// changes made meanwhile coalesce into the next one.
//
// `Progress` is an Effect primitive of its own, apart from the pure bounding,
// so `output.ts` stays synchronous and Effect-free.
//
// The order of decisions matters, so each decision below is a synchronous
// function that checks and registers in one step and returns the Effect that
// carries it out:
//
// - `mark` with no pause pending flushes in the caller's step: `write()` is
//   called there, so what the commit writes is captured before the caller's
//   next chunk, and the commit's fiber starts immediately.
// - `markAndWait` runs its caller's `admit` check, registers its waiter and
//   marks in one step, so a flush it triggers includes it, and a caller that
//   checks `ended` there never registers a waiter after `stop`. A flush takes the waiters registered so far; one
//   added while a commit is in flight belongs to the next commit.
// - The finished commit settles its waiters (after a yield, `signal.ts`)
//   before the next flush is scheduled.
// - `stop` cancels the pending pause, awaits the commit in flight and returns
//   the waiters no commit took: the
//   tool's terminal commit settles them.
import * as Cause from "effect/Cause";
import * as Clock from "effect/Clock";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Scope from "effect/Scope";
import { PROGRESS_BYTES_PER_SECOND } from "./output.ts";
import { doneAfterYield } from "./signal.ts";

/** A `markAndWait` caller no commit has settled yet; `stop` hands these to the final commit. */
export interface ProgressWaiter<E> {
  /** Settles the wait as committed. */
  readonly resolve: () => void;
  /** Fails the wait with `error`. */
  readonly reject: (error: E) => void;
  /** Ends the wait interrupted. */
  readonly interrupt: () => void;
}

/** A pause before the next commit. */
interface Pause {
  fiber: Fiber.Fiber<void> | undefined;
}

export interface ProgressOptions<E> {
  /**
   * Starts one progress commit and returns it. The call itself runs in the step that flushes, which may be the
   * caller of `mark`: capture what the commit writes there, synchronously, not inside the returned Effect. The
   * Effect succeeds with the bytes it wrote, which buy the pause before the next commit. The call must not throw:
   * a failure belongs in the Effect.
   */
  readonly write: () => Effect.Effect<number, E>;
  /** Observes a failed commit, after its waiters are rejected and before the next commit is scheduled. */
  readonly onError: (cause: Cause.Cause<E>) => Effect.Effect<void>;
  /** Minimum pause after each commit. */
  readonly minIntervalMs: number;
}

export interface Progress<E> {
  /** Schedules a commit; with no pause pending and none in flight, it starts in this step. */
  readonly mark: Effect.Effect<void>;
  /**
   * Schedules a commit and waits for the commit that includes this change; fails with that commit's failure.
   * Interrupting the wait cancels only the wait: the change stays marked. `admit` runs in the step that
   * registers the wait, before it: it makes the change, or returns an error that refuses the wait at once. A
   * wait admitted after `stop` is a defect: no commit would settle it.
   */
  readonly markAndWait: <F = never>(admit?: () => F | undefined) => Effect.Effect<void, E | F>;
  /** Stops committing and waits for the commit in flight; returns the waiters the final commit must settle. */
  readonly stop: Effect.Effect<ReadonlyArray<ProgressWaiter<E>>>;
}

/**
 * Makes a progress throttle whose fibers live in the current scope. Closing the scope stops it as `stop` does
 * and interrupts the waits left, so a handler-local region never lets a commit land after it.
 */
export const make = <E>(
  options: ProgressOptions<E>,
): Effect.Effect<Progress<E>, never, Scope.Scope> =>
  Effect.gen(function* () {
    const clock = yield* Clock.Clock;
    // Forked before the stop finalizer is added, so the scope closes it after `stop` ran: the commit in flight
    // is awaited, not interrupted.
    const fibers = yield* Scope.fork(yield* Effect.scope);
    let waiters: Array<Deferred.Deferred<void, E>> = [];
    /** The pending pause; its fiber is set once forked. */
    let timer: Pause | undefined;
    /** Completed when the commit in flight has settled its waiters. */
    let inFlight: Deferred.Deferred<void> | undefined;
    let nextAt = 0;
    let dirty = false;
    let stopped = false;

    // Synchronous: the caller runs the returned Effect uninterruptibly, so a decision
    // registered here is always carried out.
    const scheduleUnsafe = (): Effect.Effect<void> => {
      if (stopped || timer !== undefined || inFlight !== undefined) return Effect.void;
      const wait = nextAt - clock.currentTimeMillisUnsafe();
      if (wait <= 0) return flushUnsafe();
      const pause: Pause = { fiber: undefined };
      timer = pause;
      // The flush after the sleep is uninterruptible, as its decision is carried out by the Effect it returns;
      // the sleep is not, so `stop` cancels the pause.
      return Effect.uninterruptibleMask((restore) =>
        restore(Effect.sleep(wait)).pipe(
          Effect.andThen(
            Effect.suspend(() => {
              if (timer === pause) timer = undefined;
              return flushUnsafe();
            }),
          ),
        ),
      ).pipe(
        Effect.forkIn(fibers),
        Effect.tap((fiber) =>
          Effect.sync(() => {
            pause.fiber = fiber;
          }),
        ),
        Effect.asVoid,
      );
    };

    // Takes the dirty flag and the waiters, and calls `write`, in the caller's step.
    const flushUnsafe = (): Effect.Effect<void> => {
      if (stopped || !dirty) return Effect.void;
      dirty = false;
      const taken = waiters;
      waiters = [];
      const started = clock.currentTimeMillisUnsafe();
      const done = Deferred.makeUnsafe<void>();
      inFlight = done;
      const commit = options.write();
      return Effect.exit(commit).pipe(
        Effect.flatMap((exit) => {
          if (Exit.isSuccess(exit)) {
            nextAt =
              started +
              Math.max(options.minIntervalMs, (exit.value * 1000) / PROGRESS_BYTES_PER_SECOND);
            for (const waiter of taken) doneAfterYield(waiter, Effect.void);
            return finishUnsafe(done);
          }
          nextAt = started + options.minIntervalMs;
          for (const waiter of taken) doneAfterYield(waiter, Effect.failCause(exit.cause));
          return options
            .onError(exit.cause)
            .pipe(Effect.andThen(Effect.suspend(() => finishUnsafe(done))));
        }),
        Effect.forkIn(fibers, { startImmediately: true }),
        Effect.asVoid,
      );
    };

    // The commit is over; changes made meanwhile get the next one.
    const finishUnsafe = (done: Deferred.Deferred<void>): Effect.Effect<void> => {
      inFlight = undefined;
      doneAfterYield(done, Effect.void);
      return dirty ? scheduleUnsafe() : Effect.void;
    };

    const stop: Effect.Effect<ReadonlyArray<ProgressWaiter<E>>> = Effect.suspend(() => {
      stopped = true;
      const pause = timer;
      timer = undefined;
      const current = inFlight;
      return Effect.gen(function* () {
        // A pause not forked yet runs in `fibers`; its flush sees `stopped` and does nothing.
        if (pause?.fiber !== undefined) yield* Fiber.interrupt(pause.fiber);
        if (current !== undefined) yield* Deferred.await(current);
        const left = waiters;
        waiters = [];
        return left.map(
          (waiter): ProgressWaiter<E> => ({
            resolve: () => {
              doneAfterYield(waiter, Effect.void);
            },
            reject: (error) => {
              doneAfterYield(waiter, Effect.fail(error));
            },
            interrupt: () => {
              doneAfterYield(waiter, Effect.interrupt);
            },
          }),
        );
      });
    });

    // A wait no one settled ends interrupted, never pending: its caller is part of the closing region.
    yield* Effect.addFinalizer(() =>
      Effect.map(stop, (left) => {
        for (const waiter of left) waiter.interrupt();
      }),
    );

    return {
      mark: Effect.uninterruptible(
        Effect.suspend(() => {
          dirty = true;
          return scheduleUnsafe();
        }),
      ),
      markAndWait: <F = never>(admit?: () => F | undefined) =>
        Effect.uninterruptibleMask((restore) =>
          Effect.suspend((): Effect.Effect<void, E | F> => {
            const refused = admit?.();
            if (refused !== undefined) return Effect.fail(refused);
            if (stopped) return Effect.die(new Error("Progress wait after stop"));
            // The waiter is registered before the mark, so a flush in this step takes it.
            const waiter = Deferred.makeUnsafe<void, E>();
            waiters.push(waiter);
            dirty = true;
            return scheduleUnsafe().pipe(
              Effect.andThen(
                restore(Deferred.await(waiter)).pipe(
                  // A cancelled wait leaves the list, so `stop` does not hand back a wait nobody awaits. The mark
                  // stays: the update is still committed.
                  Effect.onInterrupt(() =>
                    Effect.sync(() => {
                      waiters = waiters.filter((entry) => entry !== waiter);
                    }),
                  ),
                ),
              ),
            );
          }),
        ),
      stop,
    };
  });

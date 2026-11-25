// The watch lifecycle, the buffer behind a document watch and a conversation view watch; the follower shape the
// Session and the view mounts drive; and `guardObserver`, which puts a host's synchronous `Observer` behind the
// same shape.
//
// A watch is a single-consumer pull stream. Pulling a frame is the listener call: what accumulated since the
// previous pull arrives as one frame, folded by `PendingOps`.
import type * as Delta from "@effect-durable-harness/delta/Delta";
import * as PendingOps from "@effect-durable-harness/delta/PendingOps";
import * as Cause from "effect/Cause";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Result from "effect/Result";
import type * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import type { DocumentWatch, WatchFrame } from "../Doc.ts";
import type { ObserverEnd } from "../Observer.ts";
import { SessionClosed, type SessionFailed } from "../SessionError.ts";
import { doneAfterYield } from "./signal.ts";

/**
 * What a source drives on a watch or a guarded observer: its changes, its owner's end, and the Session's end.
 */
export interface Follower<T> {
  /** One change; called synchronously on the line, inside the commit's publication. */
  readonly advance: (frame: WatchFrame<T>) => void;
  /**
   * The owner of what it follows ended (a task document's task became terminal), right after that commit's
   * changes: nothing can change the address again.
   */
  readonly ownerEnded: () => void;
  /** The Session closed (no failure) or failed. Called synchronously when close begins. */
  readonly closeSession: (failure: SessionFailed | undefined) => void;
}

/** The empty batch of a retirement call: the watch keeps the last value it was given. */
export const NO_OPERATIONS: Delta.Delta = [];

/** A watch whose `value` the consumer's pulls advance. */
interface MutableWatch<T> {
  value: Readonly<T>;
  readonly frames: DocumentWatch<T>["frames"];
}

type WatchEnd =
  | { readonly _tag: "Ended" }
  | { readonly _tag: "Failed"; readonly error: SessionFailed };

/**
 * The buffer behind one watch. Frames wait until the single consumer
 * pulls; the pull gets the newest value, the folded ops of every frame since the previous pull (or a root
 * replacement of the newest value once more than `PendingOps.maxPendingOps` are pending), and `retired`
 * when any of them was a retirement. A frame pulled alone is passed on as the source made it.
 */
export const watchObserver = <T>(
  initial: Readonly<T>,
  detach: () => void,
): Follower<T> & { readonly watch: DocumentWatch<T>; readonly stop: () => void } => {
  // Termination drops every reference the watch holds into its source: a stopped watch that
  // is still referenced keeps only its last value alive.
  let release: (() => void) | undefined = detach;
  const ops = PendingOps.make();
  // The source's frame while it is the only one pending, passed on without allocation.
  let lone: WatchFrame<T> | undefined;
  let newest: { readonly value: Readonly<T> } | undefined;
  let retired = false;
  // Set once the source sends nothing more; the watch ends once the rest is pulled.
  let ending = false;
  let end: WatchEnd | undefined;
  let waiter: Deferred.Deferred<void> | undefined;

  const wake = () => {
    const current = waiter;
    waiter = undefined;
    // The yield lets the commit finish its publication first, so consumers run off the line, after every
    // commit listener: delivery is never inline.
    if (current !== undefined) doneAfterYield(current, Effect.void);
  };
  const drop = () => {
    PendingOps.take(ops);
    lone = undefined;
    newest = undefined;
    retired = false;
  };
  // The first ending wins; pending frames are dropped.
  const terminate = (reason: WatchEnd) => {
    if (end !== undefined) return;
    end = reason;
    drop();
    const released = release;
    release = undefined;
    released?.();
    wake();
  };

  const next = (): WatchFrame<T> => {
    // SAFETY: called only while a frame is pending, which sets `newest`.
    const value = newest!.value;
    const single = lone;
    const batch = PendingOps.take(ops);
    const wasRetired = retired;
    lone = undefined;
    newest = undefined;
    retired = false;
    // One frame never folds or overflows.
    if (single !== undefined) return single;
    return {
      value,
      // SAFETY: the values of a watch are JSON, documents or views; `T` is the caller's typed view of them.
      ops: batch._tag === "Ops" ? batch.ops : [["r", value as Schema.Json]],
      retired: wasRetired,
    };
  };

  const watch: MutableWatch<T> = {
    value: initial,
    frames: Stream.fromEffectRepeat(
      Effect.suspend(function pull(): Effect.Effect<WatchFrame<T>, SessionFailed | Cause.Done> {
        if (end !== undefined) {
          return end._tag === "Failed" ? Effect.fail(end.error) : Cause.done();
        }
        if (newest !== undefined) {
          const frame = next();
          watch.value = frame.value;
          return Effect.succeed(frame);
        }
        // A watch whose source ended ends once it delivered the rest.
        if (ending) {
          terminate({ _tag: "Ended" });
          return Cause.done();
        }
        const wait = Deferred.makeUnsafe<void>();
        waiter = wait;
        return Deferred.await(wait).pipe(Effect.andThen(Effect.suspend(pull)));
      }),
    ).pipe(
      // A listener failure ends that watch, and so does cancellation. The consumer is
      // the watch's only listener: when it fails or is interrupted, the watch stops. A consumer that ends
      // normally (`Stream.take`) leaves the watch and what is pending for the next one.
      Stream.onExit((exit) =>
        exit._tag === "Failure" ? Effect.sync(() => terminate({ _tag: "Ended" })) : Effect.void,
      ),
    ),
  };

  return {
    watch,
    advance: (frame) => {
      if (end !== undefined || ending) return;
      lone = newest === undefined ? frame : undefined;
      PendingOps.push(ops, frame.ops);
      newest = { value: frame.value };
      if (frame.retired) retired = true;
      wake();
    },
    ownerEnded: () => {
      if (end !== undefined || ending) return;
      ending = true;
      wake();
    },
    // Close or failure ends the watch at once, dropping what is pending.
    closeSession: (failure) =>
      terminate(failure === undefined ? { _tag: "Ended" } : { _tag: "Failed", error: failure }),
    stop: () => terminate({ _tag: "Ended" }),
  };
};

/** The end of an observer whose observed owner ended. */
export const ENDED: ObserverEnd = { _tag: "Ended" };

/**
 * A Session's close ends a synchronous observer `Failed`, so an adapter follows the next Session; a failure ends it
 * with the error that failed the Session. Watches end `Ended` instead (`watchObserver`).
 */
// Each observer gets its own `SessionClosed`: a new error per close, which is what callers can tell.
export const closeEnd = (failure: SessionFailed | undefined): ObserverEnd => ({
  _tag: "Failed",
  error: failure ?? new SessionClosed(),
});

/** A synchronous observer as the Session attaches it: an `Observer` (frames) or a `CommitObserver` (publications). */
interface SyncObserver<A> {
  readonly advance: (call: A) => void;
  readonly end: (end: ObserverEnd) => void;
}

/** A guarded observer and its observation's detach. */
export interface Guarded<A> {
  readonly advance: (call: A) => void;
  readonly end: (end: ObserverEnd) => void;
  readonly detach: () => void;
}

/** How a guard reports a throw and runs an Effect an observer returned. */
export interface GuardServices {
  /** Reports a listener error (`listener_error`). */
  readonly report: (error: Error) => void;
  /** Runs an Effect an observer returned in the background, with the Session's services. */
  readonly runReturned: (effect: Effect.Effect<void>) => void;
}

/**
 * `observer`, a host's, behind the rule for observers that run at a commit's publication. One that throws, or
 * whose `advance` returns an Effect that fails, is reported, detached and ended `ListenerError`, so the throw
 * never reaches the publication; nothing reaches it after its detach or end, and from then on the guard
 * references neither it nor its source. `detach` is the source's.
 */
// An Effect does nothing until it runs, so a returned Effect is run in the background (`runReturned`), and its
// failure is the observer's throw, after the fact.
export const guardObserver = <A>(
  observer: SyncObserver<A>,
  detach: () => void,
  services: GuardServices,
): Guarded<A> => {
  interface Attached {
    readonly observer: SyncObserver<A>;
    readonly detach: () => void;
    readonly services: GuardServices;
  }
  let attached: Attached | undefined = { observer, detach, services };
  /** Detaches from the source once, and hands back what it was attached with. */
  const release = (): Attached | undefined => {
    const released = attached;
    attached = undefined;
    released?.detach();
    return released;
  };
  /** Reports a throw of `current`'s `advance`, then detaches and ends it, unless that happened meanwhile. */
  const fault = (current: Attached, error: Error) => {
    current.services.report(error);
    if (release() === undefined) return;
    const end: ObserverEnd = { _tag: "ListenerError", error };
    contained(() => current.observer.end(end), current.services.report);
  };
  return {
    advance: (call) => {
      const current = attached;
      if (current === undefined) return;
      // A listener's throw as the `Error` a `ListenerError` end carries.
      const result = Result.try({
        try: () => current.observer.advance(call),
        catch: (error) => (error instanceof Error ? error : new Error(String(error))),
      });
      if (Result.isFailure(result)) {
        fault(current, result.failure);
        return;
      }
      // The callback is typed to return nothing; an Effect it returns anyway is its asynchronous work.
      const returned: unknown = result.success;
      if (!Effect.isEffect(returned)) return;
      // SAFETY: only its failure is read, and only as an error to report; a service it lacks is a defect,
      // which is its failure too. Its channels are unknown by nature: the callback is typed to return nothing.
      // @effect-diagnostics-next-line anyUnknownInErrorContext:off unsafeEffectTypeAssertion:off
      const work = returned as Effect.Effect<void, Error>;
      current.services.runReturned(
        Effect.catchCause(work, (cause) =>
          Cause.hasInterruptsOnly(cause)
            ? Effect.void
            : Effect.sync(() => {
                const error = Cause.squash(cause);
                fault(current, error instanceof Error ? error : new Error(String(error)));
              }),
        ),
      );
    },
    end: (end) => {
      const ending = release();
      if (ending !== undefined) contained(() => ending.observer.end(end), ending.services.report);
    },
    detach: () => {
      release();
    },
  };
};

/** A guarded document or view observer as a `Follower`: its owner's end is `Ended`, the Session's close `closeEnd`. */
export const guardedFollower = <T>(guarded: Guarded<WatchFrame<T>>): Follower<T> => ({
  advance: guarded.advance,
  ownerEnded: () => guarded.end(ENDED),
  closeSession: (failure) => guarded.end(closeEnd(failure)),
});

/** Runs a callback that must not fail its caller; a throw goes to `onError`. */
const contained = (callback: () => void, onError: (error: Error) => void) => {
  const result = Result.try({
    try: callback,
    catch: (error) => (error instanceof Error ? error : new Error(String(error))),
  });
  if (Result.isFailure(result)) onError(result.failure);
};

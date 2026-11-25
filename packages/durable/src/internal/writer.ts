// The Session's writer. The line admits each commit as a part once
// its callback and settlement succeeded; the writer sends every part admitted while no call is in flight as one
// `AgentStorage.commit` call, off the line, and publishes the parts of a call one by one, in line order, with a
// pause between two, so the continuations of the waits one resolves run before the next is published.
//
// Each part registers the keys it writes; until it is published, it is the newest unpublished writer of each,
// and a job that touches one waits for its gate. A key leaves when its newest writer is published, so the map
// holds one entry per key with an unpublished writer.
//
// The writer is one loop fiber on a `Signal`; a call and its publication run on it, so calls never overlap.
import * as Cause from "effect/Cause";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import type * as Fiber from "effect/Fiber";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import type * as Scope from "effect/Scope";
import { type CommitResult, StorageFailure, type StorageWrite } from "../AgentStorage.ts";
import type { Seq } from "../Ids.ts";
import { SessionFailed } from "../SessionError.ts";
import * as Signal from "./signal.ts";
import { doneAfterYield } from "./signal.ts";

/** A part's own failure at publication: memory could not adopt it, or one of the Session's own observers threw. */
export interface PublishFailure {
  readonly _tag: "Adoption" | "Listener";
  readonly error: Error;
}

/** What a part's caller gets when it fails: its own error (typed or a defect), or `SessionFailed`. */
export type PartError = StorageFailure | SessionFailed;

/**
 * One logical commit the line admitted: stored (`writes`, never empty), or held in memory only (no `writes`), a
 * deferred commit published once the parts before it are.
 */
export interface PartSpec {
  /** Line index of the job that admitted it: a failure it causes makes only later jobs' outcomes `SessionFailed`. */
  readonly index: number;
  readonly writes: ReadonlyArray<StorageWrite> | undefined;
  /** Keys it writes: a read of one waits for its publication. */
  readonly keys: ReadonlyArray<string>;
  /** Publishes it in memory, in line order, once it is durable (with its seq) or, held, once the parts before it are. */
  readonly publish: (seq: Seq | undefined) => PublishFailure | undefined;
  /** It will never be published: abort what it prepared. */
  readonly discard: () => void;
  /**
   * Called once, synchronously, with its outcome: once it is published, or once it never will be; after
   * publication it can still fail, when its storage call failed or the Session failed meanwhile.
   */
  readonly settle: (exit: Exit.Exit<void, PartError>) => void;
}

/** What the writer needs of its Session. */
export interface WriterHost {
  /** The backend's `commit`, uninterruptible; it fails the Session for nothing. */
  readonly commit: (
    batches: ReadonlyArray<ReadonlyArray<StorageWrite>>,
    receive: (index: number, seq: Seq) => void,
  ) => Effect.Effect<CommitResult, StorageFailure>;
  /** Applies a stored batch to the Session's read caches, right before it is published. */
  readonly stored: (writes: ReadonlyArray<StorageWrite>) => void;
  /** The error that failed the Session, if one did. */
  readonly failure: () => StorageFailure | Error | undefined;
  /**
   * Fails the Session; `at` is the line index of the first job whose outcome the failure is, and `own` the jobs
   * after it whose outcome is still their own error (the other parts of a call that failed), not `SessionFailed`.
   */
  readonly fail: (error: StorageFailure | Error, at: number, own: ReadonlyArray<number>) => void;
  /** Reports an own observer's throw that cannot fail the Session, as it fails with a storage error. */
  readonly report: (error: Error) => void;
}

interface Admitted extends PartSpec {
  /** Opens once it is published or never will be; never fails. */
  readonly gate: Deferred.Deferred<void>;
  done: boolean;
}

/** How a part that will not resolve settles: with an error of its own, or `SessionFailed`. */
type FailedOutcome =
  | { readonly _tag: "Own"; readonly cause: Cause.Cause<PartError> }
  | { readonly _tag: "Failed" }
  | { readonly _tag: "Succeeded" };

export interface Writer {
  /** Admits a part, synchronously: registers its keys and queues it for the writer loop. */
  readonly admit: (spec: PartSpec) => void;
  /** The gate of the newest unpublished writer of `key`, or `undefined` when it has none. */
  readonly gate: (key: string) => Effect.Effect<void> | undefined;
  /** Opens once every part admitted so far is published or never will be; `undefined` when none is pending. */
  readonly drained: () => Effect.Effect<void> | undefined;
  /** Keys with an unpublished writer, for tests that bound memory. */
  readonly unpublished: () => number;
}

/**
 * The pause between two publications of one call, a macrotask hop.
 * In `effect@4.0.6` (as in 4.0.1) each fiber has its own dispatcher, and each drains its queue in a `setImmediate` of its own, so
 * the priority orders only this fiber's tasks. What makes the pause work is the platform's FIFO `setImmediate`:
 * a waiter the previous publication woke is scheduled before the publisher's yield, so its continuation runs first.
 */
const PUBLICATION_HOP_PRIORITY = 1;

/**
 * The pause itself: two yields. A waiter the previous publication woke resumes after a yield of its own
 * (`doneAfterYield`); the second yield lets that continuation run before the next part is published.
 */
const hop = Effect.yieldNowWith(PUBLICATION_HOP_PRIORITY).pipe(
  Effect.andThen(Effect.yieldNowWith(PUBLICATION_HOP_PRIORITY)),
);

/** Settles a part once, with its outcome. */
const settle = (part: Admitted, exit: Exit.Exit<void, PartError>) => {
  if (part.done) return;
  part.done = true;
  part.settle(exit);
};

/** Makes the writer of one Session and forks its loop into `scope`. */
export const make = (
  host: WriterHost,
  scope: Scope.Scope,
): Effect.Effect<{ readonly writer: Writer; readonly loop: Fiber.Fiber<never> }> =>
  Effect.gen(function* () {
    let queue: Array<Admitted> = [];
    /** Per key, the newest unpublished part that writes it. */
    const unpublished = new Map<string, Admitted>();
    /** The newest admitted part that is not yet published, whose gate opens after every earlier one's. */
    let newest: Admitted | undefined;
    const wake = Signal.make();

    /** `SessionFailed` with the Session's failure, or with `error` until the Session records it. */
    const failedWith = (error: StorageFailure | Error): SessionFailed =>
      new SessionFailed({ cause: host.failure() ?? error });

    /** Drops the keys a part is still the newest writer of: it is published, or never will be. */
    const unregister = (part: Admitted) => {
      for (const key of part.keys) if (unpublished.get(key) === part) unpublished.delete(key);
    };

    /** Opens a part's gate: the jobs waiting for it go on, after a yield. */
    const open = (part: Admitted) => {
      doneAfterYield(part.gate, Effect.void);
      if (newest === part) newest = undefined;
    };

    /** Settles a part that will never be published: discards it, fails it with `cause`, and opens its gate. */
    const unpublish = (part: Admitted, cause: Cause.Cause<PartError>) => {
      part.discard();
      settle(part, Exit.failCause(cause));
      unregister(part);
      open(part);
    };

    /**
     * Publishes the received parts in order, a pause apart, and settles every part. A refusal,
     * a failure, or a part whose adoption or own observer throws fails the Session once the parts before it are
     * published; the parts after it are not published, and fail with `SessionFailed`.
     */
    const publish = (
      parts: ReadonlyArray<Admitted>,
      seqs: ReadonlyArray<Seq | undefined>,
      exit: Exit.Exit<CommitResult, StorageFailure> | undefined,
    ) =>
      Effect.gen(function* () {
        const rejected =
          exit !== undefined && Exit.isFailure(exit) ? classify(exit.cause) : undefined;
        const result = exit !== undefined && Exit.isSuccess(exit) ? exit.value : undefined;
        let stop:
          | {
              readonly error: StorageFailure | Error;
              readonly at: number;
              readonly external: boolean;
            }
          | undefined;
        /** Settled once the Session failed: each part, whether it was published, and its outcome. */
        const failing: Array<{
          readonly part: Admitted;
          readonly outcome: FailedOutcome;
          readonly published: boolean;
        }> = [];
        let published = 0;
        let storedIndex = 0;
        for (const part of parts) {
          // A held part has no batch in the call.
          const index = part.writes === undefined ? undefined : storedIndex++;
          if (stop !== undefined) {
            // A part after a failed call's last receipt has the call's error; any other part after a stop was
            // not stored, nor any part once something else failed the Session.
            const outcome: FailedOutcome =
              rejected !== undefined && index !== undefined && !stop.external
                ? { _tag: "Own", cause: rejected.caller }
                : { _tag: "Failed" };
            failing.push({ part, outcome, published: false });
            continue;
          }
          const seq = index === undefined ? undefined : seqs[index];
          if (index !== undefined && seq === undefined) {
            const own: Classified =
              rejected !== undefined
                ? rejected
                : index === result?.stored && result.refused !== undefined
                  ? { failure: result.refused, caller: Cause.fail(result.refused) }
                  : missingReceipt();
            failing.push({ part, outcome: { _tag: "Own", cause: own.caller }, published: false });
            stop = { error: own.failure, at: part.index, external: false };
            continue;
          }
          // The pause between two parts: what the first resolves runs before the second is published.
          if (published++ > 0) yield* hop;
          // Failed by something else meanwhile, during the pause: nothing more of the call is published.
          const external = host.failure();
          if (external !== undefined) {
            stop = { error: external, at: part.index, external: true };
            failing.push({ part, outcome: { _tag: "Failed" }, published: false });
            continue;
          }
          // Its keys leave first, so what its publication schedules sees it published; its waiters wake after it.
          unregister(part);
          // A throw anywhere in it, the read caches included, leaves memory behind what is stored.
          const ran = Result.try({
            try: () => {
              if (part.writes !== undefined) host.stored(part.writes);
              return part.publish(seq);
            },
            catch: (error): PublishFailure => ({
              _tag: "Adoption",
              error: error instanceof Error ? error : new Error(String(error)),
            }),
          });
          const failure: PublishFailure | undefined = Result.isSuccess(ran)
            ? ran.success
            : ran.failure;
          if (failure?._tag === "Adoption") {
            failing.push({
              part,
              outcome: { _tag: "Own", cause: Cause.die(failure.error) },
              published: true,
            });
            stop = { error: failure.error, at: part.index, external: false };
            continue;
          }
          // A part published while its call failed is durable but fails; an own observer's throw
          // is then reported, as the Session fails with the storage error.
          if (rejected !== undefined) {
            if (failure !== undefined) host.report(failure.error);
            failing.push({
              part,
              outcome: { _tag: "Own", cause: rejected.caller },
              published: true,
            });
            continue;
          }
          // An own observer's throw: the commit stands, and the Session fails at it. `host.fail` runs
          // before the part settles, so a later job whose outcome is known already (a read, a commit without
          // writes), delivered as soon as this one is, sees the failure.
          if (failure !== undefined) {
            stop = { error: failure.error, at: part.index, external: false };
            failing.push({ part, outcome: { _tag: "Succeeded" }, published: true });
            continue;
          }
          const failedMeanwhile = host.failure();
          settle(
            part,
            failedMeanwhile !== undefined
              ? Exit.fail(new SessionFailed({ cause: failedMeanwhile }))
              : Exit.void,
          );
          open(part);
        }
        if (rejected !== undefined || stop !== undefined) {
          // Every part of the call that failed keeps the storage error; the first of them is the boundary.
          const own = failing.flatMap(({ part, outcome }) =>
            outcome._tag === "Own" ? [part.index] : [],
          );
          const first = own[0];
          if (rejected !== undefined && first !== undefined)
            host.fail(rejected.failure, first, own);
          else if (stop !== undefined) host.fail(stop.error, stop.at, own);
        }
        for (const entry of failing) {
          if (entry.outcome._tag === "Succeeded") {
            settle(entry.part, Exit.void);
            open(entry.part);
            continue;
          }
          const cause =
            entry.outcome._tag === "Own"
              ? entry.outcome.cause
              : Cause.fail(failedWith(stop?.error ?? new Error("Session failed")));
          if (entry.published) {
            settle(entry.part, Exit.failCause(cause));
            open(entry.part);
          } else unpublish(entry.part, cause);
        }
        // Admitted while this call ran, after the failure: never sent.
        const failure = host.failure();
        if (failure !== undefined) {
          const late = queue;
          queue = [];
          for (const part of late)
            unpublish(part, Cause.fail(new SessionFailed({ cause: failure })));
        }
      });

    /** Stores `parts` in one call, then publishes them in order; settles every one of them. */
    const send = (parts: ReadonlyArray<Admitted>) =>
      Effect.gen(function* () {
        const failed = host.failure();
        if (failed !== undefined) {
          for (const part of parts)
            unpublish(part, Cause.fail(new SessionFailed({ cause: failed })));
          return;
        }
        const seqs: Array<Seq | undefined> = [];
        const batches: Array<ReadonlyArray<StorageWrite>> = [];
        for (const part of parts) if (part.writes !== undefined) batches.push(part.writes);
        // Only held parts: no storage call.
        const exit =
          batches.length === 0
            ? undefined
            : yield* Effect.exit(
                host.commit(batches, (index, seq) => {
                  seqs[index] = seq;
                }),
              );
        // A call that settles once another failure failed the Session reports
        // `SessionFailed` with that first error, whatever it returned, and publishes nothing.
        const failedMeanwhile = host.failure();
        if (failedMeanwhile !== undefined) {
          for (const part of parts) {
            unpublish(part, Cause.fail(new SessionFailed({ cause: failedMeanwhile })));
          }
          return;
        }
        yield* publish(parts, seqs, exit);
      });

    /**
     * A defect the writer itself did not expect while sending or publishing `parts` (a bug): fails the Session at
     * the first of them and settles every one not settled yet, so no outcome, gate or close waits on it forever.
     */
    const abandon = (parts: ReadonlyArray<Admitted>, cause: Cause.Cause<unknown>) => {
      const error = Cause.squash(cause);
      const failure = error instanceof Error ? error : new Error(String(error));
      const first = parts[0];
      if (first !== undefined) host.fail(failure, first.index, []);
      for (const part of parts) {
        if (!part.done) unpublish(part, Cause.fail(failedWith(failure)));
        unregister(part);
        open(part);
      }
    };

    /** One pass of the loop: sends what is queued, then what was admitted meanwhile, until nothing is. */
    const flush = Effect.suspend(function loop(): Effect.Effect<void> {
      const parts = queue;
      if (parts.length === 0) return Effect.void;
      queue = [];
      return send(parts).pipe(
        Effect.catchCause((cause) => Effect.sync(() => abandon(parts, cause))),
        Effect.andThen(Effect.suspend(loop)),
      );
    }).pipe(Effect.uninterruptible);

    const loop = yield* Signal.forkLoop(wake, flush, scope);

    const writer: Writer = {
      admit: (spec) => {
        const part: Admitted = { ...spec, gate: Deferred.makeUnsafe<void>(), done: false };
        for (const key of spec.keys) unpublished.set(key, part);
        newest = part;
        queue.push(part);
        wake.fire();
      },
      gate: (key) => {
        const part = unpublished.get(key);
        return part === undefined ? undefined : Deferred.await(part.gate);
      },
      drained: () => (newest === undefined ? undefined : Deferred.await(newest.gate)),
      unpublished: () => unpublished.size,
    };
    return { writer, loop };
  });

/** How a storage failure ends: what fails the Session, and what the caller gets. */
export interface Classified<E = StorageFailure> {
  readonly failure: StorageFailure | Error;
  readonly caller: Cause.Cause<E | SessionFailed>;
}

/**
 * Classifies the failure of a storage call: a `StorageFailure` fails the Session and is the
 * caller's; a defect fails it too and stays a defect; anything else, an interruption included, fails it with an
 * `Error`, and the caller gets `SessionFailed`.
 */
export const classify = <E>(cause: Cause.Cause<E>): Classified<E> => {
  const error = Cause.squash(cause);
  if (isStorageFailure(error)) return { failure: error, caller: cause };
  if (Cause.hasDies(cause)) {
    return { failure: error instanceof Error ? error : new Error(String(error)), caller: cause };
  }
  // The caller gets `SessionFailed`, whose cause is the backend's error, so it stays within the call's error type.
  const unexpected = Cause.hasInterruptsOnly(cause)
    ? new Error("A storage call was interrupted by its backend")
    : error instanceof Error
      ? error
      : new Error(String(error));
  return { failure: unexpected, caller: Cause.fail(new SessionFailed({ cause: unexpected })) };
};

/** A call that stored a batch without its receipt. */
const missingReceipt = (): Classified => {
  const missing = new Error("Storage committed without adopting the batch");
  return { failure: missing, caller: Cause.die(missing) };
};

const isStorageFailure = Schema.is(StorageFailure);

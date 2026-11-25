// The Session: one mutation line, group commit, the storage failure guard, the read caches, the use-bounded document
// cache and committed publication.
//
// Only committed state is observable. The line runs one job at a time: a
// commit's callback, settlement and admission, or a read. A commit with writes
// then joins the writer's pending batch and the line goes on; the writer
// (`writer.ts`) stores the commits admitted meanwhile in one call and publishes
// them in line order. A job's every read touches its key (`touch`), so it
// waits for an unpublished commit that writes it. Outcomes are delivered in
// line order. State and watch consumers run later, in
// their own fibers.
//
// Storage failures are final: the first error a storage call returns, other
// than an invalid read, fails the Session. That call gets the error (or
// `SessionFailed` with it as cause, when it is outside the call's type; see
// `guard`); every later call, and every call still underway, gets
// `SessionFailed`.
import * as PendingOps from "@effect-durable-harness/delta/PendingOps";
import * as Tracker from "@effect-durable-harness/delta/Tracker";
import * as Cause from "effect/Cause";
import * as Clock from "effect/Clock";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import { IdleTimers } from "../IdleTimers.ts";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import type * as Scope from "effect/Scope";
import * as Tracer from "effect/Tracer";
import {
  AgentStorage,
  type DocumentAddress,
  type HeadMarker,
  StorageFailure,
  StorageRequestError,
  type StorageWrite,
} from "../AgentStorage.ts";
import type {
  ConversationDoc,
  ConversationDocFamily,
  Doc,
  DocumentWatch,
  RewindableConversationDoc,
  RewindableConversationDocFamily,
  SessionDoc,
  SessionDocFamily,
  TaskDoc,
  TaskDocFamily,
  WatchFrame,
} from "../Doc.ts";
import type { ConversationId, DocumentId, EntryId, Seq, TaskId } from "../Ids.ts";
import type { CommitObservation, CommitObserver, Observation, Observer } from "../Observer.ts";
import type { ConversationRecord, DocumentRecord } from "../Records.ts";
import type {
  CommitChange,
  CommitError,
  CommitOptions,
  CommitPersistence,
  CommitPublication,
  DocSnapshot,
  DocSnapshotAsOf,
  DocObserveAttach,
  DocWatchAcquire,
  DocumentCommitChange,
  ReadError,
  Session,
  SessionEnd,
  SessionOptions,
} from "../Session.ts";
import type { ReportInfo } from "../Report.ts";
import { LISTENER, type Reporter } from "./report.ts";
import * as telemetry from "./telemetry.ts";
import { Events } from "../Telemetry.ts";
import {
  DocumentAccessError,
  NotVisible,
  ReadOnlySession,
  SessionClosed,
  SessionFailed,
} from "../SessionError.ts";
import { Tx, type TxDocError } from "../Tx.ts";
import {
  type AnyDefinition,
  asValue,
  checkRecord,
  type DocArgument,
  definitionOf,
  materializeStored,
  resolveAddress,
  type SpanAttributes,
  spanAttributes,
} from "./documents.ts";
import { addressKey } from "./commit.ts";
import * as ConversationTails from "./conversationTails.ts";
import * as Line from "./line.ts";
import {
  closeEnd,
  type Follower,
  type Guarded,
  type GuardServices,
  guardedFollower,
  guardObserver,
  NO_OPERATIONS,
  watchObserver,
} from "./observation.ts";
import * as Keys from "./keys.ts";
import * as RecordCache from "./recordCache.ts";
import * as Writer from "./writer.ts";
import * as Signal from "./signal.ts";
import { doneAfterYield } from "./signal.ts";
import {
  type AdoptedDocuments,
  type GuardedStorage,
  type HeldChange,
  type HeldDocument,
  type LoadedDocument,
  openTransaction,
  storeHeld,
  type StorageError,
  transactionIdentity,
  type Transaction,
  type TransactionScope,
} from "./transaction.ts";

/**
 * What the Session's own observers get of one commit: a stored commit
 * with its own changes, or a deferred one adopted in memory only, which changes documents only. Memory is never
 * behind: a later store of held changes is not observed again.
 */
export type ObservedCommit =
  | {
      readonly kind: "stored";
      readonly seq: Seq;
      readonly changes: ReadonlyArray<CommitChange>;
    }
  | { readonly kind: "memory"; readonly changes: ReadonlyArray<DocumentCommitChange> };

/** A held document as the Session keeps it, pinned in the cache until stored. */
interface Hold extends HeldDocument {
  /** When the earliest of its held commits must be stored, on the Session clock. */
  deadline: number;
  readonly unpin: () => void;
}

const NO_HOLDS: ReadonlyArray<Hold> = [];

/** The default `withinMs` of a deferred commit. */
const DEFAULT_PERSIST_WITHIN_MS = 1_000;

/** The pending eviction of the oldest idle document. */
interface PendingSweep {
  readonly at: number;
  fiber?: Fiber.Fiber<void>;
}

type Snapshot<T> = Effect.Effect<Option.Option<Readonly<T>>, ReadError>;
type WatchOf<T> = Effect.Effect<Option.Option<DocumentWatch<T>>, ReadError, Scope.Scope>;
type ObserveOf<T> = Effect.Effect<Option.Option<Observation<T>>, ReadError>;

/** Default document retention: ten minutes, as for kept contexts. */
const DEFAULT_RETENTION_MS = 600_000;

/**
 * Jobs started on the line whose outcome is not delivered yet, beyond which the line waits.
 * A count, not bytes: what waits is bounded by what one commit can hold, times the count.
 */
const MAX_OUTSTANDING = 256;

/** A job on the line: its index in line order, and whether a touch of it waited for an unpublished writer. */
interface Job {
  readonly index: number;
  hit: boolean;
  /** Called once its outcome settles: its commit published, or never to be (`onJobEnd`). */
  ends?: Array<() => void> | undefined;
  /** The job before it, until that one's outcome is delivered. */
  previous: Job | undefined;
  /** The job after it, until this one's outcome is delivered. */
  next: Job | undefined;
  /** Its outcome, once it has one; delivered once the jobs before it have theirs. */
  outcome: Exit.Exit<unknown, unknown> | undefined;
  delivered: boolean;
  /** What its caller awaits. */
  readonly result: Deferred.Deferred<unknown, unknown>;
}

/**
 * A line job's result: its value, and whether its commit is admitted to the writer, which settles its outcome at
 * publication (`complete`).
 */
interface Ran<A> {
  readonly value: A;
  readonly admitted?: boolean | undefined;
}

/** Where `lineJob` keeps the job it started on the line, whatever happens to the caller afterwards. */
interface Slot {
  job?: Job;
}

/** Options of the kernel: the public options plus the Harness's hook point. */
export interface KernelOptions extends SessionOptions {
  /** Runs inside every transaction that creates or forks a conversation (stages built-in documents). */
  readonly conversationCreated?:
    | ((tx: Transaction, record: ConversationRecord) => Effect.Effect<void, TxDocError, Tx>)
    | undefined;
  /**
   * Runs at a commit's admission, once its callback and settlement succeeded and it has writes, before storage
   * (`membershipKeys`): the Harness loads the ownership chains the commit's task and conversation changes reach.
   * A failure discards the commit.
   */
  readonly admitting?:
    | ((tx: Transaction) => Effect.Effect<ReadonlyArray<string>, StorageFailure | SessionFailed>)
    | undefined;
  /**
   * Called synchronously right after the close listeners when the host closes the Session, in the same step
   * that sealed admission: the Harness signals its task invocations here. Not called when a failure closes
   * the Session, whose invocations are signalled in `beforeClose`, a microtask later.
   */
  readonly afterSeal?: (() => void) | undefined;
  /**
   * Runs once close has sealed admission and run the close listeners, before the line drains: the Harness
   * joins its task invocations here.
   */
  readonly beforeClose?: Effect.Effect<void> | undefined;
  /**
   * Receives what fails no caller: the Session's first failure, once (`storage_failed`), and a throwing close
   * or host commit listener (`listener_error`). A plain Session drops them.
   */
  readonly report?: Reporter | undefined;
  /**
   * Every commit with writes fails with
   * `ReadOnlySession` before it reaches storage, and the Session stays healthy.
   */
  readonly readOnly?: boolean | undefined;
}

/** The kernel: the public Session surface plus what the Harness and tests reach. */
export interface SessionKernel {
  readonly commit: <A, E>(
    change: Effect.Effect<A, E, Tx>,
    options?: CommitOptions,
  ) => Effect.Effect<A, E | CommitError>;
  /** A commit that exposes the concrete transaction; `scope` binds its defaults. */
  readonly commitWith: <A, E>(
    change: (tx: Transaction) => Effect.Effect<A, E, Tx>,
    scope?: TransactionScope,
    options?: CommitOptions,
  ) => Effect.Effect<A, E | CommitError>;
  /** Runs a read-only job on the line, so multi-read derivations see one committed state. */
  readonly readOnLine: <A, E>(
    job: Effect.Effect<A, E>,
  ) => Effect.Effect<A, E | SessionFailed | SessionClosed>;
  /**
   * Runs a storage read off the line, as a context read scans its entries. Once close has drained the line
   * it is refused; close waits for the ones underway before `closed` settles.
   */
  readonly offLine: <A, E>(
    call: Effect.Effect<A, E>,
  ) => Effect.Effect<A, E | SessionFailed | SessionClosed>;
  readonly snapshot: DocSnapshot;
  readonly snapshotAsOf: DocSnapshotAsOf;
  readonly watchDoc: DocWatchAcquire;
  readonly observeDoc: DocObserveAttach;
  /**
   * Attaches a host's commit observer, behind `guardObserver`. It sees a deferred commit
   * only once stored, with exactly what was stored.
   */
  readonly observeCommits: (
    observer: CommitObserver,
  ) => Effect.Effect<CommitObservation, SessionFailed | SessionClosed>;
  /**
   * Registers one of the Session's own listeners: a throw fails the Session. They run
   * before host observers, and see a deferred commit when memory adopts it (`kind: "memory"`). Returns its
   * idempotent disposer.
   */
  readonly followCommits: (
    listener: (publication: ObservedCommit) => void,
  ) => Effect.Effect<() => void, SessionFailed | SessionClosed>;
  /** Registers a listener called synchronously when close begins, also when a failure closes the Session. */
  readonly subscribeClose: (
    listener: () => void,
  ) => Effect.Effect<() => void, SessionFailed | SessionClosed>;
  /**
   * Storage behind the Session's failure guard, for the Harness's own reads on the line: each read waits while
   * an unpublished commit writes its key.
   */
  readonly storage: GuardedStorage;
  /** Storage behind the guard without the line's gates, for reads off the line (a context read's entry scan). */
  readonly offLineStorage: GuardedStorage;
  /** On the line: waits while `key` has an unpublished writer; `undefined` when it has none. */
  readonly touch: Keys.Touch;
  /**
   * On the line: calls `end` once the outcome of the job on the line settles, its commit published or never to
   * be; at once off the line.
   */
  readonly onJobEnd: (end: () => void) => void;
  /**
   * During close, after the commits admitted before the seal
   * settled, stores the batch `writes` builds straight to storage, publishing nothing. A failed Session writes
   * nothing; a failing store fails it. Only for close's own writes: it bypasses admission.
   */
  readonly writeAtClose: (writes: () => ReadonlyArray<StorageWrite>) => Effect.Effect<void>;
  /**
   * On the line: the newest entry ID visible from the conversation.
   * Known from the Session's own commits, so it costs no storage read after the first.
   */
  /**
   * On the line: a conversation document of the definition through the Session's document cache, held changes
   * included; `None` when absent. A record that fails
   * the definition is a `DocumentAccessError`.
   */
  readonly conversationDocumentOnLine: (
    definition: AnyDefinition,
    conversationId: ConversationId,
  ) => Effect.Effect<
    Option.Option<{
      readonly record: DocumentRecord;
      readonly version: number;
      readonly value: Schema.JsonObject;
    }>,
    DocumentAccessError | StorageError
  >;
  readonly latestEntryId: (
    conversationId: ConversationId,
  ) => Effect.Effect<EntryId | undefined, StorageError>;
  /**
   * The newest entry ID of the conversation as known from the Session's commits right now, without reading:
   * `null` for none, `undefined` when not known.
   */
  readonly knownLatestEntryId: (conversationId: ConversationId) => EntryId | null | undefined;
  /**
   * On the line: the newest visible head marker at or before `atOrBefore`, any when `undefined`;
   * known from the Session's commits as `latestEntryId` is.
   */
  readonly latestHeadMarker: (
    conversationId: ConversationId,
    atOrBefore: EntryId | undefined,
  ) => Effect.Effect<HeadMarker | undefined, StorageError>;
  /** The Session's wall clock, which stamps task lifecycle times. */
  readonly now: () => number;
  /** Fails the Session with `cause`; the first failure wins. */
  readonly fail: (cause: StorageFailure | Error) => void;
  /** The failure, once the Session failed. */
  readonly failure: Option.Option<SessionFailed>;
  /** Whether close has begun, by the host or by a failure. */
  readonly closing: boolean;
  /** Completes once the Session fails; never when it closes without a failure. */
  readonly whenFailed: Effect.Effect<void>;
  readonly closed: Effect.Effect<SessionEnd>;
  /** Seals admission, ends observers and lets admitted work settle; idempotent. */
  readonly close: Effect.Effect<void>;
  /** Drops every loaded tracker on the line; later access cold-loads from storage. */
  readonly unloadDocuments: Effect.Effect<void, StorageFailure | SessionFailed | SessionClosed>;
  /** Whether `service` is the `Tx` of one of this Session's commits. */
  readonly ownsTransaction: (service: Tx["Service"]) => boolean;
  /** For tests that bound memory: the record cache, the conversation tails and the task-document index. */
  readonly readCacheSizes: {
    readonly records: RecordCache.RecordCacheSizes;
    readonly readsInFlight: number;
    readonly tails: number;
    readonly markers: number;
    readonly taskDocuments: number;
  };
  /** For tests that bound memory: outcomes not delivered, keys with an unpublished writer, cached reads in flight. */
  readonly lineSizes: {
    readonly outstanding: number;
    readonly unpublished: number;
    readonly reads: number;
  };
  /** For tests: cached, idle and pinned documents, the sweep jobs queued so far, and documents with held changes. */
  readonly documentCacheSizes: {
    readonly cached: number;
    readonly idle: number;
    readonly attached: number;
    readonly sweeps: number;
    readonly held: number;
  };
  /** For tests: when the timer of held changes fires, on the Session clock; `undefined` when none is armed. */
  readonly heldDeadline: number | undefined;
}

/** Builds the kernel over the `AgentStorage` in context, in the current scope. */
export const make = (
  options: KernelOptions,
): Effect.Effect<SessionKernel, never, AgentStorage | Scope.Scope> =>
  Effect.gen(function* () {
    // The Session owns its storage, so it serves hot record reads and the newest
    // entry of each conversation from memory, applied at each publication.
    const cache = RecordCache.make(yield* AgentStorage);
    const backend = cache.storage;
    const tails = ConversationTails.make();
    const clock = yield* Clock.Clock;
    // Whether the host's platform keeps idle timers.
    const idleTimers = (yield* IdleTimers).enabled;
    const sessionScope = yield* Effect.scope;
    const now = () => clock.currentTimeMillisUnsafe();
    const report = (
      cause: Cause.Cause<unknown>,
      info: ReportInfo = LISTENER,
      on?: Tracer.AnySpan | undefined,
    ) => {
      if (options.report !== undefined)
        contained(
          () => options.report?.(cause, info, on),
          () => {},
        );
    };

    // What a host observer's guard reports through, and the services an Effect its `advance` returns runs with:
    // the Session's own, as for every host callback the Session runs.
    const services = yield* Effect.context<never>();
    /** The caller span of the commit being published, for the observers' guards. */
    let publishing: Tracer.AnySpan | undefined;
    const guardServices: GuardServices = {
      report: (error) => report(Cause.die(error), LISTENER, publishing),
      runReturned: (effect) => {
        Effect.runForkWith(services)(effect);
      },
    };

    const documents = new Map<string, LoadedDocument>();
    /** The current documents of each live task this Session created. */
    const taskDocuments = new Map<TaskId, Map<DocumentId, DocumentRecord>>();
    /** Cached documents with no observer, by address, oldest use first, with the time of that use. */
    const idleDocuments = new Map<string, number>();
    /** Observers per incarnation; a cached incarnation with any is never evicted. */
    const attachedDocuments = new Map<DocumentId, number>();
    /** Host commit observers (`observeCommits`), each behind `guardObserver`. */
    const commitListeners = new Set<Guarded<CommitPublication>>();
    const internalListeners = new Set<(publication: ObservedCommit) => void>();
    /** Documents with held changes, by incarnation, each pinned. */
    const holds = new Map<DocumentId, Hold>();
    /** Wakes the loop that stores held changes at their deadline (`armHolds`). */
    const holdWake = Signal.make();
    /** The deadline the hold loop sleeps until; `undefined` when it sleeps for none. */
    let holdArmedAt: number | undefined;
    const closeListeners = new Set<() => void>();
    const closedDeferred = Deferred.makeUnsafe<SessionEnd>();
    let failure: SessionFailed | undefined;
    let firstError: StorageFailure | Error | undefined;
    /** Completed when the Session fails, so its close runs without waiting for the scope. */
    const failed = Deferred.makeUnsafe<void>();
    let closing = false;
    let sweep: PendingSweep | undefined;
    let sweepNeeded = false;
    /** Wakes the sweeper when a document becomes idle outside a Session operation (a released pin). */
    const sweepWake = Signal.make();
    let sweepJobs = 0;

    // ─── The mutation line ──────────────────────────────────────────────────

    const line = Line.make();
    // `onLine` is for the Session's own upkeep: no outcome to deliver, so no fence and no
    // `SessionFailed`. Callers go through `lineJob`.
    const onLine = <A, E, R>(job: Effect.Effect<A, E, R>): Effect.Effect<A, E, R> =>
      line.run<A, E, R, never>(job, () => undefined);

    // ─── Group commit: line jobs and outcome fences ──
    //
    // The line runs one job at a time: a commit's callback, settlement and admission, or a read. A commit
    // with writes then joins the writer's pending batch and the line goes on; the writer stores and publishes
    // it off the line. Every job's outcome is delivered in line order, once the jobs before it have theirs and
    // its own commit, if it admitted one, is published.

    /** The job on the line, while one runs. */
    let current: Job | undefined;
    /** Line index of the next job; outcomes are delivered in this order. */
    let nextIndex = 0;
    /** The newest job started on the line, while its outcome is not delivered. */
    let newestJob: Job | undefined;
    let lastDelivered = -1;
    /** Jobs that started on the line and have no outcome delivered yet (`MAX_OUTSTANDING`). */
    let outstanding = 0;
    const room = Signal.make();
    /** The line index whose outcome the failure is: every later job's outcome is `SessionFailed`, except `ownOutcomes`. */
    let failAt: number | undefined;
    /** Later jobs whose outcome is their own error all the same: the other parts of a storage call that failed. */
    const ownOutcomes = new Set<number>();

    /**
     * Runs `work` on the line, after the jobs before it, and delivers its
     * outcome in line order: once the jobs before it have theirs, and once its commit, if it admitted one
     * (`after`), is published. A job whose outcome comes after the one a failure is gets `SessionFailed` instead.
     * At most `MAX_OUTSTANDING` jobs wait for their delivery; beyond it the line waits.
     *
     * Admission is checked in the same synchronous step that takes the line or queues for it,
     * so an operation is either queued before close seals admission or rejected. Only the wait for the line and
     * the job's own interruptible parts are interruptible: once a job started, its delivery runs to the end, so
     * the fence of every later job opens.
     */
    const lineJob = <A, E, R, F = never>(
      work: (job: Job) => Effect.Effect<Ran<A>, E, R>,
    ): Effect.Effect<A, E | F | SessionFailed | SessionClosed, R> =>
      Effect.uninterruptibleMask((restore) =>
        Effect.gen(function* () {
          const slot: Slot = {};
          const lined = yield* Effect.exit(
            restore(
              line.run(
                Effect.uninterruptibleMask((inner) =>
                  Effect.gen(function* () {
                    // Backpressure: the line waits while too many outcomes are due.
                    yield* inner(awaitRoom);
                    outstanding++;
                    const job: Job = {
                      index: nextIndex++,
                      hit: false,
                      previous: newestJob,
                      next: undefined,
                      outcome: undefined,
                      delivered: false,
                      result: Deferred.makeUnsafe(),
                    };
                    if (newestJob !== undefined) newestJob.next = job;
                    newestJob = job;
                    slot.job = job;
                    current = job;
                    const ran = yield* Effect.exit(inner(work(job)));
                    current = undefined;
                    if (Exit.isFailure(ran)) complete(job, ran);
                    else if (ran.value.admitted !== true)
                      complete(job, Exit.succeed(ran.value.value));
                  }),
                ),
                () => failure ?? (closing ? new SessionClosed() : undefined),
              ),
            ),
          );
          const job = slot.job;
          if (job === undefined) {
            // Refused, or interrupted before it started: it never ran.
            if (Exit.isFailure(lined)) return yield* Effect.failCause(lined.cause);
            return yield* Effect.die(new Error("A line job ended without running"));
          }
          // The caller's wait is interruptible, as a caller can stop waiting at any time; the delivery of the
          // outcomes after it does not depend on it (`deliver`).
          // SAFETY: the result holds this job's own value and errors.
          return yield* restore(
            Deferred.await(job.result as Deferred.Deferred<A, E | F | SessionFailed>),
          );
        }),
      );

    /** Waits until fewer than `MAX_OUTSTANDING` outcomes are due; deliveries fire `room`. */
    const awaitRoom: Effect.Effect<void> = Effect.suspend(() =>
      outstanding < MAX_OUTSTANDING ? Effect.void : Effect.andThen(room.wait, awaitRoom),
    );

    /** A job's outcome is known: delivers it, and those after it, once the jobs before it have theirs. */
    const complete = (job: Job, outcome: Exit.Exit<unknown, unknown>) => {
      if (job.outcome !== undefined) return;
      job.outcome = outcome;
      deliver(job);
    };

    /**
     * The outcome fences, synchronously: delivers each job whose outcome is known and
     * whose predecessor has its outcome delivered, in line order. A job whose outcome comes after the one a failure
     * is gets `SessionFailed` instead.
     */
    const deliver = (from: Job) => {
      let job: Job | undefined = from;
      while (
        job !== undefined &&
        job.outcome !== undefined &&
        !job.delivered &&
        (job.previous === undefined || job.previous.delivered)
      ) {
        job.delivered = true;
        job.previous = undefined;
        // What waited for the job's outcome runs now.
        const ends = job.ends;
        job.ends = undefined;
        for (const end of ends ?? []) contained(end, (error) => report(Cause.die(error)));
        lastDelivered = Math.max(lastDelivered, job.index);
        outstanding--;
        room.fire();
        const poisoned = failAt !== undefined && job.index > failAt && !ownOutcomes.has(job.index);
        doneAfterYield(
          job.result,
          poisoned ? Effect.fail(new SessionFailed({ cause: firstError })) : job.outcome,
        );
        const next: Job | undefined = job.next;
        job.next = undefined;
        if (newestJob === job) newestJob = undefined;
        job = next;
      }
    };

    /** A read-only job on the line: its outcome is delivered after the outcomes before it. */
    const admitOnLine = <A, E, R>(
      job: Effect.Effect<A, E, R>,
    ): Effect.Effect<A, E | SessionFailed | SessionClosed, R> =>
      lineJob(() => Effect.map(job, (value): Ran<A> => ({ value })));

    // ─── Failure and close ──────────────────────────────────────────────────

    const assertHealthy = <A, E, R>(next: () => Effect.Effect<A, E, R>) =>
      Effect.suspend(
        (): Effect.Effect<A, E | SessionFailed, R> =>
          failure !== undefined ? Effect.fail(failure) : next(),
      );
    const assertUsable = <A, E, R>(next: () => Effect.Effect<A, E, R>) =>
      Effect.suspend(
        (): Effect.Effect<A, E | SessionFailed | SessionClosed, R> =>
          failure !== undefined
            ? Effect.fail(failure)
            : closing
              ? Effect.fail(new SessionClosed())
              : next(),
      );

    /** Runs the close listeners once, each on its own: one that throws does not stop the others. */
    const notifyClose = () => {
      const listeners = Array.from(closeListeners);
      closeListeners.clear();
      for (const listener of listeners) contained(listener, (error) => report(Cause.die(error)));
    };

    // The first failure wins; admission ends and close listeners run now.
    const fail = (
      cause: StorageFailure | Error,
      at?: number | undefined,
      own?: ReadonlyArray<number> | undefined,
    ) => {
      if (failure !== undefined) return;
      failure = new SessionFailed({ cause });
      firstError = cause;
      // The job that caused it keeps its own outcome; without one, every outcome not
      // delivered yet is `SessionFailed`.
      failAt = at ?? current?.index ?? lastDelivered;
      if (own !== undefined) for (const index of own) ownOutcomes.add(index);
      // The failure is reported once.
      report(isStorageFailure(cause) ? Cause.fail(cause) : Cause.die(cause), {
        kind: "storage_failed",
      });
      if (!closing) {
        closing = true;
        notifyClose();
      }
      // A failure closes the Session itself, so `closed` settles `failed` without
      // waiting for the host to close the scope.
      doneAfterYield(failed, Effect.void);
    };

    // Close does not close storage: the storage layer is closed by its own scope, after this one.
    // One shutdown: every later close awaits it. It runs uninterruptibly, so a close
    // started by a failure finishes even when the Session's scope interrupts the fiber that runs it.
    let shutdown: Deferred.Deferred<void> | undefined;
    // Off-line reads: whether they are refused, how many are underway, and close's wait for them.
    let backendClosing = false;
    let offLineCalls = 0;
    let offLineDrained: Deferred.Deferred<void> | undefined;
    const offLine = <A, E>(call: Effect.Effect<A, E>) =>
      // The count and its release are registered in one uninterruptible step, so an interruption between
      // them cannot leave a call counted and hang close; only the call itself is interruptible.
      Effect.uninterruptibleMask((restore) =>
        Effect.suspend((): Effect.Effect<A, E | SessionFailed | SessionClosed> => {
          if (failure !== undefined) return Effect.fail(failure);
          if (backendClosing) return Effect.fail(new SessionClosed());
          offLineCalls++;
          return restore(call).pipe(
            Effect.ensuring(
              Effect.sync(() => {
                offLineCalls--;
                if (offLineCalls === 0 && offLineDrained !== undefined) {
                  doneAfterYield(offLineDrained, Effect.void);
                }
              }),
            ),
          );
        }),
      );
    /** Refuses new off-line reads and waits for those underway. */
    const drainOffLine = Effect.suspend(() => {
      backendClosing = true;
      if (offLineCalls === 0) return Effect.void;
      const drained = Deferred.makeUnsafe<void>();
      offLineDrained = drained;
      return Deferred.await(drained);
    });
    const close = Effect.suspend(() => {
      if (!closing) {
        closing = true;
        // The deadline is disarmed: close stores what is held itself.
        holdArmedAt = undefined;
        holdWake.fire();
        notifyClose();
        if (options.afterSeal !== undefined)
          contained(options.afterSeal, (error) => report(Cause.die(error)));
      }
      if (shutdown !== undefined) return Deferred.await(shutdown);
      const done = Deferred.makeUnsafe<void>();
      shutdown = done;
      const pendingSweep = sweep?.fiber;
      sweep = undefined;
      // Admitted work queued before the seal settles first: the line is FIFO.
      return (pendingSweep === undefined ? Effect.void : Fiber.interrupt(pendingSweep)).pipe(
        // Task invocations are joined before the line drains and storage closes.
        Effect.andThen(options.beforeClose ?? Effect.void),
        Effect.andThen(
          onLine(
            Effect.gen(function* () {
              // The calls in flight and their publication drain first.
              const drained = writer.drained();
              if (drained !== undefined) yield* drained;
              // Last, after the writes at close, while the trackers are still here.
              // A failed Session writes nothing; a failing store fails it (the writer), and close goes on.
              if (failure === undefined && holds.size > 0) {
                yield* Effect.ignore(storeHolds(allHolds));
              }
              holds.clear();
              // Commit observers end after the last publication: the commits admitted
              // before the seal have settled, held changes are stored.
              const observers = Array.from(commitListeners);
              commitListeners.clear();
              for (const observer of observers) observer.end(closeEnd(failure));
              internalListeners.clear();
              documents.clear();
              taskDocuments.clear();
              idleDocuments.clear();
              attachedDocuments.clear();
            }),
          ),
        ),
        // `closed` settles once the off-line reads underway have settled.
        Effect.andThen(drainOffLine),
        Effect.andThen(
          Effect.sync(() => {
            // Waiters on `closed` yield first, so they don't run on the closing fiber's stack.
            doneAfterYield(
              closedDeferred,
              Effect.succeed(
                firstError === undefined
                  ? ({ reason: "closed" } satisfies SessionEnd)
                  : ({ reason: "failed", error: firstError } satisfies SessionEnd),
              ),
            );
          }),
        ),
        Effect.ensuring(
          Effect.sync(() => {
            // oxlint-disable-next-line durable/no-inline-wakeup -- the end of close: only later closes await it.
            Deferred.doneUnsafe(done, Effect.void);
          }),
        ),
        Effect.uninterruptible,
      );
    });

    // ─── The storage guard ──────────────────────────────────────────────────

    /**
     * Runs one storage call under the guard. A failed
     * Session gets `SessionFailed` without reaching storage. A `StorageFailure`
     * fails the Session and is returned to this call; a `StorageRequestError`
     * from a read fails only this call; anything else, a defect or an interruption
     * included, fails the Session too. A call still
     * underway when another fails the Session ends with `SessionFailed`,
     * whatever it returned.
     *
     * A storage call is never cancelled: the call and the classification of
     * its outcome run uninterruptibly, to completion. So every failure it returns is the backend's, an
     * interruption included, and an interrupted caller cannot hide one.
     */
    const guard = <A, E extends { readonly _tag: string }>(
      call: () => Effect.Effect<A, E>,
      // Only a read may fail with an exempt `StorageRequestError`.
      read = true,
    ): Effect.Effect<A, E | SessionFailed> =>
      assertHealthy(() =>
        // Suspended, so a backend that throws while building its Effect is guarded too.
        Effect.suspend(call).pipe(
          Effect.catchCause((cause) =>
            Effect.suspend((): Effect.Effect<never, E | SessionFailed> => {
              if (failure !== undefined) return Effect.fail(failure);
              // An invalid read fails only that read.
              if (read && isStorageRequestError(Cause.squash(cause)) && !Cause.hasDies(cause)) {
                return Effect.failCause(cause);
              }
              const classified = Writer.classify(cause);
              fail(classified.failure);
              return Effect.failCause(classified.caller);
            }),
          ),
          Effect.flatMap((value) =>
            failure !== undefined ? Effect.fail(failure) : Effect.succeed(value),
          ),
          Effect.uninterruptible,
        ),
      );

    const storage: GuardedStorage = {
      mintId: guard(() => backend.mintId, false),
      conversation: (id) => guard(() => backend.conversation(id)),
      scanConversations: (query, limit, cursor) =>
        guard(() => backend.scanConversations(query, limit, cursor)),
      entry: (id) => guard(() => backend.entry(id)),
      visibleEntry: (conversationId, id) => guard(() => backend.visibleEntry(conversationId, id)),
      findLatestHeadMarker: (conversationId, atOrBefore) =>
        guard(() => backend.findLatestHeadMarker(conversationId, atOrBefore)),
      scanEntries: (query, limit, cursor) => guard(() => backend.scanEntries(query, limit, cursor)),
      task: (id) => guard(() => backend.task(id)),
      scanTasks: (query, limit, cursor) => guard(() => backend.scanTasks(query, limit, cursor)),
      submission: (id) => guard(() => backend.submission(id)),
      scanSubmissions: (query, limit, cursor) =>
        guard(() => backend.scanSubmissions(query, limit, cursor)),
      submissionByRequest: (conversationId, requestId) =>
        guard(() => backend.submissionByRequest(conversationId, requestId)),
      findDocument: (address, at) => guard(() => backend.findDocument(address, at)),
      document: (id, at) => guard(() => backend.document(id, at)),
      scanDocuments: (query, limit, cursor) =>
        guard(() => backend.scanDocuments(query, limit, cursor)),
    };

    // ─── The writer ─────────────────────────────────────────────────────────

    const { writer } = yield* Writer.make(
      {
        commit: (batches, receive) =>
          Effect.suspend(() => {
            // Caught, so a backend that throws while building its Effect is classified too.
            const call = Result.try({
              try: () => backend.commit(batches, receive),
              catch: (error) => error,
            });
            return Result.isSuccess(call) ? call.success : Effect.die(call.failure);
          }).pipe(Effect.uninterruptible),
        // The read caches take a batch right before memory adopts it.
        stored: (writes) => {
          cache.published(writes);
          tails.apply(writes);
        },
        failure: () => firstError,
        fail: (error, at, own) => fail(error, at, own),
        report: (error) => report(Cause.die(error)),
      },
      sessionScope,
    );

    // After the writer's loop, so the Session's scope runs close (which drains the writer) before it stops it.
    yield* Effect.addFinalizer(() => close);
    yield* Effect.forkIn(Deferred.await(failed).pipe(Effect.andThen(close)), sessionScope);

    /**
     * On the line: waits, while `key` has an unpublished writer (for
     * `ALL_KEYS`, while anything is unpublished), until it is published, then checks that the Session is
     * healthy; `undefined` when there is nothing to wait for. A job's every read touches its key first, so it
     * never observes state an earlier job admitted and the writer has not published.
     */
    const touch = (key: string): Effect.Effect<void, SessionFailed> | undefined => {
      const gate = key === Keys.ALL_KEYS ? writer.drained() : writer.gate(key);
      if (gate === undefined) return undefined;
      if (current !== undefined) current.hit = true;
      // A writer that failed the Session meanwhile ends the read.
      return Effect.andThen(
        gate,
        assertHealthy(() => Effect.void),
      );
    };

    /** The storage jobs on the line read through: each read waits for unpublished writers. */
    const lineStorage = Keys.gated(storage, touch);

    /** On the line: waits for what an unpublished commit wrote of a conversation's entries and its creation. */
    const touchEntries = (conversationId: ConversationId) =>
      Effect.suspend(() => {
        const created = touch(Keys.conversationKey(conversationId));
        const appended = touch(Keys.appendKey(conversationId));
        return Effect.andThen(created ?? Effect.void, appended ?? Effect.void);
      });

    const latestEntryId = (conversationId: ConversationId) =>
      touchEntries(conversationId).pipe(
        Effect.andThen(
          tails.latest(
            conversationId,
            Effect.map(lineStorage.scanEntries({ conversationId }, 1), (page) => page.items[0]?.id),
          ),
        ),
      );
    const latestHeadMarker = (conversationId: ConversationId, atOrBefore: EntryId | undefined) =>
      touchEntries(conversationId).pipe(
        Effect.andThen(
          tails.marker(
            conversationId,
            atOrBefore,
            Effect.map(
              lineStorage.findLatestHeadMarker(conversationId, atOrBefore),
              Option.getOrUndefined,
            ),
          ),
        ),
      );

    // ─── The document cache ─────────────────────────────────────────────────

    // Read at each use; a retention that is not zero or more milliseconds,
    // or a setting that throws, means the default: cache bookkeeping never changes the outcome of the
    // operation it follows.
    const retention = (): number => {
      const setting = options.documentRetention;
      if (setting === undefined) return DEFAULT_RETENTION_MS;
      const read = Result.try({
        try: () => Duration.toMillis(setting()),
        catch: () => DEFAULT_RETENTION_MS,
      });
      const ms = Result.isSuccess(read) ? read.success : read.failure;
      return Number.isNaN(ms) || ms < 0 ? DEFAULT_RETENTION_MS : ms;
    };

    // The default window of a deferred commit, read at each one; one that
    // is not zero or more milliseconds, or a reader that throws, means the default.
    const persistWithin = (): number => {
      const setting = options.persistWithin;
      if (setting === undefined) return DEFAULT_PERSIST_WITHIN_MS;
      const read = Result.try({
        try: () => Duration.toMillis(setting()),
        catch: () => DEFAULT_PERSIST_WITHIN_MS,
      });
      const ms = Result.isSuccess(read) ? read.success : read.failure;
      return Number.isNaN(ms) || ms < 0 ? DEFAULT_PERSIST_WITHIN_MS : ms;
    };

    /**
     * How long a commit may stay in memory before it is stored;
     * `undefined` when it is immediate. A `withinMs` that is not zero or more milliseconds is a `TypeError`.
     */
    const deferral = (
      persistence: CommitPersistence | undefined,
    ): Result.Result<number | undefined, TypeError> => {
      if (persistence?.kind !== "deferred") return Result.succeed(undefined);
      const ms = persistence.withinMs;
      if (ms === undefined) return Result.succeed(persistWithin());
      // Public options: checked here, as the types cannot.
      if (!(ms >= 0)) {
        return Result.fail(
          new TypeError("CommitPersistence.withinMs must be a number of milliseconds, 0 or more"),
        );
      }
      return Result.succeed(ms);
    };

    /** Notes a use of a cached document: an unpinned one becomes the newest idle one. */
    const touchDocument = (id: string) => {
      const cached = documents.get(id);
      if (cached === undefined || attachedDocuments.has(cached.record.id)) return;
      const at = now();
      const since = idleDocuments.get(id);
      // A hot document moves to the back at most once per granule, a tenth
      // of the retention up to 1 s.
      if (since !== undefined && at - since < Math.min(1_000, retention() / 10)) return;
      idleDocuments.delete(id);
      idleDocuments.set(id, at);
      // A pending sweep for the oldest document comes first unless the retention was shortened since.
      if (sweep === undefined || sweep.at > at + retention()) {
        sweepNeeded = true;
        // The sweeper runs after a yield, so the caller finishes what it was doing first.
        sweepWake.fire();
      }
    };

    const dropDocument = (id: string) => {
      documents.delete(id);
      idleDocuments.delete(id);
    };

    /** On the line: evicts the idle documents whose retention passed, oldest first. */
    const sweepDocuments = () => {
      const expired = now() - retention();
      for (const [id, since] of idleDocuments) {
        if (since > expired) break;
        // A document with an unpublished change stays until its publication touches it.
        // It leaves the idle order meanwhile, so no sweep is scheduled for it again and again before then; its
        // publication touches it, which puts it back.
        if (writer.gate(Keys.documentKey(id)) !== undefined) {
          idleDocuments.delete(id);
          continue;
        }
        dropDocument(id);
      }
      sweepNeeded = true;
    };

    // The eviction of the oldest idle document is one fiber in the Session's
    // scope that sleeps on `Clock` instead of an unreferenced timer. Effect
    // cannot tell whether a timer can be unreferenced, so the host says it:
    // with `IdleTimers` off, as on Cloudflare, no sleep is kept and the end of
    // each commit evicts what expired.
    // Uninterruptible, so the pending record and the fiber that runs it are installed together: an
    // interruption between them would leave a record that no fiber serves and block every later sweep.
    const scheduleSweep: Effect.Effect<void> = Effect.uninterruptible(
      Effect.suspend(() => {
        if (!sweepNeeded) return Effect.void;
        sweepNeeded = false;
        if (closing) return Effect.void;
        const oldest = idleDocuments.values().next();
        if (oldest.done === true) return Effect.void;
        const at = oldest.value + retention();
        if (!Number.isFinite(at)) return Effect.void;
        if (sweep !== undefined && sweep.at <= at) return Effect.void;
        const delay = at - now();
        // No timer where timers cannot be unreferenced.
        if (delay > 0 && !idleTimers) return Effect.void;
        const previous = sweep?.fiber;
        // The pending sweep stays set until its job runs, so touches while the line is busy queue no more.
        const pendingSweep: PendingSweep = { at };
        sweep = pendingSweep;
        const job: Effect.Effect<void> = (
          delay > 0 ? Effect.sleep(Duration.millis(delay)) : Effect.void
        ).pipe(
          Effect.andThen(
            Effect.suspend(() => {
              sweepJobs++;
              return onLine(
                Effect.suspend(() => {
                  if (sweep === pendingSweep) sweep = undefined;
                  if (closing) return Effect.void;
                  sweepDocuments();
                  return scheduleSweep;
                }),
              );
            }),
          ),
        );
        return Effect.gen(function* () {
          if (previous !== undefined) yield* Fiber.interrupt(previous).pipe(Effect.forkDetach);
          // Started at once, so a zero-delay job queues on the line before anything the caller queues next.
          pendingSweep.fiber = yield* Effect.forkIn(job, sessionScope, { startImmediately: true });
        });
      }),
    );

    // The sweeper: one fiber in the Session's scope that schedules the eviction whenever a touch outside
    // a Session operation (a released pin) made a document idle. Operations schedule it themselves too.
    yield* Signal.forkLoop(sweepWake, scheduleSweep, sessionScope);

    /** Pins a cached incarnation for one observer; the release unpins it once. */
    const pinDocument = (id: string, recordId: DocumentId) => {
      attachedDocuments.set(recordId, (attachedDocuments.get(recordId) ?? 0) + 1);
      idleDocuments.delete(id);
      let pinned = true;
      return () => {
        if (!pinned) return;
        pinned = false;
        const count = (attachedDocuments.get(recordId) ?? 1) - 1;
        if (count > 0) {
          attachedDocuments.set(recordId, count);
          return;
        }
        attachedDocuments.delete(recordId);
        if (documents.get(id)?.record.id === recordId) touchDocument(id);
      };
    };

    const loadDocument = (definition: AnyDefinition, id: string, address: DocumentAddress) =>
      Effect.gen(function* () {
        // What an unpublished commit wrote at the address is in neither the cache nor
        // what storage shows yet.
        const gate = touch(Keys.documentKey(id));
        if (gate !== undefined) yield* gate;
        yield* assertHealthy(() => Effect.void);
        const cached = documents.get(id);
        // A tracker serves only tokens of the version its value was materialized for.
        if (cached?.valueVersion === definition.version) {
          touchDocument(id);
          return Option.some(cached);
        }
        if (cached !== undefined) {
          // The reload must find what memory holds: held changes are stored
          // before their tracker goes, after a barrier.
          const kept = holds.get(cached.record.id);
          if (kept !== undefined && failure === undefined) yield* storeHolds(() => [kept]);
          dropDocument(id);
        }
        const record = yield* lineStorage.findDocument(address, "current");
        if (Option.isNone(record)) return Option.none<LoadedDocument>();
        const stored = yield* lineStorage.document(record.value.id, "current");
        if (Option.isNone(stored)) {
          return yield* Effect.die(
            new Error(`Current document ${record.value.id} (${record.value.kind}) cannot be read`),
          );
        }
        const value = yield* Effect.fromResult(materializeStored(definition, stored.value));
        const loaded: LoadedDocument = {
          addressId: id,
          record: stored.value.record,
          storedVersion: stored.value.version,
          valueVersion: definition.version,
          deltasSinceBase: stored.value.deltasSinceBase,
          tracker: Tracker.make(value),
        };
        documents.set(id, loaded);
        touchDocument(id);
        return Option.some(loaded);
      });

    // ─── Commits ────────────────────────────────────────────────────────────

    const host = {
      storage: lineStorage,
      touch,
      now,
      cached: (id: string) => {
        const cached = documents.get(id);
        if (cached !== undefined) touchDocument(id);
        return cached;
      },
      load: loadDocument,
      install: (document: LoadedDocument) => {
        documents.set(document.addressId, document);
        touchDocument(document.addressId);
      },
      evict: (id: string, recordId: DocumentId) => {
        if (documents.get(id)?.record.id === recordId) dropDocument(id);
      },
      taskDocuments: (taskId: TaskId) => taskDocuments.get(taskId)?.values(),
      latestHeadMarker: (conversationId: ConversationId) =>
        latestHeadMarker(conversationId, undefined),
      conversationCreated: (tx: Transaction, record: ConversationRecord) =>
        options.conversationCreated === undefined
          ? Effect.void
          : options.conversationCreated(tx, record),
    };

    /**
     * On the line: runs the callback, settles the transaction and admits
     * its writes to the writer (logical admission); the line then goes on while the writer stores and publishes
     * them. Returns the callback's value and, for a commit with writes, its part's outcome to deliver after.
     */
    const runCommit = <A, E>(
      job: Job,
      change: (tx: Transaction) => Effect.Effect<A, E, Tx>,
      scope: TransactionScope | undefined,
      withinMs: number | undefined,
      caller: Tracer.AnySpan | undefined,
    ): Effect.Effect<Ran<A>, E | CommitError> =>
      Effect.uninterruptibleMask((restore) =>
        Effect.gen(function* () {
          if (failure !== undefined) return yield* failure;
          const tx = openTransaction(host, scope, caller);
          // Only the callback is interruptible: an interrupted caller still seals the transaction below,
          // so its drafts are revoked and its pending operations drained.
          const exit = yield* Effect.exit(
            restore(change(tx).pipe(Effect.provideService(Tx, tx.service))),
          );
          // A callback that caught a failed read cannot commit.
          if (exit._tag === "Failure" || failure !== undefined) {
            yield* tx.settleFailure;
            sweepDocuments();
            yield* scheduleSweep;
            if (exit._tag === "Failure") return yield* Effect.failCause(exit.cause);
            return yield* failure ?? new SessionClosed();
          }
          const settled = yield* Effect.exit(
            tx.settleSuccess.pipe(
              // A settlement read of a valid query cannot be invalid.
              Effect.catchTag("StorageRequestError", (error: StorageRequestError) =>
                Effect.die(error),
              ),
            ),
          );
          if (settled._tag === "Failure") {
            sweepDocuments();
            yield* scheduleSweep;
            return yield* Effect.failCause(settled.cause);
          }
          const writes = settled.value;
          if (writes.length === 0) {
            tx.discard();
            sweepDocuments();
            yield* scheduleSweep;
            return { value: exit.value };
          }
          if (options.readOnly === true) {
            // Refused before storage: the Session stays healthy, and the caller
            // sees why.
            tx.discard();
            sweepDocuments();
            yield* scheduleSweep;
            return yield* new ReadOnlySession({ what: "a commit with writes" });
          }
          if (withinMs !== undefined) {
            const held = tx.holdable();
            if (held !== undefined) {
              // Adopted in memory when the commits before it are published, as a
              // part of no storage call.
              const value = exit.value;
              writer.admit({
                index: job.index,
                writes: undefined,
                keys: tx.writeKeys(),
                publish: () => hold(tx, held, withinMs, caller),
                discard: () => tx.discard(),
                settle: (settled) =>
                  complete(
                    job,
                    Exit.map(settled, () => value),
                  ),
              });
              sweepDocuments();
              yield* scheduleSweep;
              return { value, admitted: true };
            }
          }
          let membership: ReadonlyArray<string> = [];
          if (options.admitting !== undefined) {
            const admitted = yield* Effect.exit(options.admitting(tx));
            if (admitted._tag === "Failure") {
              tx.discard();
              sweepDocuments();
              yield* scheduleSweep;
              return yield* Effect.failCause(admitted.cause);
            }
            membership = admitted.value;
          }
          // Logical admission: once admitted, caller interruption does not interrupt
          // settlement; the writer stores it, and its outcome is delivered after its publication.
          // Held changes leave their queue only here, once nothing but storage can
          // fail; a commit never stores a hold adopted after it in line order. Taken before the keys, so the
          // documents it stores held changes of are its writes too: a later job that reads one waits for it.
          const taken = holds.size === 0 ? NO_HOLDS : tx.takeHeld((id) => holds.get(id));
          const value = exit.value;
          writer.admit({
            index: job.index,
            writes,
            keys: [...tx.writeKeys(), ...membership],
            publish: (seq) => publishPart(tx, writes, taken, seq, caller),
            discard: () => tx.discard(),
            settle: (settled) =>
              complete(
                job,
                Exit.map(settled, () => value),
              ),
          });
          sweepDocuments();
          yield* scheduleSweep;
          return { value, admitted: true };
        }),
      );

    /**
     * At publication: adopts a stored commit in memory and runs its
     * publication. A throw of the adoption leaves memory behind durable state; one of the Session's own observers
     * that throws fails it once the others and the host listeners got the commit.
     */
    const publishPart = (
      tx: Transaction,
      writes: ReadonlyArray<StorageWrite>,
      taken: ReadonlyArray<Hold>,
      seq: Seq | undefined,
      caller: Tracer.AnySpan | undefined,
    ): Writer.PublishFailure | undefined => {
      // SAFETY: a part with writes is published with the seq of its batch (`Writer`).
      const stored = seq!;
      const adopted = Result.try({
        try: () => tx.adopt(stored),
        catch: (error) => (error instanceof Error ? error : new Error(String(error))),
      });
      if (Result.isFailure(adopted)) return { _tag: "Adoption", error: adopted.failure };
      indexTaskDocuments(tx.createdTaskIds(), writes, adopted.success.stored, stored);
      const thrown = publish(stored, writes, adopted.success, caller);
      if (taken.length > 0) release(taken);
      for (const document of adopted.success.stored) touchDocument(addressKey(document.record));
      return thrown === undefined ? undefined : { _tag: "Listener", error: thrown };
    };

    /**
     * At publication: keeps the task-document index in step
     * with a stored commit: the tasks it creates start with no documents, task documents it creates join their
     * task, ones it retires leave, and tasks it ends leave the index.
     */
    const indexTaskDocuments = (
      created: ReadonlyArray<TaskId>,
      writes: ReadonlyArray<StorageWrite>,
      changed: ReadonlyArray<DocumentCommitChange>,
      seq: Seq,
    ) => {
      for (const id of created) taskDocuments.set(id, new Map());
      if (taskDocuments.size === 0) return;
      for (const change of changed) {
        const scope = change.record.scope;
        if (change.type !== "document" || scope.kind !== "task") continue;
        const known = taskDocuments.get(scope.taskId);
        if (known === undefined) continue;
        if (change.value === null) known.delete(change.record.id);
        else if (change.record.createdAt === seq) known.set(change.record.id, change.record);
      }
      for (const write of writes) {
        if (write.type === "task" && write.value.state.status === "terminal") {
          taskDocuments.delete(write.value.id);
        }
      }
    };

    const publish = (
      seq: Seq,
      writes: ReadonlyArray<StorageWrite>,
      documents: AdoptedDocuments,
      caller: Tracer.AnySpan | undefined,
    ): Error | undefined => {
      recordCommit(writes, caller);
      if (commitListeners.size === 0 && internalListeners.size === 0) return undefined;
      const changes: Array<CommitChange> = [];
      for (const write of writes) {
        if (
          write.type === "conversation" ||
          write.type === "entry" ||
          write.type === "task" ||
          write.type === "submission"
        ) {
          changes.push(write);
        }
      }
      const tables = changes.length;
      for (const document of documents.observed) changes.push(document);
      const observed: ObservedCommit = { kind: "stored", seq, changes };
      // Listeners get the publication only: there is no commit context to forward.
      // One publication for both, unless the commit also stored held changes,
      // which only host listeners get.
      const publication: CommitPublication =
        documents.stored === documents.observed
          ? { seq, changes }
          : { seq, changes: [...changes.slice(0, tables), ...documents.stored] };
      // The commit is durable: a failing listener neither fails the commit nor skips the others.
      // An own observer's throw fails the Session once the others
      // and the host listeners got the commit; the first throw is the failure.
      // A throw the observers' guards report goes on the commit's caller span; the document observers among the own listeners included, as for a held commit (`hold`).
      // Whether hosts get the commit is decided before the own observers run: a host listener one of them attaches
      // gets this commit only when another was attached at its publication.
      publishing = caller;
      const dispatch = commitListeners.size > 0;
      let thrown: Error | undefined;
      for (const listener of Array.from(internalListeners)) {
        contained(
          () => listener(observed),
          (error) => {
            thrown ??= error;
          },
        );
      }
      if (dispatch) notifyHosts(publication);
      publishing = undefined;
      return thrown;
    };

    /**
     * The `observeCommits` dispatch runs synchronously after the Session's own observers, in the same step, before
     * every wait the commit resolves and every abort it signals, which resume after a yield. It calls the host
     * listeners attached when it runs, those the own observers attached included. Their guards report a throw.
     */
    const notifyHosts = (publication: CommitPublication) => {
      for (const observer of Array.from(commitListeners)) observer.advance(publication);
    };

    // ─── Deferred commits ───────────────────────────────────────────────────

    /**
     * At publication: adopts a deferred commit in memory, queues its
     * changes per document, pinned, by the earliest deadline of the document's held commits, and shows it to the
     * Session's own observers only.
     */
    const hold = (
      tx: Transaction,
      held: ReadonlyArray<HeldChange>,
      withinMs: number,
      caller: Tracer.AnySpan | undefined,
    ): Writer.PublishFailure | undefined => {
      const adopted = Result.try({
        try: () => tx.adoptHeld(),
        catch: (error) => (error instanceof Error ? error : new Error(String(error))),
      });
      // As a stored commit's: memory could not adopt it; the writer fails the Session.
      if (Result.isFailure(adopted)) return { _tag: "Adoption", error: adopted.failure };
      const deadline = now() + withinMs;
      for (const change of held) {
        const id = change.loaded.record.id;
        let kept = holds.get(id);
        if (kept === undefined) {
          kept = {
            loaded: change.loaded,
            pending: PendingOps.make(),
            base: false,
            deadline,
            unpin: pinDocument(change.loaded.addressId, id),
          };
          holds.set(id, kept);
        }
        PendingOps.push(kept.pending, change.ops);
        if (change.base) kept.base = true;
        if (deadline < kept.deadline) kept.deadline = deadline;
      }
      armHolds();
      const publication: ObservedCommit = { kind: "memory", changes: adopted.success };
      // A held commit writes only documents, so it has no `durable.entry` or `durable.submission` event;
      // a throw the guards of the observers it reaches at adoption report goes on its own caller's span.
      publishing = caller;
      let thrown: Error | undefined;
      for (const listener of Array.from(internalListeners)) {
        contained(
          () => listener(publication),
          (error) => {
            thrown ??= error;
          },
        );
      }
      publishing = undefined;
      return thrown === undefined ? undefined : { _tag: "Listener", error: thrown };
    };

    /**
     * From a job on the line: stores the held changes `select`
     * returns in one storage call, through the writer. A barrier: they are selected once every commit admitted
     * before is published. Memory already has them, so only host observers (`observeCommits`) see the store.
     * A failure fails the Session, as every storage failure does.
     */
    const storeHolds = (select: () => ReadonlyArray<Hold>): Effect.Effect<void, Writer.PartError> =>
      Effect.gen(function* () {
        const drained = writer.drained();
        if (drained !== undefined) yield* drained;
        const chosen = select();
        if (chosen.length === 0) return;
        const writes: Array<StorageWrite> = [];
        const changes: Array<DocumentCommitChange> = [];
        const keys: Array<string> = [];
        for (const kept of chosen) {
          const record = kept.loaded.record;
          const version = kept.loaded.valueVersion;
          const value = kept.loaded.tracker.value;
          const stored = storeHeld(kept, NO_OPERATIONS, value, version);
          writes.push({ type: "document.change", id: record.id, content: stored.content });
          keys.push(Keys.documentKey(kept.loaded.addressId), Keys.incarnationKey(record.id));
          // Held documents are conversation documents.
          const conversationId =
            record.scope.kind === "conversation" ? record.scope.conversationId : undefined;
          changes.push({
            type: "document",
            record,
            conversationId,
            version,
            value,
            ops: stored.ops,
          });
        }
        const outcome = Deferred.makeUnsafe<void, Writer.PartError>();
        writer.admit({
          // Its failure is the outcome of the job on the line; off a job (close, the deadline), of the newest one.
          index: current?.index ?? nextIndex - 1,
          writes,
          keys,
          publish: (seq) => {
            for (const [at, kept] of chosen.entries()) {
              const write = writes[at];
              if (write?.type === "document.change" && write.content.kind === "base") {
                kept.loaded.deltasSinceBase = 0;
              } else kept.loaded.deltasSinceBase++;
            }
            // SAFETY: a part with writes is published with the seq of its batch (`Writer`). The store is the
            // Session's upkeep or a part of the job on the line: no commit caller's span.
            if (commitListeners.size > 0) notifyHosts({ seq: seq!, changes });
            release(chosen);
            return undefined;
          },
          discard: () => {},
          settle: (exit) => {
            doneAfterYield(outcome, exit);
          },
        });
        yield* Deferred.await(outcome);
      });

    /** After their store, forgets `chosen`, unpins, re-arms. */
    const release = (chosen: ReadonlyArray<Hold>) => {
      for (const kept of chosen) {
        holds.delete(kept.loaded.record.id);
        kept.unpin();
      }
      armHolds();
    };

    /** Every document with held changes, for a store of all of them. */
    const allHolds = () => Array.from(holds.values());

    /**
     * Keeps the deadline of the earliest held commit for the
     * hold loop. When it comes, every held document is stored in one storage call: a deadline is the latest time,
     * and the others cost little more in the same call.
     */
    const armHolds = () => {
      if (closing) return;
      let at = Number.POSITIVE_INFINITY;
      for (const kept of holds.values()) if (kept.deadline < at) at = kept.deadline;
      const next = Number.isFinite(at) ? at : undefined;
      if (next === holdArmedAt) return;
      holdArmedAt = next;
      holdWake.fire();
    };

    /**
     * The hold loop's pass: sleeps until `holdArmedAt`, or until it changes, then stores every held document from
     * a job on the line.
     */
    // A `Clock` sleep on a fiber of the Session's scope, so `TestClock` drives it and close ends it. It is a
    // foreground timer: armed whatever `IdleTimers` says, and not reported to `SessionWakeups`, which gets only
    // sleeps: an eviction before the deadline loses what is held, as a crash does.
    const awaitHolds = Effect.suspend(() => {
      const due = holdArmedAt;
      if (due === undefined) return Effect.void;
      return Effect.raceFirst(
        Effect.sleep(Duration.millis(Math.max(0, due - now()))).pipe(Effect.as(true)),
        holdWake.wait.pipe(Effect.as(false)),
      ).pipe(
        Effect.flatMap((fired) => {
          if (!fired || holdArmedAt !== due) {
            // The wait consumed a fire, or the deadline moved: the loop runs again with the new one.
            holdWake.fire();
            return Effect.void;
          }
          // Once fired, the timer stays until its job ran.
          holdArmedAt = undefined;
          return onLine(
            Effect.suspend(() =>
              // Close stores what is held itself; a failed Session stores nothing.
              closing || failure !== undefined ? Effect.void : storeHolds(allHolds),
            ),
          ).pipe(
            // A failure fails the Session, which reports it.
            Effect.ignore,
            Effect.andThen(Effect.sync(armHolds)),
          );
        }),
      );
    });

    // After the close finalizer, so the Session's scope stops it before close runs, which stores what is held.
    yield* Signal.forkLoop(holdWake, awaitHolds, sessionScope);

    /**
     * A stored commit's `durable.entry` and `durable.submission` events, on the submit or
     * phase span of its caller. Publication must not fail: a throwing tracer is reported as a `host_error`
     * with callback `telemetry`, and the commit stands.
     */
    const recordCommit = (
      writes: ReadonlyArray<StorageWrite>,
      caller: Tracer.AnySpan | undefined,
    ) => {
      const span = telemetry.ownerSpan(caller);
      if (span === undefined) return;
      const onError = (error: Error) => report(Cause.die(error), telemetry.TELEMETRY, caller);
      for (const write of writes) {
        if (write.type === "entry") {
          telemetry.event(
            span,
            Events.entry,
            { "durable.entry.id": write.value.id, "durable.entry.kind": write.value.kind },
            onError,
          );
        } else if (write.type === "submission") {
          const record = write.value;
          if (record.status !== "done" && record.status !== "unanswered") continue;
          const attributes: SubmissionEventAttributes = {
            "durable.submission.id": record.id,
            "durable.submission.status": record.status,
          };
          if ("answer" in record) attributes["durable.answer.entry_id"] = record.answer;
          telemetry.event(span, Events.submission, attributes, onError);
        }
      }
    };

    const commitWith = <A, E>(
      change: (tx: Transaction) => Effect.Effect<A, E, Tx>,
      scope?: TransactionScope,
      commitOptions?: CommitOptions,
    ): Effect.Effect<A, E | CommitError> =>
      // The caller's span, read before `Session.commit` opens: the span a created task continues and the commit's
      // events go on.
      Effect.serviceOption(Tracer.ParentSpan).pipe(
        Effect.flatMap((caller) => {
          // Checked before the line. A bad `withinMs` dies with a `TypeError`, since it is a programming error, not a failure in `CommitError`.
          const withinMs = deferral(commitOptions?.persistence);
          if (Result.isFailure(withinMs)) return Effect.die(withinMs.failure);
          return lineJob((job) =>
            runCommit(job, change, scope, withinMs.success, Option.getOrUndefined(caller)),
          ).pipe(Effect.withSpan("Session.commit", { attributes: commitAttributes(scope) }));
        }),
      );

    // ─── Observation ────────────────────────────────────────────────────────

    /**
     * Checks the definition, then follows the address of
     * the incarnation `loaded` with the observer `create` makes. It gets each committed change there: a
     * retirement as a `retired` frame with the last value it was given and no ops, and a later incarnation,
     * or a value of another definition version, as a root replacement. When a commit makes a task
     * document's task terminal, nothing can change the address again: after that commit's changes the
     * subscription and the pin go, and `ownerEnded` tells the observer, which stays subscribed to the
     * Session's close until it ends. The incarnation followed is pinned in the cache. `detach` removes both
     * subscriptions and the pin; once it ran, nothing more of the commit underway reaches the observer.
     */
    const attachDocument = <O extends Follower<Schema.JsonObject>>(
      definition: AnyDefinition,
      loaded: LoadedDocument,
      create: (value: Schema.JsonObject, detach: () => void) => O,
    ): Effect.Effect<O, DocumentAccessError | SessionClosed> =>
      Effect.gen(function* () {
        yield* Effect.fromResult(checkRecord(definition, loaded.record, loaded.storedVersion));
        // Registering for close asserts the Session is usable, so an observer
        // whose load finished after close began is rejected and pins nothing.
        if (closing) return yield* new SessionClosed();
        return followAddress(
          loaded.addressId,
          loaded.record,
          loaded.tracker.value,
          loaded.valueVersion,
          create,
        );
      });

    /**
     * The synchronous half of `attachDocument`. Its closures keep only the address and the incarnation
     * followed, never `loaded`, which holds its incarnation's tracker: a retirement frees it.
     */
    const followAddress = <O extends Follower<Schema.JsonObject>>(
      addressId: string,
      record: DocumentRecord,
      initial: Schema.JsonObject,
      initialVersion: number,
      create: (value: Schema.JsonObject, detach: () => void) => O,
    ): O => {
      const scope = record.scope;
      let incarnation: DocumentId | undefined = record.id;
      // The value of the incarnation followed; none while the address is absent, so the Session keeps no
      // retired value.
      let value: Schema.JsonObject | undefined = initial;
      // The definition version the observer's value has: a change of another version replaces it whole.
      let version = initialVersion;
      let unpin = pinDocument(addressId, incarnation);
      let attached = true;
      let following = true;
      const unfollow = () => {
        if (!following) return;
        following = false;
        internalListeners.delete(onCommit);
        unpin();
      };
      const detach = () => {
        if (!attached) return;
        attached = false;
        unfollow();
        closeListeners.delete(onClose);
      };
      const observer = create(initial, detach);
      const onCommit = (publication: ObservedCommit) => {
        // Listeners run from a copy of the set: another observer's call may have detached this one.
        if (!attached) return;
        let ownerEnds = false;
        for (const change of publication.changes) {
          if (change.type === "task") {
            if (
              scope.kind === "task" &&
              change.value.id === scope.taskId &&
              change.value.state.status === "terminal"
            ) {
              ownerEnds = true;
            }
            continue;
          }
          if (change.type !== "document") continue;
          // The incarnation followed is at the address by definition; only another record needs the
          // address compared.
          if (change.record.id !== incarnation && addressKey(change.record) !== addressId) continue;
          if (change.value === null) {
            if (change.record.id !== incarnation) continue;
            // SAFETY: set with every incarnation.
            const last = value!;
            incarnation = undefined;
            value = undefined;
            unpin();
            observer.advance({ value: last, ops: NO_OPERATIONS, retired: true });
          } else if (change.record.id !== incarnation || change.version !== version) {
            // A new incarnation at the address, or a value of another definition version, whose shape the
            // observer's value does not have: shown whole.
            if (change.record.id !== incarnation) {
              incarnation = change.record.id;
              unpin();
              unpin = pinDocument(addressId, incarnation);
            }
            // SAFETY: a published change with a value always has its version.
            version = change.version!;
            value = change.value;
            observer.advance({ value, ops: [["r", value]], retired: false });
          } else {
            // A migration-only base changes nothing for an observer of the new version.
            if (change.ops.length === 0) continue;
            value = change.value;
            observer.advance({ value, ops: change.ops, retired: false });
          }
          if (!attached) return;
        }
        if (!ownerEnds) return;
        unfollow();
        observer.ownerEnded();
      };
      const onClose = () => {
        detach();
        observer.closeSession(failure);
      };
      internalListeners.add(onCommit);
      closeListeners.add(onClose);
      return observer;
    };

    /**
     * Releases `attached` when the scope closes; the last release makes the incarnation idle again, so it may
     * need a sweep. Its own function, so the finalizer keeps only `attached`, never the acquisition's load.
     */
    const releaseWithScope = <O>(attached: O, release: (observer: O) => void) =>
      Effect.addFinalizer(() =>
        Effect.sync(() => release(attached)).pipe(Effect.andThen(scheduleSweep)),
      );

    const acquireObserver = <O extends Follower<Schema.JsonObject>>(
      span: string,
      definition: AnyDefinition,
      args: ReadonlyArray<DocArgument>,
      create: (value: Schema.JsonObject, detach: () => void) => O,
      release: (observer: O) => void,
    ): Effect.Effect<Option.Option<O>, ReadError, Scope.Scope> =>
      Effect.suspend(() => {
        // An acquisition interrupted after it attached releases at once, not when its scope closes
        // One variable per run.
        let attachedObserver: O | undefined;
        return assertUsable(() =>
          Effect.gen(function* () {
            const resolved = resolveAddress(definition, args);
            const observer = yield* admitOnLine(
              assertHealthy(() =>
                Effect.gen(function* () {
                  const loaded = yield* loadDocument(definition, resolved.id, resolved.address);
                  if (Option.isNone(loaded)) return Option.none<O>();
                  // Attach and register the release with no interruption point between them.
                  return yield* Effect.uninterruptible(
                    Effect.gen(function* () {
                      const attached = yield* attachDocument(definition, loaded.value, create);
                      attachedObserver = attached;
                      yield* releaseWithScope(attached, release);
                      return Option.some(attached);
                    }),
                  );
                }),
              ),
            );
            yield* scheduleSweep;
            return observer;
          }).pipe(
            Effect.onInterrupt(() =>
              Effect.sync(() => {
                if (attachedObserver !== undefined) release(attachedObserver);
              }),
            ),
          ),
        );
      }).pipe(
        Effect.withSpan(span, {
          attributes: spanAttributes(resolveAddress(definition, args).address),
        }),
      );

    // ─── Reads ──────────────────────────────────────────────────────────────

    const snapshotOf = (definition: AnyDefinition, args: ReadonlyArray<DocArgument>) =>
      assertUsable(() =>
        Effect.gen(function* () {
          const resolved = resolveAddress(definition, args);
          const cached = documents.get(resolved.id);
          let loaded: Option.Option<LoadedDocument>;
          if (cached?.valueVersion === definition.version) {
            touchDocument(resolved.id);
            loaded = Option.some(cached);
          } else {
            loaded = yield* admitOnLine(
              assertHealthy(() => loadDocument(definition, resolved.id, resolved.address)),
            );
          }
          yield* scheduleSweep;
          if (Option.isNone(loaded)) return Option.none<Schema.JsonObject>();
          yield* Effect.fromResult(
            checkRecord(definition, loaded.value.record, loaded.value.storedVersion),
          );
          return Option.some(loaded.value.tracker.value);
        }),
      ).pipe(
        Effect.withSpan("Session.snapshot", {
          attributes: spanAttributes(resolveAddress(definition, args).address),
        }),
      );

    const snapshotAsOfOf = (definition: AnyDefinition, args: ReadonlyArray<DocArgument>) =>
      admitOnLine(
        assertHealthy(() =>
          Effect.gen(function* () {
            const resolved = resolveAddress(definition, args);
            const docScope = resolved.address.scope;
            // SAFETY: the overloads accept only rewindable conversation tokens.
            const conversationId = (docScope as { readonly conversationId: ConversationId })
              .conversationId;
            // SAFETY: the overloads pass the entry ID after the owner and the key.
            const at = args[resolved.nextArgument] as EntryId;
            const entry = yield* storage.visibleEntry(conversationId, at);
            if (Option.isNone(entry)) {
              return yield* new DocumentAccessError({
                reason: new NotVisible({
                  message: `Entry ${at} is not visible from conversation ${conversationId}`,
                }),
              });
            }
            // The ancestor that owns the entry, at the entry's commit.
            const address: DocumentAddress = {
              ...resolved.address,
              scope: { kind: "conversation", conversationId: entry.value.entry.conversationId },
            };
            const record = yield* storage.findDocument(address, entry.value.commitSeq);
            if (Option.isNone(record)) return Option.none<Schema.JsonObject>();
            const stored = yield* storage.document(record.value.id, entry.value.commitSeq);
            if (Option.isNone(stored)) {
              return yield* Effect.die(
                new Error(
                  `Historical document ${record.value.id} (${record.value.kind}) cannot be read`,
                ),
              );
            }
            return Option.some(
              yield* Effect.fromResult(materializeStored(definition, stored.value)),
            );
          }),
        ),
      ).pipe(
        Effect.withSpan("Session.snapshotAsOf", {
          attributes: spanAttributes(resolveAddress(definition, args).address),
        }),
      );

    // The public overloads resolve their arguments through the token's scope and family shape, which
    // the erased implementations read at runtime; each returns the value its token types.

    function snapshot<T>(doc: SessionDoc<T>): Snapshot<T>;
    function snapshot<T>(doc: ConversationDoc<T>, conversationId: ConversationId): Snapshot<T>;
    function snapshot<T>(doc: TaskDoc<T>, taskId: TaskId): Snapshot<T>;
    function snapshot<T, I>(doc: SessionDocFamily<T, I>, key: string): Snapshot<T>;
    function snapshot<T, I>(
      doc: ConversationDocFamily<T, I>,
      conversationId: ConversationId,
      key: string,
    ): Snapshot<T>;
    function snapshot<T, I>(doc: TaskDocFamily<T, I>, taskId: TaskId, key: string): Snapshot<T>;
    function snapshot<T>(doc: Doc<T, object>, ...args: ReadonlyArray<DocArgument>): Snapshot<T> {
      return snapshotOf(definitionOf(doc), args).pipe(Effect.map(Option.map(asValue<T>)));
    }

    function snapshotAsOf<T>(
      doc: RewindableConversationDoc<T>,
      conversationId: ConversationId,
      at: EntryId,
    ): Snapshot<T>;
    function snapshotAsOf<T, I>(
      doc: RewindableConversationDocFamily<T, I>,
      conversationId: ConversationId,
      key: string,
      at: EntryId,
    ): Snapshot<T>;
    function snapshotAsOf<T>(
      doc: Doc<T, object>,
      ...args: ReadonlyArray<DocArgument>
    ): Snapshot<T> {
      return snapshotAsOfOf(definitionOf(doc), args).pipe(Effect.map(Option.map(asValue<T>)));
    }

    function watchDoc<T>(doc: SessionDoc<T>): WatchOf<T>;
    function watchDoc<T>(doc: ConversationDoc<T>, conversationId: ConversationId): WatchOf<T>;
    function watchDoc<T>(doc: TaskDoc<T>, taskId: TaskId): WatchOf<T>;
    function watchDoc<T, I>(doc: SessionDocFamily<T, I>, key: string): WatchOf<T>;
    function watchDoc<T, I>(
      doc: ConversationDocFamily<T, I>,
      conversationId: ConversationId,
      key: string,
    ): WatchOf<T>;
    function watchDoc<T, I>(doc: TaskDocFamily<T, I>, taskId: TaskId, key: string): WatchOf<T>;
    function watchDoc<T>(doc: Doc<T, object>, ...args: ReadonlyArray<DocArgument>): WatchOf<T> {
      return acquireObserver(
        "Session.watchDoc",
        definitionOf(doc),
        args,
        watchObserver,
        (observer) => observer.stop(),
      ).pipe(
        // SAFETY: the watch holds revisions of the document the token types.
        Effect.map(Option.map((observer) => observer.watch as DocumentWatch<T>)),
      );
    }

    function observeDoc<T>(doc: SessionDoc<T>, observer: Observer<T>): ObserveOf<T>;
    function observeDoc<T>(
      doc: ConversationDoc<T>,
      conversationId: ConversationId,
      observer: Observer<T>,
    ): ObserveOf<T>;
    function observeDoc<T>(doc: TaskDoc<T>, taskId: TaskId, observer: Observer<T>): ObserveOf<T>;
    function observeDoc<T, I>(
      doc: SessionDocFamily<T, I>,
      key: string,
      observer: Observer<T>,
    ): ObserveOf<T>;
    function observeDoc<T, I>(
      doc: ConversationDocFamily<T, I>,
      conversationId: ConversationId,
      key: string,
      observer: Observer<T>,
    ): ObserveOf<T>;
    function observeDoc<T, I>(
      doc: TaskDocFamily<T, I>,
      taskId: TaskId,
      key: string,
      observer: Observer<T>,
    ): ObserveOf<T>;
    function observeDoc<T>(
      doc: Doc<T, object>,
      ...args: ReadonlyArray<DocArgument | Observer<T>>
    ): ObserveOf<T> {
      // SAFETY: every overload ends with the observer, after the address arguments.
      const observer = args.at(-1) as Observer<T>;
      // SAFETY: what comes before the observer is the address of this token's scope.
      const address = args.slice(0, -1) as ReadonlyArray<DocArgument>;
      return observeOf(definitionOf(doc), address, observer);
    }

    /**
     * On the line, as one job, loads the document at the address and
     * attaches `observer` behind `guardObserver`, so it sees every commit after the value it starts from; `None`
     * when absent. The attaching fiber governs only the attachment: interrupted while it waits, it attaches
     * nothing; interrupted after it attached, it detaches. Nothing else ties the
     * observer to the caller.
     */
    const observeOf = <T>(
      definition: AnyDefinition,
      args: ReadonlyArray<DocArgument>,
      observer: Observer<T>,
    ): Effect.Effect<Option.Option<Observation<T>>, ReadError> =>
      Effect.suspend(() => {
        let attached: Guarded<WatchFrame<T>> | undefined;
        return assertUsable(() =>
          Effect.gen(function* () {
            const resolved = resolveAddress(definition, args);
            const observation = yield* admitOnLine(
              assertHealthy(() =>
                Effect.gen(function* () {
                  const loaded = yield* loadDocument(definition, resolved.id, resolved.address);
                  if (Option.isNone(loaded)) return Option.none<Observation<T>>();
                  // SAFETY: the token types the document the observer follows.
                  const value = loaded.value.tracker.value as Readonly<T>;
                  yield* attachDocument(definition, loaded.value, (_value, detach) => {
                    const guarded = guardObserver(observer, detach, guardServices);
                    attached = guarded;
                    // SAFETY: the follower gets revisions of the document the token types, `T`.
                    return guardedFollower(guarded) as Follower<Schema.JsonObject>;
                  });
                  // SAFETY: set by the attachment that just returned.
                  return Option.some(observationOf(value, attached!));
                }),
              ),
            );
            yield* scheduleSweep;
            return observation;
          }).pipe(
            Effect.onInterrupt(() =>
              Effect.sync(() => {
                attached?.detach();
              }),
            ),
          ),
        );
      }).pipe(
        Effect.withSpan("Session.observeDoc", {
          attributes: spanAttributes(resolveAddress(definition, args).address),
        }),
      );

    /**
     * Attaches a host's commit observer behind `guardObserver`, checked
     * and registered in one synchronous step.
     */
    const observeCommits = (observer: CommitObserver) =>
      Effect.suspend((): Effect.Effect<CommitObservation, SessionFailed | SessionClosed> => {
        if (failure !== undefined) return Effect.fail(failure);
        if (closing) return Effect.fail(new SessionClosed());
        const guarded: Guarded<CommitPublication> = guardObserver(
          observer,
          () => commitListeners.delete(guarded),
          guardServices,
        );
        commitListeners.add(guarded);
        return Effect.succeed(commitObservationOf(guarded));
      });

    // Checked and registered in one synchronous step: a listener registered
    // while close begins is either refused or notified, never silently dropped.
    const register =
      <L>(listeners: Set<L>) =>
      (listener: L) =>
        Effect.suspend((): Effect.Effect<() => void, SessionFailed | SessionClosed> => {
          if (failure !== undefined) return Effect.fail(failure);
          if (closing) return Effect.fail(new SessionClosed());
          listeners.add(listener);
          return Effect.succeed(() => {
            listeners.delete(listener);
          });
        });

    const kernel: SessionKernel = {
      commit: (change, commitOptions) => commitWith(() => change, undefined, commitOptions),
      commitWith,
      ownsTransaction: (service) => transactionIdentity(service)?.host === host,
      readOnLine: (job) => admitOnLine(assertHealthy(() => job)),
      offLineStorage: storage,
      offLine,
      snapshot,
      snapshotAsOf,
      watchDoc,
      observeDoc,
      observeCommits,
      followCommits: register(internalListeners),
      subscribeClose: register(closeListeners),
      storage: lineStorage,
      touch,
      onJobEnd: (end) => {
        const job = current;
        if (job === undefined) {
          end();
          return;
        }
        if (job.ends === undefined) job.ends = [end];
        else job.ends.push(end);
      },
      // Behind the commits admitted before the seal: the line is FIFO, and the writer drained first.
      writeAtClose: (writes) =>
        onLine(
          Effect.gen(function* () {
            const drained = writer.drained();
            if (drained !== undefined) yield* drained;
            if (failure !== undefined) return;
            const batch = writes();
            if (batch.length === 0) return;
            // Stored straight, published to no one; a failure of the call fails the Session, which reports it.
            const exit = yield* Effect.exit(
              Effect.suspend(() => backend.commit([batch], () => {})).pipe(Effect.uninterruptible),
            );
            if (Exit.isFailure(exit)) fail(Writer.classify(exit.cause).failure);
            else if (exit.value.refused !== undefined) fail(exit.value.refused);
          }),
        ),
      conversationDocumentOnLine: (definition, conversationId) =>
        Effect.gen(function* () {
          const resolved = resolveAddress(definition, [conversationId]);
          const loaded = yield* loadDocument(definition, resolved.id, resolved.address);
          if (Option.isNone(loaded)) return Option.none();
          yield* Effect.fromResult(
            checkRecord(definition, loaded.value.record, loaded.value.storedVersion),
          );
          return Option.some({
            record: loaded.value.record,
            version: loaded.value.valueVersion,
            value: loaded.value.tracker.value,
          });
        }),
      latestEntryId,
      knownLatestEntryId: tails.peek,
      latestHeadMarker,
      now,
      fail,
      get failure() {
        return Option.fromUndefinedOr(failure);
      },
      get closing() {
        return closing;
      },
      whenFailed: Deferred.await(failed),
      closed: Deferred.await(closedDeferred),
      close,
      // A `*` barrier, so no document with an unpublished change leaves the cache.
      unloadDocuments: admitOnLine(
        Effect.gen(function* () {
          const drained = writer.drained();
          if (drained !== undefined) yield* drained;
          // Held changes are stored first.
          if (failure === undefined) yield* storeHolds(allHolds);
          documents.clear();
          idleDocuments.clear();
        }),
      ),
      get readCacheSizes() {
        return {
          records: cache.sizes(),
          readsInFlight: cache.readsInFlight(),
          tails: tails.size(),
          markers: tails.markers(),
          taskDocuments: taskDocuments.size,
        };
      },
      get lineSizes() {
        return { outstanding, unpublished: writer.unpublished(), reads: cache.readsInFlight() };
      },
      get documentCacheSizes() {
        return {
          cached: documents.size,
          idle: idleDocuments.size,
          attached: attachedDocuments.size,
          sweeps: sweepJobs,
          held: holds.size,
        };
      },
      get heldDeadline() {
        return holdArmedAt;
      },
    };
    return kernel;
  });

/** A `durable.submission` event's attributes. */
type SubmissionEventAttributes = {
  "durable.submission.id": number;
  "durable.submission.status": "done" | "unanswered";
  "durable.answer.entry_id"?: number;
};

/** Span attributes of a commit: the conversation and task it is bound to, when any. */
const commitAttributes = (scope: TransactionScope | undefined): SpanAttributes => {
  let attributes: SpanAttributes = {};
  if (scope?.conversationId !== undefined) {
    attributes = { ...attributes, "durable.conversation.id": scope.conversationId };
  }
  if (scope?.taskId !== undefined) attributes = { ...attributes, "durable.task.id": scope.taskId };
  return attributes;
};

const isStorageFailure = Schema.is(StorageFailure);
const isStorageRequestError = Schema.is(StorageRequestError);

/**
 * The `Observation` of a guarded observer attached at `value`. A module
 * function, so a detached observation keeps nothing but `value` and the spent guard alive.
 */
const observationOf = <T>(value: T, guarded: Guarded<WatchFrame<T>>): Observation<T> => ({
  value,
  detach: () => guarded.detach(),
});

/** The `CommitObservation` of a guarded commit observer; a module function, as `observationOf`. */
const commitObservationOf = (guarded: Guarded<CommitPublication>): CommitObservation => ({
  detach: () => guarded.detach(),
});

/** Runs a callback that must not fail its caller; a throw goes to `onError`. */
const contained = (callback: () => void, onError: (error: Error) => void) => {
  const result = Result.try({
    try: callback,
    catch: (error) => (error instanceof Error ? error : new Error(String(error))),
  });
  if (Result.isFailure(result)) onError(result.failure);
};

/** The public `Session` service over a kernel, for `Session.layer` and `Harness.layer`. */
export const serviceOf = (kernel: SessionKernel): Session["Service"] => ({
  commit: kernel.commit,
  snapshot: kernel.snapshot,
  snapshotAsOf: kernel.snapshotAsOf,
  watchDoc: kernel.watchDoc,
  observeDoc: kernel.observeDoc,
  observeCommits: kernel.observeCommits,
  closed: kernel.closed,
});

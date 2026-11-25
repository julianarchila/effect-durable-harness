// The durable task scheduler of one Harness.
//
// The ownership index (`ownership.ts`) mirrors every committed non-terminal
// task record in `live`. The synchronous internal commit listener updates it
// on the Session line, so code running on the line reads exactly the committed
// state from it, and every set derived from a publication changes in that
// same synchronous step.
//
// Invariant: every task transition is decided and written by one callback on
// the Session line. That covers reservation, marks, runtime commits,
// finalization and the step before each phase, which applies the precedence
// rules and writes a fault. Handlers and joins run off the line. An
// invocation ends inside the step that decides its end, so a runtime commit it
// queued either lands before that decision or is rejected.
//
// Each such callback loads what it needs from storage, decides in one
// synchronous step with a pure function of `decisions.ts`, registers what it
// decided in that same step, and then applies the plan's writes in order.
//
// Design notes:
// - The registry is static: no registry subscription, no handover at a
//   phase boundary, and a blocked task is reconsidered only by the next
//   Harness.
// - An invocation's signal is the interruption of its handler's fiber. The
//   invocation fiber runs uninterruptibly, except for the handler, so the
//   steps around it always run.
// - Deferred passes (kick, reconcile, sweep) are fibers that wait for a
//   signal; a signal resumes them after a yield, so they never run on the
//   stack of the commit that fired them.
import * as Cause from "effect/Cause";
import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as Data from "effect/Data";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import type * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as Predicate from "effect/Predicate";
import * as Result from "effect/Result";
import * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";
import * as Tracer from "effect/Tracer";
import type { Agent } from "../Agent.ts";
import type { StorageFailure, StorageRequestError, StorageWrite } from "../AgentStorage.ts";
import type { ExecutionEnv, ExecutionEnvError, ExecutionEnvTarget } from "../ExecutionEnv.ts";
import type { HookFunction } from "../Hook.ts";
import type {
  AbortTaskResult,
  BlockedReason,
  HarnessInspection,
  TaskInspection,
} from "../Harness.ts";
import { type ConversationId, type EntryId, type SubmissionId, type TaskId } from "../Ids.ts";
import type {
  ConversationRecord,
  EntryRecord,
  TaskOutcome,
  TaskRecord,
  TaskState,
} from "../Records.ts";
import * as Doc from "../Doc.ts";
import type { CommitOptions, ReadError } from "../Session.ts";
import {
  type CommitRejected,
  NotFound,
  type ReadOnlySession,
  SessionClosed,
  type SessionFailed,
} from "../SessionError.ts";
import { IdleTimers } from "../IdleTimers.ts";
import { type ReportInfo, TaskFaulted, TaskInterrupted } from "../Report.ts";
import type { WakeupHost, Wake } from "../SessionWakeups.ts";
import { DEFAULT_RESTART_LIMIT, type Settings } from "../Settings.ts";
import type {
  AdmitError,
  ConversationBusy,
  ConversationIdle,
  Submission,
  SubmissionDraft,
  SubmissionError,
} from "../Submission.ts";
import * as Task from "../Task.ts";
import * as Telemetry from "../Telemetry.ts";
import {
  AbortMarked,
  InvalidSleep,
  InvalidWait,
  InvocationEnded,
  NotOwned,
  NotRunning,
  NotTerminal,
  TaskError,
} from "../TaskError.ts";
import { Tx } from "../Tx.ts";
import type { DocArgument } from "./documents.ts";
import * as Calls from "./calls.ts";
import * as SignalMemos from "./signalMemos.ts";
import * as Decisions from "./decisions.ts";
import {
  causeMessage,
  type ErasedHandler,
  type ErasedRuntime,
  type Failure,
  type Json,
  missingMigration,
  type PhaseResult,
  phaseOf,
  type RunningRecord,
  withAbortMark,
  withState,
  erased,
} from "./decisions.ts";
import * as Keys from "./keys.ts";
import * as Ownership from "./ownership.ts";
import * as Signal from "./signal.ts";
import * as telemetry from "./telemetry.ts";
import * as Waiters from "./waiters.ts";
import {
  cancellationIntent,
  conversationNode,
  failedOutcome,
  type Overlay,
  parentOf,
  taskNodeOf,
  type TaskNode,
  type Up,
} from "./ownership.ts";
import { agentHooks } from "./agent.ts";
import { LISTENER, type Reporter, reportTask } from "./report.ts";
import { advanceRange, type ContextRange } from "./context.ts";
import { readContextFrom } from "./contextRead.ts";
import { scanAll } from "./scan.ts";
import type { ObservedCommit, SessionKernel } from "./session.ts";
import type { Transaction, TransactionScope } from "./transaction.ts";

const SCAN_PAGE_SIZE = 256;

const LIVE_STATUSES = ["pending", "running", "waiting", "sleeping", "completing"] as const;

type ErasedArguments = [doc: Doc.Doc<Json, object>, ...args: ReadonlyArray<DocArgument>];
type ErasedRead = (...args: ErasedArguments) => Effect.Effect<Option.Option<Json>, ReadError>;
type ErasedWatch = (
  ...args: ErasedArguments
) => Effect.Effect<Option.Option<Doc.DocumentWatch<Json>>, ReadError, Scope.Scope>;

/** One in-memory execution of a task in run or abort mode. */
interface Invocation {
  readonly taskId: TaskId;
  readonly conversationId: ConversationId;
  readonly mode: "run" | "abort";
  ended: boolean;
  /**
   * Set in the commit listener's step that sees the run's task newly abort-marked. The interrupt loop interrupts
   * the handler after it, once `notifyHosts` has run in the same step, so host observers still see the commit
   * before the abort. Bound handle operations refuse from then on.
   */
  aborted: boolean;
  /** Completed when the invocation ends or is abort-marked; detached waits bound to it fail then. */
  readonly stopSignal: Deferred.Deferred<void>;
  /** Completed once the invocation's fiber has finished. */
  readonly done: Deferred.Deferred<void>;
  fiber: Fiber.Fiber<void> | undefined;
  /** Set when the scheduler signals the invocation before its fiber exists. */
  signalled: boolean;
  /**
   * Resources bound to the invocation, closed when it ends: the watches
   * acquired through the runtime. A child of the scheduler's scope; once closed, a resource acquired in it is
   * released at once.
   */
  readonly scope: Scope.Closeable;
  /**
   * The agent of the running phase, resolved at most once, at first use; reset at
   * every phase start. Resolved on a fiber of its own, so a caller that stops waiting cancels only its wait.
   */
  phaseAgent: Deferred.Deferred<Agent, AgentError> | undefined;
  /** The task's definition name, which its hooks name. */
  readonly kind: string;
  /** The task's creation-time links, on the phase spans of its first invocation only. */
  readonly links: ReadonlyArray<Telemetry.TraceCarrier> | undefined;
  /** The open `durable.phase` span, for the phase's checkpoint events. */
  span: Tracer.Span | undefined;
  /** The task's latest committed record while a phase runs, for the phase's outcome. */
  record: TaskRecord | undefined;
}

/** Checked first on the line by a bound operation: the error that refuses it, if any. */
export type Refuse = () => TaskError | undefined;

/** Errors of an agent resolution: its reads, and a settings reader that throws, as a defect. */
type AgentError = StorageError | ClosedError;

interface Reservation {
  readonly invocation: Invocation;
  readonly task: Task.Any;
}

/** Errors of the scheduler's own commits and reads. */
type StorageError = StorageFailure | StorageRequestError | SessionFailed;

/** Errors of a wait that close or a failure ends. */
type ClosedError = SessionClosed | SessionFailed;

export interface SchedulerOptions {
  readonly kernel: SessionKernel;
  /** The static registry: task definitions by name. */
  readonly tasks: ReadonlyMap<string, Task.Any>;
  /** Services the handlers require, captured when the Harness was built. */
  readonly services: Context.Context<never>;
  /** The host's wakeups; without one, the scheduler wakes due sleeps itself. */
  readonly wakeups: WakeupHost | undefined;
  /** Kinds whose run phases end the task themselves at `settings.restartLimit`: the built-in tasks. */
  readonly limitsRestarts: (kind: string) => boolean;
  /**
   * Whether the `abandonOnRestart` children of `owner` are kept after a restart: its record says it was not cut off
   * while running, as a tool call parked or not started yet. Wins over the child's flag.
   */
  readonly keepsAbandoned: (owner: TaskRecord) => boolean;
  /**
   * Whether a sleeping task is parked at gates its phase asks again on every wake, as a tool call before its
   * intent: every open wakes it, so the gates installed then decide.
   */
  readonly gated: (record: TaskRecord) => boolean;
  /** Forwards a failure that fails no caller to `onReport`, with what it is about. */
  readonly report: Reporter;
  /** The extension that installed a task kind's definition; none for the built-in tasks. */
  readonly extensionOf: (kind: string) => string | undefined;
  /**
   * Open loads live tasks as committed, `running` ones included,
   * and the scheduler never runs a task, reconciles or reports a wake.
   */
  readonly readOnly: boolean;
  /** Harness cleanup staged in the commit that makes an outcome the scheduler wrote itself terminal. */
  readonly settleOutcome: (
    tx: Transaction,
    record: TaskRecord,
    outcome: Decisions.SchedulerOutcome,
  ) => Effect.Effect<void, StorageError, Tx>;
  /** Withdraws a conversation's queued inputs, for conversation aborts and cascades. */
  readonly withdrawInputs: (
    tx: Transaction,
    conversationId: ConversationId,
  ) => Effect.Effect<void, StorageError, Tx>;
  /** Fibers of the scheduler: its loops and invocations. */
  readonly scope: Scope.Scope;
  /** The conversation's agent resolved now from its committed `core.agent`. */
  readonly agentOf: (conversationId: ConversationId) => Effect.Effect<Agent, AgentError>;
  /** The settings resolved now; a reader that throws throws. */
  readonly settings: () => Settings;
  /** The conversation's environment built now from its current `cwd`; `undefined` without a builder. */
  readonly envOf: (
    target: Omit<ExecutionEnvTarget, "cwd">,
  ) => Effect.Effect<ExecutionEnv | undefined, ExecutionEnvError>;
  /**
   * `Conversation.submit` of an input, for an invocation-bound conversation handle. `refuse` is checked as the
   * first step of the admission commit on the line; an error it returns fails the submit and writes nothing.
   */
  readonly submit: (
    conversationId: ConversationId,
    draft: SubmissionDraft,
    refuse: Refuse,
  ) => Effect.Effect<
    Submission,
    | StorageError
    | ClosedError
    | CommitRejected
    | ConversationBusy
    | ConversationIdle
    | SubmissionError
    | TaskError
  >;
  /** `Conversation.admit` for an invocation-bound conversation handle; `refuse` as for `submit`. */
  readonly admit: (
    conversationId: ConversationId,
    draft: SubmissionDraft,
    refuse: Refuse,
  ) => Effect.Effect<SubmissionId, AdmitError | TaskError, Tx>;
}

export interface Scheduler {
  /**
   * Loads live tasks and changes surviving `running` tasks back to `pending`, counting the restart. Dispatches
   * nothing; a task cut off `settings.restartLimit` times ends when it is next reserved.
   */
  readonly open: Effect.Effect<void, StorageError | SessionClosed>;
  /** Enables scheduling and makes due sleeps runnable: a host wakeup calls it. Idempotent; nothing once closing. */
  readonly resume: () => void;
  /** Interrupts every invocation; close calls it synchronously right after its listeners. */
  readonly signalAll: () => void;
  /**
   * At a commit's admission: the ownership memberships its task and
   * conversation changes write. For each task created or made terminal, every node from its
   * parent up to and including the nearest live task, or the root conversation, and then the roots too; for each
   * conversation created under a task, that task; and the intent set when a change alters it. Loads the chains
   * it walks.
   */
  readonly admitting: (
    tx: Transaction,
  ) => Effect.Effect<ReadonlyArray<string>, StorageFailure | SessionFailed>;
  /**
   * Interrupts every invocation and waits for them, then writes the tasks they ran back as `pending`, as a close
   * is not a crash. Close runs it after its listeners.
   */
  readonly join: Effect.Effect<void>;
  readonly abort: (
    id: TaskId,
    keepRestart?: boolean,
  ) => Effect.Effect<AbortTaskResult, TaskError | StorageError | SessionClosed | ReadOnlySession>;
  readonly awaitTask: (
    id: TaskId,
  ) => Effect.Effect<TaskRecord, TaskError | StorageError | ClosedError>;
  readonly awaitIdle: (
    conversationId: ConversationId | undefined,
  ) => Effect.Effect<void, ClosedError>;
  readonly abortConversation: (
    conversationId: ConversationId,
    background: boolean,
  ) => Effect.Effect<void, TaskError | StorageError | ClosedError>;
  /** Scheduling state and every live task with its derived state; run it on the line. */
  readonly inspect: Effect.Effect<Pick<HarnessInspection, "scheduling" | "tasks">, StorageError>;
  /** The current wake; read it on the line. */
  readonly wake: () => Wake;
  /** Why a live task cannot run here, read by the task graph. */
  readonly blockedReason: (subject: Decisions.BlockedSubject) => BlockedReason | undefined;
  /**
   * Calls `listener` when blocked reasons may have changed
   * without a commit. With a static registry that is only a failed migration. Returns the unsubscribe.
   */
  readonly subscribeBlocked: (listener: () => void) => () => void;
  /** When the scheduler's own timer for the earliest sleep fires, if it keeps one; for tests. */
  readonly sleepTimerAt: () => number | undefined;
  /** Pending task and idle waits, for tests that check an interrupted wait leaves none behind. */
  readonly waiterCount: () => number;
  /** Sizes of the ownership indexes, for tests that bound the scheduler's memory. */
  /** The number of kept context reads, for tests. */
  readonly keptContexts: () => number;
  /**
   * The kept contexts' idle state, for tests (a diagnostic computed on
   * demand): how many are kept and idle, how many disagree with a full recompute, and how many idle checks
   * covered every scope.
   */
  readonly idleState: () => Readonly<
    Record<"contexts" | "idleContexts" | "idleMismatches" | "fullIdleChecks", number>
  >;
  readonly indexSizes: () => Readonly<
    Record<
      "live" | "settled" | "edges" | "below" | "roots" | "unloaded" | "dropQueue" | "pins",
      number
    >
  >;
}

/** Builds the scheduler of one Harness. */
export const make = (options: SchedulerOptions): Effect.Effect<Scheduler> =>
  Effect.gen(function* () {
    const kernel = options.kernel;
    const storage = kernel.storage;
    const tasks = options.tasks;
    const report = options.report;
    const clock = yield* Clock.Clock;
    // An expiry timer is kept only where timers can be unreferenced; the host says whether this is such a platform.
    const idleTimers = (yield* IdleTimers).enabled;

    /**
     * The kernel's commit for the scheduler's own writes. On a read-only Harness the scheduler commits no
     * writes: its open writes nothing there and the Harness refuses every call that would run tasks first,
     * so a `ReadOnlySession` here is a bug.
     */
    const commitWith = <A, E>(
      change: (tx: Transaction) => Effect.Effect<A, E, Tx>,
      scope?: TransactionScope,
      options?: CommitOptions,
    ) =>
      kernel.commitWith(change, scope, options).pipe(
        Effect.catchIf(
          (error): error is ReadOnlySession => Predicate.isTagged(error, "ReadOnlySession"),
          (error) => Effect.die(error),
        ),
      );

    /** Keys the decision region underway reads (`region`); set only while one runs. */
    let reading: Set<string> | undefined;
    const index = Ownership.make({
      onDropQueued: () => scheduleSweep(),
      record: (key) => {
        reading?.add(key);
      },
    });

    /**
     * On the line: runs `decide`, a decision region:
     * synchronous, reading scheduler memory and recording the keys it reads, staging nothing and changing no
     * scheduler state. When a key it read has an unpublished writer, its result, or its defect, is dropped: once
     * that writer is published, `refresh` reloads what the region needs and it runs again. A waiting job holds
     * the line, so unpublished writes only shrink, and the runs converge. What a region read that had no
     * unpublished writer gets none for the rest of the job, so its result stands while the job applies it.
     *
     * `settle`, when given, makes the decision from the clean result in the same synchronous step as its check, so
     * no publication lands between them. It is for what may run only once: `reserve` and the abort probe without
     * migrations and plan with them there, reading the same keys.
     */
    const region = <A, E = never, R = never>(
      decide: () => A,
      refresh?: Effect.Effect<unknown, E, R>,
      settle?: (probe: A) => A,
    ): Effect.Effect<A, E | SessionFailed, R> =>
      Effect.gen(function* () {
        for (;;) {
          const keys = new Set<string>();
          reading = keys;
          const result = Result.try({ try: decide, catch: (error) => error });
          reading = undefined;
          const gates: Array<Effect.Effect<void, SessionFailed>> = [];
          for (const key of keys) {
            const gate = kernel.touch(key);
            if (gate !== undefined) gates.push(gate);
          }
          if (gates.length === 0) {
            if (Result.isFailure(result)) return yield* Effect.die(result.failure);
            if (settle === undefined) return result.success;
            const settled = Result.try({
              try: () => settle(result.success),
              catch: (error) => error,
            });
            if (Result.isFailure(settled)) return yield* Effect.die(settled.failure);
            return settled.success;
          }
          yield* Effect.all(gates, { discard: true });
          if (refresh !== undefined) yield* refresh;
        }
      });

    /**
     * On the line: waits until every commit admitted before is
     * published, for a decision that reads what cannot be keyed: every live task (inspection) or every task a
     * conversation abort reaches.
     */
    const published = Effect.suspend(() => kernel.touch(Keys.ALL_KEYS) ?? Effect.void);

    /** Loads the chains a region walks, again after it waited. */
    const reloadScopes = Effect.suspend(() => loadScopes(false)).pipe(internalReads);
    const invocations = new Map<TaskId, Invocation>();
    /** Invocations with an open phase span, by task: the commit listener keeps their record. */
    const phasing = new Map<TaskId, Invocation>();
    /** Tasks loaded at open whose first phase in this process has not started yet. */
    const resumed = new Set<TaskId>();
    /** The scheduler's context, for the metrics it updates from synchronous code. */
    const reportTelemetry = (error: Error) => report(Cause.die(error), telemetry.TELEMETRY);
    /** Publication must not throw, so a throw is reported. */
    const countActive = (delta: 1 | -1, kind: string) =>
      telemetry.update(
        `durable.tasks.active\u0000${kind}`,
        () => Telemetry.tasksActive({ task: kind }),
        delta,
        reportTelemetry,
      );
    const taskWaiters = Waiters.make<TaskId, TaskRecord, ClosedError>();
    /** Idle waiters by conversation; `undefined` waits for the whole Harness. */
    const idleWaiters = Waiters.make<ConversationId | undefined, void, ClosedError>();
    /** Definition whose migration failed per task; with a static registry, for this Harness's lifetime. */
    const failedMigrations = new Map<
      TaskId,
      { readonly task: Task.Any; readonly message: string }
    >();
    /** `failFast` waiters the next reconcile checks for a failed task in `on`. */
    const failFastChecks = new Set<TaskId>();
    /**
     * Tasks whose invocation ended once closing began: close writes those still `running` back as `pending`.
     * Close signals the invocations as it seals, before it joins them, so some have ended by then.
     */
    const stoppedAtClose = new Set<TaskId>();
    /** Live `abandonOnRestart` tasks found at open; the first reservation pass marks them `restart`. */
    const abandoned = new Set<TaskId>();
    let reconcileScheduled = false;
    let sweepScheduled = false;
    let cascadePending = false;
    let enabled = false;
    let closing = false;
    let dirty = false;
    let draining = false;

    const reconcileSignal = Signal.make();
    const drainSignal = Signal.make();
    const sweepSignal = Signal.make();
    const wakeSignal = Signal.make();
    /** Run invocations the commit listener signalled; the interrupt loop interrupts their handlers. */
    const signalQueue = new Set<Invocation>();
    const interruptSignal = Signal.make();
    /** Scopes of ended invocations: one closer loop closes them off the line, in order. */
    const closeQueue: Array<Scope.Closeable> = [];
    const closeSignal = Signal.make();
    /**
     * Kept context reads by conversation: a busy conversation keeps its last read, so its next
     * run reads only newer entries; an idle one keeps it for `contextRetentionMs`.
     */
    const contexts = new Map<
      ConversationId,
      {
        range: ContextRange;
        idleSince: number | undefined;
        /** When its idle period ends; fixed when it went idle. */
        expiresAt: number | undefined;
      }
    >();
    /**
     * The idle kept contexts by the start of their idle period, oldest first, so expiry
     * visits only what expired.
     */
    const idleOrder = new Map<ConversationId, number>();
    /** The newest idle period in `idleOrder`. */
    let idleNewest = Number.NEGATIVE_INFINITY;
    /**
     * A chain load resolved what `index.idle` treats as unknown for a live task
     * whose chain is not loaded, which can make a scope idle without a task change below it.
     */
    let unknownsChanged = false;
    /** Idle checks that covered every scope, for tests. */
    let fullIdleChecks = 0;
    /** When the earliest idle context expires; the expiry loop sleeps until then. */
    let expiryAt: number | undefined;
    const expirySignal = Signal.make();
    /**
     * When the scheduler's own timer for the earliest sleep fires: kept only
     * without a host's wakeups, and only while scheduling runs. The sleep loop waits until then.
     */
    let sleepAt: number | undefined;
    const sleepSignal = Signal.make();
    /**
     * One wake report at a time: open reports inline while the wake loop may run, and each report computes the
     * wake under the permit, so the host hears them in the order they were computed.
     */
    const wakeLock = Semaphore.makeUnsafe(1);

    const now = () => clock.currentTimeMillisUnsafe();

    const closedError = (): ClosedError =>
      Option.getOrElse(kernel.failure, (): ClosedError => new SessionClosed());

    // ─── Scheduling ─────────────────────────────────────────────────────────

    const observe = (publication: ObservedCommit): void => {
      advanceContexts(publication);
      const updated: Array<TaskRecord> = [];
      const failed: Array<TaskId> = [];
      let changed = false;
      // The conversations whose idleness the commit's task changes can change; undefined when a chain is not
      // known.
      let dirty: Set<ConversationId> | undefined = new Set();
      // Edges first, so the tasks a commit creates in a new conversation hang from it.
      for (const change of publication.changes) {
        if (change.type === "conversation" && index.edgeOf(change.value.id) === undefined) {
          index.setEdge(change.value.id, change.value.owner?.taskId ?? null);
        }
      }
      for (const change of publication.changes) {
        if (change.type !== "task") continue;
        changed = true;
        const record = change.value;
        const previous = index.live.get(record.id);
        if (failedOutcome(record) && (previous === undefined || !failedOutcome(previous))) {
          failed.push(record.id);
        }
        index.track(previous, record);
        // Idleness counts live tasks, so only a task that became live or ended can change it, and the
        // inactivity of kept contexts also a task that parked or woke. Collected before the sweep, which may
        // forget the chain of a task that ended.
        const liveness =
          previous === undefined ||
          record.state.status === "terminal" ||
          Ownership.parked(previous) !== Ownership.parked(record);
        if (liveness && dirty !== undefined && !addScopesAbove(record, dirty)) dirty = undefined;
        const phased = phasing.get(record.id);
        if (phased !== undefined) phased.record = record;
        if (record.state.status === "terminal") {
          if (previous !== undefined && !closing) countActive(-1, record.kind);
          resumed.delete(record.id);
        } else if (previous === undefined && !closing) {
          countActive(1, record.kind);
        }
        if (record.state.status === "terminal") {
          failedMigrations.delete(record.id);
          failFastChecks.delete(record.id);
          taskWaiters.resolve(record.id, record);
          // Its owner may finalize now.
          scheduleReconcile();
          continue;
        }
        if (record.abortRequested && previous?.abortRequested !== true) {
          cascadePending = true;
          // Signal a run invocation of the newly marked task; its next step ends it. The flag is set in this
          // step, so a bound operation queued behind this commit refuses when its turn comes.
          const invocation = invocations.get(record.id);
          if (invocation?.mode === "run") {
            invocation.aborted = true;
            signalInvocation(invocation);
          }
        }
        // A request replacing a `restart` mark upgrades the marks of the work below.
        if (previous?.abortReason !== undefined && record.abortReason === undefined) {
          cascadePending = true;
        }
        const status = record.state.status;
        if (status === "completing" && previous?.state.status !== "completing") {
          if (cancellationIntent(record)) cascadePending = true;
          scheduleReconcile();
        }
        if (
          status === "waiting" &&
          record.state.policy === "failFast" &&
          previous?.state.status !== "waiting"
        ) {
          failFastChecks.add(record.id);
          scheduleReconcile();
        }
        // Its owners and place never change: what is above it is checked once, when it becomes index.live.
        if (previous === undefined) updated.push(record);
      }
      for (const id of failed) {
        for (const waiter of index.waiters.get(id) ?? []) {
          const state = index.live.get(waiter)?.state;
          if (state?.status === "waiting" && state.policy === "failFast") {
            failFastChecks.add(waiter);
            scheduleReconcile();
          }
        }
      }
      for (const change of publication.changes) {
        // A queued input below a cancelled owner is withdrawn, even after its cascade.
        if (
          change.type !== "submission" ||
          change.value.status !== "queued" ||
          change.value.type !== "input"
        ) {
          continue;
        }
        // Only an owner with cancellation intent withdraws it; a later intent's mark runs the cascade then.
        if (index.intent.size === 0) continue;
        const conversation = change.value.conversationId;
        if (
          !index.known(conversationNode(conversation)) ||
          index.belowCancelled({ conversation })
        ) {
          cascadePending = true;
        }
      }
      for (const record of updated) {
        // Work created below a cancelled owner, even after its cascade, is aborted too.
        if (index.unloaded.has(record.id)) scheduleReconcile();
        else if (
          !record.background &&
          !record.abortRequested &&
          index.belowCancelled(parentOf(record))
        ) {
          cascadePending = true;
        }
      }
      // A cascade that a reservation or step found pending runs with the next commit of any kind.
      if (cascadePending) scheduleReconcile();
      index.sweep();
      if (changed) {
        settleIdle(dirty);
        kick();
      }
      noteWake();
    };

    /**
     * Adds the conversations above `record` to `scopes`,
     * those whose idleness a change of it can change. False when a node on the way up is not known. A
     * conversation already in `scopes` has everything above it there too.
     */
    // The walk stops at the first live task above. A conversation above a live task stays busy whatever changes
    // below it (or, above a background one, never counts it), and a live task the same commit changes walks up
    // itself. Walking to the root would be O(depth) per change: a 20,000-deep chain (`HarnessOwnership`,
    // `HarnessSchedulerRetention`) made that quadratic.
    const addScopesAbove = (record: TaskRecord, scopes: Set<ConversationId>): boolean => {
      for (const step of index.above(parentOf(record))) {
        if ("unknown" in step) return false;
        if ("task" in step && index.live.has(step.task)) return true;
        if (!("conversation" in step)) continue;
        if (scopes.has(step.conversation)) return true;
        scopes.add(step.conversation);
      }
      return true;
    };

    /**
     * Extends each kept context range with the entries a
     * stored commit appended to its conversation, so a later read needs no storage scan. Entries are published
     * in ID order.
     */
    const advanceContexts = (publication: ObservedCommit) => {
      if (contexts.size === 0) return;
      let appended: Map<ConversationId, Array<EntryRecord>> | undefined;
      for (const change of publication.changes) {
        if (change.type !== "entry" || !contexts.has(change.value.conversationId)) continue;
        appended ??= new Map();
        const entries = appended.get(change.value.conversationId);
        if (entries === undefined) appended.set(change.value.conversationId, [change.value]);
        else entries.push(change.value);
      }
      if (appended === undefined) return;
      for (const [conversationId, entries] of appended) {
        const kept = contexts.get(conversationId);
        if (kept === undefined) continue;
        // An idle range past its retention is dropped here too: entry-only commits do not settle idleness,
        // and a host may decline the expiry timer. `expiresAt` was fixed when it went idle.
        if (
          kept.idleSince !== undefined &&
          kept.expiresAt !== undefined &&
          kept.expiresAt <= now()
        ) {
          dropContext(conversationId);
          continue;
        }
        const range = advanceRange(kept.range, entries);
        if (range === undefined) dropContext(conversationId);
        else kept.range = range;
      }
    };

    /**
     * After task changes below the conversations in `scopes`
     * (every conversation when undefined), resolves their idle waiters and the Harness-wide ones, drops the
     * kept contexts whose idle period passed the retention, then starts or ends the idle periods of the kept
     * contexts in `scopes`. A task change can change the idleness only of the conversations above it, so no
     * check visits every conversation.
     */
    const settleIdle = (scopes: ReadonlySet<ConversationId> | undefined) => {
      const all = scopes === undefined || unknownsChanged;
      unknownsChanged = false;
      if (all) fullIdleChecks++;
      resolveIdleWaiters(all ? undefined : scopes);
      const at = now();
      const retention = contextRetentionMs();
      // Expired first: a context past its retention is dropped even when the same commit makes it busy again.
      expireContexts(at, retention);
      for (const conversationId of all ? Array.from(contexts.keys()) : scopes) {
        const kept = contexts.get(conversationId);
        if (kept === undefined) continue;
        if (!index.inactive(conversationId)) {
          if (kept.idleSince === undefined) continue;
          kept.idleSince = undefined;
          idleOrder.delete(conversationId);
        } else if (retention <= 0) {
          dropContext(conversationId);
        } else if (kept.idleSince === undefined) {
          kept.idleSince = at;
          kept.expiresAt = at + retention;
          markIdle(conversationId, at);
        }
      }
      scheduleExpiry();
    };

    /** Resolves the idle waiters of the conversations in `scopes` (all when undefined) and the Harness-wide ones. */
    const resolveIdleWaiters = (scopes: ReadonlySet<ConversationId> | undefined) => {
      for (const conversationId of idleWaiters.keys()) {
        if (conversationId !== undefined && scopes !== undefined && !scopes.has(conversationId)) {
          continue;
        }
        if (index.idle(conversationId)) idleWaiters.resolve(conversationId, undefined);
      }
    };

    /**
     * Drops the kept contexts idle for at least
     * `retention`, oldest first, with the retention setting of now; with no retention, every idle one, also
     * those whose idle period a clock that went back makes start after now.
     */
    const expireContexts = (at: number, retention: number) => {
      for (const [conversationId, idleSince] of idleOrder) {
        if (retention > 0 && at - idleSince < retention) return;
        dropContext(conversationId);
      }
    };

    /**
     * Starts the idle period of a kept context in the expiry
     * order. One older than the newest kept, which a clock that went back starts, sorts the order again.
     */
    const markIdle = (conversationId: ConversationId, idleSince: number) => {
      idleOrder.delete(conversationId);
      idleOrder.set(conversationId, idleSince);
      if (idleOrder.size === 1 || idleSince >= idleNewest) {
        idleNewest = idleSince;
        return;
      }
      const sorted = Array.from(idleOrder).toSorted((a, b) => a[1] - b[1]);
      idleOrder.clear();
      for (const [id, since] of sorted) idleOrder.set(id, since);
      idleNewest = sorted.at(-1)?.[1] ?? Number.NEGATIVE_INFINITY;
    };

    /** Forgets a kept context, idle or not, and its place in the expiry order. */
    const dropContext = (conversationId: ConversationId) => {
      contexts.delete(conversationId);
      idleOrder.delete(conversationId);
    };

    /**
     * The setting, or 0 when the host's settings throw. Kept
     * contexts are only a cache, so dropping them is safe, while a throw here would escape the commit listener.
     */
    const contextRetentionMs = (): number => {
      try {
        return options.settings().contextRetentionMs;
      } catch (error) {
        report(Cause.die(error), SETTINGS);
        return 0;
      }
    };

    /**
     * Runs `settleIdle` when the earliest idle context expires.
     */
    // The expiry is a `Clock` sleep on a fiber of the Harness's scope instead of a host background timer; it is
    // no active work and the scope's close ends it. Where the host turns `IdleTimers` off, as on Cloudflare, none
    // is kept and only task changes expire contexts.
    const scheduleExpiry = () => {
      const oldest = idleOrder.values().next();
      let at = oldest.done === true ? undefined : oldest.value + contextRetentionMs();
      if (closing || !idleTimers) at = undefined;
      if (expiryAt === at) return;
      expiryAt = at;
      expirySignal.fire();
    };

    /** The expiry loop's pass: sleeps until `expiryAt`, or until it changes, and settles then. */
    const awaitExpiry = Effect.suspend(() => {
      const due = expiryAt;
      if (due === undefined) return Effect.void;
      // A new deadline ends this sleep: the loop's next pass re-reads `expiryAt`. Only the sleep is raced; a
      // fire that comes while the pass settles is kept by the signal for the next pass.
      return Effect.raceFirst(
        Effect.sleep(Duration.millis(Math.max(0, due - now()))).pipe(Effect.as(true)),
        expirySignal.wait.pipe(Effect.as(false)),
      ).pipe(
        Effect.flatMap((expired) =>
          Effect.sync(() => {
            if (expired && expiryAt === due) {
              expiryAt = undefined;
              expireContexts(now(), contextRetentionMs());
              scheduleExpiry();
            }
            // The loop waits on the signal before each pass; this pass ended without consuming a fire.
            if (expired || expiryAt !== undefined) expirySignal.fire();
          }),
        ),
      );
    });

    // ─── Ownership ──────────────────────────────────────────────────────────

    const scheduleReconcile = () => {
      // A read-only Harness reconciles nothing.
      if (reconcileScheduled || closing || options.readOnly) return;
      reconcileScheduled = true;
      reconcileSignal.fire();
      noteWake();
    };

    /**
     * One commit that applies what committed records imply: abort marks below live owners with
     * cancellation intent, `failFast` marks, withdrawn queued inputs below
     * cancelled owners, and the final terminal record of every `completing` task whose ordinary owned work
     * is gone. The durable records are the intent, so this also repairs whatever a crash left unapplied.
     */
    const reconcile = Effect.suspend(() => {
      reconcileScheduled = false;
      const cascade = cascadePending;
      cascadePending = false;
      const checks = Array.from(failFastChecks);
      failFastChecks.clear();
      return commitWith((tx) =>
        Effect.gen(function* () {
          if (closing) return;
          const queued = yield* loadScopes(cascade);
          const failed: Array<TaskRecord> = [];
          for (const id of checks) {
            const waiter = yield* region(() => index.live.get(id));
            if (waiter?.state.status !== "waiting") continue;
            if (yield* anyFailed(waiter.state.on)) failed.push(waiter);
          }
          // A decision region: what it marks reads records and walks the tree.
          const plan = yield* region(
            () => Decisions.planReconcile(index, failed, queued),
            reloadScopes,
          );
          for (const record of plan.marks) yield* tx.setTask(record);
          for (const id of plan.withdraw) yield* options.withdrawInputs(tx, id);
          yield* finalize(tx);
        }),
      ).pipe(
        // No task code runs in this commit: a failure is a storage failure, a host callback, or a bug. None
        // is fixed by running the pass again, so it fails the Session, which reports it.
        Effect.catchCause((cause) => Effect.sync(() => failSession(cause))),
        Effect.andThen(
          Effect.sync(() => {
            // Its commit's task changes were settled at their publication;
            // a waiter that asked for this pass checks again, and what its chain loads resolved can make any
            // scope idle. A pass checks expiry too, for hosts that decline the expiry timer.
            if (unknownsChanged) settleIdle(undefined);
            else {
              resolveIdleWaiters(undefined);
              expireContexts(now(), contextRetentionMs());
              scheduleExpiry();
            }
            noteWake();
          }),
        ),
      );
    });

    /** Whether any of `ids` holds or ended with an outcome other than `completed`. */
    const anyFailed = (ids: ReadonlyArray<TaskId>) =>
      Effect.gen(function* () {
        for (const id of ids) {
          const known = yield* region(() => index.live.get(id));
          const record = known !== undefined ? Option.some(known) : yield* storage.task(id);
          if (Option.isSome(record) && failedOutcome(record.value)) return true;
        }
        return false;
      });

    /** Writes the terminal record of every `completing` candidate whose ordinary owned work is gone. */
    const finalize = (tx: Transaction) =>
      Effect.suspend(() => {
        const candidates = Array.from(index.finalizeChecks);
        index.finalizeChecks.clear();
        const overlay = overlayOf(tx);
        return region(() => Decisions.planFinalize(index, candidates, overlay), reloadScopes).pipe(
          Effect.flatMap((actions) => apply(tx, actions)),
        );
      });

    /** Applies a plan's writes in order. */
    const apply = (
      tx: Transaction,
      actions: ReadonlyArray<Decisions.Action>,
    ): Effect.Effect<void, CommitRejected | StorageError, Tx> =>
      Effect.forEach(
        actions,
        (action): Effect.Effect<void, CommitRejected | StorageError, Tx> =>
          action._tag === "Write"
            ? tx.setTask(action.record)
            : options.settleOutcome(tx, action.record, action.outcome),
        { discard: true },
      );

    /**
     * Loads the owner chains of the tasks in `unloaded` and, with `queued`, of every conversation with
     * queued inputs; returns the latter. Reads committed storage directly, so it may run inside a commit.
     */
    const loadScopes = (queued: boolean) =>
      Effect.gen(function* () {
        for (const id of Array.from(index.unloaded)) {
          // An earlier load in this loop may have passed it.
          if (index.unloaded.has(id)) yield* loadChain({ task: id });
        }
        if (!queued) return [];
        const submissions = yield* scanAll((cursor) =>
          storage.scanSubmissions({ status: "queued" }, SCAN_PAGE_SIZE, cursor),
        );
        const conversations = Array.from(
          new Set(
            submissions
              .filter((submission) => submission.type === "input")
              .map((submission) => submission.conversationId),
          ),
        );
        for (const id of conversations) yield* loadChain({ conversation: id });
        return conversations;
      });

    /**
     * Loads the owner edges and task nodes from `start` up to its ownerless root. A known task outside
     * `unloaded` ends the walk, as its chain is loaded. Loading a node can reveal a cancelled owner, so it
     * makes the next reconcile derive cascade marks.
     */
    const loadChain = (start: Up, overlay?: Overlay) =>
      Effect.gen(function* () {
        const passed: Array<TaskId> = [];
        let installed = false;
        // `index.idle` counts a live task whose chain is not loaded as inside
        // every scope it may be in: what this load resolved, also when it stopped early, can make such a scope
        // idle without a task change below it. A waiter such a task held is checked at the pass's end.
        const resolved = () => {
          if (passed.some((id) => index.live.has(id)) || (installed && liveUnloaded())) {
            unknownsChanged = true;
            scheduleReconcile();
          }
        };
        let at: Up | undefined = start;
        while (at !== undefined) {
          if ("task" in at) {
            const id: TaskId = at.task;
            let node: TaskNode | undefined = index.knownNode(id);
            // Created in this commit: the load from its own record covers what is above it.
            if (node === undefined && overlay?.tasks.has(id) === true) break;
            if (node !== undefined && !index.unloaded.has(id)) break;
            if (node === undefined) {
              const record: Option.Option<TaskRecord> = yield* storage.task(id);
              if (Option.isNone(record)) {
                resolved();
                return;
              }
              node = taskNodeOf(record.value);
              if (record.value.state.status === "terminal") {
                index.settle(id, node);
                pin(id);
                installed = true;
              }
              scheduleReconcile();
            }
            passed.push(id);
            at = parentOf(node);
          } else {
            let edge = index.edgeOf(at.conversation, overlay);
            if (edge === undefined) {
              const conversation: Option.Option<ConversationRecord> = yield* storage.conversation(
                at.conversation,
              );
              const owner: TaskId | null = Option.isSome(conversation)
                ? (conversation.value.owner?.taskId ?? null)
                : null;
              edge = owner;
              index.setEdge(at.conversation, owner);
              pin(Ownership.conversationNode(at.conversation));
              installed = true;
              scheduleReconcile();
            }
            at = edge === null ? undefined : { task: edge };
          }
        }
        resolved();
        index.markLoaded(passed);
      });

    /**
     * Adds the memberships from `start` up to and including the nearest
     * live task, or the root conversation, which it returns. `reached` keeps, per node walked, where its walk
     * ended, so a chain several tasks of a commit share is walked once. Whether an ancestor is live can change
     * under a pending commit, so it is read after a touch of the ancestor's record: a pending end must not stop
     * the chain early. Ownership fields are immutable, so not keyed.
     */
    const chainKeys = (
      start: Ownership.Node,
      overlay: Overlay,
      keys: Set<string>,
      reached: Map<string, ConversationId | undefined>,
    ) =>
      Effect.gen(function* () {
        const walked: Array<string> = [];
        let root: ConversationId | undefined;
        let node: Ownership.Node | null | undefined = start;
        while (node !== undefined && node !== null) {
          const key = Keys.ownedKey(node);
          // Walked up from here already, for another task of the commit.
          if (reached.has(key)) {
            root = reached.get(key);
            break;
          }
          keys.add(key);
          walked.push(key);
          if (Ownership.isConversationNode(node)) {
            const conversation = Ownership.conversationOf(node);
            const edge: TaskId | null | undefined = index.edgeOf(conversation, overlay);
            if (edge === null) root = conversation;
            node = edge;
            continue;
          }
          const id: TaskId = node;
          let record: TaskRecord | undefined = overlay.tasks.get(id);
          if (record === undefined) {
            const gate = kernel.touch(Keys.taskKey(id));
            if (gate !== undefined) {
              yield* gate;
              // It may have ended meanwhile, and its node been swept: load the chain above it again.
              yield* loadChain({ task: id }, overlay).pipe(internalReads);
            }
            record = index.live.get(id);
          }
          if (record !== undefined && record.state.status !== "terminal") break;
          const above: TaskNode | undefined = record ?? index.knownNode(id);
          node = above === undefined ? undefined : Ownership.parentNode(above);
        }
        for (const key of walked) reached.set(key, root);
        return root;
      });

    /**
     * Keeps a node a chain load installed until the line job underway ends, its
     * commit published or never to be, so a sweep at an earlier commit's publication does not drop it first.
     */
    const pin = (node: Ownership.Node) => {
      index.pin(node);
      kernel.onJobEnd(() => index.unpin(node));
    };

    /** Whether a live task's chain is not loaded yet: one `index.idle` counts as inside every scope it may be in. */
    const liveUnloaded = () => {
      for (const id of index.unloaded) if (index.live.has(id)) return true;
      return false;
    };

    /**
     * Sweeps in a job of its own on the Session line, after the operation underway: a rejected commit, one
     * with nothing to write, and a read publish nothing, so the sweep at the end of `observe` would not run
     * for what they loaded.
     */
    const scheduleSweep = () => {
      if (sweepScheduled || closing) return;
      sweepScheduled = true;
      sweepSignal.fire();
    };

    /** Fails the Session, which reports it, for a failure of one of the scheduler's own commits. */
    const failSession = (cause: Cause.Cause<unknown>) => {
      if (closing) return;
      const error = Cause.squash(cause);
      if (isStorageFailure(error)) kernel.fail(error);
      else kernel.fail(error instanceof Error ? error : new Error(causeMessage(cause)));
    };

    /** Close listener: runs synchronously once admission is sealed, or when the Session fails. */
    const seal = () => {
      // The tasks this Harness held are counted out.
      if (!closing) for (const record of index.live.values()) countActive(-1, record.kind);
      resumed.clear();
      closing = true;
      const error = closedError();
      taskWaiters.rejectAll(error);
      idleWaiters.rejectAll(error);
      // Close drops the kept contexts and their expiry.
      contexts.clear();
      expiryAt = undefined;
      expirySignal.fire();
      // Close disarms the sleep timer.
      sleepAt = undefined;
      sleepSignal.fire();
    };

    const kick = () => {
      dirty = true;
      if (draining || !enabled || closing) return;
      draining = true;
      drainSignal.fire();
    };

    const shouldDrain = () => dirty && enabled && !closing;

    const drain = Effect.gen(function* () {
      while (shouldDrain()) {
        dirty = false;
        const reservations = yield* reserve;
        for (const reservation of reservations) yield* start(reservation);
      }
    }).pipe(
      // As in `reconcile`: the reservation commit runs no extension code.
      Effect.catchCause((cause) => Effect.sync(() => failSession(cause))),
      Effect.ensuring(
        Effect.sync(() => {
          draining = false;
          // A wakeup that arrived after the loop's last check still needs its pass.
          if (dirty) kick();
          noteWake();
        }),
      ),
    );

    /**
     * Reserves every eligible task in one commit; orphans abort-marked tasks no definition can take, unless
     * abandoned after a restart. The first pass after open only abort-marks the abandoned tasks.
     */
    const reserve = Effect.suspend(() => {
      const reservations: Array<Reservation> = [];
      let marked: Array<TaskId> = [];
      let cutOff: ReadonlyArray<{ readonly record: TaskRecord; readonly message: string }> = [];
      return commitWith((tx) =>
        Effect.gen(function* () {
          if (!enabled || closing) return;
          // Decided in one step, against the owners as open left them.
          const unmarked = yield* region(() =>
            Array.from(abandoned).flatMap((id) => {
              const record = index.live.get(id);
              if (record === undefined || record.abortRequested) return [];
              const owner = record.owner === undefined ? undefined : index.live.get(record.owner);
              return owner !== undefined && options.keepsAbandoned(owner) ? [] : [record];
            }),
          );
          for (const record of unmarked) yield* tx.setTask(withAbortMark(record, "restart"));
          if (abandoned.size > 0) {
            marked = Array.from(abandoned);
            return;
          }
          yield* loadScopes(false);
          // Every pass picks up sleeps whose time has come: a pass started by
          // any change may be the first to run after a deadline.
          wakeDue();
          const overlay = overlayOf(tx);
          const planWith = (planned: Decisions.Definitions) =>
            Decisions.planReserve(
              index,
              planned,
              {
                invoked: (id) => invocations.has(id),
                candidates: Array.from(index.runnable),
                now: now(),
                restartLimit,
                limitsRestarts: options.limitsRestarts,
              },
              overlay,
            );
          // A decision region: the pass reads records and walks the tree. It probes
          // without migrations, which may run again; the plan made in the step of its clean check reads the
          // same keys and migrates once.
          const plan = yield* region(
            () => planWith({ ...definitions, migrations: false }),
            reloadScopes,
            () => planWith(definitions),
          );
          cutOff = plan.cutOff;
          // Registered in the step that decided, on the line, so marks and later reservations see the
          // invocations and close joins them; `end` makes each a candidate again.
          for (const id of plan.dropped) index.runnable.delete(id);
          for (const start of plan.start) {
            reservations.push({
              invocation: createInvocation(start.record, start.mode),
              task: start.task,
            });
          }
          recordFailures(plan.failures);
          if (plan.cascade) {
            cascadePending = true;
            scheduleReconcile();
          }
          yield* apply(tx, plan.actions);
        }),
      ).pipe(
        Effect.onError(() =>
          Effect.sync(() => {
            for (const { invocation } of reservations) {
              invocations.delete(invocation.taskId);
              index.runnable.add(invocation.taskId);
              // Its scope holds nothing yet; closed so it does not stay a child of the Harness's scope.
              closeQueue.push(invocation.scope);
              closeSignal.fire();
              // oxlint-disable-next-line durable/no-inline-wakeup -- the end of an invocation that never started.
              Deferred.doneUnsafe(invocation.done, Effect.void);
            }
          }),
        ),
        Effect.map(() => {
          // Reported once the faults are stored.
          for (const { record, message } of cutOff) {
            report(
              Cause.fail(
                new TaskFaulted({
                  taskId: record.id,
                  kind: record.kind,
                  conversationId: record.conversationId,
                  reason: "restart_limit",
                  message,
                }),
              ),
              {
                kind: "task_faulted",
                ...aboutTask({
                  taskId: record.id,
                  kind: record.kind,
                  conversationId: record.conversationId,
                }),
                reason: "restart_limit",
              },
            );
          }
          // Marked, or already marked or gone: done with them. Reserve again, now seeing the marks.
          for (const id of marked) abandoned.delete(id);
          if (marked.length > 0) dirty = true;
          return reservations;
        }),
      );
    });

    const definitions: Decisions.Definitions = { tasks, failedMigrations };

    /** `settings.restartLimit`, or the default when they throw. */
    const restartLimit = (): number => {
      try {
        return options.settings().restartLimit;
      } catch (error) {
        report(Cause.die(error), SETTINGS);
        return DEFAULT_RESTART_LIMIT;
      }
    };

    /** Makes every sleeping task whose time has come a candidate. */
    const wakeDue = () => {
      const at = now();
      for (const id of index.sleeping) {
        const record = index.live.get(id);
        if (record !== undefined && !Decisions.asleep(index, record, at)) index.runnable.add(id);
      }
    };

    /** The task graph's, told when a reason changes without a commit. */
    const blockedListeners = new Set<() => void>();
    /** A throwing listener is reported, the others still run. */
    const notifyBlocked = () => {
      for (const listener of Array.from(blockedListeners)) {
        const result = Result.try({
          try: listener,
          catch: (error) => (error instanceof Error ? error : new Error(String(error))),
        });
        if (Result.isFailure(result)) report(Cause.die(result.failure), LISTENER);
      }
    };

    /** Records and reports the migrations a decision found failing; a later fit of the task is blocked. */
    const recordFailures = (failures: ReadonlyArray<Decisions.MigrationFailure>) => {
      for (const failure of failures) {
        failedMigrations.set(failure.id, {
          task: failure.task,
          message: Decisions.causeMessage(failure.cause),
        });
        report(failure.cause, {
          kind: "migration_failed",
          ...aboutTask({
            taskId: failure.id,
            kind: failure.task.definition.name,
            conversationId: failure.conversationId,
          }),
        });
      }
      // Blocked now without a commit: the graph must hear it. The graph's listener only fires its refresh loop,
      // which runs after a yield.
      if (failures.length > 0) notifyBlocked();
    };

    /**
     * What a report about a task's own code says of it: the
     * task, and the extension that installed its definition. With a static registry the definition a
     * task ran is the one installed now.
     */
    const aboutTask = (task: Parameters<typeof reportTask>[0]) => {
      const extension = options.extensionOf(task.kind);
      return extension === undefined
        ? { task: reportTask(task) }
        : { task: reportTask(task), extension };
    };

    // ─── Invocations ────────────────────────────────────────────────────────

    const createInvocation = (record: TaskRecord, mode: "run" | "abort"): Invocation => {
      const invocation: Invocation = {
        taskId: record.id,
        conversationId: record.conversationId,
        mode,
        ended: false,
        aborted: false,
        stopSignal: Deferred.makeUnsafe<void>(),
        done: Deferred.makeUnsafe<void>(),
        fiber: undefined,
        signalled: false,
        scope: Scope.forkUnsafe(options.scope),
        phaseAgent: undefined,
        kind: record.kind,
        links: record.startedAt === undefined ? record.links : undefined,
        span: undefined,
        record: undefined,
      };
      invocations.set(record.id, invocation);
      return invocation;
    };

    /** Interrupts the invocation's handler, from the signal loop, never on the stack of the caller. */
    const signalInvocation = (invocation: Invocation) => {
      signalQueue.add(invocation);
      interruptSignal.fire();
    };

    const start = (reservation: Reservation) =>
      Effect.gen(function* () {
        const invocation = reservation.invocation;
        const body = (invocation.mode === "run" ? run(reservation) : runAbort(reservation)).pipe(
          // A failure in running the task.
          Effect.catchCause((cause) =>
            Effect.sync(() => report(cause, { kind: "task_error", ...aboutTask(invocation) })),
          ),
          Effect.ensuring(
            Effect.sync(() => {
              end(invocation);
              // oxlint-disable-next-line durable/no-inline-wakeup -- the end of the invocation's fiber, for its joiners.
              Deferred.doneUnsafe(invocation.done, Effect.void);
              kick();
            }),
          ),
        );
        // Uninterruptible from its first step: only the handler is interruptible, so the steps around it,
        // and the end of the invocation, always run.
        const fiber = yield* Effect.forkIn(body, options.scope, { uninterruptible: true });
        invocation.fiber = fiber;
        if (invocation.signalled) fiber.interruptUnsafe();
        noteWake();
      });

    /** Runs a handler interruptibly, unless the Harness is closing or the invocation was signalled. */
    const runHandler = (invocation: Invocation, handler: Effect.Effect<void, Failure>) =>
      Effect.exit(
        Effect.interruptible(
          // Close may seal between the step and dispatch; checked in the same step that
          // enters the handler, so no handler code starts after the seal.
          Effect.suspend(() =>
            closing || invocation.signalled
              ? Effect.interrupt
              : // Each phase gets a scope of its own, closed when it ends, so
                // what a phase acquires is released per phase, never kept by the Harness.
                // The Harness's services, under the phase's span and the tracer that opened it: a span the
                // build left in the services is not the handler's parent.
                Effect.flatMap(Effect.context<never>(), (current) =>
                  Effect.scoped(handler).pipe(
                    Effect.provideContext(
                      Context.merge(
                        Context.omit(Tracer.ParentSpan)(options.services),
                        Context.pick(Tracer.ParentSpan, Tracer.Tracer)(current),
                      ),
                    ),
                  ),
                ),
          ),
        ),
      );

    /**
     * A task's handlers: its definition's, or for a task declared without them, those its handler Layer put in
     * the Harness's services. The Harness requires that Layer, so it is there unless the types were
     * bypassed.
     */
    type ErasedHandlers = {
      readonly phases: { readonly [phase: string]: ErasedHandler | undefined };
      readonly abort: ErasedHandler;
    };
    const handlersOf = (task: Task.Any): ErasedHandlers | undefined => {
      const definition = erased(task);
      if (definition.phases !== undefined && definition.abort !== undefined) {
        return { phases: definition.phases, abort: definition.abort };
      }
      const found = Context.getOption(options.services, Task.handlerKey(definition.name));
      // SAFETY: a handler Layer is built by `toLayer` from handlers typed by this very definition.
      return Option.isSome(found) ? (found.value as ErasedHandlers) : undefined;
    };

    /** Runs phase handlers, each preceded by a step that decides on the line whether the invocation continues. */
    const run = (reservation: Reservation) =>
      Effect.gen(function* () {
        const invocation = reservation.invocation;
        const runtime = makeRuntime(invocation);
        let current = yield* step(invocation, (current) =>
          Decisions.decideStep(index, current, undefined),
        );
        for (;;) {
          // Close may seal between the decision and dispatch.
          if (current === undefined || closing) return;
          const task = current;
          current = yield* tracePhase(
            invocation,
            task,
            Effect.gen(function* () {
              const checkpoint = task.state.checkpoint;
              const phase = phaseOf(checkpoint);
              const handler =
                phase === undefined ? undefined : handlersOf(reservation.task)?.phases?.[phase];
              // Every phase resolves its agent anew.
              invocation.phaseAgent = undefined;
              const exit = yield* runHandler(
                invocation,
                handler === undefined
                  ? Effect.die(new Error(`Task ${task.kind} has no phase ${String(phase)}`))
                  : Effect.suspend(() => handler(task, runtime)),
              );
              const previous: PhaseResult = Exit.isSuccess(exit)
                ? { checkpoint }
                : { checkpoint, failure: exit.cause };
              return yield* step(invocation, (next) => Decisions.decideStep(index, next, previous));
            }),
          );
        }
      });

    /**
     * One phase invocation and its decision commit in a `durable.phase`
     * span, never a wait or a restart. Its parent is the task's stored carrier; the first invocation links
     * the task's links. The outcome is read from the task's latest record; a task that ended `faulted`, `failed`
     * or `orphaned` ends the span with a failure, an aborted one with success.
     */
    const tracePhase = (
      invocation: Invocation,
      task: RunningRecord,
      body: Effect.Effect<RunningRecord | undefined>,
    ): Effect.Effect<RunningRecord | undefined> =>
      Effect.suspend(() => {
        const checkpoint = task.state.checkpoint;
        const phase =
          invocation.mode === "abort"
            ? (phaseOf(checkpoint) ?? "abort")
            : (phaseOf(checkpoint) ?? "");
        const attempt = attemptOf(checkpoint);
        const links = (invocation.links ?? []).map(
          (carrier): Tracer.SpanLink => ({ span: Telemetry.spanOf(carrier), attributes: {} }),
        );
        const started = now();
        const attributes: telemetry.Attributes = {
          "durable.task.id": task.id,
          "durable.task.name": task.kind,
          "durable.conversation.id": task.conversationId,
          "durable.phase.name": phase,
          "durable.phase.kind": invocation.mode,
          "durable.phase.resumed": resumed.delete(task.id),
        };
        if (attempt !== undefined) attributes["durable.phase.attempt"] = attempt;
        invocation.record = task;
        phasing.set(task.id, invocation);
        return Effect.useSpan(
          Telemetry.Spans.phase,
          task.trace === undefined
            ? { root: true, links }
            : { parent: Telemetry.spanOf(task.trace), links },
          (span) => {
            invocation.span = span;
            telemetry.annotate(span, attributes, reportTelemetry);
            telemetry.traceIds(span, reportTelemetry);
            return body.pipe(
              Effect.withParentSpan(span),
              Effect.flatMap((next) => {
                const outcome = phaseOutcome(invocation);
                telemetry.annotate(span, { "durable.phase.outcome": outcome }, reportTelemetry);
                telemetry.update(
                  `durable.phase.duration\u0000${task.kind}\u0000${phase}\u0000${outcome}`,
                  () => Telemetry.phaseDuration({ task: task.kind, phase, outcome }),
                  now() - started,
                  reportTelemetry,
                );
                const failed = failedPhase(invocation.record);
                if (failed === undefined) return Effect.succeed(next);
                telemetry.annotate(
                  span,
                  { "durable.phase.error": failed.category },
                  reportTelemetry,
                );
                return Effect.fail(failed);
              }),
            );
          },
        ).pipe(
          Effect.catchTag("PhaseFailed", () => Effect.succeed(undefined)),
          Effect.ensuring(
            Effect.sync(() => {
              if (phasing.get(task.id) === invocation) phasing.delete(task.id);
              invocation.span = undefined;
              invocation.record = undefined;
            }),
          ),
        );
      });

    /** `done`, `aborted` or `faulted` once ended; else a wait, or the next phase. */
    const phaseOutcome = (invocation: Invocation): string => {
      const state = invocation.record?.state;
      if (state === undefined) return "aborted";
      if (state.status === "terminal" || state.status === "completing") {
        const status = state.outcome.status;
        return status === "completed" ? "done" : status === "aborted" ? "aborted" : "faulted";
      }
      if (state.status === "waiting" || state.status === "sleeping") return state.status;
      if (invocation.ended || closing) return "aborted";
      return `next=${phaseOf(state.checkpoint) ?? ""}`;
    };

    /** Runs the abort handler once; rules 1, 2 and 4 apply, and returning without an outcome faults. */
    const runAbort = (reservation: Reservation) =>
      Effect.gen(function* () {
        const invocation = reservation.invocation;
        const found = index.live.get(invocation.taskId);
        if (found?.state.status !== "running" || closing) return;
        // SAFETY: the record is running, as checked above.
        const current = found as RunningRecord;
        const runtime = makeRuntime(invocation);
        const abort = handlersOf(reservation.task)?.abort;
        const handler: ErasedHandler =
          abort ??
          (() =>
            Effect.die(
              new Error(`Task ${reservation.task.definition.name} has no implementation`),
            ));
        yield* tracePhase(
          invocation,
          current,
          Effect.gen(function* () {
            const exit = yield* runHandler(
              invocation,
              Effect.suspend(() => handler(current, runtime)),
            );
            const message = Exit.isFailure(exit)
              ? causeMessage(exit.cause)
              : `Abort handler of task ${invocation.taskId} returned without a terminal outcome`;
            const cause = Exit.isFailure(exit) ? exit.cause : undefined;
            return yield* step(invocation, () => ({ _tag: "Fault", message, cause }));
          }),
        );
      });

    /**
     * One synchronous decision on the Session line. A task that is no longer running (rule 1) or a closing
     * Harness (rule 2) ends the invocation without a write; otherwise `decide` decides whether the
     * invocation continues. Ending happens inside the callback, before a fault's Harness cleanup. A
     * rejected step, such as admission after close, also ends the invocation.
     */
    const step = (
      invocation: Invocation,
      decideStep: (current: RunningRecord) => Decisions.Decision,
    ) => {
      let fault: Extract<Decisions.Decision, { readonly _tag: "Fault" }> | undefined;
      return commitWith((tx) =>
        Effect.gen(function* () {
          // One decision region that reads the task's record.
          const { current, decision } = yield* region(() => {
            const found = index.live.get(invocation.taskId);
            // SAFETY: narrowed to a running record by its status.
            const running =
              found?.state.status === "running" ? (found as RunningRecord) : undefined;
            return {
              current: running,
              decision: running !== undefined && !closing ? decideStep(running) : Decisions.End,
            };
          });
          if (decision._tag === "Continue") return current;
          if (decision._tag === "End" && decision.cascade) {
            cascadePending = true;
            scheduleReconcile();
          }
          end(invocation);
          if (decision._tag === "Fault" && current !== undefined) {
            fault = decision;
            yield* terminate(tx, current, {
              status: "faulted",
              error: { message: decision.message },
            });
          }
          return undefined;
        }),
      ).pipe(
        // Reported once the fault is durable: a host may count it.
        Effect.tap(() =>
          Effect.sync(() => {
            if (fault === undefined) return;
            report(fault.cause ?? Cause.die(new Error(fault.message)), {
              kind: "task_faulted",
              ...aboutTask(invocation),
              reason: "threw",
            });
          }),
        ),
        // The step writes the scheduler's own decision, a fault or a terminal record; a failure there would
        // otherwise leave the task running, to be reserved and run again.
        Effect.catchCause((cause) =>
          Effect.sync(() => {
            end(invocation);
            failSession(cause);
            return undefined;
          }),
        ),
      );
    };

    /**
     * Writes an outcome the scheduler decided. While the task's ordinary owned work is live it holds as
     * `completing` and its Harness cleanup waits for the final commit.
     */
    const terminate = (tx: Transaction, record: TaskRecord, outcome: Decisions.SchedulerOutcome) =>
      loadScopes(false).pipe(
        Effect.andThen(
          Effect.suspend(() => {
            const overlay = overlayOf(tx);
            return region(
              () => Decisions.outcomeActions(index, record, outcome, overlay),
              reloadScopes,
            ).pipe(Effect.flatMap((actions) => apply(tx, actions)));
          }),
        ),
      );

    /**
     * Replaces a running task's state with what it committed. A terminal state holds as `completing` while
     * ordinary owned work is live, judged on the commit's candidates. A wait is validated first.
     */
    const commitState = (
      tx: Transaction,
      invocation: Invocation,
      current: RunningRecord,
      next: Task.NextTaskState<Json, Json>,
    ) =>
      Effect.gen(function* () {
        if (next.status === "waiting") {
          yield* validateWait(tx, invocation, current, next.on, next.policy).pipe(internalReads);
        }
        if (next.status === "sleeping") {
          const invalid = Decisions.invalidSleep(invocation.mode, current, next);
          if (invalid !== undefined) {
            return yield* new TaskError({ reason: new InvalidSleep({ message: invalid }) });
          }
        }
        if (next.status === "terminal") {
          const overlay = overlayOf(tx);
          yield* loadScopes(false).pipe(internalReads);
          for (const record of overlay.tasks.values()) {
            yield* loadChain(parentOf(record), overlay).pipe(internalReads);
          }
          // A decision region with the commit's candidates.
          if (yield* region(() => index.hasOwnedLive(current.id, overlay), reloadScopes)) {
            yield* tx.setTask(
              withState(Decisions.progressed(current), {
                status: "completing",
                outcome: next.outcome,
              }),
            );
            return;
          }
        }
        // SAFETY: a task's next state is one of the record's states with JSON checkpoint and result.
        yield* tx.setTask(Decisions.committedState(current, next as TaskState));
      });

    /**
     * A wait names existing tasks, and `failFast` only tasks the waiter owns. It may not close a cycle in
     * the wait-for graph; an abort handler cannot wait. Only a wait adds an edge between
     * existing tasks, as a new task has only incoming edges, so checking each wait keeps the graph acyclic.
     */
    const validateWait = (
      tx: Transaction,
      invocation: Invocation,
      current: RunningRecord,
      on: ReadonlyArray<TaskId>,
      policy: string,
    ) =>
      Effect.gen(function* () {
        if (invocation.mode === "abort") {
          return yield* invalidWait(`Abort handler of task ${current.id} cannot wait`);
        }
        const overlay = overlayOf(tx);
        const targets: Array<TaskId> = [];
        for (const id of on) {
          const staged = overlay.tasks.get(id) ?? (yield* region(() => index.live.get(id)));
          const member =
            staged !== undefined ? staged : Option.getOrUndefined(yield* storage.task(id));
          if (member === undefined) return yield* invalidWait(`Task ${id} does not exist`);
          if (policy === "failFast" && member.owner !== current.id) {
            return yield* invalidWait(
              `Task ${current.id} can wait failFast only on tasks it owns; ${id} is not one`,
            );
          }
          // A live child already holds the waiter: waiting on it adds no edge. A terminal task has none.
          if (member.state.status !== "terminal" && member.owner !== current.id) targets.push(id);
        }
        if (targets.length === 0) return;
        // Walks down need the chains of every live task loaded.
        yield* loadScopes(false);
        const cycle = yield* region(
          () => Decisions.waitCycle(index, current.id, targets, overlay),
          reloadScopes,
        );
        if (cycle !== undefined) {
          return yield* invalidWait(
            `Task ${current.id} cannot wait on ${cycle[0]}: ${[current.id, ...cycle].join(" -> ")} is a wait cycle`,
          );
        }
      });

    /** Ends an invocation: its runtime operations reject from now on, its watches stop, and its task is free. */
    const end = (invocation: Invocation) => {
      if (invocation.ended) return;
      invocation.ended = true;
      if (closing) stoppedAtClose.add(invocation.taskId);
      if (invocations.get(invocation.taskId) === invocation) {
        invocations.delete(invocation.taskId);
        if (index.live.has(invocation.taskId)) index.runnable.add(invocation.taskId);
      }
      // Handed to the closer in this step: ending happens inside a commit callback, on the line.
      closeQueue.push(invocation.scope);
      closeSignal.fire();
      // Pending waits bound to the invocation, such as a detached `awaitTask`, fail with it.
      Signal.doneAfterYield(invocation.stopSignal, Effect.void);
      noteWake();
    };

    // ─── Wakeups ───────────────────────────────────────────────────────────

    let reported:
      | { readonly due: boolean; readonly deadline: number | undefined; readonly active: boolean }
      | undefined;

    /** Whether the next reservation pass would reserve, orphan, fault or hold back this task. */
    const due = (id: TaskId, at: number): boolean => {
      const record = index.live.get(id);
      if (record === undefined || record.state.status === "completing" || invocations.has(id)) {
        return false;
      }
      if (Decisions.asleep(index, record, at)) return false;
      if (Decisions.waitingOn(index, record, true).length > 0) return false;
      const abort = record.abortRequested;
      if (!abort && !record.background && index.belowCancelled(parentOf(record))) return true;
      const fit = Decisions.fitOf(definitions, record);
      if ("reason" in fit) return abort && record.abortReason === undefined;
      return true;
    };

    const computeWake = () => {
      const at = now();
      let isDue = reconcileScheduled;
      // A due sleep leaves the candidates when a pass drops it early; `wakeDue` puts it back.
      for (const candidates of [index.runnable, index.sleeping]) {
        if (isDue) break;
        for (const id of candidates) {
          if (due(id, at)) {
            isDue = true;
            break;
          }
        }
      }
      // The earliest sleep not yet due; only `index.sleeping` holds sleeping tasks. A sleep without `until`
      // waits only for signals, which a delivery's commit wakes.
      let deadline: number | undefined;
      for (const id of index.sleeping) {
        const record = index.live.get(id);
        if (record === undefined || !Decisions.asleep(index, record, at)) continue;
        const until = record.state.until;
        if (until !== undefined && (deadline === undefined || until < deadline)) deadline = until;
      }
      // Every invocation is active work, also one that waits in memory for a task: losing it is a crash.
      return { due: isDue, deadline, active: invocations.size > 0 };
    };

    const toWake = (computed: ReturnType<typeof computeWake>): Wake => ({
      nextWakeAt: computed.due ? Option.some(now()) : Option.fromUndefinedOr(computed.deadline),
      activeWork: computed.active,
    });

    /**
     * Notes that the wake may have changed. The wake loop recomputes it after a yield, so a burst of changes
     * costs one computation, and reports it to the host when it changed, off the line and in order.
     */
    const noteWake = () => {
      // A read-only Harness never reports a wake.
      if (!closing && !options.readOnly) wakeSignal.fire();
    };

    const reportWake = wakeLock.withPermits(1)(
      Effect.suspend(() => {
        if (closing) return Effect.void;
        const computed = computeWake();
        const host = options.wakeups;
        // Without a host's wakeups, the scheduler keeps its own timer for the
        // earliest sleep while scheduling runs; with one, it arms none and reports the wake.
        if (host === undefined) {
          armSleep(enabled ? computed.deadline : undefined);
          return Effect.void;
        }
        if (
          reported !== undefined &&
          reported.due === computed.due &&
          reported.deadline === computed.deadline &&
          reported.active === computed.active
        ) {
          return Effect.void;
        }
        // Recorded once the host took it: a wake whose report failed is reported again at the next change.
        return host.schedule(toWake(computed)).pipe(
          Effect.tap(() =>
            Effect.sync(() => {
              reported = computed;
            }),
          ),
          Effect.catchCause((cause) => Effect.sync(() => report(cause, ON_WAKE))),
        );
      }),
    );

    /** Keeps the timer for the earliest sleep at `at`. */
    const armSleep = (at: number | undefined) => {
      if (sleepAt === at) return;
      sleepAt = at;
      sleepSignal.fire();
    };

    /**
     * The sleep loop's pass: sleeps until `sleepAt`, or until it changes, and then makes due sleeps runnable. It
     * rechecks the clock through `wakeDue`, so a wake before a sleep's `until` runs nothing; the next wake
     * arms it again.
     */
    // A `Clock` sleep on a fiber of the Harness's scope instead of a `setTimer` timer; it keeps the process alive,
    // and close ends it.
    const awaitSleep = Effect.suspend(() => {
      const due = sleepAt;
      if (due === undefined) return Effect.void;
      return Effect.raceFirst(
        Effect.sleep(Duration.millis(Math.max(0, due - now()))).pipe(Effect.as(true)),
        sleepSignal.wait.pipe(Effect.as(false)),
      ).pipe(
        Effect.flatMap((fired) =>
          Effect.sync(() => {
            if (fired && sleepAt === due) {
              sleepAt = undefined;
              wakeDue();
              kick();
              noteWake();
            }
            // The loop waits on the signal before each pass; this pass ended without consuming a fire.
            if (fired || sleepAt !== undefined) sleepSignal.fire();
          }),
        ),
      );
    });

    // ─── Runtime ────────────────────────────────────────────────────────────

    /** Runs a committed-state read unless the invocation has ended. */
    const read = <A, E, R>(invocation: Invocation, effect: Effect.Effect<A, E, R>) =>
      Effect.suspend(
        (): Effect.Effect<A, E | TaskError, R> =>
          invocation.ended ? Effect.fail(endedError(invocation)) : effect,
      );

    /**
     * A wait that also fails once its invocation ends or is abort-marked, for work the handler left running or
     * passed on. Refused at once when either already happened.
     */
    const bound = <A, E, R>(invocation: Invocation, effect: Effect.Effect<A, E, R>) =>
      Effect.suspend((): Effect.Effect<A, E | TaskError, R> => {
        const refused = refusal(invocation);
        if (refused !== undefined) return Effect.fail(refused);
        return Effect.raceFirst(
          effect,
          Deferred.await(invocation.stopSignal).pipe(
            Effect.andThen(
              Effect.suspend(() => Effect.fail(refusal(invocation) ?? endedError(invocation))),
            ),
          ),
        );
      });

    /** Commits after rereading the task on the line and gating the invocation. */
    const gated = <A, E>(
      invocation: Invocation,
      change: (tx: Transaction, current: RunningRecord) => Effect.Effect<A, E, Tx>,
      options?: CommitOptions,
    ) =>
      read(
        invocation,
        commitWith(
          (tx) =>
            // A decision region: the task's record may have an unpublished writer.
            Effect.flatMap(
              region(() => index.live.get(invocation.taskId)),
              (found): Effect.Effect<A, E | TaskError | ClosedError, Tx> => {
                if (invocation.ended) return Effect.fail(endedError(invocation));
                if (closing) return Effect.fail(closedError());
                if (found === undefined) {
                  return Effect.fail(
                    new TaskError({
                      reason: new NotRunning({ message: `Task ${invocation.taskId} is terminal` }),
                    }),
                  );
                }
                if (found.state.status !== "running") {
                  return Effect.fail(
                    new TaskError({
                      reason: new NotRunning({
                        message: `Task ${invocation.taskId} is ${found.state.status}`,
                      }),
                    }),
                  );
                }
                if (invocation.mode === "run" && found.abortRequested) {
                  return Effect.fail(
                    new TaskError({
                      reason: new AbortMarked({
                        message: `Task ${invocation.taskId} has a durable abort mark`,
                      }),
                    }),
                  );
                }
                // SAFETY: narrowed to a running record by its status.
                return change(tx, found as RunningRecord);
              },
            ),
          { conversationId: invocation.conversationId, taskId: invocation.taskId },
          options,
        ),
      );

    const makeRuntime = (invocation: Invocation): ErasedRuntime => {
      const memoImpl = (name: string, ...candidate: ReadonlyArray<Json>) => {
        if (candidate.length === 0) {
          return read(
            invocation,
            Effect.sync(() => memoOf(index.live.get(invocation.taskId), name)),
          );
        }
        const value = candidate[0] ?? null;
        return gated(invocation, (tx, current) =>
          Effect.gen(function* () {
            const winner = memoOf(current, name);
            if (Option.isSome(winner)) return winner.value;
            yield* tx.setTask({ ...current, memos: { ...current.memos, [name]: value } });
            return value;
          }),
        );
      };
      // SAFETY: the implementation serves both overloads by arity; a memo holds what its first writer
      // wrote, and the caller names its type.
      const memo = memoImpl as Task.TaskMemo;

      const entryImpl = (first: EntryId | { readonly kind: string }, second?: EntryId) => {
        const kind = Predicate.isNumber(first) ? undefined : first.kind;
        const id = Predicate.isNumber(first) ? first : second;
        if (id === undefined)
          return Effect.die(new Error("Task runtime entry() needs an entry ID"));
        return read(
          invocation,
          kernel.readOnLine(storage.visibleEntry(invocation.conversationId, id)).pipe(
            Effect.map((found) =>
              Option.filter(
                Option.map(found, (stored) => stored.entry),
                (record) => kind === undefined || record.kind === kind,
              ),
            ),
          ),
        );
      };
      // SAFETY: the implementation serves both overloads; a typed read returns entries of its kind only.
      const entry = entryImpl as Task.TaskEntryRead;

      // Only the bounds and the order come from the query, so a page
      // never reads another conversation.
      const entries: Task.TaskEntriesRead = (query, limit, cursor) =>
        read(
          invocation,
          kernel.readOnLine(
            storage.scanEntries(
              {
                conversationId: invocation.conversationId,
                minEntryId: query.minEntryId,
                maxEntryId: query.maxEntryId,
                order: query.order,
              },
              limit,
              cursor,
            ),
          ),
        );

      // SAFETY: the erased call forwards the overloaded Session method's arguments unchanged.
      const erasedSnapshot = kernel.snapshot as ErasedRead;
      // SAFETY: as above.
      const erasedSnapshotAsOf = kernel.snapshotAsOf as ErasedRead;
      const snapshotImpl = (...args: ErasedArguments) => read(invocation, erasedSnapshot(...args));
      const snapshotAsOfImpl = (...args: ErasedArguments) =>
        read(invocation, erasedSnapshotAsOf(...args));
      // SAFETY: each returns what the overload the caller chose returns.
      const snapshot = snapshotImpl as Task.TaskRuntime<Json, Json, Json>["snapshot"];
      // SAFETY: as above.
      const snapshotAsOf = snapshotAsOfImpl as Task.TaskRuntime<Json, Json, Json>["snapshotAsOf"];
      // SAFETY: the erased call forwards the overloaded Session method's arguments unchanged.
      const erasedWatch = kernel.watchDoc as ErasedWatch;

      const watchImpl = (...args: ErasedArguments) =>
        read(
          invocation,
          // Only the acquisition is interruptible: once it attached, the release is registered in the same
          // uninterruptible run, so an interruption cannot leave the watch pinned.
          Effect.uninterruptibleMask((restore) =>
            Effect.gen(function* () {
              // A child of the invocation's scope, which the closer closes when the invocation ends; forked
              // from a closed one it is born closed, so the watch stops as soon as it attaches.
              const watchScope = yield* Scope.fork(invocation.scope);
              const stop = Scope.close(watchScope, Exit.void);
              const watch = yield* restore(Scope.provide(erasedWatch(...args), watchScope)).pipe(
                Effect.onError(() => stop),
              );
              if (Option.isNone(watch)) {
                yield* stop;
                return watch;
              }
              // An acquisition that finishes after its invocation ended stops.
              if (invocation.ended) {
                yield* stop;
                return yield* endedError(invocation);
              }
              // The caller's scope may stop it earlier.
              yield* Effect.addFinalizer(() => stop);
              return watch;
            }),
          ),
        );
      // SAFETY: returns what the overload the caller chose returns.
      const watchDoc = watchImpl as Task.TaskRuntime<Json, Json, Json>["watchDoc"];

      const awaitTaskBound = (id: TaskId) => bound(invocation, awaitTask(id));

      /**
       * The phase's agent, resolved once at first use. The memo is registered in
       * the same step that checks for it, and resolved on a fiber of its own, so a caller interrupted while
       * waiting leaves the shared resolution running for the phase's other callers.
       */
      const resolveAgent = Effect.uninterruptibleMask((restore) =>
        Effect.suspend(() => {
          const existing = invocation.phaseAgent;
          if (existing !== undefined) return restore(Deferred.await(existing));
          const resolution = Deferred.makeUnsafe<Agent, AgentError>();
          invocation.phaseAgent = resolution;
          return Effect.forkIn(
            options.agentOf(invocation.conversationId).pipe(
              Effect.exit,
              Effect.flatMap((exit) =>
                Effect.sync(() => {
                  Signal.doneAfterYield(resolution, exit);
                }),
              ),
            ),
            options.scope,
            { startImmediately: true },
          ).pipe(Effect.andThen(restore(Deferred.await(resolution))));
        }),
      );
      const agent = bound(invocation, resolveAgent);

      const hooks: Task.HookRunner = {
        each: <F extends HookFunction, E, R>(
          name: string,
          invoke: (handler: F) => Effect.Effect<void, E, R>,
        ) =>
          Effect.gen(function* () {
            const resolved = yield* agent;
            for (const { extension, handlers } of agentHooks(resolved, invocation.kind)) {
              const handler = handlers[name];
              if (handler === undefined) continue;
              // SAFETY: the caller names the handler type of the hook it runs.
              const exit = yield* Effect.exit(invoke(handler as F));
              if (Exit.isSuccess(exit)) continue;
              // Once the invocation is signalled (an abort mark, its end, close), the
              // error propagates; any other is reported.
              if (Decisions.stoppedBy(exit.cause) || refusal(invocation) !== undefined || closing) {
                return yield* Effect.interrupt;
              }
              report(exit.cause, {
                kind: "extension_error",
                extension,
                task: reportTask(invocation),
              });
            }
          }).pipe(
            Effect.withSpan("durable.hook", {
              attributes: {
                "durable.task.id": invocation.taskId,
                "durable.task.kind": invocation.kind,
                "durable.hook.name": name,
              },
            }),
          ),
      };

      const boundSubmission = (submission: Submission): Submission => ({
        id: submission.id,
        status: bound(invocation, submission.status),
        await: bound(invocation, submission.await),
        withdraw: bound(invocation, submission.withdraw),
      });

      const commit: ErasedRuntime["commit"] = (change, commitOptions) => {
        let interrupted: TaskInterrupted | undefined;
        let checkpointed: string | undefined;
        return gated(
          invocation,
          (tx, current) =>
            Effect.gen(function* () {
              // An Effect change does not read the reread record.
              const next = yield* Effect.isEffect(change) ? change : change(current);
              if (next !== undefined && next !== null) {
                yield* commitState(tx, invocation, current, next);
                interrupted = interruptionReport(current, next);
                if (next.status === "running") {
                  const phase = phaseOf(next.checkpoint);
                  if (phase !== undefined && phase !== phaseOf(current.state.checkpoint))
                    checkpointed = phase;
                }
              }
            }),
          commitOptions,
        ).pipe(
          // The running checkpoint's phase changed while the phase continues.
          Effect.tap(() =>
            Effect.sync(() => {
              if (checkpointed === undefined) return;
              telemetry.event(
                invocation.span,
                Telemetry.Events.checkpoint,
                { "durable.phase.name": checkpointed },
                reportTelemetry,
              );
            }),
          ),
          // A task that ends for crashes says so once its outcome is stored,
          // so a host keeps a crash-loop signal.
          Effect.tap(() =>
            Effect.sync(() => {
              if (interrupted !== undefined) {
                report(Cause.fail(interrupted), {
                  kind: "task_interrupted",
                  ...aboutTask(invocation),
                  reason: interrupted.reason,
                  restarts: interrupted.restarts,
                });
              }
            }),
          ),
        );
      };

      const conversation: ErasedRuntime["conversation"] = (id) =>
        read(
          invocation,
          kernel.readOnLine(storage.conversation(id)).pipe(
            Effect.map(
              Option.map(
                (record): Task.ConversationHandle => ({
                  id: record.id,
                  // Every handle operation carries the invocation's signal, which its
                  // commit checks as its first step on the line. A submit does not race the
                  // signal after that: once admitted, its caller gets the submission.
                  submit: (draft) =>
                    Effect.suspend(() => {
                      const refused = refusal(invocation);
                      if (refused !== undefined) return Effect.fail(refused);
                      return options.submit(record.id, draft, () => refusal(invocation));
                    }).pipe(Effect.map(boundSubmission)),
                  abort: (abortOptions) =>
                    bound(
                      invocation,
                      abortConversation(record.id, abortOptions?.background === true, () =>
                        refusal(invocation),
                      ),
                    ),
                  awaitIdle: bound(invocation, awaitIdle(record.id)),
                  admit: (draft) =>
                    Effect.suspend(() => {
                      const refused = refusal(invocation);
                      if (refused !== undefined) return Effect.fail(refused);
                      return options.admit(record.id, draft, () => refusal(invocation));
                    }),
                }),
              ),
            ),
          ),
        );

      return {
        // SAFETY: the task ID of the invocation carries the task's result type.
        taskId: invocation.taskId as TaskId<Json>,
        conversationId: invocation.conversationId,
        commit,
        ...Calls.transitions(commit),
        key: (name) => Calls.keyOf(invocation.taskId, name),
        step: Calls.makeStep(memo),
        requireConversation: (id) => Calls.requireConversation(conversation, id),
        memo,
        readSignal: (signal, key) =>
          read(
            invocation,
            Effect.suspend(() => {
              const memos = SignalMemos.memosOf(index.live.get(invocation.taskId));
              return Effect.fromResult(SignalMemos.read(memos, signal, key));
            }),
          ),
        // An ended invocation's timers never fire, and arming one after its end
        // arms nothing. The timer is a fiber of the invocation's scope: forked into a
        // closed scope it is interrupted at once, and one forked before the closer closes it checks `ended`.
        setTimer: (callback, delay) =>
          Effect.forkIn(
            Effect.sleep(delay).pipe(
              Effect.andThen(Effect.suspend(() => (invocation.ended ? Effect.void : callback))),
              Effect.catchCause((cause) =>
                Effect.sync(() => {
                  if (!Cause.hasInterruptsOnly(cause)) {
                    report(cause, { kind: "task_error", ...aboutTask(invocation) });
                  }
                }),
              ),
            ),
            invocation.scope,
          ),
        getTask: (id) =>
          read(
            invocation,
            // SAFETY: a record of the task `id` names; its result type is the caller's.
            kernel.readOnLine(storage.task(id)) as Effect.Effect<
              Option.Option<Task.TypedTaskRecord<Json, Json, never>>,
              StorageError | ClosedError
            >,
          ),
        awaitTask: (id) =>
          // SAFETY: a terminal record of the task `id` names; its result type is the caller's.
          awaitTaskBound(id) as Effect.Effect<
            Task.SettledTask<never>,
            TaskError | StorageError | ClosedError
          >,
        abortOwned: (id) =>
          bound(
            invocation,
            Effect.gen(function* () {
              const record = yield* kernel.readOnLine(storage.task(id));
              if (Option.isNone(record) || record.value.owner !== invocation.taskId) {
                return yield* new TaskError({
                  reason: new NotOwned({
                    message: `Task ${id} is not owned by task ${invocation.taskId}`,
                  }),
                });
              }
              if (record.value.state.status === "terminal") return;
              yield* abort(id, true).pipe(Effect.andThen(awaitTask(id)));
            }),
          ),
        ownedTasks: read(
          invocation,
          kernel.readOnLine(
            Effect.gen(function* () {
              // The step that ends the invocation may have run on the line before this.
              if (invocation.ended) return yield* endedError(invocation);
              // A decision region over the owner's ownership key and its children.
              return yield* region(() => index.liveOwnedBy(invocation.taskId));
            }),
          ),
        ),
        outcomes: (ids) =>
          read(
            invocation,
            kernel.readOnLine(
              Effect.gen(function* () {
                const outcomes: Array<TaskOutcome> = [];
                for (const id of ids) {
                  const record = yield* storage.task(id);
                  const state = Option.getOrUndefined(record)?.state;
                  if (state?.status !== "terminal") {
                    return yield* new TaskError({
                      reason: new NotTerminal({ message: `Task ${id} is not terminal` }),
                    });
                  }
                  outcomes.push(state.outcome);
                }
                // SAFETY: outcomes of the tasks `ids` names; their result type is the caller's.
                return outcomes as ReadonlyArray<Task.TaskOutcome<never>>;
              }),
            ),
          ),
        entry,
        entries,
        snapshot,
        snapshotAsOf,
        watchDoc,
        conversation,
        agent,
        // The settings, read anew at every use; a reader that throws is a defect of the handler.
        settings: read(invocation, Effect.sync(options.settings)),
        env: read(
          invocation,
          options.envOf({ conversationId: invocation.conversationId, snapshot, snapshotAsOf }),
        ),
        hooks,
        context: (conversationId, contextOptions) =>
          read(
            invocation,
            Effect.gen(function* () {
              const result = yield* readContextFrom(
                kernel,
                conversationId,
                contextOptions?.at,
                contexts.get(conversationId)?.range,
              );
              // Checked again here, synchronously up to the install below: a
              // commit published since the read returned was not seen by `advanceContexts`, so a range that
              // missed it must not be advanced.
              const range =
                result.range?.current === true &&
                kernel.knownLatestEntryId(conversationId) !== result.range.bounds.tail
                  ? { ...result.range, current: false }
                  : result.range;
              // Kept unless the invocation ended or a concurrent read already kept
              // a newer range; checked and set in one step after the scan.
              const kept = contexts.get(conversationId);
              if (
                range !== undefined &&
                !closing &&
                !invocation.ended &&
                (kept === undefined || kept.range.bounds.tail <= range.bounds.tail)
              ) {
                // A read of another, inactive conversation starts or continues its retention period.
                if (!index.inactive(conversationId)) {
                  contexts.set(conversationId, {
                    range,
                    idleSince: undefined,
                    expiresAt: undefined,
                  });
                  idleOrder.delete(conversationId);
                } else if (contextRetentionMs() > 0) {
                  // An idle period that already started keeps its expiry and its place; a new one starts now.
                  const idleSince = kept?.idleSince ?? now();
                  const expiresAt =
                    kept?.idleSince !== undefined && kept.expiresAt !== undefined
                      ? kept.expiresAt
                      : idleSince + contextRetentionMs();
                  contexts.set(conversationId, { range, idleSince, expiresAt });
                  if (kept?.idleSince === undefined) markIdle(conversationId, idleSince);
                  scheduleExpiry();
                }
              }
              return result.view;
            }),
          ),
        now: read(invocation, Clock.currentTimeMillis),
        // The task's own failure names the task's extension; any other names
        // whose code failed, if anyone's.
        report: (cause, from) =>
          read(
            invocation,
            Effect.sync(() =>
              report(
                cause,
                from === undefined
                  ? { kind: "task_error", ...aboutTask(invocation) }
                  : from.kind === "extension"
                    ? {
                        kind: "extension_error",
                        task: reportTask(invocation),
                        extension: from.extension,
                      }
                    : { kind: "host_error", callback: from.callback, task: reportTask(invocation) },
              ),
            ),
          ),
      };
    };

    // ─── Public operations ──────────────────────────────────────────────────

    /**
     * Commits the abort mark, or settles a task that no registered definition can take as `orphaned` when
     * nothing it owns is live, then joins the run invocation seen on the line. A request replaces a
     * `restart` mark; with `keepRestart`, as for an owner's own cleanup, such a task keeps its mark.
     */
    const abort = (id: TaskId, keepRestart = false) =>
      Effect.gen(function* () {
        // Through the Session's commit: on a read-only Harness a terminal or
        // missing task answers as usual, and the mark's write is refused with `ReadOnlySession`.
        const marked = yield* kernel
          .commitWith((tx) =>
            Effect.gen(function* () {
              // The first table read of this commit.
              const found = yield* tx.service
                .task(id)
                .pipe(Effect.catchTag("ReadAfterWrite", (error) => Effect.die(error)));
              if (Option.isNone(found)) {
                return yield* new TaskError({
                  reason: new NotFound({ message: `Task ${id} does not exist` }),
                });
              }
              const current = found.value;
              const invocation = invocations.get(id);
              if (Decisions.abortNeedsScopes(current, invocation !== undefined, keepRestart)) {
                yield* loadScopes(false);
              }
              const overlay = overlayOf(tx);
              const planAbort = (planned: Decisions.Definitions) =>
                Decisions.planAbort(
                  index,
                  planned,
                  current,
                  invocation?.mode,
                  keepRestart,
                  overlay,
                );
              // A decision region probed without migrations, then planned with them (see `reserve`).
              const plan = yield* region(
                () => planAbort({ ...definitions, migrations: false }),
                reloadScopes,
                () => planAbort(definitions),
              );
              if (plan._tag === "AlreadyTerminal") {
                return { result: { _tag: "AlreadyTerminal" } as const, run: undefined };
              }
              if (plan.failure !== undefined) recordFailures([plan.failure]);
              yield* apply(tx, plan.actions);
              return {
                result: { _tag: "Marked" } as const,
                run: plan.joinRun ? invocation : undefined,
              };
            }),
          )
          .pipe(
            // A mark or an orphaning of a live task on the line; nothing here can reject the commit.
            Effect.catchTag("CommitRejected", (error) => Effect.die(error)),
          );
        // The commit listener signalled the run; join it. A run that ignores its signal can outlive a failed
        // Session, which ends the wait instead.
        if (marked.run !== undefined) {
          yield* Effect.raceFirst(
            Deferred.await(marked.run.done),
            // The error is read once the Session failed, so it carries the failure.
            kernel.whenFailed.pipe(
              Effect.andThen(Effect.suspend(() => Effect.fail(closedError()))),
            ),
          );
        }
        return marked.result satisfies AbortTaskResult;
      });

    const awaitTask = (id: TaskId) =>
      Effect.suspend(() => {
        // The waiter this run registered, so an interruption anywhere after the registration removes it.
        let registered: Waiters.Waiter<TaskRecord, ClosedError> | undefined;
        return kernel
          .readOnLine(
            // Checked and registered on the line, so no terminal publication falls between them.
            Effect.gen(function* () {
              if (closing) return yield* closedError();
              // Whether it is live is a decision region.
              if (yield* region(() => index.live.has(id))) {
                registered = taskWaiters.add(id);
                return Option.none<TaskRecord>();
              }
              const record = yield* storage.task(id);
              if (Option.isNone(record)) {
                return yield* new TaskError({
                  reason: new NotFound({ message: `Task ${id} does not exist` }),
                });
              }
              return record;
            }),
          )
          .pipe(
            Effect.flatMap((settled) => {
              if (Option.isSome(settled)) return Effect.succeed(settled.value);
              // A live task always registered a waiter above.
              if (registered === undefined)
                return Effect.die(new Error(`Task ${id} has no waiter`));
              return registered.await;
            }),
            Effect.onInterrupt(() =>
              Effect.sync(() => {
                registered?.cancel();
              }),
            ),
          );
      });

    const awaitIdle = (conversationId: ConversationId | undefined) =>
      Effect.suspend((): Effect.Effect<void, ClosedError> => {
        if (closing) return Effect.fail(closedError());
        if (index.idle(conversationId)) return Effect.void;
        scheduleReconcile();
        return idleWaiters.add(conversationId).await;
      });

    /**
     * `Conversation.abort`: in one commit, withdraw the queued inputs and mark every live non-background
     * task that ordinary traversal from the conversation reaches; resolves once the scope is idle. With
     * `background`, traversal crosses background boundaries, and the wait also covers every task it reached.
     */
    const abortConversation = (
      conversationId: ConversationId,
      background: boolean,
      refuse?: Refuse | undefined,
    ) =>
      Effect.gen(function* () {
        const reached = yield* commitWith((tx) =>
          Effect.gen(function* () {
            const refused = refuse?.();
            if (refused !== undefined) return yield* refused;
            yield* published;
            const queued = yield* loadScopes(true);
            const plan = yield* region(
              () => Decisions.planConversationAbort(index, conversationId, background, queued),
              reloadScopes,
            );
            for (const record of plan.marks) yield* tx.setTask(record);
            for (const id of plan.withdraw) yield* options.withdrawInputs(tx, id);
            return plan.reached;
          }),
        ).pipe(
          // Marks of live tasks on the line; nothing here can reject the commit.
          Effect.catchTag("CommitRejected", (error) => Effect.die(error)),
        );
        if (background) for (const id of reached) yield* awaitTask(id);
        yield* awaitIdle(conversationId);
      });

    const inspectTask = (record: TaskRecord): TaskInspection["state"] => {
      if (invocations.has(record.id)) return { kind: "running" };
      if (record.state.status === "completing") return { kind: "completing" };
      const on = Decisions.waitingOn(index, record);
      if (on.length > 0) return { kind: "waiting", on };
      if (Decisions.asleep(index, record, now())) {
        const until = record.state.until;
        return until === undefined ? { kind: "sleeping" } : { kind: "sleeping", until };
      }
      const fit = Decisions.fitOf(definitions, record);
      if ("reason" in fit) return { kind: "blocked", reason: fit.reason, message: fit.message };
      if (fit.migrates && erased(fit.task).migrate === undefined) {
        return {
          kind: "blocked",
          reason: "migration_failed",
          message: missingMigration(record, erased(fit.task)),
        };
      }
      return { kind: "pending", migrates: fit.migrates };
    };

    // ─── Loops ──────────────────────────────────────────────────────────────

    yield* Signal.forkLoop(
      reconcileSignal,
      Effect.suspend(() => (closing ? Effect.void : reconcile)),
      options.scope,
    );
    yield* Signal.forkLoop(drainSignal, drain, options.scope);
    yield* Signal.forkLoop(
      sweepSignal,
      kernel
        .readOnLine(
          Effect.sync(() => {
            sweepScheduled = false;
            index.sweep();
          }),
        )
        // Refused only once the Session is closing or failed, when nothing is kept anyway.
        .pipe(Effect.ignore),
      options.scope,
    );
    yield* Signal.forkLoop(
      interruptSignal,
      Effect.sync(() => {
        const signalled = Array.from(signalQueue);
        signalQueue.clear();
        for (const invocation of signalled) {
          if (invocation.ended) continue;
          interruptInvocation(invocation);
          // The handler's own waits end by its interruption, signalled first; waits it left running or passed on
          // fail after a yield.
          Signal.doneAfterYield(invocation.stopSignal, Effect.void);
        }
      }),
      options.scope,
    );
    yield* Signal.forkLoop(
      closeSignal,
      Effect.suspend(() =>
        Effect.forEach(closeQueue.splice(0), (scope) => Scope.close(scope, Exit.void), {
          discard: true,
        }),
      ),
      options.scope,
    );
    yield* Signal.forkLoop(wakeSignal, reportWake, options.scope);
    yield* Signal.forkLoop(expirySignal, awaitExpiry, options.scope);
    yield* Signal.forkLoop(sleepSignal, awaitSleep, options.scope);

    const scheduler: Scheduler = {
      open: Effect.gen(function* () {
        yield* kernel.followCommits(observe);
        yield* kernel.subscribeClose(seal);
        yield* commitWith((tx) =>
          Effect.gen(function* () {
            // Every table read before the first write.
            const scans: Array<ReadonlyArray<TaskRecord>> = [];
            for (const status of LIVE_STATUSES) {
              scans.push(
                yield* scanAll((cursor) =>
                  tx.service.scanTasks({ status }, SCAN_PAGE_SIZE, cursor),
                ),
              );
            }
            for (const records of scans) {
              for (const record of records) {
                index.track(undefined, record);
                countActive(1, record.kind);
                resumed.add(record.id);
                // A read-only open loads every task as committed.
                if (options.readOnly) continue;
                // The extensions may differ from those a call parked at its gates under.
                if (options.gated(record)) index.wakeGate(record.id);
                if (record.abandonOnRestart === true) abandoned.add(record.id);
                // A surviving `running` task was cut off by a crash: a close
                // writes the tasks it stops back as `pending`.
                if (record.state.status === "running") {
                  const restarts = (record.restarts ?? 0) + 1;
                  yield* tx.setTask({ ...Decisions.pendingAgain(record), restarts });
                }
                if (record.state.status === "waiting" && record.state.policy === "failFast") {
                  failFastChecks.add(record.id);
                }
              }
            }
          }),
        ).pipe(
          Effect.catchTag("ReadAfterWrite", (error) => Effect.die(error)),
          Effect.catchTag("CommitRejected", (error) => Effect.die(error)),
        );
        // A read-only Harness recovers nothing and reports no wake.
        if (options.readOnly) return;
        // Derive abort marks a crash left unapplied below cancelled owners, and finalize held outcomes.
        cascadePending = true;
        scheduleReconcile();
        // The host hears the wake of open's recovery before that pass can fail:
        // it keeps a recovery wakeup armed while the pass is due, so work a crash cut off resumes. Reported inline,
        // not through the wake loop: the reconcile loop may run first, and a failure there would close the
        // Session before the loop's pass. `wakeLock` keeps it in order with the loop's reports.
        yield* reportWake;
      }),
      resume: () => {
        // The Harness refuses `resume` on a read-only Harness; its internal
        // callers, such as an admission, never reach here, since their commit was refused.
        if (options.readOnly) return;
        enabled = true;
        wakeDue();
        kick();
        noteWake();
      },
      signalAll: () => {
        for (const invocation of invocations.values()) interruptInvocation(invocation);
      },
      join: Effect.suspend(() => {
        const all = Array.from(invocations.values());
        for (const invocation of all) interruptInvocation(invocation);
        return Effect.forEach(all, (invocation) => Deferred.await(invocation.done), {
          discard: true,
        }).pipe(
          // A close is not a crash: a task it stopped is `pending` again, so the
          // next open counts no restart for it. Read on the line after the commits admitted before the seal,
          // such as an abort mark, so the write keeps them. A failure leaves them `running`, counted as if cut
          // off; the storage guard fails the Session, which reports it.
          Effect.andThen(
            Effect.suspend(() =>
              stoppedAtClose.size === 0
                ? Effect.void
                : kernel.writeAtClose(() =>
                    Array.from(stoppedAtClose).flatMap((id): Array<StorageWrite> => {
                      const record = index.live.get(id);
                      if (record?.state.status !== "running") return [];
                      return [{ type: "task", value: Decisions.pendingAgain(record) }];
                    }),
                  ),
            ),
          ),
        );
      }),
      abort,
      awaitTask,
      awaitIdle,
      abortConversation,
      inspect: Effect.gen(function* () {
        yield* published;
        yield* loadScopes(false);
        const inspected: Array<TaskInspection> = [];
        for (const record of index.live.values())
          inspected.push({ record, state: inspectTask(record) });
        const scheduling: HarnessInspection["scheduling"] = closing
          ? "closing"
          : enabled
            ? "running"
            : "paused";
        return { scheduling, tasks: inspected };
      }),
      // A read-only Harness runs nothing, so it never needs a process.
      wake: () =>
        options.readOnly ? { nextWakeAt: Option.none(), activeWork: false } : toWake(computeWake()),
      blockedReason: (subject) => Decisions.blockedReason(definitions, subject),
      subscribeBlocked: (listener) => {
        blockedListeners.add(listener);
        return () => {
          blockedListeners.delete(listener);
        };
      },
      sleepTimerAt: () => sleepAt,
      waiterCount: () => taskWaiters.size() + idleWaiters.size(),
      indexSizes: index.sizes,
      admitting: (tx) =>
        Effect.gen(function* () {
          const tasks = tx.stagedTasks();
          const conversations = tx.stagedConversations();
          if (tasks.length === 0 && conversations.every((record) => record.owner === undefined)) {
            return NO_KEYS;
          }
          const keys = new Set<string>();
          const overlay = overlayOf(tx);
          const created = new Set(tx.createdTaskIds());
          const reached = new Map<string, ConversationId | undefined>();
          for (const record of tasks) {
            if (Ownership.intentChanges(index.live.get(record.id), record))
              keys.add(Keys.INTENT_KEY);
            if (!created.has(record.id) && record.state.status !== "terminal") continue;
            yield* loadChain(parentOf(record), overlay).pipe(internalReads);
            const root = yield* chainKeys(Ownership.parentNode(record), overlay, keys, reached);
            if (root !== undefined) keys.add(Keys.ROOTS_KEY);
          }
          for (const conversation of conversations) {
            const owner = conversation.owner?.taskId;
            if (owner === undefined) continue;
            yield* loadChain({ task: owner }, overlay).pipe(internalReads);
            yield* chainKeys(owner, overlay, keys, reached);
          }
          return Array.from(keys);
        }),
      keptContexts: () => contexts.size,
      idleState: () => {
        let mismatches = 0;
        let idle = 0;
        for (const [conversationId, kept] of contexts) {
          if (kept.idleSince !== undefined) idle++;
          if ((kept.idleSince !== undefined) !== index.inactive(conversationId)) mismatches++;
          if (idleOrder.get(conversationId) !== kept.idleSince) mismatches++;
        }
        let previous = Number.NEGATIVE_INFINITY;
        for (const [conversationId, idleSince] of idleOrder) {
          if (idleSince < previous || contexts.get(conversationId)?.idleSince !== idleSince) {
            mismatches++;
          }
          previous = idleSince;
        }
        return {
          contexts: contexts.size,
          idleContexts: idle,
          idleMismatches: mismatches,
          fullIdleChecks,
        };
      },
    };
    return scheduler;
  });

// ─── Helpers ────────────────────────────────────────────────────────────────

const NO_KEYS: ReadonlyArray<string> = [];

const endedError = (invocation: Invocation) =>
  new TaskError({
    reason: new InvocationEnded({
      message: `Task ${invocation.taskId} invocation has ended`,
    }),
  });

/** Why a bound operation of `invocation` is refused now: its end, then its abort mark. */
const refusal = (invocation: Invocation): TaskError | undefined => {
  if (invocation.ended) return endedError(invocation);
  if (invocation.aborted) {
    return new TaskError({
      reason: new AbortMarked({ message: `Task ${invocation.taskId} has a durable abort mark` }),
    });
  }
  return undefined;
};

const interruptInvocation = (invocation: Invocation) => {
  invocation.signalled = true;
  invocation.fiber?.interruptUnsafe();
};

const invalidWait = (message: string) => new TaskError({ reason: new InvalidWait({ message }) });

const SETTINGS: ReportInfo = { kind: "host_error", callback: "settings" };
const ON_WAKE: ReportInfo = { kind: "host_error", callback: "onWake" };

/**
 * The `task_interrupted` report of a terminal state a task committed because crashes ended it, if it did: a
 * failure whose detail says `restart_limit` or `interrupted`.
 */
const interruptionReport = (
  current: RunningRecord,
  next: Task.NextTaskState<Json, Json>,
): TaskInterrupted | undefined => {
  if (next.status !== "terminal") return undefined;
  const about = {
    taskId: current.id,
    kind: current.kind,
    conversationId: current.conversationId,
    restarts: current.restarts ?? 0,
  };
  if (next.outcome.status !== "failed") {
    // A task that ended for crashes with another outcome says so itself, as a
    // generation whose run answered ends `completed` without its after-run tasks.
    const message = Decisions.endedByCrashesMessage(next);
    if (message === undefined) return undefined;
    return new TaskInterrupted({ ...about, reason: "restart_limit", message });
  }
  const error = next.outcome.error;
  const reason = Decisions.interruptionOf(error.detail);
  if (reason === undefined) return undefined;
  return new TaskInterrupted({ ...about, reason, message: error.message });
};

/**
 * A phase whose task ended `faulted`, `failed` or `orphaned`: the span's failure, with a fixed category and the
 * diagnostic capped at 500 characters. Internal: caught as the span ends, so the
 * failure reaches the tracer as the span's exit and no caller.
 */
class PhaseFailed extends Data.TaggedError("PhaseFailed")<{
  readonly category: "task_faulted" | "task_failed" | "task_orphaned";
  readonly message: string;
}> {}

const failedPhase = (record: TaskRecord | undefined): PhaseFailed | undefined => {
  const state = record?.state;
  if (state?.status !== "terminal" && state?.status !== "completing") return undefined;
  const outcome = state.outcome;
  switch (outcome.status) {
    case "faulted":
    case "failed":
      return new PhaseFailed({
        category: `task_${outcome.status}`,
        message: outcome.error.message.slice(0, 500),
      });
    case "orphaned":
      return new PhaseFailed({ category: "task_orphaned", message: outcome.reason.slice(0, 500) });
    default:
      return undefined;
  }
};

/** The checkpoint's `attempt`, when the checkpoint is an object with a numeric one. */
const attemptOf = (checkpoint: Json): number | undefined => {
  if (!Predicate.isObject(checkpoint) || Array.isArray(checkpoint)) return undefined;
  // SAFETY: a non-array JSON object; `attempt` may be absent.
  const attempt = (checkpoint as { readonly attempt?: Json }).attempt;
  return Predicate.isNumber(attempt) ? attempt : undefined;
};

/** The scheduler's own reads use valid queries: a `StorageRequestError` there is a bug. */
const internalReads = <A, E, R>(effect: Effect.Effect<A, E | StorageRequestError, R>) =>
  Effect.catchTag(effect, "StorageRequestError", (error) => Effect.die(error));

const isStorageFailure = (error: unknown): error is StorageFailure =>
  Predicate.isTagged(error, "StorageFailure");

const overlayOf = (tx: Transaction): Overlay => ({
  tasks: new Map(tx.stagedTasks().map((record) => [record.id, record])),
  edges: new Map(
    tx.stagedConversations().map((record) => [record.id, record.owner?.taskId ?? null]),
  ),
});

/** Own memo entry only; memo names such as `toString` must not resolve to inherited properties. */
const memoOf = (record: TaskRecord | undefined, name: string): Option.Option<Json> => {
  if (record === undefined || !("memos" in record) || record.memos === undefined) {
    return Option.none();
  }
  return Object.hasOwn(record.memos, name) ? Option.some(record.memos[name]!) : Option.none();
};

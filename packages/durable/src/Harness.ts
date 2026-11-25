/**
 * The Harness: a Session with its task scheduler.
 *
 * `Harness.layer` opens the Session over the `AgentStorage` in context,
 * reconciles the tasks a previous process left (`running` back to `pending`)
 * and dispatches nothing until `resume` or a call that asks for progress.
 * Phase handlers run off the Session line, one invocation per task; every
 * task transition is decided and written on the line. Closing the layer's
 * scope seals admission and reservation, interrupts and joins every
 * invocation, and writes no outcome: the work resumes on the next open.
 *
 * On top of tasks it is an agent: conversations take
 * submissions, a built-in generation task answers them with the model of the
 * conversation's agent, and a built-in tool task runs every tool call.
 *
 * @since 0.0.0
 */
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import type * as Option from "effect/Option";
import type * as Scope from "effect/Scope";
import type * as Stream from "effect/Stream";
import type { Agent, AgentChange } from "./Agent.ts";
import type { CompactionResult } from "./Compaction.ts";
import type {
  AgentStorage,
  ConversationQuery,
  EntryQuery,
  StorageFailure,
  StorageRequestError,
} from "./AgentStorage.ts";
import type { ContextView, EntryNotVisible } from "./ContextView.ts";
import type { ExecutionEnvBuilder } from "./ExecutionEnv.ts";
import type * as Extension from "./Extension.ts";
import type { ConversationId, EntryId, SubmissionId, TaskId } from "./Ids.ts";
import * as internal from "./internal/harness.ts";
import type { Models } from "./Models.ts";
import type { ConversationRecord, EntryRecord, SubmissionRecord, TaskRecord } from "./Records.ts";
import type { Report } from "./Report.ts";
import { Session, type CommitError, type CommitOptions, type SessionEnd } from "./Session.ts";
import type {
  CommitRejected,
  DocumentAccessError,
  ReadOnlySession,
  SessionClosed,
  SessionFailed,
} from "./SessionError.ts";
import type { Wake } from "./SessionWakeups.ts";
import type { Signal, SignalResult } from "./Signal.ts";
import type { HarnessSettings } from "./Settings.ts";
import type {
  AdmitError,
  ConversationBusy,
  ConversationIdle,
  Submission,
  SubmissionDraft,
  SubmissionError,
  WithdrawSubmissionResult,
} from "./Submission.ts";
import type {
  AbortConversationOptions,
  ContextOptions,
  SettledTask,
  TypedTaskRecord,
} from "./Task.ts";
import type { TaskError } from "./TaskError.ts";
import type { TaskGraph, TaskGraphWatch } from "./TaskGraph.ts";
import type { ConversationOwnership, Tx, TxDocError } from "./Tx.ts";
import type { UsageState } from "./Usage.ts";
import type { Observation, Observer } from "./Observer.ts";
import type { ConversationView, ConversationWatch, ConversationWindow } from "./View.ts";

// -----------------------------------------------------------------------------
// models
// -----------------------------------------------------------------------------

/**
 * Errors of a Harness call: the Session ended, or a read failed.
 *
 * @category errors
 * @since 0.0.0
 */
export type HarnessError = SessionClosed | SessionFailed | StorageFailure | StorageRequestError;

/**
 * What `abortTask` did: committed the mark (or orphaned a blocked task), or
 * found the task already terminal.
 *
 * @category models
 * @since 0.0.0
 */
export type AbortTaskResult = { readonly _tag: "Marked" } | { readonly _tag: "AlreadyTerminal" };

/**
 * Why a live task cannot be reserved under the Harness's extensions. Derived,
 * never persisted.
 *
 * @category models
 * @since 0.0.0
 */
export type BlockedReason = "missing_task" | "task_too_old" | "migration_failed";

/**
 * One live task with its derived state: `pending` for a task the next reservation can take,
 * `sleeping` until its `until`.
 *
 * @category models
 * @since 0.0.0
 */
export interface TaskInspection {
  readonly record: TaskRecord;
  readonly state:
    | { readonly kind: "running" }
    | { readonly kind: "pending"; readonly migrates: boolean }
    | { readonly kind: "waiting"; readonly on: ReadonlyArray<TaskId> }
    /** Asleep until the Harness clock reaches `until`; absent for a sleep that waits only for signals. */
    | { readonly kind: "sleeping"; readonly until?: number }
    /** Outcome held until its ordinary owned work drains. */
    | { readonly kind: "completing" }
    | {
        readonly kind: "blocked";
        readonly reason: BlockedReason;
        // A message instead of the error itself; the cause went to `onReport` when the scheduler tried the
        // migration.
        /** The failure of a migration the scheduler tried, or the missing migration. */
        readonly message?: string | undefined;
      };
}

/**
 * Live work at one point on the Session line.
 *
 * @category models
 * @since 0.0.0
 */
export interface HarnessInspection {
  readonly scheduling: "paused" | "running" | "closing";
  readonly tasks: ReadonlyArray<TaskInspection>;
  /** Queued and placed submissions in ID order. */
  readonly submissions: ReadonlyArray<SubmissionRecord>;
}

/**
 * Runs inside the creating commit, after the creation hook and the `agent`
 * change; table reads fail with `ReadAfterWrite`, document access works.
 *
 * @category models
 * @since 0.0.0
 */
export type ConversationInit = (
  conversationId: ConversationId,
) => Effect.Effect<void, TxDocError, Tx>;

/**
 * Options of `Harness.ensureRoot`.
 *
 * @category models
 * @since 0.0.0
 */
export interface RootOptions {
  /** Applied in the creating commit after the creation hook, before `init`; ignored when the root exists. */
  readonly agent?: AgentChange | undefined;
  readonly init?: ConversationInit | undefined;
}

/**
 * Options of `Harness.createConversation` and `Conversation.fork`.
 *
 * @category models
 * @since 0.0.0
 */
export interface CreateConversationOptions extends RootOptions {
  readonly ownership: ConversationOwnership;
}

/**
 * A query of `Conversation.entries`: fork-aware history, newest first unless
 * `order` is `ascending`.
 *
 * @category models
 * @since 0.0.0
 */
export type ConversationEntryQuery = Omit<EntryQuery, "conversationId">;

/**
 * A handle of an existing conversation. Handles are stateless; compare them
 * by `id`.
 *
 * @category models
 * @since 0.0.0
 */
// `entries` is a `Stream` instead of cursor pages; `watch` is a scoped Stream; `observe` is on the handle, next to
// `watch`.
export interface Conversation {
  readonly id: ConversationId;
  /**
   * Durably admits user input, a passive entry write or a continue in one commit. Enables
   * scheduling. `whenBusy: "reject"` input and a busy continue fail with `ConversationBusy`, `whenIdle:
   * "reject"` input on an idle conversation with `ConversationIdle`; either writes nothing.
   */
  readonly submit: (
    draft: SubmissionDraft,
  ) => Effect.Effect<
    Submission,
    | ConversationBusy
    | ConversationIdle
    | SubmissionError
    | HarnessError
    | CommitRejected
    | ReadOnlySession
  >;
  /**
   * `submit` inside a commit: admits `draft` inside the caller's commit, with the rules of `submit`, and
   * returns its ID; the admission is stored or lost with that commit. It must come before the commit's
   * first table write, once per commit, and fails with `ReadAfterWrite` otherwise. Enables scheduling.
   */
  readonly admit: (draft: SubmissionDraft) => Effect.Effect<SubmissionId, AdmitError, Tx>;
  /**
   * Admits a manual compaction task, with its status in `core.live.compactions`, and returns its ID.
   * It summarizes while the conversation keeps working and places its summary as a write submission, whose ID is
   * its result's `submissionId`; `instructions` add a focus to the summarization prompt. Ordinary work of the
   * conversation: `abort` aborts it. Enables scheduling.
   */
  readonly compact: (
    instructions?: string | undefined,
  ) => Effect.Effect<TaskId<CompactionResult>, CommitError | ReadOnlySession>;
  /** Admits a write of a `core.reset` entry that starts a new context, carrying `handoff` as a user message. */
  readonly reset: (
    handoff?: string | undefined,
  ) => Effect.Effect<void, SubmissionError | HarnessError | CommitRejected | ReadOnlySession>;
  /** The conversation's agent, resolved now. */
  readonly agent: Effect.Effect<Agent, HarnessError>;
  /** Applies one change to `core.agent` in its own commit; starts nothing. */
  readonly configure: (
    change: AgentChange,
  ) => Effect.Effect<void, CommitError | DocumentAccessError | StorageRequestError>;
  /** The committed active transcript and model context; with `at`, as of that visible entry. */
  readonly context: (
    options?: ContextOptions,
  ) => Effect.Effect<ContextView, HarnessError | EntryNotVisible>;
  /** Fork-aware history of this conversation. */
  readonly entries: (query?: ConversationEntryQuery) => Stream.Stream<EntryRecord, HarnessError>;
  /** A conversation that inherits this one's history through the visible entry `at`. */
  readonly fork: (
    at: EntryId,
    options: CreateConversationOptions,
  ) => Effect.Effect<Conversation, CommitError | DocumentAccessError>;
  /**
   * The view as a watch with its own window (default the newest 50 entries), released with the scope. A window with bad bounds dies with a `RangeError`.
   */
  readonly watch: (
    window?: ConversationWindow | undefined,
  ) => Effect.Effect<ConversationWatch, HarnessError, Scope.Scope>;
  /**
   * Attaches a synchronous `Observer` to the view with its own window, on the same mount as `watch`: at most one call per commit, unfolded. The attaching Effect
   * governs only the attachment; the observer ends `Failed` when the Session closes or fails.
   */
  readonly observe: (
    observer: Observer<ConversationView>,
    window?: ConversationWindow | undefined,
  ) => Effect.Effect<Observation<ConversationView>, HarnessError>;
  /**
   * A Session commit bound to this conversation: `tx.createTask` defaults to it. `options.persistence` may
   * defer storing it (`CommitPersistence`).
   */
  readonly commit: <A, E>(
    change: Effect.Effect<A, E, Tx>,
    options?: CommitOptions,
  ) => Effect.Effect<A, E | CommitError>;
  /**
   * Withdraws queued inputs, marks the live non-background tasks ordinary traversal reaches (all of them
   * with `background`), and waits until the conversation is ordinarily idle. Enables scheduling.
   */
  readonly abort: (
    options?: AbortConversationOptions,
  ) => Effect.Effect<void, HarnessError | ReadOnlySession>;
  /** Waits until ordinary traversal from this conversation reaches no live non-background task. */
  readonly awaitIdle: Effect.Effect<void, HarnessError | ReadOnlySession>;
}

/**
 * What a Harness fills in when a caller leaves it out.
 *
 * @category models
 * @since 0.0.0
 */
export interface HarnessDefaults<R = never> {
  /**
   * The agent of a top-level conversation (no owner, no parent) created without one, by `ensureRoot` or
   * `createConversation`, applied in its creation commit; a fork keeps its parent's, and an explicit `agent`
   * wins. An Effect is resolved once, when the layer is built, and its requirements are the layer's.
   */
  readonly agent?: AgentChange | Effect.Effect<AgentChange, never, R> | undefined;
}

/**
 * Options of `Harness.layer`.
 *
 * @category models
 * @since 0.0.0
 */
export interface HarnessOptions<
  X extends ReadonlyArray<Extension.Any>,
  R = never,
  E = never,
> extends HarnessLayerOptions<R, E> {
  /** The static registry; extension and task names are unique across it. */
  readonly extensions: X;
}

/**
 * Options of a declared Harness's `layer`: those of `Harness.layer` without the extensions, which the
 * declaration fixes.
 *
 * @category models
 * @since 0.0.0
 */
export interface HarnessLayerOptions<R = never, E = never> {
  /** What the Harness fills in when a caller leaves it out. */
  readonly defaults?: HarnessDefaults<R> | undefined;
  /**
   * The host's settings, read at every decision that uses one. Synchronous: some
   * readers run on the Session line.
   */
  readonly settings?: (() => HarnessSettings) | undefined;
  /**
   * Builds a conversation's environment at each use: for each tool call, each request preparation and each
   * `TaskRuntime.env`, from the conversation's ID and its agent's current `cwd`. Without it there is no
   * environment. Its requirements are the layer's; a failure is classified by where the environment was
   * needed (`ExecutionEnvError`).
   *
   * The host caches what is expensive to build, such as one environment per directory, outside the builder.
   * Don't acquire scoped resources in it: its `Scope` is the Harness's, so a finalizer added at each call would
   * accumulate until the Harness closes.
   */
  readonly env?: ExecutionEnvBuilder<R, E> | undefined;
  /** Runs in every commit that creates or forks a conversation, with the new record. */
  readonly conversationCreated?:
    | ((record: ConversationRecord) => Effect.Effect<void, TxDocError, Tx>)
    | undefined;
  /**
   * Failures that fail no caller, such as a throwing hook or listener, a task that faulted, and the failure
   * that fails the Session, once: one `Report`, what happened (`kind`, with its fields) and its
   * `cause`. A host tells a failed Session (`storage_failed`) from a faulted task (`task_faulted`). Default:
   * `Effect.logWarning`. A report whose `onReport` fails is dropped.
   */
  readonly onReport?: ((report: Report) => Effect.Effect<void>) | undefined;
  /**
   * Open for reading only, as a viewer of a session another process drives. Open
   * recovers nothing: a `running` task stays as committed. Every commit with writes fails with
   * `ReadOnlySession` before it reaches storage, and so do `resume` and every call that would run tasks
   * (`awaitTask`, `awaitIdle`, a submit or an in-commit admission, a submission's `await`, a conversation's
   * `abort` and `awaitIdle`), before they read anything. `abortTask` answers a terminal or missing task as
   * usual; its mark is a write, refused. Reads and watches work; `wake` is always inactive and the host's
   * `SessionWakeups` is never called. Reopen without it to write.
   */
  readonly readOnly?: boolean | undefined;
}

// -----------------------------------------------------------------------------
// service
// -----------------------------------------------------------------------------

/**
 * The open Harness.
 *
 * @category services
 * @since 0.0.0
 */
// No cancellation parameter, `Option` for lookups and tagged results. Document reads and watches are on the
// `Session` service the same layer provides.
export class Harness extends Context.Service<
  Harness,
  {
    /** A Session commit; `options.persistence` may defer storing it. */
    readonly commit: <A, E>(
      change: Effect.Effect<A, E, Tx>,
      options?: CommitOptions,
    ) => Effect.Effect<A, E | CommitError>;
    /** The root conversation, created ownerless at its reserved ID, with `agent` and `init`, on first use. */
    readonly ensureRoot: (
      options?: RootOptions,
    ) => Effect.Effect<Conversation, CommitError | DocumentAccessError>;
    /** A handle of an existing conversation. */
    readonly conversation: (
      id: ConversationId,
    ) => Effect.Effect<Option.Option<Conversation>, HarnessError>;
    /** Creates a conversation with explicit ownership in its own commit. */
    readonly createConversation: (
      options: CreateConversationOptions,
    ) => Effect.Effect<Conversation, CommitError | DocumentAccessError>;
    /**
     * `Tx.signal` in a commit of its own: delivers `value` to task `taskId` under `key`. Enables scheduling.
     * A host arms its recovery wakeup before it, as before `submit`.
     */
    readonly signal: <T>(
      taskId: TaskId,
      signal: Signal<T>,
      key: string,
      value: T,
    ) => Effect.Effect<SignalResult, CommitError | ReadOnlySession>;
    /** The committed task record. Never enables scheduling. */
    readonly getTask: <R>(
      id: TaskId<R>,
    ) => Effect.Effect<
      Option.Option<TypedTaskRecord<TaskRecord["input"], TaskRecord["input"], R>>,
      HarnessError
    >;
    /** Waits for the terminal receipt; interrupting the wait does not abort the task. Enables scheduling. */
    readonly awaitTask: <R>(
      id: TaskId<R>,
    ) => Effect.Effect<SettledTask<R>, TaskError | HarnessError | ReadOnlySession>;
    /**
     * Commits the abort mark, then joins the active run invocation; a blocked task with no live owned work
     * is orphaned at once. The abort handler runs later, once the task's owned work has drained.
     */
    readonly abortTask: (
      id: TaskId,
    ) => Effect.Effect<AbortTaskResult, TaskError | HarnessError | ReadOnlySession>;
    /** Waits until ordinary traversal from every ownerless conversation reaches no live task. Enables scheduling. */
    readonly awaitIdle: Effect.Effect<void, HarnessError | ReadOnlySession>;
    /** Live work and its derived state, read on the line; runs no task code. */
    readonly inspect: Effect.Effect<HarnessInspection, HarnessError>;
    /** Enables scheduling and makes due sleeps runnable: a host's wakeup calls it. Idempotent. */
    readonly resume: Effect.Effect<void, SessionClosed | SessionFailed | ReadOnlySession>;
    /** When the Session needs to run next. */
    readonly wake: Effect.Effect<Wake, HarnessError>;
    /**
     * The task graph as a watch, released with the scope. The first observer
     * builds the graph on the line from the committed live tasks; it never enables scheduling.
     */
    readonly watchTaskGraph: Effect.Effect<TaskGraphWatch, HarnessError, Scope.Scope>;
    /**
     * Attaches a synchronous `Observer` to the task graph, on the same mount as `watchTaskGraph`: at most one call per commit, and one on the line, outside any commit,
     * when a failed migration changes a `blocked` reason. The attaching Effect governs only the attachment;
     * the observer ends `Failed` when the Session closes or fails.
     */
    readonly observeTaskGraph: (
      observer: Observer<TaskGraph>,
    ) => Effect.Effect<Observation<TaskGraph>, HarnessError>;
    /** Settles once the Session has ended. */
    readonly closed: Effect.Effect<SessionEnd>;
    /** A handle of an existing submission, for example after reopen. Read-only. */
    readonly submission: (
      id: SubmissionId,
    ) => Effect.Effect<Option.Option<Submission>, HarnessError>;
    /** The submission a request ID identifies in a conversation. Read-only. */
    readonly submissionByRequest: (
      conversationId: ConversationId,
      requestId: string,
    ) => Effect.Effect<Option.Option<Submission>, HarnessError>;
    /** Withdraws a queued submission; `NotFound` for one unknown or of another conversation. */
    readonly withdrawSubmission: (
      id: SubmissionId,
      conversationId?: ConversationId | undefined,
    ) => Effect.Effect<WithdrawSubmissionResult, HarnessError | ReadOnlySession>;
    /** The Session total: every conversation's `core.usage` summed. Read-only. */
    readonly usage: Effect.Effect<UsageState, HarnessError>;
    /** Every conversation, in ID order. */
    readonly conversations: (
      query?: ConversationQuery,
    ) => Stream.Stream<ConversationRecord, HarnessError>;
  }
>()("@effect-durable-harness/core/Harness") {}

/**
 * The Harness over the `AgentStorage` in context; it also provides the
 * `Session`. Building it reconciles what a previous process left; closing its
 * scope closes it. The handlers' requirements are the layer's.
 *
 * @category layers
 * @since 0.0.0
 */
export const layer = <const X extends ReadonlyArray<Extension.Any>, R = never, E = never>(
  options: HarnessOptions<X, R, E>,
): Layer.Layer<
  Harness | Session,
  StorageFailure | SessionFailed,
  AgentStorage | Models | Extension.Requirements<X> | R
> =>
  Layer.effectContext(
    Effect.map(internal.make(options), (opened) =>
      Context.make(Harness, opened.harness).pipe(Context.add(Session, opened.session)),
    ),
  );

/**
 * An agent's contract: its name and the extensions it serves, without a storage, a model or any handler
 * (as `HttpApi.make`). `layer` builds the agent from it.
 *
 * @category models
 * @since 0.0.0
 */
export interface HarnessDeclaration<X extends ReadonlyArray<Extension.Any>> {
  readonly name: string;
  /** The static registry. */
  readonly extensions: X;
  /** `Harness.layer` over these extensions: it requires one handler per tool and task they declare. */
  readonly layer: <R = never, E = never>(
    options?: HarnessLayerOptions<R, E>,
  ) => Layer.Layer<
    Harness | Session,
    StorageFailure | SessionFailed,
    AgentStorage | Models | Extension.Requirements<X> | R
  >;
}

/**
 * Declares an agent: its name and its extensions. The declaration imports no implementation; its
 * `layer(options)` is the agent, as `Harness.layer` with these extensions.
 *
 * @category constructors
 * @since 0.0.0
 */
export const make = <const X extends ReadonlyArray<Extension.Any>>(
  name: string,
  options: { readonly extensions: X },
): HarnessDeclaration<X> => ({
  name,
  extensions: options.extensions,
  layer: (layerOptions) => layer({ ...layerOptions, extensions: options.extensions }),
});

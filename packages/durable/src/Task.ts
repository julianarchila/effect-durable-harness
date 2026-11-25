/**
 * Durable task definitions.
 *
 * A task is a state machine with one durable checkpoint per phase. Each phase
 * handler runs off the Session line and commits its next state through its
 * runtime: a replacement checkpoint, a wait, or its outcome. The scheduler
 * runs the phases of a task in one in-memory invocation, applies the step
 * rules between them, and runs `abort` in a fresh invocation once the task is
 * abort-marked and its owned work has drained.
 *
 * @since 0.0.0
 */
import type * as Cause from "effect/Cause";
import * as Context from "effect/Context";
import type * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import type * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import type * as Option from "effect/Option";
import * as Predicate from "effect/Predicate";
import * as Scope from "effect/Scope";
import type * as Schema from "effect/Schema";
import type { Agent } from "./Agent.ts";
import type {
  Cursor,
  EntryQuery,
  Page,
  StorageFailure,
  StorageRequestError,
} from "./AgentStorage.ts";
import type { ContextView, EntryNotVisible } from "./ContextView.ts";
import type { ExecutionEnv, ExecutionEnvError } from "./ExecutionEnv.ts";
import type { EntryKind, TypedEntry } from "./Entry.ts";
import type { ConversationId, EntryId, SubmissionId, TaskId } from "./Ids.ts";
import type { HookFunction } from "./Hook.ts";
import type {
  EntryRecord,
  JoinPolicy,
  SignalKeyRecord,
  TaskOutcomeError,
  TaskRecord,
  WakeReason,
} from "./Records.ts";
import type { ReportFrom } from "./Report.ts";
import type {
  CommitError,
  CommitOptions,
  DocSnapshot,
  DocSnapshotAsOf,
  DocWatchAcquire,
  ReadError,
  Session,
} from "./Session.ts";
import type { ConversationNotFound, SessionClosed, SessionFailed } from "./SessionError.ts";
import type { Settings } from "./Settings.ts";
import type { InvalidSignal, Signal, SignalKey, SignalState } from "./Signal.ts";
import type {
  AdmitError,
  ConversationBusy,
  ConversationIdle,
  Submission,
  SubmissionDraft,
  SubmissionError,
} from "./Submission.ts";
import type { TaskError } from "./TaskError.ts";
import type { TraceCarrier } from "./Telemetry.ts";
import { Tx, type TxWriteError } from "./Tx.ts";
import * as calls from "./internal/calls.ts";
import { capturedServices, taskHandler } from "./internal/handlers.ts";

// -----------------------------------------------------------------------------
// states
// -----------------------------------------------------------------------------

/**
 * How a task ended, with its typed result.
 *
 * @category states
 * @since 0.0.0
 */
export type TaskOutcome<R> =
  | { readonly status: "completed"; readonly result: R }
  | { readonly status: "failed"; readonly error: TaskOutcomeError; readonly result?: R }
  | { readonly status: "aborted"; readonly reason?: string; readonly result?: R }
  | { readonly status: "orphaned"; readonly reason: string }
  | { readonly status: "faulted"; readonly error: TaskOutcomeError };

/**
 * Durable execution state of a task, typed by its checkpoint and result.
 *
 * @category states
 * @since 0.0.0
 */
export type TaskState<S, R> =
  /** `wake` says why the task woke from its last wait or sleep, kept while it runs at that checkpoint. */
  | { readonly status: "pending"; readonly checkpoint: S; readonly wake?: WakeReason }
  | { readonly status: "running"; readonly checkpoint: S; readonly wake?: WakeReason }
  /**
   * Parked without an invocation until every task in `on` is terminal, or one of them with policy `any`; then
   * resumes at `checkpoint`.
   */
  | {
      readonly status: "waiting";
      readonly checkpoint: S;
      readonly on: ReadonlyArray<TaskId>;
      readonly policy: JoinPolicy;
    }
  /**
   * Parked without an invocation until the Harness clock reaches `until`, or one of `signals` holds a value no
   * wake used, whichever comes first; then resumes at `checkpoint`. Needs `until` or a non-empty `signals`.
   */
  | {
      readonly status: "sleeping";
      readonly checkpoint: S;
      readonly until?: number;
      readonly signals?: ReadonlyArray<SignalKeyRecord>;
    }
  /** Outcome decided; terminal once no ordinary owned work below is live. Runs no more code. */
  | { readonly status: "completing"; readonly outcome: TaskOutcome<R> }
  | { readonly status: "terminal"; readonly outcome: TaskOutcome<R> };

/**
 * The state a task commits for itself: a replacement checkpoint, a wait, a
 * sleep, or its outcome. A terminal state becomes `completing` while ordinary
 * owned work below the task is live. A task never writes its own `wake`: a
 * commit of the same checkpoint keeps it, any other state drops it.
 *
 * @category states
 * @since 0.0.0
 */
export type NextTaskState<S, R> =
  | { readonly status: "running"; readonly checkpoint: S; readonly wake?: never }
  | Extract<TaskState<S, R>, { readonly status: "waiting" | "sleeping" | "terminal" }>;

/**
 * A task record typed by its input, checkpoint and result.
 *
 * @category states
 * @since 0.0.0
 */
export type TypedTaskRecord<I, S, R> = Omit<TaskRecord, "id" | "input" | "state" | "memos"> & {
  readonly id: TaskId<R>;
  readonly input: I;
  readonly state: TaskState<S, R>;
  readonly memos?: Readonly<{ [name: string]: Schema.Json }>;
};

/**
 * The record a phase handler runs with: `running`, at the phase's checkpoint.
 *
 * @category states
 * @since 0.0.0
 */
export type RunningTask<I, S, R> = TypedTaskRecord<I, S, R> & {
  readonly state: {
    readonly status: "running";
    readonly checkpoint: S;
    /** Why the task woke from its last wait or sleep; absent when it did not wake from one. */
    readonly wake?: WakeReason;
  };
};

/**
 * A terminal task record: the durable result receipt.
 *
 * @category states
 * @since 0.0.0
 */
export type SettledTask<R> = TypedTaskRecord<Schema.Json, Schema.Json, R> & {
  readonly state: { readonly status: "terminal"; readonly outcome: TaskOutcome<R> };
};

// -----------------------------------------------------------------------------
// transitions
// -----------------------------------------------------------------------------

// The transitions are the task's state objects, built by name so a phase reads as its intent. A phase commits
// one with `run.commit`, or with the runtime's own `next`, `waitFor`, `sleepUntil`, `complete`, `fail` and
// `aborted`.

/**
 * The state that continues the task at `checkpoint`: `{ status: "running", checkpoint }`.
 *
 * @category transitions
 * @since 0.0.0
 */
export const next: <const S>(checkpoint: S) => {
  readonly status: "running";
  readonly checkpoint: S;
} = calls.next;

/**
 * Options of `Task.waitFor`: where the task resumes, and how it treats the tasks it waits on.
 *
 * @category transitions
 * @since 0.0.0
 */
export interface WaitOptions<S> {
  /** The checkpoint the task resumes at once every task in `on` is terminal. */
  readonly checkpoint: S;
  readonly policy: JoinPolicy;
}

/**
 * The state that parks the task until every task in `on` is terminal:
 * `{ status: "waiting", checkpoint, on, policy }`.
 *
 * @category transitions
 * @since 0.0.0
 */
export const waitFor = <const S>(
  on: ReadonlyArray<TaskId>,
  options: WaitOptions<S>,
): {
  readonly status: "waiting";
  readonly checkpoint: S;
  readonly on: ReadonlyArray<TaskId>;
  readonly policy: JoinPolicy;
} => calls.waitFor(on, options.checkpoint, options.policy);

/**
 * The state that ends the phase asleep until the Harness clock reaches `until`, epoch milliseconds, and then
 * resumes the task at `checkpoint`: `{ status: "sleeping", checkpoint, until }`. The
 * task holds no invocation while it sleeps; only an abort mark ends a sleep early. `until` must be a finite
 * number, `0` or more, and an abort handler cannot sleep.
 *
 * @category transitions
 * @since 0.0.0
 */
export const sleepUntil: <const S>(
  until: number,
  checkpoint: S,
) => {
  readonly status: "sleeping";
  readonly checkpoint: S;
  readonly until: number;
} = calls.sleepUntil;

/**
 * Options of `Task.sleep`: where the task resumes, and what it waits for: a
 * time, signal keys, or both, whichever comes first.
 *
 * @category transitions
 * @since 0.0.0
 */
export interface SleepOptions<S> {
  /** The checkpoint the task resumes at. */
  readonly checkpoint: S;
  /** Epoch milliseconds on the Harness clock; the wake is `expired` with signals, `time` without. */
  readonly until?: number | undefined;
  /** Keys that end the sleep once one holds a value no wake used; the wake names it. */
  readonly signals?: ReadonlyArray<SignalKey<unknown>> | undefined;
}

/**
 * The state that ends the phase asleep until `until`, or a value under one of
 * `signals`, whichever comes first, and then resumes the task at `checkpoint`
 * with its `wake`. It needs `until` or a non-empty `signals`; a key that already
 * woke a wait or expired is refused, so a second wait for the same kind of
 * signal uses a new key. A value delivered before the sleep wakes it at once.
 *
 * @category transitions
 * @since 0.0.0
 */
export const sleep: <const S>(options: SleepOptions<S>) => {
  readonly status: "sleeping";
  readonly checkpoint: S;
  readonly until?: number;
  readonly signals?: ReadonlyArray<SignalKeyRecord>;
} = calls.sleep;

/**
 * The state that completes the task with `result`:
 * `{ status: "terminal", outcome: { status: "completed", result } }`.
 *
 * @category transitions
 * @since 0.0.0
 */
export const complete: <const R>(result: R) => {
  readonly status: "terminal";
  readonly outcome: { readonly status: "completed"; readonly result: R };
} = calls.complete;

/**
 * The state that fails the task with `message`:
 * `{ status: "terminal", outcome: { status: "failed", error: { message } } }`.
 *
 * @category transitions
 * @since 0.0.0
 */
export const fail: (message: string) => {
  readonly status: "terminal";
  readonly outcome: { readonly status: "failed"; readonly error: TaskOutcomeError };
} = calls.fail;

/**
 * The state that ends the task aborted, without a reason:
 * `{ status: "terminal", outcome: { status: "aborted" } }`.
 *
 * @category transitions
 * @since 0.0.0
 */
export const aborted: {
  readonly status: "terminal";
  readonly outcome: { readonly status: "aborted" };
} = calls.aborted;

// -----------------------------------------------------------------------------
// runtime
// -----------------------------------------------------------------------------

/**
 * Errors of a runtime read: the invocation ended, or the read failed.
 *
 * @category runtime
 * @since 0.0.0
 */
export type RuntimeReadError =
  | TaskError
  | StorageRequestError
  | StorageFailure
  | SessionFailed
  | SessionClosed;

/**
 * An existing conversation bound to one invocation: every operation rejects
 * once the invocation ended.
 *
 * @category runtime
 * @since 0.0.0
 */
export interface ConversationHandle {
  readonly id: ConversationId;
  /** `Conversation.submit`, rejected once the invocation ended or was aborted. */
  readonly submit: (
    draft: SubmissionDraft,
  ) => Effect.Effect<
    Submission,
    RuntimeReadError | ConversationBusy | ConversationIdle | SubmissionError | CommitError
  >;
  /**
   * `Conversation.admit` inside the invocation's own commit, rejected once the
   * invocation ended or was aborted.
   */
  readonly admit: (
    draft: SubmissionDraft,
  ) => Effect.Effect<SubmissionId, AdmitError | TaskError, Tx>;
  /** `Conversation.abort`: withdraws queued inputs, marks the ordinary work below and waits until it is idle. */
  readonly abort: (options?: AbortConversationOptions) => Effect.Effect<void, RuntimeReadError>;
  /** Waits until ordinary traversal from the conversation reaches no live non-background task. */
  readonly awaitIdle: Effect.Effect<void, RuntimeReadError>;
}

/**
 * Options of a conversation abort.
 *
 * @category runtime
 * @since 0.0.0
 */
export interface AbortConversationOptions {
  /** Also aborts background work below the conversation. */
  readonly background?: boolean | undefined;
}

/**
 * `memo`: read the committed winner, or insert a first-writer-wins candidate.
 *
 * @category runtime
 * @since 0.0.0
 */
export interface TaskMemo {
  /** The committed memo, if any. */
  <T extends Schema.Json>(name: string): Effect.Effect<Option.Option<T>, TaskError>;
  /** Inserts `candidate` unless a memo of that name exists, in one gated commit; returns the winner. */
  <T extends Schema.Json>(name: string, candidate: T): Effect.Effect<T, TaskError | CommitError>;
}

/**
 * `entry`: one committed entry visible from the task's conversation.
 *
 * @category runtime
 * @since 0.0.0
 */
export interface TaskEntryRead {
  (id: EntryId): Effect.Effect<Option.Option<EntryRecord>, RuntimeReadError>;
  <D extends Schema.Json>(
    kind: EntryKind<D>,
    id: EntryId,
  ): Effect.Effect<Option.Option<TypedEntry<D>>, RuntimeReadError>;
}

/**
 * `entries`: one page of the task's conversation's raw history:
 * fork-aware, in entry-ID order, newest first unless `order` is `ascending`. Compaction never changes it, so a
 * range bounded by entry IDs reads the same before and after a compaction and on a rerun.
 *
 * @category runtime
 * @since 0.0.0
 */
// The query's `conversationId` is the task's, which the runtime binds; a page is `AgentStorage`'s `Page`,
// whose `next` is an `Option`.
export type TaskEntriesRead = (
  query: Omit<EntryQuery, "conversationId">,
  limit: number,
  cursor?: Cursor | undefined,
) => Effect.Effect<Page<EntryRecord>, RuntimeReadError>;

/**
 * What a phase or abort handler gets from the scheduler for one invocation.
 * Every operation rejects with `InvocationEnded` once
 * the invocation ended.
 *
 * @category runtime
 * @since 0.0.0
 */
// No `signal`: the invocation's end interrupts its handler's fiber. Models are
// a service in `R`, and the extensions are static.
export interface TaskRuntime<I, S, R> {
  /** The task this invocation runs. */
  readonly taskId: TaskId<R>;
  /** The task's conversation; `commit` binds to it. */
  readonly conversationId: ConversationId;
  /**
   * Rereads the task on the line, gates the invocation, and runs `change` in one commit bound to the task:
   * a returned state replaces the task's state atomically with the callback's writes; nothing leaves it.
   * `change` is a function of the reread record, or an Effect when it does not need it.
   * `options.persistence` may defer storing a commit that changes only documents (`CommitPersistence`); one
   * that returns a state is stored at once.
   */
  readonly commit: <E>(
    change:
      | ((
          current: RunningTask<I, S, R>,
        ) => Effect.Effect<NextTaskState<S, R> | undefined | void, E, Tx>)
      | Effect.Effect<NextTaskState<S, R> | undefined | void, E, Tx>,
    options?: CommitOptions,
  ) => Effect.Effect<void, E | TaskError | CommitError>;
  /** Commits `Task.next(checkpoint)`, after `change`'s writes in the same commit. */
  readonly next: Transition<[checkpoint: S]>;
  /** Commits `Task.waitFor(on, options)`, after `change`'s writes in the same commit. */
  readonly waitFor: Transition<[on: ReadonlyArray<TaskId>, options: WaitOptions<S>]>;
  /** Commits `Task.complete(result)`, after `change`'s writes in the same commit. */
  readonly complete: Transition<[result: R]>;
  /** Commits `Task.fail(message)`, after `change`'s writes in the same commit. */
  readonly fail: Transition<[message: string]>;
  /** Commits `Task.aborted`. */
  readonly aborted: Effect.Effect<void, TaskError | CommitError>;
  /**
   * `` `${taskId}:${name}` ``: an idempotency key for an external system, stable across reruns. Unique within this
   * Session only: prefix it with the Session's identity when several share the system.
   */
  readonly key: (name: string) => string;
  /**
   * Runs `effect` at most once per task once it succeeded: its result is recorded with `memo` (as
   * `step:<name>`, encoded with `schema`) and returned, decoded, by every later run.
   */
  readonly step: <A, IA extends Schema.Json, E, Req>(
    name: string,
    schema: Schema.Codec<A, IA>,
    effect: Effect.Effect<A, E, Req>,
  ) => Effect.Effect<A, E | TaskError | CommitError | Schema.SchemaError, Req>;
  /** First-writer-wins values kept in the live task record until its outcome is decided. */
  readonly memo: TaskMemo;
  /**
   * Commits `Task.sleepUntil(until, checkpoint)`, after `change`'s writes in the same commit: the
   * phase ends asleep, holding no invocation, and the task resumes at `checkpoint` once the Harness clock
   * reaches `until`. It resolves once the sleep is stored; the phase returns right
   * after. There is no sleep inside a phase: work after the sleep belongs in the next phase.
   */
  readonly sleepUntil: Transition<[until: number, checkpoint: S]>;
  /** Commits `Task.sleep(options)`, after `change`'s writes in the same commit. */
  readonly sleep: Transition<[options: SleepOptions<S>]>;
  /**
   * A signal key's committed state in this task, the value migrated to the signal's version and decoded;
   * `None` while nothing came. Fails with `InvalidSignal` when the stored value cannot be read.
   */
  readonly readSignal: <T>(
    signal: Signal<T>,
    key: string,
  ) => Effect.Effect<Option.Option<SignalState<T>>, InvalidSignal | TaskError>;
  /**
   * Runs `callback` after `delay`, to pace work inside a phase such as progress commits:
   * a fiber of the invocation, which its end interrupts, so a timer never fires after the invocation ended; one
   * armed after the end arms nothing. Interrupt the returned fiber to disarm it earlier. A failure of `callback`
   * is reported.
   */
  readonly setTimer: <E>(
    callback: Effect.Effect<void, E>,
    delay: Duration.Input,
  ) => Effect.Effect<Fiber.Fiber<void>>;
  /** The committed task record. */
  readonly getTask: <T>(
    id: TaskId<T>,
  ) => Effect.Effect<Option.Option<TypedTaskRecord<Schema.Json, Schema.Json, T>>, RuntimeReadError>;
  /**
   * The terminal receipt of a task; rejects when the invocation ends. The invocation stays active work while it
   * waits: to wait long, end the phase waiting on the task instead (`waitFor`).
   */
  readonly awaitTask: <T>(id: TaskId<T>) => Effect.Effect<SettledTask<T>, RuntimeReadError>;
  /**
   * `abortTask` of a task this task owns, resolving once it is terminal. The invocation stays active work while it
   * waits for the abort to land, as with `awaitTask`.
   */
  readonly abortOwned: (id: TaskId) => Effect.Effect<void, RuntimeReadError | CommitError>;
  /** Live tasks this task owns directly, in ID order. */
  readonly ownedTasks: Effect.Effect<ReadonlyArray<TaskRecord>, RuntimeReadError>;
  /** Outcomes of terminal tasks, in order; rejects when one is not terminal. */
  readonly outcomes: <T>(
    ids: ReadonlyArray<TaskId<T>>,
  ) => Effect.Effect<ReadonlyArray<TaskOutcome<T>>, RuntimeReadError>;
  /** A committed entry visible from the task's conversation. */
  readonly entry: TaskEntryRead;
  /** A page of the task's conversation's raw history, by entry-ID range. */
  readonly entries: TaskEntriesRead;
  /** The committed value of one document; never creates. */
  readonly snapshot: DocSnapshot<RuntimeDocumentError>;
  /** A rewindable conversation document as of a visible entry. */
  readonly snapshotAsOf: DocSnapshotAsOf<RuntimeDocumentError>;
  /** A watch stopped when the invocation ends, or when its scope closes. */
  readonly watchDoc: DocWatchAcquire<RuntimeDocumentError>;
  /** An invocation-bound handle of an existing conversation. */
  readonly conversation: (
    id: ConversationId,
  ) => Effect.Effect<Option.Option<ConversationHandle>, RuntimeReadError>;
  /** `conversation(id)`, failing with `ConversationNotFound` when it is absent. */
  readonly requireConversation: (
    id: ConversationId,
  ) => Effect.Effect<ConversationHandle, RuntimeReadError | ConversationNotFound>;
  /**
   * The conversation's agent, resolved at most once per phase handler, at first use, and fixed for the
   * phase. A settings reader that throws fails the resolution with that defect.
   */
  readonly agent: Effect.Effect<Agent, RuntimeReadError>;
  /** The settings resolved over the defaults, read anew at every use. */
  readonly settings: Effect.Effect<Settings, TaskError>;
  /**
   * The task's conversation's environment, built now with `Harness.layer`'s `env`; `undefined` without one.
   * Fails with `ExecutionEnvError` when the builder fails.
   */
  readonly env: Effect.Effect<ExecutionEnv | undefined, ExecutionEnvError | TaskError>;
  /** Runs one hook of this task in every selected extension that declares it. */
  readonly hooks: HookRunner;
  /** The committed active transcript and model context of a conversation, optionally as of an entry. */
  readonly context: (
    conversationId: ConversationId,
    options?: ContextOptions,
  ) => Effect.Effect<ContextView, RuntimeReadError | EntryNotVisible>;
  /** The Harness clock. */
  readonly now: Effect.Effect<number, TaskError>;
  /**
   * Forwards a failure that fails nothing to `HarnessOptions.onReport`: a `task_error` about this task, or,
   * with `from`, an `extension_error` or a `host_error` about whose code failed.
   */
  readonly report: (
    cause: Cause.Cause<unknown>,
    from?: ReportFrom | undefined,
  ) => Effect.Effect<void, TaskError>;
}

/**
 * A runtime transition: one commit of the state its arguments build, with an optional `change` whose writes
 * commit with it.
 *
 * @category runtime
 * @since 0.0.0
 */
export interface Transition<Args extends ReadonlyArray<unknown>> {
  (...args: Args): Effect.Effect<void, TaskError | CommitError>;
  <E>(
    ...args: [...Args, change: Effect.Effect<unknown, E, Tx>]
  ): Effect.Effect<void, E | TaskError | CommitError>;
}

/**
 * Dispatches one hook of a task to every matching handler of the phase's
 * agent, in extension order.
 *
 * @category runtime
 * @since 0.0.0
 */
export interface HookRunner {
  /**
   * Calls `invoke` with each handler named `name`. A failure of `invoke` is reported and the next handler
   * runs; an interruption propagates. Composition happens inside `invoke`.
   */
  readonly each: <F extends HookFunction, E, R>(
    name: string,
    invoke: (handler: F) => Effect.Effect<void, E, R>,
  ) => Effect.Effect<void, RuntimeReadError, R>;
}

/**
 * Options of a context read.
 *
 * @category runtime
 * @since 0.0.0
 */
export interface ContextOptions {
  /** The context as of this visible entry, as a fork at it starts. */
  readonly at?: EntryId | undefined;
}

/**
 * The errors a document read through the runtime adds.
 *
 * @category runtime
 * @since 0.0.0
 */
export type RuntimeDocumentError = ReadError | TaskError;

// -----------------------------------------------------------------------------
// definitions
// -----------------------------------------------------------------------------

/**
 * A phase or abort handler. Any failure, typed or not, faults the task
 * with the error's message.
 *
 * @category definitions
 * @since 0.0.0
 */
export type TaskHandler<I, P, S, R, Req> = (
  task: RunningTask<I, P, R>,
  runtime: TaskRuntime<I, S, R>,
) => Effect.Effect<void, unknown, Req>;

/**
 * The exhaustive, phase-narrowed handler map of a definition.
 *
 * @category definitions
 * @since 0.0.0
 */
export type TaskPhases<I, S extends { readonly phase: string }, R, Req> = {
  readonly [P in S["phase"]]: TaskHandler<I, Extract<S, { readonly phase: P }>, S, R, Req>;
};

/**
 * A task definition.
 *
 * @category definitions
 * @since 0.0.0
 */
// `input`, `checkpoint` and `result` are schemas that type the task and check
// the result of `migrate`; handlers return Effects whose requirements bubble up
// to `Harness.layer`; hooks are attached by extensions (`Hook.make`), not declared by the
// definition.
export interface TaskDefinition<I, S extends { readonly phase: string }, R, Req> {
  /** Task kind, persisted in `TaskRecord.kind`. */
  readonly name: string;
  /** Definition version, persisted with live input and checkpoints. */
  readonly version: number;
  /** JSON schema of the input; its type is its encoding. */
  readonly input: Schema.Codec<I, I>;
  /** JSON schema of every checkpoint; `phase` names the handler that runs it. */
  readonly checkpoint: Schema.Codec<S, S>;
  /** JSON schema of the completed result. */
  readonly result: Schema.Codec<R, R>;
  /** First durable checkpoint of a new task. */
  readonly initial: (input: I) => NoInfer<S>;
  readonly phases: TaskPhases<I, S, R, Req>;
  /** Decides the outcome of an abort-marked task, once its owned work has drained. */
  readonly abort: TaskHandler<I, S, S, R, Req>;
  /** Converts the input and checkpoint stored by any older version; checked against the schemas. */
  readonly migrate?: TaskMigration<I, S> | undefined;
}

/**
 * Converts the input and checkpoint an older version stored; pure.
 *
 * @category definitions
 * @since 0.0.0
 */
export type TaskMigration<I, S> = (
  input: Schema.Json,
  checkpoint: Schema.Json,
  fromVersion: number,
) => { readonly input: I; readonly checkpoint: S };

/**
 * A typed task token.
 *
 * @category definitions
 * @since 0.0.0
 */
export interface Task<I, S extends { readonly phase: string }, R, Req = never> {
  /** Without `phases` and `abort` for a task declared on its own; its handlers are then a Layer. */
  readonly definition: TaskDeclaration<I, S, R> &
    Partial<Pick<TaskDefinition<I, S, R, unknown>, "phases" | "abort">>;
  /**
   * Creates a task of this kind inside a commit: `tx.createTask(task, input, options)`. The new
   * task is pending at its definition's first checkpoint.
   */
  readonly create: (input: I, options: TaskOptions) => Effect.Effect<TaskId<R>, TxWriteError, Tx>;
  /** The services the handlers require; a type-level record only. */
  readonly [RequirementsOf]?: Req;
}

declare const RequirementsOf: unique symbol;

/**
 * Any task, whatever its types.
 *
 * @category definitions
 * @since 0.0.0
 */
export interface Any {
  readonly definition: {
    readonly name: string;
    readonly version: number;
  };
}

/**
 * The services the handlers of a task require.
 *
 * @category definitions
 * @since 0.0.0
 */
export type Requirements<T> = T extends Task<infer _I, infer _S, infer _R, infer Req> ? Req : never;

type Json = Schema.Json;
type Checkpoint = Json & { readonly phase: string };
type HandlerRequirements<H> = H extends (
  ...args: ReadonlyArray<never>
) => Effect.Effect<infer _A, infer _E, infer Req>
  ? Req
  : never;

/**
 * What `Task.make` takes: the definition with its schemas, from which the
 * handlers are typed.
 *
 * @category definitions
 * @since 0.0.0
 */
export type MakeOptions<
  I extends Json,
  S extends Checkpoint,
  R extends Json,
  H extends TaskPhases<I, S, R, unknown>,
  AH extends TaskHandler<I, S, S, R, unknown>,
> = Omit<TaskDefinition<I, S, R, unknown>, "initial" | "phases" | "abort"> & {
  // `NoInfer`, so the checkpoint is inferred from its schema only.
  readonly initial: (input: I) => NoInfer<S>;
  readonly phases: H;
  readonly abort: AH;
};

/**
 * A task's definition without its handlers: its identity, version, schemas, first checkpoint and migration.
 * Enough to create the task from anywhere.
 *
 * @category definitions
 * @since 0.0.0
 */
export type TaskDeclaration<I, S extends { readonly phase: string }, R> = Omit<
  TaskDefinition<I, S, R, unknown>,
  "phases" | "abort"
>;

/**
 * The handlers of a task: what `toLayer` takes.
 *
 * @category definitions
 * @since 0.0.0
 */
export interface Implementation<H, AH> {
  readonly phases: H;
  readonly abort: AH;
}

/**
 * The context key of a declared task's handlers: a declared task requires it, and its `toLayer` provides it
 * (as `effect/workflow`'s `Workflow.toLayer`).
 *
 * @category definitions
 * @since 0.0.0
 */
export interface Handler<Name extends string> {
  readonly _: unique symbol;
  readonly name: Name;
}

/**
 * What a handler Layer provides: the handlers, with the services its Layer was built with. Erased: the
 * scheduler calls each with a record of the task's own kind.
 *
 * @category definitions
 * @since 0.0.0
 */
export interface HandlerService {
  readonly phases: { readonly [phase: string]: ErasedTaskHandler | undefined };
  readonly abort: ErasedTaskHandler;
}

type ErasedTaskHandler = (task: never, runtime: never) => Effect.Effect<void, ErasedFailure>;
type ErasedFailure = { readonly _tag: string } | Error;

/**
 * The context key of the task kind named `name`.
 *
 * @category definitions
 * @since 0.0.0
 */
export const handlerKey = <Name extends string>(
  name: Name,
): Context.Service<Handler<Name>, HandlerService> =>
  Context.Service<Handler<Name>, HandlerService>(
    `@effect-durable-harness/core/Task/Handler/${name}`,
  );

/**
 * A task declared without its handlers; `toLayer` implements it.
 *
 * @category definitions
 * @since 0.0.0
 */
export interface Declared<
  Name extends string,
  I,
  S extends { readonly phase: string },
  R,
> extends Task<I, S, R, Handler<Name>> {
  /**
   * The handlers as a Layer. Given an Effect, it runs once when the Layer is built and its services are
   * captured. The handlers' own requirements become the Layer's.
   */
  readonly toLayer: <
    H extends TaskPhases<I, S, R, unknown>,
    AH extends TaskHandler<I, S, S, R, unknown>,
    RB = never,
  >(
    implementation: Implementation<H, AH> | Effect.Effect<Implementation<H, AH>, never, RB>,
  ) => Layer.Layer<
    Handler<Name>,
    never,
    | Exclude<HandlerRequirements<H[keyof H]> | HandlerRequirements<AH>, Session>
    | Exclude<RB, Scope.Scope>
  >;
}

/**
 * What `Task.make` takes for a task declared without its handlers.
 *
 * @category definitions
 * @since 0.0.0
 */
export type DeclareOptions<
  Name extends string,
  I extends Json,
  S extends Checkpoint,
  R extends Json,
> = Omit<TaskDeclaration<I, S, R>, "initial" | "name"> & {
  readonly name: Name;
  readonly initial: (input: I) => NoInfer<S>;
};

/**
 * What `Task.make(name, options)` takes for a task defined with its handlers.
 *
 * @category definitions
 * @since 0.0.0
 */
export type DefinitionOptions<
  I extends Json,
  S extends Checkpoint,
  R extends Json,
  H extends TaskPhases<I, S, R, unknown>,
  AH extends TaskHandler<I, S, S, R, unknown>,
> = Omit<TaskDefinition<I, S, R, unknown>, "name" | "initial" | "phases" | "abort"> & {
  readonly initial: (input: I) => NoInfer<S>;
  readonly phases: H;
  readonly abort: AH;
};

/**
 * What `Task.make(name, options)` takes for a task declared without its handlers.
 *
 * @category definitions
 * @since 0.0.0
 */
export type DeclarationOptions<I extends Json, S extends Checkpoint, R extends Json> = Omit<
  TaskDeclaration<I, S, R>,
  "initial" | "name"
> & {
  readonly initial: (input: I) => NoInfer<S>;
};

/** What `make` takes, whichever overload: a declaration, with or without its handlers. */
interface MakeInput {
  readonly name: string;
  readonly version: number;
  readonly phases?: unknown;
}

/**
 * Defines a task kind: `Task.make(name, options)`, or the earlier object form
 * with `name` inside. The schemas type the handlers; the handlers'
 * requirements, but the `Session` the Harness provides, become the task's.
 * Versions are positive integers.
 *
 * Without `phases` and `abort` it is a declaration: `task.toLayer({ phases, abort })` implements it as a
 * Layer, and every Harness whose extensions include it requires that Layer.
 *
 * @category constructors
 * @since 0.0.0
 */
export function make<
  I extends Json,
  const S extends Checkpoint,
  R extends Json,
  H extends TaskPhases<I, S, R, unknown>,
  AH extends TaskHandler<I, S, S, R, unknown>,
>(
  name: string,
  options: DefinitionOptions<I, S, R, H, AH>,
): Task<I, S, R, Exclude<HandlerRequirements<H[keyof H]> | HandlerRequirements<AH>, Session>>;
export function make<
  const Name extends string,
  I extends Json,
  const S extends Checkpoint,
  R extends Json,
>(name: Name, options: DeclarationOptions<I, S, R>): Declared<Name, I, S, R>;
export function make<
  I extends Json,
  const S extends Checkpoint,
  R extends Json,
  H extends TaskPhases<I, S, R, unknown>,
  AH extends TaskHandler<I, S, S, R, unknown>,
>(
  definition: MakeOptions<I, S, R, H, AH>,
): Task<I, S, R, Exclude<HandlerRequirements<H[keyof H]> | HandlerRequirements<AH>, Session>>;
export function make<
  const Name extends string,
  I extends Json,
  const S extends Checkpoint,
  R extends Json,
>(definition: DeclareOptions<Name, I, S, R>): Declared<Name, I, S, R>;
export function make(first: string | MakeInput, options?: Omit<MakeInput, "name">): Any {
  // `make(name, options)`; the object form, with `name` inside, is the earlier shape.
  const definition: MakeInput = Predicate.isString(first) ? withName(first, options) : first;
  if (!Number.isInteger(definition.version) || definition.version < 1) {
    throw new TypeError(`Task ${definition.name} version must be a positive integer`);
  }
  const create = (
    input: Json,
    taskOptions: TaskOptions,
  ): Effect.Effect<TaskId<Json>, TxWriteError, Tx> =>
    Effect.gen(function* () {
      const tx = yield* Tx;
      // SAFETY: the token of this definition; the overloads type `create` and `toLayer` from it, and this
      // token is their erasure.
      return yield* tx.createTask(token as Task<Json, Checkpoint, Json>, input, taskOptions);
    });
  // `phases` is the only field the overloads' shapes differ in.
  const token =
    definition.phases === undefined
      ? { definition, create, toLayer: declaredLayer(definition.name) }
      : { definition, create };
  return token;
}

/** The object form of `make(name, options)`; the overloads always pass options with a name. */
const withName = (name: string, options: Omit<MakeInput, "name"> | undefined): MakeInput => {
  // A definition is a module constant: failing here is broken setup.
  if (options === undefined) throw new TypeError(`Task ${name} has no options`);
  return { ...options, name };
};

/** A declared task's `toLayer`, erased. */
const declaredLayer =
  (name: string) =>
  (implementation: HandlerService | Effect.Effect<HandlerService>): Layer.Layer<Handler<string>> =>
    Layer.effect(
      handlerKey(name),
      Effect.gen(function* () {
        const handlers = Effect.isEffect(implementation) ? yield* implementation : implementation;
        return taskHandler(handlers, yield* capturedServices);
      }),
    );

/**
 * Who owns a task: its conversation (a top-level task) or another task of the
 * same conversation (a child task).
 *
 * @category models
 * @since 0.0.0
 */
export type TaskOwnership =
  | { readonly kind: "conversation" }
  | { readonly kind: "task"; readonly taskId: TaskId };

/**
 * Options of `tx.createTask`.
 *
 * @category models
 * @since 0.0.0
 */
export interface TaskOptions {
  /** Required: a task always names its owner. */
  readonly ownership: TaskOwnership;
  /** Default: the owner task's conversation, or the commit's bound conversation. */
  readonly conversationId?: ConversationId | undefined;
  /** Conversation-owned tasks only: excluded from idle waits, conversation aborts and cascades. */
  readonly background?: boolean | undefined;
  /** Abort-marked with reason `restart` when a later Session starts scheduling. */
  readonly abandonOnRestart?: boolean | undefined;
  /**
   * The trace the task's phases continue. Omitted: the span that creates it, so
   * its phases are children of the commit's caller. `"root"`: each phase starts a trace of its own, and no carrier
   * is stored.
   */
  readonly trace?: TraceCarrier | "root" | undefined;
  /** Immutable creation-time links, attached to the phase spans of the task's first invocation. */
  readonly links?: ReadonlyArray<TraceCarrier> | undefined;
}

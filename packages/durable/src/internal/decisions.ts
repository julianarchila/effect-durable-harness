// The scheduler's decisions: what a pass, a step or an abort writes, decided
// from the committed state.
//
// Each operation of the scheduler runs in three parts inside its commit:
// load (every storage read the decision needs), decide (one of these
// functions, in one synchronous step) and apply (the writes of the plan, in
// order, with no further decision). These functions run no Effect, so no other
// fiber can change what they read while they decide. They read the
// ownership index and the definitions, and return a plan; they change nothing.
// Because the decisions are pure functions the scheduler applies, a yield
// cannot fall between deciding and registering what was decided.
import * as Cause from "effect/Cause";
import type * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Predicate from "effect/Predicate";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import type { BlockedReason } from "../Harness.ts";
import type { ConversationId, TaskId } from "../Ids.ts";
import type { LiveTaskRecord, TaskOutcome, TaskRecord, TaskState, WakeReason } from "../Records.ts";
import type * as Task from "../Task.ts";
import { copyJson } from "./documents.ts";
import * as SignalMemos from "./signalMemos.ts";
import type { Sleeping } from "./signalMemos.ts";
import {
  conversationNode,
  failedOutcome,
  type OwnershipIndex,
  type Overlay,
  parentOf,
} from "./ownership.ts";

export type Json = Schema.Json;

export type RunningRecord = LiveTaskRecord & {
  readonly state: { readonly status: "running"; readonly checkpoint: Schema.Json };
};

/** Terminal outcomes the scheduler writes without running task code. */
export type SchedulerOutcome = Extract<TaskOutcome, { readonly status: "faulted" | "orphaned" }>;

/** The failure of a handler, whatever its error type. */
export type Failure = { readonly _tag: string } | Error;

export type ErasedRuntime = Task.TaskRuntime<Json, Json, Json>;
export type ErasedHandler = (
  task: RunningRecord,
  runtime: ErasedRuntime,
) => Effect.Effect<void, Failure>;

/** What the scheduler calls on an erased definition. */
export interface ErasedDefinition {
  readonly name: string;
  readonly version: number;
  readonly input: Schema.Codec<Json, Json>;
  readonly checkpoint: Schema.Codec<Json, Json>;
  /** Absent for a task declared without its handlers: its handler Layer provides them. */
  readonly phases?: { readonly [phase: string]: ErasedHandler | undefined } | undefined;
  readonly abort?: ErasedHandler | undefined;
  readonly migrate?: Task.TaskMigration<Json, Json> | undefined;
}

/** The static registry, and the definition whose migration failed per task. */
export interface Definitions {
  readonly tasks: ReadonlyMap<string, Task.Any>;
  readonly failedMigrations: ReadonlyMap<
    TaskId,
    { readonly task: Task.Any; readonly message: string }
  >;
  /**
   * Whether `resolve` runs a stored version's `migrate`. A decision region that may run again probes with it off,
   * so a migration, task code, runs once, in the plan made after the region's keys were clean.
   */
  readonly migrations?: boolean | undefined;
}

/** A definition that can take a record, or why none can; deciding it runs no task code. */
export type Fit =
  | { readonly task: Task.Any; readonly migrates: boolean }
  | { readonly reason: BlockedReason; readonly message?: string };

/** A migration that failed while resolving; the scheduler records and reports it. */
export interface MigrationFailure {
  readonly id: TaskId;
  readonly conversationId: ConversationId;
  readonly task: Task.Any;
  readonly cause: Cause.Cause<unknown>;
}

export type Resolution =
  | { readonly kind: "ready"; readonly task: Task.Any; readonly record: LiveTaskRecord }
  | {
      readonly kind: "blocked";
      readonly reason: BlockedReason;
      readonly failure?: MigrationFailure | undefined;
    };

/** Outcome of the phase that just returned, judged by the next step. */
export interface PhaseResult {
  readonly checkpoint: Json;
  readonly failure?: Cause.Cause<unknown> | undefined;
}

/**
 * Step decision: continue with the next phase, end the invocation, or end it by writing `faulted`. `End`
 * with `cascade` also asks for a cascade pass.
 */
export type Decision =
  | { readonly _tag: "Continue" }
  | { readonly _tag: "End"; readonly cascade: boolean }
  | {
      readonly _tag: "Fault";
      readonly message: string;
      /** The failure that faulted the task, reported as `task_faulted`; absent: a fault the scheduler names. */
      readonly cause?: Cause.Cause<unknown> | undefined;
    };

export const Continue: Decision = { _tag: "Continue" };
export const End: Decision = { _tag: "End", cascade: false };

/** One write of a plan, applied in order: a task record, or the Harness cleanup of a scheduler outcome. */
export type Action =
  | { readonly _tag: "Write"; readonly record: TaskRecord }
  | { readonly _tag: "Settle"; readonly record: TaskRecord; readonly outcome: SchedulerOutcome };

// ─── Definitions ────────────────────────────────────────────────────────────

export const fitOf = (
  definitions: Definitions,
  record: Pick<TaskRecord, "id" | "kind" | "version">,
): Fit => {
  const task = definitions.tasks.get(record.kind);
  if (task === undefined) return { reason: "missing_task" };
  const version = task.definition.version;
  if (version === record.version) return { task, migrates: false };
  if (version < record.version) return { reason: "task_too_old" };
  const failed = definitions.failedMigrations.get(record.id);
  if (failed?.task === task) return { reason: "migration_failed", message: failed.message };
  return { task, migrates: true };
};

/** What `blockedReason` reads of a task; the task graph keeps only this. */
export interface BlockedSubject {
  readonly id: TaskId;
  readonly kind: string;
  readonly version: number;
  readonly abortRequested: boolean;
  readonly abortReason?: "restart" | undefined;
  readonly status: TaskRecord["state"]["status"];
}

/** The `BlockedSubject` of `record`. */
export const blockedSubject = (record: TaskRecord): BlockedSubject => ({
  id: record.id,
  kind: record.kind,
  version: record.version,
  abortRequested: record.abortRequested,
  abortReason: record.abortReason,
  status: record.state.status,
});

/**
 * Why a live task cannot run here, if it cannot: its definition is
 * missing, too old, or failed to migrate. A running task, an abort-marked one and one holding its outcome are never
 * blocked; a `restart` mark waits for its definition, blocked.
 */
export const blockedReason = (
  definitions: Definitions,
  subject: BlockedSubject,
): BlockedReason | undefined => {
  const status = subject.status;
  if (status === "running" || status === "completing" || status === "terminal") return undefined;
  if (subject.abortRequested && subject.abortReason === undefined) return undefined;
  const fit = fitOf(definitions, subject);
  if ("reason" in fit) return fit.reason;
  return fit.migrates && erased(fit.task).migrate === undefined ? "migration_failed" : undefined;
};

/** Resolves the record's definition by kind, migrating an older stored version. */
export const resolve = (definitions: Definitions, record: LiveTaskRecord): Resolution => {
  const fit = fitOf(definitions, record);
  if ("reason" in fit) return { kind: "blocked", reason: fit.reason };
  if (!fit.migrates || definitions.migrations === false) {
    return { kind: "ready", task: fit.task, record };
  }
  const migrated = migrateRecord(record, erased(fit.task));
  if (Result.isFailure(migrated)) {
    return {
      kind: "blocked",
      reason: "migration_failed",
      failure: {
        id: record.id,
        conversationId: record.conversationId,
        task: fit.task,
        cause: migrated.failure,
      },
    };
  }
  return { kind: "ready", task: fit.task, record: migrated.success };
};

// ─── Steps and outcomes ─────────────────────────────────────────────────────

/**
 * Live tasks a task waits for before its next invocation: its live ordinary owned work when
 * abort-marked, since abort runs bottom-up, otherwise the live part of the `on` of a wait; none for an `any`
 * wait one of whose tasks ended.
 */
export const waitingOn = (
  index: OwnershipIndex,
  record: TaskRecord,
  first = false,
): ReadonlyArray<TaskId> => {
  if (record.abortRequested) return index.ownedLive(record.id, undefined, first);
  if (record.state.status !== "waiting") return [];
  const on = record.state.on;
  if (record.state.policy === "any" && !on.every((id) => index.live.has(id))) return [];
  if (!first) return on.filter((id) => index.live.has(id));
  const waited = on.find((id) => index.live.has(id));
  return waited === undefined ? [] : [waited];
};

/**
 * Precedence rules for a run invocation, on the line. Rules 1 (terminal, `completing` or `waiting`) and
 * 2 (closing) are applied by the step. Decides whether the invocation continues with the next phase.
 */
export const decideStep = (
  index: OwnershipIndex,
  current: RunningRecord,
  previous: PhaseResult | undefined,
): Decision => {
  // 3. abort mark: end; a fresh abort invocation starts once the task's ordinary owned work is gone.
  if (current.abortRequested) return End;
  // An owner's cancellation intent ends it too, before its cascade marks it; reservation then holds it.
  if (!current.background && index.belowCancelled(parentOf(current))) {
    return { _tag: "End", cascade: true };
  }
  if (previous === undefined) return Continue;
  // 4. uncaught error.
  if (previous.failure !== undefined) {
    return { _tag: "Fault", message: causeMessage(previous.failure), cause: previous.failure };
  }
  // 6. no durable progress.
  if (jsonEqual(current.state.checkpoint, previous.checkpoint)) {
    return {
      _tag: "Fault",
      message: `Task ${current.kind} phase ${String(phaseOf(previous.checkpoint))} returned without durable progress`,
    };
  }
  // 5. progress. With a static registry the definition never changes, so there is no handover and no
  // snapshot to refresh.
  return Continue;
};

/**
 * Writes an outcome the scheduler decided. While the task's ordinary owned work is live, judged on the
 * overlay, it holds as `completing` and its Harness cleanup waits for the final commit.
 */
export const outcomeActions = (
  index: OwnershipIndex,
  record: TaskRecord,
  outcome: SchedulerOutcome,
  overlay: Overlay,
): Array<Action> => {
  if (index.hasOwnedLive(record.id, overlay)) {
    return [{ _tag: "Write", record: withState(record, { status: "completing", outcome }) }];
  }
  return [
    { _tag: "Write", record: withState(record, { status: "terminal", outcome }) },
    { _tag: "Settle", record, outcome },
  ];
};

// ─── Reservation ────────────────────────────────────────────────────────────

/** A task to start, in run or abort mode, with the definition that takes it. */
export interface Start {
  readonly record: TaskRecord;
  readonly mode: "run" | "abort";
  readonly task: Task.Any;
}

export interface ReservePlan {
  readonly actions: ReadonlyArray<Action>;
  readonly start: ReadonlyArray<Start>;
  /** Candidates done with until something changes: started, held, waiting or gone. */
  readonly dropped: ReadonlyArray<TaskId>;
  /** Work below an owner with cancellation intent waits for its cascade, which may not have run yet. */
  readonly cascade: boolean;
  readonly failures: ReadonlyArray<MigrationFailure>;
  /** Tasks the plan faults for the restart limit, with the message the scheduler reports them with. */
  readonly cutOff: ReadonlyArray<{ readonly record: TaskRecord; readonly message: string }>;
}

/** What a reservation pass reads besides the index and the definitions. */
export interface ReserveInputs {
  readonly invoked: (id: TaskId) => boolean;
  readonly candidates: ReadonlyArray<TaskId>;
  /** The Harness clock, for sleeps. */
  readonly now: number;
  /** `settings.restartLimit`, read only for a task that was cut off at all. */
  readonly restartLimit: () => number;
  /** Whether a kind's run phases end the task themselves at the limit: the built-in tasks. */
  readonly limitsRestarts: (kind: string) => boolean;
}

/**
 * Reserves every eligible candidate; orphans abort-marked tasks no definition can take, unless abandoned
 * after a restart; faults a task cut off `restartLimit` times whose kind does not end itself, and any task
 * whose abort handler was. An orphan's or fault's outcome is judged with the plan's earlier writes staged on
 * the overlay.
 */
export const planReserve = (
  index: OwnershipIndex,
  definitions: Definitions,
  inputs: ReserveInputs,
  staged: Overlay,
): ReservePlan => {
  const invoked = inputs.invoked;
  const overlay = copyOverlay(staged);
  const actions: Array<Action> = [];
  const start: Array<Start> = [];
  const dropped: Array<TaskId> = [];
  const failures: Array<MigrationFailure> = [];
  const cutOff: Array<{ readonly record: TaskRecord; readonly message: string }> = [];
  let cascade = false;
  const stage = (planned: ReadonlyArray<Action>) => {
    for (const action of planned) {
      actions.push(action);
      if (action._tag === "Write") overlay.tasks.set(action.record.id, action.record);
    }
  };
  for (const id of inputs.candidates) {
    const record = index.live.get(id);
    if (
      record === undefined ||
      record.state.status === "completing" ||
      record.state.status === "terminal" ||
      invoked(id)
    ) {
      dropped.push(id);
      continue;
    }
    // It returns once what it waits for ends, its record changes, or the work below it ends; a sleep at the
    // first pass after its time comes or a delivery under one of its keys.
    if (waitingOn(index, record, true).length > 0 || asleep(index, record, inputs.now)) {
      dropped.push(id);
      continue;
    }
    const mode = record.abortRequested ? "abort" : "run";
    // Work below an owner with cancellation intent waits for its cascade mark instead of running a phase.
    if (mode === "run" && !record.background && index.belowCancelled(parentOf(record))) {
      cascade = true;
      continue;
    }
    // Work cut off `restartLimit` times in a row would run forever: an
    // abort handler, or a phase of a kind that does not end itself at the limit, ends the task `faulted`.
    if (
      record.restarts !== undefined &&
      (mode === "abort" || !inputs.limitsRestarts(record.kind)) &&
      record.restarts >= inputs.restartLimit()
    ) {
      const message =
        mode === "abort"
          ? `Abort handler of task ${id} (${record.kind}) was cut off ${record.restarts} times`
          : `Task ${id} (${record.kind}) was cut off ${record.restarts} times without progress`;
      stage(
        outcomeActions(
          index,
          record,
          { status: "faulted", error: { message, detail: { reason: "restart_limit" } } },
          overlay,
        ),
      );
      cutOff.push({ record, message });
      dropped.push(id);
      continue;
    }
    // SAFETY: neither terminal nor completing, as checked above.
    const resolution = resolve(definitions, record as LiveTaskRecord);
    if (resolution.kind === "blocked") {
      if (resolution.failure !== undefined) failures.push(resolution.failure);
      // An abandoned task waits for its definition, so its abort handler can clean up.
      if (mode === "abort" && record.abortReason === undefined) {
        stage(
          outcomeActions(index, record, { status: "orphaned", reason: resolution.reason }, overlay),
        );
      }
      continue;
    }
    if (resolution.record !== record || record.state.status !== "running") {
      const woke = mode === "run" ? wokeOf(index, record, inputs.now) : undefined;
      stage([{ _tag: "Write", record: reserved(resolution.record, woke) }]);
    }
    dropped.push(id);
    start.push({ record, mode, task: resolution.task });
  }
  return { actions, start, dropped, cascade, failures, cutOff };
};

/** Whether a failure's detail says crashes ended the task, and why. */
export const interruptionOf = (
  detail: Json | undefined,
): "restart_limit" | "interrupted" | undefined => {
  if (
    !Predicate.isObject(detail) ||
    Array.isArray(detail) ||
    !Predicate.hasProperty(detail, "reason")
  ) {
    return undefined;
  }
  const reason = detail.reason;
  return reason === "restart_limit" || reason === "interrupted" ? reason : undefined;
};

/**
 * Terminal states a task returned because crashes ended
 * it although their outcome does not say so, with the message the scheduler reports them `task_interrupted`
 * with. Keyed by the state object the task returns, so nothing is added to the stored state.
 */
const endedByCrashesStates = new WeakMap<Task.NextTaskState<Json, Json>, string>();

/** `state`, marked as ending its task for crashes (`task_interrupted` with `message`), whatever its outcome. */
export const endedByCrashes = <T extends Task.NextTaskState<Json, Json>>(
  state: T,
  message: string,
): T => {
  endedByCrashesStates.set(state, message);
  return state;
};

/** The `task_interrupted` message `endedByCrashes` marked `state` with, if any. */
export const endedByCrashesMessage = (state: Task.NextTaskState<Json, Json>): string | undefined =>
  endedByCrashesStates.get(state);

/**
 * A sleeping record that cannot wake yet: no abort mark, no gate wake, no listed key with a value no wake used,
 * `until` not reached.
 */
export const asleep = (
  index: OwnershipIndex,
  record: TaskRecord,
  now: number,
): record is TaskRecord & {
  readonly state: Sleeping;
} =>
  record.state.status === "sleeping" &&
  !record.abortRequested &&
  !index.gateWakes.has(record.id) &&
  !SignalMemos.sleepDue(record.state, SignalMemos.memosOf(record), now);

/**
 * Why a `sleeping` state is refused, if it is: its `until` is not a time in milliseconds on the Harness clock,
 * it waits for nothing, one of its keys already served a wait or expired, or an abort handler sleeps.
 */
export const invalidSleep = (
  mode: "run" | "abort",
  current: TaskRecord,
  next: Sleeping,
): string | undefined => {
  if (mode === "abort") return `Abort handler of task ${current.id} cannot sleep`;
  const until = next.until;
  if (until !== undefined && (!Number.isFinite(until) || until < 0)) {
    return `Task ${current.id} cannot sleep until ${until}: not a time in milliseconds`;
  }
  return SignalMemos.invalidSleep(next, SignalMemos.memosOf(current), current.id);
};

/** How a run-mode reservation ends a wait or a sleep: its wake, and the signal memos it marks. */
export interface Woke {
  readonly wake: WakeReason;
  readonly memos: Readonly<Record<string, Json>>;
}

/**
 * How a run-mode reservation ends a record's wait or sleep: a wait's wake names the terminal tasks of its
 * `on` (all of them unless `any`); a sleep's, the key that woke it, consumed, or its deadline, with the keys
 * that hold nothing expired. None for a record that neither waits nor sleeps.
 */
export const wokeOf = (
  index: OwnershipIndex,
  record: TaskRecord,
  now: number,
): Woke | undefined => {
  const state = record.state;
  if (state.status === "waiting") {
    const ended = state.policy === "any" ? state.on.filter((id) => !index.live.has(id)) : state.on;
    return { wake: { reason: "tasks", ended }, memos: {} };
  }
  return state.status === "sleeping"
    ? SignalMemos.sleepWake(state, SignalMemos.memosOf(record), now)
    : undefined;
};

/**
 * The record a reservation writes: `running` at the checkpoint, with the wake that ends its wait or sleep and
 * the signal memos it marks; a task written back as `pending` keeps the wake it woke with.
 */
export const reserved = (record: TaskRecord, woke: Woke | undefined): TaskRecord => {
  const state = record.state;
  if (state.status === "terminal" || state.status === "completing") return record;
  if (woke !== undefined) {
    return withState(SignalMemos.withMemos(record, woke.memos), {
      status: "running",
      checkpoint: state.checkpoint,
      wake: woke.wake,
    });
  }
  if (state.status === "pending" || state.status === "running") {
    return withState(record, { ...state, status: "running" });
  }
  return withState(record, { status: "running", checkpoint: state.checkpoint });
};

/** The record written back as `pending` (open, close): its checkpoint, and the wake it woke with, if any. */
export const pendingAgain = (record: TaskRecord): TaskRecord => {
  const state = record.state;
  return state.status === "running" ? withState(record, { ...state, status: "pending" }) : record;
};

/**
 * The record a task's own commit of `next` replaces `current` with: a state
 * that differs from the current one, its wake left out, is progress, so the restart count starts over; the
 * same checkpoint committed again, for example as intent before an effect, is not, and keeps the wake.
 */
export const committedState = (current: TaskRecord, next: TaskState): TaskRecord => {
  // SAFETY: only `pending` and `running` states carry a wake; the others have no such key to drop.
  const { wake: _wake, ...was } = current.state as TaskState & { readonly wake?: WakeReason };
  const same = jsonEqual(was, stateJson(next));
  return withState(same ? current : progressed(current), same ? current.state : next);
};

/** The record without its restart count, as after a state the task committed. */
export const progressed = (record: TaskRecord): TaskRecord => {
  if (record.restarts === undefined) return record;
  const { restarts: _restarts, ...rest } = record;
  return rest;
};

// ─── Reconcile ──────────────────────────────────────────────────────────────

export interface ReconcilePlan {
  /** Abort marks to write, in order. */
  readonly marks: ReadonlyArray<TaskRecord>;
  /** Conversations whose queued inputs are withdrawn. */
  readonly withdraw: ReadonlyArray<ConversationId>;
}

/**
 * Abort marks below live owners with cancellation intent and `failFast` marks, and the conversations with queued inputs below a cancelled owner. `failed` are the `failFast`
 * waiters a member of whose `on` failed, in check order.
 */
export const planReconcile = (
  index: OwnershipIndex,
  failed: ReadonlyArray<TaskRecord>,
  queued: ReadonlyArray<ConversationId>,
): ReconcilePlan => {
  // A cascade from an abandoned owner passes its `restart` reason on; any other intent marks, or upgrades
  // a `restart` mark, as a request. Collected first, so a request wins over a `restart` mark of the same
  // pass.
  const marks = new Map<
    TaskId,
    { readonly record: TaskRecord; readonly reason: "restart" | undefined }
  >();
  const mark = (record: TaskRecord, reason: "restart" | undefined): void => {
    const staged = marks.get(record.id);
    if (staged !== undefined && staged.reason === undefined) return;
    if (record.abortRequested && (record.abortReason === undefined || reason !== undefined)) return;
    marks.set(record.id, { record, reason });
  };
  // Every live foreground task below a live owner with cancellation intent, which is its nearest one: a
  // walk down from each owner with intent does not enter another. Derived again on every pass, as loading
  // edges can reveal a cancelled owner.
  for (const id of index.intent) {
    const owner = index.live.get(id);
    if (owner === undefined) continue;
    // Only an owner abandoned after a restart, and nothing else, passes its reason on.
    const restart = owner.abortReason === "restart" && !failedOutcome(owner);
    for (const walked of index.walkDown(id, { enter: (next) => !index.intent.has(next) })) {
      if (walked.live !== undefined && !walked.node.background) {
        mark(walked.live, restart ? "restart" : undefined);
      }
    }
  }
  for (const waiter of failed) {
    if (waiter.state.status !== "waiting") continue;
    // Every other live task: the failed one keeps its own outcome.
    for (const member of waiter.state.on) {
      const record = index.live.get(member);
      if (record !== undefined && !failedOutcome(record)) mark(record, undefined);
    }
  }
  return {
    marks: Array.from(marks.values(), ({ record, reason }) => withAbortMark(record, reason)),
    withdraw: queued.filter((id) => index.belowCancelled({ conversation: id })),
  };
};

/**
 * The terminal record of every candidate `completing` task without live ordinary owned work. Finalizing
 * one can free its owner, so this repeats, in rounds, until nothing changes, judging each round with the
 * earlier ones staged on the overlay. A held scheduler outcome gets its Harness cleanup.
 */
export const planFinalize = (
  index: OwnershipIndex,
  candidates: ReadonlyArray<TaskId>,
  staged: Overlay,
): Array<Action> => {
  const overlay = copyOverlay(staged);
  const actions: Array<Action> = [];
  const done = new Set<TaskId>();
  let round = candidates;
  while (round.length > 0) {
    const next = new Set<TaskId>();
    for (const id of round) {
      const record = overlay.tasks.get(id) ?? index.live.get(id);
      if (record?.state.status !== "completing" || done.has(id)) continue;
      if (index.hasOwnedLive(id, overlay, done)) continue;
      const outcome = record.state.outcome;
      const terminal = withState(record, { status: "terminal", outcome });
      actions.push({ _tag: "Write", record: terminal });
      overlay.tasks.set(id, terminal);
      done.add(id);
      // Only the scheduler writes `faulted` and `orphaned`; their cleanup waits for this commit.
      if (outcome.status === "faulted" || outcome.status === "orphaned") {
        actions.push({ _tag: "Settle", record, outcome });
      }
      const above = index.nearestLiveAbove(record, overlay);
      if (above?.state.status === "completing") next.add(above.id);
    }
    round = Array.from(next);
  }
  return actions;
};

// ─── Waits ──────────────────────────────────────────────────────────────────

/**
 * The path from a member of `on` back to `task` in the wait-for graph, if the wait would close a cycle.
 * An edge X -> Y means X cannot settle before Y: Y is in the `on` of waiting X that is not abort-marked,
 * or Y is live ordinary owned work of X. Each task is visited once. Walks down need the chains of every
 * live task loaded.
 */
export const waitCycle = (
  index: OwnershipIndex,
  task: TaskId,
  on: ReadonlyArray<TaskId>,
  overlay: Overlay,
): Array<TaskId> | undefined => {
  const from = new Map<TaskId, TaskId | undefined>();
  const walked = new Set<TaskId>();
  const pending: Array<TaskId> = [];
  const reach = (id: TaskId, via: TaskId | undefined) => {
    if (from.has(id)) return;
    from.set(id, via);
    pending.push(id);
  };
  const path = (end: TaskId): Array<TaskId> => {
    const steps: Array<TaskId> = [];
    for (let at: TaskId | undefined = end; at !== undefined; at = from.get(at)) steps.unshift(at);
    return steps;
  };
  for (const id of on) reach(id, undefined);
  for (let id = pending.pop(); id !== undefined; id = pending.pop()) {
    if (id === task) return path(id);
    const record = overlay.tasks.get(id) ?? index.live.get(id);
    if (record === undefined || record.state.status === "terminal") continue;
    // An abort-marked task waits only for its owned work, as abort runs bottom-up (`waitingOn`).
    if (record.state.status === "waiting" && !record.abortRequested) {
      for (const member of record.state.on) reach(member, id);
    }
    if (walked.has(id)) continue;
    walked.add(id);
    // Below a walked task the walk does not go again: its own walk covered what lies below.
    const found: Array<TaskId> = [];
    for (const next of index.walkDown(id, { overlay, enter: (inner) => !walked.has(inner) })) {
      if (next.live === undefined || next.node.background) continue;
      found.push(next.id);
      reach(next.id, id);
    }
    for (const next of found) walked.add(next);
  }
  return undefined;
};

// ─── Aborts ─────────────────────────────────────────────────────────────────

/** Whether `planAbort` may orphan the task, which needs the chains of the live tasks loaded. */
export const abortNeedsScopes = (
  current: TaskRecord,
  invoked: boolean,
  keepRestart: boolean,
): boolean =>
  current.state.status !== "terminal" &&
  !(keepRestart && current.abortReason === "restart") &&
  !invoked &&
  current.state.status !== "completing";

export type AbortPlan =
  | { readonly _tag: "AlreadyTerminal" }
  | {
      readonly _tag: "Marked";
      readonly actions: ReadonlyArray<Action>;
      readonly failure?: MigrationFailure | undefined;
      /** Whether the caller joins the task's run invocation. */
      readonly joinRun: boolean;
    };

/**
 * Commits the abort mark, or settles a task that no registered definition can take as `orphaned` when
 * nothing it owns is live. A request replaces a `restart` mark; with `keepRestart`, as for an owner's own
 * cleanup, such a task keeps its mark.
 */
export const planAbort = (
  index: OwnershipIndex,
  definitions: Definitions,
  current: TaskRecord,
  invocation: "run" | "abort" | undefined,
  keepRestart: boolean,
  overlay: Overlay,
): AbortPlan => {
  if (current.state.status === "terminal") return { _tag: "AlreadyTerminal" };
  // A restart-marked task has no run invocation; it only waits for its abort.
  if (keepRestart && current.abortReason === "restart") {
    return { _tag: "Marked", actions: [], joinRun: false };
  }
  if (abortNeedsScopes(current, invocation !== undefined, keepRestart)) {
    if (!index.hasOwnedLive(current.id)) {
      // SAFETY: neither terminal nor completing, so a live record that can still run.
      const resolution = resolve(definitions, current as LiveTaskRecord);
      if (resolution.kind === "blocked") {
        return {
          _tag: "Marked",
          actions: outcomeActions(
            index,
            current,
            { status: "orphaned", reason: resolution.reason },
            overlay,
          ),
          failure: resolution.failure,
          joinRun: false,
        };
      }
    }
  }
  const actions: Array<Action> =
    !current.abortRequested || current.abortReason !== undefined
      ? [{ _tag: "Write", record: withAbortMark(current) }]
      : [];
  return { _tag: "Marked", actions, joinRun: invocation === "run" };
};

export interface ConversationAbortPlan {
  /** Abort marks to write, in walk order. */
  readonly marks: ReadonlyArray<TaskRecord>;
  /** Every live task the traversal reached. */
  readonly reached: ReadonlyArray<TaskId>;
  /** Conversations whose queued inputs are withdrawn. */
  readonly withdraw: ReadonlyArray<ConversationId>;
}

/**
 * `Conversation.abort`: marks every live non-background task that ordinary traversal from the
 * conversation reaches, and withdraws the queued inputs in its scope. With `background`, traversal crosses
 * background boundaries.
 */
export const planConversationAbort = (
  index: OwnershipIndex,
  conversationId: ConversationId,
  background: boolean,
  queued: ReadonlyArray<ConversationId>,
): ConversationAbortPlan => {
  const marks: Array<TaskRecord> = [];
  const reached: Array<TaskId> = [];
  const walk = index.walkDown(conversationNode(conversationId), { crossBackground: background });
  for (const walked of walk) {
    if (walked.live === undefined || (walked.node.background && !background)) continue;
    reached.push(walked.live.id);
    if (!walked.live.abortRequested || walked.live.abortReason !== undefined) {
      marks.push(withAbortMark(walked.live));
    }
  }
  const scope = { conversation: conversationId };
  return {
    marks,
    reached,
    withdraw: queued.filter(
      (id) => index.inScope({ conversation: id }, scope, background) === true,
    ),
  };
};

// ─── Records ────────────────────────────────────────────────────────────────

export const erased = (task: Task.Any): ErasedDefinition =>
  // SAFETY: every task token is built by `Task.make` from a full definition; the scheduler calls it only
  // with records of its own kind.
  task.definition as ErasedDefinition;

export const missingMigration = (record: TaskRecord, definition: ErasedDefinition) =>
  `Task ${record.kind} version ${definition.version} has no migration from ${record.version}`;

// An interruption with no abort mark and no close (the handler interrupted itself) faults the task.
/**
 * Whether a failure is the stop of its invocation, its abort mark, its end or close, rather than its own: such a
 * failure propagates instead of being reported.
 */
export const stoppedBy = (cause: Cause.Cause<unknown>): boolean => {
  if (Cause.hasInterruptsOnly(cause)) return true;
  const error = Option.getOrUndefined(Cause.findErrorOption(cause));
  if (Predicate.isTagged(error, "SessionClosed")) return true;
  return (
    Predicate.isTagged(error, "TaskError") &&
    Predicate.hasProperty(error, "reason") &&
    (Predicate.isTagged(error.reason, "AbortMarked") ||
      Predicate.isTagged(error.reason, "InvocationEnded"))
  );
};

/** An Error's message, or the value as a string. */
export const causeMessage = (cause: Cause.Cause<unknown>): string => {
  if (Cause.hasInterruptsOnly(cause)) return "Task invocation was interrupted";
  const error = Cause.squash(cause);
  if (error instanceof Error) return error.message;
  const message = Result.try({
    try: () => String(error),
    catch: () => Object.prototype.toString.call(error),
  });
  return Result.isSuccess(message) ? message.success : message.failure;
};

/**
 * The record with an abort mark: `restart` for an abandonment; without a reason, a request. The restart count
 * starts over: it now counts cut-offs of the abort handler.
 */
export const withAbortMark = (record: TaskRecord, reason?: "restart"): TaskRecord => {
  const { abortReason: _reason, restarts: _restarts, ...rest } = record;
  return reason === undefined
    ? { ...rest, abortRequested: true }
    : { ...rest, abortRequested: true, abortReason: reason };
};

/** Replaces a live record's state; memos and the restart count disappear once an outcome is decided. */
export const withState = (record: TaskRecord, state: TaskState): TaskRecord => {
  if (state.status !== "terminal" && state.status !== "completing") {
    // SAFETY: a live state keeps the live record's memos.
    return { ...record, state } as TaskRecord;
  }
  // SAFETY: a settled record carries no memos; dropping them from a live one is the point.
  const { memos: _memos, restarts: _restarts, ...rest } = record as LiveTaskRecord;
  return { ...rest, state };
};

/** A task state as the JSON value it is stored as, for a structural comparison. */
const stateJson = (state: TaskState): Json => state;

/** The phase a checkpoint names, if it is an object with a string `phase`. */
export const phaseOf = (checkpoint: Json): string | undefined => {
  if (!Predicate.isObject(checkpoint) || Array.isArray(checkpoint)) return undefined;
  // SAFETY: a non-array JSON object; `phase` may be absent.
  const phase = (checkpoint as { readonly phase?: Json }).phase;
  return Predicate.isString(phase) ? phase : undefined;
};

/**
 * The record migrated to the definition's version: `migrate`'s result, copied as strict JSON and checked
 * against the definition's schemas. A missing `migrate`, a throw or a check that fails is a failed
 * migration.
 */
const migrateRecord = (
  record: LiveTaskRecord,
  definition: ErasedDefinition,
): Result.Result<LiveTaskRecord, Cause.Cause<unknown>> => {
  const migrate = definition.migrate;
  if (migrate === undefined) {
    return Result.fail(Cause.die(new Error(missingMigration(record, definition))));
  }
  const migrated = Result.try({
    try: () => migrate(record.input, record.state.checkpoint, record.version),
    catch: (error) => Cause.die(error),
  });
  if (Result.isFailure(migrated)) return Result.fail(migrated.failure);
  const input = copyJson(migrated.success.input);
  const checkpoint = copyJson(migrated.success.checkpoint);
  if (Result.isFailure(input)) return Result.fail(Cause.die(new Error(input.failure)));
  if (Result.isFailure(checkpoint)) return Result.fail(Cause.die(new Error(checkpoint.failure)));
  if (!Schema.is(definition.input)(input.success)) {
    return Result.fail(
      Cause.die(new Error(`Task ${record.kind} migration produced an input its schema rejects`)),
    );
  }
  if (!Schema.is(definition.checkpoint)(checkpoint.success)) {
    return Result.fail(
      Cause.die(
        new Error(`Task ${record.kind} migration produced a checkpoint its schema rejects`),
      ),
    );
  }
  // SAFETY: a live record keeps its status; only its input, checkpoint and version change.
  const state = { ...record.state, checkpoint: checkpoint.success } as LiveTaskRecord["state"];
  return Result.succeed({ ...record, version: definition.version, input: input.success, state });
};

/** Structural equality of two JSON values; object key order is ignored. */
const jsonEqual = (left: Json | undefined, right: Json | undefined): boolean => {
  if (left === right) return true;
  if (left === null || right === null || left === undefined || right === undefined) return false;
  if (Array.isArray(left) || Array.isArray(right)) {
    if (!Array.isArray(left) || !Array.isArray(right) || left.length !== right.length) return false;
    return left.every((value, index) => jsonEqual(value, right[index]));
  }
  if (!Predicate.isObject(left) || !Predicate.isObject(right)) return false;
  // SAFETY: non-array JSON objects.
  const leftObject = left as { readonly [key: string]: Json };
  // SAFETY: as above.
  const rightObject = right as { readonly [key: string]: Json };
  const keys = Object.keys(leftObject);
  if (keys.length !== Object.keys(rightObject).length) return false;
  return keys.every(
    (key) => Object.hasOwn(rightObject, key) && jsonEqual(leftObject[key], rightObject[key]),
  );
};

/** A copy of the overlay a plan can stage its own writes on; the links are rebuilt on first use. */
const copyOverlay = (overlay: Overlay): Overlay => ({
  tasks: new Map(overlay.tasks),
  edges: overlay.edges,
});

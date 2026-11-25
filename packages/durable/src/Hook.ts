/**
 * Hooks. A hook is a handler an extension attaches to a task
 * by name; the task runs the handlers of its conversation's selected
 * extensions, in extension order, at its hook points. The built-in tasks have
 * nine (`beforeRequest`, `beforeProviderRequest`, `afterResponse`,
 * `onYield`, `afterTools`, `afterRun`, `beforeTool`, `afterTool`, and the
 * compaction task's `beforeCompact`).
 *
 * Handlers run off the Session line. They may read committed state and use
 * the asking task's memos, but cannot commit: there is no `Tx` in a
 * `HookContext`. A failing handler is reported and the task goes on, except a
 * `beforeTool` failure, which blocks the call; an interruption propagates.
 *
 * @since 0.0.0
 */
import type * as Effect from "effect/Effect";
import type * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import type { ToolExecutionResult } from "./AgentTool.ts";
import type { CompactionReason } from "./Compaction.ts";
import { type ConversationId, type EntryId, EntryIdValue, type TaskId } from "./Ids.ts";
import type { AssistantMessage, Message, ToolCall, UserContent } from "./Message.ts";
import type { ResolvedModel } from "./Models.ts";
import type { EntryRecord } from "./Records.ts";
import type { DocSnapshot, DocSnapshotAsOf } from "./Session.ts";
import type { InvalidSignal, Signal, SignalKey, SignalState } from "./Signal.ts";
import type * as Task from "./Task.ts";
import type { TaskError } from "./TaskError.ts";

/**
 * What a hook may use: committed reads and the asking task's memos, shared
 * with the task.
 *
 * @category models
 * @since 0.0.0
 */
export interface HookContext {
  readonly taskId: TaskId;
  readonly conversationId: ConversationId;
  readonly memo: Task.TaskMemo;
  readonly snapshot: DocSnapshot<Task.RuntimeDocumentError>;
  readonly snapshotAsOf: DocSnapshotAsOf<Task.RuntimeDocumentError>;
  /** As `TaskRuntime.entry`. */
  readonly entry: Task.TaskEntryRead;
  /**
   * As `TaskRuntime.entries`: the raw history, which compaction never changes, so a range bounded by entry IDs,
   * such as an `afterRun` hook's `{ minEntryId: run.first, maxEntryId: run.last }`, reads the same before and
   * after a compaction and in a rerun.
   */
  readonly entries: Task.TaskEntriesRead;
  /** As `TaskRuntime.readSignal`, in the asking task: a gate reads the key it slept on. */
  readonly readSignal: <T>(
    signal: Signal<T>,
    key: string,
  ) => Effect.Effect<Option.Option<SignalState<T>>, InvalidSignal | TaskError>;
  /** The Harness clock. */
  readonly now: Effect.Effect<number, TaskError>;
}

/**
 * How a run ended, for `afterRun`. Stored in the generation's `finish`
 * checkpoint.
 *
 * @category models
 * @since 0.0.0
 */
export const RunSummary = Schema.Struct({
  /** A final answer, or a tool round whose every call asked to `terminate`. */
  reason: Schema.Literals(["answer", "terminate"]),
  /** The run's first entry (`core.live.run.first`), or the answer for a run without one. */
  first: EntryIdValue,
  /** The answer, or the terminating round's tool-calling answer. */
  answer: EntryIdValue,
  /** The run's last entry: the answer; for a `terminate` round its newest result, or the reset of its handoff. */
  last: EntryIdValue,
});
/**
 * The type of `RunSummary`.
 *
 * @category models
 * @since 0.0.0
 */
export type RunSummary = typeof RunSummary.Type;

/**
 * A task an `afterTools`, `onYield` or `afterRun` hook asks for, built by `hookTask`.
 *
 * @category models
 * @since 0.0.0
 */
export interface HookTask {
  readonly task: Task.Any;
  readonly input: Schema.Json;
}

/**
 * A task for an `afterTools`, `onYield` or `afterRun` hook to ask for, its input typed by the task. The task is
 * created from the installed definition of its name, as a child of the generation. A request whose task is
 * built-in or not installed, or whose `initial` does not return a checkpoint of one of its phases, is skipped and
 * reported against the hook's extension.
 *
 * @category constructors
 * @since 0.0.0
 */
export const hookTask = <I extends Schema.Json, S extends { readonly phase: string }, R, Req>(
  task: Task.Task<I, S, R, Req>,
  input: I,
): HookTask => ({ task, input });

/**
 * What an `onYield` task completes with: `continue` sends the agent back to work in the same run, as a hook's
 * `{ continue }` does. Without it, the answer stands.
 *
 * @category models
 * @since 0.0.0
 */
export interface YieldDecision {
  readonly continue?: UserContent | undefined;
}

/**
 * What an `afterTools` task completes with: `terminate` ends the run after this round, as a round whose calls
 * all asked to terminate. `null` is a decision too, which lets the round end as it would.
 *
 * @category models
 * @since 0.0.0
 */
export interface AfterToolsDecision {
  readonly terminate?: true | undefined;
}

/**
 * Hooks of the built-in generation task.
 *
 * @category models
 * @since 0.0.0
 */
export interface GenerationHooks<E = never, R = never> {
  /** Before every request attempt, recovery included; the result replaces the messages of that request only. */
  readonly beforeRequest: (
    request: { readonly messages: ReadonlyArray<Message> },
    context: HookContext,
  ) => Effect.Effect<{ readonly messages: ReadonlyArray<Message> } | undefined, E, R>;
  /**
   * After the model service builds the provider request body, before it is sent; the result replaces the body
   * for that request only. Runs each time the body is built; compaction requests do not use it.
   */
  readonly beforeProviderRequest: (
    request: { readonly model: ResolvedModel; readonly payload: Schema.Json },
    context: HookContext,
  ) => Effect.Effect<{ readonly payload: Schema.Json } | undefined, E, R>;
  /** Every terminal response, before classification. */
  readonly afterResponse: (
    message: AssistantMessage,
    context: HookContext,
  ) => Effect.Effect<void, E, R>;
  /**
   * A final answer; the first result wins. `continue` appends a user message and continues the run, unless the
   * inbox holds a steer, a follow-up or a reset. `tasks` decide whether the answer stands: it is appended and the
   * run's inputs stay open until they are terminal; the first that completes with a `YieldDecision` holding a
   * valid `continue` provides the continuation, and without one the run ends with the answer. The tasks read the
   * answer's entry from `core.live`'s `run.yield.answer`.
   */
  readonly onYield: (
    answer: AssistantMessage,
    context: HookContext,
  ) => Effect.Effect<
    { readonly continue: UserContent } | { readonly tasks: ReadonlyArray<HookTask> } | undefined,
    E,
    R
  >;
  /**
   * After every tool of the round is terminal; `results` are the round's result entries in call order. Every
   * hook runs, and the tasks they ask for, concatenated in extension order, run before the round ends: one that
   * completes with `{ terminate: true }` (`AfterToolsDecision`) ends the run as a round whose calls all asked to
   * terminate, and one may answer the run otherwise by submitting a steer in its terminal commit.
   */
  readonly afterTools: (
    assistant: EntryId,
    results: ReadonlyArray<EntryId>,
    context: HookContext,
  ) => Effect.Effect<{ readonly tasks: ReadonlyArray<HookTask> } | undefined, E, R>;
  /**
   * After a run answered (a final answer or a `terminate` round; not when an `onYield` continuation is taken, nor
   * after a failure or an abort): the tasks to run before it ends, built with `hookTask`. The answer is
   * committed and the run's inputs settled first; the run stays busy until the tasks are terminal.
   */
  readonly afterRun: (
    run: RunSummary,
    context: HookContext,
  ) => Effect.Effect<{ readonly tasks: ReadonlyArray<HookTask> } | undefined, E, R>;
}

/**
 * A call as tool hooks see it: `parent` is set for a nested call, one a running tool made
 * through `ToolContext.executeTool`, and names the calling tool task and call.
 *
 * @category models
 * @since 0.0.0
 */
export type ToolHookCall = ToolCall & {
  readonly parent?: { readonly taskId: TaskId; readonly callId: string } | undefined;
};

/**
 * What a `beforeTool` hook sleeps for: a deadline on the Harness clock, signal keys, or both.
 *
 * @category models
 * @since 0.0.0
 */
export interface GateSleep {
  readonly until?: number | undefined;
  readonly signals?: ReadonlyArray<SignalKey<unknown>> | undefined;
}

/**
 * Hooks of the built-in tool task, for model-issued and nested calls.
 *
 * @category models
 * @since 0.0.0
 */
export interface ToolHooks<E = never, R = never> {
  /**
   * Before intent; replaces the arguments, blocks the call with error text, or parks it at its gates. Arguments
   * replacements chain in extension order; the first `block` or `sleep` stops the chain. A failure blocks. A
   * `sleep` parks the call before it runs: every wake, and every open, asks every gate again from the top, so a
   * gate reads the key it slept on with `readSignal` and passes, blocks or sleeps again.
   */
  readonly beforeTool: (
    call: ToolHookCall,
    context: HookContext,
  ) => Effect.Effect<
    | {
        readonly arguments?: ToolCall["arguments"] | undefined;
        readonly block?: string | undefined;
        readonly sleep?: GateSleep | undefined;
      }
    | undefined,
    E,
    R
  >;
  /** After execution, before the result is committed; replaces the result. */
  readonly afterTool: (
    call: ToolHookCall,
    result: ToolExecutionResult,
    context: HookContext,
  ) => Effect.Effect<ToolExecutionResult | undefined, E, R>;
}

/**
 * What `beforeCompact` is asked about: `entries` are the active entries the summary
 * replaces, the head marker first, and `messages` their model context, the summarizer's source; `firstKept` is
 * the first entry kept verbatim.
 *
 * @category models
 * @since 0.0.0
 */
export interface CompactionRange {
  readonly reason: CompactionReason;
  readonly entries: ReadonlyArray<EntryRecord>;
  readonly messages: ReadonlyArray<Message>;
  readonly firstKept: EntryId;
  readonly instructions?: string | undefined;
}

/**
 * A `beforeCompact` decision: decline, which completes the compaction without a summary, or a summary placed
 * instead of the model's.
 *
 * @category models
 * @since 0.0.0
 */
export type CompactionDecision = { readonly decline: true } | { readonly summary: string };

/**
 * Hooks of the built-in compaction task.
 *
 * @category models
 * @since 0.0.0
 */
export interface CompactionHooks<E = never, R = never> {
  /** After range selection, before summarizing; the first decision wins. A failure is reported and skipped. */
  readonly beforeCompact: (
    compaction: CompactionRange,
    context: HookContext,
  ) => Effect.Effect<CompactionDecision | undefined, E, R>;
}

/**
 * A hook handler of any task, as the registry stores it.
 *
 * @category models
 * @since 0.0.0
 */
export type HookFunction = (
  ...args: ReadonlyArray<never>
) => Effect.Effect<unknown, unknown, unknown>;

/**
 * Hook handlers attached to tasks with one name.
 *
 * @category models
 * @since 0.0.0
 */
export interface Hook<Req> {
  readonly task: string;
  readonly handlers: Readonly<Record<string, HookFunction>>;
  /** The services the handlers require; a type-level record only. */
  readonly [RequirementsOf]?: Req;
}

declare const RequirementsOf: unique symbol;

/**
 * Any hook registration.
 *
 * @category models
 * @since 0.0.0
 */
export interface Any {
  readonly task: string;
  readonly handlers: Readonly<Record<string, HookFunction>>;
}

/**
 * The services a hook registration's handlers require.
 *
 * @category models
 * @since 0.0.0
 */
export type Requirements<T> =
  T extends Hook<infer Req> ? (unknown extends Req ? never : Req) : never;

type HandlerRequirements<H> = H extends (
  ...args: ReadonlyArray<never>
) => Effect.Effect<infer _A, infer _E, infer Req>
  ? Req
  : never;

/** The built-in generation task's name. */
const GENERATION = "core.generation";
/** The built-in tool task's name. */
const TOOL = "core.tool";
/** The built-in compaction task's name. */
const COMPACTION = "core.compaction";

/**
 * Hooks of the built-in generation task.
 *
 * @category constructors
 * @since 0.0.0
 */
export const generation = <E, R>(handlers: Partial<GenerationHooks<E, R>>): Hook<R> => ({
  task: GENERATION,
  // SAFETY: the registry erases handler types; the generation task calls each with its own arguments.
  handlers: handlers as Readonly<Record<string, HookFunction>>,
});

/**
 * Hooks of the built-in tool task.
 *
 * @category constructors
 * @since 0.0.0
 */
export const tool = <E, R>(handlers: Partial<ToolHooks<E, R>>): Hook<R> => ({
  task: TOOL,
  // SAFETY: the registry erases handler types; the tool task calls each with its own arguments.
  handlers: handlers as Readonly<Record<string, HookFunction>>,
});

/**
 * Hooks of the built-in compaction task.
 *
 * @category constructors
 * @since 0.0.0
 */
export const compaction = <E, R>(handlers: Partial<CompactionHooks<E, R>>): Hook<R> => ({
  task: COMPACTION,
  // SAFETY: the registry erases handler types; the compaction task calls each with its own arguments.
  handlers: handlers as Readonly<Record<string, HookFunction>>,
});

/**
 * Hook handlers for an application task; its handlers call them through
 * `runtime.hooks.each`.
 *
 * @category constructors
 * @since 0.0.0
 */
export const make = <H extends Readonly<Record<string, HookFunction>>>(
  task: Task.Any,
  handlers: H,
): Hook<HandlerRequirements<H[keyof H]>> =>
  // The requirements are recorded on the token and provided by the Harness.
  ({ task: task.definition.name, handlers });

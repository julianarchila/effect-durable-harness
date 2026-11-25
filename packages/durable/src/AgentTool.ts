/**
 * Tools. A tool is a name, a description, a parameters schema the model is
 * offered as JSON Schema, and an `execute` Effect. The
 * built-in `core.tool` task runs every call: it validates the arguments, runs
 * the hooks, commits the call's intent (the final arguments and the replay
 * policy) before executing, and appends the result entry.
 *
 * `execute` gets the decoded arguments and the call's capabilities, the
 * `ToolContext` service, also passed as its second argument; whatever else
 * it requires bubbles up to `Harness.layer`. Errors: a returned `isError`
 * result completes; a typed failure or a defect becomes a `tool_error` result
 * and the call ends `failed`; interruption is the abort.
 *
 * `execute` may return a park instead (`AgentTool.sleep`, `AgentTool.wait`):
 * the call holds no invocation until a signal, its deadline, or the tasks it
 * waits for wake it, and then runs again with `ToolContext.wake` and
 * `ToolContext.checkpoint` set. `cancel` cleans up the external work of a
 * parked call that is aborted.
 *
 * @since 0.0.0
 */
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Predicate from "effect/Predicate";
import type * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import type { Agent } from "./Agent.ts";
import * as Doc from "./Doc.ts";
import type { ExecutionEnv } from "./ExecutionEnv.ts";
import { type ConversationId, type TaskId, TaskIdValue } from "./Ids.ts";
import { type ToolResultContent, Usage } from "./Message.ts";
import type {
  CommitError,
  CommitOptions,
  DocSnapshot,
  DocSnapshotAsOf,
  DocWatchAcquire,
  Session,
} from "./Session.ts";
import type { ConversationNotFound } from "./SessionError.ts";
import type { JoinPolicy, WakeReason } from "./Records.ts";
import type { InvalidSignal, Signal, SignalKey, SignalState } from "./Signal.ts";
import type { ToolExecutionMode } from "./Settings.ts";
import type {
  ConversationHandle,
  RuntimeDocumentError,
  RuntimeReadError,
  SettledTask,
  Task,
  TaskMemo,
  TaskOptions,
  TypedTaskRecord,
} from "./Task.ts";
import type { TaskError } from "./TaskError.ts";
import type { Tx, TxWriteError } from "./Tx.ts";
import { capturedServices, toolHandler } from "./internal/handlers.ts";

// -----------------------------------------------------------------------------
// results
// -----------------------------------------------------------------------------

/**
 * A remark about a call for the model and the UI, such as truncation; never
 * part of the tool's data.
 *
 * @category results
 * @since 0.0.0
 */
export const ToolDiagnostic = Schema.Struct({
  severity: Schema.Literals(["info", "warn", "error"]),
  message: Schema.String,
  code: Schema.optionalKey(Schema.String),
});
/**
 * The type of `ToolDiagnostic`.
 *
 * @category results
 * @since 0.0.0
 */
export type ToolDiagnostic = typeof ToolDiagnostic.Type;

/**
 * Post-tools controls a result requests.
 *
 * @category results
 * @since 0.0.0
 */
export const ToolControl = Schema.Struct({
  /** Names to enable for the next request. */
  addTools: Schema.optionalKey(Schema.Array(Schema.String)),
  /**
   * Ends the run when every result of the round requests it; `false` does not.
   */
  terminate: Schema.optionalKey(Schema.Boolean),
  /** Starts a new context with this text; the last one of a round wins. */
  handoff: Schema.optionalKey(Schema.String),
});
/**
 * The type of `ToolControl`.
 *
 * @category results
 * @since 0.0.0
 */
export type ToolControl = typeof ToolControl.Type;

/**
 * What a tool returns, on three channels: `output` for the model, `structuredOutput` for programs that call
 * the tool, and `details` for UIs.
 *
 * @category results
 * @since 0.0.0
 */
export interface ToolExecutionResult<
  Details extends Schema.Json = Schema.Json,
  Structured extends Schema.Json = Schema.Json,
> {
  /** For the model; omitted: the retained `output` text. */
  readonly output?: ToolResultContent | undefined;
  /**
   * For programs. Required unless `isError` when the tool declares a `structuredOutput` schema, and validated
   * with it after `afterTool`; not allowed without one, where programs get the bounded output itself. The
   * model never sees it, and a model-issued call's result entry does not store it.
   */
  readonly structuredOutput?: Structured | undefined;
  readonly isError?: boolean | undefined;
  /** Omitted: the last `details` value. */
  readonly details?: Details | undefined;
  /** Added after those recorded through `ToolContext.diagnostic`. */
  readonly diagnostics?: ReadonlyArray<ToolDiagnostic> | undefined;
  /** Spend of the execution itself, such as a model call; recorded in `core.usage.tools`. */
  readonly usage?: Usage | undefined;
  readonly control?: ToolControl | undefined;
}

/**
 * What a park sleeps for: until the Harness clock reaches `until`, or a value under one of `signals` that no wake
 * used, whichever comes first. It needs `until` or a non-empty `signals`.
 *
 * @category parks
 * @since 0.0.0
 */
export interface ToolSleep {
  /** What the next run reads as `ToolContext.checkpoint`. */
  readonly checkpoint?: Schema.Json | undefined;
  /** Epoch milliseconds on the Harness clock. */
  readonly until?: number | undefined;
  readonly signals?: ReadonlyArray<SignalKey<unknown>> | undefined;
}

/**
 * What a park waits for: tasks the call's task owns, with a join policy; `any` resumes once one of them ends.
 *
 * @category parks
 * @since 0.0.0
 */
export interface ToolWait {
  /** What the next run reads as `ToolContext.checkpoint`. */
  readonly checkpoint?: Schema.Json | undefined;
  readonly on: ReadonlyArray<TaskId>;
  readonly policy: JoinPolicy;
}

/**
 * What `execute` returns instead of a result to park the call: it holds no invocation until it wakes, and then
 * `execute` runs again with the same arguments, `ToolContext.wake` and `ToolContext.checkpoint` set. Exactly one
 * of `sleep` and `wait`, and no result field; anything else ends the call with an `invalid_park` result.
 *
 * @category parks
 * @since 0.0.0
 */
export type ToolPark =
  | { readonly sleep: ToolSleep; readonly wait?: never }
  | { readonly wait: ToolWait; readonly sleep?: never };

/**
 * A park that sleeps (`ToolSleep`).
 *
 * @category parks
 * @since 0.0.0
 */
export const sleep = (options: ToolSleep): ToolPark => ({ sleep: options });

/**
 * A park that waits for owned tasks (`ToolWait`).
 *
 * @category parks
 * @since 0.0.0
 */
export const wait = (options: ToolWait): ToolPark => ({ wait: options });

/**
 * Whether `execute` returned a park rather than a result.
 *
 * @category parks
 * @since 0.0.0
 */
export const isPark = (returned: ToolExecutionResult | ToolPark): returned is ToolPark =>
  "sleep" in returned || "wait" in returned;

/**
 * A tool's `structuredOutput` broke its contract: it does not match the tool's schema, is missing from a
 * success of a tool that declares one, or comes from a tool that declares none. A model-issued
 * call only loses the value, and the Harness reports this error (`extension_error`) with the tool's extension.
 *
 * @category results
 * @since 0.0.0
 */
export class InvalidStructuredOutput extends Schema.TaggedError<InvalidStructuredOutput>()(
  "InvalidStructuredOutput",
  {
    /** The tool that returned the value. */
    tool: Schema.String,
    /** What broke, with Schema's wording for a value that does not match. */
    message: Schema.String,
  },
) {}

/**
 * The result of a nested call, as `ToolContext.executeTool` returns it. Programs read `structuredOutput`; the
 * output for the model is not kept, and `control` does not apply.
 *
 * `structuredOutput` is the tool's value for programs. A tool with a `structuredOutput` schema: its validated
 * value, also on an error result that has one; absent otherwise, and an error's text is then in an error
 * diagnostic. A tool without: its bounded output, success or error, as a string for one text item, an image for
 * one image, `""` for none, and the content list otherwise. Absent on an error result the Harness wrote itself
 * (unavailable, invalid, blocked, interrupted, aborted); `diagnostics` say what went wrong.
 *
 * @category results
 * @since 0.0.0
 */
export const NestedToolExecutionResult = Schema.Struct({
  /** The nested call's tool task. */
  taskId: TaskIdValue,
  structuredOutput: Schema.optionalKey(Schema.Json),
  isError: Schema.Boolean,
  details: Schema.optionalKey(Schema.Json),
  diagnostics: Schema.Array(ToolDiagnostic),
  usage: Schema.optionalKey(Usage),
  /**
   * From the call's start to its result on the Harness clock: gate waits, parks, downtime and reruns included. A
   * call recorded before start times were kept measures the run that produced the result, and none when it
   * never executed.
   */
  durationMs: Schema.optionalKey(Schema.Number),
});
/**
 * The type of `NestedToolExecutionResult`; `Structured` types `structuredOutput` when the call named a tool
 * token that declares a schema.
 *
 * @category results
 * @since 0.0.0
 */
export type NestedToolExecutionResult<Structured extends Schema.Json = Schema.Json> = Omit<
  typeof NestedToolExecutionResult.Type,
  "structuredOutput"
> & { readonly structuredOutput?: Structured };

/**
 * The state of one key of a tool call: a nested call, its task and once it ended its result, or a task created
 * under the key. `taskId` is
 * absent only inside the commit that creates the call; `result` is absent while it runs, and for a call the
 * scheduler faulted or orphaned.
 *
 * @category results
 * @since 0.0.0
 */
export const NestedCallState = Schema.Struct({
  /** A nested call, or a task created under the key (`createTask` with `key`); absent: a nested call. */
  kind: Schema.optionalKey(Schema.Literals(["nested", "task"])),
  taskId: Schema.optionalKey(TaskIdValue),
  result: Schema.optionalKey(NestedToolExecutionResult),
});
/**
 * The type of `NestedCallState`.
 *
 * @category results
 * @since 0.0.0
 */
export type NestedCallState = typeof NestedCallState.Type;

/**
 * The nested calls of a tool call: a family of the calling tool task, keyed by
 * the nested call's key, so a rerun finds the calls it already made and the caller reads each result. Created
 * with the nested task; the nested call's terminal commit adds its result. One document per call, so no commit
 * rewrites the others. It retires with the caller.
 *
 * @category results
 * @since 0.0.0
 */
export const NestedCallDoc = Doc.family("core.tool.nested-call", {
  version: 1,
  scope: "task",
  schema: NestedCallState,
  initial: (_seed: null) => ({}),
  // The document retires with its task.
  checkpointWhen: () => false,
});

/**
 * A nested call `executeTool` refused: a bad explicit key, or a key already used for another tool or other
 * arguments.
 *
 * @category results
 * @since 0.0.0
 */
export class InvalidNestedCall extends Schema.TaggedError<InvalidNestedCall>()(
  "InvalidNestedCall",
  { message: Schema.String },
) {}

/**
 * Options of `ToolContext.executeTool`.
 *
 * @category context
 * @since 0.0.0
 */
export interface ExecuteToolOptions {
  /**
   * Names the nested call within this call; its call ID is `<callId>/<key>`. Default: the call's position among
   * this invocation's nested calls, `1`, `2`, ... An explicit key is non-empty, without `/`, not `__proto__`,
   * and not a positive integer.
   */
  readonly key?: string | undefined;
  /** `false`: commit no running output, details or diagnostics to the nested call's slot, only its status. */
  readonly progress?: boolean | undefined;
  /** `false`: start or find the nested call and return its handle at once (`NestedToolCall`). Default `true`. */
  readonly wait?: boolean | undefined;
}

/**
 * Options of `ToolContext.createTask`.
 *
 * @category context
 * @since 0.0.0
 */
export type CreateTaskOptions = Omit<TaskOptions, "conversationId"> & {
  /**
   * Names the task within this call, among its nested calls' keys: a later `createTask` with the key, in this run,
   * a rerun or a later wake, returns the task instead of creating one. Another task kind or other input fails with
   * `InvalidNestedCall`, as does a key a nested call uses.
   */
  readonly key?: string | undefined;
};

/**
 * A nested call started without waiting (`executeTool` with `wait: false`).
 *
 * @category context
 * @since 0.0.0
 */
export interface NestedToolCall<Structured extends Schema.Json = Schema.Json, E = never> {
  /** The nested call's tool task; a tool may park waiting for it. */
  readonly taskId: TaskId;
  /** The stored result once the nested call is terminal; otherwise waits for it. */
  readonly result: Effect.Effect<NestedToolExecutionResult<Structured>, ExecuteToolError | E>;
}

/**
 * Options of `AgentTool.text` and `AgentTool.json`.
 *
 * @category results
 * @since 0.0.0
 */
export interface ResultOptions<Details extends Schema.Json> {
  /** What UIs read; typed by the tool's `details` schema. */
  readonly details?: Details | undefined;
}

/**
 * A result whose output for the model is `text`.
 *
 * @category results
 * @since 0.0.0
 */
export const text = <Details extends Schema.Json = Schema.Json>(
  text: string,
  options?: ResultOptions<Details>,
): ToolExecutionResult<Details> => withDetails([{ type: "text", text }], options);

/**
 * A result whose output for the model is `value` as JSON text.
 *
 * @category results
 * @since 0.0.0
 */
export const json = <Details extends Schema.Json = Schema.Json>(
  value: Schema.Json,
  options?: ResultOptions<Details>,
): ToolExecutionResult<Details> =>
  withDetails([{ type: "text", text: encodeJson(value) }], options);

const encodeJson = Schema.encodeSync(Schema.fromJsonString(Schema.Json));

const withDetails = <Details extends Schema.Json>(
  output: ToolResultContent,
  options: ResultOptions<Details> | undefined,
): ToolExecutionResult<Details> =>
  options?.details === undefined ? { output } : { output, details: options.details };

/**
 * Whether an interrupted execution may rerun on recovery, as a
 * tool declares it and as its intent stores it.
 *
 * @category definitions
 * @since 0.0.0
 */
export type ReplayPolicy = "safe" | "unsafe";

/**
 * Bounds on the output kept for the model: default
 * 50 KiB, 2000 lines, the head.
 *
 * @category definitions
 * @since 0.0.0
 */
export interface OutputLimits {
  readonly maxBytes?: number | undefined;
  readonly maxLines?: number | undefined;
  readonly retain?: "head" | "tail" | undefined;
}

// -----------------------------------------------------------------------------
// context
// -----------------------------------------------------------------------------

/**
 * Output an environment omitted before a chunk, measured on the decoded text.
 *
 * @category context
 * @since 0.0.0
 */
export interface OutputSkip {
  /** UTF-8 byte length of the omitted text. */
  readonly bytes: number;
  /** Newlines in the omitted text. */
  readonly newlines: number;
  /** Whether the omitted text ends with a newline. */
  readonly endsWithNewline: boolean;
}

/**
 * The tail of the output a call keeps, and how often it commits it, for an
 * environment that sends output over a slow link.
 *
 * @category context
 * @since 0.0.0
 */
export interface OutputWindow {
  readonly maxBytes: number;
  readonly maxLines: number;
  readonly minIntervalMs: number;
  readonly bytesPerSecond: number;
}

/**
 * The capabilities of one tool call. `output`, `retainedOutput`, `diagnostic`, `details` and `executeTool` are
 * refused with `TaskError` (`InvocationEnded`) once `execute` has returned. The other operations, such as
 * commits, memos, task creation and reads, work until the call's invocation ends, so a fiber the tool left
 * running may still use them; after that they fail with `TaskError` (`InvocationEnded`) too.
 *
 * @category context
 * @since 0.0.0
 */
// Models are a service in `R`, and the extensions are static.
export class ToolContext extends Context.Service<
  ToolContext,
  {
    /** The call's `core.tool` task: a stable key for the consumer's own idempotency. */
    readonly taskId: TaskId;
    readonly conversationId: ConversationId;
    /** The provider's call ID; not unique within a Session. */
    readonly callId: string;
    /** Why this run started after a park; `undefined` on a fresh call, and in `cancel`. */
    readonly wake: WakeReason | undefined;
    /** What the call last parked with; `undefined` on a fresh call or a park without one. */
    readonly checkpoint: Schema.Json | undefined;
    /**
     * A signal key's committed state in this call's task, the value migrated to the signal's version and decoded;
     * `None` while nothing came. Fails with `InvalidSignal` when the stored value cannot be read.
     */
    readonly readSignal: <T>(
      signal: Signal<T>,
      key: string,
    ) => Effect.Effect<Option.Option<SignalState<T>>, InvalidSignal | TaskError>;
    /** The calling conversation's agent, as this call's phase resolved it. */
    readonly agent: Effect.Effect<Agent, TaskError>;
    /**
     * The conversation's environment, built for this run of the call from the conversation's current `cwd`;
     * `undefined` without one. Files and processes are reached through it, never through an environment
     * captured when the tool was built.
     */
    readonly env: ExecutionEnv | undefined;
    /**
     * Appends running output; it becomes the result's `output` when the result omits it. `skipped` counts
     * output an environment omitted before the chunk.
     */
    readonly output: (
      chunk: string | Uint8Array,
      skipped?: OutputSkip | undefined,
    ) => Effect.Effect<void, TaskError>;
    /** The tail this call keeps and its commit pace; absent when the tool keeps the head of its output. */
    readonly outputWindow: OutputWindow | undefined;
    /** The output retained so far, as the model will see it, and whether earlier output was dropped. */
    readonly retainedOutput: Effect.Effect<
      { readonly text: string; readonly truncated: boolean },
      TaskError
    >;
    /** Records a model-visible remark about this call. */
    readonly diagnostic: (diagnostic: ToolDiagnostic) => Effect.Effect<void, TaskError>;
    /** Replaces running details; settles once a progress commit, or the result commit, holds them. */
    readonly details: (value: Schema.Json) => Effect.Effect<void, TaskError | CommitError>;
    /**
     * A commit bound to the call's task; `options.persistence` may defer storing it (`CommitPersistence`).
     */
    readonly commit: <A, E>(
      change: Effect.Effect<A, E, Tx>,
      options?: CommitOptions,
    ) => Effect.Effect<A, E | TaskError | CommitError>;
    /** First-writer-wins values of the call's task. */
    readonly memo: TaskMemo;
    /**
     * `` `${taskId}:${name}` ``: an idempotency key for an external system, the same on every run of this
     * call. Unique within this Session only: prefix it with the Session's identity when several share the
     * system.
     */
    readonly key: (name: string) => string;
    /**
     * Runs `effect` at most once per call once it succeeded: its result is recorded with `memo` (as
     * `step:<name>`, encoded with `schema`) and returned, decoded, by every later run.
     */
    readonly step: <A, I extends Schema.Json, E, R>(
      name: string,
      schema: Schema.Codec<A, I>,
      effect: Effect.Effect<A, E, R>,
    ) => Effect.Effect<A, E | TaskError | CommitError | Schema.SchemaError, R>;
    /**
     * Creates a task in one commit. A child of a tool that is not replay-safe defaults to
     * `abandonOnRestart`: the tool never resumes after a restart. With `key`, a later call with the key returns
     * that task (`CreateTaskOptions`). After a wake, a task needs a key: unkeyed, it fails with `InvalidNestedCall`.
     */
    readonly createTask: <I extends Schema.Json, S extends { readonly phase: string }, R, Req>(
      task: Task<I, S, R, Req>,
      input: I,
      options: CreateTaskOptions,
    ) => Effect.Effect<TaskId<R>, TaskError | TxWriteError | CommitError | InvalidNestedCall>;
    readonly getTask: <T>(
      id: TaskId<T>,
    ) => Effect.Effect<
      Option.Option<TypedTaskRecord<Schema.Json, Schema.Json, T>>,
      RuntimeReadError
    >;
    /**
     * The terminal receipt of a task, waited for in memory: the call's invocation stays active work while it
     * waits. To wait long, park the call on the task instead (`AgentTool.wait`).
     */
    readonly awaitTask: <T>(id: TaskId<T>) => Effect.Effect<SettledTask<T>, RuntimeReadError>;
    /** An invocation-bound handle of an existing conversation. */
    readonly conversation: (
      id: ConversationId,
    ) => Effect.Effect<Option.Option<ConversationHandle>, RuntimeReadError>;
    /** `conversation(id)`, failing with `ConversationNotFound` when it is absent. */
    readonly requireConversation: (
      id: ConversationId,
    ) => Effect.Effect<ConversationHandle, RuntimeReadError | ConversationNotFound>;
    readonly snapshot: DocSnapshot<RuntimeDocumentError>;
    readonly snapshotAsOf: DocSnapshotAsOf<RuntimeDocumentError>;
    /** A watch stopped when the call's invocation ends. */
    readonly watchDoc: DocWatchAcquire<RuntimeDocumentError>;
    /**
     * Runs tool `name` as a nested call of this one and waits for its result. See `ExecuteTool`.
     */
    readonly executeTool: ExecuteTool;
  }
>()("@effect-durable-harness/core/AgentTool/ToolContext") {}

/**
 * Why `ToolContext.executeTool` fails instead of returning a result: the call ended or was aborted, the Harness
 * closed, a commit or read failed, or the call was refused (`InvalidNestedCall`).
 *
 * @category context
 * @since 0.0.0
 */
export type ExecuteToolError =
  | TaskError
  | CommitError
  | TxWriteError
  | RuntimeDocumentError
  | RuntimeReadError
  | InvalidNestedCall;

/**
 * A tool token, as `executeTool` reads it: its name, and its `structuredOutput` schema, if any.
 *
 * @category context
 * @since 0.0.0
 */
export interface NamedTool<Structured extends Schema.Json> {
  readonly name: string;
  readonly definition: {
    readonly structuredOutput?: Schema.Codec<Structured, Structured> | undefined;
  };
}

/**
 * `ToolContext.executeTool`: runs a tool as a nested call of this one and waits
 * for its result. The nested call is its own `core.tool` task, owned by this call: it resolves the tool among the
 * agent's `callable` tools, validates, runs the tool hooks (which see `call.parent`), applies output limits and
 * the tool's replay policy, and reports progress in `core.live.nestedTools`. Its result returns here instead of
 * entering the transcript. A call that is blocked, invalid, fails, is interrupted or is aborted returns an
 * `isError` result.
 *
 * The key names the call within this one (`ExecuteToolOptions.key`). A rerun of a replay-safe tool that makes a
 * call with a used key, by calling in the same order or passing the same key, gets that call back, finished or
 * still running, instead of starting another; a call a crash interrupted before its result committed follows
 * its own replay policy, so this is not exactly-once. Reusing a key with another tool or other arguments fails
 * with `InvalidNestedCall`. A tool that is not replay-safe never reruns: after a restart its unfinished nested
 * calls are abandoned (`TaskOptions.abandonOnRestart`).
 *
 * Nested calls run as soon as they are made, whatever their `executionMode`. Interrupting the wait stops only
 * the wait: the call runs on. When this call settles, the nested calls it left running are aborted first. A
 * nested result's `usage` is already counted under its tool.
 *
 * Given a tool token instead of a name, the call resolves the token's name, and a value is decoded with the
 * token's `structuredOutput` schema, which types it.
 *
 * With `wait: false` it returns the nested call's handle once the call is made or found, and the caller may park
 * on its task (`AgentTool.wait`); waiting holds the invocation as long as the nested call takes, also while it is
 * parked. After a wake the same position may name another call, so a call without an explicit key fails with
 * `InvalidNestedCall`.
 *
 * @category context
 * @since 0.0.0
 */
export interface ExecuteTool {
  (
    name: string,
    args: Schema.JsonObject,
    options: ExecuteToolOptions & { readonly wait: false },
  ): Effect.Effect<NestedToolCall, ExecuteToolError>;
  <Structured extends Schema.Json>(
    tool: NamedTool<Structured>,
    args: Schema.JsonObject,
    options: ExecuteToolOptions & { readonly wait: false },
  ): Effect.Effect<NestedToolCall<Structured, Schema.SchemaError>, ExecuteToolError>;
  (
    name: string,
    args: Schema.JsonObject,
    options?: ExecuteToolOptions & { readonly wait?: true | undefined },
  ): Effect.Effect<NestedToolExecutionResult, ExecuteToolError>;
  <Structured extends Schema.Json>(
    tool: NamedTool<Structured>,
    args: Schema.JsonObject,
    options?: ExecuteToolOptions & { readonly wait?: true | undefined },
  ): Effect.Effect<NestedToolExecutionResult<Structured>, ExecuteToolError | Schema.SchemaError>;
}

/**
 * The call a handler gets as its second argument: the `ToolContext` service of the call.
 *
 * @category context
 * @since 0.0.0
 */
export type Call = ToolContext["Service"];

// -----------------------------------------------------------------------------
// definitions
// -----------------------------------------------------------------------------

/**
 * A tool definition.
 *
 * @category definitions
 * @since 0.0.0
 */
export interface ToolDefinition<
  Params,
  Details extends Schema.Json,
  E,
  R,
  Structured extends Schema.Json = Schema.Json,
> {
  /** Unique within an agent's offer; what the model calls. */
  readonly name: string;
  readonly description: string;
  /** Decodes the model's arguments; its JSON Schema is the declaration the model is offered. */
  readonly parameters: Schema.Decoder<Params>;
  /**
   * The JSON Schema the model is offered instead of the one generated from `parameters`, as Effect's dynamic
   * tools allow. Effect generates it from the schema's JSON encoding.
   */
  readonly jsonSchema?: Schema.JsonObject | undefined;
  /** Whether an interrupted execution may rerun on recovery. Default `unsafe`. */
  readonly replay?: ReplayPolicy | undefined;
  /** Default: the settings' `toolExecution`. One sequential call makes its whole round sequential. */
  readonly executionMode?: ToolExecutionMode | undefined;
  /**
   * Kept from the model until the conversation's `modelTools` names it, in its list or its `add`;
   * `control.addTools` loads it, as `ToolSearch` does. Other tools may call it any time.
   */
  readonly deferred?: boolean | undefined;
  /**
   * Who may call the tool: the model, other tools through `ToolContext.executeTool`, or both (default). A tool
   * behind another one can be `["tools"]`, so the model is never offered it.
   */
  readonly callers?: ReadonlyArray<"model" | "tools"> | undefined;
  readonly outputLimits?: OutputLimits | undefined;
  /**
   * Repairs arguments models commonly get wrong before validation; pure, must not mutate `args`, and its
   * result is still decoded with `parameters`.
   */
  readonly prepareArguments?: ((args: Schema.Json) => Schema.Json) | undefined;
  /** Types the result's `details`, what UIs read; without it they are any JSON. */
  readonly details?: Schema.Codec<Details, Details> | undefined;
  /**
   * The schema of the result's `structuredOutput`, which programs that call the tool receive.
   * Without it the tool returns none, and programs get its bounded output.
   */
  // A `Schema` of JSON, as `parameters` is. The Harness validates with it, so `invalid_structured_output`
  // messages use Schema's wording. The model is offered no output schema, so none is derived for the model
  // request.
  readonly structuredOutput?: Schema.Codec<Structured, Structured> | undefined;
  /**
   * Runs the call, with its decoded arguments and its context. It may return a park instead of a result
   * (`ToolPark`): the call then runs again, with the same arguments, once it wakes.
   */
  readonly execute: (
    args: Params,
    call: Call,
  ) => Effect.Effect<ToolExecutionResult<Details, NoInfer<Structured>> | ToolPark, E, R>;
  /**
   * Cleans up the external work of a call that parked, when it is aborted while no code of the tool runs: parked,
   * or woken and cut off before it parked again. `call.checkpoint` is the last park's, `call.wake` is `undefined`,
   * and it starts no nested calls or tasks. It may run after `execute` already cleaned up, so it is idempotent. A
   * failure is reported; the call ends `aborted` either way.
   */
  readonly cancel?: ((args: Params, call: Call) => Effect.Effect<void, E, R>) | undefined;
}

/**
 * A typed tool token.
 *
 * @category definitions
 * @since 0.0.0
 */
export interface AgentTool<
  Params,
  Details extends Schema.Json,
  E,
  Req,
  Structured extends Schema.Json = Schema.Json,
> {
  readonly name: string;
  /** Without `execute` for a tool declared on its own; its handler is then a Layer. */
  readonly definition: ToolDeclaration<Params, Details, Structured> & {
    readonly execute?:
      | ToolDefinition<Params, Details, E, unknown, Structured>["execute"]
      | undefined;
    readonly cancel?: ToolDefinition<Params, Details, E, unknown, Structured>["cancel"];
  };
  /** The schema of the result's `details`, when the tool declares one. */
  readonly details?: Schema.Codec<Details, Details> | undefined;
  /** The services `execute` requires besides `ToolContext`; a type-level record only. */
  readonly [RequirementsOf]?: Req;
}

/**
 * A tool's definition without its implementation: what the model is offered and its policies.
 *
 * @category definitions
 * @since 0.0.0
 */
export type ToolDeclaration<
  Params,
  Details extends Schema.Json,
  Structured extends Schema.Json = Schema.Json,
> = Omit<ToolDefinition<Params, Details, never, never, Structured>, "execute" | "cancel">;

/**
 * The context key of a declared tool's implementation: a declared tool requires it, and its `toLayer`
 * provides it (as `effect/ai`'s `Tool.Handler`).
 *
 * @category definitions
 * @since 0.0.0
 */
export interface Handler<Name extends string> {
  readonly _: unique symbol;
  readonly name: Name;
}

/**
 * What a handler Layer provides: the execution, with the services its Layer was built with.
 *
 * @category definitions
 * @since 0.0.0
 */
export interface HandlerService {
  /** Erased: called only with what the declared tool's `parameters` decoded, and the call. */
  readonly execute: ErasedExecute;
  readonly cancel?: ErasedCancel | undefined;
}

/** How the tool task sees any tool's failure: it only reads a tag or a message. */
type ErasedFailure = { readonly _tag: string } | Error;

/**
 * The context key of the tool named `name`.
 *
 * @category definitions
 * @since 0.0.0
 */
export const handlerKey = <Name extends string>(
  name: Name,
): Context.Service<Handler<Name>, HandlerService> =>
  Context.Service<Handler<Name>, HandlerService>(
    `@effect-durable-harness/core/AgentTool/Handler/${name}`,
  );

/**
 * A tool declared without its implementation; `toLayer` implements it.
 *
 * @category definitions
 * @since 0.0.0
 */
export interface Declared<
  Name extends string,
  Params,
  Details extends Schema.Json,
  Structured extends Schema.Json = Schema.Json,
> extends AgentTool<Params, Details, never, Handler<Name>, Structured> {
  readonly name: Name;
  /**
   * The implementation as a Layer. Given an Effect, it runs once when the Layer is built: services resolved
   * there are captured, as `effect/ai`'s `toolkit.toLayer` does. The execution's own requirements other than
   * `ToolContext` and `Session`, which the Harness provides per call, become the Layer's.
   *
   * **Gotchas**
   *
   * A host may build the Layer once and share it across sessions: never capture a per-session
   * service while it is built; read the session from the call.
   */
  readonly toLayer: {
    <E, R, RB = never>(
      implementation:
        | ((
            args: Params,
            call: Call,
          ) => Effect.Effect<ToolExecutionResult<Details, Structured> | ToolPark, E, R>)
        | Effect.Effect<
            (
              args: Params,
              call: Call,
            ) => Effect.Effect<ToolExecutionResult<Details, Structured> | ToolPark, E, R>,
            never,
            RB
          >,
    ): Layer.Layer<
      Handler<Name>,
      never,
      Exclude<R, ToolContext | Session> | Exclude<RB, Scope.Scope>
    >;
    /** With `cancel` (`ToolDefinition.cancel`) beside `execute`. */
    <E, R, RB = never>(
      implementation:
        | DeclaredHandlers<Params, Details, Structured, E, R>
        | Effect.Effect<DeclaredHandlers<Params, Details, Structured, E, R>, never, RB>,
    ): Layer.Layer<
      Handler<Name>,
      never,
      Exclude<R, ToolContext | Session> | Exclude<RB, Scope.Scope>
    >;
  };
}

/**
 * The handlers of a declared tool that parks and cleans up, as its `toLayer` takes them: `execute` and `cancel`
 * (`ToolDefinition.cancel`).
 *
 * @category definitions
 * @since 0.0.0
 */
export interface DeclaredHandlers<
  Params,
  Details extends Schema.Json,
  Structured extends Schema.Json,
  E,
  R,
> {
  readonly execute: (
    args: Params,
    call: Call,
  ) => Effect.Effect<ToolExecutionResult<Details, Structured> | ToolPark, E, R>;
  readonly cancel?: ((args: Params, call: Call) => Effect.Effect<void, E, R>) | undefined;
}

/**
 * The handler of a declared tool, as its `toLayer` takes it: `AgentTool.Execute<typeof RollDice>`.
 * `E` and `R` are the handler's errors and services.
 *
 * @category definitions
 * @since 0.0.0
 */
export type Execute<T, E = never, R = never> =
  T extends AgentTool<infer Params, infer Details, infer _E, infer _R, infer Structured>
    ? (
        args: Params,
        call: Call,
      ) => Effect.Effect<ToolExecutionResult<Details, Structured> | ToolPark, E, R>
    : never;

declare const RequirementsOf: unique symbol;

/**
 * Any tool, whatever its types.
 *
 * @category definitions
 * @since 0.0.0
 */
export interface Any {
  readonly name: string;
  readonly definition: {
    readonly name: string;
    readonly description: string;
    readonly parameters: Schema.Decoder<unknown>;
    readonly jsonSchema?: Schema.JsonObject | undefined;
    readonly replay?: ReplayPolicy | undefined;
    readonly executionMode?: ToolExecutionMode | undefined;
    readonly deferred?: boolean | undefined;
    readonly callers?: ReadonlyArray<"model" | "tools"> | undefined;
  };
}

/**
 * The services a tool's `execute` requires besides `ToolContext`.
 *
 * @category definitions
 * @since 0.0.0
 */
export type Requirements<T> =
  T extends AgentTool<infer _P, infer _D, infer _E, infer Req, infer _S>
    ? unknown extends Req
      ? never
      : Req
    : never;

/**
 * Options of `AgentTool.make`.
 *
 * @category definitions
 * @since 0.0.0
 */
export type MakeOptions<
  Params,
  Details extends Schema.Json,
  E,
  R,
  Structured extends Schema.Json = Schema.Json,
> = Omit<ToolDefinition<Params, Details, E, R, Structured>, "name">;

/**
 * Defines a tool; `args` are typed by `parameters`, the result's `details` by `details` when given, and its
 * `structuredOutput` by `structuredOutput`: without that schema the result has none.
 * `execute` gets the arguments and the call. With `execute` it is defined and implemented at once. Without it,
 * it is a declaration: `tool.toLayer(execute)` implements it as a Layer, and every Harness whose extensions
 * include it requires that Layer.
 *
 * @category constructors
 * @since 0.0.0
 */
export function make<
  Params,
  E,
  R,
  Details extends Schema.Json = Schema.Json,
  Structured extends Schema.Json = Schema.Json,
>(
  name: string,
  options: MakeOptions<Params, Details, E, R, Structured>,
): AgentTool<Params, Details, E, Exclude<R, ToolContext | Session>, Structured>;
export function make<
  const Name extends string,
  Params,
  Details extends Schema.Json = Schema.Json,
  Structured extends Schema.Json = Schema.Json,
>(
  name: Name,
  options: Omit<ToolDeclaration<Params, Details, Structured>, "name">,
): Declared<Name, Params, Details, Structured>;
export function make(name: string, options: MakeInput): Any {
  // SAFETY: `execute` is the only field the overloads' shapes differ in; it is called only by `run`.
  const own = (options as { readonly execute?: ErasedExecute }).execute;
  const definition = { ...options, name };
  const details = options.details;
  if (own !== undefined) {
    const defined = { name, definition, details };
    return defined;
  }
  const key = handlerKey(name);
  const toLayer = (
    implementation: ErasedHandlers | Effect.Effect<ErasedHandlers>,
  ): Layer.Layer<Handler<string>> =>
    Layer.effect(
      key,
      Effect.gen(function* () {
        const handlers = Effect.isEffect(implementation) ? yield* implementation : implementation;
        const services = yield* capturedServices;
        return Predicate.isFunction(handlers)
          ? toolHandler({ execute: handlers }, services)
          : toolHandler(handlers, services);
      }),
    );
  // SAFETY: the overloads type `toLayer` from the declaration; this implementation is their erasure.
  return { name, definition, details, toLayer } as never;
}

/** What `make` takes, whichever overload: a declaration, with or without `execute`. */
type MakeInput = Omit<ToolDeclaration<unknown, Schema.Json>, "name">;

/** A handler, erased: called only with what the tool's `parameters` decoded, and the call. */
export type ErasedExecute = (
  args: never,
  call: Call,
) => Effect.Effect<ToolExecutionResult | ToolPark, ErasedFailure, ToolContext | Session>;

/** A `cancel` handler, erased as `ErasedExecute` is. */
export type ErasedCancel = (
  args: never,
  call: Call,
) => Effect.Effect<void, ErasedFailure, ToolContext | Session>;

/** What `toLayer` takes, erased: `execute` alone, or with `cancel`. */
type ErasedHandlers = ErasedExecute | HandlerService;

/**
 * Runs a tool: its own `execute`, or the handler the Harness's context holds for a declared one.
 * Erased, for the tool task: `args` must be what the tool's `parameters` decoded.
 *
 * @category utils
 * @since 0.0.0
 */
export const run = (
  tool: Any,
  args: never,
): Effect.Effect<ToolExecutionResult | ToolPark, ErasedFailure, ToolContext | Session> =>
  Effect.flatMap(handlersOf(tool), (handlers) =>
    Effect.flatMap(Effect.service(ToolContext), (call) => handlers.execute(args, call)),
  );

/**
 * Runs a tool's `cancel`, if it has one: its own, or the one the Harness's context holds for a declared tool.
 * Erased as `run` is.
 *
 * @category utils
 * @since 0.0.0
 */
export const runCancel = (
  tool: Any,
  args: never,
): Effect.Effect<void, ErasedFailure, ToolContext | Session> =>
  Effect.flatMap(handlersOf(tool), (handlers) => {
    const cancel = handlers.cancel;
    return cancel === undefined
      ? Effect.void
      : Effect.flatMap(Effect.service(ToolContext), (call) => cancel(args, call));
  });

/**
 * Whether a tool has a `cancel`: its own, or its declared handler's.
 *
 * @category utils
 * @since 0.0.0
 */
export const hasCancel = (tool: Any): Effect.Effect<boolean> =>
  Effect.map(handlersOf(tool), (handlers) => handlers.cancel !== undefined);

/** A tool's handlers: its definition's, or the Layer's for a declared tool, which dies when none implements it. */
const handlersOf = (tool: Any): Effect.Effect<HandlerService> => {
  // SAFETY: tools are built by `make`; a definition has `execute` unless the tool was declared on its own.
  const own = tool.definition as {
    readonly execute?: ErasedExecute;
    readonly cancel?: ErasedCancel | undefined;
  };
  const execute = own.execute;
  if (execute !== undefined) return Effect.succeed({ execute, cancel: own.cancel });
  const key = handlerKey(tool.name);
  return Effect.flatMap(Effect.context<never>(), (context) => {
    const handler = Context.getOption(context, key);
    return handler._tag === "Some"
      ? Effect.succeed(handler.value)
      : Effect.die(new Error(`Tool ${tool.name} is declared but no Layer implements it`));
  });
};

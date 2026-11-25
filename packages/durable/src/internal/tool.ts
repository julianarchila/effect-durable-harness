// The built-in `core.tool` task, which runs one tool call, model-issued or nested (a call a running tool made
// through `ToolContext.executeTool`). Its pure decisions (replay, abort result, outcome, result building) live in
// `toolPlan.ts`; the output throttle in `progress.ts`. `NestedCallDoc` is public, in `AgentTool.ts`.
//
// Structured output is checked here and shaped for nested calls in `toolPlan.ts`.
import * as Delta from "@effect-durable-harness/delta/Delta";
import type * as Tracker from "@effect-durable-harness/delta/Tracker";
import * as Json from "@effect-durable-harness/delta/Json";
import * as Cause from "effect/Cause";
import * as Clock from "effect/Clock";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Option from "effect/Option";
import * as Predicate from "effect/Predicate";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import * as SchemaIssue from "effect/SchemaIssue";
import type { Agent } from "../Agent.ts";
import * as AgentTool from "../AgentTool.ts";
import {
  type ExecuteToolOptions,
  InvalidNestedCall,
  InvalidStructuredOutput,
  NestedCallDoc,
  type NestedToolExecutionResult,
  ToolContext,
  ToolControl,
  type OutputLimits,
  type ReplayPolicy,
  type ToolDiagnostic,
  type ToolExecutionResult,
} from "../AgentTool.ts";
import { AssistantEntry, ToolResultEntry, type TypedEntry } from "../Entry.ts";
import type { ExecutionEnv } from "../ExecutionEnv.ts";
import type { GateSleep, HookContext, ToolHookCall, ToolHooks } from "../Hook.ts";
import { type ConversationId, EntryIdValue, type TaskId, TaskIdValue } from "../Ids.ts";
import { LiveDoc, type NestedToolSlot } from "../Live.ts";
import { ToolCall } from "../Message.ts";
import type { TaskRecord, WakeReason } from "../Records.ts";
import { Events } from "../Telemetry.ts";
import type { CommitError, CommitOptions } from "../Session.ts";
import * as Task from "../Task.ts";
import { InvocationEnded, TaskError } from "../TaskError.ts";
import { Tx, type TxDocError, type TxReadError } from "../Tx.ts";
import { toolExtension } from "./agent.ts";
import * as Calls from "./calls.ts";
import { coerce, dropNullOptionals } from "./coerce.ts";
import { causeMessage, type Failure } from "./decisions.ts";
import { assignJson, jsonEqual } from "./json.ts";
import {
  clearOutput,
  clearProgress,
  cutOffCount,
  cutOffTooOften,
  finishSlot,
  isNestedSlot,
  PROGRESS_COMMIT,
  removeNestedSlots,
  salvageable,
  toolSlot,
} from "./live.ts";
import {
  DEFAULT_MAX_BYTES,
  DEFAULT_MAX_LINES,
  OutputBuffer,
  PROGRESS_BYTES_PER_SECOND,
  type ResolvedOutputLimits,
  utf8ByteLength,
} from "./output.ts";
import { BASE_PARKS, isComposite, type ParkCarrier } from "./parks.ts";
import * as Progress from "./progress.ts";
import { toolJsonSchema } from "./prompt.ts";
import { doneAfterYield } from "./signal.ts";
import * as telemetry from "./telemetry.ts";
import {
  boundHarnessResult,
  boundResult,
  checkKey,
  checkStructured,
  COMPLETED,
  type FromSlot,
  decideReplay,
  fallbackResult,
  fromSlot,
  harnessError,
  mergeReported,
  nestedResult,
  summaryOf,
  resolveLimits,
  type ToolEnding,
  toolAbortResult,
  toolDiagnostic,
  toolOutcome,
  toolResultMessage,
  validationMessage,
  restartLimitEnding,
} from "./toolPlan.ts";
import { recordUsage } from "./usage.ts";

export { harnessError } from "./toolPlan.ts";

// ─── Schemas ────────────────────────────────────────────────────────────────

/** A model-issued call, read from its assistant entry. */
const ModelToolTaskInput = Schema.Struct({
  kind: Schema.Literal("model"),
  assistant: EntryIdValue,
  callId: Schema.String,
  /** A call to a tool its request did not offer: settles `tool_unavailable` without resolving the name. */
  unoffered: Schema.optionalKey(Schema.Literal(true)),
});

/**
 * A nested call a running tool made through `executeTool`, whose call the input carries because no entry holds
 * it.
 */
const NestedToolTaskInput = Schema.Struct({
  kind: Schema.Literal("nested"),
  parent: TaskIdValue,
  parentCallId: Schema.String,
  key: Schema.String,
  call: ToolCall,
  /** `false`: commit no running output, details or diagnostics to the slot; the result still gets them. */
  progress: Schema.optionalKey(Schema.Literal(false)),
});

/** A model-issued or a nested call. */
export const ToolTaskInput = Schema.Union([ModelToolTaskInput, NestedToolTaskInput]);
export type ToolTaskInput = typeof ToolTaskInput.Type;

const isNestedInput = Schema.is(NestedToolTaskInput);

/** Why a parked call woke, as its intent stores it; a checkpoint's schema decodes to what it encodes. */
const StoredWake = Schema.Union([
  Schema.Struct({ reason: Schema.Literal("time") }),
  Schema.Struct({ reason: Schema.Literal("expired") }),
  Schema.Struct({ reason: Schema.Literal("signal"), signal: Schema.String, key: Schema.String }),
  Schema.Struct({ reason: Schema.Literal("tasks"), ended: Schema.Array(TaskIdValue) }),
]);

/**
 * Durable intent: the final arguments and the replay policy recorded before execution, and for a run after a
 * park, why it woke and what it parked with. `execute()` runs in the phase that commits it; this phase is reached
 * only by recovery.
 */
const ExecuteCheckpoint = Schema.Struct({
  phase: Schema.Literal("execute"),
  arguments: Schema.JsonObject,
  replay: Schema.Literals(["safe", "unsafe"]),
  wake: Schema.optionalKey(StoredWake),
  checkpoint: Schema.optionalKey(Schema.Json),
});
type ExecuteCheckpoint = typeof ExecuteCheckpoint.Type;

/** The call is parked: the `sleeping` or `waiting` state holds this checkpoint. */
const ParkedCheckpoint = Schema.Struct({
  phase: Schema.Literal("parked"),
  arguments: Schema.JsonObject,
  replay: Schema.Literals(["safe", "unsafe"]),
  checkpoint: Schema.optionalKey(Schema.Json),
});
type ParkedCheckpoint = typeof ParkedCheckpoint.Type;

/** `call` resolves, validates and gates the call; `execute` holds the intent; `parked` a park. */
export const ToolTaskCheckpoint = Schema.Union([
  Schema.Struct({ phase: Schema.Literal("call") }),
  ExecuteCheckpoint,
  ParkedCheckpoint,
]);
export type ToolTaskCheckpoint = typeof ToolTaskCheckpoint.Type;

/**
 * A model-issued call's result is its transcript entry; a nested call's result is in its caller's
 * `NestedCallDoc`, so the receipt stays small and the result retires with the caller.
 *
 * `kind` is required: no version 1 result, which lacked it, was ever stored.
 */
export const ToolTaskResult = Schema.Union([
  Schema.Struct({
    kind: Schema.Literal("model"),
    entryId: EntryIdValue,
    control: Schema.optionalKey(ToolControl),
  }),
  Schema.Struct({ kind: Schema.Literal("nested") }),
]);
export type ToolTaskResult = typeof ToolTaskResult.Type;

/**
 * Memo of a tool task that made a nested call, in any attempt,
 * committed before the first admission of each attempt: only then can `core.live` hold slots below it, which
 * settling removes. A crash between the two only costs a scan.
 */
const MADE_NESTED_CALLS = "core.tool.madeNestedCalls";

/** The tool task's name; `settle` reads it before `ToolTask` exists as a value it may refer to. */
const TOOL_TASK = "core.tool";

/** The input of a version 1 task, which had only model-issued calls. */
const ToolTaskInputV1 = Schema.Struct({ assistant: EntryIdValue, callId: Schema.String });

type Runtime = Task.TaskRuntime<ToolTaskInput, ToolTaskCheckpoint, ToolTaskResult>;

// ─── Task ───────────────────────────────────────────────────────────────────

/**
 * Resolves the called tool among its phase agent's tools, validates, runs
 * `beforeTool`, records intent, executes, runs `afterTool`, and settles the result, all in one `call` handler,
 * so nothing separates resolution from settlement. `execute` is reached only by recovery and applies the
 * replay rule.
 */
export const ToolTask = Task.make({
  name: TOOL_TASK,
  // Version 3 added `unoffered`; version 4 added `parked` and the optional fields of `execute`. An older Harness
  // leaves a newer task blocked (`task_too_old`) instead of misreading it.
  version: 4,
  input: ToolTaskInput,
  checkpoint: ToolTaskCheckpoint,
  result: ToolTaskResult,
  initial: () => ({ phase: "call" }),
  // Version 1 had only model-issued calls, without `kind`; version 2 inputs lack `unoffered`, and checkpoints
  // before version 4 lack `parked` and the optional fields of `execute`; both are kept. A record that does not
  // decode fails the migration, which blocks the task.
  migrate: (input, checkpoint, fromVersion) => ({
    input:
      fromVersion === 1
        ? { kind: "model" as const, ...Schema.decodeUnknownSync(ToolTaskInputV1)(input) }
        : Schema.decodeUnknownSync(ToolTaskInput)(input),
    checkpoint: Schema.decodeUnknownSync(ToolTaskCheckpoint)(checkpoint),
  }),
  phases: {
    call: (task, runtime) =>
      Effect.gen(function* () {
        const call = yield* readCall(runtime, task.input);
        if (yield* reachedLimit(task, runtime)) return yield* endCutOff(runtime, task, call);
        // An unoffered call resolves nothing, so even a tool the agent has never runs.
        const agent = yield* runtime.agent;
        const tool = isUnoffered(task.input)
          ? undefined
          : resolveTool(agent, task.input, call.name);
        if (tool === undefined) {
          const error = harnessError("tool_unavailable", `Tool ${call.name} is not available`);
          return yield* reject(runtime, task.input, call, undefined, error);
        }
        const prepared = prepare(tool, call.arguments);
        const checked = "error" in prepared ? prepared : yield* validate(tool, call, prepared.args);
        if ("error" in checked) {
          const invalid = harnessError("invalid_arguments", checked.error);
          return yield* reject(runtime, task.input, call, tool, invalid);
        }
        let args = checked.args;
        let block: string | undefined;
        let sleep: GateSleep | undefined;
        const context = hookContext(runtime);
        yield* runtime.hooks.each<ToolHooks["beforeTool"], never, never>("beforeTool", (hook) =>
          Effect.gen(function* () {
            if (block !== undefined || sleep !== undefined) return;
            const exit = yield* Effect.exit(hook({ ...call, arguments: args }, context));
            if (Exit.isSuccess(exit)) {
              const decision = exit.value;
              if (decision?.block !== undefined) block = decision.block;
              else if (decision?.sleep !== undefined) sleep = decision.sleep;
              else if (decision?.arguments !== undefined) args = decision.arguments;
              return;
            }
            // A signalled invocation's interruption propagates; any other failure blocks.
            if (Cause.hasInterrupts(exit.cause)) return yield* Effect.failCause(exit.cause);
            block = causeMessage(exit.cause);
          }),
        );
        if (block !== undefined) {
          const blocked = harnessError("blocked", `Tool call blocked: ${block}`);
          return yield* reject(runtime, task.input, call, tool, blocked);
        }
        if (sleep !== undefined) return yield* parkAtGate(runtime, task.input, call, tool, sleep);
        const validated = yield* validate(tool, call, args);
        if ("error" in validated) {
          const invalid = harnessError("invalid_arguments", validated.error);
          return yield* reject(runtime, task.input, call, tool, invalid);
        }
        yield* commitIntent(runtime, {
          phase: "execute",
          arguments: validated.args,
          replay: tool.replay ?? "unsafe",
        });
        yield* run({
          runtime,
          input: task.input,
          call,
          tool,
          agent,
          args: validated.value,
          stored: validated.args,
          resumed: {},
        });
      }),
    // Woken from a park: resolve the tool again, record intent with the wake, and run `execute` again. A tool that
    // no longer resolves cannot run or clean up; the call ends `failed`, so what it owns is aborted.
    parked: (task, runtime) =>
      Effect.gen(function* () {
        const parked = task.state.checkpoint;
        const wake = task.state.wake;
        const call = yield* readCall(runtime, task.input);
        if (yield* reachedLimit(task, runtime)) return yield* endCutOff(runtime, task, call);
        const agent = yield* runtime.agent;
        const tool = resolveTool(agent, task.input, call.name);
        if (tool === undefined) {
          const message = `Tool ${call.name} is not available`;
          yield* runtime.report(
            Cause.die(new Error(`${message}: its parked call ${call.id} cannot run again`)),
          );
          return yield* settleFromSlot(
            runtime,
            task.input,
            call,
            { tool },
            {
              ending: { status: "failed", message },
              code: "tool_unavailable",
              message,
            },
          );
        }
        const validated = yield* validate(tool, call, parked.arguments);
        if ("error" in validated) {
          const invalid = harnessError("invalid_arguments", validated.error);
          return yield* reject(runtime, task.input, call, tool, invalid);
        }
        const resumed = resumedOf(wake, parked.checkpoint);
        yield* commitIntent(runtime, {
          phase: "execute",
          arguments: parked.arguments,
          replay: tool.replay ?? "unsafe",
          ...resumed,
        });
        yield* run({
          runtime,
          input: task.input,
          call,
          tool,
          agent,
          args: validated.value,
          stored: parked.arguments,
          resumed,
        });
      }),
    // Recovery after intent: rerun only when the stored and the current policy both say `safe`.
    execute: (task, runtime) =>
      Effect.gen(function* () {
        const checkpoint = task.state.checkpoint;
        const call = yield* readCall(runtime, task.input);
        // The restart limit is checked also before an interruption result.
        if (yield* reachedLimit(task, runtime)) return yield* endCutOff(runtime, task, call);
        const agent = yield* runtime.agent;
        const current = resolveTool(agent, task.input, call.name);
        const decision = decideReplay(checkpoint.replay, current, call.name);
        const resumed = resumedOf(checkpoint.wake, checkpoint.checkpoint);
        if (decision._tag === "Interrupt") {
          // A woken run may have restarted external work that nothing will collect now.
          if (checkpoint.wake !== undefined && current !== undefined) {
            yield* cancel({
              runtime,
              input: task.input,
              call,
              tool: current,
              agent,
              stored: checkpoint.arguments,
              checkpoint: checkpoint.checkpoint,
            });
          }
          // Not cut off too often (checked above), so the hooks run.
          return yield* settleFromSlot(runtime, task.input, call, { tool: current }, decision);
        }
        const tool = decision.tool;
        // The rerun reports from scratch: clear what the interrupted attempt published, and show the slot running
        // with the arguments it runs with again, as the deferred commit that showed them may have been lost with
        // the crash. A woken run keeps the details and diagnostics earlier runs left, as a park does.
        yield* runtime.commit(() =>
          Effect.gen(function* () {
            const tx = yield* Tx;
            const slot = toolSlot(yield* tx.doc(LiveDoc, runtime.conversationId), runtime.taskId);
            if (slot !== undefined) {
              if (checkpoint.wake === undefined) clearProgress(slot);
              else clearOutput(slot);
              slot.status = "running";
              if (isNestedSlot(slot)) {
                // SAFETY: the stored intent's arguments; the draft takes them as its own value.
                slot.arguments = checkpoint.arguments as Tracker.Draft<Schema.JsonObject>;
              }
            }
          }),
        );
        // `execute` takes what `parameters` decodes, so the stored arguments are decoded again; a definition
        // whose schema no longer accepts them gets the `invalid_arguments` result a fresh call would.
        const validated = yield* validate(tool, call, checkpoint.arguments);
        if ("error" in validated) {
          const invalid = harnessError("invalid_arguments", validated.error);
          return yield* reject(runtime, task.input, call, tool, invalid);
        }
        yield* run({
          runtime,
          input: task.input,
          call,
          tool,
          agent,
          args: validated.value,
          stored: checkpoint.arguments,
          resumed,
        });
      }),
  },
  abort: (task, runtime) =>
    Effect.gen(function* () {
      const call = yield* readCall(runtime, task.input);
      const checkpoint = task.state.checkpoint;
      // An unoffered call could not run in any case, and its result says so.
      const abort = isUnoffered(task.input)
        ? ({ code: "tool_unavailable", message: `Tool ${call.name} is not available` } as const)
        : toolAbortResult(task.abortReason, checkpoint.phase, call.name);
      const tool = yield* toolForLimits(runtime, task.input, call.name);
      // External work may be live while no code of the tool runs: parked, or woken and cut off before it parked
      // again. The scheduler ends an abort handler cut off `restartLimit` times itself, so `cancel` cannot keep
      // it from ending.
      if (
        checkpoint.phase === "parked" ||
        (checkpoint.phase === "execute" && checkpoint.wake !== undefined)
      ) {
        if (tool !== undefined) {
          yield* cancel({
            runtime,
            input: task.input,
            call,
            tool,
            agent: yield* runtime.agent,
            stored: checkpoint.arguments,
            checkpoint: checkpoint.checkpoint,
          });
        } else {
          yield* runtime.report(
            Cause.die(
              new Error(
                `Tool ${call.name} is not available: its parked call ${call.id} was not cancelled`,
              ),
            ),
          );
        }
      }
      // The scheduler runs an abort handler only while its restarts are below the limit, and faults the task
      // without a result when the last attempt is cut off. That attempt skips the hooks, which may be what cuts
      // it off.
      const restartLimit = (yield* runtime.settings).restartLimit;
      const hooks = (task.restarts ?? 0) < restartLimit - 1 ? { tool } : undefined;
      yield* settleFromSlot(runtime, task.input, call, hooks, {
        ending: { status: "aborted" },
        code: abort.code,
        message: abort.message,
      });
    }),
});

/** What a run of `execute` starts from after a park: why it woke, and what the call last parked with. */
interface Resumed {
  readonly wake?: WakeReason;
  readonly checkpoint?: Schema.Json;
}

/** A `Resumed` under construction: its fields are set only when present. */
interface ResumedDraft {
  wake?: WakeReason;
  checkpoint?: Schema.Json;
}

const resumedOf = (wake: WakeReason | undefined, checkpoint: Schema.Json | undefined): Resumed => {
  const resumed: ResumedDraft = {};
  if (wake !== undefined) resumed.wake = wake;
  if (checkpoint !== undefined) resumed.checkpoint = checkpoint;
  return resumed;
};

/**
 * Records intent: the slot shows the call `running` (and a nested call the arguments it runs with), stored like
 * progress (`PROGRESS_COMMIT`), so parallel calls of one round share no immediate write to `core.live`; then the
 * `execute` checkpoint, durable before the tool runs.
 */
const commitIntent = (runtime: Runtime, intent: ExecuteCheckpoint) =>
  Effect.gen(function* () {
    yield* runtime.commit(
      () =>
        Effect.gen(function* () {
          const tx = yield* Tx;
          const slot = toolSlot(yield* tx.doc(LiveDoc, runtime.conversationId), runtime.taskId);
          if (slot === undefined) return;
          slot.status = "running";
          // Assigned whole: an equal value records no change, and a rerun corrects what an earlier attempt showed.
          if (isNestedSlot(slot)) {
            // SAFETY: a strict-JSON copy; the draft takes it as its own value.
            slot.arguments = intent.arguments as Tracker.Draft<Schema.JsonObject>;
          }
        }),
      PROGRESS_COMMIT,
    );
    yield* runtime.commit(() => Effect.succeed({ status: "running" as const, checkpoint: intent }));
  });

/**
 * Parks the call at its gates, before it runs: the task sleeps at `call`, its slot `sleeping`, and every wake
 * runs the whole phase again. A sleep the commit refuses (no `until` or signals, a used key, a bad deadline)
 * ends the call `invalid_park`, through `afterTool`.
 */
const parkAtGate = (
  runtime: Runtime,
  input: ToolTaskInput,
  call: ToolHookCall,
  tool: ErasedTool,
  sleep: GateSleep,
) =>
  Effect.gen(function* () {
    const committed = yield* Effect.exit(
      runtime.commit(() =>
        Effect.gen(function* () {
          const tx = yield* Tx;
          const slot = toolSlot(yield* tx.doc(LiveDoc, runtime.conversationId), runtime.taskId);
          if (slot !== undefined) slot.status = "sleeping";
          // oxlint-disable-next-line durable/no-external-in-commit -- builds the sleeping state; nothing waits.
          return Calls.sleep<ToolTaskCheckpoint>({
            checkpoint: { phase: "call" },
            until: sleep.until,
            signals: sleep.signals,
          });
        }),
      ),
    );
    if (Exit.isSuccess(committed)) return;
    const error = Cause.findErrorOption(committed.cause);
    if (Option.isNone(error) || !refusedPark(error.value))
      return yield* Effect.failCause(committed.cause);
    const invalid = harnessError(
      "invalid_park",
      `Tool call ${call.id} cannot wait at its gates: ${error.value.message}`,
    );
    yield* reject(runtime, input, call, tool, invalid);
  });

const atGate = Schema.is(Schema.Struct({ phase: Schema.Literal("call") }));

/**
 * Whether a sleeping task is a tool call parked at its gates, before its intent: every open wakes it, so the
 * gates installed then decide.
 */
export const gatedCall = (record: TaskRecord): boolean =>
  record.kind === TOOL_TASK &&
  record.state.status === "sleeping" &&
  atGate(record.state.checkpoint);

/** The phases of a tool call that was not cut off while running: not started yet, or parked. */
const PHASES_KEEPING_CHILDREN = Schema.Struct({ phase: Schema.Literals(["call", "parked"]) });
const keepsPhase = Schema.is(PHASES_KEEPING_CHILDREN);

/**
 * Whether a task's `abandonOnRestart` children survive a restart: it is a tool call that was not cut off while
 * running, parked or not started yet, whatever its status.
 */
export const keepsChildren = (record: TaskRecord): boolean =>
  record.kind === TOOL_TASK && "checkpoint" in record.state && keepsPhase(record.state.checkpoint);

/**
 * Whether the call was cut off `settings.restartLimit` times in a
 * row. The settings are read only for a task that was cut off at all.
 */
const reachedLimit = (task: { readonly restarts?: number | undefined }, runtime: Runtime) =>
  task.restarts === undefined
    ? Effect.succeed(false)
    : Effect.map(runtime.settings, (settings) => cutOffTooOften(task, settings.restartLimit));

/**
 * Ends the call with an error result `restart_limit`, built from its
 * slot like an interrupted call's, instead of running it again.
 */
const endCutOff = (
  runtime: Runtime,
  task: { readonly restarts?: number | undefined; readonly input: ToolTaskInput },
  call: ToolHookCall,
) =>
  Effect.gen(function* () {
    const cutOff = restartLimitEnding(call.name, cutOffCount(task));
    // Past the limit the hooks may be what keeps cutting the task off.
    const hooks = salvageable(task, (yield* runtime.settings).restartLimit)
      ? { tool: yield* toolForLimits(runtime, task.input, call.name) }
      : undefined;
    yield* settleFromSlot(runtime, task.input, call, hooks, cutOff);
  });

/**
 * Settles a result the Harness decided in the `call` phase without running
 * the tool (it did not resolve, the arguments are invalid, or `beforeTool` blocked the call); `afterTool` applies
 * as to an executed call's result.
 */
const reject = (
  runtime: Runtime,
  input: ToolTaskInput,
  call: ToolHookCall,
  tool: ErasedTool | undefined,
  result: ToolExecutionResult,
) =>
  Effect.gen(function* () {
    const settled = yield* afterHarnessResult(runtime, input, call, tool, { result });
    yield* settle({ runtime, input, call, ending: COMPLETED, result: settled });
  });

/** What `settleFromSlot` settles: how the task ends, and the code and message of the result's diagnostic. */
interface SlotEnding {
  readonly ending: ToolEnding;
  readonly code: string;
  readonly message: string;
}

/**
 * Settles an error result built from the call's slot: an
 * interruption, the restart limit, or an abort. The tool is not running, so the slot read before the commit is
 * still current when it commits. With `hooks`, the result passes `afterTool` and is bounded with the limits of
 * `hooks.tool` (the defaults when it is `undefined`); without, it is settled as built, when the hooks may be what
 * cuts the task off.
 */
const settleFromSlot = (
  runtime: Runtime,
  input: ToolTaskInput,
  call: ToolHookCall,
  hooks: { readonly tool: ErasedTool | undefined } | undefined,
  end: SlotEnding,
) =>
  Effect.gen(function* () {
    const live = Option.getOrUndefined(yield* runtime.snapshot(LiveDoc, runtime.conversationId));
    const slot = live === undefined ? undefined : toolSlot(live, runtime.taskId);
    const built = fromSlot(slot, end.code, end.message);
    const settled =
      hooks !== undefined
        ? yield* afterHarnessResult(runtime, input, call, hooks.tool, built)
        : built.truncation === undefined
          ? built.result
          : {
              ...built.result,
              diagnostics: [...(built.result.diagnostics ?? []), built.truncation],
            };
    yield* settle({ runtime, input, call, ending: end.ending, result: settled });
  });

/**
 * A result the Harness wrote itself, after the `afterTool`
 * chain, bounded with the tool's output limits (the defaults when it did not resolve), without the hooks'
 * `structuredOutput` (`boundHarnessResult`). `control` applies only when the task completes, which `settle`
 * decides.
 */
const afterHarnessResult = (
  runtime: Runtime,
  input: ToolTaskInput,
  call: ToolHookCall,
  tool: ErasedTool | undefined,
  written: FromSlot,
) =>
  Effect.gen(function* () {
    let final = written.result;
    const context = hookContext(runtime);
    yield* runtime.hooks.each<ToolHooks["afterTool"], never, never>("afterTool", (hook) =>
      Effect.map(hook(call, final, context), (replaced) => {
        final = replaced ?? final;
      }),
    );
    return boundHarnessResult({
      written: written.result,
      final,
      truncation: written.truncation,
      limits: outputLimits(tool),
      kind: input.kind,
      structured: tool?.structuredOutput !== undefined,
    });
  });

/**
 * The tool whose output limits bound a hooks' result: the called tool,
 * or none when it, or the agent, fails to resolve, and for an unoffered call, which never resolves its name.
 */
const toolForLimits = (
  runtime: Runtime,
  input: ToolTaskInput,
  name: string,
): Effect.Effect<ErasedTool | undefined> =>
  isUnoffered(input)
    ? Effect.undefined
    : runtime.agent.pipe(
        Effect.map((agent) => resolveTool(agent, input, name)),
        Effect.catchCause((cause) =>
          Cause.hasInterrupts(cause) ? Effect.interrupt : Effect.undefined,
        ),
      );

/** A model-issued call to a tool its request did not offer. */
const isUnoffered = (input: ToolTaskInput): boolean =>
  input.kind === "model" && input.unoffered === true;

/** The tool's output limits, all defaults when it did not resolve. */
const outputLimits = (tool: ErasedTool | undefined): ResolvedOutputLimits =>
  resolveLimits(tool?.outputLimits, {
    maxBytes: DEFAULT_MAX_BYTES,
    maxLines: DEFAULT_MAX_LINES,
    retain: "head",
  });

// ─── Resolution and validation ──────────────────────────────────────────────

declare const DecodedArguments: unique symbol;

/** A call's arguments as its tool's `parameters` decoded them; opaque once the tool is erased. */
interface Decoded {
  readonly [DecodedArguments]: true;
}

/** What the tool task calls on a tool, whatever its types. */
interface ErasedTool {
  readonly name: string;
  readonly description: string;
  readonly parameters: Schema.Decoder<Decoded>;
  readonly replay?: ReplayPolicy | undefined;
  readonly outputLimits?: OutputLimits | undefined;
  readonly prepareArguments?: ((args: Schema.Json) => Schema.Json) | undefined;
  /** The `structuredOutput` schema, erased to JSON. */
  readonly structuredOutput?: Schema.Codec<Schema.Json, Schema.Json> | undefined;
  readonly execute: (
    args: Decoded,
  ) => Effect.Effect<ToolExecutionResult | AgentTool.ToolPark, Failure, ToolContext>;
  readonly cancel: (args: Decoded) => Effect.Effect<void, Failure, ToolContext>;
  /** Whether the tool has a `cancel`: its own, or its declared handler's. */
  readonly hasCancel: Effect.Effect<boolean>;
  /** The tool the agent resolved, which tells a wrapper's composite apart. */
  readonly source: AgentTool.Any;
}

/**
 * The called tool among the tools the model is offered for a model-issued call, else among those tools may
 * call.
 */
const resolveTool = (agent: Agent, input: ToolTaskInput, name: string): ErasedTool | undefined => {
  const tools = input.kind === "model" ? agent.tools : agent.callable;
  const tool = tools.find((candidate) => candidate.name === name);
  return tool === undefined ? undefined : erasedTool(tool);
};

const erasedTool = (tool: AgentTool.Any): ErasedTool =>
  // SAFETY: every tool is built by `AgentTool.make`, and `execute` only ever gets what its own `parameters`
  // decoded. Its requirements other than `ToolContext`, a declared tool's handler included, are the
  // Harness's, provided to the handler by the scheduler; any failure becomes a Cause the task inspects.
  ({
    ...tool.definition,
    execute: (args: Decoded) => AgentTool.run(tool, args as never),
    cancel: (args: Decoded) => AgentTool.runCancel(tool, args as never),
    hasCancel: AgentTool.hasCancel(tool),
    source: tool,
  }) as ErasedTool;

/**
 * The call, from the input for a nested call, which hooks see with its
 * parent, else from the assistant entry. A missing call faults the task.
 */
const readCall = (runtime: Runtime, input: ToolTaskInput) =>
  Effect.gen(function* () {
    const call: ToolHookCall =
      input.kind === "nested"
        ? { ...input.call, parent: { taskId: input.parent, callId: input.parentCallId } }
        : yield* modelCall(runtime, input);
    // The phase span names the call it runs: read first in every phase,
    // where the phase span is the current one. A tracer that throws there is reported, never the phase's failure.
    const span = yield* Effect.option(Effect.currentSpan);
    let thrown: Error | undefined;
    if (Option.isSome(span)) {
      telemetry.annotate(
        span.value,
        { "durable.tool.name": call.name, "durable.tool.call_id": call.id },
        (error) => {
          thrown ??= error;
        },
      );
    }
    if (thrown !== undefined) {
      yield* runtime
        .report(Cause.die(thrown), { kind: "host", callback: "telemetry" })
        .pipe(Effect.ignore);
    }
    return call;
  });

/** The model-issued call `input` names, from its assistant entry. A missing call faults the task. */
const modelCall = (runtime: Runtime, input: typeof ModelToolTaskInput.Type) =>
  Effect.gen(function* () {
    const entry = Option.getOrUndefined(yield* runtime.entry(AssistantEntry, input.assistant));
    const message = entry?.model?.[0];
    const call =
      message?.role === "assistant"
        ? message.content.find(
            (content): content is ToolCall =>
              content.type === "toolCall" && content.id === input.callId,
          )
        : undefined;
    if (call === undefined) {
      return yield* Effect.die(
        new Error(`Entry ${input.assistant} has no tool call ${input.callId}`),
      );
    }
    return call;
  });

/**
 * The text of a failed execution's `tool_error` diagnostic, and of an `ExecutionEnvError`: the error's message.
 * `causeMessage` returns a tagged error's `message`, which is empty for one declared without a `message`
 * field; the diagnostic would then read `[error] ` with no text, so such an error is named by its `_tag`.
 */
export const toolErrorMessage = <E>(cause: Cause.Cause<E>): string => {
  const message = causeMessage(cause);
  if (message !== "") return message;
  const error = Cause.squash(cause);
  return Predicate.hasProperty(error, "_tag") && Predicate.isString(error._tag)
    ? error._tag
    : message;
};

/** Arguments, or why they are invalid. */
type Prepared = { readonly args: Schema.Json } | { readonly error: string };

/** Arguments that decode, with their decoded value, or why they do not. */
type Validated =
  | { readonly args: Schema.JsonObject; readonly value: Decoded }
  | { readonly error: string };

/**
 * The call's arguments as repaired by the tool; a throwing repair is invalid. A repair that returns something
 * other than an object is left to `validate` to reject.
 */
const prepare = (tool: ErasedTool, args: Schema.JsonObject): Prepared => {
  const repair = tool.prepareArguments;
  if (repair === undefined) return { args };
  const repaired = Result.try({
    try: () => repair(args),
    catch: (error) => (error instanceof Error ? error.message : String(error)),
  });
  if (Result.isFailure(repaired)) return { error: repaired.failure };
  return { args: repaired.success };
};

/**
 * The arguments coerced towards the JSON Schema the model is offered, then decoded with the tool's `parameters`.
 *
 * The arguments are first copied as strict JSON without `undefined` properties. Schema's `optionalKey` rejects
 * an `undefined` optional property, so copying first keeps what a repair or hook may return valid. Coercion turns
 * what models often send (`"42"` for a number, a single value for an array, `null` for an omitted optional
 * property) into what the schema means; the coerced value is the value checked and stored with the intent. A
 * copy that fails (not JSON) faults the task.
 *
 * A JSON Schema may accept `null` for a property whose decoder accepts only its absence (Effect generates
 * `Schema.optional` so). When the coerced value does not decode, the same value without a `null` in any property
 * the JSON Schema does not require is tried; the error reported is the first decode's.
 */
const validate = (tool: ErasedTool, call: ToolCall, input: Schema.Json) =>
  Effect.gen(function* () {
    const jsonSchema = toolJsonSchema(tool.source);
    const copied = coerce(jsonSchema, Json.copy(input, { omitUndefinedProperties: true }));
    const decode = Schema.decodeUnknownEffect(tool.parameters);
    const exit = yield* Effect.exit(decode(copied));
    if (Exit.isSuccess(exit)) return checkObject(call, copied, exit.value);
    if (Cause.hasInterrupts(exit.cause)) return yield* Effect.interrupt;
    const withoutNulls = dropNullOptionals(jsonSchema, copied);
    if (withoutNulls !== copied) {
      const retried = yield* Effect.exit(decode(withoutNulls));
      if (Exit.isSuccess(retried)) return checkObject(call, withoutNulls, retried.value);
      if (Cause.hasInterrupts(retried.cause)) return yield* Effect.interrupt;
    }
    const error = Cause.findErrorOption(exit.cause);
    const message = Option.isSome(error)
      ? validationMessage({
          name: call.name,
          issues: SchemaIssue.makeFormatterStandardSchemaV1()(error.value.issue).issues,
          received: input,
        })
      : causeMessage(exit.cause);
    const invalid: Validated = { error: message };
    return invalid;
  });

/**
 * Decoded arguments, if they are a JSON object. `parameters` is any Schema; one that accepts a non-object still
 * leaves arguments the checkpoint and the transcript cannot store, so the call is invalid.
 */
const checkObject = (call: ToolCall, args: Schema.Json, value: Decoded): Validated =>
  Schema.is(Schema.JsonObject)(args)
    ? { args, value }
    : { error: `Tool ${call.name} arguments are not a JSON object` };

/** What a hook may use: committed reads and the task's memos, never a `Tx`. */
const hookContext = (runtime: Runtime): HookContext => ({
  taskId: runtime.taskId,
  conversationId: runtime.conversationId,
  memo: runtime.memo,
  snapshot: runtime.snapshot,
  snapshotAsOf: runtime.snapshotAsOf,
  entry: runtime.entry,
  entries: runtime.entries,
  readSignal: runtime.readSignal,
  now: runtime.now,
});

// ─── Execution ──────────────────────────────────────────────────────────────

/** Errors of the tool's own commits and reads: progress, hooks and the result. */
type ToolError = Task.RuntimeReadError | CommitError | TxDocError;

/** What `run` takes. */
interface RunInput {
  readonly runtime: Runtime;
  readonly input: ToolTaskInput;
  readonly call: ToolHookCall;
  readonly tool: ErasedTool;
  /** The phase's agent, which resolved the tool. */
  readonly agent: Agent;
  readonly args: Decoded;
  /** The arguments as stored with the intent, which a park stores again. */
  readonly stored: Schema.JsonObject;
  readonly resumed: Resumed;
}

/** What the tool reported through its `ToolContext`. */
interface Reported {
  readonly output: OutputBuffer;
  readonly limits: ResolvedOutputLimits;
  readonly diagnostics: Array<ToolDiagnostic>;
  details: Schema.Json | undefined;
  /** Set before draining; every operation checks it in the step it acts. */
  ended: boolean;
}

/** One run of the tool's code, `execute` or `cancel`, once it ended. */
interface Ran<A> {
  readonly exit: Exit.Exit<A, Failure>;
  /** This run's execution time, on a monotonic clock. */
  readonly durationMs: number;
  readonly reported: Reported;
  /** The parks the wrapped base of a composite returned in this run. */
  readonly baseParks: ReadonlySet<AgentTool.ToolPark>;
  /** `details` waits no progress commit took; the next commit, the park's or the result's, settles them. */
  readonly pending: ReadonlyArray<Progress.ProgressWaiter<ToolError>>;
}

/** What one run of the tool's code takes besides `RunInput`: whether it is `cancel`, which starts no work. */
interface AttemptInput extends RunInput {
  readonly cleanup: boolean;
}

/**
 * Runs the tool's code once, `execute` or `cancel`, with the call's `ToolContext`.
 *
 * The output `Progress` lives in an `Effect.scoped` region: it is stopped, its in-flight commit awaited, before
 * the region closes, and the region closes before the next commit, so no progress commit lands after it. A woken
 * run adds to the details and diagnostics earlier runs left in the slot, so it starts from them; a nested call
 * without progress keeps none there.
 *
 * Only the tool's code is interruptible: once it returned, the run ends its region whatever happens, so a park it
 * returned reaches its commit, whose refusal cleans up. An interruption during the code propagates.
 */
const attempt = <A>(
  input: AttemptInput,
  body: Effect.Effect<A, Failure, ToolContext>,
): Effect.Effect<Ran<A>, ToolError> =>
  Effect.gen(function* () {
    const runtime = input.runtime;
    const settings = yield* runtime.settings;
    const limits = outputLimits(input.tool);
    // A nested call made with `progress: false` commits only its slot's status.
    const streams = input.input.kind === "model" || input.input.progress !== false;
    const kept =
      input.resumed.wake !== undefined && streams
        ? yield* Effect.map(runtime.snapshot(LiveDoc, runtime.conversationId), (live) =>
            Option.isSome(live) ? toolSlot(live.value, runtime.taskId) : undefined,
          )
        : undefined;
    const reported: Reported = {
      output: new OutputBuffer(limits),
      limits,
      diagnostics: [...(kept?.diagnostics ?? [])],
      details: kept?.details,
      ended: false,
    };
    const admissions = makeAdmissions();
    const baseParks = new Set<AgentTool.ToolPark>();
    return yield* Effect.uninterruptibleMask((restore) =>
      Effect.scoped(
        Effect.gen(function* () {
          const progress = yield* Progress.make<ToolError>({
            write: publishProgress(runtime, reported, streams),
            // Failures after the invocation was signalled are expected.
            onError: (cause) =>
              signalled(cause) ? Effect.void : Effect.ignore(runtime.report(cause)),
            minIntervalMs: settings.progress.outputIntervalMs,
          });
          // Built for each run, so a rerun after recovery gets the conversation's environment at that time. A
          // failure to build it is the run's failure, and the tool's code does not run.
          const built = yield* Effect.exit(restore(runtime.env));
          if (Exit.isFailure(built) && Cause.hasInterrupts(built.cause)) {
            return yield* Effect.interrupt;
          }
          const context = toolContext({
            ...input,
            env: Exit.isSuccess(built) ? built.value : undefined,
            reported,
            progress,
            admissions,
            outputIntervalMs: settings.progress.outputIntervalMs,
            baseParks,
          });
          const startedAt = yield* Clock.monotonicTimeNanos;
          // Once the invocation is signalled, the interruption propagates; `details` waits end with it. A
          // finalizer, because an interrupted fiber runs nothing after `Effect.exit` returns.
          const interrupted = Effect.gen(function* () {
            reported.ended = true;
            for (const waiter of yield* progress.stop) waiter.interrupt();
          });
          const exit: Exit.Exit<A, Failure> = Exit.isFailure(built)
            ? Exit.failCause(built.cause)
            : yield* Effect.exit(
                restore(Effect.suspend(() => body)).pipe(
                  Effect.provideService(ToolContext, context),
                  Effect.onInterrupt(() => interrupted),
                ),
              );
          const durationMs = Math.round(
            Number((yield* Clock.monotonicTimeNanos) - startedAt) / 1e6,
          );
          // An interruption the tool raised itself, not the invocation's: it propagates the same way.
          if (Exit.isFailure(exit) && Cause.hasInterrupts(exit.cause))
            return yield* Effect.interrupt;
          // No admission starts after `ended`; those underway commit, so settlement aborts and lists their calls.
          reported.ended = true;
          yield* admissions.drain;
          reported.output.end();
          const pending = yield* progress.stop;
          const ran: Ran<A> = { exit, durationMs, reported, baseParks, pending };
          return ran;
        }),
      ),
    );
  });

/**
 * Settles the `details` waits of a run with the outcome of the commit that follows it: resolved once it holds
 * them, failed with its error, or interrupted with the invocation. A finalizer, because an interrupted fiber runs
 * nothing after `Effect.exit` returns.
 */
const settlingWaits =
  (pending: ReadonlyArray<Progress.ProgressWaiter<ToolError>>) =>
  <A, E extends ToolError, R>(effect: Effect.Effect<A, E, R>): Effect.Effect<A, E, R> =>
    effect.pipe(
      Effect.onExit((settled) =>
        Effect.sync(() => {
          const error = Exit.isFailure(settled)
            ? Cause.findErrorOption(settled.cause)
            : Option.none();
          for (const waiter of pending) {
            if (Exit.isSuccess(settled)) waiter.resolve();
            else if (Option.isSome(error)) waiter.reject(error.value);
            else waiter.interrupt();
          }
        }),
      ),
    );

/**
 * Executes with the resolved implementation, then settles its result or parks the call. A failure or a defect
 * becomes a `tool_error` result and ends the task `failed`, which cancels what the call owned; a returned
 * `isError` completes. A park that breaks a rule ends the call with an `invalid_park` result, `failed`.
 */
const run = (input: RunInput) =>
  Effect.uninterruptibleMask((restore) =>
    Effect.gen(function* () {
      const runtime = input.runtime;
      const call = input.call;
      // Only the tool's code and the settlement are interruptible: a park the code returned reaches its commit.
      const ran = yield* attempt(
        { ...input, cleanup: false },
        restore(input.tool.execute(input.args)),
      );
      yield* Effect.gen(function* () {
        let result: ToolExecutionResult;
        let ending: ToolEnding;
        if (Exit.isSuccess(ran.exit)) {
          const returned = ran.exit.value;
          if (AgentTool.isPark(returned)) {
            const invalid = yield* park(input, returned, ran);
            if (invalid === undefined) return;
            result = harnessError(
              "invalid_park",
              `Tool ${call.name} returned an invalid park: ${invalid}`,
            );
            ending = { status: "failed", message: `Tool ${call.name} returned an invalid park` };
          } else {
            result = returned;
            ending = COMPLETED;
          }
        } else {
          result = {
            isError: true,
            diagnostics: [toolDiagnostic("tool_error", toolErrorMessage(ran.exit.cause))],
          };
          ending = { status: "failed", message: `Tool ${call.name} threw` };
        }
        yield* restore(
          Effect.gen(function* () {
            const final = yield* finalResult(input, result, ran.reported);
            yield* settle({
              runtime,
              input: input.input,
              call,
              ending,
              result: final,
              durationMs: ran.durationMs,
            });
          }),
        );
      }).pipe(settlingWaits(ran.pending));
    }),
  ).pipe(
    // One span per tool call, identity only.
    Effect.withSpan("durable.tool.call", {
      attributes: {
        "durable.task.id": input.runtime.taskId,
        "durable.tool.name": input.call.name,
        "durable.tool.call_id": input.call.id,
      },
    }),
  );

/** Fields of a result; a park carrying one of them is invalid. */
const RESULT_FIELDS = [
  "output",
  "structuredOutput",
  "isError",
  "details",
  "diagnostics",
  "usage",
  "control",
] as const;

/**
 * Why a park breaks its shape, or `undefined`: exactly one of `sleep` and `wait`, no result field, and for a
 * wrapper's composite, a park its base returned in this run. Rules on what it waits for are the commit's.
 */
const parkError = (
  tool: ErasedTool,
  returned: AgentTool.ToolPark,
  baseParks: ReadonlySet<AgentTool.ToolPark>,
): string | undefined => {
  // Tool code is a trust boundary for the mixed objects its types forbid.
  if ("sleep" in returned && "wait" in returned) return "a park has either sleep or wait, not both";
  const field = RESULT_FIELDS.find((name) => name in returned);
  if (field !== undefined) return `a park has no result field, but this one has ${field}`;
  if (isComposite(tool.source) && !baseParks.has(returned)) {
    return "a wrapper returned a park its base did not return";
  }
  return undefined;
};

/** Whether a park's commit was refused for what it waits for, which makes the park invalid. */
const refusedPark = (error: ToolError): boolean =>
  error._tag === "CommitRejected" ||
  (error._tag === "TaskError" &&
    (error.reason._tag === "InvalidSleep" ||
      error.reason._tag === "InvalidWait" ||
      error.reason._tag === "NotOwned" ||
      error.reason._tag === "NotFound"));

/**
 * Parks the call. One commit persists the details and diagnostics the run reported, shows the slot `sleeping` or
 * `waiting` without output, and stores the `parked` checkpoint with the park's sleep or wait; the call's live
 * nested calls keep running. Returns why the park is invalid instead, when it is. When the commit is refused
 * because the call was abort-marked meanwhile, `cancel` cleans up what the run started, with the park's checkpoint,
 * before the error ends the run; a close leaves the run to recovery.
 */
const park = (input: RunInput, returned: AgentTool.ToolPark, ran: Ran<unknown>) =>
  Effect.gen(function* () {
    const runtime = input.runtime;
    const reported = ran.reported;
    const malformed = parkError(input.tool, returned, ran.baseParks);
    if (malformed !== undefined) return malformed;
    const sleeping = returned.sleep;
    const waiting = returned.wait;
    if (waiting !== undefined) {
      for (const id of waiting.on) {
        const record = Option.getOrUndefined(yield* runtime.getTask(id));
        if (record?.owner !== runtime.taskId) return `task ${id} is not owned by the call`;
      }
    }
    const checkpoint = sleeping !== undefined ? sleeping.checkpoint : waiting?.checkpoint;
    const parked: ParkedCheckpoint = {
      phase: "parked",
      arguments: input.stored,
      replay: input.tool.replay ?? "unsafe",
      ...(checkpoint === undefined ? undefined : { checkpoint }),
    };
    const streams = input.input.kind === "model" || input.input.progress !== false;
    const committed = yield* Effect.exit(
      runtime.commit(() =>
        Effect.gen(function* () {
          const tx = yield* Tx;
          const slot = toolSlot(yield* tx.doc(LiveDoc, runtime.conversationId), runtime.taskId);
          if (slot !== undefined) {
            if (streams && reported.details !== undefined) {
              // SAFETY: a slot draft is a JSON object; `details` is one of its JSON fields.
              assignJson(slot as { [key: string]: Schema.Json }, "details", reported.details);
            }
            if (streams && reported.diagnostics.length > 0) {
              // SAFETY: a slot draft is a JSON object; `diagnostics` is one of its JSON fields.
              assignJson(slot as { [key: string]: Schema.Json }, "diagnostics", [
                ...reported.diagnostics,
              ]);
            }
            clearOutput(slot);
            slot.status = sleeping !== undefined ? "sleeping" : "waiting";
          }
          if (sleeping !== undefined) {
            // oxlint-disable-next-line durable/no-external-in-commit -- builds the sleeping state; nothing waits.
            return Calls.sleep({
              checkpoint: parked,
              until: sleeping.until,
              signals: sleeping.signals,
            });
          }
          // SAFETY: `parkError` checked that a park without `sleep` has `wait`.
          const wait = waiting as AgentTool.ToolWait;
          return Calls.waitFor(wait.on, parked, wait.policy);
        }),
      ),
    );
    if (Exit.isSuccess(committed)) return undefined;
    const error = Cause.findErrorOption(committed.cause);
    if (Option.isSome(error) && refusedPark(error.value)) return error.value.message;
    if (
      Option.isSome(error) &&
      error.value._tag === "TaskError" &&
      error.value.reason._tag === "AbortMarked"
    ) {
      // Abort-marked meanwhile: the park is not stored, so clean up with its checkpoint now. The invocation is
      // signalled, so the cleanup runs uninterruptibly; only it holds the abort handler back.
      yield* Effect.uninterruptible(
        cancel({
          runtime,
          input: input.input,
          call: input.call,
          tool: input.tool,
          agent: input.agent,
          stored: input.stored,
          checkpoint,
        }),
      );
    }
    return yield* Effect.failCause(committed.cause);
  });

/** What `cancel` takes: the call, its resolved tool and agent, the stored arguments and the last checkpoint. */
interface CancelInput {
  readonly runtime: Runtime;
  readonly input: ToolTaskInput;
  readonly call: ToolHookCall;
  readonly tool: ErasedTool;
  readonly agent: Agent;
  readonly stored: Schema.JsonObject;
  readonly checkpoint: Schema.Json | undefined;
}

/**
 * Runs the tool's `cancel`, if it has one, to clean up the external work of a call that parked: its context's
 * `checkpoint` is the last park's, `wake` is `undefined`, and it starts no nested calls or tasks. A failure is
 * reported, naming the tool's extension; the call ends either way. Arguments the tool's schema no longer
 * decodes leave nothing to call it with, which is reported too.
 */
const cancel = (input: CancelInput) =>
  Effect.gen(function* () {
    const runtime = input.runtime;
    const tool = input.tool;
    if (!(yield* tool.hasCancel)) return;
    const validated = yield* validate(tool, input.call, input.stored);
    if ("error" in validated) {
      return yield* reportToolFailure(
        runtime,
        input.agent,
        tool,
        Cause.fail(new Error(`Tool ${tool.name} cannot cancel: ${validated.error}`)),
      );
    }
    const ran = yield* attempt(
      {
        ...input,
        args: validated.value,
        resumed: resumedOf(undefined, input.checkpoint),
        cleanup: true,
      },
      tool.cancel(validated.value),
    );
    for (const waiter of ran.pending) waiter.resolve();
    if (Exit.isFailure(ran.exit)) {
      yield* reportToolFailure(runtime, input.agent, tool, ran.exit.cause);
    }
  });

/** Reports a failure of the tool's own code to the host, naming the extension that provides the tool. */
const reportToolFailure = (
  runtime: Runtime,
  agent: Agent,
  tool: ErasedTool,
  cause: Cause.Cause<unknown>,
) => runtime.report(cause, { kind: "extension", extension: toolExtension(agent, tool.name) });

/** What `toolContext` takes. */
interface ToolContextInput extends AttemptInput {
  readonly env: ExecutionEnv | undefined;
  readonly reported: Reported;
  readonly progress: Progress.Progress<ToolError>;
  readonly admissions: Admissions;
  /** The settings' output pace when the call started executing. */
  readonly outputIntervalMs: number;
  /** Where a composite's base records the parks it returns in this run. */
  readonly baseParks: Set<AgentTool.ToolPark>;
}

/**
 * The `ToolContext` service of one call.
 *
 * Output, retained output, diagnostics, `details` and nested calls check `ended`. The rest is the runtime's,
 * gated by the invocation: a fiber the tool left running may still commit until the call's result ends it. No
 * `registry` and `models` (services in `R`).
 */
const toolContext = (input: ToolContextInput): ToolContext["Service"] & ParkCarrier => {
  const runtime = input.runtime;
  const reported = input.reported;
  const progress = input.progress;
  // A caller that can rerun reattaches to its child tasks after a restart; any other
  // caller never resumes, so they are abandoned.
  const resumes = input.tool.replay === "safe";
  const live = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
    Effect.suspend(
      (): Effect.Effect<A, E | TaskError, R> =>
        reported.ended ? Effect.fail(settledError(input.call)) : effect,
    );
  const memo = runtime.memo;
  const conversation = runtime.conversation;
  const wake = input.resumed.wake;
  // The parks a composite's base returns in this run are recorded under `BASE_PARKS`.
  return {
    [BASE_PARKS]: input.baseParks,
    ...ToolContext.of({
      taskId: runtime.taskId,
      conversationId: runtime.conversationId,
      callId: input.call.id,
      wake,
      checkpoint: input.resumed.checkpoint,
      readSignal: runtime.readSignal,
      agent: Effect.succeed(input.agent),
      env: input.env,
      // Check and push in one step, so no chunk is accepted after the call ended.
      output: (chunk, skipped) =>
        Effect.suspend(() => {
          if (reported.ended) return Effect.fail(settledError(input.call));
          return reported.output.push(chunk, skipped) ? progress.mark : Effect.void;
        }),
      // Only a tool that keeps the tail of its output offers a window.
      outputWindow:
        reported.limits.retain === "tail"
          ? {
              maxBytes: reported.limits.maxBytes,
              maxLines: reported.limits.maxLines,
              minIntervalMs: input.outputIntervalMs,
              bytesPerSecond: PROGRESS_BYTES_PER_SECOND,
            }
          : undefined,
      retainedOutput: live(
        Effect.sync(() => {
          const retained = reported.output.snapshot();
          return { text: retained.text, truncated: retained.droppedBytes > 0 };
        }),
      ),
      diagnostic: (diagnostic) =>
        Effect.suspend(() => {
          if (reported.ended) return Effect.fail(settledError(input.call));
          reported.diagnostics.push(copyDiagnostic(diagnostic));
          return progress.mark;
        }),
      // The `ended` check, the update and the wait's registration are one step: a wait
      // registered after the handler's `progress.stop` would never be settled by its terminal commit.
      details: (value) =>
        progress
          .markAndWait(() => {
            if (reported.ended) return settledError(input.call);
            // A copy, so the tool may change its value afterwards; a value that is not JSON is a defect.
            reported.details = Json.copy(value, { omitUndefinedProperties: true });
            return undefined;
          })
          .pipe(
            // `ToolContext.details` fails with `TaskError | CommitError`. A request error or a document access
            // error of the progress commit fails the tool's wait as a defect, which, unhandled, becomes the call's
            // `tool_error` result.
            Effect.catchTags({
              StorageRequestError: (error) => Effect.die(error),
              DocumentAccessError: (error) => Effect.die(error),
            }),
          ),
      commit: <A, E>(change: Effect.Effect<A, E, Tx>, options?: CommitOptions) =>
        Effect.gen(function* () {
          let committed: { readonly value: A } | undefined;
          yield* runtime.commit(
            () =>
              Effect.map(change, (value) => {
                committed = { value };
              }),
            options,
          );
          if (committed === undefined) return yield* Effect.die(new Error("Commit ran no change"));
          return committed.value;
        }),
      memo,
      // The key, the step and `requireConversation` are the runtime's, bound to the call.
      key: (name) => Calls.keyOf(runtime.taskId, name),
      step: Calls.makeStep(memo),
      requireConversation: (id) => Calls.requireConversation(conversation, id),
      createTask: <I extends Schema.Json, S extends { readonly phase: string }, R, Req>(
        task: Task.Task<I, S, R, Req>,
        taskInput: I,
        { key, ...options }: AgentTool.CreateTaskOptions,
      ) =>
        Effect.gen(function* () {
          if (input.cleanup) return yield* refusedInCancel(input.call);
          const invalid = key === undefined ? undefined : checkKey(key);
          if (invalid !== undefined) return yield* new InvalidNestedCall({ message: invalid });
          if (key === undefined && wake !== undefined) return yield* needsKeys(input.call);
          // A child of a tool that is not replay-safe is abandoned on restart by default.
          const child: Task.TaskOptions =
            options.ownership.kind === "task" && !resumes
              ? { abandonOnRestart: true, ...options }
              : options;
          let id: TaskId<R> | undefined;
          yield* runtime.commit(() =>
            Effect.gen(function* () {
              const tx = yield* Tx;
              if (key === undefined) {
                id = yield* tx.createTask(task, taskInput, child);
                return;
              }
              // Opened, or created empty for a key not used yet.
              const member = yield* tx.doc(NestedCallDoc, runtime.taskId, key, null);
              const existing = member.taskId;
              if (existing !== undefined) {
                const record =
                  member.kind === "task"
                    ? Option.getOrUndefined(yield* tx.task(existing))
                    : undefined;
                const same =
                  record?.kind === task.definition.name &&
                  jsonEqual(record.input, Json.copy(taskInput, { omitUndefinedProperties: true }));
                if (!same) {
                  return yield* new InvalidNestedCall({
                    message: `Key ${key} of tool call ${input.call.id} was already used for another call or task`,
                  });
                }
                // SAFETY: the task under the key was created below, from a task of this kind.
                id = existing as TaskId<R>;
                return;
              }
              const created = yield* tx.createTask(task, taskInput, child);
              member.kind = "task";
              member.taskId = created;
              id = created;
            }).pipe(
              // The member's document is read before anything is written, and the task table after it only when
              // the member exists, which wrote nothing.
              Effect.catchTags({
                DocumentAccessError: (error) => Effect.die(error),
                ReadAfterWrite: (error) => Effect.die(error),
              }),
            ),
          );
          if (id === undefined) return yield* Effect.die(new Error("Task creation ran no change"));
          return id;
        }),
      getTask: runtime.getTask,
      awaitTask: runtime.awaitTask,
      conversation,
      snapshot: runtime.snapshot,
      snapshotAsOf: runtime.snapshotAsOf,
      watchDoc: runtime.watchDoc,
      executeTool: nestedCalls(input),
    }),
  };
};

// ─── Nested calls ───────────────────────────────────────────────────────────

/**
 * Nested call admissions an invocation started: the run waits for them once
 * the call ended, so its settlement sees every nested call.
 */
interface Admissions {
  /** Registers one admission; call in the same synchronous step that checked the call is live. */
  readonly track: <A, E, R>(admission: Effect.Effect<A, E, R>) => Effect.Effect<A, E, R>;
  /** Waits until no admission is underway. */
  readonly drain: Effect.Effect<void>;
}

const makeAdmissions = (): Admissions => {
  let pending = 0;
  let drained: Deferred.Deferred<void> | undefined;
  return {
    track: (admission) => {
      pending++;
      return admission.pipe(
        Effect.onExit(() =>
          Effect.sync(() => {
            pending--;
            if (pending === 0 && drained !== undefined) doneAfterYield(drained, Effect.void);
          }),
        ),
      );
    },
    drain: Effect.suspend(() => {
      if (pending === 0) return Effect.void;
      const wait = Deferred.makeUnsafe<void>();
      drained = wait;
      return Deferred.await(wait);
    }),
  };
};

/**
 * Runs tool `name` as a nested call of this one and waits for its result.
 *
 * - The admission (the memo, then the creating commit) is uninterruptible, and the wait after it is not:
 *   interrupting the caller stops only the wait, and an admission underway always ends, so the run's settlement
 *   sees it.
 * - Given a tool token, the stored value is decoded with its `structuredOutput` schema for a typed read.
 */
const nestedCalls = (input: ToolContextInput): ToolContext["Service"]["executeTool"] => {
  const runtime = input.runtime;
  const reported = input.reported;
  const call = input.call;
  // A caller that can rerun reattaches to its nested calls after a restart; any other caller
  // never resumes, so they are abandoned.
  const resumes = input.tool.replay === "safe";
  // Default keys: the order of this invocation's calls, so a rerun that calls in the same
  // order reattaches.
  let sequence = 0;
  // This attempt's memo of `MADE_NESTED_CALLS`, committed once before its first admission.
  let madeNested = false;
  const memo = Effect.suspend(() =>
    madeNested
      ? Effect.void
      : Effect.map(runtime.memo(MADE_NESTED_CALLS, true), () => {
          madeNested = true;
        }),
  );
  const execute = (
    target: string | AgentTool.NamedTool<Schema.Json>,
    args: Schema.JsonObject,
    options: ExecuteToolOptions = {},
  ) =>
    Effect.uninterruptibleMask((restore) =>
      Effect.suspend(() => {
        // Check and register in one step: no admission starts once the call ended.
        if (reported.ended) return Effect.fail(settledError(call));
        if (input.cleanup) return refusedInCancel(call);
        const name = Predicate.isString(target) ? target : target.name;
        const invalid = options.key === undefined ? undefined : checkKey(options.key);
        if (invalid !== undefined) return Effect.fail(new InvalidNestedCall({ message: invalid }));
        // After a wake the same position may name another call.
        if (options.key === undefined && input.resumed.wake !== undefined) return needsKeys(call);
        const key = options.key ?? String(++sequence);
        const admission = input.admissions.track(
          Effect.andThen(
            memo,
            startNestedCall(runtime, {
              parentCallId: call.id,
              name,
              args,
              key,
              progress: options.progress,
              abandonOnRestart: !resumes,
            }),
          ),
        );
        return Effect.flatMap(
          admission,
          (
            id,
          ): Effect.Effect<
            NestedToolExecutionResult | AgentTool.NestedToolCall<Schema.Json, Schema.SchemaError>,
            AgentTool.ExecuteToolError | Schema.SchemaError
          > => {
            const result = awaitNested(runtime, { id, name, key, target });
            if (options.wait !== false) return restore(result);
            const handle: AgentTool.NestedToolCall<Schema.Json, Schema.SchemaError> = {
              taskId: id,
              result,
            };
            return Effect.succeed(handle);
          },
        );
      }),
    );
  // SAFETY: `execute` implements both overloads of `ExecuteTool`: a token's value is decoded with its schema.
  return execute as ToolContext["Service"]["executeTool"];
};

/** What `awaitNested` reads: the nested task, its tool's name, its key, and what the caller named. */
interface NestedWait {
  readonly id: TaskId<ToolTaskResult>;
  readonly name: string;
  readonly key: string;
  readonly target: string | AgentTool.NamedTool<Schema.Json>;
}

/**
 * Waits for nested call `id` and returns its stored result, or, for a call the
 * scheduler faulted or orphaned, which stored none, the fallback built from its outcome.
 */
const awaitNested = (runtime: Runtime, wait: NestedWait) =>
  Effect.gen(function* () {
    const settled = yield* runtime.awaitTask(wait.id);
    const state = yield* runtime.snapshot(NestedCallDoc, runtime.taskId, wait.key);
    const stored = Option.getOrUndefined(state)?.result;
    const result: NestedToolExecutionResult =
      stored ?? fallbackResult(wait.id, wait.name, settled.state.outcome);
    const schema = Predicate.isString(wait.target)
      ? undefined
      : wait.target.definition.structuredOutput;
    if (schema === undefined || result.structuredOutput === undefined) return result;
    const value = yield* Schema.decodeUnknownEffect(schema)(result.structuredOutput);
    return { ...result, structuredOutput: value };
  });

/** What `startNestedCall` takes. */
interface NestedCallInput {
  readonly parentCallId: string;
  readonly name: string;
  readonly args: Schema.JsonObject;
  readonly key: string;
  readonly progress: boolean | undefined;
  readonly abandonOnRestart: boolean;
}

/**
 * Creates nested call `key` of the call `parentCallId` in one
 * commit: its tool task, owned by the calling task, its slot, and its `NestedCallDoc`. A key already made returns
 * that call's task when it names the same tool and arguments.
 */
// The return type is declared: `ToolTask`'s own type depends on its handlers, which reach this function.
const startNestedCall = (
  runtime: Runtime,
  nested: NestedCallInput,
): Effect.Effect<
  TaskId<ToolTaskResult>,
  TaskError | CommitError | TxDocError | TxReadError | InvalidNestedCall
> =>
  Effect.gen(function* () {
    const key = nested.key;
    const callId = `${nested.parentCallId}/${key}`;
    // SAFETY: a strict-JSON copy of a JSON object is a JSON object.
    const args = Json.copy(nested.args, { omitUndefinedProperties: true }) as Schema.JsonObject;
    const call: ToolCall = { type: "toolCall", id: callId, name: nested.name, arguments: args };
    let id: TaskId<ToolTaskResult> | undefined;
    let replayed = false;
    yield* runtime.commit(() =>
      Effect.gen(function* () {
        const tx = yield* Tx;
        // Opened, or created empty for a key not made yet.
        const state = yield* tx.doc(NestedCallDoc, runtime.taskId, key, null);
        const existing = state.taskId;
        if (existing !== undefined) {
          // A key a keyed task uses names no nested call.
          const record =
            state.kind === "task" ? undefined : Option.getOrUndefined(yield* tx.task(existing));
          const made = record === undefined ? undefined : record.input;
          const same =
            isNestedInput(made) &&
            made.call.name === nested.name &&
            jsonEqual(made.call.arguments, call.arguments);
          if (!same) {
            return yield* new InvalidNestedCall({
              message: `Nested call ${callId} was already made with another tool or other arguments`,
            });
          }
          // SAFETY: the nested call's task is a tool task, created below by an earlier admission.
          id = existing as TaskId<ToolTaskResult>;
          replayed = state.result !== undefined;
          return;
        }
        const taskInput: ToolTaskInput =
          nested.progress === false
            ? {
                kind: "nested",
                parent: runtime.taskId,
                parentCallId: nested.parentCallId,
                key,
                call,
                progress: false,
              }
            : {
                kind: "nested",
                parent: runtime.taskId,
                parentCallId: nested.parentCallId,
                key,
                call,
              };
        // For a caller that resumes, `abandonOnRestart` is left unset rather than `false`, so the stored record
        // has none.
        const created = yield* tx.createTask(
          ToolTask,
          taskInput,
          nested.abandonOnRestart
            ? { ownership: { kind: "task", taskId: runtime.taskId }, abandonOnRestart: true }
            : { ownership: { kind: "task", taskId: runtime.taskId } },
        );
        state.kind = "nested";
        state.taskId = created;
        const live = yield* tx.doc(LiveDoc, runtime.conversationId);
        const slot: NestedToolSlot = {
          callId,
          parentCallId: nested.parentCallId,
          parentTaskId: runtime.taskId,
          name: nested.name,
          taskId: created,
          arguments: args,
          status: "pending",
        };
        if (live.nestedTools === undefined) live.nestedTools = [];
        // SAFETY: a fresh strict-JSON slot; the draft takes it as its own value.
        live.nestedTools.push(slot as Tracker.Draft<NestedToolSlot>);
        id = created;
      }),
    );
    if (id === undefined)
      return yield* Effect.die(new Error("Nested call admission ran no change"));
    if (replayed) yield* replayEvent(runtime, nested.name);
    return id;
  });

/**
 * A nested call that returns a result an earlier attempt stored is a replay, recorded
 * on the current span. A tracer that throws is reported, never the call's failure.
 */
const replayEvent = (runtime: Runtime, name: string) =>
  Effect.gen(function* () {
    const span = yield* Effect.option(Effect.currentSpan);
    let thrown: Error | undefined;
    telemetry.event(
      Option.getOrUndefined(span),
      Events.replay,
      { "durable.tool.name": name },
      (error) => {
        thrown ??= error;
      },
    );
    if (thrown !== undefined) {
      yield* runtime
        .report(Cause.die(thrown), { kind: "host", callback: "telemetry" })
        .pipe(Effect.ignore);
    }
  });

/** The failure of a nested call or task started from `cancel`. */
const refusedInCancel = (call: ToolCall) =>
  Effect.fail(
    new InvalidNestedCall({
      message: `Tool ${call.name} cannot start nested calls or tasks in cancel`,
    }),
  );

/** The failure of a nested call or task without an explicit key after a wake. */
const needsKeys = (call: ToolCall) =>
  Effect.fail(
    new InvalidNestedCall({ message: `Tool call ${call.id} needs explicit keys after a wake` }),
  );

/** The failure of an operation on a call that has settled. */
const settledError = (call: ToolCall) =>
  new TaskError({ reason: new InvocationEnded({ message: `Tool call ${call.id} has settled` }) });

/** A strict-JSON copy of a diagnostic, without an absent `code`. */
const copyDiagnostic = (diagnostic: ToolDiagnostic): ToolDiagnostic =>
  diagnostic.code === undefined
    ? { severity: diagnostic.severity, message: diagnostic.message }
    : { severity: diagnostic.severity, message: diagnostic.message, code: diagnostic.code };

/**
 * Whether a progress commit failed because the invocation was signalled: interrupted, abort-marked, ended or
 * closing.
 *
 * The runtime has no abort signal, so the failures a signalled invocation's commits get stand for it.
 */
const signalled = (cause: Cause.Cause<ToolError>): boolean => {
  if (Cause.hasInterruptsOnly(cause)) return true;
  const error = Cause.findErrorOption(cause);
  if (Option.isNone(error)) return false;
  if (error.value._tag === "SessionClosed") return true;
  return (
    error.value._tag === "TaskError" &&
    (error.value.reason._tag === "AbortMarked" || error.value.reason._tag === "InvocationEnded")
  );
};

/** What the last progress commit wrote: the retained text, the details, and how many diagnostics. */
interface Published {
  readonly text: string;
  readonly details: Schema.Json | undefined;
  readonly diagnostics: number;
}

/**
 * Throttled commits of what the tool reported into its slot, in
 * `core.live.tools` or `core.live.nestedTools`, each writing only what changed since the last one; none when
 * `enabled` is false. Called in the flushing step: everything is captured there, since the tool keeps reporting
 * while the commit is in flight.
 */
const publishProgress = (runtime: Runtime, reported: Reported, enabled: boolean) => {
  // A woken run starts from the details and diagnostics already in the slot, so only what it adds is written.
  let written: Published = {
    text: "",
    details: reported.details,
    diagnostics: reported.diagnostics.length,
  };
  return (): Effect.Effect<number, ToolError> => {
    if (!enabled) return Effect.succeed(0);
    const snapshot = reported.output.snapshot();
    const current: Published = {
      text: snapshot.text,
      details: reported.details,
      diagnostics: reported.diagnostics.length,
    };
    const added = reported.diagnostics.slice(written.diagnostics, current.diagnostics);
    const detailsChanged = current.details !== written.details;
    // What the commit writes, as the delta diffs the string: an append, a trim plus an
    // append of what follows the shared part, or the whole window when the bounded overlap search finds
    // nothing.
    let bytes = 0;
    if (snapshot.text !== written.text) {
      const shared = snapshot.text.startsWith(written.text)
        ? written.text.length
        : Delta.overlap(written.text, snapshot.text, { scan: 65_536 });
      bytes += utf8ByteLength(snapshot.text.slice(shared));
    }
    if (detailsChanged) bytes += utf8ByteLength(JSON.stringify(current.details ?? null));
    if (added.length > 0) bytes += utf8ByteLength(JSON.stringify(added));
    return runtime
      .commit(
        () =>
          Effect.gen(function* () {
            const tx = yield* Tx;
            const slot = toolSlot(yield* tx.doc(LiveDoc, runtime.conversationId), runtime.taskId);
            if (slot === undefined) return;
            // `output` is assigned as one string field, which the delta diffs into an
            // append, or a trim plus an append for a sliding tail; replacing the slot would record the whole
            // window on every commit.
            if ((slot.output ?? "") !== snapshot.text) slot.output = snapshot.text;
            if (snapshot.droppedBytes > 0) slot.droppedBytes = snapshot.droppedBytes;
            if (snapshot.droppedLines > 0) slot.droppedLines = snapshot.droppedLines;
            // Details diffed leaf by leaf and new diagnostics appended, so each commit writes only what changed.
            if (detailsChanged && current.details !== undefined) {
              // SAFETY: a slot draft is a JSON object; `details` is one of its JSON fields.
              assignJson(slot as { [key: string]: Schema.Json }, "details", current.details);
            }
            if (added.length > 0) {
              if (slot.diagnostics === undefined) slot.diagnostics = [];
              for (const diagnostic of added) slot.diagnostics.push(diagnostic);
            }
          }),
        // Deferred, stored with the next commit that reads `core.live`.
        PROGRESS_COMMIT,
      )
      .pipe(
        Effect.map(() => {
          written = current;
          return bytes;
        }),
      );
  };
};

/**
 * The tool's result with the retained output and last details as
 * fallbacks, its diagnostics after those reported through the `ToolContext`, `afterTool` applied, explicit
 * text bounded, and `structuredOutput` checked against the tool's schema (`checkStructured`), with the
 * Harness's diagnostics last. A model-issued call whose value breaks the contract only loses it, and the host
 * gets an `extension_error` report naming the tool's extension.
 */
const finalResult = (input: RunInput, result: ToolExecutionResult, reported: Reported) =>
  Effect.gen(function* () {
    const runtime = input.runtime;
    const call = input.call;
    const merged = mergeReported(result, {
      snapshot: () => reported.output.snapshot(),
      details: reported.details,
      diagnostics: reported.diagnostics,
    });
    let final = merged.result;
    const context = hookContext(runtime);
    yield* runtime.hooks.each<ToolHooks["afterTool"], never, never>("afterTool", (hook) =>
      Effect.map(hook(call, final, context), (replaced) => {
        final = replaced ?? final;
      }),
    );
    const checked = checkStructured({
      kind: input.input.kind,
      tool: input.tool,
      result: boundResult({ merged, final, limits: reported.limits }),
    });
    if (checked.broken !== undefined && input.input.kind === "model") {
      // The model never sees structured output, so its call stands; the host learns of the broken tool, with
      // the extension that provides it. The phase's agent resolved the tool, so the name is found without a
      // second resolution, which may fail.
      yield* runtime.report(
        Cause.fail(new InvalidStructuredOutput({ tool: call.name, message: checked.broken })),
        {
          kind: "extension",
          extension: toolExtension(input.agent, call.name),
        },
      );
    }
    return checked.result;
  });

// ─── Settlement ─────────────────────────────────────────────────────────────

/**
 * What `settle` takes: the call, how its task ends, its result, and the last run's execution time, used only for a
 * record without `startedAt`.
 */
interface Settlement {
  readonly runtime: Runtime;
  readonly input: ToolTaskInput;
  readonly call: ToolHookCall;
  readonly ending: ToolEnding;
  readonly result: ToolExecutionResult;
  readonly durationMs?: number | undefined;
}

/**
 * Commits the call's terminal state and marks its slot done. A model-issued
 * call appends its result entry and ends with the entry ID; a nested call writes its result to the caller's
 * `NestedCallDoc`, records its usage, and ends with a small receipt. Nested calls this call left running are
 * aborted first, so they report into their slots before the slots below the call leave `core.live`. `control`
 * applies only when the task completes.
 */
const settle = (settlement: Settlement) =>
  Effect.gen(function* () {
    const runtime = settlement.runtime;
    const input = settlement.input;
    const call = settlement.call;
    const result = settlement.result;
    // A crash in between leaves the call in `execute`: a safe rerun reattaches to the calls,
    // an interruption ends here. Every live nested call the task owns, also those an earlier attempt made and
    // this one never called again.
    const nestedIds = (yield* runtime.ownedTasks)
      .filter((record) => record.kind === TOOL_TASK && isNestedInput(record.input))
      .map((record) => record.id);
    // Read before the settling commit: no admission starts after `ended`, and those underway were awaited.
    const madeNested =
      Option.getOrUndefined(yield* runtime.memo<boolean>(MADE_NESTED_CALLS)) === true;
    // Mark them all before waiting for any: one may run until a sibling is cancelled.
    yield* Effect.forEach(nestedIds, (id) => runtime.abortOwned(id), {
      concurrency: "unbounded",
      discard: true,
    });
    yield* runtime.commit((current) =>
      Effect.gen(function* () {
        const tx = yield* Tx;
        const conversationId = runtime.conversationId;
        const timestamp = yield* runtime.now;
        // The call's whole span, parks, reruns and downtime included; a record from before lifecycle times keeps
        // the last run's execution time.
        const durationMs =
          current.startedAt === undefined ? settlement.durationMs : timestamp - current.startedAt;
        const live = yield* tx.doc(LiveDoc, conversationId);
        const slot = toolSlot(live, runtime.taskId);
        let settled: ToolTaskResult;
        if (input.kind === "nested") {
          const nested = nestedResult(runtime.taskId, result, durationMs);
          if (nested.usage !== undefined) {
            yield* recordUsage(conversationId, "tools", call.name, nested.usage);
          }
          // The caller is never terminal before its owned nested calls, so its document is
          // open; another task in it is a bug, not a race.
          const state = yield* tx.doc(NestedCallDoc, input.parent, input.key, null);
          if (state.taskId !== runtime.taskId) {
            return yield* Effect.die(
              new Error(`Nested call ${call.id} is not in its caller's index`),
            );
          }
          // SAFETY: a fresh strict-JSON copy (`nestedResult`); the draft takes it as its own value.
          state.result = nested as Tracker.Draft<NestedToolExecutionResult>;
          if (slot !== undefined && isNestedSlot(slot)) {
            finishSlot(slot);
            slot.summary = summaryOf(nested, result.output ?? []);
          }
          settled = { kind: "nested" };
        } else {
          const entry = yield* appendToolResult(conversationId, call, result, {
            timestamp,
            durationMs,
          });
          if (slot !== undefined && !isNestedSlot(slot)) {
            finishSlot(slot);
            slot.entry = entry.id;
          }
          // Tools build control objects freely; keys set to undefined are dropped, so the
          // task result is strict JSON. Only a completed call requests controls.
          settled =
            result.control === undefined || settlement.ending.status !== "completed"
              ? { kind: "model", entryId: entry.id }
              : { kind: "model", entryId: entry.id, control: copyControl(result.control) };
        }
        // A call that made no nested calls, in any attempt, has no slots below it; skipping
        // the scan keeps many leaves linear.
        if (madeNested) removeNestedSlots(live, runtime.taskId);
        return toolOutcome(settlement.ending, settled);
      }),
    );
  });

type ControlDraft = { -readonly [K in keyof ToolControl]: ToolControl[K] };

const copyControl = (control: ToolControl): ToolControl => {
  const copy: ControlDraft = {};
  if (control.addTools !== undefined) copy.addTools = [...control.addTools];
  if (control.terminate !== undefined) copy.terminate = control.terminate;
  if (control.handoff !== undefined) copy.handoff = control.handoff;
  return copy;
};

/** Options of `appendToolResult`. */
export interface AppendToolResultOptions {
  readonly timestamp: number;
  readonly durationMs?: number | undefined;
}

/**
 * Appends a `core.tool-result` entry whose content ends with the rendered diagnostics, so the stored message is
 * exactly what the model sees; `data` keeps the structured list. A result's usage is added to `core.usage` in the
 * same commit.
 */
export const appendToolResult = (
  conversationId: ConversationId,
  call: ToolCall,
  result: ToolExecutionResult,
  options: AppendToolResultOptions,
): Effect.Effect<
  TypedEntry<{ readonly diagnostics: ReadonlyArray<ToolDiagnostic> }>,
  TxDocError,
  Tx
> =>
  Effect.gen(function* () {
    const tx = yield* Tx;
    const diagnostics = [...(result.diagnostics ?? [])];
    const message = toolResultMessage({ call, result, timing: options });
    if (result.usage !== undefined) {
      yield* recordUsage(conversationId, "tools", call.name, result.usage);
    }
    return yield* tx.appendEntry(ToolResultEntry, conversationId, {
      model: [message],
      data: { diagnostics },
    });
  });

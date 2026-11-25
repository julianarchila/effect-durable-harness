// The pure decisions of the tool task: the replay rule on recovery, the abort result,
// the outcome a settled call commits, and how a result is built: from what the tool reported, from a slot's
// durable partials, or by the Harness itself. Each runs in one synchronous step between the loads and the
// writes of its commit.
import * as Delta from "@effect-durable-harness/delta/Delta";
import * as Json from "@effect-durable-harness/delta/Json";
import * as Exit from "effect/Exit";
import * as Predicate from "effect/Predicate";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import * as SchemaIssue from "effect/SchemaIssue";
import type {
  NestedToolExecutionResult,
  OutputLimits,
  ReplayPolicy,
  ToolDiagnostic,
  ToolExecutionResult,
} from "../AgentTool.ts";
import type { TaskId } from "../Ids.ts";
import type { NestedToolSummary, ToolSlot } from "../Live.ts";
import type {
  ImageContent,
  TextContent,
  ToolCall,
  ToolResultContent,
  ToolResultMessage,
} from "../Message.ts";
import type { NextTaskState, TaskOutcome } from "../Task.ts";
import { boundOutput, type BoundedOutput, type ResolvedOutputLimits } from "./output.ts";
import type { ToolTaskCheckpoint, ToolTaskResult } from "./tool.ts";

/**
 * How a tool task ends; the result entry is appended either way. `failed` (execution
 * failed or was interrupted) records cancellation intent for the conversations the call owns; a result with
 * `isError` still completes.
 */
export type ToolEnding =
  | { readonly status: "completed" | "aborted" }
  | {
      readonly status: "failed";
      readonly message: string;
      /** Recorded as the failure's detail: the restart limit ended the call. */
      readonly reason?: "restart_limit" | undefined;
    };

export const COMPLETED: ToolEnding = { status: "completed" };

// ─── Recovery and abort ─────────────────────────────────────────────────────

/** A tool as the replay rule sees it: its declared policy, `unsafe` when omitted. */
export interface ReplayDeclaration {
  readonly replay?: ReplayPolicy | undefined;
}

/** What the `execute` phase does after a restart: rerun the resolved tool, or settle as interrupted. */
export type ReplayDecision<T extends ReplayDeclaration> =
  | { readonly _tag: "Rerun"; readonly tool: T }
  | {
      readonly _tag: "Interrupt";
      readonly ending: ToolEnding;
      readonly code: "interrupted";
      readonly message: string;
    };

/**
 * A call reruns only when both the stored intent and the current
 * declaration say `safe`: a current `unsafe` vetoes a stored `safe`, and a current `safe` never upgrades a
 * stored `unsafe`. A tool that no longer resolves (`current` is `undefined`) counts as `unsafe`. Otherwise it
 * ends `failed`, which records cancellation intent, so the call's owned conversations, left unsupervised, are
 * aborted.
 */
export const decideReplay = <T extends ReplayDeclaration>(
  stored: ReplayPolicy,
  current: T | undefined,
  name: string,
): ReplayDecision<T> => {
  if (stored === "safe" && current?.replay === "safe") return { _tag: "Rerun", tool: current };
  const message = `Tool ${name} was interrupted and may have partially run`;
  return { _tag: "Interrupt", ending: { status: "failed", message }, code: "interrupted", message };
};

/**
 * A call cut off `settings.restartLimit` times in a row ends with an error
 * result built from its slot, like an interrupted call's, instead of running again. `failed`, like an
 * interruption, so its owned work is aborted.
 */
export const restartLimitEnding = (name: string, cutOff: string) => {
  const message = `Tool ${name} was interrupted ${cutOff}; it is not run again`;
  return {
    ending: { status: "failed", message, reason: "restart_limit" } satisfies ToolEnding,
    code: "restart_limit",
    message,
  } as const;
};

/** The diagnostic an abort handler writes. */
export interface ToolAbortResult {
  readonly code: "aborted" | "interrupted" | "abandoned";
  readonly message: string;
}

/**
 * An ordinary abort says `aborted`; one abandoned after a restart
 * (`TaskOptions.abandonOnRestart`) says whether the call may have run: after its intent, `interrupted`,
 * before it, `abandoned`.
 */
export const toolAbortResult = (
  reason: "restart" | undefined,
  phase: ToolTaskCheckpoint["phase"],
  name: string,
): ToolAbortResult => {
  if (reason !== "restart") return { code: "aborted", message: `Tool ${name} was aborted` };
  if (phase === "execute") {
    return {
      code: "interrupted",
      message: `Tool ${name} was interrupted by a restart and may have partially run`,
    };
  }
  if (phase === "parked") {
    return {
      code: "abandoned",
      message: `Tool ${name} was parked: its caller ended with a restart`,
    };
  }
  return {
    code: "abandoned",
    message: `Tool ${name} was not started: its caller ended with a restart`,
  };
};

/** The terminal state a settled call commits, with its result either way. */
export const toolOutcome = (
  ending: ToolEnding,
  result: ToolTaskResult,
): NextTaskState<ToolTaskCheckpoint, ToolTaskResult> => {
  if (ending.status === "aborted") {
    return { status: "terminal", outcome: { status: "aborted", result } };
  }
  if (ending.status === "failed") {
    const error =
      ending.reason === undefined
        ? { message: ending.message }
        : { message: ending.message, detail: { reason: ending.reason } };
    return { status: "terminal", outcome: { status: "failed", error, result } };
  }
  return { status: "terminal", outcome: { status: "completed", result } };
};

// ─── Results the Harness writes ─────────────────────────────────────────────

/** An error result the Harness writes itself: no content and one `error` diagnostic. */
export const harnessError = (code: string, message: string): ToolExecutionResult => ({
  output: [],
  isError: true,
  diagnostics: [toolDiagnostic(code, message)],
});

/** An `error` diagnostic with `code` and `message`. */
export const toolDiagnostic = (code: string, message: string): ToolDiagnostic => ({
  severity: "error",
  code,
  message,
});

/** Dropped output, as an `OutputBuffer` snapshot or a slot counts it. */
export interface DroppedOutput {
  readonly droppedLines: number;
  readonly droppedBytes: number;
}

/**
 * The Harness's truncation diagnostic; `retain` is unknown when rebuilt from a slot after
 * recovery.
 */
export const truncatedDiagnostic = (
  dropped: DroppedOutput,
  retain?: "head" | "tail" | undefined,
): ToolDiagnostic => {
  const kept = retain === undefined ? "" : ` to its ${retain === "head" ? "beginning" : "end"}`;
  return {
    severity: "warn",
    code: "truncated",
    message: `Output truncated${kept}: ${dropped.droppedLines} lines, ${dropped.droppedBytes} bytes dropped`,
  };
};

/** An error result built from a slot, and the truncation diagnostic of its retained output, if it dropped some. */
export interface FromSlot {
  readonly result: ToolExecutionResult;
  /** Added after `afterTool`, and only while the hooks keep the slot's output. */
  readonly truncation?: ToolDiagnostic | undefined;
}

/** What a running call publishes in its slot, model-issued or nested. */
export type SlotProgress = Pick<
  ToolSlot,
  "output" | "droppedBytes" | "droppedLines" | "details" | "diagnostics"
>;

/**
 * An error result from the slot's durable partial output, details and
 * diagnostics, ending with `code`; and the truncation diagnostic of that output, when its bounds dropped some.
 */
export const fromSlot = (
  slot: SlotProgress | undefined,
  code: string,
  message: string,
): FromSlot => {
  const diagnostics = [...(slot?.diagnostics ?? []), toolDiagnostic(code, message)];
  const output: ToolResultContent =
    slot?.output === undefined || slot.output === "" ? [] : [{ type: "text", text: slot.output }];
  const result: ToolExecutionResult =
    slot?.details === undefined
      ? { output, isError: true, diagnostics }
      : { output, isError: true, details: slot.details, diagnostics };
  const droppedBytes = slot?.droppedBytes ?? 0;
  if (droppedBytes === 0) return { result };
  return {
    result,
    truncation: truncatedDiagnostic({ droppedBytes, droppedLines: slot?.droppedLines ?? 0 }),
  };
};

/** What `boundHarnessResult` takes. */
export interface HarnessResultInput {
  /** The result the Harness wrote, before `afterTool`. */
  readonly written: ToolExecutionResult;
  /** The result after `afterTool`. */
  readonly final: ToolExecutionResult;
  /** The truncation diagnostic of `written`'s own output (a slot's retained output), if any. */
  readonly truncation?: ToolDiagnostic | undefined;
  /** The tool's output limits; the defaults when it did not resolve. */
  readonly limits: ResolvedOutputLimits;
  /** Who made the call. */
  readonly kind: CallKind;
  /** Whether the tool resolved and declares a `structuredOutput` schema. */
  readonly structured: boolean;
}

/**
 * After the hooks: a result the Harness wrote itself, normalized
 * like an executed call's. The output is bounded with the tool's limits and gets a truncation diagnostic when that
 * dropped something; the slot's own truncation diagnostic stays only while the hooks kept its output. The hooks'
 * `structuredOutput` is dropped, since the tool never produced one; a nested call the hooks turned into a success
 * gets its bounded output as value, as a schema-less tool's success does, unless the tool declares a schema.
 */
export const boundHarnessResult = (input: HarnessResultInput): ToolExecutionResult => {
  const harness: Array<ToolDiagnostic> = [];
  if (input.truncation !== undefined && input.final.output === input.written.output) {
    harness.push(input.truncation);
  }
  const bounded = boundContent(input.final.output ?? [], input.limits);
  if (bounded.droppedBytes > 0) harness.push(truncatedDiagnostic(bounded, input.limits.retain));
  const { structuredOutput: _dropped, ...rest } = input.final;
  const value =
    input.kind === "nested" && input.final.isError !== true && !input.structured
      ? { structuredOutput: outputValue(bounded.content) }
      : {};
  return {
    ...rest,
    ...value,
    output: bounded.content,
    diagnostics: [...(rest.diagnostics ?? []), ...harness],
  };
};

// ─── Nested calls ───────────────────────────────────────────────────────────

/**
 * Why an explicit nested call key is refused, or `undefined`. Keys are
 * path segments of the call ID, so IDs of different nested calls never collide, and are never plain positive
 * integers, which default keys use. `__proto__` stays reserved, as it was when keys indexed a JSON object.
 */
export const checkKey = (key: string): string | undefined =>
  key === "" || key.includes("/") || key === "__proto__" || /^[1-9][0-9]*$/.test(key)
    ? `Nested call key ${JSON.stringify(key)} must be non-empty, without "/", not "__proto__", and not a positive integer`
    : undefined;

/** A nested result being built. */
type NestedDraft = {
  -readonly [K in keyof NestedToolExecutionResult]: NestedToolExecutionResult[K];
};

/**
 * A nested call's result, as stored and returned: strict JSON, without
 * `output`, which only the model reads, and without `control`, which only model-issued calls apply.
 */
export const nestedResult = (
  taskId: TaskId,
  result: ToolExecutionResult,
  durationMs: number | undefined,
): NestedToolExecutionResult => {
  const nested: NestedDraft = {
    taskId,
    isError: result.isError ?? false,
    diagnostics: result.diagnostics ?? [],
  };
  // Built field by field, so an absent field is absent rather than `undefined`, as in a strict-JSON copy.
  if (result.structuredOutput !== undefined) nested.structuredOutput = result.structuredOutput;
  if (result.details !== undefined) nested.details = result.details;
  if (result.usage !== undefined) nested.usage = result.usage;
  if (durationMs !== undefined) nested.durationMs = durationMs;
  // SAFETY: the copy of a JSON object of these fields has the same fields; a value that is not JSON throws, which
  // faults the task.
  return Json.copy(nested, { omitUndefinedProperties: true }) as NestedToolExecutionResult;
};

/** Error text a nested call's summary keeps. */
const MAX_SUMMARY_ERROR_CHARS = 500;

/** A summary being built. */
type SummaryDraft = { -readonly [K in keyof NestedToolSummary]: NestedToolSummary[K] };

/**
 * How a nested call ended, for its slot: error text, from its
 * diagnostics or else its `output`, bounded. The bound is a copy, so the slot never keeps a large output alive
 * through a slice of it.
 */
export const summaryOf = (
  result: NestedToolExecutionResult,
  output: ToolResultContent,
): NestedToolSummary => {
  const error = result.isError
    ? Delta.ownedSlice(errorTextOf(result, output), 0, MAX_SUMMARY_ERROR_CHARS)
    : "";
  const summary: SummaryDraft = { isError: result.isError };
  if (result.durationMs !== undefined) summary.durationMs = result.durationMs;
  if (result.usage !== undefined) summary.usage = result.usage;
  if (error !== "") summary.error = error;
  return summary;
};

/** The error messages of a result, else its output's text items. */
const errorTextOf = (result: NestedToolExecutionResult, output: ToolResultContent): string => {
  const errors = result.diagnostics.filter((diagnostic) => diagnostic.severity === "error");
  if (errors.length > 0) return errors.map((diagnostic) => diagnostic.message).join("\n");
  return output.flatMap((item) => (item.type === "text" ? [item.text] : [])).join("\n");
};

/**
 * What the caller gets for a nested call without a stored result:
 * one the scheduler faulted or orphaned.
 */
export const fallbackResult = (
  taskId: TaskId,
  name: string,
  outcome: TaskOutcome<unknown>,
): NestedToolExecutionResult => {
  const message =
    outcome.status === "faulted"
      ? `Tool ${name} failed: ${outcome.error.message}`
      : outcome.status === "orphaned"
        ? `Tool ${name} could not resume: ${outcome.reason}`
        : `Tool ${name} ended without a result`;
  return { taskId, isError: true, diagnostics: [toolDiagnostic(outcome.status, message)] };
};

// ─── Arguments ──────────────────────────────────────────────────────────────

/** One schema issue of a failed validation. */
export interface ValidationIssue {
  readonly path?: ReadonlyArray<PropertyKey | { readonly key: PropertyKey }> | undefined;
  readonly message: string;
}

/** What `validationMessage` renders. */
export interface ValidationFailure {
  readonly name: string;
  readonly issues: ReadonlyArray<ValidationIssue>;
  /** The arguments as received. */
  readonly received: Schema.Json;
}

/**
 * The error text of invalid arguments, one `  - path: message` line per issue, a dotted path or `root`. The
 * issue messages are Effect Schema's (`SchemaIssue.makeFormatterStandardSchemaV1`).
 */
export const validationMessage = (failure: ValidationFailure): string => {
  const lines =
    failure.issues.map((issue) => `  - ${issuePath(issue)}: ${issue.message}`).join("\n") ||
    "Unknown validation error";
  return `Validation failed for tool "${failure.name}":\n${lines}\n\nReceived arguments:\n${JSON.stringify(failure.received, null, 2)}`;
};

/** A dotted path, or `root`. */
const issuePath = (issue: ValidationIssue): string => {
  const path = issue.path ?? [];
  if (path.length === 0) return "root";
  return path
    .map((segment) => String(Predicate.isObject(segment) ? segment.key : segment))
    .join(".");
};

// ─── Settling a result ──────────────────────────────────────────────────────

/** A tool's output limits, with the defaults applied. */
export const resolveLimits = (
  limits: OutputLimits | undefined,
  defaults: ResolvedOutputLimits,
): ResolvedOutputLimits => ({
  maxBytes: limits?.maxBytes ?? defaults.maxBytes,
  maxLines: limits?.maxLines ?? defaults.maxLines,
  retain: limits?.retain ?? defaults.retain,
});

/** What a running tool reported through its `ToolContext`, as of the end of its execution. */
export interface Reported {
  /** The retained output; read only when the result omits `output`. */
  readonly snapshot: () => BoundedOutput;
  readonly details: ToolExecutionResult["details"];
  readonly diagnostics: ReadonlyArray<ToolDiagnostic>;
}

/** A result with what the tool reported merged in, before `afterTool`. */
export interface MergedResult {
  readonly result: ToolExecutionResult;
  /** The retained output the result fell back to; `undefined` when the result had its own `output`. */
  readonly retained: BoundedOutput | undefined;
}

/**
 * The tool's result with the retained output and the last details as fallbacks, and its
 * diagnostics after those reported through the `ToolContext`.
 */
export const mergeReported = (result: ToolExecutionResult, reported: Reported): MergedResult => {
  const retained = result.output === undefined ? reported.snapshot() : undefined;
  const output: ToolResultContent =
    retained === undefined
      ? (result.output ?? [])
      : retained.text === ""
        ? []
        : [{ type: "text", text: retained.text }];
  return {
    result: {
      ...result,
      output,
      details: result.details === undefined ? reported.details : result.details,
      diagnostics: [...reported.diagnostics, ...(result.diagnostics ?? [])],
    },
    retained,
  };
};

/** Who made a call: the model, or a tool through a nested call. */
export type CallKind = "model" | "nested";

/** What `boundResult` takes: the merged result, and the result after `afterTool`. */
export interface BoundResultInput {
  readonly merged: MergedResult;
  readonly final: ToolExecutionResult;
  readonly limits: ResolvedOutputLimits;
}

/**
 * The settled result: explicit text bounded, with the Harness's diagnostics last.
 * The retained output's truncation applies only while `afterTool` kept that very output.
 */
export const boundResult = (input: BoundResultInput): ToolExecutionResult => {
  const harness: Array<ToolDiagnostic> = [];
  const retained = input.merged.retained;
  if (
    retained !== undefined &&
    retained.droppedBytes > 0 &&
    input.final.output === input.merged.result.output
  ) {
    harness.push(truncatedDiagnostic(retained, input.limits.retain));
  }
  const bounded = boundContent(input.final.output ?? [], input.limits);
  if (bounded.droppedBytes > 0) harness.push(truncatedDiagnostic(bounded, input.limits.retain));
  return {
    ...input.final,
    output: bounded.content,
    diagnostics: [...(input.final.diagnostics ?? []), ...harness],
  };
};

/** A tool, as the structured output rules see it. */
export interface StructuredTool {
  readonly name: string;
  /** The tool's `structuredOutput` schema, erased to JSON. */
  readonly structuredOutput?: Schema.Codec<Schema.Json, Schema.Json> | undefined;
}

/** What `checkStructured` takes. */
export interface CheckStructuredInput {
  readonly kind: CallKind;
  readonly tool: StructuredTool;
  /** The settled result: after `afterTool`, bounded, with the truncation diagnostics last (`boundResult`). */
  readonly result: ToolExecutionResult;
}

/** The result with its `structuredOutput` checked, and why it broke the tool's contract, if it did. */
export interface CheckedStructured {
  readonly result: ToolExecutionResult;
  /** Reported to the host for a model-issued call, whose result only loses the value. */
  readonly broken: string | undefined;
}

/**
 * `structuredOutput` checked against the tool's schema, after the
 * hooks. A nested call whose value breaks the contract gets an error result without it and an
 * `invalid_structured_output` diagnostic; a model-issued call, whose value the model never sees, only loses it,
 * and the caller reports `broken`. A nested call of a schema-less tool gets its bounded output as value; one of a
 * schema tool whose error result has no value gets the output's text as a `tool_error` diagnostic, unless an
 * error diagnostic already says what went wrong.
 */
export const checkStructured = (input: CheckStructuredInput): CheckedStructured => {
  const tool = input.tool;
  const nested = input.kind === "nested";
  const harness: Array<ToolDiagnostic> = [];
  const checked = structuredOutputError(tool, input.result);
  const broken = checked._tag === "Broken" ? checked.message : undefined;
  let final: ToolExecutionResult = input.result;
  if (checked._tag === "Broken") {
    const { structuredOutput: _dropped, ...rest } = final;
    final = nested ? { ...rest, isError: true } : rest;
    if (nested) harness.push(toolDiagnostic("invalid_structured_output", checked.message));
  } else if (checked._tag === "Valid") {
    final = { ...final, structuredOutput: checked.value };
  }
  const output = final.output ?? [];
  if (nested && tool.structuredOutput === undefined) {
    final = { ...final, structuredOutput: outputValue(output) };
  }
  if (
    nested &&
    tool.structuredOutput !== undefined &&
    final.isError === true &&
    final.structuredOutput === undefined &&
    ![...(final.diagnostics ?? []), ...harness].some(
      (diagnostic) => diagnostic.severity === "error",
    )
  ) {
    const text = output.flatMap((item) => (item.type === "text" ? [item.text] : [])).join("\n");
    if (text !== "") harness.push(toolDiagnostic("tool_error", text));
  }
  return {
    result:
      harness.length === 0
        ? final
        : { ...final, diagnostics: [...(final.diagnostics ?? []), ...harness] },
    broken,
  };
};

/** How a result's `structuredOutput` stands against its tool's contract. */
type StructuredCheck =
  | { readonly _tag: "Absent" }
  | { readonly _tag: "Valid"; readonly value: Schema.Json }
  | { readonly _tag: "Broken"; readonly message: string };

/**
 * Why a result's `structuredOutput` breaks the tool's contract. A value is required unless the result is an
 * error, and allowed only with a schema.
 *
 * The value is validated with the tool's `Schema`, so the message after the dotted path (or `root`) is Schema's
 * (`SchemaIssue.makeFormatterStandardSchemaV1`). The value is first copied as strict JSON without `undefined`
 * properties, as arguments are (`validate` in `tool.ts`), and the copy is what the result keeps: Schema's
 * `optionalKey` refuses an `undefined` property a JSON Schema validator ignores. A value that is not JSON breaks
 * the contract here, rather than failing later when the nested result is stored.
 */
const structuredOutputError = (
  tool: StructuredTool,
  result: ToolExecutionResult,
): StructuredCheck => {
  const schema = tool.structuredOutput;
  const value = result.structuredOutput;
  if (schema === undefined) {
    return value === undefined
      ? { _tag: "Absent" }
      : {
          _tag: "Broken",
          message: `Tool ${tool.name} returned structuredOutput but declares no structuredOutputSchema`,
        };
  }
  if (value === undefined) {
    return result.isError === true
      ? { _tag: "Absent" }
      : { _tag: "Broken", message: `Tool ${tool.name} returned no structuredOutput` };
  }
  const copied = Result.try({
    try: () => Json.copy(value, { omitUndefinedProperties: true }),
    catch: (error) => (error instanceof Error ? error.message : String(error)),
  });
  if (Result.isFailure(copied)) {
    return {
      _tag: "Broken",
      message: `Tool ${tool.name} returned structuredOutput that is not JSON: ${copied.failure}`,
    };
  }
  const decoded = Schema.decodeUnknownExit(schema)(copied.success);
  if (Exit.isSuccess(decoded)) return { _tag: "Valid", value: copied.success };
  const error = Exit.findErrorOption(decoded);
  const [first] =
    error._tag === "Some"
      ? SchemaIssue.makeFormatterStandardSchemaV1()(error.value.issue).issues
      : [];
  const path = first === undefined ? "root" : issuePath(first);
  return {
    _tag: "Broken",
    message: `Tool ${tool.name} returned structuredOutput that does not match its schema: ${path}: ${first?.message ?? "invalid"}`,
  };
};

/**
 * A schema-less tool's value for programs, its bounded output: one text
 * item as its string, one image as itself, no items as `""`, and the content list otherwise.
 */
export const outputValue = (
  output: ToolResultContent,
): string | ImageContent | ToolResultContent => {
  if (output.length === 0) return "";
  if (output.length > 1) return output;
  const only = output[0]!;
  return only.type === "text" ? only.text : only;
};

/** Bounded result content and what bounding dropped. */
export interface BoundedContent extends DroppedOutput {
  readonly content: ToolResultContent;
}

/**
 * Bounds the text of result content. When the joined text exceeds the limits, the text
 * items are replaced by one bounded item at the position of the first (head) or last (tail) text item; other
 * content is kept.
 */
export const boundContent = (
  content: ToolResultContent,
  limits: ResolvedOutputLimits,
): BoundedContent => {
  const texts = content.filter((item): item is TextContent => item.type === "text");
  const bounded = boundOutput(texts.map((item) => item.text).join(""), limits);
  if (bounded.droppedBytes === 0) return { content, droppedBytes: 0, droppedLines: 0 };
  const keep = limits.retain === "head" ? texts[0] : texts.at(-1);
  const result: Array<ToolResultContent[number]> = [];
  for (const item of content) {
    if (item.type !== "text") result.push(item);
    else if (item === keep) result.push({ ...item, text: bounded.text });
  }
  return {
    content: result,
    droppedBytes: bounded.droppedBytes,
    droppedLines: bounded.droppedLines,
  };
};

/** The diagnostics block that ends a result's content. */
export const renderDiagnostics = (diagnostics: ReadonlyArray<ToolDiagnostic>): string =>
  `<harness>\n${diagnostics.map((diagnostic) => `[${diagnostic.severity}] ${diagnostic.message}`).join("\n")}\n</harness>`;

/** When a result was settled, and how long its execution took in this attempt. */
export interface ResultTiming {
  readonly timestamp: number;
  readonly durationMs?: number | undefined;
}

/** A tool result message being built. */
type ToolResultMessageDraft = { -readonly [K in keyof ToolResultMessage]: ToolResultMessage[K] };

/** What `toolResultMessage` takes. */
export interface ToolResultMessageInput {
  readonly call: ToolCall;
  readonly result: ToolExecutionResult;
  readonly timing: ResultTiming;
}

/**
 * The `core.tool-result` message: the content ends with the rendered diagnostics, so the
 * stored message is exactly what the model sees; the entry's `data` keeps the structured list.
 */
export const toolResultMessage = (input: ToolResultMessageInput): ToolResultMessage => {
  const result = input.result;
  const diagnostics = result.diagnostics ?? [];
  const content: Array<ToolResultContent[number]> = [...(result.output ?? [])];
  if (diagnostics.length > 0) content.push({ type: "text", text: renderDiagnostics(diagnostics) });
  // Built field by field: an absent optional field must be absent, not `undefined`, in strict JSON.
  const message: ToolResultMessageDraft = {
    role: "toolResult",
    toolCallId: input.call.id,
    toolName: input.call.name,
    content,
    isError: result.isError ?? false,
    timestamp: input.timing.timestamp,
  };
  if (result.details !== undefined) message.details = result.details;
  if (result.usage !== undefined) message.usage = result.usage;
  if (input.timing.durationMs !== undefined) message.durationMs = input.timing.durationMs;
  return message;
};

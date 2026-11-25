/**
 * Folds `@opencode/ai` stream events into the kernel's `AssistantMessageEvent`s: `start`, per content block its
 * start, deltas and end, each with the partial so far (`stopReason: "pending"`), and one terminal `done` or `error`.
 * What a provider does while it parses a stream, on opencode's already parsed events.
 */
import type { AIError, LLMEvent, ProviderMetadata } from "@opencode/ai/effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import type * as Types from "effect/Types";
import type {
  AssistantMessage,
  Cost,
  StopReason,
  TextContent,
  ThinkingContent,
  ToolCall,
  Usage,
} from "@effect-durable-harness/core/Message";
import { emptyUsage } from "@effect-durable-harness/core/Message";
import {
  type AssistantMessageEvent,
  type CostRates,
  type ResolvedModel,
  terminalEvent,
} from "@effect-durable-harness/core/Models";
import { GEMINI, NO_FINISH_REASON, retainSignature } from "./gemini.ts";
import { ANTHROPIC_MESSAGES } from "./request.ts";

type Block =
  | { readonly key: string; readonly content: TextContent }
  | { readonly key: string; readonly content: ThinkingContent }
  | { readonly key: string; readonly content: ToolCall };

export interface State {
  readonly message: AssistantMessage;
  readonly blocks: ReadonlyArray<Block>;
  /** Set once the message is terminal; later events are ignored. */
  readonly finished: boolean;
  /** Set once the opening `start` event is out, with the first opencode event. */
  readonly opened: boolean;
}

export interface StartInput {
  readonly model: ResolvedModel;
  /** The opencode protocol, to word the adapter's own error texts. */
  readonly protocol: string;
  /** The `providerMetadata` namespace replay data is read from. */
  readonly metadataKey: string;
  readonly timestamp: number;
}

export interface Assembler {
  readonly start: State;
  /** One event: the next state, and the kernel events it produces. */
  readonly step: (
    state: State,
    event: LLMEvent,
  ) => readonly [State, ReadonlyArray<AssistantMessageEvent>];
  /** The terminal event once the stream ends. */
  readonly finish: (state: State) => ReadonlyArray<AssistantMessageEvent>;
  /** The provider-error event a failed stream ends with: the content so far is kept. */
  readonly failure: (error: AIError) => LLMEvent;
}

export const make = (input: StartInput): Assembler => {
  const cutText = incompleteStreamText(input);
  const apply = applyWith(input);
  const start: State = {
    message: {
      role: "assistant",
      content: [],
      api: input.model.api,
      provider: input.model.provider,
      model: input.model.id,
      // Anthropic reports usage in `message_start`, so a response that fails, is cut or is aborted after it could
      // still carry its input tokens and cost for `core.usage` to count. `@opencode/ai` 2.2.5 keeps the
      // `message_start` usage in its parser state and emits usage only on `step-finish` and `finish`
      // (`protocols/anthropic-messages.ts`, `onMessageStart` and `onMessageStop`; `protocols/utils/lifecycle.ts`,
      // `finish`): no event before them carries it. So usage stays zero until `finish`, and a failed, cut-off or
      // aborted attempt records no spend.
      usage: emptyUsage(),
      stopReason: "pending",
      timestamp: input.timestamp,
    },
    blocks: [],
    finished: false,
    opened: false,
  };
  return {
    start,
    step: (state, event) => {
      if (state.finished) return [state, []];
      const next = apply(state, event);
      if (next.finished) return [next, []];
      const events = eventsOf(state, next, event);
      // The stream opens with `start` once the response begins; a response that fails first only ends.
      if (state.opened) return [next, events];
      return [{ ...next, opened: true }, [{ type: "start", partial: state.message }, ...events]];
    },
    finish: (state) => [terminalEvent(state.finished ? state.message : fail(state, cutText))],
    failure: (error) => ({ type: "provider-error", message: errorText(error, cutText) }),
  };
};

// -----------------------------------------------------------------------------
// Events
// -----------------------------------------------------------------------------

const applyWith =
  (input: StartInput) =>
  (state: State, event: LLMEvent): State => {
    switch (event.type) {
      case "text-delta": {
        // Gemini signs a text block on one of its deltas.
        const signature = thoughtSignature(event.providerMetadata, input.metadataKey);
        if (event.text.length === 0 && signature === undefined) return state;
        return updateText(state, event.id, (block) =>
          signedText(block.text + event.text, retainSignature(block.textSignature, signature)),
        );
      }
      case "text-end": {
        // opencode: an end event's `text` is authoritative and replaces the deltas (`schema/events.ts`, `TextEnd`).
        const signature = thoughtSignature(event.providerMetadata, input.metadataKey);
        if (event.text === undefined && signature === undefined) return state;
        return updateText(state, event.id, (block) =>
          signedText(event.text ?? block.text, retainSignature(block.textSignature, signature)),
        );
      }
      case "reasoning-start": {
        const data = replayData(event.providerMetadata, input.metadataKey)?.redactedData;
        if (data === undefined) return state;
        return updateThinking(state, event.id, () => ({
          type: "thinking",
          thinking: "[Reasoning redacted]",
          thinkingSignature: data,
          redacted: true,
        }));
      }
      case "reasoning-delta": {
        // Gemini signs a thought on one of its deltas.
        const signature = thoughtSignature(event.providerMetadata, input.metadataKey);
        if (event.text.length === 0 && signature === undefined) return state;
        return updateThinking(state, event.id, (block) =>
          withThoughtSignature({ ...block, thinking: block.thinking + event.text }, signature),
        );
      }
      case "reasoning-end": {
        const data = replayData(event.providerMetadata, input.metadataKey);
        const signature = data?.signature;
        const thought = data?.thoughtSignature;
        const text = event.text;
        if (signature === undefined && thought === undefined && text === undefined) return state;
        return updateThinking(state, event.id, (block) => {
          if (block.redacted === true) return block;
          const next: Types.Mutable<ThinkingContent> = { ...block };
          if (text !== undefined) next.thinking = text;
          // Anthropic's signature arrives in pieces; Gemini's replaces the one it retains.
          if (signature !== undefined)
            next.thinkingSignature = (block.thinkingSignature ?? "") + signature;
          return withThoughtSignature(next, thought);
        });
      }
      case "tool-input-start":
        // A provider-executed call (a server tool) is not the kernel's to run.
        if (event.providerExecuted === true) return state;
        // A streamed call starts with no arguments.
        return putToolCall(state, toolCall(event, {}));
      case "tool-input-delta": {
        const key = toolKey(event.id);
        const index = state.blocks.findIndex((block) => block.key === key);
        const current = state.blocks[index]?.content;
        // A delta of a call this state does not hold belongs to a provider-executed call.
        if (current?.type !== "toolCall") return state;
        // opencode's best-effort parse of the argument text so far; anything but an object shows as the arguments so far.
        const args = Schema.decodeUnknownOption(Schema.JsonObject)(event.input).pipe(
          Option.getOrElse(() => current.arguments),
        );
        return withBlocks(
          state,
          replaceAt(state.blocks, index, { key, content: { ...current, arguments: args } }),
        );
      }
      case "tool-call":
        // A provider-executed call (a server tool) is not the kernel's to run.
        if (event.providerExecuted === true) return state;
        // A local call's input is the provider's decoded JSON; anything but an object becomes `{}`, a lenient parse.
        return putToolCall(
          state,
          toolCall(
            event,
            Schema.decodeUnknownOption(Schema.JsonObject)(event.input).pipe(
              Option.getOrElse(() => ({})),
            ),
            // Gemini signs the call itself.
            thoughtSignature(event.providerMetadata, input.metadataKey),
          ),
        );
      case "tool-input-error":
        // A call whose input does not parse is kept; the kernel's argument validation answers it with an error.
        return putToolCall(state, toolCall(event, {}));
      case "finish":
        return finishWith(state, event, input);
      case "provider-error":
        return { ...state, finished: true, message: fail(state, event.message) };
      default:
        return state;
    }
  };

const withBlocks = (state: State, blocks: ReadonlyArray<Block>): State => ({
  ...state,
  blocks,
  message: { ...state.message, content: blocks.map((block) => block.content) },
});

const updateText = (
  state: State,
  id: string,
  update: (block: TextContent) => TextContent,
): State => {
  const key = `text:${id}`;
  const index = state.blocks.findIndex((block) => block.key === key);
  const current = index === -1 ? undefined : state.blocks[index]?.content;
  const block: Block = {
    key,
    content: update(current?.type === "text" ? current : { type: "text", text: "" }),
  };
  return withBlocks(state, replaceAt(state.blocks, index, block));
};

const signedText = (text: string, signature: string | undefined): TextContent =>
  signature === undefined
    ? { type: "text", text }
    : { type: "text", text, textSignature: signature };

/** A Gemini thought signature replaces the retained one; a delta without one keeps it. */
const withThoughtSignature = (
  block: ThinkingContent,
  signature: string | undefined,
): ThinkingContent => {
  const retained = retainSignature(block.thinkingSignature, signature);
  return retained === undefined || retained === block.thinkingSignature
    ? block
    : { ...block, thinkingSignature: retained };
};

const updateThinking = (
  state: State,
  id: string,
  update: (block: ThinkingContent) => ThinkingContent,
): State => {
  const key = `thinking:${id}`;
  const index = state.blocks.findIndex((block) => block.key === key);
  const current = index === -1 ? undefined : state.blocks[index]?.content;
  const block: Block = {
    key,
    content: update(current?.type === "thinking" ? current : { type: "thinking", thinking: "" }),
  };
  return withBlocks(state, replaceAt(state.blocks, index, block));
};

interface ToolCallEvent {
  readonly id: string;
  readonly name: string;
  readonly namespace?: string | undefined;
}

const toolCall = (
  event: ToolCallEvent,
  args: ToolCall["arguments"],
  signature?: string | undefined,
): ToolCall => {
  const call: Types.Mutable<ToolCall> = {
    type: "toolCall",
    id: event.id,
    name: event.name,
    arguments: args,
  };
  if (signature !== undefined) call.thoughtSignature = signature;
  if (event.namespace !== undefined) call.namespace = event.namespace;
  return call;
};

const toolKey = (id: string) => `tool:${id}`;

/** The call replaces its streamed block, or is appended when it streamed none. */
const putToolCall = (state: State, call: ToolCall): State => {
  const key = toolKey(call.id);
  const index = state.blocks.findIndex((block) => block.key === key);
  return withBlocks(state, replaceAt(state.blocks, index, { key, content: call }));
};

const replaceAt = (
  blocks: ReadonlyArray<Block>,
  index: number,
  block: Block,
): ReadonlyArray<Block> =>
  index === -1 ? [...blocks, block] : blocks.map((current, i) => (i === index ? block : current));

// -----------------------------------------------------------------------------
// Kernel events
// -----------------------------------------------------------------------------

/**
 * The kernel events an opencode event produced, from the state before it and after it: a block's start when it
 * appeared, its delta, and its end. A start's partial holds the block empty, as it is pushed before the
 * first delta; a call that arrives whole (Gemini) starts and ends in one step, with no delta.
 */
const eventsOf = (
  state: State,
  next: State,
  event: LLMEvent,
): ReadonlyArray<AssistantMessageEvent> => {
  const partial = next.message;
  switch (event.type) {
    case "text-delta":
    case "reasoning-delta": {
      if (next === state) return [];
      const text = event.type === "text-delta";
      const at = blockAt(state, next, `${text ? "text" : "thinking"}:${event.id}`);
      if (at === undefined) return [];
      return [
        ...opened(at, text ? "text_start" : "thinking_start", partial),
        {
          type: text ? "text_delta" : "thinking_delta",
          contentIndex: at.index,
          delta: event.text,
          partial,
        },
      ];
    }
    case "text-end":
    case "reasoning-end": {
      const text = event.type === "text-end";
      const at = blockAt(state, next, `${text ? "text" : "thinking"}:${event.id}`);
      if (at === undefined) return [];
      const content = at.block.type === "text" ? at.block.text : textOfThinking(at.block);
      return [
        ...opened(at, text ? "text_start" : "thinking_start", partial),
        { type: text ? "text_end" : "thinking_end", contentIndex: at.index, content, partial },
      ];
    }
    case "reasoning-start": {
      const at = blockAt(state, next, `thinking:${event.id}`);
      // A redacted block starts with its placeholder text.
      return at?.isNew === true
        ? [{ type: "thinking_start", contentIndex: at.index, partial }]
        : [];
    }
    case "tool-input-start": {
      const at = blockAt(state, next, toolKey(event.id));
      return at?.isNew === true
        ? [{ type: "toolcall_start", contentIndex: at.index, partial }]
        : [];
    }
    case "tool-input-delta": {
      if (next === state) return [];
      const at = blockAt(state, next, toolKey(event.id));
      return at === undefined
        ? []
        : [{ type: "toolcall_delta", contentIndex: at.index, delta: event.text, partial }];
    }
    case "tool-call":
    case "tool-input-error": {
      const at = blockAt(state, next, toolKey(event.id));
      if (at === undefined || at.block.type !== "toolCall") return [];
      return [
        ...opened(at, "toolcall_start", partial),
        { type: "toolcall_end", contentIndex: at.index, toolCall: at.block, partial },
      ];
    }
    default:
      return [];
  }
};

interface BlockAt {
  readonly index: number;
  readonly block: TextContent | ThinkingContent | ToolCall;
  /** Whether the opencode event added the block. */
  readonly isNew: boolean;
}

const blockAt = (state: State, next: State, key: string): BlockAt | undefined => {
  const index = next.blocks.findIndex((block) => block.key === key);
  const block = next.blocks[index];
  if (block === undefined) return undefined;
  return {
    index,
    block: block.content,
    isNew: !state.blocks.some((existing) => existing.key === key),
  };
};

/** The start event of a block the step added, its partial holding the block empty. */
const opened = (
  at: BlockAt,
  type: "text_start" | "thinking_start" | "toolcall_start",
  partial: AssistantMessage,
): ReadonlyArray<AssistantMessageEvent> => {
  if (!at.isNew) return [];
  const empty: TextContent | ThinkingContent | ToolCall =
    at.block.type === "text"
      ? { type: "text", text: "" }
      : at.block.type === "thinking"
        ? { type: "thinking", thinking: "" }
        : { ...at.block, arguments: {} };
  const content = partial.content.map((block, index) => (index === at.index ? empty : block));
  return [{ type, contentIndex: at.index, partial: { ...partial, content } }];
};

const textOfThinking = (block: ThinkingContent | ToolCall): string =>
  block.type === "thinking" ? block.thinking : "";

// -----------------------------------------------------------------------------
// Finish
// -----------------------------------------------------------------------------

type FinishEvent = Extract<LLMEvent, { readonly type: "finish" }>;

const finishWith = (state: State, event: FinishEvent, input: StartInput): State => {
  const u = event.usage;
  const metadata = replayData(u?.providerMetadata, input.metadataKey);
  const cacheWrite1h = metadata?.cache_creation?.ephemeral_1h_input_tokens;
  const counts: Types.Mutable<Omit<Usage, "cost">> = {
    input: u?.nonCachedInputTokens ?? 0,
    output: u?.outputTokens ?? 0,
    cacheRead: u?.cacheReadInputTokens ?? 0,
    cacheWrite: u?.cacheWriteInputTokens ?? 0,
    totalTokens:
      u?.totalTokens ??
      (u?.nonCachedInputTokens ?? 0) +
        (u?.outputTokens ?? 0) +
        (u?.cacheReadInputTokens ?? 0) +
        (u?.cacheWriteInputTokens ?? 0),
  };
  // On Anthropic Messages `cacheWrite1h` is always written, 0 when the response reports no TTL breakdown; on other
  // protocols only when reported.
  if (cacheWrite1h !== undefined || input.protocol === ANTHROPIC_MESSAGES)
    counts.cacheWrite1h = cacheWrite1h ?? 0;
  if (u?.reasoningTokens !== undefined) counts.reasoning = u.reasoningTokens;
  const stop =
    input.protocol === GEMINI
      ? toGeminiStopReason(event.reason)
      : toStopReason(event.reason, noStopReasonText(input));
  const message: Types.Mutable<AssistantMessage> = {
    ...state.message,
    stopReason: stop.stopReason,
    usage: { ...counts, cost: calculateCost(input.model, counts) },
  };
  if (event.reason.raw !== undefined) message.rawStopReason = event.reason.raw;
  if (stop.errorMessage !== undefined) message.errorMessage = stop.errorMessage;
  return { ...state, finished: true, message };
};

interface StopMapping {
  readonly stopReason: StopReason;
  readonly errorMessage?: string | undefined;
}

const REFUSED = "The model refused to complete the request";

/**
 * opencode's finish reason → the kernel's `StopReason`. `noStopReason` is the error
 * text for a response that finished without any stop reason (opencode reports `unknown` with no raw value).
 *
 * opencode normalizes Anthropic's `model_context_window_exceeded` to `length`, so it ends as `length` rather than as
 * an unhandled reason (`error`). The raw value stays in `rawStopReason`.
 */
export const toStopReason = (reason: FinishEvent["reason"], noStopReason: string): StopMapping => {
  if (reason.raw === "refusal")
    return { stopReason: "error", errorMessage: reason.explanation ?? REFUSED };
  // Content flagged by safety filters. opencode normalizes it to `unknown`.
  if (reason.raw === "sensitive")
    return { stopReason: "error", errorMessage: "Provider stopped with: sensitive" };
  // A stream that ends with the stop reason still pending.
  if (reason.raw === undefined && reason.normalized === "unknown")
    return { stopReason: "error", errorMessage: noStopReason };
  switch (reason.normalized) {
    case "stop":
      return { stopReason: "stop" };
    case "length":
      return { stopReason: "length" };
    case "tool-calls":
      return { stopReason: "toolUse" };
    case "content-filter":
      return { stopReason: "error", errorMessage: reason.explanation ?? REFUSED };
    case "error":
    case "unknown":
      return {
        stopReason: "error",
        errorMessage: `Unhandled stop reason: ${reason.raw ?? reason.normalized}`,
      };
  }
};

/**
 * opencode's finish reason → the kernel's `StopReason` on Gemini: any reason but a stop or the token limit ends
 * `error`, with the text "Provider stopped with: …". opencode already reports `STOP` with tool calls as
 * `tool-calls`, which becomes `toolUse`.
 *
 * opencode fails the stream on the finish reasons it treats as errors (`MALFORMED_FUNCTION_CALL`,
 * `UNEXPECTED_TOOL_CALL`, `TOO_MANY_TOOL_CALLS`, `NO_IMAGE`, `MISSING_THOUGHT_SIGNATURE`), so those end with its text,
 * "Gemini stopped with …", and no `rawStopReason`. Neither text matches the kernel's retry patterns: the outcome is
 * the same.
 */
export const toGeminiStopReason = (reason: FinishEvent["reason"]): StopMapping => {
  if (reason.raw === undefined && reason.normalized === "unknown")
    return { stopReason: "error", errorMessage: NO_FINISH_REASON };
  switch (reason.normalized) {
    case "stop":
      return { stopReason: "stop" };
    case "tool-calls":
      return { stopReason: "toolUse" };
    case "length":
      return { stopReason: "length" };
    default:
      return {
        stopReason: "error",
        errorMessage: `Provider stopped with: ${reason.raw ?? reason.normalized}`,
      };
  }
};

/**
 * The cost of `usage` at the model's rates. A tier applies when input
 * tokens (uncached, cache reads and writes) exceed its `inputTokensAbove`; the highest such tier wins.
 *
 * It returns the cost instead of writing it into `usage.cost`: `Usage` is readonly data.
 */
export const calculateCost = (model: ResolvedModel, usage: Omit<Usage, "cost">): Cost => {
  const inputTokens = usage.input + usage.cacheRead + usage.cacheWrite;
  let rates: CostRates = model.cost;
  let matchedThreshold = -1;
  for (const tier of model.cost.tiers ?? []) {
    if (inputTokens > tier.inputTokensAbove && tier.inputTokensAbove > matchedThreshold) {
      rates = tier;
      matchedThreshold = tier.inputTokensAbove;
    }
  }
  // Anthropic charges 2x base input for 1h cache writes.
  const longWrite = usage.cacheWrite1h ?? 0;
  const shortWrite = usage.cacheWrite - longWrite;
  const input = (rates.input / 1_000_000) * usage.input;
  const output = (rates.output / 1_000_000) * usage.output;
  const cacheRead = (rates.cacheRead / 1_000_000) * usage.cacheRead;
  const cacheWrite = (rates.cacheWrite * shortWrite + rates.input * 2 * longWrite) / 1_000_000;
  return { input, output, cacheRead, cacheWrite, total: input + output + cacheRead + cacheWrite };
};

// -----------------------------------------------------------------------------
// Errors
// -----------------------------------------------------------------------------

const fail = (state: State, errorMessage: string): AssistantMessage => ({
  ...state.message,
  stopReason: "error",
  errorMessage,
});

/**
 * The provider's text, kept intact: the kernel classifies errors by it. The one text the adapter rewrites is
 * opencode's own for a cut stream, "The provider response ended unexpectedly.", which matches none of the kernel's
 * retry patterns; it becomes the protocol's own text for the same failure, which does.
 */
const errorText = (error: AIError, cutText: string): string =>
  error.reason._tag === "InvalidProviderOutput" &&
  error.reason.classification === "incomplete-stream"
    ? cutText
    : error.message;

/**
 * The text for a stream that ends before its terminal event, per protocol. All match the kernel's retry patterns.
 */
const incompleteStreamText = (input: StartInput): string =>
  input.protocol === ANTHROPIC_MESSAGES
    ? "Anthropic stream ended before message_stop"
    : input.protocol === GEMINI
      ? NO_FINISH_REASON
      : `${input.model.provider} stream ended without a terminal event`;

/**
 * The text for a stream that ends without a stop reason, per protocol. Both match the kernel's retry pattern
 * "ended without".
 */
const noStopReasonText = (input: StartInput): string =>
  input.protocol === ANTHROPIC_MESSAGES
    ? "Anthropic stream ended without a stop reason"
    : `${input.model.provider} stream ended without a terminal event`;

/** The replay and usage data opencode's Anthropic and Gemini protocols put in `providerMetadata`. */
const ReplayData = Schema.Struct({
  signature: Schema.optional(Schema.String),
  thoughtSignature: Schema.optional(Schema.String),
  redactedData: Schema.optional(Schema.String),
  cache_creation: Schema.optional(
    Schema.Struct({ ephemeral_1h_input_tokens: Schema.optional(Schema.Number) }),
  ),
});

const replayData = (metadata: ProviderMetadata | undefined, key: string) =>
  Schema.decodeUnknownOption(ReplayData)(metadata?.[key]).pipe(Option.getOrUndefined);

const thoughtSignature = (metadata: ProviderMetadata | undefined, key: string) =>
  replayData(metadata, key)?.thoughtSignature;

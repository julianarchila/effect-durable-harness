/**
 * The kernel's messages and request options → an `@opencode/ai` request: what a provider does before it serializes,
 * expressed in opencode's vocabulary; opencode then writes the wire format.
 */
import type {
  CachePolicy,
  ContentPart,
  HttpOptions,
  LLM,
  Message as OcMessage,
  ToolResultValue,
} from "@opencode/ai/effect";
import { Media } from "@opencode/ai/effect";
import * as Predicate from "effect/Predicate";
import type * as Types from "effect/Types";
import {
  getCurrentTools,
  type AssistantMessage,
  type ImageContent,
  type Message,
  type TextContent,
  type ThinkingLevel,
  type ToolResultMessage,
  type UserMessage,
} from "@effect-durable-harness/core/Message";
import type { GenerateRequest, StreamOptions } from "@effect-durable-harness/core/Models";
import type { OpencodeModel } from "../OpencodeModels.ts";
import { GEMINI, replayableSignature, thinkingConfig } from "./gemini.ts";
import {
  contentText,
  getSystemMessageText,
  renderSystemMessageUpdate,
  resolveTranscript,
  transformMessages,
} from "./transcript.ts";

type RequestInput = Parameters<typeof LLM.request>[0];

/** The opencode protocol id whose provider options this adapter writes. */
export const ANTHROPIC_MESSAGES = "anthropic-messages";

/** The `providerMetadata` namespace opencode reads replay data from, as its protocols compute it. */
export const providerMetadataKey = (model: OpencodeModel): string =>
  model.model.route.providerMetadataKey ?? String(model.model.provider);

export const toRequest = (model: OpencodeModel, request: GenerateRequest): RequestInput => {
  const key = providerMetadataKey(model);
  const gemini = model.model.route.protocol === GEMINI;
  const resolved = transformMessages(
    resolveTranscript(request.messages, model.supportsMidConvoSystemMessages ?? false),
    model,
  );
  const system: Array<{ readonly type: "text"; readonly text: string }> = [];
  const messages: Array<OcMessage.Input> = [];
  resolved.forEach((message, index) => {
    if (message.role === "system") {
      // The leading system message is the prompt; later ones are updates.
      const text = index === 0 ? getSystemMessageText(message) : renderSystemMessageUpdate(message);
      if (text.length === 0) return;
      if (index === 0) system.push({ type: "text", text });
      else messages.push({ role: "system", content: [{ type: "text", text }] });
      return;
    }
    const converted = toMessage(message, key, gemini);
    if (converted !== undefined) messages.push(converted);
  });
  const options = request.options;
  const input: Types.Mutable<RequestInput> = {
    model: model.model,
    system,
    messages,
    tools: getCurrentTools(request.messages).map((tool) => ({
      name: tool.name,
      description: tool.description,
      inputSchema: tool.parameters,
    })),
    // The output cap is the request's, else the model's. It is not clamped to the context window left, which needs
    // a token estimate; a request over the window fails with the provider's overflow text, which the kernel classifies.
    generation: { maxTokens: request.maxTokens ?? model.maxTokens },
  };
  const cache = cachePolicy(options.cacheRetention);
  if (cache !== undefined) input.cache = cache;
  // The session id goes with caching, and not when caching is off.
  if (request.sessionId !== undefined && options.cacheRetention !== "none")
    input.promptCacheKey = request.sessionId;
  if (model.model.route.protocol === ANTHROPIC_MESSAGES) {
    const providerOptions = anthropicOptions(model, request);
    if (providerOptions !== undefined) input.providerOptions = providerOptions;
  }
  if (gemini) {
    const thinking = thinkingConfig(model, request.reasoning);
    if (thinking !== undefined) input.providerOptions = { thinkingConfig: thinking };
  }
  const http = httpOptions(options);
  if (http !== undefined) input.http = http;
  return input;
};

// -----------------------------------------------------------------------------
// Messages
// -----------------------------------------------------------------------------

const toMessage = (
  message: Exclude<Message, { readonly role: "system" }>,
  key: string,
  gemini: boolean,
): OcMessage.Input | undefined => {
  switch (message.role) {
    case "user":
      return { role: "user", content: userContent(message) };
    case "assistant": {
      const content = gemini ? geminiContent(message, key) : assistantContent(message, key);
      return content.length === 0 ? undefined : { role: "assistant", content };
    }
    case "toolResult":
      return {
        role: "tool",
        content: [
          {
            type: "tool-result",
            id: message.toolCallId,
            name: message.toolName,
            result: toolResult(message),
          },
        ],
      };
  }
};

const userContent = (message: UserMessage): Array<ContentPart> =>
  Predicate.isString(message.content)
    ? [{ type: "text", text: message.content }]
    : message.content.map((part) => (part.type === "text" ? textPart(part) : mediaPart(part)));

const textPart = (part: TextContent): ContentPart => ({ type: "text", text: part.text });

const mediaPart = (part: ImageContent): ContentPart => ({
  type: "media",
  media: Media.base64(part.data, part.mimeType),
});

const assistantContent = (message: AssistantMessage, key: string): Array<ContentPart> =>
  message.content.flatMap((block): Array<ContentPart> => {
    switch (block.type) {
      case "text":
        return block.text.trim().length === 0 ? [] : [{ type: "text", text: block.text }];
      case "thinking": {
        // opencode reads replay data from `providerMetadata`.
        if (block.redacted === true) {
          return [
            {
              type: "reasoning",
              text: "",
              providerMetadata: { [key]: { redactedData: block.thinkingSignature ?? "" } },
            },
          ];
        }
        if (block.thinkingSignature !== undefined && block.thinkingSignature.trim().length > 0) {
          return [
            {
              type: "reasoning",
              text: block.thinking,
              providerMetadata: { [key]: { signature: block.thinkingSignature } },
            },
          ];
        }
        return block.thinking.trim().length === 0 ? [] : [{ type: "text", text: block.thinking }];
      }
      case "toolCall":
        return [
          block.namespace === undefined
            ? { type: "tool-call", id: block.id, name: block.name, input: block.arguments }
            : {
                type: "tool-call",
                id: block.id,
                name: block.name,
                input: block.arguments,
                namespace: block.namespace,
              },
        ];
    }
  });

/**
 * An assistant turn for Gemini: each signature goes back on the part it came with,
 * under opencode's `thoughtSignature` key. A block with only whitespace is dropped unless it carries a signature; a
 * thinking block stays a thought part, signed or not. Another model's blocks reach here without their signatures
 * (`transformMessages`).
 */
const geminiContent = (message: AssistantMessage, key: string): Array<ContentPart> =>
  message.content.flatMap((block): Array<ContentPart> => {
    switch (block.type) {
      case "text": {
        const signature = replayableSignature(block.textSignature);
        if (block.text.trim().length === 0 && signature === undefined) return [];
        return [{ type: "text", text: block.text, ...signed(key, signature) }];
      }
      case "thinking": {
        const signature = replayableSignature(block.thinkingSignature);
        if (block.thinking.trim().length === 0 && signature === undefined) return [];
        return [{ type: "reasoning", text: block.thinking, ...signed(key, signature) }];
      }
      case "toolCall": {
        const signature = replayableSignature(block.thoughtSignature);
        const call = {
          type: "tool-call" as const,
          id: block.id,
          name: block.name,
          input: block.arguments,
        };
        return [
          block.namespace === undefined
            ? { ...call, ...signed(key, signature) }
            : { ...call, namespace: block.namespace, ...signed(key, signature) },
        ];
      }
    }
  });

/** opencode's Gemini protocol reads a part's signature from `providerMetadata[key].thoughtSignature`. */
const signed = (key: string, signature: string | undefined) =>
  signature === undefined ? {} : { providerMetadata: { [key]: { thoughtSignature: signature } } };

/**
 * A result's text is joined by newlines, and its images are sent as blocks. opencode's `error` result carries text only (`ToolResultValue`), so an error result's
 * images are dropped and the model sees only its text. Sending it as `content` instead would keep the images but lose
 * `is_error`, which the model reads to tell a failure from a success.
 */
const toolResult = (message: ToolResultMessage): ToolResultValue => {
  const text = contentText(message.content);
  if (message.isError) return { type: "error", value: text };
  if (!message.content.some((part) => part.type === "image")) return { type: "text", value: text };
  return {
    type: "content",
    value: message.content.map((part) =>
      part.type === "text"
        ? { type: "text", text: part.text }
        : { type: "file", uri: `data:${part.mimeType};base64,${part.data}`, mime: part.mimeType },
    ),
  };
};

// -----------------------------------------------------------------------------
// Options
// -----------------------------------------------------------------------------

/** `headers` and `timeoutMs` → opencode's HTTP options; `timeoutMs` bounds the whole request. */
const httpOptions = (options: StreamOptions): Types.Mutable<HttpOptions.Input> | undefined => {
  const timeout =
    options.timeoutMs !== undefined && options.timeoutMs > 0 ? options.timeoutMs : undefined;
  if (options.headers === undefined && timeout === undefined) return undefined;
  const http: Types.Mutable<HttpOptions.Input> = {};
  if (options.headers !== undefined) http.headers = options.headers;
  if (timeout !== undefined) http.timeout = timeout;
  return http;
};

/** `cacheRetention` → opencode's cache policy. `short`, the default, is opencode's default `auto`. */
const cachePolicy = (retention: StreamOptions["cacheRetention"]): CachePolicy | undefined => {
  if (retention === "none") return "none";
  // opencode's `auto` placement (`cache-policy.ts`, `AUTO`) with a one-hour TTL, for `long`.
  if (retention === "long") return { tools: true, system: true, messages: { tail: 1 }, ttl: "1h" };
  return undefined;
};

/**
 * The Anthropic Messages options this adapter writes (`anthropic-messages.ts`, `Options`). A type alias, not an
 * interface: `providerOptions` is an index-signature record, which only an alias is assignable to.
 */
type AnthropicOptions = {
  thinking?:
    | { readonly type: "disabled" }
    | { readonly type: "adaptive"; readonly display: "summarized" };
  effort?: string;
  metadata?: { readonly user_id: string };
};

/**
 * Anthropic Messages options: thinking, effort and `metadata.user_id`.
 *
 * Thinking is always adaptive, never budget-based: the Claude 5 models reject `budget_tokens`.
 */
const anthropicOptions = (
  model: OpencodeModel,
  request: GenerateRequest,
): AnthropicOptions | undefined => {
  const options: AnthropicOptions = {};
  if (model.reasoning && request.reasoning !== undefined) {
    options.thinking = { type: "adaptive", display: "summarized" };
    options.effort = effortFor(model, request.reasoning);
  } else if (model.reasoning && model.thinkingLevelMap?.off !== null) {
    options.thinking = { type: "disabled" };
  }
  const userId = request.options.metadata?.["user_id"];
  if (Predicate.isString(userId)) options.metadata = { user_id: userId };
  return Object.keys(options).length === 0 ? undefined : options;
};

/** The Anthropic effort for a thinking level. */
const effortFor = (model: OpencodeModel, level: ThinkingLevel): string => {
  const mapped = model.thinkingLevelMap?.[level];
  if (Predicate.isString(mapped)) return mapped;
  switch (level) {
    case "minimal":
    case "low":
      return "low";
    case "medium":
      return "medium";
    default:
      return "high";
  }
};

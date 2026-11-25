/**
 * The message shape, modeled with `Schema`: what an entry's `model` column
 * holds and what a model reads. It is converted to a provider's wire format only when a request
 * is made, by the model service behind `Models`.
 *
 * Every field is modeled, also those the Harness does not write yet
 * (`deferred`, `nestedCalls`, `diagnostics`), so a later use needs no
 * migration.
 *
 * @since 0.0.0
 */
import * as Schema from "effect/Schema";

// -----------------------------------------------------------------------------
// content
// -----------------------------------------------------------------------------

/**
 * Text content.
 *
 * @category content
 * @since 0.0.0
 */
export const TextContent = Schema.Struct({
  type: Schema.Literal("text"),
  text: Schema.String,
  /** Opaque provider replay data. */
  textSignature: Schema.optionalKey(Schema.String),
});
/**
 * The type of `TextContent`.
 *
 * @category content
 * @since 0.0.0
 */
export type TextContent = typeof TextContent.Type;

/**
 * Reasoning content.
 *
 * @category content
 * @since 0.0.0
 */
export const ThinkingContent = Schema.Struct({
  type: Schema.Literal("thinking"),
  thinking: Schema.String,
  /** Opaque provider replay data: the Anthropic signature, or the redacted payload when `redacted`. */
  thinkingSignature: Schema.optionalKey(Schema.String),
  redacted: Schema.optionalKey(Schema.Boolean),
});
/**
 * The type of `ThinkingContent`.
 *
 * @category content
 * @since 0.0.0
 */
export type ThinkingContent = typeof ThinkingContent.Type;

/**
 * An image, as base64, stored inline.
 *
 * @category content
 * @since 0.0.0
 */
export const ImageContent = Schema.Struct({
  type: Schema.Literal("image"),
  data: Schema.String,
  mimeType: Schema.String,
});
/**
 * The type of `ImageContent`.
 *
 * @category content
 * @since 0.0.0
 */
export type ImageContent = typeof ImageContent.Type;

/**
 * A tool call the model made.
 *
 * @category content
 * @since 0.0.0
 */
export const ToolCall = Schema.Struct({
  type: Schema.Literal("toolCall"),
  id: Schema.String,
  name: Schema.String,
  arguments: Schema.JsonObject,
  thoughtSignature: Schema.optionalKey(Schema.String),
  namespace: Schema.optionalKey(Schema.String),
});
/**
 * The type of `ToolCall`.
 *
 * @category content
 * @since 0.0.0
 */
export type ToolCall = typeof ToolCall.Type;

/**
 * What a user message holds: a string, or text and images.
 *
 * @category content
 * @since 0.0.0
 */
export const UserContent = Schema.Union([
  Schema.String,
  Schema.Array(Schema.Union([TextContent, ImageContent])),
]);
/**
 * The type of `UserContent`.
 *
 * @category content
 * @since 0.0.0
 */
export type UserContent = typeof UserContent.Type;

/**
 * What a tool result holds: text and images.
 *
 * @category content
 * @since 0.0.0
 */
export const ToolResultContent = Schema.Array(Schema.Union([TextContent, ImageContent]));
/**
 * The type of `ToolResultContent`.
 *
 * @category content
 * @since 0.0.0
 */
export type ToolResultContent = typeof ToolResultContent.Type;

// -----------------------------------------------------------------------------
// usage
// -----------------------------------------------------------------------------

/**
 * Cost of a response in USD.
 *
 * @category usage
 * @since 0.0.0
 */
export const Cost = Schema.Struct({
  input: Schema.Number,
  output: Schema.Number,
  cacheRead: Schema.Number,
  cacheWrite: Schema.Number,
  total: Schema.Number,
});
/**
 * The type of `Cost`.
 *
 * @category usage
 * @since 0.0.0
 */
export type Cost = typeof Cost.Type;

/**
 * Token usage of a response or a tool execution.
 *
 * @category usage
 * @since 0.0.0
 */
export const Usage = Schema.Struct({
  input: Schema.Number,
  output: Schema.Number,
  cacheRead: Schema.Number,
  cacheWrite: Schema.Number,
  cacheWrite1h: Schema.optionalKey(Schema.Number),
  reasoning: Schema.optionalKey(Schema.Number),
  totalTokens: Schema.Number,
  cost: Cost,
});
/**
 * The type of `Usage`.
 *
 * @category usage
 * @since 0.0.0
 */
export type Usage = typeof Usage.Type;

/**
 * Usage with every counter at zero.
 *
 * @category usage
 * @since 0.0.0
 */
export const emptyUsage = (): Usage => ({
  input: 0,
  output: 0,
  cacheRead: 0,
  cacheWrite: 0,
  totalTokens: 0,
  cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
});

// -----------------------------------------------------------------------------
// messages
// -----------------------------------------------------------------------------

/**
 * Why a response ended; `pending` marks a partial.
 *
 * @category messages
 * @since 0.0.0
 */
export const StopReason = Schema.Literals([
  "pending",
  "stop",
  "length",
  "toolUse",
  "error",
  "aborted",
  "deferred",
]);
/**
 * The type of `StopReason`.
 *
 * @category messages
 * @since 0.0.0
 */
export type StopReason = typeof StopReason.Type;

/**
 * Reasoning effort requested from a model.
 *
 * @category messages
 * @since 0.0.0
 */
export const ThinkingLevel = Schema.Literals([
  "off",
  "minimal",
  "low",
  "medium",
  "high",
  "xhigh",
  "max",
]);
/**
 * The type of `ThinkingLevel`.
 *
 * @category messages
 * @since 0.0.0
 */
export type ThinkingLevel = typeof ThinkingLevel.Type;

/**
 * A tool declaration as the model sees it.
 *
 * @category messages
 * @since 0.0.0
 */
export const Tool = Schema.Struct({
  name: Schema.String,
  description: Schema.String,
  /** JSON Schema of the arguments. */
  parameters: Schema.JsonObject,
});
/**
 * The type of `Tool`.
 *
 * @category messages
 * @since 0.0.0
 */
export type Tool = typeof Tool.Type;

/**
 * A tool named in `SystemMessage.toolsRemoved`.
 *
 * @category messages
 * @since 0.0.0
 */
export const ToolReference = Schema.Struct({ name: Schema.String });
/**
 * The type of `ToolReference`.
 *
 * @category messages
 * @since 0.0.0
 */
export type ToolReference = typeof ToolReference.Type;

/**
 * System instructions and tool declarations at one point in the transcript.
 * The leading one is the system prompt; later ones change it.
 *
 * @category messages
 * @since 0.0.0
 */
export const SystemMessage = Schema.Struct({
  role: Schema.Literal("system"),
  content: Schema.Union([Schema.String, Schema.Array(TextContent)]),
  /** Named prompt sections; `null` removes one. */
  sections: Schema.optionalKey(Schema.Record(Schema.String, Schema.NullOr(Schema.String))),
  toolsAdded: Schema.optionalKey(Schema.Array(Tool)),
  toolsRemoved: Schema.optionalKey(Schema.Array(ToolReference)),
  timestamp: Schema.Number,
});
/**
 * The type of `SystemMessage`.
 *
 * @category messages
 * @since 0.0.0
 */
export type SystemMessage = typeof SystemMessage.Type;

/**
 * A user message.
 *
 * @category messages
 * @since 0.0.0
 */
export const UserMessage = Schema.Struct({
  role: Schema.Literal("user"),
  content: UserContent,
  timestamp: Schema.Number,
});
/**
 * The type of `UserMessage`.
 *
 * @category messages
 * @since 0.0.0
 */
export type UserMessage = typeof UserMessage.Type;

/**
 * Redacted provider or runtime diagnostic of a response.
 *
 * @category messages
 * @since 0.0.0
 */
export const AssistantMessageDiagnostic = Schema.Struct({
  type: Schema.String,
  timestamp: Schema.Number,
  error: Schema.optionalKey(
    Schema.Struct({
      name: Schema.optionalKey(Schema.String),
      message: Schema.String,
      stack: Schema.optionalKey(Schema.String),
      code: Schema.optionalKey(Schema.Union([Schema.String, Schema.Number])),
    }),
  ),
  details: Schema.optionalKey(Schema.JsonObject),
});

/**
 * A provider-side deferred response being polled:
 * the message of a request answered with `stopReason: "deferred"` carries it,
 * and the generation polls it with `Models.fetchDeferred`.
 *
 * @category messages
 * @since 0.0.0
 */
export const DeferredHandle = Schema.Struct({
  provider: Schema.String,
  modelId: Schema.String,
  api: Schema.String,
  /** Provider token, such as a response id or a batch id plus row id. */
  id: Schema.String,
  expiresAt: Schema.optionalKey(Schema.Number),
  /** How long to wait before the next poll; absent, the generation waits 5000 ms. */
  pollAfterMs: Schema.optionalKey(Schema.Number),
  /** Provider conversion data required to reconstruct the final assistant message. */
  data: Schema.optionalKey(Schema.Json),
});
/**
 * The type of `DeferredHandle`.
 *
 * @category messages
 * @since 0.0.0
 */
export type DeferredHandle = typeof DeferredHandle.Type;

/**
 * A model response, final or partial.
 *
 * @category messages
 * @since 0.0.0
 */
export const AssistantMessage = Schema.Struct({
  role: Schema.Literal("assistant"),
  content: Schema.Array(Schema.Union([TextContent, ThinkingContent, ToolCall])),
  /** The API protocol that produced the response. */
  api: Schema.String,
  provider: Schema.String,
  model: Schema.String,
  responseModel: Schema.optionalKey(Schema.String),
  responseId: Schema.optionalKey(Schema.String),
  providerThinkingLevel: Schema.optionalKey(Schema.String),
  thinkingLevel: Schema.optionalKey(ThinkingLevel),
  diagnostics: Schema.optionalKey(Schema.Array(AssistantMessageDiagnostic)),
  usage: Usage,
  stopReason: StopReason,
  deferred: Schema.optionalKey(DeferredHandle),
  errorMessage: Schema.optionalKey(Schema.String),
  rawStopReason: Schema.optionalKey(Schema.String),
  endTurn: Schema.optionalKey(Schema.Boolean),
  /** When the request started, in Unix milliseconds. */
  timestamp: Schema.Number,
  durationMs: Schema.optionalKey(Schema.Number),
});
/**
 * The type of `AssistantMessage`.
 *
 * @category messages
 * @since 0.0.0
 */
export type AssistantMessage = typeof AssistantMessage.Type;

/**
 * Calls a tool made to other tools (v1.x writes it).
 *
 * @category messages
 * @since 0.0.0
 */
export const NestedToolCalls = Schema.Struct({
  calls: Schema.Array(
    Schema.Struct({
      id: Schema.String,
      name: Schema.String,
      arguments: Schema.optionalKey(Schema.JsonObject),
      argumentsBytes: Schema.optionalKey(Schema.Number),
      status: Schema.Literals(["ok", "error", "unfinished"]),
      durationMs: Schema.optionalKey(Schema.Number),
      error: Schema.optionalKey(Schema.String),
    }),
  ),
  complete: Schema.Boolean,
});

/**
 * The result of one tool call.
 *
 * @category messages
 * @since 0.0.0
 */
export const ToolResultMessage = Schema.Struct({
  role: Schema.Literal("toolResult"),
  toolCallId: Schema.String,
  toolName: Schema.String,
  content: ToolResultContent,
  details: Schema.optionalKey(Schema.Json),
  /** Usage of the tool's own execution, such as a model call. */
  usage: Schema.optionalKey(Usage),
  nestedCalls: Schema.optionalKey(NestedToolCalls),
  isError: Schema.Boolean,
  timestamp: Schema.Number,
  durationMs: Schema.optionalKey(Schema.Number),
});
/**
 * The type of `ToolResultMessage`.
 *
 * @category messages
 * @since 0.0.0
 */
export type ToolResultMessage = typeof ToolResultMessage.Type;

/**
 * Any message.
 *
 * @category messages
 * @since 0.0.0
 */
export const Message = Schema.Union([
  SystemMessage,
  UserMessage,
  AssistantMessage,
  ToolResultMessage,
]);
/**
 * The type of `Message`.
 *
 * @category messages
 * @since 0.0.0
 */
export type Message = typeof Message.Type;

// -----------------------------------------------------------------------------
// transcript
// -----------------------------------------------------------------------------

/**
 * The tools offered after replaying every system message's `toolsRemoved` and
 * `toolsAdded`, in transcript order. Within one message removals apply first,
 * so a removal and a re-addition of the same name replace the tool.
 *
 * @category transcript
 * @since 0.0.0
 */
export const getCurrentTools = (messages: ReadonlyArray<Message>): Array<Tool> => {
  const tools = new Map<string, Tool>();
  for (const message of messages) {
    if (message.role !== "system") continue;
    for (const tool of message.toolsRemoved ?? []) tools.delete(tool.name);
    for (const tool of message.toolsAdded ?? []) tools.set(tool.name, tool);
  }
  return [...tools.values()];
};

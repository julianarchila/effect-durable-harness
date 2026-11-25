/**
 * The model boundary of the kernel: generation resolves a model and streams a
 * response through `Models`. The kernel never talks to a provider:
 * a layer of `Models` does, such as `@effect-durable-harness/opencode` or the fake
 * `FauxModel` in `testing/`.
 *
 * A conversation stores its model by name (`ModelRef`, in `core.agent`) and
 * resolves it at every request.
 *
 * @since 0.0.0
 */
import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";
import type * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import type * as Stream from "effect/Stream";
import type {
  AssistantMessage,
  DeferredHandle,
  Message,
  StopReason,
  ThinkingLevel,
  ToolCall,
} from "./Message.ts";

/**
 * A model by name: a provider and a model ID.
 *
 * @category models
 * @since 0.0.0
 */
export const ModelRef = Schema.Struct({ provider: Schema.String, modelId: Schema.String });
/**
 * The type of `ModelRef`.
 *
 * @category models
 * @since 0.0.0
 */
export type ModelRef = typeof ModelRef.Type;

/**
 * Price per million tokens, in USD.
 *
 * @category models
 * @since 0.0.0
 */
export interface CostRates {
  readonly input: number;
  readonly output: number;
  readonly cacheRead: number;
  readonly cacheWrite: number;
}

/**
 * A resolved model and the metadata the consumer supplies for it.
 *
 * @category models
 * @since 0.0.0
 */
export interface ResolvedModel {
  /** Must equal the `ModelRef`'s `provider`: usage is keyed `provider/id`. */
  readonly provider: string;
  /** Must equal the `ModelRef`'s `modelId`. */
  readonly id: string;
  /** The API protocol, written to `AssistantMessage.api`. */
  readonly api: string;
  readonly contextWindow: number;
  readonly maxTokens: number;
  /** Whether the model takes a reasoning effort. */
  readonly reasoning: boolean;
  /** Input it accepts; a model without `image` gets a text placeholder instead of an image. */
  readonly input: ReadonlyArray<"text" | "image">;
  /** A tier applies when input tokens (uncached, cache reads and writes) exceed `inputTokensAbove`. */
  readonly cost: CostRates & {
    readonly tiers?: ReadonlyArray<CostRates & { readonly inputTokensAbove: number }> | undefined;
  };
  /** Provider values for effect-durable-harness's thinking levels; `null` marks a level unsupported. */
  readonly thinkingLevelMap?: Readonly<Partial<Record<ThinkingLevel, string | null>>> | undefined;
}

/**
 * The curated request options, pinned on a request attempt. Absent fields use the model service's
 * defaults.
 *
 * @category models
 * @since 0.0.0
 */
export const StreamOptions = Schema.Struct({
  transport: Schema.optionalKey(Schema.String),
  timeoutMs: Schema.optionalKey(Schema.Number),
  /** Provider retries inside one request attempt. */
  maxRetries: Schema.optionalKey(Schema.Number),
  maxRetryDelayMs: Schema.optionalKey(Schema.Number),
  headers: Schema.optionalKey(Schema.Record(Schema.String, Schema.String)),
  metadata: Schema.optionalKey(Schema.JsonObject),
  cacheRetention: Schema.optionalKey(Schema.Literals(["none", "short", "long"])),
  /**
   * Asks a capable model service to return a durable handle (`stopReason: "deferred"`) and continue the request
   * asynchronously; the generation polls it with `Models.fetchDeferred`. A service without deferred
   * responses ignores it.
   */
  deferred: Schema.optionalKey(
    Schema.Union([
      Schema.Boolean,
      Schema.Struct({ window: Schema.optionalKey(Schema.Literals(["15m", "1h", "24h"])) }),
    ]),
  ),
});
/**
 * The type of `StreamOptions`.
 *
 * @category models
 * @since 0.0.0
 */
export type StreamOptions = typeof StreamOptions.Type;

/**
 * One model request: the model context and the attempt's pinned options. The
 * system prompt and the tools are in the context's system messages.
 *
 * @category models
 * @since 0.0.0
 */
export interface GenerateRequest {
  readonly messages: ReadonlyArray<Message>;
  /** Absent for `off`. */
  readonly reasoning?: ThinkingLevel | undefined;
  /** The conversation's provider session (`core.provider`), for provider-side caching. */
  readonly sessionId?: string | undefined;
  readonly options: StreamOptions;
  /**
   * The output cap of this request; absent: the model's `maxTokens`.
   * The compaction task's summary request sets it.
   */
  readonly maxTokens?: number | undefined;
  /**
   * Replaces the provider request body (the generation's `beforeProviderRequest` hooks chain over it): a model service calls it each time it builds a body, with the body in the provider
   * API's format, and sends what it returns. It never fails; an interruption stops the request.
   */
  readonly onPayload?: ((payload: Schema.Json) => Effect.Effect<Schema.Json>) | undefined;
}

/**
 * One event of a streamed response: `start`, then per
 * content block its start, its deltas and its end, then one terminal event, `done` or `error`. Every event but
 * the terminal one carries `partial`, the message so far with `stopReason: "pending"`; `contentIndex` is the
 * block's index in `partial.content`.
 *
 * The deltas say what a block grew by, so the kernel commits a partial as appends of the new text instead of
 * comparing whole strings. A `toolcall_delta` carries the raw argument
 * text; the call's partial arguments in `partial` are what that text parses to so far. A `*_end` event may
 * replace the block's text or arguments: the kernel then assigns the block whole.
 *
 * @category models
 * @since 0.0.0
 */
export type AssistantMessageEvent =
  | { readonly type: "start"; readonly partial: AssistantMessage }
  | {
      readonly type: "text_start" | "thinking_start" | "toolcall_start";
      readonly contentIndex: number;
      readonly partial: AssistantMessage;
    }
  | {
      readonly type: "text_delta" | "thinking_delta" | "toolcall_delta";
      readonly contentIndex: number;
      readonly delta: string;
      readonly partial: AssistantMessage;
    }
  | {
      readonly type: "text_end" | "thinking_end";
      readonly contentIndex: number;
      readonly content: string;
      readonly partial: AssistantMessage;
    }
  | {
      readonly type: "toolcall_end";
      readonly contentIndex: number;
      readonly toolCall: ToolCall;
      readonly partial: AssistantMessage;
    }
  | {
      readonly type: "done";
      readonly reason: Extract<StopReason, "stop" | "length" | "toolUse" | "deferred">;
      readonly message: AssistantMessage;
    }
  | {
      readonly type: "error";
      readonly reason: Extract<StopReason, "aborted" | "error">;
      readonly error: AssistantMessage;
    };

/**
 * The terminal event for a final message: `done` or `error` by its stop reason. A message still `pending` ends
 * as an `error`, as for a response without a stop reason.
 *
 * @category models
 * @since 0.0.0
 */
export const terminalEvent = (message: AssistantMessage): AssistantMessageEvent => {
  switch (message.stopReason) {
    case "stop":
    case "length":
    case "toolUse":
    case "deferred":
      return { type: "done", reason: message.stopReason, message };
    case "aborted":
      return { type: "error", reason: "aborted", error: message };
    case "error":
      return { type: "error", reason: "error", error: message };
    case "pending":
      // The generation takes the error event's message as the final one, so it must not stay `pending`.
      return {
        type: "error",
        reason: "error",
        error: {
          ...message,
          stopReason: "error",
          errorMessage: message.errorMessage ?? "The stream ended without a stop reason",
        },
      };
  }
};

/**
 * A deferred response's cancellation failed. The generation reports it
 * as a `host_error` with callback `models` and goes on: cancellation is best effort.
 *
 * @category errors
 * @since 0.0.0
 */
export class DeferredCancelFailed extends Schema.TaggedError<DeferredCancelFailed>()(
  "DeferredCancelFailed",
  { message: Schema.String },
) {}

/**
 * The model service generation uses.
 *
 * @category services
 * @since 0.0.0
 */
export class Models extends Context.Service<
  Models,
  {
    /** The model a `ModelRef` names, if the service knows it. */
    readonly resolve: (ref: ModelRef) => Effect.Effect<Option.Option<ResolvedModel>>;
    /**
     * Streams one response as `AssistantMessageEvent`s, the terminal `done` or `error` last. Never fails: a provider
     * failure ends with an `error` event whose message has `stopReason: "error"` and the provider's text in
     * `errorMessage`. Interrupting the stream aborts the request.
     */
    readonly stream: (
      model: ResolvedModel,
      request: GenerateRequest,
    ) => Stream.Stream<AssistantMessageEvent>;
    /**
     * Fetches a deferred response once: the final message, or a
     * message still `deferred` while the provider works. Never fails, like `stream`: a failure is a message with
     * `stopReason: "error"`. Absent on a service without deferred responses; the generation then gets the
     * error message for a provider that does not support them.
     */
    readonly fetchDeferred?:
      | ((model: ResolvedModel, handle: DeferredHandle) => Effect.Effect<AssistantMessage>)
      | undefined;
    /**
     * Cancels a deferred response no attempt will poll again, best effort.
     * Absent on a service without deferred responses, which the generation reports as a failed cancellation.
     */
    readonly cancelDeferred?:
      | ((
          model: ResolvedModel,
          handle: DeferredHandle,
        ) => Effect.Effect<void, DeferredCancelFailed>)
      | undefined;
  }
>()("@effect-durable-harness/core/Models") {}

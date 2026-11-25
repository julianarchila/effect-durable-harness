/**
 * Classification of provider responses that generation uses: context
 * overflow, and retryable errors and the retry delay. Cost is not here: it is
 * the model service's job (`@effect-durable-harness/opencode`). The token
 * estimates that compaction uses are `internal/estimate.ts`.
 */
import type { AssistantMessage } from "../Message.ts";

// -----------------------------------------------------------------------------
// overflow
// -----------------------------------------------------------------------------

/**
 * Regex patterns to detect context overflow errors from different providers.
 *
 * Provider-specific patterns (with example error messages):
 *
 * - Anthropic: "prompt is too long: 213462 tokens > 200000 maximum"
 * - Anthropic: "413 {\"error\":{\"type\":\"request_too_large\",\"message\":\"Request exceeds the maximum size\"}}"
 * - OpenAI: "Your input exceeds the context window of this model"
 * - OpenAI/LiteLLM: "Requested token count exceeds the model's maximum context length of 131072 tokens"
 * - OpenAI-compatible: "Input length (265330) exceeds model's maximum context length (262144)."
 * - Google: "The input token count (1196265) exceeds the maximum number of tokens allowed (1048575)"
 * - xAI: "This model's maximum prompt length is 131072 but the request contains 537812 tokens"
 * - Groq: "Please reduce the length of the messages or completion"
 * - OpenRouter: "This endpoint's maximum context length is X tokens. However, you requested about Y tokens"
 * - OpenRouter/Poolside: "Input length X exceeds the maximum allowed input length of Y tokens."
 * - Together AI: "The input (X tokens) is longer than the model's context length (Y tokens)."
 * - llama.cpp: "the request exceeds the available context size, try increasing it"
 * - LM Studio: "tokens to keep from the initial prompt is greater than the context length"
 * - GitHub Copilot: "prompt token count of X exceeds the limit of Y"
 * - MiniMax: "invalid params, context window exceeds limit"
 * - Kimi For Coding: "Your request exceeded model token limit: X (requested: Y)"
 * - DS4: "Prompt has X tokens, but the configured context size is Y tokens"
 * - Cerebras: "400/413 status code (no body)"
 * - Mistral: "Prompt contains X tokens ... too large for model with Y maximum context length"
 * - z.ai: `{"code":"1261","message":"Prompt too long"}`, `{"code":"1261","message":"Prompt exceeds max length"}` (CN endpoint), or silent overflow via usage.input > contextWindow
 * - Xiaomi MiMo: Truncates input to fill contextWindow exactly, then returns finish_reason "length"
 *   with output=0 (no room left to generate). Detected via stopReason "length" + zero output +
 *   input filling the context window.
 * - DashScope/Qwen: "Range of input length should be [1, X]" (HTTP 400 invalid_parameter_error)
 * - Ollama: Some deployments truncate silently, others return errors like "prompt too long; exceeded max context length by X tokens"
 */
const OVERFLOW_PATTERNS = [
  /prompt (?:is )?too long/i, // Anthropic and z.ai token overflow
  /prompt exceeds max length/i, // z.ai CN endpoint token overflow
  /request_too_large/i, // Anthropic request byte-size overflow (HTTP 413)
  /input is too long for requested model/i, // Amazon Bedrock
  /exceeds the context window/i, // OpenAI (Completions & Responses API)
  /exceeds (?:the )?(?:model'?s )?maximum context length(?: of [\d,]+ tokens?|\s*\([\d,]+\))/i, // OpenAI-compatible proxies (LiteLLM)
  /input token count.*exceeds the maximum/i, // Google (Gemini)
  /maximum prompt length is \d+/i, // xAI (Grok)
  /reduce the length of the messages/i, // Groq
  /maximum context length is \d+ tokens/i, // OpenRouter (most backends)
  /exceeds (?:the )?maximum allowed input length of [\d,]+ tokens?/i, // OpenRouter/Poolside
  /input \(\d+ tokens\) is longer than the model'?s context length \(\d+ tokens\)/i, // Together AI
  /exceeds the limit of \d+/i, // GitHub Copilot
  /exceeds the available context size/i, // llama.cpp server
  /greater than the context length/i, // LM Studio
  /context window exceeds limit/i, // MiniMax
  /exceeded model token limit/i, // Kimi For Coding
  /too large for model with \d+ maximum context length/i, // Mistral
  /prompt has [\d,]+ tokens?, but the configured context size is [\d,]+ tokens?/i, // DS4 server
  /model_context_window_exceeded/i, // z.ai non-standard finish_reason surfaced as error text
  /prompt too long; exceeded (?:max )?context length/i, // Ollama explicit overflow error
  /range of input length should be/i, // DashScope / Qwen Token Plan
  /context[_ ]length[_ ]exceeded/i, // Generic fallback
  /too many tokens/i, // Generic fallback
  /token limit exceeded/i, // Generic fallback
];

const CEREBRAS_BODYLESS_OVERFLOW_PATTERN = /^4(?:00|13)\s*(?:status code)?\s*\(no body\)/i;

/**
 * Patterns that indicate non-overflow errors (e.g. rate limiting, server errors).
 * Error messages matching any of these are excluded from overflow detection
 * even if they also match an OVERFLOW_PATTERN.
 *
 * Example: Bedrock formats throttling errors as "ThrottlingException: Too many tokens,
 * please wait before trying again." which would match the /too many tokens/i overflow
 * pattern without this exclusion.
 */
const NON_OVERFLOW_PATTERNS = [
  /^(Throttling error|Service unavailable):/i, // AWS Bedrock non-overflow errors (human-readable prefixes from formatBedrockError)
  /rate limit/i, // Generic rate limiting
  /too many requests/i, // Generic HTTP 429 style
];

/**
 * Whether an assistant message is a context overflow. Three cases:
 *
 * 1. Error-based overflow: most providers end with `stopReason: "error"` and a
 *    recognizable error message.
 * 2. Silent overflow (z.ai): the request succeeds, but input and cache reads
 *    exceed `contextWindow`.
 * 3. Length-stop overflow (Xiaomi MiMo): `stopReason: "length"` with zero
 *    output and input filling the context window.
 *
 * Cases 2 and 3 need `contextWindow`.
 */
export const isContextOverflow = (message: AssistantMessage, contextWindow?: number): boolean => {
  // Case 1: Check error message patterns
  const errorMessage = message.errorMessage;
  if (message.stopReason === "error" && errorMessage) {
    // Skip messages matching known non-overflow patterns (e.g. throttling / rate-limit)
    const isNonOverflow = NON_OVERFLOW_PATTERNS.some((p) => p.test(errorMessage));
    if (!isNonOverflow) {
      if (OVERFLOW_PATTERNS.some((p) => p.test(errorMessage))) {
        return true;
      }
      if (
        message.provider === "cerebras" &&
        CEREBRAS_BODYLESS_OVERFLOW_PATTERN.test(errorMessage)
      ) {
        return true;
      }
    }
  }

  // Case 2: Silent overflow (z.ai style) - successful but usage exceeds context
  if (contextWindow && message.stopReason === "stop") {
    const inputTokens = message.usage.input + message.usage.cacheRead;
    if (inputTokens > contextWindow) {
      return true;
    }
  }

  // Case 3: Length-stop overflow (Xiaomi MiMo style) - server truncates oversized input
  // to fit the context window, leaving no room for output. Returns stopReason "length"
  // with output=0 and input+cacheRead filling the context window.
  if (contextWindow && message.stopReason === "length" && message.usage.output === 0) {
    const inputTokens = message.usage.input + message.usage.cacheRead;
    if (inputTokens >= contextWindow * 0.99) {
      return true;
    }
  }

  return false;
};

// -----------------------------------------------------------------------------
// retry
// -----------------------------------------------------------------------------

const buildProviderErrorPattern = (patterns: ReadonlyArray<string>): RegExp =>
  new RegExp(patterns.join("|"), "i");

const NON_RETRYABLE_PROVIDER_LIMIT_ERROR_PATTERN = buildProviderErrorPattern([
  // OpenCode Go/free-tier limits returned as 429 JSON error types by OpenCode's
  // Zen API. These are subscription/account limits, not transient throttles.
  "GoUsageLimitError",
  "FreeUsageLimitError",

  // OpenCode Go subscription-limit text asks users to enable available-balance
  // usage after rolling/weekly/monthly limits are reached.
  "Monthly usage limit reached",
  "available balance",

  // Generic quota/budget/billing exhaustion. `insufficient_quota` is OpenAI's
  // quota/billing error code; the other strings cover common gateway wording.
  "insufficient_quota",
  "out of budget",
  "quota exceeded",
  "billing",

  // Sign in with ChatGPT: the subscription's shared usage limit, which resets
  // after hours rather than seconds.
  "subscription_sharing_usage_limit_exceeded",
]);

const RETRYABLE_PROVIDER_ERROR_PATTERN = buildProviderErrorPattern([
  // Generic provider load, HTTP status, and server-side transient failures.
  "overloaded",
  "server_busy",
  "servers are currently busy",
  "currently experiencing high demand",
  "model is at capacity",
  "rate.?limit",
  "too many requests",
  "429",
  "500",
  "502",
  "503",
  "504",
  "520",
  "524",
  "service.?unavailable",
  "server.?error",
  "internal.?error",

  // Wrapper/provider text for transient upstream failures, including OpenRouter
  // "Provider returned error" responses and gateway wording such as
  // "The upstream provider returned an error".
  "provider.?returned.?(?:an.?)?error",
  "exceeded request buffer limit while retrying upstream",

  // Network, proxy, and fetch transport failures. This includes OpenAI Codex
  // raw-fetch failures such as "upstream connect", "connection refused", and
  // "reset before headers", plus OpenRouter connection drops.
  "network.?error",
  "connection.?error",
  "connection.?refused",
  "connection.?lost",
  "other side closed",
  "fetch failed",
  "getaddrinfo",
  "ENOTFOUND",
  "EAI_AGAIN",
  "upstream.?connect",
  "reset before headers",
  "socket hang up",
  "socket connection was closed",
  "timed? out",
  "timeout",
  "terminated",

  // WebSocket transports can report close/error text instead of HTTP/fetch text.
  "websocket.?closed",
  "websocket.?error",

  // Premature stream endings from SDKs and transports. Anthropic can throw
  // "stream ended without ..." and "Anthropic stream ended before message_stop";
  // Bedrock/Smithy can throw an HTTP/2 no-response error.
  "ended without",
  "stream ended before message_stop",
  "stream ended before a terminal response event",
  "http2 request did not get a response",
  // Node ERR_HTTP2_STREAM_CANCEL: the HTTP/2 session died before the request was
  // sent, e.g. after the Bedrock SDK's 5-minute session timeout.
  "pending stream has been canceled",

  // Provider-requested retry delay cap failures should flow through the outer
  // retry policy so callers can surface/abort the backoff.
  "retry delay",

  // Explicit retry guidance emitted mid-stream by OpenAI Responses and Bedrock
  // stream exceptions.
  "you can retry your request",
  "try your request again",
  "please retry your request",

  // gRPC based providers (e.g. NVIDIA NIM)
  "ResourceExhausted",

  // Sign in with ChatGPT: usage or user data temporarily unavailable. Usage
  // failures can arrive mid-stream without an HTTP 503 in the message.
  "subscription_sharing_usage_unavailable",
  "subscription_sharing_user_unavailable",
]);

/** The backoff fields of a retry policy that `retryDelayMs` reads. */
export interface RetryDelayPolicy {
  /** Base delay in ms. Per-attempt delay is `baseDelayMs * 2^(attempt-1)`. */
  readonly baseDelayMs: number;
  /** Optional cap for agent-level retry delays in ms. Defaults to 60 seconds. */
  readonly maxAgentDelayMs?: number | undefined;
}

/** The default cap of a retry delay, in ms. */
export const DEFAULT_MAX_AGENT_RETRY_DELAY_MS = 60_000;

/** The delay before retry `attempt` (1-indexed): exponential backoff, capped. */
export const retryDelayMs = (policy: RetryDelayPolicy, attempt: number): number => {
  const delay = policy.baseDelayMs * 2 ** Math.max(0, attempt - 1);
  const safeDelay = Number.isSafeInteger(delay) ? delay : Number.MAX_SAFE_INTEGER;
  return Math.min(safeDelay, policy.maxAgentDelayMs ?? DEFAULT_MAX_AGENT_RETRY_DELAY_MS);
};

/**
 * Whether a failed assistant message looks like a transient provider or
 * transport error. It does not implement a retry
 * policy: callers handle context overflow first, then apply their own budget
 * and backoff.
 */
export const isRetryableAssistantError = (message: AssistantMessage): boolean => {
  if (message.stopReason !== "error" || !message.errorMessage) return false;
  const errorMessage = message.errorMessage;
  if (NON_RETRYABLE_PROVIDER_LIMIT_ERROR_PATTERN.test(errorMessage)) return false;
  return RETRYABLE_PROVIDER_ERROR_PATTERN.test(errorMessage);
};

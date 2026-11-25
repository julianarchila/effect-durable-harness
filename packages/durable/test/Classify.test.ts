import { assert, describe, it } from "@effect/vitest";
import type { AssistantMessage, StopReason, Usage } from "../src/Message.ts";
import { emptyUsage } from "../src/Message.ts";
import {
  isContextOverflow,
  isRetryableAssistantError,
  retryDelayMs,
} from "../src/internal/classify.ts";

// Tests of the error classifiers: context overflow, provider retry classification and retry delays. Cost cases live
// with the model service that computes the cost (`packages/opencode/test/Cost.test.ts`).

interface MessageOptions {
  readonly stopReason?: StopReason | undefined;
  readonly errorMessage?: string | undefined;
  readonly provider?: string | undefined;
}

/** An assistant message, reduced to what the classifiers read. */
const message = (options: MessageOptions = {}, usage: Usage = emptyUsage()): AssistantMessage => {
  const base: AssistantMessage = {
    role: "assistant",
    content: [],
    api: "openai-completions",
    provider: options.provider ?? "ollama",
    model: "qwen3.5:35b",
    usage,
    stopReason: options.stopReason ?? "stop",
    timestamp: 0,
  };
  return options.errorMessage === undefined
    ? base
    : { ...base, errorMessage: options.errorMessage };
};

// overflow.test.ts:5-29
const createErrorMessage = (errorMessage: string, provider = "ollama"): AssistantMessage =>
  message({ stopReason: "error", errorMessage, provider });

const errorMessage = (text: string): AssistantMessage =>
  message({ stopReason: "error", errorMessage: text });

interface LengthStop {
  readonly input: number;
  readonly cacheRead: number;
  readonly output: number;
}

// overflow.test.ts:167-193
const createLengthStopMessage = (options: LengthStop): AssistantMessage =>
  message(
    { stopReason: "length", provider: "test-provider" },
    {
      ...emptyUsage(),
      input: options.input,
      output: options.output,
      cacheRead: options.cacheRead,
      totalTokens: options.input + options.cacheRead + options.output,
    },
  );

describe("isContextOverflow", () => {
  it("detects explicit Ollama prompt-too-long errors", () => {
    const overflow = createErrorMessage(
      "400 `prompt too long; exceeded max context length by 100918 tokens`",
    );
    assert.isTrue(isContextOverflow(overflow, 32768));
  });

  it("detects z.ai prompt-too-long errors", () => {
    const overflow = createErrorMessage('400 {"code":"1261","message":"Prompt too long"}', "zai");
    assert.isTrue(isContextOverflow(overflow, 1048576));
  });

  it("detects z.ai CN endpoint prompt-exceeds-max-length errors", () => {
    const overflow = createErrorMessage(
      '400 {"code":"1261","message":"Prompt exceeds max length"}',
      "zai",
    );
    assert.isTrue(isContextOverflow(overflow, 1048576));
  });

  it("detects Together AI context length errors", () => {
    const overflow = createErrorMessage(
      "400 The input (516368 tokens) is longer than the model's context length (262144 tokens).",
    );
    assert.isTrue(isContextOverflow(overflow, 262144));
  });

  it("detects LiteLLM-wrapped OpenAI maximum context length errors", () => {
    const overflow = createErrorMessage(
      "Error: 503 litellm.ServiceUnavailableError: litellm.MidStreamFallbackError: litellm.APIConnectionError: APIConnectionError: OpenAIException - Requested token count exceeds the model's maximum context length of 131072 tokens.",
    );
    assert.isTrue(isContextOverflow(overflow, 131072));
  });

  it("detects OpenAI-compatible parenthesized maximum context length errors", () => {
    const overflow = createErrorMessage(
      "Error: 400 Input length (265330) exceeds model's maximum context length (262144).",
    );
    assert.isTrue(isContextOverflow(overflow, 262144));
  });

  it("detects OpenRouter Poolside maximum allowed input length errors", () => {
    const overflow = createErrorMessage(
      "Provider returned error: Input length 131393 exceeds the maximum allowed input length of 131040 tokens.",
    );
    assert.isTrue(isContextOverflow(overflow, 131072));
  });

  it("detects DS4 configured context size errors", () => {
    const overflow = createErrorMessage(
      "400 Prompt has 256468 tokens, but the configured context size is 256000 tokens",
    );
    assert.isTrue(isContextOverflow(overflow, 256000));

    const commaMessage = createErrorMessage(
      "Prompt has 5,958,968 tokens, but the configured context size is 256,000 tokens",
    );
    assert.isTrue(isContextOverflow(commaMessage, 256000));
  });

  it("does not treat generic non-overflow Ollama errors as overflow", () => {
    const failure = createErrorMessage("500 `model runner crashed unexpectedly`");
    assert.isFalse(isContextOverflow(failure, 32768));
  });

  it("only treats bodyless 400 and 413 errors as overflow for Cerebras", () => {
    for (const text of ["400 status code (no body)", "413 status code (no body)"]) {
      assert.isTrue(isContextOverflow(createErrorMessage(text, "cerebras"), 131072));
      assert.isFalse(isContextOverflow(createErrorMessage(text, "opencode-go"), 1000000));
    }
  });

  it("does not treat Bedrock throttling 'Too many tokens' as overflow", () => {
    // Bedrock returns this for HTTP 429 rate limiting, NOT context overflow.
    // formatBedrockError uses a human-readable prefix for ThrottlingException.
    const failure = createErrorMessage(
      "Throttling error: Too many tokens, please wait before trying again.",
    );
    assert.isFalse(isContextOverflow(failure, 200000));
  });

  it("does not treat Bedrock service unavailable as overflow", () => {
    const failure = createErrorMessage(
      "Service unavailable: The service is temporarily unavailable.",
    );
    assert.isFalse(isContextOverflow(failure, 200000));
  });

  it("does not treat generic rate limit errors as overflow", () => {
    const failure = createErrorMessage("Rate limit exceeded, please retry after 30 seconds.");
    assert.isFalse(isContextOverflow(failure, 200000));
  });

  it("does not treat HTTP 429 style errors as overflow", () => {
    const failure = createErrorMessage("Too many requests. Please slow down.");
    assert.isFalse(isContextOverflow(failure, 200000));
  });

  it("detects Xiaomi-style overflow (length stop with zero output and filled context)", () => {
    const overflow = createLengthStopMessage({ input: 58, cacheRead: 1048512, output: 0 });
    assert.isTrue(isContextOverflow(overflow, 1048576));
  });

  it("does not treat normal length stops with output as context overflow", () => {
    const stop = createLengthStopMessage({ input: 1000, cacheRead: 0, output: 4096 });
    assert.isFalse(isContextOverflow(stop, 200000));
  });

  it("does not treat zero-output length stops far below context as context overflow", () => {
    const stop = createLengthStopMessage({ input: 100, cacheRead: 0, output: 0 });
    assert.isFalse(isContextOverflow(stop, 200000));
  });

  // The silent-overflow rule.
  it("detects silent overflow only with a context window", () => {
    const silent = message({}, { ...emptyUsage(), input: 150_000, cacheRead: 60_000 });
    assert.isTrue(isContextOverflow(silent, 200_000));
    assert.isFalse(isContextOverflow(silent, 210_000));
    assert.isFalse(isContextOverflow(silent));
  });
});

const openAIExplicitRetryMessage =
  "An error occurred while processing your request. You can retry your request, or contact us through our help center at help.openai.com if the error persists. Please include the request ID req_******** in your message.";
const bedrockExplicitRetryMessage =
  '{"message":"The system encountered an unexpected error during processing. Try your request again."}';
const nvidiaNIMResourceExhaustedMessage =
  "ResourceExhausted: Worker local total request limit reached (288/48)";
const bunFetchSocketClosedMessage =
  "The socket connection was closed unexpectedly. For more information, pass `verbose: true` in the second argument to fetch()";
const openAIResponsesEarlyEofMessage =
  "OpenAI Responses stream ended before a terminal response event";
const wrappedDnsLookupError =
  "The pending stream has been canceled (caused by: getaddrinfo ENOTFOUND bedrock-runtime.us-east-1.amazonaws.com)";
const azurePeakLoadError =
  "The system is currently experiencing high demand and cannot process your request. Your request exceeds the maximum usage size allowed during peak load. For improved capacity reliability, consider switching to Provisioned Throughput.";

describe("provider retry classification", () => {
  it("matches explicit provider retry guidance", () => {
    assert.isTrue(isRetryableAssistantError(errorMessage(openAIExplicitRetryMessage)));
    assert.isTrue(isRetryableAssistantError(errorMessage(bedrockExplicitRetryMessage)));
    assert.isTrue(isRetryableAssistantError(errorMessage(nvidiaNIMResourceExhaustedMessage)));
  });

  it("matches Bun fetch socket drop wording", () => {
    assert.isTrue(isRetryableAssistantError(errorMessage(bunFetchSocketClosedMessage)));
  });

  it("matches upstream request buffer exhaustion wording", () => {
    assert.isTrue(
      isRetryableAssistantError(
        errorMessage("Error: exceeded request buffer limit while retrying upstream"),
      ),
    );
  });

  it.each([
    wrappedDnsLookupError,
    "connect ENOTFOUND api.example.com",
    "EAI_AGAIN api.example.com",
    "getaddrinfo failed for api.example.com",
  ])("matches DNS transport failure wording: %s", (text) => {
    assert.isTrue(isRetryableAssistantError(errorMessage(text)));
  });

  it.each([
    "The pending stream has been canceled",
    "The pending stream has been canceled (caused by: socket closed)",
  ])("matches HTTP/2 pending stream cancellation: %s", (text) => {
    assert.isTrue(isRetryableAssistantError(errorMessage(text)));
  });

  it("matches OpenAI Responses streams that end before terminal events", () => {
    assert.isTrue(isRetryableAssistantError(errorMessage(openAIResponsesEarlyEofMessage)));
  });

  it("matches Azure peak-load capacity errors", () => {
    assert.isTrue(isRetryableAssistantError(errorMessage(azurePeakLoadError)));
  });

  it("keeps provider limit errors non-retryable", () => {
    assert.isFalse(isRetryableAssistantError(errorMessage("429 quota exceeded")));
  });

  it("keeps the ChatGPT subscription usage limit non-retryable", () => {
    const text =
      'OpenAI API error (429): {"code":"subscription_sharing_usage_limit_exceeded","message":"Usage limit reached."}';
    assert.isFalse(isRetryableAssistantError(errorMessage(text)));
  });

  it.each([
    "subscription_sharing_usage_unavailable: Usage cannot be checked.",
    "subscription_sharing_user_unavailable: User cannot be loaded.",
  ])("retries temporary ChatGPT subscription errors: %s", (text) => {
    assert.isTrue(isRetryableAssistantError(errorMessage(text)));
  });

  it("classifies assistant error messages", () => {
    assert.isTrue(isRetryableAssistantError(errorMessage("overloaded_error")));
    assert.isTrue(isRetryableAssistantError(errorMessage("520 status code (no body)")));
    assert.isTrue(isRetryableAssistantError(errorMessage("524 status code (no body)")));
    assert.isFalse(isRetryableAssistantError(message()));
  });
});

describe("retryDelayMs", () => {
  it("caps agent retry delay", () => {
    assert.strictEqual(retryDelayMs({ baseDelayMs: 2000 }, 6), 60000);
    assert.strictEqual(retryDelayMs({ baseDelayMs: 2000, maxAgentDelayMs: 5000 }, 5), 5000);
    assert.strictEqual(retryDelayMs({ baseDelayMs: 2000, maxAgentDelayMs: 0 }, 5), 0);
  });

  // The backoff doubles from the base delay.
  it("doubles the base delay per attempt", () => {
    const policy = { baseDelayMs: 2000, maxAgentDelayMs: 60000 };
    assert.deepStrictEqual(
      [1, 2, 3, 4].map((attempt) => retryDelayMs(policy, attempt)),
      [2000, 4000, 8000, 16000],
    );
  });
});

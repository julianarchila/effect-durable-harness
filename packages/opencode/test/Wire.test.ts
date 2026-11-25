/**
 * The adapter against a synthetic Anthropic wire: a replaying `fetch` under opencode's real Anthropic code, including
 * the thinking level map, the cut-stream text, images, cache retention and cross-model replay.
 */
import { assert, describe, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import {
  type AssistantMessage,
  emptyUsage,
  type Message,
  type StopReason,
  type SystemMessage,
  type ThinkingLevel,
  type Tool,
  type ToolResultMessage,
  type UserMessage,
} from "@effect-durable-harness/core/Message";
import type { GenerateRequest } from "@effect-durable-harness/core/Models";
import type { OpencodeModel } from "@effect-durable-harness/opencode/OpencodeModels";
import { anthropicStream, type Canned, jsonError } from "./support/Http.ts";
import { haiku55, run, withId } from "./support/Models.ts";

const weatherTool: Tool = {
  name: "get_weather",
  description: "Current weather for a city: temperature in Celsius and sky.",
  parameters: {
    type: "object",
    properties: { city: { type: "string", description: "City name" } },
    required: ["city"],
    additionalProperties: false,
  },
};

const system: SystemMessage = {
  role: "system",
  content:
    "You are a careful assistant. Before answering anything about weather, call get_weather.",
  sections: { units: "<units>Use Celsius.</units>" },
  toolsAdded: [weatherTool],
  timestamp: 1,
};

const question: UserMessage = {
  role: "user",
  content: "Is it a good moment for a run?",
  timestamp: 2,
};

const systemUpdate: SystemMessage = {
  role: "system",
  content: "From now on, answer in Spanish.",
  sections: { units: "<units>Use Fahrenheit.</units>" },
  timestamp: 6,
};

const followUp: UserMessage = { role: "user", content: "And the temperature?", timestamp: 7 };

const answer: AssistantMessage = {
  role: "assistant",
  content: [{ type: "text", text: "Probably fine." }],
  api: haiku55.api,
  provider: "anthropic",
  model: haiku55.id,
  usage: emptyUsage(),
  stopReason: "stop",
  timestamp: 3,
};

const prefix: ReadonlyArray<Message> = [system, question];

const ok = () =>
  anthropicStream({ blocks: [{ type: "text", text: "ok" }], stopReason: "end_turn" });

interface SendInput {
  readonly canned?: ReadonlyArray<Canned> | undefined;
  readonly messages?: ReadonlyArray<Message> | undefined;
  readonly model?: OpencodeModel | undefined;
  readonly request?: Partial<GenerateRequest> | undefined;
}

const send = (input: SendInput = {}) =>
  run({
    canned: input.canned ?? [ok()],
    messages: input.messages ?? prefix,
    model: input.model,
    request: input.request,
  });

// -----------------------------------------------------------------------------
// Stop reasons and usage
// -----------------------------------------------------------------------------

describe("stop reasons", () => {
  // raw Anthropic reason → the adapter's stop reason.
  const table: ReadonlyArray<readonly [string, StopReason]> = [
    ["end_turn", "stop"],
    ["max_tokens", "length"],
    ["tool_use", "toolUse"],
    ["pause_turn", "stop"],
    ["stop_sequence", "stop"],
    ["refusal", "error"],
    // opencode normalizes it to length rather than an unhandled reason (error).
    ["model_context_window_exceeded", "length"],
  ];
  for (const [raw, stopReason] of table) {
    it.effect(raw, () =>
      Effect.gen(function* () {
        const stopDetails =
          raw === "refusal"
            ? { type: "refusal" as const, category: "cyber", explanation: "Declined: cyber" }
            : undefined;
        const { terminal } = yield* send({
          canned: [
            anthropicStream({
              blocks: [{ type: "text", text: "hi" }],
              stopReason: raw,
              stopDetails,
            }),
          ],
        });
        assert.strictEqual(terminal.stopReason, stopReason);
        assert.strictEqual(terminal.rawStopReason, raw);
        // `stop_details.explanation` is preferred.
        if (raw === "refusal") assert.strictEqual(terminal.errorMessage, "Declined: cyber");
      }),
    );
  }

  it.effect(
    "sensitive ends `error` with the provider's text (opencode normalizes it to unknown)",
    () =>
      Effect.gen(function* () {
        const { terminal } = yield* send({
          canned: [
            anthropicStream({ blocks: [{ type: "text", text: "hi" }], stopReason: "sensitive" }),
          ],
        });
        assert.strictEqual(terminal.stopReason, "error");
        assert.strictEqual(terminal.rawStopReason, "sensitive");
        assert.strictEqual(terminal.errorMessage, "Provider stopped with: sensitive");
      }),
  );

  it.effect("an unknown raw reason ends `error` as unhandled", () =>
    Effect.gen(function* () {
      const { terminal } = yield* send({
        canned: [
          anthropicStream({ blocks: [{ type: "text", text: "hi" }], stopReason: "brand_new" }),
        ],
      });
      assert.strictEqual(terminal.stopReason, "error");
      assert.strictEqual(terminal.errorMessage, "Unhandled stop reason: brand_new");
    }),
  );

  it.effect("a stream that ends without a stop reason ends `error` with a retryable text", () =>
    Effect.gen(function* () {
      const { terminal } = yield* send({
        canned: [
          anthropicStream({
            blocks: [{ type: "text", text: "partial" }],
            stopReason: "end_turn",
            tail: [
              { type: "message_delta", delta: { stop_reason: null, stop_sequence: null } },
              { type: "message_stop" },
            ],
          }),
        ],
      });
      assert.strictEqual(terminal.stopReason, "error");
      // Matches the kernel's "ended without" retry pattern.
      assert.strictEqual(terminal.errorMessage, "Anthropic stream ended without a stop reason");
      assert.isUndefined(terminal.rawStopReason);
      assert.deepStrictEqual(terminal.content, [{ type: "text", text: "partial" }]);
    }),
  );
});

describe("usage", () => {
  it.effect(
    "maps to the kernel's Usage, with reasoning tokens, 1h cache writes and the cost from the metadata",
    () =>
      Effect.gen(function* () {
        const { terminal } = yield* send({
          canned: [
            anthropicStream({
              blocks: [{ type: "text", text: "hi" }],
              stopReason: "end_turn",
              usage: {
                input_tokens: 10,
                cache_creation_input_tokens: 5,
                cache_read_input_tokens: 7,
                cache_creation: { ephemeral_5m_input_tokens: 2, ephemeral_1h_input_tokens: 3 },
                output_tokens: 5,
                output_tokens_details: { thinking_tokens: 4 },
              },
            }),
          ],
        });
        const usage = terminal.usage;
        assert.deepStrictEqual(
          { ...usage, cost: undefined },
          {
            input: 10,
            output: 5,
            cacheRead: 7,
            cacheWrite: 5,
            cacheWrite1h: 3,
            reasoning: 4,
            totalTokens: 27,
            cost: undefined,
          },
        );
        // `calculateCost`: 1h writes bill at 2x input.
        assert.closeTo(usage.cost.cacheWrite, (0.125 * 2 + 0.1 * 2 * 3) / 1_000_000, 1e-12);
        assert.closeTo(usage.cost.input, (0.1 * 10) / 1_000_000, 1e-12);
        assert.closeTo(
          usage.cost.total,
          usage.cost.input + usage.cost.output + usage.cost.cacheRead + usage.cost.cacheWrite,
          1e-15,
        );
      }),
  );

  it.effect("a tier applies above its input threshold", () =>
    Effect.gen(function* () {
      const { terminal } = yield* send({
        canned: [
          anthropicStream({
            blocks: [{ type: "text", text: "hi" }],
            stopReason: "end_turn",
            usage: { input_tokens: 100_001 },
          }),
        ],
      });
      assert.closeTo(terminal.usage.cost.input, (0.5 * 100_001) / 1_000_000, 1e-12);
    }),
  );
});

// -----------------------------------------------------------------------------
// Errors
// -----------------------------------------------------------------------------

describe("errors become terminal messages with the provider text", () => {
  const cases: ReadonlyArray<readonly [string, Canned, string]> = [
    [
      "400 prompt too long",
      jsonError(
        400,
        "invalid_request_error",
        "prompt is too long: 4200057 tokens > 1000000 maximum",
      ),
      "prompt is too long: 4200057 tokens > 1000000 maximum",
    ],
    [
      "429 rate limit",
      jsonError(
        429,
        "rate_limit_error",
        "Number of request tokens has exceeded your per-minute rate limit",
      ),
      "Number of request tokens has exceeded your per-minute rate limit",
    ],
    ["529 overloaded", jsonError(529, "overloaded_error", "Overloaded"), "Overloaded"],
    [
      "error event mid-stream",
      anthropicStream({
        blocks: [{ type: "text", text: "partial" }],
        stopReason: "end_turn",
        tail: [
          {
            type: "error",
            error: { type: "overloaded_error", message: "Overloaded" },
            request_id: "req_test",
          },
        ],
      }),
      "overloaded_error: Overloaded",
    ],
  ];
  for (const [name, canned, text] of cases) {
    it.effect(name, () =>
      Effect.gen(function* () {
        const { terminal, requests } = yield* send({ canned: [canned] });
        assert.strictEqual(terminal.stopReason, "error");
        assert.include(terminal.errorMessage ?? "", text);
        // opencode does not retry: retries are the kernel's.
        assert.strictEqual(requests.length, 1);
      }),
    );
  }

  it.effect("a mid-stream error keeps the content so far", () =>
    Effect.gen(function* () {
      const { terminal } = yield* send({ canned: [cases[3]![1]] });
      assert.deepStrictEqual(terminal.content, [{ type: "text", text: "partial" }]);
    }),
  );

  it.effect(
    "a stream cut before message_stop keeps the content and ends with the protocol's text",
    () =>
      Effect.gen(function* () {
        const { terminal } = yield* send({
          canned: [
            anthropicStream({
              blocks: [{ type: "text", text: "partial" }],
              stopReason: "end_turn",
              tail: [],
            }),
          ],
        });
        assert.strictEqual(terminal.stopReason, "error");
        // opencode's own text is "The provider response ended unexpectedly.", which the kernel's retry patterns do not match.
        assert.strictEqual(terminal.errorMessage, "Anthropic stream ended before message_stop");
        assert.deepStrictEqual(terminal.content, [{ type: "text", text: "partial" }]);
      }),
  );
});

// -----------------------------------------------------------------------------
// Streaming
// -----------------------------------------------------------------------------

describe("streaming", () => {
  it.effect(
    "thinking, redacted thinking, text and a tool call: kernel events with pending partials, then the terminal message",
    () =>
      Effect.gen(function* () {
        const { events, messages, terminal } = yield* send({
          canned: [
            anthropicStream({
              blocks: [
                { type: "thinking", thinking: "Check the weather.", signature: "sig-1" },
                { type: "redacted_thinking", data: "opaque" },
                { type: "text", text: "Let me look." },
                { type: "tool_use", id: "toolu_1", name: "get_weather", input: { city: "Bogotá" } },
              ],
              stopReason: "tool_use",
            }),
          ],
        });
        assert.deepStrictEqual(terminal.content, [
          { type: "thinking", thinking: "Check the weather.", thinkingSignature: "sig-1" },
          {
            type: "thinking",
            thinking: "[Reasoning redacted]",
            thinkingSignature: "opaque",
            redacted: true,
          },
          { type: "text", text: "Let me look." },
          { type: "toolCall", id: "toolu_1", name: "get_weather", arguments: { city: "Bogotá" } },
        ]);
        assert.strictEqual(terminal.stopReason, "toolUse");
        const partials = messages.slice(0, -1);
        assert.isAbove(partials.length, 3);
        for (const partial of partials) assert.strictEqual(partial.stopReason, "pending");
        assert.strictEqual(
          messages.filter((message) => message.stopReason !== "pending").length,
          1,
        );
        // The kernel events: each block opens, grows by deltas and ends; the call's argument text streams raw.
        assert.deepStrictEqual(
          events.map((event) => event.type),
          [
            "start",
            "thinking_start",
            "thinking_delta",
            "thinking_end",
            "thinking_start",
            "thinking_end",
            "text_start",
            "text_delta",
            "text_end",
            "toolcall_start",
            "toolcall_delta",
            "toolcall_end",
            "done",
          ],
        );
        assert.deepStrictEqual(
          events.flatMap((event) => (event.type === "toolcall_delta" ? [event.delta] : [])),
          ['{"city":"Bogotá"}'],
        );
      }),
  );
});

// -----------------------------------------------------------------------------
// System messages
// -----------------------------------------------------------------------------

/** The parts of an Anthropic request body these tests read. */
const Body = Schema.Struct({
  system: Schema.optional(Schema.Array(Schema.Struct({ text: Schema.String }))),
  messages: Schema.Array(
    Schema.Struct({
      role: Schema.String,
      content: Schema.Array(
        Schema.Struct({
          type: Schema.String,
          text: Schema.optional(Schema.String),
          thinking: Schema.optional(Schema.String),
          source: Schema.optional(
            Schema.Struct({ type: Schema.String, media_type: Schema.String, data: Schema.String }),
          ),
          content: Schema.optional(Schema.Union([Schema.String, Schema.Array(Schema.Json)])),
          is_error: Schema.optional(Schema.Boolean),
          cache_control: Schema.optional(
            Schema.Struct({ type: Schema.String, ttl: Schema.optional(Schema.String) }),
          ),
        }),
      ),
    }),
  ),
  tools: Schema.optional(Schema.Array(Schema.Struct({ name: Schema.String }))),
  thinking: Schema.optional(Schema.Struct({ type: Schema.String })),
  output_config: Schema.optional(Schema.Struct({ effort: Schema.optional(Schema.String) })),
  metadata: Schema.optional(Schema.Struct({ user_id: Schema.optional(Schema.String) })),
  max_tokens: Schema.Number,
});

const sentBody = (input: SendInput = {}) =>
  send(input).pipe(
    Effect.flatMap(({ requests }) => Schema.decodeUnknownEffect(Body)(requests[0]!.body)),
  );

const withUpdate: ReadonlyArray<Message> = [system, question, answer, systemUpdate, followUp];

describe("a later system message", () => {
  it.effect("on Claude 5, kept in place: a native system message after the latest user turn", () =>
    Effect.gen(function* () {
      const body = yield* sentBody({ messages: withUpdate });
      assert.include(body.system![0]!.text, "Celsius");
      assert.deepStrictEqual(
        body.messages.map((m) => m.role),
        ["user", "assistant", "user", "system"],
      );
      assert.include(
        body.messages[3]!.content[0]!.text ?? "",
        'Updated system prompt section "units"',
      );
    }),
  );

  it.effect(
    "on a model without native support, kept in place: wrapped as user text, markup escaped",
    () =>
      Effect.gen(function* () {
        const body = yield* sentBody({
          messages: withUpdate,
          model: withId(haiku55, "claude-haiku-4-5"),
        });
        assert.deepStrictEqual(
          body.messages.map((m) => m.role),
          ["user", "assistant", "user", "user"],
        );
        const texts = body.messages[2]!.content.map((part) => part.text ?? "").join("\n");
        assert.include(texts, "<system-update>");
        assert.include(texts, "&lt;units&gt;");
      }),
  );

  it.effect(
    "without supportsMidConvoSystemMessages (the default): collapsed into the leading prompt",
    () =>
      Effect.gen(function* () {
        const body = yield* sentBody({
          messages: withUpdate,
          model: { ...haiku55, supportsMidConvoSystemMessages: false },
        });
        const prompt = body.system!.map((part) => part.text).join("\n");
        assert.include(prompt, "careful assistant");
        assert.include(prompt, "answer in Spanish");
        assert.include(prompt, "Fahrenheit");
        assert.notInclude(prompt, "Celsius");
        assert.deepStrictEqual(
          body.messages.map((m) => m.role),
          ["user", "assistant", "user"],
        );
      }),
  );

  it.effect("tools follow toolsAdded and toolsRemoved across system messages", () =>
    Effect.gen(function* () {
      const removed: SystemMessage = {
        role: "system",
        content: "",
        toolsRemoved: [{ name: "get_weather" }],
        timestamp: 4,
      };
      const added: SystemMessage = {
        role: "system",
        content: "",
        toolsAdded: [{ name: "search", description: "Search.", parameters: { type: "object" } }],
        timestamp: 5,
      };
      const body = yield* sentBody({
        messages: [system, question, answer, removed, added, followUp],
      });
      assert.deepStrictEqual(
        body.tools?.map((tool) => tool.name),
        ["search"],
      );
    }),
  );
});

// -----------------------------------------------------------------------------
// Request options
// -----------------------------------------------------------------------------

/** The message cache breakpoints of a body. */
const breakpoints = (body: typeof Body.Type) =>
  body.messages.flatMap((m) =>
    m.content.flatMap((part) => (part.cache_control ? [part.cache_control] : [])),
  );

describe("request options", () => {
  it.effect(
    "thinking level → adaptive thinking and effort, through the model's thinking level map",
    () =>
      Effect.gen(function* () {
        const levels: ReadonlyArray<readonly [ThinkingLevel | undefined, string | undefined]> = [
          [undefined, undefined],
          ["minimal", "low"],
          ["low", "low"],
          ["medium", "medium"],
          ["high", "high"],
          ["xhigh", "xhigh"],
          ["max", "max"],
        ];
        for (const [level, effort] of levels) {
          const body = yield* sentBody({ request: { reasoning: level } });
          assert.strictEqual(body.thinking?.type, level === undefined ? "disabled" : "adaptive");
          assert.strictEqual(body.output_config?.effort, effort);
          assert.strictEqual(body.max_tokens, haiku55.maxTokens);
        }
      }),
  );

  it.effect("a request's maxTokens caps the output instead of the model's", () =>
    Effect.gen(function* () {
      const body = yield* sentBody({ request: { maxTokens: 800 } });
      assert.strictEqual(body.max_tokens, 800);
    }),
  );

  it.effect("without a map entry, xhigh and max fall back to high", () =>
    Effect.gen(function* () {
      const model = { ...haiku55, thinkingLevelMap: undefined };
      for (const level of ["xhigh", "max"] as const) {
        const body = yield* sentBody({ model, request: { reasoning: level } });
        assert.strictEqual(body.output_config?.effort, "high");
      }
    }),
  );

  // No deferred responses in `@opencode/ai`; the option is ignored.
  it.effect("a request's deferred option changes nothing in the body", () =>
    Effect.gen(function* () {
      const deferred = yield* sentBody({ request: { options: { deferred: { window: "1h" } } } });
      assert.deepStrictEqual(deferred, yield* sentBody());
    }),
  );

  it.effect("metadata.user_id comes from the options' metadata", () =>
    Effect.gen(function* () {
      const body = yield* sentBody({ request: { options: { metadata: { user_id: "u1" } } } });
      assert.strictEqual(body.metadata?.user_id, "u1");
      const without = yield* sentBody({ request: { sessionId: "s1" } });
      assert.isUndefined(without.metadata);
    }),
  );

  // The generation's `beforeProviderRequest` hooks reach the body through `onPayload`.
  it.effect("onPayload replaces the provider body opencode built, for that request", () =>
    Effect.gen(function* () {
      const seen: Array<Schema.Json> = [];
      const { requests } = yield* send({
        request: {
          onPayload: (payload) =>
            Effect.sync(() => {
              seen.push(payload);
              const replaced: Schema.JsonObject = Schema.is(Schema.JsonObject)(payload)
                ? Object.assign({}, payload, { max_tokens: 7, extra: true })
                : {};
              return replaced;
            }),
        },
      });
      assert.strictEqual(seen.length, 1);
      const built = seen[0];
      assert.isTrue(Schema.is(Schema.JsonObject)(built));
      if (Schema.is(Schema.JsonObject)(built)) {
        assert.strictEqual(built["model"], haiku55.id);
        assert.isTrue(Array.isArray(built["messages"]));
      }
      const sent = requests[0]!.body;
      assert.isTrue(Schema.is(Schema.JsonObject)(sent));
      if (Schema.is(Schema.JsonObject)(sent)) {
        assert.strictEqual(sent["max_tokens"], 7);
        assert.strictEqual(sent["extra"], true);
      }
    }),
  );

  it.effect("per-request headers are sent", () =>
    Effect.gen(function* () {
      const { requests } = yield* send({ request: { options: { headers: { "x-durable": "1" } } } });
      assert.strictEqual(requests[0]!.headers["x-durable"], "1");
    }),
  );

  it.effect("cacheRetention: none sends no breakpoints, long asks for the 1h TTL", () =>
    Effect.gen(function* () {
      const none = yield* sentBody({ request: { options: { cacheRetention: "none" } } });
      assert.deepStrictEqual(breakpoints(none), []);
      const long = yield* sentBody({ request: { options: { cacheRetention: "long" } } });
      assert.deepStrictEqual(breakpoints(long), [{ type: "ephemeral", ttl: "1h" }]);
      const short = yield* sentBody();
      assert.deepStrictEqual(breakpoints(short), [{ type: "ephemeral" }]);
    }),
  );
});

// -----------------------------------------------------------------------------
// Images and replay
// -----------------------------------------------------------------------------

const PNG = "iVBORw0KGgo=";

const withImages: ReadonlyArray<Message> = [
  system,
  {
    role: "user",
    content: [
      { type: "text", text: "What is this?" },
      { type: "image", data: PNG, mimeType: "image/png" },
    ],
    timestamp: 2,
  },
  {
    ...answer,
    content: [
      { type: "toolCall", id: "toolu_1", name: "get_weather", arguments: { city: "Bogotá" } },
    ],
    stopReason: "toolUse",
  },
  {
    role: "toolResult",
    toolCallId: "toolu_1",
    toolName: "get_weather",
    content: [
      { type: "text", text: "radar" },
      { type: "image", data: PNG, mimeType: "image/png" },
      { type: "image", data: PNG, mimeType: "image/png" },
    ],
    isError: false,
    timestamp: 4,
  } satisfies ToolResultMessage,
];

describe("images", () => {
  it.effect("a model with image input gets the images, in user messages and tool results", () =>
    Effect.gen(function* () {
      const body = yield* sentBody({ messages: withImages });
      const user = body.messages[0]!.content;
      assert.deepStrictEqual(user[1]!.source, {
        type: "base64",
        media_type: "image/png",
        data: PNG,
      });
      const result = body.messages[2]!.content[0]!;
      assert.strictEqual(result.type, "tool_result");
      assert.deepStrictEqual(result.content, [
        { type: "text", text: "radar" },
        { type: "image", source: { type: "base64", media_type: "image/png", data: PNG } },
        { type: "image", source: { type: "base64", media_type: "image/png", data: PNG } },
      ]);
    }),
  );

  it.effect("a model without image input gets placeholders, one per run of images", () =>
    Effect.gen(function* () {
      const body = yield* sentBody({
        messages: withImages,
        model: { ...haiku55, input: ["text"] },
      });
      assert.deepStrictEqual(
        body.messages[0]!.content.map((part) => part.text),
        ["What is this?", "(image omitted: model does not support images)"],
      );
      assert.strictEqual(
        body.messages[2]!.content[0]!.content,
        "radar\n(tool image omitted: model does not support images)",
      );
    }),
  );
});

describe("replay of a response from another model", () => {
  it.effect("same model: the signed thinking is replayed; another model: it becomes text", () =>
    Effect.gen(function* () {
      const thinkingTurn: AssistantMessage = {
        ...answer,
        content: [
          { type: "thinking", thinking: "Hmm.", thinkingSignature: "sig-1" },
          {
            type: "thinking",
            thinking: "[Reasoning redacted]",
            thinkingSignature: "opaque",
            redacted: true,
          },
          { type: "text", text: "Probably fine." },
        ],
      };
      const same = yield* sentBody({ messages: [system, question, thinkingTurn, followUp] });
      assert.deepStrictEqual(
        same.messages[1]!.content.map((part) => part.type),
        ["thinking", "redacted_thinking", "text"],
      );
      const other = yield* sentBody({
        messages: [system, question, { ...thinkingTurn, model: "claude-sonnet-5-5" }, followUp],
      });
      assert.deepStrictEqual(
        other.messages[1]!.content.map((part) => [part.type, part.text]),
        [
          ["text", "Hmm."],
          ["text", "Probably fine."],
        ],
      );
    }),
  );
});

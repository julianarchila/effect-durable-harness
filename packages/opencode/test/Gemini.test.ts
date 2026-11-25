/**
 * The adapter against a synthetic Gemini wire: a replaying `fetch` under opencode's real Gemini code. Thought
 * signatures kept on the block they arrive with and replayed on the same model, thinking options, stop reasons and usage.
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
import { type Canned, geminiStream } from "./support/Http.ts";
import { gemini38, haiku55, run, withGeminiId } from "./support/Models.ts";

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
  content: "Before answering anything about weather, call get_weather.",
  toolsAdded: [weatherTool],
  timestamp: 1,
};

const question: UserMessage = { role: "user", content: "Weather in Bogotá?", timestamp: 2 };

// Thought signatures are base64 (`TYPE_BYTES`); only valid ones are replayed.
const THOUGHT_SIG = "dGhvdWdodA==";
const CALL_SIG = "Y2FsbA==";
const TEXT_SIG = "dGV4dA==";
const SKIP = "skip_thought_signature_validator";

const finishChunk = (finishReason: string): Schema.JsonObject => ({
  candidates: [{ finishReason }],
  usageMetadata: {
    promptTokenCount: 100,
    cachedContentTokenCount: 40,
    candidatesTokenCount: 10,
    thoughtsTokenCount: 5,
    totalTokenCount: 115,
  },
});

const parts = (...items: ReadonlyArray<Schema.JsonObject>): Schema.JsonObject => ({
  candidates: [{ content: { role: "model", parts: items } }],
});

const textReply = (finishReason = "STOP"): Canned =>
  geminiStream([parts({ text: "ok" }), finishChunk(finishReason)]);

interface SendInput {
  readonly canned?: ReadonlyArray<Canned> | undefined;
  readonly messages?: ReadonlyArray<Message> | undefined;
  readonly model?: OpencodeModel | undefined;
  readonly request?: Partial<GenerateRequest> | undefined;
}

const send = (input: SendInput = {}) =>
  run({
    canned: input.canned ?? [textReply()],
    messages: input.messages ?? [system, question],
    model: input.model ?? gemini38,
    request: input.request,
  });

/** The parts of the Gemini request body the tests read. */
const Part = Schema.Struct({
  text: Schema.optional(Schema.String),
  thought: Schema.optional(Schema.Boolean),
  thoughtSignature: Schema.optional(Schema.String),
  functionCall: Schema.optional(
    Schema.Struct({ id: Schema.optional(Schema.String), name: Schema.String }),
  ),
  functionResponse: Schema.optional(Schema.Struct({ name: Schema.String })),
});

const Body = Schema.Struct({
  contents: Schema.Array(Schema.Struct({ role: Schema.String, parts: Schema.Array(Part) })),
  generationConfig: Schema.optional(
    Schema.Struct({
      maxOutputTokens: Schema.optional(Schema.Number),
      thinkingConfig: Schema.optional(
        Schema.Struct({
          includeThoughts: Schema.optional(Schema.Boolean),
          thinkingLevel: Schema.optional(Schema.String),
          thinkingBudget: Schema.optional(Schema.Number),
        }),
      ),
    }),
  ),
});

const sentBody = (input: SendInput = {}) =>
  send(input).pipe(
    Effect.flatMap(({ requests }) => Schema.decodeUnknownEffect(Body)(requests[0]!.body)),
  );

// -----------------------------------------------------------------------------
// Thought signatures
// -----------------------------------------------------------------------------

describe("thought signatures", () => {
  it.effect("each block keeps the signature it arrives with", () =>
    Effect.gen(function* () {
      const { events, terminal } = yield* send({
        canned: [
          geminiStream([
            parts({ text: "Checking the weather.", thought: true, thoughtSignature: THOUGHT_SIG }),
            parts({ text: "Let me look.", thoughtSignature: TEXT_SIG }),
            parts({
              functionCall: { id: "call_1", name: "get_weather", args: { city: "Bogotá" } },
              thoughtSignature: CALL_SIG,
            }),
            finishChunk("STOP"),
          ]),
        ],
      });
      assert.strictEqual(terminal.stopReason, "toolUse");
      assert.deepStrictEqual(terminal.content, [
        { type: "thinking", thinking: "Checking the weather.", thinkingSignature: THOUGHT_SIG },
        { type: "text", text: "Let me look.", textSignature: TEXT_SIG },
        {
          type: "toolCall",
          id: "call_1",
          name: "get_weather",
          arguments: { city: "Bogotá" },
          thoughtSignature: CALL_SIG,
        },
      ]);
      // Gemini sends a call whole: it starts and ends in one step, with no argument text.
      assert.deepStrictEqual(
        events.filter((event) => event.type.startsWith("toolcall")).map((event) => event.type),
        ["toolcall_start", "toolcall_end"],
      );
    }),
  );

  it.effect("a signature sent only on a block's first delta is retained", () =>
    Effect.gen(function* () {
      const { terminal } = yield* send({
        canned: [
          geminiStream([
            parts({ text: "Sunny ", thoughtSignature: TEXT_SIG }),
            parts({ text: "and warm." }),
            finishChunk("STOP"),
          ]),
        ],
      });
      assert.deepStrictEqual(terminal.content, [
        { type: "text", text: "Sunny and warm.", textSignature: TEXT_SIG },
      ]);
    }),
  );

  const signedTurn: AssistantMessage = {
    role: "assistant",
    content: [
      { type: "thinking", thinking: "Checking the weather.", thinkingSignature: THOUGHT_SIG },
      { type: "text", text: "", textSignature: TEXT_SIG },
      {
        type: "toolCall",
        id: "call_1",
        name: "get_weather",
        arguments: { city: "Bogotá" },
        thoughtSignature: CALL_SIG,
      },
    ],
    api: gemini38.api,
    provider: gemini38.provider,
    model: gemini38.id,
    usage: emptyUsage(),
    stopReason: "toolUse",
    timestamp: 3,
  };

  const toolResult: ToolResultMessage = {
    role: "toolResult",
    toolCallId: "call_1",
    toolName: "get_weather",
    content: [{ type: "text", text: "22°C, sunny" }],
    isError: false,
    timestamp: 4,
  };

  const modelParts = (turn: AssistantMessage) =>
    sentBody({ messages: [system, question, turn, toolResult] }).pipe(
      Effect.map((body) => body.contents.find((content) => content.role === "model")?.parts),
    );

  it.effect("the same model gets every signature back, on the part it came with", () =>
    Effect.gen(function* () {
      assert.deepStrictEqual(yield* modelParts(signedTurn), [
        { text: "Checking the weather.", thought: true, thoughtSignature: THOUGHT_SIG },
        // An empty text part that carries a signature is kept.
        { text: "", thoughtSignature: TEXT_SIG },
        { functionCall: { id: "call_1", name: "get_weather" }, thoughtSignature: CALL_SIG },
      ]);
    }),
  );

  it.effect(
    "another model's turn replays without signatures; opencode marks the first call as unsigned",
    () =>
      Effect.gen(function* () {
        const other = yield* modelParts({ ...signedTurn, model: "gemini-3.5-flash" });
        assert.deepStrictEqual(other, [
          { text: "Checking the weather." },
          { functionCall: { id: "call_1", name: "get_weather" }, thoughtSignature: SKIP },
        ]);
      }),
  );

  it.effect("an invalid signature is not replayed (only base64 is sent)", () =>
    Effect.gen(function* () {
      const invalid = yield* modelParts({
        ...signedTurn,
        content: [
          {
            type: "toolCall",
            id: "call_1",
            name: "get_weather",
            arguments: { city: "Bogotá" },
            thoughtSignature: "not base64!",
          },
        ],
      });
      assert.deepStrictEqual(invalid, [
        { functionCall: { id: "call_1", name: "get_weather" }, thoughtSignature: SKIP },
      ]);
    }),
  );

  it.effect("an Anthropic turn replayed to Gemini keeps no Anthropic signature", () =>
    Effect.gen(function* () {
      const anthropicTurn: AssistantMessage = {
        ...signedTurn,
        api: haiku55.api,
        provider: haiku55.provider,
        model: haiku55.id,
        content: [{ type: "thinking", thinking: "Hmm.", thinkingSignature: "sig-1" }],
        stopReason: "stop",
      };
      assert.deepStrictEqual(yield* modelParts(anthropicTurn), [{ text: "Hmm." }]);
    }),
  );
});

// -----------------------------------------------------------------------------
// Thinking options
// -----------------------------------------------------------------------------

type ThinkingConfig = {
  readonly includeThoughts?: boolean | undefined;
  readonly thinkingLevel?: string | undefined;
  readonly thinkingBudget?: number | undefined;
};

const thinkingConfig = (model: OpencodeModel, reasoning: ThinkingLevel | undefined) =>
  sentBody({ model, request: { reasoning } }).pipe(
    Effect.map((body) => body.generationConfig?.thinkingConfig),
  );

describe("thinking options", () => {
  it.effect("Gemini 3 takes a thinking level, clamped to the levels the model supports", () =>
    Effect.gen(function* () {
      const table: ReadonlyArray<readonly [ThinkingLevel | undefined, ThinkingConfig]> = [
        // Thinking off: this model cannot disable it, so the request asks for its lowest level.
        [undefined, { thinkingLevel: "low", includeThoughts: false }],
        ["minimal", { thinkingLevel: "low", includeThoughts: true }],
        ["low", { thinkingLevel: "low", includeThoughts: true }],
        ["medium", { thinkingLevel: "medium", includeThoughts: true }],
        ["high", { thinkingLevel: "high", includeThoughts: true }],
        ["xhigh", { thinkingLevel: "high", includeThoughts: true }],
        ["max", { thinkingLevel: "high", includeThoughts: true }],
      ];
      for (const [level, expected] of table) {
        assert.deepStrictEqual(yield* thinkingConfig(gemini38, level), expected, String(level));
      }
    }),
  );

  it.effect("a mapped level translates, in any case; without a map, off is a zero budget", () =>
    Effect.gen(function* () {
      const mapped = { ...gemini38, thinkingLevelMap: { high: "LOW" } };
      assert.deepStrictEqual(yield* thinkingConfig(mapped, "high"), {
        thinkingLevel: "low",
        includeThoughts: true,
      });
      const unmapped = { ...gemini38, thinkingLevelMap: undefined };
      assert.deepStrictEqual(yield* thinkingConfig(unmapped, undefined), {
        thinkingBudget: 0,
        includeThoughts: false,
      });
    }),
  );

  it.effect("Gemini 2.5 takes a token budget; off is a zero budget", () =>
    Effect.gen(function* () {
      const flash = withGeminiId("gemini-2.5-flash", { thinkingLevelMap: undefined });
      assert.deepStrictEqual(yield* thinkingConfig(flash, undefined), {
        thinkingBudget: 0,
        includeThoughts: false,
      });
      assert.deepStrictEqual(yield* thinkingConfig(flash, "low"), {
        thinkingBudget: 2048,
        includeThoughts: true,
      });
      assert.deepStrictEqual(
        yield* thinkingConfig(
          withGeminiId("gemini-2.5-pro", { thinkingLevelMap: undefined }),
          "high",
        ),
        {
          thinkingBudget: 32768,
          includeThoughts: true,
        },
      );
    }),
  );

  it.effect("a model without reasoning sends no thinking options", () =>
    Effect.gen(function* () {
      const model = { ...gemini38, reasoning: false };
      assert.isUndefined(yield* thinkingConfig(model, undefined));
      assert.isUndefined(yield* thinkingConfig(model, "high"));
    }),
  );
});

// -----------------------------------------------------------------------------
// Stop reasons, usage and errors
// -----------------------------------------------------------------------------

describe("stop reasons", () => {
  const table: ReadonlyArray<readonly [string, StopReason, string | undefined]> = [
    ["STOP", "stop", undefined],
    ["MAX_TOKENS", "length", undefined],
    ["SAFETY", "error", "Provider stopped with: SAFETY"],
    ["RECITATION", "error", "Provider stopped with: RECITATION"],
  ];
  for (const [raw, stopReason, errorMessage] of table) {
    it.effect(raw, () =>
      Effect.gen(function* () {
        const { terminal } = yield* send({ canned: [textReply(raw)] });
        assert.strictEqual(terminal.stopReason, stopReason);
        assert.strictEqual(terminal.rawStopReason, raw);
        assert.strictEqual(terminal.errorMessage, errorMessage);
      }),
    );
  }

  it.effect("a stream that ends without a finish reason ends with the Google text", () =>
    Effect.gen(function* () {
      const { terminal } = yield* send({ canned: [geminiStream([parts({ text: "Sun" })])] });
      assert.strictEqual(terminal.stopReason, "error");
      assert.strictEqual(terminal.errorMessage, "Google stream ended without a finish reason");
      assert.deepStrictEqual(terminal.content, [{ type: "text", text: "Sun" }]);
    }),
  );
});

describe("usage", () => {
  it.effect(
    "cached input apart, thoughts counted as output and as reasoning, cost at the model's rates",
    () =>
      Effect.gen(function* () {
        const { terminal } = yield* send();
        const { cost, ...counts } = terminal.usage;
        // No `cacheWrite1h`, which only Anthropic reports.
        assert.deepStrictEqual(counts, {
          input: 60,
          output: 15,
          cacheRead: 40,
          cacheWrite: 0,
          reasoning: 5,
          totalTokens: 115,
        });
        assert.closeTo(cost.total, (60 * 0.3 + 15 * 2.5 + 40 * 0.03) / 1_000_000, 1e-12);
      }),
  );
});

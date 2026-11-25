/**
 * Two live turns against `gemini-3.8-flash`, the default model, through `OpencodeModels.layer` and the real
 * `fetch`: the model calls a tool, and the next request replays the call with the thought signature it came with.
 * Opt-in: it runs only under `test:live`, and is skipped without `GOOGLE_GENERATIVE_AI_API_KEY`.
 */
import { assert, describe, it } from "@effect/vitest";
import { Google } from "@opencode/ai/providers";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import type { Message, ToolResultMessage } from "@effect-durable-harness/core/Message";
import * as OpencodeModels from "@effect-durable-harness/opencode/OpencodeModels";
import { recordingFetch } from "../support/Http.ts";
import { collect, fetchLayer, gemini38 } from "../support/Models.ts";

const apiKey = process.env["GOOGLE_GENERATIVE_AI_API_KEY"];

const messages: ReadonlyArray<Message> = [
  {
    role: "system",
    content:
      "You are a careful assistant. Before answering anything about weather, call get_weather.",
    toolsAdded: [
      {
        name: "get_weather",
        description: "Current weather for a city: temperature in Celsius and sky.",
        parameters: {
          type: "object",
          properties: { city: { type: "string", description: "City name" } },
          required: ["city"],
          additionalProperties: false,
        },
      },
    ],
    timestamp: 1,
  },
  { role: "user", content: "What is the weather in Bogotá right now?", timestamp: 2 },
];

const SentParts = Schema.Struct({
  contents: Schema.Array(
    Schema.Struct({
      role: Schema.String,
      parts: Schema.Array(
        Schema.Struct({
          functionCall: Schema.optional(Schema.Struct({ name: Schema.String })),
          thoughtSignature: Schema.optional(Schema.String),
        }),
      ),
    }),
  ),
});

describe.skipIf(apiKey === undefined)("live turns (gemini-3.8-flash)", () => {
  it.live(
    "a tool call, then its result: the call goes back with its thought signature",
    () =>
      Effect.gen(function* () {
        const model: OpencodeModels.OpencodeModel = {
          ...gemini38,
          model: Google.configure({ apiKey: apiKey ?? "" }).model(gemini38.id),
        };
        const recorder = recordingFetch(globalThis.fetch);
        const layer = OpencodeModels.layer([model]).pipe(Layer.provide(fetchLayer(recorder.fetch)));

        const first = yield* collect(model, { messages, options: {} }).pipe(Effect.provide(layer));
        assert.isUndefined(first.terminal.errorMessage, first.terminal.errorMessage);
        assert.strictEqual(first.terminal.stopReason, "toolUse");
        const call = first.terminal.content.find((block) => block.type === "toolCall");
        assert.strictEqual(call?.type === "toolCall" ? call.name : undefined, "get_weather");
        const signature = call?.type === "toolCall" ? call.thoughtSignature : undefined;
        assert.isString(signature, "Gemini 3 signs its first function call");
        assert.isAbove(first.terminal.usage.input, 0);
        assert.isAbove(first.terminal.usage.cost.total, 0);

        const result: ToolResultMessage = {
          role: "toolResult",
          toolCallId: call?.id ?? "",
          toolName: "get_weather",
          content: [{ type: "text", text: "22°C, sunny" }],
          isError: false,
          timestamp: 4,
        };
        const second = yield* collect(model, {
          messages: [...messages, first.terminal, result],
          options: {},
        }).pipe(Effect.provide(layer));
        assert.isUndefined(second.terminal.errorMessage, second.terminal.errorMessage);
        assert.strictEqual(second.terminal.stopReason, "stop");
        assert.isTrue(second.terminal.content.some((block) => block.type === "text"));

        const sent = yield* Schema.decodeUnknownEffect(SentParts)(recorder.requests[1]?.body);
        const replayed = sent.contents
          .flatMap((content) => content.parts)
          .find((part) => part.functionCall !== undefined);
        assert.strictEqual(replayed?.thoughtSignature, signature);
      }),
    60_000,
  );
});

/**
 * One live turn against `claude-haiku-5-5` through `OpencodeModels.layer` and the real `fetch`: a system message with a
 * tool and a question that needs it; the model answers with a tool call. Opt-in: it runs only under
 * `bun run --cwd packages/opencode test:live`, never in `test`, and is skipped without `ANTHROPIC_API_KEY`.
 */
import { assert, describe, it } from "@effect/vitest";
import { Anthropic } from "@opencode/ai/providers";
import * as Effect from "effect/Effect";
import * as FetchHttpClient from "effect/http/FetchHttpClient";
import * as Layer from "effect/Layer";
import type { Message } from "@effect-durable-harness/core/Message";
import * as OpencodeModels from "@effect-durable-harness/opencode/OpencodeModels";
import { collect, haiku55 } from "../support/Models.ts";

const apiKey = process.env["ANTHROPIC_API_KEY"];

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

describe.skipIf(apiKey === undefined)("live turn (claude-haiku-5-5)", () => {
  it.live(
    "answers with a get_weather call, with usage and cost",
    () =>
      Effect.gen(function* () {
        const model: OpencodeModels.OpencodeModel = {
          ...haiku55,
          model: Anthropic.configure({ apiKey: apiKey ?? "" }).model(haiku55.id),
        };
        const { terminal } = yield* collect(model, { messages, options: {} }).pipe(
          Effect.provide(OpencodeModels.layer([model]).pipe(Layer.provide(FetchHttpClient.layer))),
        );
        assert.isUndefined(terminal.errorMessage, terminal.errorMessage);
        assert.strictEqual(terminal.stopReason, "toolUse");
        const call = terminal.content.find((block) => block.type === "toolCall");
        assert.strictEqual(call?.type === "toolCall" ? call.name : undefined, "get_weather");
        assert.isString(call?.type === "toolCall" ? call.arguments["city"] : undefined);
        assert.isAbove(terminal.usage.input, 0);
        assert.isAbove(terminal.usage.cost.total, 0);
      }),
    60_000,
  );
});

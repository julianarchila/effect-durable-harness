/**
 * The service over opencode's scripted client, `TestLLM`: resolution, a tool turn whose replay is inspected in the next request, a
 * failure after partial content, and interruption.
 *
 * `Models.stream` aborts by interruption and leaves the `aborted` message to the kernel.
 */
import { assert, describe, it } from "@effect/vitest";
import { AIError, LLMEvent, ProviderInternalError } from "@opencode/ai/effect";
import { TestLLM } from "@opencode/ai/testing";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { Message, type Message as MessageType } from "@effect-durable-harness/core/Message";
import { Models } from "@effect-durable-harness/core/Models";
import * as OpencodeModels from "@effect-durable-harness/opencode/OpencodeModels";
import { collect, haiku55, messageOf } from "./support/Models.ts";

const testLayer = OpencodeModels.layerFromClient([haiku55]).pipe(
  Layer.provideMerge(TestLLM.testLayer()),
);

const prefix: ReadonlyArray<MessageType> = [
  {
    role: "system",
    content: "Call get_weather before answering.",
    toolsAdded: [{ name: "get_weather", description: "Weather.", parameters: { type: "object" } }],
    timestamp: 1,
  },
  { role: "user", content: "Weather in Bogotá?", timestamp: 2 },
];

const ModelColumn = Schema.fromJsonString(Schema.Array(Message));

const roundTrip = (messages: ReadonlyArray<MessageType>) =>
  Schema.encodeEffect(ModelColumn)(messages).pipe(Effect.flatMap(Schema.decodeEffect(ModelColumn)));

describe("resolve", () => {
  it.effect("matches a ModelRef by provider and model id", () =>
    Effect.gen(function* () {
      const models = yield* Models;
      const found = yield* models.resolve({ provider: "anthropic", modelId: "claude-haiku-5-5" });
      assert.isTrue(Option.isSome(found));
      assert.strictEqual(Option.getOrThrow(found).contextWindow, haiku55.contextWindow);
      const otherProvider = yield* models.resolve({
        provider: "openai",
        modelId: "claude-haiku-5-5",
      });
      assert.isTrue(Option.isNone(otherProvider));
      const otherId = yield* models.resolve({ provider: "anthropic", modelId: "claude-opus-5" });
      assert.isTrue(Option.isNone(otherId));
    }).pipe(Effect.provide(testLayer)),
  );
});

// `@opencode/ai` has no deferred responses for language models.
describe("deferred responses", () => {
  it.effect("are not offered", () =>
    Effect.gen(function* () {
      const models = yield* Models;
      assert.isUndefined(models.fetchDeferred);
      assert.isUndefined(models.cancelDeferred);
    }).pipe(Effect.provide(testLayer)),
  );
});

describe("stream over TestLLM", () => {
  const signed = { anthropic: { signature: "sig-1" } };

  it.effect(
    "a tool turn; the next request replays the signature and sends the tool as JSON Schema",
    () =>
      Effect.gen(function* () {
        const test = yield* TestLLM.Test;
        yield* test.push(
          TestLLM.toolCalls(
            LLMEvent.reasoningStart({ id: "r0" }),
            LLMEvent.reasoningDelta({ id: "r0", text: "Check the weather first." }),
            LLMEvent.reasoningEnd({ id: "r0", providerMetadata: signed }),
            LLMEvent.toolCall({ id: "call_1", name: "get_weather", input: { city: "Bogotá" } }),
          ),
          TestLLM.text("Light rain, 14 °C.", "t0"),
        );
        const first = (yield* collect(haiku55, {
          messages: prefix,
          reasoning: "high",
          options: {},
        })).terminal;
        assert.strictEqual(first.stopReason, "toolUse");
        assert.deepStrictEqual(first.content, [
          { type: "thinking", thinking: "Check the weather first.", thinkingSignature: "sig-1" },
          { type: "toolCall", id: "call_1", name: "get_weather", arguments: { city: "Bogotá" } },
        ]);

        const result = {
          role: "toolResult",
          toolCallId: "call_1",
          toolName: "get_weather",
          content: [{ type: "text", text: "14 °C, light rain" }],
          isError: false,
          timestamp: 4,
        } as const;
        const transcript = yield* roundTrip([...prefix, first, result]);
        const second = (yield* collect(haiku55, { messages: transcript, options: {} })).terminal;
        assert.deepStrictEqual(second.content, [{ type: "text", text: "Light rain, 14 °C." }]);
        assert.strictEqual(second.stopReason, "stop");

        const requests = yield* test.requests();
        const tool = requests[1]!.tools[0]!;
        assert.strictEqual(tool.type === "tool" ? tool.name : undefined, "get_weather");
        const assistant = requests[1]!.messages.find((m) => m.role === "assistant")!;
        assert.deepInclude(assistant.content[0], {
          type: "reasoning",
          text: "Check the weather first.",
          providerMetadata: signed,
        });
        const toolMessage = requests[1]!.messages.find((m) => m.role === "tool")!;
        assert.deepInclude(toolMessage.content[0], {
          type: "tool-result",
          id: "call_1",
          result: { type: "text", value: "14 °C, light rain" },
        });
      }).pipe(Effect.provide(testLayer)),
  );

  it.effect(
    "a provider failure mid-stream ends `error` with the text and keeps the partial content",
    () =>
      Effect.gen(function* () {
        const test = yield* TestLLM.Test;
        yield* test.push(
          TestLLM.failAfter(
            new AIError({ reason: new ProviderInternalError({ message: "Overloaded" }) }),
            LLMEvent.textDelta({ id: "t0", text: "Partial" }),
          ),
        );
        const { messages, terminal } = yield* collect(haiku55, { messages: prefix, options: {} });
        assert.strictEqual(terminal.stopReason, "error");
        assert.strictEqual(terminal.errorMessage, "Overloaded");
        assert.deepStrictEqual(terminal.content, [{ type: "text", text: "Partial" }]);
        assert.deepStrictEqual(
          messages.map((message) => message.stopReason),
          ["pending", "pending", "pending", "error"],
        );
      }).pipe(Effect.provide(testLayer)),
  );

  it.effect("interrupting the stream after the first partial stops the request", () =>
    Effect.gen(function* () {
      const test = yield* TestLLM.Test;
      yield* test.push(TestLLM.hangAfter(LLMEvent.textDelta({ id: "t0", text: "Partial" })));
      const models = yield* Models;
      // The first partial is the third event: start, text_start, text_delta.
      const taken = yield* Stream.runCollect(
        models.stream(haiku55, { messages: prefix, options: {} }).pipe(Stream.take(3)),
      );
      assert.deepStrictEqual(
        taken.map((event) => event.type),
        ["start", "text_start", "text_delta"],
      );
      assert.deepStrictEqual(
        taken.map(messageOf).map((message) => [message.stopReason, message.content]),
        [
          ["pending", []],
          ["pending", [{ type: "text", text: "" }]],
          ["pending", [{ type: "text", text: "Partial" }]],
        ],
      );
      assert.strictEqual((yield* test.requests()).length, 1);
    }).pipe(Effect.provide(testLayer)),
  );

  it.effect("a model outside the layer is broken wiring: the stream dies", () =>
    Effect.gen(function* () {
      const models = yield* Models;
      const exit = yield* Effect.exit(
        Stream.runCollect(
          models.stream({ ...haiku55, id: "claude-opus-5" }, { messages: prefix, options: {} }),
        ),
      );
      assert.isTrue(exit._tag === "Failure");
    }).pipe(Effect.provide(testLayer)),
  );
});

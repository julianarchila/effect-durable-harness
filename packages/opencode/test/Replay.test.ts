/**
 * Offline replay of conversations recorded against the real API: each recorded SSE fed back through opencode's Anthropic code gives the recorded messages, and the
 * adapter sends the recorded request bodies. The fixtures are a regression suite for the adapter.
 *
 * The request options are `GenerateRequest`'s; `thinkingLevel` is not written by the adapter (the kernel writes it),
 * so it is left out of the comparison.
 */
import { assert, describe, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { type AssistantMessage, Message } from "@effect-durable-harness/core/Message";
import { haiku55, load, run } from "./support/Models.ts";

const Exchange = Schema.Struct({
  body: Schema.optional(Schema.Json),
  status: Schema.Number,
  contentType: Schema.String,
  response: Schema.String,
});

const Conversation = Schema.Struct({
  transcript: Schema.Array(Message),
  exchanges: Schema.Array(Exchange),
});

const Overflow = Schema.Struct({
  message: Schema.Struct({ errorMessage: Schema.String }),
  exchanges: Schema.Array(Exchange),
});

/** The transcript before each assistant message: the inputs of the recorded turns. */
const turnInputs = (transcript: ReadonlyArray<Message>) =>
  transcript.flatMap((message, index) =>
    message.role === "assistant" ? [{ input: transcript.slice(0, index), output: message }] : [],
  );

const comparable = (message: AssistantMessage) => {
  const { timestamp: _t, durationMs: _d, thinkingLevel: _l, ...rest } = message;
  return rest;
};

describe("replay of the recorded conversation", () => {
  it.effect("same messages, same request bodies, partials before one terminal message", () =>
    Effect.gen(function* () {
      const recording = yield* load("opencode-ai.conversation.json", Conversation);
      const turns = turnInputs(recording.transcript);
      assert.strictEqual(turns.length, 3);
      for (const [index, turn] of turns.entries()) {
        const result = yield* run({
          canned: [recording.exchanges[index]!],
          messages: turn.input,
          request: {
            // As recorded: `max` on the tool turn, `high` after.
            reasoning: index === 0 ? "max" : "high",
            sessionId: "opencode-replay",
            options: { metadata: { user_id: "opencode-replay" } },
          },
        });
        assert.deepStrictEqual(comparable(result.terminal), comparable(turn.output));
        assert.deepStrictEqual(
          result.requests.map((request) => request.body),
          [recording.exchanges[index]!.body],
        );
        const partials = result.messages.slice(0, -1);
        assert.isAbove(partials.length, 0);
        for (const partial of partials) assert.strictEqual(partial.stopReason, "pending");
        assert.isDefined(result.terminal.durationMs);
      }
    }),
  );

  it.effect("the recorded 400 overflow ends `error` with the provider text intact", () =>
    Effect.gen(function* () {
      const recording = yield* load("opencode-ai.overflow.json", Overflow);
      const result = yield* run({
        canned: recording.exchanges,
        messages: [{ role: "user", content: "lorem ipsum", timestamp: 1 }],
      });
      assert.strictEqual(result.messages.length, 1);
      assert.strictEqual(result.terminal.stopReason, "error");
      assert.strictEqual(result.terminal.errorMessage, recording.message.errorMessage);
      assert.strictEqual(
        result.terminal.errorMessage,
        "prompt is too long: 4200057 tokens > 1000000 maximum",
      );
      assert.deepStrictEqual(result.terminal.content, []);
      assert.strictEqual(result.terminal.api, haiku55.api);
    }),
  );
});

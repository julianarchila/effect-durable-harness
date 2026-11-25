/**
 * Cost tests: a pricing tier against `calculateCost`, and 1h cache writes through the adapter over a synthetic
 * Anthropic wire, since the adapter is what parses the stream's usage here.
 */
import { assert, describe, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import type { Message } from "@effect-durable-harness/core/Message";
import type { ResolvedModel } from "@effect-durable-harness/core/Models";
import type { OpencodeModel } from "@effect-durable-harness/opencode/OpencodeModels";
import { calculateCost } from "../src/internal/assembler.ts";
import { type SseEvent, sse } from "./support/Http.ts";
import { haiku55, run } from "./support/Models.ts";

const model = (cost: ResolvedModel["cost"]): ResolvedModel => ({
  provider: "test",
  id: "test-model",
  api: "test-api",
  contextWindow: 10000,
  maxTokens: 1000,
  reasoning: false,
  input: ["text"],
  cost,
});

// It captures nothing, so it lives outside the test.
const createUsage = (cacheWrite: number) => ({
  input: 200000,
  output: 100000,
  cacheRead: 72000,
  cacheWrite,
  totalTokens: 372000 + cacheWrite,
});

describe("calculateCost", () => {
  // models-runtime.test.ts:126
  it("applies request-wide pricing tiers above the configured input threshold", () => {
    const tiered = model({
      input: 5,
      output: 30,
      cacheRead: 0.5,
      cacheWrite: 6.25,
      tiers: [{ inputTokensAbove: 272000, input: 10, output: 45, cacheRead: 1, cacheWrite: 12.5 }],
    });
    const short = calculateCost(tiered, createUsage(0));
    assert.strictEqual(short.input, 1);
    assert.strictEqual(short.output, 3);
    assert.strictEqual(short.cacheRead, 0.036);
    assert.strictEqual(short.cacheWrite, 0);

    const long = calculateCost(tiered, createUsage(1));
    assert.strictEqual(long.input, 2);
    assert.strictEqual(long.output, 4.5);
    assert.strictEqual(long.cacheRead, 0.072);
    assert.strictEqual(long.cacheWrite, 0.0000125);
  });
});

/** An Anthropic stream whose usage reports cache creation, with or without a TTL breakdown. */
const eventsWithCacheCreation = (
  cacheCreation: { readonly [key: string]: number } | undefined,
): ReadonlyArray<SseEvent> => {
  const counts = {
    input_tokens: 100,
    output_tokens: 0,
    cache_read_input_tokens: 0,
    cache_creation_input_tokens: 1_000_000,
  };
  const startUsage =
    cacheCreation === undefined ? counts : { ...counts, cache_creation: cacheCreation };
  return [
    { type: "message_start", message: { id: "msg_test", usage: startUsage } },
    { type: "content_block_start", index: 0, content_block: { type: "text", text: "" } },
    { type: "content_block_delta", index: 0, delta: { type: "text_delta", text: "Hi" } },
    { type: "content_block_stop", index: 0 },
    {
      type: "message_delta",
      delta: { stop_reason: "end_turn" },
      usage: {
        input_tokens: 100,
        output_tokens: 5,
        cache_read_input_tokens: 0,
        cache_creation_input_tokens: 1_000_000,
      },
    },
    { type: "message_stop" },
  ];
};

// claude-opus-4-8: input 5, cacheWrite (5m) 6.25 per Mtok. 1h write = 2x input = 10.
const opus: OpencodeModel = {
  ...haiku55,
  cost: { input: 5, output: 25, cacheRead: 0.5, cacheWrite: 6.25 },
};

const messages: ReadonlyArray<Message> = [{ role: "user", content: "hi", timestamp: 1 }];

const generate = (events: ReadonlyArray<SseEvent>, target: OpencodeModel = opus) =>
  run({ canned: [sse(events)], messages, model: target }).pipe(
    Effect.map(({ terminal }) => terminal),
  );

describe("Anthropic 1h cache write cost", () => {
  it.effect("prices the 1h portion at 2x input and the rest at the 5m rate", () =>
    Effect.gen(function* () {
      const result = yield* generate(
        eventsWithCacheCreation({
          ephemeral_5m_input_tokens: 600_000,
          ephemeral_1h_input_tokens: 400_000,
        }),
      );
      assert.strictEqual(result.usage.cacheWrite, 1_000_000);
      assert.strictEqual(result.usage.cacheWrite1h, 400_000);
      // 600k * 6.25/Mtok + 400k * 10/Mtok = 3.75 + 4.0 = 7.75
      assert.closeTo(result.usage.cost.cacheWrite, 7.75, 1e-10);
    }),
  );

  // Regression: Vercel AI Gateway sends cache usage in message_delta, not message_start. Here it runs on the test
  // model's own rates.
  it.effect("prices 1h cache writes reported only in message_delta", () =>
    Effect.gen(function* () {
      const result = yield* generate(
        [
          {
            type: "message_start",
            message: { id: "msg_test", usage: { input_tokens: 0, output_tokens: 0 } },
          },
          {
            type: "message_delta",
            delta: { stop_reason: "end_turn" },
            usage: {
              input_tokens: 3,
              output_tokens: 4,
              cache_creation_input_tokens: 6535,
              cache_creation: { ephemeral_5m_input_tokens: 0, ephemeral_1h_input_tokens: 6535 },
            },
          },
          { type: "message_stop" },
        ],
        haiku55,
      );
      assert.strictEqual(result.usage.cacheWrite, 6535);
      assert.strictEqual(result.usage.cacheWrite1h, 6535);
      assert.closeTo(
        result.usage.cost.cacheWrite,
        (6535 * haiku55.cost.input * 2) / 1_000_000,
        1e-10,
      );
    }),
  );

  it.effect("falls back to the 5m rate when no breakdown is reported", () =>
    Effect.gen(function* () {
      const result = yield* generate(eventsWithCacheCreation(undefined));
      assert.strictEqual(result.usage.cacheWrite, 1_000_000);
      // Anthropic Messages writes 0 when no breakdown is reported.
      assert.strictEqual(result.usage.cacheWrite1h, 0);
      // 1M * 6.25/Mtok = 6.25
      assert.closeTo(result.usage.cost.cacheWrite, 6.25, 1e-10);
    }),
  );
});

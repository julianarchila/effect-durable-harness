/**
 * The model the tests run, the layer over a replaying `fetch`, and fixture loading.
 */
import * as NodeFileSystem from "@effect/platform-node/NodeFileSystem";
import { Anthropic, Google } from "@opencode/ai/providers";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as FetchHttpClient from "effect/http/FetchHttpClient";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import type { AssistantMessage, Message } from "@effect-durable-harness/core/Message";
import {
  type AssistantMessageEvent,
  type GenerateRequest,
  Models,
} from "@effect-durable-harness/core/Models";
import * as OpencodeModels from "@effect-durable-harness/opencode/OpencodeModels";
import { type Canned, replayFetch } from "./Http.ts";

/**
 * `claude-haiku-5-5` as the fixtures recorded it: $0.10 / $0.50 up to 100K
 * prompt tokens, $0.50 / $2.50 beyond. `api` is the value the recording wrote, so the recorded responses count as this
 * model's own and their signatures are replayed. `maxTokens` is the cap the recording requested.
 */
export const haiku55: OpencodeModels.OpencodeModel = {
  provider: "anthropic",
  id: "claude-haiku-5-5",
  api: "opencode-ai:anthropic-messages",
  model: Anthropic.configure({ apiKey: "test-key" }).model("claude-haiku-5-5"),
  contextWindow: 1_000_000,
  maxTokens: 4096,
  reasoning: true,
  input: ["text", "image"],
  supportsMidConvoSystemMessages: true,
  thinkingLevelMap: { xhigh: "xhigh", max: "max" },
  cost: {
    input: 0.1,
    output: 0.5,
    cacheRead: 0.01,
    cacheWrite: 0.125,
    tiers: [
      { inputTokensAbove: 100_000, input: 0.5, output: 2.5, cacheRead: 0.05, cacheWrite: 0.625 },
    ],
  },
};

/**
 * `gemini-3.8-flash` on the Gemini API, the default model. The rates are illustrative, not Google's price list:
 * the tests check that cost follows usage.
 */
export const gemini38: OpencodeModels.OpencodeModel = {
  provider: "google",
  id: "gemini-3.8-flash",
  api: "opencode-ai:gemini",
  model: Google.configure({ apiKey: "test-key" }).model("gemini-3.8-flash"),
  contextWindow: 1_048_576,
  maxTokens: 65_536,
  reasoning: true,
  input: ["text", "image"],
  // Thinking cannot be turned off on this model.
  thinkingLevelMap: { off: null, minimal: null, xhigh: null, max: null },
  cost: { input: 0.3, output: 2.5, cacheRead: 0.03, cacheWrite: 0 },
};

/** The same metadata on another opencode model id. */
export const withId = (
  model: OpencodeModels.OpencodeModel,
  id: string,
): OpencodeModels.OpencodeModel => ({
  ...model,
  id,
  model: Anthropic.configure({ apiKey: "test-key" }).model(id),
});

export const fetchLayer = (fetch: typeof globalThis.fetch) =>
  FetchHttpClient.layer.pipe(Layer.provide(Layer.succeed(FetchHttpClient.Fetch, fetch)));

export interface Generated {
  /** Every event the stream emitted, the terminal last. */
  readonly events: ReadonlyArray<AssistantMessageEvent>;
  /** The message each event carries: its partial, or the terminal message last. */
  readonly messages: ReadonlyArray<AssistantMessage>;
  readonly terminal: AssistantMessage;
}

/** The message an event carries: its partial, or the terminal message. */
export const messageOf = (event: AssistantMessageEvent): AssistantMessage =>
  event.type === "done" ? event.message : event.type === "error" ? event.error : event.partial;

/** Collects one `Models.stream` run. */
export const collect = (model: OpencodeModels.OpencodeModel, request: GenerateRequest) =>
  Effect.gen(function* () {
    const models = yield* Models;
    const events = yield* Stream.runCollect(models.stream(model, request));
    const messages = events.map(messageOf);
    const terminal = messages.at(-1);
    if (terminal === undefined) return yield* Effect.die("the stream emitted nothing");
    return { events, messages, terminal } satisfies Generated;
  });

export interface RunInput {
  readonly canned: ReadonlyArray<Canned>;
  readonly messages: ReadonlyArray<Message>;
  readonly model?: OpencodeModels.OpencodeModel | undefined;
  readonly request?: Partial<GenerateRequest> | undefined;
}

/** One request through `OpencodeModels.layer` over a replaying `fetch`: the result and the requests sent. */
export const run = (input: RunInput) => {
  const model = input.model ?? haiku55;
  const fake = replayFetch(input.canned);
  return collect(model, { messages: input.messages, options: {}, ...input.request }).pipe(
    Effect.provide(OpencodeModels.layer([model]).pipe(Layer.provide(fetchLayer(fake.fetch)))),
    Effect.map((generated) => ({ ...generated, requests: fake.requests })),
  );
};

const FIXTURES = new URL("../fixtures/", import.meta.url).pathname;

/** Reads a fixture, decoded with `schema`. */
export const load = <S extends Schema.Codec<unknown, unknown>>(name: string, schema: S) =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const text = yield* fs.readFileString(FIXTURES + name);
    return yield* Schema.decodeEffect(Schema.fromJsonString(schema))(text);
  }).pipe(Effect.provide(NodeFileSystem.layer), Effect.orDie);

/** The Gemini metadata on another Gemini model id. */
export const withGeminiId = (
  id: string,
  overrides: Partial<OpencodeModels.OpencodeModel> = {},
): OpencodeModels.OpencodeModel => ({
  ...gemini38,
  id,
  model: Google.configure({ apiKey: "test-key" }).model(id),
  ...overrides,
});

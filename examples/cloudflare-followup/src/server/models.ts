/**
 * The model services: Gemini through `@effect-durable-harness/opencode`, or with `FOLLOWUP_MODEL=faux` a fake that the workerd
 * tests drive with no key.
 */
import { Google } from "@opencode/ai/providers";
import type { Models } from "@effect-durable-harness/core/Models";
import * as FauxModel from "@effect-durable-harness/core/testing/FauxModel";
import * as OpencodeModels from "@effect-durable-harness/opencode/OpencodeModels";
import * as Config from "effect/Config";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Predicate from "effect/Predicate";
import * as Redacted from "effect/Redacted";
import * as FetchHttpClient from "effect/http/FetchHttpClient";
import { isFollowUpInput } from "../protocol.ts";
import { AssistantModel } from "./agent/assistant.ts";

export const GEMINI = { provider: "google", modelId: "gemini-3.8-flash" } as const;
export const FAUX = { provider: "faux", modelId: "faux-1" } as const;

/**
 * `Models` and `AssistantModel`, from the Worker's env: the fake with `FOLLOWUP_MODEL=faux` or without a key, Gemini
 * otherwise. Read when the Worker's init builds the object's Layer, so Alchemy binds both; `doppler run --`
 * provides the key.
 */
export const ModelsLive: Layer.Layer<Models | AssistantModel> = Layer.unwrap(
  Effect.gen(function* () {
    const model = yield* Config.option(Config.String("FOLLOWUP_MODEL"));
    const apiKey = yield* Config.option(Config.Redacted("GOOGLE_GENERATIVE_AI_API_KEY"));
    if (Option.getOrUndefined(model) === "faux" || Option.isNone(apiKey)) {
      return Layer.mergeAll(fakeModel, Layer.succeed(AssistantModel, { ref: FAUX }));
    }
    return Layer.mergeAll(
      OpencodeModels.layer([gemini(Redacted.value(apiKey.value))]).pipe(
        Layer.provide(FetchHttpClient.layer),
      ),
      Layer.succeed(AssistantModel, { ref: GEMINI }),
    );
  }).pipe(Effect.orDie),
);

const gemini = (apiKey: string): OpencodeModels.OpencodeModel => ({
  provider: GEMINI.provider,
  id: GEMINI.modelId,
  api: "opencode-ai:gemini",
  model: Google.configure({ apiKey }).model(GEMINI.modelId),
  contextWindow: 1_048_576,
  maxTokens: 65_536,
  reasoning: true,
  input: ["text", "image"],
  thinkingLevelMap: { off: null, minimal: null, xhigh: null, max: null },
  cost: { input: 0.3, output: 2.5, cacheRead: 0.03, cacheWrite: 0 },
});

/**
 * The fake model answers from the request alone, so it behaves the same after a restart: a follow-up's input gets
 * a fixed follow-up, any other user message `reply: <text>`. Steered messages share one answer, which joins them.
 */
const fakeModel = Layer.unwrap(
  Effect.sync(() => {
    const faux = FauxModel.make({ tokensPerSecond: 200 });
    const respond: FauxModel.FauxResponseFactory = (request) =>
      Effect.sync(() => {
        faux.appendResponses([respond]);
        // Every user message since the last answer: steered messages arrive together.
        const lastAnswer = request.messages.findLastIndex(
          (message) => message.role === "assistant",
        );
        const pending = request.messages
          .slice(lastAnswer + 1)
          .flatMap((message) => (message.role === "user" ? [textOf(message.content)] : []));
        if (pending.some(isFollowUpInput)) {
          return FauxModel.text(
            "follow-up: Just checking in. Is there anything else I can help with?",
          );
        }
        return FauxModel.text(`reply: ${pending.join(" + ")}`);
      });
    faux.setResponses([respond]);
    return faux.layer;
  }),
);

const textOf = (
  content: string | ReadonlyArray<{ readonly type: string; readonly text?: string }>,
): string =>
  Predicate.isString(content)
    ? content
    : content.map((part) => (part.type === "text" ? (part.text ?? "") : "")).join("");

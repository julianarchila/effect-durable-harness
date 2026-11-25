/**
 * The model services: Gemini through `@effect-durable-harness/opencode`, or with `CHAT_MODEL=faux` a scripted fake that the
 * workerd tests drive with no key.
 */
import { Google } from "@opencode/ai/providers";
import type { Message } from "@effect-durable-harness/core/Message";
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
import { parseAttribution, parseScheduledInput } from "../protocol.ts";
import { ChatModel } from "./agent/chat.ts";

export const GEMINI = { provider: "google", modelId: "gemini-3.8-flash" } as const;
export const FAUX = { provider: "faux", modelId: "faux-1" } as const;

/**
 * `Models` and `ChatModel`, from the Worker's env: the fake with `CHAT_MODEL=faux` or without a key, Gemini
 * otherwise. Read when the Worker's init builds the object's Layer, so Alchemy binds both; `doppler run --`
 * provides them.
 */
export const ModelsLive: Layer.Layer<Models | ChatModel> = Layer.unwrap(
  Effect.gen(function* () {
    const chatModel = yield* Config.option(Config.String("CHAT_MODEL"));
    const apiKey = yield* Config.option(Config.Redacted("GOOGLE_GENERATIVE_AI_API_KEY"));
    if (Option.getOrUndefined(chatModel) === "faux" || Option.isNone(apiKey)) {
      return Layer.mergeAll(fakeModel, Layer.succeed(ChatModel, { ref: FAUX }));
    }
    return Layer.mergeAll(
      OpencodeModels.layer([gemini(Redacted.value(apiKey.value))]).pipe(
        Layer.provide(FetchHttpClient.layer),
      ),
      Layer.succeed(ChatModel, { ref: GEMINI }),
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

const textOf = (message: Message | undefined): string => {
  if (message === undefined || message.role === "system") return "";
  if (Predicate.isString(message.content)) return message.content;
  return message.content.flatMap((part) => (part.type === "text" ? [part.text] : [])).join("\n");
};

/**
 * The fake model answers from the request alone, so it behaves the same after a restart; `FauxModel.Test`'s
 * scripted queue would not survive one, and the tests drive the object from outside it. On a user message
 * (attribution stripped):
 *
 * - `weather <place>`, `dice`, `schedule <seconds> <instruction>`, `search <question>` call the matching tool;
 * - a scheduled delivery answers `done: <instruction>`;
 * - the search subagent's question calls `search_conversations`;
 * - anything else is echoed with padding that streams for about three seconds.
 *
 * On a tool result it answers `<tool>: <result>`.
 */
const fakeModel = Layer.unwrap(
  Effect.sync(() => {
    const faux = FauxModel.make({ tokensPerSecond: 40 });
    const respond: FauxModel.FauxResponseFactory = (request) =>
      Effect.sync(() => {
        faux.appendResponses([respond]);
        // The last message the conversation sent: a section that changed (the clock) adds a system message after it.
        const last = request.messages.findLast((message) => message.role !== "system");
        if (last?.role === "toolResult") {
          return FauxModel.text(`${last.toolName}: ${textOf(last)}`);
        }
        const { text } = parseAttribution(textOf(last).trim());
        const scheduled = parseScheduledInput(text);
        if (scheduled !== undefined) return FauxModel.text(`done: ${scheduled}`);
        const question = /^Answer this question using search_conversations: (.+)$/mu.exec(text);
        if (question !== null) {
          return FauxModel.toolCall("search_conversations", { term: question[1] ?? "" });
        }
        const weather = /^weather (.+)$/u.exec(text);
        if (weather !== null)
          return FauxModel.toolCall("get_weather", { location: weather[1] ?? "" });
        if (text === "dice") return FauxModel.toolCall("roll_dice", {});
        const schedule = /^schedule (\d+) (.+)$/u.exec(text);
        if (schedule !== null) {
          return FauxModel.toolCall("schedule", {
            delaySeconds: Number(schedule[1]),
            instruction: schedule[2] ?? "",
          });
        }
        const search = /^search (.+)$/u.exec(text);
        if (search !== null) {
          return FauxModel.toolCall("search_history", { question: search[1] ?? "" });
        }
        return FauxModel.text(`echo: ${text}. ${"and so on ".repeat(30)}`.trim());
      });
    faux.setResponses([respond]);
    return faux.layer;
  }),
);

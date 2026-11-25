/**
 * The model services: Gemini through `@effect-durable-harness/opencode`, or with `SUPPORT_MODEL=faux` a fake
 * that the workerd tests drive with no key.
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
import { SupportModel } from "./agent/support.ts";

export const GEMINI = { provider: "google", modelId: "gemini-3.8-flash" } as const;
export const FAUX = { provider: "faux", modelId: "faux-1" } as const;

/**
 * `Models` and `SupportModel`, from the Worker's env: the fake with `SUPPORT_MODEL=faux` or without a key, Gemini
 * otherwise. Read when the Worker's init builds the object's Layer, so Alchemy binds both.
 */
export const ModelsLive: Layer.Layer<Models | SupportModel> = Layer.unwrap(
  Effect.gen(function* () {
    const model = yield* Config.option(Config.String("SUPPORT_MODEL"));
    const apiKey = yield* Config.option(Config.Redacted("GOOGLE_GENERATIVE_AI_API_KEY"));
    if (Option.getOrUndefined(model) === "faux" || Option.isNone(apiKey)) {
      return Layer.mergeAll(fakeModel, Layer.succeed(SupportModel, { ref: FAUX }));
    }
    return Layer.mergeAll(
      OpencodeModels.layer([gemini(Redacted.value(apiKey.value))]).pipe(
        Layer.provide(FetchHttpClient.layer),
      ),
      Layer.succeed(SupportModel, { ref: GEMINI }),
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
 * The fake model answers from the request alone, so it behaves the same after a restart. On the last user message
 * (a socket's `[name] ` prefix stripped):
 *
 * - `where is order <id>` calls `lookup_order`;
 * - `refund <id> <cents>` calls `refund_order`;
 * - `human` anywhere calls `escalate`;
 * - anything else gets `support: <text>`.
 *
 * On a tool result it answers `<tool>: <result>`.
 */
const fakeModel = Layer.unwrap(
  Effect.sync(() => {
    const faux = FauxModel.make({ tokensPerSecond: 200 });
    const respond: FauxModel.FauxResponseFactory = (request) =>
      Effect.sync(() => {
        faux.appendResponses([respond]);
        const last = request.messages.findLast((message) => message.role !== "system");
        if (last?.role === "toolResult") return FauxModel.text(`${last.toolName}: ${textOf(last)}`);
        const text = textOf(last)
          .replace(/^\[[^\]]*\] /u, "")
          .trim();
        const lookup = /^where is order (\S+?)\??$/u.exec(text);
        if (lookup !== null)
          return FauxModel.toolCall("lookup_order", { orderId: lookup[1] ?? "" });
        const refund = /^refund (\S+) (\d+)$/u.exec(text);
        if (refund !== null) {
          return FauxModel.toolCall("refund_order", {
            orderId: refund[1] ?? "",
            amountCents: Number(refund[2]),
          });
        }
        if (/\bhuman\b/u.test(text)) return FauxModel.toolCall("escalate", { reason: text });
        return FauxModel.text(`support: ${text}`);
      });
    faux.setResponses([respond]);
    return faux.layer;
  }),
);

const textOf = (message: Message | undefined): string => {
  if (message === undefined || message.role === "system") return "";
  if (Predicate.isString(message.content)) return message.content;
  return message.content.flatMap((part) => (part.type === "text" ? [part.text] : [])).join("\n");
};

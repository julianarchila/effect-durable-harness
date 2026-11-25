import { Google } from "@opencode/ai/providers";
import * as AgentTool from "@effect-durable-harness/core/AgentTool";
import * as Extension from "@effect-durable-harness/core/Extension";
import * as Harness from "@effect-durable-harness/core/Harness";
import * as OpencodeModels from "@effect-durable-harness/opencode/OpencodeModels";
import * as Cloudflare from "@alchemy.run/cloudflare";
import * as Config from "effect/Config";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Predicate from "effect/Predicate";
import * as Redacted from "effect/Redacted";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as FetchHttpClient from "effect/http/FetchHttpClient";
import * as Host from "../../../src/Host.ts";

/** The default model; metadata as `@effect-durable-harness/opencode`'s tests declare it. */
const gemini = (apiKey: string): OpencodeModels.OpencodeModel => ({
  provider: "google",
  id: "gemini-3.8-flash",
  api: "opencode-ai:gemini",
  model: Google.configure({ apiKey }).model("gemini-3.8-flash"),
  contextWindow: 1_048_576,
  maxTokens: 65_536,
  reasoning: true,
  input: ["text", "image"],
  thinkingLevelMap: { off: null, minimal: null, xhigh: null, max: null },
  cost: { input: 0.3, output: 2.5, cacheRead: 0.03, cacheWrite: 0 },
});

const Weather = AgentTool.make("get_weather", {
  description: "Current weather for a city: temperature in Celsius and sky.",
  parameters: Schema.Struct({ city: Schema.String }),
});

const LiveExtension = Extension.make({ name: "live", tools: [Weather] });

/** The extension's handlers, by name. */
const LiveExtensionLive = LiveExtension.toLayer({
  tools: {
    get_weather: () => Effect.succeed({ output: [{ type: "text", text: "22°C, sunny" }] }),
  },
});

/** The key is bound to the hosting Worker at deploy time (`LiveWorker`) and read from its env per instance. */
const models = Layer.unwrap(
  Effect.gen(function* () {
    const apiKey = yield* Config.Redacted("GOOGLE_GENERATIVE_AI_API_KEY");
    return OpencodeModels.layer([gemini(Redacted.value(apiKey))]);
  }).pipe(Effect.orDie),
).pipe(Layer.provide(FetchHttpClient.layer));

/** The agent: the kernel's Layer over the tool's implementation and Gemini. */
const Agent = Harness.layer({ extensions: [LiveExtension] }).pipe(
  Layer.provide(Layer.mergeAll(LiveExtensionLive, models)),
);

/** An effect-durable-harness Session whose root talks to Gemini through `@effect-durable-harness/opencode`, inside the object. */
export default class LiveAgent extends Cloudflare.DurableObject<LiveAgent>()(
  "LiveAgent",
  Effect.map(Host.agent(Agent), (agent) =>
    Effect.gen(function* () {
      const host = yield* agent.start;
      const harness = host.harness;
      const root = yield* harness
        .ensureRoot({
          agent: {
            model: { provider: "google", modelId: "gemini-3.8-flash" },
            instructions: "Before answering anything about weather, call get_weather.",
          },
        })
        .pipe(Effect.orDie);
      return host.methods({
        /** Submits one input and answers the transcript once the run settled. */
        turn: (text: string, requestId: string) =>
          Effect.gen(function* () {
            const submission = yield* root.submit({ type: "input", content: text, requestId });
            const settled = yield* submission.await;
            const entries = yield* Stream.runCollect(root.entries({ order: "ascending" }));
            const usage = yield* harness.usage;
            return {
              status: settled.status,
              cost: Object.values(usage.models).reduce((sum, model) => sum + model.cost.total, 0),
              messages: Array.from(entries).flatMap((entry) =>
                (entry.model ?? []).flatMap((message) => {
                  if (message.role === "system") return [];
                  const text = Predicate.isString(message.content)
                    ? message.content
                    : message.content
                        .map((part) => (part.type === "text" ? part.text : ""))
                        .join("");
                  const tools =
                    message.role === "assistant"
                      ? message.content.flatMap((part) =>
                          part.type === "toolCall" ? [part.name] : [],
                        )
                      : [];
                  return [{ role: message.role, text, tools }];
                }),
              ),
            };
          }).pipe(Effect.orDie),
      });
    }),
  ),
) {}

/** `get_weather` (replay-safe): mock weather. Pure, so it is implemented inline and needs no handler Layer. */
import * as AgentTool from "@effect-durable-harness/core/AgentTool";
import * as Effect from "effect/Effect";
import * as Random from "effect/Random";
import * as Schema from "effect/Schema";

const CONDITIONS = ["sunny", "partly cloudy", "overcast", "light rain"] as const;

const Weather = Schema.Struct({
  location: Schema.String,
  temperature: Schema.String,
  condition: Schema.String,
  note: Schema.String,
});

export const GetWeather = AgentTool.make("get_weather", {
  description: "Get the current weather for a location. Use when the user asks about the weather.",
  parameters: Schema.Struct({
    location: Schema.String.annotate({ description: "The city or location to check weather for" }),
  }),
  details: Weather,
  replay: "safe",
  execute: ({ location }) =>
    Effect.gen(function* () {
      const condition = CONDITIONS[yield* Random.nextIntBetween(0, CONDITIONS.length)] ?? "sunny";
      const celsius = yield* Random.nextIntBetween(12, 32);
      const weather = { location, temperature: `${celsius}°C`, condition, note: "Mock data." };
      return AgentTool.json(weather, { details: weather });
    }),
});

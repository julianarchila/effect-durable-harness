/**
 * `roll_dice` (not replay-safe, the default): a slow roll. A crash mid-roll leaves an `interrupted` result instead
 * of rolling again, the behaviour for tools whose effects must not repeat.
 */
import * as AgentTool from "@effect-durable-harness/core/AgentTool";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Random from "effect/Random";
import * as Schema from "effect/Schema";

export const RollDice = AgentTool.make("roll_dice", {
  description: "Roll dice slowly, for games. Takes a few seconds.",
  parameters: Schema.Struct({
    sides: Schema.optional(Schema.Int.annotate({ description: "Sides of the die, default 6" })),
  }),
  details: Schema.Struct({ rolled: Schema.Int }),
});

export const RollDiceLive = RollDice.toLayer(({ sides }, call) =>
  Effect.gen(function* () {
    yield* call.output("Rolling…\n");
    yield* Effect.sleep(Duration.seconds(3));
    const rolled = yield* Random.nextIntBetween(1, (sides ?? 6) + 1);
    return AgentTool.text(`Rolled ${rolled}`, { details: { rolled } });
  }),
);

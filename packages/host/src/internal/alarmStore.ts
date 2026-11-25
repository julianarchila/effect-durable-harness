/**
 * The instance's single alarm as the host's policy applies it, over the engine's (`Engine.alarm`). The host is its
 * only writer.
 */
import type { StorageFailure } from "@effect-durable-harness/core/AgentStorage";
import * as Clock from "effect/Clock";
import * as Effect from "effect/Effect";
import type * as Option from "effect/Option";
import type { EngineAlarm } from "../Engine.ts";
import type { AlarmAction } from "./alarm.ts";

export interface AlarmStore {
  readonly now: Effect.Effect<number>;
  readonly get: Effect.Effect<Option.Option<number>, StorageFailure>;
  readonly apply: (action: AlarmAction) => Effect.Effect<void, StorageFailure>;
}

export const make = (alarm: EngineAlarm): AlarmStore => ({
  now: Clock.currentTimeMillis,
  get: alarm.get,
  apply: (action) => {
    switch (action._tag) {
      case "Keep":
        return Effect.void;
      case "Set":
        return alarm.set(action.at);
      case "Delete":
        return alarm.delete;
    }
  },
});

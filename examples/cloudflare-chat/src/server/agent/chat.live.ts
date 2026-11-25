/**
 * The handlers of every tool and task `ChatHarness` declares without one. A missing handler stays in the agent's
 * requirements and fails to compile where `Session.ts` provides this Layer.
 */
import * as Layer from "effect/Layer";
import { DeliverScheduledLive } from "./tasks/deliver-scheduled.ts";
import { RollDiceLive } from "./tools/roll-dice.ts";
import { ScheduleLive } from "./tools/schedule.ts";
import { SearchHistoryLive } from "./tools/search-history.live.ts";

export const ChatLive = Layer.mergeAll(
  RollDiceLive,
  ScheduleLive,
  SearchHistoryLive,
  DeliverScheduledLive,
);

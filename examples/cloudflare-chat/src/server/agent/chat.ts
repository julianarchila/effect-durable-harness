/**
 * The chat agent's contract: its extensions and `ChatHarness`. Each tool and task is declared in a file of its
 * own under `tools/` and `tasks/`, next to its handler Layer; `chat.live.ts` merges the handlers.
 *
 * - `chat`, every top-level conversation's: `get_weather`, `roll_dice`, `schedule` (with the
 *   `chat.deliver-scheduled` task) and `search_history`, plus the preamble and the clock.
 * - `conversation-search`, the search subagent's: `search_conversations` only.
 */
import type * as Agent from "@effect-durable-harness/core/Agent";
import * as Extension from "@effect-durable-harness/core/Extension";
import * as Harness from "@effect-durable-harness/core/Harness";
import type { ModelRef } from "@effect-durable-harness/core/Models";
import * as Section from "@effect-durable-harness/core/Section";
import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import { DeliverScheduled } from "./tasks/deliver-scheduled.ts";
import { GetWeather } from "./tools/get-weather.ts";
import { RollDice } from "./tools/roll-dice.ts";
import { Schedule } from "./tools/schedule.ts";
import { SearchConversations } from "./tools/search-conversations.ts";
import { SearchHistory } from "./tools/search-history.ts";

/** The model every conversation of this deployment uses: Gemini, or the fake model in tests. */
export class ChatModel extends Context.Service<ChatModel, { readonly ref: ModelRef }>()(
  "@effect-durable-harness/example-cloudflare-chat/server/agent/chat/ChatModel",
) {}

const Preamble = Section.make(
  "preamble",
  () =>
    Effect.succeed(
      "You are a concise assistant. User messages may begin with a display name in brackets; treat it as speaker attribution.",
    ),
  { tag: false },
);

// Minute precision: finer values would append a system-prompt change on every request.
const Now = Section.make(
  "clock",
  () =>
    Effect.map(
      Clock.currentTimeMillis,
      (now) => `Current time: ${DateTime.formatIso(DateTime.makeUnsafe(now)).slice(0, 16)}Z.`,
    ),
  { tag: false },
);

export const Chat = Extension.make("chat", {
  tasks: [DeliverScheduled],
  tools: [GetWeather, RollDice, Schedule, SearchHistory],
  sections: [Preamble, Now],
});

/** The search subagent's own extension: its conversation is offered only `search_conversations`. */
export const ConversationSearch = Extension.make("conversation-search", {
  tools: [SearchConversations],
});

export const ChatHarness = Harness.make("chat", { extensions: [Chat, ConversationSearch] });

/** The agent a top-level conversation starts with; a fork inherits its parent's as of the fork entry. */
export const chatAgent = (model: ModelRef): Agent.AgentChange => ({
  model,
  thinkingLevel: "off",
  extensions: [Chat],
});

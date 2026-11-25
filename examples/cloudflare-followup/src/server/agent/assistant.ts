/**
 * The assistant's contract: its extension and `AssistantHarness`. Its documents are in `documents.ts`; its tasks
 * are declared in files of their own under `tasks/`, next to their handler Layers, and `assistant.live.ts` merges
 * the handlers.
 */
import type * as Agent from "@effect-durable-harness/core/Agent";
import * as Extension from "@effect-durable-harness/core/Extension";
import * as Harness from "@effect-durable-harness/core/Harness";
import type { ModelRef } from "@effect-durable-harness/core/Models";
import * as Section from "@effect-durable-harness/core/Section";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import { DeliverReply } from "./tasks/deliver-reply.ts";
import { FollowUp } from "./tasks/follow-up.ts";

/** The model every conversation of this deployment uses: Gemini, or the fake model in tests. */
export class AssistantModel extends Context.Service<AssistantModel, { readonly ref: ModelRef }>()(
  "@effect-durable-harness/example-cloudflare-followup/server/agent/assistant/AssistantModel",
) {}

const Preamble = Section.make(
  "preamble",
  () =>
    Effect.succeed(
      "You are a business's assistant answering a customer by text message. Answer in one to three short sentences, " +
        "plain text, no markdown. You cannot take actions; if asked for one, say a person will follow up.",
    ),
  { tag: false },
);

export const Assistant = Extension.make("followup", {
  tasks: [DeliverReply, FollowUp],
  sections: [Preamble],
});

export const AssistantHarness = Harness.make("followup", { extensions: [Assistant] });

/** The agent every contact's conversation starts with. */
export const assistantAgent = (model: ModelRef): Agent.AgentChange => ({
  model,
  thinkingLevel: "off",
  extensions: [Assistant],
});

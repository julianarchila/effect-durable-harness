/**
 * The support agent's contract: its document, tools, task, extension and `SupportHarness`. Declarations only; the
 * handlers, which request the shop's services, are in `support.live.ts`.
 *
 * - `lookup_order` (safe): reads one of the ticket customer's orders.
 * - `refund_order` (unsafe): refunds within the policy, through the payment gateway, under the call's key.
 * - `escalate` (safe): marks the ticket escalated and creates `NotifyStaff` in the same commit.
 * - `support.notify-staff`: a durable task that pages whoever is on call, retried across restarts.
 */
import type * as Agent from "@effect-durable-harness/core/Agent";
import * as AgentTool from "@effect-durable-harness/core/AgentTool";
import * as Doc from "@effect-durable-harness/core/Doc";
import * as Extension from "@effect-durable-harness/core/Extension";
import * as Harness from "@effect-durable-harness/core/Harness";
import type { ModelRef } from "@effect-durable-harness/core/Models";
import * as Section from "@effect-durable-harness/core/Section";
import * as Task from "@effect-durable-harness/core/Task";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

/** The model every ticket of this deployment uses: Gemini, or the fake model in tests. */
export class SupportModel extends Context.Service<SupportModel, { readonly ref: ModelRef }>()(
  "@effect-durable-harness/example-cloudflare-support/server/agent/support/SupportModel",
) {}

/** The ticket, committed with the transcript: who it is for and who has it. */
export const Ticket = Doc.make("support.ticket", {
  version: 1,
  scope: "conversation",
  history: "latest",
  fork: "initial",
  schema: Schema.Struct({
    ticketId: Schema.optionalKey(Schema.String),
    customerId: Schema.optionalKey(Schema.String),
    status: Schema.Literals(["open", "escalated", "human"]),
    assignee: Schema.optionalKey(Schema.String),
  }),
  initial: () => ({ status: "open" as const }),
  checkpointWhen: () => false,
});

export const LookupOrder = AgentTool.make("lookup_order", {
  description:
    "Look up one of the customer's orders by ID: item, status, total and refunds so far.",
  parameters: Schema.Struct({ orderId: Schema.String }),
  replay: "safe",
});

export const RefundOrder = AgentTool.make("refund_order", {
  description:
    "Refund part or all of one of the customer's orders, in cents. Only within the refund policy; otherwise escalate.",
  parameters: Schema.Struct({ orderId: Schema.String, amountCents: Schema.Number }),
  // Money moves: a call cut off midway is not rerun blindly; the gateway's idempotency key covers a retry.
  replay: "unsafe",
});

export const Escalate = AgentTool.make("escalate", {
  description:
    "Hand the ticket to a person when the customer asks for one or you cannot resolve it.",
  parameters: Schema.Struct({ reason: Schema.String }),
  replay: "safe",
});

export const NotifyStaff = Task.make("support.notify-staff", {
  version: 1,
  input: Schema.Struct({ ticketId: Schema.String, reason: Schema.String }),
  checkpoint: Schema.Struct({ phase: Schema.Literal("page") }),
  result: Schema.Struct({ paged: Schema.String }),
  initial: () => ({ phase: "page" as const }),
});

const Preamble = Section.make(
  "preamble",
  () =>
    Effect.succeed(
      "You are a shop's support agent. Be brief. Use lookup_order for order questions, refund_order for refunds " +
        "within policy, and escalate when the customer asks for a person or a refund is outside policy.",
    ),
  { tag: false },
);

export const Support = Extension.make("support", {
  tools: [LookupOrder, RefundOrder, Escalate],
  tasks: [NotifyStaff],
  sections: [Preamble],
});

export const SupportHarness = Harness.make("support", { extensions: [Support] });

/** The agent every ticket's conversation starts with. */
export const supportAgent = (model: ModelRef): Agent.AgentChange => ({
  model,
  thinkingLevel: "off",
  extensions: [Support],
});

/**
 * The assistant's conversation documents, committed with the transcript:
 *
 * - `followup.contact`: who the contact is and the newest inbound message, written in the commit that receives it.
 * - `followup.deliveries`: which task claimed each answer and what the webhook answered.
 */
import * as Doc from "@effect-durable-harness/core/Doc";
import * as Schema from "effect/Schema";

/** The contact and its newest inbound message: what a follow-up checks on waking. */
export const ContactState = Doc.make("followup.contact", {
  version: 1,
  scope: "conversation",
  history: "latest",
  fork: "initial",
  schema: Schema.Struct({
    address: Schema.optionalKey(Schema.String),
    latestInbound: Schema.optionalKey(
      Schema.Struct({ providerMessageId: Schema.String, at: Schema.Number }),
    ),
  }),
  initial: () => ({}),
  checkpointWhen: () => false,
});

/**
 * One entry per answer a task claimed, keyed by the answer's entry ID: the claiming task, then the webhook's
 * receipt once delivered. Several inputs steered into one run share one answer; the first task to claim it sends it.
 */
export const Deliveries = Doc.make("followup.deliveries", {
  version: 1,
  scope: "conversation",
  history: "latest",
  fork: "initial",
  schema: Schema.Struct({
    claims: Schema.Record(Schema.String, Schema.Number),
    delivered: Schema.Record(
      Schema.String,
      Schema.Struct({ deliveryId: Schema.String, key: Schema.String }),
    ),
  }),
  initial: () => ({ claims: {}, delivered: {} }),
  // Two keys per reply: a base every 31 changes keeps a cold load short.
  checkpointWhen: (_value, _ops, info) => info.deltasSinceBase >= 31,
});

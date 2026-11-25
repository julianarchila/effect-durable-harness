/**
 * What the inbound Worker, the sink and the tests agree on: an inbound message, a delivery to the webhook, and the
 * sink's ledger. Schemas only, so the tests import no runtime of the Workers.
 */
import * as Schema from "effect/Schema";

/** A contact's address: the name of its `Contact` object and the `to` of every delivery. */
export const ContactAddress = Schema.String.check(Schema.isPattern(/^[a-zA-Z0-9_+.-]{1,64}$/u));

/** An inbound message, as a provider's webhook would post it. */
export const Inbound = Schema.Struct({
  /** The provider's message ID: a resent webhook carries the same one, and it is the submission's `requestId`. */
  providerMessageId: Schema.String.check(Schema.isMinLength(1), Schema.isMaxLength(200)),
  from: ContactAddress,
  text: Schema.String.check(Schema.isMinLength(1), Schema.isMaxLength(4_000)),
});
export type Inbound = typeof Inbound.Type;

/** What `POST /inbound` answers: a new submission, or the one the same provider message ID already made. */
export const InboundResult = Schema.Struct({
  status: Schema.Literals(["accepted", "duplicate"]),
});
export type InboundResult = typeof InboundResult.Type;

/** Why a reply goes out: it answers inbound messages, or it is the follow-up after a silence. */
export const DeliveryKind = Schema.Literals(["reply", "follow-up"]);
export type DeliveryKind = typeof DeliveryKind.Type;

/** The body of `POST /deliveries` on the webhook; the idempotency key travels in the `Idempotency-Key` header. */
export const DeliveryBody = Schema.Struct({
  to: ContactAddress,
  kind: DeliveryKind,
  text: Schema.String,
});
export type DeliveryBody = typeof DeliveryBody.Type;

/** What the webhook answers a delivery it accepted: `duplicate` when it already had the key. */
export const DeliveryReceipt = Schema.Struct({
  deliveryId: Schema.String,
  duplicate: Schema.Boolean,
});
export type DeliveryReceipt = typeof DeliveryReceipt.Type;

/** The faults the sink injects for one recipient: refuse the first `failFirst` attempts, answer after `delayMs`. */
export const SinkFaults = Schema.Struct({
  failFirst: Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: 100 })),
  delayMs: Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: 60_000 })),
});
export type SinkFaults = typeof SinkFaults.Type;

/** One attempt the sink saw, in arrival order. */
export const SinkAttempt = Schema.Struct({
  key: Schema.String,
  outcome: Schema.Literals(["recorded", "duplicate", "refused"]),
  at: Schema.Number,
});

/** One delivery the sink recorded: the first attempt with its key. */
export const SinkDelivery = Schema.Struct({
  deliveryId: Schema.String,
  key: Schema.String,
  kind: DeliveryKind,
  text: Schema.String,
  at: Schema.Number,
});
export type SinkDelivery = typeof SinkDelivery.Type;

/** `GET /deliveries/:to` on the sink: what one recipient got. */
export const SinkLedger = Schema.Struct({
  attempts: Schema.Array(SinkAttempt),
  deliveries: Schema.Array(SinkDelivery),
});
export type SinkLedger = typeof SinkLedger.Type;

const FOLLOW_UP_PREFIX = "[follow-up] ";

/**
 * The input a follow-up submits after a silence. Self-contained, so the model needs no section to understand it;
 * the prefix lets the fake model recognize it.
 */
export const followUpInput = (silentForMs: number): string =>
  `${FOLLOW_UP_PREFIX}The contact has not written for ${Math.round(silentForMs / 60_000)} minutes. ` +
  "Write one short, friendly follow-up message to them, without repeating your previous answer.";

/** Whether a user message is a follow-up's input. */
export const isFollowUpInput = (text: string): boolean => text.startsWith(FOLLOW_UP_PREFIX);

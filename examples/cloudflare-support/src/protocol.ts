/** The data the API Worker, the payment gateway and their clients exchange. */
import * as Schema from "effect/Schema";

/** Opens a ticket with its first message; `ticketId` names the ticket's `SupportSession` object. */
export const OpenTicket = Schema.Struct({
  ticketId: Schema.String,
  customerId: Schema.String,
  message: Schema.String,
});
export type OpenTicket = typeof OpenTicket.Type;

/** A person takes the ticket over. */
export const HandOff = Schema.Struct({ email: Schema.String });

/** Issues a customer a token for the session socket (a sample: a real shop issues it at sign-in). */
export const IssueToken = Schema.Struct({ name: Schema.String });

/** A customer behind a socket token. */
export const Customer = Schema.Struct({ customerId: Schema.String, name: Schema.String });
export type Customer = typeof Customer.Type;

/** An order, as the shop's database holds it. */
export const Order = Schema.Struct({
  id: Schema.String,
  customerId: Schema.String,
  status: Schema.String,
  item: Schema.String,
  totalCents: Schema.Number,
  refundedCents: Schema.Number,
  chargeId: Schema.String,
  placedAt: Schema.Number,
});
export type Order = typeof Order.Type;

/** The refund policy, kept in KV so the shop changes it without a deploy. */
export const RefundPolicy = Schema.Struct({
  /** How long after the order a refund is allowed. */
  windowDays: Schema.Number,
  /** The largest refund the agent may make on its own; more goes to a person. */
  maxAgentCents: Schema.Number,
});
export type RefundPolicy = typeof RefundPolicy.Type;

/** What the payment gateway answers for a refund. */
export const RefundReceipt = Schema.Struct({ refundId: Schema.String, duplicate: Schema.Boolean });

/** The gateway's refunds of one charge. */
export const RefundLedger = Schema.Struct({
  refunds: Schema.Array(
    Schema.Struct({ refundId: Schema.String, amountCents: Schema.Number, key: Schema.String }),
  ),
});

/** What `SupportSession.status` answers. */
export const TicketStatus = Schema.Struct({
  ticket: Schema.NullOr(
    Schema.Struct({
      ticketId: Schema.optionalKey(Schema.String),
      customerId: Schema.optionalKey(Schema.String),
      status: Schema.String,
      assignee: Schema.optionalKey(Schema.String),
    }),
  ),
  transcript: Schema.Array(Schema.String),
  tasks: Schema.Array(Schema.Struct({ kind: Schema.String, state: Schema.String })),
  escalations: Schema.Array(Schema.Struct({ reason: Schema.String, paged: Schema.String })),
});
export type TicketStatus = typeof TicketStatus.Type;

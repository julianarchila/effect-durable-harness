/**
 * The handlers of every tool and task `SupportHarness` declares. Each requests the shop's services with `yield*`
 * when its Layer is built, once per isolate; their requirements bubble up to `SupportSessionLive`, which provides
 * them. A missing handler stays in the agent's requirements and fails to compile there.
 */
import * as AgentTool from "@effect-durable-harness/core/AgentTool";
import * as Clock from "effect/Clock";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import { Orders } from "../services/Orders.ts";
import { Payments } from "../services/Payments.ts";
import { Policies } from "../services/Policies.ts";
import { Staff } from "../services/Staff.ts";
import { Escalate, LookupOrder, NotifyStaff, RefundOrder, Ticket } from "./support.ts";

/** The ticket's ID and customer, read in a commit that writes nothing. */
const ticketOf = (call: AgentTool.Call) =>
  call.commit(
    Effect.map(Ticket.get(call.conversationId), (ticket) => ({
      ticketId: ticket.ticketId ?? "",
      customerId: ticket.customerId ?? "",
    })),
  );

const LookupOrderLive = LookupOrder.toLayer(
  Effect.gen(function* () {
    const orders = yield* Orders;
    return ({ orderId }, call) =>
      Effect.gen(function* () {
        const ticket = yield* ticketOf(call);
        const order = yield* orders.find(ticket.customerId, orderId);
        return AgentTool.text(
          `${order.id}: ${order.item}, ${order.status}, total ${order.totalCents} cents, refunded ${order.refundedCents}`,
        );
      }).pipe(
        Effect.catchTag("OrderNotFound", () =>
          Effect.succeed(AgentTool.text(`no order ${orderId}`)),
        ),
      );
  }),
);

const RefundOrderLive = RefundOrder.toLayer(
  Effect.gen(function* () {
    const orders = yield* Orders;
    const payments = yield* Payments;
    const policies = yield* Policies;
    return ({ orderId, amountCents }, call) =>
      Effect.gen(function* () {
        const ticket = yield* ticketOf(call);
        const order = yield* orders.find(ticket.customerId, orderId);
        const policy = yield* policies.refund;
        const now = yield* Clock.currentTimeMillis;
        const ageMs = now - order.placedAt;
        if (ageMs > Duration.toMillis(Duration.days(policy.windowDays))) {
          return AgentTool.text(`refused: ${orderId} is past the ${policy.windowDays}-day window`);
        }
        if (amountCents > policy.maxAgentCents) {
          return AgentTool.text(`refused: over the ${policy.maxAgentCents}-cent limit`);
        }
        if (amountCents <= 0 || amountCents > order.totalCents - order.refundedCents) {
          return AgentTool.text(
            `refused: ${orderId} has ${order.totalCents - order.refundedCents} cents left`,
          );
        }
        // Stable across runs of this call, and prefixed with the ticket: `call.key` is unique within one Session
        // only, and every ticket is a Session of its own sharing one gateway.
        const receipt = yield* payments.refund({
          chargeId: order.chargeId,
          amountCents,
          idempotencyKey: `${ticket.ticketId}:${call.key("refund")}`,
        });
        // A duplicate is a refund an earlier attempt of this call already made and recorded; `replay: "unsafe"` keeps
        // a call cut off between the two from running again, so the refund is never recorded twice.
        if (!receipt.duplicate) yield* orders.recordRefund(orderId, amountCents);
        return AgentTool.text(`refunded ${amountCents} cents (${receipt.refundId})`);
      }).pipe(
        Effect.catchTag("OrderNotFound", () =>
          Effect.succeed(AgentTool.text(`no order ${orderId}`)),
        ),
      );
  }),
);

const EscalateLive = Escalate.toLayer(({ reason }, call) =>
  // One commit: the ticket's status and the page, with the transcript.
  call.commit(
    Effect.gen(function* () {
      const ticket = yield* Ticket.get(call.conversationId);
      ticket.status = "escalated";
      yield* NotifyStaff.create(
        { ticketId: ticket.ticketId ?? "", reason },
        { ownership: { kind: "conversation" }, background: true },
      );
      return AgentTool.text("escalated: a person will take over shortly");
    }),
  ),
);

const NotifyStaffLive = NotifyStaff.toLayer(
  Effect.gen(function* () {
    const staff = yield* Staff;
    return {
      phases: {
        page: (task, run) =>
          staff
            .page({
              ticketId: task.input.ticketId,
              reason: task.input.reason,
              // `run.key` is unique within one Session only: every ticket's tasks share the shop's database.
              key: `${task.input.ticketId}:${run.key("page")}`,
            })
            .pipe(Effect.flatMap((paged) => run.complete({ paged }))),
      },
      abort: (_task, run) => run.aborted,
    };
  }),
);

export const SupportHandlers = Layer.mergeAll(
  LookupOrderLive,
  RefundOrderLive,
  EscalateLive,
  NotifyStaffLive,
);

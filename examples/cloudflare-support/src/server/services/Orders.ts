/**
 * The shop's orders, in its D1 database. The client is built once per isolate; each query runs in the call that
 * makes it, on the object's fibers, which carry the instance's `RuntimeContext` (`Host.withRuntimeContext`).
 */
import * as Host from "@effect-durable-harness/cloudflare/Host";
import * as Cloudflare from "@alchemy.run/cloudflare";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import { ShopDb } from "../../infra.ts";
import type { Order } from "../../protocol.ts";

/** No order with that ID, or not the ticket's customer's. */
export class OrderNotFound extends Schema.TaggedError<OrderNotFound>()("OrderNotFound", {
  orderId: Schema.String,
}) {}

interface OrderRow {
  readonly id: string;
  readonly customer_id: string;
  readonly status: string;
  readonly item: string;
  readonly total_cents: number;
  readonly refunded_cents: number;
  readonly charge_id: string;
  readonly placed_at: number;
}

export class Orders extends Context.Service<
  Orders,
  {
    /** The customer's order `orderId`. */
    readonly find: (customerId: string, orderId: string) => Effect.Effect<Order, OrderNotFound>;
    /** Records a refund the gateway made; adding the same refund twice is the caller's to avoid. */
    readonly recordRefund: (orderId: string, amountCents: number) => Effect.Effect<void>;
  }
>()("@effect-durable-harness/example-cloudflare-support/server/services/Orders") {}

export const OrdersLive = Layer.effect(
  Orders,
  Effect.gen(function* () {
    const db = yield* Cloudflare.D1.QueryDatabase(ShopDb);
    return Orders.of({
      find: (customerId, orderId) =>
        Host.withRuntimeContext(
          db
            .prepare("SELECT * FROM orders WHERE id = ? AND customer_id = ?")
            .bind(orderId, customerId)
            .first<OrderRow>(),
        ).pipe(
          Effect.flatMap((row) =>
            row === null
              ? Effect.fail(new OrderNotFound({ orderId }))
              : Effect.succeed(toOrder(row)),
          ),
          Effect.withSpan("Orders.find", { attributes: { "shop.order.id": orderId } }),
        ),
      recordRefund: (orderId, amountCents) =>
        Host.withRuntimeContext(
          db
            .prepare("UPDATE orders SET refunded_cents = refunded_cents + ? WHERE id = ?")
            .bind(amountCents, orderId)
            .run(),
        ).pipe(Effect.asVoid, Effect.withSpan("Orders.recordRefund")),
    });
  }),
).pipe(Layer.provide(Cloudflare.D1.QueryDatabaseBinding));

const toOrder = (row: OrderRow): Order => ({
  id: row.id,
  customerId: row.customer_id,
  status: row.status,
  item: row.item,
  totalCents: row.total_cents,
  refundedCents: row.refunded_cents,
  chargeId: row.charge_id,
  placedAt: row.placed_at,
});

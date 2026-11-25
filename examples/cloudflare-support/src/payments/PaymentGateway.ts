/**
 * The payment gateway, as an external system the shop calls: a second Worker that refunds charges, once per
 * idempotency key, in its own database. It knows nothing of effect-durable-harness.
 *
 * - `POST /refunds` with an `Idempotency-Key` header and `{ chargeId, amountCents }`: `201` with a receipt for a new
 *   key, `200` with the first receipt for a key it already has.
 * - `GET /refunds/:chargeId`: the charge's refunds, for tests and a look by hand.
 */
import * as Cloudflare from "@alchemy.run/cloudflare";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Random from "effect/Random";
import * as Schema from "effect/Schema";
import { HttpServerRequest } from "effect/http/HttpServerRequest";
import * as HttpServerResponse from "effect/http/HttpServerResponse";
import { PaymentsDb } from "../infra.ts";

const RefundRequest = Schema.Struct({ chargeId: Schema.String, amountCents: Schema.Number });

interface RefundRow {
  readonly idempotency_key: string;
  readonly refund_id: string;
  readonly charge_id: string;
  readonly amount_cents: number;
}

const refundsRoute = /^\/refunds\/([^/]+)$/u;

/** The Worker's tag: the API Worker imports only this to bind it, so its bundle leaves the gateway out. */
export class PaymentGateway extends Cloudflare.Worker<PaymentGateway, {}>()("PaymentGateway") {}

export default PaymentGateway.make(
  { main: import.meta.url },
  Effect.gen(function* () {
    const db = yield* Cloudflare.D1.QueryDatabase(PaymentsDb);
    return {
      fetch: Effect.gen(function* () {
        const request = yield* HttpServerRequest;
        const path = new URL(request.url, "http://gateway").pathname;
        if (path === "/refunds" && request.method === "POST") {
          const key = request.headers["idempotency-key"] ?? "";
          const body = yield* request.json.pipe(
            Effect.flatMap(Schema.decodeUnknownEffect(RefundRequest)),
            Effect.option,
          );
          if (key === "" || Option.isNone(body)) {
            return yield* HttpServerResponse.json(
              { error: "Expected an Idempotency-Key header and a refund body" },
              { status: 400 },
            );
          }
          const refundId = `re_${yield* Random.nextIntBetween(100_000, 999_999)}`;
          const inserted = yield* db
            .prepare(
              "INSERT INTO refunds (idempotency_key, refund_id, charge_id, amount_cents) VALUES (?, ?, ?, ?) ON CONFLICT (idempotency_key) DO NOTHING",
            )
            .bind(key, refundId, body.value.chargeId, body.value.amountCents)
            .run();
          const row = yield* db
            .prepare("SELECT * FROM refunds WHERE idempotency_key = ?")
            .bind(key)
            .first<RefundRow>();
          const duplicate = inserted.meta.changes === 0;
          return yield* HttpServerResponse.json(
            { refundId: row?.refund_id ?? refundId, duplicate },
            { status: duplicate ? 200 : 201 },
          );
        }
        const refunds = refundsRoute.exec(path);
        if (refunds !== null && request.method === "GET") {
          const rows = yield* db
            .prepare("SELECT * FROM refunds WHERE charge_id = ? ORDER BY rowid")
            .bind(decodeURIComponent(refunds[1] ?? ""))
            .all<RefundRow>();
          return yield* HttpServerResponse.json({
            refunds: rows.results.map((row) => ({
              refundId: row.refund_id,
              amountCents: row.amount_cents,
              key: row.idempotency_key,
            })),
          });
        }
        return yield* HttpServerResponse.json({ error: "Not found" }, { status: 404 });
      }),
    };
  }).pipe(Effect.provide(Cloudflare.D1.QueryDatabaseBinding)),
);

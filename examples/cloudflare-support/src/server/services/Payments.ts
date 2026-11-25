/**
 * The payment gateway, as the agent sees it: one refund per idempotency key. effect-durable-harness decides when
 * and how many times it runs; the deduplication is the gateway's ("External systems belong to the consumer").
 * Reached over a service binding to the stack's `PaymentGateway` Worker; a real shop calls its provider's API here.
 */
import * as Host from "@effect-durable-harness/cloudflare/Host";
import * as Cloudflare from "@alchemy.run/cloudflare";
import * as Context from "effect/Context";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as HttpClientRequest from "effect/http/HttpClientRequest";
import * as HttpClientResponse from "effect/http/HttpClientResponse";
import { PaymentGateway } from "../../payments/PaymentGateway.ts";
import { RefundReceipt } from "../../protocol.ts";

/** The gateway refused the refund or did not answer in time; retrying under the same key is safe. */
export class PaymentFailed extends Schema.TaggedError<PaymentFailed>()("PaymentFailed", {
  message: Schema.String,
}) {}

/** What `refund` takes. */
export interface RefundRequest {
  readonly chargeId: string;
  readonly amountCents: number;
  readonly idempotencyKey: string;
}

export class Payments extends Context.Service<
  Payments,
  {
    readonly refund: (
      request: RefundRequest,
    ) => Effect.Effect<typeof RefundReceipt.Type, PaymentFailed>;
  }
>()("@effect-durable-harness/example-cloudflare-support/server/services/Payments") {}

export const PaymentsLive = Layer.effect(
  Payments,
  Effect.gen(function* () {
    const post = yield* Cloudflare.Workers.Fetch(PaymentGateway);
    return Payments.of({
      refund: ({ chargeId, amountCents, idempotencyKey }) =>
        Host.withRuntimeContext(
          post(
            HttpClientRequest.post("https://gateway/refunds").pipe(
              HttpClientRequest.setHeader("idempotency-key", idempotencyKey),
              HttpClientRequest.bodyJsonUnsafe({ chargeId, amountCents }),
            ),
          ),
        ).pipe(
          Effect.flatMap(HttpClientResponse.filterStatusOk),
          Effect.flatMap(HttpClientResponse.schemaBodyJson(RefundReceipt)),
          Effect.timeout(Duration.seconds(30)),
          Effect.mapError((error) => new PaymentFailed({ message: error.message })),
          Effect.withSpan("Payments.refund"),
        ),
    });
  }),
).pipe(Layer.provide(Cloudflare.Workers.FetchBinding));

/**
 * The consumer's external system, as the agent sees it: one call that delivers a reply under an idempotency key.
 * effect-durable-harness decides when and how many times it runs; the deduplication is the webhook's, as
 * external systems belong to the consumer.
 *
 * `WebhookLive` posts to the stack's `Sink` Worker over a service binding. A real deployment posts to its provider's
 * URL instead; only this Layer changes.
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
import { type DeliveryBody, DeliveryReceipt } from "../protocol.ts";
import { Sink } from "../sink/Sink.ts";

/** How long one delivery may take before it counts as failed. */
const DELIVERY_TIMEOUT = Duration.seconds(30);

/** The webhook refused a delivery or did not answer it in time; resending under the same key is safe. */
export class DeliveryFailed extends Schema.TaggedError<DeliveryFailed>()("DeliveryFailed", {
  message: Schema.String,
}) {}

export class Webhook extends Context.Service<
  Webhook,
  {
    /** Delivers `body` once per `key`: a resend with a key the webhook has returns its first receipt. */
    readonly deliver: (
      key: string,
      body: DeliveryBody,
    ) => Effect.Effect<DeliveryReceipt, DeliveryFailed>;
  }
>()("@effect-durable-harness/example-cloudflare-followup/server/Webhook") {}

export const WebhookLive = Layer.effect(
  Webhook,
  Effect.gen(function* () {
    // At init, so Alchemy binds the Sink to the Worker that hosts the objects.
    const post = yield* Cloudflare.Workers.Fetch(Sink);
    return Webhook.of({
      deliver: (key, body) =>
        // Handlers run on the object's fibers, which carry the instance's RuntimeContext.
        Host.withRuntimeContext(
          post(
            HttpClientRequest.post("https://sink/deliveries").pipe(
              HttpClientRequest.setHeader("idempotency-key", key),
              HttpClientRequest.bodyJsonUnsafe(body),
            ),
          ),
        ).pipe(
          Effect.flatMap(HttpClientResponse.filterStatusOk),
          Effect.flatMap(HttpClientResponse.schemaBodyJson(DeliveryReceipt)),
          // A webhook that never answers would hold the `send` invocation: time it out as a refusal, which takes the
          // durable backoff and resends under the same key.
          Effect.timeout(DELIVERY_TIMEOUT),
          Effect.mapError((error) => new DeliveryFailed({ message: error.message })),
          Effect.withSpan("Webhook.deliver"),
        ),
    });
  }),
).pipe(Layer.provide(Cloudflare.Workers.FetchBinding));

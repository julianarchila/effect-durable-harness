/**
 * The external webhook, as a consumer's system would run it: a second Worker in the stack that records deliveries,
 * deduplicates them by idempotency key, and can fail on purpose. It knows nothing of effect-durable-harness.
 *
 * - `POST /deliveries` with an `Idempotency-Key` header and a `DeliveryBody`: `201` with a receipt for a new key,
 *   `200` with the first receipt for a key it already has, `503` for an attempt it refuses on purpose.
 * - `GET /deliveries/:to`: the recipient's attempts and deliveries, for tests and for a look by hand.
 * - `PUT /faults/:to` with `SinkFaults`: refuse the recipient's next attempts, or answer them late.
 *
 * Each recipient's ledger is a `Ledger` Durable Object, so attempts are counted and deduplicated in one place.
 */
import * as Cloudflare from "@alchemy.run/cloudflare";
import * as Clock from "effect/Clock";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import { HttpServerRequest } from "effect/http/HttpServerRequest";
import * as HttpServerResponse from "effect/http/HttpServerResponse";
import {
  ContactAddress,
  DeliveryBody,
  SinkAttempt,
  SinkDelivery,
  SinkFaults,
  type SinkLedger,
} from "../protocol.ts";

const LedgerState = Schema.Struct({
  faults: SinkFaults,
  attempts: Schema.Array(SinkAttempt),
  deliveries: Schema.Array(SinkDelivery),
});
type LedgerState = typeof LedgerState.Type;

const EMPTY: LedgerState = { faults: { failFirst: 0, delayMs: 0 }, attempts: [], deliveries: [] };

/** What the ledger did with one attempt; `delayMs` is how long the Worker waits before it answers. */
export type AttemptResult =
  | { readonly outcome: "refused" }
  | {
      readonly outcome: "recorded" | "duplicate";
      readonly delivery: SinkDelivery;
      readonly delayMs: number;
    };

const STATE_KEY = "ledger";

/** One recipient's ledger. */
export class Ledger extends Cloudflare.DurableObject<Ledger>()(
  "Ledger",
  // Alchemy's two-phase constructor: nothing to capture per isolate, so the outer Effect only returns the inner.
  Effect.succeed(
    Effect.gen(function* () {
      const state = yield* Cloudflare.DurableObjectState;
      const load = state.storage.get(STATE_KEY).pipe(
        Effect.flatMap((stored) =>
          stored === undefined
            ? Effect.succeed(EMPTY)
            : Schema.decodeUnknownEffect(LedgerState)(stored),
        ),
        Effect.orDie,
      );
      const save = (next: LedgerState) => state.storage.put(STATE_KEY, next);
      return {
        configure: (faults: SinkFaults) =>
          Effect.gen(function* () {
            const current = yield* load;
            // Faults count from now: earlier attempts do not use them up.
            yield* save({
              ...current,
              faults: { ...faults, failFirst: faults.failFirst + current.attempts.length },
            });
          }),
        attempt: (key: string, body: DeliveryBody) =>
          Effect.gen(function* () {
            const current = yield* load;
            const at = yield* Clock.currentTimeMillis;
            const existing = current.deliveries.find((delivery) => delivery.key === key);
            const refused =
              existing === undefined && current.attempts.length < current.faults.failFirst;
            const delivery: SinkDelivery = existing ?? {
              deliveryId: `d${current.deliveries.length + 1}`,
              key,
              kind: body.kind,
              text: body.text,
              at,
            };
            const outcome = refused ? "refused" : existing === undefined ? "recorded" : "duplicate";
            yield* save({
              ...current,
              attempts: [...current.attempts, { key, outcome, at }],
              deliveries:
                outcome === "recorded" ? [...current.deliveries, delivery] : current.deliveries,
            });
            return outcome === "refused"
              ? ({ outcome } satisfies AttemptResult)
              : ({ outcome, delivery, delayMs: current.faults.delayMs } satisfies AttemptResult);
          }),
        read: () =>
          Effect.map(
            load,
            (current): SinkLedger => ({
              attempts: current.attempts,
              deliveries: current.deliveries,
            }),
          ),
      };
    }),
  ),
) {}

const deliveriesRoute = /^\/deliveries\/([^/]+)$/u;
const faultsRoute = /^\/faults\/([^/]+)$/u;

const decodeAddress = Schema.decodeUnknownOption(ContactAddress);

/**
 * The Worker's tag: `src/server/Webhook.ts` imports only this to bind it, so the inbound Worker's bundle leaves out
 * the implementation below and its `Ledger` (Alchemy's tagged Worker, tree-shaken `.make()`).
 */
export class Sink extends Cloudflare.Worker<Sink, {}>()("Sink") {}

// The default export is the Worker's entry point.
export default Sink.make(
  { main: import.meta.url },
  Effect.gen(function* () {
    const ledgers = yield* Ledger;
    return {
      fetch: Effect.gen(function* () {
        const request = yield* HttpServerRequest;
        const path = new URL(request.url, "http://sink").pathname;
        if (path === "/deliveries" && request.method === "POST") {
          const key = request.headers["idempotency-key"] ?? "";
          const body = yield* request.json.pipe(
            Effect.flatMap(Schema.decodeUnknownEffect(DeliveryBody)),
            Effect.option,
          );
          if (key === "" || Option.isNone(body)) {
            return yield* HttpServerResponse.json(
              { error: "Expected an Idempotency-Key header and a delivery body" },
              { status: 400 },
            );
          }
          const result = yield* ledgers.getByName(body.value.to).attempt(key, body.value);
          if (result.outcome === "refused") {
            return yield* HttpServerResponse.json({ error: "Refused on purpose" }, { status: 503 });
          }
          // The delivery is already recorded: a caller cut off now must resend with the same key.
          if (result.delayMs > 0) yield* Effect.sleep(result.delayMs);
          return yield* HttpServerResponse.json(
            { deliveryId: result.delivery.deliveryId, duplicate: result.outcome === "duplicate" },
            { status: result.outcome === "recorded" ? 201 : 200 },
          );
        }
        const deliveries = deliveriesRoute.exec(path);
        const to = decodeAddress(decodeURIComponent(deliveries?.[1] ?? ""));
        if (deliveries !== null && Option.isSome(to) && request.method === "GET") {
          return yield* HttpServerResponse.json(yield* ledgers.getByName(to.value).read());
        }
        const faults = faultsRoute.exec(path);
        const target = decodeAddress(decodeURIComponent(faults?.[1] ?? ""));
        if (faults !== null && Option.isSome(target) && request.method === "PUT") {
          const body = yield* request.json.pipe(
            Effect.flatMap(Schema.decodeUnknownEffect(SinkFaults)),
            Effect.option,
          );
          if (Option.isNone(body)) {
            return yield* HttpServerResponse.json(
              { error: "Expected SinkFaults" },
              { status: 400 },
            );
          }
          yield* ledgers.getByName(target.value).configure(body.value);
          return HttpServerResponse.empty({ status: 204 });
        }
        return yield* HttpServerResponse.json({ error: "Not found" }, { status: 404 });
      }),
    };
  }),
);

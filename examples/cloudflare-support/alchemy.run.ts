/**
 * The support example: an agent that uses the shop's own infrastructure through services.
 *
 * - `Api` (`src/server/`): the API Worker, which hosts the `SupportSession` Durable Object, one per ticket, and binds
 *   the shop's D1 database (`ShopDb`), two KV namespaces (`Settings`, `CustomerTokens`) and the gateway.
 * - `PaymentGateway` (`src/payments/`): the external payment API, a second Worker with its own D1 database.
 *
 * Run it under Doppler, which provides the Gemini key: `doppler run -- bun run dev`.
 */
import * as Alchemy from "alchemy";
import * as Cloudflare from "@alchemy.run/cloudflare";
import * as Effect from "effect/Effect";
import PaymentGatewayLive, { PaymentGateway } from "./src/payments/PaymentGateway.ts";
import Api from "./src/server/Api.ts";

export default Alchemy.Stack(
  "DurableSupport",
  {
    providers: Cloudflare.providers(),
    state: Alchemy.localState(),
  },
  Effect.gen(function* () {
    const api = yield* Api;
    const gateway = yield* PaymentGateway;
    return { api: api.url.as<string>(), gateway: gateway.url.as<string>() };
  }).pipe(Effect.provide(PaymentGatewayLive)),
);

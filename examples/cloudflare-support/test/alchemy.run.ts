/** Both Workers with the fake model (`SUPPORT_MODEL=faux`, set by the test before it deploys). */
import * as Alchemy from "alchemy";
import * as Cloudflare from "@alchemy.run/cloudflare";
import * as Effect from "effect/Effect";
import PaymentGatewayLive, { PaymentGateway } from "../src/payments/PaymentGateway.ts";
import Api from "../src/server/Api.ts";

export default Alchemy.Stack(
  "DurableSupportTest",
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

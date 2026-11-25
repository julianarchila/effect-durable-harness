/**
 * The follow-up example: deferred work and external delivery.
 *
 * - `Api` (`src/server/`): the inbound Worker, which hosts the `Contact` Durable Object, one per contact.
 * - `Sink` (`src/sink/`): the external webhook, a second Worker with its `Ledger` objects. `Api` reaches it over a
 *   service binding (`src/server/Webhook.ts`).
 *
 * Run it under Doppler, which provides the Gemini key: `doppler run -- bun run dev`.
 */
import * as Alchemy from "alchemy";
import * as Cloudflare from "@alchemy.run/cloudflare";
import * as Effect from "effect/Effect";
import Api from "./src/server/Api.ts";
import SinkLive, { Sink } from "./src/sink/Sink.ts";

export default Alchemy.Stack(
  "DurableFollowup",
  {
    providers: Cloudflare.providers(),
    state: Alchemy.localState(),
  },
  Effect.gen(function* () {
    const api = yield* Api;
    const sink = yield* Sink;
    return { api: api.url.as<string>(), sink: sink.url.as<string>() };
  }).pipe(Effect.provide(SinkLive)),
);

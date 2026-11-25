/**
 * The chat example on Rivet: `examples/cloudflare-chat`'s agent and app, with each session a
 * `Rivet.DurableObject` on Rivet Cloud and a node on Fly (`src/cluster.ts`).
 *
 * - `SessionLive` (`src/server/Session.ts`): the sessions, deployed to the pool.
 * - `Api` (`src/server/Api.ts`): a Cloudflare Worker that creates session IDs and forwards the session sockets to
 *   the objects through Rivet's gateway.
 * - `Web`: the React app on Cloudflare, whose front door forwards `/api/*` to `Api`.
 *
 * Deploy under Doppler for the Gemini key and Fly's token, with the `RivetCloud` browser sign-in in the Alchemy
 * profile: `doppler run -- bun run deploy`.
 */
import { compatibility } from "@effect-durable-harness/cloudflare";
import * as Cloudflare from "@alchemy.run/cloudflare";
import * as Fly from "@alchemy.run/fly";
import * as Rivet from "@alchemy.run/rivet";
import * as Alchemy from "alchemy";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import Api from "./src/server/Api.ts";
import { SessionLive } from "./src/server/Session.ts";

export default Alchemy.Stack(
  "DurableChatRivet",
  {
    // Each cloud's providers are independent; the language service sees the HTTP client they each provide.
    // @effect-diagnostics-next-line layerMergeAllWithDependencies:off
    providers: Layer.mergeAll(Cloudflare.providers(), Fly.providers(), Rivet.providers()),
    state: Alchemy.localState(),
  },
  Effect.gen(function* () {
    const api = yield* Api;
    const web = yield* Cloudflare.Website.Vite("Web", {
      main: "src/web/worker.ts",
      compatibility: compatibility(),
      assets: { notFoundHandling: "single-page-application", runWorkerFirst: ["/api/*"] },
      env: { API: Api },
    });
    return { url: web.url, api: api.url.as<string>() };
  }).pipe(Effect.provide(SessionLive)),
);

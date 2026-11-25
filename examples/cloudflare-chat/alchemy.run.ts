/**
 * The chat example: a multiplayer chat on effect-durable-harness.
 *
 * - `Api` (`src/server/`): the Worker that hosts the `Session` Durable Object, one per session.
 * - `Web` (`src/web/`): the React app, built by Vite. Its front-door Worker serves the app and forwards `/api/*`,
 *   the session sockets included, to `Api` over a service binding, so everything is one origin.
 *
 * Run it under Doppler, which provides the Gemini key: `doppler run -- bun run dev`.
 */
import { compatibility } from "@effect-durable-harness/cloudflare";
import * as Alchemy from "alchemy";
import * as Cloudflare from "@alchemy.run/cloudflare";
import * as Effect from "effect/Effect";
import Api from "./src/server/Api.ts";

export default Alchemy.Stack(
  "DurableChat",
  {
    providers: Cloudflare.providers(),
    state: Alchemy.localState(),
  },
  Effect.gen(function* () {
    const api = yield* Api;
    const web = yield* Cloudflare.Website.Vite("Web", {
      main: "src/web/worker.ts",
      compatibility: compatibility(),
      assets: { notFoundHandling: "single-page-application", runWorkerFirst: ["/api/*"] },
      // `VITE_API_URL` reaches the client only for `alchemy dev` (`src/web/SessionPage.tsx`).
      env: { API: Api, VITE_API_URL: api.url.as<string>() },
    });
    return { url: web.url, api: api.url.as<string>() };
  }),
);

/**
 * The API Worker: creates session IDs and routes each session's socket to its object. The web app's front door
 * forwards `/api/*` here over a service binding (`alchemy.run.ts`).
 */
import { compatibility } from "@effect-durable-harness/cloudflare";
import * as Cloudflare from "@alchemy.run/cloudflare";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Random from "effect/Random";
import { HttpServerRequest } from "effect/http/HttpServerRequest";
import * as HttpServerResponse from "effect/http/HttpServerResponse";
import { Session, SessionLive } from "./Session.ts";

const sessionRoute = /^\/api\/sessions\/([a-zA-Z0-9_-]+)\/?$/;
const crashRoute = /^\/api\/sessions\/([a-zA-Z0-9_-]+)\/crash$/;

export default class Api extends Cloudflare.Worker<Api>()(
  "Api",
  { main: import.meta.url, compatibility: compatibility() },
  Effect.gen(function* () {
    const sessions = yield* Session;
    return {
      fetch: Effect.gen(function* () {
        const request = yield* HttpServerRequest;
        const path = request.url.split("?")[0] ?? "";
        if (path === "/api/sessions" || path === "/api/sessions/") {
          if (request.method !== "POST") return HttpServerResponse.empty({ status: 405 });
          const sessionId = `s${(yield* Random.nextIntBetween(0, 2 ** 31 - 1)).toString(36)}${(yield* Random.nextIntBetween(0, 2 ** 31 - 1)).toString(36)}`;
          return yield* HttpServerResponse.json(
            { sessionId, url: `/sessions/${sessionId}` },
            { status: 201 },
          );
        }
        const crash = crashRoute.exec(path);
        if (crash !== null && request.method === "POST") {
          yield* sessions
            .getByName(crash[1] ?? "")
            .crash()
            .pipe(Effect.catchCause(() => Effect.void));
          return HttpServerResponse.empty({ status: 204 });
        }
        const session = sessionRoute.exec(path);
        if (session !== null) {
          if (request.headers["upgrade"]?.toLowerCase() !== "websocket") {
            return yield* HttpServerResponse.json(
              { error: "WebSocket upgrade required" },
              { status: 426 },
            );
          }
          // No authentication: this is a sample.
          return yield* sessions.getByName(session[1] ?? "").fetch(request);
        }
        return yield* HttpServerResponse.json({ error: "Not found" }, { status: 404 });
      }),
    };
  }).pipe(
    // Effect spans in Workers Observability: the Worker's and the `Session` objects', the agent's phases
    // included. Cloudflare samples and keeps them; `headSamplingRate` lowers the share of invocations traced.
    Effect.provide(Layer.mergeAll(SessionLive, Cloudflare.Telemetry())),
  ),
) {}

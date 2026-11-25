/**
 * The API Worker, on Cloudflare: creates session IDs and forwards each session's socket to its object on Rivet,
 * through Rivet's gateway. The same Worker as `examples/cloudflare-chat`'s, with a `Rivet.DurableObject` behind it:
 * the stub forwards a WebSocket upgrade as Cloudflare's does.
 */
import { compatibility } from "@effect-durable-harness/cloudflare";
import * as Cloudflare from "@alchemy.run/cloudflare";
import * as Effect from "effect/Effect";
import * as Random from "effect/Random";
import { HttpServerRequest } from "effect/http/HttpServerRequest";
import * as HttpServerResponse from "effect/http/HttpServerResponse";
import { Session } from "./Session.ts";

const sessionRoute = /^\/api\/sessions\/([a-zA-Z0-9_-]+)\/?$/;

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
      }).pipe(
        Effect.catchCause((cause) =>
          Effect.logError("api: request failed", cause).pipe(
            Effect.as(HttpServerResponse.empty({ status: 502 })),
          ),
        ),
      ),
    };
  }),
) {}

/**
 * The API Worker: it hosts the `SupportSession` objects, one per ticket, and routes to them over Alchemy's typed
 * binding. The tagged errors in `SupportSession`'s `errors` arrive here as the same classes.
 *
 * - `POST /customers/:id/tokens` with `{ name }`: a socket token for the customer (KV).
 * - `PUT /policies/refund` with a `RefundPolicy`: the shop's refund policy (KV).
 * - `POST /tickets` with an `OpenTicket`: opens the ticket with its first message.
 * - `GET /tickets/:id`: the ticket, its transcript, tasks and escalations.
 * - `GET /tickets/:id/socket?token=`: the ticket's session socket (`SessionProtocol`).
 * - `POST /tickets/:id/handoff` with `{ email }`: a person takes the ticket; `404` for unknown staff.
 * - `POST /tickets/:id/crash`: resets the object, as an eviction does (`SUPPORT_ALLOW_CRASH=true` only).
 */
import { compatibility } from "@effect-durable-harness/cloudflare";
import * as Cloudflare from "@alchemy.run/cloudflare";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import { HttpServerRequest } from "effect/http/HttpServerRequest";
import * as HttpServerResponse from "effect/http/HttpServerResponse";
import { HandOff, IssueToken, OpenTicket, RefundPolicy } from "../protocol.ts";
import { CustomerAuth, CustomerAuthLive } from "./services/CustomerAuth.ts";
import { Policies, PoliciesLive } from "./services/Policies.ts";
import { SupportSession, SupportSessionLive } from "./SupportSession.ts";

const tokenRoute = /^\/customers\/([^/]+)\/tokens$/u;
const ticketRoute = /^\/tickets\/([^/]+)(?:\/(socket|handoff|crash))?$/u;

export default class Api extends Cloudflare.Worker<Api>()(
  "Api",
  { main: import.meta.url, compatibility: compatibility() },
  Effect.gen(function* () {
    const tickets = yield* SupportSession;
    const auth = yield* CustomerAuth;
    const policies = yield* Policies;

    return {
      fetch: Effect.gen(function* () {
        const request = yield* HttpServerRequest;
        const path = new URL(request.url, "http://api").pathname;

        const token = tokenRoute.exec(path);
        if (token !== null && request.method === "POST") {
          const input = yield* body(IssueToken, request);
          if (Option.isNone(input)) return yield* badRequest("Expected { name }");
          const issued = yield* auth.issue({
            customerId: decodeURIComponent(token[1] ?? ""),
            name: input.value.name,
          });
          return yield* HttpServerResponse.json({ token: issued }, { status: 201 });
        }

        if (path === "/policies/refund" && request.method === "PUT") {
          const policy = yield* body(RefundPolicy, request);
          if (Option.isNone(policy)) return yield* badRequest("Expected a RefundPolicy");
          yield* policies.setRefund(policy.value);
          return HttpServerResponse.empty({ status: 204 });
        }

        if (path === "/tickets" && request.method === "POST") {
          const input = yield* body(OpenTicket, request);
          if (Option.isNone(input)) return yield* badRequest("Expected an OpenTicket");
          const opened = yield* tickets.getByName(input.value.ticketId).open(input.value);
          return yield* HttpServerResponse.json(opened, { status: 202 });
        }

        const ticket = ticketRoute.exec(path);
        const ticketId = decodeURIComponent(ticket?.[1] ?? "");
        if (ticket === null || ticketId === "") {
          return yield* HttpServerResponse.json({ error: "Not found" }, { status: 404 });
        }
        const session = tickets.getByName(ticketId);
        const action = ticket[2];
        if ((action === "handoff" || action === "crash") && request.method !== "POST") {
          return HttpServerResponse.empty({ status: 405 });
        }
        switch (action) {
          case undefined:
            return yield* HttpServerResponse.json(yield* session.status());
          case "socket":
            return yield* session.fetch(request);
          case "handoff": {
            const input = yield* body(HandOff, request);
            if (Option.isNone(input)) return yield* badRequest("Expected { email }");
            return yield* session.handOff(input.value.email).pipe(
              Effect.flatMap((result) => HttpServerResponse.json(result)),
              // Revived on this side as the same class: `errors` on `SupportSession`.
              Effect.catchTag("StaffNotFound", (error) =>
                HttpServerResponse.json({ error: `No staff ${error.email}` }, { status: 404 }),
              ),
            );
          }
          case "crash":
            yield* session.crash().pipe(Effect.catchCause(() => Effect.void));
            return HttpServerResponse.empty({ status: 204 });
          default:
            return yield* HttpServerResponse.json({ error: "Not found" }, { status: 404 });
        }
      }).pipe(
        Effect.catchCause((cause) =>
          Effect.logError("api: request failed", cause).pipe(
            Effect.andThen(HttpServerResponse.json({ error: "Internal error" }, { status: 500 })),
          ),
        ),
      ),
    };
  }).pipe(
    Effect.provide(
      Layer.mergeAll(SupportSessionLive, CustomerAuthLive, PoliciesLive, Cloudflare.Telemetry()),
    ),
  ),
) {}

/** The request's JSON body decoded with `schema`, or none. */
const body = <S extends Schema.Top>(schema: S, request: HttpServerRequest) =>
  request.json.pipe(Effect.flatMap(Schema.decodeUnknownEffect(schema)), Effect.option);

const badRequest = (error: string) => HttpServerResponse.json({ error }, { status: 400 });

/**
 * The inbound Worker: the endpoint a messaging provider's webhook posts to. It routes each message to its
 * contact's `Contact` object, which submits it under the provider message ID, so a resent webhook is the same
 * submission.
 *
 * - `POST /inbound` with an `Inbound` body: `202` with `{ status: "accepted" | "duplicate" }`.
 * - `GET /contacts/:address`: the object's transcript, tasks, documents and alarm (tests, a look by hand).
 * - `POST /contacts/:address/crash`: resets the object, as an eviction does (`FOLLOWUP_ALLOW_CRASH=true` only).
 */
import { compatibility } from "@effect-durable-harness/cloudflare";
import * as Cloudflare from "@alchemy.run/cloudflare";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import { HttpServerRequest } from "effect/http/HttpServerRequest";
import * as HttpServerResponse from "effect/http/HttpServerResponse";
import { ContactAddress, Inbound } from "../protocol.ts";
import { Contact, ContactLive } from "./Contact.ts";

const contactRoute = /^\/contacts\/([^/]+)$/u;
const crashRoute = /^\/contacts\/([^/]+)\/crash$/u;

const decodeAddress = Schema.decodeUnknownOption(ContactAddress);

export default class Api extends Cloudflare.Worker<Api>()(
  "Api",
  { main: import.meta.url, compatibility: compatibility() },
  Effect.gen(function* () {
    const contacts = yield* Contact;
    return {
      fetch: Effect.gen(function* () {
        const request = yield* HttpServerRequest;
        const path = new URL(request.url, "http://api").pathname;
        if (path === "/inbound") {
          if (request.method !== "POST") return HttpServerResponse.empty({ status: 405 });
          const inbound = yield* request.json.pipe(
            Effect.flatMap(Schema.decodeUnknownEffect(Inbound)),
            Effect.option,
          );
          if (Option.isNone(inbound)) {
            return yield* HttpServerResponse.json(
              { error: "Expected an Inbound body" },
              { status: 400 },
            );
          }
          // No authentication: a sample. A provider's webhook would be verified by its signature here.
          const result = yield* contacts.getByName(inbound.value.from).receive(inbound.value);
          return yield* HttpServerResponse.json(result, { status: 202 });
        }
        const crash = crashRoute.exec(path);
        const crashed = decodeAddress(decodeURIComponent(crash?.[1] ?? ""));
        if (crash !== null && Option.isSome(crashed) && request.method === "POST") {
          yield* contacts
            .getByName(crashed.value)
            .crash()
            .pipe(Effect.catchCause(() => Effect.void));
          return HttpServerResponse.empty({ status: 204 });
        }
        const contact = contactRoute.exec(path);
        const address = decodeAddress(decodeURIComponent(contact?.[1] ?? ""));
        if (contact !== null && Option.isSome(address) && request.method === "GET") {
          return yield* HttpServerResponse.json(yield* contacts.getByName(address.value).inspect());
        }
        return yield* HttpServerResponse.json({ error: "Not found" }, { status: 404 });
      }).pipe(
        Effect.catchCause((cause) =>
          Effect.logError("api: request failed", cause).pipe(
            Effect.andThen(HttpServerResponse.json({ error: "Internal error" }, { status: 500 })),
          ),
        ),
      ),
    };
  }).pipe(
    // Effect spans in Workers Observability: the Worker's and the `Contact` objects', the agent's phases
    // included.
    Effect.provide(Layer.mergeAll(ContactLive, Cloudflare.Telemetry())),
  ),
) {}

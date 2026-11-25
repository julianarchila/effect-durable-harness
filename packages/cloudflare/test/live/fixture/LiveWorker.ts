import * as Cloudflare from "@alchemy.run/cloudflare";
import * as Config from "effect/Config";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { HttpServerRequest } from "effect/http/HttpServerRequest";
import * as HttpServerResponse from "effect/http/HttpServerResponse";
import { compatibility } from "../../../src/Compatibility.ts";
import LiveAgent from "./LiveAgent.ts";

const turnRoute = /^\/live\/([a-zA-Z0-9_-]+)\/turn$/;
const Turn = Schema.Struct({ text: Schema.String, requestId: Schema.String });

/** `POST /live/:name/turn` with `{ text, requestId }` runs one turn in the object `:name`. */
export default class LiveWorker extends Cloudflare.Worker<LiveWorker>()(
  "LiveWorker",
  { main: import.meta.url, compatibility: compatibility() },
  Effect.gen(function* () {
    // Read during Init, so Alchemy binds it to this Worker as a secret; the object reads it from the same env.
    yield* Config.Redacted("GOOGLE_GENERATIVE_AI_API_KEY");
    const agents = yield* LiveAgent;
    return {
      fetch: Effect.gen(function* () {
        const request = yield* HttpServerRequest;
        const match = turnRoute.exec(request.url.split("?")[0] ?? "");
        if (match === null) return HttpServerResponse.text("ok");
        const body = yield* request.json.pipe(
          Effect.flatMap(Schema.decodeUnknownEffect(Turn)),
          Effect.orDie,
        );
        const result = yield* agents.getByName(match[1] ?? "").turn(body.text, body.requestId);
        return yield* HttpServerResponse.json(result);
      }),
    };
  }).pipe(Effect.orDie),
) {}

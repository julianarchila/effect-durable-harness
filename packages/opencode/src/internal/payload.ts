// `onPayload` (the generation's `beforeProviderRequest` hooks) on `@opencode/ai`: opencode builds the provider body
// inside its route and has no hook that hands it out to be replaced. Its per-call HTTP middleware
// (`LLM.stream(request, { http })`) sees the request it is about to send, body included, so the body is replaced
// there: decoded, passed to `onPayload`, and set again with the same content type.
//
// The hook runs on the HTTP request opencode sends rather than on a provider's body object, so it covers every HTTP
// JSON transport, and it runs again for each send. A body that is not JSON text (a compressed one, or a WebSocket transport's
// frames, which never pass this middleware) is sent unchanged.
import type { HttpMiddleware } from "@opencode/ai/route";
import * as Effect from "effect/Effect";
import * as HttpClientRequest from "effect/http/HttpClientRequest";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";

const JsonText = Schema.fromJsonString(Schema.Json);
const decoder = new TextDecoder();

/** The middleware that passes each JSON request body through `onPayload` before it is sent. */
export const payloadMiddleware =
  (onPayload: (payload: Schema.Json) => Effect.Effect<Schema.Json>): HttpMiddleware =>
  (request, handler) =>
    Effect.gen(function* () {
      const body = request.body;
      if (body._tag !== "Uint8Array" || request.headers["content-encoding"] !== undefined) {
        return yield* handler(request);
      }
      const decoded = Schema.decodeUnknownOption(JsonText)(decoder.decode(body.body));
      if (Option.isNone(decoded)) return yield* handler(request);
      const payload = yield* onPayload(decoded.value);
      const text = Schema.encodeSync(JsonText)(payload);
      return yield* handler(HttpClientRequest.bodyText(request, text, body.contentType));
    });

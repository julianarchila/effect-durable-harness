/**
 * The conversation view over hibernatable WebSockets: each socket follows one conversation's
 * `watch` on a fiber of the instance. A socket remembers its conversation in its attachment, which survives
 * hibernation and restarts; a new instance re-subscribes every open socket, and the first message each gets is a
 * fresh snapshot. Sockets are keyed by an ID in the attachment, not by the `WebSocket` object, which the runtime
 * makes anew for every event.
 */
import * as Wire from "@effect-durable-harness/delta/Wire";
import type { Harness, HarnessError } from "@effect-durable-harness/core/Harness";
import { ConversationId } from "@effect-durable-harness/core/Ids";
import * as Effect from "effect/Effect";
import * as FiberMap from "effect/FiberMap";
import * as Option from "effect/Option";
import * as Random from "effect/Random";
import * as Schema from "effect/Schema";
import type * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as HttpServerResponse from "effect/http/HttpServerResponse";
import type { HttpServerRequest } from "effect/http/HttpServerRequest";
import type { Engine, EngineSocket } from "../Engine.ts";
import type { ViewMessage } from "../View.ts";

/**
 * What a view socket keeps in its attachment. The `view` marker tells the host's own sockets apart from those an
 * object accepts for a protocol of its own, which restore leaves alone.
 */
const Attachment = Schema.Struct({
  view: Schema.Literal("durable"),
  /** The socket's own ID: the runtime hands each event a new `WebSocket` object for the same socket. */
  id: Schema.String,
  conversationId: ConversationId,
});

/** Close code of a socket whose conversation does not exist or retired. */
const GOING_AWAY = 1001;

export interface Views {
  readonly accept: (
    conversationId: ConversationId,
  ) => Effect.Effect<HttpServerResponse.HttpServerResponse, HarnessError, HttpServerRequest>;
  /** Re-subscribes the sockets an earlier instance accepted. */
  readonly restore: Effect.Effect<void>;
  readonly drop: (socket: EngineSocket) => Effect.Effect<void>;
}

const send = (socket: EngineSocket, message: ViewMessage) => socket.send(JSON.stringify(message));

export const make = (
  harness: Harness["Service"],
  engine: Engine["Service"],
): Effect.Effect<Views, never, Scope.Scope> =>
  Effect.gen(function* () {
    const subscriptions = yield* FiberMap.make<string>();

    /** Sends a snapshot, then one op batch per committed frame, until the socket or the conversation ends. */
    const follow = (socket: EngineSocket, conversationId: ConversationId) =>
      Effect.scoped(
        Effect.gen(function* () {
          const conversation = yield* harness.conversation(conversationId);
          if (Option.isNone(conversation)) {
            return yield* socket.close(GOING_AWAY, "unknown conversation");
          }
          const watch = yield* conversation.value.watch();
          const encoder = Wire.makeEncoder();
          yield* send(socket, { type: "snapshot", value: watch.value });
          yield* Stream.runForEach(watch.frames, (frame) =>
            send(socket, { type: "ops", ops: Wire.encode(encoder, frame.ops) }),
          );
          yield* socket.close(GOING_AWAY, "conversation retired");
        }),
      ).pipe(
        // A socket that closed under it, or a Session that failed (the object aborts): the subscription ends.
        Effect.catchCause(() => Effect.void),
      );

    const subscribe = (socket: EngineSocket, id: string, conversationId: ConversationId) =>
      FiberMap.run(subscriptions, id, follow(socket, conversationId)).pipe(Effect.asVoid);

    return {
      accept: (conversationId) =>
        Effect.gen(function* () {
          const conversation = yield* harness.conversation(conversationId);
          if (Option.isNone(conversation)) return HttpServerResponse.empty({ status: 404 });
          const id = `${yield* Random.nextIntBetween(0, 2 ** 31 - 1)}-${yield* Random.nextIntBetween(0, 2 ** 31 - 1)}`;
          const [response, socket] = yield* engine.upgrade;
          yield* socket
            .setAttachment(Attachment, { view: "durable", id, conversationId })
            .pipe(Effect.orDie);
          yield* subscribe(socket, id, conversationId);
          return response;
        }),
      restore: Effect.gen(function* () {
        for (const socket of yield* engine.sockets) {
          const attachment = yield* socket.getAttachment(Attachment).pipe(Effect.option);
          if (Option.isSome(attachment)) {
            yield* subscribe(socket, attachment.value.id, attachment.value.conversationId);
          }
        }
      }),
      drop: (socket) =>
        Effect.flatMap(socket.getAttachment(Attachment).pipe(Effect.option), (attachment) =>
          Option.isSome(attachment)
            ? FiberMap.remove(subscriptions, attachment.value.id)
            : Effect.void,
        ),
    };
  });

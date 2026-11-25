/**
 * One chat session: a Durable Object holding every conversation of the session. Alchemy's
 * modular class is the contract the API Worker binds; `SessionLive` is the implementation it provides. The
 * session itself (agent, socket, methods) is `chatSession.ts`, which `examples/rivet-chat` shares.
 *
 * The handlers and the models are provided outside the object, so they are built once per isolate, at init.
 */
import * as Host from "@effect-durable-harness/cloudflare/Host";
import * as Cloudflare from "@alchemy.run/cloudflare";
import * as Config from "effect/Config";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import { ChatLive } from "./agent/chat.live.ts";
import { ChatAgent, chatSocket, serveSession, type SessionMethods } from "./chatSession.ts";
import { ModelsLive } from "./models.ts";

export type { SessionMethods } from "./chatSession.ts";

export class Session extends Cloudflare.DurableObject<Session, SessionMethods>()("Session", {
  errors: Host.errors,
}) {}

export const SessionLive = Session.make(
  Effect.gen(function* () {
    // How far ahead the alarm stays while there is active work; the tests shorten it to recover quickly. Read at
    // init, so Alchemy binds it to the Worker.
    const keepAliveMs = yield* Config.option(Config.Int("CHAT_KEEP_ALIVE_MS")).pipe(Effect.orDie);
    const agent = yield* Host.agent(ChatAgent, {
      storage: { keepAliveMs: Option.getOrUndefined(keepAliveMs) },
      socket: chatSocket,
    });
    return yield* serveSession(agent);
  }),
).pipe(Layer.provide(ChatLive), Layer.provide(ModelsLive));

/**
 * One chat session on Rivet: `examples/cloudflare-chat`'s session (`src/server/chatSession.ts`: the agent,
 * the socket and the methods), unchanged, as a `Rivet.DurableObject` on the pool in `../cluster.ts`.
 *
 * The models are provided inside the class phase, unlike on Cloudflare (see `SessionLive`). The class is declared
 * on its own, so the API Worker that calls it bundles none of the agent: `SessionLive`, the
 * implementation, is provided where the stack deploys (`alchemy.run.ts`).
 */
import * as Rivet from "@alchemy.run/rivet";
import { ChatLive } from "@effect-durable-harness/example-cloudflare-chat/src/server/agent/chat.live.ts";
import {
  ChatAgent,
  chatSocket,
  serveSession,
  type SessionMethods,
} from "@effect-durable-harness/example-cloudflare-chat/src/server/chatSession.ts";
import { ModelsLive } from "@effect-durable-harness/example-cloudflare-chat/src/server/models.ts";
import { Host } from "@effect-durable-harness/rivet";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import { Pool } from "../cluster.ts";

export class Session extends Rivet.DurableObject<Session, SessionMethods>()("Session") {}

export const SessionLive = Session.make(
  { main: import.meta.url, handler: "SessionLive" },
  Effect.gen(function* () {
    const agent = yield* Host.agent(ChatAgent, { socket: chatSocket });
    return yield* serveSession(agent);
    // The tools and the models are built here, once per node. The models read the Gemini key with `Config` in the
    // class phase, which runs while deploying as well as once on each node: that is how the key reaches the pool's
    // environment (Alchemy's Rivet secrets). Provided with `Layer.provide` outside the class, as on Cloudflare, the
    // key would be read in the stack's context and never reach the nodes, which would fall back to the fake model.
  }).pipe(Effect.provide(Layer.mergeAll(ChatLive.pipe(Layer.provideMerge(ModelsLive)), Pool))),
);

/**
 * The chat session, whatever engine hosts it: the agent, the session socket and the instance's methods. Imports
 * nothing engine-specific (the neutral `@effect-durable-harness/host`), so `examples/rivet-chat` hosts the same
 * session on Rivet. Each engine's `Session.ts` declares the class, binds the agent with its host's `agent` and
 * provides the Layers.
 *
 * - The agent is `ChatHarness`, built once per instance over the object's SQLite by `agent.start`.
 * - The session socket is the host's (`Host.socket`): every connection carries its display name, which attributes
 *   its prompts, and a typing presence per watched conversation.
 */
import * as Host from "@effect-durable-harness/host/Host";
import * as Effect from "effect/Effect";
import * as Random from "effect/Random";
import * as Schema from "effect/Schema";
import type { HttpServerRequest } from "effect/http/HttpServerRequest";
import { attributedPrompt, normalizeUsername } from "../protocol.ts";
import { ChatHarness, ChatModel, chatAgent } from "./agent/chat.ts";
import { FAUX } from "./models.ts";

/** The one method the API Worker calls besides `fetch`. */
export interface SessionMethods {
  /** Resets the object, as an eviction does; only does so when the deployment runs the fake model (tests). */
  readonly crash: () => Effect.Effect<void>;
}

/**
 * Every top-level conversation starts with the chat agent, whoever creates it (the root, the socket's `create`); a
 * fork inherits its parent's, and a search subagent's conversation is configured by the tool that creates it.
 */
export const ChatAgent = ChatHarness.layer({
  defaults: { agent: Effect.map(Effect.service(ChatModel), (model) => chatAgent(model.ref)) },
});

/** A connection's display name, from `?username=`; kept in the socket's attachment. */
const Member = Schema.Struct({ username: Schema.String });

const accept = (request: HttpServerRequest) =>
  Effect.gen(function* () {
    const requested = normalizeUsername(
      new URL(request.url, "http://session").searchParams.get("username") ?? "",
    );
    // No authentication: this is a sample.
    return {
      username:
        requested.length > 0 ? requested : `Guest-${yield* Random.nextIntBetween(1000, 9999)}`,
    };
  });

/** The session socket: display names, attributed prompts, a typing presence. */
export const chatSocket = Host.socket({
  connection: Member,
  accept,
  input: (member, content) => attributedPrompt(member.username, content),
  presence: Schema.Struct({ typing: Schema.Boolean }),
});

/**
 * The instance phase, over an agent bound in the class phase by the engine's `Host.agent`: starts it, makes sure the
 * root exists (every client watches it first) and serves `SessionMethods`.
 */
export const serveSession = Effect.fnUntraced(function* <ROut, C, State, Socket>(
  agent: Host.HostedAgent<ROut, C, State, Socket>,
) {
  const model = yield* ChatModel;
  // @effect-diagnostics-next-line returnEffectInGen:off
  return Effect.gen(function* () {
    const host = yield* agent.start;
    yield* host.harness.ensureRoot().pipe(Effect.orDie);
    return host.methods<SessionMethods>({
      crash: () =>
        model.ref.provider === FAUX.provider ? host.abort("chat: crash (test)") : Effect.void,
    });
  });
});

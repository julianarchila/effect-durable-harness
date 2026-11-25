import * as Agent from "@effect-durable-harness/core/Agent";
import * as Harness from "@effect-durable-harness/core/Harness";
import { ConversationId } from "@effect-durable-harness/core/Ids";
import * as Cloudflare from "@alchemy.run/cloudflare";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import type { HttpServerRequest } from "effect/http/HttpServerRequest";
import * as Host from "../../../src/Host.ts";
import { FAUX, fakeModel } from "./AgentProbe.ts";

/** A connection's user, from `?username=`, kept in the socket's attachment. */
const User = Schema.Struct({ username: Schema.String });

class Refused extends Schema.TaggedError<Refused>()("Refused", {}) {}

/** Refuses a missing name and the name `intruder`. */
const accept = (request: HttpServerRequest) => {
  const username = new URL(request.url, "http://object").searchParams.get("username") ?? "";
  return username === "" || username === "intruder"
    ? Effect.fail(new Refused())
    : Effect.succeed({ username });
};

/** The session socket under test: attributed prompts, typing presence, and peers seen by name only. */
const Users = Host.socket({
  connection: User,
  accept,
  input: (user, content) => `[${user.username}]\n${content}`,
  peer: (user) => ({ name: user.username }),
  presence: Schema.Struct({ typing: Schema.Boolean, note: Schema.optionalKey(Schema.String) }),
});

/** The same socket for reading only. */
const Readers = Host.socket({ connection: User, accept, commands: false });

/** Every top-level conversation starts with the fake model (as the chat example's `conversationCreated`). */
const SessionAgent = Harness.layer({
  extensions: [],
  conversationCreated: (record) =>
    record.owner === undefined && record.parent === undefined
      ? Agent.configure(record.id, { model: FAUX })
      : Effect.void,
}).pipe(Layer.provideMerge(fakeModel));

/** The agent's RPC and the test's own methods, over the socket's server side. */
const probeMethods = (host: Host.Instance<typeof User.Type>) => ({
  ...Host.agentRpc(host.harness),
  /** Closes a user's sockets, as an application does for a member who lost access. */
  kick: (username: string) =>
    host.socket.close((user) => user.username === username, { code: 4403, reason: "kicked" }),
  connections: () =>
    Effect.map(host.socket.connections, (all) =>
      all.map((connection) => ({
        id: connection.id,
        username: connection.connection.username,
        watching: [...connection.watching],
      })),
    ),
  notify: (conversationId: number, text: string) =>
    host.socket.broadcast(ConversationId.make(conversationId), { text }),
  /** Resets the object, as an eviction or a crash does. */
  crash: () => host.abort("probe: crash"),
});

/** `SessionProbe`'s methods, as other Workers see them. */
export type SessionProbeMethods = ReturnType<typeof probeMethods>;

/** Starts the instance and answers its record. */
const start = <ROut>(agent: Host.HostedAgent<ROut, typeof User.Type>) =>
  Effect.gen(function* () {
    const host = yield* agent.start;
    yield* host.harness.ensureRoot().pipe(Effect.orDie);
    return host.methods(probeMethods(host));
  });

/** An object with a session socket: the agent's RPC is opted in. */
export class SessionProbe extends Cloudflare.DurableObject<SessionProbe, SessionProbeMethods>()(
  "SessionProbe",
  { errors: Host.errors },
) {}

export const SessionProbeLive = SessionProbe.make(
  Effect.map(Host.agent(SessionAgent, { storage: { keepAliveMs: 1_000 }, socket: Users }), start),
);

/** An object whose socket only reads. */
export class ReadOnlyProbe extends Cloudflare.DurableObject<ReadOnlyProbe, SessionProbeMethods>()(
  "ReadOnlyProbe",
  { errors: Host.errors },
) {}

export const ReadOnlyProbeLive = ReadOnlyProbe.make(
  Effect.map(Host.agent(SessionAgent, { storage: { keepAliveMs: 1_000 }, socket: Readers }), start),
);

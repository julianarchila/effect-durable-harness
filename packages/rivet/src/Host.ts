/**
 * An effect-durable-harness Session inside an Alchemy Durable Object on Rivet:
 * the engine-neutral host (`@effect-durable-harness/host`) bound to `Rivet.DurableObjectState`. The same API as
 * `@effect-durable-harness/cloudflare`'s `Host`; the object is a `Rivet.DurableObject`, which runs on a pool of nodes
 * on any cloud Alchemy deploys Rivet to.
 *
 * ```ts
 * export class Contact extends Rivet.DurableObject<Contact, ContactMethods>()("Contact") {}
 *
 * export const ContactLive = Contact.make(
 *   { main: import.meta.url, handler: "ContactLive" },
 *   Effect.gen(function* () {
 *     const agent = yield* Host.agent(AssistantAgent, { socket: Host.socket() }); // once per node
 *     return Effect.gen(function* () {
 *       const host = yield* agent.start; // once per activation
 *       return host.methods<ContactMethods>({ receive: (inbound) => … });
 *     });
 *   }).pipe(Effect.provide(Pool)),
 * ).pipe(Layer.provide(AssistantLive), Layer.provide(ModelsLive));
 * ```
 *
 * What differs from Cloudflare:
 *
 * - **Activations.** RivetKit stops an actor after 30 s without inbound events and keeps the process; the host ends
 *   the instance's Session then (`Engine.onStop`), and the next event starts a fresh one from storage. While work is
 *   active the keep-alive alarm comes every 20 s at most (`storage.keepAliveMs` is capped), which keeps it awake.
 * - **Sockets end with the activation.** Rivet 3's gateway closes them when the actor stops (1012, `actor.stopped`),
 *   and `SessionClient` reconnects; while one is open the actor stays awake (RivetKit does not let an actor with a
 *   connection sleep), where a Cloudflare object hibernates.
 * - **Calls interleave around storage** by default: Alchemy's input gate (`Workers.InputGate`) is opt-in. The
 *   kernel orders the Session's own writes and needs no gate; an application method that reads and then writes
 *   storage of its own uses a transaction, or the class takes the gate. The host runs under it too.
 * - **Errors across RPC** arrive with their tag and fields, as data: `Effect.catchTag` works, `instanceof` does not.
 *   `Rivet.DurableObject` has no `errors` option.
 * - **Recovery after a crash** waits for the engine to declare the node lost (about 45 s on a local engine).
 *
 * @since 0.0.0
 */
import type * as Rivet from "@alchemy.run/rivet";
import type { Engine } from "@effect-durable-harness/host/Engine";
import * as HostCore from "@effect-durable-harness/host/Host";
import type { RuntimeContext } from "alchemy";
import type * as Layer from "effect/Layer";
import { adapter } from "./internal/engine.ts";

// Everything engine-neutral is the host's; this module adds the engine's bindings, and its `Handlers`, `Served`,
// `Instance` and `HostedAgent` (declared below) shadow the host's generic ones.
export * from "@effect-durable-harness/host/Host";
export { MAX_KEEP_ALIVE_MS } from "./internal/engine.ts";

/**
 * The host's `Engine` over this object's state: what `storage` and `layer` require in a constructor built by hand.
 * `agent` builds it itself.
 *
 * @category layers
 * @since 0.0.0
 */
export const engine: Layer.Layer<Engine, never, Rivet.DurableObjectState | RuntimeContext> =
  adapter.layer;

/**
 * The object's handlers, to return from its per-instance constructor.
 *
 * @category models
 * @since 0.0.0
 */
export type Handlers = HostCore.Handlers<Rivet.WebSocket>;

/**
 * The object's record as `Instance.methods` answers it: the host's handlers under the application's methods, with
 * `alarm` always the host's.
 *
 * @category models
 * @since 0.0.0
 */
export type Served<M> = HostCore.Served<M, Rivet.WebSocket>;

/**
 * One started instance of the object, as `HostedAgent.start` answers it.
 *
 * @category models
 * @since 0.0.0
 */
export type Instance<C, ROut = never> = HostCore.Instance<C, ROut, Rivet.WebSocket>;

/**
 * An agent captured by `agent` in the object's class phase, to `start` in its instance phase.
 *
 * @category models
 * @since 0.0.0
 */
export type HostedAgent<ROut, C> = HostCore.HostedAgent<
  ROut,
  C,
  Rivet.DurableObjectState | RuntimeContext,
  Rivet.WebSocket
>;

/**
 * Hosts an agent in an Alchemy Durable Object on Rivet: yield it in the outer constructor of the object's
 * `make`, then yield `start` in the inner one and return `host.methods(...)`. See `HostCore.bindAgent` in
 * `@effect-durable-harness/host` for the phases.
 *
 * @category constructors
 * @since 0.0.0
 */
export const agent = HostCore.bindAgent<Rivet.DurableObjectState, Rivet.WebSocket>(adapter);

/**
 * An effect-durable-harness Session inside an Alchemy Durable Object on Cloudflare: the engine-neutral host (`@effect-durable-harness/host`) bound to
 * `Cloudflare.DurableObjectState`. The object is Alchemy's own, and the host is a piece of its two-phase constructor.
 *
 * ```ts
 * export interface SessionMethods {
 *   readonly crash: () => Effect.Effect<void>;
 * }
 *
 * export class Session extends Cloudflare.DurableObject<Session, SessionMethods>()("Session", {
 *   errors: Host.errors,
 * }) {}
 *
 * export const SessionLive = Session.make(
 *   Effect.gen(function* () {
 *     const agent = yield* Host.agent(ChatAgent, { socket: Host.socket({ connection: Member, accept }) });
 *     return Effect.gen(function* () {
 *       const host = yield* agent.start;
 *       return host.methods<SessionMethods>({ crash: () => host.abort("crash") });
 *     });
 *   }),
 * ).pipe(Layer.provide(ChatLive), Layer.provide(ModelsLive));
 * ```
 *
 * - **Isolate**, once per isolate (and once at Alchemy's plan time): the object's Layer is built, with the Layers
 *   provided to it (handlers, services, models, bindings), and `agent` captures it.
 * - **Instance**, on every construction of the object (its first event, and every wake after eviction or
 *   hibernation): `start` builds the agent over this object's SQLite, the host and the session socket;
 *   `host.methods` turns the application's methods into the object's record.
 * - The alarm is armed before a commit that leaves a live task, and set afresh from the kernel's wake when an
 *   instance starts. While the kernel reports active work a pending timer keeps the object from hibernating, so a
 *   wait in memory with no I/O pending is not lost; the kernel's idle-cache timers are off, so an idle object with
 *   open sockets hibernates.
 * - **Active work is billed.** A held object is billed for duration for as long as the kernel reports active work,
 *   and a wait in memory that never ends is not cut by `restartLimit`, since nothing restarts it. Wait for long
 *   with a parked tool (`AgentTool.wait`, `AgentTool.sleep`) or a task's `sleepUntil`, which hold no invocation,
 *   not with a wait in memory (`awaitTask`, `executeTool` on a parked call, a `Deferred`).
 *
 * **Telemetry**. Provide `Cloudflare.Telemetry()` on the Worker that hosts the object, next to the object's
 * Layer: its Effect spans, the kernel's `durable.submit` and `durable.phase` among them, join Workers Observability.
 * `Cloudflare.Telemetry({ headSamplingRate: 0.1 })` traces a tenth of the invocations; `persist: false` exports
 * without keeping traces in the dashboard. It needs a compatibility date from `2026-07-28`; `compatibility()` pins
 * `2026-08-31`. The object's fibers start their spans on the event in progress, else the latest event, or the event
 * that constructed the instance. Only scalar attributes reach Cloudflare: follow one run across invocations by
 * `durable.trace.id`. A span that ends after the object's next event started (a model request, a long tool) has lost
 * its attributes and exit in Cloudflare; the host records them on a span `<name> outcome` in an open event.
 *
 * ```ts
 * export default class Api extends Cloudflare.Worker<Api>()("Api", { main: import.meta.url, compatibility: compatibility() },
 *   handler.pipe(Effect.provide(Layer.mergeAll(SessionLive, Cloudflare.Telemetry()))),
 * ) {}
 * ```
 *
 * @since 0.0.0
 */
import type * as Cloudflare from "@alchemy.run/cloudflare";
import type { Engine } from "@effect-durable-harness/host/Engine";
import * as HostCore from "@effect-durable-harness/host/Host";
import type { RuntimeContext } from "alchemy";
import type * as Layer from "effect/Layer";
import { adapter } from "./internal/engine.ts";

// Everything engine-neutral is the host's; this module adds the engine's bindings, and its `Handlers`, `Served`,
// `Instance` and `HostedAgent` (declared below) shadow the host's generic ones.
export * from "@effect-durable-harness/host/Host";

/**
 * The host's `Engine` over this object's state: what `storage` and `layer` require in a constructor built by hand.
 * `agent` builds it itself.
 *
 * @category layers
 * @since 0.0.0
 */
export const engine: Layer.Layer<Engine, never, Cloudflare.DurableObjectState | RuntimeContext> =
  adapter.layer;

/**
 * The object's handlers, to return from its per-instance constructor.
 *
 * @category models
 * @since 0.0.0
 */
export type Handlers = HostCore.Handlers<Cloudflare.WebSocket>;

/**
 * The object's record as `Instance.methods` answers it: the host's handlers under the application's methods, with
 * `alarm` always the host's.
 *
 * @category models
 * @since 0.0.0
 */
export type Served<M> = HostCore.Served<M, Cloudflare.WebSocket>;

/**
 * One started instance of the object, as `HostedAgent.start` answers it.
 *
 * @category models
 * @since 0.0.0
 */
export type Instance<C, ROut = never> = HostCore.Instance<C, ROut, Cloudflare.WebSocket>;

/**
 * An agent captured by `agent` in the object's isolate phase, to `start` in its instance phase.
 *
 * @category models
 * @since 0.0.0
 */
export type HostedAgent<ROut, C> = HostCore.HostedAgent<
  ROut,
  C,
  Cloudflare.DurableObjectState | RuntimeContext,
  Cloudflare.WebSocket
>;

/**
 * Hosts an agent in an Alchemy Durable Object on Cloudflare: yield it in the outer constructor of the
 * object's `make`, then yield `start` in the inner one and return `host.methods(...)`. See `HostCore.bindAgent` in
 * `@effect-durable-harness/host` for the phases.
 *
 * ```ts
 * export class Contact extends Cloudflare.DurableObject<Contact, ContactMethods>()("Contact", {
 *   errors: Host.errors,
 * }) {}
 *
 * export const ContactLive = Contact.make(
 *   Effect.gen(function* () {
 *     const agent = yield* Host.agent(AssistantAgent, { socket: Members }); // once per isolate
 *     const staff = yield* Staff; // an isolate service a method uses
 *     return Effect.gen(function* () {
 *       const host = yield* agent.start; // once per instance
 *       return host.methods<ContactMethods>({ receive: (inbound) => … });
 *     });
 *   }),
 * ).pipe(Layer.provide(AssistantLive), Layer.provide(ModelsLive));
 * ```
 *
 * @category constructors
 * @since 0.0.0
 */
export const agent = HostCore.bindAgent<Cloudflare.DurableObjectState, Cloudflare.WebSocket>(
  adapter,
);

/**
 * The engine-neutral host of an effect-durable-harness Session inside an Alchemy Durable Object. It is written against `Engine` only; an engine package binds it to its object state with
 * `bindAgent` and re-exports it as its `Host` (`@effect-durable-harness/cloudflare`, `@effect-durable-harness/rivet`).
 * Applications import their engine's `Host`, not this module.
 *
 * - **Isolate**, once per isolate (and once at Alchemy's plan time): the object's Layer is built, with the Layers
 *   provided to it (handlers, services, models, bindings), and `agent` captures it.
 * - **Instance**, on every construction of the object (its first event, and every wake after eviction, hibernation or
 *   sleep): `start` builds the engine over the object's state, the agent over the engine's SQLite (`storage`), the host
 *   (`layer`) and the session socket; `host.methods` turns the application's methods into the object's record.
 * - `storage`: the Session's storage over the engine's SQLite, with a fallback alarm armed before the Harness opens
 *   and the alarm armed before a commit that leaves a live task; the kernel's wake drives the alarm
 *   (`SessionWakeups`) and holds the instance in memory while it reports active work (`Engine.keepAwake`), a
 *   failed alarm write resets the object, and its idle-cache timers are off (`IdleTimers`), so an idle object with
 *   open sockets hibernates.
 * - `layer`: once the Harness is built, resumes it, re-subscribes the view sockets, sets the alarm afresh from the
 *   wake, and aborts the object if the Session fails.
 *
 * @since 0.0.0
 */
import {
  AgentStorage,
  StorageFailure,
  StorageRequestError,
} from "@effect-durable-harness/core/AgentStorage";
import { EntryNotVisible } from "@effect-durable-harness/core/ContextView";
import { Harness } from "@effect-durable-harness/core/Harness";
import { ConversationId, ROOT_CONVERSATION_ID } from "@effect-durable-harness/core/Ids";
import { IdleTimers } from "@effect-durable-harness/core/IdleTimers";
import { Session } from "@effect-durable-harness/core/Session";
import {
  CommitRejected,
  ReadOnlySession,
  DocumentAccessError,
  SessionClosed,
  SessionFailed,
} from "@effect-durable-harness/core/SessionError";
import { SessionWakeups, type Wake } from "@effect-durable-harness/core/SessionWakeups";
import * as SqlAgentStorage from "@effect-durable-harness/core/SqlAgentStorage";
import {
  ConversationBusy,
  ConversationIdle,
  SubmissionError,
} from "@effect-durable-harness/core/Submission";
import { TaskError } from "@effect-durable-harness/core/TaskError";
import * as Alchemy from "alchemy";
import { RuntimeContext } from "alchemy";
import * as Cause from "effect/Cause";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FiberHandle from "effect/FiberHandle";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";
import * as Tracer from "effect/Tracer";
import { HttpServerRequest } from "effect/http/HttpServerRequest";
import * as HttpServerResponse from "effect/http/HttpServerResponse";
import * as SqlClient from "effect/sql/SqlClient";
import { Engine, type EngineAdapter, type EngineSocket } from "./Engine.ts";
import * as AgentRpcModule from "./internal/agentRpc.ts";
import type { AgentRpc } from "./internal/agentRpc.ts";
import * as AlarmStore from "./internal/alarmStore.ts";
import * as AlarmGuard from "./internal/alarmGuard.ts";
import { ConversationNotFound, RequestConflict } from "./internal/errors.ts";
import * as SocketModule from "./internal/socket.ts";
import * as EventTracer from "./internal/tracer.ts";
import * as View from "./internal/view.ts";

/**
 * Default interval of the keep-alive alarm while there is active work. An engine that stops an idle instance
 * sooner lowers it (`Engine.maxKeepAliveMs`).
 *
 * @category constants
 * @since 0.0.0
 */
export const DEFAULT_KEEP_ALIVE_MS = 30_000;

/**
 * Runs an Alchemy client's call from a tool, a task phase, a hook or a section. Alchemy's runtime clients (KV,
 * R2, Hyperdrive and the rest) require the event's `RuntimeContext`; a handler Layer is built at init, which has
 * none, so its handlers cannot list it. They run on the object's fibers, which carry the instance's
 * `RuntimeContext`, and this provides it from there. Outside an object it dies.
 *
 * ```ts
 * const NotesLive = SaveNote.toLayer(
 *   Effect.gen(function* () {
 *     const kv = yield* Cloudflare.KV.ReadWriteNamespace(Notes); // at init: the binding is seen at plan time
 *     return (args) => Host.withRuntimeContext(kv.put(args.key, args.text)).pipe(Effect.as(AgentTool.text("saved")));
 *   }),
 * );
 * ```
 *
 * @category utils
 * @since 0.0.0
 */
export const withRuntimeContext = <A, E, R>(
  effect: Effect.Effect<A, E, R>,
): Effect.Effect<A, E, Exclude<R, RuntimeContext>> =>
  Effect.flatMap(Effect.serviceOption(RuntimeContext), (context) =>
    Option.isSome(context)
      ? Effect.provideService(effect, RuntimeContext, context.value)
      : Effect.die(
          new Error("Host.withRuntimeContext: no RuntimeContext; run it inside a Durable Object"),
        ),
  );

/**
 * Options of `storage`.
 *
 * @category models
 * @since 0.0.0
 */
export interface StorageOptions {
  /** Table prefix of `SqlAgentStorage`; default `durable_`. */
  readonly prefix?: string | undefined;
  /**
   * How far ahead the alarm stays while there is active work; default 30 s, and never more than the engine
   * allows (`Engine.maxKeepAliveMs`).
   */
  readonly keepAliveMs?: number | undefined;
}

/**
 * The object's alarm, as `storage` drives it: `layer` re-arms it from the kernel's wake.
 *
 * @category services
 * @since 0.0.0
 */
export class Alarm extends Context.Service<
  Alarm,
  {
    /**
     * Applies the alarm policy to a wake, read under the lock that orders the wakes applied, so a wake the host
     * reads never lands after a newer one the kernel reported. `stored`: the engine's alarm is the one this instance
     * set. `unknown`: what the engine reports is not trusted, so the alarm is set anew, or deleted when nothing is
     * due: at the instance's start, and in `alarm()`, where the alarm that fired is gone once the handler
     * returns, whatever the storage still says.
     */
    readonly rearm: <E>(
      wake: Effect.Effect<Wake, E>,
      current: "stored" | "unknown",
    ) => Effect.Effect<void, E>;
  }
>()("@effect-durable-harness/host/Host/Alarm") {}

/**
 * The Session's storage over the engine's SQLite, the alarm before the commit, the wake on the alarm and the kernel's
 * idle-cache timers off. Build it in the object's per-instance constructor, under `Harness.layer`.
 *
 * @category layers
 * @since 0.0.0
 */
export const storage = (
  options: StorageOptions = {},
): Layer.Layer<AgentStorage | Alarm, never, Engine> =>
  Layer.effectContext(
    Effect.gen(function* () {
      const engine = yield* Engine;
      const keepAliveMs = Math.min(
        options.keepAliveMs ?? DEFAULT_KEEP_ALIVE_MS,
        engine.maxKeepAliveMs,
      );
      const base = yield* Layer.build(SqlAgentStorage.layer({ prefix: options.prefix })).pipe(
        Effect.provideService(SqlClient.SqlClient, engine.sql),
        Effect.orDie,
      );
      const alarms = AlarmStore.make(engine.alarm);
      // A wakeup before the Harness opens: an instance whose start fails is started again. An earlier alarm is
      // kept, and written again, since what the engine reports at the start of an instance is not trusted.
      const limit = (yield* alarms.now) + keepAliveMs;
      const earlier = Option.filter(yield* Effect.orDie(alarms.get), (at) => at <= limit);
      yield* alarms
        .apply({ _tag: "Set", at: Option.getOrElse(earlier, () => limit) })
        .pipe(Effect.orDie);
      const guard = AlarmGuard.make(alarms, keepAliveMs);
      // Applying a wake goes one at a time: the kernel's wake reports and `alarm()`.
      const lock = Semaphore.makeUnsafe(1);
      // The keep-alive alarm brings an event before the engine would stop an instance for want of one, but an
      // instance between events can still hibernate, and a wait in memory with no I/O pending is lost with it: the
      // next instance reruns the phase and counts a crash. The engine holds the instance while the latest wake
      // reports active work, and lets it go, so it can hibernate, once a wake reports none. The hold follows the wake as
      // reported: one a live commit overtook is followed by the kernel's next report, which comes at once.
      const awake = yield* FiberHandle.make<never, never>();
      const hold = (wake: Wake) =>
        wake.activeWork
          ? FiberHandle.run(awake, engine.keepAwake, { onlyIfMissing: true }).pipe(Effect.asVoid)
          : FiberHandle.clear(awake);
      const applyWake = (wake: Wake, seen: AlarmGuard.Seen, current: "stored" | "unknown") =>
        guard.apply(wake, seen, current).pipe(Effect.andThen(hold(wake)));
      // The wake is read under the lock: one read before it could be applied after the kernel reported a newer
      // one, as when the host's start read "reconcile due" and the kernel then reported idle; the stale keep-alive
      // it armed restarted an evicted idle object every keep-alive, forever.
      const rearm = <E>(read: Effect.Effect<Wake, E>, current: "stored" | "unknown") =>
        lock.withPermits(1)(
          Effect.suspend(() => {
            const seen = guard.observe();
            return Effect.flatMap(read, (wake) => applyWake(wake, seen, current));
          }),
        );
      return Context.make(AgentStorage, guard.wrap(Context.get(base, AgentStorage))).pipe(
        Context.add(Alarm, { rearm }),
        Context.add(SessionWakeups, {
          // The wake is computed when the kernel calls `schedule`, so what it has seen is taken there.
          schedule: (wake: Wake) => {
            const seen = guard.observe();
            return lock
              .withPermits(1)(applyWake(wake, seen, "stored"))
              .pipe(
                // A failed alarm write resets the object, which arms the alarm again when it next starts.
                Effect.tapCause((cause) =>
                  Cause.hasInterruptsOnly(cause)
                    ? Effect.void
                    : Effect.logWarning("durable host: alarm write failed", cause).pipe(
                        Effect.andThen(engine.abort("durable: the alarm write failed")),
                      ),
                ),
              );
          },
        }),
        // A pending idle-cache timer keeps an object with open sockets from hibernating: caches expire as they do
        // on Workers, on commits and task changes.
        Context.add(IdleTimers, { enabled: false }),
      );
    }),
  );

/**
 * The object's handlers, to return from its per-instance constructor. `Socket` is the engine's native socket, which
 * the object's `webSocket*` handlers receive.
 *
 * @category models
 * @since 0.0.0
 */
export interface Handlers<Socket = EngineSocket> {
  /** Resumes the Session and re-arms the alarm from its wake; never runs work inside the invocation. */
  readonly alarm: () => Effect.Effect<void>;
  /** Upgrades `?conversation=<id>` (the root when absent) to that conversation's view socket. */
  readonly fetch: Effect.Effect<HttpServerResponse.HttpServerResponse, never, HttpServerRequest>;
  /** A binary frame arrives as an `ArrayBuffer` on Cloudflare and a `Uint8Array` on Rivet. */
  readonly webSocketMessage: (
    socket: Socket,
    message: string | ArrayBuffer | Uint8Array,
  ) => Effect.Effect<void>;
  readonly webSocketClose: (
    socket: Socket,
    code: number,
    reason: string,
    wasClean: boolean,
  ) => Effect.Effect<void>;
  // oxlint-disable-next-line anti-slop/no-unknown-parameters -- the runtime's socket error, as Alchemy passes it
  readonly webSocketError: (socket: Socket, error: unknown) => Effect.Effect<void>;
}

/**
 * The host of one object instance.
 *
 * @category services
 * @since 0.0.0
 */
export class Host extends Context.Service<Host, { readonly handlers: Handlers }>()(
  "@effect-durable-harness/host/Host",
) {}

/**
 * Starts the host over a built Harness: resumes scheduling (`resume`, without waiting for a call; a
 * read-only Harness runs nothing and is not resumed), re-subscribes the sockets an earlier instance accepted, sets
 * the alarm afresh from the wake, and resets the object when the Session fails, so the next event opens a
 * fresh one.
 *
 * @category layers
 * @since 0.0.0
 */
export const layer: Layer.Layer<Host, never, Harness | Alarm | Engine> = Layer.effect(
  Host,
  Effect.gen(function* () {
    const harness = yield* Harness;
    const alarm = yield* Alarm;
    const engine = yield* Engine;

    yield* Effect.forkScoped(
      harness.closed.pipe(
        Effect.flatMap((end) =>
          end.reason === "failed" ? engine.abort("durable: the Session failed") : Effect.void,
        ),
      ),
    );
    const views = yield* View.make(harness, engine);
    // A read-only Harness (`readOnly`) runs no tasks and refuses `resume`: the host serves its reads and
    // views, and its wake is always inactive, so the alarm stays clear.
    const resume = harness.resume.pipe(Effect.catchTag("ReadOnlySession", () => Effect.void));
    yield* resume.pipe(Effect.orDie);
    yield* views.restore;
    yield* alarm.rearm(harness.wake.pipe(Effect.orDie), "unknown");

    const handlers: Handlers = {
      alarm: () =>
        Effect.gen(function* () {
          yield* resume;
          yield* alarm.rearm(harness.wake, "unknown");
        }).pipe(
          Effect.withSpan("durable.host.alarm"),
          // A failed alarm is retried by the platform (at-least-once delivery), so it is not swallowed: a failed
          // Session is also aborting the object, and the retry reaches a fresh instance.
          Effect.tapCause((cause) => Effect.logWarning("durable host: alarm failed", cause)),
          Effect.orDie,
        ),
      fetch: Effect.gen(function* () {
        const request = yield* HttpServerRequest;
        const id = viewConversation(request.url);
        if (id === undefined) return HttpServerResponse.empty({ status: 400 });
        return yield* views.accept(id).pipe(Effect.withSpan("durable.host.view"));
      }).pipe(Effect.catchCause(() => Effect.succeed(HttpServerResponse.empty({ status: 503 })))),
      webSocketMessage: () => Effect.void,
      webSocketClose: (socket, code, reason) =>
        views.drop(socket).pipe(
          // Completes the closing handshake; a reserved code (1005, 1006) cannot be sent back, and then there is
          // nothing left to close.
          Effect.andThen(Effect.exit(socket.close(code, reason))),
          Effect.asVoid,
        ),
      webSocketError: (socket) => views.drop(socket),
    };
    return { handlers };
  }).pipe(Effect.withSpan("durable.host.start")),
);

/**
 * The session socket's configuration, from `socket`, to pass to `agent`.
 *
 * @category models
 * @since 0.0.0
 */
export type SessionSocket<C, R = never> = SocketModule.SocketConfig<C, R>;

/**
 * One open connection of the session socket.
 *
 * @category models
 * @since 0.0.0
 */
export type SocketConnection<C> = SocketModule.SocketConnection<C>;

/**
 * Options of `SocketHandle.close`.
 *
 * @category models
 * @since 0.0.0
 */
export type SocketCloseOptions = SocketModule.SocketCloseOptions;

/**
 * Picks connections for `SocketHandle.close` and `SocketHandle.send`.
 *
 * @category models
 * @since 0.0.0
 */
export type ConnectionPredicate<C> = SocketModule.ConnectionPredicate<C>;

/**
 * The session socket's server side: its connections, `close`, `send` and `broadcast`.
 *
 * @category models
 * @since 0.0.0
 */
export type SocketHandle<C> = SocketModule.SocketHandle<C>;

export type { AgentRpc, ConversationRequest } from "./internal/agentRpc.ts";

export { ConversationNotFound, RequestConflict } from "./internal/errors.ts";

/**
 * The agent's own methods for other Workers (`AgentRpc`), over a built Harness: spread them into the object's
 * methods to expose them, call them from your own methods, or leave them out.
 *
 * ```ts
 * host.methods({ ...Host.agentRpc(host.harness), crash })
 * ```
 *
 * @category constructors
 * @since 0.0.0
 */
export const agentRpc = (harness: Harness["Service"]): AgentRpc => AgentRpcModule.make(harness);

/**
 * effect-durable-harness's tagged errors, for the `errors` option of the object's class: a method's failure arrives on the caller's side
 * as the same class, so `Effect.catchTag` works across the RPC boundary. Append the application's own errors:
 * `errors: [...Host.errors, LeadError]`.
 *
 * @category constants
 * @since 0.0.0
 */
export const errors = [
  StorageFailure,
  StorageRequestError,
  SessionFailed,
  SessionClosed,
  CommitRejected,
  ReadOnlySession,
  DocumentAccessError,
  EntryNotVisible,
  SubmissionError,
  ConversationBusy,
  ConversationIdle,
  TaskError,
  ConversationNotFound,
  RequestConflict,
] as const;

/**
 * Any of effect-durable-harness's tagged errors in `errors`: the error channel to write in a hand-written method
 * interface, beside the application's own errors.
 *
 * ```ts
 * readonly open: (input: OpenTicket) => Effect.Effect<Opened, Host.AgentError>;
 * ```
 *
 * @category models
 * @since 0.0.0
 */
export type AgentError = InstanceType<(typeof errors)[number]>;

/**
 * Options of `socket`.
 *
 * @category models
 * @since 0.0.0
 */
export interface SocketOptions<C, P, E, R> {
  /**
   * The connection state, kept in the socket's attachment and restored after hibernation. The attachment, with the
   * watched conversations (at most `MAX_WATCHED_CONVERSATIONS`) and presence (at most `MAX_PRESENCE_CHARACTERS` each),
   * holds at most 2 KiB: a connection state over it refuses the upgrade, and a watch or presence over it is refused.
   */
  readonly connection: Schema.Codec<C, Schema.Json>;
  /**
   * Authorizes an upgrade and answers the connection's state. A failure refuses it: the upgrade is accepted and closed
   * at once with `REFUSED_CLOSE_CODE`, which a browser can read (it cannot read an upgrade's HTTP status), and
   * `SessionClient` stops reconnecting. It runs in the call's context: a `Scope` it uses is the request's.
   */
  readonly accept: (request: HttpServerRequest) => Effect.Effect<C, E, R>;
  /**
   * What the other watchers of a conversation see of this connection in `presence` events; default the whole
   * connection state. **Gotcha:** every watcher receives it, so a state that holds what peers should not see (an
   * email, a role, a token) needs a projection: `peer: (member) => ({ name: member.name })`.
   */
  readonly peer?: ((connection: C) => Schema.Json) | undefined;
  /** Rewrites a prompt before it is submitted, for example to attribute it to the connection's user. */
  readonly input?: ((connection: C, content: string) => string) | undefined;
  /** Ephemeral per-connection presence, broadcast to the other watchers of a conversation. Off when absent. */
  readonly presence?: Schema.Codec<P, Schema.Json> | undefined;
  /** Whether the socket accepts commands that write; default `true`. `false` makes a socket that only reads. */
  readonly commands?: boolean | undefined;
}

const NoConnection = Schema.Struct({});

/**
 * The session socket (`@effect-durable-harness/host/SessionProtocol`), to pass to `agent`. One WebSocket per client
 * carries the session's conversations and activity, a view per watched conversation, presence, and the client's
 * commands (create, fork, submit, withdraw, abort), each idempotent by its `requestId`. Without options, every
 * upgrade is accepted with an empty connection state.
 *
 * @category constructors
 * @since 0.0.0
 */
export function socket(): SessionSocket<typeof NoConnection.Type>;
export function socket<C, P = never, E = never, R = never>(
  options: SocketOptions<C, P, E, R>,
): SessionSocket<C, R>;
export function socket<C, P, E, R>(
  options?: SocketOptions<C, P, E, R>,
): SessionSocket<C, R> | SessionSocket<typeof NoConnection.Type> {
  if (options === undefined) {
    return {
      connection: NoConnection,
      accept: () => Effect.succeedSome({}),
      input: (_connection: typeof NoConnection.Type, content: string) => content,
      peer: (connection) => connection,
      presence: undefined,
      commands: true,
    };
  }
  const presence = options.presence;
  const peer = options.peer;
  const decodeConnection = Schema.decodeUnknownOption(options.connection);
  return {
    connection: options.connection,
    // A failure refuses the upgrade; a defect stays one (503).
    accept: (request) =>
      options.accept(request).pipe(
        Effect.map(Option.some),
        Effect.catch((error) =>
          Effect.logDebug("durable host: socket refused", error).pipe(Effect.as(Option.none())),
        ),
      ),
    input: options.input ?? ((_connection: C, content: string) => content),
    // A state that no longer decodes shows as `null` rather than unprojected.
    peer:
      peer === undefined
        ? (connection) => connection
        : (connection) =>
            Option.match(decodeConnection(connection), {
              onNone: () => null,
              onSome: peer,
            }),
    presence:
      presence === undefined
        ? undefined
        : (value) =>
            Option.flatMap(Schema.decodeUnknownOption(presence)(value), (decoded) =>
              Schema.encodeOption(presence)(decoded),
            ),
    commands: options.commands ?? true,
  };
}

/**
 * Options of `agent`.
 *
 * @category models
 * @since 0.0.0
 */
export interface AgentOptions<C, RS> {
  /** The tables' prefix and the alarm's keep-alive. */
  readonly storage?: StorageOptions | undefined;
  /** The session socket; without it, `fetch` serves the read-only view of one conversation (`?conversation=<id>`). */
  readonly socket?: SessionSocket<C, RS> | undefined;
}

/**
 * The agent's services in one instance: the Harness, its Session and storage, and what the agent Layer outputs
 * besides them (`ROut`). `Instance.context` holds them, for an Effect that requires them. They hold no `Scope` or
 * event context, so providing them inside a call keeps the call's own. A service built once per isolate is yielded
 * in the object's outer constructor instead.
 *
 * @category models
 * @since 0.0.0
 */
export type InstanceServices<ROut> = Harness | Session | ROut | AgentStorage;

/**
 * The object's record as `Instance.methods` answers it: the host's handlers under the application's methods, with
 * `alarm` always the host's. `Socket` is the engine's native socket.
 *
 * @category models
 * @since 0.0.0
 */
export type Served<M, Socket> = Omit<Handlers<Socket>, keyof M> &
  Omit<M, "alarm"> &
  Pick<Handlers<Socket>, "alarm">;

/**
 * One started instance of the object, as `HostedAgent.start` answers it: the agent, the session socket's server side
 * (connection states decoded), the instance's lifecycle, and `methods`, which turns the application's methods into
 * the object's record.
 *
 * @category models
 * @since 0.0.0
 */
export interface Instance<C, ROut, Socket> {
  /** The agent, built over this object's SQLite and resumed. */
  readonly harness: Harness["Service"];
  /** The agent's Session: commits, documents, snapshots. */
  readonly session: Session["Service"];
  /** The session socket's connections, `close`, `send` and `broadcast`; none without a socket. */
  readonly socket: SocketHandle<C>;
  /**
   * Resets the instance, as an eviction does: the next event constructs a fresh one, which recovers from the last
   * commit. Keeps the application's code off the engine's own state.
   */
  readonly abort: (reason: string) => Effect.Effect<void>;
  /** The instance's services, for `Effect.provideContext` when a method's code requires them. */
  readonly context: Context.Context<InstanceServices<ROut>>;
  /**
   * The object's record: the host's handlers under `methods`, each traced into the event that runs it. A
   * `fetch` or `webSocket*` method replaces the host's; `alarm` is always the host's. Return it from
   * the inner constructor. Pass the class's shape to type the methods from it: `host.methods<SupportMethods>({ … })`.
   */
  readonly methods: <M = Record<never, never>>(methods?: M) => Served<M, Socket>;
}

/**
 * An agent captured by `agent` in the object's isolate phase, to `start` in its instance phase. `State` is what the
 * engine's object provides its instance (its state service and Alchemy's `RuntimeContext`).
 *
 * @category models
 * @since 0.0.0
 */
export interface HostedAgent<ROut, C, State, Socket> {
  /**
   * Builds the agent over this object's SQLite, resumes it, restores the sockets and arms the alarm. If the rest of
   * the construction fails, the instance's scope closes with it, so a failed construction leaves no agent running.
   */
  readonly start: Effect.Effect<Instance<C, ROut, Socket>, never, State | Scope.Scope>;
}

/**
 * Binds the host to an engine: the `agent` constructor an engine package exports as `Host.agent`.
 * Yield `agent(...)` in the outer constructor of the object's `make`, then yield `start` in the inner one and return
 * `host.methods(...)`.
 *
 * `agent` runs in Alchemy's isolate phase: once at plan time, with a mock state, and then at the start of every
 * instance's construction, so it does no I/O. It captures the context the object's Layer was provided with, where
 * handlers, models and services come from; that context is built once per isolate and shared by its instances.
 * `start` builds the engine over the instance's state, the agent (the kernel's `Harness.layer`) over the captured
 * context and the engine's `storage`, then the host and the socket, all in a scope that lasts as long as the instance:
 * closed if the construction fails, and when the engine stops the instance (`Engine.onStop`).
 *
 * The captured context never carries a `Scope`: what is re-provided from it (to `accept`) runs under the call's
 * context, so the call's own `Scope` and telemetry win, and a scoped resource of `accept` is released with the call.
 *
 * The agent's requirements other than its storage, and the socket's, become the object Layer's: a handler Layer is
 * built once per isolate, and a binding it requests is seen when Alchemy plans the Worker.
 *
 * @category constructors
 * @since 0.0.0
 */
export const bindAgent =
  <State, Socket>(adapter: EngineAdapter<State | RuntimeContext, Socket>) =>
  <ROut, E, RIn, C = Schema.Json, RS = never>(
    agentLayer: Layer.Layer<Harness | Session | ROut, E, RIn>,
    options: AgentOptions<C, RS> = {},
  ): Effect.Effect<
    HostedAgent<ROut, C, State | RuntimeContext, Socket>,
    never,
    Exclude<RIn, AgentStorage | Alarm | Engine | State | RuntimeContext> | RS
  > =>
    // SAFETY: the signature states the composition's types: `storage` provides `AgentStorage` (and the wake and the
    // alarm), `agentLayer` provides `Harness` and `Session`, the instance provides the engine's state and
    // `RuntimeContext`, the captured init context provides the rest of the agent's requirements and the socket's, and
    // `start` answers the agent's outputs. TypeScript cannot reduce those `Exclude`s over the type parameters.
    Effect.map(
      Effect.map(Effect.context<never>(), Context.omit(Scope.Scope)),
      (captured): HostedAgent<ROut, C, State | RuntimeContext, Socket> => ({
        // SAFETY: `base` holds the captured init context, which provides the agent's remaining requirements (`RIn`);
        // TypeScript cannot reduce those `Exclude`s over the type parameter.
        // @effect-diagnostics-next-line unsafeEffectTypeAssertion:off
        start: Effect.gen(function* () {
          const platform = yield* Effect.context<State | RuntimeContext>();
          const scope = yield* Scope.make();
          const fail = (cause: Cause.Cause<never>) => Scope.close(scope, Exit.failCause(cause));
          const engineContext = yield* Layer.buildWithScope(adapter.layer, scope).pipe(
            Effect.provide(platform),
          );
          const engine = Context.get(engineContext, Engine);
          // An engine that stops the instance without discarding its memory (Rivet's sleep) closes its scope there:
          // the Session ends with the activation, and the next one recovers from storage.
          yield* engine.onStop(Scope.close(scope, Exit.void));
          // The instance's fibers trace into the event in progress (`internal/tracer.ts`).
          const events = EventTracer.make();
          const base = Context.add(
            Context.merge(Context.merge(captured, platform), engineContext),
            Tracer.Tracer,
            events.tracer,
          );
          // The telemetry Alchemy builds for every event (`Cloudflare.Telemetry()`, OTLP destinations), built here for
          // the event that constructs the instance, so the work it resumes is traced there. A build that fails
          // leaves none. Its scope is closed once construction ends, as Alchemy closes an event's (`DurableObjectBridge`
          // `execute`): an OTLP exporter flushes then, and its export loop does not outlive construction. A child of
          // the instance's scope, so a construction that fails closes it too.
          const runtime = Context.get(platform, RuntimeContext);
          const constructingScope = yield* Scope.fork(scope);
          const constructing = yield* Alchemy.Telemetry.buildEventTelemetry(
            Context.merge(captured, platform),
            constructingScope,
            runtime.telemetry,
          );
          events.construct(Context.get(constructing, Tracer.Tracer));
          const instance = yield* Layer.buildWithScope(
            layer.pipe(
              Layer.provideMerge(agentLayer),
              Layer.provideMerge(storage(options.storage)),
            ),
            scope,
          ).pipe(Effect.provide(base), Effect.orDie, Effect.onError(fail));
          const built = Context.merge(base, instance);
          const harness = Context.get(built, Harness);
          const session = Context.get(built, Session);
          const config = options.socket;
          const socket =
            config === undefined
              ? undefined
              : yield* SocketModule.make({
                  config: {
                    ...config,
                    // The call's context goes over the captured one: Alchemy gives each call its own `Scope`, its
                    // object state and its telemetry.
                    accept: (request) =>
                      Effect.contextWith((call: Context.Context<never>) =>
                        Effect.provideContext(
                          config.accept(request),
                          // SAFETY: `captured` is the init context, which holds `RS`: `agent` lists it in its
                          // requirements.
                          Context.merge(captured, call) as Context.Context<RS>,
                        ),
                      ),
                  },
                  harness,
                  session,
                  engine,
                }).pipe(Scope.provide(scope), Effect.provide(built), Effect.onError(fail));
          if (socket !== undefined) yield* socket.restore.pipe(Effect.onError(fail));
          // SAFETY: without a socket there are no connections, so no connection state is ever decoded.
          const noConnections = SocketModule.none as SocketHandle<C>;
          const hostHandlers = Context.get(instance, Host).handlers;
          const handlers: Handlers =
            socket === undefined
              ? hostHandlers
              : {
                  ...hostHandlers,
                  fetch: socket.accept,
                  webSocketMessage: socket.message,
                  webSocketClose: (closed, code, reason, wasClean) =>
                    Effect.andThen(
                      socket.drop(closed),
                      hostHandlers.webSocketClose(closed, code, reason, wasClean),
                    ),
                  webSocketError: (failed, error) =>
                    Effect.andThen(socket.drop(failed), hostHandlers.webSocketError(failed, error)),
                };
          // The engine's native sockets, read as the host's.
          const native: Handlers<Socket> = {
            alarm: handlers.alarm,
            fetch: handlers.fetch,
            webSocketMessage: (raw, message) =>
              handlers.webSocketMessage(adapter.socket(raw), message),
            webSocketClose: (raw, code, reason, wasClean) =>
              handlers.webSocketClose(adapter.socket(raw), code, reason, wasClean),
            webSocketError: (raw, error) => handlers.webSocketError(adapter.socket(raw), error),
          };
          // The application's code runs after `start`, in Alchemy's constructor, whose scope closes with the
          // construction's exit: a construction that fails there closes the instance's scope too, so its agent stops
          // instead of running beside the next instance.
          yield* Effect.addFinalizer((exit) =>
            Exit.isFailure(exit) ? Scope.close(scope, exit) : Effect.void,
          );
          yield* engine.waitUntil(Scope.close(constructingScope, Exit.void));
          const started: Instance<C, ROut, Socket> = {
            harness,
            session,
            socket:
              config === undefined || socket === undefined
                ? noConnections
                : SocketModule.typed(config.connection)(socket.handle),
            abort: engine.abort,
            // The agent Layer's outputs only: no `Scope`, tracer or event context that would replace a call's.
            context: instance,
            methods: (methods) =>
              // SAFETY: the record is the host's handlers, then `methods`, then the host's `alarm`, which is
              // `Served<M, Socket>`; TypeScript cannot reduce the spread over a type parameter.
              EventTracer.tracedMembers(
                { ...native, ...methods, alarm: native.alarm },
                events,
              ) as never,
          };
          return started;
        }).pipe(Effect.withSpan("durable.host.instance")) as HostedAgent<
          ROut,
          C,
          State | RuntimeContext,
          Socket
        >["start"],
      }),
    ) as never;

/** The conversation a view request names: `?conversation=<id>`, the root when absent. */
const viewConversation = (url: string): ConversationId | undefined => {
  const raw = new URL(url, "http://object").searchParams.get("conversation");
  if (raw === null) return ROOT_CONVERSATION_ID;
  const id = Number(raw);
  return Number.isSafeInteger(id) && id > 0 ? ConversationId.make(id) : undefined;
};

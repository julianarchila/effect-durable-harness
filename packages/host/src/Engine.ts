/**
 * What a Durable Object engine gives the host: the instance's SQLite, its single alarm, its lifecycle and its
 * WebSockets. The host (`Host.ts`) is written against this service only; each engine package builds it from its own
 * object state (`@effect-durable-harness/cloudflare` from `Cloudflare.DurableObjectState`,
 * `@effect-durable-harness/rivet` from `Rivet.DurableObjectState`) and binds the host to it with `Host.bindAgent`.
 *
 * `sql` is the storage, `alarm` and `maxKeepAliveMs` are the timer and wake-up for a process that sleeps,
 * `keepAwake` holds an instance in memory while work is active, and `waitUntil`, `abort` and `onStop` are the process
 * lifecycle. The sockets carry `viewState` / `watch` to clients.
 *
 * @since 0.0.0
 */
import type { StorageFailure } from "@effect-durable-harness/core/AgentStorage";
import * as Context from "effect/Context";
import * as Data from "effect/Data";
import type * as Effect from "effect/Effect";
import type * as Layer from "effect/Layer";
import type * as Option from "effect/Option";
import type * as Schema from "effect/Schema";
import type { HttpServerRequest } from "effect/http/HttpServerRequest";
import type * as HttpServerResponse from "effect/http/HttpServerResponse";
import type * as SqlClient from "effect/sql/SqlClient";

/**
 * A socket's attachment could not be read or written: it is missing, does not decode, or is over the engine's size
 * limit (2 KiB on both engines).
 *
 * @category errors
 * @since 0.0.0
 */
export class AttachmentError extends Data.TaggedError("AttachmentError")<{
  readonly message: string;
  readonly cause?: unknown;
}> {}

/**
 * One accepted WebSocket of the instance, as the engine hands it to the host. The engine may hand each event a new
 * object for the same socket: the host keys connections by what it keeps in the attachment, never by the object.
 *
 * @category models
 * @since 0.0.0
 */
export interface EngineSocket {
  /** Sends a text frame; a no-op once the socket closed. */
  readonly send: (data: string) => Effect.Effect<void>;
  /** Closes the socket; a no-op once it closed. */
  readonly close: (code: number, reason: string) => Effect.Effect<void>;
  /** Reads and decodes the attachment, which outlives the instance hibernating. */
  readonly getAttachment: <S extends Schema.Constraint>(
    schema: S,
  ) => Effect.Effect<S["Type"], AttachmentError, S["DecodingServices"]>;
  /** Encodes and keeps the attachment; fails over the engine's size limit. */
  readonly setAttachment: <S extends Schema.Constraint>(
    schema: S,
    value: S["Type"],
  ) => Effect.Effect<void, AttachmentError, S["EncodingServices"]>;
}

/**
 * The instance's single alarm, in epoch milliseconds. The host is its only writer.
 *
 * @category models
 * @since 0.0.0
 */
export interface EngineAlarm {
  /** The alarm that is set, if any. */
  readonly get: Effect.Effect<Option.Option<number>, StorageFailure>;
  /** Sets the alarm, replacing any; durable when it returns. */
  readonly set: (at: number) => Effect.Effect<void, StorageFailure>;
  /** Deletes the alarm. */
  readonly delete: Effect.Effect<void, StorageFailure>;
}

/**
 * The engine of one object instance.
 *
 * @category services
 * @since 0.0.0
 */
export class Engine extends Context.Service<
  Engine,
  {
    /**
     * The longest the host may leave between two keep-alive alarms while work is active. An engine that stops an
     * instance with no inbound events (Rivet, after 30 s) needs the alarm to come sooner; `Infinity` otherwise.
     */
    readonly maxKeepAliveMs: number;
    /** The instance's SQLite. A nested transaction is a savepoint. */
    readonly sql: SqlClient.SqlClient;
    /** The instance's single alarm. */
    readonly alarm: EngineAlarm;
    /**
     * Runs until interrupted, and keeps the instance in memory meanwhile, between events. The host runs it while the
     * kernel reports active work: a wait in memory with no I/O pending would otherwise be lost when the engine
     * hibernates an instance that has no event (Cloudflare, about 10 s after the last one). The keep-alive alarm
     * covers what an event covers, so an engine that stops instances only for want of events (Rivet) needs nothing
     * more here.
     */
    readonly keepAwake: Effect.Effect<never>;
    /** Keeps the instance alive until `effect` settles, after the current event returned. */
    readonly waitUntil: (effect: Effect.Effect<void>) => Effect.Effect<void>;
    /**
     * Resets the instance: the next event constructs a fresh one, which recovers from the last commit. What the
     * instance runs in memory ends.
     */
    readonly abort: (reason: string) => Effect.Effect<void>;
    /**
     * Runs `effect` when the engine stops the instance without discarding its memory (Rivet's sleep): the host closes
     * the instance's scope there, so no Session outlives its activation. A no-op on an engine that evicts the whole
     * isolate (Cloudflare).
     */
    readonly onStop: (effect: Effect.Effect<void>) => Effect.Effect<void>;
    /**
     * Accepts the WebSocket upgrade the current request is: the response to return from `fetch`, and the socket.
     * Only valid inside a `fetch` whose request is an upgrade.
     */
    readonly upgrade: Effect.Effect<
      readonly [HttpServerResponse.HttpServerResponse, EngineSocket],
      never,
      HttpServerRequest
    >;
    /**
     * The instance's open sockets, those accepted by an instance before a hibernation included. A socket that is
     * closing or closed is not listed: the engine filters what its runtime still reports.
     */
    readonly sockets: Effect.Effect<ReadonlyArray<EngineSocket>>;
  }
>()("@effect-durable-harness/host/Engine") {}

/**
 * How an engine package binds the host: the Layer that builds the `Engine` from the object's state (`State`, the
 * engine's own state service), and how to read the engine's native socket (`Socket`), which the object's
 * `webSocket*` handlers receive.
 *
 * @category models
 * @since 0.0.0
 */
export interface EngineAdapter<State, Socket> {
  /** Builds the instance's `Engine`, once per instance. */
  readonly layer: Layer.Layer<Engine, never, State>;
  /** The engine socket of a native one. */
  readonly socket: (native: Socket) => EngineSocket;
}

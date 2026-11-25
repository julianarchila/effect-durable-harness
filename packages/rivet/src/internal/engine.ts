/**
 * The host's `Engine` over a Rivet Durable Object: the actor's SQLite through `sqlClient.ts`, Alchemy's alarm
 * on RivetKit's scheduler, WebSockets, and the activation's lifecycle (`patches/@alchemy.run%2Frivet@2.0.0-beta.87.patch`).
 *
 * Rivet differs from Cloudflare where the host has to know:
 *
 * - **Sleep.** RivetKit stops an actor after 30 s without inbound events, whatever its fibers do, and keeps the
 *   process: the host closes the instance's scope in `onStop`, and its keep-alive alarm comes every 20 s at most, so
 *   an actor with active work is never idle that long.
 * - **Input gate is opt-in** (`Workers.InputGate`). Without it calls interleave around storage; the host reads what
 *   must be consistent inside a transaction, so it needs no gate, and runs under one when the class takes it.
 * - **Alarm.** Set outside a transaction, the schedule exists when `set` returns. One set inside a transaction is
 *   scheduled after it commits; Alchemy repairs one whose activation stopped in between when the next one starts.
 * - **Sockets do not outlive the activation.** Rivet 3's gateway closes every client socket when the actor stops
 *   (1012, `actor.stopped`) and the client reconnects; an open socket keeps the actor awake. `sockets` lists this
 *   activation's, and the host's restore finds none in a new one.
 */
import { Backend, StorageFailure } from "@effect-durable-harness/core/AgentStorage";
import {
  AttachmentError,
  Engine,
  type EngineAdapter,
  type EngineSocket,
} from "@effect-durable-harness/host/Engine";
import * as Rivet from "@alchemy.run/rivet";
import type { RuntimeContext } from "alchemy";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "./sqlClient.ts";

/** Under RivetKit's 30 s sleep timeout, with room for an alarm that fires late. */
export const MAX_KEEP_ALIVE_MS = 20_000;

/** The most an attachment holds once encoded as JSON, as on Cloudflare (where workerd enforces it). */
const MAX_ATTACHMENT_BYTES = 2048;

/** Alchemy's storage calls have no error channel: a failure is a defect, an unknown outcome here. */
const storageCall = <A>(message: string, effect: Effect.Effect<A>) =>
  effect.pipe(
    Effect.catchDefect((cause) =>
      Effect.fail(new StorageFailure({ reason: new Backend({ message, cause }) })),
    ),
  );

/**
 * Alchemy's Rivet socket keeps an attachment as a plain value, in memory, with no size limit: the host's attachments
 * are Schema-encoded here and held to Cloudflare's 2 KiB, so a connection behaves the same on both engines.
 */
const socket = (native: Rivet.WebSocket): EngineSocket => ({
  send: (data) => native.send(data),
  close: (code, reason) => native.close(code, reason),
  getAttachment: (schema) => {
    const stored = native.deserializeAttachment<unknown>();
    return stored === null
      ? Effect.fail(new AttachmentError({ message: "WebSocket attachment is missing" }))
      : Schema.decodeUnknownEffect(schema)(stored).pipe(
          Effect.mapError(
            (cause) =>
              new AttachmentError({ message: "Could not decode WebSocket attachment", cause }),
          ),
        );
  },
  setAttachment: (schema, value) =>
    Schema.encodeEffect(schema)(value).pipe(
      Effect.mapError(
        (cause) => new AttachmentError({ message: "Could not encode WebSocket attachment", cause }),
      ),
      Effect.flatMap((encoded) =>
        new TextEncoder().encode(JSON.stringify(encoded)).byteLength > MAX_ATTACHMENT_BYTES
          ? Effect.fail(
              new AttachmentError({
                message: `WebSocket attachment exceeds ${MAX_ATTACHMENT_BYTES} bytes`,
              }),
            )
          : Effect.sync(() => native.serializeAttachment(encoded)),
      ),
    ),
});

export const adapter: EngineAdapter<Rivet.DurableObjectState | RuntimeContext, Rivet.WebSocket> = {
  layer: Layer.effect(
    Engine,
    Effect.gen(function* () {
      const state = yield* Rivet.DurableObjectState;
      const storage = state.storage;
      return Engine.of({
        maxKeepAliveMs: MAX_KEEP_ALIVE_MS,
        sql: yield* SqlClient.make(state),
        alarm: {
          get: storageCall("Failed to read the alarm", storage.getAlarm()).pipe(
            Effect.map(Option.fromNullOr),
          ),
          set: (at) => storageCall("Failed to set the alarm", storage.setAlarm(at)),
          delete: storageCall("Failed to delete the alarm", storage.deleteAlarm()),
        },
        // RivetKit puts an actor to sleep only for want of inbound events, and the keep-alive alarm brings one under
        // its timeout while work is active; a timer or a pending promise does not change that.
        keepAwake: Effect.never,
        waitUntil: (effect) => state.waitUntil(effect),
        abort: (reason) => state.abort(reason),
        onStop: (effect) => state.onStop(effect),
        // The host checks the upgrade header before it accepts.
        upgrade: Rivet.upgrade().pipe(
          Effect.map(([response, accepted]) => [response, socket(accepted)] as const),
        ),
        // Alchemy's registry drops a socket as it closes: every listed one is open.
        sockets: Effect.map(state.getWebSockets(), (all) => all.map(socket)),
      });
    }),
  ),
  socket,
};

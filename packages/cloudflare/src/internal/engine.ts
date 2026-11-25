/**
 * The host's `Engine` over a Cloudflare Durable Object: the object's SQLite through `@effect/sql-sqlite-do`, its
 * native alarm, a pending timer to stay awake, `waitUntil` and `abort`, and its hibernatable WebSockets. Cloudflare
 * evicts the whole isolate, so `onStop` has nothing to do, and an object with no events is not stopped while it is
 * busy: no keep-alive limit.
 */
import { Backend, StorageFailure } from "@effect-durable-harness/core/AgentStorage";
import {
  AttachmentError,
  Engine,
  type EngineAdapter,
  type EngineSocket,
} from "@effect-durable-harness/host/Engine";
import * as Cloudflare from "@alchemy.run/cloudflare";
import * as SqliteClient from "@effect/sql-sqlite-do/SqliteClient";
import type { RuntimeContext } from "alchemy";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as SqlClient from "effect/sql/SqlClient";

const OPEN = 1;

/** Any pending timer holds the object; its length only sets how often the holding fiber wakes. */
const KEEP_AWAKE_TIMER_MS = 60_000;

const attempt = <A>(message: string, run: () => Promise<A>) =>
  Effect.tryPromise({
    try: run,
    catch: (cause) => new StorageFailure({ reason: new Backend({ message, cause }) }),
  });

const attachmentError = (error: Cloudflare.WebSocketAttachmentError) =>
  new AttachmentError({ message: error.message, cause: error });

const socket = (native: Cloudflare.WebSocket): EngineSocket => ({
  send: (data) => native.send(data),
  close: (code, reason) => native.close(code, reason),
  getAttachment: (schema) => native.getAttachment(schema).pipe(Effect.mapError(attachmentError)),
  setAttachment: (schema, value) =>
    native.setAttachment(schema, value).pipe(Effect.mapError(attachmentError)),
});

export const adapter: EngineAdapter<
  Cloudflare.DurableObjectState | RuntimeContext,
  Cloudflare.WebSocket
> = {
  layer: Layer.effect(
    Engine,
    Effect.gen(function* () {
      const state = yield* Cloudflare.DurableObjectState;
      // The instance's context: the state's calls are only valid inside the object, with its `RuntimeContext`.
      const platform = yield* Effect.context<Cloudflare.DurableObjectState | RuntimeContext>();
      const storage = state.raw.storage;
      const sql = yield* Layer.build(SqliteClient.layer({ storage }));
      return Engine.of({
        maxKeepAliveMs: Number.POSITIVE_INFINITY,
        sql: Context.get(sql, SqlClient.SqlClient),
        alarm: {
          get: Effect.map(
            attempt("Failed to read the alarm", () => storage.getAlarm()),
            Option.fromNullOr,
          ),
          set: (at) => attempt("Failed to set the alarm", () => storage.setAlarm(at)),
          delete: attempt("Failed to delete the alarm", () => storage.deleteAlarm()),
        },
        // An object with a pending timer does not hibernate; the keep-alive alarm's events keep it from being evicted
        // for inactivity. The loop ends when the host interrupts it, as soon as a wake reports no active work, and at
        // the latest with the instance's scope, which owns its fiber.
        keepAwake: Effect.forever(Effect.sleep(KEEP_AWAKE_TIMER_MS)),
        waitUntil: (effect) => Effect.provide(state.waitUntil(effect), platform),
        abort: (reason) => Effect.provide(state.abort(reason), platform),
        onStop: () => Effect.void,
        upgrade: Cloudflare.upgrade().pipe(
          Effect.map(([response, accepted]) => [response, socket(accepted)] as const),
          Effect.provide(platform),
        ),
        // workerd still lists a socket that is closing: the host is handed the open ones only.
        sockets: Effect.map(state.getWebSockets(), (all) =>
          all.filter((native) => native.ws.readyState === OPEN).map(socket),
        ).pipe(Effect.provide(platform)),
      });
    }),
  ),
  socket,
};

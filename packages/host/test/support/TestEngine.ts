// A Durable Object engine in memory, for the host's tests in Node: one SQLite database, one alarm and the
// accepted sockets, kept in a `World` that outlives instances, so a test starts an instance, drops it (an eviction,
// a reset, a Rivet sleep) and starts another over the same state. The world records what the host did to it.
import * as SqliteClient from "@effect/sql-sqlite-node/SqliteClient";
import {
  AttachmentError,
  Engine,
  type EngineAdapter,
  type EngineSocket,
} from "../../src/Engine.ts";
import { RuntimeContext } from "alchemy";
import { Backend, StorageFailure } from "@effect-durable-harness/core/AgentStorage";
import * as Context from "effect/Context";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as HttpServerResponse from "effect/http/HttpServerResponse";
import * as SqlClient from "effect/sql/SqlClient";

/** One accepted socket: what the host sent it, its attachment, and how it was closed. */
export interface FakeSocket {
  readonly id: number;
  readonly sent: Array<string>;
  attachment: Schema.Json | undefined;
  closed: { readonly code: number; readonly reason: string } | undefined;
}

/** The object's state that outlives its instances. */
export interface World {
  readonly sql: SqlClient.SqlClient;
  alarm: number | undefined;
  /** Every alarm write, with how many tasks, and how many live ones, storage held at that moment. */
  readonly alarmWrites: Array<{
    readonly at: number | undefined;
    readonly storedTasks: number;
    readonly liveTasks: number;
    readonly signalledTasks: number;
  }>;
  /** Whether alarm writes fail, as a storage error of the engine does. */
  failAlarmWrites: boolean;
  /** Holds the next alarm read: `entered` completes when it starts, and it returns once `release` completes. */
  holdAlarmGet: AlarmHold | undefined;
  readonly sockets: Array<FakeSocket>;
  readonly aborts: Array<string>;
  /** Whether the current instance is held in memory (`Engine.keepAwake` runs). */
  awake: boolean;
  /** The stop handlers the current instance registered; `stop` runs and clears them. */
  readonly stops: Array<Effect.Effect<void>>;
  readonly maxKeepAliveMs: number;
}

/** One held alarm read. */
export interface AlarmHold {
  readonly entered: Deferred.Deferred<void>;
  readonly release: Deferred.Deferred<void>;
}

/** The test object's state service, as an engine's object provides it to its instance. */
export class TestObject extends Context.Service<TestObject, World>()(
  "@effect-durable-harness/host/test/support/TestEngine/TestObject",
) {}

/** A world over a fresh in-memory database, in the test's scope. */
export const makeWorld = (options: { readonly maxKeepAliveMs?: number | undefined } = {}) =>
  Effect.gen(function* () {
    const sql = yield* Layer.build(Layer.orDie(SqliteClient.layer({ filename: ":memory:" })));
    const world: World = {
      sql: Context.get(sql, SqlClient.SqlClient),
      alarm: undefined,
      alarmWrites: [],
      failAlarmWrites: false,
      holdAlarmGet: undefined,
      sockets: [],
      aborts: [],
      awake: false,
      stops: [],
      maxKeepAliveMs: options.maxKeepAliveMs ?? Number.POSITIVE_INFINITY,
    };
    return world;
  });

const encodeJson = Schema.encodeUnknownSync(Schema.Json);
const encodeJsonString = Schema.encodeSync(Schema.fromJsonString(Schema.Json));

/** How many tasks `SqlAgentStorage` holds (its default prefix): all, live, and holding a delivered signal. */
const storedTasks = (world: World) =>
  world.sql`SELECT COUNT(*) AS n,
      COUNT(*) FILTER (WHERE status <> 'terminal') AS live,
      COUNT(*) FILTER (WHERE record LIKE '%"core.signal.%') AS signalled
    FROM durable_tasks`.pipe(
    Effect.map((rows) => ({
      storedTasks: Number(rows[0]?.["n"] ?? 0),
      liveTasks: Number(rows[0]?.["live"] ?? 0),
      signalledTasks: Number(rows[0]?.["signalled"] ?? 0),
    })),
    Effect.orDie,
  );

/** Records an alarm write, or fails it while `failAlarmWrites` is set. */
const writeAlarm = (world: World, at: number | undefined) =>
  Effect.suspend(() =>
    world.failAlarmWrites
      ? Effect.fail(
          new StorageFailure({
            reason: new Backend({ message: "alarm write failed", cause: undefined }),
          }),
        )
      : Effect.map(storedTasks(world), (counts) => {
          world.alarm = at;
          world.alarmWrites.push({ at, ...counts });
        }),
  );

/** The alarm, read after the hold the test installed, if any; every read yields, as a real engine's does. */
const readAlarm = (world: World) =>
  Effect.gen(function* () {
    const hold = world.holdAlarmGet;
    world.holdAlarmGet = undefined;
    if (hold !== undefined) {
      yield* Deferred.succeed(hold.entered, undefined);
      yield* Deferred.await(hold.release);
    }
    yield* Effect.yieldNow;
    return Option.fromUndefinedOr(world.alarm);
  });

const engineSocket = (socket: FakeSocket): EngineSocket => ({
  send: (data) =>
    Effect.sync(() => {
      if (socket.closed === undefined) socket.sent.push(data);
    }),
  close: (code, reason) =>
    Effect.sync(() => {
      socket.closed ??= { code, reason };
    }),
  getAttachment: (schema) =>
    socket.attachment === undefined
      ? Effect.fail(new AttachmentError({ message: "WebSocket attachment is missing" }))
      : Schema.decodeUnknownEffect(schema)(socket.attachment).pipe(
          Effect.mapError((cause) => new AttachmentError({ message: "Could not decode", cause })),
        ),
  setAttachment: (schema, value) =>
    Schema.encodeEffect(schema)(value).pipe(
      Effect.mapError((cause) => new AttachmentError({ message: "Could not encode", cause })),
      Effect.flatMap((encoded) => {
        const json = encodeJson(encoded);
        return encodeJsonString(json).length > 2048
          ? Effect.fail(new AttachmentError({ message: "WebSocket attachment exceeds 2048 bytes" }))
          : Effect.sync(() => {
              socket.attachment = json;
            });
      }),
    ),
});

export const adapter: EngineAdapter<TestObject | RuntimeContext, FakeSocket> = {
  layer: Layer.effect(
    Engine,
    Effect.gen(function* () {
      const world = yield* TestObject;
      return Engine.of({
        maxKeepAliveMs: world.maxKeepAliveMs,
        sql: world.sql,
        alarm: {
          get: readAlarm(world),
          set: (at) => writeAlarm(world, at),
          delete: writeAlarm(world, undefined),
        },
        keepAwake: Effect.acquireUseRelease(
          Effect.sync(() => {
            world.awake = true;
          }),
          () => Effect.never,
          () =>
            Effect.sync(() => {
              world.awake = false;
            }),
        ),
        waitUntil: (effect) => Effect.asVoid(Effect.forkDetach(effect)),
        abort: (reason) => Effect.sync(() => void world.aborts.push(reason)),
        onStop: (effect) => Effect.sync(() => void world.stops.push(effect)),
        upgrade: Effect.sync(() => {
          const socket: FakeSocket = {
            id: world.sockets.length,
            sent: [],
            attachment: undefined,
            closed: undefined,
          };
          world.sockets.push(socket);
          return [HttpServerResponse.empty({ status: 101 }), engineSocket(socket)] as const;
        }),
        sockets: Effect.sync(() =>
          world.sockets.filter((socket) => socket.closed === undefined).map(engineSocket),
        ),
      });
    }),
  ),
  socket: engineSocket,
};

/** Runs the stop handlers the current instance registered, as Rivet's sleep does. */
export const stop = (world: World) =>
  Effect.forEach(world.stops.splice(0), (handler) => handler, { discard: true });

/** The `RuntimeContext` an engine's object gives its instance; the host reads only its telemetry. */
export const runtimeContext = Layer.succeed(RuntimeContext, {
  Type: "Test",
  id: "test",
  env: {},
  get: () => Effect.undefined,
  set: (id) => Effect.succeed(id),
});

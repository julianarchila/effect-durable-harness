// Helpers of the Rivet suite: calls to the probes through the API on the swarm, the gateway for the session socket,
// and bounded polling.
// @effect-diagnostics nodeBuiltinImport:off
import { readdirSync, readFileSync, statSync } from "node:fs";
import * as Data from "effect/Data";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Schedule from "effect/Schedule";
import * as Schema from "effect/Schema";
import * as HttpClient from "effect/http/HttpClient";
import * as HttpClientRequest from "effect/http/HttpClientRequest";
import * as HttpClientResponse from "effect/http/HttpClientResponse";
import { decodeServerEvent, type ServerEvent } from "@effect-durable-harness/host/SessionProtocol";
import * as Result from "effect/Result";
import type { TaskView } from "./fixture/Probe.ts";

/** Which stack the suite runs on: the local swarm, or Rivet Cloud with nodes on Fly (`RIVET_TARGET=cloud`). */
export const cloud = process.env["RIVET_TARGET"] === "cloud";
const STACK = cloud ? "DurableHarnessRivetCloud" : "DurableHarnessRivet";

let objects = 0;
/** A fresh actor name per test, so every test starts from an empty Session. */
export const objectName = (label: string) => `${label}-${process.pid}-${objects++}`;

/** What `Probe.state` answers. */
export interface ProbeState {
  readonly instance: string;
  readonly instanceStartedAt: number;
  readonly now: number;
  readonly alarm: number | null;
  readonly nextWakeAt: number | null;
  readonly activeWork: boolean;
  readonly sockets: number;
  /** The Fly Machine that runs the activation; empty off Fly. */
  readonly node: string;
}

const base = (url: string) => url.replace(/\/+$/, "");

const post = <A>(target: string, args: ReadonlyArray<unknown>) =>
  Effect.gen(function* () {
    const client = yield* HttpClient.HttpClient;
    const request = HttpClientRequest.post(target).pipe(HttpClientRequest.bodyJsonUnsafe(args));
    const response = yield* client.execute(request).pipe(
      Effect.flatMap(HttpClientResponse.filterStatusOk),
      // A fresh pool can take a few seconds to register with the control plane.
      Effect.retry({ schedule: Schedule.spaced("1 second"), times: 30 }),
    );
    // SAFETY: the API answers the method's JSON result.
    return (yield* response.json) as A;
  }).pipe(Effect.orDie);

/** Calls one `Probe` method of the actor `name`. */
export const call = <A>(
  url: string,
  name: string,
  method: string,
  ...args: ReadonlyArray<unknown>
) => post<A>(`${base(url)}/probe/${name}/${method}`, args);

/** Runs one conformance case in the actor `name`. */
export const conformance = (url: string, name: string, index: number) =>
  post<{ readonly ok: boolean; readonly failure?: string }>(
    `${base(url)}/storage/${name}/${index}`,
    [],
  );

export const state = (url: string, name: string) => call<ProbeState>(url, name, "state");
export const task = (url: string, name: string, id: number) =>
  call<TaskView | null>(url, name, "task", id);

class NotYet extends Data.TaggedError("NotYet")<{}> {}

/** Polls `read` until `done` holds, at most `times` times. */
export const eventually = <A, E, R>(
  read: Effect.Effect<A, E, R>,
  done: (value: A) => boolean,
  options: { readonly spacing?: Duration.Input; readonly times?: number } = {},
) =>
  read.pipe(
    Effect.filterOrFail(done, () => new NotYet()),
    Effect.retry({
      schedule: Schedule.spaced(options.spacing ?? "500 millis"),
      times: options.times ?? 120,
      while: (error) => error instanceof NotYet,
    }),
    Effect.orDie,
  );

export const settled = (url: string, name: string, id: number) =>
  eventually(task(url, name, id), (view) => view?.status === "terminal", { times: 240 });

export const idle = (duration: Duration.Input) => Effect.sleep(duration);

/** The newest deployed stage's state directory of the suite's stack. */
const stageDirectory = () => {
  const root = new URL(`../.alchemy/state/${STACK}/`, import.meta.url);
  const stages = readdirSync(root)
    .map((stage) => ({ stage, at: statSync(new URL(`${stage}/Probe.json`, root)).mtimeMs }))
    .toSorted((a, b) => b.at - a.at);
  const stage = stages[0]?.stage;
  if (stage === undefined) throw new Error(`no deployed ${STACK} stage`);
  return new URL(`${stage}/`, root);
};

/**
 * The control plane as the tests reach it, from the Probe's Alchemy state: its endpoint, namespace, pool and admin
 * token. The token stays in memory; nothing prints it.
 */
const gateway = () => {
  const attr = Schema.decodeUnknownSync(
    Schema.fromJsonString(
      Schema.Struct({
        attr: Schema.Struct({
          endpoint: Schema.String,
          namespace: Schema.String,
          pool: Schema.String,
          token: Schema.Struct({ __redacted__: Schema.String }),
        }),
      }),
    ),
  )(readFileSync(new URL("Probe.json", stageDirectory()), "utf8"));
  return {
    // The swarm's engine is published on the host; the recorded endpoint is its address inside the swarm.
    engine: process.env["RIVET_ENGINE"] ?? attr.attr.endpoint,
    namespace: attr.attr.namespace,
    pool: attr.attr.pool,
    token: attr.attr.token.__redacted__,
  };
};

/** The Fly app of the pool's nodes, from its Alchemy state (Rivet Cloud target only). */
export const flyApp = () =>
  Schema.decodeUnknownSync(
    Schema.fromJsonString(Schema.Struct({ attr: Schema.Struct({ appName: Schema.String }) })),
  )(readFileSync(new URL("DurableHarnessFlyNodes.json", stageDirectory()), "utf8")).attr.appName;

/**
 * Stops or starts one Machine of the pool through Fly's Machines API, with `FLY_API_TOKEN` from the environment
 * (`doppler run`). The token is only sent, never printed.
 */
export const flyMachine = (action: "stop" | "kill" | "start", machineId: string) =>
  Effect.gen(function* () {
    const client = yield* HttpClient.HttpClient;
    const token = process.env["FLY_API_TOKEN"] ?? "";
    // `kill` is a stop with SIGKILL and no grace: the node dies without draining its actors, as in a crash.
    const request = HttpClientRequest.post(
      `https://api.machines.dev/v1/apps/${flyApp()}/machines/${machineId}/${action === "start" ? "start" : "stop"}`,
    ).pipe(
      HttpClientRequest.bearerToken(token),
      action === "kill"
        ? HttpClientRequest.bodyJsonUnsafe({ signal: "SIGKILL", timeout: "1s" })
        : (request) => request,
    );
    yield* client.execute(request).pipe(
      Effect.flatMap(HttpClientResponse.filterStatusOk),
      // The Machines API can time out on connect; a stop or start is safe to repeat.
      Effect.retry({ schedule: Schedule.spaced("2 seconds"), times: 5 }),
    );
  }).pipe(Effect.orDie);

/** The gateway URL of the session socket of the actor `name`, as a client outside the swarm opens it. */
export const socketUrl = (name: string, username: string) => {
  const target = gateway();
  const params = new URLSearchParams({
    username,
    "rvt-namespace": target.namespace,
    "rvt-method": "getOrCreate",
    "rvt-runner": target.pool,
    "rvt-key": name,
    "rvt-crash-policy": "sleep",
    "rvt-token": target.token,
  });
  return `${target.engine.replace(/^http/, "ws")}/gateway/Probe/websocket/?${params}`;
};

/** A session socket client that records every event it receives. */
export interface Recorder {
  readonly events: ReadonlyArray<ServerEvent>;
  readonly closed: () => { readonly code: number; readonly reason: string } | undefined;
  readonly close: () => void;
}

/** Opens the session socket of `name` and records its events. */
export const openSocket = (name: string, username: string) =>
  Effect.callback<Recorder>((resume) => {
    const events: Array<ServerEvent> = [];
    let closed: { readonly code: number; readonly reason: string } | undefined;
    const socket = new WebSocket(socketUrl(name, username));
    socket.addEventListener("open", () =>
      resume(
        Effect.succeed({ events, closed: () => closed, close: () => socket.close(1000, "done") }),
      ),
    );
    socket.addEventListener("message", (event) => {
      const decoded = decodeServerEvent(String(event.data));
      if (Result.isSuccess(decoded)) events.push(decoded.success);
    });
    socket.addEventListener("close", (event) => {
      closed = { code: event.code, reason: event.reason };
    });
    socket.addEventListener("error", () =>
      resume(Effect.die(new Error("the session socket failed to open"))),
    );
  });

export const millis = (ms: number) => Duration.millis(ms);

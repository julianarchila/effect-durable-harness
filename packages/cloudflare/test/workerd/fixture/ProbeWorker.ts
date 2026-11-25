import { ConversationId, EntryId, SubmissionId } from "@effect-durable-harness/core/Ids";
import type { RuntimeContext } from "alchemy";
import * as Cloudflare from "@alchemy.run/cloudflare";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { HttpServerRequest } from "effect/http/HttpServerRequest";
import * as HttpServerResponse from "effect/http/HttpServerResponse";
import { compatibility } from "../../../src/Compatibility.ts";
import { AgentProbe, AgentProbeLive, KeepAliveProbe } from "./AgentProbe.ts";
import Probe from "./Probe.ts";
import {
  ReadOnlyProbe,
  ReadOnlyProbeLive,
  SessionProbe,
  SessionProbeLive,
} from "./SessionProbe.ts";
import * as RecordedSpans from "./RecordedSpans.ts";
import StorageConformance from "./StorageConformance.ts";

const probeRoute = /^\/probe\/([a-zA-Z0-9_-]+)\/(commit|rollback)\/(\d+)$/;
const storageRoute = /^\/storage\/([a-zA-Z0-9_-]+)\/(\d+)$/;
const agentRoute = /^\/agent\/([a-zA-Z0-9_-]+)\/([a-zA-Z]+)$/;
const keepAliveRoute =
  /^\/keepalive\/([a-zA-Z0-9_-]+)\/(startTask|task|state|submit|transcript|partialLength|startHold|holdResult|view)$/;
const sessionRoute = /^\/(session|readonly)\/([a-zA-Z0-9_-]+)$/;
const sessionCallRoute = /^\/(session|readonly)\/([a-zA-Z0-9_-]+)\/([a-zA-Z]+)$/;

const Args = Schema.Array(Schema.Json);

/**
 * - `POST /probe/:name/(commit|rollback)/:rows` answers the row count after the write.
 * - `POST /storage/:name/:case` runs one `AgentStorage` conformance case in the object `:name`.
 * - `POST /agent/:name/:method` calls one `AgentProbe` method with the JSON array body as arguments; a call that
 *   resets the object answers `{ "reset": true }`.
 * - `GET /agent/:name/view?conversation=<id>` upgrades to the conversation's view socket.
 * - `POST /keepalive/:name/(startTask|task|state|submit|transcript|partialLength|startHold|holdResult)` calls the same methods on `KeepAliveProbe`;
 *   `GET /keepalive/:name/view` opens its view socket.
 * - `GET /(session|readonly)/:name?username=` upgrades to the session socket of `SessionProbe` or `ReadOnlyProbe`.
 * - `POST /(session|readonly)/:name/:method` calls one of their methods (`AgentRpc` and the test's own).
 */
export default class ProbeWorker extends Cloudflare.Worker<ProbeWorker>()(
  "ProbeWorker",
  { main: import.meta.url, compatibility: compatibility() },
  Effect.gen(function* () {
    const probes = yield* Probe;
    const conformance = yield* StorageConformance;
    const agents = yield* AgentProbe;
    const keepAlive = yield* KeepAliveProbe;
    const sessions = yield* SessionProbe;
    const readOnly = yield* ReadOnlyProbe;
    const callAgent = (name: string, method: string, args: ReadonlyArray<Schema.Json>) => {
      const stub = agents.getByName(name);
      const [first, second] = args;
      switch (method) {
        case "state":
          return stub.state();
        case "startTask":
          return stub.startTask(
            first === "probe.sleep" ? "probe.sleep" : "probe.busy",
            second ?? null,
          );
        case "task":
          return stub.task(Number(first));
        case "recordedSpans":
          return stub.recordedSpans();
        case "submit":
          return stub.submit(String(first), String(second));
        case "submitImage":
          return stub.submitImage(Number(first), String(second));
        case "transcript":
          return stub.transcript();
        case "crash":
          return stub.crash(first === true);
        case "orphanTask":
          return stub.orphanTask(Schema.decodeUnknownSync(OrphanInput)(first));
        case "startSocket":
          return stub.startSocket(String(first), Number(second));
        case "outcome":
          return stub.outcome(Number(first));
        case "poison":
          return stub.poison();
        case "heal":
          return stub.heal();
        case "isolate":
          return stub.isolate();
        case "startKv":
          return stub.startKv(String(first), String(second));
        case "holdNote":
          return stub.holdNote(String(first), Number(second));
        case "note":
          return stub.note();
        default:
          return Effect.die(new Error(`Unknown method ${method}`));
      }
    };
    const callKeepAlive = (
      name: string,
      method: string,
      args: ReadonlyArray<Schema.Json>,
    ): Effect.Effect<unknown> => {
      const stub = keepAlive.getByName(name);
      const [first, second] = args;
      switch (method) {
        case "startTask":
          return stub.startTask("probe.busy", second ?? null);
        case "task":
          return stub.task(Number(first));
        case "submit":
          return stub.submit(String(first), String(second));
        case "transcript":
          return stub.transcript();
        case "partialLength":
          return stub.partialLength();
        case "startHold":
          return stub.startHold(Number(first));
        case "holdResult":
          return stub.holdResult(Number(first));
        default:
          return stub.state();
      }
    };
    const sessionStub = (kind: string, name: string) =>
      kind === "readonly" ? readOnly.getByName(name) : sessions.getByName(name);
    /** An `AgentRpc` or test method; a typed failure answers its tag and whether it arrived as its class. */
    const callSession = (
      kind: string,
      name: string,
      method: string,
      args: ReadonlyArray<Schema.Json>,
    ): Effect.Effect<unknown, never, RuntimeContext> => {
      const stub = sessionStub(kind, name);
      const [first, second, third] = args;
      const conversationId = ConversationId.make(Number(first));
      const call: Effect.Effect<unknown, TaggedFailure, RuntimeContext> = (() => {
        switch (method) {
          case "kick":
            return stub.kick(String(first));
          case "connections":
            return stub.connections();
          case "notify":
            return stub.notify(Number(first), String(second));
          case "crash":
            return stub.crash();
          case "ensureRoot":
            return stub.ensureRoot();
          case "conversations":
            return stub.conversations();
          case "createConversation":
            return stub.createConversation(Schema.decodeUnknownSync(RequestOptions)(first ?? {}));
          case "fork":
            return stub.fork(
              conversationId,
              EntryId.make(Number(second)),
              Schema.decodeUnknownSync(RequestOptions)(third ?? {}),
            );
          case "submit":
            return stub.submit(conversationId, {
              type: "input",
              content: String(second),
              requestId: Schema.decodeUnknownSync(RequestOptions)(third ?? {}).requestId,
            });
          case "awaitSubmission":
            return stub.awaitSubmission(SubmissionId.make(Number(first)));
          case "withdraw":
            return stub.withdraw(
              SubmissionId.make(Number(first)),
              ConversationId.make(Number(second)),
            );
          case "abortConversation":
            return stub.abortConversation(conversationId);
          default:
            return Effect.die(new Error(`Unknown method ${method}`));
        }
      })();
      return Effect.catch(call, (error) =>
        Effect.succeed({ failed: error._tag, revived: error instanceof Error }),
      );
    };
    return {
      fetch: Effect.gen(function* () {
        const request = yield* HttpServerRequest;
        const path = request.url.split("?")[0] ?? "";
        const socket = sessionRoute.exec(path);
        if (socket !== null) {
          const [, kind = "", name = ""] = socket;
          return yield* sessionStub(kind, name).fetch(request);
        }
        const session = sessionCallRoute.exec(path);
        if (session !== null) {
          const [, kind = "", name = "", method = ""] = session;
          const args = yield* request.json.pipe(
            Effect.flatMap(Schema.decodeUnknownEffect(Args)),
            Effect.orDie,
          );
          const result = yield* callSession(kind, name, method, args).pipe(
            Effect.catchCause(() => Effect.succeed({ reset: true })),
          );
          return yield* HttpServerResponse.json(result ?? null);
        }
        const agent = agentRoute.exec(path);
        if (agent !== null) {
          const [, name = "", method = ""] = agent;
          if (method === "view") return yield* agents.getByName(name).fetch(request);
          const args = yield* request.json.pipe(
            Effect.flatMap(Schema.decodeUnknownEffect(Args)),
            Effect.orDie,
          );
          const result = yield* callAgent(name, method, args).pipe(
            Effect.catchCause(() => Effect.succeed({ reset: true })),
          );
          return yield* HttpServerResponse.json(result ?? null);
        }
        const long = keepAliveRoute.exec(path);
        if (long !== null) {
          const [, name = "", method = ""] = long;
          if (method === "view") return yield* keepAlive.getByName(name).fetch(request);
          const args = yield* request.json.pipe(
            Effect.flatMap(Schema.decodeUnknownEffect(Args)),
            Effect.orDie,
          );
          const result = yield* callKeepAlive(name, method, args).pipe(
            Effect.catchCause(() => Effect.succeed({ reset: true })),
          );
          return yield* HttpServerResponse.json(result ?? null);
        }
        const storage = storageRoute.exec(path);
        if (storage !== null) {
          const [, name = "", index = "0"] = storage;
          return yield* HttpServerResponse.json(
            yield* conformance.getByName(name).run(Number(index)),
          );
        }
        const match = probeRoute.exec(path);
        if (match === null) return HttpServerResponse.text("ok");
        const [, name = "", action, rows = "0"] = match;
        const probe = probes.getByName(name);
        const count = yield* action === "commit"
          ? probe.commit(Number(rows))
          : probe.rollback(Number(rows));
        return yield* HttpServerResponse.json({ rows: count });
      }),
    };
  }).pipe(
    // The objects' spans go through the host's event tracer to Cloudflare's; on local workerd, without the
    // tracing API, they stay Effect-local, and the suite checks that tracing changes nothing else. Every
    // event's tracer is wrapped to record what it ended (`RecordedSpans`), registered after Cloudflare's.
    Effect.provide(RecordedSpans.layer),
    Effect.provide([AgentProbeLive, SessionProbeLive, ReadOnlyProbeLive, Cloudflare.Telemetry()]),
  ),
) {}

const OrphanInput = Schema.Struct({ chunks: Schema.Number, chunkMs: Schema.Number });
const RequestOptions = Schema.Struct({ requestId: Schema.optional(Schema.String) });

/** A typed failure of a method, as the stub surfaces it. */
interface TaggedFailure {
  readonly _tag: string;
}

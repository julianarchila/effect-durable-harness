import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as HttpServerRequest from "effect/http/HttpServerRequest";
import * as HttpServerResponse from "effect/http/HttpServerResponse";
import { Probe } from "./Probe.ts";
import { StorageConformance } from "./StorageConformance.ts";

const probeRoute = /^\/probe\/([a-zA-Z0-9_-]+)\/([a-zA-Z]+)$/;
const storageRoute = /^\/storage\/([a-zA-Z0-9_-]+)\/(\d+)$/;
const Args = Schema.Array(Schema.Json);

/**
 * The tests' caller, as an application's API would be: it reaches the objects through the control plane's gateway
 * with their typed stubs. Each target runs it as a local Docker service (`swarm/Api.ts`, `cloud/Api.ts`).
 *
 * - `POST /probe/:name/:method` calls one `Probe` method with the JSON array body as arguments.
 * - `POST /storage/:name/:case` runs one `AgentStorage` conformance case in the actor `:name`.
 */
export const api = Effect.gen(function* () {
  const probes = yield* Probe;
  const conformance = yield* StorageConformance;
  const callProbe = (name: string, method: string, args: ReadonlyArray<Schema.Json>) => {
    const stub = probes.getByName(name);
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
      case "startHold":
        return stub.startHold(Number(first));
      case "holdResult":
        return stub.holdResult(Number(first));
      case "submit":
        return stub.submit(String(first), String(second));
      case "transcript":
        return stub.transcript();
      case "crash":
        return stub.crash();
      case "orphanTask":
        return stub.orphanTask(Number(first), Number(second));
      case "broadcast":
        return stub.broadcast(String(first));
      case "bench":
        return stub.bench(Number(first), Number(second), Number(args[2]));
      default:
        return Effect.succeed({ error: `unknown method ${method}` });
    }
  };
  return {
    fetch: Effect.gen(function* () {
      const request = yield* HttpServerRequest.HttpServerRequest;
      const path = new URL(request.url, "http://api").pathname;
      const storage = storageRoute.exec(path);
      if (storage !== null) {
        const outcome = yield* conformance.getByName(storage[1] ?? "").run(Number(storage[2]));
        return yield* HttpServerResponse.json(outcome);
      }
      const probe = probeRoute.exec(path);
      if (probe === null) return HttpServerResponse.empty({ status: 404 });
      const args = yield* Effect.flatMap(request.json, Schema.decodeUnknownEffect(Args));
      const result = yield* callProbe(probe[1] ?? "", probe[2] ?? "", args);
      return yield* HttpServerResponse.json(result ?? null);
    }).pipe(
      Effect.catchCause((cause) =>
        Effect.succeed(HttpServerResponse.text(String(cause), { status: 502 })),
      ),
    ),
  };
});

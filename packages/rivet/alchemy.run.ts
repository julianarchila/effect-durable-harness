import * as Docker from "@alchemy.run/docker";
import * as Rivet from "@alchemy.run/rivet";
import * as Alchemy from "alchemy";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import Api, { API_PORT } from "./test/fixture/swarm/Api.ts";
import { ProbeLive, StorageConformanceLive } from "./test/fixture/swarm/objects.ts";

/**
 * The Rivet suite's stack on a local Docker swarm: a Rivet control plane, one pool running the probes, and the API in
 * front. `bun run test:rivet` deploys and destroys it.
 */
export default Alchemy.Stack(
  "DurableHarnessRivet",
  {
    providers: Layer.mergeAll(Docker.providers(), Rivet.providers()),
    state: Alchemy.localState(),
  },
  Effect.gen(function* () {
    const api = yield* Api;
    return { url: `http://localhost:${API_PORT}`, service: api.name };
    // The two classes are independent; the Api requires both.
    // @effect-diagnostics-next-line layerMergeAllWithDependencies:off
  }).pipe(Effect.provide(Layer.mergeAll(ProbeLive, StorageConformanceLive))),
);

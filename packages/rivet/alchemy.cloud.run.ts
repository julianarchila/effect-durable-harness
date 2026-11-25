import * as Docker from "@alchemy.run/docker";
import * as Fly from "@alchemy.run/fly";
import * as Rivet from "@alchemy.run/rivet";
import * as Alchemy from "alchemy";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import Api, { API_PORT } from "./test/fixture/cloud/Api.ts";
import { ProbeLive, StorageConformanceLive } from "./test/fixture/cloud/objects.ts";

/**
 * The Rivet suite's stack on Rivet Cloud: the cluster in `RIVET_CLOUD_CLUSTER` as the control plane, two nodes on
 * Fly (`iad`) running the probes, and the API as a local Docker service. Credentials come from the environment
 * (`doppler run`: `RIVET_CLOUD_TOKEN`, `RIVET_CLOUD_CLUSTER`, `FLY_API_TOKEN`). `bun run test:rivet:cloud` deploys and
 * destroys it.
 */
export default Alchemy.Stack(
  "DurableHarnessRivetCloud",
  {
    providers: Layer.mergeAll(Docker.providers(), Fly.providers(), Rivet.providers()),
    state: Alchemy.localState(),
  },
  Effect.gen(function* () {
    const api = yield* Api;
    return { url: `http://localhost:${API_PORT}`, service: api.name };
    // The two classes are independent; the Api requires both.
    // @effect-diagnostics-next-line layerMergeAllWithDependencies:off
  }).pipe(Effect.provide(Layer.mergeAll(ProbeLive, StorageConformanceLive))),
);

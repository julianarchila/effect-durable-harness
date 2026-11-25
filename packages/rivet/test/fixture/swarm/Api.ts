import * as Docker from "@alchemy.run/docker";
import * as Rivet from "@alchemy.run/rivet";
import * as Effect from "effect/Effect";
import { api } from "../Api.ts";
import { Cluster } from "./cluster.ts";

/** Where the tests reach the API from the host. */
export const API_PORT = 3033;

/** The API on the swarm, on the control plane's network. */
export default class Api extends Docker.Service<Api>()(
  "DurableHarnessApi",
  Effect.gen(function* () {
    const cluster = yield* Rivet.Cluster;
    return {
      main: import.meta.url,
      port: 3000,
      ports: [{ external: API_PORT, internal: 3000, mode: "host" as const }],
      networks: cluster.network?.kind === "docker" ? [{ name: cluster.network.name }] : [],
    };
  }).pipe(Effect.provide(Cluster)),
  api,
) {}

import * as RivetDocker from "@alchemy.run/rivet/Docker";
import * as Layer from "effect/Layer";

/** A local Rivet 3 control plane on the Docker swarm (`docker swarm init`), and one pool of nodes. */
export const Cluster = RivetDocker.SwarmControlPlane("DurableHarnessRivet");

export const Pool = RivetDocker.SwarmPool("DurableHarnessPool").pipe(Layer.provide(Cluster));

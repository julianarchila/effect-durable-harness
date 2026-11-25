import * as RivetCloud from "@alchemy.run/rivet/Cloud";
import * as RivetFly from "@alchemy.run/rivet/Fly";
import * as Layer from "effect/Layer";

/**
 * Rivet Cloud's control plane (the cluster in `RIVET_CLOUD_CLUSTER`, `us-east-1`), and a pool of two nodes on Fly in
 * `iad` (Virginia, next to it): two, so stopping the node that runs an actor shows it move to the other.
 */
export const Cluster = RivetCloud.ControlPlane("DurableHarnessCloud");

export const Pool = RivetFly.MachinePool("DurableHarnessFly", {
  replicas: 2,
  region: "iad",
  guest: { cpus: 1, memoryMb: 1024 },
}).pipe(Layer.provide(Cluster));

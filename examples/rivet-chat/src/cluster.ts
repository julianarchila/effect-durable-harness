/**
 * Where the sessions run: Rivet Cloud's control plane (the cluster of the `RivetCloud` profile) and one node on Fly
 * in `iad`, next to the cluster's `us-east-1`. Moving them to another cloud changes this file only.
 */
import * as RivetCloud from "@alchemy.run/rivet/Cloud";
import * as RivetFly from "@alchemy.run/rivet/Fly";
import * as Layer from "effect/Layer";

export const Cluster = RivetCloud.ControlPlane("DurableChat");

export const Pool = RivetFly.MachinePool("DurableChatNodes", {
  replicas: 1,
  region: "iad",
  guest: { cpus: 1, memoryMb: 1024 },
}).pipe(Layer.provide(Cluster));

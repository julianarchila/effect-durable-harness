import { probeObjects } from "../objects.ts";
import { Pool } from "./cluster.ts";

/** The probes on the local swarm's pool. */
export const { ProbeLive, StorageConformanceLive } = probeObjects(import.meta.url, Pool);

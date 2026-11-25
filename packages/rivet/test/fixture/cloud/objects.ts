import { probeObjects } from "../objects.ts";
import { Pool } from "./cluster.ts";

/** The probes on the Fly pool, on Rivet Cloud. */
export const { ProbeLive, StorageConformanceLive } = probeObjects(import.meta.url, Pool);

/**
 * The probe classes on a pool: each target's `objects.ts` calls this with its own module URL (the node imports that
 * module and reads the handlers by these names) and its pool.
 */
import * as Effect from "effect/Effect";
import type * as Layer from "effect/Layer";
import { Probe, probe } from "./Probe.ts";
import { StorageConformance, storageConformance } from "./StorageConformance.ts";

export const probeObjects = <ROut, RIn>(main: string, pool: Layer.Layer<ROut, never, RIn>) => ({
  ProbeLive: Probe.make({ main, handler: "ProbeLive" }, probe.pipe(Effect.provide(pool))),
  StorageConformanceLive: StorageConformance.make(
    { main, handler: "StorageConformanceLive" },
    storageConformance.pipe(Effect.provide(pool)),
  ),
});

import { describe, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as MemoryAgentStorage from "../src/MemoryAgentStorage.ts";
import * as AgentStorageConformance from "../src/testing/AgentStorageConformance.ts";

// The storage conformance suite on the in-memory backend. The check of a batch is
// internal (`prepareBatch`) and its apply only updates maps, so no test can make
// it throw.
describe("MemoryAgentStorage", () => {
  const storage = AgentStorageConformance.layer(MemoryAgentStorage.layer);
  for (const conformanceCase of AgentStorageConformance.cases) {
    it.effect(conformanceCase.name, () => conformanceCase.run.pipe(Effect.provide(storage)));
  }
});

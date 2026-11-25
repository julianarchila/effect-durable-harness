import { defineConfig } from "vitest/config";

/** The example's tests: the inbound Worker and the sink on local workerd with the fake model, driven over HTTP. */
export default defineConfig({
  test: {
    include: ["test/*.test.ts"],
    fileParallelism: false,
    // alchemy/Test registers its sidecar cleanup after yours and expects hooks in registration order.
    sequence: { hooks: "list" },
    testTimeout: 120_000,
    hookTimeout: 300_000,
  },
});

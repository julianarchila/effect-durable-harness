import { defineConfig } from "vitest/config";

/**
 * The Rivet suite: deploys a local Rivet control plane, a pool and the probes on the Docker swarm
 * (`docker swarm init`), so it is outside `bun run test`. Files run one at a time; the deploy builds the node image.
 */
export default defineConfig({
  test: {
    include: ["test/**/*.test.ts"],
    fileParallelism: false,
    // alchemy/Test registers its sidecar cleanup as an afterAll after yours and expects hooks to run in
    // registration order (`packages/cloudflare/vitest.workerd.config.ts`).
    sequence: { hooks: "list" },
    testTimeout: 180_000,
    hookTimeout: 900_000,
  },
});

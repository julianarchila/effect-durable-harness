import { defineConfig } from "vitest/config";

/** workerd suite: deploys an Alchemy stack per file, so files run one at a time. */
export default defineConfig({
  test: {
    include: ["test/workerd/**/*.test.ts"],
    fileParallelism: false,
    // alchemy/Test registers its sidecar cleanup as an afterAll after yours and
    // expects hooks to run in registration order. Vitest's default ("stack")
    // runs them in reverse, so destroy(Stack) waits on a closed sidecar forever.
    sequence: { hooks: "list" },
    testTimeout: 120_000,
    hookTimeout: 300_000,
  },
});

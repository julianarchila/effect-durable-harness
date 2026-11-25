import { defineConfig } from "vitest/config";

/**
 * Opt-in live tests: a real model inside the object, on local workerd or, with
 * `ALCHEMY_TEST_DEV=0`, a deployed stage. `GOOGLE_GENERATIVE_AI_API_KEY` comes from the environment, as
 * `doppler run -- bun run --cwd packages/cloudflare test:live` sets it; without it the test is skipped.
 */
export default defineConfig({
  test: {
    include: ["test/live/**/*.test.ts"],
    fileParallelism: false,
    sequence: { hooks: "list" },
    testTimeout: 180_000,
    hookTimeout: 300_000,
  },
});

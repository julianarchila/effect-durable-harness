import { defineConfig } from "vitest/config";

// Opt-in live tests. `GOOGLE_GENERATIVE_AI_API_KEY` and `ANTHROPIC_API_KEY` come from the environment,
// as `doppler run -- bun run test:live` sets them, or from `packages/opencode/.env` (ignored by git); a test whose
// key is missing is skipped. `process.loadEnvFile` is Node's own .env reader: no new dependency.
try {
  process.loadEnvFile(new URL("./.env", import.meta.url));
} catch {
  // no .env: the environment decides
}

export default defineConfig({
  test: {
    include: ["test/live/**/*.test.ts"],
    testTimeout: 60_000,
  },
});

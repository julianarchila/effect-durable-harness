import { defineConfig } from "vitest/config";

// `test/live` calls the real Anthropic API: it runs only under `vitest.live.config.ts` (`test:live`).
export default defineConfig({
  test: {
    include: ["test/**/*.test.ts"],
    exclude: ["test/live/**"],
  },
});

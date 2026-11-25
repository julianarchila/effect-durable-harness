import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    include: ["test/**/*.test.ts"],
    // The kernel's tests are CPU-bound sweeps under `TestClock`, and a loaded machine (parallel worktrees,
    // concurrent runs) slowed them up to 12 times past their serial time, short ones more, beyond vitest's 5 s
    // default. 30 s is 20 times a test that takes 1.5 s run serially; slower ones size their own timeout with
    // `serialTimeout` (`test/Timeouts.ts`), with the same margin.
    testTimeout: 30_000,
  },
});

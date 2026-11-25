// Timeouts of the kernel tests whose work is CPU-bound. They run under `TestClock` and count yields, so
// nothing in them waits on real time; only their CPU work grows with the machine's load. The package's default
// (`vitest.config.ts`, 30 s) covers the tests that take under 1.5 s run serially; this sizes the rest.

/**
 * How many times its slowest measured serial run a CPU-bound test may take. Three concurrent runs of the suite on
 * a machine already loaded by other worktrees (load average 43, peak 50, on 8 cores) slowed the sized tests up to
 * 12.3 times.
 */
const LOAD_MARGIN = 20;

/**
 * The timeout of a CPU-bound test, sized from its slowest measured serial run (`--maxWorkers=1`):
 * `LOAD_MARGIN` times it, rounded up to 5 s. A loaded machine slows the test without failing the gate, and a
 * hang still fails it.
 */
export const serialTimeout = (measuredMs: number): number =>
  Math.ceil((measuredMs * LOAD_MARGIN) / 5_000) * 5_000;

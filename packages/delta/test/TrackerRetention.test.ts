// Each scenario needs a fresh Node process with `--expose-gc` and a synchronous exit status;
// Effect's `ChildProcess` would add a platform layer for no gain in a test.
// @effect-diagnostics-next-line nodeBuiltinImport:off
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import { describe, it } from "@effect/vitest";
import { strictEqual } from "@effect/vitest/utils";

describe("delta tracker retention across GC jobs", () => {
  it.each([
    "aborted-payload",
    "unadopted-prepared",
    "draft-proxies",
    "settled-lifecycle",
    "obsolete-revisions",
    "retained-settled-prepared",
    "retained-settled-change",
    "retained-settled-proxy",
    "retained-large-settled-proxy",
    "retained-large-placement-proxies",
    "stale-unprepared-change",
    "same-job-fast-cleanup",
    "same-job-folded-ops-cleanup",
    "lifecycle-churn",
    "adopted-changes-flat",
    "abandoned-draft-released",
    "tracker-with-open-draft",
  ])(
    "validates %s retention semantics",
    (scenario) => {
      const child = spawnSync(
        process.execPath,
        [
          "--expose-gc",
          fileURLToPath(new URL("./trackerRetention.worker.ts", import.meta.url)),
          scenario,
        ],
        { encoding: "utf8", timeout: 60_000 },
      );
      strictEqual(child.error, undefined, child.stderr);
      strictEqual(child.status, 0, `${child.stdout}\n${child.stderr}`);
    },
    65_000,
  );
});

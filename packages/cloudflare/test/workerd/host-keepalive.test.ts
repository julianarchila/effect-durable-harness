import * as Alchemy from "alchemy";
import * as Cloudflare from "@alchemy.run/cloudflare";
import * as Test from "@alchemy.run/test/Vitest";
import * as Effect from "effect/Effect";
import { expect } from "vitest";
import Stack from "./alchemy.run.ts";
import type { TaskView } from "./fixture/AgentProbe.ts";
import { callKeepAlive, deployed, idle, millis, objectName, type ProbeState } from "./support.ts";

/**
 * The keep-alive measurement as a host test: with the default 30 s keep-alive, five minutes of active work outside any
 * event (timers only, which do not hold the object under the pinned compatibility date) finish in one instance, with
 * no incoming call. Deployed only, and opt-in (`DURABLE_KEEPALIVE=1`): it takes about six minutes.
 */
const run = deployed && process.env["DURABLE_KEEPALIVE"] === "1";

const { test, beforeAll, afterAll, deploy, destroy } = Test.make({
  providers: Cloudflare.providers(),
  state: Alchemy.localState(),
  dev: true,
});

const stack = beforeAll(deploy(Stack), { timeout: 300_000 });
afterAll.skipIf(!!process.env.NO_DESTROY)(destroy(Stack), { timeout: 300_000 });

test.skipIf(!run)(
  "five minutes of active work with no incoming calls finish in one instance",
  Effect.gen(function* () {
    const { url } = yield* stack;
    const name = objectName("keepalive");
    const before = yield* callKeepAlive<ProbeState>(url, name, "state");
    const id = yield* callKeepAlive<number>(url, name, "startTask", "probe.busy", {
      chunks: 30,
      chunkMs: 10_000,
    });
    yield* idle(millis(330_000));
    const done = yield* callKeepAlive<TaskView | null>(url, name, "task", id);
    expect(done?.status).toBe("terminal");
    expect(done?.result?.instance).toBe(before.instance);
  }),
  { timeout: 600_000 },
);

import * as Alchemy from "alchemy";
import * as Cloudflare from "@alchemy.run/cloudflare";
import * as Test from "@alchemy.run/test/Vitest";
import * as Clock from "effect/Clock";
import * as Effect from "effect/Effect";
import { expect } from "vitest";
import Stack from "./alchemy.run.ts";
import { KEEP_ALIVE_MS } from "./fixture/AgentProbe.ts";
import {
  call,
  crash,
  eventually,
  idle,
  millis,
  objectName,
  settled,
  state,
  task,
} from "./support.ts";

/**
 * The host's alarm: at the deadline when the Session only sleeps, at most
 * the keep-alive ahead while there is active work, deleted when nothing is due, and armed before the commit that
 * leaves a live task. Local workerd by default; `ALCHEMY_TEST_DEV=0` runs it on a deployed stage.
 */
const { test, beforeAll, afterAll, deploy, destroy } = Test.make({
  providers: Cloudflare.providers(),
  state: Alchemy.localState(),
  dev: true,
});

const stack = beforeAll(deploy(Stack), { timeout: 300_000 });
afterAll.skipIf(!!process.env.NO_DESTROY)(destroy(Stack), { timeout: 300_000 });

test(
  "a Session that only sleeps holds the alarm at its deadline, with no keep-alive",
  Effect.gen(function* () {
    const { url } = yield* stack;
    const name = objectName("alarm-sleep");
    const at = (yield* state(url, name)).now + 3_600_000;
    yield* call(url, name, "startTask", "probe.sleep", { at });
    const settledState = yield* eventually(
      state(url, name),
      (current) => current.nextWakeAt === at && !current.activeWork,
    );
    expect(settledState.alarm).toBe(at);
  }),
);

test(
  "active work keeps the alarm within the keep-alive, and work that ends deletes it",
  Effect.gen(function* () {
    const { url } = yield* stack;
    const name = objectName("alarm-busy");
    const id = yield* call<number>(url, name, "startTask", "probe.busy", {
      chunks: 8,
      chunkMs: 500,
    });
    const samples = [];
    for (let sample = 0; sample < 6; sample++) {
      yield* idle(millis(400));
      samples.push(yield* state(url, name));
    }
    const active = samples.filter((sample) => sample.activeWork);
    expect(active.length).toBeGreaterThan(0);
    for (const sample of active) {
      expect(sample.alarm).not.toBeNull();
      expect(sample.alarm ?? Infinity).toBeLessThanOrEqual(sample.now + KEEP_ALIVE_MS);
    }
    yield* settled(url, name, id);
    const after = yield* eventually(state(url, name), (current) => !current.activeWork);
    expect(after.nextWakeAt).toBeNull();
    expect(after.alarm).toBeNull();
  }),
);

test(
  "a fresh instance of an idle object leaves no alarm armed",
  Effect.gen(function* () {
    const { url } = yield* stack;
    const name = objectName("alarm-idle-start");
    yield* eventually(state(url, name), (current) => current.alarm === null && !current.activeWork);
    // Every start reads the wake while open's reconcile pass is due. Applied after the kernel reported the pass
    // done, that read armed a keep-alive nothing cleared: an evicted idle object then restarted every keep-alive,
    // forever (seen deployed). Each round restarts the object and samples it within the keep-alive.
    for (let round = 0; round < 3; round++) {
      yield* crash(url, name);
      yield* state(url, name);
      yield* idle(millis(KEEP_ALIVE_MS / 2));
      const after = yield* state(url, name);
      expect(after.nextWakeAt).toBeNull();
      expect(after.alarm).toBeNull();
    }
  }),
);

test(
  "a sleep due before the keep-alive holds the alarm at its deadline while other work is active",
  Effect.gen(function* () {
    const { url } = yield* stack;
    const name = objectName("alarm-sleep-busy");
    const busy = yield* call<number>(url, name, "startTask", "probe.busy", {
      chunks: 10,
      chunkMs: 500,
    });
    yield* eventually(state(url, name), (current) => current.activeWork);
    // With the host's wakeups the kernel arms no timer: only the alarm wakes the sleep.
    const at = (yield* state(url, name)).now + KEEP_ALIVE_MS / 2;
    const id = yield* call<number>(url, name, "startTask", "probe.sleep", { at });
    const armed = yield* eventually(state(url, name), (current) => current.alarm === at, {
      spacing: "20 millis",
      times: 20,
    });
    expect(armed.activeWork).toBe(true);
    const done = yield* settled(url, name, id);
    expect(done?.result?.at ?? 0).toBeGreaterThanOrEqual(at);
    expect((yield* task(url, name, busy))?.status).not.toBe("terminal");
  }),
);

test(
  "a commit that leaves a live task arms the alarm before it, which recovers it after a crash",
  Effect.gen(function* () {
    const { url } = yield* stack;
    const name = objectName("alarm-commit");
    yield* call(url, name, "heal");
    const before = yield* state(url, name);
    const orphan = yield* call<{ readonly id: number; readonly alarm: number | null }>(
      url,
      name,
      "orphanTask",
      { chunks: 2, chunkMs: 200 },
    );
    expect(orphan.alarm).not.toBeNull();
    expect(orphan.alarm ?? Infinity).toBeLessThanOrEqual(before.now + 60_000);
    yield* crash(url, name);
    yield* idle(millis(KEEP_ALIVE_MS + 3_000));
    const readAt = yield* Clock.currentTimeMillis;
    const done = yield* task(url, name, orphan.id);
    expect(done?.status).toBe("terminal");
    // Finished by an instance the alarm started, before this read could start one.
    expect(done?.result?.instanceStartedAt ?? Infinity).toBeLessThan(readAt);
    expect(done?.result?.instance).not.toBe(before.instance);
  }),
);

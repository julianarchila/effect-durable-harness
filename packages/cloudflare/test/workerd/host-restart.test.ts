import * as Alchemy from "alchemy";
import * as Cloudflare from "@alchemy.run/cloudflare";
import * as Test from "@alchemy.run/test/Vitest";
import * as Clock from "effect/Clock";
import * as Effect from "effect/Effect";
import { expect } from "vitest";
import Stack from "./alchemy.run.ts";
import {
  call,
  crash,
  eventually,
  idle,
  millis,
  objectName,
  state,
  task,
  transcript,
} from "./support.ts";

/**
 * Restart, eviction and the input gate. An abort stands in for an
 * eviction or a crash. Each recovery must happen with no client call: the test waits without touching the object,
 * and then checks that the instance that finished the work started before its first read.
 */
const { test, beforeAll, afterAll, deploy, destroy } = Test.make({
  providers: Cloudflare.providers(),
  state: Alchemy.localState(),
  dev: true,
});

const stack = beforeAll(deploy(Stack), { timeout: 300_000 });
afterAll.skipIf(!!process.env.NO_DESTROY)(destroy(Stack), { timeout: 300_000 });

test(
  "a turn in flight finishes after a crash, woken by the alarm",
  Effect.gen(function* () {
    const { url } = yield* stack;
    const name = objectName("restart-turn");
    const before = yield* state(url, name);
    yield* call(url, name, "submit", "hello", "turn-1");
    // The crash must cut the model request: one in `prepare` reruns it with no partial to keep.
    yield* eventually(state(url, name), (current) =>
      current.phases.includes("core.generation:request"),
    );
    yield* crash(url, name);
    yield* idle(millis(8_000));
    const readAt = yield* Clock.currentTimeMillis;
    const after = yield* state(url, name);
    expect(after.instance).not.toBe(before.instance);
    expect(after.instanceStartedAt).toBeLessThan(readAt);
    // The request the crash cut is a failed attempt. Its partial becomes an aborted
    // assistant entry, and the next attempt runs once the retry backoff's sleep is due, woken by the alarm.
    const messages = yield* transcript(url, name);
    expect(messages.map((message) => message.role)).toEqual(["user", "assistant", "assistant"]);
    const answer = messages[2]?.text ?? "";
    expect(answer.startsWith("echo: hello")).toBe(true);
    expect(answer.startsWith(messages[1]?.text ?? "-")).toBe(true);
  }),
);

test(
  "a tool call in flight runs again after a crash and the turn completes",
  Effect.gen(function* () {
    const { url } = yield* stack;
    const name = objectName("restart-tool");
    const before = yield* state(url, name);
    yield* call(url, name, "submit", "tool:3000", "turn-1");
    yield* eventually(state(url, name), (current) => current.running.includes("core.tool"));
    yield* crash(url, name);
    yield* idle(millis(9_000));
    const readAt = yield* Clock.currentTimeMillis;
    const after = yield* state(url, name);
    expect(after.instance).not.toBe(before.instance);
    expect(after.instanceStartedAt).toBeLessThan(readAt);
    const messages = yield* transcript(url, name);
    expect(messages.map((message) => message.role)).toEqual([
      "user",
      "assistant",
      "toolResult",
      "assistant",
    ]);
    expect(messages[2]?.text).toBe("waited 3000 ms");
    expect(messages[3]?.text).toBe("done");
  }),
);

test(
  "a sleeping task completes at its deadline after a crash, with no call",
  Effect.gen(function* () {
    const { url } = yield* stack;
    const name = objectName("restart-sleep");
    const before = yield* state(url, name);
    const at = before.now + 3_000;
    const id = yield* call<number>(url, name, "startTask", "probe.sleep", { at });
    yield* eventually(state(url, name), (current) => current.alarm === at);
    yield* crash(url, name);
    yield* idle(millis(6_000));
    const readAt = yield* Clock.currentTimeMillis;
    const done = yield* task(url, name, id);
    expect(done?.status).toBe("terminal");
    expect(done?.result?.at ?? 0).toBeGreaterThanOrEqual(at);
    expect(done?.result?.instanceStartedAt ?? Infinity).toBeLessThan(readAt);
    expect(done?.result?.instance).not.toBe(before.instance);
  }),
);

test(
  "the first call after a restart waits for the constructor and sees a reconciled, resumed Session",
  Effect.gen(function* () {
    const { url } = yield* stack;
    const name = objectName("restart-gate");
    const before = yield* state(url, name);
    yield* call(url, name, "startTask", "probe.busy", { chunks: 20, chunkMs: 300 });
    yield* eventually(state(url, name), (current) => current.running.includes("probe.busy"));
    yield* crash(url, name);
    const after = yield* state(url, name);
    expect(after.instance).not.toBe(before.instance);
    expect(after.scheduling).toBe("running");
    expect(after.activeWork || after.nextWakeAt !== null).toBe(true);
    yield* eventually(state(url, name), (current) => current.running.includes("probe.busy"));
  }),
);

test(
  "a crash with nothing to do leaves no alarm, and the next call opens the Session as it was",
  Effect.gen(function* () {
    const { url } = yield* stack;
    const name = objectName("restart-idle");
    yield* call(url, name, "submit", "hi", "turn-1");
    yield* eventually(transcript(url, name), (messages) => messages.length === 2);
    const idleState = yield* eventually(state(url, name), (current) => current.alarm === null);
    yield* crash(url, name);
    // Opening schedules a reconcile, which is due work: a keep-alive until it ran, then nothing.
    const after = yield* eventually(state(url, name), (current) => current.alarm === null);
    expect(after.instance).not.toBe(idleState.instance);
    expect((yield* transcript(url, name)).length).toBe(2);
    expect(yield* task(url, name, 999_999)).toBeNull();
  }),
);

import * as Docker from "@alchemy.run/docker";
import * as Fly from "@alchemy.run/fly";
import * as Rivet from "@alchemy.run/rivet";
import * as Test from "@alchemy.run/test/Vitest";
import * as AgentStorageConformance from "@effect-durable-harness/core/testing/AgentStorageConformance";
import * as Alchemy from "alchemy";
import * as Clock from "effect/Clock";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import { expect } from "vitest";
import CloudStack from "../alchemy.cloud.run.ts";
import SwarmStack from "../alchemy.run.ts";
import { MAX_KEEP_ALIVE_MS } from "../src/Host.ts";
import { KEEP_ALIVE_MS, NestResult, type HoldResult } from "./fixture/Probe.ts";
import {
  call,
  cloud,
  conformance,
  flyMachine,
  eventually,
  idle,
  millis,
  objectName,
  openSocket,
  settled,
  state,
  task,
} from "./support.ts";

/**
 * The host on Rivet: the storage conformance suite over the engine's SQLite, the alarm policy, recovery after
 * a reset and after the actor sleeps, a model turn, the session socket, and waits in memory that outlast RivetKit's
 * idle sleep, on a local Rivet control plane and pool on
 * the Docker swarm, or on Rivet Cloud with nodes on Fly (`RIVET_TARGET=cloud`); `patches/@alchemy.run%2Frivet@2.0.0-beta.87.patch` adds the
 * activation's lifecycle.
 */
const Stack = cloud ? CloudStack : SwarmStack;

const { test, beforeAll, afterAll, deploy, destroy } = Test.make({
  providers: cloud
    ? Layer.mergeAll(Docker.providers(), Fly.providers(), Rivet.providers())
    : Layer.mergeAll(Docker.providers(), Rivet.providers()),
  state: Alchemy.localState(),
});

const stack = beforeAll(deploy(Stack), { timeout: 900_000 });
afterAll.skipIf(!!process.env.NO_DESTROY)(destroy(Stack), { timeout: 600_000 });

/** RivetKit's idle sleep (30 s) and a margin for the engine to stop the actor. */
const SLEEP_MS = 40_000;

/** A wait in memory twice RivetKit's idle sleep. */
const AWAKE_MS = 60_000;

const decodeNest = Schema.decodeUnknownOption(Schema.fromJsonString(NestResult));

for (const [index, conformanceCase] of AgentStorageConformance.cases.entries()) {
  test(
    `conformance: ${conformanceCase.name}`,
    Effect.gen(function* () {
      const { url } = yield* stack;
      const outcome = yield* conformance(url, objectName(`conformance-${index}`), index);
      expect(outcome).toEqual({ ok: true });
    }),
  );
}

test(
  "the keep-alive is capped under RivetKit's sleep timeout",
  Effect.sync(() => expect(MAX_KEEP_ALIVE_MS).toBeLessThan(30_000)),
);

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
      chunks: 6,
      chunkMs: 500,
    });
    const samples = [];
    for (let sample = 0; sample < 5; sample++) {
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
    // The first wake applied after a live commit never deletes the alarm: it stays within the keep-alive, and that
    // one early alarm, which finds nothing to do, deletes it.
    expect(after.alarm ?? after.now).toBeLessThanOrEqual(after.now + KEEP_ALIVE_MS);
    const cleared = yield* eventually(state(url, name), (current) => current.alarm === null, {
      times: 20,
    });
    expect(cleared.nextWakeAt).toBeNull();
  }),
);

test(
  "a task cut by a reset resumes on the alarm, in a fresh activation, without a call",
  Effect.gen(function* () {
    const { url } = yield* stack;
    const name = objectName("reset");
    const before = yield* state(url, name);
    const id = yield* call<number>(url, name, "startTask", "probe.busy", {
      chunks: 6,
      chunkMs: 500,
    });
    yield* idle(millis(800));
    yield* call(url, name, "crash");
    // Nothing calls the actor while the alarm brings it back.
    yield* idle(millis(KEEP_ALIVE_MS + 12_000));
    const readAt = yield* Clock.currentTimeMillis;
    const done = yield* settled(url, name, id);
    expect(done?.result?.instance).not.toBe(before.instance);
    expect(done?.result?.at ?? Infinity).toBeLessThan(readAt);
  }),
);

test(
  "a commit that leaves a live task arms the alarm before it, which recovers it after a reset",
  Effect.gen(function* () {
    const { url } = yield* stack;
    const name = objectName("orphan");
    const before = yield* state(url, name);
    const orphan = yield* call<{ readonly id: number; readonly alarm: number | null }>(
      url,
      name,
      "orphanTask",
      2,
      200,
    );
    expect(orphan.alarm).not.toBeNull();
    expect(orphan.alarm ?? Infinity).toBeLessThanOrEqual(before.now + 60_000);
    yield* call(url, name, "crash");
    yield* idle(millis(KEEP_ALIVE_MS + 12_000));
    const readAt = yield* Clock.currentTimeMillis;
    const done = yield* settled(url, name, orphan.id);
    expect(done?.status).toBe("terminal");
    expect(done?.result?.at ?? Infinity).toBeLessThan(readAt);
  }),
);

test(
  "a sleep outlives the actor sleeping: the alarm wakes a fresh activation at the deadline",
  Effect.gen(function* () {
    const { url } = yield* stack;
    const name = objectName("sleep-wake");
    const before = yield* state(url, name);
    const at = before.now + SLEEP_MS + 5_000;
    const id = yield* call<number>(url, name, "startTask", "probe.sleep", { at });
    // No call until well after the deadline: RivetKit stops the idle actor, and only the alarm restarts it.
    yield* idle(millis(SLEEP_MS + 15_000));
    const done = yield* settled(url, name, id);
    expect(done?.result?.instance).not.toBe(before.instance);
    expect(done?.result?.at ?? 0).toBeGreaterThanOrEqual(at);
  }),
  { timeout: 180_000 },
);

test(
  "commit latency: sequential commits, and parallel conversations under group commit",
  Effect.gen(function* () {
    const { url } = yield* stack;
    const name = objectName("bench");
    const bench = yield* call<{
      readonly perCommitMs: number;
      readonly parallelCommitsPerSecond: number;
    }>(url, name, "bench", 50, 16, 20);
    // Recorded for reference; the bound only catches a broken path.
    // Written to the test's stdout, which vitest prints with the test's name.
    yield* Effect.sync(() =>
      process.stdout.write(
        `[${cloud ? "rivet cloud + fly" : "local swarm"}] commit latency: ${bench.perCommitMs.toFixed(1)} ms per sequential commit, ${bench.parallelCommitsPerSecond.toFixed(0)} commits/s over 16 conversations\n`,
      ),
    );
    expect(bench.perCommitMs).toBeLessThan(500);
    expect(bench.parallelCommitsPerSecond).toBeGreaterThan(0);
  }),
);

test(
  "a model turn: the submission is answered and committed to the transcript",
  Effect.gen(function* () {
    const { url } = yield* stack;
    const name = objectName("turn");
    yield* call(url, name, "submit", "hello", "request-1");
    const transcript = yield* eventually(
      call<ReadonlyArray<{ readonly role: string; readonly text: string }>>(
        url,
        name,
        "transcript",
      ),
      (messages) => messages.some((message) => message.role === "assistant"),
    );
    expect(transcript).toEqual([
      { role: "user", text: "hello" },
      { role: "assistant", text: "echo: hello" },
    ]);
  }),
);

test(
  "the session socket: hello, the session and application events; it keeps the actor awake, and a reset closes it for the client to reconnect",
  Effect.gen(function* () {
    const { url } = yield* stack;
    const name = objectName("socket");
    yield* call(url, name, "state");
    const client = yield* openSocket(name, "ana");
    const hello = yield* eventually(
      Effect.sync(() => client.events),
      (events) => events.some((event) => event.type === "activity"),
    );
    expect(hello.map((event) => event.type)).toEqual(["hello", "session", "activity"]);
    expect(hello[0]).toMatchObject({ type: "hello", connection: { username: "ana" } });
    const first = yield* state(url, name);
    expect(first.sockets).toBe(1);
    expect(yield* call<number>(url, name, "broadcast", "before")).toBe(1);
    yield* eventually(
      Effect.sync(() => client.events),
      (events) => events.some((event) => event.type === "event"),
    );
    // Rivet 3 keeps an actor with an open connection awake: past RivetKit's idle sleep, the same activation.
    yield* idle(millis(SLEEP_MS));
    const awake = yield* state(url, name);
    expect(awake.instance).toBe(first.instance);
    expect(client.closed()).toBeUndefined();
    // A reset stops the activation: the gateway closes the socket (1012, `actor.stopped`), and `SessionClient`
    // reconnects on any code but `REFUSED_CLOSE_CODE`.
    yield* call(url, name, "crash");
    const closed = yield* eventually(
      Effect.sync(() => client.closed()),
      (value) => value !== undefined,
    );
    expect(closed).toEqual({ code: 1012, reason: "actor.stopped" });
    const again = yield* openSocket(name, "ana");
    yield* eventually(
      Effect.sync(() => again.events),
      (events) => events.some((event) => event.type === "activity"),
    );
    const second = yield* state(url, name);
    expect(second.instance).not.toBe(first.instance);
    expect(second.sockets).toBe(1);
    expect(yield* call<number>(url, name, "broadcast", "after")).toBe(1);
    again.close();
  }),
  { timeout: 180_000 },
);

test(
  "a phase and a tool waiting in memory past RivetKit's idle sleep stay in one activation, which sleeps once they end",
  Effect.gen(function* () {
    const { url } = yield* stack;
    const holder = objectName("awake-task");
    const nester = objectName("awake-tool");
    const holderBefore = yield* state(url, holder);
    const nesterBefore = yield* state(url, nester);
    const id = yield* call<number>(url, holder, "startHold", AWAKE_MS);
    yield* call(url, nester, "submit", `nest:${AWAKE_MS}`, "awake-nest");
    // No call to either actor during the wait: only the host's hold keeps them in this activation.
    yield* idle(millis(AWAKE_MS + 5_000));

    const held = yield* eventually(
      call<typeof HoldResult.Type | null>(url, holder, "holdResult", id),
      (result) => result !== null,
      { times: 20 },
    );
    const messages = yield* eventually(
      call<ReadonlyArray<{ readonly role: string; readonly text: string }>>(
        url,
        nester,
        "transcript",
      ),
      (current) => current.some((message) => message.role === "toolResult"),
      { times: 20 },
    );
    const toolResult = messages.find((message) => message.role === "toolResult")?.text ?? "";
    const nested = Option.getOrUndefined(decodeNest(toolResult));
    yield* Effect.log("awake", {
      task: { before: holderBefore, held },
      tool: { before: nesterBefore, toolResult },
    });

    expect({
      waitedIn: held?.waitedIn,
      instance: held?.instance,
      instanceStartedAt: held?.instanceStartedAt,
      restarts: held?.restarts,
      child: held?.child,
    }).toEqual({
      waitedIn: holderBefore.instance,
      instance: holderBefore.instance,
      instanceStartedAt: holderBefore.instanceStartedAt,
      restarts: 0,
      child: "completed",
    });
    expect((held?.at ?? 0) - (held?.waitedFrom ?? 0)).toBeGreaterThanOrEqual(AWAKE_MS - 5_000);
    expect({
      waitedIn: nested?.waitedIn,
      instance: nested?.instance,
      instanceStartedAt: nested?.instanceStartedAt,
      nested: nested?.nested,
    }).toEqual({
      waitedIn: nesterBefore.instance,
      instance: nesterBefore.instance,
      instanceStartedAt: nesterBefore.instanceStartedAt,
      nested: "woke",
    });
    expect((nested?.at ?? 0) - (nested?.waitedFrom ?? 0)).toBeGreaterThanOrEqual(AWAKE_MS - 5_000);

    // Once the waits end, nothing holds the actors: with no call, RivetKit stops them, and the next call starts a
    // fresh activation. The first wake after a live commit may leave one early keep-alive alarm, which deletes
    // itself when it fires; wait for that before the idle sleep.
    const idleAfter = yield* eventually(
      Effect.all([state(url, holder), state(url, nester)]),
      ([task, tool]) =>
        !task.activeWork && !tool.activeWork && task.alarm === null && tool.alarm === null,
    );
    expect(idleAfter.map((current) => current.instance)).toEqual([
      holderBefore.instance,
      nesterBefore.instance,
    ]);
    yield* idle(millis(SLEEP_MS));
    const holderAfter = yield* state(url, holder);
    const nesterAfter = yield* state(url, nester);
    expect(holderAfter.instance).not.toBe(holderBefore.instance);
    expect(nesterAfter.instance).not.toBe(nesterBefore.instance);
  }),
  { timeout: 300_000 },
);

test.skipIf(!cloud)(
  "failover: stopping the Fly Machine that runs an actor (SIGINT, graceful) moves it to the other node, and its task finishes there",
  Effect.gen(function* () {
    const { url } = yield* stack;
    const name = objectName("failover-stop");
    const id = yield* call<number>(url, name, "startTask", "probe.busy", {
      chunks: 60,
      chunkMs: 500,
    });
    const before = yield* eventually(state(url, name), (current) => current.activeWork);
    expect(before.node).not.toBe("");
    yield* idle(millis(2_000));
    const stoppedAt = yield* Clock.currentTimeMillis;
    yield* flyMachine("stop", before.node);
    // The first answer from another activation, on the other node.
    const moved = yield* eventually(
      state(url, name),
      (current) => current.instance !== before.instance && current.node !== before.node,
      { spacing: "1 second", times: 180 },
    );
    const movedAt = yield* Clock.currentTimeMillis;
    const done = yield* eventually(task(url, name, id), (view) => view?.status === "terminal", {
      spacing: "1 second",
      times: 180,
    });
    const doneAt = yield* Clock.currentTimeMillis;
    yield* Effect.sync(() =>
      process.stdout.write(
        `[rivet cloud + fly] failover, graceful stop: actor answered from the other node ${((movedAt - stoppedAt) / 1000).toFixed(1)} s after the stop; task finished ${((doneAt - stoppedAt) / 1000).toFixed(1)} s after it\n`,
      ),
    );
    expect(moved.node).not.toBe(before.node);
    expect(done?.result?.instance).not.toBe(before.instance);
    yield* flyMachine("start", before.node);
  }),
  { timeout: 600_000 },
);

test.skipIf(!cloud)(
  "failover: killing the Fly Machine that runs an actor (SIGKILL, a crash) moves it to the other node, and its task finishes there",
  Effect.gen(function* () {
    const { url } = yield* stack;
    const name = objectName("failover-kill");
    const id = yield* call<number>(url, name, "startTask", "probe.busy", {
      chunks: 60,
      chunkMs: 500,
    });
    const before = yield* eventually(state(url, name), (current) => current.activeWork);
    expect(before.node).not.toBe("");
    yield* idle(millis(2_000));
    const stoppedAt = yield* Clock.currentTimeMillis;
    yield* flyMachine("kill", before.node);
    // The first answer from another activation, on the other node.
    const moved = yield* eventually(
      state(url, name),
      (current) => current.instance !== before.instance && current.node !== before.node,
      { spacing: "1 second", times: 180 },
    );
    const movedAt = yield* Clock.currentTimeMillis;
    const done = yield* eventually(task(url, name, id), (view) => view?.status === "terminal", {
      spacing: "1 second",
      times: 180,
    });
    const doneAt = yield* Clock.currentTimeMillis;
    yield* Effect.sync(() =>
      process.stdout.write(
        `[rivet cloud + fly] failover, crash (SIGKILL): actor answered from the other node ${((movedAt - stoppedAt) / 1000).toFixed(1)} s after the stop; task finished ${((doneAt - stoppedAt) / 1000).toFixed(1)} s after it\n`,
      ),
    );
    expect(moved.node).not.toBe(before.node);
    expect(done?.result?.instance).not.toBe(before.instance);
    yield* flyMachine("start", before.node);
  }),
  { timeout: 600_000 },
);

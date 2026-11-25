import * as Alchemy from "alchemy";
import * as Cloudflare from "@alchemy.run/cloudflare";
import * as Test from "@alchemy.run/test/Vitest";
import * as Effect from "effect/Effect";
import { expect } from "vitest";
import type { SinkLedger } from "../src/protocol.ts";
import {
  contactAddress,
  crash,
  eventually,
  injectFaults,
  inspect,
  ledger,
  postInbound,
} from "./support.ts";

/**
 * The follow-up example on local workerd, with the fake model (`src/server/models.ts`): idempotent inbound, the
 * reporter and the durable sleep, driven over HTTP as a provider's webhook and the sink's reader would.
 */
const KEEP_ALIVE_MS = 1_000;
const FOLLOW_UP_AFTER_MS = 8_000;
process.env["FOLLOWUP_MODEL"] = "faux";
process.env["FOLLOWUP_ALLOW_CRASH"] = "true";
process.env["FOLLOWUP_KEEP_ALIVE_MS"] = String(KEEP_ALIVE_MS);
process.env["FOLLOW_UP_AFTER_MS"] = String(FOLLOW_UP_AFTER_MS);
const { default: Stack } = await import("./alchemy.run.ts");

const { test, beforeAll, afterAll, deploy, destroy } = Test.make({
  providers: Cloudflare.providers(),
  state: Alchemy.localState(),
  dev: true,
});

const stack = beforeAll(deploy(Stack), { timeout: 300_000 });
afterAll.skipIf(!!process.env.NO_DESTROY)(destroy(Stack), { timeout: 300_000 });

const replies = (current: SinkLedger) =>
  current.deliveries.filter((delivery) => delivery.kind === "reply");

test(
  "one inbound message is answered and delivered to the webhook exactly once",
  Effect.gen(function* () {
    const { api, sink } = yield* stack;
    const to = contactAddress("one");
    const result = yield* postInbound(api, { providerMessageId: "m1", from: to, text: "hello" });
    expect(result).toEqual({ status: "accepted" });
    const delivered = yield* eventually(
      ledger(sink, to),
      (current) => current.deliveries.length > 0,
    );
    expect(delivered.deliveries).toMatchObject([{ kind: "reply", text: "reply: hello" }]);
    expect(delivered.attempts).toMatchObject([{ outcome: "recorded" }]);
    // The receipt is recorded in the commit that completes the delivery.
    const state = yield* eventually(inspect(api, to), (current) =>
      current.tasks.every((task) => task.kind !== "followup.deliver-reply"),
    );
    expect(Object.values(state.deliveries?.delivered ?? {})).toEqual([
      { deliveryId: delivered.deliveries[0]?.deliveryId, key: delivered.deliveries[0]?.key },
    ]);
    expect(state.transcript).toEqual(["user: hello", "assistant: reply: hello"]);
  }),
);

test(
  "the same inbound message twice is one submission and one delivery",
  Effect.gen(function* () {
    const { api, sink } = yield* stack;
    const to = contactAddress("twice");
    const inbound = { providerMessageId: "wamid.42", from: to, text: "are you open today?" };
    const [first, second] = yield* Effect.all([
      postInbound(api, inbound),
      postInbound(api, inbound),
    ]);
    expect([first?.status, second?.status].toSorted()).toEqual(["accepted", "duplicate"]);
    // And once more after the answer went out: the webhook's retry of an old message.
    yield* eventually(ledger(sink, to), (current) => current.deliveries.length > 0);
    expect(yield* postInbound(api, inbound)).toEqual({ status: "duplicate" });
    const state = yield* eventually(inspect(api, to), (current) =>
      current.tasks.every((task) => task.kind !== "followup.deliver-reply"),
    );
    expect(state.transcript).toEqual([
      "user: are you open today?",
      "assistant: reply: are you open today?",
    ]);
    // One follow-up sleeps, not three.
    expect(state.tasks.filter((task) => task.kind === "followup.follow-up")).toHaveLength(1);
    const final = yield* ledger(sink, to);
    expect(replies(final)).toHaveLength(1);
    expect(final.attempts).toHaveLength(1);
  }),
);

test(
  "a webhook that refuses the first attempts gets the same key again until it records one delivery",
  Effect.gen(function* () {
    const { api, sink } = yield* stack;
    const to = contactAddress("refused");
    yield* injectFaults(sink, to, { failFirst: 2, delayMs: 0 });
    yield* postInbound(api, { providerMessageId: "m1", from: to, text: "hello" });
    // Backoffs of one and two seconds, each a durable sleep.
    const delivered = yield* eventually(
      ledger(sink, to),
      (current) => current.deliveries.length > 0,
    );
    expect(delivered.attempts.map((attempt) => attempt.outcome)).toEqual([
      "refused",
      "refused",
      "recorded",
    ]);
    expect(new Set(delivered.attempts.map((attempt) => attempt.key)).size).toBe(1);
    expect(delivered.deliveries).toMatchObject([{ kind: "reply", text: "reply: hello" }]);
  }),
);

test(
  "an object killed mid-delivery resends after the restart under the same key, and the webhook keeps one",
  Effect.gen(function* () {
    const { api, sink } = yield* stack;
    const to = contactAddress("killed");
    // The sink records the delivery, then answers four seconds later: the object dies before the answer.
    yield* injectFaults(sink, to, { failFirst: 0, delayMs: 4_000 });
    yield* postInbound(api, { providerMessageId: "m1", from: to, text: "hello" });
    yield* eventually(ledger(sink, to), (current) => current.attempts.length > 0);
    yield* crash(api, to);
    // The alarm restarts the object, which reruns the `send` phase.
    const resent = yield* eventually(ledger(sink, to), (current) => current.attempts.length > 1);
    expect(resent.attempts.map((attempt) => attempt.outcome)).toEqual(["recorded", "duplicate"]);
    expect(resent.attempts[0]?.key).toBe(resent.attempts[1]?.key);
    expect(resent.deliveries).toHaveLength(1);
    const state = yield* eventually(
      inspect(api, to),
      (current) => Object.keys(current.deliveries?.delivered ?? {}).length > 0,
    );
    expect(Object.values(state.deliveries?.delivered ?? {})).toEqual([
      { deliveryId: resent.deliveries[0]?.deliveryId, key: resent.deliveries[0]?.key },
    ]);
  }),
);

test(
  "a follow-up sleeps on the alarm at its deadline, survives an eviction, and is delivered after the silence",
  Effect.gen(function* () {
    const { api, sink } = yield* stack;
    const to = contactAddress("follow-up");
    yield* postInbound(api, { providerMessageId: "m1", from: to, text: "hello" });
    yield* eventually(ledger(sink, to), (current) => current.deliveries.length > 0);
    // Only the follow-up is left: it sleeps, holding no invocation, and the alarm sits at its deadline.
    const asleep = yield* eventually(
      inspect(api, to),
      (current) =>
        current.tasks.length === 1 &&
        current.tasks[0]?.state === "sleeping" &&
        current.alarm === current.tasks[0]?.until,
    );
    expect(asleep.tasks[0]?.kind).toBe("followup.follow-up");
    expect((asleep.alarm ?? 0) - asleep.now).toBeGreaterThan(KEEP_ALIVE_MS);
    // Evicted: from here on, only the alarm starts the object again; the test reads the sink alone.
    yield* crash(api, to);
    const followedUp = yield* eventually(
      ledger(sink, to),
      (current) => current.deliveries.length > 1,
      FOLLOW_UP_AFTER_MS / 1000 + 30,
    );
    expect(followedUp.deliveries).toMatchObject([
      { kind: "reply", text: "reply: hello" },
      {
        kind: "follow-up",
        text: "follow-up: Just checking in. Is there anything else I can help with?",
      },
    ]);
    // The first wake after the follow-up's last live commit keeps one keep-alive alarm, which clears it when it fires.
    const state = yield* eventually(
      inspect(api, to),
      (current) => current.tasks.length === 0 && current.alarm === null,
    );
    expect(state.alarm).toBeNull();
    expect(state.transcript.filter((line) => line.startsWith("user: [follow-up]"))).toHaveLength(1);
  }),
);

test(
  "a newer inbound message before the deadline supersedes the pending follow-up",
  Effect.gen(function* () {
    const { api, sink } = yield* stack;
    const to = contactAddress("superseded");
    yield* postInbound(api, { providerMessageId: "m1", from: to, text: "first" });
    yield* eventually(ledger(sink, to), (current) => current.deliveries.length > 0);
    yield* Effect.sleep("3 seconds");
    yield* postInbound(api, { providerMessageId: "m2", from: to, text: "second" });
    // The first follow-up wakes about five seconds before the second and finds a newer message.
    const followedUp = yield* eventually(
      ledger(sink, to),
      (current) => current.deliveries.some((delivery) => delivery.kind === "follow-up"),
      FOLLOW_UP_AFTER_MS / 1000 + 30,
    );
    expect(followedUp.deliveries.map((delivery) => delivery.kind)).toEqual([
      "reply",
      "reply",
      "follow-up",
    ]);
    const first = followedUp.deliveries[0]?.at ?? 0;
    expect((followedUp.deliveries[2]?.at ?? 0) - first).toBeGreaterThanOrEqual(
      FOLLOW_UP_AFTER_MS + 3_000,
    );
    const state = yield* eventually(inspect(api, to), (current) => current.tasks.length === 0);
    expect(state.transcript.filter((line) => line.startsWith("user: [follow-up]"))).toHaveLength(1);
    expect((yield* ledger(sink, to)).deliveries).toHaveLength(3);
  }),
);

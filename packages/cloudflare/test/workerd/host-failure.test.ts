import * as Alchemy from "alchemy";
import * as Cloudflare from "@alchemy.run/cloudflare";
import * as Test from "@alchemy.run/test/Vitest";
import * as Effect from "effect/Effect";
import { expect } from "vitest";
import Stack from "./alchemy.run.ts";
import { call, deployed, eventually, objectName, state, transcript } from "./support.ts";

/**
 * Storage failure: a failed Session aborts the object, and the next call gets a fresh,
 * working Session. Also the per-value limit of Durable Object SQLite.
 */
const { test, beforeAll, afterAll, deploy, destroy } = Test.make({
  providers: Cloudflare.providers(),
  state: Alchemy.localState(),
  dev: true,
});

const stack = beforeAll(deploy(Stack), { timeout: 300_000 });
afterAll.skipIf(!!process.env.NO_DESTROY)(destroy(Stack), { timeout: 300_000 });

test(
  "a storage failure aborts the object; the next call gets a fresh, working Session",
  Effect.gen(function* () {
    const { url } = yield* stack;
    const name = objectName("failure-storage");
    const before = yield* state(url, name);
    yield* call(url, name, "poison");
    expect(yield* call(url, name, "submit", "lost", "turn-1")).toEqual({ reset: true });
    // The failed Session aborts the object once it has closed; until then calls fail.
    yield* eventually(
      state(url, name),
      (current) => current.instance !== undefined && current.instance !== before.instance,
    );
    yield* call(url, name, "heal");
    yield* call(url, name, "submit", "kept", "turn-2");
    const messages = yield* eventually(transcript(url, name), (current) => current.length === 2);
    expect(messages[0]?.text).toBe("kept");
    expect(messages[1]?.text.startsWith("echo: kept")).toBe(true);
  }),
);

// Local workerd does not enforce Cloudflare's per-value limit; it is checked on a deployed stage. The documented
// limit is 2 MB, but a 3 MiB entry committed on 2026-10-19; this measures where it is.
test.skipIf(!deployed)(
  "an entry of 7 MiB commits, one of 9 MiB fails its commit, and the object recovers",
  Effect.gen(function* () {
    const { url } = yield* stack;
    const name = objectName("failure-image");
    const before = yield* state(url, name);
    expect(yield* call(url, name, "submitImage", 7 * 1024 * 1024, "turn-1")).toBeTypeOf("number");
    yield* eventually(transcript(url, name), (current) => current.length === 2);
    expect(yield* call(url, name, "submitImage", 9 * 1024 * 1024, "turn-2")).toEqual({
      reset: true,
    });
    yield* eventually(
      state(url, name),
      (current) => current.instance !== undefined && current.instance !== before.instance,
    );
    yield* call(url, name, "submit", "small", "turn-3");
    yield* eventually(transcript(url, name), (current) => current.length === 4);
  }),
);

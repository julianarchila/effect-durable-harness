import * as Alchemy from "alchemy";
import * as Cloudflare from "@alchemy.run/cloudflare";
import * as Test from "@alchemy.run/test/Vitest";
import * as Effect from "effect/Effect";
import { expect } from "vitest";
import Stack from "./alchemy.run.ts";
import {
  assistantTexts,
  call,
  crash,
  deployed,
  eventually,
  idle,
  millis,
  objectName,
  openView,
  state,
  task,
} from "./support.ts";

/**
 * The conversation view over a hibernatable WebSocket: a snapshot, then op
 * batches that follow the conversation. A crash or a deploy closes the sockets, so the client reconnects; after
 * hibernation the sockets stay open, and the new instance re-subscribes them with a fresh snapshot.
 */
const { test, beforeAll, afterAll, deploy, destroy } = Test.make({
  providers: Cloudflare.providers(),
  state: Alchemy.localState(),
  dev: true,
});

const stack = beforeAll(deploy(Stack), { timeout: 300_000 });
afterAll.skipIf(!!process.env.NO_DESTROY)(destroy(Stack), { timeout: 300_000 });

test(
  "a view socket gets a snapshot, then op batches that follow the conversation",
  Effect.gen(function* () {
    const { url } = yield* stack;
    const name = objectName("view-follow");
    yield* state(url, name);
    const view = yield* openView(url, name);
    yield* eventually(
      Effect.sync(() => view.messages.length),
      (count) => count > 0,
    );
    expect(view.messages[0]?.type).toBe("snapshot");
    yield* call(url, name, "submit", "hello", "turn-1");
    const texts = yield* eventually(
      Effect.sync(() => assistantTexts(view.reader.value())),
      (current) => current.length === 1,
      { times: 120 },
    );
    expect(texts[0]?.startsWith("echo: hello")).toBe(true);
    expect(view.messages.slice(1).every((message) => message.type === "ops")).toBe(true);
    view.close();
  }),
);

test(
  "a crash closes the socket; a client that reconnects gets a fresh snapshot of the committed view",
  Effect.gen(function* () {
    const { url } = yield* stack;
    const name = objectName("view-crash");
    yield* call(url, name, "submit", "first", "turn-1");
    const view = yield* openView(url, name);
    yield* eventually(
      Effect.sync(() => assistantTexts(view.reader.value())),
      (current) => current.length === 1,
      { times: 120 },
    );
    yield* crash(url, name);
    // An abort resets the object and the runtime closes its sockets, as a deploy does.
    yield* eventually(Effect.sync(view.closed), (closed) => closed);
    const again = yield* openView(url, name);
    yield* eventually(
      Effect.sync(() => assistantTexts(again.reader.value())),
      (current) => current.length === 1,
    );
    expect(again.messages[0]?.type).toBe("snapshot");
    yield* call(url, name, "submit", "second", "turn-2");
    const texts = yield* eventually(
      Effect.sync(() => assistantTexts(again.reader.value())),
      (current) => current.length === 2,
      { times: 120 },
    );
    expect(texts[1]?.startsWith("echo: second")).toBe(true);
    again.close();
  }),
);

// Local workerd keeps an idle object with an open socket in memory (measured: 20 s, same instance); Cloudflare
// hibernates it after about 10 s, and only there does a new instance find the socket open. It hibernates only
// because the host turns the kernel's idle-cache timers off: with them, a pending timer kept it in memory (measured
// 2026-10-19).
test.skipIf(!deployed)(
  "after hibernation the next instance re-subscribes the open socket with a fresh snapshot",
  Effect.gen(function* () {
    const { url } = yield* stack;
    const name = objectName("view-hibernate");
    const before = yield* state(url, name);
    const view = yield* openView(url, name);
    yield* eventually(
      Effect.sync(() => view.messages.length),
      (count) => count === 1,
    );
    yield* idle(millis(30_000));
    yield* call(url, name, "submit", "after", "turn-1");
    const texts = yield* eventually(
      Effect.sync(() => assistantTexts(view.reader.value())),
      (current) => current.length === 1,
      { times: 120 },
    );
    expect(texts[0]?.startsWith("echo: after")).toBe(true);
    const after = yield* state(url, name);
    expect(after.instance).not.toBe(before.instance);
    expect(view.messages.filter((message) => message.type === "snapshot").length).toBe(2);
    expect(view.closed()).toBe(false);
    view.close();
  }),
  { timeout: 120_000 },
);

// Hibernation needs 10 s without events, the keep-alive alarm comes every 30 s.
test.skipIf(!deployed)(
  "work that runs longer than the hibernation delay finishes in one instance with a socket open",
  Effect.gen(function* () {
    const { url } = yield* stack;
    const name = objectName("view-busy");
    const before = yield* state(url, name);
    const view = yield* openView(url, name);
    const id = yield* call<number>(url, name, "startTask", "probe.busy", {
      chunks: 25,
      chunkMs: 1_000,
    });
    yield* idle(millis(30_000));
    const done = yield* task(url, name, id);
    expect(done?.status).toBe("terminal");
    expect(done?.result?.instance).toBe(before.instance);
    expect(view.closed()).toBe(false);
    view.close();
  }),
  { timeout: 120_000 },
);

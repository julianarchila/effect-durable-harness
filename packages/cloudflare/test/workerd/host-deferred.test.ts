import * as Alchemy from "alchemy";
import * as Cloudflare from "@alchemy.run/cloudflare";
import * as Test from "@alchemy.run/test/Vitest";
import * as Effect from "effect/Effect";
import { expect } from "vitest";
import Stack from "./alchemy.run.ts";
import type { LiveState } from "@effect-durable-harness/core/Live";
import type { ConversationView } from "@effect-durable-harness/core/View";
import {
  assistantTexts,
  call,
  callKeepAlive,
  crash,
  eventually,
  objectName,
  openView,
  type ProbeState,
  state,
} from "./support.ts";

/**
 * Deferred commits in a Durable Object. The deadline store is a foreground timer of the object: with the host's `SessionWakeups` and `IdleTimers` off it still fires, and a reset loses
 * what is held, as a crash does. Local workerd by default; `ALCHEMY_TEST_DEV=0` runs it on a deployed stage.
 */
const { test, beforeAll, afterAll, deploy, destroy } = Test.make({
  providers: Cloudflare.providers(),
  state: Alchemy.localState(),
  dev: true,
});

const stack = beforeAll(deploy(Stack), { timeout: 300_000 });
afterAll.skipIf(!!process.env.NO_DESTROY)(destroy(Stack), { timeout: 300_000 });

interface Note {
  readonly memory: string;
  readonly stored: string | null;
}

test(
  "stores a held change at its deadline, with no other commit",
  Effect.gen(function* () {
    const { url } = yield* stack;
    const name = objectName("deferred-deadline");
    yield* call(url, name, "holdNote", "held", 500);
    const held = yield* call<Note>(url, name, "note");
    expect(held).toEqual({ memory: "held", stored: "" });
    const stored = yield* eventually(
      call<Note>(url, name, "note"),
      (note) => note.stored === "held",
    );
    expect(stored.memory).toBe("held");
  }),
);

test(
  "loses a held change when the object resets before its deadline",
  Effect.gen(function* () {
    const { url } = yield* stack;
    const name = objectName("deferred-reset");
    yield* call(url, name, "holdNote", "lost", 3_600_000);
    expect(yield* call<Note>(url, name, "note")).toEqual({ memory: "lost", stored: "" });
    yield* crash(url, name);
    yield* state(url, name);
    expect(yield* call<Note>(url, name, "note")).toEqual({ memory: "", stored: "" });
  }),
);

/** The partial answer a view's documents show in `core.live`, if any. */
const partial = (docs: ConversationView["docs"] | undefined): string | undefined => {
  // SAFETY: a view's `core.live` is a `LiveState`, which its schema checked when it was written.
  const message = (docs?.["core.live"] as LiveState | undefined)?.generation?.message;
  const block = message?.content.find((part) => part.type === "text");
  return block?.type === "text" ? block.text : undefined;
};

// Review fix: a client that connects mid-answer, while `core.live` holds deferred partials and no view has mounted the
// conversation, gets a snapshot from memory, and the ops that follow apply to it. On `KeepAliveProbe`, whose settings keep the default
// `persistWithinMs` (one second).
test(
  "a view socket that connects mid-answer starts from what memory holds and follows the answer",
  Effect.gen(function* () {
    const { url } = yield* stack;
    const name = objectName("deferred-view");
    const before = yield* callKeepAlive<ProbeState>(url, name, "state");
    yield* callKeepAlive(url, name, "submit", "hello", "turn-1");
    // A partial is in memory and no view has mounted the conversation yet.
    yield* eventually(callKeepAlive<number>(url, name, "partialLength"), (length) => length > 0, {
      spacing: "20 millis",
      times: 250,
    });
    const late = yield* openView(url, name, "keepalive");
    yield* eventually(
      Effect.sync(() => late.messages.length),
      (count) => count > 0,
    );
    expect(late.messages[0]?.type).toBe("snapshot");
    // The snapshot has the held partial; storage may not have it yet.
    expect(partial(late.reader.value()?.docs)).toBeDefined();
    const texts = yield* eventually(
      Effect.sync(() => assistantTexts(late.reader.value())),
      (current) => current.length === 1,
      { times: 120 },
    );
    expect(texts[0]?.startsWith("echo: hello")).toBe(true);
    // The Session did not fail: the same instance answers.
    expect((yield* callKeepAlive<ProbeState>(url, name, "state")).instance).toBe(before.instance);
    late.close();
  }),
);

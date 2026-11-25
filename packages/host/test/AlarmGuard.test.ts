// The order of the alarm writes around a commit that leaves a live task, over an alarm in memory: a wake that a live
// commit may have overtaken never deletes the alarm the commit armed.
import { assert, describe, it } from "@effect/vitest";
import { AgentStorage, type StorageWrite } from "@effect-durable-harness/core/AgentStorage";
import * as MemoryAgentStorage from "@effect-durable-harness/core/MemoryAgentStorage";
import { ConversationId, TaskId } from "@effect-durable-harness/core/Ids";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import type { AlarmAction } from "../src/internal/alarm.ts";
import * as AlarmGuard from "../src/internal/alarmGuard.ts";
import type { AlarmStore } from "../src/internal/alarmStore.ts";

const keepAliveMs = 30_000;

const idle = { nextWakeAt: Option.none(), activeWork: false };

const live: StorageWrite = {
  type: "task",
  value: {
    id: TaskId.make(7),
    conversationId: ConversationId.make(1),
    kind: "test",
    version: 1,
    input: null,
    background: false,
    abortRequested: false,
    state: { status: "pending", checkpoint: { phase: "run" } },
  },
};

/** An alarm in memory at time 0 that records each action, and can hold its next read until `release`. */
const makeStore = Effect.gen(function* () {
  const actions: Array<AlarmAction["_tag"]> = [];
  let alarm: number | undefined;
  let hold:
    | { readonly entered: Deferred.Deferred<void>; readonly release: Deferred.Deferred<void> }
    | undefined;
  const store: AlarmStore = {
    now: Effect.succeed(0),
    get: Effect.gen(function* () {
      const held = hold;
      hold = undefined;
      if (held !== undefined) {
        yield* Deferred.succeed(held.entered, undefined);
        yield* Deferred.await(held.release);
      }
      return Option.fromUndefinedOr(alarm);
    }),
    apply: (action) =>
      Effect.sync(() => {
        actions.push(action._tag);
        if (action._tag === "Set") alarm = action.at;
        if (action._tag === "Delete") alarm = undefined;
      }),
  };
  const holdNext = Effect.gen(function* () {
    const held = { entered: yield* Deferred.make<void>(), release: yield* Deferred.make<void>() };
    hold = held;
    return held;
  });
  return { store, actions, holdNext };
});

/** Storage in memory whose commits store nothing: the guard only wraps `commit`. */
const makeStorage = Effect.map(Effect.provide(AgentStorage, MemoryAgentStorage.layer), (memory) =>
  AgentStorage.of({ ...memory, commit: () => Effect.succeed({ stored: 1 }) }),
);

describe("AlarmGuard", () => {
  it.effect(
    "an idle wake computed before a live commit and applied after its arming keeps the alarm",
    () =>
      Effect.gen(function* () {
        const { store, actions, holdNext } = yield* makeStore;
        const guard = AlarmGuard.make(store, keepAliveMs);
        const committed = guard.wrap(yield* makeStorage);
        // The kernel computed an idle wake, then a commit that leaves a live task starts arming.
        const seen = guard.observe();
        const held = yield* holdNext;
        const commit = yield* Effect.forkChild(committed.commit([[live]], () => {}));
        yield* Deferred.await(held.entered);
        // The idle wake waits for the arming, then is applied after it.
        const applied = yield* Effect.forkChild(guard.apply(idle, seen, "stored"));
        yield* Deferred.succeed(held.release, undefined);
        yield* Fiber.join(commit);
        yield* Fiber.join(applied);
        assert.deepStrictEqual(actions, ["Set", "Keep"]);
      }),
  );

  it.effect(
    "the first wake applied after a live commit keeps the alarm, even one computed after it returned",
    () =>
      Effect.gen(function* () {
        const { store, actions } = yield* makeStore;
        const guard = AlarmGuard.make(store, keepAliveMs);
        yield* guard.wrap(yield* makeStorage).commit([[live]], () => {});
        // Computed once storage returned, before the kernel adopted the commit: it does not know the task.
        yield* guard.apply(idle, guard.observe(), "stored");
        // A later wake knows the commit.
        yield* guard.apply(idle, guard.observe(), "stored");
        assert.deepStrictEqual(actions, ["Set", "Keep", "Delete"]);
      }),
  );
});

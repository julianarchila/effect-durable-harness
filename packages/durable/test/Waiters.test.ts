import { assert, describe, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Waiters from "../src/internal/waiters.ts";

// Tests of the kernel's `Waiters`.

describe("waiters", () => {
  it.effect("resolve settles the waiters of one key only", () =>
    Effect.gen(function* () {
      const waiters = Waiters.make<string, number, string>();
      const a = yield* waiters.add("a").await.pipe(Effect.forkChild({ startImmediately: true }));
      const b = yield* waiters.add("b").await.pipe(Effect.forkChild({ startImmediately: true }));
      waiters.resolve("a", 1);
      assert.strictEqual(yield* Fiber.join(a), 1);
      assert.deepStrictEqual(waiters.keys(), ["b"]);
      assert.strictEqual(waiters.size(), 1);
      waiters.rejectAll("closed");
      assert.strictEqual(yield* Effect.flip(Fiber.join(b)), "closed");
      assert.strictEqual(waiters.size(), 0);
    }),
  );

  it.effect("an interrupted wait removes its waiter, and cancel removes one not awaited yet", () =>
    Effect.gen(function* () {
      const waiters = Waiters.make<string, number, string>();
      const fiber = yield* waiters
        .add("a")
        .await.pipe(Effect.forkChild({ startImmediately: true }));
      const other = waiters.add("a");
      assert.strictEqual(waiters.size(), 2);
      yield* Fiber.interrupt(fiber);
      const exit = yield* Fiber.await(fiber);
      assert.isTrue(Exit.hasInterrupts(exit));
      assert.strictEqual(waiters.size(), 1);
      other.cancel();
      assert.strictEqual(waiters.size(), 0);
      assert.deepStrictEqual(waiters.keys(), []);
    }),
  );

  it.effect("a waiter resolved in the resolver's step resumes after it", () =>
    Effect.gen(function* () {
      const waiters = Waiters.make<string, number, string>();
      const order: Array<string> = [];
      const fiber = yield* waiters.add("a").await.pipe(
        Effect.tap(() => Effect.sync(() => order.push("waiter"))),
        Effect.forkChild({ startImmediately: true }),
      );
      waiters.resolve("a", 1);
      order.push("resolver");
      yield* Fiber.join(fiber);
      assert.deepStrictEqual(order, ["resolver", "waiter"]);
    }),
  );
});

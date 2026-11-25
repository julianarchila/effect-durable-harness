import { assert, describe, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Scope from "effect/Scope";
import * as Signal from "../src/internal/signal.ts";

// Tests of the kernel's wakeup primitives.

/** Yields until `done` holds, a bounded number of times. */
const settle = (done: () => boolean) =>
  Effect.gen(function* () {
    for (let turn = 0; turn < 100 && !done(); turn++) yield* Effect.yieldNow;
    assert.isTrue(done());
  });

describe("signal", () => {
  it.effect("a waiter woken by doneAfterYield does not run on the completer's stack", () =>
    Effect.gen(function* () {
      const deferred = yield* Deferred.make<number>();
      const order: Array<string> = [];
      const waiter = yield* Deferred.await(deferred).pipe(
        Effect.tap(() => Effect.sync(() => order.push("waiter"))),
        Effect.forkChild({ startImmediately: true }),
      );
      Signal.doneAfterYield(deferred, Effect.succeed(1));
      order.push("completer");
      assert.strictEqual(yield* Fiber.join(waiter), 1);
      assert.deepStrictEqual(order, ["completer", "waiter"]);
    }),
  );

  it.effect("a loop runs after the firing step, and fires before it runs coalesce", () =>
    Effect.gen(function* () {
      const scope = yield* Scope.make();
      const signal = Signal.make();
      const order: Array<string> = [];
      yield* Signal.forkLoop(
        signal,
        Effect.sync(() => order.push("body")),
        scope,
      );
      yield* Effect.yieldNow;
      signal.fire();
      signal.fire();
      order.push("firer");
      yield* settle(() => order.length === 2);
      for (let turn = 0; turn < 10; turn++) yield* Effect.yieldNow;
      assert.deepStrictEqual(order, ["firer", "body"]);
      yield* Scope.close(scope, Exit.void);
    }),
  );

  it.effect("a fire while the body runs wakes the loop once more", () =>
    Effect.gen(function* () {
      const scope = yield* Scope.make();
      const signal = Signal.make();
      let runs = 0;
      yield* Signal.forkLoop(
        signal,
        Effect.sync(() => {
          runs++;
          if (runs === 1) signal.fire();
        }),
        scope,
      );
      yield* Effect.yieldNow;
      signal.fire();
      yield* settle(() => runs === 2);
      for (let turn = 0; turn < 10; turn++) yield* Effect.yieldNow;
      assert.strictEqual(runs, 2);
      yield* Scope.close(scope, Exit.void);
    }),
  );
});

import { assert, describe, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Line from "../src/internal/line.ts";

// Tests of the Session's mutation line. `SessionKernel.test.ts` covers it through the Session; these pin each
// guarantee on the primitive alone.

const open = () => undefined;

describe("mutation line", () => {
  it.effect("runs jobs in call order, one at a time, even when they suspend", () =>
    Effect.gen(function* () {
      const line = Line.make();
      const gate = yield* Deferred.make<void>();
      const order: Array<string> = [];
      const first = yield* line
        .run(
          Effect.sync(() => order.push("first start")).pipe(
            Effect.andThen(Deferred.await(gate)),
            Effect.andThen(Effect.sync(() => order.push("first end"))),
          ),
          open,
        )
        .pipe(Effect.forkChild({ startImmediately: true }));
      const rest = yield* Effect.forEach(["second", "third"], (name) =>
        line
          .run(
            Effect.sync(() => order.push(name)),
            open,
          )
          .pipe(Effect.forkChild({ startImmediately: true })),
      );
      assert.deepStrictEqual(order, ["first start"]);
      yield* Deferred.succeed(gate, undefined);
      yield* Fiber.join(first);
      yield* Fiber.joinAll(rest);
      assert.deepStrictEqual(order, ["first start", "first end", "second", "third"]);
    }),
  );

  it.effect(
    "checks admission in the step that queues: a job queued before refusal still runs",
    () =>
      Effect.gen(function* () {
        const line = Line.make();
        const gate = yield* Deferred.make<void>();
        let closed = false;
        const refuse = () => (closed ? "refused" : undefined);
        const holder = yield* line
          .run(Deferred.await(gate), refuse)
          .pipe(Effect.forkChild({ startImmediately: true }));
        const queued = yield* line
          .run(Effect.succeed("ran"), refuse)
          .pipe(Effect.forkChild({ startImmediately: true }));
        closed = true;
        const late = yield* Effect.flip(line.run(Effect.succeed("ran"), refuse));
        assert.strictEqual(late, "refused");
        yield* Deferred.succeed(gate, undefined);
        yield* Fiber.join(holder);
        assert.strictEqual(yield* Fiber.join(queued), "ran");
      }),
  );

  it.effect("a waiter interrupted in the queue leaves it, and the line passes on", () =>
    Effect.gen(function* () {
      const line = Line.make();
      const gate = yield* Deferred.make<void>();
      const holder = yield* line
        .run(Deferred.await(gate), open)
        .pipe(Effect.forkChild({ startImmediately: true }));
      const interrupted = yield* line
        .run(Effect.void, open)
        .pipe(Effect.forkChild({ startImmediately: true }));
      const last = yield* line
        .run(Effect.succeed("last"), open)
        .pipe(Effect.forkChild({ startImmediately: true }));
      yield* Fiber.interrupt(interrupted);
      yield* Deferred.succeed(gate, undefined);
      yield* Fiber.join(holder);
      assert.strictEqual(yield* Fiber.join(last), "last");
      // The line is free again.
      assert.strictEqual(yield* line.run(Effect.succeed("after"), open), "after");
    }),
  );

  it.effect("a long queue of jobs that never suspend does not grow the stack", () =>
    Effect.gen(function* () {
      const line = Line.make();
      const gate = yield* Deferred.make<void>();
      const holder = yield* line
        .run(Deferred.await(gate), open)
        .pipe(Effect.forkChild({ startImmediately: true }));
      let ran = 0;
      const jobs = yield* Effect.forEach(Array.from({ length: 20_000 }), () =>
        line
          .run(
            Effect.sync(() => {
              ran++;
            }),
            open,
          )
          .pipe(Effect.forkChild({ startImmediately: true })),
      );
      yield* Deferred.succeed(gate, undefined);
      yield* Fiber.join(holder);
      yield* Fiber.joinAll(jobs);
      assert.strictEqual(ran, 20_000);
    }),
  );
});

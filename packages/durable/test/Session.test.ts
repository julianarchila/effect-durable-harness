import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Doc from "../src/Doc.ts";
import * as MemoryAgentStorage from "../src/MemoryAgentStorage.ts";
import { Session, layer } from "../src/Session.ts";
import { Tx } from "../src/Tx.ts";

// The public layer over the kernel: the kernel's behavior is covered by the
// kernel suites; this checks the wiring and the scoped subscription.

const CounterDoc = Doc.make({
  checkpointWhen: () => false,
  kind: "test.counter",
  version: 1,
  scope: "session",
  schema: Schema.Struct({ count: Schema.Number }),
  initial: () => ({ count: 0 }),
});

const increment = Effect.gen(function* () {
  const session = yield* Session;
  yield* session.commit(
    Effect.gen(function* () {
      const tx = yield* Tx;
      (yield* tx.doc(CounterDoc)).count++;
    }),
  );
});

it.effect("Session.layer commits, reads and detaches a commit observer", () =>
  Effect.gen(function* () {
    const session = yield* Session;
    let published = 0;
    const observation = yield* session.observeCommits({
      advance: () => {
        published++;
      },
      end: () => {},
    });
    yield* increment;
    observation.detach();
    yield* increment;
    assert.strictEqual(published, 1);
    assert.deepStrictEqual(Option.getOrUndefined(yield* session.snapshot(CounterDoc)), {
      count: 2,
    });
  }).pipe(Effect.provide(layer().pipe(Layer.provide(MemoryAgentStorage.layer)))),
);

it.effect("closing the layer's scope closes the Session", () =>
  Effect.gen(function* () {
    const closed = yield* Effect.scoped(
      Effect.gen(function* () {
        const session = yield* Session;
        return { awaitClosed: session.closed };
      }).pipe(Effect.provide(layer().pipe(Layer.provide(MemoryAgentStorage.layer)))),
    );
    assert.deepStrictEqual(yield* closed.awaitClosed, { reason: "closed" });
  }),
);

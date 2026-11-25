import { assert, describe, expectTypeOf, it } from "@effect/vitest";
import * as Cause from "effect/Cause";
import * as Context from "effect/Context";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Tracer from "effect/Tracer";
import * as Schema from "effect/Schema";
import type { AgentStorage, StorageFailure } from "../src/AgentStorage.ts";
import * as Extension from "../src/Extension.ts";
import * as Harness from "../src/Harness.ts";
import * as MemoryAgentStorage from "../src/MemoryAgentStorage.ts";
import type { Models } from "../src/Models.ts";
import * as FauxModel from "../src/testing/FauxModel.ts";
import { Session } from "../src/Session.ts";
import type { SessionFailed } from "../src/SessionError.ts";
import * as Task from "../src/Task.ts";
import { Tx } from "../src/Tx.ts";
import { eventually, openHarness } from "./HarnessSupport.ts";
import { controlledStorage, flush } from "./SessionSupport.ts";

// The public layer over the scheduler: its behavior is covered by the Harness suites; this checks the wiring, and
// that the handlers' requirements are the layer's.

class Doubler extends Context.Service<
  Doubler,
  { readonly double: (value: number) => Effect.Effect<number> }
>()("@effect-durable-harness/core/test/Harness.test/Doubler") {}

const Double = Task.make({
  name: "test.double",
  version: 1,
  input: Schema.Struct({ value: Schema.Number }),
  checkpoint: Schema.Struct({ phase: Schema.Literal("run") }),
  result: Schema.Number,
  initial: () => ({ phase: "run" }),
  phases: {
    run: (task, runtime) =>
      Effect.gen(function* () {
        const doubler = yield* Doubler;
        const result = yield* doubler.double(task.input.value);
        yield* runtime.commit(() =>
          Effect.succeed({ status: "terminal", outcome: { status: "completed", result } } as const),
        );
      }),
  },
  abort: (_task, runtime) =>
    runtime.commit(() =>
      Effect.succeed({ status: "terminal", outcome: { status: "aborted" } } as const),
    ),
});

/** Reports a failure that fails nothing, then completes. */
const Reporter = Task.make({
  name: "test.reporter",
  version: 1,
  input: Schema.Null,
  checkpoint: Schema.Struct({ phase: Schema.Literal("run") }),
  result: Schema.Null,
  initial: () => ({ phase: "run" }),
  phases: {
    run: (_task, runtime) =>
      runtime.report(Cause.die(new Error("important"))).pipe(
        Effect.andThen(
          runtime.commit(() =>
            Effect.succeed({
              status: "terminal",
              outcome: { status: "completed", result: null },
            } as const),
          ),
        ),
      ),
  },
  abort: (_task, runtime) =>
    runtime.commit(() =>
      Effect.succeed({ status: "terminal", outcome: { status: "aborted" } } as const),
    ),
});

const Math = Extension.make({ name: "test.math", tasks: [Double] });
const HarnessLayer = Harness.layer({ extensions: [Math] });

describe("Harness.layer", () => {
  it("requires the services of its handlers", () => {
    expectTypeOf(HarnessLayer).toEqualTypeOf<
      Layer.Layer<
        Harness.Harness | Session,
        StorageFailure | SessionFailed,
        AgentStorage | Models | Doubler
      >
    >();
  });

  it.effect("runs a task with the services provided to the layer", () =>
    Effect.gen(function* () {
      const harness = yield* Harness.Harness;
      const root = yield* harness.ensureRoot();
      const id = yield* root.commit(
        Effect.gen(function* () {
          const tx = yield* Tx;
          return yield* tx.createTask(
            Double,
            { value: 21 },
            { ownership: { kind: "conversation" } },
          );
        }),
      );
      const settled = yield* harness.awaitTask(id);
      expectTypeOf(settled.state.outcome).toExtend<Task.TaskOutcome<number>>();
      assert.deepStrictEqual(settled.state.outcome, { status: "completed", result: 42 });
    }).pipe(
      Effect.provide(
        HarnessLayer.pipe(
          Layer.provide(MemoryAgentStorage.layer),
          Layer.provide(FauxModel.make().layer),
          Layer.provide(Layer.succeed(Doubler, { double: (value) => Effect.succeed(value * 2) })),
        ),
      ),
    ),
  );

  it.effect("closing the layer's scope closes the Harness and its Session", () =>
    Effect.gen(function* () {
      const opened = yield* Effect.scoped(
        Effect.gen(function* () {
          const harness = yield* Harness.Harness;
          const session = yield* Session;
          return { harness: harness.closed, session: session.closed };
        }).pipe(
          Effect.provide(
            Harness.layer({ extensions: [] }).pipe(
              Layer.provide(MemoryAgentStorage.layer),
              Layer.provide(FauxModel.make().layer),
            ),
          ),
        ),
      );
      assert.deepStrictEqual(yield* opened.harness, { reason: "closed" });
      assert.deepStrictEqual(yield* opened.session, { reason: "closed" });
    }),
  );

  it.effect("delivers a report still in flight when the Harness closes", () =>
    Effect.gen(function* () {
      const storage = yield* controlledStorage;
      const delivering = yield* Deferred.make<void>();
      const proceed = yield* Deferred.make<void>();
      const delivered: Array<string> = [];
      const h = yield* openHarness(storage, [Reporter], {
        onReport: (report) =>
          Deferred.succeed(delivering, undefined).pipe(
            Effect.andThen(Deferred.await(proceed)),
            Effect.andThen(Effect.sync(() => delivered.push(Cause.pretty(report.cause)))),
          ),
      });
      const root = yield* h.harness.ensureRoot();
      const id = yield* root.commit(
        Effect.gen(function* () {
          const tx = yield* Tx;
          return yield* tx.createTask(Reporter, null, { ownership: { kind: "conversation" } });
        }),
      );
      yield* h.harness.resume;
      yield* h.harness.awaitTask(id);
      yield* Deferred.await(delivering);
      // A report is synchronous: it is never lost to a close that starts during it.
      const closing = yield* Effect.forkChild(h.close, { startImmediately: true });
      yield* flush;
      yield* Deferred.succeed(proceed, undefined);
      yield* Fiber.join(closing);
      assert.strictEqual(delivered.length, 1);
      assert.include(delivered[0], "important");
    }).pipe(Effect.provide(MemoryAgentStorage.layer), Effect.scoped),
  );

  it.effect(
    "traces each task phase with its identity, under the span that created the task",
    () => {
      const spans: Array<Tracer.NativeSpan> = [];
      const tracer = Tracer.make({
        span: (options) => {
          const span = new Tracer.NativeSpan(options);
          spans.push(span);
          return span;
        },
      });
      return Effect.gen(function* () {
        const harness = yield* Harness.Harness;
        const root = yield* harness.ensureRoot();
        const id = yield* root
          .commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              return yield* tx.createTask(
                Double,
                { value: 1 },
                { ownership: { kind: "conversation" } },
              );
            }),
          )
          .pipe(Effect.withSpan("creator"));
        yield* harness.awaitTask(id);
        // Group commit resolves the wait at publication, before the phase's span ends.
        yield* eventually(
          Effect.sync(() =>
            spans.some((span) => span.name === "durable.phase" && span.status._tag === "Ended"),
          ),
        );
        const creator = spans.find((span) => span.name === "creator");
        const phase = spans.find((span) => span.name === "durable.phase");
        assert.deepStrictEqual(Object.fromEntries(phase?.attributes ?? []), {
          "durable.task.id": id,
          "durable.task.name": "test.double",
          "durable.conversation.id": root.id,
          "durable.phase.name": "run",
          "durable.phase.kind": "run",
          "durable.phase.resumed": false,
          "durable.trace.id": creator?.traceId,
          "durable.phase.outcome": "done",
        });
        assert.strictEqual(
          phase?.parent.pipe(
            Option.map((parent) => parent.spanId),
            Option.getOrUndefined,
          ),
          creator?.spanId,
        );
      }).pipe(
        Effect.provide(
          HarnessLayer.pipe(
            Layer.provide(MemoryAgentStorage.layer),
            Layer.provide(FauxModel.make().layer),
            Layer.provide(Layer.succeed(Doubler, { double: (value) => Effect.succeed(value * 2) })),
          ),
        ),
        Effect.withTracer(tracer),
      );
    },
  );
});

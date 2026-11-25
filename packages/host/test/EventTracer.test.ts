// The object's tracer (`src/internal/tracer.ts`): the Harness's fibers trace into the event in progress, or
// else the latest one, and a tracer that throws gives way to an Effect-local span. A span that ends after a later
// event started records its outcome on an open event.
import { assert, describe, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as Tracer from "effect/Tracer";
import * as EventTracer from "../src/internal/tracer.ts";

const recording = (name: string) => {
  const spans: Array<string> = [];
  const tracer = Tracer.make({
    span: (options) => {
      spans.push(`${name}:${options.name}`);
      return new Tracer.NativeSpan(options);
    },
  });
  return { spans, tracer };
};

/** A tracer that keeps every span it started, to read their attributes, parents and exits. */
const keeping = () => {
  const spans: Array<Tracer.NativeSpan> = [];
  const tracer = Tracer.make({
    span: (options) => {
      const span = new Tracer.NativeSpan(options);
      spans.push(span);
      return span;
    },
  });
  const named = (name: string) => spans.filter((span) => span.name === name);
  return { spans, tracer, named };
};

const exitOf = (span: Tracer.NativeSpan) =>
  span.status._tag === "Ended" ? span.status.exit : undefined;

/**
 * Runs `phase` in the background on the object's tracer, started while `first` is the event in progress; returns
 * a gate that lets the phase end.
 */
const backgroundPhase = (events: EventTracer.EventTracer, first: Tracer.Tracer) =>
  Effect.gen(function* () {
    const gate = yield* Deferred.make<void>();
    const started = yield* Deferred.make<void>();
    const fiber = yield* events
      .during(
        Effect.forkDetach(
          Effect.gen(function* () {
            yield* Effect.annotateCurrentSpan("durable.task.id", 7);
            yield* Deferred.succeed(started, undefined);
            yield* Deferred.await(gate);
            yield* Effect.annotateCurrentSpan("gen_ai.usage.output_tokens", 42);
            yield* Effect.void.pipe(Effect.withSpan("child"));
          }).pipe(Effect.withSpan("phase"), Effect.withTracer(events.tracer)),
        ),
      )
      .pipe(Effect.withTracer(first));
    yield* Deferred.await(started);
    return { gate, fiber };
  });

describe("EventTracer", () => {
  it.effect("starts spans on the event in progress, then on the latest one", () =>
    Effect.gen(function* () {
      const events = EventTracer.make();
      const first = recording("first");
      const second = recording("second");
      const background = (name: string) =>
        Effect.void.pipe(Effect.withSpan(name), Effect.withTracer(events.tracer));
      yield* background("before");
      yield* events.during(background("during first")).pipe(Effect.withTracer(first.tracer));
      yield* events
        .during(events.during(background("nested")).pipe(Effect.withTracer(second.tracer)))
        .pipe(Effect.withTracer(first.tracer));
      yield* background("after");
      assert.deepStrictEqual(first.spans, ["first:during first"]);
      assert.deepStrictEqual(second.spans, ["second:nested", "second:after"]);
    }),
  );

  it.effect(
    "spreads background spans over the events in progress, and keeps a child in its parent's",
    () =>
      Effect.gen(function* () {
        // With every background span on the latest event, one invocation's trace took the work of every
        // concurrent turn and crossed Workers Observability's limit, which drops the invocation and most of its spans.
        const events = EventTracer.make();
        const first = recording("first");
        const second = recording("second");
        const background = (name: string) =>
          Effect.void.pipe(Effect.withSpan(name), Effect.withTracer(events.tracer));
        const parentWithChild = (name: string) =>
          Effect.void.pipe(
            Effect.withSpan(`${name} child`),
            Effect.withSpan(name),
            Effect.withTracer(events.tracer),
          );
        yield* events
          .during(
            events
              .during(
                Effect.gen(function* () {
                  for (let index = 0; index < 4; index++) yield* background(`root ${index}`);
                  yield* parentWithChild("parent");
                }),
              )
              .pipe(Effect.withTracer(second.tracer)),
          )
          .pipe(Effect.withTracer(first.tracer));
        // Ties go to the latest event in progress, as before; then each root span goes to the less loaded one.
        assert.deepStrictEqual(first.spans, ["first:root 1", "first:root 3"]);
        assert.deepStrictEqual(second.spans, [
          "second:root 0",
          "second:root 2",
          "second:parent",
          "second:parent child",
        ]);
      }),
  );

  it.effect("starts spans on the constructing event's tracer until a handler runs", () =>
    Effect.gen(function* () {
      const events = EventTracer.make();
      const constructing = recording("constructing");
      const event = recording("event");
      const background = (name: string) =>
        Effect.void.pipe(Effect.withSpan(name), Effect.withTracer(events.tracer));
      events.construct(constructing.tracer);
      yield* background("recovery");
      yield* events.during(Effect.void).pipe(Effect.withTracer(event.tracer));
      yield* background("later");
      assert.deepStrictEqual(constructing.spans, ["constructing:recovery"]);
      assert.deepStrictEqual(event.spans, ["event:later"]);
    }),
  );

  it.effect("keeps an Effect-local span when the event's tracer throws", () =>
    Effect.gen(function* () {
      const events = EventTracer.make();
      const failing = Tracer.make({
        span: () => {
          throw new Error("context gone");
        },
      });
      const name = yield* events
        .during(
          Effect.currentSpan.pipe(
            Effect.map((span) => span.name),
            Effect.withSpan("phase"),
            Effect.withTracer(events.tracer),
          ),
        )
        .pipe(Effect.withTracer(failing));
      assert.strictEqual(name, "phase");
    }),
  );

  it.effect("wraps an object's Effect members and leaves the rest", () =>
    Effect.gen(function* () {
      const events = EventTracer.make();
      const event = recording("event");
      const members = EventTracer.tracedMembers(
        {
          call: () => Effect.void.pipe(Effect.withSpan("call"), Effect.withTracer(events.tracer)),
          value: Effect.void.pipe(Effect.withSpan("value"), Effect.withTracer(events.tracer)),
          plain: () => 1,
        },
        events,
      );
      yield* members.call().pipe(Effect.withTracer(event.tracer));
      yield* members.value.pipe(Effect.withTracer(event.tracer));
      assert.strictEqual(members.plain(), 1);
      assert.deepStrictEqual(event.spans, ["event:call", "event:value"]);
    }),
  );

  it.effect(
    "records the outcome of a span that ends after a later event started, on an open event",
    () =>
      Effect.gen(function* () {
        const events = EventTracer.make();
        const first = keeping();
        const second = keeping();
        const { gate, fiber } = yield* backgroundPhase(events, first.tracer);
        // The first event's handler has returned; a later event starts and returns too.
        yield* events.during(Effect.void).pipe(Effect.withTracer(second.tracer));
        yield* Deferred.succeed(gate, undefined);
        yield* Fiber.join(fiber);

        const [phase] = first.named("phase");
        assert.isDefined(phase);
        assert.isTrue(Exit.isSuccess(exitOf(phase!)!));
        assert.deepStrictEqual(first.named("phase outcome"), []);
        const [outcome] = second.named("phase outcome");
        assert.isDefined(outcome);
        assert.isTrue(Option.isNone(outcome!.parent));
        assert.isTrue(Exit.isSuccess(exitOf(outcome!)!));
        assert.strictEqual(outcome!.attributes.get("durable.task.id"), 7);
        assert.strictEqual(outcome!.attributes.get("gen_ai.usage.output_tokens"), 42);
        assert.strictEqual(outcome!.attributes.get("durable.trace.id"), phase!.traceId);
        assert.strictEqual(outcome!.attributes.get("durable.span.id"), phase!.spanId);
        assert.strictEqual(outcome!.attributes.get("durable.outcome_of"), "phase");
        assert.isNumber(outcome!.attributes.get("durable.span.started_at"));
        assert.isNumber(outcome!.attributes.get("durable.span.duration_ms"));
        assert.deepStrictEqual(
          outcome!.links.map((link) => link.span.spanId),
          [phase!.spanId],
        );
      }),
  );

  it.effect(
    "records an outcome on the least loaded event in progress, not the most recently entered one",
    () =>
      Effect.gen(function* () {
        const events = EventTracer.make();
        const first = keeping();
        const idle = keeping();
        const busy = keeping();
        const { gate, fiber } = yield* backgroundPhase(events, first.tracer);
        yield* events
          .during(
            events
              .during(
                Effect.gen(function* () {
                  // A tie goes to the handler entered last (`busy`), and the child follows its parent there.
                  yield* Effect.void.pipe(
                    Effect.withSpan("load child"),
                    Effect.withSpan("load"),
                    Effect.withTracer(events.tracer),
                  );
                  yield* Deferred.succeed(gate, undefined);
                  yield* Fiber.join(fiber);
                }),
              )
              .pipe(Effect.withTracer(busy.tracer)),
          )
          .pipe(Effect.withTracer(idle.tracer));

        assert.strictEqual(busy.named("load").length, 1);
        assert.strictEqual(busy.named("load child").length, 1);
        assert.deepStrictEqual(busy.named("phase outcome"), []);
        assert.strictEqual(idle.named("phase outcome").length, 1);
      }),
  );

  it.effect(
    "starts the child of a span whose event was superseded on the open event, under the same ids",
    () =>
      Effect.gen(function* () {
        const events = EventTracer.make();
        const first = keeping();
        const second = keeping();
        const { gate, fiber } = yield* backgroundPhase(events, first.tracer);
        yield* events.during(Effect.void).pipe(Effect.withTracer(second.tracer));
        yield* Deferred.succeed(gate, undefined);
        yield* Fiber.join(fiber);

        const [phase] = first.named("phase");
        assert.deepStrictEqual(first.named("child"), []);
        const [child] = second.named("child");
        assert.isDefined(child);
        const parent = Option.getOrUndefined(child!.parent);
        assert.strictEqual(parent?._tag, "ExternalSpan");
        assert.strictEqual(parent?.spanId, phase!.spanId);
        assert.strictEqual(child!.traceId, phase!.traceId);
        // The child ended inside the open event: no outcome of its own.
        assert.deepStrictEqual(second.named("child outcome"), []);
      }),
  );

  it.effect(
    "records no outcome while the span's event is the latest, or its handler still runs",
    () =>
      Effect.gen(function* () {
        const events = EventTracer.make();
        const first = keeping();
        const second = keeping();
        // Ends after its event's handler returned, with no later event: the event's trace is still open.
        const latest = yield* backgroundPhase(events, first.tracer);
        yield* Deferred.succeed(latest.gate, undefined);
        yield* Fiber.join(latest.fiber);
        // Ends while its event's handler runs, though a later event started meanwhile.
        yield* events
          .during(
            Effect.gen(function* () {
              yield* events.during(Effect.void).pipe(Effect.withTracer(second.tracer));
              yield* Effect.void.pipe(Effect.withSpan("inside"), Effect.withTracer(events.tracer));
            }),
          )
          .pipe(Effect.withTracer(first.tracer));
        assert.strictEqual(first.named("phase").length, 1);
        assert.strictEqual(first.named("inside").length, 1);
        assert.deepStrictEqual(
          [...first.spans, ...second.spans].filter((span) => span.name.endsWith(" outcome")),
          [],
        );
      }),
  );

  it.effect("treats the constructing event and the first handler's as one invocation", () =>
    Effect.gen(function* () {
      const events = EventTracer.make();
      const constructing = keeping();
      const handler = keeping();
      const gate = yield* Deferred.make<void>();
      events.construct(constructing.tracer);
      const fiber = yield* Effect.forkDetach(
        Deferred.await(gate).pipe(Effect.withSpan("recovery"), Effect.withTracer(events.tracer)),
      );
      yield* Effect.yieldNow;
      yield* events
        .during(Deferred.succeed(gate, undefined))
        .pipe(Effect.withTracer(handler.tracer));
      yield* Fiber.join(fiber);
      assert.strictEqual(constructing.named("recovery").length, 1);
      assert.deepStrictEqual(handler.named("recovery outcome"), []);
    }),
  );

  it.effect("loses only the outcome when the open event's tracer throws", () =>
    Effect.gen(function* () {
      const events = EventTracer.make();
      const first = keeping();
      const throwing = Tracer.make({
        span: () => {
          throw new Error("context gone");
        },
      });
      const { gate, fiber } = yield* backgroundPhase(events, first.tracer);
      yield* events.during(Effect.void).pipe(Effect.withTracer(throwing));
      yield* Deferred.succeed(gate, undefined);
      const exit = yield* Fiber.await(fiber);
      assert.isTrue(Exit.isSuccess(exit));
      assert.strictEqual(first.named("phase").length, 1);
    }),
  );
});

// The test Worker's telemetry, recorded: every event's tracer is the one registered before (Cloudflare's),
// wrapped so each span that ends is kept in this isolate with the number of the event whose tracer started it. The
// `AgentProbe` reads them back (`recordedSpans`), so a test can see which event's tracer got a span and what it
// carried. The workerd files run one at a time and each deploys its own stack, so the isolate holds one file's spans.
import { RuntimeContext } from "alchemy";
import * as Cause from "effect/Cause";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Predicate from "effect/Predicate";
import * as Tracer from "effect/Tracer";

/** One ended span, as the event's tracer saw it. */
export interface RecordedSpan {
  readonly name: string;
  /** The event whose tracer started the span, numbered from 1 in this isolate. */
  readonly event: number;
  readonly attributes: Record<string, string | number | boolean>;
  readonly exit: "success" | "failure" | "interrupted";
}

const recorded: Array<RecordedSpan> = [];
let events = 0;

/** The spans recorded so far in this isolate. */
export const recordedSpans = (): ReadonlyArray<RecordedSpan> => recorded;

/** A span of the wrapped tracer that records itself when it ends. Its parent is handed on unwrapped. */
class RecordingSpan implements Tracer.Span {
  readonly _tag = "Span";
  readonly inner: Tracer.Span;
  readonly eventNumber: number;
  readonly parent: Option.Option<Tracer.AnySpan>;
  constructor(inner: Tracer.Span, eventNumber: number, parent: Option.Option<Tracer.AnySpan>) {
    this.inner = inner;
    this.eventNumber = eventNumber;
    this.parent = parent;
  }
  get name() {
    return this.inner.name;
  }
  get spanId() {
    return this.inner.spanId;
  }
  get traceId() {
    return this.inner.traceId;
  }
  get annotations() {
    return this.inner.annotations;
  }
  get status() {
    return this.inner.status;
  }
  get attributes() {
    return this.inner.attributes;
  }
  get links() {
    return this.inner.links;
  }
  get sampled() {
    return this.inner.sampled;
  }
  get kind() {
    return this.inner.kind;
  }
  end(endTime: bigint, exit: Exit.Exit<unknown, unknown>) {
    this.inner.end(endTime, exit);
    const attributes: Record<string, string | number | boolean> = {};
    for (const [key, value] of this.inner.attributes) {
      if (Predicate.isString(value) || Predicate.isNumber(value) || Predicate.isBoolean(value)) {
        attributes[key] = value;
      }
    }
    recorded.push({
      name: this.inner.name,
      event: this.eventNumber,
      attributes,
      exit: Exit.isSuccess(exit)
        ? "success"
        : Cause.hasInterruptsOnly(exit.cause)
          ? "interrupted"
          : "failure",
    });
  }
  // oxlint-disable-next-line anti-slop/no-unknown-parameters -- Effect's `Tracer.Span` contract
  attribute(key: string, value: unknown) {
    this.inner.attribute(key, value);
  }
  // oxlint-disable-next-line anti-slop/no-unsafe-dictionary-type -- Effect's `Tracer.Span` contract
  event(name: string, startTime: bigint, attributes?: Record<string, unknown>) {
    this.inner.event(name, startTime, attributes);
  }
  addLinks(links: ReadonlyArray<Tracer.SpanLink>) {
    this.inner.addLinks(links);
  }
}

const recording = (inner: Tracer.Tracer): Tracer.Tracer => {
  const event = ++events;
  return Tracer.make({
    span: (options) => {
      const parent = Option.getOrUndefined(options.parent);
      const span = inner.span(
        parent instanceof RecordingSpan
          ? { ...options, parent: Option.some(parent.inner) }
          : options,
      );
      return new RecordingSpan(span, event, options.parent);
    },
  });
};

/**
 * Wraps the telemetry registered so far (`Cloudflare.Telemetry()`'s) for every event. Built after it: provide it
 * inside the provide that registers it.
 */
export const layer: Layer.Layer<never> = Layer.effectDiscard(
  Effect.gen(function* () {
    const runtime = yield* Effect.serviceOption(RuntimeContext);
    if (Option.isNone(runtime)) return;
    const previous = runtime.value.telemetry ?? Layer.empty;
    runtime.value.telemetry = Layer.merge(
      previous,
      Layer.effect(
        Tracer.Tracer,
        Effect.map(Effect.context<never>(), (context) =>
          recording(Context.get(context, Tracer.Tracer)),
        ),
      ).pipe(Layer.provide(previous)),
    );
  }),
);

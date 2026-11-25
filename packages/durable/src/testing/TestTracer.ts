/**
 * An in-memory `Tracer` and `MetricRegistry` for tests of the Harness's telemetry: provide `layer` around the code
 * under test, then read the spans it recorded, in start order, and the metrics' current values.
 *
 * ```ts
 * const traced = TestTracer.make();
 * yield* program.pipe(Effect.provide(traced.layer));
 * const phases = traced.spans.filter((span) => span.name === "durable.phase");
 * ```
 *
 * @since 0.0.0
 */
import * as Context from "effect/Context";
import * as Layer from "effect/Layer";
import * as Metric from "effect/Metric";
import * as Tracer from "effect/Tracer";

/**
 * The recorded spans and metrics, and the layer that records them.
 *
 * @category models
 * @since 0.0.0
 */
export interface TestTracer {
  /** The spans started under `layer`, in start order. */
  readonly spans: ReadonlyArray<Tracer.NativeSpan>;
  /** The `Tracer` and a fresh `MetricRegistry`. */
  readonly layer: Layer.Layer<never>;
  /** The current value of every metric updated under `layer`, one per name and attribute set. */
  readonly metrics: () => ReadonlyArray<Metric.Metric.Snapshot>;
}

/**
 * What a `Tracer` gets to start a span, as `NativeSpan`'s constructor takes it.
 *
 * @category models
 * @since 0.0.0
 */
export type SpanInit = Parameters<Tracer.Tracer["span"]>[0];

/**
 * Options of `make`.
 *
 * @category models
 * @since 0.0.0
 */
export interface TestTracerOptions {
  /** Builds each span; the default is Effect's `NativeSpan`. A test of a failing exporter returns one that throws. */
  readonly span?: ((options: SpanInit) => Tracer.NativeSpan) | undefined;
  /** The registry the metrics go to; a test of a failing exporter passes one that throws. */
  readonly registry?: Metric.MetricRegistry | undefined;
}

/**
 * Makes a recording tracer with a fresh metric registry.
 *
 * @category constructors
 * @since 0.0.0
 */
export const make = (options: TestTracerOptions = {}): TestTracer => {
  const spans: Array<Tracer.NativeSpan> = [];
  const registry: Metric.MetricRegistry = options.registry ?? new Map();
  const build = options.span ?? ((span) => new Tracer.NativeSpan(span));
  const tracer = Tracer.make({
    span: (span) => {
      const recorded = build(span);
      spans.push(recorded);
      return recorded;
    },
  });
  return {
    spans,
    layer: Layer.mergeAll(
      Layer.succeed(Tracer.Tracer, tracer),
      Layer.succeed(Metric.MetricRegistry, registry),
    ),
    metrics: () => Metric.snapshotUnsafe(Context.make(Metric.MetricRegistry, registry)),
  };
};

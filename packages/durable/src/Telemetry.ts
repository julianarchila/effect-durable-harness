/**
 * The kernel's telemetry vocabulary: the content-free spans, span events and
 * metrics the Harness records through Effect's `Tracer` and `Metric`, and the trace carrier it stores on
 * submissions and tasks.
 *
 * The kernel never chooses an exporter: a host provides a `Tracer` (`Cloudflare.Telemetry()` in
 * `@effect-durable-harness/cloudflare`, `OtlpTracer` elsewhere) and reads the metrics from Effect's `MetricRegistry`. Every
 * attribute is a scalar that names a durable record or a fixed category; no prompt, model output, tool payload or
 * error message is recorded as an attribute or event.
 *
 * **Spans**
 *
 * - `durable.submit`: a public submission's admission (`Conversation.submit`, `Conversation.admit`). An input or a
 *   continue starts a root trace linked to its caller; a write is its caller's child. Start: `durable.conversation.id`,
 *   `durable.submission.type`; end: `durable.submission.id`, `durable.submission.result` (`queued`, `placed`, `done`).
 * - `durable.phase`: one phase invocation of a task, its decision commit included, never a wait or a restart. Its
 *   parent is the task's stored carrier; the first invocation links the task's stored links. Start:
 *   `durable.task.id`, `durable.task.name`, `durable.conversation.id`, `durable.phase.name`, `durable.phase.kind` (`run`,
 *   `abort`), `durable.phase.resumed`, `durable.phase.attempt`; end: `durable.phase.outcome` (`next=<phase>`, `waiting`,
 *   `sleeping`, `done`, `faulted`, `aborted`), `durable.tool.name` and `durable.tool.call_id` for a tool call. A phase
 *   whose task ended `faulted`, `failed` or `orphaned` exits with a failure, `durable.phase.error` naming the category
 *   (`task_faulted`, `task_failed`, `task_orphaned`); an aborted one exits with success.
 * - Both carry `durable.trace.id`, the span's Effect trace id, and, when they link other traces, `durable.trace.links`,
 *   their ids joined by commas. A backend that keeps only scalar attributes, as Cloudflare's does, follows a trace
 *   across invocations with them.
 *
 * **Span events**, on the submit or phase span of the commit's caller (or the caller's own span):
 *
 * - `durable.entry` (`durable.entry.id`, `durable.entry.kind`) and `durable.submission` (`durable.submission.id`,
 *   `durable.submission.status`, `durable.answer.entry_id`) once a commit is stored; never for a rolled-back one.
 * - `durable.checkpoint` (`durable.phase.name`): the running checkpoint's phase changed inside a phase.
 * - `durable.report` (`durable.report.type`, `durable.report.reason`, `durable.report.extension`, `durable.task.id`): a report.
 *
 * **Model requests** follow Effect AI's conventions, OpenTelemetry's GenAI attributes (`effect/ai/Telemetry`):
 * `durable.generation.request` is one model request, a child of the `request` phase,
 * with `gen_ai.provider.name`, `gen_ai.operation.name` (`chat`), `gen_ai.request.model`, `gen_ai.response.id`,
 * `gen_ai.response.model`, `gen_ai.response.finish_reasons`, `gen_ai.usage.input_tokens` and
 * `gen_ai.usage.output_tokens`, besides `durable.task.id`, `durable.conversation.id` and `durable.generation.attempt`. Never
 * the messages. A request that ends with stop reason `error` exits with a failure.
 *
 * @since 0.0.0
 */
import * as Metric from "effect/Metric";
import * as Schema from "effect/Schema";
import * as Tracer from "effect/Tracer";

/**
 * The stored identity of a span, for a later invocation to continue or link its trace:
 * Effect's trace and span ids and the sampling decision. A submission keeps its admission span's; a task the
 * span that created it, unless created with `trace: "root"`.
 *
 * @category models
 * @since 0.0.0
 */
export const TraceCarrier = Schema.Struct({
  traceId: Schema.String,
  spanId: Schema.String,
  sampled: Schema.optionalKey(Schema.Boolean),
});

/**
 * @category models
 * @since 0.0.0
 */
export type TraceCarrier = typeof TraceCarrier.Type;

/**
 * The carrier of a span.
 *
 * @category constructors
 * @since 0.0.0
 */
export const carrierOf = (span: Tracer.AnySpan): TraceCarrier =>
  span.sampled
    ? { traceId: span.traceId, spanId: span.spanId }
    : { traceId: span.traceId, spanId: span.spanId, sampled: false };

/**
 * The span a carrier stands for, as the parent or the link of a span.
 *
 * @category constructors
 * @since 0.0.0
 */
export const spanOf = (carrier: TraceCarrier): Tracer.ExternalSpan =>
  Tracer.externalSpan({
    traceId: carrier.traceId,
    spanId: carrier.spanId,
    sampled: carrier.sampled ?? true,
  });

/**
 * Span names.
 *
 * @category constants
 * @since 0.0.0
 */
export const Spans = {
  submit: "durable.submit",
  phase: "durable.phase",
} as const;

/**
 * Span event names. `replay` marks a nested call that returned a result an earlier attempt stored.
 *
 * @category constants
 * @since 0.0.0
 */
export const Events = {
  entry: "durable.entry",
  submission: "durable.submission",
  checkpoint: "durable.checkpoint",
  report: "durable.report",
  replay: "durable.replay",
} as const;

/**
 * Reports raised, by their fixed kind: attribute `type`. Each function returns the
 * metric of one attribute set; read them all from the `MetricRegistry`, as an exporter does.
 *
 * @category metrics
 * @since 0.0.0
 */
export const reports = (attributes: { readonly type: string }) =>
  Metric.counter("durable.reports", {
    description: "Reports raised, grouped by their fixed kind.",
    incremental: true,
    attributes,
  });

/**
 * Duration in milliseconds of a phase invocation and its decision commit: attributes `task`, `phase`, `outcome`.
 *
 * @category metrics
 * @since 0.0.0
 */
export const phaseDuration = (attributes: {
  readonly task: string;
  readonly phase: string;
  readonly outcome: string;
}) =>
  Metric.histogram("durable.phase.duration", {
    description: "Duration of a phase invocation and its decision commit, in milliseconds.",
    boundaries: Metric.exponentialBoundaries({ start: 1, factor: 4, count: 10 }),
    attributes,
  });

/**
 * Live tasks this Harness holds, by attribute `task`: counted in when loaded or created, out at terminal
 * settlement or close (an up-down counter).
 *
 * @category metrics
 * @since 0.0.0
 */
export const tasksActive = (attributes: { readonly task: string }) =>
  Metric.counter("durable.tasks.active", {
    description: "Live tasks held by this Harness; decremented at terminal settlement or close.",
    attributes,
  });

// The kernel's telemetry calls (`Telemetry.ts`). They run inside commit publication and the reporter, which must not
// fail: each call is contained, and the caller decides what a throw becomes.
import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as Fiber from "effect/Fiber";
import * as Metric from "effect/Metric";
import * as Option from "effect/Option";
import * as Result from "effect/Result";
import * as Tracer from "effect/Tracer";
import type { ReportInfo } from "../Report.ts";
import * as Telemetry from "../Telemetry.ts";

export type Attributes = Record<string, string | number | boolean>;

/** A telemetry call that threw while a commit was published. */
export const TELEMETRY: ReportInfo = { kind: "host_error", callback: "telemetry" };

const OWNERS: ReadonlySet<string> = new Set([Telemetry.Spans.submit, Telemetry.Spans.phase]);

/**
 * The span a commit's or a report's events go on (the current span of the context, which is the submit or phase
 * span): the nearest submit or phase span from `start`, or else `start` itself. The kernel's own spans in
 * between, such as `Session.commit` or `durable.hook`, are skipped; an external parent records nothing.
 */
export const ownerSpan = (start: Tracer.AnySpan | undefined): Tracer.Span | undefined => {
  let current: Tracer.AnySpan | undefined = start;
  while (current?._tag === "Span") {
    if (OWNERS.has(current.name)) return current;
    current = Option.getOrUndefined(current.parent);
  }
  return start?._tag === "Span" ? start : undefined;
};

/** The current fiber's span, for synchronous code that runs on a fiber: the reporter, commit publication. */
export const currentSpanUnsafe = (): Tracer.AnySpan | undefined => {
  const fiber = Fiber.getCurrent();
  return fiber === undefined
    ? undefined
    : Option.getOrUndefined(Context.getOption(fiber.context, Tracer.ParentSpan));
};

/** Records an event on `span`; a throw goes to `onError`. */
export const event = (
  span: Tracer.Span | undefined,
  name: string,
  attributes: Attributes,
  onError: (error: Error) => void,
): void => {
  if (span === undefined) return;
  contained(() => {
    const fiber = Fiber.getCurrent();
    const nanos =
      fiber === undefined ? BigInt(0) : fiber.getRef(Clock.Clock).currentTimeNanosUnsafe();
    span.event(name, nanos, attributes);
  }, onError);
};

/**
 * One metric per series, built once: a metric keeps its series key while its attributes stay the same object,
 * which `Metric.withAttributes` would rebuild at every update.
 */
const series = new Map<string, Metric.Metric<number, unknown>>();

/** Updates the series `key` from synchronous code on a fiber; a throw goes to `onError`. */
export const update = (
  key: string,
  make: () => Metric.Metric<number, unknown>,
  value: number,
  onError: (error: Error) => void,
): void => {
  const fiber = Fiber.getCurrent();
  if (fiber === undefined) return;
  contained(() => {
    let metric = series.get(key);
    if (metric === undefined) {
      metric = make();
      series.set(key, metric);
    }
    metric.updateUnsafe(value, fiber.context);
  }, onError);
};

/** The fixed fields of a report event: its kind, reason, extension and task, never the failure. */
export type ReportAttributes = {
  "durable.report.type": ReportInfo["kind"];
  "durable.report.reason"?: string;
  "durable.report.extension"?: string;
  "durable.task.id"?: number;
};

export const reportAttributes = (info: ReportInfo): ReportAttributes => {
  const attributes: ReportAttributes = { "durable.report.type": info.kind };
  if ("reason" in info) attributes["durable.report.reason"] = info.reason;
  if ("extension" in info && info.extension !== undefined)
    attributes["durable.report.extension"] = info.extension;
  if ("task" in info && info.task !== undefined) attributes["durable.task.id"] = info.task.id;
  return attributes;
};

/**
 * Counts a report by its kind and records its event on the current span. A
 * throwing metric or event is dropped, as reporting it would report again.
 */
export const recordReport = (info: ReportInfo, on?: Tracer.AnySpan | undefined): void => {
  update(
    `durable.reports\u0000${info.kind}`,
    () => Telemetry.reports({ type: info.kind }),
    1,
    dropped,
  );
  event(
    ownerSpan(on ?? currentSpanUnsafe()),
    Telemetry.Events.report,
    reportAttributes(info),
    dropped,
  );
};

/** A throw dropped: reporting it would report again. */
const dropped = () => {};

/**
 * Records `durable.trace.id` and, when the span links other traces, `durable.trace.links`: a backend that keeps only
 * scalar attributes, as Cloudflare's does, follows a trace across invocations with them.
 */
export const traceIds = (span: Tracer.Span, onError: (error: Error) => void): void => {
  annotate(span, { "durable.trace.id": span.traceId }, onError);
  contained(() => {
    if (span.links.length === 0) return;
    span.attribute("durable.trace.links", span.links.map((link) => link.span.traceId).join(","));
  }, onError);
};

/**
 * Sets attributes on a span the kernel started; each throw goes to `onError`. Telemetry never fails the work it
 * traces: the kernel's spans take their attributes here, not through `withSpan`'s `attributes`, which Effect sets
 * with no guard.
 */
export const annotate = (
  span: Tracer.Span,
  attributes: Attributes,
  onError: (error: Error) => void,
): void => {
  for (const [key, value] of Object.entries(attributes)) {
    contained(() => span.attribute(key, value), onError);
  }
};

/** Links `span` to `links`; a throw goes to `onError`. */
export const link = (
  span: Tracer.Span,
  links: ReadonlyArray<Tracer.SpanLink>,
  onError: (error: Error) => void,
): void => contained(() => span.addLinks(links), onError);

/** Runs a telemetry call on a span the kernel started; a throw goes to `onError`. */
export const guarded = (call: () => void, onError: (error: Error) => void): void =>
  contained(call, onError);

/** A throw dropped, where no reporter is at hand (an admission's span). */
export const drop = (): void => {};

/** Runs a callback that must not fail its caller; a throw goes to `onError`. */
const contained = (callback: () => void, onError: (error: Error) => void) => {
  const result = Result.try({
    try: callback,
    catch: (error) => (error instanceof Error ? error : new Error(String(error))),
  });
  if (Result.isFailure(result)) onError(result.failure);
};

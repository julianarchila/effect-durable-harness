// The object's tracer. Alchemy builds a Durable Object's instance with no telemetry and gives each event (a
// call, a fetch, an alarm, a socket message) a `Tracer` of its own: `Cloudflare.Telemetry()`'s records into that
// event's trace, through the event's async context. The Harness's fibers outlive the event that built the instance or
// started them: a phase that runs after a submit returned, or one an alarm woke. They use this tracer, which starts
// each span on the tracer of an event in progress, or else of the latest event, so the scheduler's spans reach the
// trace of an invocation that keeps the object alive.
//
// Among the events in progress, a span without a live parent (and an outcome span, below) goes to the one this tracer
// has started the fewest spans on (on a tie, the one whose running handler was entered most recently), and a child
// stays in its parent's event. Workers Observability drops the invocation record of an event whose trace grows past
// roughly 9,000-10,000 spans, and every span after a varying prefix; with every
// background span on the latest event, 32 turns at once in one object put the work of all of them into one
// invocation and crossed it.
//
// Starting a span must not fail the work it traces: a tracer that throws (a latest event's tracer whose context is
// gone) gives way to an Effect-local span. The tracer forwards `span` only, never `context`: running a fiber's
// steps inside an ended event's async context could tie its I/O to that event.
//
// Spans that outlive their event. An event's trace stays open after its handler returns, until the object's
// next event starts (measured on a deployed stage). A span still open when
// its event's trace closes reaches Workers Observability with its name, parent and the duration until the cut, and
// none of its attributes or `effect.exit`; a span started inside a closed trace (a child of such a span, through
// Alchemy's parent context) does not reach it at all. So this tracer:
//
// - starts a child of a span whose event has been superseded under an external copy of that parent (same Effect ids),
//   so the child opens in the event in progress instead of the closed trace;
// - when a span ends after its event was superseded, records its outcome on the event in progress or the latest one:
//   a span `<name> outcome` (a space, not a dot: `durable.phase.outcome` is already an attribute) with the span's
//   attributes, `durable.outcome_of` (the span's name), `durable.trace.id`, `durable.span.id`, its start and duration, ended
//   with the span's exit (`effect.exit` on Cloudflare). The original span is ended too, for the tracers that keep it.
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Predicate from "effect/Predicate";
import * as Result from "effect/Result";
import * as Tracer from "effect/Tracer";
import type * as Context from "effect/Context";
import type * as Exit from "effect/Exit";

export interface EventTracer {
  /** The tracer the Harness is built with. */
  readonly tracer: Tracer.Tracer;
  /** Runs one event's handler, its tracer the current one while it runs. */
  readonly during: <A, E, R>(effect: Effect.Effect<A, E, R>) => Effect.Effect<A, E, R>;
  /**
   * The tracer of the event that constructs the instance, latest until the first handler runs: Alchemy builds the
   * instance before any handler, with no telemetry, and the recovery that construction starts runs then.
   */
  readonly construct: (tracer: Tracer.Tracer) => void;
}

/** One event the object traced into: its tracer, its order, and how many of its handlers are running. */
interface Event {
  readonly tracer: Tracer.Tracer;
  readonly sequence: number;
  running: number;
  /** Spans this tracer started on the event's tracer. */
  started: number;
}

/** Called once a forwarded span has ended, with its duration. */
type Ended = (span: ForwardedSpan, duration: bigint, exit: Exit.Exit<unknown, unknown>) => void;

/**
 * A span this tracer started on an event's tracer. It delegates to that tracer's span, and remembers the event, so
 * its end can tell whether the event's trace was still open.
 */
class ForwardedSpan implements Tracer.Span {
  readonly _tag = "Span";
  readonly inner: Tracer.Span;
  readonly origin: Event;
  readonly parent: Option.Option<Tracer.AnySpan>;
  private readonly ended: Ended;
  constructor(
    inner: Tracer.Span,
    origin: Event,
    parent: Option.Option<Tracer.AnySpan>,
    ended: Ended,
  ) {
    this.inner = inner;
    this.origin = origin;
    this.parent = parent;
    this.ended = ended;
  }
  get name(): string {
    return this.inner.name;
  }
  get spanId(): string {
    return this.inner.spanId;
  }
  get traceId(): string {
    return this.inner.traceId;
  }
  get annotations(): Context.Context<never> {
    return this.inner.annotations;
  }
  get status(): Tracer.SpanStatus {
    return this.inner.status;
  }
  get attributes(): ReadonlyMap<string, unknown> {
    return this.inner.attributes;
  }
  get links(): ReadonlyArray<Tracer.SpanLink> {
    return this.inner.links;
  }
  get sampled(): boolean {
    return this.inner.sampled;
  }
  get kind(): Tracer.SpanKind {
    return this.inner.kind;
  }
  end(endTime: bigint, exit: Exit.Exit<unknown, unknown>): void {
    const start = this.inner.status.startTime;
    this.inner.end(endTime, exit);
    this.ended(this, endTime - start, exit);
  }
  // oxlint-disable-next-line anti-slop/no-unknown-parameters -- Effect's `Tracer.Span` contract
  attribute(key: string, value: unknown): void {
    this.inner.attribute(key, value);
  }
  // oxlint-disable-next-line anti-slop/no-unsafe-dictionary-type -- Effect's `Tracer.Span` contract
  event(name: string, startTime: bigint, attributes?: Record<string, unknown>): void {
    this.inner.event(name, startTime, attributes);
  }
  addLinks(links: ReadonlyArray<Tracer.SpanLink>): void {
    this.inner.addLinks(links);
  }
}

const NANOS_PER_MILLI = 1_000_000n;

export const make = (): EventTracer => {
  const events = new WeakMap<Tracer.Tracer, Event>();
  const active: Array<Event> = [];
  let latest: Event | undefined;
  let sequence = 0;
  // The constructing event is the invocation of the first handler, which Alchemy runs once the instance is built:
  // that handler's event does not supersede it.
  let constructing = false;

  /** Whether `event`'s trace has closed: none of its handlers runs, and a later event has started. */
  const superseded = (event: Event) =>
    event.running === 0 && latest !== undefined && latest.sequence > event.sequence;

  /**
   * The event in progress with the fewest spans started on it; on a tie, the one whose running handler was entered
   * most recently (`active` keeps handlers in the order they were entered).
   */
  const leastLoaded = (): Event | undefined => {
    let chosen: Event | undefined;
    for (const event of active) {
      if (chosen === undefined || event.started <= chosen.started) chosen = event;
    }
    return chosen;
  };

  /** Records a superseded span's outcome on the least loaded event in progress, or else the latest one. */
  const recordOutcome = (
    span: ForwardedSpan,
    duration: bigint,
    exit: Exit.Exit<unknown, unknown>,
  ) => {
    if (!superseded(span.origin)) return;
    const target = leastLoaded() ?? latest;
    if (target === undefined) return;
    // Telemetry never fails the work it traces: a throwing tracer loses the outcome only.
    Result.try(() => {
      const now = span.status._tag === "Ended" ? span.status.endTime : span.status.startTime;
      const outcome = target.tracer.span({
        name: `${span.name} outcome`,
        parent: Option.none(),
        annotations: span.annotations,
        links: [{ span, attributes: {} }],
        startTime: now,
        kind: span.kind,
        root: true,
        sampled: span.sampled,
      });
      target.started += 1;
      for (const [key, value] of span.attributes) outcome.attribute(key, value);
      outcome.attribute("durable.outcome_of", span.name);
      if (!span.attributes.has("durable.trace.id"))
        outcome.attribute("durable.trace.id", span.traceId);
      outcome.attribute("durable.span.id", span.spanId);
      outcome.attribute("durable.span.started_at", Number(span.status.startTime / NANOS_PER_MILLI));
      outcome.attribute("durable.span.duration_ms", Number(duration / NANOS_PER_MILLI));
      outcome.end(now, exit);
    });
  };

  const tracer: Tracer.Tracer = Tracer.make({
    span: (options) => {
      const parent = Option.getOrUndefined(options.parent);
      // A child of a live forwarded span opens in its parent's event, as Alchemy's tracer nests it there anyway.
      const live =
        parent instanceof ForwardedSpan && !superseded(parent.origin) ? parent.origin : undefined;
      const target = live ?? leastLoaded() ?? latest;
      if (target !== undefined) {
        const forwarded =
          parent instanceof ForwardedSpan
            ? Option.some<Tracer.AnySpan>(
                live === undefined
                  ? Tracer.externalSpan({
                      spanId: parent.spanId,
                      traceId: parent.traceId,
                      sampled: parent.sampled,
                      annotations: parent.annotations,
                    })
                  : parent.inner,
              )
            : options.parent;
        const started = Result.try(() => target.tracer.span({ ...options, parent: forwarded }));
        if (Result.isSuccess(started)) {
          target.started += 1;
          return new ForwardedSpan(started.success, target, options.parent, recordOutcome);
        }
      }
      return new Tracer.NativeSpan(options);
    },
  });

  const enter = (current: Tracer.Tracer): Event => {
    const known = events.get(current);
    const event = known ?? {
      tracer: current,
      sequence: constructing ? sequence : ++sequence,
      running: 0,
      started: 0,
    };
    constructing = false;
    if (known === undefined) events.set(current, event);
    event.running += 1;
    active.push(event);
    if (latest === undefined || event.sequence >= latest.sequence) latest = event;
    return event;
  };
  const leave = (event: Event) => {
    event.running -= 1;
    const index = active.lastIndexOf(event);
    if (index !== -1) active.splice(index, 1);
  };

  const during = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
    Effect.flatMap(Effect.tracer, (current) =>
      // The object's own tracer is not an event's: a handler called from the object's fibers keeps the current one.
      current === tracer
        ? effect
        : Effect.acquireUseRelease(
            Effect.sync(() => enter(current)),
            () => effect,
            (event) => Effect.sync(() => leave(event)),
          ),
    );
  const construct = (current: Tracer.Tracer) => {
    if (current === tracer || latest !== undefined) return;
    const event: Event = { tracer: current, sequence, running: 0, started: 0 };
    events.set(current, event);
    latest = event;
    constructing = true;
  };
  return { tracer, during, construct };
};

/**
 * Wraps every Effect member of an object's handlers in `during`: a method returning an Effect, or an Effect value.
 * Streams pass through, as Alchemy encodes them itself.
 */
export const tracedMembers = <T extends object>(members: T, events: EventTracer): T => {
  const traced = { ...members };
  for (const name of Object.keys(members)) {
    // SAFETY: a key `Object.keys` returned for `members`.
    const key = name as keyof T;
    traced[key] = traceMember(members[key], events);
  }
  return traced;
};

/** One member: `during` keeps an Effect's channels and a method's signature, whatever they are. */
const traceMember = <V>(member: V, events: EventTracer): V => {
  if (Effect.isEffect(member)) {
    // SAFETY: `during` returns an Effect with the member's own channels.
    // @effect-diagnostics-next-line anyUnknownInErrorContext:off
    return events.during(member) as V;
  }
  if (!Predicate.isFunction(member)) return member;
  const method = (...args: ReadonlyArray<never>) => {
    const result = member(...args);
    // @effect-diagnostics-next-line anyUnknownInErrorContext:off
    return Effect.isEffect(result) ? events.during(result) : result;
  };
  // SAFETY: the same parameters, and the same result with an Effect's channels unchanged.
  return method as V;
};

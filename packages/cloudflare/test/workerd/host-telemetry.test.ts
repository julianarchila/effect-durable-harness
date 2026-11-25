import * as Alchemy from "alchemy";
import * as Cloudflare from "@alchemy.run/cloudflare";
import * as Test from "@alchemy.run/test/Vitest";
import * as Effect from "effect/Effect";
import { expect } from "vitest";
import Stack from "./alchemy.run.ts";
import type { RecordedSpan } from "./fixture/RecordedSpans.ts";
import { call, idle, millis, objectName, settled, state } from "./support.ts";

/**
 * Spans that outlive their event (`@effect-durable-harness/host` `internal/tracer.ts`): a span that ends after a later event started
 * gets an `<name> outcome` span on the tracer of an event that is still open, with its attributes, its exit, its
 * duration and its Effect ids. The test Worker records what each event's tracer ended (`fixture/RecordedSpans.ts`).
 * Local workerd by default; `ALCHEMY_TEST_DEV=0` runs it on a deployed stage.
 */
const { test, beforeAll, afterAll, deploy, destroy } = Test.make({
  providers: Cloudflare.providers(),
  state: Alchemy.localState(),
  dev: true,
});

const stack = beforeAll(deploy(Stack), { timeout: 300_000 });
afterAll.skipIf(!!process.env.NO_DESTROY)(destroy(Stack), { timeout: 300_000 });

test(
  "a phase that ends after a later event started records its outcome on an open event",
  Effect.gen(function* () {
    const { url } = yield* stack;
    const name = objectName("telemetry-outcome");
    yield* state(url, name);
    const id = yield* call<number>(url, name, "startTask", "probe.busy", {
      chunks: 1,
      chunkMs: 1_500,
    });
    // A later event while the phase runs: the call that started it no longer holds an open trace.
    yield* idle(millis(400));
    yield* state(url, name);
    yield* settled(url, name, id);
    const spans = yield* call<ReadonlyArray<RecordedSpan>>(url, name, "recordedSpans");
    const run = (span: RecordedSpan) =>
      span.attributes["durable.task.id"] === id && span.attributes["durable.phase.name"] === "run";
    const phase = spans.find((span) => span.name === "durable.phase" && run(span));
    const outcome = spans.find((span) => span.name === "durable.phase outcome" && run(span));
    expect(phase).toBeDefined();
    // The outcome span carries the phase's attributes, `durable.phase.outcome` (the phase's result) among them.
    expect(outcome).toMatchObject({
      exit: "success",
      attributes: {
        "durable.phase.outcome": phase?.attributes["durable.phase.outcome"],
        "durable.trace.id": phase?.attributes["durable.trace.id"],
        "durable.outcome_of": "durable.phase",
      },
    });
    expect(outcome?.event).toBeGreaterThan(phase?.event ?? Infinity);
    expect(outcome?.attributes["durable.span.id"]).toBeTypeOf("string");
    expect(outcome?.attributes["durable.span.duration_ms"]).toBeGreaterThanOrEqual(1_500);
    // Only the phase outlived its event: the spans of the calls, which end inside them, have no outcome span.
    expect(spans.filter((span) => span.name === "durable.phase outcome")).toHaveLength(1);
  }),
);

// The Harness's telemetry on Effect's tracer (`src/Telemetry.ts`), on every backend.
//
// Notes:
// - `TestTracer`'s `layer` provides the `Tracer` and a fresh `MetricRegistry` around the test, so the Harness built
//   inside it and the test's own spans record there. Model requests are `durable.generation.request` spans with
//   Effect AI's GenAI attributes. A span's carrier is `carrierOf`, `parentId` the in-process parent's span ID
//   (`null` for a root or an external parent), `parentContext` the parent's carrier, and `ended` an ended span.
// - A span's status is its exit: a phase whose task failed ends with a `PhaseFailed` failure
//   (`durable.phase.error` names the category); a success is a success.
// - A caller's span is `Effect.withSpan("creator")` around the calls.
// - Not covered: the nested `executeTool` half of the first case and "emits replay for a codemode journal
//   result…" (codemode and nested tool journals are not supported). The Harness reads time from `Clock`, which
//   cannot throw, so "reports hook failures…" has no clock half.
// - "returns admission results without re-reading records…": Effect has no propagation adapter to spy on, and the
//   storage's reads are counted by the commits the test holds.
// - "counts an outside-span session failure without starting a span": the kernel always has its own
//   `Session.commit` span; the case checks no `durable.submit` or `durable.phase` span is started and the report counted.
// - "counts reports raised with a NOOP span…" and "attaches synchronous commit-listener reports…" observe through
//   the kernel's `observeCommits`, as in `HarnessReports`; an asynchronous observer is one whose `advance` returns
//   an Effect.
// - "reports telemetry adapters that throw…": the throwing adapter is a span whose `event` throws and a metric
//   registry whose writes throw, which is how an Effect exporter fails.
import { assert, it } from "@effect/vitest";
import * as Cause from "effect/Cause";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import type * as Metric from "effect/Metric";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Tracer from "effect/Tracer";
import * as AgentTool from "../src/AgentTool.ts";
import * as Extension from "../src/Extension.ts";
import * as Hook from "../src/Hook.ts";
import { Models } from "../src/Models.ts";
import type { SubmissionRecord } from "../src/Records.ts";
import type { Submission } from "../src/Submission.ts";
import * as Task from "../src/Task.ts";
import { carrierOf, type TraceCarrier } from "../src/Telemetry.ts";
import { fauxAssistantMessage, fauxToolCall } from "../src/testing/FauxModel.ts";
import * as TestTracer from "../src/testing/TestTracer.ts";
import { Tx } from "../src/Tx.ts";
import { type ChatSetup, chatSetup, openChat, waitFor } from "./ChatSupport.ts";
import { abortedWith, completed, eventually, openHarness } from "./HarnessSupport.ts";
import {
  type ControlledStorage,
  controlledStorage,
  describeEachBackend,
  run,
} from "./SessionSupport.ts";

const EMPTY_OBJECT = { type: "object", properties: {} } as const;

/** A value the case needs: asserted present, so a comparison never passes on two absent values. */
const need = <A>(value: A | undefined, what: string): A => {
  assert.isDefined(value, what);
  return Option.getOrThrow(Option.fromNullishOr(value));
};

const toolReply = (name: string) =>
  fauxAssistantMessage([fauxToolCall(name, {}, { id: "call-1" })], { stopReason: "toolUse" });

/** A chat whose Harness records into `traced`, its root configured with the fake model. */
const openTracked = (
  storage: ControlledStorage,
  setup: ChatSetup,
  extensions: ReadonlyArray<Extension.Any> = [],
) => openChat(storage, setup, { extensions });

/** A test body inside `traced`'s layer. */
const tracing =
  (traced: TestTracer.TestTracer) =>
  <A, E, R>(body: Effect.Effect<A, E, R>) =>
    body.pipe(Effect.provide(traced.layer));

const named = (traced: TestTracer.TestTracer, name: string) =>
  traced.spans.filter((span) => span.name === name);

const attributesOf = (span: Tracer.Span | undefined) => Object.fromEntries(span?.attributes ?? []);

const eventsOf = (span: Tracer.NativeSpan | undefined) =>
  (span?.events ?? []).map(([name, , attributes]) => ({ name, attributes }));

/** The in-process parent's span ID, `null` for a root or an external parent. */
const parentId = (span: Tracer.Span | undefined) =>
  Option.match(span?.parent ?? Option.none(), {
    onNone: () => null,
    onSome: (parent) => (parent._tag === "Span" ? parent.spanId : null),
  });

/** The parent's carrier, `null` for a root. */
const parentContext = (span: Tracer.Span | undefined): TraceCarrier | null =>
  Option.match(span?.parent ?? Option.none(), {
    onNone: () => null,
    onSome: (parent) => carrierOf(parent),
  });

const linksOf = (span: Tracer.Span | undefined) =>
  (span?.links ?? []).map((link) => carrierOf(link.span));

const ended = (span: Tracer.Span) => span.status._tag === "Ended";

/** Waits until every span has ended. */
const allSettled = (traced: TestTracer.TestTracer) =>
  waitFor(Effect.sync(() => traced.spans.every(ended)));

const exitOf = (span: Tracer.Span | undefined) =>
  span?.status._tag === "Ended" ? span.status.exit : undefined;

/** The metric values of `name`, one per attribute set. */
const metric = (traced: TestTracer.TestTracer, name: string) =>
  traced.metrics().filter((snapshot) => snapshot.id === name);

const counted = (snapshot: Metric.Metric.Snapshot): number =>
  snapshot.type === "Counter" || snapshot.type === "Histogram" ? Number(snapshot.state.count) : 0;

/** Runs `effect` to its end, letting the `TestClock` advance meanwhile. */
const finished = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  Effect.gen(function* () {
    const fiber = yield* Effect.forkChild(effect, { startImmediately: true });
    yield* waitFor(Effect.sync(() => fiber.pollUnsafe() !== undefined));
    return yield* Fiber.join(fiber);
  });

const settle = (submission: Submission) => finished(submission.await);

const reached = (deferred: Deferred.Deferred<void>) => waitFor(Deferred.isDone(deferred));

/** A response that signals `reached`, then waits for the gate before it returns `message`. */
const gatedResponse = (message: ReturnType<typeof fauxAssistantMessage>) => {
  const reachedGate = Deferred.makeUnsafe<void>();
  const gate = Deferred.makeUnsafe<void>();
  return {
    reached: reachedGate,
    open: Deferred.succeed(gate, undefined),
    step: () =>
      Deferred.succeed(reachedGate, undefined).pipe(
        Effect.andThen(Deferred.await(gate)),
        Effect.as(message),
      ),
  };
};

const Step = Schema.Struct({ phase: Schema.Literal("finish") });

/** The `test.trace` task: one phase that completes. */
const Finish = Task.make({
  name: "test.trace",
  version: 1,
  input: Schema.Null,
  checkpoint: Step,
  result: Schema.Null,
  initial: () => ({ phase: "finish" as const }),
  phases: { finish: (_task, runtime) => runtime.commit(() => Effect.succeed(completed(null))) },
  abort: (_task, runtime) => runtime.commit(() => Effect.succeed(abortedWith("aborted"))),
});

const encodeJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));

const create = <S extends { readonly phase: string }, R>(
  task: Task.Task<null, S, R>,
  options: Partial<Task.TaskOptions> = {},
) =>
  Effect.gen(function* () {
    const tx = yield* Tx;
    return yield* tx.createTask(task, null, { ownership: { kind: "conversation" }, ...options });
  });

const settledRecord = (submission: Submission) =>
  Effect.map(submission.status, (record): SubmissionRecord => record);

describeEachBackend("Durable telemetry", (backend) => {
  it.effect(
    "records content-free submit and phase spans, nested parents, checkpoints, entry events, and metrics",
    () => {
      const traced = TestTracer.make();
      return run(backend)(
        tracing(traced)(
          Effect.gen(function* () {
            const setup = chatSetup();
            const outer = Extension.make({
              name: "tool:outer",
              tools: [
                AgentTool.make("outer", {
                  description: "outer",
                  parameters: Schema.Struct({}),
                  jsonSchema: EMPTY_OBJECT,
                  execute: () =>
                    Effect.succeed({ output: [{ type: "text", text: "private-output" }] }),
                }),
              ],
            });
            setup.faux.setResponses([toolReply("outer"), fauxAssistantMessage("private-answer")]);
            const { harness, root } = yield* openTracked(yield* controlledStorage, setup, [outer]);
            const submitted = yield* root.submit({
              type: "input",
              content: "private-input",
              requestId: "dedupe",
            });
            yield* settle(submitted);
            yield* finished(harness.harness.awaitIdle);
            yield* allSettled(traced);
            const record = yield* settledRecord(submitted);
            const again = yield* root.submit({
              type: "input",
              content: "another-private-input",
              requestId: "dedupe",
            });
            assert.deepStrictEqual(yield* again.status, record);
            const submit = named(traced, "durable.submit")[0];
            assert.strictEqual(parentId(submit), null);
            assert.containSubset(attributesOf(submit), {
              "durable.submission.id": submitted.id,
              "durable.submission.result": "placed",
            });
            assert.deepStrictEqual(
              record.trace,
              submit === undefined ? undefined : carrierOf(submit),
            );
            const phases = named(traced, "durable.phase");
            assert.isAbove(phases.length, 4);
            const requests = named(traced, "durable.generation.request");
            assert.strictEqual(requests.length, 2);
            for (const request of requests) {
              const phase = phases.find((span) => span.spanId === parentId(request));
              assert.strictEqual(attributesOf(phase)["durable.phase.name"], "request");
            }
            assert.deepStrictEqual(
              parentContext(phases[0]),
              submit === undefined ? null : carrierOf(submit),
            );
            assert.containSubset(attributesOf(phases[0]), {
              "durable.phase.name": "prepare",
              "durable.phase.resumed": false,
              "durable.phase.outcome": "next=request",
            });
            const tool = phases.find((span) => attributesOf(span)["durable.tool.name"] === "outer");
            assert.containSubset(attributesOf(tool), {
              "durable.phase.name": "call",
              "durable.phase.outcome": "done",
              "durable.tool.call_id": "call-1",
            });
            assert.deepInclude(eventsOf(tool), {
              name: "durable.checkpoint",
              attributes: { "durable.phase.name": "execute" },
            });
            assert.isTrue(
              phases.some((span) => attributesOf(span)["durable.phase.outcome"] === "waiting"),
            );
            assert.deepInclude(eventsOf(submit), {
              name: "durable.entry",
              attributes: {
                "durable.entry.id": "entry" in record ? record.entry : -1,
                "durable.entry.kind": "core.user",
              },
            });
            assert.deepInclude(phases.flatMap(eventsOf), {
              name: "durable.submission",
              attributes: {
                "durable.submission.id": submitted.id,
                "durable.submission.status": "done",
                "durable.answer.entry_id": "answer" in record ? record.answer : -1,
              },
            });
            const recorded = encodeJson(
              traced.spans.map((span) => ({
                attributes: attributesOf(span),
                events: eventsOf(span),
              })),
            );
            // The model request follows Effect AI's conventions, OpenTelemetry's GenAI attributes: scalars about the
            // request, never its messages, which the checks here cover.
            for (const content of ["private-input", "private-answer", "private-output"]) {
              assert.notInclude(recorded, content);
            }
            assert.containSubset(attributesOf(requests[0]), {
              "gen_ai.provider.name": "faux",
              "gen_ai.operation.name": "chat",
              "gen_ai.request.model": "faux-1",
              "gen_ai.response.finish_reasons": ["toolUse"],
            });
            assert.containSubset(attributesOf(requests[1]), {
              "gen_ai.response.finish_reasons": ["stop"],
            });
            assert.isNumber(attributesOf(requests[0])["gen_ai.usage.input_tokens"]);
            assert.isNumber(attributesOf(requests[0])["gen_ai.usage.output_tokens"]);
            yield* harness.close;
            const durations = metric(traced, "durable.phase.duration");
            assert.strictEqual(
              durations.reduce((sum, snapshot) => sum + counted(snapshot), 0),
              phases.length,
            );
            assert.strictEqual(
              metric(traced, "durable.tasks.active").reduce(
                (sum, snapshot) => sum + counted(snapshot),
                0,
              ),
              0,
            );
            for (const snapshot of traced
              .metrics()
              .filter((entry) => entry.id.startsWith("durable."))) {
              assert.isFalse(
                Object.keys(snapshot.attributes ?? {}).some((key) => key.includes("id")),
              );
            }
            assert.deepStrictEqual(setup.reports, []);
          }),
        ),
      );
    },
  );

  it.effect(
    "returns admission results without re-reading records or probing root link targets",
    () => {
      const traced = TestTracer.make();
      return run(backend)(
        tracing(traced)(
          Effect.gen(function* () {
            const setup = chatSetup();
            const held = gatedResponse(fauxAssistantMessage("first"));
            setup.faux.setResponses([held.step, fauxAssistantMessage("second")]);
            const { harness, root } = yield* openTracked(yield* controlledStorage, setup);
            const first = yield* root.submit({ type: "input", content: "first" });
            yield* reached(held.reached);
            const second = yield* root.submit({
              type: "input",
              content: "second",
              requestId: "queued",
            });
            const duplicate = yield* root.submit({
              type: "input",
              content: "ignored",
              requestId: "queued",
            });
            assert.strictEqual(duplicate.id, second.id);
            assert.deepStrictEqual(
              named(traced, "durable.submit").map(
                (span) => attributesOf(span)["durable.submission.result"],
              ),
              ["placed", "queued", "queued"],
            );
            yield* held.open;
            yield* settle(first);
            yield* settle(second);
            yield* harness.close;
          }),
        ),
      );
    },
  );

  it.effect(
    "inherits omitted or undefined task traces and stores no carrier for an explicit root",
    () => {
      const traced = TestTracer.make();
      return run(backend)(
        tracing(traced)(
          Effect.gen(function* () {
            const setup = chatSetup();
            const { harness, root } = yield* openTracked(yield* controlledStorage, setup, [
              Extension.make({ name: "task:test.trace", tasks: [Finish] }),
            ]);
            const ids = yield* root
              .commit(
                Effect.all([
                  create(Finish),
                  create(Finish, { trace: undefined }),
                  create(Finish, { trace: "root" }),
                ]),
              )
              .pipe(Effect.withSpan("creator"));
            yield* harness.harness.resume;
            for (const id of ids) yield* finished(harness.harness.awaitTask(id));
            yield* allSettled(traced);
            const creator = named(traced, "creator")[0];
            const context = creator === undefined ? undefined : carrierOf(creator);
            const records: Array<TraceCarrier | undefined> = [];
            for (const id of ids) {
              records.push(Option.getOrUndefined(yield* harness.harness.getTask(id))?.trace);
            }
            assert.deepStrictEqual(records, [context, context, undefined]);
            assert.deepStrictEqual(named(traced, "durable.phase").map(parentContext), [
              context ?? null,
              context ?? null,
              null,
            ]);
            yield* harness.close;
          }),
        ),
      );
    },
  );

  it.effect("records checkpoints only when the running phase changes", () => {
    const traced = TestTracer.make();
    const Checkpoint = Task.make({
      name: "test.checkpoint",
      version: 1,
      input: Schema.Null,
      checkpoint: Schema.Struct({
        phase: Schema.Literals(["call", "finish"]),
        count: Schema.Number,
      }),
      result: Schema.Null,
      initial: () => ({ phase: "call" as const, count: 0 }),
      phases: {
        call: (_task, runtime) =>
          Effect.gen(function* () {
            yield* runtime.commit(() =>
              Effect.succeed({
                status: "running" as const,
                checkpoint: { phase: "call" as const, count: 1 },
              }),
            );
            yield* runtime.commit(() =>
              Effect.succeed({
                status: "running" as const,
                checkpoint: { phase: "call" as const, count: 2 },
              }),
            );
            yield* runtime.commit(() =>
              Effect.succeed({
                status: "running" as const,
                checkpoint: { phase: "finish" as const, count: 2 },
              }),
            );
          }),
        finish: (_task, runtime) => runtime.commit(() => Effect.succeed(completed(null))),
      },
      abort: (_task, runtime) => runtime.commit(() => Effect.succeed(abortedWith("aborted"))),
    });
    return run(backend)(
      tracing(traced)(
        Effect.gen(function* () {
          const { harness, root } = yield* openTracked(yield* controlledStorage, chatSetup(), [
            Extension.make({ name: "task:test.checkpoint", tasks: [Checkpoint] }),
          ]);
          const id = yield* root.commit(create(Checkpoint));
          yield* harness.harness.resume;
          yield* finished(harness.harness.awaitTask(id));
          yield* allSettled(traced);
          assert.deepStrictEqual(
            traced.spans.flatMap(eventsOf).filter((event) => event.name === "durable.checkpoint"),
            [{ name: "durable.checkpoint", attributes: { "durable.phase.name": "finish" } }],
          );
          yield* harness.close;
        }),
      ),
    );
  });

  it.effect(
    "starts each queued input's run in its admission trace and links input roots to their caller",
    () => {
      const traced = TestTracer.make();
      return run(backend)(
        tracing(traced)(
          Effect.gen(function* () {
            const setup = chatSetup();
            const held = gatedResponse(fauxAssistantMessage("first"));
            setup.faux.setResponses([held.step, fauxAssistantMessage("second")]);
            const { harness, root } = yield* openTracked(yield* controlledStorage, setup);
            yield* Effect.gen(function* () {
              const first = yield* root.submit({ type: "input", content: "first" });
              yield* reached(held.reached);
              const second = yield* root.submit({ type: "input", content: "second" });
              assert.strictEqual((yield* second.status).status, "queued");
              yield* held.open;
              yield* settle(first);
              yield* settle(second);
            }).pipe(Effect.withSpan("caller"));
            yield* finished(harness.harness.awaitIdle);
            yield* allSettled(traced);
            const caller = named(traced, "caller")[0];
            const submits = named(traced, "durable.submit");
            assert.strictEqual(submits.length, 2);
            for (const submit of submits) {
              assert.strictEqual(parentId(submit), null);
              assert.deepStrictEqual(linksOf(submit), [
                carrierOf(need(caller, "the caller's span")),
              ]);
              assert.strictEqual(attributesOf(submit)["durable.trace.links"], caller?.traceId);
              assert.isTrue(
                named(traced, "durable.phase").some(
                  (span) =>
                    attributesOf(span)["durable.phase.name"] === "prepare" &&
                    span.traceId === submit.traceId &&
                    parentContext(span)?.spanId === submit.spanId,
                ),
              );
            }
            assert.notStrictEqual(submits[0]?.traceId, submits[1]?.traceId);
            yield* harness.close;
          }),
        ),
      );
    },
  );

  it.effect("links the other queued inputs to the first invocation of their shared run", () => {
    const traced = TestTracer.make();
    return run(backend)(
      tracing(traced)(
        Effect.gen(function* () {
          const setup = chatSetup();
          setup.settings.followUpMode = "all";
          const held = gatedResponse(fauxAssistantMessage("first"));
          setup.faux.setResponses([held.step, fauxAssistantMessage("shared")]);
          const { harness, root } = yield* openTracked(yield* controlledStorage, setup);
          const first = yield* root.submit({ type: "input", content: "first" });
          yield* reached(held.reached);
          const second = yield* root.submit({ type: "input", content: "second" });
          const third = yield* root.submit({ type: "input", content: "third" });
          const secondTrace = (yield* second.status).trace;
          const thirdTrace = (yield* third.status).trace;
          yield* held.open;
          yield* settle(first);
          yield* settle(second);
          yield* settle(third);
          yield* finished(harness.harness.awaitIdle);
          yield* allSettled(traced);
          const tasks = yield* harness.harness.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              return yield* tx.scanTasks({ kind: "core.generation" }, 10);
            }),
          );
          const shared = tasks.items[1];
          assert.deepStrictEqual(shared?.trace, secondTrace);
          assert.deepStrictEqual(shared?.links, [need(thirdTrace, "the third input's carrier")]);
          const phases = named(traced, "durable.phase").filter(
            (span) => attributesOf(span)["durable.task.id"] === shared?.id,
          );
          assert.deepStrictEqual(
            phases.map((span) => attributesOf(span)["durable.phase.name"]),
            ["prepare", "request"],
          );
          for (const phase of phases) {
            assert.deepStrictEqual(linksOf(phase), [need(thirdTrace, "the third input's carrier")]);
          }
          yield* harness.close;
        }),
      ),
    );
  });

  it.effect(
    "links a steer admission to the current run without moving its work into the steer's trace",
    () => {
      const traced = TestTracer.make();
      return run(backend)(
        tracing(traced)(
          Effect.gen(function* () {
            const setup = chatSetup();
            const reachedHold = Deferred.makeUnsafe<void>();
            const release = Deferred.makeUnsafe<void>();
            const hold = Extension.make({
              name: "tool:hold",
              tools: [
                AgentTool.make("hold", {
                  description: "hold",
                  parameters: Schema.Struct({}),
                  jsonSchema: EMPTY_OBJECT,
                  execute: () =>
                    Deferred.succeed(reachedHold, undefined).pipe(
                      Effect.andThen(Deferred.await(release)),
                      Effect.as({ output: [] }),
                    ),
                }),
              ],
            });
            setup.faux.setResponses([toolReply("hold"), fauxAssistantMessage("answer")]);
            const { harness, root } = yield* openTracked(yield* controlledStorage, setup, [hold]);
            const first = yield* root.submit({ type: "input", content: "first" });
            yield* reached(reachedHold);
            const run = (yield* harness.harness.inspect).tasks.find(
              (task) => task.record.kind === "core.generation",
            )?.record;
            const steer = yield* root.submit({
              type: "input",
              content: "steer",
              whenBusy: "steer",
            });
            const steerTrace = (yield* steer.status).trace;
            const steerSpan = traced.spans.find(
              (span) => attributesOf(span)["durable.submission.id"] === steer.id,
            );
            assert.strictEqual(parentId(steerSpan), null);
            assert.deepStrictEqual(linksOf(steerSpan), [
              need(run?.trace, "the running task's carrier"),
            ]);
            assert.strictEqual(attributesOf(steerSpan)["durable.trace.links"], run?.trace?.traceId);
            yield* Deferred.succeed(release, undefined);
            yield* settle(first);
            yield* settle(steer);
            yield* finished(harness.harness.awaitIdle);
            yield* allSettled(traced);
            assert.isFalse(
              named(traced, "durable.phase").some(
                (span) => parentContext(span)?.spanId === steerTrace?.spanId,
              ),
            );
            yield* harness.close;
          }),
        ),
      );
    },
  );

  it.effect("keeps a public write admission open through commit as a child of its caller", () => {
    const traced = TestTracer.make();
    return run(backend)(
      tracing(traced)(
        Effect.gen(function* () {
          const storage = yield* controlledStorage;
          const { harness, root } = yield* openTracked(storage, chatSetup());
          yield* Effect.gen(function* () {
            const held = yield* storage.holdCommits;
            const writing = yield* Effect.forkChild(
              root.submit({ type: "write", entry: { kind: "app.note", data: "private-note" } }),
              { startImmediately: true },
            );
            yield* held.entered;
            const inFlight = named(traced, "durable.submit")[0];
            assert.isFalse(inFlight === undefined || ended(inFlight));
            assert.deepStrictEqual(eventsOf(inFlight), []);
            yield* held.release;
            const submission = yield* Fiber.join(writing);
            const record = yield* submission.status;
            const span = named(traced, "durable.submit")[0];
            const caller = named(traced, "caller")[0];
            assert.strictEqual(parentId(span), caller?.spanId);
            assert.isTrue(span !== undefined && ended(span));
            assert.strictEqual(attributesOf(span)["durable.submission.result"], "done");
            assert.deepStrictEqual(record.trace, span === undefined ? undefined : carrierOf(span));
            assert.deepStrictEqual(
              eventsOf(span).map((event) => event.name),
              ["durable.entry", "durable.submission"],
            );
          }).pipe(Effect.withSpan("caller"));
          yield* harness.close;
        }),
      ),
    );
  });

  it.effect(
    "links an input submitted inside a turn to the calling phase without parenting it",
    () => {
      const traced = TestTracer.make();
      return run(backend)(
        tracing(traced)(
          Effect.gen(function* () {
            const setup = chatSetup();
            const delegate = Extension.make({
              name: "tool:delegate",
              tools: [
                AgentTool.make("delegate", {
                  description: "delegate",
                  parameters: Schema.Struct({}),
                  jsonSchema: EMPTY_OBJECT,
                  execute: () =>
                    Effect.gen(function* () {
                      const context = yield* AgentTool.ToolContext;
                      const child = yield* context.commit(
                        Effect.gen(function* () {
                          const tx = yield* Tx;
                          return yield* tx.createConversation({
                            ownership: { kind: "task", taskId: context.taskId },
                          });
                        }),
                      );
                      const conversation = yield* context.requireConversation(child.id);
                      const submission = yield* conversation.submit({
                        type: "input",
                        content: "child",
                      });
                      yield* submission.await;
                      return { output: [] };
                    }).pipe(Effect.orDie),
                }),
              ],
            });
            setup.faux.setResponses([
              toolReply("delegate"),
              fauxAssistantMessage("child answer"),
              fauxAssistantMessage("parent answer"),
            ]);
            const { harness, root } = yield* openTracked(yield* controlledStorage, setup, [
              delegate,
            ]);
            yield* settle(yield* root.submit({ type: "input", content: "parent" }));
            yield* finished(harness.harness.awaitIdle);
            yield* allSettled(traced);
            const tool = traced.spans.find(
              (span) =>
                span.name === "durable.phase" &&
                attributesOf(span)["durable.tool.name"] === "delegate",
            );
            const child = named(traced, "durable.submit")[1];
            assert.strictEqual(parentId(child), null);
            // The handle's submit runs inside the tool's own `durable.tool.call` span, a child of the phase.
            const linked = child?.links[0]?.span;
            assert.isDefined(linked);
            assert.strictEqual(linked?.traceId, tool?.traceId);
            assert.isTrue(
              linked?._tag === "Span" &&
                (linked.spanId === tool?.spanId ||
                  Option.getOrUndefined(linked.parent)?.spanId === tool?.spanId),
            );
            yield* harness.close;
          }),
        ),
      );
    },
  );

  it.effect("parents background compaction to the prepare phase that creates it", () => {
    const traced = TestTracer.make();
    return run(backend)(
      tracing(traced)(
        Effect.gen(function* () {
          const setup = chatSetup({
            models: [{ id: "faux-1", contextWindow: 1000, maxTokens: 100 }],
          });
          setup.settings.compaction = {
            enabled: true,
            reserveTokens: 300,
            keepRecentTokens: 150,
            backgroundTokens: 1000,
          };
          const decline = Extension.make({
            name: "hooks:decline",
            hooks: [
              Hook.compaction({ beforeCompact: () => Effect.succeed({ decline: true as const }) }),
            ],
          });
          setup.faux.setResponses([fauxAssistantMessage("done")]);
          const { harness, root } = yield* openTracked(yield* controlledStorage, setup, [decline]);
          yield* root.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              yield* tx.appendEntry(root.id, {
                kind: "core.user",
                model: [{ role: "user", content: "x".repeat(400), timestamp: 1 }],
              });
              yield* tx.appendEntry(root.id, {
                kind: "core.assistant",
                model: [fauxAssistantMessage("y".repeat(400))],
              });
            }),
          );
          yield* settle(yield* root.submit({ type: "input", content: "z".repeat(400) }));
          yield* finished(harness.harness.awaitIdle);
          yield* waitFor(
            Effect.sync(() =>
              traced.spans.some(
                (span) =>
                  attributesOf(span)["durable.task.name"] === "core.compaction" && ended(span),
              ),
            ),
          );
          const prepare = traced.spans.find(
            (span) =>
              attributesOf(span)["durable.task.name"] === "core.generation" &&
              attributesOf(span)["durable.phase.name"] === "prepare",
          );
          const compaction = traced.spans.find(
            (span) => attributesOf(span)["durable.task.name"] === "core.compaction",
          );
          const carrier = carrierOf(need(prepare, "the prepare phase's span"));
          assert.deepStrictEqual(parentContext(compaction), carrier);
          const tasks = yield* harness.harness.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              return yield* tx.scanTasks({ kind: "core.compaction" }, 10);
            }),
          );
          assert.strictEqual(tasks.items[0]?.background, true);
          assert.deepStrictEqual(tasks.items[0]?.trace, carrier);
          yield* harness.close;
        }),
      ),
    );
  });

  it.effect(
    "marks only the first invocation after reopen resumed, retaining its stored parent",
    () => {
      const traced = TestTracer.make();
      const reopened = TestTracer.make();
      let recover = false;
      const holding = Deferred.makeUnsafe<void>();
      const Resume = Task.make({
        name: "test.resume",
        version: 1,
        input: Schema.Null,
        checkpoint: Schema.Struct({ phase: Schema.Literals(["hold", "finish"]) }),
        result: Schema.Null,
        initial: () => ({ phase: "hold" as const }),
        phases: {
          hold: (_task, runtime) =>
            Effect.gen(function* () {
              if (!recover) {
                yield* Deferred.succeed(holding, undefined);
                return yield* Effect.never;
              }
              yield* runtime.commit(() =>
                Effect.succeed({
                  status: "running" as const,
                  checkpoint: { phase: "finish" as const },
                }),
              );
            }),
          finish: (_task, runtime) => runtime.commit(() => Effect.succeed(completed(null))),
        },
        abort: (_task, runtime) => runtime.commit(() => Effect.succeed(abortedWith("aborted"))),
      });
      const extensions = [Extension.make({ name: "task:test.resume", tasks: [Resume] })];
      return run(backend)(
        Effect.gen(function* () {
          const storage = yield* controlledStorage;
          const first = yield* tracing(traced)(
            Effect.gen(function* () {
              const { harness, root } = yield* openTracked(storage, chatSetup(), extensions);
              const id = yield* root.commit(create(Resume)).pipe(Effect.withSpan("creator"));
              yield* harness.harness.resume;
              yield* reached(holding);
              const before = Option.getOrUndefined(yield* harness.harness.getTask(id));
              yield* harness.close;
              return { id, before };
            }),
          );
          recover = true;
          yield* tracing(reopened)(
            Effect.gen(function* () {
              const { harness } = yield* openTracked(storage, chatSetup(), extensions);
              yield* harness.harness.resume;
              yield* finished(harness.harness.awaitTask(first.id));
              yield* allSettled(reopened);
              const phases = [...traced.spans, ...reopened.spans].filter(
                (span) => span.name === "durable.phase",
              );
              assert.deepStrictEqual(
                phases.map((span) => attributesOf(span)["durable.phase.resumed"]),
                [false, true, false],
              );
              assert.strictEqual(attributesOf(phases[0])["durable.phase.outcome"], "aborted");
              assert.isTrue(Exit.isSuccess(exitOf(phases[0]) ?? Exit.fail("open")));
              for (const phase of phases) {
                assert.deepStrictEqual(parentContext(phase), first.before?.trace ?? null);
              }
              yield* harness.close;
            }),
          );
          const active = [...traced.metrics(), ...reopened.metrics()]
            .filter((snapshot) => snapshot.id === "durable.tasks.active")
            .map(counted);
          // Effect's counters keep a sum, not a stream of adds: each Harness counted the task in and out.
          assert.deepStrictEqual(active, [0, 0]);
        }),
      );
    },
  );

  it.effect("emits entry and settlement events only after successful storage commits", () => {
    const traced = TestTracer.make();
    return run(backend)(
      tracing(traced)(
        Effect.gen(function* () {
          const storage = yield* controlledStorage;
          const { harness, root } = yield* openTracked(storage, chatSetup());
          yield* Effect.gen(function* () {
            const caller = named(traced, "caller")[0];
            const rolledBack = yield* Effect.exit(
              root.commit(
                Effect.gen(function* () {
                  const tx = yield* Tx;
                  yield* tx.appendEntry(root.id, {
                    kind: "app.rolled-back",
                    data: "private-rolled-back",
                  });
                  return yield* Effect.die(new Error("rollback"));
                }),
              ),
            );
            assert.isTrue(Exit.isFailure(rolledBack));
            assert.deepStrictEqual(eventsOf(caller), []);
            const held = yield* storage.holdCommits;
            const writing = yield* Effect.forkChild(
              root.commit(
                Effect.gen(function* () {
                  const tx = yield* Tx;
                  return yield* tx.appendEntry(root.id, {
                    kind: "app.note",
                    data: "private-content",
                  });
                }),
              ),
              { startImmediately: true },
            );
            yield* held.entered;
            assert.deepStrictEqual(eventsOf(caller), []);
            yield* held.release;
            const entry = yield* Fiber.join(writing);
            const submission = yield* root.commit(
              Effect.gen(function* () {
                const tx = yield* Tx;
                return yield* tx.createSubmission({
                  conversationId: root.id,
                  type: "write",
                  status: "done",
                  entry: entry.id,
                });
              }),
            );
            assert.deepStrictEqual(eventsOf(caller), [
              {
                name: "durable.entry",
                attributes: { "durable.entry.id": entry.id, "durable.entry.kind": "app.note" },
              },
              {
                name: "durable.submission",
                attributes: {
                  "durable.submission.id": submission.id,
                  "durable.submission.status": "done",
                },
              },
            ]);
          }).pipe(Effect.withSpan("caller"));
          yield* harness.close;
        }),
      ),
    );
  });

  it.effect("records internal compaction admissions as phase events without a submit span", () => {
    const traced = TestTracer.make();
    return run(backend)(
      tracing(traced)(
        Effect.gen(function* () {
          const setup = chatSetup();
          setup.settings.compaction = { keepRecentTokens: 1 };
          const storage = yield* controlledStorage;
          let held: Effect.Success<ControlledStorage["holdCommits"]> | undefined;
          const supply = Extension.make({
            name: "hooks:supply",
            hooks: [
              Hook.compaction({
                beforeCompact: () =>
                  Effect.gen(function* () {
                    held = yield* storage.holdCommits;
                    return { summary: "private-summary" };
                  }),
              }),
            ],
          });
          const { harness, root } = yield* openTracked(storage, setup, [supply]);
          yield* root.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              yield* tx.appendEntry(root.id, {
                kind: "core.user",
                model: [{ role: "user", content: "old", timestamp: 1 }],
              });
              yield* tx.appendEntry(root.id, {
                kind: "core.assistant",
                model: [fauxAssistantMessage("answer")],
              });
            }),
          );
          const id = yield* root.compact();
          yield* waitFor(Effect.sync(() => held !== undefined));
          yield* finished(need(held, "the held commit").entered);
          const phaseOf = () =>
            traced.spans.find(
              (span) => attributesOf(span)["durable.task.name"] === "core.compaction",
            );
          assert.isFalse(traced.spans.some((span) => span.name === "durable.submit"));
          const open = phaseOf();
          assert.isFalse(open !== undefined && ended(open));
          assert.deepStrictEqual(eventsOf(open), []);
          yield* need(held, "the held commit").release;
          yield* finished(harness.harness.awaitTask(id));
          yield* allSettled(traced);
          const phase = phaseOf();
          assert.isFalse(traced.spans.some((span) => span.name === "durable.submit"));
          assert.isTrue(eventsOf(phase).some((event) => event.name === "durable.entry"));
          assert.isTrue(
            eventsOf(phase).some(
              (event) =>
                event.name === "durable.submission" &&
                event.attributes["durable.submission.status"] === "done",
            ),
          );
          assert.notInclude(
            encodeJson(traced.spans.map((span) => [span.name, attributesOf(span), eventsOf(span)])),
            "private-summary",
          );
          yield* harness.close;
        }),
      ),
    );
  });

  for (const status of ["faulted", "failed", "orphaned"] as const) {
    it.effect(`records a fixed ${status} category and a bounded diagnostic`, () => {
      const traced = TestTracer.make();
      const message = "diagnostic ".repeat(80);
      const Fault = Task.make({
        name: "test.fault",
        version: 1,
        input: Schema.Null,
        checkpoint: Schema.Struct({ phase: Schema.Literal("fail") }),
        result: Schema.Null,
        initial: () => ({ phase: "fail" as const }),
        phases: {
          fail: (_task, runtime) =>
            status === "faulted"
              ? Effect.die(new Error(message))
              : runtime.commit(() =>
                  Effect.succeed({
                    status: "terminal" as const,
                    outcome:
                      status === "orphaned"
                        ? { status: "orphaned" as const, reason: message }
                        : {
                            status: "failed" as const,
                            error: { message, detail: "private-error-detail" },
                          },
                  }),
                ),
        },
        abort: (_task, runtime) => runtime.commit(() => Effect.succeed(abortedWith("aborted"))),
      });
      return run(backend)(
        tracing(traced)(
          Effect.gen(function* () {
            const setup = chatSetup();
            const { harness, root } = yield* openTracked(yield* controlledStorage, setup, [
              Extension.make({ name: "task:test.fault", tasks: [Fault] }),
            ]);
            const id = yield* root.commit(
              create(Fault, { trace: { traceId: "unknown", spanId: "carrier" } }),
            );
            yield* harness.harness.resume;
            const record = yield* finished(harness.harness.awaitTask(id));
            assert.strictEqual(record.state.outcome.status, status);
            yield* allSettled(traced);
            const span = named(traced, "durable.phase")[0];
            assert.strictEqual(parentId(span), null);
            assert.containSubset(attributesOf(span), {
              "durable.phase.outcome": "faulted",
              "durable.phase.error": `task_${status}`,
            });
            const exit = exitOf(span);
            const error =
              exit !== undefined && Exit.isFailure(exit) ? Cause.squash(exit.cause) : undefined;
            assert.containSubset(error, {
              _tag: "PhaseFailed",
              category: `task_${status}`,
              message: message.slice(0, 500),
            });
            if (status === "faulted") {
              assert.deepInclude(eventsOf(span), {
                name: "durable.report",
                attributes: {
                  "durable.report.type": "task_faulted",
                  "durable.report.reason": "threw",
                  "durable.report.extension": "task:test.fault",
                  "durable.task.id": id,
                },
              });
            }
            assert.notInclude(encodeJson(eventsOf(span)), "private-error-detail");
            assert.notInclude(
              encodeJson({ attributes: attributesOf(span), events: eventsOf(span) }),
              "diagnostic",
            );
            yield* harness.close;
          }),
        ),
      );
    });
  }

  it.effect("keeps both interrupted runs and explicit abort handlers at status ok", () => {
    const traced = TestTracer.make();
    const holding = Deferred.makeUnsafe<void>();
    const Abort = Task.make({
      name: "test.abort",
      version: 1,
      input: Schema.Null,
      checkpoint: Schema.Struct({ phase: Schema.Literal("hold") }),
      result: Schema.Null,
      initial: () => ({ phase: "hold" as const }),
      phases: {
        hold: () => Deferred.succeed(holding, undefined).pipe(Effect.andThen(Effect.never)),
      },
      abort: (_task, runtime) =>
        runtime.commit(() => Effect.succeed(abortedWith("private-abort-reason"))),
    });
    return run(backend)(
      tracing(traced)(
        Effect.gen(function* () {
          const { harness, root } = yield* openTracked(yield* controlledStorage, chatSetup(), [
            Extension.make({ name: "task:test.abort", tasks: [Abort] }),
          ]);
          const id = yield* root.commit(create(Abort));
          yield* harness.harness.resume;
          yield* reached(holding);
          yield* finished(harness.harness.abortTask(id));
          yield* finished(harness.harness.awaitTask(id));
          yield* allSettled(traced);
          const phases = named(traced, "durable.phase");
          assert.deepStrictEqual(
            phases.map((span) => attributesOf(span)["durable.phase.kind"]),
            ["run", "abort"],
          );
          for (const span of phases) {
            assert.strictEqual(attributesOf(span)["durable.phase.outcome"], "aborted");
            assert.isTrue(Exit.isSuccess(exitOf(span) ?? Exit.fail("open")));
          }
          assert.notInclude(
            encodeJson(phases.map((span) => [attributesOf(span), eventsOf(span)])),
            "private-abort-reason",
          );
          yield* harness.close;
        }),
      ),
    );
  });

  it.effect("mirrors runtime reports as fixed-field phase events and type-only counters", () => {
    const traced = TestTracer.make();
    const error = new Error("private-report-payload");
    const Reporting = Task.make({
      name: "test.report",
      version: 1,
      input: Schema.Null,
      checkpoint: Schema.Struct({ phase: Schema.Literal("report") }),
      result: Schema.Null,
      initial: () => ({ phase: "report" as const }),
      phases: {
        report: (_task, runtime) =>
          Effect.gen(function* () {
            yield* runtime.report(Cause.die(error));
            yield* runtime.report(Cause.die(error), {
              kind: "extension",
              extension: "reporting-extension",
            });
            yield* runtime.report(Cause.die(error), { kind: "host", callback: "settings" });
            yield* runtime.commit(() => Effect.succeed(completed(null)));
          }),
      },
      abort: (_task, runtime) => runtime.commit(() => Effect.succeed(abortedWith("aborted"))),
    });
    return run(backend)(
      tracing(traced)(
        Effect.gen(function* () {
          const setup = chatSetup();
          const { harness, root } = yield* openTracked(yield* controlledStorage, setup, [
            Extension.make({ name: "task:test.report", tasks: [Reporting] }),
          ]);
          const id = yield* root.commit(create(Reporting));
          yield* harness.harness.resume;
          yield* finished(harness.harness.awaitTask(id));
          yield* allSettled(traced);
          const span = named(traced, "durable.phase")[0];
          assert.deepStrictEqual(eventsOf(span), [
            {
              name: "durable.report",
              attributes: {
                "durable.report.type": "task_error",
                "durable.report.extension": "task:test.report",
                "durable.task.id": id,
              },
            },
            {
              name: "durable.report",
              attributes: {
                "durable.report.type": "extension_error",
                "durable.report.extension": "reporting-extension",
                "durable.task.id": id,
              },
            },
            {
              name: "durable.report",
              attributes: { "durable.report.type": "host_error", "durable.task.id": id },
            },
          ]);
          yield* eventually(Effect.sync(() => setup.reports.length === 3));
          assert.deepStrictEqual(
            setup.reports.map((report) => Cause.squash(report.cause)),
            [error, error, error],
          );
          assert.isFalse(traced.spans.some((span) => span.name === "durable.report"));
          assert.deepStrictEqual(
            metric(traced, "durable.reports").map((snapshot) => ({
              attributes: snapshot.attributes,
              value: counted(snapshot),
            })),
            [
              { attributes: { type: "task_error" }, value: 1 },
              { attributes: { type: "extension_error" }, value: 1 },
              { attributes: { type: "host_error" }, value: 1 },
            ],
          );
          assert.notInclude(
            encodeJson(traced.spans.map((span) => [attributesOf(span), eventsOf(span)])),
            "private-report-payload",
          );
          yield* harness.close;
        }),
      ),
    );
  });

  it.effect("reports hook failures on the phase", () => {
    const traced = TestTracer.make();
    const hookError = new Error("private-hook-error");
    return run(backend)(
      tracing(traced)(
        Effect.gen(function* () {
          const setup = chatSetup();
          setup.faux.setResponses([fauxAssistantMessage("answer")]);
          const { harness, root } = yield* openTracked(yield* controlledStorage, setup, [
            Extension.make({
              name: "reporting-hooks",
              hooks: [Hook.generation({ beforeRequest: () => Effect.die(hookError) })],
            }),
          ]);
          yield* settle(yield* root.submit({ type: "input", content: "input" }));
          yield* finished(harness.harness.awaitIdle);
          yield* allSettled(traced);
          const request = named(traced, "durable.phase").find(
            (span) => attributesOf(span)["durable.phase.name"] === "request",
          );
          assert.deepInclude(eventsOf(request), {
            name: "durable.report",
            attributes: {
              "durable.report.type": "extension_error",
              "durable.report.extension": "reporting-hooks",
              "durable.task.id": attributesOf(request)["durable.task.id"] ?? -1,
            },
          });
          assert.isFalse(traced.spans.some((span) => span.name === "durable.report"));
          assert.deepStrictEqual(
            metric(traced, "durable.reports").map((snapshot) => snapshot.attributes),
            [{ type: "extension_error" }],
          );
          assert.notInclude(
            encodeJson(traced.spans.map((span) => [attributesOf(span), eventsOf(span)])),
            "private-hook-error",
          );
          yield* harness.close;
        }),
      ),
    );
  });

  // A model request that stopped with `error` ends its span with a failure.
  it.effect(
    "ends a failed model request's span with a failure and no error text in its attributes",
    () => {
      const traced = TestTracer.make();
      return run(backend)(
        tracing(traced)(
          Effect.gen(function* () {
            const setup = chatSetup();
            setup.faux.setResponses([
              fauxAssistantMessage([], {
                stopReason: "error",
                errorMessage: "private-provider-error",
              }),
            ]);
            const { harness, root } = yield* openTracked(yield* controlledStorage, setup);
            yield* settle(yield* root.submit({ type: "input", content: "input" }));
            yield* finished(harness.harness.awaitIdle);
            yield* allSettled(traced);
            const request = named(traced, "durable.generation.request")[0];
            assert.containSubset(attributesOf(request), {
              "gen_ai.response.finish_reasons": ["error"],
            });
            const exit = exitOf(request);
            const error =
              exit !== undefined && Exit.isFailure(exit) ? Cause.squash(exit.cause) : undefined;
            assert.containSubset(error, {
              _tag: "ModelRequestFailed",
              message: "private-provider-error",
            });
            assert.notInclude(
              encodeJson(traced.spans.map((span) => [attributesOf(span), eventsOf(span)])),
              "private-provider-error",
            );
            yield* harness.close;
          }),
        ),
      );
    },
  );

  // Each fetch of a deferred response is its own `durable.generation.poll` span under its `poll` phase, with the
  // request's GenAI attributes: one span per poll, not one for the response's lifetime.
  it.effect("records each deferred poll as a span of its poll phase", () => {
    const traced = TestTracer.make();
    return run(backend)(
      tracing(traced)(
        Effect.gen(function* () {
          const base = chatSetup({ deferred: { pendingFetches: 1, pollAfterMs: 1 } });
          base.settings.stream = { deferred: true };
          base.faux.setResponses([fauxAssistantMessage("late")]);
          // A model service's own span inside its fetch, as a real provider's HTTP call has.
          const setup: ChatSetup = {
            ...base,
            faux: {
              ...base.faux,
              layer: Layer.effect(
                Models,
                Effect.map(Effect.service(Models), (models) =>
                  Models.of({
                    ...models,
                    fetchDeferred: (model, handle) =>
                      (models.fetchDeferred?.(model, handle) ?? Effect.die("no fetch")).pipe(
                        Effect.withSpan("provider.fetch"),
                      ),
                  }),
                ),
              ).pipe(Layer.provide(base.faux.layer)),
            },
          };
          const { harness, root } = yield* openTracked(yield* controlledStorage, setup);
          assert.strictEqual(
            (yield* settle(yield* root.submit({ type: "input", content: "input" }))).status,
            "done",
          );
          yield* finished(harness.harness.awaitIdle);
          yield* allSettled(traced);
          assert.deepStrictEqual(
            named(traced, "durable.generation.request").map(
              (span) => attributesOf(span)["gen_ai.response.finish_reasons"],
            ),
            [["deferred"]],
          );
          const polls = named(traced, "durable.generation.poll");
          assert.deepStrictEqual(
            polls.map((span) => attributesOf(span)["gen_ai.response.finish_reasons"]),
            [["deferred"], ["stop"]],
          );
          const phases = named(traced, "durable.phase");
          for (const poll of polls) {
            assert.containSubset(attributesOf(poll), {
              "gen_ai.provider.name": "faux",
              "gen_ai.request.model": "faux-1",
              "durable.generation.attempt": 1,
            });
            const phase = phases.find((span) => span.spanId === parentId(poll));
            assert.strictEqual(attributesOf(phase)["durable.phase.name"], "poll");
          }
          const fetches = named(traced, "provider.fetch");
          assert.strictEqual(fetches.length, 2);
          for (const fetch of fetches) {
            assert.isTrue(polls.some((poll) => poll.spanId === parentId(fetch)));
          }
          yield* harness.close;
        }),
      ),
    );
  });

  it.effect("counts an outside-span session failure without starting a span", () => {
    const traced = TestTracer.make();
    return run(backend)(
      tracing(traced)(
        Effect.gen(function* () {
          const setup = chatSetup();
          const storage = yield* controlledStorage;
          const { harness, root } = yield* openTracked(storage, setup);
          storage.failNextCommit("private-storage-error");
          const failed = yield* Effect.exit(
            root.commit(
              Effect.gen(function* () {
                const tx = yield* Tx;
                return yield* tx.appendEntry(root.id, { kind: "app.failed" });
              }),
            ),
          );
          assert.isTrue(Exit.isFailure(failed));
          yield* harness.harness.closed;
          yield* eventually(Effect.sync(() => setup.reports.length === 1));
          assert.strictEqual(setup.reports[0]?.kind, "storage_failed");
          assert.isFalse(
            traced.spans.some(
              (span) => span.name === "durable.submit" || span.name === "durable.phase",
            ),
          );
          assert.notInclude(encodeJson(traced.metrics()), "private-storage-error");
          assert.deepStrictEqual(
            metric(traced, "durable.reports").map((snapshot) => snapshot.attributes),
            [{ type: "storage_failed" }],
          );
        }),
      ),
    );
  });

  it.effect("counts reports raised with a NOOP span without creating a root", () => {
    const traced = TestTracer.make();
    return run(backend)(
      tracing(traced)(
        Effect.gen(function* () {
          const setup = chatSetup();
          const { harness, root } = yield* openTracked(yield* controlledStorage, setup);
          yield* harness.kernel.observeCommits({
            advance: () => {
              throw new Error("private-noop-report");
            },
            end: () => {},
          });
          yield* root.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              return yield* tx.appendEntry(root.id, { kind: "app.note" });
            }),
          );
          yield* eventually(Effect.sync(() => setup.reports.length === 1));
          assert.strictEqual(setup.reports[0]?.kind, "listener_error");
          assert.isFalse(
            traced.spans.some(
              (span) => span.name === "durable.submit" || span.name === "durable.phase",
            ),
          );
          assert.deepStrictEqual(
            metric(traced, "durable.reports").map((snapshot) => snapshot.attributes),
            [{ type: "listener_error" }],
          );
          yield* harness.close;
        }),
      ),
    );
  });

  it.effect("attaches synchronous commit-listener reports to the active submission span", () => {
    const traced = TestTracer.make();
    return run(backend)(
      tracing(traced)(
        Effect.gen(function* () {
          const setup = chatSetup();
          setup.faux.setResponses([fauxAssistantMessage("answer")]);
          const { harness, root } = yield* openTracked(yield* controlledStorage, setup);
          yield* harness.kernel.observeCommits({
            advance: (publication) => {
              if (
                publication.changes.some(
                  (change) => change.type === "entry" && change.value.kind === "core.user",
                )
              ) {
                throw new Error("private-listener-error");
              }
            },
            end: () => {},
          });
          yield* settle(yield* root.submit({ type: "input", content: "input" }));
          yield* finished(harness.harness.awaitIdle);
          yield* allSettled(traced);
          yield* eventually(Effect.sync(() => setup.reports.length === 1));
          const span = named(traced, "durable.submit")[0];
          assert.deepInclude(eventsOf(span), {
            name: "durable.report",
            attributes: { "durable.report.type": "listener_error" },
          });
          assert.notInclude(
            encodeJson(traced.spans.map((span) => [attributesOf(span), eventsOf(span)])),
            "private-listener-error",
          );
          yield* harness.close;
        }),
      ),
    );
  });

  // An asynchronous observer that fails is one whose `advance` returns an Effect that fails later
  // (`guardObserver`).
  it.effect("preserves async listener error containment and report counting", () => {
    const traced = TestTracer.make();
    return run(backend)(
      tracing(traced)(
        Effect.gen(function* () {
          const setup = chatSetup();
          setup.faux.setResponses([fauxAssistantMessage("answer")]);
          const { harness, root } = yield* openTracked(yield* controlledStorage, setup);
          const failed = Deferred.makeUnsafe<void, Error>();
          yield* harness.kernel.observeCommits({
            advance: (publication) => {
              if (
                publication.changes.some(
                  (change) => change.type === "entry" && change.value.kind === "core.user",
                )
              ) {
                return Deferred.await(failed);
              }
              return undefined;
            },
            end: () => {},
          });
          yield* settle(yield* root.submit({ type: "input", content: "input" }));
          yield* finished(harness.harness.awaitIdle);
          yield* allSettled(traced);
          const error = new Error("private-async-listener-error");
          yield* Deferred.fail(failed, error);
          yield* eventually(Effect.sync(() => setup.reports.length === 1));
          yield* allSettled(traced);
          assert.strictEqual(Cause.squash(setup.reports[0]!.cause), error);
          assert.strictEqual(setup.reports[0]?.kind, "listener_error");
          assert.deepStrictEqual(
            metric(traced, "durable.reports").map((snapshot) => snapshot.attributes),
            [{ type: "listener_error" }],
          );
          assert.notInclude(
            encodeJson(traced.spans.map((span) => [attributesOf(span), eventsOf(span)])),
            "private-async-listener-error",
          );
          yield* harness.close;
        }),
      ),
    );
  });

  // Codemode and nested tool journals are not supported.
  it.skip("emits replay for a codemode journal result without rerunning its nested tool", () => {});

  it.effect(
    "reports telemetry adapters that throw while a commit is published, and the commit stands",
    () => {
      const Idle = Task.make({
        name: "test.telemetry-idle",
        version: 1,
        input: Schema.Null,
        checkpoint: Schema.Struct({ phase: Schema.Literal("run") }),
        result: Schema.Null,
        initial: () => ({ phase: "run" as const }),
        phases: { run: () => Effect.void },
        abort: () => Effect.void,
      });
      // A registry whose writes throw: a metric exporter that fails.
      const registry = new (class extends Map<string, never> {
        override set(): this {
          throw new Error("metrics down");
        }
      })();
      const traced = TestTracer.make({
        registry,
        span: (options) => {
          const span = new Tracer.NativeSpan(options);
          if (options.name === "failing") {
            span.event = () => {
              throw new Error("telemetry down");
            };
          }
          return span;
        },
      });
      return run(backend)(
        tracing(traced)(
          Effect.gen(function* () {
            const h = yield* openHarness(yield* controlledStorage, [Idle]);
            const root = yield* h.harness.ensureRoot();
            const seqs: Array<number> = [];
            yield* h.kernel.observeCommits({
              advance: (publication) => {
                seqs.push(publication.seq);
              },
              end: () => {},
            });
            const { entry, task } = yield* root
              .commit(
                Effect.gen(function* () {
                  const tx = yield* Tx;
                  const entry = yield* tx.appendEntry(root.id, { kind: "note", data: {} });
                  const task = yield* tx.createTask(Idle, null, {
                    ownership: { kind: "conversation" },
                  });
                  return { entry: entry.id, task };
                }),
              )
              .pipe(Effect.withSpan("failing"));
            // The entry event, then the active-task metric; the reports' own event and metric throw too and are
            // dropped.
            yield* eventually(Effect.sync(() => h.reports.length === 2));
            assert.deepStrictEqual(
              h.reports.map(({ cause: _cause, ...info }) => info),
              [
                { kind: "host_error", callback: "telemetry" },
                { kind: "host_error", callback: "telemetry" },
              ],
            );
            assert.strictEqual(seqs.length, 1);
            const entries = yield* h.harness.commit(
              Effect.gen(function* () {
                const tx = yield* Tx;
                return yield* tx.entry(entry);
              }),
            );
            assert.isTrue(Option.isSome(entries));
            assert.strictEqual(
              Option.getOrUndefined(yield* h.harness.getTask(task))?.state.status,
              "pending",
            );
            yield* h.close;
            assert.deepStrictEqual(yield* h.harness.closed, { reason: "closed" });
          }),
        ),
      );
    },
  );

  // The kernel sets its spans' attributes and links through guards, so a host tracer that throws there fails no
  // admission, phase or model request. Effect's own `withSpan` sets the `attributes` it is given with no guard, so
  // the throwing setters are on the kernel's three spans only.
  it.effect("completes a turn when the tracer's setters throw on the kernel's spans", () => {
    const kernelSpans = new Set(["durable.submit", "durable.phase", "durable.generation.request"]);
    const traced = TestTracer.make({
      span: (options) => {
        const span = new Tracer.NativeSpan(options);
        if (kernelSpans.has(options.name)) {
          span.attribute = () => {
            throw new Error("tracer down");
          };
          span.addLinks = () => {
            throw new Error("tracer down");
          };
          span.event = () => {
            throw new Error("tracer down");
          };
        }
        return span;
      },
    });
    return run(backend)(
      tracing(traced)(
        Effect.gen(function* () {
          const setup = chatSetup();
          const reachedHold = Deferred.makeUnsafe<void>();
          const release = Deferred.makeUnsafe<void>();
          const hold = Extension.make({
            name: "tool:hold",
            tools: [
              AgentTool.make("hold", {
                description: "hold",
                parameters: Schema.Struct({}),
                jsonSchema: EMPTY_OBJECT,
                execute: () =>
                  Deferred.succeed(reachedHold, undefined).pipe(
                    Effect.andThen(Deferred.await(release)),
                    Effect.as({ output: [] }),
                  ),
              }),
            ],
          });
          setup.faux.setResponses([toolReply("hold"), fauxAssistantMessage("answer")]);
          const { harness, root } = yield* openTracked(yield* controlledStorage, setup, [hold]);
          const first = yield* root.submit({ type: "input", content: "first" });
          yield* reached(reachedHold);
          const steer = yield* root.submit({ type: "input", content: "steer", whenBusy: "steer" });
          yield* Deferred.succeed(release, undefined);
          assert.strictEqual((yield* settle(first)).status, "done");
          assert.strictEqual((yield* settle(steer)).status, "done");
          yield* finished(harness.harness.awaitIdle);
          // The phases' throwing setters are reported; the admissions' and the requests' are dropped.
          yield* eventually(Effect.sync(() => setup.reports.length > 0));
          assert.isTrue(
            setup.reports.every(
              (report) => report.kind === "host_error" && report.callback === "telemetry",
            ),
          );
          assert.isTrue(named(traced, "durable.generation.request").length >= 2);
          yield* harness.close;
        }),
      ),
    );
  });

  it.effect("drops a report's own metric that throws, after onReport got the report", () => {
    const registry = new (class extends Map<string, never> {
      override set(): this {
        throw new Error("metrics down");
      }
    })();
    const traced = TestTracer.make({ registry });
    return run(backend)(
      tracing(traced)(
        Effect.gen(function* () {
          const h = yield* openHarness(yield* controlledStorage, []);
          const root = yield* h.harness.ensureRoot();
          const bug = new Error("listener bug");
          yield* h.kernel.observeCommits({
            advance: () => {
              throw bug;
            },
            end: () => {},
          });
          yield* root.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              return yield* tx.appendEntry(root.id, { kind: "note", data: {} });
            }),
          );
          yield* eventually(Effect.sync(() => h.reports.length === 1));
          assert.strictEqual(Cause.squash(h.reports[0]!.cause), bug);
          assert.strictEqual(h.reports[0]?.kind, "listener_error");
          yield* h.close;
          assert.deepStrictEqual(yield* h.harness.closed, { reason: "closed" });
        }),
      ),
    );
  });
});

describeEachBackend("HarnessOptions.telemetry", (backend) => {
  // A Harness traces with Effect's `Tracer` in context, whichever is there, the default one included, and has no
  // telemetry option of its own.
  it.effect("records with a provided tracer and runs with the default one", () => {
    const traced = TestTracer.make();
    return run(backend)(
      Effect.gen(function* () {
        const untraced = yield* openHarness(yield* controlledStorage, [Finish]);
        const id = yield* (yield* untraced.harness.ensureRoot()).commit(create(Finish));
        yield* untraced.harness.resume;
        yield* finished(untraced.harness.awaitTask(id));
        yield* untraced.close;
        yield* tracing(traced)(
          Effect.gen(function* () {
            const h = yield* openHarness(yield* controlledStorage, [Finish]);
            const created = yield* (yield* h.harness.ensureRoot()).commit(create(Finish));
            yield* h.harness.resume;
            yield* finished(h.harness.awaitTask(created));
            yield* h.close;
          }),
        );
        assert.isTrue(traced.spans.some((span) => span.name === "durable.phase"));
      }),
    );
  });
});

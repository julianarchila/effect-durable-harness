// The generation task: model requests, streaming, settings, run failures and deferred responses, on every backend.
//
// Storage is a `ControlledStorage` over each backend; `faux` is `FauxModel`, whose response factories take the
// request (messages, reasoning, provider session and pinned options in one `GenerateRequest`); settings are the
// reader of `setup.settings`; extensions are the static extensions of `openChat`; waits let pending work run while
// the `TestClock` advances in 5 ms steps; `harness.observeCommits` and `commitWith` are the kernel's.
//
// - "forwards stream options and the thinking level": no abort signal is forwarded, since cancellation is
//   interruption of the stream; the pinned options are `request.options`.
// - "reads settings through getters at every decision": the getter is on the object the settings reader
//   returns.
// - "resolves settings over the built-in defaults": `Settings.resolve`.
// - "commits partials no more often than progress.partialIntervalMs": a `Models` layer with a custom stream.
// - "faults a run task, settling its inputs and converting the committed partial": the `model` column is
//   typed, so no non-JSON final message can reach the classification commit; the fault is a defect of the
//   model stream after its first partial was committed, which the scheduler faults the same way, and the
//   `faulted` detail is the stream's defect.
// - The built-in tasks are always registered: an extension declaring `core.generation` fails the layer build.
// - `core.live` after an aborted, failed, no-model, faulted or orphaned run holds `runFailure` (its time checked with
//   `anyNumber`); `run` literals carry `turns`; settings resolve `afterFailedRun`.
//
// Skipped: "attributes model requests by default and merges the Harness identity over it": there is no request
// identity or attribution.
//
// Deferred responses: `withModels` is a `Models` layer over the fake's with members replaced, such as
// `cancelDeferred`; the failed cancellation's report is a `host_error` with callback `models` whose cause is
// `DeferredCancelFailed`.
import { assert, it } from "@effect/vitest";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Predicate from "effect/Predicate";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as Doc from "../src/Doc.ts";
import { AssistantEntry, UserEntry } from "../src/Entry.ts";
import * as Extension from "../src/Extension.ts";
import type { Conversation } from "../src/Harness.ts";
import type { EntryId, TaskId } from "../src/Ids.ts";
import { GenerationTask } from "../src/internal/generation.ts";
import { LiveDoc, type LiveState } from "../src/Live.ts";
import {
  type AssistantMessageEvent,
  DeferredCancelFailed,
  type GenerateRequest,
  Models,
  type ResolvedModel,
  terminalEvent,
} from "../src/Models.ts";
import { ProviderDoc } from "../src/ProviderSession.ts";
import * as Section from "../src/Section.ts";
import * as Settings from "../src/Settings.ts";
import type { HarnessSettings } from "../src/Settings.ts";
import type { Submission } from "../src/Submission.ts";
import * as Task from "../src/Task.ts";
import { fauxAssistantMessage } from "../src/testing/FauxModel.ts";
import { Tx } from "../src/Tx.ts";
import {
  allEntries,
  type ChatSetup,
  chatSetup,
  FAUX_MODEL,
  openChat,
  textOf,
  unanswered,
  waitFor,
} from "./ChatSupport.ts";
import { anyNumber, awaited, type TestHarness } from "./HarnessSupport.ts";
import { controlledStorage, describeEachBackend, failureMessage, run } from "./SessionSupport.ts";

const ERROR_503 = fauxAssistantMessage([], {
  stopReason: "error",
  errorMessage: "503 Service Unavailable",
});

/** One extension per section, named `section:<key>`. */
const addSection = <E, R>(
  key: string,
  render: (input: Section.PromptInput) => Effect.Effect<string | undefined, E, R>,
  options?: Section.SectionOptions,
) => Extension.make({ name: `section:${key}`, sections: [Section.make(key, render, options)] });

const live = (h: TestHarness, conversation: Conversation) =>
  h.session.snapshot(LiveDoc, conversation.id).pipe(Effect.map(Option.getOrUndefined));

/** The run's task once `core.live.run` names one. */
const runTask = (h: TestHarness, conversation: Conversation) =>
  Effect.gen(function* () {
    let taskId: TaskId | undefined;
    yield* waitFor(
      live(h, conversation).pipe(
        Effect.map((state) => {
          taskId = state?.run?.taskId;
          return taskId !== undefined;
        }),
      ),
    );
    // SAFETY: `waitFor` returned, so the check saw a task.
    return taskId as TaskId;
  });

/** Waits for the settlement while timers run in 5 ms steps. */
const settle = (submission: Submission) =>
  Effect.gen(function* () {
    const fiber = yield* Effect.forkChild(submission.await, { startImmediately: true });
    yield* waitFor(Effect.sync(() => fiber.pollUnsafe() !== undefined));
    return yield* Fiber.join(fiber);
  });

/** Waits for a task, with timers running. */
const settleTask = (h: TestHarness, id: TaskId) =>
  Effect.gen(function* () {
    const fiber = yield* Effect.forkChild(h.harness.awaitTask(id), { startImmediately: true });
    yield* waitFor(Effect.sync(() => fiber.pollUnsafe() !== undefined));
    return yield* Fiber.join(fiber);
  });

/** Every `core.live` value the commits of `h` publish from now on. */
// Follows the Session's own observers (`followCommits`), which see deferred partials when memory adopts them.
const livePublications = (h: TestHarness) =>
  Effect.gen(function* () {
    const values: Array<LiveState> = [];
    yield* h.kernel.followCommits((publication) => {
      for (const change of publication.changes) {
        if (
          change.type === "document" &&
          change.record.kind === "core.live" &&
          change.value !== null
        ) {
          // SAFETY: a published `core.live` value, which its schema checked when it was written.
          values.push(change.value as LiveState);
        }
      }
    });
    return values;
  });

const tasksOf = (h: TestHarness, conversation: Conversation) =>
  h.harness
    .commit(
      Effect.gen(function* () {
        const tx = yield* Tx;
        return yield* tx.scanTasks({ conversationId: conversation.id }, 10);
      }),
    )
    .pipe(Effect.map((page) => page.items));

const entryOf = (conversation: Conversation, id: EntryId) =>
  conversation.commit(
    Effect.gen(function* () {
      const tx = yield* Tx;
      return Option.getOrUndefined(yield* tx.entry(AssistantEntry, id));
    }),
  );

/** The fake's `Models` with `stream` replaced. */
const withStream = (
  setup: ChatSetup,
  stream: (model: ResolvedModel, request: GenerateRequest) => Stream.Stream<AssistantMessageEvent>,
): ChatSetup => ({
  ...setup,
  faux: {
    ...setup.faux,
    layer: Layer.succeed(
      Models,
      Models.of({
        resolve: (ref) =>
          Effect.succeed(
            ref.provider === setup.faux.provider
              ? Option.fromUndefinedOr(setup.faux.getModel(ref.modelId))
              : Option.none(),
          ),
        stream,
      }),
    ),
  },
});

/** The fake's `Models` with some members replaced. */
const withModels = (
  setup: ChatSetup,
  replace: (base: Models["Service"]) => Partial<Models["Service"]>,
): ChatSetup => ({
  ...setup,
  faux: {
    ...setup.faux,
    layer: Layer.effect(
      Models,
      Effect.map(Effect.service(Models), (base) => Models.of({ ...base, ...replace(base) })),
    ).pipe(Layer.provide(setup.faux.layer)),
  },
});

/** A stream that emits one partial, then waits 300 ms: longer than the default throttle. */
const shortStream = (end: Stream.Stream<AssistantMessageEvent>) =>
  Stream.make<ReadonlyArray<AssistantMessageEvent>>({
    type: "start",
    partial: fauxAssistantMessage("partial", { stopReason: "pending" }),
  }).pipe(
    Stream.concat(Stream.fromEffect(Effect.sleep("300 millis")).pipe(Stream.drain)),
    Stream.concat(end),
  );

/** The message of a conversation's first `core.system` entry. */
const firstSystem = (conversation: Conversation) =>
  allEntries(conversation).pipe(
    Effect.map((entries) => entries.find((entry) => entry.kind === "core.system")?.model?.[0]),
  );

const messageText = (state: LiveState | undefined) => textOf(state?.generation?.message);

describeEachBackend("generation", (backend) => {
  it.effect("answers an input and settles its submission", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        setup.faux.setResponses([fauxAssistantMessage("Hello there")]);
        const storage = yield* controlledStorage;
        const { harness: h, root } = yield* openChat(storage, setup, {
          extensions: [
            addSection("preamble", () => Effect.succeed("You are helpful."), { tag: false }),
          ],
        });
        yield* h.harness.resume;
        const submission = yield* root.submit({ type: "input", content: "hi" });
        const settled = yield* settle(submission);
        if (settled.status !== "done" || settled.type !== "input")
          return assert.fail(`Unexpected ${settled.status}`);

        const answer = yield* entryOf(root, settled.answer);
        assert.strictEqual(textOf(answer?.model?.[0]), "Hello there");
        const entries = yield* allEntries(root);
        assert.deepStrictEqual(
          entries.map((entry) => entry.kind),
          ["core.user", "core.system", "core.assistant"],
        );
        assert.strictEqual(entries[0]?.id, settled.entry);
        const system = entries[1]?.model?.[0];
        assert.deepStrictEqual(system, {
          role: "system",
          content: "",
          sections: { preamble: "You are helpful." },
          timestamp: system?.timestamp ?? Number.NaN,
        });
        assert.isNumber(system?.timestamp);
        assert.deepStrictEqual(yield* live(h, root), {});
        const task = (yield* tasksOf(h, root))[0];
        assert.strictEqual(task?.kind, "core.generation");
        // Entries written by the generation are attributed to it; the admitted user entry is not task work.
        assert.deepStrictEqual(
          entries.map((entry) => entry.byTaskId),
          [undefined, task?.id, task?.id],
        );
        assert.deepStrictEqual(task?.state, {
          status: "terminal",
          outcome: { status: "completed", result: { entryId: settled.answer } },
        });
        yield* h.close;
      }),
    ),
  );

  it.effect("stores partials as deltas and a complete base once nothing is in flight", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup({ tokensPerSecond: 200, tokenSize: { min: 1, max: 1 } });
        setup.faux.setResponses([fauxAssistantMessage("w".repeat(200))]);
        // Store deferred partials well within the stream, so some reach Storage before the answer.
        setup.settings.progress = { persistWithinMs: 100 };
        const storage = yield* controlledStorage;
        const { harness: h, root } = yield* openChat(storage, setup);
        const record = Option.getOrThrow(
          yield* storage.findDocument(
            { kind: "core.live", scope: { kind: "conversation", conversationId: root.id } },
            "current",
          ),
        );
        yield* h.harness.resume;
        yield* settle(yield* root.submit({ type: "input", content: "hi" }));
        const contents = storage.commits.flatMap((writes) =>
          writes.flatMap((write) =>
            write.type === "document.change" && write.id === record.id ? [write.content.kind] : [],
          ),
        );
        // Streaming writes deltas; the commit that settles the answer clears generation and writes a base.
        assert.include(contents, "delta");
        assert.strictEqual(contents.at(-1), "base");
        yield* h.close;
      }),
    ),
  );

  it.effect("still ends a run whose input something else already settled", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        const busy = unanswered();
        setup.faux.setResponses([busy.step]);
        const storage = yield* controlledStorage;
        const { harness: h, root } = yield* openChat(storage, setup);
        const submission = yield* root.submit({ type: "input", content: "hi" });
        yield* awaited(busy.reached);
        yield* root.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            yield* tx.settleSubmission(submission.id, {
              status: "unanswered",
              reason: "withdrawn",
            });
          }),
        );
        const taskId = yield* runTask(h, root);
        yield* h.harness.abortTask(taskId);
        assert.deepStrictEqual((yield* settleTask(h, taskId)).state.outcome, {
          status: "aborted",
        });
        // The earlier settlement stays; the run's own settlement leaves it unchanged.
        const status = yield* submission.status;
        assert.strictEqual(status.status, "unanswered");
        assert.strictEqual(status.status === "unanswered" ? status.reason : undefined, "withdrawn");
        // A run ends without an answer even when its inputs were settled already.
        const state = yield* live(h, root);
        assert.deepStrictEqual(state, {
          runFailure: {
            taskId,
            inputs: [submission.id],
            reason: "aborted",
            first: anyNumber(state?.runFailure?.first),
          },
        });
        yield* h.close;
      }),
    ),
  );

  it.effect("fails with no_model when no model is configured or the model is unknown", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        const storage = yield* controlledStorage;
        const { harness: h, root } = yield* openChat(storage, setup);
        const plain = yield* h.harness.createConversation({ ownership: { kind: "ownerless" } });
        yield* h.harness.resume;
        const unset = yield* settle(yield* plain.submit({ type: "input", content: "hi" }));
        assert.strictEqual(unset.status, "unanswered");
        assert.strictEqual(unset.status === "unanswered" ? unset.reason : undefined, "no_model");

        yield* root.configure({ model: { provider: "faux", modelId: "missing" } });
        const unknown = yield* settle(yield* root.submit({ type: "input", content: "hi" }));
        if (unknown.status !== "unanswered") return assert.fail(`Unexpected ${unknown.status}`);
        assert.strictEqual(unknown.reason, "no_model");
        assert.isNumber(unknown.type === "input" ? unknown.entry : undefined);
        assert.deepStrictEqual(
          (yield* allEntries(root)).map((entry) => entry.kind),
          ["core.user"],
        );
        const tasks = yield* tasksOf(h, root);
        assert.deepStrictEqual(tasks[0]?.state, {
          status: "terminal",
          outcome: {
            status: "failed",
            error: {
              message: "Model faux/missing is not available",
              detail: { reason: "no_model" },
            },
          },
        });
        const state = yield* live(h, root);
        assert.deepStrictEqual(state, {
          runFailure: {
            taskId: tasks[0]?.id ?? assert.fail("no task"),
            inputs: [anyNumber(state?.runFailure?.inputs[0])],
            reason: "no_model",
            detail: "Model faux/missing is not available",
            first: anyNumber(state?.runFailure?.first),
          },
        });
        const plainFailure = (yield* live(h, plain))?.runFailure;
        assert.strictEqual(plainFailure?.reason, "no_model");
        assert.strictEqual(plainFailure?.detail, "No model is configured");
        yield* h.close;
      }),
    ),
  );

  it.effect("retries a retryable error after a durable backoff and then answers", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        setup.faux.setResponses([ERROR_503, fauxAssistantMessage("recovered")]);
        setup.settings.retry = { enabled: true, maxRetries: 3, baseDelayMs: 1 };
        const storage = yield* controlledStorage;
        const { harness: h, root } = yield* openChat(storage, setup, {
          extensions: [addSection("preamble", () => Effect.succeed("p"), { tag: false })],
        });
        const values = yield* livePublications(h);
        yield* h.harness.resume;
        const settled = yield* settle(yield* root.submit({ type: "input", content: "hi" }));
        assert.strictEqual(settled.status, "done");
        const entries = yield* allEntries(root);
        assert.deepStrictEqual(
          entries.map((entry) => entry.kind),
          ["core.user", "core.system", "core.assistant", "core.assistant"],
        );
        const failed = entries[2]?.model?.[0];
        assert.strictEqual(failed?.role === "assistant" ? failed.stopReason : undefined, "error");
        assert.isTrue(
          values.some((value) => value.generation?.retry?.error === "503 Service Unavailable"),
        );
        assert.isTrue(values.some((value) => value.generation?.attempt === 2));
        assert.deepStrictEqual(yield* live(h, root), {});
        yield* h.close;
      }),
    ),
  );

  it.effect("fails with model_error once retries are exhausted", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        setup.faux.setResponses([ERROR_503, ERROR_503, fauxAssistantMessage("never")]);
        setup.settings.retry = { enabled: true, maxRetries: 1, baseDelayMs: 1 };
        const storage = yield* controlledStorage;
        const { harness: h, root } = yield* openChat(storage, setup);
        yield* h.harness.resume;
        const settled = yield* settle(yield* root.submit({ type: "input", content: "hi" }));
        if (settled.status !== "unanswered") return assert.fail(`Unexpected ${settled.status}`);
        assert.strictEqual(settled.reason, "model_error");
        assert.strictEqual(settled.detail, "503 Service Unavailable");
        assert.deepStrictEqual(
          (yield* allEntries(root)).map((entry) => entry.kind),
          ["core.user", "core.assistant", "core.assistant"],
        );
        assert.strictEqual(setup.faux.pendingResponseCount(), 1);
        yield* h.close;
      }),
    ),
  );

  it.effect("fails a retryable error without retrying when the retry policy is disabled", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        setup.faux.setResponses([ERROR_503, fauxAssistantMessage("never")]);
        setup.settings.retry = { enabled: false };
        const storage = yield* controlledStorage;
        const { harness: h, root } = yield* openChat(storage, setup);
        yield* h.harness.resume;
        const settled = yield* settle(yield* root.submit({ type: "input", content: "hi" }));
        assert.strictEqual(settled.status, "unanswered");
        assert.strictEqual(
          settled.status === "unanswered" ? settled.reason : undefined,
          "model_error",
        );
        assert.strictEqual(setup.faux.state.callCount, 1);
        yield* h.close;
      }),
    ),
  );

  it.effect("reports section wrapper failures while preparing", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        setup.faux.setResponses([fauxAssistantMessage("ok")]);
        const storage = yield* controlledStorage;
        const { harness: h, root } = yield* openChat(storage, setup, {
          extensions: [
            addSection("cwd", () => Effect.succeed("/repo")),
            Extension.make({
              name: "broken",
              wraps: [
                Extension.wrapSection("cwd", () => {
                  throw new Error("wrapper failed");
                }),
              ],
            }),
          ],
        });
        yield* h.harness.resume;
        const settled = yield* settle(yield* root.submit({ type: "input", content: "hi" }));
        assert.strictEqual(settled.status, "done");
        assert.isTrue(
          setup.reports.some((report) => {
            const error = Cause.squash(report.cause);
            return error instanceof Error && error.message === "wrapper failed";
          }),
        );
        // The failed section is absent, so nothing was rendered.
        assert.deepStrictEqual(
          (yield* allEntries(root)).map((entry) => entry.kind),
          ["core.user", "core.assistant"],
        );
        yield* h.close;
      }),
    ),
  );

  it.effect("fails a non-retryable error without retrying", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        setup.faux.setResponses([
          fauxAssistantMessage([], { stopReason: "error", errorMessage: "Invalid request" }),
          fauxAssistantMessage("never"),
        ]);
        const storage = yield* controlledStorage;
        const { harness: h, root } = yield* openChat(storage, setup);
        yield* h.harness.resume;
        const settled = yield* settle(yield* root.submit({ type: "input", content: "hi" }));
        if (settled.status !== "unanswered") return assert.fail(`Unexpected ${settled.status}`);
        assert.strictEqual(settled.reason, "model_error");
        assert.strictEqual(settled.detail, "Invalid request");
        const tasks = yield* tasksOf(h, root);
        const state = tasks[0]?.state;
        assert.deepStrictEqual(state?.status === "terminal" ? state.outcome : undefined, {
          status: "failed",
          error: { message: "Invalid request", detail: { reason: "model_error" } },
        });
        yield* h.close;
      }),
    ),
  );

  it.effect("polls a deferred response until it is ready", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup({ deferred: { pendingFetches: 1, pollAfterMs: 1 } });
        setup.faux.setResponses([fauxAssistantMessage("deferred answer")]);
        setup.settings.stream = { deferred: true };
        const storage = yield* controlledStorage;
        const { harness: h, root } = yield* openChat(storage, setup);
        const values = yield* livePublications(h);
        yield* h.harness.resume;
        const settled = yield* settle(yield* root.submit({ type: "input", content: "hi" }));
        if (settled.status !== "done" || settled.type !== "input") {
          return assert.fail(`Unexpected ${settled.status}`);
        }
        assert.strictEqual(setup.faux.state.deferredFetchCount, 2);
        const pollTimes = values.flatMap((value) =>
          value.generation?.deferred === undefined ? [] : [value.generation.deferred.pollAt],
        );
        assert.strictEqual(pollTimes.length, 2);
        assert.isAbove(pollTimes[1] ?? 0, pollTimes[0] ?? 0);
        const answer = yield* entryOf(root, settled.answer);
        assert.strictEqual(textOf(answer?.model?.[0]), "deferred answer");
        yield* h.close;
      }),
    ),
  );

  it.effect("converts the committed partial when aborted during streaming", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup({ tokensPerSecond: 20, tokenSize: { min: 1, max: 1 } });
        setup.faux.setResponses([fauxAssistantMessage("x".repeat(400))]);
        const storage = yield* controlledStorage;
        const { harness: h, root } = yield* openChat(storage, setup);
        yield* h.harness.resume;
        const submission = yield* root.submit({ type: "input", content: "hi" });
        const taskId = yield* runTask(h, root);
        yield* waitFor(live(h, root).pipe(Effect.map((state) => messageText(state) !== undefined)));
        const partial = messageText(yield* live(h, root));
        assert.deepStrictEqual(yield* h.harness.abortTask(taskId), { _tag: "Marked" });
        const settled = yield* settle(submission);
        assert.strictEqual(settled.status, "unanswered");
        assert.strictEqual(settled.status === "unanswered" ? settled.reason : undefined, "aborted");
        const entries = yield* allEntries(root);
        assert.deepStrictEqual(
          entries.map((entry) => entry.kind),
          ["core.user", "core.assistant"],
        );
        const converted = entries[1]?.model?.[0];
        assert.strictEqual(
          converted?.role === "assistant" ? converted.stopReason : undefined,
          "aborted",
        );
        assert.isTrue(textOf(converted)?.startsWith(partial ?? "missing"));
        const state = yield* live(h, root);
        assert.deepStrictEqual(state, {
          runFailure: {
            taskId,
            inputs: [submission.id],
            reason: "aborted",
            first: anyNumber(state?.runFailure?.first),
          },
        });
        assert.deepStrictEqual((yield* settleTask(h, taskId)).state.outcome, {
          status: "aborted",
        });
        yield* h.close;
      }),
    ),
  );

  it.effect("cancels a deferred response when aborted during polling", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup({ deferred: { pendingFetches: 100, pollAfterMs: 60_000 } });
        setup.faux.setResponses([fauxAssistantMessage("never")]);
        setup.settings.stream = { deferred: true };
        const storage = yield* controlledStorage;
        const { harness: h, root } = yield* openChat(storage, setup);
        yield* h.harness.resume;
        const submission = yield* root.submit({ type: "input", content: "hi" });
        const taskId = yield* runTask(h, root);
        yield* waitFor(
          live(h, root).pipe(Effect.map((state) => state?.generation?.deferred !== undefined)),
        );
        yield* h.harness.abortTask(taskId);
        const settled = yield* settle(submission);
        assert.strictEqual(settled.status === "unanswered" ? settled.reason : undefined, "aborted");
        assert.strictEqual(setup.faux.state.cancelledDeferred.length, 1);
        const state = yield* live(h, root);
        assert.deepStrictEqual(state, {
          runFailure: {
            taskId,
            inputs: [submission.id],
            reason: "aborted",
            first: anyNumber(state?.runFailure?.first),
          },
        });
        yield* h.close;
      }),
    ),
  );

  it.effect("reports a failed deferred cancellation and still ends the run aborted", () =>
    run(backend)(
      Effect.gen(function* () {
        const base = chatSetup({ deferred: { pendingFetches: 100, pollAfterMs: 60_000 } });
        base.faux.setResponses([fauxAssistantMessage("never")]);
        const setup = withModels(base, () => ({
          cancelDeferred: () => Effect.fail(new DeferredCancelFailed({ message: "cancel failed" })),
        }));
        setup.settings.stream = { deferred: true };
        const storage = yield* controlledStorage;
        const { harness: h, root } = yield* openChat(storage, setup);
        yield* h.harness.resume;
        const submission = yield* root.submit({ type: "input", content: "hi" });
        const taskId = yield* runTask(h, root);
        yield* waitFor(
          live(h, root).pipe(Effect.map((state) => state?.generation?.deferred !== undefined)),
        );
        yield* h.harness.abortTask(taskId);
        const settled = yield* settle(submission);
        assert.strictEqual(settled.status === "unanswered" ? settled.reason : undefined, "aborted");
        const reported = setup.reports.find(
          (report) => report.kind === "host_error" && report.callback === "models",
        );
        const error = Option.getOrUndefined(Cause.findErrorOption(reported?.cause ?? Cause.empty));
        assert.isTrue(Schema.is(DeferredCancelFailed)(error));
        assert.strictEqual(
          Schema.is(DeferredCancelFailed)(error) ? error.message : undefined,
          "cancel failed",
        );
        const state = yield* live(h, root);
        assert.deepStrictEqual(state, {
          runFailure: {
            taskId,
            inputs: [submission.id],
            reason: "aborted",
            first: anyNumber(state?.runFailure?.first),
          },
        });
        yield* h.close;
      }),
    ),
  );

  it.effect("forwards stream options and the thinking level", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        const seen: Array<GenerateRequest> = [];
        setup.faux.setResponses([
          (request) =>
            Effect.sync(() => {
              seen.push(request);
              return fauxAssistantMessage("a");
            }),
          (request) =>
            Effect.sync(() => {
              seen.push(request);
              return fauxAssistantMessage("b");
            }),
        ]);
        setup.settings.stream = { timeoutMs: 1234, headers: { "x-test": "1" } };
        const storage = yield* controlledStorage;
        const { harness: h, root } = yield* openChat(storage, setup);
        yield* root.configure({ thinkingLevel: "high" });
        yield* h.harness.resume;
        yield* settle(yield* root.submit({ type: "input", content: "one" }));
        // Both are read at the next preparation: the thinking level from core.agent, the stream options live from settings.
        yield* root.configure({ thinkingLevel: null });
        setup.settings.stream = { timeoutMs: 99 };
        yield* settle(yield* root.submit({ type: "input", content: "two" }));
        const sessionId = Option.getOrThrow(
          yield* h.session.snapshot(ProviderDoc, root.id),
        ).sessionId;
        assert.deepStrictEqual(seen[0]?.options, { timeoutMs: 1234, headers: { "x-test": "1" } });
        assert.strictEqual(seen[0]?.reasoning, "high");
        assert.strictEqual(seen[0]?.sessionId, sessionId);
        assert.isUndefined(seen[1]?.reasoning);
        assert.deepStrictEqual(seen[1]?.options, { timeoutMs: 99 });
        assert.strictEqual(seen[1]?.sessionId, sessionId);
        assert.isUndefined(seen[1]?.options.headers);
        yield* h.close;
      }),
    ),
  );

  it.effect("records the requested thinking level on answers, streamed and polled", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup({ deferred: { pendingFetches: 1, pollAfterMs: 1 } });
        setup.faux.setResponses([fauxAssistantMessage("streamed"), fauxAssistantMessage("polled")]);
        const storage = yield* controlledStorage;
        const { harness: h, root } = yield* openChat(storage, setup);
        yield* h.harness.resume;
        const answer = (content: string) =>
          Effect.gen(function* () {
            const settled = yield* settle(yield* root.submit({ type: "input", content }));
            if (settled.status !== "done" || settled.type !== "input") {
              return assert.fail(`Unexpected ${settled.status}`);
            }
            const message = (yield* entryOf(root, settled.answer))?.model?.[0];
            return message?.role === "assistant" ? message.thinkingLevel : undefined;
          });
        yield* root.configure({ thinkingLevel: "high" });
        assert.strictEqual(yield* answer("one"), "high");
        setup.settings.stream = { deferred: true };
        assert.strictEqual(yield* answer("two"), "high");
        assert.strictEqual(setup.faux.state.deferredFetchCount, 2);
        yield* h.close;
      }),
    ),
  );

  // There is no request identity or attribution.
  it.skip("attributes model requests by default and merges the Harness identity over it", () => {});

  it.effect("keeps provider session IDs request-local across concurrent conversations", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        const seen = new Map<string, Array<string>>();
        const capture = (request: GenerateRequest) =>
          Effect.sync(() => {
            const message = request.messages.findLast((candidate) => candidate.role === "user");
            const content = message?.role === "user" ? message.content : undefined;
            const text = Predicate.isString(content) ? content : "";
            const values = seen.get(text) ?? [];
            values.push(request.sessionId ?? "");
            seen.set(text, values);
            return fauxAssistantMessage(`answer:${text}`);
          });
        setup.faux.setResponses([capture, capture, capture, capture]);
        const storage = yield* controlledStorage;
        const { harness: h, root } = yield* openChat(storage, setup);
        const child = yield* h.harness.createConversation({
          ownership: { kind: "ownerless" },
          agent: { model: FAUX_MODEL },
        });
        yield* h.harness.resume;
        for (const round of ["first", "second"]) {
          const submissions = yield* Effect.forEach([root, child], (conversation, index) =>
            conversation.submit({ type: "input", content: `${round}-${index}` }),
          );
          yield* Effect.forEach(submissions, settle);
        }
        const rootId = Option.getOrThrow(yield* h.session.snapshot(ProviderDoc, root.id)).sessionId;
        const childId = Option.getOrThrow(
          yield* h.session.snapshot(ProviderDoc, child.id),
        ).sessionId;
        assert.notStrictEqual(rootId, childId);
        assert.deepStrictEqual(seen.get("first-0"), [rootId]);
        assert.deepStrictEqual(seen.get("second-0"), [rootId]);
        assert.deepStrictEqual(seen.get("first-1"), [childId]);
        assert.deepStrictEqual(seen.get("second-1"), [childId]);
        yield* h.close;
      }),
    ),
  );

  it.effect("creates and persists provider state before a legacy conversation's request", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        let sent: string | undefined;
        setup.faux.setResponses([
          (request) =>
            Effect.sync(() => {
              sent = request.sessionId;
              return fauxAssistantMessage("ok");
            }),
        ]);
        const storage = yield* controlledStorage;
        const { harness: h, root } = yield* openChat(storage, setup);
        yield* root.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            yield* tx.retireDoc(ProviderDoc, root.id);
          }),
        );
        assert.isTrue(Option.isNone(yield* h.session.snapshot(ProviderDoc, root.id)));
        yield* h.harness.resume;
        yield* settle(yield* root.submit({ type: "input", content: "legacy" }));
        const stored = Option.getOrUndefined(yield* h.session.snapshot(ProviderDoc, root.id));
        assert.match(
          stored?.sessionId ?? "",
          /^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/u,
        );
        assert.strictEqual(sent, stored?.sessionId);
        yield* h.close;
      }),
    ),
  );

  it.effect("reads settings through getters at every decision", () =>
    run(backend)(
      Effect.gen(function* () {
        const base = chatSetup();
        let timeoutMs = 111;
        const seen: Array<number | undefined> = [];
        base.faux.setResponses([
          (request) =>
            Effect.sync(() => {
              seen.push(request.options.timeoutMs);
              // The user changes the setting while the first attempt runs.
              timeoutMs = 222;
              return ERROR_503;
            }),
          (request) =>
            Effect.sync(() => {
              seen.push(request.options.timeoutMs);
              return fauxAssistantMessage("ok");
            }),
        ]);
        const settings: HarnessSettings = {
          get stream() {
            return { timeoutMs };
          },
          retry: { baseDelayMs: 1 },
        };
        const setup: ChatSetup = { ...base, settings };
        const storage = yield* controlledStorage;
        const { harness: h, root } = yield* openChat(storage, setup);
        yield* h.harness.resume;
        const settled = yield* settle(yield* root.submit({ type: "input", content: "hi" }));
        assert.strictEqual(settled.status, "done");
        // The retry prepares again, so it resolves the settings again and sends the new timeout.
        assert.deepStrictEqual(seen, [111, 222]);
        yield* h.close;
      }),
    ),
  );

  it("resolves settings over the built-in defaults", () => {
    const resolved = Settings.resolve(undefined);
    assert.deepStrictEqual(resolved, {
      // An unset key.
      extensions: undefined,
      stream: {},
      retry: { enabled: true, maxRetries: 3, baseDelayMs: 2000, maxAgentDelayMs: 60000 },
      compaction: {
        enabled: true,
        reserveTokens: 16384,
        keepRecentTokens: 20000,
        backgroundTokens: 32768,
      },
      progress: { partialIntervalMs: 100, outputIntervalMs: 100, persistWithinMs: 1000 },
      toolExecution: "parallel",
      steeringMode: "one-at-a-time",
      followUpMode: "one-at-a-time",
      contextRetentionMs: 600_000,
      documentRetentionMs: resolved.documentRetentionMs,
      afterFailedRun: "keep",
      restartLimit: 5,
    });
    assert.isNumber(resolved.documentRetentionMs);
    assert.strictEqual(Settings.resolve({ restartLimit: 0 }).restartLimit, 5);
    assert.strictEqual(
      Settings.resolve({ restartLimit: Number.POSITIVE_INFINITY }).restartLimit,
      Number.POSITIVE_INFINITY,
    );
    const partial = Settings.resolve({
      retry: { enabled: false },
      compaction: { backgroundTokens: 0 },
    });
    assert.strictEqual(partial.retry.enabled, false);
    assert.strictEqual(partial.retry.maxRetries, 3);
    assert.strictEqual(partial.retry.baseDelayMs, 2000);
    assert.strictEqual(partial.compaction.enabled, true);
    assert.strictEqual(partial.compaction.backgroundTokens, 0);
    assert.deepStrictEqual(Settings.resolve({ progress: { outputIntervalMs: 500 } }).progress, {
      partialIntervalMs: 100,
      outputIntervalMs: 500,
      persistWithinMs: 1000,
    });
    assert.strictEqual(
      Settings.resolve({ progress: { persistWithinMs: -1 } }).progress.persistWithinMs,
      1000,
    );
    assert.strictEqual(
      Settings.resolve({ progress: { persistWithinMs: 0 } }).progress.persistWithinMs,
      0,
    );
  });

  it.effect("commits partials no more often than progress.partialIntervalMs", () =>
    Effect.gen(function* () {
      const publishedPartial = (settings: ChatSetup["settings"]) =>
        run(backend)(
          Effect.gen(function* () {
            const setup = withStream({ ...chatSetup(), settings }, () =>
              shortStream(Stream.make(terminalEvent(fauxAssistantMessage("final")))),
            );
            const storage = yield* controlledStorage;
            const { harness: h, root } = yield* openChat(storage, setup);
            const values = yield* livePublications(h);
            yield* h.harness.resume;
            const submission = yield* root.submit({ type: "input", content: "hi" });
            assert.strictEqual((yield* settle(submission)).status, "done");
            yield* h.close;
            return values.some((value) => messageText(value) === "partial");
          }),
        );
      assert.isTrue(yield* publishedPartial({}));
      assert.isFalse(yield* publishedPartial({ progress: { partialIntervalMs: 5000 } }));
    }),
  );

  it.effect("renders sections that read conversation documents through input.read", () =>
    run(backend)(
      Effect.gen(function* () {
        const Agent = Doc.make({
          checkpointWhen: () => false,
          kind: "test.agent",
          version: 1,
          scope: "conversation",
          history: "latest",
          fork: "current",
          schema: Schema.Struct({ cwd: Schema.String, kind: Schema.String }),
          initial: () => ({ cwd: "/", kind: "main" }),
        });
        const setup = chatSetup();
        setup.faux.setResponses([fauxAssistantMessage("a"), fauxAssistantMessage("b")]);
        const storage = yield* controlledStorage;
        const { harness: h, root } = yield* openChat(storage, setup, {
          extensions: [
            addSection("cwd", (input) =>
              input
                .snapshot(Agent, input.conversationId)
                .pipe(Effect.map((agent) => Option.getOrUndefined(agent)?.cwd)),
            ),
            addSection("agents", (input) =>
              input
                .snapshot(Agent, input.conversationId)
                .pipe(
                  Effect.map((agent) =>
                    Option.getOrUndefined(agent)?.kind === "sub" ? undefined : "Read AGENTS.md",
                  ),
                ),
            ),
          ],
        });
        yield* root.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            (yield* tx.doc(Agent, root.id)).cwd = "/repo";
          }),
        );
        const sub = yield* h.harness.createConversation({
          ownership: { kind: "ownerless" },
          agent: { model: FAUX_MODEL },
          init: (id) =>
            Effect.gen(function* () {
              const tx = yield* Tx;
              const agent = yield* tx.doc(Agent, id);
              agent.kind = "sub";
              agent.cwd = "/sub";
            }),
        });
        yield* h.harness.resume;
        yield* settle(yield* root.submit({ type: "input", content: "one" }));
        yield* settle(yield* sub.submit({ type: "input", content: "two" }));
        const rootSystem = yield* firstSystem(root);
        assert.deepStrictEqual(rootSystem?.role === "system" ? rootSystem.sections : undefined, {
          cwd: "<cwd>\n/repo\n</cwd>",
          agents: "<agents>\nRead AGENTS.md\n</agents>",
        });
        const subSystem = yield* firstSystem(sub);
        assert.deepStrictEqual(subSystem, {
          role: "system",
          content: "",
          sections: { cwd: "<cwd>\n/sub\n</cwd>" },
          timestamp: subSystem?.timestamp ?? Number.NaN,
        });
        assert.isNumber(subSystem?.timestamp);
        yield* h.close;
      }),
    ),
  );

  it.effect(
    "commits no partial for a response that turns deferred after an empty start event",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const handle = {
            provider: "faux",
            modelId: "faux-1",
            api: "faux",
            id: "handle-1",
            pollAfterMs: 60_000,
          };
          // An empty start event, then, longer than the partial throttle, the deferred final message: an empty
          // partial would be committed meanwhile.
          const setup = withModels(chatSetup(), () => ({
            stream: () =>
              Stream.make<ReadonlyArray<AssistantMessageEvent>>({
                type: "start",
                partial: fauxAssistantMessage([], { stopReason: "pending" }),
              }).pipe(
                Stream.concat(Stream.fromEffect(Effect.sleep("300 millis")).pipe(Stream.drain)),
                Stream.concat(
                  Stream.make(
                    terminalEvent(
                      fauxAssistantMessage([], { stopReason: "deferred", deferred: handle }),
                    ),
                  ),
                ),
              ),
          }));
          const storage = yield* controlledStorage;
          const { harness: h, root } = yield* openChat(storage, setup);
          const values = yield* livePublications(h);
          yield* h.harness.resume;
          const submission = yield* root.submit({ type: "input", content: "hi" });
          yield* waitFor(
            live(h, root).pipe(Effect.map((state) => state?.generation?.deferred !== undefined)),
          );
          assert.isFalse(values.some((value) => value.generation?.message !== undefined));
          const taskId = (yield* live(h, root))?.run?.taskId ?? assert.fail("no run");
          yield* h.harness.abortTask(taskId);
          const settled = yield* settle(submission);
          assert.strictEqual(
            settled.status === "unanswered" ? settled.reason : undefined,
            "aborted",
          );
          assert.deepStrictEqual(
            (yield* allEntries(root)).map((entry) => entry.kind),
            ["core.user"],
          );
          yield* h.close;
        }),
      ),
  );

  it.effect("faults a run task, settling its inputs and converting the committed partial", () =>
    run(backend)(
      Effect.gen(function* () {
        // Adapted (header): a defect of the model stream once its partial was committed.
        const setup = withStream(chatSetup(), () =>
          shortStream(Stream.die(new Error("Invalid final message"))),
        );
        const storage = yield* controlledStorage;
        const { harness: h, root } = yield* openChat(storage, setup);
        const values = yield* livePublications(h);
        yield* h.harness.resume;
        const submission = yield* root.submit({ type: "input", content: "hi" });
        const settled = yield* settle(submission);
        if (settled.status !== "unanswered") return assert.fail(`Unexpected ${settled.status}`);
        assert.strictEqual(settled.reason, "faulted");
        assert.include(String(settled.detail), "Invalid final message");
        assert.isTrue(values.some((value) => messageText(value) === "partial"));
        const entries = yield* allEntries(root);
        assert.deepStrictEqual(
          entries.map((entry) => entry.kind),
          ["core.user", "core.assistant"],
        );
        const converted = entries[1]?.model?.[0];
        assert.strictEqual(converted?.role, "assistant");
        assert.strictEqual(
          converted?.role === "assistant" ? converted.stopReason : undefined,
          "aborted",
        );
        assert.strictEqual(textOf(converted), "partial");
        // The detail is the stream's defect (see the header).
        const failed = yield* live(h, root);
        const detail = failed?.runFailure?.detail ?? assert.fail("no detail");
        assert.include(detail, "Invalid final message");
        assert.deepStrictEqual(failed, {
          runFailure: {
            taskId: anyNumber(failed?.runFailure?.taskId),
            inputs: [submission.id],
            reason: "faulted",
            detail,
            first: anyNumber(failed?.runFailure?.first),
          },
        });
        const tasks = yield* tasksOf(h, root);
        const state = tasks[0]?.state;
        assert.strictEqual(state?.status, "terminal");
        assert.strictEqual(
          state?.status === "terminal" ? state.outcome.status : undefined,
          "faulted",
        );
        yield* h.close;
      }),
    ),
  );

  it.effect("orphans a blocked run task with full run cleanup", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        const storage = yield* controlledStorage;
        const { harness: h, root } = yield* openChat(storage, setup);
        // A run whose task was stored by a newer generation definition this process cannot run.
        const newer: Task.Any = {
          definition: {
            ...GenerationTask.definition,
            version: GenerationTask.definition.version + 1,
          },
        };
        const { taskId, submissionId } = yield* h.kernel.commitWith(() =>
          Effect.gen(function* () {
            const tx = yield* Tx;
            const entry = yield* tx.appendEntry(UserEntry, root.id, {
              model: [{ role: "user", content: "hi", timestamp: 1 }],
            });
            const submission = yield* tx.createSubmission({
              conversationId: root.id,
              type: "input",
              status: "placed",
              entry: entry.id,
            });
            const created = yield* tx.createTask(
              // SAFETY: the built-in generation's definition at another version, spread from the built-in.
              newer as typeof GenerationTask,
              {},
              { ownership: { kind: "conversation" }, conversationId: root.id },
            );
            (yield* tx.doc(LiveDoc, root.id)).run = {
              taskId: created,
              inputs: [submission.id],
              turns: 0,
            };
            return { taskId: created, submissionId: submission.id };
          }),
        );
        yield* h.harness.resume;
        const busy = yield* failureMessage(
          root.submit({ type: "input", content: "busy", whenBusy: "reject" }),
        );
        assert.include(busy, "is busy");
        assert.deepStrictEqual(yield* h.harness.abortTask(taskId), { _tag: "Marked" });
        assert.deepStrictEqual((yield* settleTask(h, taskId)).state.outcome, {
          status: "orphaned",
          reason: "task_too_old",
        });
        const submission = Option.getOrThrow(yield* h.harness.submission(submissionId));
        const status = yield* submission.status;
        assert.strictEqual(status.status, "unanswered");
        assert.strictEqual(
          status.status === "unanswered" ? status.reason : undefined,
          "task_too_old",
        );
        assert.deepStrictEqual(yield* live(h, root), {
          runFailure: {
            taskId,
            inputs: [submissionId],
            reason: "orphaned",
            detail: "task_too_old",
          },
        });
        yield* h.close;
      }),
    ),
  );

  it.effect("an extension declaring core.generation fails the layer build", () =>
    run(backend)(
      Effect.gen(function* () {
        // Adapted from "rejects a registry without the built-in tasks" (header).
        const Shadow = Task.make({
          name: "core.generation",
          version: 1,
          input: Schema.Null,
          checkpoint: Schema.Struct({ phase: Schema.Literal("run") }),
          result: Schema.Null,
          initial: () => ({ phase: "run" }),
          phases: { run: () => Effect.void },
          abort: () => Effect.void,
        });
        const storage = yield* controlledStorage;
        const message = yield* failureMessage(
          Effect.scoped(
            openChat(storage, chatSetup(), {
              extensions: [Extension.make({ name: "x", tasks: [Shadow] })],
            }),
          ),
        );
        assert.include(message, "Task core.generation of extension x is already installed");
      }),
    ),
  );
});

// Restart failure, on every backend: a crash (a host that dies, is evicted, or is reset) charges a generation's
// interrupted request as a failed attempt under the retry policy, and every other task ends visibly once cut off
// `settings.restartLimit` times in a row.
//
// Test setup:
// - A crash is `Killable.kill` (`HarnessSupport.killable`): every later storage call of the killed Harness
//   dies, so it writes nothing more, not even the `pending` records a close writes. The killed Harness is then
//   closed, which settles at once on its failed storage. A reopened SQLite file is a second Harness over the
//   same storage of the backend.
// - Time is the `TestClock`; `ChatSupport.waitFor` advances it in 5 ms steps.
// - Reports' causes fail with `Report.TaskInterrupted` and `Report.TaskFaulted`.
// - "resolves the settings no more often than before": the bound is the count for one round, measured on `main`
//   before the limit landed.
// - `deferredSetup` is a `Models` layer over the fake's, whose first fetch never returns until the killed
//   Harness's close interrupts it. A clock moved backwards is `TestClock.setTime` to an earlier time.
import { assert, it } from "@effect/vitest";
import * as Cause from "effect/Cause";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as TestClock from "effect/testing/TestClock";
import * as AgentTool from "../src/AgentTool.ts";
import { AgentStorage } from "../src/AgentStorage.ts";
import { ToolContext } from "../src/AgentTool.ts";
import * as Extension from "../src/Extension.ts";
import type { Conversation } from "../src/Harness.ts";
import { ROOT_CONVERSATION_ID, type SubmissionId, type TaskId } from "../src/Ids.ts";
import { LiveDoc, type LiveState } from "../src/Live.ts";
import type { Message, ToolResultMessage } from "../src/Message.ts";
import { Models } from "../src/Models.ts";
import { TaskFaulted, TaskInterrupted } from "../src/Report.ts";
import * as Section from "../src/Section.ts";
import {
  type FauxResponseStep,
  fauxAssistantMessage,
  fauxText,
  fauxToolCall,
} from "../src/testing/FauxModel.ts";
import { Tx } from "../src/Tx.ts";
import {
  allEntries,
  type ChatSetup,
  chatSetup,
  openChat,
  stored,
  textOf,
  unanswered,
  waitFor,
} from "./ChatSupport.ts";
import { anyNumber, eventually, killable, type TestHarness } from "./HarnessSupport.ts";
import { describeEachBackend, run } from "./SessionSupport.ts";
import { commitWrites } from "../src/testing/CommitWrites.ts";

type Inner = AgentStorage["Service"];

/** A chat over `inner`; `kill()` simulates a crash. */
const open = (inner: Inner, setup: ChatSetup, extensions: ReadonlyArray<Extension.Any> = []) =>
  Effect.gen(function* () {
    const crashable = yield* killable(inner);
    const opened = yield* openChat(crashable.storage, setup, { extensions });
    return { ...opened, kill: crashable.kill.pipe(Effect.andThen(opened.harness.close)) };
  });

const live = (h: TestHarness) =>
  h.session
    .snapshot(LiveDoc, ROOT_CONVERSATION_ID)
    .pipe(Effect.map((value): LiveState => Option.getOrElse(value, () => ({}))));

/** Every `core.live` value from now on, as the Session publishes it. */
const liveValues = (h: TestHarness) =>
  Effect.gen(function* () {
    const values: Array<LiveState> = [];
    yield* h.kernel.observeCommits({
      advance: (publication) => {
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
      },
      end: () => {},
    });
    return values;
  });

const taskOf = (h: TestHarness, id: TaskId) =>
  h.harness.getTask(id).pipe(Effect.map(Option.getOrUndefined));

/** Waits for a submission to settle, while the `TestClock` advances. */
const settled = (h: TestHarness, id: SubmissionId) =>
  Effect.gen(function* () {
    const submission = Option.getOrThrow(yield* h.harness.submission(id));
    const fiber = yield* Effect.forkChild(submission.await, { startImmediately: true });
    yield* waitFor(Effect.sync(() => fiber.pollUnsafe() !== undefined));
    return yield* Fiber.join(fiber);
  });

/** The reports of one kind among a setup's reported causes. */
const infos = <A>(setup: ChatSetup, is: (error: unknown) => error is A): Array<A> =>
  setup.reports.flatMap((report) => {
    const error = Option.getOrUndefined(Cause.findErrorOption(report.cause));
    return is(error) ? [error] : [];
  });

const isInterrupted = Schema.is(TaskInterrupted);
const isFaulted = Schema.is(TaskFaulted);

/** A setup whose model streams a long answer slowly; partials are stored as soon as they are committed. */
const streaming = (): ChatSetup => {
  const setup = chatSetup({ tokensPerSecond: 20, tokenSize: { min: 1, max: 1 } });
  setup.faux.setResponses([fauxAssistantMessage("z".repeat(400))]);
  setup.settings.progress = { persistWithinMs: 0 };
  return setup;
};

/** Submit an input to a fresh chat over `inner` and crash once its streamed partial is stored. */
const crashWhileStreaming = (inner: Inner, setup: ChatSetup) =>
  Effect.gen(function* () {
    const opened = yield* open(inner, setup);
    yield* opened.harness.harness.resume;
    const id = (yield* opened.root.submit({ type: "input", content: "hi" })).id;
    yield* waitFor(
      live(opened.harness).pipe(Effect.map((value) => value.generation?.message !== undefined)),
    );
    const taskId = (yield* live(opened.harness)).run!.taskId;
    yield* stored;
    yield* opened.kill;
    return { id, taskId };
  });

/** Reopen after a crash in the middle of a request, with another streaming answer, and crash again. */
const crashAgain = (inner: Inner, setup: ChatSetup, taskId: TaskId) =>
  Effect.gen(function* () {
    const opened = yield* open(inner, setup);
    yield* opened.harness.harness.resume;
    // The first attempt was charged; the second one streams.
    yield* waitFor(
      taskOf(opened.harness, taskId).pipe(
        Effect.map((record) => {
          const state = record?.state;
          if (state?.status !== "running") return false;
          const checkpoint = state.checkpoint;
          return (
            Schema.is(Schema.Struct({ phase: Schema.Literal("request"), attempt: Schema.Number }))(
              checkpoint,
            ) && checkpoint.attempt === 2
          );
        }),
      ),
    );
    yield* waitFor(
      live(opened.harness).pipe(Effect.map((value) => value.generation?.message !== undefined)),
    );
    yield* stored;
    yield* opened.kill;
  });

/**
 * A setup whose model answers deferred, and whose first fetch of each deferred response is in
 * flight until the Harness stops; later fetches answer. `fetching` completes when the first fetch began.
 */
const deferredSetup = (pollAfterMs = 10) => {
  const base = chatSetup({ deferred: { pollAfterMs } });
  base.settings.stream = { deferred: true };
  base.faux.setResponses([fauxAssistantMessage("deferred answer")]);
  const fetching = Deferred.makeUnsafe<void>();
  let fetches = 0;
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
              Effect.suspend(() => {
                fetches++;
                if (fetches > 1 && models.fetchDeferred !== undefined) {
                  return models.fetchDeferred(model, handle);
                }
                return Deferred.succeed(fetching, undefined).pipe(Effect.andThen(Effect.never));
              }),
          }),
        ),
      ).pipe(Layer.provide(base.faux.layer)),
    },
  };
  return { setup, fetching, fetches: () => fetches };
};

/** Submits to a fresh chat, lets its deferred response be fetched, and crashes during the fetch. */
const crashWhilePolling = (inner: Inner, setup: ChatSetup, fetching: Deferred.Deferred<void>) =>
  Effect.gen(function* () {
    const opened = yield* open(inner, setup);
    yield* opened.harness.harness.resume;
    const id = (yield* opened.root.submit({ type: "input", content: "hi" })).id;
    yield* waitFor(Deferred.isDone(fetching));
    const taskId = (yield* live(opened.harness)).run!.taskId;
    const state = (yield* taskOf(opened.harness, taskId))?.state;
    assert.strictEqual(state?.status, "running");
    assert.deepInclude(state?.status === "running" ? state.checkpoint : undefined, {
      phase: "poll",
    });
    yield* opened.kill;
    return { id, taskId };
  });

const kinds = (conversation: Conversation) =>
  allEntries(conversation).pipe(Effect.map((entries) => entries.map((entry) => entry.kind)));

const stopReason = (message: Message | undefined) =>
  message?.role === "assistant" ? message.stopReason : undefined;

describeEachBackend("a generation interrupted while the model answers (#11139)", (backend) => {
  it.effect(
    "charges the attempt: keeps the partial, waits for the retry backoff, and answers on the next attempt",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const inner = yield* AgentStorage;
          const first = streaming();
          first.settings.retry = { enabled: true, maxRetries: 2, baseDelayMs: 1 };
          const { id, taskId } = yield* crashWhileStreaming(inner, first);

          const setup = chatSetup();
          setup.settings.retry = { enabled: true, maxRetries: 2, baseDelayMs: 1 };
          const sent: Array<Array<string>> = [];
          setup.faux.setResponses([
            (request) =>
              Effect.sync(() => {
                sent.push(request.messages.map((message) => message.role));
                return fauxAssistantMessage("answer");
              }),
          ]);
          const opened = yield* open(inner, setup);
          const values = yield* liveValues(opened.harness);
          const partial = textOf((yield* live(opened.harness)).generation?.message);
          assert.strictEqual((yield* taskOf(opened.harness, taskId))?.restarts, 1);
          yield* opened.harness.harness.resume;
          assert.strictEqual((yield* settled(opened.harness, id)).status, "done");
          // The backoff showed in `core.live` with why.
          const retry = values.find((value) => value.generation?.retry !== undefined)?.generation;
          assert.deepStrictEqual(retry, {
            attempt: 1,
            retry: {
              at: anyNumber(retry?.retry?.at),
              error: "The host stopped while the model was answering, on attempt 1",
            },
          });
          assert.isTrue(values.some((value) => value.generation?.attempt === 2));
          const entries = yield* allEntries(opened.root);
          assert.deepStrictEqual(
            entries.map((entry) => entry.kind),
            ["core.user", "core.assistant", "core.assistant"],
          );
          const converted = entries[1]?.model?.[0];
          assert.strictEqual(stopReason(converted), "aborted");
          assert.strictEqual(textOf(converted), partial);
          // The aborted partial is excluded from the next request.
          assert.deepStrictEqual(sent, [["user"]]);
          assert.deepStrictEqual(yield* live(opened.harness), {});
          yield* opened.harness.close;
        }),
      ),
  );

  it.effect("sleeps the backoff of `settings.retry` before the next attempt", () =>
    run(backend)(
      Effect.gen(function* () {
        const inner = yield* AgentStorage;
        const first = streaming();
        first.settings.retry = { enabled: true, maxRetries: 2, baseDelayMs: 60_000 };
        const { taskId } = yield* crashWhileStreaming(inner, first);

        const setup = chatSetup();
        setup.settings.retry = { enabled: true, maxRetries: 2, baseDelayMs: 60_000 };
        const now = 1_000_000;
        yield* TestClock.setTime(now);
        const opened = yield* open(inner, setup);
        yield* opened.harness.harness.resume;
        yield* eventually(
          taskOf(opened.harness, taskId).pipe(
            Effect.map((record) => record?.state.status === "sleeping"),
          ),
        );
        const record = yield* taskOf(opened.harness, taskId);
        assert.deepStrictEqual(record?.state, {
          status: "sleeping",
          checkpoint: { phase: "retry", attempt: 1, until: now + 60_000 },
          until: now + 60_000,
        });
        assert.isUndefined(record?.restarts);
        assert.deepStrictEqual((yield* live(opened.harness)).generation, {
          attempt: 1,
          retry: {
            at: now + 60_000,
            error: "The host stopped while the model was answering, on attempt 1",
          },
        });
        yield* eventually(opened.harness.harness.wake.pipe(Effect.map((wake) => !wake.activeWork)));
        assert.deepStrictEqual(yield* opened.harness.harness.wake, {
          nextWakeAt: Option.some(now + 60_000),
          activeWork: false,
        });
        yield* opened.harness.close;
      }),
    ),
  );

  it.effect("ends the run interrupted once no retry is left, keeping every partial", () =>
    run(backend)(
      Effect.gen(function* () {
        const inner = yield* AgentStorage;
        const first = streaming();
        first.settings.retry = { enabled: true, maxRetries: 1, baseDelayMs: 1 };
        const { id, taskId } = yield* crashWhileStreaming(inner, first);
        // Attempt 2 is interrupted too.
        const second = streaming();
        second.settings.retry = { enabled: true, maxRetries: 1, baseDelayMs: 1 };
        yield* crashAgain(inner, second, taskId);

        const setup = chatSetup();
        setup.settings.retry = { enabled: true, maxRetries: 1, baseDelayMs: 1 };
        const opened = yield* open(inner, setup);
        yield* opened.harness.harness.resume;
        const text = "The host stopped while the model was answering, on attempt 2";
        assert.deepInclude(yield* settled(opened.harness, id), {
          status: "unanswered",
          reason: "interrupted",
          detail: text,
        });
        const value = yield* live(opened.harness);
        assert.deepStrictEqual(value, {
          runFailure: {
            taskId,
            inputs: [id],
            reason: "interrupted",
            detail: text,
            first: anyNumber(value.runFailure?.first),
          },
        });
        assert.deepStrictEqual((yield* opened.harness.harness.awaitTask(taskId)).state.outcome, {
          status: "failed",
          error: { message: text, detail: { reason: "interrupted" } },
        });
        const entries = yield* allEntries(opened.root);
        assert.deepStrictEqual(
          entries.map((entry) => entry.kind),
          ["core.user", "core.assistant", "core.assistant"],
        );
        assert.deepStrictEqual(
          entries.slice(1).map((entry) => stopReason(entry.model?.[0])),
          ["aborted", "aborted"],
        );
        // No request was sent after the second crash.
        assert.strictEqual(setup.faux.state.callCount, 0);
        yield* opened.harness.close;
      }),
    ),
  );

  it.effect("ends the run at once when retries are disabled", () =>
    run(backend)(
      Effect.gen(function* () {
        const inner = yield* AgentStorage;
        const { id } = yield* crashWhileStreaming(inner, streaming());
        const setup = chatSetup();
        setup.settings.retry = { enabled: false };
        const opened = yield* open(inner, setup);
        yield* opened.harness.harness.resume;
        assert.deepInclude(yield* settled(opened.harness, id), {
          status: "unanswered",
          reason: "interrupted",
        });
        yield* opened.harness.close;
      }),
    ),
  );

  it.effect("resends after a clean close: a close is not a crash", () =>
    run(backend)(
      Effect.gen(function* () {
        const inner = yield* AgentStorage;
        const first = streaming();
        first.settings.retry = { enabled: false };
        const opened = yield* open(inner, first);
        yield* opened.harness.harness.resume;
        const id = (yield* opened.root.submit({ type: "input", content: "hi" })).id;
        yield* waitFor(
          live(opened.harness).pipe(Effect.map((value) => value.generation?.message !== undefined)),
        );
        const taskId = (yield* live(opened.harness)).run!.taskId;
        yield* opened.harness.close;

        const setup = chatSetup();
        setup.settings.retry = { enabled: false };
        setup.faux.setResponses([fauxAssistantMessage("answer")]);
        const reopened = yield* open(inner, setup);
        assert.isUndefined((yield* taskOf(reopened.harness, taskId))?.restarts);
        yield* reopened.harness.harness.resume;
        assert.strictEqual((yield* settled(reopened.harness, id)).status, "done");
        yield* reopened.harness.close;
      }),
    ),
  );

  it.effect("does not charge a generation that sleeps in a retry or waits for its tools", () =>
    run(backend)(
      Effect.gen(function* () {
        const inner = yield* AgentStorage;
        const gate = yield* Deferred.make<void>();
        const started = yield* Deferred.make<void>();
        const hold = Extension.make({
          name: "tool:hold",
          tools: [
            AgentTool.make("hold", {
              description: "Waits",
              parameters: Schema.Struct({}),
              replay: "safe",
              execute: () =>
                Deferred.succeed(started, undefined).pipe(
                  Effect.andThen(Deferred.await(gate)),
                  Effect.as({}),
                ),
            }),
          ],
        });
        const setup = chatSetup();
        setup.settings.retry = { enabled: true, maxRetries: 1, baseDelayMs: 60_000 };
        setup.faux.setResponses([
          fauxAssistantMessage([], {
            stopReason: "error",
            errorMessage: "503 Service Unavailable",
          }),
        ]);
        let opened = yield* open(inner, setup, [hold]);
        yield* opened.harness.harness.resume;
        const id = (yield* opened.root.submit({ type: "input", content: "hi" })).id;
        const taskId = (yield* live(opened.harness)).run!.taskId;
        yield* eventually(
          taskOf(opened.harness, taskId).pipe(
            Effect.map((record) => record?.state.status === "sleeping"),
          ),
        );
        yield* opened.kill;

        // Asleep in its retry: not running, so not cut off.
        const later = chatSetup();
        later.settings.retry = { enabled: true, maxRetries: 1, baseDelayMs: 60_000 };
        yield* TestClock.adjust("120 seconds");
        later.faux.setResponses([
          fauxAssistantMessage([fauxToolCall("hold", {}, { id: "c1" })], { stopReason: "toolUse" }),
          fauxAssistantMessage([fauxText("done")]),
        ]);
        opened = yield* open(inner, later, [hold]);
        assert.isUndefined((yield* taskOf(opened.harness, taskId))?.restarts);
        yield* opened.harness.harness.resume;
        // Waiting for its tool round: not running either.
        yield* Deferred.await(started);
        const waiting = yield* taskOf(opened.harness, taskId);
        assert.strictEqual(waiting?.state.status, "waiting");
        assert.deepInclude(
          waiting?.state.status === "waiting" ? waiting.state.checkpoint : undefined,
          { phase: "tools" },
        );
        yield* opened.kill;

        opened = yield* open(inner, later, [hold]);
        assert.isUndefined((yield* taskOf(opened.harness, taskId))?.restarts);
        yield* Deferred.succeed(gate, undefined);
        yield* opened.harness.harness.resume;
        assert.strictEqual((yield* settled(opened.harness, id)).status, "done");
        yield* opened.harness.close;
      }),
    ),
  );

  it.effect("ends a run whose generation is cut off restartLimit times outside a request", () =>
    run(backend)(
      Effect.gen(function* () {
        const inner = yield* AgentStorage;
        const setup = chatSetup();
        setup.settings.restartLimit = 1;
        const reached = yield* Deferred.make<void>();
        // Preparation never finishes, as one that exhausts the host's memory would not.
        const preamble = Extension.make({
          name: "section:preamble",
          sections: [
            Section.make("preamble", () =>
              Deferred.succeed(reached, undefined).pipe(Effect.andThen(Effect.never)),
            ),
          ],
        });
        let opened = yield* open(inner, setup, [preamble]);
        yield* opened.harness.harness.resume;
        const id = (yield* opened.root.submit({ type: "input", content: "hi" })).id;
        yield* Deferred.await(reached);
        const taskId = (yield* live(opened.harness)).run!.taskId;
        yield* opened.kill;

        opened = yield* open(inner, setup, [preamble]);
        yield* opened.harness.harness.resume;
        const text = "The generation was interrupted once in its prepare phase";
        assert.deepInclude(yield* settled(opened.harness, id), {
          status: "unanswered",
          reason: "restart_limit",
          detail: text,
        });
        const value = yield* live(opened.harness);
        assert.deepStrictEqual(value, {
          runFailure: {
            taskId,
            inputs: [id],
            reason: "restart_limit",
            detail: text,
            first: anyNumber(value.runFailure?.first),
          },
        });
        yield* eventually(Effect.sync(() => infos(setup, isInterrupted).length > 0));
        assert.deepInclude(infos(setup, isInterrupted)[0], {
          reason: "restart_limit",
          restarts: 1,
        });
        yield* opened.harness.close;
      }),
    ),
  );
});

describeEachBackend("other tasks cut off restartLimit times in a row (#11139)", (backend) => {
  it.effect("ends a tool call with an error result instead of running it again", () =>
    run(backend)(
      Effect.gen(function* () {
        const inner = yield* AgentStorage;
        const setup = chatSetup();
        setup.settings.restartLimit = 2;
        setup.settings.progress = { outputIntervalMs: 0, persistWithinMs: 0 };
        let runs = 0;
        let running = Deferred.makeUnsafe<void>();
        const heavy = Extension.make({
          name: "tool:heavy",
          tools: [
            AgentTool.make("heavy", {
              description: "Never finishes",
              parameters: Schema.Struct({}),
              replay: "safe",
              execute: () =>
                Effect.gen(function* () {
                  const api = yield* ToolContext;
                  runs++;
                  yield* api.output("partial\n");
                  yield* Deferred.succeed(running, undefined);
                  return yield* Effect.never;
                }).pipe(Effect.orDie),
            }),
          ],
        });
        const answers: Array<FauxResponseStep> = [
          () =>
            Effect.succeed(
              fauxAssistantMessage([fauxToolCall("heavy", {}, { id: "c1" })], {
                stopReason: "toolUse",
              }),
            ),
          (request) =>
            Effect.sync(() => {
              const result = request.messages.at(-1);
              return fauxAssistantMessage(`saw: ${textOf(result) ?? ""}`);
            }),
        ];
        setup.faux.setResponses(answers);
        let opened = yield* open(inner, setup, [heavy]);
        yield* opened.harness.harness.resume;
        const id = (yield* opened.root.submit({ type: "input", content: "go" })).id;
        for (let crash = 1; crash <= 2; crash++) {
          yield* Deferred.await(running);
          running = Deferred.makeUnsafe<void>();
          // The output is shown once its throttled commit lands, and stored right after it.
          yield* waitFor(
            live(opened.harness).pipe(
              Effect.map((value) => value.tools?.[0]?.output?.includes("partial") === true),
            ),
          );
          yield* stored;
          yield* opened.kill;
          opened = yield* open(inner, setup, [heavy]);
          yield* opened.harness.harness.resume;
        }
        assert.strictEqual((yield* settled(opened.harness, id)).status, "done");
        assert.strictEqual(runs, 2);
        const result = (yield* allEntries(opened.root)).find(
          (entry) => entry.kind === "core.tool-result",
        );
        // SAFETY: a tool result entry's model message.
        const message = result?.model?.[0] as ToolResultMessage | undefined;
        assert.isTrue(message?.isError);
        assert.deepInclude(result?.data, {
          diagnostics: [
            {
              severity: "error",
              code: "restart_limit",
              message: "Tool heavy was interrupted 2 times in a row; it is not run again",
            },
          ],
        });
        // The slot's partial output is kept, as for an interrupted call.
        assert.include(textOf(message), "partial");
        const toolTask = (yield* opened.harness.harness.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            return yield* tx.scanTasks({ kind: "core.tool" }, 10);
          }),
        )).items[0]!;
        assert.strictEqual(toolTask.state.status, "terminal");
        assert.deepInclude(
          toolTask.state.status === "terminal" && toolTask.state.outcome.status === "failed"
            ? toolTask.state.outcome.error
            : undefined,
          { detail: { reason: "restart_limit" } },
        );
        yield* eventually(Effect.sync(() => infos(setup, isInterrupted).length > 0));
        const report = infos(setup, isInterrupted)[0];
        assert.deepStrictEqual(
          {
            taskId: report?.taskId,
            kind: report?.kind,
            conversationId: report?.conversationId,
            reason: report?.reason,
            restarts: report?.restarts,
          },
          {
            taskId: toolTask.id,
            kind: "core.tool",
            conversationId: ROOT_CONVERSATION_ID,
            reason: "restart_limit",
            restarts: 2,
          },
        );
        yield* opened.harness.close;
      }),
    ),
  );

  it.effect("fails a compaction into compactionFailure", () =>
    run(backend)(
      Effect.gen(function* () {
        const inner = yield* AgentStorage;
        const setup = chatSetup();
        setup.settings.restartLimit = 1;
        // Little recent context is kept, so the first turn is summarized.
        setup.settings.compaction = { enabled: false, keepRecentTokens: 10 };
        setup.faux.setResponses([fauxAssistantMessage("a1"), fauxAssistantMessage("a2")]);
        let opened = yield* open(inner, setup);
        yield* opened.harness.harness.resume;
        yield* settled(
          opened.harness,
          (yield* opened.root.submit({ type: "input", content: "u1 ".repeat(200) })).id,
        );
        yield* settled(
          opened.harness,
          (yield* opened.root.submit({ type: "input", content: "u2 ".repeat(200) })).id,
        );
        // The summary request never returns.
        const hanging = unanswered();
        setup.faux.setResponses([hanging.step]);
        const compaction = yield* opened.root.compact();
        yield* waitFor(Deferred.isDone(hanging.reached));
        yield* opened.kill;

        opened = yield* open(inner, setup);
        yield* opened.harness.harness.resume;
        const error = "Compaction was interrupted once";
        const fiber = yield* Effect.forkChild(opened.harness.harness.awaitTask(compaction), {
          startImmediately: true,
        });
        yield* waitFor(Effect.sync(() => fiber.pollUnsafe() !== undefined));
        assert.deepStrictEqual((yield* Fiber.join(fiber)).state.outcome, {
          status: "failed",
          error: { message: error, detail: { reason: "restart_limit" } },
        });
        assert.deepStrictEqual((yield* live(opened.harness)).compactionFailure, {
          taskId: compaction,
          trigger: "manual",
          error,
        });
        assert.isUndefined((yield* live(opened.harness)).compactions);
        yield* eventually(Effect.sync(() => infos(setup, isInterrupted).length > 0));
        const report = infos(setup, isInterrupted)[0];
        assert.deepStrictEqual(
          {
            taskId: report?.taskId,
            kind: report?.kind,
            conversationId: report?.conversationId,
            reason: report?.reason,
            restarts: report?.restarts,
          },
          {
            taskId: compaction,
            kind: "core.compaction",
            conversationId: ROOT_CONVERSATION_ID,
            reason: "restart_limit",
            restarts: 1,
          },
        );
        yield* opened.harness.close;
      }),
    ),
  );
});

describeEachBackend("crash edges (#11139)", (backend) => {
  it.effect(
    "charges a crash during a poll's fetch, then polls the same deferred response again",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const inner = yield* AgentStorage;
          const { setup, fetching, fetches } = deferredSetup();
          setup.settings.retry = { enabled: true, maxRetries: 2, baseDelayMs: 1 };
          const { id, taskId } = yield* crashWhilePolling(inner, setup, fetching);

          const opened = yield* open(inner, setup);
          const values = yield* liveValues(opened.harness);
          assert.strictEqual((yield* taskOf(opened.harness, taskId))?.restarts, 1);
          yield* opened.harness.harness.resume;
          const answer = yield* settled(opened.harness, id);
          assert.strictEqual(answer.status, "done");
          const charged = values.find((value) => value.generation?.retry !== undefined)?.generation;
          assert.deepStrictEqual(charged, {
            attempt: 1,
            retry: {
              at: anyNumber(charged?.retry?.at),
              error: "The host stopped while the deferred answer was fetched, on attempt 1",
            },
          });
          assert.isTrue(
            values.some(
              (value) => value.generation?.attempt === 2 && value.generation.deferred !== undefined,
            ),
          );
          // One request, polled twice: the provider's job is neither dropped nor submitted again.
          assert.strictEqual(setup.faux.state.callCount, 1);
          assert.strictEqual(fetches(), 2);
          assert.deepStrictEqual(setup.faux.state.cancelledDeferred, []);
          const entries = yield* allEntries(opened.root);
          assert.strictEqual(textOf(entries.at(-1)?.model?.[0]), "deferred answer");
          yield* opened.harness.close;
        }),
      ),
  );

  it.effect("cancels the deferred response when a crash during its poll ends the run", () =>
    run(backend)(
      Effect.gen(function* () {
        const inner = yield* AgentStorage;
        const { setup, fetching } = deferredSetup();
        setup.settings.retry = { enabled: false };
        const { id, taskId } = yield* crashWhilePolling(inner, setup, fetching);
        const opened = yield* open(inner, setup);
        yield* opened.harness.harness.resume;
        const text = "The host stopped while the deferred answer was fetched, on attempt 1";
        assert.deepInclude(yield* settled(opened.harness, id), {
          status: "unanswered",
          reason: "interrupted",
          detail: text,
        });
        const value = yield* live(opened.harness);
        assert.deepStrictEqual(value, {
          runFailure: {
            taskId,
            inputs: [id],
            reason: "interrupted",
            detail: text,
            first: anyNumber(value.runFailure?.first),
          },
        });
        assert.strictEqual(setup.faux.state.cancelledDeferred.length, 1);
        yield* opened.harness.close;
      }),
    ),
  );

  it.effect("charges an interrupted poll even when the clock moved backwards", () =>
    run(backend)(
      Effect.gen(function* () {
        const inner = yield* AgentStorage;
        const { setup, fetching } = deferredSetup(10);
        yield* TestClock.setTime(1000);
        setup.settings.retry = { enabled: false };
        const first = yield* open(inner, setup);
        yield* first.harness.harness.resume;
        const id = (yield* first.root.submit({ type: "input", content: "hi" })).id;
        yield* waitFor(
          live(first.harness).pipe(Effect.map((value) => value.generation?.deferred !== undefined)),
        );
        yield* TestClock.setTime(1010);
        yield* first.harness.harness.resume;
        yield* waitFor(Deferred.isDone(fetching));
        yield* first.kill;
        // A replacement host whose clock is behind: the poll looks not due yet, but its fetch was cut off.
        yield* TestClock.setTime(1000);
        const opened = yield* open(inner, setup);
        yield* opened.harness.harness.resume;
        assert.deepInclude(yield* settled(opened.harness, id), {
          status: "unanswered",
          reason: "interrupted",
        });
        yield* opened.harness.close;
      }),
    ),
  );

  it.effect(
    "charges a request once when a crash is followed by a clean close before the charge",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const inner = yield* AgentStorage;
          const first = streaming();
          first.settings.retry = { enabled: true, maxRetries: 3, baseDelayMs: 1 };
          const { id, taskId } = yield* crashWhileStreaming(inner, first);
          const setup = chatSetup();
          setup.settings.retry = { enabled: true, maxRetries: 3, baseDelayMs: 1 };
          setup.faux.setResponses([fauxAssistantMessage("answer")]);
          let opened = yield* open(inner, setup);
          yield* opened.harness.close;
          opened = yield* open(inner, setup);
          assert.strictEqual((yield* taskOf(opened.harness, taskId))?.restarts, 1);
          const values = yield* liveValues(opened.harness);
          yield* opened.harness.harness.resume;
          assert.strictEqual((yield* settled(opened.harness, id)).status, "done");
          const charged = values.filter((value) => value.generation?.retry !== undefined);
          assert.deepStrictEqual(
            charged.map((value) => value.generation?.attempt),
            [1],
          );
          assert.deepStrictEqual(yield* kinds(opened.root), [
            "core.user",
            "core.assistant",
            "core.assistant",
          ]);
          yield* opened.harness.close;
        }),
      ),
  );

  it.effect(
    "ends a run restart_limit when charging the interrupted request keeps being cut off",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const inner = yield* AgentStorage;
          const first = streaming();
          first.settings.restartLimit = 1;
          const { id, taskId } = yield* crashWhileStreaming(inner, first);
          // At the limit: the partial is still kept.
          const setup = chatSetup();
          setup.settings.restartLimit = 1;
          const opened = yield* open(inner, setup);
          const partial = textOf((yield* live(opened.harness)).generation?.message);
          yield* opened.harness.harness.resume;
          const text = "The generation was interrupted once in its request phase";
          assert.deepInclude(yield* settled(opened.harness, id), {
            status: "unanswered",
            reason: "restart_limit",
            detail: text,
          });
          const value = yield* live(opened.harness);
          assert.deepStrictEqual(value, {
            runFailure: {
              taskId,
              inputs: [id],
              reason: "restart_limit",
              detail: text,
              first: anyNumber(value.runFailure?.first),
            },
          });
          const entries = yield* allEntries(opened.root);
          assert.deepStrictEqual(
            entries.map((entry) => entry.kind),
            ["core.user", "core.assistant"],
          );
          assert.strictEqual(textOf(entries[1]?.model?.[0]), partial);
          assert.strictEqual(setup.faux.state.callCount, 0);
          yield* opened.harness.close;
        }),
      ),
  );

  it.effect("drops the partial past the limit, where converting it may be what keeps failing", () =>
    run(backend)(
      Effect.gen(function* () {
        const inner = yield* AgentStorage;
        const { id, taskId } = yield* crashWhileStreaming(inner, streaming());
        // As if the charge had been cut off twice before: the open after this crash counts 3, past the limit of 2.
        const record = Option.getOrThrow(yield* inner.task(taskId));
        yield* commitWrites(inner, [{ type: "task", value: { ...record, restarts: 2 } }]);
        const setup = chatSetup();
        setup.settings.restartLimit = 2;
        const opened = yield* open(inner, setup);
        yield* opened.harness.harness.resume;
        assert.deepInclude(yield* settled(opened.harness, id), {
          status: "unanswered",
          reason: "restart_limit",
          detail: "The generation was interrupted 3 times in a row in its request phase",
        });
        assert.deepStrictEqual(yield* kinds(opened.root), ["core.user"]);
        assert.isUndefined((yield* live(opened.harness)).generation);
        yield* opened.harness.close;
      }),
    ),
  );

  it.effect(
    "ends a run someone aborted as aborted when its abort handler is cut off restartLimit times",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const inner = yield* AgentStorage;
          const base = chatSetup({ deferred: { pollAfterMs: 3_600_000 } });
          base.settings.restartLimit = 2;
          base.settings.stream = { deferred: true };
          base.faux.setResponses([fauxAssistantMessage("never")]);
          let cancels = 0;
          // The abort handler of a polling generation cancels its response: make that hang.
          const setup: ChatSetup = {
            ...base,
            faux: {
              ...base.faux,
              layer: Layer.effect(
                Models,
                Effect.map(Effect.service(Models), (models) =>
                  Models.of({
                    ...models,
                    cancelDeferred: () =>
                      Effect.suspend(() => {
                        cancels++;
                        return Effect.never;
                      }),
                  }),
                ),
              ).pipe(Layer.provide(base.faux.layer)),
            },
          };
          let opened = yield* open(inner, setup);
          yield* opened.harness.harness.resume;
          const id = (yield* opened.root.submit({ type: "input", content: "hi" })).id;
          const taskId = (yield* live(opened.harness)).run!.taskId;
          const first = opened;
          yield* waitFor(
            taskOf(first.harness, taskId).pipe(
              Effect.map((record) => record?.state.status === "sleeping"),
            ),
          );
          yield* Effect.forkDetach(Effect.ignore(opened.root.abort()), { startImmediately: true });
          for (let crash = 1; crash <= 2; crash++) {
            yield* waitFor(Effect.sync(() => cancels === crash));
            yield* opened.kill;
            opened = yield* open(inner, setup);
            assert.strictEqual((yield* taskOf(opened.harness, taskId))?.restarts, crash);
            yield* opened.harness.harness.resume;
          }
          const detail = `Abort handler of task ${taskId} (core.generation) was cut off 2 times`;
          assert.deepInclude(yield* settled(opened.harness, id), {
            status: "unanswered",
            reason: "aborted",
            detail,
          });
          const value = yield* live(opened.harness);
          assert.deepStrictEqual(value, {
            runFailure: {
              taskId,
              inputs: [id],
              reason: "aborted",
              detail,
              first: anyNumber(value.runFailure?.first),
            },
          });
          const outcome = (yield* opened.harness.harness.awaitTask(taskId)).state.outcome;
          assert.strictEqual(outcome.status, "faulted");
          assert.deepInclude(outcome.status === "faulted" ? outcome.error : undefined, {
            detail: { reason: "restart_limit" },
          });
          yield* eventually(Effect.sync(() => infos(setup, isFaulted).length > 0));
          assert.deepInclude(infos(setup, isFaulted)[0], { reason: "restart_limit" });
          yield* opened.harness.close;
        }),
      ),
  );
});

describeEachBackend("reports of work crashes ended (#11139)", (backend) => {
  it.effect(
    "reports task_interrupted for a run that ends interrupted and for a task that ends at the limit",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const inner = yield* AgentStorage;
          const first = streaming();
          first.settings.retry = { enabled: false };
          const { taskId } = yield* crashWhileStreaming(inner, first);
          const setup = chatSetup();
          setup.settings.retry = { enabled: false };
          const opened = yield* open(inner, setup);
          yield* opened.harness.harness.resume;
          yield* opened.harness.harness.awaitTask(taskId);
          yield* eventually(Effect.sync(() => setup.reports.length > 0));
          const reports = infos(setup, isInterrupted);
          assert.strictEqual(reports.length, setup.reports.length);
          assert.deepStrictEqual(
            reports.map((report) => ({
              taskId: report.taskId,
              kind: report.kind,
              conversationId: report.conversationId,
              reason: report.reason,
              restarts: report.restarts,
            })),
            [
              {
                taskId,
                kind: "core.generation",
                conversationId: ROOT_CONVERSATION_ID,
                reason: "interrupted",
                restarts: 1,
              },
            ],
          );
          assert.include(
            reports[0]?.message,
            "The host stopped while the model was answering, on attempt 1",
          );
          // A built-in task names no extension.
          assert.deepStrictEqual(
            setup.reports.map(({ cause: _cause, ...info }) => info),
            [
              {
                kind: "task_interrupted",
                task: { id: taskId, kind: "core.generation", conversationId: ROOT_CONVERSATION_ID },
                reason: "interrupted",
                restarts: 1,
              },
            ],
          );
          yield* opened.harness.close;
        }),
      ),
  );
});

describeEachBackend("the restart limit on the normal path (#11139)", (backend) => {
  it.effect(
    "resolves the settings no more often than before: only a task that was cut off reads the limit",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const inner = yield* AgentStorage;
          let resolutions = 0;
          // Every resolution of the host settings reads `extensions` once.
          const settings: ChatSetup["settings"] = {
            get extensions() {
              resolutions++;
              return undefined;
            },
          };
          const setup: ChatSetup = { ...chatSetup(), settings };
          const t = Extension.make({
            name: "tool:t",
            tools: [
              AgentTool.make("t", {
                description: "t",
                parameters: Schema.Struct({}),
                replay: "safe",
                execute: () => Effect.succeed({}),
              }),
            ],
          });
          const rounds = 5;
          const responses: Array<FauxResponseStep> = [];
          for (let round = 0; round <= rounds; round++) {
            responses.push(() =>
              Effect.succeed(
                fauxAssistantMessage([fauxToolCall("t", {}, { id: `c${round}` })], {
                  stopReason: "toolUse",
                }),
              ),
            );
            responses.push(() => Effect.succeed(fauxAssistantMessage([fauxText("done")])));
          }
          setup.faux.setResponses(responses);
          const { harness, root } = yield* open(inner, setup, [t]);
          yield* harness.harness.resume;
          const warmUp = (yield* root.submit({ type: "input", content: "warm up" })).id;
          yield* settled(harness, warmUp);
          resolutions = 0;
          for (let round = 0; round < rounds; round++) {
            const id = (yield* root.submit({ type: "input", content: `round ${round}` })).id;
            yield* settled(harness, id);
          }
          // One generation round with one tool call, as on `main` before the restart limit.
          assert.isAtMost(resolutions / rounds, RESOLUTIONS_PER_ROUND);
          yield* harness.close;
        }),
      ),
  );
});

/**
 * The settings resolutions for one round above: 151 on `main` before the restart limit landed (measured with this
 * round on `main`'s Harness) and 151 with it. Only a task that was cut off reads
 * `restartLimit`, so the normal path resolves nothing more for it.
 */
const RESOLUTIONS_PER_ROUND = 151;

// Recovery of an interrupted generation after a reopen, on every backend.
//
// A reopen is a second Harness over the same storage, so each case runs on memory and on SQLite; a section or
// response that waits for its signal is `Effect.never`, interrupted when close signals the invocation; the clock
// is the `TestClock`; the live settings are the reader of `setup.settings`; extensions are the static extensions
// of `openChat`; `submission.wait()` lets pending work run while the `TestClock` advances in 5 ms steps.
//
// Notes on some cases:
// - "resends a request interrupted before any partial without repeating preparation": the settings are
//   changed before the second build reads them.
// - "fails no_model when the pinned model is gone after reopen, in request and in poll": a Harness reopened
//   "without the faux provider" gets a fake of another provider.
// - "resumes a retry backoff after reopen": the wait for the backoff lets work run without advancing the
//   clock, since the test clock is fixed until the test moves it.
// - "sleeps again for a retry stored as a running phase by an earlier version" and "polls a checkpoint stored
//   without a thinking level and records none": the old record is written straight to storage instead of
//   editing the SQLite file.
import { assert, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import { AssistantEntry } from "../src/Entry.ts";
import * as Extension from "../src/Extension.ts";
import type { Conversation } from "../src/Harness.ts";
import type { EntryId, SubmissionId, TaskId } from "../src/Ids.ts";
import { LiveDoc } from "../src/Live.ts";
import * as Section from "../src/Section.ts";
import * as FauxModel from "../src/testing/FauxModel.ts";
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
import { anyNumber, awaited, eventually, type TestHarness } from "./HarnessSupport.ts";
import {
  acquire,
  type ControlledStorage,
  controlledStorage,
  describeEachBackend,
  flush,
  run,
} from "./SessionSupport.ts";
import { commitWrites } from "../src/testing/CommitWrites.ts";

/** One extension per section, named `section:<key>`. */
const addSection = <E, R>(
  key: string,
  render: (input: Section.PromptInput) => Effect.Effect<string | undefined, E, R>,
  options?: Section.SectionOptions,
) => Extension.make({ name: `section:${key}`, sections: [Section.make(key, render, options)] });

/** A Harness over the same storage, as a reopened SQLite file. */
const open = (
  storage: ControlledStorage,
  setup: ChatSetup,
  extensions: ReadonlyArray<Extension.Any> = [],
) => openChat(storage, setup, { extensions });

const liveOf = (h: TestHarness, conversation: Conversation) =>
  h.session.snapshot(LiveDoc, conversation.id).pipe(Effect.map(Option.getOrUndefined));

/** The task that holds the root's run. */
const runTaskId = (h: TestHarness, root: Conversation) =>
  liveOf(h, root).pipe(
    Effect.map((live) => {
      const taskId = live?.run?.taskId;
      if (taskId === undefined) throw new Error("The root has no run");
      return taskId;
    }),
  );

/** The checkpoint of a live task, none for a terminal one. */
const checkpoint = (h: TestHarness, id: TaskId) =>
  h.harness.getTask(id).pipe(
    Effect.map((found) => {
      if (Option.isNone(found)) return undefined;
      const state = found.value.state;
      // A terminal state carries no checkpoint.
      return "checkpoint" in state ? state.checkpoint : undefined;
    }),
  );

/** Waits for submission `id`, with timers running in 5 ms steps. */
const settleById = (h: TestHarness, id: SubmissionId) =>
  Effect.gen(function* () {
    const submission = Option.getOrThrow(yield* h.harness.submission(id));
    const fiber = yield* Effect.forkChild(submission.await, { startImmediately: true });
    yield* waitFor(Effect.sync(() => fiber.pollUnsafe() !== undefined));
    return yield* Fiber.join(fiber);
  });

const answerOf = (conversation: Conversation, id: EntryId) =>
  conversation.commit(
    Effect.gen(function* () {
      const tx = yield* Tx;
      return Option.getOrUndefined(yield* tx.entry(AssistantEntry, id));
    }),
  );

const kinds = (conversation: Conversation) =>
  allEntries(conversation).pipe(Effect.map((entries) => entries.map((entry) => entry.kind)));

describeEachBackend("generation recovery", (backend) => {
  it.effect("reruns preparation interrupted before its commit", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        const reached = Deferred.makeUnsafe<void>();
        let block = true;
        const extensions = [
          addSection("preamble", () =>
            Effect.suspend(() => {
              if (!block) return Effect.succeed("p");
              block = false;
              return Deferred.succeed(reached, undefined).pipe(Effect.andThen(Effect.never));
            }),
          ),
        ];
        setup.faux.setResponses([fauxAssistantMessage("answer")]);
        const storage = yield* controlledStorage;
        let opened = yield* open(storage, setup, extensions);
        yield* opened.harness.harness.resume;
        const id = (yield* opened.root.submit({ type: "input", content: "hi" })).id;
        yield* awaited(reached);
        const taskId = yield* runTaskId(opened.harness, opened.root);
        yield* opened.harness.close;

        opened = yield* open(storage, setup, extensions);
        assert.deepStrictEqual(yield* checkpoint(opened.harness, taskId), {
          phase: "prepare",
          attempt: 1,
        });
        yield* opened.harness.harness.resume;
        assert.strictEqual((yield* settleById(opened.harness, id)).status, "done");
        assert.deepStrictEqual(yield* kinds(opened.root), [
          "core.user",
          "core.system",
          "core.assistant",
        ]);
        yield* opened.harness.close;
      }),
    ),
  );

  it.effect("resends a request interrupted before any partial without repeating preparation", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        const extensions = [addSection("preamble", () => Effect.succeed("p"), { tag: false })];
        const reached = Deferred.makeUnsafe<void>();
        const sent: Array<Array<string>> = [];
        const timeouts: Array<number | undefined> = [];
        setup.faux.setResponses([
          () => Deferred.succeed(reached, undefined).pipe(Effect.andThen(Effect.never)),
          (request) =>
            Effect.sync(() => {
              sent.push(request.messages.map((message) => message.role));
              timeouts.push(request.options.timeoutMs);
              return fauxAssistantMessage("answer");
            }),
        ]);
        const storage = yield* controlledStorage;
        let opened = yield* open(storage, setup, extensions);
        setup.settings.stream = { timeoutMs: 1234 };
        yield* opened.harness.harness.resume;
        const id = (yield* opened.root.submit({ type: "input", content: "hi" })).id;
        yield* awaited(reached);
        const taskId = yield* runTaskId(opened.harness, opened.root);
        yield* opened.harness.close;

        opened = yield* open(storage, setup, extensions);
        const cutoff = (yield* allEntries(opened.root)).at(-1)?.id;
        assert.deepStrictEqual(yield* checkpoint(opened.harness, taskId), {
          phase: "request",
          attempt: 1,
          model: FAUX_MODEL,
          thinkingLevel: "off",
          streamOptions: { timeoutMs: 1234 },
          cutoff: cutoff ?? -1,
        });
        // The resend uses the pinned request, not options changed after preparation.
        setup.settings.stream = { timeoutMs: 999 };
        assert.strictEqual((yield* liveOf(opened.harness, opened.root))?.generation?.attempt, 1);
        yield* opened.harness.harness.resume;
        assert.strictEqual((yield* settleById(opened.harness, id)).status, "done");
        assert.deepStrictEqual(sent, [["system", "user"]]);
        assert.deepStrictEqual(timeouts, [1234]);
        assert.deepStrictEqual(yield* kinds(opened.root), [
          "core.user",
          "core.system",
          "core.assistant",
        ]);
        yield* opened.harness.close;
      }),
    ),
  );

  it.effect(
    "converts a committed partial into an aborted entry and resends the same messages",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const slow = chatSetup({ tokensPerSecond: 20, tokenSize: { min: 1, max: 1 } });
          slow.faux.setResponses([fauxAssistantMessage("z".repeat(400))]);
          const storage = yield* controlledStorage;
          let opened = yield* open(storage, slow);
          yield* opened.harness.harness.resume;
          const watch = yield* acquire(opened.harness.session.watchDoc(LiveDoc, opened.root.id));
          let watched: string | undefined;
          yield* Effect.forkChild(
            Stream.runForEach(watch.value.frames, (frame) =>
              Effect.sync(() => {
                watched = textOf(frame.value?.generation?.message) ?? watched;
              }),
            ),
            { startImmediately: true },
          );
          const id = (yield* opened.root.submit({ type: "input", content: "hi" })).id;
          yield* waitFor(Effect.sync(() => watched !== undefined));
          yield* opened.harness.close;

          const setup = chatSetup();
          const sent: Array<Array<string>> = [];
          setup.faux.setResponses([
            (request) =>
              Effect.sync(() => {
                sent.push(request.messages.map((message) => message.role));
                return fauxAssistantMessage("answer");
              }),
          ]);
          opened = yield* open(storage, setup);
          const stored = yield* liveOf(opened.harness, opened.root);
          const partial = textOf(stored?.generation?.message) ?? "";
          // Everything observers saw before the crash is durable.
          assert.isTrue(partial.startsWith(watched ?? "missing"));
          yield* opened.harness.harness.resume;
          assert.strictEqual((yield* settleById(opened.harness, id)).status, "done");
          assert.deepStrictEqual(sent, [["user"]]);
          const entries = yield* allEntries(opened.root);
          assert.deepStrictEqual(
            entries.map((entry) => entry.kind),
            ["core.user", "core.assistant", "core.assistant"],
          );
          const converted = entries[1]?.model?.[0];
          assert.strictEqual(
            converted?.role === "assistant" ? converted.stopReason : undefined,
            "aborted",
          );
          assert.strictEqual(textOf(converted), partial);
          assert.deepStrictEqual(yield* liveOf(opened.harness, opened.root), {});
          yield* opened.harness.close;
        }),
      ),
  );

  it.effect("resumes a retry backoff after reopen", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        yield* TestClock.setTime(1_000);
        setup.faux.setResponses([
          fauxAssistantMessage([], {
            stopReason: "error",
            errorMessage: "503 Service Unavailable",
          }),
          fauxAssistantMessage("recovered"),
        ]);
        const storage = yield* controlledStorage;
        let opened = yield* open(storage, setup);
        yield* opened.harness.harness.resume;
        setup.settings.retry = { enabled: true, maxRetries: 2, baseDelayMs: 60_000 };
        const id = (yield* opened.root.submit({ type: "input", content: "hi" })).id;
        const first = opened;
        yield* eventually(
          liveOf(first.harness, first.root).pipe(
            Effect.map((live) => live?.generation?.retry !== undefined),
          ),
        );
        const taskId = yield* runTaskId(opened.harness, opened.root);
        yield* opened.harness.close;

        opened = yield* open(storage, setup);
        assert.deepStrictEqual(
          Option.getOrUndefined(yield* opened.harness.harness.getTask(taskId))?.state,
          {
            status: "sleeping",
            checkpoint: { phase: "retry", attempt: 1, until: 61_000 },
            until: 61_000,
          },
        );
        // Once open's recovery pass is done, nothing is due before the backoff ends.
        yield* eventually(
          opened.harness.harness.wake.pipe(
            Effect.map((wake) => Option.getOrUndefined(wake.nextWakeAt) === 61_000),
          ),
        );
        assert.deepStrictEqual(yield* opened.harness.harness.wake, {
          nextWakeAt: Option.some(61_000),
          activeWork: false,
        });
        // An early wake retries nothing. The scheduler's own timer stands for the host's alarm: the clock does
        // not move, so it does not fire.
        yield* opened.harness.harness.resume;
        yield* flush;
        assert.strictEqual(setup.faux.state.callCount, 1);
        const live = yield* liveOf(opened.harness, opened.root);
        assert.deepStrictEqual(live, {
          run: { taskId, inputs: [id], turns: 0, first: anyNumber(live?.run?.first) },
          generation: { attempt: 1, retry: { at: 61_000, error: "503 Service Unavailable" } },
        });
        yield* TestClock.setTime(61_000);
        yield* opened.harness.harness.resume;
        assert.strictEqual((yield* settleById(opened.harness, id)).status, "done");
        assert.deepStrictEqual(yield* kinds(opened.root), [
          "core.user",
          "core.assistant",
          "core.assistant",
        ]);
        yield* opened.harness.close;
      }),
    ),
  );

  it.effect("sleeps again for a retry stored as a running phase by an earlier version", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        yield* TestClock.setTime(1_000);
        setup.faux.setResponses([
          fauxAssistantMessage([], {
            stopReason: "error",
            errorMessage: "503 Service Unavailable",
          }),
          fauxAssistantMessage("recovered"),
        ]);
        const storage = yield* controlledStorage;
        let opened = yield* open(storage, setup);
        yield* opened.harness.harness.resume;
        setup.settings.retry = { enabled: true, maxRetries: 2, baseDelayMs: 60_000 };
        const id = (yield* opened.root.submit({ type: "input", content: "hi" })).id;
        const taskId = yield* runTaskId(opened.harness, opened.root);
        const first = opened;
        yield* eventually(
          first.harness.harness
            .getTask(taskId)
            .pipe(
              Effect.map((record) => Option.getOrUndefined(record)?.state.status === "sleeping"),
            ),
        );
        yield* opened.harness.close;
        // Earlier versions slept inside a running `retry` phase.
        const stored = Option.getOrThrow(yield* storage.task(taskId));
        if (stored.state.status !== "sleeping") throw new Error("expected a sleeping task");
        yield* commitWrites(storage, [
          {
            type: "task",
            value: { ...stored, state: { status: "running", checkpoint: stored.state.checkpoint } },
          },
        ]);

        opened = yield* open(storage, setup);
        yield* opened.harness.harness.resume;
        yield* eventually(
          opened.harness.harness
            .getTask(taskId)
            .pipe(
              Effect.map((record) => Option.getOrUndefined(record)?.state.status === "sleeping"),
            ),
        );
        const state = Option.getOrUndefined(yield* opened.harness.harness.getTask(taskId))?.state;
        assert.strictEqual(state?.status === "sleeping" ? state.until : undefined, 61_000);
        assert.strictEqual(setup.faux.state.callCount, 1);
        yield* TestClock.setTime(61_000);
        yield* opened.harness.harness.resume;
        assert.strictEqual((yield* settleById(opened.harness, id)).status, "done");
        assert.strictEqual(setup.faux.state.callCount, 2);
        yield* opened.harness.close;
      }),
    ),
  );

  it.effect("resumes polling a deferred response after reopen", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup({ deferred: { pollAfterMs: 60_000 } });
        yield* TestClock.setTime(1_000);
        setup.faux.setResponses([fauxAssistantMessage("deferred answer")]);
        const storage = yield* controlledStorage;
        let opened = yield* open(storage, setup);
        yield* opened.harness.harness.resume;
        setup.settings.stream = { deferred: true };
        const id = (yield* opened.root.submit({ type: "input", content: "hi" })).id;
        const first = opened;
        yield* eventually(
          liveOf(first.harness, first.root).pipe(
            Effect.map((live) => live?.generation?.deferred !== undefined),
          ),
        );
        const taskId = yield* runTaskId(opened.harness, opened.root);
        yield* opened.harness.close;

        opened = yield* open(storage, setup);
        assert.deepInclude(yield* checkpoint(opened.harness, taskId), {
          phase: "poll",
          attempt: 1,
          pollAt: 61_000,
        });
        assert.deepInclude(
          Option.getOrUndefined(yield* opened.harness.harness.getTask(taskId))?.state,
          { status: "sleeping", until: 61_000 },
        );
        yield* eventually(opened.harness.harness.wake.pipe(Effect.map((wake) => !wake.activeWork)));
        assert.deepStrictEqual(yield* opened.harness.harness.wake, {
          nextWakeAt: Option.some(61_000),
          activeWork: false,
        });
        yield* TestClock.setTime(61_000);
        yield* opened.harness.harness.resume;
        const settled = yield* settleById(opened.harness, id);
        if (settled.status !== "done" || settled.type !== "input") {
          return assert.fail(`Unexpected ${settled.status}`);
        }
        const answer = yield* answerOf(opened.root, settled.answer);
        assert.strictEqual(textOf(answer?.model?.[0]), "deferred answer");
        assert.strictEqual(setup.faux.state.deferredFetchCount, 1);
        yield* opened.harness.close;
      }),
    ),
  );

  it.effect("polls a checkpoint stored without a thinking level and records none", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup({ deferred: { pollAfterMs: 60_000 } });
        yield* TestClock.setTime(1_000);
        setup.faux.setResponses([fauxAssistantMessage("deferred answer")]);
        const storage = yield* controlledStorage;
        let opened = yield* open(storage, setup);
        yield* opened.root.configure({ thinkingLevel: "high" });
        yield* opened.harness.harness.resume;
        setup.settings.stream = { deferred: true };
        const id = (yield* opened.root.submit({ type: "input", content: "hi" })).id;
        const first = opened;
        yield* eventually(
          liveOf(first.harness, first.root).pipe(
            Effect.map((live) => live?.generation?.deferred !== undefined),
          ),
        );
        const taskId = yield* runTaskId(opened.harness, opened.root);
        assert.deepInclude(yield* checkpoint(opened.harness, taskId), {
          phase: "poll",
          thinkingLevel: "high",
        });
        yield* opened.harness.close;
        // Earlier versions stored poll checkpoints without the field.
        const stored = Option.getOrThrow(yield* storage.task(taskId));
        if (stored.state.status !== "sleeping") throw new Error("expected a sleeping task");
        const PollCheckpoint = Schema.Record(Schema.String, Schema.Json);
        const { thinkingLevel: _thinkingLevel, ...older } = Schema.decodeUnknownSync(
          PollCheckpoint,
        )(stored.state.checkpoint);
        yield* commitWrites(storage, [
          { type: "task", value: { ...stored, state: { ...stored.state, checkpoint: older } } },
        ]);

        opened = yield* open(storage, setup);
        assert.notProperty(yield* checkpoint(opened.harness, taskId), "thinkingLevel");
        yield* TestClock.setTime(61_000);
        yield* opened.harness.harness.resume;
        const settled = yield* settleById(opened.harness, id);
        if (settled.status !== "done" || settled.type !== "input") {
          return assert.fail(`Unexpected ${settled.status}`);
        }
        const answer = (yield* answerOf(opened.root, settled.answer))?.model?.[0];
        assert.strictEqual(textOf(answer), "deferred answer");
        assert.notProperty(answer, "thinkingLevel");
        yield* opened.harness.close;
      }),
    ),
  );

  it.effect(
    "fails no_model when the pinned model is gone after reopen, in request and in poll",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const setup = chatSetup({ deferred: { pollAfterMs: 60_000 } });
          yield* TestClock.setTime(1_000);
          const busy = unanswered();
          setup.faux.setResponses([busy.step, fauxAssistantMessage("deferred")]);
          const storage = yield* controlledStorage;
          let opened = yield* open(storage, setup);
          yield* opened.harness.harness.resume;
          const requesting = (yield* opened.root.submit({ type: "input", content: "one" })).id;
          yield* awaited(busy.reached);
          yield* opened.harness.close;

          // Reopened without the faux provider: the request's pinned model is unknown.
          const empty: ChatSetup = { ...setup, faux: FauxModel.make({ provider: "other" }) };
          opened = yield* open(storage, empty);
          yield* opened.harness.harness.resume;
          const settled = yield* settleById(opened.harness, requesting);
          assert.strictEqual(settled.status, "unanswered");
          assert.strictEqual(
            settled.status === "unanswered" ? settled.reason : undefined,
            "no_model",
          );
          yield* opened.harness.close;

          opened = yield* open(storage, setup);
          setup.settings.stream = { deferred: true };
          yield* opened.harness.harness.resume;
          const polling = (yield* opened.root.submit({ type: "input", content: "two" })).id;
          const second = opened;
          yield* waitFor(
            liveOf(second.harness, second.root).pipe(
              Effect.map((live) => live?.generation?.deferred !== undefined),
            ),
          );
          yield* opened.harness.close;

          // The poll sleeps until it is due; then the model is resolved.
          yield* TestClock.setTime(61_000);
          opened = yield* open(storage, empty);
          yield* opened.harness.harness.resume;
          const polled = yield* settleById(opened.harness, polling);
          assert.strictEqual(
            polled.status === "unanswered" ? polled.reason : undefined,
            "no_model",
          );
          yield* opened.harness.close;
        }),
      ),
  );

  it.effect("runs a print-style turn and reads the durable answer after reopen", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        const extensions = [
          addSection("preamble", () => Effect.succeed("You are terse."), { tag: false }),
        ];
        setup.faux.setResponses([fauxAssistantMessage("42")]);
        const storage = yield* controlledStorage;
        let opened = yield* open(storage, setup, extensions);
        yield* opened.harness.harness.resume;
        const submission = yield* opened.root.submit({ type: "input", content: "answer?" });
        const settled = yield* settleById(opened.harness, submission.id);
        if (settled.status !== "done" || settled.type !== "input")
          return assert.fail(`Unexpected ${settled.status}`);
        const answerOf = (conversation: Conversation) =>
          conversation.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              return Option.getOrUndefined(yield* tx.entry(AssistantEntry, settled.answer));
            }),
          );
        const answer = yield* answerOf(opened.root);
        assert.strictEqual(textOf(answer?.model?.[0]), "42");
        yield* opened.harness.close;

        opened = yield* open(storage, setup, extensions);
        const reopened = Option.getOrThrow(yield* opened.harness.harness.submission(submission.id));
        assert.deepStrictEqual(yield* reopened.status, settled);
        assert.deepStrictEqual(yield* answerOf(opened.root), answer);
        yield* opened.harness.close;
      }),
    ),
  );
});

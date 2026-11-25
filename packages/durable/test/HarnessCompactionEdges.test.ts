// Part of the compaction suite: the inbox, edge cases, blocking and manual compaction together, pinning and late
// policy changes, a blocked compaction, context contributions, per-model overrides and per-conversation settings,
// on every backend. The header
// of `HarnessCompaction.test.ts` lists the shared setup; that of this part:
// - Watches and view histories are `Conversation.observe`: its value, and one view per commit, unfolded.
// - A section added late is a section of `open`'s extensions that renders nothing until the test turns it on; an
//   omitted section plans no entry.
// - A failing summarization is a `Models` layer over the faux one whose summarization stream dies.
// - A reopen is a new Harness over the backend's storage.
// - An invalid compaction setting is a `RangeError` defect of `configure`.
import { assert, it } from "@effect/vitest";
import * as Cause from "effect/Cause";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import { AgentDoc } from "../src/Agent.ts";
import type { CompactionResult } from "../src/Compaction.ts";
import type { ConversationId, TaskId } from "../src/Ids.ts";
import * as Extension from "../src/Extension.ts";
import { CompactionTask } from "../src/internal/compaction.ts";
import { LiveDoc, type LiveState } from "../src/Live.ts";
import type { AssistantMessage, UserMessage } from "../src/Message.ts";
import { Models } from "../src/Models.ts";
import * as Section from "../src/Section.ts";
import type { CompactionPolicy, CompactionSettings } from "../src/Settings.ts";
import type * as Task from "../src/Task.ts";
import { fauxAssistantMessage } from "../src/testing/FauxModel.ts";
import { Tx } from "../src/Tx.ts";
import { UsageDoc } from "../src/Usage.ts";
import type { ConversationView } from "../src/View.ts";
import { allEntries, chatSetup, openChat, waitFor } from "./ChatSupport.ts";
import {
  answer,
  type Chat,
  compactionHooks,
  failure,
  finished,
  firstUserText,
  gated,
  history,
  isSummaryRequest,
  kinds,
  live,
  MANUAL,
  open,
  OVERFLOW,
  placedBy,
  result,
  script,
  settle,
  submissionOf,
  summary,
  text,
  turn,
  userText,
} from "./CompactionSupport.ts";
import type { TestHarness } from "./HarnessSupport.ts";
import { controlledStorage, describeEachBackend, run } from "./SessionSupport.ts";

/** Background threshold at 500 and blocking threshold at 1500 tokens of a 2000-token window. */
const BACKGROUND: CompactionPolicy = {
  enabled: true,
  reserveTokens: 500,
  keepRecentTokens: 150,
  backgroundTokens: 1000,
};
/** Blocking threshold at 700 tokens of a 1000-token window, no background compaction. */
const BLOCKING: CompactionPolicy = {
  enabled: true,
  reserveTokens: 300,
  keepRecentTokens: 150,
  backgroundTokens: 0,
};

/** The live compaction tasks. */
const compactionTasks = (chat: Chat) =>
  chat.harness.harness.inspect.pipe(
    Effect.map((inspection) =>
      inspection.tasks
        .map((task) => task.record)
        .filter((record) => record.kind === "core.compaction"),
    ),
  );

/** A compaction task's ID, typed by its result. */
// SAFETY: every `core.compaction` task completes with a `CompactionResult`.
const compactionId = (id: TaskId) => id as TaskId<CompactionResult>;

const user = (content: string): UserMessage => ({ role: "user", content, timestamp: 0 });

/** Every view of the root from now on, one per commit, unfolded (`Conversation.observe`). */
const viewHistory = (chat: Chat) =>
  Effect.gen(function* () {
    const views: Array<ConversationView> = [];
    const observation = yield* chat.root.observe({
      advance: (frame) => {
        views.push(frame.value);
      },
      end: () => {},
    });
    views.push(observation.value);
    return {
      views,
      // SAFETY: the mount holds `core.live` as its definition materializes it.
      live: (): ReadonlyArray<LiveState> =>
        views.map((value) => (value.docs["core.live"] ?? {}) as LiveState),
      detach: observation.detach,
    };
  });

/** A section that renders nothing until `render` returns text, as if added after open. */
const lateSection = (key: string, render: () => Effect.Effect<string | undefined>) =>
  Extension.make({ name: `section:${key}`, sections: [Section.make(key, render)] });

/** Waits for task `id`. */
const settleTask = (h: TestHarness, id: TaskId) => finished(h.harness.awaitTask(id));

/** A manual compaction's summary write, once the compaction completed. */
const queuedSummary = (chat: Chat, content = "SUMMARY") =>
  Effect.gen(function* () {
    chat.faux.summaries.push(summary(content));
    const outcome = yield* result(chat, yield* chat.root.compact());
    return yield* submissionOf(chat, placedBy(outcome));
  });

/** A run whose answer waits for `release`. */
const busy = (chat: Chat, reply: AssistantMessage = answer("done")) =>
  Effect.gen(function* () {
    const gate = yield* Deferred.make<void>();
    const reached = yield* Deferred.make<void>();
    chat.faux.agent.push(gated(gate, reply, reached));
    const input = yield* chat.root.submit({ type: "input", content: "busy" });
    yield* finished(Deferred.await(reached));
    return { input, release: Deferred.succeed(gate, undefined) };
  });

/** The faux model's bucket of a conversation's `core.usage`. */
const usage = (h: TestHarness, conversationId: ConversationId) =>
  h.session
    .snapshot(UsageDoc, conversationId)
    .pipe(Effect.map((value) => Option.getOrThrow(value).models["faux/faux-1"]));

describeEachBackend("compaction and the inbox", (backend) => {
  it.effect(
    "places a reset queued after the summary last, and makes a summary queued after a reset stale",
    () =>
      Effect.gen(function* () {
        for (const order of ["summary first", "reset first"] as const) {
          yield* run(backend)(
            Effect.gen(function* () {
              const chat = yield* open();
              yield* history(chat);
              const running = yield* busy(chat);
              let submission;
              if (order === "summary first") {
                submission = yield* queuedSummary(chat);
                yield* chat.root.reset();
              } else {
                yield* chat.root.reset();
                submission = yield* queuedSummary(chat);
              }
              yield* running.release;
              yield* settle(running.input);
              const settled = yield* settle(submission);
              assert.strictEqual(settled.status, order === "summary first" ? "done" : "unanswered");
              assert.strictEqual((yield* kinds(chat.root)).at(-1), "core.reset");
              assert.strictEqual((yield* chat.root.context()).head?.kind, "core.reset");
              yield* chat.harness.close;
            }),
          );
        }
      }),
  );

  it.effect(
    "places a summary left queued by a failed run at the next submission, before its input",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const chat = yield* open();
          yield* history(chat);
          const running = yield* busy(chat, failure("bad request"));
          const submission = yield* queuedSummary(chat);
          yield* running.release;
          assert.strictEqual((yield* settle(running.input)).status, "unanswered");
          assert.strictEqual((yield* submission.status).status, "queued");
          yield* turn(chat, "again", "ok");
          assert.strictEqual((yield* submission.status).status, "done");
          const request = chat.faux.agentRequests.at(-1)!.messages;
          assert.include(firstUserText(request), "SUMMARY");
          assert.include(request.map(userText), "again");
          yield* chat.harness.close;
        }),
      ),
  );

  it.effect("keeps the full retry budget after an overflow compaction", () =>
    run(backend)(
      Effect.gen(function* () {
        const chat = yield* open();
        yield* history(chat);
        chat.setup.settings.compaction = { ...MANUAL, enabled: true };
        chat.faux.summaries.push(summary());
        chat.faux.agent.push(
          failure("prompt is too long"),
          failure("overloaded"),
          failure("overloaded"),
          answer("finally"),
        );
        const input = yield* chat.root.submit({ type: "input", content: text("u4", 100) });
        assert.strictEqual((yield* settle(input)).status, "done");
        yield* chat.harness.close;
      }),
    ),
  );

  it.effect("loses an application edit placed while a compaction summarizes", () =>
    run(backend)(
      Effect.gen(function* () {
        const chat = yield* open();
        yield* history(chat);
        const [u1] = yield* allEntries(chat.root);
        const gate = yield* Deferred.make<void>();
        const reached = yield* Deferred.make<void>();
        chat.faux.summaries.push(gated(gate, summary(), reached));
        const id = yield* chat.root.compact();
        yield* finished(Deferred.await(reached));
        const edit = { target: u1!.id, action: "replace", messages: [user("REDACTED")] } as const;
        yield* chat.root.submit({ type: "write", entry: { kind: "app.redact", edits: [edit] } });
        yield* Deferred.succeed(gate, undefined);
        yield* result(chat, id);
        // The summary was made from the unredacted entry, and the edit's target left the range.
        assert.include(userText(chat.faux.summaryRequests[0]!.messages[1]), "[User]: u1 ");
        assert.notInclude((yield* chat.root.context()).messages.map(userText), "REDACTED");
        yield* chat.harness.close;
      }),
    ),
  );

  it.effect("appends the summary to the conversation view in place, its head on a kept entry", () =>
    run(backend)(
      Effect.gen(function* () {
        const chat = yield* open();
        yield* history(chat);
        const views = yield* viewHistory(chat);
        const before = views.views[0]!.entries;
        const latest = () => views.views.at(-1)!;
        chat.faux.summaries.push(summary());
        yield* result(chat, yield* chat.root.compact());
        yield* waitFor(Effect.sync(() => latest().entries.at(-1)?.kind === "core.compaction"));
        // Nothing leaves the window: the summary is appended where it was written.
        assert.deepStrictEqual(latest().entries.slice(0, before.length), before);
        assert.deepStrictEqual(latest().entries, yield* allEntries(chat.root));
        const head = (yield* chat.root.context()).head!;
        assert.deepStrictEqual(latest().entries.at(-1), head);
        assert.isTrue(latest().entries.some((entry) => entry.id === head.head));
        views.detach();
        yield* chat.harness.close;
      }),
    ),
  );
});

describeEachBackend("compaction edge cases", (backend) => {
  it.effect("keeps the one-compaction limit through a retry backoff", () =>
    run(backend)(
      Effect.gen(function* () {
        let asked = 0;
        const chat = yield* open({
          contextWindow: 1000,
          extensions: [
            compactionHooks({
              beforeCompact: () =>
                Effect.sync(() => {
                  asked++;
                  return { decline: true } as const;
                }),
            }),
          ],
        });
        yield* history(chat);
        chat.setup.settings.compaction = BLOCKING;
        chat.faux.agent.push(failure("overloaded"));
        yield* turn(chat, text("u4", 200), "a4");
        // One blocking compaction, declined; the retried preparation, still above the threshold, started no other.
        assert.strictEqual(asked, 1);
        assert.strictEqual(chat.faux.agentRequests.length, 5);
        yield* chat.harness.close;
      }),
    ),
  );

  it.effect(
    "does not start a background compaction when a manual one was admitted during preparation",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const rendering = yield* Deferred.make<void>();
          const release = yield* Deferred.make<void>();
          let hold = false;
          const slow = lateSection("slow", () =>
            Effect.gen(function* () {
              if (hold) {
                hold = false;
                yield* Deferred.succeed(rendering, undefined);
                yield* Deferred.await(release);
              }
              return "slow";
            }),
          );
          const chat = yield* open({ contextWindow: 2000, extensions: [slow] });
          yield* history(chat);
          chat.setup.settings.compaction = BACKGROUND;
          // The section renders from the start and holds from now on.
          hold = true;
          const reached = yield* Deferred.make<void>();
          chat.faux.summaries.push(gated(yield* Deferred.make<void>(), summary(), reached));
          chat.faux.agent.push(answer("a4"));
          const input = yield* chat.root.submit({ type: "input", content: text("u4", 100) });
          yield* finished(Deferred.await(rendering));
          const manual = yield* chat.root.compact();
          yield* finished(Deferred.await(reached));
          yield* Deferred.succeed(release, undefined);
          assert.strictEqual((yield* settle(input)).status, "done");
          assert.deepStrictEqual(
            (yield* compactionTasks(chat)).map((task) => task.id),
            [manual],
          );
          // A background compaction would have sent its own summarization request.
          assert.strictEqual(chat.faux.summaryRequests.length, 1);
          yield* chat.harness.harness.abortTask(manual);
          yield* chat.harness.close;
        }),
      ),
  );

  it.effect("starts no background compaction after a blocking one in the same generation", () =>
    run(backend)(
      Effect.gen(function* () {
        const chat = yield* open({ contextWindow: 2000 });
        yield* history(chat);
        // Blocking at 1500, background at 200: the kept part stays above the background threshold.
        chat.setup.settings.compaction = {
          ...BACKGROUND,
          backgroundTokens: 1300,
          keepRecentTokens: 400,
        };
        chat.faux.summaries.push(summary());
        yield* turn(chat, text("u4", 1000), "a4");
        assert.strictEqual(chat.faux.summaryRequests.length, 1);
        assert.deepStrictEqual(yield* compactionTasks(chat), []);
        yield* chat.harness.close;
      }),
    ),
  );

  // The summarization stream dies, so the compaction's phase faults.
  it.effect("fails the run when its blocking compaction faults, and records the fault", () =>
    run(backend)(
      Effect.gen(function* () {
        const base = chatSetup({
          models: [{ id: "faux-1", contextWindow: 1000, maxTokens: 900 }],
        });
        let broken = false;
        const setup = {
          ...base,
          faux: {
            ...base.faux,
            layer: Layer.effect(
              Models,
              Effect.gen(function* () {
                const inner = yield* Models;
                return Models.of({
                  resolve: inner.resolve,
                  stream: (model, request) =>
                    broken && isSummaryRequest(request.messages)
                      ? Stream.die(new Error("no credentials"))
                      : inner.stream(model, request),
                });
              }),
            ).pipe(Layer.provide(base.faux.layer)),
          },
        };
        const chat = yield* open({}, { setup, faux: script(base) });
        yield* history(chat);
        chat.setup.settings.compaction = BLOCKING;
        broken = true;
        const input = yield* chat.root.submit({ type: "input", content: text("u4", 200) });
        const settled = yield* settle(input);
        assert.deepStrictEqual(
          settled.status === "unanswered"
            ? { status: settled.status, reason: settled.reason, detail: settled.detail }
            : settled,
          {
            status: "unanswered",
            reason: "model_error",
            detail: "Compaction failed: no credentials",
          },
        );
        broken = false;
        assert.isFalse((yield* kinds(chat.root)).includes("core.compaction"));
        const state = yield* live(chat);
        assert.isUndefined(state.compactions);
        assert.strictEqual(state.compactionFailure?.trigger, "threshold");
        assert.strictEqual(state.compactionFailure?.error, "no credentials");
        yield* chat.harness.close;
      }),
    ),
  );

  it.effect(
    "fails an overflow run with its text when its compaction is aborted directly or itself overflows",
    () =>
      Effect.gen(function* () {
        for (const how of ["abort", "overflow"] as const) {
          yield* run(backend)(
            Effect.gen(function* () {
              const chat = yield* open();
              yield* history(chat);
              chat.setup.settings.compaction = { ...MANUAL, enabled: true };
              const reached = yield* Deferred.make<void>();
              if (how === "abort") {
                chat.faux.summaries.push(gated(yield* Deferred.make<void>(), summary(), reached));
              } else chat.faux.summaries.push(failure("prompt is too long for the summary"));
              chat.faux.agent.push(failure(OVERFLOW));
              const input = yield* chat.root.submit({ type: "input", content: text("u4", 100) });
              if (how === "abort") {
                yield* finished(Deferred.await(reached));
                const [child] = yield* compactionTasks(chat);
                yield* chat.harness.harness.abortTask(child!.id);
              }
              const settled = yield* settle(input);
              assert.strictEqual(settled.status, "unanswered");
              assert.strictEqual(settled.status === "unanswered" && settled.reason, "model_error");
              assert.strictEqual(settled.status === "unanswered" && settled.detail, OVERFLOW);
              assert.isUndefined((yield* live(chat)).compactions);
              yield* chat.harness.close;
            }),
          );
        }
      }),
  );

  it.effect("treats a length stop as an ordinary answer, not an overflow", () =>
    run(backend)(
      Effect.gen(function* () {
        const chat = yield* open();
        yield* history(chat);
        chat.setup.settings.compaction = { ...MANUAL, enabled: true };
        chat.faux.agent.push(
          fauxAssistantMessage("cut short", { stopReason: "length", errorMessage: OVERFLOW }),
        );
        const input = yield* chat.root.submit({ type: "input", content: "go" });
        assert.strictEqual((yield* settle(input)).status, "done");
        assert.strictEqual(chat.faux.summaryRequests.length, 0);
        yield* chat.harness.close;
      }),
    ),
  );

  it.effect("places a summary at once in the commit that ends its compaction", () =>
    run(backend)(
      Effect.gen(function* () {
        const chat = yield* open();
        yield* history(chat);
        const views = yield* viewHistory(chat);
        chat.faux.summaries.push(summary());
        yield* result(chat, yield* chat.root.compact());
        views.detach();
        const states = views.live();
        const listed = states.findIndex((state) => state.compactions !== undefined);
        const ended = states.findIndex(
          (state, index) => index > listed && state.compactions === undefined,
        );
        const before = views.views[ended - 1]!;
        const after = views.views[ended]!;
        assert.deepStrictEqual(
          after.entries.slice(before.entries.length).map((entry) => entry.kind),
          ["core.compaction"],
        );
        yield* chat.harness.close;
      }),
    ),
  );

  it.effect(
    "lists a background compaction in the preparation commit, with the entries it appends",
    () =>
      run(backend)(
        Effect.gen(function* () {
          let extra = false;
          // The section renders nothing until now.
          const section = lateSection("extra", () =>
            Effect.sync(() => (extra ? "extra" : undefined)),
          );
          const chat = yield* open({ contextWindow: 2000, extensions: [section] });
          yield* history(chat);
          chat.setup.settings.compaction = BACKGROUND;
          const views = yield* viewHistory(chat);
          // A new section makes the preparation commit append a system entry next to the compaction's status.
          extra = true;
          chat.faux.summaries.push(gated(yield* Deferred.make<void>(), summary()));
          yield* turn(chat, text("u4", 100), "a4");
          yield* waitFor(Effect.map(live(chat), (state) => state.compactions !== undefined));
          views.detach();
          const started = views.live().findIndex((state) => state.compactions !== undefined);
          const appended = views.views[started]!.entries.slice(
            views.views[started - 1]!.entries.length,
          );
          assert.deepStrictEqual(
            appended.map((entry) => entry.kind),
            ["core.system"],
          );
          yield* chat.harness.harness.abortTask((yield* compactionTasks(chat))[0]!.id);
          yield* chat.harness.close;
        }),
      ),
  );

  it.effect(
    "keeps a background summary queued through a retry backoff while a blocking compaction wins",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const chat = yield* open({ contextWindow: 2000 });
          yield* history(chat);
          chat.setup.settings.compaction = BACKGROUND;
          chat.setup.settings.retry = { enabled: true, maxRetries: 2, baseDelayMs: 300 };
          const summaryGate = yield* Deferred.make<void>();
          const summaryReached = yield* Deferred.make<void>();
          chat.faux.summaries.push(gated(summaryGate, summary("BACKGROUND"), summaryReached));
          chat.faux.summaries.push(summary("BLOCKING"));
          const failed = yield* Deferred.make<void>();
          chat.faux.agent.push(() =>
            Deferred.succeed(failed, undefined).pipe(Effect.as(failure("overloaded"))),
          );
          chat.faux.agent.push(answer("a4"));
          const input = yield* chat.root.submit({ type: "input", content: text("u4", 100) });
          yield* finished(Effect.all([Deferred.await(failed), Deferred.await(summaryReached)]));
          // During the backoff: the background summary queues, and the thresholds drop so the retry blocks.
          yield* waitFor(Effect.map(live(chat), (state) => state.generation?.retry !== undefined));
          const [background] = yield* compactionTasks(chat);
          yield* Deferred.succeed(summaryGate, undefined);
          const queued = yield* result(chat, compactionId(background!.id));
          const submission = yield* submissionOf(chat, placedBy(queued));
          assert.strictEqual((yield* submission.status).status, "queued");
          chat.setup.settings.compaction = {
            ...BACKGROUND,
            reserveTokens: 1500,
            keepRecentTokens: 50,
          };
          assert.strictEqual((yield* settle(input)).status, "done");
          assert.include(firstUserText(chat.faux.agentRequests.at(-1)!.messages), "BLOCKING");
          const settled = yield* settle(submission);
          assert.strictEqual(settled.status, "unanswered");
          assert.strictEqual(settled.status === "unanswered" && settled.reason, "stale");
          yield* chat.harness.close;
        }),
      ),
  );

  it.effect("summarizes the previous summary in a second compaction", () =>
    run(backend)(
      Effect.gen(function* () {
        const chat = yield* open();
        yield* history(chat);
        chat.faux.summaries.push(summary("FIRST"));
        yield* result(chat, yield* chat.root.compact());
        yield* turn(chat, text("u4", 100), text("a4", 100));
        yield* turn(chat, text("u5", 100), text("a5", 100));
        chat.faux.summaries.push(summary("SECOND"));
        yield* result(chat, yield* chat.root.compact());
        const prompt = userText(chat.faux.summaryRequests[1]!.messages[1]);
        assert.match(
          prompt,
          /^<conversation>\n\[User\]: The conversation history before this point was compacted/,
        );
        assert.include(prompt, "FIRST");
        assert.include(firstUserText((yield* chat.root.context()).messages), "SECOND");
        yield* chat.harness.close;
      }),
    ),
  );

  // `failNextCommit` takes a message and fails with a `StorageFailure`; the reopen is a new
  // Harness over the same storage.
  it.effect(
    "leaves no usage, submission, or summary when the classifying commit fails, which fails the Harness",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const storage = yield* controlledStorage;
          const chat = yield* open({ storage });
          yield* history(chat);
          const before = yield* usage(chat.harness, chat.root.id);
          chat.faux.summaries.push(() =>
            Effect.sync(() => {
              storage.failNextCommit("disk gone");
              return summary();
            }),
          );
          const id = yield* chat.root.compact();
          const closed = yield* finished(chat.harness.harness.closed);
          assert.strictEqual(closed.reason, "failed");
          assert.strictEqual(closed.reason === "failed" && closed.error.message, "disk gone");
          yield* chat.harness.close;
          // Nothing of the failed commit landed: the task is still live, without usage, entry, or submission.
          assert.isTrue(
            Option.isNone(yield* storage.submissionByRequest(chat.root.id, `compaction:${id}`)),
          );
          assert.notStrictEqual(
            Option.getOrUndefined(yield* storage.task(id))?.state.status,
            "terminal",
          );
          const reopened = yield* openChat(storage, chat.setup);
          assert.deepStrictEqual(yield* usage(reopened.harness, reopened.root.id), before);
          assert.isFalse((yield* kinds(reopened.root)).includes("core.compaction"));
          yield* reopened.harness.close;
        }),
      ),
  );
});

describeEachBackend("blocking and manual compaction together", (backend) => {
  /** A run whose generation waits on a blocking compaction whose summary is held; returns the input and gate. */
  const blockingRun = (chat: Chat) =>
    Effect.gen(function* () {
      yield* history(chat);
      chat.setup.settings.compaction = BLOCKING;
      const gate = yield* Deferred.make<void>();
      const reached = yield* Deferred.make<void>();
      chat.faux.summaries.push(gated(gate, summary("BLOCKING"), reached));
      const input = yield* chat.root.submit({ type: "input", content: text("u4", 200) });
      yield* finished(Deferred.await(reached));
      const [blocking] = yield* compactionTasks(chat);
      return { input, gate, blocking: compactionId(blocking!.id) };
    });

  it.effect(
    "places a manual summary selected before the blocking one landed; its equal cut replaces it",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const chat = yield* open({ contextWindow: 1000 });
          const running = yield* blockingRun(chat);
          // Selected from the same context as the blocking compaction, so it cuts at the same entry.
          chat.faux.summaries.push(summary("MANUAL"));
          const manual = yield* result(chat, yield* chat.root.compact());
          const submission = yield* submissionOf(chat, placedBy(manual));
          assert.strictEqual((yield* submission.status).status, "queued");
          chat.faux.agent.push(answer("a4"));
          yield* Deferred.succeed(running.gate, undefined);
          assert.strictEqual((yield* settle(running.input)).status, "done");
          // The request after the blocking compaction used its summary; the manual one landed at the final boundary.
          assert.include(firstUserText(chat.faux.agentRequests.at(-1)!.messages), "BLOCKING");
          assert.strictEqual((yield* settle(submission)).status, "done");
          const markers = (yield* allEntries(chat.root)).filter(
            (record) => record.kind === "core.compaction",
          );
          assert.strictEqual(markers.length, 2);
          assert.strictEqual(markers[1]!.head, markers[0]!.head);
          const messages = (yield* chat.root.context()).messages;
          assert.include(firstUserText(messages), "MANUAL");
          assert.isFalse(messages.some((message) => userText(message).includes("BLOCKING")));
          yield* chat.harness.close;
        }),
      ),
  );

  it.effect(
    "finds nothing to compact for a manual compaction selected after the blocking summary landed",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const chat = yield* open({ contextWindow: 1000 });
          const running = yield* blockingRun(chat);
          const answerGate = yield* Deferred.make<void>();
          const answerReached = yield* Deferred.make<void>();
          chat.faux.agent.push(gated(answerGate, answer("a4"), answerReached));
          yield* Deferred.succeed(running.gate, undefined);
          yield* finished(Deferred.await(answerReached));
          assert.deepStrictEqual(yield* result(chat, yield* chat.root.compact()), {
            status: "completed",
            result: {},
          });
          assert.strictEqual(chat.faux.summaryRequests.length, 1);
          yield* Deferred.succeed(answerGate, undefined);
          assert.strictEqual((yield* settle(running.input)).status, "done");
          yield* chat.harness.close;
        }),
      ),
  );

  it.effect("aborts the run and both compactions on Esc and appends nothing", () =>
    run(backend)(
      Effect.gen(function* () {
        const chat = yield* open({ contextWindow: 1000 });
        const running = yield* blockingRun(chat);
        const manualReached = yield* Deferred.make<void>();
        chat.faux.summaries.push(
          gated(yield* Deferred.make<void>(), summary("MANUAL"), manualReached),
        );
        const manual = yield* chat.root.compact();
        yield* finished(Deferred.await(manualReached));
        assert.deepStrictEqual(
          (yield* live(chat)).compactions?.map((status) => status.blocking),
          [true, false],
        );
        yield* finished(chat.root.abort());
        const settled = yield* settle(running.input);
        assert.strictEqual(settled.status, "unanswered");
        assert.strictEqual(settled.status === "unanswered" && settled.reason, "aborted");
        assert.strictEqual((yield* result(chat, running.blocking)).status, "aborted");
        assert.strictEqual((yield* result(chat, manual)).status, "aborted");
        assert.isUndefined((yield* live(chat)).compactions);
        assert.isUndefined((yield* live(chat)).run);
        assert.isFalse((yield* kinds(chat.root)).includes("core.compaction"));
        assert.deepStrictEqual((yield* chat.harness.harness.inspect).submissions, []);
        yield* chat.harness.close;
      }),
    ),
  );

  it.effect("places a summary that survived Esc at the next submission, before its input", () =>
    run(backend)(
      Effect.gen(function* () {
        const chat = yield* open();
        yield* history(chat);
        const reached = yield* Deferred.make<void>();
        chat.faux.agent.push(gated(yield* Deferred.make<void>(), answer("never"), reached));
        const busy = yield* chat.root.submit({ type: "input", content: "busy" });
        yield* finished(Deferred.await(reached));
        chat.faux.summaries.push(summary());
        const outcome = yield* result(chat, yield* chat.root.compact());
        const submission = yield* submissionOf(chat, placedBy(outcome));
        yield* finished(chat.root.abort());
        const settled = yield* settle(busy);
        assert.strictEqual(settled.status, "unanswered");
        assert.strictEqual(settled.status === "unanswered" && settled.reason, "aborted");
        assert.strictEqual((yield* submission.status).status, "queued");
        yield* turn(chat, "u5", "a5");
        assert.strictEqual((yield* submission.status).status, "done");
        const request = chat.faux.agentRequests.at(-1)!.messages;
        assert.include(firstUserText(request), "SUMMARY");
        assert.include(request.map(userText), "u5");
        const tail = (yield* allEntries(chat.root)).slice(-4).map((record) => record.kind);
        assert.deepStrictEqual(tail, [
          "core.compaction",
          "core.user",
          "core.system",
          "core.assistant",
        ]);
        yield* chat.harness.close;
      }),
    ),
  );
});

describeEachBackend("compaction pinning, silent overflow, and late policy changes", (backend) => {
  it.effect("keeps the pinned model through a retry and advances the live attempt", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup({
          models: [
            { id: "faux-1", contextWindow: 100_000, maxTokens: 900 },
            { id: "faux-2", contextWindow: 100_000, maxTokens: 900 },
          ],
        });
        const chat = yield* open({}, { setup, faux: script(setup) });
        yield* history(chat);
        let attempt: number | undefined;
        chat.faux.summaries.push(() =>
          // Switched during the attempt: the retry still uses the pinned model.
          chat.root
            .configure({ model: { provider: "faux", modelId: "faux-2" } })
            .pipe(Effect.orDie, Effect.as(failure("overloaded"))),
        );
        chat.faux.summaries.push(() =>
          live(chat).pipe(
            Effect.orDie,
            Effect.map((state) => {
              attempt = state.compactions?.[0]?.attempt;
              return summary();
            }),
          ),
        );
        assert.strictEqual((yield* result(chat, yield* chat.root.compact())).status, "completed");
        assert.deepStrictEqual(
          chat.faux.summaryRequests.map((request) => request.model),
          ["faux-1", "faux-1"],
        );
        assert.strictEqual(attempt, 2);
        const usage = Option.getOrThrow(
          yield* chat.harness.session.snapshot(UsageDoc, chat.root.id),
        ).models;
        assert.deepStrictEqual(Object.keys(usage), ["faux/faux-1"]);
        yield* chat.harness.close;
      }),
    ),
  );

  // A late observer attached now: its first value.
  it.effect("shows a late joiner a compaction that is summarizing", () =>
    run(backend)(
      Effect.gen(function* () {
        const chat = yield* open();
        yield* history(chat);
        const reached = yield* Deferred.make<void>();
        chat.faux.summaries.push(gated(yield* Deferred.make<void>(), summary(), reached));
        const id = yield* chat.root.compact();
        yield* finished(Deferred.await(reached));
        const views = yield* viewHistory(chat);
        assert.deepStrictEqual(views.live()[0]!.compactions, [
          { taskId: id, trigger: "manual", blocking: false, attempt: 1 },
        ]);
        views.detach();
        yield* chat.harness.harness.abortTask(id);
        yield* chat.harness.close;
      }),
    ),
  );

  for (const [name, response] of [
    ["a stop whose input exceeds the window", answer("fine")],
    [
      "a length stop that fills the window without output",
      fauxAssistantMessage("", { stopReason: "length" }),
    ],
  ] as const) {
    it.effect(`treats silent overflow as an ordinary answer: ${name}`, () =>
      run(backend)(
        Effect.gen(function* () {
          const chat = yield* open({ contextWindow: 300 });
          yield* history(chat);
          // Thresholds out of reach, so only overflow classification could compact.
          chat.setup.settings.compaction = {
            enabled: true,
            reserveTokens: -100_000,
            keepRecentTokens: 150,
            backgroundTokens: 0,
          };
          chat.faux.agent.push(response);
          const input = yield* chat.root.submit({ type: "input", content: "go" });
          assert.strictEqual((yield* settle(input)).status, "done");
          const [last] = (yield* allEntries(chat.root)).slice(-1);
          // SAFETY: the last entry is the run's `core.assistant` answer.
          const message = last!.model![0] as AssistantMessage;
          assert.isAtLeast(message.usage.input, 300);
          assert.strictEqual(chat.faux.summaryRequests.length, 0);
          yield* chat.harness.close;
        }),
      ),
    );
  }

  it.effect(
    "sends the request when a blocking compaction finds nothing under a policy changed after preparation",
    () =>
      run(backend)(
        Effect.gen(function* () {
          let armed = false;
          let changed = false;
          let chat: Chat | undefined;
          const policy = lateSection("policy", () =>
            Effect.sync(() => {
              // Rendering runs after preparation read the policy; the compaction reads this one.
              if (armed && !changed && chat !== undefined) {
                changed = true;
                chat.setup.settings.compaction = { ...BLOCKING, keepRecentTokens: 100_000 };
              }
              return "p";
            }),
          );
          chat = yield* open({ contextWindow: 1000, extensions: [policy] });
          yield* history(chat);
          chat.setup.settings.compaction = BLOCKING;
          // The section renders from the start and changes the policy from now on.
          armed = true;
          yield* turn(chat, text("u4", 200), "a4");
          assert.deepStrictEqual(yield* compactionTasks(chat), []);
          assert.strictEqual(chat.faux.summaryRequests.length, 0);
          assert.isFalse((yield* kinds(chat.root)).includes("core.compaction"));
          yield* chat.harness.close;
        }),
      ),
  );
});

describeEachBackend("blocked compaction", (backend) => {
  it.effect("survives reopen blocked and is orphaned on abort with its status removed", () =>
    run(backend)(
      Effect.gen(function* () {
        const storage = yield* controlledStorage;
        const setup = chatSetup();
        const first = yield* openChat(storage, setup);
        // A compaction stored by a newer version than this process registers, for example after a downgrade.
        const newer: Task.Any = {
          definition: { ...CompactionTask.definition, version: 2 },
        };
        const id = yield* first.root.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            const taskId = yield* tx.createTask(
              // SAFETY: the built-in compaction's definition at another version, spread from the built-in.
              newer as typeof CompactionTask,
              { reason: "manual" },
              { ownership: { kind: "conversation" } },
            );
            const state = yield* tx.doc(LiveDoc, first.root.id);
            state.compactions = [{ taskId, trigger: "manual", blocking: false, attempt: 1 }];
            return taskId;
          }),
        );
        yield* first.harness.close;
        const { harness, root } = yield* openChat(storage, setup);
        yield* harness.harness.resume;
        const chat: Chat = { harness, root, setup, faux: script(setup) };
        const inspection = yield* harness.harness.inspect;
        const state = inspection.tasks.find((task) => task.record.id === id)?.state;
        assert.strictEqual(state?.kind, "blocked");
        assert.strictEqual(state?.kind === "blocked" && state.reason, "task_too_old");
        yield* harness.harness.abortTask(id);
        assert.deepStrictEqual((yield* settleTask(harness, id)).state.outcome, {
          status: "orphaned",
          reason: "task_too_old",
        });
        assert.isUndefined((yield* live(chat)).compactions);
        assert.deepStrictEqual((yield* live(chat)).compactionFailure, {
          taskId: id,
          trigger: "manual",
          error: "task_too_old",
        });
        yield* harness.close;
      }),
    ),
  );
});

describeEachBackend("context contributions", (backend) => {
  it.effect("apply edits carried by an older head marker in the range", () =>
    run(backend)(
      Effect.gen(function* () {
        const chat = yield* open();
        const conversationId = chat.root.id;
        const ids = yield* chat.root.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            const a = yield* tx.appendEntry(conversationId, {
              kind: "app.note",
              model: [user("a")],
            });
            const b = yield* tx.appendEntry(conversationId, {
              kind: "app.note",
              model: [user("b")],
            });
            // An older marker that omits b, then a newer one whose range still contains the older marker.
            yield* tx.appendEntry(conversationId, {
              kind: "app.head",
              head: a.id,
              edits: [{ target: b.id, action: "omit" }],
            });
            const c = yield* tx.appendEntry(conversationId, {
              kind: "app.note",
              model: [user("c")],
            });
            yield* tx.appendEntry(conversationId, {
              kind: "app.head",
              head: a.id,
              model: [user("H")],
            });
            return { a: a.id, b: b.id, c: c.id };
          }),
        );
        const view = yield* chat.root.context();
        assert.deepStrictEqual(view.entries.map((record) => record.id).slice(1), [
          ids.a,
          ids.b,
          ids.c,
        ]);
        assert.deepStrictEqual(
          view.contributions.map((messages) => messages.map(userText)),
          [["H"], ["a"], [], ["c"]],
        );
        assert.deepStrictEqual(view.messages.map(userText), ["H", "a", "c"]);
        // The summarizer sees the same contributions: the omitted entry stays out.
        chat.setup.settings.compaction = { ...MANUAL, keepRecentTokens: 1 };
        chat.faux.summaries.push(summary());
        yield* result(chat, yield* chat.root.compact());
        const prompt = userText(chat.faux.summaryRequests[0]!.messages[1]);
        assert.include(prompt, "<conversation>\n[User]: H\n\n[User]: a\n</conversation>");
        yield* chat.harness.close;
      }),
    ),
  );
});

// Per-model compaction settings.
describeEachBackend("per-model compaction overrides", (backend) => {
  it.effect("applies the current model's override to the blocking threshold", () =>
    Effect.gen(function* () {
      for (const key of ["faux/faux-1", "faux/other"]) {
        yield* run(backend)(
          Effect.gen(function* () {
            const chat = yield* open({ contextWindow: 1000 });
            yield* history(chat);
            // Harness-wide: blocking only above the full window; the override for faux-1 blocks above 700.
            chat.setup.settings.compaction = {
              ...BLOCKING,
              reserveTokens: 0,
              modelOverrides: { [key]: { reserveTokens: 300 } },
            };
            chat.faux.summaries.push(summary());
            yield* turn(chat, text("u4", 200), "a4");
            assert.strictEqual(chat.faux.summaryRequests.length, key === "faux/faux-1" ? 1 : 0);
            yield* chat.harness.close;
          }),
        );
      }
    }),
  );

  it.effect("applies the request model's override on a context overflow", () =>
    run(backend)(
      Effect.gen(function* () {
        const chat = yield* open();
        yield* history(chat);
        chat.setup.settings.compaction = {
          ...MANUAL,
          modelOverrides: { "faux/faux-1": { enabled: true } },
        };
        chat.faux.summaries.push(summary());
        chat.faux.agent.push(failure(OVERFLOW), answer("fits"));
        const input = yield* chat.root.submit({ type: "input", content: text("u4", 100) });
        assert.strictEqual((yield* settle(input)).status, "done");
        assert.strictEqual(chat.faux.summaryRequests.length, 1);
        yield* chat.harness.close;
      }),
    ),
  );

  it.effect("applies the model's override when a compaction selects and sizes its summary", () =>
    run(backend)(
      Effect.gen(function* () {
        const chat = yield* open();
        yield* history(chat);
        chat.setup.settings.compaction = {
          ...MANUAL,
          keepRecentTokens: 100_000,
          modelOverrides: { "faux/faux-1": { keepRecentTokens: 150, reserveTokens: 500 } },
        };
        chat.faux.summaries.push(summary());
        assert.strictEqual((yield* result(chat, yield* chat.root.compact())).status, "completed");
        // 0.8 * 500 from the override, below the model's 900.
        assert.strictEqual(chat.faux.summaryRequests[0]!.options.maxTokens, 400);
        const messages = (yield* chat.root.context()).messages;
        assert.deepStrictEqual(messages.slice(1).map(userText), [text("u3", 100), text("a3", 100)]);
        yield* chat.harness.close;
      }),
    ),
  );
});

// Per-conversation compaction settings.
describeEachBackend("per-conversation compaction settings", (backend) => {
  it.effect(
    "resolves model-specific over general, and the conversation over the harness at the same level",
    () =>
      Effect.gen(function* () {
        // The summary's maxTokens is 0.8 * the resolved reserveTokens, below the model's 900.
        const cases: ReadonlyArray<{
          readonly harness: CompactionSettings;
          readonly conversation: CompactionSettings;
          readonly maxTokens: number;
        }> = [
          { harness: {}, conversation: { reserveTokens: 500 }, maxTokens: 400 },
          {
            harness: { modelOverrides: { "faux/faux-1": { reserveTokens: 250 } } },
            conversation: { reserveTokens: 500 },
            maxTokens: 200,
          },
          {
            harness: { modelOverrides: { "faux/faux-1": { reserveTokens: 250 } } },
            conversation: {
              reserveTokens: 500,
              modelOverrides: { "faux/faux-1": { reserveTokens: 125 } },
            },
            maxTokens: 100,
          },
          {
            harness: {},
            conversation: {
              reserveTokens: 500,
              modelOverrides: { "faux/other": { reserveTokens: 125 } },
            },
            maxTokens: 400,
          },
        ];
        for (const { harness, conversation, maxTokens } of cases) {
          yield* run(backend)(
            Effect.gen(function* () {
              const chat = yield* open();
              yield* history(chat);
              chat.setup.settings.compaction = { ...MANUAL, ...harness };
              yield* chat.root.configure({ compaction: conversation });
              chat.faux.summaries.push(summary());
              assert.strictEqual(
                (yield* result(chat, yield* chat.root.compact())).status,
                "completed",
              );
              assert.strictEqual(chat.faux.summaryRequests[0]!.options.maxTokens, maxTokens);
              yield* chat.harness.close;
            }),
          );
        }
      }),
  );

  it.effect(
    "lets the conversation's enabled: false beat the harness's model override, and its own model override beat that",
    () =>
      Effect.gen(function* () {
        const harness: CompactionSettings = {
          ...BLOCKING,
          reserveTokens: 0,
          modelOverrides: { "faux/faux-1": { enabled: true, reserveTokens: 300 } },
        };
        const cases: ReadonlyArray<{
          readonly conversation: CompactionSettings | null;
          readonly summaries: number;
        }> = [
          { conversation: null, summaries: 1 },
          { conversation: { enabled: false }, summaries: 0 },
          {
            conversation: { enabled: false, modelOverrides: { "faux/faux-1": { enabled: true } } },
            summaries: 1,
          },
        ];
        for (const { conversation, summaries } of cases) {
          yield* run(backend)(
            Effect.gen(function* () {
              const chat = yield* open({ contextWindow: 1000 });
              yield* history(chat);
              chat.setup.settings.compaction = harness;
              yield* chat.root.configure({ compaction: conversation });
              chat.faux.summaries.push(summary());
              yield* turn(chat, text("u4", 200), "a4");
              assert.strictEqual(chat.faux.summaryRequests.length, summaries);
              yield* chat.harness.close;
            }),
          );
        }
      }),
  );

  it.effect("skips overflow compaction when the conversation turns compaction off", () =>
    run(backend)(
      Effect.gen(function* () {
        const chat = yield* open();
        yield* history(chat);
        chat.setup.settings.compaction = { ...MANUAL, enabled: true };
        yield* chat.root.configure({ compaction: { enabled: false } });
        chat.faux.agent.push(failure(OVERFLOW));
        const input = yield* chat.root.submit({ type: "input", content: text("u4", 100) });
        assert.strictEqual((yield* settle(input)).status, "unanswered");
        assert.strictEqual(chat.faux.summaryRequests.length, 0);
        yield* chat.harness.close;
      }),
    ),
  );

  // `configure` dies with a `RangeError` (a defect, `internal/agent.ts`).
  it.effect(
    "drops undefined fields, rejects token counts that are not integers 0 or more, and clears on null",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const chat = yield* open();
          const settings: CompactionSettings = {
            keepRecentTokens: 0,
            enabled: undefined,
            modelOverrides: { "faux/faux-1": { reserveTokens: 10 } },
          };
          yield* chat.root.configure({ compaction: settings });
          const stored = chat.harness.session
            .snapshot(AgentDoc, chat.root.id)
            .pipe(Effect.map((agent) => Option.getOrUndefined(agent)?.compaction));
          const expected = {
            keepRecentTokens: 0,
            modelOverrides: { "faux/faux-1": { reserveTokens: 10 } },
          };
          assert.deepStrictEqual(yield* stored, expected);
          assert.isFalse(Object.hasOwn((yield* stored)!, "enabled"));
          for (const bad of [
            { reserveTokens: -1 },
            { modelOverrides: { "faux/faux-1": { backgroundTokens: 1.5 } } },
          ]) {
            const exit = yield* Effect.exit(chat.root.configure({ compaction: bad }));
            assert.isTrue(Exit.isFailure(exit));
            if (Exit.isFailure(exit)) assert.instanceOf(Cause.squash(exit.cause), RangeError);
          }
          // A rejected change leaves the stored settings as they were.
          assert.deepStrictEqual(yield* stored, expected);
          yield* chat.root.configure({ compaction: null });
          assert.isUndefined(yield* stored);
          yield* chat.harness.close;
        }),
      ),
  );
});

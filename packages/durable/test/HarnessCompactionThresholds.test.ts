// Part of the compaction suite: background threshold, blocking threshold and overflow compaction, on every backend.
// See `HarnessCompaction.test.ts` for the shared setup. Here:
// - A section added late is a section installed at open that renders nothing until the case turns it on, so
//   preparation first has it to append at the turn that compacts.
// - A hook added late is a hook extension passed to `open`: no compaction runs before it is needed.
// - View histories are `Conversation.observe`, a synchronous observer.
import { assert, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import type { CompactionResult } from "../src/Compaction.ts";
import * as Extension from "../src/Extension.ts";
import type { Conversation } from "../src/Harness.ts";
import type { TaskId } from "../src/Ids.ts";
import type { LiveState } from "../src/Live.ts";
import type { SubmissionRecord } from "../src/Records.ts";
import * as Section from "../src/Section.ts";
import type { CompactionPolicy } from "../src/Settings.ts";
import type { ConversationView } from "../src/View.ts";
import { allEntries } from "./ChatSupport.ts";
import {
  answer,
  type Chat,
  compactionHooks,
  failure,
  finished,
  firstUserText,
  gated,
  history,
  kinds,
  live,
  MANUAL,
  OVERFLOW,
  open,
  placedBy,
  result,
  settle,
  submissionOf,
  summary,
  text,
  turn,
  userText,
} from "./CompactionSupport.ts";
import { describeEachBackend, run } from "./SessionSupport.ts";

// ─── Automatic compaction ─────────────────────────────────────────────────

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

const compactionTasks = (chat: Chat) =>
  chat.harness.harness.inspect.pipe(
    Effect.map((inspection) =>
      inspection.tasks
        .map((task) => task.record)
        .filter((record) => record.kind === "core.compaction"),
    ),
  );

/** A compaction task's ID, typed by its result. */
// SAFETY: the record is a `core.compaction` task's, whose result is a `CompactionResult`.
const compactionId = (id: TaskId): TaskId<CompactionResult> => id as TaskId<CompactionResult>;

const taskState = (chat: Chat, id: TaskId) =>
  chat.harness.harness
    .getTask(id)
    .pipe(Effect.map((record) => Option.getOrUndefined(record)?.state));

/** Every view of `conversationId` from now on, one per commit, unfolded. */
const viewHistory = (root: Conversation) =>
  Effect.gen(function* () {
    const views: Array<ConversationView> = [];
    const observation = yield* root.observe({
      advance: (change) => {
        views.push(change.value);
      },
      end: () => {},
    });
    views.push(observation.value);
    return {
      views,
      // SAFETY: the mount holds `core.live` as its definition materializes it.
      live: (): ReadonlyArray<LiveState> =>
        views.map((value) => (value.docs["core.live"] ?? {}) as LiveState),
      detach: () => observation.detach(),
    };
  });

/** The unanswered settlement of the `record` fields the checks read. */
const unansweredOf = (record: SubmissionRecord) =>
  record.status === "unanswered"
    ? { status: record.status, reason: record.reason, detail: record.detail }
    : { status: record.status };

describeEachBackend("background threshold compaction", (backend) => {
  it.effect(
    "starts above the background threshold without blocking the run; idle waits and Esc ignore it",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const chat = yield* open({ contextWindow: 2000 });
          yield* history(chat);
          chat.setup.settings.compaction = BACKGROUND;
          const gate = Deferred.makeUnsafe<void>();
          const reached = Deferred.makeUnsafe<void>();
          chat.faux.summaries.push(gated(gate, summary(), reached));
          yield* turn(chat, text("u4", 100), text("a4", 100));
          yield* finished(Deferred.await(reached));
          const [task] = yield* compactionTasks(chat);
          assert.strictEqual(task?.background, true);
          assert.deepStrictEqual(task?.input, { reason: "threshold" });
          assert.isUndefined(task!.owner);
          assert.deepStrictEqual((yield* live(chat)).compactions, [
            { taskId: task!.id, trigger: "threshold", blocking: false, attempt: 1 },
          ]);
          // Background work: neither conversation idle nor Esc waits for or stops it.
          yield* finished(chat.root.awaitIdle);
          yield* finished(chat.root.abort());
          assert.strictEqual((yield* taskState(chat, task!.id))?.status, "running");
          yield* Deferred.succeed(gate, undefined);
          const outcome = yield* result(chat, compactionId(task!.id));
          assert.strictEqual(outcome.status, "completed");
          assert.strictEqual((yield* kinds(chat.root)).at(-1), "core.compaction");
          assert.include(firstUserText((yield* chat.root.context()).messages), "SUMMARY");
          yield* chat.harness.close;
        }),
      ),
  );

  for (const [name, policy] of [
    ["disabled", { ...BACKGROUND, enabled: false }],
    ["backgroundTokens is 0", { ...BACKGROUND, backgroundTokens: 0 }],
    ["there is no cut", { ...BACKGROUND, keepRecentTokens: 100_000 }],
  ] as const) {
    it.effect(`does not start when ${name}`, () =>
      run(backend)(
        Effect.gen(function* () {
          const chat = yield* open({ contextWindow: 2000 });
          yield* history(chat);
          chat.setup.settings.compaction = policy;
          yield* turn(chat, text("u4", 100), text("a4", 100));
          assert.deepStrictEqual(yield* compactionTasks(chat), []);
          assert.strictEqual(chat.faux.summaryRequests.length, 0);
          yield* chat.harness.close;
        }),
      ),
    );
  }

  it.effect("does not start while another compaction is listed", () =>
    run(backend)(
      Effect.gen(function* () {
        const chat = yield* open({ contextWindow: 2000 });
        yield* history(chat);
        const reached = Deferred.makeUnsafe<void>();
        chat.faux.summaries.push(gated(Deferred.makeUnsafe<void>(), summary(), reached));
        const manual = yield* chat.root.compact();
        yield* finished(Deferred.await(reached));
        chat.setup.settings.compaction = BACKGROUND;
        yield* turn(chat, text("u4", 100), text("a4", 100));
        assert.deepStrictEqual(
          (yield* compactionTasks(chat)).map((task) => task.id),
          [manual],
        );
        yield* finished(chat.harness.harness.abortTask(manual));
        yield* chat.harness.close;
      }),
    ),
  );

  it.effect(
    "records a failure in core.live, in the commit that ends the compaction, until a summary is produced",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const chat = yield* open({ contextWindow: 2000 });
          yield* history(chat);
          chat.setup.settings.compaction = BACKGROUND;
          const views = yield* viewHistory(chat.root);
          chat.faux.summaries.push(failure("bad request"));
          yield* turn(chat, text("u4", 100), text("a4", 100));
          yield* finished(
            Effect.repeat(live(chat), {
              until: (state) => state.compactionFailure !== undefined,
            }),
          );
          const recorded = (yield* live(chat)).compactionFailure;
          assert.strictEqual(recorded?.trigger, "threshold");
          assert.strictEqual(recorded?.error, "Summarization failed: bad request");
          // The value that drops the compaction's status records its failure.
          const ended = (taskId: TaskId) => {
            const states = views.live();
            const listed = states.findIndex((state) =>
              state.compactions?.some((status) => status.taskId === taskId),
            );
            return states.find(
              (state, index) =>
                index > listed &&
                !(state.compactions?.some((status) => status.taskId === taskId) ?? false),
            )!;
          };
          assert.deepStrictEqual(ended(recorded!.taskId).compactionFailure, recorded);
          // A late watch starts with it.
          const late = yield* Effect.scoped(Effect.map(chat.root.watch(), (watch) => watch.value));
          // SAFETY: the mount holds `core.live` as its definition materializes it.
          const lateLive = late.docs["core.live"] as LiveState;
          assert.deepStrictEqual(lateLive.compactionFailure, recorded);
          // A compaction that produces a summary removes the failure in the commit that ends it.
          chat.setup.settings.compaction = MANUAL;
          chat.faux.summaries.push(summary());
          const manual = yield* chat.root.compact();
          assert.strictEqual((yield* result(chat, manual)).status, "completed");
          assert.isUndefined((yield* live(chat)).compactionFailure);
          assert.isUndefined(ended(manual).compactionFailure);
          views.detach();
          yield* chat.harness.close;
        }),
      ),
  );

  it.effect("stops through abortTask() and Conversation.abort() with background", () =>
    Effect.gen(function* () {
      for (const stop of ["task", "conversation"] as const) {
        yield* run(backend)(
          Effect.gen(function* () {
            const chat = yield* open({ contextWindow: 2000 });
            yield* history(chat);
            chat.setup.settings.compaction = BACKGROUND;
            const reached = Deferred.makeUnsafe<void>();
            chat.faux.summaries.push(gated(Deferred.makeUnsafe<void>(), summary(), reached));
            yield* turn(chat, text("u4", 100), text("a4", 100));
            yield* finished(Deferred.await(reached));
            const [task] = yield* compactionTasks(chat);
            if (stop === "task") yield* finished(chat.harness.harness.abortTask(task!.id));
            else yield* finished(chat.root.abort({ background: true }));
            assert.strictEqual((yield* result(chat, compactionId(task!.id))).status, "aborted");
            assert.isUndefined((yield* live(chat)).compactions);
            yield* chat.harness.close;
          }),
        );
      }
    }),
  );
});

/** An `extra` section that renders `EXTRA` once `on` is set, as if added after open. */
const extraSection = () => {
  const state = { on: false };
  const extension = Extension.make({
    name: "section:extra",
    sections: [Section.make("extra", () => Effect.sync(() => (state.on ? "EXTRA" : undefined)))],
  });
  return { state, extension };
};

const declining = () => compactionHooks({ beforeCompact: () => Effect.succeed({ decline: true }) });

describeEachBackend("blocking threshold compaction", (backend) => {
  it.effect("waits for its compaction, which appends the summary before the request", () =>
    run(backend)(
      Effect.gen(function* () {
        const extra = extraSection();
        const chat = yield* open({ contextWindow: 1000, extensions: [extra.extension] });
        yield* history(chat);
        chat.setup.settings.compaction = BLOCKING;
        // A new section: preparation has a system entry to append, but must not append it before the wait.
        extra.state.on = true;
        const gate = Deferred.makeUnsafe<void>();
        const reached = Deferred.makeUnsafe<void>();
        chat.faux.summaries.push(gated(gate, summary(), reached));
        chat.faux.agent.push(answer("a4"));
        const input = yield* chat.root.submit({ type: "input", content: text("u4", 200) });
        yield* finished(Deferred.await(reached));
        const [child] = yield* compactionTasks(chat);
        const generation = (yield* live(chat)).run!.taskId;
        assert.strictEqual(child?.owner, generation);
        assert.strictEqual(child?.background, false);
        assert.deepStrictEqual(child?.input, { reason: "threshold" });
        const state = yield* taskState(chat, generation);
        assert.strictEqual(state?.status, "waiting");
        if (state?.status === "waiting") {
          assert.deepStrictEqual(state.on, [child!.id]);
          assert.deepStrictEqual(state.checkpoint, {
            phase: "prepare",
            attempt: 1,
            compacted: child!.id,
            threshold: true,
          });
        }
        assert.deepStrictEqual((yield* live(chat)).compactions, [
          { taskId: child!.id, trigger: "threshold", blocking: true, attempt: 1 },
        ]);
        // Nothing was appended before the wait.
        assert.strictEqual((yield* kinds(chat.root)).at(-1), "core.user");
        yield* Deferred.succeed(gate, undefined);
        assert.strictEqual((yield* settle(input)).status, "done");
        assert.strictEqual((yield* result(chat, compactionId(child!.id))).status, "completed");
        assert.deepStrictEqual((yield* kinds(chat.root)).slice(-3), [
          "core.compaction",
          "core.system",
          "core.assistant",
        ]);
        const request = chat.faux.agentRequests.at(-1)!.messages;
        assert.include(firstUserText(request), "SUMMARY");
        const systems = request.filter((message) => message.role === "system");
        assert.strictEqual(systems.length, 1);
        assert.deepStrictEqual(systems[0]?.role === "system" ? systems[0].sections : undefined, {
          preamble: "You are helpful.",
          extra: "<extra>\nEXTRA\n</extra>",
        });
        yield* chat.harness.close;
      }),
    ),
  );

  it.effect(
    "sends the request once, without a second compaction, when the kept part is still above the threshold",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const chat = yield* open({ contextWindow: 1000 });
          yield* history(chat);
          chat.setup.settings.compaction = { ...BLOCKING, keepRecentTokens: 700 };
          chat.faux.summaries.push(summary());
          yield* turn(chat, text("u4", 400), "a4");
          assert.strictEqual(chat.faux.summaryRequests.length, 1);
          assert.strictEqual(
            (yield* kinds(chat.root)).filter((kind) => kind === "core.compaction").length,
            1,
          );
          yield* chat.harness.close;
        }),
      ),
  );

  it.effect("sends the request anyway when its compaction declines", () =>
    run(backend)(
      Effect.gen(function* () {
        const chat = yield* open({ contextWindow: 1000, extensions: [declining()] });
        yield* history(chat);
        chat.setup.settings.compaction = BLOCKING;
        yield* turn(chat, text("u4", 200), "a4");
        assert.isFalse((yield* kinds(chat.root)).includes("core.compaction"));
        assert.strictEqual(
          firstUserText(chat.faux.agentRequests.at(-1)!.messages),
          text("u1", 100),
        );
        assert.isUndefined((yield* live(chat)).compactionFailure);
        yield* chat.harness.close;
      }),
    ),
  );

  // A failed compaction leaves a context it judged too large; sending it once filled a 1M window and got an empty
  // `length` answer on every turn while the failure stayed invisible.
  it.effect(
    "fails the run with the compaction's error, without a request, when its compaction fails",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const chat = yield* open({ contextWindow: 1000 });
          yield* history(chat);
          chat.setup.settings.compaction = BLOCKING;
          chat.faux.summaries.push(failure("bad request"));
          const requests = chat.faux.agentRequests.length;
          const input = yield* chat.root.submit({ type: "input", content: text("u4", 200) });
          assert.deepStrictEqual(unansweredOf(yield* settle(input)), {
            status: "unanswered",
            reason: "model_error",
            detail: "Compaction failed: Summarization failed: bad request",
          });
          assert.strictEqual(chat.faux.agentRequests.length, requests);
          assert.strictEqual((yield* kinds(chat.root)).at(-1), "core.user");
          const state = yield* live(chat);
          assert.isUndefined(state.compactions);
          assert.strictEqual(state.compactionFailure?.trigger, "threshold");
          assert.strictEqual(state.compactionFailure?.error, "Summarization failed: bad request");
          yield* chat.harness.close;
        }),
      ),
  );

  it.effect("fails the run when its compaction is aborted directly", () =>
    run(backend)(
      Effect.gen(function* () {
        const chat = yield* open({ contextWindow: 1000 });
        yield* history(chat);
        chat.setup.settings.compaction = BLOCKING;
        const reached = Deferred.makeUnsafe<void>();
        chat.faux.summaries.push(gated(Deferred.makeUnsafe<void>(), summary(), reached));
        const requests = chat.faux.agentRequests.length;
        const input = yield* chat.root.submit({ type: "input", content: text("u4", 200) });
        yield* finished(Deferred.await(reached));
        const [child] = yield* compactionTasks(chat);
        yield* finished(chat.harness.harness.abortTask(child!.id));
        assert.deepStrictEqual(unansweredOf(yield* settle(input)), {
          status: "unanswered",
          reason: "model_error",
          detail: "Compaction was aborted",
        });
        assert.strictEqual(chat.faux.agentRequests.length, requests);
        assert.isFalse((yield* kinds(chat.root)).includes("core.compaction"));
        // Aborting is not a failure.
        assert.isUndefined((yield* live(chat)).compactionFailure);
        yield* chat.harness.close;
      }),
    ),
  );

  it.effect("is aborted with its generation by Esc, before the generation's abort handler", () =>
    run(backend)(
      Effect.gen(function* () {
        const chat = yield* open({ contextWindow: 1000 });
        yield* history(chat);
        chat.setup.settings.compaction = BLOCKING;
        const reached = Deferred.makeUnsafe<void>();
        chat.faux.summaries.push(gated(Deferred.makeUnsafe<void>(), summary(), reached));
        const views = yield* viewHistory(chat.root);
        const input = yield* chat.root.submit({ type: "input", content: text("u4", 200) });
        yield* finished(Deferred.await(reached));
        const [child] = yield* compactionTasks(chat);
        yield* finished(chat.root.abort());
        const settled = yield* settle(input);
        assert.strictEqual(settled.status, "unanswered");
        assert.strictEqual(settled.status === "unanswered" ? settled.reason : undefined, "aborted");
        const state = yield* taskState(chat, child!.id);
        assert.strictEqual(
          state?.status === "terminal" ? state.outcome.status : undefined,
          "aborted",
        );
        views.detach();
        // The compaction's status leaves before the run ends.
        const states = views.live();
        const busy = states.findIndex((value) => value.compactions !== undefined);
        const compactionEnd = states.findIndex(
          (value, index) => index > busy && value.compactions === undefined,
        );
        const runEnd = states.findIndex((value, index) => index > busy && value.run === undefined);
        assert.isAbove(busy, 0);
        assert.isBelow(compactionEnd, runEnd);
        assert.deepStrictEqual(states[runEnd]!.runFailure?.inputs, [input.id]);
        assert.strictEqual(states[runEnd]!.runFailure?.reason, "aborted");
        yield* chat.harness.close;
      }),
    ),
  );

  it.effect("wins over a background compaction still in flight, which then settles stale", () =>
    run(backend)(
      Effect.gen(function* () {
        const chat = yield* open({ contextWindow: 2000 });
        yield* history(chat);
        chat.setup.settings.compaction = BACKGROUND;
        const gate = Deferred.makeUnsafe<void>();
        const reached = Deferred.makeUnsafe<void>();
        chat.faux.summaries.push(gated(gate, summary("BACKGROUND"), reached));
        yield* turn(chat, text("u4", 100), text("a4", 100));
        yield* finished(Deferred.await(reached));
        const [background] = yield* compactionTasks(chat);
        chat.faux.summaries.push(summary("BLOCKING"));
        yield* turn(chat, text("u5", 1000), "a5");
        assert.include(firstUserText((yield* chat.root.context()).messages), "BLOCKING");
        yield* Deferred.succeed(gate, undefined);
        const outcome = yield* result(chat, compactionId(background!.id));
        const submission = yield* submissionOf(chat, placedBy(outcome));
        const status = yield* submission.status;
        assert.strictEqual(status.status, "unanswered");
        assert.strictEqual(status.status === "unanswered" ? status.reason : undefined, "stale");
        yield* chat.harness.close;
      }),
    ),
  );
});

describeEachBackend("overflow compaction", (backend) => {
  const ENABLED: CompactionPolicy = { ...MANUAL, enabled: true };

  it.effect("compacts and retries with the same attempt, leaving the error out of the retry", () =>
    run(backend)(
      Effect.gen(function* () {
        const chat = yield* open();
        yield* history(chat);
        chat.setup.settings.compaction = ENABLED;
        chat.faux.summaries.push(summary());
        let attempt: number | undefined;
        chat.faux.agent.push(failure(OVERFLOW));
        chat.faux.agent.push(() =>
          live(chat).pipe(
            Effect.map((state) => {
              attempt = state.generation?.attempt;
              return answer("fits");
            }),
            Effect.orDie,
          ),
        );
        const input = yield* chat.root.submit({ type: "input", content: text("u4", 100) });
        assert.strictEqual((yield* settle(input)).status, "done");
        assert.strictEqual(attempt, 1);
        assert.deepStrictEqual((yield* kinds(chat.root)).slice(-5), [
          "core.user",
          "core.assistant",
          "core.compaction",
          "core.system",
          "core.assistant",
        ]);
        const compaction = (yield* allEntries(chat.root)).find(
          (record) => record.kind === "core.compaction",
        );
        assert.deepStrictEqual(compaction?.data, { reason: "overflow" });
        const retry = chat.faux.agentRequests.at(-1)!.messages;
        assert.strictEqual(retry[0]?.role, "system");
        assert.include(userText(retry[1]), "SUMMARY");
        assert.isFalse(
          retry.some((message) => message.role === "assistant" && message.stopReason === "error"),
        );
        yield* chat.harness.close;
      }),
    ),
  );

  it.effect("fails a second overflow with its error entry", () =>
    run(backend)(
      Effect.gen(function* () {
        const chat = yield* open();
        yield* history(chat);
        chat.setup.settings.compaction = ENABLED;
        chat.faux.summaries.push(summary());
        chat.faux.agent.push(failure(OVERFLOW), failure(OVERFLOW));
        const input = yield* chat.root.submit({ type: "input", content: text("u4", 100) });
        assert.deepStrictEqual(unansweredOf(yield* settle(input)), {
          status: "unanswered",
          reason: "model_error",
          detail: OVERFLOW,
        });
        assert.strictEqual(
          (yield* kinds(chat.root)).filter((kind) => kind === "core.compaction").length,
          1,
        );
        assert.strictEqual((yield* kinds(chat.root)).at(-1), "core.assistant");
        yield* chat.harness.close;
      }),
    ),
  );

  it.effect(
    "fails without compacting when compaction is disabled, even for a retryable-looking overflow",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const chat = yield* open();
          yield* history(chat);
          chat.faux.agent.push(failure(`overloaded: ${OVERFLOW}`));
          const input = yield* chat.root.submit({ type: "input", content: text("u4", 100) });
          const settled = yield* settle(input);
          assert.strictEqual(settled.status, "unanswered");
          assert.strictEqual(
            settled.status === "unanswered" ? settled.reason : undefined,
            "model_error",
          );
          assert.strictEqual(chat.faux.agentRequests.length, 4);
          assert.strictEqual(chat.faux.summaryRequests.length, 0);
          yield* chat.harness.close;
        }),
      ),
  );

  it.effect("fails after a blocking threshold compaction in the same generation", () =>
    run(backend)(
      Effect.gen(function* () {
        const chat = yield* open({ contextWindow: 1000 });
        yield* history(chat);
        chat.setup.settings.compaction = BLOCKING;
        chat.faux.summaries.push(summary());
        chat.faux.agent.push(failure(OVERFLOW));
        const input = yield* chat.root.submit({ type: "input", content: text("u4", 200) });
        const settled = yield* settle(input);
        assert.strictEqual(settled.status, "unanswered");
        assert.strictEqual(
          settled.status === "unanswered" ? settled.reason : undefined,
          "model_error",
        );
        assert.strictEqual(chat.faux.summaryRequests.length, 1);
        yield* chat.harness.close;
      }),
    ),
  );

  // The declining hook is an extension of `open`.
  for (const [name, extensions, prepare, requests] of [
    ["declines", () => [declining()], () => {}, 0],
    [
      "fails",
      () => [],
      (chat: Chat) => {
        chat.faux.summaries.push(failure("bad request"));
      },
      1,
    ],
    // Classification finds no cut, so no compaction starts and the ordinary failure carries the text.
    [
      "cannot cut",
      () => [],
      (chat: Chat) => {
        chat.setup.settings.compaction = { ...ENABLED, keepRecentTokens: 100_000 };
      },
      0,
    ],
  ] as const) {
    it.effect(`fails with the overflow text when compaction ${name}`, () =>
      run(backend)(
        Effect.gen(function* () {
          const chat = yield* open({ extensions: extensions() });
          yield* turn(chat, text("u1", 100), text("a1", 100));
          chat.setup.settings.compaction = ENABLED;
          prepare(chat);
          chat.faux.agent.push(failure(OVERFLOW));
          const input = yield* chat.root.submit({ type: "input", content: text("u4", 100) });
          assert.deepStrictEqual(unansweredOf(yield* settle(input)), {
            status: "unanswered",
            reason: "model_error",
            detail: OVERFLOW,
          });
          assert.strictEqual(chat.faux.summaryRequests.length, requests);
          yield* chat.harness.close;
        }),
      ),
    );
  }
});

// Compaction, on every backend: manual, threshold and overflow compaction, `beforeCompact`, `settings.compaction`
// with `modelOverrides`, the summary entry and head marker, `core.live.compactions` and `compactionFailure`,
// recovery, and the `core.live` migrations.
//
// The suite is split so the parts run in parallel: this file holds range selection, serialization, manual
// compaction and outcomes; `HarnessCompactionThresholds`, `HarnessCompactionRecovery` and `HarnessCompactionEdges`
// the rest, and `LiveMigrations` the `core.live migrations` cases. The helpers are `CompactionSupport`.
//
// - Hooks, tools, tasks and sections are extensions passed to `open`; `toolsNamed` is the tool tokens themselves.
// - The scripted faux model answers from two queues; a step is a message or a function of the request returning
//   an Effect. The stream options are spread over `GenerateRequest` (`options`, `maxTokens`, `sessionId`,
//   `reasoning`), so `optionsOf` puts them back together for assertions on a request's options.
// - A gated step waits for a `Deferred`; an aborted request is the stream's interruption. Waits run while the
//   `TestClock` advances (`ChatSupport.waitFor`): every wait for a submission, a task or idleness goes through
//   `finished`.
// - A reopen is a new Harness over the backend's storage; a crash is `HarnessSupport.killable`.
import { assert, describe, it } from "@effect/vitest";
import * as Cause from "effect/Cause";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as TestClock from "effect/testing/TestClock";
import type { ContextView } from "../src/ContextView.ts";
import type { CompactionDecision, CompactionHooks } from "../src/Hook.ts";
import { ConversationId, EntryId } from "../src/Ids.ts";
import { selectCut, serializeConversation } from "../src/internal/compaction.ts";
import { orderToolResults } from "../src/internal/context.ts";
import type { AssistantMessage, Message, ToolResultMessage, UserMessage } from "../src/Message.ts";
import { ProviderDoc } from "../src/ProviderSession.ts";
import type { EntryRecord } from "../src/Records.ts";
import { fauxAssistantMessage, fauxText, fauxToolCall } from "../src/testing/FauxModel.ts";
import { Tx } from "../src/Tx.ts";
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
  kinds,
  live,
  MANUAL,
  open,
  placedBy,
  result,
  script,
  sessionIdOf,
  settle,
  submissionOf,
  summary,
  text,
  toolExtension,
  toolNamed,
  turn,
  usageOf,
  userText,
} from "./CompactionSupport.ts";
import { controlledStorage, describeEachBackend, flush, run } from "./SessionSupport.ts";

// ─── Range selection ──────────────────────────────────────────────────────

let nextId = 1;
const entry = (kind: string, model: ReadonlyArray<Message>, head?: number): EntryRecord => {
  const record: EntryRecord = {
    id: EntryId.make(nextId++),
    conversationId: ConversationId.make(1),
    kind,
    model,
  };
  return head === undefined ? record : { ...record, head: EntryId.make(head) };
};
const user = (content: string): UserMessage => ({ role: "user", content, timestamp: 0 });
const assistant = (
  content: string,
  calls: ReadonlyArray<string> = [],
  stopReason: AssistantMessage["stopReason"] = "stop",
): AssistantMessage => ({
  ...fauxAssistantMessage([
    fauxText(content),
    ...calls.map((id) => fauxToolCall("read", {}, { id })),
  ]),
  stopReason,
});
const toolResult = (callId: string, content: string): ToolResultMessage => ({
  role: "toolResult",
  toolCallId: callId,
  toolName: "read",
  content: [{ type: "text", text: content }],
  isError: false,
  timestamp: 0,
});

/** A view over `entries` whose contributions are their models, with excluded assistants removed. */
const view = (entries: ReadonlyArray<EntryRecord>, head?: EntryRecord): ContextView => {
  const all = head === undefined ? entries : [head, ...entries];
  const contributions = all.map((record) =>
    (record.model ?? []).filter(
      (message) =>
        message.role !== "assistant" ||
        !["error", "aborted", "deferred"].includes(message.stopReason),
    ),
  );
  return { head, entries: all, contributions, messages: orderToolResults(contributions.flat()) };
};

describe("range selection", () => {
  it("keeps about keepRecentTokens and cuts at the first candidate at or after the budget (the documented example)", () => {
    const entries = [
      entry("core.user", [user(text("1", 10))]),
      entry("core.assistant", [assistant("2", ["c1"])]),
      entry("core.tool-result", [toolResult("c1", text("3", 3000))]),
      entry("core.assistant", [assistant(text("4", 10))]),
      entry("core.user", [user(text("5", 10))]),
      entry("core.assistant", [assistant(text("6", 10))]),
    ];
    assert.strictEqual(selectCut(view(entries), 2000), 3);
  });

  it("cuts at a user entry", () => {
    const entries = [
      entry("core.user", [user(text("u1", 100))]),
      entry("core.assistant", [assistant(text("a1", 100))]),
      entry("core.user", [user(text("u2", 100))]),
      entry("core.assistant", [assistant(text("a2", 100))]),
    ];
    assert.strictEqual(selectCut(view(entries), 150), 2);
  });

  it("cuts at an assistant in the middle of one long run and never at a tool result", () => {
    const entries = [entry("core.user", [user("do it")])];
    for (let index = 0; index < 5; index++) {
      entries.push(entry("core.assistant", [assistant(`step ${index}`, [`c${index}`])]));
      entries.push(entry("core.tool-result", [toolResult(`c${index}`, text(`r${index}`, 100))]));
    }
    const cut = selectCut(view(entries), 150)!;
    // The budget is reached at the fourth result; the cut is the last call, whose result it keeps.
    assert.strictEqual(entries[cut]!.kind, "core.assistant");
    assert.strictEqual(cut, 9);
  });

  it("never cuts at a display-only entry where the budget is reached", () => {
    const entries = [
      entry("core.user", [user(text("u1", 100))]),
      entry("core.assistant", [assistant(text("a1", 100))]),
      entry("chat.message", []),
      entry("core.assistant", [assistant(text("a2", 100))]),
    ];
    // The budget is reached at a1; the display-only entry after it is skipped for the assistant a2.
    assert.strictEqual(selectCut(view(entries), 150), 1);
    assert.strictEqual(selectCut(view(entries), 50), 3);
  });

  it("keeps a huge last tool result together with its assistant", () => {
    const entries = [
      entry("core.user", [user("u")]),
      entry("core.assistant", [assistant("a", ["c"])]),
      entry("core.tool-result", [toolResult("c", text("big", 5000))]),
    ];
    assert.strictEqual(selectCut(view(entries), 100), 1);
  });

  it("never cuts at a system entry or an excluded error or aborted answer", () => {
    const entries = [
      entry("core.user", [user(text("u1", 100))]),
      entry("core.assistant", [assistant(text("a1", 100))]),
      entry("core.system", [
        { role: "system", content: "", sections: { s: text("s", 100) }, timestamp: 0 },
      ]),
      entry("core.assistant", [assistant(text("err", 100), [], "error")]),
      entry("core.assistant", [assistant(text("stopped", 100), [], "aborted")]),
      entry("core.assistant", [assistant(text("a2", 100))]),
    ];
    // The walk reaches 150 at the system entry; the excluded answers after it contribute nothing.
    assert.strictEqual(selectCut(view(entries), 150), 5);
  });

  it("follows edited contributions: an omitted entry adds nothing and is no candidate", () => {
    const entries = [
      entry("core.user", [user(text("u1", 100))]),
      entry("core.assistant", [assistant(text("a1", 100))]),
      entry("core.user", [user(text("u2", 100))]),
      entry("core.assistant", [assistant(text("a2", 100))]),
    ];
    const plain = view(entries);
    assert.strictEqual(selectCut(plain, 150), 2);
    const contributions = plain.contributions.map((messages, index) =>
      index === 2 ? [] : messages,
    );
    const omitted = { ...plain, contributions, messages: orderToolResults(contributions.flat()) };
    assert.strictEqual(selectCut(omitted, 150), 1);
  });

  it("does not cut at a user entry that a result of the preceding call still follows", () => {
    const entries = [
      entry("core.user", [user(text("u1", 100))]),
      entry("core.assistant", [assistant("a", ["c"])]),
      entry("core.user", [user(text("steer", 100))]),
      entry("core.tool-result", [toolResult("c", text("r", 100))]),
      entry("core.assistant", [assistant(text("a2", 100))]),
    ];
    // The budget is reached at the steer; its result follows it, so the cut moves to the next assistant.
    assert.strictEqual(selectCut(view(entries), 250), 4);
  });

  it("finds nothing when the budget is never reached or only the marker precedes the cut", () => {
    const small = [entry("core.user", [user("hi")]), entry("core.assistant", [assistant("hello")])];
    assert.isUndefined(selectCut(view(small), 150));
    const marker = entry("core.compaction", [user("summary")], 0);
    // The budget is reached at the only entry after the marker, so the marker alone would be summarized.
    assert.isUndefined(selectCut(view([entry("core.user", [user(text("u", 200))])], marker), 150));
  });

  it("summarizes an earlier summary marker first", () => {
    const marker = entry("core.compaction", [user("EARLIER")], 0);
    const kept = [
      entry("core.user", [user(text("u1", 100))]),
      entry("core.assistant", [assistant(text("a1", 100))]),
      entry("core.user", [user(text("u2", 100))]),
      entry("core.assistant", [assistant(text("a2", 100))]),
    ];
    const selected = view(kept, marker);
    assert.strictEqual(selectCut(selected, 150), 3);
    assert.match(
      serializeConversation(selected.contributions.slice(0, 3).flat()),
      /^\[User\]: EARLIER/,
    );
  });
});

describe("serialization", () => {
  it("writes a transcript, truncates tool results, and omits system messages and thinking", () => {
    const call = fauxToolCall("read", { path: "a.ts" }, { id: "c" });
    const messages: ReadonlyArray<Message> = [
      { role: "system", content: "", sections: { s: "hidden" }, timestamp: 0 },
      user("hello"),
      fauxAssistantMessage([{ type: "thinking", thinking: "hmm" }, fauxText("sure"), call]),
      toolResult("c", "y".repeat(2500)),
    ];
    const serialized = serializeConversation(messages);
    assert.notInclude(serialized, "hidden");
    assert.include(serialized, "[User]: hello");
    assert.notInclude(serialized, "hmm");
    assert.notInclude(serialized, "[Assistant thinking]");
    assert.include(serialized, "[Assistant]: sure");
    assert.include(serialized, '[Assistant tool calls]: read(path="a.ts")');
    assert.include(
      serialized,
      `[Tool result]: ${"y".repeat(2000)}\n\n[... 500 more characters truncated]`,
    );
  });
});

const deferred = () => Deferred.makeUnsafe<void>();
const resolve = (gate: Deferred.Deferred<void>) => Deferred.succeed(gate, undefined);

/** The status of submission `id`. */
const statusOf = (chat: Chat, id: Parameters<typeof submissionOf>[1]) =>
  Effect.flatMap(submissionOf(chat, id), (submission) => submission.status);

/** Input tokens a compaction added to the ledger. */
const compactionInput = <E, R>(chat: Chat, body: Effect.Effect<unknown, E, R>) =>
  Effect.gen(function* () {
    const before = (yield* usageOf(chat)).input;
    yield* body;
    return (yield* usageOf(chat)).input - before;
  });

// ─── Manual compaction ────────────────────────────────────────────────────

describeEachBackend("manual compaction", (backend) => {
  it.effect("places the summary at once when idle and keeps raw history", () =>
    run(backend)(
      Effect.gen(function* () {
        const chat = yield* open();
        yield* history(chat);
        const before = yield* allEntries(chat.root);
        const usageBefore = yield* usageOf(chat);
        chat.faux.summaries.push(summary());

        const id = yield* chat.root.compact("focus on files");
        const outcome = yield* result(chat, id);
        assert.strictEqual(outcome.status, "completed");
        const placed = yield* settle(yield* submissionOf(chat, placedBy(outcome)));
        assert.strictEqual(placed.status, "done");

        // Raw history is unchanged; one summary entry heads the first kept entry, u3.
        const after = yield* allEntries(chat.root);
        assert.deepStrictEqual(after.slice(0, before.length), before);
        const marker = after.at(-1)!;
        const u3 = before.find((record) => userText(record.model?.[0]).startsWith("u3"))!;
        assert.strictEqual(marker.kind, "core.compaction");
        assert.strictEqual(marker.head, u3.id);
        assert.deepStrictEqual(marker.data, { reason: "manual" });
        assert.strictEqual(
          placed.status === "done" && "entry" in placed && placed.entry,
          marker.id,
        );
        assert.strictEqual(
          userText(marker.model?.[0]),
          "The conversation history before this point was compacted into the following summary:\n\n<summary>\nSUMMARY\n</summary>",
        );

        // The model context is the summary followed by the kept entries.
        const context = yield* chat.root.context();
        assert.deepStrictEqual(context.messages.map(userText), [
          userText(marker.model?.[0]),
          text("u3", 100),
          text("a3", 100),
        ]);

        // The summarizer saw the serialized prefix, the prompt, and the instructions, without tools or caching.
        const request = chat.faux.summaryRequests[0]!;
        assert.strictEqual(request.messages.length, 2);
        const prompt = userText(request.messages[1]);
        assert.match(prompt, /^<conversation>\n\[User\]: u1 /);
        assert.include(prompt, "[Assistant]: a2 ");
        assert.notInclude(prompt, "u3 ");
        assert.include(prompt, "## Goal");
        assert.isTrue(prompt.endsWith("\n\nAdditional focus: focus on files"));
        assert.strictEqual(request.options.cacheRetention, "none");
        assert.strictEqual(request.options.maxTokens, 800);
        assert.strictEqual(request.options.sessionId, yield* sessionIdOf(chat));

        // The summarizer's spend is in the ledger, and nothing counts it again later.
        const usage = yield* usageOf(chat);
        assert.isAbove(usage.input, usageBefore.input);
        yield* turn(chat, "next", "done");
        const agent = chat.faux.agentRequests.at(-1)!.messages;
        // The next request: one complete system baseline leading the summary, the kept turn, and the new input.
        assert.deepStrictEqual(
          agent.map((message) => message.role),
          ["system", "user", "user", "assistant", "user"],
        );
        const baseline = agent[0];
        assert.deepStrictEqual(baseline?.role === "system" ? baseline.sections : undefined, {
          preamble: "You are helpful.",
        });
        yield* chat.harness.close;
      }),
    ),
  );

  it.effect("creates provider state before a legacy conversation's summarization request", () =>
    run(backend)(
      Effect.gen(function* () {
        const chat = yield* open();
        yield* history(chat);
        yield* chat.root.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            yield* tx.retireDoc(ProviderDoc, chat.root.id);
          }),
        );
        assert.isTrue(
          Option.isNone(yield* chat.harness.session.snapshot(ProviderDoc, chat.root.id)),
        );
        chat.faux.summaries.push(summary());
        assert.strictEqual((yield* result(chat, yield* chat.root.compact())).status, "completed");
        const stored = yield* sessionIdOf(chat);
        assert.match(
          stored,
          /^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/u,
        );
        assert.strictEqual(chat.faux.summaryRequests[0]!.options.sessionId, stored);
        yield* chat.harness.close;
      }),
    ),
  );

  it.effect("keeps working while busy and places the summary at the next final boundary", () =>
    run(backend)(
      Effect.gen(function* () {
        const chat = yield* open();
        yield* history(chat);
        const gate = deferred();
        const reached = deferred();
        chat.faux.agent.push(gated(gate, answer("late answer"), reached));
        const input = yield* chat.root.submit({ type: "input", content: "busy" });
        yield* finished(Deferred.await(reached));
        chat.faux.summaries.push(summary());
        const outcome = yield* result(chat, yield* chat.root.compact());
        const submissionId = placedBy(outcome);
        assert.strictEqual((yield* statusOf(chat, submissionId)).status, "queued");
        yield* resolve(gate);
        assert.strictEqual((yield* settle(input)).status, "done");
        const placed = yield* settle(yield* submissionOf(chat, submissionId));
        assert.strictEqual(placed.status, "done");
        // The summary follows the answer; the kept range still includes the busy turn.
        assert.deepStrictEqual((yield* kinds(chat.root)).slice(-3), [
          "core.user",
          "core.assistant",
          "core.compaction",
        ]);
        yield* chat.harness.close;
      }),
    ),
  );

  it.effect(
    "places a queued summary at postTools and the run continues in the compacted context",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const wait = toolNamed("wait", "waited");
          const chat = yield* open({ extensions: [toolExtension(wait)] });
          yield* chat.root.configure({ tools: [wait] });
          yield* history(chat);
          const gate = deferred();
          const reached = deferred();
          chat.faux.agent.push(
            gated(
              gate,
              fauxAssistantMessage([fauxToolCall("wait", {})], { stopReason: "toolUse" }),
              reached,
            ),
          );
          chat.faux.agent.push(answer("after tools"));
          const input = yield* chat.root.submit({ type: "input", content: "use a tool" });
          yield* finished(Deferred.await(reached));
          chat.faux.summaries.push(summary());
          yield* result(chat, yield* chat.root.compact());
          yield* resolve(gate);
          assert.strictEqual((yield* settle(input)).status, "done");
          // The continuation request starts with the new baseline, then the summary, ahead of the kept tool call.
          const continuation = chat.faux.agentRequests.at(-1)!.messages;
          assert.strictEqual(continuation[0]?.role, "system");
          assert.include(userText(continuation[1]), "<summary>\nSUMMARY\n</summary>");
          assert.deepStrictEqual((yield* kinds(chat.root)).slice(-4), [
            "core.tool-result",
            "core.compaction",
            "core.system",
            "core.assistant",
          ]);
          yield* chat.harness.close;
        }),
      ),
  );

  it.effect("runs follow-ups left by a failed run after placing the summary", () =>
    run(backend)(
      Effect.gen(function* () {
        const chat = yield* open();
        yield* history(chat);
        const gate = deferred();
        const reached = deferred();
        chat.faux.agent.push(gated(gate, failure("bad request"), reached));
        const failed = yield* chat.root.submit({ type: "input", content: "fails" });
        yield* finished(Deferred.await(reached));
        const followUp = yield* chat.root.submit({ type: "input", content: "follow-up" });
        yield* resolve(gate);
        assert.strictEqual((yield* settle(failed)).status, "unanswered");
        assert.strictEqual((yield* followUp.status).status, "queued");

        chat.faux.summaries.push(summary());
        chat.faux.agent.push(answer("followed"));
        yield* result(chat, yield* chat.root.compact());
        assert.strictEqual((yield* settle(followUp)).status, "done");
        const request = chat.faux.agentRequests.at(-1)!.messages;
        assert.include(firstUserText(request), "SUMMARY");
        assert.isTrue(request.some((message) => userText(message) === "follow-up"));
        yield* chat.harness.close;
      }),
    ),
  );

  it.effect("settles stale when a reset lands while it summarizes", () =>
    run(backend)(
      Effect.gen(function* () {
        const chat = yield* open();
        yield* history(chat);
        const gate = deferred();
        const reached = deferred();
        chat.faux.summaries.push(gated(gate, summary(), reached));
        const id = yield* chat.root.compact();
        yield* finished(Deferred.await(reached));
        yield* chat.root.reset();
        yield* resolve(gate);
        const outcome = yield* result(chat, id);
        const status = yield* statusOf(chat, placedBy(outcome));
        assert.strictEqual(status.status, "unanswered");
        assert.strictEqual(status.status === "unanswered" ? status.reason : undefined, "stale");
        assert.strictEqual((yield* kinds(chat.root)).at(-1), "core.reset");
        yield* chat.harness.close;
      }),
    ),
  );

  it.effect(
    "does not make the conversation busy: a submission during summarization starts its run at once",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const chat = yield* open();
          yield* history(chat);
          const summaryGate = deferred();
          const summaryReached = deferred();
          chat.faux.summaries.push(gated(summaryGate, summary(), summaryReached));
          const id = yield* chat.root.compact();
          yield* finished(Deferred.await(summaryReached));
          const answerGate = deferred();
          const answerReached = deferred();
          chat.faux.agent.push(gated(answerGate, answer("a4"), answerReached));
          const input = yield* chat.root.submit({ type: "input", content: "u4" });
          // Placed and answered immediately with the uncompacted context, not queued behind the compaction.
          assert.strictEqual((yield* input.status).status, "placed");
          yield* finished(Deferred.await(answerReached));
          assert.strictEqual(
            firstUserText(chat.faux.agentRequests.at(-1)!.messages),
            text("u1", 100),
          );
          // The summary is ready while the run is busy, so it queues and lands after the answer.
          yield* resolve(summaryGate);
          const outcome = yield* result(chat, id);
          const submission = yield* submissionOf(chat, placedBy(outcome));
          assert.strictEqual((yield* submission.status).status, "queued");
          yield* resolve(answerGate);
          assert.strictEqual((yield* settle(input)).status, "done");
          assert.strictEqual((yield* settle(submission)).status, "done");
          assert.deepStrictEqual((yield* kinds(chat.root)).slice(-3), [
            "core.user",
            "core.assistant",
            "core.compaction",
          ]);
          yield* chat.harness.close;
        }),
      ),
  );

  it.effect("counts the spend of a summary that ends stale, and writes no entry for it", () =>
    run(backend)(
      Effect.gen(function* () {
        const chat = yield* open();
        yield* history(chat);
        const before = (yield* usageOf(chat)).input;
        const gate = deferred();
        const reached = deferred();
        chat.faux.summaries.push(gated(gate, summary(), reached));
        const id = yield* chat.root.compact();
        yield* finished(Deferred.await(reached));
        yield* chat.root.reset();
        yield* resolve(gate);
        const outcome = yield* result(chat, id);
        const status = yield* statusOf(chat, placedBy(outcome));
        assert.strictEqual(status.status === "unanswered" ? status.reason : undefined, "stale");
        assert.isAbove((yield* usageOf(chat)).input, before);
        assert.isFalse((yield* kinds(chat.root)).includes("core.compaction"));
        yield* chat.harness.close;
      }),
    ),
  );

  it.effect("lets the compaction that cuts furthest win, whatever finishes first", () =>
    run(backend)(
      Effect.gen(function* () {
        const chat = yield* open();
        yield* history(chat);
        const first = deferred();
        const firstReached = deferred();
        chat.faux.summaries.push(gated(first, summary("FIRST"), firstReached));
        const early = yield* chat.root.compact();
        yield* finished(Deferred.await(firstReached));
        yield* turn(chat, text("u4", 100), text("a4", 100));
        chat.faux.summaries.push(summary("SECOND"));
        const later = yield* result(chat, yield* chat.root.compact());
        assert.strictEqual(later.status, "completed");
        yield* resolve(first);
        const outcome = yield* result(chat, early);
        // The early compaction cut at u3, before the later cut at u4.
        const status = yield* statusOf(chat, placedBy(outcome));
        assert.strictEqual(status.status, "unanswered");
        assert.strictEqual(status.status === "unanswered" ? status.reason : undefined, "stale");
        assert.include(firstUserText((yield* chat.root.context()).messages), "SECOND");
        yield* chat.harness.close;
      }),
    ),
  );

  it.effect("places an older-selected summary that cuts later than the newer one", () =>
    run(backend)(
      Effect.gen(function* () {
        const chat = yield* open();
        yield* history(chat);
        // A: small budget, late cut; selected first, finishes last.
        const gate = deferred();
        const reached = deferred();
        chat.faux.summaries.push(gated(gate, summary("A"), reached));
        const a = yield* chat.root.compact();
        yield* finished(Deferred.await(reached));
        // B: larger budget, earlier cut; placed first.
        chat.setup.settings.compaction = { ...MANUAL, keepRecentTokens: 350 };
        chat.faux.summaries.push(summary("B"));
        yield* result(chat, yield* chat.root.compact());
        assert.include(firstUserText((yield* chat.root.context()).messages), "B");
        yield* resolve(gate);
        const outcome = yield* result(chat, a);
        assert.strictEqual((yield* statusOf(chat, placedBy(outcome))).status, "done");
        const messages = (yield* chat.root.context()).messages;
        assert.include(firstUserText(messages), "<summary>\nA\n</summary>");
        assert.deepStrictEqual(messages.slice(1).map(userText), [text("u3", 100), text("a3", 100)]);
        yield* chat.harness.close;
      }),
    ),
  );

  for (const [order, keeps, stale] of [
    ["before", [150, 350], true],
    ["at", [150, 150], false],
    ["after", [350, 150], false],
  ] as const) {
    it.effect(
      `places two queued summaries in one boundary when the second cuts ${order} the first`,
      () =>
        run(backend)(
          Effect.gen(function* () {
            const chat = yield* open();
            yield* history(chat);
            const gate = deferred();
            const reached = deferred();
            chat.faux.agent.push(gated(gate, answer("done"), reached));
            const input = yield* chat.root.submit({ type: "input", content: "busy" });
            yield* finished(Deferred.await(reached));
            const submissions = [];
            for (const [index, keep] of keeps.entries()) {
              chat.setup.settings.compaction = { ...MANUAL, keepRecentTokens: keep };
              chat.faux.summaries.push(summary(`S${index}`));
              const outcome = yield* result(chat, yield* chat.root.compact());
              submissions.push(placedBy(outcome));
            }
            yield* resolve(gate);
            yield* settle(input);
            const statuses = [];
            for (const id of submissions) {
              statuses.push((yield* settle(yield* submissionOf(chat, id))).status);
            }
            assert.deepStrictEqual(statuses, ["done", stale ? "unanswered" : "done"]);
            yield* chat.harness.close;
          }),
        ),
    );
  }

  it.effect("is aborted by Conversation.abort(); an already queued summary survives it", () =>
    run(backend)(
      Effect.gen(function* () {
        const chat = yield* open();
        yield* history(chat);
        const reached = deferred();
        chat.faux.summaries.push(gated(deferred(), summary(), reached));
        const id = yield* chat.root.compact();
        yield* finished(Deferred.await(reached));
        assert.strictEqual((yield* live(chat)).compactions?.length, 1);
        yield* finished(chat.root.abort());
        assert.strictEqual((yield* result(chat, id)).status, "aborted");
        assert.isUndefined((yield* live(chat)).compactions);
        assert.isFalse((yield* kinds(chat.root)).includes("core.compaction"));

        // Queued while busy, then Esc: the queued write stays and lands with the next run's boundary.
        const gate = deferred();
        const busy = deferred();
        chat.faux.agent.push(gated(gate, answer("never"), busy));
        yield* chat.root.submit({ type: "input", content: "busy" });
        yield* finished(Deferred.await(busy));
        chat.faux.summaries.push(summary());
        const queued = yield* result(chat, yield* chat.root.compact());
        yield* finished(chat.root.abort());
        const submission = yield* submissionOf(chat, placedBy(queued));
        assert.strictEqual((yield* submission.status).status, "queued");
        assert.strictEqual((yield* submission.withdraw)._tag, "Withdrawn");
        yield* turn(chat, "next", "ok");
        const status = yield* submission.status;
        assert.strictEqual(status.status, "unanswered");
        assert.strictEqual(status.status === "unanswered" ? status.reason : undefined, "aborted");
        assert.isFalse((yield* kinds(chat.root)).includes("core.compaction"));
        yield* chat.harness.close;
      }),
    ),
  );

  it.effect("is ordinary work: idle waits include it", () =>
    run(backend)(
      Effect.gen(function* () {
        const chat = yield* open();
        yield* history(chat);
        const gate = deferred();
        const reached = deferred();
        chat.faux.summaries.push(gated(gate, summary(), reached));
        const id = yield* chat.root.compact();
        yield* finished(Deferred.await(reached));
        // Pending work runs while the `TestClock` advances.
        const wait = yield* Effect.forkChild(chat.root.awaitIdle, { startImmediately: true });
        yield* flush;
        yield* TestClock.adjust("20 millis");
        yield* flush;
        assert.isUndefined(wait.pollUnsafe());
        yield* resolve(gate);
        yield* finished(Fiber.join(wait));
        const task = Option.getOrUndefined(yield* chat.harness.harness.getTask(id));
        assert.strictEqual(task?.state.status, "terminal");
        yield* chat.harness.close;
      }),
    ),
  );

  it.effect("enables scheduling right after open", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        const faux = script(setup);
        // Opens with `openChat` alone, without `open`'s resume.
        const { harness, root } = yield* openChat(yield* controlledStorage, setup);
        setup.settings.compaction = { ...MANUAL, keepRecentTokens: 10 };
        const id = yield* root.compact();
        // getTask() only reads, so progress here comes from compact() itself.
        yield* waitFor(
          Effect.map(
            harness.harness.getTask(id),
            (task) => Option.getOrUndefined(task)?.state.status === "terminal",
          ),
        );
        const state = Option.getOrThrow(yield* harness.harness.getTask(id)).state;
        assert.deepStrictEqual(state.status === "terminal" ? state.outcome : undefined, {
          status: "completed",
          result: {},
        });
        assert.strictEqual(faux.summaryRequests.length, 0);
        yield* harness.close;
      }),
    ),
  );
});

describeEachBackend("compaction outcomes", (backend) => {
  it.effect("completes without a summary when there is nothing to compact", () =>
    run(backend)(
      Effect.gen(function* () {
        const chat = yield* open();
        yield* turn(chat, "hi", "hello");
        const outcome = yield* result(chat, yield* chat.root.compact());
        assert.deepStrictEqual(outcome, { status: "completed", result: {} });
        assert.strictEqual(chat.faux.summaryRequests.length, 0);
        assert.isUndefined((yield* live(chat)).compactions);
        yield* chat.harness.close;
      }),
    ),
  );

  it.effect("asks beforeCompact: the first decision wins, a throw is reported and skipped", () =>
    run(backend)(
      Effect.gen(function* () {
        const seen: Array<Parameters<CompactionHooks["beforeCompact"]>[0]> = [];
        const chat = yield* open({
          extensions: [
            // A throwing hook is a defect of its Effect.
            compactionHooks({ beforeCompact: () => Effect.die(new Error("hook broke")) }),
            compactionHooks({
              beforeCompact: (compaction) =>
                Effect.sync(() => {
                  seen.push(compaction);
                  return { summary: "FROM HOOK" };
                }),
            }),
            compactionHooks({ beforeCompact: () => Effect.succeed({ decline: true as const }) }),
          ],
        });
        yield* history(chat);
        const outcome = yield* result(chat, yield* chat.root.compact("why"));
        assert.strictEqual(outcome.status, "completed");
        assert.strictEqual(chat.faux.summaryRequests.length, 0);
        assert.include(
          firstUserText((yield* chat.root.context()).messages),
          "<summary>\nFROM HOOK\n</summary>",
        );
        assert.deepStrictEqual(
          chat.setup.reports.map((report) => messageOf(report.cause)),
          ["hook broke"],
        );
        const compaction = seen[0]!;
        assert.strictEqual(compaction.reason, "manual");
        assert.strictEqual(compaction.instructions, "why");
        assert.deepStrictEqual(
          compaction.entries.map((record: EntryRecord) => record.kind),
          ["core.user", "core.system", "core.assistant", "core.user", "core.assistant"],
        );
        assert.deepStrictEqual(
          compaction.messages.filter((message: Message) => message.role !== "system").map(userText),
          [text("u1", 100), text("a1", 100), text("u2", 100), text("a2", 100)],
        );
        yield* chat.harness.close;
      }),
    ),
  );

  it.effect("completes without a summary when a hook declines", () =>
    run(backend)(
      Effect.gen(function* () {
        const chat = yield* open({
          extensions: [
            compactionHooks({ beforeCompact: () => Effect.succeed({ decline: true as const }) }),
          ],
        });
        yield* history(chat);
        assert.deepStrictEqual(yield* result(chat, yield* chat.root.compact()), {
          status: "completed",
          result: {},
        });
        assert.strictEqual(chat.faux.summaryRequests.length, 0);
        yield* chat.harness.close;
      }),
    ),
  );

  it.effect("fails with no_model without a configured model", () =>
    run(backend)(
      Effect.gen(function* () {
        const chat = yield* open();
        yield* history(chat);
        yield* chat.root.configure({ model: null });
        const outcome = yield* result(chat, yield* chat.root.compact());
        assert.strictEqual(outcome.status, "failed");
        assert.deepStrictEqual(outcome.status === "failed" ? outcome.error.detail : undefined, {
          reason: "no_model",
        });
        assert.isUndefined((yield* live(chat)).compactions);
        yield* chat.harness.close;
      }),
    ),
  );

  // Each `open` runs over a fresh storage of the backend.
  it.effect("retries a retryable error with the pinned request and counts every attempt once", () =>
    Effect.gen(function* () {
      const once = yield* run(backend)(
        Effect.gen(function* () {
          const single = yield* open();
          yield* history(single);
          yield* single.root.configure({ thinkingLevel: "high" });
          single.faux.summaries.push(summary());
          const input = yield* compactionInput(
            single,
            Effect.flatMap(single.root.compact(), (id) => result(single, id)),
          );
          assert.isAbove(input, 0);
          yield* single.harness.close;
          return input;
        }),
      );
      yield* run(backend)(
        Effect.gen(function* () {
          const chat = yield* open();
          yield* history(chat);
          yield* chat.root.configure({ thinkingLevel: "high" });
          chat.setup.settings.stream = { timeoutMs: 1234, deferred: true };
          chat.faux.summaries.push(() =>
            Effect.gen(function* () {
              // Changed during the attempt: the retry still uses the pinned request.
              yield* Effect.orDie(chat.root.configure({ thinkingLevel: "low" }));
              chat.setup.settings.stream = { timeoutMs: 1 };
              return failure("overloaded");
            }),
          );
          chat.faux.summaries.push(summary());
          const twice = yield* compactionInput(
            chat,
            Effect.gen(function* () {
              assert.strictEqual(
                (yield* result(chat, yield* chat.root.compact())).status,
                "completed",
              );
            }),
          );
          assert.strictEqual(chat.faux.summaryRequests.length, 2);
          assert.strictEqual(twice, 2 * once);
          const sessionId = yield* sessionIdOf(chat);
          for (const request of chat.faux.summaryRequests) {
            assert.strictEqual(request.options.reasoning, "high");
            assert.strictEqual(request.options.timeoutMs, 1234);
            assert.strictEqual(request.options.cacheRetention, "none");
            assert.strictEqual(request.options.sessionId, sessionId);
            assert.notProperty(request.options, "deferred");
          }
          yield* chat.harness.close;
        }),
      );
    }),
  );

  it.effect("adds no usage when a hook declines or supplies the summary", () =>
    Effect.gen(function* () {
      for (const decision of [{ decline: true } as const, { summary: "HOOK" }]) {
        // A fresh storage per chat.
        yield* run(backend)(
          Effect.gen(function* () {
            const chat = yield* open({
              extensions: [
                compactionHooks({
                  beforeCompact: () => Effect.succeed<CompactionDecision>(decision),
                }),
              ],
            });
            yield* history(chat);
            assert.strictEqual(
              yield* compactionInput(
                chat,
                Effect.flatMap(chat.root.compact(), (id) => result(chat, id)),
              ),
              0,
            );
            yield* chat.harness.close;
          }),
        );
      }
    }),
  );

  it.effect("caps maxTokens at the model's output limit and sends no tools", () =>
    run(backend)(
      Effect.gen(function* () {
        const read = toolNamed("read");
        const chat = yield* open({ extensions: [toolExtension(read)] });
        yield* chat.root.configure({ tools: [read] });
        yield* history(chat);
        chat.setup.settings.compaction = { ...MANUAL, reserveTokens: 2000 };
        chat.faux.summaries.push(summary());
        yield* result(chat, yield* chat.root.compact());
        const request = chat.faux.summaryRequests[0]!;
        // 0.8 * 2000 = 1600, above the model's 900.
        assert.strictEqual(request.options.maxTokens, 900);
        assert.strictEqual(request.messages.length, 2);
        assert.isFalse(
          request.messages.some(
            (message) => message.role === "system" && message.toolsAdded !== undefined,
          ),
        );
        yield* chat.harness.close;
      }),
    ),
  );

  for (const [name, response, message] of [
    ["retries run out", failure("overloaded"), "Summarization failed: overloaded"],
    ["a non-retryable error", failure("bad request"), "Summarization failed: bad request"],
    [
      "a length stop",
      fauxAssistantMessage("partial", { stopReason: "length" }),
      "Summarization hit the token limit; the summary is incomplete",
    ],
    [
      "a tool call",
      fauxAssistantMessage([fauxToolCall("read", {})], { stopReason: "stop" }),
      "Summarization attempted to call a tool",
    ],
    ["empty text", answer("  "), "Summarization produced no text"],
  ] as const satisfies ReadonlyArray<readonly [string, AssistantMessage, string]>) {
    it.effect(`fails with model_error on ${name}`, () =>
      run(backend)(
        Effect.gen(function* () {
          const chat = yield* open();
          yield* history(chat);
          for (let index = 0; index < 3; index++) chat.faux.summaries.push(response);
          const outcome = yield* result(chat, yield* chat.root.compact());
          assert.strictEqual(outcome.status, "failed");
          if (outcome.status === "failed") {
            assert.strictEqual(outcome.error.message, message);
            assert.deepStrictEqual(outcome.error.detail, { reason: "model_error" });
          }
          // The retry policy allows two retries after the first attempt.
          assert.strictEqual(chat.faux.summaryRequests.length, name === "retries run out" ? 3 : 1);
          assert.isUndefined((yield* live(chat)).compactions);
          assert.isFalse((yield* kinds(chat.root)).includes("core.compaction"));
          yield* chat.harness.close;
        }),
      ),
    );
  }
});

const messageOf = (cause: Cause.Cause<unknown>): string => {
  const error = Cause.squash(cause);
  return error instanceof Error ? error.message : String(error);
};

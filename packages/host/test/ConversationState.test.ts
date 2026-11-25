// The UI state of a conversation view: entries from the client's transcript, and the built-in documents.
import { assert, describe, it } from "@effect/vitest";
import * as ConversationTranscript from "@effect-durable-harness/core/ConversationTranscript";
import { ConversationId, EntryId } from "@effect-durable-harness/core/Ids";
import { emptyUsage, type Message } from "@effect-durable-harness/core/Message";
import type { EntryRecord } from "@effect-durable-harness/core/Records";
import type { ConversationView } from "@effect-durable-harness/core/View";
import * as Effect from "effect/Effect";
import * as ConversationState from "../src/ConversationState.ts";

/** A transcript that has only merged views; it never loads. */
const transcriptOf = (...views: ReadonlyArray<ConversationView>) => {
  const transcript = ConversationTranscript.make(() => Effect.die(new Error("no loads here")));
  let merged: ConversationTranscript.TranscriptView | undefined;
  for (const each of views) merged = transcript.merge(each);
  return merged!;
};

const conversationId = ConversationId.make(1);

const entry = (id: number, kind: string, model?: Message): EntryRecord =>
  model === undefined
    ? { id: EntryId.make(id), conversationId, kind, data: { note: "kept" } }
    : { id: EntryId.make(id), conversationId, kind, model: [model] };

const assistant = (
  content: Extract<Message, { readonly role: "assistant" }>["content"],
  stopReason: "stop" | "toolUse" | "aborted",
): Message => ({
  role: "assistant",
  content,
  api: "faux",
  provider: "faux",
  model: "faux-1",
  usage: emptyUsage(),
  stopReason,
  timestamp: 20,
});

const view: ConversationView = {
  conversation: { id: conversationId },
  entries: [
    entry(1, "core.user", { role: "user", content: "[Ada]\nweather?", timestamp: 10 }),
    entry(
      2,
      "core.assistant",
      assistant(
        [
          { type: "text", text: "Checking." },
          { type: "toolCall", id: "call-1", name: "get_weather", arguments: { city: "Lisbon" } },
        ],
        "toolUse",
      ),
    ),
    entry(3, "core.toolResult", {
      role: "toolResult",
      toolCallId: "call-1",
      toolName: "get_weather",
      content: [{ type: "text", text: "21°C" }],
      details: { celsius: 21 },
      isError: false,
      timestamp: 30,
    }),
    entry(4, "core.assistant", assistant([{ type: "text", text: "cut" }], "aborted")),
    entry(5, "app.note"),
  ],
  docs: {
    "core.live": {
      run: { taskId: 7, inputs: [] },
      generation: {
        message: { content: [{ type: "text", text: "It is " }, { type: "thinking" }] },
        retry: { at: 1, error: "overloaded" },
      },
      tools: [{ callId: "call-2", name: "roll", status: "running", output: "Rolling…" }],
    },
    "core.inbox": {
      items: [
        { id: 8, mode: "steer", content: "faster" },
        { id: 9, mode: "followUp", content: [{ type: "text", text: "then this" }] },
        { id: 10, mode: "write" },
      ],
    },
    "core.agent": { model: { provider: "google", modelId: "gemini" }, tools: ["roll"] },
    "core.usage": {
      models: { "google/gemini": { totalTokens: 100, cost: { total: 0.5 } } },
      tools: { roll: { totalTokens: 10, cost: { total: 0.25 } } },
    },
  },
};

describe("ConversationState.make", () => {
  it("keeps every shown entry with its raw text and message", () => {
    const state = ConversationState.make(transcriptOf(view));
    assert.deepStrictEqual(
      state.entries.map((shown) => [shown.id, shown.role, shown.text]),
      [
        [1, "user", "[Ada]\nweather?"],
        [2, "assistant", "Checking."],
        [3, "tool", "21°C"],
        [4, "assistant", "cut"],
        [5, "data", ""],
      ],
    );
    assert.deepStrictEqual(state.entries[1]?.toolCalls, [
      { callId: "call-1", name: "get_weather", args: { city: "Lisbon" } },
    ]);
    assert.deepStrictEqual(state.entries[2]?.toolResult, {
      callId: "call-1",
      name: "get_weather",
      text: "21°C",
      isError: false,
      details: { celsius: 21 },
    });
    assert.strictEqual(state.entries[3]?.stopped, "aborted");
    assert.deepStrictEqual(state.entries[4]?.data, { note: "kept" });
    assert.strictEqual(state.entries[0]?.message?.role, "user");
  });

  it("reads the live partial, the tool slots, the queued inputs, the agent and the usage", () => {
    const state = ConversationState.make(transcriptOf(view));
    assert.deepStrictEqual(state.live, { text: "It is ", retry: "overloaded" });
    assert.strictEqual(state.running, true);
    assert.deepStrictEqual(state.tools, [
      { callId: "call-2", name: "roll", status: "running", output: "Rolling…", details: undefined },
    ]);
    assert.deepStrictEqual(state.queued, [
      { id: 8, text: "faster", steer: true },
      { id: 9, text: "then this", steer: false },
    ]);
    assert.strictEqual(state.model, "google/gemini");
    assert.deepStrictEqual(state.toolNames, ["roll"]);
    assert.strictEqual(state.tokens, 110);
    assert.strictEqual(state.cost, 0.75);
  });

  it("is empty without a view, and idle without live documents", () => {
    assert.strictEqual(ConversationState.make(null), ConversationState.empty);
    const idle = ConversationState.make(transcriptOf({ ...view, docs: {} }));
    assert.strictEqual(idle.running, false);
    assert.strictEqual(idle.live, undefined);
    assert.deepStrictEqual(idle.queued, []);
  });

  it("keeps the transcript's gaps and its earlier marker in place, around the entries it shows", () => {
    const later: ConversationView = {
      conversation: view.conversation,
      entries: [entry(9, "app.note")],
      before: EntryId.make(8),
      docs: {},
    };
    const state = ConversationState.make(
      transcriptOf({ ...view, entries: view.entries.slice(2), before: EntryId.make(2) }, later),
    );
    assert.deepStrictEqual(
      state.items.map((item) =>
        item.kind === "entry"
          ? item.entry.id
          : item.kind === "gap"
            ? `gap ${item.after}..${item.upTo}`
            : item.kind,
      ),
      ["earlier", 3, 4, 5, "gap 5..9", 9],
    );
    assert.deepStrictEqual(
      state.entries.map((shown) => shown.id),
      [3, 4, 5, 9],
    );
  });
});

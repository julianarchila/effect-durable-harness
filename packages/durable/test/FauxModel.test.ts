import { assert, describe, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import type { AssistantMessage, Message, UserMessage } from "../src/Message.ts";
import {
  type AssistantMessageEvent,
  type GenerateRequest,
  Models,
  type ResolvedModel,
  type StreamOptions,
} from "../src/Models.ts";
import * as FauxModel from "../src/testing/FauxModel.ts";
import {
  fauxAssistantMessage,
  fauxText,
  fauxThinking,
  fauxToolCall,
} from "../src/testing/FauxModel.ts";

// Tests of the fake model: the "faux provider" block covers the provider's behavior, the "FauxModel" block the
// `FauxModel` API.
//
// - Tests that read each event's partial read `collect`, the partial or terminal message of each event; tests that
//   read events read `collectEvents`.
// - `FauxModel.make`'s `layer` provides `Models`; time and pacing are the `TestClock`.
// - Aborting is the stream's interruption: an interrupted stream emits nothing more, so the paced abort cases
//   check that no element follows the delta they interrupt at. A stream interrupted before it starts emits
//   nothing, so there is no case for aborting before the first chunk.
// - There is no provider registry: the fake lives as long as its layer.

const user = (content: string, timestamp = 1): UserMessage => ({
  role: "user",
  content,
  timestamp,
});

const request = (
  messages: ReadonlyArray<Message>,
  extra: { readonly sessionId?: string; readonly options?: StreamOptions } = {},
): GenerateRequest => ({ messages, options: extra.options ?? {}, sessionId: extra.sessionId });

const hi = request([user("hi")]);

/** Every event of one response, the terminal last. */
const collectEvents = (
  faux: FauxModel.FauxModel,
  generate: GenerateRequest,
  model?: ResolvedModel,
) =>
  Effect.gen(function* () {
    const models = yield* Models;
    return yield* Stream.runCollect(models.stream(model ?? faux.getModel(), generate));
  }).pipe(Effect.provide(faux.layer));

/** The message an event carries: its partial, or the terminal message. */
const messageOf = (event: AssistantMessageEvent): AssistantMessage =>
  event.type === "done" ? event.message : event.type === "error" ? event.error : event.partial;

/** The message of every event of one response, the terminal last. */
const collect = (faux: FauxModel.FauxModel, generate: GenerateRequest, model?: ResolvedModel) =>
  Effect.map(collectEvents(faux, generate, model), (events) => events.map(messageOf));

/** The terminal message of one response. */
const complete = (faux: FauxModel.FauxModel, generate: GenerateRequest, model?: ResolvedModel) =>
  Effect.map(collect(faux, generate, model), (elements) => {
    const terminal = elements.at(-1);
    assert.isDefined(terminal);
    // SAFETY: `assert.isDefined` above throws when the stream emitted nothing.
    return terminal as AssistantMessage;
  });

/** Yields until `done` holds, a bounded number of times. */
const settle = (done: () => boolean) =>
  Effect.gen(function* () {
    for (let turn = 0; turn < 100 && !done(); turn++) yield* Effect.yieldNow;
    assert.isTrue(done());
  });

/** Runs a response in a fiber, recording each element as it arrives. */
const forkCollect = (faux: FauxModel.FauxModel, generate: GenerateRequest) =>
  Effect.gen(function* () {
    const seen: Array<AssistantMessage> = [];
    const models = yield* Models;
    const fiber = yield* models.stream(faux.getModel(), generate).pipe(
      Stream.runForEach((event) => Effect.sync(() => seen.push(messageOf(event)))),
      Effect.forkChild({ startImmediately: true }),
    );
    return { seen, fiber };
  });

describe("faux provider", () => {
  it.effect("registers a custom provider and estimates usage", () =>
    Effect.gen(function* () {
      const registration = FauxModel.make();
      registration.setResponses([fauxAssistantMessage("hello world")]);

      const response = yield* complete(
        registration,
        request([{ role: "system", content: "Be concise.", timestamp: 1 }, user("hi there")]),
      );
      assert.deepStrictEqual(response.content, [{ type: "text", text: "hello world" }]);
      assert.isAbove(response.usage.input, 0);
      assert.isAbove(response.usage.output, 0);
      assert.strictEqual(response.usage.totalTokens, response.usage.input + response.usage.output);
      assert.strictEqual(registration.state.callCount, 1);
    }),
  );

  it.effect("supports helper blocks for text, thinking, and tool calls", () =>
    Effect.gen(function* () {
      const registration = FauxModel.make();
      registration.setResponses([
        fauxAssistantMessage(
          [fauxThinking("think"), fauxToolCall("echo", { text: "hi" }), fauxText("done")],
          { stopReason: "toolUse" },
        ),
      ]);

      const response = yield* complete(registration, hi);

      assert.strictEqual(response.content.length, 3);
      assert.deepStrictEqual(response.content[0], { type: "thinking", thinking: "think" });
      const call = response.content[1];
      assert.strictEqual(call?.type, "toolCall");
      if (call?.type === "toolCall") {
        assert.isString(call.id);
        assert.strictEqual(call.name, "echo");
        assert.deepStrictEqual(call.arguments, { text: "hi" });
      }
      assert.deepStrictEqual(response.content[2], { type: "text", text: "done" });
      assert.strictEqual(response.stopReason, "toolUse");
    }),
  );

  it.effect("supports multiple models with per-model reasoning and model-aware factories", () =>
    Effect.gen(function* () {
      const registration = FauxModel.make({
        models: [
          { id: "faux-fast", reasoning: false },
          { id: "faux-thinker", reasoning: true },
        ],
      });
      registration.setResponses([
        (_request, _state, model) =>
          Effect.succeed(fauxAssistantMessage(`${model.id}:${String(model.reasoning)}`)),
        (_request, _state, model) =>
          Effect.succeed(fauxAssistantMessage(`${model.id}:${String(model.reasoning)}`)),
      ]);

      assert.deepStrictEqual(
        registration.models.map((model) => model.id),
        ["faux-fast", "faux-thinker"],
      );
      assert.strictEqual(registration.getModel(), registration.models[0]);
      assert.isFalse(registration.getModel("faux-fast")?.reasoning);
      assert.isTrue(registration.getModel("faux-thinker")?.reasoning);

      const fast = yield* complete(registration, hi, registration.getModel("faux-fast"));
      const thinker = yield* complete(registration, hi, registration.getModel("faux-thinker"));

      assert.deepStrictEqual(fast.content, [{ type: "text", text: "faux-fast:false" }]);
      assert.deepStrictEqual(thinker.content, [{ type: "text", text: "faux-thinker:true" }]);
    }),
  );

  it.effect("rewrites api, provider, and model on returned messages", () =>
    Effect.gen(function* () {
      const registration = FauxModel.make({
        api: "faux:test",
        provider: "faux-provider",
        models: [{ id: "faux-model" }],
      });
      registration.setResponses([fauxAssistantMessage("hello")]);

      const response = yield* complete(registration, hi);

      assert.strictEqual(response.api, "faux:test");
      assert.strictEqual(response.provider, "faux-provider");
      assert.strictEqual(response.model, "faux-model");
    }),
  );

  it.effect("consumes queued responses in order and errors when exhausted", () =>
    Effect.gen(function* () {
      const registration = FauxModel.make();
      registration.setResponses([fauxAssistantMessage("first"), fauxAssistantMessage("second")]);

      const first = yield* complete(registration, hi);
      const second = yield* complete(registration, hi);
      const exhausted = yield* complete(registration, hi);

      assert.deepStrictEqual(first.content, [{ type: "text", text: "first" }]);
      assert.deepStrictEqual(second.content, [{ type: "text", text: "second" }]);
      assert.strictEqual(exhausted.stopReason, "error");
      assert.strictEqual(exhausted.errorMessage, "No more faux responses queued");
      assert.strictEqual(registration.pendingResponseCount(), 0);
      assert.strictEqual(registration.state.callCount, 3);
    }),
  );

  it.effect("can replace and append queued responses", () =>
    Effect.gen(function* () {
      const registration = FauxModel.make();
      registration.setResponses([fauxAssistantMessage("first")]);

      assert.deepStrictEqual((yield* complete(registration, hi)).content, [
        { type: "text", text: "first" },
      ]);
      assert.strictEqual(registration.pendingResponseCount(), 0);

      registration.setResponses([fauxAssistantMessage("second")]);
      assert.strictEqual(registration.pendingResponseCount(), 1);
      assert.deepStrictEqual((yield* complete(registration, hi)).content, [
        { type: "text", text: "second" },
      ]);

      registration.appendResponses([fauxAssistantMessage("third"), fauxAssistantMessage("fourth")]);
      assert.strictEqual(registration.pendingResponseCount(), 2);
      assert.deepStrictEqual((yield* complete(registration, hi)).content, [
        { type: "text", text: "third" },
      ]);
      assert.deepStrictEqual((yield* complete(registration, hi)).content, [
        { type: "text", text: "fourth" },
      ]);
      assert.strictEqual(registration.pendingResponseCount(), 0);
    }),
  );

  it.effect("supports async response factories", () =>
    Effect.gen(function* () {
      const registration = FauxModel.make();
      registration.setResponses([
        (generate, state) =>
          Effect.yieldNow.pipe(
            Effect.as(fauxAssistantMessage(`${generate.messages.length}:${state.callCount}`)),
          ),
      ]);

      const response = yield* complete(registration, hi);

      assert.deepStrictEqual(response.content, [{ type: "text", text: "1:1" }]);
    }),
  );

  it.effect("emits an error when a response factory throws", () =>
    Effect.gen(function* () {
      const registration = FauxModel.make();
      registration.setResponses([() => Effect.die(new Error("boom"))]);

      const events = yield* collectEvents(registration, hi);

      assert.strictEqual(events.length, 1);
      assert.strictEqual(events[0]?.type, "error");
      assert.strictEqual(events[0] && messageOf(events[0]).stopReason, "error");
      assert.strictEqual(events[0] && messageOf(events[0]).errorMessage, "boom");
    }),
  );

  it.effect("rejects a queued response without a terminal stop reason", () =>
    Effect.gen(function* () {
      const registration = FauxModel.make();
      registration.setResponses([fauxAssistantMessage("partial", { stopReason: "pending" })]);

      const events = yield* collectEvents(registration, hi);

      assert.isFalse(events.some((event) => event.type === "done"));
      assert.strictEqual(events.at(-1)?.type, "error");
      const terminal = events.map(messageOf).at(-1);
      assert.strictEqual(terminal?.stopReason, "error");
      assert.strictEqual(terminal?.errorMessage, "Faux response ended without a stop reason");
    }),
  );

  it.effect("estimates prompt and output tokens from serialized context", () =>
    Effect.gen(function* () {
      const registration = FauxModel.make();
      registration.setResponses([fauxAssistantMessage("done")]);

      // The request carries `systemPrompt` and `tools` in a system message, which the estimate renders as
      // `tool+:` lines.
      const tool = {
        name: "echo",
        description: "Echo back text",
        parameters: { type: "object", properties: { text: { type: "string" } } },
      };
      const response = yield* complete(
        registration,
        request([
          { role: "system", content: "sys", toolsAdded: [tool], timestamp: 0 },
          {
            role: "user",
            content: [
              { type: "text", text: "hello" },
              { type: "image", mimeType: "image/png", data: "abcd" },
            ],
            timestamp: 1,
          },
          fauxAssistantMessage("prior"),
          {
            role: "toolResult",
            toolCallId: "tool-1",
            toolName: "echo",
            content: [{ type: "text", text: "tool out" }],
            isError: false,
            timestamp: 2,
          },
        ]),
      );
      const promptText = [
        `system:sys\ntool+:{"name":"echo","description":"Echo back text","parameters":{"type":"object","properties":{"text":{"type":"string"}}}}`,
        "user:hello\n[image:image/png:4]",
        "assistant:prior",
        "toolResult:echo\ntool out",
      ].join("\n\n");
      const expectedPromptTokens = Math.ceil(promptText.length / 4);
      const expectedOutputTokens = Math.ceil("done".length / 4);

      assert.strictEqual(response.usage.input, expectedPromptTokens);
      assert.strictEqual(response.usage.output, expectedOutputTokens);
      assert.strictEqual(response.usage.cacheRead, 0);
      assert.strictEqual(response.usage.cacheWrite, 0);
      assert.strictEqual(response.usage.totalTokens, expectedPromptTokens + expectedOutputTokens);
    }),
  );

  it.effect("does not share cache across sessions or requests without sessionId", () =>
    Effect.gen(function* () {
      const registration = FauxModel.make();
      registration.setResponses([
        fauxAssistantMessage("first"),
        fauxAssistantMessage("second"),
        fauxAssistantMessage("third"),
      ]);
      const short = { cacheRetention: "short" } as const;

      const messages: Array<Message> = [user("hello")];
      const first = yield* complete(
        registration,
        request(messages, { sessionId: "session-1", options: short }),
      );
      assert.isAbove(first.usage.cacheWrite, 0);
      messages.push(first, user("follow up", 2));

      const second = yield* complete(
        registration,
        request(messages, { sessionId: "session-2", options: short }),
      );
      assert.strictEqual(second.usage.cacheRead, 0);
      assert.isAbove(second.usage.cacheWrite, 0);

      const third = yield* complete(registration, request(messages));
      assert.strictEqual(third.usage.cacheRead, 0);
      assert.strictEqual(third.usage.cacheWrite, 0);
    }),
  );

  it.effect("simulates prompt caching per sessionId", () =>
    Effect.gen(function* () {
      const registration = FauxModel.make();
      registration.setResponses([fauxAssistantMessage("first"), fauxAssistantMessage("second")]);
      const extra = { sessionId: "session-1", options: { cacheRetention: "short" } } as const;

      const messages: Array<Message> = [
        { role: "system", content: "Be concise.", timestamp: 0 },
        user("hello"),
      ];
      const first = yield* complete(registration, request(messages, extra));
      assert.strictEqual(first.usage.cacheRead, 0);
      assert.isAbove(first.usage.cacheWrite, 0);

      messages.push(first, user("follow up", 2));

      const second = yield* complete(registration, request(messages, extra));
      assert.isAbove(second.usage.cacheRead, 0);
      assert.isAbove(second.usage.input + second.usage.cacheRead, second.usage.input);
    }),
  );

  it.effect("counts cached characters up to the first difference in the joined prompt", () =>
    Effect.gen(function* () {
      const registration = FauxModel.make();
      registration.setResponses([
        fauxAssistantMessage("a"),
        fauxAssistantMessage("b"),
        fauxAssistantMessage("c"),
      ]);
      const extra = { sessionId: "session-1", options: { cacheRetention: "short" } } as const;

      // Prompt texts: "user:hello world" (16 characters), then 16 + 2 + "user:next" (9) = 27.
      yield* complete(registration, request([user("hello world")], extra));
      const extended = yield* complete(
        registration,
        request([user("hello world"), user("next")], extra),
      );
      assert.deepInclude(extended.usage, { input: 3, cacheRead: 4, cacheWrite: 3 });

      // The first message now differs after "user:hello w" (12 characters): "user:hello wide" + 2 + 9 = 26.
      const edited = yield* complete(
        registration,
        request([user("hello wide"), user("next")], extra),
      );
      assert.deepInclude(edited.usage, { input: 4, cacheRead: 3, cacheWrite: 4 });
    }),
  );

  it.effect("does not simulate caching when cacheRetention is none", () =>
    Effect.gen(function* () {
      const registration = FauxModel.make();
      registration.setResponses([fauxAssistantMessage("first"), fauxAssistantMessage("second")]);
      const extra = { sessionId: "session-1", options: { cacheRetention: "none" } } as const;

      const messages: Array<Message> = [user("hello")];
      yield* complete(registration, request(messages, extra));
      messages.push(fauxAssistantMessage("first"), user("follow up", 2));
      const second = yield* complete(registration, request(messages, extra));
      assert.strictEqual(second.usage.cacheRead, 0);
      assert.strictEqual(second.usage.cacheWrite, 0);
    }),
  );

  it.effect("streams thinking, text, and partial tool call deltas", () =>
    Effect.gen(function* () {
      const registration = FauxModel.make();
      registration.setResponses([
        fauxAssistantMessage(
          [
            fauxThinking("thinking text"),
            fauxText("answer text"),
            fauxToolCall("echo", { text: "hi", count: 12 }, { id: "tool-1" }),
          ],
          { stopReason: "toolUse" },
        ),
      ]);

      const events = yield* collectEvents(registration, hi);
      const types = events.map((event) => event.type);
      const toolCallDeltas = events.flatMap((event) =>
        event.type === "toolcall_delta" ? [event.delta] : [],
      );

      for (const type of [
        "thinking_start",
        "thinking_delta",
        "text_start",
        "text_delta",
        "toolcall_start",
        "toolcall_delta",
        "toolcall_end",
      ] as const)
        assert.include(types, type);
      assert.isAbove(toolCallDeltas.length, 1);
      assert.deepStrictEqual(
        Schema.decodeSync(Schema.fromJsonString(Schema.Unknown))(toolCallDeltas.join("")),
        {
          text: "hi",
          count: 12,
        },
      );
      // effect-durable-harness: the partial arguments grow with the parsed text, and end as the scripted ones.
      const calls = events
        .map((event) => messageOf(event).content[2])
        .filter((call) => call?.type === "toolCall");
      assert.isTrue(calls.some((call) => Object.keys(call.arguments).length === 1));
      assert.deepStrictEqual(calls.at(-1)?.arguments, { text: "hi", count: 12 });
    }),
  );

  it.effect("streams an exact event order for fixed-size chunks", () =>
    Effect.gen(function* () {
      const registration = FauxModel.make({ tokenSize: { min: 1, max: 1 } });
      registration.setResponses([
        fauxAssistantMessage(
          [fauxThinking("go"), fauxText("ok"), fauxToolCall("echo", {}, { id: "tool-1" })],
          { stopReason: "toolUse" },
        ),
      ]);

      const events = yield* collectEvents(registration, hi);

      assert.deepStrictEqual(
        events.map((event) => event.type),
        [
          "start",
          "thinking_start",
          "thinking_delta",
          "thinking_end",
          "text_start",
          "text_delta",
          "text_end",
          "toolcall_start",
          "toolcall_delta",
          "toolcall_end",
          "done",
        ],
      );
      // effect-durable-harness: the message each event carries.
      const thought = { type: "thinking", thinking: "go" } as const;
      const text = { type: "text", text: "ok" } as const;
      const call = { type: "toolCall", id: "tool-1", name: "echo", arguments: {} } as const;
      assert.deepStrictEqual(
        events.map(messageOf).map((element) => [element.stopReason, element.content]),
        [
          ["pending", []], // start
          ["pending", [{ type: "thinking", thinking: "" }]], // thinking_start
          ["pending", [thought]], // thinking_delta
          ["pending", [thought]], // thinking_end
          ["pending", [thought, { type: "text", text: "" }]], // text_start
          ["pending", [thought, text]], // text_delta
          ["pending", [thought, text]], // text_end
          ["pending", [thought, text, call]], // toolcall_start
          ["pending", [thought, text, call]], // toolcall_delta
          ["pending", [thought, text, call]], // toolcall_end
          ["toolUse", [thought, text, call]], // done
        ],
      );
    }),
  );

  it.effect("streams multiple tool calls in one message", () =>
    Effect.gen(function* () {
      const registration = FauxModel.make();
      registration.setResponses([
        fauxAssistantMessage(
          [
            fauxToolCall("echo", { text: "one" }, { id: "tool-1" }),
            fauxToolCall("echo", { text: "two" }, { id: "tool-2" }),
          ],
          { stopReason: "toolUse" },
        ),
      ]);

      const events = yield* collectEvents(registration, hi);
      const elements = events.map(messageOf);

      assert.strictEqual(events.filter((event) => event.type === "toolcall_start").length, 2);
      assert.strictEqual(events.filter((event) => event.type === "toolcall_end").length, 2);
      assert.deepStrictEqual(
        [...new Set(elements.map((element) => element.content.length))],
        [0, 1, 2],
      );
      assert.deepStrictEqual(
        elements.at(-1)?.content.map((call) => (call.type === "toolCall" ? call.id : "")),
        ["tool-1", "tool-2"],
      );
    }),
  );

  it.effect("streams an explicit assistant error message as a terminal error", () =>
    Effect.gen(function* () {
      const registration = FauxModel.make({ tokenSize: { min: 2, max: 2 } });
      registration.setResponses([
        {
          ...fauxAssistantMessage("partial"),
          stopReason: "error",
          errorMessage: "upstream failed",
        },
      ]);

      const events = yield* collectEvents(registration, hi);
      const elements = events.map(messageOf);

      assert.deepStrictEqual(
        events.map((event) => event.type),
        ["start", "text_start", "text_delta", "text_end", "error"],
      );
      const terminal = events.at(-1);
      assert.strictEqual(terminal?.type === "error" ? terminal.reason : undefined, "error");
      assert.deepStrictEqual(
        elements.map((element) => element.stopReason),
        ["pending", "pending", "pending", "pending", "error"],
      );
      assert.strictEqual(elements.at(-1)?.errorMessage, "upstream failed");
    }),
  );

  it.effect("streams an explicit assistant aborted message as a terminal error", () =>
    Effect.gen(function* () {
      const registration = FauxModel.make({ tokenSize: { min: 2, max: 2 } });
      registration.setResponses([
        {
          ...fauxAssistantMessage("partial"),
          stopReason: "aborted",
          errorMessage: "Request was aborted",
        },
      ]);

      const events = yield* collectEvents(registration, hi);
      const elements = events.map(messageOf);

      assert.deepStrictEqual(
        events.map((event) => event.type),
        ["start", "text_start", "text_delta", "text_end", "error"],
      );
      const terminal = events.at(-1);
      assert.strictEqual(terminal?.type === "error" ? terminal.reason : undefined, "aborted");
      assert.deepStrictEqual(
        elements.map((element) => element.stopReason),
        ["pending", "pending", "pending", "pending", "aborted"],
      );
      assert.strictEqual(elements.at(-1)?.errorMessage, "Request was aborted");
    }),
  );

  // Aborting is the stream's interruption: an interrupted stream emits nothing more.
  it.effect("supports aborting mid-text stream when paced", () =>
    Effect.gen(function* () {
      const registration = FauxModel.make({ tokensPerSecond: 100, tokenSize: { min: 3, max: 3 } });
      registration.setResponses([fauxAssistantMessage("abcdefghijklmnopqrstuvwxyz")]);

      const { seen, fiber } = yield* forkCollect(registration, hi).pipe(
        Effect.provide(registration.layer),
      );
      yield* settle(() => seen.length === 2); // start, text_start
      // 12 characters are 3 tokens, 30ms at 100 tokens per second.
      yield* TestClock.adjust(30);
      yield* settle(() => seen.length === 3);
      yield* Fiber.interrupt(fiber);
      yield* TestClock.adjust(1000);

      assert.strictEqual(seen.length, 3);
      assert.deepStrictEqual(seen.at(-1)?.content, [{ type: "text", text: "abcdefghijkl" }]);
      assert.isTrue(seen.every((element) => element.stopReason === "pending"));
    }),
  );

  // As "supports aborting mid-text stream when paced", over a thinking block.
  it.effect("supports aborting mid-thinking stream when paced", () =>
    Effect.gen(function* () {
      const registration = FauxModel.make({ tokensPerSecond: 100, tokenSize: { min: 3, max: 3 } });
      registration.setResponses([
        {
          ...fauxAssistantMessage("ignored"),
          content: [{ type: "thinking", thinking: "abcdefghijklmnopqrstuvwxyz" }],
        },
      ]);

      const { seen, fiber } = yield* forkCollect(registration, hi).pipe(
        Effect.provide(registration.layer),
      );
      yield* settle(() => seen.length === 2); // start, thinking_start
      // 12 characters are 3 tokens, 30ms at 100 tokens per second.
      yield* TestClock.adjust(30);
      yield* settle(() => seen.length === 3);
      yield* Fiber.interrupt(fiber);
      yield* TestClock.adjust(1000);

      assert.strictEqual(seen.length, 3);
      assert.deepStrictEqual(seen.at(-1)?.content, [
        { type: "thinking", thinking: "abcdefghijkl" },
      ]);
      assert.isTrue(seen.every((element) => element.stopReason === "pending"));
    }),
  );

  // As "supports aborting mid-text stream when paced", over a tool call: its arguments stream as JSON text.
  it.effect("supports aborting mid-toolcall stream when paced", () =>
    Effect.gen(function* () {
      const registration = FauxModel.make({ tokensPerSecond: 100, tokenSize: { min: 3, max: 3 } });
      registration.setResponses([
        {
          ...fauxAssistantMessage("done"),
          content: [
            {
              type: "toolCall",
              id: "tool-1",
              name: "echo",
              arguments: { text: "abcdefghijklmnopqrstuvwxyz", count: 123456789 },
            },
          ],
          stopReason: "toolUse",
        },
      ]);

      const { seen, fiber } = yield* forkCollect(registration, hi).pipe(
        Effect.provide(registration.layer),
      );
      yield* settle(() => seen.length === 2); // start, toolcall_start
      // 12 characters are 3 tokens, 30ms at 100 tokens per second.
      yield* TestClock.adjust(30);
      yield* settle(() => seen.length === 3);
      yield* Fiber.interrupt(fiber);
      yield* TestClock.adjust(1000);

      assert.strictEqual(seen.length, 3);
      // The partial after the first delta, `{"text":"abc`: the arguments that text parses to so far.
      assert.deepStrictEqual(seen.at(-1)?.content, [
        { type: "toolCall", id: "tool-1", name: "echo", arguments: { text: "abc" } },
      ]);
      assert.isTrue(seen.every((element) => element.stopReason === "pending"));
    }),
  );
});

describe("FauxModel", () => {
  it.effect("resolves its provider's models and nothing else", () => {
    const faux = FauxModel.make({ models: [{ id: "big", contextWindow: 3000 }, { id: "small" }] });
    return Effect.gen(function* () {
      const models = yield* Models;

      const big = yield* models.resolve({ provider: "faux", modelId: "big" });
      assert.deepStrictEqual(big, Option.some(faux.models[0]));
      assert.deepStrictEqual(Option.getOrThrow(big), {
        provider: "faux",
        id: "big",
        api: "faux",
        contextWindow: 3000,
        maxTokens: 16384,
        reasoning: false,
        input: ["text", "image"],
        cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
      });
      assert.isTrue(Option.isNone(yield* models.resolve({ provider: "faux", modelId: "faux-1" })));
      assert.isTrue(Option.isNone(yield* models.resolve({ provider: "other", modelId: "big" })));
    }).pipe(Effect.provide(faux.layer));
  });

  it.effect("paces deltas by tokens per second on the clock and times the response", () =>
    Effect.gen(function* () {
      const faux = FauxModel.make({ tokensPerSecond: 20, tokenSize: { min: 1, max: 1 } });
      faux.setResponses([fauxAssistantMessage("abcdefgh")]);
      yield* TestClock.setTime(1000);

      const { seen, fiber } = yield* forkCollect(faux, hi).pipe(Effect.provide(faux.layer));
      yield* settle(() => seen.length === 2);
      // Each 4-character chunk is one token: 50ms at 20 tokens per second.
      yield* TestClock.adjust(49);
      assert.strictEqual(seen.length, 2);
      yield* TestClock.adjust(1);
      yield* settle(() => seen.length === 3);
      yield* TestClock.adjust(50);
      yield* Fiber.join(fiber);

      assert.strictEqual(seen.length, 6); // start, text_start, 2 deltas, text_end, done
      const terminal = seen.at(-1);
      assert.strictEqual(terminal?.stopReason, "stop");
      assert.strictEqual(terminal?.timestamp, 1000);
      assert.strictEqual(terminal?.durationMs, 100);
    }),
  );

  it.effect("keeps an explicit timestamp that predates the request, untimed", () =>
    Effect.gen(function* () {
      const faux = FauxModel.make();
      faux.setResponses([fauxAssistantMessage("hi", { timestamp: 5, responseId: "r1" })]);
      yield* TestClock.setTime(1000);

      const terminal = yield* complete(faux, hi);

      assert.strictEqual(terminal.timestamp, 5);
      assert.strictEqual(terminal.responseId, "r1");
      assert.isUndefined(terminal.durationMs);
    }),
  );

  it.effect("emits each partial as its own snapshot", () =>
    Effect.gen(function* () {
      const faux = FauxModel.make({ tokenSize: { min: 1, max: 1 } });
      faux.setResponses([fauxAssistantMessage("abcdefgh")]);

      const elements = yield* collect(faux, hi);

      assert.deepStrictEqual(
        elements.map((element) => element.content[0]),
        [
          undefined,
          { type: "text", text: "" },
          { type: "text", text: "abcd" },
          { type: "text", text: "abcdefgh" },
          { type: "text", text: "abcdefgh" },
          { type: "text", text: "abcdefgh" },
        ],
      );
    }),
  );

  it.effect("a request that never answers ends only when interrupted", () =>
    Effect.gen(function* () {
      const faux = FauxModel.make();
      faux.setResponses([() => Effect.never, fauxAssistantMessage("next")]);

      const { seen, fiber } = yield* forkCollect(faux, hi).pipe(Effect.provide(faux.layer));
      yield* settle(() => faux.state.callCount === 1);
      yield* TestClock.adjust(60_000);
      assert.strictEqual(seen.length, 0);

      const exit = yield* Fiber.interrupt(fiber).pipe(Effect.andThen(Fiber.await(fiber)));
      assert.isTrue(Exit.hasInterrupts(exit));
      assert.strictEqual(faux.pendingResponseCount(), 1);
      assert.deepStrictEqual((yield* complete(faux, hi)).content, [{ type: "text", text: "next" }]);
    }),
  );

  // The `deferred` options: a request answered with a handle that is polled later.
  it.effect(
    "answers a deferred request with a handle, fetches it after its pending fetches, and cancels",
    () =>
      Effect.gen(function* () {
        const faux = FauxModel.make({ deferred: { pendingFetches: 1, pollAfterMs: 7 } });
        faux.setResponses([fauxAssistantMessage("late"), fauxAssistantMessage("never")]);
        const deferred = request([user("hi")], { options: { deferred: true } });
        const models = yield* Effect.provide(Effect.service(Models), faux.layer);
        const fetch = models.fetchDeferred ?? assert.fail("no fetchDeferred");
        const cancel = models.cancelDeferred ?? assert.fail("no cancelDeferred");

        const first = yield* complete(faux, deferred);
        assert.strictEqual(first.stopReason, "deferred");
        assert.deepStrictEqual(first.content, []);
        const handle = first.deferred ?? assert.fail("no handle");
        assert.deepStrictEqual(handle, {
          provider: "faux",
          modelId: "faux-1",
          api: "faux",
          id: handle.id,
          pollAfterMs: 7,
        });
        assert.strictEqual((yield* fetch(faux.getModel(), handle)).stopReason, "deferred");
        const ready = yield* fetch(faux.getModel(), handle);
        assert.strictEqual(ready.stopReason, "stop");
        assert.deepStrictEqual(ready.content, [{ type: "text", text: "late" }]);
        // The response is resolved once.
        assert.deepStrictEqual(yield* fetch(faux.getModel(), handle), ready);
        assert.strictEqual(faux.state.deferredFetchCount, 3);

        const second = (yield* complete(faux, deferred)).deferred ?? assert.fail("no handle");
        assert.notStrictEqual(second.id, handle.id);
        yield* cancel(faux.getModel(), second);
        assert.deepStrictEqual(faux.state.cancelledDeferred, [second]);
        const cancelled = yield* fetch(faux.getModel(), second);
        assert.strictEqual(cancelled.stopReason, "error");
        assert.strictEqual(
          cancelled.errorMessage,
          `Faux deferred response was cancelled: ${second.id}`,
        );
        const unknown = yield* fetch(faux.getModel(), { ...second, id: "missing" });
        assert.strictEqual(unknown.errorMessage, "Unknown faux deferred response: missing");
        assert.strictEqual(faux.state.callCount, 2);
      }),
  );

  it.effect("answers a request without the deferred option at once", () =>
    Effect.gen(function* () {
      const faux = FauxModel.make({ deferred: { pendingFetches: 1 } });
      faux.setResponses([fauxAssistantMessage("now")]);
      const answer = yield* complete(faux, request([user("hi")], { options: { deferred: false } }));
      assert.strictEqual(answer.stopReason, "stop");
      assert.strictEqual(faux.state.deferredFetchCount, 0);
    }),
  );
});

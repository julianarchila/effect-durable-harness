/**
 * The adapter's transcript helpers, system replay part; schema and context-rule cases live in
 * `@effect-durable-harness/core`, which owns them.
 */
import { assert, describe, it } from "@effect/vitest";
import {
  emptyUsage,
  getCurrentTools,
  type AssistantMessage,
  type SystemMessage,
  type Tool,
} from "@effect-durable-harness/core/Message";
import {
  collapseSystemMessages,
  renderSystemMessageUpdate,
  transformMessages,
} from "../src/internal/transcript.ts";
import { haiku55 } from "./support/Models.ts";

const weatherTool: Tool = {
  name: "get_weather",
  description: "Weather.",
  parameters: { type: "object" },
};

const system: SystemMessage = {
  role: "system",
  content: "Be careful.",
  sections: { units: "<units>Use Celsius.</units>", tone: "Be brief." },
  toolsAdded: [weatherTool],
  timestamp: 1,
};

const update: SystemMessage = {
  role: "system",
  content: "From now on, answer in Spanish.",
  sections: { units: "<units>Use Fahrenheit.</units>", tone: null },
  timestamp: 6,
};

describe("system replay", () => {
  it("collapse keeps base content, appends later content, patches sections by name, keeps tools", () => {
    const [head, ...rest] = collapseSystemMessages([
      system,
      { role: "user", content: "hi", timestamp: 2 },
      update,
    ]);
    assert.deepStrictEqual(head, {
      role: "system",
      content: "Be careful.\n\nFrom now on, answer in Spanish.",
      sections: { units: "<units>Use Fahrenheit.</units>" },
      toolsAdded: [weatherTool],
      timestamp: 1,
    });
    assert.deepStrictEqual(
      rest.map((message) => message.role),
      ["user"],
    );
  });

  it("toolsRemoved then toolsAdded, in order", () => {
    assert.deepStrictEqual(
      getCurrentTools([
        system,
        { role: "system", content: "", toolsRemoved: [{ name: "get_weather" }], timestamp: 9 },
      ]),
      [],
    );
  });

  it("an update frames its sections by name (renderSystemMessageUpdate)", () => {
    assert.strictEqual(
      renderSystemMessageUpdate(update),
      'From now on, answer in Spanish.\n\nUpdated system prompt section "units":\n\n<units>Use Fahrenheit.</units>\n\nRemoved system prompt section "tone".',
    );
  });
});

describe("transformMessages", () => {
  const response: AssistantMessage = {
    role: "assistant",
    content: [
      { type: "thinking", thinking: "", thinkingSignature: "encrypted" },
      { type: "thinking", thinking: "  " },
      { type: "toolCall", id: "c1", name: "get_weather", arguments: {}, thoughtSignature: "ts" },
    ],
    api: haiku55.api,
    provider: haiku55.provider,
    model: haiku55.id,
    usage: emptyUsage(),
    stopReason: "toolUse",
    timestamp: 3,
  };

  it("same model: a signed thinking block stays even when empty; an empty unsigned one is dropped", () => {
    const [message] = transformMessages([response], haiku55);
    assert.deepStrictEqual(message, {
      ...response,
      content: [response.content[0]!, response.content[2]!],
    });
  });

  it("another model: signatures and thought signatures are dropped", () => {
    const [message] = transformMessages([{ ...response, provider: "google" }], haiku55);
    assert.deepStrictEqual(message, {
      ...response,
      provider: "google",
      content: [{ type: "toolCall", id: "c1", name: "get_weather", arguments: {} }],
    });
  });

  it("each run of images becomes one placeholder (replaceImagesWithPlaceholder)", () => {
    const [message] = transformMessages(
      [
        {
          role: "user",
          content: [
            { type: "image", data: "a", mimeType: "image/png" },
            { type: "image", data: "b", mimeType: "image/png" },
            { type: "text", text: "between" },
            { type: "image", data: "c", mimeType: "image/png" },
          ],
          timestamp: 1,
        },
      ],
      { ...haiku55, input: ["text"] },
    );
    assert.deepStrictEqual(message, {
      role: "user",
      content: [
        { type: "text", text: "(image omitted: model does not support images)" },
        { type: "text", text: "between" },
        { type: "text", text: "(image omitted: model does not support images)" },
      ],
      timestamp: 1,
    });
  });
});

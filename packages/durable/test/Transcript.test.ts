import { assert, describe, it } from "@effect/vitest";
import { getCurrentTools, type Message, type Tool } from "../src/Message.ts";
import * as Transcript from "../src/internal/transcript.ts";

// Tests of the transcript helpers: replaying system messages and comparing tool declarations. Helpers that
// render provider requests stay behind `Models` and are not tested here.

const tool = (name: string, description = `${name} tool`): Tool => ({
  name,
  description,
  parameters: { type: "object", properties: {} },
});

const names = (tools: ReadonlyArray<Tool>): Array<string> => tools.map((each) => each.name);

const transcript: ReadonlyArray<Message> = [
  {
    role: "system",
    content: "base",
    sections: { a: "<a>1</a>", b: "<b>1</b>" },
    toolsAdded: [tool("first")],
    timestamp: 10,
  },
  { role: "user", content: "hello", timestamp: 11 },
  { role: "system", content: "also do this", timestamp: 12 },
  {
    role: "system",
    content: "",
    sections: { a: "<a>2</a>", b: null, c: "<c>1</c>" },
    toolsRemoved: [{ name: "first" }],
    toolsAdded: [tool("second")],
    timestamp: 14,
  },
];

describe("Transcript", () => {
  describe("getCurrentTools", () => {
    it("replays tool additions and removals in order", () => {
      assert.deepStrictEqual(getCurrentTools(transcript), [tool("second")]);
      assert.deepStrictEqual(getCurrentTools(transcript.slice(0, 3)), [tool("first")]);
    });

    it("is empty without system messages", () => {
      assert.deepStrictEqual(getCurrentTools([{ role: "user", content: "hi", timestamp: 1 }]), []);
    });

    it("applies removals before additions, so a re-added tool moves to the end", () => {
      const tools = getCurrentTools([
        { role: "system", content: "", toolsAdded: [tool("a"), tool("b")], timestamp: 1 },
        {
          role: "system",
          content: "",
          toolsRemoved: [{ name: "a" }],
          toolsAdded: [tool("a", "changed")],
          timestamp: 2,
        },
      ]);
      assert.deepStrictEqual(tools, [tool("b"), tool("a", "changed")]);
    });

    it("replaces a tool added again without a removal in place", () => {
      const tools = getCurrentTools([
        { role: "system", content: "", toolsAdded: [tool("a"), tool("b")], timestamp: 1 },
        { role: "system", content: "", toolsAdded: [tool("a", "changed")], timestamp: 2 },
      ]);
      assert.deepStrictEqual(names(tools), ["a", "b"]);
      assert.strictEqual(tools[0]!.description, "changed");
    });
  });

  describe("declarations", () => {
    it("compares tool declarations without executable fields", () => {
      const executable = { ...tool("a"), execute: () => undefined };
      assert.isTrue(Transcript.declarationsEqual(executable, tool("a")));
      assert.isFalse(Transcript.declarationsEqual(tool("a"), tool("a", "changed")));
    });

    it("compares parameters by their serialized form, key order included", () => {
      const ordered: Tool = { ...tool("a"), parameters: { type: "object", required: [] } };
      const reordered: Tool = { ...tool("a"), parameters: { required: [], type: "object" } };
      assert.isTrue(Transcript.declarationsEqual(ordered, { ...ordered }));
      assert.isFalse(Transcript.declarationsEqual(ordered, reordered));
    });

    it("keeps only the declared fields", () => {
      const executable = { ...tool("a"), execute: () => undefined };
      assert.deepStrictEqual(Transcript.toToolDeclaration(executable), tool("a"));
    });
  });
});

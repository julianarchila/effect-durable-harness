// `PartialChanges` turns a streaming partial into live generation changes, checked with
// `@effect-durable-harness/delta`'s `Tracker`, the kernel's streaming JSON parser (`internal/streamingJson.ts`) and
// `Json.copy`. The draft is the JSON record `PartialChanges.commit` takes; a case's message names the piece size and
// rhythm.
import { assert, describe, it } from "@effect/vitest";
import type * as Delta from "@effect-durable-harness/delta/Delta";
import * as Json from "@effect-durable-harness/delta/Json";
import * as Tracker from "@effect-durable-harness/delta/Tracker";
import * as Schema from "effect/Schema";
import type { JsonRecord } from "../src/internal/json.ts";
import { PartialChanges, type PartialMessage } from "../src/internal/partialChanges.ts";
import { ToolCallArgumentsStream } from "../src/internal/streamingJson.ts";
import {
  type AssistantMessage,
  emptyUsage,
  type TextContent,
  type ToolCall,
} from "../src/Message.ts";
import type { AssistantMessageEvent } from "../src/Models.ts";

// #9265: partial commits reassigned the growing strings of a streamed answer, and the tracker compared each one whole.

interface Live {
  generation: JsonRecord;
}

const encodeJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));

/** The committed partial, as far as the test reads it. */
const Committed = Schema.Struct({
  content: Schema.Array(
    Schema.Struct({
      text: Schema.optional(Schema.String),
      arguments: Schema.optional(Schema.Json),
    }),
  ),
});

/**
 * A response streaming one text block (started empty) and then one tool call, as a provider produces it: the
 * partial message it mutates and the events it emits. Partial commits go into a tracked document, as the
 * kernel's generation does.
 */
function setup() {
  // The partial's blocks, which the provider replaces as they grow.
  const content: Array<TextContent | ToolCall> = [{ type: "text", text: "" }];
  const partial: AssistantMessage = {
    role: "assistant",
    content,
    api: "test",
    provider: "test",
    model: "test",
    usage: emptyUsage(),
    stopReason: "pending",
    timestamp: 0,
  };
  const changes = new PartialChanges();
  const tracker = Tracker.make<Live>({ generation: { attempt: 1 } });
  const args = new ToolCallArgumentsStream();
  const emit = (event: AssistantMessageEvent) => changes.observe(event);
  const ops: Array<Delta.Delta> = [];
  const textOf = () => (content[0]?.type === "text" ? content[0].text : "");
  const toolCall = (): ToolCall => {
    const block = content[1];
    assert.strictEqual(block?.type, "toolCall");
    // SAFETY: the assertion above checked the block is the tool call.
    return block as ToolCall;
  };
  return {
    content,
    tracker,
    ops,
    text(delta: string) {
      content[0] = { type: "text", text: textOf() + delta };
      emit({ type: "text_delta", contentIndex: 0, delta, partial });
    },
    argument(delta: string) {
      if (content[1] === undefined) {
        content.push({ type: "toolCall", id: "c1", name: "write", arguments: {} });
        emit({ type: "toolcall_start", contentIndex: 1, partial });
      }
      content[1] = { ...toolCall(), arguments: args.append(delta) };
      emit({ type: "toolcall_delta", contentIndex: 1, delta, partial });
    },
    /** The provider ends both blocks. */
    endBlocks() {
      emit({ type: "text_end", contentIndex: 0, content: textOf(), partial });
      emit({ type: "toolcall_end", contentIndex: 1, toolCall: toolCall(), partial });
    },
    /** One partial commit, as the generation's throttle makes it; `fail` aborts it like a rejected commit. */
    commit(fail = false) {
      // As the generation copies it: `Json.copy` returns untyped JSON, here a message's JSON representation.
      // SAFETY: a JSON copy of an `AssistantMessage`, whose `content` is an array.
      const copy = Json.copy(partial) as PartialMessage;
      const taken = changes.take(copy);
      const change = Tracker.beginChange(tracker);
      changes.commit(change.state.generation, copy, taken);
      if (fail) {
        Tracker.abort(change);
        changes.restore(taken);
        return;
      }
      const prepared = Tracker.prepare(change);
      ops.push([...prepared.ops]);
      Tracker.adopt(tracker, prepared);
    },
    committed() {
      const committed = Schema.decodeUnknownSync(Committed)(tracker.value.generation["message"]);
      return { text: committed.content[0]?.text, arguments: committed.content[1]?.arguments };
    },
  };
}

function pieces(text: string, size: number): Array<string> {
  const result: Array<string> = [];
  for (let index = 0; index < text.length; index += size)
    result.push(text.slice(index, index + size));
  return result;
}

const ARGUMENTS = {
  path: "src/a.ts",
  n: 1234,
  ok: true,
  content: 'line "one"\n\tline \\ two é 😀\n',
  edits: [
    { oldText: "a", newText: "b\nc" },
    { oldText: "", newText: "x".repeat(50) },
  ],
  nested: { deep: { list: [1, [2, "three"], { four: null }] } },
};

describe("PartialChanges", () => {
  it("commits tool-call arguments equal to the final parse, at every piece size and commit rhythm", () => {
    const text = encodeJson(ARGUMENTS);
    for (const size of [1, 2, 3, 5, 7, 16]) {
      for (const every of [1, 3, 10]) {
        const stream = setup();
        pieces(text, size).forEach((piece, index) => {
          stream.argument(piece);
          if (index % every === every - 1) stream.commit();
        });
        stream.commit();
        assert.deepStrictEqual(
          stream.committed().arguments,
          ARGUMENTS,
          `size ${size}, every ${every}`,
        );
      }
    }
  });

  it("handles a number ending and a string starting in one piece, and escapes split across pieces", () => {
    const stream = setup();
    for (const piece of [
      '{"n":1',
      '2,"s":"a',
      "b\\",
      'u00e9\\"',
      "q\\u",
      "00",
      "41",
      '","t":tr',
      "ue,",
      '"u":"\\ud83d',
      '\\ude00"}',
    ]) {
      stream.argument(piece);
      stream.commit();
    }
    assert.deepStrictEqual(stream.committed().arguments, { n: 12, s: 'abé"qA', t: true, u: "😀" });
  });

  it("appends growing argument and text strings instead of reassigning them", () => {
    const stream = setup();
    stream.text("Hello");
    stream.argument('{"path":"a.ts","content":"x');
    stream.commit();
    for (let index = 0; index < 20; index++) {
      stream.text(" more");
      stream.argument("yz");
      stream.commit();
    }
    for (const ops of stream.ops.slice(1)) {
      assert.deepStrictEqual(ops, [
        ["a", ["generation", "message", "content", 0, "text"], " more"],
        ["a", ["generation", "message", "content", 1, "arguments", "content"], "yz"],
      ]);
    }
    assert.deepStrictEqual(stream.committed(), {
      text: `Hello${" more".repeat(20)}`,
      arguments: { path: "a.ts", content: `x${"yz".repeat(20)}` },
    });
  });

  it("applies the changes of a failed commit in the next one", () => {
    const stream = setup();
    stream.argument('{"a":"x');
    stream.commit();
    stream.argument('y","b":[1');
    stream.commit(true);
    stream.argument(',2],"c":"z"}');
    stream.commit();
    assert.deepStrictEqual(stream.committed().arguments, { a: "xy", b: [1, 2], c: "z" });
  });

  it("re-applies a failed commit's changes even when it would have added the call", () => {
    const stream = setup();
    stream.commit();
    stream.argument('{"a":"x');
    stream.commit(true);
    stream.argument('y"');
    stream.commit();
    assert.deepStrictEqual(stream.committed().arguments, { a: "xy" });
  });

  it("falls back to the snapshot when the committed message lost a call whose changes were committed", () => {
    const stream = setup();
    stream.argument('{"a":"x');
    stream.commit();
    // Another writer resets the generation's partial.
    const change = Tracker.beginChange(stream.tracker);
    change.state.generation = { attempt: 1 };
    Tracker.adopt(stream.tracker, Tracker.prepare(change));
    stream.argument('y"');
    stream.commit();
    assert.deepStrictEqual(stream.committed().arguments, { a: "xy" });
    stream.argument(',"b":2}');
    stream.commit();
    assert.deepStrictEqual(stream.committed().arguments, { a: "xy", b: 2 });
  });

  it("commits the final arguments after the call ends, and drops text appends when a block ends", () => {
    const stream = setup();
    stream.text("draft");
    stream.argument('{"a":"x');
    stream.commit();
    // Providers may replace a block's text and arguments as it ends; the last commit assigns what they ended with.
    stream.text(" more");
    stream.content[0] = { type: "text", text: "replaced" };
    stream.content[1] = {
      type: "toolCall",
      id: "c1",
      name: "write",
      arguments: { a: "final" },
    };
    stream.endBlocks();
    stream.commit();
    assert.deepStrictEqual(stream.committed(), { text: "replaced", arguments: { a: "final" } });
  });
});

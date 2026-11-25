import { assert, describe, it } from "@effect/vitest";
import * as Schema from "effect/Schema";
import type { ToolExecutionResult } from "../src/AgentTool.ts";
import { EntryId, TaskId } from "../src/Ids.ts";
import type { ToolSlot } from "../src/Live.ts";
import type { ImageContent, ToolCall, ToolResultContent } from "../src/Message.ts";
import type * as Task from "../src/Task.ts";
import { ToolTask, ToolTaskInput } from "../src/internal/tool.ts";
import * as ToolPlan from "../src/internal/toolPlan.ts";

// Tests of the tool task's pure decisions: each is a function of what its commit loaded, so these check "given
// this, this plan" without a Harness. The Harness suites cover the same rules end to end.

const entryId = EntryId.make(7);
const call: ToolCall = { type: "toolCall", id: "c1", name: "work", arguments: {} };
const head = { maxBytes: 50 * 1024, maxLines: 2, retain: "head" } as const;
const tail = { maxBytes: 50 * 1024, maxLines: 2, retain: "tail" } as const;

/** Content items joined with `|`, an image as `[image]`. */
const text = (content: ToolResultContent): string =>
  content.map((item) => (item.type === "text" ? item.text : "[image]")).join("|");

const slot = (fields: Partial<ToolSlot>): ToolSlot => ({
  callId: "c1",
  name: "work",
  status: "running",
  taskId: TaskId.make(3),
  ...fields,
});

describe("decideReplay", () => {
  const safe = { replay: "safe" } as const;
  const unsafe = { replay: "unsafe" } as const;

  it("reruns only when the stored and the current policy are both safe", () => {
    const decision = ToolPlan.decideReplay("safe", safe, "work");
    assert.deepStrictEqual(decision, { _tag: "Rerun", tool: safe });
  });

  it("lets a current unsafe declaration veto a stored safe intent", () => {
    assert.strictEqual(ToolPlan.decideReplay("safe", unsafe, "work")._tag, "Interrupt");
    // An omitted `replay` is `unsafe`.
    assert.strictEqual(ToolPlan.decideReplay("safe", {}, "work")._tag, "Interrupt");
  });

  it("never upgrades a stored unsafe intent", () => {
    assert.strictEqual(ToolPlan.decideReplay("unsafe", safe, "work")._tag, "Interrupt");
  });

  it("treats a tool that no longer resolves as unsafe", () => {
    assert.strictEqual(ToolPlan.decideReplay("safe", undefined, "work")._tag, "Interrupt");
  });

  it("ends failed with the interrupted message", () => {
    const decision = ToolPlan.decideReplay("unsafe", undefined, "work");
    const message = "Tool work was interrupted and may have partially run";
    assert.deepStrictEqual(decision, {
      _tag: "Interrupt",
      ending: { status: "failed", message },
      code: "interrupted",
      message,
    });
  });
});

describe("toolAbortResult", () => {
  it("says aborted for an ordinary abort, whatever the phase", () => {
    const expected = { code: "aborted", message: "Tool work was aborted" };
    assert.deepStrictEqual(ToolPlan.toolAbortResult(undefined, "call", "work"), expected);
    assert.deepStrictEqual(ToolPlan.toolAbortResult(undefined, "execute", "work"), expected);
  });

  it("says interrupted for a call abandoned by a restart after its intent", () => {
    assert.deepStrictEqual(ToolPlan.toolAbortResult("restart", "execute", "work"), {
      code: "interrupted",
      message: "Tool work was interrupted by a restart and may have partially run",
    });
  });

  it("says abandoned for a call abandoned by a restart before its intent", () => {
    assert.deepStrictEqual(ToolPlan.toolAbortResult("restart", "call", "work"), {
      code: "abandoned",
      message: "Tool work was not started: its caller ended with a restart",
    });
  });
});

describe("toolOutcome", () => {
  // The result entry is the task's result for every ending.
  const result = { kind: "model", entryId } as const;

  it("completes, fails with the ending's message, or aborts, keeping the result", () => {
    assert.deepStrictEqual(ToolPlan.toolOutcome(ToolPlan.COMPLETED, result), {
      status: "terminal",
      outcome: { status: "completed", result },
    });
    assert.deepStrictEqual(
      ToolPlan.toolOutcome({ status: "failed", message: "Tool work threw" }, result),
      {
        status: "terminal",
        outcome: { status: "failed", error: { message: "Tool work threw" }, result },
      },
    );
    assert.deepStrictEqual(ToolPlan.toolOutcome({ status: "aborted" }, result), {
      status: "terminal",
      outcome: { status: "aborted", result },
    });
  });
});

describe("results the Harness writes", () => {
  it("writes an error result with no content and one error diagnostic", () => {
    // As for a call to a tool the Harness did not offer.
    const result = ToolPlan.harnessError("tool_unavailable", "Tool ghost is not available");
    assert.deepStrictEqual(result, {
      output: [],
      isError: true,
      diagnostics: [
        { severity: "error", code: "tool_unavailable", message: "Tool ghost is not available" },
      ],
    });
    const message = ToolPlan.toolResultMessage({ call, result, timing: { timestamp: 1 } });
    assert.strictEqual(
      text(message.content),
      "<harness>\n[error] Tool ghost is not available\n</harness>",
    );
  });

  it("builds an interrupted result from the slot's durable partials", () => {
    // harness-tools-recovery: "run 1\n|<harness>\n[error] Tool work was interrupted …\n</harness>".
    const message = "Tool work was interrupted and may have partially run";
    const result = ToolPlan.fromSlot(
      slot({ output: "run 1\n", details: { step: 1 } }),
      "interrupted",
      message,
    );
    assert.deepStrictEqual(result, {
      result: {
        output: [{ type: "text", text: "run 1\n" }],
        isError: true,
        details: { step: 1 },
        diagnostics: [{ severity: "error", code: "interrupted", message }],
      },
    });
    const stored = ToolPlan.toolResultMessage({
      call,
      result: result.result,
      timing: { timestamp: 1 },
    });
    assert.strictEqual(text(stored.content), `run 1\n|<harness>\n[error] ${message}\n</harness>`);
  });

  it("keeps the slot's diagnostics and reports its truncation apart, without a retain side", () => {
    // `retain` is unknown when rebuilt from a slot, and the truncation diagnostic is
    // added after `afterTool`, only while the hooks keep the slot's output.
    const result = ToolPlan.fromSlot(
      slot({
        output: "",
        droppedBytes: 9,
        droppedLines: 2,
        diagnostics: [{ severity: "info", message: "from api" }],
      }),
      "aborted",
      "Tool work was aborted",
    );
    assert.deepStrictEqual(result.result.output, []);
    assert.deepStrictEqual(result.result.diagnostics, [
      { severity: "info", message: "from api" },
      { severity: "error", code: "aborted", message: "Tool work was aborted" },
    ]);
    assert.deepStrictEqual(result.truncation, {
      severity: "warn",
      code: "truncated",
      message: "Output truncated: 2 lines, 9 bytes dropped",
    });
  });

  it("builds a result without a slot", () => {
    const result = ToolPlan.fromSlot(undefined, "aborted", "Tool work was aborted");
    assert.deepStrictEqual(result, {
      result: ToolPlan.harnessError("aborted", "Tool work was aborted"),
    });
  });
});

describe("settling a result", () => {
  it("uses retained output and the last details when the result omits them, with diagnostics in order", () => {
    // harness-tools "uses retained output and the last details …": maxLines 2 over three lines.
    const limits = { ...head, maxLines: 2 };
    const merged = ToolPlan.mergeReported(
      { diagnostics: [{ severity: "warn", message: "from result" }] },
      {
        snapshot: () => ({ text: "line 1\nline 2\n", droppedBytes: 7, droppedLines: 1 }),
        details: { step: 2 },
        diagnostics: [{ severity: "info", message: "from api" }],
      },
    );
    const settled = ToolPlan.boundResult({ merged, final: merged.result, limits });
    assert.deepStrictEqual(settled.details, { step: 2 });
    const message = ToolPlan.toolResultMessage({ call, result: settled, timing: { timestamp: 1 } });
    assert.strictEqual(
      text(message.content),
      "line 1\nline 2\n|<harness>\n[info] from api\n[warn] from result\n[warn] Output truncated to its beginning: 1 lines, 7 bytes dropped\n</harness>",
    );
  });

  it("drops the retained output's truncation once afterTool replaced the output", () => {
    const merged = ToolPlan.mergeReported(
      {},
      {
        snapshot: () => ({ text: "kept\n", droppedBytes: 5, droppedLines: 1 }),
        details: undefined,
        diagnostics: [],
      },
    );
    const final: ToolExecutionResult = {
      ...merged.result,
      output: [{ type: "text", text: "redacted" }],
    };
    const settled = ToolPlan.boundResult({ merged, final, limits: head });
    assert.deepStrictEqual(settled.diagnostics, []);
    assert.strictEqual(text(settled.output ?? []), "redacted");
  });

  it("keeps the result's own output and details over what was reported", () => {
    let read = false;
    const merged = ToolPlan.mergeReported(
      { output: [], details: { own: true } },
      {
        snapshot: () => {
          read = true;
          return { text: "ignored", droppedBytes: 0, droppedLines: 0 };
        },
        details: { reported: true },
        diagnostics: [],
      },
    );
    assert.isFalse(read);
    assert.isUndefined(merged.retained);
    assert.deepStrictEqual(merged.result.output, []);
    assert.deepStrictEqual(merged.result.details, { own: true });
  });

  it("bounds explicit text content and keeps other content", () => {
    // harness-tools "bounds explicit text content and keeps other content".
    const image: ImageContent = { type: "image", data: "AAAA", mimeType: "image/png" };
    const bounded = ToolPlan.boundContent(
      [{ type: "text", text: "a\nb\n" }, image, { type: "text", text: "c\nd\n" }],
      tail,
    );
    assert.strictEqual(text(bounded.content), "[image]|c\nd\n");
    assert.strictEqual(bounded.droppedLines, 2);
    assert.strictEqual(bounded.droppedBytes, 4);
    const settled = ToolPlan.boundResult({
      merged: { result: {}, retained: undefined },
      final: {
        output: [{ type: "text", text: "a\nb\n" }, image, { type: "text", text: "c\nd\n" }],
      },
      limits: tail,
    });
    const message = ToolPlan.toolResultMessage({ call, result: settled, timing: { timestamp: 1 } });
    assert.strictEqual(
      text(message.content),
      "[image]|c\nd\n|<harness>\n[warn] Output truncated to its end: 2 lines, 4 bytes dropped\n</harness>",
    );
  });

  it("keeps content within the limits as it is", () => {
    const content: ToolResultContent = [{ type: "text", text: "a\n" }];
    const bounded = ToolPlan.boundContent(content, head);
    assert.strictEqual(bounded.content, content);
    assert.strictEqual(bounded.droppedBytes, 0);
  });

  it("resolves output limits over the defaults", () => {
    const defaults = { maxBytes: 50 * 1024, maxLines: 2000, retain: "head" } as const;
    assert.deepStrictEqual(ToolPlan.resolveLimits(undefined, defaults), defaults);
    assert.deepStrictEqual(ToolPlan.resolveLimits({ maxLines: 1, retain: "tail" }, defaults), {
      maxBytes: 50 * 1024,
      maxLines: 1,
      retain: "tail",
    });
  });
});

const codes = (result: ToolExecutionResult) =>
  (result.diagnostics ?? []).map((diagnostic) => diagnostic.code);

describe("structured output", () => {
  // The nested cases are the rules `executeTool` gives callers.
  const Count: Schema.Codec<Schema.Json, Schema.Json> = Schema.Struct({ count: Schema.Number });
  const plain = { name: "plain" };
  const counter = { name: "counter", structuredOutput: Count };
  const image: ImageContent = { type: "image", data: "AAAA", mimeType: "image/png" };
  const one = { type: "text", text: "one" } as const;

  it("gives a schema-less tool's output as value: one text as a string, one image as itself, else the list", () => {
    const cases: ReadonlyArray<readonly [ToolResultContent, Schema.Json]> = [
      [[one], "one"],
      [[image], image],
      [[], ""],
      [
        [one, image],
        [one, image],
      ],
      [
        [one, one],
        [one, one],
      ],
    ];
    for (const [output, value] of cases) {
      const checked = ToolPlan.checkStructured({ kind: "nested", tool: plain, result: { output } });
      assert.deepStrictEqual(checked, {
        result: { output, structuredOutput: value },
        broken: undefined,
      });
      // A model-issued call gets no value built: the model reads only the output.
      const model = ToolPlan.checkStructured({ kind: "model", tool: plain, result: { output } });
      assert.deepStrictEqual(model.result, { output });
    }
  });

  it("keeps a schema tool's valid value, also a null one, and an error result's value", () => {
    const valid = ToolPlan.checkStructured({
      kind: "nested",
      tool: counter,
      result: { output: [], structuredOutput: { count: 3 } },
    });
    assert.deepStrictEqual(valid.result.structuredOutput, { count: 3 });
    const nothing = ToolPlan.checkStructured({
      kind: "nested",
      tool: { name: "nothing", structuredOutput: Schema.Null },
      result: { structuredOutput: null },
    });
    assert.isNull(nothing.result.structuredOutput);
    assert.isUndefined(nothing.broken);
    const failed = ToolPlan.checkStructured({
      kind: "nested",
      tool: counter,
      result: { output: [one], structuredOutput: { count: 0 }, isError: true },
    });
    assert.deepStrictEqual(failed.result, {
      output: [one],
      structuredOutput: { count: 0 },
      isError: true,
    });
  });

  it("turns a broken value into an error result for a nested call, and only drops it for the model's", () => {
    const cases: ReadonlyArray<readonly [ToolPlan.StructuredTool, ToolExecutionResult, string]> = [
      [
        counter,
        { structuredOutput: { count: "three" } },
        "Tool counter returned structuredOutput that does not match its schema: count: Expected number",
      ],
      [counter, { output: [] }, "Tool counter returned no structuredOutput"],
      [
        plain,
        { structuredOutput: { count: 3 } },
        "Tool plain returned structuredOutput but declares no structuredOutputSchema",
      ],
    ];
    for (const [tool, result, message] of cases) {
      const nested = ToolPlan.checkStructured({ kind: "nested", tool, result });
      assert.isTrue(nested.result.isError);
      assert.deepStrictEqual(codes(nested.result), ["invalid_structured_output"]);
      assert.include(nested.result.diagnostics?.[0]?.message, message);
      // The broken value is dropped; a schema-less tool's error result still carries its output, here none.
      if (tool.structuredOutput === undefined)
        assert.strictEqual(nested.result.structuredOutput, "");
      else assert.isFalse("structuredOutput" in nested.result);
      const model = ToolPlan.checkStructured({ kind: "model", tool, result });
      assert.include(model.broken, message);
      assert.isUndefined(model.result.isError);
      assert.isFalse("structuredOutput" in model.result);
      assert.deepStrictEqual(model.result.diagnostics, undefined);
    }
  });

  it("names the root of a value that is not an object", () => {
    const checked = ToolPlan.checkStructured({
      kind: "model",
      tool: counter,
      result: { structuredOutput: 3 },
    });
    assert.match(checked.broken ?? "", /does not match its schema: root: /);
  });

  it("gives a schema-less tool's error result its output as value", () => {
    const checked = ToolPlan.checkStructured({
      kind: "nested",
      tool: plain,
      result: { output: [{ type: "text", text: "bad" }], isError: true },
    });
    assert.deepStrictEqual(checked.result, {
      output: [{ type: "text", text: "bad" }],
      isError: true,
      structuredOutput: "bad",
    });
  });

  it("gives a schema tool's error text as a tool_error diagnostic, unless an error diagnostic says what went wrong", () => {
    const fails = ToolPlan.checkStructured({
      kind: "nested",
      tool: counter,
      result: { output: [{ type: "text", text: "nope" }], isError: true },
    });
    assert.deepStrictEqual(fails.result.diagnostics, [
      { severity: "error", code: "tool_error", message: "nope" },
    ]);
    assert.isFalse("structuredOutput" in fails.result);
    // A thrown error: its tool_error diagnostic, and no second one from its (empty) output.
    const thrown = ToolPlan.checkStructured({
      kind: "nested",
      tool: counter,
      result: {
        isError: true,
        diagnostics: [ToolPlan.toolDiagnostic("tool_error", "permission denied")],
      },
    });
    assert.deepStrictEqual(codes(thrown.result), ["tool_error"]);
    const explained = ToolPlan.checkStructured({
      kind: "nested",
      tool: counter,
      result: {
        output: [{ type: "text", text: "partial log" }],
        isError: true,
        diagnostics: [{ severity: "error", code: "quota", message: "quota exceeded" }],
      },
    });
    assert.deepStrictEqual(explained.result.diagnostics, [
      { severity: "error", code: "quota", message: "quota exceeded" },
    ]);
    // The model's call keeps the text in its output, without the extra diagnostic.
    const model = ToolPlan.checkStructured({
      kind: "model",
      tool: counter,
      result: { output: [{ type: "text", text: "nope" }], isError: true },
    });
    assert.isUndefined(model.result.diagnostics);
  });

  it("drops the hooks' value from a result the Harness wrote, and gives a nested success its output", () => {
    const written = ToolPlan.harnessError("blocked", "Tool call blocked: no");
    const input = { written, truncation: undefined, limits: head };
    const value = { ...written, structuredOutput: { count: 1 } };
    // An error result the Harness wrote has no value; its diagnostics say what went wrong.
    for (const kind of ["model", "nested"] as const) {
      const settled = ToolPlan.boundHarnessResult({
        ...input,
        final: value,
        kind,
        structured: false,
      });
      assert.isFalse("structuredOutput" in settled);
    }
    const success = { output: [one], structuredOutput: { count: 1 } };
    const nested = ToolPlan.boundHarnessResult({
      ...input,
      final: success,
      kind: "nested",
      structured: false,
    });
    assert.strictEqual(nested.structuredOutput, "one");
    const schema = ToolPlan.boundHarnessResult({
      ...input,
      final: success,
      kind: "nested",
      structured: true,
    });
    assert.isFalse("structuredOutput" in schema);
    const model = ToolPlan.boundHarnessResult({
      ...input,
      final: success,
      kind: "model",
      structured: false,
    });
    assert.isFalse("structuredOutput" in model);
  });
});

describe("toolResultMessage", () => {
  it("writes optional fields only when present, and a content without diagnostics as it is", () => {
    // Calls that did not execute have no `durationMs`.
    const bare = ToolPlan.toolResultMessage({
      call,
      result: { output: [] },
      timing: { timestamp: 5 },
    });
    assert.deepStrictEqual(bare, {
      role: "toolResult",
      toolCallId: "c1",
      toolName: "work",
      content: [],
      isError: false,
      timestamp: 5,
    });
    const usage = {
      input: 1,
      output: 2,
      cacheRead: 0,
      cacheWrite: 0,
      totalTokens: 3,
      cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
    };
    const full = ToolPlan.toolResultMessage({
      call,
      result: { output: [{ type: "text", text: "ok" }], details: { a: 1 }, usage, isError: true },
      timing: { timestamp: 5, durationMs: 12 },
    });
    assert.deepStrictEqual(full, {
      role: "toolResult",
      toolCallId: "c1",
      toolName: "work",
      content: [{ type: "text", text: "ok" }],
      details: { a: 1 },
      usage,
      isError: true,
      durationMs: 12,
      timestamp: 5,
    });
  });
});

describe("validationMessage", () => {
  it("renders the validation frame with dotted paths, root and the received arguments", () => {
    const message = ToolPlan.validationMessage({
      name: "echo",
      issues: [
        { path: ["text"], message: "Expected string, got 1" },
        { path: [{ key: "items" }, 0], message: "Missing key" },
        { message: "Expected object" },
      ],
      received: { text: 1 },
    });
    assert.strictEqual(
      message,
      'Validation failed for tool "echo":\n  - text: Expected string, got 1\n  - items.0: Missing key\n  - root: Expected object\n\nReceived arguments:\n{\n  "text": 1\n}',
    );
  });
});

describe("ToolTask", () => {
  it("migrates a version 1 input to a model-issued call", () => {
    const migrate = ToolTask.definition.migrate;
    assert.isDefined(migrate);
    const migrated = migrate?.({ assistant: 4, callId: "c1" }, { phase: "call" }, 1);
    assert.deepStrictEqual(migrated, {
      input: { kind: "model", assistant: EntryId.make(4), callId: "c1" },
      checkpoint: { phase: "call" },
    });
    assert.isTrue(Schema.is(ToolTaskInput)(migrated?.input));
  });

  it("requires nothing of the Harness layer", () => {
    // Tools' own requirements are the extensions', provided to the handler by the scheduler.
    const requirements: [Task.Requirements<typeof ToolTask>] extends [never] ? true : false = true;
    assert.isTrue(requirements);
  });
});

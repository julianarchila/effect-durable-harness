// A tool's `structuredOutput`, for programs that call it, on every backend.
//
// Notes:
// - The schema is the tool's `structuredOutput` `Schema`.
// - Tools are installed as extensions of `openChat`; reports' messages are the reported causes' error messages.
// - `direct` reads the call's result entry. Every model-issued half below is a test of its own, named with
//   "(model-issued call)"; the nested half stays in the shared case.
// - `nested` runs the call from a `probe` tool through `ToolContext.executeTool`; a thrown error is a typed
//   failure (`Boom`) of the tool's Effect.
// - A change to a document is a document change with ops (a creation has none).
//
// Not covered: `bash` and its environment ("gives callers of bash its retained output and exit code, also for a
// nonzero exit").
import { assert, it } from "@effect/vitest";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Schema from "effect/Schema";
import * as AgentTool from "../src/AgentTool.ts";
import {
  NestedCallDoc,
  type NestedCallState,
  type NestedToolExecutionResult,
  type ToolExecutionResult,
} from "../src/AgentTool.ts";
import { ToolResultEntry } from "../src/Entry.ts";
import * as Extension from "../src/Extension.ts";
import * as Hook from "../src/Hook.ts";
import type { AssistantMessage, ToolResultMessage } from "../src/Message.ts";
import type { Report } from "../src/Report.ts";
import type { Submission } from "../src/Submission.ts";
import { fauxAssistantMessage, fauxText, fauxToolCall } from "../src/testing/FauxModel.ts";
import { allEntries, type ChatSetup, chatSetup, openChat, waitFor } from "./ChatSupport.ts";
import { controlledStorage, describeEachBackend, documentChanges, run } from "./SessionSupport.ts";

const Count = Schema.Struct({ count: Schema.Number });

const Parameters = Schema.Struct({});

type Structured = Schema.Codec<Schema.Json, Schema.Json>;

/** A tool that returns `result`, with a `structuredOutput` schema when given. */
const returning = (name: string, result: ToolExecutionResult, schema?: Structured) =>
  AgentTool.make(name, {
    description: name,
    parameters: Parameters,
    structuredOutput: schema,
    execute: () => Effect.succeed(result),
  });

/** Installs `agentTool` as an extension of its own. */
const toolExtension = (agentTool: AgentTool.Any) =>
  Extension.make({ name: `tool:${agentTool.name}`, tools: [agentTool] });

const call = (name: string): AssistantMessage =>
  fauxAssistantMessage([fauxToolCall(name, {}, { id: "c1" })], { stopReason: "toolUse" });

const DONE = fauxAssistantMessage([fauxText("done")]);

/** Runs `effect` to its end while the `TestClock` advances. */
const finished = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  Effect.gen(function* () {
    const fiber = yield* Effect.forkChild(effect, { startImmediately: true });
    yield* waitFor(Effect.sync(() => fiber.pollUnsafe() !== undefined));
    return yield* Fiber.join(fiber);
  });

/** Awaits a submission while the `TestClock` advances. */
const settle = (submission: Submission) => finished(submission.await);

/** Runs `name` as a model-issued call and returns its transcript message. */
const direct = (setup: ChatSetup, name: string, extensions: ReadonlyArray<Extension.Any>) =>
  Effect.gen(function* () {
    setup.faux.setResponses([call(name), DONE]);
    const { root } = yield* openChat(yield* controlledStorage, setup, { extensions });
    yield* settle(yield* root.submit({ type: "input", content: "go" }));
    const entry = (yield* allEntries(root)).find((candidate) => ToolResultEntry.is(candidate));
    const message = entry?.model?.[0];
    assert.strictEqual(message?.role, "toolResult");
    // SAFETY: asserted just above.
    return message as ToolResultMessage;
  });

/** A typed failure carrying the message. */
class Boom extends Schema.TaggedError<Boom>()("Boom", { message: Schema.String }) {}

/** Runs `name` as a nested call of a `probe` tool and returns what the probe received. */
const nested = (setup: ChatSetup, name: string, extensions: ReadonlyArray<Extension.Any>) =>
  Effect.gen(function* () {
    let received: NestedToolExecutionResult | undefined;
    const probe = AgentTool.make("probe", {
      description: "probe",
      parameters: Parameters,
      execute: (_args, api) =>
        Effect.gen(function* () {
          received = yield* api.executeTool(name, {});
          return {};
        }),
    });
    setup.faux.setResponses([call("probe"), DONE]);
    const { harness, root } = yield* openChat(yield* controlledStorage, setup, {
      extensions: [...extensions, toolExtension(probe)],
    });
    yield* settle(yield* root.submit({ type: "input", content: "go" }));
    yield* harness.close;
    assert.isDefined(received);
    return received!;
  });

const codes = (result: {
  readonly diagnostics?: ReadonlyArray<{ readonly code?: string | undefined }> | undefined;
}): Array<string | undefined> => (result.diagnostics ?? []).map((diagnostic) => diagnostic.code);

/** The message of a reported cause. */
const reportMessage = (report: Report): string => {
  const error = Cause.squash(report.cause);
  return error instanceof Error ? error.message : String(error);
};

describeEachBackend("structured output", (backend) => {
  it.effect(
    "gives callers a schema-less tool's output: one text as a string, one image as itself, else the list",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const text = { type: "text" as const, text: "one" };
          const image = { type: "image" as const, data: "AAAA", mimeType: "image/png" };
          const cases: ReadonlyArray<
            readonly [output: ReadonlyArray<typeof text | typeof image>, value: Schema.Json]
          > = [
            [[text], "one"],
            [[image], image],
            [[], ""],
            [
              [text, image],
              [text, image],
            ],
            [
              [text, text],
              [text, text],
            ],
          ];
          for (const [output, value] of cases) {
            const setup = chatSetup();
            const received = yield* nested(setup, "plain", [
              toolExtension(returning("plain", { output })),
            ]);
            assert.isNumber(received.durationMs);
            assert.deepStrictEqual(received, {
              taskId: received.taskId,
              structuredOutput: value,
              isError: false,
              diagnostics: [],
              durationMs: received.durationMs!,
            });
          }
        }),
      ),
  );

  it.effect(
    "gives callers the bounded output, as the model sees it, without the rendered diagnostics",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const setup = chatSetup();
          const long = AgentTool.make("long", {
            description: "long",
            parameters: Parameters,
            outputLimits: { maxLines: 1 },
            execute: () =>
              Effect.succeed({
                output: [{ type: "text", text: "a\nb\nc" }],
                diagnostics: [{ severity: "info", message: "note" }],
              } satisfies ToolExecutionResult),
          });
          const received = yield* nested(setup, "long", [toolExtension(long)]);
          assert.strictEqual(received.structuredOutput, "a\n");
          assert.deepStrictEqual(codes(received), [undefined, "truncated"]);
        }),
      ),
  );

  it.effect(
    "stores exactly what callers and events get, without the model's output, and keeps a stored null",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const setup = chatSetup();
          const received: Array<NestedToolExecutionResult> = [];
          const probe = AgentTool.make("probe", {
            description: "probe",
            parameters: Parameters,
            execute: (_args, api) =>
              Effect.gen(function* () {
                received.push(yield* api.executeTool("plain", {}));
                received.push(yield* api.executeTool("nothing", {}));
                return {};
              }),
          });
          setup.faux.setResponses([call("probe"), DONE]);
          const { harness, root } = yield* openChat(yield* controlledStorage, setup, {
            extensions: [
              toolExtension(returning("plain", { output: [{ type: "text", text: "a" }] })),
              toolExtension(returning("nothing", { structuredOutput: null }, Schema.Null)),
              toolExtension(probe),
            ],
          });
          // The result each nested call's document got, as committed.
          const stored: Array<Schema.Json> = [];
          yield* harness.session.observeCommits({
            advance: (publication) => {
              for (const change of documentChanges(publication)) {
                if (
                  change.value === null ||
                  change.ops.length === 0 ||
                  change.record.kind !== NestedCallDoc.definition.kind
                ) {
                  continue;
                }
                // SAFETY: a `core.tool.nested-call` change carries a `NestedCallState`.
                const result = (change.value as NestedCallState).result;
                if (result !== undefined) stored.push(result);
              }
            },
            end: () => {},
          });
          yield* settle(yield* root.submit({ type: "input", content: "go" }));
          yield* finished(harness.harness.awaitIdle);
          yield* harness.close;

          assert.include(received[0], { structuredOutput: "a", isError: false });
          assert.include(received[1], { structuredOutput: null, isError: false });
          assert.isFalse("output" in received[0]!);
          // A client reads a live caller's nested results from the same documents.
          assert.deepStrictEqual(stored, received);
        }),
      ),
  );

  it.effect(
    "gives callers a schema tool's validated structured output, which the transcript does not store",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const setup = chatSetup();
          const result: ToolExecutionResult = {
            output: [{ type: "text", text: "3 things" }],
            structuredOutput: { count: 3 },
          };
          const received = yield* nested(setup, "counter", [
            toolExtension(returning("counter", result, Count)),
          ]);
          assert.deepStrictEqual(received.structuredOutput, { count: 3 });
          assert.strictEqual(received.isError, false);
        }),
      ),
  );

  it.effect(
    "gives callers a schema tool's validated structured output, which the transcript does not store (model-issued call)",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const setup = chatSetup();
          const result: ToolExecutionResult = {
            output: [{ type: "text", text: "3 things" }],
            structuredOutput: { count: 3 },
          };
          const counter = returning("counter", result, Count);
          const stored = yield* direct(setup, "counter", [toolExtension(counter)]);
          assert.strictEqual(stored.isError, false);
          assert.deepStrictEqual(stored.content, [{ type: "text", text: "3 things" }]);
          assert.isFalse("structuredOutput" in stored);
          assert.deepStrictEqual(setup.reports, []);
        }),
      ),
  );

  it.effect(
    "turns structured output that breaks the schema into an error result for callers, and a report for the model's calls",
    () =>
      run(backend)(
        Effect.gen(function* () {
          // The "wrong" message after the dotted path is Schema's (`toolPlan.ts` `structuredOutputError`).
          const cases: ReadonlyArray<
            readonly [name: string, result: ToolExecutionResult, schema: boolean, message: string]
          > = [
            [
              "wrong",
              { structuredOutput: { count: "three" } },
              true,
              "does not match its schema: count:",
            ],
            ["missing", { output: [] }, true, "returned no structuredOutput"],
            [
              "undeclared",
              { structuredOutput: { count: 3 } },
              false,
              "declares no structuredOutputSchema",
            ],
          ];
          for (const [name, result, schema, message] of cases) {
            const setup = chatSetup();
            const tool = returning(name, result, schema ? Count : undefined);
            const received = yield* nested(setup, name, [toolExtension(tool)]);
            assert.strictEqual(received.isError, true);
            // The broken value is dropped; a schema-less tool's error result still carries its output, here none.
            if (schema) assert.isFalse("structuredOutput" in received);
            else assert.strictEqual(received.structuredOutput, "");
            assert.deepStrictEqual(codes(received), ["invalid_structured_output"]);
            assert.include(received.diagnostics[0]?.message, message);
          }
        }),
      ),
  );

  it.effect(
    "turns structured output that breaks the schema into an error result for callers, and a report for the model's calls (model-issued call)",
    () =>
      run(backend)(
        Effect.gen(function* () {
          // The "wrong" message after the dotted path is Schema's (`toolPlan.ts` `structuredOutputError`).
          const cases: ReadonlyArray<
            readonly [name: string, result: ToolExecutionResult, schema: boolean, message: string]
          > = [
            [
              "wrong",
              { structuredOutput: { count: "three" } },
              true,
              "does not match its schema: count: Expected number",
            ],
            ["missing", { output: [] }, true, "returned no structuredOutput"],
            [
              "undeclared",
              { structuredOutput: { count: 3 } },
              false,
              "declares no structuredOutputSchema",
            ],
          ];
          for (const [name, result, schema, message] of cases) {
            const setup = chatSetup();
            const tool = returning(name, result, schema ? Count : undefined);
            // The model never sees structured output: its call stands, and the host gets a report.
            const stored = yield* direct(setup, name, [toolExtension(tool)]);
            assert.strictEqual(stored.isError, false);
            assert.notInclude(
              stored.content.map((item) => (item.type === "text" ? item.text : "")).join("\n"),
              message,
            );
            assert.include(setup.reports.map(reportMessage).join("\n"), message);
            // An `extension_error`, naming the tool's extension.
            const report = setup.reports.find((each) => reportMessage(each).includes(message));
            assert.strictEqual(report?.kind, "extension_error");
            assert.strictEqual(
              report?.kind === "extension_error" ? report.extension : undefined,
              `tool:${name}`,
            );
          }
        }),
      ),
  );

  it.effect("gives callers the output of an error result of a tool without a schema", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        const bad = returning("bad", { output: [{ type: "text", text: "bad" }], isError: true });
        const received = yield* nested(setup, "bad", [toolExtension(bad)]);
        assert.include(received, { isError: true, structuredOutput: "bad" });
      }),
    ),
  );

  it.effect(
    "gives callers no structured output for an error result the Harness wrote, only diagnostics",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const setup = chatSetup();
          const received = yield* nested(setup, "missing", []);
          assert.strictEqual(received.isError, true);
          assert.deepStrictEqual(codes(received), ["tool_unavailable"]);
          assert.isFalse("structuredOutput" in received);
        }),
      ),
  );

  it.effect(
    "gives callers a schema tool's error text as a diagnostic, never as structured output",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const failing = (result: ToolExecutionResult) =>
            toolExtension(returning("fails", result, Count));
          // Its text reaches the caller as an error diagnostic: structuredOutput is only ever the tool's own value.
          const received = yield* nested(chatSetup(), "fails", [
            failing({ output: [{ type: "text", text: "nope" }], isError: true }),
          ]);
          assert.strictEqual(received.isError, true);
          assert.deepStrictEqual(received.diagnostics, [
            { severity: "error", code: "tool_error", message: "nope" },
          ]);
          assert.isFalse("structuredOutput" in received);

          // A thrown error's message is in its tool_error diagnostic, not in the output.
          const throws = AgentTool.make("throws", {
            description: "throws",
            parameters: Parameters,
            structuredOutput: Count,
            execute: () => Effect.fail(new Boom({ message: "permission denied" })),
          });
          const thrownResult = yield* nested(chatSetup(), "throws", [toolExtension(throws)]);
          // One diagnostic: the thrown message, not a second one from its (empty) output.
          assert.strictEqual(thrownResult.isError, true);
          assert.deepStrictEqual(
            thrownResult.diagnostics.map((diagnostic) => [diagnostic.code, diagnostic.message]),
            [["tool_error", "permission denied"]],
          );
          assert.isFalse("structuredOutput" in thrownResult);

          // An error result whose own diagnostics say what went wrong gets no second one from its output.
          const explained = yield* nested(chatSetup(), "fails", [
            failing({
              output: [{ type: "text", text: "partial log" }],
              isError: true,
              diagnostics: [{ severity: "error", code: "quota", message: "quota exceeded" }],
            }),
          ]);
          assert.deepStrictEqual(explained.diagnostics, [
            { severity: "error", code: "quota", message: "quota exceeded" },
          ]);

          // An error result that has structured output keeps it.
          const kept = yield* nested(chatSetup(), "fails", [
            failing({
              output: [{ type: "text", text: "nope" }],
              structuredOutput: { count: 0 },
              isError: true,
            }),
          ]);
          assert.strictEqual(kept.isError, true);
          assert.deepStrictEqual(kept.structuredOutput, { count: 0 });
        }),
      ),
  );

  it.effect(
    "gives callers a schema tool's error text as a diagnostic, never as structured output (model-issued call)",
    () =>
      run(backend)(
        Effect.gen(function* () {
          // The model's call keeps the text in its output, without the extra diagnostic.
          const setup = chatSetup();
          const fails = returning(
            "fails",
            { output: [{ type: "text", text: "nope" }], isError: true },
            Count,
          );
          const stored = yield* direct(setup, "fails", [toolExtension(fails)]);
          assert.deepStrictEqual(stored.content, [{ type: "text", text: "nope" }]);
          assert.deepStrictEqual(setup.reports, []);
        }),
      ),
  );

  it.effect(
    "keeps structured output when afterTool replaces the output, and validates one afterTool replaces",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const setup = chatSetup();
          const hooks = Hook.tool({
            afterTool: (hookCall, result) =>
              Effect.succeed(
                hookCall.name === "other"
                  ? { ...result, structuredOutput: { count: "broken" } }
                  : hookCall.name === "probe"
                    ? result
                    : { ...result, output: [{ type: "text" as const, text: "rewritten" }] },
              ),
          });
          let plain: NestedToolExecutionResult | undefined;
          let counter: NestedToolExecutionResult | undefined;
          let other: NestedToolExecutionResult | undefined;
          const probe = AgentTool.make("probe", {
            description: "probe",
            parameters: Parameters,
            execute: (_args, api) =>
              Effect.gen(function* () {
                plain = yield* api.executeTool("plain", {});
                counter = yield* api.executeTool("counter", {});
                other = yield* api.executeTool("other", {});
                return {};
              }),
          });
          setup.faux.setResponses([call("probe"), DONE]);
          const { harness, root } = yield* openChat(yield* controlledStorage, setup, {
            extensions: [
              toolExtension(returning("plain", { output: [{ type: "text", text: "secret" }] })),
              toolExtension(returning("counter", { structuredOutput: { count: 1 } }, Count)),
              toolExtension(returning("other", { structuredOutput: { count: 1 } }, Count)),
              Extension.make({ name: "hooks", hooks: [hooks] }),
              toolExtension(probe),
            ],
          });
          yield* settle(yield* root.submit({ type: "input", content: "go" }));
          yield* harness.close;
          // A schema-less tool's value is its output, so redacting the output redacts what programs get.
          assert.strictEqual(plain?.structuredOutput, "rewritten");
          assert.deepStrictEqual(counter?.structuredOutput, { count: 1 });
          assert.strictEqual(other?.isError, true);
          assert.deepStrictEqual(codes(other!), ["invalid_structured_output"]);
        }),
      ),
  );

  // The case above runs only nested calls. On a model-issued call, the value `afterTool` leaves is the one
  // validated.
  it.effect("validates the structured output afterTool leaves on a model-issued call", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        const counter = returning("counter", { structuredOutput: { count: 1 } }, Count);
        const breaks = Hook.tool({
          afterTool: (_call, result) =>
            Effect.succeed({ ...result, structuredOutput: { count: "broken" } }),
        });
        const stored = yield* direct(setup, "counter", [
          toolExtension(counter),
          Extension.make({ name: "hooks", hooks: [breaks] }),
        ]);
        assert.strictEqual(stored.isError, false);
        assert.include(setup.reports.map(reportMessage).join("\n"), "does not match its schema");
      }),
    ),
  );

  it.effect.skip(
    "gives callers of bash its retained output and exit code, also for a nonzero exit",
    () => Effect.void,
  );
});

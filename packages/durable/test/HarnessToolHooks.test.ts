// `afterTool` runs on every result a tool task writes, also those the Harness decides without executing the tool,
// on every backend.
//
// Notes:
// - Tools and hooks are extensions of `openChat`: a tool is an extension `tool:<name>`, hooks one holding them, in
//   registration order. A hook's API is its `HookContext`.
// - A crash uses `HarnessSupport.killable`: a killed Harness writes nothing more, and the next Harness opens over
//   the same storage, as a reopened process would.
// - A call waiting on its abort signal is a tool or hook that never returns until its fiber is interrupted.
// - Reports are causes; their messages are the causes' error messages.
// - "keep their flag in a version 2 generation's sequential round, also one migrated from version 1": the stored
//   record is committed straight to the backend's storage.
// - Nested calls are `ToolContext.executeTool`; a nested call's hook sees its caller in `Hook.ToolHookCall.parent`.
import { assert, it } from "@effect/vitest";
import * as Cause from "effect/Cause";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as AgentTool from "../src/AgentTool.ts";
import { AgentStorage } from "../src/AgentStorage.ts";
import {
  type NestedToolExecutionResult,
  ToolContext,
  type ToolExecutionResult,
} from "../src/AgentTool.ts";
import { ResetEntry, ToolResultEntry } from "../src/Entry.ts";
import * as Extension from "../src/Extension.ts";
import type { Conversation } from "../src/Harness.ts";
import * as Hook from "../src/Hook.ts";
import type { ToolHookCall } from "../src/Hook.ts";
import { ROOT_CONVERSATION_ID, type SubmissionId, type TaskId } from "../src/Ids.ts";
import { LiveDoc } from "../src/Live.ts";
import type { AssistantMessage, ToolResultMessage } from "../src/Message.ts";
import type { EntryRecord } from "../src/Records.ts";
import type { Submission } from "../src/Submission.ts";
import {
  type FauxResponseStep,
  fauxAssistantMessage,
  fauxText,
  fauxToolCall,
} from "../src/testing/FauxModel.ts";
import { commitWrites } from "../src/testing/CommitWrites.ts";
import { allEntries, type ChatSetup, chatSetup, openChat, stored, waitFor } from "./ChatSupport.ts";
import { killable, type TestHarness } from "./HarnessSupport.ts";
import { controlledStorage, describeEachBackend, run } from "./SessionSupport.ts";
import type { Report } from "../src/Report.ts";

type Inner = AgentStorage["Service"];

const Parameters = Schema.Struct({ text: Schema.optionalKey(Schema.String) });
type Args = typeof Parameters.Type;
type Extra = Omit<
  AgentTool.MakeOptions<Args, Schema.Json, never, never>,
  "description" | "parameters" | "execute"
>;

/** A tool with an optional `text` parameter, described by its name. */
const tool = <E>(
  name: string,
  execute: () => Effect.Effect<ToolExecutionResult, E, ToolContext>,
  extra: Extra = {},
) => AgentTool.make(name, { description: name, parameters: Parameters, execute, ...extra });

/** Installs `agentTool` as an extension of its own. */
const toolExtension = (agentTool: AgentTool.Any) =>
  Extension.make({ name: `tool:${agentTool.name}`, tools: [agentTool] });

let hookExtensions = 0;
/** Installs `hook` as an extension of its own. */
const hookExtension = (hook: Hook.Any) =>
  Extension.make({ name: `hooks:${++hookExtensions}`, hooks: [hook] });

const calls = (
  ...list: ReadonlyArray<readonly [string, Record<string, Schema.Json>, string]>
): AssistantMessage =>
  fauxAssistantMessage(
    list.map(([name, args, id]) => fauxToolCall(name, args, { id })),
    { stopReason: "toolUse" },
  );

const DONE = fauxAssistantMessage([fauxText("done")]);

const text = (value: string) => ({ type: "text" as const, text: value });

const results = (entries: ReadonlyArray<EntryRecord>): Map<string, ToolResultMessage> => {
  const messages = entries.flatMap((entry) => {
    const message = entry.model?.[0];
    return ToolResultEntry.is(entry) && message?.role === "toolResult" ? [message] : [];
  });
  return new Map(messages.map((message) => [message.toolCallId, message]));
};

const textOf = (message: ToolResultMessage | undefined): string =>
  (message?.content ?? []).map((item) => (item.type === "text" ? item.text : "")).join("|");

const codes = (result: {
  readonly diagnostics?: ReadonlyArray<{ readonly code?: string | undefined }> | undefined;
}): Array<string | undefined> => (result.diagnostics ?? []).map((diagnostic) => diagnostic.code);

const DiagnosticsData = Schema.Struct({
  diagnostics: Schema.Array(Schema.Struct({ code: Schema.optionalKey(Schema.String) })),
});

const diagnosticsOf = (entries: ReadonlyArray<EntryRecord>, callId: string) => {
  const entry = entries.find((each) => {
    const message = each.model?.[0];
    return (
      ToolResultEntry.is(each) && message?.role === "toolResult" && message.toolCallId === callId
    );
  });
  const data = Schema.decodeUnknownOption(DiagnosticsData)(entry?.data);
  return Option.isSome(data) ? codes(data.value) : [];
};

interface Seen {
  readonly call: ToolHookCall;
  readonly result: ToolExecutionResult;
}

/** Runs `effect` to its end while the `TestClock` advances. */
const finished = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  Effect.gen(function* () {
    const fiber = yield* Effect.forkChild(effect, { startImmediately: true });
    yield* waitFor(Effect.sync(() => fiber.pollUnsafe() !== undefined));
    return yield* Fiber.join(fiber);
  });

/** Awaits a submission while the `TestClock` advances. */
const settle = (submission: Submission) => finished(submission.await);

/** Awaits the submission `id`. */
const settled = (h: TestHarness, id: SubmissionId) =>
  Effect.flatMap(h.harness.submission(id), (found) => settle(Option.getOrThrow(found)));

/** A chat over `inner`, resumed; `kill` simulates a crash. */
const open = (inner: Inner, setup: ChatSetup, extensions: ReadonlyArray<Extension.Any>) =>
  Effect.gen(function* () {
    const crashable = yield* killable(inner);
    const opened = yield* openChat(crashable.storage, setup, { extensions });
    yield* opened.harness.harness.resume;
    return { ...opened, kill: crashable.kill.pipe(Effect.andThen(opened.harness.close)) };
  });

/** The task of the round's slot `index`, once listed. */
const toolTaskId = (h: TestHarness, index = 0) =>
  Effect.gen(function* () {
    let id: TaskId | undefined;
    yield* waitFor(
      h.session.snapshot(LiveDoc, ROOT_CONVERSATION_ID).pipe(
        Effect.map((live) => {
          id = Option.getOrUndefined(live)?.tools?.[index]?.taskId;
          return id !== undefined;
        }),
      ),
    );
    return id!;
  });

/** The message of a reported cause. */
const reportMessage = (report: Report): string => {
  const error = Cause.squash(report.cause);
  return error instanceof Error ? error.message : String(error);
};

class Boom extends Schema.TaggedError<Boom>()("Boom", { message: Schema.String }) {}

/** A tool that writes output and details, then never returns until its invocation is cancelled. */
const blocking = (name: string, extra: Extra = {}) => {
  const started = Deferred.makeUnsafe<void>();
  const state = { runs: 0 };
  const registration = tool(
    name,
    () =>
      Effect.gen(function* () {
        state.runs++;
        const api = yield* ToolContext;
        yield* api.output(`${name} ${"o".repeat(40)}\n`);
        yield* api.details({ run: state.runs });
        yield* Deferred.succeed(started, undefined);
        return yield* Effect.never;
      }),
    extra,
  );
  return { registration, started, state };
};

describeEachBackend("afterTool on results the call phase decides", (backend) => {
  it.effect(
    "sees and replaces unavailable, invalid, and blocked results, with the raw arguments",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const setup = chatSetup();
          let ran = 0;
          const echo = tool("echo", () =>
            Effect.sync(() => {
              ran++;
              return { output: [text("ran")] };
            }),
          );
          const gone = tool("gone", () => Effect.succeed({ output: [] }));
          const seen: Array<Seen> = [];
          const hooks = Hook.tool({
            beforeTool: (call) =>
              Effect.succeed(
                call.id === "block"
                  ? { block: "not today" }
                  : call.id === "rewrite"
                    ? { arguments: { text: { not: "a string" } } }
                    : undefined,
              ),
            afterTool: (call, result) =>
              Effect.sync(() => {
                seen.push({ call, result });
                return { ...result, output: [text(`hooked ${call.id}`)], details: { by: "hook" } };
              }),
          });
          let root: Conversation | undefined;
          // The tool is offered but no longer resolves when its task runs.
          const respond: FauxResponseStep = () =>
            Effect.suspend(() => root!.configure({ tools: [echo] })).pipe(
              Effect.orDie,
              Effect.as(
                calls(
                  ["gone", {}, "unavailable"],
                  ["echo", { text: { not: "a string" } }, "invalid"],
                  ["echo", { text: "x" }, "rewrite"],
                  ["echo", {}, "block"],
                ),
              ),
            );
          setup.faux.setResponses([respond, DONE]);
          const opened = yield* openChat(yield* controlledStorage, setup, {
            extensions: [toolExtension(echo), toolExtension(gone), hookExtension(hooks)],
          });
          root = opened.root;
          const done = yield* settle(yield* root.submit({ type: "input", content: "go" }));
          assert.strictEqual(done.status, "done");
          assert.strictEqual(ran, 0);
          const byId = new Map(seen.map((each) => [each.call.id, each]));
          assert.deepStrictEqual([...byId.keys()].toSorted(), [
            "block",
            "invalid",
            "rewrite",
            "unavailable",
          ]);
          assert.deepStrictEqual(codes(byId.get("unavailable")!.result), ["tool_unavailable"]);
          assert.deepStrictEqual(codes(byId.get("invalid")!.result), ["invalid_arguments"]);
          assert.deepStrictEqual(codes(byId.get("rewrite")!.result), ["invalid_arguments"]);
          assert.deepStrictEqual(codes(byId.get("block")!.result), ["blocked"]);
          // Hooks see the call as the model made it, also when it is invalid or `beforeTool` rewrote it.
          assert.deepStrictEqual(byId.get("invalid")!.call.arguments, {
            text: { not: "a string" },
          });
          assert.deepStrictEqual(byId.get("rewrite")!.call.arguments, { text: "x" });
          const messages = results(yield* allEntries(root));
          for (const id of ["unavailable", "invalid", "rewrite", "block"]) {
            const message = messages.get(id)!;
            assert.strictEqual(message.isError, true);
            assert.deepStrictEqual(message.details, { by: "hook" });
            assert.isTrue(textOf(message).startsWith(`hooked ${id}|<harness>`));
          }
          yield* opened.harness.close;
        }),
      ),
  );

  it.effect(
    "bounds a replacement output with the tool's limits, or the defaults for an unresolved tool",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const setup = chatSetup();
          const small = tool("small", () => Effect.succeed({ output: [] }), {
            outputLimits: { maxBytes: 10 },
          });
          const hooks = Hook.tool({
            beforeTool: () => Effect.succeed({ block: "no" }),
            afterTool: (call, result) =>
              Effect.succeed({
                ...result,
                output: [text(call.id === "c2" ? "x".repeat(100) : "y".repeat(60 * 1024))],
              }),
          });
          let root: Conversation | undefined;
          const respond: FauxResponseStep = () =>
            Effect.suspend(() => root!.configure({ tools: [] })).pipe(
              Effect.orDie,
              Effect.as(calls(["small", {}, "c1"])),
            );
          setup.faux.setResponses([respond, DONE]);
          const opened = yield* openChat(yield* controlledStorage, setup, {
            extensions: [toolExtension(small), hookExtension(hooks)],
          });
          root = opened.root;
          yield* settle(yield* root.submit({ type: "input", content: "go" }));
          yield* root.configure({ tools: null });
          setup.faux.setResponses([calls(["small", {}, "c2"]), DONE]);
          yield* settle(yield* root.submit({ type: "input", content: "again" }));
          const entries = yield* allEntries(root);
          const messages = results(entries);
          // Unresolved: the default 50 KB.
          const unresolved = messages.get("c1")!.content[0];
          assert.strictEqual(unresolved?.type === "text" && unresolved.text.length, 50 * 1024);
          assert.deepStrictEqual(diagnosticsOf(entries, "c1"), ["tool_unavailable", "truncated"]);
          const bounded = messages.get("c2")!.content[0];
          assert.strictEqual(bounded?.type === "text" && bounded.text, "x".repeat(10));
          assert.deepStrictEqual(diagnosticsOf(entries, "c2"), ["blocked", "truncated"]);
          yield* opened.harness.close;
        }),
      ),
  );

  it.effect("lets a hook's terminate on a blocked call count toward the round", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        const echo = tool("echo", () =>
          Effect.succeed({ output: [], control: { terminate: true } }),
        );
        const hooks = Hook.tool({
          beforeTool: (call) => Effect.succeed(call.id === "c2" ? { block: "no" } : undefined),
          afterTool: (_call, result) => Effect.succeed({ ...result, control: { terminate: true } }),
        });
        setup.faux.setResponses([calls(["echo", {}, "c1"], ["echo", {}, "c2"]), DONE]);
        const { harness, root } = yield* openChat(yield* controlledStorage, setup, {
          extensions: [toolExtension(echo), hookExtension(hooks)],
        });
        const done = yield* settle(yield* root.submit({ type: "input", content: "go" }));
        assert.strictEqual(done.status, "done");
        // The round terminated: no second request.
        assert.strictEqual(setup.faux.state.callCount, 1);
        yield* harness.close;
      }),
    ),
  );

  it.effect(
    "runs on nested invalid and blocked calls; the caller gets diagnostics and details, not output",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const setup = chatSetup();
          const echo = tool("echo", () => Effect.succeed({ output: [] }));
          const nested: Array<NestedToolExecutionResult> = [];
          const batch = tool("batch", () =>
            Effect.gen(function* () {
              const api = yield* ToolContext;
              nested.push(yield* api.executeTool("echo", { text: { not: "a string" } }));
              nested.push(yield* api.executeTool("echo", { text: "secret" }));
              nested.push(yield* api.executeTool("missing", {}));
              return {};
            }),
          );
          const seen: Array<ToolHookCall> = [];
          const hooks = Hook.tool({
            beforeTool: (call) =>
              Effect.succeed(
                call.arguments["text"] === "secret" ? { block: "no secrets" } : undefined,
              ),
            afterTool: (call, result) =>
              Effect.sync(() => {
                if (call.parent === undefined) return undefined;
                seen.push(call);
                return {
                  ...result,
                  output: [text("dropped")],
                  structuredOutput: { dropped: true },
                  details: { hooked: call.id },
                  diagnostics: [
                    ...(result.diagnostics ?? []),
                    { severity: "info" as const, code: "hooked", message: "seen" },
                  ],
                };
              }),
          });
          setup.faux.setResponses([calls(["batch", {}, "c1"]), DONE]);
          const { harness, root } = yield* openChat(yield* controlledStorage, setup, {
            extensions: [toolExtension(echo), toolExtension(batch), hookExtension(hooks)],
          });
          yield* settle(yield* root.submit({ type: "input", content: "go" }));
          assert.deepStrictEqual(
            seen.map((call) => call.id),
            ["c1/1", "c1/2", "c1/3"],
          );
          assert.isTrue(seen.every((call) => call.parent?.callId === "c1"));
          assert.deepStrictEqual(
            nested.map((result) => [result.isError, codes(result)]),
            [
              [true, ["invalid_arguments", "hooked"]],
              [true, ["blocked", "hooked"]],
              [true, ["tool_unavailable", "hooked"]],
            ],
          );
          assert.deepStrictEqual(
            nested.map((result) => result.details),
            [{ hooked: "c1/1" }, { hooked: "c1/2" }, { hooked: "c1/3" }],
          );
          // A Harness error result has no structured output, even when a hook supplies one.
          assert.isUndefined(nested[1]!.structuredOutput);
          assert.isUndefined(nested[2]!.structuredOutput);
          yield* harness.close;
        }),
      ),
  );

  it.effect(
    "gives a nested call a hook turns into a success its output as value, unless the tool has a schema",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const setup = chatSetup();
          const plain = tool("plain", () => Effect.succeed({ output: [] }));
          const typed = AgentTool.make("typed", {
            description: "typed",
            parameters: Parameters,
            structuredOutput: Schema.Struct({ ok: Schema.Boolean }),
            execute: () => Effect.succeed({ output: [], structuredOutput: { ok: true } }),
          });
          const nested: Array<NestedToolExecutionResult> = [];
          const batch = tool("batch", () =>
            Effect.gen(function* () {
              const api = yield* ToolContext;
              nested.push(yield* api.executeTool("plain", {}));
              nested.push(yield* api.executeTool("typed", {}));
              return {};
            }),
          );
          const hooks = Hook.tool({
            beforeTool: (call) =>
              Effect.succeed(call.parent === undefined ? undefined : { block: "no" }),
            afterTool: (call) =>
              Effect.succeed(
                call.parent === undefined
                  ? undefined
                  : { output: [text(`fine ${call.name}`)], structuredOutput: { ok: false } },
              ),
          });
          setup.faux.setResponses([calls(["batch", {}, "c1"]), DONE]);
          const { harness, root } = yield* openChat(yield* controlledStorage, setup, {
            extensions: [
              toolExtension(plain),
              toolExtension(typed),
              toolExtension(batch),
              hookExtension(hooks),
            ],
          });
          yield* settle(yield* root.submit({ type: "input", content: "go" }));
          assert.deepStrictEqual(
            nested.map((result) => [result.isError, result.structuredOutput]),
            [
              [false, "fine plain"],
              [false, undefined],
            ],
          );
          yield* harness.close;
        }),
      ),
  );
});

describeEachBackend("afterTool on results rebuilt from the slot", (backend) => {
  it.effect(
    "runs on an interrupted call; a replaced output loses the slot's truncation diagnostic",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const inner = yield* AgentStorage;
          const setup = chatSetup();
          setup.settings.progress = { outputIntervalMs: 0, persistWithinMs: 0 };
          const limits = { outputLimits: { maxBytes: 10, retain: "tail" } } as const;
          const work = blocking("work", limits);
          const keep = blocking("keep", limits);
          const seen: Array<Seen> = [];
          const hooks = Hook.tool({
            afterTool: (call, result) =>
              Effect.sync(() => {
                seen.push({ call, result });
                return call.name === "work" ? { ...result, output: [text("redacted")] } : undefined;
              }),
          });
          const extensions = [
            toolExtension(work.registration),
            toolExtension(keep.registration),
            hookExtension(hooks),
          ];
          setup.faux.setResponses([calls(["work", {}, "c1"], ["keep", {}, "c2"]), DONE]);
          let opened = yield* open(inner, setup, extensions);
          const id = (yield* opened.root.submit({ type: "input", content: "go" })).id;
          yield* waitFor(Deferred.isDone(work.started));
          yield* waitFor(Deferred.isDone(keep.started));
          yield* stored;
          yield* opened.kill;

          opened = yield* open(inner, setup, extensions);
          assert.strictEqual((yield* settled(opened.harness, id)).status, "done");
          assert.deepStrictEqual(
            seen.map((each) => [each.call.id, codes(each.result)]).toSorted(),
            [
              ["c1", ["interrupted"]],
              ["c2", ["interrupted"]],
            ],
          );
          const entries = yield* allEntries(opened.root);
          const messages = results(entries);
          assert.isTrue(textOf(messages.get("c1")).startsWith("redacted|"));
          assert.strictEqual(messages.get("c1")?.isError, true);
          assert.deepStrictEqual(messages.get("c1")?.details, { run: 1 });
          assert.deepStrictEqual(diagnosticsOf(entries, "c1"), ["interrupted"]);
          // The kept output keeps its truncation diagnostic, after the hooks' result.
          assert.deepStrictEqual(diagnosticsOf(entries, "c2"), ["interrupted", "truncated"]);
          yield* opened.harness.close;
        }),
      ),
  );

  it.effect(
    "runs on an abort result, but not on the aborted results of calls that never started",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const setup = chatSetup();
          setup.settings.toolExecution = "sequential";
          const first = blocking("first");
          const second = tool("second", () => Effect.succeed({ output: [] }));
          const seen: Array<Seen> = [];
          const hooks = Hook.tool({
            afterTool: (call, result) =>
              Effect.sync(() => {
                seen.push({ call, result });
                return { ...result, details: { hooked: true } };
              }),
          });
          setup.faux.setResponses([calls(["first", {}, "c1"], ["second", {}, "c2"]), DONE]);
          const { harness, root } = yield* openChat(yield* controlledStorage, setup, {
            extensions: [
              toolExtension(first.registration),
              toolExtension(second),
              hookExtension(hooks),
            ],
          });
          const submission = yield* root.submit({ type: "input", content: "go" });
          yield* waitFor(Deferred.isDone(first.started));
          yield* finished(root.abort());
          assert.strictEqual((yield* settle(submission)).status, "unanswered");
          assert.deepStrictEqual(
            seen.map((each) => [each.call.id, codes(each.result)]),
            [["c1", ["aborted"]]],
          );
          const messages = results(yield* allEntries(root));
          assert.strictEqual(messages.get("c1")?.isError, true);
          assert.deepStrictEqual(messages.get("c1")?.details, { hooked: true });
          assert.isUndefined(messages.get("c2")?.details);
          yield* harness.close;
        }),
      ),
  );

  it.effect("reports a throwing hook on an abort result and settles the result as built", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        const work = blocking("work");
        const hooks = Hook.tool({
          afterTool: () => Effect.fail(new Boom({ message: "hook failed" })),
        });
        setup.faux.setResponses([calls(["work", {}, "c1"]), DONE]);
        const { harness, root } = yield* openChat(yield* controlledStorage, setup, {
          extensions: [toolExtension(work.registration), hookExtension(hooks)],
        });
        const submission = yield* root.submit({ type: "input", content: "go" });
        yield* waitFor(Deferred.isDone(work.started));
        yield* finished(root.abort());
        assert.strictEqual((yield* settle(submission)).status, "unanswered");
        const entries = yield* allEntries(root);
        assert.isTrue(textOf(results(entries).get("c1")).startsWith(`work ${"o".repeat(40)}`));
        assert.deepStrictEqual(diagnosticsOf(entries, "c1"), ["aborted"]);
        assert.include(setup.reports.map(reportMessage), "hook failed");
        yield* harness.close;
      }),
    ),
  );

  it.effect("settles an abort without the hooks in the abort handler's last attempt", () =>
    run(backend)(
      Effect.gen(function* () {
        const inner = yield* AgentStorage;
        const setup = chatSetup();
        setup.settings.restartLimit = 2;
        setup.settings.progress = { outputIntervalMs: 0, persistWithinMs: 0 };
        const work = blocking("work");
        const seen: Array<Seen> = [];
        const hanging = Deferred.makeUnsafe<void>();
        const hooks = Hook.tool({
          afterTool: (call, result) =>
            Effect.gen(function* () {
              seen.push({ call, result });
              yield* Deferred.succeed(hanging, undefined);
              // Never returns: the host dies inside the hook.
              return yield* Effect.never;
            }),
        });
        const extensions = [toolExtension(work.registration), hookExtension(hooks)];
        setup.faux.setResponses([calls(["work", {}, "c1"]), DONE]);
        let opened = yield* open(inner, setup, extensions);
        const id = (yield* opened.root.submit({ type: "input", content: "go" })).id;
        yield* waitFor(Deferred.isDone(work.started));
        const taskId = yield* toolTaskId(opened.harness);
        yield* Effect.forkDetach(Effect.ignore(opened.root.abort()), { startImmediately: true });
        yield* waitFor(Deferred.isDone(hanging));
        yield* stored;
        yield* opened.kill;

        // One restart, at the limit minus one: the attempt the scheduler would fault when cut off runs no hooks.
        opened = yield* open(inner, setup, extensions);
        assert.strictEqual((yield* settled(opened.harness, id)).status, "unanswered");
        assert.deepStrictEqual(
          seen.map((each) => codes(each.result)),
          [["aborted"]],
        );
        const entries = yield* allEntries(opened.root);
        assert.isTrue(textOf(results(entries).get("c1")).startsWith(`work ${"o".repeat(40)}`));
        assert.deepStrictEqual(diagnosticsOf(entries, "c1"), ["aborted"]);
        const record = Option.getOrUndefined(yield* opened.harness.harness.getTask(taskId));
        assert.strictEqual(
          record?.state.status === "terminal" ? record.state.outcome.status : undefined,
          "aborted",
        );
        yield* opened.harness.close;
      }),
    ),
  );

  it.effect("sees the held progress of an aborted call, and the result stores it", () =>
    run(backend)(
      Effect.gen(function* () {
        const inner = yield* AgentStorage;
        const setup = chatSetup();
        setup.settings.progress = { outputIntervalMs: 0, persistWithinMs: 60_000 };
        const work = blocking("work");
        const seen: Array<Seen> = [];
        const hooks = Hook.tool({
          afterTool: (call, result) =>
            Effect.sync(() => {
              seen.push({ call, result });
              return undefined;
            }),
        });
        const extensions = [toolExtension(work.registration), hookExtension(hooks)];
        setup.faux.setResponses([calls(["work", {}, "c1"]), DONE]);
        let opened = yield* open(inner, setup, extensions);
        const submission = yield* opened.root.submit({ type: "input", content: "go" });
        yield* waitFor(Deferred.isDone(work.started));
        yield* finished(opened.root.abort());
        assert.strictEqual((yield* settle(submission)).status, "unanswered");
        assert.strictEqual(seen.length, 1);
        assert.deepStrictEqual(seen[0]!.result.details, { run: 1 });
        assert.strictEqual(
          (seen[0]!.result.output ?? [])
            .map((item) => (item.type === "text" ? item.text : ""))
            .join("|"),
          `work ${"o".repeat(40)}\n`,
        );
        yield* opened.harness.close;

        opened = yield* open(inner, setup, extensions);
        const message = results(yield* allEntries(opened.root)).get("c1");
        assert.strictEqual(message?.isError, true);
        assert.deepStrictEqual(message?.details, { run: 1 });
        assert.isTrue(textOf(message).startsWith(`work ${"o".repeat(40)}`));
        yield* opened.harness.close;
      }),
    ),
  );

  // The next Harness opens over the same storage.
  it.effect("runs on an abandoned nested call's result", () =>
    run(backend)(
      Effect.gen(function* () {
        const inner = yield* AgentStorage;
        const setup = chatSetup();
        const gated = Deferred.makeUnsafe<void>();
        let gate = true;
        const gatedTool = tool("gated", () => Effect.succeed({ output: [] }));
        const batch = tool("batch", () =>
          Effect.gen(function* () {
            const api = yield* ToolContext;
            yield* api.executeTool("gated", {});
            return {};
          }),
        );
        const seen: Array<Seen> = [];
        const hooks = Hook.tool({
          // Holds the nested call in its `call` phase, before intent, until the Harness closes.
          beforeTool: (call) =>
            call.name !== "gated" || !gate
              ? Effect.succeed(undefined)
              : Deferred.succeed(gated, undefined).pipe(Effect.andThen(Effect.never)),
          afterTool: (call, result) =>
            Effect.sync(() => {
              if (call.parent === undefined) return undefined;
              seen.push({ call, result });
              return { ...result, details: { hooked: true } };
            }),
        });
        const extensions = [toolExtension(gatedTool), toolExtension(batch), hookExtension(hooks)];
        setup.faux.setResponses([calls(["batch", {}, "c1"]), DONE]);
        let opened = yield* open(inner, setup, extensions);
        const id = (yield* opened.root.submit({ type: "input", content: "go" })).id;
        yield* waitFor(Deferred.isDone(gated));
        yield* opened.harness.close;
        gate = false;

        opened = yield* open(inner, setup, extensions);
        assert.strictEqual((yield* settled(opened.harness, id)).status, "done");
        assert.deepStrictEqual(
          seen.map((each) => [each.call.id, codes(each.result)]),
          [["c1/1", ["abandoned"]]],
        );
        yield* opened.harness.close;
      }),
    ),
  );

  it.effect(
    "runs on a restart_limit result at the limit, and not once the hooks themselves were cut off",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const inner = yield* AgentStorage;
          const setup = chatSetup();
          setup.settings.restartLimit = 1;
          setup.settings.progress = { outputIntervalMs: 0, persistWithinMs: 0 };
          const work = blocking("work");
          const seen: Array<Seen> = [];
          const hanging = Deferred.makeUnsafe<void>();
          const hooks = Hook.tool({
            afterTool: (call, result) =>
              Effect.gen(function* () {
                seen.push({ call, result });
                yield* Deferred.succeed(hanging, undefined);
                // Never returns: the host dies inside the hook.
                return yield* Effect.never;
              }),
          });
          const extensions = [toolExtension(work.registration), hookExtension(hooks)];
          setup.faux.setResponses([calls(["work", {}, "c1"]), DONE]);
          let opened = yield* open(inner, setup, extensions);
          const id = (yield* opened.root.submit({ type: "input", content: "go" })).id;
          yield* waitFor(Deferred.isDone(work.started));
          const taskId = yield* toolTaskId(opened.harness);
          yield* stored;
          yield* opened.kill;

          // Cut off once, at the limit: the task ends itself, and the hooks run on its result.
          opened = yield* open(inner, setup, extensions);
          yield* waitFor(Deferred.isDone(hanging));
          assert.deepStrictEqual(
            seen.map((each) => codes(each.result)),
            [["restart_limit"]],
          );
          yield* opened.kill;

          // Past the limit: ends without the hooks.
          opened = yield* open(inner, setup, extensions);
          assert.strictEqual((yield* settled(opened.harness, id)).status, "done");
          assert.strictEqual(seen.length, 1);
          assert.strictEqual(work.state.runs, 1);
          const entries = yield* allEntries(opened.root);
          assert.deepStrictEqual(diagnosticsOf(entries, "c1"), ["restart_limit"]);
          const record = Option.getOrUndefined(yield* opened.harness.harness.getTask(taskId));
          const outcome = record?.state.status === "terminal" ? record.state.outcome : undefined;
          assert.strictEqual(outcome?.status, "failed");
          assert.deepStrictEqual(outcome?.status === "failed" ? outcome.error.detail : undefined, {
            reason: "restart_limit",
          });
          yield* opened.harness.close;
        }),
      ),
  );
});

describeEachBackend("calls to tools the request did not offer", (backend) => {
  it.effect("run afterTool inside their own tool task", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        const seen: Array<{ readonly id: string; readonly taskId: TaskId }> = [];
        const hooks = Hook.tool({
          afterTool: (call, result, api) =>
            Effect.sync(() => {
              seen.push({ id: call.id, taskId: api.taskId });
              return { ...result, output: [text("try another tool")] };
            }),
        });
        setup.faux.setResponses([calls(["ghost", {}, "c1"]), DONE]);
        const { harness, root } = yield* openChat(yield* controlledStorage, setup, {
          extensions: [hookExtension(hooks)],
        });
        yield* settle(yield* root.submit({ type: "input", content: "go" }));
        assert.deepStrictEqual(
          seen.map((each) => each.id),
          ["c1"],
        );
        const record = Option.getOrUndefined(yield* harness.harness.getTask(seen[0]!.taskId));
        assert.strictEqual(record?.kind, "core.tool");
        const input = record?.input;
        assert.isTrue(Schema.is(Schema.JsonObject)(input));
        if (Schema.is(Schema.JsonObject)(input)) {
          assert.strictEqual(input["kind"], "model");
          assert.strictEqual(input["callId"], "c1");
          assert.strictEqual(input["unoffered"], true);
        }
        const entries = yield* allEntries(root);
        assert.isTrue(textOf(results(entries).get("c1")).startsWith("try another tool|"));
        assert.deepStrictEqual(diagnosticsOf(entries, "c1"), ["tool_unavailable"]);
        yield* harness.close;
      }),
    ),
  );

  it.effect("count toward a round's terminate", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        const echo = tool("echo", () => Effect.succeed({ output: [] }));
        const hooks = Hook.tool({
          afterTool: (_call, result) => Effect.succeed({ ...result, control: { terminate: true } }),
        });
        setup.faux.setResponses([calls(["echo", {}, "c1"], ["ghost", {}, "c2"]), DONE]);
        const { harness, root } = yield* openChat(yield* controlledStorage, setup, {
          extensions: [toolExtension(echo), hookExtension(hooks)],
        });
        const done = yield* settle(yield* root.submit({ type: "input", content: "go" }));
        assert.strictEqual(done.status, "done");
        assert.strictEqual(setup.faux.state.callCount, 1);
        yield* harness.close;
      }),
    ),
  );

  it.effect("take their turn in a sequential round, and the last handoff in call order wins", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        setup.settings.toolExecution = "sequential";
        const order: Array<string> = [];
        const [a, b] = ["a", "b"].map((name) =>
          tool(name, () =>
            Effect.sync(() => {
              order.push(name);
              return { output: [] };
            }),
          ),
        );
        const hooks = Hook.tool({
          afterTool: (call, result) =>
            Effect.sync(() => {
              order.push(`hook ${call.id}`);
              return call.id === "c1"
                ? undefined
                : { ...result, control: { handoff: `from ${call.id}` } };
            }),
        });
        setup.faux.setResponses([
          calls(["a", {}, "c1"], ["ghost", {}, "c2"], ["b", {}, "c3"]),
          DONE,
        ]);
        const { harness, root } = yield* openChat(yield* controlledStorage, setup, {
          extensions: [toolExtension(a!), toolExtension(b!), hookExtension(hooks)],
        });
        yield* settle(yield* root.submit({ type: "input", content: "go" }));
        assert.deepStrictEqual(order, ["a", "hook c1", "hook c2", "b", "hook c3"]);
        const resets = (yield* allEntries(root)).filter((entry) => ResetEntry.is(entry));
        assert.deepStrictEqual(
          resets.map((entry) => {
            const message = entry.model?.[0];
            return message?.role === "user" ? message.content : undefined;
          }),
          ["from c3"],
        );
        yield* harness.close;
      }),
    ),
  );

  it.effect("each get a tool task in a parallel round of only such calls", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        const seen: Array<string> = [];
        const hooks = Hook.tool({
          afterTool: (call) =>
            Effect.sync(() => {
              seen.push(call.id);
              return undefined;
            }),
        });
        setup.faux.setResponses([
          calls(["ghost", {}, "c1"], ["spirit", {}, "c2"], ["ghost", {}, "c3"]),
          DONE,
        ]);
        const { harness, root } = yield* openChat(yield* controlledStorage, setup, {
          extensions: [hookExtension(hooks)],
        });
        const done = yield* settle(yield* root.submit({ type: "input", content: "go" }));
        assert.strictEqual(done.status, "done");
        assert.deepStrictEqual(seen.toSorted(), ["c1", "c2", "c3"]);
        const entries = yield* allEntries(root);
        for (const id of ["c1", "c2", "c3"]) {
          assert.deepStrictEqual(diagnosticsOf(entries, id), ["tool_unavailable"]);
        }
        // The round's results go to the next request.
        assert.strictEqual(setup.faux.state.callCount, 2);
        yield* harness.close;
      }),
    ),
  );

  it.effect(
    "keep their flag in a version 2 generation's sequential round, also one migrated from version 1",
    () =>
      Effect.gen(function* () {
        for (const version of [2, 1]) {
          yield* run(backend)(
            Effect.gen(function* () {
              const inner = yield* AgentStorage;
              const setup = chatSetup();
              setup.settings.toolExecution = "sequential";
              const started = Deferred.makeUnsafe<void>();
              let runs = 0;
              const a = tool(
                "a",
                () =>
                  Effect.gen(function* () {
                    runs++;
                    if (runs > 1) return { output: [] };
                    yield* Deferred.succeed(started, undefined);
                    return yield* Effect.never;
                  }),
                { replay: "safe" },
              );
              const ghost = tool("ghost", () => Effect.succeed({ output: [text("ran")] }));
              const extensions = [toolExtension(a), toolExtension(ghost)];
              setup.faux.setResponses([calls(["a", {}, "c1"], ["ghost", {}, "c2"]), DONE]);
              let opened = yield* open(inner, setup, extensions);
              // `ghost` is registered, but not offered to this request.
              yield* opened.root.configure({ tools: [a] });
              const id = (yield* opened.root.submit({ type: "input", content: "go" })).id;
              yield* waitFor(Deferred.isDone(started));
              const live = Option.getOrThrow(
                yield* opened.harness.session.snapshot(LiveDoc, ROOT_CONVERSATION_ID),
              );
              const generation = live.run!.taskId;
              assert.deepStrictEqual(
                live.tools?.map((slot) => [slot.callId, slot.unoffered]),
                [
                  ["c1", undefined],
                  ["c2", true],
                ],
              );
              const record = Option.getOrThrow(yield* opened.harness.harness.getTask(generation));
              assert.strictEqual(record.version, 3);
              // The tools are offered when the round goes on: only the flag keeps `ghost` from running.
              yield* opened.root.configure({ tools: null });
              yield* opened.harness.close;
              if (version === 1) {
                const stored = Option.getOrThrow(yield* inner.task(generation));
                yield* commitWrites(inner, [{ type: "task", value: { ...stored, version: 1 } }]);
              }

              opened = yield* open(inner, setup, extensions);
              assert.strictEqual((yield* settled(opened.harness, id)).status, "done");
              const after = Option.getOrThrow(yield* opened.harness.harness.getTask(generation));
              assert.strictEqual(after.version, 3);
              const entries = yield* allEntries(opened.root);
              assert.deepStrictEqual(diagnosticsOf(entries, "c2"), ["tool_unavailable"]);
              assert.notInclude(textOf(results(entries).get("c2")), "ran");
              yield* opened.harness.close;
            }),
          );
        }
      }),
  );

  it.effect(
    "settle unavailable, not aborted, when their sequential round is aborted before they start",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const setup = chatSetup();
          setup.settings.toolExecution = "sequential";
          const started = Deferred.makeUnsafe<void>();
          const wait = tool("wait", () =>
            Deferred.succeed(started, undefined).pipe(Effect.andThen(Effect.never)),
          );
          setup.faux.setResponses([calls(["wait", {}, "c1"], ["ghost", {}, "c2"]), DONE]);
          const { harness, root } = yield* openChat(yield* controlledStorage, setup, {
            extensions: [toolExtension(wait)],
          });
          const submission = yield* root.submit({ type: "input", content: "go" });
          yield* waitFor(Deferred.isDone(started));
          yield* finished(root.abort());
          assert.strictEqual((yield* settle(submission)).status, "unanswered");
          const entries = yield* allEntries(root);
          assert.deepStrictEqual(diagnosticsOf(entries, "c1"), ["aborted"]);
          assert.deepStrictEqual(diagnosticsOf(entries, "c2"), ["tool_unavailable"]);
          yield* harness.close;
        }),
      ),
  );
});

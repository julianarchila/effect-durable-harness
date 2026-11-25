// Tool calls in a run, on every backend.
//
// The registry is the static extensions of `openChat`: a tool is an extension named `tool:<name>` and hooks an
// extension holding them, in registration order. `setup.settings` is a live settings object, read at each
// decision; the model is `FauxModel`; a failure a tool raises is a typed failure (`Boom`) of its Effect; an
// aborted signal is the interruption of the fiber. Waits are `Effect.sleep` on the `TestClock`, which `waitFor`
// advances while a submission is awaited; each `run` opens a fresh storage of the backend. A `details(...)` call
// left pending is a fiber forked detached and started immediately, so it marks in the tool's step.
//
// Not covered: coding tools (a failing command's retained tail, a run that reads, edits and runs a command), which
// belong to the execution environment package, and replacing a tool or reloading an extension mid-run, since the
// registry is static.
import { assert, it } from "@effect/vitest";
import * as Cause from "effect/Cause";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Predicate from "effect/Predicate";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import { AgentDoc, configure } from "../src/Agent.ts";
import * as Doc from "../src/Doc.ts";
import * as AgentTool from "../src/AgentTool.ts";
import { type OutputWindow, ToolContext } from "../src/AgentTool.ts";
import { ToolResultEntry } from "../src/Entry.ts";
import type { ExecutionEnvBuilder } from "../src/ExecutionEnv.ts";
import * as Extension from "../src/Extension.ts";
import type { Conversation } from "../src/Harness.ts";
import * as Hook from "../src/Hook.ts";
import type { TaskId } from "../src/Ids.ts";
import { LiveDoc } from "../src/Live.ts";
import type { AssistantMessage, ToolResultMessage } from "../src/Message.ts";
import { Models } from "../src/Models.ts";
import type { EntryRecord } from "../src/Records.ts";
import type { Submission } from "../src/Submission.ts";
import * as Task from "../src/Task.ts";
import {
  type FauxResponseStep,
  fauxAssistantMessage,
  fauxText,
  fauxToolCall,
} from "../src/testing/FauxModel.ts";
import { TaskError } from "../src/TaskError.ts";
import { Tx } from "../src/Tx.ts";
import {
  allEntries,
  type ChatSetup,
  chatSetup,
  FAUX_MODEL,
  openChat,
  textOf,
  waitFor,
} from "./ChatSupport.ts";
import { completed, type TestHarness } from "./HarnessSupport.ts";
import {
  type Backend,
  type ControlledStorage,
  controlledStorage,
  describeEachBackend,
  run,
} from "./SessionSupport.ts";
import type { Report } from "../src/Report.ts";

/** A typed failure carrying the message. */
class Boom extends Schema.TaggedError<Boom>()("Boom", { message: Schema.String }) {}

/** A tagged error with no `message` field. */
class Silent extends Schema.TaggedError<Silent>()("Silent", {}) {}

const EchoParameters = Schema.Struct({ text: Schema.optionalKey(Schema.String) });
type EchoArgs = typeof EchoParameters.Type;
type Execute<E, R> = (args: EchoArgs) => Effect.Effect<AgentTool.ToolExecutionResult, E, R>;
type Extra = Omit<
  AgentTool.MakeOptions<EchoArgs, Schema.Json, never, never>,
  "description" | "parameters" | "execute"
>;

/** The echo-parameters tool `name`. */
const tool = <E, R>(name: string, execute: Execute<E, R>, extra: Extra = {}) =>
  AgentTool.make(name, {
    description: `The ${name} tool`,
    parameters: EchoParameters,
    execute,
    ...extra,
  });

/** An extension holding one tool, named `tool:<name>`. */
const toolExtension = (agentTool: AgentTool.Any) =>
  Extension.make({ name: `tool:${agentTool.name}`, tools: [agentTool] });

let hookExtensions = 0;
/** An extension holding one hook registration. */
const hookExtension = (hook: Hook.Any) =>
  Extension.make({ name: `hooks:${++hookExtensions}`, hooks: [hook] });

/** A tool-calling answer with one call per `[name, args, id]`. */
const calls = (
  ...list: ReadonlyArray<readonly [string, Record<string, Schema.Json>, string]>
): AssistantMessage =>
  fauxAssistantMessage(
    list.map(([name, args, id]) => fauxToolCall(name, args, { id })),
    { stopReason: "toolUse" },
  );

const DONE = fauxAssistantMessage([fauxText("done")]);

const text = (value: string) => ({ type: "text" as const, text: value });

/** A fresh storage of the backend, alive for the test's scope. */
const freshStorage = (backend: Backend) =>
  Effect.gen(function* () {
    const context = yield* Layer.buildWithMemoMap(
      backend.layer,
      yield* Layer.makeMemoMap,
      yield* Effect.scope,
    );
    return yield* controlledStorage.pipe(Effect.provideContext(context));
  });

/** Awaits the submission while the `TestClock` advances in 5 ms steps. */
const settle = (submission: Submission) =>
  Effect.gen(function* () {
    const fiber = yield* Effect.forkChild(submission.await, { startImmediately: true });
    yield* waitFor(Effect.sync(() => fiber.pollUnsafe() !== undefined));
    return yield* Fiber.join(fiber);
  });

/** Submits input and waits for it to settle. */
const ask = (conversation: Conversation, content: string) =>
  conversation.submit({ type: "input", content }).pipe(Effect.flatMap(settle));

interface RunOptions<E> {
  readonly extensions?: ReadonlyArray<Extension.Any> | undefined;
  readonly env?: ExecutionEnvBuilder<never, E> | undefined;
  readonly prepare?:
    | ((harness: TestHarness, root: Conversation) => Effect.Effect<void>)
    | undefined;
}

/** Opens a chat over a fresh storage, submits "go" and waits for it. */
const runChat = <E = never>(
  backend: Backend,
  setup: ChatSetup,
  responses: ReadonlyArray<FauxResponseStep>,
  options: RunOptions<E> = {},
) =>
  Effect.gen(function* () {
    setup.faux.setResponses(responses);
    const storage = yield* freshStorage(backend);
    const { harness, root } = yield* openChat(storage, setup, {
      extensions: options.extensions,
      env: options.env,
    });
    if (options.prepare !== undefined) yield* options.prepare(harness, root);
    const settled = yield* ask(root, "go");
    return { harness, root, storage, entries: yield* allEntries(root), status: settled.status };
  });

const results = (entries: ReadonlyArray<EntryRecord>): Array<ToolResultMessage> =>
  entries.flatMap((entry) => {
    const message = entry.model?.[0];
    return ToolResultEntry.is(entry) && message?.role === "toolResult" ? [message] : [];
  });

const resultText = (message: ToolResultMessage): string =>
  message.content.map((item) => (item.type === "text" ? item.text : `[${item.type}]`)).join("|");

const systemMessages = (entries: ReadonlyArray<EntryRecord>) =>
  entries.flatMap((entry) => {
    const message = entry.model?.[0];
    return entry.kind === "core.system" && message?.role === "system" ? [message] : [];
  });

const scanTasks = (h: TestHarness, conversation: Conversation) =>
  h.harness.commit(
    Effect.gen(function* () {
      const tx = yield* Tx;
      return (yield* tx.scanTasks({ conversationId: conversation.id }, 20)).items;
    }),
  );

const live = (h: TestHarness, conversation: Conversation) =>
  h.session.snapshot(LiveDoc, conversation.id).pipe(Effect.map(Option.getOrUndefined));

/** A JSON object's fields, for a hook that adds one to a provider body. */
const asObject = (payload: Schema.Json): Schema.JsonObject =>
  Schema.is(Schema.JsonObject)(payload) ? payload : {};

/** The message of a reported cause. */
const reportMessage = (report: Report): string => {
  const cause = report.cause;
  const error = Cause.squash(cause);
  return error instanceof Error ? error.message : String(error);
};

/** Present only on conversations that run in a container. */
const ContainerDoc = Doc.make({
  checkpointWhen: () => false,
  kind: "test.container",
  version: 1,
  scope: "conversation",
  history: "latest",
  fork: "current",
  schema: Schema.Struct({ image: Schema.String }),
  initial: () => ({ image: "" }),
});

/** A one-phase task no definition takes when unregistered: it stays live and pending. */
const pendingTask = (name: string) =>
  Task.make({
    name,
    version: 1,
    input: Schema.Struct({}),
    checkpoint: Schema.Struct({ phase: Schema.Literal("never") }),
    result: Schema.Null,
    initial: () => ({ phase: "never" }),
    phases: { never: () => Effect.void },
    abort: () => Effect.void,
  });

describeEachBackend("tool round", (backend) => {
  it.effect("runs input, tool call, tool result, and answer, and settles the input", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        const echo = tool("echo", (args) =>
          Effect.succeed({ output: [text(`echo ${args.text}`)] }),
        );
        const { harness, root, entries, status } = yield* runChat(
          backend,
          setup,
          [calls(["echo", { text: "hi" }, "c1"]), DONE],
          { extensions: [toolExtension(echo)] },
        );
        assert.strictEqual(status, "done");
        assert.deepStrictEqual(
          entries.map((entry) => entry.kind),
          ["core.user", "core.system", "core.assistant", "core.tool-result", "core.assistant"],
        );
        const system = systemMessages(entries)[0]!;
        const declared = system.toolsAdded ?? [];
        assert.deepStrictEqual(declared, [
          { name: "echo", description: "The echo tool", parameters: declared[0]!.parameters },
        ]);
        assert.isTrue(Predicate.isObject(declared[0]?.parameters));
        assert.isTrue(Predicate.isObject(system.toolsAdded?.[0]?.parameters));
        const [result] = results(entries);
        assert.strictEqual(result?.toolCallId, "c1");
        assert.strictEqual(result?.toolName, "echo");
        assert.strictEqual(result?.isError, false);
        assert.strictEqual(resultText(result!), "echo hi");
        assert.deepStrictEqual(entries[3]!.data, { diagnostics: [] });
        assert.isDefined(entries[3]!.byTaskId);
        // The second request sees the tool result right after its call.
        assert.strictEqual(setup.faux.state.callCount, 2);
        assert.deepStrictEqual(yield* live(harness, root), {});
        yield* harness.close;
      }),
    ),
  );

  // The tool and the hook require `Models` in `R`, and the Harness provides its own service, the one `setup.faux.layer`
  // builds.
  it.effect("gives tools and hooks the Harness's models", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        const seen: Array<unknown> = [];
        const echo = tool("echo", () =>
          Effect.gen(function* () {
            seen.push(yield* Models);
            return { output: [] };
          }),
        );
        const hooks = Hook.tool({
          beforeTool: () =>
            Effect.gen(function* () {
              seen.push(yield* Models);
              return undefined;
            }),
        });
        const { harness, status } = yield* runChat(
          backend,
          setup,
          [calls(["echo", {}, "c1"]), DONE],
          {
            extensions: [toolExtension(echo), hookExtension(hooks)],
          },
        );
        assert.strictEqual(status, "done");
        const models = yield* Effect.provide(Effect.service(Models), setup.faux.layer);
        assert.strictEqual(seen.length, 2);
        assert.isTrue(seen.every((each) => each === models));
        yield* harness.close;
      }),
    ),
  );

  it.effect("answers calls to tools the request did not offer through a flagged tool task", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        const echo = tool("echo", () => Effect.succeed({ output: [] }));
        const { harness, root, entries, status } = yield* runChat(
          backend,
          setup,
          [calls(["ghost", {}, "c1"], ["echo", {}, "c2"]), DONE],
          { extensions: [toolExtension(echo)] },
        );
        assert.strictEqual(status, "done");
        const [ghost, echoed] = results(entries);
        assert.strictEqual(ghost?.toolCallId, "c1");
        assert.strictEqual(ghost?.isError, true);
        assert.strictEqual(
          resultText(ghost!),
          "<harness>\n[error] Tool ghost is not available\n</harness>",
        );
        assert.strictEqual(echoed?.toolCallId, "c2");
        assert.strictEqual(echoed?.isError, false);
        const ghostEntry = entries.find(
          (entry) => ToolResultEntry.is(entry) && entry.model![0]!.role === "toolResult",
        );
        assert.deepStrictEqual(ghostEntry?.data, {
          diagnostics: [
            { severity: "error", code: "tool_unavailable", message: "Tool ghost is not available" },
          ],
        });
        // The unoffered call has its own task, so `afterTool` and `control` apply to it as to any call.
        const tasks = yield* scanTasks(harness, root);
        const inputs = tasks.filter((task) => task.kind === "core.tool").map((task) => task.input);
        assert.strictEqual(inputs.length, 2);
        assert.isTrue(
          inputs.some(
            (input) =>
              Predicate.isObject(input) && input["callId"] === "c1" && input["unoffered"] === true,
          ),
        );
        assert.isTrue(inputs.some((input) => Predicate.isObject(input) && !("unoffered" in input)));
        yield* harness.close;
      }),
    ),
  );

  // A deferred tool the agent has, never loaded, is not offered, so its call resolves nothing.
  it.effect("never runs a tool the request did not offer, even when the agent has it", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        let ran = 0;
        const hidden = tool(
          "hidden",
          () =>
            Effect.sync(() => {
              ran++;
              return { output: [] };
            }),
          { deferred: true },
        );
        const { harness, entries, status } = yield* runChat(
          backend,
          setup,
          [calls(["hidden", {}, "c1"]), DONE],
          { extensions: [toolExtension(hidden)] },
        );
        assert.strictEqual(status, "done");
        assert.strictEqual(ran, 0);
        assert.strictEqual(
          resultText(results(entries)[0]!),
          "<harness>\n[error] Tool hidden is not available\n</harness>",
        );
        yield* harness.close;
      }),
    ),
  );

  it.effect("answers a call to a tool deactivated after preparation with tool_unavailable", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        const seen: Array<string> = [];
        const echo = tool("echo", () =>
          Effect.sync(() => {
            seen.push("ran");
            return { output: [] };
          }),
        );
        let root: Conversation | undefined;
        const deactivate: FauxResponseStep = () =>
          Effect.suspend(() => root!.configure({ tools: [] })).pipe(
            Effect.orDie,
            Effect.as(calls(["echo", {}, "c1"])),
          );
        const result = yield* runChat(backend, setup, [deactivate, DONE], {
          extensions: [toolExtension(echo)],
          prepare: (_harness, conversation) =>
            Effect.sync(() => {
              root = conversation;
            }),
        });
        assert.deepStrictEqual(seen, []);
        assert.strictEqual(
          resultText(results(result.entries)[0]!),
          "<harness>\n[error] Tool echo is not available\n</harness>",
        );
        // The next preparation removes it.
        assert.deepStrictEqual(systemMessages(result.entries).at(-1)!.toolsRemoved, [
          { name: "echo" },
        ]);
        yield* result.harness.close;
      }),
    ),
  );

  // The Harness is reopened over the same storage without the tool's extension, and again with it.
  it.effect(
    "removes unregistered active tools from the offer and adds them back after re-registration",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const setup = chatSetup();
          const echo = toolExtension(tool("echo", () => Effect.succeed({ output: [] })));
          const first = yield* runChat(backend, setup, [DONE], { extensions: [echo] });
          yield* first.harness.close;
          const without = yield* openChat(first.storage, setup, { extensions: [] });
          setup.faux.setResponses([calls(["echo", {}, "c1"]), DONE]);
          const second = yield* ask(without.root, "again");
          assert.strictEqual(second.status, "done");
          let entries = yield* allEntries(without.root);
          assert.deepStrictEqual(systemMessages(entries).at(-1)!.toolsRemoved, [{ name: "echo" }]);
          assert.strictEqual(results(entries)[0]?.isError, true);
          // The stored agent is not rewritten; the tool is only not resolved.
          assert.deepStrictEqual((yield* without.root.agent).tools, []);
          yield* without.harness.close;

          const back = yield* openChat(first.storage, setup, { extensions: [echo] });
          setup.faux.setResponses([DONE]);
          yield* ask(back.root, "back");
          entries = yield* allEntries(back.root);
          assert.deepStrictEqual(
            systemMessages(entries)
              .at(-1)!
              .toolsAdded?.map((declared) => declared.name),
            ["echo"],
          );
          yield* back.harness.close;
        }),
      ),
  );

  // `first` (replay `safe`) is held until the Harness closes mid-round, and the reopened Harness has `first` without
  // the hold and no `second`. The rerun of `first` completes and `second`'s task, created after it, resolves nothing.
  it.effect(
    "produces tool_unavailable when the implementation is unregistered before its task runs",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const setup = chatSetup();
          const started = yield* Deferred.make<void>();
          const held = tool(
            "first",
            () => Deferred.succeed(started, undefined).pipe(Effect.andThen(Effect.never)),
            { executionMode: "sequential", replay: "safe" },
          );
          const second = tool("second", () => Effect.succeed({ output: [text("ran")] }));
          setup.faux.setResponses([calls(["first", {}, "c1"], ["second", {}, "c2"]), DONE]);
          const storage: ControlledStorage = yield* freshStorage(backend);
          const opened = yield* openChat(storage, setup, {
            extensions: [toolExtension(held), toolExtension(second)],
          });
          const submission = yield* opened.root.submit({ type: "input", content: "go" });
          yield* waitFor(Deferred.isDone(started));
          yield* opened.harness.close;

          const free = tool("first", () => Effect.succeed({ output: [] }), {
            executionMode: "sequential",
            replay: "safe",
          });
          const reopened = yield* openChat(storage, setup, { extensions: [toolExtension(free)] });
          const handle = Option.getOrThrow(
            yield* reopened.harness.harness.submission(submission.id),
          );
          const settled = yield* settle(handle);
          assert.strictEqual(settled.status, "done");
          const [, late] = results(yield* allEntries(reopened.root));
          assert.strictEqual(
            resultText(late!),
            "<harness>\n[error] Tool second is not available\n</harness>",
          );
          yield* reopened.harness.close;
        }),
      ),
  );

  // The test changes `setup.settings`, which the Harness reads.
  it.effect("reads the execution mode when a round starts and keeps it for the round", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        const events: Array<string> = [];
        const slow = (name: string) =>
          tool(name, () =>
            Effect.gen(function* () {
              events.push(`start ${name}`);
              // Changed after the round started: not seen by this round.
              setup.settings.toolExecution = "parallel";
              yield* Effect.sleep("20 millis");
              events.push(`end ${name}`);
              return { output: [] };
            }),
          );
        // Changed while the model request runs: the round that follows uses it.
        const request: FauxResponseStep = () =>
          Effect.sync(() => {
            setup.settings.toolExecution = "sequential";
            return calls(["a", {}, "c1"], ["b", {}, "c2"]);
          });
        const { harness } = yield* runChat(backend, setup, [request, DONE], {
          extensions: [toolExtension(slow("a")), toolExtension(slow("b"))],
        });
        assert.deepStrictEqual(events, ["start a", "end a", "start b", "end b"]);
        yield* harness.close;
      }),
    ),
  );

  it.effect(
    "runs a round in parallel by default and sequentially when configured or required by a tool",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const trace = (setup: ChatSetup) =>
            Effect.gen(function* () {
              const events: Array<string> = [];
              const slow = (name: string) =>
                tool(name, () =>
                  Effect.gen(function* () {
                    events.push(`start ${name}`);
                    yield* Effect.sleep("20 millis");
                    events.push(`end ${name}`);
                    return { output: [] };
                  }),
                );
              const result = yield* runChat(
                backend,
                setup,
                [calls(["a", {}, "c1"], ["b", {}, "c2"]), DONE],
                { extensions: [toolExtension(slow("a")), toolExtension(slow("b"))] },
              );
              yield* result.harness.close;
              return events;
            });
          assert.deepStrictEqual((yield* trace(chatSetup())).slice(0, 2), ["start a", "start b"]);
          const configured = chatSetup();
          configured.settings.toolExecution = "sequential";
          assert.deepStrictEqual(yield* trace(configured), [
            "start a",
            "end a",
            "start b",
            "end b",
          ]);
          const perTool = chatSetup();
          const events: Array<string> = [];
          const a = tool(
            "a",
            () =>
              Effect.gen(function* () {
                yield* Effect.sleep("20 millis");
                events.push("a");
                return { output: [] };
              }),
            { executionMode: "sequential" },
          );
          const b = tool("b", () =>
            Effect.sync(() => {
              events.push("b");
              return { output: [] };
            }),
          );
          const result = yield* runChat(
            backend,
            perTool,
            [calls(["a", {}, "c1"], ["b", {}, "c2"]), DONE],
            { extensions: [toolExtension(a), toolExtension(b)] },
          );
          const tasks = yield* scanTasks(result.harness, result.root);
          const tools = tasks
            .filter((task) => task.kind === "core.tool")
            .toSorted((x, y) => x.id - y.id);
          // The generation owns both tools and creates the second only after the first ended.
          const generation = tasks.find((task) => task.kind === "core.generation");
          assert.deepStrictEqual(
            tools.map((task) => task.owner),
            [generation!.id, generation!.id],
          );
          assert.deepStrictEqual(events, ["a", "b"]);
          yield* result.harness.close;
        }),
      ),
  );
});

describeEachBackend("tool results", (backend) => {
  it.effect(
    "uses retained output and the last details when the result omits them, with diagnostics in order",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const setup = chatSetup();
          const log = tool(
            "log",
            () =>
              Effect.gen(function* () {
                const api = yield* ToolContext;
                yield* api.output("line 1\n");
                yield* api.output(new TextEncoder().encode("line 2\nline 3\n"));
                yield* api.diagnostic({ severity: "info", message: "from api" });
                yield* api.details({ step: 1 });
                yield* api.details({ step: 2 });
                return { diagnostics: [{ severity: "warn" as const, message: "from result" }] };
              }),
            { outputLimits: { maxLines: 2 } },
          );
          const { harness, entries } = yield* runChat(
            backend,
            setup,
            [calls(["log", {}, "c1"]), DONE],
            {
              extensions: [toolExtension(log)],
            },
          );
          const [result] = results(entries);
          assert.deepStrictEqual(result!.details, { step: 2 });
          assert.strictEqual(
            resultText(result!),
            "line 1\nline 2\n|<harness>\n[info] from api\n[warn] from result\n[warn] Output truncated to its beginning: 1 lines, 7 bytes dropped\n</harness>",
          );
          const entry = entries.find((candidate) => ToolResultEntry.is(candidate));
          assert.deepStrictEqual(
            ToolResultEntry.is(entry) &&
              entry.data.diagnostics.map((diagnostic) => diagnostic.code),
            [undefined, undefined, "truncated"],
          );
          yield* harness.close;
        }),
      ),
  );

  it.effect(
    "offers the tail window with the configured pace, not a head window, and accepts skipped output",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const setup = chatSetup();
          setup.settings.progress = { outputIntervalMs: 250 };
          const windows: Array<OutputWindow | undefined> = [];
          const tailed = tool(
            "tailed",
            () =>
              Effect.gen(function* () {
                const api = yield* ToolContext;
                windows.push(api.outputWindow);
                yield* api.output("dropped\n");
                yield* api.output("x\ny\n", { bytes: 8, newlines: 1, endsWithNewline: true });
                return {};
              }),
            { outputLimits: { maxLines: 1, retain: "tail" } },
          );
          const headed = tool(
            "headed",
            () =>
              Effect.gen(function* () {
                windows.push((yield* ToolContext).outputWindow);
                return {};
              }),
            { outputLimits: { maxLines: 1 } },
          );
          const { harness, entries } = yield* runChat(
            backend,
            setup,
            [calls(["tailed", {}, "c1"], ["headed", {}, "c2"]), DONE],
            { extensions: [toolExtension(tailed), toolExtension(headed)] },
          );
          assert.deepInclude(windows, {
            maxBytes: 50 * 1024,
            maxLines: 1,
            minIntervalMs: 250,
            bytesPerSecond: 100 * 1024,
          });
          assert.include(windows, undefined);
          // 8 bytes written, 8 skipped, then "x\n" dropped by the window: 3 lines, 18 bytes in all.
          assert.strictEqual(
            resultText(results(entries).find((result) => result.toolCallId === "c1")!),
            "y\n|<harness>\n[warn] Output truncated to its end: 3 lines, 18 bytes dropped\n</harness>",
          );
          yield* harness.close;
        }),
      ),
  );

  it.effect("bounds explicit text content and keeps other content", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        const image = { type: "image", data: "AAAA", mimeType: "image/png" } as const;
        const big = tool(
          "big",
          () => Effect.succeed({ output: [text("a\nb\n"), image, text("c\nd\n")] }),
          { outputLimits: { maxLines: 2, retain: "tail" } },
        );
        const { harness, entries } = yield* runChat(
          backend,
          setup,
          [calls(["big", {}, "c1"]), DONE],
          {
            extensions: [toolExtension(big)],
          },
        );
        assert.strictEqual(
          resultText(results(entries)[0]!),
          "[image]|c\nd\n|<harness>\n[warn] Output truncated to its end: 2 lines, 4 bytes dropped\n</harness>",
        );
        yield* harness.close;
      }),
    ),
  );

  it.effect("turns a throw into a tool_error result with the partial output", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        const fail = tool("fail", () =>
          Effect.gen(function* () {
            const api = yield* ToolContext;
            yield* api.output("partial\n");
            return yield* new Boom({ message: "boom" });
          }),
        );
        const { harness, entries, status } = yield* runChat(
          backend,
          setup,
          [calls(["fail", {}, "c1"]), DONE],
          { extensions: [toolExtension(fail)] },
        );
        assert.strictEqual(status, "done");
        const [result] = results(entries);
        assert.strictEqual(result!.isError, true);
        assert.strictEqual(resultText(result!), "partial\n|<harness>\n[error] boom\n</harness>");
        yield* harness.close;
      }),
    ),
  );

  // A tagged error declared without a `message` names the failure by its `_tag` (`tool.ts`,
  // `toolErrorMessage`).
  it.effect("names a tool_error by the error's tag when it has no message", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        const fail = tool("fail", () => Effect.fail(new Silent()));
        const { harness, entries } = yield* runChat(
          backend,
          setup,
          [calls(["fail", {}, "c1"]), DONE],
          { extensions: [toolExtension(fail)] },
        );
        assert.strictEqual(
          resultText(results(entries)[0]!),
          "<harness>\n[error] Silent\n</harness>",
        );
        yield* harness.close;
      }),
    ),
  );

  it.effect(
    "records each result's span from the call's first run, hooks included, also for calls that did not execute",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const setup = chatSetup();
          const slow = tool("slow", () =>
            Effect.sleep("30 millis").pipe(Effect.as({ output: [] })),
          );
          const thrower = tool("thrower", () =>
            Effect.sleep("30 millis").pipe(Effect.andThen(new Boom({ message: "boom" }))),
          );
          const hooks = Hook.tool({
            beforeTool: (call) =>
              Effect.sleep("100 millis").pipe(
                Effect.as(call.id === "blocked" ? { block: "no" } : undefined),
              ),
          });
          const { harness, entries } = yield* runChat(
            backend,
            setup,
            [
              calls(["slow", {}, "slow"], ["thrower", {}, "thrower"], ["slow", {}, "blocked"]),
              DONE,
            ],
            { extensions: [toolExtension(slow), toolExtension(thrower), hookExtension(hooks)] },
          );
          const byId = new Map(results(entries).map((result) => [result.toolCallId, result]));
          for (const id of ["slow", "thrower"]) {
            assert.isAtLeast(byId.get(id)?.durationMs ?? -1, 125);
          }
          assert.strictEqual(byId.get("thrower")?.isError, true);
          assert.isAtLeast(byId.get("blocked")?.durationMs ?? -1, 95);
          yield* harness.close;
        }),
      ),
  );

  it.effect(
    "validates arguments before and after beforeTool and applies blocks and replacements",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const setup = chatSetup();
          const seen: Array<EchoArgs> = [];
          const echo = tool("echo", (args) =>
            Effect.sync(() => {
              seen.push(args);
              return { output: [] };
            }),
          );
          const hooks = Hook.tool({
            beforeTool: (call) => {
              if (call.id === "block") return Effect.succeed({ block: "not today" });
              if (call.id === "throw") return Effect.fail(new Boom({ message: "hook failed" }));
              if (call.id === "bad")
                return Effect.succeed({ arguments: { text: { not: "a string" } } });
              return Effect.succeed({ arguments: { text: `${String(call.arguments.text)}!` } });
            },
          });
          const { harness, entries } = yield* runChat(
            backend,
            setup,
            [
              calls(
                ["echo", { text: 1 }, "coerced"],
                ["echo", { text: { not: "a string" } }, "invalid"],
                ["echo", {}, "block"],
                ["echo", {}, "throw"],
                ["echo", { text: "x" }, "bad"],
                ["echo", { text: "x" }, "ok"],
              ),
              DONE,
            ],
            { extensions: [toolExtension(echo), hookExtension(hooks)] },
          );
          // Parallel tools append their results in completion order.
          const byId = new Map(
            results(entries).map((result) => [
              result.toolCallId,
              [result.isError, resultText(result)] as const,
            ]),
          );
          assert.deepStrictEqual(byId.get("block"), [
            true,
            "<harness>\n[error] Tool call blocked: not today\n</harness>",
          ]);
          assert.deepStrictEqual(byId.get("throw"), [
            true,
            "<harness>\n[error] Tool call blocked: hook failed\n</harness>",
          ]);
          assert.strictEqual(byId.get("bad")![0], true);
          assert.include(byId.get("bad")![1], "Validation failed");
          assert.deepStrictEqual(byId.get("ok"), [false, ""]);
          assert.strictEqual(byId.get("invalid")![0], true);
          assert.include(byId.get("invalid")![1], "Validation failed");
          // The number is coerced to a string before the first validation.
          assert.deepStrictEqual(byId.get("coerced"), [false, ""]);
          assert.sameDeepMembers(seen, [{ text: "1!" }, { text: "x!" }]);
          yield* harness.close;
        }),
      ),
  );

  it.effect(
    "repairs arguments with prepareArguments before validation, and a throwing repair is invalid",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const setup = chatSetup();
          const seen: Array<EchoArgs> = [];
          const echo = tool(
            "echo",
            (args) =>
              Effect.sync(() => {
                seen.push(args);
                return { output: [] };
              }),
            {
              prepareArguments: (args) => {
                const value = Predicate.isObject(args) && "text" in args ? args.text : undefined;
                if (value === "throw") throw new Error("cannot repair");
                return Predicate.isNumber(value) ? { text: `#${value}` } : args;
              },
            },
          );
          const { harness, entries } = yield* runChat(
            backend,
            setup,
            [calls(["echo", { text: 7 }, "fixed"], ["echo", { text: "throw" }, "broken"]), DONE],
            { extensions: [toolExtension(echo)] },
          );
          const byId = new Map(
            results(entries).map((result) => [result.toolCallId, resultText(result)]),
          );
          assert.strictEqual(byId.get("fixed"), "");
          assert.strictEqual(byId.get("broken"), "<harness>\n[error] cannot repair\n</harness>");
          assert.deepStrictEqual(seen, [{ text: "#7" }]);
          // The stored call keeps what the model sent.
          // SAFETY: entry 2 is the `core.assistant` entry of the round.
          const assistant = entries[2]!.model![0] as AssistantMessage;
          const call = assistant.content.find((item) => item.type === "toolCall");
          assert.deepStrictEqual(call?.type === "toolCall" && call.arguments, { text: 7 });
          yield* harness.close;
        }),
      ),
  );

  // A repair that returns something other than an object is rejected by validation.
  it.effect("lets validation reject a repair that does not return an object", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        const echo = tool("echo", () => Effect.succeed({ output: [] }), {
          prepareArguments: () => "not an object",
        });
        const { harness, entries } = yield* runChat(
          backend,
          setup,
          [calls(["echo", { text: "x" }, "c1"]), DONE],
          { extensions: [toolExtension(echo)] },
        );
        const [result] = results(entries);
        assert.strictEqual(result!.isError, true);
        assert.include(resultText(result!), "Validation failed");
        yield* harness.close;
      }),
    ),
  );

  it.effect("lets the first beforeTool block win and skips later handlers", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        const echo = tool("echo", () => Effect.succeed({ output: [] }));
        const asked: Array<string> = [];
        const first = Hook.tool({
          beforeTool: () =>
            Effect.sync(() => {
              asked.push("first");
              return { block: "first says no" };
            }),
        });
        const second = Hook.tool({
          beforeTool: () =>
            Effect.sync(() => {
              asked.push("second");
              return { block: "second says no" };
            }),
        });
        const { harness, entries } = yield* runChat(
          backend,
          setup,
          [calls(["echo", {}, "c1"]), DONE],
          {
            extensions: [toolExtension(echo), hookExtension(first), hookExtension(second)],
          },
        );
        assert.deepStrictEqual(asked, ["first"]);
        assert.strictEqual(
          resultText(results(entries)[0]!),
          "<harness>\n[error] Tool call blocked: first says no\n</harness>",
        );
        yield* harness.close;
      }),
    ),
  );

  it.effect("chains afterTool replacements and observes the round with afterTools", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        const echo = tool("echo", () => Effect.succeed({ output: [text("raw")] }));
        const first = Hook.tool({
          afterTool: (_call, result) => Effect.succeed({ ...result, output: [text("first")] }),
        });
        const second = Hook.tool({
          afterTool: (_call, result) =>
            Effect.succeed({
              ...result,
              details: {
                replaced: (result.output ?? [])
                  .map((item) => (item.type === "text" ? item.text : ""))
                  .join("|"),
              },
            }),
        });
        const observed: Array<unknown> = [];
        const afterTools = Hook.generation({
          afterTools: (assistant, entries) =>
            Effect.sync(() => {
              observed.push(assistant, entries);
              return undefined;
            }),
        });
        const { harness, entries } = yield* runChat(
          backend,
          setup,
          [calls(["echo", {}, "c1"]), DONE],
          {
            extensions: [
              toolExtension(echo),
              hookExtension(first),
              hookExtension(second),
              hookExtension(afterTools),
            ],
          },
        );
        const [result] = results(entries);
        assert.strictEqual(resultText(result!), "first");
        assert.deepStrictEqual(result!.details, { replaced: "first" });
        const resultEntry = entries.find((entry) => ToolResultEntry.is(entry))!;
        assert.deepStrictEqual(observed, [entries[2]!.id, [resultEntry.id]]);
        yield* harness.close;
      }),
    ),
  );

  it.effect(
    "runs the hooks of the selected extensions; a task-owned child copies its owner's selection",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const setup = chatSetup();
          const calledIn: Array<number> = [];
          const Echo = Extension.make({
            name: "echo",
            tools: [tool("echo", () => Effect.succeed({ output: [] }))],
          });
          const Audit = Extension.make({
            name: "audit",
            hooks: [
              Hook.tool({
                beforeTool: (_call, context) =>
                  Effect.sync(() => {
                    calledIn.push(context.conversationId);
                    return undefined;
                  }),
              }),
            ],
          });
          // Audit is installed but not in the default selection.
          setup.settings.extensions = [Echo];
          const first = yield* runChat(backend, setup, [DONE], { extensions: [Echo, Audit] });
          yield* first.root.configure({ extensions: { add: [Audit] } });
          // An owner task no registered definition takes stays live and pending.
          const owner = pendingTask("test.owner");
          const childId = yield* first.harness.harness.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              const taskId = yield* tx.createTask(
                owner,
                {},
                { ownership: { kind: "conversation" }, conversationId: first.root.id },
              );
              return (yield* tx.createConversation({ ownership: { kind: "task", taskId } })).id;
            }),
          );
          const child = Option.getOrThrow(yield* first.harness.harness.conversation(childId));
          const other = yield* first.harness.harness.createConversation({
            ownership: { kind: "ownerless" },
            agent: { model: FAUX_MODEL },
          });
          for (const conversation of [first.root, child, other]) {
            setup.faux.setResponses([calls(["echo", {}, "c1"]), DONE]);
            yield* ask(conversation, "go");
          }
          assert.deepStrictEqual(calledIn, [first.root.id, child.id]);
          yield* first.harness.close;
        }),
      ),
  );

  it.effect("applies addTools and terminates only when every result of the round asks to", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        const stop = tool("stop", () =>
          Effect.succeed({ output: [], control: { terminate: true as const } }),
        );
        const grow = tool("grow", () =>
          Effect.succeed({ output: [], control: { addTools: ["extra", "stop"] } }),
        );
        const extra = tool("extra", () => Effect.succeed({ output: [] }));
        const first = yield* runChat(backend, setup, [calls(["stop", {}, "c1"])], {
          extensions: [toolExtension(stop), toolExtension(grow), toolExtension(extra)],
          prepare: (_harness, root) => Effect.orDie(root.configure({ tools: [stop, grow] })),
        });
        assert.strictEqual(first.status, "done");
        assert.strictEqual(first.entries.at(-1)!.kind, "core.tool-result");
        const settled = yield* scanTasks(first.harness, first.root);
        assert.isTrue(settled.every((task) => task.state.status === "terminal"));

        setup.faux.setResponses([calls(["stop", {}, "c1"], ["grow", {}, "c2"]), DONE]);
        const second = yield* ask(first.root, "again");
        assert.strictEqual(second.status, "done");
        assert.strictEqual((yield* allEntries(first.root)).at(-1)!.kind, "core.assistant");
        // addTools appends to the stored tool array, skipping names it already holds.
        const agent = Option.getOrUndefined(
          yield* first.harness.session.snapshot(AgentDoc, first.root.id),
        );
        assert.deepStrictEqual(agent?.tools, ["stop", "grow", "extra"]);
        yield* first.harness.close;
      }),
    ),
  );

  it.effect("does not terminate on terminate: false", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        const keep = tool("keep", () =>
          Effect.succeed({ output: [], control: { terminate: false } }),
        );
        const { harness, entries, status } = yield* runChat(
          backend,
          setup,
          [calls(["keep", {}, "c1"]), DONE],
          {
            extensions: [toolExtension(keep)],
            prepare: (_harness, root) => Effect.orDie(root.configure({ tools: [keep] })),
          },
        );
        assert.strictEqual(status, "done");
        assert.strictEqual(entries.at(-1)!.kind, "core.assistant");
        yield* harness.close;
      }),
    ),
  );
});

describeEachBackend("generation hooks", (backend) => {
  it.effect("replaces request messages, observes responses, and continues on yield", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        const requests: Array<Array<string>> = [];
        const record: FauxResponseStep = (request) =>
          Effect.sync(() => {
            requests.push(
              request.messages.map((message) => `${message.role}:${textOf(message) ?? ""}`),
            );
            return fauxAssistantMessage([fauxText(`answer ${requests.length}`)]);
          });
        const replace = Hook.generation({
          beforeRequest: ({ messages }) =>
            Effect.succeed({
              messages: [...messages, { role: "user" as const, content: "injected", timestamp: 0 }],
            }),
        });
        const responses: Array<string> = [];
        const observe = Hook.generation({
          afterResponse: (message) =>
            Effect.sync(() => {
              responses.push(textOf(message) ?? "");
            }),
        });
        let yields = 0;
        const continueOnce = Hook.generation({
          onYield: () =>
            Effect.sync(() => (yields++ === 0 ? { continue: [text("keep going")] } : undefined)),
        });
        const { harness, entries, status } = yield* runChat(backend, setup, [record, record], {
          extensions: [hookExtension(replace), hookExtension(observe), hookExtension(continueOnce)],
        });
        assert.strictEqual(status, "done");
        assert.deepStrictEqual(requests[0]!.slice(-2), ["user:go", "user:injected"]);
        assert.deepStrictEqual(responses, ["answer 1", "answer 2"]);
        assert.deepStrictEqual(
          entries.map((entry) => entry.kind),
          ["core.user", "core.assistant", "core.user", "core.assistant"],
        );
        // The injected message was used for the request only.
        assert.isFalse(entries.some((entry) => textOf(entry.model?.[0]) === "injected"));
        yield* harness.close;
      }),
    ),
  );

  // The hook chains over the request's `onPayload(payload)`: the Harness knows the model it requests.
  it.effect("chains beforeProviderRequest over every provider request body", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        const seen: Array<{ hook: string; model: string; payload: Schema.Json }> = [];
        const first = Hook.generation({
          beforeProviderRequest: ({ model, payload }) =>
            Effect.sync(() => {
              seen.push({ hook: "first", model: `${model.provider}/${model.id}`, payload });
              return { payload: { ...asObject(payload), first: true } };
            }),
        });
        const keep = Hook.generation({
          beforeProviderRequest: ({ model, payload }) =>
            Effect.sync(() => {
              seen.push({ hook: "keep", model: `${model.provider}/${model.id}`, payload });
              return undefined;
            }),
        });
        const last = Hook.generation({
          beforeProviderRequest: ({ payload }) =>
            Effect.succeed({ payload: { ...asObject(payload), last: true } }),
        });
        const sent: Array<Schema.Json> = [];
        // A model service builds a body per provider call, and again when it resends within one attempt.
        const respond: FauxResponseStep = (request) =>
          Effect.gen(function* () {
            sent.push(yield* request.onPayload!({ body: 1 }));
            sent.push(yield* request.onPayload!({ body: 2 }));
            return DONE;
          });
        const { harness, status } = yield* runChat(backend, setup, [respond], {
          extensions: [hookExtension(first), hookExtension(keep), hookExtension(last)],
        });
        assert.strictEqual(status, "done");
        assert.deepStrictEqual(sent, [
          { body: 1, first: true, last: true },
          { body: 2, first: true, last: true },
        ]);
        assert.deepStrictEqual(seen, [
          { hook: "first", model: "faux/faux-1", payload: { body: 1 } },
          { hook: "keep", model: "faux/faux-1", payload: { body: 1, first: true } },
          { hook: "first", model: "faux/faux-1", payload: { body: 2 } },
          { hook: "keep", model: "faux/faux-1", payload: { body: 2, first: true } },
        ]);
        yield* harness.close;
      }),
    ),
  );

  // The report carries the hook's error.
  it.effect("reports a throwing beforeProviderRequest hook and continues the chain", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        const broken = Hook.generation({
          beforeProviderRequest: () => Effect.fail(new Boom({ message: "broken hook" })),
        });
        const after = Hook.generation({
          beforeProviderRequest: ({ payload }) =>
            Effect.succeed({ payload: { ...asObject(payload), after: true } }),
        });
        let sent: Schema.Json | undefined;
        const respond: FauxResponseStep = (request) =>
          Effect.gen(function* () {
            sent = yield* request.onPayload!({ body: 1 });
            return DONE;
          });
        const { harness, status } = yield* runChat(backend, setup, [respond], {
          extensions: [hookExtension(broken), hookExtension(after)],
        });
        assert.strictEqual(status, "done");
        assert.deepStrictEqual(sent, { body: 1, after: true });
        assert.deepStrictEqual(setup.reports.map(reportMessage), ["broken hook"]);
        yield* harness.close;
      }),
    ),
  );

  it.effect(
    "keeps the run's input open across an onYield continuation and answers it with the final answer",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const setup = chatSetup();
          let yields = 0;
          let harness: TestHarness | undefined;
          let root: Conversation | undefined;
          let statusAtSecondRequest: string | undefined;
          let input: Submission | undefined;
          const continueOnce = Hook.generation({
            onYield: () => Effect.sync(() => (yields++ === 0 ? { continue: "again" } : undefined)),
          });
          const second: FauxResponseStep = () =>
            Effect.gen(function* () {
              const current = yield* live(harness!, root!);
              input = Option.getOrThrow(
                yield* harness!.harness.submission(current!.run!.inputs[0]!),
              );
              statusAtSecondRequest = (yield* input.status).status;
              return fauxAssistantMessage([fauxText("second")]);
            }).pipe(Effect.orDie);
          const result = yield* runChat(
            backend,
            setup,
            [fauxAssistantMessage([fauxText("first")]), second],
            {
              extensions: [hookExtension(continueOnce)],
              prepare: (opened, conversation) =>
                Effect.sync(() => {
                  harness = opened;
                  root = conversation;
                }),
            },
          );
          assert.strictEqual(statusAtSecondRequest, "placed");
          const answers = result.entries.filter((entry) => entry.kind === "core.assistant");
          const status = yield* input!.status;
          assert.strictEqual(status.status, "done");
          assert.strictEqual("answer" in status ? status.answer : undefined, answers[1]!.id);
          yield* result.harness.close;
        }),
      ),
  );

  it.effect("observes responses that arrive by polling a deferred request", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup({ deferred: { pendingFetches: 1, pollAfterMs: 1 } });
        const observed: Array<string> = [];
        const observer = Hook.generation({
          afterResponse: (message) =>
            Effect.sync(() => {
              observed.push(`${message.stopReason}:${textOf(message) ?? ""}`);
            }),
        });
        setup.settings.stream = { deferred: true };
        const result = yield* runChat(backend, setup, [fauxAssistantMessage([fauxText("late")])], {
          extensions: [hookExtension(observer)],
        });
        assert.strictEqual(result.status, "done");
        // The still-deferred results are not terminal.
        assert.deepStrictEqual(observed, ["stop:late"]);
        yield* result.harness.close;
      }),
    ),
  );

  it.effect(
    "lets the first onYield continuation win and reports throws without stopping later handlers",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const setup = chatSetup();
          const called: Array<string> = [];
          const throwing = Hook.generation({
            afterResponse: () =>
              Effect.suspend(() => {
                called.push("throwing observer");
                return Effect.fail(new Boom({ message: "observer failed" }));
              }),
          });
          const next = Hook.generation({
            afterResponse: () =>
              Effect.sync(() => {
                called.push("next observer");
              }),
          });
          let yields = 0;
          const firstYield = Hook.generation({
            onYield: () => Effect.sync(() => (yields++ === 0 ? { continue: "first" } : undefined)),
          });
          const secondYield = Hook.generation({
            onYield: () =>
              Effect.sync(() => {
                called.push("second onYield");
                return called.filter((name) => name === "second onYield").length === 1
                  ? { continue: "second" }
                  : undefined;
              }),
          });
          const { harness, entries } = yield* runChat(
            backend,
            setup,
            [
              fauxAssistantMessage([fauxText("a")]),
              fauxAssistantMessage([fauxText("b")]),
              fauxAssistantMessage([fauxText("c")]),
            ],
            {
              extensions: [
                hookExtension(throwing),
                hookExtension(next),
                hookExtension(firstYield),
                hookExtension(secondYield),
              ],
            },
          );
          // The first continuation skips the second handler; on the next answer the second handler's
          // continuation wins.
          const users = entries
            .filter((entry) => entry.kind === "core.user")
            .map((entry) => textOf(entry.model![0]));
          assert.deepStrictEqual(users, ["go", "first", "second"]);
          assert.strictEqual(called.filter((name) => name === "second onYield").length, 2);
          assert.strictEqual(called.filter((name) => name === "next observer").length, 3);
          assert.include(setup.reports.map(reportMessage), "observer failed");
          yield* harness.close;
        }),
      ),
  );

  it.effect("keeps durable hook decisions in task memos", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        let asked = 0;
        const echo = tool("echo", () => Effect.succeed({ output: [] }));
        const repeated: Array<boolean> = [];
        const approve = Hook.tool({
          beforeTool: (_call, context) =>
            Effect.gen(function* () {
              asked++;
              const decision = yield* context.memo<string>("approval:decision", "approved");
              repeated.push(
                (yield* context.memo<string>("approval:decision", "denied")) === decision,
              );
              return undefined;
            }).pipe(Effect.orDie),
        });
        const { harness } = yield* runChat(backend, setup, [calls(["echo", {}, "c1"]), DONE], {
          extensions: [toolExtension(echo), hookExtension(approve)],
        });
        assert.strictEqual(asked, 1);
        assert.deepStrictEqual(repeated, [true]);
        yield* harness.close;
      }),
    ),
  );
});

describeEachBackend("tool execution api", (backend) => {
  it.effect(
    "builds the environment per call from the conversation's cwd and runs commits, memos, and child tasks",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const setup = chatSetup();
          const Child = Task.make({
            name: "test.child",
            version: 1,
            input: Schema.Struct({ n: Schema.Number }),
            checkpoint: Schema.Struct({ phase: Schema.Literal("run") }),
            result: Schema.Number,
            initial: () => ({ phase: "run" }),
            phases: {
              run: (task, runtime) =>
                runtime.commit(() => Effect.succeed(completed(task.input.n * 2))),
            },
            abort: () => Effect.void,
          });
          const seen: Array<unknown> = [];
          const probe = tool("probe", () =>
            Effect.gen(function* () {
              const api = yield* ToolContext;
              seen.push(api.env?.cwd);
              seen.push((yield* api.agent).tools.map((each) => each.name));
              const entry = yield* api.commit(
                Effect.gen(function* () {
                  const tx = yield* Tx;
                  // The next call runs in the new directory: its environment is built when it executes.
                  yield* configure(api.conversationId, { cwd: "/" });
                  return yield* tx.appendEntry(api.conversationId, {
                    kind: "test.note",
                    data: api.callId,
                  });
                }),
              );
              seen.push(entry.byTaskId === api.taskId);
              seen.push(yield* api.memo("m", 1), yield* api.memo("m", 2));
              const id: TaskId<number> = yield* api.createTask(
                Child,
                { n: 21 },
                { ownership: { kind: "conversation" } },
              );
              const done = yield* api.awaitTask(id);
              seen.push(done.state.outcome);
              return { output: [] };
            }).pipe(Effect.orDie),
          );
          setup.settings.toolExecution = "sequential";
          const targets: Array<unknown> = [];
          const { harness, root } = yield* runChat(
            backend,
            setup,
            [calls(["probe", {}, "c1"], ["probe", {}, "c2"]), DONE],
            {
              extensions: [Extension.make({ name: "tasks", tasks: [Child] }), toolExtension(probe)],
              env: (target) =>
                Effect.sync(() => {
                  targets.push([target.conversationId, target.cwd]);
                  return { cwd: target.cwd ?? "/tmp" };
                }),
              prepare: (_harness, root) => Effect.orDie(root.configure({ cwd: "/tmp" })),
            },
          );
          const outcome = { status: "completed", result: 42 };
          const call = (cwd: string) => [cwd, ["probe"], true, 1, 1, outcome];
          assert.deepStrictEqual(seen, [...call("/tmp"), ...call("/")]);
          assert.deepInclude(targets, [root.id, "/tmp"]);
          assert.deepInclude(targets, [root.id, "/"]);
          yield* harness.close;
        }),
      ),
  );

  it.effect(
    "answers a throwing environment with a tool_error result and reports it once while preparing",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const setup = chatSetup();
          const probe = tool("probe", () =>
            Effect.succeed({ output: [{ type: "text" as const, text: "ran" }] }),
          );
          const { harness, entries } = yield* runChat(
            backend,
            setup,
            [calls(["probe", {}, "c1"]), DONE],
            {
              extensions: [toolExtension(probe)],
              env: () => Effect.fail(new Boom({ message: "no sandbox" })),
            },
          );
          const [result] = results(entries);
          assert.strictEqual(result!.isError, true);
          assert.strictEqual(resultText(result!), "<harness>\n[error] no sandbox\n</harness>");
          // Each preparation reports the failure and renders without an environment.
          assert.strictEqual(
            setup.reports.map(reportMessage).filter((message) => message === "no sandbox").length,
            2,
          );
          yield* harness.close;
        }),
      ),
  );

  it.effect("answers a builder that throws synchronously or dies with a tool_error result", () =>
    Effect.forEach(
      [
        (): never => {
          throw new Error("no sandbox");
        },
        () => Effect.die(new Error("no sandbox")),
      ],
      (env) =>
        run(backend)(
          Effect.gen(function* () {
            const setup = chatSetup();
            const probe = tool("probe", () =>
              Effect.succeed({ output: [{ type: "text" as const, text: "ran" }] }),
            );
            const { harness, entries } = yield* runChat(
              backend,
              setup,
              [calls(["probe", {}, "c1"]), DONE],
              { extensions: [toolExtension(probe)], env },
            );
            const [result] = results(entries);
            assert.strictEqual(result!.isError, true);
            assert.strictEqual(resultText(result!), "<harness>\n[error] no sandbox\n</harness>");
            yield* harness.close;
          }),
        ),
    ),
  );

  it.effect("builds the environment from a document the builder reads through its target", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        const seen: Array<string | undefined> = [];
        const probe = tool("probe", () =>
          Effect.gen(function* () {
            const api = yield* ToolContext;
            seen.push(api.env?.cwd);
            // Only conversations with this document run in a container, from the next call on.
            yield* api.commit(
              Effect.gen(function* () {
                const tx = yield* Tx;
                (yield* tx.doc(ContainerDoc, api.conversationId)).image = "node";
              }),
            );
            return { output: [] };
          }).pipe(Effect.orDie),
        );
        setup.settings.toolExecution = "sequential";
        const { harness } = yield* runChat(
          backend,
          setup,
          [calls(["probe", {}, "c1"], ["probe", {}, "c2"]), DONE],
          {
            extensions: [toolExtension(probe)],
            env: (target) =>
              Effect.map(target.snapshot(ContainerDoc, target.conversationId), (container) =>
                Option.isSome(container)
                  ? { cwd: `${container.value.image}:${target.cwd ?? "/work"}` }
                  : { cwd: target.cwd ?? "/local" },
              ),
          },
        );
        assert.deepStrictEqual(seen, ["/local", "node:/work"]);
        yield* harness.close;
      }),
    ),
  );

  it.effect("gives a task its conversation's environment, and the builder's failure", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        const Probe = Task.make({
          name: "test.env",
          version: 1,
          input: Schema.Null,
          checkpoint: Schema.Struct({ phase: Schema.Literal("run") }),
          result: Schema.String,
          initial: () => ({ phase: "run" }),
          phases: {
            run: (_task, runtime) =>
              Effect.gen(function* () {
                const env = yield* runtime.env.pipe(
                  Effect.map((built) => built?.cwd ?? "none"),
                  Effect.catchTag("ExecutionEnvError", (error) =>
                    Effect.succeed(`failed: ${error.message}`),
                  ),
                );
                return yield* runtime.complete(env);
              }),
          },
          abort: () => Effect.void,
        });
        const { harness, root } = yield* openChat(yield* controlledStorage, setup, {
          extensions: [Extension.make({ name: "tasks", tasks: [Probe] })],
          env: (target) =>
            target.cwd === "/broken"
              ? Effect.fail(new Boom({ message: "no sandbox" }))
              : Effect.succeed({ cwd: target.cwd ?? "/" }),
        });
        const outcomes: Array<unknown> = [];
        for (const cwd of ["/one", "/broken"]) {
          yield* root.configure({ cwd });
          const id = yield* root.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              return yield* tx.createTask(Probe, null, { ownership: { kind: "conversation" } });
            }),
          );
          outcomes.push((yield* harness.harness.awaitTask(id)).state.outcome);
        }
        assert.deepStrictEqual(outcomes, [
          { status: "completed", result: "/one" },
          { status: "completed", result: "failed: no sandbox" },
        ]);
        yield* harness.close;
      }),
    ),
  );
});

describeEachBackend("tool progress and lifetime", (backend) => {
  it.effect("applies the default output limits", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        const lines = tool("lines", () =>
          Effect.gen(function* () {
            const api = yield* ToolContext;
            for (let index = 1; index <= 2500; index++) yield* api.output(`${index}\n`);
            return {};
          }),
        );
        const { harness, entries } = yield* runChat(
          backend,
          setup,
          [calls(["lines", {}, "c1"]), DONE],
          {
            extensions: [toolExtension(lines)],
          },
        );
        const output = resultText(results(entries)[0]!);
        assert.isTrue(output.startsWith("1\n2\n"));
        assert.isTrue(
          output.endsWith(
            "\n2000\n|<harness>\n[warn] Output truncated to its beginning: 500 lines, 2500 bytes dropped\n</harness>",
          ),
        );
        yield* harness.close;
      }),
    ),
  );

  it.effect(
    "sanitizes running output but keeps explicit result content as the tool returned it",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const setup = chatSetup();
          let slotOutput: string | undefined;
          const noisy = tool("noisy", () =>
            Effect.gen(function* () {
              const api = yield* ToolContext;
              yield* api.output("a\u0007b\r\n");
              yield* api.details({ ready: true });
              const current = Option.getOrUndefined(
                yield* api.snapshot(LiveDoc, api.conversationId),
              );
              slotOutput = current?.tools?.[0]?.output;
              return {};
            }).pipe(Effect.orDie),
          );
          const explicit = tool("explicit", () => Effect.succeed({ output: [text("c\u001bd")] }));
          const { harness, entries } = yield* runChat(
            backend,
            setup,
            [calls(["noisy", {}, "c1"], ["explicit", {}, "c2"]), DONE],
            { extensions: [toolExtension(noisy), toolExtension(explicit)] },
          );
          assert.strictEqual(slotOutput, "ab\n");
          const byId = new Map(
            results(entries).map((result) => [result.toolCallId, resultText(result)]),
          );
          assert.strictEqual(byId.get("c1"), "ab\n");
          assert.strictEqual(byId.get("c2"), "c\u001bd");
          yield* harness.close;
        }),
      ),
  );

  it.effect("drops control keys set to undefined instead of faulting", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        // Tools build control objects freely; a key set to `undefined` is what this case checks, and
        // `ToolControl`'s optional keys (exact optional properties) do not admit it in the type.
        const control: AgentTool.ToolControl = { addTools: ["extra"] };
        Reflect.set(control, "terminate", undefined);
        const grow = tool("grow", () => Effect.succeed({ output: [], control }));
        const extra = tool("extra", () => Effect.succeed({ output: [] }));
        const { harness, root, status } = yield* runChat(
          backend,
          setup,
          [calls(["grow", {}, "c1"]), DONE],
          {
            extensions: [toolExtension(grow), toolExtension(extra)],
            prepare: (_harness, conversation) =>
              Effect.orDie(conversation.configure({ tools: { remove: [extra] } })),
          },
        );
        assert.strictEqual(status, "done");
        // addTools deletes the name from a stored `{ remove }` filter.
        const agent = Option.getOrUndefined(yield* harness.session.snapshot(AgentDoc, root.id));
        assert.deepStrictEqual(agent?.tools, { remove: [] });
        yield* harness.close;
      }),
    ),
  );

  it.effect("uses explicit null details instead of the last reported value", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        const nulled = tool("null", () =>
          Effect.gen(function* () {
            const api = yield* ToolContext;
            yield* api.details({ old: 1 });
            return { output: [], details: null };
          }),
        );
        const { harness, entries } = yield* runChat(
          backend,
          setup,
          [calls(["null", {}, "c1"]), DONE],
          {
            extensions: [toolExtension(nulled)],
          },
        );
        assert.isNull(results(entries)[0]!.details);
        yield* harness.close;
      }),
    ),
  );

  // `details()` calls are detached fibers started immediately.
  it.effect(
    "settles details() promises with coalesced progress commits and the terminal commit",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const setup = chatSetup();
          const settled: Array<string> = [];
          const detailsTool = tool("details", () =>
            Effect.gen(function* () {
              const api = yield* ToolContext;
              const update = (n: number, name: string) =>
                Effect.forkDetach(
                  api.details({ n }).pipe(
                    Effect.andThen(
                      Effect.sync(() => {
                        settled.push(name);
                      }),
                    ),
                  ),
                  { startImmediately: true },
                );
              // Three updates in one throttle window coalesce; the last is still pending when execute() returns.
              const first = yield* update(1, "first");
              yield* update(2, "second");
              yield* Fiber.join(first);
              yield* update(3, "third");
              return { output: [] };
            }).pipe(Effect.orDie),
          );
          const { harness, entries } = yield* runChat(
            backend,
            setup,
            [calls(["details", {}, "c1"]), DONE],
            {
              extensions: [toolExtension(detailsTool)],
            },
          );
          assert.deepStrictEqual(settled, ["first", "second", "third"]);
          assert.deepStrictEqual(results(entries)[0]!.details, { n: 3 });
          yield* harness.close;
        }),
      ),
  );

  // The wait is a detached fiber; the watch is closed when its frame stream ends, consumed by a detached fiber in a
  // scope the tool's end does not close.
  it.effect(
    "rejects invocation-bound waits and stops watches when the tool's invocation ends",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const setup = chatSetup();
          const never = pendingTask("test.never");
          const watchScope = yield* Effect.scope;
          let wait: Fiber.Fiber<unknown, unknown> | undefined;
          let watchClosed: Fiber.Fiber<unknown, unknown> | undefined;
          const detach = tool("detach", () =>
            Effect.gen(function* () {
              const api = yield* ToolContext;
              // The child's definition is not registered, so it stays pending.
              const child = yield* api.createTask(
                never,
                {},
                { ownership: { kind: "conversation" } },
              );
              wait = yield* Effect.forkDetach(api.awaitTask(child), { startImmediately: true });
              const watch = Option.getOrThrow(
                yield* Scope.provide(api.watchDoc(LiveDoc, api.conversationId), watchScope),
              );
              watchClosed = yield* Effect.forkDetach(Stream.runDrain(watch.frames), {
                startImmediately: true,
              });
              return { output: [] };
            }).pipe(Effect.orDie),
          );
          const { harness } = yield* runChat(backend, setup, [calls(["detach", {}, "c1"]), DONE], {
            extensions: [toolExtension(detach)],
          });
          const waited = yield* waitForExit(wait!);
          assert.isTrue(Exit.isFailure(waited));
          if (Exit.isFailure(waited))
            assert.include(Cause.pretty(waited.cause), "invocation has ended");
          assert.isTrue(Exit.isSuccess(yield* waitForExit(watchClosed!)));
          yield* harness.close;
        }),
      ),
  );

  // The pending `details()` call is a detached fiber; `afterTool` never returns until its fiber is interrupted; a
  // rejection of the wait is its failure or interruption.
  it.effect("rejects details() still waiting when the call is aborted during afterTool", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        let pendingDetails: Fiber.Fiber<void, unknown> | undefined;
        const inAfterTool = yield* Deferred.make<void>();
        const slow = tool("slow", () =>
          Effect.gen(function* () {
            const api = yield* ToolContext;
            yield* api.output("first\n");
            // The output commit is in flight, so these details wait for the next throttle window.
            pendingDetails = yield* Effect.forkDetach(api.details({ step: 1 }), {
              startImmediately: true,
            });
            return {};
          }),
        );
        const hooks = Hook.tool({
          // The abort handler's result runs the hooks too; only the executed call's waits.
          afterTool: (_call, result) =>
            result.diagnostics?.some((diagnostic) => diagnostic.code === "aborted") === true
              ? Effect.undefined
              : Deferred.succeed(inAfterTool, undefined).pipe(Effect.andThen(Effect.never)),
        });
        setup.faux.setResponses([calls(["slow", {}, "c1"]), DONE]);
        const storage = yield* freshStorage(backend);
        const { harness, root } = yield* openChat(storage, setup, {
          extensions: [toolExtension(slow), hookExtension(hooks)],
        });
        const submission = yield* root.submit({ type: "input", content: "go" });
        yield* waitFor(Deferred.isDone(inAfterTool));
        const taskId = (yield* live(harness, root))!.tools![0]!.taskId!;
        yield* harness.harness.abortTask(taskId);
        assert.isTrue(Exit.isFailure(yield* waitForExit(pendingDetails!)));
        yield* settle(submission);
        yield* harness.close;
      }),
    ),
  );

  // A fiber the tool leaves running is detached; `afterTool` runs after `execute` returned and before the result
  // commit ends the invocation, so it lets the fiber work in that window and waits for it.
  it.effect(
    "lets a fiber left running commit after execute returned, refusing output, and refuses commits once the invocation ended",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const setup = chatSetup();
          const returned = yield* Deferred.make<void>();
          const ended = yield* Deferred.make<void>();
          let during:
            | Fiber.Fiber<
                readonly [
                  Exit.Exit<unknown, unknown>,
                  Exit.Exit<unknown, unknown>,
                  Exit.Exit<unknown, unknown>,
                ]
              >
            | undefined;
          let after: Fiber.Fiber<unknown, unknown> | undefined;
          const leaves = tool("leaves", () =>
            Effect.gen(function* () {
              const api = yield* ToolContext;
              during = yield* Effect.forkDetach(
                Deferred.await(returned).pipe(
                  Effect.andThen(
                    Effect.all(
                      [
                        Effect.exit(note(api, "during")),
                        Effect.exit(api.memo("test:during", "kept")),
                        Effect.exit(api.output("late\n")),
                      ],
                      { concurrency: 1 },
                    ),
                  ),
                ),
              );
              after = yield* Effect.forkDetach(
                Deferred.await(ended).pipe(Effect.andThen(note(api, "after"))),
              );
              return { output: [text("ok")] };
            }),
          );
          const hooks = Hook.tool({
            afterTool: () =>
              Deferred.succeed(returned, undefined).pipe(
                Effect.andThen(Effect.suspend(() => Fiber.await(during!))),
                Effect.as(undefined),
              ),
          });
          setup.faux.setResponses([calls(["leaves", {}, "c1"]), DONE]);
          const storage = yield* freshStorage(backend);
          const { harness, root } = yield* openChat(storage, setup, {
            extensions: [toolExtension(leaves), hookExtension(hooks)],
          });
          yield* ask(root, "go");
          const [committed, memo, output] = yield* Fiber.join(during!);
          assert.isTrue(Exit.isSuccess(committed));
          assert.deepStrictEqual(memo, Exit.succeed("kept"));
          assert.isTrue(Exit.isFailure(output));
          if (Exit.isFailure(output)) assert.include(Cause.pretty(output.cause), "has settled");
          yield* Deferred.succeed(ended, undefined);
          const refused = yield* waitForExit(after!);
          assert.isTrue(Exit.isFailure(refused));
          if (Exit.isFailure(refused)) {
            const error = Option.getOrUndefined(Cause.findErrorOption(refused.cause));
            assert.instanceOf(error, TaskError);
            assert.strictEqual(error.reason._tag, "InvocationEnded");
          }
          const kinds = (yield* allEntries(root)).map((entry) => entry.kind);
          assert.include(kinds, "during");
          assert.notInclude(kinds, "after");
          assert.strictEqual(resultText(results(yield* allEntries(root))[0]!), "ok");
          yield* harness.close;
        }),
      ),
  );

  it.effect(
    "answers an aborted tool with only its durable output, discarding buffered output",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const setup = chatSetup();
          const buffered = yield* Deferred.make<void>();
          const slow = tool("slow", () =>
            Effect.gen(function* () {
              const api = yield* ToolContext;
              yield* api.output("durable\n");
              // The first output commits at once; this one waits for the next throttle window.
              yield* Effect.sleep("20 millis");
              yield* api.output("buffered\n");
              yield* Deferred.succeed(buffered, undefined);
              return yield* Effect.never;
            }),
          );
          setup.faux.setResponses([calls(["slow", {}, "c1"]), DONE]);
          const storage = yield* freshStorage(backend);
          const { harness, root } = yield* openChat(storage, setup, {
            extensions: [toolExtension(slow)],
          });
          const submission = yield* root.submit({ type: "input", content: "go" });
          yield* waitFor(Deferred.isDone(buffered));
          const taskId = (yield* live(harness, root))!.tools![0]!.taskId!;
          yield* harness.harness.abortTask(taskId);
          yield* settle(submission);
          assert.strictEqual(
            resultText(results(yield* allEntries(root))[0]!),
            "durable\n|<harness>\n[error] Tool slow was aborted\n</harness>",
          );
          yield* harness.close;
        }),
      ),
  );
});

/** Appends an entry of `kind` to the call's conversation through its context. */
const note = (api: ToolContext["Service"], kind: string) =>
  api.commit(
    Effect.gen(function* () {
      const tx = yield* Tx;
      yield* tx.appendEntry(api.conversationId, { kind });
    }),
  );

/** A detached fiber's exit, once it settled while the `TestClock` advanced. */
const waitForExit = <A, E>(fiber: Fiber.Fiber<A, E>) =>
  waitFor(Effect.sync(() => fiber.pollUnsafe() !== undefined)).pipe(
    Effect.andThen(Fiber.await(fiber)),
  );

// Nested tool calls, a running tool's calls through `ToolContext.executeTool`, on every backend.
//
// - `addTool` is an extension `tool:<name>` of `openChat`, `addHooks` one holding the hooks. A fresh storage is a
//   new storage of the backend; a closed and reopened storage is the backend's storage under a second Harness.
// - Concurrent calls run with `Effect.all` with unbounded concurrency; a call left pending is a fiber forked and
//   started immediately, so its admission registers in the tool's step.
// - A tool that waits for its abort waits on `Effect.never`, which the invocation's interruption ends. A tool that
//   keeps going after its abort catches the interruption in an `uninterruptibleMask` (`afterAbort`); one that
//   ignores its abort waits uninterruptibly.
// - A wait is cancelled by interrupting the waiting fiber.
// - Failures are typed (`Boom`); rejection messages are the failures' messages.
// - The controlled storage's `commits` observes commits; `commitWith` writes a task record directly.
// - A watch listener is a consumer fiber over the watch's `frames`.
import { assert, it } from "@effect/vitest";
import * as Delta from "@effect-durable-harness/delta/Delta";
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
import * as AgentTool from "../src/AgentTool.ts";
import { AgentStorage } from "../src/AgentStorage.ts";
import {
  NestedCallDoc,
  type NestedToolExecutionResult,
  type ToolContext,
  type ToolExecutionResult,
} from "../src/AgentTool.ts";
import { AssistantEntry, ToolResultEntry } from "../src/Entry.ts";
import * as Extension from "../src/Extension.ts";
import type { Conversation } from "../src/Harness.ts";
import * as Hook from "../src/Hook.ts";
import type { ToolHookCall } from "../src/Hook.ts";
import type { SubmissionId, TaskId } from "../src/Ids.ts";
import { LiveDoc, type LiveState } from "../src/Live.ts";
import type { AssistantMessage, ToolResultMessage, Usage } from "../src/Message.ts";
import type { EntryRecord, TaskRecord } from "../src/Records.ts";
import type { Submission } from "../src/Submission.ts";
import { Tx } from "../src/Tx.ts";
import { UsageDoc } from "../src/Usage.ts";
import type { ConversationView } from "../src/View.ts";
import { ToolTask, type ToolTaskInput } from "../src/internal/tool.ts";
import { commitWrites } from "../src/testing/CommitWrites.ts";
import { fauxAssistantMessage, fauxText, fauxToolCall } from "../src/testing/FauxModel.ts";
import { allEntries, type ChatSetup, chatSetup, openChat, waitFor } from "./ChatSupport.ts";
import type { TestHarness } from "./HarnessSupport.ts";
import {
  endedAt,
  nestedResultOf,
  nestedResults,
  slotsOf,
  startOrder,
  viewHistory,
} from "./LiveSupport.ts";
import {
  type Backend,
  type ControlledStorage,
  controlledStorage,
  describeEachBackend,
  flush,
  run,
} from "./SessionSupport.ts";

/** A typed failure carrying the message. */
class Boom extends Schema.TaggedError<Boom>()("Boom", { message: Schema.String }) {}

const Parameters = Schema.Struct({ text: Schema.optionalKey(Schema.String) });
type Args = typeof Parameters.Type;
type Extra = Omit<
  AgentTool.MakeOptions<Args, Schema.Json, never, never>,
  "description" | "parameters" | "execute"
>;
type Execute<E> = (
  args: Args,
  api: ToolContext["Service"],
) => Effect.Effect<ToolExecutionResult, E, ToolContext>;

/** A tool named `name` with an empty parameter schema. */
const tool = <E>(name: string, execute: Execute<E>, extra: Extra = {}) =>
  AgentTool.make(name, { description: name, parameters: Parameters, execute, ...extra });

/** Returns `echo <text>` and counts its runs. */
const echoTool = (extra: Extra = {}) => {
  const state = { runs: 0 };
  const registration = tool(
    "echo",
    (args) =>
      Effect.sync(() => {
        state.runs++;
        return { output: [{ type: "text" as const, text: `echo ${args.text ?? ""}` }] };
      }),
    extra,
  );
  return { registration, state };
};

const toolExtension = (agentTool: AgentTool.Any) =>
  Extension.make({ name: `tool:${agentTool.name}`, tools: [agentTool] });

let hookExtensions = 0;
const hookExtension = (hook: Hook.Any) =>
  Extension.make({ name: `hooks:${++hookExtensions}`, hooks: [hook] });

const call = (name: string, args: Record<string, string> = {}, id = "c1"): AssistantMessage =>
  fauxAssistantMessage([fauxToolCall(name, args, { id })], { stopReason: "toolUse" });

const DONE = fauxAssistantMessage([fauxText("done")]);

const textItem = (value: string) => ({ type: "text" as const, text: value });

const results = (entries: ReadonlyArray<EntryRecord>): Array<ToolResultMessage> =>
  entries.flatMap((entry) => {
    const message = entry.model?.[0];
    return ToolResultEntry.is(entry) && message?.role === "toolResult" ? [message] : [];
  });

/** The text of a result message, or the value a schema-less nested call gives programs. */
const text = (result: NestedToolExecutionResult | ToolResultMessage | undefined): string => {
  if (result === undefined) return "";
  if (!("role" in result)) {
    return Predicate.isString(result.structuredOutput) ? result.structuredOutput : "";
  }
  return result.content.map((item) => (item.type === "text" ? item.text : "")).join("|");
};

/** Every tool task, in ID order. */
const toolTasks = (h: TestHarness) =>
  h.harness.commit(
    Effect.gen(function* () {
      const tx = yield* Tx;
      return (yield* tx.scanTasks({ kind: ToolTask.definition.name }, 100)).items;
    }),
  );

const inputOf = (record: { readonly input: Schema.Json } | undefined): ToolTaskInput =>
  Schema.decodeUnknownSync(ToolTask.definition.input)(record?.input);

const isNested = (record: TaskRecord): boolean => inputOf(record).kind === "nested";

const outcomeOf = (record: TaskRecord | undefined) =>
  record?.state.status === "terminal" ? record.state.outcome : undefined;

const liveOf = (h: TestHarness, root: Conversation) =>
  h.session.snapshot(LiveDoc, root.id).pipe(Effect.map(Option.getOrUndefined));

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

/** Runs `effect` to its end while the `TestClock` advances. */
const finished = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  Effect.gen(function* () {
    const fiber = yield* Effect.forkChild(effect, { startImmediately: true });
    yield* waitFor(Effect.sync(() => fiber.pollUnsafe() !== undefined));
    return yield* Fiber.join(fiber);
  });

/** Waits for a submission to settle. */
const settle = (submission: Submission) => finished(submission.await);

/** Waits for the submission `id` to settle. */
const settled = (h: TestHarness, id: SubmissionId) =>
  Effect.flatMap(h.harness.submission(id), (found) => settle(Option.getOrThrow(found)));

/** Waits for `deferred`, while the `TestClock` advances. */
const reached = (deferred: Deferred.Deferred<void>) => finished(Deferred.await(deferred));

const extensionsOf = (tools: ReadonlyArray<AgentTool.Any>, hooks: ReadonlyArray<Hook.Any> = []) => [
  ...tools.map(toolExtension),
  ...hooks.map(hookExtension),
];

/** Opens a chat over a fresh storage of the backend. */
const openFresh = (
  backend: Backend,
  setup: ChatSetup,
  tools: ReadonlyArray<AgentTool.Any>,
  hooks: ReadonlyArray<Hook.Any> = [],
) =>
  Effect.gen(function* () {
    const storage = yield* freshStorage(backend);
    const opened = yield* openChat(storage, setup, { extensions: extensionsOf(tools, hooks) });
    return { ...opened, storage };
  });

/** Opens a chat, submits "go", and waits for it. */
const runOnce = (
  backend: Backend,
  setup: ChatSetup,
  responses: ReadonlyArray<AssistantMessage>,
  tools: ReadonlyArray<AgentTool.Any>,
  hooks: ReadonlyArray<Hook.Any> = [],
) =>
  Effect.gen(function* () {
    setup.faux.setResponses(responses);
    const { harness, root, storage } = yield* openFresh(backend, setup, tools, hooks);
    const submitted = yield* settle(yield* root.submit({ type: "input", content: "go" }));
    return { harness, root, storage, status: submitted.status, entries: yield* allEntries(root) };
  });

/** A chat over the backend's own storage, as a reopened file is. */
const openOver = (
  inner: AgentStorage["Service"],
  setup: ChatSetup,
  tools: ReadonlyArray<AgentTool.Any>,
) =>
  Effect.gen(function* () {
    const storage: ControlledStorage = yield* controlledStorage.pipe(
      Effect.provideService(AgentStorage, inner),
    );
    return yield* openChat(storage, setup, { extensions: extensionsOf(tools) });
  });

/** Starts `effect` in a fiber of its own, now. */
const start = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  Effect.forkChild(Effect.ignore(effect), { startImmediately: true });

/**
 * A tool that keeps working after its abort: waits until the invocation is interrupted, then runs `after`
 * uninterruptibly.
 */
const afterAbort = <A, E, R>(after: Effect.Effect<A, E, R>) =>
  Effect.uninterruptibleMask((restore) =>
    Effect.exit(restore(Effect.never)).pipe(Effect.andThen(after)),
  );

/** A repair that sets an optional property to `undefined`, which the strict-JSON copy drops. */
const repairToUndefined = (): Schema.Json => {
  const repaired = new Map<string, string | undefined>([["text", undefined]]);
  // SAFETY: not JSON as typed, which is the point: the tool task copies it as strict JSON first.
  return Object.fromEntries(repaired) as Schema.Json;
};

const usage: Usage = {
  input: 1,
  output: 2,
  cacheRead: 0,
  cacheWrite: 0,
  totalTokens: 3,
  cost: { input: 0.1, output: 0.2, cacheRead: 0, cacheWrite: 0, total: 0.3 },
};

const nestedOf = (view: ConversationView) =>
  // SAFETY: the mount holds `core.live` as its definition materializes it.
  ((view.docs["core.live"] ?? {}) as LiveState).nestedTools ?? [];

describeEachBackend("nested tool calls", (backend) => {
  it.effect(
    "stores one document per nested call, keyed by its key, written only by that call",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const setup = chatSetup();
          const echo = echoTool();
          const batch = tool("batch", (_args, api) =>
            Effect.as(
              Effect.all(
                [
                  api.executeTool("echo", { text: "a" }),
                  api.executeTool("echo", { text: "b" }, { key: "named" }),
                ],
                { concurrency: "unbounded" },
              ),
              {},
            ),
          );
          const { harness, storage } = yield* runOnce(
            backend,
            setup,
            [call("batch"), DONE],
            [echo.registration, batch],
          );
          const written = storage.commits.flatMap((writes) =>
            writes.flatMap((write) =>
              write.type === "document.create" && write.record.kind.startsWith("core.tool.")
                ? [{ kind: write.record.kind, key: write.record.key }]
                : [],
            ),
          );
          assert.deepStrictEqual(written, [
            { kind: NestedCallDoc.definition.kind, key: "1" },
            { kind: NestedCallDoc.definition.kind, key: "named" },
          ]);
          yield* harness.close;
        }),
      ),
  );

  it.effect("rejects a nested call of an aborted caller and leaves no document for it", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        const echo = echoTool();
        const started = Deferred.makeUnsafe<void>();
        const marked = Deferred.makeUnsafe<void>();
        let rejected: unknown;
        let caller: TaskId | undefined;
        // The tool waits and calls uninterruptibly after its abort, so the admission itself must refuse.
        const late = tool("late", (_args, api) =>
          Effect.uninterruptible(
            Effect.gen(function* () {
              caller = api.taskId;
              yield* Deferred.succeed(started, undefined);
              yield* Deferred.await(marked);
              const exit = yield* Effect.exit(api.executeTool("echo", { text: "a" }));
              if (Exit.isFailure(exit)) rejected = Cause.squash(exit.cause);
              return {};
            }),
          ),
        );
        setup.faux.setResponses([call("late"), DONE]);
        const { harness, root } = yield* openFresh(backend, setup, [echo.registration, late]);
        const submission = yield* root.submit({ type: "input", content: "go" });
        yield* reached(started);
        yield* harness.kernel.commitWith((tx) =>
          Effect.gen(function* () {
            const record = Option.getOrThrow(yield* tx.service.task(caller!));
            yield* tx.setTask({ ...record, abortRequested: true });
          }),
        );
        yield* Deferred.succeed(marked, undefined);
        yield* settle(submission);
        assert.match(
          String(rejected),
          /abort mark|aborted|has settled|AbortMarked|InvocationEnded/,
        );
        assert.isTrue(Option.isNone(yield* harness.session.snapshot(NestedCallDoc, caller!, "1")));
        yield* harness.close;
      }),
    ),
  );

  it.effect(
    "runs a nested call as its own tool task and returns its result to the caller, not the transcript",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const setup = chatSetup();
          const echo = echoTool();
          let nested: NestedToolExecutionResult | undefined;
          const batch = tool("batch", (_args, api) =>
            Effect.gen(function* () {
              nested = yield* api.executeTool("echo", { text: "a" });
              return { output: [textItem(`got ${text(nested)}`)] };
            }),
          );
          const { harness, root, status, entries } = yield* runOnce(
            backend,
            setup,
            [call("batch"), DONE],
            [echo.registration, batch],
          );
          assert.strictEqual(status, "done");
          const [parent, child] = yield* toolTasks(harness);
          assert.isNumber(nested?.durationMs);
          assert.deepStrictEqual(nested, {
            taskId: child!.id,
            structuredOutput: "echo a",
            isError: false,
            diagnostics: [],
            durationMs: nested!.durationMs!,
          });
          // One result entry: the model-issued call's, which records nothing of its nested calls.
          assert.deepStrictEqual(
            results(entries).map((result) => [result.toolCallId, text(result)]),
            [["c1", "got echo a"]],
          );
          assert.notProperty(results(entries)[0], "nestedCalls");
          assert.include(inputOf(parent), { kind: "model", callId: "c1" });
          assert.strictEqual(child!.owner, parent!.id);
          assert.deepStrictEqual(inputOf(child), {
            kind: "nested",
            parent: parent!.id,
            parentCallId: "c1",
            key: "1",
            call: { type: "toolCall", id: "c1/1", name: "echo", arguments: { text: "a" } },
          });
          // The receipt stays small: the result lived in the caller's documents, which retired with it.
          assert.deepStrictEqual(child!.state, {
            status: "terminal",
            outcome: { status: "completed", result: { kind: "nested" } },
          });
          assert.isTrue(
            Option.isNone(yield* harness.session.snapshot(NestedCallDoc, parent!.id, "1")),
          );
          assert.deepStrictEqual(yield* liveOf(harness, root), {});
          yield* harness.close;
        }),
      ),
  );

  it.effect("gives each nested call its own task and call ID", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        const seen: Array<{ taskId: TaskId; callId: string }> = [];
        const who = tool("who", (_args, api) =>
          Effect.sync(() => {
            seen.push({ taskId: api.taskId, callId: api.callId });
            return {};
          }),
        );
        const batch = tool("batch", (_args, api) =>
          Effect.as(
            Effect.all(
              [api.executeTool("who", {}, { key: "a" }), api.executeTool("who", {}, { key: "b" })],
              { concurrency: "unbounded" },
            ),
            {},
          ),
        );
        const { harness } = yield* runOnce(backend, setup, [call("batch"), DONE], [who, batch]);
        assert.deepStrictEqual(seen.map((each) => each.callId).toSorted(), ["c1/a", "c1/b"]);
        assert.strictEqual(new Set(seen.map((each) => each.taskId)).size, 2);
        yield* harness.close;
      }),
    ),
  );

  it.effect("runs the tool hooks on nested calls, which see their parent", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        const echo = echoTool();
        const parents: Array<ToolHookCall["parent"]> = [];
        const hooks = Hook.tool({
          beforeTool: (hookCall) =>
            Effect.sync(() => {
              parents.push(hookCall.parent);
              return hookCall.parent !== undefined && hookCall.arguments["text"] === "secret"
                ? { block: "no secrets" }
                : undefined;
            }),
          afterTool: (hookCall, result) =>
            Effect.succeed(
              hookCall.parent === undefined
                ? undefined
                : { ...result, output: [textItem("rewritten")] },
            ),
        });
        const nested: Array<NestedToolExecutionResult> = [];
        const batch = tool("batch", (_args, api) =>
          Effect.gen(function* () {
            nested.push(yield* api.executeTool("echo", { text: "secret" }));
            nested.push(yield* api.executeTool("echo", { text: "public" }));
            return {};
          }),
        );
        const { harness } = yield* runOnce(
          backend,
          setup,
          [call("batch"), DONE],
          [echo.registration, batch],
          [hooks],
        );
        const [parent] = yield* toolTasks(harness);
        assert.deepStrictEqual(parents, [
          undefined,
          { taskId: parent!.id, callId: "c1" },
          { taskId: parent!.id, callId: "c1" },
        ]);
        assert.strictEqual(nested[0]?.isError, true);
        assert.deepStrictEqual(nested[0]?.diagnostics, [
          { severity: "error", code: "blocked", message: "Tool call blocked: no secrets" },
        ]);
        assert.strictEqual(text(nested[1]), "rewritten");
        assert.strictEqual(echo.state.runs, 1);
        yield* harness.close;
      }),
    ),
  );

  it.effect("returns an error result for an unavailable tool and invalid arguments", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        const nested: Array<NestedToolExecutionResult> = [];
        const batch = tool("batch", (_args, api) =>
          Effect.gen(function* () {
            nested.push(yield* api.executeTool("missing", {}));
            nested.push(yield* api.executeTool("echo", { text: { not: "a string" } }));
            return {};
          }),
        );
        const { harness } = yield* runOnce(
          backend,
          setup,
          [call("batch"), DONE],
          [echoTool().registration, batch],
        );
        assert.deepStrictEqual(
          nested.map((result) => [result.isError, result.diagnostics[0]?.code]),
          [
            [true, "tool_unavailable"],
            [true, "invalid_arguments"],
          ],
        );
        yield* harness.close;
      }),
    ),
  );

  it.effect(
    "returns the call a key already names, and rejects reusing the key for another call",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const setup = chatSetup();
          const echo = echoTool();
          let reused: NestedToolExecutionResult | undefined;
          const batch = tool("batch", (_args, api) =>
            Effect.gen(function* () {
              yield* api.executeTool("echo", { text: "a" }, { key: "k" });
              reused = yield* api.executeTool("echo", { text: "a" }, { key: "k" });
              yield* api.executeTool("echo", { text: "b" }, { key: "k" });
              return {};
            }),
          );
          const { harness, entries } = yield* runOnce(
            backend,
            setup,
            [call("batch"), DONE],
            [echo.registration, batch],
          );
          assert.strictEqual(echo.state.runs, 1);
          assert.strictEqual(text(reused), "echo a");
          assert.include(
            text(results(entries)[0]),
            "Nested call c1/k was already made with another tool or other arguments",
          );
          yield* harness.close;
        }),
      ),
  );

  it.effect("matches a reused key's arguments regardless of key order", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        let runs = 0;
        const pair = AgentTool.make("pair", {
          description: "pair",
          parameters: Schema.Struct({ a: Schema.String, b: Schema.String }),
          execute: (args) =>
            Effect.sync(() => {
              runs++;
              return { output: [textItem(`${args.a}${args.b}`)] };
            }),
        });
        const nested: Array<NestedToolExecutionResult> = [];
        const batch = tool("batch", (_args, api) =>
          Effect.gen(function* () {
            nested.push(yield* api.executeTool("pair", { a: "x", b: "y" }, { key: "k" }));
            nested.push(yield* api.executeTool("pair", { b: "y", a: "x" }, { key: "k" }));
            return {};
          }),
        );
        const { harness } = yield* runOnce(backend, setup, [call("batch"), DONE], [pair, batch]);
        assert.strictEqual(runs, 1);
        assert.deepStrictEqual(
          nested.map((result) => text(result)),
          ["xy", "xy"],
        );
        yield* harness.close;
      }),
    ),
  );

  it.effect(
    "aborts the nested calls a caller left running when it returns, keeping their details",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const setup = chatSetup();
          const running = Deferred.makeUnsafe<void>();
          const hang = tool("hang", (_args, api) =>
            Effect.gen(function* () {
              yield* api.output("partial\n");
              yield* api.details({ step: 1 });
              yield* Deferred.succeed(running, undefined);
              return yield* Effect.never;
            }),
          );
          const batch = tool("batch", (_args, api) =>
            Effect.gen(function* () {
              yield* start(api.executeTool("hang", {}));
              yield* Deferred.await(running);
              return { output: [textItem("returned early")] };
            }),
          );
          setup.faux.setResponses([call("batch"), DONE]);
          const { harness, root } = yield* openFresh(backend, setup, [hang, batch]);
          const history = yield* viewHistory(root);
          const nested = yield* nestedResults(harness);
          const submitted = yield* settle(yield* root.submit({ type: "input", content: "go" }));
          assert.strictEqual(submitted.status, "done");
          yield* finished(harness.harness.awaitIdle);
          history.detach();
          const [result] = results(yield* allEntries(root));
          assert.strictEqual(text(result), "returned early");
          const [, child] = yield* toolTasks(harness);
          assert.deepStrictEqual(outcomeOf(child), {
            status: "aborted",
            result: { kind: "nested" },
          });
          // The aborted call's result keeps its details; programs get no partial output, which only the model read.
          const childResult = nestedResultOf(history.live(), nested, "c1/1");
          assert.strictEqual(childResult?.isError, true);
          assert.deepStrictEqual(childResult?.details, { step: 1 });
          assert.notProperty(childResult, "structuredOutput");
          // The nested call ends before its caller.
          const live = history.live();
          assert.isAbove(endedAt(live, "c1/1"), 0);
          assert.isBelow(endedAt(live, "c1/1"), endedAt(live, "c1"));
          assert.deepStrictEqual(yield* liveOf(harness, root), {});
          yield* harness.close;
        }),
      ),
  );

  it.effect("records a nested call's usage under its tool", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        const paid = tool("paid", () => Effect.succeed({ usage }));
        const batch = tool("batch", (_args, api) => Effect.as(api.executeTool("paid", {}), {}));
        const { harness, root } = yield* runOnce(
          backend,
          setup,
          [call("batch"), DONE],
          [paid, batch],
        );
        const ledger = Option.getOrUndefined(yield* harness.session.snapshot(UsageDoc, root.id));
        assert.deepStrictEqual(ledger?.tools, { paid: usage });
        yield* harness.close;
      }),
    ),
  );

  it.effect("reports nested calls in core.live.nestedTools with their parent", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        const release = Deferred.makeUnsafe<void>();
        const running = Deferred.makeUnsafe<void>();
        const slow = tool("slow", (_args, api) =>
          Effect.gen(function* () {
            yield* api.output("working\n");
            yield* api.details({ step: 1 });
            yield* Deferred.succeed(running, undefined);
            yield* Deferred.await(release);
            return { output: [textItem("slow done")] };
          }),
        );
        const batch = tool("batch", (_args, api) =>
          Effect.gen(function* () {
            const result = yield* api.executeTool("slow", {});
            return { output: [textItem(text(result))] };
          }),
        );
        setup.faux.setResponses([call("batch"), DONE]);
        const { harness, root } = yield* openFresh(backend, setup, [slow, batch]);
        const history = yield* viewHistory(root);
        assert.isUndefined(history.live()[0]!.nestedTools);
        const nested = yield* nestedResults(harness);
        const submission = yield* root.submit({ type: "input", content: "go" });
        yield* reached(running);
        const slots = (yield* liveOf(harness, root))?.nestedTools ?? [];
        assert.strictEqual(slots.length, 1);
        assert.isNumber(slots[0]?.parentTaskId);
        assert.isNumber(slots[0]?.taskId);
        assert.deepStrictEqual(slots, [
          {
            callId: "c1/1",
            parentCallId: "c1",
            parentTaskId: slots[0]!.parentTaskId,
            name: "slow",
            taskId: slots[0]!.taskId,
            arguments: {},
            status: "running",
            output: "working\n",
            details: { step: 1 },
          },
        ]);
        yield* Deferred.succeed(release, undefined);
        yield* settle(submission);
        yield* finished(harness.harness.awaitIdle);
        history.detach();
        const states = history.live();
        assert.deepStrictEqual(startOrder(states), ["c1", "c1/1"]);
        assert.isBelow(endedAt(states, "c1/1"), endedAt(states, "c1"));
        assert.include(nestedResultOf(states, nested, "c1/1"), {
          structuredOutput: "slow done",
          isError: false,
        });
        // Slots name the call's task and, for a nested call, the calling one.
        const [parent, child] = yield* toolTasks(harness);
        assert.include(
          slotsOf(states, "c1/1").find((slot) => slot !== undefined),
          { taskId: child!.id, parentCallId: "c1", parentTaskId: parent!.id },
        );
        assert.strictEqual(
          slotsOf(states, "c1").find((slot) => slot?.taskId !== undefined)?.taskId,
          parent!.id,
        );
        assert.deepStrictEqual(yield* liveOf(harness, root), {});
        yield* harness.close;
      }),
    ),
  );

  it.effect("aborts nested calls with their caller", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        const running = Deferred.makeUnsafe<void>();
        const hang = tool("hang", () =>
          Effect.andThen(Deferred.succeed(running, undefined), Effect.never),
        );
        const batch = tool("batch", (_args, api) => Effect.as(api.executeTool("hang", {}), {}));
        setup.faux.setResponses([call("batch"), DONE]);
        const { harness, root } = yield* openFresh(backend, setup, [hang, batch]);
        const submission = yield* root.submit({ type: "input", content: "go" });
        yield* reached(running);
        yield* finished(root.abort());
        assert.strictEqual((yield* settle(submission)).status, "unanswered");
        const [parent, child] = yield* toolTasks(harness);
        assert.deepInclude(outcomeOf(child), { status: "aborted", result: { kind: "nested" } });
        assert.strictEqual(outcomeOf(parent)?.status, "aborted");
        assert.include(
          // SAFETY: an aborted tool task keeps its receipt.
          (outcomeOf(parent) as { readonly result?: { readonly kind: string } }).result,
          { kind: "model" },
        );
        const live = yield* liveOf(harness, root);
        assert.deepStrictEqual(Object.keys(live ?? {}), ["runFailure"]);
        assert.strictEqual(live?.runFailure?.reason, "aborted");
        yield* harness.close;
      }),
    ),
  );

  it.effect("aborts a running nested call when its caller throws", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        const running = Deferred.makeUnsafe<void>();
        const hang = tool("hang", () =>
          Effect.andThen(Deferred.succeed(running, undefined), Effect.never),
        );
        const batch = tool("batch", (_args, api) =>
          Effect.gen(function* () {
            yield* start(api.executeTool("hang", {}));
            yield* Deferred.await(running);
            return yield* new Boom({ message: "caller failed" });
          }),
        );
        const { harness, status, entries } = yield* runOnce(
          backend,
          setup,
          [call("batch"), DONE],
          [hang, batch],
        );
        assert.strictEqual(status, "done");
        assert.include(text(results(entries)[0]), "caller failed");
        const [, child] = yield* toolTasks(harness);
        assert.strictEqual(outcomeOf(child)?.status, "aborted");
        yield* harness.close;
      }),
    ),
  );

  it.effect("reattaches a replay-safe caller to the nested calls it made before a restart", () =>
    run(backend)(
      Effect.gen(function* () {
        const inner = yield* AgentStorage;
        const setup = chatSetup();
        const echo = echoTool();
        const blocked = Deferred.makeUnsafe<void>();
        let runs = 0;
        const batch = tool(
          "batch",
          (_args, api) =>
            Effect.gen(function* () {
              runs++;
              const first = yield* api.executeTool("echo", { text: "a" });
              if (runs === 1) {
                yield* Deferred.succeed(blocked, undefined);
                return yield* Effect.never;
              }
              // A call new to the rerun: its slot, after the reattached one's, is found and finishes.
              const second = yield* api.executeTool("echo", { text: "b" });
              return { output: [textItem(`run ${runs}: ${text(first)}, ${text(second)}`)] };
            }),
          { replay: "safe" },
        );
        const tools = [echo.registration, batch];
        setup.faux.setResponses([call("batch"), DONE]);
        let opened = yield* openOver(inner, setup, tools);
        const id = (yield* opened.root.submit({ type: "input", content: "go" })).id;
        yield* reached(blocked);
        yield* opened.harness.close;

        opened = yield* openOver(inner, setup, tools);
        yield* opened.harness.harness.resume;
        assert.strictEqual((yield* settled(opened.harness, id)).status, "done");
        assert.strictEqual(runs, 2);
        assert.strictEqual(echo.state.runs, 2);
        assert.strictEqual(
          text(results(yield* allEntries(opened.root))[0]),
          "run 2: echo a, echo b",
        );
        const tasks = yield* toolTasks(opened.harness);
        assert.strictEqual(tasks.filter(isNested).length, 2);
        const outcome = outcomeOf(tasks[1]);
        assert.deepStrictEqual(
          // SAFETY: a completed tool task's result is its receipt.
          (outcome as { readonly result?: { readonly kind: string } } | undefined)?.result?.kind,
          "nested",
        );
        yield* opened.harness.close;
      }),
    ),
  );

  it.effect(
    "stops only the wait when a caller cancels it; the call runs on and its key returns its result",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const setup = chatSetup();
          const cancel = Deferred.makeUnsafe<void>();
          const release = Deferred.makeUnsafe<void>();
          const attempts: Array<string> = [];
          let retry: NestedToolExecutionResult | undefined;
          const slow = tool("slow", () =>
            Effect.gen(function* () {
              yield* Deferred.succeed(cancel, undefined);
              yield* Deferred.await(release);
              return { output: [textItem("slow done")] };
            }),
          );
          // The caller interrupts its waiting fiber once `slow` runs.
          const batch = tool("batch", (_args, api) =>
            Effect.gen(function* () {
              const first = yield* Effect.forkChild(api.executeTool("slow", {}, { key: "same" }), {
                startImmediately: true,
              });
              yield* Deferred.await(cancel);
              yield* Fiber.interrupt(first);
              const exit = yield* Fiber.await(first);
              attempts.push(Exit.isSuccess(exit) ? "first done" : "first cancelled");
              yield* Deferred.succeed(release, undefined);
              retry = yield* api.executeTool("slow", {}, { key: "same" });
              return {};
            }),
          );
          const { harness } = yield* runOnce(backend, setup, [call("batch"), DONE], [slow, batch]);
          assert.deepStrictEqual(attempts, ["first cancelled"]);
          assert.include(retry, { isError: false, structuredOutput: "slow done" });
          assert.strictEqual((yield* toolTasks(harness)).length, 2);
          yield* harness.close;
        }),
      ),
  );

  it.effect("admits one nested call for a key used by concurrent calls", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        const echo = echoTool();
        let both: ReadonlyArray<NestedToolExecutionResult> = [];
        const batch = tool("batch", (_args, api) =>
          Effect.gen(function* () {
            both = yield* Effect.all(
              [
                api.executeTool("echo", { text: "a" }, { key: "k" }),
                api.executeTool("echo", { text: "a" }, { key: "k" }),
              ],
              { concurrency: "unbounded" },
            );
            return {};
          }),
        );
        const { harness } = yield* runOnce(
          backend,
          setup,
          [call("batch"), DONE],
          [echo.registration, batch],
        );
        assert.strictEqual(echo.state.runs, 1);
        assert.deepStrictEqual(
          both.map((result) => text(result)),
          ["echo a", "echo a"],
        );
        assert.strictEqual((yield* toolTasks(harness)).length, 2);
        yield* harness.close;
      }),
    ),
  );

  it.effect(
    "aborts left-running nested calls of a caller interrupted while it cleaned them up",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const inner = yield* AgentStorage;
          const setup = chatSetup();
          const running = Deferred.makeUnsafe<void>();
          const cleaning = Deferred.makeUnsafe<void>();
          const release = Deferred.makeUnsafe<void>();
          let hangRuns = 0;
          const hang = tool("hang", (_args, api) =>
            Effect.gen(function* () {
              hangRuns++;
              yield* api.output("partial\n");
              yield* api.details({ step: 1 });
              yield* Deferred.succeed(running, undefined);
              // The caller's cleanup waits for this abort; hold it there until the Harness closes.
              return yield* afterAbort(
                Effect.gen(function* () {
                  yield* Deferred.succeed(cleaning, undefined);
                  yield* Deferred.await(release);
                  return yield* new Boom({ message: "aborted" });
                }),
              );
            }),
          );
          const batch = tool("batch", (_args, api) =>
            Effect.gen(function* () {
              yield* start(api.executeTool("hang", {}));
              yield* Deferred.await(running);
              return { output: [textItem("returned early")] };
            }),
          );
          const tools = [hang, batch];
          setup.faux.setResponses([call("batch"), DONE]);
          let opened = yield* openOver(inner, setup, tools);
          const id = (yield* opened.root.submit({ type: "input", content: "go" })).id;
          yield* reached(cleaning);
          const closing = yield* Effect.forkChild(opened.harness.close, { startImmediately: true });
          yield* Deferred.succeed(release, undefined);
          yield* finished(Fiber.join(closing));

          opened = yield* openOver(inner, setup, tools);
          yield* opened.harness.harness.resume;
          assert.strictEqual((yield* settled(opened.harness, id)).status, "done");
          yield* finished(opened.harness.harness.awaitIdle);
          // The caller is replay-unsafe: it reports the interruption, and its cleanup aborts the nested call.
          const [result] = results(yield* allEntries(opened.root));
          assert.include(result, { toolCallId: "c1", isError: true });
          assert.include(text(result), "Tool batch was interrupted");
          const [, child] = yield* toolTasks(opened.harness);
          assert.strictEqual(outcomeOf(child)?.status, "aborted");
          assert.strictEqual(hangRuns, 1);
          assert.deepStrictEqual(yield* liveOf(opened.harness, opened.root), {});
          yield* opened.harness.close;
        }),
      ),
  );

  it.effect(
    "aborts a nested call whose admission was still committing when its caller returned",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const setup = chatSetup();
          const hang = tool("hang", () => Effect.never);
          const batch = tool("batch", (_args, api) =>
            Effect.gen(function* () {
              yield* api.executeTool("echo", { text: "warm" }, { key: "warm" });
              yield* start(api.executeTool("hang", {}, { key: "late" }));
              return {};
            }),
          );
          const { harness, root, status } = yield* runOnce(
            backend,
            setup,
            [call("batch"), DONE],
            [echoTool().registration, hang, batch],
          );
          assert.strictEqual(status, "done");
          const late = (yield* toolTasks(harness)).find((task) => {
            const input = inputOf(task);
            return input.kind === "nested" && input.call.name === "hang";
          });
          assert.strictEqual(outcomeOf(late)?.status, "aborted");
          assert.deepStrictEqual(yield* liveOf(harness, root), {});
          yield* harness.close;
        }),
      ),
  );

  it.effect("aborts every left-running nested call before waiting for any", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        const bAborted = Deferred.makeUnsafe<void>();
        const startedA = Deferred.makeUnsafe<void>();
        const startedB = Deferred.makeUnsafe<void>();
        // A only stops once B was told to stop: it ignores its own abort.
        const a = tool("a", () =>
          Effect.uninterruptible(
            Effect.gen(function* () {
              yield* Deferred.succeed(startedA, undefined);
              yield* Deferred.await(bAborted);
              return {};
            }),
          ),
        );
        const b = tool("b", () =>
          Effect.gen(function* () {
            yield* Deferred.succeed(startedB, undefined);
            return yield* afterAbort(
              Effect.gen(function* () {
                yield* Deferred.succeed(bAborted, undefined);
                return yield* new Boom({ message: "aborted" });
              }),
            );
          }),
        );
        const batch = tool("batch", (_args, api) =>
          Effect.gen(function* () {
            yield* start(api.executeTool("a", {}, { key: "a" }));
            yield* start(api.executeTool("b", {}, { key: "b" }));
            yield* Deferred.await(startedA);
            yield* Deferred.await(startedB);
            return {};
          }),
        );
        const { harness, status } = yield* runOnce(
          backend,
          setup,
          [call("batch"), DONE],
          [a, b, batch],
        );
        assert.strictEqual(status, "done");
        const nested = (yield* toolTasks(harness)).filter(isNested);
        assert.deepStrictEqual(
          nested.map((task) => task.state.status),
          ["terminal", "terminal"],
        );
        yield* harness.close;
      }),
    ),
  );

  it.effect(
    "rejects explicit keys that are empty, have a slash, are __proto__, or are positive integers",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const setup = chatSetup();
          const errors: Array<string> = [];
          const batch = tool("batch", (_args, api) =>
            Effect.gen(function* () {
              for (const key of ["", "a/b", "__proto__", "7"]) {
                const error = yield* Effect.flip(api.executeTool("echo", {}, { key }));
                errors.push(error.message);
              }
              return {};
            }),
          );
          const { harness } = yield* runOnce(
            backend,
            setup,
            [call("batch"), DONE],
            [echoTool().registration, batch],
          );
          const rule =
            'must be non-empty, without "/", not "__proto__", and not a positive integer';
          assert.deepStrictEqual(errors, [
            `Nested call key "" ${rule}`,
            `Nested call key "a/b" ${rule}`,
            `Nested call key "__proto__" ${rule}`,
            `Nested call key "7" ${rule}`,
          ]);
          yield* harness.close;
        }),
      ),
  );

  it.effect("runs nested calls of nested calls, and ends them children first", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        const inner = tool("inner", (_args, api) =>
          Effect.gen(function* () {
            const result = yield* api.executeTool("echo", { text: "deep" }, { key: "x" });
            return { output: [textItem(text(result))] };
          }),
        );
        const outer = tool("outer", (_args, api) =>
          Effect.gen(function* () {
            const result = yield* api.executeTool("inner", {});
            return { output: [textItem(text(result))] };
          }),
        );
        setup.faux.setResponses([call("outer"), DONE]);
        const { harness, root } = yield* openFresh(backend, setup, [
          echoTool().registration,
          inner,
          outer,
        ]);
        const history = yield* viewHistory(root);
        yield* settle(yield* root.submit({ type: "input", content: "go" }));
        yield* finished(harness.harness.awaitIdle);
        history.detach();
        assert.strictEqual(text(results(yield* allEntries(root))[0]), "echo deep");
        const [outerTask, innerTask, deepTask] = yield* toolTasks(harness);
        assert.deepStrictEqual([innerTask!.owner, deepTask!.owner], [outerTask!.id, innerTask!.id]);
        const states = history.live();
        assert.deepStrictEqual(startOrder(states), ["c1", "c1/1", "c1/1/x"]);
        assert.strictEqual(
          slotsOf(states, "c1/1/x").find((slot) => slot !== undefined)?.parentCallId,
          "c1/1",
        );
        // Children end first.
        assert.isBelow(endedAt(states, "c1/1/x"), endedAt(states, "c1/1"));
        assert.isBelow(endedAt(states, "c1/1"), endedAt(states, "c1"));
        assert.deepStrictEqual(yield* liveOf(harness, root), {});
        yield* harness.close;
      }),
    ),
  );

  it.effect(
    "reports the output of several running nested calls, and shows them to a late subscriber",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const setup = chatSetup();
          const release = Deferred.makeUnsafe<void>();
          const running = { one: Deferred.makeUnsafe<void>(), two: Deferred.makeUnsafe<void>() };
          const wrote = { one: Deferred.makeUnsafe<void>(), two: Deferred.makeUnsafe<void>() };
          const slow = tool("slow", (args, api) =>
            Effect.gen(function* () {
              const name = args.text === "one" ? "one" : "two";
              yield* Deferred.succeed(running[name], undefined);
              yield* Deferred.await(wrote[name]);
              yield* api.output(`${name}\n`);
              yield* api.details({ name });
              yield* Deferred.await(release);
              return {};
            }),
          );
          const batch = tool("batch", (_args, api) =>
            Effect.as(
              Effect.all(
                [
                  api.executeTool("slow", { text: "one" }),
                  api.executeTool("slow", { text: "two" }),
                ],
                { concurrency: "unbounded" },
              ),
              {},
            ),
          );
          setup.faux.setResponses([call("batch"), DONE]);
          const { harness, root } = yield* openFresh(backend, setup, [slow, batch]);
          const submission = yield* root.submit({ type: "input", content: "go" });
          yield* reached(running.one);
          yield* reached(running.two);
          const scope = yield* Scope.make();
          const watch = yield* Scope.provide(root.watch(), scope);
          assert.deepStrictEqual(
            nestedOf(watch.value).map((slot) => [slot.callId, slot.status]),
            [
              ["c1/1", "running"],
              ["c1/2", "running"],
            ],
          );
          // Each value's operations rebuild the next one, the late subscriber's starting value included.
          let replayed: ConversationView = watch.value;
          const outputs: Array<readonly [string | undefined, string | undefined]> = [];
          const replays: Array<readonly [ConversationView, ConversationView]> = [];
          const consumer = yield* Effect.forkChild(
            Stream.runForEach(watch.frames, (frame) =>
              Effect.sync(() => {
                replayed = Delta.applyImmutableUnsafe(replayed, frame.ops);
                replays.push([replayed, frame.value]);
                const [one, two] = nestedOf(frame.value);
                outputs.push([one?.output, two?.output]);
              }),
            ),
            { startImmediately: true },
          );
          // The second slot writes first, so a wrong slot index in the update path would show.
          yield* Deferred.succeed(wrote.two, undefined);
          yield* waitFor(Effect.sync(() => outputs.at(-1)?.[1] === "two\n"));
          assert.deepStrictEqual(outputs.at(-1), [undefined, "two\n"]);
          yield* Deferred.succeed(wrote.one, undefined);
          yield* waitFor(Effect.sync(() => outputs.at(-1)?.[0] === "one\n"));
          assert.deepStrictEqual(outputs.at(-1), ["one\n", "two\n"]);
          yield* Deferred.succeed(release, undefined);
          yield* settle(submission);
          yield* flush;
          yield* Fiber.interrupt(consumer);
          yield* Scope.close(scope, Exit.void);
          for (const [rebuilt, value] of replays) assert.deepStrictEqual(rebuilt, value);
          yield* harness.close;
        }),
      ),
  );

  it.effect(
    "commits only the status of a nested call made with progress: false, and keeps its output in the result",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const setup = chatSetup();
          const running = Deferred.makeUnsafe<void>();
          const release = Deferred.makeUnsafe<void>();
          const chatty = tool("chatty", (_args, api) =>
            Effect.gen(function* () {
              yield* api.output("line\n");
              yield* api.diagnostic({ severity: "info", message: "note" });
              yield* api.details({ step: 1 });
              yield* Deferred.succeed(running, undefined);
              yield* Deferred.await(release);
              return {};
            }),
          );
          let result: NestedToolExecutionResult | undefined;
          const batch = tool("batch", (_args, api) =>
            Effect.gen(function* () {
              result = yield* api.executeTool("chatty", {}, { progress: false });
              return {};
            }),
          );
          setup.faux.setResponses([call("batch"), DONE]);
          const { harness, root } = yield* openFresh(backend, setup, [chatty, batch]);
          const submission = yield* root.submit({ type: "input", content: "go" });
          // `details` resolved before this, so a progress commit would have landed already.
          yield* reached(running);
          const [slot] = (yield* liveOf(harness, root))?.nestedTools ?? [];
          assert.deepStrictEqual(slot, {
            callId: "c1/1",
            parentCallId: "c1",
            parentTaskId: slot!.parentTaskId,
            name: "chatty",
            taskId: slot!.taskId,
            arguments: {},
            status: "running",
          });
          yield* Deferred.succeed(release, undefined);
          yield* settle(submission);
          assert.strictEqual(result?.structuredOutput, "line\n");
          assert.deepStrictEqual(result?.details, { step: 1 });
          assert.deepStrictEqual(result?.diagnostics, [{ severity: "info", message: "note" }]);
          yield* harness.close;
        }),
      ),
  );

  it.effect(
    "loses the running output of a nested call made with progress: false when it is aborted",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const setup = chatSetup();
          const running = Deferred.makeUnsafe<void>();
          const chatty = tool("chatty", (_args, api) =>
            Effect.gen(function* () {
              yield* api.output("line\n");
              yield* api.diagnostic({ severity: "info", message: "note" });
              yield* api.details({ step: 1 });
              yield* Deferred.succeed(running, undefined);
              return yield* Effect.never;
            }),
          );
          const batch = tool("batch", (_args, api) =>
            Effect.gen(function* () {
              yield* start(api.executeTool("chatty", {}, { progress: false }));
              yield* Deferred.await(running);
              return {};
            }),
          );
          setup.faux.setResponses([call("batch"), DONE]);
          const { harness, root } = yield* openFresh(backend, setup, [chatty, batch]);
          const history = yield* viewHistory(root);
          const nested = yield* nestedResults(harness);
          yield* settle(yield* root.submit({ type: "input", content: "go" }));
          yield* finished(harness.harness.awaitIdle);
          history.detach();
          // The abort's result is built from the slot, which never got the output.
          const result = nestedResultOf(history.live(), nested, "c1/1");
          assert.strictEqual(result?.isError, true);
          assert.notProperty(result, "structuredOutput");
          assert.notProperty(result, "details");
          assert.deepStrictEqual(
            result?.diagnostics.map((diagnostic) => diagnostic.code),
            ["aborted"],
          );
          yield* harness.close;
        }),
      ),
  );

  it.effect(
    "shows a nested call's arguments in its slot: as made, then, unbounded, as it runs with them",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const setup = chatSetup();
          const hookEntered = Deferred.makeUnsafe<void>();
          const hookRelease = Deferred.makeUnsafe<void>();
          const running = Deferred.makeUnsafe<void>();
          const release = Deferred.makeUnsafe<void>();
          const large = "x".repeat(64 * 1024);
          const slow = tool("slow", () =>
            Effect.gen(function* () {
              yield* Deferred.succeed(running, undefined);
              yield* Deferred.await(release);
              return {};
            }),
          );
          const hooks = Hook.tool({
            beforeTool: (hookCall) =>
              Effect.gen(function* () {
                if (hookCall.parent === undefined) return undefined;
                yield* Deferred.succeed(hookEntered, undefined);
                yield* Deferred.await(hookRelease);
                return { arguments: { text: large } };
              }),
          });
          const batch = tool("batch", (_args, api) =>
            Effect.as(api.executeTool("slow", { text: "asked" }), {}),
          );
          setup.faux.setResponses([call("batch"), DONE]);
          const { harness, root } = yield* openFresh(backend, setup, [slow, batch], [hooks]);
          const submission = yield* root.submit({ type: "input", content: "go" });
          const slot = Effect.map(liveOf(harness, root), (live) => live?.nestedTools?.[0]);
          yield* reached(hookEntered);
          assert.include(yield* slot, { status: "pending" });
          assert.deepStrictEqual((yield* slot)?.arguments, { text: "asked" });
          yield* Deferred.succeed(hookRelease, undefined);
          yield* reached(running);
          assert.include(yield* slot, { status: "running" });
          assert.deepStrictEqual((yield* slot)?.arguments, { text: large });
          yield* Deferred.succeed(release, undefined);
          yield* settle(submission);
          yield* finished(harness.harness.awaitIdle);
          // The slot showed the arguments the call runs with; the task input keeps the ones it was made with.
          const nested = (yield* toolTasks(harness)).find(isNested);
          const input = inputOf(nested);
          assert.deepStrictEqual(input.kind === "nested" ? input.call.arguments : undefined, {
            text: "asked",
          });
          yield* harness.close;
        }),
      ),
  );

  it.effect(
    "runs a nested call whose repaired arguments set an optional property to undefined",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const setup = chatSetup();
          const seen: Array<Args> = [];
          const repaired = tool(
            "repaired",
            (args) =>
              Effect.sync(() => {
                seen.push(args);
                return {};
              }),
            { prepareArguments: repairToUndefined },
          );
          let result: NestedToolExecutionResult | undefined;
          const batch = tool("batch", (_args, api) =>
            Effect.gen(function* () {
              result = yield* api.executeTool("repaired", { text: "x" });
              return {};
            }),
          );
          const { harness } = yield* runOnce(
            backend,
            setup,
            [call("batch"), DONE],
            [repaired, batch],
          );
          assert.strictEqual(result?.isError, false);
          assert.deepStrictEqual(seen, [{}]);
          yield* harness.close;
        }),
      ),
  );

  it.effect("finds each nested call's slot after slots in the middle were removed", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        const inner = tool("inner", (_args, api) =>
          Effect.gen(function* () {
            yield* api.executeTool("echo", { text: "m1" });
            yield* api.executeTool("echo", { text: "m2" });
            return {};
          }),
        );
        const before = Deferred.makeUnsafe<void>();
        const checked = Deferred.makeUnsafe<void>();
        let slots: Array<{ callId: string; status: string; summary?: unknown }> = [];
        const batch = tool("batch", (_args, api) =>
          Effect.gen(function* () {
            yield* api.executeTool("echo", { text: "n1" });
            // n2 makes m1 and m2 and settles, which removes them from between n2 and n3.
            yield* api.executeTool("inner", {});
            yield* api.executeTool("echo", { text: "n3" });
            yield* api.executeTool("echo", { text: "n4" });
            const live = Option.getOrUndefined(yield* api.snapshot(LiveDoc, api.conversationId));
            slots = (live?.nestedTools ?? []).map((slot) => ({
              callId: slot.callId,
              status: slot.status,
              summary: slot.summary,
            }));
            yield* Deferred.succeed(before, undefined);
            yield* Deferred.await(checked);
            return {};
          }),
        );
        setup.faux.setResponses([call("batch"), DONE]);
        const { harness, root } = yield* openFresh(backend, setup, [
          echoTool().registration,
          inner,
          batch,
        ]);
        const submission = yield* root.submit({ type: "input", content: "go" });
        yield* reached(before);
        assert.deepStrictEqual(
          slots.map((slot) => slot.callId),
          ["c1/1", "c1/2", "c1/3", "c1/4"],
        );
        for (const slot of slots) {
          assert.strictEqual(slot.status, "done");
          // SAFETY: a done nested slot carries its summary.
          const summary = slot.summary as {
            readonly isError: boolean;
            readonly durationMs: number;
          };
          assert.deepStrictEqual(Object.keys(summary).toSorted(), ["durationMs", "isError"]);
          assert.strictEqual(summary.isError, false);
          assert.isNumber(summary.durationMs);
        }
        yield* Deferred.succeed(checked, undefined);
        yield* settle(submission);
        yield* harness.close;
      }),
    ),
  );

  it.effect(
    "gives callers a result with its task ID and only diagnostics for a nested call the scheduler faulted",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const setup = chatSetup();
          // Details that are not JSON make the result commit fail, so the task faults and stores no result: a
          // non-finite number, which the types admit and the strict-JSON copy refuses.
          const echo = tool("echo", () => Effect.succeed({ details: { count: Number.NaN } }));
          let result: NestedToolExecutionResult | undefined;
          const batch = tool("batch", (_args, api) =>
            Effect.gen(function* () {
              result = yield* api.executeTool("echo", { text: "a" });
              return {};
            }),
          );
          setup.faux.setResponses([call("batch"), DONE]);
          const { harness, root } = yield* openFresh(backend, setup, [echo, batch]);
          yield* settle(yield* root.submit({ type: "input", content: "go" }));
          const [, child] = yield* toolTasks(harness);
          assert.strictEqual(outcomeOf(child)?.status, "faulted");
          assert.strictEqual(result?.taskId, child!.id);
          assert.strictEqual(result?.isError, true);
          assert.deepStrictEqual(Object.keys(result ?? {}).toSorted(), [
            "diagnostics",
            "isError",
            "taskId",
          ]);
          assert.strictEqual(result?.diagnostics.length, 1);
          assert.include(result?.diagnostics[0], { severity: "error", code: "faulted" });
          assert.include(result?.diagnostics[0]?.message, "Tool echo failed");
          yield* harness.close;
        }),
      ),
  );

  it.effect("gives callers what a schema-less tool streamed before it threw", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        const fails = tool("fails", (_args, api) =>
          Effect.gen(function* () {
            yield* api.output("half way\n");
            return yield* new Boom({ message: "broke" });
          }),
        );
        let result: NestedToolExecutionResult | undefined;
        const batch = tool("batch", (_args, api) =>
          Effect.gen(function* () {
            result = yield* api.executeTool("fails", {});
            return {};
          }),
        );
        const { harness } = yield* runOnce(backend, setup, [call("batch"), DONE], [fails, batch]);
        assert.strictEqual(result?.isError, true);
        assert.strictEqual(result?.structuredOutput, "half way\n");
        assert.deepStrictEqual(
          result?.diagnostics.map((diagnostic) => [diagnostic.code, diagnostic.message]),
          [["tool_error", "broke"]],
        );
        yield* harness.close;
      }),
    ),
  );

  it.effect(
    "shows a finished nested call as a done slot with its summary until its caller settles",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const setup = chatSetup();
          const release = Deferred.makeUnsafe<void>();
          const checked = Deferred.makeUnsafe<void>();
          // Diagnostics win over output for the error text, which is cut to 500 characters.
          const verbose = tool("verbose", () =>
            Effect.succeed({
              output: [textItem("ignored")],
              isError: true,
              diagnostics: [{ severity: "error" as const, message: "e".repeat(600) }],
            }),
          );
          const paid = tool("paid", () => Effect.succeed({ usage }));
          // An error with mixed content: programs get the list, the summary its text.
          const broken = tool("broken", () =>
            Effect.succeed({
              output: [
                textItem("bad image"),
                { type: "image" as const, data: "AAAA", mimeType: "image/png" },
              ],
              isError: true,
            }),
          );
          const batch = tool("batch", (_args, api) =>
            Effect.gen(function* () {
              yield* api.executeTool("echo", { text: "a" });
              yield* api.executeTool("missing", {});
              yield* api.executeTool("paid", {});
              yield* api.executeTool("broken", {});
              yield* api.executeTool("verbose", {});
              yield* Deferred.succeed(checked, undefined);
              yield* Deferred.await(release);
              return {};
            }),
          );
          setup.faux.setResponses([call("batch"), DONE]);
          const { harness, root } = yield* openFresh(backend, setup, [
            verbose,
            echoTool().registration,
            paid,
            broken,
            batch,
          ]);
          const submission = yield* root.submit({ type: "input", content: "go" });
          yield* reached(checked);
          const slots = (yield* liveOf(harness, root))?.nestedTools ?? [];
          const durationOf = (index: number) => slots[index]?.summary?.durationMs ?? -1;
          for (const index of [0, 2, 3, 4]) assert.isAtLeast(durationOf(index), 0);
          assert.deepStrictEqual(
            slots.map((slot) => ({
              callId: slot.callId,
              status: slot.status,
              summary: slot.summary,
            })),
            [
              {
                callId: "c1/1",
                status: "done",
                summary: { isError: false, durationMs: durationOf(0) },
              },
              {
                callId: "c1/2",
                status: "done",
                summary: {
                  isError: true,
                  durationMs: durationOf(1),
                  error: "Tool missing is not available",
                },
              },
              {
                callId: "c1/3",
                status: "done",
                summary: { isError: false, durationMs: durationOf(2), usage },
              },
              {
                callId: "c1/4",
                status: "done",
                summary: { isError: true, durationMs: durationOf(3), error: "bad image" },
              },
              {
                callId: "c1/5",
                status: "done",
                summary: { isError: true, durationMs: durationOf(4), error: "e".repeat(500) },
              },
            ],
          );
          yield* Deferred.succeed(release, undefined);
          yield* settle(submission);
          assert.deepStrictEqual(yield* liveOf(harness, root), {});
          yield* harness.close;
        }),
      ),
  );

  it("migrates version 1 tool task input to a model-issued call", () => {
    const migrate = ToolTask.definition.migrate!;
    assert.deepStrictEqual<unknown>(migrate({ assistant: 3, callId: "c1" }, { phase: "call" }, 1), {
      input: { kind: "model", assistant: 3, callId: "c1" },
      checkpoint: { phase: "call" },
    });
  });

  // #11043
  it("keeps version 2 tool task input as it is", () => {
    const migrate = ToolTask.definition.migrate!;
    const model = { kind: "model", assistant: 3, callId: "c1" };
    const nested = {
      kind: "nested",
      parent: 7,
      parentCallId: "c1",
      key: "k",
      call: { type: "toolCall", id: "c1/k", name: "echo", arguments: {} },
    };
    assert.deepStrictEqual<unknown>(migrate(model, { phase: "call" }, 2).input, model);
    assert.deepStrictEqual<unknown>(migrate(nested, { phase: "call" }, 2).input, nested);
  });
});

describeEachBackend("tool task version 1 and 2 records", (backend) => {
  /**
   * Stores a live tool task for call `c1` of an assistant entry, closes, and rewrites its
   * record as `version` stored it, with `checkpoint`: version 1 had `{ assistant, callId }` input, version 2
   * added `kind`. Returns the task ID.
   */
  const storeVersion = (
    inner: AgentStorage["Service"],
    setup: ChatSetup,
    tools: ReadonlyArray<AgentTool.Any>,
    checkpoint: Schema.Json,
    version: 1 | 2 = 1,
  ) =>
    Effect.gen(function* () {
      // No submission, so the Harness never starts scheduling and the task stays pending.
      const { harness, root } = yield* openOver(inner, setup, tools);
      const id = yield* root.commit(
        Effect.gen(function* () {
          const tx = yield* Tx;
          const entry = yield* tx.appendEntry(AssistantEntry, root.id, {
            model: [call("echo", { text: "old" })],
          });
          return yield* tx.createTask(
            ToolTask,
            { kind: "model", assistant: entry.id, callId: "c1" },
            { ownership: { kind: "conversation" } },
          );
        }),
      );
      yield* harness.close;
      const record = Option.getOrThrow(yield* inner.task(id));
      const input = inputOf(record);
      const versioned =
        version === 1 && input.kind === "model"
          ? { assistant: input.assistant, callId: input.callId }
          : record.input;
      // SAFETY: the record with its version, input and checkpoint as the old version stored them.
      const value = {
        ...record,
        version,
        input: versioned,
        state: { ...record.state, checkpoint },
      } as TaskRecord;
      yield* commitWrites(inner, [{ type: "task", value }]);
      return id;
    });

  /** Waits for the task `id`, while the `TestClock` advances. */
  const awaitTask = (h: TestHarness, id: TaskId) => finished(h.harness.awaitTask(id));

  it.effect("migrates a pending version 1 task and runs it as a model-issued call", () =>
    run(backend)(
      Effect.gen(function* () {
        const inner = yield* AgentStorage;
        const setup = chatSetup();
        const echo = echoTool();
        const tools = [echo.registration];
        const id = yield* storeVersion(inner, setup, tools, { phase: "call" });
        const { harness, root } = yield* openOver(inner, setup, tools);
        assert.strictEqual(Option.getOrThrow(yield* harness.harness.getTask(id)).version, 1);
        yield* harness.harness.resume;
        const done = yield* awaitTask(harness, id);
        assert.strictEqual(done.version, 4);
        assert.include(inputOf(done), { kind: "model", callId: "c1" });
        assert.strictEqual(done.state.outcome.status, "completed");
        assert.strictEqual(echo.state.runs, 1);
        const [result] = results(yield* allEntries(root));
        assert.deepStrictEqual([result!.toolCallId, text(result)], ["c1", "echo old"]);
        yield* harness.close;
      }),
    ),
  );

  it.effect("migrates a version 1 task interrupted after intent and settles it interrupted", () =>
    run(backend)(
      Effect.gen(function* () {
        const inner = yield* AgentStorage;
        const setup = chatSetup();
        const echo = echoTool();
        const tools = [echo.registration];
        const intent = { phase: "execute", arguments: { text: "old" }, replay: "unsafe" };
        const id = yield* storeVersion(inner, setup, tools, intent);
        const { harness, root } = yield* openOver(inner, setup, tools);
        yield* harness.harness.resume;
        const done = yield* awaitTask(harness, id);
        assert.strictEqual(done.version, 4);
        assert.strictEqual(done.state.outcome.status, "failed");
        assert.strictEqual(echo.state.runs, 0);
        const [result] = results(yield* allEntries(root));
        assert.include(result, { toolCallId: "c1", isError: true });
        assert.include(text(result), "Tool echo was interrupted");
        yield* harness.close;
      }),
    ),
  );

  // #11043
  it.effect(
    "reserves a stored version 2 task under version 4 and runs it as a model-issued call",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const inner = yield* AgentStorage;
          const setup = chatSetup();
          const echo = echoTool();
          const tools = [echo.registration];
          const id = yield* storeVersion(inner, setup, tools, { phase: "call" }, 2);
          const { harness, root } = yield* openOver(inner, setup, tools);
          assert.strictEqual(Option.getOrThrow(yield* harness.harness.getTask(id)).version, 2);
          yield* harness.harness.resume;
          const done = yield* awaitTask(harness, id);
          assert.strictEqual(done.version, 4);
          const input = inputOf(done);
          assert.deepStrictEqual(Object.keys(input).toSorted(), ["assistant", "callId", "kind"]);
          assert.include(input, { kind: "model", callId: "c1" });
          assert.strictEqual(done.state.outcome.status, "completed");
          assert.strictEqual(echo.state.runs, 1);
          const [result] = results(yield* allEntries(root));
          assert.deepStrictEqual([result!.toolCallId, text(result)], ["c1", "echo old"]);
          yield* harness.close;
        }),
      ),
  );
});

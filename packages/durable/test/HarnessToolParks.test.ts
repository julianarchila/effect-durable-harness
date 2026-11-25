// Tools that park: a sleep on signals or time, a wait on owned tasks, the parked slot, the run after the wake,
// invalid parks, cleanup through `cancel`, recovery, and wrappers.
//
// Notes:
// - A reopened process is a second Harness over the same storage, on every backend; a crash kills the first one's
//   storage (`HarnessSupport.killable`).
// - The registry is static: "a tool that is gone" reopens the Harness without the tool's extension.
// - The abort reaches a running tool as its interruption; "cleans up through its context too" observes it with
//   `Effect.onInterrupt`. A tool cannot ignore it and park anyway, so "runs in the run itself when the park's
//   commit finds the call abort-marked" holds the Session line until the abort's commit is queued before the
//   park's. The cancel the refused commit runs is uninterruptible.
// - The Harness clock is the `TestClock`, which `waitFor` advances in 5 ms steps, so spans are checked as bounds.
import { assert, it } from "@effect/vitest";
import * as Clock from "effect/Clock";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as TestClock from "effect/testing/TestClock";
import * as AgentTool from "../src/AgentTool.ts";
import { ToolResultEntry } from "../src/Entry.ts";
import type { ExecutionEnvBuilder } from "../src/ExecutionEnv.ts";
import * as Extension from "../src/Extension.ts";
import type { Conversation } from "../src/Harness.ts";
import type { SubmissionId, TaskId } from "../src/Ids.ts";
import { LiveDoc } from "../src/Live.ts";
import type { AssistantMessage, ToolResultMessage } from "../src/Message.ts";
import type { EntryRecord, WakeReason } from "../src/Records.ts";
import * as Signal from "../src/Signal.ts";
import type { Submission } from "../src/Submission.ts";
import * as Task from "../src/Task.ts";
import { AgentStorage } from "../src/AgentStorage.ts";
import { fauxAssistantMessage, fauxText, fauxToolCall } from "../src/testing/FauxModel.ts";
import { allEntries, type ChatSetup, chatSetup, openChat, waitFor } from "./ChatSupport.ts";
import { abortedWith, holdLine, killable, type TestHarness } from "./HarnessSupport.ts";
import {
  type ControlledStorage,
  controlledStorage,
  describeEachBackend,
  flush,
  run,
} from "./SessionSupport.ts";

/** A failure of the environment builder. */
class EnvFailed extends Schema.TaggedError<EnvFailed>()("EnvFailed", {
  message: Schema.String,
}) {}

/** A failure of a tool's `cancel`. */
class CancelFailed extends Schema.TaggedError<CancelFailed>()("CancelFailed", {
  message: Schema.String,
}) {}

const Done = Signal.make("test.done", {
  version: 1,
  schema: Schema.Struct({ text: Schema.String }),
});

const NoParameters = Schema.Struct({});
type Execute = AgentTool.ToolDefinition<
  typeof NoParameters.Type,
  Schema.Json,
  unknown,
  AgentTool.ToolContext
>["execute"];
type Extra = Omit<
  AgentTool.MakeOptions<typeof NoParameters.Type, Schema.Json, unknown, AgentTool.ToolContext>,
  "description" | "parameters" | "execute"
>;

/** A tool without parameters, described by its name. */
const tool = (name: string, execute: Execute, extra: Extra = {}) =>
  AgentTool.make(name, { description: name, parameters: NoParameters, execute, ...extra });

/** Installs `agentTool` as an extension of its own. */
const toolExtension = (agentTool: AgentTool.Any) =>
  Extension.make({ name: `tool:${agentTool.name}`, tools: [agentTool] });

const call = (name: string, id = "c1"): AssistantMessage =>
  fauxAssistantMessage([fauxToolCall(name, {}, { id })], { stopReason: "toolUse" });

const DONE = fauxAssistantMessage([fauxText("done")]);

const results = (entries: ReadonlyArray<EntryRecord>): Array<ToolResultMessage> =>
  entries.flatMap((entry) => {
    const message = entry.model?.[0];
    return ToolResultEntry.is(entry) && message?.role === "toolResult" ? [message] : [];
  });

const text = (message: ToolResultMessage | undefined): string =>
  (message?.content ?? []).map((item) => (item.type === "text" ? item.text : "")).join("|");

/** What one run of a parking tool saw. */
interface Run {
  readonly wake: WakeReason | undefined;
  readonly checkpoint: Schema.Json | undefined;
}

/** A tool that reports, then parks on `Done` until the key holds a value, and answers with it. */
const signalTool = (runs: Array<Run>, extra: Extra = {}) =>
  tool(
    "ci",
    (_args, api) =>
      Effect.gen(function* () {
        runs.push({ wake: api.wake, checkpoint: api.checkpoint });
        const state = Option.getOrUndefined(yield* api.readSignal(Done, "done"));
        if (state !== undefined && "value" in state) return AgentTool.text(state.value.text);
        yield* api.output("started\n");
        yield* api.diagnostic({ severity: "info", message: `run ${runs.length}` });
        yield* api.details({ build: runs.length });
        return AgentTool.sleep({ checkpoint: { build: runs.length }, signals: [Done.key("done")] });
      }),
    extra,
  );

/** A Harness over `storage` that schedules at once. */
const open = <E = never>(
  storage: ControlledStorage,
  setup: ChatSetup,
  extensions: ReadonlyArray<Extension.Any>,
  env?: ExecutionEnvBuilder<never, E>,
) =>
  Effect.gen(function* () {
    const opened = yield* openChat(storage, setup, { extensions, env });
    yield* opened.harness.harness.resume;
    return opened;
  });

/** The first tool slot of `root`. */
const slotOf = (h: TestHarness, root: Conversation) =>
  Effect.map(
    h.session.snapshot(LiveDoc, root.id),
    (live) => Option.getOrUndefined(live)?.tools?.[0],
  );

/** The tool task of the first slot, once it is `status`. */
const parkedTask = (
  h: TestHarness,
  root: Conversation,
  status: "sleeping" | "waiting" = "sleeping",
) =>
  Effect.gen(function* () {
    let id: TaskId | undefined;
    yield* waitFor(
      Effect.gen(function* () {
        id = (yield* slotOf(h, root))?.taskId;
        if (id === undefined) return false;
        return Option.getOrUndefined(yield* h.harness.getTask(id))?.state.status === status;
      }),
    );
    return id!;
  });

/** Awaits the submission while the `TestClock` advances in 5 ms steps. */
const settle = (submission: Submission) =>
  Effect.gen(function* () {
    const fiber = yield* Effect.forkChild(submission.await, { startImmediately: true });
    yield* waitFor(Effect.sync(() => fiber.pollUnsafe() !== undefined));
    return yield* Fiber.join(fiber);
  });

/** Awaits a submission found by ID, as `settle` does. */
const settleById = (h: TestHarness, id: SubmissionId) =>
  Effect.flatMap(h.harness.submission(id), (found) => settle(Option.getOrThrow(found)));

/** Whether the Session needs no process now: nothing runs and no sleep is due. */
const inactive = (h: TestHarness) =>
  waitFor(
    Effect.gen(function* () {
      const now = yield* Clock.currentTimeMillis;
      const wake = yield* h.harness.wake;
      const next = Option.getOrUndefined(wake.nextWakeAt);
      return !wake.activeWork && (next === undefined || next > now);
    }),
  );

/** The messages of the reports collected so far. */
const reportMessages = (setup: ChatSetup): Array<string> =>
  setup.reports.flatMap((report) => {
    const error = report.cause.reasons.flatMap((reason) =>
      reason._tag === "Fail" ? [reason.error] : reason._tag === "Die" ? [reason.defect] : [],
    )[0];
    return error instanceof Error ? [error.message] : [];
  });

describeEachBackend("parking on a signal", (backend) => {
  it.effect(
    "holds no invocation, shows the slot sleeping without output, and runs again once woken",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const setup = chatSetup();
          const runs: Array<Run> = [];
          setup.faux.setResponses([call("ci"), DONE]);
          const { harness, root } = yield* open(yield* controlledStorage, setup, [
            toolExtension(signalTool(runs)),
          ]);
          const submission = yield* root.submit({ type: "input", content: "go" });
          const id = yield* parkedTask(harness, root);
          assert.deepStrictEqual(Option.getOrUndefined(yield* harness.harness.getTask(id))?.state, {
            status: "sleeping",
            checkpoint: {
              phase: "parked",
              arguments: {},
              replay: "unsafe",
              checkpoint: { build: 1 },
            },
            signals: [{ signal: "test.done", key: "done" }],
          });
          // Details and diagnostics stay, the output goes.
          const slot = yield* slotOf(harness, root);
          assert.strictEqual(slot?.status, "sleeping");
          assert.deepStrictEqual(slot?.details, { build: 1 });
          assert.deepStrictEqual(slot?.diagnostics, [{ severity: "info", message: "run 1" }]);
          assert.isUndefined(slot?.output);
          yield* inactive(harness);

          assert.strictEqual(
            yield* harness.harness.signal(id, Done, "done", { text: "passed" }),
            "delivered",
          );
          assert.strictEqual((yield* settle(submission)).status, "done");
          assert.deepStrictEqual(runs, [
            { wake: undefined, checkpoint: undefined },
            {
              wake: { reason: "signal", signal: "test.done", key: "done" },
              checkpoint: { build: 1 },
            },
          ]);
          const [result] = results(yield* allEntries(root));
          // The result keeps the details and diagnostics the parked run reported.
          assert.isFalse(result!.isError);
          assert.deepStrictEqual(result!.details, { build: 1 });
          assert.strictEqual(text(result), "passed|<harness>\n[info] run 1\n</harness>");
          assert.isAtLeast(result!.durationMs ?? -1, 0);
          yield* harness.close;
        }),
      ),
  );

  it.effect("writes only what a woken run adds to the details and diagnostics it starts from", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        const release = yield* Deferred.make<void>();
        const reporter = tool("reporter", (_args, api) =>
          Effect.gen(function* () {
            if (api.wake === undefined) {
              yield* api.diagnostic({ severity: "info", message: "parked" });
              return AgentTool.sleep({ until: (yield* Clock.currentTimeMillis) + 100 });
            }
            yield* api.diagnostic({ severity: "info", message: "woken" });
            yield* api.output("working\n");
            yield* Deferred.await(release);
            return AgentTool.text("ok");
          }),
        );
        setup.faux.setResponses([call("reporter"), DONE]);
        const { harness, root } = yield* open(yield* controlledStorage, setup, [
          toolExtension(reporter),
        ]);
        const submission = yield* root.submit({ type: "input", content: "go" });
        yield* parkedTask(harness, root);
        // The woken run's progress reaches the slot while it runs.
        yield* waitFor(Effect.map(slotOf(harness, root), (slot) => slot?.output === "working\n"));
        assert.deepStrictEqual((yield* slotOf(harness, root))?.diagnostics, [
          { severity: "info", message: "parked" },
          { severity: "info", message: "woken" },
        ]);
        yield* Deferred.succeed(release, undefined);
        assert.strictEqual((yield* settle(submission)).status, "done");
        const [result] = results(yield* allEntries(root));
        assert.strictEqual(text(result), "ok|<harness>\n[info] parked\n[info] woken\n</harness>");
        yield* harness.close;
      }),
    ),
  );

  it.effect("parks on time and wakes with reason time", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        const wakes: Array<WakeReason | undefined> = [];
        const poll = tool("poll", (_args, api) =>
          Effect.gen(function* () {
            wakes.push(api.wake);
            if (api.wake !== undefined) return {};
            return AgentTool.sleep({ until: (yield* Clock.currentTimeMillis) + 500 });
          }),
        );
        setup.faux.setResponses([call("poll"), DONE]);
        const { harness, root } = yield* open(yield* controlledStorage, setup, [
          toolExtension(poll),
        ]);
        const submission = yield* root.submit({ type: "input", content: "go" });
        yield* parkedTask(harness, root);
        assert.strictEqual((yield* settle(submission)).status, "done");
        assert.deepStrictEqual(wakes, [undefined, { reason: "time" }]);
        const [result] = results(yield* allEntries(root));
        // The span from the call's first run to its result, on the Harness clock.
        assert.isAtLeast(result!.durationMs ?? -1, 500);
        assert.isBelow(result!.durationMs ?? Infinity, 600);
        yield* harness.close;
      }),
    ),
  );

  it.effect("stays parked across reopen, running no tool code, and wakes on a later delivery", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        const runs: Array<Run> = [];
        const extensions = [toolExtension(signalTool(runs))];
        setup.faux.setResponses([call("ci"), DONE]);
        const first = yield* open(yield* controlledStorage, setup, extensions);
        const submissionId = (yield* first.root.submit({ type: "input", content: "go" })).id;
        const id = yield* parkedTask(first.harness, first.root);
        yield* first.harness.close;
        const second = yield* open(yield* controlledStorage, setup, extensions);
        yield* inactive(second.harness);
        assert.lengthOf(runs, 1);
        yield* second.harness.harness.signal(id, Done, "done", { text: "later" });
        assert.strictEqual((yield* settleById(second.harness, submissionId)).status, "done");
        assert.lengthOf(runs, 2);
        yield* second.harness.close;
      }),
    ),
  );
});

describeEachBackend("parking nested calls", (backend) => {
  it.effect(
    "parks a nested call, shown sleeping, while its caller awaits it, and returns its result once woken",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const setup = chatSetup();
          const runs: Array<Run> = [];
          let nested: { readonly isError: boolean; readonly durationMs?: number } | undefined;
          const outer = tool(
            "outer",
            (_args, api) =>
              Effect.gen(function* () {
                const result = yield* api.executeTool("ci", {}, { key: "ci" });
                nested =
                  result.durationMs === undefined
                    ? { isError: result.isError }
                    : { isError: result.isError, durationMs: result.durationMs };
                return AgentTool.text(String(result.structuredOutput));
              }),
            { replay: "safe" },
          );
          setup.faux.setResponses([call("outer"), DONE]);
          const { harness, root } = yield* open(yield* controlledStorage, setup, [
            Extension.make({ name: "tools", tools: [signalTool(runs), outer] }),
          ]);
          const submission = yield* root.submit({ type: "input", content: "go" });
          let inner: TaskId | undefined;
          yield* waitFor(
            Effect.map(harness.session.snapshot(LiveDoc, root.id), (live) => {
              const slot = Option.getOrUndefined(live)?.nestedTools?.[0];
              inner = slot?.taskId;
              return slot?.status === "sleeping";
            }),
          );
          // The clock jumps two seconds while the call is parked.
          yield* TestClock.adjust("2 seconds");
          yield* harness.harness.signal(inner!, Done, "done", { text: "nested passed" });
          assert.strictEqual((yield* settle(submission)).status, "done");
          assert.isFalse(nested!.isError);
          assert.isAtLeast(nested!.durationMs ?? -1, 2_000);
          assert.strictEqual(text(results(yield* allEntries(root))[0]), "nested passed");
          assert.lengthOf(runs, 2);
          yield* harness.close;
        }),
      ),
  );
});

describeEachBackend("parking on tasks", (backend) => {
  it.effect(
    "waits for a child it owns, with policy any, and runs again with the ended task in its wake",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const setup = chatSetup();
          const Child = Task.make({
            name: "test.child",
            version: 1,
            input: Schema.Null,
            checkpoint: Schema.Union([
              Schema.Struct({ phase: Schema.Literal("sleep") }),
              Schema.Struct({ phase: Schema.Literal("done") }),
            ]),
            result: Schema.String,
            initial: () => ({ phase: "sleep" }),
            phases: {
              sleep: (_task, runtime) =>
                runtime.sleep({ checkpoint: { phase: "done" }, signals: [Done.key("child")] }),
              done: (_task, runtime) => runtime.complete("child done"),
            },
            abort: (_task, runtime) => runtime.commit(Effect.succeed(abortedWith("test"))),
          });
          let child: TaskId | undefined;
          const wakes: Array<WakeReason | undefined> = [];
          const spawn = tool(
            "spawn",
            (_args, api) =>
              Effect.gen(function* () {
                wakes.push(api.wake);
                if (api.wake !== undefined) return AgentTool.text("joined");
                child = yield* api.createTask(Child, null, {
                  ownership: { kind: "task", taskId: api.taskId },
                });
                return AgentTool.wait({ on: [child], policy: "any" });
              }),
            { replay: "safe" },
          );
          setup.faux.setResponses([call("spawn"), DONE]);
          const { harness, root } = yield* open(yield* controlledStorage, setup, [
            Extension.make({ name: "spawn", tools: [spawn], tasks: [Child] }),
          ]);
          const submission = yield* root.submit({ type: "input", content: "go" });
          yield* parkedTask(harness, root, "waiting");
          assert.strictEqual((yield* slotOf(harness, root))?.status, "waiting");
          yield* harness.harness.signal(child!, Done, "child", { text: "go" });
          assert.strictEqual((yield* settle(submission)).status, "done");
          assert.deepStrictEqual(wakes, [undefined, { reason: "tasks", ended: [child!] }]);
          yield* harness.close;
        }),
      ),
  );
});

describeEachBackend("invalid parks", (backend) => {
  it.effect("end the call with an invalid_park result, through afterTool, and fail its task", () =>
    run(backend)(
      Effect.gen(function* () {
        const parks: ReadonlyArray<readonly [name: string, park: Schema.JsonObject]> = [
          ["mixed", { sleep: { until: 1 }, wait: { on: [], policy: "any" } }],
          ["result", { sleep: { until: 1 }, output: [] }],
          ["empty", { sleep: {} }],
          ["unowned", { wait: { on: [1], policy: "any" } }],
        ];
        for (const [name, returned] of parks) {
          const setup = chatSetup();
          // SAFETY: the parks a tool's types forbid, which tool code may still return.
          const invalid = tool(name, () =>
            Effect.succeed(returned as Schema.JsonObject & AgentTool.ToolPark),
          );
          setup.faux.setResponses([call(name), DONE]);
          const { harness, root } = yield* open(yield* controlledStorage, setup, [
            toolExtension(invalid),
          ]);
          const submission = yield* root.submit({ type: "input", content: "go" });
          assert.strictEqual((yield* settle(submission)).status, "done");
          // Every case runs over the same storage, so its result is the newest.
          const result = results(yield* allEntries(root)).at(-1);
          assert.isTrue(result!.isError);
          assert.include(text(result), `Tool ${name} returned an invalid park`);
          yield* harness.close;
        }
      }),
    ),
  );

  it.effect("end a call that sleeps again on the key that woke it with invalid_park", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        let runs = 0;
        const again = tool("again", () =>
          Effect.sync(() => {
            runs++;
            return AgentTool.sleep({ signals: [Done.key("k")] });
          }),
        );
        setup.faux.setResponses([call("again"), DONE]);
        const { harness, root } = yield* open(yield* controlledStorage, setup, [
          toolExtension(again),
        ]);
        const submission = yield* root.submit({ type: "input", content: "go" });
        const id = yield* parkedTask(harness, root);
        yield* harness.harness.signal(id, Done, "k", { text: "x" });
        assert.strictEqual((yield* settle(submission)).status, "done");
        assert.strictEqual(runs, 2);
        assert.include(text(results(yield* allEntries(root))[0]), "it already woke a wait");
        const state = Option.getOrUndefined(yield* harness.harness.getTask(id))?.state;
        assert.strictEqual(
          state?.status === "terminal" ? state.outcome.status : undefined,
          "failed",
        );
        yield* harness.close;
      }),
    ),
  );
});

describeEachBackend("cancel", (backend) => {
  it.effect(
    "runs when a parked call is aborted, with the park's checkpoint and the environment, and the call ends aborted",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const setup = chatSetup();
          const cancels: Array<Run> = [];
          const envs: Array<string | undefined> = [];
          const ci = signalTool([], {
            cancel: (_args, api) =>
              Effect.sync(() => {
                cancels.push({ wake: api.wake, checkpoint: api.checkpoint });
                envs.push(api.env?.cwd);
              }),
          });
          setup.faux.setResponses([call("ci"), DONE]);
          const { harness, root } = yield* open(
            yield* controlledStorage,
            setup,
            [toolExtension(ci)],
            () => Effect.succeed({ cwd: "/work" }),
          );
          const submission = yield* root.submit({ type: "input", content: "go" });
          yield* parkedTask(harness, root);
          yield* root.abort();
          assert.strictEqual((yield* settle(submission)).status, "unanswered");
          assert.deepStrictEqual(cancels, [{ wake: undefined, checkpoint: { build: 1 } }]);
          assert.deepStrictEqual(envs, ["/work"]);
          const [result] = results(yield* allEntries(root));
          assert.include(text(result), "Tool ci was aborted");
          yield* harness.close;
        }),
      ),
  );

  // A deploy that adds a required parameter: the stored arguments no longer decode, so `cancel` has no value
  // to run with.
  it.effect(
    "does not run when the call's stored arguments no longer decode, and reports it while the call ends aborted",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const setup = chatSetup();
          const storage = yield* controlledStorage;
          setup.faux.setResponses([call("ci"), DONE]);
          const first = yield* open(storage, setup, [toolExtension(signalTool([]))]);
          const submissionId = (yield* first.root.submit({ type: "input", content: "go" })).id;
          yield* parkedTask(first.harness, first.root);
          yield* first.harness.close;

          let cancels = 0;
          const narrowed = AgentTool.make("ci", {
            description: "ci",
            parameters: Schema.Struct({ total: Schema.Number }),
            execute: () => Effect.succeed(AgentTool.text("ran")),
            cancel: () =>
              Effect.sync(() => {
                cancels++;
              }),
          });
          const next = yield* open(storage, setup, [toolExtension(narrowed)]);
          yield* next.root.abort();
          assert.strictEqual((yield* settleById(next.harness, submissionId)).status, "unanswered");
          assert.strictEqual(cancels, 0);
          assert.isTrue(
            reportMessages(setup).some((message) => message.includes("Tool ci cannot cancel")),
          );
          const [result] = results(yield* allEntries(next.root));
          assert.include(text(result), "Tool ci was aborted");
          yield* next.harness.close;
        }),
      ),
  );

  it.effect(
    "runs also under restartLimit 1, and a failure from it is reported while the call still ends aborted",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const setup = chatSetup();
          setup.settings.restartLimit = 1;
          let cancels = 0;
          const ci = signalTool([], {
            cancel: () =>
              Effect.suspend(() => {
                cancels++;
                return Effect.fail(new CancelFailed({ message: "cancel failed" }));
              }),
          });
          setup.faux.setResponses([call("ci"), DONE]);
          const { harness, root } = yield* open(yield* controlledStorage, setup, [
            toolExtension(ci),
          ]);
          const submission = yield* root.submit({ type: "input", content: "go" });
          const id = yield* parkedTask(harness, root);
          yield* root.abort();
          yield* settle(submission);
          assert.strictEqual(cancels, 1);
          const state = Option.getOrUndefined(yield* harness.harness.getTask(id))?.state;
          assert.strictEqual(
            state?.status === "terminal" ? state.outcome.status : undefined,
            "aborted",
          );
          assert.include(reportMessages(setup), "cancel failed");
          yield* harness.close;
        }),
      ),
  );

  it.effect(
    "does not run when its environment fails to build, which is reported while the call still ends aborted",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const setup = chatSetup();
          let cancels = 0;
          let broken = false;
          const ci = signalTool([], {
            cancel: () =>
              Effect.sync(() => {
                cancels++;
              }),
          });
          setup.faux.setResponses([call("ci"), DONE]);
          // The run that parks gets an environment; the cleanup after the park does not.
          const { harness, root } = yield* open(
            yield* controlledStorage,
            setup,
            [toolExtension(ci)],
            () =>
              Effect.suspend(() =>
                broken
                  ? Effect.fail(new EnvFailed({ message: "no sandbox" }))
                  : Effect.succeed({ cwd: "/work" }),
              ),
          );
          const submission = yield* root.submit({ type: "input", content: "go" });
          const id = yield* parkedTask(harness, root);
          broken = true;
          yield* root.abort();
          yield* settle(submission);
          assert.strictEqual(cancels, 0);
          const state = Option.getOrUndefined(yield* harness.harness.getTask(id))?.state;
          assert.strictEqual(
            state?.status === "terminal" ? state.outcome.status : undefined,
            "aborted",
          );
          const report = setup.reports.find((each) =>
            reportMessages({ ...setup, reports: [each] }).includes("no sandbox"),
          );
          assert.strictEqual(report?.kind, "extension_error");
          assert.containSubset(report, { extension: "tool:ci" });
          yield* harness.close;
        }),
      ),
  );

  it.effect(
    "runs after an abort that cut off a woken run, which cleans up through its context too",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const setup = chatSetup();
          const cancels: Array<Run> = [];
          const running = Deferred.makeUnsafe<void>();
          const cleaned: Array<string> = [];
          const ci = tool(
            "ci",
            (_args, api) =>
              Effect.gen(function* () {
                if (api.wake === undefined) {
                  return AgentTool.sleep({ checkpoint: "first", signals: [Done.key("done")] });
                }
                yield* Deferred.succeed(running, undefined);
                return yield* Effect.never.pipe(
                  Effect.onInterrupt(() =>
                    Effect.sync(() => {
                      cleaned.push("execute");
                    }),
                  ),
                );
              }),
            {
              cancel: (_args, api) =>
                Effect.sync(() => {
                  cancels.push({ wake: api.wake, checkpoint: api.checkpoint });
                }),
            },
          );
          setup.faux.setResponses([call("ci"), DONE]);
          const { harness, root } = yield* open(yield* controlledStorage, setup, [
            toolExtension(ci),
          ]);
          const submission = yield* root.submit({ type: "input", content: "go" });
          const id = yield* parkedTask(harness, root);
          yield* harness.harness.signal(id, Done, "done", { text: "x" });
          yield* waitFor(Deferred.isDone(running));
          yield* root.abort();
          yield* settle(submission);
          assert.deepStrictEqual(cleaned, ["execute"]);
          assert.deepStrictEqual(cancels, [{ wake: undefined, checkpoint: "first" }]);
          yield* harness.close;
        }),
      ),
  );

  it.effect("does not run for a parked call whose tool is gone; the abort is reported", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        const cancels: Array<Run> = [];
        const ci = signalTool([], {
          cancel: (_args, api) =>
            Effect.sync(() => {
              cancels.push({ wake: api.wake, checkpoint: null });
            }),
        });
        setup.faux.setResponses([call("ci"), DONE]);
        const first = yield* open(yield* controlledStorage, setup, [toolExtension(ci)]);
        const submissionId = (yield* first.root.submit({ type: "input", content: "go" })).id;
        yield* parkedTask(first.harness, first.root);
        yield* first.harness.close;
        // The tool's extension is gone in the next process.
        const second = yield* open(yield* controlledStorage, setup, []);
        yield* second.root.abort();
        assert.strictEqual((yield* settleById(second.harness, submissionId)).status, "unanswered");
        assert.deepStrictEqual(cancels, []);
        assert.include(
          reportMessages(setup),
          "Tool ci is not available: its parked call c1 was not cancelled",
        );
        yield* second.harness.close;
      }),
    ),
  );

  it.effect("runs in the run itself when the park's commit finds the call abort-marked", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        const release = Deferred.makeUnsafe<void>();
        const entered = Deferred.makeUnsafe<void>();
        const cancels: Array<Run> = [];
        const race = tool(
          "race",
          () =>
            Effect.gen(function* () {
              yield* Deferred.succeed(entered, undefined);
              yield* Deferred.await(release);
              return AgentTool.sleep({ checkpoint: "started", signals: [Done.key("k")] });
            }),
          {
            cancel: (_args, api) =>
              Effect.gen(function* () {
                // The cleanup runs to its end: the abort does not interrupt it.
                yield* Effect.yieldNow;
                cancels.push({ wake: api.wake, checkpoint: api.checkpoint });
              }),
          },
        );
        setup.faux.setResponses([call("race"), DONE]);
        const { harness, root } = yield* open(yield* controlledStorage, setup, [
          toolExtension(race),
        ]);
        const submission = yield* root.submit({ type: "input", content: "go" });
        yield* waitFor(Deferred.isDone(entered));
        const id = (yield* slotOf(harness, root))!.taskId!;
        // The abort's commit queues on the held line before the park's, so the park finds the call abort-marked.
        const held = yield* holdLine(harness);
        yield* held.entered;
        const aborting = yield* Effect.forkChild(harness.harness.abortTask(id), {
          startImmediately: true,
        });
        yield* flush;
        yield* Deferred.succeed(release, undefined);
        yield* flush;
        yield* held.release;
        yield* waitFor(Effect.sync(() => aborting.pollUnsafe() !== undefined));
        yield* Fiber.join(aborting);
        // The run's cancel with the park's checkpoint; the abort handler finds `execute` without a wake and runs
        // none.
        assert.deepStrictEqual(cancels, [{ wake: undefined, checkpoint: "started" }]);
        yield* root.abort();
        yield* settle(submission);
        yield* harness.close;
      }),
    ),
  );
});

describeEachBackend("waking", (backend) => {
  it.effect(
    "ends a woken call whose tool is gone with tool_unavailable, failed, and reports it",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const setup = chatSetup();
          const runs: Array<Run> = [];
          setup.faux.setResponses([call("ci"), DONE]);
          const first = yield* open(yield* controlledStorage, setup, [
            toolExtension(signalTool(runs)),
          ]);
          const submissionId = (yield* first.root.submit({ type: "input", content: "go" })).id;
          const id = yield* parkedTask(first.harness, first.root);
          yield* first.harness.close;
          // The tool's extension is gone in the next process.
          const second = yield* open(yield* controlledStorage, setup, []);
          yield* second.harness.harness.signal(id, Done, "done", { text: "late" });
          assert.strictEqual((yield* settleById(second.harness, submissionId)).status, "done");
          const state = Option.getOrUndefined(yield* second.harness.harness.getTask(id))?.state;
          assert.strictEqual(
            state?.status === "terminal" ? state.outcome.status : undefined,
            "failed",
          );
          const [result] = results(yield* allEntries(second.root));
          // Built from the slot: the details and diagnostics the parked run left stay.
          assert.isTrue(result!.isError);
          assert.deepStrictEqual(result!.details, { build: 1 });
          assert.include(text(result), "[info] run 1");
          assert.include(text(result), "Tool ci is not available");
          assert.include(
            reportMessages(setup),
            "Tool ci is not available: its parked call c1 cannot run again",
          );
          assert.lengthOf(runs, 1);
          yield* second.harness.close;
        }),
      ),
  );

  it.effect("settles a woken unsafe run cut off by a crash interrupted, after cancel", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        const cancels: Array<Run> = [];
        const started = Deferred.makeUnsafe<void>();
        let runs = 0;
        const ci = tool(
          "ci",
          (_args, api) =>
            Effect.gen(function* () {
              runs++;
              if (api.wake === undefined) {
                return AgentTool.sleep({ checkpoint: "first", signals: [Done.key("done")] });
              }
              yield* api.output("rerun\n");
              yield* api.details({ woken: true });
              yield* Deferred.succeed(started, undefined);
              return yield* Effect.never;
            }),
          {
            cancel: (_args, api) =>
              Effect.sync(() => {
                cancels.push({ wake: api.wake, checkpoint: api.checkpoint });
              }),
          },
        );
        const extensions = [toolExtension(ci)];
        setup.faux.setResponses([call("ci"), DONE]);
        const crashable = yield* killable(yield* AgentStorage);
        const crashed = yield* open(crashable.storage, setup, extensions);
        const submissionId = (yield* crashed.root.submit({ type: "input", content: "go" })).id;
        const id = yield* parkedTask(crashed.harness, crashed.root);
        yield* crashed.harness.harness.signal(id, Done, "done", { text: "x" });
        yield* waitFor(Deferred.isDone(started));
        // The process dies here: another one opens the same storage.
        yield* crashable.kill;
        yield* crashed.harness.close;
        const next = yield* open(yield* controlledStorage, setup, extensions);
        assert.strictEqual((yield* settleById(next.harness, submissionId)).status, "done");
        assert.strictEqual(runs, 2);
        assert.deepStrictEqual(cancels, [{ wake: undefined, checkpoint: "first" }]);
        const [result] = results(yield* allEntries(next.root));
        assert.include(text(result), "Tool ci was interrupted and may have partially run");
        yield* next.harness.close;
      }),
    ),
  );

  it.effect(
    "reruns a woken safe run cut off by a crash, keeping the slot's details and diagnostics, not its output",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const setup = chatSetup();
          const started = Deferred.makeUnsafe<void>();
          const seen: Array<Run> = [];
          const ci = tool(
            "ci",
            (_args, api) =>
              Effect.gen(function* () {
                seen.push({ wake: api.wake, checkpoint: api.checkpoint });
                if (api.wake === undefined) {
                  yield* api.diagnostic({ severity: "info", message: "parked" });
                  yield* api.details({ build: 1 });
                  return AgentTool.sleep({ checkpoint: "first", signals: [Done.key("done")] });
                }
                if (seen.length === 2) {
                  yield* api.output("cut off\n");
                  yield* Deferred.succeed(started, undefined);
                  return yield* Effect.never;
                }
                return {};
              }),
            { replay: "safe" },
          );
          const extensions = [toolExtension(ci)];
          setup.faux.setResponses([call("ci"), DONE]);
          const crashable = yield* killable(yield* AgentStorage);
          const crashed = yield* open(crashable.storage, setup, extensions);
          const submissionId = (yield* crashed.root.submit({ type: "input", content: "go" })).id;
          const id = yield* parkedTask(crashed.harness, crashed.root);
          yield* crashed.harness.harness.signal(id, Done, "done", { text: "x" });
          yield* waitFor(Deferred.isDone(started));
          yield* crashable.kill;
          yield* crashed.harness.close;
          const next = yield* open(yield* controlledStorage, setup, extensions);
          assert.strictEqual((yield* settleById(next.harness, submissionId)).status, "done");
          const wake = { reason: "signal", signal: "test.done", key: "done" } as const;
          assert.deepStrictEqual(seen, [
            { wake: undefined, checkpoint: undefined },
            { wake, checkpoint: "first" },
            { wake, checkpoint: "first" },
          ]);
          // The park stored the details and diagnostics; the rerun starts from them, without the cut-off run's
          // output.
          const [result] = results(yield* allEntries(next.root));
          assert.isFalse(result!.isError);
          assert.deepStrictEqual(result!.details, { build: 1 });
          assert.strictEqual(text(result), "<harness>\n[info] parked\n</harness>");
          yield* next.harness.close;
        }),
      ),
  );
});

/** A wrapper of `base` whose `execute` maps what its base returned. */
const wrapping = (
  base: AgentTool.Any,
  map: (
    returned: AgentTool.ToolExecutionResult | AgentTool.ToolPark,
  ) => AgentTool.ToolExecutionResult | AgentTool.ToolPark,
) =>
  Extension.make({
    name: "wrapper",
    wraps: [
      Extension.wrapTool(base, (inner) =>
        AgentTool.make(inner.name, {
          ...inner.definition,
          parameters: NoParameters,
          execute: (args) =>
            // SAFETY: the wrapper passes on the arguments its own `parameters`, the base's, decoded.
            Effect.map(AgentTool.run(inner, args as never), map),
        }),
      ),
    ],
  });

describeEachBackend("wrappers", (backend) => {
  it.effect("pass a base's park through, and a park of their own is invalid", () =>
    run(backend)(
      Effect.gen(function* () {
        for (const own of [false, true]) {
          const setup = chatSetup();
          const runs: Array<Run> = [];
          const base = signalTool(runs);
          setup.faux.setResponses([call("ci"), DONE]);
          const { harness, root } = yield* open(yield* controlledStorage, setup, [
            toolExtension(base),
            wrapping(base, (returned) =>
              own && AgentTool.isPark(returned) && returned.sleep !== undefined
                ? AgentTool.sleep({ ...returned.sleep })
                : returned,
            ),
          ]);
          const submission = yield* root.submit({ type: "input", content: "go" });
          if (own) {
            assert.strictEqual((yield* settle(submission)).status, "done");
            assert.include(
              text(results(yield* allEntries(root)).at(-1)),
              "a wrapper returned a park its base did not return",
            );
          } else {
            const id = yield* parkedTask(harness, root);
            yield* harness.harness.signal(id, Done, "done", { text: "wrapped" });
            assert.strictEqual((yield* settle(submission)).status, "done");
            assert.lengthOf(runs, 2);
          }
          yield* harness.close;
        }
      }),
    ),
  );

  it.effect("accept one park object its base returns to calls running at once", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        const shared = AgentTool.sleep({ signals: [Done.key("done")] });
        const base = tool("ci", (_args, api) =>
          Effect.map(api.readSignal(Done, "done"), (state) =>
            Option.isNone(state) ? shared : AgentTool.text("ok"),
          ),
        );
        setup.faux.setResponses([
          fauxAssistantMessage(
            [fauxToolCall("ci", {}, { id: "a" }), fauxToolCall("ci", {}, { id: "b" })],
            {
              stopReason: "toolUse",
            },
          ),
          DONE,
        ]);
        const { harness, root } = yield* open(yield* controlledStorage, setup, [
          toolExtension(base),
          Extension.make({
            name: "wrapper",
            wraps: [Extension.wrapTool(base, (inner) => ({ ...inner }))],
          }),
        ]);
        const submission = yield* root.submit({ type: "input", content: "go" });
        let ids: Array<TaskId> = [];
        yield* waitFor(
          Effect.map(harness.session.snapshot(LiveDoc, root.id), (live) => {
            const slots = Option.getOrUndefined(live)?.tools ?? [];
            ids = slots.flatMap((slot) =>
              slot.status === "sleeping" && slot.taskId !== undefined ? [slot.taskId] : [],
            );
            return ids.length === 2;
          }),
        );
        for (const id of ids) yield* harness.harness.signal(id, Done, "done", { text: "x" });
        assert.strictEqual((yield* settle(submission)).status, "done");
        assert.deepStrictEqual(
          results(yield* allEntries(root)).map((result) => result.isError),
          [false, false],
        );
        yield* harness.close;
      }),
    ),
  );

  it.effect("refuse a park the wrapper kept from an earlier run instead of asking its base", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        const base = signalTool([]);
        let kept: AgentTool.ToolPark | undefined;
        setup.faux.setResponses([call("ci"), DONE]);
        const { harness, root } = yield* open(yield* controlledStorage, setup, [
          toolExtension(base),
          Extension.make({
            name: "wrapper",
            wraps: [
              Extension.wrapTool(base, (inner) =>
                AgentTool.make(inner.name, {
                  ...inner.definition,
                  parameters: NoParameters,
                  execute: (args) =>
                    kept !== undefined
                      ? Effect.succeed(kept)
                      : // SAFETY: the wrapper passes on the arguments its own `parameters`, the base's, decoded.
                        Effect.map(AgentTool.run(inner, args as never), (returned) => {
                          if (AgentTool.isPark(returned) && returned.sleep !== undefined)
                            kept = returned;
                          return returned;
                        }),
                }),
              ),
            ],
          }),
        ]);
        const submission = yield* root.submit({ type: "input", content: "go" });
        const id = yield* parkedTask(harness, root);
        yield* harness.harness.signal(id, Done, "done", { text: "x" });
        assert.strictEqual((yield* settle(submission)).status, "done");
        assert.include(
          text(results(yield* allEntries(root))[0]),
          "a wrapper returned a park its base did not return",
        );
        yield* harness.close;
      }),
    ),
  );
});

// Gates before a tool runs: `beforeTool` sleeps, the `call` phase asked again on every wake, opens that wake
// calls parked at their gates, and the deadline path of such a wake.
//
// Notes:
// - The registry is static, so the only change of the installed gates is a reopen: a reopened process is a
//   second Harness over the same storage, with other extensions. "Registry changes … wake a call parked at its
//   gates" reopens with another extension and then without the gate. "Wake a sleep committed under a snapshot a
//   publication replaced while the gate ran" has no counterpart: nothing is published while a Harness runs.
// - The Harness clock is the `TestClock`; a deadline that passes is a `TestClock.adjust` followed by `resume`.
import { assert, it } from "@effect/vitest";
import * as Clock from "effect/Clock";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as TestClock from "effect/testing/TestClock";
import * as AgentTool from "../src/AgentTool.ts";
import { ToolResultEntry } from "../src/Entry.ts";
import * as Extension from "../src/Extension.ts";
import type { Conversation } from "../src/Harness.ts";
import * as Hook from "../src/Hook.ts";
import type { SubmissionId, TaskId } from "../src/Ids.ts";
import { LiveDoc } from "../src/Live.ts";
import type { AssistantMessage, ToolResultMessage } from "../src/Message.ts";
import type { EntryRecord } from "../src/Records.ts";
import * as Signal from "../src/Signal.ts";
import type { Submission } from "../src/Submission.ts";
import { fauxAssistantMessage, fauxText, fauxToolCall } from "../src/testing/FauxModel.ts";
import { allEntries, type ChatSetup, chatSetup, openChat, waitFor } from "./ChatSupport.ts";
import type { TestHarness } from "./HarnessSupport.ts";
import {
  type ControlledStorage,
  controlledStorage,
  describeEachBackend,
  run,
} from "./SessionSupport.ts";

const Approval = Signal.make("test.approval", {
  version: 1,
  schema: Schema.Struct({ allow: Schema.Boolean }),
});
const DAY = 24 * 60 * 60 * 1000;
/** The checkpoint of a call parked in `execute`. */
const Parked = Schema.Struct({ phase: Schema.Literal("parked") });

const call = (command = "rm -rf build"): AssistantMessage =>
  fauxAssistantMessage([fauxToolCall("bash", { command }, { id: "c1" })], {
    stopReason: "toolUse",
  });

const DONE = fauxAssistantMessage([fauxText("done")]);

const results = (entries: ReadonlyArray<EntryRecord>): Array<ToolResultMessage> =>
  entries.flatMap((entry) => {
    const message = entry.model?.[0];
    return ToolResultEntry.is(entry) && message?.role === "toolResult" ? [message] : [];
  });

const text = (message: ToolResultMessage | undefined): string =>
  (message?.content ?? []).map((item) => (item.type === "text" ? item.text : "")).join("|");

/** A mutable count a tool or gate bumps on every run. */
interface Count {
  count: number;
}

/** A `bash` stand-in that counts its runs. */
const bashTool = (runs: Count) =>
  AgentTool.make("bash", {
    description: "bash",
    parameters: Schema.Struct({ command: Schema.String }),
    execute: ({ command }) =>
      Effect.sync(() => {
        runs.count++;
        return AgentTool.text(`ran ${command}`);
      }),
  });

const tools = (runs: Count) => Extension.make({ name: "tools", tools: [bashTool(runs)] });

/** The approval gate: sleeps on an approval keyed by the command, with a deadline fixed at its first pass. */
const approvals = (passes: Count) =>
  Extension.make({
    name: "approvals",
    hooks: [
      Hook.tool({
        beforeTool: (gated, api) =>
          Effect.gen(function* () {
            passes.count++;
            const key = String(gated.arguments["command"]).replaceAll(" ", "_");
            const state = Option.getOrUndefined(yield* api.readSignal(Approval, key));
            const until = yield* api.memo(`approvals.until.${key}`, (yield* api.now) + DAY);
            if (state === undefined) return { sleep: { signals: [Approval.key(key)], until } };
            if ("expired" in state) return { block: "No approval within a day" };
            return state.value.allow ? undefined : { block: "Not approved" };
          }),
      }),
    ],
  });

/** A Harness over `storage` that schedules at once. */
const open = (
  storage: ControlledStorage,
  setup: ChatSetup,
  extensions: ReadonlyArray<Extension.Any>,
) =>
  Effect.gen(function* () {
    const opened = yield* openChat(storage, setup, { extensions });
    yield* opened.harness.harness.resume;
    return opened;
  });

/** The task of the root's first tool slot, once it sleeps. */
const gatedTask = (h: TestHarness, root: Conversation) =>
  Effect.gen(function* () {
    let id: TaskId | undefined;
    yield* waitFor(
      Effect.map(h.session.snapshot(LiveDoc, root.id), (live) => {
        const slot = Option.getOrUndefined(live)?.tools?.[0];
        id = slot?.taskId;
        return id !== undefined && slot?.status === "sleeping";
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

describeEachBackend("a beforeTool sleep", (backend) => {
  it.effect("parks the call before it runs, and runs it once approved", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        const runs = { count: 0 };
        const passes = { count: 0 };
        setup.faux.setResponses([call(), DONE]);
        const { harness, root } = yield* open(yield* controlledStorage, setup, [
          tools(runs),
          approvals(passes),
        ]);
        const submission = yield* root.submit({ type: "input", content: "go" });
        const id = yield* gatedTask(harness, root);
        const state = Option.getOrUndefined(yield* harness.harness.getTask(id))?.state;
        assert.strictEqual(state?.status, "sleeping");
        assert.deepStrictEqual(state?.status === "sleeping" ? state.checkpoint : undefined, {
          phase: "call",
        });
        assert.deepStrictEqual(state?.status === "sleeping" ? state.signals : undefined, [
          { signal: "test.approval", key: "rm_-rf_build" },
        ]);
        yield* inactive(harness);
        assert.strictEqual(runs.count, 0);
        yield* harness.harness.signal(id, Approval, "rm_-rf_build", { allow: true });
        assert.strictEqual((yield* settle(submission)).status, "done");
        assert.strictEqual(runs.count, 1);
        assert.strictEqual(passes.count, 2);
        assert.strictEqual(text(results(yield* allEntries(root))[0]), "ran rm -rf build");
        yield* harness.close;
      }),
    ),
  );

  for (const outcome of ["denied", "expired"] as const) {
    it.effect(
      `blocks on a denial, and on the deadline, after which a late answer is closed (${outcome})`,
      () =>
        run(backend)(
          Effect.gen(function* () {
            const setup = chatSetup();
            const runs = { count: 0 };
            setup.faux.setResponses([call(), DONE]);
            const { harness, root } = yield* open(yield* controlledStorage, setup, [
              tools(runs),
              approvals({ count: 0 }),
            ]);
            const submission = yield* root.submit({ type: "input", content: "go" });
            const id = yield* gatedTask(harness, root);
            if (outcome === "denied") {
              yield* harness.harness.signal(id, Approval, "rm_-rf_build", { allow: false });
            } else {
              yield* TestClock.adjust(DAY);
              yield* harness.harness.resume;
            }
            assert.strictEqual((yield* settle(submission)).status, "done");
            assert.strictEqual(runs.count, 0);
            assert.include(
              text(results(yield* allEntries(root))[0]),
              outcome === "denied" ? "Not approved" : "No approval within a day",
            );
            if (outcome === "expired") {
              assert.strictEqual(
                yield* harness.harness.signal(id, Approval, "rm_-rf_build", { allow: true }),
                "closed",
              );
            }
            yield* harness.close;
          }),
        ),
    );
  }

  it.effect(
    "asks every gate again on each wake, in extension order, and stops at the first that sleeps",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const setup = chatSetup();
          const runs = { count: 0 };
          const passes = { count: 0 };
          const quiet = { count: 0 };
          const quietHours = Extension.make({
            name: "quiet-hours",
            hooks: [
              Hook.tool({
                beforeTool: () =>
                  Effect.sync(() => {
                    quiet.count++;
                    return undefined;
                  }),
              }),
            ],
          });
          setup.faux.setResponses([call(), DONE]);
          const { harness, root } = yield* open(yield* controlledStorage, setup, [
            tools(runs),
            approvals(passes),
            quietHours,
          ]);
          const submission = yield* root.submit({ type: "input", content: "go" });
          const id = yield* gatedTask(harness, root);
          assert.deepStrictEqual([passes.count, quiet.count], [1, 0]);
          yield* harness.harness.signal(id, Approval, "rm_-rf_build", { allow: true });
          yield* settle(submission);
          assert.deepStrictEqual([passes.count, quiet.count, runs.count], [2, 1, 1]);
          yield* harness.close;
        }),
      ),
  );

  // Covers the spec's statement that `cancel` does not run for a call parked at its gates, which ran no tool code.
  it.effect("aborts a call parked at its gates without running the tool's cancel", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        const runs = { count: 0 };
        const cancels = { count: 0 };
        const bash = AgentTool.make("bash", {
          description: "bash",
          parameters: Schema.Struct({ command: Schema.String }),
          execute: () =>
            Effect.sync(() => {
              runs.count++;
              return AgentTool.text("ran");
            }),
          cancel: () =>
            Effect.sync(() => {
              cancels.count++;
            }),
        });
        setup.faux.setResponses([call(), DONE]);
        const { harness, root } = yield* open(yield* controlledStorage, setup, [
          Extension.make({ name: "tools", tools: [bash] }),
          approvals({ count: 0 }),
        ]);
        const submission = yield* root.submit({ type: "input", content: "go" });
        const id = yield* gatedTask(harness, root);
        yield* root.abort();
        assert.strictEqual((yield* settle(submission)).status, "unanswered");
        const state = Option.getOrUndefined(yield* harness.harness.getTask(id))?.state;
        assert.strictEqual(
          state?.status === "terminal" ? state.outcome.status : undefined,
          "aborted",
        );
        assert.deepStrictEqual([runs.count, cancels.count], [0, 0]);
        assert.include(text(results(yield* allEntries(root))[0]), "Tool bash was aborted");
        yield* harness.close;
      }),
    ),
  );

  it.effect("ends the call invalid_park when the sleep waits for nothing", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        const runs = { count: 0 };
        const bad = Extension.make({
          name: "bad",
          hooks: [Hook.tool({ beforeTool: () => Effect.succeed({ sleep: {} }) })],
        });
        setup.faux.setResponses([call(), DONE]);
        const { harness, root } = yield* open(yield* controlledStorage, setup, [tools(runs), bad]);
        const submission = yield* root.submit({ type: "input", content: "go" });
        assert.strictEqual((yield* settle(submission)).status, "done");
        assert.strictEqual(runs.count, 0);
        assert.include(text(results(yield* allEntries(root))[0]), "cannot wait at its gates");
        yield* harness.close;
      }),
    ),
  );
});

describeEachBackend("opens", (backend) => {
  it.effect("wake a call parked at its gates, so the gates installed then decide", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        const runs = { count: 0 };
        const passes = { count: 0 };
        setup.faux.setResponses([call(), DONE]);
        const first = yield* open(yield* controlledStorage, setup, [
          tools(runs),
          approvals(passes),
        ]);
        const submissionId = (yield* first.root.submit({ type: "input", content: "go" })).id;
        yield* gatedTask(first.harness, first.root);
        yield* first.harness.close;
        // Another extension installed: asked again, and still asleep.
        const other = Extension.make({ name: "other" });
        const second = yield* open(yield* controlledStorage, setup, [
          tools(runs),
          approvals(passes),
          other,
        ]);
        yield* waitFor(Effect.sync(() => passes.count === 2));
        yield* gatedTask(second.harness, second.root);
        yield* inactive(second.harness);
        yield* second.harness.close;
        // The gate uninstalled: the call runs at open.
        const third = yield* open(yield* controlledStorage, setup, [tools(runs), other]);
        assert.strictEqual((yield* settleById(third.harness, submissionId)).status, "done");
        assert.deepStrictEqual([passes.count, runs.count], [2, 1]);
        yield* third.harness.close;
      }),
    ),
  );

  it.effect(
    "wake a call parked at its gates at open, before its deadline, so a gate removed meanwhile lets it run",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const setup = chatSetup();
          const passes = { count: 0 };
          const runs = { count: 0 };
          setup.faux.setResponses([call(), DONE]);
          const first = yield* open(yield* controlledStorage, setup, [
            tools(runs),
            approvals(passes),
          ]);
          const submissionId = (yield* first.root.submit({ type: "input", content: "go" })).id;
          yield* gatedTask(first.harness, first.root);
          yield* first.harness.close;
          // The next process installs no approvals: the call runs at open, no signal, no deadline.
          setup.faux.setResponses([DONE]);
          const second = yield* open(yield* controlledStorage, setup, [tools(runs)]);
          assert.strictEqual((yield* settleById(second.harness, submissionId)).status, "done");
          assert.deepStrictEqual([passes.count, runs.count], [1, 1]);
          yield* second.harness.close;
        }),
      ),
  );

  it.effect("leave a call parked in execute alone", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        const runs = { count: 0 };
        const parking = AgentTool.make("bash", {
          description: "bash",
          parameters: Schema.Struct({ command: Schema.String }),
          execute: (_args, api) =>
            Effect.sync(() => {
              runs.count++;
              return api.wake === undefined
                ? AgentTool.sleep({ signals: [Approval.key("done")] })
                : AgentTool.text("");
            }),
        });
        const extension = Extension.make({ name: "tools", tools: [parking] });
        setup.faux.setResponses([call(), DONE]);
        const first = yield* open(yield* controlledStorage, setup, [extension]);
        yield* first.root.submit({ type: "input", content: "go" });
        const id = yield* gatedTask(first.harness, first.root);
        yield* first.harness.close;
        const second = yield* open(yield* controlledStorage, setup, [
          extension,
          Extension.make({ name: "other" }),
        ]);
        yield* inactive(second.harness);
        const state = Option.getOrUndefined(yield* second.harness.harness.getTask(id))?.state;
        assert.strictEqual(state?.status, "sleeping");
        assert.isTrue(
          Schema.is(Parked)(state?.status === "sleeping" ? state.checkpoint : undefined),
        );
        assert.strictEqual(runs.count, 1);
        yield* second.harness.close;
      }),
    ),
  );

  it.effect("take the deadline path for a gate whose deadline passed, so its keys expire", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        const passes = { count: 0 };
        const runs = { count: 0 };
        const extensions = [tools(runs), approvals(passes)];
        setup.faux.setResponses([call(), DONE]);
        const first = yield* open(yield* controlledStorage, setup, extensions);
        const submissionId = (yield* first.root.submit({ type: "input", content: "go" })).id;
        const id = yield* gatedTask(first.harness, first.root);
        yield* first.harness.close;
        // Reopened after the deadline: the open's wake expires the key, and the gate blocks.
        yield* TestClock.adjust(2 * DAY);
        const second = yield* open(yield* controlledStorage, setup, extensions);
        assert.strictEqual((yield* settleById(second.harness, submissionId)).status, "done");
        assert.strictEqual(passes.count, 2);
        assert.strictEqual(runs.count, 0);
        assert.include(
          text(results(yield* allEntries(second.root))[0]),
          "No approval within a day",
        );
        assert.strictEqual(
          yield* second.harness.harness.signal(id, Approval, "rm_-rf_build", { allow: true }),
          "closed",
        );
        yield* second.harness.close;
      }),
    ),
  );
});

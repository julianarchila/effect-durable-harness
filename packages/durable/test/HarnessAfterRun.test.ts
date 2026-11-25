// Run-end tasks that `afterRun` hooks ask for once a run answered, and how the run waits for them, on every backend.
//
// - Extensions are the static extensions of `openChat`; an uninstall is a reopen without the extension.
// - A crash is `HarnessSupport.killable`: a killed Harness writes nothing more, and the next one opens over the
//   same storage.
// - An aborted hook or task waits until its fiber is interrupted; waits run while the `TestClock` advances
//   (`ChatSupport.waitFor`); a settled check is a forked fiber polled once pending work had its turns.
// - Reports are values in the reported causes: `extension_error` reports are the hooks' own errors, naming the
//   extension; `task_interrupted` and `task_faulted` are `Report.TaskInterrupted` and `Report.TaskFaulted`.
// - "endRun" calls the internal `endRun` inside a commit, on a plain `core.live` value.
import { assert, it } from "@effect/vitest";
import type * as Tracker from "@effect-durable-harness/delta/Tracker";
import * as Cause from "effect/Cause";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as AgentTool from "../src/AgentTool.ts";
import { AgentStorage } from "../src/AgentStorage.ts";
import * as Extension from "../src/Extension.ts";
import type { Conversation } from "../src/Harness.ts";
import * as Hook from "../src/Hook.ts";
import type { GenerationHooks, HookTask, RunSummary } from "../src/Hook.ts";
import { EntryId, TaskId } from "../src/Ids.ts";
import { InboxDoc } from "../src/Inbox.ts";
import { GenerationTask } from "../src/internal/generation.ts";
import { endRun } from "../src/internal/live.ts";
import { LiveDoc, type LiveState } from "../src/Live.ts";
import type { AssistantMessage } from "../src/Message.ts";
import type { EntryRecord } from "../src/Records.ts";
import { TaskFaulted, TaskInterrupted } from "../src/Report.ts";
import type { Submission } from "../src/Submission.ts";
import * as Task from "../src/Task.ts";
import { fauxAssistantMessage, fauxToolCall } from "../src/testing/FauxModel.ts";
import { Tx } from "../src/Tx.ts";
import { allEntries, type ChatSetup, chatSetup, openChat, textOf, waitFor } from "./ChatSupport.ts";
import { completed, killable, type TestHarness } from "./HarnessSupport.ts";
import { controlledStorage, describeEachBackend, flush, run } from "./SessionSupport.ts";

type Inner = AgentStorage["Service"];

/** A chat over `inner`; `kill` simulates a crash: nothing more lands. */
const openKillable = (inner: Inner, setup: ChatSetup, extensions: ReadonlyArray<Extension.Any>) =>
  Effect.gen(function* () {
    const crashable = yield* killable(inner);
    const opened = yield* openChat(crashable.storage, setup, { extensions });
    return { ...opened, kill: crashable.kill.pipe(Effect.andThen(opened.harness.close)) };
  });

/** What the after-run task and hook of a test did, and how they behave. */
interface Probe {
  /** Labels of the after-run task's runs, in order, reruns included. */
  readonly runs: Array<string>;
  /** Summaries the `afterRun` hook was asked with. */
  readonly asked: Array<RunSummary>;
  aborted: number;
  /** Holds every run of the after-run task until done. */
  gate?: Deferred.Deferred<void> | undefined;
  /** Holds the hook until done. */
  hookGate?: Deferred.Deferred<void> | undefined;
  /** Admitted in the after-run task's commit, before its own entry. */
  admit?:
    | ((handle: Task.ConversationHandle) => ReturnType<Task.ConversationHandle["admit"]>)
    | undefined;
}

const probe = (): Probe => ({ runs: [], asked: [], aborted: 0 });

const TestsInput = Schema.Struct({ label: Schema.String });
const RunCheckpoint = Schema.Struct({ phase: Schema.Literal("run") });

const ABORTED = { status: "terminal", outcome: { status: "aborted" } } as const;

/** The after-run task: records its run, waits for the gate, then appends a `test.tests` entry and completes. */
const testsTask = (state: Probe) =>
  Task.make({
    name: "test.run-tests",
    version: 1,
    input: TestsInput,
    checkpoint: RunCheckpoint,
    result: Schema.String,
    initial: () => ({ phase: "run" }),
    phases: {
      run: (task, runtime) =>
        Effect.gen(function* () {
          state.runs.push(task.input.label);
          const gate = state.gate;
          if (gate !== undefined) yield* Deferred.await(gate);
          const admit = state.admit;
          const handle =
            admit === undefined
              ? undefined
              : Option.getOrThrow(yield* runtime.conversation(runtime.conversationId));
          yield* runtime.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              if (admit !== undefined && handle !== undefined) yield* admit(handle);
              yield* tx.appendEntry(runtime.conversationId, {
                kind: "test.tests",
                data: task.input.label,
              });
              return completed(task.input.label);
            }),
          );
        }),
    },
    abort: (_task, runtime) =>
      Effect.gen(function* () {
        state.aborted++;
        yield* runtime.commit(Effect.succeed(ABORTED));
      }),
  });

type AfterRun = GenerationHooks["afterRun"];

/**
 * The after-run extension, with its task and an `afterRun` hook, by default asking for one task per
 * run.
 */
const install = (state: Probe, afterRun?: AfterRun) => {
  const Tests = testsTask(state);
  const handler: AfterRun =
    afterRun ??
    ((run) =>
      Effect.gen(function* () {
        state.asked.push(run);
        const gate = state.hookGate;
        if (gate !== undefined) yield* Deferred.await(gate);
        return { tasks: [Hook.hookTask(Tests, { label: `run ${state.asked.length}` })] };
      }));
  const extension = Extension.make({
    name: "after-run",
    tasks: [Tests],
    hooks: [Hook.generation({ afterRun: handler })],
  });
  return { Tests, extension };
};

const Parameters = Schema.Struct({});
const tools = Extension.make({
  name: "tools",
  tools: [
    AgentTool.make("edit", {
      description: "Edit a file",
      parameters: Parameters,
      execute: () => Effect.succeed({ output: [{ type: "text", text: "edited" }] }),
    }),
    AgentTool.make("finish", {
      description: "End the run",
      parameters: Parameters,
      execute: () =>
        Effect.succeed({
          output: [{ type: "text", text: "finished" }],
          control: { terminate: true },
        }),
    }),
    AgentTool.make("handoff", {
      description: "End the run in a new context",
      parameters: Parameters,
      execute: () =>
        Effect.succeed({
          output: [{ type: "text", text: "handed off" }],
          control: { terminate: true, handoff: "fresh start" },
        }),
    }),
  ],
});

/** Slow tools: `execute` signals `started`, then waits for `gate`. */
const slowTool = (
  name: string,
  started: Deferred.Deferred<void>,
  gate: Deferred.Deferred<void>,
  output: AgentTool.ToolExecutionResult,
) =>
  AgentTool.make(name, {
    description: name === "edit" ? "Edit a file" : "End the run in a new context",
    parameters: Parameters,
    execute: () =>
      Deferred.succeed(started, undefined).pipe(
        Effect.andThen(Deferred.await(gate)),
        Effect.as(output),
      ),
  });

/** A hook that supplies the summary, so no summarization request runs. */
const summaryHook = Hook.compaction({
  beforeCompact: () => Effect.succeed({ summary: "earlier work" }),
});

const call = (name: string, id: string): AssistantMessage =>
  fauxAssistantMessage([fauxToolCall(name, {}, { id })], { stopReason: "toolUse" });

const kindOf = (entries: ReadonlyArray<EntryRecord>, kind: string) =>
  entries.filter((entry) => entry.kind === kind);

/** Texts of the user messages of the conversation's entries, in order. */
const userTexts = (entries: ReadonlyArray<EntryRecord>) =>
  entries.flatMap((entry) =>
    (entry.model ?? []).flatMap((message) =>
      message.role === "user" ? [textOf(message) ?? ""] : [],
    ),
  );

/** Runs `effect` to its end while the `TestClock` advances. */
const finished = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  Effect.gen(function* () {
    const fiber = yield* Effect.forkChild(effect, { startImmediately: true });
    yield* waitFor(Effect.sync(() => fiber.pollUnsafe() !== undefined));
    return yield* Fiber.join(fiber);
  });

/** Waits for a submission to settle. */
const settle = (submission: Submission) => finished(submission.await);

/** Waits until the conversation is idle. */
const idle = (root: Conversation) => finished(root.awaitIdle);

/** Waits until `check` holds, with timers running. */
const eventually = (check: () => boolean) => waitFor(Effect.sync(check));

const live = (h: TestHarness, root: Conversation) =>
  h.session.snapshot(LiveDoc, root.id).pipe(Effect.map(Option.getOrUndefined));

const taskOf = (h: TestHarness, id: TaskId) =>
  h.harness.getTask(id).pipe(Effect.map(Option.getOrUndefined));

const outcomeOf = (h: TestHarness, id: TaskId) =>
  taskOf(h, id).pipe(
    Effect.map((record) =>
      record?.state.status === "terminal" ? record.state.outcome.status : undefined,
    ),
  );

/** The reported errors of one kind. */
const infos = <A>(setup: ChatSetup, is: (error: unknown) => error is A): Array<A> =>
  setup.reports.flatMap((report) => {
    const error = Option.getOrUndefined(Cause.findErrorOption(report.cause));
    return is(error) ? [error] : [];
  });

const isInterrupted = Schema.is(TaskInterrupted);
const isFaulted = Schema.is(TaskFaulted);

describeEachBackend("afterRun", (backend) => {
  it.effect(
    "waits for the tasks an answer's hooks ask for: inputs done at the answer, the run busy until they end",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const setup = chatSetup();
          const state = probe();
          state.gate = Deferred.makeUnsafe<void>();
          const { extension } = install(state);
          setup.faux.setResponses([call("edit", "a"), fauxAssistantMessage("done")]);
          const { harness, root } = yield* openChat(yield* controlledStorage, setup, {
            extensions: [extension, tools],
          });
          const submission = yield* root.submit({ type: "input", content: "go" });
          // The input settles with the answer, before the task finishes.
          assert.strictEqual((yield* settle(submission)).status, "done");
          yield* eventually(() => state.runs.length === 1);
          const current = (yield* live(harness, root))!;
          assert.deepStrictEqual(current.run?.inputs, []);
          assert.strictEqual(current.run?.afterRun?.tasks?.length, 1);
          const child = yield* taskOf(harness, current.run!.afterRun!.tasks![0]!);
          assert.strictEqual(child?.owner, current.run!.taskId);
          assert.strictEqual(child?.background, false);
          const waiting = yield* Effect.forkChild(root.awaitIdle, { startImmediately: true });
          yield* flush;
          assert.isUndefined(waiting.pollUnsafe());

          const entries = yield* allEntries(root);
          const answer = kindOf(entries, "core.assistant").at(-1)!;
          assert.deepStrictEqual(state.asked, [
            {
              reason: "answer",
              first: kindOf(entries, "core.user")[0]!.id,
              answer: answer.id,
              last: answer.id,
            },
          ]);
          yield* Deferred.succeed(state.gate, undefined);
          yield* finished(Fiber.join(waiting));
          const after = yield* live(harness, root);
          assert.isUndefined(after?.run);
          assert.isUndefined(after?.runFailure);
          assert.strictEqual((yield* allEntries(root)).at(-1)?.kind, "test.tests");
          const generation = yield* taskOf(harness, current.run!.taskId);
          assert.deepStrictEqual(generation?.state, {
            status: "terminal",
            outcome: { status: "completed", result: { entryId: answer.id } },
          });
          yield* harness.close;
        }),
      ),
  );

  it.effect(
    "asks after a terminate round, the run's last entry its newest result or its handoff's reset",
    () =>
      Effect.gen(function* () {
        for (const [tool, lastKind] of [
          ["finish", "core.tool-result"],
          ["handoff", "core.reset"],
        ] as const) {
          yield* run(backend)(
            Effect.gen(function* () {
              const setup = chatSetup();
              const state = probe();
              const { extension } = install(state);
              setup.faux.setResponses([call(tool, "a")]);
              const { harness, root } = yield* openChat(yield* controlledStorage, setup, {
                extensions: [extension, tools],
              });
              yield* settle(yield* root.submit({ type: "input", content: "go" }));
              yield* idle(root);
              const entries = yield* allEntries(root);
              const assistant = kindOf(entries, "core.assistant").at(-1)!;
              assert.deepStrictEqual(state.asked, [
                {
                  reason: "terminate",
                  first: kindOf(entries, "core.user")[0]!.id,
                  answer: assistant.id,
                  last: kindOf(entries, lastKind).at(-1)!.id,
                },
              ]);
              assert.deepStrictEqual(state.runs, ["run 1"]);
              assert.isUndefined((yield* live(harness, root))?.run);
              yield* harness.close;
            }),
          );
        }
      }),
  );

  it.effect(
    "ends the run in one commit without an afterRun hook, and after the hook when it asks for nothing",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const plain = chatSetup();
          const state = probe();
          plain.faux.setResponses([fauxAssistantMessage("done")]);
          const opened = yield* openChat(yield* controlledStorage, plain, {
            extensions: [Extension.make({ name: "tasks-only", tasks: [testsTask(state)] })],
          });
          yield* settle(yield* opened.root.submit({ type: "input", content: "go" }));
          // No hook: the answer's commit ended the run.
          assert.deepStrictEqual(yield* live(opened.harness, opened.root), {});
          yield* opened.harness.close;

          const setup = chatSetup();
          const { extension } = install(state, (summary) =>
            Effect.sync(() => {
              state.asked.push(summary);
              return undefined;
            }),
          );
          setup.faux.setResponses([fauxAssistantMessage("done")]);
          const { harness, root } = yield* openChat(yield* controlledStorage, setup, {
            extensions: [extension],
          });
          yield* settle(yield* root.submit({ type: "input", content: "go" }));
          yield* idle(root);
          assert.strictEqual(state.asked.length, 1);
          assert.deepStrictEqual(state.runs, []);
          assert.deepStrictEqual(yield* live(harness, root), {});
          yield* harness.close;
        }),
      ),
  );

  it.effect(
    "queues input during the tasks and places it after them: writes first, then steers and follow-ups",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const setup = chatSetup();
          const state = probe();
          state.gate = Deferred.makeUnsafe<void>();
          const Tests = testsTask(state);
          const extension = Extension.make({
            name: "after-run",
            tasks: [Tests],
            hooks: [
              Hook.generation({
                afterRun: (summary) =>
                  Effect.sync(() => {
                    state.asked.push(summary);
                    // Only the first run gets a task.
                    return state.asked.length === 1
                      ? { tasks: [Hook.hookTask(Tests, { label: "run 1" })] }
                      : undefined;
                  }),
              }),
            ],
          });
          // The task admits its result for the model in its own commit.
          state.admit = (handle) =>
            handle.admit({
              type: "write",
              entry: {
                kind: "test.result",
                model: [{ role: "user", content: "tests passed", timestamp: 1 }],
              },
            });
          setup.faux.setResponses([
            fauxAssistantMessage("first"),
            fauxAssistantMessage("second"),
            fauxAssistantMessage("third"),
          ]);
          const { harness, root } = yield* openChat(yield* controlledStorage, setup, {
            extensions: [extension],
          });
          yield* settle(yield* root.submit({ type: "input", content: "go" }));
          yield* eventually(() => state.runs.length === 1);

          const steer = yield* root.submit({ type: "input", content: "steer", whenBusy: "steer" });
          const followUp = yield* root.submit({ type: "input", content: "later" });
          const rejected = yield* Effect.flip(
            root.submit({ type: "input", content: "x", whenBusy: "reject" }),
          );
          assert.strictEqual(rejected._tag, "ConversationBusy");
          const busy = yield* Effect.flip(root.submit({ type: "continue" }));
          assert.strictEqual(busy._tag, "ConversationBusy");
          // Busy, so not idle: it queues and starts the next run after the tasks.
          const idleSteer = yield* root.submit({
            type: "input",
            content: "steer 2",
            whenBusy: "steer",
            whenIdle: "reject",
          });
          const note = yield* root.submit({ type: "write", entry: { kind: "test.note" } });
          assert.strictEqual((yield* note.status).status, "done");
          assert.strictEqual((yield* steer.status).status, "queued");
          const inbox = Option.getOrThrow(yield* harness.session.snapshot(InboxDoc, root.id));
          assert.strictEqual(inbox.items.length, 3);

          yield* Deferred.succeed(state.gate, undefined);
          yield* idle(root);
          for (const submission of [steer, followUp, idleSteer]) {
            assert.strictEqual((yield* settle(submission)).status, "done");
          }
          const entries = yield* allEntries(root);
          assert.deepStrictEqual(
            entries.filter((entry) => entry.kind !== "core.system").map((entry) => entry.kind),
            [
              "core.user",
              "core.assistant",
              "test.note",
              "test.tests",
              "test.result",
              "core.user",
              "core.user",
              "core.assistant",
              "core.user",
              "core.assistant",
            ],
          );
          assert.deepStrictEqual(userTexts(entries), [
            "go",
            "tests passed",
            "steer",
            "later",
            "steer 2",
          ]);
          yield* harness.close;
        }),
      ),
  );

  it.effect("places a reset queued during the tasks after them", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        const state = probe();
        state.gate = Deferred.makeUnsafe<void>();
        const { extension } = install(state);
        setup.faux.setResponses([fauxAssistantMessage("done")]);
        const { harness, root } = yield* openChat(yield* controlledStorage, setup, {
          extensions: [extension],
        });
        yield* settle(yield* root.submit({ type: "input", content: "go" }));
        yield* eventually(() => state.runs.length === 1);
        yield* root.reset(undefined);
        assert.deepStrictEqual(kindOf(yield* allEntries(root), "core.reset"), []);
        yield* Deferred.succeed(state.gate, undefined);
        yield* idle(root);
        const kinds = (yield* allEntries(root)).map((entry) => entry.kind);
        assert.deepStrictEqual(kinds.slice(-2), ["test.tests", "core.reset"]);
        yield* harness.close;
      }),
    ),
  );

  it.effect(
    "asks once a run really ends: not while onYield continues it, and when a queued steer drops the continuation",
    () =>
      Effect.gen(function* () {
        yield* run(backend)(
          Effect.gen(function* () {
            const setup = chatSetup();
            const state = probe();
            const { extension } = install(state);
            let yields = 0;
            const again = Extension.make({
              name: "again",
              hooks: [
                Hook.generation({
                  onYield: () =>
                    Effect.sync(() => (++yields <= 2 ? { continue: "again" } : undefined)),
                }),
              ],
            });
            setup.faux.setResponses([
              fauxAssistantMessage("1"),
              fauxAssistantMessage("2"),
              fauxAssistantMessage("3"),
            ]);
            const { harness, root } = yield* openChat(yield* controlledStorage, setup, {
              extensions: [extension, again],
            });
            yield* settle(yield* root.submit({ type: "input", content: "go" }));
            yield* idle(root);
            const entries = yield* allEntries(root);
            assert.deepStrictEqual(state.asked, [
              {
                reason: "answer",
                first: kindOf(entries, "core.user")[0]!.id,
                answer: kindOf(entries, "core.assistant").at(-1)!.id,
                last: kindOf(entries, "core.assistant").at(-1)!.id,
              },
            ]);
            yield* harness.close;
          }),
        );

        // A steer queued while the answer's hooks run: the continuation is dropped, and the run ends after its task.
        // A fresh storage.
        yield* run(backend)(
          Effect.gen(function* () {
            const dropped = chatSetup();
            const second = probe();
            const secondInstall = install(second);
            let root2: Conversation | undefined;
            const steering = Extension.make({
              name: "steering",
              hooks: [
                Hook.generation({
                  onYield: () =>
                    Effect.gen(function* () {
                      if (second.asked.length > 0) return undefined;
                      yield* root2!.submit({ type: "input", content: "steer", whenBusy: "steer" });
                      return { continue: "again" };
                    }).pipe(Effect.orDie),
                }),
              ],
            });
            dropped.faux.setResponses([fauxAssistantMessage("1"), fauxAssistantMessage("2")]);
            const opened = yield* openChat(yield* controlledStorage, dropped, {
              extensions: [secondInstall.extension, steering],
            });
            root2 = opened.root;
            yield* settle(yield* opened.root.submit({ type: "input", content: "go" }));
            yield* idle(opened.root);
            assert.strictEqual(second.asked.length, 2);
            assert.deepStrictEqual(second.runs, ["run 1", "run 2"]);
            assert.deepStrictEqual(userTexts(yield* allEntries(opened.root)), ["go", "steer"]);
            yield* opened.harness.close;
          }),
        );
      }),
  );

  it.effect(
    "asks the hooks again after a crash before the tasks exist, and never once they do",
    () =>
      Effect.gen(function* () {
        yield* run(backend)(
          Effect.gen(function* () {
            const inner = yield* AgentStorage;
            const setup = chatSetup();
            const state = probe();
            state.hookGate = Deferred.makeUnsafe<void>();
            const { extension } = install(state);
            setup.faux.setResponses([fauxAssistantMessage("done")]);
            let opened = yield* openKillable(inner, setup, [extension]);
            yield* settle(yield* opened.root.submit({ type: "input", content: "go" }));
            yield* eventually(() => state.asked.length === 1);
            yield* opened.kill; // a crash while the hook runs, in `finish`
            state.hookGate = undefined;
            state.gate = Deferred.makeUnsafe<void>();
            opened = yield* openKillable(inner, setup, [extension]);
            yield* opened.harness.harness.resume;
            yield* eventually(() => state.runs.length === 1);
            assert.strictEqual(state.asked.length, 2);
            yield* opened.kill; // a crash while the task runs, in `drain`
            state.gate = undefined;
            opened = yield* openKillable(inner, setup, [extension]);
            yield* opened.harness.harness.resume;
            yield* idle(opened.root);
            // The task ran again; the hook was not asked again, and only one task exists.
            assert.strictEqual(state.asked.length, 2);
            assert.deepStrictEqual(state.runs, ["run 2", "run 2"]);
            assert.strictEqual(kindOf(yield* allEntries(opened.root), "test.tests").length, 1);
            assert.deepStrictEqual((yield* opened.harness.harness.inspect).tasks, []);
            yield* opened.harness.close;
          }),
        );

        // An orderly close in `drain` resumes the same way.
        yield* run(backend)(
          Effect.gen(function* () {
            const inner = yield* AgentStorage;
            const other = probe();
            other.gate = Deferred.makeUnsafe<void>();
            const setup = chatSetup();
            const { extension } = install(other);
            setup.faux.setResponses([fauxAssistantMessage("done")]);
            let reopened = yield* openKillable(inner, setup, [extension]);
            yield* settle(yield* reopened.root.submit({ type: "input", content: "go" }));
            yield* eventually(() => other.runs.length === 1);
            yield* reopened.harness.close;
            other.gate = undefined;
            reopened = yield* openKillable(inner, setup, [extension]);
            yield* reopened.harness.harness.resume;
            yield* idle(reopened.root);
            assert.strictEqual(other.asked.length, 1);
            assert.deepStrictEqual(other.runs, ["run 1", "run 1"]);
            yield* reopened.harness.close;
          }),
        );
      }),
  );

  it.effect(
    "ends an answered run without a failure when Stop comes while the hooks run or the tasks run",
    () =>
      Effect.gen(function* () {
        for (const stage of ["finish", "drain"] as const) {
          yield* run(backend)(
            Effect.gen(function* () {
              const setup = chatSetup();
              const state = probe();
              const Tests = testsTask(state);
              state.gate = Deferred.makeUnsafe<void>();
              const extension = Extension.make({
                name: "after-run",
                tasks: [Tests],
                hooks: [
                  Hook.generation({
                    afterRun: (summary) =>
                      Effect.gen(function* () {
                        state.asked.push(summary);
                        if (stage === "finish") return yield* Effect.never;
                        return { tasks: [Hook.hookTask(Tests, { label: "run 1" })] };
                      }),
                  }),
                ],
              });
              setup.faux.setResponses([fauxAssistantMessage("done")]);
              const { harness, root } = yield* openChat(yield* controlledStorage, setup, {
                extensions: [extension],
              });
              const submission = yield* root.submit({ type: "input", content: "go" });
              yield* settle(submission);
              yield* eventually(() =>
                stage === "finish" ? state.asked.length === 1 : state.runs.length === 1,
              );
              yield* finished(root.abort());
              const after = yield* live(harness, root);
              assert.isUndefined(after?.run);
              assert.isUndefined(after?.runFailure);
              assert.strictEqual((yield* submission.status).status, "done");
              assert.strictEqual(state.aborted, stage === "drain" ? 1 : 0);
              assert.deepStrictEqual(state.runs, stage === "drain" ? ["run 1"] : []);
              yield* harness.close;
            }),
          );
        }
      }),
  );

  it.effect("ends the run normally when one of its tasks is aborted on its own", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        const state = probe();
        state.gate = Deferred.makeUnsafe<void>();
        const { extension } = install(state);
        setup.faux.setResponses([fauxAssistantMessage("done")]);
        const { harness, root } = yield* openChat(yield* controlledStorage, setup, {
          extensions: [extension],
        });
        yield* settle(yield* root.submit({ type: "input", content: "go" }));
        yield* eventually(() => state.runs.length === 1);
        const current = (yield* live(harness, root))!;
        yield* harness.harness.abortTask(current.run!.afterRun!.tasks![0]!);
        yield* idle(root);
        assert.strictEqual(state.aborted, 1);
        assert.deepStrictEqual(yield* live(harness, root), {});
        assert.strictEqual(yield* outcomeOf(harness, current.run!.taskId), "completed");
        yield* harness.close;
      }),
    ),
  );

  // One report per failing hook, with the hook's own error.
  it.effect("reports broken requests and hooks and creates the rest", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        const state = probe();
        const Tests = testsTask(state);
        const Throwing = Task.make({
          name: "test.throwing",
          version: 1,
          input: Schema.Null,
          checkpoint: RunCheckpoint,
          result: Schema.Null,
          initial: () => {
            throw new Error("initial failed");
          },
          phases: { run: () => Effect.void },
          abort: () => Effect.void,
        });
        // Its `initial()` starts at a phase the definition does not have.
        const BadStart = Task.make({
          name: "test.bad-start",
          version: 1,
          input: Schema.Null,
          // Any phase name decodes, so `initial` may name one the handlers do not serve.
          checkpoint: Schema.Struct({ phase: Schema.String }),
          result: Schema.Null,
          initial: () => ({ phase: "nowhere" }),
          phases: { run: () => Effect.void },
          abort: () => Effect.void,
        });
        const notJson: Schema.JsonObject = { label: "x" };
        Reflect.set(notJson, "label", () => "not JSON");
        const requests: ReadonlyArray<HookTask> = [
          { task: { definition: { ...Tests.definition, name: "test.unknown" } }, input: null },
          { task: GenerationTask, input: {} },
          { task: Tests, input: notJson },
          Hook.hookTask(Throwing, null),
          Hook.hookTask(BadStart, null),
          Hook.hookTask(Tests, { label: "ok" }),
        ];
        const notAList = { tasks: requests.slice(0, 0) };
        Reflect.set(notAList, "tasks", "x");
        const extensions = [
          Extension.make({
            name: "after-run",
            tasks: [Tests, Throwing, BadStart],
            hooks: [Hook.generation({ afterRun: () => Effect.succeed({ tasks: requests }) })],
          }),
          Extension.make({
            name: "broken",
            hooks: [Hook.generation({ afterRun: () => Effect.die(new Error("hook failed")) })],
          }),
          Extension.make({
            name: "not-a-list",
            hooks: [Hook.generation({ afterRun: () => Effect.succeed(notAList) })],
          }),
        ];
        setup.faux.setResponses([fauxAssistantMessage("done")]);
        const { harness, root } = yield* openChat(yield* controlledStorage, setup, {
          extensions,
        });
        yield* settle(yield* root.submit({ type: "input", content: "go" }));
        yield* idle(root);
        assert.deepStrictEqual(state.runs, ["ok"]);
        // One report per hook; the requests of one hook come together.
        assert.strictEqual(setup.reports.length, 3);
        // Each names the hook's extension.
        assert.deepStrictEqual(
          setup.reports.map((report) => report.kind),
          ["extension_error", "extension_error", "extension_error"],
        );
        const rejected = Cause.squash(setup.reports[0]!.cause);
        assert.instanceOf(rejected, AggregateError);
        if (rejected instanceof AggregateError) {
          assert.deepStrictEqual(
            rejected.errors.map((error: Error) => error.message),
            [
              "Task test.unknown is not installed",
              "A hook cannot start the built-in task core.generation",
              "Value contains a non-JSON function; expected strict JSON",
              "initial failed",
              "Task test.bad-start starts at the unknown phase nowhere",
            ],
          );
        }
        const broken = Cause.squash(setup.reports[1]!.cause);
        assert.strictEqual(broken instanceof Error ? broken.message : undefined, "hook failed");
        assert.instanceOf(Cause.squash(setup.reports[2]!.cause), TypeError);
        assert.deepStrictEqual(yield* live(harness, root), {});
        yield* harness.close;
      }),
    ),
  );

  // Each request is checked on its own, so a request whose task name is not a string is
  // rejected on its own and the hook's later requests are still created.
  it.effect(
    "rejects a request whose task name is not a string and creates the hook's later ones",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const setup = chatSetup();
          const state = probe();
          const Tests = testsTask(state);
          const definition = { ...Tests.definition };
          Reflect.set(definition, "name", 42);
          const requests: ReadonlyArray<HookTask> = [
            { task: { definition }, input: null },
            Hook.hookTask(Tests, { label: "ok" }),
          ];
          setup.faux.setResponses([fauxAssistantMessage("done")]);
          const { harness, root } = yield* openChat(yield* controlledStorage, setup, {
            extensions: [
              Extension.make({
                name: "after-run",
                tasks: [Tests],
                hooks: [Hook.generation({ afterRun: () => Effect.succeed({ tasks: requests }) })],
              }),
            ],
          });
          yield* settle(yield* root.submit({ type: "input", content: "go" }));
          yield* idle(root);
          assert.deepStrictEqual(state.runs, ["ok"]);
          // One rejection, the request's own, reported against its hook.
          assert.strictEqual(setup.reports.length, 1);
          assert.instanceOf(Cause.squash(setup.reports[0]!.cause), TypeError);
          assert.deepStrictEqual(yield* live(harness, root), {});
          yield* harness.close;
        }),
      ),
  );

  it.effect(
    "creates a requested task from the installed definition, calling its initial() once, off the line",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const setup = chatSetup();
          const counts = { installed: 0, token: 0 };
          const define = (version: number, count: () => void) =>
            Task.make({
              name: "test.versioned",
              version,
              input: Schema.Null,
              checkpoint: RunCheckpoint,
              result: Schema.Null,
              initial: () => {
                count();
                return { phase: "run" };
              },
              phases: {
                run: (_task, runtime) => runtime.commit(Effect.succeed(completed(null))),
              },
              abort: () => Effect.void,
            });
          const Installed = define(2, () => counts.installed++);
          // The hook's extension holds an older copy of the task.
          const Stale = define(1, () => counts.token++);
          setup.faux.setResponses([fauxAssistantMessage("done")]);
          const { harness, root } = yield* openChat(yield* controlledStorage, setup, {
            extensions: [
              Extension.make({ name: "tasks", tasks: [Installed] }),
              Extension.make({
                name: "asker",
                hooks: [
                  Hook.generation({
                    afterRun: () => Effect.succeed({ tasks: [Hook.hookTask(Stale, null)] }),
                  }),
                ],
              }),
            ],
          });
          yield* settle(yield* root.submit({ type: "input", content: "go" }));
          yield* idle(root);
          const created = yield* harness.harness.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              return yield* tx.scanTasks({ conversationId: root.id, kind: "test.versioned" }, 10);
            }),
          );
          assert.strictEqual(created.items.length, 1);
          assert.strictEqual(created.items[0]!.version, 2);
          assert.strictEqual(created.items[0]!.state.status, "terminal");
          assert.deepStrictEqual(counts, { installed: 1, token: 0 });
          yield* harness.close;
        }),
      ),
  );

  it.effect("lets a hook read the run's history, tool results included", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        const kinds: Array<string> = [];
        const answers: Array<string | undefined> = [];
        const reader = Extension.make({
          name: "reader",
          hooks: [
            Hook.generation({
              afterRun: (summary, api) =>
                Effect.gen(function* () {
                  const page = yield* api.entries(
                    { minEntryId: summary.first, maxEntryId: summary.last, order: "ascending" },
                    100,
                  );
                  kinds.push(...page.items.map((entry) => entry.kind));
                  answers.push(Option.getOrUndefined(yield* api.entry(summary.answer))?.kind);
                  return undefined;
                }).pipe(Effect.orDie),
            }),
          ],
        });
        setup.faux.setResponses([call("edit", "a"), fauxAssistantMessage("done")]);
        const { harness, root } = yield* openChat(yield* controlledStorage, setup, {
          extensions: [tools, reader],
        });
        yield* settle(yield* root.submit({ type: "input", content: "go" }));
        yield* idle(root);
        assert.deepStrictEqual(
          kinds.filter((kind) => kind !== "core.system"),
          ["core.user", "core.assistant", "core.tool-result", "core.assistant"],
        );
        assert.deepStrictEqual(answers, ["core.assistant"]);
        yield* harness.close;
      }),
    ),
  );

  it.effect(
    "lets a hook read the run's history after a compaction summarized the start of the run",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const setup = chatSetup();
          setup.settings.compaction = {
            enabled: false,
            reserveTokens: 1000,
            keepRecentTokens: 1,
            backgroundTokens: 0,
          };
          const gate = Deferred.makeUnsafe<void>();
          const started = Deferred.makeUnsafe<void>();
          const runs: Array<Array<string>> = [];
          const slow = Extension.make({
            name: "slow",
            tools: [
              slowTool("edit", started, gate, { output: [{ type: "text", text: "edited" }] }),
            ],
            hooks: [
              summaryHook,
              Hook.generation({
                afterRun: (summary, api) =>
                  Effect.gen(function* () {
                    const page = yield* api.entries(
                      { minEntryId: summary.first, maxEntryId: summary.last, order: "ascending" },
                      100,
                    );
                    runs.push(page.items.map((entry) => entry.kind));
                    return undefined;
                  }).pipe(Effect.orDie),
              }),
            ],
          });
          setup.faux.setResponses([call("edit", "a"), fauxAssistantMessage("done")]);
          const { harness, root } = yield* openChat(yield* controlledStorage, setup, {
            extensions: [slow],
          });
          const submission = yield* root.submit({ type: "input", content: "go" });
          yield* finished(Deferred.await(started));
          // Its summary is placed at the round's post-tools boundary, in the middle of the run.
          const compaction = yield* root.compact();
          yield* finished(harness.harness.awaitTask(compaction));
          yield* Deferred.succeed(gate, undefined);
          yield* settle(submission);
          yield* idle(root);
          const active = (yield* root.context()).entries.map((entry) => entry.kind);
          // The active context lost the run's input to the summary; the run's history did not.
          assert.notInclude(active, "core.user");
          assert.strictEqual(runs.length, 1);
          assert.deepStrictEqual(
            runs[0]!.filter((kind) => kind !== "core.system"),
            [
              "core.user",
              "core.assistant",
              "core.tool-result",
              "core.compaction",
              "core.assistant",
            ],
          );
          yield* harness.close;
        }),
      ),
  );

  it.effect(
    "ends an answered run completed, without a failure, once finish was cut off restartLimit times",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const inner = yield* AgentStorage;
          const setup = chatSetup();
          setup.settings.restartLimit = 1;
          const state = probe();
          state.hookGate = Deferred.makeUnsafe<void>();
          const { extension } = install(state);
          setup.faux.setResponses([fauxAssistantMessage("done")]);
          let opened = yield* openKillable(inner, setup, [extension]);
          yield* settle(yield* opened.root.submit({ type: "input", content: "go" }));
          yield* eventually(() => state.asked.length === 1);
          yield* opened.root.submit({ type: "input", content: "later" });
          yield* opened.kill;
          opened = yield* openKillable(inner, setup, [extension]);
          yield* opened.harness.harness.resume;
          yield* idle(opened.root);
          assert.strictEqual(state.asked.length, 1);
          const after = yield* live(opened.harness, opened.root);
          assert.isUndefined(after?.run);
          assert.isUndefined(after?.runFailure);
          // Queued items stay, as after a failed run.
          const inbox = Option.getOrThrow(
            yield* opened.harness.session.snapshot(InboxDoc, opened.root.id),
          );
          assert.strictEqual(inbox.items.length, 1);
          yield* waitFor(Effect.sync(() => infos(setup, isInterrupted).length > 0));
          assert.strictEqual(infos(setup, isInterrupted)[0]!.reason, "restart_limit");
          yield* opened.harness.close;
        }),
      ),
  );

  it.effect("keeps the run busy while an after-run task cannot run, until Stop orphans it", () =>
    run(backend)(
      Effect.gen(function* () {
        const inner = yield* AgentStorage;
        const setup = chatSetup();
        const state = probe();
        state.gate = Deferred.makeUnsafe<void>();
        const { extension } = install(state);
        setup.faux.setResponses([fauxAssistantMessage("done")]);
        let opened = yield* openKillable(inner, setup, [extension]);
        yield* settle(yield* opened.root.submit({ type: "input", content: "go" }));
        yield* eventually(() => state.runs.length === 1);
        yield* opened.kill;
        // Reopened without the extension: its task is blocked.
        const bare = chatSetup();
        opened = yield* openKillable(inner, bare, []);
        yield* opened.harness.harness.resume;
        const current = (yield* live(opened.harness, opened.root))!;
        assert.strictEqual(current.run?.afterRun?.tasks?.length, 1);
        yield* waitFor(
          opened.harness.harness.inspect.pipe(
            Effect.map((inspection) =>
              inspection.tasks.some((task) => task.state.kind === "blocked"),
            ),
          ),
        );
        const waiting = yield* Effect.forkChild(opened.root.awaitIdle, { startImmediately: true });
        yield* flush;
        assert.isUndefined(waiting.pollUnsafe());
        yield* finished(opened.root.abort());
        const after = yield* live(opened.harness, opened.root);
        assert.isUndefined(after?.run);
        assert.isUndefined(after?.runFailure);
        assert.strictEqual(
          yield* outcomeOf(opened.harness, current.run!.afterRun!.tasks![0]!),
          "orphaned",
        );
        yield* Fiber.interrupt(waiting);
        yield* opened.harness.close;
      }),
    ),
  );

  it.effect("starts a fix run with a follow-up a task admits, after the tasks", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        const state = probe();
        const Tests = testsTask(state);
        const extension = Extension.make({
          name: "after-run",
          tasks: [Tests],
          hooks: [
            Hook.generation({
              afterRun: (summary) =>
                Effect.sync(() => {
                  state.asked.push(summary);
                  return state.asked.length === 1
                    ? { tasks: [Hook.hookTask(Tests, { label: "run 1" })] }
                    : undefined;
                }),
            }),
          ],
        });
        state.admit = (handle) =>
          handle.admit({
            type: "input",
            content: "fix the tests",
            requestId: "fix-1",
            whenBusy: "followUp",
          });
        setup.faux.setResponses([fauxAssistantMessage("done"), fauxAssistantMessage("fixed")]);
        const { harness, root } = yield* openChat(yield* controlledStorage, setup, {
          extensions: [extension],
        });
        yield* settle(yield* root.submit({ type: "input", content: "go" }));
        yield* idle(root);
        const entries = yield* allEntries(root);
        assert.deepStrictEqual(
          entries.filter((entry) => entry.kind !== "core.system").map((entry) => entry.kind),
          ["core.user", "core.assistant", "test.tests", "core.user", "core.assistant"],
        );
        assert.deepStrictEqual(userTexts(entries), ["go", "fix the tests"]);
        assert.strictEqual(state.asked.length, 2);
        // The fix run's history starts with its own input.
        assert.strictEqual(state.asked[1]!.first, kindOf(entries, "core.user")[1]!.id);
        yield* harness.close;
      }),
    ),
  );

  it.effect("gives a continue after a failed run the failed run's history", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        const state = probe();
        const { extension } = install(state);
        setup.faux.setResponses([call("edit", "a"), fauxAssistantMessage("done")]);
        const { harness, root } = yield* openChat(yield* controlledStorage, setup, {
          extensions: [extension, tools],
        });
        yield* root.configure({ maxTurns: 1 });
        const failed = yield* settle(yield* root.submit({ type: "input", content: "go" }));
        assert.strictEqual(failed.status, "unanswered");
        const user = kindOf(yield* allEntries(root), "core.user")[0]!;
        const failure = (yield* live(harness, root))?.runFailure;
        assert.strictEqual(failure?.reason, "turn_limit");
        assert.strictEqual(failure?.first, user.id);
        assert.deepStrictEqual(state.asked, []);
        yield* harness.harness.commit(root.admit({ type: "continue" }));
        yield* idle(root);
        const answer = kindOf(yield* allEntries(root), "core.assistant").at(-1)!;
        assert.deepStrictEqual(state.asked, [
          { reason: "answer", first: user.id, answer: answer.id, last: answer.id },
        ]);
        yield* harness.close;
      }),
    ),
  );

  // The extension is uninstalled by reopening the Harness without it.
  it.effect("ends the run without asking when the hooks are deselected before finish runs", () =>
    run(backend)(
      Effect.gen(function* () {
        const inner = yield* AgentStorage;
        const setup = chatSetup();
        const state = probe();
        state.hookGate = Deferred.makeUnsafe<void>();
        const { extension } = install(state);
        setup.faux.setResponses([fauxAssistantMessage("done")]);
        let opened = yield* openKillable(inner, setup, [extension]);
        yield* settle(yield* opened.root.submit({ type: "input", content: "go" }));
        yield* eventually(() => state.asked.length === 1);
        yield* opened.kill;
        opened = yield* openKillable(inner, setup, []);
        yield* opened.harness.harness.resume;
        yield* idle(opened.root);
        assert.strictEqual(state.asked.length, 1);
        assert.deepStrictEqual(state.runs, []);
        assert.deepStrictEqual(yield* live(opened.harness, opened.root), {});
        yield* opened.harness.close;
      }),
    ),
  );

  it.effect("starts a fork of a conversation running its after-run tasks idle", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        const state = probe();
        state.gate = Deferred.makeUnsafe<void>();
        const { extension } = install(state);
        setup.faux.setResponses([fauxAssistantMessage("done")]);
        const { harness, root } = yield* openChat(yield* controlledStorage, setup, {
          extensions: [extension],
        });
        yield* settle(yield* root.submit({ type: "input", content: "go" }));
        yield* eventually(() => state.runs.length === 1);
        yield* root.submit({ type: "input", content: "later" });
        const answer = kindOf(yield* allEntries(root), "core.assistant").at(-1)!;
        const fork = yield* root.fork(answer.id, { ownership: { kind: "ownerless" } });
        assert.isUndefined((yield* live(harness, fork))?.run);
        const inbox = Option.getOrUndefined(yield* harness.session.snapshot(InboxDoc, fork.id));
        assert.deepStrictEqual(inbox?.items ?? [], []);
        yield* Deferred.succeed(state.gate, undefined);
        yield* idle(root);
        yield* harness.close;
      }),
    ),
  );

  it.effect("gives a run without a recorded first entry its answer as first", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        const state = probe();
        const { extension } = install(state);
        let root: Conversation | undefined;
        const legacy = Extension.make({
          name: "legacy",
          hooks: [
            Hook.generation({
              // As a run started before `first` existed.
              onYield: () =>
                root!
                  .commit(
                    Effect.gen(function* () {
                      const tx = yield* Tx;
                      delete (yield* tx.doc(LiveDoc, root!.id)).run!.first;
                    }),
                  )
                  .pipe(Effect.orDie, Effect.as(undefined)),
            }),
          ],
        });
        setup.faux.setResponses([fauxAssistantMessage("done")]);
        const opened = yield* openChat(yield* controlledStorage, setup, {
          extensions: [extension, legacy],
        });
        root = opened.root;
        yield* settle(yield* root.submit({ type: "input", content: "go" }));
        yield* idle(root);
        const answer = kindOf(yield* allEntries(root), "core.assistant").at(-1)!;
        assert.deepStrictEqual(state.asked, [
          { reason: "answer", first: answer.id, answer: answer.id, last: answer.id },
        ]);
        yield* opened.harness.close;
      }),
    ),
  );

  it.effect("ends the run normally when its after-run task is cut off restartLimit times", () =>
    run(backend)(
      Effect.gen(function* () {
        const inner = yield* AgentStorage;
        const setup = chatSetup();
        setup.settings.restartLimit = 1;
        const state = probe();
        state.gate = Deferred.makeUnsafe<void>();
        const { extension } = install(state);
        setup.faux.setResponses([fauxAssistantMessage("done")]);
        let opened = yield* openKillable(inner, setup, [extension]);
        yield* settle(yield* opened.root.submit({ type: "input", content: "go" }));
        yield* eventually(() => state.runs.length === 1);
        const generation = (yield* live(opened.harness, opened.root))!.run!.taskId;
        // The task is cut off, and the waiting generation is not: the scheduler faults the task, and the run ends.
        yield* opened.kill;
        state.gate = undefined;
        opened = yield* openKillable(inner, setup, [extension]);
        yield* opened.harness.harness.resume;
        yield* idle(opened.root);
        assert.strictEqual(yield* outcomeOf(opened.harness, generation), "completed");
        assert.isUndefined((yield* live(opened.harness, opened.root))?.runFailure);
        assert.deepStrictEqual(state.runs, ["run 1"]);
        yield* waitFor(Effect.sync(() => infos(setup, isFaulted).length > 0));
        assert.strictEqual(infos(setup, isFaulted)[0]!.reason, "restart_limit");
        yield* opened.harness.close;
      }),
    ),
  );

  it.effect(
    "reports no task_interrupted for a task that ends normally after the limit was lowered",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const inner = yield* AgentStorage;
          const setup = chatSetup();
          const state = { started: 0 };
          const Worker = Task.make({
            name: "test.worker",
            version: 1,
            input: Schema.Null,
            checkpoint: RunCheckpoint,
            result: Schema.Null,
            initial: () => ({ phase: "run" }),
            phases: {
              run: (_task, runtime) =>
                Effect.gen(function* () {
                  state.started++;
                  // The run a crash cuts off.
                  if (state.started === 1) return yield* Effect.never;
                  // The host lowers the limit while the rerun runs.
                  setup.settings.restartLimit = 1;
                  yield* runtime.commit(Effect.succeed(completed(null)));
                }),
            },
            abort: () => Effect.void,
          });
          const extensions = [Extension.make({ name: "worker", tasks: [Worker] })];
          let opened = yield* openKillable(inner, setup, extensions);
          const id = yield* opened.root.commit(
            Effect.flatMap(Tx, (tx) =>
              tx.createTask(Worker, null, { ownership: { kind: "conversation" } }),
            ),
          );
          yield* opened.harness.harness.resume;
          yield* eventually(() => state.started === 1);
          yield* opened.kill;
          opened = yield* openKillable(inner, setup, extensions);
          yield* opened.harness.harness.resume;
          const settledTask = yield* finished(opened.harness.harness.awaitTask(id));
          assert.strictEqual(settledTask.state.outcome.status, "completed");
          assert.deepStrictEqual(infos(setup, isInterrupted), []);
          setup.settings.restartLimit = undefined;
          yield* opened.harness.close;
        }),
      ),
  );

  it.effect(
    "reports no task_interrupted for a generation that ends normally after the limit was lowered in finish",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const inner = yield* AgentStorage;
          const setup = chatSetup();
          const state = probe();
          state.hookGate = Deferred.makeUnsafe<void>();
          const { extension } = install(state);
          setup.faux.setResponses([fauxAssistantMessage("done")]);
          let opened = yield* openKillable(inner, setup, [extension]);
          yield* settle(yield* opened.root.submit({ type: "input", content: "go" }));
          yield* eventually(() => state.asked.length === 1);
          yield* opened.kill; // `finish` cut off once
          const rerun = Deferred.makeUnsafe<void>();
          state.hookGate = rerun;
          opened = yield* openKillable(inner, setup, [extension]);
          yield* opened.harness.harness.resume;
          yield* eventually(() => state.asked.length === 2);
          // The host lowers the limit while the hook runs; the phase had started under the old one and ends
          // normally.
          setup.settings.restartLimit = 1;
          yield* Deferred.succeed(rerun, undefined);
          yield* idle(opened.root);
          assert.deepStrictEqual(state.runs, ["run 2"]);
          assert.deepStrictEqual(infos(setup, isInterrupted), []);
          yield* opened.harness.close;
        }),
      ),
  );

  it.effect("asks after a terminate round at the turn limit: terminate wins", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        const state = probe();
        const { extension } = install(state);
        setup.faux.setResponses([call("finish", "a")]);
        const { harness, root } = yield* openChat(yield* controlledStorage, setup, {
          extensions: [extension, tools],
        });
        yield* root.configure({ maxTurns: 1 });
        const done = yield* settle(yield* root.submit({ type: "input", content: "go" }));
        assert.strictEqual(done.status, "done");
        yield* idle(root);
        assert.deepStrictEqual(
          state.asked.map((summary) => summary.reason),
          ["terminate"],
        );
        assert.deepStrictEqual(state.runs, ["run 1"]);
        assert.isUndefined((yield* live(harness, root))?.runFailure);
        yield* harness.close;
      }),
    ),
  );

  it.effect(
    "starts a continue's history at its first tool round when nothing failed and nothing was placed",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const setup = chatSetup();
          const kinds: Array<Array<string>> = [];
          const reader = Extension.make({
            name: "reader",
            hooks: [
              Hook.generation({
                afterRun: (summary, api) =>
                  Effect.gen(function* () {
                    const range = {
                      minEntryId: summary.first,
                      maxEntryId: summary.last,
                      order: "ascending",
                    } as const;
                    kinds.push((yield* api.entries(range, 100)).items.map((entry) => entry.kind));
                    return undefined;
                  }).pipe(Effect.orDie),
              }),
            ],
          });
          setup.faux.setResponses([call("edit", "a"), fauxAssistantMessage("done")]);
          const { harness, root } = yield* openChat(yield* controlledStorage, setup, {
            extensions: [tools, reader],
          });
          // A host writes the user message itself, then continues.
          yield* root.submit({
            type: "write",
            entry: { kind: "app.prompt", model: [{ role: "user", content: "go", timestamp: 1 }] },
          });
          yield* settle(yield* root.submit({ type: "continue" }));
          yield* idle(root);
          assert.deepStrictEqual(kinds, [["core.assistant", "core.tool-result", "core.assistant"]]);
          yield* harness.close;
        }),
      ),
  );

  it.effect(
    "leaves a summary queued during the tasks stale behind a terminate round's handoff",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const setup = chatSetup();
          setup.settings.compaction = {
            enabled: false,
            reserveTokens: 1000,
            keepRecentTokens: 1,
            backgroundTokens: 0,
          };
          const state = probe();
          const { extension } = install(state);
          const gate = Deferred.makeUnsafe<void>();
          const started = Deferred.makeUnsafe<void>();
          const slow = Extension.make({
            name: "slow-handoff",
            tools: [
              slowTool("handoff", started, gate, {
                output: [{ type: "text", text: "handed off" }],
                control: { terminate: true, handoff: "fresh start" },
              }),
            ],
            hooks: [summaryHook],
          });
          setup.faux.setResponses([call("handoff", "a")]);
          const { harness, root } = yield* openChat(yield* controlledStorage, setup, {
            extensions: [extension, slow],
          });
          const submission = yield* root.submit({ type: "input", content: "go" });
          yield* finished(Deferred.await(started));
          const compaction = yield* root.compact();
          yield* finished(harness.harness.awaitTask(compaction));
          yield* Deferred.succeed(gate, undefined);
          yield* settle(submission);
          yield* idle(root);
          const entries = yield* allEntries(root);
          // The handoff's reset landed with the answer; the summary, cutting before it, settled stale at the run's
          // end.
          assert.deepStrictEqual(kindOf(entries, "core.compaction"), []);
          assert.deepStrictEqual(entries.map((entry) => entry.kind).slice(-2), [
            "core.reset",
            "test.tests",
          ]);
          assert.strictEqual(state.asked[0]?.reason, "terminate");
          assert.strictEqual(state.asked[0]?.last, kindOf(entries, "core.reset")[0]!.id);
          yield* harness.close;
        }),
      ),
  );

  it.effect(
    "stores the answer's held progress with commit A, and asks again after an orderly close in finish",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const inner = yield* AgentStorage;
          const setup = chatSetup({ tokensPerSecond: 200, tokenSize: { min: 1, max: 1 } });
          setup.settings.progress = { persistWithinMs: Number.POSITIVE_INFINITY };
          const state = probe();
          const Tests = testsTask(state);
          let hookGate: Deferred.Deferred<void> | undefined = Deferred.makeUnsafe<void>();
          const extension = Extension.make({
            name: "after-run",
            tasks: [Tests],
            hooks: [
              Hook.generation({
                afterRun: (summary) =>
                  Effect.gen(function* () {
                    state.asked.push(summary);
                    const gate = hookGate;
                    if (gate !== undefined) yield* Deferred.await(gate);
                    return { tasks: [Hook.hookTask(Tests, { label: "run" })] };
                  }),
              }),
            ],
          });
          setup.faux.setResponses([fauxAssistantMessage("x".repeat(40))]);
          let opened = yield* openKillable(inner, setup, [extension]);
          yield* settle(yield* opened.root.submit({ type: "input", content: "go" }));
          yield* eventually(() => state.asked.length === 1);
          yield* opened.kill;
          opened = yield* openKillable(inner, setup, [extension]);
          const stored = (yield* live(opened.harness, opened.root))!;
          assert.isUndefined(stored.generation);
          assert.deepStrictEqual(stored.run?.afterRun, {});
          assert.strictEqual(
            textOf(kindOf(yield* allEntries(opened.root), "core.assistant")[0]!.model![0]),
            "x".repeat(40),
          );
          yield* opened.harness.harness.resume;
          yield* eventually(() => state.asked.length === 2);
          yield* opened.harness.close;
          hookGate = undefined;
          opened = yield* openKillable(inner, setup, [extension]);
          yield* opened.harness.harness.resume;
          yield* idle(opened.root);
          assert.strictEqual(state.asked.length, 3);
          assert.deepStrictEqual(state.runs, ["run"]);
          yield* opened.harness.close;
        }),
      ),
  );
});

/** Runs `endRun` on a `core.live` value inside a commit. */
const ending = <E>(body: Effect.Effect<void, E, Tx>) =>
  Effect.gen(function* () {
    const { harness } = yield* openChat(yield* controlledStorage, chatSetup());
    const exit = yield* Effect.exit(harness.harness.commit(body));
    yield* harness.close;
    return exit;
  });

describeEachBackend("endRun", (backend) => {
  it.effect("leaves a run it does not own alone, and ends an answered run without a failure", () =>
    run(backend)(
      Effect.gen(function* () {
        const value: Tracker.Draft<LiveState> = {
          run: {
            taskId: TaskId.make(1),
            inputs: [],
            turns: 0,
            afterRun: { tasks: [TaskId.make(5)] },
          },
        };
        const exit = yield* ending(
          Effect.gen(function* () {
            yield* endRun(value, TaskId.make(2), { status: "unanswered", reason: "faulted" });
            assert.deepStrictEqual(value.run?.afterRun, { tasks: [TaskId.make(5)] });
            yield* endRun(value, TaskId.make(1), { status: "unanswered", reason: "aborted" });
          }),
        );
        assert.isTrue(Exit.isSuccess(exit));
        assert.isUndefined(value.run);
        assert.isUndefined(value.runFailure);
      }),
    ),
  );

  it.effect("copies a failed run's first entry into runFailure", () =>
    run(backend)(
      Effect.gen(function* () {
        const value: Tracker.Draft<LiveState> = {
          run: { taskId: TaskId.make(1), inputs: [], turns: 0, first: EntryId.make(7) },
        };
        const exit = yield* ending(
          endRun(value, TaskId.make(1), { status: "unanswered", reason: "model_error" }),
        );
        assert.isTrue(Exit.isSuccess(exit));
        assert.strictEqual(value.runFailure?.reason, "model_error");
        assert.strictEqual(value.runFailure?.first, 7);
      }),
    ),
  );
});

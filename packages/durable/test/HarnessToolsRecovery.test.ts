// Recovery of tool calls across a close, a crash or a reopen, on every backend.
//
// A reopen is a new Harness over the same storage of the backend; the registry is the static extensions of
// `openChat`, so a registration changed between runs is a reopen with other extensions. A call waiting on its abort
// signal is `Effect.never`, interrupted when the invocation is signalled. `waitFor` advances the `TestClock`
// (`ChatSupport.waitFor`), which also lets throttled progress commit.
//
// Not covered: a real bash command interrupted by close and reopen (no bash tool).
import { assert, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as AgentTool from "../src/AgentTool.ts";
import { ToolContext } from "../src/AgentTool.ts";
import { AgentStorage } from "../src/AgentStorage.ts";
import { ToolResultEntry } from "../src/Entry.ts";
import type { ExecutionEnvBuilder } from "../src/ExecutionEnv.ts";
import * as Extension from "../src/Extension.ts";
import type { Conversation } from "../src/Harness.ts";
import * as Hook from "../src/Hook.ts";
import type { SubmissionId, TaskId } from "../src/Ids.ts";
import { LiveDoc } from "../src/Live.ts";
import type {
  AssistantMessage,
  TextContent,
  ToolResultContent,
  ToolResultMessage,
} from "../src/Message.ts";
import type { EntryRecord } from "../src/Records.ts";
import {
  type FauxResponseStep,
  fauxAssistantMessage,
  fauxText,
  fauxToolCall,
} from "../src/testing/FauxModel.ts";
import { Tx } from "../src/Tx.ts";
import { allEntries, type ChatSetup, chatSetup, openChat, waitFor } from "./ChatSupport.ts";
import { bounded, killable, type TestHarness } from "./HarnessSupport.ts";
import {
  type ControlledStorage,
  controlledStorage,
  describeEachBackend,
  run,
} from "./SessionSupport.ts";

const NoParameters = Schema.Struct({});
type Execute<E, R> = (
  args: typeof NoParameters.Type,
) => Effect.Effect<AgentTool.ToolExecutionResult, E, R>;
type Extra = Omit<
  AgentTool.MakeOptions<typeof NoParameters.Type, Schema.Json, never, never>,
  "description" | "parameters" | "execute"
>;

/** A tool without parameters, described by its name. */
const tool = <E, R>(name: string, execute: Execute<E, R>, extra: Extra = {}) =>
  AgentTool.make(name, { description: name, parameters: NoParameters, execute, ...extra });

/** Installs `agentTool` as an extension of its own. */
const toolExtension = (agentTool: AgentTool.Any) =>
  Extension.make({ name: `tool:${agentTool.name}`, tools: [agentTool] });

/** Installs `hook` as an extension named `name`. */
const hookExtension = (name: string, hook: Hook.Any) => Extension.make({ name, hooks: [hook] });

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

/** A Harness over `storage` that schedules at once. */
const open = (
  storage: ControlledStorage,
  setup: ChatSetup,
  extensions: ReadonlyArray<Extension.Any>,
  env?: ExecutionEnvBuilder,
) =>
  Effect.gen(function* () {
    const opened = yield* openChat(storage, setup, { extensions, env });
    yield* opened.harness.harness.resume;
    return opened;
  });

/** An environment in the conversation's `cwd`, `/` without one. */
const env: ExecutionEnvBuilder = (target) => Effect.succeed({ cwd: target.cwd ?? "/" });

/** The first slot's task, once it has one. */
const toolTaskId = (harness: TestHarness, root: Conversation) =>
  Effect.gen(function* () {
    let id: TaskId | undefined;
    yield* waitFor(
      Effect.map(harness.session.snapshot(LiveDoc, root.id), (value) => {
        id = Option.getOrUndefined(value)?.tools?.[0]?.taskId;
        return id !== undefined;
      }),
    );
    return id!;
  });

/** Awaits a submission found by ID, while the `TestClock` advances. */
const settleById = (harness: TestHarness, id: SubmissionId) =>
  Effect.gen(function* () {
    const submission = Option.getOrThrow(yield* harness.harness.submission(id));
    const fiber = yield* Effect.forkChild(submission.await, { startImmediately: true });
    yield* waitFor(Effect.sync(() => fiber.pollUnsafe() !== undefined));
    return yield* Fiber.join(fiber);
  });

/**
 * A tool that writes output, then blocks until its invocation is interrupted the first `blocking` times it
 * runs. `started` completes once the output is durable.
 */
const blockingTool = (name: string, extra: Extra = {}) => {
  const started = Deferred.makeUnsafe<void>();
  const state = { runs: 0, blocking: 1 };
  const execute: Execute<never, ToolContext> = () =>
    Effect.gen(function* () {
      const api = yield* ToolContext;
      state.runs++;
      yield* api.output(`run ${state.runs}\n`);
      yield* api.details({ run: state.runs });
      if (state.runs <= state.blocking) {
        yield* Deferred.succeed(started, undefined);
        return yield* Effect.never;
      }
      return {};
    }).pipe(Effect.orDie);
  return { registration: tool(name, execute, extra), execute, started, state };
};

describeEachBackend("tool recovery", (backend) => {
  it.effect("answers an unsafe tool interrupted after intent with its durable partial output", () =>
    run(backend)(
      Effect.gen(function* () {
        const storage = yield* controlledStorage;
        const setup = chatSetup();
        const { registration, started, state } = blockingTool("work");
        const extensions = [toolExtension(registration)];
        setup.faux.setResponses([call("work"), DONE]);
        let opened = yield* open(storage, setup, extensions);
        const id = (yield* opened.root.submit({ type: "input", content: "go" })).id;
        yield* waitFor(Deferred.isDone(started));
        const taskId = yield* toolTaskId(opened.harness, opened.root);
        yield* opened.harness.close;

        opened = yield* open(storage, setup, extensions);
        const task = Option.getOrThrow(yield* opened.harness.harness.getTask(taskId));
        assert.deepStrictEqual(task.state.status === "pending" && task.state.checkpoint, {
          phase: "execute",
          arguments: {},
          replay: "unsafe",
        });
        assert.strictEqual((yield* settleById(opened.harness, id)).status, "done");
        assert.strictEqual(state.runs, 1);
        const [result] = results(yield* allEntries(opened.root));
        assert.strictEqual(result?.isError, true);
        assert.deepStrictEqual(result?.details, { run: 1 });
        assert.strictEqual(
          text(result),
          "run 1\n|<harness>\n[error] Tool work was interrupted and may have partially run\n</harness>",
        );
        assert.deepStrictEqual(
          Option.getOrUndefined(yield* opened.harness.session.snapshot(LiveDoc, opened.root.id)),
          {},
        );
        yield* opened.harness.close;
      }),
    ),
  );

  // The crashed Harness's storage is killed (`HarnessSupport.killable`), so it writes nothing more, as a dead
  // process would not, while another Harness opens the same storage.
  it.effect(
    "reruns a safe tool whose running slot a crash after its intent lost, or a sibling stored, and shows it running",
    () =>
      Effect.gen(function* () {
        for (const sibling of [false, true]) {
          yield* run(backend)(
            Effect.gen(function* () {
              const inner = yield* AgentStorage;
              const setup = chatSetup();
              // The running slot is presentation, held in memory; no deadline stores it before the crash.
              setup.settings.progress = { persistWithinMs: Number.POSITIVE_INFINITY };
              const started = Deferred.makeUnsafe<void>();
              let runs = 0;
              let reopened: TestHarness | undefined;
              let root: Conversation | undefined;
              const seen: Array<string | undefined> = [];
              const work = tool(
                "work",
                () =>
                  Effect.gen(function* () {
                    const api = yield* ToolContext;
                    runs++;
                    if (runs === 1) {
                      yield* Deferred.succeed(started, undefined);
                      return yield* Effect.never;
                    }
                    const slots =
                      Option.getOrUndefined(yield* reopened!.session.snapshot(LiveDoc, root!.id))
                        ?.tools ?? [];
                    seen.push(slots.find((slot) => slot.taskId === api.taskId)?.status);
                    return {};
                  }).pipe(Effect.orDie),
                { replay: "safe" },
              );
              // A sibling that ends once `work` started: its result commit reads `core.live`, which stores what is
              // held there.
              const quick = tool("quick", () => Deferred.await(started).pipe(Effect.as({})));
              const extensions = [toolExtension(work), toolExtension(quick)];
              const calls = [fauxToolCall("work", {}, { id: "w" })];
              if (sibling) calls.push(fauxToolCall("quick", {}, { id: "q" }));
              setup.faux.setResponses([
                fauxAssistantMessage(calls, { stopReason: "toolUse" }),
                DONE,
              ]);
              const crashable = yield* killable(inner);
              const crashed = yield* open(crashable.storage, setup, extensions);
              root = crashed.root;
              const id = (yield* crashed.root.submit({ type: "input", content: "go" })).id;
              yield* waitFor(Deferred.isDone(started));
              const slot = (harness: TestHarness, name: string) =>
                Effect.map(harness.session.snapshot(LiveDoc, crashed.root.id), (value) =>
                  Option.getOrUndefined(value)?.tools?.find((each) => each.name === name),
                );
              const workId = (yield* slot(crashed.harness, "work"))!.taskId!;
              if (sibling) {
                yield* waitFor(
                  Effect.map(slot(crashed.harness, "quick"), (found) => found?.status === "done"),
                );
              }
              // The process dies here: another one opens the same storage, without the held presentation.
              yield* crashable.kill;
              yield* crashed.harness.close;
              const next = yield* openChat(yield* controlledStorage, setup, { extensions });
              reopened = next.harness;
              const task = Option.getOrThrow(yield* next.harness.harness.getTask(workId));
              assert.deepInclude(
                task.state.status === "pending" ? task.state.checkpoint : undefined,
                {
                  phase: "execute",
                },
              );
              assert.strictEqual(
                (yield* slot(next.harness, "work"))?.status,
                sibling ? "running" : "pending",
              );
              yield* next.harness.harness.resume;
              assert.strictEqual((yield* settleById(next.harness, id)).status, "done");
              // The rerun shows the slot running again.
              assert.deepStrictEqual(seen, ["running"]);
              const result = results(yield* allEntries(next.root)).find(
                (message) => message.toolCallId === "w",
              );
              assert.isFalse(result!.isError);
              yield* next.harness.close;
            }),
          );
        }
      }),
  );

  // The reopened Harness's extension holds the tool with the current policy. Each case opens a fresh storage.
  it.effect("reruns a tool only when both the stored and the current replay policy are safe", () =>
    Effect.gen(function* () {
      const cases: ReadonlyArray<
        readonly [stored: "safe" | "unsafe", current: "safe" | "unsafe", reruns: boolean]
      > = [
        ["safe", "safe", true],
        ["safe", "unsafe", false],
        ["unsafe", "safe", false],
      ];
      for (const [stored, current, reruns] of cases) {
        yield* run(backend)(
          Effect.gen(function* () {
            const storage = yield* controlledStorage;
            const setup = chatSetup();
            const blocking = blockingTool("work", { replay: stored });
            setup.faux.setResponses([call("work"), DONE]);
            let opened = yield* open(storage, setup, [toolExtension(blocking.registration)]);
            const id = (yield* opened.root.submit({ type: "input", content: "go" })).id;
            yield* waitFor(Deferred.isDone(blocking.started));
            yield* opened.harness.close;

            const replaced = tool("work", blocking.execute, { replay: current });
            opened = yield* open(storage, setup, [toolExtension(replaced)]);
            assert.strictEqual((yield* settleById(opened.harness, id)).status, "done");
            const [result] = results(yield* allEntries(opened.root));
            assert.strictEqual(blocking.state.runs, reruns ? 2 : 1);
            assert.strictEqual(result!.isError, !reruns);
            if (reruns) assert.strictEqual(text(result), "run 2\n");
            yield* opened.harness.close;
          }),
        );
      }
    }),
  );

  it.effect(
    "reruns a safe tool with the environment of the conversation's cwd at rerun, and not once it is deselected",
    () =>
      // Each half on a fresh storage of the backend.
      Effect.forEach(["cwd", "deselect"] as const, (change) =>
        run(backend)(
          Effect.gen(function* () {
            const storage = yield* controlledStorage;
            const setup = chatSetup();
            const cwds: Array<string | undefined> = [];
            const started = yield* Deferred.make<void>();
            const work = tool(
              "work",
              () =>
                Effect.gen(function* () {
                  const api = yield* ToolContext;
                  cwds.push(api.env?.cwd);
                  if (cwds.length === 1) {
                    yield* Deferred.succeed(started, undefined);
                    return yield* Effect.never;
                  }
                  return {};
                }),
              { replay: "safe" },
            );
            const extensions = [toolExtension(work)];
            setup.faux.setResponses([call("work"), DONE]);
            let opened = yield* open(storage, setup, extensions, env);
            yield* opened.root.configure({ cwd: "/one" });
            const id = (yield* opened.root.submit({ type: "input", content: "go" })).id;
            yield* waitFor(Deferred.isDone(started));
            yield* opened.root.configure(change === "cwd" ? { cwd: "/two" } : { extensions: [] });
            yield* opened.harness.close;

            opened = yield* open(storage, setup, extensions, env);
            assert.strictEqual((yield* settleById(opened.harness, id)).status, "done");
            const [result] = results(yield* allEntries(opened.root));
            if (change === "cwd") {
              assert.deepStrictEqual(cwds, ["/one", "/two"]);
              assert.strictEqual(result!.isError, false);
            } else {
              // A tool that no longer resolves is treated as unsafe: interrupted, not rerun.
              assert.deepStrictEqual(cwds, ["/one"]);
              assert.include(text(result), "was interrupted");
            }
            yield* opened.harness.close;
          }),
        ),
      ),
  );

  it.effect("reruns beforeTool when interrupted before intent, and executes once", () =>
    run(backend)(
      Effect.gen(function* () {
        const storage = yield* controlledStorage;
        const setup = chatSetup();
        let runs = 0;
        const work = tool("work", () =>
          Effect.sync(() => {
            runs++;
            return { output: [] };
          }),
        );
        const reached = yield* Deferred.make<void>();
        let asked = 0;
        const decisions: Array<string> = [];
        const hooks = Hook.tool({
          beforeTool: (_call, context) =>
            Effect.gen(function* () {
              asked++;
              // A durable first-writer-wins decision survives the rerun.
              decisions.push(yield* context.memo("test:decision", `attempt ${asked}`));
              if (asked === 1) {
                yield* Deferred.succeed(reached, undefined);
                return yield* Effect.never;
              }
              return undefined;
            }).pipe(Effect.orDie),
        });
        const extensions = [toolExtension(work), hookExtension("hooks", hooks)];
        setup.faux.setResponses([call("work"), DONE]);
        let opened = yield* open(storage, setup, extensions);
        const id = (yield* opened.root.submit({ type: "input", content: "go" })).id;
        yield* waitFor(Deferred.isDone(reached));
        yield* opened.harness.close;

        opened = yield* open(storage, setup, extensions);
        assert.strictEqual((yield* settleById(opened.harness, id)).status, "done");
        assert.deepStrictEqual([asked, runs], [2, 1]);
        assert.deepStrictEqual(decisions, ["attempt 1", "attempt 1"]);
        yield* opened.harness.close;
      }),
    ),
  );

  it.effect("reruns the generation tools phase interrupted before its commit", () =>
    run(backend)(
      Effect.gen(function* () {
        const storage = yield* controlledStorage;
        const setup = chatSetup();
        const work = tool("work", () => Effect.succeed({ output: [] }));
        const reached = yield* Deferred.make<void>();
        let observed = 0;
        const hooks = Hook.generation({
          afterTools: () =>
            Effect.gen(function* () {
              observed++;
              if (observed === 1) {
                yield* Deferred.succeed(reached, undefined);
                return yield* Effect.never;
              }
            }),
        });
        const extensions = [toolExtension(work), hookExtension("hooks", hooks)];
        setup.faux.setResponses([call("work"), DONE]);
        let opened = yield* open(storage, setup, extensions);
        const id = (yield* opened.root.submit({ type: "input", content: "go" })).id;
        yield* waitFor(Deferred.isDone(reached));
        yield* opened.harness.close;

        opened = yield* open(storage, setup, extensions);
        assert.strictEqual((yield* settleById(opened.harness, id)).status, "done");
        assert.strictEqual(observed, 2);
        assert.deepStrictEqual(
          (yield* allEntries(opened.root)).map((entry) => entry.kind),
          ["core.user", "core.system", "core.assistant", "core.tool-result", "core.assistant"],
        );
        yield* opened.harness.close;
      }),
    ),
  );

  it.effect("answers an aborted tool with its partial output and continues the run", () =>
    run(backend)(
      Effect.gen(function* () {
        const storage = yield* controlledStorage;
        const setup = chatSetup();
        const { registration, started } = blockingTool("work");
        setup.faux.setResponses([call("work"), DONE]);
        const opened = yield* open(storage, setup, [toolExtension(registration)]);
        const submission = yield* opened.root.submit({ type: "input", content: "go" });
        yield* waitFor(Deferred.isDone(started));
        const taskId = yield* toolTaskId(opened.harness, opened.root);
        assert.deepStrictEqual(yield* opened.harness.harness.abortTask(taskId), { _tag: "Marked" });
        const outcome = (yield* bounded(opened.harness.harness.awaitTask(taskId))).state.outcome;
        assert.strictEqual(outcome.status, "aborted");
        const entryId =
          outcome.status === "aborted" &&
          Schema.is(Schema.Struct({ entryId: Schema.Number }))(outcome.result)
            ? outcome.result.entryId
            : undefined;
        assert.isNumber(entryId);
        assert.strictEqual((yield* settleById(opened.harness, submission.id)).status, "done");
        const [result] = results(yield* allEntries(opened.root));
        assert.strictEqual(
          text(result),
          "run 1\n|<harness>\n[error] Tool work was aborted\n</harness>",
        );
        yield* opened.harness.close;
      }),
    ),
  );

  // The result carries a content block that is JSON but no valid model content, which the result commit rejects
  // (`InvalidJson`), and the scheduler faults the task.
  it.effect("lets context derivation answer a faulted tool and continues the run", () =>
    run(backend)(
      Effect.gen(function* () {
        const storage = yield* controlledStorage;
        const setup = chatSetup();
        // A JSON block that is no valid model content, the fault this case injects: a text block without its text.
        const block: TextContent = { type: "text", text: "" };
        Reflect.deleteProperty(block, "text");
        const broken: ToolResultContent = [block];
        const bad = tool("bad", () => Effect.succeed({ output: broken }));
        const requests: Array<string> = [];
        const answer: FauxResponseStep = (request) =>
          Effect.sync(() => {
            const result = request.messages.find((message) => message.role === "toolResult");
            requests.push(result?.role === "toolResult" ? text(result) : "none");
            return DONE;
          });
        setup.faux.setResponses([call("bad"), answer]);
        const opened = yield* open(storage, setup, [toolExtension(bad)]);
        const submission = yield* opened.root.submit({ type: "input", content: "go" });
        const settled = yield* settleById(opened.harness, submission.id);
        assert.strictEqual(settled.status, "done");
        assert.deepStrictEqual(results(yield* allEntries(opened.root)), []);
        assert.deepStrictEqual(requests, [
          "Tool result unavailable: history ends before this call completed.",
        ]);
        const tasks = yield* opened.harness.harness.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            return (yield* tx.scanTasks({ conversationId: opened.root.id }, 20)).items;
          }),
        );
        const faulted = tasks.find((task) => task.kind === "core.tool")!;
        assert.strictEqual(faulted.state.status, "terminal");
        assert.strictEqual(
          faulted.state.status === "terminal" ? faulted.state.outcome.status : undefined,
          "faulted",
        );
        yield* opened.harness.close;
      }),
    ),
  );

  it.effect("clears the interrupted attempt's progress before a safe rerun", () =>
    run(backend)(
      Effect.gen(function* () {
        const storage = yield* controlledStorage;
        const setup = chatSetup();
        const started = [Deferred.makeUnsafe<void>(), Deferred.makeUnsafe<void>()] as const;
        let runs = 0;
        const work = tool(
          "work",
          () =>
            Effect.gen(function* () {
              const api = yield* ToolContext;
              const attempt = runs++;
              if (attempt === 0) {
                yield* api.diagnostic({ severity: "info", message: "first a" });
                yield* api.diagnostic({ severity: "info", message: "first b" });
                yield* api.details({ run: 1, extra: true });
              } else {
                yield* api.diagnostic({ severity: "info", message: "second" });
                yield* api.details({ run: 2 });
              }
              yield* Deferred.succeed(started[attempt === 0 ? 0 : 1], undefined);
              return yield* Effect.never;
            }).pipe(Effect.orDie),
          { replay: "safe" },
        );
        const extensions = [toolExtension(work)];
        setup.faux.setResponses([call("work"), DONE]);
        let opened = yield* open(storage, setup, extensions);
        yield* opened.root.submit({ type: "input", content: "go" });
        yield* waitFor(Deferred.isDone(started[0]));
        yield* opened.harness.close;

        opened = yield* open(storage, setup, extensions);
        yield* waitFor(Deferred.isDone(started[1]));
        const slot = Option.getOrUndefined(
          yield* opened.harness.session.snapshot(LiveDoc, opened.root.id),
        )?.tools?.[0];
        assert.deepStrictEqual(slot?.diagnostics, [{ severity: "info", message: "second" }]);
        assert.deepStrictEqual(slot?.details, { run: 2 });
        yield* opened.harness.close;
      }),
    ),
  );

  // `execute` takes what `parameters` decodes, so a rerun decodes the stored arguments with the current schema.
  it.effect(
    "ends a safe rerun invalid_arguments when the current parameters reject the stored arguments",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const storage = yield* controlledStorage;
          const setup = chatSetup();
          const started = yield* Deferred.make<void>();
          const received: Array<unknown> = [];
          const make = <A>(parameters: Schema.Decoder<A>) =>
            AgentTool.make("work", {
              description: "work",
              parameters,
              replay: "safe",
              execute: (args) =>
                Effect.gen(function* () {
                  received.push(args);
                  yield* Deferred.succeed(started, undefined);
                  return yield* Effect.never;
                }),
            });
          setup.faux.setResponses([
            fauxAssistantMessage([fauxToolCall("work", { count: 1 }, { id: "c1" })], {
              stopReason: "toolUse",
            }),
            DONE,
          ]);
          const wide = make(Schema.Struct({ count: Schema.Number }));
          let opened = yield* open(storage, setup, [toolExtension(wide)]);
          const id = (yield* opened.root.submit({ type: "input", content: "go" })).id;
          yield* waitFor(Deferred.isDone(started));
          yield* opened.harness.close;

          // A new required key: no coercion of the stored arguments can supply it.
          const narrowed = make(Schema.Struct({ count: Schema.Number, total: Schema.Number }));
          opened = yield* open(storage, setup, [toolExtension(narrowed)]);
          assert.strictEqual((yield* settleById(opened.harness, id)).status, "done");
          assert.deepStrictEqual(received, [{ count: 1 }]);
          const [result] = results(yield* allEntries(opened.root));
          assert.isTrue(result!.isError);
          assert.include(text(result), 'Validation failed for tool "work"');
          yield* opened.harness.close;
        }),
      ),
  );
});

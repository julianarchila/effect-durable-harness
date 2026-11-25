// Per-conversation turn limits (`AgentState.maxTurns`) and continuing a run that reached one, on every backend.
//
// `openChat` opens a Harness over a `ControlledStorage`; a call waiting on its abort signal is `Effect.never`,
// interrupted with the call; the model is `FauxModel`; tools and hooks are extensions given to `openChat`, in
// registration order. Waits let the `TestClock` advance in 5 ms steps (`waitFor`).
//
// Notes on some cases:
// - "rejects a limit that is not an integer 1 or more": an invalid limit is a `RangeError` defect of `configure`.
// - "leaves items queued during the last round to afterFailedRun": each policy opens a fresh storage of the
//   backend.
// - "keeps the stored count across a restart": a restart is a closed Harness reopened over the same storage, so it
//   runs on every backend; closing interrupts the call as a dead host leaves it.
import { assert, it } from "@effect/vitest";
import * as Cause from "effect/Cause";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as AgentTool from "../src/AgentTool.ts";
import { ResetEntry, ToolResultEntry } from "../src/Entry.ts";
import * as Extension from "../src/Extension.ts";
import type { Conversation } from "../src/Harness.ts";
import * as Hook from "../src/Hook.ts";
import type { ConversationId, SubmissionId } from "../src/Ids.ts";
import { LiveDoc } from "../src/Live.ts";
import type { AssistantMessage } from "../src/Message.ts";
import type { AfterFailedRun } from "../src/Settings.ts";
import type { Submission } from "../src/Submission.ts";
import {
  type FauxResponseStep,
  fauxAssistantMessage,
  fauxText,
  fauxToolCall,
} from "../src/testing/FauxModel.ts";
import { Tx } from "../src/Tx.ts";
import { allEntries, type ChatSetup, chatSetup, openChat, textOf, waitFor } from "./ChatSupport.ts";
import type { TestHarness } from "./HarnessSupport.ts";
import {
  type Backend,
  type ControlledStorage,
  controlledStorage,
  describeEachBackend,
  run,
} from "./SessionSupport.ts";

const Parameters = Schema.Struct({});

/** A tool with no parameters. */
const tool = <E>(
  name: string,
  execute: (call: AgentTool.Call) => Effect.Effect<AgentTool.ToolExecutionResult, E>,
  options: { readonly replay?: AgentTool.ReplayPolicy } = {},
) =>
  AgentTool.make(name, {
    description: name,
    parameters: Parameters,
    execute: (_args, call) => execute(call),
    ...options,
  });

/** An extension holding one tool. */
const toolExtension = (agentTool: AgentTool.Any) =>
  Extension.make({ name: `tool:${agentTool.name}`, tools: [agentTool] });

let hookExtensions = 0;
/** An extension holding one hook registration. */
const hookExtension = (hook: Hook.Any) =>
  Extension.make({ name: `hooks:${++hookExtensions}`, hooks: [hook] });

/** A tool-calling answer with one `noop` call. */
const round = (id: string): AssistantMessage =>
  fauxAssistantMessage([fauxToolCall("noop", {}, { id })], { stopReason: "toolUse" });

const DONE = fauxAssistantMessage([fauxText("done")]);

const noop = tool("noop", () => Effect.succeed({ output: [] }));

/** A chat with a `noop` tool and the given turn limit; `extensions` follow the tool. */
const limited = (
  storage: ControlledStorage,
  maxTurns: number | undefined,
  extensions: ReadonlyArray<Extension.Any> = [],
) =>
  Effect.gen(function* () {
    const setup = chatSetup();
    const opened = yield* openChat(storage, setup, {
      extensions: [toolExtension(noop), ...extensions],
    });
    if (maxTurns !== undefined) yield* opened.root.configure({ maxTurns });
    return { setup, ...opened };
  });

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

/** Runs `effect` and returns its result once it settles, letting the `TestClock` advance meanwhile. */
const finished = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  Effect.gen(function* () {
    const fiber = yield* Effect.forkChild(effect, { startImmediately: true });
    yield* waitFor(Effect.sync(() => fiber.pollUnsafe() !== undefined));
    return yield* Fiber.join(fiber);
  });

/** Awaits a submission while the `TestClock` advances. */
const settle = (submission: Submission) => finished(submission.await);

const ask = (conversation: Conversation, content: string) =>
  conversation.submit({ type: "input", content }).pipe(Effect.flatMap(settle));

const reached = (deferred: Deferred.Deferred<void>) => waitFor(Deferred.isDone(deferred));

const live = (harness: TestHarness, conversation: Conversation) =>
  harness.session.snapshot(LiveDoc, conversation.id).pipe(Effect.map(Option.getOrUndefined));

const lastKind = (root: Conversation) =>
  allEntries(root).pipe(Effect.map((entries) => entries.at(-1)?.kind));

/** Asserts `record` is unanswered with `reason`, and `detail` when given. */
const assertUnanswered = (
  record: { readonly status: string; readonly reason?: string; readonly detail?: unknown },
  reason: string,
  detail?: string,
) => {
  assert.strictEqual(record.status, "unanswered");
  assert.strictEqual(record.reason, reason);
  if (detail !== undefined) assert.strictEqual(record.detail, detail);
};

describeEachBackend("turn limit", (backend) => {
  it.effect("ends a run after the tool round that reaches the limit, like a failure", () =>
    run(backend)(
      Effect.gen(function* () {
        const { setup, harness, root } = yield* limited(yield* controlledStorage, 1);
        setup.faux.setResponses([round("c1"), DONE]);
        const submission = yield* root.submit({ type: "input", content: "go" });
        assertUnanswered(yield* settle(submission), "turn_limit", "1 turn");
        assert.strictEqual(setup.faux.state.callCount, 1);
        // The transcript ends with the round's results; the generation completed, so nothing is reported.
        assert.strictEqual(yield* lastKind(root), ToolResultEntry.kind);
        const runFailure = (yield* live(harness, root))?.runFailure;
        assert.strictEqual(runFailure?.reason, "turn_limit");
        assert.deepStrictEqual(runFailure?.inputs, [submission.id]);
        const generations = yield* harness.harness.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            return yield* tx.scanTasks({ conversationId: root.id }, 20);
          }),
        );
        const generation = generations.items.find((task) => task.kind === "core.generation");
        assert.strictEqual(generation?.state.status, "terminal");
        assert.strictEqual(
          generation?.state.status === "terminal" ? generation.state.outcome.status : undefined,
          "completed",
        );
        assert.deepStrictEqual(setup.reports, []);
        yield* harness.close;
      }),
    ),
  );

  it.effect(
    "counts tool rounds only: a run answers below its limit, and an answer at the limit settles done",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const { setup, harness, root } = yield* limited(yield* controlledStorage, 2);
          setup.faux.setResponses([round("c1"), DONE, round("c2"), round("c3"), DONE]);
          assert.strictEqual((yield* ask(root, "one")).status, "done");
          // A new run starts with a fresh budget.
          assertUnanswered(yield* ask(root, "two"), "turn_limit", "2 turns");
          yield* harness.close;
        }),
      ),
  );

  it.effect("runs onYield at the limit, and a continuation starts the count over", () =>
    run(backend)(
      Effect.gen(function* () {
        let yields = 0;
        const { setup, harness, root } = yield* limited(yield* controlledStorage, 2, [
          hookExtension(
            Hook.generation({
              onYield: () =>
                Effect.sync(() => (yields++ === 0 ? { continue: "keep going" } : undefined)),
            }),
          ),
        ]);
        // Round (1 turn), answer: the continuation resets the count; then two rounds reach the limit.
        setup.faux.setResponses([round("c1"), DONE, round("c2"), round("c3"), DONE]);
        assertUnanswered(yield* ask(root, "go"), "turn_limit");
        assert.strictEqual(yields, 1);
        assert.strictEqual(setup.faux.state.callCount, 4);
        yield* harness.close;
      }),
    ),
  );

  it.effect("settles a steer that joined the run at an earlier round with turn_limit too", () =>
    run(backend)(
      Effect.gen(function* () {
        const gate = Deferred.makeUnsafe<void>();
        const started = Deferred.makeUnsafe<void>();
        const setup = chatSetup();
        let calls = 0;
        const held = tool("noop", () =>
          Effect.suspend(() =>
            calls++ === 0
              ? Deferred.succeed(started, undefined).pipe(Effect.andThen(Deferred.await(gate)))
              : Effect.void,
          ).pipe(Effect.as({ output: [] })),
        );
        const { harness, root } = yield* openChat(yield* controlledStorage, setup, {
          extensions: [toolExtension(held)],
        });
        yield* root.configure({ maxTurns: 2 });
        setup.faux.setResponses([round("c1"), round("c2"), DONE]);
        const first = yield* root.submit({ type: "input", content: "go" });
        yield* reached(started);
        const steer = yield* root.submit({
          type: "input",
          content: "also this",
          whenBusy: "steer",
        });
        yield* Deferred.succeed(gate, undefined);
        for (const submission of [first, steer]) {
          assertUnanswered(yield* settle(submission), "turn_limit", "2 turns");
        }
        assert.deepStrictEqual((yield* live(harness, root))?.runFailure?.inputs, [
          first.id,
          steer.id,
        ]);
        assert.strictEqual(setup.faux.state.callCount, 2);
        yield* harness.close;
      }),
    ),
  );

  it.effect("leaves items queued during the last round to afterFailedRun", () =>
    run(backend)(
      Effect.gen(function* () {
        for (const policy of [
          "keep",
          "continue",
          "withdraw",
        ] as const satisfies ReadonlyArray<AfterFailedRun>) {
          const gate = Deferred.makeUnsafe<void>();
          const started = Deferred.makeUnsafe<void>();
          const setup = chatSetup();
          setup.settings.afterFailedRun = policy;
          const held = tool("noop", () =>
            Deferred.succeed(started, undefined).pipe(
              Effect.andThen(Deferred.await(gate)),
              Effect.as({ output: [] }),
            ),
          );
          setup.faux.setResponses([round("c1"), DONE]);
          const { harness, root } = yield* openChat(yield* freshStorage(backend), setup, {
            extensions: [toolExtension(held)],
          });
          yield* root.configure({ maxTurns: 1 });
          const first = yield* root.submit({ type: "input", content: "go" });
          yield* reached(started);
          const steer = yield* root.submit({
            type: "input",
            content: "also",
            whenBusy: "followUp",
          });
          yield* Deferred.succeed(gate, undefined);
          assert.strictEqual((yield* settle(first)).status, "unanswered");
          yield* finished(harness.harness.awaitIdle);
          const queued = yield* steer.status;
          if (policy === "keep") assert.strictEqual(queued.status, "queued", policy);
          if (policy === "continue") assert.strictEqual(queued.status, "done", policy);
          if (policy === "withdraw") assertUnanswered(queued, "run_failed");
          yield* harness.close;
        }
      }),
    ),
  );

  it.effect("applies a changed limit at the end of the round; null removes it", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        let root!: Conversation;
        let rounds = 0;
        const lowering = tool("noop", () =>
          Effect.gen(function* () {
            rounds++;
            // Lowered during the second round: that round ends the run.
            if (rounds === 2) yield* Effect.orDie(root.configure({ maxTurns: 1 }));
            return { output: [] };
          }),
        );
        setup.faux.setResponses([round("c1"), round("c2"), round("c3"), DONE]);
        const opened = yield* openChat(yield* controlledStorage, setup, {
          extensions: [toolExtension(lowering)],
        });
        root = opened.root;
        yield* root.configure({ maxTurns: 5 });
        assertUnanswered(yield* ask(root, "go"), "turn_limit", "2 turns");
        yield* root.configure({ maxTurns: null });
        assert.isUndefined((yield* root.agent).maxTurns);
        setup.faux.setResponses([round("c4"), round("c5"), DONE]);
        assert.strictEqual((yield* ask(root, "again")).status, "done");
        yield* opened.harness.close;
      }),
    ),
  );

  // `configure` dies with a `RangeError` (a defect, `internal/agent.ts`).
  it.effect("rejects a limit that is not an integer 1 or more", () =>
    run(backend)(
      Effect.gen(function* () {
        const { harness, root } = yield* limited(yield* controlledStorage, undefined);
        for (const value of [0, -1, 1.5, Number.NaN, Number.POSITIVE_INFINITY]) {
          const exit = yield* Effect.exit(root.configure({ maxTurns: value }));
          assert.isTrue(Exit.isFailure(exit), String(value));
          if (Exit.isFailure(exit)) {
            assert.instanceOf(Cause.squash(exit.cause), RangeError, String(value));
          }
        }
        yield* root.configure({ maxTurns: 3 });
        assert.strictEqual((yield* root.agent).maxTurns, 3);
        yield* harness.close;
      }),
    ),
  );

  it.effect(
    "lets terminate settle done at the limit, and writes a handoff's reset before ending",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const { setup, harness, root } = yield* limited(yield* controlledStorage, 1, [
            hookExtension(
              Hook.tool({
                afterTool: (call, result) =>
                  Effect.succeed(
                    call.id === "t"
                      ? { ...result, control: { terminate: true as const } }
                      : { ...result, control: { handoff: "fresh" } },
                  ),
              }),
            ),
          ]);
          setup.faux.setResponses([round("t"), round("h"), DONE]);
          assert.strictEqual((yield* ask(root, "one")).status, "done");
          assertUnanswered(yield* ask(root, "two"), "turn_limit");
          const resets = (yield* allEntries(root)).filter((entry) => ResetEntry.is(entry));
          assert.deepStrictEqual(
            resets.map((entry) => textOf(entry.model?.[0])),
            ["fresh"],
          );
          yield* harness.close;
        }),
      ),
  );

  it.effect("is resumed by a continue, with a fresh budget, on the round's results", () =>
    run(backend)(
      Effect.gen(function* () {
        const { setup, harness, root } = yield* limited(yield* controlledStorage, 1);
        let continued: string | undefined;
        const finishing: FauxResponseStep = (request) =>
          Effect.sync(() => {
            continued = request.messages.at(-1)?.role;
            return fauxAssistantMessage([fauxText("finished")]);
          });
        setup.faux.setResponses([round("c1"), finishing]);
        yield* ask(root, "go");
        const resumed = yield* root.submit({ type: "continue" });
        assert.strictEqual((yield* live(harness, root))?.run?.turns, 0);
        const settled = yield* settle(resumed);
        if (settled.status !== "done" || settled.type === "write") {
          throw new Error(`Unexpected ${settled.status}`);
        }
        const answer = (yield* allEntries(root)).find((entry) => entry.id === settled.answer);
        assert.strictEqual(textOf(answer?.model?.[0]), "finished");
        assert.strictEqual(continued, "toolResult");
        yield* harness.close;
      }),
    ),
  );

  it.effect("is inherited by forks and children, which may set their own", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        let child: ConversationId | undefined;
        const spawn = tool("spawn", (api) =>
          Effect.gen(function* () {
            child = yield* api.commit(
              Effect.gen(function* () {
                const tx = yield* Tx;
                return (yield* tx.createConversation({
                  ownership: { kind: "task", taskId: api.taskId },
                })).id;
              }),
            );
            return { output: [] };
          }).pipe(Effect.orDie),
        );
        setup.faux.setResponses([
          fauxAssistantMessage([fauxToolCall("spawn", {}, { id: "c1" })], {
            stopReason: "toolUse",
          }),
          DONE,
        ]);
        const { harness, root } = yield* openChat(yield* controlledStorage, setup, {
          extensions: [toolExtension(spawn)],
        });
        yield* root.configure({ maxTurns: 4 });
        yield* ask(root, "go");
        const spawned = Option.getOrThrow(yield* harness.harness.conversation(child!));
        assert.strictEqual((yield* spawned.agent).maxTurns, 4);
        const at = (yield* allEntries(root)).at(-1)!.id;
        const fork = yield* root.fork(at, { ownership: { kind: "ownerless" } });
        assert.strictEqual((yield* fork.agent).maxTurns, 4);
        const own = yield* harness.harness.createConversation({
          ownership: { kind: "ownerless" },
          agent: { maxTurns: 2 },
        });
        assert.strictEqual((yield* own.agent).maxTurns, 2);
        yield* harness.close;
      }),
    ),
  );

  // A restart is a closed Harness reopened over the same storage (see the header).
  it.effect("keeps the stored count across a restart", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup: ChatSetup = chatSetup();
        setup.settings.progress = { outputIntervalMs: 0, persistWithinMs: 0 };
        const started = Deferred.makeUnsafe<void>();
        let runs = 0;
        const replayed = tool(
          "noop",
          () =>
            Effect.suspend(() => {
              runs++;
              // Never answers; closing the Harness interrupts it.
              if (runs === 2) {
                return Deferred.succeed(started, undefined).pipe(Effect.andThen(Effect.never));
              }
              return Effect.succeed({ output: [] });
            }),
          { replay: "safe" },
        );
        const extensions = [toolExtension(replayed)];
        setup.faux.setResponses([round("c1"), round("c2"), DONE]);
        const storage = yield* controlledStorage;
        const first = yield* openChat(storage, setup, { extensions });
        yield* first.root.configure({ maxTurns: 2 });
        const id: SubmissionId = (yield* first.root.submit({ type: "input", content: "go" })).id;
        yield* reached(started);
        assert.strictEqual((yield* live(first.harness, first.root))?.run?.turns, 2);
        yield* first.harness.close;

        // The replay-safe call runs again after reopen; the stored count still ends the run at this round.
        runs = 0;
        const second = yield* openChat(storage, setup, { extensions });
        yield* second.harness.harness.resume;
        assert.strictEqual((yield* live(second.harness, second.root))?.run?.turns, 2);
        const submission = Option.getOrThrow(yield* second.harness.harness.submission(id));
        assertUnanswered(yield* settle(submission), "turn_limit", "2 turns");
        yield* second.harness.close;
      }),
    ),
  );
});

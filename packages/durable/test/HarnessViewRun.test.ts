// What a renderer sees of a run through the conversation view, on every backend: the transcript window and `core.live`,
// every change a commit makes, rebuilt from operations alone.
//
// Notes:
// - A watch is consumed by a fiber over its `frames` stream; stopping it closes the acquisition's scope (and
//   interrupts the consumer).
// - `viewHistory` observes through the internal `views.attach` under `Conversation.observe`, whose observer is
//   made from the window's first value.
// - Committed values come from the kernel's `followCommits` (the Session's own listeners, which see deferred
//   commits when memory adopts them); `documentValues` is a document change with a value.
// - The registry is the static extensions of `openChat`; a tool is an extension named `tool:<name>`.
// - Waits (the fake model's pacing included) run on the `TestClock`, which `waitFor` advances in 5 ms steps; a
//   submission is awaited while it advances (`settle`). An aborted call is the interruption of the tool's or the
//   response's fiber.
// - `flush` lets pending work run.
// - A partial is copied with `structuredClone`.
// - "commits a growing text and tool call at a cost that follows the new text, not the string lengths": each
//   stream opens a fresh storage of the backend.
import { GCProfiler, getHeapStatistics, setFlagsFromString } from "node:v8";
import { runInNewContext } from "node:vm";
import { assert, it } from "@effect/vitest";
import * as Delta from "@effect-durable-harness/delta/Delta";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as AgentTool from "../src/AgentTool.ts";
import { ToolContext } from "../src/AgentTool.ts";
import * as Extension from "../src/Extension.ts";
import type { Conversation } from "../src/Harness.ts";
import type { ConversationId } from "../src/Ids.ts";
import { LiveDoc, type LiveState, type ToolSlot } from "../src/Live.ts";
import type { AssistantMessage } from "../src/Message.ts";
import type { Submission } from "../src/Submission.ts";
import {
  fauxAssistantMessage,
  fauxText,
  fauxThinking,
  fauxToolCall,
} from "../src/testing/FauxModel.ts";
import type { FauxModelOptions } from "../src/testing/FauxModel.ts";
import { Tx } from "../src/Tx.ts";
import type { ConversationView } from "../src/View.ts";
import { type ChatSetup, chatSetup, openChat, textOf, waitFor } from "./ChatSupport.ts";
import { anyNumber, type TestHarness } from "./HarnessSupport.ts";
import {
  type Backend,
  controlledStorage,
  describeEachBackend,
  flush,
  run,
} from "./SessionSupport.ts";

interface Frame {
  readonly value: ConversationView;
  readonly ops: Delta.Delta;
}

/** Acquires in a scope of its own; closing it stops the watch or detaches the observer. */
const acquire = <A, E>(open: Effect.Effect<A, E, Scope.Scope>) =>
  Effect.gen(function* () {
    const scope = yield* Scope.make();
    const value = yield* Scope.provide(open, scope);
    return { value, release: Scope.close(scope, Exit.void) };
  });

/** What the cases compare with `JSON.stringify`: views and their parts. */
type Comparable = ConversationView | Schema.Json | undefined;

const sameJson = (left: Comparable, right: Comparable) =>
  JSON.stringify(left) === JSON.stringify(right);

/**
 * Starts a watch of `conversation` that records every frame and, with `check`, checks that its operations turn
 * the value before it into its value. The frames are what a renderer that keeps up gets.
 */
const listen = (conversation: Conversation, check = true) =>
  Effect.gen(function* () {
    const { value: watch, release } = yield* acquire(conversation.watch());
    const frames: Array<Frame> = [];
    const state = { replayed: watch.value, mismatches: 0 };
    const consumer = yield* Effect.forkChild(
      Stream.runForEach(watch.frames, (frame) =>
        Effect.sync(() => {
          if (check) {
            state.replayed = Delta.applyImmutableUnsafe(state.replayed, frame.ops);
            if (!sameJson(state.replayed, frame.value)) state.mismatches++;
          }
          frames.push({ value: frame.value, ops: frame.ops });
        }),
      ),
      { startImmediately: true },
    );
    return {
      frames,
      mismatches: () => state.mismatches,
      stop: Fiber.interrupt(consumer).pipe(Effect.andThen(release)),
    };
  });

/** A view's `core.live`, or `{}` when it is absent. */
// SAFETY: the mount holds `core.live` as its definition materializes it.
const liveOf = (view: ConversationView) => (view.docs["core.live"] ?? {}) as LiveState;

/** Lets watch consumers, which run after the commit, catch up. */
const drained = flush;

const slow = (): ChatSetup => chatSetup({ tokensPerSecond: 400, tokenSize: { min: 1, max: 1 } });

/** Every view of `conversationId` from now on, one per commit, unfolded. */
const viewHistory = (h: TestHarness, conversationId: ConversationId) =>
  Effect.gen(function* () {
    const views: Array<ConversationView> = [];
    const { release } = yield* acquire(
      h.views.attach(conversationId, undefined, (value) => {
        views.push(value);
        return {
          advance: (frame) => {
            views.push(frame.value);
          },
          closeSession: () => {},
        };
      }),
    );
    return {
      views,
      live: (): ReadonlyArray<LiveState> => views.map(liveOf),
      detach: release,
    };
  });

/** The slots of tool call `callId` in each `core.live` value. */
const slotsOf = (
  history: ReadonlyArray<LiveState>,
  callId: string,
): ReadonlyArray<ToolSlot | undefined> =>
  history.map((live) => (live.tools ?? []).find((slot) => slot.callId === callId));

/** Committed `core.live` values of the Harness, one per commit that changes one. */
const liveValues = (h: TestHarness, record: (live: LiveState) => void) =>
  h.kernel.followCommits((publication) => {
    for (const change of publication.changes) {
      if (change.type !== "document" || change.record.kind !== "core.live" || change.value === null)
        continue;
      // SAFETY: a `core.live` change carries a `LiveState`.
      record(change.value as LiveState);
    }
  });

/** Committed partials of the Harness's generations, one per commit that has one. */
const partialsOf = (h: TestHarness) =>
  Effect.gen(function* () {
    const partials: Array<AssistantMessage> = [];
    yield* liveValues(h, (live) => {
      const message = live.generation?.message;
      if (message !== undefined) partials.push(message);
    });
    return partials;
  });

/** The distinct in-flight partials in `states`, in order. */
const partialsIn = (states: ReadonlyArray<LiveState>): Array<AssistantMessage> => {
  const partials: Array<AssistantMessage> = [];
  for (const state of states) {
    const message = state.generation?.message;
    if (message !== undefined && message !== partials.at(-1)) partials.push(message);
  }
  return partials;
};

/** Awaits a submission while the `TestClock` advances. */
const settle = (submission: Submission, steps?: number) => finished(submission.await, steps);

/** Runs `effect` to its end while the `TestClock` advances. */
const finished = <A, E, R>(effect: Effect.Effect<A, E, R>, steps?: number) =>
  Effect.gen(function* () {
    const fiber = yield* Effect.forkChild(effect, { startImmediately: true });
    yield* waitFor(
      Effect.sync(() => fiber.pollUnsafe() !== undefined),
      steps,
    );
    return yield* Fiber.join(fiber);
  });

const NoParameters = Schema.Struct({});

/** An extension holding one tool, named `tool:<name>`. */
const toolExtension = <E>(
  name: string,
  description: string,
  execute: () => Effect.Effect<AgentTool.ToolExecutionResult, E, ToolContext>,
  outputLimits: AgentTool.OutputLimits = {},
) =>
  Extension.make({
    name: `tool:${name}`,
    tools: [AgentTool.make(name, { description, parameters: NoParameters, outputLimits, execute })],
  });

/** A view's queued inputs. */
const inbox = (view: ConversationView) =>
  // SAFETY: the mount holds `core.inbox` as its definition materializes it.
  (view.docs["core.inbox"] as { readonly items: ReadonlyArray<{ id: number; mode: string }> })
    .items;

/** A streamed source line. */
const line = (index: number) =>
  `line ${index}: some streamed source text with "quotes" and \\ escapes\n`;

const live = (h: TestHarness, root: Conversation) =>
  h.session.snapshot(LiveDoc, root.id).pipe(Effect.map(Option.getOrUndefined));

describeEachBackend("a run as the conversation view shows it", (backend) => {
  it.effect(
    "streams the answer as operations that rebuild every committed partial, then appends it",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const setup = slow();
          const text = "streamed answer text ".repeat(20);
          setup.faux.setResponses([fauxAssistantMessage([fauxText(text)])]);
          const { harness, root } = yield* openChat(yield* controlledStorage, setup);
          const partials = yield* partialsOf(harness);
          const history = yield* viewHistory(harness, root.id);
          const { stop, mismatches } = yield* listen(root);
          const submission = yield* root.submit({ type: "input", content: "hi" });
          yield* settle(submission);
          yield* drained;
          yield* history.detach;
          assert.strictEqual(mismatches(), 0);
          const states = history.live();
          assert.isAbove(partials.length, 1);
          assert.deepStrictEqual(partialsIn(states).map(textOf), partials.map(textOf));
          // The run shows from the input's commit until the commit that appends the answer and clears the partial.
          const started = states.findIndex((state) => state.run !== undefined);
          const runState = states[started]!.run;
          assert.deepStrictEqual(runState, {
            taskId: anyNumber(runState?.taskId),
            inputs: [submission.id],
            turns: 0,
            first: anyNumber(runState?.first),
          });
          assert.strictEqual(history.views[started]!.entries.at(-1)!.kind, "core.user");
          const ended = states.findIndex(
            (state, index) => index > started && state.run === undefined,
          );
          assert.isUndefined(states[ended]!.generation);
          assert.strictEqual(textOf(history.views[ended]!.entries.at(-1)!.model![0]), text);
          assert.deepStrictEqual(states.at(-1), {});
          yield* stop;
          yield* harness.close;
        }),
      ),
  );

  it.effect(
    "shows a tool call's slot from pending through its running output to its result entry",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const setup = chatSetup();
          const gate = yield* Deferred.make<void>();
          const print = toolExtension("print", "Prints", () =>
            Effect.gen(function* () {
              const api = yield* ToolContext;
              yield* api.output("one\n");
              yield* Effect.sleep("150 millis");
              yield* api.output("two\n");
              yield* Deferred.await(gate);
              return {};
            }),
          );
          setup.faux.setResponses([
            fauxAssistantMessage([fauxToolCall("print", { n: 1 }, { id: "c1" })], {
              stopReason: "toolUse",
            }),
            fauxAssistantMessage([fauxText("done")]),
          ]);
          const { harness, root } = yield* openChat(yield* controlledStorage, setup, {
            extensions: [print],
          });
          const history = yield* viewHistory(harness, root.id);
          const submission = yield* root.submit({ type: "input", content: "go" });
          yield* waitFor(
            Effect.sync(() => slotsOf(history.live(), "c1").at(-1)?.output === "one\ntwo\n"),
          );
          yield* Deferred.succeed(gate, undefined);
          yield* settle(submission);
          yield* history.detach;
          const slots = slotsOf(history.live(), "c1");
          const listed = slots.findIndex((slot) => slot !== undefined);
          // Listed pending with the tool-calling answer, in the same commit.
          assert.deepStrictEqual(slots[listed], {
            callId: "c1",
            name: "print",
            status: "pending",
            taskId: anyNumber(slots[listed]?.taskId),
          });
          assert.strictEqual(history.views[listed]!.entries.at(-1)!.kind, "core.assistant");
          const outputs = slots.flatMap((slot) =>
            slot?.status === "running" ? [slot.output ?? ""] : [],
          );
          assert.deepStrictEqual([...new Set(outputs)], ["", "one\n", "one\ntwo\n"]);
          // Done with its result entry, appended in the same commit.
          const done = slots.findIndex((slot) => slot?.status === "done");
          assert.deepStrictEqual(slots[done], {
            callId: "c1",
            name: "print",
            status: "done",
            taskId: anyNumber(slots[done]?.taskId),
            entry: history.views[done]!.entries.at(-1)!.id,
          });
          assert.deepInclude(history.views[done]!.entries.at(-1)!.model![0], {
            role: "toolResult",
            toolCallId: "c1",
          });
          // The run's two generations, the tool round and the answer, hand the run over.
          const runTasks = new Set(
            history.live().flatMap((state) => (state.run === undefined ? [] : [state.run.taskId])),
          );
          assert.strictEqual(runTasks.size, 2);
          yield* harness.close;
        }),
      ),
  );

  it.effect("shows queued inputs in the inbox, a retry backoff, and the next run", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        const release = yield* Deferred.make<void>();
        // The generation's interruption ends the wait.
        const held = () =>
          Deferred.await(release).pipe(Effect.as(fauxAssistantMessage([fauxText("first")])));
        const error = fauxAssistantMessage([], {
          stopReason: "error",
          errorMessage: "503 Service Unavailable",
        });
        setup.faux.setResponses([held, error, fauxAssistantMessage([fauxText("second")])]);
        const { harness, root } = yield* openChat(yield* controlledStorage, setup);
        setup.settings.retry = { enabled: true, maxRetries: 1, baseDelayMs: 1 };
        const history = yield* viewHistory(harness, root.id);
        const first = yield* root.submit({ type: "input", content: "a" });
        const followUp = yield* root.submit({ type: "input", content: "f" });
        const queued = inbox(history.views.at(-1)!);
        assert.lengthOf(queued, 1);
        assert.deepInclude(queued[0], { id: followUp.id, mode: "followUp" });
        yield* Deferred.succeed(release, undefined);
        yield* settle(followUp);
        yield* history.detach;
        const states = history.live();
        assert.isTrue(
          states.some((state) => state.generation?.retry?.error === "503 Service Unavailable"),
        );
        const retried = states.findIndex((state) => state.generation?.retry !== undefined);
        assert.isTrue(states.slice(retried).some((state) => state.generation?.attempt === 2));
        const runs = states.flatMap((state, index) =>
          state.run !== undefined && state.run.inputs !== states[index - 1]?.run?.inputs
            ? [state.run.inputs]
            : [],
        );
        assert.deepStrictEqual(runs.at(0), [first.id]);
        assert.deepStrictEqual(runs.at(-1), [followUp.id]);
        assert.deepStrictEqual(inbox(history.views.at(-1)!), []);
        yield* harness.close;
      }),
    ),
  );

  it.effect(
    "rebuilds every committed partial of thinking, text, and tool-call arguments from the view's operations",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const setup = chatSetup({ tokensPerSecond: 150, tokenSize: { min: 1, max: 1 } });
          const message = fauxAssistantMessage(
            [
              fauxThinking("thinking about it ".repeat(10)),
              fauxText("some text ".repeat(10)),
              fauxToolCall(
                "missing",
                { path: "a/long/path/".repeat(10), note: "x".repeat(60) },
                { id: "c1" },
              ),
            ],
            { stopReason: "toolUse" },
          );
          setup.faux.setResponses([message, fauxAssistantMessage([fauxText("done")])]);
          const { harness, root } = yield* openChat(yield* controlledStorage, setup);
          const partials = yield* partialsOf(harness);
          const views: Array<ConversationView> = [];
          const ops: Array<Delta.Delta> = [];
          const { value: observation, release: detach } = yield* acquire(
            harness.views.attach(root.id, undefined, (value) => ({
              value,
              advance: (frame) => {
                views.push(frame.value);
                ops.push(frame.ops);
              },
              closeSession: () => {},
            })),
          );
          yield* settle(yield* root.submit({ type: "input", content: "go" }));
          yield* detach;
          // Replaying the operations alone, from the observation's value, gives every committed partial.
          let replayed = observation.value;
          const rebuilt: Array<AssistantMessage> = [];
          for (const batch of ops) {
            replayed = Delta.applyImmutableUnsafe(replayed, batch);
            const partial = liveOf(replayed).generation?.message;
            if (partial !== undefined && partial !== rebuilt.at(-1)) rebuilt.push(partial);
          }
          assert.deepStrictEqual(replayed, views.at(-1));
          assert.isAbove(partials.length, 2);
          assert.deepStrictEqual(
            rebuilt.map((partial) => partial.content),
            partials.map((partial) => partial.content),
          );
          // Growing text and thinking travel as appends.
          const appends = ops.flat().filter((op) => op[0] === "a" && op[1][2] === "generation");
          assert.isAbove(appends.length, 2);
          yield* harness.close;
        }),
      ),
  );

  it.effect("rebuilds a sliding tail window from output trims and appends", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        const gate = yield* Deferred.make<void>();
        const tail = toolExtension(
          "tail",
          "Prints lines",
          () =>
            Effect.gen(function* () {
              const api = yield* ToolContext;
              for (let line = 0; line < 6; line++) {
                yield* api.output(`line ${line}\n`);
                yield* Effect.sleep("120 millis");
              }
              yield* Deferred.await(gate);
              return {};
            }),
          { maxLines: 3, retain: "tail" },
        );
        setup.faux.setResponses([
          fauxAssistantMessage([fauxToolCall("tail", {}, { id: "c1" })], {
            stopReason: "toolUse",
          }),
          fauxAssistantMessage([fauxText("done")]),
        ]);
        const { harness, root } = yield* openChat(yield* controlledStorage, setup, {
          extensions: [tail],
        });
        const outputs: Array<string> = [];
        yield* liveValues(harness, (value) => {
          const output = value.tools?.[0]?.output;
          if (output !== undefined && output !== outputs.at(-1)) outputs.push(output);
        });
        const { frames, mismatches, stop } = yield* listen(root);
        const submission = yield* root.submit({ type: "input", content: "go" });
        yield* waitFor(Effect.sync(() => outputs.at(-1)?.endsWith("line 5\n") === true));
        yield* Deferred.succeed(gate, undefined);
        yield* settle(submission);
        yield* drained;
        yield* stop;
        assert.strictEqual(mismatches(), 0);
        const shown: Array<string> = [];
        for (const frame of frames) {
          const output = liveOf(frame.value).tools?.[0]?.output;
          if (output !== undefined && output !== shown.at(-1)) shown.push(output);
        }
        assert.deepStrictEqual(shown, outputs);
        assert.isTrue(frames.some((frame) => frame.ops.some((op) => op[0] === "t")));
        yield* harness.close;
      }),
    ),
  );

  it.effect("ends a run and starts the queued follow-up's in one value", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        const release = yield* Deferred.make<void>();
        // The generation's interruption ends the wait.
        const held = () =>
          Deferred.await(release).pipe(Effect.as(fauxAssistantMessage([fauxText("first")])));
        setup.faux.setResponses([held, fauxAssistantMessage([fauxText("second")])]);
        const { harness, root } = yield* openChat(yield* controlledStorage, setup);
        const input = yield* root.submit({ type: "input", content: "a" });
        const followUp = yield* root.submit({ type: "input", content: "f" });
        const history = yield* viewHistory(harness, root.id);
        yield* Deferred.succeed(release, undefined);
        yield* settle(followUp);
        yield* history.detach;
        const states = history.live();
        const boundary = states.findIndex((state) => state.run?.inputs[0] === followUp.id);
        assert.deepStrictEqual(states[boundary - 1]!.run?.inputs, [input.id]);
        const before = history.views[boundary - 1]!;
        const after = history.views[boundary]!;
        const appended = after.entries.slice(before.entries.length);
        assert.deepStrictEqual(
          appended.map((entry) => [entry.kind, textOf(entry.model?.[0])]),
          [
            ["core.assistant", "first"],
            ["core.user", "f"],
          ],
        );
        // SAFETY: the mount holds `core.inbox` as its definition materializes it.
        assert.deepStrictEqual(
          (after.docs["core.inbox"] as { items: Array<Schema.Json> }).items,
          [],
        );
        yield* harness.close;
      }),
    ),
  );

  it.effect(
    "ends a call that never runs through its own task, and a tool aborted with its generation",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const setup = chatSetup();
          const wait = toolExtension("wait", "Waits until aborted", () => Effect.never);
          setup.faux.setResponses([
            fauxAssistantMessage(
              [fauxToolCall("ghost", {}, { id: "c1" }), fauxToolCall("wait", {}, { id: "c2" })],
              { stopReason: "toolUse" },
            ),
          ]);
          const { harness, root } = yield* openChat(yield* controlledStorage, setup, {
            extensions: [wait],
          });
          const history = yield* viewHistory(harness, root.id);
          const submission = yield* root.submit({ type: "input", content: "go" });
          yield* waitFor(
            Effect.sync(() =>
              slotsOf(history.live(), "c2").some((slot) => slot?.status === "running"),
            ),
          );
          // Aborting the generation aborts its round: the tool ends with its aborted result first.
          const live = Option.getOrThrow(yield* harness.session.snapshot(LiveDoc, root.id));
          yield* finished(harness.harness.abortTask(live.run!.taskId));
          yield* settle(submission);
          yield* history.detach;
          const states = history.live();
          // The call not offered is listed pending with its task in the commit of its calling answer, and
          // its task ends it done with its result entry.
          const listed = slotsOf(states, "c1").findIndex((slot) => slot !== undefined);
          assert.strictEqual(slotsOf(states, "c1")[listed]?.status, "pending");
          assert.strictEqual(slotsOf(states, "c1")[listed]?.unoffered, true);
          assert.isDefined(slotsOf(states, "c1")[listed]?.taskId);
          assert.strictEqual(history.views[listed]!.entries.at(-1)!.kind, "core.assistant");
          const ended = slotsOf(states, "c1").findIndex((slot) => slot?.status === "done");
          assert.strictEqual(
            slotsOf(states, "c1")[ended]!.entry,
            history.views[ended]!.entries.at(-1)!.id,
          );
          // The aborted tool ends with its result entry, then the run ends without an answer.
          const done = slotsOf(states, "c2").findIndex((slot) => slot?.status === "done");
          assert.strictEqual(
            slotsOf(states, "c2")[done]!.entry,
            history.views[done]!.entries.at(-1)!.id,
          );
          assert.deepStrictEqual(states.at(-1)!.runFailure?.inputs, [submission.id]);
          assert.strictEqual(states.at(-1)!.runFailure?.reason, "aborted");
          yield* harness.close;
        }),
      ),
  );

  it.effect(
    "keeps a steer in the current run, shows nothing for other conversations, and appends a reset",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const setup = chatSetup();
          const gate = yield* Deferred.make<void>();
          const hold = toolExtension("hold", "Waits", () =>
            Deferred.await(gate).pipe(Effect.as({})),
          );
          setup.faux.setResponses([
            fauxAssistantMessage([fauxToolCall("hold", {}, { id: "c1" })], {
              stopReason: "toolUse",
            }),
            fauxAssistantMessage([fauxText("done")]),
          ]);
          const { harness, root } = yield* openChat(yield* controlledStorage, setup, {
            extensions: [hold],
          });
          const other = yield* harness.harness.createConversation({
            ownership: { kind: "ownerless" },
          });
          const history = yield* viewHistory(harness, root.id);
          const input = yield* root.submit({ type: "input", content: "a" });
          yield* waitFor(
            Effect.sync(() =>
              slotsOf(history.live(), "c1").some((slot) => slot?.status === "running"),
            ),
          );
          const steer = yield* root.submit({ type: "input", content: "s", whenBusy: "steer" });
          const count = history.views.length;
          yield* other.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              yield* tx.appendEntry(other.id, { kind: "note" });
            }),
          );
          assert.strictEqual(history.views.length, count);
          yield* Deferred.succeed(gate, undefined);
          yield* settle(input);
          yield* root.reset(undefined);
          yield* history.detach;
          const states = history.live();
          // One run: the steer joined it.
          const runs = new Set(
            states.flatMap((state) => (state.run === undefined ? [] : [state.run.inputs[0]])),
          );
          assert.deepStrictEqual([...runs], [input.id]);
          assert.isTrue(states.some((state) => state.run?.inputs.includes(steer.id) === true));
          // The reset is appended in place, with no run.
          assert.strictEqual(history.views.at(-1)!.entries.at(-1)!.kind, "core.reset");
          assert.isUndefined(states.at(-1)!.run);
          yield* harness.close;
        }),
      ),
  );

  it.effect(
    "converts the streamed partial into one aborted answer entry when the run is aborted",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const setup = chatSetup({ tokensPerSecond: 100, tokenSize: { min: 1, max: 1 } });
          setup.faux.setResponses([fauxAssistantMessage([fauxText("x".repeat(400))])]);
          const { harness, root } = yield* openChat(yield* controlledStorage, setup);
          const history = yield* viewHistory(harness, root.id);
          const submission = yield* root.submit({ type: "input", content: "hi" });
          yield* waitFor(
            Effect.sync(() =>
              history.live().some((state) => state.generation?.message !== undefined),
            ),
          );
          const taskId = (yield* live(harness, root))?.run?.taskId;
          if (taskId === undefined) return assert.fail("no run");
          yield* finished(harness.harness.abortTask(taskId));
          yield* settle(submission);
          yield* history.detach;
          const states = history.live();
          const ended = states.findIndex((state, index) => index > 0 && state.run === undefined);
          assert.isDefined(states[ended - 1]!.generation?.message);
          const appended = history.views[ended]!.entries.slice(
            history.views[ended - 1]!.entries.length,
          );
          assert.deepStrictEqual(
            appended.map((entry) => entry.kind),
            ["core.assistant"],
          );
          assert.deepInclude(appended[0]!.model![0], { role: "assistant", stopReason: "aborted" });
          assert.strictEqual(states[ended]!.runFailure?.reason, "aborted");
          yield* harness.close;
        }),
      ),
  );

  it.effect(
    "applies later operations to an overflow replacement that holds an in-flight partial",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const { harness, root } = yield* openChat(yield* controlledStorage, chatSetup());
          const partial = fauxAssistantMessage([fauxText("hel")]);
          const { value: watch, release } = yield* acquire(root.watch({ min: 1, max: 1 }));
          const change = <E>(edit: (state: LiveState) => Effect.Effect<void, E>) =>
            root.commit(
              Effect.gen(function* () {
                const tx = yield* Tx;
                yield* edit(yield* tx.doc(LiveDoc, root.id));
              }),
            );
          yield* change((state) =>
            Effect.sync(() => {
              // SAFETY: the draft of `core.live` is mutable; `LiveState` types its readonly value.
              (state as { generation?: LiveState["generation"] }).generation = {
                attempt: 1,
                message: structuredClone(partial),
              };
            }),
          );
          // Past 10,000 pending operations: an append, a splice, and a `before` per commit.
          for (let index = 0; index < 3400; index++) {
            yield* root.commit(
              Effect.gen(function* () {
                const tx = yield* Tx;
                yield* tx.appendEntry(root.id, { kind: "note" });
              }),
            );
          }
          const frames: Array<Frame> = [];
          const consumer = yield* Effect.forkChild(
            Stream.runForEach(watch.frames, (frame) =>
              Effect.sync(() => {
                frames.push({ value: frame.value, ops: frame.ops });
              }),
            ),
            { startImmediately: true },
          );
          yield* drained;
          assert.deepStrictEqual<unknown>(frames[0]!.ops, [["r", frames[0]!.value]]);
          yield* change((state) =>
            Effect.sync(() => {
              // SAFETY: the draft of `core.live` is mutable, and the partial's first block is its text.
              (state.generation!.message!.content[0] as { text: string }).text += "lo";
            }),
          );
          yield* drained;
          const update = frames.at(-1)!;
          assert.deepStrictEqual(update.ops, [
            ["a", ["docs", "core.live", "generation", "message", "content", 0, "text"], "lo"],
          ]);
          const rebuilt = Delta.applyImmutableUnsafe(frames[0]!.value, update.ops);
          assert.deepStrictEqual(liveOf(rebuilt).generation?.message?.content, [
            { type: "text", text: "hello" },
          ]);
          yield* Fiber.interrupt(consumer);
          yield* release;
          yield* harness.close;
        }),
      ),
    60_000,
  );
});

/** A full collection, through the collector `--expose-gc` exposes in a new context. */
const gc = (() => {
  setFlagsFromString("--expose-gc");
  // SAFETY: `gc` is the collector the flag exposes in a new context.
  return runInNewContext("gc") as () => void;
})();

/** Bytes `body` allocates, counting what collections during it freed. */
const allocatedBytes = <A, E, R>(body: Effect.Effect<A, E, R>) =>
  Effect.gen(function* () {
    gc();
    const profiler = new GCProfiler();
    profiler.start();
    const before = getHeapStatistics().used_heap_size;
    yield* body;
    const after = getHeapStatistics().used_heap_size;
    let freed = 0;
    for (const collection of profiler.stop().statistics) {
      freed +=
        collection.beforeGC.heapStatistics.usedHeapSize -
        collection.afterGC.heapStatistics.usedHeapSize;
    }
    return after - before + freed;
  });

/** Heap bytes in use after a full collection: three rounds of a macrotask turn then `gc()`. */
const heapInUse = Effect.promise(async () => {
  for (let round = 0; round < 3; round++) {
    await new Promise((resolve) => setImmediate(resolve));
    gc();
  }
  return getHeapStatistics().used_heap_size;
});

/** A chat whose generation commits partials as often as possible. */
const progressive = (options: FauxModelOptions) => {
  const setup = chatSetup(options);
  setup.settings.progress = { partialIntervalMs: 1, outputIntervalMs: 100 };
  return setup;
};

// A streamed partial is committed as appends: text by `Tracker.appendText`, tool-call arguments by the changes the
// streaming parser reports (`PartialChanges`).
describeEachBackend("streamed partials", (backend: Backend) => {
  it.effect("streams a tool-call partial as appends at its argument paths", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup({ tokensPerSecond: 500, tokenSize: { min: 1, max: 1 } });
        setup.settings.progress = { partialIntervalMs: 20, outputIntervalMs: 100 };
        const args = { path: "src/file.ts", content: "const a = 1;\n".repeat(150) };
        setup.faux.setResponses([
          fauxAssistantMessage([fauxToolCall("missing", args, { id: "c1" })], {
            stopReason: "toolUse",
          }),
          fauxAssistantMessage([fauxText("done")]),
        ]);
        const { harness, root } = yield* openChat(yield* controlledStorage, setup);
        const partials = yield* partialsOf(harness);
        const ops: Array<Delta.Operation> = [];
        const { release: detach } = yield* acquire(
          harness.views.attach(root.id, undefined, () => ({
            advance: (frame) => {
              ops.push(...frame.ops);
            },
            closeSession: () => {},
          })),
        );
        yield* settle(yield* root.submit({ type: "input", content: "go" }), 2000);
        yield* detach;
        const MESSAGE = ["docs", "core.live", "generation", "message"];
        // Operations on the in-flight message, with their paths relative to it.
        const onMessage = ops.flatMap((op) => {
          const path: Delta.Path = op[0] === "r" ? [] : op[1];
          return MESSAGE.every((segment, index) => path[index] === segment)
            ? [{ kind: op[0], path: path.slice(MESSAGE.length) }]
            : [];
        });
        const argumentAppends = onMessage.filter(
          (op) => op.kind === "a" && op.path[2] === "arguments",
        );
        assert.isAbove(argumentAppends.length, 10);
        assert.isTrue(
          argumentAppends.every((op) => ["path", "content"].includes(String(op.path.at(-1)))),
        );
        // The call is set whole only when a member appears (at most once per argument); growing strings are
        // appends. With the raw argument text on the block, every commit resent the whole call.
        const blockSets = onMessage.filter((op) => op.kind !== "a" && op.path.length === 2);
        assert.isAtMost(blockSets.length, 2);
        // The committed partials carry only the parsed arguments, never the raw argument text.
        for (const partial of partials) {
          for (const block of partial.content) {
            assert.deepStrictEqual(Object.keys(block).toSorted(), [
              "arguments",
              "id",
              "name",
              "type",
            ]);
          }
        }
        yield* harness.close;
      }),
    ),
  );

  it.effect(
    "commits a growing text and tool call at a cost that follows the new text, not the string lengths",
    () =>
      Effect.gen(function* () {
        /** Bytes allocated while streaming one answer of `size` characters of text and as much tool-call content. */
        const stream = (size: number) =>
          // A fresh storage of the backend per stream.
          run(backend)(
            Effect.gen(function* () {
              const setup = progressive({
                tokensPerSecond: 64_000,
                tokenSize: { min: 64, max: 64 },
              });
              // Keep `core.live` cached between commits, as the default retention does. Evicted after each use (the
              // suite's evicting run), every commit replays the document's delta chain: a cost of eviction, not
              // of appending.
              setup.settings.documentRetentionMs = 60_000;
              const text = 'streamed text with "quotes" and \\ escapes\n'.repeat(size / 40);
              setup.faux.setResponses([
                fauxAssistantMessage(
                  [
                    fauxText(text),
                    fauxToolCall("missing", { path: "a.ts", content: text }, { id: "c1" }),
                  ],
                  { stopReason: "toolUse" },
                ),
                fauxAssistantMessage([fauxText("done")]),
              ]);
              const { harness, root } = yield* openChat(yield* controlledStorage, setup);
              const counter = { commits: 0 };
              yield* harness.kernel.followCommits(() => {
                counter.commits++;
              });
              const bytes = yield* allocatedBytes(
                Effect.gen(function* () {
                  yield* settle(yield* root.submit({ type: "input", content: "go" }), 10_000);
                }),
              );
              yield* harness.close;
              return { bytes, commits: counter.commits };
            }),
          );
        const small = yield* stream(40_000);
        const large = yield* stream(400_000);
        assert.isAbove(large.commits, small.commits * 5);
        // Per commit, the cost stays flat: comparing the whole strings on every commit made it grow with their
        // length.
        assert.isBelow(large.bytes / large.commits / (small.bytes / small.commits), 2);
      }),
    120_000,
  );

  it.effect(
    "keeps a watcher's memory proportional to a long streamed text and tool call, not to their commits",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const setup = progressive({ tokensPerSecond: 16_000, tokenSize: { min: 16, max: 16 } });
          const text = Array.from({ length: 2_000 }, (_, index) => line(index)).join("");
          const content = Array.from({ length: 2_000 }, (_, index) => line(index)).join("");
          setup.faux.setResponses([
            fauxAssistantMessage(
              [fauxText(text), fauxToolCall("missing", { path: "a.ts", content }, { id: "c1" })],
              { stopReason: "toolUse" },
            ),
            fauxAssistantMessage([fauxText("done")]),
          ]);
          const { harness, root } = yield* openChat(yield* controlledStorage, setup);
          const { frames, stop } = yield* listen(root, false);
          const before = yield* heapInUse;
          const submission = yield* root.submit({ type: "input", content: "go" });
          const shown = (index: number) =>
            live(harness, root).pipe(
              Effect.map((value) => {
                const block = value?.generation?.message?.content[index];
                if (block?.type === "text") return block.text.length;
                return block?.type === "toolCall"
                  ? String(block.arguments["content"] ?? "").length
                  : 0;
              }),
            );
          const retained: Array<number> = [];
          // Measure while the text, then the tool call, is almost complete: the watcher's view holds the most then.
          // A step lets only a few deltas commit, whatever the pace, so the bound is in
          // steps that cover the stream: about 40 characters per step.
          yield* waitFor(
            Effect.map(shown(0), (length) => length > text.length * 0.95),
            20_000,
          );
          retained.push((yield* heapInUse) - before);
          yield* waitFor(
            Effect.map(shown(1), (length) => length > content.length * 0.95),
            20_000,
          );
          retained.push((yield* heapInUse) - before);
          yield* settle(submission, 20_000);
          const commits = frames.length;
          assert.isAbove(commits, 500);
          // The two 140 KB strings, the watcher's recorded frames and the rest of the stream: about 3 MB. Sliced
          // append ops made the watcher's view hold a copy of the text per commit: well over 100 MB.
          for (const bytes of retained) assert.isBelow(bytes, 20_000_000);
          yield* stop;
          yield* harness.close;
        }),
      ),
    120_000,
  );
});

// `submit({ type: "continue" })`, a run without a new user message, on every backend.
//
// `openChat` opens a Harness over a `ControlledStorage`; a call that waits for its abort is `Effect.never`,
// interrupted with the call; `faux` is `FauxModel`; tools are extensions given to `openChat`; the Session's
// `snapshot` is an `Option`; `ConversationBusy` is the failure of the submit Effect. Waits let the `TestClock`
// advance in 5 ms steps (`waitFor`). The answer is found among the conversation's entries.
//
// - "answers a run interrupted by a host that died while the model answered": the dying host is
//   `HarnessSupport.killable`.
import { assert, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as TestClock from "effect/testing/TestClock";
import * as Agent from "../src/Agent.ts";
import { AgentStorage } from "../src/AgentStorage.ts";
import * as AgentTool from "../src/AgentTool.ts";
import { UserEntry } from "../src/Entry.ts";
import * as Extension from "../src/Extension.ts";
import type { Conversation } from "../src/Harness.ts";
import { InboxDoc } from "../src/Inbox.ts";
import { LiveDoc } from "../src/Live.ts";
import type { AssistantMessage, Message } from "../src/Message.ts";
import type { SettledSubmission, Submission } from "../src/Submission.ts";
import { carrierOf } from "../src/Telemetry.ts";
import * as TestTracer from "../src/testing/TestTracer.ts";
import {
  type FauxResponseStep,
  fauxAssistantMessage,
  fauxText,
  fauxToolCall,
} from "../src/testing/FauxModel.ts";
import { Tx } from "../src/Tx.ts";
import {
  allEntries,
  type ChatSetup,
  chatSetup,
  FAUX_MODEL,
  openChat,
  textOf,
  unanswered,
  waitFor,
} from "./ChatSupport.ts";
import { killable, type TestHarness } from "./HarnessSupport.ts";
import {
  type ControlledStorage,
  controlledStorage,
  describeEachBackend,
  run,
} from "./SessionSupport.ts";

const failureMessage = (): AssistantMessage =>
  fauxAssistantMessage([], { stopReason: "error", errorMessage: "Invalid request" });

/** A step that answers with a fresh error message. */
const failure: FauxResponseStep = () => Effect.sync(failureMessage);

/** A response step that records the roles of the context it was asked with, then answers `text`. */
const answering =
  (text: string, seen: Array<Array<Message["role"]>>): FauxResponseStep =>
  (request) =>
    Effect.sync(() => {
      seen.push(request.messages.map((message) => message.role));
      return fauxAssistantMessage([fauxText(text)]);
    });

/** A faux response held until `release`; `reached` completes when the request is sent. */
const gated = (message: () => AssistantMessage) => {
  const reached = Deferred.makeUnsafe<void>();
  const gate = Deferred.makeUnsafe<void>();
  const step: FauxResponseStep = () =>
    Deferred.succeed(reached, undefined).pipe(
      Effect.andThen(Deferred.await(gate)),
      Effect.map(message),
    );
  return { step, reached, release: Deferred.succeed(gate, undefined) };
};

/** Runs `effect` and returns its result once it settles, letting the `TestClock` advance meanwhile. */
const finished = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  Effect.gen(function* () {
    const fiber = yield* Effect.forkChild(effect, { startImmediately: true });
    yield* waitFor(Effect.sync(() => fiber.pollUnsafe() !== undefined));
    return yield* Fiber.join(fiber);
  });

/** Waits for a submission to settle. */
const settle = (submission: Submission) => finished(submission.await);

/** Waits until a deferred is done, letting the `TestClock` advance meanwhile. */
const reached = (deferred: Deferred.Deferred<void>) => waitFor(Deferred.isDone(deferred));

const live = (harness: TestHarness, root: Conversation) =>
  harness.session.snapshot(LiveDoc, root.id).pipe(Effect.map(Option.getOrUndefined));

const inboxItems = (harness: TestHarness, root: Conversation) =>
  harness.session
    .snapshot(InboxDoc, root.id)
    .pipe(Effect.map((found) => Option.getOrUndefined(found)?.items));

const answerText = (root: Conversation, settled: SettledSubmission) =>
  Effect.gen(function* () {
    if (settled.status !== "done" || settled.type === "write") {
      throw new Error(`Unexpected ${settled.status}`);
    }
    const answer = (yield* allEntries(root)).find((entry) => entry.id === settled.answer);
    return textOf(answer?.model?.[0]);
  });

const userEntries = (root: Conversation) =>
  allEntries(root).pipe(
    Effect.map((entries) => entries.filter((entry) => UserEntry.is(entry)).length),
  );

/** Checks a record's `status` and `reason`. */
const assertUnanswered = (
  record: { readonly status: string; readonly reason?: string },
  reason: string,
) => {
  assert.strictEqual(record.status, "unanswered");
  assert.strictEqual(record.reason, reason);
};

/** An extension holding one tool. */
const toolExtension = (agentTool: AgentTool.Any) =>
  Extension.make({ name: `tool:${agentTool.name}`, tools: [agentTool] });

const chat = (storage: ControlledStorage, setup: ChatSetup, tools: Array<AgentTool.Any> = []) =>
  openChat(storage, setup, { extensions: tools.map(toolExtension) });

/** What the `delegate` tool saw. */
interface DelegateOutcome {
  busy?: Exit.Exit<unknown, { readonly _tag: string }>;
  first?: string;
  resumed?: string;
}

describeEachBackend("continue", (backend) => {
  it.effect("answers a run that failed, without a new user message", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        const seen: Array<Array<Message["role"]>> = [];
        setup.faux.setResponses([failure, answering("recovered", seen)]);
        const { harness, root } = yield* chat(yield* controlledStorage, setup);
        const first = yield* root.submit({ type: "input", content: "Analyze this" });
        assertUnanswered(yield* settle(first), "model_error");
        assert.strictEqual((yield* live(harness, root))?.runFailure?.reason, "model_error");

        const resumed = yield* root.submit({ type: "continue" });
        assert.strictEqual((yield* resumed.status).status, "placed");
        const settled = yield* settle(resumed);
        assert.strictEqual(settled.type, "continue");
        assert.strictEqual(yield* answerText(root, settled), "recovered");
        assert.strictEqual(seen.at(-1)?.at(-1), "user");
        // The failed input stays unanswered; no user entry was added.
        assert.strictEqual((yield* first.status).status, "unanswered");
        assert.strictEqual(yield* userEntries(root), 1);
        assert.isUndefined((yield* live(harness, root))?.runFailure);
        yield* harness.close;
      }),
    ),
  );

  it.effect("answers the tool results of a round aborted midway", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        const started = Deferred.makeUnsafe<void>();
        const wait = AgentTool.make("wait", {
          description: "Waits until aborted",
          parameters: Schema.Struct({}),
          // Never answers; the abort interrupts it.
          execute: () => Deferred.succeed(started, undefined).pipe(Effect.andThen(Effect.never)),
        });
        const seen: Array<Array<Message["role"]>> = [];
        setup.faux.setResponses([
          fauxAssistantMessage([fauxToolCall("wait", {}, { id: "c1" })], { stopReason: "toolUse" }),
          answering("after the abort", seen),
        ]);
        const { harness, root } = yield* chat(yield* controlledStorage, setup, [wait]);
        const first = yield* root.submit({ type: "input", content: "go" });
        yield* reached(started);
        yield* finished(root.abort());
        assertUnanswered(yield* settle(first), "aborted");
        const settled = yield* settle(yield* root.submit({ type: "continue" }));
        assert.strictEqual(yield* answerText(root, settled), "after the abort");
        assert.strictEqual(seen.at(-1)?.at(-1), "toolResult");
        yield* harness.close;
      }),
    ),
  );

  it.effect("skips system messages when deciding there is something to answer", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        const seen: Array<Array<Message["role"]>> = [];
        setup.faux.setResponses([
          fauxAssistantMessage("first answer"),
          failure,
          answering("resumed", seen),
        ]);
        const { harness, root } = yield* chat(yield* controlledStorage, setup);
        yield* root.configure({ instructions: "Be thorough." });
        yield* settle(yield* root.submit({ type: "input", content: "one" }));
        // Changed instructions append a system delta after the next user message, before its request.
        yield* root.configure({ instructions: "Be brief." });
        const second = yield* root.submit({ type: "input", content: "two" });
        assertUnanswered(yield* settle(second), "model_error");
        const roles = (yield* root.context()).messages.map((message) => message.role);
        assert.strictEqual(roles.at(-1), "system");
        const settled = yield* settle(yield* root.submit({ type: "continue" }));
        assert.strictEqual(yield* answerText(root, settled), "resumed");
        yield* harness.close;
      }),
    ),
  );

  it.effect("is rejected while busy, after request ID deduplication", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        const busy = unanswered();
        setup.faux.setResponses([failure, busy.step]);
        const { harness, root } = yield* chat(yield* controlledStorage, setup);
        yield* settle(yield* root.submit({ type: "input", content: "go" }));
        const resumed = yield* root.submit({ type: "continue", requestId: "resume-1" });
        yield* reached(busy.reached);
        const rejected = yield* Effect.flip(root.submit({ type: "continue" }));
        assert.strictEqual(rejected._tag, "ConversationBusy");
        // The same request ID returns the existing submission instead of rejecting.
        assert.strictEqual(
          (yield* root.submit({ type: "continue", requestId: "resume-1" })).id,
          resumed.id,
        );
        yield* harness.close;
      }),
    ),
  );

  it.effect("ends without a request when there is nothing to answer", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        setup.faux.setResponses([fauxAssistantMessage("answered")]);
        const { harness, root } = yield* chat(yield* controlledStorage, setup);
        const empty = yield* settle(yield* root.submit({ type: "continue" }));
        assert.strictEqual(empty.type, "continue");
        assertUnanswered(empty, "nothing_to_answer");
        yield* settle(yield* root.submit({ type: "input", content: "hi" }));
        const calls = setup.faux.state.callCount;
        // A context ending in an answer has nothing to continue.
        const after = yield* settle(yield* root.submit({ type: "continue" }));
        assertUnanswered(after, "nothing_to_answer");
        assert.strictEqual(setup.faux.state.callCount, calls);
        assert.strictEqual((yield* live(harness, root))?.runFailure?.reason, "nothing_to_answer");
        yield* harness.close;
      }),
    ),
  );

  it.effect(
    "continues an answer with tool calls cut off at its output limit, which ends in their missing results",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const setup = chatSetup();
          const seen: Array<Array<Message["role"]>> = [];
          setup.faux.setResponses([
            fauxAssistantMessage([fauxText("calling"), fauxToolCall("echo", {}, { id: "c1" })], {
              stopReason: "length",
            }),
            answering("resumed", seen),
          ]);
          const { harness, root } = yield* chat(yield* controlledStorage, setup);
          // The run answered: a cut-off answer is an answer, and its calls never ran.
          assert.strictEqual(
            (yield* settle(yield* root.submit({ type: "input", content: "go" }))).status,
            "done",
          );
          const settled = yield* settle(yield* root.submit({ type: "continue" }));
          assert.strictEqual(yield* answerText(root, settled), "resumed");
          assert.deepStrictEqual(seen.at(-1)?.slice(-2), ["assistant", "toolResult"]);
          yield* harness.close;
        }),
      ),
  );

  it.effect("does not continue a text answer cut off at its output limit", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        setup.faux.setResponses([
          fauxAssistantMessage([fauxText("cut")], { stopReason: "length" }),
        ]);
        const { harness, root } = yield* chat(yield* controlledStorage, setup);
        yield* settle(yield* root.submit({ type: "input", content: "write a lot" }));
        const settled = yield* settle(yield* root.submit({ type: "continue" }));
        assertUnanswered(settled, "nothing_to_answer");
        assert.strictEqual(setup.faux.state.callCount, 1);
        yield* harness.close;
      }),
    ),
  );

  it.effect("answers a run interrupted by a host that died while the model answered", () =>
    run(backend)(
      Effect.gen(function* () {
        const inner = yield* AgentStorage;
        const crashable = yield* killable(inner);
        const first = chatSetup({ tokensPerSecond: 20, tokenSize: { min: 1, max: 1 } });
        first.faux.setResponses([fauxAssistantMessage("z".repeat(400))]);
        first.settings.progress = { persistWithinMs: 0 };
        let opened = yield* chat(crashable.storage, first);
        yield* opened.harness.harness.resume;
        const id = (yield* opened.root.submit({ type: "input", content: "hi" })).id;
        yield* waitFor(
          live(opened.harness, opened.root).pipe(
            Effect.map((value) => value?.generation?.message !== undefined),
          ),
        );
        yield* TestClock.adjust("50 millis");
        yield* crashable.kill;
        yield* opened.harness.close;

        const setup = chatSetup();
        setup.settings.retry = { enabled: false };
        const seen: Array<Array<Message["role"]>> = [];
        setup.faux.setResponses([answering("after the crash", seen)]);
        opened = yield* chat(yield* controlledStorage, setup);
        yield* opened.harness.harness.resume;
        const submission = Option.getOrThrow(yield* opened.harness.harness.submission(id));
        assertUnanswered(yield* settle(submission), "interrupted");
        const settled = yield* settle(yield* opened.root.submit({ type: "continue" }));
        assert.strictEqual(yield* answerText(opened.root, settled), "after the crash");
        // The aborted partial is not model context: the request ends in the user message.
        assert.strictEqual(seen.at(-1)?.at(-1), "user");
        yield* opened.harness.close;
      }),
    ),
  );

  it.effect(
    "places a reset a failed run kept, and finds nothing to answer in the context it starts",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const setup = chatSetup();
          const held = gated(failureMessage);
          setup.faux.setResponses([held.step]);
          const { harness, root } = yield* chat(yield* controlledStorage, setup);
          const first = yield* root.submit({ type: "input", content: "first" });
          yield* reached(held.reached);
          yield* root.reset(undefined);
          yield* held.release;
          assertUnanswered(yield* settle(first), "model_error");
          assert.lengthOf((yield* inboxItems(harness, root)) ?? [], 1);

          const settled = yield* settle(yield* root.submit({ type: "continue" }));
          assertUnanswered(settled, "nothing_to_answer");
          assert.deepStrictEqual((yield* inboxItems(harness, root)) ?? [], []);
          assert.deepStrictEqual(
            (yield* root.context()).messages.filter((message) => message.role !== "system"),
            [],
          );
          assert.strictEqual(setup.faux.state.callCount, 1);
          yield* harness.close;
        }),
      ),
  );

  it.effect(
    "starts the inputs queued behind a continue with nothing to answer under afterFailedRun continue",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const setup = chatSetup();
          setup.settings.afterFailedRun = "continue";
          setup.faux.setResponses([
            fauxAssistantMessage("answered"),
            fauxAssistantMessage("second answered"),
          ]);
          const { harness, root } = yield* chat(yield* controlledStorage, setup);
          yield* settle(yield* root.submit({ type: "input", content: "one" }));
          const resumed = yield* root.submit({ type: "continue" });
          const queued = yield* root.submit({
            type: "input",
            content: "two",
            whenBusy: "followUp",
          });
          assert.strictEqual((yield* queued.status).status, "queued");
          assertUnanswered(yield* settle(resumed), "nothing_to_answer");
          const settled = yield* settle(queued);
          assert.strictEqual(yield* answerText(root, settled), "second answered");
          yield* harness.close;
        }),
      ),
  );

  it.effect(
    "is submitted by a tool through the handle of a conversation it owns, and rejected there while busy",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const setup = chatSetup();
          const busy = Deferred.makeUnsafe<void>();
          const release = Deferred.makeUnsafe<void>();
          const outcome: DelegateOutcome = {};
          const delegate = AgentTool.make("delegate", {
            description: "Delegates",
            parameters: Schema.Struct({}),
            execute: (_args, api) =>
              Effect.gen(function* () {
                const child = yield* api.commit(
                  Effect.gen(function* () {
                    const tx = yield* Tx;
                    const created = yield* tx.createConversation({
                      ownership: { kind: "task", taskId: api.taskId },
                    });
                    yield* Agent.configure(created.id, { model: FAUX_MODEL });
                    return created.id;
                  }),
                );
                const handle = Option.getOrThrow(yield* api.conversation(child));
                const first = yield* (yield* handle.submit({ type: "input", content: "work" }))
                  .await;
                outcome.first = first.status;
                const resumed = yield* handle.submit({ type: "continue" });
                yield* Deferred.await(busy);
                // The submit's exit keeps its `ConversationBusy` failure.
                outcome.busy = yield* Effect.exit(handle.submit({ type: "continue" }));
                yield* Deferred.succeed(release, undefined);
                const settled = yield* resumed.await;
                outcome.resumed = settled.status;
                return { output: [{ type: "text" as const, text: settled.status }] };
              }).pipe(Effect.orDie),
          });
          setup.faux.setResponses([
            fauxAssistantMessage([fauxToolCall("delegate", {}, { id: "c1" })], {
              stopReason: "toolUse",
            }),
            failure,
            () =>
              Deferred.succeed(busy, undefined).pipe(
                Effect.andThen(Deferred.await(release)),
                Effect.as(fauxAssistantMessage("child recovered")),
              ),
            fauxAssistantMessage("done"),
          ]);
          const { harness, root } = yield* chat(yield* controlledStorage, setup, [delegate]);
          yield* settle(yield* root.submit({ type: "input", content: "go" }));
          assert.strictEqual(outcome.first, "unanswered");
          assert.strictEqual(outcome.resumed, "done");
          const rejected = outcome.busy;
          assert.isTrue(rejected !== undefined && Exit.isFailure(rejected));
          const error = Exit.isFailure(rejected!) ? Exit.findErrorOption(rejected) : Option.none();
          assert.strictEqual(Option.getOrUndefined(error)?._tag, "ConversationBusy");
          yield* harness.close;
        }),
      ),
  );

  // The traces are recorded by a `TestTracer`.
  it.effect("starts a root trace linked to its caller, as an input does", () => {
    const traced = TestTracer.make();
    return run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        setup.faux.setResponses([failure, fauxAssistantMessage("recovered")]);
        const { harness, root } = yield* chat(yield* controlledStorage, setup);
        yield* settle(yield* root.submit({ type: "input", content: "go" }));
        const id = yield* Effect.gen(function* () {
          const resumed = yield* root.submit({ type: "continue" });
          yield* settle(resumed);
          return resumed.id;
        }).pipe(Effect.withSpan("caller"));
        yield* finished(harness.harness.awaitIdle);
        yield* waitFor(
          Effect.sync(() => traced.spans.every((span) => span.status._tag === "Ended")),
        );
        const caller = traced.spans.find((span) => span.name === "caller");
        const submit = traced.spans.find(
          (span) =>
            span.name === "durable.submit" && span.attributes.get("durable.submission.id") === id,
        );
        assert.isTrue(submit !== undefined && Option.isNone(submit.parent));
        assert.strictEqual(submit?.attributes.get("durable.submission.result"), "placed");
        assert.deepStrictEqual(
          submit?.links.map((link) => carrierOf(link.span)),
          // Asserted present, so the comparison never passes on two absent values.
          [carrierOf(Option.getOrThrow(Option.fromNullishOr(caller)))],
        );
        const record = yield* Option.getOrThrow(yield* harness.harness.submission(id)).status;
        assert.deepStrictEqual(record.trace, submit === undefined ? undefined : carrierOf(submit));
        assert.isTrue(
          traced.spans.some(
            (span) =>
              span.name === "durable.phase" &&
              span.attributes.get("durable.phase.name") === "prepare" &&
              span.parent.pipe(
                Option.map((parent) => parent.spanId === submit?.spanId),
                Option.getOrElse(() => false),
              ),
          ),
        );
        yield* harness.close;
      }).pipe(Effect.provide(traced.layer)),
    );
  });

  it.effect("places inputs a failed run left queued first, and they join its run", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        const held = gated(failureMessage);
        setup.faux.setResponses([held.step, fauxAssistantMessage("both handled")]);
        const { harness, root } = yield* chat(yield* controlledStorage, setup);
        yield* root.submit({ type: "input", content: "first" });
        yield* reached(held.reached);
        const queued = yield* root.submit({
          type: "input",
          content: "second",
          whenBusy: "followUp",
        });
        yield* held.release;
        yield* finished(harness.harness.awaitIdle);
        assert.strictEqual((yield* queued.status).status, "queued");

        const resumed = yield* root.submit({ type: "continue" });
        assert.deepStrictEqual((yield* live(harness, root))?.run?.inputs, [queued.id, resumed.id]);
        const byQueued = yield* settle(queued);
        const byContinue = yield* settle(resumed);
        assert.strictEqual(yield* answerText(root, byContinue), "both handled");
        assert.strictEqual(byQueued.status, "done");
        assert.strictEqual(
          "answer" in byQueued ? byQueued.answer : undefined,
          "answer" in byContinue ? byContinue.answer : undefined,
        );
        yield* harness.close;
      }),
    ),
  );
});

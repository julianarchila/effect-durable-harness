// `core.live.runFailure`, on every backend: why the newest run ended without an answer, as state a renderer shows.
//
// `openChat` opens a Harness over a `ControlledStorage`; `addTool` is an extension given to `openChat`; the model
// is a `FauxModel`; snapshots are the Session's `snapshot` (an `Option`). Waits on the agent let the `TestClock`
// advance, in 5 ms steps (`waitFor`). A timestamp is checked to be a number, then compared with the value read.
//
// "keeps the failure while a run continues the queue …" loops over the policies with a fresh storage of the
// backend each time.
import { assert, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as AgentTool from "../src/AgentTool.ts";
import * as Extension from "../src/Extension.ts";
import type { Conversation } from "../src/Harness.ts";
import { LiveDoc, type LiveState, type RunFailure } from "../src/Live.ts";
import type { AfterFailedRun } from "../src/Settings.ts";
import type { Submission } from "../src/Submission.ts";
import {
  type FauxResponseFactory,
  fauxAssistantMessage,
  fauxText,
  fauxToolCall,
} from "../src/testing/FauxModel.ts";
import { chatSetup, openChat, unanswered, waitFor } from "./ChatSupport.ts";
import type { TestHarness } from "./HarnessSupport.ts";
import { type Backend, controlledStorage, describeEachBackend, run } from "./SessionSupport.ts";

/** Runs `effect` and returns its result once it settles, letting the `TestClock` advance meanwhile. */
const finished = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  Effect.gen(function* () {
    const fiber = yield* Effect.forkChild(effect, { startImmediately: true });
    yield* waitFor(Effect.sync(() => fiber.pollUnsafe() !== undefined));
    return yield* Fiber.join(fiber);
  });

/** Waits for `submission` to finish. */
const settle = (submission: Submission) => finished(submission.await);

/** Waits until a deferred is done, letting the `TestClock` advance meanwhile. */
const reached = (deferred: Deferred.Deferred<void>) => waitFor(Deferred.isDone(deferred));

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

const live = (harness: TestHarness, conversation: Conversation) =>
  harness.session
    .snapshot(LiveDoc, conversation.id)
    .pipe(Effect.map((found): Readonly<LiveState> => Option.getOrElse(found, () => ({}))));

/** A response that fails for good once `gate` opens; `reached` completes when it was requested. */
const failingOnce = () => {
  const gate = Deferred.makeUnsafe<void>();
  const reached = Deferred.makeUnsafe<void>();
  const step: FauxResponseFactory = () =>
    Deferred.succeed(reached, undefined).pipe(
      Effect.andThen(Deferred.await(gate)),
      Effect.as(fauxAssistantMessage([], { stopReason: "error", errorMessage: "Invalid request" })),
    );
  return { step, gate, reached };
};

describeEachBackend("core.live.runFailure", (backend) => {
  it.effect("records a model error with its text, and a later run clears it", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        setup.faux.setResponses([
          fauxAssistantMessage([], { stopReason: "error", errorMessage: "Invalid request" }),
          fauxAssistantMessage([fauxText("fine")]),
        ]);
        const { harness, root } = yield* openChat(yield* controlledStorage, setup);
        const submission = yield* root.submit({ type: "input", content: "hi" });
        const settled = yield* settle(submission);
        if (settled.status !== "unanswered") return assert.fail(`Unexpected ${settled.status}`);
        assert.strictEqual(settled.reason, "model_error");
        assert.strictEqual(settled.detail, "Invalid request");
        const failure = (yield* live(harness, root)).runFailure;
        if (failure?.first === undefined) return assert.fail("no runFailure.first");
        assert.isNumber(failure.taskId);
        assert.isNumber(failure.first);
        assert.deepStrictEqual(failure, {
          taskId: failure.taskId,
          inputs: [submission.id],
          reason: "model_error",
          detail: "Invalid request",
          first: failure.first,
        } satisfies RunFailure);
        const next = yield* root.submit({ type: "input", content: "again" });
        // The commit that starts the next run removes it.
        assert.isUndefined((yield* live(harness, root)).runFailure);
        assert.strictEqual((yield* settle(next)).status, "done");
        assert.deepStrictEqual(yield* live(harness, root), {});
        yield* harness.close;
      }),
    ),
  );

  it.effect("records a missing model with which one", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        const { harness, root } = yield* openChat(yield* controlledStorage, setup);
        yield* root.configure({ model: { provider: "faux", modelId: "gone" } });
        const submission = yield* root.submit({ type: "input", content: "hi" });
        const settled = yield* settle(submission);
        if (settled.status !== "unanswered") return assert.fail(`Unexpected ${settled.status}`);
        assert.strictEqual(settled.reason, "no_model");
        // The settlement is unchanged: no detail.
        assert.isFalse("detail" in settled);
        const failure = (yield* live(harness, root)).runFailure;
        assert.deepStrictEqual(failure?.inputs, [submission.id]);
        assert.strictEqual(failure?.reason, "no_model");
        assert.strictEqual(failure?.detail, "Model faux/gone is not available");
        yield* harness.close;
      }),
    ),
  );

  it.effect("records an abort", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        const busy = unanswered();
        setup.faux.setResponses([busy.step]);
        const { harness, root } = yield* openChat(yield* controlledStorage, setup);
        const submission = yield* root.submit({ type: "input", content: "hi" });
        yield* reached(busy.reached);
        const taskId = (yield* live(harness, root)).run!.taskId;
        yield* finished(root.abort());
        const settled = yield* settle(submission);
        if (settled.status !== "unanswered") return assert.fail(`Unexpected ${settled.status}`);
        assert.strictEqual(settled.reason, "aborted");
        const failure = (yield* live(harness, root)).runFailure;
        if (failure?.first === undefined) return assert.fail("no runFailure.first");
        assert.isNumber(failure.first);
        assert.deepStrictEqual(failure, {
          taskId,
          inputs: [submission.id],
          reason: "aborted",
          first: failure.first,
        });
        yield* harness.close;
      }),
    ),
  );

  it.effect(
    "records a reset that cut the run before an answer, kept by the run its queued input starts",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const setup = chatSetup();
          const gate = Deferred.makeUnsafe<void>();
          const started = Deferred.makeUnsafe<void>();
          const hold = Extension.make({
            name: "tool:hold",
            tools: [
              AgentTool.make("hold", {
                description: "Waits",
                parameters: Schema.Struct({}),
                execute: () =>
                  Deferred.succeed(started, undefined).pipe(
                    Effect.andThen(Deferred.await(gate)),
                    Effect.as({}),
                  ),
              }),
            ],
          });
          const second = unanswered();
          setup.faux.setResponses([
            fauxAssistantMessage([fauxToolCall("hold", {}, { id: "c1" })], {
              stopReason: "toolUse",
            }),
            second.step,
          ]);
          const { harness, root } = yield* openChat(yield* controlledStorage, setup, {
            extensions: [hold],
          });
          const first = yield* root.submit({ type: "input", content: "go" });
          yield* reached(started);
          yield* root.reset(undefined);
          const followUp = yield* root.submit({
            type: "input",
            content: "next",
            whenBusy: "followUp",
          });
          yield* Deferred.succeed(gate, undefined);
          const settled = yield* settle(first);
          assert.strictEqual(settled.status, "unanswered");
          assert.strictEqual(settled.status === "unanswered" ? settled.reason : undefined, "reset");
          yield* reached(second.reached);
          const state = yield* live(harness, root);
          // The follow-up's run started in the commit that ended the first; the failure stays until a later run starts.
          assert.deepStrictEqual(state.run?.inputs, [followUp.id]);
          assert.deepStrictEqual(state.runFailure?.inputs, [first.id]);
          assert.strictEqual(state.runFailure?.reason, "reset");
          yield* finished(root.abort());
          const after = (yield* live(harness, root)).runFailure;
          assert.deepStrictEqual(after?.inputs, [followUp.id]);
          assert.strictEqual(after?.reason, "aborted");
          yield* harness.close;
        }),
      ),
  );

  it.effect(
    "keeps the failure while a run continues the queue in the same commit, and replaces it with the next",
    () =>
      run(backend)(
        Effect.gen(function* () {
          for (const policy of [
            "continue",
            "keep",
          ] as const satisfies ReadonlyArray<AfterFailedRun>) {
            const setup = chatSetup();
            setup.settings.afterFailedRun = policy;
            const failing = failingOnce();
            const busy = unanswered();
            setup.faux.setResponses([
              failing.step,
              busy.step,
              fauxAssistantMessage([fauxText("third")]),
            ]);
            const { harness, root } = yield* openChat(yield* freshStorage(backend), setup);
            const first = yield* root.submit({ type: "input", content: "first" });
            yield* reached(failing.reached);
            const second = yield* root.submit({
              type: "input",
              content: "second",
              whenBusy: "followUp",
            });
            yield* Deferred.succeed(failing.gate, undefined);
            yield* settle(first);
            if (policy === "continue") {
              yield* reached(busy.reached);
              const state = yield* live(harness, root);
              assert.deepStrictEqual(state.run?.inputs, [second.id]);
              assert.deepStrictEqual(state.runFailure?.inputs, [first.id]);
              assert.strictEqual(state.runFailure?.reason, "model_error");
            } else {
              yield* finished(harness.harness.awaitIdle);
              assert.deepStrictEqual((yield* live(harness, root)).runFailure?.inputs, [first.id]);
            }
            yield* harness.close;
          }
        }),
      ),
  );
});

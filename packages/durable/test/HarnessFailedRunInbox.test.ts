// Inputs queued behind a failed run, on every backend.
//
// `openChat` opens a Harness over a `ControlledStorage`; `faux` is `FauxModel`. Waits on the agent let the
// `TestClock` advance, in 5 ms steps (`waitFor`). The settings are `setup.settings`, read at each decision.
//
// `failWithQueuedInput` takes the storage, and "place queued writes, and keep them with withdraw" loops over the
// policies with a fresh storage of the backend each time.
import { assert, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import { AssistantEntry } from "../src/Entry.ts";
import type { AfterFailedRun } from "../src/Settings.ts";
import type { Submission } from "../src/Submission.ts";
import { type FauxResponseFactory, fauxAssistantMessage } from "../src/testing/FauxModel.ts";
import { Tx } from "../src/Tx.ts";
import { chatSetup, openChat, textOf, waitFor } from "./ChatSupport.ts";
import {
  type Backend,
  type ControlledStorage,
  controlledStorage,
  describeEachBackend,
  run,
} from "./SessionSupport.ts";

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

/** A run that fails with an input queued behind it, under `policy`; returns how the queued input settled. */
const failWithQueuedInput = (storage: ControlledStorage, policy: AfterFailedRun | undefined) =>
  Effect.gen(function* () {
    const setup = chatSetup();
    if (policy !== undefined) setup.settings.afterFailedRun = policy;
    const failing = failingOnce();
    setup.faux.setResponses([
      failing.step,
      fauxAssistantMessage("second answer"),
      fauxAssistantMessage("third answer"),
    ]);
    const { harness, root } = yield* openChat(storage, setup);
    const first = yield* root.submit({ type: "input", content: "Analyze this report" });
    yield* reached(failing.reached);
    const second = yield* root.submit({
      type: "input",
      content: "Email me the summary",
      whenBusy: "followUp",
    });
    yield* Deferred.succeed(failing.gate, undefined);
    const settled = yield* settle(first);
    assert.strictEqual(settled.status, "unanswered");
    assert.strictEqual(settled.status === "unanswered" ? settled.reason : undefined, "model_error");
    return { harness, root, second };
  });

describeEachBackend("inputs queued behind a failed run", (backend) => {
  it.effect("stay queued by default", () =>
    run(backend)(
      Effect.gen(function* () {
        const { harness, root, second } = yield* failWithQueuedInput(
          yield* controlledStorage,
          undefined,
        );
        yield* finished(harness.harness.awaitIdle);
        assert.strictEqual((yield* second.status).status, "queued");
        // The next input runs the queue, as before.
        const third = yield* root.submit({ type: "input", content: "Anything else?" });
        assert.strictEqual((yield* settle(second)).status, "done");
        assert.strictEqual((yield* settle(third)).status, "done");
        yield* harness.close;
      }),
    ),
  );

  it.effect("start the next run with continue", () =>
    run(backend)(
      Effect.gen(function* () {
        const { harness, root, second } = yield* failWithQueuedInput(
          yield* controlledStorage,
          "continue",
        );
        const settled = yield* settle(second);
        if (settled.status !== "done" || settled.type !== "input") {
          return assert.fail(`Unexpected ${settled.status}`);
        }
        const answer = yield* root.commit(
          Effect.flatMap(Tx, (tx) => tx.entry(AssistantEntry, settled.answer)),
        );
        assert.strictEqual(textOf(Option.getOrUndefined(answer)?.model?.[0]), "second answer");
        yield* harness.close;
      }),
    ),
  );

  it.effect("place queued writes, and keep them with withdraw", () =>
    run(backend)(
      Effect.gen(function* () {
        for (const policy of ["continue", "withdraw"] as const) {
          const setup = chatSetup();
          setup.settings.afterFailedRun = policy;
          const failing = failingOnce();
          setup.faux.setResponses([failing.step, fauxAssistantMessage("second answer")]);
          const { harness, root } = yield* openChat(yield* freshStorage(backend), setup);
          const first = yield* root.submit({ type: "input", content: "first" });
          yield* reached(failing.reached);
          const note = { role: "user", content: "a note the model sees", timestamp: 1 } as const;
          const write = yield* root.submit({
            type: "write",
            entry: { kind: "test.note", model: [note] },
          });
          assert.strictEqual((yield* write.status).status, "queued");
          yield* Deferred.succeed(failing.gate, undefined);
          yield* settle(first);
          yield* finished(harness.harness.awaitIdle);
          // `continue` places the write at the failed run's boundary; `withdraw` withdraws inputs only.
          assert.strictEqual(
            (yield* write.status).status,
            policy === "continue" ? "done" : "queued",
          );
          yield* harness.close;
        }
      }),
    ),
  );

  it.effect("settle unanswered with run_failed with withdraw", () =>
    run(backend)(
      Effect.gen(function* () {
        const { harness, second } = yield* failWithQueuedInput(
          yield* controlledStorage,
          "withdraw",
        );
        const settled = yield* settle(second);
        assert.strictEqual(settled.status, "unanswered");
        assert.strictEqual(
          settled.status === "unanswered" ? settled.reason : undefined,
          "run_failed",
        );
        yield* harness.close;
      }),
    ),
  );
});

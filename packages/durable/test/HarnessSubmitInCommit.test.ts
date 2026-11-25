// Admission for hosts that must not start runs: `whenIdle: "reject"`, and `Conversation.admit` inside the
// caller's commit, on every backend.
//
// `openChat` opens a Harness over a `ControlledStorage`; tools and tasks are extensions given to `openChat`; the
// model is `FauxModel`; a failure the host raises is a typed failure of its Effect. A reopened process is a new
// Harness over the same storage; a second storage is a fresh storage of the backend. Waits on the agent let the
// `TestClock` advance in 5 ms steps (`waitFor`).
//
// Submitting inside a commit is `Conversation.admit(draft)`, an Effect that requires the `Tx` of the commit it
// runs in: `harness.commit(root.admit(draft))`, and a handle's `handle.admit(draft)` inside the call's or the
// runtime's `commit`. A `Tx` of another Session, or a `Tx` service value that is not the Session's own, is a defect.
//
// An aborted tool's fiber is interrupted, seen by `onInterrupt`, and the tool holds in a finalizer until released.
//
// Traced cases run `openChat` inside a `TestTracer`'s layer, with the caller's span from `Effect.withSpan("caller")`.
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
import { StorageFailure } from "../src/AgentStorage.ts";
import * as Doc from "../src/Doc.ts";
import { UserEntry } from "../src/Entry.ts";
import * as Extension from "../src/Extension.ts";
import type { Conversation } from "../src/Harness.ts";
import { SubmissionId, SubmissionIdValue } from "../src/Ids.ts";
import { InboxDoc } from "../src/Inbox.ts";
import { LiveDoc } from "../src/Live.ts";
import type { AssistantMessage } from "../src/Message.ts";
import { ReadAfterWrite } from "../src/SessionError.ts";
import { ConversationBusy, ConversationIdle, type Submission } from "../src/Submission.ts";
import * as Task from "../src/Task.ts";
import { carrierOf } from "../src/Telemetry.ts";
import type { ConversationHandle } from "../src/Task.ts";
import {
  type FauxResponseFactory,
  fauxAssistantMessage,
  fauxText,
  fauxToolCall,
} from "../src/testing/FauxModel.ts";
import * as TestTracer from "../src/testing/TestTracer.ts";
import { Tx } from "../src/Tx.ts";
import { allEntries, chatSetup, FAUX_MODEL, openChat, unanswered, waitFor } from "./ChatSupport.ts";
import { completed, type TestHarness } from "./HarnessSupport.ts";
import {
  type Backend,
  type ControlledStorage,
  controlledStorage,
  describeEachBackend,
  failureMessage,
  run,
} from "./SessionSupport.ts";

const MODEL = FAUX_MODEL;

/** A value the case needs: asserted present, so a comparison never passes on two absent values. */
const need = <A>(value: A | undefined, what: string): A => {
  assert.isDefined(value, what);
  return Option.getOrThrow(Option.fromNullishOr(value));
};

const failure = (): AssistantMessage =>
  fauxAssistantMessage([], { stopReason: "error", errorMessage: "Invalid request" });

/** A typed failure carrying the message. */
class HostError extends Schema.TaggedError<HostError>()("HostError", {
  message: Schema.String,
}) {}

/** A host's own conversation document. */
const Sent = Doc.make({
  checkpointWhen: () => false,
  kind: "test.sent",
  version: 1,
  scope: "conversation",
  history: "latest",
  fork: "initial",
  schema: Schema.Struct({ ids: Schema.Array(Schema.Number) }),
  initial: () => ({ ids: [] }),
});

/** An empty object's JSON Schema. */
const EMPTY_OBJECT = { type: "object", properties: {} } as const;

/** Runs `effect` and returns its result once it settles, letting the `TestClock` advance meanwhile. */
const finished = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  Effect.gen(function* () {
    const fiber = yield* Effect.forkChild(effect, { startImmediately: true });
    yield* waitFor(Effect.sync(() => fiber.pollUnsafe() !== undefined));
    return yield* Fiber.join(fiber);
  });

/** Awaits a submission while the `TestClock` advances. */
const settle = (submission: Submission) => finished(submission.await);

/** Waits until a deferred is done, letting the `TestClock` advance meanwhile. */
const reached = (deferred: Deferred.Deferred<void>) => waitFor(Deferred.isDone(deferred));

/** A second, fresh storage of the backend, alive for the test's scope. */
const freshStorage = (backend: Backend) =>
  Effect.gen(function* () {
    const context = yield* Layer.buildWithMemoMap(
      backend.layer,
      yield* Layer.makeMemoMap,
      yield* Effect.scope,
    );
    return yield* controlledStorage.pipe(Effect.provideContext(context));
  });

const live = (harness: TestHarness, id: Conversation["id"]) =>
  harness.session.snapshot(LiveDoc, id).pipe(Effect.map(Option.getOrUndefined));

const inbox = (harness: TestHarness, id: Conversation["id"]) =>
  harness.session
    .snapshot(InboxDoc, id)
    .pipe(Effect.map((found) => Option.getOrUndefined(found)?.items));

/** Open a chat whose run hangs until aborted, and wait until it works on its first input. */
const openBusy = (storage: ControlledStorage) =>
  Effect.gen(function* () {
    const setup = chatSetup();
    const busy = unanswered();
    setup.faux.setResponses([busy.step]);
    const opened = yield* openChat(storage, setup);
    const first = yield* opened.root.submit({ type: "input", content: "work" });
    yield* reached(busy.reached);
    return { ...opened, storage, first };
  });

/** Install tool `hold`, which reports its call and waits for `release`; its first call answers `tool`. */
const addHold = () => {
  const reachedHold = Deferred.makeUnsafe<void>();
  const release = Deferred.makeUnsafe<void>();
  const extension = Extension.make({
    name: "tool:hold",
    tools: [
      AgentTool.make("hold", {
        description: "Holds until released",
        parameters: Schema.Struct({}),
        jsonSchema: EMPTY_OBJECT,
        execute: () =>
          Deferred.succeed(reachedHold, undefined).pipe(
            Effect.andThen(Deferred.await(release)),
            Effect.as({ output: [] }),
          ),
      }),
    ],
  });
  const call = fauxAssistantMessage([fauxToolCall("hold", {}, { id: "c1" })], {
    stopReason: "toolUse",
  });
  return { reached: reachedHold, release, call, extension };
};

/** Calls `delegate` when the last message is the user's "delegate", otherwise answers "ok". */
const reply: FauxResponseFactory = (request) => {
  const last = request.messages.at(-1);
  if (last?.role === "user" && last.content === "delegate") {
    return Effect.succeed(
      fauxAssistantMessage([fauxToolCall("delegate", {}, { id: "c1" })], {
        stopReason: "toolUse",
      }),
    );
  }
  return Effect.succeed(fauxAssistantMessage([fauxText("ok")]));
};

/** Settled status of submission `id`, polled without asking for progress (`Submission.await` would). */
const settledStatus = (harness: TestHarness, id: SubmissionId) =>
  Effect.gen(function* () {
    const submission = Option.getOrThrow(yield* harness.harness.submission(id));
    yield* waitFor(
      submission.status.pipe(
        Effect.map(({ status }) => status === "done" || status === "unanswered"),
      ),
    );
    return yield* submission.status;
  });

/** The effect dies, and the rendered cause holds `message`. */
const assertDies = <A, E>(effect: Effect.Effect<A, E>, message: string) =>
  Effect.gen(function* () {
    const exit = yield* Effect.exit(effect);
    assert.isTrue(Exit.isFailure(exit) && Cause.hasDies(exit.cause), "expected a defect");
    assert.include(yield* failureMessage(effect), message);
  });

describeEachBackend("whenIdle reject", (backend) => {
  it.effect("rejects input to an idle conversation without writing", () =>
    run(backend)(
      Effect.gen(function* () {
        const storage = yield* controlledStorage;
        const { harness, root } = yield* openChat(storage, chatSetup());
        const commits = storage.commits.length;
        const rejected = yield* Effect.flip(
          root.submit({ type: "input", content: "hi", whenBusy: "steer", whenIdle: "reject" }),
        );
        assert.instanceOf(rejected, ConversationIdle);
        assert.strictEqual(
          rejected._tag === "ConversationIdle" ? rejected.conversationId : undefined,
          root.id,
        );
        assert.strictEqual(storage.commits.length, commits);
        assert.isUndefined((yield* live(harness, root.id))?.run);
        yield* harness.close;
      }),
    ),
  );

  it.effect(
    "rejects input to an idle conversation that keeps queued items after a failed run",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const setup = chatSetup();
          const failing = Deferred.makeUnsafe<void>();
          const requested = Deferred.makeUnsafe<void>();
          const step: FauxResponseFactory = () =>
            Deferred.succeed(requested, undefined).pipe(
              Effect.andThen(Deferred.await(failing)),
              Effect.as(failure()),
            );
          setup.faux.setResponses([step]);
          const { harness, root } = yield* openChat(yield* controlledStorage, setup);
          const first = yield* root.submit({ type: "input", content: "first" });
          yield* reached(requested);
          const queued = yield* root.submit({ type: "input", content: "queued" });
          yield* Deferred.succeed(failing, undefined);
          const settled = yield* settle(first);
          assert.strictEqual(settled.status, "unanswered");
          assert.strictEqual(
            settled.status === "unanswered" ? settled.reason : undefined,
            "model_error",
          );
          yield* finished(harness.harness.awaitIdle);
          const rejected = yield* Effect.flip(
            root.submit({ type: "input", content: "steer", whenBusy: "steer", whenIdle: "reject" }),
          );
          assert.instanceOf(rejected, ConversationIdle);
          assert.strictEqual((yield* queued.status).status, "queued");
          assert.isUndefined((yield* live(harness, root.id))?.run);
          yield* harness.close;
        }),
      ),
  );

  it.effect("steers a working run", () =>
    run(backend)(
      Effect.gen(function* () {
        const { harness, root, first } = yield* openBusy(yield* controlledStorage);
        const steer = yield* root.submit({
          type: "input",
          content: "also this",
          whenBusy: "steer",
          whenIdle: "reject",
        });
        const record = yield* steer.status;
        assert.strictEqual(record.type, "input");
        assert.strictEqual(record.status, "queued");
        assert.deepStrictEqual(yield* inbox(harness, root.id), [
          { id: steer.id, mode: "steer", content: "also this" },
        ]);
        assert.deepStrictEqual((yield* live(harness, root.id))?.run?.inputs, [first.id]);
        yield* harness.close;
      }),
    ),
  );

  it.effect("returns the submission of a known request ID, also once idle", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        setup.faux.setResponses([fauxAssistantMessage([fauxText("done")])]);
        const { harness, root } = yield* openChat(yield* controlledStorage, setup);
        const first = yield* root.submit({ type: "input", content: "work", requestId: "r1" });
        yield* settle(first);
        const draft = {
          type: "input",
          content: "work",
          requestId: "r1",
          whenBusy: "steer",
          whenIdle: "reject",
        } as const;
        assert.strictEqual((yield* root.submit(draft)).id, first.id);
        // Deduplication runs before the idle rejection in the caller's commit too.
        assert.strictEqual(yield* harness.harness.commit(root.admit(draft)), first.id);
        yield* harness.close;
      }),
    ),
  );

  it.effect("rejects through a tool's conversation handle", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        let other: Conversation | undefined;
        let rejection: unknown;
        const nudge = Extension.make({
          name: "tool:nudge",
          tools: [
            AgentTool.make("nudge", {
              description: "Steers another conversation",
              parameters: Schema.Struct({}),
              jsonSchema: EMPTY_OBJECT,
              execute: (_args, call) =>
                Effect.gen(function* () {
                  const handle = Option.getOrThrow(yield* call.conversation(other!.id));
                  const draft = {
                    type: "input",
                    content: "nudge",
                    whenBusy: "steer",
                    whenIdle: "reject",
                  } as const;
                  rejection = yield* Effect.flip(handle.submit(draft));
                  return { output: [{ type: "text" as const, text: "nudged" }] };
                }).pipe(Effect.orDie),
            }),
          ],
        });
        setup.faux.setResponses([
          fauxAssistantMessage([fauxToolCall("nudge", {}, { id: "c1" })], {
            stopReason: "toolUse",
          }),
          fauxAssistantMessage([fauxText("ok")]),
        ]);
        const { harness, root } = yield* openChat(yield* controlledStorage, setup, {
          extensions: [nudge],
        });
        other = yield* harness.harness.createConversation({
          ownership: { kind: "ownerless" },
          agent: { model: MODEL },
        });
        yield* settle(yield* root.submit({ type: "input", content: "go" }));
        assert.instanceOf(rejection, ConversationIdle);
        assert.isUndefined((yield* live(harness, other.id))?.run);
        yield* harness.close;
      }),
    ),
  );
});

describeEachBackend("submit inside the caller's commit", (backend) => {
  it.effect("admits with the host's own check and writes in one commit", () =>
    run(backend)(
      Effect.gen(function* () {
        const { harness, root, storage, first } = yield* openBusy(yield* controlledStorage);
        const commits = storage.commits.length;
        const id = yield* harness.harness.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            if ((yield* tx.doc(LiveDoc, root.id)).run === undefined) {
              return yield* new HostError({ message: "not working" });
            }
            // The host's own document write may come before the admission.
            const sent = yield* tx.doc(Sent, root.id);
            const admitted = yield* root.admit({
              type: "input",
              content: "steer",
              whenBusy: "steer",
            });
            sent.ids.push(admitted);
            // The host's own table write comes after it.
            yield* tx.appendEntry(root.id, { kind: "app.sent", data: { submission: admitted } });
            return admitted;
          }),
        );
        assert.strictEqual(storage.commits.length, commits + 1);
        const writes = storage.commits.at(-1)!;
        assert.isTrue(writes.some((write) => write.type === "submission" && write.value.id === id));
        assert.isTrue(
          writes.some((write) => write.type === "entry" && write.value.kind === "app.sent"),
        );
        assert.deepStrictEqual(
          Option.getOrUndefined(yield* harness.session.snapshot(Sent, root.id))?.ids,
          [id],
        );
        assert.deepStrictEqual(yield* inbox(harness, root.id), [
          { id, mode: "steer", content: "steer" },
        ]);
        assert.deepStrictEqual((yield* live(harness, root.id))?.run?.inputs, [first.id]);
        yield* harness.close;
      }),
    ),
  );

  it.effect("is rolled back with the commit when the callback throws later", () =>
    run(backend)(
      Effect.gen(function* () {
        const storage = yield* controlledStorage;
        const { harness, root } = yield* openChat(storage, chatSetup());
        const commits = storage.commits.length;
        const failed = yield* Effect.flip(
          harness.harness.commit(
            Effect.gen(function* () {
              yield* root.admit({ type: "input", content: "hi", requestId: "r1" });
              return yield* new HostError({ message: "host check failed" });
            }),
          ),
        );
        assert.strictEqual(failed.message, "host check failed");
        assert.strictEqual(storage.commits.length, commits);
        assert.isUndefined((yield* live(harness, root.id))?.run);
        const found = yield* harness.harness.commit(
          Effect.flatMap(Tx, (tx) => tx.submissionByRequest(root.id, "r1")),
        );
        assert.isTrue(Option.isNone(found));
        yield* harness.close;
      }),
    ),
  );

  it.effect(
    "leaves no submission and no run when Storage fails the commit, also after reopening",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const storage = yield* controlledStorage;
          const setup = chatSetup();
          const { harness, root } = yield* openChat(storage, setup);
          storage.failNextCommit("disk full");
          const failed = yield* Effect.flip(
            harness.harness.commit(
              Effect.gen(function* () {
                const tx = yield* Tx;
                yield* root.admit({ type: "input", content: "hi", requestId: "r1" });
                yield* tx.appendEntry(root.id, { kind: "app.sent" });
              }),
            ),
          );
          assert.instanceOf(failed, StorageFailure);
          assert.strictEqual(failed.message, "disk full");
          yield* finished(harness.harness.closed);
          const reopened = yield* openChat(storage, setup);
          assert.strictEqual(reopened.root.id, root.id);
          const found = yield* reopened.harness.harness.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              const submission = yield* tx.submissionByRequest(root.id, "r1");
              const tasks = yield* tx.scanTasks({ conversationId: root.id }, 10);
              const entries = yield* tx.scanEntries({ conversationId: root.id }, 10);
              return {
                submission: Option.getOrUndefined(submission),
                tasks: tasks.items,
                entries: entries.items,
              };
            }),
          );
          assert.deepStrictEqual(found, { submission: undefined, tasks: [], entries: [] });
          assert.isUndefined((yield* live(reopened.harness, root.id))?.run);
          yield* reopened.harness.close;
        }),
      ),
  );

  it.effect("starts the run of an idle input once the commit is stored", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        setup.faux.setResponses([fauxAssistantMessage([fauxText("answer")])]);
        // No `resume`: the admission asks for progress itself.
        const { harness, root } = yield* openChat(yield* controlledStorage, setup);
        const id = yield* harness.harness.commit(root.admit({ type: "input", content: "hi" }));
        const settled = yield* settledStatus(harness, id);
        assert.strictEqual(settled.type, "input");
        assert.strictEqual(settled.status, "done");
        yield* harness.close;
      }),
    ),
  );

  it.effect("reads a submission by ID in a commit, with the entry its input was placed at", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        setup.faux.setResponses([fauxAssistantMessage([fauxText("answer")])]);
        const { harness, root } = yield* openChat(yield* controlledStorage, setup);
        const id = yield* harness.harness.commit(root.admit({ type: "input", content: "hi" }));
        yield* settledStatus(harness, id);
        const found = yield* harness.harness.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            const submission = Option.getOrUndefined(yield* tx.submission(id));
            const placed =
              submission !== undefined && "entry" in submission ? submission.entry : undefined;
            const entry =
              placed === undefined ? undefined : Option.getOrUndefined(yield* tx.entry(placed));
            return {
              status: submission?.status,
              conversationId: entry?.conversationId,
              kind: entry?.kind,
            };
          }),
        );
        assert.deepStrictEqual(found, {
          status: "done",
          conversationId: root.id,
          kind: "core.user",
        });
        const missing = yield* harness.harness.commit(
          Effect.flatMap(Tx, (tx) => tx.submission(SubmissionId.make(999_999))),
        );
        assert.isTrue(Option.isNone(missing));
        yield* harness.close;
      }),
    ),
  );

  it.effect("queues a model-visible write while busy and places it at the next boundary", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        const hold = addHold();
        setup.faux.setResponses([hold.call, fauxAssistantMessage([fauxText("answer")])]);
        const { harness, root } = yield* openChat(yield* controlledStorage, setup, {
          extensions: [hold.extension],
        });
        const first = yield* root.submit({ type: "input", content: "work" });
        yield* reached(hold.reached);
        const note = { role: "user", content: "a note the model sees", timestamp: 1 } as const;
        const id = yield* harness.harness.commit(
          root.admit({ type: "write", entry: { kind: "test.note", model: [note] } }),
        );
        assert.deepStrictEqual(
          (yield* inbox(harness, root.id))?.map((item) => item.id),
          [id],
        );
        yield* Deferred.succeed(hold.release, undefined);
        assert.strictEqual((yield* settle(first)).status, "done");
        const settled = yield* settledStatus(harness, id);
        assert.strictEqual(settled.type, "write");
        assert.strictEqual(settled.status, "done");
        assert.isNumber(
          settled.status === "done" && "entry" in settled ? settled.entry : undefined,
        );
        yield* harness.close;
      }),
    ),
  );

  it.effect("deduplicates request IDs", () =>
    run(backend)(
      Effect.gen(function* () {
        const { harness, root, storage } = yield* openBusy(yield* controlledStorage);
        const draft = {
          type: "input",
          content: "steer",
          whenBusy: "steer",
          requestId: "r1",
        } as const;
        const first = yield* harness.harness.commit(root.admit(draft));
        const commits = storage.commits.length;
        const again = yield* harness.harness.commit(root.admit(draft));
        assert.strictEqual(again, first);
        assert.strictEqual(storage.commits.length, commits);
        yield* harness.close;
      }),
    ),
  );

  it.effect("rejects before any write, so the callback may catch the rejection", () =>
    run(backend)(
      Effect.gen(function* () {
        const storage = yield* controlledStorage;
        const { harness, root } = yield* openChat(storage, chatSetup());
        const commits = storage.commits.length;
        const outcome = yield* harness.harness.commit(
          root.admit({ type: "input", content: "hi", whenBusy: "steer", whenIdle: "reject" }).pipe(
            Effect.as("admitted"),
            Effect.catchTag("ConversationIdle", () => Effect.succeed("idle")),
            Effect.orElseSucceed(() => "other"),
          ),
        );
        assert.strictEqual(outcome, "idle");
        assert.strictEqual(storage.commits.length, commits);
        assert.isUndefined((yield* live(harness, root.id))?.run);
        yield* harness.close;
      }),
    ),
  );

  it.effect("throws ReadAfterWrite after a table write of the commit, on every path", () =>
    run(backend)(
      Effect.gen(function* () {
        const { harness, root } = yield* openBusy(yield* controlledStorage);
        // A busy input without request ID or trace reads no table, and is still refused.
        const afterAppend = yield* Effect.flip(
          harness.harness.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              yield* tx.appendEntry(root.id, { kind: "app.note" });
              yield* root.admit({ type: "input", content: "steer", whenBusy: "steer" });
            }),
          ),
        );
        assert.instanceOf(afterAppend, ReadAfterWrite);
        assert.include(
          afterAppend.message,
          "Conversation.admit() cannot read tables after the first table write",
        );
        const second = yield* Effect.flip(
          harness.harness.commit(
            Effect.gen(function* () {
              yield* root.admit({ type: "input", content: "one", whenBusy: "steer" });
              yield* root.admit({ type: "input", content: "two", whenBusy: "steer" });
            }),
          ),
        );
        assert.instanceOf(second, ReadAfterWrite);
        assert.deepStrictEqual(yield* inbox(harness, root.id), []);

        const idle = yield* openChat(yield* freshStorage(backend), chatSetup());
        const late = yield* Effect.flip(
          idle.harness.harness.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              yield* tx.appendEntry(idle.root.id, { kind: "app.note" });
              yield* idle.root.admit({ type: "input", content: "hi" });
            }),
          ),
        );
        assert.instanceOf(late, ReadAfterWrite);
        yield* idle.harness.close;
        yield* harness.close;
      }),
    ),
  );

  it.effect("continues a failed run, and must precede the commit's table writes", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        setup.faux.setResponses([failure(), fauxAssistantMessage([fauxText("recovered")])]);
        const { harness, root } = yield* openChat(yield* controlledStorage, setup);
        const first = yield* root.submit({ type: "input", content: "Analyze this" });
        const settled = yield* settle(first);
        assert.strictEqual(settled.status, "unanswered");
        assert.strictEqual(
          settled.status === "unanswered" ? settled.reason : undefined,
          "model_error",
        );

        const late = yield* Effect.flip(
          harness.harness.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              yield* tx.appendEntry(root.id, { kind: "app.note" });
              yield* root.admit({ type: "continue" });
            }),
          ),
        );
        assert.instanceOf(late, ReadAfterWrite);

        const id = yield* harness.harness.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            const continued = yield* root.admit({ type: "continue" });
            yield* tx.appendEntry(root.id, { kind: "app.note" });
            return continued;
          }),
        );
        const status = yield* settledStatus(harness, id);
        assert.strictEqual(status.type, "continue");
        assert.strictEqual(status.status, "done");
        yield* harness.close;
      }),
    ),
  );

  it.effect("rejects a busy continue and steers with whenIdle inside the commit", () =>
    run(backend)(
      Effect.gen(function* () {
        const { harness, root } = yield* openBusy(yield* controlledStorage);
        const busy = yield* Effect.flip(harness.harness.commit(root.admit({ type: "continue" })));
        assert.instanceOf(busy, ConversationBusy);
        const steer = yield* harness.harness.commit(
          root.admit({ type: "input", content: "steer", whenBusy: "steer", whenIdle: "reject" }),
        );
        assert.deepStrictEqual(
          (yield* inbox(harness, root.id))?.map((item) => item.id),
          [steer],
        );
        yield* harness.close;
      }),
    ),
  );

  it.effect("throws for a transaction of another Session, or a wrapped one", () =>
    run(backend)(
      Effect.gen(function* () {
        const a = yield* openChat(yield* controlledStorage, chatSetup());
        const b = yield* openChat(yield* freshStorage(backend), chatSetup());
        const draft = { type: "input", content: "hi" } as const;
        yield* assertDies(
          b.harness.harness.commit(a.root.admit(draft)),
          "The transaction belongs to another Session",
        );
        yield* assertDies(
          a.harness.harness.commit(
            Effect.flatMap(Tx, (tx) =>
              a.root.admit(draft).pipe(Effect.provideService(Tx, new Proxy(tx, {}))),
            ),
          ),
          "The transaction belongs to another Session",
        );
        // A copy of the `Tx` service value, not the Session's own.
        yield* assertDies(
          a.harness.harness.commit(
            Effect.flatMap(Tx, (tx) =>
              a.root.admit(draft).pipe(Effect.provideService(Tx, { ...tx })),
            ),
          ),
          "The transaction belongs to another Session",
        );
        assert.isUndefined((yield* live(a.harness, a.root.id))?.run);
        yield* a.harness.close;
        yield* b.harness.close;
      }),
    ),
  );

  it.effect(
    "records the trace carrier and links a steer to its caller and the running trace",
    () => {
      const traced = TestTracer.make();
      return run(backend)(
        Effect.gen(function* () {
          const setup = chatSetup();
          const busy = unanswered();
          setup.faux.setResponses([busy.step]);
          const { harness, root } = yield* openChat(yield* controlledStorage, setup);
          yield* root.submit({ type: "input", content: "work" });
          yield* reached(busy.reached);
          const runTask = (yield* harness.harness.inspect).tasks.find(
            (task) => task.record.kind === "core.generation",
          )?.record;
          const id = yield* harness.harness
            .commit(root.admit({ type: "input", content: "steer", whenBusy: "steer" }))
            .pipe(Effect.withSpan("caller"));
          const caller = traced.spans.find((span) => span.name === "caller");
          const submit = traced.spans.find(
            (span) =>
              span.name === "durable.submit" && span.attributes.get("durable.submission.id") === id,
          );
          assert.isTrue(submit !== undefined && Option.isNone(submit.parent));
          assert.strictEqual(submit?.attributes.get("durable.submission.result"), "queued");
          assert.deepStrictEqual(
            submit?.links.map((link) => carrierOf(link.span)),
            [
              carrierOf(need(caller, "the caller's span")),
              need(runTask?.trace, "the running task's carrier"),
            ],
          );
          const submission = Option.getOrThrow(yield* harness.harness.submission(id));
          assert.deepStrictEqual(
            (yield* submission.status).trace,
            submit === undefined ? undefined : carrierOf(submit),
          );
          yield* harness.close;
        }).pipe(Effect.provide(traced.layer)),
      );
    },
  );

  it.effect("starts an idle input's run in the admission's trace", () => {
    const traced = TestTracer.make();
    return run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        const busy = unanswered();
        setup.faux.setResponses([busy.step]);
        const { harness, root } = yield* openChat(yield* controlledStorage, setup);
        const id = yield* harness.harness
          .commit(root.admit({ type: "input", content: "hi" }))
          .pipe(Effect.withSpan("caller"));
        yield* reached(busy.reached);
        const submit = traced.spans.find(
          (span) =>
            span.name === "durable.submit" && span.attributes.get("durable.submission.id") === id,
        );
        const record = yield* Option.getOrThrow(yield* harness.harness.submission(id)).status;
        assert.deepStrictEqual(record.trace, submit === undefined ? undefined : carrierOf(submit));
        const runTask = (yield* harness.harness.inspect).tasks.find(
          (task) => task.record.kind === "core.generation",
        )?.record;
        assert.deepStrictEqual(runTask?.trace, record.trace);
        yield* harness.close;
      }).pipe(Effect.provide(traced.layer)),
    );
  });

  it.effect(
    "makes a write a child of its caller and leaves the commit's events to the caller's commit",
    () => {
      const traced = TestTracer.make();
      return run(backend)(
        Effect.gen(function* () {
          const { harness, root } = yield* openChat(yield* controlledStorage, chatSetup());
          const id = yield* harness.harness
            .commit(root.admit({ type: "write", entry: { kind: "app.note" } }))
            .pipe(Effect.withSpan("caller"));
          const caller = traced.spans.find((span) => span.name === "caller");
          const submit = traced.spans.find((span) => span.name === "durable.submit");
          assert.strictEqual(
            submit?.parent.pipe(
              Option.map((parent) => parent.spanId),
              Option.getOrUndefined,
            ),
            caller?.spanId,
          );
          assert.strictEqual(submit?.status._tag, "Ended");
          assert.strictEqual(submit?.attributes.get("durable.submission.id"), id);
          assert.strictEqual(submit?.attributes.get("durable.submission.result"), "done");
          assert.deepStrictEqual(submit?.events, []);
          assert.deepStrictEqual(
            caller?.events.map(([name]) => name),
            ["durable.entry", "durable.submission"],
          );
          yield* harness.close;
        }).pipe(Effect.provide(traced.layer)),
      );
    },
  );

  it.effect("ends its span before the commit, and a later throw leaves no submission", () => {
    const traced = TestTracer.make();
    return run(backend)(
      Effect.gen(function* () {
        const { harness, root } = yield* openChat(yield* controlledStorage, chatSetup());
        const failed = yield* Effect.exit(
          harness.harness
            .commit(
              root
                .admit({ type: "write", entry: { kind: "app.note" } })
                .pipe(Effect.andThen(Effect.fail(new HostError({ message: "later" })))),
            )
            .pipe(Effect.withSpan("caller")),
        );
        assert.isTrue(Exit.isFailure(failed));
        const submit = traced.spans.find((span) => span.name === "durable.submit");
        // The span reported an admission the commit then discarded.
        assert.strictEqual(submit?.status._tag, "Ended");
        assert.strictEqual(submit?.attributes.get("durable.submission.result"), "done");
        const id = SubmissionId.make(Number(submit?.attributes.get("durable.submission.id")));
        assert.isTrue(Option.isNone(yield* harness.harness.submission(id)));
        assert.deepStrictEqual(traced.spans.find((span) => span.name === "caller")?.events, []);
        yield* harness.close;
      }).pipe(Effect.provide(traced.layer)),
    );
  });

  it.effect("admits atomically from a tool's commit through its conversation handle", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        let other: Conversation | undefined;
        let toolTask: number | undefined;
        const delegate = Extension.make({
          name: "tool:delegate",
          tools: [
            AgentTool.make("delegate", {
              description: "Delegates to another conversation",
              parameters: Schema.Struct({}),
              jsonSchema: EMPTY_OBJECT,
              execute: (_args, call) =>
                Effect.gen(function* () {
                  toolTask = call.taskId;
                  const handle = Option.getOrThrow(yield* call.conversation(other!.id));
                  const id = yield* call.commit(
                    Effect.gen(function* () {
                      const tx = yield* Tx;
                      const admitted = yield* handle.admit({
                        type: "input",
                        content: "from the tool",
                      });
                      yield* tx.appendEntry(other!.id, {
                        kind: "app.delegated",
                        data: { submission: admitted },
                      });
                      return admitted;
                    }),
                  );
                  return { output: [{ type: "text" as const, text: `submitted ${id}` }] };
                }).pipe(Effect.orDie),
            }),
          ],
        });
        setup.faux.setResponses([reply, reply, reply]);
        const { harness, root } = yield* openChat(yield* controlledStorage, setup, {
          extensions: [delegate],
        });
        other = yield* harness.harness.createConversation({
          ownership: { kind: "ownerless" },
          agent: { model: MODEL },
        });
        const runSubmission = yield* root.submit({ type: "input", content: "delegate" });
        assert.strictEqual((yield* settle(runSubmission)).status, "done");
        yield* finished(harness.harness.awaitIdle);
        const entries = yield* allEntries(other);
        const delegated = entries.find((entry) => entry.kind === "app.delegated");
        if (delegated === undefined) return assert.fail("no app.delegated entry");
        // SAFETY: the tool appended `app.delegated` with this data.
        const submission = (delegated.data as { submission: SubmissionId }).submission;
        const settled = yield* settledStatus(harness, submission);
        if (settled.type !== "input" || settled.status !== "done") {
          return assert.fail(`Unexpected ${settled.status}`);
        }
        // The placed input carries the tool task's byTaskId, as the commit's own appends do.
        const user = yield* other.commit(
          Effect.flatMap(Tx, (tx) => tx.entry(UserEntry, settled.entry)),
        );
        assert.strictEqual(Option.getOrUndefined(user)?.byTaskId, toolTask);
        yield* harness.close;
      }),
    ),
  );

  it.effect("admits inside a task's terminal commit", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        setup.faux.setResponses([fauxAssistantMessage([fauxText("answer")])]);
        const Admitting = Task.make({
          name: "test.admitting",
          version: 1,
          input: Schema.Null,
          checkpoint: Schema.Struct({ phase: Schema.Literal("run") }),
          result: SubmissionIdValue,
          initial: () => ({ phase: "run" }),
          phases: {
            run: (_task, runtime) =>
              Effect.gen(function* () {
                const handle = Option.getOrThrow(
                  yield* runtime.conversation(runtime.conversationId),
                );
                yield* runtime.commit(
                  Effect.gen(function* () {
                    const id = yield* handle.admit({ type: "input", content: "from the task" });
                    return completed(id);
                  }),
                );
              }),
          },
          abort: (_task, runtime) =>
            runtime.commit(() =>
              Effect.succeed({ status: "terminal", outcome: { status: "aborted" } } as const),
            ),
        });
        const { harness, root } = yield* openChat(yield* controlledStorage, setup, {
          extensions: [Extension.make({ name: "tasks", tasks: [Admitting] })],
        });
        const task = yield* root.commit(
          Effect.flatMap(Tx, (tx) =>
            tx.createTask(Admitting, null, { ownership: { kind: "conversation" } }),
          ),
        );
        yield* harness.harness.resume;
        const settled = yield* finished(harness.harness.awaitTask(task));
        const outcome = settled.state.outcome;
        if (outcome.status !== "completed") return assert.fail(outcome.status);
        const status = yield* settledStatus(harness, outcome.result);
        if (status.type !== "input" || status.status !== "done") {
          return assert.fail(`Unexpected ${status.status}`);
        }
        const user = yield* root.commit(
          Effect.flatMap(Tx, (tx) => tx.entry(UserEntry, status.entry)),
        );
        assert.strictEqual(Option.getOrUndefined(user)?.byTaskId, task);
        yield* harness.close;
      }),
    ),
  );

  it.effect("rejects through a handle once its invocation is aborted, and once it ended", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        let other: Conversation | undefined;
        let handle: ConversationHandle | undefined;
        const started = Deferred.makeUnsafe<void>();
        const abortSeen = Deferred.makeUnsafe<void>();
        const release = Deferred.makeUnsafe<void>();
        const hold = Extension.make({
          name: "tool:hold",
          tools: [
            AgentTool.make("hold", {
              description: "Holds until released, also when aborted",
              parameters: Schema.Struct({}),
              jsonSchema: EMPTY_OBJECT,
              execute: (_args, call) =>
                Effect.gen(function* () {
                  handle = Option.getOrThrow(yield* call.conversation(other!.id));
                  yield* Deferred.succeed(started, undefined);
                  // The abort interrupts the fiber, and the finalizer holds the invocation until `release`.
                  yield* Deferred.await(release).pipe(
                    Effect.onInterrupt(() => Deferred.succeed(abortSeen, undefined)),
                    Effect.ensuring(Deferred.await(release)),
                  );
                  return { output: [] };
                }).pipe(Effect.orDie),
            }),
          ],
        });
        setup.faux.setResponses([
          fauxAssistantMessage([fauxToolCall("hold", {}, { id: "c1" })], {
            stopReason: "toolUse",
          }),
          fauxAssistantMessage([fauxText("ok")]),
        ]);
        const { harness, root } = yield* openChat(yield* controlledStorage, setup, {
          extensions: [hold],
        });
        other = yield* harness.harness.createConversation({
          ownership: { kind: "ownerless" },
          agent: { model: MODEL },
        });
        const runSubmission = yield* root.submit({ type: "input", content: "go" });
        yield* reached(started);
        const tool = (yield* harness.harness.inspect).tasks.find(
          (task) => task.record.kind === "core.tool",
        )!.record;
        const aborting = yield* Effect.forkChild(harness.harness.abortTask(tool.id), {
          startImmediately: true,
        });
        yield* reached(abortSeen);
        const draft = { type: "input", content: "late" } as const;
        assert.isTrue(Exit.isFailure(yield* Effect.exit(handle!.submit(draft))));
        assert.isTrue(
          Exit.isFailure(yield* Effect.exit(harness.harness.commit(handle!.admit(draft)))),
        );
        assert.isUndefined((yield* live(harness, other.id))?.run);
        yield* Deferred.succeed(release, undefined);
        yield* finished(Fiber.join(aborting));
        yield* settle(runSubmission);
        assert.isTrue(
          Exit.isFailure(yield* Effect.exit(harness.harness.commit(handle!.admit(draft)))),
        );
        assert.isUndefined((yield* live(harness, other.id))?.run);
        yield* harness.close;
      }),
    ),
  );
});

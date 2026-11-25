// Submissions to a conversation, on every backend.
//
// `openChat` opens a Harness over a `ControlledStorage`; a cancelled caller context is an interrupted fiber running
// that call; the clock is the `TestClock`; a reopened process is a new Harness over the same storage; a storage that
// overrides one call is a wrapper of the controlled one; the model is `FauxModel`. Waits on the agent let the
// `TestClock` advance in 5 ms steps (`waitFor`), since a turn may span throttle windows.
//
// `Conversation.commit` with the raw `createSubmission` on `Tx` writes a submission record directly.
//
// A record's `entry` is read after narrowing on `type === "input"`, since a continue record has none.
import { assert, it } from "@effect/vitest";
import * as Cause from "effect/Cause";
import * as Context from "effect/Context";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as TestClock from "effect/testing/TestClock";
import { AgentStorage } from "../src/AgentStorage.ts";
import * as Entry from "../src/Entry.ts";
import { AssistantEntry, UserEntry } from "../src/Entry.ts";
import { EntryId, SubmissionId } from "../src/Ids.ts";
import { GenerationTask } from "../src/internal/generation.ts";
import { LiveDoc } from "../src/Live.ts";
import type { Submission } from "../src/Submission.ts";
import { fauxAssistantMessage } from "../src/testing/FauxModel.ts";
import { Tx } from "../src/Tx.ts";
import { allEntries, chatSetup, openChat, unanswered, waitFor } from "./ChatSupport.ts";
import {
  type Backend,
  type ControlledStorage,
  controlledStorage,
  describeEachBackend,
  failureMessage,
  flush,
  run,
} from "./SessionSupport.ts";

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

/** A second, empty storage of the backend, for a case that needs one. */
const freshStorage = (backend: Backend) =>
  Layer.build(backend.layer).pipe(
    Effect.flatMap((context) =>
      controlledStorage.pipe(
        Effect.provideService(AgentStorage, Context.get(context, AgentStorage)),
      ),
    ),
  );

describeEachBackend("submissions", (backend) => {
  it.effect("appends an idle write and settles it done without a turn", () =>
    run(backend)(
      Effect.gen(function* () {
        const storage = yield* controlledStorage;
        const { harness, root } = yield* openChat(storage, chatSetup());
        const submission = yield* root.submit({
          type: "write",
          entry: { kind: "note", data: { text: "x" } },
        });
        const settled = yield* settle(submission);
        assert.strictEqual(settled.status, "done");
        if (settled.type === "continue" || settled.status !== "done") return;
        assert.isNumber(settled.entry);
        assert.isDefined(settled.trace);
        assert.deepStrictEqual<unknown>(settled, {
          id: submission.id,
          conversationId: root.id,
          type: "write",
          status: "done",
          entry: settled.entry,
          // The admission span's carrier.
          trace: settled.trace,
        });
        const entries = yield* allEntries(root);
        assert.deepStrictEqual(entries, [
          { id: settled.entry, conversationId: root.id, kind: "note", data: { text: "x" } },
        ]);
        assert.deepStrictEqual(yield* harness.session.snapshot(LiveDoc, root.id), Option.some({}));
        const tasks = yield* harness.harness.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            return yield* tx.scanTasks({ conversationId: root.id }, 10);
          }),
        );
        assert.deepStrictEqual(tasks.items, []);
        yield* harness.close;
      }),
    ),
  );

  it.effect("places idle input, and rejects busy input with whenBusy reject without writing", () =>
    run(backend)(
      Effect.gen(function* () {
        const storage = yield* controlledStorage;
        const setup = chatSetup();
        // The clock reads 42.
        yield* TestClock.setTime(42);
        const busy = unanswered();
        setup.faux.setResponses([busy.step]);
        const { harness, root } = yield* openChat(storage, setup);
        const submission = yield* root.submit({ type: "input", content: "hi" });
        yield* reached(busy.reached);
        const record = yield* submission.status;
        if (record.type !== "input" || record.status !== "placed")
          return assert.fail(`Unexpected ${record.status}`);
        const entry = yield* root.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            return yield* tx.entry(UserEntry, record.entry);
          }),
        );
        assert.deepStrictEqual(Option.getOrUndefined(entry)?.model, [
          { role: "user", content: "hi", timestamp: 42 },
        ]);
        const live = Option.getOrThrow(yield* harness.session.snapshot(LiveDoc, root.id));
        assert.deepStrictEqual(live.run?.inputs, [submission.id]);
        const liveRun = live.run;
        if (liveRun === undefined) return assert.fail("no run");
        const task = yield* harness.harness.getTask(liveRun.taskId);
        assert.strictEqual(Option.getOrUndefined(task)?.kind, "core.generation");

        const commits = storage.commits.length;
        const rejected = yield* Effect.flip(
          root.submit({ type: "input", content: "again", whenBusy: "reject" }),
        );
        assert.strictEqual(rejected._tag, "ConversationBusy");
        if (rejected._tag !== "ConversationBusy") return;
        assert.strictEqual(rejected.conversationId, root.id);
        assert.strictEqual(storage.commits.length, commits);
        yield* harness.close;
      }),
    ),
  );

  it.effect("deduplicates request IDs per conversation before any write", () =>
    run(backend)(
      Effect.gen(function* () {
        const storage = yield* controlledStorage;
        const setup = chatSetup();
        const busy = unanswered();
        setup.faux.setResponses([busy.step]);
        const { harness, root } = yield* openChat(storage, setup);
        const first = yield* root.submit({ type: "input", content: "hi", requestId: "r1" });
        yield* reached(busy.reached);
        const commits = storage.commits.length;
        // Deduplication runs before the busy check.
        const again = yield* root.submit({ type: "input", content: "different", requestId: "r1" });
        assert.strictEqual(again.id, first.id);
        assert.strictEqual(storage.commits.length, commits);
        const mismatch = yield* Effect.flip(
          root.submit({ type: "write", entry: { kind: "note" }, requestId: "r1" }),
        );
        assert.include(
          mismatch.message,
          "Request r1 already identifies a submission of type input",
        );
        const status = yield* first.status;
        assert.strictEqual(status.requestId, "r1");
        assert.strictEqual(status.status, "placed");

        const other = yield* harness.harness.createConversation({
          ownership: { kind: "ownerless" },
        });
        const write = yield* other.submit({
          type: "write",
          entry: { kind: "note" },
          requestId: "r1",
        });
        assert.notStrictEqual(write.id, first.id);
        const repeated = yield* other.submit({
          type: "write",
          entry: { kind: "note" },
          requestId: "r1",
        });
        assert.strictEqual(repeated.id, write.id);
        yield* harness.close;
      }),
    ),
  );

  it.effect("reports abort results and looks submissions up by conversation", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        const release = Deferred.makeUnsafe<void>();
        setup.faux.setResponses([
          () => Deferred.await(release).pipe(Effect.as(fauxAssistantMessage("answer"))),
        ]);
        const { harness, root } = yield* openChat(yield* controlledStorage, setup);
        const submission = yield* root.submit({ type: "input", content: "hi" });
        assert.deepStrictEqual(yield* submission.withdraw, { _tag: "AlreadyPlaced" });
        assert.deepStrictEqual(yield* harness.harness.withdrawSubmission(submission.id, root.id), {
          _tag: "AlreadyPlaced",
        });
        const other = yield* harness.harness.createConversation({
          ownership: { kind: "ownerless" },
        });
        assert.deepStrictEqual(yield* harness.harness.withdrawSubmission(submission.id, other.id), {
          _tag: "NotFound",
        });
        assert.deepStrictEqual(
          yield* harness.harness.withdrawSubmission(SubmissionId.make(999_999)),
          { _tag: "NotFound" },
        );
        assert.isTrue(Option.isNone(yield* harness.harness.submission(SubmissionId.make(999_999))));

        yield* Deferred.succeed(release, undefined);
        yield* settle(submission);
        assert.deepStrictEqual(yield* submission.withdraw, { _tag: "AlreadySettled" });
        assert.deepStrictEqual(yield* harness.harness.withdrawSubmission(submission.id), {
          _tag: "AlreadySettled",
        });
        yield* harness.close;
      }),
    ),
  );

  it.effect("cancels only a wait and rejects pending waits on close", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        setup.faux.setResponses([unanswered().step]);
        const { harness, root } = yield* openChat(yield* controlledStorage, setup);
        const submission = yield* root.submit({ type: "input", content: "hi" });
        // One caller's wait is cancelled: the fiber running it is interrupted.
        const cancelled = yield* Effect.forkChild(submission.await, { startImmediately: true });
        const pending = yield* Effect.forkChild(submission.await, { startImmediately: true });
        const exit = yield* Fiber.interrupt(cancelled).pipe(Effect.andThen(Fiber.await(cancelled)));
        // The wait ends by that interruption alone.
        assert.isTrue(exit._tag === "Failure" && Cause.hasInterruptsOnly(exit.cause));
        assert.strictEqual((yield* submission.status).status, "placed");
        yield* harness.close;
        const error = yield* Effect.flip(finished(Fiber.join(pending)));
        assert.strictEqual(error._tag, "SessionClosed");
      }),
    ),
  );

  it.effect("rejects a wait whose submission read spans the start of close", () =>
    run(backend)(
      Effect.gen(function* () {
        const entered = Deferred.makeUnsafe<void>();
        const release = Deferred.makeUnsafe<void>();
        let hold = false;
        const base = yield* controlledStorage;
        // A storage that holds `submission` reads.
        const storage: ControlledStorage = {
          ...base,
          submission: (id) =>
            Effect.suspend(() =>
              hold
                ? Deferred.succeed(entered, undefined).pipe(
                    Effect.andThen(Deferred.await(release)),
                    Effect.andThen(base.submission(id)),
                  )
                : base.submission(id),
            ),
        };
        const setup = chatSetup();
        setup.faux.setResponses([unanswered().step, unanswered().step]);
        const first = yield* openChat(storage, setup);
        const { id } = yield* first.root.submit({ type: "input", content: "hi" });
        yield* first.harness.close;
        // Reopen: the Session caches the submissions it writes, so only a reopened one reads it from storage.
        const { harness } = yield* openChat(storage, setup);
        const submission = Option.getOrThrow(yield* harness.harness.submission(id));
        hold = true;
        const waiting = yield* Effect.forkChild(submission.await, { startImmediately: true });
        yield* reached(entered);
        const closing = yield* Effect.forkChild(harness.close, { startImmediately: true });
        yield* Deferred.succeed(release, undefined);
        const error = yield* Effect.flip(finished(Fiber.join(waiting)));
        assert.strictEqual(error._tag, "SessionClosed");
        yield* finished(Fiber.join(closing));
      }),
    ),
  );

  it.effect("reacquires a submission after reopen and settles it durably", () =>
    run(backend)(
      Effect.gen(function* () {
        // A reopened process: the same storage under a new Harness.
        const storage = yield* controlledStorage;
        const setup = chatSetup();
        // The first process never answers; the reopened one does.
        const busy = unanswered();
        setup.faux.setResponses([busy.step, fauxAssistantMessage("after reopen")]);
        let opened = yield* openChat(storage, setup);
        const id = (yield* opened.root.submit({ type: "input", content: "hi", requestId: "print" }))
          .id;
        yield* reached(busy.reached);
        yield* opened.harness.close;

        opened = yield* openChat(storage, setup);
        const submission = Option.getOrThrow(yield* opened.harness.harness.submission(id));
        assert.strictEqual((yield* submission.status).status, "placed");
        yield* opened.harness.harness.resume;
        const settled = yield* settle(submission);
        if (settled.status !== "done" || settled.type !== "input") {
          return assert.fail(`Unexpected ${settled.status}`);
        }
        yield* opened.harness.close;

        opened = yield* openChat(storage, setup);
        const reacquired = Option.getOrThrow(yield* opened.harness.harness.submission(id));
        assert.deepStrictEqual(yield* settle(reacquired), settled);
        const again = yield* opened.root.submit({
          type: "input",
          content: "hi",
          requestId: "print",
        });
        assert.strictEqual(again.id, id);
        yield* opened.harness.close;
      }),
    ),
  );

  it.effect("enables scheduling when a caller submits or waits", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        setup.faux.setResponses([fauxAssistantMessage("answer")]);
        const { harness, root } = yield* openChat(yield* controlledStorage, setup);
        // No resume(): submitting asks for progress.
        const submission = yield* root.submit({ type: "input", content: "hi" });
        assert.strictEqual((yield* settle(submission)).status, "done");
        yield* harness.close;

        const passive = yield* openChat(yield* freshStorage(backend), chatSetup());
        const taskId = yield* passive.root.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            return yield* tx.createTask(
              GenerationTask,
              {},
              { ownership: { kind: "conversation" } },
            );
          }),
        );
        // A committed task alone does not start scheduling; waiting for it does. Pending work gets its turns first,
        // so the check is not just that nothing ran yet.
        yield* flush;
        const pending = Option.getOrThrow(yield* passive.harness.harness.getTask(taskId));
        assert.strictEqual(pending.state.status, "pending");
        const settled = yield* finished(passive.harness.harness.awaitTask(taskId));
        assert.strictEqual(settled.state.status, "terminal");
        yield* passive.harness.close;
      }),
    ),
  );

  it.effect("settles submissions by their current record in the transaction", () =>
    run(backend)(
      Effect.gen(function* () {
        const { harness, root } = yield* openChat(yield* controlledStorage, chatSetup());
        const entry = yield* root.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            return (yield* tx.appendEntry(root.id, { kind: "note" })).id;
          }),
        );
        const create = (record: Parameters<Tx["Service"]["createSubmission"]>[0]) =>
          root.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              return (yield* tx.createSubmission(record)).id;
            }),
          );
        const queued = yield* create({ conversationId: root.id, type: "input", status: "queued" });
        const write = yield* create({ conversationId: root.id, type: "write", status: "queued" });
        const answer = { status: "done", answer: entry } as const;
        const settleIn = (
          id: SubmissionId,
          settlement: Parameters<Tx["Service"]["settleSubmission"]>[1],
        ) =>
          root.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              yield* tx.settleSubmission(id, settlement);
            }),
          );
        assert.include(yield* failureMessage(settleIn(queued, answer)), "is not a placed input");
        assert.include(yield* failureMessage(settleIn(write, answer)), "is not a placed input");
        assert.include(
          yield* failureMessage(
            settleIn(SubmissionId.make(999_999), { status: "unanswered", reason: "x" }),
          ),
          "does not exist",
        );

        // A submission created earlier in the same commit settles; a second settlement leaves the first.
        const placed = yield* root.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            const created = yield* tx.createSubmission({
              conversationId: root.id,
              type: "input",
              status: "placed",
              entry,
            });
            yield* tx.settleSubmission(created.id, answer);
            yield* tx.settleSubmission(created.id, { status: "unanswered", reason: "late" });
            return created.id;
          }),
        );
        const record = yield* Option.getOrThrow(yield* harness.harness.submission(placed)).status;
        assert.strictEqual(record.status, "done");
        assert.strictEqual("entry" in record ? record.entry : undefined, entry);
        assert.strictEqual("answer" in record ? record.answer : undefined, entry);
        yield* harness.close;
      }),
    ),
  );

  it.effect("appends and reads typed entries through tokens", () =>
    run(backend)(
      Effect.gen(function* () {
        const Counter = Entry.make("app.counter", { data: Schema.Struct({ n: Schema.Number }) });
        const Marker = Entry.make("app.marker");
        const { harness, root } = yield* openChat(yield* controlledStorage, chatSetup());
        const inTx = <A, E>(body: (tx: Tx["Service"]) => Effect.Effect<A, E>) =>
          root.commit(
            Effect.gen(function* () {
              return yield* body(yield* Tx);
            }),
          );
        const counter = yield* inTx((tx) => tx.appendEntry(Counter, root.id, { data: { n: 1 } }));
        const n: number = counter.data.n;
        assert.strictEqual(n, 1);
        assert.deepStrictEqual(counter, {
          id: counter.id,
          conversationId: root.id,
          kind: "app.counter",
          data: { n: 1 },
        });
        const marker = yield* inTx((tx) => tx.appendEntry(Marker, root.id, {}));
        assert.strictEqual(marker.kind, "app.marker");
        assert.deepStrictEqual(
          yield* inTx((tx) => tx.entry(Counter, counter.id)),
          Option.some(counter),
        );
        assert.isTrue(Option.isNone(yield* inTx((tx) => tx.entry(Marker, counter.id))));
        assert.isTrue(Option.isNone(yield* inTx((tx) => tx.entry(Counter, EntryId.make(999_999)))));
        assert.isTrue(Counter.is(counter));
        assert.isFalse(AssistantEntry.is(counter));
        assert.deepStrictEqual(
          [UserEntry.kind, AssistantEntry.kind],
          ["core.user", "core.assistant"],
        );
        yield* harness.close;
      }),
    ),
  );

  // A storage that resolves each commit only after a flush: the controlled storage's commit hook flushes before
  // it returns.
  it.effect(
    "resumes a wait only once observeCommits() observers saw the commit that settled it",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const setup = chatSetup();
          setup.faux.setResponses([fauxAssistantMessage("hi")]);
          const storage = yield* controlledStorage;
          storage.intercept.commit = (_batches, next) => Effect.tap(next, () => flush);
          const { harness, root } = yield* openChat(storage, setup);
          const settled = new Set<SubmissionId>();
          yield* harness.session.observeCommits({
            advance: (publication) => {
              for (const change of publication.changes) {
                if (change.type === "submission" && change.value.status === "done") {
                  settled.add(change.value.id);
                }
              }
            },
            end: () => {},
          });
          const submission = yield* root.submit({ type: "input", content: "hello" });
          yield* settle(submission);
          assert.isTrue(settled.has(submission.id));
          yield* harness.close;
        }),
      ),
  );
});

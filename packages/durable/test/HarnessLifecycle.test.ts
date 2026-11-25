// The Harness lifecycle: opening, closing, and what runs or is rejected around them, on every backend.
//
// - Opening is building the Harness: an open that fails is a failed `openHarness`, with no caller context to cancel
//   and no registry subscription to release.
// - The Session does not close storage: "Storage is open/closed" checks are whether the Session has ended
//   (`harness.closed`).
// - Closing is closing the Harness's scope; "a cancelled close" is an interrupted fiber that closes it, and a
//   handler that "ignores its signal" is an uninterruptible region.
// - A reopen is a new Harness over the same storage.
// - `wait` is `Submission.await`, `abortSubmission` is `Submission.withdraw`, and `entries` is a `Stream` collected whole.
import { assert, it } from "@effect/vitest";
import * as Cause from "effect/Cause";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as Predicate from "effect/Predicate";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as AgentTool from "../src/AgentTool.ts";
import { StorageFailure } from "../src/AgentStorage.ts";
import * as Doc from "../src/Doc.ts";
import * as Extension from "../src/Extension.ts";
import type { Conversation } from "../src/Harness.ts";
import * as Hook from "../src/Hook.ts";
import { ConversationId, type EntryId, TaskId } from "../src/Ids.ts";
import * as Task from "../src/Task.ts";
import { fauxAssistantMessage, fauxText, fauxToolCall } from "../src/testing/FauxModel.ts";
import { Tx } from "../src/Tx.ts";
import { chatSetup, openChat } from "./ChatSupport.ts";
import {
  assertNoCallsAfter,
  awaitBounded,
  awaited,
  bounded,
  completed,
  eventually,
  joined,
  openHarness,
  settled,
  type TestHarness,
} from "./HarnessSupport.ts";
import {
  acquire,
  type ControlledStorage,
  controlledStorage,
  describeEachBackend,
  flush,
  recordCalls,
  run,
} from "./SessionSupport.ts";
import { commitWrites } from "../src/testing/CommitWrites.ts";
import type { Report } from "../src/Report.ts";

const Step = Schema.Struct({ phase: Schema.Literal("run") });

/** A one-phase task running `body` and completing with null. */
function oneStep(name: string, body: Effect.Effect<void> = Effect.void) {
  return Task.make({
    name,
    version: 1,
    input: Schema.Null,
    checkpoint: Step,
    result: Schema.Null,
    initial: () => ({ phase: "run" }),
    phases: {
      run: (_task, runtime) =>
        body.pipe(Effect.andThen(runtime.commit(() => Effect.succeed(completed(null))))),
    },
    abort: () => Effect.void,
  });
}

const start = (conversation: Conversation, task: ReturnType<typeof oneStep>, background = false) =>
  conversation.commit(
    Effect.gen(function* () {
      const tx = yield* Tx;
      return yield* tx.createTask(task, null, { ownership: { kind: "conversation" }, background });
    }),
  );

/** Commit a conversation with a `running` task, as a crash leaves it, so open has a reconciliation commit to fail. */
const seedRunningTask = (storage: ControlledStorage) =>
  Effect.gen(function* () {
    const conversationId = ConversationId.make(yield* storage.mintId);
    const id = TaskId.make(yield* storage.mintId);
    yield* commitWrites(storage, [
      { type: "conversation", value: { id: conversationId } },
      {
        type: "task",
        value: {
          id,
          conversationId,
          kind: "test.seeded",
          version: 1,
          input: null,
          background: false,
          abortRequested: false,
          state: { status: "running", checkpoint: { phase: "run" } },
        },
      },
    ]);
  });

/**
 * A fiber that settles once the Session has ended. While it has not settled,
 * code of a joined invocation can still use the Session.
 */
const watchEnd = (h: TestHarness) => Effect.forkChild(h.harness.closed, { startImmediately: true });

/** How an operation settled: `resolved`, `undefined` for an absent lookup, `closed`, or the rendered failure. */
const outcomeOf = <A, E>(operation: Effect.Effect<A, E>) =>
  Effect.exit(operation).pipe(
    Effect.map((exit) => {
      if (Exit.isSuccess(exit)) {
        return Option.isOption(exit.value) && Option.isNone(exit.value) ? "undefined" : "resolved";
      }
      return Predicate.isTagged(Cause.squash(exit.cause), "SessionClosed")
        ? "closed"
        : Cause.pretty(exit.cause);
    }),
  );

const notesDoc = (kind: string) =>
  Doc.make({
    checkpointWhen: () => false,
    kind,
    version: 1,
    scope: "session",
    schema: Schema.Struct({ text: Schema.String }),
    initial: () => ({ text: "" }),
  });

describeEachBackend("Harness open", (backend) => {
  it.effect(
    "closes without a cancelled caller context and rethrows the original error when open fails",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const storage = yield* controlledStorage;
          yield* seedRunningTask(storage);
          const calls = recordCalls(storage);
          const held = yield* storage.holdCommits;
          storage.failNextCommit("disk full");
          const opening = yield* Effect.forkChild(openHarness(storage, []), {
            startImmediately: true,
          });
          yield* bounded(held.entered);
          // Building a layer has no caller context to cancel, so the open is only released.
          yield* held.release;
          const error = yield* Effect.flip(joined(opening));
          assert.strictEqual(error._tag, "StorageFailure");
          assert.strictEqual(error.message, "disk full");
          // The failed open ended the Session: no storage call follows the failed commit.
          const failedAt = calls.length;
          const commits = storage.commits.length;
          yield* assertNoCallsAfter(calls, failedAt);
          assert.lengthOf(storage.commits, commits);
        }),
      ),
  );

  it.effect("rethrows the open error and reports it once, though closing then fails too", () =>
    run(backend)(
      Effect.gen(function* () {
        // The Session does not close storage, so only the open fails here.
        const storage = yield* controlledStorage;
        yield* seedRunningTask(storage);
        const calls = recordCalls(storage);
        storage.failNextCommit("disk full");
        const reports: Array<Report> = [];
        const error = yield* Effect.flip(
          openHarness(storage, [], {
            onReport: (report) =>
              Effect.sync(() => {
                reports.push(report);
              }),
          }),
        );
        assert.strictEqual(error.message, "disk full");
        // The Session ended with the failed open: no storage call follows it.
        yield* assertNoCallsAfter(calls, calls.length);
        // The storage failure that failed open is the cause; the close after it adds no report. Reports are
        // delivered on a fiber of their own, which the flushes above let run.
        assert.lengthOf(reports, 1);
        assert.strictEqual(reports[0]!.kind, "storage_failed");
        const reported = Cause.squash(reports[0]!.cause);
        assert.instanceOf(reported, StorageFailure);
        assert.strictEqual(reported.message, "disk full");
      }),
    ),
  );
});

describeEachBackend("Harness close", (backend) => {
  it.effect("joins a task handler that ignores its signal before closing Storage", () =>
    run(backend)(
      Effect.gen(function* () {
        const storage = yield* controlledStorage;
        const reached = yield* Deferred.make<void>();
        const gate = yield* Deferred.make<void>();
        // A failed assertion must not leave the stubborn handler holding the test's scope open.
        yield* Effect.addFinalizer(() => Deferred.succeed(gate, undefined));
        let readAfterRelease: boolean | undefined;
        let end: Fiber.Fiber<unknown> | undefined;
        const Stubborn = oneStep(
          "test.close-stubborn",
          Effect.uninterruptible(
            Effect.gen(function* () {
              yield* Deferred.succeed(reached, undefined);
              yield* Deferred.await(gate);
              readAfterRelease = end?.pollUnsafe() === undefined;
            }),
          ),
        );
        const h = yield* openHarness(storage, [Stubborn]);
        end = yield* watchEnd(h);
        yield* start(yield* h.harness.ensureRoot(), Stubborn);
        yield* h.harness.resume;
        yield* awaited(reached);
        const closing = yield* Effect.forkChild(h.close, { startImmediately: true });
        assert.isFalse(yield* settled(closing));
        assert.isFalse(yield* settled(end));
        yield* Deferred.succeed(gate, undefined);
        yield* joined(closing);
        assert.isTrue(readAfterRelease);
        assert.deepStrictEqual(yield* joined(end), { reason: "closed" });
      }),
    ),
  );

  // A tool execute and a hook that ignore their signal are uninterruptible regions; whether Storage is open is
  // whether the Session has ended (`watchEnd`). Each case gets a fresh storage of the backend.
  it.effect("joins a tool execute and a hook that ignore their signal before closing Storage", () =>
    Effect.gen(function* () {
      for (const where of ["tool", "hook"] as const) {
        yield* run(backend)(
          Effect.gen(function* () {
            const setup = chatSetup();
            const storage = yield* controlledStorage;
            const reached = yield* Deferred.make<void>();
            const gate = yield* Deferred.make<void>();
            // A failed assertion must not leave the stubborn code holding the test's scope open.
            yield* Effect.addFinalizer(() => Deferred.succeed(gate, undefined));
            let readAfterRelease: boolean | undefined;
            let end: Fiber.Fiber<unknown> | undefined;
            const stubborn = Effect.uninterruptible(
              Effect.gen(function* () {
                yield* Deferred.succeed(reached, undefined);
                yield* Deferred.await(gate);
                readAfterRelease = end?.pollUnsafe() === undefined;
              }),
            );
            const Wait = AgentTool.make("wait", {
              description: "wait",
              parameters: Schema.Struct({}),
              execute: () =>
                (where === "tool" ? stubborn : Effect.void).pipe(Effect.as({ output: [] })),
            });
            const Stubborn = Extension.make({
              name: "test.stubborn",
              tools: [Wait],
              hooks: [
                Hook.generation({
                  beforeRequest: () =>
                    (where === "hook" ? stubborn : Effect.void).pipe(Effect.as(undefined)),
                }),
              ],
            });
            setup.faux.setResponses([
              fauxAssistantMessage([fauxToolCall("wait", {}, { id: "c1" })], {
                stopReason: "toolUse",
              }),
              fauxAssistantMessage([fauxText("done")]),
            ]);
            const { harness: h, root } = yield* openChat(storage, setup, {
              extensions: [Stubborn],
            });
            end = yield* watchEnd(h);
            yield* root.submit({ type: "input", content: "go" });
            yield* awaited(reached);
            const closing = yield* Effect.forkChild(h.close, { startImmediately: true });
            assert.isFalse(yield* settled(closing));
            assert.isFalse(yield* settled(end));
            yield* Deferred.succeed(gate, undefined);
            yield* joined(closing);
            assert.isTrue(readAfterRelease);
            assert.deepStrictEqual(yield* joined(end), { reason: "closed" });
          }),
        );
      }
    }),
  );

  it.effect(
    "lets a new Harness open the same Storage once close resolved: no old invocation code runs",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const storage = yield* controlledStorage;
          const log: Array<string> = [];
          const gate = yield* Deferred.make<void>();
          // A failed assertion must not leave the stubborn handler holding the test's scope open.
          yield* Effect.addFinalizer(() => Deferred.succeed(gate, undefined));
          let generation = 1;
          // The first generation ignores its signal: it awaits its gate whatever close does.
          const Stubborn = oneStep(
            "test.generations",
            Effect.uninterruptible(
              Effect.gen(function* () {
                const mine = generation;
                log.push(`start ${mine}`);
                if (mine === 1) yield* Deferred.await(gate);
                log.push(`end ${mine}`);
              }),
            ),
          );
          const first = yield* openHarness(storage, [Stubborn]);
          const id = yield* start(yield* first.harness.ensureRoot(), Stubborn);
          yield* first.harness.resume;
          yield* eventually(Effect.sync(() => log.length === 1));
          const closing = yield* Effect.forkChild(
            first.close.pipe(Effect.andThen(Effect.sync(() => void log.push("closed")))),
            { startImmediately: true },
          );
          assert.isFalse(yield* settled(closing));
          yield* Deferred.succeed(gate, undefined);
          yield* joined(closing);
          assert.deepStrictEqual(log, ["start 1", "end 1", "closed"]);
          generation = 2;
          const second = yield* openHarness(storage, [Stubborn]);
          yield* second.harness.resume;
          yield* second.harness.awaitTask(id);
          assert.deepStrictEqual(log, ["start 1", "end 1", "closed", "start 2", "end 2"]);
          yield* second.close;
        }),
      ),
  );

  it.effect(
    "keeps shutting down after a cancelled close, and a second close awaits the same shutdown",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const storage = yield* controlledStorage;
          const reached = yield* Deferred.make<void>();
          const gate = yield* Deferred.make<void>();
          // A failed assertion must not leave the stubborn handler holding the test's scope open.
          yield* Effect.addFinalizer(() => Deferred.succeed(gate, undefined));
          const Stubborn = oneStep(
            "test.close-cancelled",
            Effect.uninterruptible(
              Deferred.succeed(reached, undefined).pipe(Effect.andThen(Deferred.await(gate))),
            ),
          );
          const h = yield* openHarness(storage, [Stubborn]);
          const end = yield* watchEnd(h);
          yield* start(yield* h.harness.ensureRoot(), Stubborn);
          yield* h.harness.resume;
          yield* awaited(reached);
          const cancelled = yield* Effect.forkChild(h.close, { startImmediately: true });
          // The close's fiber is interrupted: the shutdown runs on a fiber of its own and the caller only awaits its end
          // (`openHarness` in HarnessSupport.ts), so the interruption ends that wait at once, while the stubborn
          // handler still holds the shutdown.
          yield* Effect.forkChild(Fiber.interrupt(cancelled), { startImmediately: true });
          assert.isTrue(Exit.hasInterrupts(yield* awaitBounded(cancelled)));
          // Admission stays sealed and the invocation still holds the Session open.
          const rejected = yield* Effect.flip(h.harness.commit(Effect.void));
          assert.strictEqual(rejected._tag, "SessionClosed");
          assert.isFalse(yield* settled(end));
          const second = yield* Effect.forkChild(h.close, { startImmediately: true });
          assert.isFalse(yield* settled(second));
          yield* Deferred.succeed(gate, undefined);
          yield* joined(second);
          assert.deepStrictEqual(yield* joined(end), { reason: "closed" });
        }),
      ),
  );

  it.effect("settles durably a commit whose committer was cancelled while it was in Storage", () =>
    run(backend)(
      Effect.gen(function* () {
        const storage = yield* controlledStorage;
        const h = yield* openHarness(storage, []);
        const root = yield* h.harness.ensureRoot();
        const held = yield* storage.holdCommits;
        let staged: EntryId | undefined;
        const committing = yield* Effect.forkChild(
          root.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              const entry = yield* tx.appendEntry(root.id, { kind: "note" });
              staged = entry.id;
              return entry.id;
            }),
          ),
          { startImmediately: true },
        );
        yield* bounded(held.entered);
        // The committer's fiber is interrupted. A commit in storage runs to completion,
        // so the interrupt is forked and settles with it.
        const interrupting = yield* Effect.forkChild(Fiber.interrupt(committing), {
          startImmediately: true,
        });
        yield* held.release;
        yield* joined(interrupting);
        // An interrupted fiber may end without a value, so the ID is the one its callback staged.
        const exit = yield* awaitBounded(committing);
        const id = Exit.isSuccess(exit) ? exit.value : staged;
        // `Conversation.entries` is a Stream (Harness.ts), collected here.
        const entries = yield* bounded(Stream.runCollect(Stream.take(root.entries(), 10)));
        assert.deepStrictEqual(
          entries.map((entry) => entry.id),
          [id],
        );
        yield* h.close;
      }),
    ),
  );

  it.effect("publishes no frame to watches from a commit that settles during close", () =>
    run(backend)(
      Effect.gen(function* () {
        const Notes = notesDoc("test.close-frames");
        const storage = yield* controlledStorage;
        const h = yield* openHarness(storage, []);
        const root = yield* h.harness.ensureRoot();
        yield* h.harness.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            (yield* tx.doc(Notes)).text = "before";
          }),
        );
        const docWatch = (yield* acquire(h.session.watchDoc(Notes))).value;
        const viewWatch = yield* root.watch();
        const graphWatch = yield* h.harness.watchTaskGraph;
        const frames: Array<string> = [];
        const graphWatchFrames = yield* graphWatch.frames.pipe(
          Stream.runForEach(() => Effect.sync(() => void frames.push("graphWatch"))),
          Effect.forkChild,
        );
        const watchFrames = yield* docWatch.frames.pipe(
          Stream.runForEach(() => Effect.sync(() => void frames.push("docWatch"))),
          Effect.forkChild,
        );
        const viewWatchFrames = yield* viewWatch.frames.pipe(
          Stream.runForEach(() => Effect.sync(() => void frames.push("viewWatch"))),
          Effect.forkChild,
        );
        const docValue = docWatch.value;
        const viewValue = viewWatch.value;

        const held = yield* storage.holdCommits;
        const committing = yield* Effect.forkChild(
          h.harness.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              (yield* tx.doc(Notes)).text = "during close";
              yield* tx.appendEntry(root.id, { kind: "note" });
              yield* tx.createTask(oneStep("test.close-graph"), null, {
                ownership: { kind: "conversation" },
                conversationId: root.id,
              });
            }),
          ),
          { startImmediately: true },
        );
        yield* bounded(held.entered);
        const closing = yield* Effect.forkChild(h.close, { startImmediately: true });
        // Closing the Harness's scope reaches the seal a few turns later, and with group commit the writer could publish the held commit first.
        yield* eventually(Effect.sync(() => h.kernel.closing));
        yield* held.release;
        yield* joined(committing);
        yield* joined(closing);
        yield* flush;
        assert.deepStrictEqual(frames, []);
        assert.strictEqual(docWatch.value, docValue);
        assert.strictEqual(viewWatch.value, viewValue);
        assert.deepStrictEqual(graphWatch.value, { tasks: {} });
        // Once closed, the frame streams end with no frame.
        yield* joined(graphWatchFrames);
        yield* joined(watchFrames);
        yield* joined(viewWatchFrames);
        // The commit itself settled.
        assert.isTrue(storage.commits.at(-1)!.some((write) => write.type === "entry"));
      }),
    ),
  );

  it.effect("leaves no subscription behind a watch acquisition cancelled on the line", () =>
    run(backend)(
      Effect.gen(function* () {
        const Notes = notesDoc("test.cancelled-watch");
        const storage = yield* controlledStorage;
        const h = yield* openHarness(storage, []);
        const root = yield* h.harness.ensureRoot();
        yield* h.harness.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            (yield* tx.doc(Notes)).text = "x";
          }),
        );
        // Document observers attach to the document cache, not to commit listeners, so the count is the kernel's
        // attached observers.

        // Cancel a document watch while its acquisition loads the document on the line.
        yield* h.kernel.unloadDocuments;
        const find = yield* storage.holdFindDocument;
        const docWatch = yield* Effect.forkChild(acquire(h.session.watchDoc(Notes)), {
          startImmediately: true,
        });
        yield* bounded(find.entered);
        // A storage call runs to completion once started, so the interruption takes effect after the read.
        const interrupting = yield* Effect.forkChild(Fiber.interrupt(docWatch), {
          startImmediately: true,
        });
        yield* find.release;
        yield* joined(interrupting);
        assert.isTrue(Exit.isFailure(yield* awaitBounded(docWatch)));
        assert.strictEqual(h.kernel.documentCacheSizes.attached, 0);

        // Cancel a view watch while it builds its mount on the line.
        yield* h.kernel.unloadDocuments;
        const viewFind = yield* storage.holdFindDocument;
        const viewWatch = yield* Effect.forkChild(Effect.scoped(root.watch()), {
          startImmediately: true,
        });
        yield* bounded(viewFind.entered);
        const interruptingView = yield* Effect.forkChild(Fiber.interrupt(viewWatch), {
          startImmediately: true,
        });
        yield* viewFind.release;
        yield* joined(interruptingView);
        assert.isTrue(Exit.hasInterrupts(yield* awaitBounded(viewWatch)));
        // No observer kept the mount: the next observer builds a new one, and the one after that another.
        const firstValue = yield* Effect.scoped(
          root.watch().pipe(Effect.map((watch) => watch.value)),
        );
        const secondValue = yield* Effect.scoped(
          root.watch().pipe(Effect.map((watch) => watch.value)),
        );
        assert.notStrictEqual(secondValue, firstValue);
        yield* h.close;
      }),
    ),
  );

  it.effect(
    "rejects conversation and Harness operations once close begins; inspect queued before reports closing",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const storage = yield* controlledStorage;
          const h = yield* openHarness(storage, []);
          const root = yield* h.harness.ensureRoot();
          const entry = yield* root.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              return yield* tx.appendEntry(root.id, { kind: "note" });
            }),
          );
          const submission = yield* root.submit({ type: "write", entry: { kind: "note" } });

          const held = yield* storage.holdCommits;
          const blocking = yield* Effect.forkChild(
            h.harness.commit(
              Effect.gen(function* () {
                const tx = yield* Tx;
                yield* tx.appendEntry(root.id, { kind: "blocker" });
              }),
            ),
            { startImmediately: true },
          );
          yield* bounded(held.entered);
          const queuedInspect = yield* Effect.forkChild(h.harness.inspect, {
            startImmediately: true,
          });
          const closing = yield* Effect.forkChild(h.close, { startImmediately: true });
          // Closing the Harness's scope reaches the seal a few turns later (the line does not wait for the held
          // commit, so the turns run).
          yield* eventually(Effect.sync(() => h.kernel.closing));
          const missing = TaskId.make(1);
          const operations = {
            submit: outcomeOf(root.submit({ type: "input", content: "x" })),
            agent: outcomeOf(root.agent),
            configure: outcomeOf(root.configure({ thinkingLevel: "low" })),
            commit: outcomeOf(root.commit(Effect.void)),
            context: outcomeOf(root.context()),
            entries: outcomeOf(Stream.runCollect(root.entries())),
            fork: outcomeOf(root.fork(entry.id, { ownership: { kind: "ownerless" } })),
            compact: outcomeOf(root.compact()),
            reset: outcomeOf(root.reset()),
            abort: outcomeOf(root.abort()),
            conversationIdle: outcomeOf(root.awaitIdle),
            watch: outcomeOf(Effect.scoped(root.watch())),
            watchTaskGraph: outcomeOf(Effect.scoped(h.harness.watchTaskGraph)),
            status: outcomeOf(submission.status),
            wait: outcomeOf(submission.await),
            abortSubmission: outcomeOf(submission.withdraw),
            root: outcomeOf(h.harness.ensureRoot()),
            conversation: outcomeOf(h.harness.conversation(root.id)),
            createConversation: outcomeOf(
              h.harness.createConversation({ ownership: { kind: "ownerless" } }),
            ),
            getTask: outcomeOf(h.harness.getTask(missing)),
            inspect: outcomeOf(h.harness.inspect),
            submission: outcomeOf(h.harness.submission(submission.id)),
            abortTask: outcomeOf(h.harness.abortTask(missing)),
            waitForTask: outcomeOf(h.harness.awaitTask(missing)),
            harnessIdle: outcomeOf(h.harness.awaitIdle),
            usage: outcomeOf(h.harness.usage),
            // The host additions and the Harness's withdraw by ID.
            submissionByRequest: outcomeOf(h.harness.submissionByRequest(root.id, "r1")),
            withdrawSubmission: outcomeOf(h.harness.withdrawSubmission(submission.id)),
            conversations: outcomeOf(Stream.runCollect(h.harness.conversations())),
          };
          const outcomes: Record<string, string> = {};
          for (const [name, operation] of Object.entries(operations)) {
            outcomes[name] = yield* operation;
          }
          assert.deepStrictEqual(
            outcomes,
            Object.fromEntries(Object.keys(operations).map((name) => [name, "closed"])),
          );
          const resumed = yield* Effect.flip(h.harness.resume);
          assert.strictEqual(resumed._tag, "SessionClosed");
          yield* held.release;
          yield* joined(blocking);
          assert.strictEqual((yield* joined(queuedInspect)).scheduling, "closing");
          yield* joined(closing);
        }),
      ),
  );

  it.effect(
    "completes reads queued at the seal and rejects queued waits and acquisitions that would follow commits",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const Notes = notesDoc("test.queued-at-seal");
          const Absent = notesDoc("test.queued-absent");
          const Pending = oneStep("test.queued-pending");
          const storage = yield* controlledStorage;
          const h = yield* openHarness(storage, []);
          const root = yield* h.harness.ensureRoot();
          yield* h.harness.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              yield* tx.doc(Notes);
            }),
          );
          const taskId = yield* start(root, Pending);
          const write = yield* root.submit({ type: "write", entry: { kind: "note" } });
          yield* write.await;

          const held = yield* storage.holdCommits;
          const blocking = yield* Effect.forkChild(
            h.harness.commit(
              Effect.gen(function* () {
                const tx = yield* Tx;
                yield* tx.appendEntry(root.id, { kind: "blocker" });
              }),
            ),
            { startImmediately: true },
          );
          yield* bounded(held.entered);
          // Each is queued on the line now, before close seals it. A watch is acquired and released in a
          // scope of its own.
          const queue = <A, E>(operation: Effect.Effect<A, E>) =>
            Effect.forkChild(outcomeOf(operation), { startImmediately: true });
          const queued = {
            context: yield* queue(root.context()),
            snapshot: yield* queue(h.session.snapshot(Notes)),
            settledWait: yield* queue(write.await),
            absentWatch: yield* queue(Effect.scoped(h.session.watchDoc(Absent))),
            waitForTask: yield* queue(h.harness.awaitTask(taskId)),
            watchDoc: yield* queue(Effect.scoped(h.session.watchDoc(Notes))),
            watch: yield* queue(Effect.scoped(root.watch())),
            watchTaskGraph: yield* queue(Effect.scoped(h.harness.watchTaskGraph)),
          };
          const closing = yield* Effect.forkChild(h.close, { startImmediately: true });
          yield* held.release;
          yield* joined(blocking);
          const outcomes: Record<string, string> = {};
          for (const [name, outcome] of Object.entries(queued))
            outcomes[name] = yield* joined(outcome);
          assert.deepStrictEqual(outcomes, {
            context: "resolved",
            snapshot: "resolved",
            settledWait: "resolved",
            absentWatch: "undefined",
            waitForTask: "closed",
            watchDoc: "closed",
            watch: "closed",
            watchTaskGraph: "closed",
          });
          yield* joined(closing);
        }),
      ),
  );
});

describeEachBackend("scheduling on a paused Harness", (backend) => {
  /** Open a paused Harness with one background task pending and a queued write submission. */
  const paused = Effect.gen(function* () {
    let ran = false;
    const Marker = oneStep(
      "test.marker",
      Effect.sync(() => {
        ran = true;
      }),
    );
    const h = yield* openHarness(yield* controlledStorage, [Marker]);
    const root = yield* h.harness.ensureRoot();
    // Background, so idle waits and conversation abort leave it alone.
    const id = yield* start(root, Marker, true);
    const submissionId = yield* h.harness.commit(
      Effect.gen(function* () {
        const tx = yield* Tx;
        return (yield* tx.createSubmission({
          conversationId: root.id,
          type: "write",
          status: "queued",
        })).id;
      }),
    );
    return { h, root, id, submissionId, ran: () => ran };
  });

  it.effect("never schedules from a read-only viewer", () =>
    run(backend)(
      Effect.gen(function* () {
        const { h, root, id, submissionId, ran } = yield* paused;
        const Notes = notesDoc("test.viewer-notes");
        yield* h.harness.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            yield* tx.doc(Notes);
          }),
        );
        yield* h.harness.inspect;
        yield* h.harness.getTask(id);
        yield* h.harness.usage;
        yield* h.harness.conversation(root.id);
        yield* h.session.snapshot(Notes);
        yield* (yield* acquire(h.session.watchDoc(Notes))).release;
        const submission = Option.getOrThrow(yield* h.harness.submission(submissionId));
        yield* submission.status;
        yield* root.agent;
        yield* root.context();
        yield* Stream.runCollect(root.entries());
        yield* Effect.scoped(root.watch());
        yield* Effect.scoped(h.harness.watchTaskGraph);
        // `wake` is a read on the line too.
        yield* h.harness.wake;
        yield* flush;
        yield* flush;
        assert.isFalse(ran());
        assert.strictEqual((yield* h.harness.inspect).scheduling, "paused");
        yield* h.close;
      }),
    ),
  );

  it.effect("schedules from every progress call", () =>
    Effect.gen(function* () {
      type Opened = Effect.Success<typeof paused>;
      const calls = {
        submit: ({ root }: Opened) => root.submit({ type: "write", entry: { kind: "note" } }),
        compact: ({ root }: Opened) => root.compact(),
        abort: ({ root }: Opened) => root.abort(),
        conversationIdle: ({ root }: Opened) => root.awaitIdle,
        submissionWait: ({ h, submissionId }: Opened) =>
          h.harness
            .submission(submissionId)
            .pipe(Effect.flatMap((found) => Option.getOrThrow(found).await)),
        waitForTask: ({ h, id }: Opened) => h.harness.awaitTask(id),
        harnessIdle: ({ h }: Opened) => h.harness.awaitIdle,
      };
      for (const [name, call] of Object.entries(calls)) {
        // Each call gets a fresh storage of the backend.
        yield* run(backend)(
          Effect.gen(function* () {
            const opened = yield* paused;
            // A queued write's wait settles only on placement; close rejects it.
            const pending = yield* Effect.forkChild(Effect.ignore(call(opened)), {
              startImmediately: true,
            });
            yield* eventually(Effect.sync(opened.ran)).pipe(
              Effect.catchDefect(() => Effect.die(new Error(`${name} did not enable scheduling`))),
            );
            yield* opened.h.close;
            yield* joined(pending);
          }),
        );
      }
    }),
  );
});

describeEachBackend("registry changes before resume", (backend) => {
  // The registry is static, so installing or replacing a definition after open is a Harness opened over the same
  // storage with other extensions, each before any resume.
  it.effect(
    "runs what the registry holds at resume: a definition installed or replaced after open",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const log: Array<string> = [];
          const define = (label: string) =>
            oneStep(
              "test.late-definition",
              Effect.sync(() => {
                log.push(label);
              }),
            );
          const storage = yield* controlledStorage;
          const first = yield* openHarness(storage, []);
          const root = yield* first.harness.ensureRoot();
          const missing = yield* start(root, define("unused"));
          const blocked = (yield* first.harness.inspect).tasks;
          assert.lengthOf(blocked, 1);
          assert.deepInclude(blocked[0]!.state, { kind: "blocked", reason: "missing_task" });
          yield* first.close;
          const installed = yield* openHarness(storage, [define("v1")]);
          const ready = (yield* installed.harness.inspect).tasks;
          assert.lengthOf(ready, 1);
          assert.strictEqual(ready[0]!.state.kind, "pending");
          // The same task name with another definition, still before resume.
          yield* installed.close;
          const replaced = yield* openHarness(storage, [define("v2")]);
          yield* replaced.harness.resume;
          yield* replaced.harness.awaitTask(missing);
          assert.deepStrictEqual(log, ["v2"]);
          yield* replaced.close;
        }),
      ),
  );
});

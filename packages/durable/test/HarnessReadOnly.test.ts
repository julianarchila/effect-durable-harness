// A read-only Harness, on every backend: a viewer of a session another process drives, such as a synced copy on
// another device.
//
// - Both Harnesses open over the same storage of the backend, each behind a controlled storage of its own. The
//   check that the viewer writes nothing is the viewer's own commits: none reaches storage.
// - Wakes are a host's `SessionWakeups`.
// - The viewer's snapshots come from the `Session` the same layer provides.
// - `resume` on a viewer fails with `ReadOnlySession`.
import { assert, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import { AgentStorage } from "../src/AgentStorage.ts";
import { UserEntry } from "../src/Entry.ts";
import * as Extension from "../src/Extension.ts";
import { ROOT_CONVERSATION_ID, TaskId } from "../src/Ids.ts";
import { LiveDoc } from "../src/Live.ts";
import type { Report } from "../src/Report.ts";
import { SessionWakeups, type Wake } from "../src/SessionWakeups.ts";
import * as Task from "../src/Task.ts";
import { fauxAssistantMessage } from "../src/testing/FauxModel.ts";
import { Tx } from "../src/Tx.ts";
import {
  allEntries,
  type ChatSetup,
  chatSetup,
  FAUX_MODEL,
  openChat,
  unanswered,
  waitFor,
} from "./ChatSupport.ts";
import { completed, openHarness } from "./HarnessSupport.ts";
import { controlledStorage, describeEachBackend, run } from "./SessionSupport.ts";

/** A read-only Harness over the backend's storage, telling `wakes` every wake. */
const openReadOnly = (setup: ChatSetup, wakes: Array<Wake> = []) =>
  Effect.gen(function* () {
    const storage = yield* controlledStorage;
    const harness = yield* openHarness(storage, [], {
      models: setup.faux.layer,
      readOnly: true,
      onReport: (report: Report) =>
        Effect.sync(() => {
          setup.reports.push(report);
        }),
    }).pipe(
      Effect.provideService(SessionWakeups, {
        schedule: (wake) =>
          Effect.sync(() => {
            wakes.push(wake);
          }),
      }),
    );
    return { harness, storage };
  });

/** A session whose root generation runs in another Harness, still open, waiting for the model. */
const runningSession = (setup: ChatSetup, requestId?: string) =>
  Effect.gen(function* () {
    const busy = unanswered();
    setup.faux.setResponses([busy.step]);
    const driver = yield* openChat(yield* controlledStorage, setup, {
      extensions: [Extension.make({ name: "notes", tasks: [Note] })],
    });
    yield* driver.root.submit({ type: "input", content: "slow", requestId });
    yield* waitFor(Deferred.isDone(busy.reached));
    return driver;
  });

const isReadOnly = (error: { readonly _tag: string }) => error._tag === "ReadOnlySession";

/** A task that completes at once. */
const Note = Task.make({
  name: "test.note",
  version: 1,
  input: Schema.Null,
  checkpoint: Schema.Struct({ phase: Schema.Literal("run") }),
  result: Schema.Null,
  initial: () => ({ phase: "run" as const }),
  phases: { run: (_task, runtime) => runtime.commit(() => Effect.succeed(completed(null))) },
  abort: () => Effect.void,
});

describeEachBackend("read-only Harness", (backend) => {
  it.effect("opens a session with work running elsewhere without writing", () =>
    run(backend)(
      Effect.gen(function* () {
        const driver = yield* runningSession(chatSetup());
        const wakes: Array<Wake> = [];

        const viewer = yield* openReadOnly(chatSetup(), wakes);
        const root = yield* viewer.harness.harness.ensureRoot();
        assert.deepStrictEqual(
          (yield* allEntries(root)).map((entry) => entry.kind),
          [UserEntry.kind],
        );
        const live = yield* viewer.harness.session.snapshot(LiveDoc, root.id);
        const runState = Option.getOrUndefined(live)?.run;
        assert.isDefined(runState);
        const task = yield* viewer.harness.harness.getTask(runState!.taskId);
        assert.strictEqual(Option.getOrUndefined(task)?.state.status, "running");
        const inspection = yield* viewer.harness.harness.inspect;
        assert.strictEqual(inspection.scheduling, "paused");
        assert.deepStrictEqual(yield* viewer.harness.harness.wake, {
          nextWakeAt: Option.none(),
          activeWork: false,
        });
        yield* viewer.harness.close;

        assert.deepStrictEqual(viewer.storage.commits, []);
        assert.deepStrictEqual(wakes, []);
        yield* driver.harness.close;
      }),
    ),
  );

  it.effect("refuses writes and scheduling, and stays usable", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        setup.faux.setResponses([fauxAssistantMessage("one")]);
        const driver = yield* openChat(yield* controlledStorage, setup);
        const first = yield* driver.root.submit({ type: "input", content: "hi" });
        yield* first.await;
        yield* driver.harness.close;

        const viewerSetup = chatSetup();
        const viewer = yield* openReadOnly(viewerSetup);
        const harness = viewer.harness.harness;
        const root = yield* harness.ensureRoot();
        const note = yield* Effect.flip(
          root.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              return yield* tx.appendEntry(root.id, { kind: "app.note" });
            }),
          ),
        );
        assert.isTrue(isReadOnly(note));
        assert.isTrue(
          isReadOnly(yield* Effect.flip(root.submit({ type: "input", content: "again" }))),
        );
        assert.isTrue(isReadOnly(yield* Effect.flip(harness.resume)));
        assert.isTrue(isReadOnly(yield* Effect.flip(harness.awaitIdle)));
        assert.isTrue(isReadOnly(yield* Effect.flip(root.abort())));
        assert.isTrue(
          isReadOnly(
            yield* Effect.flip(harness.createConversation({ ownership: { kind: "ownerless" } })),
          ),
        );

        // Still healthy: reads work, and a commit that writes nothing succeeds.
        assert.strictEqual(yield* root.commit(Effect.succeed("nothing")), "nothing");
        assert.lengthOf(yield* allEntries(root), 2);
        yield* viewer.harness.close;
        assert.deepStrictEqual(viewer.storage.commits, []);
        assert.deepStrictEqual(viewerSetup.reports, []);
      }),
    ),
  );

  it.effect("refuses to create the root of an empty storage", () =>
    run(backend)(
      Effect.gen(function* () {
        const viewer = yield* openReadOnly(chatSetup());
        const refused = yield* Effect.flip(
          viewer.harness.harness.ensureRoot({ agent: { model: FAUX_MODEL } }),
        );
        assert.isTrue(isReadOnly(refused));
        yield* viewer.harness.close;
        assert.deepStrictEqual(viewer.storage.commits, []);
        const storage = yield* AgentStorage;
        assert.isTrue(Option.isNone(yield* storage.conversation(ROOT_CONVERSATION_ID)));
      }),
    ),
  );

  it.effect("leaves recovery to the next writable open", () =>
    run(backend)(
      Effect.gen(function* () {
        const driver = yield* runningSession(chatSetup());
        const live = yield* driver.harness.session.snapshot(LiveDoc, driver.root.id);
        const taskId = Option.getOrThrow(live).run!.taskId;

        const viewer = yield* openReadOnly(chatSetup());
        yield* viewer.harness.close;

        // The writable open recovers the task once, as if the viewer had never opened it.
        const writer = yield* openChat(yield* controlledStorage, chatSetup());
        const record = Option.getOrUndefined(yield* writer.harness.harness.getTask(taskId));
        assert.strictEqual(record?.state.status, "pending");
        assert.strictEqual(record?.restarts, 1);
        yield* writer.harness.close;
        yield* driver.harness.close;
      }),
    ),
  );

  // `abortTask` goes through the Session's commit, and `submit` and `submit(tx, draft)` resume first, which a
  // read-only Harness refuses before anything is read.
  it.effect("answers abortTask through the commit, and refuses submissions before reading", () =>
    run(backend)(
      Effect.gen(function* () {
        const driver = yield* runningSession(chatSetup(), "slow-1");
        const live = yield* driver.harness.session.snapshot(LiveDoc, driver.root.id);
        const running = Option.getOrThrow(live).run!.taskId;
        const done = yield* driver.root.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            return yield* tx.createTask(Note, null, { ownership: { kind: "conversation" } });
          }),
        );
        yield* driver.harness.harness.awaitTask(done);

        const viewer = yield* openReadOnly(chatSetup());
        const harness = viewer.harness.harness;
        const root = yield* harness.ensureRoot();
        // A terminal task answers without a write; a missing one does not exist; a mark is a refused write.
        assert.deepStrictEqual(yield* harness.abortTask(done), { _tag: "AlreadyTerminal" });
        const missing = yield* Effect.flip(harness.abortTask(TaskId.make(9_999)));
        assert.strictEqual(missing._tag, "TaskError");
        assert.isTrue(isReadOnly(yield* Effect.flip(harness.abortTask(running))));
        // Neither the admitted request ID nor the busy conversation is answered.
        assert.isTrue(
          isReadOnly(
            yield* Effect.flip(
              root.submit({ type: "input", content: "slow", requestId: "slow-1" }),
            ),
          ),
        );
        assert.isTrue(
          isReadOnly(
            yield* Effect.flip(root.submit({ type: "input", content: "now", whenBusy: "reject" })),
          ),
        );
        const admitted = yield* Effect.flip(
          root.commit(root.admit({ type: "input", content: "in commit", whenBusy: "reject" })),
        );
        assert.isTrue(isReadOnly(admitted));
        yield* viewer.harness.close;
        assert.deepStrictEqual(viewer.storage.commits, []);
        yield* driver.harness.close;
      }),
    ),
  );
});

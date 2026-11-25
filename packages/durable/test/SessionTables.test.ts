// Session tables, on every backend. `WorkTask` carries only identity; `setTask`
// goes through the kernel's `commitWith`.
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Doc from "../src/Doc.ts";
import { ConversationId, EntryId, TaskId } from "../src/Ids.ts";
import type { SessionKernel } from "../src/internal/session.ts";
import type { Message } from "../src/Message.ts";
import type { TaskRecord } from "../src/Records.ts";
import * as Task from "../src/Task.ts";
import { Tx } from "../src/Tx.ts";
import {
  createConversation,
  describeEachBackend,
  documentChanges,
  failureMessage,
  openTestSession,
  run,
} from "./SessionSupport.ts";

const WorkTask = Task.make({
  name: "test.work",
  version: 1,
  input: Schema.Struct({ path: Schema.String }),
  checkpoint: Schema.Struct({ phase: Schema.Literal("start") }),
  result: Schema.Struct({ ok: Schema.Boolean }),
  initial: () => ({ phase: "start" }),
  phases: { start: () => Effect.void },
  abort: () => Effect.void,
});

const Progress = Schema.Struct({ lines: Schema.Array(Schema.String) });
const ProgressDoc = Doc.make({
  checkpointWhen: () => false,
  kind: "test.progress",
  version: 1,
  scope: "task",
  schema: Progress,
  initial: () => ({ lines: [] }),
});
const StepDoc = Doc.family({
  checkpointWhen: () => false,
  kind: "test.step",
  version: 1,
  family: true,
  scope: "task",
  schema: Progress,
  initial: (_seed: null) => ({ lines: [] }),
});

const NotesDoc = Doc.make({
  checkpointWhen: () => false,
  kind: "test.notes",
  version: 1,
  scope: "conversation",
  history: "rewindable",
  fork: "asOf",
  schema: Schema.Struct({ text: Schema.String }),
  initial: () => ({ text: "" }),
});

const terminal = (task: TaskRecord): TaskRecord => ({
  id: task.id,
  conversationId: task.conversationId,
  kind: task.kind,
  version: task.version,
  input: task.input,
  background: task.background,
  abortRequested: task.abortRequested,
  state: { status: "terminal", outcome: { status: "completed", result: { ok: true } } },
});

const createTask = (session: SessionKernel, conversationId: ConversationId, withDocument = false) =>
  session.commit(
    Effect.gen(function* () {
      const tx = yield* Tx;
      const taskId = yield* tx.createTask(
        WorkTask,
        { path: "a" },
        { ownership: { kind: "conversation" }, conversationId },
      );
      if (withDocument) (yield* tx.doc(ProgressDoc, taskId)).lines.push("started");
      return taskId;
    }),
  );

/** The committed task, read through a transaction with `tx.task`. */
const readTask = (tx: Tx["Service"], id: TaskId) =>
  tx.task(id).pipe(Effect.map((task) => Option.getOrThrow(task)));

describeEachBackend("Session transaction tables", (backend) => {
  it.effect("allows table reads only before the first table write", () =>
    run(backend)(
      Effect.gen(function* () {
        const { session } = yield* openTestSession();
        const conversationId = yield* createConversation(session);
        yield* session.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            assert.deepStrictEqual(Option.getOrUndefined(yield* tx.conversation(conversationId)), {
              id: conversationId,
            });
            const conversations = yield* tx.scanConversations({}, 1);
            assert.deepStrictEqual(conversations.items, [{ id: conversationId }]);
            assert.deepStrictEqual((yield* tx.scanTasks({ conversationId }, 10)).items, []);
            assert.deepStrictEqual((yield* tx.scanEntries({ conversationId }, 10)).items, []);
            yield* tx.appendEntry(conversationId, { kind: "note" });
            assert.strictEqual(
              (yield* Effect.flip(tx.conversation(conversationId)))._tag,
              "ReadAfterWrite",
            );
            const taskRead = yield* Effect.flip(tx.task(TaskId.make(1)));
            assert.strictEqual(
              taskRead.message,
              "Tx.task() cannot read tables after the first table write",
            );
            assert.strictEqual(
              (yield* Effect.flip(tx.entry(EntryId.make(1))))._tag,
              "ReadAfterWrite",
            );
            assert.strictEqual(
              (yield* Effect.flip(tx.scanConversations({}, 10)))._tag,
              "ReadAfterWrite",
            );
            assert.strictEqual(
              (yield* Effect.flip(tx.scanEntries({ conversationId }, 10)))._tag,
              "ReadAfterWrite",
            );
            // Document access remains available after table writes.
            (yield* tx.doc(NotesDoc, conversationId)).text = "after write";
          }),
        );
        assert.deepStrictEqual(
          Option.getOrUndefined(yield* session.snapshot(NotesDoc, conversationId)),
          {
            text: "after write",
          },
        );
      }),
    ),
  );

  it.effect("passes caller-selected limits and cursors through table scans", () =>
    run(backend)(
      Effect.gen(function* () {
        const { session } = yield* openTestSession();
        const ids = [
          yield* createConversation(session),
          yield* createConversation(session),
          yield* createConversation(session),
        ];
        yield* session.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            const first = yield* tx.scanConversations({}, 2);
            assert.deepStrictEqual(
              first.items.map(({ id }) => id),
              ids.slice(0, 2),
            );
            assert.isTrue(Option.isSome(first.next));
            const second = yield* tx.scanConversations({}, 2, Option.getOrUndefined(first.next));
            assert.deepStrictEqual(
              second.items.map(({ id }) => id),
              ids.slice(2),
            );
            assert.isTrue(Option.isNone(second.next));
          }),
        );
      }),
    ),
  );

  it.effect("treats synchronous setTask as the first table write", () =>
    run(backend)(
      Effect.gen(function* () {
        const { session } = yield* openTestSession();
        const conversationId = yield* createConversation(session);
        const taskId = yield* createTask(session, conversationId);
        yield* session.commitWith((tx) =>
          Effect.gen(function* () {
            yield* tx.setTask(yield* readTask(tx.service, taskId));
            assert.strictEqual(
              (yield* Effect.flip(tx.service.task(taskId)))._tag,
              "ReadAfterWrite",
            );
          }),
        );
      }),
    ),
  );

  it.effect("creates conversations, entries, and tasks with minted IDs", () =>
    run(backend)(
      Effect.gen(function* () {
        const { session, storage, publications } = yield* openTestSession();
        const created = yield* session.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            const conversation = yield* tx.createConversation({ ownership: { kind: "ownerless" } });
            const first = yield* tx.appendEntry(conversation.id, { kind: "note", data: "one" });
            const headed = yield* tx.appendEntry(conversation.id, {
              kind: "summary",
              head: "self",
            });
            const task = yield* tx.createTask(
              WorkTask,
              { path: "x" },
              {
                ownership: { kind: "conversation" },
                conversationId: conversation.id,
                background: true,
              },
            );
            return { conversation, first, headed, task };
          }),
        );
        const ids = [created.conversation.id, created.first.id, created.headed.id, created.task];
        assert.strictEqual(new Set(ids).size, 4);
        assert.strictEqual(created.headed.head, created.headed.id);
        assert.deepStrictEqual(created.first, {
          id: created.first.id,
          conversationId: created.conversation.id,
          kind: "note",
          data: "one",
        });
        assert.deepStrictEqual(
          Option.getOrUndefined(yield* storage.entry(created.headed.id))?.entry,
          created.headed,
        );
        assert.deepStrictEqual(Option.getOrUndefined(yield* storage.task(created.task)), {
          id: created.task,
          conversationId: created.conversation.id,
          kind: "test.work",
          version: 1,
          input: { path: "x" },
          background: true,
          abortRequested: false,
          state: { status: "pending", checkpoint: { phase: "start" } },
        });
        const changes = publications.at(-1)!.changes;
        assert.lengthOf(changes, 4);
        const admitted = storage.admittedCommits.at(-1)!;
        for (const change of changes) assert.isTrue(admitted.some((write) => write === change));
        assert.sameMembers(
          changes.map((change) => change.type),
          ["conversation", "entry", "entry", "task"],
        );
        const conversation = changes.find((change) => change.type === "conversation")!;
        if (conversation.type !== "conversation") throw new Error("expected a conversation");
        assert.strictEqual(conversation.value, created.conversation);
        const entries = changes.flatMap((change) =>
          change.type === "entry" ? [change.value] : [],
        );
        assert.include(entries, created.first);
        assert.include(entries, created.headed);
        assert.include(
          yield* failureMessage(
            session.commit(
              Effect.gen(function* () {
                const tx = yield* Tx;
                yield* tx.createTask(
                  WorkTask,
                  { path: "x" },
                  { ownership: { kind: "conversation" } },
                );
              }),
            ),
          ),
          "requires options.conversationId",
        );
        assert.include(
          yield* failureMessage(
            session.commit(
              Effect.gen(function* () {
                const tx = yield* Tx;
                yield* tx.appendEntry(ConversationId.make(12345), { kind: "note" });
              }),
            ),
          ),
          "Conversation 12345 does not exist",
        );
      }),
    ),
  );

  it.effect("creates a conversation with an explicitly staged task owner", () =>
    run(backend)(
      Effect.gen(function* () {
        const { session, storage } = yield* openTestSession();
        const parentId = yield* createConversation(session);
        const created = yield* session.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            const supervisorId = yield* tx.createTask(
              WorkTask,
              { path: "background" },
              { ownership: { kind: "conversation" }, conversationId: parentId, background: true },
            );
            const child = yield* tx.createConversation({
              ownership: { kind: "task", taskId: supervisorId },
            });
            return { supervisorId, child };
          }),
        );

        assert.deepStrictEqual(created.child.owner, {
          conversationId: parentId,
          taskId: created.supervisorId,
        });
        assert.deepStrictEqual(
          Option.getOrUndefined(yield* storage.conversation(created.child.id)),
          created.child,
        );

        assert.include(
          yield* failureMessage(
            session.commitWith((tx) =>
              Effect.gen(function* () {
                const supervisor = yield* readTask(tx.service, created.supervisorId);
                yield* tx.setTask({ ...supervisor, conversationId: created.child.id });
              }),
            ),
          ),
          `Task ${created.supervisorId} cannot change conversations`,
        );
        assert.strictEqual(
          Option.getOrUndefined(yield* storage.task(created.supervisorId))?.conversationId,
          parentId,
        );
      }),
    ),
  );

  it.effect("rejects missing, terminal, and abort-marked conversation owners atomically", () =>
    run(backend)(
      Effect.gen(function* () {
        const { session, storage } = yield* openTestSession();
        const parentId = yield* createConversation(session);
        let movedStagedTaskId: TaskId | undefined;
        assert.include(
          yield* failureMessage(
            session.commitWith((tx) =>
              Effect.gen(function* () {
                const taskId = yield* tx.service.createTask(
                  WorkTask,
                  { path: "move" },
                  { ownership: { kind: "conversation" }, conversationId: parentId },
                );
                movedStagedTaskId = taskId;
                yield* tx.setTask({
                  id: taskId,
                  conversationId: ConversationId.make(998),
                  kind: WorkTask.definition.name,
                  version: WorkTask.definition.version,
                  input: { path: "move" },
                  background: false,
                  abortRequested: false,
                  state: { status: "pending", checkpoint: { phase: "start" } },
                });
              }),
            ),
          ),
          "cannot change conversations",
        );
        assert.isDefined(movedStagedTaskId);
        assert.isTrue(Option.isNone(yield* storage.task(movedStagedTaskId!)));

        const missingTaskId = TaskId.make(999);
        assert.include(
          yield* failureMessage(
            session.commit(
              Effect.gen(function* () {
                const tx = yield* Tx;
                yield* tx.createConversation({
                  ownership: { kind: "task", taskId: missingTaskId },
                });
              }),
            ),
          ),
          "Conversation owner task 999 does not exist",
        );

        let rejectedChildId: ConversationId | undefined;
        assert.include(
          yield* failureMessage(
            session.commitWith((tx) =>
              Effect.gen(function* () {
                const supervisorId = yield* tx.service.createTask(
                  WorkTask,
                  { path: "aborting" },
                  { ownership: { kind: "conversation" }, conversationId: parentId },
                );
                rejectedChildId = (yield* tx.service.createConversation({
                  ownership: { kind: "task", taskId: supervisorId },
                })).id;
                yield* tx.setTask({
                  id: supervisorId,
                  conversationId: parentId,
                  kind: WorkTask.definition.name,
                  version: WorkTask.definition.version,
                  input: { path: "aborting" },
                  background: false,
                  abortRequested: true,
                  state: { status: "pending", checkpoint: { phase: "start" } },
                });
              }),
            ),
          ),
          "is abort-marked",
        );
        assert.isDefined(rejectedChildId);
        assert.isTrue(Option.isNone(yield* storage.conversation(rejectedChildId!)));

        assert.include(
          yield* failureMessage(
            session.commitWith((tx) =>
              Effect.gen(function* () {
                const supervisorId = yield* tx.service.createTask(
                  WorkTask,
                  { path: "terminal" },
                  { ownership: { kind: "conversation" }, conversationId: parentId },
                );
                yield* tx.service.createConversation({
                  ownership: { kind: "task", taskId: supervisorId },
                });
                yield* tx.setTask({
                  id: supervisorId,
                  conversationId: parentId,
                  kind: WorkTask.definition.name,
                  version: WorkTask.definition.version,
                  input: { path: "terminal" },
                  background: false,
                  abortRequested: false,
                  state: {
                    status: "terminal",
                    outcome: { status: "completed", result: { ok: true } },
                  },
                });
              }),
            ),
          ),
          "is terminal",
        );

        const terminalOwnerId = yield* createTask(session, parentId);
        yield* session.commitWith((tx) =>
          Effect.gen(function* () {
            yield* tx.setTask(terminal(yield* readTask(tx.service, terminalOwnerId)));
          }),
        );
        assert.include(
          yield* failureMessage(
            session.commit(
              Effect.gen(function* () {
                const tx = yield* Tx;
                yield* tx.createConversation({
                  ownership: { kind: "task", taskId: terminalOwnerId },
                });
              }),
            ),
          ),
          "is terminal",
        );
      }),
    ),
  );

  it.effect("takes ownership of table JSON and rejects non-strict values", () =>
    run(backend)(
      Effect.gen(function* () {
        const { session, storage } = yield* openTestSession();
        const conversationId = yield* createConversation(session);
        const payload = { nested: { value: 1 } };
        const entry = yield* session.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            const created = yield* tx.appendEntry(conversationId, { kind: "data", data: payload });
            payload.nested.value = 2;
            return created;
          }),
        );
        assert.deepStrictEqual(Option.getOrUndefined(yield* storage.entry(entry.id))?.entry.data, {
          nested: { value: 1 },
        });
        const omitted = yield* session.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            return yield* tx.appendEntry(conversationId, { kind: "omitted", data: undefined });
          }),
        );
        assert.isFalse(Object.hasOwn(omitted, "data"));
        assert.isFalse(
          Object.hasOwn(Option.getOrThrow(yield* storage.entry(omitted.id)).entry, "data"),
        );

        const commits = storage.commits.length;
        const failure = yield* Effect.flip(
          session.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              yield* tx.appendEntry(conversationId, { kind: "invalid", data: Number.NaN });
            }),
          ),
        );
        assert.strictEqual(failure._tag, "CommitRejected");
        assert.include(failure.message, "strict JSON");
        assert.strictEqual(storage.commits.length, commits);
      }),
    ),
  );

  // `model` is typed as model `Message`s, not only strict JSON, so a malformed message is rejected.
  it.effect("rejects an entry whose model is not model messages", () =>
    run(backend)(
      Effect.gen(function* () {
        const { session, storage } = yield* openTestSession();
        const conversationId = yield* createConversation(session);
        const commits = storage.commits.length;
        const failure = yield* Effect.flip(
          session.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              const model: ReadonlyArray<Message> = [
                // SAFETY: deliberately not a message's content, as an untyped caller could write.
                { role: "user", content: 42 as never, timestamp: 1 },
              ];
              yield* tx.appendEntry(conversationId, { kind: "invalid", model });
            }),
          ),
        );
        assert.strictEqual(failure._tag, "CommitRejected");
        assert.include(failure.message, "model messages");
        assert.strictEqual(storage.commits.length, commits);
      }),
    ),
  );

  it.effect("replaces task records completely", () =>
    run(backend)(
      Effect.gen(function* () {
        const { session, storage } = yield* openTestSession();
        const conversationId = yield* createConversation(session);
        const taskId = yield* createTask(session, conversationId);
        yield* session.commitWith((tx) =>
          Effect.gen(function* () {
            const task = yield* readTask(tx.service, taskId);
            yield* tx.setTask({
              id: task.id,
              conversationId: task.conversationId,
              kind: task.kind,
              version: task.version,
              input: task.input,
              background: task.background,
              abortRequested: task.abortRequested,
              state: { status: "running", checkpoint: { phase: "next", step: 2 } },
              memos: { choice: "b" },
            });
          }),
        );
        assert.deepInclude(Option.getOrThrow(yield* storage.task(taskId)), {
          state: { status: "running", checkpoint: { phase: "next", step: 2 } },
          memos: { choice: "b" },
        });
      }),
    ),
  );

  it.effect("creates a task and then its document in one transaction without ReadAfterWrite", () =>
    run(backend)(
      Effect.gen(function* () {
        const { session, publications } = yield* openTestSession();
        const conversationId = yield* createConversation(session);
        const taskId = yield* session.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            const createdTaskId = yield* tx.createTask(
              WorkTask,
              { path: "a" },
              { ownership: { kind: "conversation" }, conversationId },
            );
            // Validation uses the candidate task record, not a caller table read.
            (yield* tx.doc(ProgressDoc, createdTaskId)).lines.push("created");
            (yield* tx.doc(StepDoc, createdTaskId, "one", null)).lines.push("step");
            return createdTaskId;
          }),
        );
        assert.deepStrictEqual(
          Option.getOrUndefined(yield* session.snapshot(ProgressDoc, taskId)),
          {
            lines: ["created"],
          },
        );
        const publication = publications.at(-1)!;
        const documents = documentChanges(publication);
        assert.isTrue(
          publication.changes.some(
            (change) => change.type === "task" && change.value.id === taskId,
          ),
        );
        assert.lengthOf(documents, 2);
        // Task documents derive their conversation from the task record.
        for (const document of documents)
          assert.strictEqual(document.conversationId, conversationId);

        yield* session.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            yield* tx.createConversation({ ownership: { kind: "ownerless" } });
            (yield* tx.doc(ProgressDoc, taskId)).lines.push("committed task");
          }),
        );
        assert.strictEqual(
          documentChanges(publications.at(-1)!)[0]!.conversationId,
          conversationId,
        );
      }),
    ),
  );

  it.effect("rejects task documents after a terminal candidate", () =>
    run(backend)(
      Effect.gen(function* () {
        const { session } = yield* openTestSession();
        const conversationId = yield* createConversation(session);
        const taskId = yield* createTask(session, conversationId, true);
        yield* session.commitWith((tx) =>
          Effect.gen(function* () {
            const task = yield* readTask(tx.service, taskId);
            const progress = yield* tx.service.doc(ProgressDoc, taskId);
            yield* tx.setTask(terminal(task));
            assert.include(
              yield* failureMessage(tx.service.doc(ProgressDoc, taskId)),
              `Task ${taskId} is terminal`,
            );
            assert.include(
              yield* failureMessage(tx.service.doc(StepDoc, taskId, "late", null)),
              `Task ${taskId} is terminal`,
            );
            assert.include(yield* failureMessage(tx.setTask(task)), "terminal candidate");
            progress.lines.push("final");
          }),
        );
        assert.include(
          yield* failureMessage(
            session.commit(
              Effect.gen(function* () {
                const tx = yield* Tx;
                yield* tx.doc(ProgressDoc, taskId);
              }),
            ),
          ),
          `Task ${taskId} is terminal`,
        );
      }),
    ),
  );

  it.effect(
    "retires task documents at terminal settlement, including documents created in the same transaction",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const { session, storage, publications } = yield* openTestSession();
          const conversationId = yield* createConversation(session);
          const taskId = yield* createTask(session, conversationId, true);
          yield* session.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              (yield* tx.doc(StepDoc, taskId, "committed", null)).lines.push("x");
            }),
          );
          const published = publications.length;
          yield* session.commitWith((tx) =>
            Effect.gen(function* () {
              const task = yield* readTask(tx.service, taskId);
              (yield* tx.service.doc(StepDoc, taskId, "new", null)).lines.push(
                "created then retired",
              );
              yield* tx.setTask(terminal(task));
            }),
          );
          const writes = storage.commits.at(-1)!;
          assert.lengthOf(writes, 5);
          assert.lengthOf(
            writes.filter((write) => write.type === "task"),
            1,
          );
          const creations = writes.flatMap((write) =>
            write.type === "document.create" ? [write] : [],
          );
          const retirements = writes.flatMap((write) =>
            write.type === "document.retire" ? [write] : [],
          );
          assert.lengthOf(creations, 1);
          assert.lengthOf(retirements, 3);
          assert.include(
            retirements.map((write) => write.id),
            creations[0]!.record.id,
          );
          assert.strictEqual(publications.length, published + 1);
          const publication = publications.at(-1)!;
          const documents = documentChanges(publication);
          assert.lengthOf(
            publication.changes.filter((change) => change.type === "task"),
            1,
          );
          assert.deepStrictEqual(
            documents.map((document) => document.value),
            [null, null, null],
          );
          for (const document of documents) assert.deepStrictEqual(document.ops, []);
          for (const document of documents)
            assert.strictEqual(document.conversationId, conversationId);
          assert.isTrue(Option.isNone(yield* session.snapshot(ProgressDoc, taskId)));
          assert.isTrue(Option.isNone(yield* session.snapshot(StepDoc, taskId, "committed")));
          const alive = yield* storage.scanDocuments(
            { scope: { kind: "task", taskId }, at: "current" },
            10,
          );
          assert.deepStrictEqual(alive.items, []);
          assert.include(
            yield* failureMessage(
              session.commitWith((tx) =>
                Effect.gen(function* () {
                  yield* tx.setTask(terminal(yield* readTask(tx.service, taskId)));
                }),
              ),
            ),
            `Task ${taskId} is already terminal`,
          );
        }),
      ),
  );

  it.effect("validates document owners", () =>
    run(backend)(
      Effect.gen(function* () {
        const { session } = yield* openTestSession();
        assert.include(
          yield* failureMessage(
            session.commit(
              Effect.gen(function* () {
                const tx = yield* Tx;
                yield* tx.doc(ProgressDoc, TaskId.make(4242));
              }),
            ),
          ),
          "Task 4242 does not exist",
        );
        assert.include(
          yield* failureMessage(
            session.commit(
              Effect.gen(function* () {
                const tx = yield* Tx;
                yield* tx.doc(NotesDoc, ConversationId.make(4242));
              }),
            ),
          ),
          "Conversation 4242 does not exist",
        );
      }),
    ),
  );
});

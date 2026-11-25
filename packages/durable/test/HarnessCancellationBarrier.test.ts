// Abort and restart marks as a barrier: a marked task does not run on while the cascade that marks it is
// uncommitted, and marks combine down the task tree.
//
// A Session is a Session kernel over the same storage, in a scope closed before the Harness opens; raw commits and
// task reads are the `AgentStorage` calls themselves.
import { assert, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import type { StorageWrite } from "../src/AgentStorage.ts";
import type { TaskId } from "../src/Ids.ts";
import type { TaskRecord } from "../src/Records.ts";
import * as Task from "../src/Task.ts";
import { Tx } from "../src/Tx.ts";
import { eventually, openHarness } from "./HarnessSupport.ts";
import {
  type ControlledStorage,
  controlledStorage,
  describeEachBackend,
  createConversation,
  flush,
  run,
  withSession,
} from "./SessionSupport.ts";
import { commitWrites } from "../src/testing/CommitWrites.ts";

const Work = Schema.Struct({ phase: Schema.Literal("work") });

/** Runs until aborted; its abort handler ends it `aborted`. */
const Dormant = Task.make({
  name: "test.dormant",
  version: 1,
  input: Schema.Null,
  checkpoint: Work,
  result: Schema.Null,
  initial: () => ({ phase: "work" }),
  phases: {
    // Blocks until the invocation is interrupted.
    work: () => Effect.never,
  },
  abort: (_task, runtime) =>
    runtime.commit(() =>
      Effect.succeed({ status: "terminal", outcome: { status: "aborted" } } as const),
    ),
});

interface HoldMarks {
  target: TaskId | undefined;
  readonly held: Deferred.Deferred<void>;
  readonly release: Deferred.Deferred<void>;
}

/**
 * Holds every commit that abort-marks `target`, while set, until released. Here an
 * intercept of the controlled storage's commit.
 */
const holdMarks = (storage: ControlledStorage) =>
  Effect.gen(function* () {
    const marks: HoldMarks = {
      target: undefined,
      held: yield* Deferred.make<void>(),
      release: yield* Deferred.make<void>(),
    };
    storage.intercept.commit = (batches, next) =>
      batches
        .flat()
        .some(
          (write) =>
            write.type === "task" && write.value.id === marks.target && write.value.abortRequested,
        )
        ? Deferred.succeed(marks.held, undefined).pipe(
            Effect.andThen(Deferred.await(marks.release)),
            Effect.andThen(next),
          )
        : next;
    return marks;
  });

const storedTask = (storage: ControlledStorage, id: TaskId) =>
  storage.task(id).pipe(Effect.map(Option.getOrThrow));

/** A raw storage write that replaces a task record; the test builds states storage accepts as they are. */
const taskWrite = (value: TaskRecord): StorageWrite => ({ type: "task", value });

describeEachBackend("work below a cancelled owner", (backend) => {
  it.effect("does not start its next phase while the cascade that marks it has not committed", () =>
    run(backend)(
      Effect.gen(function* () {
        const storage = yield* controlledStorage;
        const marks = yield* holdMarks(storage);
        const entered = yield* Deferred.make<void>();
        const advance = yield* Deferred.make<void>();
        let ranNext = false;
        const Child = Task.make({
          name: "test.child",
          version: 1,
          input: Schema.Null,
          checkpoint: Schema.Union([
            Schema.Struct({ phase: Schema.Literal("first") }),
            Schema.Struct({ phase: Schema.Literal("next") }),
          ]),
          result: Schema.Null,
          initial: () => ({ phase: "first" }),
          phases: {
            first: (_task, runtime) =>
              Effect.gen(function* () {
                yield* Deferred.succeed(entered, undefined);
                yield* Deferred.await(advance);
                yield* runtime.commit(() =>
                  Effect.succeed({ status: "running", checkpoint: { phase: "next" } } as const),
                );
              }),
            next: () =>
              Effect.gen(function* () {
                ranNext = true;
                return yield* Effect.never;
              }),
          },
          abort: (_task, runtime) =>
            runtime.commit(() =>
              Effect.succeed({ status: "terminal", outcome: { status: "aborted" } } as const),
            ),
        });
        const harness = yield* openHarness(storage, [Dormant, Child]);
        const root = yield* harness.harness.ensureRoot();
        const parent = yield* root.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            return yield* tx.createTask(Dormant, null, { ownership: { kind: "conversation" } });
          }),
        );
        const child = yield* root.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            return yield* tx.createTask(Child, null, {
              ownership: { kind: "task", taskId: parent },
            });
          }),
        );
        marks.target = child;
        yield* Effect.gen(function* () {
          yield* harness.harness.resume;
          yield* Deferred.await(entered);
          const aborting = yield* Effect.forkChild(harness.harness.abortTask(parent));
          yield* Deferred.await(marks.held);
          // The parent's request is durable, the cascade's mark of the child is not: its phase must not advance.
          yield* Deferred.succeed(advance, undefined);
          for (let i = 0; i < 5; i++) yield* flush;
          assert.isFalse(ranNext);
          yield* Deferred.succeed(marks.release, undefined);
          assert.strictEqual(
            (yield* harness.harness.awaitTask(child)).state.outcome.status,
            "aborted",
          );
          yield* Fiber.join(aborting);
          assert.isFalse(ranNext);
        }).pipe(
          Effect.ensuring(
            Deferred.succeed(advance, undefined).pipe(
              Effect.andThen(Deferred.succeed(marks.release, undefined)),
              Effect.andThen(harness.close),
            ),
          ),
        );
      }),
    ),
  );

  it.effect("lets a failFast request in the same pass override a restart mark", () =>
    run(backend)(
      Effect.gen(function* () {
        const storage = yield* controlledStorage;
        const { parent, failed, child } = yield* withSession(storage, (session) =>
          Effect.gen(function* () {
            const conversationId = yield* createConversation(session);
            const create = (options: Task.TaskOptions) =>
              session.commit(
                Effect.gen(function* () {
                  const tx = yield* Tx;
                  return yield* tx.createTask(Dormant, null, options);
                }),
              );
            const parent = yield* create({
              conversationId,
              ownership: { kind: "conversation" },
            });
            const failed = yield* create({ ownership: { kind: "task", taskId: parent } });
            const child = yield* create({ ownership: { kind: "task", taskId: parent } });
            return { parent, failed, child };
          }),
        );
        // A restart-abandoned owner that waits failFast on a failed child and a live one.
        const owner = yield* storedTask(storage, parent);
        const failedRecord = yield* storedTask(storage, failed);
        const waiting = {
          status: "waiting",
          checkpoint: { phase: "work" },
          on: [failed, child],
          policy: "failFast",
        } as const;
        const failure = { status: "failed", error: { message: "failed" } } as const;
        yield* commitWrites(storage, [
          taskWrite({ ...owner, abortRequested: true, abortReason: "restart", state: waiting }),
          taskWrite({ ...failedRecord, state: { status: "terminal", outcome: failure } }),
        ]);
        // The child's definition is missing: a restart mark would keep it waiting, a request orphans it.
        const harness = yield* openHarness(storage, []);
        yield* harness.harness.resume;
        yield* eventually(
          harness.harness
            .getTask(child)
            .pipe(
              Effect.map((record) => Option.getOrUndefined(record)?.state.status === "terminal"),
            ),
        );
        assert.containSubset(Option.getOrUndefined(yield* harness.harness.getTask(child))?.state, {
          outcome: { status: "orphaned", reason: "missing_task" },
        });
        yield* harness.close;
      }),
    ),
  );

  it.effect("upgrades restart marks down the tree when an abort is requested above them", () =>
    run(backend)(
      Effect.gen(function* () {
        const storage = yield* controlledStorage;
        const Missing = Task.make({ ...Dormant.definition, name: "test.missing" });
        const { top, middle, leaf } = yield* withSession(storage, (session) =>
          Effect.gen(function* () {
            const conversationId = yield* createConversation(session);
            const top = yield* session.commit(
              Effect.gen(function* () {
                const tx = yield* Tx;
                return yield* tx.createTask(Dormant, null, {
                  conversationId,
                  ownership: { kind: "conversation" },
                });
              }),
            );
            const middle = yield* session.commit(
              Effect.gen(function* () {
                const tx = yield* Tx;
                return yield* tx.createTask(Dormant, null, {
                  ownership: { kind: "task", taskId: top },
                  abandonOnRestart: true,
                });
              }),
            );
            const leaf = yield* session.commit(
              Effect.gen(function* () {
                const tx = yield* Tx;
                return yield* tx.createTask(Missing, null, {
                  ownership: { kind: "task", taskId: middle },
                });
              }),
            );
            return { top, middle, leaf };
          }),
        );
        // Only Dormant is registered: the leaf waits for its definition under the restart cascade.
        const harness = yield* openHarness(storage, [Dormant]);
        const getTask = (id: TaskId) =>
          harness.harness.getTask(id).pipe(Effect.map(Option.getOrUndefined));
        yield* harness.harness.resume;
        yield* eventually(
          getTask(leaf).pipe(Effect.map((record) => record?.abortReason === "restart")),
        );
        yield* eventually(
          harness.harness.inspect.pipe(
            Effect.map(
              (inspected) =>
                inspected.tasks.find((task) => task.record.id === leaf)?.state.kind === "blocked",
            ),
          ),
        );
        assert.notStrictEqual((yield* getTask(leaf))?.state.status, "terminal");
        // A request on the top task reaches the leaf two levels down, which is then orphaned.
        yield* harness.harness.abortTask(top);
        for (const [id, status] of [
          [leaf, "orphaned"],
          [middle, "aborted"],
          [top, "aborted"],
        ] as const) {
          assert.strictEqual((yield* harness.harness.awaitTask(id)).state.outcome.status, status);
        }
        yield* harness.close;
      }),
    ),
  );

  it.effect(
    "marks a flagged task that holds its outcome, aborting its owned work, and keeps the outcome",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const storage = yield* controlledStorage;
          const { holder, child } = yield* withSession(storage, (session) =>
            Effect.gen(function* () {
              const conversationId = yield* createConversation(session);
              const holder = yield* session.commit(
                Effect.gen(function* () {
                  const tx = yield* Tx;
                  return yield* tx.createTask(Dormant, null, {
                    conversationId,
                    ownership: { kind: "conversation" },
                    abandonOnRestart: true,
                  });
                }),
              );
              const child = yield* session.commit(
                Effect.gen(function* () {
                  const tx = yield* Tx;
                  return yield* tx.createTask(Dormant, null, {
                    ownership: { kind: "task", taskId: holder },
                  });
                }),
              );
              return { holder, child };
            }),
          );
          const record = yield* storedTask(storage, holder);
          const completing = {
            status: "completing",
            outcome: { status: "completed", result: null },
          } as const;
          yield* commitWrites(storage, [taskWrite({ ...record, state: completing })]);
          const harness = yield* openHarness(storage, [Dormant]);
          yield* harness.harness.resume;
          assert.strictEqual(
            (yield* harness.harness.awaitTask(child)).state.outcome.status,
            "aborted",
          );
          const settled = yield* harness.harness.awaitTask(holder);
          assert.containSubset(settled, {
            abortReason: "restart",
            state: { outcome: { status: "completed" } },
          });
          yield* harness.close;
        }),
      ),
  );
});

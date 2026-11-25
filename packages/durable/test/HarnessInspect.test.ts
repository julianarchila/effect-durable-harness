// Task inspection, on every backend.
//
// `openTasks` is `openHarness` with the tasks as its static registry; a blocked task's reason is the
// inspection's `message`; a migration that throws is a defect of the migration.
import { assert, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import type { TaskInspection } from "../src/Harness.ts";
import type { TaskId } from "../src/Ids.ts";
import * as Task from "../src/Task.ts";
import { Tx } from "../src/Tx.ts";
import { chatSetup, openChat, unanswered, waitFor } from "./ChatSupport.ts";
import { bounded, completed, eventually, openHarness } from "./HarnessSupport.ts";
import { controlledStorage, describeEachBackend, run } from "./SessionSupport.ts";

const Step = Schema.Struct({ phase: Schema.Literal("run") });

interface TaskOptions {
  readonly gate?: Deferred.Deferred<void> | undefined;
  readonly migrate?: (() => void) | undefined;
}

/** A one-phase task that completes once `gate` is done; `migrate` runs when an older stored version migrates. */
const task = (name: string, version: number, options: TaskOptions = {}) => {
  const migrate = options.migrate;
  const gate = options.gate;
  return Task.make({
    name,
    version,
    input: Schema.Null,
    checkpoint: Step,
    result: Schema.Null,
    initial: () => ({ phase: "run" }),
    phases: {
      run: (_task, runtime) =>
        (gate === undefined ? Effect.void : Deferred.await(gate)).pipe(
          Effect.andThen(runtime.commit(() => Effect.succeed(completed(null)))),
        ),
    },
    abort: () => Effect.void,
    migrate:
      migrate === undefined
        ? undefined
        : () => {
            migrate();
            return { input: null, checkpoint: { phase: "run" as const } };
          },
  });
};

/** The task's state, without keys whose value is `undefined`. */
const stateOf = (
  tasks: ReadonlyArray<TaskInspection>,
  id: TaskId,
): TaskInspection["state"] | undefined => {
  const state = tasks.find((entry) => entry.record.id === id)?.state;
  if (state === undefined) return undefined;
  // SAFETY: the same state with its `undefined` optional keys removed.
  return Object.fromEntries(
    Object.entries(state).filter((field) => field[1] !== undefined),
  ) as TaskInspection["state"];
};

const conversationOwned = { ownership: { kind: "conversation" } } as const;

describeEachBackend("Harness.inspect()", (backend) => {
  it.effect("derives every live task's state without running task code", () =>
    run(backend)(
      Effect.gen(function* () {
        const gate = yield* Deferred.make<void>();
        const Gate = task("test.gate", 1, { gate });
        let gateId: TaskId | undefined;
        const Dependent = Task.make({
          name: "test.dependent",
          version: 1,
          input: Schema.Null,
          checkpoint: Schema.Union([
            Schema.Struct({ phase: Schema.Literal("wait") }),
            Schema.Struct({ phase: Schema.Literal("done") }),
          ]),
          result: Schema.Null,
          initial: () => ({ phase: "wait" }),
          phases: {
            wait: (_task, runtime) =>
              runtime.commit(() =>
                Effect.succeed({
                  status: "waiting",
                  checkpoint: { phase: "done" },
                  on: [gateId!],
                  policy: "allSettled",
                } as const),
              ),
            done: (_task, runtime) => runtime.commit(() => Effect.succeed(completed(null))),
          },
          abort: () => Effect.void,
        });
        let migrations = 0;
        const registered = [
          Gate,
          Dependent,
          task("test.migrating", 2, {
            migrate: () => {
              migrations++;
            },
          }),
          task("test.no-migration", 2),
          task("test.failing", 2, {
            migrate: () => {
              throw new Error("cannot migrate");
            },
          }),
          task("test.too-old", 1),
        ];
        const h = yield* openHarness(yield* controlledStorage, registered);
        const root = yield* h.harness.ensureRoot();
        const ids = yield* root.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            gateId = yield* tx.createTask(Gate, null, conversationOwned);
            return {
              gate: gateId,
              dependent: yield* tx.createTask(Dependent, null, conversationOwned),
              // Stored by definitions other than the registered ones.
              migrating: yield* tx.createTask(task("test.migrating", 1), null, conversationOwned),
              noMigration: yield* tx.createTask(
                task("test.no-migration", 1),
                null,
                conversationOwned,
              ),
              failing: yield* tx.createTask(task("test.failing", 1), null, conversationOwned),
              tooOld: yield* tx.createTask(task("test.too-old", 2), null, conversationOwned),
              missing: yield* tx.createTask(task("test.missing", 1), null, conversationOwned),
            };
          }),
        );

        const paused = yield* h.harness.inspect;
        assert.strictEqual(paused.scheduling, "paused");
        assert.deepStrictEqual(
          paused.tasks.map((entry) => entry.record.id),
          Object.values(ids),
        );
        assert.deepStrictEqual(stateOf(paused.tasks, ids.gate), {
          kind: "pending",
          migrates: false,
        });
        assert.deepStrictEqual(stateOf(paused.tasks, ids.dependent), {
          kind: "pending",
          migrates: false,
        });
        assert.deepStrictEqual(stateOf(paused.tasks, ids.migrating), {
          kind: "pending",
          migrates: true,
        });
        // A migration that was never tried is not run to find out.
        assert.deepStrictEqual(stateOf(paused.tasks, ids.failing), {
          kind: "pending",
          migrates: true,
        });
        assert.deepStrictEqual(stateOf(paused.tasks, ids.noMigration), {
          kind: "blocked",
          reason: "migration_failed",
          message: "Task test.no-migration version 2 has no migration from 1",
        });
        assert.deepStrictEqual(stateOf(paused.tasks, ids.tooOld), {
          kind: "blocked",
          reason: "task_too_old",
        });
        assert.deepStrictEqual(stateOf(paused.tasks, ids.missing), {
          kind: "blocked",
          reason: "missing_task",
        });
        assert.strictEqual(migrations, 0);
        assert.strictEqual((yield* h.harness.inspect).scheduling, "paused");

        yield* h.harness.resume;
        yield* eventually(Effect.sync(() => migrations === 1));
        yield* bounded(h.harness.awaitTask(ids.migrating));
        let running = yield* h.harness.inspect;
        yield* waitFor(
          Effect.gen(function* () {
            running = yield* h.harness.inspect;
            return (
              stateOf(running.tasks, ids.gate)?.kind === "running" &&
              stateOf(running.tasks, ids.dependent)?.kind === "waiting"
            );
          }),
        );
        assert.deepStrictEqual(stateOf(running.tasks, ids.dependent), {
          kind: "waiting",
          on: [ids.gate],
        });
        assert.strictEqual(running.scheduling, "running");
        assert.deepStrictEqual(stateOf(running.tasks, ids.gate), { kind: "running" });
        assert.isUndefined(stateOf(running.tasks, ids.migrating));
        assert.deepStrictEqual(stateOf(running.tasks, ids.failing), {
          kind: "blocked",
          reason: "migration_failed",
          message: "cannot migrate",
        });

        yield* Deferred.succeed(gate, undefined);
        yield* bounded(h.harness.awaitTask(ids.dependent));
        const settled = yield* h.harness.inspect;
        assert.deepStrictEqual(
          settled.tasks.map((entry) => entry.record.id),
          [ids.noMigration, ids.failing, ids.tooOld, ids.missing],
        );
        yield* h.close;
      }),
    ),
  );

  it.effect("lists unsettled submissions", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        const busy = unanswered();
        setup.faux.setResponses([busy.step]);
        const { harness, root } = yield* openChat(yield* controlledStorage, setup);
        const other = yield* harness.harness.createConversation({
          ownership: { kind: "ownerless" },
        });
        yield* other.submit({ type: "write", entry: { kind: "note" } });
        const input = yield* root.submit({ type: "input", content: "hi" });
        yield* waitFor(Deferred.isDone(busy.reached));

        const inspection = yield* harness.harness.inspect;
        assert.deepStrictEqual(inspection.submissions, [yield* input.status]);
        assert.deepStrictEqual(
          inspection.tasks.map((entry) => [entry.record.kind, entry.state.kind]),
          [["core.generation", "running"]],
        );
        yield* harness.close;
      }),
    ),
  );
});

// Structured concurrency, on every backend: task ownership, waits and their policies, held (`completing`)
// outcomes, bottom-up abort, background boundaries, recovery, and the built-in tool rounds that use them.
//
// Notes (as in `HarnessTasks` and `HarnessOwnership`):
// - `openHarness` runs over a `ControlledStorage`; a reopen is a new Harness over the same storage, and
//   `freshStorage` is a fresh storage of the backend.
// - Setup goes through a Session kernel over the same storage (`withSession`), whose `commitWith` exposes
//   `setTask`.
// - The abort mark interrupts a handler, and a handler that awaits its signal is `Effect.never`. A handler's
//   throw is a defect (`Effect.die`).
// - Waits use `eventually` (bounded turns), or `ChatSupport.waitFor` in the chat cases, which advances the
//   `TestClock` in 5 ms steps; `settled(fiber)` checks a forked wait.
// - The registry is static: tasks, tools and hooks are extensions given when the Harness opens, and a
//   registration disposed or replaced while the Harness runs is a reopen with other extensions.
// - A storage that refuses a commit is the controlled storage's commit hook.
import { afterEach, assert, it } from "@effect/vitest";
import * as Cause from "effect/Cause";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as AgentStorageModule from "../src/AgentStorage.ts";
import { StorageFailure } from "../src/AgentStorage.ts";
import * as AgentTool from "../src/AgentTool.ts";
import { ToolContext } from "../src/AgentTool.ts";
import * as Doc from "../src/Doc.ts";
import { ToolResultEntry } from "../src/Entry.ts";
import * as Extension from "../src/Extension.ts";
import type { Conversation } from "../src/Harness.ts";
import * as Hook from "../src/Hook.ts";
import { type ConversationId, TaskId } from "../src/Ids.ts";
import { LiveDoc, type LiveState, type ToolSlot } from "../src/Live.ts";
import type {
  AssistantMessage,
  TextContent,
  ToolResultContent,
  ToolResultMessage,
} from "../src/Message.ts";
import {
  type AssistantMessageEvent,
  type GenerateRequest,
  Models,
  type ResolvedModel,
} from "../src/Models.ts";
import type { EntryRecord, JoinPolicy, TaskRecord } from "../src/Records.ts";
import type { ReadError, CommitError } from "../src/Session.ts";
import type { ToolExecutionMode } from "../src/Settings.ts";
import type { Submission } from "../src/Submission.ts";
import type { TaskError } from "../src/TaskError.ts";
import * as Task from "../src/Task.ts";
import type { TaskGraph, TaskGraphNode } from "../src/TaskGraph.ts";
import { fauxAssistantMessage, fauxText, fauxToolCall } from "../src/testing/FauxModel.ts";
import { Tx, type TxDocError } from "../src/Tx.ts";
import type { ConversationView } from "../src/View.ts";
import { allEntries, type ChatSetup, chatSetup, openChat, waitFor } from "./ChatSupport.ts";
import {
  anyNumber,
  bounded,
  eventually,
  joined,
  openHarness,
  settled,
  type TestHarness,
} from "./HarnessSupport.ts";
import {
  type Backend,
  type ControlledStorage,
  controlledStorage,
  describeEachBackend,
  failureMessage,
  flush,
  run,
  withSession,
} from "./SessionSupport.ts";

// ─── A scriptable task ──────────────────────────────────────────────────────

const NodeInput = Schema.Struct({ name: Schema.String });
type NodeInput = typeof NodeInput.Type;
const NodeCheckpoint = Schema.Union([
  Schema.Struct({ phase: Schema.Literal("run") }),
  Schema.Struct({ phase: Schema.Literal("resume"), round: Schema.Number }),
]);
type NodeCheckpoint = typeof NodeCheckpoint.Type;
type NodeRuntime = Task.TaskRuntime<NodeInput, NodeCheckpoint, string>;
type Next = Task.NextTaskState<NodeCheckpoint, string>;
/** What a scripted handler may fail with; a throw is a defect (header). */
type HandlerError = TaskError | CommitError | ReadError | TxDocError;
type Handler = Effect.Effect<void, HandlerError>;

/** Per-name behavior of `Node`; unscripted parts use the defaults below. */
interface Behavior {
  readonly run?: ((runtime: NodeRuntime) => Handler) | undefined;
  readonly resume?: ((runtime: NodeRuntime, round: number) => Handler) | undefined;
  readonly abort?: ((runtime: NodeRuntime) => Handler) | undefined;
}

/** How a default run ends once its gate opens: its outcome, or a throw that faults it. */
type Ending = "completed" | "failed" | "throw";

const behaviors = new Map<string, Behavior>();
const gates = new Map<string, Deferred.Deferred<Ending>>();
/** Handler starts and ends in order, such as `run:a`, `abort:a`. */
const log: Array<string> = [];

function gate(name: string): Deferred.Deferred<Ending> {
  let found = gates.get(name);
  if (found === undefined) {
    found = Deferred.makeUnsafe<Ending>();
    gates.set(name, found);
  }
  return found;
}

function open(name: string, ending: Ending = "completed"): void {
  Deferred.doneUnsafe(gate(name), Exit.succeed(ending));
}

function script(name: string, behavior: Behavior): void {
  behaviors.set(name, behavior);
}

function end(ending: "completed" | "failed", name: string): Next {
  return ending === "completed"
    ? { status: "terminal", outcome: { status: "completed", result: name } }
    : { status: "terminal", outcome: { status: "failed", error: { message: `${name} failed` } } };
}

/** Wait for the gate, then end as the gate says; the abort mark interrupts the wait (header). */
const defaultRun = (runtime: NodeRuntime, name: string): Handler =>
  Effect.gen(function* () {
    const ending = yield* Deferred.await(gate(name));
    if (ending === "throw") return yield* Effect.die(new Error(`${name} threw`));
    yield* runtime.commit(Effect.succeed(end(ending, name)));
  });

/** A task that runs its name's behavior. */
const Node = Task.make({
  name: "test.node",
  version: 1,
  input: NodeInput,
  checkpoint: NodeCheckpoint,
  result: Schema.String,
  initial: () => ({ phase: "run" }),
  phases: {
    run: (task, runtime) =>
      Effect.suspend(() => {
        const name = task.input.name;
        log.push(`run:${name}`);
        const behavior = behaviors.get(name)?.run;
        return behavior === undefined ? defaultRun(runtime, name) : behavior(runtime);
      }),
    resume: (task, runtime) =>
      Effect.suspend(() => {
        const name = task.input.name;
        log.push(`resume:${name}`);
        const behavior = behaviors.get(name)?.resume;
        if (behavior !== undefined) return behavior(runtime, task.state.checkpoint.round);
        return runtime.commit(Effect.succeed(end("completed", name)));
      }),
  },
  abort: (task, runtime) =>
    Effect.suspend(() => {
      const name = task.input.name;
      log.push(`abort:${name}`);
      const behavior = behaviors.get(name)?.abort;
      if (behavior !== undefined) return behavior(runtime);
      return runtime.commit(
        Effect.succeed({
          status: "terminal",
          outcome: { status: "aborted", result: name },
        } as const),
      );
    }),
});

/** Never registered: aborting it can only orphan it. */
const Unregistered = Task.make({
  name: "test.unregistered",
  version: 1,
  input: NodeInput,
  checkpoint: Schema.Struct({ phase: Schema.Literal("run") }),
  result: Schema.String,
  initial: () => ({ phase: "run" }),
  phases: { run: () => Effect.void },
  abort: () => Effect.void,
});

const TaskNotes = Doc.make({
  checkpointWhen: () => false,
  kind: "test.task-notes",
  version: 1,
  scope: "task",
  schema: Schema.Struct({ text: Schema.String }),
  initial: () => ({ text: "" }),
});

const ABORTED = { status: "terminal", outcome: { status: "aborted" } } as const;

const OWN_CONVERSATION: Task.TaskOptions = { ownership: { kind: "conversation" } };

function owned(owner: TaskId): Task.TaskOptions {
  return { ownership: { kind: "task", taskId: owner } };
}

/** A child task named `name`, owned by `owner`. */
const spawn = (owner: TaskId, name: string) =>
  Effect.gen(function* () {
    const tx = yield* Tx;
    return yield* tx.createTask(Node, { name }, owned(owner));
  });

/** Wait on `on`, resuming in round `round`. */
function waitOn(on: ReadonlyArray<TaskId>, policy: JoinPolicy, round = 1): Next {
  return { status: "waiting", checkpoint: { phase: "resume", round }, on, policy };
}

/** Keep running at the `resume` phase, round `round`. */
function toResume(round = 1): Next {
  return { status: "running", checkpoint: { phase: "resume", round } };
}

const start = (
  conversation: Conversation,
  name: string,
  options: Task.TaskOptions = OWN_CONVERSATION,
) =>
  conversation.commit(
    Effect.gen(function* () {
      const tx = yield* Tx;
      return yield* tx.createTask(Node, { name }, options);
    }),
  );

/** A Harness with `Node` (and `tasks`), its root, resumed. */
const openNodes = (storage: ControlledStorage, tasks: ReadonlyArray<Task.Any> = [Node]) =>
  Effect.gen(function* () {
    const h = yield* openHarness(storage, tasks);
    const root = yield* h.harness.ensureRoot();
    yield* h.harness.resume;
    return { h, root };
  });

const record = (h: TestHarness, id: TaskId) =>
  h.harness.getTask(id).pipe(Effect.map(Option.getOrThrow));

const state = (h: TestHarness, id: TaskId) =>
  record(h, id).pipe(Effect.map((found) => found.state));

const abortRequested = (h: TestHarness, id: TaskId) =>
  record(h, id).pipe(Effect.map((found) => found.abortRequested));

/** The terminal receipt of `id`, once it settled within a bounded number of turns. */
const settledTask = (h: TestHarness, id: TaskId) => bounded(h.harness.awaitTask(id));

const outcomeOf = (h: TestHarness, id: TaskId) =>
  settledTask(h, id).pipe(Effect.map((done) => done.state.outcome.status));

/** The message of a faulted outcome; fails the test when the outcome is another. */
const faultOf = (h: TestHarness, id: TaskId) =>
  settledTask(h, id).pipe(
    Effect.map((done) => {
      const outcome = done.state.outcome;
      if (outcome.status !== "faulted") return assert.fail(`Unexpected ${outcome.status}`);
      return outcome.error.message;
    }),
  );

const until = <E, R>(check: Effect.Effect<boolean, E, R>) => eventually(check);

const logged = (line: string) => Effect.sync(() => log.includes(line));

const statusIs = (h: TestHarness, id: TaskId, status: string) =>
  state(h, id).pipe(Effect.map((found) => found.status === status));

/** The derived state `inspect` shows for `id`. */
const inspected = (h: TestHarness, id: TaskId) =>
  h.harness.inspect.pipe(
    Effect.map((inspection) => inspection.tasks.find((task) => task.record.id === id)?.state),
  );

/** Forks `effect` at once, for waits a case checks with `settled` or awaits later. */
const fork = <A, E>(effect: Effect.Effect<A, E>) =>
  Effect.forkChild(effect, { startImmediately: true });

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

const diskGone = () =>
  new StorageFailure({
    reason: new AgentStorageModule.Backend({ message: "disk gone", cause: "disk gone" }),
  });

/** The task whose finalizing commit storage refuses, while set. */
interface Refused {
  id: TaskId | undefined;
}

/** Refuses the commit that makes `target()` terminal, once. */
const refuseFinalization = (
  storage: ControlledStorage,
  failure: StorageFailure,
  target: Refused,
) => {
  storage.intercept.commit = (batches, next) => {
    const finalizes = batches
      .flat()
      .some(
        (write) =>
          write.type === "task" &&
          write.value.id === target.id &&
          write.value.state.status === "terminal",
      );
    if (!finalizes) return next;
    target.id = undefined;
    return Effect.fail(failure);
  };
};

afterEach(() => {
  behaviors.clear();
  gates.clear();
  log.length = 0;
});

// ─── Ownership ──────────────────────────────────────────────────────────────

describeEachBackend("task ownership", (backend) => {
  it.effect("creates a child in its owner's conversation with its owner recorded", () =>
    run(backend)(
      Effect.gen(function* () {
        const { h, root } = yield* openNodes(yield* controlledStorage);
        let child: TaskId | undefined;
        script("parent", {
          run: (runtime) =>
            runtime.commit(
              Effect.gen(function* () {
                child = yield* spawn(runtime.taskId, "child");
                return waitOn([child], "allSettled");
              }),
            ),
        });
        const parent = yield* start(root, "parent");
        yield* until(Effect.sync(() => child !== undefined));
        const found = yield* record(h, child!);
        assert.containSubset(found, { owner: parent, conversationId: root.id, background: false });
        assert.isUndefined((yield* record(h, parent)).owner);
        open("child");
        assert.strictEqual(yield* outcomeOf(h, parent), "completed");
        yield* h.close;
      }),
    ),
  );

  it.effect(
    "rejects no ownership, a missing owner, a child in another conversation, and a background child",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const { h, root } = yield* openNodes(yield* controlledStorage);
          const parent = yield* start(root, "parent");
          const other = yield* h.harness.createConversation({ ownership: { kind: "ownerless" } });
          const create = (options: Task.TaskOptions) =>
            failureMessage(
              root.commit(
                Effect.gen(function* () {
                  const tx = yield* Tx;
                  return yield* tx.createTask(Node, { name: "x" }, options);
                }),
              ),
            );
          // SAFETY: options without ownership pass the types, to check the runtime rejection.
          yield* create({} as Task.TaskOptions);
          assert.include(yield* create(owned(TaskId.make(999_999))), "does not exist");
          assert.include(
            yield* create({ ...owned(parent), conversationId: other.id }),
            "owner's conversation",
          );
          assert.include(
            yield* create({ ...owned(parent), background: true }),
            "cannot be background",
          );
          // The owner's conversation is the default, even from a commit bound to another conversation.
          const child = yield* other.commit(spawn(parent, "child"));
          assert.strictEqual((yield* record(h, child)).conversationId, root.id);
          open("parent");
          open("child");
          yield* settledTask(h, parent);
          yield* h.close;
        }),
      ),
  );

  it.effect(
    "rejects new owned work below an owner that is completing, terminal, or abort-marked",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const { h, root } = yield* openNodes(yield* controlledStorage);
          script("parent", {
            run: (runtime) =>
              runtime.commit(
                Effect.gen(function* () {
                  yield* spawn(runtime.taskId, "child");
                  return toResume();
                }),
              ),
          });
          const parent = yield* start(root, "parent");
          yield* until(statusIs(h, parent, "completing"));
          const createChild = () => failureMessage(root.commit(spawn(parent, "late")));
          const createConversation = () =>
            failureMessage(
              root.commit(
                Effect.gen(function* () {
                  const tx = yield* Tx;
                  return yield* tx.createConversation({
                    ownership: { kind: "task", taskId: parent },
                  });
                }),
              ),
            );
          assert.include(yield* createChild(), "is completing");
          assert.include(yield* createConversation(), "is completing");
          open("child");
          yield* settledTask(h, parent);
          assert.include(yield* createChild(), "is terminal");
          assert.include(yield* createConversation(), "is terminal");

          script("slow", {
            abort: (runtime) =>
              Effect.gen(function* () {
                yield* Deferred.await(gate("abort.slow"));
                yield* runtime.commit(Effect.succeed(ABORTED));
              }),
          });
          const slow = yield* start(root, "slow");
          yield* until(logged("run:slow"));
          yield* h.harness.abortTask(slow);
          assert.include(
            yield* failureMessage(root.commit(spawn(slow, "late"))),
            "is abort-marked",
          );
          open("abort.slow");
          assert.strictEqual(yield* outcomeOf(h, slow), "aborted");
          yield* h.close;
        }),
      ),
  );

  it.effect(
    "cannot create a child in its finishing commit, but work it starts in an owned conversation holds it",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const { h, root } = yield* openNodes(yield* controlledStorage);
          script("eager", {
            run: (runtime) =>
              runtime.commit(
                Effect.gen(function* () {
                  yield* spawn(runtime.taskId, "never");
                  return end("completed", "eager");
                }),
              ),
          });
          const eager = yield* start(root, "eager");
          assert.include(yield* faultOf(h, eager), "is completing");
          // The rejected commit wrote nothing.
          const nodes = yield* h.harness.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              return yield* tx.scanTasks({ kind: "test.node" }, 20);
            }),
          );
          assert.deepStrictEqual(
            nodes.items.map((task) => Schema.decodeUnknownSync(NodeInput)(task.input).name),
            ["eager"],
          );

          let child: ConversationId | undefined;
          script("host", {
            run: (runtime) =>
              runtime.commit(
                Effect.gen(function* () {
                  const tx = yield* Tx;
                  child = (yield* tx.createConversation({
                    ownership: { kind: "task", taskId: runtime.taskId },
                  })).id;
                  return toResume();
                }),
              ),
            resume: (runtime) =>
              runtime.commit(
                Effect.gen(function* () {
                  const tx = yield* Tx;
                  yield* tx.createTask(
                    Node,
                    { name: "inner" },
                    { ...OWN_CONVERSATION, conversationId: child! },
                  );
                  return end("completed", "host");
                }),
              ),
          });
          const host = yield* start(root, "host");
          yield* until(statusIs(h, host, "completing"));
          assert.isFalse(yield* settled(yield* fork(h.harness.awaitTask(host))));
          open("inner");
          assert.strictEqual(yield* outcomeOf(h, host), "completed");
          yield* h.close;
        }),
      ),
  );
});

// ─── Waiting ────────────────────────────────────────────────────────────────

/** The children a scripted parent spawned, and their outcomes once it resumed. */
interface Spawned<T = string> {
  readonly ids: Array<TaskId<T>>;
  outcomes: Array<string>;
}

/** A task a script creates, once it has. */
interface Created {
  id?: TaskId | undefined;
}

/** Script `parent` to spawn `children` in one commit and wait on them with `policy`; it records their outcomes. */
function parentOf(parent: string, children: ReadonlyArray<string>, policy: JoinPolicy) {
  const found: Spawned = { ids: [], outcomes: [] };
  script(parent, {
    run: (runtime) =>
      runtime.commit(
        Effect.gen(function* () {
          for (const name of children) found.ids.push(yield* spawn(runtime.taskId, name));
          return waitOn(found.ids, policy);
        }),
      ),
    resume: (runtime) =>
      Effect.gen(function* () {
        found.outcomes = (yield* runtime.outcomes(found.ids)).map((outcome) => outcome.status);
        yield* runtime.commit(Effect.succeed(end("completed", parent)));
      }),
  });
  return found;
}

/** Script `name` to spawn `child` and then complete, holding while the child lives. */
function spawnAndFinish(name: string, child: string): Created {
  const found: Created = {};
  script(name, {
    run: (runtime) =>
      runtime.commit(
        Effect.gen(function* () {
          found.id = yield* spawn(runtime.taskId, child);
          return toResume();
        }),
      ),
  });
  return found;
}

/** Script `name` to wait, with allSettled, on the task `on()` names when it runs. */
function waiting(name: string, on: () => TaskId | undefined): void {
  script(name, {
    run: (runtime) => runtime.commit(Effect.sync(() => waitOn([on()!], "allSettled"))),
  });
}

describeEachBackend("waiting", (backend) => {
  it.effect(
    "resumes once every awaited task is terminal and reads their outcomes in order (allSettled)",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const { h, root } = yield* openNodes(yield* controlledStorage);
          const found: Spawned<unknown> = { ids: [], outcomes: [] };
          script("parent", {
            run: (runtime) =>
              runtime.commit(
                Effect.gen(function* () {
                  const tx = yield* Tx;
                  for (const name of ["ok", "fails", "throws", "aborted"])
                    found.ids.push(yield* spawn(runtime.taskId, name));
                  found.ids.push(
                    yield* tx.createTask(Unregistered, { name: "orphan" }, owned(runtime.taskId)),
                  );
                  return waitOn(found.ids, "allSettled");
                }),
              ),
            resume: (runtime) =>
              Effect.gen(function* () {
                found.outcomes = (yield* runtime.outcomes(found.ids)).map(
                  (outcome) => outcome.status,
                );
                yield* runtime.commit(Effect.succeed(end("completed", "parent")));
              }),
          });
          const parent = yield* start(root, "parent");
          yield* until(Effect.sync(() => found.ids.length === 5));
          yield* until(statusIs(h, parent, "waiting"));
          open("ok");
          open("fails", "failed");
          open("throws", "throw");
          yield* h.harness.abortTask(found.ids[3]!);
          assert.deepStrictEqual(yield* h.harness.abortTask(found.ids[4]!), { _tag: "Marked" });
          assert.strictEqual(yield* outcomeOf(h, parent), "completed");
          assert.deepStrictEqual(found.outcomes, [
            "completed",
            "failed",
            "faulted",
            "aborted",
            "orphaned",
          ]);
          // allSettled never marks siblings.
          assert.deepStrictEqual(
            log.filter((line) => line.startsWith("abort:")),
            ["abort:aborted"],
          );
          yield* h.close;
        }),
      ),
  );

  for (const ending of ["failed", "throw"] as const) {
    it.effect(
      `fails fast when a child ends ${ending === "throw" ? "faulted" : "failed"}: its live siblings are aborted, the parent is not`,
      () =>
        run(backend)(
          Effect.gen(function* () {
            const { h, root } = yield* openNodes(yield* controlledStorage);
            const found = parentOf("checkout", ["p1", "p2", "p3", "p4"], "failFast");
            const parent = yield* start(root, "checkout");
            yield* until(statusIs(h, parent, "waiting"));
            open("p2", ending);
            assert.strictEqual(yield* outcomeOf(h, parent), "completed");
            assert.deepStrictEqual(found.outcomes, [
              "aborted",
              ending === "throw" ? "faulted" : "failed",
              "aborted",
              "aborted",
            ]);
            assert.isFalse(yield* abortRequested(h, parent));
            assert.deepStrictEqual(log.filter((line) => line.startsWith("abort:")).toSorted(), [
              "abort:p1",
              "abort:p3",
              "abort:p4",
            ]);
            yield* h.close;
          }),
        ),
    );
  }

  it.effect("fails fast on a held failure before the failing child drains", () =>
    run(backend)(
      Effect.gen(function* () {
        const { h, root } = yield* openNodes(yield* controlledStorage);
        const found: Spawned<unknown> = { ids: [], outcomes: [] };
        script("p1", {
          run: (runtime) =>
            runtime.commit(
              Effect.gen(function* () {
                yield* spawn(runtime.taskId, "grandchild");
                return toResume();
              }),
            ),
          resume: (runtime) => runtime.commit(Effect.succeed(end("failed", "p1"))),
        });
        script("grandchild", {
          abort: (runtime) =>
            Effect.gen(function* () {
              yield* Deferred.await(gate("abort.grandchild"));
              yield* runtime.commit(Effect.succeed(ABORTED));
            }),
        });
        let outside: TaskId | undefined;
        script("parent", {
          run: (runtime) =>
            runtime.commit(
              Effect.gen(function* () {
                for (const name of ["p1", "p2"]) found.ids.push(yield* spawn(runtime.taskId, name));
                outside = yield* spawn(runtime.taskId, "outside");
                return waitOn(found.ids, "failFast");
              }),
            ),
        });
        const parent = yield* start(root, "parent");
        yield* until(Effect.sync(() => found.ids.length === 2));
        // p1 holds `failed` while its grandchild's abort handler runs; p2 is already aborted.
        assert.strictEqual(yield* outcomeOf(h, found.ids[1]!), "aborted");
        assert.containSubset(yield* state(h, found.ids[0]!), {
          status: "completing",
          outcome: { status: "failed" },
        });
        assert.strictEqual((yield* state(h, parent)).status, "waiting");
        open("abort.grandchild");
        yield* until(logged("resume:parent"));
        // Only the other tasks in `on` are marked: not the failed one, not the parent, not a child outside `on`.
        assert.strictEqual(yield* outcomeOf(h, found.ids[0]!), "failed");
        assert.deepStrictEqual(
          [
            yield* abortRequested(h, found.ids[0]!),
            yield* abortRequested(h, found.ids[1]!),
            yield* abortRequested(h, outside!),
          ],
          [false, true, false],
        );
        assert.isFalse(yield* abortRequested(h, parent));
        assert.strictEqual((yield* state(h, outside!)).status, "running");
        // Finished, the parent holds for the child outside `on`.
        yield* until(statusIs(h, parent, "completing"));
        open("outside");
        assert.strictEqual(yield* outcomeOf(h, parent), "completed");
        yield* h.close;
      }),
    ),
  );

  it.effect("waits with allSettled on tasks it does not own, including already terminal ones", () =>
    run(backend)(
      Effect.gen(function* () {
        const { h, root } = yield* openNodes(yield* controlledStorage);
        const done = yield* start(root, "done");
        open("done");
        yield* settledTask(h, done);
        const live = yield* start(root, "live");
        let outcomes: Array<string> = [];
        script("parent", {
          run: (runtime) => runtime.commit(Effect.succeed(waitOn([done, live], "allSettled"))),
          resume: (runtime) =>
            Effect.gen(function* () {
              outcomes = (yield* runtime.outcomes([done, live])).map((outcome) => outcome.status);
              yield* runtime.commit(Effect.succeed(end("completed", "parent")));
            }),
        });
        const parent = yield* start(root, "parent");
        yield* until(statusIs(h, parent, "waiting"));
        // A task it does not own is not its work: the parent may finish while it lives, but here it waits for it.
        open("live", "failed");
        assert.strictEqual(yield* outcomeOf(h, parent), "completed");
        assert.deepStrictEqual(outcomes, ["completed", "failed"]);
        yield* h.close;
      }),
    ),
  );

  it.effect(
    "rejects waits on itself, its owner, a missing task, and failFast on a task it does not own",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const { h, root } = yield* openNodes(yield* controlledStorage);
          const other = yield* start(root, "other");
          const cases: Array<
            [string, (runtime: NodeRuntime) => ReadonlyArray<TaskId>, JoinPolicy, string]
          > = [
            ["self", (runtime) => [runtime.taskId], "allSettled", "is a wait cycle"],
            ["missing", () => [TaskId.make(999_999)], "allSettled", "does not exist"],
            ["foreign", () => [other], "failFast", "only on tasks it owns"],
          ];
          for (const [name, on, policy, message] of cases) {
            script(name, {
              run: (runtime) => runtime.commit(Effect.sync(() => waitOn(on(runtime), policy))),
            });
            const id = yield* start(root, name);
            assert.include(yield* faultOf(h, id), message);
          }
          // A child waiting on its owner could never resume.
          let child: TaskId | undefined;
          let parent: TaskId | undefined;
          waiting("child", () => parent);
          script("parent", {
            run: (runtime) =>
              runtime.commit(
                Effect.gen(function* () {
                  child = yield* spawn(runtime.taskId, "child");
                  return waitOn([child], "allSettled");
                }),
              ),
          });
          parent = yield* start(root, "parent");
          assert.strictEqual(yield* outcomeOf(h, parent), "completed");
          assert.include(yield* faultOf(h, child!), "is a wait cycle");
          open("other");
          yield* h.close;
        }),
      ),
  );

  it.effect("rejects the wait that closes a cycle", () =>
    run(backend)(
      Effect.gen(function* () {
        const { h, root } = yield* openNodes(yield* controlledStorage);
        let a: TaskId | undefined;
        let b: TaskId | undefined;
        waiting("a", () => b);
        waiting("b", () => a);
        yield* root.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            a = yield* tx.createTask(Node, { name: "a" }, OWN_CONVERSATION);
            b = yield* tx.createTask(Node, { name: "b" }, OWN_CONVERSATION);
          }),
        );
        // a -> b and b -> a are waits, and whichever comes second closes the cycle.
        const outcomes = [yield* outcomeOf(h, a!), yield* outcomeOf(h, b!)];
        assert.deepStrictEqual(outcomes.toSorted(), ["completed", "faulted"]);
        yield* h.close;
      }),
    ),
  );

  it.effect("rejects the wait that closes a cycle through a held owner", () =>
    run(backend)(
      Effect.gen(function* () {
        const { h, root } = yield* openNodes(yield* controlledStorage);
        let held: TaskId | undefined;
        let other: TaskId | undefined;
        const child = spawnAndFinish("held", "child");
        waiting("child", () => other);
        waiting("other", () => held);
        yield* root.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            held = yield* tx.createTask(Node, { name: "held" }, OWN_CONVERSATION);
            other = yield* tx.createTask(Node, { name: "other" }, OWN_CONVERSATION);
          }),
        );
        // held -> child is a hold; child -> other -> held are waits, and whichever wait comes second closes the cycle.
        assert.strictEqual(yield* outcomeOf(h, held!), "completed");
        const outcomes = [yield* outcomeOf(h, child.id!), yield* outcomeOf(h, other!)];
        assert.deepStrictEqual(outcomes.toSorted(), ["completed", "faulted"]);
        yield* h.close;
      }),
    ),
  );

  it.effect("rejects a cycle through holds two levels deep", () =>
    run(backend)(
      Effect.gen(function* () {
        const { h, root } = yield* openNodes(yield* controlledStorage);
        let held: TaskId | undefined;
        let other: TaskId | undefined;
        const child = spawnAndFinish("held", "child");
        const grandchild = spawnAndFinish("child", "grandchild");
        waiting("grandchild", () => other);
        waiting("other", () => held);
        yield* root.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            held = yield* tx.createTask(Node, { name: "held" }, OWN_CONVERSATION);
            other = yield* tx.createTask(Node, { name: "other" }, OWN_CONVERSATION);
          }),
        );
        // held -> child -> grandchild are holds; grandchild -> other -> held are waits.
        yield* until(Effect.sync(() => grandchild.id !== undefined));
        const outcomes = [yield* outcomeOf(h, grandchild.id!), yield* outcomeOf(h, other!)];
        assert.deepStrictEqual(outcomes.toSorted(), ["completed", "faulted"]);
        assert.strictEqual(yield* outcomeOf(h, child.id!), "completed");
        yield* h.close;
      }),
    ),
  );

  it.effect("lets a background task wait on the owner of its conversation", () =>
    run(backend)(
      Effect.gen(function* () {
        const { h, root } = yield* openNodes(yield* controlledStorage);
        let owner: TaskId | undefined;
        waiting("background", () => owner);
        const background = yield* root.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            owner = yield* tx.createTask(Node, { name: "owner" }, OWN_CONVERSATION);
            const conversation = yield* tx.createConversation({
              ownership: { kind: "task", taskId: owner },
            });
            return yield* tx.createTask(
              Node,
              { name: "background" },
              { ...OWN_CONVERSATION, conversationId: conversation.id, background: true },
            );
          }),
        );
        // background -> owner is a wait but owner -> background is no hold, so there is no cycle.
        yield* until(statusIs(h, background, "waiting"));
        open("owner");
        assert.strictEqual(yield* outcomeOf(h, background), "completed");
        yield* h.close;
      }),
    ),
  );

  // An orchestrator, a worker, and a reviewer.
  it.effect("rejects a cycle of a hold and two waits that each look reasonable", () =>
    run(backend)(
      Effect.gen(function* () {
        const { h, root } = yield* openNodes(yield* controlledStorage);
        const ids: Record<string, TaskId> = {};
        const reviewerWaits = Deferred.makeUnsafe<void>();
        // The orchestrator spawns a worker and finishes; the running worker holds it as completing.
        script("orchestrator", {
          run: (runtime) =>
            runtime.commit(
              Effect.gen(function* () {
                ids.worker = yield* spawn(runtime.taskId, "worker");
                return toResume();
              }),
            ),
        });
        // The reviewer waits for the orchestrator's result.
        script("reviewer", {
          run: (runtime) =>
            Effect.gen(function* () {
              yield* runtime.commit(Effect.sync(() => waitOn([ids.orchestrator!], "allSettled")));
              yield* Deferred.succeed(reviewerWaits, undefined);
            }),
        });
        // The worker waits for the reviewer, closing the cycle.
        script("worker", {
          run: (runtime) =>
            Effect.gen(function* () {
              yield* Deferred.await(reviewerWaits);
              yield* runtime.commit(Effect.sync(() => waitOn([ids.reviewer!], "allSettled")));
            }),
        });
        ids.orchestrator = yield* start(root, "orchestrator");
        yield* until(statusIs(h, ids.orchestrator, "completing"));
        ids.reviewer = yield* start(root, "reviewer");
        const outcomes: Array<string> = [];
        for (const name of ["orchestrator", "worker", "reviewer"])
          outcomes.push(yield* outcomeOf(h, ids[name]!));
        assert.deepStrictEqual(outcomes, ["completed", "faulted", "completed"]);
        // The cycle is in the outcome's error message.
        assert.include(
          yield* faultOf(h, ids.worker!),
          `${ids.worker} -> ${ids.reviewer} -> ${ids.orchestrator} -> ${ids.worker} is a wait cycle`,
        );
        yield* h.close;
      }),
    ),
  );

  it.effect("allows a diamond of waits", () =>
    run(backend)(
      Effect.gen(function* () {
        const { h, root } = yield* openNodes(yield* controlledStorage);
        const ids: Record<string, TaskId> = {};
        // A diamond: top waits on left and right, both wait on bottom. No cycle.
        script("top", {
          run: (runtime) =>
            runtime.commit(Effect.sync(() => waitOn([ids.left!, ids.right!], "allSettled"))),
        });
        waiting("left", () => ids.bottom);
        waiting("right", () => ids.bottom);
        yield* root.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            ids.bottom = yield* tx.createTask(Node, { name: "bottom" }, OWN_CONVERSATION);
            ids.left = yield* tx.createTask(Node, { name: "left" }, OWN_CONVERSATION);
            ids.right = yield* tx.createTask(Node, { name: "right" }, OWN_CONVERSATION);
            ids.top = yield* tx.createTask(Node, { name: "top" }, OWN_CONVERSATION);
          }),
        );
        open("bottom");
        assert.strictEqual(yield* outcomeOf(h, ids.top!), "completed");
        yield* h.close;
      }),
    ),
  );

  it.effect("follows no wait of an abort-marked task, which waits only for its owned work", () =>
    run(backend)(
      Effect.gen(function* () {
        const { h, root } = yield* openNodes(yield* controlledStorage);
        const ids: Record<string, TaskId> = {};
        const cleanup = Deferred.makeUnsafe<void>();
        // b owns c and waits on a; aborting b leaves c's cleanup pending, so b waits only for c.
        script("b", {
          run: (runtime) =>
            runtime.commit(
              Effect.gen(function* () {
                ids.c = yield* spawn(runtime.taskId, "c");
                return waitOn([ids.a!], "allSettled");
              }),
            ),
        });
        script("c", {
          abort: (runtime) =>
            Effect.gen(function* () {
              yield* Deferred.await(cleanup);
              yield* runtime.commit(
                Effect.succeed({
                  status: "terminal",
                  outcome: { status: "aborted", result: "c" },
                } as const),
              );
            }),
        });
        const aWaits = Deferred.makeUnsafe<void>();
        script("a", {
          run: (runtime) =>
            Effect.gen(function* () {
              yield* Deferred.await(aWaits);
              yield* runtime.commit(Effect.sync(() => waitOn([ids.b!], "allSettled")));
            }),
          resume: (runtime) => runtime.commit(Effect.succeed(end("completed", "a"))),
        });
        yield* root.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            ids.a = yield* tx.createTask(Node, { name: "a" }, OWN_CONVERSATION);
            ids.b = yield* tx.createTask(Node, { name: "b" }, OWN_CONVERSATION);
          }),
        );
        yield* until(statusIs(h, ids.b!, "waiting"));
        assert.deepStrictEqual(yield* h.harness.abortTask(ids.b!), { _tag: "Marked" });
        yield* until(logged("abort:c"));
        // a -> b would close a -> b -> a through b's stale wait; b now waits only for c.
        yield* Deferred.succeed(aWaits, undefined);
        yield* until(statusIs(h, ids.a!, "waiting"));
        yield* Deferred.succeed(cleanup, undefined);
        assert.strictEqual(yield* outcomeOf(h, ids.b!), "aborted");
        assert.strictEqual(yield* outcomeOf(h, ids.a!), "completed");
        yield* h.close;
      }),
    ),
  );

  it.effect("rejects a longer cycle of waits", () =>
    run(backend)(
      Effect.gen(function* () {
        const { h, root } = yield* openNodes(yield* controlledStorage);
        const ids: Record<string, TaskId> = {};
        const names = ["x", "y", "z"];
        for (const [index, name] of names.entries()) {
          const next = names[(index + 1) % names.length]!;
          waiting(name, () => ids[next]);
        }
        yield* root.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            for (const name of names)
              ids[name] = yield* tx.createTask(Node, { name }, OWN_CONVERSATION);
          }),
        );
        const outcomes: Array<string> = [];
        for (const name of names) outcomes.push(yield* outcomeOf(h, ids[name]!));
        assert.lengthOf(
          outcomes.filter((outcome) => outcome === "faulted"),
          1,
        );
        assert.lengthOf(
          outcomes.filter((outcome) => outcome === "completed"),
          2,
        );
        yield* h.close;
      }),
    ),
  );

  it.effect("resumes at the next pass when it waits on nothing", () =>
    run(backend)(
      Effect.gen(function* () {
        const { h, root } = yield* openNodes(yield* controlledStorage);
        script("parent", {
          run: (runtime) => runtime.commit(Effect.succeed(waitOn([], "failFast"))),
        });
        assert.strictEqual(yield* outcomeOf(h, yield* start(root, "parent")), "completed");
        assert.deepStrictEqual(log, ["run:parent", "resume:parent"]);
        yield* h.close;
      }),
    ),
  );

  it.effect("keeps running phases after spawning and waits on subsets in sequence", () =>
    run(backend)(
      Effect.gen(function* () {
        const { h, root } = yield* openNodes(yield* controlledStorage);
        const ids: Array<TaskId> = [];
        script("parent", {
          run: (runtime) =>
            runtime.commit(
              Effect.gen(function* () {
                for (const name of ["a", "b", "c"]) ids.push(yield* spawn(runtime.taskId, name));
                return toResume(0);
              }),
            ),
          resume: (runtime, round) =>
            Effect.suspend(() => {
              log.push(`round:${round}`);
              if (round === 0)
                return runtime.commit(Effect.sync(() => waitOn([ids[0]!], "allSettled", 1)));
              if (round === 1)
                return runtime.commit(Effect.sync(() => waitOn([ids[1]!, ids[2]!], "failFast", 2)));
              return runtime.commit(Effect.succeed(end("completed", "parent")));
            }),
        });
        const parent = yield* start(root, "parent");
        yield* until(logged("round:0"));
        yield* until(statusIs(h, parent, "waiting"));
        open("b");
        const b = yield* fork(h.harness.awaitTask(ids[1]!));
        yield* until(Effect.sync(() => b.pollUnsafe() !== undefined));
        assert.isTrue(yield* settled(b));
        assert.strictEqual((yield* state(h, parent)).status, "waiting");
        open("a");
        yield* until(logged("round:1"));
        open("c");
        assert.strictEqual(yield* outcomeOf(h, parent), "completed");
        assert.deepStrictEqual(
          log.filter((line) => line.startsWith("round:")),
          ["round:0", "round:1", "round:2"],
        );
        yield* h.close;
      }),
    ),
  );
});

// ─── Completing ─────────────────────────────────────────────────────────────

describeEachBackend("completing", (backend) => {
  it.effect(
    "holds a finished task until its owned work drains; waiters and task documents wait for the final commit",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const { h, root } = yield* openNodes(yield* controlledStorage);
          let child: TaskId | undefined;
          script("parent", {
            run: (runtime) =>
              runtime.commit(
                Effect.gen(function* () {
                  const tx = yield* Tx;
                  (yield* tx.doc(TaskNotes, runtime.taskId)).text = "notes";
                  child = yield* spawn(runtime.taskId, "child");
                  return toResume();
                }),
              ),
          });
          const parent = yield* start(root, "parent");
          yield* until(statusIs(h, parent, "completing"));
          assert.deepStrictEqual(yield* state(h, parent), {
            status: "completing",
            outcome: { status: "completed", result: "parent" },
          });
          const waiter = yield* fork(h.harness.awaitTask(parent));
          assert.isFalse(yield* settled(waiter));
          assert.deepStrictEqual(
            Option.getOrUndefined(yield* h.session.snapshot(TaskNotes, parent)),
            { text: "notes" },
          );
          assert.deepStrictEqual(yield* inspected(h, parent), { kind: "completing" });
          assert.isFalse(yield* settled(yield* fork(root.awaitIdle)));
          open("child");
          assert.deepStrictEqual((yield* joined(waiter)).state.outcome, {
            status: "completed",
            result: "parent",
          });
          assert.isTrue(Option.isNone(yield* h.session.snapshot(TaskNotes, parent)));
          yield* bounded(root.awaitIdle);
          assert.isDefined(child);
          yield* h.close;
        }),
      ),
  );

  it.effect("keeps holding for ordinary work created during the hold", () =>
    run(backend)(
      Effect.gen(function* () {
        const { h, root } = yield* openNodes(yield* controlledStorage);
        let conversation: ConversationId | undefined;
        script("parent", {
          run: (runtime) =>
            runtime.commit(
              Effect.gen(function* () {
                const tx = yield* Tx;
                conversation = (yield* tx.createConversation({
                  ownership: { kind: "task", taskId: runtime.taskId },
                })).id;
                yield* tx.createTask(
                  Node,
                  { name: "first" },
                  { ...OWN_CONVERSATION, conversationId: conversation },
                );
                return toResume();
              }),
            ),
        });
        const parent = yield* start(root, "parent");
        yield* until(statusIs(h, parent, "completing"));
        const child = Option.getOrThrow(yield* h.harness.conversation(conversation!));
        yield* start(child, "second");
        open("first");
        yield* until(logged("run:second"));
        assert.strictEqual((yield* state(h, parent)).status, "completing");
        open("second");
        assert.strictEqual(yield* outcomeOf(h, parent), "completed");
        yield* h.close;
      }),
    ),
  );

  for (const [label, ending] of [
    ["a held failure", "failed"],
    ["a held scheduler fault", "throw"],
  ] as const) {
    it.effect(`aborts the work below ${label}, then finishes with the held outcome`, () =>
      run(backend)(
        Effect.gen(function* () {
          const { h, root } = yield* openNodes(yield* controlledStorage);
          let child: TaskId | undefined;
          script("parent", {
            run: (runtime) =>
              runtime.commit(
                Effect.gen(function* () {
                  child = yield* spawn(runtime.taskId, "child");
                  return toResume();
                }),
              ),
            resume: (runtime) =>
              ending === "throw"
                ? Effect.die(new Error("parent threw"))
                : runtime.commit(Effect.succeed(end("failed", "parent"))),
          });
          const parent = yield* start(root, "parent");
          yield* until(Effect.sync(() => child !== undefined));
          assert.strictEqual(yield* outcomeOf(h, child!), "aborted");
          assert.strictEqual(
            yield* outcomeOf(h, parent),
            ending === "throw" ? "faulted" : "failed",
          );
          yield* h.close;
        }),
      ),
    );
  }

  it.effect(
    "only marks a completing task when aborted: the work below is aborted, the held outcome stays",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const { h, root } = yield* openNodes(yield* controlledStorage);
          let child: TaskId | undefined;
          script("parent", {
            run: (runtime) =>
              runtime.commit(
                Effect.gen(function* () {
                  child = yield* spawn(runtime.taskId, "child");
                  return toResume();
                }),
              ),
          });
          const parent = yield* start(root, "parent");
          yield* until(statusIs(h, parent, "completing"));
          assert.deepStrictEqual(yield* h.harness.abortTask(parent), { _tag: "Marked" });
          assert.strictEqual(yield* outcomeOf(h, child!), "aborted");
          const settledParent = yield* settledTask(h, parent);
          assert.deepStrictEqual(settledParent.state.outcome, {
            status: "completed",
            result: "parent",
          });
          assert.isTrue(settledParent.abortRequested);
          assert.notInclude(log, "abort:parent");
          assert.deepStrictEqual(yield* h.harness.abortTask(parent), { _tag: "AlreadyTerminal" });
          yield* h.close;
        }),
      ),
  );
});

// ─── Abort order ────────────────────────────────────────────────────────────

describeEachBackend("abort order", (backend) => {
  /** Script `name` to spawn `child` and wait on it; its abort handler records the child's status first. */
  function chain(harness: () => TestHarness, name: string, child: string): Created {
    const found: Created = {};
    script(name, {
      run: (runtime) =>
        runtime.commit(
          Effect.gen(function* () {
            found.id = yield* spawn(runtime.taskId, child);
            return waitOn([found.id], "allSettled");
          }),
        ),
      abort: (runtime) =>
        Effect.gen(function* () {
          log.push(`${name} saw ${(yield* state(harness(), found.id!)).status}`);
          yield* runtime.commit(Effect.succeed(ABORTED));
        }),
    });
    return found;
  }

  it.effect(
    "runs abort handlers bottom-up across three levels, each after the level below is terminal",
    () =>
      run(backend)(
        Effect.gen(function* () {
          let opened: TestHarness | undefined;
          const b = chain(() => opened!, "a", "b");
          const c = chain(() => opened!, "b", "c");
          const { h, root } = yield* openNodes(yield* controlledStorage);
          opened = h;
          const a = yield* start(root, "a");
          yield* until(logged("run:c"));
          yield* until(statusIs(h, c.id!, "running"));
          assert.deepStrictEqual(yield* h.harness.abortTask(a), { _tag: "Marked" });
          assert.strictEqual(yield* outcomeOf(h, a), "aborted");
          assert.deepStrictEqual(
            log.filter((line) => line.startsWith("abort:") || line.includes(" saw ")),
            ["abort:c", "abort:b", "b saw terminal", "abort:a", "a saw terminal"],
          );
          assert.isDefined(b.id);
          yield* h.close;
        }),
      ),
  );

  it.effect("does not wait for a task it waits on but does not own", () =>
    run(backend)(
      Effect.gen(function* () {
        const { h, root } = yield* openNodes(yield* controlledStorage);
        const other = yield* start(root, "other");
        script("parent", {
          run: (runtime) => runtime.commit(Effect.succeed(waitOn([other], "allSettled"))),
        });
        const parent = yield* start(root, "parent");
        yield* until(statusIs(h, parent, "waiting"));
        yield* h.harness.abortTask(parent);
        assert.strictEqual(yield* outcomeOf(h, parent), "aborted");
        assert.strictEqual((yield* state(h, other)).status, "running");
        open("other");
        yield* h.close;
      }),
    ),
  );

  it.effect("reports an abort-marked task as waiting for its live owned work", () =>
    run(backend)(
      Effect.gen(function* () {
        const { h, root } = yield* openNodes(yield* controlledStorage);
        let child: TaskId | undefined;
        script("child", {
          abort: (runtime) =>
            Effect.gen(function* () {
              yield* Deferred.await(gate("abort.child"));
              yield* runtime.commit(Effect.succeed(ABORTED));
            }),
        });
        script("parent", {
          run: (runtime) =>
            runtime.commit(
              Effect.gen(function* () {
                child = yield* spawn(runtime.taskId, "child");
                return waitOn([child], "allSettled");
              }),
            ),
        });
        const parent = yield* start(root, "parent");
        yield* until(statusIs(h, parent, "waiting"));
        yield* h.harness.abortTask(parent);
        yield* until(logged("abort:child"));
        assert.deepStrictEqual(yield* inspected(h, parent), { kind: "waiting", on: [child!] });
        open("abort.child");
        assert.strictEqual(yield* outcomeOf(h, parent), "aborted");
        yield* h.close;
      }),
    ),
  );

  it.effect("faults an abort handler that tries to wait", () =>
    run(backend)(
      Effect.gen(function* () {
        const { h, root } = yield* openNodes(yield* controlledStorage);
        script("parent", {
          abort: (runtime) => runtime.commit(Effect.succeed(waitOn([], "allSettled"))),
        });
        const parent = yield* start(root, "parent");
        yield* until(logged("run:parent"));
        yield* h.harness.abortTask(parent);
        assert.include(yield* faultOf(h, parent), "cannot wait");
        yield* h.close;
      }),
    ),
  );

  it.effect("orphans a blocked task only after its owned work drained", () =>
    run(backend)(
      Effect.gen(function* () {
        const { h, root } = yield* openNodes(yield* controlledStorage);
        const { owner, child } = yield* root.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            const owner = yield* tx.createTask(Unregistered, { name: "owner" }, OWN_CONVERSATION);
            return { owner, child: yield* spawn(owner, "child") };
          }),
        );
        yield* until(logged("run:child"));
        assert.deepStrictEqual(yield* h.harness.abortTask(owner), { _tag: "Marked" });
        assert.strictEqual(yield* outcomeOf(h, child), "aborted");
        assert.deepStrictEqual((yield* settledTask(h, owner)).state.outcome, {
          status: "orphaned",
          reason: "missing_task",
        });
        yield* h.close;
      }),
    ),
  );

  it.effect("cascades through task and conversation edges, bottom-up", () =>
    run(backend)(
      Effect.gen(function* () {
        const { h, root } = yield* openNodes(yield* controlledStorage);
        let b: TaskId | undefined;
        let x: TaskId | undefined;
        script("a", {
          run: (runtime) =>
            runtime.commit(
              Effect.gen(function* () {
                b = yield* spawn(runtime.taskId, "b");
                return waitOn([b], "allSettled");
              }),
            ),
        });
        script("b", {
          run: (runtime) =>
            runtime.commit(
              Effect.gen(function* () {
                const tx = yield* Tx;
                const conversation = yield* tx.createConversation({
                  ownership: { kind: "task", taskId: runtime.taskId },
                });
                x = yield* tx.createTask(
                  Node,
                  { name: "x" },
                  { ...OWN_CONVERSATION, conversationId: conversation.id },
                );
                return toResume();
              }),
            ),
          // Blocks until the invocation is interrupted.
          resume: () => Effect.never,
        });
        const a = yield* start(root, "a");
        yield* until(logged("run:x"));
        yield* h.harness.abortTask(a);
        assert.strictEqual(yield* outcomeOf(h, a), "aborted");
        assert.strictEqual(yield* outcomeOf(h, x!), "aborted");
        assert.deepStrictEqual(
          log.filter((line) => line.startsWith("abort:")),
          ["abort:x", "abort:b", "abort:a"],
        );
        yield* h.close;
      }),
    ),
  );
});

// ─── Background and terminal owners ─────────────────────────────────────────

describeEachBackend("boundaries", (backend) => {
  it.effect(
    "keeps background work through Conversation.abort(); { background: true } aborts it and waits",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const { h, root } = yield* openNodes(yield* controlledStorage);
          const foreground = yield* start(root, "foreground");
          let child: TaskId | undefined;
          let below: TaskId | undefined;
          script("background", {
            run: (runtime) =>
              runtime.commit(
                Effect.gen(function* () {
                  const tx = yield* Tx;
                  child = yield* spawn(runtime.taskId, "child");
                  const conversation = yield* tx.createConversation({
                    ownership: { kind: "task", taskId: runtime.taskId },
                  });
                  below = yield* tx.createTask(
                    Node,
                    { name: "below" },
                    { ...OWN_CONVERSATION, conversationId: conversation.id },
                  );
                  return waitOn([child], "allSettled");
                }),
              ),
          });
          const background = yield* start(root, "background", {
            ...OWN_CONVERSATION,
            background: true,
          });
          yield* until(Effect.sync(() => log.includes("run:below") && log.includes("run:child")));
          yield* bounded(root.abort());
          assert.strictEqual(yield* outcomeOf(h, foreground), "aborted");
          for (const id of [background, child!, below!])
            assert.notStrictEqual((yield* state(h, id)).status, "terminal");
          yield* bounded(root.abort({ background: true }));
          for (const id of [background, child!, below!])
            assert.strictEqual((yield* state(h, id)).status, "terminal");
          yield* h.close;
        }),
      ),
  );

  it.effect(
    "never cascades from a terminal owner: an aborted subagent's conversation runs new work normally",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const { h, root } = yield* openNodes(yield* controlledStorage);
          let conversation: ConversationId | undefined;
          script("agent", {
            run: (runtime) =>
              runtime.commit(
                Effect.gen(function* () {
                  const tx = yield* Tx;
                  conversation = (yield* tx.createConversation({
                    ownership: { kind: "task", taskId: runtime.taskId },
                  })).id;
                  return toResume();
                }),
              ),
            // Blocks until the invocation is interrupted.
            resume: () => Effect.never,
          });
          const agent = yield* start(root, "agent");
          yield* until(logged("resume:agent"));
          yield* h.harness.abortTask(agent);
          assert.strictEqual(yield* outcomeOf(h, agent), "aborted");
          const child = Option.getOrThrow(yield* h.harness.conversation(conversation!));
          const question = yield* start(child, "question");
          open("question");
          assert.strictEqual(yield* outcomeOf(h, question), "completed");
          yield* h.close;
        }),
      ),
  );
});

// ─── Recovery ───────────────────────────────────────────────────────────────

interface Seeded {
  readonly parent: TaskId;
  readonly children: Array<TaskId>;
}

type Patch = Partial<Pick<TaskRecord, "state" | "abortRequested">>;

/**
 * Write records a crash could leave, without a Harness: `parent` with children named `children`, then `edit`
 * replaces states through the internal task write.
 */
const seed = (
  storage: ControlledStorage,
  children: ReadonlyArray<string>,
  edit: (
    set: (id: TaskId, patch: Patch) => Effect.Effect<void>,
    seeded: Seeded,
  ) => Effect.Effect<void>,
) =>
  withSession(storage, (session) =>
    Effect.gen(function* () {
      const seeded = yield* session.commit(
        Effect.gen(function* () {
          const tx = yield* Tx;
          const root = yield* tx.createConversation({ ownership: { kind: "ownerless" } });
          const parent = yield* tx.createTask(
            Node,
            { name: "parent" },
            { ...OWN_CONVERSATION, conversationId: root.id },
          );
          const ids: Array<TaskId> = [];
          for (const name of children) ids.push(yield* spawn(parent, name));
          return { parent, children: ids };
        }),
      );
      // The kernel's `commitWith` exposes `setTask`.
      const set = (id: TaskId, patch: Patch) =>
        session
          .commitWith((tx) =>
            Effect.gen(function* () {
              const found = Option.getOrThrow(yield* tx.service.task(id));
              // SAFETY: the patch puts a state or a mark of a live record.
              yield* tx.setTask({ ...found, ...patch } as TaskRecord);
            }),
          )
          .pipe(Effect.orDie);
      yield* edit(set, seeded);
      return seeded;
    }),
  ).pipe(Effect.orDie);

const COMPLETED = { status: "terminal", outcome: { status: "completed", result: "done" } } as const;
const FAILED = {
  status: "terminal",
  outcome: { status: "failed", error: { message: "declined" } },
} as const;

const waitingOn = (on: ReadonlyArray<TaskId>, policy: JoinPolicy) =>
  ({ status: "waiting", checkpoint: { phase: "resume", round: 1 }, on, policy }) as const;

describeEachBackend("recovery", (backend) => {
  it.effect("resumes a parent whose awaited children finished before the crash", () =>
    run(backend)(
      Effect.gen(function* () {
        const storage = yield* controlledStorage;
        const { parent, children } = yield* seed(storage, ["c1"], (set, seeded) =>
          Effect.gen(function* () {
            yield* set(seeded.children[0]!, { state: COMPLETED });
            yield* set(seeded.parent, { state: waitingOn(seeded.children, "allSettled") });
          }),
        );
        const { h } = yield* openNodes(storage);
        assert.strictEqual(yield* outcomeOf(h, parent), "completed");
        assert.deepStrictEqual(log, ["resume:parent"]);
        assert.lengthOf(children, 1);
        yield* h.close;
      }),
    ),
  );

  it.effect("marks failFast siblings a crash left unmarked", () =>
    run(backend)(
      Effect.gen(function* () {
        const storage = yield* controlledStorage;
        const { parent, children } = yield* seed(storage, ["c1", "c2"], (set, seeded) =>
          Effect.gen(function* () {
            yield* set(seeded.children[0]!, { state: FAILED });
            yield* set(seeded.parent, { state: waitingOn(seeded.children, "failFast") });
          }),
        );
        const { h } = yield* openNodes(storage);
        assert.strictEqual(yield* outcomeOf(h, children[1]!), "aborted");
        assert.strictEqual(yield* outcomeOf(h, parent), "completed");
        assert.notInclude(log, "run:c2");
        yield* h.close;
      }),
    ),
  );

  it.effect(
    "finalizes a held outcome whose work drained before the crash, and keeps one whose work lives",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const storage = yield* controlledStorage;
          const held = {
            status: "completing",
            outcome: { status: "completed", result: "parent" },
          } as const;
          const { parent, children } = yield* seed(storage, ["c1"], (set, seeded) =>
            set(seeded.parent, { state: held }),
          );
          let opened = yield* openNodes(storage);
          yield* until(logged("run:c1"));
          assert.deepStrictEqual(yield* state(opened.h, parent), held);
          yield* opened.h.close;

          opened = yield* openNodes(storage);
          open("c1");
          assert.strictEqual(yield* outcomeOf(opened.h, children[0]!), "completed");
          assert.strictEqual(yield* outcomeOf(opened.h, parent), "completed");
          yield* opened.h.close;

          const drained = yield* freshStorage(backend);
          const second = yield* seed(drained, ["c1"], (set, seeded) =>
            Effect.gen(function* () {
              yield* set(seeded.children[0]!, { state: COMPLETED });
              yield* set(seeded.parent, { state: held });
            }),
          );
          opened = yield* openNodes(drained);
          assert.strictEqual(yield* outcomeOf(opened.h, second.parent), "completed");
          yield* opened.h.close;
        }),
      ),
  );

  it.effect("resumes a bottom-up abort a crash interrupted before the cascade", () =>
    run(backend)(
      Effect.gen(function* () {
        const storage = yield* controlledStorage;
        const { parent, children } = yield* seed(storage, ["c1"], (set, seeded) =>
          set(seeded.parent, {
            abortRequested: true,
            state: waitingOn(seeded.children, "allSettled"),
          }),
        );
        const { h } = yield* openNodes(storage);
        assert.strictEqual(yield* outcomeOf(h, parent), "aborted");
        assert.strictEqual(yield* outcomeOf(h, children[0]!), "aborted");
        assert.deepStrictEqual(
          log.filter((line) => line.startsWith("abort:")),
          ["abort:c1", "abort:parent"],
        );
        yield* h.close;
      }),
    ),
  );

  it.effect("reopens a checkout waiting on live payments and finishes it", () =>
    run(backend)(
      Effect.gen(function* () {
        const storage = yield* controlledStorage;
        const found = parentOf("checkout", ["p1", "p2"], "failFast");
        let opened = yield* openNodes(storage);
        const parent = yield* start(opened.root, "checkout");
        yield* until(statusIs(opened.h, parent, "waiting"));
        yield* until(Effect.sync(() => log.includes("run:p1") && log.includes("run:p2")));
        yield* opened.h.close;

        opened = yield* openNodes(storage);
        const waited = yield* state(opened.h, parent);
        found.ids.splice(
          0,
          found.ids.length,
          // SAFETY: the checkout waits on its payments, whose results are strings.
          ...(waited.status === "waiting" ? (waited.on as ReadonlyArray<TaskId<string>>) : []),
        );
        open("p1");
        open("p2");
        assert.strictEqual(yield* outcomeOf(opened.h, parent), "completed");
        assert.deepStrictEqual(found.outcomes, ["completed", "completed"]);
        yield* opened.h.close;
      }),
    ),
  );
});

// ─── Built-in tool rounds ───────────────────────────────────────────────────

const NoParameters = Schema.Struct({});

/** An extension with one tool. */
const toolExtension = (agentTool: AgentTool.Any) =>
  Extension.make({ name: `tool:${agentTool.name}`, tools: [agentTool] });

/** An extension with the `Node` task. */
const NodeExtension = Extension.make({ name: "test.nodes", tasks: [Node] });

/** An extension with one hook. */
const hookExtension = (name: string, hook: Hook.Any) => Extension.make({ name, hooks: [hook] });

const noop = AgentTool.make("noop", {
  description: "Does nothing",
  parameters: NoParameters,
  execute: () => Effect.succeed({ output: [] }),
});

/** A tool that blocks, and when it started. */
interface Blocking {
  readonly started: Deferred.Deferred<void>;
  readonly registration: AgentTool.Any;
}

/** A tool that writes output, then blocks until its invocation is interrupted. */
function blockingTool(name: string, executionMode: ToolExecutionMode = "sequential"): Blocking {
  const started = Deferred.makeUnsafe<void>();
  return {
    started,
    registration: AgentTool.make(name, {
      description: `The ${name} tool`,
      parameters: NoParameters,
      executionMode,
      execute: () =>
        Effect.gen(function* () {
          const api = yield* ToolContext;
          yield* api.output("partial");
          yield* Deferred.succeed(started, undefined);
          return yield* Effect.never;
        }).pipe(Effect.orDie),
    }),
  };
}

function toolCalls(...ids: ReadonlyArray<readonly [string, string]>): AssistantMessage {
  return fauxAssistantMessage(
    ids.map(([name, id]) => fauxToolCall(name, {}, { id })),
    { stopReason: "toolUse" },
  );
}

/** Runs `effect` to its end while the `TestClock` advances. */
const finished = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  Effect.gen(function* () {
    const fiber = yield* Effect.forkChild(effect, { startImmediately: true });
    yield* waitFor(Effect.sync(() => fiber.pollUnsafe() !== undefined));
    return yield* Fiber.join(fiber);
  });

/** Awaits a submission while the `TestClock` advances. */
const settle = (submission: Submission) => finished(submission.await);

const live = (h: TestHarness, conversation: Conversation) =>
  h.session.snapshot(LiveDoc, conversation.id).pipe(Effect.map(Option.getOrUndefined));

const resultMessages = (entries: ReadonlyArray<EntryRecord>): Array<ToolResultMessage> =>
  entries.flatMap((entry) => {
    const message = entry.model?.[0];
    return ToolResultEntry.is(entry) && message?.role === "toolResult" ? [message] : [];
  });

const tasksIn = (h: TestHarness, conversation: Conversation, kind?: string) =>
  h.harness.commit(
    Effect.gen(function* () {
      const tx = yield* Tx;
      return (yield* tx.scanTasks(
        kind === undefined
          ? { conversationId: conversation.id }
          : { conversationId: conversation.id, kind },
        20,
      )).items;
    }),
  );

/** A Models layer whose streams are `stream`, resolving the fake model's references (`HarnessGeneration`). */
const withStream = (
  setup: ChatSetup,
  stream: (model: ResolvedModel, request: GenerateRequest) => Stream.Stream<AssistantMessageEvent>,
): ChatSetup => ({
  ...setup,
  faux: {
    ...setup.faux,
    layer: Layer.succeed(
      Models,
      Models.of({
        resolve: (ref) =>
          Effect.succeed(
            ref.provider === setup.faux.provider
              ? Option.fromUndefinedOr(setup.faux.getModel(ref.modelId))
              : Option.none(),
          ),
        stream,
      }),
    ),
  },
});

describeEachBackend("tool rounds", (backend) => {
  it.effect(
    "owns its tool tasks, waits for them, and hands the run to a conversation-owned generation",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const setup = chatSetup();
          setup.faux.setResponses([
            toolCalls(["noop", "c1"], ["noop", "c2"]),
            fauxAssistantMessage([fauxText("done")]),
          ]);
          const { harness, root } = yield* openChat(yield* controlledStorage, setup, {
            extensions: [toolExtension(noop)],
          });
          assert.strictEqual(
            (yield* settle(yield* root.submit({ type: "input", content: "go" }))).status,
            "done",
          );
          const tasks = yield* tasksIn(harness, root);
          const [first, second] = tasks.filter((task) => task.kind === "core.generation");
          assert.deepStrictEqual(
            tasks.filter((task) => task.kind === "core.tool").map((task) => task.owner),
            [first!.id, first!.id],
          );
          assert.isUndefined(first!.owner);
          assert.isUndefined(second!.owner);
          const assistant = (yield* allEntries(root)).find(
            (entry) => entry.kind === "core.assistant",
          )!;
          assert.deepStrictEqual(first!.state, {
            status: "terminal",
            outcome: { status: "completed", result: { entryId: assistant.id } },
          });
          yield* harness.close;
        }),
      ),
  );

  it.effect(
    "aborts a parallel round with its generation: tools first, then the generation, with every result written",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const setup = chatSetup();
          const one = blockingTool("one", "parallel");
          const two = blockingTool("two", "parallel");
          setup.faux.setResponses([toolCalls(["one", "c1"], ["two", "c2"])]);
          const { harness, root } = yield* openChat(yield* controlledStorage, setup, {
            extensions: [toolExtension(one.registration), toolExtension(two.registration)],
          });
          const submission = yield* root.submit({ type: "input", content: "go" });
          yield* waitFor(
            Effect.zipWith(
              Deferred.isDone(one.started),
              Deferred.isDone(two.started),
              (a, b) => a && b,
            ),
          );
          const generation = (yield* live(harness, root))!.run!.taskId;
          yield* finished(harness.harness.abortTask(generation));
          assert.containSubset(yield* settle(submission), {
            status: "unanswered",
            reason: "aborted",
          });
          const results = resultMessages(yield* allEntries(root));
          assert.deepStrictEqual(
            results.map((message) => message.toolCallId),
            ["c1", "c2"],
          );
          const failed = yield* live(harness, root);
          assert.deepStrictEqual(failed, {
            runFailure: {
              taskId: generation,
              inputs: [submission.id],
              reason: "aborted",
              first: anyNumber(failed?.runFailure?.first),
            },
          });
          assert.strictEqual(
            (yield* finished(harness.harness.awaitTask(generation))).state.outcome.status,
            "aborted",
          );
          yield* harness.close;
        }),
      ),
  );

  it.effect(
    "answers the unstarted calls of an aborted sequential round with aborted results, in call order",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const setup = chatSetup();
          const one = blockingTool("one");
          setup.faux.setResponses([toolCalls(["one", "c1"], ["two", "c2"], ["two", "c3"])]);
          const { harness, root } = yield* openChat(yield* controlledStorage, setup, {
            extensions: [
              toolExtension(one.registration),
              toolExtension(blockingTool("two").registration),
            ],
          });
          const submission = yield* root.submit({ type: "input", content: "go" });
          yield* waitFor(Deferred.isDone(one.started));
          assert.deepStrictEqual(
            (yield* live(harness, root))!.tools!.map((slot) => slot.taskId !== undefined),
            [true, false, false],
          );
          const generation = (yield* live(harness, root))!.run!.taskId;
          yield* finished(harness.harness.abortTask(generation));
          assert.containSubset(yield* settle(submission), {
            status: "unanswered",
            reason: "aborted",
          });
          const results = resultMessages(yield* allEntries(root));
          assert.deepStrictEqual(
            results.map((result) => result.toolCallId),
            ["c1", "c2", "c3"],
          );
          assert.deepStrictEqual(
            results.map((result) => result.isError),
            [true, true, true],
          );
          assert.deepStrictEqual(results[1]!.content, [
            { type: "text", text: "<harness>\n[error] Tool two was aborted\n</harness>" },
          ]);
          assert.lengthOf(yield* tasksIn(harness, root, "core.tool"), 1);
          yield* harness.close;
        }),
      ),
  );

  it.effect(
    "hands run control to the successor while the generation holds, and keeps it in the graph until it ends",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const setup = chatSetup();
          // An extension's hook starts work owned by the generation, which holds it while the next turn runs.
          let opened: TestHarness | undefined;
          const hooks = Hook.generation({
            afterTools: (_assistant, _results, api) =>
              opened!.harness
                .commit(
                  Effect.gen(function* () {
                    const tx = yield* Tx;
                    return yield* tx.createTask(Node, { name: "hooked" }, owned(api.taskId));
                  }),
                )
                .pipe(Effect.orDie, Effect.as(undefined)),
          });
          setup.faux.setResponses([
            toolCalls(["noop", "c1"]),
            fauxAssistantMessage([fauxText("done")]),
          ]);
          const { harness, root } = yield* openChat(yield* controlledStorage, setup, {
            extensions: [NodeExtension, toolExtension(noop), hookExtension("hooks", hooks)],
          });
          opened = harness;
          const graph = yield* graphHistory(harness);
          const history = yield* viewHistory(harness, root.id);
          yield* settle(yield* root.submit({ type: "input", content: "go" }));
          const [first, successor] = yield* scanGenerations(harness);
          assert.strictEqual((yield* state(harness, first!.id)).status, "completing");
          // The first generation's outcome is committed at its hold: the successor already runs the run, and the
          // graph shows the first one completing beside it.
          const handed = graph.graphs.find((value) => generations(value).length === 2)!;
          const [held, handedTo] = generations(handed);
          assert.strictEqual(held!.id, first!.id);
          assert.strictEqual(held!.state.status, "completing");
          assert.notStrictEqual(handedTo!.state.status, "completing");
          assert.notStrictEqual(successor!.state.status, "completing");
          assert.isTrue(history.live().some((value) => value.run?.taskId === successor!.id));
          open("hooked");
          assert.strictEqual(yield* outcomeOf(harness, first!.id), "completed");
          yield* waitFor(
            Effect.sync(() => graph.graphs.at(-1)!.tasks[String(first!.id)] === undefined),
          );
          graph.detach();
          yield* history.detach;
          yield* harness.close;
        }),
      ),
  );

  it.effect(
    "keeps run control with a faulted generation until its owned work drains, and fails the Harness on a failed final commit",
    () =>
      run(backend)(
        Effect.gen(function* () {
          // Adapted (as `HarnessGeneration`'s "faults a run task"): the `model` column is typed, so no non-JSON
          // final message reaches the classification commit; the fault is a defect of the model stream after its
          // first partial, which faults the generation the same way.
          const setup = withStream(chatSetup(), () =>
            Stream.make<ReadonlyArray<AssistantMessageEvent>>({
              type: "start",
              partial: fauxAssistantMessage("partial", { stopReason: "pending" }),
            }).pipe(
              Stream.concat(Stream.fromEffect(Effect.sleep("300 millis")).pipe(Stream.drain)),
              Stream.concat(Stream.die(new Error("Invalid final message"))),
            ),
          );
          // The partial is stored at once, so the failed final commit leaves it for the reopened run to convert;
          // held, it would be lost with the failed Session.
          setup.settings.progress = { persistWithinMs: 0 };
          let opened: TestHarness | undefined;
          const hooks = Hook.generation({
            beforeRequest: (_request, api) =>
              opened!.harness
                .commit(
                  Effect.gen(function* () {
                    const tx = yield* Tx;
                    yield* tx.createTask(Node, { name: "hooked" }, owned(api.taskId));
                    return undefined;
                  }),
                )
                .pipe(Effect.orDie),
          });
          script("hooked", {
            abort: (runtime) =>
              Effect.gen(function* () {
                yield* Deferred.await(gate("abort.hooked"));
                yield* runtime.commit(Effect.succeed(ABORTED));
              }),
          });
          const failure = diskGone();
          const reject: Refused = { id: undefined };
          const storage = yield* controlledStorage;
          const extensions = [NodeExtension, hookExtension("hooks", hooks)];
          const { harness, root } = yield* openChat(storage, setup, { extensions });
          opened = harness;
          const submission = yield* root.submit({ type: "input", content: "hi" });
          yield* waitFor(
            Effect.gen(function* () {
              const generation = (yield* live(harness, root))?.run?.taskId;
              return (
                generation !== undefined &&
                (yield* state(harness, generation)).status === "completing"
              );
            }),
          );
          const generation = (yield* live(harness, root))!.run!.taskId;
          assert.containSubset(yield* state(harness, generation), {
            status: "completing",
            outcome: { status: "faulted" },
          });
          const waiting = yield* fork(submission.await);
          assert.isFalse(yield* settled(waiting));
          assert.strictEqual((yield* live(harness, root))!.run?.taskId, generation);
          // The faulted outcome is cancellation intent: the hooked work is aborted, then the run settles.
          yield* waitFor(logged("abort:hooked"));
          reject.id = generation;
          refuseFinalization(storage, failure, reject);
          open("abort.hooked");
          // The final commit, with the run's cleanup, fails: nothing of the cleanup lands, and the Harness fails.
          const ended = yield* finished(harness.harness.closed);
          assert.strictEqual(ended.reason, "failed");
          assert.strictEqual(ended.reason === "failed" ? ended.error : undefined, failure);
          const exit = yield* Fiber.await(waiting);
          assert.isTrue(Exit.isFailure(exit));
          if (Exit.isFailure(exit)) {
            const error = Cause.squash(exit.cause);
            assert.containSubset(error, { _tag: "SessionFailed" });
          }
          yield* harness.close;
          // Reopening finalizes the run; the partial becomes one aborted entry.
          storage.intercept.commit = undefined;
          const reopened = yield* openChat(storage, setup, { extensions });
          opened = reopened.harness;
          yield* reopened.harness.harness.resume;
          const again = Option.getOrThrow(
            yield* reopened.harness.harness.submission(submission.id),
          );
          assert.containSubset(yield* settle(again), { status: "unanswered", reason: "faulted" });
          const failed = yield* live(reopened.harness, reopened.root);
          const detail = failed?.runFailure?.detail ?? assert.fail("no detail");
          // The detail is the stream's defect (above).
          assert.include(detail, "Invalid final message");
          assert.deepStrictEqual(failed, {
            runFailure: {
              taskId: anyNumber(failed?.runFailure?.taskId),
              inputs: [submission.id],
              reason: "faulted",
              detail,
              first: anyNumber(failed?.runFailure?.first),
            },
          });
          assert.deepStrictEqual(
            (yield* allEntries(reopened.root)).map((entry) => entry.kind),
            ["core.user", "core.assistant"],
          );
          yield* reopened.harness.close;
        }),
      ),
  );
});

// ─── Review follow-ups ──────────────────────────────────────────────────────

const VersionedCheckpoint = Schema.Union([
  Schema.Struct({ phase: Schema.Literal("wait") }),
  Schema.Struct({ phase: Schema.Literal("resume") }),
  Schema.Struct({ phase: Schema.Literal("migrated") }),
]);
// The input holds plain numbers: a task schema's type equals its JSON encoding.
const VersionedInput = Schema.Struct({ on: Schema.Array(Schema.Number) });

/** A waiter of its own kind, so its definition can be removed or replaced. */
function versioned(version: 1 | 2) {
  return Task.make({
    name: "test.versioned",
    version,
    input: VersionedInput,
    checkpoint: VersionedCheckpoint,
    result: Schema.String,
    initial: () => ({ phase: "wait" }),
    phases: {
      wait: (task, runtime) =>
        runtime.commit(
          Effect.succeed({
            status: "waiting",
            checkpoint: { phase: "resume" },
            on: task.input.on.map((id) => TaskId.make(id)),
            policy: "allSettled",
          } as const),
        ),
      resume: (_task, runtime) =>
        runtime.commit(
          Effect.succeed({
            status: "terminal",
            outcome: { status: "completed", result: "v1" },
          } as const),
        ),
      migrated: (_task, runtime) =>
        runtime.commit(
          Effect.succeed({
            status: "terminal",
            outcome: { status: "completed", result: "v2" },
          } as const),
        ),
    },
    abort: (_task, runtime) => runtime.commit(Effect.succeed(ABORTED)),
    migrate:
      version === 2
        ? (input) => ({
            input: Schema.decodeUnknownSync(VersionedInput)(input),
            checkpoint: { phase: "migrated" },
          })
        : undefined,
  });
}

const createVersioned = (conversation: Conversation, version: 1 | 2, on: ReadonlyArray<TaskId>) =>
  conversation.commit(
    Effect.gen(function* () {
      const tx = yield* Tx;
      return yield* tx.createTask(versioned(version), { on: [...on] }, OWN_CONVERSATION);
    }),
  );

describeEachBackend("definitions and waits", (backend) => {
  it.effect(
    "keeps a waiting task blocked without its definition and resumes it migrated under a newer one",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const storage = yield* controlledStorage;
          let opened = yield* openNodes(storage, [Node, versioned(1)]);
          const other = yield* start(opened.root, "other");
          const waiter = yield* createVersioned(opened.root, 1, [other]);
          yield* until(statusIs(opened.h, waiter, "waiting"));
          // The registry is static: the Harness reopens without the registration.
          yield* opened.h.close;
          opened = yield* openNodes(storage, [Node]);
          open("other");
          yield* settledTask(opened.h, other);
          // `message` (`TaskInspection`) is absent here.
          assert.deepStrictEqual(yield* inspected(opened.h, waiter), {
            kind: "blocked",
            reason: "missing_task",
            message: undefined,
          });
          assert.strictEqual((yield* state(opened.h, waiter)).status, "waiting");
          // The registry is static: a reopen with the newer definition.
          yield* opened.h.close;
          opened = yield* openNodes(storage, [Node, versioned(2)]);
          assert.deepStrictEqual((yield* settledTask(opened.h, waiter)).state.outcome, {
            status: "completed",
            result: "v2",
          });
          yield* opened.h.close;
        }),
      ),
  );

  it.effect(
    "orphans an aborted waiting task without its definition at once, leaving the task it waits on running",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const storage = yield* controlledStorage;
          let opened = yield* openNodes(storage, [Node, versioned(1)]);
          const other = yield* start(opened.root, "other");
          const waiter = yield* createVersioned(opened.root, 1, [other]);
          yield* until(statusIs(opened.h, waiter, "waiting"));
          // The registry is static: the Harness reopens without the registration.
          yield* opened.h.close;
          opened = yield* openNodes(storage, [Node]);
          yield* until(logged("run:other"));
          assert.deepStrictEqual(yield* opened.h.harness.abortTask(waiter), { _tag: "Marked" });
          const after = yield* state(opened.h, waiter);
          assert.deepStrictEqual(after.status === "terminal" ? after.outcome : undefined, {
            status: "orphaned",
            reason: "missing_task",
          });
          assert.strictEqual((yield* state(opened.h, other)).status, "running");
          open("other");
          yield* opened.h.close;
        }),
      ),
  );

  it.effect(
    "never migrates a held outcome: a newer definition leaves it and its version alone",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const storage = yield* controlledStorage;
          const holder = (version: number) =>
            Task.make({
              name: "test.holder",
              version,
              input: Schema.Null,
              checkpoint: Schema.Struct({ phase: Schema.Literal("run") }),
              result: Schema.String,
              initial: () => ({ phase: "run" }),
              phases: {
                run: (task, runtime) =>
                  Effect.gen(function* () {
                    yield* runtime.commit(
                      Effect.gen(function* () {
                        yield* spawn(task.id, "child");
                        return undefined;
                      }),
                    );
                    yield* runtime.commit(
                      Effect.succeed({
                        status: "terminal",
                        outcome: { status: "completed", result: "held" },
                      } as const),
                    );
                  }),
              },
              abort: (_task, runtime) => runtime.commit(Effect.succeed(ABORTED)),
              migrate: () => {
                throw new Error("never migrates");
              },
            });
          let opened = yield* openNodes(storage, [Node, holder(1)]);
          const parent = yield* opened.root.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              return yield* tx.createTask(holder(1), null, OWN_CONVERSATION);
            }),
          );
          yield* until(statusIs(opened.h, parent, "completing"));
          // The registry is static: a reopen with the newer definition.
          yield* opened.h.close;
          opened = yield* openNodes(storage, [Node, holder(2)]);
          open("child");
          const settledParent = yield* settledTask(opened.h, parent);
          assert.deepStrictEqual(settledParent.state.outcome, {
            status: "completed",
            result: "held",
          });
          assert.strictEqual(settledParent.version, 1);
          assert.deepStrictEqual(opened.h.reports, []);
          yield* opened.h.close;
        }),
      ),
  );

  it.effect(
    "treats a held task as live: outcomes() rejects, and a task waiting on it resumes at its final commit",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const { h, root } = yield* openNodes(yield* controlledStorage);
          spawnAndFinish("held", "child");
          const held = yield* start(root, "held");
          yield* until(statusIs(h, held, "completing"));
          let rejection = "";
          script("reader", {
            run: (runtime) =>
              Effect.gen(function* () {
                rejection = yield* runtime.outcomes([held]).pipe(
                  Effect.match({
                    onSuccess: () => "resolved",
                    onFailure: (error) => error.message,
                  }),
                );
                yield* runtime.commit(Effect.succeed(waitOn([held], "allSettled")));
              }),
          });
          const reader = yield* start(root, "reader");
          yield* until(statusIs(h, reader, "waiting"));
          assert.strictEqual(rejection, `Task ${held} is not terminal`);
          open("child");
          assert.strictEqual(yield* outcomeOf(h, reader), "completed");
          assert.isAbove(log.indexOf("resume:reader"), -1);
          yield* h.close;
        }),
      ),
  );
});

describeEachBackend("conversation abort and boundaries", (backend) => {
  it.effect(
    "marks a held completed task with Conversation.abort(): the work below is aborted, the outcome stays",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const { h, root } = yield* openNodes(yield* controlledStorage);
          const child = spawnAndFinish("held", "child");
          const held = yield* start(root, "held");
          yield* until(statusIs(h, held, "completing"));
          yield* bounded(root.abort());
          assert.strictEqual(yield* outcomeOf(h, child.id!), "aborted");
          const found = yield* settledTask(h, held);
          assert.strictEqual(found.state.outcome.status, "completed");
          assert.isTrue(found.abortRequested);
          yield* h.close;
        }),
      ),
  );

  it.effect(
    "marks and awaits only the background work reached when { background: true } is admitted",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const { h, root } = yield* openNodes(yield* controlledStorage);
          script("first", {
            abort: (runtime) =>
              Effect.gen(function* () {
                yield* Deferred.await(gate("abort.first"));
                yield* runtime.commit(Effect.succeed(ABORTED));
              }),
          });
          const background: Task.TaskOptions = { ...OWN_CONVERSATION, background: true };
          const first = yield* start(root, "first", background);
          yield* until(logged("run:first"));
          const aborting = yield* fork(root.abort({ background: true }));
          yield* until(abortRequested(h, first));
          const later = yield* start(root, "later", background);
          open("abort.first");
          yield* joined(aborting);
          assert.notStrictEqual((yield* state(h, later)).status, "terminal");
          assert.isFalse(yield* abortRequested(h, later));
          yield* h.harness.abortTask(later);
          yield* h.close;
        }),
      ),
  );

  it.effect("stops a cascade at an unmarked background task, but not at a marked one", () =>
    run(backend)(
      Effect.gen(function* () {
        const { h, root } = yield* openNodes(yield* controlledStorage);
        script("owner", {
          abort: (runtime) =>
            Effect.gen(function* () {
              yield* Deferred.await(gate("abort.owner"));
              yield* runtime.commit(Effect.succeed(ABORTED));
            }),
        });
        const tree = yield* root.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            const owner = yield* tx.createTask(Node, { name: "owner" }, OWN_CONVERSATION);
            const outer = yield* tx.createConversation({
              ownership: { kind: "task", taskId: owner },
            });
            const background = yield* tx.createTask(
              Node,
              { name: "background" },
              { ...OWN_CONVERSATION, conversationId: outer.id, background: true },
            );
            const inner = yield* tx.createConversation({
              ownership: { kind: "task", taskId: background },
            });
            return { owner, outer: outer.id, background, inner: inner.id };
          }),
        );
        yield* until(
          Effect.sync(() => log.includes("run:owner") && log.includes("run:background")),
        );
        // The owner's abort handler starts at once: background work is not its ordinary owned work.
        yield* h.harness.abortTask(tree.owner);
        yield* until(logged("abort:owner"));
        const inner = Option.getOrThrow(yield* h.harness.conversation(tree.inner));
        const outer = Option.getOrThrow(yield* h.harness.conversation(tree.outer));
        const shielded = yield* start(inner, "shielded");
        const exposed = yield* start(outer, "exposed");
        assert.strictEqual(yield* outcomeOf(h, exposed), "aborted");
        assert.isFalse(yield* abortRequested(h, shielded));
        // Marked directly, the background task cascades into its own subtree.
        yield* h.harness.abortTask(tree.background);
        assert.strictEqual(yield* outcomeOf(h, shielded), "aborted");
        assert.strictEqual(yield* outcomeOf(h, tree.background), "aborted");
        open("abort.owner");
        assert.strictEqual(yield* outcomeOf(h, tree.owner), "aborted");
        yield* h.close;
      }),
    ),
  );

  it.effect("fails the Harness on a failed finalization, which reopening applies", () =>
    run(backend)(
      Effect.gen(function* () {
        const failure = diskGone();
        const reject: Refused = { id: undefined };
        const storage = yield* controlledStorage;
        refuseFinalization(storage, failure, reject);
        const { h, root } = yield* openNodes(storage);
        spawnAndFinish("parent", "child");
        const parent = yield* start(root, "parent");
        yield* until(statusIs(h, parent, "completing"));
        reject.id = parent;
        open("child");
        const ended = yield* bounded(h.harness.closed);
        assert.strictEqual(ended.reason, "failed");
        assert.strictEqual(ended.reason === "failed" ? ended.error : undefined, failure);
        // Reports are Causes, delivered on a fiber of their own: one report whose Cause fails with the storage
        // failure alone.
        yield* flush;
        assert.deepStrictEqual(
          h.reports.map((report) =>
            report.cause.reasons.map((reason) =>
              Cause.isFailReason(reason) ? reason.error : reason,
            ),
          ),
          [[failure]],
        );
        yield* h.close;
        // Reopens the storage without the refusal.
        storage.intercept.commit = undefined;
        const reopened = yield* openNodes(storage);
        assert.strictEqual(yield* outcomeOf(reopened.h, parent), "completed");
        yield* reopened.h.close;
      }),
    ),
  );
});

describeEachBackend("recovery through ownership edges", (backend) => {
  it.effect(
    "shows an abort-marked owner waiting for work in its owned conversation before resume after reopen",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const storage = yield* controlledStorage;
          const tree = yield* withSession(storage, (session) =>
            Effect.gen(function* () {
              const created = yield* session.commit(
                Effect.gen(function* () {
                  const tx = yield* Tx;
                  const conversation = yield* tx.createConversation({
                    ownership: { kind: "ownerless" },
                  });
                  const owner = yield* tx.createTask(
                    Node,
                    { name: "owner" },
                    { ...OWN_CONVERSATION, conversationId: conversation.id },
                  );
                  const child = yield* tx.createConversation({
                    ownership: { kind: "task", taskId: owner },
                  });
                  const inner = yield* tx.createTask(
                    Node,
                    { name: "inner" },
                    { ...OWN_CONVERSATION, conversationId: child.id },
                  );
                  return { owner, inner };
                }),
              );
              // The kernel's `commitWith` exposes `setTask`.
              yield* session.commitWith((tx) =>
                Effect.gen(function* () {
                  const found = Option.getOrThrow(yield* tx.service.task(created.owner));
                  yield* tx.setTask({ ...found, abortRequested: true });
                }),
              );
              return created;
            }),
          ).pipe(Effect.orDie);

          const h = yield* openHarness(storage, [Node]);
          assert.deepStrictEqual(yield* inspected(h, tree.owner), {
            kind: "waiting",
            on: [tree.inner],
          });
          yield* h.harness.resume;
          assert.strictEqual(yield* outcomeOf(h, tree.owner), "aborted");
          assert.deepStrictEqual(
            log.filter((line) => line.startsWith("abort:")),
            ["abort:inner", "abort:owner"],
          );
          yield* h.close;
        }),
      ),
  );

  it.effect(
    "keeps the root busy after reopen for work below a child task's owned conversation",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const storage = yield* controlledStorage;
          let inner: TaskId | undefined;
          script("child", {
            run: (runtime) =>
              runtime.commit(
                Effect.gen(function* () {
                  const tx = yield* Tx;
                  const conversation = yield* tx.createConversation({
                    ownership: { kind: "task", taskId: runtime.taskId },
                  });
                  inner = yield* tx.createTask(
                    Node,
                    { name: "inner" },
                    { ...OWN_CONVERSATION, conversationId: conversation.id },
                  );
                  return toResume();
                }),
              ),
          });
          parentOf("parent", ["child"], "allSettled");
          let opened = yield* openNodes(storage);
          const parent = yield* start(opened.root, "parent");
          yield* until(logged("run:inner"));
          yield* opened.h.close;

          opened = yield* openNodes(storage);
          assert.isFalse(yield* settled(yield* fork(opened.root.awaitIdle)));
          open("inner");
          yield* bounded(opened.root.awaitIdle);
          assert.strictEqual(yield* outcomeOf(opened.h, parent), "completed");
          assert.isDefined(inner);
          yield* opened.h.close;
        }),
      ),
  );

  it.effect(
    "keeps a finished background ancestor a boundary after reopen, which { background: true } crosses",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const storage = yield* controlledStorage;
          let conversation: ConversationId | undefined;
          script("child", {
            run: (runtime) =>
              runtime.commit(
                Effect.gen(function* () {
                  const tx = yield* Tx;
                  conversation = (yield* tx.createConversation({
                    ownership: { kind: "task", taskId: runtime.taskId },
                  })).id;
                  return toResume();
                }),
              ),
          });
          parentOf("background", ["child"], "allSettled");
          let opened = yield* openNodes(storage);
          const background = yield* start(opened.root, "background", {
            ...OWN_CONVERSATION,
            background: true,
          });
          yield* settledTask(opened.h, background);
          yield* opened.h.close;

          opened = yield* openNodes(storage);
          const below = yield* start(
            Option.getOrThrow(yield* opened.h.harness.conversation(conversation!)),
            "below",
          );
          yield* until(logged("run:below"));
          yield* bounded(opened.root.awaitIdle);
          yield* bounded(opened.root.abort());
          assert.strictEqual((yield* state(opened.h, below)).status, "running");
          yield* bounded(opened.root.abort({ background: true }));
          assert.strictEqual(yield* outcomeOf(opened.h, below), "aborted");
          yield* opened.h.close;
        }),
      ),
  );
});

// ─── Tool rounds as the view shows them ─────────────────────────────────────

/** A view's `core.live`, or `{}` when it is absent. */
// SAFETY: the mount holds `core.live` as its definition materializes it.
const liveOf = (view: ConversationView) => (view.docs["core.live"] ?? {}) as LiveState;

/** Every view of `conversationId` from now on, one per commit, unfolded. */
const viewHistory = (h: TestHarness, conversationId: ConversationId) =>
  Effect.gen(function* () {
    const views: Array<ConversationView> = [];
    const scope = yield* Scope.make();
    yield* Scope.provide(
      h.views.attach(conversationId, undefined, (value) => {
        views.push(value);
        return {
          advance: (frame) => {
            views.push(frame.value);
          },
          closeSession: () => {},
        };
      }),
      scope,
    );
    return {
      views,
      live: (): ReadonlyArray<LiveState> => views.map(liveOf),
      detach: Scope.close(scope, Exit.void),
    };
  });

/** The slots of tool call `callId` in each `core.live` value. */
const slotsOf = (
  history: ReadonlyArray<LiveState>,
  callId: string,
): ReadonlyArray<ToolSlot | undefined> =>
  history.map((value) => (value.tools ?? []).find((slot) => slot.callId === callId));

/** Every task graph an observer gets, from the one at attachment. */
const graphHistory = (h: TestHarness) =>
  Effect.gen(function* () {
    const graphs: Array<TaskGraph> = [];
    const observation = yield* h.harness.observeTaskGraph({
      advance: (frame) => {
        graphs.push(frame.value);
      },
      end: () => {},
    });
    graphs.push(observation.value);
    return { graphs, detach: observation.detach };
  });

/** The live `core.generation` nodes of `graph`, in task ID order. */
const generations = (graph: TaskGraph): Array<TaskGraphNode> =>
  Object.values(graph.tasks)
    .filter((node) => node.kind === "core.generation")
    .toSorted((a, b) => a.id - b.id);

/** The Harness's generations, oldest first. */
const scanGenerations = (h: TestHarness) =>
  h.harness.commit(
    Effect.gen(function* () {
      const tx = yield* Tx;
      return (yield* tx.scanTasks({ kind: "core.generation" }, 20)).items;
    }),
  );

/** The tool result entries of a view, by call ID, in transcript order. */
function resultsOf(view: ConversationView): Array<string> {
  return view.entries.flatMap((entry) => {
    const message = entry.model?.[0];
    return message?.role === "toolResult" ? [message.toolCallId] : [];
  });
}

describeEachBackend("tool rounds as the view shows them", (backend) => {
  it.effect(
    "ends an aborted sequential round's unstarted calls with their result entries, right before them",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const setup = chatSetup();
          const one = blockingTool("one");
          setup.faux.setResponses([toolCalls(["one", "c1"], ["two", "c2"], ["two", "c3"])]);
          const { harness, root } = yield* openChat(yield* controlledStorage, setup, {
            extensions: [
              toolExtension(one.registration),
              toolExtension(blockingTool("two").registration),
            ],
          });
          const history = yield* viewHistory(harness, root.id);
          const submission = yield* root.submit({ type: "input", content: "go" });
          yield* waitFor(Deferred.isDone(one.started));
          yield* finished(harness.harness.abortTask((yield* live(harness, root))!.run!.taskId));
          yield* settle(submission);
          yield* history.detach;
          // The started call ends with its result entry; the commit that ends the run appends the unstarted calls'
          // results, in call order, and removes the round.
          const states = history.live();
          const c1Done = slotsOf(states, "c1").find((slot) => slot?.status === "done");
          const last = history.views.at(-1)!;
          assert.deepStrictEqual(resultsOf(last), ["c1", "c2", "c3"]);
          const c1Result = last.entries.find((entry) => {
            const message = entry.model?.[0];
            return message?.role === "toolResult" && message.toolCallId === "c1";
          });
          assert.strictEqual(c1Done?.entry, c1Result?.id);
          const ending = history.views.findIndex((view) => resultsOf(view).includes("c2"));
          assert.deepStrictEqual(resultsOf(history.views[ending]!), ["c1", "c2", "c3"]);
          assert.isUndefined(states[ending]!.run);
          assert.isUndefined(states[ending]!.tools);
          assert.deepStrictEqual(
            states[ending - 1]!.tools?.map((slot) => [slot.callId, slot.status]),
            [
              ["c1", "done"],
              ["c2", "pending"],
              ["c3", "pending"],
            ],
          );
          yield* harness.close;
        }),
      ),
  );

  it.effect("shows a held generation as completing to an observer attached while it holds", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        let opened: TestHarness | undefined;
        const hooks = Hook.generation({
          afterTools: (_assistant, _results, api) =>
            opened!.harness
              .commit(
                Effect.gen(function* () {
                  const tx = yield* Tx;
                  return yield* tx.createTask(Node, { name: "hooked" }, owned(api.taskId));
                }),
              )
              .pipe(Effect.orDie, Effect.as(undefined)),
        });
        setup.faux.setResponses([
          toolCalls(["noop", "c1"]),
          fauxAssistantMessage([fauxText("done")]),
        ]);
        const { harness, root } = yield* openChat(yield* controlledStorage, setup, {
          extensions: [NodeExtension, toolExtension(noop), hookExtension("hooks", hooks)],
        });
        opened = harness;
        const early = yield* graphHistory(harness);
        yield* settle(yield* root.submit({ type: "input", content: "go" }));
        const late = yield* graphHistory(harness);
        open("hooked");
        const [first] = yield* scanGenerations(harness);
        yield* settledTask(harness, first!.id);
        // The early observer saw both generations; the late one starts with the held one, completing, and sees it
        // leave.
        const seen = (graphs: ReadonlyArray<TaskGraph>) =>
          new Set(graphs.flatMap((value) => generations(value).map((node) => node.id)));
        assert.strictEqual(seen(early.graphs).size, 2);
        const [startsWith, ...others] = generations(late.graphs[0]!);
        assert.strictEqual(startsWith?.id, first!.id);
        assert.strictEqual(startsWith?.state.status, "completing");
        assert.lengthOf(others, 0);
        assert.isUndefined(late.graphs.at(-1)!.tasks[String(first!.id)]);
        early.detach();
        late.detach();
        yield* harness.close;
      }),
    ),
  );

  it.effect(
    "lets a tool that owns live work finish its call at the hold while the generation waits for its final commit",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const setup = chatSetup();
          const delegate = AgentTool.make("delegate", {
            description: "Starts work in a conversation it owns",
            parameters: NoParameters,
            execute: () =>
              Effect.gen(function* () {
                const api = yield* ToolContext;
                yield* api.commit(
                  Effect.gen(function* () {
                    const tx = yield* Tx;
                    const child = yield* tx.createConversation({
                      ownership: { kind: "task", taskId: api.taskId },
                    });
                    yield* tx.createTask(
                      Node,
                      { name: "sub" },
                      { ...OWN_CONVERSATION, conversationId: child.id },
                    );
                  }),
                );
                return { output: [{ type: "text" as const, text: "started" }] };
              }).pipe(Effect.orDie),
          });
          setup.faux.setResponses([
            toolCalls(["delegate", "c1"]),
            fauxAssistantMessage([fauxText("done")]),
          ]);
          const { harness, root } = yield* openChat(yield* controlledStorage, setup, {
            extensions: [NodeExtension, toolExtension(delegate)],
          });
          const submission = yield* root.submit({ type: "input", content: "go" });
          yield* waitFor(
            live(harness, root).pipe(Effect.map((value) => value?.tools?.[0]?.status === "done")),
          );
          const value = (yield* live(harness, root))!;
          const tool = value.tools![0]!.taskId!;
          assert.isDefined(value.tools![0]!.entry);
          assert.strictEqual((yield* state(harness, tool)).status, "completing");
          assert.strictEqual((yield* state(harness, value.run!.taskId)).status, "waiting");
          assert.isFalse(yield* settled(yield* fork(submission.await)));
          open("sub");
          assert.strictEqual((yield* settle(submission)).status, "done");
          assert.strictEqual(yield* outcomeOf(harness, tool), "completed");
          yield* harness.close;
        }),
      ),
  );

  it.effect("holds a faulted tool's slot and its graph node until the work it owns drained", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        script("held", {
          abort: (runtime) =>
            Effect.gen(function* () {
              yield* Deferred.await(gate("abort.held"));
              yield* runtime.commit(Effect.succeed(ABORTED));
            }),
        });
        // As in `HarnessToolsRecovery`'s "lets context derivation answer a faulted tool": the result carries a
        // content block that is JSON but not valid model content, which the result commit rejects.
        const block: TextContent = { type: "text", text: "" };
        Reflect.deleteProperty(block, "text");
        const broken: ToolResultContent = [block];
        const brokenTool = AgentTool.make("broken", {
          description: "Starts owned work, then returns a result that is not strict JSON",
          parameters: NoParameters,
          execute: () =>
            Effect.gen(function* () {
              const api = yield* ToolContext;
              yield* api.commit(
                Effect.gen(function* () {
                  const tx = yield* Tx;
                  return yield* tx.createTask(Node, { name: "held" }, owned(api.taskId));
                }),
              );
              return { output: broken };
            }).pipe(Effect.orDie),
        });
        setup.faux.setResponses([
          toolCalls(["broken", "c1"]),
          fauxAssistantMessage([fauxText("done")]),
        ]);
        const { harness, root } = yield* openChat(yield* controlledStorage, setup, {
          extensions: [NodeExtension, toolExtension(brokenTool)],
        });
        const history = yield* viewHistory(harness, root.id);
        const graph = yield* graphHistory(harness);
        const submission = yield* root.submit({ type: "input", content: "go" });
        yield* waitFor(logged("abort:held"));
        const value = (yield* live(harness, root))!;
        const tool = value.tools![0]!.taskId!;
        assert.containSubset(yield* state(harness, tool), {
          status: "completing",
          outcome: { status: "faulted" },
        });
        assert.notStrictEqual(value.tools![0]!.status, "done");
        // The graph shows the held fault; the task is not terminal yet.
        assert.deepStrictEqual(graph.graphs.at(-1)!.tasks[String(tool)]?.state, {
          status: "completing",
          outcome: "faulted",
        });
        open("abort.held");
        assert.strictEqual((yield* settle(submission)).status, "done");
        assert.strictEqual(yield* outcomeOf(harness, tool), "faulted");
        yield* waitFor(Effect.sync(() => graph.graphs.at(-1)!.tasks[String(tool)] === undefined));
        // The slot ends done without a result entry.
        const done = slotsOf(history.live(), "c1").find((slot) => slot?.status === "done");
        assert.isDefined(done);
        assert.notProperty(done, "entry");
        yield* history.detach;
        graph.detach();
        yield* harness.close;
      }),
    ),
  );
});

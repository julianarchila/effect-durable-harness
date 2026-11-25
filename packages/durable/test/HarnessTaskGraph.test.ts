// The task graph view, on every backend.
//
// Adaptations:
// - A watch's `start(listener)` is a consumer fiber over `frames`, and its `stop()` the end of the scope that
//   acquired it. Its `value` is the value of the last frame the consumer pulled.
// - Waits on the invocation are `Effect.never`, interrupted when an abort mark signals the invocation; a
//   cancelled caller context is an interrupted fiber running that call (`HarnessTasks.test.ts`).
// - "builds from committed tasks…" opens a new Harness over the same storage, on each backend.
// - No test checks that an observer runs outside storage adoption (`SessionObservers.test.ts`): the storage hands
//   no receipts outside `commit`, so every observer call is outside it.
import { assert, it } from "@effect/vitest";
import * as Delta from "@effect-durable-harness/delta/Delta";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import type { WatchFrame } from "../src/Doc.ts";
import type { ConversationId, EntryId, TaskId } from "../src/Ids.ts";
import type { Observer } from "../src/Observer.ts";
import * as Task from "../src/Task.ts";
import type { TaskGraph, TaskGraphWatch } from "../src/TaskGraph.ts";
import { Tx } from "../src/Tx.ts";
import type { ConversationView } from "../src/View.ts";
import {
  abortedWith,
  completed,
  eventually,
  holdLine,
  openHarness,
  type TestHarness,
} from "./HarnessSupport.ts";
import * as Cause from "effect/Cause";
import type { BlockedReason } from "../src/Harness.ts";
import type { Report } from "../src/Report.ts";
import * as TaskGraphs from "../src/internal/taskGraph.ts";
import {
  controlledStorage,
  createConversation,
  describeEachBackend,
  flush,
  openTestSession,
  run,
} from "./SessionSupport.ts";

const Work = Schema.Struct({ phase: Schema.Literal("work") });

const ParentState = Schema.Union([
  Schema.Struct({ phase: Schema.Literal("spawn") }),
  Schema.Struct({ phase: Schema.Literal("join"), child: Schema.Number }),
  Schema.Struct({ phase: Schema.Literal("finish") }),
]);

interface Gates {
  readonly child: Effect.Effect<void>;
  readonly late: Effect.Effect<void>;
}

/**
 * A parent that creates a child task and a conversation it owns, waits for the child, creates a second child, and
 * completes while that child is still live, so its outcome is held as `completing`.
 */
const family = (gates: Gates) => {
  const Child = Task.make({
    name: "test.graph-child",
    version: 1,
    input: Schema.Struct({ late: Schema.Boolean }),
    checkpoint: Work,
    result: Schema.Null,
    initial: () => ({ phase: "work" }),
    phases: {
      work: (task, runtime) =>
        Effect.gen(function* () {
          yield* task.input.late ? gates.late : gates.child;
          yield* runtime.commit(() => Effect.succeed(completed(null)));
        }),
    },
    abort: () => Effect.void,
  });
  const Parent = Task.make({
    name: "test.graph-parent",
    version: 1,
    input: Schema.Null,
    checkpoint: ParentState,
    result: Schema.Null,
    initial: () => ({ phase: "spawn" }),
    phases: {
      spawn: (task, runtime) =>
        Effect.gen(function* () {
          const ownership = { kind: "task", taskId: task.id } as const;
          // Two conversations in a commit that leaves the parent's record unchanged.
          yield* runtime.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              yield* tx.createConversation({ ownership });
              yield* tx.createConversation({ ownership });
            }),
          );
          yield* runtime.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              const child = yield* tx.createTask(Child, { late: false }, { ownership });
              return {
                status: "waiting",
                checkpoint: { phase: "join", child },
                on: [child],
                policy: "allSettled",
              } as const;
            }),
          );
        }),
      join: (task, runtime) =>
        runtime.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            yield* tx.createTask(
              Child,
              { late: true },
              { ownership: { kind: "task", taskId: task.id } },
            );
            return { status: "running", checkpoint: { phase: "finish" } } as const;
          }),
        ),
      finish: (_task, runtime) => runtime.commit(() => Effect.succeed(completed(null))),
    },
    abort: () => Effect.void,
  });
  return { Parent, Child };
};

const gatesNever: Gates = { child: Effect.never, late: Effect.never };

/** Node states by task kind, for compact assertions. */
const statuses = (graph: TaskGraph): Record<string, string> =>
  Object.fromEntries(
    Object.values(graph.tasks).map(
      (node) => [`${node.kind}#${node.id}`, node.state.status] as const,
    ),
  );

/** A watch acquired in a scope of its own, so the test stops it by closing that scope. */
interface OpenedWatch {
  readonly watch: TaskGraphWatch;
  readonly stop: Effect.Effect<void>;
}

const acquireWatch = (h: TestHarness) =>
  Effect.gen(function* () {
    const scope = yield* Scope.make();
    const watch = yield* h.harness.watchTaskGraph.pipe(Scope.provide(scope));
    return { watch, stop: Scope.close(scope, Exit.void) } satisfies OpenedWatch;
  });

/** A consumer fiber that pulls every frame. */
const start = (watch: TaskGraphWatch, listener: (frame: WatchFrame<TaskGraph>) => void) =>
  Effect.forkChild(
    Stream.runForEach(watch.frames, (frame) => Effect.sync(() => listener(frame))),
    { startImmediately: true },
  );

const createIn = <I extends Schema.Json, S extends { readonly phase: string }, R>(
  h: TestHarness,
  task: Task.Task<I, S, R>,
  input: I,
  options: Task.TaskOptions,
) =>
  Effect.gen(function* () {
    const root = yield* h.harness.ensureRoot();
    return yield* root.commit(
      Effect.gen(function* () {
        const tx = yield* Tx;
        return yield* tx.createTask(task, input, options);
      }),
    );
  });

describeEachBackend("task graph view", (backend) => {
  it.effect(
    "follows every live task through its statuses, owner edges, and owned conversations",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const child = yield* Deferred.make<void>();
          const late = yield* Deferred.make<void>();
          const { Parent, Child } = family({
            child: Deferred.await(child),
            late: Deferred.await(late),
          });
          const h = yield* openHarness(yield* controlledStorage, [Parent, Child]);
          const root = yield* h.harness.ensureRoot();
          const seen: Array<TaskGraph> = [];
          const observe = Effect.gen(function* () {
            const opened = yield* acquireWatch(h);
            yield* start(opened.watch, (frame) => seen.push(frame.value));
            return opened;
          });
          let graph = yield* observe;
          assert.deepStrictEqual(graph.watch.value, { tasks: {} });
          /** The advanced value equals a fresh build from storage once the last observer left. */
          const rebuild = Effect.gen(function* () {
            const advanced = graph.watch.value;
            yield* graph.stop;
            graph = yield* observe;
            assert.notStrictEqual(graph.watch.value, advanced);
            assert.deepStrictEqual(graph.watch.value, advanced);
          });

          const parent = yield* root.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              return yield* tx.createTask(Parent, null, { ownership: { kind: "conversation" } });
            }),
          );
          yield* flush;
          assert.deepStrictEqual(graph.watch.value.tasks[String(parent)], {
            id: parent,
            kind: "test.graph-parent",
            conversationId: root.id,
            background: false,
            abortRequested: false,
            state: { status: "pending", phase: "spawn" },
            conversations: [],
          });

          const childId = () =>
            Object.values(graph.watch.value.tasks)
              .find((node) => node.kind === "test.graph-child")
              ?.id.toString() ?? "";
          yield* h.harness.resume;
          yield* eventually(
            Effect.sync(
              () =>
                Object.keys(graph.watch.value.tasks).length === 2 &&
                statuses(graph.watch.value)[`test.graph-child#${childId()}`] === "running",
            ),
          );
          // SAFETY: the decimal key of a live task's node.
          const first = Number(childId()) as TaskId;
          const parentNode = graph.watch.value.tasks[String(parent)]!;
          assert.deepStrictEqual(parentNode.state, {
            status: "waiting",
            phase: "join",
            on: [first],
            policy: "allSettled",
          });
          assert.lengthOf(parentNode.conversations, 2);
          assert.deepStrictEqual(
            [...parentNode.conversations].toSorted((a, b) => a - b),
            [...parentNode.conversations],
          );
          const owned = parentNode.conversations[0]!;
          const firstNode = graph.watch.value.tasks[String(first)];
          assert.strictEqual(firstNode?.owner, parent);
          assert.strictEqual(firstNode?.conversationId, root.id);
          assert.isTrue(Option.isSome(yield* h.harness.conversation(owned)));
          yield* rebuild;

          yield* Deferred.succeed(child, undefined);
          // The parent completes while the late child lives: its outcome is held.
          yield* eventually(
            Effect.sync(
              () => graph.watch.value.tasks[String(parent)]?.state.status === "completing",
            ),
          );
          assert.deepStrictEqual(graph.watch.value.tasks[String(parent)]!.state, {
            status: "completing",
            outcome: "completed",
          });
          assert.isUndefined(graph.watch.value.tasks[String(first)]);
          // Owned conversations stay listed while the owner lives.
          assert.deepStrictEqual(
            graph.watch.value.tasks[String(parent)]!.conversations,
            parentNode.conversations,
          );
          yield* rebuild;

          yield* Deferred.succeed(late, undefined);
          yield* h.harness.awaitTask(parent);
          yield* flush;
          assert.deepStrictEqual(graph.watch.value, { tasks: {} });
          // Every revision is one commit that changed a node; none repeats its predecessor.
          for (let index = 1; index < seen.length; index++) {
            assert.notDeepEqual(seen[index], seen[index - 1]);
          }
          // The commit that created the two conversations published a revision setting them.
          assert.isTrue(
            seen.some((value) => value.tasks[String(parent)]?.conversations.length === 2),
          );
          yield* graph.stop;
          yield* h.close;
        }),
      ),
  );

  it.effect(
    "builds from committed tasks, shows surviving tasks as pending after reopen, and marks aborts",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const storage = yield* controlledStorage;
          const gate = yield* Deferred.make<void>();
          const WorkTask = Task.make({
            name: "test.graph-work",
            version: 1,
            input: Schema.Null,
            checkpoint: Work,
            result: Schema.Null,
            initial: () => ({ phase: "work" }),
            phases: {
              // An abort mark interrupts the wait on the gate.
              work: (_task, runtime) =>
                Effect.gen(function* () {
                  yield* Deferred.await(gate);
                  yield* runtime.commit(() => Effect.succeed(completed(null)));
                }),
            },
            abort: (_task, runtime) => runtime.commit(() => Effect.succeed(abortedWith("test"))),
          });
          const first = yield* openHarness(storage, [WorkTask]);
          const firstRoot = yield* first.harness.ensureRoot();
          const [foreground, background, owned] = yield* firstRoot.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              const foreground = yield* tx.createTask(WorkTask, null, {
                ownership: { kind: "conversation" },
              });
              const background = yield* tx.createTask(WorkTask, null, {
                ownership: { kind: "conversation" },
                background: true,
              });
              const owned = yield* tx.createConversation({
                ownership: { kind: "task", taskId: foreground },
              });
              return [foreground, background, owned.id] as const;
            }),
          );
          yield* first.harness.resume;
          yield* eventually(
            first.harness.inspect.pipe(
              Effect.map((inspection) =>
                inspection.tasks.every((task) => task.state.kind === "running"),
              ),
            ),
          );
          const running = yield* acquireWatch(first);
          assert.deepStrictEqual(statuses(running.watch.value), {
            [`test.graph-work#${foreground}`]: "running",
            [`test.graph-work#${background}`]: "running",
          });
          yield* first.close;

          // Acquired after reopen: built from the committed records and owner edges; open reconciled running
          // to pending.
          const h = yield* openHarness(storage, [WorkTask]);
          const opened = yield* acquireWatch(h);
          const watch = opened.watch;
          assert.deepStrictEqual(statuses(watch.value), {
            [`test.graph-work#${foreground}`]: "pending",
            [`test.graph-work#${background}`]: "pending",
          });
          assert.deepStrictEqual(watch.value.tasks[String(foreground)]!.conversations, [owned]);
          assert.isTrue(watch.value.tasks[String(background)]!.background);

          // Exact frames: replaying their operations from the acquisition revision gives each delivered value.
          let replica: TaskGraph = watch.value;
          const frames: Array<WatchFrame<TaskGraph>> = [];
          yield* start(watch, (frame) => {
            replica = Delta.applyImmutableUnsafe(replica, frame.ops);
            assert.deepStrictEqual(replica, frame.value);
            frames.push(frame);
          });
          yield* h.harness.resume;
          yield* eventually(
            Effect.sync(() => watch.value.tasks[String(background)]?.state.status === "running"),
          );
          assert.deepStrictEqual(yield* h.harness.abortTask(background), { _tag: "Marked" });
          yield* h.harness.awaitTask(background);
          yield* flush;
          // Expected: a frame `[["s", ["tasks", background], { …, abortRequested: true }]]`.
          const key = String(background);
          // The replay above checks that the set node is the frame's.
          const marked = frames.find((frame) => {
            const [op] = frame.ops;
            return (
              frame.ops.length === 1 &&
              op?.[0] === "s" &&
              op[1].length === 2 &&
              op[1][1] === key &&
              frame.value.tasks[key]?.abortRequested === true
            );
          });
          assert.isDefined(marked);
          assert.deepStrictEqual(frames.at(-1)?.ops, [["d", ["tasks", key]]]);
          assert.deepStrictEqual(Object.keys(replica.tasks), [String(foreground)]);
          yield* opened.stop;
          yield* Deferred.succeed(gate, undefined);
          yield* h.harness.awaitTask(foreground);
          yield* h.close;
        }),
      ),
  );

  it.effect("lists owned conversations in ID order whatever order one commit creates them in", () =>
    run(backend)(
      Effect.gen(function* () {
        const gate = yield* Deferred.make<void>();
        const created: Array<ConversationId> = [];
        const Spawner = Task.make({
          name: "test.graph-spawner",
          version: 1,
          input: Schema.Struct({ at: Schema.Number }),
          checkpoint: Schema.Struct({ phase: Schema.Literal("spawn") }),
          result: Schema.Null,
          initial: () => ({ phase: "spawn" }),
          phases: {
            spawn: (task, runtime) =>
              Effect.gen(function* () {
                yield* runtime.commit(
                  Effect.gen(function* () {
                    const tx = yield* Tx;
                    const ownership = { kind: "task", taskId: task.id } as const;
                    const [forked, fresh] = yield* Effect.all(
                      [
                        // SAFETY: the input carries the entry ID the creating commit appended.
                        tx.forkConversation(runtime.conversationId, task.input.at as EntryId, {
                          ownership,
                        }),
                        tx.createConversation({ ownership }),
                      ],
                      { concurrency: "unbounded" },
                    );
                    created.push(forked.id, fresh.id);
                  }),
                );
                yield* Deferred.await(gate);
                yield* runtime.commit(() => Effect.succeed(completed(null)));
              }),
          },
          abort: () => Effect.void,
        });
        const h = yield* openHarness(yield* controlledStorage, [Spawner]);
        const root = yield* h.harness.ensureRoot();
        const graph = yield* acquireWatch(h);
        yield* start(graph.watch, () => {});
        const id = yield* root.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            const entry = yield* tx.appendEntry(root.id, { kind: "note" });
            return yield* tx.createTask(
              Spawner,
              { at: entry.id },
              { ownership: { kind: "conversation" } },
            );
          }),
        );
        yield* h.harness.resume;
        yield* eventually(
          Effect.sync(() => (graph.watch.value.tasks[String(id)]?.conversations.length ?? 0) === 2),
        );
        const advanced = graph.watch.value;
        assert.deepStrictEqual(
          advanced.tasks[String(id)]!.conversations,
          created.toSorted((a, b) => a - b),
        );
        yield* graph.stop;
        const rebuilt = yield* acquireWatch(h);
        assert.deepStrictEqual(rebuilt.watch.value, advanced);
        yield* rebuilt.stop;
        yield* Deferred.succeed(gate, undefined);
        yield* h.harness.awaitTask(id);
        yield* h.close;
      }),
    ),
  );

  it.effect("registers nothing for an acquisition cancelled while it waits for the line", () =>
    run(backend)(
      Effect.gen(function* () {
        const { Child } = family(gatesNever);
        const h = yield* openHarness(yield* controlledStorage, []);
        yield* createIn(h, Child, { late: false }, { ownership: { kind: "conversation" } });
        // A commit holds the line, so the acquisition waits behind it.
        const held = yield* holdLine(h);
        yield* held.entered;
        const cancelled = yield* Effect.forkChild(Effect.scoped(h.harness.watchTaskGraph), {
          startImmediately: true,
        });
        yield* Fiber.interrupt(cancelled);
        yield* held.release;
        assert.isTrue(Exit.hasInterrupts(yield* Fiber.await(cancelled)));
        // No observer kept the mount: each new observer builds a new revision.
        const first = yield* acquireWatch(h);
        const value = first.watch.value;
        yield* first.stop;
        const second = yield* acquireWatch(h);
        assert.notStrictEqual(second.watch.value, value);
        assert.deepStrictEqual(second.watch.value, value);
        yield* second.stop;
        yield* h.close;
      }),
    ),
  );

  it.effect(
    "publishes no revision for a commit that changes no node, and shares one mount between observers",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const gate = yield* Deferred.make<void>();
          const reached = yield* Deferred.make<void>();
          const Memo = Task.make({
            name: "test.graph-memo",
            version: 1,
            input: Schema.Null,
            checkpoint: Work,
            result: Schema.Null,
            initial: () => ({ phase: "work" }),
            phases: {
              work: (_task, runtime) =>
                Effect.gen(function* () {
                  yield* runtime.memo("seen", true);
                  yield* Deferred.succeed(reached, undefined);
                  yield* Deferred.await(gate);
                  yield* runtime.commit(() => Effect.succeed(completed(null)));
                }),
            },
            abort: () => Effect.void,
          });
          const h = yield* openHarness(yield* controlledStorage, [Memo]);
          const id = yield* createIn(h, Memo, null, { ownership: { kind: "conversation" } });
          const first = yield* acquireWatch(h);
          const second = yield* acquireWatch(h);
          assert.strictEqual(second.watch.value, first.watch.value);
          const updates: Array<string> = [];
          yield* start(first.watch, (frame) => {
            updates.push(frame.value.tasks[String(id)]?.state.status ?? "gone");
          });
          yield* h.harness.resume;
          yield* Deferred.await(reached);
          yield* flush;
          // Reservation changed the node; the memo commit did not.
          assert.deepStrictEqual(updates, ["running"]);
          yield* Deferred.succeed(gate, undefined);
          yield* h.harness.awaitTask(id);
          yield* flush;
          assert.deepStrictEqual(updates, ["running", "gone"]);
          const last = first.watch.value;
          yield* first.stop;
          yield* second.stop;
          // No observer is left, so the mount was dropped: a new observer builds a new revision.
          const rebuilt = yield* acquireWatch(h);
          assert.deepStrictEqual(rebuilt.watch.value, last);
          assert.notStrictEqual(rebuilt.watch.value, last);
          yield* rebuilt.stop;
          yield* h.close;
        }),
      ),
  );

  it.effect(
    "advances with the conversation view at the publication of the one commit that changes both",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const { Parent, Child } = family(gatesNever);
          const h = yield* openHarness(yield* controlledStorage, [Parent, Child]);
          const root = yield* h.harness.ensureRoot();
          const seen: Array<string> = [];
          const record = <T>(name: string): Observer<T> => ({
            advance: () => {
              seen.push(name);
            },
            end: () => {},
          });
          const view = yield* root.observe(record<ConversationView>("view"));
          const graph = yield* h.harness.observeTaskGraph(record<TaskGraph>("graph"));
          yield* root.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              yield* tx.appendEntry(root.id, { kind: "note" });
              yield* tx.createTask(Parent, null, { ownership: { kind: "conversation" } });
            }),
          );
          // Both at the one publication, before the commit returns, in no promised order.
          assert.deepStrictEqual(seen.toSorted(), ["graph", "view"]);
          view.detach();
          graph.detach();
          yield* h.close;
        }),
      ),
  );

  // A refresh that dies is reported, and the loop goes on: the next notice runs a new one.
  it.effect("reports a blocked refresh that dies and keeps refreshing", () =>
    run(backend)(
      Effect.gen(function* () {
        const { session } = yield* openTestSession();
        const conversationId = yield* createConversation(session);
        const { Child } = family(gatesNever);
        const id = yield* session.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            return yield* tx.createTask(
              Child,
              { late: false },
              { ownership: { kind: "conversation" }, conversationId },
            );
          }),
        );
        const reports: Array<Report> = [];
        let reason: BlockedReason | "throw" | undefined;
        let notify: (() => void) | undefined;
        const scope = yield* Scope.make();
        const graphs = yield* TaskGraphs.make({
          kernel: session,
          blocking: {
            blocked: () => {
              if (reason === "throw") throw new Error("reason bug");
              return reason;
            },
            subscribeBlocked: (listener) => {
              notify = listener;
              return () => {};
            },
          },
          report: (cause, info) => {
            reports.push({ ...info, cause });
          },
          scope,
        }).pipe(Effect.orDie);
        const watch = yield* graphs.watch;
        yield* start(watch, () => {});
        assert.isUndefined(watch.value.tasks[String(id)]?.blocked);
        reason = "throw";
        notify?.();
        yield* eventually(Effect.sync(() => reports.length === 1));
        assert.strictEqual(reports[0]?.kind, "listener_error");
        assert.include(String(Cause.squash(reports[0]!.cause)), "reason bug");
        reason = "missing_task";
        notify?.();
        yield* eventually(
          Effect.sync(() => watch.value.tasks[String(id)]?.blocked === "missing_task"),
        );
        yield* Scope.close(scope, Exit.void);
        yield* session.close;
      }),
    ),
  );
});

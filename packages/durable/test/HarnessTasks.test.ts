// Durable tasks run by the Harness, on every backend.
//
// `openHarness` opens a Harness over a `ControlledStorage`. Waits on the invocation are `Effect.never`, interrupted
// when the invocation is signalled; a handler that ignores its signal is an uninterruptible region; a cancelled
// caller context is an interrupted fiber running that call; the clock is the `TestClock`; a reopened process is a
// new Harness over the same storage. Work started inside a handler without awaiting it is a detached fiber started
// immediately, so it reaches the line in the same synchronous step.
//
// "publication at adoption": host listeners are the Session's `observeCommits` and the Harness's own observers its
// kernel's `followCommits`. A storage that still works after adopting is the controlled storage with an
// `intercept.commit` that lets pending work run before it resolves (`resolveLate`). Host listeners run in the
// publication's synchronous step, after the Session's own observers, rather than one microtask later; waits and
// aborts still resume after them. The task's abort signal is the interruption of its phase. A listener starts
// work with `Effect.runForkWith` on the test's context, at once. The active-task counter is Effect's, which keeps a
// sum: a task counted in and out shows one counter at 0 instead of the adds `[1, -1]`.
//
// Not covered: refreshing a registry snapshot at the phase boundary, since the registry is static.
import { assert, it } from "@effect/vitest";
import * as Cause from "effect/Cause";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as Scheduler from "effect/Scheduler";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import { Backend, StorageFailure, type StorageWrite } from "../src/AgentStorage.ts";
import type { EntryNotVisible } from "../src/ContextView.ts";
import type { CommitObservation } from "../src/Observer.ts";
import * as Doc from "../src/Doc.ts";
import * as Entry from "../src/Entry.ts";
import * as Extension from "../src/Extension.ts";
import type { Conversation } from "../src/Harness.ts";
import * as Hook from "../src/Hook.ts";
import { EntryId, type Seq, TaskId } from "../src/Ids.ts";
import type { CommitError, CommitPublication, ReadError } from "../src/Session.ts";
import * as Task from "../src/Task.ts";
import type { TaskError } from "../src/TaskError.ts";
import * as TestTracer from "../src/testing/TestTracer.ts";
import { Tx, type TxDocError } from "../src/Tx.ts";
import {
  abortedWith,
  awaitBounded,
  awaited,
  completed,
  eventually,
  holdLine,
  joined,
  openHarness,
  type OpenHarnessOptions,
  settled,
  type TestHarness,
} from "./HarnessSupport.ts";
import {
  type ControlledStorage,
  controlledStorage,
  describeEachBackend,
  failureMessage,
  flush,
  type Gate,
  run,
} from "./SessionSupport.ts";
import { serialTimeout } from "./Timeouts.ts";

const Step = Schema.Struct({ phase: Schema.Literal("run") });
type Step = typeof Step.Type;
type StepRuntime<R> = Task.TaskRuntime<null, Step, R>;
type HandlerError = TaskError | CommitError | ReadError | TxDocError | EntryNotVisible;
type StepHandler<R> = (
  task: Task.RunningTask<null, Step, R>,
  runtime: StepRuntime<R>,
) => Effect.Effect<void, HandlerError>;

/** A one-phase task whose result has the schema `result`; the default abort handler settles `aborted` "test". */
const oneStepOf =
  <R extends Schema.Json>(result: Schema.Codec<R, R>) =>
  (
    name: string,
    runPhase: StepHandler<R>,
    abort: StepHandler<R> = (_task, runtime) =>
      runtime.commit(() => Effect.succeed(abortedWith("test"))),
  ) =>
    Task.make({
      name,
      version: 1,
      input: Schema.Null,
      checkpoint: Step,
      result,
      initial: () => ({ phase: "run" }),
      phases: { run: (task, runtime) => runPhase(task, runtime) },
      abort: (task, runtime) => abort(task, runtime),
    });

/** A one-step task whose result is `null`. */
const oneStep = oneStepOf(Schema.Null);

/** A one-phase task that waits for `gate` and completes with null. */
const gated = (name: string, gate: Deferred.Deferred<void>) =>
  oneStep(name, (_task, runtime) =>
    Effect.gen(function* () {
      yield* Deferred.await(gate);
      yield* runtime.commit(() => Effect.succeed(completed(null)));
    }),
  );

interface StartOptions {
  readonly background?: boolean | undefined;
}

const start = <R>(
  conversation: Conversation,
  task: Task.Task<null, Step, R>,
  options: StartOptions = {},
) =>
  conversation.commit(
    Effect.gen(function* () {
      const tx = yield* Tx;
      return yield* tx.createTask(task, null, { ownership: { kind: "conversation" }, ...options });
    }),
  );

const openRoot = (
  storage: ControlledStorage,
  tasks: ReadonlyArray<Task.Any>,
  options?: OpenHarnessOptions,
) =>
  Effect.gen(function* () {
    const opened = yield* openHarness(storage, tasks, options);
    const root = yield* opened.harness.ensureRoot();
    return { ...opened, root };
  });

/** Start `abortTask()` and return once its mark is durable, before it has joined the run. */
const markDurably = (h: TestHarness, id: TaskId) =>
  Effect.gen(function* () {
    const aborting = yield* Effect.forkChild(h.harness.abortTask(id), { startImmediately: true });
    yield* eventually(
      h.harness
        .getTask(id)
        .pipe(Effect.map((found) => Option.isSome(found) && found.value.abortRequested)),
    );
    return aborting;
  });

/** The tag a forked fiber failed with once it settled within a bounded number of turns, or `success`. */
const closedTag = <A, E extends { readonly _tag: string }>(fiber: Fiber.Fiber<A, E>) =>
  awaitBounded(fiber).pipe(
    Effect.map((exit) =>
      Exit.isFailure(exit)
        ? Option.getOrUndefined(Cause.findErrorOption(exit.cause))?._tag
        : "success",
    ),
  );

/** A user message, in the model `Message` shape. */
const user = (text: string) => ({ role: "user", content: text, timestamp: 1 }) as const;

/** The status of every committed write of task `id`, in commit order. */
const taskWrites = (storage: ControlledStorage, id: TaskId) =>
  storage.commits.flatMap((writes) =>
    writes.flatMap((write) =>
      write.type === "task" && write.value.id === id ? [write.value] : [],
    ),
  );

/** The task records of the last commit. */
const lastTaskWrites = (storage: ControlledStorage) =>
  (storage.commits.at(-1) ?? []).flatMap((write) => (write.type === "task" ? [write.value] : []));

const outcomeOf = (h: TestHarness, id: TaskId) =>
  h.harness.awaitTask(id).pipe(Effect.map((receipt) => receipt.state.outcome));

describeEachBackend("task phases", (backend) => {
  it.effect(
    "continues one invocation through checkpoint progress and completes with a typed result",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const seen: Array<number> = [];
          const runtimes = new Set<object>();
          const Counter = Task.make({
            name: "test.counter",
            version: 1,
            input: Schema.Struct({ to: Schema.Number }),
            checkpoint: Schema.Struct({ phase: Schema.Literal("count"), n: Schema.Number }),
            result: Schema.Number,
            initial: () => ({ phase: "count", n: 0 }),
            phases: {
              count: (task, runtime) =>
                Effect.gen(function* () {
                  seen.push(task.state.checkpoint.n);
                  runtimes.add(runtime);
                  yield* runtime.commit((current) =>
                    Effect.sync(() => {
                      const n = current.state.checkpoint.n + 1;
                      return n === task.input.to
                        ? completed(n)
                        : ({ status: "running", checkpoint: { phase: "count", n } } as const);
                    }),
                  );
                }),
            },
            abort: () => Effect.void,
          });
          const storage = yield* controlledStorage;
          const h = yield* openRoot(storage, [Counter]);
          const id = yield* h.root.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              return yield* tx.createTask(
                Counter,
                { to: 3 },
                { ownership: { kind: "conversation" } },
              );
            }),
          );
          yield* h.harness.resume;
          const receipt = yield* h.harness.awaitTask(id);
          const result: number | undefined =
            receipt.state.outcome.status === "completed" ? receipt.state.outcome.result : undefined;
          assert.strictEqual(result, 3);
          assert.deepStrictEqual(seen, [0, 1, 2]);
          assert.strictEqual(runtimes.size, 1);
          yield* h.close;
        }),
      ),
  );

  it.effect("faults a phase without durable progress and a throwing phase", () =>
    run(backend)(
      Effect.gen(function* () {
        const Idle = oneStep("test.idle", () => Effect.void);
        const DocumentOnly = oneStep("test.document-only", (_task, runtime) =>
          // A commit that returns no state is not progress.
          runtime.commit(() => Effect.void),
        );
        const Throws = oneStep("test.throws", () => Effect.die(new Error("boom")));
        const storage = yield* controlledStorage;
        const h = yield* openRoot(storage, [Idle, DocumentOnly, Throws]);
        const ids = [
          yield* start(h.root, Idle),
          yield* start(h.root, DocumentOnly),
          yield* start(h.root, Throws),
        ];
        yield* h.harness.resume;
        const outcomes = yield* Effect.forEach(ids, (id) => outcomeOf(h, id), {
          concurrency: "unbounded",
        });
        assert.deepStrictEqual(outcomes, [
          {
            status: "faulted",
            error: { message: "Task test.idle phase run returned without durable progress" },
          },
          {
            status: "faulted",
            error: {
              message: "Task test.document-only phase run returned without durable progress",
            },
          },
          { status: "faulted", error: { message: "boom" } },
        ]);
        yield* h.close;
      }),
    ),
  );

  it.effect("keeps a committed terminal outcome when the handler throws afterwards", () =>
    run(backend)(
      Effect.gen(function* () {
        let lateCommit: string | undefined;
        const Done = oneStepOf(Schema.String)("test.done", (_task, runtime) =>
          Effect.gen(function* () {
            yield* runtime.commit(() => Effect.succeed(completed("ok")));
            lateCommit = yield* failureMessage(runtime.commit(() => Effect.void));
            return yield* Effect.die(new Error("after terminal"));
          }),
        );
        const storage = yield* controlledStorage;
        const h = yield* openRoot(storage, [Done]);
        const id = yield* start(h.root, Done);
        yield* h.harness.resume;
        assert.deepStrictEqual(yield* outcomeOf(h, id), { status: "completed", result: "ok" });
        yield* eventually(Effect.sync(() => lateCommit !== undefined));
        assert.include(lateCommit, `Task ${id} is terminal`);
        yield* h.close;
      }),
    ),
  );

  it.effect("compares checkpoints by value, including arrays", () =>
    run(backend)(
      Effect.gen(function* () {
        const Collect = Task.make({
          name: "test.collect",
          version: 1,
          input: Schema.Null,
          checkpoint: Schema.Struct({
            phase: Schema.Literal("collect"),
            items: Schema.Array(Schema.String),
          }),
          result: Schema.Null,
          initial: () => ({ phase: "collect", items: [] }),
          phases: {
            collect: (task, runtime) => {
              const items = task.state.checkpoint.items;
              // Two rounds of progress, then an equal copy of the checkpoint, which is no progress.
              const next = items.length < 2 ? [...items, `item${items.length}`] : [...items];
              return runtime.commit(() =>
                Effect.succeed({
                  status: "running",
                  checkpoint: { phase: "collect", items: next },
                } as const),
              );
            },
          },
          abort: () => Effect.void,
        });
        const storage = yield* controlledStorage;
        const h = yield* openRoot(storage, [Collect]);
        const id = yield* h.root.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            return yield* tx.createTask(Collect, null, { ownership: { kind: "conversation" } });
          }),
        );
        yield* h.harness.resume;
        assert.deepStrictEqual(yield* outcomeOf(h, id), {
          status: "faulted",
          error: { message: "Task test.collect phase collect returned without durable progress" },
        });
        yield* h.close;
      }),
    ),
  );

  // A task document's watch gets the retirement of its task's terminal commit as its last frame, then ends.
  it.effect("ends a task document's watch after the retirement of its task's terminal commit", () =>
    run(backend)(
      Effect.gen(function* () {
        const Progress = Doc.make({
          checkpointWhen: () => false,
          kind: "test.task-watch",
          version: 1,
          scope: "task",
          schema: Schema.Struct({ lines: Schema.Array(Schema.String) }),
          initial: () => ({ lines: [] }),
        });
        const proceed = yield* Deferred.make<void>();
        const Writer = Task.make({
          name: "test.task-watch-writer",
          version: 1,
          input: Schema.Null,
          checkpoint: Schema.Union([
            Schema.Struct({ phase: Schema.Literal("write") }),
            Schema.Struct({ phase: Schema.Literal("finish") }),
          ]),
          result: Schema.Null,
          initial: () => ({ phase: "write" }),
          phases: {
            write: (task, runtime) =>
              runtime.commit(() =>
                Effect.gen(function* () {
                  const tx = yield* Tx;
                  (yield* tx.doc(Progress, task.id)).lines.push("one");
                  return { status: "running", checkpoint: { phase: "finish" } } as const;
                }),
              ),
            finish: (task, runtime) =>
              Effect.gen(function* () {
                yield* Deferred.await(proceed);
                yield* runtime.commit(() =>
                  Effect.gen(function* () {
                    const tx = yield* Tx;
                    (yield* tx.doc(Progress, task.id)).lines.push("two");
                    return completed(null);
                  }),
                );
              }),
          },
          abort: () => Effect.void,
        });
        const h = yield* openRoot(yield* controlledStorage, [Writer]);
        const conversation = yield* h.harness.createConversation({
          ownership: { kind: "ownerless" },
        });
        const id = yield* conversation.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            return yield* tx.createTask(Writer, null, { ownership: { kind: "conversation" } });
          }),
        );
        yield* h.harness.resume;
        const watch = yield* eventually(
          Effect.scoped(h.session.watchDoc(Progress, id)).pipe(Effect.map(Option.isSome)),
        ).pipe(Effect.andThen(h.session.watchDoc(Progress, id)), Effect.map(Option.getOrThrow));
        assert.deepStrictEqual(watch.value, { lines: ["one"] });
        const frames = yield* Effect.forkChild(Stream.runCollect(watch.frames), {
          startImmediately: true,
        });
        yield* Deferred.succeed(proceed, undefined);
        yield* h.harness.awaitTask(id);
        // The terminal commit changed the document and retired it: the retirement only, with the last value.
        assert.deepStrictEqual(yield* Fiber.join(frames), [
          { value: { lines: ["one"] }, ops: [], retired: true },
        ]);
        yield* h.close;
      }),
    ),
  );

  it.effect(
    "commits results with entries atomically, keeps memos until terminal, and retires task documents",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const Progress = Doc.make({
            checkpointWhen: () => false,
            kind: "test.task-progress",
            version: 1,
            scope: "task",
            schema: Schema.Struct({ lines: Schema.Array(Schema.String) }),
            initial: () => ({ lines: [] }),
          });
          const Answer = Entry.make("answer");
          const Child = oneStep("test.child", (_task, runtime) =>
            runtime.commit(() => Effect.succeed(completed(null))),
          );
          let childId: TaskId | undefined;
          let progressSeen: ReadonlyArray<string> | undefined;
          const memoReads: Array<unknown> = [];
          let memosSeen: unknown;
          const Writer = Task.make({
            name: "test.writer",
            version: 1,
            input: Schema.Null,
            checkpoint: Schema.Union([
              Schema.Struct({ phase: Schema.Literal("write") }),
              Schema.Struct({ phase: Schema.Literal("answer") }),
            ]),
            // The result schema's type must equal its encoding, so the entry ID is a plain number.
            result: Schema.Struct({ entryId: Schema.Number }),
            initial: () => ({ phase: "write" }),
            phases: {
              write: (task, runtime) =>
                Effect.gen(function* () {
                  // The reads are recorded and asserted by the test.
                  memoReads.push(Option.getOrUndefined(yield* runtime.memo("choice")));
                  memoReads.push(
                    yield* Effect.all([runtime.memo("choice", "a"), runtime.memo("choice", "b")], {
                      concurrency: "unbounded",
                    }),
                  );
                  memoReads.push(Option.getOrUndefined(yield* runtime.memo("choice")));
                  // Memo names never resolve to inherited object properties.
                  memoReads.push(Option.getOrUndefined(yield* runtime.memo("toString")));
                  memoReads.push(yield* runtime.memo("toString", "own"));
                  yield* runtime.commit(() =>
                    Effect.gen(function* () {
                      const tx = yield* Tx;
                      (yield* tx.doc(Progress, task.id)).lines.push("wrote");
                      // Task creation defaults to the task's own conversation.
                      childId = yield* tx.createTask(Child, null, {
                        ownership: { kind: "conversation" },
                      });
                      return { status: "running", checkpoint: { phase: "answer" } } as const;
                    }),
                  );
                }),
              answer: (task, runtime) =>
                Effect.gen(function* () {
                  memosSeen = task.memos;
                  yield* runtime.commit(() =>
                    Effect.gen(function* () {
                      const tx = yield* Tx;
                      progressSeen = [...(yield* tx.doc(Progress, task.id)).lines];
                    }),
                  );
                  yield* runtime.commit((current) =>
                    Effect.gen(function* () {
                      const tx = yield* Tx;
                      const entry = yield* tx.appendEntry(current.conversationId, {
                        kind: "answer",
                        model: [user("done")],
                      });
                      return completed({ entryId: entry.id });
                    }),
                  );
                }),
            },
            abort: () => Effect.void,
          });
          const storage = yield* controlledStorage;
          const h = yield* openRoot(storage, [Writer, Child]);
          const conversation = yield* h.harness.createConversation({
            ownership: { kind: "ownerless" },
          });
          const id = yield* conversation.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              return yield* tx.createTask(Writer, null, { ownership: { kind: "conversation" } });
            }),
          );
          yield* h.harness.resume;
          const receipt = yield* h.harness.awaitTask(id);
          assert.deepStrictEqual(memoReads, [undefined, ["a", "a"], "a", undefined, "own"]);
          assert.deepStrictEqual(memosSeen, { choice: "a", toString: "own" });
          assert.notProperty(receipt.state, "checkpoint");
          assert.notProperty(receipt, "memos");
          const outcome = receipt.state.outcome;
          if (outcome.status !== "completed")
            return yield* Effect.die(new Error("expected completion"));
          const entry = yield* h.harness.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              return yield* tx.entry(EntryId.make(outcome.result.entryId));
            }),
          );
          assert.isTrue(Answer.is(Option.getOrUndefined(entry)));
          assert.strictEqual(Option.getOrUndefined(entry)?.conversationId, conversation.id);
          assert.deepStrictEqual(progressSeen, ["wrote"]);
          assert.isTrue(Option.isNone(yield* h.session.snapshot(Progress, id)));
          assert.deepStrictEqual(yield* h.harness.getTask(id), Option.some(receipt));
          assert.isDefined(childId);
          assert.strictEqual((yield* h.harness.awaitTask(childId)).conversationId, conversation.id);
          assert.notStrictEqual(h.root.id, conversation.id);
          yield* h.close;
        }),
      ),
  );

  it.effect(
    "resumes a waiting task once every task in `on` is terminal, whatever the outcome",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const gate = yield* Deferred.make<void>();
          const order: Array<string> = [];
          let on: Array<TaskId> = [];
          let outcomes: Array<string> = [];
          const Waiter = Task.make({
            name: "test.waiter",
            version: 1,
            input: Schema.Null,
            checkpoint: Schema.Union([
              Schema.Struct({ phase: Schema.Literal("wait") }),
              Schema.Struct({ phase: Schema.Literal("resume") }),
            ]),
            result: Schema.Null,
            initial: () => ({ phase: "wait" }),
            phases: {
              wait: (_task, runtime) =>
                Effect.gen(function* () {
                  order.push("wait");
                  const checkpoint = { phase: "resume" } as const;
                  yield* runtime.commit(() =>
                    Effect.succeed({
                      status: "waiting",
                      checkpoint,
                      on,
                      policy: "allSettled",
                    } as const),
                  );
                }),
              resume: (_task, runtime) =>
                Effect.gen(function* () {
                  order.push("resume");
                  outcomes = (yield* runtime.outcomes(on)).map((outcome) => outcome.status);
                  yield* runtime.commit(() => Effect.succeed(completed(null)));
                }),
            },
            abort: (_task, runtime) => runtime.commit(() => Effect.succeed(abortedWith("test"))),
          });
          const First = oneStep("test.first", (_task, runtime) =>
            Effect.gen(function* () {
              order.push("first");
              yield* Deferred.await(gate);
              yield* runtime.commit(() => Effect.succeed(completed(null)));
            }),
          );
          const Faulting = oneStep("test.faulting", () =>
            Effect.suspend(() => {
              order.push("faulting");
              return Effect.die(new Error("fails"));
            }),
          );
          const storage = yield* controlledStorage;
          const h = yield* openRoot(storage, [First, Faulting, Waiter]);
          const first = yield* start(h.root, First);
          const faulting = yield* start(h.root, Faulting);
          on = [first, faulting];
          const waiter = yield* h.root.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              return yield* tx.createTask(Waiter, null, { ownership: { kind: "conversation" } });
            }),
          );
          yield* h.harness.resume;
          yield* eventually(Effect.sync(() => order.length === 3));
          yield* flush;
          assert.deepStrictEqual(order.toSorted(), ["faulting", "first", "wait"]);
          const state = Option.getOrUndefined(yield* h.harness.getTask(waiter))?.state;
          assert.strictEqual(state?.status, "waiting");
          assert.deepStrictEqual(state?.status === "waiting" ? state.on : undefined, on);
          yield* Deferred.succeed(gate, undefined);
          yield* h.harness.awaitTask(waiter);
          assert.strictEqual(order.at(-1), "resume");
          assert.deepStrictEqual(outcomes, ["completed", "faulted"]);
          yield* h.close;
        }),
      ),
  );

  it.effect(
    "stamps startedAt at the first run and endedAt when terminal, keeping startedAt through a wait",
    () =>
      run(backend)(
        Effect.gen(function* () {
          // The Harness reads the TestClock.
          yield* TestClock.setTime(1_000);
          const gate = yield* Deferred.make<void>();
          let on: Array<TaskId> = [];
          const Waiter = Task.make({
            name: "test.timed-waiter",
            version: 1,
            input: Schema.Null,
            checkpoint: Schema.Union([
              Schema.Struct({ phase: Schema.Literal("wait") }),
              Schema.Struct({ phase: Schema.Literal("resume") }),
            ]),
            result: Schema.Null,
            initial: () => ({ phase: "wait" }),
            phases: {
              wait: (_task, runtime) => {
                const checkpoint = { phase: "resume" } as const;
                return runtime.commit(() =>
                  Effect.succeed({
                    status: "waiting",
                    checkpoint,
                    on,
                    policy: "allSettled",
                  } as const),
                );
              },
              resume: (_task, runtime) => runtime.commit(() => Effect.succeed(completed(null))),
            },
            abort: (_task, runtime) => runtime.commit(() => Effect.succeed(abortedWith("test"))),
          });
          const Held = gated("test.timed-held", gate);
          const storage = yield* controlledStorage;
          const h = yield* openRoot(storage, [Waiter, Held]);
          const held = yield* start(h.root, Held);
          on = [held];
          const waiter = yield* h.root.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              return yield* tx.createTask(Waiter, null, { ownership: { kind: "conversation" } });
            }),
          );
          const times = h.harness.inspect.pipe(
            Effect.map((inspection) =>
              inspection.tasks.map(({ record }) => [record.id, record.startedAt, record.endedAt]),
            ),
          );
          assert.deepStrictEqual(yield* times, [
            [held, undefined, undefined],
            [waiter, undefined, undefined],
          ]);

          yield* TestClock.setTime(2_000);
          yield* h.harness.resume;
          yield* eventually(
            h.harness
              .getTask(waiter)
              .pipe(
                Effect.map((found) => Option.getOrUndefined(found)?.state.status === "waiting"),
              ),
          );
          assert.deepStrictEqual(yield* times, [
            [held, 2_000, undefined],
            [waiter, 2_000, undefined],
          ]);

          // The waiter runs again at 5_000; its start stays the first run.
          yield* TestClock.setTime(5_000);
          yield* Deferred.succeed(gate, undefined);
          const settledWaiter = yield* h.harness.awaitTask(waiter);
          const settledHeld = Option.getOrUndefined(yield* h.harness.getTask(held));
          assert.deepStrictEqual([settledHeld?.startedAt, settledHeld?.endedAt], [2_000, 5_000]);
          assert.deepStrictEqual([settledWaiter.startedAt, settledWaiter.endedAt], [2_000, 5_000]);
          yield* h.close;
        }),
      ),
  );

  // The registry is static, so no hooks are installed mid-phase: the hooks are the Harness's extensions from the
  // start, and the next phase sees the same ones. The configuration changes between phases.
  it.effect(
    "resolves the agent at first use in a phase from its snapshot, keeps it for the phase, and anew at the next",
    () =>
      run(backend)(
        Effect.gen(function* () {
          type Ping = () => Effect.Effect<void>;
          const pings: Array<string> = [];
          const seen: Array<{
            phase: string;
            pings: Array<string>;
            thinking: string;
            same?: boolean;
          }> = [];
          const beforeUse = yield* Deferred.make<void>();
          const afterUse = yield* Deferred.make<void>();
          const waitingBeforeUse = yield* Deferred.make<void>();
          const waitingAfterUse = yield* Deferred.make<void>();
          const ping = (runtime: { readonly hooks: Task.HookRunner }) =>
            Effect.gen(function* () {
              pings.length = 0;
              yield* runtime.hooks.each<Ping, never, never>("ping", (handler) => handler());
              return [...pings];
            });
          const Phases = Task.make({
            name: "test.agent-phases",
            version: 1,
            input: Schema.Null,
            checkpoint: Schema.Union([
              Schema.Struct({ phase: Schema.Literal("a") }),
              Schema.Struct({ phase: Schema.Literal("b") }),
            ]),
            result: Schema.Null,
            initial: () => ({ phase: "a" }),
            phases: {
              a: (_task, runtime) =>
                Effect.gen(function* () {
                  yield* Deferred.succeed(waitingBeforeUse, undefined);
                  yield* Deferred.await(beforeUse);
                  const first = yield* runtime.agent;
                  seen.push({
                    phase: "a",
                    pings: yield* ping(runtime),
                    thinking: first.thinkingLevel,
                  });
                  yield* Deferred.succeed(waitingAfterUse, undefined);
                  yield* Deferred.await(afterUse);
                  const second = yield* runtime.agent;
                  seen.push({
                    phase: "a",
                    pings: yield* ping(runtime),
                    thinking: second.thinkingLevel,
                    same: second === first,
                  });
                  yield* runtime.commit(() =>
                    Effect.succeed({ status: "running", checkpoint: { phase: "b" } } as const),
                  );
                }),
              b: (_task, runtime) =>
                Effect.gen(function* () {
                  const agent = yield* runtime.agent;
                  seen.push({
                    phase: "b",
                    pings: yield* ping(runtime),
                    thinking: agent.thinkingLevel,
                  });
                  yield* runtime.commit(() => Effect.succeed(completed(null)));
                }),
            },
            abort: () => Effect.void,
          });
          const Pings = Extension.make({
            name: "test.pings",
            hooks: [
              Hook.make(Phases, {
                ping: () =>
                  Effect.sync(() => {
                    pings.push("before");
                  }),
              }),
            ],
          });
          const storage = yield* controlledStorage;
          const h = yield* openRoot(storage, [Phases], { extensions: [Pings] });
          const id = yield* h.root.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              return yield* tx.createTask(Phases, null, { ownership: { kind: "conversation" } });
            }),
          );
          yield* h.harness.resume;

          // Configured during the phase but before its first use: the lazy resolution reads it.
          yield* awaited(waitingBeforeUse);
          yield* h.root.configure({ thinkingLevel: "low" });
          yield* Deferred.succeed(beforeUse, undefined);
          // Configured after the first use: not seen until the next phase.
          yield* awaited(waitingAfterUse);
          yield* h.root.configure({ thinkingLevel: "high" });
          yield* Deferred.succeed(afterUse, undefined);

          yield* h.harness.awaitTask(id);
          assert.deepStrictEqual(seen, [
            { phase: "a", pings: ["before"], thinking: "low" },
            { phase: "a", pings: ["before"], thinking: "low", same: true },
            { phase: "b", pings: ["before"], thinking: "high" },
          ]);
          yield* h.close;
        }),
      ),
  );

  // A cancelled caller context is an interrupted fiber running the call: it starts the phase's resolution in its
  // first step, before it races the context, and is interrupted while it waits.
  it.effect(
    "rejects an agent() wait whose caller context is cancelled, without affecting the phase's resolution",
    () =>
      run(backend)(
        Effect.gen(function* () {
          let cancelled: Exit.Exit<unknown, unknown> | undefined;
          let resolved: string | undefined;
          const Agent = oneStep("test.agent-cancel", (_task, runtime) =>
            Effect.gen(function* () {
              const caller = yield* Effect.forkChild(runtime.agent, { startImmediately: true });
              cancelled = yield* Fiber.interrupt(caller).pipe(Effect.andThen(Fiber.await(caller)));
              resolved = (yield* runtime.agent).thinkingLevel;
              yield* runtime.commit(() => Effect.succeed(completed(null)));
            }),
          );
          const storage = yield* controlledStorage;
          const h = yield* openRoot(storage, [Agent]);
          const id = yield* start(h.root, Agent);
          yield* h.harness.resume;
          yield* h.harness.awaitTask(id);
          assert.isDefined(cancelled);
          assert.isTrue(Exit.hasInterrupts(cancelled));
          assert.strictEqual(resolved, "off");
          yield* h.close;
        }),
      ),
  );

  // The resolution fails through a settings reader whose `extensions` getter throws. An Effect failure nobody
  // awaits is not reported by the runtime, so there is no unobserved rejection to check.
  it.effect("observes a failed agent resolution whose only caller stopped waiting", () =>
    run(backend)(
      Effect.gen(function* () {
        let fail = false;
        const settings = {
          get extensions(): undefined {
            if (fail) throw new Error("settings broke");
            return undefined;
          },
        };
        let resolved: string | undefined;
        const Agent = oneStep("test.agent-failure", (_task, runtime) =>
          Effect.gen(function* () {
            fail = true;
            const caller = yield* Effect.forkChild(runtime.agent, { startImmediately: true });
            yield* Fiber.interrupt(caller);
            resolved = yield* failureMessage(runtime.agent);
            yield* runtime.commit(() => Effect.succeed(completed(null)));
          }),
        );
        const storage = yield* controlledStorage;
        const h = yield* openRoot(storage, [Agent], { settings: () => settings });
        const id = yield* start(h.root, Agent);
        yield* h.harness.resume;
        yield* h.harness.awaitTask(id);
        assert.include(resolved, "settings broke");
        yield* h.close;
      }),
    ),
  );
});

describeEachBackend("task runtime", (backend) => {
  // A watch is closed when its frame stream ends; `now()` and `report()` after the invocation ends are failed
  // Effects, and `agent` is run after the handler's fiber ended.
  it.effect("rejects runtime operations after the invocation ends and stops its watches", () =>
    run(backend)(
      Effect.gen(function* () {
        const Notes = Doc.make({
          checkpointWhen: () => false,
          kind: "test.task-notes",
          version: 1,
          scope: "session",
          schema: Schema.Struct({ text: Schema.String }),
          initial: () => ({ text: "" }),
        });
        const Absent = Doc.make({
          checkpointWhen: () => false,
          kind: "test.task-absent",
          version: 1,
          scope: "session",
          schema: Schema.Struct({ text: Schema.String }),
          initial: () => ({ text: "" }),
        });
        // The watches outlive the handler: the invocation's end must stop them, not this scope.
        const watchScope = yield* Effect.scope;
        let captured: StepRuntime<null> | undefined;
        let watchClosed: Fiber.Fiber<void, unknown> | undefined;
        const delivered: Array<string> = [];
        let absent: Option.Option<object> | "unset" = "unset";
        const Watcher = oneStep("test.watcher", (_task, runtime) =>
          Effect.gen(function* () {
            captured = runtime;
            absent = yield* Scope.provide(runtime.watchDoc(Absent), watchScope);
            const watch = yield* Scope.provide(runtime.watchDoc(Notes), watchScope);
            if (Option.isNone(watch)) return yield* Effect.die(new Error("expected a watch"));
            watchClosed = yield* Effect.forkDetach(
              watch.value.frames.pipe(
                Stream.runForEach((frame) =>
                  Effect.sync(() => {
                    delivered.push(frame.value?.text ?? "retired");
                  }),
                ),
              ),
            );
            yield* runtime.commit(() => Effect.succeed(completed(null)));
          }),
        );
        const storage = yield* controlledStorage;
        const h = yield* openRoot(storage, [Watcher]);
        yield* h.harness.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            (yield* tx.doc(Notes)).text = "hello";
          }),
        );
        const id = yield* start(h.root, Watcher);
        yield* h.harness.resume;
        yield* h.harness.awaitTask(id);
        assert.isTrue(absent !== "unset" && Option.isNone(absent));
        // The watch stops when the step after the phase ends the invocation; later commits deliver nothing.
        assert.isDefined(watchClosed);
        assert.isTrue(Exit.isSuccess(yield* awaitBounded(watchClosed)));
        yield* h.harness.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            (yield* tx.doc(Notes)).text = "after";
          }),
        );
        yield* flush;
        assert.deepStrictEqual(delivered, []);
        assert.isDefined(captured);
        const runtime = captured;
        const ended = "invocation has ended";
        assert.include(yield* failureMessage(runtime.commit(() => Effect.void)), ended);
        assert.include(yield* failureMessage(runtime.memo("x")), ended);
        assert.include(yield* failureMessage(runtime.memo("x", 1)), ended);
        assert.include(yield* failureMessage(runtime.sleepUntil(0, { phase: "run" })), ended);
        assert.include(yield* failureMessage(Effect.scoped(runtime.watchDoc(Notes))), ended);
        // The handler's fiber has ended by now; the ended invocation still wins.
        assert.include(yield* failureMessage(runtime.agent), ended);
        assert.include(yield* failureMessage(runtime.now), ended);
        assert.include(yield* failureMessage(runtime.report(Cause.die(new Error("late")))), ended);
        yield* h.close;
      }),
    ),
  );

  it.effect(
    "reads committed documents and context through the runtime, and forwards the clock and reports",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const Notes = Doc.make({
            checkpointWhen: () => false,
            kind: "test.runtime-notes",
            version: 1,
            scope: "conversation",
            history: "rewindable",
            fork: "asOf",
            schema: Schema.Struct({ text: Schema.String }),
            initial: () => ({ text: "" }),
          });
          let captured: StepRuntime<null> | undefined;
          const seen: Array<unknown> = [];
          const reported = new Error("reported");
          const Reader = oneStep("test.reader", (_task, runtime) =>
            Effect.gen(function* () {
              captured = runtime;
              const [first, second] = (yield* runtime.context(runtime.conversationId)).entries.map(
                (entry) => entry.id,
              );
              if (first === undefined || second === undefined) {
                return yield* Effect.die(new Error("expected two entries"));
              }
              seen.push(
                Option.getOrUndefined(yield* runtime.snapshot(Notes, runtime.conversationId))?.text,
              );
              seen.push(
                Option.getOrUndefined(
                  yield* runtime.snapshotAsOf(Notes, runtime.conversationId, first),
                )?.text,
              );
              seen.push(
                (yield* runtime.context(runtime.conversationId, { at: first })).entries.length,
              );
              seen.push(
                (yield* runtime.context(runtime.conversationId, { at: second })).messages.length,
              );
              seen.push(yield* runtime.now);
              yield* runtime.report(Cause.die(reported));
              yield* runtime.commit(() => Effect.succeed(completed(null)));
            }),
          );
          // The Harness reads the TestClock.
          yield* TestClock.setTime(1234);
          const storage = yield* controlledStorage;
          const h = yield* openRoot(storage, [Reader]);
          for (const text of ["one", "two"]) {
            yield* h.root.commit(
              Effect.gen(function* () {
                const tx = yield* Tx;
                (yield* tx.doc(Notes, h.root.id)).text = text;
                yield* tx.appendEntry(h.root.id, { kind: "message", model: [user(text)] });
              }),
            );
          }
          const id = yield* start(h.root, Reader);
          yield* h.harness.resume;
          yield* h.harness.awaitTask(id);
          assert.deepStrictEqual(seen, ["two", "one", 1, 2, 1234]);
          assert.deepStrictEqual(
            h.reports.map((report) => Cause.squash(report.cause)),
            [reported],
          );
          // The step after the phase ends the invocation.
          yield* flush;
          assert.isDefined(captured);
          assert.include(
            yield* failureMessage(captured.snapshot(Notes, h.root.id)),
            "invocation has ended",
          );
          assert.include(
            yield* failureMessage(captured.context(h.root.id)),
            "invocation has ended",
          );
          yield* h.close;
        }),
      ),
  );

  it.effect(
    "orders runtime commits against the step: one queued before it lands, one after it rejects",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const storage = yield* controlledStorage;
          let before: Fiber.Fiber<void, unknown> | undefined;
          let after: Fiber.Fiber<string> | undefined;
          let afterQueued = false;
          let held: Gate | undefined;
          let harnessRef: TestHarness | undefined;
          const Detached = oneStepOf(Schema.String)("test.detached", (_task, runtime) =>
            Effect.gen(function* () {
              // Hold the line, queue a commit without awaiting it, and return; the step queues behind that commit.
              held = yield* storage.holdCommits;
              const blocker = harnessRef?.harness.commit(
                Effect.gen(function* () {
                  const tx = yield* Tx;
                  yield* tx.appendEntry(runtime.conversationId, { kind: "blocker" });
                }),
              );
              if (blocker !== undefined)
                yield* Effect.forkDetach(blocker, { startImmediately: true });
              before = yield* Effect.forkDetach(
                runtime.commit(() => Effect.succeed(completed("before"))),
                { startImmediately: true },
              );
              // Queued after the handler returned, so after the step.
              after = yield* Effect.forkDetach(
                flush.pipe(
                  Effect.andThen(
                    Effect.suspend(() => {
                      afterQueued = true;
                      return failureMessage(
                        runtime.commit(() => Effect.succeed(completed("after"))),
                      );
                    }),
                  ),
                ),
              );
            }),
          );
          const h = yield* openRoot(storage, [Detached]);
          harnessRef = h;
          const id = yield* start(h.root, Detached);
          yield* h.harness.resume;
          yield* eventually(Effect.sync(() => held !== undefined));
          assert.isDefined(held);
          yield* held.entered;
          yield* eventually(Effect.sync(() => afterQueued));
          yield* flush;
          yield* held.release;
          assert.isDefined(before);
          assert.isTrue(Exit.isSuccess(yield* awaitBounded(before)));
          assert.deepStrictEqual(yield* outcomeOf(h, id), {
            status: "completed",
            result: "before",
          });
          assert.isDefined(after);
          const afterExit = yield* awaitBounded(after);
          assert.isTrue(Exit.isSuccess(afterExit));
          if (Exit.isSuccess(afterExit))
            assert.match(afterExit.value, /invocation has ended|is terminal/);
          yield* h.close;
        }),
      ),
  );

  it.effect("stops a watch whose acquisition finishes after the invocation ended", () =>
    run(backend)(
      Effect.gen(function* () {
        const Notes = Doc.make({
          checkpointWhen: () => false,
          kind: "test.late-watch",
          version: 1,
          scope: "session",
          schema: Schema.Struct({ text: Schema.String }),
          initial: () => ({ text: "" }),
        });
        const storage = yield* controlledStorage;
        let watching: Fiber.Fiber<string> | undefined;
        let watchQueued = false;
        let held: Gate | undefined;
        let harnessRef: TestHarness | undefined;
        const Late = oneStep("test.late-watch", (_task, runtime) =>
          Effect.gen(function* () {
            held = yield* storage.holdCommits;
            const blocker = harnessRef?.harness.commit(
              Effect.gen(function* () {
                const tx = yield* Tx;
                yield* tx.appendEntry(runtime.conversationId, { kind: "blocker" });
              }),
            );
            if (blocker !== undefined)
              yield* Effect.forkDetach(blocker, { startImmediately: true });
            // Starts after the handler returned, so its line job queues behind the step that ends the invocation.
            watching = yield* Effect.forkDetach(
              flush.pipe(
                Effect.andThen(
                  Effect.suspend(() => {
                    watchQueued = true;
                    return failureMessage(Effect.scoped(runtime.watchDoc(Notes)));
                  }),
                ),
              ),
            );
          }),
        );
        const h = yield* openRoot(storage, [Late]);
        harnessRef = h;
        yield* h.harness.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            (yield* tx.doc(Notes)).text = "x";
          }),
        );
        const id = yield* start(h.root, Late);
        yield* h.harness.resume;
        yield* eventually(Effect.sync(() => held !== undefined));
        assert.isDefined(held);
        yield* held.entered;
        yield* eventually(Effect.sync(() => watchQueued));
        yield* flush;
        yield* held.release;
        assert.isDefined(watching);
        const exit = yield* awaitBounded(watching);
        assert.isTrue(Exit.isSuccess(exit));
        if (Exit.isSuccess(exit)) assert.include(exit.value, "invocation has ended");
        assert.strictEqual((yield* outcomeOf(h, id)).status, "faulted");
        yield* h.close;
      }),
    ),
  );
});

describeEachBackend("task scheduling", (backend) => {
  it.effect("fails the Harness on a failed reservation commit; reopening runs the task", () =>
    run(backend)(
      Effect.gen(function* () {
        let runs = 0;
        const Once = oneStep("test.once", (_task, runtime) =>
          Effect.gen(function* () {
            runs++;
            yield* runtime.commit(() => Effect.succeed(completed(null)));
          }),
        );
        const storage = yield* controlledStorage;
        const h = yield* openRoot(storage, [Once]);
        const id = yield* start(h.root, Once);
        const waiting = yield* Effect.forkChild(h.harness.awaitTask(id));
        storage.failNextCommit("disk gone");
        yield* h.harness.resume;
        const end = yield* h.harness.closed;
        assert.strictEqual(end.reason, "failed");
        const failure = end.reason === "failed" ? end.error : undefined;
        assert.strictEqual(failure?.message, "disk gone");
        assert.deepStrictEqual(
          h.reports.map((report) => Cause.squash(report.cause)),
          [failure],
        );
        // The pending wait and every later call get SessionFailed with the storage error.
        const waitError = yield* Effect.flip(Fiber.join(waiting));
        assert.strictEqual(waitError._tag, "SessionFailed");
        assert.strictEqual(
          waitError._tag === "SessionFailed" ? waitError.cause : undefined,
          failure,
        );
        const readError = yield* Effect.flip(h.harness.getTask(id));
        assert.strictEqual(readError._tag, "SessionFailed");
        assert.strictEqual(
          readError._tag === "SessionFailed" ? readError.cause : undefined,
          failure,
        );
        const submitError = yield* Effect.flip(h.root.submit({ type: "input", content: "x" }));
        assert.strictEqual(submitError._tag, "SessionFailed");
        assert.strictEqual(runs, 0);
        yield* h.close;

        const reopened = yield* openRoot(storage, [Once]);
        yield* reopened.harness.resume;
        assert.strictEqual((yield* outcomeOf(reopened, id)).status, "completed");
        assert.strictEqual(runs, 1);
        yield* reopened.close;
      }),
    ),
  );

  it.effect("fails the Harness instead of running a task again when its fault write fails", () =>
    run(backend)(
      Effect.gen(function* () {
        let runs = 0;
        const storage = yield* controlledStorage;
        const Throws = oneStep("test.failed-fault", () =>
          Effect.suspend(() => {
            runs++;
            // The next commit is the step's fault write.
            if (runs === 1) storage.failNextCommit("disk gone");
            return Effect.die(new Error("boom"));
          }),
        );
        const h = yield* openRoot(storage, [Throws]);
        const id = yield* start(h.root, Throws);
        yield* h.harness.resume;
        const end = yield* h.harness.closed;
        assert.strictEqual(end.reason, "failed");
        const failure = end.reason === "failed" ? end.error : undefined;
        assert.strictEqual(failure?.message, "disk gone");
        assert.strictEqual(runs, 1);
        assert.deepStrictEqual(
          h.reports.map((report) => Cause.squash(report.cause)),
          [failure],
        );
        yield* h.close;

        // Reopening recovers the task as after a crash: still running, so its phase runs again.
        const reopened = yield* openRoot(storage, [Throws]);
        yield* reopened.harness.resume;
        assert.deepStrictEqual(yield* outcomeOf(reopened, id), {
          status: "faulted",
          error: { message: "boom" },
        });
        assert.strictEqual(runs, 2);
        yield* reopened.close;
      }),
    ),
  );

  // A cancelled wait is the interruption of the fiber running it.
  it.effect(
    "waits for Harness and conversation idleness, counting blocked work and ignoring background tasks",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const gates = new Map<TaskId, Deferred.Deferred<void>>();
          const Gated = oneStep("test.gated", (task, runtime) =>
            Effect.gen(function* () {
              const gate = yield* Deferred.make<void>();
              gates.set(task.id, gate);
              yield* Deferred.await(gate);
              yield* runtime.commit(() => Effect.succeed(completed(null)));
            }),
          );
          const open = (id: TaskId) => {
            const gate = gates.get(id);
            return gate === undefined
              ? Effect.die(new Error(`no gate for ${id}`))
              : Deferred.succeed(gate, undefined);
          };
          const storage = yield* controlledStorage;
          const h = yield* openRoot(storage, [Gated]);
          yield* h.harness.awaitIdle;
          const other = yield* h.harness.createConversation({ ownership: { kind: "ownerless" } });
          const foreground = yield* start(h.root, Gated);
          const background = yield* start(h.root, Gated, { background: true });
          const elsewhere = yield* start(other, Gated);
          // Work that has not started yet is live; cancelling a wait only rejects that wait.
          const cancelledWait = yield* Effect.forkChild(h.harness.awaitIdle, {
            startImmediately: true,
          });
          yield* flush;
          assert.isUndefined(cancelledWait.pollUnsafe());
          yield* Fiber.interrupt(cancelledWait);
          assert.isTrue(Exit.hasInterrupts(yield* Fiber.await(cancelledWait)));
          // A wait whose caller is already cancelled: an Effect cannot start in a fiber already interrupted, so the
          // wait starts at once (enabling scheduling and registering, in its first step) and is interrupted before
          // any other fiber runs.
          const alreadyCancelled = yield* Effect.forkChild(h.root.awaitIdle, {
            startImmediately: true,
          });
          assert.isUndefined(alreadyCancelled.pollUnsafe());
          yield* Fiber.interrupt(alreadyCancelled);
          assert.isTrue(Exit.hasInterrupts(yield* Fiber.await(alreadyCancelled)));
          // Waits call `resume()` before they register: the cancelled ones enabled scheduling already.
          assert.strictEqual((yield* h.harness.inspect).scheduling, "running");
          yield* h.harness.resume;
          yield* eventually(Effect.sync(() => gates.size === 3));
          const rootIdle = yield* Effect.forkChild(h.root.awaitIdle, { startImmediately: true });
          const harnessIdle = yield* Effect.forkChild(h.harness.awaitIdle, {
            startImmediately: true,
          });
          yield* open(foreground);
          assert.isTrue(Exit.isSuccess(yield* awaitBounded(rootIdle)));
          assert.isFalse(yield* settled(harnessIdle));
          yield* open(elsewhere);
          assert.isTrue(Exit.isSuccess(yield* awaitBounded(harnessIdle)));
          assert.strictEqual(
            Option.getOrUndefined(yield* h.harness.getTask(background))?.state.status,
            "running",
          );
          yield* open(background);
          yield* h.harness.awaitTask(background);
          yield* h.close;
          assert.strictEqual((yield* Effect.flip(h.harness.awaitIdle))._tag, "SessionClosed");
          assert.strictEqual((yield* Effect.flip(h.root.awaitIdle))._tag, "SessionClosed");
        }),
      ),
  );

  it.effect("pages the newest tasks first without scanning older ones", () =>
    run(backend)(
      Effect.gen(function* () {
        const Idle = oneStep("test.idle", () => Effect.void);
        const storage = yield* controlledStorage;
        const h = yield* openRoot(storage, [Idle]);
        const ids: Array<TaskId> = [];
        for (let index = 0; index < 5; index++) ids.push(yield* start(h.root, Idle));
        const first = yield* h.harness.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            return yield* tx.scanTasks({ order: "descending" }, 2);
          }),
        );
        assert.deepStrictEqual(
          first.items.map(({ id }) => id),
          ids.slice(3).toReversed(),
        );
        const second = yield* h.harness.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            return yield* tx.scanTasks({}, 2, Option.getOrUndefined(first.next));
          }),
        );
        assert.deepStrictEqual(
          second.items.map(({ id }) => id),
          ids.slice(1, 3).toReversed(),
        );
        yield* h.close;
      }),
    ),
  );

  it.effect("rejects unknown tasks and reports terminal tasks", () =>
    run(backend)(
      Effect.gen(function* () {
        const Done = oneStep("test.quick", (_task, runtime) =>
          runtime.commit(() => Effect.succeed(completed(null))),
        );
        const storage = yield* controlledStorage;
        const h = yield* openRoot(storage, [Done]);
        const unknown = TaskId.make(999_999);
        assert.isTrue(Option.isNone(yield* h.harness.getTask(unknown)));
        assert.include(
          (yield* Effect.flip(h.harness.awaitTask(unknown))).message,
          "does not exist",
        );
        assert.include(
          (yield* Effect.flip(h.harness.abortTask(unknown))).message,
          "does not exist",
        );
        const id = yield* start(h.root, Done);
        yield* h.harness.resume;
        yield* h.harness.awaitTask(id);
        assert.strictEqual((yield* h.harness.awaitTask(id)).state.status, "terminal");
        assert.deepStrictEqual(yield* h.harness.abortTask(id), { _tag: "AlreadyTerminal" });
        yield* h.close;
        assert.strictEqual((yield* Effect.flip(h.harness.resume))._tag, "SessionClosed");
      }),
    ),
  );

  // A cancelled queued wait is the interruption of the fiber running it.
  it.effect(
    "rejects task waits cancelled or closed while queued on the line, and pending waits on close",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const gate = yield* Deferred.make<void>();
          const Blocking = gated("test.wait-close", gate);
          const storage = yield* controlledStorage;
          const h = yield* openRoot(storage, [Blocking]);
          const id = yield* start(h.root, Blocking);
          const blockerCommit = h.harness.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              yield* tx.appendEntry(h.root.id, { kind: "blocker" });
            }),
          );

          // Hold the line with a commit, then queue waits behind it.
          const held = yield* storage.holdCommits;
          const blocker = yield* Effect.forkChild(blockerCommit, { startImmediately: true });
          yield* held.entered;
          const cancelledWhileQueued = yield* Effect.forkChild(h.harness.awaitTask(id), {
            startImmediately: true,
          });
          yield* flush;
          yield* Fiber.interrupt(cancelledWhileQueued);
          yield* held.release;
          yield* Fiber.join(blocker);
          assert.isTrue(Exit.hasInterrupts(yield* Fiber.await(cancelledWhileQueued)));

          const pending = yield* Effect.forkChild(h.harness.awaitTask(id), {
            startImmediately: true,
          });
          const idle = yield* Effect.forkChild(h.harness.awaitIdle, { startImmediately: true });
          const conversationIdle = yield* Effect.forkChild(h.root.awaitIdle, {
            startImmediately: true,
          });
          yield* flush;
          const heldAgain = yield* storage.holdCommits;
          const blockerAgain = yield* Effect.forkChild(blockerCommit, { startImmediately: true });
          yield* heldAgain.entered;
          const closedWhileQueued = yield* Effect.forkChild(h.harness.awaitTask(id), {
            startImmediately: true,
          });
          const closing = yield* Effect.forkChild(h.close, { startImmediately: true });
          yield* heldAgain.release;
          yield* Fiber.join(blockerAgain);
          assert.strictEqual(yield* closedTag(closedWhileQueued), "SessionClosed");
          assert.strictEqual(yield* closedTag(pending), "SessionClosed");
          assert.strictEqual(yield* closedTag(idle), "SessionClosed");
          assert.strictEqual(yield* closedTag(conversationIdle), "SessionClosed");
          yield* Deferred.succeed(gate, undefined);
          yield* Fiber.join(closing);
        }),
      ),
  );
});

describeEachBackend("task abort", (backend) => {
  // The work after the abort signal runs in the interruption's finalizer, since an interrupted fiber cannot resume.
  it.effect(
    "rejects the run's commits and memo writes after the mark, and settles through a fresh abort invocation",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const reached = yield* Deferred.make<void>();
          const errors: Array<string> = [];
          let memoRead: unknown;
          const abortRuntimes: Array<object> = [];
          let runRuntime: object | undefined;
          const abortSeen: Array<unknown> = [];
          const Marked = oneStep(
            "test.marked",
            (_task, runtime) =>
              Effect.gen(function* () {
                runRuntime = runtime;
                yield* runtime.memo("kept", 1);
                yield* Deferred.succeed(reached, undefined);
                // Keep working after the signal: every later write of this run must reject.
                return yield* Effect.never.pipe(
                  Effect.onInterrupt(() =>
                    Effect.gen(function* () {
                      memoRead = Option.getOrUndefined(yield* runtime.memo("kept"));
                      const late = yield* Effect.flip(runtime.memo("late", 2));
                      errors.push(late.message);
                      const commit = yield* Effect.flip(
                        runtime.commit(() => Effect.succeed(completed(null))),
                      );
                      errors.push(commit.message);
                    }).pipe(
                      Effect.catchCause((cause) =>
                        Effect.sync(() => {
                          errors.push(Cause.pretty(cause));
                        }),
                      ),
                    ),
                  ),
                );
              }),
            (_task, runtime) =>
              Effect.gen(function* () {
                abortRuntimes.push(runtime);
                yield* runtime.commit((current) =>
                  Effect.sync(() => {
                    abortSeen.push(current.abortRequested, current.memos);
                    return abortedWith("mark");
                  }),
                );
              }),
          );
          const storage = yield* controlledStorage;
          const h = yield* openRoot(storage, [Marked]);
          const id = yield* start(h.root, Marked);
          yield* h.harness.resume;
          yield* Deferred.await(reached);
          assert.deepStrictEqual(yield* h.harness.abortTask(id), { _tag: "Marked" });
          assert.deepStrictEqual(yield* outcomeOf(h, id), { status: "aborted", reason: "mark" });
          assert.deepStrictEqual(abortSeen, [true, { kept: 1 }]);
          assert.strictEqual(memoRead, 1);
          assert.deepStrictEqual(errors, [
            `Task ${id} has a durable abort mark`,
            `Task ${id} has a durable abort mark`,
          ]);
          assert.strictEqual(abortRuntimes.length, 1);
          assert.notStrictEqual(abortRuntimes[0], runRuntime);
          yield* h.close;
        }),
      ),
  );

  it.effect("starts no further phase after a mark that lands during a phase with progress", () =>
    run(backend)(
      Effect.gen(function* () {
        const reached = yield* Deferred.make<void>();
        const proceed = yield* Deferred.make<void>();
        const phases: Array<string> = [];
        const Two = Task.make({
          name: "test.mark-boundary",
          version: 1,
          input: Schema.Null,
          checkpoint: Schema.Union([
            Schema.Struct({ phase: Schema.Literal("one") }),
            Schema.Struct({ phase: Schema.Literal("two") }),
          ]),
          result: Schema.Null,
          initial: () => ({ phase: "one" }),
          phases: {
            // Ignores the signal and returns normally after the mark: an uninterruptible region.
            one: (_task, runtime) =>
              Effect.uninterruptible(
                Effect.gen(function* () {
                  phases.push("one");
                  yield* runtime.commit(() =>
                    Effect.succeed({ status: "running", checkpoint: { phase: "two" } } as const),
                  );
                  yield* Deferred.succeed(reached, undefined);
                  yield* Deferred.await(proceed);
                }),
              ),
            two: () =>
              Effect.sync(() => {
                phases.push("two");
              }),
          },
          abort: (_task, runtime) =>
            Effect.gen(function* () {
              phases.push("abort");
              yield* runtime.commit(() => Effect.succeed(abortedWith("boundary")));
            }),
        });
        const storage = yield* controlledStorage;
        const h = yield* openRoot(storage, [Two]);
        const id = yield* h.root.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            return yield* tx.createTask(Two, null, { ownership: { kind: "conversation" } });
          }),
        );
        yield* h.harness.resume;
        yield* Deferred.await(reached);
        const aborting = yield* markDurably(h, id);
        yield* Deferred.succeed(proceed, undefined);
        assert.deepStrictEqual(yield* Fiber.join(aborting), { _tag: "Marked" });
        assert.deepStrictEqual(yield* outcomeOf(h, id), { status: "aborted", reason: "boundary" });
        assert.deepStrictEqual(phases, ["one", "abort"]);
        yield* h.close;
      }),
    ),
  );

  it.effect("signals and joins the run before returning, then runs the abort handler", () =>
    run(backend)(
      Effect.gen(function* () {
        const reached = yield* Deferred.make<void>();
        let runEnded = false;
        const Signalled = oneStep("test.signalled", () =>
          Deferred.succeed(reached, undefined).pipe(
            Effect.andThen(Effect.never),
            Effect.ensuring(
              Effect.sync(() => {
                runEnded = true;
              }),
            ),
          ),
        );
        const storage = yield* controlledStorage;
        const h = yield* openRoot(storage, [Signalled]);
        const id = yield* start(h.root, Signalled);
        yield* h.harness.resume;
        yield* Deferred.await(reached);
        assert.deepStrictEqual(yield* h.harness.abortTask(id), { _tag: "Marked" });
        assert.isTrue(runEnded);
        assert.deepStrictEqual(yield* h.harness.abortTask(id), { _tag: "Marked" });
        assert.deepStrictEqual(yield* outcomeOf(h, id), { status: "aborted", reason: "test" });
        yield* h.close;
      }),
    ),
  );

  it.effect(
    "aborts waiting work before its wait ends and faults abort handlers that throw or settle nothing",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const gate = yield* Deferred.make<void>();
          const First = gated("test.dependency", gate);
          let first: TaskId | undefined;
          const wait: StepHandler<null> = (_task, runtime) => {
            const checkpoint = { phase: "run" } as const;
            return runtime.commit(() =>
              Effect.succeed({
                status: "waiting",
                checkpoint,
                on: first === undefined ? [] : [first],
                policy: "allSettled",
              } as const),
            );
          };
          const Lazy = oneStep("test.lazy-abort", wait, () => Effect.void);
          const Throwing = oneStep("test.throwing-abort", wait, () =>
            Effect.die(new Error("abort failed")),
          );
          const storage = yield* controlledStorage;
          const h = yield* openRoot(storage, [First, Lazy, Throwing]);
          first = yield* start(h.root, First);
          const lazy = yield* start(h.root, Lazy);
          const throwing = yield* start(h.root, Throwing);
          yield* h.harness.resume;
          const isWaiting = (id: TaskId) =>
            h.harness
              .getTask(id)
              .pipe(
                Effect.map((found) => Option.getOrUndefined(found)?.state.status === "waiting"),
              );
          yield* eventually(isWaiting(throwing));
          yield* eventually(isWaiting(lazy));
          yield* h.harness.abortTask(lazy);
          yield* h.harness.abortTask(throwing);
          assert.deepStrictEqual(yield* outcomeOf(h, lazy), {
            status: "faulted",
            error: { message: `Abort handler of task ${lazy} returned without a terminal outcome` },
          });
          assert.deepStrictEqual(yield* outcomeOf(h, throwing), {
            status: "faulted",
            error: { message: "abort failed" },
          });
          assert.strictEqual(
            Option.getOrUndefined(yield* h.harness.getTask(first))?.state.status,
            "running",
          );
          yield* Deferred.succeed(gate, undefined);
          yield* h.harness.awaitTask(first);
          yield* h.close;
        }),
      ),
  );

  // An interruption of the abort handler is its abort signal. The caller that gives up is an interrupted fiber running
  // `abortTask`.
  it.effect(
    "does not signal a running abort handler when aborted again, and a cancelled caller leaves the mark durable",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const reached = yield* Deferred.make<void>();
          const proceed = yield* Deferred.make<void>();
          const runRelease = yield* Deferred.make<void>();
          let abortSignalled = false;
          let aborts = 0;
          const Run = oneStep(
            "test.abort-again",
            // Ignores the signal, so the first caller is still joining when it gives up.
            () => Effect.uninterruptible(Deferred.await(runRelease)),
            (_task, runtime) =>
              Effect.gen(function* () {
                aborts++;
                yield* Deferred.succeed(reached, undefined);
                yield* Deferred.await(proceed).pipe(
                  Effect.onInterrupt(() =>
                    Effect.sync(() => {
                      abortSignalled = true;
                    }),
                  ),
                );
                yield* runtime.commit(() => Effect.succeed(abortedWith("once")));
              }),
          );
          const storage = yield* controlledStorage;
          const h = yield* openRoot(storage, [Run]);
          const id = yield* start(h.root, Run);
          yield* h.harness.resume;
          yield* flush;
          // This caller gives up while joining; the mark and the abort invocation are unaffected.
          const cancelled = yield* markDurably(h, id);
          yield* Fiber.interrupt(cancelled);
          assert.isTrue(Exit.hasInterrupts(yield* Fiber.await(cancelled)));
          yield* Deferred.succeed(runRelease, undefined);
          yield* Deferred.await(reached);
          assert.deepStrictEqual(yield* h.harness.abortTask(id), { _tag: "Marked" });
          yield* Deferred.succeed(proceed, undefined);
          assert.deepStrictEqual(yield* outcomeOf(h, id), { status: "aborted", reason: "once" });
          assert.isFalse(abortSignalled);
          assert.strictEqual(aborts, 1);
          yield* h.close;
        }),
      ),
  );

  it.effect("keeps a terminal outcome committed by an abort handler that throws afterwards", () =>
    run(backend)(
      Effect.gen(function* () {
        const Run = oneStep(
          "test.abort-then-throw",
          () => Effect.never,
          (_task, runtime) =>
            Effect.gen(function* () {
              yield* runtime.commit(() => Effect.succeed(abortedWith("done")));
              return yield* Effect.die(new Error("after terminal"));
            }),
        );
        const storage = yield* controlledStorage;
        const h = yield* openRoot(storage, [Run]);
        const id = yield* start(h.root, Run);
        yield* h.harness.resume;
        yield* flush;
        yield* h.harness.abortTask(id);
        assert.deepStrictEqual(yield* outcomeOf(h, id), { status: "aborted", reason: "done" });
        yield* h.close;
      }),
    ),
  );

  it.effect("never starts phase one when the abort lands while the reservation settles", () =>
    run(backend)(
      Effect.gen(function* () {
        let ran = false;
        const Reserved = oneStep("test.reserved", () =>
          Effect.suspend(() => {
            ran = true;
            return Effect.never;
          }),
        );
        const storage = yield* controlledStorage;
        const h = yield* openRoot(storage, [Reserved]);
        const id = yield* start(h.root, Reserved);
        const held = yield* storage.holdCommits;
        yield* h.harness.resume;
        yield* held.entered;
        // The reservation commit is in storage; the abort mark commit queues behind it.
        const aborting = yield* Effect.forkChild(h.harness.abortTask(id), {
          startImmediately: true,
        });
        yield* held.release;
        assert.deepStrictEqual(yield* Fiber.join(aborting), { _tag: "Marked" });
        assert.deepStrictEqual(yield* outcomeOf(h, id), { status: "aborted", reason: "test" });
        assert.isFalse(ran);
        yield* h.close;
      }),
    ),
  );

  it.effect("lets an abort mark win over a fault that races it", () =>
    run(backend)(
      Effect.gen(function* () {
        const storage = yield* controlledStorage;
        let marking: Fiber.Fiber<unknown, unknown> | undefined;
        let held: Gate | undefined;
        let harnessRef: TestHarness | undefined;
        const Racing = oneStep("test.racing-fault", (task) =>
          Effect.gen(function* () {
            const gate = yield* storage.holdCommits;
            held = gate;
            if (harnessRef !== undefined) {
              marking = yield* Effect.forkDetach(harnessRef.harness.abortTask(task.id), {
                startImmediately: true,
              });
            }
            yield* gate.entered;
            // The mark is in storage but not committed when the handler throws, so the fault commit queues
            // behind it.
            return yield* Effect.die(new Error("would fault"));
          }),
        );
        const h = yield* openRoot(storage, [Racing]);
        harnessRef = h;
        const id = yield* start(h.root, Racing);
        yield* h.harness.resume;
        yield* eventually(Effect.sync(() => held !== undefined));
        assert.isDefined(held);
        yield* held.entered;
        yield* flush;
        yield* held.release;
        assert.deepStrictEqual(yield* outcomeOf(h, id), { status: "aborted", reason: "test" });
        assert.isDefined(marking);
        const marked = yield* awaitBounded(marking);
        assert.deepStrictEqual(Exit.isSuccess(marked) ? marked.value : marked, { _tag: "Marked" });
        yield* h.close;
      }),
    ),
  );
});

describeEachBackend("task close", (backend) => {
  it.effect(
    "stops without outcomes and starts no fresh phase or abort invocation while closing",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const reached = yield* Deferred.make<void>();
          const proceed = yield* Deferred.make<void>();
          const phases: Array<string> = [];
          let abortRan = false;
          const Two = Task.make({
            name: "test.two",
            version: 1,
            input: Schema.Null,
            checkpoint: Schema.Union([
              Schema.Struct({ phase: Schema.Literal("one") }),
              Schema.Struct({ phase: Schema.Literal("two") }),
            ]),
            result: Schema.Null,
            initial: () => ({ phase: "one" }),
            phases: {
              // Ignores signals and returns normally once released; the closing rule wins over the next phase.
              one: (_task, runtime) =>
                Effect.uninterruptible(
                  Effect.gen(function* () {
                    phases.push("one");
                    yield* runtime.commit(() =>
                      Effect.succeed({ status: "running", checkpoint: { phase: "two" } } as const),
                    );
                    yield* Deferred.succeed(reached, undefined);
                    yield* Deferred.await(proceed);
                  }),
                ),
              two: () =>
                Effect.sync(() => {
                  phases.push("two");
                }),
            },
            abort: () =>
              Effect.sync(() => {
                abortRan = true;
              }),
          });
          const storage = yield* controlledStorage;
          const h = yield* openRoot(storage, [Two]);
          const id = yield* h.root.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              return yield* tx.createTask(Two, null, { ownership: { kind: "conversation" } });
            }),
          );
          yield* h.harness.resume;
          yield* Deferred.await(reached);
          const aborting = yield* markDurably(h, id);
          const record = Option.getOrUndefined(yield* h.harness.getTask(id));
          const commits = storage.commits.length;
          const closing = yield* Effect.forkChild(h.close, { startImmediately: true });
          yield* Deferred.succeed(proceed, undefined);
          yield* Fiber.join(closing);
          assert.deepStrictEqual(yield* Fiber.join(aborting), { _tag: "Marked" });
          // No outcome: close writes only the stopped task back as pending, so a reopen counts no restart.
          assert.strictEqual(storage.commits.length, commits + 1);
          const written = storage.commits.at(-1)?.[0];
          assert.strictEqual(written?.type, "task");
          if (written?.type === "task") {
            assert.strictEqual(written.value.id, id);
            assert.isTrue(written.value.abortRequested);
            assert.deepStrictEqual(written.value.state, {
              status: "pending",
              checkpoint: { phase: "two" },
            });
          }
          assert.deepStrictEqual(phases, ["one"]);
          assert.isFalse(abortRan);
          assert.isTrue(record?.abortRequested);
          assert.deepStrictEqual(record?.state, {
            status: "running",
            checkpoint: { phase: "two" },
          });
          assert.strictEqual(
            (yield* Effect.flip(h.harness.commit(Effect.void)))._tag,
            "SessionClosed",
          );
        }),
      ),
  );

  // The handler's abort signal is its interruption; a watch is closed when its frame stream ends, which ends
  // the same way for a stopped watch and a closed Session.
  it.effect(
    "seals admission before signalling handlers and stops watches before joining them",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const Notes = Doc.make({
            checkpointWhen: () => false,
            kind: "test.close-notes",
            version: 1,
            scope: "session",
            schema: Schema.Struct({ text: Schema.String }),
            initial: () => ({ text: "" }),
          });
          const reached = yield* Deferred.make<void>();
          let fromListener: Exit.Exit<void, CommitError> | undefined;
          let harnessRef: TestHarness | undefined;
          let watchEnd: ReadonlyArray<unknown> | undefined;
          const watchScope = yield* Effect.scope;
          const Stubborn = oneStep("test.stubborn", (_task, runtime) =>
            Effect.gen(function* () {
              // Uses a scope close does not end, then waits for the watch to close.
              const watch = yield* Scope.provide(runtime.watchDoc(Notes), watchScope);
              if (Option.isNone(watch)) return yield* Effect.die(new Error("expected a watch"));
              // The listener is installed before `reached`.
              return yield* Deferred.succeed(reached, undefined).pipe(
                Effect.andThen(Effect.never),
                Effect.onInterrupt(() =>
                  Effect.gen(function* () {
                    if (harnessRef !== undefined) {
                      fromListener = yield* Effect.exit(harnessRef.harness.commit(Effect.void));
                    }
                    watchEnd = yield* Stream.runCollect(watch.value.frames).pipe(Effect.orDie);
                  }),
                ),
              );
            }),
          );
          const storage = yield* controlledStorage;
          const h = yield* openRoot(storage, [Stubborn]);
          harnessRef = h;
          yield* h.harness.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              (yield* tx.doc(Notes)).text = "x";
            }),
          );
          yield* start(h.root, Stubborn);
          yield* h.harness.resume;
          yield* Deferred.await(reached);
          yield* h.close;
          assert.isDefined(fromListener);
          assert.isTrue(Exit.isFailure(fromListener));
          if (Exit.isFailure(fromListener)) {
            assert.strictEqual(
              Option.getOrUndefined(Cause.findErrorOption(fromListener.cause))?._tag,
              "SessionClosed",
            );
          }
          assert.deepStrictEqual(watchEnd, []);
        }),
      ),
  );

  it.effect("writes no fault when close seals while the step after a failed phase is queued", () =>
    run(backend)(
      Effect.gen(function* () {
        const storage = yield* controlledStorage;
        let held: Gate | undefined;
        let harnessRef: TestHarness | undefined;
        const Throws = oneStep("test.close-fault", () =>
          Effect.gen(function* () {
            // The step after this phase queues behind a job that holds the line.
            if (harnessRef !== undefined) held = yield* holdLine(harnessRef);
            return yield* Effect.die(new Error("would fault"));
          }),
        );
        const h = yield* openRoot(storage, [Throws]);
        harnessRef = h;
        const id = yield* start(h.root, Throws);
        yield* h.harness.resume;
        yield* eventually(Effect.sync(() => held !== undefined));
        assert.isDefined(held);
        yield* held.entered;
        yield* flush;
        const closing = yield* Effect.forkChild(h.close, { startImmediately: true });
        yield* held.release;
        yield* Fiber.join(closing);
        // Created, reserved, and written back as pending by close: no fault.
        assert.deepStrictEqual(
          taskWrites(storage, id).map((record) => record.state.status),
          ["pending", "running", "pending"],
        );
      }),
    ),
  );

  it.effect("starts no abort handler whose reservation settles while closing", () =>
    run(backend)(
      Effect.gen(function* () {
        let ran = false;
        const Marked = oneStep(
          "test.close-abort-reservation",
          () => Effect.void,
          () =>
            Effect.sync(() => {
              ran = true;
            }),
        );
        const storage = yield* controlledStorage;
        const h = yield* openRoot(storage, [Marked]);
        const id = yield* start(h.root, Marked);
        assert.deepStrictEqual(yield* h.harness.abortTask(id), { _tag: "Marked" });
        const held = yield* storage.holdCommits;
        yield* h.harness.resume;
        yield* held.entered;
        const closing = yield* Effect.forkChild(h.close, { startImmediately: true });
        yield* held.release;
        yield* Fiber.join(closing);
        assert.isFalse(ran);
        // The reservation landed; close wrote the task back as pending.
        const reserved = (storage.commits.at(-2) ?? []).flatMap((write) =>
          write.type === "task" ? [write.value] : [],
        );
        assert.strictEqual(reserved.length, 1);
        assert.strictEqual(reserved[0]?.id, id);
        assert.isTrue(reserved[0]?.abortRequested);
        assert.strictEqual(reserved[0]?.state.status, "running");
        const writes = lastTaskWrites(storage);
        assert.strictEqual(writes.length, 1);
        assert.isTrue(writes[0]?.abortRequested);
        assert.strictEqual(writes[0]?.state.status, "pending");
      }),
    ),
  );

  it.effect("rejects a runtime commit that was queued on the line when close sealed it", () =>
    run(backend)(
      Effect.gen(function* () {
        const storage = yield* controlledStorage;
        let queued: Fiber.Fiber<Exit.Exit<void, unknown>> | undefined;
        let held: Gate | undefined;
        let harnessRef: TestHarness | undefined;
        const proceed = yield* Deferred.make<void>();
        const Queued = oneStep("test.close-queued-commit", (_task, runtime) =>
          // Ignores the close signal, so the invocation is still alive when the queued commit reaches the line.
          Effect.uninterruptible(
            Effect.gen(function* () {
              // A commit whose callback waits holds the line.
              if (harnessRef !== undefined) held = yield* holdLine(harnessRef);
              queued = yield* Effect.forkDetach(
                Effect.exit(runtime.commit(() => Effect.succeed(completed(null)))),
                { startImmediately: true },
              );
              yield* Deferred.await(proceed);
            }),
          ),
        );
        const h = yield* openRoot(storage, [Queued]);
        harnessRef = h;
        const id = yield* start(h.root, Queued);
        yield* h.harness.resume;
        yield* eventually(Effect.sync(() => held !== undefined));
        assert.isDefined(held);
        yield* held.entered;
        yield* flush;
        const closing = yield* Effect.forkChild(h.close, { startImmediately: true });
        yield* held.release;
        assert.isDefined(queued);
        const queuedExit = yield* awaitBounded(queued);
        // The queued commit fails with `SessionClosed`.
        const commitExit = Exit.isSuccess(queuedExit) ? queuedExit.value : undefined;
        assert.isTrue(commitExit !== undefined && Exit.isFailure(commitExit));
        if (commitExit !== undefined && Exit.isFailure(commitExit)) {
          assert.include(Cause.pretty(commitExit.cause), "Session is closed");
        }
        yield* Deferred.succeed(proceed, undefined);
        yield* Fiber.join(closing);
        // Created, reserved, and written back as pending by close; the queued outcome never landed.
        assert.deepStrictEqual(
          taskWrites(storage, id).map((record) => record.state.status),
          ["pending", "running", "pending"],
        );
      }),
    ),
  );

  // Close starts from a fiber swept across the yield points of the step after phase one and of phase two's
  // dispatch, with the Harness's fibers on small yield budgets. Closing a scope is not synchronous, so the check is
  // the invariant itself: phase two never starts once close has sealed the Session.
  it.effect(
    "starts no next phase when close seals during a step that decided to continue",
    () =>
      Effect.gen(function* () {
        // The sweep moves the seal through the window, so some iterations land after it (phase two ran before close);
        // at least one must land inside.
        let sealedInWindow = 0;
        for (const ops of [3, 5, 8]) {
          for (let yields = 0; yields < 100; yields++) {
            const log: Array<string> = [];
            yield* run(backend)(
              Effect.gen(function* () {
                let harnessRef: TestHarness | undefined;
                let closed = false;
                const Two = Task.make({
                  name: "test.close-in-step",
                  version: 1,
                  input: Schema.Null,
                  checkpoint: Schema.Union([
                    Schema.Struct({ phase: Schema.Literal("one") }),
                    Schema.Struct({ phase: Schema.Literal("two") }),
                  ]),
                  result: Schema.Null,
                  initial: () => ({ phase: "one" }),
                  phases: {
                    one: (_task, runtime) =>
                      Effect.gen(function* () {
                        log.push("one");
                        yield* runtime.commit(() =>
                          Effect.succeed({
                            status: "running",
                            checkpoint: { phase: "two" },
                          } as const),
                        );
                        const ref = harnessRef;
                        if (ref === undefined) return;
                        // Close interrupts this handler, so the close fiber reports its own end.
                        yield* Effect.forkDetach(
                          Effect.gen(function* () {
                            for (let turn = 0; turn < yields; turn++) yield* Effect.yieldNow;
                            yield* ref.close;
                            closed = true;
                          }),
                          { startImmediately: true },
                        );
                      }),
                    two: () =>
                      Effect.sync(() => {
                        log.push(
                          harnessRef?.kernel.closing === true ? "two after the seal" : "two",
                        );
                      }),
                  },
                  abort: () => Effect.void,
                });
                const storage = yield* controlledStorage;
                // The Harness's fibers inherit the yield budget, so the close fiber's turns fall between their
                // steps. Budgets below 3 never let the Session open.
                const h = yield* openRoot(storage, [Two]).pipe(
                  Effect.provideService(Scheduler.MaxOpsBeforeYield, ops),
                );
                harnessRef = h;
                yield* h.root.commit(
                  Effect.gen(function* () {
                    const tx = yield* Tx;
                    return yield* tx.createTask(Two, null, { ownership: { kind: "conversation" } });
                  }),
                );
                yield* h.harness.resume;
                yield* eventually(Effect.sync(() => closed));
              }),
            );
            assert.notInclude(log, "two after the seal", `ops ${ops}, ${yields} yields`);
            if (log.length === 1 && log[0] === "one") sealedInWindow++;
          }
        }
        assert.isAbove(sealedInWindow, 0, "no iteration sealed between phase one and phase two");
      }),
    // 300 Harness opens: a larger bound than the default 5 s.
    serialTimeout(5_538),
  );

  it.effect("joins a reservation that settles while closing without starting its handler", () =>
    run(backend)(
      Effect.gen(function* () {
        let ran = false;
        const Never = oneStep("test.close-reservation", () =>
          Effect.sync(() => {
            ran = true;
          }),
        );
        const storage = yield* controlledStorage;
        const h = yield* openRoot(storage, [Never]);
        const id = yield* start(h.root, Never);
        const held = yield* storage.holdCommits;
        yield* h.harness.resume;
        yield* held.entered;
        const closing = yield* Effect.forkChild(h.close, { startImmediately: true });
        yield* held.release;
        yield* Fiber.join(closing);
        assert.isFalse(ran);
        // The reservation landed; close wrote the task back as pending.
        const taskWrites = (index: number) =>
          (storage.commits.at(index) ?? []).flatMap((write) =>
            write.type === "task" ? [write.value] : [],
          );
        const reserved = taskWrites(-2);
        assert.strictEqual(reserved.length, 1);
        assert.strictEqual(reserved[0]?.id, id);
        assert.strictEqual(reserved[0]?.state.status, "running");
        const written = taskWrites(-1);
        assert.strictEqual(written.length, 1);
        assert.strictEqual(written[0]?.id, id);
        assert.strictEqual(written[0]?.state.status, "pending");
      }),
    ),
  );
});

const reclaimFailed = new StorageFailure({
  reason: new Backend({ message: "reclaim failed", cause: "reclaim failed" }),
});

/** What a storage that still works after adopting a commit lets a test do in that gap. */
interface LateResolving {
  /** Runs `run` once a commit stored a batch `when` holds for, after adoption and before the commit resolves. */
  gap:
    | {
        readonly when: (writes: ReadonlyArray<StorageWrite>) => boolean;
        readonly run: Effect.Effect<void>;
      }
    | undefined;
  /** Fails the next commit that stored a batch this holds for, after adoption. */
  rejectAfterAdopt: ((writes: ReadonlyArray<StorageWrite>) => boolean) | undefined;
}

/**
 * Makes `storage` resolve each commit only after pending work had its turns once the Session adopted it, as a
 * storage that still works after adopting does.
 */
const resolveLate = (storage: ControlledStorage): LateResolving => {
  const late: LateResolving = { gap: undefined, rejectAfterAdopt: undefined };
  storage.intercept.commit = (batches, next) =>
    Effect.gen(function* () {
      const result = yield* next;
      const stored = batches.slice(0, result.stored);
      yield* flush;
      const gap = late.gap;
      if (gap !== undefined && stored.some(gap.when)) {
        late.gap = undefined;
        yield* gap.run;
        // What the gap started runs before the commit resolves.
        yield* flush;
      }
      const reject = late.rejectAfterAdopt;
      if (reject !== undefined && stored.some(reject)) {
        late.rejectAfterAdopt = undefined;
        return yield* reclaimFailed;
      }
      return result;
    });
  return late;
};

/** Whether `publication` makes task `id` terminal. */
const terminal = (publication: CommitPublication, id: TaskId): boolean =>
  publication.changes.some(
    (change) =>
      change.type === "task" && change.value.id === id && change.value.state.status === "terminal",
  );

/** Whether `writes` make a task terminal. */
const endsTask = (writes: ReadonlyArray<StorageWrite>): boolean =>
  writes.some((write) => write.type === "task" && write.value.state.status === "terminal");

/** A host listener of the Harness's Session. */
const observeHost = (h: TestHarness, advance: (publication: CommitPublication) => void) =>
  h.session.observeCommits({ advance, end: () => {} });

/** The `durable.tasks.active` counters `traced` recorded, with their sums. */
const activeTasks = (traced: TestTracer.TestTracer) =>
  traced
    .metrics()
    .filter((snapshot) => snapshot.id === "durable.tasks.active")
    .map((snapshot) => (snapshot.type === "Counter" ? Number(snapshot.state.count) : NaN));

describeEachBackend("publication at adoption", (backend) => {
  /** Both storages: one that resolves right after adopting, one that still works after it. */
  const storages = [
    { name: "memory", open: (_storage: ControlledStorage) => {} },
    { name: "work after adopt", open: (storage: ControlledStorage) => void resolveLate(storage) },
  ] as const;

  it.effect(
    "resumes task and idle waits only once observeCommits observers saw the terminal commit",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const gate = yield* Deferred.make<void>();
          const Gated = gated("test.wake-gated", gate);
          const storage = yield* controlledStorage;
          resolveLate(storage);
          const h = yield* openRoot(storage, [Gated]);
          const seen = new Set<TaskId>();
          yield* observeHost(h, (publication) => {
            for (const change of publication.changes) {
              if (change.type === "task" && change.value.state.status === "terminal") {
                seen.add(change.value.id);
              }
            }
          });
          const id = yield* start(h.root, Gated);
          yield* h.harness.resume;
          const seenWhen = <A, E>(wait: Effect.Effect<A, E>) =>
            Effect.forkChild(wait.pipe(Effect.map(() => seen.has(id))), {
              startImmediately: true,
            });
          const task = yield* seenWhen(h.harness.awaitTask(id));
          const idle = yield* seenWhen(h.harness.awaitIdle);
          const conversationIdle = yield* seenWhen(h.root.awaitIdle);
          yield* Deferred.succeed(gate, undefined);
          assert.deepStrictEqual(
            [yield* joined(task), yield* joined(idle), yield* joined(conversationIdle)],
            [true, true, true],
          );
          yield* h.close;
        }),
      ),
  );

  for (const { name, open } of storages) {
    it.effect(`runs host listeners before the waits a terminal commit resolves (${name})`, () =>
      run(backend)(
        Effect.gen(function* () {
          const storage = yield* controlledStorage;
          open(storage);
          const gate = yield* Deferred.make<void>();
          const Gated = gated("test.order-gated", gate);
          const h = yield* openRoot(storage, [Gated]);
          const order: Array<string> = [];
          const id = yield* start(h.root, Gated);
          yield* observeHost(h, (publication) => {
            if (terminal(publication, id)) order.push(`host adopting=${storage.adopting}`);
          });
          yield* h.harness.resume;
          const waited = yield* Effect.forkChild(
            h.harness.awaitTask(id).pipe(Effect.andThen(Effect.sync(() => order.push("wait")))),
            { startImmediately: true },
          );
          yield* Deferred.succeed(gate, undefined);
          yield* joined(waited);
          assert.deepStrictEqual(order, ["host adopting=false", "wait"]);
          yield* h.close;
        }),
      ),
    );

    it.effect(
      `signals a newly marked run after host listeners saw the mark, outside the storage's adopt (${name})`,
      () =>
        run(backend)(
          Effect.gen(function* () {
            const storage = yield* controlledStorage;
            open(storage);
            const reached = yield* Deferred.make<void>();
            const order: Array<string> = [];
            const Signalled = oneStep("test.order-signalled", () =>
              Deferred.succeed(reached, undefined).pipe(
                Effect.andThen(Effect.never),
                Effect.onInterrupt(() =>
                  Effect.sync(() => order.push(`abort adopting=${storage.adopting}`)),
                ),
              ),
            );
            const h = yield* openRoot(storage, [Signalled]);
            const id = yield* start(h.root, Signalled);
            yield* observeHost(h, (publication) => {
              for (const change of publication.changes) {
                if (
                  change.type === "task" &&
                  change.value.id === id &&
                  change.value.abortRequested
                ) {
                  order.push("host");
                }
              }
            });
            yield* h.harness.resume;
            yield* awaited(reached);
            assert.deepStrictEqual(yield* h.harness.abortTask(id), { _tag: "Marked" });
            assert.deepStrictEqual(order, ["host", "abort adopting=false"]);
            yield* h.close;
          }),
        ),
    );
  }

  it.effect(
    "resolves an awaitIdle called while the storage stores the commit at its publication, after host listeners",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const storage = yield* controlledStorage;
          const late = resolveLate(storage);
          const gate = yield* Deferred.make<void>();
          const Gated = gated("test.gap-idle", gate);
          const h = yield* openRoot(storage, [Gated]);
          const order: Array<string> = [];
          const id = yield* start(h.root, Gated);
          yield* observeHost(h, (publication) => {
            if (terminal(publication, id)) order.push("host");
          });
          const gapped = yield* Deferred.make<Fiber.Fiber<number>>();
          late.gap = {
            when: endsTask,
            run: Effect.gen(function* () {
              const idle = yield* Effect.forkDetach(
                h.harness.awaitIdle.pipe(
                  Effect.orDie,
                  Effect.andThen(
                    Effect.sync(() => order.push(`idle committing=${storage.committing}`)),
                  ),
                ),
                { startImmediately: true },
              );
              yield* Deferred.succeed(gapped, idle);
            }),
          };
          yield* h.harness.resume;
          yield* Deferred.succeed(gate, undefined);
          yield* joined(yield* awaited(gapped));
          assert.deepStrictEqual(order, ["host", "idle committing=false"]);
          yield* h.close;
        }),
      ),
  );

  it.effect(
    "publishes a commit the storage rejects after adopting it, resolves its waits, then fails the Session",
    () => {
      const traced = TestTracer.make();
      return run(backend)(
        Effect.gen(function* () {
          const storage = yield* controlledStorage;
          const late = resolveLate(storage);
          const gate = yield* Deferred.make<void>();
          const Gated = gated("test.reject-after-adopt", gate);
          const h = yield* openRoot(storage, [Gated]);
          const id = yield* start(h.root, Gated);
          const seen: Array<string> = [];
          yield* observeHost(h, (publication) => {
            if (terminal(publication, id)) seen.push("terminal");
          });
          late.rejectAfterAdopt = endsTask;
          yield* h.harness.resume;
          const waited = yield* Effect.forkChild(outcomeOf(h, id), { startImmediately: true });
          yield* Deferred.succeed(gate, undefined);
          assert.deepStrictEqual(yield* joined(waited), { status: "completed", result: null });
          assert.deepStrictEqual(seen, ["terminal"]);
          assert.deepStrictEqual(yield* h.harness.closed, {
            reason: "failed",
            error: reclaimFailed,
          });
          assert.include(
            h.reports.map((report) => report.kind),
            "storage_failed",
          );
          // Counted in when created and out when it ended: Effect's counters keep the sum.
          assert.deepStrictEqual(activeTasks(traced), [0]);
        }).pipe(Effect.provide(traced.layer)),
      );
    },
  );

  it.effect(
    "counts a task created by a commit the storage rejects after adopting it, and its close balances it",
    () => {
      const traced = TestTracer.make();
      return run(backend)(
        Effect.gen(function* () {
          const storage = yield* controlledStorage;
          const late = resolveLate(storage);
          const Idle = oneStep("test.reject-created", () => Effect.void);
          const h = yield* openRoot(storage, [Idle]);
          late.rejectAfterAdopt = (writes) => writes.some((write) => write.type === "task");
          assert.strictEqual(yield* Effect.flip(start(h.root, Idle)), reclaimFailed);
          assert.strictEqual((yield* h.harness.closed).reason, "failed");
          // Counted in by the adopted commit and out by the close: Effect's counters keep the sum.
          assert.deepStrictEqual(activeTasks(traced), [0]);
        }).pipe(Effect.provide(traced.layer)),
      );
    },
  );

  it.effect(
    "keeps a task wait resolved when a host listener closes the Harness on the terminal commit",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const gate = yield* Deferred.make<void>();
          const Gated = gated("test.close-in-listener", gate);
          const storage = yield* controlledStorage;
          const h = yield* openRoot(storage, [Gated]);
          const id = yield* start(h.root, Gated);
          const runFork = Effect.runForkWith(yield* Effect.context<never>());
          let closing: Fiber.Fiber<void> | undefined;
          yield* observeHost(h, (publication) => {
            if (terminal(publication, id)) closing = runFork(h.close);
          });
          yield* h.harness.resume;
          const waited = yield* Effect.forkChild(outcomeOf(h, id), { startImmediately: true });
          yield* Deferred.succeed(gate, undefined);
          assert.deepStrictEqual(yield* joined(waited), { status: "completed", result: null });
          assert.isDefined(closing);
          yield* joined(closing!);
        }),
      ),
  );

  it.effect(
    "ends a task wait when the Harness closes while the terminal commit is stored, and still stores it",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const storage = yield* controlledStorage;
          const late = resolveLate(storage);
          const gate = yield* Deferred.make<void>();
          const Gated = gated("test.close-in-gap", gate);
          const h = yield* openRoot(storage, [Gated]);
          const id = yield* start(h.root, Gated);
          const gapped = yield* Deferred.make<Fiber.Fiber<void>>();
          late.gap = {
            when: endsTask,
            run: Effect.forkDetach(h.close, { startImmediately: true }).pipe(
              Effect.flatMap((closing) => Deferred.succeed(gapped, closing)),
            ),
          };
          yield* h.harness.resume;
          const waited = yield* Effect.forkChild(outcomeOf(h, id), { startImmediately: true });
          yield* Deferred.succeed(gate, undefined);
          yield* joined(yield* awaited(gapped));
          // Close ends the waits before it drains the commit in flight, which is published and stored all the
          // same.
          assert.strictEqual(yield* closedTag(waited), "SessionClosed");
          assert.deepStrictEqual(yield* h.harness.closed, { reason: "closed" });
          const reopened = yield* openRoot(storage, [Gated]);
          const record = yield* reopened.harness.getTask(id);
          assert.strictEqual(Option.getOrUndefined(record)?.state.status, "terminal");
          yield* reopened.close;
        }),
      ),
  );

  it.effect(
    "fails the Session with the storage error when it rejects after adopting, and reports an observer's throw",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const storage = yield* controlledStorage;
          const late = resolveLate(storage);
          const h = yield* openRoot(storage, []);
          const bug = new Error("observer bug");
          yield* h.kernel.followCommits((publication) => {
            if (
              publication.kind === "stored" &&
              publication.changes.some((change) => change.type === "entry")
            ) {
              throw bug;
            }
          });
          late.rejectAfterAdopt = (writes) => writes.some((write) => write.type === "entry");
          const note = h.root.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              yield* tx.appendEntry(h.root.id, { kind: "note", data: {} });
            }),
          );
          assert.strictEqual(yield* Effect.flip(note), reclaimFailed);
          assert.deepStrictEqual(yield* h.harness.closed, {
            reason: "failed",
            error: reclaimFailed,
          });
          const thrown = h.reports.find((report) => Cause.squash(report.cause) === bug);
          assert.strictEqual(thrown?.kind, "listener_error");
          assert.include(
            h.reports.map((report) => report.kind),
            "storage_failed",
          );
        }),
      ),
  );

  it.effect(
    "stores a host listener's own commit before the reconcile commit that the same publication asks for",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const storage = yield* controlledStorage;
          const gate = yield* Deferred.make<void>();
          const Child = gated("test.order-child", gate);
          const Parent = oneStep("test.order-parent", (task, runtime) =>
            Effect.gen(function* () {
              yield* runtime.commit(() =>
                Effect.gen(function* () {
                  const tx = yield* Tx;
                  yield* tx.createTask(Child, null, {
                    ownership: { kind: "task", taskId: task.id },
                  });
                  return undefined;
                }),
              );
              // The child is still live, so the outcome is held as `completing` until a reconcile pass
              // finalizes it.
              yield* runtime.commit(() => Effect.succeed(completed(null)));
            }),
          );
          const h = yield* openRoot(storage, [Parent, Child]);
          const parent = yield* start(h.root, Parent);
          yield* h.harness.resume;
          yield* eventually(
            h.harness
              .getTask(parent)
              .pipe(
                Effect.map(
                  (found) => Option.isSome(found) && found.value.state.status === "completing",
                ),
              ),
          );
          const runFork = Effect.runForkWith(yield* Effect.context<never>());
          yield* observeHost(h, (publication) => {
            const child = publication.changes.find(
              (change) =>
                change.type === "task" &&
                change.value.id !== parent &&
                change.value.state.status === "terminal",
            );
            if (child !== undefined) {
              runFork(
                h.root.commit(
                  Effect.gen(function* () {
                    const tx = yield* Tx;
                    yield* tx.appendEntry(h.root.id, { kind: "note", data: {} });
                  }),
                ),
              );
            }
          });
          const before = storage.commits.length;
          yield* Deferred.succeed(gate, undefined);
          yield* h.harness.awaitTask(parent);
          const kinds = storage.commits.slice(before).map((writes) => {
            if (writes.some((write) => write.type === "entry")) return "host entry";
            const task = writes.find((write) => write.type === "task");
            if (task?.type !== "task" || task.value.state.status !== "terminal") return "other";
            return task.value.id === parent ? "parent terminal" : "child terminal";
          });
          assert.deepStrictEqual(
            kinds.filter((kind) => kind !== "other"),
            ["child terminal", "host entry", "parent terminal"],
          );
          yield* h.close;
        }),
      ),
  );

  it.effect(
    "calls a host listener added on a Harness during a publication only when one was registered at it",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const storage = yield* controlledStorage;
          const h = yield* openRoot(storage, []);
          const note = h.root.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              yield* tx.appendEntry(h.root.id, { kind: "note", data: {} });
            }),
          );
          const runSync = Effect.runSyncWith(yield* Effect.context<never>());
          /** Attaches a host listener from inside a publication, where the caller cannot yield. */
          const attach = (advance: (publication: CommitPublication) => void) =>
            runSync(observeHost(h, advance));
          const late: Array<Seq> = [];
          let stopLate: CommitObservation | undefined;
          // Runs once, during the next publication, after its dispatch was decided.
          let during: (() => void) | undefined;
          yield* h.kernel.followCommits(() => {
            const next = during;
            during = undefined;
            next?.();
          });
          // No host listener at the publication, although the Harness's own observers run: no dispatch, so the
          // late one misses it.
          during = () => {
            stopLate = attach((publication) => {
              late.push(publication.seq);
            });
          };
          yield* note;
          yield* flush;
          assert.deepStrictEqual(late, []);
          yield* note;
          assert.lengthOf(late, 1);
          stopLate?.detach();
          // One registered at adoption: the dispatch reads the listeners when it runs, the late one included.
          yield* observeHost(h, () => {});
          const second: Array<Seq> = [];
          during = () => {
            attach((publication) => {
              second.push(publication.seq);
            });
          };
          yield* note;
          assert.lengthOf(second, 1);
          yield* h.close;
        }),
      ),
  );
});

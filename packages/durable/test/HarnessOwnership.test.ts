// Task and conversation ownership: owned trees, bottom-up abort, and what closing and reopening derive. Every
// case runs on both backends; a reopen is a new Harness over the same storage.
//
// An aborted signal or cancelled context is an interrupted fiber, tasks and tools are static extensions of
// `openChat`, and the internal `configure` is the one the public one uses. Waits on the agent let the
// `TestClock` advance in 5 ms steps (`waitFor`), since a turn spans throttle windows. A tool that awaits its
// abort signal blocks on `Effect.never`, which its invocation's interruption ends.
import { afterEach, assert, it } from "@effect/vitest";
import * as Cause from "effect/Cause";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as AgentStorageModule from "../src/AgentStorage.ts";
import { StorageFailure } from "../src/AgentStorage.ts";
import * as AgentTool from "../src/AgentTool.ts";
import { ToolContext } from "../src/AgentTool.ts";
import * as Doc from "../src/Doc.ts";
import * as Extension from "../src/Extension.ts";
import type { Conversation } from "../src/Harness.ts";
import { ConversationId, type SubmissionId, TaskId } from "../src/Ids.ts";
import { configure } from "../src/internal/agent.ts";
import { LiveDoc } from "../src/Live.ts";
import type { SubmissionRecord } from "../src/Records.ts";
import type { Submission } from "../src/Submission.ts";
import * as Task from "../src/Task.ts";
import { fauxAssistantMessage, fauxText, fauxToolCall } from "../src/testing/FauxModel.ts";
import { Tx } from "../src/Tx.ts";
import { allEntries, chatSetup, FAUX_MODEL, openChat, unanswered, waitFor } from "./ChatSupport.ts";
import {
  awaitBounded,
  bounded,
  completed,
  eventually,
  joined,
  openHarness as openTasks,
  settled,
  type TestHarness,
} from "./HarnessSupport.ts";
import {
  type ControlledStorage,
  controlledStorage,
  describeEachBackend,
  flush,
  run,
  withSession,
} from "./SessionSupport.ts";
import { serialTimeout } from "./Timeouts.ts";

/** Outcome a held task commits once its gate opens. */
type Ending = "completed" | "failed";
const gates = new Map<string, Deferred.Deferred<Ending>>();

function gate(name: string): Deferred.Deferred<Ending> {
  let found = gates.get(name);
  if (found === undefined) {
    found = Deferred.makeUnsafe<Ending>();
    gates.set(name, found);
  }
  return found;
}

/** How often each named task started its run phase. */
const runs = new Map<string, number>();

const aborted = { status: "terminal", outcome: { status: "aborted" } } as const;

/**
 * A task that holds until its named gate opens or it is aborted. With `slowAbort`, its abort handler first waits for
 * the gate `abort.<name>`.
 */
const Hold = Task.make({
  name: "test.hold",
  version: 1,
  input: Schema.Struct({ name: Schema.String, slowAbort: Schema.optionalKey(Schema.Boolean) }),
  checkpoint: Schema.Struct({ phase: Schema.Literal("hold") }),
  result: Schema.Null,
  initial: () => ({ phase: "hold" }),
  phases: {
    // The abort mark interrupts the handler while it waits on its gate.
    hold: (task, runtime) =>
      Effect.gen(function* () {
        runs.set(task.input.name, (runs.get(task.input.name) ?? 0) + 1);
        const ending = yield* Deferred.await(gate(task.input.name));
        yield* runtime.commit(() =>
          Effect.succeed(
            ending === "completed"
              ? completed(null)
              : ({
                  status: "terminal",
                  outcome: { status: "failed", error: { message: "gate failed" } },
                } as const),
          ),
        );
      }),
  },
  abort: (task, runtime) =>
    Effect.gen(function* () {
      if (task.input.slowAbort === true) yield* Deferred.await(gate(`abort.${task.input.name}`));
      yield* runtime.commit(() => Effect.succeed(aborted));
    }),
});

/** Waits on the tasks in its input, then completes; its abort handler ends it `aborted`. */
const Waiter = Task.make({
  name: "test.waiter",
  version: 1,
  // The input holds plain numbers: a task schema's type equals its JSON encoding.
  input: Schema.Struct({ on: Schema.Array(Schema.Number) }),
  checkpoint: Schema.Union([
    Schema.Struct({ phase: Schema.Literal("wait") }),
    Schema.Struct({ phase: Schema.Literal("done") }),
  ]),
  result: Schema.Null,
  initial: () => ({ phase: "wait" }),
  phases: {
    wait: (task, runtime) =>
      runtime.commit(() =>
        Effect.succeed({
          status: "waiting",
          checkpoint: { phase: "done" },
          on: task.input.on.map((id) => TaskId.make(id)),
          policy: "allSettled",
        } as const),
      ),
    done: (_task, runtime) => runtime.commit(() => Effect.succeed(completed(null))),
  },
  abort: (_task, runtime) => runtime.commit(() => Effect.succeed(aborted)),
});

/** Tries to create, in the commit that ends it, a conversation it owns with a held task in it, named by its input. */
const Spawner = Task.make({
  name: "test.spawner",
  version: 1,
  input: Schema.Struct({ name: Schema.String }),
  checkpoint: Schema.Struct({ phase: Schema.Literal("spawn") }),
  result: Schema.Null,
  initial: () => ({ phase: "spawn" }),
  phases: {
    spawn: (task, runtime) =>
      runtime.commit(() =>
        Effect.gen(function* () {
          const tx = yield* Tx;
          const child = yield* tx.createConversation({
            ownership: { kind: "task", taskId: task.id },
          });
          yield* tx.createTask(
            Hold,
            { name: task.input.name },
            { ownership: { kind: "conversation" }, conversationId: child.id },
          );
          return completed(null);
        }),
      ),
  },
  abort: (_task, runtime) => runtime.commit(() => Effect.succeed(aborted)),
});

/** Never registered: aborting it can only orphan it. */
const Unregistered = Task.make({
  name: "test.unregistered",
  version: 1,
  input: Schema.Struct({ name: Schema.String }),
  checkpoint: Schema.Struct({ phase: Schema.Literal("hold") }),
  result: Schema.Null,
  initial: () => ({ phase: "hold" }),
  phases: { hold: () => Effect.void },
  abort: () => Effect.void,
});

function open(name: string, ending: Ending): void {
  Deferred.doneUnsafe(gate(name), Exit.succeed(ending));
}

afterEach(() => {
  gates.clear();
  runs.clear();
});

type Tree = { owner: TaskId; child: ConversationId; inner: TaskId };

interface OwnedChildOptions {
  readonly background?: boolean;
  readonly slowInner?: boolean;
  readonly slowOwner?: boolean;
}

/** The input of a held task, with `slowAbort` only when it is set. */
const holdInput = (name: string, slowAbort: boolean | undefined) =>
  slowAbort === true ? { name, slowAbort: true } : { name };

/** In `parent`, stage task `owner` and a conversation it owns holding task `inner`, in one commit. */
const ownedChild = (parent: Conversation, name: string, options: OwnedChildOptions = {}) =>
  parent.commit(
    Effect.gen(function* () {
      const tx = yield* Tx;
      const ownerInput = holdInput(name, options.slowOwner);
      const innerInput = holdInput(`${name}.inner`, options.slowInner);
      const owner = yield* tx.createTask(Hold, ownerInput, {
        ownership: { kind: "conversation" },
        background: options.background ?? false,
      });
      const child = yield* tx.createConversation({ ownership: { kind: "task", taskId: owner } });
      const inner = yield* tx.createTask(Hold, innerInput, {
        ownership: { kind: "conversation" },
        conversationId: child.id,
      });
      return { owner, child: child.id, inner } satisfies Tree;
    }),
  );

/** Creates a held task named `name` in `conversation`, which owns it. */
const createHold = (conversation: Conversation, name: string) =>
  conversation.commit(
    Effect.gen(function* () {
      const tx = yield* Tx;
      return yield* tx.createTask(Hold, { name }, { ownership: { kind: "conversation" } });
    }),
  );

const status = (opened: TestHarness, id: TaskId) =>
  opened.harness.getTask(id).pipe(Effect.map((found) => Option.getOrThrow(found).state));

const abortRequested = (opened: TestHarness, id: TaskId) =>
  opened.harness.getTask(id).pipe(Effect.map((found) => Option.getOrThrow(found).abortRequested));

/** The outcome status of a task's state, once it has one. */
const outcomeStatus = (state: Effect.Success<ReturnType<typeof status>>) =>
  state.status === "terminal" || state.status === "completing" ? state.outcome.status : undefined;

/** The outcome status of the terminal receipt of `id`. */
const settledStatus = (opened: TestHarness, id: TaskId) =>
  opened.harness.awaitTask(id).pipe(Effect.map((settledTask) => settledTask.state.outcome.status));

const conversationOf = (opened: TestHarness, id: ConversationId) =>
  opened.harness.conversation(id).pipe(Effect.map(Option.getOrThrow));

const diskGone = () =>
  new StorageFailure({
    reason: new AgentStorageModule.Backend({ message: "disk gone", cause: "disk gone" }),
  });

/** Mark a conversation busy with `task` standing in for its run, so submissions queue. */
const busy = (conversation: Conversation, task: TaskId) =>
  conversation.commit(
    Effect.gen(function* () {
      const tx = yield* Tx;
      (yield* tx.doc(LiveDoc, conversation.id)).run = { taskId: task, inputs: [], turns: 0 };
    }),
  );

/** A write that affects model context, so it queues while busy; a data-only write would be appended at once. */
const queuedNote = (text: string) =>
  ({ kind: "note", model: [{ role: "user", content: text, timestamp: 0 }] }) as const;

/** The committed record of a submission. */
const submissionStatus = (opened: TestHarness, id: SubmissionId) =>
  opened.harness.submission(id).pipe(Effect.flatMap((found) => Option.getOrThrow(found).status));

/** The reason of a submission record once unanswered. */
const reasonOf = (record: SubmissionRecord) =>
  record.status === "unanswered" ? record.reason : undefined;

/** Runs `effect` and returns its result once it settles, letting the `TestClock` advance meanwhile. */
const finished = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  Effect.gen(function* () {
    const fiber = yield* Effect.forkChild(effect, { startImmediately: true });
    yield* waitFor(Effect.sync(() => fiber.pollUnsafe() !== undefined));
    return yield* Fiber.join(fiber);
  });

/** Waits until a deferred is done, letting the `TestClock` advance meanwhile. */
const reached = <A>(deferred: Deferred.Deferred<A>) =>
  waitFor(Deferred.isDone(deferred)).pipe(Effect.andThen(Deferred.await(deferred)));

/** The message of the error `effect` fails with. */
const failureOf = <A, E extends Error, R>(effect: Effect.Effect<A, E, R>) =>
  Effect.flip(effect).pipe(Effect.map((error) => error.message));

/** The outcome status of a task's terminal receipt, letting the `TestClock` advance meanwhile. */
const outcomeOf = (opened: TestHarness, id: TaskId) =>
  finished(opened.harness.awaitTask(id)).pipe(Effect.map((done) => done.state.outcome.status));

/** A text result for the model. */
const textOutput = (text: string) => ({ output: [{ type: "text" as const, text }] });

/** A child conversation owned by the calling tool, with the fake model, as a subagent tool creates it. */
const createSubagent = (api: ToolContext["Service"]) =>
  api.commit(
    Effect.gen(function* () {
      const tx = yield* Tx;
      const created = yield* tx.createConversation({
        ownership: { kind: "task", taskId: api.taskId },
      });
      yield* configure(created.id, { model: FAUX_MODEL });
      return created.id;
    }),
  );

const toolCall = (name: string, args: { readonly [key: string]: string }, id: string) =>
  fauxAssistantMessage([fauxToolCall(name, args, { id })], { stopReason: "toolUse" });

const tools = (...list: ReadonlyArray<AgentTool.Any>) => [
  Extension.make({ name: "test.tools", tools: list }),
];

const NoParameters = Schema.Struct({});

/**
 * Counts the Set entries iterated while `body` runs, patching `Set.prototype[Symbol.iterator]`; a
 * finalizer restores it.
 */
const countingSetEntries = <A, E, R>(body: Effect.Effect<A, E, R>) =>
  Effect.gen(function* () {
    const iterator = Set.prototype[Symbol.iterator];
    const values = Set.prototype.values;
    let entries = 0;
    function* counted(this: ReadonlySet<never>) {
      for (const value of values.call(this)) {
        entries++;
        yield value;
      }
    }
    const value = yield* Effect.acquireUseRelease(
      Effect.sync(() => {
        // oxlint-disable-next-line no-extend-native -- iterations are counted by patching the prototype.
        Object.defineProperty(Set.prototype, Symbol.iterator, {
          value: counted,
          configurable: true,
          writable: true,
        });
      }),
      () => body,
      () =>
        Effect.sync(() => {
          // oxlint-disable-next-line no-extend-native -- restores the original iterator.
          Object.defineProperty(Set.prototype, Symbol.iterator, {
            value: iterator,
            configurable: true,
            writable: true,
          });
        }),
    );
    return { value, entries };
  });

const openHarness = (storage?: ControlledStorage) =>
  Effect.gen(function* () {
    const opened = yield* openTasks(storage ?? (yield* controlledStorage), [Hold, Waiter, Spawner]);
    const root = yield* opened.harness.ensureRoot();
    yield* opened.harness.resume;
    return { ...opened, root };
  });

describeEachBackend("ownership", (backend) => {
  it.effect(
    "keeps a conversation busy while its owned foreground subtree has live work, holding the completed owner",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const opened = yield* openHarness();
          const { harness, root } = opened;
          const tree = yield* ownedChild(root, "owner");
          open("owner", "completed");
          yield* eventually(
            status(opened, tree.owner).pipe(Effect.map((state) => state.status === "completing")),
          );
          const idle = yield* Effect.forkChild(root.awaitIdle);
          assert.isFalse(yield* settled(idle));
          assert.isFalse(yield* settled(yield* Effect.forkChild(harness.awaitIdle)));
          open("owner.inner", "completed");
          yield* Fiber.join(idle);
          yield* harness.awaitIdle;
          assert.strictEqual(outcomeStatus(yield* status(opened, tree.owner)), "completed");
          yield* opened.close;
        }),
      ),
  );

  it.effect("holds an owner for work in a conversation its ended child task owns", () =>
    run(backend)(
      Effect.gen(function* () {
        const opened = yield* openHarness();
        const { root } = opened;
        const { owner, child, conversation } = yield* root.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            const owner = yield* tx.createTask(
              Hold,
              { name: "owner" },
              { ownership: { kind: "conversation" } },
            );
            const child = yield* tx.createTask(
              Hold,
              { name: "child" },
              { ownership: { kind: "task", taskId: owner } },
            );
            const conversation = yield* tx.createConversation({
              ownership: { kind: "task", taskId: child },
            });
            return { owner, child, conversation: conversation.id };
          }),
        );
        open("child", "completed");
        assert.strictEqual(yield* settledStatus(opened, child), "completed");
        // The ended child's conversation still belongs to the owner above it.
        const handle = yield* conversationOf(opened, conversation);
        yield* createHold(handle, "late");
        open("owner", "completed");
        yield* eventually(
          status(opened, owner).pipe(Effect.map((state) => state.status === "completing")),
        );
        open("late", "completed");
        assert.strictEqual(yield* settledStatus(opened, owner), "completed");
        yield* opened.close;
      }),
    ),
  );

  it.effect(
    "cascades through ended tasks into an owned background subtree when aborting across background",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const opened = yield* openHarness();
          const { root } = opened;
          // root conversation -> A -> B -> conversation -> background C -> D
          const tree = yield* root.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              const a = yield* tx.createTask(
                Hold,
                { name: "a" },
                { ownership: { kind: "conversation" } },
              );
              const b = yield* tx.createTask(
                Hold,
                { name: "b" },
                { ownership: { kind: "task", taskId: a } },
              );
              const conversation = yield* tx.createConversation({
                ownership: { kind: "task", taskId: b },
              });
              const c = yield* tx.createTask(
                Hold,
                { name: "c" },
                {
                  ownership: { kind: "conversation" },
                  conversationId: conversation.id,
                  background: true,
                },
              );
              const d = yield* tx.createTask(
                Hold,
                { name: "d" },
                { ownership: { kind: "task", taskId: c } },
              );
              return { a, b, c, d };
            }),
          );
          open("b", "completed");
          open("a", "completed");
          assert.strictEqual(yield* settledStatus(opened, tree.a), "completed");
          yield* root.abort({ background: true });
          for (const id of [tree.c, tree.d]) {
            assert.strictEqual(yield* settledStatus(opened, id), "aborted");
          }
          yield* opened.close;
        }),
      ),
  );

  it.effect("keeps ownership queries off finished subtrees", () =>
    run(backend)(
      Effect.gen(function* () {
        const opened = yield* openHarness();
        const { harness, root } = opened;
        // Ended chains under a live owner, each ending in an empty owned conversation.
        const owner = yield* createHold(root, "owner");
        const chains = 50;
        const tasks: Array<TaskId> = [];
        for (let n = 0; n < chains; n++) {
          const ids = yield* root.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              const first = yield* tx.createTask(
                Hold,
                { name: `c${n}.0` },
                { ownership: { kind: "task", taskId: owner } },
              );
              const second = yield* tx.createTask(
                Hold,
                { name: `c${n}.1` },
                { ownership: { kind: "task", taskId: first } },
              );
              yield* tx.createConversation({ ownership: { kind: "task", taskId: second } });
              return [first, second];
            }),
          );
          tasks.push(...ids);
        }
        for (let n = 0; n < chains; n++) {
          open(`c${n}.1`, "completed");
          open(`c${n}.0`, "completed");
        }
        for (const id of tasks) yield* harness.awaitTask(id);
        // Set iterations while the owner settles, a measure of the walks down: one through every ended task iterates
        // hundreds of entries; one over live work alone, none below the owner.
        const { value, entries } = yield* countingSetEntries(
          Effect.suspend(() => {
            open("owner", "completed");
            return settledStatus(opened, owner);
          }),
        );
        assert.strictEqual(value, "completed");
        assert.isBelow(entries, chains);
        yield* opened.close;
      }),
    ),
  );

  it.effect(
    "tracks ownership chains deeper than the call stack",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const opened = yield* openHarness();
          const { root } = opened;
          // 20,000 tasks, each owning the next, created in one commit above a live leaf; they end top-down and hold.
          const depth = 20_000;
          const { top, leaf } = yield* root.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              const top = yield* tx.createTask(
                Hold,
                { name: "top" },
                { ownership: { kind: "conversation" } },
              );
              let owner: TaskId = top;
              for (let n = 0; n < depth; n++) {
                owner = yield* tx.createTask(
                  Hold,
                  { name: `link${n}` },
                  { ownership: { kind: "task", taskId: owner } },
                );
              }
              const leaf = yield* tx.createTask(
                Hold,
                { name: "leaf" },
                { ownership: { kind: "task", taskId: owner } },
              );
              return { top, leaf };
            }),
          );
          for (let n = 0; n < depth; n++) open(`link${n}`, "completed");
          open("top", "completed");
          // Thousands of tasks commit before the condition can hold, so the bound grows with them.
          yield* eventually(
            status(opened, top).pipe(Effect.map((state) => state.status === "completing")),
            depth,
          );
          open("leaf", "completed");
          assert.strictEqual(yield* settledStatus(opened, leaf), "completed");
          assert.strictEqual(yield* settledStatus(opened, top), "completed");
          yield* opened.close;
        }),
      ),
    serialTimeout(28_219),
  );

  it.effect("holds an owner for work a commit creates below ended tasks loaded from storage", () =>
    run(backend)(
      Effect.gen(function* () {
        const storage = yield* controlledStorage;
        const first = yield* openHarness(storage);
        // Live P owns B, which owns A, which owns the empty conversation C; B and A end.
        const tree = yield* first.root.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            const p = yield* tx.createTask(
              Hold,
              { name: "p" },
              { ownership: { kind: "conversation" } },
            );
            const b = yield* tx.createTask(
              Hold,
              { name: "b" },
              { ownership: { kind: "task", taskId: p } },
            );
            const a = yield* tx.createTask(
              Hold,
              { name: "a" },
              { ownership: { kind: "task", taskId: b } },
            );
            const c = yield* tx.createConversation({ ownership: { kind: "task", taskId: a } });
            return { p, a, b, c: c.id };
          }),
        );
        open("a", "completed");
        open("b", "completed");
        yield* first.harness.awaitTask(tree.a);
        yield* first.harness.awaitTask(tree.b);
        yield* first.close;
        // After reopen, B and A are known only once a chain load reads them.
        const reopened = yield* openHarness(storage);
        const c = yield* conversationOf(reopened, tree.c);
        const d = yield* createHold(c, "d");
        open("p", "completed");
        yield* eventually(
          status(reopened, tree.p).pipe(Effect.map((state) => state.status === "completing")),
        );
        open("d", "completed");
        assert.strictEqual(yield* settledStatus(reopened, d), "completed");
        assert.strictEqual(yield* settledStatus(reopened, tree.p), "completed");
        yield* reopened.close;
      }),
    ),
  );

  it.effect("holds an owner for work created later below ended tasks, without a reopen", () =>
    run(backend)(
      Effect.gen(function* () {
        const opened = yield* openHarness();
        const { harness, root } = opened;
        // Live P owns B, which owns A, which owns the empty conversation C; B and A end, C stays.
        const tree = yield* root.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            const p = yield* tx.createTask(
              Hold,
              { name: "p" },
              { ownership: { kind: "conversation" } },
            );
            const b = yield* tx.createTask(
              Hold,
              { name: "b" },
              { ownership: { kind: "task", taskId: p } },
            );
            const a = yield* tx.createTask(
              Hold,
              { name: "a" },
              { ownership: { kind: "task", taskId: b } },
            );
            const c = yield* tx.createConversation({ ownership: { kind: "task", taskId: a } });
            return { p, a, b, c: c.id };
          }),
        );
        open("a", "completed");
        open("b", "completed");
        yield* harness.awaitTask(tree.b);
        const c = yield* conversationOf(opened, tree.c);
        const d = yield* createHold(c, "d");
        open("p", "completed");
        yield* eventually(
          status(opened, tree.p).pipe(Effect.map((state) => state.status === "completing")),
        );
        open("d", "completed");
        assert.strictEqual(yield* settledStatus(opened, d), "completed");
        assert.strictEqual(yield* settledStatus(opened, tree.p), "completed");
        yield* opened.close;
      }),
    ),
  );

  // Live P owns `depth` ended tasks in a chain, the last owning conversation C, empty when they ended. Work started in C
  // later is still P's: every case below must reach it through the ended tasks.
  const endedBridge = (depth: number) =>
    Effect.gen(function* () {
      const opened = yield* openHarness();
      const tree = yield* opened.root.commit(
        Effect.gen(function* () {
          const tx = yield* Tx;
          const p = yield* tx.createTask(
            Hold,
            { name: "p", slowAbort: true },
            { ownership: { kind: "conversation" } },
          );
          const links: Array<TaskId> = [];
          let owner: TaskId = p;
          for (let n = 0; n < depth; n++) {
            owner = yield* tx.createTask(
              Hold,
              { name: `link${n}` },
              { ownership: { kind: "task", taskId: owner } },
            );
            links.push(owner);
          }
          const c = yield* tx.createConversation({ ownership: { kind: "task", taskId: owner } });
          return { p, links, c: c.id };
        }),
      );
      for (let n = depth - 1; n >= 0; n--) open(`link${n}`, "completed");
      for (const id of tree.links) yield* opened.harness.awaitTask(id);
      const c = yield* conversationOf(opened, tree.c);
      const create = (name: string) => createHold(c, name);
      return { opened, root: opened.root, p: tree.p, create };
    });

  for (const depth of [2, 5]) {
    it.effect(
      `aborts work below ${depth} ended tasks with their live owner, which waits for it`,
      () =>
        run(backend)(
          Effect.gen(function* () {
            const { opened, p, create } = yield* endedBridge(depth);
            const d = yield* create("d");
            yield* opened.harness.abortTask(p);
            assert.strictEqual(yield* settledStatus(opened, d), "aborted");
            // P's abort handler ran only after D ended.
            open("abort.p", "completed");
            assert.strictEqual(yield* settledStatus(opened, p), "aborted");
            yield* opened.close;
          }),
        ),
    );

    it.effect(`keeps the root busy and aborts with it work below ${depth} ended tasks`, () =>
      run(backend)(
        Effect.gen(function* () {
          const { opened, root, p, create } = yield* endedBridge(depth);
          open("p", "completed");
          yield* opened.harness.awaitTask(p);
          // P has ended: D and D2 hang below ended tasks only, and are still the root conversation's work.
          const d = yield* create("d");
          const d2 = yield* create("d2");
          assert.isFalse(yield* settled(yield* Effect.forkChild(root.awaitIdle)));
          assert.isFalse(yield* settled(yield* Effect.forkChild(opened.harness.awaitIdle)));
          yield* root.abort();
          for (const id of [d, d2]) assert.strictEqual(yield* settledStatus(opened, id), "aborted");
          yield* opened.close;
        }),
      ),
    );
  }

  it.effect("reaches work below ended tasks across two owned conversations", () =>
    run(backend)(
      Effect.gen(function* () {
        const opened = yield* openHarness();
        const { harness, root } = opened;
        // Live P -> ended B -> conversation C -> ended X -> ended A -> conversation C2; work starts in C2 later. X owns no
        // conversation, only the task A, so only owning a known task keeps it.
        const tree = yield* root.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            const p = yield* tx.createTask(
              Hold,
              { name: "p" },
              { ownership: { kind: "conversation" } },
            );
            const b = yield* tx.createTask(
              Hold,
              { name: "b" },
              { ownership: { kind: "task", taskId: p } },
            );
            const c = yield* tx.createConversation({ ownership: { kind: "task", taskId: b } });
            const x = yield* tx.createTask(
              Hold,
              { name: "x" },
              { ownership: { kind: "conversation" }, conversationId: c.id },
            );
            const a = yield* tx.createTask(
              Hold,
              { name: "a" },
              { ownership: { kind: "task", taskId: x } },
            );
            const c2 = yield* tx.createConversation({ ownership: { kind: "task", taskId: a } });
            return { p, b, x, c2: c2.id };
          }),
        );
        open("a", "completed");
        open("x", "completed");
        open("b", "completed");
        yield* harness.awaitTask(tree.b);
        const c2 = yield* conversationOf(opened, tree.c2);
        const d = yield* createHold(c2, "d");
        open("p", "completed");
        yield* eventually(
          status(opened, tree.p).pipe(Effect.map((state) => state.status === "completing")),
        );
        assert.isFalse(yield* settled(yield* Effect.forkChild(root.awaitIdle)));
        yield* root.abort();
        assert.strictEqual(yield* settledStatus(opened, d), "aborted");
        assert.strictEqual(yield* settledStatus(opened, tree.p), "completed");
        yield* opened.close;
      }),
    ),
  );

  for (const ending of ["completes", "is aborted"] as const) {
    it.effect(
      `frees an owner that ${ending} once the last work below ended tasks ends after a background task there`,
      () =>
        run(backend)(
          Effect.gen(function* () {
            const opened = yield* openHarness();
            const { harness, root } = opened;
            // Live P owns B, which owns A, which owns conversation C with background Z and, later, foreground D.
            const tree = yield* root.commit(
              Effect.gen(function* () {
                const tx = yield* Tx;
                const p = yield* tx.createTask(
                  Hold,
                  { name: "p" },
                  { ownership: { kind: "conversation" } },
                );
                const b = yield* tx.createTask(
                  Hold,
                  { name: "b" },
                  { ownership: { kind: "task", taskId: p } },
                );
                const a = yield* tx.createTask(
                  Hold,
                  { name: "a" },
                  { ownership: { kind: "task", taskId: b } },
                );
                const c = yield* tx.createConversation({ ownership: { kind: "task", taskId: a } });
                const z = yield* tx.createTask(
                  Hold,
                  { name: "z" },
                  { ownership: { kind: "conversation" }, conversationId: c.id, background: true },
                );
                return { p, a, b, c: c.id, z };
              }),
            );
            open("a", "completed");
            open("b", "completed");
            yield* harness.awaitTask(tree.b);
            const c = yield* conversationOf(opened, tree.c);
            const d = yield* c.commit(
              Effect.gen(function* () {
                const tx = yield* Tx;
                return yield* tx.createTask(
                  Hold,
                  { name: "d", slowAbort: true },
                  { ownership: { kind: "conversation" } },
                );
              }),
            );
            if (ending === "completes") {
              open("p", "completed");
              yield* eventually(
                status(opened, tree.p).pipe(Effect.map((state) => state.status === "completing")),
              );
            } else {
              // P waits for D's abort before its own abort handler runs.
              yield* harness.abortTask(tree.p);
              yield* eventually(abortRequested(opened, d));
            }
            // Z ends first, D last.
            open("z", "completed");
            yield* harness.awaitTask(tree.z);
            if (ending === "completes") open("d", "completed");
            else open("abort.d", "completed");
            yield* harness.awaitTask(d);
            const expected = ending === "completes" ? "completed" : "aborted";
            assert.strictEqual(yield* settledStatus(opened, tree.p), expected);
            yield* opened.close;
          }),
        ),
    );
  }

  it.effect("finalizes many held siblings that end together in one pass", () =>
    run(backend)(
      Effect.gen(function* () {
        const opened = yield* openHarness();
        const { root } = opened;
        // Top holds 1000 held children, each holding one live leaf; the leaves end in one commit.
        const count = 1000;
        const { top, leaves } = yield* root.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            const top = yield* tx.createTask(
              Hold,
              { name: "top" },
              { ownership: { kind: "conversation" } },
            );
            const siblings: Array<TaskId> = [];
            for (let n = 0; n < count; n++) {
              siblings.push(
                yield* tx.createTask(
                  Hold,
                  { name: `s${n}` },
                  { ownership: { kind: "task", taskId: top } },
                ),
              );
            }
            // Leaves in reverse sibling order: the last sibling is freed first, so a holder checked after each freed
            // sibling would scan the siblings already finalized each time.
            const leaves: Array<TaskId> = [];
            for (let n = count - 1; n >= 0; n--) {
              const owner = siblings[n]!;
              leaves.push(
                yield* tx.createTask(
                  Hold,
                  { name: `l${n}` },
                  { ownership: { kind: "task", taskId: owner } },
                ),
              );
            }
            return { top, leaves };
          }),
        );
        for (let n = 0; n < count; n++) open(`s${n}`, "completed");
        open("top", "completed");
        // Thousands of tasks commit before the condition can hold, so the bound grows with them.
        yield* eventually(
          status(opened, top).pipe(Effect.map((state) => state.status === "completing")),
          count * 10,
        );
        // The kernel's `commitWith` exposes the transaction's `setTask`.
        yield* opened.kernel.commitWith(
          (tx) =>
            Effect.gen(function* () {
              // Reads before the first write. Ended last to first, so a holder checked after each sibling would scan the
              // ones already finalized every time.
              const records = [];
              for (const id of leaves) records.push(Option.getOrThrow(yield* tx.service.task(id)));
              for (const record of records) {
                yield* tx.setTask({ ...record, state: completed(null) });
              }
            }),
          { conversationId: root.id },
        );
        // Set entries iterated while it all finalizes: one pass is linear, a recheck per sibling quadratic (~500,000).
        const { value, entries } = yield* countingSetEntries(settledStatus(opened, top));
        assert.strictEqual(value, "completed");
        assert.isBelow(entries, count * 20);
        yield* opened.close;
      }),
    ),
  );

  // Pins the Transaction rule that a conversation cannot be created in the commit that ends its owner task.
  it.effect("rejects a conversation created in the commit that ends its owner task", () =>
    run(backend)(
      Effect.gen(function* () {
        const opened = yield* openHarness();
        const spawner = yield* opened.root.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            return yield* tx.createTask(
              Spawner,
              { name: "spawned" },
              { ownership: { kind: "conversation" } },
            );
          }),
        );
        const outcome = (yield* opened.harness.awaitTask(spawner)).state.outcome;
        const message = outcome.status === "faulted" ? outcome.error.message : "";
        assert.deepStrictEqual(outcome, { status: "faulted", error: { message } });
        assert.match(
          message,
          new RegExp(`^Conversation owner task ${spawner} is (completing|terminal)$`),
        );
        yield* opened.close;
      }),
    ),
  );

  it.effect("stops idle traversal at a background owner", () =>
    run(backend)(
      Effect.gen(function* () {
        const opened = yield* openHarness();
        const { harness, root } = opened;
        const tree = yield* ownedChild(root, "background", { background: true });
        yield* root.awaitIdle;
        yield* harness.awaitIdle;
        // The background child is its own scope.
        const child = yield* conversationOf(opened, tree.child);
        assert.isFalse(yield* settled(yield* Effect.forkChild(child.awaitIdle)));
        yield* child.abort();
        yield* opened.close;
      }),
    ),
  );

  it.effect(
    "cascades an abort mark to the owned foreground subtree and withdraws its queued inputs",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const opened = yield* openHarness();
          const { harness, root } = opened;
          const tree = yield* ownedChild(root, "owner");
          const nested = yield* conversationOf(opened, tree.child);
          const deeper = yield* ownedChild(nested, "deeper");
          const shielded = yield* ownedChild(nested, "shielded", { background: true });
          // Busy conversations queue submissions; the inner task stands in for the child's run.
          yield* busy(nested, tree.inner);
          const queued = (yield* nested.submit({ type: "input", content: "later" })).id;
          assert.deepStrictEqual(yield* harness.abortTask(tree.owner), { _tag: "Marked" });
          for (const id of [tree.owner, tree.inner, deeper.owner, deeper.inner]) {
            assert.strictEqual(yield* settledStatus(opened, id), "aborted");
          }
          // A nested background owner is a boundary.
          assert.notStrictEqual((yield* status(opened, shielded.owner)).status, "terminal");
          assert.notStrictEqual((yield* status(opened, shielded.inner)).status, "terminal");
          const record = yield* submissionStatus(opened, queued);
          assert.strictEqual(record.status, "unanswered");
          assert.strictEqual(reasonOf(record), "aborted");
          yield* harness.abortTask(shielded.owner);
          yield* opened.close;
        }),
      ),
  );

  it.effect("cascades a failed owner but not a completed one", () =>
    run(backend)(
      Effect.gen(function* () {
        const opened = yield* openHarness();
        const { root } = opened;
        const failed = yield* ownedChild(root, "failed");
        const completedTree = yield* ownedChild(root, "done");
        open("failed", "failed");
        open("done", "completed");
        assert.strictEqual(yield* settledStatus(opened, failed.inner), "aborted");
        assert.strictEqual(yield* settledStatus(opened, failed.owner), "failed");
        yield* eventually(
          status(opened, completedTree.owner).pipe(
            Effect.map((state) => state.status === "completing"),
          ),
        );
        assert.notStrictEqual((yield* status(opened, completedTree.inner)).status, "terminal");
        open("done.inner", "completed");
        assert.strictEqual(yield* settledStatus(opened, completedTree.owner), "completed");
        yield* opened.close;
      }),
    ),
  );

  it.effect("aborts a background task directly with its ordinary subtree", () =>
    run(backend)(
      Effect.gen(function* () {
        const opened = yield* openHarness();
        const tree = yield* ownedChild(opened.root, "background", { background: true });
        yield* opened.harness.abortTask(tree.owner);
        assert.strictEqual(yield* settledStatus(opened, tree.inner), "aborted");
        yield* opened.close;
      }),
    ),
  );

  it.effect(
    "aborts a conversation: queued inputs withdrawn, writes kept, foreground work aborted, background kept",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const opened = yield* openHarness();
          const { harness, root } = opened;
          const foreground = yield* ownedChild(root, "foreground");
          const background = yield* ownedChild(root, "background", { background: true });
          yield* busy(root, foreground.owner);
          const input = (yield* root.submit({ type: "input", content: "later" })).id;
          const write = (yield* root.submit({ type: "write", entry: queuedNote("w") })).id;
          yield* root.abort();
          for (const id of [foreground.owner, foreground.inner]) {
            assert.strictEqual((yield* status(opened, id)).status, "terminal");
          }
          assert.notStrictEqual((yield* status(opened, background.owner)).status, "terminal");
          assert.notStrictEqual((yield* status(opened, background.inner)).status, "terminal");
          assert.strictEqual(reasonOf(yield* submissionStatus(opened, input)), "aborted");
          assert.strictEqual((yield* submissionStatus(opened, write)).status, "queued");
          yield* harness.abortTask(background.owner);
          yield* opened.close;
        }),
      ),
  );

  it.effect("aborts work created below a held failed owner, but not below a terminal one", () =>
    run(backend)(
      Effect.gen(function* () {
        const opened = yield* openHarness();
        const tree = yield* ownedChild(opened.root, "owner", { slowInner: true });
        open("owner", "failed");
        yield* eventually(abortRequested(opened, tree.inner));
        assert.strictEqual((yield* status(opened, tree.owner)).status, "completing");
        const child = yield* conversationOf(opened, tree.child);
        const create = (name: string) => createHold(child, name);
        const during = yield* create("during");
        assert.strictEqual(yield* settledStatus(opened, during), "aborted");
        open("abort.owner.inner", "completed");
        assert.strictEqual(yield* settledStatus(opened, tree.owner), "failed");
        // A terminal owner never cascades: interrogating its conversation runs normally.
        const after = yield* create("after");
        open("after", "completed");
        assert.strictEqual(yield* settledStatus(opened, after), "completed");
        yield* opened.close;
      }),
    ),
  );

  it.effect(
    "withdraws queued inputs below the aborted task but keeps its own conversation's queue and queued writes",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const opened = yield* openHarness();
          const { harness, root } = opened;
          const tree = yield* ownedChild(root, "owner");
          const child = yield* conversationOf(opened, tree.child);
          yield* busy(root, tree.owner);
          yield* busy(child, tree.inner);
          const own = (yield* root.submit({ type: "input", content: "own" })).id;
          const below = (yield* child.submit({ type: "input", content: "below" })).id;
          const write = (yield* child.submit({ type: "write", entry: queuedNote("w") })).id;
          yield* harness.abortTask(tree.owner);
          yield* harness.awaitTask(tree.inner);
          const statusOf = (id: SubmissionId) =>
            submissionStatus(opened, id).pipe(Effect.map((record) => record.status));
          assert.strictEqual(yield* statusOf(own), "queued");
          assert.strictEqual(yield* statusOf(below), "unanswered");
          assert.strictEqual(yield* statusOf(write), "queued");
          yield* opened.close;
        }),
      ),
  );

  it.effect(
    "keeps a nested background owner's subtree when its cancelled background owner is aborted",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const opened = yield* openHarness();
          const outer = yield* ownedChild(opened.root, "outer", { background: true });
          const inner = yield* ownedChild(yield* conversationOf(opened, outer.child), "inner", {
            background: true,
          });
          yield* opened.harness.abortTask(outer.owner);
          assert.strictEqual(yield* settledStatus(opened, outer.inner), "aborted");
          assert.notStrictEqual((yield* status(opened, inner.owner)).status, "terminal");
          assert.notStrictEqual((yield* status(opened, inner.inner)).status, "terminal");
          yield* opened.harness.abortTask(inner.owner);
          yield* opened.close;
        }),
      ),
  );

  it.effect("decides idle after reopen from owner edges it has to load first", () =>
    run(backend)(
      Effect.gen(function* () {
        const storage = yield* controlledStorage;
        const first = yield* openHarness(storage);
        const foreground = yield* ownedChild(first.root, "fg");
        const deeper = yield* ownedChild(yield* conversationOf(first, foreground.child), "fg2");
        const background = yield* ownedChild(first.root, "bg", { background: true });
        open("fg", "completed");
        open("fg2", "completed");
        // Both owners hold their outcomes while the work below them runs.
        yield* eventually(
          status(first, foreground.owner).pipe(
            Effect.map((state) => state.status === "completing"),
          ),
        );
        yield* eventually(
          status(first, deeper.owner).pipe(Effect.map((state) => state.status === "completing")),
        );
        yield* first.close;

        const opened = yield* openHarness(storage);
        // Two levels below completed foreground owners, the inner tasks keep the root busy.
        assert.isFalse(yield* settled(yield* Effect.forkChild(opened.root.awaitIdle)));
        assert.isFalse(yield* settled(yield* Effect.forkChild(opened.harness.awaitIdle)));
        open("fg.inner", "completed");
        open("fg2.inner", "completed");
        // The background subtree does not count.
        yield* opened.root.awaitIdle;
        yield* opened.harness.awaitIdle;
        assert.strictEqual((yield* status(opened, foreground.owner)).status, "terminal");
        assert.notStrictEqual((yield* status(opened, background.inner)).status, "terminal");
        yield* opened.harness.abortTask(background.owner);
        yield* opened.close;
      }),
    ),
  );

  for (const [label, abortRequestedMark, state] of [
    [
      "a held failed owner",
      false,
      { status: "completing", outcome: { status: "failed", error: { message: "crash" } } },
    ],
    ["an abort-marked owner", true, { status: "pending", checkpoint: { phase: "hold" } }],
  ] as const) {
    it.effect(`derives marks a crash left unapplied below ${label} at open`, () =>
      run(backend)(
        Effect.gen(function* () {
          const storage = yield* controlledStorage;
          // Without a Harness, nothing derives marks: a cancelled owner with a live task below it.
          const tree = yield* withSession(storage, (session) =>
            Effect.gen(function* () {
              const tree = yield* session.commit(
                Effect.gen(function* () {
                  const tx = yield* Tx;
                  const root = yield* tx.createConversation({ ownership: { kind: "ownerless" } });
                  const owner = yield* tx.createTask(
                    Hold,
                    { name: "gone" },
                    { ownership: { kind: "conversation" }, conversationId: root.id },
                  );
                  const child = yield* tx.createConversation({
                    ownership: { kind: "task", taskId: owner },
                  });
                  return {
                    owner,
                    inner: yield* tx.createTask(
                      Hold,
                      { name: "orphan" },
                      { ownership: { kind: "conversation" }, conversationId: child.id },
                    ),
                  };
                }),
              );
              yield* session.commitWith((tx) =>
                Effect.gen(function* () {
                  const record = Option.getOrThrow(yield* tx.service.task(tree.owner));
                  // Narrowed per state: a record holds a live state or a settled one, never the union of both.
                  yield* tx.setTask(
                    state.status === "pending"
                      ? { ...record, abortRequested: abortRequestedMark, state }
                      : { ...record, abortRequested: abortRequestedMark, state },
                  );
                }),
              );
              return tree;
            }),
          );

          const opened = yield* openHarness(storage);
          assert.strictEqual(yield* settledStatus(opened, tree.inner), "aborted");
          // The owner finishes only after the work below it.
          const outcome = yield* settledStatus(opened, tree.owner);
          assert.strictEqual(outcome, abortRequestedMark ? "aborted" : "failed");
          yield* opened.close;
        }),
      ),
    );
  }

  it.effect("withdraws an input queued below a held failed owner after its cascade", () =>
    run(backend)(
      Effect.gen(function* () {
        const opened = yield* openHarness();
        const tree = yield* ownedChild(opened.root, "owner", { slowInner: true });
        const child = yield* conversationOf(opened, tree.child);
        // The inner task stands in for the child's run, which stays busy after the cascade.
        yield* busy(child, tree.inner);
        open("owner", "failed");
        yield* eventually(abortRequested(opened, tree.inner));
        const late = yield* child.submit({ type: "input", content: "late" });
        const settledLate = yield* bounded(late.await);
        assert.strictEqual(settledLate.status, "unanswered");
        assert.strictEqual(reasonOf(settledLate), "aborted");
        open("abort.owner.inner", "completed");
        yield* opened.harness.awaitTask(tree.owner);
        yield* opened.close;
      }),
    ),
  );

  it.effect(
    "marks work admitted after reopen below a cancelled owner whose edge was not loaded",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const storage = yield* controlledStorage;
          const first = yield* openHarness(storage);
          const tree = yield* ownedChild(first.root, "owner", { slowOwner: true });
          open("owner.inner", "completed");
          yield* first.harness.awaitTask(tree.inner);
          // The owner stays live and cancelled in its slow abort handler.
          yield* first.harness.abortTask(tree.owner);
          yield* first.close;

          // The child is empty at open, so nothing loads its edge until new work arrives.
          const opened = yield* openHarness(storage);
          const child = yield* conversationOf(opened, tree.child);
          const late = yield* createHold(child, "late");
          assert.strictEqual(yield* settledStatus(opened, late), "aborted");
          open("abort.owner", "completed");
          assert.strictEqual(yield* settledStatus(opened, tree.owner), "aborted");
          yield* opened.close;
        }),
      ),
  );

  it.effect("cascades from an owner the scheduler orphans", () =>
    run(backend)(
      Effect.gen(function* () {
        const opened = yield* openHarness();
        const { owner, inner } = yield* opened.root.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            const owner = yield* tx.createTask(
              Unregistered,
              { name: "unregistered" },
              { ownership: { kind: "conversation" } },
            );
            const child = yield* tx.createConversation({
              ownership: { kind: "task", taskId: owner },
            });
            return {
              owner,
              inner: yield* tx.createTask(
                Hold,
                { name: "below" },
                { ownership: { kind: "conversation" }, conversationId: child.id },
              ),
            };
          }),
        );
        assert.deepStrictEqual(yield* opened.harness.abortTask(owner), { _tag: "Marked" });
        assert.deepStrictEqual((yield* opened.harness.awaitTask(owner)).state.outcome, {
          status: "orphaned",
          reason: "missing_task",
        });
        assert.strictEqual(yield* settledStatus(opened, inner), "aborted");
        yield* opened.close;
      }),
    ),
  );

  it.effect("cancels only the caller's wait, never the shared work", () =>
    run(backend)(
      Effect.gen(function* () {
        const opened = yield* openHarness();
        const { root } = opened;
        const tree = yield* ownedChild(root, "owner");
        // The caller's fiber is interrupted once its wait is registered.
        const idle = yield* Effect.forkChild(root.awaitIdle, { startImmediately: true });
        assert.isFalse(yield* settled(idle));
        yield* Fiber.interrupt(idle);
        assert.isTrue(Exit.hasInterrupts(yield* Fiber.await(idle)));
        assert.notStrictEqual((yield* status(opened, tree.inner)).status, "terminal");

        // Cancelling an abort after its commit leaves the marks in place.
        const slow = yield* ownedChild(root, "slow");
        yield* opened.kernel.commitWith(
          (tx) =>
            Effect.gen(function* () {
              const record = Option.getOrThrow(yield* tx.service.task(slow.owner));
              yield* tx.setTask({ ...record, input: { name: "slow", slowAbort: true } });
            }),
          { conversationId: root.id },
        );
        const abort = yield* Effect.forkChild(root.abort(), { startImmediately: true });
        yield* eventually(abortRequested(opened, slow.owner));
        yield* Fiber.interrupt(abort);
        assert.isTrue(Exit.hasInterrupts(yield* Fiber.await(abort)));
        open("abort.slow", "completed");
        for (const id of [tree.owner, tree.inner, slow.owner, slow.inner]) {
          assert.strictEqual(yield* settledStatus(opened, id), "aborted");
        }
        yield* opened.close;
      }),
    ),
  );

  it.effect(
    "aborts a waiting child whose awaited task completes in the commit that marks its owner",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const opened = yield* openHarness();
          const { owner, dependency, blocked } = yield* opened.root.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              const owner = yield* tx.createTask(
                Hold,
                { name: "owner" },
                { ownership: { kind: "conversation" } },
              );
              const child = yield* tx.createConversation({
                ownership: { kind: "task", taskId: owner },
              });
              const dependency = yield* tx.createTask(
                Hold,
                { name: "dependency" },
                { ownership: { kind: "conversation" } },
              );
              const blocked = yield* tx.createTask(
                Waiter,
                { on: [dependency] },
                { ownership: { kind: "conversation" }, conversationId: child.id },
              );
              return { owner, dependency, blocked };
            }),
          );
          yield* eventually(
            Effect.sync(() => runs.get("dependency") === 1 && runs.get("owner") === 1),
          );
          yield* eventually(
            status(opened, blocked).pipe(Effect.map((state) => state.status === "waiting")),
          );
          // One commit completes the dependency and marks the owner.
          yield* opened.kernel.commitWith(
            (tx) =>
              Effect.gen(function* () {
                const completedDependency = Option.getOrThrow(yield* tx.service.task(dependency));
                const marked = Option.getOrThrow(yield* tx.service.task(owner));
                yield* tx.setTask({ ...completedDependency, state: completed(null) });
                yield* tx.setTask({ ...marked, abortRequested: true });
              }),
            { conversationId: opened.root.id },
          );
          assert.strictEqual(yield* settledStatus(opened, blocked), "aborted");
          yield* opened.close;
        }),
      ),
  );

  it.effect(
    "applies marks found through an edge loaded after reopen once a failed cascade commit is reopened",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const storage = yield* controlledStorage;
          const first = yield* openHarness(storage);
          const tree = yield* ownedChild(first.root, "owner", { slowOwner: true });
          open("owner.inner", "completed");
          yield* first.harness.awaitTask(tree.inner);
          yield* first.harness.abortTask(tree.owner);
          yield* first.close;

          // The controlled storage's commit hook rejects the commit that writes the mark.
          let rejectMark: TaskId | undefined;
          storage.intercept.commit = (batches, next) => {
            if (
              batches
                .flat()
                .some(
                  (write) =>
                    write.type === "task" &&
                    write.value.id === rejectMark &&
                    write.value.abortRequested,
                )
            ) {
              rejectMark = undefined;
              return Effect.fail(diskGone());
            }
            return next;
          };
          const failing = yield* openHarness(storage);
          const child = yield* conversationOf(failing, tree.child);
          const late = yield* child.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              const id = yield* tx.createTask(
                Hold,
                { name: "late" },
                { ownership: { kind: "conversation" } },
              );
              rejectMark = id;
              return id;
            }),
          );
          // The failed cascade commit fails the Harness. `closed` settles once its invocations have ended; the owner's
          // abort handler is interrupted, so opening its gate changes nothing.
          open("abort.owner", "completed");
          assert.strictEqual((yield* failing.harness.closed).reason, "failed");
          yield* failing.close;
          // Reopening, without the failing hook, derives the mark again.
          storage.intercept.commit = undefined;
          const opened = yield* openHarness(storage);
          assert.strictEqual(yield* settledStatus(opened, late), "aborted");
          yield* opened.close;
        }),
      ),
  );

  it.effect("fails the Harness on a failed cascade commit, and reopening applies the cascade", () =>
    run(backend)(
      Effect.gen(function* () {
        const failure = diskGone();
        let failMark: TaskId | undefined;
        // The controlled storage's commit hook fails the commit that writes the mark.
        const storage = yield* controlledStorage;
        storage.intercept.commit = (batches, next) => {
          const marks = batches
            .flat()
            .some(
              (write) =>
                write.type === "task" && write.value.id === failMark && write.value.abortRequested,
            );
          if (marks) {
            failMark = undefined;
            return Effect.fail(failure);
          }
          return next;
        };
        const { harness, root, reports, close } = yield* openHarness(storage);
        const tree = yield* ownedChild(root, "owner");
        failMark = tree.inner;
        open("owner", "failed");
        const end = yield* harness.closed;
        assert.strictEqual(end.reason, "failed");
        assert.strictEqual(end.reason === "failed" ? end.error : undefined, failure);
        // Reports are Causes, delivered on a fiber of their own: one report whose Cause fails with the storage
        // failure alone.
        yield* flush;
        assert.deepStrictEqual(
          reports.map((report) => [
            report.kind,
            report.cause.reasons.map((reason) =>
              Cause.isFailReason(reason) ? reason.error : reason,
            ),
          ]),
          [["storage_failed", [failure]]],
        );
        yield* close;
        const reopened = yield* openHarness(storage);
        assert.strictEqual(yield* settledStatus(reopened, tree.inner), "aborted");
        yield* reopened.close;
      }),
    ),
  );
});

describeEachBackend("owned conversations from tools and supervisors", (backend) => {
  it.effect(
    "gives a tool an invocation-bound handle whose submissions stay durable after the call ends",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const setup = chatSetup();
          let handle: Task.ConversationHandle | undefined;
          let missing: Option.Option<Task.ConversationHandle> | undefined;
          let submission: Submission | undefined;
          const delegate = AgentTool.make("delegate", {
            description: "Delegates",
            parameters: NoParameters,
            execute: () =>
              Effect.gen(function* () {
                const api = yield* ToolContext;
                const child = yield* createSubagent(api);
                missing = yield* api.conversation(ConversationId.make(99_999));
                const found = Option.getOrThrow(yield* api.conversation(child));
                handle = found;
                const submitted = yield* found.submit({
                  type: "input",
                  content: "child task",
                  requestId: "child",
                });
                submission = submitted;
                const settledChild = yield* submitted.await;
                return textOutput(settledChild.status);
              }),
          });
          setup.faux.setResponses([
            toolCall("delegate", {}, "c1"),
            fauxAssistantMessage([fauxText("child answer")]),
            fauxAssistantMessage([fauxText("done")]),
          ]);
          const { harness, root } = yield* openChat(yield* controlledStorage, setup, {
            extensions: tools(delegate),
          });
          yield* finished(
            root.submit({ type: "input", content: "go" }).pipe(Effect.flatMap((s) => s.await)),
          );
          assert.isTrue(missing !== undefined && Option.isNone(missing));
          if (handle === undefined || submission === undefined) {
            return assert.fail("the tool did not run");
          }
          // The call ended: the handle and its submission reject, while the submission record remains.
          const ended = "invocation has ended";
          assert.include(
            yield* failureOf(handle.submit({ type: "input", content: "again" })),
            ended,
          );
          assert.include(yield* failureOf(handle.awaitIdle), ended);
          assert.include(yield* failureOf(handle.abort()), ended);
          assert.include(yield* failureOf(submission.await), ended);
          // Nothing was admitted or marked after the call ended.
          const childConversation = yield* conversationOf(harness, handle.id);
          const childEntries = yield* allEntries(childConversation);
          assert.lengthOf(
            childEntries.filter((entry) => entry.kind === "core.user"),
            1,
          );
          const childId = handle.id;
          const childTasks = (yield* harness.harness.inspect).tasks.filter(
            (task) => task.record.conversationId === childId,
          );
          assert.deepStrictEqual(childTasks, []);
          assert.strictEqual((yield* submissionStatus(harness, submission.id)).status, "done");
          yield* harness.close;
        }),
      ),
  );

  // The submit runs on a fiber forked detached from the tool's fiber, so the tool's interruption does not end it and
  // only the handle's binding to the invocation can reject it.
  it.effect(
    "rejects a handle operation queued on the line when the invocation ends before it runs",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const setup = chatSetup();
          const ready = Deferred.makeUnsafe<{ child: ConversationId; taskId: TaskId }>();
          const go = Deferred.makeUnsafe<void>();
          const queued = Deferred.makeUnsafe<Fiber.Fiber<Submission, unknown>>();
          const delegate = AgentTool.make("delegate", {
            description: "Delegates late",
            parameters: NoParameters,
            execute: () =>
              Effect.gen(function* () {
                const api = yield* ToolContext;
                const child = yield* api.commit(
                  Effect.gen(function* () {
                    const tx = yield* Tx;
                    const created = yield* tx.createConversation({
                      ownership: { kind: "task", taskId: api.taskId },
                    });
                    return created.id;
                  }),
                );
                const handle = Option.getOrThrow(yield* api.conversation(child));
                yield* Deferred.succeed(ready, { child, taskId: api.taskId });
                yield* Deferred.await(go);
                // Called while the invocation is alive, from a fiber that is not the call's: the handle binds it to
                // the invocation. It reaches the line only after the abort mark.
                const submitting = yield* Effect.forkDetach(
                  handle.submit({ type: "input", content: "late" }),
                  { startImmediately: true },
                );
                yield* Deferred.succeed(queued, submitting);
                return yield* Effect.never;
              }),
          });
          setup.faux.setResponses([toolCall("delegate", {}, "c1")]);
          const { harness, root } = yield* openChat(yield* controlledStorage, setup, {
            extensions: tools(delegate),
          });
          yield* root.submit({ type: "input", content: "go" });
          const { child, taskId } = yield* reached(ready);
          // Hold the Session line, queue the abort mark behind it, then let the tool queue its submit.
          const release = Deferred.makeUnsafe<void>();
          const holding = yield* Effect.forkChild(root.commit(Deferred.await(release)), {
            startImmediately: true,
          });
          const aborting = yield* Effect.forkChild(harness.harness.abortTask(taskId), {
            startImmediately: true,
          });
          yield* Deferred.succeed(go, undefined);
          const submitting = yield* reached(queued);
          yield* Deferred.succeed(release, undefined);
          yield* joined(holding);
          yield* joined(aborting);
          const exit = yield* awaitBounded(submitting);
          assert.isTrue(Exit.isFailure(exit));
          const childConversation = yield* conversationOf(harness, child);
          assert.deepStrictEqual(yield* allEntries(childConversation), []);
          assert.deepStrictEqual(
            (yield* harness.harness.inspect).submissions.filter(
              (record) => record.conversationId === child,
            ),
            [],
          );
          yield* harness.close;
        }),
      ),
  );

  it.effect(
    "aborts a subagent run with its parent's conversation, rejecting the waiting tool",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const setup = chatSetup();
          let child: ConversationId | undefined;
          const toolWait = Deferred.makeUnsafe<string>();
          const delegate = AgentTool.make("delegate", {
            description: "Delegates",
            parameters: NoParameters,
            execute: () =>
              Effect.gen(function* () {
                const api = yield* ToolContext;
                const id = yield* createSubagent(api);
                child = id;
                const submission = yield* Option.getOrThrow(yield* api.conversation(id)).submit({
                  type: "input",
                  content: "child task",
                });
                // The abort interrupts the wait, which records its cause and propagates.
                const settledChild = yield* submission.await.pipe(
                  Effect.onError((cause) => Deferred.succeed(toolWait, Cause.pretty(cause))),
                );
                return textOutput(settledChild.status);
              }),
          });
          const childRun = unanswered();
          setup.faux.setResponses([toolCall("delegate", {}, "c1"), childRun.step]);
          const { harness, root } = yield* openChat(yield* controlledStorage, setup, {
            extensions: tools(delegate),
          });
          const input = yield* root.submit({ type: "input", content: "go" });
          yield* reached(childRun.reached);
          yield* finished(root.abort());
          const settledInput = yield* finished(input.await);
          assert.strictEqual(settledInput.status, "unanswered");
          assert.strictEqual(reasonOf(settledInput), "aborted");
          assert.isNotEmpty(yield* reached(toolWait));
          if (child === undefined) return assert.fail("the tool did not run");
          const childId = child;
          const childConversation = yield* conversationOf(harness, childId);
          yield* finished(childConversation.awaitIdle);
          // The child's run failed as aborted.
          const childLive = Option.getOrUndefined(
            yield* harness.session.snapshot(LiveDoc, childId),
          );
          assert.deepStrictEqual(Object.keys(childLive ?? {}), ["runFailure"]);
          assert.strictEqual(childLive?.runFailure?.reason, "aborted");
          const childInputs = (yield* harness.harness.inspect).submissions.filter(
            (record) => record.conversationId === childId,
          );
          assert.deepStrictEqual(childInputs, []);
          yield* finished(harness.harness.awaitIdle);
          yield* harness.close;
        }),
      ),
  );

  it.effect("lets a running tool abort its owned child and continue", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        const childRun = unanswered();
        const delegate = AgentTool.make("delegate", {
          description: "Delegates and cancels",
          parameters: NoParameters,
          execute: () =>
            Effect.gen(function* () {
              const api = yield* ToolContext;
              const child = yield* createSubagent(api);
              const handle = Option.getOrThrow(yield* api.conversation(child));
              const submission = yield* handle.submit({ type: "input", content: "child task" });
              yield* Deferred.await(childRun.reached);
              yield* handle.abort();
              const settledChild = yield* submission.await;
              return textOutput(`child ${settledChild.status}`);
            }),
        });
        setup.faux.setResponses([
          toolCall("delegate", {}, "c1"),
          childRun.step,
          fauxAssistantMessage([fauxText("done")]),
        ]);
        const { harness, root } = yield* openChat(yield* controlledStorage, setup, {
          extensions: tools(delegate),
        });
        const settledGo = yield* finished(
          root.submit({ type: "input", content: "go" }).pipe(Effect.flatMap((s) => s.await)),
        );
        assert.strictEqual(settledGo.status, "done");
        yield* harness.close;
      }),
    ),
  );

  it.effect(
    "aborts the children of a tool call that throws or is interrupted, while the run continues",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const storage = yield* controlledStorage;
          const children: Array<TaskId> = [];
          const toolRunning = Deferred.makeUnsafe<void>();
          const setup = chatSetup();
          const spawn = AgentTool.make("spawn", {
            description: "Starts owned work, then throws or hangs",
            parameters: Schema.Struct({ mode: Schema.String }),
            execute: (args) =>
              Effect.gen(function* () {
                const api = yield* ToolContext;
                const child = yield* api.commit(
                  Effect.gen(function* () {
                    const tx = yield* Tx;
                    const created = yield* tx.createConversation({
                      ownership: { kind: "task", taskId: api.taskId },
                    });
                    return yield* tx.createTask(
                      Hold,
                      { name: `child.${api.callId}` },
                      { ownership: { kind: "conversation" }, conversationId: created.id },
                    );
                  }),
                );
                children.push(child);
                if (args.mode === "throw") return yield* Effect.die(new Error("spawn failed"));
                yield* Deferred.succeed(toolRunning, undefined);
                return yield* Effect.never;
              }),
          });
          const extensions = [
            Extension.make({ name: "test.spawn", tasks: [Hold], tools: [spawn] }),
          ];
          const call = (mode: string, id: string) => toolCall("spawn", { mode }, id);
          setup.faux.setResponses([
            call("throw", "c1"),
            fauxAssistantMessage([fauxText("after throw")]),
            call("hang", "c2"),
          ]);
          const first = yield* openChat(storage, setup, { extensions });
          const one = yield* finished(
            first.root
              .submit({ type: "input", content: "one" })
              .pipe(Effect.flatMap((s) => s.await)),
          );
          assert.strictEqual(one.status, "done");
          assert.strictEqual(yield* outcomeOf(first.harness, children[0]!), "aborted");

          // The second call hangs until the process stops; on reopen it is interrupted.
          const second = (yield* first.root.submit({ type: "input", content: "two" })).id;
          yield* reached(toolRunning);
          yield* first.harness.close;
          setup.faux.setResponses([fauxAssistantMessage([fauxText("after interrupt")])]);
          const opened = yield* openChat(storage, setup, { extensions });
          yield* opened.harness.harness.resume;
          assert.strictEqual(yield* outcomeOf(opened.harness, children[1]!), "aborted");
          const settledSecond = yield* finished(
            opened.harness.harness
              .submission(second)
              .pipe(Effect.flatMap((found) => Option.getOrThrow(found).await)),
          );
          assert.strictEqual(settledSecond.status, "done");
          const toolTasks = yield* opened.harness.harness.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              return yield* tx.scanTasks({ kind: "core.tool" }, 10);
            }),
          );
          assert.deepStrictEqual(
            toolTasks.items.map(
              (task) => task.state.status === "terminal" && task.state.outcome.status,
            ),
            ["failed", "failed"],
          );
          yield* opened.harness.close;
        }),
      ),
  );

  it.effect(
    "reruns a replay-safe subagent tool after a restart with the same child and submission",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const storage = yield* controlledStorage;
          const children: Array<ConversationId> = [];
          const setup = chatSetup();
          const subagent = AgentTool.make("subagent", {
            description: "Delegates",
            parameters: NoParameters,
            replay: "safe",
            execute: () =>
              Effect.gen(function* () {
                const api = yield* ToolContext;
                const child = yield* api.commit(
                  Effect.gen(function* () {
                    const tx = yield* Tx;
                    const existing = (yield* tx.scanConversations({ ownerTaskId: api.taskId }, 1))
                      .items[0];
                    if (existing !== undefined) return existing.id;
                    const created = yield* tx.createConversation({
                      ownership: { kind: "task", taskId: api.taskId },
                    });
                    yield* configure(created.id, { model: FAUX_MODEL });
                    return created.id;
                  }),
                );
                children.push(child);
                const submission = yield* Option.getOrThrow(yield* api.conversation(child)).submit({
                  type: "input",
                  content: "child task",
                  requestId: `subagent:${api.taskId}`,
                });
                const settledChild = yield* submission.await;
                return textOutput(settledChild.status);
              }),
          });
          const childRun = unanswered();
          setup.faux.setResponses([toolCall("subagent", {}, "c1"), childRun.step]);
          const first = yield* openChat(storage, setup, { extensions: tools(subagent) });
          const input = (yield* first.root.submit({ type: "input", content: "go" })).id;
          yield* reached(childRun.reached);
          yield* first.harness.close;

          setup.faux.setResponses([
            fauxAssistantMessage([fauxText("child answer")]),
            fauxAssistantMessage([fauxText("done")]),
          ]);
          const opened = yield* openChat(storage, setup, { extensions: tools(subagent) });
          const settledInput = yield* finished(
            opened.harness.harness
              .submission(input)
              .pipe(Effect.flatMap((found) => Option.getOrThrow(found).await)),
          );
          assert.strictEqual(settledInput.status, "done");
          assert.lengthOf(children, 2);
          assert.strictEqual(children[1], children[0]);
          const child = yield* conversationOf(opened.harness, children[0]!);
          const entries = yield* allEntries(child);
          assert.lengthOf(
            entries.filter((entry) => entry.kind === "core.user"),
            1,
          );
          const results = (yield* opened.root.context()).messages.filter(
            (message) => message.role === "toolResult",
          );
          assert.deepStrictEqual(
            results.map((message) => message.role === "toolResult" && message.isError),
            [false],
          );
          yield* opened.harness.close;
        }),
      ),
  );

  it.effect("lets a background supervisor resubmit after a restart without submitting twice", () =>
    run(backend)(
      Effect.gen(function* () {
        const storage = yield* controlledStorage;
        const Children = Doc.make({
          checkpointWhen: () => false,
          kind: "test.children",
          version: 1,
          scope: "conversation",
          history: "latest",
          fork: "initial",
          // The document holds plain numbers: a schema's type equals its JSON encoding.
          schema: Schema.Struct({ child: Schema.optionalKey(Schema.Number) }),
          initial: () => ({}),
        });
        const Supervisor = Task.make({
          name: "test.supervisor",
          version: 1,
          input: Schema.Struct({ parent: Schema.Number }),
          checkpoint: Schema.Struct({ phase: Schema.Literal("run") }),
          result: Schema.Struct({ answer: Schema.Number }),
          initial: () => ({ phase: "run" }),
          phases: {
            run: (task, runtime) =>
              Effect.gen(function* () {
                const children = yield* runtime.snapshot(
                  Children,
                  ConversationId.make(task.input.parent),
                );
                const id = Option.getOrThrow(children).child;
                if (id === undefined) return yield* Effect.die(new Error("no child"));
                const child = Option.getOrThrow(
                  yield* runtime.conversation(ConversationId.make(id)),
                );
                const submission = yield* child.submit({
                  type: "input",
                  content: "work",
                  requestId: "stable",
                });
                const settledWork = yield* submission.await;
                if (settledWork.status !== "done" || settledWork.type !== "input") {
                  return yield* Effect.die(new Error(settledWork.status));
                }
                const answer = settledWork.answer;
                yield* runtime.commit(() => Effect.succeed(completed({ answer })));
              }),
          },
          abort: (_task, runtime) => runtime.commit(() => Effect.succeed(aborted)),
        });
        const setup = chatSetup();
        const extensions = [Extension.make({ name: "test.supervisor", tasks: [Supervisor] })];
        const firstRun = unanswered();
        setup.faux.setResponses([firstRun.step, fauxAssistantMessage([fauxText("answer")])]);
        const first = yield* openChat(storage, setup, { extensions });
        const root = first.root;
        const { supervisor, child } = yield* root.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            const supervisor = yield* tx.createTask(
              Supervisor,
              { parent: root.id },
              { ownership: { kind: "conversation" }, background: true },
            );
            const created = yield* tx.createConversation({
              ownership: { kind: "task", taskId: supervisor },
            });
            yield* configure(created.id, { model: FAUX_MODEL });
            (yield* tx.doc(Children, root.id)).child = created.id;
            return { supervisor, child: created.id };
          }),
        );
        yield* first.harness.harness.resume;
        // The submission is admitted and its generation requested; then the process stops.
        yield* reached(firstRun.reached);
        yield* first.harness.close;

        const opened = yield* openChat(storage, setup, { extensions });
        yield* opened.harness.harness.resume;
        const done = yield* finished(opened.harness.harness.awaitTask(supervisor));
        assert.strictEqual(done.state.outcome.status, "completed");
        const childConversation = yield* conversationOf(opened.harness, child);
        const entries = yield* allEntries(childConversation);
        assert.lengthOf(
          entries.filter((entry) => entry.kind === "core.user"),
          1,
        );
        // The root never waited for the background supervisor.
        yield* finished(opened.root.awaitIdle);
        yield* opened.harness.close;
      }),
    ),
  );
});

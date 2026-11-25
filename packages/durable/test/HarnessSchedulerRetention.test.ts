// Scheduler memory is bounded by live work, on every backend: an ended task or a conversation edge is kept only
// while live work is below it, and a chain dropped that way is loaded again from storage when work appears below it.
//
// A reopen is a new Harness over the same `ControlledStorage` once the first one closed. Index sizes are read with
// `h.scheduler.indexSizes()`; read counting and held reads go through `storage.intercept`, submission scans through
// `storage.intercept.scanSubmissions`; a task change is the kernel's `commitWith`. The live document is `core.live`
// (`src/Live.ts`).
import { assert, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as TestClock from "effect/testing/TestClock";
import type { Conversation } from "../src/Harness.ts";
import { ConversationId, type SubmissionId, TaskId } from "../src/Ids.ts";
import type { TaskRecord } from "../src/Records.ts";
import { LiveDoc } from "../src/Live.ts";
import * as Task from "../src/Task.ts";
import { Tx } from "../src/Tx.ts";
import { completed, eventually, openHarness, settled, type TestHarness } from "./HarnessSupport.ts";
import {
  type ControlledStorage,
  controlledStorage,
  describeEachBackend,
  flush,
  run,
} from "./SessionSupport.ts";
import { serialTimeout } from "./Timeouts.ts";

const gates = new Map<string, Deferred.Deferred<void>>();
function gate(name: string): Deferred.Deferred<void> {
  let found = gates.get(name);
  if (found === undefined) {
    found = Deferred.makeUnsafe<void>();
    gates.set(name, found);
  }
  return found;
}
const openGate = (name: string) => Deferred.succeed(gate(name), undefined);

/** How often each named task started its phase. */
const runs = new Map<string, number>();

/** The next state an abort handler commits: `aborted`, with no reason. */
const aborted = { status: "terminal", outcome: { status: "aborted" } } as const;

const HoldPhase = Schema.Struct({ phase: Schema.Literal("hold") });

/**
 * Holds until its gate opens, then completes; its abort handler ends it `aborted`, after the gate `abort.<name>` opens
 * when a case created that gate.
 */
const Hold = Task.make({
  name: "test.retention.hold",
  version: 1,
  input: Schema.Struct({ name: Schema.String }),
  checkpoint: HoldPhase,
  result: Schema.Null,
  initial: () => ({ phase: "hold" }),
  phases: {
    // The abort mark interrupts the handler while it waits for the gate.
    hold: (task, runtime) =>
      Effect.gen(function* () {
        runs.set(task.input.name, (runs.get(task.input.name) ?? 0) + 1);
        yield* Deferred.await(gate(task.input.name));
        yield* runtime.commit(() => Effect.succeed(completed(null)));
      }),
  },
  abort: (task, runtime) =>
    Effect.gen(function* () {
      const held = gates.get(`abort.${task.input.name}`);
      if (held !== undefined) yield* Deferred.await(held);
      yield* runtime.commit(() => Effect.succeed(aborted));
    }),
});

/** Holds until its gate opens, then creates a held task `inside.<name>` in conversation `input.into` and ends. */
const Spawner = Task.make({
  name: "test.retention.spawner",
  version: 1,
  input: Schema.Struct({ name: Schema.String, into: Schema.Number }),
  checkpoint: HoldPhase,
  result: Schema.Null,
  initial: () => ({ phase: "hold" }),
  phases: {
    hold: (task, runtime) =>
      Effect.gen(function* () {
        yield* Deferred.await(gate(task.input.name));
        yield* runtime.commit(() =>
          Effect.gen(function* () {
            const tx = yield* Tx;
            yield* tx.createTask(
              Hold,
              { name: `inside.${task.input.name}` },
              {
                ownership: { kind: "conversation" },
                conversationId: ConversationId.make(task.input.into),
              },
            );
            return completed(null);
          }),
        );
      }),
  },
  abort: (_task, runtime) => runtime.commit(() => Effect.succeed(aborted)),
});

/** Rejected commits it caught. */
const rejections: Array<unknown> = [];

/**
 * Holds until `rejected` opens, then tries one terminal commit that creates a task in conversation `into` and a child of
 * its own, which the Transaction rejects as the task is ending. Catches the rejection and holds.
 */
const Rejected = Task.make({
  name: "test.retention.rejected",
  version: 1,
  input: Schema.Struct({ into: Schema.Number }),
  checkpoint: HoldPhase,
  result: Schema.Null,
  initial: () => ({ phase: "hold" }),
  phases: {
    hold: (task, runtime) =>
      Effect.gen(function* () {
        yield* Deferred.await(gate("rejected"));
        yield* runtime
          .commit(() =>
            Effect.gen(function* () {
              const tx = yield* Tx;
              yield* tx.createTask(
                Hold,
                { name: "staged" },
                {
                  ownership: { kind: "conversation" },
                  conversationId: ConversationId.make(task.input.into),
                },
              );
              yield* tx.createTask(
                Hold,
                { name: "child" },
                { ownership: { kind: "task", taskId: task.id } },
              );
              return completed(null);
            }),
          )
          .pipe(
            Effect.catch((error) =>
              Effect.sync(() => {
                rejections.push(error);
              }),
            ),
          );
        // The handler holds until it is interrupted.
        return yield* Effect.never;
      }),
  },
  abort: (_task, runtime) => runtime.commit(() => Effect.succeed(aborted)),
});

/** Waits on the tasks in its input, then completes. */
const Waiter = Task.make({
  name: "test.retention.waiter",
  version: 1,
  // Task IDs are plain numbers in the input: a JSON codec's type is its encoding.
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

const sizes = (h: TestHarness) => h.scheduler.indexSizes();

// Each case starts from empty state.
const reset = Effect.sync(() => {
  gates.clear();
  runs.clear();
  rejections.length = 0;
});

/** A resumed Harness over `storage` (a fresh one by default) with the root handle. */
const open = (storage?: ControlledStorage) =>
  Effect.gen(function* () {
    const opened = yield* openHarness(storage ?? (yield* controlledStorage), [
      Hold,
      Spawner,
      Rejected,
    ]);
    const root = yield* opened.harness.ensureRoot();
    yield* opened.harness.resume;
    return { ...opened, root };
  });

const status = (h: TestHarness, id: TaskId) =>
  h.harness.getTask(id).pipe(Effect.map((found) => Option.getOrThrow(found).state));

/** Under live `parent`: tool task T owning conversation C, a task in C; all of them end. Returns C. */
const subagentCycle = (h: TestHarness, root: Conversation, parent: TaskId, n: number) =>
  Effect.gen(function* () {
    const { tool, inner, child } = yield* root.commit(
      Effect.gen(function* () {
        const tx = yield* Tx;
        const tool = yield* tx.createTask(
          Hold,
          { name: `tool${n}` },
          { ownership: { kind: "task", taskId: parent } },
        );
        const child = yield* tx.createConversation({ ownership: { kind: "task", taskId: tool } });
        const inner = yield* tx.createTask(
          Hold,
          { name: `inner${n}` },
          { ownership: { kind: "conversation" }, conversationId: child.id },
        );
        return { tool, inner, child: child.id };
      }),
    );
    yield* openGate(`inner${n}`);
    yield* openGate(`tool${n}`);
    yield* h.harness.awaitTask(inner);
    yield* h.harness.awaitTask(tool);
    return child;
  });

/**
 * Replaces task `id`'s record through the kernel's `commitWith`. `conversationId` binds the commit to that
 * conversation, as a commit through its handle does; without it the commit is bound to none.
 */
const replaceTask = (
  h: TestHarness,
  id: TaskId,
  change: (record: TaskRecord) => TaskRecord,
  conversationId?: ConversationId,
) =>
  h.kernel.commitWith(
    (tx) =>
      Effect.gen(function* () {
        yield* tx.setTask(change(Option.getOrThrow(yield* tx.service.task(id))));
      }),
    conversationId === undefined ? undefined : { conversationId },
  );

/** The committed record of submission `id`. */
const submissionStatus = (h: TestHarness, id: SubmissionId) =>
  h.harness.submission(id).pipe(Effect.flatMap((found) => Option.getOrThrow(found).status));

/** Marks `conversation` busy in `core.live` with a run answered by `taskId`, so an input queues instead of starting a run. */
const markBusy = (conversation: Conversation, taskId: TaskId) =>
  conversation.commit(
    Effect.gen(function* () {
      const tx = yield* Tx;
      (yield* tx.doc(LiveDoc, conversation.id)).run = { taskId, inputs: [], turns: 0 };
    }),
  );

describeEachBackend("scheduler retention", (backend) => {
  it.effect("forgets ended subagent chains once their work ends", () =>
    run(backend)(
      Effect.gen(function* () {
        yield* reset;
        const h = yield* open();
        const parent = yield* h.root.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            return yield* tx.createTask(
              Hold,
              { name: "parent" },
              { ownership: { kind: "conversation" } },
            );
          }),
        );
        yield* subagentCycle(h, h.root, parent, 0);
        const baseline = sizes(h);
        for (let n = 1; n <= 200; n++) yield* subagentCycle(h, h.root, parent, n);
        assert.deepStrictEqual(sizes(h), baseline);
        yield* openGate("parent");
        yield* h.harness.awaitTask(parent);
        yield* h.close;
      }),
    ),
  );

  it.effect(
    "holds a live owner for work later created in a forgotten conversation, loading its chain again",
    () =>
      run(backend)(
        Effect.gen(function* () {
          yield* reset;
          const h = yield* open();
          // Live P -> ended B -> ended A -> conversation C, empty: forgotten.
          const tree = yield* h.root.commit(
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
              return { p, b, a, c: c.id };
            }),
          );
          yield* openGate("a");
          yield* openGate("b");
          yield* h.harness.awaitTask(tree.b);
          const before = sizes(h);
          const c = Option.getOrThrow(yield* h.harness.conversation(tree.c));
          const d = yield* c.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              return yield* tx.createTask(
                Hold,
                { name: "d" },
                { ownership: { kind: "conversation" } },
              );
            }),
          );
          yield* openGate("p");
          // P holds for D: the scheduler loaded the chain from C up to P again.
          yield* eventually(
            status(h, tree.p).pipe(Effect.map((state) => state.status === "completing")),
          );
          yield* openGate("d");
          assert.strictEqual((yield* h.harness.awaitTask(d)).state.outcome.status, "completed");
          assert.strictEqual(
            (yield* h.harness.awaitTask(tree.p)).state.outcome.status,
            "completed",
          );
          assert.isAtMost(sizes(h).settled, before.settled);
          yield* h.close;
        }),
      ),
  );

  it.effect("holds an owner for work its own terminal commit creates below a forgotten chain", () =>
    run(backend)(
      Effect.gen(function* () {
        yield* reset;
        const h = yield* open();
        // Live P (a spawner) -> ended B -> ended A -> conversation C, empty: forgotten. P's terminal commit creates D
        // in C.
        const tree = yield* h.root.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            const p = yield* tx.createTask(
              Spawner,
              { name: "p", into: 0 },
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
            return { p, b, c: c.id };
          }),
        );
        // Point the spawner at C, now that its ID is known.
        // Bound to the root conversation.
        yield* replaceTask(
          h,
          tree.p,
          (record) => ({ ...record, input: { name: "p", into: tree.c } }),
          h.root.id,
        );
        yield* openGate("a");
        yield* openGate("b");
        yield* h.harness.awaitTask(tree.b);
        yield* openGate("p");
        yield* eventually(
          status(h, tree.p).pipe(Effect.map((state) => state.status !== "running")),
        );
        assert.strictEqual((yield* status(h, tree.p)).status, "completing");
        yield* openGate("inside.p");
        assert.strictEqual((yield* h.harness.awaitTask(tree.p)).state.outcome.status, "completed");
        yield* h.close;
      }),
    ),
  );

  it.effect(
    "withdraws an input queued in a forgotten conversation below an abort-marked owner",
    () =>
      run(backend)(
        Effect.gen(function* () {
          yield* reset;
          const h = yield* open();
          // Live P -> ended B -> conversation C with no task; C gets a queued input while P is cancelling.
          const tree = yield* h.root.commit(
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
              // A live child keeps P holding its abort until the input is withdrawn and the child ends.
              const keep = yield* tx.createTask(
                Hold,
                { name: "keep" },
                { ownership: { kind: "task", taskId: p } },
              );
              return { p, b, c: c.id, keep };
            }),
          );
          yield* openGate("b");
          yield* h.harness.awaitTask(tree.b);
          const c = Option.getOrThrow(yield* h.harness.conversation(tree.c));
          // Busy, so the input queues instead of starting a run.
          yield* markBusy(c, tree.keep);
          const queued = (yield* c.submit({ type: "input", content: "later" })).id;
          yield* h.harness.abortTask(tree.p);
          yield* eventually(
            submissionStatus(h, queued).pipe(
              Effect.map((record) => record.status === "unanswered"),
            ),
          );
          assert.deepInclude(yield* submissionStatus(h, queued), { reason: "aborted" });
          yield* h.harness.awaitTask(tree.p);
          yield* h.close;
        }),
      ),
  );

  it.effect(
    "aborts work created in a forgotten conversation below an abort-marked owner before it runs",
    () =>
      run(backend)(
        Effect.gen(function* () {
          yield* reset;
          const h = yield* open();
          const tree = yield* h.root.commit(
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
              const keep = yield* tx.createTask(
                Hold,
                { name: "keep" },
                { ownership: { kind: "task", taskId: p } },
              );
              return { p, b, c: c.id, keep };
            }),
          );
          yield* openGate("b");
          yield* h.harness.awaitTask(tree.b);
          // The scheduler may end keep, and then P, before the check runs, and a terminal P cascades nothing.
          // Holding keep's abort handler keeps P holding, as the case intends.
          gate("abort.keep");
          // P cancels; its live child keeps it holding. Not bound to a conversation.
          yield* replaceTask(h, tree.p, (record) => ({ ...record, abortRequested: true }));
          yield* eventually(
            h.harness
              .getTask(tree.keep)
              .pipe(Effect.map((found) => Option.getOrThrow(found).abortRequested)),
          );
          const c = Option.getOrThrow(yield* h.harness.conversation(tree.c));
          const late = yield* c.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              return yield* tx.createTask(
                Hold,
                { name: "late" },
                { ownership: { kind: "conversation" } },
              );
            }),
          );
          assert.strictEqual((yield* h.harness.awaitTask(late)).state.outcome.status, "aborted");
          assert.strictEqual(runs.get("late") ?? 0, 0);
          yield* h.close;
        }),
      ),
  );

  it.effect(
    "loads the chain of new work below a forgotten conversation before admitting it, and keeps the scope busy",
    () =>
      run(backend)(
        Effect.gen(function* () {
          yield* reset;
          const storage = yield* controlledStorage;
          const h = yield* open(storage);
          const tree = yield* h.root.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              const b = yield* tx.createTask(
                Hold,
                { name: "b" },
                { ownership: { kind: "conversation" } },
              );
              const c = yield* tx.createConversation({ ownership: { kind: "task", taskId: b } });
              return { b, c: c.id };
            }),
          );
          yield* openGate("b");
          yield* h.harness.awaitTask(tree.b);
          yield* h.close;
          // Reopen, so that B is in no cache: the chain load reads it from storage. Hold that read.
          const held = yield* Deferred.make<void>();
          const reached = yield* Deferred.make<void>();
          let holding = false;
          storage.intercept.task = (id, next) =>
            Effect.suspend(() => {
              if (holding && id === tree.b) {
                return Deferred.succeed(reached, undefined).pipe(
                  Effect.andThen(Deferred.await(held)),
                  Effect.andThen(next),
                );
              }
              return next;
            });
          const second = yield* open(storage);
          const c = Option.getOrThrow(yield* second.harness.conversation(tree.c));
          holding = true;
          const committing = yield* Effect.forkChild(
            c.commit(
              Effect.gen(function* () {
                const tx = yield* Tx;
                return yield* tx.createTask(
                  Hold,
                  { name: "d" },
                  { ownership: { kind: "conversation" } },
                );
              }),
            ),
          );
          // Its admission loads the chain (the scheduler's `admitting`): the commit waits for the read.
          yield* Deferred.await(reached);
          assert.isFalse(yield* settled(committing));
          yield* Deferred.succeed(held, undefined);
          yield* Fiber.join(committing);
          const idle = yield* Effect.forkChild(second.harness.awaitIdle);
          assert.isFalse(yield* settled(idle));
          yield* openGate("d");
          yield* second.harness.awaitIdle;
          yield* second.close;
        }),
      ),
  );

  it.effect("keeps a chain when one commit ends its last task and creates new work below it", () =>
    run(backend)(
      Effect.gen(function* () {
        yield* reset;
        const h = yield* open();
        // Live P -> ended B -> conversation C with background task `old`, which keeps the chain known without holding
        // B.
        const tree = yield* h.root.commit(
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
            const old = yield* tx.createTask(
              Hold,
              { name: "old" },
              { ownership: { kind: "conversation" }, conversationId: c.id, background: true },
            );
            return { p, b, c: c.id, old };
          }),
        );
        yield* openGate("b");
        yield* h.harness.awaitTask(tree.b);
        // One commit: the old task ends, which leaves nothing live below B, and a replacement starts in C.
        const replacement = yield* h.kernel.commitWith((tx) =>
          Effect.gen(function* () {
            const old = Option.getOrThrow(yield* tx.service.task(tree.old));
            const id = yield* tx.service.createTask(
              Hold,
              { name: "new" },
              { ownership: { kind: "conversation" }, conversationId: tree.c },
            );
            yield* tx.setTask({
              ...old,
              state: { status: "terminal", outcome: { status: "completed", result: null } },
            });
            return id;
          }),
        );
        // The chain stayed known through the publication, so P holds for the replacement.
        yield* openGate("p");
        yield* eventually(
          status(h, tree.p).pipe(Effect.map((state) => state.status === "completing")),
        );
        yield* openGate("new");
        yield* h.harness.awaitTask(replacement);
        assert.strictEqual((yield* h.harness.awaitTask(tree.p)).state.outcome.status, "completed");
        yield* h.close;
      }),
    ),
  );

  it.effect("drops what a rejected commit's chain load read, without a later publication", () =>
    run(backend)(
      Effect.gen(function* () {
        yield* reset;
        const h = yield* open();
        // Live P owns ended B, which owns conversation C: forgotten. P's terminal commit stages work in C, which loads
        // C's chain, and a child of P, which the Transaction rejects, as P is ending.
        const tree = yield* h.root.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            const p = yield* tx.createTask(
              Rejected,
              { into: 0 },
              { ownership: { kind: "conversation" } },
            );
            const b = yield* tx.createTask(
              Hold,
              { name: "b" },
              { ownership: { kind: "task", taskId: p } },
            );
            const c = yield* tx.createConversation({ ownership: { kind: "task", taskId: b } });
            return { p, b, c: c.id };
          }),
        );
        // Bound to the root conversation.
        yield* replaceTask(
          h,
          tree.p,
          (record) => ({ ...record, input: { into: tree.c } }),
          h.root.id,
        );
        yield* openGate("b");
        yield* h.harness.awaitTask(tree.b);
        const baseline = sizes(h);
        yield* openGate("rejected");
        yield* eventually(Effect.sync(() => rejections.length > 0));
        // The rejected commit published nothing; the sweep job still cleans up what its chain load read.
        yield* eventually(Effect.sync(() => sizes(h).dropQueue === 0));
        assert.deepStrictEqual(sizes(h), baseline);
        yield* h.close;
      }),
    ),
  );

  it.effect("drops what a read loaded after reopen, without a later publication", () =>
    run(backend)(
      Effect.gen(function* () {
        yield* reset;
        const storage = yield* controlledStorage;
        let opened = yield* open(storage);
        // A live task D in conversation C, owned by ended B: after reopen D's chain is not loaded.
        const tree = yield* opened.root.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            const b = yield* tx.createTask(
              Hold,
              { name: "b" },
              { ownership: { kind: "conversation" } },
            );
            const c = yield* tx.createConversation({ ownership: { kind: "task", taskId: b } });
            const d = yield* tx.createTask(
              Hold,
              { name: "d" },
              { ownership: { kind: "conversation" }, conversationId: c.id },
            );
            return { b, d };
          }),
        );
        yield* openGate("b");
        const first = opened;
        yield* eventually(
          status(first, tree.b).pipe(Effect.map((state) => state.status === "completing")),
        );
        yield* opened.close;
        gates.clear();
        opened = yield* open(storage);
        yield* opened.harness.inspect;
        // D is still live, so its chain stays; once D ends, everything below the root goes, published or not.
        assert.strictEqual(sizes(opened).unloaded, 0);
        yield* openGate("d");
        yield* opened.harness.awaitTask(tree.d);
        yield* opened.harness.awaitTask(tree.b);
        const second = opened;
        yield* eventually(Effect.sync(() => sizes(second).dropQueue === 0));
        assert.deepInclude(sizes(opened), { settled: 0, below: 0, unloaded: 0 });
        yield* opened.close;
      }),
    ),
  );

  it.effect(
    "withdraws an input queued two ended levels below an owner whose intent appears later",
    () =>
      run(backend)(
        Effect.gen(function* () {
          yield* reset;
          const h = yield* open();
          // Live P -> ended B -> ended A -> conversation C with no task, and a live child keeping P holding.
          const tree = yield* h.root.commit(
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
              const keep = yield* tx.createTask(
                Hold,
                { name: "keep" },
                { ownership: { kind: "task", taskId: p } },
              );
              return { p, b, c: c.id, keep };
            }),
          );
          yield* openGate("a");
          yield* openGate("b");
          yield* h.harness.awaitTask(tree.b);
          const c = Option.getOrThrow(yield* h.harness.conversation(tree.c));
          yield* markBusy(c, tree.keep);
          // Queued while nothing cancels: no withdrawal scan.
          const queued = (yield* c.submit({ type: "input", content: "later" })).id;
          yield* eventually(Effect.sync(() => sizes(h).dropQueue === 0));
          // P fails while its child runs: it holds the failed outcome, which is cancellation intent.
          // Not bound to a conversation.
          yield* replaceTask(h, tree.p, (record) => ({
            ...record,
            state: { status: "completing", outcome: { status: "failed", error: { message: "x" } } },
          }));
          yield* eventually(
            submissionStatus(h, queued).pipe(
              Effect.map((record) => record.status === "unanswered"),
            ),
          );
          assert.deepInclude(yield* submissionStatus(h, queued), { reason: "aborted" });
          yield* h.close;
        }),
      ),
  );

  it.effect(
    "forgets a 20,000-deep chain of ended tasks without recursion",
    () =>
      run(backend)(
        Effect.gen(function* () {
          yield* reset;
          const h = yield* open();
          const depth = 20_000;
          const baseline = sizes(h);
          const { links, leaf } = yield* h.root.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              const links: Array<TaskId> = [];
              let owner: TaskId = yield* tx.createTask(
                Hold,
                { name: "link0" },
                { ownership: { kind: "conversation" } },
              );
              links.push(owner);
              for (let n = 1; n < depth; n++) {
                owner = yield* tx.createTask(
                  Hold,
                  { name: `link${n}` },
                  { ownership: { kind: "task", taskId: owner } },
                );
                links.push(owner);
              }
              const leaf = yield* tx.createTask(
                Hold,
                { name: "leaf" },
                { ownership: { kind: "task", taskId: owner } },
              );
              return { links, leaf };
            }),
          );
          for (let n = 0; n < depth; n++) yield* openGate(`link${n}`);
          yield* openGate("leaf");
          yield* h.harness.awaitTask(leaf);
          yield* h.harness.awaitTask(links[0]!);
          yield* h.harness.awaitIdle;
          assert.deepStrictEqual(sizes(h), baseline);
          yield* h.close;
        }),
      ),
    serialTimeout(23_487),
  );

  it.effect("keeps no more after all work ended than a fresh reopen", () =>
    run(backend)(
      Effect.gen(function* () {
        yield* reset;
        const storage = yield* controlledStorage;
        let opened = yield* open(storage);
        const parent = yield* opened.root.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            return yield* tx.createTask(
              Hold,
              { name: "parent" },
              { ownership: { kind: "conversation" } },
            );
          }),
        );
        for (let n = 0; n < 20; n++) yield* subagentCycle(opened, opened.root, parent, n);
        yield* openGate("parent");
        yield* opened.harness.awaitTask(parent);
        yield* opened.harness.awaitIdle;
        const ended = sizes(opened);
        yield* opened.close;
        opened = yield* open(storage);
        yield* opened.harness.awaitIdle;
        assert.deepStrictEqual(ended, sizes(opened));
        yield* opened.close;
      }),
    ),
  );

  it.effect("does not scan queued submissions for an input when nothing cancels", () =>
    run(backend)(
      Effect.gen(function* () {
        yield* reset;
        const storage = yield* controlledStorage;
        let scans = 0;
        storage.intercept.scanSubmissions = ([query], next) =>
          Effect.suspend(() => {
            if (query.status === "queued") scans++;
            return next;
          });
        const h = yield* open(storage);
        const busy = yield* h.root.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            return yield* tx.createTask(
              Hold,
              { name: "busy" },
              { ownership: { kind: "conversation" } },
            );
          }),
        );
        yield* markBusy(h.root, busy);
        // The reconcile the open schedules scans queued submissions once, and must run before `before` is read. A
        // fiber yields only when it waits, and the setup commits over this storage do not, so the setup gives
        // pending fibers their turns here.
        yield* flush;
        const before = scans;
        yield* h.root.submit({ type: "input", content: "queued" });
        // Let any reconcile it would cause run: pending fibers get their turns, and the clock moves 20 ms.
        yield* flush;
        yield* TestClock.adjust("20 millis");
        yield* flush;
        assert.strictEqual(scans, before);
        yield* openGate("busy");
        yield* h.close;
      }),
    ),
  );

  // A wait on an ended task closes no cycle (#10411), also when the ended task owns the waiter's conversation and
  // the scheduler forgot it: the wait resumes at once.
  it.effect("lets a task wait on a forgotten ended owner", () =>
    run(backend)(
      Effect.gen(function* () {
        yield* reset;
        const opened = yield* openHarness(yield* controlledStorage, [Hold, Waiter]);
        const root = yield* opened.harness.ensureRoot();
        yield* opened.harness.resume;
        const tree = yield* root.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            const b = yield* tx.createTask(
              Hold,
              { name: "b" },
              { ownership: { kind: "conversation" } },
            );
            const c = yield* tx.createConversation({ ownership: { kind: "task", taskId: b } });
            return { b, c: c.id };
          }),
        );
        yield* openGate("b");
        yield* opened.harness.awaitTask(tree.b);
        const c = Option.getOrThrow(yield* opened.harness.conversation(tree.c));
        const waiter = yield* c.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            return yield* tx.createTask(
              Waiter,
              { on: [tree.b] },
              { ownership: { kind: "conversation" } },
            );
          }),
        );
        const outcome = (yield* opened.harness.awaitTask(waiter)).state.outcome;
        assert.strictEqual(outcome.status, "completed");
        yield* opened.close;
      }),
    ),
  );
});

// Group commit and the scheduler, on every backend: decisions read the scheduler's memory in regions that wait
// for the unpublished commits whose keys they read, memberships written at admission, outcomes, and close.
//
// The abort mark interrupts a handler, and an abort handler that "holds" never returns (close interrupts it).
// Index sizes are read with `h.scheduler.indexSizes()`; a touch is `h.kernel.touch`; a task change is the
// kernel's `commitWith`. A reopen is a new Harness over the same controlled storage.
import { assert, it } from "@effect/vitest";
import * as Cause from "effect/Cause";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import type { Conversation } from "../src/Harness.ts";
import type { ConversationId, TaskId } from "../src/Ids.ts";
import { ownedKey, ROOTS_KEY } from "../src/internal/keys.ts";
import type { TaskRecord } from "../src/Records.ts";
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

const gates = new Map<string, Deferred.Deferred<void>>();
const gate = (name: string) => {
  let found = gates.get(name);
  if (found === undefined) {
    found = Deferred.makeUnsafe<void>();
    gates.set(name, found);
  }
  return found;
};
const open = (name: string) => Deferred.succeed(gate(name), undefined);

/** How often each named phase ran. */
const runs = new Map<string, number>();
const ran = (name: string) => runs.get(name) ?? 0;
const count = (name: string) => runs.set(name, ran(name) + 1);

const refusals = new Map<string, unknown>();

// Each case starts from empty state.
const reset = Effect.sync(() => {
  gates.clear();
  runs.clear();
  refusals.clear();
});

/** Any of the tagged errors a Harness operation fails with. */
type Tagged = { readonly _tag: string };

const aborted = { status: "terminal", outcome: { status: "aborted" } } as const;
const Named = Schema.Struct({ name: Schema.String });

/** Holds until its named gate opens, then completes; its abort handler ends it `aborted`. */
const Hold = Task.make({
  name: "gc.hold",
  version: 1,
  input: Named,
  checkpoint: Schema.Struct({ phase: Schema.Literal("hold") }),
  result: Schema.Null,
  initial: () => ({ phase: "hold" }),
  phases: {
    hold: (task, runtime) =>
      Effect.gen(function* () {
        count(task.input.name);
        yield* Deferred.await(gate(task.input.name));
        yield* runtime.commit(() => Effect.succeed(completed(null)));
      }),
  },
  abort: (_task, runtime) => runtime.commit(() => Effect.succeed(aborted)),
});

/** Commits its next phase, returns once `<name>.return` opens; the next phase counts its run and completes. */
const TwoPhases = Task.make({
  name: "gc.two-phases",
  version: 1,
  input: Named,
  checkpoint: Schema.Union([
    Schema.Struct({ phase: Schema.Literal("one") }),
    Schema.Struct({ phase: Schema.Literal("two") }),
  ]),
  result: Schema.Null,
  initial: () => ({ phase: "one" }),
  phases: {
    one: (task, runtime) =>
      Effect.gen(function* () {
        yield* runtime.commit(() =>
          Effect.succeed({ status: "running", checkpoint: { phase: "two" } } as const),
        );
        yield* Deferred.await(gate(`${task.input.name}.return`));
      }),
    two: (task, runtime) =>
      Effect.gen(function* () {
        count(`${task.input.name}.two`);
        yield* runtime.commit(() => Effect.succeed(completed(null)));
      }),
  },
  abort: (_task, runtime) => runtime.commit(() => Effect.succeed(aborted)),
});

/** Once its gate opens, waits on the task in its memo `on`; records why a refused wait was refused, then completes. */
const Waits = Task.make({
  name: "gc.waits",
  version: 1,
  input: Named,
  checkpoint: Schema.Union([
    Schema.Struct({ phase: Schema.Literal("wait") }),
    Schema.Struct({ phase: Schema.Literal("done") }),
  ]),
  result: Schema.Null,
  initial: () => ({ phase: "wait" }),
  phases: {
    wait: (task, runtime) =>
      Effect.gen(function* () {
        yield* Deferred.await(gate(task.input.name));
        // SAFETY: the case names the task to wait on in the memo `on`, a task ID.
        const on = Option.getOrThrow(yield* runtime.memo("on")) as TaskId;
        yield* runtime
          .commit(() =>
            Effect.succeed({
              status: "waiting",
              checkpoint: { phase: "done" },
              on: [on],
              policy: "allSettled",
            } as const),
          )
          .pipe(
            Effect.catchTag("TaskError", (error) =>
              Effect.sync(() => refusals.set(task.input.name, error)).pipe(
                Effect.andThen(runtime.commit(() => Effect.succeed(completed(null)))),
              ),
            ),
          );
      }),
    done: (_task, runtime) => runtime.commit(() => Effect.succeed(completed(null))),
  },
  abort: (_task, runtime) => runtime.commit(() => Effect.succeed(aborted)),
});

/** A Harness running `tasks` over a controlled storage, with its root. */
const openWith = (storage: ControlledStorage, tasks: ReadonlyArray<Task.Any>) =>
  Effect.gen(function* () {
    const h = yield* openHarness(storage, tasks);
    const root = yield* h.harness.ensureRoot();
    return { h, root };
  });

const statusOf = (h: TestHarness, id: TaskId) =>
  h.harness.getTask(id).pipe(Effect.map((found) => Option.getOrUndefined(found)?.state.status));

const statusIs = (h: TestHarness, id: TaskId, status: string) =>
  eventually(statusOf(h, id).pipe(Effect.map((current) => current === status)));

/** Stages a task record as is, the way only the scheduler and recovery write them. */
const setTask = (
  h: TestHarness,
  change: (tx: Tx["Service"]) => Effect.Effect<TaskRecord, Tagged, Tx>,
) =>
  h.kernel.commitWith((tx) =>
    Effect.gen(function* () {
      yield* tx.setTask(yield* change(tx.service));
    }),
  );

/** The handle of conversation `id`, obtained before a test holds a call. */
const handleOf = (h: TestHarness, id: ConversationId) =>
  Effect.map(h.harness.conversation(id), Option.getOrThrow);

/** Creates a held task named `name` in `conversation`. */
const createIn = (conversation: Conversation, name: string) =>
  conversation.commit(
    Effect.gen(function* () {
      const tx = yield* Tx;
      return yield* tx.createTask(Hold, { name }, { ownership: { kind: "conversation" } });
    }),
  );

const fork = <A, E>(effect: Effect.Effect<A, E>) =>
  Effect.forkChild(effect, { startImmediately: true });

/** The checkpoint of task `id`, when its state has one. */
const checkpointOf = (h: TestHarness, id: TaskId) =>
  h.harness.getTask(id).pipe(
    Effect.map((found) => {
      const state = Option.getOrUndefined(found)?.state;
      return state !== undefined && "checkpoint" in state ? state.checkpoint : undefined;
    }),
  );

/** The IDs of the tasks every publication from now on carries. */
const followTasks = (h: TestHarness, filter: (record: TaskRecord) => boolean = () => true) =>
  Effect.gen(function* () {
    const seen = new Set<TaskId>();
    yield* h.kernel.followCommits((publication) => {
      for (const change of publication.changes) {
        if (change.type === "task" && filter(change.value)) seen.add(change.value.id);
      }
    });
    return seen;
  });

describeEachBackend("group commit: scheduler regions", (backend) => {
  it.effect(
    "holds a live owner whose end races a creation below its ended tasks: live P, ended B and A, empty C",
    () =>
      run(backend)(
        Effect.gen(function* () {
          yield* reset;
          const storage = yield* controlledStorage;
          const { h, root } = yield* openWith(storage, [Hold]);
          yield* h.harness.resume;
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
              return { p, b, a, c: c.id };
            }),
          );
          yield* open("a");
          yield* h.harness.awaitTask(tree.a);
          yield* open("b");
          yield* h.harness.awaitTask(tree.b);
          // The creation of D in C is admitted and in flight when P commits its end.
          const c = yield* handleOf(h, tree.c);
          const held = yield* storage.holdCommits;
          const creating = yield* fork(createIn(c, "d"));
          yield* held.entered;
          // P's decision reads what the creation writes, so it waits for its publication, holding the line.
          yield* open("p");
          yield* flush;
          yield* held.release;
          const d = yield* Fiber.join(creating);
          yield* statusIs(h, tree.p, "completing");
          yield* open("d");
          yield* h.harness.awaitTask(d);
          assert.strictEqual(
            (yield* h.harness.awaitTask(tree.p)).state.outcome.status,
            "completed",
          );
          yield* h.close;
        }),
      ),
  );

  it.effect(
    "holds an owner whose end follows a creation below its child's pending end: live P, ending B, conversation C",
    () =>
      run(backend)(
        Effect.gen(function* () {
          yield* reset;
          const storage = yield* controlledStorage;
          const { h, root } = yield* openWith(storage, [Hold]);
          yield* h.harness.resume;
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
              return { p, b, c: c.id };
            }),
          );
          const c = yield* handleOf(h, tree.c);
          yield* statusIs(h, tree.b, "running");
          // A call in flight, so what follows is admitted and stays unpublished, in this order.
          const held = yield* storage.holdCommits;
          const blocker = yield* fork(
            root.commit(
              Effect.gen(function* () {
                const tx = yield* Tx;
                yield* tx.appendEntry(root.id, { kind: "note" });
              }),
            ),
          );
          yield* held.entered;
          // B ends; then D is created in C while B still shows live; then P ends.
          yield* open("b");
          yield* flush;
          const creating = yield* fork(createIn(c, "d"));
          yield* flush;
          yield* open("p");
          yield* flush;
          yield* held.release;
          yield* Fiber.join(blocker);
          const d = yield* Fiber.join(creating);
          // In line order D's creation precedes P's decision: P holds while D is live.
          yield* statusIs(h, tree.p, "completing");
          assert.strictEqual(yield* statusOf(h, d), "running");
          yield* open("d");
          assert.strictEqual(
            (yield* h.harness.awaitTask(tree.p)).state.outcome.status,
            "completed",
          );
          yield* h.close;
        }),
      ),
  );

  it.effect(
    "ends a child before its next phase when its owner's abort mark is unpublished at its step",
    () =>
      run(backend)(
        Effect.gen(function* () {
          yield* reset;
          const storage = yield* controlledStorage;
          const { h, root } = yield* openWith(storage, [Hold, TwoPhases]);
          yield* h.harness.resume;
          const { owner, child } = yield* root.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              const owner = yield* tx.createTask(
                Hold,
                { name: "o" },
                { ownership: { kind: "conversation" } },
              );
              const child = yield* tx.createTask(
                TwoPhases,
                { name: "t" },
                { ownership: { kind: "task", taskId: owner } },
              );
              return { owner, child };
            }),
          );
          yield* eventually(
            h.harness.getTask(child).pipe(
              Effect.map((found) => {
                const state = Option.getOrUndefined(found)?.state;
                return (
                  state?.status === "running" &&
                  // SAFETY: a TwoPhases checkpoint has a `phase`.
                  (state.checkpoint as { phase: string }).phase === "two"
                );
              }),
            ),
          );
          const held = yield* storage.holdCommits;
          const marking = yield* fork(h.harness.abortTask(owner));
          yield* held.entered;
          // The child's phase returns: its step reads the intent set, which the unpublished mark changes.
          yield* open("t.return");
          yield* flush;
          assert.strictEqual(ran("t.two"), 0);
          yield* held.release;
          assert.strictEqual((yield* Fiber.join(marking))._tag, "Marked");
          assert.strictEqual((yield* h.harness.awaitTask(child)).state.outcome.status, "aborted");
          assert.strictEqual((yield* h.harness.awaitTask(owner)).state.outcome.status, "aborted");
          assert.strictEqual(ran("t.two"), 0);
          yield* h.close;
        }),
      ),
  );

  it.effect("refuses the second of two opposite waits admitted behind each other", () =>
    run(backend)(
      Effect.gen(function* () {
        yield* reset;
        const storage = yield* controlledStorage;
        const { h, root } = yield* openWith(storage, [Waits]);
        const { x, y } = yield* root.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            const x = yield* tx.createTask(
              Waits,
              { name: "x" },
              { ownership: { kind: "conversation" } },
            );
            const y = yield* tx.createTask(
              Waits,
              { name: "y" },
              { ownership: { kind: "conversation" } },
            );
            return { x, y };
          }),
        );
        // Each waits on the other, named in a memo.
        yield* h.kernel.commitWith((tx) =>
          Effect.gen(function* () {
            // Every table read before the first write.
            const recordX = Option.getOrThrow(yield* tx.service.task(x));
            const recordY = Option.getOrThrow(yield* tx.service.task(y));
            yield* tx.setTask({ ...recordX, memos: { on: y } });
            yield* tx.setTask({ ...recordY, memos: { on: x } });
          }),
        );
        yield* h.harness.resume;
        yield* statusIs(h, x, "running");
        yield* statusIs(h, y, "running");
        const held = yield* storage.holdCommits;
        yield* open("x");
        yield* held.entered;
        yield* open("y");
        yield* flush;
        yield* held.release;
        yield* h.harness.awaitTask(y);
        assert.include(String(refusals.get("y")), "wait cycle");
        assert.isFalse(refusals.has("x"));
        assert.strictEqual((yield* h.harness.awaitTask(x)).state.outcome.status, "completed");
        yield* h.close;
      }),
    ),
  );

  it.effect("starts no invocation on a reservation before the reservation is published", () =>
    run(backend)(
      Effect.gen(function* () {
        yield* reset;
        const storage = yield* controlledStorage;
        const { h, root } = yield* openWith(storage, [Hold]);
        const id = yield* root.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            return yield* tx.createTask(
              Hold,
              { name: "r" },
              { ownership: { kind: "conversation" } },
            );
          }),
        );
        // The reservation's write is in flight: a scheduler commit with writes is never delivered before it is
        // published.
        const held = yield* storage.holdCommits;
        yield* h.harness.resume;
        yield* held.entered;
        yield* flush;
        assert.strictEqual(ran("r"), 0);
        yield* held.release;
        yield* eventually(Effect.sync(() => ran("r") === 1));
        assert.strictEqual(yield* statusOf(h, id), "running");
        yield* open("r");
        yield* h.harness.awaitTask(id);
        yield* h.close;
      }),
    ),
  );

  /** A task of version 2 whose `migrate` counts its calls and throws when `fails`. */
  const migrated = (name: string, migrations: { count: number }, fails: boolean) =>
    Task.make({
      name,
      version: 2,
      input: Named,
      checkpoint: Schema.Struct({ phase: Schema.Literal("hold") }),
      result: Schema.Null,
      initial: () => ({ phase: "hold" }),
      migrate: (input, checkpoint) => {
        migrations.count++;
        if (fails) throw new Error("cannot migrate");
        // SAFETY: the stored input and checkpoint have the current shapes; only the version moved.
        return {
          input: input as { readonly name: string },
          checkpoint: checkpoint as { readonly phase: "hold" },
        };
      },
      phases: { hold: (_task, runtime) => runtime.commit(() => Effect.succeed(completed(null))) },
      abort: () => Effect.void,
    });

  it.effect(
    "runs a migration once although its reservation waits for an unpublished write of the task",
    () =>
      run(backend)(
        Effect.gen(function* () {
          yield* reset;
          const migrations = { count: 0 };
          const V2 = migrated("gc.migrated", migrations, false);
          const storage = yield* controlledStorage;
          const { h, root } = yield* openWith(storage, [V2]);
          const id = yield* root.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              return yield* tx.createTask(
                V2,
                { name: "m" },
                { ownership: { kind: "conversation" } },
              );
            }),
          );
          // Stored at version 1, and written again by a commit that is in flight when the reservation reads it.
          const held = yield* storage.holdCommits;
          const writing = yield* fork(
            setTask(h, (tx) =>
              Effect.map(tx.task(id), (found) => ({
                ...Option.getOrThrow(found),
                version: 1,
                memos: { touched: true },
              })),
            ),
          );
          yield* held.entered;
          yield* h.harness.resume;
          yield* flush;
          assert.strictEqual(migrations.count, 0);
          yield* held.release;
          yield* Fiber.join(writing);
          assert.strictEqual((yield* h.harness.awaitTask(id)).state.outcome.status, "completed");
          assert.strictEqual(migrations.count, 1);
          yield* h.close;
        }),
      ),
  );

  it.effect(
    "reports a failed migration once although its reservation waits for an unpublished write of the task",
    () =>
      run(backend)(
        Effect.gen(function* () {
          yield* reset;
          const migrations = { count: 0 };
          const V2 = migrated("gc.unmigratable", migrations, true);
          const storage = yield* controlledStorage;
          const { h, root } = yield* openWith(storage, [V2]);
          const id = yield* root.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              return yield* tx.createTask(
                V2,
                { name: "m" },
                { ownership: { kind: "conversation" } },
              );
            }),
          );
          const held = yield* storage.holdCommits;
          const writing = yield* fork(
            setTask(h, (tx) =>
              Effect.map(tx.task(id), (found) => ({ ...Option.getOrThrow(found), version: 1 })),
            ),
          );
          yield* held.entered;
          yield* h.harness.resume;
          yield* flush;
          yield* held.release;
          yield* Fiber.join(writing);
          yield* eventually(Effect.sync(() => migrations.count > 0));
          yield* flush;
          assert.strictEqual(migrations.count, 1);
          const failed = h.reports.filter((report) =>
            String(Cause.squash(report.cause)).includes("cannot migrate"),
          );
          assert.lengthOf(failed, 1);
          yield* h.close;
        }),
      ),
  );

  it.effect(
    "makes a reader of the roots wait for a creation under a root a pending end would unlist",
    () =>
      run(backend)(
        Effect.gen(function* () {
          yield* reset;
          const storage = yield* controlledStorage;
          const { h, root } = yield* openWith(storage, [Hold]);
          yield* h.harness.resume;
          // Root R is listed for its last live task, T.
          const last = yield* createIn(root, "t");
          yield* statusIs(h, last, "running");
          const published = yield* followTasks(h);
          // T's end is in flight; a creation under R is admitted after it, while R still shows listed.
          const held = yield* storage.holdCommits;
          yield* open("t");
          yield* held.entered;
          let created: TaskId | undefined;
          const creating = yield* fork(
            root.commit(
              Effect.gen(function* () {
                const tx = yield* Tx;
                created = yield* tx.createTask(
                  Hold,
                  { name: "d" },
                  { ownership: { kind: "conversation" } },
                );
                return created;
              }),
            ),
          );
          yield* flush;
          // A reader of the roots on the line (global idle's key) waits until the creation is published too.
          const reading = yield* fork(
            h.kernel.readOnLine(
              Effect.gen(function* () {
                const touched = h.kernel.touch(ROOTS_KEY);
                if (touched !== undefined) yield* touched;
                return created !== undefined && published.has(created);
              }),
            ),
          );
          yield* held.release;
          yield* Fiber.join(creating);
          assert.isTrue(yield* Fiber.join(reading));
          yield* open("d");
          yield* h.harness.awaitIdle;
          yield* h.close;
        }),
      ),
  );

  it.effect(
    "walks a creation's memberships past an ancestor whose pending end is unpublished",
    () =>
      run(backend)(
        Effect.gen(function* () {
          yield* reset;
          const storage = yield* controlledStorage;
          const { h, root } = yield* openWith(storage, [Hold]);
          yield* h.harness.resume;
          // Live P, in the root conversation, owns the empty conversation C.
          const { p, c } = yield* root.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              const p = yield* tx.createTask(
                Hold,
                { name: "p" },
                { ownership: { kind: "conversation" } },
              );
              const c = yield* tx.createConversation({ ownership: { kind: "task", taskId: p } });
              return { p, c: c.id };
            }),
          );
          yield* statusIs(h, p, "running");
          const published = yield* followTasks(h);
          // P's end is in flight; a creation in C is admitted after it. Its chain must not stop at P, which shows live.
          const conversation = yield* handleOf(h, c);
          const held = yield* storage.holdCommits;
          yield* open("p");
          yield* held.entered;
          let created: TaskId | undefined;
          const creating = yield* fork(
            Effect.tap(createIn(conversation, "d"), (id) =>
              Effect.sync(() => {
                created = id;
              }),
            ),
          );
          yield* flush;
          const reading = yield* fork(
            h.kernel.readOnLine(
              Effect.gen(function* () {
                const touched = h.kernel.touch(ROOTS_KEY);
                if (touched !== undefined) yield* touched;
                return published.size > 0 && Array.from(published).some((id) => id !== p);
              }),
            ),
          );
          yield* held.release;
          yield* Fiber.join(creating);
          assert.isTrue(yield* Fiber.join(reading));
          assert.isTrue(created !== undefined && published.has(created));
          yield* open("d");
          yield* h.harness.awaitIdle;
          yield* h.close;
        }),
      ),
  );

  it.effect(
    "reserves the live work of the owner above an ancestor whose end is pending, for a creation below it",
    () =>
      run(backend)(
        Effect.gen(function* () {
          yield* reset;
          const storage = yield* controlledStorage;
          const { h, root } = yield* openWith(storage, [Hold]);
          yield* h.harness.resume;
          // Live G owns P, which owns the empty conversation C.
          const { g, p, c } = yield* root.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              const g = yield* tx.createTask(
                Hold,
                { name: "g" },
                { ownership: { kind: "conversation" } },
              );
              const p = yield* tx.createTask(
                Hold,
                { name: "p" },
                { ownership: { kind: "task", taskId: g } },
              );
              const c = yield* tx.createConversation({ ownership: { kind: "task", taskId: p } });
              return { g, p, c: c.id };
            }),
          );
          yield* statusIs(h, p, "running");
          const published = yield* followTasks(h);
          // P's end is in flight; a creation in C is admitted after it, while P still shows live.
          const conversation = yield* handleOf(h, c);
          const held = yield* storage.holdCommits;
          yield* open("p");
          yield* held.entered;
          let created: TaskId | undefined;
          const creating = yield* fork(
            Effect.tap(createIn(conversation, "d"), (id) =>
              Effect.sync(() => {
                created = id;
              }),
            ),
          );
          yield* flush;
          // A reader of G's live work (as G's end decision is) waits until the creation is published too.
          const reading = yield* fork(
            h.kernel.readOnLine(
              Effect.gen(function* () {
                const touched = h.kernel.touch(ownedKey(g));
                if (touched !== undefined) yield* touched;
                return Array.from(published).some((id) => id !== p);
              }),
            ),
          );
          yield* held.release;
          yield* Fiber.join(creating);
          assert.isTrue(yield* Fiber.join(reading));
          assert.isTrue(created !== undefined && published.has(created));
          yield* open("d");
          yield* open("g");
          yield* h.harness.awaitTask(g);
          yield* h.close;
        }),
      ),
  );

  it.effect(
    "keeps the nodes a chain load installed for an unpublished creation through the sweep",
    () =>
      run(backend)(
        Effect.gen(function* () {
          yield* reset;
          const storage = yield* controlledStorage;
          const first = yield* openWith(storage, [Hold]);
          yield* first.h.harness.resume;
          const tree = yield* first.root.commit(
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
          yield* open("b");
          yield* first.h.harness.awaitTask(tree.b);
          yield* first.h.close;
          // After reopen, B and C's edge are known only once a chain load reads them.
          const h = yield* openHarness(storage, [Hold]);
          const sizes = () => h.scheduler.indexSizes();
          assert.strictEqual(sizes().settled, 0);
          const c = yield* handleOf(h, tree.c);
          const held = yield* storage.holdCommits;
          const creating = yield* fork(createIn(c, "d"));
          yield* held.entered;
          // The admission loaded B; the sweep that load scheduled has run on the line since, and kept it.
          yield* h.harness.awaitIdle.pipe(Effect.timeout("1 second"), Effect.ignore);
          yield* flush;
          assert.strictEqual(sizes().settled, 1);
          assert.isAbove(sizes().pins, 0);
          yield* held.release;
          yield* Fiber.join(creating);
          yield* flush;
          // Published below a known chain: nothing is left to load, and no pin is left.
          assert.strictEqual(sizes().unloaded, 0);
          assert.strictEqual(sizes().pins, 0);
          const idle = yield* fork(h.harness.awaitIdle);
          assert.isFalse(yield* settled(idle));
          yield* h.harness.resume;
          yield* open("d");
          yield* Fiber.join(idle);
          yield* h.close;
        }),
      ),
  );
});

describeEachBackend("group commit: records a decision copies", (backend) => {
  const ProbeState = Schema.Struct({ phase: Schema.Literal("hold"), n: Schema.Number });
  /** Runtime commits of running probes, by name, so a test commits for them. */
  const advances = new Map<
    string,
    (n: number, prepared: () => void) => Effect.Effect<void, Tagged>
  >();
  const probeIds = new Map<string, TaskId>();

  /** Holds until abort-marked, its runtime exposed; fails once its gate opens. Its abort handler holds too. */
  const Probe = Task.make({
    name: "gc.probe",
    version: 1,
    input: Named,
    checkpoint: ProbeState,
    result: Schema.Null,
    initial: () => ({ phase: "hold", n: 0 }),
    phases: {
      hold: (task, runtime) =>
        Effect.gen(function* () {
          probeIds.set(task.input.name, task.id);
          advances.set(task.input.name, (n, prepared) =>
            runtime.commit(() =>
              Effect.sync(() => {
                prepared();
                return { status: "running", checkpoint: { phase: "hold", n } } as const;
              }),
            ),
          );
          yield* Deferred.await(gate(task.input.name));
          yield* runtime.commit(() => Effect.succeed(Task.fail("gate")));
        }),
    },
    abort: () => Effect.never,
  });

  /** Creates probes `x` and `y` and waits on them `failFast`. */
  const Waiter = Task.make({
    name: "gc.fail-fast",
    version: 1,
    input: Schema.Null,
    checkpoint: Schema.Union([
      Schema.Struct({ phase: Schema.Literal("spawn") }),
      Schema.Struct({ phase: Schema.Literal("done") }),
    ]),
    result: Schema.Null,
    initial: () => ({ phase: "spawn" }),
    phases: {
      spawn: (task, runtime) =>
        runtime.commit(() =>
          Effect.gen(function* () {
            const tx = yield* Tx;
            const on: Array<TaskId> = [];
            for (const name of ["x", "y"]) {
              on.push(
                yield* tx.createTask(
                  Probe,
                  { name },
                  { ownership: { kind: "task", taskId: task.id } },
                ),
              );
            }
            return {
              status: "waiting",
              checkpoint: { phase: "done" },
              on,
              policy: "failFast",
            } as const;
          }),
        ),
      done: (_task, runtime) => runtime.commit(() => Effect.succeed(completed(null))),
    },
    abort: () => Effect.never,
  });

  /** Advances probe `name` to `n = 1` while the mark-writing commit is in flight, and runs a reconcile pass meanwhile. */
  const raceMark = (
    storage: ControlledStorage,
    h: TestHarness,
    name: string,
    mark: Effect.Effect<unknown, Tagged>,
  ) =>
    Effect.gen(function* () {
      const heldMark = yield* storage.holdCommits;
      const marking = yield* fork(mark);
      yield* heldMark.entered;
      let prepared = false;
      const advance = advances.get(name)!;
      const advancing = yield* fork(
        advance(1, () => {
          prepared = true;
        }),
      );
      yield* eventually(Effect.sync(() => prepared));
      yield* flush;
      // Global idle schedules a reconcile pass, which reaches the line while both are unpublished.
      yield* fork(h.harness.awaitIdle.pipe(Effect.ignore));
      yield* flush;
      const heldAdvance = yield* storage.holdCommits;
      yield* heldMark.release;
      yield* heldAdvance.entered;
      yield* flush;
      yield* heldAdvance.release;
      yield* Fiber.await(marking);
      yield* Fiber.await(advancing);
      yield* flush;
    });

  it.effect(
    "marks a child in a cascade from its current record, not one its pending commit replaces",
    () =>
      run(backend)(
        Effect.gen(function* () {
          yield* reset;
          advances.clear();
          probeIds.clear();
          const storage = yield* controlledStorage;
          const { h, root } = yield* openWith(storage, [Probe]);
          const { parent, child } = yield* root.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              const parent = yield* tx.createTask(
                Probe,
                { name: "p" },
                { ownership: { kind: "conversation" } },
              );
              const child = yield* tx.createTask(
                Probe,
                { name: "c" },
                { ownership: { kind: "task", taskId: parent } },
              );
              return { parent, child };
            }),
          );
          yield* h.harness.resume;
          yield* eventually(Effect.sync(() => advances.has("c")));
          yield* flush;
          yield* raceMark(storage, h, "c", h.harness.abortTask(parent));
          yield* eventually(
            h.harness
              .getTask(child)
              .pipe(Effect.map((found) => Option.getOrUndefined(found)?.abortRequested === true)),
          );
          assert.deepInclude(yield* checkpointOf(h, child), { n: 1 });
          yield* h.close;
        }),
      ),
  );

  it.effect("marks the other tasks of a failFast wait from their current records", () =>
    run(backend)(
      Effect.gen(function* () {
        yield* reset;
        advances.clear();
        probeIds.clear();
        const storage = yield* controlledStorage;
        const { h, root } = yield* openWith(storage, [Probe, Waiter]);
        yield* root.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            yield* tx.createTask(Waiter, null, { ownership: { kind: "conversation" } });
          }),
        );
        yield* h.harness.resume;
        yield* eventually(Effect.sync(() => advances.has("x") && advances.has("y")));
        yield* flush;
        const y = probeIds.get("y")!;
        // X fails while its commit is in flight; the pass that marks Y runs while Y's own commit is pending.
        yield* raceMark(storage, h, "y", open("x"));
        yield* eventually(
          h.harness
            .getTask(y)
            .pipe(Effect.map((found) => Option.getOrUndefined(found)?.abortRequested === true)),
        );
        assert.deepInclude(yield* checkpointOf(h, y), { n: 1 });
        yield* h.close;
      }),
    ),
  );
});

describeEachBackend("group commit: reads on the line", (backend) => {
  it.effect(
    "reads no task record from Storage for more live and awaited tasks than the record cache's count",
    () =>
      run(backend)(
        Effect.gen(function* () {
          yield* reset;
          const Child = Task.make({
            name: "gc.child",
            version: 1,
            input: Schema.Null,
            checkpoint: Schema.Struct({ phase: Schema.Literal("end") }),
            result: Schema.Number,
            initial: () => ({ phase: "end" }),
            phases: {
              end: (task, runtime) =>
                runtime.commit(() => Effect.succeed(completed(Number(task.id)))),
            },
            abort: () => Effect.void,
          });
          const Parent = Task.make({
            name: "gc.parent",
            version: 1,
            input: Schema.Null,
            checkpoint: Schema.Union([
              Schema.Struct({ phase: Schema.Literal("spawn") }),
              Schema.Struct({ phase: Schema.Literal("read"), on: Schema.Array(Schema.Number) }),
            ]),
            result: Schema.Number,
            initial: () => ({ phase: "spawn" }),
            phases: {
              spawn: (task, runtime) =>
                runtime.commit(() =>
                  Effect.gen(function* () {
                    const tx = yield* Tx;
                    const on: Array<TaskId> = [];
                    for (let n = 0; n < 300; n++) {
                      on.push(
                        yield* tx.createTask(Child, null, {
                          ownership: { kind: "task", taskId: task.id },
                        }),
                      );
                    }
                    return {
                      status: "waiting",
                      checkpoint: { phase: "read", on },
                      on,
                      policy: "allSettled",
                    } as const;
                  }),
                ),
              read: (task, runtime) =>
                Effect.gen(function* () {
                  // SAFETY: the read phase's checkpoint names the children it waited on.
                  const on = (task.state.checkpoint as { readonly on: ReadonlyArray<number> })
                    .on as ReadonlyArray<TaskId>;
                  const outcomes = yield* runtime.outcomes(on);
                  const sum = outcomes.filter((outcome) => outcome.status === "completed").length;
                  yield* runtime.commit(() => Effect.succeed(completed(sum)));
                }),
            },
            abort: () => Effect.void,
          });
          const storage = yield* controlledStorage;
          let taskReads = 0;
          const { h, root } = yield* openWith(storage, [Parent, Child]);
          const id = yield* root.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              return yield* tx.createTask(Parent, null, { ownership: { kind: "conversation" } });
            }),
          );
          storage.intercept.task = (_id, next) =>
            Effect.suspend(() => {
              taskReads++;
              return next;
            });
          yield* h.harness.resume;
          assert.deepStrictEqual((yield* h.harness.awaitTask(id)).state.outcome, {
            status: "completed",
            result: 300,
          });
          // Each change of a live task, and the parent's read of the outcomes it waited on, finds the record in
          // memory.
          assert.strictEqual(taskReads, 0);
          yield* h.close;
        }),
      ),
    30_000,
  );
});

describeEachBackend("group commit: outcomes and publication through the Harness", (backend) => {
  it.effect("finds the submission an unpublished commit admitted with the same request ID", () =>
    run(backend)(
      Effect.gen(function* () {
        yield* reset;
        const storage = yield* controlledStorage;
        const { h, root } = yield* openWith(storage, []);
        const draft = {
          type: "write",
          entry: { kind: "note", data: {} },
          requestId: "r1",
        } as const;
        const held = yield* storage.holdCommits;
        const first = yield* fork(root.submit(draft));
        yield* held.entered;
        const second = yield* fork(root.submit(draft));
        yield* flush;
        yield* held.release;
        assert.strictEqual((yield* Fiber.join(second)).id, (yield* Fiber.join(first)).id);
        yield* h.close;
      }),
    ),
  );

  it.effect(
    "runs a wait continuation of one commit before the next commit of the same call is published",
    () =>
      run(backend)(
        Effect.gen(function* () {
          yield* reset;
          const storage = yield* controlledStorage;
          const { h, root } = yield* openWith(storage, [Hold]);
          const other = yield* h.harness.createConversation({ ownership: { kind: "ownerless" } });
          const one = yield* createIn(root, "one");
          const two = yield* createIn(other, "two");
          yield* h.harness.resume;
          yield* statusIs(h, one, "running");
          yield* statusIs(h, two, "running");
          const ended = yield* followTasks(h, (record) => record.state.status === "terminal");
          // Both end in one call, behind a commit in flight.
          const held = yield* storage.holdCommits;
          const blocker = yield* fork(
            root.commit(
              Effect.gen(function* () {
                const tx = yield* Tx;
                yield* tx.appendEntry(root.id, { kind: "note" });
              }),
            ),
          );
          yield* held.entered;
          let seen: boolean | undefined;
          const waited = yield* fork(
            h.harness.awaitTask(one).pipe(
              Effect.tap(() =>
                Effect.sync(() => {
                  seen = ended.has(two);
                }),
              ),
            ),
          );
          yield* open("one");
          yield* flush;
          yield* open("two");
          yield* flush;
          yield* held.release;
          yield* Fiber.join(blocker);
          yield* Fiber.join(waited);
          yield* h.harness.awaitTask(two);
          assert.strictEqual(seen, false);
          yield* h.close;
        }),
      ),
  );

  it.effect(
    "signals an invocation when it closes during a call in flight, and stores that call before Storage closes",
    () =>
      run(backend)(
        Effect.gen(function* () {
          yield* reset;
          const signalled = yield* Deferred.make<void>();
          const Listens = Task.make({
            name: "gc.listens",
            version: 1,
            input: Schema.Null,
            checkpoint: Schema.Struct({ phase: Schema.Literal("wait") }),
            result: Schema.Null,
            initial: () => ({ phase: "wait" }),
            phases: {
              // The close's signal interrupts the handler.
              wait: () =>
                Effect.never.pipe(Effect.onInterrupt(() => Deferred.succeed(signalled, undefined))),
            },
            abort: () => Effect.void,
          });
          const storage = yield* controlledStorage;
          const { h, root } = yield* openWith(storage, [Listens]);
          const id = yield* root.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              return yield* tx.createTask(Listens, null, { ownership: { kind: "conversation" } });
            }),
          );
          yield* h.harness.resume;
          yield* statusIs(h, id, "running");
          const held = yield* storage.holdCommits;
          const entry = yield* fork(
            root.commit(
              Effect.gen(function* () {
                const tx = yield* Tx;
                yield* tx.appendEntry(root.id, { kind: "note" });
              }),
            ),
          );
          yield* held.entered;
          const closing = yield* fork(h.close);
          yield* Deferred.await(signalled);
          assert.isFalse(yield* settled(closing));
          yield* held.release;
          yield* Fiber.await(entry);
          yield* Fiber.join(closing);
          const reopened = yield* openWith(storage, [Listens]);
          const kinds = yield* Stream.runCollect(reopened.root.entries()).pipe(
            Effect.map((entries) => Array.from(entries).map((entry) => entry.kind)),
          );
          assert.include(kinds, "note");
          // Close wrote the task it stopped back as pending.
          assert.strictEqual(yield* statusOf(reopened.h, id), "pending");
          yield* reopened.h.close;
          assert.isTrue(Exit.isSuccess(yield* Fiber.await(entry)));
        }),
      ),
  );
});

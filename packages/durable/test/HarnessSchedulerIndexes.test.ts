// The scheduler's in-memory indexes, on every backend: cases where a wrong index changes behaviour, each against
// what the committed records imply.
//
// A reopen is a new Harness over the same `ControlledStorage` once the first one closed. `createSession` is a
// Session kernel in a scope of its own, and a task change is the kernel's `commitWith`. Storage interception goes
// through `storage.intercept`.
import { assert, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import type { TaskId } from "../src/Ids.ts";
import * as Task from "../src/Task.ts";
import { Tx } from "../src/Tx.ts";
import { completed, eventually, openHarness, settled, type TestHarness } from "./HarnessSupport.ts";
import { controlledStorage, describeEachBackend, run, withSession } from "./SessionSupport.ts";

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

const Named = Schema.Struct({ name: Schema.String });

/** The next state an abort handler commits: `aborted`, with no reason. */
const aborted = { status: "terminal", outcome: { status: "aborted" } } as const;

/** Holds until its gate opens, then completes; its abort handler ends it `aborted`. */
const Hold = Task.make({
  name: "test.index.hold",
  version: 1,
  input: Named,
  checkpoint: Schema.Struct({ phase: Schema.Literal("hold") }),
  result: Schema.Null,
  initial: () => ({ phase: "hold" }),
  phases: {
    // The abort mark interrupts the handler while it waits for the gate.
    hold: (task, runtime) =>
      Effect.gen(function* () {
        yield* Deferred.await(gate(task.input.name));
        yield* runtime.commit(() => Effect.succeed(completed(null)));
      }),
  },
  abort: (_task, runtime) => runtime.commit(() => Effect.succeed(aborted)),
});

type ProbeRuntime = Pick<
  Task.TaskRuntime<{ readonly name: string }, { readonly phase: "look" }, null>,
  "ownedTasks"
>;

/** What `ownedTasks` returned to each run of a `Probe`, by its name, and the runtime of its last run. */
const owned = new Map<string, Array<TaskId>>();
const runtimes = new Map<string, ProbeRuntime>();

/** Records its owned tasks once its `look` gate opens, then holds until its own gate opens. */
const Probe = Task.make({
  name: "test.index.probe",
  version: 1,
  input: Named,
  checkpoint: Schema.Struct({ phase: Schema.Literal("look") }),
  result: Schema.Null,
  initial: () => ({ phase: "look" }),
  phases: {
    look: (task, runtime) =>
      Effect.gen(function* () {
        runtimes.set(task.input.name, runtime);
        yield* Deferred.await(gate(`look.${task.input.name}`));
        owned.set(
          task.input.name,
          (yield* runtime.ownedTasks).map((record) => record.id),
        );
        yield* Deferred.await(gate(task.input.name));
        yield* runtime.commit(() => Effect.succeed(completed(null)));
      }),
  },
  abort: (_task, runtime) => runtime.commit(() => Effect.succeed(aborted)),
});

// Each case starts from empty maps.
const reset = Effect.sync(() => {
  gates.clear();
  owned.clear();
  runtimes.clear();
});

const state = (h: TestHarness, id: TaskId) =>
  h.harness.getTask(id).pipe(Effect.map(Option.getOrThrow));

describeEachBackend("scheduler indexes", (backend) => {
  it.effect("takes a cascade's reason from the nearest cancelling owner", () =>
    run(backend)(
      Effect.gen(function* () {
        yield* reset;
        // top (abort requested) -> middle (restart-marked) -> leaf: the leaf gets middle's `restart` first, as the
        // nearest cancelling owner, and the request only once middle carries it.
        const storage = yield* controlledStorage;
        const ids = yield* withSession(storage, (session) =>
          Effect.gen(function* () {
            const created = yield* session.commit(
              Effect.gen(function* () {
                const tx = yield* Tx;
                const conversation = yield* tx.createConversation({
                  ownership: { kind: "ownerless" },
                });
                const top = yield* tx.createTask(
                  Hold,
                  { name: "top" },
                  { conversationId: conversation.id, ownership: { kind: "conversation" } },
                );
                const middle = yield* tx.createTask(
                  Hold,
                  { name: "middle" },
                  { ownership: { kind: "task", taskId: top } },
                );
                const leaf = yield* tx.createTask(
                  Hold,
                  { name: "leaf" },
                  { ownership: { kind: "task", taskId: middle } },
                );
                return { top, middle, leaf };
              }),
            );
            yield* session.commitWith((tx) =>
              Effect.gen(function* () {
                const top = Option.getOrThrow(yield* tx.service.task(created.top));
                const middle = Option.getOrThrow(yield* tx.service.task(created.middle));
                yield* tx.setTask({ ...top, abortRequested: true });
                yield* tx.setTask({ ...middle, abortRequested: true, abortReason: "restart" });
              }),
            );
            return created;
          }),
        );
        // Record every mark of the leaf, in commit order.
        const reasons: Array<string | undefined> = [];
        storage.intercept.commit = (batches, next) => {
          for (const write of batches.flat()) {
            if (
              write.type === "task" &&
              write.value.id === ids.leaf &&
              write.value.abortRequested
            ) {
              reasons.push(write.value.abortReason);
            }
          }
          return next;
        };
        const h = yield* openHarness(storage, [Hold]);
        yield* h.harness.resume;
        for (const id of [ids.leaf, ids.middle, ids.top]) yield* h.harness.awaitTask(id);
        assert.strictEqual(reasons[0], "restart");
        yield* h.close;
      }),
    ),
  );

  it.effect("finalizes a chain of held owners in the commit that frees the last one", () =>
    run(backend)(
      Effect.gen(function* () {
        yield* reset;
        const storage = yield* controlledStorage;
        // The commit, by count, that wrote each terminal record.
        const endedIn = new Map<TaskId, number>();
        let commits = 0;
        storage.intercept.commit = (batches, next) => {
          for (const writes of batches) {
            commits++;
            for (const write of writes) {
              if (write.type === "task" && write.value.state.status === "terminal") {
                endedIn.set(write.value.id, commits);
              }
            }
          }
          return next;
        };
        const h = yield* openHarness(storage, [Hold]);
        const root = yield* h.harness.ensureRoot();
        yield* h.harness.resume;
        const { top, middle, leaf } = yield* root.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            const top = yield* tx.createTask(
              Hold,
              { name: "top" },
              { ownership: { kind: "conversation" } },
            );
            const middle = yield* tx.createTask(
              Hold,
              { name: "middle" },
              { ownership: { kind: "task", taskId: top } },
            );
            const leaf = yield* tx.createTask(
              Hold,
              { name: "leaf" },
              { ownership: { kind: "task", taskId: middle } },
            );
            return { top, middle, leaf };
          }),
        );
        yield* openGate("top");
        yield* openGate("middle");
        yield* eventually(
          state(h, top).pipe(Effect.map((record) => record.state.status === "completing")),
        );
        yield* eventually(
          state(h, middle).pipe(Effect.map((record) => record.state.status === "completing")),
        );
        yield* openGate("leaf");
        assert.strictEqual((yield* h.harness.awaitTask(top)).state.outcome.status, "completed");
        // Both owners end in one finalize commit, after the leaf's.
        assert.strictEqual(endedIn.get(middle), endedIn.get(top));
        assert.isAbove(endedIn.get(middle)!, endedIn.get(leaf)!);
        yield* h.close;
      }),
    ),
  );

  it.effect(
    "keeps a scope busy after reopen for work below an ended task whose chain is not loaded yet",
    () =>
      run(backend)(
        Effect.gen(function* () {
          yield* reset;
          const storage = yield* controlledStorage;
          let opened = yield* openHarness(storage, [Hold]);
          let root = yield* opened.harness.ensureRoot();
          yield* opened.harness.resume;
          // root conversation -> owner (ends) -> conversation C -> inner (created after owner ended), as a subagent
          // call's.
          const { owner, child } = yield* root.commit(
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
              return { owner, child: child.id };
            }),
          );
          yield* openGate("owner");
          yield* opened.harness.awaitTask(owner);
          const conversation = Option.getOrThrow(yield* opened.harness.conversation(child));
          const inner = yield* conversation.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              return yield* tx.createTask(
                Hold,
                { name: "inner" },
                { ownership: { kind: "conversation" } },
              );
            }),
          );
          yield* opened.close;
          gates.clear();
          // Hold the read of C's record, so the chain above `inner` stays unloaded while idle is decided.
          const held = yield* Deferred.make<void>();
          storage.intercept.conversation = (id, next) =>
            id === child ? Deferred.await(held).pipe(Effect.andThen(next)) : next;
          opened = yield* openHarness(storage, [Hold]);
          // A task whose chain is not loaded counts as inside every scope. (The held read blocks the Session line,
          // so the root handle comes after.)
          const idle = yield* Effect.forkChild(opened.harness.awaitIdle);
          assert.isFalse(yield* settled(idle));
          yield* Deferred.succeed(held, undefined);
          root = yield* opened.harness.ensureRoot();
          yield* opened.harness.resume;
          yield* openGate("inner");
          yield* opened.harness.awaitTask(inner);
          yield* root.awaitIdle;
          yield* Fiber.join(idle);
          yield* opened.close;
        }),
      ),
  );
});

describeEachBackend("TaskRuntime.ownedTasks", (backend) => {
  it.effect("lists the live tasks a task owns directly, in ID order, also after a reopen", () =>
    run(backend)(
      Effect.gen(function* () {
        yield* reset;
        const storage = yield* controlledStorage;
        let opened = yield* openHarness(storage, [Hold, Probe]);
        const root = yield* opened.harness.ensureRoot();
        yield* opened.harness.resume;
        // The probe owns: two held tasks, one that ends, a grandchild below the first, and a conversation with a
        // task.
        const ids = yield* root.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            const probe = yield* tx.createTask(
              Probe,
              { name: "probe" },
              { ownership: { kind: "conversation" } },
            );
            const first = yield* tx.createTask(
              Hold,
              { name: "first" },
              { ownership: { kind: "task", taskId: probe } },
            );
            const ends = yield* tx.createTask(
              Hold,
              { name: "ends" },
              { ownership: { kind: "task", taskId: probe } },
            );
            const second = yield* tx.createTask(
              Hold,
              { name: "second" },
              { ownership: { kind: "task", taskId: probe } },
            );
            yield* tx.createTask(
              Hold,
              { name: "grandchild" },
              { ownership: { kind: "task", taskId: first } },
            );
            const child = yield* tx.createConversation({
              ownership: { kind: "task", taskId: probe },
            });
            yield* tx.createTask(
              Hold,
              { name: "inside" },
              { ownership: { kind: "conversation" }, conversationId: child.id },
            );
            return { probe, first, ends, second };
          }),
        );
        yield* openGate("ends");
        yield* opened.harness.awaitTask(ids.ends);
        // The first ends too, but holds its outcome as `completing` while its grandchild runs: still live, so listed.
        yield* openGate("first");
        const firstHarness = opened;
        yield* eventually(
          state(firstHarness, ids.first).pipe(
            Effect.map((record) => record.state.status === "completing"),
          ),
        );
        yield* openGate("look.probe");
        yield* eventually(Effect.sync(() => owned.has("probe")));
        assert.deepStrictEqual(owned.get("probe"), [ids.first, ids.second]);
        yield* opened.close;

        // After a reopen the rerun sees the same tasks, which an earlier invocation created.
        gates.clear();
        owned.clear();
        opened = yield* openHarness(storage, [Hold, Probe]);
        yield* opened.harness.resume;
        yield* openGate("look.probe");
        yield* eventually(Effect.sync(() => owned.has("probe")));
        assert.deepStrictEqual(owned.get("probe"), [ids.first, ids.second]);
        yield* opened.close;
      }),
    ),
  );

  it.effect("rejects once its invocation has ended", () =>
    run(backend)(
      Effect.gen(function* () {
        yield* reset;
        const storage = yield* controlledStorage;
        const opened = yield* openHarness(storage, [Hold, Probe]);
        const root = yield* opened.harness.ensureRoot();
        yield* opened.harness.resume;
        const probe = yield* root.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            return yield* tx.createTask(
              Probe,
              { name: "probe" },
              { ownership: { kind: "conversation" } },
            );
          }),
        );
        yield* eventually(Effect.sync(() => runtimes.has("probe")));
        const runtime = runtimes.get("probe")!;
        // Aborting joins the run invocation, which has ended by then.
        yield* opened.harness.abortTask(probe);
        const error = yield* Effect.flip(runtime.ownedTasks);
        assert.strictEqual(error._tag, "TaskError");
        assert.include(error.message, "invocation has ended");
        yield* opened.close;
      }),
    ),
  );
});

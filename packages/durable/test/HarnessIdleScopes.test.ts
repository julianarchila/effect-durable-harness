// Idle checks without per-commit scans: a task change settles only the conversations above it (idle waiters and
// kept context ranges), expiry runs oldest first from an ordered index, and `wake()` reads only sleeping tasks while
// work is active, on every backend. Every case checks the incremental idle state against a full recompute
// (`h.scheduler.idleState()`).
//
// The clock is the `TestClock`, which `setTime` can move back; background timers declined is the host's
// `IdleTimers` turned off; scanned rows are counted with `storage.intercept.scanEntries`; a reopen is a new
// Harness over the same controlled storage. The abort mark interrupts a gated handler.
import { assert, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as TestClock from "effect/testing/TestClock";
import type { EntryDraft } from "../src/Entry.ts";
import type { Conversation } from "../src/Harness.ts";
import { ConversationId, ROOT_CONVERSATION_ID, TaskId } from "../src/Ids.ts";
import type { TaskRecord } from "../src/Records.ts";
import type { StorageWrite } from "../src/AgentStorage.ts";
import type { Wake } from "../src/SessionWakeups.ts";
import { commitWrites } from "../src/testing/CommitWrites.ts";
import { IdleTimers } from "../src/IdleTimers.ts";
import * as Signal from "../src/Signal.ts";
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
/** Resolves once the named task's context read returned. */
const reads = new Map<string, Deferred.Deferred<void>>();

const named = (map: Map<string, Deferred.Deferred<void>>, name: string) => {
  let found = map.get(name);
  if (found === undefined) {
    found = Deferred.makeUnsafe<void>();
    map.set(name, found);
  }
  return found;
};

// Each case starts from empty state.
const reset = Effect.sync(() => {
  gates.clear();
  reads.clear();
});

const aborted = { status: "terminal", outcome: { status: "aborted" } } as const;

/**
 * Reads the context of conversation `read` (when given), then holds until its gate opens and completes; its abort
 * handler ends it `aborted`.
 */
const Work = Task.make({
  name: "test.idle.work",
  version: 1,
  input: Schema.Struct({ name: Schema.String, read: Schema.optionalKey(Schema.Number) }),
  checkpoint: Schema.Struct({ phase: Schema.Literal("run") }),
  result: Schema.Null,
  initial: () => ({ phase: "run" }),
  phases: {
    run: (task, runtime) =>
      Effect.gen(function* () {
        if (task.input.read !== undefined) {
          yield* runtime.context(ConversationId.make(task.input.read));
          yield* Deferred.succeed(named(reads, task.input.name), undefined);
        }
        yield* Deferred.await(named(gates, task.input.name));
        yield* runtime.commit(() => Effect.succeed(completed(null)));
      }),
  },
  abort: (_task, runtime) => runtime.commit(() => Effect.succeed(aborted)),
});

const Go = Signal.make("test.idle.wake", { version: 1, schema: Schema.Struct({}) });

/** Reads the context of conversation `read`, then parks on `Go` until a delivery, and completes. */
const Parker = Task.make({
  name: "test.idle.parker",
  version: 1,
  input: Schema.Struct({ read: Schema.Number }),
  checkpoint: Schema.Union([
    Schema.Struct({ phase: Schema.Literal("run") }),
    Schema.Struct({ phase: Schema.Literal("woke") }),
  ]),
  result: Schema.Null,
  initial: () => ({ phase: "run" }),
  phases: {
    run: (task, runtime) =>
      Effect.gen(function* () {
        yield* runtime.context(ConversationId.make(task.input.read));
        yield* runtime.sleep({ checkpoint: { phase: "woke" }, signals: [Go.key("go")] });
      }),
    woke: (_task, runtime) => runtime.commit(() => Effect.succeed(completed(null))),
  },
  abort: (_task, runtime) => runtime.commit(() => Effect.succeed(aborted)),
});

interface OpenOptions {
  /** The retention setting, read at each use. */
  readonly retention?: (() => number) | undefined;
  /** Whether the host keeps background timers; the default here declines them, as Cloudflare Workers do. */
  readonly timers?: boolean | undefined;
  readonly storage?: ControlledStorage | undefined;
}

/**
 * A resumed Harness running `Work` over the controlled storage, counting the entry rows each scan returns, with the
 * retention `retention` returns, and background timers declined unless `timers`.
 */
const open = (options: OpenOptions = {}) =>
  Effect.gen(function* () {
    const scanned = { rows: 0 };
    const storage = options.storage ?? (yield* controlledStorage);
    storage.intercept.scanEntries = (_args, next) =>
      Effect.tap(next, (page) =>
        Effect.sync(() => {
          scanned.rows += page.items.length;
        }),
      );
    const retention = options.retention ?? (() => 600_000);
    const h = yield* openHarness(storage, [Work, Parker], {
      settings: () => ({ contextRetentionMs: retention() }),
    }).pipe(Effect.provideService(IdleTimers, { enabled: options.timers === true }));
    yield* h.harness.resume;
    const root = yield* h.harness.ensureRoot();
    /** A new ownerless conversation with `entries` entries. */
    const conversation = (entries = 2) =>
      Effect.gen(function* () {
        const created = yield* h.harness.createConversation({ ownership: { kind: "ownerless" } });
        for (let index = 0; index < entries; index++) {
          yield* created.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              yield* tx.appendEntry(created.id, { kind: "note", data: { index } });
            }),
          );
        }
        return created;
      });
    /** Starts `Work` named `name` in `into`, reading conversation `read`, and waits for its read. */
    const work = (name: string, into: Conversation, read?: ConversationId) =>
      Effect.gen(function* () {
        const input = read === undefined ? { name } : { name, read };
        const id = yield* into.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            return yield* tx.createTask(Work, input, {
              ownership: { kind: "conversation" },
              conversationId: into.id,
            });
          }),
        );
        if (read !== undefined) yield* Deferred.await(named(reads, name));
        return id;
      });
    /** Opens the gate of `name` and waits for its task, `id`, to end. */
    const end = (name: string, id: TaskId) =>
      Deferred.succeed(named(gates, name), undefined).pipe(
        Effect.andThen(h.harness.awaitTask(id)),
        Effect.asVoid,
      );
    /** The entry rows a read of `read` from a new task in `into` scans. */
    const rowsOf = (name: string, into: Conversation, read: ConversationId) =>
      Effect.gen(function* () {
        const before = scanned.rows;
        const id = yield* work(name, into, read);
        const rows = scanned.rows - before;
        yield* end(name, id);
        return rows;
      });
    return { h, root, conversation, work, end, rowsOf };
  });

/** Flushes until scheduler passes started by the last changes settled. */
const quiet = Effect.forEach(Array.from({ length: 4 }), () => flush, { discard: true });

const idleState = (h: TestHarness) => h.scheduler.idleState();

const openGate = (name: string) => Deferred.succeed(named(gates, name), undefined);

const statusOf = (h: TestHarness, id: TaskId) =>
  h.harness.getTask(id).pipe(Effect.map((found) => Option.getOrThrow(found).state.status));

describeEachBackend("idle waiters by scope", (backend) => {
  it.effect(
    "resolve through owner chains, owned conversations and ended tasks between, ignoring background work",
    () =>
      run(backend)(
        Effect.gen(function* () {
          yield* reset;
          const { h, root } = yield* open();
          const tree = yield* root.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              const p = yield* tx.createTask(
                Work,
                { name: "p" },
                { ownership: { kind: "conversation" } },
              );
              const b = yield* tx.createTask(
                Work,
                { name: "b" },
                { ownership: { kind: "task", taskId: p } },
              );
              const a = yield* tx.createTask(
                Work,
                { name: "a" },
                { ownership: { kind: "task", taskId: b } },
              );
              const c = yield* tx.createConversation({ ownership: { kind: "task", taskId: a } });
              const d = yield* tx.createTask(
                Work,
                { name: "d" },
                { ownership: { kind: "conversation" }, conversationId: c.id },
              );
              const g = yield* tx.createTask(
                Work,
                { name: "g" },
                { ownership: { kind: "conversation" }, conversationId: c.id, background: true },
              );
              return { p, b, a, c: c.id, d, g };
            }),
          );
          const c = Option.getOrThrow(yield* h.harness.conversation(tree.c));
          const idleC = yield* Effect.forkChild(c.awaitIdle);
          const idleRoot = yield* Effect.forkChild(root.awaitIdle);
          const idleAll = yield* Effect.forkChild(h.harness.awaitIdle);
          // The owners A and B hold for D: live, though their handlers returned.
          yield* openGate("a");
          yield* openGate("b");
          yield* eventually(
            statusOf(h, tree.b).pipe(Effect.map((status) => status === "completing")),
          );
          assert.deepStrictEqual(
            [yield* settled(idleC), yield* settled(idleRoot), yield* settled(idleAll)],
            [false, false, false],
          );
          // D ends: C is idle (G is background); A and B finalize; P still holds the root.
          yield* openGate("d");
          yield* eventually(settled(idleC));
          yield* h.harness.awaitTask(tree.b);
          assert.deepStrictEqual(
            [yield* settled(idleRoot), yield* settled(idleAll)],
            [false, false],
          );
          yield* openGate("p");
          yield* eventually(settled(idleRoot));
          yield* eventually(settled(idleAll));
          assert.notStrictEqual(yield* statusOf(h, tree.g), "terminal");
          yield* h.close;
        }),
      ),
  );

  it.effect(
    "count work whose chain is not loaded yet as inside, until the open reconcile loads it",
    () =>
      run(backend)(
        Effect.gen(function* () {
          yield* reset;
          const storage = yield* controlledStorage;
          const first = yield* openHarness(storage, [Work]);
          const firstRoot = yield* first.harness.ensureRoot();
          const tree = yield* firstRoot.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              const p = yield* tx.createTask(
                Work,
                { name: "p" },
                { ownership: { kind: "conversation" } },
              );
              const c = yield* tx.createConversation({ ownership: { kind: "task", taskId: p } });
              const d = yield* tx.createTask(
                Work,
                { name: "d" },
                { ownership: { kind: "conversation" }, conversationId: c.id },
              );
              const other = yield* tx.createConversation({ ownership: { kind: "ownerless" } });
              return { p, c: c.id, d, other: other.id };
            }),
          );
          yield* first.close;
          // Reopened, no chain is loaded: P and D count as inside every scope until the reconcile pass loads theirs.
          const h = yield* openHarness(storage, [Work]);
          const other = Option.getOrThrow(yield* h.harness.conversation(tree.other));
          const idleOther = yield* Effect.forkChild(other.awaitIdle);
          const idleRoot = yield* Effect.forkChild((yield* h.harness.ensureRoot()).awaitIdle);
          yield* eventually(settled(idleOther));
          assert.isFalse(yield* settled(idleRoot));
          yield* h.harness.resume;
          yield* openGate("d");
          yield* h.harness.awaitTask(tree.d);
          assert.isFalse(yield* settled(idleRoot));
          yield* openGate("p");
          yield* eventually(settled(idleRoot));
          yield* h.close;
        }),
      ),
  );

  it.effect("resolve a waiter on an unrelated conversation without settling the busy one", () =>
    run(backend)(
      Effect.gen(function* () {
        yield* reset;
        const { h, conversation, work, end } = yield* open();
        const busy = yield* conversation(0);
        const other = yield* conversation(0);
        const held = yield* work("held", busy);
        const quick = yield* work("quick", other);
        const idleBusy = yield* Effect.forkChild(busy.awaitIdle);
        const idleOther = yield* Effect.forkChild(other.awaitIdle);
        yield* end("quick", quick);
        yield* eventually(settled(idleOther));
        assert.isFalse(yield* settled(idleBusy));
        yield* end("held", held);
        yield* eventually(settled(idleBusy));
        yield* h.close;
      }),
    ),
  );
});

describeEachBackend("kept context ranges", (backend) => {
  it.effect(
    "count a conversation whose live work is all parked as inactive, and as busy once it runs again",
    () =>
      run(backend)(
        Effect.gen(function* () {
          yield* reset;
          const { h, conversation, work, end } = yield* open();
          const x = yield* conversation();
          const parker = yield* x.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              return yield* tx.createTask(
                Parker,
                { read: x.id },
                { ownership: { kind: "conversation" }, conversationId: x.id },
              );
            }),
          );
          yield* eventually(
            statusOf(h, parker).pipe(Effect.map((status) => status === "sleeping")),
          );
          yield* quiet;
          // Not idle, as a wait for idleness says, but inactive: its context ages like an idle one's.
          const idle = yield* Effect.forkChild(x.awaitIdle);
          yield* quiet;
          assert.isFalse(yield* settled(idle));
          assert.deepInclude(idleState(h), { contexts: 1, idleContexts: 1, idleMismatches: 0 });
          const running = yield* work("running", x);
          yield* quiet;
          assert.deepInclude(idleState(h), { contexts: 1, idleContexts: 0, idleMismatches: 0 });
          yield* end("running", running);
          yield* h.harness.signal(parker, Go, "go", {});
          yield* h.harness.awaitTask(parker);
          yield* h.close;
        }),
      ),
  );

  it.effect("settles a run end whose sweep drops the chain without a check of every scope", () =>
    run(backend)(
      Effect.gen(function* () {
        yield* reset;
        const { h, root } = yield* open();
        const tree = yield* root.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            const p = yield* tx.createTask(
              Work,
              { name: "p" },
              { ownership: { kind: "conversation" } },
            );
            const t = yield* tx.createTask(
              Work,
              { name: "t" },
              { ownership: { kind: "task", taskId: p } },
            );
            const c = yield* tx.createConversation({ ownership: { kind: "task", taskId: t } });
            // A context range keeps entries: an empty conversation has none to keep.
            yield* tx.appendEntry(c.id, { kind: "note" });
            return { p, t, c: c.id };
          }),
        );
        const c = Option.getOrThrow(yield* h.harness.conversation(tree.c));
        const d = yield* c.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            return yield* tx.createTask(
              Work,
              { name: "d", read: tree.c },
              { ownership: { kind: "conversation" }, conversationId: tree.c },
            );
          }),
        );
        yield* Deferred.await(named(reads, "d"));
        yield* openGate("t");
        yield* eventually(
          statusOf(h, tree.t).pipe(Effect.map((status) => status === "completing")),
        );
        yield* quiet;
        const before = idleState(h);
        assert.deepInclude(before, { contexts: 1, idleContexts: 0, idleMismatches: 0 });
        // D's end makes C idle, T finalizes, and the sweep forgets C's chain: none of it checks every scope.
        yield* openGate("d");
        yield* h.harness.awaitTask(d);
        yield* h.harness.awaitTask(tree.t);
        yield* quiet;
        assert.deepStrictEqual(idleState(h), { ...before, idleContexts: 1 });
        yield* openGate("p");
        yield* h.harness.awaitTask(tree.p);
        yield* h.close;
      }),
    ),
  );

  it.effect(
    "drops a range past its retention when a task change makes it busy again, with background timers declined",
    () =>
      run(backend)(
        Effect.gen(function* () {
          yield* reset;
          yield* TestClock.setTime(1_000);
          const { h, conversation, work, end } = yield* open({ retention: () => 1_000 });
          const x = yield* conversation();
          const z = yield* conversation(0);
          yield* end("reader", yield* work("reader", z, x.id));
          assert.deepInclude(idleState(h), { contexts: 1, idleContexts: 1, idleMismatches: 0 });
          yield* TestClock.setTime(2_500);
          // No timer expired it; the task change that makes X busy checks expiry first.
          const busy = yield* work("busy", x);
          yield* quiet;
          assert.deepInclude(idleState(h), { contexts: 0, idleContexts: 0, idleMismatches: 0 });
          yield* end("busy", busy);
          yield* h.close;
        }),
      ),
  );

  it.effect(
    "drops a range past its retention at a reconcile pass with nothing to do, with background timers declined (review)",
    () =>
      run(backend)(
        Effect.gen(function* () {
          yield* reset;
          yield* TestClock.setTime(1_000);
          const { h, conversation, work, end } = yield* open({ retention: () => 1_000 });
          const x = yield* conversation();
          const z = yield* conversation(0);
          yield* end("reader", yield* work("reader", z, x.id));
          const held = yield* work("held", z);
          // Held runs: no task changes from here.
          yield* quiet;
          assert.deepInclude(idleState(h), { contexts: 1, idleContexts: 1 });
          yield* TestClock.setTime(2_500);
          // A wait on busy Z asks for a reconcile pass, which has no work and changes no task: it checks expiry.
          const idle = yield* Effect.forkChild(z.awaitIdle);
          yield* quiet;
          assert.deepInclude(idleState(h), { contexts: 0, idleMismatches: 0 });
          yield* end("held", held);
          yield* eventually(settled(idle));
          yield* h.close;
        }),
      ),
  );

  it.effect("expires in idle order when the clock goes back", () =>
    run(backend)(
      Effect.gen(function* () {
        yield* reset;
        yield* TestClock.setTime(1_000);
        const { h, conversation, work, end, rowsOf } = yield* open({ retention: () => 1_000 });
        const a = yield* conversation(3);
        const b = yield* conversation(5);
        const z = yield* conversation(0);
        yield* end("read a", yield* work("read a", z, a.id));
        yield* TestClock.setTime(500);
        yield* end("read b", yield* work("read b", z, b.id));
        assert.deepInclude(idleState(h), { contexts: 2, idleContexts: 2, idleMismatches: 0 });
        // B went idle at 500 and expires first, though it went idle last.
        yield* TestClock.setTime(1_600);
        yield* end("unrelated", yield* work("unrelated", z));
        assert.deepInclude(idleState(h), { contexts: 1, idleMismatches: 0 });
        assert.strictEqual(yield* rowsOf("again b", z, b.id), 5);
        assert.strictEqual(yield* rowsOf("again a", z, a.id), 0);
        yield* h.close;
      }),
    ),
  );

  it.effect(
    "drops every idle range when retention becomes zero, also one that went idle after the clock's now (review)",
    () =>
      run(backend)(
        Effect.gen(function* () {
          yield* reset;
          yield* TestClock.setTime(2_000);
          let retention = 1_000;
          const { h, conversation, work, end } = yield* open({ retention: () => retention });
          const a = yield* conversation();
          const z = yield* conversation(0);
          yield* end("read", yield* work("read", z, a.id));
          assert.deepInclude(idleState(h), { contexts: 1, idleContexts: 1 });
          // The clock goes back below A's idle start; then retention 0 and an unrelated task change.
          yield* TestClock.setTime(1_000);
          retention = 0;
          yield* end("unrelated", yield* work("unrelated", z));
          assert.deepInclude(idleState(h), { contexts: 0, idleMismatches: 0 });
          yield* h.close;
        }),
      ),
  );

  it.effect("keeps an idle range's expiry when another task reads it again", () =>
    run(backend)(
      Effect.gen(function* () {
        yield* reset;
        yield* TestClock.setTime(1_000);
        const { h, conversation, work, end, rowsOf } = yield* open({ retention: () => 1_000 });
        const a = yield* conversation(3);
        const b = yield* conversation(5);
        const z = yield* conversation(0);
        yield* end("read a", yield* work("read a", z, a.id));
        yield* TestClock.setTime(1_500);
        yield* end("read b", yield* work("read b", z, b.id));
        yield* TestClock.setTime(1_800);
        assert.strictEqual(yield* rowsOf("again a", z, a.id), 0);
        yield* TestClock.setTime(2_100);
        yield* end("unrelated", yield* work("unrelated", z));
        assert.deepInclude(idleState(h), { contexts: 1, idleMismatches: 0 });
        assert.strictEqual(yield* rowsOf("late b", z, b.id), 0);
        yield* h.close;
      }),
    ),
  );

  it.effect("drops a range from the expiry order when an entry commit finds it expired", () =>
    run(backend)(
      Effect.gen(function* () {
        yield* reset;
        yield* TestClock.setTime(1_000);
        const { h, conversation, work, end } = yield* open({ retention: () => 1_000 });
        const a = yield* conversation();
        const z = yield* conversation(0);
        yield* end("read", yield* work("read", z, a.id));
        yield* TestClock.setTime(2_500);
        // An entry-only commit settles no idleness: the range is dropped where it would be extended.
        yield* a.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            yield* tx.appendEntry(a.id, { kind: "note", data: { late: true } });
          }),
        );
        assert.deepInclude(idleState(h), { contexts: 0, idleContexts: 0, idleMismatches: 0 });
        yield* h.close;
      }),
    ),
  );

  it.effect("drops a range from the expiry order when an entry commit cannot extend it", () =>
    run(backend)(
      Effect.gen(function* () {
        yield* reset;
        const { h, conversation, work, end } = yield* open();
        const a = yield* conversation(0);
        const z = yield* conversation(0);
        const append = (draft: EntryDraft) =>
          a.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              return yield* tx.appendEntry(a.id, draft);
            }),
          );
        const first = yield* append({ kind: "note" });
        const second = yield* append({ kind: "note" });
        yield* append({ kind: "summary", head: second.id });
        yield* end("read", yield* work("read", z, a.id));
        assert.deepInclude(idleState(h), { contexts: 1, idleContexts: 1, idleMismatches: 0 });
        // A marker whose head is older than the range's: the range cannot be extended and is dropped.
        yield* append({ kind: "summary", head: first.id });
        assert.deepInclude(idleState(h), { contexts: 0, idleContexts: 0, idleMismatches: 0 });
        yield* h.close;
      }),
    ),
  );

  for (const [name, changed, kept] of [
    ["shorter", 400, 0],
    ["longer", 10_000, 1],
    ["zero", 0, 0],
  ] as const) {
    it.effect(`applies a ${name} retention to idle ranges at an unrelated commit`, () =>
      run(backend)(
        Effect.gen(function* () {
          yield* reset;
          yield* TestClock.setTime(1_000);
          let retention = 1_000;
          const { h, conversation, work, end } = yield* open({ retention: () => retention });
          const a = yield* conversation();
          const z = yield* conversation(0);
          yield* end("read", yield* work("read", z, a.id));
          yield* TestClock.setTime(1_500);
          retention = changed;
          yield* end("unrelated", yield* work("unrelated", z));
          assert.deepInclude(idleState(h), { contexts: kept, idleMismatches: 0 });
          yield* h.close;
        }),
      ),
    );
  }

  it.effect("drops a range that goes idle with zero retention", () =>
    run(backend)(
      Effect.gen(function* () {
        yield* reset;
        const { h, conversation, work, end } = yield* open({ retention: () => 0 });
        const a = yield* conversation();
        const own = yield* work("own", a, a.id);
        yield* quiet;
        assert.deepInclude(idleState(h), { contexts: 1, idleContexts: 0, idleMismatches: 0 });
        yield* end("own", own);
        yield* quiet;
        assert.deepInclude(idleState(h), { contexts: 0, idleMismatches: 0 });
        yield* h.close;
      }),
    ),
  );
});

/** A random source with a fixed seed. */
const seeded = (seed: number): (() => number) => {
  let state = seed;
  return () => {
    state = (state * 1103515245 + 12345) % 2147483648;
    return state / 2147483648;
  };
};

describeEachBackend("incremental idle state", (backend) => {
  it.effect(
    "equals a full recompute after every step of random nested work",
    () =>
      run(backend)(
        Effect.gen(function* () {
          yield* reset;
          const random = seeded(42);
          const pick = <T>(items: ReadonlyArray<T>): T => {
            const item = items[Math.floor(random() * items.length)];
            if (item === undefined) throw new Error("pick from an empty list");
            return item;
          };
          const { h, root } = yield* open();
          // A context range keeps entries: every conversation gets one.
          yield* root.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              yield* tx.appendEntry(root.id, { kind: "note" });
            }),
          );
          const conversations: Array<ConversationId> = [root.id];
          const live = new Map<
            string,
            { readonly id: TaskId; readonly conversation: ConversationId }
          >();
          let count = 0;
          let fullChecks = idleState(h).fullIdleChecks;
          const handle = (id: ConversationId) =>
            Effect.map(h.harness.conversation(id), Option.getOrThrow);
          for (let step = 0; step < 200; step++) {
            const action = random();
            const name = `t${count++}`;
            if (action < 0.1) {
              const created = yield* h.harness.createConversation({
                ownership: { kind: "ownerless" },
              });
              yield* created.commit(
                Effect.gen(function* () {
                  const tx = yield* Tx;
                  yield* tx.appendEntry(created.id, { kind: "note" });
                }),
              );
              conversations.push(created.id);
            } else if (action < 0.45 || live.size === 0) {
              // A task in any conversation, also one whose owner chain ended and was swept; it may read a context.
              const into = pick(conversations);
              const read = random() < 0.6 ? pick(conversations) : undefined;
              const background = random() < 0.15;
              const input = read === undefined ? { name } : { name, read };
              const target = yield* handle(into);
              const id = yield* target.commit(
                Effect.gen(function* () {
                  const tx = yield* Tx;
                  return yield* tx.createTask(Work, input, {
                    ownership: { kind: "conversation" },
                    conversationId: into,
                    background,
                  });
                }),
              );
              live.set(name, { id, conversation: into });
              if (read !== undefined) yield* Deferred.await(named(reads, name));
            } else if (action < 0.6) {
              // Work a live task owns: a child task, or an owned conversation with a task that reads it.
              const [ownerName, owner] = pick(Array.from(live));
              const state = Option.getOrUndefined(yield* h.harness.getTask(owner.id))?.state.status;
              if (state === undefined || state === "terminal" || state === "completing") {
                live.delete(ownerName);
                continue;
              }
              const conversation = random() < 0.5;
              const created = yield* (yield* handle(owner.conversation)).commit(
                Effect.gen(function* () {
                  const tx = yield* Tx;
                  if (!conversation) {
                    const id = yield* tx.createTask(
                      Work,
                      { name },
                      { ownership: { kind: "task", taskId: owner.id } },
                    );
                    return { id, into: owner.conversation, read: false };
                  }
                  const child = yield* tx.createConversation({
                    ownership: { kind: "task", taskId: owner.id },
                  });
                  yield* tx.appendEntry(child.id, { kind: "note" });
                  const id = yield* tx.createTask(
                    Work,
                    { name, read: child.id },
                    { ownership: { kind: "conversation" }, conversationId: child.id },
                  );
                  return { id, into: child.id, read: true };
                }),
              );
              if (created.read) conversations.push(created.into);
              live.set(name, { id: created.id, conversation: created.into });
              if (created.read) yield* Deferred.await(named(reads, name));
            } else {
              const [ended] = pick(Array.from(live));
              live.delete(ended);
              yield* openGate(ended);
            }
            yield* quiet;
            const state = idleState(h);
            assert.strictEqual(state.idleMismatches, 0, `step ${step}`);
            fullChecks = state.fullIdleChecks;
          }
          const state = idleState(h);
          assert.isAbove(state.contexts, 5);
          assert.isAbove(state.idleContexts, 0);
          assert.isBelow(state.idleContexts, state.contexts);
          assert.strictEqual(fullChecks, 0);
          for (const gate of gates.values()) yield* Deferred.succeed(gate, undefined);
          yield* h.close;
        }),
      ),
    { timeout: 60_000 },
  );
});

// Each case checks `wake()` reduced to `{ active, at }` through `reducedWake` (as `HarnessSleep` does). Where work can
// run now, `nextWakeAt` is now, so `at` (the earliest sleep) is not reported alongside it.
const reducedWake = (wake: Wake, now: number) => {
  const next = Option.getOrUndefined(wake.nextWakeAt);
  const active = wake.activeWork || (next !== undefined && next <= now);
  return next !== undefined && next > now ? { active, at: next } : { active };
};

const seededTask = (
  id: number,
  state: TaskRecord["state"],
  extra: Partial<Pick<TaskRecord, "abortRequested" | "owner" | "kind">> = {},
): StorageWrite => ({
  type: "task",
  // SAFETY: a seeded record of `Work`, complete but for the fields the case sets.
  value: {
    id: TaskId.make(id),
    conversationId: ROOT_CONVERSATION_ID,
    kind: Work.definition.name,
    version: 1,
    input: { name: `seeded${id}` },
    state,
    background: false,
    abortRequested: false,
    ...extra,
  } as TaskRecord,
});
const sleeping = (until: number): TaskRecord["state"] => ({
  status: "sleeping",
  checkpoint: { phase: "run" },
  until,
});
const SLEEPS = seededTask(10, sleeping(5_000));

describeEachBackend("wake()", (backend) => {
  const cases: ReadonlyArray<
    readonly [
      string,
      ReadonlyArray<StorageWrite>,
      { readonly active: boolean; readonly at?: number },
    ]
  > = [
    ["a sleeping task", [SLEEPS], { active: false, at: 5_000 }],
    [
      "an abort-marked sleeping task, which runs its handler",
      [SLEEPS, seededTask(11, sleeping(3_000), { abortRequested: true })],
      // Work can run now, so the earliest sleep (5_000) is not reported.
      { active: true },
    ],
    ["a sleeping task that is due", [seededTask(11, sleeping(500))], { active: true }],
    [
      "a task waiting on a live one",
      [
        SLEEPS,
        seededTask(11, {
          status: "waiting",
          checkpoint: { phase: "run" },
          on: [TaskId.make(10)],
          policy: "allSettled",
        }),
      ],
      { active: false, at: 5_000 },
    ],
    [
      "a completing task holding for a sleeping child",
      [
        seededTask(11, { status: "completing", outcome: { status: "completed", result: null } }),
        seededTask(10, sleeping(5_000), { owner: TaskId.make(11) }),
      ],
      { active: false, at: 5_000 },
    ],
    [
      "a task no definition can take",
      [
        SLEEPS,
        seededTask(
          11,
          { status: "pending", checkpoint: { phase: "run" } },
          { kind: "test.idle.missing" },
        ),
      ],
      { active: false, at: 5_000 },
    ],
    [
      "a pending task",
      [SLEEPS, seededTask(11, { status: "pending", checkpoint: { phase: "run" } })],
      // Work can run now, so the earliest sleep (5_000) is not reported.
      { active: true },
    ],
  ];
  for (const [name, writes, expected] of cases) {
    it.effect(`with ${name}`, () =>
      run(backend)(
        Effect.gen(function* () {
          yield* reset;
          yield* TestClock.setTime(1_000);
          const storage = yield* controlledStorage;
          yield* commitWrites(storage, [
            { type: "conversation", value: { id: ROOT_CONVERSATION_ID } },
            ...writes,
          ]);
          const h = yield* openHarness(storage, [Work]);
          yield* quiet;
          assert.deepStrictEqual(reducedWake(yield* h.harness.wake, 1_000), expected);
          yield* h.close;
        }),
      ),
    );
  }

  it.effect("with an invocation running: active, and the earliest sleep still found", () =>
    run(backend)(
      Effect.gen(function* () {
        yield* reset;
        yield* TestClock.setTime(1_000);
        const storage = yield* controlledStorage;
        yield* commitWrites(storage, [
          { type: "conversation", value: { id: ROOT_CONVERSATION_ID } },
          SLEEPS,
        ]);
        const h = yield* openHarness(storage, [Work]);
        yield* h.harness.resume;
        const root = yield* h.harness.ensureRoot();
        const id = yield* root.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            return yield* tx.createTask(
              Work,
              { name: "running" },
              { ownership: { kind: "conversation" } },
            );
          }),
        );
        yield* eventually(statusOf(h, id).pipe(Effect.map((status) => status === "running")));
        assert.deepStrictEqual(reducedWake(yield* h.harness.wake, 1_000), {
          active: true,
          at: 5_000,
        });
        yield* openGate("running");
        yield* h.harness.awaitTask(id);
        yield* quiet;
        assert.deepStrictEqual(reducedWake(yield* h.harness.wake, 1_000), {
          active: false,
          at: 5_000,
        });
        yield* h.close;
      }),
    ),
  );
});

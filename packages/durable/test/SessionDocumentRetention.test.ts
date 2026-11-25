// Document retention, on every backend. The Session's document cache is
// bounded by use: unattached documents leave it after the retention period and
// load from storage again; documents with a watch or a document state never
// leave it.
//
// Time is `TestClock`; adjusting it also fires the sweep fiber's sleep. The
// retention is a synchronous reader called at each use. A platform without
// unreferenced timers is the host's `IdleTimers` switch: "keeps no timer where
// timers cannot be unreferenced" runs with it off.
//
// The two Harness cases read the Harness settings at each use, and compare
// conversation view frames.
import { assert, it } from "@effect/vitest";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as Fiber from "effect/Fiber";
import { TestClock } from "effect/testing";
import * as AgentTool from "../src/AgentTool.ts";
import type { StorageWrite } from "../src/AgentStorage.ts";
import * as Doc from "../src/Doc.ts";
import * as Extension from "../src/Extension.ts";
import type { ConversationId, EntryId } from "../src/Ids.ts";
import type { SessionKernel } from "../src/internal/session.ts";
import { LiveDoc, LiveState } from "../src/Live.ts";
import { fauxAssistantMessage, fauxText, fauxToolCall } from "../src/testing/FauxModel.ts";
import { IdleTimers } from "../src/IdleTimers.ts";
import { Tx } from "../src/Tx.ts";
import type { ConversationView } from "../src/View.ts";
import { chatSetup, openChat, waitFor } from "./ChatSupport.ts";
import {
  acquire,
  controlledStorage,
  createConversation,
  describeEachBackend,
  failureMessage,
  openSession,
  openTestSession,
  run,
} from "./SessionSupport.ts";

const Counter = Schema.Struct({ count: Schema.Number });

const CounterDoc = Doc.make({
  checkpointWhen: () => false,
  kind: "test.retention.counter",
  version: 1,
  scope: "conversation",
  history: "latest",
  fork: "initial",
  schema: Counter,
  initial: () => ({ count: 0 }),
});
const RewindableDoc = Doc.make({
  checkpointWhen: () => false,
  kind: "test.retention.rewindable",
  version: 1,
  scope: "conversation",
  history: "rewindable",
  fork: "asOf",
  schema: Counter,
  initial: () => ({ count: 0 }),
});
/** A third-party conversation family: one member per event. */
const EventDoc = Doc.family({
  checkpointWhen: () => false,
  kind: "test.retention.event",
  version: 1,
  scope: "conversation",
  history: "latest",
  fork: "initial",
  family: true,
  schema: Schema.Struct({ seq: Schema.Number }),
  initial: (seq: number) => ({ seq }),
});
const SessionCounterDoc = Doc.make({
  checkpointWhen: () => false,
  kind: "test.retention.session-counter",
  version: 1,
  scope: "session",
  schema: Counter,
  initial: () => ({ count: 0 }),
});

/** A session document whose checkpoint predicate throws, so the commit that changes it fails while it settles. */
const Failing = Doc.make({
  kind: "test.retention.failing",
  version: 1,
  scope: "session",
  schema: Counter,
  initial: () => ({ count: 0 }),
  checkpointWhen: () => {
    throw new Error("checkpoint failed");
  },
});

/** A conversation document any Harness test can read. */
const TestDoc = Doc.make({
  checkpointWhen: () => false,
  kind: "test.retention.harness",
  version: 1,
  scope: "conversation",
  history: "latest",
  fork: "initial",
  schema: Counter,
  initial: () => ({ count: 0 }),
});

/** The `core.live` document of a conversation view; absent before its first write. */
const liveOf = (view: ConversationView): LiveState =>
  Schema.decodeUnknownSync(LiveState)(view.docs[LiveDoc.definition.kind] ?? {});

/** A Session whose retention a test can change, on `TestClock`. */
const open = (retentionMs: number) =>
  Effect.gen(function* () {
    const retention = { ms: retentionMs };
    const harness = yield* openTestSession({
      documentRetention: () => Duration.millis(retention.ms),
    });
    return { ...harness, retention, sizes: () => harness.session.documentCacheSizes };
  });

/** Lets the line run what is queued on it: an eviction job, then returns. */
const settleLine = (session: SessionKernel) => session.readOnLine(Effect.void);

/** Polls `check` a bounded number of times, letting queued work run between tries. */
const eventually = <E>(check: Effect.Effect<boolean, E>) =>
  Effect.gen(function* () {
    for (let attempt = 0; attempt < 100; attempt++) {
      if (yield* check) return;
      yield* Effect.yieldNow;
    }
    assert.fail("condition not reached");
  });

const bump = (session: SessionKernel, conversationId: ConversationId, by = 1) =>
  session.commit(
    Effect.gen(function* () {
      const tx = yield* Tx;
      (yield* tx.doc(CounterDoc, conversationId)).count += by;
    }),
  );

const touchSessionCounter = (session: SessionKernel) =>
  session.commit(
    Effect.gen(function* () {
      const tx = yield* Tx;
      yield* tx.doc(SessionCounterDoc);
    }),
  );

const counter = (session: SessionKernel, conversationId: ConversationId) =>
  session.snapshot(CounterDoc, conversationId).pipe(Effect.map(Option.getOrUndefined));

describeEachBackend("document cache retention", (backend) => {
  it.effect(
    "evicts an unattached document once its retention passed, and loads it again with the same value",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const { session, storage, sizes } = yield* open(60_000);
          const conversationId = yield* createConversation(session);
          yield* bump(session, conversationId, 3);
          assert.isAbove(sizes().cached, 0);
          // Not yet: inside the retention period.
          yield* TestClock.adjust(Duration.millis(59_000));
          yield* bump(session, conversationId, 0);
          yield* settleLine(session);
          assert.isAbove(sizes().cached, 0);
          // Past it, with no use since.
          yield* TestClock.adjust(Duration.millis(61_000));
          yield* session.commit(Effect.void);
          yield* touchSessionCounter(session);
          yield* settleLine(session);
          const reads = storage.documentReadCount;
          assert.deepStrictEqual(yield* counter(session, conversationId), { count: 3 });
          assert.isAbove(storage.documentReadCount, reads);
          yield* session.close;
        }),
      ),
  );

  it.effect("keeps no timer where timers cannot be unreferenced, and evicts on commits", () =>
    run(backend)(
      Effect.gen(function* () {
        const { session, sizes } = yield* open(1_000);
        const conversationId = yield* createConversation(session);
        yield* bump(session, conversationId);
        yield* bump(session, conversationId);
        assert.strictEqual(sizes().cached, 1);
        // No sweep is pending: time alone evicts nothing.
        yield* TestClock.adjust(Duration.millis(10_000));
        yield* settleLine(session);
        assert.strictEqual(sizes().cached, 1);
        // The end of the next commit evicts what expired.
        yield* session.commit(Effect.void);
        yield* settleLine(session);
        assert.strictEqual(sizes().cached, 0);
        yield* session.close;
      }).pipe(Effect.provideService(IdleTimers, { enabled: false })),
    ),
  );

  it.effect("evicts with retention 0 right after use, on the line", () =>
    run(backend)(
      Effect.gen(function* () {
        const { session, sizes } = yield* open(0);
        const conversationId = yield* createConversation(session);
        yield* bump(session, conversationId);
        yield* eventually(settleLine(session).pipe(Effect.map(() => sizes().cached === 0)));
        assert.deepStrictEqual(yield* counter(session, conversationId), { count: 1 });
        yield* session.close;
      }),
    ),
  );

  it.effect(
    "never evicts a document a watch observes, and evicts it once the last watch detaches",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const { session, storage, sizes } = yield* open(1_000);
          const conversationId = yield* createConversation(session);
          yield* bump(session, conversationId);
          const watch = yield* acquire(session.watchDoc(CounterDoc, conversationId));
          const kept = yield* acquire(session.watchDoc(CounterDoc, conversationId));
          const keptConsumer = yield* kept.value.frames.pipe(Stream.runDrain, Effect.forkChild);
          const seen: Array<number> = [];
          const consumer = yield* watch.value.frames.pipe(
            Stream.runForEach((frame) => Effect.sync(() => seen.push(frame.value.count))),
            Effect.forkChild,
          );
          yield* TestClock.adjust(Duration.millis(1_000_000));
          yield* bump(session, conversationId);
          yield* settleLine(session);
          // Still cached: a read long after its retention passed loads nothing from storage.
          yield* eventually(Effect.sync(() => seen.includes(2) && kept.value.value.count === 2));
          const reads = storage.documentReadCount;
          yield* TestClock.adjust(Duration.millis(1_000_000));
          yield* touchSessionCounter(session);
          yield* settleLine(session);
          assert.deepStrictEqual(yield* counter(session, conversationId), { count: 2 });
          assert.strictEqual(storage.documentReadCount, reads);
          yield* watch.release;
          yield* Fiber.join(consumer);
          yield* TestClock.adjust(Duration.millis(1_000_000));
          yield* bump(session, conversationId);
          yield* settleLine(session);
          yield* eventually(Effect.sync(() => kept.value.value.count === 3));
          yield* kept.release;
          yield* Fiber.join(keptConsumer);
          yield* TestClock.adjust(Duration.millis(1_000_000));
          yield* session.commit(Effect.void);
          yield* touchSessionCounter(session);
          yield* settleLine(session);
          // The session document was just used; the counter is gone.
          assert.strictEqual(sizes().cached, 1);
          yield* session.close;
        }),
      ),
  );

  it.effect("never takes a tracker from a transaction that holds it", () =>
    run(backend)(
      Effect.gen(function* () {
        const { session } = yield* open(0);
        const conversationId = yield* createConversation(session);
        yield* bump(session, conversationId);
        yield* session.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            const draft = yield* tx.doc(CounterDoc, conversationId);
            // Retention passes while the transaction holds the document; eviction waits on the line behind it.
            yield* TestClock.adjust(Duration.millis(1_000_000));
            draft.count += 10;
          }),
        );
        assert.deepStrictEqual(yield* counter(session, conversationId), { count: 11 });
        yield* bump(session, conversationId);
        assert.deepStrictEqual(yield* counter(session, conversationId), { count: 12 });
        yield* session.close;
      }),
    ),
  );

  it.effect("writes after a reload continue the stored deltas and bases", () =>
    run(backend)(
      Effect.gen(function* () {
        const { session, storage } = yield* open(0);
        const conversationId = yield* createConversation(session);
        const before = storage.commits.length;
        for (let n = 0; n < 5; n++) {
          yield* bump(session, conversationId);
          yield* settleLine(session);
        }
        assert.deepStrictEqual(yield* counter(session, conversationId), { count: 5 });
        // Every bump after the first is a change of the same incarnation, never a second creation.
        const creations = storage.commits
          .slice(before)
          .flat()
          .filter(
            (write: StorageWrite) =>
              write.type === "document.create" && write.record.kind === CounterDoc.definition.kind,
          );
        assert.lengthOf(creations, 1);
        yield* session.close;
        // And a fresh Session over the same storage reads the same value.
        const reopened = yield* openSession(storage, {
          documentRetention: () => Duration.zero,
        });
        assert.deepStrictEqual(yield* counter(reopened, conversationId), { count: 5 });
        yield* reopened.close;
      }),
    ),
  );

  it.effect("keeps history and forks of rewindable documents after eviction", () =>
    run(backend)(
      Effect.gen(function* () {
        const { session } = yield* open(0);
        const conversationId = yield* createConversation(session);
        const write = (count: number) =>
          session.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              (yield* tx.doc(RewindableDoc, conversationId)).count = count;
              return (yield* tx.appendEntry(conversationId, { kind: "note", data: null })).id;
            }),
          );
        const first = yield* write(1);
        yield* settleLine(session);
        const second = yield* write(2);
        yield* settleLine(session);
        const asOf = (at: typeof first) =>
          session
            .snapshotAsOf(RewindableDoc, conversationId, at)
            .pipe(Effect.map(Option.getOrUndefined));
        assert.deepStrictEqual(yield* asOf(first), { count: 1 });
        assert.deepStrictEqual(yield* asOf(second), { count: 2 });
        const fork = yield* session.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            return (yield* tx.forkConversation(conversationId, first, {
              ownership: { kind: "ownerless" },
            })).id;
          }),
        );
        yield* settleLine(session);
        assert.deepStrictEqual(
          Option.getOrUndefined(yield* session.snapshot(RewindableDoc, fork)),
          { count: 1 },
        );
        assert.deepStrictEqual(
          Option.getOrUndefined(yield* session.snapshot(RewindableDoc, conversationId)),
          {
            count: 2,
          },
        );
        yield* session.close;
      }),
    ),
  );

  it.effect("bounds a third-party family with a member per event", () =>
    run(backend)(
      Effect.gen(function* () {
        const { session, sizes } = yield* open(0);
        const conversationId = yield* createConversation(session);
        for (let seq = 0; seq < 200; seq++) {
          yield* session.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              yield* tx.doc(EventDoc, conversationId, String(seq), seq);
            }),
          );
        }
        yield* eventually(settleLine(session).pipe(Effect.map(() => sizes().cached === 0)));
        assert.deepStrictEqual(
          Option.getOrUndefined(yield* session.snapshot(EventDoc, conversationId, "123")),
          {
            seq: 123,
          },
        );
        yield* session.close;
      }),
    ),
  );

  it.effect("keeps the cache of a long-lived Session bounded across many conversations", () =>
    run(backend)(
      Effect.gen(function* () {
        const { session, sizes } = yield* open(10_000);
        for (let n = 0; n < 300; n++) {
          const conversationId = yield* createConversation(session);
          yield* bump(session, conversationId);
          yield* TestClock.adjust(Duration.millis(1_000));
        }
        yield* settleLine(session);
        // Only what was used in the last 10 s, about ten conversations' worth.
        assert.isAtMost(sizes().cached, 12);
        yield* session.close;
      }),
    ),
  );

  it.effect("evicts after a commit whose settlement fails", () =>
    run(backend)(
      Effect.gen(function* () {
        const { session, sizes } = yield* open(10_000);
        const conversationId = yield* createConversation(session);
        yield* bump(session, conversationId);
        // Created first: the predicate runs only for changes to an existing document.
        yield* session.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            yield* tx.doc(Failing);
          }),
        );
        yield* settleLine(session);
        // Retention passes inside a commit whose checkpoint predicate throws while it settles.
        assert.include(
          yield* failureMessage(
            session.commit(
              Effect.gen(function* () {
                const tx = yield* Tx;
                (yield* tx.doc(Failing)).count++;
                yield* TestClock.adjust(Duration.millis(1_000_000));
              }),
            ),
          ),
          "checkpoint failed",
        );
        assert.strictEqual(sizes().cached, 0);
        yield* session.close;
      }),
    ),
  );

  it.effect("evicts promptly once the retention is shortened", () =>
    run(backend)(
      Effect.gen(function* () {
        const { session, retention, sizes } = yield* open(600_000);
        const conversationId = yield* createConversation(session);
        yield* bump(session, conversationId);
        retention.ms = 0;
        yield* TestClock.adjust(Duration.millis(2_000));
        // A read, no commit: the sweep pending for ten minutes from now moves up.
        yield* counter(session, conversationId);
        yield* eventually(settleLine(session).pipe(Effect.map(() => sizes().cached === 0)));
        yield* session.close;
      }),
    ),
  );

  it.effect(
    "pins only what is at the address a watch follows: nothing while retired, then the new incarnation",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const { session, sizes } = yield* open(0);
          const conversationId = yield* createConversation(session);
          yield* bump(session, conversationId);
          const watch = yield* acquire(session.watchDoc(CounterDoc, conversationId));
          const consumer = yield* watch.value.frames.pipe(Stream.runDrain, Effect.forkChild);
          yield* session.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              yield* tx.retireDoc(CounterDoc, conversationId);
            }),
          );
          yield* eventually(settleLine(session).pipe(Effect.map(() => sizes().cached === 0)));
          assert.strictEqual(sizes().attached, 0);
          yield* bump(session, conversationId, 5);
          yield* settleLine(session);
          assert.deepInclude(sizes(), { cached: 1, attached: 1 });
          yield* eventually(Effect.sync(() => watch.value.value.count === 5));
          yield* watch.release;
          yield* Fiber.join(consumer);
          yield* eventually(settleLine(session).pipe(Effect.map(() => sizes().cached === 0)));
          yield* session.close;
        }),
      ),
  );

  it.effect("leaves no pin behind when an observer's load loses a race with close", () =>
    run(backend)(
      Effect.gen(function* () {
        const { session, storage, sizes } = yield* open(0);
        const conversationId = yield* createConversation(session);
        yield* bump(session, conversationId);
        yield* eventually(settleLine(session).pipe(Effect.map(() => sizes().cached === 0)));
        const gate = yield* storage.holdFindDocument;
        const attaching = yield* acquire(session.watchDoc(CounterDoc, conversationId)).pipe(
          Effect.forkChild,
        );
        yield* gate.entered;
        const closing = yield* session.close.pipe(Effect.forkChild({ startImmediately: true }));
        yield* gate.release;
        assert.include(yield* failureMessage(Fiber.join(attaching)), "Session is closed");
        yield* Fiber.join(closing);
        assert.deepInclude(sizes(), { cached: 0, idle: 0, attached: 0 });
      }),
    ),
  );

  it.effect("evicts on its timer without a commit, and tolerates a stop called twice", () =>
    run(backend)(
      Effect.gen(function* () {
        const { session, sizes } = yield* open(20);
        const conversationId = yield* createConversation(session);
        yield* bump(session, conversationId);
        const watch = yield* acquire(session.watchDoc(CounterDoc, conversationId));
        yield* watch.release;
        yield* watch.release;
        yield* TestClock.adjust(Duration.millis(20));
        yield* eventually(Effect.sync(() => sizes().cached === 0));
        yield* session.close;
      }),
    ),
  );

  it.effect("migrates a document again after its eviction", () =>
    run(backend)(
      Effect.gen(function* () {
        const V1 = Doc.make({
          checkpointWhen: () => false,
          kind: "test.retention.migrating",
          version: 1,
          scope: "session",
          schema: Counter,
          initial: () => ({ count: 1 }),
        });
        let migrations = 0;
        const V2 = Doc.make({
          checkpointWhen: () => false,
          kind: "test.retention.migrating",
          version: 2,
          scope: "session",
          schema: Schema.Struct({ count: Schema.Number, migrated: Schema.Boolean }),
          initial: () => ({ count: 0, migrated: true }),
          migrate: (value) => {
            migrations++;
            return { count: Number(value["count"]), migrated: true };
          },
        });
        const { session } = yield* open(0);
        yield* session.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            yield* tx.doc(V1);
          }),
        );
        yield* settleLine(session);
        assert.deepStrictEqual(Option.getOrUndefined(yield* session.snapshot(V2)), {
          count: 1,
          migrated: true,
        });
        yield* settleLine(session);
        assert.deepStrictEqual(Option.getOrUndefined(yield* session.snapshot(V2)), {
          count: 1,
          migrated: true,
        });
        // Not written back, so each cold load migrates again.
        assert.strictEqual(migrations, 2);
        yield* session.close;
      }),
    ),
  );

  it.effect("treats a negative retention as the default, and an infinite one as never", () =>
    run(backend)(
      Effect.gen(function* () {
        // `NaN` and non-numbers are not checked: a `Duration` cannot carry either (`Duration.millis(NaN)` is zero).
        {
          const { session, sizes } = yield* open(-5);
          const conversationId = yield* createConversation(session);
          yield* bump(session, conversationId);
          yield* TestClock.adjust(Duration.millis(60_000));
          yield* session.commit(Effect.void);
          yield* settleLine(session);
          // Not dropped at once, as a retention of 0 would: the ten-minute default holds it.
          assert.strictEqual(sizes().cached, 1);
          yield* session.close;
        }
        const { session, sizes } = yield* open(Number.POSITIVE_INFINITY);
        const conversationId = yield* createConversation(session);
        yield* bump(session, conversationId);
        yield* TestClock.adjust(Duration.millis(1e15));
        yield* session.commit(Effect.void);
        yield* settleLine(session);
        assert.strictEqual(sizes().cached, 1);
        yield* session.close;
      }),
    ),
  );

  // The Harness reads the settings reader at each use, so changing `documentRetentionMs` takes effect at the
  // next use; an absent field is the ten-minute default.
  it.effect("follows a Harness settings change at the next use", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        const { harness, root } = yield* openChat(yield* controlledStorage, setup);
        yield* root.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            (yield* tx.doc(TestDoc, root.id)).count++;
          }),
        );
        const sizesOf = () => harness.kernel.documentCacheSizes;
        assert.isAbove(sizesOf().cached, 0);
        setup.settings.documentRetentionMs = 0;
        yield* harness.session.snapshot(TestDoc, root.id);
        yield* eventually(
          settleLine(harness.kernel).pipe(Effect.map(() => sizesOf().cached === 0)),
        );
        yield* harness.close;
      }),
    ),
  );

  // A watch of the conversation view collects the deduplicated sequence of appended entry kinds and
  // `core.live.tools[0].output`, the frames a tool's progress and its entries show up in. The tool sleeps 5 ms
  // between steps on the `TestClock`, which `waitFor` advances.
  it.effect(
    "gives the same views with documents evicted right after use as with documents kept",
    () =>
      Effect.gen(function* () {
        // Each run over a fresh storage of the backend.
        const runOnce = (documentRetentionMs: number) =>
          run(backend)(
            Effect.gen(function* () {
              const setup = chatSetup();
              setup.settings.documentRetentionMs = documentRetentionMs;
              setup.settings.progress = { outputIntervalMs: 0 };
              const steps = AgentTool.make("steps", {
                description: "Streams three steps",
                parameters: Schema.Struct({}),
                execute: () =>
                  Effect.gen(function* () {
                    const api = yield* AgentTool.ToolContext;
                    for (const step of ["a\n", "b\n", "c\n"]) {
                      yield* api.output(step);
                      yield* Effect.sleep(Duration.millis(5));
                    }
                    return {};
                  }),
              });
              setup.faux.setResponses([
                fauxAssistantMessage([fauxToolCall("steps", {}, { id: "s1" })], {
                  stopReason: "toolUse",
                }),
                fauxAssistantMessage([fauxText("done")]),
              ]);
              const { harness, root } = yield* openChat(yield* controlledStorage, setup, {
                extensions: [Extension.make({ name: "steps", tools: [steps] })],
              });
              const watch = yield* root.watch();
              // Ids and times differ between runs; the sequence of entry kinds and tool outputs must not.
              const events: Array<string> = [];
              const seen = new Set<EntryId>();
              let lastOutput: string | undefined;
              let last: ConversationView | undefined;
              const record = (view: ConversationView) => {
                last = view;
                for (const entry of view.entries) {
                  if (seen.has(entry.id)) continue;
                  seen.add(entry.id);
                  events.push(entry.kind);
                }
                const output = liveOf(view).tools?.[0]?.output;
                if (output !== undefined && output !== lastOutput) events.push(`update:${output}`);
                lastOutput = output;
              };
              record(watch.value);
              const consumer = yield* watch.frames.pipe(
                Stream.runForEach((frame) => Effect.sync(() => record(frame.value))),
                Effect.forkScoped,
              );
              const submission = yield* root.submit({ type: "input", content: "go" });
              const settled = yield* Effect.forkChild(submission.await, {
                startImmediately: true,
              });
              yield* waitFor(Effect.sync(() => settled.pollUnsafe() !== undefined));
              yield* Fiber.join(settled);
              yield* root.awaitIdle;
              // The consumer has every frame once it holds the view's latest value.
              const newest = Option.getOrUndefined(yield* Stream.runHead(root.entries()));
              yield* waitFor(
                Effect.sync(
                  () =>
                    last !== undefined &&
                    liveOf(last).run === undefined &&
                    last.entries.at(-1)?.id === newest?.id,
                ),
              );
              yield* Fiber.interrupt(consumer);
              yield* harness.close;
              return events;
            }),
          );
        const evicted = yield* runOnce(0);
        const kept = yield* runOnce(600_000);
        assert.deepStrictEqual(evicted, kept);
        // Not vacuous: the tool's output reached the view step by step.
        assert.includeMembers(evicted, ["update:a\n", "update:a\nb\n", "update:a\nb\nc\n"]);
      }),
  );

  it.effect(
    "keeps a reloaded newer version cached while an observer of the older one is attached",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const V1 = Doc.make({
            checkpointWhen: () => false,
            kind: "test.retention.versioned",
            version: 1,
            scope: "session",
            schema: Counter,
            initial: () => ({ count: 3 }),
          });
          const V2 = Doc.make({
            checkpointWhen: () => false,
            kind: "test.retention.versioned",
            version: 2,
            scope: "session",
            schema: Schema.Struct({ count: Schema.Number, v2: Schema.Literal(true) }),
            initial: () => ({ count: 0, v2: true }),
            migrate: (value) => ({ count: Number(value["count"]), v2: true }),
          });
          const { session, sizes } = yield* open(0);
          yield* session.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              yield* tx.doc(V1);
            }),
          );
          const watch = yield* acquire(session.watchDoc(V1));
          assert.deepStrictEqual(Option.getOrUndefined(yield* session.snapshot(V2)), {
            count: 3,
            v2: true,
          });
          yield* settleLine(session);
          assert.strictEqual(sizes().cached, 1);
          yield* watch.release;
          yield* eventually(settleLine(session).pipe(Effect.map(() => sizes().cached === 0)));
          yield* session.close;
        }),
      ),
  );

  it.effect("queues one sweep job, not one per document, while a commit holds the line", () =>
    run(backend)(
      Effect.gen(function* () {
        const { session, sizes } = yield* open(0);
        const conversationIds: Array<ConversationId> = [];
        for (let n = 0; n < 50; n++) {
          const id = yield* createConversation(session);
          yield* bump(session, id);
          conversationIds.push(id);
        }
        yield* eventually(settleLine(session).pipe(Effect.map(() => sizes().cached === 0)));
        const before = sizes().sweeps;
        // One commit loads 50 evicted documents from storage, each touched as it loads; the sweep job
        // stays pending while the line is held.
        yield* session.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            for (const id of conversationIds) {
              (yield* tx.doc(CounterDoc, id)).count++;
              yield* Effect.yieldNow;
            }
          }),
        );
        yield* settleLine(session);
        assert.strictEqual(sizes().cached, 0);
        assert.isAtMost(sizes().sweeps - before, 2);
        yield* session.close;
      }),
    ),
  );
});

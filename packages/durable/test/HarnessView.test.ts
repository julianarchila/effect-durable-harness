// Conversation views and their watches, on every backend.
//
// Notes:
// - A watch is consumed by a fiber over its `frames` stream; stopping it closes the acquisition's scope (and
//   interrupts the consumer).
// - A consumer ends when its stream ends, or with its own failure when it fails.
// - `flush` lets pending work run; an aborted request is the generation's interruption.
// - `snapshot` and `observeCommits` are on the `Session` service. `Conversation.observe` attaches through the
//   internal `views.attach`, whose observer is made from the window's first value.
// - An entries page of `size + 1` is the first `size + 1` of the `entries` stream.
// - A bad window is a `RangeError` defect of the acquisition (`resolveWindow`).
// - A consumer runs only once the committing fiber yields, so `note`, `notes` and the cases' back-to-back
//   commits let consumers catch up after each commit (`drained`). Without it, those commits fold into one frame.
//
// Cases:
// - "ends watches at close and rejects later acquisition": the watch's stream ends, its value stays readable,
//   and later acquisitions fail.
// - "rejects an acquisition cancelled or closed while it waits for the Session line": cancelling is interrupting
//   the acquiring fiber; a close makes the queued acquisition fail with `SessionClosed`.
// - "shares one mount between concurrent observers and isolates a failing listener": the failing consumer ends its
//   own stream with its failure.
// - "lets a long-lived watch drop the entries that left its window": runs on the `sqlite` backend (`:memory:`,
//   which keeps no entry objects) and is skipped on `memory`, whose storage holds every entry object. The SQLite
//   client's statement cache keeps the first commit that appends an entry alive through its spans, so an entry of
//   another conversation is written first; the controlled storage's recorded batches are cleared before collecting.
import { setFlagsFromString } from "node:v8";
import { runInNewContext } from "node:vm";
import { assert, it } from "@effect/vitest";
import * as Delta from "@effect-durable-harness/delta/Delta";
import * as Cause from "effect/Cause";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import { AgentDoc } from "../src/Agent.ts";
import * as Doc from "../src/Doc.ts";
import type { WatchFrame } from "../src/Doc.ts";
import type { Conversation } from "../src/Harness.ts";
import type { EntryId } from "../src/Ids.ts";
import { InboxDoc } from "../src/Inbox.ts";
import { LiveDoc } from "../src/Live.ts";
import type { AssistantMessage } from "../src/Message.ts";
import { ProviderDoc } from "../src/ProviderSession.ts";
import type { EntryRecord } from "../src/Records.ts";
import { fauxAssistantMessage, fauxText } from "../src/testing/FauxModel.ts";
import type { SessionFailed } from "../src/SessionError.ts";
import { Tx } from "../src/Tx.ts";
import { UsageDoc } from "../src/Usage.ts";
import type { ConversationView, ConversationWindow } from "../src/View.ts";
import { allEntries, chatSetup, openChat, waitFor } from "./ChatSupport.ts";
import { awaitBounded, joined, type TestHarness } from "./HarnessSupport.ts";
import {
  controlledStorage,
  describeEachBackend,
  failureMessage,
  flush,
  run,
} from "./SessionSupport.ts";
import { serialTimeout } from "./Timeouts.ts";

interface Frame {
  readonly value: ConversationView;
  readonly ops: Delta.Delta;
}

const MOUNTED = ["core.agent", "core.inbox", "core.live", "core.provider", "core.usage"];

/** Acquires in a scope of its own; closing it stops the watch. */
const acquire = <A, E>(open: Effect.Effect<A, E, Scope.Scope>) =>
  Effect.gen(function* () {
    const scope = yield* Scope.make();
    const value = yield* Scope.provide(open, scope);
    return { value, release: Scope.close(scope, Exit.void) };
  });

/** Starts a consumer of `frames` that records every delivered frame. */
const consume = (frames: Stream.Stream<WatchFrame<ConversationView>, SessionFailed>) =>
  Effect.gen(function* () {
    const recorded: Array<Frame> = [];
    const consumer = yield* Effect.forkChild(
      Stream.runForEach(frames, (frame) =>
        Effect.sync(() => {
          recorded.push({ value: frame.value, ops: frame.ops });
        }),
      ),
      { startImmediately: true },
    );
    return { recorded, consumer };
  });

/** Starts a watch of `conversation` that records its acquisition revision and every delivered frame. */
const record = (conversation: Conversation, window?: ConversationWindow) =>
  Effect.gen(function* () {
    const { value: watch, release } = yield* acquire(conversation.watch(window));
    const initial = watch.value;
    const { recorded, consumer } = yield* consume(watch.frames);
    return {
      initial,
      frames: recorded,
      stop: Fiber.interrupt(consumer).pipe(Effect.andThen(release)),
    };
  });

/** A freshly built view of `conversation`, for comparing with an advanced one. */
const fresh = (conversation: Conversation, window?: ConversationWindow) =>
  Effect.gen(function* () {
    const { value: watch, release } = yield* acquire(conversation.watch(window));
    const value = watch.value;
    yield* release;
    return value;
  });

/**
 * The view as committed state defines it, read without any mount: the newest `size` entries, the entry before
 * them, and the built-in documents.
 */
const committed = (
  h: TestHarness,
  conversation: Conversation,
  record: ConversationView["conversation"],
  size: number,
) =>
  Effect.gen(function* () {
    const docs: { [kind: string]: Schema.JsonObject } = {};
    for (const token of [AgentDoc, LiveDoc, InboxDoc, ProviderDoc, UsageDoc] as const) {
      // SAFETY: each built-in token is read the same way; only its value type differs.
      const value = yield* h.session.snapshot(token as typeof LiveDoc, conversation.id);
      if (Option.isSome(value)) docs[token.definition.kind] = value.value;
    }
    const newest = Array.from(
      yield* Stream.runCollect(conversation.entries().pipe(Stream.take(size + 1))),
    );
    const before = newest.length > size ? newest.pop()?.id : undefined;
    const entries = newest.toReversed();
    const view: ConversationView =
      before === undefined
        ? { conversation: record, entries, docs }
        : { conversation: record, entries, before, docs };
    return view;
  });

/** Replays every frame's operations from `initial`, checking each delivered revision on the way. */
const replay = (initial: ConversationView, frames: ReadonlyArray<Frame>): ConversationView => {
  let value = initial;
  for (const frame of frames) {
    value = Delta.applyImmutableUnsafe(value, frame.ops);
    assert.deepStrictEqual(value, frame.value);
  }
  return value;
};

const answer = (text: string): AssistantMessage => fauxAssistantMessage([fauxText(text)]);

/** Lets watch consumers, which run after the commit, catch up. */
const drained = flush;

const touches = (h: TestHarness, conversation: Conversation) =>
  Effect.gen(function* () {
    const counter = { count: 0 };
    yield* h.session.observeCommits({
      advance: (publication) => {
        const touched = publication.changes.some(
          (change) =>
            (change.type === "entry" && change.value.conversationId === conversation.id) ||
            (change.type === "document" &&
              change.conversationId === conversation.id &&
              MOUNTED.includes(change.record.kind) &&
              change.ops.length > 0),
        );
        if (touched) counter.count++;
      },
      end: () => {},
    });
    return counter;
  });

/**
 * Appends one entry of `kind` in its own commit, then lets watch consumers catch up. A consumer runs only once
 * the committing fiber yields, and commits made back to back would fold into one frame.
 */
const note = (conversation: Conversation, kind: string) =>
  conversation
    .commit(
      Effect.gen(function* () {
        const tx = yield* Tx;
        return yield* tx.appendEntry(conversation.id, { kind });
      }),
    )
    .pipe(Effect.tap(() => drained));

const kinds = (view: ConversationView) => view.entries.map((entry) => entry.kind);

/** An observer that ignores what it is given. */
const recorder = () => ({ advance: () => {}, closeSession: () => {} });

/** The acquisition's defect, which a bad window is (a `RangeError`). */
const defectOf = <A, E>(open: Effect.Effect<A, E, Scope.Scope>) =>
  Effect.gen(function* () {
    const exit = yield* Effect.exit(Effect.scoped(open));
    return Exit.isFailure(exit) ? Cause.squash(exit.cause) : undefined;
  });

describeEachBackend("conversation view", (backend) => {
  it.effect("hydrates the newest entries and the built-in documents", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        setup.faux.setResponses([answer("hello")]);
        const { harness, root } = yield* openChat(yield* controlledStorage, setup);
        yield* (yield* root.submit({ type: "input", content: "hi" })).await;
        const view = yield* fresh(root);
        assert.deepStrictEqual(view.conversation, { id: root.id });
        assert.deepStrictEqual(view.entries, yield* allEntries(root));
        assert.isUndefined(view.before);
        assert.deepStrictEqual(Object.keys(view.docs).toSorted(), MOUNTED);
        assert.deepStrictEqual(view.docs["core.live"], {});
        assert.deepStrictEqual(view.docs["core.inbox"], { items: [] });
        yield* harness.close;
      }),
    ),
  );

  it.effect(
    "publishes one frame per touching commit, whose operations rebuild every revision",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const setup = chatSetup();
          const release = yield* Deferred.make<void>();
          // The generation's interruption ends the wait.
          setup.faux.setResponses([
            () => Deferred.await(release).pipe(Effect.as(answer("a longer answer"))),
          ]);
          const { harness, root } = yield* openChat(yield* controlledStorage, setup);
          const { initial, frames, stop } = yield* record(root);
          const touching = yield* touches(harness, root);
          const submission = yield* root.submit({ type: "input", content: "hi" });
          yield* waitFor(
            Effect.sync(() =>
              frames.some((frame) => {
                const live: { readonly generation?: unknown } = frame.value.docs["core.live"] ?? {};
                return live.generation !== undefined;
              }),
            ),
          );
          yield* Deferred.succeed(release, undefined);
          yield* submission.await;
          yield* harness.harness.awaitIdle;
          yield* drained;
          // One frame per stored commit, and one for the attempt, a deferred commit stored with the answer.
          assert.lengthOf(frames, touching.count + 1);
          const last = replay(initial, frames);
          assert.deepStrictEqual(
            last,
            yield* committed(harness, root, initial.conversation, last.entries.length),
          );
          const splice = frames[0]!.ops.find(
            (op) => op[0] === "p" && op[1].length === 1 && op[1][0] === "entries",
          );
          assert.isDefined(splice);
          assert.strictEqual(splice![0] === "p" ? splice![2] : undefined, 0);
          assert.strictEqual(splice![0] === "p" ? splice![3] : undefined, 0);
          const inserted = splice![0] === "p" ? splice![4] : [];
          assert.lengthOf(inserted, 1);
          assert.deepInclude(inserted[0], { kind: "core.user" });
          // Document operations keep their exact shape under the mount path.
          assert.deepInclude(
            frames.flatMap((frame) => frame.ops),
            ["s", ["docs", "core.live", "generation"], { attempt: 1 }],
          );
          yield* stop;
          yield* harness.close;
        }),
      ),
  );

  it.effect(
    "shares unchanged parts between revisions and skips commits that touch nothing mounted",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const { harness, root } = yield* openChat(yield* controlledStorage, chatSetup());
          const other = yield* harness.harness.createConversation({
            ownership: { kind: "ownerless" },
          });
          const { initial, frames, stop } = yield* record(root);
          yield* note(other, "note");
          yield* root.configure({ thinkingLevel: "high" });
          // See `note`.
          yield* drained;
          yield* note(root, "note");
          yield* drained;
          assert.lengthOf(frames, 2);
          assert.deepStrictEqual(frames[0]!.ops, [
            ["s", ["docs", "core.agent", "thinkingLevel"], "high"],
          ]);
          assert.strictEqual(frames[0]!.value.entries, initial.entries);
          assert.strictEqual(frames[0]!.value.docs["core.live"], initial.docs["core.live"]);
          assert.strictEqual(frames[1]!.value.docs, frames[0]!.value.docs);
          yield* stop;
          yield* harness.close;
        }),
      ),
  );

  it.effect("keeps head markers in place: a summary and a reset are appended entries", () =>
    run(backend)(
      Effect.gen(function* () {
        const { harness, root } = yield* openChat(yield* controlledStorage, chatSetup());
        yield* note(root, "a");
        const b = yield* note(root, "b");
        yield* note(root, "c");
        const { initial, frames, stop } = yield* record(root);
        const summary = yield* root.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            return yield* tx.appendEntry(root.id, { kind: "summary", head: b.id });
          }),
        );
        // See `note`.
        yield* drained;
        yield* note(root, "d");
        yield* root.reset(undefined);
        yield* drained;
        assert.deepStrictEqual(
          frames.map((frame) => kinds(frame.value)),
          [
            ["a", "b", "c", "summary"],
            ["a", "b", "c", "summary", "d"],
            ["a", "b", "c", "summary", "d", "core.reset"],
          ],
        );
        assert.deepStrictEqual(frames[0]!.ops, [["p", ["entries"], 3, 0, [summary]]]);
        // The summary's `head` tells a renderer where the entries it summarizes end.
        assert.strictEqual(frames[0]!.value.entries[3]!.head, b.id);
        const last = replay(initial, frames);
        assert.deepStrictEqual(last, yield* committed(harness, root, initial.conversation, 6));
        yield* stop;
        yield* harness.close;
      }),
    ),
  );

  it.effect("shows a fork's inherited entries in ID order, its head markers in place", () =>
    run(backend)(
      Effect.gen(function* () {
        const { harness, root } = yield* openChat(yield* controlledStorage, chatSetup());
        const a = yield* note(root, "a");
        const b = yield* note(root, "b");
        const fork = yield* root.fork(b.id, { ownership: { kind: "ownerless" } });
        const { initial, frames, stop } = yield* record(fork);
        yield* fork.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            yield* tx.appendEntry(fork.id, { kind: "summary", head: b.id });
          }),
        );
        yield* drained;
        assert.deepStrictEqual(
          initial.entries.map((entry) => entry.id),
          [a.id, b.id],
        );
        assert.deepStrictEqual(kinds(frames[0]!.value), ["a", "b", "summary"]);
        assert.deepStrictEqual(
          frames[0]!.value,
          yield* committed(harness, fork, initial.conversation, 3),
        );
        yield* stop;
        yield* harness.close;
      }),
    ),
  );

  it.effect("shows a fork's inherited entries and follows only the fork's own commits", () =>
    run(backend)(
      Effect.gen(function* () {
        const { harness, root } = yield* openChat(yield* controlledStorage, chatSetup());
        const first = yield* note(root, "first");
        const fork = yield* root.fork(first.id, { ownership: { kind: "ownerless" } });
        const { initial, frames, stop } = yield* record(fork);
        assert.deepStrictEqual(kinds(initial), ["first"]);
        assert.deepStrictEqual(initial.conversation.parent, {
          conversationId: root.id,
          at: first.id,
        });
        yield* note(root, "parent");
        yield* note(fork, "child");
        yield* drained;
        assert.deepStrictEqual(
          frames.map((frame) => kinds(frame.value)),
          [["first", "child"]],
        );
        yield* stop;
        yield* harness.close;
      }),
    ),
  );

  it.effect("unmounts a retired document and mounts its recreation whole", () =>
    run(backend)(
      Effect.gen(function* () {
        const { harness, root } = yield* openChat(yield* controlledStorage, chatSetup());
        const { frames, stop } = yield* record(root);
        yield* root.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            yield* tx.retireDoc(LiveDoc, root.id);
          }),
        );
        // See `note`.
        yield* drained;
        yield* root.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            (yield* tx.doc(LiveDoc, root.id)).tools = [];
          }),
        );
        yield* drained;
        assert.deepStrictEqual(
          frames.map((frame) => frame.ops),
          [[["d", ["docs", "core.live"]]], [["s", ["docs", "core.live"], { tools: [] }]]],
        );
        assert.isUndefined(frames[0]!.value.docs["core.live"]);
        yield* stop;
        yield* harness.close;
      }),
    ),
  );

  it.effect("folds undelivered commits into one call whose ops reach the newest view", () =>
    run(backend)(
      Effect.gen(function* () {
        const { harness, root } = yield* openChat(yield* controlledStorage, chatSetup());
        const { value: watch, release } = yield* acquire(root.watch());
        const initial = watch.value;
        for (let index = 0; index < 101; index++) yield* note(root, "note");
        const { recorded: frames, consumer } = yield* consume(watch.frames);
        yield* drained;
        assert.lengthOf(frames, 1);
        assert.notStrictEqual(frames[0]!.ops[0]?.[0], "r");
        assert.deepStrictEqual(
          Delta.applyImmutableUnsafe(initial, frames[0]!.ops),
          frames[0]!.value,
        );
        // The default window took 100 appends, then dropped its oldest 51 at the 101st, down to 50.
        assert.deepStrictEqual(
          frames[0]!.value,
          yield* committed(harness, root, initial.conversation, 50),
        );
        yield* Fiber.interrupt(consumer);
        yield* release;
        yield* harness.close;
      }),
    ),
  );

  it.effect(
    "keeps watches of one conversation independent and remounts after the last one detaches",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const { harness, root } = yield* openChat(yield* controlledStorage, chatSetup());
          const kept = yield* record(root);
          const { frames, stop } = yield* record(root);
          yield* note(root, "one");
          yield* drained;
          assert.deepStrictEqual(kinds(kept.frames.at(-1)!.value), ["one"]);
          yield* stop;
          assert.lengthOf(frames, 1);
          yield* note(root, "two");
          yield* drained;
          const last = kept.frames.at(-1)!.value;
          assert.deepStrictEqual(kinds(last), ["one", "two"]);
          yield* kept.stop;
          // No observer is left, so the mount was dropped: a new observer builds a new revision from committed
          // state.
          const rebuilt = yield* fresh(root);
          assert.deepStrictEqual(rebuilt, last);
          assert.notStrictEqual(rebuilt, last);
          yield* harness.close;
        }),
      ),
  );

  it.effect("ends watches at close and rejects later acquisition", () =>
    run(backend)(
      Effect.gen(function* () {
        const { harness, root } = yield* openChat(yield* controlledStorage, chatSetup());
        const { value: watch } = yield* acquire(root.watch());
        const { recorded, consumer } = yield* consume(watch.frames);
        yield* harness.close;
        // The stream ends without a frame.
        assert.isTrue(Exit.isSuccess(yield* Fiber.await(consumer)));
        assert.lengthOf(recorded, 0);
        assert.deepStrictEqual(watch.value.entries, []);
        yield* failureMessage(Effect.scoped(root.watch()));
      }),
    ),
  );

  it.effect("rejects an acquisition cancelled or closed while it waits for the Session line", () =>
    run(backend)(
      Effect.gen(function* () {
        const { harness, root } = yield* openChat(yield* controlledStorage, chatSetup());
        const hold = Effect.gen(function* () {
          const release = yield* Deferred.make<void>();
          const blocking = yield* Effect.forkChild(root.commit(Deferred.await(release)), {
            startImmediately: true,
          });
          return { release: Deferred.succeed(release, undefined), blocking };
        });
        let held = yield* hold;
        const cancelled = yield* Effect.forkChild(Effect.scoped(root.watch()), {
          startImmediately: true,
        });
        yield* Fiber.interrupt(cancelled);
        yield* held.release;
        yield* joined(held.blocking);
        assert.isTrue(Exit.hasInterrupts(yield* awaitBounded(cancelled)));

        held = yield* hold;
        const closedWhileQueued = yield* Effect.forkChild(Effect.scoped(root.watch()), {
          startImmediately: true,
        });
        const closing = yield* Effect.forkChild(harness.close, { startImmediately: true });
        yield* held.release;
        yield* joined(held.blocking);
        assert.include(yield* failureMessage(joined(closedWhileQueued)), "is closed");
        yield* joined(closing);
      }),
    ),
  );

  it.effect("shares one mount between concurrent observers and isolates a failing listener", () =>
    run(backend)(
      Effect.gen(function* () {
        const { harness, root } = yield* openChat(yield* controlledStorage, chatSetup());
        const { value: failing } = yield* acquire(root.watch());
        const { frames, stop } = yield* record(root);
        const { value: shared, release: stopShared } = yield* acquire(root.watch());
        // Each observer has its own window; the documents are the mount's.
        assert.deepStrictEqual(failing.value, shared.value);
        assert.strictEqual(failing.value.docs, shared.value.docs);
        yield* stopShared;
        const failed = yield* Effect.forkChild(
          Stream.runForEach(failing.frames, () => Effect.fail("listener failed")),
          { startImmediately: true },
        );
        yield* note(root, "one");
        yield* note(root, "two");
        yield* drained;
        // The failing consumer ends its own stream with its failure.
        assert.strictEqual(yield* Effect.flip(Fiber.join(failed)), "listener failed");
        assert.lengthOf(frames, 2);
        yield* stop;
        yield* harness.close;
      }),
    ),
  );

  it.effect(
    "publishes one frame for a commit that appends several entries and edits a document",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const { harness, root } = yield* openChat(yield* controlledStorage, chatSetup());
          const Other = Doc.make({
            checkpointWhen: () => false,
            kind: "app.other",
            version: 1,
            scope: "conversation",
            history: "latest",
            fork: "initial",
            schema: Schema.Struct({ n: Schema.Number }),
            initial: () => ({ n: 0 }),
          });
          const { initial, frames, stop } = yield* record(root);
          yield* root.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              yield* tx.appendEntry(root.id, { kind: "a" });
              (yield* tx.doc(LiveDoc, root.id)).tools = [];
              yield* tx.appendEntry(root.id, { kind: "b" });
            }),
          );
          // A document that is not mounted publishes nothing.
          yield* root.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              (yield* tx.doc(Other, root.id)).n = 1;
            }),
          );
          yield* drained;
          assert.lengthOf(frames, 1);
          assert.deepStrictEqual(kinds(frames[0]!.value), ["a", "b"]);
          assert.deepStrictEqual(
            replay(initial, frames),
            yield* committed(harness, root, initial.conversation, 2),
          );
          yield* stop;
          yield* harness.close;
        }),
      ),
  );
});

/** Appends one entry of `kind` per commit; returns the entries. */
const notes = (conversation: Conversation, count: number, kind = "note") =>
  Effect.gen(function* () {
    const entries: Array<EntryRecord> = [];
    for (let index = 0; index < count; index++) {
      entries.push(
        yield* conversation.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            return yield* tx.appendEntry(conversation.id, { kind, data: index });
          }),
        ),
      );
      // See `note`.
      yield* drained;
    }
    return entries;
  });

const ids = (entries: ReadonlyArray<EntryRecord>) => entries.map((entry) => entry.id);

/** An entry ID `offset` past `id`. */
// SAFETY: entry IDs are integers; the tests address IDs around the written ones.
const shifted = (id: EntryId, offset: number) => (id + offset) as EntryId;

describeEachBackend("conversation view window", (backend) => {
  it.effect(
    "starts with the newest `min` entries and the entry before them, or the whole short transcript",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const { harness, root } = yield* openChat(yield* controlledStorage, chatSetup());
          const written = yield* notes(root, 10);
          const view = yield* fresh(root, { min: 4 });
          assert.deepStrictEqual(ids(view.entries), ids(written.slice(6)));
          assert.strictEqual(view.before, written[5]!.id);
          // The default window: the newest 50, here all ten, starting the transcript.
          const all = yield* fresh(root);
          assert.deepStrictEqual(ids(all.entries), ids(written));
          assert.isUndefined(all.before);
          assert.isFalse("before" in all);
          yield* harness.close;
        }),
      ),
  );

  it.effect("takes appends up to `max`, then drops its oldest in one splice down to `min`", () =>
    run(backend)(
      Effect.gen(function* () {
        const { harness, root } = yield* openChat(yield* controlledStorage, chatSetup());
        const written = yield* notes(root, 4);
        const { initial, frames, stop } = yield* record(root, { min: 3, max: 5 });
        assert.deepStrictEqual(ids(initial.entries), ids(written.slice(1)));
        const appended = yield* notes(root, 3);
        yield* drained;
        assert.deepStrictEqual(
          frames.map((frame) => frame.value.entries.length),
          [4, 5, 3],
        );
        // SAFETY: `notes` returned the three entries it appended.
        const [e5, e6, e7] = appended as [EntryRecord, EntryRecord, EntryRecord];
        assert.deepStrictEqual(frames[0]!.ops, [["p", ["entries"], 3, 0, [e5]]]);
        assert.deepStrictEqual(frames[2]!.ops, [
          ["p", ["entries"], 5, 0, [e7]],
          ["p", ["entries"], 0, 3, []],
          ["s", ["before"], written[3]!.id],
        ]);
        assert.deepStrictEqual(ids(frames[2]!.value.entries), ids([e5, e6, e7]));
        assert.strictEqual(frames[2]!.value.before, written[3]!.id);
        const last = replay(initial, frames);
        assert.deepStrictEqual(last, yield* committed(harness, root, initial.conversation, 3));
        // Entries that stay keep their identity: a splice moves them, it never copies them.
        assert.strictEqual(frames[2]!.value.entries[0], frames[1]!.value.entries[3]);
        yield* stop;
        yield* harness.close;
      }),
    ),
  );

  it.effect("keeps the newest `min` of a commit that appends more than `max` at once", () =>
    run(backend)(
      Effect.gen(function* () {
        const { harness, root } = yield* openChat(yield* controlledStorage, chatSetup());
        yield* notes(root, 2);
        const { initial, frames, stop } = yield* record(root, { min: 2, max: 3 });
        const burst = yield* root.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            const entries: Array<EntryRecord> = [];
            for (let index = 0; index < 5; index++) {
              entries.push(yield* tx.appendEntry(root.id, { kind: "burst" }));
            }
            return entries;
          }),
        );
        yield* drained;
        assert.lengthOf(frames, 1);
        assert.deepStrictEqual(ids(frames[0]!.value.entries), ids(burst.slice(3)));
        assert.strictEqual(frames[0]!.value.before, burst[2]!.id);
        assert.deepStrictEqual(
          replay(initial, frames),
          yield* committed(harness, root, initial.conversation, 2),
        );
        yield* stop;
        yield* harness.close;
      }),
    ),
  );

  it.effect(
    "starts at `from` when the entries since fit in `max`, otherwise at the newest `min`",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const { harness, root } = yield* openChat(yield* controlledStorage, chatSetup());
          const written = yield* notes(root, 30);
          const window = { min: 5, max: 10 };
          // Ten entries since `from`: all of them, with the entry before `from`.
          const within = yield* fresh(root, { ...window, from: written[20]!.id });
          assert.deepStrictEqual(ids(within.entries), ids(written.slice(20)));
          assert.strictEqual(within.before, written[19]!.id);
          // Fewer than `min` since `from`: only those.
          const short = yield* fresh(root, { ...window, from: written[28]!.id });
          assert.deepStrictEqual(ids(short.entries), ids(written.slice(28)));
          assert.strictEqual(short.before, written[27]!.id);
          // Resuming after the newest entry: an empty window right after it.
          const resumed = yield* fresh(root, { ...window, from: shifted(written[29]!.id, 1) });
          assert.deepStrictEqual(resumed.entries, []);
          assert.strictEqual(resumed.before, written[29]!.id);
          // Eleven since `from`: more than `max`, so the newest `min`; `before` shows the client a gap.
          const beyond = yield* fresh(root, { ...window, from: written[19]!.id });
          assert.deepStrictEqual(ids(beyond.entries), ids(written.slice(25)));
          assert.strictEqual(beyond.before, written[24]!.id);
          // From the transcript's start.
          const whole = yield* fresh(root, { min: 40, from: written[0]!.id });
          assert.deepStrictEqual(ids(whole.entries), ids(written));
          assert.isUndefined(whole.before);
          yield* harness.close;
        }),
      ),
  );

  it.effect("advances a resumed window by appends from its start", () =>
    run(backend)(
      Effect.gen(function* () {
        const { harness, root } = yield* openChat(yield* controlledStorage, chatSetup());
        const written = yield* notes(root, 5);
        const { initial, frames, stop } = yield* record(root, {
          from: shifted(written[4]!.id, 1),
        });
        const later = yield* notes(root, 2);
        yield* drained;
        assert.deepStrictEqual(ids(frames.at(-1)!.value.entries), ids(later));
        assert.strictEqual(frames.at(-1)!.value.before, written[4]!.id);
        assert.deepStrictEqual(replay(initial, frames), frames.at(-1)!.value);
        yield* stop;
        yield* harness.close;
      }),
    ),
  );

  it.effect(
    "uses `from` only for the first value: a `from` past the newest entry starts empty and takes every append",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const { harness, root } = yield* openChat(yield* controlledStorage, chatSetup());
          const written = yield* notes(root, 3);
          const from = shifted(written[2]!.id, 1000);
          const { initial, frames, stop } = yield* record(root, { from });
          assert.deepStrictEqual(initial.entries, []);
          assert.strictEqual(initial.before, written[2]!.id);
          // An entry below `from` is still appended: the window continues from its attachment.
          const [next] = yield* notes(root, 1);
          assert.isBelow(next!.id, from);
          yield* drained;
          assert.deepStrictEqual(frames.at(-1)!.value.entries, [next!]);
          assert.strictEqual(frames.at(-1)!.value.before, written[2]!.id);
          yield* stop;
          yield* harness.close;
        }),
      ),
  );

  it.effect("rejects window bounds that are not integers or put `max` below `min`", () =>
    run(backend)(
      Effect.gen(function* () {
        const { harness, root } = yield* openChat(yield* controlledStorage, chatSetup());
        assert.instanceOf(yield* defectOf(root.watch({ min: 0 })), RangeError);
        assert.instanceOf(yield* defectOf(root.watch({ min: 1.5 })), RangeError);
        assert.instanceOf(yield* defectOf(root.watch({ min: 10, max: 9 })), RangeError);
        // SAFETY: a NaN entry ID checks the guard.
        const nan = Number.NaN as EntryId;
        assert.instanceOf(yield* defectOf(root.watch({ from: nan })), RangeError);
        assert.instanceOf(
          yield* defectOf(harness.views.attach(root.id, { max: 0 }, recorder)),
          RangeError,
        );
        // `max` may equal `min`.
        yield* Effect.scoped(root.watch({ min: 3, max: 3 }));
        yield* harness.close;
      }),
    ),
  );

  it.effect("gives each observer its own window over one mount", () =>
    run(backend)(
      Effect.gen(function* () {
        const { harness, root } = yield* openChat(yield* controlledStorage, chatSetup());
        const written = yield* notes(root, 6);
        const changes: Array<ConversationView> = [];
        const { value: observation, release: detach } = yield* acquire(
          harness.views.attach(root.id, { min: 2, max: 3 }, (value) => ({
            value,
            advance: (frame) => {
              changes.push(frame.value);
            },
            closeSession: () => {},
          })),
        );
        const wide = yield* record(root, { min: 5 });
        assert.deepStrictEqual(ids(observation.value.entries), ids(written.slice(4)));
        assert.deepStrictEqual(ids(wide.initial.entries), ids(written.slice(1)));
        const [next] = yield* notes(root, 1);
        yield* drained;
        assert.deepStrictEqual(ids(changes[0]!.entries), ids([...written.slice(4), next!]));
        assert.deepStrictEqual(
          ids(wide.frames[0]!.value.entries),
          ids([...written.slice(1), next!]),
        );
        assert.strictEqual(changes[0]!.docs, wide.frames[0]!.value.docs);
        yield* detach;
        yield* wide.stop;
        yield* harness.close;
      }),
    ),
  );

  it.effect(
    "bounds an overflow replacement by `max`",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const { harness, root } = yield* openChat(yield* controlledStorage, chatSetup());
          const { value: watch, release } = yield* acquire(root.watch({ min: 5, max: 6 }));
          // Past 10,000 pending operations, the next call is one root replacement: here an append per commit,
          // and a splice and a `before` every other commit.
          for (let index = 0; index < 6000; index++) yield* note(root, "note");
          const { recorded: frames, consumer } = yield* consume(watch.frames);
          yield* drained;
          assert.lengthOf(frames, 1);
          assert.deepStrictEqual<unknown>(frames[0]!.ops, [["r", frames[0]!.value]]);
          assert.isAtMost(frames[0]!.value.entries.length, 6);
          yield* Fiber.interrupt(consumer);
          yield* release;
          yield* harness.close;
        }),
      ),
    serialTimeout(2_381),
  );

  // On SQLite, which keeps no entry objects, so only the watch can hold the first one; the memory backend
  // keeps every entry it stores.
  it.effect.skipIf(backend.name === "memory")(
    "lets a long-lived watch drop the entries that left its window",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const { harness, root } = yield* openChat(yield* controlledStorage, chatSetup());
          // An entry written first in another conversation. The SQLite client caches each statement
          // with the fiber that prepared it, whose spans keep the exit of the commit around it: the first
          // commit that appends an entry would keep that entry alive, whatever the watch does.
          const other = yield* harness.harness.createConversation({
            ownership: { kind: "ownerless" },
          });
          yield* notes(other, 1);
          const { value: watch, release } = yield* acquire(root.watch({ min: 5, max: 10 }));
          const seen: Seen = { first: undefined, count: 0 };
          const consumer = yield* Effect.forkChild(
            Stream.runForEach(watch.frames, (frame) =>
              Effect.sync(() => {
                seen.count = frame.value.entries.length;
                const head = frame.value.entries[0];
                if (seen.first === undefined && head !== undefined) seen.first = new WeakRef(head);
              }),
            ),
            { startImmediately: true },
          );
          yield* notes(root, 1);
          yield* waitFor(Effect.sync(() => seen.first !== undefined));
          yield* notes(root, 200);
          yield* drained;
          assert.isAtMost(seen.count, 10);
          // The controlled storage records every admitted batch, which holds the entries it wrote.
          harness.storage.admittedCommits.length = 0;
          harness.storage.commits.length = 0;
          yield* collect;
          assert.isUndefined(seen.first!.deref());
          yield* Fiber.interrupt(consumer);
          yield* release;
          yield* harness.close;
        }),
      ),
  );
});

/** What the long-lived watch's consumer saw: its first entry, weakly, and its last window's size. */
interface Seen {
  first: WeakRef<EntryRecord> | undefined;
  count: number;
}

/** A few rounds of macrotask turns, each followed by a full collection. */
const collect = Effect.promise(async () => {
  setFlagsFromString("--expose-gc");
  // SAFETY: `gc` is the collector the flag exposes in a new context.
  const gc = runInNewContext("gc") as () => void;
  for (let round = 0; round < 3; round++) {
    await new Promise((resolve) => setImmediate(resolve));
    gc();
  }
});

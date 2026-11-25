// `ConversationTranscript`, the client-side layer that merges view windows into one transcript.
//
// - A load is cancelled by interruption.
// - `newest` and `resume` are functions, so the newest view is `helper.newest()`.
// - The loader's `Page` has `next: Option<Cursor>`.
// - "shares one load": Effects have no identity to compare, so the concurrent calls are forked fibers, the loader
//   is held on a `Deferred`, and the test checks that it ran once per shared load and that the callers got the
//   same view.
// - Loading before the first merge is a defect (`make`: "merge a view first").
// - "over a Harness": the consumer is a fiber over the watch's `frames`; stopping interrupts it and closes the
//   watch's scope; the loader is `Conversation.entries`, a Stream, taken to `limit + 1` items to know whether a
//   next page follows.
import { assert, describe, it } from "@effect/vitest";
import * as Cause from "effect/Cause";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as ConversationTranscript from "../src/ConversationTranscript.ts";
import type { EntryLoader, TranscriptGap, TranscriptView } from "../src/ConversationTranscript.ts";
import type { Conversation } from "../src/Harness.ts";
import { ConversationId, EntryId } from "../src/Ids.ts";
import type { EntryRecord } from "../src/Records.ts";
import { Tx } from "../src/Tx.ts";
import type { ConversationView } from "../src/View.ts";
import { allEntries, chatSetup, openChat } from "./ChatSupport.ts";
import { controlledStorage, describeEachBackend, flush, run } from "./SessionSupport.ts";

const CONVERSATION: ConversationView["conversation"] = { id: ConversationId.make(1) };

/** Entries with IDs that have holes: every third ID is missing. */
function transcript(count: number): Array<EntryRecord> {
  const entries: Array<EntryRecord> = [];
  for (let id = 1; entries.length < count; id++) {
    if (id % 3 !== 0) {
      entries.push({ id: EntryId.make(id), conversationId: CONVERSATION.id, kind: "note" });
    }
  }
  return entries;
}

type CountingLoader = EntryLoader & { calls: number };

/** A loader over `entries` with `Conversation.entries()` semantics, counting its calls. */
function loader(entries: ReadonlyArray<EntryRecord>): CountingLoader {
  const load: CountingLoader = Object.assign(
    (query: Parameters<EntryLoader>[0], limit: number) =>
      Effect.sync(() => {
        load.calls++;
        const matching = entries.filter(
          (entry) =>
            (query.minEntryId === undefined || entry.id >= query.minEntryId) &&
            (query.maxEntryId === undefined || entry.id <= query.maxEntryId),
        );
        const ordered = query.order === "ascending" ? matching : matching.toReversed();
        const items = ordered.slice(0, limit);
        return { items, next: ordered.length > limit ? Option.some({}) : Option.none() };
      }),
    { calls: 0 },
  );
  return load;
}

/** The view of a window: `entries` with the entry before them in `all`. */
function windowOf(
  all: ReadonlyArray<EntryRecord>,
  first: number,
  last: number,
  docs: ConversationView["docs"] = {},
): ConversationView {
  const before = all[first - 1]?.id;
  const entries = all.slice(first, last + 1);
  return before === undefined
    ? { conversation: CONVERSATION, entries, docs }
    : { conversation: CONVERSATION, entries, before, docs };
}

/** Items as a compact list: entry IDs, `gap after..upTo`, and `earlier`. */
function layout(view: TranscriptView): Array<number | string> {
  return view.items.map((item) =>
    item.kind === "entry"
      ? item.entry.id
      : item.kind === "gap"
        ? `gap ${item.after}..${item.upTo}`
        : "earlier",
  );
}

const idsOf = (entries: ReadonlyArray<EntryRecord>) => entries.map((entry) => entry.id);
const gapOf = (view: TranscriptView) =>
  view.items.find((item): item is TranscriptGap => item.kind === "gap")!;

/** The message of the defect `effect` dies with; fails the test when it succeeds or fails instead. */
const defectMessage = <A, E>(effect: Effect.Effect<A, E>) =>
  Effect.gen(function* () {
    const exit = yield* Effect.exit(effect);
    if (Exit.isSuccess(exit)) return assert.fail("expected a defect");
    const cause = exit.cause;
    assert.isTrue(Cause.hasDies(cause));
    assert.isFalse(Cause.hasFails(cause));
    const defect = Cause.squash(cause);
    return defect instanceof Error ? defect.message : String(defect);
  });

/** Forks `effect` started at once, so a shared load registers before the next call. */
const fork = <A, E>(effect: Effect.Effect<A, E>) =>
  Effect.forkChild(effect, { startImmediately: true });

describe("ConversationTranscript", () => {
  it("shows a first window under an `earlier` marker, or alone when it starts the transcript", () => {
    const all = transcript(20);
    const later = ConversationTranscript.make(loader(all));
    assert.deepStrictEqual(layout(later.merge(windowOf(all, 15, 19))), [
      "earlier",
      ...idsOf(all.slice(15)),
    ]);
    assert.strictEqual(later.newest(), all[19]!.id);
    const whole = ConversationTranscript.make(loader(all));
    assert.deepStrictEqual(layout(whole.merge(windowOf(all, 0, 19))), idsOf(all));
  });

  it("appends, keeps entries the window dropped, and returns the same items when nothing is added", () => {
    const all = transcript(30);
    const helper = ConversationTranscript.make(loader(all));
    const docsA = { a: { n: 1 } };
    const first = helper.merge(windowOf(all, 10, 19, docsA));
    const grown = helper.merge(windowOf(all, 10, 21, docsA));
    assert.deepStrictEqual(layout(grown), ["earlier", ...idsOf(all.slice(10, 22))]);
    // The window dropped its oldest; the transcript keeps them.
    const spliced = helper.merge(windowOf(all, 18, 21, docsA));
    assert.strictEqual(spliced.items, grown.items);
    assert.strictEqual(spliced, grown);
    // A docs-only change: a new view, the same items.
    const docs = { a: { n: 2 } };
    const streamed = helper.merge({ ...windowOf(all, 18, 21), docs });
    assert.notStrictEqual(streamed, grown);
    assert.strictEqual(streamed.items, grown.items);
    assert.strictEqual(streamed.docs, docs);
    assert.notStrictEqual(first.items, grown.items);
  });

  it.effect(
    "opens a gap when a window does not continue the transcript, and fills it page by page",
    () =>
      Effect.gen(function* () {
        const all = transcript(200);
        const load = loader(all);
        const helper = ConversationTranscript.make(load, { pageSize: 50 });
        helper.merge(windowOf(all, 150, 159));
        // A stall: the next window starts well past the newest entry held.
        const stalled = helper.merge(windowOf(all, 190, 199));
        const gap = gapOf(stalled);
        assert.deepStrictEqual(gap, { kind: "gap", after: all[159]!.id, upTo: all[190]!.id });
        assert.deepStrictEqual(layout(stalled), [
          "earlier",
          ...idsOf(all.slice(150, 160)),
          `gap ${all[159]!.id}..${all[190]!.id}`,
          ...idsOf(all.slice(190)),
        ]);
        // 30 entries are missing; one page loads them all and closes the gap.
        const filled = yield* helper.fillGap(gap);
        assert.deepStrictEqual(layout(filled), ["earlier", ...idsOf(all.slice(150))]);
        assert.strictEqual(load.calls, 1);
      }),
  );

  it.effect("shrinks a gap from its start while more than a page is missing", () =>
    Effect.gen(function* () {
      const all = transcript(200);
      const helper = ConversationTranscript.make(loader(all), { pageSize: 10 });
      helper.merge(windowOf(all, 100, 109));
      let view = helper.merge(windowOf(all, 190, 199));
      view = yield* helper.fillGap(gapOf(view));
      assert.deepStrictEqual(gapOf(view), {
        kind: "gap",
        after: all[119]!.id,
        upTo: all[190]!.id,
      });
      while (view.items.some((item) => item.kind === "gap")) {
        view = yield* helper.fillGap(gapOf(view));
      }
      assert.deepStrictEqual(layout(view), ["earlier", ...idsOf(all.slice(100))]);
    }),
  );

  it.effect("loads earlier pages until the start, then drops the marker", () =>
    Effect.gen(function* () {
      const all = transcript(45);
      const load = loader(all);
      const helper = ConversationTranscript.make(load, { pageSize: 20 });
      helper.merge(windowOf(all, 40, 44));
      let view = yield* helper.loadEarlier;
      assert.deepStrictEqual(layout(view), ["earlier", ...idsOf(all.slice(20))]);
      // A page without a next cursor reaches the start.
      view = yield* helper.loadEarlier;
      assert.deepStrictEqual(layout(view), idsOf(all));
      const calls = load.calls;
      assert.strictEqual(yield* helper.loadEarlier, view);
      assert.strictEqual(load.calls, calls);
    }),
  );

  it.effect(
    "shares one load between concurrent calls of loadEarlier, and of fillGap for one gap",
    () =>
      Effect.gen(function* () {
        const all = transcript(100);
        const inner = loader(all);
        const gate = yield* Deferred.make<void>();
        let started = 0;
        const load: EntryLoader = (query, limit) =>
          Effect.sync(() => {
            started++;
          }).pipe(Effect.andThen(Deferred.await(gate)), Effect.andThen(inner(query, limit)));
        const helper = ConversationTranscript.make(load);
        helper.merge(windowOf(all, 40, 49));
        const stalled = helper.merge(windowOf(all, 90, 99));
        const earlier = [yield* fork(helper.loadEarlier), yield* fork(helper.loadEarlier)];
        const fills = [
          yield* fork(helper.fillGap(gapOf(stalled))),
          yield* fork(helper.fillGap(gapOf(stalled))),
        ];
        // One load started for each pair of calls.
        yield* flush;
        assert.strictEqual(started, 2);
        yield* Deferred.succeed(gate, undefined);
        const [earlierA, earlierB] = [
          yield* Fiber.join(earlier[0]!),
          yield* Fiber.join(earlier[1]!),
        ];
        const [fillA, fillB] = [yield* Fiber.join(fills[0]!), yield* Fiber.join(fills[1]!)];
        assert.strictEqual(earlierA, earlierB);
        assert.strictEqual(fillA, fillB);
        assert.strictEqual(inner.calls, 2);
        assert.deepStrictEqual(layout(yield* helper.loadEarlier), idsOf(all));
      }),
  );

  it("resumes after `newest`: an empty window right after it adds nothing; one past it opens a gap", () => {
    const all = transcript(30);
    const helper = ConversationTranscript.make(loader(all));
    const view = helper.merge(windowOf(all, 10, 19));
    const resumed: ConversationView = {
      conversation: CONVERSATION,
      entries: [],
      before: helper.newest()!,
      docs: {},
    };
    assert.strictEqual(helper.merge(resumed).items, view.items);
    // A window that is empty but starts past `newest`: the entries up to its `before` are missing.
    const past = helper.merge({ ...resumed, before: all[25]!.id });
    assert.strictEqual(layout(past).at(-1), `gap ${all[19]!.id}..${all[25]!.id + 1}`);
    assert.strictEqual(helper.newest(), all[25]!.id);
    // The next window continues it.
    assert.deepStrictEqual(layout(helper.merge(windowOf(all, 26, 27))).slice(-3), [
      `gap ${all[19]!.id}..${all[25]!.id + 1}`,
      all[26]!.id,
      all[27]!.id,
    ]);
  });

  it("fills a gap from a window that covers it", () => {
    const all = transcript(100);
    const helper = ConversationTranscript.make(loader(all));
    helper.merge(windowOf(all, 40, 49));
    helper.merge(windowOf(all, 90, 99));
    // A wider window, such as a new watch with a larger `max`, covers the second half of the gap.
    const view = helper.merge(windowOf(all, 70, 99));
    assert.deepStrictEqual(layout(view), [
      "earlier",
      ...idsOf(all.slice(40, 50)),
      `gap ${all[49]!.id}..${all[70]!.id}`,
      ...idsOf(all.slice(70)),
    ]);
  });

  it.effect(
    "rejects loadEarlier before the first merge and returns the view for a gap that is gone",
    () =>
      Effect.gen(function* () {
        const all = transcript(10);
        const helper = ConversationTranscript.make(loader(all));
        // Loading before the first merge is a defect.
        assert.include(yield* defectMessage(helper.loadEarlier), "merge a view first");
        const view = helper.merge(windowOf(all, 0, 9));
        const stale = { kind: "gap", after: EntryId.make(1), upTo: EntryId.make(2) } as const;
        assert.strictEqual(yield* helper.fillGap(stale), view);
      }),
  );
});

describe("ConversationTranscript loads", () => {
  it.effect("retries a load whose loader threw synchronously, for loadEarlier and fillGap", () =>
    Effect.gen(function* () {
      const all = transcript(100);
      const inner = loader(all);
      let offline = true;
      let calls = 0;
      // The loader's Effect dies.
      const load: EntryLoader = (query, limit) =>
        Effect.suspend(() => {
          calls++;
          // An adapter that checks its connection before it loads.
          if (offline) throw new Error("temporarily offline");
          return inner(query, limit);
        });
      const helper = ConversationTranscript.make(load);
      helper.merge(windowOf(all, 40, 49));
      const stalled = helper.merge(windowOf(all, 90, 99));
      assert.include(yield* defectMessage(helper.loadEarlier), "temporarily offline");
      assert.include(yield* defectMessage(helper.fillGap(gapOf(stalled))), "temporarily offline");
      offline = false;
      assert.deepStrictEqual(layout(yield* helper.loadEarlier), [
        ...idsOf(all.slice(0, 50)),
        `gap ${all[49]!.id}..${all[90]!.id}`,
        ...idsOf(all.slice(90)),
      ]);
      assert.deepStrictEqual(layout(yield* helper.fillGap(gapOf(stalled))), idsOf(all));
      assert.strictEqual(calls, 4);
    }),
  );

  it.effect("retries a load that rejected", () =>
    Effect.gen(function* () {
      const all = transcript(30);
      const inner = loader(all);
      let fail = true;
      // The load fails.
      const load: EntryLoader<string> = (query, limit) =>
        Effect.suspend(() => (fail ? Effect.fail("timeout") : inner(query, limit)));
      const helper = ConversationTranscript.make(load);
      helper.merge(windowOf(all, 20, 29));
      assert.strictEqual(yield* Effect.flip(helper.loadEarlier), "timeout");
      fail = false;
      assert.deepStrictEqual(layout(yield* helper.loadEarlier), idsOf(all));
    }),
  );

  it("keeps one gap for an empty window whose `before` lies inside it", () => {
    const all = transcript(100);
    const helper = ConversationTranscript.make(loader(all));
    helper.merge(windowOf(all, 40, 49));
    const before = helper.merge(windowOf(all, 90, 99));
    const view = helper.merge({
      conversation: CONVERSATION,
      entries: [],
      before: all[65]!.id,
      docs: before.docs,
    });
    assert.strictEqual(view.items, before.items);
    assert.lengthOf(
      view.items.filter((item) => item.kind === "gap"),
      1,
    );
  });

  it.effect("shares one load between a stale gap descriptor and the gap it shrank into", () =>
    Effect.gen(function* () {
      const all = transcript(100);
      const inner = loader(all);
      const gate = yield* Deferred.make<void>();
      const load: EntryLoader = (query, limit) =>
        Deferred.await(gate).pipe(Effect.andThen(inner(query, limit)));
      const helper = ConversationTranscript.make(load, { pageSize: 10 });
      helper.merge(windowOf(all, 10, 19));
      const stale = gapOf(helper.merge(windowOf(all, 90, 99)));
      yield* Deferred.succeed(gate, undefined);
      const current = gapOf(yield* helper.fillGap(stale));
      assert.notDeepEqual(current, stale);
      const loads = inner.calls;
      // Both callers join one load and get the same view.
      const [a, b] = [yield* fork(helper.fillGap(stale)), yield* fork(helper.fillGap(current))];
      assert.strictEqual(yield* Fiber.join(a), yield* Fiber.join(b));
      assert.strictEqual(inner.calls, loads + 1);
    }),
  );

  it.effect("treats an empty page with a next cursor as the end of the range", () =>
    Effect.gen(function* () {
      const all = transcript(60);
      // A remote loader that always returns a cursor.
      const inner = loader(all);
      const load: EntryLoader = (query, limit) =>
        inner(query, limit).pipe(Effect.map((page) => ({ ...page, next: Option.some({}) })));
      const helper = ConversationTranscript.make(load, { pageSize: 100 });
      helper.merge(windowOf(all, 10, 19));
      let view = helper.merge(windowOf(all, 40, 49));
      view = yield* helper.fillGap(gapOf(view));
      // The first page held every missing entry; the next one is empty and closes the gap.
      view = yield* helper.fillGap(gapOf(view));
      assert.isUndefined(gapOf(view));
      yield* helper.loadEarlier;
      view = yield* helper.loadEarlier;
      assert.deepStrictEqual(layout(view), idsOf(all.slice(0, 50)));
    }),
  );

  it("resumes with `resume`: undefined before a merge, `{}` for an empty transcript, `{ from }` after the newest", () => {
    const all = transcript(30);
    const helper = ConversationTranscript.make(loader(all));
    assert.isUndefined(helper.resume());
    helper.merge({ conversation: CONVERSATION, entries: [], docs: {} });
    assert.deepStrictEqual(helper.resume(), {});
    helper.merge(windowOf(all, 0, 9));
    assert.deepStrictEqual(helper.resume(), { from: EntryId.make(all[9]!.id + 1) });
  });
});

/** A loader over `root.entries(query, limit)`: a page of `limit` items, with `next` when more follow. */
const entryLoader = (conversation: Conversation) =>
  ConversationTranscript.make((query, limit) =>
    Stream.runCollect(conversation.entries(query).pipe(Stream.take(limit + 1))).pipe(
      Effect.map((collected) => {
        const items = Array.from(collected);
        return {
          items: items.slice(0, limit),
          next: items.length > limit ? Option.some({}) : Option.none(),
        };
      }),
    ),
  );

/** Acquires in a scope of its own; closing it stops what was acquired. */
const acquire = <A, E>(open: Effect.Effect<A, E, Scope.Scope>) =>
  Effect.gen(function* () {
    const scope = yield* Scope.make();
    const value = yield* Scope.provide(open, scope);
    return { value, release: Scope.close(scope, Exit.void) };
  });

describeEachBackend("ConversationTranscript over a Harness", (backend) => {
  it.effect(
    "follows the walkthrough: live appends, a stall, fillGap, loadEarlier, and a resume with `from`",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const { harness, root } = yield* openChat(yield* controlledStorage, chatSetup());
          const note = root.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              return yield* tx.appendEntry(root.id, { kind: "note" });
            }),
          );
          for (let index = 0; index < 120; index++) yield* note;
          const helper = entryLoader(root);
          const window = { min: 10, max: 20 };
          const { value: watch, release: stopWatch } = yield* acquire(root.watch(window));
          let view = helper.merge(watch.value);
          // A consumer fiber that merges every delivered value.
          const consumer = yield* fork(
            Stream.runForEach(watch.frames, (frame) =>
              Effect.sync(() => {
                view = helper.merge(frame.value);
              }),
            ),
          );
          for (let index = 0; index < 15; index++) yield* note;
          yield* flush;
          assert.isFalse(view.items.some((item) => item.kind === "gap"));
          assert.lengthOf(
            view.items.filter((item) => item.kind === "entry"),
            25,
          );
          // A drop: the client misses 40 entries, more than `max`, and resumes after the newest it has.
          yield* Fiber.interrupt(consumer);
          yield* stopWatch;
          for (let index = 0; index < 40; index++) yield* note;
          const { value: resumed, release: stopResumed } = yield* acquire(
            root.watch({ ...window, ...helper.resume() }),
          );
          view = helper.merge(resumed.value);
          assert.lengthOf(
            view.items.filter((item) => item.kind === "gap"),
            1,
          );
          while (view.items.some((item) => item.kind === "gap")) {
            view = yield* helper.fillGap(gapOf(view));
          }
          while (view.items[0]?.kind === "earlier") view = yield* helper.loadEarlier;
          const entries = view.items.flatMap((item) =>
            item.kind === "entry" ? [item.entry.id] : [],
          );
          assert.deepStrictEqual(entries, idsOf(yield* allEntries(root)));
          // A short drop resumes without a gap.
          for (let index = 0; index < 3; index++) yield* note;
          const { value: again, release: stopAgain } = yield* acquire(
            root.watch({ ...window, ...helper.resume() }),
          );
          assert.lengthOf(again.value.entries, 3);
          view = helper.merge(again.value);
          assert.isFalse(view.items.some((item) => item.kind !== "entry"));
          yield* stopResumed;
          yield* stopAgain;
          yield* harness.close;
        }),
      ),
  );
});

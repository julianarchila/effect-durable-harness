// Tests of the conversation view mounts (`internal/view.ts`), over the Session kernel on every backend. The
// Harness creation hook stages the built-in documents; here each test creates them with `tx` itself.
import { assert, it } from "@effect/vitest";
import * as Delta from "@effect-durable-harness/delta/Delta";
import * as Cause from "effect/Cause";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import { AgentDoc } from "../src/Agent.ts";
import type { ConversationId, EntryId } from "../src/Ids.ts";
import { InboxDoc } from "../src/Inbox.ts";
import * as View from "../src/internal/view.ts";
import type { SessionKernel } from "../src/internal/session.ts";
import { LiveDoc } from "../src/Live.ts";
import { ProviderDoc } from "../src/ProviderSession.ts";
import type { EntryRecord } from "../src/Records.ts";
import { Tx } from "../src/Tx.ts";
import { UsageDoc } from "../src/Usage.ts";
import type { ConversationView } from "../src/View.ts";
import {
  createConversation,
  describeEachBackend,
  flush,
  openTestSession,
  run,
} from "./SessionSupport.ts";

/** A Session, one conversation with its built-in documents, and the view mounts over them. */
const setup = Effect.gen(function* () {
  const test = yield* openTestSession();
  const session = test.session;
  const conversationId = yield* createConversation(session);
  yield* session.commit(
    Effect.gen(function* () {
      const tx = yield* Tx;
      yield* tx.doc(AgentDoc, conversationId);
      yield* tx.doc(LiveDoc, conversationId);
      yield* tx.doc(InboxDoc, conversationId);
      yield* tx.doc(ProviderDoc, conversationId);
      yield* tx.doc(UsageDoc, conversationId);
    }),
  );
  const reports: Array<Cause.Cause<unknown>> = [];
  const views = yield* View.make({
    kernel: session,
    report: (cause) => {
      reports.push(cause);
    },
  });
  return { ...test, conversationId, views, reports };
}).pipe(Effect.orDie);

const append = (session: SessionKernel, conversationId: ConversationId, kind: string) =>
  session.commit(
    Effect.gen(function* () {
      const tx = yield* Tx;
      return yield* tx.appendEntry(conversationId, { kind });
    }),
  );

const appendHead = (
  session: SessionKernel,
  conversationId: ConversationId,
  head: EntryId | "self",
) =>
  session.commit(
    Effect.gen(function* () {
      const tx = yield* Tx;
      return yield* tx.appendEntry(conversationId, { kind: "marker", head });
    }),
  );

const setProviderSession = (
  session: SessionKernel,
  conversationId: ConversationId,
  sessionId: string,
) =>
  session.commit(
    Effect.gen(function* () {
      const tx = yield* Tx;
      (yield* tx.doc(ProviderDoc, conversationId)).sessionId = sessionId;
    }),
  );

/** Acquires in a scope of its own; closing it releases the acquisition. */
const acquire = <A, E>(open: Effect.Effect<A, E, Scope.Scope>) =>
  Effect.gen(function* () {
    const scope = yield* Scope.make();
    const value = yield* Scope.provide(open, scope);
    return { value, release: Scope.close(scope, Exit.void) };
  });

const kinds = (view: ConversationView | undefined) =>
  view?.entries.map((entry) => entry.kind) ?? [];

/** The view's current value through a watch acquired and released at once. */
const current = (views: View.Views, conversationId: ConversationId) =>
  Effect.scoped(views.watch(conversationId)).pipe(Effect.map((watch) => watch.value));

/** An observer that records every revision it is handed. */
const recorder = () => {
  const revisions: Array<{ readonly value: ConversationView; readonly ops: Delta.Delta }> = [];
  let initial: ConversationView | undefined;
  const create: View.CreateViewObserver<View.ViewObserver> = (value) => {
    initial = value;
    return {
      advance: (frame) => {
        revisions.push({ value: frame.value, ops: frame.ops });
      },
      closeSession: () => {},
    };
  };
  return {
    create,
    revisions,
    get initial() {
      return initial;
    },
  };
};

describeEachBackend("Conversation view mounts", (backend) => {
  it.effect("builds the committed transcript window and the built-in documents", () =>
    run(backend)(
      Effect.gen(function* () {
        const { session, conversationId, views } = yield* setup;
        const first = yield* append(session, conversationId, "first");
        const second = yield* append(session, conversationId, "second");
        const value = yield* current(views, conversationId);
        assert.strictEqual(value?.conversation.id, conversationId);
        assert.deepStrictEqual(value?.entries, [first, second]);
        assert.deepStrictEqual(Object.keys(value?.docs ?? {}).toSorted(), [
          "core.agent",
          "core.inbox",
          "core.live",
          "core.provider",
          "core.usage",
        ]);
        assert.deepStrictEqual(value?.docs["core.provider"], { sessionId: "" });
        assert.deepStrictEqual(value?.docs["core.usage"], { models: {}, tools: {} });
      }),
    ),
  );

  it.effect("leaves absent documents absent and sets one when it is created", () =>
    run(backend)(
      Effect.gen(function* () {
        const { session } = yield* openTestSession();
        const conversationId = yield* createConversation(session);
        const views = yield* View.make({ kernel: session, report: () => {} });
        const { value: watch } = yield* acquire(views.watch(conversationId));
        assert.deepStrictEqual(watch.value.docs, {});
        yield* setProviderSession(session, conversationId, "s1");
        const [frame] = yield* watch.frames.pipe(Stream.take(1), Stream.runCollect);
        assert.deepStrictEqual(frame?.ops, [["s", ["docs", "core.provider"], { sessionId: "s1" }]]);
        assert.deepStrictEqual(frame?.value.docs, { "core.provider": { sessionId: "s1" } });
      }),
    ),
  );

  it.effect("appends an entry as a splice at the end that applies to the previous frame", () =>
    run(backend)(
      Effect.gen(function* () {
        const { session, conversationId, views } = yield* setup;
        yield* append(session, conversationId, "first");
        const { value: watch } = yield* acquire(views.watch(conversationId));
        const before = watch.value;
        const entry = yield* append(session, conversationId, "second");
        const [frame] = yield* watch.frames.pipe(Stream.take(1), Stream.runCollect);
        assert.deepStrictEqual(frame?.ops, [["p", ["entries"], 1, 0, [entry]]]);
        assert.deepStrictEqual(Delta.applyImmutableUnsafe(before, frame!.ops), frame?.value);
        assert.deepStrictEqual(kinds(frame!.value), ["first", "second"]);
        // Untouched parts are shared with the previous revision.
        assert.strictEqual(frame?.value.docs, before.docs);
        assert.strictEqual(frame?.value.conversation, before.conversation);
      }),
    ),
  );

  it.effect("appends a head marker in place, like any other entry", () =>
    run(backend)(
      Effect.gen(function* () {
        const { session, conversationId, views } = yield* setup;
        yield* append(session, conversationId, "a");
        const b = yield* append(session, conversationId, "b");
        const { value: watch } = yield* acquire(views.watch(conversationId));
        const previous = watch.value;
        // The view does not derive the active transcript; a marker is appended.
        const marker = yield* appendHead(session, conversationId, b.id);
        const [frame] = yield* watch.frames.pipe(Stream.take(1), Stream.runCollect);
        assert.deepStrictEqual(frame?.ops, [["p", ["entries"], 2, 0, [marker]]]);
        assert.deepStrictEqual(kinds(frame!.value), ["a", "b", "marker"]);
        assert.deepStrictEqual(Delta.applyImmutableUnsafe(previous, frame!.ops), frame!.value);
        // A fresh mount agrees with the advanced one.
        assert.deepStrictEqual(
          (yield* current(views, conversationId)).entries.map((entry: EntryRecord) => entry.id),
          frame!.value.entries.map((entry) => entry.id),
        );
      }),
    ),
  );

  it.effect("prefixes document operations, and deletes a retired document", () =>
    run(backend)(
      Effect.gen(function* () {
        const { session, conversationId, views } = yield* setup;
        const { value: watch } = yield* acquire(views.watch(conversationId));
        const before = watch.value;
        yield* setProviderSession(session, conversationId, "s1");
        const [set] = yield* watch.frames.pipe(Stream.take(1), Stream.runCollect);
        yield* session.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            yield* tx.retireDoc(ProviderDoc, conversationId);
          }),
        );
        // One pull per commit: frames pulled together are folded into one.
        const [retired] = yield* watch.frames.pipe(Stream.take(1), Stream.runCollect);
        const frames = [set, retired];
        assert.deepStrictEqual(frames[0]?.ops, [
          ["a", ["docs", "core.provider", "sessionId"], "s1"],
        ]);
        assert.deepStrictEqual(
          Delta.applyImmutableUnsafe(before, frames[0]!.ops),
          frames[0]?.value,
        );
        assert.deepStrictEqual(frames[1]?.ops, [["d", ["docs", "core.provider"]]]);
        assert.isFalse(Object.hasOwn(frames[1]?.value.docs ?? {}, "core.provider"));
        assert.strictEqual(frames[1]?.value.entries, before.entries);
      }),
    ),
  );

  it.effect("creates no revision for a publication that touches nothing mounted", () =>
    run(backend)(
      Effect.gen(function* () {
        const { session, conversationId, views } = yield* setup;
        const other = yield* createConversation(session);
        const observer = recorder();
        yield* views.attach(conversationId, undefined, observer.create);
        yield* append(session, other, "elsewhere");
        yield* setProviderSession(session, other, "s1");
        assert.lengthOf(observer.revisions, 0);
        yield* append(session, conversationId, "here");
        assert.lengthOf(observer.revisions, 1);
      }),
    ),
  );

  it.effect("shares one mount between observers and drops it with the last", () =>
    run(backend)(
      Effect.gen(function* () {
        const { session, conversationId, views } = yield* setup;
        const first = recorder();
        const second = recorder();
        const a = yield* acquire(views.attach(conversationId, undefined, first.create));
        const b = yield* acquire(views.attach(conversationId, undefined, second.create));
        // Each observer has its own window: an observer of a live mount shares its documents.
        assert.strictEqual(first.initial?.docs, second.initial?.docs);
        yield* append(session, conversationId, "shared");
        assert.lengthOf(first.revisions, 1);
        assert.deepStrictEqual(first.revisions[0]?.value, second.revisions[0]?.value);
        assert.deepStrictEqual(first.revisions[0]?.ops, second.revisions[0]?.ops);

        yield* a.release;
        yield* append(session, conversationId, "one left");
        assert.lengthOf(first.revisions, 1);
        assert.lengthOf(second.revisions, 2);
        const joined = recorder();
        const c = yield* acquire(views.attach(conversationId, undefined, joined.create));
        assert.deepStrictEqual(joined.initial, second.revisions[1]?.value);
        assert.strictEqual(joined.initial?.docs, second.revisions[1]?.value.docs);

        yield* b.release;
        yield* c.release;
        yield* append(session, conversationId, "unobserved");
        assert.lengthOf(second.revisions, 2);
        const third = recorder();
        yield* acquire(views.attach(conversationId, undefined, third.create));
        // The dropped mount is rebuilt: a new state, with the entry committed while nothing observed it. Its
        // conversation record is the Session's frozen cached one, so only the state is new.
        assert.notStrictEqual(third.initial, second.revisions[1]?.value);
        assert.deepStrictEqual(kinds(third.initial), ["shared", "one left", "unobserved"]);
      }),
    ),
  );

  it.effect("folds unpulled frames into one whose operations reach the newest view", () =>
    run(backend)(
      Effect.gen(function* () {
        const { session, conversationId, views } = yield* setup;
        const { value: watch } = yield* acquire(views.watch(conversationId));
        const before = watch.value;
        for (let index = 0; index < 101; index++) {
          yield* append(session, conversationId, `entry ${index}`);
        }
        const [frame] = yield* watch.frames.pipe(Stream.take(1), Stream.runCollect);
        assert.deepStrictEqual(Delta.applyImmutableUnsafe(before, frame!.ops), frame!.value);
        // The default window holds at most 100 entries: it spliced its oldest down to 50.
        assert.lengthOf(frame!.value.entries, 50);
        assert.strictEqual(kinds(frame!.value).at(-1), "entry 100");
        yield* append(session, conversationId, "after");
        const [next] = yield* watch.frames.pipe(Stream.take(1), Stream.runCollect);
        assert.deepStrictEqual(kinds(next!.value).at(-1), "after");
      }),
    ),
  );

  it.effect("ends only the stream whose consumer fails", () =>
    run(backend)(
      Effect.gen(function* () {
        const { session, conversationId, views } = yield* setup;
        const { value: failing } = yield* acquire(views.watch(conversationId));
        const { value: healthy } = yield* acquire(views.watch(conversationId));
        yield* append(session, conversationId, "a");
        const failed = yield* failing.frames.pipe(
          Stream.runForEach(() => Effect.fail("consumer broke")),
          Effect.flip,
        );
        assert.strictEqual(failed, "consumer broke");
        yield* append(session, conversationId, "b");
        assert.deepStrictEqual(yield* failing.frames.pipe(Stream.runCollect), []);
        // Both commits came before the first pull: one folded frame.
        const frames = yield* healthy.frames.pipe(Stream.take(1), Stream.runCollect);
        assert.deepStrictEqual(
          frames.map((frame) => kinds(frame.value)),
          [["a", "b"]],
        );
      }),
    ),
  );

  it.effect("reports a throwing attached observer and keeps feeding the mount's watches", () =>
    run(backend)(
      Effect.gen(function* () {
        const { session, conversationId, views, reports } = yield* setup;
        const failure = new Error("observer broke");
        yield* views.attach(conversationId, undefined, () => ({
          advance: () => {
            throw failure;
          },
          closeSession: () => {},
        }));
        const { value: watch } = yield* acquire(views.watch(conversationId));
        yield* append(session, conversationId, "note");
        const [frame] = yield* watch.frames.pipe(Stream.take(1), Stream.runCollect);
        assert.deepStrictEqual(kinds(frame!.value), ["note"]);
        assert.lengthOf(reports, 1);
        assert.strictEqual(Cause.squash(reports[0]!), failure);
        assert.isTrue(session.failure._tag === "None");
        yield* session.close;
        assert.deepStrictEqual(yield* watch.frames.pipe(Stream.runCollect), []);
      }),
    ),
  );

  it.effect("ends watches when the Session closes", () =>
    run(backend)(
      Effect.gen(function* () {
        const { session, conversationId, views } = yield* setup;
        const { value: watch } = yield* acquire(views.watch(conversationId));
        yield* append(session, conversationId, "a");
        const frames = yield* watch.frames.pipe(Stream.runCollect, Effect.forkChild);
        yield* flush;
        yield* session.close;
        // The watch keeps its last value and drops what is pending.
        assert.lengthOf(yield* Fiber.join(frames), 1);
        assert.deepStrictEqual(kinds(watch.value), ["a"]);
      }),
    ),
  );

  it.effect("rejects acquisition once close begins, also while the mount hydrates", () =>
    run(backend)(
      Effect.gen(function* () {
        const { session, conversationId, views, storage } = yield* setup;
        // The mount hydrates through the Session's document cache: with
        // the cache empty it loads from storage, where the gate holds it.
        yield* session.unloadDocuments;
        const gate = yield* storage.holdFindDocument;
        const pending = yield* views
          .watch(conversationId)
          .pipe(Effect.scoped, Effect.flip, Effect.forkChild);
        yield* gate.entered;
        const closing = yield* session.close.pipe(Effect.forkChild);
        yield* flush;
        yield* gate.release;
        assert.strictEqual((yield* Fiber.join(pending))._tag, "SessionClosed");
        yield* Fiber.join(closing);
        const later = yield* views.watch(conversationId).pipe(Effect.scoped, Effect.flip);
        assert.strictEqual(later._tag, "SessionClosed");
      }),
    ),
  );

  it.effect("publishes no frame from a commit that settles during close", () =>
    run(backend)(
      Effect.gen(function* () {
        const { session, conversationId, views, storage } = yield* setup;
        const observer = recorder();
        yield* views.attach(conversationId, undefined, observer.create);
        const { value: watch } = yield* acquire(views.watch(conversationId));
        const gate = yield* storage.holdCommits;
        const committed = yield* Deferred.make<void>();
        const commit = yield* append(session, conversationId, "late").pipe(
          Effect.ensuring(Deferred.succeed(committed, undefined)),
          Effect.forkChild,
        );
        yield* gate.entered;
        const closing = yield* session.close.pipe(Effect.forkChild);
        yield* flush;
        yield* gate.release;
        yield* Deferred.await(committed);
        assert.strictEqual((yield* Fiber.join(commit)).kind, "late");
        yield* Fiber.join(closing);
        assert.lengthOf(observer.revisions, 0);
        assert.deepStrictEqual(yield* watch.frames.pipe(Stream.runCollect), []);
      }),
    ),
  );

  it.effect("fails acquisition with the Session's failure", () =>
    run(backend)(
      Effect.gen(function* () {
        const { session, conversationId, views, storage } = yield* setup;
        const { value: watch } = yield* acquire(views.watch(conversationId));
        storage.failNextCommit("disk gone");
        yield* append(session, conversationId, "lost").pipe(Effect.flip);
        const ended = yield* watch.frames.pipe(Stream.runCollect, Effect.flip);
        assert.strictEqual(ended._tag, "SessionFailed");
        const later = yield* views.watch(conversationId).pipe(Effect.scoped, Effect.flip);
        assert.strictEqual(later._tag, "SessionFailed");
      }),
    ),
  );
});

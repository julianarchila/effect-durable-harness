// Type-level overload checks of the Session definitions, against the Effect
// signatures and `Doc.make` / `Doc.family` / `Entry.make`.
import { assert, describe, expectTypeOf, it } from "@effect/vitest";
import type * as Tracker from "@effect-durable-harness/delta/Tracker";
import type * as Effect from "effect/Effect";
import type * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import type * as Scope from "effect/Scope";
import * as Doc from "../src/Doc.ts";
import * as Entry from "../src/Entry.ts";
import { type ConversationId, type EntryId, ROOT_CONVERSATION_ID, TaskId } from "../src/Ids.ts";
import type { EntryRecord } from "../src/Records.ts";
import type { ReadError, Session } from "../src/Session.ts";
import type { Tx, TxDocError, TxReadError, TxWriteError } from "../src/Tx.ts";

const State = Schema.Struct({ value: Schema.Number });
type State = typeof State.Type;
const initial = (): State => ({ value: 0 });

const SessionDoc = Doc.make({
  checkpointWhen: () => false,
  kind: "t.session",
  version: 1,
  scope: "session",
  schema: State,
  initial,
});
const LatestDoc = Doc.make({
  checkpointWhen: () => false,
  kind: "t.latest",
  version: 1,
  scope: "conversation",
  history: "latest",
  fork: "current",
  schema: State,
  initial,
});
const RewindableDoc = Doc.make({
  checkpointWhen: () => false,
  kind: "t.rewindable",
  version: 1,
  scope: "conversation",
  history: "rewindable",
  fork: "asOf",
  schema: State,
  initial,
});
const TaskDoc = Doc.make({
  checkpointWhen: () => false,
  kind: "t.task",
  version: 1,
  scope: "task",
  schema: State,
  initial,
});
const SessionFamily = Doc.family({
  checkpointWhen: () => false,
  kind: "t.session-family",
  version: 1,
  family: true,
  scope: "session",
  schema: State,
  initial: (seed: number) => ({ value: seed }),
});
const ConversationFamily = Doc.family({
  checkpointWhen: () => false,
  kind: "t.conversation-family",
  version: 1,
  family: true,
  scope: "conversation",
  history: "rewindable",
  fork: "initial",
  schema: State,
  initial: (seed: number) => ({ value: seed }),
});
const TaskFamily = Doc.family({
  checkpointWhen: () => false,
  kind: "t.task-family",
  version: 1,
  family: true,
  scope: "task",
  schema: State,
  initial: (seed: number) => ({ value: seed }),
});

type Draft = Effect.Effect<Tracker.Draft<State>, TxDocError>;
type Retired = Effect.Effect<void, TxDocError>;
type Snapshot = Effect.Effect<Option.Option<Readonly<State>>, ReadError>;

describe("document definitions", () => {
  it("requires an explicit checkpoint predicate for documents and families", () => {
    const check = () => {
      const definition = {
        kind: "t.missing-checkpoint",
        version: 1,
        scope: "session",
        schema: State,
        initial,
      } as const;
      // @ts-expect-error document definitions require checkpointWhen
      Doc.make(definition);
      const family = {
        kind: "t.missing-family-checkpoint",
        version: 1,
        family: true,
        scope: "session",
        schema: State,
        initial: (seed: number) => ({ value: seed }),
      } as const;
      // @ts-expect-error document family definitions require checkpointWhen
      Doc.family(family);
    };
    assert.isFunction(check);
  });

  it("validates persisted version semantics", () => {
    assert.throws(
      () =>
        Doc.make({
          checkpointWhen: () => false,
          kind: "k",
          version: 0,
          scope: "session",
          schema: State,
          initial,
        }),
      "positive integer",
    );
    assert.throws(
      () =>
        Doc.make({
          checkpointWhen: () => false,
          kind: "k",
          version: 1.5,
          scope: "session",
          schema: State,
          initial,
        }),
      "positive integer",
    );
    assert.strictEqual(
      Doc.make({
        checkpointWhen: () => false,
        kind: "",
        version: 1,
        scope: "session",
        schema: State,
        initial,
      }).definition.kind,
      "",
    );
    assert.strictEqual(SessionDoc.definition.kind, "t.session");
  });

  it("types every owner, key, and seed overload", () => {
    const check = (tx: Tx["Service"], conversationId: ConversationId, taskId: TaskId): void => {
      expectTypeOf(tx.doc(SessionDoc)).toEqualTypeOf<Draft>();
      expectTypeOf(tx.doc(LatestDoc, conversationId)).toEqualTypeOf<Draft>();
      expectTypeOf(tx.doc(RewindableDoc, conversationId)).toEqualTypeOf<Draft>();
      expectTypeOf(tx.doc(TaskDoc, taskId)).toEqualTypeOf<Draft>();
      expectTypeOf(tx.doc(SessionFamily, "k", 1)).toEqualTypeOf<Draft>();
      expectTypeOf(tx.doc(ConversationFamily, conversationId, "k", 1)).toEqualTypeOf<Draft>();
      expectTypeOf(tx.doc(TaskFamily, taskId, "k", 1)).toEqualTypeOf<Draft>();
      expectTypeOf(tx.retireDoc(SessionDoc)).toEqualTypeOf<Retired>();
      expectTypeOf(tx.retireDoc(LatestDoc, conversationId)).toEqualTypeOf<Retired>();
      expectTypeOf(tx.retireDoc(TaskDoc, taskId)).toEqualTypeOf<Retired>();
      expectTypeOf(tx.retireDoc(SessionFamily, "k")).toEqualTypeOf<Retired>();
      expectTypeOf(tx.retireDoc(ConversationFamily, conversationId, "k")).toEqualTypeOf<Retired>();
      expectTypeOf(tx.retireDoc(TaskFamily, taskId, "k")).toEqualTypeOf<Retired>();

      // @ts-expect-error Session documents take no owner
      void tx.doc(SessionDoc, conversationId);
      // @ts-expect-error conversation documents require a conversation ID
      void tx.doc(LatestDoc);
      // @ts-expect-error family access requires a creation seed
      void tx.doc(SessionFamily, "k");
      // @ts-expect-error family seeds are typed
      void tx.doc(SessionFamily, "k", "seed");
      // @ts-expect-error family retirement takes no seed
      void tx.retireDoc(TaskFamily, taskId, "k", 1);
    };
    assert.isFunction(check);

    const reads = (session: Session["Service"]): void => {
      const taskId = TaskId.make(1);
      expectTypeOf(session.snapshot(SessionDoc)).toEqualTypeOf<Snapshot>();
      expectTypeOf(session.snapshot(LatestDoc, ROOT_CONVERSATION_ID)).toEqualTypeOf<Snapshot>();
      expectTypeOf(session.snapshot(TaskDoc, taskId)).toEqualTypeOf<Snapshot>();
      expectTypeOf(session.snapshot(SessionFamily, "k")).toEqualTypeOf<Snapshot>();
      expectTypeOf(
        session.snapshot(ConversationFamily, ROOT_CONVERSATION_ID, "k"),
      ).toEqualTypeOf<Snapshot>();
      expectTypeOf(session.snapshot(TaskFamily, taskId, "k")).toEqualTypeOf<Snapshot>();
      // @ts-expect-error snapshots never take a creation seed
      void session.snapshot(SessionFamily, "k", 1);
    };
    assert.isFunction(reads);
  });

  it("types historical reads, watches, and typed entries", () => {
    const Note = Entry.make("t.note", { data: Schema.Struct({ text: Schema.String }) });
    const Marker = Entry.make("t.marker");
    type WatchOf = Effect.Effect<Option.Option<Doc.DocumentWatch<State>>, ReadError, Scope.Scope>;
    const check = (
      session: Session["Service"],
      tx: Tx["Service"],
      conversationId: ConversationId,
      taskId: TaskId,
      at: EntryId,
    ): void => {
      expectTypeOf(
        session.snapshotAsOf(RewindableDoc, conversationId, at),
      ).toEqualTypeOf<Snapshot>();
      expectTypeOf(
        session.snapshotAsOf(ConversationFamily, conversationId, "k", at),
      ).toEqualTypeOf<Snapshot>();
      // @ts-expect-error latest conversation documents keep no history
      void session.snapshotAsOf(LatestDoc, conversationId, at);
      // @ts-expect-error Session documents keep no history
      void session.snapshotAsOf(SessionDoc, conversationId, at);
      // @ts-expect-error task documents keep no history
      void session.snapshotAsOf(TaskDoc, taskId, at);

      expectTypeOf(session.watchDoc(SessionDoc)).toEqualTypeOf<WatchOf>();
      expectTypeOf(session.watchDoc(LatestDoc, conversationId)).toEqualTypeOf<WatchOf>();
      expectTypeOf(session.watchDoc(TaskDoc, taskId)).toEqualTypeOf<WatchOf>();
      expectTypeOf(session.watchDoc(SessionFamily, "k")).toEqualTypeOf<WatchOf>();
      expectTypeOf(
        session.watchDoc(ConversationFamily, conversationId, "k"),
      ).toEqualTypeOf<WatchOf>();
      expectTypeOf(session.watchDoc(TaskFamily, taskId, "k")).toEqualTypeOf<WatchOf>();
      // @ts-expect-error a watch of a Session document takes no owner
      void session.watchDoc(SessionDoc, conversationId);

      expectTypeOf(tx.entry(at)).toEqualTypeOf<
        Effect.Effect<Option.Option<EntryRecord>, TxReadError>
      >();
      expectTypeOf(tx.entry(Note, at)).toEqualTypeOf<
        Effect.Effect<Option.Option<Entry.TypedEntry<{ readonly text: string }>>, TxReadError>
      >();
      expectTypeOf(tx.appendEntry(conversationId, { kind: "t.raw" })).toEqualTypeOf<
        Effect.Effect<EntryRecord, TxWriteError>
      >();
      const note = tx.appendEntry(Note, conversationId, { data: { text: "x" } });
      expectTypeOf(note).toEqualTypeOf<
        Effect.Effect<Entry.TypedEntry<{ readonly text: string }>, TxWriteError>
      >();
      expectTypeOf<Effect.Success<typeof note>["data"]["text"]>().toEqualTypeOf<string>();
      expectTypeOf(tx.appendEntry(Marker, conversationId, {})).toEqualTypeOf<
        Effect.Effect<Entry.TypedEntry<never>, TxWriteError>
      >();
      // @ts-expect-error a data entry requires its data
      void tx.appendEntry(Note, conversationId, {});
      // @ts-expect-error data is typed by the kind
      void tx.appendEntry(Note, conversationId, { data: { text: 1 } });
      // @ts-expect-error the kind supplies `kind`
      void tx.appendEntry(Note, conversationId, { kind: "t.note", data: { text: "x" } });
      // @ts-expect-error an entry kind without data takes none
      void tx.appendEntry(Marker, conversationId, { data: 1 });
    };
    assert.isFunction(check);
  });
});

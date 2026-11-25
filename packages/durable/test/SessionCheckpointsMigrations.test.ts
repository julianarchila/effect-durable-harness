// Session checkpoints and migrations, on every backend. A throwing checkpoint
// predicate is a defect that rolls the commit back.
import { assert, describe, it } from "@effect/vitest";
import type * as Delta from "@effect-durable-harness/delta/Delta";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import type { StorageWrite } from "../src/AgentStorage.ts";
import * as Doc from "../src/Doc.ts";
import type { EntryId } from "../src/Ids.ts";
import { Tx, type TxDocError } from "../src/Tx.ts";
import {
  acquire,
  createConversation,
  describeEachBackend,
  documentChanges,
  failureMessage,
  flush,
  openTestSession,
  run,
} from "./SessionSupport.ts";

const Count = Schema.Struct({ count: Schema.Number });

const documentWrites = (writes: ReadonlyArray<StorageWrite>): ReadonlyArray<StorageWrite> =>
  writes.filter(
    (write) =>
      write.type === "document.create" ||
      write.type === "document.change" ||
      write.type === "document.retire",
  );

const changeKinds = (writes: ReadonlyArray<StorageWrite>) =>
  writes.flatMap((write) => (write.type === "document.change" ? [write.content.kind] : []));

/** Touches a document in its own commit, creating it when absent. */
const touch = <T>(token: Doc.SessionDoc<T>) =>
  Effect.gen(function* () {
    const tx = yield* Tx;
    yield* tx.doc(token);
  });

describeEachBackend("Session document checkpoints", (backend) => {
  it.effect("calls checkpointWhen with its definition as the receiver", () =>
    run(backend)(
      Effect.gen(function* () {
        const CheckpointDoc = Doc.make({
          kind: "checkpoint.receiver",
          version: 1,
          scope: "session",
          schema: Count,
          initial: () => ({ count: 0 }),
          checkpointWhen(value) {
            // `this` is typed before `T` is inferred, so the initial value reads as plain JSON here.
            return value.count === Number(this.initial()["count"]) + 2;
          },
        });
        const { session, storage } = yield* openTestSession();
        for (let count = 0; count <= 2; count++) {
          yield* session.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              (yield* tx.doc(CheckpointDoc)).count = count;
            }),
          );
        }
        assert.deepStrictEqual(storage.commits.flatMap(changeKinds), ["delta", "base"]);
      }),
    ),
  );

  it.effect(
    "selects bases only for nonempty ordinary batches and passes the exact prepared revision and ops",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const calls: Array<{ value: Schema.JsonObject; ops: Delta.Delta }> = [];
          const falseCalls: Array<{ value: Schema.JsonObject; ops: Delta.Delta }> = [];
          const BaseDoc = Doc.make({
            kind: "checkpoint.base",
            version: 1,
            scope: "session",
            schema: Schema.Struct({ items: Schema.Array(Schema.String) }),
            initial: () => ({ items: [] }),
            checkpointWhen: (value, ops) => {
              calls.push({ value, ops });
              return true;
            },
          });
          const DeltaDoc = Doc.make({
            kind: "checkpoint.delta",
            version: 1,
            scope: "session",
            schema: Count,
            initial: () => ({ count: 0 }),
            checkpointWhen: (value, ops) => {
              falseCalls.push({ value, ops });
              return false;
            },
          });
          const DefaultDoc = Doc.make({
            checkpointWhen: () => false,
            kind: "checkpoint.default",
            version: 1,
            scope: "session",
            schema: Count,
            initial: () => ({ count: 0 }),
          });
          const { session, storage, publications } = yield* openTestSession();

          yield* session.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              yield* tx.doc(BaseDoc);
              yield* tx.doc(DeltaDoc);
              yield* tx.doc(DefaultDoc);
            }),
          );
          assert.lengthOf(calls, 0);
          assert.lengthOf(falseCalls, 0);
          assert.isTrue(
            documentWrites(storage.commits.at(-1)!).every(
              (write) => write.type === "document.create" && write.content.kind === "base",
            ),
          );

          yield* session.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              (yield* tx.doc(BaseDoc)).items.push("x");
              (yield* tx.doc(DeltaDoc)).count++;
              (yield* tx.doc(DefaultDoc)).count++;
            }),
          );
          const writes = storage.admittedCommits
            .at(-1)!
            .filter((write) => write.type === "document.change");
          assert.deepStrictEqual(changeKinds(writes), ["base", "delta", "delta"]);
          assert.lengthOf(calls, 1);
          assert.lengthOf(falseCalls, 1);
          const snapshot = Option.getOrThrow(yield* session.snapshot(BaseDoc));
          const deltaSnapshot = Option.getOrThrow(yield* session.snapshot(DeltaDoc));
          const [published, publishedDelta] = documentChanges(publications.at(-1)!);
          assert.strictEqual(calls[0]!.value, snapshot);
          assert.strictEqual(calls[0]!.ops, published!.ops);
          assert.strictEqual(falseCalls[0]!.value, deltaSnapshot);
          assert.strictEqual(falseCalls[0]!.ops, publishedDelta!.ops);
          const delta = writes[1]!;
          if (delta.type !== "document.change" || delta.content.kind !== "delta") {
            throw new Error("Expected ordinary delta");
          }
          assert.strictEqual(delta.content.ops, falseCalls[0]!.ops);
          const base = writes[0]!;
          if (base.type !== "document.change" || base.content.kind !== "base") {
            throw new Error("Expected checkpoint base");
          }
          assert.strictEqual(base.content.value, snapshot);
        }),
      ),
  );

  it.effect(
    "passes the stored delta count since the newest base, including after unload and version bases",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const seen: Array<number> = [];
          const V1 = Doc.make({
            kind: "checkpoint.deltas-since-base",
            version: 1,
            scope: "session",
            schema: Count,
            initial: () => ({ count: 0 }),
            checkpointWhen: (_value, _ops, info) => {
              seen.push(info.deltasSinceBase);
              return info.deltasSinceBase >= 2;
            },
          });
          const V2 = Doc.make({
            kind: "checkpoint.deltas-since-base",
            version: 2,
            scope: "session",
            schema: Count,
            initial: () => ({ count: 0 }),
            migrate: (value) => ({ count: Number(value["count"]) }),
            checkpointWhen: (_value, _ops, info) => {
              seen.push(info.deltasSinceBase);
              return false;
            },
          });
          const { session, storage } = yield* openTestSession();
          const increment = (token: typeof V1) =>
            session.commit(
              Effect.gen(function* () {
                const tx = yield* Tx;
                (yield* tx.doc(token)).count++;
              }),
            );
          yield* session.commit(touch(V1));
          yield* increment(V1);
          yield* increment(V1);
          yield* increment(V1);
          yield* session.unloadDocuments;
          yield* increment(V1);
          assert.deepStrictEqual(seen, [0, 1, 2, 0]);
          const kinds = storage.commits
            .slice(-4)
            .map((writes) =>
              writes[0]!.type === "document.change" ? writes[0]!.content.kind : writes[0]!.type,
            );
          assert.deepStrictEqual(kinds, ["delta", "delta", "base", "delta"]);

          // A required version base resets the count without calling the predicate.
          yield* increment(V2);
          yield* increment(V2);
          assert.deepStrictEqual(seen, [0, 1, 2, 0, 0]);
          const versionBase = storage.commits.at(-2)![0]!;
          assert.isTrue(
            versionBase.type === "document.change" &&
              versionBase.content.kind === "base" &&
              versionBase.content.version === 2,
          );
        }),
      ),
  );

  it.effect(
    "skips the predicate for empty batches but calls it for nonempty structural no-ops",
    () =>
      run(backend)(
        Effect.gen(function* () {
          let calls = 0;
          const ItemsDoc = Doc.make({
            kind: "checkpoint.no-op",
            version: 1,
            scope: "session",
            schema: Schema.Struct({ items: Schema.Array(Schema.String) }),
            initial: () => ({ items: ["a", "b"] }),
            checkpointWhen: () => {
              calls++;
              return false;
            },
          });
          const { session, storage } = yield* openTestSession();
          yield* session.commit(touch(ItemsDoc));
          const commits = storage.commits.length;

          yield* session.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              const value = yield* tx.doc(ItemsDoc);
              value.items.push("x");
              value.items.pop();
            }),
          );
          assert.strictEqual(calls, 0);
          assert.lengthOf(storage.commits, commits);

          yield* session.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              const value = yield* tx.doc(ItemsDoc);
              const first = value.items.shift()!;
              value.items.unshift(first);
            }),
          );
          assert.strictEqual(calls, 1);
          assert.deepStrictEqual(changeKinds(storage.commits.at(-1)!), ["delta"]);
        }),
      ),
  );

  it.effect("rolls back every prepared document when a checkpoint predicate throws", () =>
    run(backend)(
      Effect.gen(function* () {
        let throwCheckpoint = true;
        let firstCalls = 0;
        const First = Doc.make({
          kind: "checkpoint.rollback.first",
          version: 1,
          scope: "session",
          schema: Count,
          initial: () => ({ count: 0 }),
          checkpointWhen: () => {
            firstCalls++;
            return false;
          },
        });
        const Second = Doc.make({
          kind: "checkpoint.rollback.second",
          version: 1,
          scope: "session",
          schema: Count,
          initial: () => ({ count: 0 }),
          checkpointWhen: () => {
            if (throwCheckpoint) throw new Error("checkpoint failed");
            return false;
          },
        });
        const { session, storage, publications } = yield* openTestSession();
        yield* session.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            yield* tx.doc(First);
            yield* tx.doc(Second);
          }),
        );
        const first = Option.getOrThrow(yield* session.snapshot(First));
        const second = Option.getOrThrow(yield* session.snapshot(Second));
        const commits = storage.commits.length;
        const published = publications.length;

        const set = (a: number, b: number) =>
          session.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              (yield* tx.doc(First)).count = a;
              (yield* tx.doc(Second)).count = b;
            }),
          );
        // A throw in `checkpointWhen` is a defect: the commit rolls back and the Session stays healthy.
        assert.include(yield* failureMessage(set(1, 2)), "checkpoint failed");
        assert.strictEqual(firstCalls, 1);
        assert.lengthOf(storage.commits, commits);
        assert.lengthOf(publications, published);
        assert.strictEqual(Option.getOrThrow(yield* session.snapshot(First)), first);
        assert.strictEqual(Option.getOrThrow(yield* session.snapshot(Second)), second);

        throwCheckpoint = false;
        yield* set(3, 4);
        assert.deepStrictEqual(Option.getOrThrow(yield* session.snapshot(First)), { count: 3 });
        assert.deepStrictEqual(Option.getOrThrow(yield* session.snapshot(Second)), { count: 4 });
      }),
    ),
  );

  it.effect("persists repeated false decisions as deltas and replays the complete tail", () =>
    run(backend)(
      Effect.gen(function* () {
        let calls = 0;
        const TailDoc = Doc.make({
          kind: "checkpoint.tail",
          version: 1,
          scope: "session",
          schema: Schema.Struct({ values: Schema.Array(Schema.Number) }),
          initial: () => ({ values: [] }),
          checkpointWhen: () => {
            calls++;
            return false;
          },
        });
        const { session, storage } = yield* openTestSession();
        yield* session.commit(touch(TailDoc));
        for (let value = 1; value <= 8; value++) {
          yield* session.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              (yield* tx.doc(TailDoc)).values.push(value);
            }),
          );
        }
        const changes = storage.commits.flat().filter((write) => write.type === "document.change");
        assert.lengthOf(changes, 8);
        assert.deepStrictEqual(
          changeKinds(changes),
          Array.from({ length: 8 }, () => "delta"),
        );
        assert.strictEqual(calls, 8);
        yield* session.unloadDocuments;
        assert.deepStrictEqual(Option.getOrThrow(yield* session.snapshot(TailDoc)), {
          values: [1, 2, 3, 4, 5, 6, 7, 8],
        });
      }),
    ),
  );

  it.effect("keeps a prepared root replacement as a delta when the predicate is false", () =>
    run(backend)(
      Effect.gen(function* () {
        const initial: Record<string, number> = {};
        for (let index = 0; index < 4_100; index++) initial[`field${index}`] = 0;
        const WideDoc = Doc.make({
          kind: "checkpoint.root-replacement",
          version: 1,
          scope: "session",
          schema: Schema.Record(Schema.String, Schema.Number),
          initial: () => initial,
          checkpointWhen: () => false,
        });
        const { session, storage } = yield* openTestSession();
        yield* session.commit(touch(WideDoc));
        yield* session.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            const value = yield* tx.doc(WideDoc);
            for (let index = 0; index < 4_100; index++) value[`field${index}`] = 1;
          }),
        );
        const write = storage.commits.at(-1)![0]!;
        if (write.type !== "document.change" || write.content.kind !== "delta") {
          throw new Error("Expected root replacement delta");
        }
        assert.lengthOf(write.content.ops, 1);
        assert.strictEqual(write.content.ops[0]![0], "r");
        yield* session.unloadDocuments;
        assert.strictEqual(Option.getOrThrow(yield* session.snapshot(WideDoc))["field4099"], 1);
      }),
    ),
  );

  it.effect("uses ordinary checkpoint selection before retirement", () =>
    run(backend)(
      Effect.gen(function* () {
        let calls = 0;
        const RetiredDoc = Doc.make({
          kind: "checkpoint.retire",
          version: 1,
          scope: "session",
          schema: Count,
          initial: () => ({ count: 0 }),
          checkpointWhen: () => {
            calls++;
            return true;
          },
        });
        const { session, storage } = yield* openTestSession();
        yield* session.commit(touch(RetiredDoc));
        yield* session.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            (yield* tx.doc(RetiredDoc)).count = 1;
            yield* tx.retireDoc(RetiredDoc);
          }),
        );
        assert.strictEqual(calls, 1);
        const writes = documentWrites(storage.commits.at(-1)!);
        assert.deepStrictEqual(
          writes.map((write) => write.type),
          ["document.change", "document.retire"],
        );
        assert.deepStrictEqual(changeKinds(writes), ["base"]);
      }),
    ),
  );
});

describeEachBackend("Session document migrations", (backend) => {
  it.effect(
    "migrates read-only once per cold load, copies the callback result, and writes nothing",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const Old = Doc.make({
            checkpointWhen: () => false,
            kind: "migration.read-only",
            version: 1,
            scope: "session",
            schema: Count,
            initial: () => ({ count: 2 }),
          });
          let calls = 0;
          let retained: { count: number; labels: Array<string> } | undefined;
          const Current = Doc.make({
            checkpointWhen: () => false,
            kind: "migration.read-only",
            version: 3,
            scope: "session",
            schema: Schema.Struct({ count: Schema.Number, labels: Schema.Array(Schema.String) }),
            initial: () => ({ count: 0, labels: [] }),
            migrate: (value, fromVersion) => {
              assert.strictEqual(fromVersion, 1);
              calls++;
              retained = { count: Number(value["count"]), labels: ["migrated"] };
              return retained;
            },
          });
          const { session, storage } = yield* openTestSession();
          yield* session.commit(touch(Old));
          yield* session.unloadDocuments;
          const commits = storage.commits.length;

          const first = Option.getOrThrow(yield* session.snapshot(Current));
          assert.strictEqual(Option.getOrThrow(yield* session.snapshot(Current)), first);
          assert.strictEqual(calls, 1);
          assert.lengthOf(storage.commits, commits);
          retained!.count = 99;
          retained!.labels.push("mutated");
          assert.deepStrictEqual(first, { count: 2, labels: ["migrated"] });

          yield* session.unloadDocuments;
          const second = Option.getOrThrow(yield* session.snapshot(Current));
          assert.notStrictEqual(second, first);
          assert.deepStrictEqual(second, first);
          assert.strictEqual(calls, 2);
          assert.lengthOf(storage.commits, commits);
        }),
      ),
  );

  it.effect(
    "writes the required base on the first successful transaction, then writes deltas",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const Old = Doc.make({
            checkpointWhen: () => false,
            kind: "migration.transition",
            version: 1,
            scope: "session",
            schema: Count,
            initial: () => ({ count: 4 }),
          });
          let checkpoints = 0;
          const Current = Doc.make({
            kind: "migration.transition",
            version: 3,
            scope: "session",
            schema: Count,
            initial: () => ({ count: 0 }),
            migrate: (value, fromVersion) => ({ count: Number(value["count"]) + fromVersion - 1 }),
            checkpointWhen: () => {
              checkpoints++;
              return false;
            },
          });
          const { session, storage, publications } = yield* openTestSession();
          yield* session.commit(touch(Old));
          yield* session.unloadDocuments;
          // An observer of the older shape.
          const older = yield* acquire(session.watchDoc(Old));
          const frames: Array<unknown> = [];
          const olderConsumer = yield* older.value.frames.pipe(
            Stream.runForEach((frame) => Effect.sync(() => frames.push(frame))),
            Effect.forkChild,
          );
          assert.deepStrictEqual(Option.getOrThrow(yield* session.snapshot(Current)), { count: 4 });
          const snapshot = Option.getOrThrow(yield* session.snapshot(Current));
          // An observer of the new shape: the migration changes nothing it sees.
          const current = yield* acquire(session.watchDoc(Current));
          const currentFrames: Array<unknown> = [];
          const currentConsumer = yield* current.value.frames.pipe(
            Stream.runForEach((frame) => Effect.sync(() => currentFrames.push(frame.value))),
            Effect.forkChild,
          );

          yield* session.commit(touch(Current));
          yield* flush;
          const write = storage.commits.at(-1)![0]!;
          assert.isTrue(write.type === "document.change" && write.content.kind === "base");
          if (write.type === "document.change" && write.content.kind === "base") {
            assert.deepStrictEqual(write.content, {
              kind: "base",
              version: 3,
              value: { count: 4 },
            });
          }
          assert.strictEqual(checkpoints, 0);
          // The migration-only base is published, so the older-shape watch receives the new value as a root replacement.
          const [published] = documentChanges(publications.at(-1)!);
          assert.strictEqual(published!.version, 3);
          assert.deepStrictEqual(published!.value, { count: 4 });
          assert.deepStrictEqual(published!.ops, []);
          assert.deepStrictEqual(frames, [
            { value: { count: 4 }, ops: [["r", { count: 4 }]], retired: false },
          ]);
          assert.deepStrictEqual(currentFrames, []);
          yield* older.release;
          yield* current.release;
          yield* Fiber.join(olderConsumer);
          yield* Fiber.join(currentConsumer);
          assert.strictEqual(Option.getOrThrow(yield* session.snapshot(Current)), snapshot);

          yield* session.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              (yield* tx.doc(Current)).count = 7;
            }),
          );
          const delta = storage.commits.at(-1)![0]!;
          assert.isTrue(
            delta.type === "document.change" &&
              delta.content.kind === "delta" &&
              delta.content.version === 3,
          );
          assert.strictEqual(checkpoints, 1);
          yield* session.unloadDocuments;
          assert.deepStrictEqual(Option.getOrThrow(yield* session.snapshot(Current)), { count: 7 });
        }),
      ),
  );

  it.effect(
    "rolls migration and edits back with the callback, then coalesces later edits into one base",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const Old = Doc.make({
            checkpointWhen: () => false,
            kind: "migration.rollback",
            version: 1,
            scope: "session",
            schema: Count,
            initial: () => ({ count: 1 }),
          });
          let migrations = 0;
          const Current = Doc.make({
            checkpointWhen: () => false,
            kind: "migration.rollback",
            version: 2,
            scope: "session",
            schema: Schema.Struct({ count: Schema.Number, migrated: Schema.Boolean }),
            initial: () => ({ count: 0, migrated: false }),
            migrate: (value) => {
              migrations++;
              return { count: Number(value["count"]), migrated: true };
            },
          });
          const { session, storage, publications } = yield* openTestSession();
          yield* session.commit(touch(Old));
          yield* session.unloadDocuments;
          const commits = storage.commits.length;

          assert.include(
            yield* failureMessage(
              session.commit(
                Effect.gen(function* () {
                  const tx = yield* Tx;
                  (yield* tx.doc(Current)).count = 8;
                  return yield* Effect.fail("rollback");
                }),
              ),
            ),
            "rollback",
          );
          assert.lengthOf(storage.commits, commits);
          assert.deepStrictEqual(Option.getOrThrow(yield* session.snapshot(Current)), {
            count: 1,
            migrated: true,
          });
          assert.strictEqual(migrations, 1);

          yield* session.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              const value = yield* tx.doc(Current);
              value.count = 9;
              value.migrated = false;
            }),
          );
          const writes = documentWrites(storage.commits.at(-1)!);
          assert.lengthOf(writes, 1);
          const write = writes[0]!;
          if (write.type !== "document.change") throw new Error("expected a change");
          assert.deepStrictEqual(write.content, {
            kind: "base",
            version: 2,
            value: { count: 9, migrated: false },
          });
          const published = documentChanges(publications.at(-1)!)[0]!;
          const admitted = storage.admittedCommits.at(-1)![0]!;
          if (admitted.type !== "document.change" || admitted.content.kind !== "base") {
            throw new Error("Expected migration base");
          }
          assert.strictEqual(published.value, Option.getOrThrow(yield* session.snapshot(Current)));
          assert.strictEqual(published.value, admitted.content.value);
          assert.isAbove(published.ops.length, 0);
          assert.strictEqual(migrations, 1);
        }),
      ),
  );

  it.effect(
    "strict-checks migration results before tracker ownership and remains usable after rejection",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const Old = Doc.make({
            checkpointWhen: () => false,
            kind: "migration.invalid",
            version: 1,
            scope: "session",
            schema: Count,
            initial: () => ({ count: 1 }),
          });
          const Invalid = Doc.make({
            checkpointWhen: () => false,
            kind: "migration.invalid",
            version: 2,
            scope: "session",
            // `Schema.Any` lets `migrate` return a class instance, to check the migration copy.
            schema: Schema.Struct({ invalid: Schema.Any }),
            initial: () => ({ invalid: null }),
            migrate: () => ({ invalid: new Map() }),
          });
          const Other = Doc.make({
            checkpointWhen: () => false,
            kind: "migration.invalid.other",
            version: 1,
            scope: "session",
            schema: Schema.Struct({ ok: Schema.Boolean }),
            initial: () => ({ ok: true }),
          });
          const { session, storage } = yield* openTestSession();
          yield* session.commit(touch(Old));
          yield* session.unloadDocuments;
          const commits = storage.commits.length;
          const read = yield* Effect.flip(session.snapshot(Invalid));
          assert.strictEqual(read._tag, "DocumentAccessError");
          assert.include(read.message, "strict JSON");
          assert.include(yield* failureMessage(session.commit(touch(Invalid))), "strict JSON");
          assert.lengthOf(storage.commits, commits);
          yield* session.commit(touch(Other));
          assert.deepStrictEqual(Option.getOrThrow(yield* session.snapshot(Other)), { ok: true });
        }),
      ),
  );

  it.effect(
    "rejects newer stored versions and older versions without migration for snapshots and transactions",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const V2 = Doc.make({
            checkpointWhen: () => false,
            kind: "migration.compatibility",
            version: 2,
            scope: "session",
            schema: Count,
            initial: () => ({ count: 2 }),
          });
          const V1 = Doc.make({
            checkpointWhen: () => false,
            kind: "migration.compatibility",
            version: 1,
            scope: "session",
            schema: Count,
            initial: () => ({ count: 1 }),
          });
          const V3WithoutMigration = Doc.make({
            checkpointWhen: () => false,
            kind: "migration.compatibility",
            version: 3,
            scope: "session",
            schema: Count,
            initial: () => ({ count: 3 }),
          });
          const { session, storage } = yield* openTestSession();
          yield* session.commit(touch(V2));
          yield* session.unloadDocuments;
          const commits = storage.commits.length;

          assert.include(yield* failureMessage(session.snapshot(V1)), "newer version 2 than 1");
          assert.include(
            yield* failureMessage(session.commit(touch(V1))),
            "newer version 2 than 1",
          );
          assert.include(
            yield* failureMessage(session.snapshot(V3WithoutMigration)),
            "requires migration from version 2",
          );
          assert.include(
            yield* failureMessage(session.commit(touch(V3WithoutMigration))),
            "requires migration from version 2",
          );
          assert.lengthOf(storage.commits, commits);
        }),
      ),
  );

  it.effect(
    "persists a required migration base before retirement without consulting the checkpoint predicate",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const Old = Doc.make({
            checkpointWhen: () => false,
            kind: "migration.retire",
            version: 1,
            scope: "session",
            schema: Count,
            initial: () => ({ count: 1 }),
          });
          const Current = Doc.make({
            kind: "migration.retire",
            version: 2,
            scope: "session",
            schema: Count,
            initial: () => ({ count: 0 }),
            migrate: (value) => ({ count: Number(value["count"]) }),
            checkpointWhen: () => {
              throw new Error("must not run");
            },
          });
          const { session, storage } = yield* openTestSession();
          yield* session.commit(touch(Old));
          yield* session.unloadDocuments;
          yield* session.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              yield* tx.doc(Current);
              yield* tx.retireDoc(Current);
            }),
          );
          const writes = documentWrites(storage.commits.at(-1)!);
          assert.deepStrictEqual(
            writes.map((write) => write.type),
            ["document.change", "document.retire"],
          );
          const base = writes[0]!;
          if (base.type !== "document.change") throw new Error("expected a change");
          assert.deepStrictEqual(base.content, { kind: "base", version: 2, value: { count: 1 } });
        }),
      ),
  );

  it.effect("leaves unaccessed older documents and unavailable definitions untouched", () =>
    run(backend)(
      Effect.gen(function* () {
        const FirstV1 = Doc.make({
          checkpointWhen: () => false,
          kind: "migration.lazy.first",
          version: 1,
          scope: "session",
          schema: Count,
          initial: () => ({ count: 1 }),
        });
        const SecondV1 = Doc.make({
          checkpointWhen: () => false,
          kind: "migration.lazy.second",
          version: 1,
          scope: "session",
          schema: Count,
          initial: () => ({ count: 2 }),
        });
        let secondMigrations = 0;
        const FirstV2 = Doc.make({
          checkpointWhen: () => false,
          kind: "migration.lazy.first",
          version: 2,
          scope: "session",
          schema: Count,
          initial: () => ({ count: 0 }),
          migrate: (value) => ({ count: Number(value["count"]) }),
        });
        Doc.make({
          checkpointWhen: () => false,
          kind: "migration.lazy.second",
          version: 2,
          scope: "session",
          schema: Count,
          initial: () => ({ count: 0 }),
          migrate: (value) => {
            secondMigrations++;
            return { count: Number(value["count"]) };
          },
        });
        const { session, storage } = yield* openTestSession();
        yield* session.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            yield* tx.doc(FirstV1);
            yield* tx.doc(SecondV1);
          }),
        );
        yield* session.unloadDocuments;
        const commits = storage.commits.length;
        assert.deepStrictEqual(Option.getOrThrow(yield* session.snapshot(FirstV2)), { count: 1 });
        assert.lengthOf(storage.commits, commits);
        assert.strictEqual(secondMigrations, 0);
        const secondRecord = Option.getOrThrow(
          yield* storage.findDocument(
            { kind: "migration.lazy.second", scope: { kind: "session" } },
            "current",
          ),
        );
        assert.strictEqual(
          Option.getOrThrow(yield* storage.document(secondRecord.id, "current")).version,
          1,
        );
      }),
    ),
  );
});

describeEachBackend("Session historical document snapshots", (backend) => {
  it.effect(
    "migrates current and historical rewindable values independently and follows fork ancestry",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const V1 = Doc.make({
            checkpointWhen: () => false,
            kind: "history.migration",
            version: 1,
            scope: "conversation",
            history: "rewindable",
            fork: "asOf",
            schema: Count,
            initial: () => ({ count: 0 }),
          });
          const Family = Doc.family({
            checkpointWhen: () => false,
            kind: "history.family",
            version: 1,
            family: true,
            scope: "conversation",
            history: "rewindable",
            fork: "asOf",
            schema: Schema.Struct({ seed: Schema.String, count: Schema.Number }),
            initial: (seed: string) => ({ seed, count: 0 }),
          });
          const migrations: Array<number> = [];
          const V3 = Doc.make({
            checkpointWhen: () => false,
            kind: "history.migration",
            version: 3,
            scope: "conversation",
            history: "rewindable",
            fork: "asOf",
            schema: Schema.Struct({ count: Schema.Number, version: Schema.Number }),
            initial: () => ({ count: 0, version: 3 }),
            migrate: (value, fromVersion) => {
              migrations.push(fromVersion);
              return { count: Number(value["count"]), version: 3 };
            },
          });
          const { session, storage } = yield* openTestSession();
          const conversationId = yield* createConversation(session);
          const appendWith = (
            kind: string,
            edit: (tx: Tx["Service"]) => Effect.Effect<void, TxDocError>,
          ) =>
            session.commit(
              Effect.gen(function* () {
                const tx = yield* Tx;
                const id = (yield* tx.appendEntry(conversationId, { kind })).id;
                yield* edit(tx);
                return id;
              }),
            );
          const firstEntry: EntryId = yield* appendWith("first", (tx) =>
            Effect.gen(function* () {
              (yield* tx.doc(V1, conversationId)).count = 1;
              (yield* tx.doc(Family, conversationId, "member", "seed")).count = 1;
            }),
          );
          const secondEntry: EntryId = yield* appendWith("second", (tx) =>
            Effect.gen(function* () {
              (yield* tx.doc(V1, conversationId)).count = 2;
            }),
          );
          yield* session.unloadDocuments;
          const commits = storage.commits.length;
          assert.deepStrictEqual(Option.getOrThrow(yield* session.snapshot(V3, conversationId)), {
            count: 2,
            version: 3,
          });
          assert.deepStrictEqual(migrations, [1]);
          assert.lengthOf(storage.commits, commits);

          const thirdEntry: EntryId = yield* appendWith("third", (tx) =>
            tx.doc(V3, conversationId).pipe(Effect.asVoid),
          );
          assert.isTrue(
            storage.commits
              .at(-1)!
              .some(
                (write) =>
                  write.type === "document.change" &&
                  write.content.kind === "base" &&
                  write.content.version === 3,
              ),
          );

          const asOf = (conversation: typeof conversationId, at: EntryId) =>
            session.snapshotAsOf(V3, conversation, at).pipe(Effect.map(Option.getOrUndefined));
          assert.deepStrictEqual(yield* asOf(conversationId, firstEntry), { count: 1, version: 3 });
          assert.deepStrictEqual(yield* asOf(conversationId, secondEntry), {
            count: 2,
            version: 3,
          });
          assert.deepStrictEqual(yield* asOf(conversationId, thirdEntry), { count: 2, version: 3 });
          assert.deepStrictEqual(migrations, [1, 1, 1]);
          assert.deepStrictEqual(
            Option.getOrUndefined(
              yield* session.snapshotAsOf(Family, conversationId, "member", firstEntry),
            ),
            { seed: "seed", count: 1 },
          );

          const childId = yield* session.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              return (yield* tx.forkConversation(conversationId, secondEntry, {
                ownership: { kind: "ownerless" },
              })).id;
            }),
          );
          assert.deepStrictEqual(yield* asOf(childId, firstEntry), { count: 1, version: 3 });
          assert.deepStrictEqual(yield* asOf(childId, secondEntry), { count: 2, version: 3 });
          assert.deepStrictEqual(
            Option.getOrUndefined(
              yield* session.snapshotAsOf(Family, childId, "member", firstEntry),
            ),
            { seed: "seed", count: 1 },
          );
          assert.include(
            yield* failureMessage(asOf(childId, thirdEntry)),
            `Entry ${thirdEntry} is not visible`,
          );
        }),
      ),
  );

  it.effect(
    "selects the incarnation alive at the entry commit across retirement and recreation",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const ValueDoc = Doc.make({
            checkpointWhen: () => false,
            kind: "history.incarnation",
            version: 1,
            scope: "conversation",
            history: "rewindable",
            fork: "asOf",
            schema: Schema.Struct({ value: Schema.String }),
            initial: () => ({ value: "initial" }),
          });
          const { session } = yield* openTestSession();
          const conversationId = yield* createConversation(session);
          const append = (
            kind: string,
            edit: (tx: Tx["Service"]) => Effect.Effect<void, TxDocError>,
          ) =>
            session.commit(
              Effect.gen(function* () {
                const tx = yield* Tx;
                const id = (yield* tx.appendEntry(conversationId, { kind })).id;
                yield* edit(tx);
                return id;
              }),
            );
          const beforeCreation = yield* append("before", () => Effect.void);
          const createdAt = yield* append("create", (tx) =>
            Effect.gen(function* () {
              (yield* tx.doc(ValueDoc, conversationId)).value = "old";
            }),
          );
          const retiredAt = yield* append("retire", (tx) => tx.retireDoc(ValueDoc, conversationId));
          const recreatedAt = yield* append("recreate", (tx) =>
            Effect.gen(function* () {
              (yield* tx.doc(ValueDoc, conversationId)).value = "new";
            }),
          );

          const asOf = (at: EntryId) =>
            session
              .snapshotAsOf(ValueDoc, conversationId, at)
              .pipe(Effect.map(Option.getOrUndefined));
          assert.isUndefined(yield* asOf(beforeCreation));
          assert.deepStrictEqual(yield* asOf(createdAt), { value: "old" });
          assert.isUndefined(yield* asOf(retiredAt));
          assert.deepStrictEqual(yield* asOf(recreatedAt), { value: "new" });
          yield* session.close;
          assert.include(yield* failureMessage(asOf(recreatedAt)), "closed");
        }),
      ),
  );
});

describe("Session tracker cache across definition versions", () => {
  const V1Doc = Doc.make({
    checkpointWhen: () => false,
    kind: "cache.versioned",
    version: 1,
    scope: "session",
    schema: Schema.Struct({ name: Schema.String }),
    initial: () => ({ name: "first" }),
  });
  const V2Doc = Doc.make({
    checkpointWhen: () => false,
    kind: "cache.versioned",
    version: 2,
    scope: "session",
    schema: Schema.Struct({ names: Schema.Array(Schema.String) }),
    initial: () => ({ names: [] }),
    migrate: (value) => ({ names: [String(value["name"])] }),
  });

  describeEachBackend("tracker cache", (backend) => {
    it.effect("migrates a document cached by an older token without unloading", () =>
      run(backend)(
        Effect.gen(function* () {
          const { session, storage } = yield* openTestSession();
          yield* session.commit(touch(V1Doc));
          assert.deepStrictEqual(Option.getOrThrow(yield* session.snapshot(V1Doc)), {
            name: "first",
          });

          // Reloaded extension code accesses the still-cached document with a newer token.
          assert.deepStrictEqual(Option.getOrThrow(yield* session.snapshot(V2Doc)), {
            names: ["first"],
          });
          yield* session.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              (yield* tx.doc(V2Doc)).names.push("second");
            }),
          );
          const write = documentWrites(storage.commits.at(-1)!)[0]!;
          if (write.type !== "document.change") throw new Error("expected a change");
          assert.deepStrictEqual(write.content, {
            version: 2,
            kind: "base",
            value: { names: ["first", "second"] },
          });
          assert.include(yield* failureMessage(session.snapshot(V1Doc)), "newer version 2");
        }),
      ),
    );

    it.effect(
      "sends observers of an older shape a root replacement after a newer token writes",
      () =>
        run(backend)(
          Effect.gen(function* () {
            const { session } = yield* openTestSession();
            yield* session.commit(touch(V1Doc));
            const watch = yield* acquire(session.watchDoc(V1Doc));
            const delivered = yield* Deferred.make<Delta.Delta>();
            yield* watch.value.frames.pipe(
              Stream.take(1),
              Stream.runForEach((frame) => Deferred.succeed(delivered, frame.ops)),
              Effect.forkChild,
            );
            yield* session.commit(
              Effect.gen(function* () {
                const tx = yield* Tx;
                (yield* tx.doc(V2Doc)).names.push("second");
              }),
            );
            const ops = yield* Deferred.await(delivered);
            // An observer of the older shape now holds the newer one; its static type no longer says so.
            const watchValue: Schema.Json = watch.value.value;
            assert.deepStrictEqual(watchValue, { names: ["first", "second"] });
            assert.deepStrictEqual(ops, [["r", { names: ["first", "second"] }]]);
            yield* watch.release;
          }),
        ),
    );

    it.effect(
      "serves an older token from Storage after a newer token migrated only in memory",
      () =>
        run(backend)(
          Effect.gen(function* () {
            const { session } = yield* openTestSession();
            yield* session.commit(touch(V1Doc));
            yield* session.unloadDocuments;
            assert.deepStrictEqual(Option.getOrThrow(yield* session.snapshot(V2Doc)), {
              names: ["first"],
            });
            assert.deepStrictEqual(Option.getOrThrow(yield* session.snapshot(V1Doc)), {
              name: "first",
            });
            yield* session.commit(
              Effect.gen(function* () {
                const tx = yield* Tx;
                (yield* tx.doc(V1Doc)).name = "renamed";
              }),
            );
            assert.deepStrictEqual(Option.getOrThrow(yield* session.snapshot(V2Doc)), {
              names: ["renamed"],
            });
          }),
        ),
    );
  });
});

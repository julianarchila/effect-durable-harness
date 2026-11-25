// Migrations of `core.live`, on every backend: a compaction's `reason` stored by `core.live` version 1 reads as
// `trigger`, and a run stored by version 1 or 2 without `run.turns` counts its turns from 0.
//
// Each case opens a Session kernel over the backend's storage (`openTestSession`); the version 3 token reloads
// the stored value after `unloadDocuments`, so the migration runs on a value read back from storage.
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Doc from "../src/Doc.ts";
import { LiveDoc } from "../src/Live.ts";
import { Tx } from "../src/Tx.ts";
import { createConversation, describeEachBackend, openTestSession, run } from "./SessionSupport.ts";

/** `core.live` as an earlier version wrote it: any JSON object. */
const olderLive = (version: number) =>
  Doc.make({
    kind: "core.live",
    version,
    scope: "conversation",
    history: "latest",
    fork: "initial",
    schema: Schema.Record(Schema.String, Schema.Json),
    initial: () => ({}),
    checkpointWhen: () => false,
  });

describeEachBackend("core.live migrations", (backend) => {
  it.effect(
    "reads a version 1 value with each compaction's `reason` as `trigger`, and stores it migrated on its next write",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const { session, storage } = yield* openTestSession();
          const LiveV1 = olderLive(1);
          const conversationId = yield* createConversation(session);
          yield* session.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              const live = yield* tx.doc(LiveV1, conversationId);
              live["run"] = { taskId: 9, inputs: [4] };
              live["compactions"] = [
                { taskId: 7, reason: "threshold", blocking: true, attempt: 2 },
              ];
              live["compactionFailure"] = { taskId: 5, reason: "manual", error: "no model" };
            }),
          );
          const stored = Effect.gen(function* () {
            const record = Option.getOrThrow(
              yield* storage.findDocument(
                { kind: "core.live", scope: { kind: "conversation", conversationId } },
                "current",
              ),
            );
            return Option.getOrThrow(yield* storage.document(record.id, "current"));
          });
          // The version 3 token reloads the stored value and migrates it.
          yield* session.unloadDocuments;
          const read = Option.getOrThrow(yield* session.snapshot(LiveDoc, conversationId));
          const actual: unknown = read;
          assert.deepStrictEqual(actual, {
            // A run in flight across the upgrade counts its turns from 0.
            run: { taskId: 9, inputs: [4], turns: 0 },
            compactions: [{ taskId: 7, trigger: "threshold", blocking: true, attempt: 2 }],
            compactionFailure: { taskId: 5, trigger: "manual", error: "no model" },
          });
          assert.deepStrictEqual(Object.keys(read.compactions![0]!), [
            "taskId",
            "trigger",
            "blocking",
            "attempt",
          ]);
          // A read migrates in memory only: storage keeps the version 1 value.
          const before = yield* stored;
          assert.strictEqual(before.version, 1);
          assert.deepStrictEqual(before.value["compactions"], [
            { taskId: 7, reason: "threshold", blocking: true, attempt: 2 },
          ]);
          yield* session.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              (yield* tx.doc(LiveDoc, conversationId)).compactions![0]!.attempt = 3;
            }),
          );
          const after = yield* stored;
          assert.strictEqual(after.version, 3);
          assert.deepStrictEqual(after.value, {
            run: { taskId: 9, inputs: [4], turns: 0 },
            compactions: [{ taskId: 7, trigger: "threshold", blocking: true, attempt: 3 }],
            compactionFailure: { taskId: 5, trigger: "manual", error: "no model" },
          });
        }),
      ),
  );

  // An unreleased build stored version 2 without `run.turns`; a run in flight then counted NaN turns.
  it.effect("gives a version 2 run without turns a count of 0", () =>
    run(backend)(
      Effect.gen(function* () {
        const { session } = yield* openTestSession();
        const LiveV2 = olderLive(2);
        const conversationId = yield* createConversation(session);
        yield* session.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            const live = yield* tx.doc(LiveV2, conversationId);
            live["run"] = { taskId: 9, inputs: [4] };
            live["compactions"] = [{ taskId: 7, trigger: "threshold", blocking: true, attempt: 2 }];
          }),
        );
        yield* session.unloadDocuments;
        const read: unknown = Option.getOrUndefined(
          yield* session.snapshot(LiveDoc, conversationId),
        );
        assert.deepStrictEqual(read, {
          run: { taskId: 9, inputs: [4], turns: 0 },
          compactions: [{ taskId: 7, trigger: "threshold", blocking: true, attempt: 2 }],
        });
        // A version 2 run that has a count keeps it.
        yield* session.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            (yield* tx.doc(LiveV2, conversationId))["run"] = { taskId: 9, inputs: [4], turns: 3 };
          }),
        );
        yield* session.unloadDocuments;
        assert.strictEqual(
          Option.getOrUndefined(yield* session.snapshot(LiveDoc, conversationId))?.run?.turns,
          3,
        );
      }),
    ),
  );
});

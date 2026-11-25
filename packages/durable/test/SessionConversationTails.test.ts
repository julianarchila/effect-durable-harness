// Conversation tails: the Session's reads of a conversation's latest entries.
import { assert, describe, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import type { HeadMarker, StorageWrite } from "../src/AgentStorage.ts";
import { ConversationId, EntryId } from "../src/Ids.ts";
import * as ConversationTails from "../src/internal/conversationTails.ts";
import type { EntryRecord } from "../src/Records.ts";

const conversation = (id: number) => ConversationId.make(id);
const entryId = (id: number) => EntryId.make(id);

const entry = (id: number, conversationId: number, head?: number): StorageWrite => {
  const value: EntryRecord = {
    id: entryId(id),
    conversationId: conversation(conversationId),
    kind: "note",
  };
  return { type: "entry", value: head === undefined ? value : { ...value, head: entryId(head) } };
};

// A read that must not run dies, which fails the test.
const never = Effect.die(new Error("read"));

describe("conversation tails", () => {
  it.effect("reads once, then follows the commits", () =>
    Effect.gen(function* () {
      const tails = ConversationTails.make();
      let reads = 0;
      const read = Effect.sync(() => {
        reads++;
        return entryId(5);
      });
      assert.strictEqual(yield* tails.latest(conversation(1), read), 5);
      assert.strictEqual(yield* tails.latest(conversation(1), read), 5);
      assert.strictEqual(reads, 1);
      tails.apply([entry(9, 1)]);
      assert.strictEqual(yield* tails.latest(conversation(1), never), 9);
    }),
  );

  it.effect("knows a new root conversation is empty, and a fork only once read or written", () =>
    Effect.gen(function* () {
      const tails = ConversationTails.make();
      tails.apply([
        { type: "conversation", value: { id: conversation(2) } },
        {
          type: "conversation",
          value: {
            id: conversation(3),
            parent: { conversationId: conversation(2), at: entryId(4) },
          },
        },
      ]);
      assert.isUndefined(yield* tails.latest(conversation(2), never));
      assert.isUndefined(yield* tails.marker(conversation(2), undefined, never));
      assert.strictEqual(yield* tails.latest(conversation(3), Effect.succeed(entryId(4))), 4);
    }),
  );

  it.effect("answers a cutoff from the newest marker only at or after it", () =>
    Effect.gen(function* () {
      const tails = ConversationTails.make();
      tails.apply([
        { type: "conversation", value: { id: conversation(1) } },
        entry(10, 1),
        entry(11, 1, 10),
      ]);
      const marker = yield* tails.marker(conversation(1), undefined, never);
      assert.strictEqual(marker?.id, 11);
      assert.strictEqual((yield* tails.marker(conversation(1), entryId(12), never))?.id, 11);
      // An older cutoff may have an older marker: read, and not kept.
      let reads = 0;
      const older = Effect.sync((): HeadMarker | undefined => {
        reads++;
        return undefined;
      });
      assert.isUndefined(yield* tails.marker(conversation(1), entryId(10), older));
      assert.isUndefined(yield* tails.marker(conversation(1), entryId(10), older));
      assert.strictEqual(reads, 2);
      assert.strictEqual((yield* tails.marker(conversation(1), undefined, never))?.id, 11);
    }),
  );

  it("keeps a bounded number of conversations, fewer with markers", () => {
    const tails = ConversationTails.make();
    for (let id = 1; id <= 5000; id++)
      tails.apply([entry(2 * id, id), entry(2 * id + 1, id, 2 * id)]);
    assert.strictEqual(tails.size(), 4096);
    assert.strictEqual(tails.markers(), 256);
  });

  it("freezes the markers it keeps", () => {
    const tails = ConversationTails.make();
    const write = entry(2, 1, 1);
    tails.apply([write]);
    assert.isTrue(Object.isFrozen(write.type === "entry" ? write.value : undefined));
  });
});

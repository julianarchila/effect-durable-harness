// A kept context range is advanced at each publication only while it is current, on every backend. The controlled
// storage's `scanEntries` hook holds a range scan.
import { assert, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import type { EntryDraft } from "../src/Entry.ts";
import type { ConversationId } from "../src/Ids.ts";
import { advanceRange, deriveContext, rangeQuery } from "../src/internal/context.ts";
import { captureContextBounds, readContextFrom } from "../src/internal/contextRead.ts";
import { scanAll } from "../src/internal/scan.ts";
import type { SessionKernel } from "../src/internal/session.ts";
import { Tx } from "../src/Tx.ts";
import { controlledStorage, describeEachBackend, openSession, run } from "./SessionSupport.ts";

const append = (session: SessionKernel, conversationId: ConversationId, draft: EntryDraft) =>
  session.commit(
    Effect.gen(function* () {
      const tx = yield* Tx;
      return yield* tx.appendEntry(conversationId, draft);
    }),
  );

/** A whole read, with no earlier range. */
const readContext = (session: SessionKernel, conversationId: ConversationId) =>
  Effect.gen(function* () {
    const bounds = yield* session.readOnLine(
      captureContextBounds(session, conversationId, undefined),
    );
    if (bounds === undefined) return deriveContext(undefined, []);
    const entries = yield* scanAll((cursor) =>
      session.storage.scanEntries(
        { conversationId, ...rangeQuery(bounds), order: "ascending" },
        256,
        cursor,
      ),
    );
    return deriveContext(bounds, entries);
  });

describeEachBackend("kept context ranges", (backend) => {
  it.effect("is not advanced when an entry was committed while its scan ran off the line", () =>
    run(backend)(
      Effect.gen(function* () {
        const entered = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        let pause = false;
        const storage = yield* controlledStorage;
        storage.intercept.scanEntries = (_args, next) =>
          Effect.tap(next, () =>
            Effect.suspend(() => {
              if (!pause) return Effect.void;
              pause = false;
              return Deferred.succeed(entered, undefined).pipe(
                Effect.andThen(Deferred.await(release)),
              );
            }),
          );
        const session = yield* openSession(storage);
        const conversation = yield* session.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            return yield* tx.createConversation({ ownership: { kind: "ownerless" } });
          }),
        );
        yield* append(session, conversation.id, { kind: "first" });
        pause = true;
        const read = yield* Effect.forkChild(
          readContextFrom(session, conversation.id, undefined, undefined),
        );
        yield* Deferred.await(entered);
        yield* append(session, conversation.id, { kind: "missed" });
        yield* Deferred.succeed(release, undefined);
        const range = (yield* Fiber.join(read)).range;
        if (range === undefined) return assert.fail("Expected a range");
        assert.isFalse(range.current);
        const last = yield* append(session, conversation.id, { kind: "last" });
        // Not current: left as it is, so the next read extends it from storage instead of skipping "missed".
        assert.strictEqual(advanceRange(range, [last]), range);
        const again = yield* readContextFrom(session, conversation.id, undefined, range);
        const fresh = yield* readContext(session, conversation.id);
        assert.deepStrictEqual(
          again.view.entries.map((entry) => entry.kind),
          ["first", "missed", "last"],
        );
        assert.deepStrictEqual(again.view, fresh);
        assert.isTrue(again.range?.current);
        yield* session.close;
      }),
    ),
  );
});

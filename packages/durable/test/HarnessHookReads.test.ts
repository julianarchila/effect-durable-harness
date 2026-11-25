// A hook or a task reads the conversation's raw history, by entry, or in pages bounded by entry IDs, on every
// backend.
//
// Each hook is an extension of `openChat`; `api.entries(query, limit, cursor)` returns `AgentStorage`'s `Page`,
// whose `next` is an `Option`; a read with a token of another kind uses `AssistantEntry`; `awaitTask` is awaited
// while the `TestClock` advances.
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import { AssistantEntry, UserEntry } from "../src/Entry.ts";
import * as Extension from "../src/Extension.ts";
import * as Hook from "../src/Hook.ts";
import type { EntryId } from "../src/Ids.ts";
import type { Submission } from "../src/Submission.ts";
import * as Task from "../src/Task.ts";
import { fauxAssistantMessage } from "../src/testing/FauxModel.ts";
import { Tx } from "../src/Tx.ts";
import { allEntries, chatSetup, openChat, waitFor } from "./ChatSupport.ts";
import { completed } from "./HarnessSupport.ts";
import { controlledStorage, describeEachBackend, run } from "./SessionSupport.ts";

/** Runs `effect` to its end while the `TestClock` advances. */
const finished = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  Effect.gen(function* () {
    const fiber = yield* Effect.forkChild(effect, { startImmediately: true });
    yield* waitFor(Effect.sync(() => fiber.pollUnsafe() !== undefined));
    return yield* Fiber.join(fiber);
  });

/** Waits for `submission` to finish. */
const settle = (submission: Submission) => finished(submission.await);

describeEachBackend("entry reads for hooks and tasks", (backend) => {
  // #11141
  it.effect("reads raw history and single entries from a hook", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        const seen: Array<{ kinds: Array<string>; user?: string }> = [];
        const other: Array<boolean> = [];
        const reader = Extension.make({
          name: "reader",
          hooks: [
            Hook.generation({
              onYield: (_answer, api) =>
                Effect.gen(function* () {
                  const page = yield* api.entries({ order: "ascending" }, 10);
                  const first = page.items[0]!;
                  const user = yield* api.entry(UserEntry, first.id);
                  seen.push(
                    Option.isSome(user)
                      ? { kinds: page.items.map((entry) => entry.kind), user: user.value.kind }
                      : { kinds: page.items.map((entry) => entry.kind) },
                  );
                  // Another kind: none.
                  other.push(Option.isNone(yield* api.entry(AssistantEntry, first.id)));
                  return undefined;
                }).pipe(Effect.orDie),
            }),
          ],
        });
        setup.faux.setResponses([fauxAssistantMessage("done")]);
        const { harness, root } = yield* openChat(yield* controlledStorage, setup, {
          extensions: [reader],
        });
        yield* settle(yield* root.submit({ type: "input", content: "go" }));
        // The answer is not committed yet when `onYield` asks.
        assert.deepStrictEqual(seen, [{ kinds: ["core.user"], user: "core.user" }]);
        assert.deepStrictEqual(other, [true]);
        yield* harness.close;
      }),
    ),
  );

  // #11141
  it.effect("pages a task's conversation history by entry ID range", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        const pages: Array<ReadonlyArray<EntryId>> = [];
        const Reader = Task.make({
          name: "test.reader",
          version: 1,
          input: Schema.Null,
          checkpoint: Schema.Struct({ phase: Schema.Literal("run") }),
          result: Schema.Null,
          initial: () => ({ phase: "run" }),
          phases: {
            run: (_task, runtime) =>
              Effect.gen(function* () {
                const all = yield* runtime.entries({ order: "ascending" }, 1);
                pages.push(all.items.map((entry) => entry.id));
                const rest = yield* runtime.entries(
                  { order: "ascending" },
                  1,
                  Option.getOrUndefined(all.next),
                );
                pages.push(rest.items.map((entry) => entry.id));
                const bounded = yield* runtime.entries(
                  { minEntryId: rest.items[0]!.id, maxEntryId: rest.items[0]!.id },
                  10,
                );
                pages.push(bounded.items.map((entry) => entry.id));
                yield* runtime.commit(() => Effect.succeed(completed(null)));
              }),
          },
          abort: () => Effect.void,
        });
        setup.faux.setResponses([fauxAssistantMessage("done")]);
        const { harness, root } = yield* openChat(yield* controlledStorage, setup, {
          extensions: [Extension.make({ name: "reader", tasks: [Reader] })],
        });
        yield* settle(yield* root.submit({ type: "input", content: "go" }));
        const id = yield* root.commit(
          Effect.flatMap(Tx, (tx) =>
            tx.createTask(Reader, null, { ownership: { kind: "conversation" } }),
          ),
        );
        yield* finished(harness.harness.awaitTask(id));
        const entries = yield* allEntries(root);
        assert.deepStrictEqual(
          entries.map((entry) => entry.kind),
          ["core.user", "core.assistant"],
        );
        assert.deepStrictEqual(pages, [[entries[0]!.id], [entries[1]!.id], [entries[1]!.id]]);
        yield* harness.close;
      }),
    ),
  );
});

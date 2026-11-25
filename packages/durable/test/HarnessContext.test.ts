// The context a task reads of its conversation, on every backend.
//
// - Scanned rows are counted, and a range scan held, by `intercept.scanEntries` over the backend under test; the
//   tasks a case runs are static extensions of its open.
// - Time and timers are the `TestClock`; concurrent reads are `Effect.all` with unbounded concurrency.
// - An `assert` inside a handler fails the task when it fails, which the case's outcome check catches.
// - Returned values are not frozen, so "keeps returned views and kept ranges apart" and "freezes kept entries deeply
//   even when storage freezes them shallowly" are isolation checks: changing a returned view's arrays, a nested
//   field of a kept message, or an entry's message in place leaves the next read alone.
// - "does not keep a read that finishes after its task ended": the read left running after the handler returns
//   is a detached fiber started immediately.
// - "drops an idle conversation's context read on a timer when nothing else runs": the `TestClock` runs the expiry
//   sleep, and read counts show the range kept a millisecond before it and dropped once it fired.
// - The baseline cases, and the random test's `core.system` baseline entries with its "baseline system" and
//   "baseline past assistant" categories (the latter compares messages by value, as `context()` returns copies).
//   "does not lead with a first system entry…" takes its second conversation from the same Harness, since the
//   run's storage is shared. The scanned-row counts follow from the Session knowing the bounds (conversation
//   tails) and the kept range taking each committed entry.
import { isDeepStrictEqual } from "node:util";
import { assert, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Schema from "effect/Schema";
import * as TestClock from "effect/testing/TestClock";
import type { ContextView, EntryNotVisible } from "../src/ContextView.ts";
import type { EntryDraft } from "../src/Entry.ts";
import type { Conversation, HarnessError } from "../src/Harness.ts";
import type { ConversationId, EntryId } from "../src/Ids.ts";
import type {
  AssistantMessage,
  Message,
  StopReason,
  SystemMessage,
  ToolResultMessage,
  UserMessage,
} from "../src/Message.ts";
import type { EntryRecord } from "../src/Records.ts";
import type { HarnessSettings } from "../src/Settings.ts";
import * as Task from "../src/Task.ts";
import type { CommitError } from "../src/Session.ts";
import type { TaskError } from "../src/TaskError.ts";
import { IdleTimers } from "../src/IdleTimers.ts";
import { Tx, type TxDocError } from "../src/Tx.ts";
import { awaitBounded, bounded, completed, openHarness } from "./HarnessSupport.ts";
import { controlledStorage, describeEachBackend, failureMessage, run } from "./SessionSupport.ts";
import { serialTimeout } from "./Timeouts.ts";

// ─── message builders ────────────────────────────────────────────────────────

const user = (text: string): UserMessage => ({ role: "user", content: text, timestamp: 1 });

interface AssistantOptions {
  readonly calls?: ReadonlyArray<string> | undefined;
  readonly stopReason?: StopReason | undefined;
}

const assistant = (text: string, options: AssistantOptions = {}): AssistantMessage => {
  const calls = (options.calls ?? []).map((id) => ({
    type: "toolCall" as const,
    id,
    name: `tool-${id}`,
    arguments: {},
  }));
  return {
    role: "assistant",
    content: [{ type: "text", text }, ...calls],
    api: "faux",
    provider: "faux",
    model: "faux",
    usage: {
      input: 0,
      output: 0,
      cacheRead: 0,
      cacheWrite: 0,
      totalTokens: 0,
      cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
    },
    stopReason: options.stopReason ?? (calls.length > 0 ? "toolUse" : "stop"),
    timestamp: 2,
  };
};

const toolResult = (id: string, text = `result ${id}`): ToolResultMessage => ({
  role: "toolResult",
  toolCallId: id,
  toolName: `tool-${id}`,
  content: [{ type: "text", text }],
  isError: false,
  timestamp: 3,
});

const system = (sections: Record<string, string | null>): SystemMessage => ({
  role: "system",
  content: "",
  sections,
  timestamp: 4,
});

/** Compact message rendering for assertions. */
const describeMessage = (message: Message): string => {
  switch (message.role) {
    case "user":
      return `user:${String(message.content)}`;
    case "assistant": {
      const text = message.content.find((content) => content.type === "text");
      return `assistant:${text?.type === "text" ? text.text : ""}`;
    }
    case "toolResult": {
      const text = message.content.find((content) => content.type === "text");
      return `result:${message.toolCallId}:${message.isError ? "error" : text?.type === "text" ? text.text : ""}`;
    }
    case "system":
      return `system:${Object.keys(message.sections ?? {}).join(",")}`;
  }
};

// ─── setups ─────────────────────────────────────────────────────────────────

const ownerless = { ownership: { kind: "ownerless" } } as const;

const setup = Effect.gen(function* () {
  const storage = yield* controlledStorage;
  const h = yield* openHarness(storage, []);
  const root = yield* h.harness.ensureRoot();
  const append = (draft: EntryDraft) =>
    root.commit(
      Effect.gen(function* () {
        const tx = yield* Tx;
        return yield* tx.appendEntry(root.id, draft);
      }),
    );
  const message = (model: Message, kind = "message") => append({ kind, model: [model] });
  return { h, root, append, message };
});

/** Scanned entry rows so far. */
interface Scanned {
  rows: number;
}

/** When set, the next range scan waits for it; bounds probes (one row, on the Session line) do not. */
interface Hold {
  scan?: Deferred.Deferred<void> | undefined;
}

/** What a counting setup's storage records and holds; tasks built before the Harness opens close over it. */
interface Counters {
  readonly scanned: Scanned;
  readonly hold: Hold;
}

const counters = (): Counters => ({ scanned: { rows: 0 }, hold: {} });

interface CountingOptions {
  readonly settings?: HarnessSettings | undefined;
  readonly counters?: Counters | undefined;
}

/** A Harness that counts scanned entry rows, with a 21-entry root transcript and `tasks` registered. */
const countingSetup = (tasks: ReadonlyArray<Task.Any>, options: CountingOptions = {}) =>
  Effect.gen(function* () {
    const { scanned, hold } = options.counters ?? counters();
    const storage = yield* controlledStorage;
    storage.intercept.scanEntries = (args, next) =>
      Effect.gen(function* () {
        const held = args[1] > 1 ? hold.scan : undefined;
        if (held !== undefined) {
          hold.scan = undefined;
          yield* Deferred.await(held);
        }
        const page = yield* next;
        scanned.rows += page.items.length;
        return page;
      });
    const settings = options.settings;
    const h = yield* openHarness(storage, tasks, {
      settings: settings === undefined ? undefined : () => settings,
    });
    const root = yield* h.harness.ensureRoot();
    const first = yield* root.commit(
      Effect.gen(function* () {
        const tx = yield* Tx;
        return yield* tx.appendEntry(root.id, { kind: "message", model: [user("first")] });
      }),
    );
    for (let index = 0; index < 20; index++) {
      yield* root.commit(
        Effect.gen(function* () {
          const tx = yield* Tx;
          yield* tx.appendEntry(root.id, { kind: "message", model: [assistant(`old ${index}`)] });
        }),
      );
    }
    return { h, root, first, scanned, hold };
  });

const ids = (entries: ReadonlyArray<EntryRecord>): Array<EntryId> =>
  entries.map((entry) => entry.id);

const describeAll = (view: ContextView) => view.messages.map(describeMessage);

const DONE = completed(null);

const ABORTED = { status: "terminal", outcome: { status: "aborted" } } as const;

/** What a handler body may fail with: its runtime's errors and those of the test's own conversation reads. */
type HandlerError =
  | TaskError
  | CommitError
  | TxDocError
  | Task.RuntimeReadError
  | EntryNotVisible
  | HarnessError;

const Run = Schema.Struct({ phase: Schema.Literal("run") });

/** A one-phase task with no input that runs `body` and completes with null. */
const oneStep = (
  name: string,
  body: (
    runtime: Task.TaskRuntime<{}, { readonly phase: "run" }, null>,
  ) => Effect.Effect<void, HandlerError>,
) =>
  Task.make({
    name,
    version: 1,
    input: Schema.Struct({}),
    checkpoint: Run,
    result: Schema.Null,
    initial: () => ({ phase: "run" }),
    phases: {
      run: (_task, runtime) =>
        body(runtime).pipe(Effect.andThen(runtime.commit(() => Effect.succeed(DONE)))),
    },
    abort: (_task, runtime) => runtime.commit(() => Effect.succeed(ABORTED)),
  });

type ProbeRead = (
  name: string,
  load: Effect.Effect<ContextView, HandlerError>,
) => Effect.Effect<void, HandlerError>;

/** Reads its conversation's context once, named by its input. */
const probeTask = (read: ProbeRead) =>
  Task.make({
    name: "test.context-probe",
    version: 1,
    input: Schema.Struct({ name: Schema.String }),
    checkpoint: Run,
    result: Schema.Null,
    initial: () => ({ phase: "run" }),
    phases: {
      run: (task, runtime) =>
        read(task.input.name, runtime.context(runtime.conversationId)).pipe(
          Effect.andThen(runtime.commit(() => Effect.succeed(DONE))),
        ),
    },
    abort: (_task, runtime) => runtime.commit(() => Effect.succeed(ABORTED)),
  });

const onRoot = (root: Conversation) =>
  ({ ownership: { kind: "conversation" }, conversationId: root.id }) as const;

describeEachBackend("conversation context", (backend) => {
  it.effect(
    "returns the whole transcript without a head and excludes model-less entries from messages",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const { root, append, message } = yield* setup;
          const first = yield* message(user("hi"));
          const note = yield* append({ kind: "note", data: { text: "display only" } });
          const answer = yield* message(assistant("hello"));
          const view = yield* root.context();
          assert.isUndefined(view.head);
          assert.deepStrictEqual(ids(view.entries), [first.id, note.id, answer.id]);
          assert.deepStrictEqual(describeAll(view), ["user:hi", "assistant:hello"]);
        }),
      ),
  );

  it.effect(
    "excludes aborted, error, and deferred assistant messages but keeps their raw entries",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const { root, message } = yield* setup;
          yield* message(user("q"));
          const aborted = yield* message(assistant("partial", { stopReason: "aborted" }));
          yield* message(assistant("failed", { stopReason: "error" }));
          yield* message(assistant("later", { stopReason: "deferred" }));
          yield* message(assistant("done", { stopReason: "length" }));
          const view = yield* root.context();
          assert.lengthOf(view.entries, 5);
          assert.strictEqual(view.entries[1]!.id, aborted.id);
          assert.deepStrictEqual(describeAll(view), ["user:q", "assistant:done"]);
        }),
      ),
  );

  it.effect("resolves self heads and uses the newest head marker", () =>
    run(backend)(
      Effect.gen(function* () {
        const { root, append, message } = yield* setup;
        yield* message(user("old"));
        const reset = yield* append({ kind: "reset", head: "self", model: [user("fresh start")] });
        assert.strictEqual(reset.head, reset.id);
        const after = yield* message(assistant("after reset"));
        let view = yield* root.context();
        assert.strictEqual(view.head?.id, reset.id);
        assert.deepStrictEqual(ids(view.entries), [reset.id, after.id]);
        assert.deepStrictEqual(describeAll(view), ["user:fresh start", "assistant:after reset"]);

        // A compaction summary heads an earlier kept entry; older head markers in range drop out.
        const summary = yield* append({
          kind: "summary",
          head: after.id,
          model: [user("summary")],
        });
        const tail = yield* message(user("next"));
        view = yield* root.context();
        assert.strictEqual(view.head?.id, summary.id);
        assert.deepStrictEqual(ids(view.entries), [summary.id, after.id, tail.id]);
        assert.deepStrictEqual(describeAll(view), [
          "user:summary",
          "assistant:after reset",
          "user:next",
        ]);
      }),
    ),
  );

  it.effect("applies the newest edit per target within the active range", () =>
    run(backend)(
      Effect.gen(function* () {
        const { root, append, message } = yield* setup;
        const first = yield* message(user("first"));
        const second = yield* message(user("second"));
        yield* append({
          kind: "edit",
          edits: [{ target: first.id, action: "replace", messages: [user("first v2")] }],
        });
        yield* append({
          kind: "edit",
          edits: [{ target: first.id, action: "replace", messages: [user("first v3")] }],
        });
        yield* append({ kind: "edit", edits: [{ target: second.id, action: "omit" }] });
        let view = yield* root.context();
        assert.lengthOf(view.entries, 5);
        assert.deepStrictEqual(describeAll(view), ["user:first v3"]);

        // Edits before the active range no longer apply.
        const reset = yield* append({ kind: "reset", head: second.id });
        view = yield* root.context();
        assert.strictEqual(view.head?.id, reset.id);
        assert.deepStrictEqual(describeAll(view), []);
        yield* append({
          kind: "edit",
          edits: [{ target: second.id, action: "replace", messages: [user("second v2")] }],
        });
        view = yield* root.context();
        assert.deepStrictEqual(describeAll(view), ["user:second v2"]);
      }),
    ),
  );

  it.effect("keeps positional system messages and orders tool results by call order", () =>
    run(backend)(
      Effect.gen(function* () {
        const { root, message, append } = yield* setup;
        yield* message(system({ preamble: "You help." }), "core.system");
        yield* message(user("run tools"));
        yield* message(assistant("calling", { calls: ["b", "a"] }));
        yield* message(toolResult("a"));
        yield* append({ kind: "core.system", model: [system({ cwd: "/repo" })] });
        yield* message(toolResult("b"));
        yield* message(toolResult("zz"));
        yield* message(assistant("done"));
        const view = yield* root.context();
        assert.deepStrictEqual(describeAll(view), [
          "system:preamble",
          "user:run tools",
          "assistant:calling",
          "result:b:result b",
          "result:a:result a",
          "system:cwd",
          "assistant:done",
        ]);
      }),
    ),
  );

  it.effect(
    "leads with a system message that only user messages precede, and keeps later ones in place",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const { root, message, append } = yield* setup;
          yield* message(user("first"));
          yield* message(user("steered"));
          yield* message(system({ preamble: "You help." }), "core.system");
          yield* message(assistant("answer"));
          yield* message(user("next"));
          yield* message(system({ cwd: "/repo" }), "core.system");
          let view = yield* root.context();
          assert.deepStrictEqual(describeAll(view), [
            "system:preamble",
            "user:first",
            "user:steered",
            "assistant:answer",
            "user:next",
            "system:cwd",
          ]);
          // Stored order and contributions stay as committed.
          assert.deepStrictEqual(view.contributions.flat().map(describeMessage).slice(0, 3), [
            "user:first",
            "user:steered",
            "system:preamble",
          ]);

          // After a reset, the baseline written after the handoff leads too.
          yield* append({ kind: "reset", head: "self", model: [user("handoff")] });
          yield* message(system({ preamble: "Baseline." }), "core.system");
          view = yield* root.context();
          assert.deepStrictEqual(describeAll(view), ["system:preamble", "user:handoff"]);
        }),
      ),
  );

  it.effect("leads with the baseline after a compaction that keeps assistant messages", () =>
    run(backend)(
      Effect.gen(function* () {
        const { root, message, append } = yield* setup;
        yield* message(user("old"));
        const kept = yield* message(user("kept"));
        yield* message(assistant("calling", { calls: ["a"] }));
        yield* message(toolResult("a"));
        yield* message(assistant("answer"));
        yield* append({ kind: "summary", head: kept.id, model: [user("summary")] });
        yield* message(user("next"));
        yield* message(system({ preamble: "Baseline." }), "core.system");
        let view = yield* root.context();
        assert.deepStrictEqual(describeAll(view), [
          "system:preamble",
          "user:summary",
          "user:kept",
          "assistant:calling",
          "result:a:result a",
          "assistant:answer",
          "user:next",
        ]);

        // Later system messages stay in place.
        yield* message(assistant("done"));
        yield* message(system({ cwd: "/repo" }), "core.system");
        view = yield* root.context();
        assert.deepStrictEqual(describeAll(view).slice(0, 2), ["system:preamble", "user:summary"]);
        assert.deepStrictEqual(describeAll(view).slice(-2), ["assistant:done", "system:cwd"]);
      }),
    ),
  );

  it.effect(
    "leads with a baseline written after the first answers, when the agent had no section or tool before",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const { root, message } = yield* setup;
          yield* message(user("first"), "core.user");
          yield* message(assistant("answer"), "core.assistant");
          yield* message(user("next"), "core.user");
          yield* message(system({ plan: "Plan." }), "core.system");
          const view = yield* root.context();
          assert.deepStrictEqual(describeAll(view), [
            "system:plan",
            "user:first",
            "assistant:answer",
            "user:next",
          ]);
        }),
      ),
  );

  it.effect(
    "keeps system messages other than the baseline in place once an assistant message precedes them",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const { root, message, append } = yield* setup;
          yield* message(user("first"));
          yield* message(assistant("answer"));
          // Another kind's system message is positional; the baseline after it is not the first system message.
          yield* message(system({ note: "From here on." }), "host.note");
          yield* message(system({ preamble: "Baseline." }), "core.system");
          let view = yield* root.context();
          assert.deepStrictEqual(describeAll(view), [
            "user:first",
            "assistant:answer",
            "system:note",
            "system:preamble",
          ]);

          // After a compaction that keeps the other kind's system message, the new baseline does not move ahead
          // of it.
          const kept = yield* message(user("kept"));
          yield* message(assistant("kept answer"));
          yield* message(system({ mode: "plan" }), "host.note");
          yield* append({ kind: "summary", head: kept.id, model: [user("summary")] });
          yield* message(user("next"));
          yield* message(system({ preamble: "Baseline." }), "core.system");
          view = yield* root.context();
          assert.deepStrictEqual(describeAll(view), [
            "user:summary",
            "user:kept",
            "assistant:kept answer",
            "system:mode",
            "user:next",
            "system:preamble",
          ]);
        }),
      ),
  );

  it.effect(
    "does not lead with a first system entry written after another system message, even once that is omitted",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const { h, root, message, append } = yield* setup;
          const host = yield* message(system({ legacy: "Use the legacy format." }), "host.system");
          yield* message(user("first"));
          yield* message(assistant("answer"));
          // Generation wrote this entry against the host's message: it only removes a section, so it is no
          // baseline.
          yield* message(system({ legacy: null }), "core.system");
          yield* append({ kind: "edit", edits: [{ target: host.id, action: "omit" }] });
          let view = yield* root.context();
          assert.deepStrictEqual(describeAll(view), [
            "user:first",
            "assistant:answer",
            "system:legacy",
          ]);

          // The same holds for a system message an edit put before it.
          // The run's storage is shared, so a second root would be the same conversation. An ownerless conversation of the same Harness starts empty instead.
          const otherRoot = yield* h.harness.createConversation(ownerless);
          const otherAppend = (draft: EntryDraft) =>
            otherRoot.commit(
              Effect.gen(function* () {
                const tx = yield* Tx;
                return yield* tx.appendEntry(otherRoot.id, draft);
              }),
            );
          const other = {
            root: otherRoot,
            append: otherAppend,
            message: (model: Message, kind = "message") => otherAppend({ kind, model: [model] }),
          };
          const first = yield* other.message(user("first"));
          yield* other.append({
            kind: "edit",
            edits: [
              {
                target: first.id,
                action: "replace",
                messages: [user("first"), system({ note: "Note." })],
              },
            ],
          });
          yield* other.message(assistant("answer"));
          yield* other.message(system({ note: null }), "core.system");
          yield* other.append({
            kind: "edit",
            edits: [{ target: first.id, action: "replace", messages: [user("first")] }],
          });
          view = yield* other.root.context();
          assert.deepStrictEqual(describeAll(view), [
            "user:first",
            "assistant:answer",
            "system:note",
          ]);
        }),
      ),
  );

  it.effect(
    "does not lead with a retained, edited, or later system entry in place of the baseline",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const { root, message, append } = yield* setup;
          const kept = yield* message(user("kept"));
          yield* message(assistant("answer"));
          const retained = yield* message(system({ cwd: "/repo" }), "core.system");
          // Before generation writes the baseline after the head marker, a retained system entry stays in place.
          yield* append({ kind: "summary", head: kept.id, model: [user("summary")] });
          yield* message(user("next"));
          let view = yield* root.context();
          assert.deepStrictEqual(describeAll(view), [
            "user:summary",
            "user:kept",
            "assistant:answer",
            "system:cwd",
            "user:next",
          ]);

          // The baseline omits the retained entry, as generation writes it, and leads.
          const baseline = yield* append({
            kind: "core.system",
            model: [system({ preamble: "Baseline." })],
            edits: [{ target: retained.id, action: "omit" }],
          });
          yield* message(assistant("done"));
          yield* message(system({ preamble: null }), "core.system");
          view = yield* root.context();
          assert.deepStrictEqual(describeAll(view), [
            "system:preamble",
            "user:summary",
            "user:kept",
            "assistant:answer",
            "user:next",
            "assistant:done",
            "system:preamble",
          ]);

          // An omitted baseline leaves no message to lead; the delta after it is not a baseline and stays in place.
          yield* append({ kind: "edit", edits: [{ target: baseline.id, action: "omit" }] });
          view = yield* root.context();
          assert.deepStrictEqual(describeAll(view), [
            "user:summary",
            "user:kept",
            "assistant:answer",
            "user:next",
            "assistant:done",
            "system:preamble",
          ]);

          // A replacement of the baseline is not one either.
          yield* append({
            kind: "edit",
            edits: [
              { target: baseline.id, action: "replace", messages: [system({ other: "Other." })] },
            ],
          });
          view = yield* root.context();
          assert.deepStrictEqual(describeAll(view), [
            "user:summary",
            "user:kept",
            "assistant:answer",
            "user:next",
            "system:other",
            "assistant:done",
            "system:preamble",
          ]);
        }),
      ),
  );

  it.effect(
    "synthesizes missing tool results after a fork and drops results cut from their call",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const { root, message } = yield* setup;
          yield* message(user("go"));
          const call = yield* message(assistant("calling", { calls: ["x", "y"] }));
          yield* message(toolResult("x"));
          const second = yield* message(toolResult("y"));
          const child = yield* root.fork(call.id, ownerless);
          const childView = yield* child.context();
          assert.deepStrictEqual(describeAll(childView), [
            "user:go",
            "assistant:calling",
            "result:x:error",
            "result:y:error",
          ]);
          const missing = childView.messages[2]!;
          assert.strictEqual(missing.role, "toolResult");
          assert.strictEqual(
            missing.role === "toolResult" ? missing.toolName : undefined,
            "tool-x",
          );
          assert.deepInclude(missing.role === "toolResult" ? missing.details : undefined, {
            reason: "missing_result",
          });

          // A head between a call and its results leaves stray results that are not sent.
          yield* root.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              yield* tx.appendEntry(root.id, { kind: "reset", head: second.id });
            }),
          );
          const parentView = yield* root.context();
          assert.deepStrictEqual(describeAll(parentView), []);
          assert.deepStrictEqual(
            parentView.entries.map((entry) => entry.kind),
            ["reset", "message"],
          );
        }),
      ),
  );

  it.effect("reads the context as of an earlier entry, as a fork at that entry starts", () =>
    run(backend)(
      Effect.gen(function* () {
        const { root, append, message } = yield* setup;
        const first = yield* message(user("first"));
        const call = yield* message(assistant("calling", { calls: ["x", "y"] }));
        const result = yield* message(toolResult("x"));
        yield* message(toolResult("y"));
        const edit = yield* append({
          kind: "edit",
          edits: [{ target: first.id, action: "replace", messages: [user("first v2")] }],
        });
        const reset = yield* append({ kind: "reset", head: "self", model: [user("fresh start")] });
        const tail = yield* message(assistant("after reset"));

        for (const at of [first, call, result, edit, reset, tail]) {
          const fork = yield* root.fork(at.id, ownerless);
          assert.deepStrictEqual(yield* root.context({ at: at.id }), yield* fork.context());
        }
        // Stepped back before the edit and the reset: neither applies, and a cut call gets synthesized results.
        assert.deepStrictEqual(describeAll(yield* root.context({ at: call.id })), [
          "user:first",
          "assistant:calling",
          "result:x:error",
          "result:y:error",
        ]);
        assert.deepStrictEqual(yield* root.context({ at: tail.id }), yield* root.context());
        assert.deepStrictEqual(yield* root.context({}), yield* root.context());

        const other = yield* root.fork(first.id, ownerless);
        assert.include(yield* failureMessage(other.context({ at: tail.id })), "is not visible");
      }),
    ),
  );

  it.effect("extends a task's context read with only newer entries", () =>
    run(backend)(
      Effect.gen(function* () {
        let root: Conversation | undefined;
        let first: EntryRecord | undefined;
        const counted = counters();
        const Reads = oneStep("test.context-reads", (runtime) =>
          Effect.gen(function* () {
            const conversation = root!;
            const scanned = counted.scanned;
            const write = (draft: EntryDraft) =>
              runtime.commit(() =>
                Effect.gen(function* () {
                  const tx = yield* Tx;
                  yield* tx.appendEntry(conversation.id, draft);
                  return undefined;
                }),
              );
            const read = (at?: EntryId) =>
              Effect.gen(function* () {
                const before = scanned.rows;
                const view = yield* runtime.context(
                  conversation.id,
                  at === undefined ? undefined : { at },
                );
                return { view, rows: scanned.rows - before };
              });
            const initial = yield* read();
            assert.deepStrictEqual(initial.view, yield* conversation.context());

            // Three new entries: the kept range took them as they were committed.
            yield* write({ kind: "message", model: [user("new")] });
            yield* write({ kind: "note", data: { text: "display only" } });
            yield* write({ kind: "message", model: [assistant("answer")] });
            const extended = yield* read();
            assert.strictEqual(extended.rows, 0);
            assert.deepStrictEqual(extended.view, yield* conversation.context());

            // A newer edit of an older entry applies to the extended range.
            yield* write({ kind: "edit", edits: [{ target: first!.id, action: "omit" }] });
            const edited = yield* read();
            assert.strictEqual(edited.rows, 0);
            assert.deepStrictEqual(edited.view, yield* conversation.context());
            assert.notInclude(describeAll(edited.view), "user:first");

            // An earlier cutoff reuses the range.
            const cutoff = yield* read(extended.view.entries.at(-1)!.id);
            assert.strictEqual(cutoff.rows, 0);
            assert.deepStrictEqual(cutoff.view, extended.view);

            // A new head marker changes the range: read it whole.
            yield* write({ kind: "reset", head: "self", model: [user("fresh")] });
            const reset = yield* read();
            assert.deepStrictEqual(reset.view, yield* conversation.context());
            assert.deepStrictEqual(describeAll(reset.view), ["user:fresh"]);
          }),
        );
        const opened = yield* countingSetup([Reads], { counters: counted });
        root = opened.root;
        first = opened.first;
        const id = yield* opened.h.harness.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            return yield* tx.createTask(Reads, {}, onRoot(opened.root));
          }),
        );
        yield* opened.h.harness.resume;
        const settled = yield* bounded(opened.h.harness.awaitTask(id));
        assert.deepStrictEqual(settled.state.outcome, DONE.outcome);
        yield* opened.h.close;
      }),
    ),
  );

  it.effect(
    "keeps a conversation's context read across its tasks and for the retention period once idle",
    () =>
      run(backend)(
        Effect.gen(function* () {
          let root: Conversation | undefined;
          const counted = counters();
          const rows: Record<string, number> = {};
          const read = (name: string, load: Effect.Effect<ContextView, HandlerError>) =>
            Effect.gen(function* () {
              const scanned = counted.scanned;
              const before = scanned.rows;
              const view = yield* load;
              rows[name] = scanned.rows - before;
              assert.deepStrictEqual(view, yield* root!.context());
            });
          const Child = oneStep("test.context-child", (runtime) =>
            Effect.gen(function* () {
              yield* runtime.commit(() =>
                Effect.gen(function* () {
                  const tx = yield* Tx;
                  yield* tx.appendEntry(root!.id, {
                    kind: "message",
                    model: [user("from child")],
                  });
                  return undefined;
                }),
              );
              yield* read("child", runtime.context(root!.id));
            }),
          );
          const Parent = Task.make({
            name: "test.context-parent",
            version: 1,
            input: Schema.Struct({}),
            checkpoint: Schema.Union([
              Schema.Struct({ phase: Schema.Literal("start") }),
              Schema.Struct({ phase: Schema.Literal("after") }),
            ]),
            result: Schema.Null,
            initial: () => ({ phase: "start" }),
            phases: {
              start: (_task, runtime) =>
                Effect.gen(function* () {
                  yield* read("parent", runtime.context(root!.id));
                  yield* runtime.commit(() =>
                    Effect.gen(function* () {
                      const tx = yield* Tx;
                      const child = yield* tx.createTask(
                        Child,
                        {},
                        {
                          ownership: { kind: "task", taskId: runtime.taskId },
                          conversationId: root!.id,
                        },
                      );
                      return {
                        status: "waiting",
                        checkpoint: { phase: "after" },
                        on: [child],
                        policy: "allSettled",
                      } as const;
                    }),
                  );
                }),
              after: (_task, runtime) =>
                Effect.gen(function* () {
                  yield* read("after", runtime.context(root!.id));
                  yield* runtime.commit(() => Effect.succeed(DONE));
                }),
            },
            abort: (_task, runtime) => runtime.commit(() => Effect.succeed(ABORTED)),
          });
          const Probe = probeTask(read);
          const opened = yield* countingSetup([Child, Parent, Probe], { counters: counted });
          root = opened.root;
          const h = opened.h;
          yield* h.harness.resume;
          const options = onRoot(opened.root);
          const parent = yield* h.harness.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              return yield* tx.createTask(Parent, {}, options);
            }),
          );
          assert.deepStrictEqual(
            (yield* bounded(h.harness.awaitTask(parent))).state.outcome,
            DONE.outcome,
          );
          yield* opened.root.awaitIdle;
          const probe = (name: string) =>
            Effect.gen(function* () {
              const id = yield* h.harness.commit(
                Effect.gen(function* () {
                  const tx = yield* Tx;
                  return yield* tx.createTask(Probe, { name }, options);
                }),
              );
              assert.deepStrictEqual(
                (yield* bounded(h.harness.awaitTask(id))).state.outcome,
                DONE.outcome,
              );
              yield* opened.root.awaitIdle;
            });
          yield* probe("idle");
          yield* TestClock.adjust("600000 millis");
          yield* probe("expired");
          // The kept range takes each committed entry, so only the first read and the one after the retention
          // period scan the transcript.
          assert.deepStrictEqual(rows, { parent: 21, child: 0, after: 0, idle: 0, expired: 22 });
          yield* h.close;
        }),
      ),
  );

  it.effect(
    "derives the same view from extended reads as from whole reads",
    () =>
      run(backend)(
        Effect.gen(function* () {
          let root: Conversation | undefined;
          // Deterministic pseudo-random transcript (32-bit LCG, high bits): calls and results in any order, missing
          // and stray results, excluded stop reasons, system entries, notes, edits of earlier entries, and head
          // markers.
          let seed = 7;
          const random = (n: number) => {
            seed = (Math.imul(seed, 1_664_525) + 1_013_904_223) >>> 0;
            return Math.floor((seed / 2 ** 32) * n);
          };
          const seen = new Map<string, number>();
          const count = (category: string) => seen.set(category, (seen.get(category) ?? 0) + 1);
          const Steps = oneStep("test.context-steps", (runtime) =>
            Effect.gen(function* () {
              const conversation = root!;
              const written: Array<EntryRecord> = [];
              const calls: Array<string> = [];
              let next = 0;
              const draft = (): EntryDraft => {
                const pick = random(14);
                if (pick < 3) {
                  count("user");
                  return { kind: "message", model: [user(`u${next++}`)] };
                }
                if (pick < 5) {
                  const callIds = Array.from({ length: random(3) }, () => `c${next++}`);
                  calls.push(...callIds);
                  const stopReason = (["aborted", "error", "deferred"] as const)[random(9)];
                  count(stopReason ?? "assistant");
                  return {
                    kind: "message",
                    model: [
                      assistant(`a${next++}`, {
                        calls: callIds,
                        stopReason,
                      }),
                    ],
                  };
                }
                if (pick < 8) {
                  const matched = calls.length > 0 && random(5) > 0;
                  count(matched ? "result" : "stray result");
                  const id = matched ? calls.splice(random(calls.length), 1)[0]! : `stray${next++}`;
                  return { kind: "message", model: [toolResult(id)] };
                }
                if (pick < 9) {
                  // `core.system` entries can be the baseline that leads the context.
                  const kind = random(2) === 0 ? "core.system" : "message";
                  count(kind === "core.system" ? "baseline system" : "system");
                  return { kind, model: [system({ s: `v${next++}` })] };
                }
                if (pick < 10) {
                  count("note");
                  return { kind: "note", data: { n: next++ } };
                }
                const target = written[random(written.length)];
                if (pick < 12 && target !== undefined) {
                  const omit = random(2) === 0;
                  count(omit ? "omit" : "replace");
                  return {
                    kind: "edit",
                    edits: [
                      omit
                        ? { target: target.id, action: "omit" }
                        : { target: target.id, action: "replace", messages: [user(`r${next++}`)] },
                    ],
                  };
                }
                if (pick < 13 && target !== undefined) {
                  count("head");
                  return { kind: "summary", head: target.id, model: [user(`h${next++}`)] };
                }
                count("user");
                return { kind: "message", model: [user(`u${next++}`)] };
              };
              const append = (conversationId: ConversationId) =>
                runtime.commit(() =>
                  Effect.gen(function* () {
                    const tx = yield* Tx;
                    const added = 1 + random(3);
                    for (let index = 0; index < added; index++) {
                      written.push(yield* tx.appendEntry(conversationId, draft()));
                    }
                    return undefined;
                  }),
                );
              for (let step = 0; step < 150; step++) {
                yield* append(conversation.id);
                const whole = yield* conversation.context();
                assert.deepStrictEqual(yield* runtime.context(conversation.id), whole);
                const contributed = whole.contributions.flat();
                const firstSystem = contributed.findIndex((message) => message.role === "system");
                // `context()` returns copies, so compare by value. Each
                // system message has its own section value, so equal values are the same message.
                if (
                  firstSystem > 0 &&
                  isDeepStrictEqual(whole.messages[0], contributed[firstSystem]) &&
                  contributed.slice(0, firstSystem).some((message) => message.role === "assistant")
                ) {
                  count("baseline past assistant");
                }
                if (step % 10 === 9) {
                  // Overlapping reads, one with an earlier cutoff, share the kept range.
                  const at = written[random(written.length)]!.id;
                  const [whole, cut] = yield* Effect.all(
                    [runtime.context(conversation.id), runtime.context(conversation.id, { at })],
                    { concurrency: "unbounded" },
                  );
                  assert.deepStrictEqual(whole, yield* conversation.context());
                  assert.deepStrictEqual(cut, yield* conversation.context({ at }));
                }
              }
              // A fork sees its parent's entries through the fork point and extends with its own.
              const fork = yield* conversation.fork(written[random(written.length)]!.id, ownerless);
              for (let step = 0; step < 20; step++) {
                assert.deepStrictEqual(yield* runtime.context(fork.id), yield* fork.context());
                yield* append(fork.id);
              }
            }),
          );
          const opened = yield* countingSetup([Steps]);
          root = opened.root;
          yield* opened.h.harness.resume;
          const id = yield* opened.h.harness.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              return yield* tx.createTask(Steps, {}, onRoot(opened.root));
            }),
          );
          assert.deepStrictEqual(
            (yield* bounded(opened.h.harness.awaitTask(id))).state.outcome,
            DONE.outcome,
          );
          const categories = [
            "user",
            "assistant",
            "aborted",
            "error",
            "deferred",
            "result",
            "stray result",
            "system",
            "baseline system",
            "baseline past assistant",
            "note",
          ];
          for (const category of [...categories, "omit", "replace", "head"]) {
            assert.isAbove(seen.get(category) ?? 0, 0, category);
          }
          yield* opened.h.close;
        }),
      ),
    serialTimeout(1_683),
  );

  it.effect("finds the baseline among the entries an extended read adds", () =>
    run(backend)(
      Effect.gen(function* () {
        let root: Conversation | undefined;
        let first: EntryRecord | undefined;
        const counted = counters();
        const Steps = oneStep("test.context-baseline", (runtime) =>
          Effect.gen(function* () {
            const conversation = root!;
            const append = (conversationId: ConversationId, draft: EntryDraft) =>
              runtime.commit(() =>
                Effect.gen(function* () {
                  const tx = yield* Tx;
                  yield* tx.appendEntry(conversationId, draft);
                  return undefined;
                }),
              );
            const read = (target: Conversation) =>
              Effect.gen(function* () {
                const before = counted.scanned.rows;
                const view = yield* runtime.context(target.id);
                const rows = counted.scanned.rows - before;
                assert.deepStrictEqual(view, yield* target.context());
                return { messages: describeAll(view), rows };
              });
            /** A read the kept range answers: it took the added entries as they were committed. */
            const extendedRead = (target: Conversation) =>
              Effect.gen(function* () {
                const extended = yield* read(target);
                assert.strictEqual(extended.rows, 0);
                return extended.messages;
              });

            // The cached range has answers and no system message; the baseline arrives in a later read.
            assert.strictEqual((yield* read(conversation)).messages[0], "user:first");
            yield* append(conversation.id, {
              kind: "core.system",
              model: [system({ plan: "Plan." })],
            });
            // Nothing: the kept range took the added entry as it was committed.
            assert.deepStrictEqual((yield* extendedRead(conversation)).slice(0, 3), [
              "system:plan",
              "user:first",
              "assistant:old 0",
            ]);

            // A system message the cached range already has keeps a later first `core.system` entry in place.
            const fork = yield* conversation.fork(first!.id, ownerless);
            yield* append(fork.id, { kind: "message", model: [assistant("answer")] });
            yield* append(fork.id, { kind: "host.system", model: [system({ note: "Note." })] });
            assert.deepStrictEqual((yield* read(fork)).messages, [
              "user:first",
              "assistant:answer",
              "system:note",
            ]);
            yield* append(fork.id, { kind: "core.system", model: [system({ note: null })] });
            assert.deepStrictEqual(yield* extendedRead(fork), [
              "user:first",
              "assistant:answer",
              "system:note",
              "system:note",
            ]);

            // An omitted system message still rules out a later baseline; the extended read keeps that.
            const omitted = yield* conversation.fork(first!.id, ownerless);
            yield* append(omitted.id, { kind: "message", model: [assistant("answer")] });
            let host: EntryRecord | undefined;
            yield* runtime.commit(() =>
              Effect.gen(function* () {
                const tx = yield* Tx;
                host = yield* tx.appendEntry(omitted.id, {
                  kind: "host.system",
                  model: [system({ legacy: "Legacy." })],
                });
                return undefined;
              }),
            );
            yield* append(omitted.id, {
              kind: "edit",
              edits: [{ target: host!.id, action: "omit" }],
            });
            assert.deepStrictEqual((yield* read(omitted)).messages, [
              "user:first",
              "assistant:answer",
            ]);
            yield* append(omitted.id, { kind: "core.system", model: [system({ legacy: null })] });
            assert.deepStrictEqual(yield* extendedRead(omitted), [
              "user:first",
              "assistant:answer",
              "system:legacy",
            ]);

            // A first `core.system` entry without a system message leaves no baseline for a later one to take.
            const empty = yield* conversation.fork(first!.id, ownerless);
            yield* append(empty.id, { kind: "core.system" });
            yield* append(empty.id, { kind: "message", model: [assistant("answer")] });
            assert.deepStrictEqual((yield* read(empty)).messages, [
              "user:first",
              "assistant:answer",
            ]);
            yield* append(empty.id, { kind: "core.system", model: [system({ s: "Delta." })] });
            assert.deepStrictEqual(yield* extendedRead(empty), [
              "user:first",
              "assistant:answer",
              "system:s",
            ]);
          }),
        );
        const opened = yield* countingSetup([Steps], { counters: counted });
        root = opened.root;
        first = opened.first;
        yield* opened.h.harness.resume;
        const id = yield* opened.h.harness.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            return yield* tx.createTask(Steps, {}, onRoot(opened.root));
          }),
        );
        assert.deepStrictEqual(
          (yield* bounded(opened.h.harness.awaitTask(id))).state.outcome,
          DONE.outcome,
        );
        yield* opened.h.close;
      }),
    ),
  );

  it.effect("keeps returned views and kept ranges apart", () =>
    run(backend)(
      Effect.gen(function* () {
        let root: Conversation | undefined;
        const Mutate = oneStep("test.context-mutate", (runtime) =>
          Effect.gen(function* () {
            const view = yield* runtime.context(root!.id);
            // SAFETY: the test changes the view's own arrays, which the read must not share with its kept range.
            (view.messages as Array<Message>).length = 0;
            // SAFETY: as above.
            (view.entries as Array<EntryRecord>).pop();
            // Kept entries are not frozen, so this changes a kept message in place and checks the next read does not see it.
            // SAFETY: the test changes a nested field of a returned message, which the kept range must not share.
            (view.contributions[0]![0] as { content: unknown }).content = "changed";
            assert.deepStrictEqual(yield* runtime.context(root!.id), yield* root!.context());
            assert.deepStrictEqual(describeAll(yield* runtime.context(root!.id))[0], "user:first");
          }),
        );
        const opened = yield* countingSetup([Mutate]);
        root = opened.root;
        yield* opened.h.harness.resume;
        const id = yield* opened.h.harness.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            return yield* tx.createTask(Mutate, {}, onRoot(opened.root));
          }),
        );
        assert.deepStrictEqual(
          (yield* bounded(opened.h.harness.awaitTask(id))).state.outcome,
          DONE.outcome,
        );
        yield* opened.h.close;
      }),
    ),
  );

  it.effect("freezes kept entries deeply even when storage freezes them shallowly", () =>
    run(backend)(
      Effect.gen(function* () {
        let root: Conversation | undefined;
        const Mutate = oneStep("test.context-shallow", (runtime) =>
          Effect.gen(function* () {
            const view = yield* runtime.context(root!.id);
            // SAFETY: the test changes a returned message in place; the kept range must not share it.
            (view.messages[0] as { content: unknown }).content = "changed";
            // SAFETY: as above, through the returned entry's own message.
            (view.entries[0]!.model![0] as { content: unknown }).content = "changed";
            assert.deepStrictEqual(describeAll(yield* runtime.context(root!.id)), ["user:hello"]);
          }),
        );
        const storage = yield* controlledStorage;
        const h = yield* openHarness(storage, [Mutate]);
        root = yield* h.harness.ensureRoot();
        yield* root.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            yield* tx.appendEntry(root!.id, { kind: "message", model: [user("hello")] });
          }),
        );
        yield* h.harness.resume;
        const id = yield* h.harness.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            return yield* tx.createTask(Mutate, {}, onRoot(root!));
          }),
        );
        assert.deepStrictEqual(
          (yield* bounded(h.harness.awaitTask(id))).state.outcome,
          DONE.outcome,
        );
        yield* h.close;
      }),
    ),
  );

  it.effect("does not keep a read that finishes after its task ended", () =>
    run(backend)(
      Effect.gen(function* () {
        let root: Conversation | undefined;
        const counted = counters();
        const scans = yield* Deferred.make<void>();
        const probeStart = yield* Deferred.make<void>();
        let late: Fiber.Fiber<unknown, unknown> | undefined;
        const Late = oneStep("test.context-late", (runtime) =>
          Effect.gen(function* () {
            counted.hold.scan = scans;
            // A read left running after the handler returns: a detached fiber started at once.
            late = yield* Effect.forkDetach(
              runtime.context(root!.id).pipe(Effect.catchCause(Effect.succeed)),
              { startImmediately: true },
            );
          }),
        );
        const rows: Record<string, number> = {};
        const Probe = probeTask((name, load) =>
          Effect.gen(function* () {
            yield* Deferred.await(probeStart);
            const before = counted.scanned.rows;
            yield* load;
            rows[name] = counted.scanned.rows - before;
          }),
        );
        const opened = yield* countingSetup([Late, Probe], {
          settings: { contextRetentionMs: 0 },
          counters: counted,
        });
        root = opened.root;
        const h = opened.h;
        yield* h.harness.resume;
        const options = onRoot(opened.root);
        const lateId = yield* h.harness.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            return yield* tx.createTask(Late, {}, options);
          }),
        );
        assert.deepStrictEqual(
          (yield* bounded(h.harness.awaitTask(lateId))).state.outcome,
          DONE.outcome,
        );
        // The conversation is busy again when the late read finishes; its range must not be kept.
        const probeId = yield* h.harness.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            return yield* tx.createTask(Probe, { name: "probe" }, options);
          }),
        );
        yield* Deferred.succeed(scans, undefined);
        yield* awaitBounded(late!);
        yield* Deferred.succeed(probeStart, undefined);
        assert.deepStrictEqual(
          (yield* bounded(h.harness.awaitTask(probeId))).state.outcome,
          DONE.outcome,
        );
        assert.deepStrictEqual(rows, { probe: 21 });
        yield* h.close;
      }),
    ),
  );

  it.effect("drops an idle conversation's context read on a timer when nothing else runs", () =>
    run(backend)(
      Effect.gen(function* () {
        const counted = counters();
        const rows: Record<string, number> = {};
        const Probe = probeTask((name, load) =>
          Effect.gen(function* () {
            const before = counted.scanned.rows;
            yield* load;
            rows[name] = counted.scanned.rows - before;
          }),
        );
        const opened = yield* countingSetup([Probe], {
          settings: { contextRetentionMs: 1_000 },
          counters: counted,
        });
        const h = opened.h;
        yield* h.harness.resume;
        const options = onRoot(opened.root);
        const probe = (name: string) =>
          Effect.gen(function* () {
            const id = yield* h.harness.commit(
              Effect.gen(function* () {
                const tx = yield* Tx;
                return yield* tx.createTask(Probe, { name }, options);
              }),
            );
            assert.deepStrictEqual(
              (yield* bounded(h.harness.awaitTask(id))).state.outcome,
              DONE.outcome,
            );
            yield* opened.root.awaitIdle;
          });
        yield* probe("first");
        // The expiry is a `Clock` sleep (scheduler.ts), so the reads show it: a millisecond before it the range is kept (no row read), and once it fired the next
        // read scans the whole transcript.
        yield* TestClock.adjust("999 millis");
        yield* probe("kept");
        yield* TestClock.adjust("1000 millis");
        yield* probe("second");
        assert.deepStrictEqual(rows, { first: 21, kept: 0, second: 21 });
        yield* h.close;
      }),
    ),
  );

  // Where the host declines the background timer, as on Cloudflare (the host's `IdleTimers`), no expiry timer is
  // kept and only task changes check it.
  it.effect("keeps no expiry timer where idle timers are off, and expires on task changes", () =>
    run(backend)(
      Effect.gen(function* () {
        const counted = counters();
        const rows: Record<string, number> = {};
        const Probe = probeTask((name, load) =>
          Effect.gen(function* () {
            const before = counted.scanned.rows;
            yield* load;
            rows[name] = counted.scanned.rows - before;
          }),
        );
        const opened = yield* countingSetup([Probe], {
          settings: { contextRetentionMs: 1_000 },
          counters: counted,
        });
        const h = opened.h;
        yield* h.harness.resume;
        const options = onRoot(opened.root);
        const probe = (name: string) =>
          Effect.gen(function* () {
            const id = yield* h.harness.commit(
              Effect.gen(function* () {
                const tx = yield* Tx;
                return yield* tx.createTask(Probe, { name }, options);
              }),
            );
            assert.deepStrictEqual(
              (yield* bounded(h.harness.awaitTask(id))).state.outcome,
              DONE.outcome,
            );
            yield* opened.root.awaitIdle;
          });
        yield* probe("first");
        assert.strictEqual(h.scheduler.keptContexts(), 1);
        // No timer: past the retention the idle range is still kept.
        yield* TestClock.adjust("5000 millis");
        assert.strictEqual(h.scheduler.keptContexts(), 1);
        // The next task change drops it, so the probe reads the whole transcript again.
        yield* probe("second");
        assert.deepStrictEqual(rows, { first: 21, second: 21 });
        yield* h.close;
      }).pipe(Effect.provideService(IdleTimers, { enabled: false })),
    ),
  );

  it.effect("drops a conversation's context read once idle with zero retention", () =>
    run(backend)(
      Effect.gen(function* () {
        const counted = counters();
        const rows: Record<string, number> = {};
        const Probe = probeTask((name, load) =>
          Effect.gen(function* () {
            const before = counted.scanned.rows;
            yield* load;
            rows[name] = counted.scanned.rows - before;
          }),
        );
        const opened = yield* countingSetup([Probe], {
          settings: { contextRetentionMs: 0 },
          counters: counted,
        });
        const h = opened.h;
        yield* h.harness.resume;
        const options = onRoot(opened.root);
        for (const name of ["first", "second"]) {
          const id = yield* h.harness.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              return yield* tx.createTask(Probe, { name }, options);
            }),
          );
          assert.deepStrictEqual(
            (yield* bounded(h.harness.awaitTask(id))).state.outcome,
            DONE.outcome,
          );
          yield* opened.root.awaitIdle;
        }
        assert.deepStrictEqual(rows, { first: 21, second: 21 });
        yield* h.close;
      }),
    ),
  );
});

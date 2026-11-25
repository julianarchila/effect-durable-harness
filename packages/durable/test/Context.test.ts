import { assert, describe, it } from "@effect/vitest";
import * as Predicate from "effect/Predicate";
import type * as Types from "effect/Types";
import { ConversationId, EntryId } from "../src/Ids.ts";
import type {
  AssistantMessage,
  Message,
  StopReason,
  SystemMessage,
  ToolCall,
  ToolResultMessage,
  UserMessage,
} from "../src/Message.ts";
import type { ContextEdit, EntryRecord } from "../src/Records.ts";
import * as Context from "../src/internal/context.ts";

// Tests of context derivation: the derivation is pure, so these build entries in memory, without a Session. The
// scenarios are those of `HarnessContext.test.ts`, which runs them end to end through storage and the Harness. `boundsAt` stands in for the storage reads (`captureContextBounds` and the
// range scan) that the Harness does on and off the Session line.

const conversation = ConversationId.make(1);

interface Draft {
  readonly kind?: string | undefined;
  readonly model?: ReadonlyArray<Message> | undefined;
  readonly data?: { readonly text: string } | undefined;
  /** `"self"` resolves to the appended entry's own ID. */
  readonly head?: EntryRecord | "self" | undefined;
  readonly edits?: ReadonlyArray<ContextEdit> | undefined;
}

/** An in-memory transcript: entries in append order, IDs from 1. */
const transcript = () => {
  const entries: Array<EntryRecord> = [];
  const append = (draft: Draft): EntryRecord => {
    const id = EntryId.make(entries.length + 1);
    const head = draft.head === "self" ? id : draft.head?.id;
    const entry: Types.Mutable<EntryRecord> = {
      id,
      conversationId: conversation,
      kind: draft.kind ?? "message",
    };
    if (draft.model !== undefined) entry.model = draft.model;
    if (draft.data !== undefined) entry.data = draft.data;
    if (head !== undefined) entry.head = head;
    if (draft.edits !== undefined) entry.edits = draft.edits;
    entries.push(entry);
    return entry;
  };
  const message = (model: Message, kind = "message"): EntryRecord =>
    append({ kind, model: [model] });
  return { entries, append, message };
};

const hasHead = (entry: EntryRecord): entry is EntryRecord & { readonly head: EntryId } =>
  entry.head !== undefined;

interface Read {
  readonly bounds: Context.ContextBounds;
  readonly range: ReadonlyArray<EntryRecord>;
}

/** The bounds at `at` (default the tail) and the visible entries of their range, oldest first. */
const boundsAt = (entries: ReadonlyArray<EntryRecord>, at?: EntryId): Read => {
  const tail = at ?? entries[entries.length - 1]!.id;
  const visible = entries.filter((entry) => entry.id <= tail);
  const head = visible.findLast(hasHead);
  const bounds: Context.ContextBounds = { head, tail };
  const query = Context.rangeQuery(bounds);
  const range = visible.filter(
    (entry) => query.minEntryId === undefined || entry.id >= query.minEntryId,
  );
  return { bounds, range };
};

const contextOf = (entries: ReadonlyArray<EntryRecord>, at?: EntryId): Context.ContextView => {
  const read = boundsAt(entries, at);
  return Context.deriveContext(read.bounds, read.range);
};

const user = (text: string): UserMessage => ({ role: "user", content: text, timestamp: 1 });

interface AssistantOptions {
  readonly calls?: ReadonlyArray<string> | undefined;
  readonly stopReason?: StopReason | undefined;
}

const assistant = (text: string, options: AssistantOptions = {}): AssistantMessage => {
  const calls: Array<ToolCall> = (options.calls ?? []).map((id) => ({
    type: "toolCall",
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
      return `user:${Predicate.isString(message.content) ? message.content : ""}`;
    case "assistant": {
      const text = message.content.find((content) => content.type === "text");
      return `assistant:${text?.type === "text" ? text.text : ""}`;
    }
    case "toolResult": {
      const text = message.content.find((content) => content.type === "text");
      const shown = message.isError ? "error" : text?.type === "text" ? text.text : "";
      return `result:${message.toolCallId}:${shown}`;
    }
    case "system":
      return `system:${Object.keys(message.sections ?? {}).join(",")}`;
  }
};

const described = (view: Context.ContextView): Array<string> => view.messages.map(describeMessage);

const ids = (entries: ReadonlyArray<EntryRecord>): Array<EntryId> =>
  entries.map((entry) => entry.id);

describe("Context", () => {
  it("returns the whole transcript without a head and excludes model-less entries from messages", () => {
    const t = transcript();
    const first = t.message(user("hi"));
    const note = t.append({ kind: "note", data: { text: "display only" } });
    const answer = t.message(assistant("hello"));
    const view = contextOf(t.entries);
    assert.isUndefined(view.head);
    assert.deepStrictEqual(ids(view.entries), [first.id, note.id, answer.id]);
    assert.deepStrictEqual(view.contributions[1], []);
    assert.deepStrictEqual(described(view), ["user:hi", "assistant:hello"]);
  });

  it("excludes aborted, error, and deferred assistant messages but keeps their raw entries", () => {
    const t = transcript();
    t.message(user("q"));
    const aborted = t.message(assistant("partial", { stopReason: "aborted" }));
    t.message(assistant("failed", { stopReason: "error" }));
    t.message(assistant("later", { stopReason: "deferred" }));
    t.message(assistant("done", { stopReason: "length" }));
    const view = contextOf(t.entries);
    assert.strictEqual(view.entries.length, 5);
    assert.strictEqual(view.entries[1]!.id, aborted.id);
    assert.deepStrictEqual(described(view), ["user:q", "assistant:done"]);
  });

  it("resolves self heads and uses the newest head marker", () => {
    const t = transcript();
    t.message(user("old"));
    const reset = t.append({ kind: "reset", head: "self", model: [user("fresh start")] });
    const after = t.message(assistant("after reset"));
    let view = contextOf(t.entries);
    assert.strictEqual(view.head?.id, reset.id);
    assert.deepStrictEqual(ids(view.entries), [reset.id, after.id]);
    assert.deepStrictEqual(described(view), ["user:fresh start", "assistant:after reset"]);

    // A compaction summary heads an earlier kept entry; older head markers in range drop out.
    const summary = t.append({ kind: "summary", head: after, model: [user("summary")] });
    const tail = t.message(user("next"));
    view = contextOf(t.entries);
    assert.strictEqual(view.head?.id, summary.id);
    assert.deepStrictEqual(ids(view.entries), [summary.id, after.id, tail.id]);
    assert.deepStrictEqual(described(view), ["user:summary", "assistant:after reset", "user:next"]);
  });

  it("applies the newest edit per target within the active range", () => {
    const t = transcript();
    const first = t.message(user("first"));
    const second = t.message(user("second"));
    t.append({
      kind: "edit",
      edits: [{ target: first.id, action: "replace", messages: [user("first v2")] }],
    });
    t.append({
      kind: "edit",
      edits: [{ target: first.id, action: "replace", messages: [user("first v3")] }],
    });
    t.append({ kind: "edit", edits: [{ target: second.id, action: "omit" }] });
    let view = contextOf(t.entries);
    assert.strictEqual(view.entries.length, 5);
    assert.deepStrictEqual(described(view), ["user:first v3"]);

    // Edits before the active range no longer apply.
    const reset = t.append({ kind: "reset", head: second });
    view = contextOf(t.entries);
    assert.strictEqual(view.head?.id, reset.id);
    assert.deepStrictEqual(described(view), []);
    t.append({
      kind: "edit",
      edits: [{ target: second.id, action: "replace", messages: [user("second v2")] }],
    });
    view = contextOf(t.entries);
    assert.deepStrictEqual(described(view), ["user:second v2"]);
  });

  it("keeps positional system messages and orders tool results by call order", () => {
    const t = transcript();
    t.message(system({ preamble: "You help." }), "core.system");
    t.message(user("run tools"));
    t.message(assistant("calling", { calls: ["b", "a"] }));
    t.message(toolResult("a"));
    t.append({ kind: "core.system", model: [system({ cwd: "/repo" })] });
    t.message(toolResult("b"));
    t.message(toolResult("zz"));
    t.message(assistant("done"));
    assert.deepStrictEqual(described(contextOf(t.entries)), [
      "system:preamble",
      "user:run tools",
      "assistant:calling",
      "result:b:result b",
      "result:a:result a",
      "system:cwd",
      "assistant:done",
    ]);
  });

  it("leads with a system message that only user messages precede, and keeps later ones in place", () => {
    const t = transcript();
    t.message(user("first"));
    t.message(user("steered"));
    t.message(system({ preamble: "You help." }), "core.system");
    t.message(assistant("answer"));
    t.message(user("next"));
    t.message(system({ cwd: "/repo" }), "core.system");
    let view = contextOf(t.entries);
    assert.deepStrictEqual(described(view), [
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
    t.append({ kind: "reset", head: "self", model: [user("handoff")] });
    t.message(system({ preamble: "Baseline." }), "core.system");
    view = contextOf(t.entries);
    assert.deepStrictEqual(described(view), ["system:preamble", "user:handoff"]);
  });

  // The fork is a cutoff at the call (a fork at an entry starts with the context as of that entry, as the next
  // case checks).
  it("synthesizes missing tool results after a fork and drops results cut from their call", () => {
    const t = transcript();
    t.message(user("go"));
    const call = t.message(assistant("calling", { calls: ["x", "y"] }));
    t.message(toolResult("x"));
    const second = t.message(toolResult("y"));
    const cut = contextOf(t.entries, call.id);
    assert.deepStrictEqual(described(cut), [
      "user:go",
      "assistant:calling",
      "result:x:error",
      "result:y:error",
    ]);
    assert.deepStrictEqual(cut.messages[2], {
      role: "toolResult",
      toolCallId: "x",
      toolName: "tool-x",
      content: [
        {
          type: "text",
          text: "Tool result unavailable: history ends before this call completed.",
        },
      ],
      isError: true,
      details: { reason: "missing_result" },
      timestamp: 2,
    });

    // A head between a call and its results leaves stray results that are not sent.
    t.append({ kind: "reset", head: second });
    const view = contextOf(t.entries);
    assert.deepStrictEqual(described(view), []);
    assert.deepStrictEqual(
      view.entries.map((entry) => entry.kind),
      ["reset", "message"],
    );
  });

  it("reads the context as of an earlier entry", () => {
    const t = transcript();
    const first = t.message(user("first"));
    const call = t.message(assistant("calling", { calls: ["x", "y"] }));
    t.message(toolResult("x"));
    t.message(toolResult("y"));
    t.append({
      kind: "edit",
      edits: [{ target: first.id, action: "replace", messages: [user("first v2")] }],
    });
    t.append({ kind: "reset", head: "self", model: [user("fresh start")] });
    const tail = t.message(assistant("after reset"));
    // Stepped back before the edit and the reset: neither applies, and a cut call gets synthesized results.
    assert.deepStrictEqual(described(contextOf(t.entries, call.id)), [
      "user:first",
      "assistant:calling",
      "result:x:error",
      "result:y:error",
    ]);
    assert.deepStrictEqual(contextOf(t.entries, tail.id), contextOf(t.entries));
  });

  it("keeps the first of duplicate results and the call's timestamp on a synthesized one", () => {
    const messages = Context.orderToolResults([
      assistant("calling", { calls: ["a"] }),
      toolResult("a", "first"),
      toolResult("a", "second"),
      user("between"),
      assistant("again", { calls: ["b"] }),
      assistant("later"),
      toolResult("b", "too late"),
    ]);
    assert.deepStrictEqual(messages.map(describeMessage), [
      "assistant:calling",
      "result:a:first",
      "user:between",
      "assistant:again",
      "result:b:error",
      "assistant:later",
    ]);
  });

  it("leads with a system message only when user messages alone precede it", () => {
    const lead = (messages: ReadonlyArray<Message>) =>
      Context.leadWithSystem(messages, undefined).map(describeMessage);
    assert.deepStrictEqual(lead([user("a"), system({ s: "1" }), assistant("b")]), [
      "system:s",
      "user:a",
      "assistant:b",
    ]);
    assert.deepStrictEqual(lead([user("a"), assistant("b"), system({ s: "1" })]), [
      "user:a",
      "assistant:b",
      "system:s",
    ]);
    assert.deepStrictEqual(lead([system({ s: "1" }), user("a")]), ["system:s", "user:a"]);
    assert.deepStrictEqual(lead([user("a"), user("b")]), ["user:a", "user:b"]);
  });

  it("settles the ordered messages before the last assistant message", () => {
    const open = [
      user("q"),
      assistant("calling", { calls: ["x"] }),
      toolResult("x"),
      assistant("answer", { calls: ["y"] }),
      toolResult("y"),
    ];
    const split = Context.settle([], open);
    assert.deepStrictEqual(split.settled.map(describeMessage), [
      "user:q",
      "assistant:calling",
      "result:x:result x",
    ]);
    assert.deepStrictEqual(split.open.map(describeMessage), [
      "assistant:answer",
      "result:y:result y",
    ]);
    // Without an assistant message past the first position nothing settles.
    assert.deepStrictEqual(Context.settle([], [assistant("only"), toolResult("z")]).settled, []);
  });

  it("returns the active entries within the bounds, and none without bounds", () => {
    const t = transcript();
    t.message(user("old"));
    const kept = t.message(user("kept"));
    const older = t.append({ kind: "reset", head: "self", model: [user("older head")] });
    const summary = t.append({ kind: "summary", head: kept, model: [user("summary")] });
    const tail = t.message(user("tail"));
    const read = boundsAt(t.entries);
    assert.deepStrictEqual(Context.rangeQuery(read.bounds), {
      minEntryId: kept.id,
      maxEntryId: tail.id,
    });
    assert.deepStrictEqual(ids(read.range), [kept.id, older.id, summary.id, tail.id]);
    assert.deepStrictEqual(ids(Context.activeEntries(read.bounds, read.range)), [
      summary.id,
      kept.id,
      tail.id,
    ]);
    assert.deepStrictEqual(Context.activeEntries(undefined, []), []);
    assert.deepStrictEqual(Context.deriveContext(undefined, []), Context.emptyView());
  });

  // Over the pure range functions: extending a kept range must equal deriving the whole range.
  it("derives the same view from extended reads as from whole reads", () => {
    // Deterministic pseudo-random transcript (32-bit LCG, high bits): calls and results in any
    // order, missing and stray results, excluded stop reasons, system entries, notes, edits of earlier
    // entries, and head markers.
    let seed = 7;
    const random = (n: number) => {
      seed = (Math.imul(seed, 1_664_525) + 1_013_904_223) >>> 0;
      return Math.floor((seed / 2 ** 32) * n);
    };
    const seen = new Map<string, number>();
    const count = (category: string) => seen.set(category, (seen.get(category) ?? 0) + 1);
    const t = transcript();
    const calls: Array<string> = [];
    let next = 0;
    const draft = (): Draft => {
      const pick = random(14);
      if (pick < 3) {
        count("user");
        return { model: [user(`u${next++}`)] };
      }
      if (pick < 5) {
        const callIds = Array.from({ length: random(3) }, () => `c${next++}`);
        calls.push(...callIds);
        const stopReason = (["aborted", "error", "deferred"] as const)[random(9)];
        count(stopReason ?? "assistant");
        return { model: [assistant(`a${next++}`, { calls: callIds, stopReason })] };
      }
      if (pick < 8) {
        const matched = calls.length > 0 && random(5) > 0;
        count(matched ? "result" : "stray result");
        const id = matched ? calls.splice(random(calls.length), 1)[0]! : `stray${next++}`;
        return { model: [toolResult(id)] };
      }
      if (pick < 9) {
        count("system");
        return { model: [system({ s: `v${next++}` })] };
      }
      if (pick < 10) {
        count("note");
        return { kind: "note", data: { text: `n${next++}` } };
      }
      const target = t.entries[random(t.entries.length)];
      if (pick < 12 && target !== undefined) {
        const omit = random(2) === 0;
        count(omit ? "omit" : "replace");
        const edit: ContextEdit = omit
          ? { target: target.id, action: "omit" }
          : { target: target.id, action: "replace", messages: [user(`r${next++}`)] };
        return { kind: "edit", edits: [edit] };
      }
      if (pick < 13 && target !== undefined) {
        count("head");
        return { kind: "summary", head: target, model: [user(`h${next++}`)] };
      }
      count("user");
      return { model: [user(`u${next++}`)] };
    };

    let kept: Context.ContextRange | undefined;
    for (let step = 0; step < 150; step++) {
      const added = 1 + random(3);
      for (let index = 0; index < added; index++) t.append(draft());
      const read = boundsAt(t.entries);
      const previous = kept;
      kept =
        previous === undefined || previous.bounds.head?.id !== read.bounds.head?.id
          ? Context.deriveRange(read.bounds, read.range, false)
          : Context.extendRange(
              previous,
              read.bounds,
              read.range.filter((entry) => entry.id > previous.bounds.tail),
              false,
            );
      assert.deepStrictEqual(kept.view, Context.deriveContext(read.bounds, read.range));
      if (step % 10 === 9) {
        // An earlier cutoff under the same head marker derives from the kept entries.
        const at = t.entries[random(t.entries.length)]!.id;
        const cut = boundsAt(t.entries, at);
        if (cut.bounds.head?.id === kept.bounds.head?.id) {
          const reused = Context.deriveRange(
            cut.bounds,
            kept.entries.filter((entry) => entry.id <= at),
            false,
          );
          assert.deepStrictEqual(reused.view, contextOf(t.entries, at));
        }
      }
    }
    const categories = [
      "user",
      "assistant",
      "aborted",
      "error",
      "deferred",
      "result",
      "stray result",
      "system",
      "note",
      "omit",
      "replace",
      "head",
    ];
    for (const category of categories) assert.isAbove(seen.get(category) ?? 0, 0, category);
  });

  // Returned views are not frozen. The derivation shares records between a kept range and a view extended from
  // it; only its arrays are its own. A context read hands out a deep copy of the view (`copyView` in
  // src/internal/contextRead.ts), so HarnessContext.test.ts changes a nested field of a returned message and
  // checks the next read. `advanceRange` does freeze the published entries it keeps (`freezeJson(appended)`).
  it("freezes the entries a kept range takes from a publication", () => {
    const t = transcript();
    t.message(user("first"));
    const read = boundsAt(t.entries);
    const range = { ...Context.deriveRange(read.bounds, read.range, false), current: true };
    const second = t.message(assistant("second"));
    const advanced = Context.advanceRange(range, [second]);
    assert.isTrue(Object.isFrozen(second));
    assert.strictEqual(advanced?.entries.at(-1), second);
  });

  it("keeps extended views and kept ranges apart", () => {
    const t = transcript();
    t.message(user("first"));
    const read = boundsAt(t.entries);
    const range = Context.deriveRange(read.bounds, read.range, false);
    const second = t.message(assistant("second"));
    const extended = Context.extendRange(range, boundsAt(t.entries).bounds, [second], false);
    assert.strictEqual(range.view.entries.length, 1);
    assert.strictEqual(range.view.messages.length, 1);
    assert.strictEqual(extended.view.entries.length, 2);
    assert.notStrictEqual(extended.view.contributions, range.view.contributions);
  });
});

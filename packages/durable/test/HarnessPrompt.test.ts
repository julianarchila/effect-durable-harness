// The system prompt, on every backend: sections, system entries and the tool loadout.
//
// It calls the internals directly: `planSystemEntries`, `renderSections` and `replaySections` from
// `internal/prompt.ts`, `resolveAgent` from `internal/agent.ts`, `Settings.resolve`, and `getCurrentTools` and
// `toToolDeclaration` from `internal/transcript.ts`.
//
// Extensions are the ones `resolveAgent` takes; a section's `render` is an Effect, so a throwing section is a
// failing one (one fails, one dies, both reported); `wrapSection` wraps the section token, whose definition holds
// `render`; `planSystemEntries` takes tool declarations, which the generation task builds with `toolDeclaration`,
// so the loadout tests pass declarations; an empty object schema is its JSON Schema `{ type: "object",
// properties: {} }`.
//
// "propagates section errors after cancellation": a cancelled context is an interrupted fiber, and the
// interruption propagates out of `renderSections` without a report.
import { assert, it } from "@effect/vitest";
import * as Cause from "effect/Cause";
import * as Data from "effect/Data";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import type { Agent } from "../src/Agent.ts";
import { SystemEntry } from "../src/Entry.ts";
import * as Extension from "../src/Extension.ts";
import type { Conversation } from "../src/Harness.ts";
import type { ConversationId, EntryId } from "../src/Ids.ts";
import { resolveAgent } from "../src/internal/agent.ts";
import { planSystemEntries, renderSections, replaySections } from "../src/internal/prompt.ts";
import { getCurrentTools, toToolDeclaration } from "../src/internal/transcript.ts";
import type { Message, SystemMessage, Tool, UserMessage } from "../src/Message.ts";
import * as Section from "../src/Section.ts";
import * as Settings from "../src/Settings.ts";
import { Tx } from "../src/Tx.ts";
import { openHarness } from "./HarnessSupport.ts";
import { controlledStorage, describeEachBackend, flush, run } from "./SessionSupport.ts";

/** A section's failure. */
class RenderFailed extends Data.TaggedError("RenderFailed")<{ readonly message: string }> {}

interface Planned {
  readonly sections: Readonly<Record<string, string | null>>;
  readonly omit?: ReadonlyArray<EntryId>;
}

const user = (text: string): UserMessage => ({ role: "user", content: text, timestamp: 1 });

const systemOf = (message: Message | undefined): SystemMessage => {
  if (message?.role !== "system") throw new Error("Expected a system message");
  return message;
};

/** Appends planned system entries to `conversation`. */
const appendAll = (
  conversation: Conversation,
  drafts: ReadonlyArray<ReturnType<typeof planSystemEntries>[number]>,
) =>
  conversation.commit(
    Effect.gen(function* () {
      const tx = yield* Tx;
      for (const draft of drafts) yield* tx.appendEntry(SystemEntry, conversation.id, draft);
    }),
  );

/** Plan against the current context, append the plan, and check that replay then yields `desired` in order. */
const apply = (conversation: Conversation, desired: Record<string, string>) =>
  Effect.gen(function* () {
    const drafts = planSystemEntries(
      yield* conversation.context(),
      new Map(Object.entries(desired)),
      [],
      7,
    );
    yield* appendAll(conversation, drafts);
    const replayed = replaySections((yield* conversation.context()).messages);
    assert.deepStrictEqual([...replayed], Object.entries(desired));
    return drafts.map((draft): Planned => {
      const message = systemOf(draft.model?.[0]);
      assert.strictEqual(message.role, "system");
      assert.strictEqual(message.content, "");
      assert.strictEqual(message.timestamp, 7);
      const omit = draft.edits?.map((edit) => {
        assert.strictEqual(edit.action, "omit");
        return edit.target;
      });
      const sections = message.sections ?? {};
      return omit === undefined ? { sections } : { sections, omit };
    });
  });

/** A fresh root over the backend's storage. */
const root = Effect.gen(function* () {
  const storage = yield* controlledStorage;
  const opened = yield* openHarness(storage, []);
  return yield* opened.harness.ensureRoot();
});

const lastSystemId = (conversation: Conversation) =>
  Stream.runCollect(conversation.entries()).pipe(
    Effect.map((entries) => {
      const found = Array.from(entries).find((entry) => entry.kind === "core.system");
      if (found === undefined) throw new Error("No system entry");
      return found.id;
    }),
  );

const marker = (conversation: Conversation, head: EntryId | "self") =>
  conversation.commit(
    Effect.gen(function* () {
      const tx = yield* Tx;
      return (yield* tx.appendEntry(conversation.id, {
        kind: "summary",
        head,
        model: [user("summary")],
      })).id;
    }),
  );

/** A section with key `key` rendered by `render`. */
const section = (
  key: string,
  render: () => Effect.Effect<string | undefined, RenderFailed>,
  options?: Section.SectionOptions,
) => Section.make(key, render, options);

/** One extension per section, named `section:<key>`. */
const addSection = (
  key: string,
  render: () => Effect.Effect<string | undefined, RenderFailed>,
  options?: Section.SectionOptions,
) => Extension.make({ name: `section:${key}`, sections: [section(key, render, options)] });

/** The render of a section token as the registry stores it. */
const renderOf = (wrapped: Section.Any) =>
  // SAFETY: every section token is built by `Section.make`, whose definition holds `render`.
  (wrapped as Section.Section<RenderFailed, never>).definition.render;

const noRead = () => Effect.succeed(Option.none());

const input: Section.PromptInput = {
  // SAFETY: the sections these tests render never read the conversation.
  conversationId: 1 as ConversationId,
  agent: {
    thinkingLevel: "off",
    extensions: [],
    tools: [],
    deferred: [],
    callable: [],
    sections: [],
  } satisfies Agent,
  env: undefined,
  shown: {},
  // SAFETY: a read that finds nothing satisfies every overload.
  snapshot: noRead as Section.PromptInput["snapshot"],
  // SAFETY: as above.
  snapshotAsOf: noRead as Section.PromptInput["snapshotAsOf"],
};

/** A system message with these section changes. */
const system = (sections: Record<string, string | null>): SystemMessage => ({
  role: "system",
  content: "",
  sections,
  timestamp: 1,
});

/** A tool declaration whose parameters are an empty object schema. */
const declaration = (name: string, description = name): Tool => ({
  name,
  description,
  parameters: { type: "object", properties: {} },
});

const messageOf = (cause: Cause.Cause<unknown>) => {
  const error = Cause.squash(cause);
  return error instanceof Error ? error.message : String(error);
};

describeEachBackend("system prompt preparation", (backend) => {
  it.effect("renders sections in order with tags, omissions, wrappers, and failures", () =>
    Effect.gen(function* () {
      const extensions = [
        addSection("preamble", () => Effect.succeed("You are helpful."), { tag: false }),
        addSection("cwd", () => Effect.succeed("/repo")),
        addSection("skipped", () => Effect.sync(() => undefined)),
        addSection("failing", () => Effect.fail(new RenderFailed({ message: "render failed" }))),
        addSection("new-failing", () => Effect.die(new Error("also failed"))),
        Extension.make({
          name: "git",
          wraps: [
            Extension.wrapSection("cwd", (inner) =>
              Section.make(
                inner.key,
                (value: Section.PromptInput) =>
                  renderOf(inner)(value).pipe(Effect.map((text) => `${text} (git)`)),
                inner.definition.tag === undefined ? undefined : { tag: inner.definition.tag },
              ),
            ),
          ],
        }),
      ];
      const reports: Array<Cause.Cause<unknown>> = [];
      const shown = new Map([
        ["failing", "<failing>\nold\n</failing>"],
        ["cwd", "stale"],
      ]);
      const agent = resolveAgent(undefined, extensions, Settings.resolve(undefined), (cause) => {
        throw Cause.squash(cause);
      });
      const desired = yield* renderSections(agent.sections, input, shown, (cause) =>
        Effect.sync(() => {
          reports.push(cause);
        }),
      );
      assert.deepStrictEqual(
        [...desired],
        [
          ["preamble", "You are helpful."],
          ["cwd", "<cwd>\n/repo (git)\n</cwd>"],
          ["failing", "<failing>\nold\n</failing>"],
        ],
      );
      assert.deepStrictEqual(reports.map(messageOf), ["render failed", "also failed"]);
    }),
  );

  it.effect("propagates section errors after cancellation", () =>
    Effect.gen(function* () {
      const reports: Array<Cause.Cause<unknown>> = [];
      const cancelled = section("a", () => Effect.never);
      const rendering = yield* Effect.forkChild(
        renderSections([cancelled], input, new Map(), (cause) =>
          Effect.sync(() => {
            reports.push(cause);
          }),
        ),
        { startImmediately: true },
      );
      yield* flush;
      yield* Fiber.interrupt(rendering);
      const exit = yield* Fiber.await(rendering);
      assert.isTrue(Exit.isFailure(exit) && Cause.hasInterruptsOnly(exit.cause));
      assert.deepStrictEqual(reports, []);
    }),
  );

  it("replays sections in place, deletes on null, and appends re-additions", () => {
    const shown = replaySections([
      system({ a: "1", b: "2", c: "3" }),
      user("x"),
      system({ b: "20", a: null }),
      system({ a: "10" }),
    ]);
    assert.deepStrictEqual(
      [...shown],
      [
        ["b", "20"],
        ["c", "3"],
        ["a", "10"],
      ],
    );
  });

  it.effect("emits minimal value patches, removals, and additions", () =>
    run(backend)(
      Effect.gen(function* () {
        const conversation = yield* root;
        assert.deepStrictEqual(yield* apply(conversation, { a: "1", b: "2", c: "3" }), [
          { sections: { a: "1", b: "2", c: "3" } },
        ]);
        assert.deepStrictEqual(yield* apply(conversation, { a: "1", b: "20", c: "3", d: "4" }), [
          { sections: { b: "20", d: "4" } },
        ]);
        assert.deepStrictEqual(yield* apply(conversation, { a: "1", c: "3", d: "4" }), [
          { sections: { b: null } },
        ]);
        assert.deepStrictEqual(yield* apply(conversation, { a: "1", c: "3", d: "4" }), []);
        assert.deepStrictEqual(yield* apply(conversation, {}), [
          { sections: { a: null, c: null, d: null } },
        ]);
      }),
    ),
  );

  it.effect("rewrites order-only changes and re-additions as two entries", () =>
    run(backend)(
      Effect.gen(function* () {
        const conversation = yield* root;
        yield* apply(conversation, { a: "1", b: "2" });
        assert.deepStrictEqual(yield* apply(conversation, { b: "2", a: "1" }), [
          { sections: { a: null, b: null } },
          { sections: { b: "2", a: "1" } },
        ]);
        yield* run(backend)(
          Effect.gen(function* () {
            const readded = yield* root;
            yield* apply(readded, { a: "1", b: "2", c: "3" });
            assert.deepStrictEqual(yield* apply(readded, { a: "1", c: "3" }), [
              { sections: { b: null } },
            ]);
            // Patching would append `b` after `c`.
            assert.deepStrictEqual(yield* apply(readded, { a: "1", b: "2", c: "3" }), [
              { sections: { a: null, c: null } },
              { sections: { a: "1", b: "2", c: "3" } },
            ]);
          }),
        );
      }),
    ),
  );

  it.effect("rebaselines after a head marker, omitting retained deltas on both sides of it", () =>
    run(backend)(
      Effect.gen(function* () {
        const conversation = yield* root;
        yield* apply(conversation, { a: "1", b: "2" });
        yield* conversation.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            yield* tx.appendEntry(conversation.id, { kind: "core.user", model: [user("hi")] });
          }),
        );
        yield* apply(conversation, { a: "1", b: "20" });
        const delta = yield* lastSystemId(conversation);
        // The head keeps the delta but cuts its baseline: replay alone would show only `b`.
        yield* marker(conversation, delta);
        assert.deepStrictEqual(yield* apply(conversation, { a: "1", b: "20" }), [
          { sections: { a: "1", b: "20" }, omit: [delta] },
        ]);
        const baseline = yield* lastSystemId(conversation);
        // A system entry follows the marker now, so later changes are ordinary patches.
        assert.deepStrictEqual(yield* apply(conversation, { a: "1", b: "21" }), [
          { sections: { b: "21" } },
        ]);
        const after = yield* lastSystemId(conversation);
        // A second marker keeps deltas from both sides of the first one.
        yield* marker(conversation, delta);
        assert.deepStrictEqual(yield* apply(conversation, { a: "1", b: "21" }), [
          { sections: { a: "1", b: "21" }, omit: [delta, baseline, after] },
        ]);
      }),
    ),
  );

  it.effect("writes a complete post-head baseline even when replay already matches", () =>
    run(backend)(
      Effect.gen(function* () {
        const conversation = yield* root;
        yield* apply(conversation, { a: "1" });
        const baseline = yield* lastSystemId(conversation);
        yield* marker(conversation, baseline);
        assert.deepStrictEqual(yield* apply(conversation, { a: "1" }), [
          { sections: { a: "1" }, omit: [baseline] },
        ]);
        yield* marker(conversation, "self");
        assert.deepStrictEqual(yield* apply(conversation, {}), [{ sections: {} }]);
        assert.deepStrictEqual(yield* apply(conversation, {}), []);
      }),
    ),
  );
});

interface ToolChange {
  readonly removed?: ReadonlyArray<string>;
  readonly added?: ReadonlyArray<string>;
  readonly sections?: Readonly<Record<string, string | null>>;
}

describeEachBackend("tool loadout preparation", (backend) => {
  /** Plan tools only, append the plan, check that replay offers `tools` in order, and return each message's changes. */
  const applyTools = (
    conversation: Conversation,
    tools: ReadonlyArray<Tool>,
    sections: Record<string, string> = {},
  ) =>
    Effect.gen(function* () {
      const drafts = planSystemEntries(
        yield* conversation.context(),
        new Map(Object.entries(sections)),
        tools,
        7,
      );
      yield* appendAll(conversation, drafts);
      const offered = getCurrentTools((yield* conversation.context()).messages);
      assert.deepStrictEqual(offered, tools.map(toToolDeclaration));
      return drafts.map((draft): ToolChange => {
        const message = systemOf(draft.model?.[0]);
        const change: { -readonly [K in keyof ToolChange]: ToolChange[K] } = {};
        if (message.toolsRemoved !== undefined)
          change.removed = message.toolsRemoved.map((tool) => tool.name);
        if (message.toolsAdded !== undefined)
          change.added = message.toolsAdded.map((tool) => tool.name);
        if (message.sections !== undefined) change.sections = message.sections;
        return change;
      });
    });

  it.effect(
    "adds, removes, replaces changed declarations, and rewrites the order when needed",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const conversation = yield* root;
          const [a, b, c] = [declaration("a"), declaration("b"), declaration("c")];
          assert.deepStrictEqual(yield* applyTools(conversation, [a, b]), [{ added: ["a", "b"] }]);
          assert.deepStrictEqual(yield* applyTools(conversation, [a, b]), []);
          assert.deepStrictEqual(yield* applyTools(conversation, [a, b, c]), [{ added: ["c"] }]);
          assert.deepStrictEqual(yield* applyTools(conversation, [a, c]), [{ removed: ["b"] }]);
          // A changed declaration at the end is removed and re-added in place.
          const c2 = declaration("c", "changed");
          assert.deepStrictEqual(yield* applyTools(conversation, [a, c2]), [
            { removed: ["c"], added: ["c"] },
          ]);
          // A changed declaration in the middle would move to the end, so the whole order is rewritten.
          const a2 = declaration("a", "changed");
          assert.deepStrictEqual(yield* applyTools(conversation, [a2, c2]), [
            { removed: ["a", "c"], added: ["a", "c"] },
          ]);
          // Order-only change.
          assert.deepStrictEqual(yield* applyTools(conversation, [c2, a2]), [
            { removed: ["a", "c"], added: ["c", "a"] },
          ]);
          assert.deepStrictEqual(yield* applyTools(conversation, []), [{ removed: ["c", "a"] }]);
        }),
      ),
  );

  it.effect(
    "puts tool changes on the last section entry and re-declares every tool after a head cut",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const conversation = yield* root;
          const [a, b] = [declaration("a"), declaration("b")];
          assert.deepStrictEqual(yield* applyTools(conversation, [a], { x: "1", y: "2" }), [
            { added: ["a"], sections: { x: "1", y: "2" } },
          ]);
          // Section order changes need two entries; the tool change rides on the second.
          assert.deepStrictEqual(yield* applyTools(conversation, [a, b], { y: "2", x: "1" }), [
            { sections: { x: null, y: null } },
            { added: ["b"], sections: { y: "2", x: "1" } },
          ]);
          yield* marker(conversation, "self");
          assert.deepStrictEqual(yield* applyTools(conversation, [a, b], { y: "2", x: "1" }), [
            { added: ["a", "b"], sections: { y: "2", x: "1" } },
          ]);
        }),
      ),
  );
});

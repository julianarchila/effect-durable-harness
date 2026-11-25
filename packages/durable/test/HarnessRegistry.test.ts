// The extension registry: validation, resolution of tools, sections, settings and hooks, and the built-in tasks.
//
// The registry is the static list of extensions a Harness is built with: an invalid registry fails the layer
// build, and an uninstall or reinstall is a reopen with other extensions, on every backend. `resolveAgent` takes
// the extensions, settings resolve with `Settings.resolve`, and reports are Causes. Tools and sections are tokens
// whose `definition` holds `description` and `render`, so a wrapper spreads the definition. The resolved
// `progress` includes `persistWithinMs`, and `agentHooks` returns each handler set with its extension's name.
//
// Notes on some cases:
// - "validates the registry as it would be after an install and publishes nothing when invalid": each
//   invalid extension fails the layer build with its message; nothing is published, as no Harness opens.
//   Replacing the extension that holds a task name is a reopen with the new version of it, under which a task
//   of that version is ready instead of blocked.
// - "always holds the built-in tasks, which are not an extension": the built-in tasks are
//   `[GenerationTask, ToolTask, CompactionTask]`, seen through `inspect`: their tasks are ready under a
//   Harness without extensions, and a custom task is ready only under a Harness built with its extension.
// - "skips uninstalled names and resolves them again once they are installed": two extension lists.
//
// Omitted: "installs, replaces in place, and uninstalls extensions by name", since there is no runtime registry.
import { assert, describe, it } from "@effect/vitest";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import type { AgentState } from "../src/Agent.ts";
import * as AgentTool from "../src/AgentTool.ts";
import * as Extension from "../src/Extension.ts";
import type { TaskInspection } from "../src/Harness.ts";
import * as Hook from "../src/Hook.ts";
import type { ConversationId, EntryId, TaskId } from "../src/Ids.ts";
import { agentHooks, resolveAgent } from "../src/internal/agent.ts";
import { CompactionTask } from "../src/internal/compaction.ts";
import { GenerationTask } from "../src/internal/generation.ts";
import { ToolTask } from "../src/internal/tool.ts";
import * as Section from "../src/Section.ts";
import * as Settings from "../src/Settings.ts";
import type { HarnessSettings } from "../src/Settings.ts";
import * as Task from "../src/Task.ts";
import { Tx } from "../src/Tx.ts";
import { completed, openHarness, type TestHarness } from "./HarnessSupport.ts";
import {
  type ControlledStorage,
  controlledStorage,
  describeEachBackend,
  failureMessage,
  run,
} from "./SessionSupport.ts";

/** A tool whose description is `<name> tool`. */
const tool = (name: string, description = `${name} tool`) =>
  AgentTool.make(name, {
    description,
    parameters: Schema.Struct({}),
    execute: () => Effect.succeed({ output: [] }),
  });

/** A one-phase task that completes with null. */
const task = (name: string, version = 1) =>
  Task.make({
    name,
    version,
    input: Schema.Null,
    checkpoint: Schema.Struct({ phase: Schema.Literal("run") }),
    result: Schema.Null,
    initial: () => ({ phase: "run" }),
    phases: {
      run: (_task, runtime) => runtime.commit(() => Effect.succeed(completed(null))),
    },
    abort: () => Effect.void,
  });

/** A section with key `key` rendered by `render`. */
const section = (key: string, text: string, options?: Section.SectionOptions) =>
  Section.make(key, () => Effect.succeed(text), options);

const names = (items: ReadonlyArray<{ readonly name: string }>) => items.map((item) => item.name);

const messageOf = (cause: Cause.Cause<unknown>) => {
  const error = Cause.squash(cause);
  return error instanceof Error ? error.message : String(error);
};

/** Resolve `state` against `extensions` and `settings`, collecting reports. */
const resolve = (
  state: AgentState | undefined,
  extensions: ReadonlyArray<Extension.Any>,
  settings: HarnessSettings = {},
  reports: Array<Cause.Cause<unknown>> = [],
) =>
  resolveAgent(state, extensions, Settings.resolve(settings), (cause) => {
    reports.push(cause);
  });

/** The render of a section token as the registry stores it. */
const renderOf = (wrapped: Section.Any) =>
  // SAFETY: every section token is built by `Section.make`, whose definition holds `render`.
  (wrapped as Section.Section<never, never>).definition.render;

const noRead = () => Effect.succeed(Option.none());

const rendered = (agent: ReturnType<typeof resolve>) => {
  const input: Section.PromptInput = {
    // SAFETY: these sections never read the conversation.
    conversationId: 1 as ConversationId,
    agent,
    env: undefined,
    shown: {},
    // SAFETY: a read that finds nothing satisfies every overload.
    snapshot: noRead as Section.PromptInput["snapshot"],
    // SAFETY: as above.
    snapshotAsOf: noRead as Section.PromptInput["snapshotAsOf"],
  };
  return Effect.forEach(agent.sections, (item) =>
    renderOf(item)(input).pipe(Effect.map((text) => [item.key, text] as const)),
  );
};

/** A wrapper of a tool's description. */
const describedAs =
  (describe: (description: string) => string) =>
  (inner: AgentTool.Any): AgentTool.Any => ({
    ...inner,
    definition: { ...inner.definition, description: describe(inner.definition.description) },
  });

/** The derived state `inspect` gives task `id`. */
const stateOf = (h: TestHarness, id: TaskId) =>
  h.harness.inspect.pipe(
    Effect.map(
      (inspection): TaskInspection["state"] | undefined =>
        inspection.tasks.find((each) => each.record.id === id)?.state,
    ),
  );

const openTasks = (storage: ControlledStorage, extensions: ReadonlyArray<Extension.Any>) =>
  Effect.gen(function* () {
    const h = yield* openHarness(storage, [], { extensions });
    const root = yield* h.harness.ensureRoot();
    return { h, root };
  });

describeEachBackend("registry", (backend) => {
  it.effect(
    "validates the registry as it would be after an install and publishes nothing when invalid",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const Tasks = Extension.make({ name: "tasks", tasks: [task("app.index")] });
          const invalid = [
            [
              Extension.make({ name: "x", tools: [tool("read"), tool("read")] }),
              "two tools named read",
            ],
            [
              Extension.make({ name: "x", sections: [section("a", "1"), section("a", "2")] }),
              "two sections",
            ],
            [Extension.make({ name: "x", sections: [section("Bad Key", "")] }), "must match"],
            [Extension.make({ name: "x", sections: [section("instructions", "")] }), "reserved"],
            [Extension.make({ name: "x", tasks: [task("core.generation")] }), "already installed"],
            [Extension.make({ name: "x", tasks: [task("app.index")] }), "already installed"],
          ] as const;
          const storage = yield* controlledStorage;
          for (const [extension, message] of invalid) {
            assert.include(
              yield* failureMessage(
                Effect.scoped(openHarness(storage, [], { extensions: [Tasks, extension] })),
              ),
              message,
            );
          }
          // Replacing the extension that holds a task name is valid: a reopen with its new version.
          const first = yield* openTasks(storage, [Tasks]);
          const id = yield* first.root.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              return yield* tx.createTask(task("app.index", 2), null, {
                ownership: { kind: "conversation" },
              });
            }),
          );
          assert.deepStrictEqual(yield* stateOf(first.h, id), {
            kind: "blocked",
            reason: "task_too_old",
            message: undefined,
          });
          yield* first.h.close;
          const second = yield* openTasks(storage, [
            Extension.make({ name: "tasks", tasks: [task("app.index", 2)] }),
          ]);
          assert.deepStrictEqual(yield* stateOf(second.h, id), {
            kind: "pending",
            migrates: false,
          });
          yield* second.h.close;
        }),
      ),
  );

  it.effect("always holds the built-in tasks, which are not an extension", () =>
    run(backend)(
      Effect.gen(function* () {
        const custom = task("app.custom");
        const storage = yield* controlledStorage;
        const bare = yield* openTasks(storage, []);
        const ids = yield* bare.root.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            const options = { ownership: { kind: "conversation" } } as const;
            return [
              yield* tx.createTask(GenerationTask, {}, options),
              yield* tx.createTask(
                ToolTask,
                // SAFETY: an entry ID the task never reads; inspection runs no task code.
                { kind: "model", assistant: 1 as EntryId, callId: "call" },
                options,
              ),
              yield* tx.createTask(CompactionTask, { reason: "manual" }, options),
              yield* tx.createTask(custom, null, options),
            ];
          }),
        );
        const states = (h: TestHarness) => Effect.forEach(ids, (id) => stateOf(h, id));
        const ready = { kind: "pending", migrates: false } as const;
        const missing = { kind: "blocked", reason: "missing_task", message: undefined } as const;
        assert.deepStrictEqual(yield* states(bare.h), [ready, ready, ready, missing]);
        yield* bare.h.close;
        const installed = yield* openTasks(storage, [
          Extension.make({ name: "custom", tasks: [custom] }),
        ]);
        assert.deepStrictEqual(yield* states(installed.h), [ready, ready, ready, ready]);
        yield* installed.h.close;
        const uninstalled = yield* openTasks(storage, []);
        assert.deepStrictEqual(yield* states(uninstalled.h), [ready, ready, ready, missing]);
        yield* uninstalled.h.close;
      }),
    ),
  );
});

describe("settings", () => {
  it("keeps the default of a progress interval given as undefined", () => {
    const progress = { partialIntervalMs: undefined, outputIntervalMs: 250 };
    assert.deepStrictEqual(Settings.resolve({ progress }).progress, {
      partialIntervalMs: 100,
      outputIntervalMs: 250,
      persistWithinMs: 1000,
    });
  });
});

describe("agent resolution", () => {
  const read = tool("read");
  const bash = tool("bash");
  const edit = tool("edit");
  const Coding = Extension.make({
    name: "coding",
    tools: [read, bash, edit],
    sections: [section("preamble", "You code.", { tag: false }), section("cwd", "/repo")],
  });
  const Skills = Extension.make({ name: "skills", sections: [section("skills", "S")] });
  const Reviewer = Extension.make({ name: "reviewer", sections: [section("role", "Review.")] });
  const installed: ReadonlyArray<Extension.Any> = [Coding, Skills, Reviewer];

  it("selects the default, an array, or the default edited by add and remove", () => {
    assert.deepStrictEqual(names(resolve(undefined, installed).extensions), [
      "coding",
      "skills",
      "reviewer",
    ]);
    const settings = { extensions: [Coding, Skills] };
    assert.deepStrictEqual(names(resolve({}, installed, settings).extensions), [
      "coding",
      "skills",
    ]);
    assert.deepStrictEqual(
      names(resolve({ extensions: ["reviewer", "coding"] }, installed, settings).extensions),
      ["reviewer", "coding"],
    );
    // Add appends, remove drops, duplicates keep their first position, uninstalled names are skipped.
    const edited = { extensions: { add: ["reviewer", "coding", "gone"], remove: ["skills"] } };
    assert.deepStrictEqual(names(resolve(edited, installed, settings).extensions), [
      "coding",
      "reviewer",
    ]);
    // An old object stands for its name: the installed extension is selected.
    const stale = { extensions: [Extension.make({ name: "skills" }), Skills] };
    const selected = resolve({}, installed, stale).extensions;
    assert.strictEqual(selected.length, 1);
    assert.strictEqual(selected[0], Skills);
  });

  it("skips uninstalled names and resolves them again once they are installed", () => {
    const state: AgentState = { extensions: ["coding", "skills"] };
    assert.deepStrictEqual(names(resolve(state, [Coding]).extensions), ["coding"]);
    assert.deepStrictEqual(names(resolve(state, [Coding, Skills]).extensions), [
      "coding",
      "skills",
    ]);
  });

  it("replaces same-name tools in place, wraps the winner, then applies the filter", () => {
    const venvBash = tool("bash", "venv bash");
    const calls: Array<string> = [];
    const Venv = Extension.make({ name: "venv", tools: [venvBash] });
    const Timing = Extension.make({
      name: "timing",
      wraps: [
        Extension.wrapTool(
          bash,
          describedAs((description) => `${description} (timed)`),
        ),
        Extension.wrapTool(
          bash,
          describedAs((description) => `${description} [2]`),
        ),
        // No `grep` is selected: the wrapper does nothing and reports nothing.
        Extension.wrapTool(tool("grep"), () => {
          calls.push("grep");
          return tool("grep");
        }),
      ],
    });
    const local = [Coding, Venv, Timing];
    const reports: Array<Cause.Cause<unknown>> = [];
    const agent = resolve(undefined, local, {}, reports);
    assert.deepStrictEqual(
      agent.tools.map((each) => [each.name, each.definition.description]),
      [
        ["read", "read tool"],
        ["bash", "venv bash (timed) [2]"],
        ["edit", "edit tool"],
      ],
    );
    assert.deepStrictEqual(reports, []);
    assert.deepStrictEqual(calls, []);

    // An array keeps exactly these names in its order, a repeated name at its first position.
    const filtered = resolve({ tools: ["edit", "missing", "read", "edit"] }, local);
    assert.deepStrictEqual(names(filtered.tools), ["edit", "read"]);
    assert.deepStrictEqual(names(resolve({ tools: { remove: ["bash"] } }, local).tools), [
      "read",
      "edit",
    ]);
  });

  it("drops a tool or section whose wrapper throws or renames it and reports the failure", () => {
    const Broken = Extension.make({
      name: "broken",
      wraps: [
        Extension.wrapTool(read, () => {
          throw new Error("wrapper failed");
        }),
        Extension.wrapTool(edit, (inner) => ({ ...inner, name: "renamed" })),
        Extension.wrapSection("cwd", () => {
          throw new Error("section wrapper failed");
        }),
      ],
    });
    const reports: Array<Cause.Cause<unknown>> = [];
    const agent = resolve(undefined, [Coding, Broken], {}, reports);
    assert.deepStrictEqual(names(agent.tools), ["bash"]);
    assert.deepStrictEqual(
      agent.sections.map((each) => each.key),
      ["preamble"],
    );
    assert.deepStrictEqual(reports.map(messageOf), [
      "wrapper failed",
      "Wrapper renamed edit to renamed",
      "section wrapper failed",
    ]);
  });

  it.effect(
    "orders sections by extension, replaces same keys in place, and renders instructions last and unwrapped",
    () =>
      Effect.gen(function* () {
        const Override = Extension.make({
          name: "override",
          sections: [section("preamble", "You review.", { tag: false })],
          wraps: [
            Extension.wrapSection("cwd", (inner) =>
              Section.make(inner.key, (input: Section.PromptInput) =>
                renderOf(inner)(input).pipe(Effect.map((text) => `${text}!`)),
              ),
            ),
            // Instructions are not wrapped.
            Extension.wrapSection("instructions", () => {
              throw new Error("never");
            }),
          ],
        });
        const reports: Array<Cause.Cause<unknown>> = [];
        const agent = resolve(
          { instructions: "Be terse." },
          [Coding, Skills, Override],
          {},
          reports,
        );
        assert.deepStrictEqual(yield* rendered(agent), [
          ["preamble", "You review."],
          ["cwd", "/repo!"],
          ["skills", "S"],
          ["instructions", "Be terse."],
        ]);
        assert.isUndefined(agent.sections.at(-1)?.definition.tag);
        assert.deepStrictEqual(reports, []);
      }),
  );

  it("collects hooks of the selected extensions in extension order and applies field defaults", () => {
    const first = { beforeTool: () => Effect.sync(() => undefined) };
    const second = { beforeTool: () => Effect.sync(() => undefined) };
    const onYield = { onYield: () => Effect.sync(() => undefined) };
    const local = [
      Extension.make({ name: "a", hooks: [Hook.tool(first), Hook.generation(onYield)] }),
      Extension.make({ name: "b", hooks: [Hook.tool(second)] }),
    ];
    const inOrder = agentHooks(resolve(undefined, local), "core.tool");
    assert.strictEqual(inOrder.length, 2);
    assert.deepStrictEqual(inOrder[0], { extension: "a", handlers: first });
    assert.deepStrictEqual(inOrder[1], { extension: "b", handlers: second });
    const reversed = agentHooks(resolve({ extensions: ["b", "a"] }, local), "core.tool");
    assert.strictEqual(reversed.length, 2);
    assert.deepStrictEqual(reversed[0], { extension: "b", handlers: second });
    assert.deepStrictEqual(reversed[1], { extension: "a", handlers: first });
    assert.deepStrictEqual(
      agentHooks(resolve({ extensions: ["b"] }, local), "core.generation"),
      [],
    );

    const defaults = resolve(undefined, local);
    assert.isUndefined(defaults.model);
    assert.strictEqual(defaults.thinkingLevel, "off");
    assert.isUndefined(defaults.cwd);
    const configured = resolve(
      { model: { provider: "p", modelId: "m" }, thinkingLevel: "high", cwd: "/w" },
      local,
    );
    assert.deepStrictEqual(configured.model, { provider: "p", modelId: "m" });
    assert.strictEqual(configured.thinkingLevel, "high");
    assert.strictEqual(configured.cwd, "/w");
  });
});

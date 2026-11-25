// Deferred tools and the opt-in `tool_search` extension, on every backend.
//
// The registry is the static extensions of `openChat`; each tool is an extension named `tool:<name>` holding it, in
// registration order; `faux` is `FauxModel`; `settle` lets the `TestClock` advance in 5 ms steps; each case runs over
// a fresh storage of the backend; `harness.snapshot` is the Session's `snapshot` (an `Option`).
//
// "keeps a loaded tool when its extension is gone for a while, ...": the registry is static, so the Harness is
// closed and reopened over the same storage with each registry, the extension in the same place.
//
// "finds a deferred tool by a field of a nested schema class" covers the `$ref`s Effect's JSON Schema generator
// leaves (`ToolSearch.ts`, `schemaText`).
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import { AgentDoc } from "../src/Agent.ts";
import * as AgentTool from "../src/AgentTool.ts";
import * as Extension from "../src/Extension.ts";
import type { Conversation } from "../src/Harness.ts";
import type { AssistantMessage } from "../src/Message.ts";
import type { EntryRecord } from "../src/Records.ts";
import type { Submission } from "../src/Submission.ts";
import { fauxAssistantMessage, fauxText, fauxToolCall } from "../src/testing/FauxModel.ts";
import * as ToolSearch from "../src/ToolSearch.ts";
import { allEntries, type ChatSetup, chatSetup, openChat, waitFor } from "./ChatSupport.ts";
import type { TestHarness } from "./HarnessSupport.ts";
import { controlledStorage, describeEachBackend, run } from "./SessionSupport.ts";

const Parameters = Schema.Struct({
  number: Schema.optionalKey(Schema.Number.annotate({ description: "Issue number" })),
});

type Extra = Omit<
  AgentTool.MakeOptions<typeof Parameters.Type, Schema.Json, never, never>,
  "description" | "parameters" | "execute"
>;

/** A tool with an optional `number` parameter that answers `<name> ran`. */
const tool = (name: string, description: string, extra: Extra = {}) =>
  AgentTool.make(name, {
    description,
    parameters: Parameters,
    execute: () => Effect.succeed(AgentTool.text(`${name} ran`)),
    ...extra,
  });

/** An extension holding one tool, named `tool:<name>`. */
const toolExtension = (agentTool: AgentTool.Any) =>
  Extension.make({ name: `tool:${agentTool.name}`, tools: [agentTool] });

/** A nested, recursive schema class: Effect's JSON Schema puts it in `$defs` and refers to it with `$ref`. */
class IssueFilter extends Schema.Class<IssueFilter>("IssueFilter")({
  milestone: Schema.String.annotate({ description: "The sprint the issues belong to" }),
  children: Schema.Array(Schema.suspend((): Schema.Codec<IssueFilter> => IssueFilter)),
}) {}

const DONE = fauxAssistantMessage([fauxText("done")]);

const call = (name: string, args: Record<string, Schema.Json> = {}, id = "c1"): AssistantMessage =>
  fauxAssistantMessage([fauxToolCall(name, args, { id })], { stopReason: "toolUse" });

const alpha = tool("alpha", "Alpha tool");
const getIssue = tool("github_get_issue", "Get a GitHub issue by number", { deferred: true });
const listRepos = tool("github_list_repos", "List the repositories of a user", { deferred: true });
const omega = tool("omega", "Omega tool");

/** `alpha`, the deferred `github_get_issue` and `github_list_repos`, then `omega`, and `tool_search` last. */
const setup = (): ReadonlyArray<Extension.Any> => [
  toolExtension(alpha),
  toolExtension(getIssue),
  toolExtension(listRepos),
  toolExtension(omega),
  ToolSearch.extension,
];

/** Awaits the submission while the `TestClock` advances in 5 ms steps. */
const settle = (submission: Submission) =>
  Effect.gen(function* () {
    const fiber = yield* Effect.forkChild(submission.await, { startImmediately: true });
    yield* waitFor(Effect.sync(() => fiber.pollUnsafe() !== undefined));
    return yield* Fiber.join(fiber);
  });

const ask = (conversation: Conversation, content: string) =>
  conversation.submit({ type: "input", content }).pipe(Effect.flatMap(settle));

const offered = (root: Conversation) =>
  root.agent.pipe(Effect.map((agent) => agent.tools.map((each) => each.name)));

const deferredNames = (root: Conversation) =>
  root.agent.pipe(Effect.map((agent) => agent.deferred.map((each) => each.name)));

const modelTools = (harness: TestHarness, root: Conversation) =>
  harness.session
    .snapshot(AgentDoc, root.id)
    .pipe(Effect.map((agent) => Option.getOrUndefined(agent)?.modelTools));

/** The model messages of the `core.system` entries, oldest first. */
const systemMessages = (entries: ReadonlyArray<EntryRecord>) =>
  entries.flatMap((entry) => {
    const message = entry.model?.[0];
    return entry.kind === "core.system" && message?.role === "system" ? [message] : [];
  });

const toolResultTexts = (entries: ReadonlyArray<EntryRecord>): Array<string> =>
  entries.flatMap((entry) => {
    const message = entry.model?.[0];
    return entry.kind === "core.tool-result" && message?.role === "toolResult"
      ? [message.content.map((item) => (item.type === "text" ? item.text : "")).join("")]
      : [];
  });

const ownerless = { ownership: { kind: "ownerless" } } as const;

describeEachBackend("deferred tools and tool_search", (backend) => {
  it.effect(
    "offers a deferred tool only once tool_search loads it, by appending its declaration",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const chat: ChatSetup = chatSetup();
          chat.faux.setResponses([
            call("tool_search", { query: "github issue", limit: 1 }),
            call("github_get_issue", { number: 42 }, "c2"),
            DONE,
          ]);
          const { harness, root } = yield* openChat(yield* controlledStorage, chat, {
            extensions: setup(),
          });
          assert.deepStrictEqual(yield* offered(root), ["alpha", "omega", "tool_search"]);
          assert.deepStrictEqual(yield* deferredNames(root), [
            "github_get_issue",
            "github_list_repos",
          ]);
          // Deferred tools stay callable by other tools, such as code mode scripts.
          assert.include(
            (yield* root.agent).callable.map((each) => each.name),
            "github_get_issue",
          );
          yield* ask(root, "go");
          const entries = yield* allEntries(root);
          const [search, issue] = toolResultTexts(entries);
          assert.strictEqual(
            search,
            "Loaded 1 tool. They are available from your next call:\n- github_get_issue: Get a GitHub issue by number",
          );
          assert.strictEqual(issue, "github_get_issue ran");
          assert.deepStrictEqual(yield* modelTools(harness, root), { add: ["github_get_issue"] });
          assert.deepStrictEqual(yield* offered(root), [
            "alpha",
            "omega",
            "tool_search",
            "github_get_issue",
          ]);
          // A later search sees only the tools not loaded yet.
          assert.deepStrictEqual(yield* deferredNames(root), ["github_list_repos"]);
          // The load appended the declaration: no tool was removed and declared again, so the cached prefix holds.
          const [baseline, load] = systemMessages(entries);
          assert.deepStrictEqual(
            baseline!.toolsAdded!.map((each) => each.name),
            ["alpha", "omega", "tool_search"],
          );
          assert.deepStrictEqual(
            load!.toolsAdded!.map((each) => each.name),
            ["github_get_issue"],
          );
          assert.isUndefined(load!.toolsRemoved);
          yield* harness.close;
        }),
      ),
  );

  it.effect("appends each loaded tool in load order, not registry order", () =>
    run(backend)(
      Effect.gen(function* () {
        const chat = chatSetup();
        chat.faux.setResponses([
          call("tool_search", { query: "repositories", limit: 1 }),
          call("tool_search", { query: "issue", limit: 1 }, "c2"),
          DONE,
        ]);
        const { harness, root } = yield* openChat(yield* controlledStorage, chat, {
          extensions: setup(),
        });
        yield* ask(root, "go");
        assert.deepStrictEqual(yield* offered(root), [
          "alpha",
          "omega",
          "tool_search",
          "github_list_repos",
          "github_get_issue",
        ]);
        const loads = systemMessages(yield* allEntries(root)).slice(1);
        assert.deepStrictEqual(
          loads.map((message) => message.toolsAdded?.map((each) => each.name)),
          [["github_list_repos"], ["github_get_issue"]],
        );
        assert.isTrue(loads.every((message) => message.toolsRemoved === undefined));
        yield* harness.close;
      }),
    ),
  );

  it.effect(
    "records loads by name: unknown and non-deferred names change nothing, remove wins, and a load lifts it",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const chat = chatSetup();
          const grow = AgentTool.make("grow", {
            description: "grow",
            parameters: Schema.Struct({}),
            execute: () =>
              Effect.succeed({
                output: [],
                control: { addTools: ["github_get_issue", "alpha", "ghost"] },
              }),
          });
          chat.faux.setResponses([call("grow"), DONE, call("grow"), DONE]);
          const { harness, root } = yield* openChat(yield* controlledStorage, chat, {
            extensions: [...setup(), toolExtension(grow)],
          });
          yield* ask(root, "go");
          assert.deepStrictEqual(yield* modelTools(harness, root), {
            add: ["github_get_issue", "alpha", "ghost"],
          });
          assert.deepStrictEqual(yield* offered(root), [
            "alpha",
            "omega",
            "tool_search",
            "grow",
            "github_get_issue",
          ]);
          // A name in both remove and add is not offered.
          const callableIssue = (yield* root.agent).callable.find(
            (each) => each.name === "github_get_issue",
          );
          yield* root.configure({
            modelTools: { remove: [callableIssue!], add: [callableIssue!] },
          });
          assert.notInclude(yield* offered(root), "github_get_issue");
          // Loading it again drops it from remove.
          yield* ask(root, "again");
          assert.deepStrictEqual(yield* modelTools(harness, root), {
            add: ["github_get_issue", "alpha", "ghost"],
          });
          assert.include(yield* offered(root), "github_get_issue");
          yield* harness.close;
        }),
      ),
  );

  it.effect(
    "keeps a loaded tool when its extension is gone for a while, and appends to an explicit array",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const chat = chatSetup();
          const storage = yield* controlledStorage;
          chat.faux.setResponses([call("tool_search", { query: "github issue", limit: 1 }), DONE]);
          const first = yield* openChat(storage, chat, { extensions: setup() });
          yield* ask(first.root, "go");
          yield* first.harness.close;
          // The server goes away: its extension is installed again without the tool.
          const gone = setup().map((extension) =>
            extension.name === "tool:github_get_issue"
              ? Extension.make({ name: "tool:github_get_issue", tools: [] })
              : extension,
          );
          const second = yield* openChat(storage, chat, { extensions: gone });
          assert.notInclude(yield* offered(second.root), "github_get_issue");
          yield* second.harness.close;
          const { harness, root } = yield* openChat(storage, chat, { extensions: setup() });
          assert.include(yield* offered(root), "github_get_issue");
          // With an explicit array, a load appends to it as before.
          yield* root.configure({ modelTools: [alpha, ToolSearch.tool] });
          chat.faux.setResponses([call("tool_search", { query: "repositories", limit: 1 }), DONE]);
          yield* ask(root, "more");
          assert.deepStrictEqual(yield* modelTools(harness, root), [
            "alpha",
            "tool_search",
            "github_list_repos",
          ]);
          yield* harness.close;
        }),
      ),
  );

  it.effect("keeps loaded tools per branch and in the baseline after a reset", () =>
    run(backend)(
      Effect.gen(function* () {
        const chat = chatSetup();
        chat.faux.setResponses([call("tool_search", { query: "github issue", limit: 1 }), DONE]);
        const { harness, root } = yield* openChat(yield* controlledStorage, chat, {
          extensions: setup(),
        });
        yield* ask(root, "go");
        const entries = yield* allEntries(root);
        const before = yield* root.fork(entries[0]!.id, ownerless);
        const after = yield* root.fork(entries.at(-1)!.id, ownerless);
        assert.notInclude(yield* offered(before), "github_get_issue");
        assert.include(yield* offered(after), "github_get_issue");
        yield* root.reset(undefined);
        chat.faux.setResponses([DONE]);
        yield* ask(root, "fresh");
        const baseline = systemMessages(yield* allEntries(root)).at(-1)!;
        assert.deepStrictEqual(
          baseline.toolsAdded!.map((each) => each.name),
          ["alpha", "omega", "tool_search", "github_get_issue"],
        );
        yield* harness.close;
      }),
    ),
  );

  it.effect("finds a deferred tool by a field of a nested schema class", () =>
    run(backend)(
      Effect.gen(function* () {
        const chat = chatSetup();
        const triage = AgentTool.make("issue_triage", {
          description: "Sort issues",
          parameters: Schema.Struct({ filter: IssueFilter }),
          deferred: true,
          execute: () => Effect.succeed(AgentTool.text("triaged")),
        });
        chat.faux.setResponses([call("tool_search", { query: "sprint" }), DONE]);
        const { harness, root } = yield* openChat(yield* controlledStorage, chat, {
          extensions: [...setup(), toolExtension(triage)],
        });
        yield* ask(root, "go");
        assert.deepStrictEqual(toolResultTexts(yield* allEntries(root)), [
          "Loaded 1 tool. They are available from your next call:\n- issue_triage: Sort issues",
        ]);
        yield* harness.close;
      }),
    ),
  );

  it.effect("finds nothing for an unrelated query and loads nothing", () =>
    run(backend)(
      Effect.gen(function* () {
        const chat = chatSetup();
        chat.faux.setResponses([call("tool_search", { query: "weather forecast" }), DONE]);
        const { harness, root } = yield* openChat(yield* controlledStorage, chat, {
          extensions: setup(),
        });
        yield* ask(root, "go");
        assert.deepStrictEqual(toolResultTexts(yield* allEntries(root)), [
          "No matching tools found.",
        ]);
        assert.isUndefined(yield* modelTools(harness, root));
        yield* harness.close;
      }),
    ),
  );
});

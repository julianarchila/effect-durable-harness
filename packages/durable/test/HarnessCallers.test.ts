// Who may call a tool (`callers`), the model or other tools through `ToolContext.executeTool`, on every backend.
//
// Tools are extensions of `openChat`, one per tool, named `tool:<name>`; a fresh storage is a fresh storage of the
// backend; waiting for a submission advances the `TestClock` while it waits.
//
// "keeps a deferred tool only tools may call out of the deferred list" checks that the deferred list is drawn
// only from the tools the model may call.
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import { AgentDoc } from "../src/Agent.ts";
import * as AgentTool from "../src/AgentTool.ts";
import type { NestedToolExecutionResult, ToolExecutionResult } from "../src/AgentTool.ts";
import * as Extension from "../src/Extension.ts";
import type { Conversation } from "../src/Harness.ts";
import type { AssistantMessage } from "../src/Message.ts";
import type { Submission } from "../src/Submission.ts";
import { fauxAssistantMessage, fauxText, fauxToolCall } from "../src/testing/FauxModel.ts";
import { allEntries, type ChatSetup, chatSetup, openChat, waitFor } from "./ChatSupport.ts";
import { type Backend, controlledStorage, describeEachBackend, run } from "./SessionSupport.ts";

const Parameters = Schema.Struct({});
type Extra = Omit<
  AgentTool.MakeOptions<typeof Parameters.Type, Schema.Json, never, never>,
  "description" | "parameters" | "execute"
>;

/** A tool that answers `<name> ran`. */
const tool = (
  name: string,
  extra: Extra = {},
  execute: () => Effect.Effect<ToolExecutionResult> = () =>
    Effect.succeed({ output: [{ type: "text", text: `${name} ran` }] }),
) => AgentTool.make(name, { description: name, parameters: Parameters, execute, ...extra });

const DONE = fauxAssistantMessage([fauxText("done")]);

const call = (name: string, id = "c1"): AssistantMessage =>
  fauxAssistantMessage([fauxToolCall(name, {}, { id })], { stopReason: "toolUse" });

/** A tool that calls each of `names` as a nested call and records what it got back. */
const prober = (names: ReadonlyArray<string>) => {
  const received = new Map<string, NestedToolExecutionResult>();
  const registration = AgentTool.make("probe", {
    description: "probe",
    parameters: Parameters,
    execute: (_args, api) =>
      Effect.gen(function* () {
        for (const name of names) received.set(name, yield* api.executeTool(name, {}));
        return {};
      }).pipe(Effect.orDie),
  });
  return { registration, received };
};

const extensionsOf = (tools: ReadonlyArray<AgentTool.Any>) =>
  tools.map((each) => Extension.make({ name: `tool:${each.name}`, tools: [each] }));

const offered = (root: Conversation) =>
  Effect.map(root.agent, (agent) => agent.tools.map((each) => each.name));

const callable = (root: Conversation) =>
  Effect.map(root.agent, (agent) => agent.callable.map((each) => each.name));

/** A fresh storage of the backend, alive for the test's scope. */
const freshStorage = (backend: Backend) =>
  Effect.gen(function* () {
    const context = yield* Layer.buildWithMemoMap(
      backend.layer,
      yield* Layer.makeMemoMap,
      yield* Effect.scope,
    );
    return yield* controlledStorage.pipe(Effect.provideContext(context));
  });

/** Waits for a submission to settle. */
const settle = (submission: Submission) =>
  Effect.gen(function* () {
    const fiber = yield* Effect.forkChild(submission.await, { startImmediately: true });
    yield* waitFor(Effect.sync(() => fiber.pollUnsafe() !== undefined));
    return yield* Fiber.join(fiber);
  });

const open = (backend: Backend, setup: ChatSetup, tools: ReadonlyArray<AgentTool.Any>) =>
  Effect.gen(function* () {
    const storage = yield* freshStorage(backend);
    return yield* openChat(storage, setup, { extensions: extensionsOf(tools) });
  });

const go = (root: Conversation) =>
  root.submit({ type: "input", content: "go" }).pipe(Effect.flatMap(settle));

describeEachBackend("who may call a tool", (backend) => {
  it.effect(
    "offers the model only tools it may call, and lets tools call only tools they may call",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const script = tool("script", { callers: ["model"] });
          const hidden = tool("hidden", { callers: ["tools"] });
          const both = tool("both");
          const probe = prober(["script", "hidden", "both"]);
          const setup = chatSetup();
          setup.faux.setResponses([call("probe"), DONE]);
          const { harness, root } = yield* open(backend, setup, [
            script,
            hidden,
            both,
            probe.registration,
          ]);
          assert.deepStrictEqual(yield* offered(root), ["script", "both", "probe"]);
          assert.deepStrictEqual(yield* callable(root), ["hidden", "both", "probe"]);
          yield* go(root);
          assert.strictEqual(
            probe.received.get("script")?.diagnostics[0]?.code,
            "tool_unavailable",
          );
          assert.include(probe.received.get("hidden"), {
            isError: false,
            structuredOutput: "hidden ran",
          });
          assert.include(probe.received.get("both"), {
            isError: false,
            structuredOutput: "both ran",
          });
          yield* harness.close;
        }),
      ),
  );

  it.effect(
    "narrows what the model is offered with modelTools, leaving the rest callable by tools",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const read = tool("read");
          const bash = tool("bash");
          const probe = prober(["read", "bash"]);
          const setup = chatSetup();
          setup.faux.setResponses([call("probe"), DONE]);
          const { harness, root } = yield* open(backend, setup, [read, bash, probe.registration]);
          yield* root.configure({ modelTools: [probe.registration] });
          assert.deepStrictEqual(yield* offered(root), ["probe"]);
          assert.deepStrictEqual(yield* callable(root), ["read", "bash", "probe"]);
          yield* go(root);
          assert.strictEqual(probe.received.get("read")?.isError, false);
          assert.strictEqual(probe.received.get("bash")?.isError, false);

          yield* root.configure({ modelTools: { remove: [bash] } });
          assert.deepStrictEqual(yield* offered(root), ["read", "probe"]);
          const agent = Option.getOrUndefined(yield* harness.session.snapshot(AgentDoc, root.id));
          assert.deepStrictEqual(agent?.modelTools, { remove: ["bash"] });
          yield* root.configure({ modelTools: null });
          assert.deepStrictEqual(yield* offered(root), ["read", "bash", "probe"]);
          yield* harness.close;
        }),
      ),
  );

  it.effect("never offers a tool that is not enabled, nor lets tools call it", () =>
    run(backend)(
      Effect.gen(function* () {
        const read = tool("read");
        const bash = tool("bash");
        const probe = prober(["bash"]);
        const setup = chatSetup();
        setup.faux.setResponses([call("probe"), DONE]);
        const { harness, root } = yield* open(backend, setup, [read, bash, probe.registration]);
        // modelTools cannot bring back what tools disabled.
        yield* root.configure({
          tools: { remove: [bash] },
          modelTools: [bash, probe.registration],
        });
        assert.deepStrictEqual(yield* offered(root), ["probe"]);
        assert.deepStrictEqual(yield* callable(root), ["read", "probe"]);
        yield* go(root);
        assert.strictEqual(probe.received.get("bash")?.diagnostics[0]?.code, "tool_unavailable");
        yield* harness.close;
      }),
    ),
  );

  it.effect("answers a model-issued call of a tool it was not offered as unavailable", () =>
    run(backend)(
      Effect.gen(function* () {
        const read = tool("read");
        const bash = tool("bash");
        const setup = chatSetup();
        setup.faux.setResponses([call("bash"), DONE]);
        const { harness, root } = yield* open(backend, setup, [read, bash]);
        yield* root.configure({ modelTools: [read] });
        yield* go(root);
        const result = (yield* allEntries(root)).find((entry) => entry.kind === "core.tool-result");
        const message = result?.model?.[0];
        const texts =
          message?.role === "toolResult"
            ? message.content.map((item) => (item.type === "text" ? item.text : ""))
            : [];
        assert.include(texts.join("\n"), "Tool bash is not available");
        yield* harness.close;
      }),
    ),
  );

  it.effect("keeps a deferred tool only tools may call out of the deferred list", () =>
    run(backend)(
      Effect.gen(function* () {
        const hidden = tool("hidden", { callers: ["tools"], deferred: true });
        const later = tool("later", { deferred: true });
        const setup = chatSetup();
        const { harness, root } = yield* open(backend, setup, [hidden, later]);
        const agent = yield* root.agent;
        assert.deepStrictEqual(
          agent.deferred.map((each) => each.name),
          ["later"],
        );
        assert.deepStrictEqual(yield* callable(root), ["hidden", "later"]);
        yield* harness.close;
      }),
    ),
  );

  it.effect("applies addTools to modelTools too, so the model is offered the added tools", () =>
    run(backend)(
      Effect.gen(function* () {
        const extra = tool("extra");
        const grow = tool("grow", {}, () =>
          Effect.succeed({ output: [], control: { addTools: ["extra"] } }),
        );
        const setup = chatSetup();
        setup.faux.setResponses([call("grow"), DONE]);
        const { harness, root } = yield* open(backend, setup, [extra, grow]);
        yield* root.configure({ modelTools: [grow] });
        yield* go(root);
        const agent = Option.getOrUndefined(yield* harness.session.snapshot(AgentDoc, root.id));
        assert.deepStrictEqual(agent?.modelTools, ["grow", "extra"]);
        assert.deepStrictEqual(yield* offered(root), ["grow", "extra"]);
        yield* harness.close;
      }),
    ),
  );
});

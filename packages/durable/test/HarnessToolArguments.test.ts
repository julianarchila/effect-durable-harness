// Coercion of a call's arguments in a run, on every backend: what `execute` receives, what the intent stores and
// reruns with, the JSON Schema that drives it, and the error text of arguments that still do not decode.
//
// A reopen is a new Harness over the same storage of the backend; the model is `FauxModel`; `waitFor` advances the
// `TestClock`.
import { assert, it } from "@effect/vitest";
import * as Clock from "effect/Clock";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as AgentTool from "../src/AgentTool.ts";
import { ToolResultEntry } from "../src/Entry.ts";
import * as Extension from "../src/Extension.ts";
import type { Conversation } from "../src/Harness.ts";
import type { SubmissionId, TaskId } from "../src/Ids.ts";
import { LiveDoc } from "../src/Live.ts";
import type { AssistantMessage, ToolResultMessage } from "../src/Message.ts";
import type { EntryRecord } from "../src/Records.ts";
import type { Submission } from "../src/Submission.ts";
import { fauxAssistantMessage, fauxText, fauxToolCall } from "../src/testing/FauxModel.ts";
import { allEntries, type ChatSetup, chatSetup, openChat, waitFor } from "./ChatSupport.ts";
import type { TestHarness } from "./HarnessSupport.ts";
import {
  type ControlledStorage,
  controlledStorage,
  describeEachBackend,
  run,
} from "./SessionSupport.ts";

/** Installs `agentTool` as an extension of its own. */
const toolExtension = (agentTool: AgentTool.Any) =>
  Extension.make({ name: `tool:${agentTool.name}`, tools: [agentTool] });

/** A tool-calling answer with one call per `[name, args, id]`. */
const calls = (
  ...list: ReadonlyArray<readonly [string, Schema.JsonObject, string]>
): AssistantMessage =>
  fauxAssistantMessage(
    list.map(([name, args, id]) => fauxToolCall(name, args, { id })),
    { stopReason: "toolUse" },
  );

const DONE = fauxAssistantMessage([fauxText("done")]);

const results = (entries: ReadonlyArray<EntryRecord>): Array<ToolResultMessage> =>
  entries.flatMap((entry) => {
    const message = entry.model?.[0];
    return ToolResultEntry.is(entry) && message?.role === "toolResult" ? [message] : [];
  });

const text = (message: ToolResultMessage | undefined): string =>
  (message?.content ?? []).map((item) => (item.type === "text" ? item.text : "")).join("|");

/** Results by call ID, as `[isError, text]`. */
const byId = (entries: ReadonlyArray<EntryRecord>) =>
  new Map(results(entries).map((result) => [result.toolCallId, [result.isError, text(result)]]));

/** A Harness over `storage` that schedules at once. */
const open = (
  storage: ControlledStorage,
  setup: ChatSetup,
  extensions: ReadonlyArray<Extension.Any>,
) =>
  Effect.gen(function* () {
    const opened = yield* openChat(storage, setup, { extensions });
    yield* opened.harness.harness.resume;
    return opened;
  });

/** Awaits the submission while the `TestClock` advances. */
const settle = (submission: Submission) =>
  Effect.gen(function* () {
    const fiber = yield* Effect.forkChild(submission.await, { startImmediately: true });
    yield* waitFor(Effect.sync(() => fiber.pollUnsafe() !== undefined));
    return yield* Fiber.join(fiber);
  });

/** Awaits a submission found by ID, as `settle` does. */
const settleById = (h: TestHarness, id: SubmissionId) =>
  Effect.flatMap(h.harness.submission(id), (found) => settle(Option.getOrThrow(found)));

/** The first slot's task, once it has one. */
const toolTaskId = (h: TestHarness, root: Conversation) =>
  Effect.gen(function* () {
    let id: TaskId | undefined;
    yield* waitFor(
      Effect.map(h.session.snapshot(LiveDoc, root.id), (value) => {
        id = Option.getOrUndefined(value)?.tools?.[0]?.taskId;
        return id !== undefined;
      }),
    );
    return id!;
  });

/** Runs one submission over a fresh storage with `tools`, and returns its entries. */
const runCalls = (
  setup: ChatSetup,
  tools: ReadonlyArray<AgentTool.Any>,
  answer: AssistantMessage,
) =>
  Effect.gen(function* () {
    setup.faux.setResponses([answer, DONE]);
    const { harness, root } = yield* open(
      yield* controlledStorage,
      setup,
      tools.map(toolExtension),
    );
    const submission = yield* root.submit({ type: "input", content: "go" });
    assert.strictEqual((yield* settle(submission)).status, "done");
    const entries = yield* allEntries(root);
    yield* harness.close;
    return entries;
  });

const Counted = Schema.Struct({ count: Schema.Number });

describeEachBackend("tool argument coercion", (backend) => {
  it.effect(
    "drops a null the model sends for a Schema.optional property, also nested, and keeps one optional(NullOr) accepts",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const setup = chatSetup();
          const seen: Array<Schema.Json> = [];
          const parameters = Schema.Struct({
            n: Schema.optional(Schema.Number),
            o: Schema.optional(Schema.Struct({ a: Schema.optional(Schema.String) })),
            k: Schema.optional(Schema.NullOr(Schema.String)),
          });
          const probe = AgentTool.make("probe", {
            description: "probe",
            parameters,
            execute: (args) =>
              Effect.sync(() => {
                seen.push(Schema.encodeSync(Schema.toCodecJson(parameters))(args));
                return {};
              }),
          });
          const entries = yield* runCalls(
            setup,
            [probe],
            calls(
              ["probe", { n: null }, "top"],
              ["probe", { o: { a: null } }, "nested"],
              ["probe", { k: null, n: "4" }, "nullable"],
            ),
          );
          const result = byId(entries);
          assert.deepStrictEqual(result.get("top"), [false, ""]);
          assert.deepStrictEqual(result.get("nested"), [false, ""]);
          assert.deepStrictEqual(result.get("nullable"), [false, ""]);
          assert.sameDeepMembers(seen, [{}, { o: {} }, { k: null, n: 4 }]);
        }),
      ),
  );

  it.effect("reports arguments that still do not decode as the call sent them", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        const probe = AgentTool.make("probe", {
          description: "probe",
          parameters: Schema.Struct({ count: Schema.Number, label: Schema.Boolean }),
          execute: () => Effect.succeed({}),
        });
        const entries = yield* runCalls(
          setup,
          [probe],
          calls(["probe", { count: "7", label: "maybe" }, "bad"]),
        );
        const [isError, message] = byId(entries).get("bad")!;
        assert.strictEqual(isError, true);
        assert.include(message, "Validation failed");
        // The coerced copy holds `count: 7`; the message shows what the model sent.
        assert.include(message, '"count": "7"');
        assert.include(message, '"label": "maybe"');
      }),
    ),
  );

  it.effect(
    "coerces by a tool's own jsonSchema rather than the one generated from parameters",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const setup = chatSetup();
          const seen: Array<number | string> = [];
          const parameters = Schema.Struct({ count: Schema.Union([Schema.Number, Schema.String]) });
          const execute = (args: typeof parameters.Type) =>
            Effect.sync(() => {
              seen.push(args.count);
              return {};
            });
          const own = AgentTool.make("own", {
            description: "own",
            parameters,
            jsonSchema: {
              type: "object",
              properties: { count: { type: "integer" } },
              required: ["count"],
            },
            execute,
          });
          const generated = AgentTool.make("generated", {
            description: "generated",
            parameters,
            execute,
          });
          yield* runCalls(
            setup,
            [own, generated],
            calls(["own", { count: "5" }, "own"], ["generated", { count: "5" }, "generated"]),
          );
          // The generated schema accepts the string, so it stays; the tool's own schema says integer.
          assert.sameMembers(seen, [5, "5"]);
        }),
      ),
  );

  it.effect("stores the coerced arguments with the intent, and recovery reruns with them", () =>
    run(backend)(
      Effect.gen(function* () {
        const storage = yield* controlledStorage;
        const setup = chatSetup();
        const started = Deferred.makeUnsafe<void>();
        const seen: Array<typeof Counted.Type> = [];
        const work = AgentTool.make("work", {
          description: "work",
          parameters: Counted,
          replay: "safe",
          execute: (args) =>
            Effect.gen(function* () {
              seen.push(args);
              if (seen.length > 1) return {};
              yield* Deferred.succeed(started, undefined);
              return yield* Effect.never;
            }),
        });
        setup.faux.setResponses([calls(["work", { count: "3" }, "c1"]), DONE]);
        let opened = yield* open(storage, setup, [toolExtension(work)]);
        const id = (yield* opened.root.submit({ type: "input", content: "go" })).id;
        yield* waitFor(Deferred.isDone(started));
        const taskId = yield* toolTaskId(opened.harness, opened.root);
        yield* opened.harness.close;

        opened = yield* open(storage, setup, [toolExtension(work)]);
        const task = Option.getOrThrow(yield* opened.harness.harness.getTask(taskId));
        assert.deepStrictEqual(task.state.status === "pending" && task.state.checkpoint, {
          phase: "execute",
          arguments: { count: 3 },
          replay: "safe",
        });
        assert.strictEqual((yield* settleById(opened.harness, id)).status, "done");
        assert.deepStrictEqual(seen, [{ count: 3 }, { count: 3 }]);
        yield* opened.harness.close;
      }),
    ),
  );

  it.effect("runs a woken call with the coerced arguments its park stored", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        const seen: Array<typeof Counted.Type> = [];
        const poll = AgentTool.make("poll", {
          description: "poll",
          parameters: Counted,
          execute: (args, api) =>
            Effect.gen(function* () {
              seen.push(args);
              if (api.wake !== undefined) return AgentTool.text(`count ${args.count}`);
              return AgentTool.sleep({ until: (yield* Clock.currentTimeMillis) + 100 });
            }),
        });
        const entries = yield* runCalls(setup, [poll], calls(["poll", { count: "8" }, "c1"]));
        assert.deepStrictEqual(seen, [{ count: 8 }, { count: 8 }]);
        assert.deepStrictEqual(byId(entries).get("c1"), [false, "count 8"]);
      }),
    ),
  );
});

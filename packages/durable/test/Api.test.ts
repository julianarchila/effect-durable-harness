// Tests of the declaration API: declarations made as `make(identifier, options)`, tool handlers that get the call,
// tool `details` and result helpers, the call's `key`, `step` and `requireConversation`, transitions as values
// and as runtime commits, operations on declarations (`create`, `get`, `read`, `require`, `changes`, `draft`,
// `append`), the `Session` from a handler, `Harness.make` with `defaults`, and the missing-handler error.
import { assert, describe, it } from "@effect/vitest";
import * as Cause from "effect/Cause";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as AgentTool from "../src/AgentTool.ts";
import type { AgentStorage, StorageFailure } from "../src/AgentStorage.ts";
import * as Doc from "../src/Doc.ts";
import * as Entry from "../src/Entry.ts";
import * as Extension from "../src/Extension.ts";
import * as Harness from "../src/Harness.ts";
import { ConversationId, type TaskId } from "../src/Ids.ts";
import * as MemoryAgentStorage from "../src/MemoryAgentStorage.ts";
import type { Models } from "../src/Models.ts";
import type { EntryRecord } from "../src/Records.ts";
import { Session } from "../src/Session.ts";
import type { SessionFailed } from "../src/SessionError.ts";
import * as Section from "../src/Section.ts";
import * as Task from "../src/Task.ts";
import { Tx } from "../src/Tx.ts";
import * as FauxModel from "../src/testing/FauxModel.ts";
import {
  fauxAssistantMessage,
  fauxText,
  fauxToolCall,
  make as makeFaux,
} from "../src/testing/FauxModel.ts";
import { eventually, openHarness } from "./HarnessSupport.ts";
import { controlledStorage, describeEachBackend, flush, run } from "./SessionSupport.ts";

const FAUX_MODEL = { provider: "faux", modelId: "faux-1" } as const;
const OTHER_MODEL = { provider: "faux", modelId: "faux-2" } as const;

/** A domain service a handler resolves when its Layer is built. */
class Greeter extends Context.Service<Greeter, { readonly greet: (name: string) => string }>()(
  "@effect-durable-harness/core/test/Api.test/Greeter",
) {}
const GreeterLive = Layer.succeed(Greeter, { greet: (name: string) => `hello ${name}` });

// ── declarations, as `make(identifier, options)` ───────────────────────────────

const Lead = Doc.make("test.d22.lead", {
  checkpointWhen: () => false,
  version: 1,
  scope: "conversation",
  history: "latest",
  fork: "initial",
  schema: Schema.Struct({ mode: Schema.String }),
  initial: () => ({ mode: "auto" }),
});

const Counter = Doc.make("test.d22.counter", {
  checkpointWhen: () => false,
  version: 1,
  scope: "session",
  schema: Schema.Struct({ count: Schema.Number }),
  initial: () => ({ count: 0 }),
});

const Approvals = Doc.family("test.d22.approvals", {
  checkpointWhen: () => false,
  version: 1,
  scope: "conversation",
  history: "latest",
  fork: "initial",
  schema: Schema.Struct({ approved: Schema.Boolean, by: Schema.String }),
  initial: (by: string) => ({ approved: false, by }),
});

const Note = Entry.make("test.d22.note", { data: Schema.Struct({ text: Schema.String }) });
const Marker = Entry.make("test.d22.marker");

const Weather = AgentTool.make("weather", {
  description: "The weather in a place.",
  parameters: Schema.Struct({ location: Schema.String }),
  details: Schema.Struct({ celsius: Schema.Number }),
  replay: "safe",
});

const Greet = AgentTool.make("greet", {
  description: "Greets someone.",
  parameters: Schema.Struct({ name: Schema.String }),
});

const Charge = AgentTool.make("charge", {
  description: "Charges once.",
  parameters: Schema.Struct({}),
});

/** Implemented inline, with the call as its second argument. */
const Echo = AgentTool.make("echo", {
  description: "Echoes its text.",
  parameters: Schema.Struct({ text: Schema.String }),
  execute: (args, call) => Effect.succeed(AgentTool.json({ text: args.text, callId: call.callId })),
});

const Child = Task.make("test.d22.child", {
  version: 1,
  input: Schema.Null,
  checkpoint: Schema.Struct({ phase: Schema.Literal("run") }),
  result: Schema.Null,
  initial: () => ({ phase: "run" }),
  phases: { run: (_task, run) => run.complete(null) },
  abort: (_task, run) => run.aborted,
});

const FlowState = Schema.Union([
  Schema.Struct({ phase: Schema.Literal("start") }),
  Schema.Struct({ phase: Schema.Literal("spawn") }),
  Schema.Struct({ phase: Schema.Literal("finish") }),
]);

const Flow = Task.make("test.d22.flow", {
  version: 1,
  input: Schema.Struct({ label: Schema.String }),
  checkpoint: FlowState,
  result: Schema.String,
  initial: () => ({ phase: "start" }),
});

const Failing = Task.make("test.d22.failing", {
  version: 1,
  input: Schema.Null,
  checkpoint: Schema.Struct({ phase: Schema.Literal("run") }),
  result: Schema.Null,
  initial: () => ({ phase: "run" }),
  phases: { run: (_task, run) => run.fail("nope") },
  abort: (_task, run) => run.aborted,
});

const Blocked = Task.make("test.d22.blocked", {
  version: 1,
  input: Schema.Null,
  checkpoint: Schema.Struct({ phase: Schema.Literal("run") }),
  result: Schema.Null,
  initial: () => ({ phase: "run" }),
  phases: { run: () => Effect.never },
  abort: (_task, run) => run.aborted,
});

const Chat = Extension.make("chat", {
  tools: [Weather, Greet, Charge, Echo],
  tasks: [Child, Flow, Failing, Blocked],
});

const ChatHarness = Harness.make("chat", { extensions: [Chat] });

// ── handlers ────────────────────────────────────────────────────────────────────

interface Seen {
  readonly calls: Array<{ readonly callId: string; readonly key: string; readonly taskId: TaskId }>;
  readonly sessions: Array<Session["Service"]>;
  readonly leads: Array<Option.Option<{ readonly mode: string }>>;
  readonly missing: Array<string>;
  charges: number;
  concurrent: number;
}

const seen: Seen = { calls: [], sessions: [], leads: [], missing: [], charges: 0, concurrent: 0 };

const WeatherLive = Weather.toLayer((args, call) =>
  Effect.gen(function* () {
    seen.calls.push({ callId: call.callId, key: call.key("forecast"), taskId: call.taskId });
    // The session, per call: a handler Layer is built once and captures none.
    seen.sessions.push(yield* Session);
    seen.leads.push(yield* Lead.read(call.conversationId));
    return AgentTool.text(`${args.location}: 21`, { details: { celsius: 21 } });
  }),
);

const GreetLive = Greet.toLayer(
  Effect.gen(function* () {
    const greeter = yield* Greeter;
    return (args, call) =>
      Effect.succeed(AgentTool.text(`${greeter.greet(args.name)} (${call.callId})`));
  }),
);

const Receipt = Schema.Struct({ id: Schema.String });

const ChargeLive = Charge.toLayer((_args, call) =>
  Effect.gen(function* () {
    const charge = Effect.sync(() => {
      seen.charges++;
      return { id: call.key("charge") };
    });
    const first = yield* call.step("charge", Receipt, charge);
    // A second run of the same step returns the recorded result without running.
    const again = yield* call.step("charge", Receipt, Effect.die("ran twice"));
    assert.deepStrictEqual(again, first);
    // Two concurrent steps of one name run their effect once and agree.
    const slow = Effect.yieldNow.pipe(
      Effect.andThen(
        Effect.sync(() => {
          seen.concurrent++;
          return { id: `concurrent-${seen.concurrent}` };
        }),
      ),
    );
    const [left, right] = yield* Effect.all(
      [call.step("concurrent", Receipt, slow), call.step("concurrent", Receipt, slow)],
      { concurrency: 2 },
    );
    assert.deepStrictEqual(left, right);
    const missing = yield* Effect.flip(call.requireConversation(ConversationId.make(9999)));
    seen.missing.push(missing._tag);
    return AgentTool.text(first.id);
  }),
);

const FlowLive = Flow.toLayer({
  phases: {
    // A state chosen inside the commit, from what the commit reads.
    start: (task, run) =>
      run.commit(
        Effect.gen(function* () {
          const counter = yield* Counter.get();
          counter.count += 1;
          return task.input.label === "short"
            ? Task.complete("short")
            : Task.next({ phase: "spawn" });
        }),
      ),
    spawn: (task, run) =>
      Effect.gen(function* () {
        const handle = yield* run.requireConversation(task.conversationId);
        assert.strictEqual(handle.id, task.conversationId);
        let child: TaskId | undefined;
        yield* run.commit(
          Effect.map(Child.create(null, { ownership: { kind: "task", taskId: task.id } }), (id) => {
            child = id;
          }),
        );
        yield* run.waitFor(
          [child!],
          { checkpoint: { phase: "finish" }, policy: "allSettled" },
          Note.append(task.conversationId, { text: "waiting" }),
        );
      }),
    finish: (task, run) =>
      run.complete(task.input.label, Note.append(task.conversationId, { text: "finished" })),
  },
  abort: (_task, run) => run.aborted,
});

const ChatLive = Layer.mergeAll(WeatherLive, GreetLive, ChargeLive, FlowLive).pipe(
  Layer.provide(GreeterLive),
);

/** The agent in memory, whose top-level conversations start with the fake model. */
const agent = (models: Layer.Layer<Models>) =>
  ChatHarness.layer({ defaults: { agent: { model: FAUX_MODEL } } }).pipe(
    Layer.provide(ChatLive),
    Layer.provide(Layer.mergeAll(MemoryAgentStorage.layer, models)),
  );

const withAgent = <A, E>(
  body: Effect.Effect<A, E, Harness.Harness | Session>,
  models: Layer.Layer<Models> = makeFaux().layer,
) =>
  Effect.gen(function* () {
    const harness = yield* Harness.Harness;
    yield* harness.resume;
    return yield* body;
  }).pipe(Effect.provide(agent(models)));

/** Type-level: a handler typed by its declaration, with details its schema accepts. */
const typedHandler: AgentTool.Execute<typeof Weather> = () =>
  Effect.succeed(AgentTool.text("x", { details: { celsius: 1 } }));
const wronglyTypedHandler: AgentTool.Execute<typeof Weather> = () =>
  // @ts-expect-error: details are typed by the tool's schema
  Effect.succeed(AgentTool.text("x", { details: { celsius: "hot" } }));

/** Builds a Harness over `extension` without its handlers, and returns how the build ended. */
const buildWithoutHandlers = (extension: Extension.Any) =>
  Effect.exit(
    Layer.build(
      // SAFETY: the cast drops the handlers the types require, as a consumer could.
      (
        Harness.layer({ extensions: [extension] }) as Layer.Layer<
          Harness.Harness | Session,
          StorageFailure | SessionFailed,
          AgentStorage | Models
        >
      ).pipe(Layer.provide(Layer.mergeAll(MemoryAgentStorage.layer, makeFaux().layer))),
    ).pipe(Effect.scoped),
  );

const toolResults = (entries: ReadonlyArray<EntryRecord>) =>
  entries.flatMap((entry) =>
    (entry.model ?? []).filter((message) => message.role === "toolResult"),
  );

describe("declarations", () => {
  it("make(identifier, options) builds the same definitions as the object forms", () => {
    assert.strictEqual(Lead.definition.kind, "test.d22.lead");
    assert.strictEqual(Lead.definition.scope, "conversation");
    assert.strictEqual(Approvals.definition.kind, "test.d22.approvals");
    assert.strictEqual(Approvals.definition.family, true);
    assert.strictEqual(Flow.definition.name, "test.d22.flow");
    assert.strictEqual(Child.definition.name, "test.d22.child");
    assert.isDefined(Child.definition.phases);
    assert.strictEqual(Chat.name, "chat");
    assert.deepStrictEqual(
      Chat.tools.map((tool) => tool.name),
      ["weather", "greet", "charge", "echo"],
    );
    assert.strictEqual(ChatHarness.name, "chat");
    assert.deepStrictEqual(ChatHarness.extensions, [Chat]);
    // The object forms still work.
    const Old = Extension.make({ name: "old", tasks: [Child] });
    assert.strictEqual(Old.name, "old");
    assert.throws(() => Task.make("test.d22.bad", { ...Flow.definition, version: 0 }));
  });

  it("a tool's details schema is on its token, and text and json build results", () => {
    assert.strictEqual(Weather.details, Weather.definition.details);
    assert.isDefined(Weather.details);
    assert.deepStrictEqual(AgentTool.text("hi"), { output: [{ type: "text", text: "hi" }] });
    assert.deepStrictEqual(AgentTool.text("hi", { details: { celsius: 3 } }), {
      output: [{ type: "text", text: "hi" }],
      details: { celsius: 3 },
    });
    assert.deepStrictEqual(AgentTool.json({ a: [1, null] }), {
      output: [{ type: "text", text: '{"a":[1,null]}' }],
    });
  });

  it("transitions are plain state objects", () => {
    assert.deepStrictEqual(Task.next({ phase: "spawn" }), {
      status: "running",
      checkpoint: { phase: "spawn" },
    });
    // SAFETY: an ID for a value only; no task is read.
    const on = [7 as TaskId];
    assert.deepStrictEqual(
      Task.waitFor(on, { checkpoint: { phase: "finish" }, policy: "failFast" }),
      {
        status: "waiting",
        checkpoint: { phase: "finish" },
        on,
        policy: "failFast",
      },
    );
    assert.deepStrictEqual(Task.complete("done"), {
      status: "terminal",
      outcome: { status: "completed", result: "done" },
    });
    assert.deepStrictEqual(Task.fail("nope"), {
      status: "terminal",
      outcome: { status: "failed", error: { message: "nope" } },
    });
    assert.deepStrictEqual(Task.aborted, { status: "terminal", outcome: { status: "aborted" } });
  });

  it("entry kinds draft entries", () => {
    assert.deepStrictEqual(Note.draft({ text: "hi" }), {
      kind: "test.d22.note",
      data: { text: "hi" },
    });
    const model = [{ role: "user", content: "hi", timestamp: 0 } as const];
    assert.deepStrictEqual(Note.draft({ text: "hi" }, { model }), {
      kind: "test.d22.note",
      data: { text: "hi" },
      model,
    });
    assert.deepStrictEqual(Marker.draft(), { kind: "test.d22.marker" });
    assert.deepStrictEqual(Marker.draft({ head: "self" }), {
      kind: "test.d22.marker",
      head: "self",
    });
  });

  it("handler Layers require neither the ToolContext nor the Session", () => {
    // Type-level: the assignments fail to compile if `Session` stays in a Layer's requirements.
    const tool: Layer.Layer<AgentTool.Handler<"weather">> = WeatherLive;
    const task: Layer.Layer<Task.Handler<"test.d22.flow">> = Flow.toLayer({
      phases: {
        start: () => Effect.asVoid(Effect.service(Session)),
        spawn: () => Effect.void,
        finish: () => Effect.void,
      },
      abort: () => Effect.void,
    });
    assert.isDefined([tool, task, typedHandler, wronglyTypedHandler]);
  });
});

describe("the agent", () => {
  it.live("tools get the call: key, step, requireConversation and the Session, per call", () =>
    Effect.gen(function* () {
      seen.calls.length = 0;
      seen.sessions.length = 0;
      seen.leads.length = 0;
      seen.missing.length = 0;
      seen.charges = 0;
      seen.concurrent = 0;
      const faux = makeFaux();
      faux.setResponses([
        fauxAssistantMessage(
          [
            fauxToolCall("weather", { location: "Lisbon" }, { id: "c1" }),
            fauxToolCall("greet", { name: "ada" }, { id: "c2" }),
            fauxToolCall("charge", {}, { id: "c3" }),
            fauxToolCall("echo", { text: "hi" }, { id: "c4" }),
          ],
          { stopReason: "toolUse" },
        ),
        fauxAssistantMessage([fauxText("done")]),
      ]);
      const results = yield* withAgent(
        Effect.gen(function* () {
          const harness = yield* Harness.Harness;
          // No agent: the Harness's default one answers.
          const root = yield* harness.ensureRoot();
          yield* root.commit(Effect.map(Lead.get(root.id), (lead) => void (lead.mode = "off")));
          const submission = yield* root.submit({ type: "input", content: "go" });
          yield* submission.await.pipe(Effect.timeout("5 seconds"));
          assert.strictEqual(seen.sessions[0], yield* Session);
          return toolResults(yield* Stream.runCollect(root.entries({ order: "ascending" })));
        }),
        faux.layer,
      );
      // By call: results land in the order the calls finish.
      const textOf = (callId: string) => {
        const message = results.find(
          (result) => result.role === "toolResult" && result.toolCallId === callId,
        );
        return message?.role === "toolResult" && message.content[0]?.type === "text"
          ? message.content[0].text
          : "";
      };
      const texts = ["c1", "c2", "c3", "c4"].map(textOf);
      const [weather] = seen.calls;
      assert.isDefined(weather);
      assert.strictEqual(weather.callId, "c1");
      assert.strictEqual(weather.key, `${weather.taskId}:forecast`);
      assert.deepStrictEqual(seen.leads, [Option.some({ mode: "off" })]);
      assert.strictEqual(texts[0], "Lisbon: 21");
      const weatherResult = results.find(
        (result) => result.role === "toolResult" && result.toolCallId === "c1",
      );
      assert.deepStrictEqual(
        weatherResult?.role === "toolResult" ? weatherResult.details : undefined,
        {
          celsius: 21,
        },
      );
      assert.strictEqual(texts[1], "hello ada (c2)");
      assert.match(texts[2] ?? "", /^\d+:charge$/);
      assert.strictEqual(seen.charges, 1);
      assert.strictEqual(seen.concurrent, 1);
      assert.deepStrictEqual(seen.missing, ["ConversationNotFound"]);
      assert.strictEqual(texts[3], '{"text":"hi","callId":"c4"}');
    }),
  );

  it.live("tasks commit transitions, with their changes in the same commit", () =>
    withAgent(
      Effect.gen(function* () {
        const harness = yield* Harness.Harness;
        const root = yield* harness.ensureRoot();
        const flow = yield* root.commit(
          Flow.create({ label: "long" }, { ownership: { kind: "conversation" } }),
        );
        const settled = yield* harness.awaitTask(flow).pipe(Effect.timeout("5 seconds"));
        assert.deepStrictEqual(settled.state.outcome, { status: "completed", result: "long" });
        const notes = (yield* Stream.runCollect(root.entries({ order: "ascending" })))
          .filter(Note.is)
          .map((entry) => entry.data.text);
        assert.deepStrictEqual(notes, ["waiting", "finished"]);

        // The second run reads the counter the first one wrote, and completes at once.
        const short = yield* root.commit(
          Flow.create({ label: "short" }, { ownership: { kind: "conversation" } }),
        );
        const shortSettled = yield* harness.awaitTask(short).pipe(Effect.timeout("5 seconds"));
        assert.deepStrictEqual(shortSettled.state.outcome, {
          status: "completed",
          result: "short",
        });
        assert.deepStrictEqual(yield* Counter.read(), Option.some({ count: 2 }));

        const failing = yield* root.commit(
          Failing.create(null, { ownership: { kind: "conversation" } }),
        );
        const failed = yield* harness.awaitTask(failing).pipe(Effect.timeout("5 seconds"));
        assert.deepStrictEqual(failed.state.outcome, {
          status: "failed",
          error: { message: "nope" },
        });

        const blocked = yield* root.commit(
          Blocked.create(null, { ownership: { kind: "conversation" } }),
        );
        yield* harness.abortTask(blocked);
        const aborted = yield* harness.awaitTask(blocked).pipe(Effect.timeout("5 seconds"));
        assert.deepStrictEqual(aborted.state.outcome, { status: "aborted" });
      }),
    ),
  );

  it.live("documents: get and retire in a commit; read, require and changes from the Session", () =>
    withAgent(
      Effect.gen(function* () {
        const harness = yield* Harness.Harness;
        const root = yield* harness.ensureRoot();
        assert.deepStrictEqual(yield* Lead.read(root.id), Option.none());
        const absent = yield* Effect.flip(Lead.require(root.id));
        assert.strictEqual(absent._tag, "DocumentAccessError");
        assert.strictEqual(
          absent._tag === "DocumentAccessError" ? absent.reason._tag : "",
          "NotFound",
        );
        assert.deepStrictEqual(yield* Stream.runCollect(Lead.changes(root.id)), [Option.none()]);

        yield* root.commit(Effect.asVoid(Lead.get(root.id)));
        const changes = yield* Effect.forkChild(
          Stream.runCollect(Lead.changes(root.id).pipe(Stream.take(3))),
          { startImmediately: true },
        );
        yield* flush;
        yield* root.commit(Effect.map(Lead.get(root.id), (lead) => void (lead.mode = "off")));
        assert.deepStrictEqual(yield* Lead.require(root.id), { mode: "off" });
        // Changes are a watch's folded frames: the consumer takes this one before the retirement.
        yield* flush;
        yield* root.commit(Lead.retire(root.id));
        assert.deepStrictEqual(yield* Fiber.join(changes).pipe(Effect.timeout("5 seconds")), [
          Option.some({ mode: "auto" }),
          Option.some({ mode: "off" }),
          Option.none(),
        ]);
        assert.deepStrictEqual(yield* Lead.read(root.id), Option.none());

        // A family member is addressed by its owner and key, and created from its seed.
        yield* root.commit(
          Effect.map(Approvals.get(root.id, "send", "ada"), (approval) => {
            approval.approved = true;
          }),
        );
        assert.deepStrictEqual(
          yield* Approvals.read(root.id, "send"),
          Option.some({ approved: true, by: "ada" }),
        );
      }),
    ),
  );

  it.live("entries: append in a commit, and drafts for write submissions", () =>
    withAgent(
      Effect.gen(function* () {
        const harness = yield* Harness.Harness;
        const root = yield* harness.ensureRoot();
        const appended = yield* root.commit(Note.append(root.id, { text: "a" }));
        assert.strictEqual(appended.kind, "test.d22.note");
        assert.deepStrictEqual(appended.data, { text: "a" });
        const marker = yield* root.commit(Marker.append(root.id));
        assert.strictEqual(marker.kind, "test.d22.marker");
        const submission = yield* root.submit({ type: "write", entry: Note.draft({ text: "b" }) });
        yield* submission.await.pipe(Effect.timeout("5 seconds"));
        const kinds = (yield* Stream.runCollect(root.entries({ order: "ascending" }))).map(
          (entry) => (Note.is(entry) ? entry.data.text : entry.kind),
        );
        assert.deepStrictEqual(kinds, ["a", "test.d22.marker", "b"]);
      }),
    ),
  );

  it.live(
    "defaults: a top-level conversation gets the default agent; a fork its parent's; explicit wins",
    () =>
      withAgent(
        Effect.gen(function* () {
          const harness = yield* Harness.Harness;
          const root = yield* harness.ensureRoot();
          assert.deepStrictEqual((yield* root.agent).model, FAUX_MODEL);
          const ownerless = yield* harness.createConversation({ ownership: { kind: "ownerless" } });
          assert.deepStrictEqual((yield* ownerless.agent).model, FAUX_MODEL);
          const explicit = yield* harness.createConversation({
            ownership: { kind: "ownerless" },
            agent: { model: OTHER_MODEL },
          });
          assert.deepStrictEqual((yield* explicit.agent).model, OTHER_MODEL);

          const at = yield* explicit.commit(Note.append(explicit.id, { text: "fork here" }));
          const fork = yield* explicit.fork(at.id, { ownership: { kind: "ownerless" } });
          assert.deepStrictEqual((yield* fork.agent).model, OTHER_MODEL);

          // A conversation created in a commit, as a host does with `Tx.createConversation`, gets it too.
          const viaTx = yield* harness.commit(
            Effect.flatMap(Tx, (tx) => tx.createConversation({ ownership: { kind: "ownerless" } })),
          );
          const created = yield* harness.conversation(viaTx.id);
          assert.isTrue(Option.isSome(created));
          if (Option.isSome(created)) {
            assert.deepStrictEqual((yield* created.value.agent).model, FAUX_MODEL);
          }
        }),
      ),
  );

  it.live("defaults: an Effect is resolved when the layer is built, with its services", () =>
    Effect.gen(function* () {
      class ModelChoice extends Context.Service<
        ModelChoice,
        { readonly model: typeof OTHER_MODEL }
      >()("@effect-durable-harness/core/test/Api.test/ModelChoice") {}
      const layer = ChatHarness.layer({
        defaults: {
          agent: Effect.map(Effect.service(ModelChoice), (choice) => ({ model: choice.model })),
        },
      }).pipe(
        Layer.provide(ChatLive),
        Layer.provide(
          Layer.mergeAll(
            MemoryAgentStorage.layer,
            makeFaux().layer,
            Layer.succeed(ModelChoice, { model: OTHER_MODEL }),
          ),
        ),
      );
      const model = yield* Effect.gen(function* () {
        const root = yield* (yield* Harness.Harness).ensureRoot();
        return (yield* root.agent).model;
      }).pipe(Effect.provide(layer));
      assert.deepStrictEqual(model, OTHER_MODEL);
    }),
  );

  it.live("a declared tool or task with no handler fails the build, naming it", () =>
    Effect.gen(function* () {
      const Lonely = AgentTool.make("lonely", {
        description: "Has no handler.",
        parameters: Schema.Struct({}),
      });
      const LonelyTask = Task.make("test.d22.lonely", {
        version: 1,
        input: Schema.Null,
        checkpoint: Schema.Struct({ phase: Schema.Literal("run") }),
        result: Schema.Null,
        initial: () => ({ phase: "run" }),
      });
      const messages = [
        yield* buildWithoutHandlers(Extension.make("chat", { tools: [Lonely] })),
        yield* buildWithoutHandlers(Extension.make("chat", { tasks: [LonelyTask] })),
      ].map((exit) =>
        Exit.isFailure(exit) ? Cause.squash(exit.cause) : new Error("the build succeeded"),
      );
      assert.deepStrictEqual(
        messages.map((error) => (error instanceof Error ? error.message : String(error))),
        [
          'Extension "chat" declares tool "lonely" with no handler',
          'Extension "chat" declares task "test.d22.lonely" with no handler',
        ],
      );
    }),
  );
});

// ── the fake model's controls ────────────────────────────────────────────────────────

/** A section that reads a document through the `Session` it gets per call. */
const Mode = Section.make("mode", (input) =>
  Effect.map(Lead.read(input.conversationId), (lead) =>
    Option.match(lead, { onNone: () => "unset", onSome: (value) => value.mode }),
  ),
);

const Probe = Harness.make("probe", {
  extensions: [Extension.make("probe", { tools: [Echo], sections: [Mode] })],
});

const ProbeAgent = Probe.layer({ defaults: { agent: { model: FauxModel.ref } } }).pipe(
  Layer.provideMerge(FauxModel.layer),
  Layer.provide(MemoryAgentStorage.layer),
);

describe("FauxModel.Test", () => {
  it.live("scripts the model, records its requests, and holds one at a gate", () =>
    Effect.gen(function* () {
      const faux = yield* FauxModel.Test;
      yield* faux.push(FauxModel.toolCall("echo", { text: "hi" }), FauxModel.text("done"));
      const harness = yield* Harness.Harness;
      yield* harness.resume;
      const root = yield* harness.ensureRoot();
      yield* root.commit(Effect.map(Lead.get(root.id), (lead) => void (lead.mode = "off")));

      const gate = yield* faux.gate();
      const submission = yield* root.submit({ type: "input", content: "go" });
      yield* gate.started.pipe(Effect.timeout("5 seconds"));
      assert.strictEqual((yield* faux.requests).length, 1);
      // Held: no answer yet.
      assert.strictEqual(
        (yield* Stream.runCollect(root.entries())).filter(
          (entry) => entry.kind === "core.assistant",
        ).length,
        0,
      );
      yield* gate.release;
      yield* submission.await.pipe(Effect.timeout("5 seconds"));

      const requests = yield* faux.requests;
      assert.strictEqual(requests.length, 2);
      const system = requests[0]?.messages.find((message) => message.role === "system");
      assert.include(system?.role === "system" ? system.sections?.["mode"] : undefined, "off");
      yield* faux.assertExhausted;

      yield* faux.push(FauxModel.text("never asked for"));
      const unused = yield* Effect.exit(faux.assertExhausted);
      assert.isTrue(Exit.isFailure(unused));
    }).pipe(Effect.scoped, Effect.provide(ProbeAgent)),
  );
});

// ── steps across a restart ──────────────────────────────────────────────────────

describeEachBackend("step", (backend) => {
  it.effect("returns the recorded result after a restart without running the effect again", () =>
    run(backend)(
      Effect.gen(function* () {
        const storage = yield* controlledStorage;
        const external = { calls: 0 };
        const runs: Array<string> = [];
        const block = { first: true };
        const Stepped = Task.make("test.d22.stepped", {
          version: 1,
          input: Schema.Null,
          checkpoint: Schema.Struct({ phase: Schema.Literal("run") }),
          result: Schema.String,
          initial: () => ({ phase: "run" }),
          phases: {
            run: (_task, runtime) =>
              Effect.gen(function* () {
                const receipt = yield* runtime.step(
                  "external",
                  Receipt,
                  Effect.sync(() => {
                    external.calls++;
                    return { id: runtime.key("external") };
                  }),
                );
                runs.push(receipt.id);
                if (block.first) {
                  block.first = false;
                  return yield* Effect.never;
                }
                yield* runtime.complete(receipt.id);
              }),
          },
          abort: (_task, runtime) => runtime.aborted,
        });

        const first = yield* openHarness(storage, [Stepped]);
        const root = yield* first.harness.ensureRoot();
        const id = yield* root.commit(
          Stepped.create(null, { ownership: { kind: "conversation" } }),
        );
        yield* first.harness.resume;
        yield* eventually(Effect.sync(() => runs.length === 1));
        yield* first.close;

        const second = yield* openHarness(storage, [Stepped]);
        const record = Option.getOrUndefined(yield* second.harness.getTask(id));
        assert.deepStrictEqual(record?.memos, { "step:external": { id: `${id}:external` } });
        yield* second.harness.resume;
        const settled = yield* second.harness.awaitTask(id);
        assert.deepStrictEqual(settled.state.outcome, {
          status: "completed",
          result: `${id}:external`,
        });
        assert.strictEqual(external.calls, 1);
        assert.deepStrictEqual(runs, [`${id}:external`, `${id}:external`]);
        yield* second.close;
      }),
    ),
  );
});

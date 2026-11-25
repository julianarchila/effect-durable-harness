// Tests of tool and task definitions without their implementation, implemented by a Layer
// (`toLayer`), as `effect/ai`'s `Tool` and `Toolkit.toLayer` are; and a `Scope` per task phase.
import { assert, describe, it } from "@effect/vitest";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as AgentTool from "../src/AgentTool.ts";
import * as Extension from "../src/Extension.ts";
import * as Harness from "../src/Harness.ts";
import * as MemoryAgentStorage from "../src/MemoryAgentStorage.ts";
import type { Models } from "../src/Models.ts";
import * as Task from "../src/Task.ts";
import {
  fauxAssistantMessage,
  fauxText,
  fauxToolCall,
  make as makeFaux,
} from "../src/testing/FauxModel.ts";
import { Tx } from "../src/Tx.ts";

/** A domain service the implementations use. */
class Greeter extends Context.Service<Greeter, { readonly greet: (name: string) => string }>()(
  "@effect-durable-harness/core/test/Definitions.test/Greeter",
) {}
const GreeterLive = Layer.succeed(Greeter, { greet: (name: string) => `hello ${name}` });

const FAUX_MODEL = { provider: "faux", modelId: "faux-1" } as const;

const completed = <R>(result: R) =>
  ({ status: "terminal", outcome: { status: "completed", result } }) as const;
const aborted = { status: "terminal", outcome: { status: "aborted" } } as const;

// ── definitions: no implementation ──────────────────────────────────────────────

const Greet = AgentTool.make("greet", {
  description: "Greets someone.",
  parameters: Schema.Struct({ name: Schema.String }),
  replay: "safe",
});

const Remember = Task.make({
  name: "test.definitions.remember",
  version: 1,
  input: Schema.Struct({ name: Schema.String }),
  checkpoint: Schema.Union([
    Schema.Struct({ phase: Schema.Literal("first") }),
    Schema.Struct({ phase: Schema.Literal("second"), greeting: Schema.String }),
  ]),
  result: Schema.String,
  initial: () => ({ phase: "first" }),
});

const Definitions = Extension.make({ name: "definitions", tools: [Greet], tasks: [Remember] });

// ── implementations: Layers ─────────────────────────────────────────────────────

let built = 0;
const GreetLive = Greet.toLayer(
  Effect.gen(function* () {
    built++;
    const greeter = yield* Greeter; // resolved once, when the Layer is built
    return ({ name }) =>
      Effect.succeed({ output: [{ type: "text" as const, text: greeter.greet(name) }] });
  }),
);

/** The phases record what their own scope closes, so a test sees when each phase's resources are released. */
const closed: Array<string> = [];
const RememberLive = Remember.toLayer({
  phases: {
    first: (task, runtime) =>
      Effect.gen(function* () {
        const greeter = yield* Greeter;
        yield* Effect.addFinalizer(() => Effect.sync(() => closed.push("first")));
        const greeting = greeter.greet(task.input.name);
        yield* runtime.commit(() =>
          Effect.succeed({ status: "running", checkpoint: { phase: "second", greeting } } as const),
        );
      }),
    second: (task, runtime) =>
      Effect.gen(function* () {
        // The first phase's scope is closed before the second one starts.
        const seen = [...closed];
        yield* Effect.addFinalizer(() => Effect.sync(() => closed.push("second")));
        yield* runtime.commit(() =>
          Effect.succeed(completed(`${task.state.checkpoint.greeting} (${seen.join(",")})`)),
        );
      }),
  },
  abort: (_task, runtime) => runtime.commit(() => Effect.succeed(aborted)),
});

/** Handlers that type-check against `Remember`, for the type-level test. */
const RememberHandlers = {
  phases: { first: () => Effect.void, second: () => Effect.void },
  abort: () => Effect.void,
};

/** The agent as a Layer, its implementations and their services provided under it. */
const agent = (models: Layer.Layer<Models>) =>
  Harness.layer({ extensions: [Definitions] }).pipe(
    Layer.provide(Layer.mergeAll(GreetLive, RememberLive)),
    Layer.provide(Layer.mergeAll(GreeterLive, MemoryAgentStorage.layer, models)),
  );

describe("definitions implemented by Layers", () => {
  it.live("a tool with no execute runs its handler Layer, built once with its services", () =>
    Effect.gen(function* () {
      built = 0;
      const faux = makeFaux();
      faux.setResponses([
        fauxAssistantMessage([fauxToolCall("greet", { name: "ada" }, { id: "c1" })], {
          stopReason: "toolUse",
        }),
        fauxAssistantMessage([fauxToolCall("greet", { name: "bob" }, { id: "c2" })], {
          stopReason: "toolUse",
        }),
        fauxAssistantMessage([fauxText("done")]),
      ]);
      const context = yield* Layer.build(agent(faux.layer));
      const harness = Context.get(context, Harness.Harness);
      yield* harness.resume;
      const root = yield* harness.ensureRoot({ agent: { model: FAUX_MODEL } });
      const submission = yield* root.submit({ type: "input", content: "greet them" });
      yield* submission.await.pipe(Effect.timeout("5 seconds"));
      const results = (yield* Stream.runCollect(root.entries({ order: "ascending" })))
        .flatMap((entry) => entry.model ?? [])
        .filter((message) => message.role === "toolResult")
        .map((message) =>
          message.role === "toolResult" && message.content[0]?.type === "text"
            ? message.content[0].text
            : "",
        );
      assert.deepStrictEqual(results, ["hello ada", "hello bob"]);
      assert.strictEqual(built, 1);
    }).pipe(Effect.scoped),
  );

  it.live("a task with no phases runs its handler Layer; each phase has its own scope", () =>
    Effect.gen(function* () {
      closed.length = 0;
      const context = yield* Layer.build(agent(makeFaux().layer));
      const harness = Context.get(context, Harness.Harness);
      yield* harness.resume;
      const root = yield* harness.ensureRoot();
      const id = yield* root.commit(
        Effect.gen(function* () {
          const tx = yield* Tx;
          return yield* tx.createTask(
            Remember,
            { name: "ada" },
            { ownership: { kind: "conversation" } },
          );
        }),
      );
      const settled = yield* harness.awaitTask(id).pipe(Effect.timeout("5 seconds"));
      assert.deepStrictEqual(settled.state.outcome, completed("hello ada (first)").outcome);
      // The second phase's scope closed when it ended, not when the Harness does. With group commit
      // the wait wakes at the terminal commit's publication, before the scope closer's next pass, so it is
      // checked within a bounded number of turns, with the Harness still open.
      for (let turn = 0; turn < 50 && closed.length < 2; turn++) yield* Effect.yieldNow;
      assert.deepStrictEqual(closed, ["first", "second"]);
    }).pipe(Effect.scoped),
  );

  it.live("a phase does not see the Harness's scope", () =>
    Effect.gen(function* () {
      let phaseScope: Scope.Scope | undefined;
      const Probe = Task.make({
        name: "test.definitions.scope",
        version: 1,
        input: Schema.Struct({}),
        checkpoint: Schema.Struct({ phase: Schema.Literal("run") }),
        result: Schema.Null,
        initial: () => ({ phase: "run" }),
        phases: {
          run: (_task, runtime) =>
            Effect.gen(function* () {
              phaseScope = yield* Effect.scope;
              yield* runtime.commit(() => Effect.succeed(completed(null)));
            }),
        },
        abort: (_task, runtime) => runtime.commit(() => Effect.succeed(aborted)),
      });
      const harnessScope = yield* Scope.make();
      const context = yield* Layer.buildWithScope(
        Harness.layer({ extensions: [Extension.make({ name: "probe", tasks: [Probe] })] }).pipe(
          Layer.provide(Layer.mergeAll(MemoryAgentStorage.layer, makeFaux().layer)),
        ),
        harnessScope,
      );
      const harness = Context.get(context, Harness.Harness);
      yield* harness.resume;
      const root = yield* harness.ensureRoot();
      const id = yield* root.commit(
        Effect.gen(function* () {
          const tx = yield* Tx;
          return yield* tx.createTask(Probe, {}, { ownership: { kind: "conversation" } });
        }),
      );
      yield* harness.awaitTask(id).pipe(Effect.timeout("5 seconds"));
      assert.isDefined(phaseScope);
      assert.notStrictEqual(phaseScope, harnessScope);
      yield* Scope.close(harnessScope, Exit.void);
    }),
  );

  it.live("an extension's toLayer implements all its declarations at once", () =>
    Effect.gen(function* () {
      built = 0;
      closed.length = 0;
      const DefinitionsLive = Definitions.toLayer(
        Effect.gen(function* () {
          built++;
          const greeter = yield* Greeter;
          return Definitions.of({
            tools: {
              greet: ({ name }) =>
                Effect.succeed({ output: [{ type: "text", text: `${greeter.greet(name)}!` }] }),
            },
            tasks: {
              "test.definitions.remember": {
                phases: {
                  first: (task, runtime) => {
                    // The service runs before the commit, which only gets its result.
                    const greeting = greeter.greet(task.input.name);
                    return runtime.commit(() =>
                      Effect.succeed({
                        status: "running",
                        checkpoint: { phase: "second", greeting },
                      } as const),
                    );
                  },
                  second: (task, runtime) =>
                    runtime.commit(() => Effect.succeed(completed(task.state.checkpoint.greeting))),
                },
                abort: (_task, runtime) => runtime.commit(() => Effect.succeed(aborted)),
              },
            },
          });
        }),
      );
      const faux = makeFaux();
      faux.setResponses([
        fauxAssistantMessage([fauxToolCall("greet", { name: "ada" }, { id: "c1" })], {
          stopReason: "toolUse",
        }),
        fauxAssistantMessage([fauxText("done")]),
      ]);
      const context = yield* Layer.build(
        Harness.layer({ extensions: [Definitions] }).pipe(
          Layer.provide(DefinitionsLive),
          Layer.provide(Layer.mergeAll(GreeterLive, MemoryAgentStorage.layer, faux.layer)),
        ),
      );
      const harness = Context.get(context, Harness.Harness);
      yield* harness.resume;
      const root = yield* harness.ensureRoot({ agent: { model: FAUX_MODEL } });
      const submission = yield* root.submit({ type: "input", content: "greet her" });
      yield* submission.await.pipe(Effect.timeout("5 seconds"));
      const results = (yield* Stream.runCollect(root.entries({ order: "ascending" })))
        .flatMap((entry) => entry.model ?? [])
        .flatMap((message) =>
          message.role === "toolResult" && message.content[0]?.type === "text"
            ? [message.content[0].text]
            : [],
        );
      assert.deepStrictEqual(results, ["hello ada!"]);
      const id = yield* root.commit(
        Effect.flatMap(Tx, (tx) =>
          tx.createTask(Remember, { name: "bob" }, { ownership: { kind: "conversation" } }),
        ),
      );
      const settled = yield* harness.awaitTask(id).pipe(Effect.timeout("5 seconds"));
      assert.deepStrictEqual(settled.state.outcome, completed("hello bob").outcome);
      // One build for every handler of the extension.
      assert.strictEqual(built, 1);
    }).pipe(Effect.scoped),
  );

  it("an extension's toLayer requires a handler for every declaration, and provides them all", () => {
    const layer = Definitions.toLayer({
      tools: {
        greet: ({ name }) =>
          Effect.gen(function* () {
            const greeter = yield* Greeter;
            return { output: [{ type: "text" as const, text: greeter.greet(name) }] };
          }),
      },
      tasks: { "test.definitions.remember": RememberHandlers },
    });
    type Provides = Layer.Success<typeof layer>;
    type Requires = Layer.Services<typeof layer>;
    const tool: AgentTool.Handler<"greet"> extends Provides ? true : false = true;
    const task: Task.Handler<"test.definitions.remember"> extends Provides ? true : false = true;
    const greeter: Greeter extends Requires ? true : false = true;
    // Without the task's handlers the record is incomplete (a misspelled name leaves the real one missing too).
    type Complete = Parameters<typeof Definitions.of>[0];
    type ToolsOnly = { readonly tools: Complete["tools"] };
    const incomplete: ToolsOnly extends Complete ? true : false = false;
    assert.isTrue(tool && task && greeter && !incomplete);
  });

  it("the Harness requires a handler for every definition without an implementation", () => {
    const layer = Harness.layer({ extensions: [Definitions] });
    type Requires = Layer.Services<typeof layer>;
    const tool: AgentTool.Handler<"greet"> extends Requires ? true : false = true;
    const task: Task.Handler<"test.definitions.remember"> extends Requires ? true : false = true;
    const greeter: Greeter extends Requires ? true : false = false; // only the implementations ask for it
    assert.isTrue(tool && task && !greeter);
  });
});

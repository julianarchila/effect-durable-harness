import * as NodeFileSystem from "@effect/platform-node/NodeFileSystem";
import { assert, describe, expectTypeOf, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Schema from "effect/Schema";
import * as Extension from "../src/Extension.ts";
import type { CompactionResult } from "../src/Compaction.ts";
import type * as Harness from "../src/Harness.ts";
import * as Hook from "../src/Hook.ts";
import type { TaskId } from "../src/Ids.ts";
import type { SubmissionDraft } from "../src/Submission.ts";
import * as Task from "../src/Task.ts";
import { Tx } from "../src/Tx.ts";

// The package root and the SQL storage must load no host runtime.

const sourceRoot = new URL("../src/", import.meta.url).pathname;
const forbidden =
  /^(?:node:|@effect\/sql-sqlite-|@effect\/platform-|@cloudflare\/|alchemy|@alchemy\.run\/)/;

const resolvePath = (from: string, specifier: string): string =>
  new URL(specifier, `file://${from}`).pathname;

/** Every source file a module loads at runtime, and the bare imports among them. */
const sourceGraph = (entry: string) =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const visited = new Set<string>();
    const external = new Set<string>();
    const pending = [`${sourceRoot}${entry}`];
    while (pending.length > 0) {
      const path = pending.pop();
      if (path === undefined || visited.has(path)) continue;
      visited.add(path);
      const source = yield* fs.readFileString(path);
      // Type-only imports are erased and load nothing.
      for (const match of source.matchAll(
        /\b(import|export)(\s+type\b)?[^;"']*?(?:from\s*)?["']([^"']+)["']/g,
      )) {
        const [, , typeOnly, specifier = ""] = match;
        if (typeOnly !== undefined) continue;
        if (specifier.startsWith(".")) pending.push(resolvePath(path, specifier));
        else external.add(specifier);
      }
    }
    return { files: [...visited], external: [...external] };
  });

const boundary = (name: string, entry: string) =>
  it.live(name, () =>
    Effect.gen(function* () {
      const graph = yield* sourceGraph(entry);
      assert.deepStrictEqual(
        graph.external.filter((specifier) => forbidden.test(specifier)),
        [],
      );
      assert.strictEqual(
        graph.files.some((path) => path.includes("/testing/")),
        false,
      );
    }).pipe(Effect.provide(NodeFileSystem.layer)),
  );

describe("durable storage runtime boundaries", () => {
  boundary("keeps the package root free of host runtimes", "index.ts");
  boundary("keeps SqlAgentStorage free of host runtimes and SQLite drivers", "SqlAgentStorage.ts");
  boundary("keeps MemoryAgentStorage free of host runtimes", "MemoryAgentStorage.ts");
  boundary("keeps Session free of host runtimes", "Session.ts");
  boundary("keeps Harness free of host runtimes", "Harness.ts");
  boundary("keeps Task free of host runtimes", "Task.ts");
  boundary("keeps SessionWakeups free of host runtimes", "SessionWakeups.ts");
});

// Type-level checks of a task definition: its input, checkpoint and result types flow to `createTask`,
// `awaitTask` and its handlers, and wrong uses fail to compile.
const StepperInput = Schema.Struct({ steps: Schema.Number });
const StepperCheckpoint = Schema.Union([
  Schema.Struct({ phase: Schema.Literal("plan"), steps: Schema.Number }),
  Schema.Struct({ phase: Schema.Literal("run"), step: Schema.Number }),
]);
type Checkpoint = typeof StepperCheckpoint.Type;
const StepperResult = Schema.Struct({ ran: Schema.Number });
type Ran = typeof StepperResult.Type;
// A task declares no hooks: its handlers name the handler type at `runtime.hooks.each`, and a handler gets a
// `HookContext` and returns an Effect.
type Hooks = {
  readonly beforeStep: (
    step: number,
    context: Hook.HookContext,
  ) => Effect.Effect<{ readonly skip: boolean } | undefined>;
};

it("types submissions, task waits, compaction, and tasks erased into extensions", () => {
  const input = { type: "input", content: "hi", whenBusy: "steer" } satisfies SubmissionDraft;
  const write = { type: "write", entry: { kind: "note" } } satisfies SubmissionDraft;
  expectTypeOf(input.type).toEqualTypeOf<"input">();
  expectTypeOf(write.type).toEqualTypeOf<"write">();

  // A task with narrowed input, several phases, and custom hooks.
  const Stepper = Task.make({
    name: "test.stepper",
    version: 1,
    input: StepperInput,
    checkpoint: StepperCheckpoint,
    result: StepperResult,
    initial: (input) => ({ phase: "plan", steps: input.steps }),
    phases: {
      plan: (task, runtime) =>
        Effect.gen(function* () {
          expectTypeOf(task.state.checkpoint.steps).toEqualTypeOf<number>();
          expectTypeOf(task.input.steps).toEqualTypeOf<number>();
          yield* runtime.commit(() =>
            Effect.succeed({ status: "running", checkpoint: { phase: "run", step: 0 } } as const),
          );
        }),
      run: (task, runtime) =>
        Effect.gen(function* () {
          expectTypeOf(task.state.checkpoint.step).toEqualTypeOf<number>();
          yield* runtime.hooks.each<Hooks["beforeStep"], never, never>("beforeStep", (handler) => {
            expectTypeOf(handler).toEqualTypeOf<Hooks["beforeStep"]>();
            return Effect.void;
          });
          yield* runtime.commit(() =>
            Effect.succeed({
              status: "terminal",
              outcome: { status: "completed", result: { ran: 1 } },
            } as const),
          );
        }),
    },
    abort: () => Effect.void,
  });
  // A task carries no hook types (see `Hooks` above); what it carries instead is its handlers' requirements,
  // here none.
  expectTypeOf<Task.Requirements<typeof Stepper>>().toEqualTypeOf<never>();
  const registration = Hook.make(Stepper, {
    beforeStep: (step: number) => Effect.succeed(step > 1 ? { skip: true } : undefined),
  });
  // Erased into an extension, whatever its input, phases, and hooks.
  const erased: Task.Any = Stepper;
  const extension: Extension.Any = Extension.make({
    name: "stepper",
    tasks: [Stepper],
    hooks: [registration],
  });
  void [erased, extension];

  // `awaitTask`, on the Harness and on the runtime, is typed by the task's result.
  const typedWaits = (
    harness: Harness.Harness["Service"],
    conversation: Harness.Conversation,
    runtime: Task.TaskRuntime<null, Checkpoint, null>,
  ) =>
    Effect.gen(function* () {
      const id = yield* conversation.commit(
        Effect.gen(function* () {
          const tx = yield* Tx;
          return yield* tx.createTask(
            Stepper,
            { steps: 2 },
            { ownership: { kind: "conversation" } },
          );
        }),
      );
      expectTypeOf(id).toEqualTypeOf<TaskId<Ran>>();
      const settled = yield* harness.awaitTask(id);
      expectTypeOf(settled).toEqualTypeOf<Task.SettledTask<Ran>>();
      if (settled.state.outcome.status === "completed") {
        expectTypeOf(settled.state.outcome.result).toEqualTypeOf<Ran>();
      }
      expectTypeOf(yield* runtime.awaitTask(id)).toEqualTypeOf<Task.SettledTask<Ran>>();
      const compaction = yield* conversation.compact();
      expectTypeOf(compaction).toEqualTypeOf<TaskId<CompactionResult>>();
      expectTypeOf(yield* harness.awaitTask(compaction)).toEqualTypeOf<
        Task.SettledTask<CompactionResult>
      >();
    });
  expectTypeOf(typedWaits).toBeFunction();

  const compileTimeFailures = (conversation: Harness.Conversation) => {
    const inputWithModel: SubmissionDraft = {
      type: "input",
      content: "hi",
      // @ts-expect-error an input's entry has no model: its model message is the input's content
      entry: { kind: "note", model: [] },
    };
    const writeWithContent: SubmissionDraft = {
      type: "write",
      entry: { kind: "note" },
      // @ts-expect-error a write submission carries no content
      content: "hi",
    };
    const writeWhenBusy: SubmissionDraft = {
      type: "write",
      entry: { kind: "note" },
      // @ts-expect-error a write submission never generates, so it has no busy policy
      whenBusy: "steer",
    };
    // `Hook.make` takes any named Effect handlers, since a task declares no hook types.
    const wrongInput = conversation.commit(
      Effect.gen(function* () {
        const tx = yield* Tx;
        return yield* tx.createTask(
          Stepper,
          // @ts-expect-error task input is typed by the definition
          { steps: "two" },
          { ownership: { kind: "conversation" } },
        );
      }),
    );
    const missingPhase = Task.make({
      name: "test.missing-phase",
      version: 1,
      input: Schema.Null,
      checkpoint: StepperCheckpoint,
      result: Schema.Null,
      initial: () => ({ phase: "plan", steps: 1 }),
      // @ts-expect-error the phase map is exhaustive
      phases: { plan: () => Effect.void },
      abort: () => Effect.void,
    });
    void [inputWithModel, writeWithContent, writeWhenBusy, wrongInput, missingPhase];
  };
  expectTypeOf(compileTimeFailures).toBeFunction();
});

// The `core.live` document's deltas: the exact ops each commit writes while a turn runs, on every backend.
//
// Extensions are the static extensions of `openChat`; storage that inspects commits is an `intercept` of the
// controlled storage of the backend. Real-time waits run on the `TestClock`, which `waitFor` advances in 5 ms
// steps. Ops are `@effect-durable-harness/delta`'s.
//
// The driven tool takes its actions from a queue; its abort is the interruption of its fiber.
import * as Delta from "@effect-durable-harness/delta/Delta";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as Predicate from "effect/Predicate";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as AgentTool from "../src/AgentTool.ts";
import { ToolContext } from "../src/AgentTool.ts";
import * as Extension from "../src/Extension.ts";
import type { Conversation } from "../src/Harness.ts";
import { TaskId } from "../src/Ids.ts";
import { LiveDoc, LiveState } from "../src/Live.ts";
import type { TextContent, ToolResultContent } from "../src/Message.ts";
import type { CommitChange, CommitPublication } from "../src/Session.ts";
import type { Submission } from "../src/Submission.ts";
import { fauxAssistantMessage, fauxText, fauxToolCall } from "../src/testing/FauxModel.ts";
import { Tx } from "../src/Tx.ts";
import { chatSetup, openChat, waitFor } from "./ChatSupport.ts";
import type { TestHarness } from "./HarnessSupport.ts";
import { controlledStorage, describeEachBackend, documentChanges, run } from "./SessionSupport.ts";

type Op = Delta.Operation;
type Action = Effect.Effect<void, never, ToolContext>;

const NoParameters = Schema.Struct({});

/** A tool extension without parameters. */
const toolExtension = <E>(
  name: string,
  description: string,
  execute: () => Effect.Effect<AgentTool.ToolExecutionResult, E, ToolContext>,
  outputLimits: AgentTool.OutputLimits = {},
) =>
  Extension.make({
    name: `tool:${name}`,
    tools: [AgentTool.make(name, { description, parameters: NoParameters, outputLimits, execute })],
  });

/**
 * Every `core.live` commit's ops, recorded from the Session's own observers, which see each deferred commit when
 * memory adopts it, or, with `stored`, from the host listeners, which see exactly what each storage call stored.
 */
const recordLiveOps = (harness: TestHarness, stored = false) =>
  Effect.gen(function* () {
    const commits: Array<Array<Op>> = [];
    const record = (publication: { readonly changes: ReadonlyArray<CommitChange> }) => {
      for (const change of publication.changes) {
        if (
          change.type === "document" &&
          change.record.kind === "core.live" &&
          change.ops.length > 0
        )
          commits.push([...change.ops]);
      }
    };
    if (stored) yield* harness.session.observeCommits({ advance: record, end: () => {} });
    else yield* harness.kernel.followCommits(record);
    return commits;
  });

/** Waits for a submission to settle, while the `TestClock` advances. */
const settle = (submission: Submission) =>
  Effect.gen(function* () {
    const fiber = yield* Effect.forkChild(submission.await, { startImmediately: true });
    yield* waitFor(Effect.sync(() => fiber.pollUnsafe() !== undefined));
    return yield* Fiber.join(fiber);
  });

const live = (harness: TestHarness, root: Conversation) =>
  harness.session.snapshot(LiveDoc, root.id).pipe(Effect.map(Option.getOrUndefined));

const entryIds = (root: Conversation, kind: string) =>
  Stream.runCollect(root.entries({ order: "ascending" })).pipe(
    Effect.map((entries) => Array.from(entries).filter((entry) => entry.kind === kind)),
  );

/** What the cases compare: ops, paths and JSON values. */
type Comparable = Schema.Json | Delta.Path | Op | ReadonlyArray<Op> | undefined;

const sameJson = (left: Comparable, right: Comparable) =>
  JSON.stringify(left) === JSON.stringify(right);

/** Asserts that `ops` contains every op of `expected`. */
const assertContains = (ops: ReadonlyArray<Op> | undefined, expected: ReadonlyArray<Op>) => {
  for (const op of expected) {
    assert.isTrue(
      (ops ?? []).some((candidate) => sameJson(candidate, op)),
      `${JSON.stringify(op)} in ${JSON.stringify(ops)}`,
    );
  }
};

/**
 * One tool call driven step by step, capturing the exact ops of every `core.live` commit. Each
 * `step` runs one action inside the tool and returns the ops of the commit it caused.
 */
const drive = (outputLimits: AgentTool.OutputLimits = {}) =>
  Effect.gen(function* () {
    const setup = chatSetup();
    const actions = yield* Queue.unbounded<Action | "return">();
    const driven = toolExtension(
      "drive",
      "Driven by the test",
      () =>
        Effect.gen(function* () {
          for (;;) {
            const action = yield* Queue.take(actions);
            if (action === "return") return {};
            yield* action;
          }
        }),
      outputLimits,
    );
    setup.faux.setResponses([
      fauxAssistantMessage([fauxToolCall("drive", {}, { id: "c1" })], { stopReason: "toolUse" }),
      fauxAssistantMessage([fauxText("done")]),
    ]);
    const storage = yield* controlledStorage;
    const { harness, root } = yield* openChat(storage, setup, { extensions: [driven] });
    const commits = yield* recordLiveOps(harness);
    const submission = yield* root.submit({ type: "input", content: "go" });
    yield* waitFor(
      Effect.map(live(harness, root), (value) => value?.tools?.[0]?.status === "running"),
    );
    return {
      harness,
      root,
      commits,
      /** Ops of the commit the action caused. */
      step: (action: Action) =>
        Effect.gen(function* () {
          const before = commits.length;
          yield* Queue.offer(actions, action);
          yield* waitFor(Effect.sync(() => commits.length > before));
          return commits.at(-1)!;
        }),
      /** Lets the tool return and the run finish; returns the commits made meanwhile. */
      finish: Effect.gen(function* () {
        const before = commits.length;
        yield* Queue.offer(actions, "return");
        yield* settle(submission);
        return commits.slice(before);
      }),
    };
  });

const OUTPUT: Delta.NonEmptyPath = ["tools", 0, "output"];

const isOutput = (op: Op): boolean => sameJson(op[1], OUTPUT);

const output = (chunk: string): Action =>
  Effect.gen(function* () {
    const api = yield* ToolContext;
    yield* api.output(chunk);
  }).pipe(Effect.orDie);

const details = (value: Schema.Json): Action =>
  Effect.gen(function* () {
    const api = yield* ToolContext;
    yield* api.details(value);
  }).pipe(Effect.orDie);

const diagnostic = (value: AgentTool.ToolDiagnostic): Action =>
  Effect.gen(function* () {
    const api = yield* ToolContext;
    yield* api.diagnostic(value);
  }).pipe(Effect.orDie);

/** A 1000-byte line of the wide-window case. */
const line = (index: number) => `${String(index).padStart(10, "0")} ${"x".repeat(989)}\n`;

const nothingRuns = (value: LiveState) =>
  value.generation === undefined && !(value.tools ?? []).some((slot) => slot.status === "running");

/** Whether `core.live` stores this value as a complete base, many deltas after the last one. */
const base = (value: LiveState) =>
  LiveDoc.definition.checkpointWhen!(value, [], { deltasSinceBase: 1000 });

const slot = (status: "pending" | "running" | "done") => ({ callId: "c", name: "n", status });

describeEachBackend("core.live deltas", (backend) => {
  it.effect(
    "hands a generation over to its tool round and starts a tool with one field write each",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const setup = chatSetup();
          const noop = toolExtension("noop", "noop", () => Effect.succeed({ output: [] }));
          setup.faux.setResponses([
            fauxAssistantMessage([fauxToolCall("noop", {}, { id: "c1" })], {
              stopReason: "toolUse",
            }),
            fauxAssistantMessage([fauxText("done")]),
          ]);
          const storage = yield* controlledStorage;
          const { harness, root } = yield* openChat(storage, setup, { extensions: [noop] });
          const commits = yield* recordLiveOps(harness, true);
          yield* settle(yield* root.submit({ type: "input", content: "go" }));
          const tasks = yield* harness.harness.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              return (yield* tx.scanTasks({ conversationId: root.id }, 20)).items;
            }),
          );
          const id = (kind: string) =>
            tasks.filter((task) => task.kind === kind).map((task) => task.id);
          const [firstGeneration, secondGeneration] = id("core.generation").toSorted(
            (a, b) => a - b,
          );
          const [tool] = id("core.tool");
          const result = (yield* entryIds(root, "core.tool-result"))[0]!.id;
          assert.strictEqual(commits.length, 5);
          // submission
          const submission = commits[0]!;
          assert.strictEqual(submission.length, 1);
          const [setRun] = submission;
          assert.isTrue(setRun?.[0] === "s" && sameJson(setRun[1], ["run"]));
          const runValue = setRun?.[0] === "s" ? setRun[2] : undefined;
          assert.isTrue(Predicate.isObject(runValue) && "inputs" in runValue);
          if (Predicate.isObject(runValue) && "inputs" in runValue && "taskId" in runValue) {
            assert.strictEqual(runValue.taskId, firstGeneration);
            // The run starts at 0 turns, from the first entry its boundary placed.
            assert.strictEqual(runValue["turns"], 0);
            assert.isNumber(runValue["first"]);
            assert.isTrue(Array.isArray(runValue.inputs) && runValue.inputs.length === 1);
            assert.isNumber(Array.isArray(runValue.inputs) ? runValue.inputs[0] : undefined);
          }
          // the generation starts its tool round and keeps the run, counting the round as one turn (#11043); the
          // request's attempt is deferred, so it is stored here, its held operation first
          assert.deepStrictEqual(commits[1]![0], ["s", ["generation"], { attempt: 1 }]);
          assertContains(commits[1], [
            ["d", ["generation"]],
            ["s", ["tools"], [{ callId: "c1", name: "noop", taskId: tool!, status: "pending" }]],
            ["s", ["run", "turns"], 1],
          ]);
          assert.strictEqual(commits[1]!.length, 4);
          // result: the running status is deferred like progress, so it is stored with the result, folded into
          // its write
          assertContains(commits[2], [
            ["s", ["tools", 0, "status"], "done"],
            ["s", ["tools", 0, "entry"], result],
          ]);
          assert.strictEqual(commits[2]!.length, 2);
          // the generation's tools phase hands the run to the next generation
          assertContains(commits[3], [
            ["d", ["tools"]],
            ["s", ["run", "taskId"], secondGeneration!],
          ]);
          assert.strictEqual(commits[3]!.length, 2);
          // the answer ends the run, with the second request's deferred attempt first
          assert.deepStrictEqual(commits[4]![0], ["s", ["generation"], { attempt: 1 }]);
          assertContains(commits[4], [
            ["d", ["run"]],
            ["d", ["generation"]],
          ]);
          assert.strictEqual(commits[4]!.length, 3);
          yield* harness.close;
        }),
      ),
  );

  it.effect(
    "appends head output and then only updates the dropped counts once the window is full",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const driven = yield* drive({ maxLines: 2 });
          assert.deepStrictEqual(yield* driven.step(output("one\n")), [["s", OUTPUT, "one\n"]]);
          assert.deepStrictEqual(yield* driven.step(output("two\n")), [["a", OUTPUT, "two\n"]]);
          // The window is full: the retained text stays; only the counts change.
          assert.deepStrictEqual(yield* driven.step(output("three\n")), [
            ["s", ["tools", 0, "droppedBytes"], 6],
            ["s", ["tools", 0, "droppedLines"], 1],
          ]);
          assert.deepStrictEqual(yield* driven.step(output("four\n")), [
            ["s", ["tools", 0, "droppedBytes"], 11],
            ["s", ["tools", 0, "droppedLines"], 2],
          ]);
          yield* driven.finish;
          yield* driven.harness.close;
        }),
      ),
  );

  it.effect("slides a tail window as a front trim plus an append", () =>
    run(backend)(
      Effect.gen(function* () {
        const driven = yield* drive({ maxLines: 3, retain: "tail" });
        assert.deepStrictEqual(yield* driven.step(output("line 1\nline 2\nline 3\n")), [
          ["s", OUTPUT, "line 1\nline 2\nline 3\n"],
        ]);
        assert.deepStrictEqual(yield* driven.step(output("line 4\n")), [
          ["t", OUTPUT, 7],
          ["a", OUTPUT, "line 4\n"],
          ["s", ["tools", 0, "droppedBytes"], 7],
          ["s", ["tools", 0, "droppedLines"], 1],
        ]);
        // The buffer keeps only the window, and later slides stay minimal and exact.
        assert.deepStrictEqual(yield* driven.step(output("line 5\nline 6\n")), [
          ["t", OUTPUT, 14],
          ["a", OUTPUT, "line 5\nline 6\n"],
          ["s", ["tools", 0, "droppedBytes"], 21],
          ["s", ["tools", 0, "droppedLines"], 3],
        ]);
        assert.strictEqual(
          (yield* live(driven.harness, driven.root))?.tools?.[0]?.output,
          "line 4\nline 5\nline 6\n",
        );
        yield* driven.finish;
        yield* driven.harness.close;
      }),
    ),
  );

  it.effect(
    "writes the whole window when the delta's overlap search cannot find the shared part",
    () =>
      run(backend)(
        Effect.gen(function* () {
          // A retained window beyond the 64 KiB overlap scan.
          const wide = yield* drive({ maxBytes: 100 * 1024, maxLines: 1_000_000, retain: "tail" });
          let text = "";
          for (let index = 0; index < 100; index++) text += line(index);
          yield* wide.step(output(text));
          const slid = yield* wide.step(output(line(100) + line(101) + line(102) + line(103)));
          assert.deepStrictEqual(
            slid.filter(isOutput).map((op) => op[0]),
            ["s"],
          );
          yield* wide.finish;
          yield* wide.harness.close;
        }),
      ).pipe(
        Effect.andThen(
          run(backend)(
            Effect.gen(function* () {
              // Repetitive output still finds an overlap here; the bounded candidate search can give up on other
              // inputs and then writes one window.
              const repetitive = yield* drive({ maxLines: 50, retain: "tail" });
              yield* repetitive.step(output("y\n".repeat(50)));
              const repeated = yield* repetitive.step(output("z\n"));
              assert.deepStrictEqual(repeated.filter(isOutput), [
                ["t", OUTPUT, 2],
                ["a", OUTPUT, "z\n"],
              ]);
              yield* repetitive.finish;
              yield* repetitive.harness.close;
            }),
          ),
        ),
      ),
  );

  it.effect("diffs details leaf by leaf and appends diagnostics", () =>
    run(backend)(
      Effect.gen(function* () {
        const driven = yield* drive();
        const detailsPath: Delta.NonEmptyPath = ["tools", 0, "details"];
        assert.deepStrictEqual(yield* driven.step(details({ step: 1, log: "a" })), [
          ["s", detailsPath, { step: 1, log: "a" }],
        ]);
        assertContains(yield* driven.step(details({ step: 2, log: "ab" })), [
          ["s", [...detailsPath, "step"], 2],
          ["a", [...detailsPath, "log"], "b"],
        ]);
        assert.deepStrictEqual(yield* driven.step(details({ step: 2 })), [
          ["d", [...detailsPath, "log"]],
        ]);
        const diagnosticsPath: Delta.NonEmptyPath = ["tools", 0, "diagnostics"];
        const first = { severity: "info", message: "first" } as const;
        const second = { severity: "warn", message: "second" } as const;
        assert.deepStrictEqual(yield* driven.step(diagnostic(first)), [
          ["s", diagnosticsPath, [first]],
        ]);
        assert.deepStrictEqual(yield* driven.step(diagnostic(second)), [
          ["p", diagnosticsPath, 1, 0, [second]],
        ]);
        // Settlement moves everything into the result entry and keeps the slot small.
        const [settled] = yield* driven.finish;
        assertContains(settled, [
          ["s", ["tools", 0, "status"], "done"],
          ["d", detailsPath],
          ["d", diagnosticsPath],
        ]);
        yield* driven.harness.close;
      }),
    ),
  );

  it.effect("streams partial text as appends", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup({ tokensPerSecond: 400, tokenSize: { min: 4, max: 4 } });
        setup.faux.setResponses([fauxAssistantMessage([fauxText("word ".repeat(250))])]);
        const storage = yield* controlledStorage;
        const { harness, root } = yield* openChat(storage, setup);
        const commits = yield* recordLiveOps(harness);
        yield* settle(yield* root.submit({ type: "input", content: "go" }));
        const partials = commits.slice(2, -1);
        assert.isAbove(partials.length, 2);
        const [first] = partials[0]!;
        assert.strictEqual(partials[0]!.length, 1);
        assert.isTrue(first?.[0] === "s" && sameJson(first[1], ["generation", "message"]));
        assert.isTrue(first?.[0] === "s" && Predicate.isObject(first[2]));
        for (const ops of partials.slice(1)) {
          assert.strictEqual(ops.length, 1);
          const [append] = ops;
          assert.isTrue(
            append?.[0] === "a" &&
              sameJson(append[1], ["generation", "message", "content", 0, "text"]) &&
              Predicate.isString(append[2]),
          );
        }
        yield* harness.close;
      }),
    ),
  );

  it.effect("stores a complete base exactly in the commits where nothing runs", () =>
    run(backend)(
      Effect.gen(function* () {
        // Storage that remembers whether each commit wrote core.live as a base or a delta.
        const written = new Map<number, "base" | "delta">();
        let liveId: number | undefined;
        const storage = yield* controlledStorage;
        storage.intercept.receive = (writes, seq) => {
          let kind: "base" | "delta" | undefined;
          for (const write of writes) {
            if (write.type === "document.change" && write.id === liveId) kind = write.content.kind;
          }
          if (kind !== undefined) written.set(seq, kind);
        };
        const setup = chatSetup();
        const extensions = ["first", "second"].map((name) =>
          toolExtension(name, name, () =>
            Effect.gen(function* () {
              const api = yield* ToolContext;
              yield* api.output(`${name} output\n`);
              yield* Effect.sleep("150 millis");
              yield* api.output(`${name} more\n`);
              return {};
            }),
          ),
        );
        setup.faux.setResponses([
          fauxAssistantMessage(
            [fauxToolCall("first", {}, { id: "a" }), fauxToolCall("second", {}, { id: "b" })],
            { stopReason: "toolUse" },
          ),
          fauxAssistantMessage([fauxText("done")]),
        ]);
        setup.settings.toolExecution = "sequential";
        // Store deferred progress within each tool's run, so running slots reach Storage as deltas.
        setup.settings.progress = { persistWithinMs: 50 };
        const { harness, root } = yield* openChat(storage, setup, { extensions });
        const values = new Map<number, LiveState>();
        yield* harness.session.observeCommits({
          advance: (publication) => {
            for (const change of documentChanges(publication)) {
              if (change.record.kind !== "core.live") continue;
              liveId = change.record.id;
              if (change.value !== null) {
                values.set(publication.seq, Schema.decodeUnknownSync(LiveState)(change.value));
              }
            }
          },
          end: () => {},
        });
        yield* settle(yield* root.submit({ type: "input", content: "go" }));
        const kinds = [...written].map(([seq, kind]) => {
          assert.strictEqual(kind === "base", nothingRuns(values.get(seq)!));
          return kind;
        });
        // Bases at the handover, after each sequential tool, after the round, and when the run ends.
        assert.isAtLeast(kinds.filter((kind) => kind === "base").length, 5);
        assert.include(kinds, "delta");
        yield* harness.close;
      }),
    ),
  );

  // The tool's result carries a content block that is JSON but no valid model content, which the result commit
  // rejects, and the scheduler faults the task.
  it.effect(
    "starts calls the request did not offer as flagged tasks and marks a faulted tool's slot done without an entry",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const setup = chatSetup();
          // A JSON block that is no valid model content, the fault this case injects: a text block without its text.
          const block: TextContent = { type: "text", text: "" };
          Reflect.deleteProperty(block, "text");
          const broken: ToolResultContent = [block];
          const bad = toolExtension("bad", "bad", () => Effect.succeed({ output: broken }));
          setup.faux.setResponses([
            fauxAssistantMessage(
              [fauxToolCall("ghost", {}, { id: "g" }), fauxToolCall("bad", {}, { id: "b" })],
              { stopReason: "toolUse" },
            ),
            fauxAssistantMessage([fauxText("done")]),
          ]);
          const storage = yield* controlledStorage;
          const { harness, root } = yield* openChat(storage, setup, { extensions: [bad] });
          const commits = yield* recordLiveOps(harness);
          yield* settle(yield* root.submit({ type: "input", content: "go" }));
          const handover = commits.find((ops) =>
            ops.some((op) => op[0] === "s" && op[1][0] === "tools"),
          )!;
          const slots = handover.find((op) => op[0] === "s" && sameJson(op[1], ["tools"]));
          const value = slots?.[0] === "s" ? slots[2] : undefined;
          assert.isTrue(Array.isArray(value) && value.length === 2);
          // #11043: an unoffered call gets a task too, flagged on its slot.
          if (Array.isArray(value)) {
            const [first, second] = value;
            assert.isTrue(Predicate.isObject(first) && Predicate.isObject(second));
            if (Predicate.isObject(first) && Predicate.isObject(second)) {
              assert.isNumber(first["taskId"]);
              assert.isNumber(second["taskId"]);
              assert.deepStrictEqual(first, {
                callId: "g",
                name: "ghost",
                taskId: first["taskId"],
                status: "pending",
                unoffered: true,
              });
              assert.deepStrictEqual(second, {
                callId: "b",
                name: "bad",
                taskId: second["taskId"],
                status: "pending",
              });
            }
          }
          // The fault cleanup writes only the status.
          assert.isTrue(
            commits.some((ops) => sameJson(ops, [["s", ["tools", 1, "status"], "done"]])),
          );
          yield* harness.close;
        }),
      ),
  );

  it.effect(
    "commits the tool-calling answer, its tool tasks, the generation's wait, and the tool round in one commit",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const setup = chatSetup();
          const noop = toolExtension("noop", "noop", () => Effect.succeed({ output: [] }));
          setup.faux.setResponses([
            fauxAssistantMessage(
              [fauxToolCall("noop", {}, { id: "a" }), fauxToolCall("noop", {}, { id: "b" })],
              { stopReason: "toolUse" },
            ),
            fauxAssistantMessage([fauxText("done")]),
          ]);
          const storage = yield* controlledStorage;
          const { harness, root } = yield* openChat(storage, setup, { extensions: [noop] });
          let handover: CommitPublication | undefined;
          yield* harness.session.observeCommits({
            advance: (publication) => {
              for (const change of documentChanges(publication)) {
                const tools = change.value?.["tools"];
                if (
                  change.record.kind === "core.live" &&
                  Array.isArray(tools) &&
                  tools.length === 2
                ) {
                  handover ??= publication;
                }
              }
            },
            end: () => {},
          });
          yield* settle(yield* root.submit({ type: "input", content: "go" }));
          const kinds = handover!.changes.flatMap((change) =>
            change.type === "entry" || change.type === "task" ? [change.value.kind] : [],
          );
          assert.deepStrictEqual(kinds.toSorted(), [
            "core.assistant",
            "core.generation",
            "core.tool",
            "core.tool",
          ]);
          yield* harness.close;
        }),
      ),
  );

  it.effect("writes an aborted tool's slot with field-level ops", () =>
    run(backend)(
      Effect.gen(function* () {
        const driven = yield* drive();
        yield* driven.step(output("partial\n"));
        yield* driven.step(details({ n: 1 }));
        const taskId = (yield* live(driven.harness, driven.root))!.tools![0]!.taskId!;
        const before = driven.commits.length;
        yield* driven.harness.harness.abortTask(taskId);
        const isAbortCommit = (ops: ReadonlyArray<Op>) =>
          ops.some((op) => op[0] === "s" && sameJson(op[1], ["tools", 0, "status"]));
        yield* waitFor(Effect.sync(() => driven.commits.slice(before).some(isAbortCommit)));
        const abortCommit = driven.commits.slice(before).find(isAbortCommit)!;
        assertContains(abortCommit, [
          ["s", ["tools", 0, "status"], "done"],
          ["d", OUTPUT],
          ["d", ["tools", 0, "details"]],
        ]);
        const entry = abortCommit.find(
          (op) => op[0] === "s" && sameJson(op[1], ["tools", 0, "entry"]),
        );
        assert.isNumber(entry?.[0] === "s" ? entry[2] : undefined);
        assert.strictEqual(abortCommit.length, 4);
        yield* driven.harness.close;
      }),
    ),
  );

  it("keeps a complete base exactly while nothing runs", () => {
    const runControl = { taskId: TaskId.make(1), inputs: [], turns: 0 };
    assert.isTrue(base({}));
    assert.isFalse(base({ run: runControl, generation: { attempt: 1 } }));
    assert.isTrue(base({ run: runControl, tools: [slot("pending"), slot("done")] }));
    assert.isFalse(base({ run: runControl, tools: [slot("done"), slot("running")] }));
    assert.isTrue(base({ run: runControl }));
  });
});

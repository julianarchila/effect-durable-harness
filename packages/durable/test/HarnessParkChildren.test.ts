// Work a parked tool owns: nested calls started without waiting, tasks created under keys, which children a
// restart keeps, and submissions that signal a task when they settle.
//
// Notes:
// - A reopened process is a second Harness over the same storage, on every backend; a crash kills the first one's
//   storage (`HarnessSupport.killable`).
// - The abort reaches a running tool as its interruption: "cut off while it ran" blocks in `Effect.never`.
// - The registry is static: "a submission created settled, before the task ran" opens a Harness without the
//   waiter's definition, so it stays blocked, then reopens with it.
// - Values that are no settled record are passed through a cast, as a value from outside may be.
import { assert, it } from "@effect/vitest";
import * as Clock from "effect/Clock";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as Predicate from "effect/Predicate";
import * as Schema from "effect/Schema";
import { AgentStorage } from "../src/AgentStorage.ts";
import * as AgentTool from "../src/AgentTool.ts";
import { ToolResultEntry } from "../src/Entry.ts";
import * as Extension from "../src/Extension.ts";
import type { Conversation } from "../src/Harness.ts";
import * as Hook from "../src/Hook.ts";
import type { SubmissionId, TaskId } from "../src/Ids.ts";
import { LiveDoc } from "../src/Live.ts";
import type { AssistantMessage, ToolResultMessage } from "../src/Message.ts";
import type { EntryRecord, SettledSubmissionRecord, WakeReason } from "../src/Records.ts";
import * as Signal from "../src/Signal.ts";
import type { Submission } from "../src/Submission.ts";
import * as Task from "../src/Task.ts";
import { fauxAssistantMessage, fauxText, fauxToolCall } from "../src/testing/FauxModel.ts";
import { Tx } from "../src/Tx.ts";
import { allEntries, type ChatSetup, chatSetup, openChat, waitFor } from "./ChatSupport.ts";
import { abortedWith, killable, type TestHarness } from "./HarnessSupport.ts";
import {
  type ControlledStorage,
  controlledStorage,
  describeEachBackend,
  run,
} from "./SessionSupport.ts";

/** Sleeps on `SubmissionSettled` under `settled`, then completes with the settled record's status. */
const Waiter = Task.make({
  name: "test.waiter",
  version: 1,
  input: Schema.Null,
  checkpoint: Schema.Union([
    Schema.Struct({ phase: Schema.Literal("sleep") }),
    Schema.Struct({ phase: Schema.Literal("woke") }),
  ]),
  result: Schema.NullOr(Schema.String),
  initial: () => ({ phase: "sleep" }),
  phases: {
    sleep: (_task, runtime) =>
      runtime.sleep({
        checkpoint: { phase: "woke" },
        signals: [Signal.SubmissionSettled.key("settled")],
      }),
    woke: (_task, runtime) =>
      Effect.gen(function* () {
        const state = Option.getOrUndefined(
          yield* runtime.readSignal(Signal.SubmissionSettled, "settled"),
        );
        yield* runtime.complete(
          state !== undefined && "value" in state ? state.value.status : null,
        );
      }),
  },
  abort: (_task, runtime) => runtime.commit(Effect.succeed(abortedWith("test"))),
});

const tasks = (...list: ReadonlyArray<Task.Any>) => [
  Extension.make({ name: "tasks", tasks: list }),
];

const create = <S extends { readonly phase: string }, R>(
  root: { readonly commit: TestHarness["harness"]["commit"] },
  task: Task.Task<null, S, R, never>,
) =>
  root.commit(
    Effect.gen(function* () {
      const tx = yield* Tx;
      return yield* tx.createTask(task, null, { ownership: { kind: "conversation" } });
    }),
  );

const statusOf = (h: TestHarness, id: TaskId) =>
  h.harness.getTask(id).pipe(Effect.map((record) => Option.getOrUndefined(record)?.state.status));

describeEachBackend("submissions that notify a task", (backend) => {
  it.effect("deliver SubmissionSettled in the commit that settles the submission", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        const gate = Deferred.makeUnsafe<void>();
        setup.faux.setResponses([
          () => Deferred.await(gate).pipe(Effect.as(fauxAssistantMessage("answer"))),
        ]);
        const storage = yield* controlledStorage;
        const { harness, root } = yield* openChat(storage, setup, { extensions: tasks(Waiter) });
        const waiter = yield* create(root, Waiter);
        yield* harness.harness.resume;
        yield* waitFor(
          statusOf(harness, waiter).pipe(Effect.map((status) => status === "sleeping")),
        );
        const submission = yield* root.submit({
          type: "input",
          content: "go",
          notify: { taskId: waiter, key: "settled" },
        });
        yield* Deferred.succeed(gate, undefined);
        assert.strictEqual((yield* submission.await).status, "done");
        assert.deepStrictEqual((yield* harness.harness.awaitTask(waiter)).state.outcome, {
          status: "completed",
          result: "done",
        });
        // One batch settles the submission and stores the signal in the waiter.
        const settling = storage.commits.find((batch) =>
          batch.some(
            (write) =>
              write.type === "submission" &&
              write.value.id === submission.id &&
              write.value.status === "done",
          ),
        );
        const signalled = settling?.find(
          (write) => write.type === "task" && write.value.id === waiter,
        );
        const memos =
          signalled?.type === "task" && "memos" in signalled.value ? signalled.value.memos : {};
        assert.property(memos, "core.signal.core.submission-settled/settled");
        yield* harness.close;
      }),
    ),
  );

  it.effect(
    "deliver it for a submission created settled, before the task ran, which then wakes at once",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const setup = chatSetup();
          const storage = yield* controlledStorage;
          const first = yield* openChat(storage, setup);
          // Blocked without its definition, so it has not run when the write settles at its admission.
          const waiter = yield* create(first.root, Waiter);
          const write = yield* first.root.submit({
            type: "write",
            entry: { kind: "test.note", data: "hi" },
            notify: { taskId: waiter, key: "settled" },
          });
          assert.strictEqual((yield* write.status).status, "done");
          const record = Option.getOrUndefined(yield* first.harness.harness.getTask(waiter));
          assert.isUndefined(record?.startedAt);
          yield* first.harness.close;
          const { harness } = yield* openChat(storage, setup, { extensions: tasks(Waiter) });
          assert.deepStrictEqual((yield* harness.harness.awaitTask(waiter)).state.outcome, {
            status: "completed",
            result: "done",
          });
          yield* harness.close;
        }),
      ),
  );

  it.effect(
    "compose in one commit: two queued writes placed together signal one task under two keys",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const setup = chatSetup();
          const gate = Deferred.makeUnsafe<void>();
          setup.faux.setResponses([
            () => Deferred.await(gate).pipe(Effect.as(fauxAssistantMessage("answer"))),
          ]);
          const Two = Task.make({
            name: "test.two-keys",
            version: 1,
            input: Schema.Null,
            checkpoint: Schema.Union([
              Schema.Struct({ phase: Schema.Literal("sleep") }),
              Schema.Struct({ phase: Schema.Literal("woke") }),
            ]),
            result: Schema.Null,
            initial: () => ({ phase: "sleep" }),
            phases: {
              sleep: (_task, runtime) =>
                runtime.sleep({
                  checkpoint: { phase: "woke" },
                  signals: [
                    Signal.SubmissionSettled.key("one"),
                    Signal.SubmissionSettled.key("two"),
                  ],
                }),
              woke: (_task, runtime) => runtime.complete(null),
            },
            abort: (_task, runtime) => runtime.commit(Effect.succeed(abortedWith("test"))),
          });
          const storage = yield* controlledStorage;
          const { harness, root } = yield* openChat(storage, setup, { extensions: tasks(Two) });
          const waiter = yield* create(root, Two);
          yield* harness.harness.resume;
          yield* waitFor(
            statusOf(harness, waiter).pipe(Effect.map((status) => status === "sleeping")),
          );
          // Busy: the writes, which the model sees, queue until the run's final boundary places both.
          const runSubmission = yield* root.submit({ type: "input", content: "go" });
          const note = (key: string) =>
            root.submit({
              type: "write",
              entry: { kind: "test.note", model: [{ role: "user", content: key, timestamp: 0 }] },
              notify: { taskId: waiter, key },
            });
          const one = yield* note("one");
          const two = yield* note("two");
          yield* Deferred.succeed(gate, undefined);
          yield* runSubmission.await;
          assert.deepStrictEqual(
            [(yield* one.await).status, (yield* two.await).status],
            ["done", "done"],
          );
          const placing = storage.commits.find((batch) =>
            batch.some(
              (write) =>
                write.type === "submission" &&
                write.value.id === two.id &&
                write.value.status === "done",
            ),
          );
          // Both placed in this batch, and the waiter written once: one transaction composed both deliveries.
          assert.isTrue(
            placing?.some(
              (write) =>
                write.type === "submission" &&
                write.value.id === one.id &&
                write.value.status === "done",
            ),
          );
          const writes =
            placing?.filter((write) => write.type === "task" && write.value.id === waiter) ?? [];
          assert.strictEqual(writes.length, 1);
          const signalled = writes[0];
          const memos =
            signalled?.type === "task" && "memos" in signalled.value
              ? Object.keys(signalled.value.memos ?? {})
              : [];
          assert.deepStrictEqual(memos, [
            "core.signal.core.submission-settled/one",
            "core.signal.core.submission-settled/two",
          ]);
          yield* harness.harness.awaitTask(waiter);
          yield* harness.close;
        }),
      ),
  );

  it.effect("refuse a delivery of SubmissionSettled that is no settled record", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        const { harness, root } = yield* openChat(yield* controlledStorage, setup, {
          extensions: tasks(Waiter),
        });
        const waiter = yield* create(root, Waiter);
        const deliver = (value: Schema.Json) =>
          harness.harness.signal(
            waiter,
            Signal.SubmissionSettled,
            "settled",
            // SAFETY: deliberately unchecked: a delivery must refuse what its schema rejects.
            value as SettledSubmissionRecord,
          );
        assert.strictEqual(yield* deliver(17), "invalid");
        // A done input without its entry and answer is no settled record either.
        assert.strictEqual(
          yield* deliver({ id: 2, conversationId: 1, type: "input", status: "done" }),
          "invalid",
        );
        assert.strictEqual(
          yield* deliver({
            id: 2,
            conversationId: 1,
            type: "input",
            status: "unanswered",
            reason: "x",
            entry: "e1",
          }),
          "invalid",
        );
        yield* harness.harness.abortTask(waiter);
        yield* harness.close;
      }),
    ),
  );

  it.effect("settle without failing when the task to notify is gone", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        setup.faux.setResponses([fauxAssistantMessage("answer")]);
        const { harness, root } = yield* openChat(yield* controlledStorage, setup);
        const submission = yield* root.submit({
          type: "input",
          content: "go",
          // SAFETY: an ID no task has.
          notify: { taskId: 999_999 as TaskId, key: "settled" },
        });
        assert.strictEqual((yield* submission.await).status, "done");
        yield* harness.close;
      }),
    ),
  );
});

// ─── Nested calls, keyed tasks, and children after a restart ───────────────────

const Done = Signal.make("test.done", {
  version: 1,
  schema: Schema.Struct({ text: Schema.String }),
});

const Topic = Schema.Struct({ topic: Schema.String });
type TopicExecute = AgentTool.ToolDefinition<
  typeof Topic.Type,
  Schema.Json,
  unknown,
  AgentTool.ToolContext
>["execute"];

/** A tool whose arguments are a topic. */
const topicTool = (name: string, execute: TopicExecute, replay?: AgentTool.ReplayPolicy) =>
  AgentTool.make(name, {
    description: name,
    parameters: Topic,
    execute,
    ...(replay === undefined ? undefined : { replay }),
  });

const call = (name: string, args: Record<string, string> = { topic: "" }): AssistantMessage =>
  fauxAssistantMessage([fauxToolCall(name, args, { id: "c1" })], { stopReason: "toolUse" });

const DONE = fauxAssistantMessage([fauxText("done")]);

const results = (entries: ReadonlyArray<EntryRecord>): Array<ToolResultMessage> =>
  entries.flatMap((entry) => {
    const message = entry.model?.[0];
    return ToolResultEntry.is(entry) && message?.role === "toolResult" ? [message] : [];
  });

const text = (message: ToolResultMessage | undefined): string =>
  (message?.content ?? []).map((item) => (item.type === "text" ? item.text : "")).join("|");

/** A nested tool that parks on `Done` under the key `done` until it holds a value, and answers with its text. */
const research = topicTool(
  "research",
  (args, api) =>
    Effect.map(api.readSignal(Done, "done"), (state) => {
      const value = Option.getOrUndefined(state);
      return value !== undefined && "value" in value
        ? AgentTool.text(`${args.topic}: ${value.value.text}`)
        : AgentTool.sleep({ signals: [Done.key("done")] });
    }),
  "safe",
);

/** A Harness over `storage` that schedules at once. */
const openTools = (
  storage: ControlledStorage,
  setup: ChatSetup,
  extensions: ReadonlyArray<Extension.Any>,
) =>
  Effect.gen(function* () {
    const opened = yield* openChat(storage, setup, { extensions });
    yield* opened.harness.harness.resume;
    return opened;
  });

/** The nested calls of `root` that are parked, by call ID, once there are `count`. */
const parkedNested = (h: TestHarness, root: Conversation, count: number) =>
  Effect.gen(function* () {
    let parked = new Map<string, TaskId>();
    yield* waitFor(
      Effect.map(h.session.snapshot(LiveDoc, root.id), (live) => {
        const slots = Option.getOrUndefined(live)?.nestedTools ?? [];
        parked = new Map(
          slots.flatMap((slot) => (slot.status === "sleeping" ? [[slot.callId, slot.taskId]] : [])),
        );
        return parked.size === count;
      }),
    );
    return parked;
  });

/** Awaits the submission while the `TestClock` advances in 5 ms steps. */
const settle = (submission: Submission) =>
  Effect.gen(function* () {
    const fiber = yield* Effect.forkChild(submission.await, { startImmediately: true });
    yield* waitFor(Effect.sync(() => fiber.pollUnsafe() !== undefined));
    return yield* Fiber.join(fiber);
  });

/** Awaits a submission found by ID, as `settle` does. */
const settleById = (h: TestHarness, id: SubmissionId) =>
  Effect.flatMap(h.harness.submission(id), (found) => settle(Option.getOrThrow(found)));

/** Whether the Session needs no process now: nothing runs and no sleep is due. */
const inactive = (h: TestHarness) =>
  waitFor(
    Effect.gen(function* () {
      const now = yield* Clock.currentTimeMillis;
      const wake = yield* h.harness.wake;
      const next = Option.getOrUndefined(wake.nextWakeAt);
      return !wake.activeWork && (next === undefined || next > now);
    }),
  );

/** A task that completes at once with `null`. */
const quick = <I extends Schema.Json>(name: string, input: Schema.Codec<I, I>) =>
  Task.make({
    name,
    version: 1,
    input,
    checkpoint: Schema.Struct({ phase: Schema.Literal("run") }),
    result: Schema.Null,
    initial: () => ({ phase: "run" }),
    phases: { run: (_task, runtime) => runtime.complete(null) },
    abort: (_task, runtime) => runtime.commit(Effect.succeed(abortedWith("test"))),
  });

describeEachBackend("nested calls without waiting", (backend) => {
  it.effect(
    "park on their handles with policy any and collect the results by key across wakes",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const setup = chatSetup();
          const wakes: Array<WakeReason | undefined> = [];
          const DoneSoFar = Schema.Struct({ done: Schema.Array(Schema.Number) });
          const both = topicTool(
            "both",
            (_args, api) =>
              Effect.gen(function* () {
                wakes.push(api.wake);
                const a = yield* api.executeTool(
                  "research",
                  { topic: "x" },
                  { key: "a", wait: false },
                );
                const b = yield* api.executeTool(
                  "research",
                  { topic: "y" },
                  { key: "b", wait: false },
                );
                const checkpoint =
                  api.checkpoint === undefined
                    ? undefined
                    : yield* Schema.decodeUnknownEffect(DoneSoFar)(api.checkpoint);
                const done = new Set<number>(checkpoint?.done ?? []);
                if (api.wake?.reason === "tasks") for (const id of api.wake.ended) done.add(id);
                const open = [a, b].filter((nested) => !done.has(nested.taskId));
                if (open.length > 0) {
                  return AgentTool.wait({
                    checkpoint: { done: [...done] },
                    on: open.map((nested) => nested.taskId),
                    policy: "any",
                  });
                }
                const [ra, rb] = yield* Effect.all([a.result, b.result]);
                return AgentTool.text(
                  `${String(ra.structuredOutput)} + ${String(rb.structuredOutput)}`,
                );
              }),
            "safe",
          );
          setup.faux.setResponses([call("both"), DONE]);
          const { harness, root } = yield* openTools(yield* controlledStorage, setup, [
            Extension.make({ name: "tools", tools: [research, both] }),
          ]);
          const submission = yield* root.submit({ type: "input", content: "go" });
          const parked = yield* parkedNested(harness, root, 2);
          yield* inactive(harness);
          yield* harness.harness.signal(parked.get("c1/b")!, Done, "done", { text: "bee" });
          yield* waitFor(
            Effect.map(
              harness.session.snapshot(LiveDoc, root.id),
              (live) => Option.getOrUndefined(live)?.tools?.[0]?.status === "waiting",
            ),
          );
          yield* harness.harness.signal(parked.get("c1/a")!, Done, "done", { text: "ay" });
          assert.strictEqual((yield* settle(submission)).status, "done");
          assert.strictEqual(text(results(yield* allEntries(root))[0]), "x: ay + y: bee");
          assert.deepStrictEqual(wakes, [
            undefined,
            { reason: "tasks", ended: [parked.get("c1/b")!] },
            { reason: "tasks", ended: [parked.get("c1/a")!] },
          ]);
          yield* harness.close;
        }),
      ),
  );

  it.effect("need explicit keys after a wake, for nested calls and tasks alike", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        const Idle = quick("test.idle", Schema.Null);
        const errors: Array<string> = [];
        const woken = topicTool("woken", (_args, api) =>
          Effect.gen(function* () {
            if (api.wake === undefined) return AgentTool.sleep({ until: 0 });
            yield* api
              .executeTool("research", { topic: "x" }, { wait: false })
              .pipe(
                Effect.catchTag("InvalidNestedCall", (error) =>
                  Effect.sync(() => errors.push(error.message)),
                ),
              );
            yield* api
              .createTask(Idle, null, { ownership: { kind: "task", taskId: api.taskId } })
              .pipe(
                Effect.catchTag("InvalidNestedCall", (error) =>
                  Effect.sync(() => errors.push(error.message)),
                ),
              );
            return {};
          }),
        );
        setup.faux.setResponses([call("woken"), DONE]);
        const { harness, root } = yield* openTools(yield* controlledStorage, setup, [
          Extension.make({ name: "tools", tools: [research, woken], tasks: [Idle] }),
        ]);
        yield* settle(yield* root.submit({ type: "input", content: "go" }));
        assert.deepStrictEqual(errors, [
          "Tool call c1 needs explicit keys after a wake",
          "Tool call c1 needs explicit keys after a wake",
        ]);
        yield* harness.close;
      }),
    ),
  );
});

describeEachBackend("tasks created under a key", (backend) => {
  it.effect(
    "return the task a key names, and refuse another kind, other input, or a nested call's key",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const setup = chatSetup();
          const Child = quick("test.keyed", Schema.Struct({ n: Schema.Number }));
          const seen: Array<TaskId | string> = [];
          const keyed = topicTool("keyed", (_args, api) =>
            Effect.gen(function* () {
              const own = { ownership: { kind: "task", taskId: api.taskId } } as const;
              const attempt = <
                A extends TaskId | AgentTool.NestedToolCall,
                E extends { readonly _tag: string },
              >(
                effect: Effect.Effect<A, E>,
              ) =>
                effect.pipe(
                  Effect.map((value) => {
                    seen.push(Predicate.isNumber(value) ? value : value.taskId);
                  }),
                  Effect.catchIf(
                    (error): error is E & AgentTool.InvalidNestedCall =>
                      error._tag === "InvalidNestedCall",
                    (error) => Effect.sync(() => seen.push(error.message)),
                  ),
                );
              yield* attempt(api.createTask(Child, { n: 1 }, { ...own, key: "k" }));
              yield* attempt(api.createTask(Child, { n: 1 }, { ...own, key: "k" }));
              yield* attempt(api.createTask(Child, { n: 2 }, { ...own, key: "k" }));
              yield* api.executeTool("research", { topic: "x" }, { key: "n", wait: false });
              yield* attempt(api.createTask(Child, { n: 1 }, { ...own, key: "n" }));
              yield* attempt(
                api.executeTool("research", { topic: "x" }, { key: "k", wait: false }),
              );
              return {};
            }),
          );
          setup.faux.setResponses([call("keyed"), DONE]);
          const { harness, root } = yield* openTools(yield* controlledStorage, setup, [
            Extension.make({ name: "tools", tools: [research, keyed], tasks: [Child] }),
          ]);
          yield* settle(yield* root.submit({ type: "input", content: "go" }));
          const [first, again, ...refused] = seen;
          assert.isTrue(Predicate.isNumber(first));
          assert.strictEqual(again, first);
          assert.deepStrictEqual(refused, [
            "Key k of tool call c1 was already used for another call or task",
            "Key n of tool call c1 was already used for another call or task",
            "Nested call c1/k was already made with another tool or other arguments",
          ]);
          yield* harness.close;
        }),
      ),
  );
});

describeEachBackend("children after a restart", (backend) => {
  it.effect("keeps the children of a parked replay-unsafe call", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        const unsafe = topicTool("unsafe", (_args, api) =>
          Effect.gen(function* () {
            const nested = yield* api.executeTool(
              "research",
              { topic: "x" },
              { key: "a", wait: false },
            );
            if (api.wake === undefined)
              return AgentTool.wait({ on: [nested.taskId], policy: "allSettled" });
            return AgentTool.text(String((yield* nested.result).structuredOutput));
          }),
        );
        const extensions = [Extension.make({ name: "tools", tools: [research, unsafe] })];
        setup.faux.setResponses([call("unsafe"), DONE]);
        const first = yield* openTools(yield* controlledStorage, setup, extensions);
        const submissionId = (yield* first.root.submit({ type: "input", content: "go" })).id;
        const parked = yield* parkedNested(first.harness, first.root, 1);
        yield* first.harness.close;
        const second = yield* openTools(yield* controlledStorage, setup, extensions);
        const child = parked.get("c1/a")!;
        yield* inactive(second.harness);
        // Not abandoned: its owner was parked, not cut off while running.
        assert.notStrictEqual(
          Option.getOrUndefined(yield* second.harness.harness.getTask(child))?.abortRequested,
          true,
        );
        yield* second.harness.harness.signal(child, Done, "done", { text: "kept" });
        assert.strictEqual((yield* settleById(second.harness, submissionId)).status, "done");
        assert.strictEqual(text(results(yield* allEntries(second.root))[0]), "x: kept");
        yield* second.harness.close;
      }),
    ),
  );

  it.effect("abandons the parked children of a replay-unsafe call cut off while it ran", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        const nestedResults: Array<string> = [];
        const hooks = Hook.tool({
          afterTool: (hooked, result) =>
            Effect.sync(() => {
              if (hooked.parent !== undefined) {
                nestedResults.push(
                  ...(result.diagnostics ?? []).map((diagnostic) => diagnostic.message),
                );
              }
              return undefined;
            }),
        });
        const started = Deferred.makeUnsafe<void>();
        const unsafe = topicTool("unsafe", (_args, api) =>
          Effect.gen(function* () {
            yield* api.executeTool("research", { topic: "x" }, { key: "a", wait: false });
            yield* Deferred.succeed(started, undefined);
            return yield* Effect.never;
          }),
        );
        const extensions = [
          Extension.make({ name: "tools", tools: [research, unsafe], hooks: [hooks] }),
        ];
        setup.faux.setResponses([call("unsafe"), DONE]);
        const crashable = yield* killable(yield* AgentStorage);
        const crashed = yield* openTools(crashable.storage, setup, extensions);
        const submissionId = (yield* crashed.root.submit({ type: "input", content: "go" })).id;
        yield* waitFor(Deferred.isDone(started));
        const parked = yield* parkedNested(crashed.harness, crashed.root, 1);
        // The process dies here: another one opens the same storage.
        yield* crashable.kill;
        yield* crashed.harness.close;
        const next = yield* openTools(yield* controlledStorage, setup, extensions);
        assert.strictEqual((yield* settleById(next.harness, submissionId)).status, "done");
        const child = Option.getOrUndefined(
          yield* next.harness.harness.getTask(parked.get("c1/a")!),
        );
        assert.strictEqual(child?.abortReason, "restart");
        assert.strictEqual(
          child?.state.status === "terminal" ? child.state.outcome.status : undefined,
          "aborted",
        );
        assert.deepStrictEqual(nestedResults, [
          "Tool research was parked: its caller ended with a restart",
        ]);
        const stored = Option.getOrUndefined(
          yield* next.harness.session.snapshot(LiveDoc, next.root.id),
        );
        assert.isUndefined(stored?.nestedTools);
        yield* next.harness.close;
      }),
    ),
  );
});

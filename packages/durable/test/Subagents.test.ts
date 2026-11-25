// Subagents and child tasks, on every backend: a foreground subagent, background subagents and child tasks. There
// is no subagent module: these are patterns on ownership, with no API of their own.
//
// Each example prints a transcript into an array (`transcript`) the test asserts on. Waits are `waitFor`, which
// advances the `TestClock`; the background example's slow faux model (`tokensPerSecond: 50`) and the payments'
// 100 ms sleep pace on that clock. A restart is a new Harness over the same storage of the backend. The registry is
// the static extensions of `openChat`. A tool's failure is a typed failure (`SubagentFailed`), which the tool's
// result reports. The child tasks example's lines go to an array (`log`); its task graph is `HarnessTaskGraph`'s
// subject, not repeated here.
//
// Properties checked:
// - foreground: the child is owned by the call's task and not offered the tool; aborting the parent aborts the child
//   bottom-up, and the parent is idle only once the child is; a call that fails, or that is not replay-safe and is
//   cut off by a restart, aborts its child; a replay-safe rerun finds the child and the submission it made;
// - background: the parent is idle while a subagent works, an ordinary abort stops at the anchor, and
//   `abort({ background: true })` reaches the subagent through it;
// - a reporter that delivers to an external system under the calling tool's `key`, so the system sees one
//   delivery when a crash makes the reporter post twice.
import { assert, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as Predicate from "effect/Predicate";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as Agent from "../src/Agent.ts";
import type { AgentStorage } from "../src/AgentStorage.ts";
import * as AgentTool from "../src/AgentTool.ts";
import * as Doc from "../src/Doc.ts";
import { AssistantEntry } from "../src/Entry.ts";
import * as Extension from "../src/Extension.ts";
import type { Conversation } from "../src/Harness.ts";
import { ConversationId, EntryIdValue, TaskId, TaskIdValue } from "../src/Ids.ts";
import { LiveDoc } from "../src/Live.ts";
import type { Message } from "../src/Message.ts";
import * as Task from "../src/Task.ts";
import {
  type FauxResponseFactory,
  type FauxResponseStep,
  fauxAssistantMessage,
  fauxText,
  fauxToolCall,
} from "../src/testing/FauxModel.ts";
import { Tx } from "../src/Tx.ts";
import {
  allEntries,
  type ChatSetup,
  chatSetup,
  openChat,
  unanswered,
  waitFor,
} from "./ChatSupport.ts";
import { killable, openHarness, type TestHarness } from "./HarnessSupport.ts";
import {
  type ControlledStorage,
  controlledStorage,
  describeEachBackend,
  flush,
  run,
} from "./SessionSupport.ts";

/** A tool's failure: a typed failure the tool's result reports. */
class SubagentFailed extends Schema.TaggedError<SubagentFailed>()("SubagentFailed", {
  message: Schema.String,
}) {}

/** The text blocks of an assistant message, joined. */
const textOf = (message: Message | undefined): string =>
  message?.role === "assistant"
    ? message.content.flatMap((block) => (block.type === "text" ? [block.text] : [])).join("")
    : "";

/** The text of a user or tool result message. */
const contentText = (message: Message): string => {
  if (message.role === "system" || message.role === "assistant") return "";
  if (Predicate.isString(message.content)) return message.content;
  return message.content.flatMap((part) => (part.type === "text" ? [part.text] : [])).join("");
};

/** Runs `effect` and returns its result once it settles, letting the `TestClock` advance meanwhile. */
const finished = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  Effect.gen(function* () {
    const fiber = yield* Effect.forkChild(effect, { startImmediately: true });
    yield* waitFor(Effect.sync(() => fiber.pollUnsafe() !== undefined));
    return yield* Fiber.join(fiber);
  });

/**
 * The examples' UI: a conversation's messages, one line each, as the examples print them. User input is `> text`, a
 * subagent's report `> name: text`, a tool call `tool name(args)`, a tool result `→ text`, an answer `assistant:
 * text`.
 */
const transcript = (conversation: Conversation) =>
  Effect.map(allEntries(conversation), (entries) =>
    entries.flatMap((entry): Array<string> => {
      const message = entry.model?.[0];
      if (message === undefined) return [];
      if (message.role === "user") {
        const text = contentText(message);
        const report = /^\[subagent (\S+) ([^\]]*)\] ?(.*)$/s.exec(text);
        return [report === null ? `> ${text}` : `> ${report[1]}: ${report[3] || report[2]}`];
      }
      if (message.role === "toolResult") return [`→ ${contentText(message)}`];
      if (message.role !== "assistant") return [];
      return message.content.flatMap((block) => {
        if (block.type === "toolCall") return [`tool ${block.name}(${toJson(block.arguments)})`];
        if (block.type === "text" && block.text !== "") return [`assistant: ${block.text}`];
        return [];
      });
    }),
  );

const toJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));

/** The conversations owned by tasks of `root`. */
const ownedChildren = (h: TestHarness, root: Conversation) =>
  Effect.gen(function* () {
    const records = Array.from(yield* Stream.runCollect(h.harness.conversations()));
    return records.filter((record) => record.owner?.conversationId === root.id);
  });

/** Every task that turns terminal from now on, as `kind@conversation:status`, in commit order. */
const terminals = (h: TestHarness) =>
  Effect.gen(function* () {
    const seen: Array<string> = [];
    yield* h.kernel.followCommits((publication) => {
      for (const change of publication.changes) {
        if (change.type !== "task" || change.value.state.status !== "terminal") continue;
        const { kind, conversationId, state } = change.value;
        seen.push(`${kind}@${conversationId}:${state.outcome.status}`);
      }
    });
    return seen;
  });

// ─── The foreground subagent ─────────────────────────────────────────────────

interface ForegroundOptions {
  /** `"safe"` for a call a rerun may repeat; `"unsafe"` for a call a restart must not rerun. */
  readonly replay: AgentTool.ReplayPolicy;
  /** When set, the call fails once this completes instead of waiting for the child's answer. */
  readonly failWhen?: Deferred.Deferred<void> | undefined;
}

/**
 * The foreground `Subagent` extension: the call's task owns a child conversation, runs one input there and returns
 * the child's answer. Aborting the call aborts the child; the call finishes only once the child's work is done.
 */
const foreground = (options: ForegroundOptions): Extension.Any => {
  const tool = AgentTool.make("subagent", {
    description: "Delegate a self-contained task to a subagent and get its answer back.",
    parameters: Schema.Struct({
      task: Schema.String.annotate({ description: "What the subagent should do" }),
    }),
    // `"safe"`: a rerun after a crash finds the child it already created and the submission it already
    // made.
    replay: options.replay,
    execute: (args, call) =>
      Effect.gen(function* () {
        const child = yield* call.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            // Ownership records the child: a rerun of this call finds it instead of creating another.
            const existing = (yield* tx.scanConversations({ ownerTaskId: call.taskId }, 1))
              .items[0];
            if (existing !== undefined) return existing.id;
            // Starts as a copy of this conversation's agent: model, thinking level, extensions, tools.
            const created = yield* tx.createConversation({
              ownership: { kind: "task", taskId: call.taskId },
            });
            // Without this extension, the child is not offered this tool.
            yield* Agent.configure(created.id, { extensions: { remove: [extension] } });
            return created.id;
          }),
        );
        // A UI watching the parent sees this and can attach to the child.
        yield* call.details({ conversationId: child });
        const handle = yield* call.requireConversation(child);
        // The request ID makes a rerun get back the submission it made before the crash. It comes from the task ID:
        // provider call IDs are not unique within a Session.
        const submission = yield* handle.submit({
          type: "input",
          content: args.task,
          requestId: `subagent:${call.taskId}`,
        });
        if (options.failWhen !== undefined) {
          yield* Deferred.await(options.failWhen);
          return yield* new SubagentFailed({ message: "The subagent was abandoned" });
        }
        const settled = yield* submission.await;
        if (settled.status !== "done" || settled.type !== "input") {
          return yield* new SubagentFailed({ message: `Subagent failed: ${settled.status}` });
        }
        const answerId = settled.answer;
        const answer = yield* call.commit(
          Effect.flatMap(Tx, (tx) => tx.entry(AssistantEntry, answerId)),
        );
        const text = textOf(Option.getOrUndefined(answer)?.model?.[0]);
        return AgentTool.text(text, { details: { conversationId: child } });
      }),
  });
  const extension: Extension.Any = Extension.make({ name: "subagent", tools: [tool] });
  return extension;
};

const DELEGATE = fauxAssistantMessage(
  [fauxToolCall("subagent", { task: "Name three prime numbers." }, { id: "call-1" })],
  { stopReason: "toolUse" },
);
const CHILD_ANSWER = fauxAssistantMessage([fauxText("2, 3, and 5.")]);
const PARENT_ANSWER = fauxAssistantMessage([fauxText("The subagent says: 2, 3, and 5.")]);

/**
 * A model for the cases that stop the child: the parent delegates on "Delegate.", the child never answers (`reached`
 * completes once it was asked), and the parent answers a tool result with "Noted.". Each request takes the next
 * response, so it is the same answer in any order of the two conversations' requests.
 */
const stuckChild = (setup: ChatSetup) => {
  const busy = unanswered();
  const route: FauxResponseFactory = (request, state, model) => {
    const last = request.messages.findLast((message) => message.role !== "system")!;
    if (last.role === "toolResult")
      return Effect.succeed(fauxAssistantMessage([fauxText("Noted.")]));
    if (contentText(last) === "Delegate.") return Effect.succeed(DELEGATE);
    return busy.step(request, state, model);
  };
  setup.faux.setResponses(Array.from({ length: 10 }, (): FauxResponseStep => route));
  return busy;
};

/** Opens the foreground example's Harness over `storage`, scheduling at once. */
const openForeground = (
  storage: ControlledStorage,
  setup: ChatSetup,
  options: ForegroundOptions = { replay: "safe" },
) =>
  Effect.gen(function* () {
    const opened = yield* openChat(storage, setup, { extensions: [foreground(options)] });
    yield* opened.harness.harness.resume;
    return opened;
  });

/** The child's submission, found by the request ID its owner's task derived. */
const childSubmission = (h: TestHarness, root: Conversation) =>
  Effect.gen(function* () {
    const [child] = yield* ownedChildren(h, root);
    const owner = Option.getOrThrow(Option.fromNullishOr(child?.owner));
    const submission = yield* h.harness.submissionByRequest(child!.id, `subagent:${owner.taskId}`);
    return { child: child!, owner, submission: Option.getOrThrow(submission) };
  });

/** A settled submission's status, with its reason when unanswered. */
const outcome = (settled: { readonly status: string; readonly reason?: string }) =>
  settled.reason === undefined ? settled.status : `${settled.status}: ${settled.reason}`;

describeEachBackend("foreground subagent", (backend) => {
  it.effect("delegates to a child conversation and reports its answer", () =>
    run(backend)(
      Effect.gen(function* () {
        // The parent delegates, the child answers, and the parent reports.
        const setup = chatSetup();
        setup.faux.setResponses([DELEGATE, CHILD_ANSWER, PARENT_ANSWER]);
        const { harness, root } = yield* openForeground(yield* controlledStorage, setup);
        const submission = yield* root.submit({
          type: "input",
          content: "Use the subagent tool to find three prime numbers, then tell me what it said.",
        });
        yield* finished(submission.await);
        yield* finished(harness.harness.awaitIdle);

        assert.deepStrictEqual(yield* transcript(root), [
          "> Use the subagent tool to find three prime numbers, then tell me what it said.",
          'tool subagent({"task":"Name three prime numbers."})',
          "→ 2, 3, and 5.",
          "assistant: The subagent says: 2, 3, and 5.",
        ]);
        // The child is owned by the call's task, which a UI finds in the call's details.
        const children = yield* ownedChildren(harness, root);
        assert.strictEqual(children.length, 1);
        const childId = children[0]!.id;
        const owner = Option.getOrThrow(
          yield* harness.harness.getTask(
            Option.getOrThrow(Option.fromNullishOr(children[0]!.owner)).taskId,
          ),
        );
        assert.strictEqual(owner.kind, "core.tool");
        const results = (yield* allEntries(root)).flatMap((entry) => {
          const message = entry.model?.[0];
          return message?.role === "toolResult" ? [message.details] : [];
        });
        assert.deepStrictEqual(results, [{ conversationId: childId }]);
        const child = Option.getOrThrow(yield* harness.harness.conversation(childId));
        assert.deepStrictEqual(yield* transcript(child), [
          "> Name three prime numbers.",
          "assistant: 2, 3, and 5.",
        ]);
        // Copied from the parent's agent, without the subagent extension.
        const agent = yield* child.agent;
        assert.deepStrictEqual(agent.model, (yield* root.agent).model);
        assert.isFalse(agent.tools.some((tool) => tool.name === "subagent"));
        assert.isTrue((yield* root.agent).tools.some((tool) => tool.name === "subagent"));
        yield* harness.close;
      }),
    ),
  );

  // Aborting the parent cascades through the call's task into the child, and
  // the parent is idle only once the child is. Owned work ends first, so the child's run is terminal before the call.
  it.effect("aborting the parent aborts the child's work bottom-up", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        const busy = stuckChild(setup);
        const { harness, root } = yield* openForeground(yield* controlledStorage, setup);
        const ended = yield* terminals(harness);
        const submission = yield* root.submit({ type: "input", content: "Delegate." });
        // The child works on the delegated task.
        yield* waitFor(Deferred.isDone(busy.reached));
        const { child } = yield* childSubmission(harness, root);

        // The parent waits for the child.
        const idle = yield* Effect.forkChild(root.awaitIdle, { startImmediately: true });
        yield* flush;
        assert.isUndefined(idle.pollUnsafe());

        yield* finished(root.abort());
        assert.strictEqual(outcome(yield* finished(submission.await)), "unanswered: aborted");
        const found = yield* childSubmission(harness, root);
        assert.strictEqual(outcome(yield* finished(found.submission.await)), "unanswered: aborted");
        yield* finished(Fiber.join(idle));
        assert.isUndefined(
          Option.getOrUndefined(yield* harness.session.snapshot(LiveDoc, child.id))?.run,
        );
        const childRun = ended.indexOf(`core.generation@${child.id}:aborted`);
        const call = ended.indexOf(`core.tool@${root.id}:aborted`);
        assert.isAtLeast(childRun, 0);
        assert.isAbove(call, childRun);
        assert.isAbove(ended.indexOf(`core.generation@${root.id}:aborted`), call);
        yield* harness.close;
      }),
    ),
  );

  // So does the call failing. A failing `execute` ends the call's task `failed`, which records cancellation intent:
  // the child it owned, left unsupervised, is aborted.
  it.effect("a call that fails aborts its child's work", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        const busy = stuckChild(setup);
        const { harness, root } = yield* openForeground(yield* controlledStorage, setup, {
          replay: "safe",
          failWhen: busy.reached,
        });
        const submission = yield* root.submit({ type: "input", content: "Delegate." });
        assert.strictEqual(outcome(yield* finished(submission.await)), "done");

        const { child, owner, submission: delegated } = yield* childSubmission(harness, root);
        assert.strictEqual(outcome(yield* finished(delegated.await)), "unanswered: aborted");
        const call = Option.getOrThrow(yield* harness.harness.getTask(owner.taskId));
        assert.strictEqual(call.state.status === "terminal" && call.state.outcome.status, "failed");
        assert.isUndefined(
          Option.getOrUndefined(yield* harness.session.snapshot(LiveDoc, child.id))?.run,
        );
        assert.strictEqual((yield* transcript(root)).at(-1), "assistant: Noted.");
        yield* harness.close;
      }),
    ),
  );

  // A replay-safe rerun finds the child with `scanConversations({ ownerTaskId })` and gets back its
  // submission by the request ID derived from the task ID.
  it.effect("a rerun after a restart finds its child and its submission", () =>
    run(backend)(
      Effect.gen(function* () {
        const storage = yield* controlledStorage;
        const setup = chatSetup();
        const busy = unanswered();
        setup.faux.setResponses([DELEGATE, busy.step, CHILD_ANSWER, PARENT_ANSWER]);
        const first = yield* openForeground(storage, setup);
        const submission = yield* first.root.submit({ type: "input", content: "Delegate." });
        // The child's request reached the model when the process stops.
        yield* waitFor(Deferred.isDone(busy.reached));
        const childId = (yield* ownedChildren(first.harness, first.root))[0]!.id;
        yield* first.harness.close;

        const { harness, root } = yield* openForeground(storage, setup);
        const reopened = Option.getOrThrow(yield* harness.harness.submission(submission.id));
        assert.strictEqual(outcome(yield* finished(reopened.await)), "done");
        yield* finished(harness.harness.awaitIdle);

        const children = yield* ownedChildren(harness, root);
        assert.deepStrictEqual(
          children.map((child) => child.id),
          [childId],
        );
        const child = Option.getOrThrow(yield* harness.harness.conversation(childId));
        assert.deepStrictEqual(yield* transcript(child), [
          "> Name three prime numbers.",
          "assistant: 2, 3, and 5.",
        ]);
        assert.deepStrictEqual((yield* transcript(root)).slice(-2), [
          "→ 2, 3, and 5.",
          "assistant: The subagent says: 2, 3, and 5.",
        ]);
        yield* harness.close;
      }),
    ),
  );

  // So does a crash that interrupts a call that is not replay-safe. The call is not rerun: its task ends `failed`
  // with an interruption result, and that aborts its child.
  it.effect("a call not safe to rerun, cut off by a restart, aborts its child", () =>
    run(backend)(
      Effect.gen(function* () {
        const storage = yield* controlledStorage;
        const setup = chatSetup();
        const busy = stuckChild(setup);
        const first = yield* openForeground(storage, setup, { replay: "unsafe" });
        const submission = yield* first.root.submit({ type: "input", content: "Delegate." });
        yield* waitFor(Deferred.isDone(busy.reached));
        yield* first.harness.close;

        const { harness, root } = yield* openForeground(storage, setup, { replay: "unsafe" });
        const reopened = Option.getOrThrow(yield* harness.harness.submission(submission.id));
        assert.strictEqual(outcome(yield* finished(reopened.await)), "done");
        yield* finished(harness.harness.awaitIdle);

        const { child, owner, submission: delegated } = yield* childSubmission(harness, root);
        assert.strictEqual(outcome(yield* finished(delegated.await)), "unanswered: aborted");
        const call = Option.getOrThrow(yield* harness.harness.getTask(owner.taskId));
        assert.strictEqual(call.state.status === "terminal" && call.state.outcome.status, "failed");
        assert.isUndefined(
          Option.getOrUndefined(yield* harness.session.snapshot(LiveDoc, child.id))?.run,
        );
        const lines = yield* transcript(root);
        assert.include(lines.at(-2) ?? "", "Tool subagent was interrupted");
        assert.strictEqual(lines.at(-1), "assistant: Noted.");
        yield* harness.close;
      }),
    ),
  );
});

// ─── Persistent background subagents ─────────────────────────────────────────

// Each subagent is its own conversation with its own transcript, so it remembers earlier messages. The main
// conversation keeps a small document that maps subagent names to their conversations.
const SubagentRecord = Schema.Struct({
  // Plain numbers: task inputs and documents hold their encoded values.
  conversationId: Schema.Number,
  /** Answers already reported to the main agent: several messages can end in one answer, reported once. */
  reported: Schema.Array(EntryIdValue),
});
const Registry = Schema.Struct({
  agents: Schema.Record(Schema.String, SubagentRecord),
  /** The reporter each `subagent` call created, keyed by the call's task ID. */
  reporters: Schema.Record(Schema.String, TaskIdValue),
});
const emptyRegistry = (): typeof Registry.Type => ({ agents: {}, reporters: {} });

/** The named subagent, if the registry has one; never a property every object inherits. */
const agentNamed = (registry: typeof Registry.Type, name: string) =>
  Object.hasOwn(registry.agents, name) ? registry.agents[name] : undefined;

const Subagents = Doc.make({
  checkpointWhen: () => false,
  kind: "app.subagents",
  version: 1,
  scope: "conversation",
  history: "latest",
  // A fork of the main conversation starts without subagents.
  fork: "initial",
  schema: Registry,
  initial: emptyRegistry,
});

// Every task and conversation has an owner, and that decides what an abort or an idle wait reaches. A subagent must
// outlive the main agent's turns, so its conversation is owned by an anchor: a background task that finishes at
// once. A background task is a boundary: the main agent's abort and idle waits stop there, but
// `abort({ background: true })` still reaches past it, so a host can stop everything.
const Anchor = Task.make({
  name: "app.subagent-anchor",
  version: 1,
  input: Schema.Null,
  checkpoint: Schema.Struct({ phase: Schema.Literal("done") }),
  result: Schema.Null,
  initial: () => ({ phase: "done" }),
  phases: { done: (_anchor, runtime) => runtime.complete(null) },
  abort: (_anchor, runtime) => runtime.aborted,
});

/** The reporter's next checkpoint: the report to post, if any. */
const reportNext = (report?: string) =>
  Task.next(
    report === undefined ? { phase: "report" as const } : { phase: "report" as const, report },
  );

// A reporter delivers one message to a subagent and reports the answer to the main agent. It is a background task
// too, so the main agent's abort and idle waits leave it alone. Each phase ends with a saved checkpoint, and after a
// restart the task continues from the last one. Request IDs make a repeated submission return the first one, so the
// subagent gets the message once and the main agent gets the report once.
const Reporter = Task.make({
  name: "app.subagent-reporter",
  version: 1,
  input: Schema.Struct({
    name: Schema.String,
    conversationId: Schema.Number,
    message: Schema.String,
    followUp: Schema.Boolean,
  }),
  checkpoint: Schema.Union([
    Schema.Struct({ phase: Schema.Literal("deliver") }),
    Schema.Struct({ phase: Schema.Literal("report"), report: Schema.optionalKey(Schema.String) }),
  ]),
  result: Schema.Null,
  initial: () => ({ phase: "deliver" }),
  phases: {
    // Send the message, wait for its answer, and decide what to report.
    deliver: (reporter, runtime) =>
      Effect.gen(function* () {
        const input = reporter.input;
        // While the subagent is busy, a steer reaches it at its next step and a follow-up after its current answer.
        const subagent = yield* runtime.requireConversation(
          ConversationId.make(input.conversationId),
        );
        const submission = yield* subagent.submit({
          type: "input",
          content: input.message,
          whenBusy: input.followUp ? "followUp" : "steer",
          requestId: `subagent:${runtime.taskId}`,
        });
        const settled = yield* submission.await;
        // One commit decides the report and records the answer as delivered, so a restart does not decide again.
        yield* runtime.commit(
          Effect.gen(function* () {
            // `aborted`: stopped, or withdrawn while queued. Nothing to report.
            if (settled.status === "unanswered") {
              return reportNext(
                settled.reason === "aborted"
                  ? undefined
                  : `[subagent ${input.name} failed: ${settled.reason}]`,
              );
            }
            // Always an answered input here; the check tells TypeScript.
            if (settled.type !== "input") return reportNext();
            const tx = yield* Tx;
            const agent = (yield* tx.doc(Subagents, runtime.conversationId)).agents[input.name]!;
            if (agent.reported.includes(settled.answer)) return reportNext();
            agent.reported.push(settled.answer);
            const answer = yield* tx.entry(AssistantEntry, settled.answer);
            const text = textOf(Option.getOrUndefined(answer)?.model?.[0]);
            return reportNext(`[subagent ${input.name} answered, no reply needed] ${text}`);
          }),
        );
      }),
    // Post the report as a follow-up input: it starts a turn when the main agent is idle, or waits for its current
    // answer. Dropped with the other queued input if the main agent is aborted while it waits.
    report: (reporter, runtime) =>
      Effect.gen(function* () {
        const report = reporter.state.checkpoint.report;
        if (report !== undefined) {
          const main = yield* runtime.requireConversation(runtime.conversationId);
          yield* main.submit({
            type: "input",
            content: report,
            whenBusy: "followUp",
            requestId: `subagent-report:${runtime.taskId}`,
          });
        }
        yield* runtime.complete(null);
      }),
  },
  abort: (_reporter, runtime) => runtime.aborted,
});

/** The details a call reports: a UI can attach to the subagent's conversation through them. */
const reply = (text: string, name?: string, conversationId?: number) =>
  conversationId === undefined || name === undefined
    ? AgentTool.text(text)
    : AgentTool.text(text, { details: { name, conversationId } });

const backgroundTool = AgentTool.make("subagent", {
  description:
    "Manage persistent subagents that work in the background. Actions: spawn (name, message), send (name, message; " +
    "followUp: true queues it after the current answer instead of steering), stop (name: aborts its current work), " +
    "status (name, or all subagents without one). Answers are reported back to you when they arrive.",
  parameters: Schema.Struct({
    action: Schema.Literals(["spawn", "send", "stop", "status"]),
    name: Schema.optionalKey(Schema.String),
    message: Schema.optionalKey(Schema.String),
    followUp: Schema.optionalKey(Schema.Boolean),
  }),
  // A call interrupted by a crash is not rerun: repeating `stop` could stop newer work. The model sees that the call
  // was interrupted and can check with `status`.
  replay: "unsafe",
  execute: (args, call) =>
    Effect.gen(function* () {
      const registry = Option.getOrElse(
        yield* call.snapshot(Subagents, call.conversationId),
        emptyRegistry,
      );
      const name = args.name;
      if (args.action === "status") {
        const names = name === undefined ? Object.keys(registry.agents) : [name];
        const lines: Array<string> = [];
        for (const each of names) {
          const found = agentNamed(registry, each);
          if (found === undefined) continue;
          // A conversation is busy while it has a run: from an input until its final answer.
          const live = yield* call.snapshot(LiveDoc, ConversationId.make(found.conversationId));
          lines.push(
            `${each}: ${Option.getOrUndefined(live)?.run !== undefined ? "working" : "idle"}`,
          );
        }
        return reply(lines.length === 0 ? "No subagents." : lines.join("\n"));
      }
      if (name === undefined) return reply(`${args.action} needs a name.`);
      const agent = agentNamed(registry, name);
      if (args.action !== "spawn" && agent === undefined)
        return reply(`No subagent named ${name}.`);

      if (args.action === "stop" && agent !== undefined) {
        // Aborts the subagent's current answer and tools and drops its queued messages. It stays usable.
        const subagent = yield* call.requireConversation(ConversationId.make(agent.conversationId));
        yield* subagent.abort();
        return reply(`Stopped ${name}.`, name, agent.conversationId);
      }
      const message = args.message;
      if (message === undefined) return reply(`${args.action} needs a message.`);

      // spawn and send: one commit starts a reporter for the message.
      const action = args.action;
      const result = yield* call.commit(
        Effect.gen(function* () {
          const tx = yield* Tx;
          const state = yield* tx.doc(Subagents, call.conversationId);
          // Both tasks belong to the main conversation and are background: its abort and idle waits skip them.
          const background = { ownership: { kind: "conversation" }, background: true } as const;
          if (action === "spawn") {
            if (Object.hasOwn(state.agents, name)) return `${name} already exists; use send.`;
            const anchor = yield* tx.createTask(Anchor, null, background);
            // Owned by a task of the main conversation: starts as a copy of the main agent.
            const child = yield* tx.createConversation({
              ownership: { kind: "task", taskId: anchor },
            });
            // Subagents cannot start subagents, and know who they are.
            yield* Agent.configure(child.id, {
              extensions: { remove: [SubagentTools] },
              instructions: `You are the subagent "${name}". Answer the main agent's requests.`,
            });
            state.agents[name] = { conversationId: child.id, reported: [] };
          }
          const conversationId = state.agents[name]!.conversationId;
          const input = {
            name,
            conversationId,
            message,
            followUp: action === "send" && args.followUp === true,
          };
          state.reporters[String(call.taskId)] = yield* tx.createTask(Reporter, input, background);
          return action === "send" ? `Sent to ${name}.` : `Started ${name}.`;
        }),
      );
      const current = Option.getOrUndefined(yield* call.snapshot(Subagents, call.conversationId))
        ?.agents[name];
      return reply(result, name, current?.conversationId);
    }),
});

// The task definitions come with the extension, so pending reporters resume after a restart once the host installs
// it again. Annotated: the tool's handler names the extension that holds it.
const SubagentTools: Extension.Any = Extension.make({
  name: "subagent-tools",
  tasks: [Anchor, Reporter],
  tools: [backgroundTool],
});

const callSubagent = (input: Record<string, string>) =>
  Effect.succeed(
    fauxAssistantMessage([fauxToolCall("subagent", input)], { stopReason: "toolUse" }),
  );
const answerWith = (text: string) => Effect.succeed(fauxAssistantMessage([fauxText(text)]));

/** The text of a request's last message, and its role. System messages carry prompt changes; it is the one before. */
const lastMessage = (request: Parameters<FauxResponseFactory>[0]) => {
  const last = request.messages.findLast((message) => message.role !== "system")!;
  return { role: last.role, text: contentText(last) };
};

/**
 * The background example's scripted model, shared by the main agent and the subagent: it answers each request by its last
 * message.
 */
const route: FauxResponseFactory = (request) => {
  const { role, text } = lastMessage(request);
  // The main agent repeats what the tool said.
  if (role === "toolResult") return answerWith(`OK. ${text}`);
  // The main agent.
  if (text.includes("Start a subagent")) {
    return callSubagent({
      action: "spawn",
      name: "reader",
      message: "Summarize the plot of Moby Dick.",
    });
  }
  if (text.includes("whale's name")) {
    return callSubagent({ action: "send", name: "reader", message: "What is the whale called?" });
  }
  if (text.includes("every chapter")) {
    return callSubagent({
      action: "send",
      name: "reader",
      message: "Now go through all chapters in detail.",
    });
  }
  if (text.includes("Stop reader")) return callSubagent({ action: "stop", name: "reader" });
  if (text.includes("my subagents")) return callSubagent({ action: "status" });
  if (text.includes("[subagent")) return answerWith("Noted.");
  // The subagent: short answers, and a long chapter walk-through that is stopped halfway.
  if (text.includes("Summarize the plot")) return answerWith("A whale, a captain, an obsession.");
  if (text.includes("whale called")) return answerWith("Moby Dick.");
  const chapters = Array.from({ length: 135 }, (_, index) => `Chapter ${index + 1}: more whaling.`);
  return answerWith(chapters.join("\n"));
};

/** Opens a background example's Harness over `storage`, scheduling at once. */
const openBackground = (
  storage: ControlledStorage,
  setup: ChatSetup,
  extension: Extension.Any = SubagentTools,
) =>
  Effect.gen(function* () {
    const opened = yield* openChat(storage, setup, { extensions: [extension] });
    yield* opened.harness.harness.resume;
    return opened;
  });

/** Say something to the main agent and wait for its answer. */
const say = (root: Conversation, text: string) =>
  finished(
    Effect.flatMap(root.submit({ type: "input", content: text }), (submission) => submission.await),
  );

const registryOf = (h: TestHarness, root: Conversation) =>
  Effect.map(h.session.snapshot(Subagents, root.id), (found) =>
    Option.getOrElse(found, emptyRegistry),
  );

/** Wait until every message to a subagent was answered and reported, and the main agent has reacted. */
const settle = (h: TestHarness, root: Conversation) =>
  finished(
    Effect.gen(function* () {
      for (const id of Object.values((yield* registryOf(h, root)).reporters)) {
        yield* h.harness.awaitTask(id);
      }
      yield* root.awaitIdle;
    }),
  );

/** Whether the named subagent has a run. */
const working = (h: TestHarness, root: Conversation, name: string) =>
  Effect.gen(function* () {
    const agent = agentNamed(yield* registryOf(h, root), name);
    if (agent === undefined) return false;
    return (
      Option.getOrUndefined(
        yield* h.session.snapshot(LiveDoc, ConversationId.make(agent.conversationId)),
      )?.run !== undefined
    );
  });

/** The named subagent's conversation. */
const subagentOf = (h: TestHarness, root: Conversation, name: string) =>
  Effect.gen(function* () {
    const agent = agentNamed(yield* registryOf(h, root), name)!;
    return Option.getOrThrow(
      yield* h.harness.conversation(ConversationId.make(agent.conversationId)),
    );
  });

describeEachBackend("background subagents", (backend) => {
  it.effect(
    "spawns, messages, stops and lists subagents, and reports an answer across a restart",
    () =>
      run(backend)(
        Effect.gen(function* () {
          // The main agent and the subagent share one scripted model, which streams its answers at 50 tokens per
          // second, like a slow real model. More responses than the script needs; each request takes the next one.
          const setup = chatSetup({ tokensPerSecond: 50 });
          setup.faux.setResponses(Array.from({ length: 40 }, (): FauxResponseStep => route));
          const storage = yield* controlledStorage;
          let { harness, root } = yield* openBackground(storage, setup);

          // The main agent answers at once; the subagent's answer is reported back when it arrives.
          yield* say(root, "Start a subagent named reader that summarizes Moby Dick.");
          yield* settle(harness, root);
          assert.deepStrictEqual(yield* transcript(root), [
            "> Start a subagent named reader that summarizes Moby Dick.",
            'tool subagent({"action":"spawn","name":"reader","message":"Summarize the plot of Moby Dick."})',
            "→ Started reader.",
            "assistant: OK. Started reader.",
            "> reader: A whale, a captain, an obsession.",
            "assistant: Noted.",
          ]);
          // The subagent's conversation is owned by a background anchor of the main conversation, and is not offered
          // the subagent tool.
          const reader = yield* subagentOf(harness, root, "reader");
          const [readerRecord] = yield* ownedChildren(harness, root);
          assert.strictEqual(readerRecord?.id, reader.id);
          const anchorId = Option.getOrThrow(Option.fromNullishOr(readerRecord?.owner?.taskId));
          const anchor = Option.getOrThrow(yield* harness.harness.getTask(anchorId));
          assert.strictEqual(anchor.kind, "app.subagent-anchor");
          assert.strictEqual(anchor.conversationId, root.id);
          assert.isTrue(anchor.background);
          assert.strictEqual(anchor.state.status, "terminal");
          assert.isFalse((yield* reader.agent).tools.some((tool) => tool.name === "subagent"));
          assert.strictEqual(
            (yield* reader.agent).instructions,
            'You are the subagent "reader". Answer the main agent\'s requests.',
          );

          // A long request, stopped while the subagent is still answering.
          yield* say(root, "Ask reader to summarize every chapter.");
          yield* waitFor(working(harness, root, "reader"));
          // The background anchor and reporter are boundaries, so the main conversation and the
          // Harness are idle while the subagent works.
          yield* finished(root.awaitIdle);
          yield* finished(harness.harness.awaitIdle);
          assert.isTrue(yield* working(harness, root, "reader"));
          // The anchor ended with the first answer; this run is the subagent's own work, which no owner holds.
          assert.strictEqual(
            Option.getOrThrow(yield* harness.harness.getTask(anchorId)).state.status,
            "terminal",
          );
          yield* say(root, "Stop reader.");
          yield* settle(harness, root);
          assert.isFalse(yield* working(harness, root, "reader"));
          assert.deepStrictEqual((yield* transcript(root)).slice(6), [
            "> Ask reader to summarize every chapter.",
            'tool subagent({"action":"send","name":"reader","message":"Now go through all chapters in detail."})',
            "→ Sent to reader.",
            "assistant: OK. Sent to reader.",
            "> Stop reader.",
            'tool subagent({"action":"stop","name":"reader"})',
            "→ Stopped reader.",
            "assistant: OK. Stopped reader.",
          ]);

          yield* say(root, "What are my subagents doing?");
          yield* settle(harness, root);
          assert.deepStrictEqual((yield* transcript(root)).slice(14), [
            "> What are my subagents doing?",
            'tool subagent({"action":"status"})',
            "→ reader: idle",
            "assistant: OK. reader: idle",
          ]);

          // The process stops while the subagent works on a message; after the restart its answer still arrives.
          const reporters = Effect.map(
            registryOf(harness, root),
            (registry) => Object.keys(registry.reporters).length,
          );
          const before = yield* reporters;
          yield* root.submit({ type: "input", content: "Ask reader for the whale's name." });
          yield* waitFor(Effect.map(reporters, (count) => count > before));
          // The newest reporter is still delivering when the process stops.
          const pending = Object.values((yield* registryOf(harness, root)).reporters).at(-1)!;
          assert.notStrictEqual(
            Option.getOrThrow(yield* harness.harness.getTask(pending)).state.status,
            "terminal",
          );
          yield* harness.close;
          ({ harness, root } = yield* openBackground(storage, setup));
          yield* settle(harness, root);

          const lines = yield* transcript(root);
          assert.strictEqual(lines[18], "> Ask reader for the whale's name.");
          assert.deepStrictEqual(
            lines.filter((line) => line === "> reader: Moby Dick."),
            ["> reader: Moby Dick."],
          );
          assert.strictEqual(lines.at(-1), "assistant: Noted.");
          // The subagent got the message once, and remembers the earlier ones.
          const readerLines = yield* transcript(yield* subagentOf(harness, root, "reader"));
          assert.deepStrictEqual(
            readerLines.filter((line) => line.startsWith(">")),
            [
              "> Summarize the plot of Moby Dick.",
              "> Now go through all chapters in detail.",
              "> What is the whale called?",
            ],
          );
          assert.strictEqual(readerLines.at(-1), "assistant: Moby Dick.");
          yield* harness.close;
        }),
      ),
  );

  // An ordinary abort of the main conversation stops at the background anchor and
  // reporter; `abort({ background: true })` reaches the subagent's work through the anchor, and aborts the reporter.
  it.effect("an ordinary abort leaves a subagent working; a background abort stops it", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup({ tokensPerSecond: 50 });
        // The subagent starts on the long walk-through at once.
        const busyRoute: FauxResponseFactory = (request, state, model) =>
          lastMessage(request).text.includes("Start a busy subagent")
            ? callSubagent({
                action: "spawn",
                name: "reader",
                message: "Now go through all chapters in detail.",
              })
            : route(request, state, model);
        setup.faux.setResponses(Array.from({ length: 10 }, (): FauxResponseStep => busyRoute));
        const { harness, root } = yield* openBackground(yield* controlledStorage, setup);
        yield* say(root, "Start a busy subagent named reader.");
        yield* waitFor(working(harness, root, "reader"));
        const [reporter] = Object.values((yield* registryOf(harness, root)).reporters);

        yield* finished(root.abort());
        yield* flush;
        assert.isTrue(yield* working(harness, root, "reader"));
        assert.notStrictEqual(
          Option.getOrThrow(yield* harness.harness.getTask(reporter!)).state.status,
          "terminal",
        );

        yield* finished(root.abort({ background: true }));
        assert.isFalse(yield* working(harness, root, "reader"));
        const settled = yield* finished(harness.harness.awaitTask(reporter!));
        assert.strictEqual(settled.state.outcome.status, "aborted");
        // Nothing was reported, and the main agent's last word is its answer to the spawn.
        const lines = yield* transcript(root);
        assert.isFalse(lines.some((line) => line.startsWith("> reader:")));
        assert.strictEqual(lines.at(-1), "assistant: OK. Started reader.");
        yield* harness.close;
      }),
    ),
  );
});

// ─── A reporter that delivers to an external system ──────────────────────────

/**
 * A consumer's external system that deduplicates by idempotency key, as a webhook receiver or a queue with a
 * deduplication ID does. `afterPost` runs after each post: a test makes it crash the Harness.
 */
interface Outbox {
  /** The key of every post, in order, duplicates included. */
  readonly posts: Array<string>;
  /** What the system kept: one message per key. */
  readonly delivered: Map<string, string>;
  afterPost: Effect.Effect<void>;
  readonly post: (key: string, text: string) => Effect.Effect<void>;
}

const makeOutbox = (): Outbox => {
  const outbox: Outbox = {
    posts: [],
    delivered: new Map(),
    afterPost: Effect.void,
    post: (key, text) =>
      Effect.sync(() => {
        outbox.posts.push(key);
        if (!outbox.delivered.has(key)) outbox.delivered.set(key, text);
      }).pipe(Effect.andThen(Effect.suspend(() => outbox.afterPost))),
  };
  return outbox;
};
/** The couriers the `delegate` calls created, keyed by the call's task ID, and the one subagent. */
const Couriers = Doc.make({
  checkpointWhen: () => false,
  kind: "app.couriers",
  version: 1,
  scope: "conversation",
  history: "latest",
  fork: "initial",
  schema: Schema.Struct({
    subagent: Schema.optionalKey(Schema.Number),
    couriers: Schema.Record(Schema.String, TaskIdValue),
  }),
  initial: () => ({ couriers: {} }),
});

/**
 * Example 22's background pattern, with a courier that posts the subagent's answer to `outbox` instead of reporting
 * it to the main agent. The calling tool hands it `call.key("post")`, which every run of that call computes the same,
 * so the external system can recognize a repeated post. Provider call IDs are not unique within a Session and do
 * not serve.
 */
const courierTools = (outbox: Outbox): Extension.Any => {
  const Courier = Task.make({
    name: "app.courier",
    version: 1,
    input: Schema.Struct({
      conversationId: Schema.Number,
      message: Schema.String,
      key: Schema.String,
    }),
    checkpoint: Schema.Union([
      Schema.Struct({ phase: Schema.Literal("deliver") }),
      Schema.Struct({ phase: Schema.Literal("post"), text: Schema.String }),
    ]),
    result: Schema.Null,
    initial: () => ({ phase: "deliver" }),
    phases: {
      deliver: (courier, runtime) =>
        Effect.gen(function* () {
          const subagent = yield* runtime.requireConversation(
            ConversationId.make(courier.input.conversationId),
          );
          const submission = yield* subagent.submit({
            type: "input",
            content: courier.input.message,
            requestId: `courier:${runtime.taskId}`,
          });
          const settled = yield* submission.await;
          if (settled.status !== "done" || settled.type !== "input") {
            return yield* runtime.fail(`The subagent did not answer: ${settled.status}`);
          }
          // The answer is in the subagent's conversation, which the runtime's `entry` does not see; a commit reads
          // any entry.
          const answerId = settled.answer;
          yield* runtime.commit(
            Effect.gen(function* () {
              const answer = yield* (yield* Tx).entry(AssistantEntry, answerId);
              const text = textOf(Option.getOrUndefined(answer)?.model?.[0]);
              return Task.next({ phase: "post" as const, text });
            }),
          );
        }),
      // The external effect, then the commit that records it. A crash between the two reruns this phase, and the
      // key lets the system drop the second post.
      post: (courier, runtime) =>
        Effect.gen(function* () {
          yield* outbox.post(courier.input.key, courier.state.checkpoint.text);
          yield* runtime.complete(null);
        }),
    },
    abort: (_courier, runtime) => runtime.aborted,
  });
  const delegate = AgentTool.make("delegate", {
    description: "Ask the subagent something; its answer is posted to the team channel.",
    parameters: Schema.Struct({ message: Schema.String }),
    // A rerun finds the courier this call created.
    replay: "safe",
    execute: (args, call) =>
      Effect.gen(function* () {
        yield* call.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            const state = yield* tx.doc(Couriers, call.conversationId);
            if (Object.hasOwn(state.couriers, String(call.taskId))) return;
            const background = { ownership: { kind: "conversation" }, background: true } as const;
            let subagent = state.subagent;
            if (subagent === undefined) {
              const anchor = yield* tx.createTask(Anchor, null, background);
              const child = yield* tx.createConversation({
                ownership: { kind: "task", taskId: anchor },
              });
              yield* Agent.configure(child.id, { extensions: { remove: [extension] } });
              subagent = child.id;
              state.subagent = subagent;
            }
            const input = {
              conversationId: subagent,
              message: args.message,
              key: call.key("post"),
            };
            state.couriers[String(call.taskId)] = yield* tx.createTask(Courier, input, background);
          }),
        );
        return AgentTool.text("Asked; the answer goes to the channel.");
      }),
  });
  const extension: Extension.Any = Extension.make({
    name: "courier-tools",
    tasks: [Anchor, Courier],
    tools: [delegate],
  });
  return extension;
};

describeEachBackend("a reporter that delivers externally", (backend) => {
  it.effect("posts under the calling tool's key, so a crash after the post delivers once", () =>
    run(backend)(
      Effect.gen(function* () {
        const inner = yield* controlledStorage;
        const setup = chatSetup();
        const delegateCall: FauxResponseFactory = (request) => {
          const { role, text } = lastMessage(request);
          if (role === "toolResult") return answerWith("OK.");
          if (text.includes("Ask the subagent")) {
            return Effect.succeed(
              fauxAssistantMessage(
                [fauxToolCall("delegate", { message: "What is the whale called?" })],
                { stopReason: "toolUse" },
              ),
            );
          }
          return answerWith("Moby Dick.");
        };
        setup.faux.setResponses(Array.from({ length: 10 }, (): FauxResponseStep => delegateCall));
        const outbox = makeOutbox();
        const open = (storage: AgentStorage["Service"]) =>
          Effect.gen(function* () {
            const crashable = yield* killable(storage);
            const opened = yield* openBackground(crashable.storage, setup, courierTools(outbox));
            return { ...opened, kill: crashable.kill };
          });

        const first = yield* open(inner);
        // The process dies right after the external post, before the courier's commit records it.
        outbox.afterPost = first.kill;
        // Submitted, not awaited: the crash fails the Session, and with it every wait.
        yield* first.root.submit({
          type: "input",
          content: "Ask the subagent for the whale's name.",
        });
        yield* waitFor(Effect.sync(() => outbox.posts.length === 1));
        yield* first.harness.close;
        outbox.afterPost = Effect.void;

        const { harness, root } = yield* open(inner);
        const state = Option.getOrThrow(yield* harness.session.snapshot(Couriers, root.id));
        const [callTaskId, courierId] = Object.entries(state.couriers)[0]!;
        const settled = yield* finished(harness.harness.awaitTask(courierId));
        assert.strictEqual(settled.state.outcome.status, "completed");
        // Posted twice under the call's key, kept once.
        const key = `${callTaskId}:post`;
        assert.deepStrictEqual(outbox.posts, [key, key]);
        assert.deepStrictEqual([...outbox.delivered], [[key, "Moby Dick."]]);
        const call = Option.getOrThrow(
          yield* harness.harness.getTask(TaskId.make(Number(callTaskId))),
        );
        assert.strictEqual(call.kind, "core.tool");
        yield* harness.close;
      }),
    ),
  );
});

// ─── Child tasks ─────────────────────────────────────────────────────────────

/** The child tasks example's checkout and its payments, over a fake bank of its own; `log` holds the example's lines. */
const checkoutTasks = () => {
  // A fake bank: it declines an expired card at once, and confirms other charges after a moment.
  const charged = new Set<string>();
  const log: Array<string> = [];

  const Payment = Task.make({
    name: "example.payment",
    version: 1,
    input: Schema.Struct({ card: Schema.String }),
    checkpoint: Schema.Union([
      Schema.Struct({ phase: Schema.Literal("charge") }),
      Schema.Struct({ phase: Schema.Literal("confirm") }),
    ]),
    result: Schema.Struct({ card: Schema.String }),
    initial: () => ({ phase: "charge" }),
    phases: {
      charge: (task, runtime) =>
        Effect.gen(function* () {
          const card = task.input.card;
          if (card.startsWith("expired")) return yield* runtime.fail(`${card} declined`);
          charged.add(card);
          // The bank confirms after a moment: wait for it without holding an invocation.
          const now = yield* runtime.now;
          yield* runtime.sleepUntil(now + 100, { phase: "confirm" });
        }),
      confirm: (task, runtime) => runtime.complete({ card: task.input.card }),
    },
    // Each payment undoes its own effect when aborted.
    abort: (task, runtime) =>
      Effect.gen(function* () {
        const refunded = charged.delete(task.input.card);
        log.push(`payment ${task.input.card} aborted${refunded ? ", refunded" : ""}`);
        yield* runtime.aborted;
      }),
  });

  // The checkout creates its payments as child tasks and waits for all of them. With `failFast`, the first payment
  // that fails aborts the others; the checkout itself is not aborted and decides its outcome once every payment is
  // done.
  const Checkout = Task.make({
    name: "example.checkout",
    version: 1,
    input: Schema.Struct({ cards: Schema.Array(Schema.String) }),
    checkpoint: Schema.Union([
      Schema.Struct({ phase: Schema.Literal("pay") }),
      Schema.Struct({ phase: Schema.Literal("decide"), payments: Schema.Array(TaskIdValue) }),
    ]),
    result: Schema.String,
    initial: () => ({ phase: "pay" }),
    phases: {
      pay: (task, runtime) =>
        runtime.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            const payments: Array<TaskId> = [];
            for (const card of task.input.cards) {
              payments.push(
                yield* tx.createTask(
                  Payment,
                  { card },
                  { ownership: { kind: "task", taskId: runtime.taskId } },
                ),
              );
            }
            return Task.waitFor(payments, {
              checkpoint: { phase: "decide", payments },
              policy: "failFast",
            });
          }),
        ),
      decide: (task, runtime) =>
        Effect.gen(function* () {
          const outcomes = yield* runtime.outcomes(task.state.checkpoint.payments);
          log.push(`payments: ${outcomes.map((outcome) => outcome.status).join(", ")}`);
          const paid = outcomes.every((outcome) => outcome.status === "completed");
          yield* paid ? runtime.complete("order placed") : runtime.fail("payment failed");
        }),
    },
    // Runs only after every payment is done, so the refunds have already happened.
    abort: (_task, runtime) =>
      Effect.gen(function* () {
        log.push("checkout aborted");
        yield* runtime.aborted;
      }),
  });
  return { Payment, Checkout, charged, log };
};

/** Starts a checkout of `cards` in the root conversation. */
const startCheckout = (
  h: TestHarness,
  cards: ReadonlyArray<string>,
  tasks: ReturnType<typeof checkoutTasks>,
) =>
  Effect.gen(function* () {
    const root = yield* h.harness.ensureRoot();
    return yield* root.commit(
      Effect.flatMap(Tx, (tx) =>
        tx.createTask(
          tasks.Checkout,
          { cards: [...cards] },
          { ownership: { kind: "conversation" } },
        ),
      ),
    );
  });
/** Opens a Harness over `storage` with the checkout tasks, scheduling at once. */
const openCheckout = (storage: ControlledStorage, tasks: ReturnType<typeof checkoutTasks>) =>
  Effect.gen(function* () {
    const h = yield* openHarness(storage, [tasks.Payment, tasks.Checkout]);
    yield* h.harness.resume;
    return h;
  });

describeEachBackend("child tasks", (backend) => {
  it.effect(
    "one card is declined: the others are aborted and refunded, and the checkout fails",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const tasks = checkoutTasks();
          const h = yield* openCheckout(yield* controlledStorage, tasks);
          const id = yield* startCheckout(h, ["visa-1", "expired-2", "visa-3", "visa-4"], tasks);
          const settled = yield* finished(h.harness.awaitTask(id));
          assert.deepStrictEqual(settled.state.outcome, {
            status: "failed",
            error: { message: "payment failed" },
          });
          // The siblings' abort handlers run in no promised order; the checkout decides after all of them.
          assert.deepStrictEqual(tasks.log.slice(0, -1).toSorted(), [
            "payment visa-1 aborted, refunded",
            "payment visa-3 aborted, refunded",
            "payment visa-4 aborted, refunded",
          ]);
          assert.strictEqual(tasks.log.at(-1), "payments: aborted, failed, aborted, aborted");
          assert.strictEqual(tasks.charged.size, 0);
          yield* h.close;
        }),
      ),
  );

  // Abort runs bottom-up: every payment's abort handler, then the checkout's.
  it.effect("the customer cancels: the payments abort first, then the checkout", () =>
    run(backend)(
      Effect.gen(function* () {
        const tasks = checkoutTasks();
        const h = yield* openCheckout(yield* controlledStorage, tasks);
        const id = yield* startCheckout(h, ["visa-5", "visa-6", "visa-7", "visa-8"], tasks);
        yield* waitFor(Effect.sync(() => tasks.charged.size === 4));
        yield* finished(h.harness.abortTask(id));
        const settled = yield* finished(h.harness.awaitTask(id));
        assert.strictEqual(settled.state.outcome.status, "aborted");
        assert.deepStrictEqual(tasks.log.slice(0, -1).toSorted(), [
          "payment visa-5 aborted, refunded",
          "payment visa-6 aborted, refunded",
          "payment visa-7 aborted, refunded",
          "payment visa-8 aborted, refunded",
        ]);
        assert.strictEqual(tasks.log.at(-1), "checkout aborted");
        yield* h.close;
      }),
    ),
  );

  it.effect("the process stops while the payments run, and a new one continues", () =>
    run(backend)(
      Effect.gen(function* () {
        const storage = yield* controlledStorage;
        const tasks = checkoutTasks();
        const first = yield* openCheckout(storage, tasks);
        const id = yield* startCheckout(first, ["visa-9", "visa-10", "visa-11", "visa-12"], tasks);
        // The payments are asleep, waiting for the bank, when the process stops.
        yield* waitFor(Effect.sync(() => tasks.charged.size === 4));
        yield* first.close;

        const h = yield* openCheckout(storage, tasks);
        const settled = yield* finished(h.harness.awaitTask(id));
        assert.deepStrictEqual(settled.state.outcome, {
          status: "completed",
          result: "order placed",
        });
        assert.deepStrictEqual(tasks.log, ["payments: completed, completed, completed, completed"]);
        yield* h.close;
      }),
    ),
  );
});

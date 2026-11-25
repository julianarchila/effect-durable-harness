// The helpers of `HarnessCompaction.test.ts`: a chat with a scripted faux model that answers agent and
// summarization requests from separate queues.
import { assert } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as Predicate from "effect/Predicate";
import * as Schema from "effect/Schema";
import * as AgentTool from "../src/AgentTool.ts";
import type { CompactionResult } from "../src/Compaction.ts";
import * as Extension from "../src/Extension.ts";
import type { Conversation } from "../src/Harness.ts";
import * as Hook from "../src/Hook.ts";
import type { TaskId } from "../src/Ids.ts";
import { LiveDoc, type LiveState } from "../src/Live.ts";
import type { AssistantMessage, Message } from "../src/Message.ts";
import type { GenerateRequest } from "../src/Models.ts";
import { ProviderDoc } from "../src/ProviderSession.ts";
import * as Section from "../src/Section.ts";
import type { CompactionPolicy } from "../src/Settings.ts";
import type { Submission } from "../src/Submission.ts";
import { fauxAssistantMessage, type FauxResponseFactory } from "../src/testing/FauxModel.ts";
import { UsageDoc } from "../src/Usage.ts";
import { allEntries, type ChatSetup, chatSetup, openChat, textOf, waitFor } from "./ChatSupport.ts";
import type { TestHarness } from "./HarnessSupport.ts";
import { type ControlledStorage, controlledStorage } from "./SessionSupport.ts";

/** Text of about `tokens` estimated tokens, starting with `label`. */
export const text = (label: string, tokens: number): string =>
  `${label} ${"x".repeat(Math.max(0, tokens * 4 - label.length - 1))}`;

/** One model request as the script records it: the messages, the options, and the model's id. */
export interface Request {
  readonly messages: ReadonlyArray<Message>;
  readonly options: RequestOptions;
  readonly model: string;
}

/** The stream options of a request, put back together from its `GenerateRequest`. */
export type RequestOptions = GenerateRequest["options"] & {
  readonly maxTokens?: number | undefined;
  readonly sessionId?: string | undefined;
  readonly reasoning?: GenerateRequest["reasoning"];
};

export const optionsOf = (request: GenerateRequest): RequestOptions => ({
  ...request.options,
  maxTokens: request.maxTokens,
  sessionId: request.sessionId,
  reasoning: request.reasoning,
});

export type Step = AssistantMessage | ((request: Request) => Effect.Effect<AssistantMessage>);

/** Scripted faux model that answers agent requests and summarization requests from separate queues. */
export interface Script {
  readonly agent: Array<Step>;
  readonly summaries: Array<Step>;
  readonly agentRequests: Array<Request>;
  readonly summaryRequests: Array<Request>;
}

export const isSummaryRequest = (messages: ReadonlyArray<Message>): boolean => {
  const first = messages[0];
  return (
    first?.role === "system" &&
    Predicate.isString(first.content) &&
    first.content.startsWith("You are a context summarization assistant")
  );
};

export const script = (setup: ChatSetup): Script => {
  const result: Script = { agent: [], summaries: [], agentRequests: [], summaryRequests: [] };
  const respond: FauxResponseFactory = (generate, _state, model) =>
    Effect.suspend(() => {
      const request = {
        messages: [...generate.messages],
        options: optionsOf(generate),
        model: model.id,
      };
      const summary = isSummaryRequest(request.messages);
      (summary ? result.summaryRequests : result.agentRequests).push(request);
      const step = (summary ? result.summaries : result.agent).shift();
      if (step === undefined) {
        return Effect.die(new Error(`No scripted ${summary ? "summary" : "agent"} response`));
      }
      return Predicate.isFunction(step) ? step(request) : Effect.succeed(step);
    });
  setup.faux.setResponses(Array.from({ length: 500 }, () => respond));
  return result;
};

/** A step that waits for `gate`; a cancelled request is its interruption. */
export const gated =
  (
    gate: Deferred.Deferred<void>,
    message: AssistantMessage,
    reached?: Deferred.Deferred<void>,
  ): Step =>
  () =>
    Effect.gen(function* () {
      if (reached !== undefined) yield* Deferred.succeed(reached, undefined);
      yield* Deferred.await(gate);
      return message;
    });

export const answer = (content: string) => fauxAssistantMessage(content);
export const failure = (errorMessage: string) =>
  fauxAssistantMessage("", { stopReason: "error", errorMessage });

export const OVERFLOW = "prompt is too long: 250000 tokens > 200000 maximum";

/** Small thresholds: no automatic compaction unless a test enables it. */
export const MANUAL: CompactionPolicy = {
  enabled: false,
  reserveTokens: 1000,
  keepRecentTokens: 150,
  backgroundTokens: 0,
};

export interface Chat {
  readonly harness: TestHarness;
  readonly root: Conversation;
  readonly setup: ChatSetup;
  readonly faux: Script;
}

/** Options of `open`. */
export interface OpenOptions {
  readonly policy?: CompactionPolicy | undefined;
  readonly contextWindow?: number | undefined;
  /** The storage; default a fresh controlled storage over the backend. */
  readonly storage?: ControlledStorage | undefined;
  /** The static extensions, installed before opening. */
  readonly extensions?: ReadonlyArray<Extension.Any> | undefined;
}

/** An untagged `preamble` section. */
export const preamble = Extension.make({
  name: "section:preamble",
  sections: [Section.make("preamble", () => Effect.succeed("You are helpful."), { tag: false })],
});

/** A chat with the faux model's window, the preamble section, and small policies. */
export const open = (
  options: OpenOptions = {},
  given?: { readonly setup: ChatSetup; readonly faux: Script },
) =>
  Effect.gen(function* () {
    const setup =
      given?.setup ??
      chatSetup({
        models: [{ id: "faux-1", contextWindow: options.contextWindow ?? 100_000, maxTokens: 900 }],
      });
    const faux = given?.faux ?? script(setup);
    const storage = options.storage ?? (yield* controlledStorage);
    const { harness, root } = yield* openChat(storage, setup, {
      extensions: [preamble, ...(options.extensions ?? [])],
    });
    // These tests exercise compaction accounting and thresholds, not the faux provider's cache simulation.
    setup.settings.stream = { cacheRetention: "none" };
    setup.settings.compaction = options.policy ?? MANUAL;
    setup.settings.retry = { enabled: true, maxRetries: 2, baseDelayMs: 1 };
    yield* harness.harness.resume;
    return { harness, root, setup, faux } satisfies Chat;
  });

/** Runs `effect` to its end while the `TestClock` advances, for waits on timers. */
export const finished = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  Effect.gen(function* () {
    const fiber = yield* Effect.forkChild(effect, { startImmediately: true });
    yield* waitFor(Effect.sync(() => fiber.pollUnsafe() !== undefined));
    return yield* Fiber.join(fiber);
  });

/** Waits for `submission` to settle. */
export const settle = (submission: Submission) => finished(submission.await);

/** The submission `id`, which must exist. */
export const submissionOf = (chat: Chat, id: CompactionResult["submissionId"]) =>
  Effect.gen(function* () {
    if (id === undefined) return assert.fail("expected a submission ID");
    return Option.getOrThrow(yield* chat.harness.harness.submission(id));
  });

/** Run one turn: `user` answered by `reply`. */
export const turn = (chat: Chat, user: string, reply: string) =>
  Effect.gen(function* () {
    chat.faux.agent.push(answer(reply));
    const submission = yield* chat.root.submit({ type: "input", content: user });
    assert.strictEqual((yield* settle(submission)).status, "done");
  });

/** Three turns of about 100-token messages. */
export const history = (chat: Chat) =>
  Effect.gen(function* () {
    yield* turn(chat, text("u1", 100), text("a1", 100));
    yield* turn(chat, text("u2", 100), text("a2", 100));
    yield* turn(chat, text("u3", 100), text("a3", 100));
  });

export const summary = (content = "SUMMARY") => answer(content);

/** The outcome of compaction `id`, once terminal. */
export const result = (chat: Chat, id: TaskId<CompactionResult>) =>
  finished(chat.harness.harness.awaitTask(id)).pipe(Effect.map((record) => record.state.outcome));

/** The submission ID of a completed conversation-owned compaction's outcome. */
export const placedBy = (outcome: Effect.Success<ReturnType<typeof result>>) =>
  outcome.status === "completed" ? outcome.result.submissionId : undefined;

export const kinds = (conversation: Conversation) =>
  allEntries(conversation).pipe(Effect.map((entries) => entries.map((entry) => entry.kind)));

export const live = (chat: Chat) =>
  chat.harness.session
    .snapshot(LiveDoc, chat.root.id)
    .pipe(Effect.map((value) => Option.getOrElse(value, (): LiveState => ({}))));

export const usageOf = (chat: Chat) =>
  chat.harness.session
    .snapshot(UsageDoc, chat.root.id)
    .pipe(Effect.map((value) => Option.getOrThrow(value).models["faux/faux-1"]!));

export const sessionIdOf = (chat: Chat) =>
  chat.harness.session
    .snapshot(ProviderDoc, chat.root.id)
    .pipe(Effect.map((value) => Option.getOrThrow(value).sessionId));

export const userText = (message: Message | undefined): string => textOf(message) ?? "";

/** Text of the first user message; a leading baseline system message comes before it. */
export const firstUserText = (messages: ReadonlyArray<Message>): string =>
  userText(messages.find((message) => message.role === "user"));

/** An extension holding one hook registration. */
let hookExtensions = 0;
export const compactionHooks = (handlers: Partial<Hook.CompactionHooks>) =>
  Extension.make({ name: `hooks:${++hookExtensions}`, hooks: [Hook.compaction(handlers)] });

/** The parameters of a tool without parameters. */
export const Parameters = Schema.Struct({});
export const toolNamed = (name: string, output = "") =>
  AgentTool.make(name, {
    description: name,
    parameters: Parameters,
    execute: () =>
      Effect.succeed({ output: output === "" ? [] : [{ type: "text" as const, text: output }] }),
  });
export const toolExtension = (tool: AgentTool.Any) =>
  Extension.make({ name: `tool:${tool.name}`, tools: [tool] });

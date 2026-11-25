// The built-in compaction task, which selects an old prefix of the model context, summarizes it, and places a
// summary entry whose `head` is the first kept entry. A compaction the generation owns blocks it and appends
// directly; a conversation-owned one places its summary through a write submission. Range selection, the context
// estimate and the serialization are pure functions here, and so are the generation's threshold decision and
// failure text, which live here to keep the generation's own changes small.
import type * as Tracker from "@effect-durable-harness/delta/Tracker";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Predicate from "effect/Predicate";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { CompactionReason, CompactionResult } from "../Compaction.ts";
import type { ContextView } from "../ContextView.ts";
import { CompactionEntry } from "../Entry.ts";
import type { CompactionDecision, CompactionHooks, CompactionRange } from "../Hook.ts";
import { type ConversationId, type EntryId, EntryIdValue, type TaskId } from "../Ids.ts";
import { LiveDoc, type LiveState } from "../Live.ts";
import {
  type AssistantMessage,
  type Message,
  type TextContent,
  ThinkingLevel,
} from "../Message.ts";
import {
  type GenerateRequest,
  ModelRef,
  Models,
  type ResolvedModel,
  StreamOptions,
} from "../Models.ts";
import type { CompactionPolicy, CompactionSettings, Settings } from "../Settings.ts";
import * as Task from "../Task.ts";
import { Tx, type TxDocError, type TxWriteError } from "../Tx.ts";
import { isRetryableAssistantError, retryDelayMs } from "./classify.ts";
import { orderToolResults } from "./context.ts";
import { calculateContextTokens, estimateMessageTokens } from "./estimate.ts";
import {
  addCompactionStatus,
  compactionStatus,
  cutOffCount,
  cutOffTooOften,
  failCompactionStatus,
  removeCompactionStatus,
} from "./live.ts";
import * as modelTelemetry from "./modelTelemetry.ts";
import { ensureProviderSessionId } from "./provider.ts";
import { admitSubmission } from "./submissions.ts";
import { recordUsage } from "./usage.ts";

/** What started a compaction, and the instructions of a manual one. */
const CompactionInput = Schema.Struct({
  reason: CompactionReason,
  instructions: Schema.optionalKey(Schema.String),
});
export type CompactionInput = typeof CompactionInput.Type;

/** The pinned summarization request. */
const SummaryRequest = {
  attempt: Schema.Number,
  model: ModelRef,
  thinkingLevel: ThinkingLevel,
  streamOptions: StreamOptions,
  maxTokens: Schema.Number,
  /** Newest entry of the context the range was selected from. */
  tail: EntryIdValue,
  /** First entry kept verbatim; the summary's `head`. */
  firstKept: EntryIdValue,
};

/** The compaction task's checkpoint, one per phase. */
const CompactionCheckpoint = Schema.Union([
  Schema.Struct({ phase: Schema.Literal("select") }),
  Schema.Struct({ phase: Schema.Literal("summarize"), ...SummaryRequest }),
  Schema.Struct({ phase: Schema.Literal("retry"), until: Schema.Number, ...SummaryRequest }),
]);
type CompactionCheckpoint = typeof CompactionCheckpoint.Type;
type SummaryRequest = Omit<Extract<CompactionCheckpoint, { readonly phase: "summarize" }>, "phase">;

type Runtime = Task.TaskRuntime<CompactionInput, CompactionCheckpoint, CompactionResult>;
type Next = Task.NextTaskState<CompactionCheckpoint, CompactionResult>;
type LiveDraft = Tracker.Draft<LiveState>;

/** The compaction task's name, as hooks name it. */
const COMPACTION = "core.compaction";

/** Longest tool result text a serialized summary source keeps. */
const TOOL_RESULT_MAX_CHARS = 2000;

const SUMMARY_PREFIX =
  "The conversation history before this point was compacted into the following summary:\n\n<summary>\n";
const SUMMARY_SUFFIX = "\n</summary>";

const SUMMARIZATION_SYSTEM_PROMPT = `You are a context summarization assistant. Your task is to read a conversation between a user and an AI assistant, then produce a structured summary following the exact format specified.

Do NOT continue the conversation. Do NOT respond to any questions in the conversation. ONLY output the structured summary.`;

const SUMMARIZATION_PROMPT = `The messages above are a conversation to summarize. Create a structured context checkpoint summary that another LLM will use to continue the work. If the conversation starts with an earlier summary, preserve its information and fold the newer messages into it.

Use this EXACT format:

## Goal
[What is the user trying to accomplish? Can be multiple items if the session covers different tasks.]

## Constraints & Preferences
- [Any constraints, preferences, or requirements mentioned by user]
- [Or "(none)" if none were mentioned]

## Progress
### Done
- [x] [Completed tasks/changes]

### In Progress
- [ ] [Current work]

### Blocked
- [Issues preventing progress, if any]

## Key Decisions
- **[Decision]**: [Brief rationale]

## Next Steps
1. [Ordered list of what should happen next]

## Critical Context
- [Any data, examples, or references needed to continue]
- [Or "(none)" if not applicable]

Keep each section concise. Preserve exact file paths, function names, and error messages.`;

/**
 * Built-in compaction task: select an old prefix of the model context, summarize it, and place a summary
 * entry whose `head` is the first kept entry. A compaction the generation owns blocks it and appends directly; a
 * conversation-owned one places its summary through a write submission.
 */
export const CompactionTask = Task.make({
  name: COMPACTION,
  version: 1,
  input: CompactionInput,
  checkpoint: CompactionCheckpoint,
  result: CompactionResult,
  initial: () => ({ phase: "select" }),
  phases: {
    /**
     * Resolves the phase's agent and the settings once, selects the range over the
     * committed context, asks `beforeCompact`, and pins the summarization request.
     */
    select: (task, runtime) =>
      Effect.gen(function* () {
        if (yield* cutOffAtLimit(task, runtime)) return yield* failCutOff(runtime, task);
        const conversationId = runtime.conversationId;
        const agent = yield* runtime.agent;
        const settings = yield* runtime.settings;
        const ref = agent.model;
        const models = yield* Models;
        const model = ref === undefined ? Option.none<ResolvedModel>() : yield* models.resolve(ref);
        if (ref === undefined || Option.isNone(model)) return yield* failNoModel(runtime, ref);
        const policy = compactionPolicy(settings, ref, agent.compaction);
        const view = yield* runtime.context(conversationId);
        const cut = selectCut(view, policy.keepRecentTokens);
        if (cut === undefined) return yield* complete(runtime);
        // SAFETY: `selectCut` returns an index of `view.entries`.
        const firstKept = view.entries[cut]!.id;
        const input = task.input;
        const compaction: CompactionRange = {
          reason: input.reason,
          entries: view.entries.slice(0, cut),
          messages: summarizedMessages(view, cut),
          firstKept,
          instructions: input.instructions,
        };
        let decision: CompactionDecision | undefined;
        yield* runtime.hooks.each("beforeCompact", (hook: CompactionHooks["beforeCompact"]) =>
          decision !== undefined
            ? Effect.void
            : Effect.map(hook(compaction, runtime), (decided) => {
                decision = decided;
              }),
        );
        if (decision !== undefined && "decline" in decision) return yield* complete(runtime);
        if (decision !== undefined) return yield* place(runtime, firstKept, decision.summary);
        const request: SummaryRequest = {
          attempt: 1,
          model: ref,
          thinkingLevel: agent.thinkingLevel,
          streamOptions: settings.stream,
          maxTokens: Math.min(
            Math.floor(0.8 * policy.reserveTokens),
            model.value.maxTokens > 0 ? model.value.maxTokens : Number.POSITIVE_INFINITY,
          ),
          tail: view.entries.reduce(
            (tail, entry) => (entry.id > tail ? entry.id : tail),
            firstKept,
          ),
          firstKept,
        };
        yield* runtime.next({ phase: "summarize", ...request });
      }),
    /**
     * Resends the pinned request over the context at `tail`, which is immutable, and
     * classifies the response in one commit that adds its usage to `core.usage`.
     */
    summarize: (task, runtime) =>
      Effect.gen(function* () {
        if (yield* cutOffAtLimit(task, runtime)) return yield* failCutOff(runtime, task);
        const { phase: _, ...request } = task.state.checkpoint;
        const models = yield* Models;
        const model = yield* models.resolve(request.model);
        if (Option.isNone(model)) return yield* failNoModel(runtime, request.model);
        // The context at `tail` is immutable, so this is the range `select` chose.
        const view = yield* runtime.context(runtime.conversationId, { at: request.tail });
        const cut = view.entries.findIndex((entry) => entry.id === request.firstKept);
        const now = yield* runtime.now;
        const messages: ReadonlyArray<Message> = [
          { role: "system", content: SUMMARIZATION_SYSTEM_PROMPT, timestamp: now },
          {
            role: "user",
            content: [
              {
                type: "text",
                text: summaryPrompt(summarizedMessages(view, cut), task.input.instructions),
              },
            ],
            timestamp: now,
          },
        ];
        // A summary is never deferred: the task has no poll phase. The request carries no telemetry option, since
        // its span is the caller's.
        const { deferred: _deferred, ...forwarded } = request.streamOptions;
        const generate: GenerateRequest = {
          messages,
          reasoning: request.thinkingLevel === "off" ? undefined : request.thinkingLevel,
          sessionId: yield* ensureProviderSessionId(runtime),
          options: { ...forwarded, cacheRetention: "none" },
          maxTokens: request.maxTokens,
        };
        const message = yield* summaryResponse(runtime, model.value, generate, request.attempt);
        const summary = summaryText(message);
        const policy = (yield* runtime.settings).retry;
        const retry =
          message.stopReason === "error" &&
          isRetryableAssistantError(message) &&
          policy.enabled &&
          request.attempt <= policy.maxRetries;
        const until = retry ? (yield* runtime.now) + retryDelayMs(policy, request.attempt) : 0;
        yield* runtime.commit((current) =>
          Effect.gen(function* () {
            const tx = yield* Tx;
            yield* recordUsage(
              runtime.conversationId,
              "models",
              `${message.provider}/${message.model}`,
              message.usage,
            );
            const live = yield* tx.doc(LiveDoc, runtime.conversationId);
            if (summary !== undefined) {
              return yield* placeSummary(runtime, current, live, request.firstKept, summary);
            }
            if (retry) {
              const status = compactionStatus(live, runtime.taskId);
              if (status !== undefined)
                status.retry = { at: until, error: message.errorMessage ?? "" };
              return {
                status: "sleeping",
                checkpoint: { phase: "retry", ...request, until },
                until,
              } satisfies Next;
            }
            const text = summaryFailure(message);
            failCompactionStatus(live, runtime.taskId, text);
            return {
              status: "terminal",
              outcome: {
                status: "failed",
                error: { message: text, detail: { reason: "model_error" } },
              },
            } satisfies Next;
          }),
        );
      }),
    /**
     * After the backoff's sleep, summarizes again with the next attempt; a run before
     * `until` (an early wake, or a record of an earlier version) sleeps again.
     */
    retry: (task, runtime) =>
      Effect.gen(function* () {
        const { phase: _, until, ...request } = task.state.checkpoint;
        if (yield* cutOffAtLimit(task, runtime)) return yield* failCutOff(runtime, task);
        if (until > (yield* runtime.now)) {
          return yield* runtime.sleepUntil(until, task.state.checkpoint);
        }
        const attempt = request.attempt + 1;
        yield* runtime.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            const status = compactionStatus(
              yield* tx.doc(LiveDoc, runtime.conversationId),
              runtime.taskId,
            );
            if (status !== undefined) {
              status.attempt = attempt;
              delete status.retry;
            }
            return {
              status: "running",
              checkpoint: { phase: "summarize", ...request, attempt },
            } satisfies Next;
          }),
        );
      }),
  },
  /** Removes the task's status and ends `aborted`; it writes no entry. */
  abort: (_task, runtime) =>
    runtime.commit(
      Effect.gen(function* () {
        const tx = yield* Tx;
        removeCompactionStatus(yield* tx.doc(LiveDoc, runtime.conversationId), runtime.taskId);
        return { status: "terminal", outcome: { status: "aborted" } } satisfies Next;
      }),
    ),
});

/**
 * A compaction task with its status in this commit. `owner` is the
 * generation that waits for it (a blocking compaction); without one it is conversation-owned, and `background`
 * unless it is manual.
 */
export const createCompaction = (
  conversationId: ConversationId,
  input: CompactionInput,
  owner?: TaskId | undefined,
): Effect.Effect<TaskId<CompactionResult>, TxWriteError | TxDocError, Tx> =>
  Effect.gen(function* () {
    const tx = yield* Tx;
    const ownership: Task.TaskOwnership =
      owner === undefined ? { kind: "conversation" } : { kind: "task", taskId: owner };
    const background = owner === undefined && input.reason !== "manual";
    const taskId = yield* tx.createTask(CompactionTask, input, {
      ownership,
      conversationId,
      background,
    });
    addCompactionStatus(yield* tx.doc(LiveDoc, conversationId), {
      taskId,
      trigger: input.reason,
      blocking: owner !== undefined,
      attempt: 1,
    });
    return taskId;
  });

/**
 * The compaction policy for `model`, field by field: the conversation's `modelOverrides` entry, then the
 * Harness's, then the conversation's own fields, then the Harness-wide policy. The conversation's
 * `enabled: false` also beats the Harness's model override.
 */
export const compactionPolicy = (
  settings: Settings,
  model: ModelRef,
  conversation: CompactionSettings | undefined,
): CompactionPolicy => {
  const harness = settings.compaction;
  const key = `${model.provider}/${model.modelId}`;
  const conversationModel = ownEntry(conversation?.modelOverrides, key);
  const layers = [conversationModel, ownEntry(harness.modelOverrides, key), conversation];
  const field = <K extends keyof CompactionPolicy>(name: K): CompactionPolicy[K] => {
    for (const layer of layers) {
      const value = layer?.[name];
      if (value !== undefined) return value;
    }
    return harness[name];
  };
  return {
    enabled:
      conversationModel?.enabled ?? (conversation?.enabled === false ? false : field("enabled")),
    reserveTokens: field("reserveTokens"),
    keepRecentTokens: field("keepRecentTokens"),
    backgroundTokens: field("backgroundTokens"),
  };
};

// Own keys only, as `recordUsage` reads `core.usage`: a model may be called `toString`.
const ownEntry = <A>(
  record: { readonly [key: string]: A | undefined } | undefined,
  key: string,
): A | undefined => (record !== undefined && Object.hasOwn(record, key) ? record[key] : undefined);

/**
 * Index in `view.entries` of the first entry a summary keeps, or `undefined` when there is nothing to compact. Walks
 * back from the tail until `keepRecentTokens` are kept, then cuts
 * at the first candidate at or after that entry: an entry whose contribution starts with a user or assistant
 * message, never a tool result, and never a user entry that a result of the preceding assistant's calls still
 * follows.
 */
export const selectCut = (view: ContextView, keepRecentTokens: number): number | undefined => {
  const contributions = view.contributions;
  const start = view.head === undefined ? 0 : 1;
  const candidates: Array<number> = [];
  for (let index = start; index < contributions.length; index++) {
    if (isCandidate(contributions, index)) candidates.push(index);
  }
  let kept = 0;
  let cut: number | undefined;
  for (let index = contributions.length - 1; index >= start; index--) {
    for (const message of contributions[index] ?? []) kept += estimateMessageTokens(message);
    if (kept < keepRecentTokens) continue;
    cut = candidates.find((candidate) => candidate >= index) ?? candidates.at(-1);
    break;
  }
  if (cut === undefined) return undefined;
  for (let index = start; index < cut; index++) {
    if ((contributions[index] ?? []).length > 0) return cut;
  }
  return undefined;
};

/** Whether a summary may cut at the entry at `index`: see `selectCut`. */
const isCandidate = (contributions: ContextView["contributions"], index: number): boolean => {
  const first = contributions[index]?.[0];
  if (first?.role === "assistant") return true;
  if (first?.role !== "user") return false;
  // A result of the preceding assistant's calls that follows this entry, before the next assistant, belongs before
  // it.
  let calls: ReadonlySet<string> = new Set();
  for (let before = index - 1; before >= 0; before--) {
    const assistant = (contributions[before] ?? []).findLast(
      (message): message is AssistantMessage => message.role === "assistant",
    );
    if (assistant === undefined) continue;
    calls = new Set(
      assistant.content.flatMap((content) => (content.type === "toolCall" ? [content.id] : [])),
    );
    break;
  }
  if (calls.size === 0) return true;
  for (let after = index; after < contributions.length; after++) {
    for (const [position, message] of (contributions[after] ?? []).entries()) {
      if (message.role === "assistant" && (after > index || position > 0)) return true;
      if (message.role === "toolResult" && calls.has(message.toolCallId)) return false;
    }
  }
  return true;
};

/**
 * Model messages of the entries before `cut`, the head marker first, ordered like model context.
 */
export const summarizedMessages = (view: ContextView, cut: number): Array<Message> =>
  orderToolResults(view.contributions.slice(0, cut).flat());

/**
 * Size of a request over `view` followed by `extra`: the usage of the newest assistant appended after the head marker, whose request included the marker, plus estimates of
 * the messages after it; without one, estimates of every message.
 */
export const estimateContext = (view: ContextView, extra: ReadonlyArray<Message>): number => {
  let measured: AssistantMessage | undefined;
  const after = view.head?.id ?? Number.NEGATIVE_INFINITY;
  for (let index = view.entries.length - 1; index >= 0 && measured === undefined; index--) {
    // SAFETY: `index` is within `view.entries`, which `view.contributions` matches one for one.
    if (view.entries[index]!.id <= after) continue;
    measured = (view.contributions[index] ?? []).findLast(
      (message): message is AssistantMessage =>
        message.role === "assistant" && calculateContextTokens(message.usage) > 0,
    );
  }
  // A context read copies its view (`internal/context.ts`), so `messages` and `contributions` hold equal copies,
  // never the same object: the measured message is the newest message equal to it.
  const from = measured === undefined ? 0 : lastIndexOfEqual(view.messages, measured) + 1;
  let tokens = measured === undefined ? 0 : calculateContextTokens(measured.usage);
  for (const message of view.messages.slice(from)) tokens += estimateMessageTokens(message);
  for (const message of extra) tokens += estimateMessageTokens(message);
  return tokens;
};

/** The index of the newest message of `messages` equal to `message`, or -1. */
const lastIndexOfEqual = (messages: ReadonlyArray<Message>, message: AssistantMessage): number => {
  const encoded = encodeJson(message);
  return messages.findLastIndex(
    (candidate) =>
      candidate.role === "assistant" &&
      candidate.timestamp === message.timestamp &&
      encodeJson(candidate) === encoded,
  );
};

/**
 * Which threshold compaction preparation starts before its request: `blocking` above `contextWindow - reserveTokens`, `background` above the background threshold, and
 * only when range selection finds a cut. The caller starts a background one only while no compaction is listed.
 */
export const thresholdCompaction = (
  view: ContextView,
  planned: ReadonlyArray<{ readonly model?: ReadonlyArray<Message> | undefined }>,
  contextWindow: number,
  policy: CompactionPolicy,
): "blocking" | "background" | undefined => {
  if (!policy.enabled || contextWindow <= 0) return undefined;
  const tokens = estimateContext(
    view,
    planned.flatMap((entry) => entry.model ?? []),
  );
  const blocking = contextWindow - policy.reserveTokens;
  const background = blocking - policy.backgroundTokens;
  const over =
    tokens > blocking
      ? "blocking"
      : policy.backgroundTokens > 0 && tokens > background
        ? "background"
        : undefined;
  if (over === undefined || selectCut(view, policy.keepRecentTokens) === undefined)
    return undefined;
  return over;
};

/** Error text for a generation whose blocking compaction ended without completing. */
export const compactionFailureText = (
  outcome: Exclude<Task.TaskOutcome<CompactionResult>, { readonly status: "completed" }>,
): string => {
  switch (outcome.status) {
    case "failed":
    case "faulted":
      return `Compaction failed: ${outcome.error.message}`;
    case "orphaned":
      return `Compaction failed: ${outcome.reason}`;
    case "aborted":
      return "Compaction was aborted";
  }
};

/** The summary of a clean `stop` with text and no tool call. */
const summaryText = (message: AssistantMessage): string | undefined => {
  if (
    message.stopReason !== "stop" ||
    message.content.some((content) => content.type === "toolCall")
  ) {
    return undefined;
  }
  const text = message.content
    .flatMap((content) => (content.type === "text" ? [content.text] : []))
    .join("\n")
    .trim();
  return text.length === 0 ? undefined : text;
};

/** Error text for a summarization response `summaryText` rejected. */
const summaryFailure = (message: AssistantMessage): string => {
  if (message.stopReason === "error" || message.stopReason === "aborted") {
    return `Summarization failed: ${message.errorMessage ?? message.stopReason}`;
  }
  if (message.stopReason === "length") {
    return "Summarization hit the token limit; the summary is incomplete";
  }
  if (message.content.some((content) => content.type === "toolCall")) {
    return "Summarization attempted to call a tool";
  }
  return "Summarization produced no text";
};

/** The serialized conversation, the prompt, and any instructions. */
const summaryPrompt = (
  messages: ReadonlyArray<Message>,
  instructions: string | undefined,
): string => {
  const focus = instructions === undefined ? "" : `\n\nAdditional focus: ${instructions}`;
  return `<conversation>\n${serializeConversation(messages)}\n</conversation>\n\n${SUMMARIZATION_PROMPT}${focus}`;
};

/**
 * Messages as plain text, so the summarizer reads a transcript
 * instead of continuing it. System messages and thinking are omitted: providers return summaries of the real
 * reasoning, and replayed thinking is often not part of the live context, so including it can make the request
 * larger than the context it summarizes. Visible text, tool calls and tool results record what happened.
 */
export const serializeConversation = (messages: ReadonlyArray<Message>): string => {
  const parts: Array<string> = [];
  for (const message of messages) {
    if (message.role === "user") {
      const text = contentText(message.content);
      if (text.length > 0) parts.push(`[User]: ${text}`);
    } else if (message.role === "assistant") {
      const text = message.content.flatMap((content) =>
        content.type === "text" ? [content.text] : [],
      );
      const calls = message.content.flatMap((content) =>
        content.type === "toolCall"
          ? [
              `${content.name}(${Object.entries(content.arguments)
                .map(([key, value]) => `${key}=${encodeJson(value)}`)
                .join(", ")})`,
            ]
          : [],
      );
      if (text.length > 0) parts.push(`[Assistant]: ${text.join("\n")}`);
      if (calls.length > 0) parts.push(`[Assistant tool calls]: ${calls.join("; ")}`);
    } else if (message.role === "toolResult") {
      const text = contentText(message.content);
      if (text.length > 0) parts.push(`[Tool result]: ${truncate(text, TOOL_RESULT_MAX_CHARS)}`);
    }
  }
  return parts.join("\n\n");
};

const contentText = (
  content: string | ReadonlyArray<{ readonly type: string; readonly text?: string }>,
): string =>
  Predicate.isString(content)
    ? content
    : content
        .flatMap((block) => (block.type === "text" && block.text !== undefined ? [block.text] : []))
        .join("\n");

const truncate = (text: string, maxChars: number): string =>
  text.length <= maxChars
    ? text
    : `${text.slice(0, maxChars)}\n\n[... ${text.length - maxChars} more characters truncated]`;

// Encodes a tool call's argument value, which is JSON.
const encodeJson = Schema.encodeSync(Schema.fromJsonString(Schema.Json));

/**
 * One summarization request, streamed to its terminal message. Its span follows the generation's request span; a
 * request that stops with `error` ends it with a failure and
 * the task classifies the message.
 */
const summaryResponse = (
  runtime: Runtime,
  model: ResolvedModel,
  request: GenerateRequest,
  attempt: number,
) =>
  Effect.useSpan("durable.compaction.request", (span) => {
    modelTelemetry.annotateRequest(span, model, {
      "durable.task.id": runtime.taskId,
      "durable.conversation.id": runtime.conversationId,
      "durable.compaction.attempt": attempt,
    });
    return Effect.gen(function* () {
      const models = yield* Models;
      let final: AssistantMessage | undefined;
      yield* models.stream(model, request).pipe(
        Stream.runForEach((event) =>
          Effect.sync(() => {
            if (event.type === "done") final = event.message;
            else if (event.type === "error") final = event.error;
          }),
        ),
      );
      const message = final ?? streamFailure(model);
      const failed = modelTelemetry.annotateResponse(span, message);
      return failed === undefined ? message : yield* failed;
    }).pipe(Effect.withParentSpan(span));
  }).pipe(Effect.catchTag("ModelRequestFailed", (failed) => Effect.succeed(failed.result)));

/** A terminal error message for a stream that broke the `Models` contract. */
const streamFailure = (model: ResolvedModel): AssistantMessage => ({
  role: "assistant",
  content: [],
  api: model.api,
  provider: model.provider,
  model: model.id,
  usage: {
    input: 0,
    output: 0,
    cacheRead: 0,
    cacheWrite: 0,
    totalTokens: 0,
    cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
  },
  stopReason: "error",
  errorMessage: "Model stream ended without a final message",
  timestamp: 0,
});

/** Places a summary a hook supplied, in its own commit. */
const place = (runtime: Runtime, firstKept: EntryId, summary: string) =>
  runtime.commit((current) =>
    Effect.gen(function* () {
      const tx = yield* Tx;
      const live = yield* tx.doc(LiveDoc, runtime.conversationId);
      return yield* placeSummary(runtime, current, live, firstKept, summary);
    }),
  );

/**
 * Places the summary entry and completes. A blocking
 * compaction, owned by its generation, appends it: the generation holds the run and waits. A conversation-owned one
 * admits it as a write submission, placed at once when idle, otherwise at the next boundary, or settled `stale`.
 * Nothing else may append to a busy conversation, so every non-blocking summary goes through admission.
 */
const placeSummary = (
  runtime: Runtime,
  current: Task.RunningTask<CompactionInput, CompactionCheckpoint, CompactionResult>,
  live: LiveDraft,
  firstKept: EntryId,
  summary: string,
) =>
  Effect.gen(function* () {
    const tx = yield* Tx;
    removeCompactionStatus(live, runtime.taskId);
    delete live.compactionFailure;
    const now = yield* runtime.now;
    const text: TextContent = {
      type: "text",
      text: `${SUMMARY_PREFIX}${summary}${SUMMARY_SUFFIX}`,
    };
    const entry = CompactionEntry.draft(
      { reason: current.input.reason },
      { head: firstKept, model: [{ role: "user", content: [text], timestamp: now }] },
    );
    const result: CompactionResult =
      current.owner === undefined
        ? {
            submissionId: (yield* admitSubmission(
              runtime.conversationId,
              { type: "write", requestId: `compaction:${runtime.taskId}`, entry },
              now,
              yield* runtime.settings,
            )).id,
          }
        : { entryId: (yield* tx.appendEntry(runtime.conversationId, entry)).id };
    return { status: "terminal", outcome: { status: "completed", result } } satisfies Next;
  });

/** Removes the status and completes without a summary. */
const complete = (runtime: Runtime) =>
  runtime.commit(
    Effect.gen(function* () {
      const tx = yield* Tx;
      removeCompactionStatus(yield* tx.doc(LiveDoc, runtime.conversationId), runtime.taskId);
      return { status: "terminal", outcome: { status: "completed", result: {} } } satisfies Next;
    }),
  );

/**
 * Whether the task was cut off `settings.restartLimit` times in a row.
 * The settings are read only for a task that was cut off at all.
 */
const cutOffAtLimit = (task: { readonly restarts?: number | undefined }, runtime: Runtime) =>
  task.restarts === undefined
    ? Effect.succeed(false)
    : Effect.map(runtime.settings, (settings) => cutOffTooOften(task, settings.restartLimit));

/**
 * Fails a compaction cut off `settings.restartLimit` times in a row into `core.live.compactionFailure`, instead of
 * running it again.
 */
const failCutOff = (runtime: Runtime, task: { readonly restarts?: number | undefined }) => {
  const message = `Compaction was interrupted ${cutOffCount(task)}`;
  return runtime.commit(
    Effect.gen(function* () {
      const tx = yield* Tx;
      failCompactionStatus(yield* tx.doc(LiveDoc, runtime.conversationId), runtime.taskId, message);
      return {
        status: "terminal",
        outcome: { status: "failed", error: { message, detail: { reason: "restart_limit" } } },
      } satisfies Next;
    }),
  );
};

/** Fails a compaction with no model configured, or whose model is not available. */
const failNoModel = (runtime: Runtime, ref: ModelRef | undefined) => {
  const message =
    ref === undefined
      ? "No model is configured"
      : `Model ${ref.provider}/${ref.modelId} is not available`;
  return runtime.commit(
    Effect.gen(function* () {
      const tx = yield* Tx;
      failCompactionStatus(yield* tx.doc(LiveDoc, runtime.conversationId), runtime.taskId, message);
      return {
        status: "terminal",
        outcome: { status: "failed", error: { message, detail: { reason: "no_model" } } },
      } satisfies Next;
    }),
  );
};

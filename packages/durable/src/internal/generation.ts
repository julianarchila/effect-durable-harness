// The built-in generation task. It prepares the positional system prompt and tool loadout, requests or polls the
// model with throttled partials, retries, classifies the response, and runs the tool round. The run's inputs live
// in `core.live.run`. Deferred responses are the `poll` phase and the `poll` a `retry`
// keeps, `classify`'s `deferred` branch, and the cancellations in `interrupted`, `endCutOff` and `abort`
// (`cancelDeferred`). Compaction enters at three places, each marked "compaction": `prepare`'s checks of a blocking
// compaction it waited for and its threshold check, and `classify`'s overflow recovery; the task and its pure parts
// are `internal/compaction.ts`.
import * as Json from "@effect-durable-harness/delta/Json";
import type * as Tracker from "@effect-durable-harness/delta/Tracker";
import * as Cause from "effect/Cause";
import * as Clock from "effect/Clock";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as Predicate from "effect/Predicate";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import type * as Types from "effect/Types";
import type { ToolControl } from "../AgentTool.ts";
import { AssistantEntry, ResetEntry, SystemEntry, UserEntry } from "../Entry.ts";
import {
  type AfterToolsDecision,
  type GenerationHooks,
  type HookTask,
  RunSummary,
} from "../Hook.ts";
import {
  type ConversationId,
  type EntryId,
  EntryIdValue,
  type SubmissionId,
  type TaskId,
  TaskIdValue,
} from "../Ids.ts";
import { LiveDoc, type LiveState, type ToolSlot } from "../Live.ts";
import {
  type AssistantMessage,
  DeferredHandle,
  type Message,
  ThinkingLevel,
  type ToolCall,
  type UserContent,
} from "../Message.ts";
import {
  DeferredCancelFailed,
  type GenerateRequest,
  ModelRef,
  Models,
  type ResolvedModel,
  StreamOptions,
} from "../Models.ts";
import type * as Section from "../Section.ts";
import * as Task from "../Task.ts";
import type { TraceCarrier } from "../Telemetry.ts";
import { transactionIdentity } from "./transaction.ts";
import * as modelTelemetry from "./modelTelemetry.ts";
import type { AfterFailedRun } from "../Settings.ts";
import { Tx, type TxWriteError } from "../Tx.ts";
import { addTools, agentHooks, sectionExtension } from "./agent.ts";
import {
  classifyAnswer,
  classifyError,
  classifyInterruption,
  decideFinal,
  decideRoundEnd,
  nextPollAt,
  planToolRound,
} from "./generationPlan.ts";
import { endedByCrashes, stoppedBy } from "./decisions.ts";
import {
  applyBoundary,
  type Boundary,
  hasQueuedReset,
  prepareBoundary,
  withdrawQueuedInputs,
} from "./inbox.ts";
import type { JsonRecord } from "./json.ts";
import { PartialChanges, type PartialMessage } from "./partialChanges.ts";
import {
  appendAssistant,
  convertPartial,
  cutOffCount,
  cutOffTooOften,
  endRun,
  PROGRESS_COMMIT,
  type RunEnd,
  salvageable,
} from "./live.ts";
import { type InstalledTask, InstalledTasks } from "./installed.ts";
import { planSystemEntries, renderSections, replaySections, toolDeclaration } from "./prompt.ts";
import { ensureProviderSessionId } from "./provider.ts";
import {
  compactionFailureText,
  compactionPolicy,
  createCompaction,
  selectCut,
  thresholdCompaction,
} from "./compaction.ts";
import { isContextOverflow } from "./classify.ts";
import type { CompactionResult } from "../Compaction.ts";
import * as Signal from "./signal.ts";
import { appendToolResult, harnessError, ToolTask, type ToolTaskResult } from "./tool.ts";
import { getCurrentTools } from "./transcript.ts";

/** The deferred response a poll was fetching when the host died. */
const PolledResponse = Schema.Struct({
  model: ModelRef,
  thinkingLevel: Schema.optionalKey(ThinkingLevel),
  cutoff: EntryIdValue,
  handle: DeferredHandle,
});

const GenerationCheckpoint = Schema.Union([
  Schema.Struct({
    phase: Schema.Literal("prepare"),
    attempt: Schema.Number,
    /** The blocking compaction this generation waited for; it starts no other compaction. */
    compacted: Schema.optionalKey(TaskIdValue),
    /** Error text of the overflow that started `compacted`; checked once when `prepare` resumes. */
    overflow: Schema.optionalKey(Schema.String),
    /** Set when the blocking threshold started `compacted`; its outcome is checked once when `prepare` resumes. */
    threshold: Schema.optionalKey(Schema.Literal(true)),
  }),
  Schema.Struct({
    phase: Schema.Literal("request"),
    attempt: Schema.Number,
    compacted: Schema.optionalKey(TaskIdValue),
    model: ModelRef,
    thinkingLevel: ThinkingLevel,
    /** The settings' request options when preparation committed; a resend after recovery uses them unchanged. */
    streamOptions: StreamOptions,
    /** Newest entry included in the request. */
    cutoff: EntryIdValue,
  }),
  Schema.Struct({
    phase: Schema.Literal("retry"),
    attempt: Schema.Number,
    compacted: Schema.optionalKey(TaskIdValue),
    until: Schema.Number,
    /**
     * The deferred response a poll was fetching when the host died: the next attempt polls it again instead of
     * sending another request, so the provider's job is neither lost nor paid twice. Absent otherwise.
     */
    poll: Schema.optionalKey(PolledResponse),
  }),
  Schema.Struct({
    phase: Schema.Literal("poll"),
    attempt: Schema.Number,
    compacted: Schema.optionalKey(TaskIdValue),
    model: ModelRef,
    /** Absent in checkpoints of earlier versions. */
    thinkingLevel: Schema.optionalKey(ThinkingLevel),
    cutoff: EntryIdValue,
    handle: DeferredHandle,
    pollAt: Schema.Number,
  }),
  Schema.Struct({
    /** Waiting on the round's tool tasks, which the generation owns. */
    phase: Schema.Literal("tools"),
    /** The tool-calling answer. */
    assistant: EntryIdValue,
    /** Tool tasks created so far, in call order; grows by one per started call of a sequential round. */
    tools: Schema.Array(TaskIdValue),
    /** Calls of a sequential round not started yet, in call order. */
    pending: Schema.Array(Schema.String),
  }),
  Schema.Struct({
    /**
     * The run answered: its answer is committed and its inputs settled, and it keeps `core.live.run` while
     * `afterRun` hooks are asked for tasks. A crash here asks them again: nothing was created.
     */
    phase: Schema.Literal("finish"),
    run: RunSummary,
  }),
  Schema.Struct({
    /** Waiting on the tasks `afterRun` hooks asked for, which the generation owns; then the run ends. */
    phase: Schema.Literal("drain"),
    answer: EntryIdValue,
    tasks: Schema.Array(TaskIdValue),
  }),
  Schema.Struct({
    /**
     * The round is complete and waits on the tasks `afterTools` hooks asked for, which the generation owns; then
     * it ends as the `tools` phase ends it. A crash before they exist asks the hooks again.
     */
    phase: Schema.Literal("afterTools"),
    assistant: EntryIdValue,
    tools: Schema.Array(TaskIdValue),
    tasks: Schema.Array(TaskIdValue),
  }),
  Schema.Struct({
    /**
     * The answer is appended, and the run's inputs stay open while the tasks an `onYield` hook asked for, which
     * the generation owns, decide whether it stands (`LiveRun.yield`).
     */
    phase: Schema.Literal("yield"),
    answer: EntryIdValue,
    tasks: Schema.Array(TaskIdValue),
  }),
]);
type GenerationCheckpoint = typeof GenerationCheckpoint.Type;

const GenerationResult = Schema.Struct({ entryId: EntryIdValue });
type GenerationResult = typeof GenerationResult.Type;

const GenerationInput = Schema.Struct({});
type GenerationInput = typeof GenerationInput.Type;

type Runtime = Task.TaskRuntime<GenerationInput, GenerationCheckpoint, GenerationResult>;

/** The generation task's name, as hooks name it. */
const GENERATION = "core.generation";
type Next = Task.NextTaskState<GenerationCheckpoint, GenerationResult>;
type LiveDraft = Tracker.Draft<LiveState>;

/** What classification needs from the request that produced a message. */
interface Request {
  readonly attempt: number;
  /** The blocking compaction this generation waited for, which rules out another. */
  readonly compacted?: TaskId | undefined;
  readonly model: ModelRef;
  /**
   * The thinking level the request asked for, recorded on the response; absent for a poll checkpoint an earlier
   * version stored, whose response records none.
   */
  readonly thinkingLevel?: ThinkingLevel | undefined;
  readonly cutoff: GenerationCheckpointOf<"request">["cutoff"];
  /** Committed model context through `cutoff`, when the phase already derived it. */
  readonly messages?: ReadonlyArray<Message> | undefined;
  /** Set when the message came from polling, so a still deferred result polls strictly later. */
  readonly pollAt?: number | undefined;
}

type GenerationCheckpointOf<P extends GenerationCheckpoint["phase"]> = Extract<
  GenerationCheckpoint,
  { readonly phase: P }
>;

/**
 * Built-in generation task: prepares the positional system prompt and tool loadout, requests or polls the
 * model, retries, and classifies the response. The run's inputs live in `core.live.run`.
 */
export const GenerationTask = Task.make({
  name: GENERATION,
  // Version 2 leaves calls to tools not offered in a sequential round's `pending`,
  // flagged `unoffered` in their slots, and adds `finish` and `drain`. A version 1 Harness would start those calls'
  // tool tasks unflagged, resolving the name, so it leaves version 2 blocked (`task_too_old`). A version 1
  // checkpoint has no such calls and is kept as it is. Version 3 adds `afterTools` and `yield`; older checkpoints
  // are kept as they are.
  version: 3,
  input: GenerationInput,
  checkpoint: GenerationCheckpoint,
  result: GenerationResult,
  initial: () => ({ phase: "prepare", attempt: 1 }),
  migrate: (input, checkpoint) => ({
    input: Schema.decodeUnknownSync(GenerationInput)(input),
    checkpoint: Schema.decodeUnknownSync(GenerationCheckpoint)(checkpoint),
  }),
  phases: {
    /**
     * Renders the system prompt and tool loadout, appends the positional `core.system` entries they need, and moves
     * to `request`. The agent and settings resolved here are fixed for this request. Only the Harness writes model
     * context to a busy conversation, so the context read here is still current at the commit.
     */
    prepare: (task, runtime) =>
      Effect.gen(function* () {
        const limit = yield* cutOffLimit(task, runtime);
        if (limit !== undefined) return yield* endCutOff(runtime, task, limit);
        const conversationId = runtime.conversationId;
        const agent = yield* runtime.agent;
        const settings = yield* runtime.settings;
        const model = agent.model;
        const models = yield* Models;
        const resolved =
          model === undefined ? Option.none<ResolvedModel>() : yield* models.resolve(model);
        if (model === undefined || Option.isNone(resolved))
          return yield* failNoModel(runtime, model);
        const checkpoint = task.state.checkpoint;
        const attempt = checkpoint.attempt;
        // compaction — the blocking compaction this generation waited for is checked
        // once. An overflow fails unless it placed a summary; a threshold compaction that found nothing to compact or
        // was declined leaves a request that may still fit, while one that failed left a context it judged too
        // large: sending it would at best leave no room for the answer.
        // SAFETY: `compacted` is the ID of a compaction task this generation created (`createCompaction`).
        const compacted = checkpoint.compacted as TaskId<CompactionResult> | undefined;
        if (compacted !== undefined && checkpoint.overflow !== undefined) {
          const [outcome] = yield* runtime.outcomes([compacted]);
          if (outcome?.status !== "completed" || outcome.result.entryId === undefined) {
            return yield* failModelError(runtime, checkpoint.overflow);
          }
        }
        if (compacted !== undefined && checkpoint.threshold === true) {
          const [outcome] = yield* runtime.outcomes([compacted]);
          if (outcome !== undefined && outcome.status !== "completed") {
            return yield* failModelError(runtime, compactionFailureText(outcome));
          }
        }
        const view = yield* runtime.context(conversationId);
        // The model owes an answer only to a context ending in a user message or
        // tool results; system messages say nothing it answers. Otherwise, as for a continue after an answer or
        // in an empty conversation, the run ends before any request.
        const last = view.messages.findLast((message) => message.role !== "system");
        if (last?.role !== "user" && last?.role !== "toolResult") {
          return yield* failNothingToAnswer(runtime);
        }
        const shown = replaySections(view.messages);
        // A failure to build the environment is reported, and the sections render without one.
        const env = yield* runtime.env.pipe(
          Effect.catchTag("ExecutionEnvError", (error) =>
            runtime
              .report(Cause.fail(error), { kind: "host", callback: "env" })
              .pipe(Effect.as(undefined)),
          ),
        );
        const input: Section.PromptInput = {
          conversationId,
          agent,
          env,
          shown: Object.fromEntries(shown),
          snapshot: runtime.snapshot,
          snapshotAsOf: runtime.snapshotAsOf,
        };
        // A failing section names the extension whose code it runs.
        const desired = yield* renderSections(agent.sections, input, shown, (cause, section) =>
          runtime
            .report(cause, { kind: "extension", extension: sectionExtension(agent, section.key) })
            .pipe(Effect.ignore),
        );
        const now = yield* runtime.now;
        const entries = planSystemEntries(view, desired, agent.tools.map(toolDeclaration), now);
        // compaction — above the blocking threshold, compact first and prepare again;
        // the transcript is unchanged until the compaction appends.
        const threshold =
          compacted === undefined
            ? thresholdCompaction(
                view,
                entries,
                resolved.value.contextWindow,
                compactionPolicy(settings, model, agent.compaction),
              )
            : undefined;
        if (threshold === "blocking") {
          return yield* runtime.commit(
            Effect.gen(function* () {
              const child = yield* createCompaction(
                conversationId,
                { reason: "threshold" },
                runtime.taskId,
              );
              return {
                status: "waiting",
                checkpoint: { phase: "prepare", attempt, compacted: child, threshold: true },
                on: [child],
                policy: "allSettled",
              } satisfies Next;
            }),
          );
        }
        yield* runtime.commit(() =>
          Effect.gen(function* () {
            const tx = yield* Tx;
            let cutoff = (yield* tx.scanEntries({ conversationId }, 1)).items[0]?.id;
            for (const entry of entries)
              cutoff = (yield* tx.appendEntry(SystemEntry, conversationId, entry)).id;
            // The context ends in a message the model answers, so the conversation has an entry.
            if (cutoff === undefined) {
              return yield* Effect.die(
                new Error(`Conversation ${conversationId} has no entries to send`),
              );
            }
            // compaction — checked in this commit, so a compaction admitted during
            // preparation counts.
            if (
              threshold === "background" &&
              (yield* tx.doc(LiveDoc, conversationId)).compactions === undefined
            ) {
              yield* createCompaction(conversationId, { reason: "threshold" });
            }
            return {
              status: "running",
              checkpoint: withCompacted(
                {
                  phase: "request",
                  attempt,
                  model,
                  thinkingLevel: agent.thinkingLevel,
                  streamOptions: settings.stream,
                  cutoff,
                },
                compacted,
              ),
            } satisfies Next;
          }),
        );
      }),
    /**
     * Converts a partial an interrupted attempt left, resolves the pinned
     * model, runs `beforeRequest`, streams with throttled partials, and classifies the terminal message. A
     * request the host died during is a failed attempt, not a resend; after a close, which is not a
     * crash, the same committed messages are sent again with the same pinned options.
     */
    request: (task, runtime) =>
      Effect.gen(function* () {
        const checkpoint = task.state.checkpoint;
        const conversationId = runtime.conversationId;
        // Charging the attempt may itself be what keeps failing, such as converting a huge partial: the limit
        // ends it.
        const limit = yield* cutOffLimit(task, runtime);
        if (limit !== undefined) return yield* endCutOff(runtime, task, limit);
        if (task.restarts !== undefined) return yield* interrupted(runtime, checkpoint);
        // The attempt shown is presentation, stored like progress
        // (`PROGRESS_COMMIT`); converting a partial left by an interrupted attempt appends an entry, which makes the
        // commit immediate.
        yield* runtime.commit(
          () =>
            Effect.gen(function* () {
              const tx = yield* Tx;
              const live = yield* tx.doc(LiveDoc, conversationId);
              yield* convertPartial(live, conversationId);
              live.generation = { attempt: checkpoint.attempt };
            }),
          PROGRESS_COMMIT,
        );
        const models = yield* Models;
        const model = yield* models.resolve(checkpoint.model);
        if (Option.isNone(model)) return yield* failNoModel(runtime, checkpoint.model);
        const view = yield* runtime.context(conversationId, { at: checkpoint.cutoff });
        let messages = view.messages;
        yield* runtime.hooks.each("beforeRequest", (hook: GenerationHooks["beforeRequest"]) =>
          Effect.map(hook({ messages }, runtime), (replaced) => {
            if (replaced !== undefined) messages = replaced.messages;
          }),
        );
        const sessionId = yield* ensureProviderSessionId(runtime);
        const request: GenerateRequest = {
          messages,
          reasoning: checkpoint.thinkingLevel === "off" ? undefined : checkpoint.thinkingLevel,
          sessionId,
          options: checkpoint.streamOptions,
          onPayload: (payload) => providerPayload(runtime, model.value, payload),
        };
        const message = yield* Effect.useSpan("durable.generation.request", (span) => {
          modelTelemetry.annotateRequest(span, model.value, {
            "durable.task.id": runtime.taskId,
            "durable.conversation.id": runtime.conversationId,
            "durable.generation.attempt": checkpoint.attempt,
          });
          return streamResponse(runtime, model.value, request, checkpoint.attempt).pipe(
            Effect.flatMap((result) => {
              const failed = modelTelemetry.annotateResponse(span, result);
              return failed === undefined ? Effect.succeed(result) : Effect.fail(failed);
            }),
            Effect.withParentSpan(span),
          );
        }).pipe(
          // A request that stopped with `error` ended its span with a failure; the run classifies it.
          Effect.catchTag("ModelRequestFailed", (failed) => Effect.succeed(failed.result)),
        );
        yield* classify(
          runtime,
          {
            attempt: checkpoint.attempt,
            compacted: checkpoint.compacted,
            model: checkpoint.model,
            thinkingLevel: checkpoint.thinkingLevel,
            cutoff: checkpoint.cutoff,
            messages: view.messages,
          },
          message,
        );
      }),
    /**
     * After the backoff's sleep, prepares again with the next attempt, or polls
     * again the deferred response a crash cut its fetch off from. A run before `until` (an early wake, or a record
     * an earlier version stored as a running phase) sleeps again.
     */
    retry: (task, runtime) =>
      Effect.gen(function* () {
        const checkpoint = task.state.checkpoint;
        const limit = yield* cutOffLimit(task, runtime);
        if (limit !== undefined) return yield* endCutOff(runtime, task, limit);
        if (checkpoint.until > (yield* runtime.now)) {
          return yield* runtime.sleepUntil(checkpoint.until, checkpoint);
        }
        const now = yield* runtime.now;
        yield* runtime.commit(() =>
          Effect.gen(function* () {
            const tx = yield* Tx;
            const live = yield* tx.doc(LiveDoc, runtime.conversationId);
            const attempt = checkpoint.attempt + 1;
            // A poll the host died during polls its response again, as the next
            // attempt, due now.
            if (checkpoint.poll !== undefined) {
              live.generation = { attempt, deferred: { pollAt: now } };
              return {
                status: "running",
                checkpoint: withCompacted(
                  { phase: "poll", attempt, ...checkpoint.poll, pollAt: now },
                  checkpoint.compacted,
                ),
              } satisfies Next;
            }
            live.generation = { attempt };
            return {
              status: "running",
              checkpoint: withCompacted({ phase: "prepare", attempt }, checkpoint.compacted),
            } satisfies Next;
          }),
        );
      }),
    /**
     * After the sleep until `pollAt`, fetches the deferred response once and
     * classifies it; a still deferred result sleeps again, strictly later. A run before `pollAt` (an early wake,
     * or a record of an earlier version) sleeps again.
     */
    poll: (task, runtime) =>
      Effect.gen(function* () {
        const checkpoint = task.state.checkpoint;
        const limit = yield* cutOffLimit(task, runtime);
        if (limit !== undefined) return yield* endCutOff(runtime, task, limit);
        // The host stopped while the poll's fetch was in flight: a failed attempt, as for a request. Checked before
        // the time, which may have moved backwards on a replacement host; a sleeping poll is never cut off.
        if (task.restarts !== undefined) return yield* interrupted(runtime, checkpoint);
        if (checkpoint.pollAt > (yield* runtime.now)) {
          return yield* runtime.sleepUntil(checkpoint.pollAt, checkpoint);
        }
        const models = yield* Models;
        const model = yield* models.resolve(checkpoint.model);
        if (Option.isNone(model)) return yield* failNoModel(runtime, checkpoint.model);
        const message = yield* Effect.useSpan("durable.generation.poll", (span) => {
          modelTelemetry.annotateRequest(span, model.value, {
            "durable.task.id": runtime.taskId,
            "durable.conversation.id": runtime.conversationId,
            "durable.generation.attempt": checkpoint.attempt,
          });
          return fetchDeferred(models, model.value, checkpoint.handle).pipe(
            Effect.flatMap((result) => {
              const failed = modelTelemetry.annotateResponse(span, result);
              return failed === undefined ? Effect.succeed(result) : Effect.fail(failed);
            }),
            Effect.withParentSpan(span),
          );
        }).pipe(Effect.catchTag("ModelRequestFailed", (failed) => Effect.succeed(failed.result)));
        yield* classify(
          runtime,
          {
            attempt: checkpoint.attempt,
            compacted: checkpoint.compacted,
            model: checkpoint.model,
            thinkingLevel: checkpoint.thinkingLevel,
            cutoff: checkpoint.cutoff,
            pollAt: checkpoint.pollAt,
          },
          message,
        );
      }),
    /** The next call of a sequential round, or the end of the round. */
    tools: (task, runtime) =>
      Effect.gen(function* () {
        const limit = yield* cutOffLimit(task, runtime);
        if (limit !== undefined) return yield* endCutOff(runtime, task, limit);
        const checkpoint = task.state.checkpoint;
        const [next, ...rest] = checkpoint.pending;
        if (next === undefined) {
          // SAFETY: the round's tasks are tool tasks, created by `createToolTask`.
          const tools = checkpoint.tools as ReadonlyArray<TaskId<ToolTaskResult>>;
          return yield* finishToolRound(runtime, checkpoint.assistant, tools);
        }
        yield* runtime.commit(() =>
          Effect.gen(function* () {
            const tx = yield* Tx;
            const live = yield* tx.doc(LiveDoc, runtime.conversationId);
            const slot = live.tools?.find(
              (each) => each.callId === next && each.taskId === undefined,
            );
            const taskId = yield* createToolTask(
              runtime,
              checkpoint.assistant,
              next,
              slot?.unoffered === true,
            );
            if (slot !== undefined) slot.taskId = taskId;
            return {
              status: "waiting",
              checkpoint: {
                phase: "tools",
                assistant: checkpoint.assistant,
                tools: [...checkpoint.tools, taskId],
                pending: rest,
              },
              on: [taskId],
              policy: "allSettled",
            } satisfies Next;
          }),
        );
      }),
    /**
     * Asks the `afterRun` hooks for tasks and creates them, as the generation's
     * children, in the commit that moves to `drain`; without any, ends the run now.
     */
    finish: (task, runtime) =>
      Effect.gen(function* () {
        const run = task.state.checkpoint.run;
        const limit = yield* cutOffLimit(task, runtime);
        if (limit !== undefined) return yield* endAnsweredCutOff(runtime, task, run.answer);
        const installed = yield* InstalledTasks;
        const prepared: Array<PreparedTask> = [];
        yield* runtime.hooks.each("afterRun", (hook: GenerationHooks["afterRun"]) =>
          Effect.flatMap(hook(run, runtime), (asked) =>
            asked === undefined ? Effect.void : prepareHookTasks(installed, asked.tasks, prepared),
          ),
        );
        if (prepared.length === 0) return yield* endAnsweredRun(runtime, run.answer);
        yield* runtime.commit(() =>
          Effect.gen(function* () {
            const tx = yield* Tx;
            const tasks = yield* createHookTasks(runtime, prepared);
            const live = yield* tx.doc(LiveDoc, runtime.conversationId);
            if (live.run?.taskId === runtime.taskId && live.run.afterRun !== undefined) {
              live.run.afterRun.tasks = tasks;
            }
            return {
              status: "waiting",
              checkpoint: { phase: "drain", answer: run.answer, tasks },
              on: tasks,
              policy: "allSettled",
            } satisfies Next;
          }),
        );
      }),
    /** The after-run tasks are terminal, whatever their outcomes: ends the run. */
    drain: (task, runtime) =>
      Effect.gen(function* () {
        const answer = task.state.checkpoint.answer;
        const limit = yield* cutOffLimit(task, runtime);
        if (limit !== undefined) return yield* endAnsweredCutOff(runtime, task, answer);
        yield* endAnsweredRun(runtime, answer);
      }),
    /** The `afterTools` tasks are terminal: ends the round, and the run too when one asked to terminate. */
    afterTools: (task, runtime) =>
      Effect.gen(function* () {
        const limit = yield* cutOffLimit(task, runtime);
        if (limit !== undefined) return yield* endCutOff(runtime, task, limit);
        const checkpoint = task.state.checkpoint;
        const outcomes = yield* runtime.outcomes(jsonTasks(checkpoint.tasks));
        let terminate = false;
        for (const [index, outcome] of outcomes.entries()) {
          if (outcome.status !== "completed") continue;
          const decision = afterToolsDecision(outcome.result);
          if (decision?.terminate === true) terminate = true;
          if (decision !== undefined) continue;
          yield* reportNoDecision(runtime, checkpoint.tasks[index]);
        }
        // SAFETY: the round's tasks are tool tasks, created by `createToolTask`.
        const tools = checkpoint.tools as ReadonlyArray<TaskId<ToolTaskResult>>;
        yield* endToolRound(runtime, checkpoint.assistant, tools, terminate);
      }),
    /**
     * The `onYield` tasks are terminal: the first in creation order that completed with a continuation continues
     * the run, as a hook's `continue` does; otherwise the answer stands and the run ends with it.
     */
    yield: (task, runtime) =>
      Effect.gen(function* () {
        const checkpoint = task.state.checkpoint;
        // The run answered: the limit ends it with the answer.
        const limit = yield* cutOffLimit(task, runtime);
        if (limit !== undefined) return yield* endAnsweredCutOff(runtime, task, checkpoint.answer);
        const outcomes = yield* runtime.outcomes(jsonTasks(checkpoint.tasks));
        const continuation = outcomes.map(yieldContinuation).find((input) => input !== undefined);
        const afterRun = yield* asksAfterRun(runtime);
        yield* runtime.commit(() =>
          Effect.gen(function* () {
            const tx = yield* Tx;
            const boundary = yield* prepareBoundary(
              runtime.conversationId,
              yield* runtime.settings,
            );
            const live = yield* tx.doc(LiveDoc, runtime.conversationId);
            if (live.run?.taskId === runtime.taskId) delete live.run.yield;
            return yield* concludeAnswer(
              runtime,
              live,
              boundary,
              checkpoint.answer,
              continuation,
              afterRun,
            );
          }),
        );
      }),
  },
  /**
   * Cancels a deferred response it polled; runs after the round's tool tasks are
   * terminal; calls never started get `aborted` results, unoffered ones `tool_unavailable`, a committed partial is
   * converted, and the run's inputs
   * settle `unanswered`. In `finish` and `drain` the run answered already: it ends without a failure, its inputs
   * stay `done` (`endRun`). In `yield` it answered too, and its inputs settle `done` with that answer.
   */
  abort: (task, runtime) =>
    Effect.gen(function* () {
      const checkpoint = task.state.checkpoint;
      // A deferred response no attempt will poll is cancelled.
      const polled = deferredOf(checkpoint);
      if (polled !== undefined) yield* cancelDeferred(runtime, polled);
      const conversationId = runtime.conversationId;
      const unstarted =
        checkpoint.phase === "tools"
          ? yield* readCalls(runtime, checkpoint.assistant, checkpoint.pending)
          : [];
      const now = yield* runtime.now;
      yield* runtime.commit(() =>
        Effect.gen(function* () {
          const tx = yield* Tx;
          const live = yield* tx.doc(LiveDoc, conversationId);
          yield* convertPartial(live, conversationId);
          for (const call of unstarted) {
            // A call its request did not offer could not have run either way.
            const unoffered =
              live.tools?.find((slot) => slot.callId === call.id)?.unoffered === true;
            const result = unoffered
              ? harnessError("tool_unavailable", `Tool ${call.name} is not available`)
              : harnessError("aborted", `Tool ${call.name} was aborted`);
            yield* appendToolResult(conversationId, call, result, { timestamp: now });
          }
          yield* endRun(
            live,
            runtime.taskId,
            checkpoint.phase === "yield"
              ? { status: "done", answer: checkpoint.answer }
              : { status: "unanswered", reason: "aborted" },
          );
          return { status: "terminal", outcome: { status: "aborted" } } satisfies Next;
        }),
      );
    }),
});

/**
 * compaction — `checkpoint` carrying the blocking compaction this generation waited for, which `request`, `retry`
 * and the next `prepare` keep.
 */
const withCompacted = <C extends GenerationCheckpoint>(
  checkpoint: C,
  compacted: TaskId | undefined,
): C => (compacted === undefined ? checkpoint : { ...checkpoint, compacted });

/** The calls `callIds` of the assistant entry, in the given order. */
const readCalls = (
  runtime: Runtime,
  assistant: GenerationCheckpointOf<"tools">["assistant"],
  callIds: ReadonlyArray<string>,
) =>
  Effect.gen(function* () {
    const found = yield* runtime.entry(AssistantEntry, assistant);
    const message = Option.getOrUndefined(found)?.model?.[0];
    const calls =
      message?.role === "assistant"
        ? message.content.filter((content): content is ToolCall => content.type === "toolCall")
        : [];
    return callIds.flatMap((id) => calls.filter((call) => call.id === id).slice(0, 1));
  });

/**
 * A tool task for call `callId`, owned by the generation;
 * `unoffered` for a call to a tool its request did not offer.
 */
const createToolTask = (
  runtime: Runtime,
  assistant: GenerationCheckpointOf<"tools">["assistant"],
  callId: string,
  unoffered: boolean,
) =>
  Effect.gen(function* () {
    const tx = yield* Tx;
    return yield* tx.createTask(
      ToolTask,
      unoffered
        ? { kind: "model", assistant, callId, unoffered: true }
        : { kind: "model", assistant, callId },
      { ownership: { kind: "task", taskId: runtime.taskId } },
    );
  });

/**
 * The restart limit when the task was cut off that often, so its
 * phase ends it instead of running. The settings are read only for a task that was cut off at all, so the
 * normal path resolves nothing more.
 */
const cutOffLimit = (task: { readonly restarts?: number | undefined }, runtime: Runtime) =>
  Effect.gen(function* () {
    if (task.restarts === undefined) return undefined;
    const limit = (yield* runtime.settings).restartLimit;
    return cutOffTooOften(task, limit) ? limit : undefined;
  });

/**
 * The host stopped while this attempt's request or poll was in flight (`TaskRecord.restarts`), a failed attempt
 * under `settings.retry`. The committed partial becomes an
 * aborted entry; the next attempt follows the backoff, a poll's by polling its deferred response again, or, with
 * no retry left, the run ends `interrupted`.
 */
const interrupted = (
  runtime: Runtime,
  checkpoint: GenerationCheckpointOf<"request"> | GenerationCheckpointOf<"poll">,
) =>
  Effect.gen(function* () {
    const attempt = checkpoint.attempt;
    const polled = checkpoint.phase === "poll" ? checkpoint : undefined;
    const classification = classifyInterruption(
      (yield* runtime.settings).retry,
      attempt,
      yield* runtime.now,
      polled !== undefined,
    );
    // A run that ends does not leave the provider's job running.
    if (classification._tag === "Fail" && polled !== undefined) {
      yield* cancelDeferred(runtime, polled);
    }
    const conversationId = runtime.conversationId;
    yield* runtime.commit(() =>
      Effect.gen(function* () {
        const tx = yield* Tx;
        if (classification._tag === "Retry") {
          const live = yield* tx.doc(LiveDoc, conversationId);
          yield* convertPartial(live, conversationId);
          live.generation = {
            attempt,
            retry: { at: classification.until, error: classification.error },
          };
          const retry: Types.Mutable<GenerationCheckpointOf<"retry">> = {
            phase: "retry",
            attempt,
            until: classification.until,
          };
          // The next attempt polls the same response again.
          if (polled !== undefined) retry.poll = polledResponse(polled);
          return {
            status: "sleeping",
            checkpoint: withCompacted(retry, checkpoint.compacted),
            until: classification.until,
          } satisfies Next;
        }
        // Read before the first table write.
        const failure = yield* prepareFailure(runtime);
        const live = yield* tx.doc(LiveDoc, conversationId);
        yield* convertPartial(live, conversationId);
        yield* endFailedRun(runtime, live, failure, {
          status: "unanswered",
          reason: "interrupted",
          detail: classification.text,
        });
        return {
          status: "terminal",
          outcome: {
            status: "failed",
            error: { message: classification.text, detail: { reason: "interrupted" } },
          },
        } satisfies Next;
      }),
    );
  });

/**
 * Ends a run whose generation was cut off `settings.restartLimit` times in a row. At the limit it still keeps a
 * committed partial as an aborted entry and cancels a deferred response it polled; past the limit that cleanup may
 * be what cut it off, so it ends without it.
 */
const endCutOff = (
  runtime: Runtime,
  task: {
    readonly restarts?: number | undefined;
    readonly state: { readonly checkpoint: GenerationCheckpoint };
  },
  limit: number,
) => {
  const text = `The generation was interrupted ${cutOffCount(task)} in its ${task.state.checkpoint.phase} phase`;
  const salvage = salvageable(task, limit);
  const polled = deferredOf(task.state.checkpoint);
  const cancel = salvage && polled !== undefined ? cancelDeferred(runtime, polled) : Effect.void;
  return cancel.pipe(
    Effect.andThen(
      runtime.commit(() =>
        Effect.gen(function* () {
          const tx = yield* Tx;
          const failure = yield* prepareFailure(runtime);
          const live = yield* tx.doc(LiveDoc, runtime.conversationId);
          if (salvage) yield* convertPartial(live, runtime.conversationId);
          yield* endFailedRun(runtime, live, failure, {
            status: "unanswered",
            reason: "restart_limit",
            detail: text,
          });
          return {
            status: "terminal",
            outcome: {
              status: "failed",
              error: { message: text, detail: { reason: "restart_limit" } },
            },
          } satisfies Next;
        }),
      ),
    ),
  );
};

/**
 * The response a poll fetches, without a thinking level a poll checkpoint of an earlier version did not store.
 */
const polledResponse = (source: {
  readonly model: ModelRef;
  readonly thinkingLevel?: ThinkingLevel | undefined;
  readonly cutoff: GenerationCheckpointOf<"poll">["cutoff"];
  readonly handle: DeferredHandle;
}): typeof PolledResponse.Type => {
  const polled: Types.Mutable<typeof PolledResponse.Type> = {
    model: source.model,
    cutoff: source.cutoff,
    handle: source.handle,
  };
  if (source.thinkingLevel !== undefined) polled.thinkingLevel = source.thinkingLevel;
  return polled;
};

/** The deferred response a checkpoint polls, or will poll again. */
const deferredOf = (
  checkpoint: GenerationCheckpoint,
): { readonly model: ModelRef; readonly handle: DeferredHandle } | undefined => {
  if (checkpoint.phase === "poll") return checkpoint;
  return checkpoint.phase === "retry" ? checkpoint.poll : undefined;
};

/**
 * Cancels a deferred response no attempt will poll again. A
 * model gone is skipped; a failure is reported as a `host_error` with callback `models`, never raised.
 */
const cancelDeferred = (
  runtime: Runtime,
  polled: { readonly model: ModelRef; readonly handle: DeferredHandle },
) =>
  Effect.gen(function* () {
    const models = yield* Models;
    const model = yield* models.resolve(polled.model);
    if (Option.isNone(model)) return;
    // `Models.cancelDeferred` is optional: a provider without it rejects with this text.
    const cancel =
      models.cancelDeferred?.(model.value, polled.handle) ??
      Effect.fail(
        new DeferredCancelFailed({
          message: `Provider ${model.value.provider} does not support deferred responses`,
        }),
      );
    yield* cancel.pipe(
      Effect.catchCause((cause) =>
        stoppedBy(cause)
          ? Effect.failCause(cause)
          : runtime.report(cause, { kind: "host", callback: "models" }).pipe(Effect.ignore),
      ),
    );
  });

/**
 * `Models.fetchDeferred`: the deferred response once. The member is optional: a provider without it answers an
 * error message.
 */
const fetchDeferred = (
  models: Models["Service"],
  model: ResolvedModel,
  handle: DeferredHandle,
): Effect.Effect<AssistantMessage> =>
  models.fetchDeferred?.(model, handle) ??
  Effect.map(Clock.currentTimeMillis, (now) => ({
    ...failedMessage(model, `Provider ${model.provider} does not support deferred responses`),
    timestamp: now,
  }));

/**
 * Settles the run's inputs `unanswered` with `no_model` and
 * fails; `runFailure` names the missing model, the settlement has no detail.
 */
const failNoModel = (runtime: Runtime, ref: ModelRef | undefined) => {
  const message =
    ref === undefined
      ? "No model is configured"
      : `Model ${ref.provider}/${ref.modelId} is not available`;
  return runtime.commit(() =>
    Effect.gen(function* () {
      const tx = yield* Tx;
      const failure = yield* prepareFailure(runtime);
      const live = yield* tx.doc(LiveDoc, runtime.conversationId);
      yield* endFailedRun(runtime, live, failure, {
        status: "unanswered",
        reason: "no_model",
        detail: message,
        settlement: { status: "unanswered", reason: "no_model" },
      });
      return {
        status: "terminal",
        outcome: { status: "failed", error: { message, detail: { reason: "no_model" } } },
      } satisfies Next;
    }),
  );
};

/** Ends the run without a request. */
const failNothingToAnswer = (runtime: Runtime) =>
  runtime.commit(() =>
    Effect.gen(function* () {
      const tx = yield* Tx;
      const message = "The conversation has nothing to answer";
      const failure = yield* prepareFailure(runtime);
      const live = yield* tx.doc(LiveDoc, runtime.conversationId);
      yield* endFailedRun(runtime, live, failure, {
        status: "unanswered",
        reason: "nothing_to_answer",
      });
      return {
        status: "terminal",
        outcome: { status: "failed", error: { message, detail: { reason: "nothing_to_answer" } } },
      } satisfies Next;
    }),
  );

/**
 * compaction — settles the run's inputs `unanswered` with
 * `model_error` and fails with `text`, for a blocking or overflow compaction that did not compact.
 */
const failModelError = (runtime: Runtime, text: string) =>
  runtime.commit(() =>
    Effect.gen(function* () {
      const tx = yield* Tx;
      const failure = yield* prepareFailure(runtime);
      const live = yield* tx.doc(LiveDoc, runtime.conversationId);
      yield* endFailedRun(runtime, live, failure, {
        status: "unanswered",
        reason: "model_error",
        detail: text,
      });
      return {
        status: "terminal",
        outcome: { status: "failed", error: { message: text, detail: { reason: "model_error" } } },
      } satisfies Next;
    }),
  );

/** What a failed run's end needs from the table, read first. */
interface Failure {
  readonly policy: AfterFailedRun;
  readonly boundary?: Boundary | undefined;
}

/**
 * The `afterFailedRun` policy, and for `continue` the
 * boundary, which reads the table, so it comes before the commit's first table write.
 */
const prepareFailure = (runtime: Runtime) =>
  Effect.gen(function* () {
    const settings = yield* runtime.settings;
    const policy = settings.afterFailedRun;
    if (policy !== "continue") return { policy } satisfies Failure;
    const boundary = yield* prepareBoundary(runtime.conversationId, settings);
    return { policy, boundary } satisfies Failure;
  });

/**
 * Ends a failed run, settling its inputs, and does with the
 * inputs queued behind it what `settings.afterFailedRun` says: keep them, start the next run with
 * them, or withdraw them.
 */
const endFailedRun = (
  runtime: Runtime,
  live: LiveDraft,
  failure: Failure,
  end: Extract<RunEnd, { readonly status: "unanswered" }>,
) =>
  Effect.gen(function* () {
    const conversationId = runtime.conversationId;
    yield* endRun(live, runtime.taskId, end);
    if (failure.policy === "withdraw") yield* withdrawQueuedInputs(conversationId, "run_failed");
    if (failure.boundary === undefined) return;
    const placed = yield* applyBoundary(failure.boundary, "final", yield* runtime.now);
    if (placed.users.length > 0) {
      yield* startRun(conversationId, live, placed, { keepFailure: true });
    }
  });

/**
 * Streams one request and returns the terminal message. Partials commit as trailing writes at most every
 * `progress.partialIntervalMs` with one commit in flight. The throttle is a fiber of this region: closing the
 * region interrupts it, which waits for the commit in flight, so no stale partial lands after the outcome. A commit
 * appends what text and thinking grew by and applies the argument changes since the last one (`PartialChanges`), so
 * its cost follows the new text, not the strings' length.
 */
const streamResponse = (
  runtime: Runtime,
  model: ResolvedModel,
  request: GenerateRequest,
  attempt: number,
) =>
  Effect.scoped(
    Effect.gen(function* () {
      const models = yield* Models;
      const interval = (yield* runtime.settings).progress.partialIntervalMs;
      let pending: AssistantMessage | undefined;
      // A flush is waiting out its interval or committing. A partial that arrives meanwhile only replaces `pending`;
      // the interval starts when a partial arrives with the throttle idle, or when a commit ends with a newer partial
      // pending.
      let scheduled = false;
      const arrived = Signal.make();
      const changes = new PartialChanges();
      const flush = Effect.suspend(() => {
        const partial = pending;
        pending = undefined;
        if (partial === undefined) {
          scheduled = false;
          return Effect.void;
        }
        // Copied in the flushing step: a provider may keep mutating its
        // partial. The changes since the last commit belong to the copy.
        // SAFETY: a JSON copy of an `AssistantMessage`, whose `content` is an array.
        const message = Json.copy(partial, { omitUndefinedProperties: true }) as PartialMessage;
        const taken = changes.take(message);
        return runtime
          .commit(
            () =>
              Effect.gen(function* () {
                const tx = yield* Tx;
                const live = yield* tx.doc(LiveDoc, runtime.conversationId);
                live.generation ??= { attempt };
                // SAFETY: the draft's `generation` object, which takes the message's JSON leaf by leaf.
                changes.commit(live.generation as JsonRecord, message, taken);
              }),
            // Deferred, stored with the next commit that reads `core.live`.
            PROGRESS_COMMIT,
          )
          .pipe(
            // A failed commit puts back what it took.
            Effect.tapCause(() => Effect.sync(() => changes.restore(taken))),
            // A rejection after an abort mark or close is expected; the committed
            // state stays consistent. Others are reported.
            Effect.catchCause((cause) =>
              stoppedBy(cause) ? Effect.void : runtime.report(cause).pipe(Effect.ignore),
            ),
            // Once the commit ends, a newer partial starts the next interval.
            Effect.andThen(
              Effect.sync(() => {
                if (pending === undefined) scheduled = false;
                else arrived.fire();
              }),
            ),
            // One commit in flight: the throttle waits for it, and stopping the throttle waits for it too.
            Effect.uninterruptible,
          );
      });
      const throttle = yield* Effect.forkScoped(
        Effect.forever(
          arrived.wait.pipe(
            Effect.andThen(Effect.sleep(`${interval} millis`)),
            Effect.andThen(flush),
          ),
        ),
      );
      let final: AssistantMessage | undefined;
      yield* models.stream(model, request).pipe(
        Stream.runForEach((event) =>
          Effect.sync(() => {
            if (event.type === "done") {
              final = event.message;
              return;
            }
            if (event.type === "error") {
              final = event.error;
              return;
            }
            // A partial without content, such as the opening `start` event, shows nothing.
            if (event.partial.content.length === 0) return;
            changes.observe(event);
            pending = event.partial;
            if (scheduled) return;
            scheduled = true;
            arrived.fire();
          }),
        ),
      );
      pending = undefined;
      yield* Fiber.interrupt(throttle);
      return final ?? failedMessage(model, "Model stream ended without a final message");
    }),
  );

/**
 * The `beforeProviderRequest` replacement chain over one
 * provider request body, in extension order; a failing hook is reported and the chain continues.
 */
const providerPayload = (runtime: Runtime, model: ResolvedModel, payload: Schema.Json) =>
  Effect.gen(function* () {
    let current = payload;
    yield* runtime.hooks.each(
      "beforeProviderRequest",
      (hook: GenerationHooks["beforeProviderRequest"]) =>
        Effect.map(hook({ model, payload: current }, runtime), (replaced) => {
          if (replaced !== undefined) current = replaced.payload;
        }),
    );
    return current;
  }).pipe(
    // The chain reads only the phase's agent, which the request already resolved: a read failure here is a broken
    // invariant. A signalled invocation interrupts, which stops the request.
    Effect.orDie,
  );

/** A terminal error message for a stream that broke the `Models` contract, or a model service without polls. */
const failedMessage = (model: ResolvedModel, text: string): AssistantMessage => ({
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
  errorMessage: text,
  timestamp: 0,
});

/** Classifies a terminal message in one commit that clears the partial. */
const classify = (runtime: Runtime, request: Request, response: AssistantMessage) =>
  Effect.gen(function* () {
    // Records the requested level; a poll checkpoint an earlier version stored without one records none.
    const message: AssistantMessage =
      request.thinkingLevel === undefined
        ? response
        : { ...response, thinkingLevel: request.thinkingLevel };
    // A deferred response sleeps until its poll, which a still deferred result
    // of a poll moves strictly later. It is not terminal: `afterResponse` does not see it.
    if (message.stopReason === "deferred" && message.deferred !== undefined) {
      const handle = message.deferred;
      const pollAt = nextPollAt(handle, yield* runtime.now, request.pollAt);
      return yield* runtime.commit(() =>
        Effect.gen(function* () {
          const tx = yield* Tx;
          (yield* tx.doc(LiveDoc, runtime.conversationId)).generation = {
            attempt: request.attempt,
            deferred: { pollAt },
          };
          return {
            status: "sleeping",
            checkpoint: withCompacted(
              {
                phase: "poll",
                attempt: request.attempt,
                ...polledResponse({ ...request, handle }),
                pollAt,
              },
              request.compacted,
            ),
            until: pollAt,
          } satisfies Next;
        }),
      );
    }
    yield* runtime.hooks.each("afterResponse", (hook: GenerationHooks["afterResponse"]) =>
      hook(message, runtime),
    );
    const answered = classifyAnswer(message);
    if (answered._tag === "Tools")
      return yield* startToolRound(runtime, request, message, answered.calls);
    if (answered._tag === "Answer") return yield* answer(runtime, message);
    // The retry and compaction policies govern the next attempt, so they are read now rather than pinned at
    // preparation.
    const settings = yield* runtime.settings;
    const conversationId = runtime.conversationId;
    // compaction — a context overflow, while the policy is enabled, no compaction ran
    // for this generation yet, and range selection finds a cut, compacts and prepares the same attempt again; the
    // recovery request does not count against the retry policy.
    const overflow = message.stopReason === "error" && isContextOverflow(message);
    const compaction =
      overflow && request.compacted === undefined
        ? compactionPolicy(settings, request.model, (yield* runtime.agent).compaction)
        : undefined;
    if (compaction?.enabled === true) {
      const view = yield* runtime.context(conversationId, { at: request.cutoff });
      if (selectCut(view, compaction.keepRecentTokens) !== undefined) {
        const text = message.errorMessage ?? "Context overflow";
        return yield* runtime.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            const live = yield* tx.doc(LiveDoc, conversationId);
            yield* appendAssistant(conversationId, message);
            delete live.generation;
            const child = yield* createCompaction(
              conversationId,
              { reason: "overflow" },
              runtime.taskId,
            );
            return {
              status: "waiting",
              checkpoint: {
                phase: "prepare",
                attempt: request.attempt,
                compacted: child,
                overflow: text,
              },
              on: [child],
              policy: "allSettled",
            } satisfies Next;
          }),
        );
      }
    }
    const classification = classifyError(
      message,
      settings.retry,
      request.attempt,
      yield* runtime.now,
    );
    yield* runtime.commit(() =>
      Effect.gen(function* () {
        const tx = yield* Tx;
        if (classification._tag === "Retry") {
          const live = yield* tx.doc(LiveDoc, conversationId);
          yield* appendAssistant(conversationId, message);
          live.generation = {
            attempt: request.attempt,
            retry: { at: classification.until, error: classification.error },
          };
          // The backoff is a durable sleep, holding no invocation.
          return {
            status: "sleeping",
            checkpoint: withCompacted(
              { phase: "retry", attempt: request.attempt, until: classification.until },
              request.compacted,
            ),
            until: classification.until,
          } satisfies Next;
        }
        // Read before the first table write.
        const failure = yield* prepareFailure(runtime);
        const live = yield* tx.doc(LiveDoc, conversationId);
        yield* appendAssistant(conversationId, message);
        yield* endFailedRun(runtime, live, failure, {
          status: "unanswered",
          reason: "model_error",
          detail: classification.text,
        });
        return {
          status: "terminal",
          outcome: {
            status: "failed",
            error: { message: classification.text, detail: { reason: "model_error" } },
          },
        } satisfies Next;
      }),
    );
  });

/**
 * A final answer; the final boundary places queued items. The first `onYield` result wins: a continuation appends
 * a user message and hands the run to a successor generation, but only when the inbox holds no user item and no
 * reset; tasks decide whether the answer stands, in `yield`. Otherwise the run's inputs settle `done`; with
 * `afterRun` hooks selected the run waits for the tasks they ask for, in `finish` and `drain`, before the final
 * boundary; without, selected user items start the next run now.
 */
const answer = (runtime: Runtime, message: AssistantMessage) =>
  Effect.gen(function* () {
    let decided = false;
    let continuation: UserContent | undefined;
    const prepared: Array<PreparedTask> = [];
    const installed = yield* InstalledTasks;
    yield* runtime.hooks.each("onYield", (hook: GenerationHooks["onYield"]) =>
      decided
        ? Effect.void
        : Effect.flatMap(hook(message, runtime), (decision) => {
            if (decision === undefined) return Effect.void;
            decided = true;
            if ("tasks" in decision) return prepareHookTasks(installed, decision.tasks, prepared);
            continuation = decision.continue;
            return Effect.void;
          }),
    );
    const conversationId = runtime.conversationId;
    if (prepared.length > 0) {
      // The answer waits for its deciders: appended, no boundary, the inputs still open.
      return yield* runtime.commit(() =>
        Effect.gen(function* () {
          const tx = yield* Tx;
          const live = yield* tx.doc(LiveDoc, conversationId);
          const entry = yield* appendAssistant(conversationId, message);
          const tasks = yield* createHookTasks(runtime, prepared);
          delete live.generation;
          if (live.run?.taskId === runtime.taskId) live.run.yield = { answer: entry.id, tasks };
          return {
            status: "waiting",
            checkpoint: { phase: "yield", answer: entry.id, tasks },
            on: tasks,
            policy: "allSettled",
          } satisfies Next;
        }),
      );
    }
    const afterRun = yield* asksAfterRun(runtime);
    yield* runtime.commit(() =>
      Effect.gen(function* () {
        const tx = yield* Tx;
        // Queue modes are read on the Session line, when the boundary is decided.
        const boundary = yield* prepareBoundary(conversationId, yield* runtime.settings);
        const live = yield* tx.doc(LiveDoc, conversationId);
        const entry = yield* appendAssistant(conversationId, message);
        return yield* concludeAnswer(runtime, live, boundary, entry.id, continuation, afterRun);
      }),
    );
  });

/**
 * Settles an answer that stands, or takes its continuation: shared by `answer` and the `yield` phase. Whether the
 * continuation applies is decided from the inbox before the boundary places anything (`takesContinuation`);
 * without one, a run with `afterRun` hooks is held.
 */
const concludeAnswer = (
  runtime: Runtime,
  live: LiveDraft,
  boundary: Boundary,
  answer: EntryId,
  continuation: UserContent | undefined,
  afterRun: boolean,
) =>
  Effect.gen(function* () {
    const tx = yield* Tx;
    const conversationId = runtime.conversationId;
    const result = {
      status: "terminal",
      outcome: { status: "completed", result: { entryId: answer } },
    } satisfies Next;
    if (afterRun && !takesContinuation(continuation, boundary)) {
      const held = yield* holdRun(live, runtime, { reason: "answer", answer, last: answer });
      if (held !== undefined) return held;
    }
    const now = yield* runtime.now;
    const placed = yield* applyBoundary(boundary, "final", now);
    if (decideFinal(placed.users.length, placed.reset, continuation !== undefined) === "Continue") {
      // SAFETY: `Continue` is decided only with a continuation.
      const user = {
        role: "user",
        content: continuation as UserContent,
        timestamp: now,
      } as const;
      yield* tx.appendEntry(UserEntry, conversationId, { model: [user] });
      // The continuation starts the run's turn count over: bounding
      // `onYield` loops is the extension's job.
      if (live.run?.taskId === runtime.taskId) live.run.turns = 0;
      handOver(live, runtime.taskId, yield* createGeneration(conversationId));
      delete live.generation;
      return result;
    }
    yield* endRun(live, runtime.taskId, { status: "done", answer });
    if (placed.users.length > 0) yield* startRun(conversationId, live, placed);
    return result;
  });

/**
 * Appends the tool-calling answer and starts its round in one
 * commit. The offered tools are replayed from the request's context; whether the round is sequential is read
 * as it starts, and a tool is resolved as its tool task resolves it.
 */
const startToolRound = (
  runtime: Runtime,
  request: Request,
  message: AssistantMessage,
  calls: ReadonlyArray<ToolCall>,
) =>
  Effect.gen(function* () {
    const conversationId = runtime.conversationId;
    const messages =
      request.messages ?? (yield* runtime.context(conversationId, { at: request.cutoff })).messages;
    const offered = new Set(getCurrentTools(messages).map((tool) => tool.name));
    const tools = (yield* runtime.agent).tools;
    const settings = yield* runtime.settings;
    const sequential =
      settings.toolExecution === "sequential" ||
      calls.some(
        (call) =>
          offered.has(call.name) &&
          tools.find((tool) => tool.name === call.name)?.definition.executionMode === "sequential",
      );
    const plan = planToolRound(calls, offered, sequential);
    yield* runtime.commit(() =>
      Effect.gen(function* () {
        const tx = yield* Tx;
        const live = yield* tx.doc(LiveDoc, conversationId);
        const entry = yield* appendAssistant(conversationId, message);
        const slots: Array<Tracker.Draft<ToolSlot>> = [];
        const started: Array<TaskId<ToolTaskResult>> = [];
        const pending: Array<string> = [];
        for (const step of plan) {
          const call = step.call;
          // A call to a tool not offered gets a flagged task too.
          const unoffered = step.unoffered ? { unoffered: true as const } : {};
          if (step._tag === "Pending") {
            pending.push(call.id);
            slots.push({ callId: call.id, name: call.name, status: "pending", ...unoffered });
            continue;
          }
          const taskId = yield* createToolTask(runtime, entry.id, call.id, step.unoffered);
          started.push(taskId);
          slots.push({ callId: call.id, name: call.name, taskId, status: "pending", ...unoffered });
        }
        delete live.generation;
        live.tools = slots;
        if (live.run?.taskId === runtime.taskId) {
          // A tool round is one turn of the run (`AgentState.maxTurns`); a
          // run its boundary started without placing anything, as a continue after an answer, begins here.
          live.run.turns += 1;
          live.run.first ??= entry.id;
        }
        return {
          status: "waiting",
          checkpoint: { phase: "tools", assistant: entry.id, tools: started, pending },
          on: started,
          policy: "allSettled",
        } satisfies Next;
      }),
    );
  });

/**
 * The round's tools are terminal: asks the `afterTools` hooks, and waits for the tasks they ask for in
 * `afterTools`, or ends the round now.
 */
const finishToolRound = (
  runtime: Runtime,
  assistant: GenerationCheckpointOf<"tools">["assistant"],
  tools: ReadonlyArray<TaskId<ToolTaskResult>>,
) =>
  Effect.gen(function* () {
    const { results } = yield* roundSlots(runtime);
    const installed = yield* InstalledTasks;
    const prepared: Array<PreparedTask> = [];
    yield* runtime.hooks.each("afterTools", (hook: GenerationHooks["afterTools"]) =>
      Effect.flatMap(hook(assistant, results, runtime), (asked) =>
        asked === undefined ? Effect.void : prepareHookTasks(installed, asked.tasks, prepared),
      ),
    );
    if (prepared.length === 0) return yield* endToolRound(runtime, assistant, tools, false);
    yield* runtime.commit(() =>
      Effect.gen(function* () {
        const tasks = yield* createHookTasks(runtime, prepared);
        return {
          status: "waiting",
          checkpoint: { phase: "afterTools", assistant, tools, tasks },
          on: tasks,
          policy: "allSettled",
        } satisfies Next;
      }),
    );
  });

/** The round's slots in call order, and the result entries they hold. */
const roundSlots = (runtime: Runtime) =>
  Effect.map(runtime.snapshot(LiveDoc, runtime.conversationId), (live) => {
    const slots = Option.getOrUndefined(live)?.tools ?? [];
    return {
      slots,
      results: slots.flatMap((slot) => (slot.entry === undefined ? [] : [slot.entry])),
    };
  });

/**
 * Ends a tool round: applies its controls in call order, appends a `handoff`'s reset, and then ends the run at the
 * final boundary (`terminate`, asked by every call or by an `afterTools` task; with `afterRun` hooks selected the
 * run is held for its tasks instead), ends it like a failure at
 * the conversation's turn limit (`turn_limit`), ends it at a queued reset, or hands it to the next generation at
 * the `postTools` boundary, in the handoff's new context when there is one.
 * The records and slots are read before the commit: nothing else writes `core.live.tools` once every tool task
 * of the round is terminal.
 */
const endToolRound = (
  runtime: Runtime,
  assistant: GenerationCheckpointOf<"tools">["assistant"],
  tools: ReadonlyArray<TaskId<ToolTaskResult>>,
  taskTerminates: boolean,
) =>
  Effect.gen(function* () {
    const conversationId = runtime.conversationId;
    const outcomes = yield* runtime.outcomes(tools);
    const controls = new Map<TaskId, ToolControl | undefined>();
    tools.forEach((id, index) => {
      const outcome = outcomes[index];
      // The round's tools are model-issued calls, whose receipt carries the control.
      controls.set(
        id,
        outcome?.status === "completed" && outcome.result.kind === "model"
          ? outcome.result.control
          : undefined,
      );
    });
    const { slots, results } = yield* roundSlots(runtime);
    const round = decideRoundEnd(slots, controls);
    // Every call of the round must ask to terminate, unless an `afterTools` task asked for it.
    const end = { ...round, terminate: taskTerminates || round.terminate };
    // Read from the phase's agent, as every agent field; a `configure` during the round applies at its end.
    const maxTurns = (yield* runtime.agent).maxTurns;
    const afterRun = end.terminate && (yield* asksAfterRun(runtime));
    yield* runtime.commit(() =>
      Effect.gen(function* () {
        const tx = yield* Tx;
        const settings = yield* runtime.settings;
        const boundary = yield* prepareBoundary(conversationId, settings);
        if (end.added.length > 0) yield* addTools(conversationId, end.added);
        const live = yield* tx.doc(LiveDoc, conversationId);
        const now = yield* runtime.now;
        // The run's last entry so far: the newest result, or the assistant when no result has an entry.
        let last = results.reduce((newest, id) => (id > newest ? id : newest), assistant);
        if (end.handoff !== undefined) {
          // The handoff's reset precedes every queued item, so queued writes and selected steers land in its
          // context.
          const message = { role: "user", content: end.handoff, timestamp: now } as const;
          const entry = yield* tx.appendEntry(ResetEntry, conversationId, {
            head: "self",
            model: [message],
          });
          boundary.head = entry.id;
          last = entry.id;
        }
        const turns = live.run?.taskId === runtime.taskId ? live.run.turns : 0;
        const held = afterRun
          ? yield* holdRun(live, runtime, { reason: "terminate", answer: assistant, last })
          : undefined;
        if (held !== undefined) return held;
        if (end.terminate) {
          const placed = yield* applyBoundary(boundary, "final", now);
          yield* endRun(live, runtime.taskId, { status: "done", answer: assistant });
          if (placed.users.length > 0) yield* startRun(conversationId, live, placed);
        } else if (maxTurns !== undefined && turns >= maxTurns) {
          // The turn limit ends the run like a failure: `afterFailedRun` decides the queued items, and a
          // continue resumes it on the round's results. The table was read for the boundary already.
          const policy = settings.afterFailedRun;
          yield* endFailedRun(
            runtime,
            live,
            policy === "continue" ? { policy, boundary } : { policy },
            {
              status: "unanswered",
              reason: "turn_limit",
              detail: turns === 1 ? "1 turn" : `${turns} turns`,
            },
          );
        } else {
          const placed = yield* applyBoundary(boundary, "postTools", now);
          if (placed.reset) {
            // The queued reset cut the run's context before an answer.
            yield* endRun(live, runtime.taskId, { status: "unanswered", reason: "reset" });
            if (placed.users.length > 0) {
              yield* startRun(conversationId, live, placed, { keepFailure: true });
            }
          } else {
            delete live.tools;
            delete live.nestedTools;
            if (live.run?.taskId === runtime.taskId) live.run.inputs.push(...placed.users);
            handOver(live, runtime.taskId, yield* createGeneration(conversationId));
          }
        }
        return {
          status: "terminal",
          outcome: { status: "completed", result: { entryId: assistant } },
        } satisfies Next;
      }),
    );
  });

/**
 * Whether the phase's agent selects an `afterRun` hook, so a run
 * that answers waits for its tasks.
 */
const asksAfterRun = (runtime: Runtime) =>
  Effect.map(runtime.agent, (agent) =>
    agentHooks(agent, GENERATION).some((hook) => hook.handlers["afterRun"] !== undefined),
  );

/**
 * Whether an answer's `onYield` continuation is taken,
 * decided from the inbox before the final boundary places anything: only when it holds no user item, of which the
 * boundary would select one, and no reset, which would otherwise run first.
 */
const takesContinuation = (continuation: UserContent | undefined, boundary: Boundary): boolean => {
  const items = boundary.inbox.items;
  return (
    continuation !== undefined &&
    !items.some((item) => item.mode === "steer" || item.mode === "followUp") &&
    !hasQueuedReset(items)
  );
};

/**
 * Commit A of a run that answered with `afterRun` hooks selected:
 * settles the run's inputs `done` with the answer and keeps `core.live.run` for the after-run tasks, then asks the
 * hooks in `finish`. The final boundary waits for the run's end, so queued items stay queued. `undefined` when
 * this generation does not hold the run.
 */
const holdRun = (
  live: LiveDraft,
  runtime: Runtime,
  end: Omit<RunSummary, "first">,
): Effect.Effect<Next | undefined, TxWriteError, Tx> =>
  Effect.gen(function* () {
    const tx = yield* Tx;
    const run = live.run;
    if (run?.taskId !== runtime.taskId) return undefined;
    for (const id of run.inputs) {
      yield* tx.settleSubmission(id, { status: "done", answer: end.answer });
    }
    run.inputs = [];
    run.afterRun = {};
    delete live.generation;
    delete live.tools;
    delete live.nestedTools;
    // A run started before `first` was recorded has its answer as its first entry.
    const summary: RunSummary = { ...end, first: run.first ?? end.answer };
    return { status: "running", checkpoint: { phase: "finish", run: summary } } satisfies Next;
  });

/** A task a hook asked for, checked and ready to create. */
interface PreparedTask {
  readonly task: InstalledTask["task"];
  readonly input: Schema.Json;
}

/**
 * Checks the tasks one hook (`afterTools`, `onYield`, `afterRun`) asked for, off the line, and adds those that pass to `prepared`, so a broken request is skipped instead of failing the creating
 * commit: its kind must be an installed task that is not built-in, its input JSON, and its definition's `initial()`
 * must return a JSON checkpoint of one of its phases. The task is created from the installed definition, as every
 * task runs under it, with that checkpoint, so `initial()` runs once. Then dies with the rejections, which
 * `hooks.each` reports.
 */
const prepareHookTasks = (
  installed: InstalledTasks["Service"],
  requests: ReadonlyArray<HookTask>,
  prepared: Array<PreparedTask>,
) =>
  Effect.suspend(() => {
    const rejected: Array<unknown> = [];
    for (const request of requests) {
      const checked = prepareHookTask(installed, request);
      if (Result.isSuccess(checked)) prepared.push(checked.success);
      else rejected.push(checked.failure);
    }
    if (rejected.length === 0) return Effect.void;
    return Effect.die(
      rejected.length === 1
        ? rejected[0]
        : new AggregateError(rejected, `${rejected.length} hook task requests were rejected`),
    );
  });

/** One request of `prepareHookTasks`, or why it is rejected. */
const prepareHookTask = (
  installed: InstalledTasks["Service"],
  request: HookTask,
): Result.Result<PreparedTask, unknown> =>
  Result.try({
    // A hook's result is not checked by a schema: a broken request (one without a task, or whose name is not a
    // string) throws here and is rejected on its own.
    try: () => {
      const name = request.task.definition.name;
      return { name, builtIn: name.startsWith("core.") };
    },
    catch: (error) => error,
  }).pipe(
    Result.flatMap(({ name, builtIn }) => {
      if (builtIn) {
        return Result.fail(new Error(`A hook cannot start the built-in task ${name}`));
      }
      const found = installed.get(name);
      if (found === undefined) return Result.fail(new Error(`Task ${name} is not installed`));
      return Result.try({
        try: () => {
          const input = Json.copy(request.input, { omitUndefinedProperties: true });
          const definition = found.task.definition;
          const initial = Json.copy(definition.initial(input), { omitUndefinedProperties: true });
          return { input, definition, initial };
        },
        catch: (error) => error,
      }).pipe(
        Result.flatMap(({ input, definition, initial }) => {
          const phase = Schema.decodeUnknownOption(Schema.Struct({ phase: Schema.String }))(
            initial,
          );
          if (Option.isNone(phase) || !found.phases.has(phase.value.phase)) {
            const named = Option.isNone(phase) ? String(undefined) : phase.value.phase;
            return Result.fail(new Error(`Task ${name} starts at the unknown phase ${named}`));
          }
          // SAFETY: `initial` is the definition's own first checkpoint, copied as JSON, with a phase it serves.
          const checkpoint = initial as { readonly phase: string } & Schema.Json;
          const task: PreparedTask["task"] = {
            ...found.task,
            definition: { ...definition, initial: () => checkpoint },
          };
          return Result.succeed({ task, input });
        }),
      );
    }),
  );

/** Creates the tasks hooks asked for, prepared by `prepareHookTasks`, as children of the generation. */
const createHookTasks = (runtime: Runtime, prepared: ReadonlyArray<PreparedTask>) =>
  Effect.gen(function* () {
    const tx = yield* Tx;
    const tasks: Array<TaskId> = [];
    for (const each of prepared) {
      tasks.push(
        yield* tx.createTask(each.task, each.input, {
          ownership: { kind: "task", taskId: runtime.taskId },
        }),
      );
    }
    return tasks;
  });

/** Tasks a hook asked for, whose results are JSON as every task's. */
const jsonTasks = (tasks: ReadonlyArray<TaskId>) =>
  // SAFETY: a task's result is stored as JSON; its kind's schema is not this generation's to know.
  tasks as ReadonlyArray<TaskId<Schema.Json>>;

/** The continuation an `onYield` task completed with (`YieldDecision`), when it holds a valid user input. */
const yieldContinuation = (outcome: Task.TaskOutcome<Schema.Json>): UserContent | undefined => {
  if (outcome.status !== "completed") return undefined;
  // Task results are task code's: the trust boundary of the decision's shape.
  const decision = Schema.decodeUnknownOption(YieldDecisionResult)(outcome.result);
  if (Option.isNone(decision)) return undefined;
  const input = decision.value.continue;
  return Predicate.isString(input) || input.length > 0 ? input : undefined;
};

/**
 * A completed `onYield` result whose `continue` is a user input: text, or a list of text items with their text and
 * image items with their data and MIME type. Only those fields are checked, and only they are kept: other keys of
 * an item, such as a malformed `textSignature`, are dropped.
 */
const YieldDecisionResult = Schema.Struct({
  continue: Schema.Union([
    Schema.String,
    Schema.Array(
      Schema.Union([
        Schema.Struct({ type: Schema.Literal("text"), text: Schema.String }),
        Schema.Struct({
          type: Schema.Literal("image"),
          data: Schema.String,
          mimeType: Schema.String,
        }),
      ]),
    ),
  ]),
});

/** An `afterTools` task's result as an `AfterToolsDecision`: none (`null`) is one; anything else is not. */
const afterToolsDecision = (result: Schema.Json): AfterToolsDecision | undefined => {
  if (result === null) return {};
  // Another key makes it something else than a decision.
  const decision = Schema.decodeUnknownOption(AfterToolsDecisionResult)(result, {
    onExcessProperty: "error",
  });
  return Option.isSome(decision) ? decision.value : undefined;
};

/** `{}` or `{ terminate: true }`. */
const AfterToolsDecisionResult = Schema.Struct({
  terminate: Schema.optionalKey(Schema.Literal(true)),
});

/**
 * Reports an `afterTools` task that completed with no `AfterToolsDecision`, against the extension that installed
 * it, as a rule the one whose hook asked for it.
 */
const reportNoDecision = (runtime: Runtime, taskId: TaskId | undefined) =>
  Effect.gen(function* () {
    if (taskId === undefined) return;
    const kind = Option.getOrUndefined(yield* runtime.getTask(taskId))?.kind;
    const installed = yield* InstalledTasks;
    const extension = kind === undefined ? undefined : installed.get(kind)?.extension;
    const error = new Error(
      `afterTools task ${taskId} (${kind}) completed with no AfterToolsDecision`,
    );
    yield* runtime.report(Cause.die(error), { kind: "extension", extension }).pipe(Effect.ignore);
  });

/**
 * Commit B of a run that answered: its after-run tasks are
 * terminal, or there were none. Places what the final boundary selects, ends the run, and starts the next one for
 * selected user items.
 */
const endAnsweredRun = (runtime: Runtime, answer: EntryId) =>
  runtime.commit(() =>
    Effect.gen(function* () {
      const tx = yield* Tx;
      const conversationId = runtime.conversationId;
      const boundary = yield* prepareBoundary(conversationId, yield* runtime.settings);
      const live = yield* tx.doc(LiveDoc, conversationId);
      const placed = yield* applyBoundary(boundary, "final", yield* runtime.now);
      yield* endRun(live, runtime.taskId, { status: "done", answer });
      if (placed.users.length > 0) yield* startRun(conversationId, live, placed);
      return {
        status: "terminal",
        outcome: { status: "completed", result: { entryId: answer } },
      } satisfies Next;
    }),
  );

/**
 * Ends a run that answered once its `finish` or `drain`
 * phase was cut off `settings.restartLimit` times: it ends `completed`, without a failure and without the tasks it
 * would still have waited for, marked so the scheduler reports it `task_interrupted`. Queued items stay queued, as
 * after a failed run.
 */
const endAnsweredCutOff = (
  runtime: Runtime,
  task: {
    readonly restarts?: number | undefined;
    readonly state: { readonly checkpoint: GenerationCheckpoint };
  },
  answer: EntryId,
) => {
  const text = `The generation was interrupted ${cutOffCount(task)} in its ${task.state.checkpoint.phase} phase`;
  return runtime.commit(() =>
    Effect.gen(function* () {
      const tx = yield* Tx;
      const live = yield* tx.doc(LiveDoc, runtime.conversationId);
      yield* endRun(live, runtime.taskId, { status: "done", answer });
      const completed: Next = {
        status: "terminal",
        outcome: { status: "completed", result: { entryId: answer } },
      };
      return endedByCrashes(completed, text);
    }),
  );
};

/** Options of `startRun`. */
interface StartRunOptions {
  /** A run started in the commit that ended a failed one keeps its failure. */
  readonly keepFailure?: boolean | undefined;
}

/**
 * A new generation takes `core.live.run` for `inputs`, placed input submissions, whose first entry is `first`.
 * Removes `runFailure`, unless `keepFailure`.
 */
export const startRun = (
  conversationId: ConversationId,
  live: LiveDraft,
  run: { readonly users: ReadonlyArray<SubmissionId>; readonly first: EntryId | undefined },
  options?: StartRunOptions,
): Effect.Effect<void, TxWriteError, Tx> =>
  Effect.gen(function* () {
    const taskId = yield* createGeneration(conversationId, yield* runTrace(run.users));
    const started: NonNullable<LiveDraft["run"]> = { taskId, inputs: [...run.users], turns: 0 };
    if (run.first !== undefined) started.first = run.first;
    live.run = started;
    if (options?.keepFailure !== true) delete live.runFailure;
  });

/**
 * A generation owned by its conversation. Annotated, since the
 * generation's own handlers call it.
 */
const createGeneration = (
  conversationId: ConversationId,
  trace?: Pick<Task.TaskOptions, "trace" | "links">,
): Effect.Effect<TaskId<GenerationResult>, TxWriteError, Tx> =>
  Effect.gen(function* () {
    const tx = yield* Tx;
    return yield* tx.createTask(
      GenerationTask,
      {},
      { ownership: { kind: "conversation" }, conversationId, ...trace },
    );
  });

/**
 * A run continues its first input's admission trace, a root one when that input has none, and links the other
 * inputs' traces.
 */
const runTrace = (users: ReadonlyArray<SubmissionId>) =>
  Effect.gen(function* () {
    const tx = yield* Tx;
    const identity = transactionIdentity(tx);
    if (identity === undefined || users.length === 0) return {};
    let trace: Task.TaskOptions["trace"];
    const links: Array<TraceCarrier> = [];
    for (const [index, id] of users.entries()) {
      const carrier = yield* identity.submissionTrace(id);
      if (index === 0) trace = carrier ?? "root";
      else if (carrier !== undefined) links.push(carrier);
    }
    return { trace, links };
  });

/** Hands run control from `from` to `to`; the inputs move with it. */
const handOver = (live: LiveDraft, from: TaskId, to: TaskId): void => {
  if (live.run?.taskId === from) live.run.taskId = to;
};

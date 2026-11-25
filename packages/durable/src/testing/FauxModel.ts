/**
 * A scripted fake of `Models` for tests, with builders, options, a response
 * queue and simulated streaming, usage estimate and prompt cache. Tests drive
 * generation with it.
 *
 * ```ts
 * const faux = FauxModel.make();
 * faux.setResponses([fauxAssistantMessage("hi")]);
 * program.pipe(Effect.provide(faux.layer));
 * ```
 *
 * A request with the `deferred` stream option answers deferred:
 * `fetchDeferred` answers it after the `deferred.pendingFetches` first
 * fetches, and `cancelDeferred` records it.
 *
 * `FauxModel.layer` provides the same fake with its controls as the `Test`
 * service (in the spirit of `@opencode/ai`'s `TestLLM`):
 *
 * ```ts
 * const faux = yield* FauxModel.Test;
 * yield* faux.push(FauxModel.toolCall("roll_dice", { sides: 6 }), FauxModel.text("You rolled."));
 * ```
 *
 * @since 0.0.0
 */
import * as Cause from "effect/Cause";
import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Predicate from "effect/Predicate";
import * as Random from "effect/Random";
import * as Schema from "effect/Schema";
import type * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import type * as Types from "effect/Types";
import { doneAfterYield } from "../internal/signal.ts";
import { ToolCallArgumentsStream } from "../internal/streamingJson.ts";
import {
  type AssistantMessage,
  type DeferredHandle,
  emptyUsage,
  type ImageContent,
  type Message,
  type StopReason,
  type SystemMessage,
  type TextContent,
  type ThinkingContent,
  type ToolCall,
  type ToolResultMessage,
} from "../Message.ts";
import {
  type AssistantMessageEvent,
  type CostRates,
  type GenerateRequest,
  type ModelRef,
  Models,
  type ResolvedModel,
  terminalEvent,
} from "../Models.ts";

const DEFAULT_API = "faux";
const DEFAULT_PROVIDER = "faux";
const DEFAULT_MODEL_ID = "faux-1";
const DEFAULT_MIN_TOKEN_SIZE = 3;
const DEFAULT_MAX_TOKEN_SIZE = 5;

// -----------------------------------------------------------------------------
// builders
// -----------------------------------------------------------------------------

/**
 * A content block a scripted response holds.
 *
 * @category builders
 * @since 0.0.0
 */
export type FauxContentBlock = TextContent | ThinkingContent | ToolCall;

/**
 * A text block.
 *
 * @category builders
 * @since 0.0.0
 */
export const fauxText = (text: string): TextContent => ({ type: "text", text });

/**
 * A thinking block.
 *
 * @category builders
 * @since 0.0.0
 */
export const fauxThinking = (thinking: string): ThinkingContent => ({
  type: "thinking",
  thinking,
});

/**
 * Options of `fauxToolCall`.
 *
 * @category builders
 * @since 0.0.0
 */
export interface FauxToolCallOptions {
  readonly id?: string | undefined;
}

let toolCallCounter = 0;

/**
 * A tool call block. Without an `id`, it gets a fresh one.
 *
 * @category builders
 * @since 0.0.0
 */
export const fauxToolCall = (
  name: string,
  args: ToolCall["arguments"],
  options: FauxToolCallOptions = {},
): ToolCall => ({
  type: "toolCall",
  // A synchronous builder has no `Clock` or `Random`, so the id is a module counter: unique,
  // and deterministic across runs.
  id: options.id ?? `tool:${++toolCallCounter}`,
  name,
  arguments: args,
});

/**
 * Options of `fauxAssistantMessage`.
 *
 * @category builders
 * @since 0.0.0
 */
export interface FauxAssistantMessageOptions {
  readonly stopReason?: StopReason | undefined;
  /** The handle of a message whose `stopReason` is `deferred`. */
  readonly deferred?: DeferredHandle | undefined;
  readonly errorMessage?: string | undefined;
  readonly responseId?: string | undefined;
  readonly timestamp?: number | undefined;
}

/**
 * A scripted assistant message. A string becomes one text block; the stop
 * reason defaults to `stop`. The stream rewrites `api`, `provider`, `model`
 * and `usage`.
 *
 * **Gotchas**
 *
 * - Without `timestamp`, the message gets `0`, and the stream stamps it with
 *   the `Clock` time of the request.
 *
 * @category builders
 * @since 0.0.0
 */
export const fauxAssistantMessage = (
  content: string | FauxContentBlock | ReadonlyArray<FauxContentBlock>,
  options: FauxAssistantMessageOptions = {},
): AssistantMessage => {
  const message: Types.Mutable<AssistantMessage> = {
    role: "assistant",
    content: normalizeContent(content),
    api: DEFAULT_API,
    provider: DEFAULT_PROVIDER,
    model: DEFAULT_MODEL_ID,
    usage: emptyUsage(),
    stopReason: options.stopReason ?? "stop",
    // A synchronous builder has no `Clock`, so `0` marks it unset and the stream stamps it
    // when the request starts.
    timestamp: options.timestamp ?? UNSET_TIMESTAMP,
  };
  if (options.deferred !== undefined) message.deferred = options.deferred;
  if (options.errorMessage !== undefined) message.errorMessage = options.errorMessage;
  if (options.responseId !== undefined) message.responseId = options.responseId;
  return message;
};

const UNSET_TIMESTAMP = 0;

const normalizeContent = (
  content: string | FauxContentBlock | ReadonlyArray<FauxContentBlock>,
): ReadonlyArray<FauxContentBlock> => {
  if (Predicate.isString(content)) return [fauxText(content)];
  return isBlockArray(content) ? content : [content];
};

const isBlockArray = (
  content: FauxContentBlock | ReadonlyArray<FauxContentBlock>,
): content is ReadonlyArray<FauxContentBlock> => Array.isArray(content);

// -----------------------------------------------------------------------------
// handle
// -----------------------------------------------------------------------------

/**
 * A model the fake serves. Absent fields default to a 128000 context window,
 * 16384 max tokens, no reasoning, text and image input, zero cost.
 *
 * @category models
 * @since 0.0.0
 */
export interface FauxModelDefinition {
  readonly id: string;
  readonly reasoning?: boolean | undefined;
  readonly input?: ReadonlyArray<"text" | "image"> | undefined;
  readonly cost?: CostRates | undefined;
  readonly contextWindow?: number | undefined;
  readonly maxTokens?: number | undefined;
}

/**
 * Token sizes of streamed chunks, drawn at random between `min` and `max`
 * (default 3 to 5).
 *
 * @category models
 * @since 0.0.0
 */
export interface FauxTokenSize {
  readonly min?: number | undefined;
  readonly max?: number | undefined;
}

/**
 * How the fake answers a request with the `deferred` stream option.
 *
 * @category models
 * @since 0.0.0
 */
export interface FauxDeferredOptions {
  /** Fetches that return the handle again before the scripted response is ready. Default `0`. */
  readonly pendingFetches?: number | undefined;
  /** The handle's `pollAfterMs`; absent, the generation's default (5000 ms). */
  readonly pollAfterMs?: number | undefined;
}

/**
 * Options of `make`.
 *
 * @category models
 * @since 0.0.0
 */
export interface FauxModelOptions {
  /** Written to every message's `api`. Default `faux`. */
  readonly api?: string | undefined;
  /** Default `faux`. */
  readonly provider?: string | undefined;
  /** Default one model, `faux-1`. */
  readonly models?: ReadonlyArray<FauxModelDefinition> | undefined;
  /** Paces deltas with `Effect.sleep`; absent, each delta only yields. */
  readonly tokensPerSecond?: number | undefined;
  readonly tokenSize?: FauxTokenSize | undefined;
  readonly deferred?: FauxDeferredOptions | undefined;
}

/**
 * Counters of the fake, read by tests.
 *
 * @category models
 * @since 0.0.0
 */
export interface FauxModelState {
  /** Requests streamed so far, answered or not. */
  readonly callCount: number;
  /** Fetches of deferred responses so far, ready or not. */
  readonly deferredFetchCount: number;
  /** The handles `cancelDeferred` was called with, in order. */
  readonly cancelledDeferred: ReadonlyArray<DeferredHandle>;
}

/**
 * A scripted response computed at request time. It
 * runs in the stream's fiber: `Effect.never` is a request that never answers
 * until the stream is interrupted, and a defect ends the request with
 * `stopReason: "error"` and the defect's message.
 *
 * @category models
 * @since 0.0.0
 */
export type FauxResponseFactory = (
  request: GenerateRequest,
  state: FauxModelState,
  model: ResolvedModel,
) => Effect.Effect<AssistantMessage>;

/**
 * One queued response: a message, or a factory of one.
 *
 * @category models
 * @since 0.0.0
 */
export type FauxResponseStep = AssistantMessage | FauxResponseFactory;

/**
 * The test handle of a fake. Its methods are synchronous, so tests script
 * responses between steps.
 *
 * @category models
 * @since 0.0.0
 */
export interface FauxModel {
  /** The `Models` service over this handle's queue. */
  readonly layer: Layer.Layer<Models>;
  readonly api: string;
  readonly provider: string;
  readonly models: readonly [ResolvedModel, ...Array<ResolvedModel>];
  /** The first model, or the one with this id. */
  readonly getModel: {
    (): ResolvedModel;
    (modelId: string): ResolvedModel | undefined;
  };
  readonly state: FauxModelState;
  /** Replaces the queue. */
  readonly setResponses: (responses: ReadonlyArray<FauxResponseStep>) => void;
  readonly appendResponses: (responses: ReadonlyArray<FauxResponseStep>) => void;
  /** Responses still queued. */
  readonly pendingResponseCount: () => number;
  /** The controls of this handle as an Effect service. */
  readonly test: Test["Service"];
}

// -----------------------------------------------------------------------------
// test service
// -----------------------------------------------------------------------------

/**
 * A request held by `Test.gate`.
 *
 * @category test
 * @since 0.0.0
 */
export interface FauxGate {
  /** Completes once the held request reached the fake: the generation is streaming. */
  readonly started: Effect.Effect<void>;
  /** Lets the held request answer. */
  readonly release: Effect.Effect<void>;
}

/**
 * The fake's controls, as a service a test reaches with `yield* FauxModel.Test`.
 *
 * @category test
 * @since 0.0.0
 */
export class Test extends Context.Service<
  Test,
  {
    /** Queues responses after those already queued. */
    readonly push: (...responses: ReadonlyArray<FauxResponseStep>) => Effect.Effect<void>;
    /** Every request made so far, in order, answered or not. */
    readonly requests: Effect.Effect<ReadonlyArray<GenerateRequest>>;
    /** Dies when a queued response was never used. */
    readonly assertExhausted: Effect.Effect<void>;
    /**
     * Holds the next request once it reached the fake, before its first event, until `release` or the
     * scope's end. The request takes its queued response when it arrives, so push the response
     * before the request is made. One gate is armed at a time; arming a second dies.
     */
    readonly gate: () => Effect.Effect<FauxGate, never, Scope.Scope>;
  }
>()("@effect-durable-harness/core/testing/FauxModel/Test") {}

/**
 * A fresh fake with an empty queue, as `Models` and as its `Test` controls.
 *
 * @category test
 * @since 0.0.0
 */
export const layer: Layer.Layer<Models | Test> = Layer.unwrap(
  Effect.sync(() => {
    const faux = make();
    return Layer.merge(faux.layer, Layer.succeed(Test, faux.test));
  }),
);

/**
 * The reference of the default model, for an agent's `model`.
 *
 * @category test
 * @since 0.0.0
 */
export const ref: ModelRef = { provider: DEFAULT_PROVIDER, modelId: DEFAULT_MODEL_ID };

/**
 * A response that answers with `content`.
 *
 * @category builders
 * @since 0.0.0
 */
export const text = (content: string): AssistantMessage => fauxAssistantMessage(content);

/**
 * A response that calls the tool `name` with `args`.
 *
 * @category builders
 * @since 0.0.0
 */
export const toolCall = (name: string, args: ToolCall["arguments"]): AssistantMessage =>
  fauxAssistantMessage([fauxToolCall(name, args)], { stopReason: "toolUse" });

/**
 * Makes a fake model with an empty response queue. Each request takes the
 * next response; with none queued, it ends with `stopReason: "error"` and
 * `No more faux responses queued`.
 *
 * @category constructors
 * @since 0.0.0
 */
export const make = (options: FauxModelOptions = {}): FauxModel => {
  const api = options.api ?? DEFAULT_API;
  const provider = options.provider ?? DEFAULT_PROVIDER;
  const minTokenSize = Math.max(
    1,
    Math.min(
      options.tokenSize?.min ?? DEFAULT_MIN_TOKEN_SIZE,
      options.tokenSize?.max ?? DEFAULT_MAX_TOKEN_SIZE,
    ),
  );
  const maxTokenSize = Math.max(minTokenSize, options.tokenSize?.max ?? DEFAULT_MAX_TOKEN_SIZE);
  const tokensPerSecond = options.tokensPerSecond;
  const state: Types.Mutable<FauxModelState> & { cancelledDeferred: Array<DeferredHandle> } = {
    callCount: 0,
    deferredFetchCount: 0,
    cancelledDeferred: [],
  };
  // The deferred responses by handle id, with what resolving their step needs.
  const deferredResponses = new Map<
    string,
    {
      readonly handle: DeferredHandle;
      readonly step: FauxResponseStep;
      readonly request: GenerateRequest;
      readonly model: ResolvedModel;
      pendingFetches: number;
      cancelled: boolean;
      final?: AssistantMessage;
    }
  >();
  let deferredCounter = 0;
  const promptCache = new Map<string, ReadonlyArray<string>>();
  let pending: Array<FauxResponseStep> = [];
  const requests: Array<GenerateRequest> = [];
  /** The gate the next request is held by, taken in the step the request arrives. */
  let gated:
    | { readonly started: Deferred.Deferred<void>; readonly release: Deferred.Deferred<void> }
    | undefined;

  const definitions: ReadonlyArray<FauxModelDefinition> =
    options.models !== undefined && options.models.length > 0
      ? options.models
      : [{ id: DEFAULT_MODEL_ID }];
  const resolved = definitions.map(
    (definition): ResolvedModel => ({
      provider,
      id: definition.id,
      api,
      contextWindow: definition.contextWindow ?? 128000,
      maxTokens: definition.maxTokens ?? 16384,
      reasoning: definition.reasoning ?? false,
      input: definition.input ?? ["text", "image"],
      cost: definition.cost ?? { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
    }),
  );
  const first = resolved[0];
  // SAFETY: `definitions` falls back to one default model, so `resolved` is never empty.
  const models = [first, ...resolved.slice(1)] as readonly [ResolvedModel, ...Array<ResolvedModel>];

  function getModel(): ResolvedModel;
  function getModel(modelId: string): ResolvedModel | undefined;
  function getModel(modelId?: string): ResolvedModel | undefined {
    return modelId === undefined ? models[0] : models.find((model) => model.id === modelId);
  }

  const errorMessage = (message: string, modelId: string, timestamp: number): AssistantMessage => ({
    role: "assistant",
    content: [],
    api,
    provider,
    model: modelId,
    usage: emptyUsage(),
    stopReason: "error",
    errorMessage: message,
    timestamp,
  });

  // The step's message, rewritten as the fake's, with its usage estimated. A factory that dies ends the
  // request with its message; an interruption interrupts.
  const resolveResponse = (
    step: FauxResponseStep,
    request: GenerateRequest,
    model: ResolvedModel,
    startedAt: number,
  ) =>
    Effect.gen(function* () {
      const exit = yield* Effect.exit(
        Predicate.isFunction(step) ? step(request, state, model) : Effect.succeed(step),
      );
      if (Exit.isFailure(exit)) {
        if (Cause.hasInterruptsOnly(exit.cause)) return yield* Effect.failCause(exit.cause);
        const now = yield* Clock.currentTimeMillis;
        return {
          _tag: "Failed",
          message: errorMessage(causeMessage(exit.cause), model.id, now),
        } as const;
      }
      const message = withUsageEstimate(
        {
          ...exit.value,
          api,
          provider,
          model: model.id,
          timestamp: exit.value.timestamp === UNSET_TIMESTAMP ? startedAt : exit.value.timestamp,
        },
        request,
        promptCache,
      );
      return { _tag: "Scripted", message } as const;
    });

  // The handle again while fetches are pending, then the step's message, resolved once with the submission's
  // request without its `deferred` option. Only the final message, as `Models.fetchDeferred` returns it, not
  // its events.
  const fetchDeferred = (model: ResolvedModel, handle: DeferredHandle) =>
    Effect.gen(function* () {
      state.deferredFetchCount++;
      const now = yield* Clock.currentTimeMillis;
      const entry = deferredResponses.get(handle.id);
      if (
        entry === undefined ||
        entry.handle.provider !== handle.provider ||
        entry.handle.modelId !== handle.modelId ||
        entry.handle.api !== handle.api
      ) {
        return errorMessage(`Unknown faux deferred response: ${handle.id}`, model.id, now);
      }
      if (entry.cancelled) {
        return errorMessage(`Faux deferred response was cancelled: ${handle.id}`, model.id, now);
      }
      if (entry.pendingFetches > 0) {
        entry.pendingFetches--;
        return deferredMessage(model, entry.handle, now);
      }
      if (entry.final === undefined) {
        const { deferred: _deferred, ...submitted } = entry.request.options;
        const resolved = yield* resolveResponse(
          entry.step,
          { ...entry.request, options: submitted },
          entry.model,
          now,
        );
        // A script without a stop reason ends in an error.
        entry.final =
          resolved.message.stopReason === "pending"
            ? errorMessage("Faux response ended without a stop reason", model.id, now)
            : resolved.message;
      }
      return entry.final;
    });

  // Records the handle and marks its response cancelled.
  const cancelDeferred = (_model: ResolvedModel, handle: DeferredHandle) =>
    Effect.sync(() => {
      state.cancelledDeferred.push({ ...handle });
      const entry = deferredResponses.get(handle.id);
      if (entry !== undefined) entry.cancelled = true;
    });

  // Each event is one element, with a fresh snapshot as its partial; the `done` or `error` event is the last.
  const stream = (
    model: ResolvedModel,
    request: GenerateRequest,
  ): Stream.Stream<AssistantMessageEvent> =>
    Stream.unwrap(
      Effect.gen(function* () {
        const startedAt = yield* Clock.currentTimeMillis;
        // The step is taken and the call counted when the request is made.
        const step = pending.shift();
        state.callCount++;
        requests.push(request);
        const gate = gated;
        gated = undefined;
        if (gate !== undefined) {
          doneAfterYield(gate.started, Effect.void);
          yield* Deferred.await(gate.release);
        }
        // The terminal message is timed from the stream's start,
        // unless it has a duration already or predates the stream.
        const timed = (message: AssistantMessage) =>
          Effect.map(Clock.currentTimeMillis, (now) =>
            terminalEvent(
              message.durationMs !== undefined || message.timestamp < startedAt
                ? message
                : { ...message, durationMs: Math.max(0, Math.round(now - startedAt)) },
            ),
          );

        if (step === undefined) {
          const exhausted = withUsageEstimate(
            errorMessage("No more faux responses queued", model.id, startedAt),
            request,
            promptCache,
          );
          return Stream.fromEffect(timed(exhausted));
        }

        // A request with the `deferred` option answers with a handle, and its step is resolved when a fetch
        // finds it ready.
        if (request.options.deferred !== undefined && request.options.deferred !== false) {
          const handle: Types.Mutable<DeferredHandle> = {
            provider: model.provider,
            modelId: model.id,
            api: model.api,
            // A counter is unique and deterministic.
            id: `deferred:${++deferredCounter}`,
          };
          const pollAfterMs = options.deferred?.pollAfterMs;
          if (pollAfterMs !== undefined) handle.pollAfterMs = pollAfterMs;
          deferredResponses.set(handle.id, {
            handle,
            step,
            request,
            model,
            pendingFetches: Math.max(0, Math.floor(options.deferred?.pendingFetches ?? 0)),
            cancelled: false,
          });
          const message = deferredMessage(model, handle, startedAt);
          return Stream.fromIterable(planEvents(message, [])).pipe(
            Stream.map((planned) => planned.event),
            Stream.concat(Stream.fromEffect(timed(message))),
          );
        }

        const resolved = yield* resolveResponse(step, request, model, startedAt);
        if (resolved._tag === "Failed") return Stream.fromEffect(timed(resolved.message));
        const message = resolved.message;
        const chunks = yield* Effect.forEach(message.content, (block) =>
          splitByTokenSize(blockText(block), minTokenSize, maxTokenSize),
        );
        const terminal =
          message.stopReason === "pending"
            ? // A script without a stop reason ends in an error.
              Effect.map(Clock.currentTimeMillis, (now) =>
                errorMessage("Faux response ended without a stop reason", model.id, now),
              )
            : Effect.succeed(message);
        return Stream.fromIterable(planEvents(message, chunks)).pipe(
          Stream.mapEffect((planned) =>
            Effect.as(pace(planned.chunk, tokensPerSecond), planned.event),
          ),
          Stream.concat(Stream.fromEffect(Effect.flatMap(terminal, timed))),
        );
      }),
    );

  return {
    layer: Layer.succeed(
      Models,
      Models.of({
        resolve: Effect.fn("Models.resolve")(function* (ref: ModelRef) {
          return ref.provider === provider
            ? Option.fromUndefinedOr(getModel(ref.modelId))
            : Option.none();
        }),
        stream,
        fetchDeferred,
        cancelDeferred,
      }),
    ),
    api,
    provider,
    models,
    getModel,
    state,
    setResponses: (responses) => {
      pending = [...responses];
    },
    appendResponses: (responses) => {
      pending.push(...responses);
    },
    pendingResponseCount: () => pending.length,
    test: Test.of({
      push: (...responses) =>
        Effect.sync(() => {
          pending.push(...responses);
        }),
      requests: Effect.sync(() => [...requests]),
      assertExhausted: Effect.suspend(() =>
        pending.length === 0
          ? Effect.void
          : Effect.die(new Error(`${pending.length} faux responses were never used`)),
      ),
      gate: () =>
        Effect.gen(function* () {
          // One gate at a time: a second would leave the first's `started` waiting forever.
          if (gated !== undefined) {
            return yield* Effect.die(new Error("A faux gate is already armed"));
          }
          const gate = {
            started: Deferred.makeUnsafe<void>(),
            release: Deferred.makeUnsafe<void>(),
          };
          gated = gate;
          const release = Effect.sync(() => {
            doneAfterYield(gate.release, Effect.void);
          });
          // A gate left held would hang the request: the scope's end lets it answer, and disarms a gate no
          // request reached, so a later request is not held by it.
          yield* Effect.addFinalizer(() =>
            Effect.andThen(
              Effect.sync(() => {
                if (gated === gate) gated = undefined;
              }),
              release,
            ),
          );
          return { started: Deferred.await(gate.started), release };
        }),
    }),
  };
};

// -----------------------------------------------------------------------------
// streaming
// -----------------------------------------------------------------------------

const deferredMessage = (
  model: ResolvedModel,
  handle: DeferredHandle,
  timestamp: number,
): AssistantMessage => ({
  role: "assistant",
  content: [],
  api: model.api,
  provider: model.provider,
  model: model.id,
  usage: emptyUsage(),
  stopReason: "deferred",
  deferred: handle,
  timestamp,
});

interface PlannedEvent {
  /** The delta's text, paced before the event is emitted; absent for start and end events. */
  readonly chunk: string | undefined;
  readonly event: AssistantMessageEvent;
}

/**
 * The events streamed before the terminal one, in order: `start`, then per block its start, one
 * delta per chunk and its end. A tool call's arguments stream as JSON text, and its partial
 * arguments are what that text parses to so far, until its end assigns the scripted ones.
 */
const planEvents = (
  message: AssistantMessage,
  chunks: ReadonlyArray<ReadonlyArray<string>>,
): ReadonlyArray<PlannedEvent> => {
  const plan: Array<PlannedEvent> = [];
  const content: Array<FauxContentBlock> = [];
  // Each snapshot owns its blocks, so an element never changes after it is emitted.
  const partial = (): AssistantMessage => ({
    ...message,
    content: [...content],
    stopReason: "pending",
  });
  plan.push({ chunk: undefined, event: { type: "start", partial: partial() } });
  message.content.forEach((block, contentIndex) => {
    const blockChunks = chunks[contentIndex] ?? [];
    if (block.type === "thinking") {
      content.push({ type: "thinking", thinking: "" });
      plan.push({
        chunk: undefined,
        event: { type: "thinking_start", contentIndex, partial: partial() },
      });
      for (const chunk of blockChunks) {
        content[contentIndex] = {
          type: "thinking",
          thinking: thinkingOf(content[contentIndex]) + chunk,
        };
        plan.push({
          chunk,
          event: { type: "thinking_delta", contentIndex, delta: chunk, partial: partial() },
        });
      }
      plan.push({
        chunk: undefined,
        event: {
          type: "thinking_end",
          contentIndex,
          content: block.thinking,
          partial: partial(),
        },
      });
      return;
    }
    if (block.type === "text") {
      content.push({ type: "text", text: "" });
      plan.push({
        chunk: undefined,
        event: { type: "text_start", contentIndex, partial: partial() },
      });
      for (const chunk of blockChunks) {
        content[contentIndex] = { type: "text", text: textOf(content[contentIndex]) + chunk };
        plan.push({
          chunk,
          event: { type: "text_delta", contentIndex, delta: chunk, partial: partial() },
        });
      }
      plan.push({
        chunk: undefined,
        event: { type: "text_end", contentIndex, content: block.text, partial: partial() },
      });
      return;
    }
    content.push({ type: "toolCall", id: block.id, name: block.name, arguments: {} });
    plan.push({
      chunk: undefined,
      event: { type: "toolcall_start", contentIndex, partial: partial() },
    });
    // Partial arguments come from the same parser real providers use, so
    // consumers see them grow.
    const args = new ToolCallArgumentsStream();
    for (const chunk of blockChunks) {
      content[contentIndex] = {
        type: "toolCall",
        id: block.id,
        name: block.name,
        arguments: args.append(chunk),
      };
      plan.push({
        chunk,
        event: { type: "toolcall_delta", contentIndex, delta: chunk, partial: partial() },
      });
    }
    content[contentIndex] = block;
    plan.push({
      chunk: undefined,
      event: { type: "toolcall_end", contentIndex, toolCall: block, partial: partial() },
    });
  });
  return plan;
};

const thinkingOf = (block: FauxContentBlock | undefined): string =>
  block?.type === "thinking" ? block.thinking : "";

const textOf = (block: FauxContentBlock | undefined): string =>
  block?.type === "text" ? block.text : "";

const blockText = (block: FauxContentBlock): string => {
  if (block.type === "thinking") return block.thinking;
  if (block.type === "text") return block.text;
  return encodeJson(block.arguments);
};

const encodeJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));

// Splits text into chunks of random token sizes, drawn with the `Random` service.
const splitByTokenSize = Effect.fnUntraced(function* (
  text: string,
  minTokenSize: number,
  maxTokenSize: number,
) {
  const chunks: Array<string> = [];
  let index = 0;
  while (index < text.length) {
    const tokenSize = yield* Random.nextIntBetween(minTokenSize, maxTokenSize);
    const charSize = Math.max(1, tokenSize * 4);
    chunks.push(text.slice(index, index + charSize));
    index += charSize;
  }
  return chunks.length > 0 ? chunks : [""];
});

// A delta waits for its tokens at `tokensPerSecond`, or yields once when no pace is set. `Effect.sleep` follows
// the `TestClock` in tests.
const pace = (
  chunk: string | undefined,
  tokensPerSecond: number | undefined,
): Effect.Effect<void> => {
  if (chunk === undefined) return Effect.void;
  if (tokensPerSecond === undefined || tokensPerSecond <= 0) return Effect.yieldNow;
  return Effect.sleep((estimateTokens(chunk) / tokensPerSecond) * 1000);
};

const causeMessage = (cause: Cause.Cause<never>): string => {
  const error = Cause.squash(cause);
  return error instanceof Error ? error.message : String(error);
};

// -----------------------------------------------------------------------------
// usage estimate
// -----------------------------------------------------------------------------

const estimateTokens = (text: string): number => Math.ceil(text.length / 4);

/**
 * Input and output tokens are characters / 4. With a `sessionId` and
 * caching on, the common prefix with the session's previous prompt counts as cache reads and the
 * rest as cache writes. Costs are zero.
 */
const withUsageEstimate = (
  message: AssistantMessage,
  request: GenerateRequest,
  promptCache: Map<string, ReadonlyArray<string>>,
): AssistantMessage => {
  // One text per message; the whole prompt joins them with blank lines.
  const prompt = request.messages.map((entry) => `${entry.role}:${messageToText(entry)}`);
  const promptLength = joinedLength(prompt, prompt.length);
  const promptTokens = Math.ceil(promptLength / 4);
  const output = estimateTokens(assistantContentToText(message.content));
  const sessionId = request.sessionId;
  const cached =
    sessionId !== undefined && sessionId.length > 0 && request.options.cacheRetention !== "none"
      ? simulateCache(promptCache, sessionId, prompt, promptLength, promptTokens)
      : { input: promptTokens, cacheRead: 0, cacheWrite: 0 };
  return {
    ...message,
    usage: {
      input: cached.input,
      output,
      cacheRead: cached.cacheRead,
      cacheWrite: cached.cacheWrite,
      totalTokens: cached.input + output + cached.cacheRead + cached.cacheWrite,
      cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
    },
  };
};

const simulateCache = (
  promptCache: Map<string, ReadonlyArray<string>>,
  sessionId: string,
  prompt: ReadonlyArray<string>,
  promptLength: number,
  promptTokens: number,
) => {
  const previous = promptCache.get(sessionId);
  promptCache.set(sessionId, prompt);
  if (previous === undefined)
    return { input: promptTokens, cacheRead: 0, cacheWrite: promptTokens };
  const cachedChars = commonPromptPrefixLength(previous, prompt);
  const cacheRead = Math.ceil(cachedChars / 4);
  return {
    input: Math.max(0, promptTokens - cacheRead),
    cacheRead,
    cacheWrite: Math.ceil((promptLength - cachedChars) / 4),
  };
};

/** Length of the prompt text that joins the first `count` messages with blank lines. */
const joinedLength = (messages: ReadonlyArray<string>, count: number): number => {
  let length = count > 0 ? (count - 1) * 2 : 0;
  for (let index = 0; index < count; index++) length += messages[index]?.length ?? 0;
  return length;
};

/**
 * Length of the common prefix of the two joined prompts. Equal messages are compared whole;
 * characters are compared only from the first message that differs.
 */
const commonPromptPrefixLength = (
  previous: ReadonlyArray<string>,
  current: ReadonlyArray<string>,
): number => {
  let index = 0;
  while (index < previous.length && index < current.length && previous[index] === current[index]) {
    index++;
  }
  const rest = (messages: ReadonlyArray<string>) =>
    index === messages.length ? "" : (index > 0 ? "\n\n" : "") + messages.slice(index).join("\n\n");
  return joinedLength(previous, index) + commonPrefixLength(rest(previous), rest(current));
};

const commonPrefixLength = (a: string, b: string): number => {
  const length = Math.min(a.length, b.length);
  let index = 0;
  while (index < length && a[index] === b[index]) index++;
  return index;
};

const contentToText = (content: string | ReadonlyArray<TextContent | ImageContent>): string => {
  if (Predicate.isString(content)) return content;
  return content
    .map((block) =>
      block.type === "text" ? block.text : `[image:${block.mimeType}:${block.data.length}]`,
    )
    .join("\n");
};

const assistantContentToText = (content: ReadonlyArray<FauxContentBlock>): string =>
  content
    .map((block) => {
      if (block.type === "text") return block.text;
      if (block.type === "thinking") return block.thinking;
      return `${block.name}:${encodeJson(block.arguments)}`;
    })
    .join("\n");

const toolResultToText = (message: ToolResultMessage): string =>
  [message.toolName, ...message.content.map((block) => contentToText([block]))].join("\n");

// The content, then its sections.
const systemMessageText = (message: SystemMessage): string => {
  const parts = [
    Predicate.isString(message.content)
      ? message.content
      : message.content.map((block) => block.text).join("\n"),
  ];
  for (const text of Object.values(message.sections ?? {})) {
    if (text !== null) parts.push(text);
  }
  return parts.filter((part) => part.length > 0).join("\n\n");
};

const messageToText = (message: Message): string => {
  if (message.role === "system") {
    return [
      systemMessageText(message),
      ...(message.toolsRemoved?.map((tool) => `tool-:${encodeJson(tool)}`) ?? []),
      ...(message.toolsAdded?.map((tool) => `tool+:${encodeJson(tool)}`) ?? []),
    ]
      .filter((part) => part.length > 0)
      .join("\n");
  }
  if (message.role === "user") return contentToText(message.content);
  if (message.role === "assistant") return assistantContentToText(message.content);
  return toolResultToText(message);
};

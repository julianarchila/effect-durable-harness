/**
 * The kernel's `Models` service on `@opencode/ai`. This module is the adapter between the kernel and `@opencode/ai`:
 * the kernel's messages to an opencode request, opencode events back to the kernel's events.
 *
 * @since 0.0.0
 */
import type { LanguageModel, LLMClientService } from "@opencode/ai/effect";
import { ai, LLMClient } from "@opencode/ai/effect";
import { RequestExecutor } from "@opencode/ai/route";
import * as Clock from "effect/Clock";
import * as Effect from "effect/Effect";
import type * as HttpClient from "effect/http/HttpClient";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import type { AssistantMessage } from "@effect-durable-harness/core/Message";
import {
  type AssistantMessageEvent,
  type GenerateRequest,
  type ModelRef,
  Models,
  type ResolvedModel,
  terminalEvent,
} from "@effect-durable-harness/core/Models";
import * as Assembler from "./internal/assembler.ts";
import { payloadMiddleware } from "./internal/payload.ts";
import { providerMetadataKey, toRequest } from "./internal/request.ts";

/**
 * A model the layer serves: the metadata the kernel reads (`ResolvedModel`) and the `@opencode/ai` model that makes
 * the requests, such as `Anthropic.configure({ apiKey }).model("claude-haiku-5-5")`. The metadata is the consumer's:
 * opencode does not supply prices, windows or thinking levels.
 *
 * **Gotchas**
 *
 * - No deferred responses: `@opencode/ai` has none for language models, so a request's `deferred` option is ignored
 *   and the layer offers no `fetchDeferred`.
 * - Provider-specific work is written for two opencode protocols: Anthropic Messages (thinking and effort,
 *   `metadata.user_id`, thinking signatures) and Gemini, for the Gemini API and Vertex Gemini (thinking options, thought
 *   signatures). A model on another protocol gets the messages, tools, cache policy, headers and timeout, but no
 *   reasoning options, and its provider replay data is not kept.
 *
 * @category models
 * @since 0.0.0
 */
export interface OpencodeModel extends ResolvedModel {
  /** The `@opencode/ai` model that serves the requests. */
  readonly model: LanguageModel;
  /**
   * Keep later system messages in place, where opencode sends them as
   * native system messages or wraps them as user text, depending on the model. Default `false`: they are collapsed
   * into the leading prompt.
   */
  readonly supportsMidConvoSystemMessages?: boolean | undefined;
}

/**
 * Builds `Models` over an opencode client already in context. Use it with `TestLLM.testLayer()` from
 * `@opencode/ai/testing`, or with a client the consumer configures.
 *
 * @category constructors
 * @since 0.0.0
 */
export const make = Effect.fnUntraced(function* (models: ReadonlyArray<OpencodeModel>) {
  const client = yield* LLMClient.Service;
  const find = (provider: string, id: string) =>
    models.find((model) => model.provider === provider && model.id === id);

  const resolve = Effect.fn("OpencodeModels.resolve")(function* (ref: ModelRef) {
    const model: ResolvedModel | undefined = find(ref.provider, ref.modelId);
    return Option.fromNullishOr(model);
  });

  const stream = (
    resolved: ResolvedModel,
    request: GenerateRequest,
  ): Stream.Stream<AssistantMessageEvent> => {
    const model = find(resolved.provider, resolved.id);
    // The kernel streams only what `resolve` returned: a model outside this layer is broken wiring.
    if (model === undefined)
      return Stream.die(
        `OpencodeModels: no model ${resolved.provider}/${resolved.id} in this layer`,
      );
    return Stream.unwrap(
      Effect.gen(function* () {
        const startedAt = yield* Clock.currentTimeMillis;
        const assembler = Assembler.make({
          model,
          protocol: model.model.route.protocol,
          metadataKey: providerMetadataKey(model),
          timestamp: startedAt,
        });
        // `onPayload`: the generation's `beforeProviderRequest` hooks replace the body opencode built.
        const options =
          request.onPayload === undefined
            ? undefined
            : { http: payloadMiddleware(request.onPayload) };
        return ai.stream(toRequest(model, request), options).pipe(
          Stream.provideService(LLMClient.Service, client),
          // The stream contract: a failure ends the stream with a terminal message.
          Stream.catch((error) => Stream.succeed(assembler.failure(error))),
          Stream.mapAccum(() => assembler.start, assembler.step, { onHalt: assembler.finish }),
          // The terminal message carries the request's duration.
          Stream.mapEffect((event) =>
            event.type === "done" || event.type === "error"
              ? Effect.map(Clock.currentTimeMillis, (now) =>
                  terminalEvent(
                    withDuration(
                      event.type === "done" ? event.message : event.error,
                      now - startedAt,
                    ),
                  ),
                )
              : Effect.succeed(event),
          ),
        );
      }),
    ).pipe(
      Stream.withSpan("OpencodeModels.stream", {
        attributes: { "durable.model.provider": model.provider, "durable.model.id": model.id },
      }),
    );
  };

  // Deferred responses: `@opencode/ai` 2.2.5 has none for language models, so
  // this layer declares no `fetchDeferred` or `cancelDeferred`, and a request's `deferred` option is not sent: it
  // streams its answer at once, as a provider without deferred responses does. opencode's  `Generation`;
  // (submit, status, result, cancel, resumable by token) serves only its media routes: images, video, speech,
  // transcription.
  return Models.of({ resolve, stream });
});

const withDuration = (message: AssistantMessage, durationMs: number): AssistantMessage =>
  Object.assign({}, message, { durationMs: Math.max(0, durationMs) });

/**
 * `Models` over an opencode client already in context (`make`).
 *
 * @category layers
 * @since 0.0.0
 */
export const layerFromClient = (
  models: ReadonlyArray<OpencodeModel>,
): Layer.Layer<Models, never, LLMClientService> => Layer.effect(Models, make(models));

/**
 * `Models` for the given models, with opencode's client and request executor built in. The consumer provides the
 * `HttpClient`, such as `FetchHttpClient.layer`. `resolve` matches a `ModelRef` by provider and model id; `stream`
 * never fails, and interrupting it aborts the request.
 *
 * @category layers
 * @since 0.0.0
 */
export const layer = (
  models: ReadonlyArray<OpencodeModel>,
): Layer.Layer<Models, never, HttpClient.HttpClient> =>
  layerFromClient(models).pipe(
    Layer.provide(LLMClient.layer),
    Layer.provide(RequestExecutor.layer),
  );

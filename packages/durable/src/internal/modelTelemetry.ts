// What one model request records on its `durable.generation.request` span: Effect AI's conventions, the
// OpenTelemetry GenAI attributes its providers write (`effect/ai/Telemetry`, as `AnthropicLanguageModel`'s
// `annotateRequest` and `annotateStreamResponse` do), never the messages. It follows the Effect ecosystem's
// attributes, so GenAI dashboards and OTLP backends read them as they read Effect AI's.
import * as AiTelemetry from "effect/ai/Telemetry";
import * as Data from "effect/Data";
import type * as Tracer from "effect/Tracer";
import type { AssistantMessage } from "../Message.ts";
import type { ResolvedModel } from "../Models.ts";
import * as telemetry from "./telemetry.ts";

/**
 * A request that ended with stop reason `error`, as the span's failure (the message capped at 500 characters).
 * Internal: caught as the span ends, so it reaches the tracer as the span's exit and no caller.
 */
export class ModelRequestFailed extends Data.TaggedError("ModelRequestFailed")<{
  readonly message: string;
  readonly result: AssistantMessage;
}> {}

/**
 * A throw of the tracer while annotating is dropped: telemetry never fails the request it traces, and the
 * request's own reports are about the model.
 */
const dropped = telemetry.drop;

/** The request's attributes, when it starts: effect-durable-harness's identity attributes and the GenAI request's. */
export const annotateRequest = (
  span: Tracer.Span,
  model: ResolvedModel,
  identity: telemetry.Attributes,
): void => {
  telemetry.annotate(span, identity, dropped);
  telemetry.guarded(
    () =>
      AiTelemetry.addGenAIAnnotations(span, {
        provider: { name: model.provider },
        operation: { name: "chat" },
        request: { model: model.id },
      }),
    dropped,
  );
};

/** The response's attributes, from the final message; a message that stopped with `error` is the span's failure. */
export const annotateResponse = (
  span: Tracer.Span,
  message: AssistantMessage,
): ModelRequestFailed | undefined => {
  telemetry.guarded(
    () =>
      AiTelemetry.addGenAIAnnotations(span, {
        response: {
          id: message.responseId,
          model: message.model,
          finishReasons: [message.stopReason],
        },
        usage: { inputTokens: message.usage.input, outputTokens: message.usage.output },
      }),
    dropped,
  );
  if (message.stopReason !== "error") return undefined;
  return new ModelRequestFailed({
    message: (message.errorMessage ?? "").slice(0, 500),
    result: message,
  });
};

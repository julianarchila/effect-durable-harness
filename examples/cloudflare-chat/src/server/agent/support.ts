/** What several tools share: their typed failure and the text of a model message. */
import type { Message } from "@effect-durable-harness/core/Message";
import * as Predicate from "effect/Predicate";
import * as Schema from "effect/Schema";

/** A tool failure the model reads as the call's error. */
export class ToolFailed extends Schema.TaggedError<ToolFailed>()("ToolFailed", {
  message: Schema.String,
}) {}

/** The text of a user or assistant message; empty for any other. */
export const messageText = (message: Message | undefined): string => {
  if (message === undefined || message.role === "system" || message.role === "toolResult")
    return "";
  if (Predicate.isString(message.content)) return message.content;
  return message.content.flatMap((part) => (part.type === "text" ? [part.text] : [])).join("\n");
};

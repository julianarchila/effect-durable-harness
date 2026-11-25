/**
 * Pure transcript helpers and the first pass of `transformMessages`: what is done to a context before a provider
 * converts it. The helpers take the message list, which is all they read. `getCurrentTools` is the kernel's, public
 * in `@effect-durable-harness/core/Message`, since the kernel uses it too.
 */
import * as Predicate from "effect/Predicate";
import type * as Types from "effect/Types";
import {
  getCurrentTools,
  type AssistantMessage,
  type ImageContent,
  type Message,
  type SystemMessage,
  type TextContent,
} from "@effect-durable-harness/core/Message";
import type { ResolvedModel } from "@effect-durable-harness/core/Models";

const isSystem = (message: Message): message is SystemMessage => message.role === "system";

/** The text of a message's content. */
export const contentText = (
  content: string | ReadonlyArray<{ readonly type: string }>,
  separator = "\n",
): string =>
  Predicate.isString(content)
    ? content
    : content
        .filter((block): block is TextContent => block.type === "text")
        .map((block) => block.text)
        .join(separator);

/** A system message's text: content followed by its sections. */
export const getSystemMessageText = (message: SystemMessage): string => {
  const parts = [contentText(message.content)];
  for (const text of Object.values(message.sections ?? {})) {
    if (text !== null) parts.push(text);
  }
  return parts.filter((part) => part.length > 0).join("\n\n");
};

/** A later system message, sections framed by name. */
export const renderSystemMessageUpdate = (message: SystemMessage): string => {
  const parts: Array<string> = [];
  const text = contentText(message.content);
  if (text.length > 0) parts.push(text);
  for (const [name, value] of Object.entries(message.sections ?? {})) {
    parts.push(
      value === null
        ? `Removed system prompt section "${name}".`
        : `Updated system prompt section "${name}":\n\n${value}`,
    );
  }
  return parts.join("\n\n");
};

/** Every system message replayed into one. */
export const getCurrentSystemMessage = (
  messages: ReadonlyArray<Message>,
): SystemMessage | undefined => {
  const content: Array<string> = [];
  const sections = new Map<string, string>();
  let timestamp: number | undefined;
  for (const message of messages) {
    if (!isSystem(message)) continue;
    timestamp ??= message.timestamp;
    const text = contentText(message.content);
    if (text.length > 0) content.push(text);
    for (const [name, value] of Object.entries(message.sections ?? {})) {
      if (value === null) sections.delete(name);
      else sections.set(name, value);
    }
  }
  const tools = getCurrentTools(messages);
  if (timestamp === undefined && tools.length === 0) return undefined;
  const current: Types.Mutable<SystemMessage> = {
    role: "system",
    content: content.join("\n\n"),
    timestamp: timestamp ?? 0,
  };
  if (sections.size > 0) current.sections = Object.fromEntries(sections);
  if (tools.length > 0) current.toolsAdded = tools;
  return current;
};

/** The replayed system message leads. */
export const collapseSystemMessages = (messages: ReadonlyArray<Message>): Array<Message> => {
  const head = getCurrentSystemMessage(messages);
  const rest = messages.filter((message) => message.role !== "system");
  return head ? [head, ...rest] : rest;
};

/** Later system messages in place, or collapsed. */
export const resolveTranscript = (
  messages: ReadonlyArray<Message>,
  supportsMidConvoSystemMessages: boolean,
): ReadonlyArray<Message> =>
  supportsMidConvoSystemMessages ? messages : collapseSystemMessages(messages);

// -----------------------------------------------------------------------------
// transformMessages, first pass
// -----------------------------------------------------------------------------

export const NON_VISION_USER_IMAGE_PLACEHOLDER = "(image omitted: model does not support images)";
export const NON_VISION_TOOL_IMAGE_PLACEHOLDER =
  "(tool image omitted: model does not support images)";

/** One placeholder per run of images. */
const replaceImagesWithPlaceholder = (
  content: ReadonlyArray<TextContent | ImageContent>,
  placeholder: string,
): Array<TextContent> => {
  const result: Array<TextContent> = [];
  let previousWasPlaceholder = false;
  for (const block of content) {
    if (block.type === "image") {
      if (!previousWasPlaceholder) result.push({ type: "text", text: placeholder });
      previousWasPlaceholder = true;
      continue;
    }
    result.push(block);
    previousWasPlaceholder = block.text === placeholder;
  }
  return result;
};

/**
 * `transformMessages`, first pass: images become a placeholder for a model
 * without image input; a response from another model loses what only its own model can read (redacted thinking,
 * signatures, empty thinking), and its thinking becomes text.
 *
 * There is no second pass that drops errored or aborted responses and synthesizes missing tool results: the kernel's
 * context derivation already does both before a request. Tool call ids are not normalized either: `@opencode/ai` scrubs them per
 * protocol (`anthropic-messages.ts`, `scrubToolCallID`).
 */
export const transformMessages = (
  messages: ReadonlyArray<Message>,
  model: ResolvedModel,
): Array<Message> => {
  const vision = model.input.includes("image");
  return messages.map((message): Message => {
    switch (message.role) {
      case "system":
        return message;
      case "user":
        return vision || Predicate.isString(message.content)
          ? message
          : {
              ...message,
              content: replaceImagesWithPlaceholder(
                message.content,
                NON_VISION_USER_IMAGE_PLACEHOLDER,
              ),
            };
      case "toolResult":
        return vision
          ? message
          : {
              ...message,
              content: replaceImagesWithPlaceholder(
                message.content,
                NON_VISION_TOOL_IMAGE_PLACEHOLDER,
              ),
            };
      case "assistant":
        return transformAssistant(message, model);
    }
  });
};

const transformAssistant = (message: AssistantMessage, model: ResolvedModel): AssistantMessage => {
  const isSameModel =
    message.provider === model.provider && message.api === model.api && message.model === model.id;
  const content = message.content.flatMap((block): AssistantMessage["content"] => {
    if (block.type === "thinking") {
      if (block.redacted === true) return isSameModel ? [block] : [];
      if (isSameModel && block.thinkingSignature !== undefined && block.thinkingSignature !== "")
        return [block];
      if (block.thinking.trim() === "") return [];
      if (isSameModel) return [block];
      return [{ type: "text", text: block.thinking }];
    }
    if (block.type === "text") return isSameModel ? [block] : [{ type: "text", text: block.text }];
    if (!isSameModel && block.thoughtSignature !== undefined) {
      const { thoughtSignature: _dropped, ...rest } = block;
      return [rest];
    }
    return [block];
  });
  return { ...message, content };
};

// The token estimates compaction uses: the size of a model context, measured by a provider's usage where one is
// known and estimated at 3.5 characters a token otherwise. Compaction's `estimateContext` walks the view itself.
import * as Predicate from "effect/Predicate";
import * as Schema from "effect/Schema";
import type { ImageContent, Message, SystemMessage, TextContent, Usage } from "../Message.ts";

const CHARS_PER_TOKEN = 3.5;
const ESTIMATED_IMAGE_CHARS = 4800;

/** The context a response's usage measured. */
export const calculateContextTokens = (usage: Usage): number =>
  usage.totalTokens || usage.input + usage.output + usage.cacheRead + usage.cacheWrite;

/** The estimated tokens of a text. */
export const estimateTextTokens = (text: string): number =>
  Math.ceil(text.length / CHARS_PER_TOKEN);

/** The estimated tokens of a message: system text and tools, user and tool result content, or assistant blocks. */
export const estimateMessageTokens = (message: Message): number => {
  switch (message.role) {
    case "system":
      return (
        estimateTextTokens(systemMessageText(message)) +
        estimateToolsTokens(message.toolsAdded) +
        estimateToolsTokens(message.toolsRemoved)
      );
    case "user":
    case "toolResult":
      return Math.ceil(contentChars(message.content) / CHARS_PER_TOKEN);
    case "assistant": {
      let chars = 0;
      for (const block of message.content) {
        if (block.type === "text") chars += block.text.length;
        else if (block.type === "thinking") chars += block.thinking.length;
        else chars += block.name.length + encodeJson(block.arguments).length;
      }
      return Math.ceil(chars / CHARS_PER_TOKEN);
    }
  }
};

/** The system message's content followed by its sections. */
const systemMessageText = (message: SystemMessage): string => {
  const parts = [contentText(message.content)];
  for (const text of Object.values(message.sections ?? {})) if (text !== null) parts.push(text);
  return parts.filter((part) => part.length > 0).join("\n\n");
};

const contentText = (content: string | ReadonlyArray<TextContent>): string =>
  Predicate.isString(content) ? content : content.map((block) => block.text).join("\n");

/** The characters of text and image content, an image counting as a fixed size. */
const contentChars = (content: string | ReadonlyArray<TextContent | ImageContent>): number => {
  if (Predicate.isString(content)) return content.length;
  let chars = 0;
  for (const block of content)
    chars += block.type === "text" ? block.text.length : ESTIMATED_IMAGE_CHARS;
  return chars;
};

const estimateToolsTokens = (tools: ReadonlyArray<Schema.Json> | undefined): number =>
  tools === undefined || tools.length === 0 ? 0 : estimateTextTokens(encodeJson(tools));

// Values here are a message's JSON, so the encoding never fails.
const encodeJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));

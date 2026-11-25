/**
 * `search_history` (replay-safe): a subagent in a child conversation the tool call owns searches the session's other
 * conversations with `search_conversations` and answers; the parent gets only the answer. Its handler is in
 * `search-history.live.ts`: it configures the child with `ConversationSearch`, from `../chat.ts`, which lists this
 * contract, so one file would import itself through `chat.ts`.
 */
import * as AgentTool from "@effect-durable-harness/core/AgentTool";
import * as Schema from "effect/Schema";

export const SearchHistory = AgentTool.make("search_history", {
  description:
    "Search ordinary conversation turns in this session to answer a question about earlier discussions. " +
    "A subagent searches the conversations and replies with a short answer that cites conversation IDs.",
  parameters: Schema.Struct({
    question: Schema.String.annotate({ description: "What to find out from past conversations" }),
  }),
  /** The subagent's conversation: a client watching the parent links to it. */
  details: Schema.Struct({ conversationId: Schema.Number }),
  // Safe to rerun after a crash: a rerun finds the child it created and the submission it made.
  replay: "safe",
});

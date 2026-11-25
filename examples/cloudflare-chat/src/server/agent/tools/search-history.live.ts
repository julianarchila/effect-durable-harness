/** `search_history`'s handler (the contract is `search-history.ts`). */
import * as Agent from "@effect-durable-harness/core/Agent";
import * as AgentTool from "@effect-durable-harness/core/AgentTool";
import { AssistantEntry } from "@effect-durable-harness/core/Entry";
import { Tx } from "@effect-durable-harness/core/Tx";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import { ChatModel, ConversationSearch } from "../chat.ts";
import { messageText, ToolFailed } from "../support.ts";
import { SearchHistory } from "./search-history.ts";

export const SearchHistoryLive = SearchHistory.toLayer(
  Effect.gen(function* () {
    const model = yield* ChatModel;
    return (args, call) =>
      Effect.gen(function* () {
        const childId = yield* call.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            // Ownership is the durable record of the child, so a rerun reuses it instead of creating another.
            const existing = (yield* tx.scanConversations({ ownerTaskId: call.taskId }, 1))
              .items[0];
            if (existing !== undefined) return existing.id;
            const child = yield* tx.createConversation({
              ownership: { kind: "task", taskId: call.taskId },
            });
            yield* Agent.configure(child.id, {
              model: model.ref,
              extensions: [ConversationSearch],
            });
            return child.id;
          }),
        );
        // A client watching the parent finds the child in the call's details.
        yield* call.details({ conversationId: childId });
        const child = yield* call.requireConversation(childId);
        const submission = yield* child.submit({
          type: "input",
          content:
            `Answer this question using search_conversations: ${args.question}\n\n` +
            "Try a few relevant terms. Reply briefly, cite conversation IDs, and say so if nothing relevant was found.",
          // The task ID is stable across reruns, so a rerun gets back this submission.
          requestId: `search-history:${call.taskId}`,
        });
        const settled = yield* submission.await;
        if (settled.status !== "done" || settled.type !== "input") {
          return yield* new ToolFailed({ message: `The search did not answer: ${settled.status}` });
        }
        const answerId = settled.answer;
        const answer = yield* call.commit(
          Effect.flatMap(Tx, (tx) => tx.entry(AssistantEntry, answerId)),
        );
        const reply = Option.match(answer, {
          onNone: () => "",
          onSome: (entry) => messageText(entry.model?.[0]),
        });
        return AgentTool.text(reply, { details: { conversationId: childId } });
      });
  }),
);

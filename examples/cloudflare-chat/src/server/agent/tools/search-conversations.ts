/**
 * `search_conversations` (replay-safe): the search subagent's only tool. Case-insensitive substring search over
 * ordinary turns of ownerless conversations. Owned conversations (subagents) and turns that call `search_history`
 * are skipped, so earlier searches never become evidence. Each page is read in a commit of its own, so a scan never
 * holds the Session's line for long. It only reads through its call, so it is implemented inline.
 */
import * as AgentTool from "@effect-durable-harness/core/AgentTool";
import { AssistantEntry, UserEntry } from "@effect-durable-harness/core/Entry";
import type { ConversationRecord, EntryRecord } from "@effect-durable-harness/core/Records";
import { Tx } from "@effect-durable-harness/core/Tx";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import { messageText, ToolFailed } from "../support.ts";

const PAGE_SIZE = 50;
const MAX_MATCHES = 20;
const SNIPPET_RADIUS = 80;

const Match = Schema.Struct({
  conversationId: Schema.Number,
  entryId: Schema.Number,
  role: Schema.Literals(["user", "assistant"]),
  snippet: Schema.String,
});
type Match = typeof Match.Type;

export const SearchConversations = AgentTool.make("search_conversations", {
  description:
    "Find user and assistant messages containing a term in ordinary top-level conversation turns. " +
    `Case-insensitive substring match; returns at most ${MAX_MATCHES} matches with snippets.`,
  parameters: Schema.Struct({
    term: Schema.String.annotate({ description: "Word or phrase to find, for example postgres" }),
  }),
  details: Schema.Struct({ matches: Schema.Number, truncated: Schema.Boolean }),
  replay: "safe",
  execute: ({ term }, call) =>
    Effect.gen(function* () {
      const needle = term.trim().toLowerCase();
      if (needle.length === 0)
        return yield* new ToolFailed({ message: "Search term must contain text" });
      const matches: Array<Match> = [];
      const scan = { truncated: false };
      const conversations: Array<ConversationRecord> = [];
      let cursor = Option.none<Schema.JsonObject>();
      do {
        const page = yield* call.commit(
          Effect.flatMap(Tx, (tx) =>
            tx.scanConversations({}, PAGE_SIZE, Option.getOrUndefined(cursor)),
          ),
        );
        conversations.push(...page.items.filter((record) => record.owner === undefined));
        cursor = page.next;
      } while (Option.isSome(cursor));
      for (const conversation of conversations) {
        let turn: Array<EntryRecord> = [];
        let entries = Option.none<Schema.JsonObject>();
        const flush = () => {
          if (!turn.some(invokesHistorySearch)) {
            for (const entry of turn) {
              const found = matchOf(entry, needle);
              if (found === undefined) continue;
              if (matches.length === MAX_MATCHES) scan.truncated = true;
              else matches.push(found);
            }
          }
          turn = [];
        };
        do {
          const page = yield* call.commit(
            Effect.flatMap(Tx, (tx) =>
              tx.scanEntries(
                { conversationId: conversation.id },
                PAGE_SIZE,
                Option.getOrUndefined(entries),
              ),
            ),
          );
          for (const entry of page.items) {
            // A fork's scan includes its inherited history: count each entry once, where it was written.
            if (entry.conversationId !== conversation.id) continue;
            // Newest first: reaching the user entry completes one turn.
            turn.push(entry);
            if (UserEntry.is(entry)) flush();
          }
          entries = page.next;
        } while (Option.isSome(entries) && !scan.truncated);
        flush();
        if (scan.truncated) break;
      }
      return AgentTool.json(
        { matches, truncated: scan.truncated },
        { details: { matches: matches.length, truncated: scan.truncated } },
      );
    }),
});

const invokesHistorySearch = (entry: EntryRecord): boolean =>
  AssistantEntry.is(entry) &&
  (entry.model ?? []).some(
    (message) =>
      message.role === "assistant" &&
      message.content.some((part) => part.type === "toolCall" && part.name === "search_history"),
  );

const matchOf = (entry: EntryRecord, needle: string): Match | undefined => {
  const role = UserEntry.is(entry) ? "user" : AssistantEntry.is(entry) ? "assistant" : undefined;
  if (role === undefined) return undefined;
  const body = messageText(entry.model?.[0]);
  const index = body.toLowerCase().indexOf(needle);
  if (index === -1) return undefined;
  const start = Math.max(0, index - SNIPPET_RADIUS);
  const end = Math.min(body.length, index + needle.length + SNIPPET_RADIUS);
  return {
    conversationId: entry.conversationId,
    entryId: entry.id,
    role,
    snippet: `${start > 0 ? "…" : ""}${body.slice(start, end)}${end < body.length ? "…" : ""}`,
  };
};

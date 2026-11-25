/**
 * What a UI shows of one conversation, derived from its transcript (`ConversationTranscript`'s view of the
 * `ConversationView`): the committed entries the client holds, with the gaps and the `earlier` marker a
 * renderer pages with, and the
 * built-in documents `core.live` (the streaming partial and the tool round), `core.inbox` (queued inputs), `core.agent`
 * (model and tools) and `core.usage` (tokens and cost).
 *
 * The documents are read with permissive local schemas, so a browser bundle does not include the kernel. Entries keep
 * their raw text and message: an application that frames its inputs (attribution, a scheduled prefix) parses them.
 *
 * @since 0.0.0
 */
import type {
  TranscriptGap,
  TranscriptView,
} from "@effect-durable-harness/core/ConversationTranscript";
import type {
  AssistantMessage,
  Message,
  ToolResultMessage,
} from "@effect-durable-harness/core/Message";
import type { EntryRecord } from "@effect-durable-harness/core/Records";
import * as Option from "effect/Option";
import * as Predicate from "effect/Predicate";
import * as Schema from "effect/Schema";

/**
 * A tool call an assistant entry made.
 *
 * @category models
 * @since 0.0.0
 */
export interface ToolCallView {
  readonly callId: string;
  readonly name: string;
  readonly args: Schema.Json;
}

/**
 * A tool result entry.
 *
 * @category models
 * @since 0.0.0
 */
export interface ToolResultView {
  readonly callId: string;
  readonly name: string;
  readonly text: string;
  readonly isError: boolean;
  readonly details: Schema.Json | undefined;
}

/**
 * One committed entry. `role` is its first model message's (`tool` for a tool result), or `data` for an entry with
 * no model message (display-only, application-defined); system messages and markers are left out.
 *
 * @category models
 * @since 0.0.0
 */
export interface EntryView {
  readonly id: number;
  readonly kind: string;
  readonly role: "user" | "assistant" | "tool" | "data";
  /** The text of the message, as the model reads it; empty for `data`. */
  readonly text: string;
  /** The first model message, raw. */
  readonly message: Message | undefined;
  /** The entry's JSON payload. */
  readonly data: Schema.Json | undefined;
  readonly at: number | undefined;
  readonly toolCalls: ReadonlyArray<ToolCallView>;
  readonly toolResult: ToolResultView | undefined;
  /** The response stopped short: aborted by a restart or an abort, or failed. */
  readonly stopped: string | undefined;
}

/**
 * A tool call of the running round (`core.live`).
 *
 * @category models
 * @since 0.0.0
 */
export interface LiveToolView {
  readonly callId: string;
  readonly name: string;
  readonly status: "pending" | "running" | "sleeping" | "waiting" | "done";
  readonly output: string | undefined;
  readonly details: Schema.Json | undefined;
}

/**
 * An input waiting in `core.inbox`: a steer joins the run at its next boundary, a follow-up waits for the next turn.
 *
 * @category models
 * @since 0.0.0
 */
export interface QueuedView {
  readonly id: number;
  readonly text: string;
  readonly steer: boolean;
}

/**
 * One row of a conversation as a UI renders it, in ID order: a shown entry, a gap of entries not loaded yet (render
 * "load more" and call `SessionClient.fillGap`), or the marker that the transcript continues above (render "load
 * earlier" and call `SessionClient.loadEarlier`).
 *
 * @category models
 * @since 0.0.0
 */
export type ItemView =
  | { readonly kind: "entry"; readonly entry: EntryView }
  | TranscriptGap
  | { readonly kind: "earlier" };

/**
 * The UI state of a conversation.
 *
 * @category models
 * @since 0.0.0
 */
export interface ConversationState {
  /** Every entry shown, in ID order: `items` without the gaps and the marker. */
  readonly entries: ReadonlyArray<EntryView>;
  readonly items: ReadonlyArray<ItemView>;
  /** The response being streamed, while a generation runs. */
  readonly live: { readonly text: string; readonly retry: string | undefined } | undefined;
  /** A run is in progress. */
  readonly running: boolean;
  readonly tools: ReadonlyArray<LiveToolView>;
  readonly queued: ReadonlyArray<QueuedView>;
  /** `provider/modelId`. */
  readonly model: string | undefined;
  readonly toolNames: ReadonlyArray<string>;
  readonly tokens: number;
  readonly cost: number;
}

/**
 * The state of a conversation with no view yet.
 *
 * @category constants
 * @since 0.0.0
 */
export const empty: ConversationState = {
  entries: [],
  items: [],
  live: undefined,
  running: false,
  tools: [],
  queued: [],
  model: undefined,
  toolNames: [],
  tokens: 0,
  cost: 0,
};

const LiveDoc = Schema.Struct({
  run: Schema.optional(Schema.Struct({ taskId: Schema.Number })),
  generation: Schema.optional(
    Schema.Struct({
      message: Schema.optional(Schema.Struct({ content: Schema.Array(Schema.Json) })),
      retry: Schema.optional(Schema.Struct({ error: Schema.String })),
    }),
  ),
  tools: Schema.optional(
    Schema.Array(
      Schema.Struct({
        callId: Schema.String,
        name: Schema.String,
        status: Schema.Literals(["pending", "running", "sleeping", "waiting", "done"]),
        output: Schema.optional(Schema.String),
        details: Schema.optional(Schema.Json),
      }),
    ),
  ),
});

const InboxDoc = Schema.Struct({
  items: Schema.Array(
    Schema.Struct({
      id: Schema.Number,
      mode: Schema.String,
      content: Schema.optional(Schema.Json),
    }),
  ),
});

const AgentDoc = Schema.Struct({
  model: Schema.optional(Schema.Struct({ provider: Schema.String, modelId: Schema.String })),
  tools: Schema.optional(Schema.Json),
});

const Totals = Schema.Struct({
  totalTokens: Schema.Number,
  cost: Schema.Struct({ total: Schema.Number }),
});
const UsageDoc = Schema.Struct({
  models: Schema.optional(Schema.Record(Schema.String, Totals)),
  tools: Schema.optional(Schema.Record(Schema.String, Totals)),
});

const TextPart = Schema.Struct({ type: Schema.Literal("text"), text: Schema.String });

const contentText = (content: Message["content"]): string =>
  Predicate.isString(content)
    ? content
    : content.flatMap((part) => (part.type === "text" ? [part.text] : [])).join("");

const inputText = (content: Schema.Json | undefined): string => {
  if (Predicate.isString(content)) return content;
  const parts = Schema.decodeUnknownOption(Schema.Array(Schema.Json))(content);
  if (Option.isNone(parts)) return "";
  return parts.value
    .flatMap((part) => Option.toArray(Schema.decodeUnknownOption(TextPart)(part)))
    .map((part) => part.text)
    .join("");
};

const noCalls: ReadonlyArray<ToolCallView> = [];

const assistantOf = (message: AssistantMessage) => ({
  toolCalls: message.content.flatMap((part) =>
    part.type === "toolCall" ? [{ callId: part.id, name: part.name, args: part.arguments }] : [],
  ),
  stopped:
    message.stopReason === "aborted" || message.stopReason === "error"
      ? (message.errorMessage ?? message.stopReason)
      : undefined,
});

const toolResultOf = (message: ToolResultMessage): ToolResultView => ({
  callId: message.toolCallId,
  name: message.toolName,
  text: contentText(message.content),
  isError: message.isError,
  details: message.details,
});

/**
 * One entry as a UI shows it; `undefined` for an entry it does not show (a system message, a head marker).
 *
 * @category constructors
 * @since 0.0.0
 */
export const entryOf = (entry: EntryRecord): EntryView | undefined => {
  const message = entry.model?.[0];
  const base = {
    id: entry.id,
    kind: entry.kind,
    data: entry.data,
    message,
    toolCalls: noCalls,
    toolResult: undefined,
    stopped: undefined,
  };
  if (message === undefined) {
    return entry.data === undefined
      ? undefined
      : { ...base, role: "data", text: "", at: undefined };
  }
  if (message.role === "system") return undefined;
  const at = message.timestamp > 0 ? message.timestamp : undefined;
  const text = contentText(message.content);
  if (message.role === "user") return { ...base, role: "user", text, at };
  if (message.role === "assistant") {
    return { ...base, role: "assistant", text, at, ...assistantOf(message) };
  }
  return { ...base, role: "tool", text, at, toolResult: toolResultOf(message) };
};

/**
 * The UI state of a conversation's transcript; `empty` for none.
 *
 * @category constructors
 * @since 0.0.0
 */
export const make = (view: TranscriptView | null): ConversationState => {
  if (view === null) return empty;
  const items = view.items.flatMap((item): ReadonlyArray<ItemView> => {
    if (item.kind !== "entry") return [item];
    const shown = entryOf(item.entry);
    return shown === undefined ? [] : [{ kind: "entry", entry: shown }];
  });
  const live = Option.getOrUndefined(
    Schema.decodeUnknownOption(LiveDoc)(view.docs["core.live"] ?? {}),
  );
  const inbox = Option.getOrUndefined(
    Schema.decodeUnknownOption(InboxDoc)(view.docs["core.inbox"] ?? {}),
  );
  const agent = Option.getOrUndefined(
    Schema.decodeUnknownOption(AgentDoc)(view.docs["core.agent"] ?? {}),
  );
  const usage = Option.getOrUndefined(
    Schema.decodeUnknownOption(UsageDoc)(view.docs["core.usage"] ?? {}),
  );
  const generation = live?.generation;
  const partial = generation?.message?.content
    .flatMap((part) => Option.toArray(Schema.decodeUnknownOption(TextPart)(part)))
    .map((part) => part.text)
    .join("");
  let tokens = 0;
  let cost = 0;
  for (const bucket of [usage?.models ?? {}, usage?.tools ?? {}]) {
    for (const totals of Object.values(bucket)) {
      tokens += totals.totalTokens;
      cost += totals.cost.total;
    }
  }
  const toolNames = Schema.decodeUnknownOption(Schema.Array(Schema.String))(agent?.tools);
  return {
    entries: items.flatMap((item) => (item.kind === "entry" ? [item.entry] : [])),
    items,
    live:
      generation === undefined
        ? undefined
        : { text: partial ?? "", retry: generation.retry?.error },
    running: live?.run !== undefined,
    tools: (live?.tools ?? []).map((slot) => ({
      callId: slot.callId,
      name: slot.name,
      status: slot.status,
      output: slot.output,
      details: slot.details,
    })),
    queued: (inbox?.items ?? [])
      .filter((item) => item.mode !== "write")
      .map((item) => ({
        id: item.id,
        text: inputText(item.content),
        steer: item.mode === "steer",
      })),
    model:
      agent?.model === undefined ? undefined : `${agent.model.provider}/${agent.model.modelId}`,
    toolNames: Option.getOrElse(toolNames, () => []),
    tokens,
    cost,
  };
};

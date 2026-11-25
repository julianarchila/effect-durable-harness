/**
 * What the chat reads into the generic conversation state (`@effect-durable-harness/host/ConversationState`): who sent a
 * prompt (its attribution), whether an input is a scheduled delivery, who is typing, and which conversation a
 * `search_history` call's subagent runs in.
 */
import type { Peer } from "@effect-durable-harness/host/SessionProtocol";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import { parseAttribution, parseScheduledInput } from "../protocol.ts";

/** A user entry or queued input as the transcript shows it. */
export type Input =
  | { readonly kind: "user"; readonly username: string | undefined; readonly text: string }
  | { readonly kind: "scheduled"; readonly text: string };

/** Reads the attribution or the scheduled prefix of an input's text. */
export const inputOf = (raw: string): Input => {
  const { username, text } = parseAttribution(raw);
  const scheduled = parseScheduledInput(text);
  return scheduled === undefined
    ? { kind: "user", username, text }
    : { kind: "scheduled", text: scheduled };
};

const Member = Schema.Struct({ username: Schema.String });
const Typing = Schema.Struct({ typing: Schema.Boolean });

/** The names of the other connections typing in a conversation, sorted, each once. */
export const typingNames = (peers: ReadonlyArray<Peer>): ReadonlyArray<string> =>
  [
    ...new Set(
      peers.flatMap((peer) => {
        const typing = Schema.decodeUnknownOption(Typing)(peer.presence);
        const member = Schema.decodeUnknownOption(Member)(peer.connection);
        return Option.isSome(typing) && typing.value.typing && Option.isSome(member)
          ? [member.value.username]
          : [];
      }),
    ),
  ].toSorted((left, right) => left.localeCompare(right));

/** The conversation a tool call's details name, for a subagent's link. */
export const subagentOf = (details: Schema.Json | undefined): number | undefined =>
  Option.getOrUndefined(
    Option.map(
      Schema.decodeUnknownOption(Schema.Struct({ conversationId: Schema.Number }))(details),
      (value) => value.conversationId,
    ),
  );

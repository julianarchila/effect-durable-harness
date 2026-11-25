/**
 * The session socket's protocol (`Host.socket`): JSON text frames, decoded with these schemas on both ends. It
 * imports no kernel runtime, so a browser bundle can use it.
 *
 * A client sends `ClientCommand`s. Every command but `unwatch` and `presence` carries a `requestId` and is answered
 * by one `result` with the same `requestId`; a command resent under the same `requestId` (after a reconnect) gets
 * the answer of the first: `submit` through the kernel's own `requestId`, `create` and `fork` through a receipt the
 * host commits with the conversation.
 *
 * The object sends `ServerEvent`s: `hello` first, then `session` (the conversation list) and `activity`; `view`
 * frames for each watched conversation (a `@effect-durable-harness/host/View` message: a snapshot, then op batches, of a window
 * over the conversation's entries); `entries` pages answer the client's `entries` requests, with which
 * a `ConversationTranscript` loads what lies outside the window; `presence`
 * of the other watchers of a conversation; `conversation` when one is created; `event` for an application's own
 * messages (`Host.Instance.socket`).
 *
 * The connection and presence values are carried as JSON, as the object's schemas encode them; a client that needs
 * them typed decodes them with the same schemas.
 *
 * @since 0.0.0
 */
import * as Schema from "effect/Schema";

/**
 * Maximum length of a prompt the socket accepts.
 *
 * @category constants
 * @since 0.0.0
 */
export const MAX_CONTENT_CHARACTERS = 100_000;

/**
 * Maximum length of a request ID.
 *
 * @category constants
 * @since 0.0.0
 */
export const MAX_REQUEST_ID_CHARACTERS = 256;

/**
 * Maximum number of conversations one connection watches at once. The socket's attachment, which keeps them with
 * the connection state and presence across hibernation, holds at most 2 KiB; a `watch` over the cap is refused
 * with `invalid`.
 *
 * @category constants
 * @since 0.0.0
 */
export const MAX_WATCHED_CONVERSATIONS = 8;

/**
 * Maximum length of a presence value, as JSON text. A larger one is refused with `invalid_message`, as is any
 * presence that would make the attachment exceed its 2 KiB.
 *
 * @category constants
 * @since 0.0.0
 */
export const MAX_PRESENCE_CHARACTERS = 128;

/**
 * The close code of a connection the object refused (`accept` failed) or closed for good: a client does not
 * reconnect after it. A browser cannot read the HTTP status of a failed upgrade, so the object accepts the upgrade
 * and closes it at once with this code. An application that closes a connection for good (`Host.Instance.socket.close`) can
 * use it too.
 *
 * @category constants
 * @since 0.0.0
 */
export const REFUSED_CLOSE_CODE = 4403;

const RequestId = Schema.String.check(
  Schema.isMinLength(1),
  Schema.isMaxLength(MAX_REQUEST_ID_CHARACTERS),
);
const Id = Schema.Int.check(Schema.isGreaterThan(0));

/**
 * Maximum entries one `entries` request returns.
 *
 * @category constants
 * @since 0.0.0
 */
export const MAX_ENTRY_PAGE = 200;

/** The kernel's default `ConversationWindow.min`, repeated here so the protocol bundles no kernel runtime. */
const DEFAULT_WINDOW_MIN = 50;

/**
 * Which entries a watch's view holds (`@effect-durable-harness/core/View` `ConversationWindow`): a client resumes with
 * `ConversationTranscript.resume`.
 *
 * @category models
 * @since 0.0.0
 */
export const ConversationWindow = Schema.Struct({
  from: Schema.optionalKey(Id),
  min: Schema.optionalKey(
    Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: MAX_ENTRY_PAGE })),
  ),
  max: Schema.optionalKey(
    Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 2 * MAX_ENTRY_PAGE })),
  ),
}).check(
  // The kernel rejects `max` below `min` with a `RangeError` (`ConversationWindow`); refusing it here answers the
  // client instead of leaving its watch loading. `min` defaults to 50 there.
  Schema.makeFilter((window) =>
    window.max === undefined || window.max >= (window.min ?? DEFAULT_WINDOW_MIN)
      ? undefined
      : { path: ["max"], issue: "max must be at least min (50 when min is absent)" },
  ),
);

/**
 * The type of `ConversationWindow`.
 *
 * @category models
 * @since 0.0.0
 */
export type ConversationWindow = typeof ConversationWindow.Type;

/**
 * A conversation as a session list shows it.
 *
 * @category models
 * @since 0.0.0
 */
export const ConversationSummary = Schema.Struct({
  id: Id,
  /** Set for a fork: the conversation and entry it was forked from. */
  parent: Schema.optionalKey(Schema.Struct({ conversationId: Id, at: Id })),
  /** Set for a conversation a tool call owns, such as a subagent's. */
  owner: Schema.optionalKey(Schema.Struct({ conversationId: Id, taskId: Id })),
});

/**
 * The type of `ConversationSummary`.
 *
 * @category models
 * @since 0.0.0
 */
export type ConversationSummary = typeof ConversationSummary.Type;

/**
 * One live task, as `Harness.inspect` reports it.
 *
 * @category models
 * @since 0.0.0
 */
export const ActivityTask = Schema.Struct({
  id: Id,
  kind: Schema.String,
  conversationId: Id,
  owner: Schema.optionalKey(Id),
  /** The scheduler's state: `running`, `ready`, `waiting`, `sleeping`, `completing`, `blocked`. */
  status: Schema.String,
  /** The checkpoint's `phase`, when it has one. */
  phase: Schema.optionalKey(Schema.String),
  waitingOn: Schema.optionalKey(Schema.Array(Id)),
  background: Schema.Boolean,
  abortRequested: Schema.Boolean,
});

/**
 * The type of `ActivityTask`.
 *
 * @category models
 * @since 0.0.0
 */
export type ActivityTask = typeof ActivityTask.Type;

/**
 * What a busy conversation does with a prompt (`whenBusy`): `followUp` (the default) waits for the run to end and
 * starts a new turn; `steer` joins the run at its next boundary.
 *
 * @category models
 * @since 0.0.0
 */
export const WhenBusy = Schema.Literals(["steer", "followUp"]);

/**
 * The type of `WhenBusy`.
 *
 * @category models
 * @since 0.0.0
 */
export type WhenBusy = typeof WhenBusy.Type;

// ─── Client → object ───────────────────────────────────────────────────────

/**
 * A command of the client. `watch` follows a conversation's view, with an optional window; `unwatch` stops; `entries`
 * reads a page of a conversation's fork-aware history (`Conversation.entries`, at most `limit` entries, with `more`
 * when the range goes on); `create` and `fork` make an ownerless conversation; `submit` admits a prompt; `withdraw`
 * removes a queued one; `abort` stops a conversation's work; `presence` sets (or, with `null`, clears) this
 * connection's presence in a watched conversation.
 *
 * @category models
 * @since 0.0.0
 */
export const ClientCommand = Schema.Union([
  Schema.Struct({
    type: Schema.Literal("watch"),
    requestId: RequestId,
    conversationId: Id,
    window: Schema.optionalKey(ConversationWindow),
  }),
  Schema.Struct({ type: Schema.Literal("unwatch"), conversationId: Id }),
  Schema.Struct({
    type: Schema.Literal("entries"),
    requestId: RequestId,
    conversationId: Id,
    minEntryId: Schema.optionalKey(Schema.Int),
    maxEntryId: Schema.optionalKey(Schema.Int),
    order: Schema.optionalKey(Schema.Literals(["ascending", "descending"])),
    limit: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: MAX_ENTRY_PAGE })),
  }),
  Schema.Struct({ type: Schema.Literal("create"), requestId: RequestId }),
  Schema.Struct({
    type: Schema.Literal("fork"),
    requestId: RequestId,
    conversationId: Id,
    entryId: Id,
  }),
  Schema.Struct({
    type: Schema.Literal("submit"),
    requestId: RequestId,
    conversationId: Id,
    content: Schema.String.check(Schema.isMinLength(1), Schema.isMaxLength(MAX_CONTENT_CHARACTERS)),
    whenBusy: Schema.optionalKey(WhenBusy),
  }),
  Schema.Struct({
    type: Schema.Literal("withdraw"),
    requestId: RequestId,
    conversationId: Id,
    submissionId: Id,
  }),
  Schema.Struct({ type: Schema.Literal("abort"), requestId: RequestId, conversationId: Id }),
  Schema.Struct({
    type: Schema.Literal("presence"),
    conversationId: Id,
    presence: Schema.NullOr(Schema.Json),
  }),
]);

/**
 * The type of `ClientCommand`.
 *
 * @category models
 * @since 0.0.0
 */
export type ClientCommand = typeof ClientCommand.Type;

/**
 * A command the object answers with a `result`.
 *
 * @category models
 * @since 0.0.0
 */
export type Request = Extract<ClientCommand, { readonly requestId: string }>;

// ─── Object → client ───────────────────────────────────────────────────────

/**
 * Why a command failed: `not_found` (no such conversation or submission), `read_only` (a socket without commands),
 * `busy` (the conversation is busy, or the connection has too many aborts in flight), `conflict` (a request ID reused
 * for another command, a fork at an entry that is not visible, a submission no longer queued), `invalid` (a
 * submission the kernel rejects, a watch over `MAX_WATCHED_CONVERSATIONS` or past the attachment's 2 KiB) or
 * `failed`.
 *
 * @category models
 * @since 0.0.0
 */
export const ErrorCode = Schema.Literals([
  "not_found",
  "read_only",
  "busy",
  "conflict",
  "invalid",
  "failed",
  "invalid_message",
]);

/**
 * The type of `ErrorCode`.
 *
 * @category models
 * @since 0.0.0
 */
export type ErrorCode = typeof ErrorCode.Type;

/**
 * A failure as the protocol carries it.
 *
 * @category models
 * @since 0.0.0
 */
export const ProtocolError = Schema.Struct({ code: ErrorCode, message: Schema.String });

/**
 * The type of `ProtocolError`.
 *
 * @category models
 * @since 0.0.0
 */
export type ProtocolError = typeof ProtocolError.Type;

/**
 * A `@effect-durable-harness/host/View` message, carried as JSON: `snapshot` with the view, or `ops` with a wire batch.
 *
 * @category models
 * @since 0.0.0
 */
export const ViewFrame = Schema.Union([
  Schema.Struct({ type: Schema.Literal("snapshot"), value: Schema.NullOr(Schema.JsonObject) }),
  Schema.Struct({ type: Schema.Literal("ops"), ops: Schema.Array(Schema.Json) }),
]);

/**
 * Another connection watching the same conversation, with its presence.
 *
 * @category models
 * @since 0.0.0
 */
export const Peer = Schema.Struct({
  connectionId: Schema.String,
  connection: Schema.Json,
  presence: Schema.Json,
});

/**
 * The type of `Peer`.
 *
 * @category models
 * @since 0.0.0
 */
export type Peer = typeof Peer.Type;

/**
 * An event of the object.
 *
 * @category models
 * @since 0.0.0
 */
export const ServerEvent = Schema.Union([
  /** The first event of a connection: its ID and the connection state `accept` returned. */
  Schema.Struct({
    type: Schema.Literal("hello"),
    connectionId: Schema.String,
    connection: Schema.Json,
  }),
  Schema.Struct({
    type: Schema.Literal("session"),
    conversations: Schema.Array(ConversationSummary),
  }),
  Schema.Struct({ type: Schema.Literal("conversation"), conversation: ConversationSummary }),
  Schema.Struct({ type: Schema.Literal("activity"), tasks: Schema.Array(ActivityTask) }),
  Schema.Struct({ type: Schema.Literal("view"), conversationId: Id, frame: ViewFrame }),
  Schema.Struct({
    type: Schema.Literal("presence"),
    conversationId: Id,
    peers: Schema.Array(Peer),
  }),
  Schema.Struct({
    type: Schema.Literal("result"),
    requestId: RequestId,
    ok: Schema.Literal(true),
    conversationId: Schema.optionalKey(Id),
    submissionId: Schema.optionalKey(Id),
    /** The page of an `entries` request, as JSON entry records, and whether the range goes on past it. */
    entries: Schema.optionalKey(Schema.Array(Schema.JsonObject)),
    more: Schema.optionalKey(Schema.Boolean),
  }),
  Schema.Struct({
    type: Schema.Literal("result"),
    requestId: RequestId,
    ok: Schema.Literal(false),
    error: ProtocolError,
  }),
  /** An application's own message (`Host.Instance.socket`), for a conversation's watchers or chosen connections. */
  Schema.Struct({
    type: Schema.Literal("event"),
    conversationId: Schema.optionalKey(Id),
    data: Schema.Json,
  }),
  Schema.Struct({ type: Schema.Literal("error"), error: ProtocolError }),
]);

/**
 * The type of `ServerEvent`.
 *
 * @category models
 * @since 0.0.0
 */
export type ServerEvent = typeof ServerEvent.Type;

/**
 * The answer to a `Request`.
 *
 * @category models
 * @since 0.0.0
 */
export type Result = Extract<ServerEvent, { readonly type: "result" }>;

/**
 * Decodes a client frame.
 *
 * @category codecs
 * @since 0.0.0
 */
export const decodeClientCommand = Schema.decodeUnknownResult(Schema.fromJsonString(ClientCommand));

/**
 * Encodes a client frame.
 *
 * @category codecs
 * @since 0.0.0
 */
export const encodeClientCommand = Schema.encodeSync(Schema.fromJsonString(ClientCommand));

/**
 * Decodes an object frame.
 *
 * @category codecs
 * @since 0.0.0
 */
export const decodeServerEvent = Schema.decodeUnknownResult(Schema.fromJsonString(ServerEvent));

/**
 * Encodes an object frame.
 *
 * @category codecs
 * @since 0.0.0
 */
export const encodeServerEvent = Schema.encodeSync(Schema.fromJsonString(ServerEvent));

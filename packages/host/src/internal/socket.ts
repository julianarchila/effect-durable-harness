/**
 * The session socket (`Host.socket`): one hibernatable WebSocket per client, carrying its commands, the session's
 * conversation list and activity, presence, and a view per watched conversation (`SessionProtocol`).
 *
 * A socket's attachment holds its connection ID, the connection state `accept` returned (encoded), and the
 * conversations it watches with its presence in each, so a new instance (after hibernation) re-subscribes it. The
 * runtime hands each event a new `WebSocket` object for the same socket, so connections are keyed by the ID in the
 * attachment, never by the object. Commands of one connection run one at a time.
 */
import * as Wire from "@effect-durable-harness/delta/Wire";
import type { Harness, HarnessError } from "@effect-durable-harness/core/Harness";
import type { CommitRejected } from "@effect-durable-harness/core/SessionError";
import type {
  ConversationBusy,
  ConversationIdle,
  SubmissionError,
} from "@effect-durable-harness/core/Submission";
import type { TaskError } from "@effect-durable-harness/core/TaskError";
import { ConversationId, EntryId, SubmissionId } from "@effect-durable-harness/core/Ids";
import type { ConversationRecord } from "@effect-durable-harness/core/Records";
import type { Session } from "@effect-durable-harness/core/Session";
import * as Cause from "effect/Cause";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as FiberMap from "effect/FiberMap";
import * as FiberSet from "effect/FiberSet";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Random from "effect/Random";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import type * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import type * as Types from "effect/Types";
import { HttpServerRequest } from "effect/http/HttpServerRequest";
import * as HttpServerResponse from "effect/http/HttpServerResponse";
import type { Engine, EngineSocket } from "../Engine.ts";
import {
  type ActivityTask,
  type ClientCommand,
  type ConversationSummary,
  decodeClientCommand,
  encodeServerEvent,
  MAX_PRESENCE_CHARACTERS,
  MAX_WATCHED_CONVERSATIONS,
  type Peer,
  type ProtocolError,
  REFUSED_CLOSE_CODE,
  type ConversationWindow,
  type Request,
  type ServerEvent,
} from "../SessionProtocol.ts";
import { ConversationNotFound } from "./errors.ts";
import * as Receipts from "./receipts.ts";

// ─── Configuration ─────────────────────────────────────────────────────────

/** The session socket's configuration, as `Host.socket` builds it. */
export interface SocketConfig<C, R = never> {
  /** The connection state's schema; it encodes to the JSON kept in the socket's attachment. */
  readonly connection: Schema.Codec<C, Schema.Json>;
  /** Authorizes an upgrade: `None` refuses it (`REFUSED_CLOSE_CODE`). */
  readonly accept: (request: HttpServerRequest) => Effect.Effect<Option.Option<C>, never, R>;
  /** Rewrites a prompt before it is submitted. */
  readonly input: (connection: C, content: string) => string;
  /** What the other watchers see of a connection in `presence` events, from its encoded state. */
  readonly peer: (connection: Schema.Json) => Schema.Json;
  /** Checks and normalizes a presence value; `None` rejects it. Absent: presence is off. */
  readonly presence: ((value: Schema.Json) => Option.Option<Schema.Json>) | undefined;
  /** Whether the socket accepts commands that write (create, fork, submit, withdraw, abort). */
  readonly commands: boolean;
}

/**
 * One open connection of the session socket.
 *
 * @category models
 * @since 0.0.0
 */
export interface SocketConnection<C> {
  /** Stable for the socket's life, across hibernation. */
  readonly id: string;
  /** The state `accept` returned. */
  readonly connection: C;
  /** The conversations it watches. */
  readonly watching: ReadonlyArray<ConversationId>;
}

/**
 * Options of `SocketHandle.close`.
 *
 * @category models
 * @since 0.0.0
 */
export interface SocketCloseOptions {
  /** Default 1000. */
  readonly code?: number | undefined;
  readonly reason?: string | undefined;
}

/**
 * Picks connections of `SocketHandle.close` and `SocketHandle.send`.
 *
 * @category models
 * @since 0.0.0
 */
export type ConnectionPredicate<C> = (connection: C, info: SocketConnection<C>) => boolean;

/**
 * The session socket's server side.
 *
 * @category models
 * @since 0.0.0
 */
export interface SocketHandle<C> {
  /** The open connections. */
  readonly connections: Effect.Effect<ReadonlyArray<SocketConnection<C>>>;
  /** Closes the connections `predicate` picks, for example a member who lost access; answers how many. */
  readonly close: (
    predicate: ConnectionPredicate<C>,
    options?: SocketCloseOptions,
  ) => Effect.Effect<number>;
  /** Sends an application event to the connections `predicate` picks; answers how many. */
  readonly send: (predicate: ConnectionPredicate<C>, data: Schema.Json) => Effect.Effect<number>;
  /** Sends an application event to the watchers of a conversation. */
  readonly broadcast: (conversationId: ConversationId, data: Schema.Json) => Effect.Effect<void>;
}

/** The handle of an object without a socket. */
export const none: SocketHandle<Schema.Json> = {
  connections: Effect.succeed([]),
  close: () => Effect.succeed(0),
  send: () => Effect.succeed(0),
  broadcast: () => Effect.void,
};

/** `handle` over the raw service: connection states that no longer decode are skipped. */
export const typed =
  <C>(schema: Schema.Codec<C, Schema.Json>) =>
  (raw: SocketHandle<Schema.Json>): SocketHandle<C> => {
    const decode = Schema.decodeUnknownOption(schema);
    const pick =
      (predicate: ConnectionPredicate<C>) =>
      (connection: Schema.Json, info: SocketConnection<Schema.Json>) =>
        Option.match(decode(connection), {
          onNone: () => false,
          onSome: (value) => predicate(value, { ...info, connection: value }),
        });
    return {
      connections: Effect.map(raw.connections, (all) =>
        all.flatMap((info) =>
          Option.match(decode(info.connection), {
            onNone: () => [],
            onSome: (connection) => [{ ...info, connection }],
          }),
        ),
      ),
      close: (predicate, options) => raw.close(pick(predicate), options),
      send: (predicate, data) => raw.send(pick(predicate), data),
      broadcast: raw.broadcast,
    };
  };

// ─── Runtime ───────────────────────────────────────────────────────────────

const Attachment = Schema.Struct({
  socket: Schema.Literal("durable"),
  id: Schema.String,
  connection: Schema.Json,
  watching: Schema.Array(
    Schema.Struct({ conversationId: Schema.Number, presence: Schema.optionalKey(Schema.Json) }),
  ),
});
type Attachment = typeof Attachment.Type;

/** An open socket and what its attachment holds. */
interface Open {
  readonly socket: EngineSocket;
  readonly attachment: Attachment;
}

const ACTIVITY_DELAY = Duration.millis(250);
/** Conversations created within one refresh that are announced one by one; past it, the whole list is sent. */
const MAX_ANNOUNCED = 32;
/** Aborts of one connection waiting at once; past it, an abort is answered `busy`. */
const MAX_ABORTS = 4;
const encodeJson = Schema.encodeSync(Schema.fromJsonString(Schema.Json));
const ACTIVITY_RECHECK = Duration.seconds(1);

/** The key of one connection's subscription to one conversation. */
const followKey = (connectionId: string, conversationId: number) =>
  `${connectionId}:${conversationId}`;

const infoOf = (open: Open): SocketConnection<Schema.Json> => ({
  id: open.attachment.id,
  connection: open.attachment.connection,
  watching: open.attachment.watching.map((item) => ConversationId.make(item.conversationId)),
});

// A socket that closed under us is dropped by its close handler; a failed send changes nothing else.
const send = (socket: EngineSocket, event: ServerEvent) =>
  socket.send(encodeServerEvent(event)).pipe(Effect.exit, Effect.asVoid);

const summaryOf = (record: ConversationRecord): ConversationSummary => {
  const summary: Types.Mutable<ConversationSummary> = { id: record.id };
  if (record.parent !== undefined) {
    summary.parent = { conversationId: record.parent.conversationId, at: record.parent.at };
  }
  if (record.owner !== undefined) {
    summary.owner = { conversationId: record.owner.conversationId, taskId: record.owner.taskId };
  }
  return summary;
};

/** Failures a command answers with, mapped once. */
type CommandError =
  | Receipts.ReceiptError
  | ConversationNotFound
  | HarnessError
  | ConversationBusy
  | ConversationIdle
  | SubmissionError
  | CommitRejected
  | TaskError
  | ReadOnly
  | WithdrawRefused
  | AttachmentFull
  | TooWatched
  | TooManyAborts;

class ReadOnly extends Schema.TaggedError<ReadOnly>()("ReadOnly", {}) {
  override get message(): string {
    return "This socket only reads";
  }
}

// The runtime limits a socket's attachment to 2 KiB; what would not fit is refused, not a defect.
class AttachmentFull extends Schema.TaggedError<AttachmentFull>()("AttachmentFull", {}) {
  override get message(): string {
    return "The connection's attachment is full (2 KiB): watch fewer conversations or set a smaller presence";
  }
}

class TooWatched extends Schema.TaggedError<TooWatched>()("TooWatched", {}) {
  override get message(): string {
    return `A connection watches at most ${MAX_WATCHED_CONVERSATIONS} conversations`;
  }
}

class TooManyAborts extends Schema.TaggedError<TooManyAborts>()("TooManyAborts", {}) {
  override get message(): string {
    return `A connection waits on at most ${MAX_ABORTS} aborts at once`;
  }
}

class WithdrawRefused extends Schema.TaggedError<WithdrawRefused>()("WithdrawRefused", {
  status: Schema.String,
}) {
  override get message(): string {
    return `The submission is ${this.status}`;
  }
}

const protocolError = (error: CommandError): ProtocolError => {
  switch (error._tag) {
    case "ConversationNotFound":
      return { code: "not_found", message: error.message };
    case "ReadOnly":
      return { code: "read_only", message: error.message };
    case "RequestConflict":
    case "WithdrawRefused":
      return { code: "conflict", message: error.message };
    case "ConversationBusy":
    case "TooManyAborts":
      return { code: "busy", message: error.message };
    case "AttachmentFull":
    case "TooWatched":
    // A submission the kernel rejects; the socket's prompts never set `whenIdle`.
    case "ConversationIdle":
      return { code: "invalid", message: error.message };
    case "SubmissionError":
      return error.reason._tag === "SubmissionNotFound"
        ? { code: "not_found", message: error.message }
        : error.reason._tag === "RequestIdMismatch"
          ? { code: "conflict", message: error.message }
          : { code: "invalid", message: error.message };
    case "CommitRejected":
      return error.reason._tag === "NotFound"
        ? { code: "not_found", message: error.message }
        : error.reason._tag === "ForkConflict" || error.reason._tag === "SubmissionConflict"
          ? { code: "conflict", message: error.message }
          : { code: "invalid", message: error.message };
    default:
      return { code: "failed", message: error.message };
  }
};

/** What the object's handlers need from the socket. */
export interface SocketRuntime {
  readonly accept: Effect.Effect<HttpServerResponse.HttpServerResponse, never, HttpServerRequest>;
  readonly message: (
    socket: EngineSocket,
    message: string | ArrayBuffer | Uint8Array,
  ) => Effect.Effect<void>;
  /** Forgets a socket that closed or failed: its subscriptions end and its presence leaves. */
  readonly drop: (socket: EngineSocket) => Effect.Effect<void>;
  /** Re-subscribes the sockets an earlier instance accepted. */
  readonly restore: Effect.Effect<void>;
  readonly handle: SocketHandle<Schema.Json>;
}

/** Options of `make`. */
export interface SocketRuntimeOptions<C> {
  readonly config: SocketConfig<C>;
  readonly harness: Harness["Service"];
  readonly session: Session["Service"];
  readonly engine: Engine["Service"];
}

/** The session socket of this object, over its agent; in the instance's scope. */
export const make = <C>(
  options: SocketRuntimeOptions<C>,
): Effect.Effect<SocketRuntime, never, Scope.Scope> =>
  Effect.gen(function* () {
    const { config, harness, session, engine } = options;
    const follows = yield* FiberMap.make<string>();
    const background = yield* FiberSet.make();
    const serial = new Map<string, Semaphore.Semaphore>();
    const aborts = new Map<string, number>();
    const decodeConnection = Schema.decodeUnknownOption(config.connection);

    const attachmentOf = (socket: EngineSocket) =>
      socket.getAttachment(Attachment).pipe(Effect.option);
    const setAttachment = (socket: EngineSocket, value: Attachment) =>
      socket.setAttachment(Attachment, value);
    const opened: Effect.Effect<ReadonlyArray<Open>> = Effect.gen(function* () {
      const sockets = yield* engine.sockets;
      const result: Array<Open> = [];
      for (const socket of sockets) {
        const attachment = yield* attachmentOf(socket);
        if (Option.isSome(attachment)) result.push({ socket, attachment: attachment.value });
      }
      return result;
    });
    const broadcast = (event: ServerEvent) =>
      Effect.flatMap(opened, (all) =>
        Effect.forEach(all, (open) => send(open.socket, event), { discard: true }),
      );
    const conversations = Stream.runCollect(harness.conversations()).pipe(
      Effect.map(
        (records): ServerEvent => ({
          type: "session",
          conversations: Array.from(records, summaryOf),
        }),
      ),
      Effect.orDie,
    );

    const activity = Effect.gen(function* () {
      const inspection = yield* harness.inspect.pipe(Effect.orDie);
      const tasks = inspection.tasks.map((task): ActivityTask => {
        const record = task.record;
        const checkpoint =
          record.state.status === "terminal" || record.state.status === "completing"
            ? undefined
            : record.state.checkpoint;
        const phase = Schema.decodeUnknownOption(Schema.Struct({ phase: Schema.String }))(
          checkpoint,
        );
        const view: Types.Mutable<ActivityTask> = {
          id: record.id,
          kind: record.kind,
          conversationId: record.conversationId,
          status: task.state.kind,
          background: record.background,
          abortRequested: record.abortRequested,
        };
        if (record.owner !== undefined) view.owner = record.owner;
        if (Option.isSome(phase)) view.phase = phase.value.phase;
        if (task.state.kind === "waiting") view.waitingOn = [...task.state.on];
        return view;
      });
      return { type: "activity", tasks } satisfies ServerEvent;
    });

    // ─── Presence ────────────────────────────────────────────────────────

    /**
     * Sends each watcher of a conversation the presence of the others. `leaving` is a connection that is going away:
     * a closing socket can still be listed as open, so it is left out by its ID.
     */
    const broadcastPresence = (conversationId: number, leaving?: string) =>
      Effect.gen(function* () {
        if (config.presence === undefined) return;
        const watching = (yield* opened).flatMap((open) => {
          if (open.attachment.id === leaving) return [];
          const watch = open.attachment.watching.find(
            (item) => item.conversationId === conversationId,
          );
          return watch === undefined ? [] : [{ open, presence: watch.presence }];
        });
        for (const receiver of watching) {
          const peers = watching.flatMap(
            ({ open, presence }): ReadonlyArray<Peer> =>
              open.attachment.id === receiver.open.attachment.id || presence === undefined
                ? []
                : [
                    {
                      connectionId: open.attachment.id,
                      connection: config.peer(open.attachment.connection),
                      presence,
                    },
                  ],
          );
          yield* send(receiver.open.socket, { type: "presence", conversationId, peers });
        }
      });

    // ─── Watching a conversation ─────────────────────────────────────────

    /**
     * A snapshot of the conversation's view in `window`, then one op batch per frame (what accumulated while the
     * socket was busy arrives folded), until unwatched or closed.
     */
    const follow = (
      socket: EngineSocket,
      conversationId: number,
      window: ConversationWindow | undefined,
    ) =>
      Effect.scoped(
        Effect.gen(function* () {
          const conversation = yield* harness.conversation(ConversationId.make(conversationId));
          if (Option.isNone(conversation)) return;
          const watch = yield* conversation.value.watch(
            window === undefined
              ? undefined
              : {
                  ...window,
                  from: window.from === undefined ? undefined : EntryId.make(window.from),
                },
          );
          const encoder = Wire.makeEncoder();
          // SAFETY: a conversation view is JSON: records, entries and document values.
          const value = watch.value as Schema.JsonObject;
          yield* send(socket, {
            type: "view",
            conversationId,
            frame: { type: "snapshot", value },
          });
          yield* Stream.runForEach(watch.frames, (frame) =>
            send(socket, {
              type: "view",
              conversationId,
              frame: { type: "ops", ops: Wire.encode(encoder, frame.ops) },
            }),
          );
        }),
      ).pipe(
        // A socket that closed under it, or a Session that failed (the object aborts): the subscription ends.
        Effect.catchCause(() => Effect.void),
      );

    const subscribe = (
      socket: EngineSocket,
      connectionId: string,
      conversationId: number,
      window?: ConversationWindow | undefined,
    ) =>
      FiberMap.run(
        follows,
        followKey(connectionId, conversationId),
        follow(socket, conversationId, window),
      ).pipe(Effect.asVoid);

    /**
     * Updates a connection's attachment from its current value. A value over the runtime's 2 KiB is refused
     * (`AttachmentFull`) and the attachment stays as it was.
     */
    const updateAttachment = (socket: EngineSocket, change: (current: Attachment) => Attachment) =>
      Effect.gen(function* () {
        const current = yield* attachmentOf(socket);
        if (Option.isNone(current)) return Option.none<Attachment>();
        const next = change(current.value);
        yield* setAttachment(socket, next).pipe(Effect.mapError(() => new AttachmentFull()));
        return Option.some(next);
      });

    const watch = (
      socket: EngineSocket,
      connectionId: string,
      conversationId: number,
      window: ConversationWindow | undefined,
    ) =>
      Effect.gen(function* () {
        const current = yield* attachmentOf(socket);
        if (
          Option.isSome(current) &&
          !current.value.watching.some((item) => item.conversationId === conversationId) &&
          current.value.watching.length >= MAX_WATCHED_CONVERSATIONS
        ) {
          return yield* new TooWatched();
        }
        yield* updateAttachment(socket, (attachment) =>
          attachment.watching.some((item) => item.conversationId === conversationId)
            ? attachment
            : { ...attachment, watching: [...attachment.watching, { conversationId }] },
        );
        yield* subscribe(socket, connectionId, conversationId, window);
        yield* broadcastPresence(conversationId);
      });

    const unwatch = (socket: EngineSocket, connectionId: string, conversationId: number) =>
      Effect.gen(function* () {
        // A smaller attachment always fits.
        yield* updateAttachment(socket, (current) => ({
          ...current,
          watching: current.watching.filter((item) => item.conversationId !== conversationId),
        })).pipe(Effect.orDie);
        yield* FiberMap.remove(follows, followKey(connectionId, conversationId));
        yield* broadcastPresence(conversationId);
      });

    const setPresence = (
      socket: EngineSocket,
      conversationId: number,
      presence: Schema.Json | null,
    ) =>
      Effect.gen(function* () {
        if (config.presence === undefined) return;
        const checked = presence === null ? Option.none() : config.presence(presence);
        if (presence !== null && Option.isNone(checked)) {
          return yield* send(socket, {
            type: "error",
            error: { code: "invalid_message", message: "Invalid presence" },
          });
        }
        if (Option.isSome(checked) && encodeJson(checked.value).length > MAX_PRESENCE_CHARACTERS) {
          return yield* send(socket, {
            type: "error",
            error: {
              code: "invalid_message",
              message: `A presence is at most ${MAX_PRESENCE_CHARACTERS} characters of JSON`,
            },
          });
        }
        const updated = yield* updateAttachment(socket, (current) => ({
          ...current,
          watching: current.watching.map((item) => {
            if (item.conversationId !== conversationId) return item;
            return Option.isSome(checked)
              ? { conversationId, presence: checked.value }
              : { conversationId };
          }),
        })).pipe(
          Effect.catchTag("AttachmentFull", (error) =>
            send(socket, {
              type: "error",
              error: { code: "invalid_message", message: error.message },
            }).pipe(Effect.as(Option.none<Attachment>())),
          ),
        );
        if (Option.isSome(updated)) yield* broadcastPresence(conversationId);
      });

    // ─── Commands ────────────────────────────────────────────────────────

    const requireConversation = (id: number) =>
      Effect.flatMap(harness.conversation(ConversationId.make(id)), (found) =>
        Option.match(found, {
          onNone: () =>
            Effect.fail(new ConversationNotFound({ conversationId: ConversationId.make(id) })),
          onSome: Effect.succeed,
        }),
      );

    const writes = config.commands ? Effect.void : Effect.fail(new ReadOnly());

    /** Runs one request and answers it. */
    const answer = <E extends CommandError>(
      socket: EngineSocket,
      request: Request,
      run: Effect.Effect<
        {
          readonly conversationId?: number;
          readonly submissionId?: number;
          readonly entries?: ReadonlyArray<Schema.JsonObject>;
          readonly more?: boolean;
        },
        E
      >,
    ) =>
      run.pipe(
        Effect.matchEffect({
          onSuccess: (ids) =>
            send(socket, { type: "result", requestId: request.requestId, ok: true, ...ids }),
          onFailure: (error) =>
            send(socket, {
              type: "result",
              requestId: request.requestId,
              ok: false,
              error: protocolError(error),
            }),
        }),
      );

    const handle = (socket: EngineSocket, attachment: Attachment, command: ClientCommand) => {
      const id = attachment.id;
      switch (command.type) {
        case "presence":
          return attachment.watching.some((item) => item.conversationId === command.conversationId)
            ? setPresence(socket, command.conversationId, command.presence)
            : Effect.void;
        case "unwatch":
          return unwatch(socket, id, command.conversationId);
        case "watch":
          return answer(
            socket,
            command,
            Effect.gen(function* () {
              yield* requireConversation(command.conversationId);
              yield* watch(socket, id, command.conversationId, command.window);
              return { conversationId: command.conversationId };
            }),
          );
        case "entries":
          return answer(
            socket,
            command,
            Effect.gen(function* () {
              const conversation = yield* requireConversation(command.conversationId);
              // One more than the page tells whether the range goes on (a `next` cursor).
              const page = yield* Stream.runCollect(
                conversation
                  .entries({
                    minEntryId:
                      command.minEntryId === undefined
                        ? undefined
                        : EntryId.make(command.minEntryId),
                    maxEntryId:
                      command.maxEntryId === undefined
                        ? undefined
                        : EntryId.make(command.maxEntryId),
                    order: command.order,
                  })
                  .pipe(Stream.take(command.limit + 1)),
              );
              return {
                conversationId: command.conversationId,
                // SAFETY: entry records are JSON.
                entries: page.slice(0, command.limit) as ReadonlyArray<Schema.JsonObject>,
                more: page.length > command.limit,
              };
            }),
          );
        case "create":
          return answer(
            socket,
            command,
            Effect.gen(function* () {
              yield* writes;
              const created = yield* Receipts.ensure(harness, command.requestId, undefined);
              return { conversationId: created.id };
            }),
          );
        case "fork":
          return answer(
            socket,
            command,
            Effect.gen(function* () {
              yield* writes;
              const created = yield* Receipts.ensure(harness, command.requestId, {
                conversationId: ConversationId.make(command.conversationId),
                entryId: EntryId.make(command.entryId),
              });
              return { conversationId: created.id };
            }),
          );
        case "submit":
          return answer(
            socket,
            command,
            Effect.gen(function* () {
              yield* writes;
              const conversation = yield* requireConversation(command.conversationId);
              const connection = decodeConnection(attachment.connection);
              const content = Option.match(connection, {
                onNone: () => command.content,
                onSome: (value) => config.input(value, command.content),
              });
              // A resent command (same request ID) answers the existing submission. While the agent works, the
              // input waits in the conversation's durable inbox.
              const submission = yield* conversation.submit({
                type: "input",
                content,
                requestId: command.requestId,
                whenBusy: command.whenBusy ?? "followUp",
              });
              return { conversationId: conversation.id, submissionId: submission.id };
            }),
          );
        case "withdraw":
          return answer(
            socket,
            command,
            Effect.gen(function* () {
              yield* writes;
              const withdrawn = yield* harness.withdrawSubmission(
                SubmissionId.make(command.submissionId),
                ConversationId.make(command.conversationId),
              );
              if (withdrawn._tag === "NotFound") {
                return yield* new ConversationNotFound({
                  conversationId: ConversationId.make(command.conversationId),
                });
              }
              const ids = {
                conversationId: command.conversationId,
                submissionId: command.submissionId,
              };
              if (withdrawn._tag === "Withdrawn") return ids;
              if (withdrawn._tag === "AlreadyPlaced") {
                return yield* new WithdrawRefused({ status: "placed" });
              }
              // A resent withdraw (its first answer was lost) finds the submission it withdrew settled: a queued
              // submission withdrawn, or dropped from the inbox by an abort, is `unanswered` with the reason
              // `aborted` and no entry. That is the state a withdraw leaves, so it answers ok; the kernel keeps no
              // record of which request withdrew it.
              const settled = yield* harness.submission(SubmissionId.make(command.submissionId));
              const record = Option.isSome(settled) ? yield* settled.value.status : undefined;
              if (
                record?.status === "unanswered" &&
                record.reason === "aborted" &&
                !("entry" in record)
              ) {
                return ids;
              }
              return yield* new WithdrawRefused({ status: "settled" });
            }),
          );
        case "abort": {
          // An abort waits until the conversation is idle: it runs on its own, so the connection's next commands
          // are not held behind it. A connection waits on a few at most.
          const waiting = aborts.get(id) ?? 0;
          if (waiting >= MAX_ABORTS)
            return answer(socket, command, Effect.fail(new TooManyAborts()));
          aborts.set(id, waiting + 1);
          return FiberSet.run(
            background,
            answer(
              socket,
              command,
              Effect.gen(function* () {
                yield* writes;
                const conversation = yield* requireConversation(command.conversationId);
                yield* conversation.abort();
                return { conversationId: command.conversationId };
              }),
            ).pipe(
              Effect.ensuring(
                Effect.sync(() => {
                  const left = (aborts.get(id) ?? 1) - 1;
                  if (left > 0) aborts.set(id, left);
                  else aborts.delete(id);
                }),
              ),
            ),
          ).pipe(Effect.asVoid);
        }
      }
    };

    const lockOf = (id: string) => {
      const existing = serial.get(id);
      if (existing !== undefined) return existing;
      const created = Semaphore.makeUnsafe(1);
      serial.set(id, created);
      return created;
    };

    const drop = (socket: EngineSocket) =>
      Effect.gen(function* () {
        const attachment = yield* attachmentOf(socket);
        if (Option.isNone(attachment)) return;
        for (const item of attachment.value.watching) {
          yield* FiberMap.remove(follows, followKey(attachment.value.id, item.conversationId));
        }
        serial.delete(attachment.value.id);
        // The others see it gone, even while the closing socket is still listed as open.
        for (const item of attachment.value.watching) {
          if (item.presence !== undefined) {
            yield* broadcastPresence(item.conversationId, attachment.value.id);
          }
        }
      });

    // ─── Session-wide updates ────────────────────────────────────────────

    // A created conversation is announced; task changes refresh the activity, at most every 250 ms, and only while
    // some client is connected. What is pending is a flag and a bounded set of conversations, and `wake` holds one
    // mark, so nothing grows while the loop is busy.
    const wake = yield* Queue.sliding<void>(1);
    let activityPending = false;
    let listPending = false;
    const created = new Map<number, ConversationSummary>();
    // Detached with the socket's scope; it ends by itself when the Session closes.
    yield* Effect.acquireRelease(
      session.observeCommits({
        end: () => {},
        advance: (publication) => {
          let changed = false;
          for (const change of publication.changes) {
            if (change.type === "task") {
              activityPending = true;
              changed = true;
            }
            if (change.type === "conversation") {
              // Past the bound, the whole list replaces the announcements.
              if (created.size < MAX_ANNOUNCED)
                created.set(change.value.id, summaryOf(change.value));
              else listPending = true;
              changed = true;
            }
          }
          if (changed) Queue.offerUnsafe(wake, undefined);
        },
      }),
      (observation) => Effect.sync(observation.detach),
    ).pipe(Effect.orDie);
    const announce = Effect.gen(function* () {
      if (listPending) {
        listPending = false;
        created.clear();
        return yield* broadcast(yield* conversations);
      }
      const items = Array.from(created.values());
      created.clear();
      for (const item of items) yield* broadcast({ type: "conversation", conversation: item });
    });
    // A task's last commit can publish before its fiber settles, so a refresh can still list it: while any task is
    // listed as running, the activity is refreshed again after a quiet second. A sleeping task polls nothing.
    let listed = false;
    yield* Effect.forkScoped(
      Effect.forever(
        Effect.gen(function* () {
          const woke = listed
            ? Option.isSome(yield* Queue.take(wake).pipe(Effect.timeoutOption(ACTIVITY_RECHECK)))
            : yield* Queue.take(wake).pipe(Effect.as(true));
          if (!woke) activityPending = true;
          yield* announce;
          if (!activityPending) return;
          yield* Effect.sleep(ACTIVITY_DELAY);
          // Later marks are folded into this refresh.
          yield* Queue.clear(wake);
          activityPending = false;
          yield* announce;
          if ((yield* opened).length === 0) {
            listed = false;
            return;
          }
          const refreshed = yield* activity;
          listed = refreshed.tasks.some((task) => task.status === "running");
          yield* broadcast(refreshed);
        }).pipe(
          // One failed refresh (a failed read, a Session that is failing) is logged; the loop goes on.
          Effect.catchCause((cause) =>
            Cause.hasInterruptsOnly(cause)
              ? Effect.failCause(cause)
              : Effect.logWarning("durable host: session update failed", cause),
          ),
        ),
      ),
    );

    // ─── Server side ─────────────────────────────────────────────────────

    const picked = (predicate: ConnectionPredicate<Schema.Json>) =>
      Effect.map(opened, (all) =>
        all.filter((open) => predicate(open.attachment.connection, infoOf(open))),
      );

    const server: SocketHandle<Schema.Json> = {
      connections: Effect.map(opened, (all) => all.map(infoOf)),
      close: Effect.fn("Host.Socket.close")(function* (predicate, closeOptions) {
        const targets = yield* picked(predicate);
        for (const target of targets) {
          // `drop` leaves the target out of the presence it sends the others.
          yield* drop(target.socket);
          yield* Effect.exit(
            target.socket.close(closeOptions?.code ?? 1000, closeOptions?.reason ?? ""),
          );
        }
        return targets.length;
      }),
      send: Effect.fn("Host.Socket.send")(function* (predicate, data) {
        const targets = yield* picked(predicate);
        yield* Effect.forEach(targets, (target) => send(target.socket, { type: "event", data }), {
          discard: true,
        });
        return targets.length;
      }),
      broadcast: Effect.fn("Host.Socket.broadcast")(function* (conversationId, data) {
        const all = yield* opened;
        for (const open of all) {
          if (open.attachment.watching.some((item) => item.conversationId === conversationId)) {
            yield* send(open.socket, { type: "event", conversationId, data });
          }
        }
      }),
    };

    return {
      accept: Effect.gen(function* () {
        const request = yield* HttpServerRequest;
        if (request.headers["upgrade"]?.toLowerCase() !== "websocket") {
          return HttpServerResponse.text("WebSocket upgrade required", { status: 426 });
        }
        const accepted = yield* config.accept(request);
        if (Option.isNone(accepted)) {
          // A browser sees a failed upgrade only as a closed socket, with no status: the upgrade is accepted and
          // closed at once with a code the client reads as refused, and stops reconnecting. The socket has no
          // attachment, so the session ignores it.
          const [refusal, refused] = yield* engine.upgrade;
          yield* refused.close(REFUSED_CLOSE_CODE, "refused");
          return refusal;
        }
        const connection = yield* Schema.encodeEffect(config.connection)(accepted.value);
        const id = `${yield* Random.nextIntBetween(0, 2 ** 31 - 1)}-${yield* Random.nextIntBetween(0, 2 ** 31 - 1)}`;
        const [response, socket] = yield* engine.upgrade;
        // The attachment is limited to 2 KiB: a connection state over it fails here and refuses the upgrade.
        yield* setAttachment(socket, { socket: "durable", id, connection, watching: [] });
        yield* send(socket, { type: "hello", connectionId: id, connection });
        yield* send(socket, yield* conversations);
        yield* send(socket, yield* activity);
        return response;
      }).pipe(
        Effect.withSpan("durable.host.socket.accept"),
        Effect.catchCause((cause) =>
          Effect.logWarning("durable host: socket refused", cause).pipe(
            Effect.as(HttpServerResponse.empty({ status: 503 })),
          ),
        ),
      ),
      message: (socket, message) =>
        Effect.gen(function* () {
          const attachment = yield* attachmentOf(socket);
          // Not a session socket: a view socket (read-only) or one the application accepted itself.
          if (Option.isNone(attachment)) return;
          if (message instanceof ArrayBuffer || message instanceof Uint8Array) {
            yield* send(socket, {
              type: "error",
              error: { code: "invalid_message", message: "Binary frames are not supported" },
            });
            return yield* socket.close(1003, "Binary frame");
          }
          const decoded = decodeClientCommand(message);
          if (Result.isFailure(decoded)) {
            yield* send(socket, {
              type: "error",
              error: { code: "invalid_message", message: decoded.failure.message },
            });
            return yield* socket.close(1002, "Invalid command frame");
          }
          const id = attachment.value.id;
          yield* lockOf(id).withPermits(1)(
            // The attachment is read again under the lock: the previous command may have changed it.
            Effect.flatMap(attachmentOf(socket), (current) =>
              Option.match(current, {
                onNone: () => Effect.void,
                onSome: (value) => handle(socket, value, decoded.success),
              }),
            ),
          );
        }).pipe(
          Effect.withSpan("durable.host.socket.message"),
          Effect.catchCause((cause) => Effect.logWarning("durable host: command failed", cause)),
        ),
      drop,
      restore: Effect.gen(function* () {
        const all = yield* opened;
        const presence = new Set<number>();
        for (const open of all) {
          for (const item of open.attachment.watching) {
            yield* subscribe(open.socket, open.attachment.id, item.conversationId);
            if (item.presence !== undefined) presence.add(item.conversationId);
          }
        }
        for (const conversationId of presence) yield* broadcastPresence(conversationId);
      }),
      handle: server,
    } satisfies SocketRuntime;
  });

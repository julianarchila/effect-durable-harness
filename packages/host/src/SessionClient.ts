/**
 * A browser client of the session socket (`Host.socket`, `SessionProtocol`), outside any framework.
 *
 * - It reconnects with backoff, and on every reconnect watches its conversations again (each answers with a fresh
 *   snapshot), restores its presence, and resends every command not yet answered under the same `requestId`: the
 *   object answers a resent submit with its first submission and a resent create or fork with its first
 *   conversation, so nothing happens twice.
 * - A view holds a window over the conversation's entries. The client keeps a `ConversationTranscript`
 *   per watched conversation: what left the window stays, `loadEarlier` and `fillGap` page the rest in with `entries`
 *   requests, and a rewatch resumes right after the newest entry it has (`ConversationTranscript.resume`), so a
 *   short drop returns only what was missed.
 * - It keeps a `View.Reader` per watched conversation; a batch that does not apply means the copy drifted, and the
 *   client reconnects for a fresh snapshot. After a (re)watch, batches are ignored until the snapshot that answers
 *   it: the ones still in flight belong to the previous watch.
 * - A connection the object refuses (`REFUSED_CLOSE_CODE`) ends the client: its status is `refused` and it does not
 *   reconnect.
 * - State is exposed as `Subscribable`s (`get` and `subscribe`), the shape `useSyncExternalStore` takes.
 * - Commands are Effects that send when run, as any Effect: `session.submit(...)` alone sends nothing. A UI handler
 *   runs one with `SessionClient.run` (or `Effect.runFork`); each run is a new command with a new `requestId`.
 *
 * It bundles no kernel runtime: only `effect` (for `Schema` and the commands' results), `@effect-durable-harness/delta` and
 * `@effect-durable-harness/core/ConversationTranscript`, which imports no kernel module at runtime.
 *
 * ```ts
 * const session = SessionClient.make({ url: `/api/sessions/${id}`, params: { username } });
 * const conversation = session.conversation(ROOT_CONVERSATION_ID); // watched while subscribed
 * const unsubscribe = conversation.subscribe(() => render(conversation.get()));
 * SessionClient.run(session.submit(ROOT_CONVERSATION_ID, "hello", { whenBusy: "steer" }));
 * ```
 *
 * @since 0.0.0
 */
import type { Cursor } from "@effect-durable-harness/core/AgentStorage";
import * as ConversationTranscript from "@effect-durable-harness/core/ConversationTranscript";
import type { EntryRecord } from "@effect-durable-harness/core/Records";
import type { ConversationView } from "@effect-durable-harness/core/View";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import type * as Types from "effect/Types";
import {
  type ActivityTask,
  type ClientCommand,
  type ConversationSummary,
  type ConversationWindow,
  decodeServerEvent,
  encodeClientCommand,
  ErrorCode,
  type Peer,
  REFUSED_CLOSE_CODE,
  type ProtocolError,
  type Request,
  type Result as CommandResult,
  type ServerEvent,
  type WhenBusy,
} from "./SessionProtocol.ts";
import * as View from "./View.ts";

/**
 * A value that changes, in the shape `useSyncExternalStore` takes: `get` answers the same object until it changes.
 *
 * @category models
 * @since 0.0.0
 */
export interface Subscribable<A> {
  readonly get: () => A;
  /** Calls `listener` after every change; answers the unsubscribe. */
  readonly subscribe: (listener: () => void) => () => void;
}

/**
 * The session as the client sees it.
 *
 * @category models
 * @since 0.0.0
 */
export interface SessionState {
  /** `refused` when the object refused the connection (`REFUSED_CLOSE_CODE`): the client is closed for good. */
  readonly status: "connecting" | "open" | "reconnecting" | "closed" | "refused";
  /** This connection's ID and state, from the object's `hello`. */
  readonly connectionId: string | undefined;
  readonly connection: Schema.Json | undefined;
  readonly conversations: ReadonlyArray<ConversationSummary>;
  readonly activity: ReadonlyArray<ActivityTask>;
  /** The last error the object reported, or the last command that failed. */
  readonly error: ProtocolError | undefined;
}

/**
 * One prompt sent and not yet answered.
 *
 * @category models
 * @since 0.0.0
 */
export interface Sending {
  readonly requestId: string;
  readonly content: string;
}

/**
 * A watched conversation.
 *
 * @category models
 * @since 0.0.0
 */
export interface WatchedConversation {
  /** `loading` until the first snapshot; `missing` when the object has no such conversation. */
  readonly status: "loading" | "ready" | "missing";
  /** The latest value of the view, with its window of entries. */
  readonly view: ConversationView | null;
  /** The client's whole transcript (`ConversationTranscript`): every entry it holds, gaps and `earlier`. */
  readonly transcript: ConversationTranscript.TranscriptView | null;
  /** The other connections watching it, with their presence. */
  readonly peers: ReadonlyArray<Peer>;
  /** Its prompts sent and not yet answered. */
  readonly sending: ReadonlyArray<Sending>;
}

/**
 * A command the object refused, or that could not be sent because the client closed.
 *
 * @category errors
 * @since 0.0.0
 */
export class CommandFailed extends Schema.TaggedError<CommandFailed>()("CommandFailed", {
  code: Schema.Union([ErrorCode, Schema.Literal("closed")]),
  message: Schema.String,
}) {}

/**
 * What `ClientSocket` hands its listeners: a `message` carries `data`, a `close` its `code`.
 *
 * @category models
 * @since 0.0.0
 */
export interface SocketEvent {
  readonly data?: string | ArrayBuffer;
  readonly code?: number;
}

/**
 * The socket the client drives: the browser's `WebSocket`, or one a test provides.
 *
 * @category models
 * @since 0.0.0
 */
export interface ClientSocket {
  readonly readyState: number;
  addEventListener(
    type: "open" | "close" | "message",
    listener: (event: SocketEvent) => void,
  ): void;
  send(data: string): void;
  close(code?: number, reason?: string): void;
}

/**
 * Options of `make`.
 *
 * @category models
 * @since 0.0.0
 */
export interface SessionClientOptions {
  /** The session's socket URL: absolute, or relative to the page; `http(s)` becomes `ws(s)`. */
  readonly url: string;
  /** Query parameters of the upgrade, such as what the object's `accept` reads. */
  readonly params?: ReadonlyArray<readonly [name: string, value: string]> | undefined;
  /** Opens a socket; default the global `WebSocket`. */
  readonly connect?: ((url: string) => ClientSocket) | undefined;
  /** First reconnect delay, doubled per attempt up to `maxBackoffMs`; default 500 ms. */
  readonly backoffMs?: number | undefined;
  /** Default 10 s. */
  readonly maxBackoffMs?: number | undefined;
}

/**
 * Options of `SessionClient.submit`.
 *
 * @category models
 * @since 0.0.0
 */
export interface SubmitOptions {
  readonly whenBusy?: WhenBusy | undefined;
}

/**
 * An application event (`Host.Instance.socket.send` or `broadcast`).
 *
 * @category models
 * @since 0.0.0
 */
export interface AppEvent {
  readonly conversationId: number | undefined;
  readonly data: Schema.Json;
}

/**
 * The client of one session's socket.
 *
 * @category models
 * @since 0.0.0
 */
export interface SessionClient {
  readonly state: Subscribable<SessionState>;
  /**
   * A conversation, watched while it has subscribers. Handles of the same ID share one state, which the client
   * forgets once nothing subscribes to it.
   */
  readonly conversation: (conversationId: number) => Subscribable<WatchedConversation>;
  /** Calls `listener` for each application event; answers the unsubscribe. */
  readonly events: (listener: (event: AppEvent) => void) => () => void;
  /**
   * Each command is an Effect that sends when run (or as soon as the socket opens), is resent after a reconnect
   * until answered, and waits for the answer. Nothing is sent until it runs; each run is a new command with its own
   * `requestId`. Interrupting a run stops the wait, not the command. Run it from a UI handler with `run`.
   */
  readonly create: () => Effect.Effect<number, CommandFailed>;
  /** Forks `conversationId` through the visible entry `entryId`; answers the new conversation's ID. */
  readonly fork: (conversationId: number, entryId: number) => Effect.Effect<number, CommandFailed>;
  /** Submits a prompt; answers the submission's ID. */
  readonly submit: (
    conversationId: number,
    content: string,
    options?: SubmitOptions,
  ) => Effect.Effect<number, CommandFailed>;
  /** Withdraws a queued submission. */
  readonly withdraw: (
    conversationId: number,
    submissionId: number,
  ) => Effect.Effect<void, CommandFailed>;
  /** Aborts a conversation's work; answers once it is idle. */
  readonly abort: (conversationId: number) => Effect.Effect<void, CommandFailed>;
  /**
   * Loads the page of entries before the oldest one a watched conversation holds (its transcript's `earlier`
   * item). Concurrent calls share one load.
   */
  readonly loadEarlier: (conversationId: number) => Effect.Effect<void, CommandFailed>;
  /** Loads the next page of a gap of a watched conversation's transcript. */
  readonly fillGap: (
    conversationId: number,
    gap: ConversationTranscript.TranscriptGap,
  ) => Effect.Effect<void, CommandFailed>;
  /** Sets this connection's presence in a watched conversation; `null` clears it. Restored after a reconnect. */
  readonly presence: (conversationId: number, value: Schema.Json | null) => void;
  /** Closes the socket for good; unanswered commands fail with `closed`. */
  readonly close: () => void;
}

const OPEN = 1;

/** A command waiting for its answer. */
interface Pending {
  readonly command: Request;
  result: CommandResult | undefined;
  readonly waiters: Set<(result: CommandResult) => void>;
}

/** A conversation's store and what the client keeps for it. */
interface Watch {
  readonly store: Store<WatchedConversation>;
  listeners: number;
  /** Whether the object was asked to watch it and not to unwatch it since. */
  active: boolean;
  /** Set by a (re)watch until its snapshot arrives: batches before it belong to the previous watch. */
  awaitingSnapshot: boolean;
  reader: View.Reader;
  readonly transcript: ConversationTranscript.ConversationTranscript<CommandFailed>;
  presence: Schema.Json | null;
}

interface Store<A> extends Subscribable<A> {
  readonly set: (next: A) => void;
}

const makeStore = <A>(initial: A): Store<A> => {
  let value = initial;
  const listeners = new Set<() => void>();
  return {
    get: () => value,
    subscribe: (listener) => {
      listeners.add(listener);
      return () => listeners.delete(listener);
    },
    set: (next) => {
      value = next;
      for (const listener of listeners) listener();
    },
  };
};

const loading: WatchedConversation = {
  status: "loading",
  view: null,
  transcript: null,
  peers: [],
  sending: [],
};

// Only whether a page has a next cursor matters to a transcript, never its value.
const MORE = Option.some<Cursor>({ more: true });

const socketUrl = (options: SessionClientOptions): string => {
  // SAFETY: a browser page has a `location`; elsewhere (a test, a Worker) the URL must be absolute.
  const page = (globalThis as { readonly location?: { readonly href: string } }).location;
  const base = page?.href ?? "http://localhost/";
  const url = new URL(options.url, base);
  if (url.protocol === "http:") url.protocol = "ws:";
  if (url.protocol === "https:") url.protocol = "wss:";
  for (const [name, value] of options.params ?? []) url.searchParams.set(name, value);
  return url.toString();
};

const defaultConnect = (url: string): ClientSocket =>
  // SAFETY: the browser's `WebSocket` has every member `ClientSocket` names, with wider listener types.
  new globalThis.WebSocket(url) as ClientSocket;

/**
 * Connects to a session's socket.
 *
 * @category constructors
 * @since 0.0.0
 */
export const make = (options: SessionClientOptions): SessionClient => {
  const url = socketUrl(options);
  const connect = options.connect ?? defaultConnect;
  const backoffMs = options.backoffMs ?? 500;
  const maxBackoffMs = options.maxBackoffMs ?? 10_000;
  const state = makeStore<SessionState>({
    status: "connecting",
    connectionId: undefined,
    connection: undefined,
    conversations: [],
    activity: [],
    error: undefined,
  });
  const outbox = new Map<string, Pending>();
  const watches = new Map<number, Watch>();
  const eventListeners = new Set<(event: AppEvent) => void>();
  let socket: ClientSocket | undefined;
  let attempts = 0;
  let closed = false;
  let retry: ReturnType<typeof globalThis.setTimeout> | undefined;
  // @effect-diagnostics-next-line cryptoRandomUUID:off
  const prefix = globalThis.crypto.randomUUID();
  let requests = 0;
  const nextRequestId = () => `${prefix}-${requests++}`;

  const update = (change: Partial<SessionState>) => state.set({ ...state.get(), ...change });

  const write = (command: ClientCommand) => {
    if (socket?.readyState === OPEN) socket.send(encodeClientCommand(command));
  };

  // ─── Conversations ───────────────────────────────────────────────────

  const sendingOf = (conversationId: number): ReadonlyArray<Sending> =>
    Array.from(outbox.values()).flatMap((pending) =>
      pending.command.type === "submit" && pending.command.conversationId === conversationId
        ? [{ requestId: pending.command.requestId, content: pending.command.content }]
        : [],
    );

  const watchOf = (conversationId: number): Watch => {
    const existing = watches.get(conversationId);
    if (existing !== undefined) return existing;
    const created: Watch = {
      store: makeStore(loading),
      listeners: 0,
      active: false,
      awaitingSnapshot: true,
      reader: View.makeReader(),
      transcript: ConversationTranscript.make((query, limit) =>
        Effect.map(
          command(() => {
            const request: Types.Mutable<Extract<Request, { readonly type: "entries" }>> = {
              type: "entries",
              requestId: nextRequestId(),
              conversationId,
              limit,
            };
            if (query.minEntryId !== undefined) request.minEntryId = query.minEntryId;
            if (query.maxEntryId !== undefined) request.maxEntryId = query.maxEntryId;
            if (query.order !== undefined) request.order = query.order;
            return request;
          }),
          (result) => ({
            // SAFETY: the object answers `entries` with entry records, which the protocol carries as JSON.
            items: (result.entries ?? []) as ReadonlyArray<EntryRecord>,
            next: result.more === true ? MORE : Option.none(),
          }),
        ),
      ),
      presence: null,
    };
    watches.set(conversationId, created);
    return created;
  };

  const patch = (conversationId: number, change: Partial<WatchedConversation>) => {
    const watch = watches.get(conversationId);
    if (watch !== undefined) watch.store.set({ ...watch.store.get(), ...change });
  };

  /** Asks the object to watch from `window`; the answer is a snapshot, or `not_found`. */
  const requestWatch = (conversationId: number, window: ConversationWindow | undefined) => {
    const requestId = nextRequestId();
    const pending: Pending = {
      command:
        window === undefined
          ? { type: "watch", requestId, conversationId }
          : { type: "watch", requestId, conversationId, window },
      result: undefined,
      waiters: new Set([
        (result) => {
          if (!result.ok)
            patch(conversationId, { status: "missing", view: null, transcript: null });
        },
      ]),
    };
    outbox.set(requestId, pending);
    write(pending.command);
  };

  /**
   * Resets a watch's copy and asks the object for a fresh snapshot, from right after the newest entry the
   * transcript holds: a window that fits what was missed continues it, and a longer one opens a gap.
   */
  const rewatch = (conversationId: number, watch: Watch) => {
    watch.active = true;
    watch.awaitingSnapshot = true;
    watch.reader = View.makeReader();
    const resume = watch.transcript.resume();
    requestWatch(conversationId, resume?.from === undefined ? undefined : { from: resume.from });
  };

  /** Runs a transcript load of a watched conversation and shows what it added. */
  const load = (
    conversationId: number,
    run: (
      transcript: ConversationTranscript.ConversationTranscript<CommandFailed>,
    ) => Effect.Effect<ConversationTranscript.TranscriptView, CommandFailed>,
  ) =>
    Effect.suspend(() => {
      const watch = watches.get(conversationId);
      if (watch === undefined || watch.store.get().transcript === null) return Effect.void;
      return run(watch.transcript).pipe(
        Effect.map((transcript) => {
          if (watches.get(conversationId) === watch) patch(conversationId, { transcript });
        }),
      );
    });

  const conversation = (conversationId: number): Subscribable<WatchedConversation> => ({
    get: () => watches.get(conversationId)?.store.get() ?? loading,
    subscribe: (listener) => {
      const watch = watchOf(conversationId);
      const unsubscribe = watch.store.subscribe(listener);
      watch.listeners += 1;
      // A watch still active (a remount: unsubscribed and subscribed again in one tick) keeps its copy.
      if (watch.listeners === 1 && !watch.active) {
        watch.store.set({ ...loading, sending: sendingOf(conversationId) });
        rewatch(conversationId, watch);
      }
      return () => {
        unsubscribe();
        watch.listeners -= 1;
        // Deferred, so an unsubscribe followed at once by a subscribe (a remount) does not unwatch.
        globalThis.queueMicrotask(() => {
          if (watch.listeners > 0 || closed || !watch.active) return;
          watch.active = false;
          write({ type: "unwatch", conversationId });
          if (watches.get(conversationId) === watch) watches.delete(conversationId);
        });
      };
    },
  });

  // ─── Commands ────────────────────────────────────────────────────────

  const enqueue = (
    command: Request,
  ): Effect.Effect<CommandResult & { readonly ok: true }, CommandFailed> => {
    const pending: Pending = { command, result: undefined, waiters: new Set() };
    if (closed) {
      pending.result = {
        type: "result",
        requestId: command.requestId,
        ok: false,
        error: { code: "failed", message: "The session client is closed" },
      };
    } else {
      outbox.set(command.requestId, pending);
      write(command);
      if (command.type === "submit")
        patch(command.conversationId, { sending: sendingOf(command.conversationId) });
    }
    return Effect.callback<CommandResult & { readonly ok: true }, CommandFailed>((resume) => {
      const settle = (result: CommandResult) =>
        resume(
          result.ok
            ? Effect.succeed(result)
            : Effect.fail(
                new CommandFailed({
                  code: closed && pending.result === result ? "closed" : result.error.code,
                  message: result.error.message,
                }),
              ),
        );
      if (pending.result !== undefined) return settle(pending.result);
      pending.waiters.add(settle);
      return Effect.sync(() => pending.waiters.delete(settle));
    });
  };

  const settle = (result: CommandResult) => {
    const pending = outbox.get(result.requestId);
    if (pending === undefined) return;
    outbox.delete(result.requestId);
    pending.result = result;
    if (!result.ok && pending.command.type !== "watch") update({ error: result.error });
    if (pending.command.type === "submit") {
      patch(pending.command.conversationId, { sending: sendingOf(pending.command.conversationId) });
    }
    for (const waiter of pending.waiters) waiter(result);
  };

  // ─── Events ──────────────────────────────────────────────────────────

  const receive = (event: ServerEvent) => {
    switch (event.type) {
      case "hello":
        return update({ connectionId: event.connectionId, connection: event.connection });
      case "session":
        return update({ conversations: event.conversations });
      case "conversation": {
        const current = state.get().conversations;
        if (current.some((item) => item.id === event.conversation.id)) return;
        return update({
          conversations: [...current, event.conversation].toSorted(
            (left, right) => left.id - right.id,
          ),
        });
      }
      case "activity":
        return update({ activity: event.tasks });
      case "view": {
        const watch = watches.get(event.conversationId);
        if (watch === undefined || watch.listeners === 0) return;
        if (event.frame.type === "ops" && watch.awaitingSnapshot) return;
        if (event.frame.type === "snapshot") watch.awaitingSnapshot = false;
        // SAFETY: the object sends `View` messages; the protocol's schema only checks they are JSON.
        const applied = watch.reader.apply(event.frame as View.ViewMessage);
        // A batch that does not apply means this copy drifted: reconnect for a fresh snapshot.
        if (Result.isFailure(applied) || applied.success === null) return socket?.close();
        return patch(event.conversationId, {
          status: "ready",
          view: applied.success,
          transcript: watch.transcript.merge(applied.success),
        });
      }
      case "presence":
        return patch(event.conversationId, { peers: event.peers });
      case "result":
        return settle(event);
      case "event":
        for (const listener of eventListeners) {
          listener({ conversationId: event.conversationId, data: event.data });
        }
        return;
      case "error":
        return update({ error: event.error });
    }
  };

  const open = () => {
    const next = connect(url);
    socket = next;
    next.addEventListener("open", () => {
      attempts = 0;
      update({ status: "open" });
      // A new connection watches nothing: every watched conversation is requested again and answers with a fresh
      // snapshot. Unanswered watch requests of the old connection are dropped; the new ones replace them.
      for (const [requestId, pending] of outbox) {
        if (pending.command.type === "watch") outbox.delete(requestId);
      }
      for (const [conversationId, watch] of watches) {
        if (watch.listeners === 0) continue;
        rewatch(conversationId, watch);
        if (watch.presence !== null) {
          write({ type: "presence", conversationId, presence: watch.presence });
        }
      }
      for (const pending of outbox.values()) {
        if (pending.command.type !== "watch") write(pending.command);
      }
    });
    next.addEventListener("message", (message) => {
      if (socket !== next || !Schema.is(Schema.String)(message.data)) return;
      const decoded = decodeServerEvent(message.data);
      if (Result.isSuccess(decoded)) receive(decoded.success);
    });
    next.addEventListener("close", (event) => {
      if (closed || socket !== next) return;
      if (event.code === REFUSED_CLOSE_CODE) {
        return shutdown("refused", "The session refused the connection");
      }
      update({ status: "reconnecting" });
      attempts += 1;
      // @effect-diagnostics-next-line globalTimers:off
      retry = globalThis.setTimeout(
        open,
        Math.min(maxBackoffMs, backoffMs * 2 ** Math.min(attempts - 1, 10)),
      );
    });
  };

  /** Ends the client for good: no reconnect, and unanswered commands fail with `closed`. */
  const shutdown = (status: "closed" | "refused", message: string) => {
    if (closed) return;
    closed = true;
    globalThis.clearTimeout(retry);
    socket?.close();
    update({ status });
    for (const pending of Array.from(outbox.values())) {
      settle({
        type: "result",
        requestId: pending.command.requestId,
        ok: false,
        error: { code: "failed", message },
      });
    }
  };

  /** A command made when its Effect runs. */
  const command = (make: () => Request) => Effect.suspend(() => enqueue(make()));

  open();

  return {
    state,
    conversation,
    events: (listener) => {
      eventListeners.add(listener);
      return () => eventListeners.delete(listener);
    },
    create: () =>
      Effect.map(
        command(() => ({ type: "create", requestId: nextRequestId() })),
        (result) => result.conversationId ?? 0,
      ),
    fork: (conversationId, entryId) =>
      Effect.map(
        command(() => ({ type: "fork", requestId: nextRequestId(), conversationId, entryId })),
        (result) => result.conversationId ?? 0,
      ),
    submit: (conversationId, content, submitOptions) =>
      Effect.map(
        command(() =>
          submitOptions?.whenBusy === undefined
            ? { type: "submit", requestId: nextRequestId(), conversationId, content }
            : {
                type: "submit",
                requestId: nextRequestId(),
                conversationId,
                content,
                whenBusy: submitOptions.whenBusy,
              },
        ),
        (result) => result.submissionId ?? 0,
      ),
    withdraw: (conversationId, submissionId) =>
      Effect.asVoid(
        command(() => ({
          type: "withdraw",
          requestId: nextRequestId(),
          conversationId,
          submissionId,
        })),
      ),
    abort: (conversationId) =>
      Effect.asVoid(command(() => ({ type: "abort", requestId: nextRequestId(), conversationId }))),
    loadEarlier: (conversationId) => load(conversationId, (transcript) => transcript.loadEarlier),
    fillGap: (conversationId, gap) => load(conversationId, (transcript) => transcript.fillGap(gap)),
    presence: (conversationId, value) => {
      const watch = watches.get(conversationId);
      if (watch === undefined || watch.listeners === 0) return;
      watch.presence = value;
      write({ type: "presence", conversationId, presence: value });
    },
    close: () => shutdown("closed", "The session client is closed"),
  };
};

/**
 * Runs a command from a UI handler and answers at once: `onClick={() => SessionClient.run(session.abort(id))}`. Its
 * failure is also the session's `error`. Answers the fiber, to await or interrupt (which stops only the wait).
 *
 * @category utils
 * @since 0.0.0
 */
export const run = <A>(command: Effect.Effect<A, CommandFailed>): Fiber.Fiber<A, CommandFailed> =>
  Effect.runFork(command);

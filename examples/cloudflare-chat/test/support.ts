// A test client of the session socket: it speaks `@effect-durable-harness/host/SessionProtocol`, keeps a `View.Reader` per
// watched conversation, and records what the object sent.
import * as View from "@effect-durable-harness/host/View";
import {
  type ClientCommand,
  type ConversationSummary,
  decodeServerEvent,
  encodeClientCommand,
  type ServerEvent,
} from "@effect-durable-harness/host/SessionProtocol";
import type { ConversationView } from "@effect-durable-harness/core/View";
import * as Data from "effect/Data";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Predicate from "effect/Predicate";
import * as Result from "effect/Result";
import * as Schedule from "effect/Schedule";
import * as HttpClient from "effect/http/HttpClient";
import * as HttpClientResponse from "effect/http/HttpClientResponse";
import * as Schema from "effect/Schema";

const base = (url: string) => url.replace(/\/+$/, "");

let requests = 0;
/** A fresh request ID. */
export const requestId = (label: string) => `${label}-${process.pid}-${requests++}`;

/** Creates a session through the API, as the web app does. */
export const createSession = (url: string) =>
  Effect.gen(function* () {
    const client = yield* HttpClient.HttpClient;
    const response = yield* client.post(`${base(url)}/api/sessions`).pipe(
      Effect.flatMap(HttpClientResponse.filterStatus((status) => status === 201)),
      // A fresh deployment can answer 404 / error 1042 for a few seconds.
      Effect.retry({ schedule: Schedule.spaced("1 second"), times: 30 }),
    );
    const body = yield* response.json.pipe(
      Effect.flatMap(Schema.decodeUnknownEffect(Schema.Struct({ sessionId: Schema.String }))),
    );
    return body.sessionId;
  }).pipe(Effect.orDie);

/** Resets a session's object, as an eviction does (test deployments only). */
export const crash = (url: string, sessionId: string) =>
  Effect.gen(function* () {
    const client = yield* HttpClient.HttpClient;
    yield* client.post(`${base(url)}/api/sessions/${sessionId}/crash`);
  }).pipe(Effect.orDie);

class SocketFailed extends Data.TaggedError("SocketFailed")<{}> {}

/** One connected client. */
export interface Client {
  readonly events: Array<ServerEvent>;
  readonly conversations: () => ReadonlyArray<ConversationSummary>;
  readonly view: (conversationId: number) => ConversationView | null;
  readonly send: (command: ClientCommand) => void;
  readonly close: () => void;
}

/** Connects to a session's socket as `username`, retrying while a fresh deployment refuses connections. */
export const connect = (url: string, sessionId: string, username: string) =>
  Effect.callback<Client, SocketFailed>((resume) => {
    const target = `${base(url).replace(/^http/, "ws")}/api/sessions/${sessionId}?username=${encodeURIComponent(username)}`;
    const socket = new WebSocket(target);
    const events: Array<ServerEvent> = [];
    const readers = new Map<number, View.Reader>();
    let conversations: ReadonlyArray<ConversationSummary> = [];
    socket.addEventListener("message", (message) => {
      const decoded = decodeServerEvent(String(message.data));
      if (Result.isFailure(decoded)) return;
      const event = decoded.success;
      events.push(event);
      if (event.type === "session") conversations = event.conversations;
      if (event.type === "conversation") conversations = [...conversations, event.conversation];
      if (event.type === "view") {
        const reader = readers.get(event.conversationId) ?? View.makeReader();
        readers.set(event.conversationId, reader);
        // SAFETY: the object sends `View` messages; the protocol's schema only checks they are JSON.
        reader.apply(event.frame as View.ViewMessage);
      }
    });
    socket.addEventListener("open", () =>
      resume(
        Effect.succeed({
          events,
          conversations: () => conversations,
          view: (conversationId) => readers.get(conversationId)?.value() ?? null,
          send: (command) => socket.send(encodeClientCommand(command)),
          close: () => socket.close(),
        }),
      ),
    );
    socket.addEventListener("error", () => resume(Effect.fail(new SocketFailed())));
  }).pipe(Effect.retry({ schedule: Schedule.spaced("1 second"), times: 30 }), Effect.orDie);

/** Polls `check` every 200 ms until it holds, at most `seconds`; dies with what it saw otherwise. */
export const eventually = <A>(check: () => A | undefined | false, seconds = 30) =>
  Effect.suspend(() => {
    const value = check();
    return value === undefined || value === false
      ? Effect.fail("pending" as const)
      : Effect.succeed(value);
  }).pipe(
    Effect.retry({ schedule: Schedule.spaced(Duration.millis(200)), times: seconds * 5 }),
    Effect.orDie,
  );

/** The `result`s for `id`, in arrival order. */
export const resultsOf = (client: Client, id: string) =>
  client.events.filter((event) => event.type === "result" && event.requestId === id);

/** The first `result` for `id`, once it arrived. */
export const resultOf = (client: Client, id: string) => eventually(() => resultsOf(client, id)[0]);

/** The conversation a successful `result` names. */
export const conversationOf = (result: ServerEvent): number =>
  result.type === "result" && result.ok ? (result.conversationId ?? 0) : 0;

/** Watches a conversation and waits for its snapshot. */
export const watch = (client: Client, conversationId: number) =>
  Effect.gen(function* () {
    const id = requestId("watch");
    client.send({ type: "watch", requestId: id, conversationId });
    yield* resultOf(client, id);
    yield* eventually(() => client.view(conversationId) !== null);
  });

/** The messages of a conversation's view as `role: text`, in order. */
export const transcript = (view: ConversationView | null): ReadonlyArray<string> =>
  (view?.entries ?? []).flatMap((entry) =>
    (entry.model ?? []).flatMap((message) => {
      if (message.role === "system") return [];
      const text = Predicate.isString(message.content)
        ? message.content
        : message.content.map((part) => (part.type === "text" ? part.text : "")).join("");
      return [`${message.role}: ${text}`];
    }),
  );

/** The entry ID of the first user message containing `text`. */
export const userEntryId = (view: ConversationView | null, text: string): number | undefined =>
  view?.entries.find((entry) =>
    (entry.model ?? []).some(
      (message) =>
        message.role === "user" &&
        (Predicate.isString(message.content) ? message.content : "").includes(text),
    ),
  )?.id;

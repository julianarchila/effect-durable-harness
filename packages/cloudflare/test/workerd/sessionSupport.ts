// Helpers of the session socket suite: a raw client of `SessionProtocol` that records what the object sent, and calls
// to `SessionProbe` / `ReadOnlyProbe` through the test Worker.
import type { ConversationView } from "@effect-durable-harness/core/View";
import * as Data from "effect/Data";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Predicate from "effect/Predicate";
import * as Result from "effect/Result";
import * as Schedule from "effect/Schedule";
import * as HttpClient from "effect/http/HttpClient";
import * as HttpClientRequest from "effect/http/HttpClientRequest";
import * as HttpClientResponse from "effect/http/HttpClientResponse";
import {
  type ClientCommand,
  decodeServerEvent,
  encodeClientCommand,
  REFUSED_CLOSE_CODE,
  type ServerEvent,
} from "@effect-durable-harness/host/SessionProtocol";
import * as View from "@effect-durable-harness/host/View";

export type Kind = "session" | "readonly";

const base = (url: string) => url.replace(/\/+$/, "");

let requests = 0;
/** A fresh request ID. */
export const requestId = (label: string) => `${label}-${process.pid}-${requests++}`;

/** Calls a method of the object; a typed failure answers `{ failed, revived }`, a reset `{ reset: true }`. */
export const callSession = <A>(
  url: string,
  kind: Kind,
  name: string,
  method: string,
  ...args: ReadonlyArray<unknown>
) =>
  Effect.gen(function* () {
    const client = yield* HttpClient.HttpClient;
    const request = HttpClientRequest.post(`${base(url)}/${kind}/${name}/${method}`).pipe(
      HttpClientRequest.bodyJsonUnsafe(args),
    );
    const response = yield* client
      .execute(request)
      .pipe(
        Effect.flatMap(HttpClientResponse.filterStatusOk),
        Effect.retry({ schedule: Schedule.spaced("1 second"), times: 30 }),
      );
    // SAFETY: the test Worker answers the method's JSON result.
    return (yield* response.json) as A;
  }).pipe(Effect.orDie);

class SocketFailed extends Data.TaggedError("SocketFailed")<{}> {}

/** One connected client. */
export interface Client {
  readonly events: Array<ServerEvent>;
  readonly view: (conversationId: number) => ConversationView | null;
  readonly send: (command: ClientCommand) => void;
  readonly closed: () => { readonly code: number; readonly reason: string } | undefined;
  readonly close: () => void;
}

const socketUrl = (url: string, kind: Kind, name: string, username: string) =>
  `${base(url).replace(/^http/, "ws")}/${kind}/${name}?username=${encodeURIComponent(username)}`;

/** Connects as `username`, retrying while a fresh deployment refuses connections. */
export const connect = (url: string, kind: Kind, name: string, username: string) =>
  Effect.callback<Client, SocketFailed>((resume) => {
    const socket = new WebSocket(socketUrl(url, kind, name, username));
    const events: Array<ServerEvent> = [];
    const readers = new Map<number, View.Reader>();
    let closed: { readonly code: number; readonly reason: string } | undefined;
    socket.addEventListener("message", (message) => {
      const decoded = decodeServerEvent(String(message.data));
      if (Result.isFailure(decoded)) return;
      const event = decoded.success;
      events.push(event);
      if (event.type === "view") {
        const reader = readers.get(event.conversationId) ?? View.makeReader();
        readers.set(event.conversationId, reader);
        // SAFETY: the object sends `View` messages; the protocol's schema only checks they are JSON.
        reader.apply(event.frame as View.ViewMessage);
      }
    });
    socket.addEventListener("close", (event) => {
      closed = { code: event.code, reason: event.reason };
    });
    socket.addEventListener("open", () =>
      resume(
        Effect.succeed({
          events,
          view: (conversationId) => readers.get(conversationId)?.value() ?? null,
          send: (command) => socket.send(encodeClientCommand(command)),
          closed: () => closed,
          close: () => socket.close(),
        }),
      ),
    );
    socket.addEventListener("error", () => resume(Effect.fail(new SocketFailed())));
  }).pipe(Effect.retry({ schedule: Schedule.spaced("1 second"), times: 30 }), Effect.orDie);

/**
 * Whether the object refuses `username`'s upgrade: it accepts the upgrade and closes it at once with
 * `REFUSED_CLOSE_CODE`, before sending anything (no retry: the deployment already answers).
 */
export const refused = (url: string, kind: Kind, name: string, username: string) =>
  Effect.callback<boolean>((resume) => {
    const socket = new WebSocket(socketUrl(url, kind, name, username));
    let received = false;
    socket.addEventListener("message", () => {
      received = true;
    });
    socket.addEventListener("close", (event) =>
      resume(Effect.succeed(!received && event.code === REFUSED_CLOSE_CODE)),
    );
    socket.addEventListener("error", () => resume(Effect.succeed(false)));
  });

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

/** The `result` for `id`, once it arrived. */
export const resultOf = (client: Client, id: string) =>
  eventually(() =>
    client.events.find((event) => event.type === "result" && event.requestId === id),
  );

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

/** Watches a conversation and waits for its snapshot. */
export const watch = (client: Client, conversationId: number) =>
  Effect.gen(function* () {
    const id = requestId("watch");
    client.send({ type: "watch", requestId: id, conversationId });
    yield* resultOf(client, id);
    yield* eventually(() => client.view(conversationId) !== null);
  });

// HTTP and socket clients of the API Worker and the payment gateway, as a shop's frontend and back office would
// call them.
import {
  type ClientCommand,
  decodeServerEvent,
  encodeClientCommand,
  type ServerEvent,
} from "@effect-durable-harness/host/SessionProtocol";
import * as Data from "effect/Data";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Result from "effect/Result";
import * as Schedule from "effect/Schedule";
import * as Schema from "effect/Schema";
import * as HttpClient from "effect/http/HttpClient";
import * as HttpClientRequest from "effect/http/HttpClientRequest";
import * as HttpClientResponse from "effect/http/HttpClientResponse";
import { type OpenTicket, type RefundPolicy, RefundLedger, TicketStatus } from "../src/protocol.ts";

const base = (url: string) => url.replace(/\/+$/, "");

let tickets = 0;
/** A fresh ticket ID, so each test has its own object. */
export const ticketId = (label: string) => `${label}-${process.pid}-${tickets++}`;

// A fresh deployment can answer 404 / error 1042 for a few seconds.
const retryFresh = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  Effect.retry(effect, { schedule: Schedule.spaced("1 second"), times: 30 });

const send = (request: HttpClientRequest.HttpClientRequest, expected: number) =>
  Effect.gen(function* () {
    const client = yield* HttpClient.HttpClient;
    return yield* client
      .execute(request)
      .pipe(
        Effect.flatMap(HttpClientResponse.filterStatus((status) => status === expected)),
        retryFresh,
      );
  });

/** Opens a ticket, as the shop's contact form does. */
export const openTicket = (api: string, input: OpenTicket) =>
  send(
    HttpClientRequest.post(`${base(api)}/tickets`).pipe(HttpClientRequest.bodyJsonUnsafe(input)),
    202,
  ).pipe(Effect.asVoid, Effect.orDie);

/** The ticket's status. */
export const status = (api: string, id: string) =>
  send(HttpClientRequest.get(`${base(api)}/tickets/${id}`), 200).pipe(
    Effect.flatMap(HttpClientResponse.schemaBodyJson(TicketStatus)),
    Effect.orDie,
  );

/** A person takes the ticket; answers the HTTP status and body. */
export const handOff = (api: string, id: string, email: string) =>
  Effect.gen(function* () {
    const client = yield* HttpClient.HttpClient;
    const response = yield* client.execute(
      HttpClientRequest.post(`${base(api)}/tickets/${id}/handoff`).pipe(
        HttpClientRequest.bodyJsonUnsafe({ email }),
      ),
    );
    return { status: response.status, body: yield* response.json };
  }).pipe(Effect.orDie);

/** Resets the ticket's object, as an eviction does. */
export const crash = (api: string, id: string) =>
  send(HttpClientRequest.post(`${base(api)}/tickets/${id}/crash`), 204).pipe(
    Effect.asVoid,
    Effect.orDie,
  );

/** Issues a socket token for a customer. */
export const issueToken = (api: string, customerId: string, name: string) =>
  send(
    HttpClientRequest.post(`${base(api)}/customers/${customerId}/tokens`).pipe(
      HttpClientRequest.bodyJsonUnsafe({ name }),
    ),
    201,
  ).pipe(
    Effect.flatMap(HttpClientResponse.schemaBodyJson(Schema.Struct({ token: Schema.String }))),
    Effect.map((body) => body.token),
    Effect.orDie,
  );

/** Changes the shop's refund policy. */
export const setRefundPolicy = (api: string, policy: RefundPolicy) =>
  send(
    HttpClientRequest.put(`${base(api)}/policies/refund`).pipe(
      HttpClientRequest.bodyJsonUnsafe(policy),
    ),
    204,
  ).pipe(Effect.asVoid, Effect.orDie);

/** The gateway's refunds of one charge. */
export const refunds = (gateway: string, chargeId: string) =>
  send(HttpClientRequest.get(`${base(gateway)}/refunds/${chargeId}`), 200).pipe(
    Effect.flatMap(HttpClientResponse.schemaBodyJson(RefundLedger)),
    Effect.map((ledger) => ledger.refunds),
    Effect.orDie,
  );

class SocketFailed extends Data.TaggedError("SocketFailed")<{}> {}

/** One connected socket: what the object sent, and how it closed. */
export interface Socket {
  readonly events: Array<ServerEvent>;
  readonly closed: () => number | undefined;
  readonly send: (command: ClientCommand) => void;
  readonly close: () => void;
}

/** Connects to a ticket's socket with `token`. */
export const connect = (api: string, id: string, token: string) =>
  Effect.callback<Socket, SocketFailed>((resume) => {
    const target = `${base(api).replace(/^http/u, "ws")}/tickets/${id}/socket?token=${encodeURIComponent(token)}`;
    const socket = new WebSocket(target);
    const events: Array<ServerEvent> = [];
    let closed: number | undefined;
    socket.addEventListener("message", (message) => {
      const decoded = decodeServerEvent(String(message.data));
      if (Result.isSuccess(decoded)) events.push(decoded.success);
    });
    socket.addEventListener("close", (event) => {
      closed = event.code;
    });
    socket.addEventListener("open", () =>
      resume(
        Effect.succeed({
          events,
          closed: () => closed,
          send: (command) => socket.send(encodeClientCommand(command)),
          close: () => socket.close(),
        }),
      ),
    );
    socket.addEventListener("error", () => resume(Effect.fail(new SocketFailed())));
  }).pipe(Effect.retry({ schedule: Schedule.spaced("1 second"), times: 30 }), Effect.orDie);

/** Repeats `read` every 250 ms until `done` holds, at most `seconds`; dies with the last value otherwise. */
export const eventually = <A, E, R>(
  read: Effect.Effect<A, E, R>,
  done: (value: A) => boolean,
  seconds = 30,
) =>
  read.pipe(
    Effect.flatMap((value) => (done(value) ? Effect.succeed(value) : Effect.fail(value))),
    Effect.retry({ schedule: Schedule.spaced(Duration.millis(250)), times: seconds * 4 }),
    Effect.catch((last) =>
      Effect.die(new Error(`eventually: gave up; last value ${JSON.stringify(last)}`)),
    ),
  );

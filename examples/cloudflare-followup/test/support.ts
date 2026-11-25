// HTTP clients of the inbound Worker and the sink, as a provider's webhook and a test harness would call them.
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Schedule from "effect/Schedule";
import * as Schema from "effect/Schema";
import * as HttpClient from "effect/http/HttpClient";
import * as HttpClientRequest from "effect/http/HttpClientRequest";
import * as HttpClientResponse from "effect/http/HttpClientResponse";
import { type Inbound, InboundResult, type SinkFaults, SinkLedger } from "../src/protocol.ts";

const base = (url: string) => url.replace(/\/+$/, "");

let contacts = 0;
/** A fresh contact address, so each test has its own object and ledger. */
export const contactAddress = (label: string) => `${label}-${process.pid}-${contacts++}`;

// A fresh deployment can answer 404 / error 1042 for a few seconds.
const retryFresh = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  Effect.retry(effect, { schedule: Schedule.spaced("1 second"), times: 30 });

/** Posts an inbound message, as the provider's webhook does. */
export const postInbound = (api: string, inbound: Inbound) =>
  Effect.gen(function* () {
    const client = yield* HttpClient.HttpClient;
    const response = yield* client
      .execute(
        HttpClientRequest.post(`${base(api)}/inbound`).pipe(
          HttpClientRequest.bodyJsonUnsafe(inbound),
        ),
      )
      .pipe(
        Effect.flatMap(HttpClientResponse.filterStatus((status) => status === 202)),
        retryFresh,
      );
    return yield* HttpClientResponse.schemaBodyJson(InboundResult)(response);
  }).pipe(Effect.orDie);

/** What one recipient got at the sink. */
export const ledger = (sink: string, to: string) =>
  Effect.gen(function* () {
    const client = yield* HttpClient.HttpClient;
    const response = yield* client
      .get(`${base(sink)}/deliveries/${encodeURIComponent(to)}`)
      .pipe(Effect.flatMap(HttpClientResponse.filterStatusOk), retryFresh);
    return yield* HttpClientResponse.schemaBodyJson(SinkLedger)(response);
  }).pipe(Effect.orDie);

/** Makes the sink refuse the recipient's next attempts, or answer them late. */
export const injectFaults = (sink: string, to: string, faults: SinkFaults) =>
  Effect.gen(function* () {
    const client = yield* HttpClient.HttpClient;
    yield* client
      .execute(
        HttpClientRequest.put(`${base(sink)}/faults/${encodeURIComponent(to)}`).pipe(
          HttpClientRequest.bodyJsonUnsafe(faults),
        ),
      )
      .pipe(
        Effect.flatMap(HttpClientResponse.filterStatus((status) => status === 204)),
        retryFresh,
      );
  }).pipe(Effect.orDie);

/** The part of `Contact.inspect` the tests read. */
const Inspection = Schema.Struct({
  now: Schema.Number,
  alarm: Schema.NullOr(Schema.Number),
  tasks: Schema.Array(
    Schema.Struct({
      kind: Schema.String,
      state: Schema.String,
      until: Schema.NullOr(Schema.Number),
    }),
  ),
  transcript: Schema.Array(Schema.String),
  deliveries: Schema.NullOr(
    Schema.Struct({
      delivered: Schema.Record(
        Schema.String,
        Schema.Struct({ deliveryId: Schema.String, key: Schema.String }),
      ),
    }),
  ),
});

/** The contact's object, as `GET /contacts/:address` shows it. Starts the object if it is not running. */
export const inspect = (api: string, to: string) =>
  Effect.gen(function* () {
    const client = yield* HttpClient.HttpClient;
    const response = yield* client
      .get(`${base(api)}/contacts/${encodeURIComponent(to)}`)
      .pipe(Effect.flatMap(HttpClientResponse.filterStatusOk), retryFresh);
    return yield* HttpClientResponse.schemaBodyJson(Inspection)(response);
  }).pipe(Effect.orDie);

/** Resets the contact's object, as an eviction does (`FOLLOWUP_ALLOW_CRASH=true` only). */
export const crash = (api: string, to: string) =>
  Effect.gen(function* () {
    const client = yield* HttpClient.HttpClient;
    yield* client.post(`${base(api)}/contacts/${encodeURIComponent(to)}/crash`);
  }).pipe(Effect.orDie);

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

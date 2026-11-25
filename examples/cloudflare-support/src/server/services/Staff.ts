/**
 * The shop's staff and its escalation queue, in its D1 database. `page` is what the `NotifyStaff` task runs;
 * `byEmail` is what `SupportSession.handOff` checks, a service the object's methods use and the agent does not.
 */
import * as Host from "@effect-durable-harness/cloudflare/Host";
import * as Cloudflare from "@alchemy.run/cloudflare";
import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import { ShopDb } from "../../infra.ts";

/** No staff member with that email. Crosses RPC: `SupportSession` lists it in its `errors`. */
export class StaffNotFound extends Schema.TaggedError<StaffNotFound>()("StaffNotFound", {
  email: Schema.String,
}) {}

/** Nobody is on call to page. */
export class NobodyOnCall extends Schema.TaggedError<NobodyOnCall>()("NobodyOnCall", {}) {}

export interface StaffMember {
  readonly email: string;
  readonly name: string;
}

export interface Escalation {
  readonly reason: string;
  readonly paged: string;
}

/** What `page` takes. */
export interface PageRequest {
  readonly ticketId: string;
  readonly reason: string;
  /** Makes the page idempotent: a retried page with the same key writes nothing and answers the first. */
  readonly key: string;
}

export class Staff extends Context.Service<
  Staff,
  {
    /** Queues an escalation for whoever is on call and answers their email; once per `key`. */
    readonly page: (request: PageRequest) => Effect.Effect<string, NobodyOnCall>;
    readonly byEmail: (email: string) => Effect.Effect<StaffMember, StaffNotFound>;
    readonly escalations: (ticketId: string) => Effect.Effect<ReadonlyArray<Escalation>>;
  }
>()("@effect-durable-harness/example-cloudflare-support/server/services/Staff") {}

export const StaffLive = Layer.effect(
  Staff,
  Effect.gen(function* () {
    const db = yield* Cloudflare.D1.QueryDatabase(ShopDb);
    return Staff.of({
      page: ({ ticketId, reason, key }) =>
        Effect.gen(function* () {
          const onCall = yield* db
            .prepare("SELECT email, name FROM staff WHERE on_call = 1 ORDER BY email LIMIT 1")
            .first<StaffMember>();
          if (onCall === null) return yield* new NobodyOnCall();
          yield* db
            .prepare(
              "INSERT INTO escalations (key, ticket_id, reason, paged, created_at) VALUES (?, ?, ?, ?, ?) ON CONFLICT (key) DO NOTHING",
            )
            .bind(key, ticketId, reason, onCall.email, yield* Clock.currentTimeMillis)
            .run();
          const row = yield* db
            .prepare("SELECT paged FROM escalations WHERE key = ?")
            .bind(key)
            .first<{ paged: string }>();
          return row?.paged ?? onCall.email;
        }).pipe(Host.withRuntimeContext, Effect.withSpan("Staff.page")),
      byEmail: (email) =>
        Host.withRuntimeContext(
          db
            .prepare("SELECT email, name FROM staff WHERE email = ?")
            .bind(email)
            .first<StaffMember>(),
        ).pipe(
          Effect.flatMap((row) =>
            row === null ? Effect.fail(new StaffNotFound({ email })) : Effect.succeed(row),
          ),
        ),
      escalations: (ticketId) =>
        Host.withRuntimeContext(
          db
            .prepare(
              "SELECT reason, paged FROM escalations WHERE ticket_id = ? ORDER BY created_at",
            )
            .bind(ticketId)
            .all<Escalation>(),
        ).pipe(Effect.map((result) => result.results)),
    });
  }),
).pipe(Layer.provide(Cloudflare.D1.QueryDatabaseBinding));

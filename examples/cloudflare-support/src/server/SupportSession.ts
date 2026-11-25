/**
 * One support ticket: a Durable Object whose root conversation is the conversation with the customer. The class is
 * Alchemy's own, typed by its methods; the host is a piece of its two-phase constructor.
 *
 * - **Isolate phase** (once per isolate): `Host.agent` captures the handlers, models and services provided to the
 *   object's Layer; `Staff` is yielded here because a method uses it.
 * - **Instance phase** (every construction): `agent.start` builds the agent over the object's SQLite, recovers,
 *   restores the socket and arms the alarm; `host.methods` answers the host's handlers under the ticket's methods,
 *   written inline against the hand-written `SupportMethods`.
 * - The session socket authorizes customers with a token from KV (`CustomerAuth`), in `accept`.
 */
import * as Host from "@effect-durable-harness/cloudflare/Host";
import * as Cloudflare from "@alchemy.run/cloudflare";
import * as Config from "effect/Config";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Predicate from "effect/Predicate";
import * as Stream from "effect/Stream";
import type { HttpServerRequest } from "effect/http/HttpServerRequest";
import { Customer, type OpenTicket, type TicketStatus } from "../protocol.ts";
import { SupportHarness, SupportModel, Ticket, supportAgent } from "./agent/support.ts";
import { SupportHandlers } from "./agent/support.live.ts";
import { ModelsLive } from "./models.ts";
import { CustomerAuth, CustomerAuthLive } from "./services/CustomerAuth.ts";
import { OrdersLive } from "./services/Orders.ts";
import { PaymentsLive } from "./services/Payments.ts";
import { PoliciesLive } from "./services/Policies.ts";
import { Staff, StaffLive, StaffNotFound } from "./services/Staff.ts";

const SupportAgent = SupportHarness.layer({
  defaults: { agent: Effect.map(Effect.service(SupportModel), (model) => supportAgent(model.ref)) },
});

/** The customer's socket: `?token=` checked against KV; peers see the name only; prompts are attributed. */
const Customers = Host.socket({
  connection: Customer,
  accept: (request: HttpServerRequest) =>
    Effect.gen(function* () {
      const auth = yield* CustomerAuth;
      return yield* auth.verify(
        new URL(request.url, "http://ticket").searchParams.get("token") ?? "",
      );
    }),
  peer: (customer) => ({ name: customer.name }),
  input: (customer, text) => `[${customer.name}] ${text}`,
});

/** `SupportSession`'s methods: the contract the API Worker binds. */
export interface SupportMethods {
  /** Opens the ticket with its first message; idempotent by `ticketId`. */
  readonly open: (
    input: OpenTicket,
  ) => Effect.Effect<{ readonly submissionId: number }, Host.AgentError>;
  /** A person takes the ticket: the ticket records it and the customer's socket hears it. */
  readonly handOff: (
    email: string,
  ) => Effect.Effect<{ readonly assignee: string }, Host.AgentError | StaffNotFound>;
  /** The ticket, its transcript, its live tasks and the shop's escalations for it. */
  readonly status: () => Effect.Effect<TicketStatus, Host.AgentError>;
  /** Resets the object, as an eviction does; only with `SUPPORT_ALLOW_CRASH=true` (tests). */
  readonly crash: () => Effect.Effect<void>;
}

export class SupportSession extends Cloudflare.DurableObject<SupportSession, SupportMethods>()(
  "SupportSession",
  { errors: [...Host.errors, StaffNotFound] },
) {}

export const SupportSessionLive = SupportSession.make(
  Effect.gen(function* () {
    // Isolate phase. Read at init, so Alchemy binds them to the Worker; the tests shorten the keep-alive.
    const keepAliveMs = yield* Config.option(Config.Int("SUPPORT_KEEP_ALIVE_MS")).pipe(
      Effect.orDie,
    );
    const allowCrash = yield* Config.Boolean("SUPPORT_ALLOW_CRASH").pipe(
      Config.withDefault(false),
      Effect.orDie,
    );
    const agent = yield* Host.agent(SupportAgent, {
      storage: { keepAliveMs: Option.getOrUndefined(keepAliveMs) },
      socket: Customers,
    });
    const staff = yield* Staff;

    // Instance phase: Alchemy's two-phase constructor returns it.
    // @effect-diagnostics-next-line returnEffectInGen:off
    return Effect.gen(function* () {
      const host = yield* agent.start;
      const rpc = Host.agentRpc(host.harness);
      const root = yield* host.harness.ensureRoot().pipe(Effect.orDie);

      return host.methods<SupportMethods>({
        open: (input) =>
          Effect.gen(function* () {
            yield* root.commit(
              Effect.gen(function* () {
                const ticket = yield* Ticket.get(root.id);
                ticket.ticketId = input.ticketId;
                ticket.customerId = input.customerId;
              }),
            );
            const submission = yield* rpc.submit(root.id, {
              type: "input",
              content: input.message,
              requestId: `ticket:${input.ticketId}`,
              whenBusy: "followUp",
            });
            return { submissionId: submission.id };
          }),

        handOff: (email) =>
          Effect.gen(function* () {
            const member = yield* staff.byEmail(email);
            yield* root.commit(
              Effect.gen(function* () {
                const ticket = yield* Ticket.get(root.id);
                ticket.status = "human";
                ticket.assignee = member.name;
              }),
            );
            yield* host.socket.broadcast(root.id, { type: "handoff", name: member.name });
            return { assignee: member.name };
          }),

        status: () =>
          Effect.gen(function* () {
            const ticket = Option.getOrNull(yield* host.session.snapshot(Ticket, root.id));
            const inspection = yield* host.harness.inspect;
            const entries = yield* Stream.runCollect(root.entries({ order: "ascending" }));
            return {
              ticket,
              transcript: Array.from(entries).flatMap((entry) =>
                (entry.model ?? []).flatMap((message) =>
                  message.role === "user" || message.role === "assistant"
                    ? [`${message.role}: ${textOf(message.content)}`]
                    : [],
                ),
              ),
              tasks: inspection.tasks.map((task) => ({
                kind: task.record.kind,
                state: task.state.kind,
              })),
              escalations: [...(yield* staff.escalations(ticket?.ticketId ?? ""))],
            };
          }),

        crash: () => (allowCrash ? host.abort("support: crash (test)") : Effect.void),
      });
    });
  }),
).pipe(
  // Built once per isolate: the handlers request the services, which bind D1, KV and the gateway.
  Layer.provide(SupportHandlers),
  Layer.provide(
    Layer.mergeAll(OrdersLive, PaymentsLive, PoliciesLive, StaffLive, CustomerAuthLive, ModelsLive),
  ),
);

const textOf = (
  content: string | ReadonlyArray<{ readonly type: string; readonly text?: string }>,
): string =>
  Predicate.isString(content)
    ? content
    : content.map((part) => (part.type === "text" ? (part.text ?? "") : "")).join("");

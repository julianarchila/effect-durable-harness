/**
 * One contact: a Durable Object whose root conversation is the conversation with that contact.
 * Alchemy's modular class is the contract the inbound Worker binds; `ContactLive`
 * is the implementation it provides. The methods are written inline against the hand-written
 * `ContactMethods`.
 *
 * - `receive` takes an inbound message: in one commit it admits the input under the provider message ID, records it
 *   as the contact's newest, and creates the `DeliverReply` that delivers the answer and the `FollowUp` that sleeps
 *   past it. A resent webhook finds its submission and creates nothing.
 * - `inspect` and `crash` are for tests and a look by hand; `crash` resets the object only on a
 *   deployment with `FOLLOWUP_ALLOW_CRASH=true` (the tests and a scratch stage).
 * - The handlers, the webhook and the models are provided outside the object, so they are built once per isolate.
 */
import * as Host from "@effect-durable-harness/cloudflare/Host";
import type { ReadAfterWrite } from "@effect-durable-harness/core/SessionError";
import * as Tx from "@effect-durable-harness/core/Tx";
import type { RuntimeContext } from "alchemy";
import * as Cloudflare from "@alchemy.run/cloudflare";
import * as Clock from "effect/Clock";
import * as Config from "effect/Config";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Predicate from "effect/Predicate";
import * as Stream from "effect/Stream";
import type { Inbound, InboundResult } from "../protocol.ts";
import { AssistantHarness, AssistantModel, assistantAgent } from "./agent/assistant.ts";
import { AssistantLive } from "./agent/assistant.live.ts";
import { ContactState, Deliveries } from "./agent/documents.ts";
import { deliverReply } from "./agent/tasks/deliver-reply.ts";
import { FollowUp } from "./agent/tasks/follow-up.ts";
import { ModelsLive } from "./models.ts";
import { WebhookLive } from "./Webhook.ts";

const AssistantAgent = AssistantHarness.layer({
  defaults: {
    agent: Effect.map(Effect.service(AssistantModel), (model) => assistantAgent(model.ref)),
  },
});

/** What `Contact.inspect` answers: the object's state, for the tests and a look by hand. */
export interface ContactInspection {
  readonly now: number;
  readonly alarm: number | null;
  readonly tasks: ReadonlyArray<{
    readonly id: number;
    readonly kind: string;
    readonly state: string;
    readonly until: number | null;
  }>;
  readonly transcript: ReadonlyArray<string>;
  readonly contact: typeof ContactState.definition.schema.Type | null;
  readonly deliveries: typeof Deliveries.definition.schema.Type | null;
}

/** `Contact`'s methods: the contract the inbound Worker binds. */
export interface ContactMethods {
  /** Takes an inbound message; a resent one answers `duplicate` and creates nothing. */
  readonly receive: (
    inbound: Inbound,
  ) => Effect.Effect<InboundResult, Host.AgentError | ReadAfterWrite>;
  /** Reads the native alarm, the one use of the engine's own state, which ties it to Cloudflare. */
  readonly inspect: () => Effect.Effect<ContactInspection, Host.AgentError, RuntimeContext>;
  /** Resets the object, as an eviction does; only with `FOLLOWUP_ALLOW_CRASH=true`. */
  readonly crash: () => Effect.Effect<void>;
}

export class Contact extends Cloudflare.DurableObject<Contact, ContactMethods>()("Contact", {
  errors: Host.errors,
}) {}

export const ContactLive = Contact.make(
  Effect.gen(function* () {
    // Isolate phase. Read at init, so Alchemy binds them to the Worker. The tests shorten both.
    const keepAliveMs = yield* Config.option(Config.Int("FOLLOWUP_KEEP_ALIVE_MS")).pipe(
      Effect.orDie,
    );
    const followUpAfterMs = yield* Config.Int("FOLLOW_UP_AFTER_MS").pipe(
      Config.withDefault(4 * 60 * 60_000),
      Effect.orDie,
    );
    const allowCrash = yield* Config.Boolean("FOLLOWUP_ALLOW_CRASH").pipe(
      Config.withDefault(false),
      Effect.orDie,
    );
    const agent = yield* Host.agent(AssistantAgent, {
      storage: { keepAliveMs: Option.getOrUndefined(keepAliveMs) },
    });

    // Instance phase: Alchemy's two-phase constructor returns it.
    // @effect-diagnostics-next-line returnEffectInGen:off
    return Effect.gen(function* () {
      const host = yield* agent.start;
      const harness = host.harness;
      const session = host.session;
      const state = yield* Cloudflare.DurableObjectState;

      const receive = (inbound: Inbound) =>
        Effect.gen(function* () {
          const root = yield* harness.ensureRoot();
          const receivedAt = yield* Clock.currentTimeMillis;
          // Prefixed, so no provider ID can take a follow-up's request ID (`tasks/follow-up.ts`).
          const requestId = `inbound:${inbound.providerMessageId}`;
          return yield* root.commit(
            Effect.gen(function* () {
              const tx = yield* Tx.Tx;
              if (Option.isSome(yield* tx.submissionByRequest(root.id, requestId))) {
                return { status: "duplicate" } satisfies InboundResult;
              }
              // Admitted here, not by the task: a resend that arrives before the task runs finds it above.
              // `steer`: messages that arrive while the agent answers join that answer (`DeliverReply`'s claim).
              yield* root.admit({
                type: "input",
                content: inbound.text,
                requestId,
                whenBusy: "steer",
              });
              const contact = yield* ContactState.get(root.id);
              contact.address = inbound.from;
              contact.latestInbound = {
                providerMessageId: inbound.providerMessageId,
                at: receivedAt,
              };
              yield* deliverReply({
                requestId,
                content: inbound.text,
                whenBusy: "steer",
                to: inbound.from,
                kind: "reply",
              });
              yield* FollowUp.create(
                {
                  providerMessageId: inbound.providerMessageId,
                  receivedAt,
                  wakeAt: receivedAt + followUpAfterMs,
                  to: inbound.from,
                },
                { ownership: { kind: "conversation" }, background: true },
              );
              return { status: "accepted" } satisfies InboundResult;
            }),
          );
        });

      const inspect = () =>
        Effect.gen(function* () {
          const root = yield* harness.ensureRoot();
          const inspection = yield* harness.inspect;
          const entries = yield* Stream.runCollect(root.entries({ order: "ascending" }));
          return {
            now: yield* Clock.currentTimeMillis,
            alarm: yield* state.storage.getAlarm(),
            tasks: inspection.tasks.map((task) => ({
              id: task.record.id,
              kind: task.record.kind,
              state: task.state.kind,
              until: task.state.kind === "sleeping" ? (task.state.until ?? null) : null,
            })),
            transcript: entries.flatMap((entry) =>
              (entry.model ?? []).flatMap((message) =>
                message.role === "user" || message.role === "assistant"
                  ? [`${message.role}: ${textOf(message.content)}`]
                  : [],
              ),
            ),
            contact: Option.getOrNull(yield* session.snapshot(ContactState, root.id)),
            deliveries: Option.getOrNull(yield* session.snapshot(Deliveries, root.id)),
          };
        });

      return host.methods<ContactMethods>({
        receive,
        inspect,
        crash: () => (allowCrash ? host.abort("followup: crash (test)") : Effect.void),
      });
    });
  }),
).pipe(Layer.provide(AssistantLive), Layer.provide(WebhookLive), Layer.provide(ModelsLive));

const textOf = (
  content: string | ReadonlyArray<{ readonly type: string; readonly text?: string }>,
): string =>
  Predicate.isString(content)
    ? content
    : content.map((part) => (part.type === "text" ? (part.text ?? "") : "")).join("");

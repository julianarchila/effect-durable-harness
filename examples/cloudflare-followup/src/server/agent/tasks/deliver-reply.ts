/**
 * `followup.deliver-reply`: the reporter pattern. It submits an input, waits for the
 * answer, claims the answer, and delivers it to the webhook under a key derived from the answer's entry ID.
 *
 * ```text
 * answer: submit ──► await ──► claim(answer) ──► send: webhook(key) ──► record receipt
 *   ▲ rerun: the same requestId returns the same submission
 *                              ▲ first-writer-wins: one task sends each answer
 *                                                      ▲ rerun or refusal: the same key again
 * ```
 *
 * The send comes before the commit that records it, so a crash between the two sends again under the same key: the
 * webhook's deduplication is what makes it exactly once. A refusal sleeps a backoff (`sleepUntil`, no invocation
 * held) and retries with the same key, up to `MAX_ATTEMPTS`.
 */
import { EntryId } from "@effect-durable-harness/core/Ids";
import type { Message } from "@effect-durable-harness/core/Message";
import * as Task from "@effect-durable-harness/core/Task";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Predicate from "effect/Predicate";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import { DeliveryKind } from "../../../protocol.ts";
import { Webhook } from "../../Webhook.ts";
import { Deliveries } from "../documents.ts";

/** Attempts per answer before the task fails: about 13 minutes of backoff in all. */
const MAX_ATTEMPTS = 10;
const BACKOFF_BASE_MS = 1_000;
const BACKOFF_MAX_MS = 5 * 60_000;

/** What to submit and where its answer goes. */
export const ReplyRequest = Schema.Struct({
  /** The submission's request ID: from the provider message ID, or from the follow-up's task ID. */
  requestId: Schema.String,
  content: Schema.String,
  whenBusy: Schema.Literals(["steer", "followUp"]),
  to: Schema.String,
  kind: DeliveryKind,
});
export type ReplyRequest = typeof ReplyRequest.Type;

export const DeliverReply = Task.make("followup.deliver-reply", {
  version: 1,
  input: ReplyRequest,
  checkpoint: Schema.Union([
    Schema.Struct({ phase: Schema.Literal("answer") }),
    Schema.Struct({ phase: Schema.Literal("send"), answer: Schema.Number, attempt: Schema.Int }),
  ]),
  result: Schema.Union([
    Schema.TaggedStruct("Delivered", { answer: Schema.Number, deliveryId: Schema.String }),
    // Another task claimed the answer: steered inputs share one.
    Schema.TaggedStruct("Shared", { answer: Schema.Number }),
    Schema.TaggedStruct("Unanswered", { reason: Schema.String }),
  ]),
  initial: () => ({ phase: "answer" }),
});

/** Creates the delivery as a background task of the conversation, inside the caller's commit. */
export const deliverReply = (request: ReplyRequest) =>
  DeliverReply.create(request, { ownership: { kind: "conversation" }, background: true });

export const DeliverReplyLive = DeliverReply.toLayer(
  Effect.gen(function* () {
    const webhook = yield* Webhook;
    return {
      phases: {
        answer: (task, run) =>
          Effect.gen(function* () {
            const conversation = yield* run.requireConversation(run.conversationId);
            const submission = yield* conversation.submit({
              type: "input",
              content: task.input.content,
              requestId: task.input.requestId,
              whenBusy: task.input.whenBusy,
            });
            const settled = yield* submission.await;
            if (settled.status !== "done" || settled.type === "write") {
              const reason = settled.status === "unanswered" ? settled.reason : "no answer";
              return yield* run.complete({ _tag: "Unanswered", reason });
            }
            const answer = settled.answer;
            yield* run.commit(
              Effect.gen(function* () {
                const deliveries = yield* Deliveries.get(run.conversationId);
                deliveries.claims[String(answer)] ??= run.taskId;
                return deliveries.claims[String(answer)] === run.taskId
                  ? Task.next({ phase: "send", answer, attempt: 0 } as const)
                  : Task.complete({ _tag: "Shared", answer } as const);
              }),
            );
          }),
        send: (task, run) =>
          Effect.gen(function* () {
            const { answer, attempt } = task.state.checkpoint;
            const entry = yield* run.entry(EntryId.make(answer));
            // Never sent empty: the webhook would keep an empty reply under this answer's key for good.
            if (Option.isNone(entry)) {
              return yield* run.fail(`answer entry ${answer} is not visible from the conversation`);
            }
            const text = (entry.value.model ?? []).map(messageText).join("\n").trim();
            // The answer's entry ID is durable and unique in this object; the address makes it unique at the webhook.
            const key = `followup:${task.input.to}:answer:${answer}`;
            const sent = yield* webhook
              .deliver(key, { to: task.input.to, kind: task.input.kind, text })
              .pipe(Effect.result);
            if (Result.isSuccess(sent)) {
              const deliveryId = sent.success.deliveryId;
              return yield* run.complete(
                { _tag: "Delivered", answer, deliveryId },
                Effect.map(Deliveries.get(run.conversationId), (deliveries) => {
                  deliveries.delivered[String(answer)] = { deliveryId, key };
                }),
              );
            }
            if (attempt + 1 >= MAX_ATTEMPTS) {
              return yield* run.fail(
                `webhook refused ${MAX_ATTEMPTS} times: ${sent.failure.message}`,
              );
            }
            const now = yield* run.now;
            const backoff = Math.min(BACKOFF_BASE_MS * 2 ** attempt, BACKOFF_MAX_MS);
            yield* run.sleepUntil(now + backoff, { phase: "send", answer, attempt: attempt + 1 });
          }),
      },
      abort: (_task, run) => run.aborted,
    };
  }),
);

/** The text of an assistant message; empty for any other. */
const messageText = (message: Message): string => {
  if (message.role !== "assistant") return "";
  if (Predicate.isString(message.content)) return message.content;
  return message.content.flatMap((part) => (part.type === "text" ? [part.text] : [])).join("");
};

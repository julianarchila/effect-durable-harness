/**
 * `followup.follow-up`: created with each inbound message, it sleeps until the
 * contact has been silent for the configured delay. Its `sleep` phase ends asleep (`sleepUntil`), holding no
 * invocation: the object's alarm sits at the deadline, not at the keep-alive, and the object may be evicted
 * meanwhile. On waking, one commit checks whether a newer inbound arrived and, if not,
 * creates a `DeliverReply` and completes the task, so the agent drafts a follow-up that goes out to the webhook.
 */
import * as Task from "@effect-durable-harness/core/Task";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { followUpInput } from "../../../protocol.ts";
import { ContactState } from "../documents.ts";
import { deliverReply } from "./deliver-reply.ts";

export const FollowUp = Task.make("followup.follow-up", {
  version: 1,
  input: Schema.Struct({
    /** The inbound message this follow-up answers a silence after; a newer one supersedes it. */
    providerMessageId: Schema.String,
    receivedAt: Schema.Number,
    wakeAt: Schema.Number,
    to: Schema.String,
  }),
  checkpoint: Schema.Union([
    Schema.Struct({ phase: Schema.Literal("sleep") }),
    Schema.Struct({ phase: Schema.Literal("check") }),
  ]),
  result: Schema.Struct({ _tag: Schema.Literals(["Sent", "Superseded"]) }),
  initial: () => ({ phase: "sleep" }),
});

export const FollowUpLive = FollowUp.toLayer({
  phases: {
    sleep: (task, run) => run.sleepUntil(task.input.wakeAt, { phase: "check" }),
    // The check and the delivery are one commit: a `receive` commit lands either before it, and the follow-up is
    // superseded, or after it, and the new message is already behind the follow-up. Read outside the commit, the
    // check could pass and a new message land before the delivery was created.
    check: (task, run) =>
      run.commit(
        Effect.gen(function* () {
          const contact = yield* ContactState.get(run.conversationId);
          if (contact.latestInbound?.providerMessageId !== task.input.providerMessageId) {
            return Task.complete({ _tag: "Superseded" } as const);
          }
          yield* deliverReply({
            // From this task's ID, which a rerun keeps; inbound request IDs start with `inbound:` (`receive`).
            requestId: `follow-up:${run.key("input")}`,
            content: followUpInput(task.input.wakeAt - task.input.receivedAt),
            whenBusy: "followUp",
            to: task.input.to,
            kind: "follow-up",
          });
          return Task.complete({ _tag: "Sent" } as const);
        }),
      ),
  },
  abort: (_task, run) => run.aborted,
});

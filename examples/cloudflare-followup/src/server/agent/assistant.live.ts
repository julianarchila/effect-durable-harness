/**
 * The handlers of every task `AssistantHarness` declares. A missing handler stays in the agent's requirements and
 * fails to compile where `Contact.ts` provides this Layer. `DeliverReplyLive` requires the `Webhook`.
 */
import * as Layer from "effect/Layer";
import { DeliverReplyLive } from "./tasks/deliver-reply.ts";
import { FollowUpLive } from "./tasks/follow-up.ts";

export const AssistantLive = Layer.mergeAll(DeliverReplyLive, FollowUpLive);

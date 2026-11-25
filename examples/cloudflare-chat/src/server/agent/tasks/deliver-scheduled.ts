/**
 * `chat.deliver-scheduled`: delivers an instruction to its conversation at `runAt`, then, when it recurs, schedules
 * its successor in the same commit. It sleeps until then even with every browser closed.
 */
import { ConversationId } from "@effect-durable-harness/core/Ids";
import * as Task from "@effect-durable-harness/core/Task";
import * as Clock from "effect/Clock";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import { scheduledInput } from "../../../protocol.ts";

export const Recurrence = Schema.Struct({
  unit: Schema.Literals(["minute", "hour", "day", "week"]),
  interval: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 10_000 })),
});
type Recurrence = typeof Recurrence.Type;

const UNIT_MS: Record<Recurrence["unit"], number> = {
  minute: 60_000,
  hour: 3_600_000,
  day: 86_400_000,
  week: 604_800_000,
};

/** One scheduled delivery: the task's input. */
export const Scheduled = Schema.Struct({
  id: Schema.String,
  conversationId: Schema.Number,
  instruction: Schema.String,
  runAt: Schema.Number,
  recurrence: Schema.optionalKey(Recurrence),
});
export type Scheduled = typeof Scheduled.Type;

export const DeliverScheduled = Task.make("chat.deliver-scheduled", {
  version: 1,
  input: Scheduled,
  checkpoint: Schema.Union([
    Schema.Struct({ phase: Schema.Literal("wait") }),
    Schema.Struct({ phase: Schema.Literal("deliver") }),
  ]),
  result: Schema.Null,
  initial: () => ({ phase: "wait" }),
});

/** Creates the delivery as a background task of its conversation, inside the caller's commit. */
export const scheduleDelivery = (scheduled: Scheduled) =>
  DeliverScheduled.create(scheduled, {
    ownership: { kind: "conversation" },
    conversationId: ConversationId.make(scheduled.conversationId),
    background: true,
  });

export const DeliverScheduledLive = DeliverScheduled.toLayer({
  phases: {
    // The phase ends asleep, holding no invocation: the object's alarm sits at `runAt` and the object may be
    // evicted; `deliver` runs once the alarm resumes the Session.
    wait: (task, run) => run.sleepUntil(task.input.runAt, { phase: "deliver" }),
    deliver: (task, run) =>
      Effect.gen(function* () {
        const conversation = yield* run.conversation(
          ConversationId.make(task.input.conversationId),
        );
        if (Option.isNone(conversation)) return yield* run.complete(null);
        // The schedule's ID is the request ID, so a rerun after a crash gets the same submission back.
        const submission = yield* conversation.value.submit({
          type: "input",
          content: scheduledInput(task.input.instruction),
          requestId: task.input.id,
          whenBusy: "followUp",
        });
        yield* submission.await;
        const recurrence = task.input.recurrence;
        if (recurrence === undefined) return yield* run.complete(null);
        const every = recurrence.interval * UNIT_MS[recurrence.unit];
        const now = yield* Clock.currentTimeMillis;
        const missed = Math.max(0, Math.floor((now - task.input.runAt) / every));
        yield* run.complete(
          null,
          scheduleDelivery({
            ...task.input,
            id: `${task.input.id}+${missed + 1}`,
            runAt: task.input.runAt + (missed + 1) * every,
          }),
        );
      }),
  },
  abort: (_task, run) => run.aborted,
});

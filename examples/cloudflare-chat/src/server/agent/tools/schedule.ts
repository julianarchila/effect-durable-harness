/**
 * `schedule` (replay-safe): delivers an instruction to this conversation later, by a `chat.deliver-scheduled` task.
 * The task is created in the same commit as a task-scoped intent document, so a replay after a crash finds what the
 * first run scheduled instead of scheduling again.
 */
import * as AgentTool from "@effect-durable-harness/core/AgentTool";
import * as Doc from "@effect-durable-harness/core/Doc";
import * as Clock from "effect/Clock";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import type * as Types from "effect/Types";
import { MAX_CONTENT_CHARACTERS } from "@effect-durable-harness/host/SessionProtocol";
import { Recurrence, Scheduled, scheduleDelivery } from "../tasks/deliver-scheduled.ts";
import { ToolFailed } from "../support.ts";

const MAX_DELAY_SECONDS = 366 * 24 * 60 * 60;

/**
 * What a `schedule` call created, written in the commit that creates the task. Not `call.step`: a step records in
 * a commit of its own, so a crash between the two could schedule twice.
 */
const ScheduleIntent = Doc.make("chat.schedule-intent", {
  checkpointWhen: () => false,
  version: 1,
  scope: "task",
  schema: Schema.Struct({ scheduled: Schema.optionalKey(Scheduled) }),
  initial: () => ({}),
});

export const Schedule = AgentTool.make("schedule", {
  description:
    "Schedule an instruction to be delivered to this conversation at a later time. The conversation resumes then " +
    "with the instruction as its input and carries it out with its normal tools. Use this whenever the user wants " +
    "something to happen later; scheduling is the complete response to such a request.",
  parameters: Schema.Struct({
    instruction: Schema.String.check(
      Schema.isMinLength(1),
      Schema.isMaxLength(MAX_CONTENT_CHARACTERS),
    ).annotate({
      description:
        "What to do when the schedule fires, phrased as a self-contained request without timing words, e.g. 'Check the weather in London'",
    }),
    delaySeconds: Schema.optional(
      Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: MAX_DELAY_SECONDS })).annotate({
        description: "Run after this many seconds. Use for relative times such as 'in 10 minutes'",
      }),
    ),
    at: Schema.optional(
      Schema.String.annotate({
        description:
          "Run at this ISO 8601 date-time with a timezone offset, e.g. 2026-10-01T09:00:00+02:00. Provide exactly one of delaySeconds or at",
      }),
    ),
    recurrence: Schema.optional(Recurrence),
  }),
  details: Schema.Struct({ id: Schema.String, instruction: Schema.String, runAt: Schema.String }),
  replay: "safe",
});

export const ScheduleLive = Schedule.toLayer((args, call) =>
  Effect.gen(function* () {
    const now = yield* Clock.currentTimeMillis;
    // A replay after a crash finds what the first run committed.
    const existing = Option.flatMap(yield* ScheduleIntent.read(call.taskId), (intent) =>
      Option.fromUndefinedOr(intent.scheduled),
    );
    if (Option.isSome(existing)) return scheduledResult(existing.value, now);
    const requested: Types.Mutable<Scheduled> = {
      id: `schedule:${call.taskId}`,
      conversationId: call.conversationId,
      instruction: args.instruction,
      runAt: yield* resolveRunAt(args, now),
    };
    if (args.recurrence !== undefined) requested.recurrence = args.recurrence;
    const persisted = yield* call.commit(
      Effect.gen(function* () {
        const intent = yield* ScheduleIntent.get(call.taskId);
        if (intent.scheduled !== undefined) return intent.scheduled;
        intent.scheduled = requested;
        yield* scheduleDelivery(requested);
        return requested;
      }),
    );
    return scheduledResult(persisted, now);
  }),
);

const resolveRunAt = (
  args: { readonly delaySeconds?: number | undefined; readonly at?: string | undefined },
  now: number,
) => {
  if ((args.delaySeconds === undefined) === (args.at === undefined)) {
    return Effect.fail(new ToolFailed({ message: "Provide exactly one of delaySeconds or at" }));
  }
  if (args.delaySeconds !== undefined) return Effect.succeed(now + args.delaySeconds * 1000);
  const at = args.at ?? "";
  const runAt = /(?:Z|[+-]\d{2}:\d{2})$/iu.test(at) ? Date.parse(at) : Number.NaN;
  if (!Number.isFinite(runAt)) {
    return Effect.fail(
      new ToolFailed({ message: "at must be an ISO 8601 date-time with a timezone offset" }),
    );
  }
  if (runAt <= now) return Effect.fail(new ToolFailed({ message: "at must be in the future" }));
  return Effect.succeed(runAt);
};

const encodeJson = Schema.encodeSync(Schema.fromJsonString(Schema.Json));

const scheduledResult = (scheduled: Scheduled, now: number) => {
  const runAt = DateTime.formatIso(DateTime.makeUnsafe(scheduled.runAt));
  const seconds = Math.max(0, Math.round((scheduled.runAt - now) / 1000));
  return AgentTool.text(
    `Scheduled. At ${runAt} (in ${seconds} seconds) this conversation will receive: ${encodeJson(scheduled.instruction)}. Nothing further is needed now.`,
    { details: { id: scheduled.id, instruction: scheduled.instruction, runAt } },
  );
};

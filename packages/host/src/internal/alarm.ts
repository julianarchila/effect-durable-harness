/**
 * The host's alarm policy, as pure functions of the kernel's wake, the clock
 * and the alarm the object has. The host applies their answer; it never decides elsewhere.
 */
import type { StorageWrite } from "@effect-durable-harness/core/AgentStorage";
import type { Wake } from "@effect-durable-harness/core/SessionWakeups";
import * as Option from "effect/Option";

/** What to do with the object's alarm. */
export type AlarmAction =
  | { readonly _tag: "Keep" }
  | { readonly _tag: "Set"; readonly at: number }
  | { readonly _tag: "Delete" };

/** The clock and the alarm the object has. */
export interface AlarmInput {
  readonly now: number;
  readonly current: Option.Option<number>;
  readonly keepAliveMs: number;
}

const keep: AlarmAction = { _tag: "Keep" };

/**
 * An alarm no later than the keep-alive: the current one when it is already that close (or due), so changes while
 * work stays active never push it later: it fires every keep-alive whatever the progress.
 */
export const earlyAlarm = ({ now, current, keepAliveMs }: AlarmInput): AlarmAction => {
  const limit = now + keepAliveMs;
  return Option.isSome(current) && current.value <= limit ? keep : { _tag: "Set", at: limit };
};

/**
 * The alarm for a wake:
 *
 * - active work, or work due now: the keep-alive, or a sleep's deadline before it. Due work runs in memory once
 *   scheduling is resumed; the alarm only covers a crash before it does. A sleep that comes due while other work
 *   is active needs the alarm too: with the host's wakeups the kernel arms no timer for it.
 * - otherwise the next deadline, exactly: a Session that only sleeps keeps no keep-alive.
 * - otherwise no alarm.
 */
export const plan = (input: AlarmInput & { readonly wake: Wake }): AlarmAction => {
  const { wake, now, current } = input;
  const next = Option.getOrUndefined(wake.nextWakeAt);
  if (next !== undefined && next <= now) return earlyAlarm(input);
  if (wake.activeWork) {
    // The earlier of the keep-alive and the next deadline.
    const keepAliveMs =
      next === undefined ? input.keepAliveMs : Math.min(input.keepAliveMs, next - now);
    return earlyAlarm({ ...input, keepAliveMs });
  }
  if (next !== undefined) {
    return Option.isSome(current) && current.value === next ? keep : { _tag: "Set", at: next };
  }
  return Option.isSome(current) ? { _tag: "Delete" } : keep;
};

/**
 * The alarm for a wake when the alarm the engine reports is not trusted, so it is never kept: set afresh, or deleted
 * when nothing is due. At an instance's start, a Rivet activation that stopped between committing an alarm
 * and scheduling it leaves one stored that never fires; in `alarm()`, the alarm that fired is gone once the handler
 * returns. Deleting one that is already gone does nothing.
 */
export const planUnknown = (
  input: Omit<AlarmInput, "current"> & { readonly wake: Wake },
): AlarmAction => {
  const action = plan({ ...input, current: Option.none() });
  return action._tag === "Keep" ? { _tag: "Delete" } : action;
};

/** Whether a storage batch leaves a task able to run: the commits that must carry an alarm. */
export const writesLiveTask = (writes: ReadonlyArray<StorageWrite>): boolean =>
  writes.some((write) => write.type === "task" && write.value.state.status !== "terminal");

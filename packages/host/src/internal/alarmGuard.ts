/**
 * Every write of the instance's alarm, ordered so that a live task never goes without one.
 *
 * A storage batch that leaves a task able to run arms the alarm before it is committed. If the instance dies right
 * after the commit, before the kernel reports its wake, that alarm still restarts it. Arming the alarm inside the
 * commit's own transaction would be atomic on Cloudflare's storage, but Rivet schedules an alarm only after its
 * transaction commits, so a crash between the two would leave an alarm stored and never scheduled. Arming first holds
 * on every engine: a crash before the commit leaves an early alarm that finds nothing to do, which the host already
 * tolerates (the alarm only resumes).
 *
 * A wake computed before such a commit does not know its task. Applied after the arming, it could delete the alarm
 * or move it past the keep-alive, and a crash before the kernel reports the next wake would strand the task. So
 * alarm writes go one at a time, and a wake that a live commit may have overtaken is applied as active work: the
 * alarm stays within the keep-alive until the kernel reports a wake that knows the commit.
 *
 * The kernel adopts a commit some pauses after storage returns, so a wake computed in between looks fresh by the
 * counts and may still miss the task. The first wake applied after a live commit started therefore never deletes
 * the alarm: where it would, the alarm stays within the keep-alive. It still sets a deadline, so a Session that
 * only sleeps keeps its alarm at the deadline. It costs at most one early keep-alive alarm after a burst of live
 * commits, which finds nothing to do.
 */
import { AgentStorage } from "@effect-durable-harness/core/AgentStorage";
import type { Wake } from "@effect-durable-harness/core/SessionWakeups";
import * as Effect from "effect/Effect";
import * as Semaphore from "effect/Semaphore";
import { earlyAlarm, plan, planUnknown, writesLiveTask } from "./alarm.ts";
import type { AlarmStore } from "./alarmStore.ts";

/** The commits a wake was computed after: taken when the wake is computed, or before it is read. */
export interface Seen {
  readonly armed: number;
  readonly committing: boolean;
}

export interface AlarmGuard {
  /** What the commits have done so far, to take before computing or reading a wake. */
  readonly observe: () => Seen;
  /**
   * Applies the alarm policy to `wake`, after the commits in `seen`. `stored`: the engine's alarm is the one this
   * instance set. `unknown`: it is set anew, or deleted when nothing is due. A failed write dies.
   */
  readonly apply: (wake: Wake, seen: Seen, current: "stored" | "unknown") => Effect.Effect<void>;
  /** `storage` whose commits that write a live task first bring the alarm within the keep-alive. */
  readonly wrap: (storage: AgentStorage["Service"]) => AgentStorage["Service"];
}

export const make = (alarms: AlarmStore, keepAliveMs: number): AlarmGuard => {
  const writes = Semaphore.makeUnsafe(1);
  // Live commits that started, and those still running; a wake taken before one started, or while one ran,
  // may not know its task.
  // A failed arm counts too: the commit fails, and the next wake keeps the alarm once, which is harmless.
  let armed = 0;
  let committing = 0;
  // `armed` as the last wake applied had seen it: a wake applied while `armed` differs is the first since a live
  // commit, which the kernel may not have adopted when it computed the wake, so it does not delete the alarm.
  let appliedAfter = 0;

  const apply = (wake: Wake, seen: Seen, current: "stored" | "unknown") =>
    writes
      .withPermits(1)(
        Effect.gen(function* () {
          const overtaken = seen.committing || committing > 0 || seen.armed !== armed;
          const firstSinceCommit = armed !== appliedAfter;
          appliedAfter = seen.armed;
          const now = yield* alarms.now;
          const stored = current === "stored" ? yield* alarms.get : undefined;
          const decide = (assumed: Wake) =>
            stored === undefined
              ? planUnknown({ wake: assumed, now, keepAliveMs })
              : plan({ wake: assumed, now, current: stored, keepAliveMs });
          const asActive = { ...wake, activeWork: true };
          const action = overtaken ? decide(asActive) : decide(wake);
          yield* alarms.apply(
            firstSinceCommit && action._tag === "Delete" ? decide(asActive) : action,
          );
        }),
      )
      .pipe(Effect.orDie);

  const arm = writes.withPermits(1)(
    Effect.gen(function* () {
      yield* alarms.apply(
        earlyAlarm({ now: yield* alarms.now, current: yield* alarms.get, keepAliveMs }),
      );
    }),
  );

  return {
    observe: () => ({ armed, committing: committing > 0 }),
    apply,
    // A failure to arm fails the commit before anything is written.
    wrap: (storage) =>
      AgentStorage.of({
        ...storage,
        commit: (batches, receive) =>
          batches.some(writesLiveTask)
            ? Effect.suspend(() => {
                armed++;
                committing++;
                return Effect.andThen(arm, storage.commit(batches, receive)).pipe(
                  Effect.ensuring(
                    Effect.sync(() => {
                      committing--;
                    }),
                  ),
                );
              })
            : storage.commit(batches, receive),
      }),
  };
};

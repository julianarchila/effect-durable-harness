/**
 * How the kernel tells its host when to wake the Session.
 *
 * A host whose process can be evicted, such as a Durable Object, needs two
 * signals from the kernel: when the next work is due (`nextWakeAt`), and
 * whether work is running that eviction would lose (`activeWork`). With a
 * host here, the kernel calls `SessionWakeups.schedule` after every change,
 * off the Session line and in order, and the host calls `Harness.resume` when
 * its wakeup fires, which makes due sleeps runnable. Without one, the default, the kernel keeps a `Clock` sleep for
 * the earliest sleeping task while scheduling runs, which is right wherever
 * the process stays alive, as in Node and in tests. Arming an alarm atomically
 * with a commit is the host's job.
 *
 * @since 0.0.0
 */
import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";
import type * as Option from "effect/Option";

/**
 * When the Session needs to run next.
 *
 * @category models
 * @since 0.0.0
 */
export interface Wake {
  /**
   * Wall-clock milliseconds of the next due work: now when a task can run now (a pending task that is not
   * blocked, a due sleep, a waiting task whose `on` is settled, an abort-marked task whose owned work
   * drained) or a reconcile is due, otherwise the earliest `until` of a sleeping task; none when nothing is
   * due. Blocked tasks, `completing` tasks and waits on live tasks count for neither.
   */
  readonly nextWakeAt: Option.Option<number>;
  /**
   * Whether some invocation is running, also one that waits in memory for a task (`awaitTask`, `abortOwned`):
   * losing it is a crash, which reruns its phase and counts a restart. Work that waits durably holds no
   * invocation: a sleeping task, a `waiting` task, a parked tool call.
   */
  readonly activeWork: boolean;
}

/**
 * What a host provides to be told when to wake the Session.
 *
 * @category models
 * @since 0.0.0
 */
export interface WakeupHost {
  /** Called off the Session line, in order, with the latest wake; a failure is reported and dropped. */
  readonly schedule: (wake: Wake) => Effect.Effect<void>;
}

/**
 * The host capability the kernel calls whenever `Wake` changes. Its default
 * is no host: the kernel then wakes due sleeps itself with a `Clock` sleep. A
 * host overrides it with `Layer.succeed(SessionWakeups, ...)` and calls
 * `Harness.resume` when its wakeup fires.
 *
 * @category services
 * @since 0.0.0
 */
// Without a host, the earliest sleep is a `Clock` sleep on a fiber of the Harness's scope, so `TestClock` drives it and close ends it.
export const SessionWakeups: Context.Reference<WakeupHost | undefined> = Context.Reference<
  WakeupHost | undefined
>("@effect-durable-harness/core/SessionWakeups", {
  defaultValue: () => undefined,
});

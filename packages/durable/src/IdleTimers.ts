/**
 * Whether the kernel keeps timers for its idle caches.
 *
 * Two caches expire by idleness: the Session's loaded documents and the scheduler's kept context reads. Where
 * timers can be unreferenced the kernel schedules one that never keeps the process alive; where they cannot, as in
 * Cloudflare Workers, it keeps none, and only commits evict documents and only task changes expire contexts. Effect has no unreferenced timers, so the host says which branch applies: the default
 * keeps the timers, and a host where a pending timer holds the process, such as a Durable Object that would
 * otherwise not hibernate, turns them off.
 *
 * @since 0.0.0
 */
import * as Context from "effect/Context";

/**
 * The host's choice of idle-cache timers.
 *
 * @category models
 * @since 0.0.0
 */
export interface IdleTimersHost {
  /** Keep a timer for each idle cache's next expiry. When `false`, only commits evict documents and only task changes expire contexts. */
  readonly enabled: boolean;
}

/**
 * Read when the Session and the Harness are built. Default: timers on.
 *
 * @category services
 * @since 0.0.0
 */
export const IdleTimers: Context.Reference<IdleTimersHost> = Context.Reference<IdleTimersHost>(
  "@effect-durable-harness/core/IdleTimers",
  { defaultValue: () => ({ enabled: true }) },
);

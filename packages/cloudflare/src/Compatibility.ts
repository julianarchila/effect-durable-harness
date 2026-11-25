/**
 * The Cloudflare compatibility settings a Worker hosting effect-durable-harness objects uses.
 *
 * The compatibility date belongs to the Worker that hosts the Durable Object class, so the consumer spreads these
 * settings into its Worker's props:
 *
 * ```ts
 * export default class Api extends Cloudflare.Worker<Api>()(
 *   "Api",
 *   { main: import.meta.url, compatibility: compatibility() },
 *   ...
 * ) {}
 * ```
 *
 * **Why a date before `2026-10-01`.** From that date a pending timer, `waitUntil` or service binding call holds a
 * Durable Object in memory for up to 15 minutes each, billed as duration.
 * A timer inside a task's phase (`Effect.sleep`, `run.setTimer`) would then keep an idle object awake; a durable
 * `sleepUntil` holds none. Before it, only outbound `fetch`, TCP and outbound
 * WebSockets hold the object, which is what the host's keep-alive was measured against. (The host turns
 * the kernel's idle-cache timers off under any date: they would keep an object with open sockets from
 * hibernating.) Moving the date past `2026-10-01` means adding the flag
 * `durable_object_io_tasks_do_not_prevent_eviction` here, and a deployed run of the keep-alive test.
 *
 * @since 0.0.0
 */

/**
 * A Worker's compatibility settings, as Alchemy's Worker props take them.
 *
 * @category models
 * @since 0.0.0
 */
export interface CompatibilitySettings {
  readonly date: string;
  /** Extend Alchemy's default flags. */
  readonly flags: Array<string>;
}

/**
 * The compatibility settings for the Worker that hosts effect-durable-harness objects: the date the host was measured with, and
 * the consumer's own flags, which extend Alchemy's defaults.
 *
 * @category constructors
 * @since 0.0.0
 */
export const compatibility = (
  options: { readonly flags?: ReadonlyArray<string> | undefined } = {},
): CompatibilitySettings => ({
  date: "2026-08-31",
  flags: [...(options.flags ?? [])],
});

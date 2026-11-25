/**
 * `@effect-durable-harness/rivet`: hosts `@effect-durable-harness/core` sessions inside Rivet Durable Objects through
 * Alchemy: the engine-neutral host of `@effect-durable-harness/host` bound to `Rivet.DurableObjectState`, on a
 * Rivet control plane and pools on any cloud Alchemy deploys to. A browser imports the client modules from
 * `@effect-durable-harness/host` by path, as on Cloudflare.
 *
 * @since 0.0.0
 */
export * as Host from "./Host.ts";

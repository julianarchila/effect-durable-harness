/**
 * `@effect-durable-harness/cloudflare`: hosts `@effect-durable-harness/core` sessions inside Cloudflare Durable Objects
 * through Alchemy: the engine-neutral host of
 * `@effect-durable-harness/host` bound to `Cloudflare.DurableObjectState`. A browser imports the client modules from
 * `@effect-durable-harness/host` by path (`/SessionClient`, `/ConversationState`, `/SessionProtocol`, `/View`,
 * `/react`), so its bundle does not include the host.
 *
 * @since 0.0.0
 */
export { compatibility } from "./Compatibility.ts";
export * as Host from "./Host.ts";

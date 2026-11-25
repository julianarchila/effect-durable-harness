/**
 * The built-in provider session document, `core.provider`: a
 * stable provider-facing identity of one conversation, sent as the request's
 * `sessionId` for provider-side caching. Every fork starts with a fresh one.
 *
 * @since 0.0.0
 */
import * as Schema from "effect/Schema";
import * as Doc from "./Doc.ts";

/**
 * The value of `core.provider`.
 *
 * @category models
 * @since 0.0.0
 */
export const ProviderState = Schema.Struct({ sessionId: Schema.String });
/**
 * The type of `ProviderState`.
 *
 * @category models
 * @since 0.0.0
 */
export type ProviderState = typeof ProviderState.Type;

/**
 * The `core.provider` document.
 *
 * @category documents
 * @since 0.0.0
 */
// Starts empty: the identity is a UUIDv7 whose randomness and time come from `Random` and `Clock`, which a
// synchronous `initial` cannot read, so and the creation hook, or the first request of a conversation that lacks one,
// writes the identity in the same commit.
export const ProviderDoc = Doc.make({
  kind: "core.provider",
  version: 1,
  scope: "conversation",
  history: "latest",
  fork: "initial",
  schema: ProviderState,
  initial: () => ({ sessionId: "" }),
  checkpointWhen: () => true,
});

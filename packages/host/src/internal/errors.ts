// The host's own errors, re-exported by `Host.ts`.
import * as Schema from "effect/Schema";

// One error for a missing conversation, the kernel's own: `Host.errors` matches errors by tag.
export { ConversationNotFound } from "@effect-durable-harness/core/SessionError";

/**
 * A request ID already identifies another command: a create or a fork resent with other arguments.
 *
 * @category errors
 * @since 0.0.0
 */
export class RequestConflict extends Schema.TaggedError<RequestConflict>()("RequestConflict", {
  requestId: Schema.String,
}) {
  override get message(): string {
    return `Request ${this.requestId} was already used for another command`;
  }
}

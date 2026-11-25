/**
 * The built-in inbox document, `core.inbox`: the submissions
 * waiting for a boundary, in ID order. A boundary places them and removes
 * them positionally.
 *
 * @since 0.0.0
 */
import * as Schema from "effect/Schema";
import * as Doc from "./Doc.ts";
import { SubmissionIdValue } from "./Ids.ts";
import { UserContent } from "./Message.ts";

/**
 * The entry an input is placed as, instead of `core.user`: an application kind
 * and data the transcript shows. Its model message is still the input's user
 * message.
 *
 * @category models
 * @since 0.0.0
 */
export const InputEntry = Schema.Struct({
  kind: Schema.String,
  data: Schema.optionalKey(Schema.Json),
});
/**
 * The type of `InputEntry`.
 *
 * @category models
 * @since 0.0.0
 */
export type InputEntry = typeof InputEntry.Type;

/**
 * A queued submission: user input for a run, or an entry write.
 *
 * @category models
 * @since 0.0.0
 */
export const InboxItem = Schema.Union([
  Schema.Struct({
    id: SubmissionIdValue,
    mode: Schema.Literals(["steer", "followUp"]),
    content: UserContent,
    entry: Schema.optionalKey(InputEntry),
  }),
  /** `entry` is the write's `EntryDraft`, stored as plain JSON. */
  Schema.Struct({ id: SubmissionIdValue, mode: Schema.Literal("write"), entry: Schema.JsonObject }),
]);
/**
 * The type of `InboxItem`.
 *
 * @category models
 * @since 0.0.0
 */
export type InboxItem = typeof InboxItem.Type;

/**
 * The value of `core.inbox`.
 *
 * @category models
 * @since 0.0.0
 */
export const InboxState = Schema.Struct({ items: Schema.Array(InboxItem) });
/**
 * The type of `InboxState`.
 *
 * @category models
 * @since 0.0.0
 */
export type InboxState = typeof InboxState.Type;

/**
 * The `core.inbox` document; a complete base whenever it is empty.
 *
 * @category documents
 * @since 0.0.0
 */
export const InboxDoc = Doc.make({
  kind: "core.inbox",
  version: 1,
  scope: "conversation",
  history: "latest",
  fork: "initial",
  schema: InboxState,
  initial: () => ({ items: [] }),
  checkpointWhen: (value) => value.items.length === 0,
});

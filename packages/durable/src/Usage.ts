/**
 * The built-in usage ledger, `core.usage`: one conversation's
 * own spend, by model and by tool. `Harness.usage` sums every conversation's.
 *
 * @since 0.0.0
 */
import * as Schema from "effect/Schema";
import * as Doc from "./Doc.ts";
import { Usage } from "./Message.ts";

/**
 * The value of `core.usage`.
 *
 * @category models
 * @since 0.0.0
 */
export const UsageState = Schema.Struct({
  /** Assistant entries, keyed `provider/modelId`. */
  models: Schema.Record(Schema.String, Usage),
  /** Tool results, keyed by tool name. */
  tools: Schema.Record(Schema.String, Usage),
});
/**
 * The type of `UsageState`.
 *
 * @category models
 * @since 0.0.0
 */
export type UsageState = typeof UsageState.Type;

/**
 * The `core.usage` document; a fork starts with an empty ledger.
 *
 * @category documents
 * @since 0.0.0
 */
export const UsageDoc = Doc.make({
  kind: "core.usage",
  version: 1,
  scope: "conversation",
  history: "latest",
  fork: "initial",
  schema: UsageState,
  initial: () => ({ models: {}, tools: {} }),
  checkpointWhen: () => true,
});

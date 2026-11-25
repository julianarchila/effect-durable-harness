/**
 * Compaction: a built-in task that replaces an old prefix of a
 * conversation's model context with a summary entry, `core.compaction`, whose
 * `head` is the first entry it keeps verbatim. Raw history stays in storage.
 *
 * It starts three ways: `Conversation.compact()` (manual), a generation's
 * threshold check before a request (`threshold`, in the background or blocking
 * the generation), and a context-length error (`overflow`, blocking). Its
 * policy is `settings.compaction` (`Settings.CompactionPolicy`), its hook
 * `beforeCompact` (`Hook.compaction`), and its presentation
 * `core.live.compactions` and `core.live.compactionFailure` (`Live`).
 *
 * @since 0.0.0
 */
import * as Schema from "effect/Schema";
import { EntryIdValue, SubmissionIdValue } from "./Ids.ts";

/**
 * Why a compaction runs: `compact()`, a threshold in generation preparation, or a context overflow.
 *
 * @category models
 * @since 0.0.0
 */
export const CompactionReason = Schema.Literals(["manual", "threshold", "overflow"]);
/**
 * The type of `CompactionReason`.
 *
 * @category models
 * @since 0.0.0
 */
export type CompactionReason = typeof CompactionReason.Type;

/**
 * A compaction task's result: the `entryId` of a blocking compaction's summary, or the `submissionId` of a
 * conversation-owned compaction's summary write; both absent when nothing was compacted.
 *
 * @category models
 * @since 0.0.0
 */
export const CompactionResult = Schema.Struct({
  entryId: Schema.optionalKey(EntryIdValue),
  submissionId: Schema.optionalKey(SubmissionIdValue),
});
/**
 * The type of `CompactionResult`.
 *
 * @category models
 * @since 0.0.0
 */
export type CompactionResult = typeof CompactionResult.Type;

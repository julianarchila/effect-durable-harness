/**
 * Typed entry kinds. An entry kind names the
 * `kind` an entry is written with and types its `data`; `is` narrows a raw
 * entry by kind. The built-in kinds (`core.user`, `core.assistant`, …) are the
 * Harness's.
 *
 * @since 0.0.0
 */
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { ToolDiagnostic } from "./AgentTool.ts";
import { CompactionReason } from "./Compaction.ts";
import type { ConversationId, EntryId } from "./Ids.ts";
import type { EntryRecord } from "./Records.ts";
import { Tx, type TxWriteError } from "./Tx.ts";

const TypeId = "~@effect-durable-harness/core/Entry";

/**
 * An entry to append. The Session assigns `id`, `conversationId` and
 * `byTaskId`; `head: "self"` points the head at the new entry itself.
 *
 * @category models
 * @since 0.0.0
 */
export type EntryDraft = Omit<
  EntryRecord,
  "id" | "conversationId" | "byTaskId" | "head" | "data"
> & {
  readonly head?: EntryId | "self" | undefined;
  /** An `undefined` value is omitted, as `JSON.stringify` would. */
  readonly data?: Schema.Json | undefined;
};

/**
 * An entry whose `data` has type `D`; `never` means the kind carries no data.
 *
 * @category models
 * @since 0.0.0
 */
export type TypedEntry<D extends Schema.Json> = Omit<EntryRecord, "data"> &
  ([D] extends [never] ? { readonly data?: never } : { readonly data: D });

/**
 * An entry draft of one kind: the kind supplies `kind` and types `data`.
 *
 * @category models
 * @since 0.0.0
 */
export type TypedEntryDraft<D extends Schema.Json> = Omit<EntryDraft, "kind" | "data"> &
  ([D] extends [never] ? { readonly data?: never } : { readonly data: D });

/**
 * The fields of an entry draft besides its kind and data: what the model reads (`model`), the context it
 * starts (`head`) and its context edits.
 *
 * @category models
 * @since 0.0.0
 */
export type EntryOptions = Omit<EntryDraft, "kind" | "data">;

/**
 * What `draft` and `append` take after the conversation: the data, unless the kind carries none, then the
 * other fields of the entry.
 *
 * @category models
 * @since 0.0.0
 */
export type EntryContents<D extends Schema.Json> = [D] extends [never]
  ? [options?: EntryOptions]
  : [data: D, options?: EntryOptions];

/**
 * A typed entry kind.
 *
 * @category models
 * @since 0.0.0
 */
export interface EntryKind<D extends Schema.Json> {
  readonly [TypeId]: typeof TypeId;
  readonly kind: string;
  /** Narrows an entry to this kind, by `kind` alone. */
  readonly is: (entry: EntryRecord | undefined) => entry is TypedEntry<D>;
  /**
   * An entry of this kind to submit (`{ type: "write", entry }`) or to pass as an input entry.
   * `model` is what the model reads; without it the entry is for display and bookkeeping only.
   */
  readonly draft: (...contents: EntryContents<D>) => EntryDraft;
  /** Inside a commit: appends an entry of this kind to `conversationId` (`tx.appendEntry`). */
  readonly append: (
    conversationId: ConversationId,
    ...contents: EntryContents<D>
  ) => Effect.Effect<TypedEntry<D>, TxWriteError, Tx>;
}

/**
 * Options of `Entry.make` for a kind that carries data.
 *
 * @category models
 * @since 0.0.0
 */
export interface EntryKindOptions<D> {
  /** JSON schema of `data`; it types the payload. */
  readonly data: Schema.Codec<D, D>;
}

/**
 * Defines an entry kind. Without options the kind carries no data.
 *
 * **Gotchas**
 *
 * Throws a `TypeError` for an empty kind. The data schema types `data` but is
 * not checked when an entry is appended: the Session only requires strict JSON
 * there.
 *
 * @category constructors
 * @since 0.0.0
 */
export function make(kind: string): EntryKind<never>;
export function make<D extends Schema.Json>(
  kind: string,
  options: EntryKindOptions<D>,
): EntryKind<D>;
// The implementation is erased: `draft` and `append` make `EntryKind` invariant in `D`, so no typed signature
// fits both overloads. The overloads keep a kind with data from being made without its schema.
export function make<D extends Schema.Json>(
  kind: string,
  options?: EntryKindOptions<D>,
): EntryKind<D> | EntryKind<never> {
  if (kind.length === 0) {
    // A kind is a module constant: failing here is broken setup.
    throw new TypeError("Entry kind must be a non-empty string");
  }
  const draft = (...contents: EntryContents<D>): EntryDraft => {
    const [first, second] = contents;
    // A kind made without options carries no data and takes its entry options first.
    if (options === undefined) {
      // SAFETY: `EntryContents<never>` is `[options?: EntryOptions]`.
      return { ...(first as EntryOptions | undefined), kind };
    }
    // SAFETY: `EntryContents<D>` of a kind with data is `[data: D, options?: EntryOptions]`.
    return { ...(second as EntryOptions | undefined), kind, data: first as D };
  };
  const self: EntryKind<D> = {
    [TypeId]: TypeId,
    kind,
    is: (entry: EntryRecord | undefined): entry is TypedEntry<D> =>
      entry !== undefined && entry.kind === kind,
    draft,
    append: (conversationId, ...contents) =>
      Effect.gen(function* () {
        const tx = yield* Tx;
        // SAFETY: `draft` built an entry of this kind with data of type `D`, as the typed overload takes.
        const typed = draft(...contents) as never;
        return yield* tx.appendEntry(self, conversationId, typed);
      }),
  };
  return self;
}

// -----------------------------------------------------------------------------
// built-in kinds
// -----------------------------------------------------------------------------

/**
 * User input: `model` is `[UserMessage]`. Written by submissions.
 *
 * @category built-in
 * @since 0.0.0
 */
export const UserEntry = make("core.user");

/**
 * A provider result with any stop reason: `model` is `[AssistantMessage]`.
 * Written by generation.
 *
 * @category built-in
 * @since 0.0.0
 */
export const AssistantEntry = make("core.assistant");

/**
 * A positional prompt and tool change: `model` is `[SystemMessage]` with
 * empty `content`. Written by generation's preparation.
 *
 * @category built-in
 * @since 0.0.0
 */
export const SystemEntry = make("core.system");

/**
 * A tool result: `model` is `[ToolResultMessage]`, whose content ends with
 * the rendered diagnostics block; `data` holds the structured diagnostics,
 * possibly none.
 *
 * @category built-in
 * @since 0.0.0
 */
export const ToolResultEntry = make("core.tool-result", {
  data: Schema.Struct({ diagnostics: Schema.Array(ToolDiagnostic) }),
});

/**
 * The start of a new context: always `head: "self"`, with `model` absent for
 * a plain reset or `[UserMessage]` carrying the handoff text.
 *
 * @category built-in
 * @since 0.0.0
 */
export const ResetEntry = make("core.reset");

/**
 * A compaction summary: a head marker whose `head` is the first entry kept verbatim, with
 * `model` the summary as one `UserMessage`, and `data` what started the compaction. Written by the
 * compaction task.
 *
 * @category built-in
 * @since 0.0.0
 */
export const CompactionEntry = make("core.compaction", {
  data: Schema.Struct({ reason: CompactionReason }),
});

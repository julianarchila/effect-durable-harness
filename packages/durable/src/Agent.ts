/**
 * The per-conversation agent. A
 * conversation stores its choices by name in the built-in `core.agent`
 * document: a model, a thinking level, which extensions it selects, which of
 * their tools it enables. The Harness resolves them against its static
 * extensions and the settings into an `Agent` once per task phase.
 *
 * @since 0.0.0
 */
import type * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import type * as AgentTool from "./AgentTool.ts";
import * as Doc from "./Doc.ts";
import type * as Extension from "./Extension.ts";
import { ThinkingLevel } from "./Message.ts";
import { ModelRef } from "./Models.ts";
import type * as Section from "./Section.ts";
import type { CompactionSettings } from "./Settings.ts";
import type { ConversationId } from "./Ids.ts";
import * as internal from "./internal/agent.ts";
import type { Tx, TxDocError } from "./Tx.ts";

const CompactionFields = {
  enabled: Schema.optionalKey(Schema.Boolean),
  reserveTokens: Schema.optionalKey(Schema.Number),
  keepRecentTokens: Schema.optionalKey(Schema.Number),
  backgroundTokens: Schema.optionalKey(Schema.Number),
};

// `Settings.CompactionSettings` as stored: without undefined fields.
const StoredCompactionSettings = Schema.Struct({
  ...CompactionFields,
  modelOverrides: Schema.optionalKey(Schema.Record(Schema.String, Schema.Struct(CompactionFields))),
});

/**
 * What a conversation stores: names, not objects. An unset field follows the
 * host.
 *
 * @category models
 * @since 0.0.0
 */
export const AgentState = Schema.Struct({
  model: Schema.optionalKey(ModelRef),
  thinkingLevel: Schema.optionalKey(ThinkingLevel),
  /** An array selects exactly these extensions, in order; an object edits the default selection. */
  extensions: Schema.optionalKey(
    Schema.Union([
      Schema.Array(Schema.String),
      Schema.Struct({
        add: Schema.optionalKey(Schema.Array(Schema.String)),
        remove: Schema.optionalKey(Schema.Array(Schema.String)),
      }),
    ]),
  ),
  /** Filters the selected extensions' tools: an array enables exactly these, in order. */
  tools: Schema.optionalKey(
    Schema.Union([
      Schema.Array(Schema.String),
      Schema.Struct({ remove: Schema.Array(Schema.String) }),
    ]),
  ),
  /**
   * Filters the enabled tools the model may call; never widens. Unset: all but deferred ones. An array: exactly
   * these, in order, deferred ones too. An object: all but deferred ones and those in `remove`, in registry
   * order, followed by the deferred tools in `add` that are not in `remove`, in `add` order.
   */
  modelTools: Schema.optionalKey(
    Schema.Union([
      Schema.Array(Schema.String),
      Schema.Struct({
        remove: Schema.optionalKey(Schema.Array(Schema.String)),
        add: Schema.optionalKey(Schema.Array(Schema.String)),
      }),
    ]),
  ),
  /** Rendered after every extension section, as the section `instructions`. */
  instructions: Schema.optionalKey(Schema.String),
  /** Directory within the environment's file system. */
  cwd: Schema.optionalKey(Schema.String),
  /**
   * Tool rounds one run takes at most (`core.live.run.turns`): the round that reaches it runs its tools, then
   * the run ends `unanswered` with `turn_limit`. An integer, `1` or more; unset, no limit. Task-owned
   * children and forks inherit it.
   */
  maxTurns: Schema.optionalKey(Schema.Number),
  /**
   * This conversation's compaction settings over the Harness's, field by field: its `modelOverrides` entry,
   * then the Harness's, then its own fields, then the Harness-wide policy. Its `enabled: false` also beats the
   * Harness's model override. Task-owned children and forks inherit it.
   */
  compaction: Schema.optionalKey(StoredCompactionSettings),
});
/**
 * The type of `AgentState`.
 *
 * @category models
 * @since 0.0.0
 */
export type AgentState = typeof AgentState.Type;

/**
 * The built-in `core.agent` document: rewindable, so a fork starts from the
 * agent at its fork entry.
 *
 * @category documents
 * @since 0.0.0
 */
export const AgentDoc = Doc.make({
  kind: "core.agent",
  version: 1,
  scope: "conversation",
  history: "rewindable",
  fork: "asOf",
  schema: AgentState,
  initial: () => ({}),
  checkpointWhen: () => true,
});

/**
 * A change to `core.agent`: a given field replaces the stored one, `null`
 * clears it, `undefined` changes nothing. Objects are stored by name.
 *
 * @category models
 * @since 0.0.0
 */
export interface AgentChange {
  readonly model?: ModelRef | null | undefined;
  readonly thinkingLevel?: ThinkingLevel | null | undefined;
  readonly extensions?:
    | ReadonlyArray<Extension.Any>
    | {
        readonly add?: ReadonlyArray<Extension.Any> | undefined;
        readonly remove?: ReadonlyArray<Extension.Any> | undefined;
      }
    | null
    | undefined;
  readonly tools?:
    | ReadonlyArray<AgentTool.Any>
    | { readonly remove: ReadonlyArray<AgentTool.Any> }
    | null
    | undefined;
  /** Replaces the stored filter as a whole. */
  readonly modelTools?:
    | ReadonlyArray<AgentTool.Any>
    | {
        readonly remove?: ReadonlyArray<AgentTool.Any> | undefined;
        readonly add?: ReadonlyArray<AgentTool.Any> | undefined;
      }
    | null
    | undefined;
  readonly instructions?: string | null | undefined;
  readonly cwd?: string | null | undefined;
  /** Dies with a `RangeError` unless it is an integer `1` or more (or `null`). */
  readonly maxTurns?: number | null | undefined;
  /**
   * Replaces the stored settings as a whole, without their undefined fields; dies with a `RangeError` unless every
   * token count is an integer `0` or more.
   */
  readonly compaction?: CompactionSettings | null | undefined;
}

/**
 * A conversation's agent resolved against the Harness's extensions and the
 * settings.
 *
 * @category models
 * @since 0.0.0
 */
export interface Agent {
  readonly model?: ModelRef | undefined;
  readonly thinkingLevel: ThinkingLevel;
  readonly extensions: ReadonlyArray<Extension.Any>;
  /** The tools a request offers, in order: enabled, callable by the model, and selected by `modelTools`. */
  readonly tools: ReadonlyArray<AgentTool.Any>;
  /** Enabled deferred tools the model may call and is not offered yet, in registry order. */
  readonly deferred: ReadonlyArray<AgentTool.Any>;
  /** The tools nested calls (`ToolContext.executeTool`) resolve among, in order: enabled and callable by tools. */
  readonly callable: ReadonlyArray<AgentTool.Any>;
  /** Extension sections, then `instructions` when set. */
  readonly sections: ReadonlyArray<Section.Any>;
  readonly instructions?: string | undefined;
  readonly cwd?: string | undefined;
  /** Tool rounds one run takes at most; see `AgentState.maxTurns`. */
  readonly maxTurns?: number | undefined;
  /** This conversation's compaction settings over the Harness's; see `AgentState.compaction`. */
  readonly compaction?: CompactionSettings | undefined;
}

/**
 * The reserved section key of the agent's `instructions`.
 *
 * @category constants
 * @since 0.0.0
 */
export const INSTRUCTIONS_KEY = "instructions";

/**
 * One change to a conversation's `core.agent`, inside the caller's commit,
 * for example the commit that creates the conversation. `Conversation.configure` is the same change in a commit
 * of its own.
 *
 * @category constructors
 * @since 0.0.0
 */
export const configure = (
  conversationId: ConversationId,
  change: AgentChange,
): Effect.Effect<void, TxDocError, Tx> => internal.configure(conversationId, change);

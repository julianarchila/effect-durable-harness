/**
 * System prompt sections. The sections of a conversation's
 * agent render in order before each request; the generation task appends a
 * `core.system` entry with the sections that changed, so the transcript carries
 * the prompt the model saw. A section renders as an Effect; its requirements
 * bubble up to `Harness.layer`.
 *
 * @since 0.0.0
 */
import type * as Effect from "effect/Effect";
import type { Agent } from "./Agent.ts";
import type { ExecutionEnv } from "./ExecutionEnv.ts";
import type { ConversationId } from "./Ids.ts";
import type { DocSnapshot, DocSnapshotAsOf } from "./Session.ts";
import type { RuntimeDocumentError } from "./Task.ts";

/**
 * What a section renders from, for one request preparation.
 *
 * @category models
 * @since 0.0.0
 */
export interface PromptInput {
  readonly conversationId: ConversationId;
  /** The request's resolution; `agent.tools` are the tools offered in this request. */
  readonly agent: Agent;
  /** The environment built for this preparation; `undefined` without one, or when building it failed. */
  readonly env: ExecutionEnv | undefined;
  /** Sections already in effect after replaying the active transcript. */
  readonly shown: Readonly<Record<string, string>>;
  /** Committed document reads. */
  readonly snapshot: DocSnapshot<RuntimeDocumentError>;
  readonly snapshotAsOf: DocSnapshotAsOf<RuntimeDocumentError>;
}

/**
 * A section definition. `render` returns the text, or `undefined` to leave
 * the section out of this request.
 *
 * @category models
 * @since 0.0.0
 */
export interface SectionDefinition<E, R> {
  readonly key: string;
  readonly render: (input: PromptInput) => Effect.Effect<string | undefined, E, R>;
  /** Default true: wrap the text as `<key>\n...\n</key>`. */
  readonly tag?: boolean | undefined;
}

/**
 * A typed section token.
 *
 * @category models
 * @since 0.0.0
 */
export interface Section<E, Req> {
  readonly key: string;
  readonly definition: SectionDefinition<E, Req>;
  /** The services `render` requires; a type-level record only. */
  readonly [RequirementsOf]?: Req;
}

declare const RequirementsOf: unique symbol;

/**
 * Any section, whatever its types.
 *
 * @category models
 * @since 0.0.0
 */
export interface Any {
  readonly key: string;
  readonly definition: { readonly key: string; readonly tag?: boolean | undefined };
}

/**
 * The services a section's `render` requires.
 *
 * @category models
 * @since 0.0.0
 */
export type Requirements<T> =
  T extends Section<infer _E, infer Req> ? (unknown extends Req ? never : Req) : never;

/**
 * Options of `Section.make`.
 *
 * @category models
 * @since 0.0.0
 */
export interface SectionOptions {
  /** Default true: wrap the text as `<key>\n...\n</key>`. */
  readonly tag?: boolean | undefined;
}

/**
 * Defines a section. The key `instructions` is reserved for
 * the agent's own instructions.
 *
 * @category constructors
 * @since 0.0.0
 */
export const make = <E, R>(
  key: string,
  render: (input: PromptInput) => Effect.Effect<string | undefined, E, R>,
  options?: SectionOptions,
): Section<E, R> => ({
  key,
  definition: options?.tag === undefined ? { key, render } : { key, render, tag: options.tag },
});

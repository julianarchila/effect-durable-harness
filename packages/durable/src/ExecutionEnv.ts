/**
 * The execution environment: where a conversation's tools reach files and
 * processes. The host builds it with `Harness.layer`'s `env` for each use, from
 * the conversation's ID and its agent's `cwd`: for each tool call (`ToolContext.env`),
 * for each request preparation (`PromptInput.env`), and at each `TaskRuntime.env`.
 * A rerun after recovery gets the conversation's environment at that time.
 *
 * The kernel declares only `cwd`, the directory it builds from, which it reads
 * from the conversation's agent. An environment package declares the rest by
 * augmenting `ExecutionEnv` (module `@effect-durable-harness/core/ExecutionEnv`).
 *
 * @since 0.0.0
 */
import type * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import type { ConversationId } from "./Ids.ts";
import type { DocSnapshot, DocSnapshotAsOf } from "./Session.ts";
import type { RuntimeDocumentError } from "./Task.ts";

/**
 * A conversation's environment, as the host built it.
 *
 * @category models
 * @since 0.0.0
 */
export interface ExecutionEnv {
  /** The directory the environment runs in. */
  readonly cwd: string;
}

/**
 * What the host builds an environment from.
 *
 * @category models
 * @since 0.0.0
 */
export interface ExecutionEnvTarget {
  readonly conversationId: ConversationId;
  /** The conversation's agent's `cwd`, when it has one. */
  readonly cwd?: string | undefined;
  /** Committed document reads, for an application that keeps more than a directory per conversation. */
  readonly snapshot: DocSnapshot<RuntimeDocumentError>;
  readonly snapshotAsOf: DocSnapshotAsOf<RuntimeDocumentError>;
}

/**
 * Builds a conversation's environment, or `undefined` for none. Never called on the Session line; its
 * requirements are `Harness.layer`'s, and a `Scope` it requires is the Harness's. A failure `E`, or a defect,
 * becomes an `ExecutionEnvError`.
 *
 * @category models
 * @since 0.0.0
 */
export type ExecutionEnvBuilder<R = never, E = never> = (
  target: ExecutionEnvTarget,
) => Effect.Effect<ExecutionEnv | undefined, E, R>;

/**
 * The host's builder failed: its failure or defect is the `cause`, and the cause's message the `message`
 * (a tagged error without one is named by its `_tag`).
 *
 * @category errors
 * @since 0.0.0
 */
export class ExecutionEnvError extends Schema.TaggedError<ExecutionEnvError>()(
  "ExecutionEnvError",
  {
    message: Schema.String,
    cause: Schema.Defect(),
  },
) {}

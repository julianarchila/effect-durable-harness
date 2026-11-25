/**
 * Harness-wide run policy. The host passes a
 * synchronous reader, `HarnessOptions.settings`; the Harness reads it at every
 * decision that uses a setting and never copies it, so a host can change
 * settings while the Harness runs. Some readers run on
 * the Session line, which is why the reader is synchronous.
 *
 * @since 0.0.0
 */
import type * as Extension from "./Extension.ts";
import type { StreamOptions } from "./Models.ts";

/**
 * How many queued items of one mode a boundary places: the first, or all.
 *
 * @category models
 * @since 0.0.0
 */
export type QueueMode = "all" | "one-at-a-time";

/**
 * What a failed run does with the inputs queued behind it.
 *
 * @category models
 * @since 0.0.0
 */
export type AfterFailedRun = "keep" | "continue" | "withdraw";

/**
 * Whether the tools of one round run at once or one after another.
 *
 * @category models
 * @since 0.0.0
 */
export type ToolExecutionMode = "parallel" | "sequential";

/**
 * Durable generation attempt retries.
 *
 * @category models
 * @since 0.0.0
 */
export interface RetryPolicy {
  readonly enabled: boolean;
  /** Retries after the first attempt. */
  readonly maxRetries: number;
  readonly baseDelayMs: number;
  readonly maxAgentDelayMs?: number | undefined;
}

/**
 * Automatic compaction thresholds; manual compaction ignores `enabled`.
 *
 * @category models
 * @since 0.0.0
 */
export interface CompactionPolicy {
  /** Threshold and overflow compaction. */
  readonly enabled: boolean;
  /** Room kept free for the answer: generation blocks to compact above `contextWindow - reserveTokens`. */
  readonly reserveTokens: number;
  /** Approximate size of the recent context a summary keeps verbatim. */
  readonly keepRecentTokens: number;
  /** Background compaction starts `backgroundTokens` below the blocking threshold; `0` disables it. */
  readonly backgroundTokens: number;
}

/**
 * Harness-wide compaction settings. `modelOverrides` is keyed by exact
 * `"provider/modelId"`; each field it sets replaces the Harness-wide one for that model, so small- and
 * large-window models can share a Harness.
 *
 * @category models
 * @since 0.0.0
 */
export interface CompactionSettings extends PartialPolicy<CompactionPolicy> {
  readonly modelOverrides?:
    | { readonly [model: string]: PartialPolicy<CompactionPolicy> | undefined }
    | undefined;
}

/**
 * How often running progress is committed.
 *
 * @category models
 * @since 0.0.0
 */
export interface ProgressPolicy {
  /** Minimum pause between commits of the answer being generated. */
  readonly partialIntervalMs: number;
  /** Minimum pause between commits of running tool output; large commits pause in proportion. */
  readonly outputIntervalMs: number;
  /**
   * How long streamed progress may stay in memory before it is stored:
   * the window of every deferred commit without its own `withinMs`. `0` stores right after each commit;
   * `Infinity` waits for the next commit that reads `core.live`, or close.
   */
  readonly persistWithinMs: number;
}

/**
 * Every field of `A` optional; an explicitly undefined one keeps its default.
 *
 * @category models
 * @since 0.0.0
 */
export type PartialPolicy<A> = { readonly [K in keyof A]?: A[K] | undefined };

/**
 * What the host sets; every field is optional and falls back to its default.
 *
 * @category models
 * @since 0.0.0
 */
export interface HarnessSettings {
  /** Default extension selection; absent: every extension of the Harness, in order. */
  readonly extensions?: ReadonlyArray<Extension.Any> | undefined;
  readonly stream?: StreamOptions | undefined;
  readonly retry?: PartialPolicy<RetryPolicy> | undefined;
  readonly compaction?: CompactionSettings | undefined;
  readonly progress?: PartialPolicy<ProgressPolicy> | undefined;
  readonly toolExecution?: ToolExecutionMode | undefined;
  readonly steeringMode?: QueueMode | undefined;
  readonly followUpMode?: QueueMode | undefined;
  /** How long an idle conversation keeps its last context read; `0` drops it once idle. Default ten minutes. */
  readonly contextRetentionMs?: number | undefined;
  /**
   * How long an unobserved loaded document stays in memory after its last use. Default ten minutes; `0`
   * drops it right after use; `Infinity` keeps it. Anything else that is not milliseconds means the default.
   */
  readonly documentRetentionMs?: number | undefined;
  /**
   * What a run that fails with `model_error`, `no_model`, `interrupted`, `restart_limit`,
   * `nothing_to_answer` or `turn_limit` does with the inputs queued behind it:
   * keep them queued (`keep`, the default), start the next run with them (`continue`), or settle them
   * `unanswered` with reason `run_failed` (`withdraw`).
   */
  readonly afterFailedRun?: AfterFailedRun | undefined;
  /**
   * How many times in a row a task may be cut off by a crash, eviction or reset (a close does not count)
   * without changing its state before it ends instead of running again: a tool
   * call with an error result `restart_limit`, a compaction failed into `core.live.compactionFailure`, a run with
   * `core.live.runFailure` reason `restart_limit`, any
   * other task `faulted` with detail `{ reason: "restart_limit" }`. A generation's request in flight when the
   * host died is a failed attempt under `retry` first. Default 5; `Infinity` never ends a task for it;
   * anything below 1 means the default, and a fraction is rounded down.
   */
  readonly restartLimit?: number | undefined;
}

/**
 * Settings resolved over the defaults: every field present, objects merged.
 *
 * @category models
 * @since 0.0.0
 */
export interface Settings {
  readonly extensions?: ReadonlyArray<Extension.Any> | undefined;
  readonly stream: StreamOptions;
  readonly retry: RetryPolicy;
  /** Harness-wide policy over the defaults; per-model overrides apply where the model is known. */
  readonly compaction: CompactionPolicy & Pick<CompactionSettings, "modelOverrides">;
  readonly progress: ProgressPolicy;
  readonly toolExecution: ToolExecutionMode;
  readonly steeringMode: QueueMode;
  readonly followUpMode: QueueMode;
  readonly contextRetentionMs: number;
  readonly documentRetentionMs: number;
  readonly afterFailedRun: AfterFailedRun;
  readonly restartLimit: number;
}

/**
 * The default retry policy.
 *
 * @category defaults
 * @since 0.0.0
 */
export const DEFAULT_RETRY_POLICY: RetryPolicy = {
  enabled: true,
  maxRetries: 3,
  baseDelayMs: 2000,
  maxAgentDelayMs: 60000,
};

/**
 * The default compaction policy.
 *
 * @category defaults
 * @since 0.0.0
 */
export const DEFAULT_COMPACTION_POLICY: CompactionPolicy = {
  enabled: true,
  reserveTokens: 16384,
  keepRecentTokens: 20000,
  backgroundTokens: 32768,
};

/**
 * The default progress policy.
 *
 * @category defaults
 * @since 0.0.0
 */
export const DEFAULT_PROGRESS_POLICY: ProgressPolicy = {
  partialIntervalMs: 100,
  outputIntervalMs: 100,
  persistWithinMs: 1_000,
};

/**
 * The default `restartLimit`.
 *
 * @category defaults
 * @since 0.0.0
 */
export const DEFAULT_RESTART_LIMIT = 5;

/** Default `contextRetentionMs`: ten minutes. */
const DEFAULT_CONTEXT_RETENTION_MS = 600_000;

/** Default `documentRetentionMs`: ten minutes. */
const DEFAULT_DOCUMENT_RETENTION_MS = 600_000;

/**
 * Resolves host settings over the defaults.
 *
 * @category constructors
 * @since 0.0.0
 */
export const resolve = (settings: HarnessSettings | undefined): Settings => ({
  extensions: settings?.extensions,
  stream: { ...settings?.stream },
  // Field by field, so an explicitly undefined field keeps its default.
  retry: {
    enabled: settings?.retry?.enabled ?? DEFAULT_RETRY_POLICY.enabled,
    maxRetries: settings?.retry?.maxRetries ?? DEFAULT_RETRY_POLICY.maxRetries,
    baseDelayMs: settings?.retry?.baseDelayMs ?? DEFAULT_RETRY_POLICY.baseDelayMs,
    maxAgentDelayMs: settings?.retry?.maxAgentDelayMs ?? DEFAULT_RETRY_POLICY.maxAgentDelayMs,
  },
  compaction: resolveCompaction(settings?.compaction),
  progress: {
    partialIntervalMs:
      settings?.progress?.partialIntervalMs ?? DEFAULT_PROGRESS_POLICY.partialIntervalMs,
    outputIntervalMs:
      settings?.progress?.outputIntervalMs ?? DEFAULT_PROGRESS_POLICY.outputIntervalMs,
    persistWithinMs: persistWithinMs(settings?.progress?.persistWithinMs),
  },
  toolExecution: settings?.toolExecution ?? "parallel",
  steeringMode: settings?.steeringMode ?? "one-at-a-time",
  followUpMode: settings?.followUpMode ?? "one-at-a-time",
  contextRetentionMs: settings?.contextRetentionMs ?? DEFAULT_CONTEXT_RETENTION_MS,
  documentRetentionMs: documentRetentionMs(settings?.documentRetentionMs),
  afterFailedRun: settings?.afterFailedRun ?? "keep",
  restartLimit: restartLimit(settings?.restartLimit),
});

/**
 * The compaction policy over the defaults, `modelOverrides` kept as given. Field by field, as `retry`, so an
 * explicitly undefined field keeps its default.
 */
const resolveCompaction = (
  settings: CompactionSettings | undefined,
): CompactionPolicy & Pick<CompactionSettings, "modelOverrides"> => {
  const policy = {
    enabled: settings?.enabled ?? DEFAULT_COMPACTION_POLICY.enabled,
    reserveTokens: settings?.reserveTokens ?? DEFAULT_COMPACTION_POLICY.reserveTokens,
    keepRecentTokens: settings?.keepRecentTokens ?? DEFAULT_COMPACTION_POLICY.keepRecentTokens,
    backgroundTokens: settings?.backgroundTokens ?? DEFAULT_COMPACTION_POLICY.backgroundTokens,
  };
  const overrides = settings?.modelOverrides;
  return overrides === undefined ? policy : { ...policy, modelOverrides: overrides };
};

/**
 * `restartLimit`: a number of restarts, `1` or more, rounded down (`Infinity`: no
 * task ends for restarts); anything else is the default.
 */
const restartLimit = (value: number | undefined): number =>
  value !== undefined && value >= 1 ? Math.floor(value) : DEFAULT_RESTART_LIMIT;

/** `persistWithinMs`: a number of milliseconds, `0` or more; otherwise the default. */
const persistWithinMs = (value: number | undefined): number =>
  value !== undefined && value >= 0 ? value : DEFAULT_PROGRESS_POLICY.persistWithinMs;

/** `documentRetentionMs`: a non-negative number of milliseconds, `Infinity` included; anything else is the default. */
const documentRetentionMs = (value: number | undefined): number =>
  value !== undefined && !Number.isNaN(value) && value >= 0 ? value : DEFAULT_DOCUMENT_RETENTION_MS;

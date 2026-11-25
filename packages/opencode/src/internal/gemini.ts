/**
 * What is decided for Google before opencode's Gemini protocol writes the wire: thinking options, which thought
 * signatures are replayed, and the texts of a stream that ends without a finish reason.
 */
import * as Predicate from "effect/Predicate";
import type { ThinkingLevel } from "@effect-durable-harness/core/Message";
import type { ResolvedModel } from "@effect-durable-harness/core/Models";

/** The opencode protocol id of the Gemini API and of Vertex Gemini. */
export const GEMINI = "gemini";

/** The text for a stream that ends without a finish reason. */
export const NO_FINISH_REASON = "Google stream ended without a finish reason";

// Thought signatures must be base64 for Google APIs (TYPE_BYTES).
const BASE64 = /^[A-Za-z0-9+/]+={0,2}$/;

/**
 * A signature worth replaying. The same-model check is
 * `transformMessages`'s: a block from another model reaches the request without its signature.
 */
export const replayableSignature = (signature: string | undefined): string | undefined =>
  signature !== undefined && signature.length % 4 === 0 && BASE64.test(signature)
    ? signature
    : undefined;

/** A later delta without a signature keeps the earlier one. */
export const retainSignature = (
  existing: string | undefined,
  incoming: string | undefined,
): string | undefined => (incoming !== undefined && incoming.length > 0 ? incoming : existing);

/** opencode's Gemini `thinkingConfig` (`protocols/gemini.ts`, `GeminiThinkingConfigInput`). */
export type ThinkingConfig = {
  includeThoughts: boolean;
  thinkingLevel?: GoogleLevel;
  thinkingBudget?: number;
};

type GoogleLevel = "minimal" | "low" | "medium" | "high";

/**
 * The thinking options for a request, or none for a model without reasoning.
 *
 * opencode turns thoughts on whenever a `thinkingConfig` is sent and `includeThoughts` is absent
 * (`protocols/gemini.ts:465-470`), so thinking off sends `includeThoughts: false`, which is Google's default and what
 * a request with thinking off means.
 */
export const thinkingConfig = (
  model: ResolvedModel,
  reasoning: ThinkingLevel | undefined,
): ThinkingConfig | undefined => {
  if (!model.reasoning) return undefined;
  const level = reasoning === undefined ? "off" : clampThinkingLevel(model, reasoning);
  if (level === "off") return disabledThinkingConfig(model);
  const resolved = resolveGoogleLevel(model, level);
  return usesThinkingLevel(model.id)
    ? { includeThoughts: true, thinkingLevel: resolved }
    : { includeThoughts: true, thinkingBudget: thinkingBudget(model.id, resolved) };
};

/** The thinking options of a request with thinking off. */
const disabledThinkingConfig = (model: ResolvedModel): ThinkingConfig => {
  if (!usesThinkingLevel(model.id)) return { includeThoughts: false, thinkingBudget: 0 };
  const fallback = clampThinkingLevel(model, "off");
  if (fallback === "off") return { includeThoughts: false, thinkingBudget: 0 };
  return { includeThoughts: false, thinkingLevel: resolveGoogleLevel(model, fallback) };
};

/** Gemini 3 takes a level, Gemini 2.5 a token budget. */
const usesThinkingLevel = (modelId: string): boolean => {
  const id = modelId.toLowerCase();
  return (
    /gemini-3(?:\.\d+)?-(?:pro|flash)/.test(id) ||
    id === "gemini-flash-latest" ||
    id === "gemini-flash-lite-latest" ||
    /gemma-?4/.test(id)
  );
};

/**
 * The model's map value, in any case, or the level. A value that is no Google level is broken model metadata: this
 * throws, as a defect of the stream.
 */
const resolveGoogleLevel = (model: ResolvedModel, level: ThinkingLevel): GoogleLevel => {
  const mapped = model.thinkingLevelMap?.[level];
  const resolved = Predicate.isString(mapped) ? mapped.toLowerCase() : level;
  switch (resolved) {
    case "minimal":
    case "low":
    case "medium":
    case "high":
      return resolved;
    default:
      throw new Error(
        `Unsupported Google thinking level mapping for ${model.provider}/${model.id}: ${level} -> ${String(mapped)}`,
      );
  }
};

/** The token budget of a thinking level on a Gemini 2.5 model, without per-request custom budgets. */
const thinkingBudget = (modelId: string, level: GoogleLevel): number => {
  if (modelId.includes("2.5-pro"))
    return { minimal: 128, low: 2048, medium: 8192, high: 32768 }[level];
  if (modelId.includes("2.5-flash-lite"))
    return { minimal: 512, low: 2048, medium: 8192, high: 24576 }[level];
  if (modelId.includes("2.5-flash"))
    return { minimal: 128, low: 2048, medium: 8192, high: 24576 }[level];
  return -1;
};

const THINKING_LEVELS: ReadonlyArray<ThinkingLevel> = [
  "off",
  "minimal",
  "low",
  "medium",
  "high",
  "xhigh",
  "max",
];

/** The thinking levels a model supports. */
const supportedLevels = (model: ResolvedModel): ReadonlyArray<ThinkingLevel> =>
  model.reasoning
    ? THINKING_LEVELS.filter((level) => {
        const mapped = model.thinkingLevelMap?.[level];
        if (mapped === null) return false;
        if (level === "xhigh" || level === "max") return mapped !== undefined;
        return true;
      })
    : ["off"];

/** The level, or the nearest supported one, upward first. */
const clampThinkingLevel = (model: ResolvedModel, level: ThinkingLevel): ThinkingLevel => {
  const available = supportedLevels(model);
  if (available.includes(level)) return level;
  const index = THINKING_LEVELS.indexOf(level);
  const upward = THINKING_LEVELS.slice(index).find((candidate) => available.includes(candidate));
  if (upward !== undefined) return upward;
  const downward = THINKING_LEVELS.slice(0, index).findLast((candidate) =>
    available.includes(candidate),
  );
  return downward ?? available[0] ?? "off";
};

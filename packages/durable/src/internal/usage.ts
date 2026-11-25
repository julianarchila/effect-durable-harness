// The `core.usage` ledger. Recording runs inside the commit that records
// the response or the tool result; summing builds `Harness.usage`.
import type * as Tracker from "@effect-durable-harness/delta/Tracker";
import * as Effect from "effect/Effect";
import type { ConversationId } from "../Ids.ts";
import type { Usage } from "../Message.ts";
import { Tx, type TxDocError } from "../Tx.ts";
import { UsageDoc, type UsageState } from "../Usage.ts";

type Mutable<T> = { -readonly [K in keyof T]: T[K] extends object ? Mutable<T[K]> : T[K] };

/** Adds `usage` to one bucket of the conversation's `core.usage`. */
export const recordUsage = (
  conversationId: ConversationId,
  bucket: keyof UsageState,
  key: string,
  usage: Usage,
): Effect.Effect<void, TxDocError, Tx> =>
  Effect.gen(function* () {
    const tx = yield* Tx;
    const totals = (yield* tx.doc(UsageDoc, conversationId))[bucket];
    // Own keys only: a tool may be called `toString`.
    const total = Object.hasOwn(totals, key) ? totals[key] : undefined;
    if (total === undefined) totals[key] = copyUsage(usage);
    else addUsage(total, usage);
  });

/** Adds every counter; optional counters once either side reports them. */
export const addUsage = (total: Tracker.Draft<Usage> | Mutable<Usage>, usage: Usage): void => {
  total.input += usage.input;
  total.output += usage.output;
  total.cacheRead += usage.cacheRead;
  total.cacheWrite += usage.cacheWrite;
  total.totalTokens += usage.totalTokens;
  if (usage.cacheWrite1h !== undefined)
    total.cacheWrite1h = (total.cacheWrite1h ?? 0) + usage.cacheWrite1h;
  if (usage.reasoning !== undefined) total.reasoning = (total.reasoning ?? 0) + usage.reasoning;
  total.cost.input += usage.cost.input;
  total.cost.output += usage.cost.output;
  total.cost.cacheRead += usage.cost.cacheRead;
  total.cost.cacheWrite += usage.cost.cacheWrite;
  total.cost.total += usage.cost.total;
};

/** Adds every bucket of `state` into `sum`. */
export const addUsageState = (sum: Mutable<UsageState>, state: UsageState): void => {
  for (const bucket of ["models", "tools"] as const) {
    for (const [key, usage] of Object.entries(state[bucket])) {
      const total = Object.hasOwn(sum[bucket], key) ? sum[bucket][key] : undefined;
      // Define rather than assign: assigning a tool named `__proto__` would set the prototype.
      if (total !== undefined) addUsage(total, usage);
      else {
        Object.defineProperty(sum[bucket], key, {
          value: copyUsage(usage),
          enumerable: true,
          writable: true,
        });
      }
    }
  }
};

/** A copy without absent optional counters: drafts take strict JSON, and providers may leave them undefined. */
const copyUsage = (usage: Usage): Mutable<Usage> => {
  const copy: Mutable<Usage> = {
    input: usage.input,
    output: usage.output,
    cacheRead: usage.cacheRead,
    cacheWrite: usage.cacheWrite,
    totalTokens: usage.totalTokens,
    cost: { ...usage.cost },
  };
  if (usage.cacheWrite1h !== undefined) copy.cacheWrite1h = usage.cacheWrite1h;
  if (usage.reasoning !== undefined) copy.reasoning = usage.reasoning;
  return copy;
};

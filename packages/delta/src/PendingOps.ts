/**
 * The pending ops of one slow consumer of a state. Batches are queued raw,
 * in order, and folded while they wait, so a consumer that falls behind
 * receives fewer ops than were committed and the queue stays bounded.
 *
 * **Details**
 *
 * - Appends to one path join into one append.
 * - A primitive set replaces the earlier leaf ops on its path, and an append
 *   after a string set appends into it.
 * - A root replacement `r` drops everything before it.
 * - Past {@link maxPendingOps} pending ops after folding, only "replace with
 *   the current value" stays pending.
 *
 * Folding is exact for the batches of `Tracker`, `Delta.diff` and
 * `Tracker.appendText`: applying the taken ops gives the value that applying
 * every queued batch gives. Received batches and op tuples are never mutated,
 * as they may feed other consumers too.
 *
 * @since 0.1.0
 */
import { dual } from "effect/Function";
import type * as Delta from "./Delta.ts";
import * as internal from "./internal/pending.ts";

const TypeId = internal.TypeId;

/**
 * A queue of the pending ops of one state for one consumer.
 *
 * @category models
 * @since 0.1.0
 */
export interface PendingOps {
  readonly [TypeId]: typeof TypeId;
  /**
   * The pending ops after folding; `1` once overflowed (the replacement the
   * owner sends). A single batch counts as received, so one commit larger
   * than {@link maxPendingOps} counts in full until a second one arrives and
   * overflows.
   *
   * @since 0.1.0
   */
  readonly size: number;
}

/**
 * What {@link take} hands over: the pending ops, folded, or `Replace` after an
 * overflow, which the owner of the queue turns into a root replacement with
 * the current value.
 *
 * **Details**
 *
 * The union is tagged by `_tag`, as Effect does. The value is never persisted
 * or sent; only its ops are.
 *
 * @category models
 * @since 0.1.0
 */
export type PendingTake =
  | { readonly _tag: "Ops"; readonly ops: Delta.Delta }
  | { readonly _tag: "Replace" };

/**
 * Pending ops past which only "replace with the current value" stays
 * pending.
 *
 * @category constants
 * @since 0.1.0
 */
export const maxPendingOps: number = internal.maxPendingOps;

/**
 * Checks whether a value is a {@link PendingOps} queue.
 *
 * @category guards
 * @since 0.1.0
 */
export const isPendingOps: (u: unknown) => u is PendingOps = internal.isPendingOps;

/**
 * Creates an empty queue.
 *
 * @category constructors
 * @since 0.1.0
 */
export const make: () => PendingOps = internal.make;

/**
 * Queues the ops of one commit.
 *
 * **Details**
 *
 * An empty batch is ignored. While nothing else is pending, the batch is kept
 * by reference, so a consumer that keeps up never pays for folding.
 *
 * @category combinators
 * @since 0.1.0
 */
export const push: {
  (ops: Delta.Delta): (self: PendingOps) => void;
  (self: PendingOps, ops: Delta.Delta): void;
} = dual(2, internal.push);

/**
 * Hands over everything pending and leaves the queue empty.
 *
 * **Details**
 *
 * A single pending batch is handed over as received, by identity. The ops of
 * an empty queue are an empty batch.
 *
 * @category combinators
 * @since 0.1.0
 */
export const take: (self: PendingOps) => PendingTake = internal.take;

/**
 * The pending queue: the ops a slow consumer of one state has not taken yet, folded while they wait.
 *
 * @since 0.1.0
 * @internal
 */
import type * as Schema from "effect/Schema";
import type * as Delta from "../Delta.ts";
import type * as PendingOps from "../PendingOps.ts";

/** @internal */
export const TypeId = "~@effect-durable-harness/delta/PendingOps";

/** @internal */
export const maxPendingOps = 10_000;

/** A leaf op: an append, or a set of a primitive. The leaf index points at these only. */
type LeafOp =
  | readonly ["a", Delta.NonEmptyPath, string]
  | readonly ["s", Delta.NonEmptyPath, Schema.Json];

type Folding = {
  readonly kind: "folding";
  /** Owned by the queue until `take()` hands it over; never one of the received op arrays. */
  readonly ops: Array<Delta.Operation>;
  /** Path key to the position of the last pending leaf op on that path. */
  readonly leaves: Map<string, number>;
};

type State =
  | { readonly kind: "empty" }
  /** One batch, kept by reference: a consumer that keeps up never pays for folding. */
  | { readonly kind: "batch"; readonly ops: Delta.Delta }
  | Folding
  /** Overflowed: nothing more accumulates until `take()` or a batch that starts with `r`. */
  | { readonly kind: "replace" };

const EMPTY: State = { kind: "empty" };

const REPLACE: State = { kind: "replace" };

const NO_OPS: PendingOps.PendingTake = { _tag: "Ops", ops: [] };

const REPLACE_TAKE: PendingOps.PendingTake = { _tag: "Replace" };

class PendingOpsImpl implements PendingOps.PendingOps {
  readonly [TypeId]: typeof TypeId = TypeId;
  #state: State = EMPTY;

  push(ops: Delta.Delta): void {
    if (ops.length === 0) return;
    const state = this.#state;
    // A batch that starts with `r` (the tracker's and `diff()`'s only place for it) drops everything pending, also an
    // overflow, and is kept as received.
    if (ops[0]![0] === "r") {
      this.#state = { kind: "batch", ops };
      return;
    }
    switch (state.kind) {
      case "empty":
        this.#state = { kind: "batch", ops };
        return;
      case "batch": {
        const folding: Folding = { kind: "folding", ops: [], leaves: new Map() };
        this.#state = folding;
        if (this.#fold(state.ops, folding)) this.#fold(ops, folding);
        return;
      }
      case "folding":
        this.#fold(ops, state);
        return;
      case "replace":
        return;
    }
  }

  get size(): number {
    const state = this.#state;
    if (state.kind === "empty") return 0;
    return state.kind === "replace" ? 1 : state.ops.length;
  }

  take(): PendingOps.PendingTake {
    const state = this.#state;
    this.#state = EMPTY;
    switch (state.kind) {
      case "empty":
        return NO_OPS;
      case "batch":
      case "folding":
        return { _tag: "Ops", ops: state.ops };
      case "replace":
        return REPLACE_TAKE;
    }
  }

  /** Fold `ops` into `folding`; false once the queue overflowed into `replace`. */
  #fold(ops: Delta.Delta, folding: Folding): boolean {
    for (const op of ops) {
      switch (op[0]) {
        case "r":
          folding.ops.length = 0;
          folding.leaves.clear();
          break;
        case "a":
          if (mergeLeaf(op, folding)) continue;
          break;
        case "s":
          if (isPrimitive(op[2])) {
            if (mergeLeaf(op, folding)) continue;
          } else if (folding.leaves.size > 0) folding.leaves.clear();
          break;
        default:
          if (folding.leaves.size > 0) folding.leaves.clear();
      }
      folding.ops.push(op);
      if (folding.ops.length > maxPendingOps) {
        this.#state = REPLACE;
        return false;
      }
    }
    return true;
  }
}

/** @internal */
export const make = (): PendingOps.PendingOps => new PendingOpsImpl();

/** @internal */
export const push = (self: PendingOps.PendingOps, ops: Delta.Delta): void =>
  (self as PendingOpsImpl).push(ops);

/** @internal */
export const take = (self: PendingOps.PendingOps): PendingOps.PendingTake =>
  (self as PendingOpsImpl).take();

/** @internal */
export const isPendingOps = (u: unknown): u is PendingOps.PendingOps =>
  typeof u === "object" && u !== null && TypeId in u;

const isPrimitive = (value: Schema.Json): boolean => value === null || typeof value !== "object";

/**
 * Merge the leaf op `op` into the last pending leaf op on its path, or index it as that op at the position it is about
 * to be pushed to. True when merged.
 */
function mergeLeaf(op: LeafOp, folding: Folding): boolean {
  const key = pathKey(op[1]);
  const at = folding.leaves.get(key);
  if (at !== undefined) {
    // SAFETY: the index points at leaf ops only.
    const merged = merge(folding.ops[at] as LeafOp, op);
    if (merged !== undefined) {
      folding.ops[at] = merged;
      return true;
    }
  }
  folding.leaves.set(key, folding.ops.length);
  return false;
}

/**
 * The key of a path in the leaf index. Injective, as merging two distinct paths would lose a write: `["a.b"]` and
 * `["a", "b"]` are different keys. A number and its string (`1`, `"1"`) may share one: within one index lifetime a
 * parent is either an array or an object, as changing it is a structural op that clears the index.
 */
function pathKey(path: Delta.Path): string {
  return JSON.stringify(path);
}

/** `next` folded into `previous` (same path), or undefined when they do not fold. */
function merge(previous: LeafOp, next: LeafOp): LeafOp | undefined {
  if (next[0] === "s") return next;
  if (previous[0] === "a") return ["a", previous[1], previous[2] + next[2]];
  // Only a string set takes an append; the tracker never appends to another primitive.
  if (typeof previous[2] === "string") return ["s", previous[1], previous[2] + next[2]];
  return undefined;
}

/**
 * Operation validation and application.
 *
 * @since 0.1.0
 * @internal
 */
import * as Result from "effect/Result";
import type * as Delta from "../Delta.ts";
import {
  DeltaError,
  InvalidOperationError,
  isDeltaError,
  UnresolvablePathError,
  UnsafePathError,
} from "./errors.ts";
import type { Json } from "./json.ts";

/**
 * A decoded operation whose payloads are mutable kernel-owned JSON.
 *
 * @internal
 */
export type Op =
  | readonly ["r", Json]
  | readonly ["s", Delta.NonEmptyPath, Json]
  | readonly ["d", Delta.NonEmptyPath]
  | readonly ["a", Delta.NonEmptyPath, string]
  | readonly ["t", Delta.NonEmptyPath, number]
  | readonly ["p", Delta.Path, number, number, Array<Json>]
  | readonly ["m", Delta.Path, Array<number>];

type Container = Array<Json> | Record<string, Json>;

const isObject = (value: unknown): value is object => value !== null && typeof value === "object";

// -----------------------------------------------------------------------------
// errors
// -----------------------------------------------------------------------------

/** @internal */
export const invalidOperation = (message: string): DeltaError =>
  new DeltaError({ reason: new InvalidOperationError({ message }) });

/** @internal */
export const unsafePath = (segment: Delta.Segment): DeltaError =>
  new DeltaError({ reason: new UnsafePathError({ segment }) });

/** @internal */
export const unresolvablePath = (path: Delta.Path | number): DeltaError =>
  new DeltaError({ reason: new UnresolvablePathError({ path }) });

/** @internal */
export const attempt = <A>(f: () => A): Result.Result<A, DeltaError> => {
  try {
    return Result.succeed(f());
  } catch (error) {
    if (isDeltaError(error)) return Result.fail(error);
    throw error;
  }
};

// -----------------------------------------------------------------------------
// overlap
// -----------------------------------------------------------------------------

/**
 * Longest suffix of `before` that is a prefix of `after`. Probes with `indexOf` and verifies exact substring
 * equality, so the hot loops are native. A hand-written KMP is asymptotically equivalent and much slower in
 * practice.
 *
 * Always correct: the returned `n` satisfies `before.slice(before.length - n) === after.slice(0, n)`.
 *
 * @internal
 */
export const overlap = (
  before: string,
  after: string,
  options: {
    readonly scan: number;
    readonly probe?: number | undefined;
    readonly maxCandidates?: number | undefined;
  },
): number => {
  const { scan } = options;
  const probe = options.probe ?? 64;
  const maxCandidates = options.maxCandidates ?? 8;
  if (before.length === 0 || after.length === 0 || scan === 0) return 0;
  const tail = before.length > scan ? before.slice(before.length - scan) : before;

  // A probe of length h can only find overlaps of at least h: the head must actually occur in `before`. So try
  // a long head first (few candidates, and it catches the large overlaps a rolling window produces), then fall
  // back to one character, which finds any overlap at the cost of more candidates.
  //
  // Candidates are bounded because repetitive output (a build log, or any run of one character) makes a long
  // head match at thousands of positions. Giving up returns 0, which emits a set: larger, never wrong.
  for (const h of [Math.min(probe, after.length), 1]) {
    const head = after.slice(0, h);
    let tried = 0;
    for (let k = tail.indexOf(head); k !== -1; k = tail.indexOf(head, k + 1)) {
      if (++tried > maxCandidates) break;
      const n = tail.length - k;
      if (n <= after.length && tail.slice(k) === after.slice(0, n)) return n;
    }
    if (h === 1) break;
  }
  return 0;
};

/**
 * `text.slice(start, end)` as a string of its own. V8 represents a slice of 13 or more characters as a view that keeps
 * all of `text` alive, so an append op cut from a long, growing string would retain that whole string for as long as
 * the op lives: in a delta chain, a watch backlog, or a value built by applying the op. Appending a character and
 * slicing it off again makes V8 copy just the slice.
 *
 * @internal
 */
export const ownedSlice = (text: string, start: number, end: number = text.length): string =>
  `${text.slice(start, end)} `.slice(0, -1);

// -----------------------------------------------------------------------------
// path safety
// -----------------------------------------------------------------------------

/**
 * Segments that reach the prototype chain.
 *
 * `JSON.parse` is safe on its own: it makes `__proto__` an own property. What is not safe is
 * `parent[key] = value`, which is exactly what an applier does, and paths are data:
 * `["s", ["__proto__", "isAdmin"], true]` pollutes `Object.prototype` for the whole process.
 *
 * @internal
 */
export const reservedSegments: ReadonlySet<string> = new Set([
  "__proto__",
  "constructor",
  "prototype",
]);

/**
 * Verb, arity, and payload shape for a decoded operation: paths inline, no `#`, no short forms.
 *
 * Validating an `Operation` against the wire grammar would be laxer than the type: a two-element
 * `["s", value]` would pass, and `apply` would then read the value as a path. Each vocabulary gets the
 * validator that matches it.
 *
 * @internal
 */
export const assertValidOperation: (u: unknown) => asserts u is Delta.Operation = (u) => {
  if (!Array.isArray(u) || u.length === 0) throw invalidOperation("op is not a tuple");
  switch (u[0]) {
    case "r":
      if (u.length !== 2) throw invalidOperation("r arity");
      return;
    case "s":
      if (u.length !== 3) throw invalidOperation("s arity");
      assertPathArgument(u[1], true);
      return;
    case "d":
      if (u.length !== 2) throw invalidOperation("d arity");
      assertPathArgument(u[1], true);
      return;
    case "a":
      if (u.length !== 3 || typeof u[2] !== "string") throw invalidOperation("a shape");
      assertPathArgument(u[1], true);
      return;
    case "t":
      if (u.length !== 3 || !Number.isInteger(u[2]) || u[2] < 0) throw invalidOperation("t shape");
      assertPathArgument(u[1], true);
      return;
    case "p": {
      if (u.length !== 5) throw invalidOperation("p arity");
      assertPathArgument(u[1]);
      if (!Number.isInteger(u[2]) || u[2] < 0) throw invalidOperation("p index");
      if (!Number.isInteger(u[3]) || u[3] < 0) throw invalidOperation("p remove");
      if (!Array.isArray(u[4])) throw invalidOperation("p items");
      return;
    }
    case "m":
      if (u.length !== 3) throw invalidOperation("m arity");
      assertPathArgument(u[1]);
      assertPermutation(u[2]);
      return;
    // Silently skipping an unknown verb is how a newer producer's operation vanishes.
    default:
      throw invalidOperation(`unknown op verb: ${String(u[0])}`);
  }
};

const assertPathArgument = (u: unknown, nonEmpty = false): void => {
  if (!Array.isArray(u)) throw invalidOperation("path is not an array");
  if (nonEmpty && u.length === 0) throw invalidOperation("path is empty");
  assertSafePath(u);
};

/** @internal */
export const assertPermutation: (u: unknown) => asserts u is Array<number> = (u) => {
  if (!Array.isArray(u)) throw invalidOperation("m permutation is not an array");
  const seen = new Uint8Array(u.length);
  for (const index of u) {
    if (!Number.isInteger(index) || index < 0 || index >= u.length || seen[index] !== 0) {
      throw invalidOperation("m permutation is not a bijection");
    }
    seen[index] = 1;
  }
};

/** @internal */
export const assertSafePath = (path: Delta.Path): void => {
  for (const segment of path) {
    if (typeof segment === "string") {
      if (reservedSegments.has(segment)) throw unsafePath(segment);
    } else if (!Number.isInteger(segment) || segment < 0) {
      throw unsafePath(segment);
    }
  }
};

/**
 * An index may address an existing element or append exactly one past the end.
 *
 * This is what keeps the value strict JSON. A sparse array does not survive a JSON round trip: holes
 * serialize to `null` and return as real properties, so `arr[7] = x` on a length-3 array already produces
 * state a replica cannot match. It also removes the denial of service it would otherwise permit:
 * `["s", ["xs", 4294967290], 1]` would allocate a 4.29-billion-entry array from one operation.
 */
const assertIndexInRange = (parent: ReadonlyArray<unknown>, index: number): void => {
  if (index > parent.length) throw unsafePath(index);
};

// -----------------------------------------------------------------------------
// application
// -----------------------------------------------------------------------------

/**
 * Applies decoded operations to a plain mutable value. Returns the value, because `r` replaces it outright and
 * cannot be done in place.
 *
 * @internal
 */
export const apply = <A>(target: A | undefined, operations: Delta.Delta): A =>
  applyOps(target, operations as ReadonlyArray<Op>);

const applyOps = <A>(target: A | undefined, operations: ReadonlyArray<Op>): A => {
  let root = target as unknown as Json;

  for (const op of operations) {
    assertValidOperation(op);
    if (op[0] === "r") {
      // Adopted, not copied. The consumer owns the batch it was handed. Fanning one batch out to several
      // consumers in-process therefore makes their replicas alias each other: copy the batch at the fan-out
      // point, or let each consumer decode its own.
      root = op[1];
      continue;
    }

    const path = op[1];
    assertSafePath(path);

    if (op[0] === "p") {
      const array = path.length === 0 ? root : resolve(root, path);
      if (!Array.isArray(array)) throw unresolvablePath(path);
      array.splice(op[2], op[3]);
      const chunkSize = 10_000;
      for (let offset = 0; offset < op[4].length; offset += chunkSize) {
        array.splice(op[2] + offset, 0, ...op[4].slice(offset, offset + chunkSize));
      }
      continue;
    }
    if (op[0] === "m") {
      const array = path.length === 0 ? root : resolve(root, path);
      if (!Array.isArray(array) || array.length !== op[2].length) throw unresolvablePath(path);
      const previous = array.slice();
      for (let index = 0; index < op[2].length; index++) array[index] = previous[op[2][index]!]!;
      continue;
    }

    // s/d/a/t can never target the root: the type forbids it.
    const parent = resolve(root, path.slice(0, -1)) as Record<Delta.Segment, Json>;
    const key = path[path.length - 1]!;
    if (Array.isArray(parent)) {
      if (typeof key !== "number") throw unsafePath(key);
      assertIndexInRange(parent, key);
    }
    // `defineProperty` rather than assignment: a setter inherited from the prototype chain would otherwise
    // run on write.
    const write = (value: Json) => {
      Object.defineProperty(parent, key, {
        value,
        writable: true,
        enumerable: true,
        configurable: true,
      });
    };
    const read = (): unknown => (Object.hasOwn(parent, key) ? parent[key] : undefined);
    switch (op[0]) {
      case "s":
        write(op[2]);
        break;
      case "d":
        if (Array.isArray(parent)) {
          if (typeof key !== "number" || key >= parent.length) throw unresolvablePath(path);
          (parent as unknown as Array<Json>).splice(key, 1);
        } else delete parent[key];
        break;
      case "a": {
        const current = read();
        if (typeof current !== "string") throw unresolvablePath(path);
        write(`${current}${op[2]}`);
        break;
      }
      case "t": {
        const current = read();
        if (typeof current !== "string") throw unresolvablePath(path);
        write(current.slice(op[2]));
        break;
      }
    }
  }
  return root as unknown as A;
};

/**
 * Applies one decoded batch without mutating the previous immutable value.
 *
 * @internal
 */
export const applyImmutable = <A>(target: A | undefined, operations: Delta.Delta): A =>
  applyImmutableBatches(target, [operations]);

/**
 * Applies decoded batches as one final-result-only replay.
 *
 * Containers copied for an earlier batch may be mutated while applying a later batch, so this deliberately
 * exposes no intermediate revisions.
 *
 * @internal
 */
export const applyImmutableBatches = <A>(
  target: A | undefined,
  batches: Iterable<Delta.Delta>,
): A => {
  let root = target as unknown as Json;
  const owned = new WeakSet<object>();
  for (const operations of batches) {
    for (const op of operations as ReadonlyArray<Op>) {
      assertValidOperation(op);
      if (op[0] === "r") {
        root = op[1];
        continue;
      }
      if (op[0] === "p") {
        const appended = appendCopy(root, op, owned);
        if (appended !== undefined) {
          root = appended;
          continue;
        }
      }
      root = copyContainers(
        root,
        op[0] === "p" || op[0] === "m" ? op[1] : op[1].slice(0, -1),
        owned,
      );
      root = applyOps(root, [op]);
    }
  }
  return root as unknown as A;
};

/**
 * An append to an array not yet copied in this replay, built as one copy with the items (`concat`) instead of
 * a copy that the insert then grows, which copies it again. `undefined` for any other splice and for any path
 * that is not a plain walk over own properties, which takes the general path and so fails with its errors.
 */
const appendCopy = (
  root: Json,
  op: Extract<Op, { 0: "p" }>,
  owned: WeakSet<object>,
): Json | undefined => {
  const [, path, index, deleteCount, items] = op;
  if (deleteCount !== 0) return undefined;
  let target: Json = root;
  for (const segment of path) {
    const valid =
      typeof segment === "string"
        ? !reservedSegments.has(segment) && isObject(target) && !Array.isArray(target)
        : Number.isInteger(segment) && segment >= 0 && Array.isArray(target);
    if (!valid || !Object.hasOwn(target as object, segment)) return undefined;
    target = (target as Record<Delta.Segment, Json>)[segment]!;
  }
  if (!Array.isArray(target) || owned.has(target) || index !== target.length) return undefined;
  const appended = target.concat(items);
  owned.add(appended);
  if (path.length === 0) return appended;
  const parentPath = path.slice(0, -1);
  const copiedRoot = copyContainers(root, parentPath, owned);
  Object.defineProperty(resolve(copiedRoot, parentPath), path[path.length - 1]!, {
    value: appended,
    writable: true,
    enumerable: true,
    configurable: true,
  });
  return copiedRoot;
};

const copyContainers = (root: Json, path: Delta.Path, owned: WeakSet<object>): Json => {
  const copy = (value: Json): Container => {
    if (Array.isArray(value)) return value.slice();
    if (!isObject(value)) throw unresolvablePath(path);
    // Spread and `Object.assign()` onto a null-prototype object define own data properties, `__proto__` included,
    // as `Object.defineProperty()` per key does, at a fraction of its cost.
    return Object.getPrototypeOf(value) === null
      ? Object.assign(Object.create(null) as Record<string, Json>, value)
      : { ...value };
  };
  if (!isObject(root)) throw unresolvablePath(path);
  let copiedRoot: Container;
  if (owned.has(root)) copiedRoot = root as Container;
  else {
    copiedRoot = copy(root);
    owned.add(copiedRoot);
  }
  let destination = copiedRoot;
  for (const segment of path) {
    if (!Object.hasOwn(destination, segment)) throw unresolvablePath(path);
    if (Array.isArray(destination) && typeof segment !== "number") throw unsafePath(segment);
    const child = (destination as Record<Delta.Segment, Json>)[segment]!;
    if (!isObject(child)) throw unresolvablePath(path);
    if (owned.has(child)) {
      destination = child as Container;
      continue;
    }
    const copiedChild = copy(child);
    Object.defineProperty(destination, segment, {
      value: copiedChild,
      writable: true,
      enumerable: true,
      configurable: true,
    });
    owned.add(copiedChild);
    destination = copiedChild;
  }
  return copiedRoot;
};

const resolveValue = (root: Json, path: Delta.Path): Json => {
  let node: Json = root;
  for (const segment of path) {
    if (!isObject(node)) throw unresolvablePath(path);
    if (Array.isArray(node) && typeof segment !== "number") throw unsafePath(segment);
    // Own properties only: an inherited getter must not run, and a walk must not escape the value into the
    // prototype chain.
    if (!Object.hasOwn(node, segment)) throw unresolvablePath(path);
    node = (node as Record<Delta.Segment, Json>)[segment]!;
  }
  return node;
};

const resolve = (root: Json, path: Delta.Path): Json => {
  const node = resolveValue(root, path);
  if (!isObject(node)) throw unresolvablePath(path);
  return node;
};

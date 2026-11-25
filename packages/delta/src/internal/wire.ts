/**
 * Path-interning codec for ordered operation streams.
 *
 * Path interning and arity omission live between the tracker and a boundary; `Delta.Operation` and the
 * appliers know nothing about them. Use one pair per independent state stream: every decoder must observe
 * exactly the batches encoded by its matching encoder, beginning with that state's base. Consecutive
 * batches of one encoder may be concatenated into one batch: only path ids span batches, and every batch
 * starts with an explicit path.
 *
 * @since 0.1.0
 * @internal
 */
import type * as Delta from "../Delta.ts";
import type * as Wire from "../Wire.ts";
import {
  assertPermutation,
  assertSafePath,
  invalidOperation,
  type Op,
  unresolvablePath,
} from "./delta.ts";
import type { Json } from "./json.ts";

/** @internal */
export const EncoderTypeId = "~@effect-durable-harness/delta/Wire/Encoder";

/** @internal */
export const DecoderTypeId = "~@effect-durable-harness/delta/Wire/Decoder";

const pathKey = (path: Delta.Path): string => JSON.stringify(path);

/**
 * Interns on second use. A definition costs more than the path it replaces, so interning on first use loses on
 * the many paths written exactly once.
 */
class EncoderImpl implements Wire.Encoder {
  readonly [EncoderTypeId]: typeof EncoderTypeId = EncoderTypeId;
  readonly seen = new Set<string>();
  readonly ids = new Map<string, number>();
  nextId = 0;
}

class DecoderImpl implements Wire.Decoder {
  readonly [DecoderTypeId]: typeof DecoderTypeId = DecoderTypeId;
  readonly paths = new Map<number, Delta.Path>();
}

/** @internal */
export const makeEncoder = (): Wire.Encoder => new EncoderImpl();

/** @internal */
export const makeDecoder = (): Wire.Decoder => new DecoderImpl();

/** @internal */
export const encode = (self: Wire.Encoder, operations: Delta.Delta): Array<Wire.Operation> => {
  const encoder = self as EncoderImpl;
  // Arity omission is scoped to a batch. Letting it span batches would make a batch's first operation depend
  // on the previous batch's last one, so a reader that skips or reorders a batch decodes into the wrong path.
  // Ids are the only cross-batch state, and the dictionary makes those explicit.
  let previous: string | undefined;
  const out: Array<Wire.Operation> = [];
  for (const op of operations) {
    if (op[0] === "r") {
      out.push(op);
      // A base batch is a recovery point: a reader replays from the last one with a fresh decoder, so
      // everything after it must be self-contained. Keeping ids across a replacement emits references to
      // definitions the reader never saw.
      encoder.seen.clear();
      encoder.ids.clear();
      encoder.nextId = 0;
      previous = undefined;
      continue;
    }
    const path = op[1];
    const key = pathKey(path);

    // Same path as the previous operation: drop the reference entirely.
    if (key === previous) {
      switch (op[0]) {
        case "s":
          out.push(["s", op[2]]);
          break;
        case "d":
          out.push(["d"]);
          break;
        case "a":
          out.push(["a", op[2]]);
          break;
        case "t":
          out.push(["t", op[2]]);
          break;
        case "p":
          out.push(["p", op[2], op[3], op[4]]);
          break;
        case "m":
          out.push(["m", op[2]]);
          break;
      }
      continue;
    }

    let ref: Wire.PathRef = path;
    const existing = encoder.ids.get(key);
    if (existing !== undefined) {
      ref = existing;
    } else if (encoder.seen.has(key)) {
      const id = encoder.nextId++;
      encoder.ids.set(key, id);
      out.push(["#", id, path]); // second use: define, then reference
      ref = id;
    } else {
      encoder.seen.add(key); // first use: inline
    }

    switch (op[0]) {
      case "s":
        out.push(["s", ref as Wire.PathRef<Delta.NonEmptyPath>, op[2]]);
        break;
      case "d":
        out.push(["d", ref as Wire.PathRef<Delta.NonEmptyPath>]);
        break;
      case "a":
        out.push(["a", ref as Wire.PathRef<Delta.NonEmptyPath>, op[2]]);
        break;
      case "t":
        out.push(["t", ref as Wire.PathRef<Delta.NonEmptyPath>, op[2]]);
        break;
      case "p":
        out.push(["p", ref, op[2], op[3], op[4]]);
        break;
      case "m":
        out.push(["m", ref, op[2]]);
        break;
    }
    previous = key;
  }
  return out;
};

/** @internal */
export const decode = (
  self: Wire.Decoder,
  wire: ReadonlyArray<unknown>,
): Array<Delta.Operation> => {
  const decoder = self as DecoderImpl;
  let previous: Delta.Path | undefined; // scoped to the batch, as in encode
  const out: Array<Op> = [];
  for (const op of wire) {
    assertValidOperation(op);
    if (op[0] === "#") {
      assertSafePath(op[2]);
      decoder.paths.set(op[1], op[2]);
      continue;
    }
    if (op[0] === "r") {
      out.push(op as Op);
      decoder.paths.clear();
      previous = undefined;
      continue;
    }

    // Arity tells whether a reference is present: the short forms omit it.
    const short =
      (op[0] === "d" && op.length === 1) ||
      (op[0] !== "d" && op[0] !== "p" && op.length === 2) ||
      (op[0] === "p" && op.length === 4);

    let path: Delta.Path;
    if (short) {
      if (previous === undefined) throw unresolvablePath([]);
      path = previous;
    } else {
      const ref = op[1] as Wire.PathRef;
      if (typeof ref === "number") {
        const resolved = decoder.paths.get(ref);
        if (resolved === undefined) throw unresolvablePath(ref);
        path = resolved;
      } else {
        path = ref;
      }
      previous = path;
    }

    if (op[0] !== "p" && op[0] !== "m" && path.length === 0) throw unresolvablePath(path);
    switch (op[0]) {
      case "s":
        out.push(["s", path as Delta.NonEmptyPath, (short ? op[1] : op[2]) as Json]);
        break;
      case "d":
        out.push(["d", path as Delta.NonEmptyPath]);
        break;
      case "a":
        out.push(["a", path as Delta.NonEmptyPath, (short ? op[1] : op[2]) as string]);
        break;
      case "t":
        out.push(["t", path as Delta.NonEmptyPath, (short ? op[1] : op[2]) as number]);
        break;
      case "p": {
        const [index, remove, items] = short
          ? [op[1] as number, op[2] as number, op[3] as Array<Json>]
          : [op[2] as number, op[3] as number, op[4] as Array<Json>];
        out.push(["p", path, index, remove, items]);
        break;
      }
      case "m":
        out.push(["m", path, (short ? op[1] : op[2]) as Array<number>]);
        break;
    }
  }
  return out;
};

/**
 * Validates the encoded vocabulary: ids and short forms are legal here.
 *
 * @internal
 */
export const assertValidOperation: (u: unknown) => asserts u is Wire.Operation = (u) => {
  if (!Array.isArray(u) || u.length === 0) throw invalidOperation("op is not a tuple");
  const op: ReadonlyArray<unknown> = u;
  const assertRef = (ref: unknown): void => {
    if (typeof ref === "number") {
      if (!Number.isInteger(ref) || ref < 0) throw invalidOperation("bad path id");
      return;
    }
    // A string is not a path. Unchecked, `"a".slice(0, -1)` is `""`, so it resolves to the root and writes
    // there: a path that is not a path, accepted.
    if (!Array.isArray(ref)) throw invalidOperation("path is not an array");
    assertSafePath(ref);
  };
  const assertCount = (value: unknown, message: string): void => {
    if (!Number.isInteger(value) || (value as number) < 0) throw invalidOperation(message);
  };
  switch (op[0]) {
    case "r":
      if (op.length !== 2) throw invalidOperation("r arity");
      return;
    case "s":
      if (op.length === 3) assertRef(op[1]);
      else if (op.length !== 2) throw invalidOperation("s arity");
      return;
    case "d":
      if (op.length === 2) assertRef(op[1]);
      else if (op.length !== 1) throw invalidOperation("d arity");
      return;
    case "a":
      if (op.length === 3) {
        assertRef(op[1]);
        if (typeof op[2] !== "string") throw invalidOperation("a value");
      } else if (op.length === 2) {
        if (typeof op[1] !== "string") throw invalidOperation("a value");
      } else throw invalidOperation("a arity");
      return;
    case "t":
      if (op.length === 3) {
        assertRef(op[1]);
        assertCount(op[2], "t count");
      } else if (op.length === 2) {
        assertCount(op[1], "t count");
      } else throw invalidOperation("t arity");
      return;
    case "p": {
      const [index, remove, items] =
        op.length === 5 ? [op[2], op[3], op[4]] : op.length === 4 ? [op[1], op[2], op[3]] : [];
      if (items === undefined) throw invalidOperation("p arity");
      if (op.length === 5) assertRef(op[1]);
      assertCount(index, "p index");
      assertCount(remove, "p remove");
      if (!Array.isArray(items)) throw invalidOperation("p items");
      return;
    }
    case "m":
      if (op.length === 3) assertRef(op[1]);
      else if (op.length !== 2) throw invalidOperation("m arity");
      assertPermutation(op[op.length - 1]);
      return;
    case "#": {
      if (
        op.length !== 3 ||
        !Number.isInteger(op[1]) ||
        (op[1] as number) < 0 ||
        !Array.isArray(op[2])
      ) {
        throw invalidOperation("# shape");
      }
      assertSafePath(op[2]);
      return;
    }
    // Silently skipping an unknown verb is how a newer producer's operation vanishes.
    default:
      throw invalidOperation(`unknown op verb: ${String(op[0])}`);
  }
};

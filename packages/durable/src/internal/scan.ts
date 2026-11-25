/**
 * Scan order and cursors shared by the built-in backends.
 *
 * @internal
 */
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import { type Cursor, type Page, type ScanOrder, StorageRequestError } from "../AgentStorage.ts";

/** Where a scan starts: its order and the last ID a previous page returned. */
export interface ScanStart {
  readonly order: ScanOrder;
  readonly after: number | undefined;
}

// The cursor is decoded with `Schema` instead of `typeof` checks, because
// `no-runtime-typeof` forbids them.
const BuiltinCursor = Schema.Struct({
  after: Schema.Int,
  // A cursor without an order, written before scans had one, continues in the default order.
  order: Schema.optionalKey(Schema.Literals(["ascending", "descending"])),
});

const decodeCursor = Schema.decodeUnknownOption(BuiltinCursor);

/**
 * Resolve a scan's order and position. A cursor continues in the order it was
 * created with, whether the query repeats that order or omits it; a different
 * `order` is a `StorageRequestError`, and so is a malformed cursor.
 */
export const scanStart = (
  requested: ScanOrder | undefined,
  cursor: Cursor | undefined,
  fallback: ScanOrder,
): Result.Result<ScanStart, StorageRequestError> => {
  if (cursor === undefined)
    return Result.succeed({ order: requested ?? fallback, after: undefined });
  const decoded = decodeCursor(cursor);
  if (Option.isNone(decoded)) {
    return Result.fail(new StorageRequestError({ message: "Invalid storage cursor" }));
  }
  const order = decoded.value.order ?? fallback;
  if (requested !== undefined && requested !== order) {
    return Result.fail(
      new StorageRequestError({
        message: `The cursor continues a ${order} scan; the query asks for ${requested}`,
      }),
    );
  }
  return Result.succeed({ order, after: decoded.value.after });
};

/** The continuation of a scan whose last returned item has `id`. */
export const nextCursor = (id: number, order: ScanOrder): Cursor => ({ after: id, order });

/** The first `limit` of `values`, which holds up to `limit + 1` matches, and a cursor when more remain. */
export const page = <A extends { readonly id: number }>(
  values: ReadonlyArray<A>,
  limit: number,
  order: ScanOrder,
): Page<A> => {
  const items = values.slice(0, limit);
  const last = items.at(-1);
  return {
    items,
    next:
      values.length > limit && last !== undefined
        ? Option.some(nextCursor(last.id, order))
        : Option.none(),
  };
};

/** Every item of a paginated scan, in page order. */
export const scanAll = <A, E, R>(
  scan: (cursor: Cursor | undefined) => Effect.Effect<Page<A>, E, R>,
): Effect.Effect<Array<A>, E, R> =>
  Effect.gen(function* () {
    const items: Array<A> = [];
    let cursor: Cursor | undefined;
    do {
      const page = yield* scan(cursor);
      items.push(...page.items);
      cursor = Option.getOrUndefined(page.next);
    } while (cursor !== undefined);
    return items;
  });

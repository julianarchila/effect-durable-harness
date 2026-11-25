// Pending in-memory waits by key, each settled once, by `resolve`,
// `rejectAll` or its own cancellation.
//
// `add` returns the waiter with its `cancel`, and its `await` cancels it when
// interrupted. A caller that can be interrupted between `add` and `await`
// cancels it itself (`awaitTask` does). Waiters resume after a yield
// (`signal.ts`), as a promise `await` resumes in a microtask.
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import { doneAfterYield } from "./signal.ts";

/** One registered wait. */
export interface Waiter<A, E> {
  /** Waits for the outcome; interrupting it cancels the waiter. */
  readonly await: Effect.Effect<A, E>;
  /** Removes the waiter, if still registered; it then never settles. */
  readonly cancel: () => void;
}

export interface Waiters<K, A, E> {
  /** Registers a waiter under `key`, synchronously: call it in the same step as the check it depends on. */
  readonly add: (key: K) => Waiter<A, E>;
  /** Keys with at least one waiter. */
  readonly keys: () => Array<K>;
  /** Settles every waiter under `key` with `value`. */
  readonly resolve: (key: K, value: A) => void;
  /** Fails every waiter with `error`. */
  readonly rejectAll: (error: E) => void;
  /** Number of registered waiters. */
  readonly size: () => number;
}

/** Makes an empty set of waiters. */
export const make = <K, A, E>(): Waiters<K, A, E> => {
  const sets = new Map<K, Set<Deferred.Deferred<A, E>>>();
  return {
    add: (key) => {
      const deferred = Deferred.makeUnsafe<A, E>();
      const set = sets.get(key) ?? new Set();
      sets.set(key, set.add(deferred));
      const cancel = () => {
        set.delete(deferred);
        if (set.size === 0 && sets.get(key) === set) sets.delete(key);
      };
      return {
        await: Deferred.await(deferred).pipe(Effect.onInterrupt(() => Effect.sync(cancel))),
        cancel,
      };
    },
    keys: () => Array.from(sets.keys()),
    resolve: (key, value) => {
      const set = sets.get(key);
      sets.delete(key);
      for (const deferred of set ?? []) doneAfterYield(deferred, Effect.succeed(value));
    },
    rejectAll: (error) => {
      const all = Array.from(sets.values());
      sets.clear();
      for (const set of all) {
        for (const deferred of set) doneAfterYield(deferred, Effect.fail(error));
      }
    },
    size: () => {
      let count = 0;
      for (const set of sets.values()) count += set.size;
      return count;
    },
  };
};

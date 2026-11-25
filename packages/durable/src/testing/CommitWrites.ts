/**
 * One batch through `AgentStorage.commit`, for tests and suites that seed or
 * write a storage directly.
 *
 * @since 0.0.0
 */
import * as Effect from "effect/Effect";
import type { AgentStorage, StorageFailure, StorageWrite } from "../AgentStorage.ts";
import type { Seq } from "../Ids.ts";

/**
 * Commits `writes` as one batch and returns its seq; fails with the refusal
 * when the storage refused it.
 *
 * @category commits
 * @since 0.0.0
 */
export const commitWrites = (
  storage: AgentStorage["Service"],
  writes: ReadonlyArray<StorageWrite>,
): Effect.Effect<Seq, StorageFailure> =>
  Effect.suspend(() => {
    let stored: Seq | undefined;
    return storage
      .commit([writes], (_index, seq) => {
        stored = seq;
      })
      .pipe(
        Effect.flatMap((result) => {
          if (result.refused !== undefined) return Effect.fail(result.refused);
          // A storage that stores the batch calls `receive` before it settles; one that does not breaks the
          // contract, which is a broken setup in a test.
          if (stored === undefined) {
            return Effect.die(new Error("Storage committed without adopting the batch"));
          }
          return Effect.succeed(stored);
        }),
      );
  });

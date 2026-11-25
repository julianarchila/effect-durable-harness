import * as SqliteClient from "@effect/sql-sqlite-do/SqliteClient";
import * as Cloudflare from "@alchemy.run/cloudflare";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";

class ProbeRollback extends Schema.TaggedError<ProbeRollback>()("ProbeRollback", {}) {}

/**
 * Smallest Alchemy Durable Object over `@effect/sql-sqlite-do`: the fixture
 * the workerd suite grows from (storage conformance on DO SQLite, host tests).
 */
export default class Probe extends Cloudflare.DurableObject<Probe>()(
  "Probe",
  Effect.gen(function* () {
    const state = yield* Cloudflare.DurableObjectState;
    // @effect-diagnostics-next-line returnEffectInGen:off
    return Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient.pipe(
        Effect.provide(SqliteClient.layer({ storage: state.raw.storage })),
      );
      yield* sql`CREATE TABLE IF NOT EXISTS rows (id INTEGER PRIMARY KEY AUTOINCREMENT)`;
      const count = sql<{ readonly n: number }>`SELECT COUNT(*) AS n FROM rows`.pipe(
        Effect.map((result) => result[0]?.n ?? 0),
      );
      const write = (rows: number, rollback: boolean) =>
        sql
          .withTransaction(
            Effect.gen(function* () {
              for (let index = 0; index < rows; index++)
                yield* sql`INSERT INTO rows DEFAULT VALUES`;
              if (rollback) return yield* new ProbeRollback();
            }),
          )
          .pipe(
            Effect.catchTag("ProbeRollback", () => Effect.void),
            Effect.andThen(count),
          );
      return {
        commit: (rows: number) => write(rows, false).pipe(Effect.orDie),
        rollback: (rows: number) => write(rows, true).pipe(Effect.orDie),
      };
    }).pipe(Effect.orDie);
  }),
) {}

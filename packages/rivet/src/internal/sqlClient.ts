/**
 * `effect/sql`'s `SqlClient` over a Rivet Durable Object's SQLite (`Rivet.DurableObjectState.storage`), which neither
 * Alchemy nor `@rivetkit/effect` provides. Shaped after `@effect/sql-sqlite-do`, with two differences Rivet forces
 * (measured on Rivet):
 *
 * - **Top-level transaction = `storage.transaction`.** Alchemy routes a statement to the open RivetKit transaction
 *   through a fiber reference that only `storage.transaction(effect)` provides, so the body runs inside that call.
 *   `SqlClient.makeWithTransaction`'s begin, commit and rollback are no-ops at depth 0: a failing body fails
 *   `storage.transaction`, which rolls back; a successful one commits when it returns.
 * - **Nested transaction = `SAVEPOINT` on the open transaction.** A nested `storage.transaction` opens a new RivetKit
 *   transaction that waits behind the outer one until it times out; a savepoint issued inside the outer one does not.
 *
 * Statements outside a transaction and whole transactions take one permit, as in `sql-sqlite-do`, so a statement of
 * another fiber never lands inside, or waits forever behind, an open transaction.
 *
 * Alchemy's `sql.exec` has no error channel: a failing statement (a constraint, a syntax error) is a defect. Here
 * every defect of a statement becomes a `SqlError`, classified as SQLite's.
 */
import type * as Rivet from "@alchemy.run/rivet";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Reactivity from "effect/reactivity/Reactivity";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import * as Client from "effect/sql/SqlClient";
import type { Connection } from "effect/sql/SqlConnection";
import { classifySqliteError, SqlError } from "effect/sql/SqlError";
import * as Statement from "effect/sql/Statement";

const RivetTransaction = Context.Service<
  Client.TransactionConnection,
  Client.TransactionConnection.Service
>("@effect-durable-harness/rivet/internal/sqlClient/Transaction");

/** RivetKit rolls a transaction back after this long, as Alchemy's default. */
const TRANSACTION_TIMEOUT_MS = 30_000;

const toSqlError = (cause: unknown, operation: string) =>
  new SqlError({
    reason: classifySqliteError(cause, { message: `Failed to ${operation} statement`, operation }),
  });

/** A row as RivetKit returns it, column name to SQLite value; `effect/sql` reads rows untyped. */
type Row = { readonly [column: string]: string | number | bigint | Uint8Array | null };

export const make = (state: Rivet.DurableObjectState["Service"]) =>
  Effect.gen(function* () {
    const storage = state.storage;
    const run = (sql: string, params: ReadonlyArray<unknown>) =>
      Effect.flatMap(storage.sql.exec<Row>(sql, ...params), (cursor) => cursor.toArray()).pipe(
        Effect.catchDefect((defect) => Effect.fail(toSqlError(defect, "execute"))),
      );
    const values = (sql: string, params: ReadonlyArray<unknown>) =>
      Effect.map(run(sql, params), (rows) => rows.map((row) => Object.values(row)));
    const rows = (
      sql: string,
      params: ReadonlyArray<unknown>,
      transformRows: (<A extends object>(rows: ReadonlyArray<A>) => ReadonlyArray<A>) | undefined,
    ) => (transformRows ? Effect.map(run(sql, params), transformRows) : run(sql, params));

    const connection: Connection = {
      execute: rows,
      executeRaw: (sql, params) => run(sql, params),
      executeValues: values,
      executeValuesUnprepared: values,
      executeUnprepared: rows,
      executeStream: (sql, params, transformRows) =>
        Stream.fromIterableEffect(rows(sql, params, transformRows)),
    };

    const permit = yield* Semaphore.make(1);
    const acquirer = Effect.as(
      Effect.acquireRelease(permit.take(1), () => permit.release(1), { interruptible: true }),
      connection,
    );
    const spanAttributes: ReadonlyArray<readonly [string, unknown]> = [
      ["db.system.name", "sqlite"],
    ];

    const control = (sql: string) => Effect.asVoid(run(sql, []));
    const levels = Client.makeWithTransaction({
      transactionService: RivetTransaction,
      spanAttributes,
      // Inside `storage.transaction` already, holding the permit.
      acquireConnection: () => Effect.succeed(connection),
      begin: () => Effect.void,
      commit: () => Effect.void,
      rollback: () => Effect.void,
      savepoint: (_conn, id) => control(`SAVEPOINT effect_sql_${id}`),
      rollbackSavepoint: (_conn, id) => control(`ROLLBACK TO SAVEPOINT effect_sql_${id}`),
      releaseSavepoint: (_conn, id) => control(`RELEASE SAVEPOINT effect_sql_${id}`),
    });

    const withTransaction: Client.SqlClient["withTransaction"] = (effect) =>
      Effect.flatMap(Effect.serviceOption(RivetTransaction), (open) =>
        Option.isSome(open)
          ? levels(effect)
          : permit.withPermit(
              storage.transaction(levels(effect), { timeout: TRANSACTION_TIMEOUT_MS }).pipe(
                // A failure to begin or commit is a defect in Alchemy; it is an unknown outcome here.
                Effect.catchDefect((defect) => Effect.fail(toSqlError(defect, "commit"))),
              ),
            ),
      );

    const client = yield* Client.make({
      driver: "rivet",
      acquirer,
      borrower: (f) => permit.withPermit(f(connection)),
      compiler: Statement.makeCompilerSqlite(),
      transactionService: RivetTransaction,
      spanAttributes,
    }).pipe(Effect.provide(Reactivity.layer));
    // SAFETY: `client` is a `SqlClient`; only `withTransaction` is replaced, with one of the same type.
    return Object.assign(client, { withTransaction }) as Client.SqlClient;
  });

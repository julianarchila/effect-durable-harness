import * as Rivet from "@alchemy.run/rivet";
import * as SqlAgentStorage from "@effect-durable-harness/core/SqlAgentStorage";
import * as AgentStorageConformance from "@effect-durable-harness/core/testing/AgentStorageConformance";
import { Engine } from "@effect-durable-harness/host/Engine";
import * as Cause from "effect/Cause";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/sql/SqlClient";
import * as Host from "../../src/Host.ts";

/** What one conformance case reported: `ok`, or the failure it raised. */
export type CaseOutcome = { readonly ok: true } | { readonly ok: false; readonly failure: string };

/** `StorageConformance`'s one method. */
export interface StorageConformanceMethods {
  readonly run: (index: number) => Effect.Effect<CaseOutcome>;
}

/**
 * The `AgentStorage` conformance suite on `SqlAgentStorage` over the host's `Engine.sql` on Rivet (the actor's
 * SQLite through `internal/sqlClient.ts`), one case per call. Each call uses a fresh actor, so every case starts from
 * an empty database.
 */
export class StorageConformance extends Rivet.DurableObject<
  StorageConformance,
  StorageConformanceMethods
>()("StorageConformance") {}

/** Its implementation, before the pool a target provides. */
export const storageConformance = Effect.gen(function* () {
  // @effect-diagnostics-next-line returnEffectInGen:off
  return Effect.gen(function* () {
    const engine = yield* Layer.build(Host.engine);
    const sql = Context.get(engine, Engine).sql;
    const storage = AgentStorageConformance.layer(
      SqlAgentStorage.layer().pipe(Layer.provide(Layer.succeed(SqlClient.SqlClient, sql))),
    );
    return {
      run: (index: number): Effect.Effect<CaseOutcome> => {
        const conformanceCase = AgentStorageConformance.cases[index];
        if (conformanceCase === undefined) {
          return Effect.succeed({ ok: false, failure: `No conformance case ${index}` });
        }
        return conformanceCase.run.pipe(
          Effect.provide(storage),
          Effect.as<CaseOutcome>({ ok: true }),
          Effect.catchCause((cause) =>
            Effect.succeed<CaseOutcome>({ ok: false, failure: Cause.pretty(cause) }),
          ),
        );
      },
    };
  });
});

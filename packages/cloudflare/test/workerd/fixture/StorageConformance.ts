import * as SqliteClient from "@effect/sql-sqlite-do/SqliteClient";
import * as SqlAgentStorage from "@effect-durable-harness/core/SqlAgentStorage";
import * as AgentStorageConformance from "@effect-durable-harness/core/testing/AgentStorageConformance";
import * as Cloudflare from "@alchemy.run/cloudflare";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";

/** What one conformance case reported: `ok`, or the failure it raised. */
export type CaseOutcome = { readonly ok: true } | { readonly ok: false; readonly failure: string };

/**
 * Runs the `AgentStorage` conformance suite on `SqlAgentStorage` over this
 * object's SQLite (`@effect/sql-sqlite-do`), one case per call. Each call
 * should use a fresh object name, so every case starts from an empty database.
 */
export default class StorageConformance extends Cloudflare.DurableObject<StorageConformance>()(
  "StorageConformance",
  Effect.gen(function* () {
    const state = yield* Cloudflare.DurableObjectState;
    const storage = AgentStorageConformance.layer(
      SqlAgentStorage.layer().pipe(
        Layer.provide(SqliteClient.layer({ storage: state.raw.storage })),
      ),
    );
    // @effect-diagnostics-next-line returnEffectInGen:off
    return Effect.succeed({
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
    });
  }),
) {}

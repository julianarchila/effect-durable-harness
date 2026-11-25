import * as AgentStorageConformance from "@effect-durable-harness/core/testing/AgentStorageConformance";
import * as Alchemy from "alchemy";
import * as Cloudflare from "@alchemy.run/cloudflare";
import * as Test from "@alchemy.run/test/Vitest";
import * as Effect from "effect/Effect";
import * as Schedule from "effect/Schedule";
import * as HttpClient from "effect/http/HttpClient";
import * as HttpClientResponse from "effect/http/HttpClientResponse";
import { expect } from "vitest";
import Stack from "./alchemy.run.ts";

/**
 * The `AgentStorage` conformance suite on `SqlAgentStorage` over
 * `@effect/sql-sqlite-do`, inside an Alchemy Durable Object on local workerd.
 * Each case runs in a fresh object, so it starts from an empty
 * database. `ALCHEMY_TEST_DEV=0` runs the same file against a deployed stage.
 */
const { test, beforeAll, afterAll, deploy, destroy } = Test.make({
  providers: Cloudflare.providers(),
  state: Alchemy.localState(),
  dev: true,
});

const stack = beforeAll(deploy(Stack), { timeout: 300_000 });
afterAll.skipIf(!!process.env.NO_DESTROY)(destroy(Stack), { timeout: 300_000 });

const run = (url: string, name: string, index: number) =>
  Effect.gen(function* () {
    const client = yield* HttpClient.HttpClient;
    const response = yield* client.post(`${url.replace(/\/+$/, "")}/storage/${name}/${index}`).pipe(
      Effect.flatMap(HttpClientResponse.filterStatusOk),
      // A fresh deployment can answer 404 / error 1042 for a few seconds.
      Effect.retry({ schedule: Schedule.spaced("1 second"), times: 30 }),
    );
    return yield* response.json;
  });

for (const [index, conformanceCase] of AgentStorageConformance.cases.entries()) {
  test(
    conformanceCase.name,
    Effect.gen(function* () {
      const { url } = yield* stack;
      expect(yield* run(url, `conformance-${process.pid}-${index}`, index)).toEqual({ ok: true });
    }),
  );
}

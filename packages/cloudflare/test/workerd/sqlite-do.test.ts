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
 * Runs on local workerd (`dev: true`); `ALCHEMY_TEST_DEV=0` and
 * `ALCHEMY_TEST_STAGE=<stage>` run the same file against a deployed stage.
 * See `.claude/skills/verify-on-cloudflare`.
 */
const { test, beforeAll, afterAll, deploy, destroy } = Test.make({
  providers: Cloudflare.providers(),
  state: Alchemy.localState(),
  dev: true,
});

const stack = beforeAll(deploy(Stack), { timeout: 300_000 });
afterAll.skipIf(!!process.env.NO_DESTROY)(destroy(Stack), { timeout: 300_000 });

const post = (url: string, path: string) =>
  Effect.gen(function* () {
    const client = yield* HttpClient.HttpClient;
    const response = yield* client.post(`${url.replace(/\/+$/, "")}${path}`).pipe(
      Effect.flatMap(HttpClientResponse.filterStatusOk),
      // A fresh deployment can answer 404 / error 1042 for a few seconds.
      Effect.retry({ schedule: Schedule.spaced("1 second"), times: 30 }),
    );
    return yield* response.json;
  });

test(
  "sqlite-do withTransaction commits and rolls back inside an Alchemy Durable Object",
  Effect.gen(function* () {
    const { url } = yield* stack;
    const name = `probe-${process.pid}`;
    expect(yield* post(url, `/probe/${name}/commit/3`)).toEqual({ rows: 3 });
    expect(yield* post(url, `/probe/${name}/rollback/3`)).toEqual({ rows: 3 });
  }),
);

import * as Alchemy from "alchemy";
import * as Cloudflare from "@alchemy.run/cloudflare";
import * as Test from "@alchemy.run/test/Vitest";
import * as Effect from "effect/Effect";
import * as Schedule from "effect/Schedule";
import * as Schema from "effect/Schema";
import * as HttpClient from "effect/http/HttpClient";
import * as HttpClientRequest from "effect/http/HttpClientRequest";
import * as HttpClientResponse from "effect/http/HttpClientResponse";
import { expect } from "vitest";
import Stack from "./alchemy.run.ts";

/**
 * One real turn inside the object: `gemini-3.8-flash` through `@effect-durable-harness/opencode`, with a
 * tool call and its result, on local workerd or, with `ALCHEMY_TEST_DEV=0`, a deployed stage. Opt-in: skipped
 * without `GOOGLE_GENERATIVE_AI_API_KEY`, as `doppler run --` provides it.
 */
const skip = process.env["GOOGLE_GENERATIVE_AI_API_KEY"] === undefined;

const { test, beforeAll, afterAll, deploy, destroy } = Test.make({
  providers: Cloudflare.providers(),
  state: Alchemy.localState(),
  dev: true,
});

// Deploying binds the key as a secret, so nothing is deployed without it.
const stack = skip
  ? Effect.die(new Error("skipped"))
  : beforeAll(deploy(Stack), { timeout: 300_000 });
if (!skip) afterAll.skipIf(!!process.env.NO_DESTROY)(destroy(Stack), { timeout: 300_000 });

const TurnResult = Schema.Struct({
  status: Schema.String,
  cost: Schema.Number,
  messages: Schema.Array(
    Schema.Struct({ role: Schema.String, text: Schema.String, tools: Schema.Array(Schema.String) }),
  ),
});

test.skipIf(skip)(
  "a turn with a tool call on Gemini, inside the Durable Object",
  Effect.gen(function* () {
    const { url } = yield* stack;
    const client = yield* HttpClient.HttpClient;
    const request = HttpClientRequest.post(
      `${url.replace(/\/+$/, "")}/live/gemini-${process.pid}/turn`,
    ).pipe(
      HttpClientRequest.bodyJsonUnsafe({
        text: "What is the weather in Bogotá right now?",
        requestId: "turn-1",
      }),
    );
    const response = yield* client
      .execute(request)
      .pipe(
        Effect.flatMap(HttpClientResponse.filterStatusOk),
        Effect.retry({ schedule: Schedule.spaced("1 second"), times: 30 }),
      );
    const result = yield* response.json.pipe(
      Effect.flatMap(Schema.decodeUnknownEffect(TurnResult)),
    );
    expect(result.status).toBe("done");
    expect(result.messages.map((message) => message.role)).toEqual([
      "user",
      "assistant",
      "toolResult",
      "assistant",
    ]);
    expect(result.messages[1]?.tools).toEqual(["get_weather"]);
    expect(result.messages[3]?.text).toContain("22");
    expect(result.cost).toBeGreaterThan(0);
  }),
);

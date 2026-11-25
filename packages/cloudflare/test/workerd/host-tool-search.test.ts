import * as Alchemy from "alchemy";
import * as Cloudflare from "@alchemy.run/cloudflare";
import * as Test from "@alchemy.run/test/Vitest";
import * as Effect from "effect/Effect";
import { expect } from "vitest";
import Stack from "./alchemy.run.ts";
import { call, eventually, objectName, transcript } from "./support.ts";

/**
 * `tool_search` inside the object: its tokenizer segments text without spaces, such as
 * Chinese, with `Intl.Segmenter`. The probe's deferred `repo_issue_search` is described as `搜索仓库问题`; the query
 * `问题` finds it only when workerd's segmenter splits that description into `搜索`, `仓库` and `问题`.
 */
const { test, beforeAll, afterAll, deploy, destroy } = Test.make({
  providers: Cloudflare.providers(),
  state: Alchemy.localState(),
  dev: true,
});

const stack = beforeAll(deploy(Stack), { timeout: 300_000 });
afterAll.skipIf(!!process.env.NO_DESTROY)(destroy(Stack), { timeout: 300_000 });

test(
  "tool_search segments a CJK description with Intl.Segmenter and loads the match",
  Effect.gen(function* () {
    const { url } = yield* stack;
    const name = objectName("tool-search");
    yield* call(url, name, "submit", "search:问题", "turn-1");
    const messages = yield* eventually(transcript(url, name), (current) => current.length === 4);
    expect(messages.map((message) => message.role)).toEqual([
      "user",
      "assistant",
      "toolResult",
      "assistant",
    ]);
    expect(messages[2]?.text).toBe(
      "Loaded 1 tool. They are available from your next call:\n- repo_issue_search: 搜索仓库问题",
    );
  }),
);

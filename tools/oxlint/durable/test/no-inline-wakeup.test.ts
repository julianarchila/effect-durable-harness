import { RuleTester } from "oxlint/plugins-dev";
import { describe, it } from "vitest";

import { noInlineWakeupRule } from "../rules/no-inline-wakeup.ts";

RuleTester.describe = describe;
RuleTester.it = it;

const tester = new RuleTester({
  languageOptions: { sourceType: "module", parserOptions: { lang: "ts" } },
});

tester.run("no-inline-wakeup", noInlineWakeupRule, {
  valid: [
    {
      name: "making and awaiting a deferred",
      code: `import * as Deferred from "effect/Deferred";
const deferred = Deferred.makeUnsafe();
Deferred.await(deferred);`,
    },
    {
      name: "a type-only import",
      code: `import type { succeed } from "effect/Deferred";
succeed(deferred, 1);`,
    },
    {
      name: "a latch opened with open",
      code: `latch.open;`,
    },
    {
      name: "a succeed from another module",
      code: `import * as Effect from "effect/Effect";
Effect.succeed(1);`,
    },
  ],
  invalid: [
    {
      name: "a namespace completion",
      code: `import * as Deferred from "effect/Deferred";
Deferred.succeed(deferred, 1);
Deferred.doneUnsafe(deferred, exit);`,
      errors: [
        { messageId: "deferred", data: { name: "succeed" } },
        { messageId: "deferred", data: { name: "doneUnsafe" } },
      ],
    },
    {
      name: "a named completion, renamed",
      code: `import { done as finish } from "effect/Deferred";
finish(deferred, exit);`,
      errors: [{ messageId: "deferred", data: { name: "done" } }],
    },
    {
      name: "openUnsafe on a latch",
      code: `latch.openUnsafe();`,
      errors: [{ messageId: "latch" }],
    },
  ],
});

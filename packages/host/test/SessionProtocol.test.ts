import { describe, it } from "@effect/vitest";
import { strictEqual } from "@effect/vitest/utils";
import * as Schema from "effect/Schema";
import { ConversationWindow } from "../src/SessionProtocol.ts";

const valid = Schema.is(ConversationWindow);

describe("SessionProtocol.ConversationWindow", () => {
  it("accepts the bounds the kernel accepts", () => {
    strictEqual(valid({}), true);
    strictEqual(valid({ min: 10 }), true);
    strictEqual(valid({ max: 50 }), true);
    strictEqual(valid({ min: 20, max: 20 }), true);
  });

  it("refuses max below min, also below the kernel's default min", () => {
    strictEqual(valid({ min: 150, max: 100 }), false);
    strictEqual(valid({ max: 10 }), false);
  });
});

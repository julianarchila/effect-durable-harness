// `assignJson` against `__proto__` members, checked with `@effect-durable-harness/delta`'s `Tracker`; `JSON.parse` is
// the Schema JSON decoder, which also keeps `__proto__` as an own member.
import { assert, describe, it } from "@effect/vitest";
import * as Tracker from "@effect-durable-harness/delta/Tracker";
import * as Predicate from "effect/Predicate";
import * as Schema from "effect/Schema";
import { assignJson, type JsonRecord } from "../src/internal/json.ts";

type Doc = { message?: Schema.Json };

const parse = Schema.decodeSync(Schema.fromJsonString(Schema.Json));

function assign(tracker: Tracker.Tracker<Doc>, value: Schema.Json): void {
  const change = Tracker.beginChange(tracker);
  // SAFETY: the draft of a JSON document.
  assignJson(change.state as JsonRecord, "message", value);
  Tracker.adopt(tracker, Tracker.prepare(change));
}

describe("assignJson", () => {
  it("does not merge a new __proto__ member into Object.prototype", () => {
    const tracker = Tracker.make<Doc>({});
    assign(tracker, { args: parse('{"path":"x"}') });
    assign(tracker, { args: parse('{"path":"xy","__proto__":{"polluted":true}}') });

    assert.isFalse("polluted" in {});
    const message = tracker.value.message;
    const args = Predicate.hasProperty(message, "args") ? message.args : undefined;
    assert.deepStrictEqual(Object.getOwnPropertyDescriptor(args, "__proto__")?.value, {
      polluted: true,
    });
  });
});

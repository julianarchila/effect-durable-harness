// `Agent.configure`: a change to a conversation's `core.agent` inside the caller's commit, for example in the commit that
// creates it.
import { assert, describe, it } from "@effect/vitest";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Agent from "../src/Agent.ts";
import * as AgentTool from "../src/AgentTool.ts";
import * as Extension from "../src/Extension.ts";
import * as Harness from "../src/Harness.ts";
import * as MemoryAgentStorage from "../src/MemoryAgentStorage.ts";
import { make as makeFaux } from "../src/testing/FauxModel.ts";
import { Tx } from "../src/Tx.ts";

const Lookup = AgentTool.make("lookup", {
  description: "Looks something up.",
  parameters: Schema.Struct({ term: Schema.String }),
  execute: () => Effect.succeed({ output: [] }),
});
const Search = Extension.make({ name: "search", tools: [Lookup] });

describe("Agent.configure", () => {
  it.effect("configures a conversation in the commit that creates it", () =>
    Effect.gen(function* () {
      const context = yield* Layer.build(
        Harness.layer({ extensions: [Search] }).pipe(
          Layer.provide(Layer.mergeAll(MemoryAgentStorage.layer, makeFaux().layer)),
        ),
      );
      const harness = Context.get(context, Harness.Harness);
      const id = yield* harness.commit(
        Effect.gen(function* () {
          const tx = yield* Tx;
          const created = yield* tx.createConversation({ ownership: { kind: "ownerless" } });
          yield* Agent.configure(created.id, {
            model: { provider: "faux", modelId: "faux-1" },
            extensions: [Search],
            tools: [Lookup],
            instructions: "Search first.",
          });
          return created.id;
        }),
      );
      const conversation = yield* harness.conversation(id);
      assert.isTrue(conversation._tag === "Some");
      if (conversation._tag !== "Some") return;
      const agent = yield* conversation.value.agent;
      assert.deepStrictEqual(agent.model, { provider: "faux", modelId: "faux-1" });
      assert.deepStrictEqual(
        agent.tools.map((tool) => tool.name),
        ["lookup"],
      );
      assert.strictEqual(agent.instructions, "Search first.");
    }).pipe(Effect.scoped),
  );
});

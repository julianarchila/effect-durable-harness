// `HarnessOptions.models` and `settings` are read at each use, on every backend.
//
// - `Models` is a service, and the Harness asks it for the model and the stream at every request, never keeping
//   what an earlier request resolved. A host that replaces its model configuration provides a `Models` whose
//   calls read the current one (`switching`). A plain `Models` layer is what every other suite uses.
// - A tool asks for `Models` with `yield*`, the same service the generation uses. The "second-only" provider
//   marks the second `Models`: resolving it tells the two apart.
// - `HarnessOptions.settings` is a reader, called at each use.
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as AgentTool from "../src/AgentTool.ts";
import * as Extension from "../src/Extension.ts";
import type { Conversation } from "../src/Harness.ts";
import { Models } from "../src/Models.ts";
import type { HarnessSettings } from "../src/Settings.ts";
import * as FauxModel from "../src/testing/FauxModel.ts";
import { allEntries, FAUX_MODEL, textOf, waitFor } from "./ChatSupport.ts";
import { openHarness } from "./HarnessSupport.ts";
import { controlledStorage, describeEachBackend, run } from "./SessionSupport.ts";

/** The `Models` service of a fake. */
const serviceOf = (faux: FauxModel.FauxModel) => Effect.provide(Effect.service(Models), faux.layer);

/** A `Models` whose only provider is a fake answering every request with `text`. */
const answering = (text: string) => {
  const faux = FauxModel.make();
  faux.setResponses(
    Array.from({ length: 10 }, () => FauxModel.fauxAssistantMessage([FauxModel.fauxText(text)])),
  );
  return serviceOf(faux);
};

/** A `Models` whose every call goes to the one `current` returns then. */
const switching = (current: () => Models["Service"]): Layer.Layer<Models> =>
  Layer.succeed(
    Models,
    Models.of({
      resolve: (ref) => Effect.suspend(() => current().resolve(ref)),
      stream: (model, request) => Stream.suspend(() => current().stream(model, request)),
    }),
  );

const open = (
  models: Layer.Layer<Models>,
  settings?: () => HarnessSettings,
  extensions: ReadonlyArray<Extension.Any> = [],
) =>
  Effect.gen(function* () {
    const storage = yield* controlledStorage;
    const harness = yield* openHarness(storage, [], { models, settings, extensions });
    const root = yield* harness.harness.ensureRoot({ agent: { model: FAUX_MODEL } });
    return { harness, root };
  });

/** Submits `content`, waits for the answer, and returns the text of the last entry. */
const turn = (root: Conversation, content: string) =>
  Effect.gen(function* () {
    const submission = yield* root.submit({ type: "input", content });
    const fiber = yield* Effect.forkChild(submission.await, { startImmediately: true });
    yield* waitFor(Effect.sync(() => fiber.pollUnsafe() !== undefined));
    assert.strictEqual((yield* Fiber.join(fiber)).status, "done");
    const entries = yield* allEntries(root);
    return textOf(entries.at(-1)?.model?.[0]);
  });

describeEachBackend("HarnessOptions.models and settings as functions", (backend) => {
  it.effect("generation uses the current return value of a models function at each request", () =>
    run(backend)(
      Effect.gen(function* () {
        let current = yield* answering("first");
        const { harness, root } = yield* open(switching(() => current));
        assert.strictEqual(yield* turn(root, "one"), "first");
        current = yield* answering("second");
        assert.strictEqual(yield* turn(root, "two"), "second");
        yield* harness.close;
      }),
    ),
  );

  it.effect("still accepts a plain Models object", () =>
    run(backend)(
      Effect.gen(function* () {
        const faux = FauxModel.make();
        faux.setResponses([FauxModel.fauxAssistantMessage([FauxModel.fauxText("plain")])]);
        const { harness, root } = yield* open(faux.layer);
        assert.strictEqual(yield* turn(root, "one"), "plain");
        yield* harness.close;
      }),
    ),
  );

  it.effect("tools read the current models at each access of api.models", () =>
    run(backend)(
      Effect.gen(function* () {
        const faux = FauxModel.make();
        faux.setResponses([
          FauxModel.fauxAssistantMessage([FauxModel.fauxToolCall("probe", {}, { id: "call-1" })], {
            stopReason: "toolUse",
          }),
          FauxModel.fauxAssistantMessage([FauxModel.fauxText("done")]),
        ]);
        const first = yield* serviceOf(faux);
        // Marks the second Models: only it knows the provider `second-only`.
        const marker = yield* serviceOf(FauxModel.make({ provider: "second-only" }));
        const second = Models.of({
          resolve: (ref) => (ref.provider === "second-only" ? marker : first).resolve(ref),
          stream: (model, request) =>
            (model.provider === "second-only" ? marker : first).stream(model, request),
        });
        let current = first;
        const seen: Array<boolean> = [];
        const knowsSecond = Effect.gen(function* () {
          const models = yield* Models;
          const found = yield* models.resolve({ provider: "second-only", modelId: "faux-1" });
          seen.push(Option.isSome(found));
        });
        const probe = AgentTool.make("probe", {
          description: "Records the models before and after the host swaps them",
          parameters: Schema.Struct({}),
          execute: () =>
            Effect.gen(function* () {
              yield* knowsSecond;
              current = second;
              yield* knowsSecond;
              return { output: [] };
            }),
        });
        const { harness, root } = yield* open(
          switching(() => current),
          undefined,
          [Extension.make({ name: "probe", tools: [probe] })],
        );
        assert.strictEqual(yield* turn(root, "go"), "done");
        assert.deepStrictEqual(seen, [false, true]);
        yield* harness.close;
      }),
    ),
  );

  it.effect("generation reads the current return value of a settings function", () =>
    run(backend)(
      Effect.gen(function* () {
        const timeouts: Array<number | undefined> = [];
        const faux = FauxModel.make();
        const reply: FauxModel.FauxResponseFactory = (request) =>
          Effect.sync(() => {
            timeouts.push(request.options.timeoutMs);
            return FauxModel.fauxAssistantMessage([FauxModel.fauxText("ok")]);
          });
        faux.setResponses([reply, reply]);
        let settings: HarnessSettings = { stream: { timeoutMs: 1000 } };
        const { harness, root } = yield* open(faux.layer, () => settings);
        yield* turn(root, "one");
        settings = { stream: { timeoutMs: 2000 } };
        yield* turn(root, "two");
        assert.deepStrictEqual(timeouts, [1000, 2000]);
        yield* harness.close;
      }),
    ),
  );
});

// A Harness over a controlled storage with the fake model, its root configured with the fake model on first
// creation, and the helpers chat suites share.
//
// Settings are a reader of `setup.settings`, read at each decision, so tests can assign fields between decisions;
// extensions are the static extensions of `openChat`; `faux` is `FauxModel`, whose layer survives a close and a
// reopen; `waitFor` polls by advancing the `TestClock` in 5 ms steps, up to 5 s.
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Predicate from "effect/Predicate";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import type { ExecutionEnvBuilder } from "../src/ExecutionEnv.ts";
import type * as Extension from "../src/Extension.ts";
import type { Conversation } from "../src/Harness.ts";
import type { Message } from "../src/Message.ts";
import type { EntryRecord } from "../src/Records.ts";
import type { Report } from "../src/Report.ts";
import type { HarnessSettings } from "../src/Settings.ts";
import * as FauxModel from "../src/testing/FauxModel.ts";
import { openHarness, type TestHarness } from "./HarnessSupport.ts";
import { type ControlledStorage, flush } from "./SessionSupport.ts";

/** The fake model, settings and reports that survive a close and a reopen, like a host process's own objects. */
export interface ChatSetup {
  readonly faux: FauxModel.FauxModel;
  readonly reports: Array<Report>;
  /** Live Harness settings; tests assign fields to change them between decisions. */
  readonly settings: { -readonly [K in keyof HarnessSettings]: HarnessSettings[K] };
}

/** A fake model and empty settings. */
export const chatSetup = (options: FauxModel.FauxModelOptions = {}): ChatSetup => ({
  faux: FauxModel.make(options),
  reports: [],
  settings: {},
});

/** Options of `openChat`. */
/** `E` is the environment builder's failure. */
export interface OpenChatOptions<E = never> {
  /** The static extensions, installed before opening. */
  readonly extensions?: ReadonlyArray<Extension.Any> | undefined;
  /** Builds each conversation's environment; without it there is none. */
  readonly env?: ExecutionEnvBuilder<never, E> | undefined;
}

/** The fake model's reference, which `openChat` configures on the root. */
export const FAUX_MODEL = { provider: "faux", modelId: "faux-1" } as const;

/** Opens a Harness over `storage` and returns its root, configured with the fake model on creation. */
export const openChat = <E = never>(
  storage: ControlledStorage,
  setup: ChatSetup,
  options: OpenChatOptions<E> = {},
) =>
  Effect.gen(function* () {
    const harness: TestHarness = yield* openHarness(storage, [], {
      extensions: options.extensions,
      env: options.env,
      settings: () => setup.settings,
      models: setup.faux.layer,
      onReport: (report) =>
        Effect.sync(() => {
          setup.reports.push(report);
        }),
    });
    const root: Conversation = yield* harness.harness.ensureRoot({ agent: { model: FAUX_MODEL } });
    return { harness, root };
  });

/** The raw entries of a conversation, oldest first. */
export const allEntries = (conversation: Conversation) =>
  Stream.runCollect(conversation.entries({ order: "ascending" })).pipe(
    Effect.map((entries): ReadonlyArray<EntryRecord> => Array.from(entries)),
  );

/** The text of the first text content of a message. */
export const textOf = (message: Message | undefined): string | undefined => {
  if (message === undefined || message.role === "system") return undefined;
  if (Predicate.isString(message.content)) return message.content;
  const text = message.content.find((content) => content.type === "text");
  return text?.type === "text" ? text.text : undefined;
};

/**
 * Lets work run until `check` holds, for waits that span throttle windows and timers: every step lets pending work
 * run and advances the `TestClock` by 5 ms, up to 1000 steps.
 */
export const waitFor = <E, R>(check: Effect.Effect<boolean, E, R>, steps = 1000) =>
  Effect.gen(function* () {
    for (let step = 0; step < steps; step++) {
      if (yield* check) return;
      yield* flush;
      yield* TestClock.adjust("5 millis");
    }
    return yield* Effect.die(new Error("Condition was not reached"));
  });

/**
 * Lets deferred progress commits (`persistWithinMs: 0`) reach storage before a crash: pending work runs while the
 * `TestClock` advances 50 ms.
 */
export const stored: Effect.Effect<void> = Effect.gen(function* () {
  yield* flush;
  yield* TestClock.adjust("50 millis");
  yield* flush;
});

/**
 * A response that never answers; the run stays busy until its generation is interrupted.
 * `reached` completes once the request was sent, after the generation's preparation and request commits.
 */
export interface Unanswered {
  readonly step: FauxModel.FauxResponseFactory;
  readonly reached: Deferred.Deferred<void>;
}

/** See `Unanswered`. */
export const unanswered = (): Unanswered => {
  const reached = Deferred.makeUnsafe<void>();
  return {
    step: () => Deferred.succeed(reached, undefined).pipe(Effect.andThen(Effect.never)),
    reached,
  };
};

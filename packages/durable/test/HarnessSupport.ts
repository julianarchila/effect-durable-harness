// Test support for the Harness suites: open a Harness over a controlled storage, collect its reports, and build the
// next states the suites commit.
import { assert } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import type * as Layer from "effect/Layer";
import * as Scope from "effect/Scope";
import * as TestClock from "effect/testing/TestClock";
import { AgentStorage } from "../src/AgentStorage.ts";
import * as Extension from "../src/Extension.ts";
import type { HarnessOptions } from "../src/Harness.ts";
import * as internal from "../src/internal/harness.ts";
import type { Models } from "../src/Models.ts";
import * as FauxModel from "../src/testing/FauxModel.ts";
import type { Report } from "../src/Report.ts";
import type * as Task from "../src/Task.ts";
import { type ControlledStorage, controlledStorage, flush, type Gate } from "./SessionSupport.ts";

/** An open Harness, what tests reach behind it, its reports, and the scope that closes it. */
export interface TestHarness extends internal.HarnessKernel {
  readonly storage: ControlledStorage;
  readonly reports: Array<Report>;
  /** Closes the Harness's scope; a second call awaits the same shutdown. */
  readonly close: Effect.Effect<void>;
}

/** `E` is the environment builder's failure. */
export interface OpenHarnessOptions<E = never> {
  readonly settings?: HarnessOptions<ReadonlyArray<Extension.Any>>["settings"];
  readonly env?: HarnessOptions<ReadonlyArray<Extension.Any>, never, E>["env"];
  readonly onReport?: HarnessOptions<ReadonlyArray<Extension.Any>>["onReport"];
  readonly conversationCreated?: HarnessOptions<
    ReadonlyArray<Extension.Any>
  >["conversationCreated"];
  /** Extensions besides the one that holds `tasks`. */
  readonly extensions?: ReadonlyArray<Extension.Any> | undefined;
  /** The model service; default a `FauxModel` with no responses, which suites without generation never call. */
  readonly models?: Layer.Layer<Models> | undefined;
  readonly readOnly?: boolean | undefined;
}

/**
 * Opens a Harness whose registry holds `tasks` over `storage`; failures passed to
 * `onReport` are collected. It does not resume. The Harness lives in a scope of its own, so a test can
 * close it and open another over the same storage, as a reopened process would.
 */
export const openHarness = <E = never>(
  storage: ControlledStorage,
  tasks: ReadonlyArray<Task.Any>,
  options: OpenHarnessOptions<E> = {},
) =>
  Effect.gen(function* () {
    const reports: Array<Report> = [];
    const scope = yield* Scope.make();
    const opened = yield* internal
      .make({
        extensions: [
          ...(tasks.length === 0 ? [] : [Extension.make({ name: "tasks", tasks })]),
          ...(options.extensions ?? []),
        ],
        settings: options.settings,
        env: options.env,
        conversationCreated: options.conversationCreated,
        readOnly: options.readOnly,
        onReport:
          options.onReport ??
          ((report) =>
            Effect.sync(() => {
              reports.push(report);
            })),
      })
      .pipe(
        Effect.provideService(AgentStorage, storage),
        Effect.provide(options.models ?? FauxModel.make().layer),
        Scope.provide(scope),
        Effect.onError((cause) => Scope.close(scope, Exit.failCause(cause))),
      );
    // One shutdown: interrupting a `close` cancels only that wait, and every later
    // `close` awaits the same shutdown. A closed `Scope` returns at once from a second close, so the
    // shutdown runs on a fiber of its own and callers await its end.
    let shutdown: Deferred.Deferred<void> | undefined;
    const close = Effect.suspend(() => {
      if (shutdown !== undefined) return Deferred.await(shutdown);
      const done = Deferred.makeUnsafe<void>();
      shutdown = done;
      // Started at once, so close seals admission in the caller's step.
      return Effect.forkDetach(
        Scope.close(scope, Exit.void).pipe(
          Effect.ensuring(Effect.sync(() => Deferred.doneUnsafe(done, Exit.void))),
        ),
        { startImmediately: true },
      ).pipe(Effect.andThen(Deferred.await(done)));
    });
    // The outer scope closes a Harness the test left open.
    yield* Effect.addFinalizer(() => close);
    return { ...opened, storage, reports, close } satisfies TestHarness;
  });

/**
 * Occupies the Session line with a commit whose callback waits until released. With group commit, holding a storage
 * call no longer holds the line; a waiting callback does.
 */
export const holdLine = (h: Pick<TestHarness, "harness">): Effect.Effect<Gate> =>
  Effect.gen(function* () {
    const entered = yield* Deferred.make<void>();
    const release = yield* Deferred.make<void>();
    yield* Effect.forkDetach(
      h.harness.commit(
        Deferred.succeed(entered, undefined).pipe(Effect.andThen(Deferred.await(release))),
      ),
      { startImmediately: true },
    );
    return {
      entered: Deferred.await(entered),
      release: Deferred.succeed(release, undefined).pipe(Effect.asVoid),
    };
  });

/** Next state that completes a task with `result`. */
export const completed = <R>(result: R) =>
  ({ status: "terminal", outcome: { status: "completed", result } }) as const;

/** Next state that aborts a task. */
export const abortedWith = (reason: string) =>
  ({ status: "terminal", outcome: { status: "aborted", reason } }) as const;

/**
 * Lets forked fibers run until `check` holds, for a bounded number of turns; `attempts` raises the bound where
 * much work must commit first.
 */
export const eventually = <E, R>(check: Effect.Effect<boolean, E, R>, attempts = 200) =>
  Effect.gen(function* () {
    for (let attempt = 0; attempt < attempts; attempt++) {
      if (yield* check) return;
      yield* flush;
    }
    return yield* Effect.die(new Error("Condition was not reached"));
  });

/** Whether a forked fiber has finished once pending work had its turns. */
export const settled = <A, E>(fiber: Fiber.Fiber<A, E>) =>
  flush.pipe(Effect.andThen(Effect.sync(() => fiber.pollUnsafe() !== undefined)));

/** Awaits a forked fiber's exit once it has settled within a bounded number of turns. */
export const awaitBounded = <A, E>(fiber: Fiber.Fiber<A, E>) =>
  eventually(Effect.sync(() => fiber.pollUnsafe() !== undefined)).pipe(
    Effect.andThen(Fiber.await(fiber)),
  );

/** Joins a forked fiber once it has settled within a bounded number of turns. */
export const joined = <A, E>(fiber: Fiber.Fiber<A, E>) =>
  eventually(Effect.sync(() => fiber.pollUnsafe() !== undefined)).pipe(
    Effect.andThen(Fiber.join(fiber)),
  );

/** Awaits a deferred once it is done within a bounded number of turns. */
export const awaited = <A, E>(deferred: Deferred.Deferred<A, E>) =>
  eventually(Deferred.isDone(deferred)).pipe(Effect.andThen(Deferred.await(deferred)));

/** Runs `effect` and returns once it has settled within a bounded number of turns. */
export const bounded = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  Effect.forkChild(effect, { startImmediately: true }).pipe(Effect.flatMap(joined));

/**
 * The end of a Session whose open failed: the Session does not close storage, so its end is
 * that no storage call follows the first `from` of `calls` (`recordCalls`), once pending work and timers had their
 * turns: no reconciliation, sweep or wakeup of a Session left open.
 */
export const assertNoCallsAfter = (calls: ReadonlyArray<string>, from: number) =>
  Effect.gen(function* () {
    yield* flush;
    yield* TestClock.adjust("1 day");
    yield* flush;
    assert.deepStrictEqual(calls.slice(from), []);
  });

/**
 * A wildcard number inside a `deepStrictEqual`: asserts `actual` is a number and returns it, so the surrounding
 * `deepStrictEqual` compares everything else.
 */
export const anyNumber = <A extends number>(actual: A | undefined): A => {
  if (actual === undefined) return assert.fail("expected a number, got undefined");
  assert.isNumber(actual);
  return actual;
};

/** A storage that a test can kill, as a crash would, and the kill. */
export interface Killable {
  readonly storage: ControlledStorage;
  /** From now on every call of `storage` dies without reaching the storage below. */
  readonly kill: Effect.Effect<void>;
}

/**
 * A controlled storage over `inner` that a test can
 * kill, so a crashed Harness writes nothing more, not even the `pending` records a close writes for the tasks it
 * stops. Each Harness gets its own, so killing one leaves `inner` usable by the next, as a reopened file is.
 */
// Calls die once killed rather than hang, so the crashed Session fails and its close settles instead of holding the
// test's scope open.
export const killable = (inner: AgentStorage["Service"]) =>
  Effect.gen(function* () {
    let dead = false;
    const alive = <A, E>(effect: Effect.Effect<A, E>): Effect.Effect<A, E> =>
      Effect.suspend(() => (dead ? Effect.die(new Error("The storage was killed")) : effect));
    const wrapped = AgentStorage.of({
      commit: (batches, receive) => alive(inner.commit(batches, receive)),
      mintId: alive(inner.mintId),
      conversation: (id) => alive(inner.conversation(id)),
      scanConversations: (query, limit, cursor) =>
        alive(inner.scanConversations(query, limit, cursor)),
      entry: (id) => alive(inner.entry(id)),
      visibleEntry: (conversationId, id) => alive(inner.visibleEntry(conversationId, id)),
      findLatestHeadMarker: (conversationId, atOrBefore) =>
        alive(inner.findLatestHeadMarker(conversationId, atOrBefore)),
      scanEntries: (query, limit, cursor) => alive(inner.scanEntries(query, limit, cursor)),
      task: (id) => alive(inner.task(id)),
      scanTasks: (query, limit, cursor) => alive(inner.scanTasks(query, limit, cursor)),
      submission: (id) => alive(inner.submission(id)),
      scanSubmissions: (query, limit, cursor) => alive(inner.scanSubmissions(query, limit, cursor)),
      submissionByRequest: (conversationId, requestId) =>
        alive(inner.submissionByRequest(conversationId, requestId)),
      findDocument: (address, at) => alive(inner.findDocument(address, at)),
      document: (id, at) => alive(inner.document(id, at)),
      scanDocuments: (query, limit, cursor) => alive(inner.scanDocuments(query, limit, cursor)),
    });
    const storage = yield* controlledStorage.pipe(Effect.provideService(AgentStorage, wrapped));
    return {
      storage,
      kill: Effect.sync(() => {
        dead = true;
      }),
    } satisfies Killable;
  });

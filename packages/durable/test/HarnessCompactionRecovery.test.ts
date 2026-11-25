// Part of the compaction suite: compaction estimates and interactions, compaction live status, and compaction
// recovery, on every backend. The header of `HarnessCompaction.test.ts` lists the shared setup; that of this part
// is noted where it applies:
// - A fixed clock is `TestClock.setTime(1_000)`.
// - A failing model lookup is a `Models` layer over the faux one whose `resolve` dies while a flag is set.
// - A task record rewritten in storage is a `commitWrites` of the `running` record, as
//   `HarnessGenerationRecovery` does.
import { assert, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as TestClock from "effect/testing/TestClock";
import * as AgentTool from "../src/AgentTool.ts";
import type { CompactionResult } from "../src/Compaction.ts";
import * as Extension from "../src/Extension.ts";
import type { ConversationId, TaskId } from "../src/Ids.ts";
import type { LiveState } from "../src/Live.ts";
import type { Message } from "../src/Message.ts";
import { Models } from "../src/Models.ts";
import * as Section from "../src/Section.ts";
import type { CompactionPolicy } from "../src/Settings.ts";
import * as Task from "../src/Task.ts";
import { commitWrites } from "../src/testing/CommitWrites.ts";
import { fauxAssistantMessage, fauxToolCall } from "../src/testing/FauxModel.ts";
import { Tx } from "../src/Tx.ts";
import { UsageDoc } from "../src/Usage.ts";
import type { ConversationView } from "../src/View.ts";
import { allEntries, type ChatSetup, chatSetup, openChat } from "./ChatSupport.ts";
import {
  answer,
  type Chat,
  compactionHooks,
  failure,
  finished,
  firstUserText,
  gated,
  history,
  kinds,
  live,
  MANUAL,
  OVERFLOW,
  open,
  placedBy,
  preamble,
  result,
  type Script,
  script,
  settle,
  submissionOf,
  summary,
  text,
  turn,
  userText,
} from "./CompactionSupport.ts";
import { eventually, type TestHarness } from "./HarnessSupport.ts";
import {
  type ControlledStorage,
  controlledStorage,
  describeEachBackend,
  run,
} from "./SessionSupport.ts";

/** Background threshold at 500 and blocking threshold at 1500 tokens of a 2000-token window. */
const BACKGROUND: CompactionPolicy = {
  enabled: true,
  reserveTokens: 500,
  keepRecentTokens: 150,
  backgroundTokens: 1000,
};
/** Blocking threshold at 700 tokens of a 1000-token window, no background compaction. */
const BLOCKING: CompactionPolicy = {
  enabled: true,
  reserveTokens: 300,
  keepRecentTokens: 150,
  backgroundTokens: 0,
};

const compactionTasks = (chat: Chat) =>
  chat.harness.harness.inspect.pipe(
    Effect.map((inspection) =>
      inspection.tasks
        .map((task) => task.record)
        .filter((record) => record.kind === "core.compaction"),
    ),
  );

/** Acquires in a scope of its own; closing it stops or detaches what was acquired. */
const acquire = <A, E>(open: Effect.Effect<A, E, Scope.Scope>) =>
  Effect.gen(function* () {
    const scope = yield* Scope.make();
    const value = yield* Scope.provide(open, scope);
    return { value, release: Scope.close(scope, Exit.void) };
  });

// SAFETY: the mount holds `core.live` as its definition materializes it.
const liveOf = (view: ConversationView) => (view.docs["core.live"] ?? {}) as LiveState;

/** Every view of `conversationId` from now on, one per commit, unfolded. */
const viewHistory = (h: TestHarness, conversationId: ConversationId) =>
  Effect.gen(function* () {
    const views: Array<ConversationView> = [];
    const { release } = yield* acquire(
      h.views.attach(conversationId, undefined, (value) => {
        views.push(value);
        return {
          advance: (frame) => {
            views.push(frame.value);
          },
          closeSession: () => {},
        };
      }),
    );
    return { views, live: (): ReadonlyArray<LiveState> => views.map(liveOf), detach: release };
  });

const getTask = (chat: Chat, id: TaskId) =>
  chat.harness.harness.getTask(id).pipe(Effect.map(Option.getOrUndefined));

/** A child that completes once `gate` opens. */
const childTask = (gate: Deferred.Deferred<void>) =>
  Task.make({
    name: "test.child",
    version: 1,
    input: Schema.Struct({}),
    checkpoint: Schema.Struct({ phase: Schema.Literal("run") }),
    result: Schema.Null,
    initial: () => ({ phase: "run" }),
    phases: {
      run: (_task, runtime) => Deferred.await(gate).pipe(Effect.andThen(runtime.complete(null))),
    },
    abort: (_task, runtime) => runtime.aborted,
  });

describeEachBackend("compaction estimates and interactions", (backend) => {
  for (const clock of ["real", "fixed"] as const) {
    // The clock is the `TestClock`, which also drives sleeps: the fixed variant sets it once and waits only for pending
    // work, which this case needs no timer for, so every entry and message carries the same time.
    it.effect(`ignores usage measured before a summary placed mid-run (${clock} clock)`, () =>
      run(backend)(
        Effect.gen(function* () {
          if (clock === "fixed") yield* TestClock.setTime(1_000);
          const wait = <A, E>(effect: Effect.Effect<A, E>) =>
            clock === "real"
              ? finished(effect)
              : Effect.gen(function* () {
                  const fiber = yield* Effect.forkChild(effect, { startImmediately: true });
                  yield* eventually(
                    Effect.sync(() => fiber.pollUnsafe() !== undefined),
                    2000,
                  );
                  return yield* Fiber.join(fiber);
                });
          const toolGate = Deferred.makeUnsafe<void>();
          const toolReached = Deferred.makeUnsafe<void>();
          const slow = AgentTool.make("slow", {
            description: "slow",
            parameters: Schema.Struct({}),
            execute: () =>
              Effect.gen(function* () {
                yield* Deferred.succeed(toolReached, undefined);
                yield* Deferred.await(toolGate);
                return { output: [{ type: "text" as const, text: text("result", 200) }] };
              }),
          });
          const chat = yield* open({
            contextWindow: 2000,
            extensions: [Extension.make({ name: "tool:slow", tools: [slow] })],
          });
          yield* chat.root.configure({ tools: [slow] });
          const localTurn = (input: string, reply: string) =>
            Effect.gen(function* () {
              chat.faux.agent.push(answer(reply));
              const submission = yield* chat.root.submit({ type: "input", content: input });
              assert.strictEqual((yield* wait(submission.await)).status, "done");
            });
          yield* localTurn(text("u1", 220), text("a1", 220));
          yield* localTurn(text("u2", 220), text("a2", 220));
          yield* localTurn(text("u3", 220), text("a3", 220));
          // Background at 900, blocking at 1500: the tool call's usage plus its result would cross 1500.
          chat.setup.settings.compaction = { ...BACKGROUND, backgroundTokens: 600 };
          // The request starts a background compaction; its tool call's usage measures the whole context.
          chat.faux.agent.push(
            fauxAssistantMessage([fauxToolCall("slow", {})], { stopReason: "toolUse" }),
          );
          chat.faux.agent.push(answer("done"));
          const summaryGate = Deferred.makeUnsafe<void>();
          const summaryReached = Deferred.makeUnsafe<void>();
          chat.faux.summaries.push(gated(summaryGate, summary(), summaryReached));
          const input = yield* chat.root.submit({ type: "input", content: text("u4", 50) });
          yield* wait(Effect.all([Deferred.await(toolReached), Deferred.await(summaryReached)]));
          const [background] = yield* compactionTasks(chat);
          yield* Deferred.succeed(summaryGate, undefined);
          // Queued: the run is busy in its tool round.
          // SAFETY: a compaction task's ID.
          const backgroundId = background!.id as TaskId<CompactionResult>;
          const queued = yield* wait(chat.harness.harness.awaitTask(backgroundId)).pipe(
            Effect.map((record) => record.state.outcome),
          );
          const submission = yield* submissionOf(chat, placedBy(queued));
          assert.strictEqual((yield* submission.status).status, "queued");
          yield* Deferred.succeed(toolGate, undefined);
          assert.strictEqual((yield* wait(input.await)).status, "done");
          // The summary landed at postTools; the successor saw a small context and did not compact again.
          assert.strictEqual(chat.faux.summaryRequests.length, 1);
          assert.deepStrictEqual(yield* compactionTasks(chat), []);
          assert.include(firstUserText(chat.faux.agentRequests.at(-1)!.messages), "SUMMARY");
          yield* chat.harness.close;
        }),
      ),
    );
  }

  it.effect("rebaselines the system prompt over kept system deltas", () =>
    run(backend)(
      Effect.gen(function* () {
        let mood = "cheerful";
        const chat = yield* open({
          extensions: [
            Extension.make({
              name: "section:mood",
              sections: [Section.make("mood", () => Effect.sync(() => mood), { tag: false })],
            }),
          ],
        });
        yield* turn(chat, text("u1", 100), text("a1", 100));
        yield* turn(chat, text("u2", 100), text("a2", 100));
        mood = "terse";
        yield* turn(chat, text("u3", 100), text("a3", 100));
        chat.setup.settings.compaction = { ...MANUAL, keepRecentTokens: 250 };
        chat.faux.summaries.push(summary());
        yield* result(chat, yield* chat.root.compact());
        // The kept range holds the delta for the terse mood; the next request has one complete baseline.
        assert.isTrue(
          (yield* chat.root.context()).entries.some((record) => record.kind === "core.system"),
        );
        yield* turn(chat, "next", "ok");
        const systems = chat.faux.agentRequests
          .at(-1)!
          .messages.filter((message) => message.role === "system");
        assert.strictEqual(systems.length, 1);
        const baseline = systems[0];
        assert.deepStrictEqual(baseline?.role === "system" ? baseline.sections : undefined, {
          preamble: "You are helpful.",
          mood: "terse",
        });
        yield* chat.harness.close;
      }),
    ),
  );

  it.effect("summarizes a replaced entry's replacement and shows it to the hook", () =>
    run(backend)(
      Effect.gen(function* () {
        let messages: ReadonlyArray<Message> = [];
        const chat = yield* open({
          extensions: [
            compactionHooks({
              beforeCompact: (compaction) =>
                Effect.sync(() => {
                  messages = compaction.messages;
                  return undefined;
                }),
            }),
          ],
        });
        yield* history(chat);
        const [u1] = yield* allEntries(chat.root);
        yield* chat.root.submit({
          type: "write",
          entry: {
            kind: "app.redact",
            edits: [
              {
                target: u1!.id,
                action: "replace",
                messages: [{ role: "user", content: "REDACTED", timestamp: 0 }],
              },
            ],
          },
        });
        chat.faux.summaries.push(summary());
        yield* result(chat, yield* chat.root.compact());
        assert.strictEqual(firstUserText(messages), "REDACTED");
        assert.include(userText(chat.faux.summaryRequests[0]!.messages[1]), "[User]: REDACTED");
        yield* chat.harness.close;
      }),
    ),
  );

  it.effect("compacts a fork whose cut falls on a parent entry", () =>
    run(backend)(
      Effect.gen(function* () {
        const chat = yield* open();
        yield* history(chat);
        const entries = yield* allEntries(chat.root);
        const fork = yield* chat.root.fork(entries.at(-1)!.id, {
          ownership: { kind: "ownerless" },
        });
        chat.faux.summaries.push(summary());
        const outcome = yield* result(chat, yield* fork.compact());
        assert.strictEqual(
          (yield* settle(yield* submissionOf(chat, placedBy(outcome)))).status,
          "done",
        );
        const u3 = entries.find((record) => userText(record.model?.[0]).startsWith("u3"))!;
        const view = yield* fork.context();
        assert.strictEqual(view.head?.kind, "core.compaction");
        assert.strictEqual(view.head?.head, u3.id);
        assert.strictEqual(view.head?.conversationId, fork.id);
        assert.deepStrictEqual(view.messages.slice(1).map(userText), [
          text("u3", 100),
          text("a3", 100),
        ]);
        // The parent is untouched.
        assert.isFalse((yield* kinds(chat.root)).includes("core.compaction"));
        // The fork's view shows the parent entries in ID order, the summary after them, its head on a parent entry.
        const { value: watch, release } = yield* acquire(fork.watch());
        assert.deepStrictEqual(watch.value.entries, yield* allEntries(fork));
        assert.deepStrictEqual(watch.value.entries.at(-1), view.head);
        assert.isTrue(watch.value.entries.some((each) => each.id === u3.id));
        yield* release;
        yield* chat.harness.close;
      }),
    ),
  );

  it.effect("settles stale in a fork reset while it summarizes", () =>
    run(backend)(
      Effect.gen(function* () {
        const chat = yield* open();
        yield* history(chat);
        const entries = yield* allEntries(chat.root);
        const fork = yield* chat.root.fork(entries.at(-1)!.id, {
          ownership: { kind: "ownerless" },
        });
        const gate = Deferred.makeUnsafe<void>();
        const reached = Deferred.makeUnsafe<void>();
        chat.faux.summaries.push(gated(gate, summary(), reached));
        const id = yield* fork.compact();
        yield* finished(Deferred.await(reached));
        yield* fork.reset();
        yield* Deferred.succeed(gate, undefined);
        const outcome = yield* result(chat, id);
        const submission = yield* submissionOf(chat, placedBy(outcome));
        const status = yield* submission.status;
        assert.strictEqual(status.status, "unanswered");
        assert.strictEqual(status.status === "unanswered" ? status.reason : undefined, "stale");
        assert.strictEqual((yield* fork.context()).head?.kind, "core.reset");
        yield* chat.harness.close;
      }),
    ),
  );

  it.effect(
    "places an older queued summary and the current one together when idle admission drains the inbox",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const chat = yield* open();
          yield* history(chat);
          const gate = Deferred.makeUnsafe<void>();
          const reached = Deferred.makeUnsafe<void>();
          chat.faux.agent.push(gated(gate, failure("bad request"), reached));
          const failed = yield* chat.root.submit({ type: "input", content: "fails" });
          yield* finished(Deferred.await(reached));
          chat.faux.summaries.push(summary("OLDER"));
          const older = yield* result(chat, yield* chat.root.compact());
          yield* Deferred.succeed(gate, undefined);
          yield* settle(failed);
          // Idle now, with the older summary still queued; the current one queues behind it and a final boundary
          // runs.
          chat.faux.summaries.push(summary("CURRENT"));
          const current = yield* result(chat, yield* chat.root.compact());
          const statuses: Array<string> = [];
          for (const outcome of [older, current]) {
            statuses.push((yield* (yield* submissionOf(chat, placedBy(outcome))).status).status);
          }
          assert.deepStrictEqual(statuses, ["done", "done"]);
          assert.include(firstUserText((yield* chat.root.context()).messages), "CURRENT");
          yield* chat.harness.close;
        }),
      ),
  );

  it.effect("places a hook's summary and holds while work the hook created runs", () =>
    run(backend)(
      Effect.gen(function* () {
        const childGate = Deferred.makeUnsafe<void>();
        const Child = childTask(childGate);
        let current: Chat | undefined;
        const chat = yield* open({
          extensions: [
            Extension.make({ name: "tasks", tasks: [Child] }),
            compactionHooks({
              beforeCompact: (_compaction, hook) =>
                Effect.gen(function* () {
                  const opened = current!;
                  yield* opened.harness.harness.commit(
                    Effect.gen(function* () {
                      const tx = yield* Tx;
                      yield* tx.createTask(
                        Child,
                        {},
                        {
                          ownership: { kind: "task", taskId: hook.taskId },
                          conversationId: opened.root.id,
                        },
                      );
                    }),
                  );
                  return { summary: "HOOK" };
                }).pipe(Effect.orDie),
            }),
          ],
        });
        current = chat;
        yield* history(chat);
        const id = yield* chat.root.compact();
        yield* finished(
          Effect.gen(function* () {
            while ((yield* getTask(chat, id))?.state.status !== "completing") {
              yield* Effect.yieldNow;
            }
          }),
        );
        // The summary and the status removal landed at the hold.
        assert.strictEqual((yield* kinds(chat.root)).at(-1), "core.compaction");
        assert.isUndefined((yield* live(chat)).compactions);
        yield* Deferred.succeed(childGate, undefined);
        assert.strictEqual((yield* result(chat, id)).status, "completed");
        yield* chat.harness.close;
      }),
    ),
  );

  // The model lookup is `Models.resolve`, here a
  // layer over the faux model's that dies while `broken` is set.
  it.effect("removes the status of a faulted compaction", () =>
    run(backend)(
      Effect.gen(function* () {
        const base = chatSetup({
          models: [{ id: "faux-1", contextWindow: 100_000, maxTokens: 900 }],
        });
        const faux = script(base);
        let broken = false;
        const layer = Layer.effect(
          Models,
          Effect.map(Effect.service(Models), (models) =>
            Models.of({
              resolve: (ref) =>
                Effect.suspend(() =>
                  broken ? Effect.die(new Error("models broke")) : models.resolve(ref),
                ),
              stream: models.stream,
            }),
          ),
        ).pipe(Layer.provide(base.faux.layer));
        const setup: ChatSetup = { ...base, faux: { ...base.faux, layer } };
        const chat = yield* open({}, { setup, faux });
        yield* history(chat);
        broken = true;
        const outcome = yield* result(chat, yield* chat.root.compact());
        broken = false;
        assert.strictEqual(outcome.status, "faulted");
        assert.strictEqual(
          outcome.status === "faulted" ? outcome.error.message : undefined,
          "models broke",
        );
        assert.isUndefined((yield* live(chat)).compactions);
        yield* chat.harness.close;
      }),
    ),
  );
});

describeEachBackend("compaction live status", (backend) => {
  it.effect(
    "lists a compaction from start to end, with its retry backoff in a late joiner's view",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const chat = yield* open();
          yield* history(chat);
          chat.setup.settings.retry = { enabled: true, maxRetries: 2, baseDelayMs: 60_000 };
          const views = yield* viewHistory(chat.harness, chat.root.id);
          chat.faux.summaries.push(failure("overloaded"));
          const id = yield* chat.root.compact();
          yield* finished(
            Effect.gen(function* () {
              while ((yield* live(chat)).compactions?.[0]?.retry === undefined)
                yield* Effect.yieldNow;
            }),
          );
          const { value: late, release } = yield* acquire(chat.root.watch());
          const compactions = liveOf(late.value).compactions;
          const at = compactions?.[0]?.retry?.at;
          assert.isNumber(at);
          assert.deepStrictEqual(compactions, [
            {
              taskId: id,
              trigger: "manual",
              blocking: false,
              attempt: 1,
              retry: { at: at!, error: "overloaded" },
            },
          ]);
          yield* release;
          yield* chat.harness.harness.abortTask(id);
          yield* finished(
            Effect.gen(function* () {
              while ((yield* live(chat)).compactions !== undefined) yield* Effect.yieldNow;
            }),
          );
          yield* views.detach;
          const listed = views.live().flatMap((state) => state.compactions ?? []);
          assert.deepStrictEqual(listed[0], {
            taskId: id,
            trigger: "manual",
            blocking: false,
            attempt: 1,
          });
          // An abort is not a failure.
          assert.isUndefined(views.live().at(-1)!.compactionFailure);
          yield* chat.harness.close;
        }),
      ),
  );

  it.effect("lists concurrent compactions in task ID order", () =>
    run(backend)(
      Effect.gen(function* () {
        const chat = yield* open();
        yield* history(chat);
        const first = Deferred.makeUnsafe<void>();
        const second = Deferred.makeUnsafe<void>();
        chat.faux.summaries.push(
          gated(Deferred.makeUnsafe<void>(), summary(), first),
          gated(Deferred.makeUnsafe<void>(), summary(), second),
        );
        const a = yield* chat.root.compact();
        const b = yield* chat.root.compact();
        yield* finished(Effect.all([Deferred.await(first), Deferred.await(second)]));
        assert.deepStrictEqual(
          (yield* live(chat)).compactions?.map((status) => status.taskId),
          [a, b],
        );
        yield* finished(chat.root.abort());
        assert.isUndefined((yield* live(chat)).compactions);
        yield* chat.harness.close;
      }),
    ),
  );
});

// ─── Recovery ─────────────────────────────────────────────────────────────

// A reopen is a new Harness over it with the same setup and
// script, which closing the first Harness leaves as it stored them.
const reopen = (
  storage: ControlledStorage,
  setup: ChatSetup,
  faux: Script,
  extensions: ReadonlyArray<Extension.Any> = [],
) =>
  Effect.gen(function* () {
    const { harness, root } = yield* openChat(storage, setup, {
      extensions: [preamble, ...extensions],
    });
    yield* harness.harness.resume;
    return { harness, root, setup, faux } satisfies Chat;
  });

const first = (storage: ControlledStorage, extensions: ReadonlyArray<Extension.Any> = []) =>
  Effect.gen(function* () {
    const setup = chatSetup();
    const faux = script(setup);
    const { harness, root } = yield* openChat(storage, setup, {
      extensions: [preamble, ...extensions],
    });
    setup.settings.compaction = MANUAL;
    setup.settings.retry = { enabled: true, maxRetries: 2, baseDelayMs: 1 };
    yield* harness.harness.resume;
    const chat: Chat = { harness, root, setup, faux };
    yield* history(chat);
    return chat;
  });

const usage = (chat: Chat) =>
  chat.harness.session
    .snapshot(UsageDoc, chat.root.id)
    .pipe(Effect.map((value) => Option.getOrThrow(value).models["faux/faux-1"]!));

/** Messages with their timestamps zeroed. */
const untimed = (messages: ReadonlyArray<Message>) =>
  messages.map((message) => ({ ...message, timestamp: 0 }));

describeEachBackend("compaction recovery", (backend) => {
  it.effect("repeats nothing after reopen once the summary is placed", () =>
    run(backend)(
      Effect.gen(function* () {
        const storage = yield* controlledStorage;
        let chat = yield* first(storage);
        chat.faux.summaries.push(summary());
        yield* result(chat, yield* chat.root.compact());
        const entries = yield* allEntries(chat.root);
        const before = yield* chat.harness.session.snapshot(UsageDoc, chat.root.id);
        yield* chat.harness.close;
        chat = yield* reopen(storage, chat.setup, chat.faux);
        yield* finished(chat.harness.harness.awaitIdle);
        assert.strictEqual(chat.faux.summaryRequests.length, 1);
        assert.deepStrictEqual(yield* allEntries(chat.root), entries);
        assert.deepStrictEqual(
          yield* chat.harness.session.snapshot(UsageDoc, chat.root.id),
          before,
        );
        const inspection = yield* chat.harness.harness.inspect;
        assert.deepStrictEqual([inspection.tasks, inspection.submissions], [[], []]);
        yield* chat.harness.close;
      }),
    ),
  );

  it.effect("fails an overflow run with its text when its compaction fails after reopen", () =>
    run(backend)(
      Effect.gen(function* () {
        const storage = yield* controlledStorage;
        let chat = yield* first(storage);
        chat.setup.settings.compaction = { ...MANUAL, enabled: true };
        const reached = Deferred.makeUnsafe<void>();
        chat.faux.summaries.push(gated(Deferred.makeUnsafe<void>(), summary(), reached));
        chat.faux.agent.push(failure(OVERFLOW));
        const input = yield* chat.root.submit({ type: "input", content: text("u4", 100) });
        yield* finished(Deferred.await(reached));
        const generation = (yield* live(chat)).run!.taskId;
        const state = (yield* getTask(chat, generation))?.state;
        assert.strictEqual(state?.status, "waiting");
        assert.deepInclude(state?.status === "waiting" ? state.checkpoint : {}, {
          phase: "prepare",
          overflow: OVERFLOW,
        });
        yield* chat.harness.close;
        chat = yield* reopen(storage, chat.setup, chat.faux);
        chat.faux.summaries.push(failure("bad request"));
        const settled = yield* settle(
          Option.getOrThrow(yield* chat.harness.harness.submission(input.id)),
        );
        assert.strictEqual(settled.status, "unanswered");
        assert.strictEqual(
          settled.status === "unanswered" ? settled.reason : undefined,
          "model_error",
        );
        assert.strictEqual(settled.status === "unanswered" ? settled.detail : undefined, OVERFLOW);
        yield* chat.harness.close;
      }),
    ),
  );

  it.effect("reruns select and its hook after a crash in select", () =>
    run(backend)(
      Effect.gen(function* () {
        const storage = yield* controlledStorage;
        let calls = 0;
        const reached = Deferred.makeUnsafe<void>();
        // With static extensions both Harnesses are built with the hook. The hook's wait is one that close
        // interrupts.
        const hooks = compactionHooks({
          beforeCompact: () =>
            Effect.gen(function* () {
              calls++;
              if (calls === 1) {
                yield* Deferred.succeed(reached, undefined);
                return yield* Effect.never;
              }
              return undefined;
            }),
        });
        let chat = yield* first(storage, [hooks]);
        const id = yield* chat.root.compact();
        yield* finished(Deferred.await(reached));
        yield* chat.harness.close;
        chat = yield* reopen(storage, chat.setup, chat.faux, [hooks]);
        chat.faux.summaries.push(summary());
        assert.strictEqual((yield* result(chat, id)).status, "completed");
        assert.strictEqual(calls, 2);
        yield* chat.harness.close;
      }),
    ),
  );

  it.effect("resends an interrupted summarization once and counts only the answered attempt", () =>
    run(backend)(
      Effect.gen(function* () {
        const storage = yield* controlledStorage;
        let chat = yield* first(storage);
        const reached = Deferred.makeUnsafe<void>();
        chat.faux.summaries.push(gated(Deferred.makeUnsafe<void>(), summary(), reached));
        const id = yield* chat.root.compact();
        yield* finished(Deferred.await(reached));
        const usageBefore = yield* usage(chat);
        const state = (yield* getTask(chat, id))?.state;
        assert.deepInclude(state !== undefined && "checkpoint" in state ? state.checkpoint : {}, {
          phase: "summarize",
        });
        yield* chat.harness.close;
        chat = yield* reopen(storage, chat.setup, chat.faux);
        chat.faux.summaries.push(summary());
        assert.strictEqual((yield* result(chat, id)).status, "completed");
        assert.strictEqual(chat.faux.summaryRequests.length, 2);
        const [sent, resent] = chat.faux.summaryRequests;
        // Timestamps aside: the resend is stamped when it is built.
        assert.deepStrictEqual(untimed(resent!.messages), untimed(sent!.messages));
        const after = yield* usage(chat);
        assert.isAbove(after.input, usageBefore.input);
        assert.strictEqual(after.output - usageBefore.output, 2);
        yield* chat.harness.close;
      }),
    ),
  );

  // The clock is the `TestClock`, and the waits for the sleep let work run without advancing it; the record of an
  // earlier version is written straight to storage (as `HarnessGenerationRecovery`'s generation case does).
  it.effect("resumes a retry backoff after reopen", () =>
    run(backend)(
      Effect.gen(function* () {
        const storage = yield* controlledStorage;
        let chat = yield* first(storage);
        const now = 1_000_000;
        yield* TestClock.setTime(now);
        chat.setup.settings.retry = { enabled: true, maxRetries: 2, baseDelayMs: 60_000 };
        chat.faux.summaries.push(failure("overloaded"));
        const id = yield* chat.root.compact();
        const sleeping = (opened: Chat) =>
          eventually(
            Effect.map(getTask(opened, id), (record) => record?.state.status === "sleeping"),
            2000,
          );
        yield* sleeping(chat);
        const until = now + 60_000;
        const state = (yield* getTask(chat, id))?.state;
        assert.strictEqual(state?.status, "sleeping");
        if (state?.status !== "sleeping") return;
        assert.strictEqual(state.until, until);
        assert.deepInclude(state.checkpoint, { phase: "retry", attempt: 1, until });
        yield* chat.harness.close;
        // Earlier versions slept inside a running `retry` phase: reopened before its time, it sleeps again.
        const stored = Option.getOrThrow(yield* storage.task(id));
        if (stored.state.status !== "sleeping") return assert.fail("expected a sleeping task");
        yield* commitWrites(storage, [
          {
            type: "task",
            value: { ...stored, state: { status: "running", checkpoint: stored.state.checkpoint } },
          },
        ]);
        chat = yield* reopen(storage, chat.setup, chat.faux);
        yield* sleeping(chat);
        assert.strictEqual(chat.faux.summaryRequests.length, 1);
        yield* chat.harness.close;
        yield* TestClock.setTime(now + 120_000);
        chat = yield* reopen(storage, chat.setup, chat.faux);
        chat.faux.summaries.push(summary());
        assert.strictEqual((yield* result(chat, id)).status, "completed");
        assert.strictEqual(chat.faux.summaryRequests.length, 2);
        yield* chat.harness.close;
      }),
    ),
  );

  it.effect("keeps a generation waiting on its blocking compaction across reopen", () =>
    run(backend)(
      Effect.gen(function* () {
        const storage = yield* controlledStorage;
        let chat = yield* first(storage);
        const setup = chat.setup;
        const reached = Deferred.makeUnsafe<void>();
        chat.faux.summaries.push(gated(Deferred.makeUnsafe<void>(), summary(), reached));
        // The faux model window is 128k; this blocking threshold needs a small context window.
        chat.setup.settings.compaction = { ...BLOCKING, reserveTokens: 128_000 - 700 };
        const input = yield* chat.root.submit({ type: "input", content: text("u4", 200) });
        yield* finished(Deferred.await(reached));
        yield* chat.harness.close;
        chat = yield* reopen(storage, setup, chat.faux);
        chat.faux.summaries.push(summary());
        chat.faux.agent.push(answer("a4"));
        const settled = yield* settle(
          Option.getOrThrow(yield* chat.harness.harness.submission(input.id)),
        );
        assert.strictEqual(settled.status, "done");
        assert.deepStrictEqual((yield* kinds(chat.root)).slice(-3), [
          "core.compaction",
          "core.system",
          "core.assistant",
        ]);
        yield* chat.harness.close;
      }),
    ),
  );

  it.effect("keeps a queued summary across reopen and places it at the next boundary", () =>
    run(backend)(
      Effect.gen(function* () {
        const storage = yield* controlledStorage;
        let chat = yield* first(storage);
        const reached = Deferred.makeUnsafe<void>();
        chat.faux.agent.push(gated(Deferred.makeUnsafe<void>(), answer("never"), reached));
        const input = yield* chat.root.submit({ type: "input", content: "busy" });
        yield* finished(Deferred.await(reached));
        chat.faux.summaries.push(summary());
        const outcome = yield* result(chat, yield* chat.root.compact());
        yield* chat.harness.close;
        chat = yield* reopen(storage, chat.setup, chat.faux);
        chat.faux.agent.push(answer("answered"));
        const settled = yield* settle(
          Option.getOrThrow(yield* chat.harness.harness.submission(input.id)),
        );
        assert.strictEqual(settled.status, "done");
        assert.strictEqual(
          (yield* settle(yield* submissionOf(chat, placedBy(outcome)))).status,
          "done",
        );
        yield* chat.harness.close;
      }),
    ),
  );
});

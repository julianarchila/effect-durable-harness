// The partial answer and tool output reach watches at once and storage at the next commit that reads or writes
// `core.live` or within `settings.progress.persistWithinMs` (`PROGRESS_COMMIT`), on every backend.
//
// - A crash is `HarnessSupport.killable`: every later storage call of the crashed Harness dies, so its close
//   stores nothing held, and the next Harness opens over the same storage of the backend.
// - Commits are observed with the kernel's `followCommits` and `Session.observeCommits`.
// - Waits run on the `TestClock` (`ChatSupport.waitFor`, 5 ms steps); an aborted call is a fiber that never
//   returns until it is interrupted.
// - "never counts progress commits as progress of a task cut off again and again": `restarts` is counted at open,
//   so it is read from the reopened Harness's task.
// - "resumes polling a deferred response after a crash": the clock is set with `TestClock.setTime`; the reopened
//   Harness runs tasks at once, so its poll sleeps until the clock reaches it.
import { assert, it } from "@effect/vitest";
import * as Delta from "@effect-durable-harness/delta/Delta";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import * as AgentTool from "../src/AgentTool.ts";
import { AgentStorage, type DocumentAddress } from "../src/AgentStorage.ts";
import { ToolContext } from "../src/AgentTool.ts";
import { AssistantEntry, ToolResultEntry } from "../src/Entry.ts";
import * as Extension from "../src/Extension.ts";
import type { ConversationId } from "../src/Ids.ts";
import { LiveDoc, type LiveState } from "../src/Live.ts";
import type { EntryRecord } from "../src/Records.ts";
import * as Task from "../src/Task.ts";
import { fauxAssistantMessage, fauxText, fauxToolCall } from "../src/testing/FauxModel.ts";
import { Tx } from "../src/Tx.ts";
import { allEntries, type ChatSetup, chatSetup, openChat, textOf, waitFor } from "./ChatSupport.ts";
import { completed, killable, type TestHarness } from "./HarnessSupport.ts";
import { describeEachBackend, run } from "./SessionSupport.ts";

type Inner = AgentStorage["Service"];

/** A slow stream: 4 characters 80 times a second. */
const slowSetup = (persistWithinMs: number): ChatSetup => {
  const setup = chatSetup({ tokensPerSecond: 80, tokenSize: { min: 4, max: 4 } });
  setup.settings.progress = { persistWithinMs };
  // A request a crash interrupted is a failed attempt: retry at once.
  setup.settings.retry = { enabled: true, maxRetries: 5, baseDelayMs: 1 };
  return setup;
};

const LONG = "streamed ".repeat(400);

/** Open (or reopen) a Harness over `inner` and let it run tasks; `crash` kills it as a dead process. */
const open = (inner: Inner, setup: ChatSetup, extensions: ReadonlyArray<Extension.Any> = []) =>
  Effect.gen(function* () {
    const crashable = yield* killable(inner);
    const opened = yield* openChat(crashable.storage, setup, { extensions });
    yield* opened.harness.harness.resume;
    const memory = yield* memoryPartials(opened.harness);
    return {
      ...opened,
      storage: crashable.storage,
      memory,
      crash: crashable.kill.pipe(Effect.andThen(opened.harness.close)),
    };
  });

/** The partial text of every `core.live` revision memory adopted, the last one last. */
const memoryPartials = (h: TestHarness) =>
  Effect.gen(function* () {
    const texts: Array<string> = [];
    yield* h.kernel.followCommits((publication) => {
      for (const change of publication.changes) {
        if (
          change.type !== "document" ||
          change.record.kind !== "core.live" ||
          change.value === null
        )
          continue;
        // SAFETY: a published `core.live` value, which its schema checked when it was written.
        const message = (change.value as LiveState).generation?.message;
        if (message !== undefined) texts.push(textOf(message) ?? "");
      }
    });
    return texts;
  });

const liveAddress = (conversationId: ConversationId): DocumentAddress => ({
  kind: LiveDoc.definition.kind,
  scope: { kind: "conversation", conversationId },
});

/** `core.live` as storage has it, read past the Session, also after a crash closed it. */
const storedLive = (inner: Inner, conversationId: ConversationId) =>
  Effect.gen(function* () {
    const record = yield* inner.findDocument(liveAddress(conversationId), "current");
    if (Option.isNone(record)) return undefined;
    const stored = yield* inner.document(record.value.id, "current");
    // SAFETY: storage values are untyped; this one is `core.live`.
    return Option.getOrUndefined(Option.map(stored, (document) => document.value as LiveState));
  });

const storedText = (live: LiveState | undefined): string | undefined => {
  const message = live?.generation?.message;
  return message === undefined ? undefined : (textOf(message) ?? "");
};

const storedTextOf = (inner: Inner, conversationId: ConversationId) =>
  Effect.map(storedLive(inner, conversationId), storedText);

/** Assistant entries: their text and stop reason. */
const answers = (entries: ReadonlyArray<EntryRecord>) =>
  entries.filter(AssistantEntry.is).map((entry) => {
    const message = entry.model?.[0];
    return {
      text: textOf(message),
      stopReason: message?.role === "assistant" ? message.stopReason : undefined,
    };
  });

/** Waits for a submission found by ID, while the `TestClock` advances. */
const settled = (h: TestHarness, id: Parameters<TestHarness["harness"]["submission"]>[0]) =>
  Effect.gen(function* () {
    const submission = Option.getOrThrow(yield* h.harness.submission(id));
    let result: Effect.Success<typeof submission.await> | undefined;
    const fiber = yield* Effect.forkChild(
      Effect.tap(submission.await, (value) =>
        Effect.sync(() => {
          result = value;
        }),
      ),
      { startImmediately: true },
    );
    yield* waitFor(Effect.sync(() => fiber.pollUnsafe() !== undefined));
    return result!;
  });

describeEachBackend("generation partials", (backend) => {
  it.effect("hold partials in memory and store them together, at most every persistWithinMs", () =>
    run(backend)(
      Effect.gen(function* () {
        const inner = yield* AgentStorage;
        const setup = slowSetup(400);
        setup.faux.setResponses([fauxAssistantMessage(LONG.slice(0, 640))]);
        const { harness, root, memory, storage } = yield* open(inner, setup);
        const from = storage.commits.length;
        const id = (yield* root.submit({ type: "input", content: "hi" })).id;
        yield* settled(harness, id);
        const writesLive = storage.commits
          .slice(from)
          .filter((batch) => batch.some((write) => write.type === "document.change"));
        // About 2 s of streaming at 100 ms per partial: some 20 partials in memory, a store about every 400 ms.
        assert.isAbove(memory.length, 12);
        assert.isBelow(writesLive.length, memory.length / 1.5);
        yield* harness.close;
      }),
    ),
  );

  it.effect(
    "converts what was stored into the aborted entry after a crash, and retries the charged attempt",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const inner = yield* AgentStorage;
          const setup = slowSetup(150);
          setup.faux.setResponses([fauxAssistantMessage(LONG)]);
          let opened = yield* open(inner, setup);
          // A replica fed by `observeCommits` alone, as a replication feed would be.
          let replica: Schema.Json | undefined = yield* storedLive(inner, opened.root.id);
          yield* opened.harness.session.observeCommits({
            advance: (publication) => {
              for (const change of publication.changes) {
                if (
                  change.type === "document" &&
                  change.value !== null &&
                  change.record.kind === LiveDoc.definition.kind
                ) {
                  replica = Result.getOrThrow(Delta.applyImmutable(replica, change.ops));
                }
              }
            },
            end: () => {},
          });
          const submission = yield* opened.root.submit({ type: "input", content: "hi" });
          yield* waitFor(
            Effect.map(storedTextOf(inner, opened.root.id), (text) => (text?.length ?? 0) > 40),
          );
          const inMemory = opened.memory.at(-1)!;
          yield* opened.crash;
          const stored = (yield* storedTextOf(inner, opened.root.id))!;
          // What storage has is a state memory had, behind it by at most the window, and what the replica has.
          assert.include(opened.memory, stored);
          assert.isTrue(inMemory.startsWith(stored));
          assert.deepStrictEqual(replica, yield* storedLive(inner, opened.root.id));

          setup.faux.setResponses([fauxAssistantMessage("answer")]);
          opened = yield* open(inner, setup);
          assert.strictEqual((yield* settled(opened.harness, submission.id)).status, "done");
          assert.deepStrictEqual(answers(yield* allEntries(opened.root)), [
            { text: stored, stopReason: "aborted" },
            { text: "answer", stopReason: "stop" },
          ]);
          assert.deepStrictEqual(
            Option.getOrUndefined(yield* opened.harness.session.snapshot(LiveDoc, opened.root.id)),
            {},
          );
          yield* opened.harness.close;
        }),
      ),
  );

  it.effect("leaves no aborted entry for a stream that crashed before its first store", () =>
    run(backend)(
      Effect.gen(function* () {
        const inner = yield* AgentStorage;
        const setup = slowSetup(Number.POSITIVE_INFINITY);
        setup.faux.setResponses([fauxAssistantMessage(LONG)]);
        let opened = yield* open(inner, setup);
        const submission = yield* opened.root.submit({ type: "input", content: "hi" });
        const memory = opened.memory;
        yield* waitFor(Effect.sync(() => (memory.at(-1)?.length ?? 0) > 40));
        yield* opened.crash;
        // The attempt is presentation, deferred like the partials: none of it was stored.
        assert.isUndefined((yield* storedLive(inner, opened.root.id))?.generation);
        assert.isUndefined(yield* storedTextOf(inner, opened.root.id));

        setup.faux.setResponses([fauxAssistantMessage("answer")]);
        opened = yield* open(inner, setup);
        assert.strictEqual((yield* settled(opened.harness, submission.id)).status, "done");
        assert.deepStrictEqual(answers(yield* allEntries(opened.root)), [
          { text: "answer", stopReason: "stop" },
        ]);
        yield* opened.harness.close;
      }),
    ),
  );

  it.effect(
    "converts a stored partial once through two crashes, before and after the conversion",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const inner = yield* AgentStorage;
          const setup = slowSetup(150);
          setup.faux.setResponses([fauxAssistantMessage(LONG)]);
          let opened = yield* open(inner, setup);
          const submission = yield* opened.root.submit({ type: "input", content: "hi" });
          yield* waitFor(
            Effect.map(storedTextOf(inner, opened.root.id), (text) => (text?.length ?? 0) > 40),
          );
          yield* opened.crash;
          const stored = (yield* storedTextOf(inner, opened.root.id))!;

          // Crash again before anything ran: nothing was converted, so nothing is lost or doubled.
          const idle = yield* killable(inner);
          const reopened = yield* openChat(idle.storage, setup);
          yield* idle.kill;
          yield* reopened.harness.close;
          assert.strictEqual(yield* storedTextOf(inner, opened.root.id), stored);

          // Crash again while the resend streams: its conversion is stored, its own partials only held.
          setup.settings.progress = { persistWithinMs: Number.POSITIVE_INFINITY };
          setup.faux.setResponses([fauxAssistantMessage(LONG)]);
          opened = yield* open(inner, setup);
          const memory = opened.memory;
          yield* waitFor(Effect.sync(() => (memory.at(-1)?.length ?? 0) > 40));
          yield* opened.crash;
          assert.isUndefined(yield* storedTextOf(inner, opened.root.id));

          setup.faux.setResponses([fauxAssistantMessage("answer")]);
          opened = yield* open(inner, setup);
          assert.strictEqual((yield* settled(opened.harness, submission.id)).status, "done");
          assert.deepStrictEqual(answers(yield* allEntries(opened.root)), [
            { text: stored, stopReason: "aborted" },
            { text: "answer", stopReason: "stop" },
          ]);
          yield* opened.harness.close;
        }),
      ),
  );

  it.effect("stores everything held at a clean close", () =>
    run(backend)(
      Effect.gen(function* () {
        const inner = yield* AgentStorage;
        const setup = slowSetup(Number.POSITIVE_INFINITY);
        setup.faux.setResponses([fauxAssistantMessage(LONG)]);
        let opened = yield* open(inner, setup);
        const submission = yield* opened.root.submit({ type: "input", content: "hi" });
        const memory = opened.memory;
        yield* waitFor(Effect.sync(() => (memory.at(-1)?.length ?? 0) > 40));
        yield* opened.harness.close;
        const atClose = memory.at(-1)!;
        assert.strictEqual(yield* storedTextOf(inner, opened.root.id), atClose);

        setup.faux.setResponses([fauxAssistantMessage("answer")]);
        opened = yield* open(inner, setup);
        yield* settled(opened.harness, submission.id);
        assert.deepStrictEqual(answers(yield* allEntries(opened.root))[0], {
          text: atClose,
          stopReason: "aborted",
        });
        yield* opened.harness.close;
      }),
    ),
  );

  it.effect("resumes polling a deferred response after a crash", () =>
    run(backend)(
      Effect.gen(function* () {
        const inner = yield* AgentStorage;
        const setup = chatSetup({ deferred: { pollAfterMs: 60_000 } });
        yield* TestClock.setTime(1_000);
        setup.faux.setResponses([fauxAssistantMessage("deferred answer")]);
        let opened = yield* open(inner, setup);
        setup.settings.stream = { deferred: true };
        const id = (yield* opened.root.submit({ type: "input", content: "hi" })).id;
        const rootId = opened.root.id;
        yield* waitFor(
          Effect.map(storedLive(inner, rootId), (live) => live?.generation?.deferred !== undefined),
        );
        yield* opened.crash;
        opened = yield* open(inner, setup);
        yield* TestClock.setTime(61_000);
        const answered = yield* settled(opened.harness, id);
        if (answered.status !== "done" || answered.type !== "input") {
          return assert.fail(`Unexpected ${answered.status}`);
        }
        const entries = yield* allEntries(opened.root);
        const answer = entries.find((entry) => entry.id === answered.answer);
        assert.strictEqual(textOf(answer?.model?.[0]), "deferred answer");
        assert.strictEqual(setup.faux.state.deferredFetchCount, 1);
        yield* opened.harness.close;
      }),
    ),
  );
});

/**
 * A tool that writes a line every 20 ms until its invocation is interrupted on its first run. A later run waits
 * to be interrupted while `stall` is set (`stalled` completes then), and otherwise writes one line and returns.
 */
const streamingTool = (replay: "safe" | "unsafe") => {
  const state = {
    runs: 0,
    lines: new Array<string>(),
    stall: false,
    stalled: Deferred.makeUnsafe<void>(),
  };
  const registration = AgentTool.make("work", {
    description: "work",
    parameters: Schema.Struct({}),
    replay,
    execute: () =>
      Effect.gen(function* () {
        const api = yield* ToolContext;
        const runNumber = ++state.runs;
        if (runNumber > 1 && state.stall) {
          yield* Deferred.succeed(state.stalled, undefined);
          return yield* Effect.never;
        }
        if (runNumber > 1) {
          yield* api.output(`rerun ${runNumber}\n`);
          return {};
        }
        for (let line = 0; ; line++) {
          yield* api.output(`line ${line}\n`);
          state.lines.push(`line ${line}\n`);
          yield* Effect.sleep("20 millis");
        }
      }).pipe(Effect.orDie),
  });
  return {
    extension: Extension.make({ name: "tool:work", tools: [registration] }),
    state,
  };
};

const storedOutput = (inner: Inner, conversationId: ConversationId) =>
  Effect.map(storedLive(inner, conversationId), (live) => live?.tools?.[0]?.output);

const results = (entries: ReadonlyArray<EntryRecord>): Array<string> =>
  entries.filter(ToolResultEntry.is).map((entry) => {
    const message = entry.model?.[0];
    if (message?.role !== "toolResult") return "";
    return message.content.map((item) => (item.type === "text" ? item.text : "")).join("|");
  });

const toolCall = () =>
  fauxAssistantMessage([fauxToolCall("work", {}, { id: "c1" })], { stopReason: "toolUse" });

describeEachBackend("tool output", (backend) => {
  // A generation's request is charged at once after a crash, so its partials never stream with a count; a
  // replay-safe tool reruns and streams its output with one.
  it.effect("never counts progress commits as progress of a task cut off again and again", () =>
    run(backend)(
      Effect.gen(function* () {
        const inner = yield* AgentStorage;
        const setup = slowSetup(50);
        let runs = 0;
        const work = Extension.make({
          name: "tool:work",
          tools: [
            AgentTool.make("work", {
              description: "work",
              parameters: Schema.Struct({}),
              replay: "safe",
              execute: () =>
                Effect.gen(function* () {
                  const api = yield* ToolContext;
                  runs++;
                  for (let line = 0; ; line++) {
                    yield* api.output(`run ${runs} line ${line}\n`);
                    yield* Effect.sleep("20 millis");
                  }
                }).pipe(Effect.orDie),
            }),
          ],
        });
        setup.faux.setResponses([toolCall()]);
        let opened = yield* open(inner, setup, [work]);
        yield* opened.root.submit({ type: "input", content: "go" });
        for (let crashes = 1; crashes <= 2; crashes++) {
          // This run stored output of its own, a progress commit, before it was cut off.
          yield* waitFor(
            Effect.map(
              storedOutput(inner, opened.root.id),
              (output) => output?.startsWith(`run ${crashes} `) === true,
            ),
          );
          yield* opened.crash;
          opened = yield* open(inner, setup, [work]);
          const tool = yield* opened.harness.harness.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              return (yield* tx.scanTasks({ kind: "core.tool" }, 1)).items[0]!;
            }),
          );
          assert.strictEqual(tool.restarts, crashes);
        }
        yield* opened.harness.close;
      }),
    ),
  );

  it.effect("answers a tool without replay that crashed with the output stored by then", () =>
    run(backend)(
      Effect.gen(function* () {
        const inner = yield* AgentStorage;
        const setup = slowSetup(150);
        const { extension, state } = streamingTool("unsafe");
        setup.faux.setResponses([toolCall()]);
        let opened = yield* open(inner, setup, [extension]);
        const submission = yield* opened.root.submit({ type: "input", content: "go" });
        yield* waitFor(
          Effect.map(storedOutput(inner, opened.root.id), (output) => (output?.length ?? 0) > 20),
        );
        yield* opened.crash;
        const stored = (yield* storedOutput(inner, opened.root.id))!;
        assert.isTrue(state.lines.join("").startsWith(stored));

        setup.faux.setResponses([fauxAssistantMessage([fauxText("done")])]);
        opened = yield* open(inner, setup, [extension]);
        assert.strictEqual((yield* settled(opened.harness, submission.id)).status, "done");
        assert.strictEqual(state.runs, 1);
        assert.deepStrictEqual(results(yield* allEntries(opened.root)), [
          `${stored}|<harness>\n[error] Tool work was interrupted and may have partially run\n</harness>`,
        ]);
        yield* opened.harness.close;
      }),
    ),
  );

  it.effect(
    "reruns a replay-safe tool after a crash with its stored progress cleared, also through a second crash",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const inner = yield* AgentStorage;
          const setup = slowSetup(150);
          const { extension, state } = streamingTool("safe");
          setup.faux.setResponses([toolCall()]);
          let opened = yield* open(inner, setup, [extension]);
          const submission = yield* opened.root.submit({ type: "input", content: "go" });
          yield* waitFor(
            Effect.map(storedOutput(inner, opened.root.id), (output) => (output?.length ?? 0) > 20),
          );
          yield* opened.crash;

          // The rerun's first commit stores the cleared progress; it crashes again before writing any of its own.
          state.stall = true;
          opened = yield* open(inner, setup, [extension]);
          yield* waitFor(Deferred.isDone(state.stalled));
          assert.isUndefined(yield* storedOutput(inner, opened.root.id));
          yield* opened.crash;
          assert.isUndefined(yield* storedOutput(inner, opened.root.id));

          state.stall = false;
          setup.faux.setResponses([fauxAssistantMessage([fauxText("done")])]);
          opened = yield* open(inner, setup, [extension]);
          assert.strictEqual((yield* settled(opened.harness, submission.id)).status, "done");
          assert.strictEqual(state.runs, 3);
          assert.deepStrictEqual(results(yield* allEntries(opened.root)), ["rerun 3\n"]);
          yield* opened.harness.close;
        }),
      ),
  );
});

describeEachBackend("compaction", (backend) => {
  it.effect("reruns a summarization cut off by a crash from its checkpoint", () =>
    run(backend)(
      Effect.gen(function* () {
        const inner = yield* AgentStorage;
        const setup = chatSetup();
        setup.settings.compaction = {
          enabled: false,
          reserveTokens: 1000,
          keepRecentTokens: 150,
          backgroundTokens: 0,
        };
        const summarizing = Deferred.makeUnsafe<void>();
        setup.faux.setResponses([
          ...["a1", "a2", "a3"].map((label) =>
            fauxAssistantMessage(`${label} ${"word ".repeat(100)}`),
          ),
          // A request that never answers until it is interrupted.
          () => Deferred.succeed(summarizing, undefined).pipe(Effect.andThen(Effect.never)),
        ]);
        let opened = yield* open(inner, setup);
        for (const label of ["u1", "u2", "u3"]) {
          const submission = yield* opened.root.submit({
            type: "input",
            content: `${label} ${"word ".repeat(100)}`,
          });
          yield* settled(opened.harness, submission.id);
        }
        const id = yield* opened.root.compact();
        yield* waitFor(Deferred.isDone(summarizing));
        yield* opened.crash;

        setup.faux.setResponses([fauxAssistantMessage("SUMMARY")]);
        opened = yield* open(inner, setup);
        const task = yield* Effect.forkChild(opened.harness.harness.awaitTask(id), {
          startImmediately: true,
        });
        yield* waitFor(Effect.sync(() => task.pollUnsafe() !== undefined));
        assert.strictEqual((yield* Fiber.join(task)).state.outcome.status, "completed");
        const root = opened.root;
        yield* waitFor(
          allEntries(root).pipe(
            Effect.map((entries) => entries.some((entry) => entry.kind === "core.compaction")),
          ),
        );
        assert.deepStrictEqual(
          Option.getOrUndefined(yield* opened.harness.session.snapshot(LiveDoc, root.id)),
          {},
        );
        yield* opened.harness.close;
      }),
    ),
  );
});

const AuditCheckpoint = Schema.Struct({ phase: Schema.Literal("run") });

describeEachBackend("reading core.live in a commit", (backend) => {
  it.effect("stores the held partial a third-party task's commit read, with what it derived", () =>
    run(backend)(
      Effect.gen(function* () {
        const inner = yield* AgentStorage;
        const setup = slowSetup(Number.POSITIVE_INFINITY);
        const Audit = Task.make({
          name: "test.audit",
          version: 1,
          input: Schema.Struct({ conversationId: Schema.Number }),
          checkpoint: AuditCheckpoint,
          result: Schema.Null,
          initial: () => ({ phase: "run" as const }),
          phases: {
            run: (task, runtime) =>
              runtime.commit(
                Effect.gen(function* () {
                  const tx = yield* Tx;
                  // SAFETY: the input holds the conversation the task was created for.
                  const conversationId = task.input.conversationId as ConversationId;
                  const live = yield* tx.doc(LiveDoc, conversationId);
                  const message = live.generation?.message;
                  const text = message === undefined ? "" : (textOf(message) ?? "");
                  yield* tx.appendEntry(conversationId, { kind: "test.audit", data: text });
                  return completed(null);
                }),
              ),
          },
          abort: () => Effect.void,
        });
        setup.faux.setResponses([fauxAssistantMessage(LONG)]);
        const audits = Extension.make({ name: "audit", tasks: [Audit] });
        let opened = yield* open(inner, setup, [audits]);
        const submission = yield* opened.root.submit({ type: "input", content: "hi" });
        const memory = opened.memory;
        yield* waitFor(Effect.sync(() => (memory.at(-1)?.length ?? 0) > 40));
        const conversationId = opened.root.id;
        const audit = yield* opened.harness.harness.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            return yield* tx.createTask(
              Audit,
              { conversationId },
              { ownership: { kind: "conversation" }, conversationId },
            );
          }),
        );
        yield* opened.harness.harness.awaitTask(audit);
        const derived = String(
          (yield* allEntries(opened.root)).find((entry) => entry.kind === "test.audit")!.data,
        );
        assert.isAbove(derived.length, 40);
        // The read stored the partial it saw, in the same storage call as the entry.
        const batch = opened.storage.commits.find((writes) =>
          writes.some((write) => write.type === "entry" && write.value.kind === "test.audit"),
        )!;
        assert.isTrue(batch.some((write) => write.type === "document.change"));
        yield* opened.crash;
        assert.strictEqual(yield* storedTextOf(inner, conversationId), derived);

        setup.faux.setResponses([fauxAssistantMessage("answer")]);
        opened = yield* open(inner, setup, [audits]);
        yield* settled(opened.harness, submission.id);
        assert.deepStrictEqual(answers(yield* allEntries(opened.root))[0], {
          text: derived,
          stopReason: "aborted",
        });
        yield* opened.harness.close;
      }),
    ),
  );
});

/** The partial text a view's `core.live` shows, if any. */
const viewPartial = (docs: Readonly<Record<string, Schema.JsonObject>>): string | undefined =>
  // SAFETY: a view's `core.live` is a `LiveState`, which its schema checked when it was written.
  storedText(docs["core.live"] as LiveState | undefined);

// A view mounted while `core.live` holds deferred changes starts from memory: it reads through the Session's document
// cache, not from storage, which is behind it.
describeEachBackend("views mounted mid-answer", (backend) => {
  it.effect(
    "mounts a watch and an observer mid-answer from memory, and follows the stream to the answer",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const inner = yield* AgentStorage;
          // The default window: storage stays behind memory for a second.
          const setup = chatSetup({ tokensPerSecond: 80, tokenSize: { min: 4, max: 4 } });
          setup.faux.setResponses([fauxAssistantMessage(LONG.slice(0, 640))]);
          const opened = yield* open(inner, setup);
          const submission = yield* opened.root.submit({ type: "input", content: "hi" });
          const memory = opened.memory;
          yield* waitFor(Effect.sync(() => memory.length >= 3));
          // Held, not stored yet: the mount must not start from storage.
          assert.isUndefined(yield* storedTextOf(inner, opened.root.id));
          const watch = yield* opened.root.watch();
          assert.strictEqual(viewPartial(watch.value.docs), memory.at(-1));
          const frames: Array<string | undefined> = [];
          yield* Effect.forkScoped(
            Stream.runForEach(watch.frames, (frame) =>
              Effect.sync(() => {
                frames.push(viewPartial(frame.value.docs));
              }),
            ),
          );
          const advanced: Array<string | undefined> = [];
          let ended = false;
          const observation = yield* opened.root.observe({
            advance: (frame) => {
              advanced.push(viewPartial(frame.value.docs));
            },
            end: () => {
              ended = true;
            },
          });
          assert.strictEqual(viewPartial(observation.value.docs), memory.at(-1));
          assert.strictEqual((yield* settled(opened.harness, submission.id)).status, "done");
          assert.isTrue(Option.isNone(opened.harness.kernel.failure));
          assert.isFalse(ended);
          // Both followed the stream past the mount and saw the answer clear it.
          assert.isAbove(advanced.filter((text) => text !== undefined).length, 0);
          assert.isUndefined(advanced.at(-1));
          assert.isUndefined(frames.at(-1));
          observation.detach();
          yield* opened.harness.close;
        }),
      ),
  );
});

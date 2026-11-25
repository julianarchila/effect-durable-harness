// Inbox tests for the Harness, on every backend: queued submissions while a run is busy (steer, follow-up,
// reject, write), queue modes, tool handoffs that continue the run in a new context, withdrawal, and recovery
// after a reopen.
//
// `openChat` opens a Harness over a `ControlledStorage`; reopening a SQLite file is a new Harness over the same
// storage, and `intercept` records or alters the storage's commits. Waits on the agent let the `TestClock`
// advance in 5 ms steps (`waitFor`), since a turn may span throttle windows and retry backoffs. Settings
// changes go through the live settings reader. "reads queue modes when the final boundary's commit runs on the
// Session line" occupies the line with a commit whose callback waits.
import { assert, it } from "@effect/vitest";
import type * as Delta from "@effect-durable-harness/delta/Delta";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as TestClock from "effect/testing/TestClock";
import * as AgentTool from "../src/AgentTool.ts";
import { ResetEntry } from "../src/Entry.ts";
import * as Extension from "../src/Extension.ts";
import type { Conversation } from "../src/Harness.ts";
import { EntryId, SubmissionId } from "../src/Ids.ts";
import * as Hook from "../src/Hook.ts";
import { InboxDoc, type InboxItem } from "../src/Inbox.ts";
import { recordUsage } from "../src/internal/usage.ts";
import { LiveDoc } from "../src/Live.ts";
import type { AssistantMessage, Usage } from "../src/Message.ts";
import type { EntryRecord, SubmissionRecord } from "../src/Records.ts";
import type { Submission } from "../src/Submission.ts";
import {
  type FauxResponseFactory,
  fauxAssistantMessage,
  fauxText,
  fauxToolCall,
} from "../src/testing/FauxModel.ts";
import { Tx } from "../src/Tx.ts";
import { UsageDoc } from "../src/Usage.ts";
import { allEntries, chatSetup, openChat, textOf, waitFor } from "./ChatSupport.ts";
import type { TestHarness } from "./HarnessSupport.ts";
import {
  controlledStorage,
  describeEachBackend,
  documentChanges,
  flush,
  run,
} from "./SessionSupport.ts";

const answer = (text: string): AssistantMessage => fauxAssistantMessage([fauxText(text)]);

/** A faux response held until `release` or interruption; `reached` completes when the request is sent. */
const gated = (message: AssistantMessage) => {
  const reached = Deferred.makeUnsafe<void>();
  const gate = Deferred.makeUnsafe<void>();
  const step: FauxResponseFactory = () =>
    Deferred.succeed(reached, undefined).pipe(
      Effect.andThen(Deferred.await(gate)),
      Effect.as(message),
    );
  return { step, reached, release: Deferred.succeed(gate, undefined) };
};

/** The JSON Schema of an empty object. */
const EMPTY_OBJECT = { type: "object", properties: {} } as const;

/** A `hold` tool whose calls wait for `gate` and then return `result`. */
const holdTool = (
  gate: Deferred.Deferred<void>,
  result: AgentTool.ToolExecutionResult = { output: [] },
) =>
  Extension.make({
    name: "hold",
    tools: [
      AgentTool.make("hold", {
        description: "Waits for the test",
        parameters: Schema.Struct({}),
        jsonSchema: EMPTY_OBJECT,
        execute: () => Deferred.await(gate).pipe(Effect.as(result)),
      }),
    ],
  });

/** An extension with generation hooks. */
const generationHooks = (handlers: Parameters<typeof Hook.generation>[0]) =>
  Extension.make({ name: "hooks", hooks: [Hook.generation(handlers)] });

/** A write that affects model context, so it queues while busy; a data-only write would be appended at once. */
const queuedNote = (text: string) =>
  ({ kind: "note", model: [{ role: "user", content: text, timestamp: 0 }] }) as const;

/** A faux response that records the request's messages as `role:text` and answers `text`. */
const recordRequest =
  (requests: Array<Array<string>>, text: string): FauxResponseFactory =>
  (request) =>
    Effect.sync(() => {
      requests.push(request.messages.map((message) => `${message.role}:${textOf(message) ?? ""}`));
      return answer(text);
    });

const HOLD = fauxAssistantMessage([fauxToolCall("hold", {}, { id: "c1" })], {
  stopReason: "toolUse",
});

/** Kind and text of each entry, skipping system entries. */
const transcript = (entries: ReadonlyArray<EntryRecord>): Array<string> =>
  entries
    .filter((entry) => entry.kind !== "core.system")
    .map((entry) => {
      const message = entry.model?.[0];
      const text = message?.role === "toolResult" ? undefined : textOf(message);
      return text === undefined ? entry.kind : `${entry.kind}:${text}`;
    });

/** Runs `effect` and returns its result once it settles, letting the `TestClock` advance meanwhile. */
const finished = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  Effect.gen(function* () {
    const fiber = yield* Effect.forkChild(effect, { startImmediately: true });
    yield* waitFor(Effect.sync(() => fiber.pollUnsafe() !== undefined));
    return yield* Fiber.join(fiber);
  });

/** Awaits a submission's settled record, letting the `TestClock` advance meanwhile. */
const settle = (submission: Submission) => finished(submission.await);

/** Waits until a deferred is done, letting the `TestClock` advance meanwhile. */
const reached = (deferred: Deferred.Deferred<void>) => waitFor(Deferred.isDone(deferred));

const status = (submission: Submission) => submission.status;

/** The fields of a submission record a case matches. */
interface Expected {
  readonly status?: SubmissionRecord["status"];
  readonly reason?: string | undefined;
  readonly entry?: EntryId | undefined;
  /** The answer entry, or `false` when the submission did not finish with one. */
  readonly answer?: EntryId | false | undefined;
}

/** Asserts that `record` has every field given in `expected`. */
const assertMatches = (record: SubmissionRecord, expected: Expected) => {
  if ("status" in expected) assert.strictEqual(record.status, expected.status, "status");
  if ("reason" in expected) {
    assert.strictEqual("reason" in record ? record.reason : undefined, expected.reason, "reason");
  }
  if ("entry" in expected) {
    assert.strictEqual("entry" in record ? record.entry : undefined, expected.entry, "entry");
  }
  if ("answer" in expected) {
    assert.strictEqual("answer" in record ? record.answer : undefined, expected.answer, "answer");
  }
};

/** Asserts `record` equals `settled` except for its own ID and any entry. */
const assertSameButEntry = (
  record: SubmissionRecord,
  settled: SubmissionRecord,
  id: SubmissionId,
) => {
  const entry = "entry" in record ? record.entry : undefined;
  assert.isNumber(entry);
  // Each admission stores its own span's trace carrier, so the trace is not compared.
  assert.deepStrictEqual<unknown>(record, { ...settled, id, entry, trace: record.trace });
};

const inbox = (harness: TestHarness, root: Conversation) =>
  harness.session
    .snapshot(InboxDoc, root.id)
    .pipe(
      Effect.map((found) =>
        Option.getOrThrow(found).items.map((item: InboxItem) => [item.id, item.mode]),
      ),
    );

const live = (harness: TestHarness, root: Conversation) =>
  harness.session.snapshot(LiveDoc, root.id).pipe(Effect.map(Option.getOrUndefined));

const toolRunning = (harness: TestHarness, root: Conversation) =>
  waitFor(live(harness, root).pipe(Effect.map((value) => value?.tools?.[0]?.status === "running")));

const idle = (harness: TestHarness) => finished(harness.harness.awaitIdle);

/** The ID of the `index`-th assistant entry. */
const assistantIds = (entries: ReadonlyArray<EntryRecord>) =>
  entries.filter((entry) => entry.kind === "core.assistant").map((entry) => entry.id);

const assistantMessages = (entries: ReadonlyArray<EntryRecord>) =>
  entries.flatMap((entry) => {
    const message = entry.model?.[0];
    return entry.kind === "core.assistant" && message?.role === "assistant" ? [message] : [];
  });

/** Records every non-empty op batch committed to `core.inbox`. */
const recordInboxOps = (harness: TestHarness) =>
  Effect.gen(function* () {
    const ops: Array<Delta.Delta> = [];
    yield* harness.session.observeCommits({
      advance: (publication) => {
        for (const change of documentChanges(publication)) {
          if (change.record.kind === "core.inbox" && change.ops.length > 0)
            ops.push([...change.ops]);
        }
      },
      end: () => {},
    });
    return ops;
  });

describeEachBackend("inbox", (backend) => {
  it.effect(
    "queues busy submissions and places writes before user items at the final boundary, one follow-up per run",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const setup = chatSetup();
          const first = gated(answer("first"));
          setup.faux.setResponses([first.step, answer("second"), answer("third")]);
          const { harness, root } = yield* openChat(yield* controlledStorage, setup);
          const input = yield* root.submit({ type: "input", content: "a" });
          yield* reached(first.reached);
          const f1 = yield* root.submit({ type: "input", content: "f1" });
          const write = yield* root.submit({ type: "write", entry: queuedNote("w") });
          const f2 = yield* root.submit({ type: "input", content: "f2", whenBusy: "followUp" });
          for (const submission of [f1, write, f2]) {
            assert.strictEqual((yield* status(submission)).status, "queued");
          }
          assert.deepStrictEqual(yield* inbox(harness, root), [
            [f1.id, "followUp"],
            [write.id, "write"],
            [f2.id, "followUp"],
          ]);
          assert.deepStrictEqual(transcript(yield* allEntries(root)), ["core.user:a"]);

          yield* first.release;
          yield* settle(f2);
          const entries = yield* allEntries(root);
          assert.deepStrictEqual(transcript(entries), [
            "core.user:a",
            "core.assistant:first",
            "note:w",
            "core.user:f1",
            "core.assistant:second",
            "core.user:f2",
            "core.assistant:third",
          ]);
          const answers = assistantIds(entries);
          assertMatches(yield* status(input), { status: "done", answer: answers[0] });
          assertMatches(yield* status(write), { status: "done" });
          assertMatches(yield* status(f1), { status: "done", answer: answers[1] });
          assertMatches(yield* status(f2), { status: "done", answer: answers[2] });
          assert.deepStrictEqual(yield* inbox(harness, root), []);
          assert.deepStrictEqual(yield* live(harness, root), {});
          yield* harness.close;
        }),
      ),
  );

  it.effect("places every follow-up in one successor run with followUpMode all", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        const first = gated(answer("first"));
        setup.faux.setResponses([first.step, answer("both")]);
        const { harness, root } = yield* openChat(yield* controlledStorage, setup);
        setup.settings.followUpMode = "all";
        yield* root.submit({ type: "input", content: "a" });
        yield* reached(first.reached);
        const f1 = yield* root.submit({ type: "input", content: "f1" });
        const f2 = yield* root.submit({ type: "input", content: "f2" });
        yield* first.release;
        const settled = yield* settle(f2);
        const record = yield* status(f1);
        assertSameButEntry(record, settled, f1.id);
        assert.deepStrictEqual(transcript(yield* allEntries(root)), [
          "core.user:a",
          "core.assistant:first",
          "core.user:f1",
          "core.user:f2",
          "core.assistant:both",
        ]);
        assert.strictEqual(setup.faux.state.callCount, 2);
        yield* harness.close;
      }),
    ),
  );

  it.effect("reads queue modes when the final boundary's commit runs on the Session line", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        const first = gated(answer("first"));
        setup.faux.setResponses([first.step, answer("both")]);
        const yielded = Deferred.makeUnsafe<void>();
        const hooks = generationHooks({
          onYield: () => Deferred.succeed(yielded, undefined).pipe(Effect.as(undefined)),
        });
        const storage = yield* controlledStorage;
        const { harness, root } = yield* openChat(storage, setup, { extensions: [hooks] });
        yield* root.submit({ type: "input", content: "a" });
        yield* reached(first.reached);
        const f1 = yield* root.submit({ type: "input", content: "f1" });
        const f2 = yield* root.submit({ type: "input", content: "f2" });
        // Occupy the line with a commit whose callback waits, let the answer queue its boundary commit behind it,
        // then change the mode.
        const entered = Deferred.makeUnsafe<void>();
        const release = Deferred.makeUnsafe<void>();
        const occupying = yield* Effect.forkChild(
          root.commit(
            Deferred.succeed(entered, undefined).pipe(Effect.andThen(Deferred.await(release))),
          ),
          { startImmediately: true },
        );
        yield* finished(Deferred.await(entered));
        yield* first.release;
        yield* reached(yielded);
        yield* flush;
        setup.settings.followUpMode = "all";
        yield* Deferred.succeed(release, undefined);
        yield* finished(Fiber.join(occupying));
        const settled = yield* settle(f2);
        const record = yield* status(f1);
        assertSameButEntry(record, settled, f1.id);
        assert.strictEqual(setup.faux.state.callCount, 2);
        yield* harness.close;
      }),
    ),
  );

  it.effect(
    "adds steers to the run at the postTools boundary and holds follow-ups for the final boundary",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const setup = chatSetup();
          const gate = Deferred.makeUnsafe<void>();
          setup.faux.setResponses([HOLD, answer("after tools"), answer("follow-up")]);
          const { harness, root } = yield* openChat(yield* controlledStorage, setup, {
            extensions: [holdTool(gate)],
          });
          const input = yield* root.submit({ type: "input", content: "a" });
          yield* toolRunning(harness, root);
          const steer = yield* root.submit({ type: "input", content: "s", whenBusy: "steer" });
          const followUp = yield* root.submit({ type: "input", content: "f" });
          yield* Deferred.succeed(gate, undefined);
          yield* settle(followUp);
          const entries = yield* allEntries(root);
          assert.deepStrictEqual(transcript(entries), [
            "core.user:a",
            "core.assistant",
            "core.tool-result",
            "core.user:s",
            "core.assistant:after tools",
            "core.user:f",
            "core.assistant:follow-up",
          ]);
          const answers = assistantIds(entries);
          assertMatches(yield* status(input), { status: "done", answer: answers[1] });
          assertMatches(yield* status(steer), { status: "done", answer: answers[1] });
          assertMatches(yield* status(followUp), { status: "done", answer: answers[2] });
          yield* harness.close;
        }),
      ),
  );

  it.effect(
    "ends the run at a queued reset after tools and runs earlier follow-ups in the new context",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const setup = chatSetup();
          const gate = Deferred.makeUnsafe<void>();
          const requests: Array<Array<string>> = [];
          setup.faux.setResponses([HOLD, recordRequest(requests, "fresh")]);
          const { harness, root } = yield* openChat(yield* controlledStorage, setup, {
            extensions: [holdTool(gate)],
          });
          const input = yield* root.submit({ type: "input", content: "a" });
          yield* toolRunning(harness, root);
          const followUp = yield* root.submit({ type: "input", content: "f" });
          yield* root.reset(undefined);
          yield* Deferred.succeed(gate, undefined);
          yield* settle(followUp);
          assertMatches(yield* status(input), { status: "unanswered", reason: "reset" });
          const entries = yield* allEntries(root);
          assert.deepStrictEqual(transcript(entries), [
            "core.user:a",
            "core.assistant",
            "core.tool-result",
            "core.reset",
            "core.user:f",
            "core.assistant:fresh",
          ]);
          const reset = entries.find((entry) => ResetEntry.is(entry));
          assert.strictEqual(reset?.head, reset?.id);
          // The follow-up's request starts at the reset: the complete system baseline after the cut leads the
          // follow-up.
          assert.deepStrictEqual(requests, [["system:", "user:f"]]);
          assert.strictEqual(setup.faux.state.callCount, 2);
          yield* harness.close;
        }),
      ),
  );

  it.effect("places a queued reset after the answer at the final boundary", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        const first = gated(answer("first"));
        setup.faux.setResponses([first.step]);
        const { harness, root } = yield* openChat(yield* controlledStorage, setup);
        const input = yield* root.submit({ type: "input", content: "a" });
        yield* reached(first.reached);
        yield* root.reset("handoff");
        yield* first.release;
        yield* settle(input);
        yield* idle(harness);
        assertMatches(yield* status(input), { status: "done" });
        assert.deepStrictEqual(transcript(yield* allEntries(root)), [
          "core.user:a",
          "core.assistant:first",
          "core.reset:handoff",
        ]);
        const messages = (yield* root.context()).messages;
        assert.strictEqual(messages.length, 1);
        const message = messages[0];
        assert.strictEqual(message?.role, "user");
        if (message?.role !== "user") return;
        assert.strictEqual(message.content, "handoff");
        assert.isNumber(message.timestamp);
        yield* harness.close;
      }),
    ),
  );

  it.effect("resets an idle conversation at once, with or without handoff text", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        yield* TestClock.setTime(7);
        const { harness, root } = yield* openChat(yield* controlledStorage, setup);
        yield* root.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            yield* tx.appendEntry(root.id, { kind: "note" });
          }),
        );
        yield* root.reset(undefined);
        let view = yield* root.context();
        assert.strictEqual(view.head?.kind, "core.reset");
        assert.isUndefined(view.head?.model);
        assert.deepStrictEqual(view.messages, []);
        yield* root.reset("carry on");
        view = yield* root.context();
        assert.strictEqual(view.head?.head, view.head?.id);
        assert.deepStrictEqual(view.messages, [
          { role: "user", content: "carry on", timestamp: 7 },
        ]);
        yield* harness.close;
      }),
    ),
  );

  it.effect(
    "makes a queued head write stale when it targets an entry before the active range",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const setup = chatSetup();
          const first = gated(answer("first"));
          setup.faux.setResponses([first.step, answer("second")]);
          const { harness, root } = yield* openChat(yield* controlledStorage, setup);
          const old = yield* root.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              return yield* tx.appendEntry(root.id, { kind: "note" });
            }),
          );
          yield* root.reset(undefined);
          const reset = (yield* root.context()).head;
          if (reset === undefined) return assert.fail("no reset");
          const input = yield* root.submit({ type: "input", content: "a" });
          yield* reached(first.reached);
          const stale = yield* root.submit({
            type: "write",
            entry: { kind: "summary", head: old.id },
          });
          const fresh = yield* root.submit({
            type: "write",
            entry: { kind: "summary", head: reset.id },
          });
          yield* first.release;
          yield* settle(input);
          assertMatches(yield* status(stale), { status: "unanswered", reason: "stale" });
          assertMatches(yield* status(fresh), { status: "done" });
          assert.deepStrictEqual(yield* inbox(harness, root), []);

          // The fresh summary's marker starts the range at the reset: a target inside the range is not stale, even
          // when it is older than the marker itself. A reset placed earlier in the same boundary makes it stale.
          const inside = (yield* allEntries(root)).find((entry) => entry.kind === "core.user");
          if (inside === undefined) return assert.fail("no input entry");
          const second = yield* root.submit({ type: "input", content: "b" });
          const kept = yield* root.submit({
            type: "write",
            entry: { kind: "summary", head: inside.id },
          });
          yield* settle(second);
          assertMatches(yield* status(kept), { status: "done" });
          yield* harness.close;
        }),
      ),
  );

  it.effect("makes a head write stale behind a reset placed earlier in the same boundary", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        const first = gated(answer("first"));
        setup.faux.setResponses([first.step]);
        const { harness, root } = yield* openChat(yield* controlledStorage, setup);
        const input = yield* root.submit({ type: "input", content: "a" });
        yield* reached(first.reached);
        const target = (yield* allEntries(root))[0];
        if (target === undefined) return assert.fail("no entry");
        yield* root.reset(undefined);
        const summary = yield* root.submit({
          type: "write",
          entry: { kind: "summary", head: target.id },
        });
        yield* first.release;
        yield* settle(input);
        assertMatches(yield* status(summary), { status: "unanswered", reason: "stale" });
        yield* harness.close;
      }),
    ),
  );

  // The reset's entry ID is minted when it is appended; a later head is compared with it, so one above it is placed.
  it.effect("places a head write above a reset placed earlier in the same boundary", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        const first = gated(answer("first"));
        setup.faux.setResponses([first.step]);
        const { harness, root } = yield* openChat(yield* controlledStorage, setup);
        const input = yield* root.submit({ type: "input", content: "a" });
        yield* reached(first.reached);
        yield* root.reset(undefined);
        const above = EntryId.make(1_000_000);
        const summary = yield* root.submit({
          type: "write",
          entry: { kind: "summary", head: above },
        });
        yield* first.release;
        yield* settle(input);
        assertMatches(yield* status(summary), { status: "done" });
        const entries = yield* allEntries(root);
        const reset = entries.find((entry) => entry.kind === "core.reset");
        const placed = entries.find((entry) => entry.kind === "summary");
        if (reset === undefined || placed === undefined) return assert.fail("not placed");
        assert.isBelow(reset.id, above);
        assert.isAbove(placed.id, reset.id);
        assert.strictEqual(placed.head, above);
        yield* harness.close;
      }),
    ),
  );

  it.effect("continues the run in a new context when a tool requests a handoff", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        const gate = Deferred.makeUnsafe<void>();
        yield* Deferred.succeed(gate, undefined);
        const requests: Array<Array<string>> = [];
        setup.faux.setResponses([HOLD, recordRequest(requests, "after handoff")]);
        const { harness, root } = yield* openChat(yield* controlledStorage, setup, {
          extensions: [holdTool(gate, { output: [], control: { handoff: "continue here" } })],
        });
        const settled = yield* settle(yield* root.submit({ type: "input", content: "a" }));
        const entries = yield* allEntries(root);
        assertMatches(settled, { status: "done", answer: entries.at(-1)?.id });
        assert.deepStrictEqual(transcript(entries), [
          "core.user:a",
          "core.assistant",
          "core.tool-result",
          "core.reset:continue here",
          "core.assistant:after handoff",
        ]);
        const reset = entries.find((entry) => ResetEntry.is(entry));
        assert.strictEqual(reset?.head, reset?.id);
        assert.deepStrictEqual(requests, [["system:", "user:continue here"]]);
        assert.strictEqual(setup.faux.state.callCount, 2);
        assert.deepStrictEqual(yield* live(harness, root), {});
        yield* harness.close;
      }),
    ),
  );

  it.effect(
    "places queued writes and steers after the handoff text and adds the steers to the continuing run",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const setup = chatSetup();
          const gate = Deferred.makeUnsafe<void>();
          const requests: Array<Array<string>> = [];
          setup.faux.setResponses([HOLD, recordRequest(requests, "after handoff")]);
          const { harness, root } = yield* openChat(yield* controlledStorage, setup, {
            extensions: [holdTool(gate, { output: [], control: { handoff: "continue here" } })],
          });
          const input = yield* root.submit({ type: "input", content: "a" });
          yield* toolRunning(harness, root);
          const steer = yield* root.submit({ type: "input", content: "s", whenBusy: "steer" });
          const write = yield* root.submit({ type: "write", entry: queuedNote("w") });
          yield* Deferred.succeed(gate, undefined);
          yield* settle(input);
          const entries = yield* allEntries(root);
          assert.deepStrictEqual(transcript(entries), [
            "core.user:a",
            "core.assistant",
            "core.tool-result",
            "core.reset:continue here",
            "note:w",
            "core.user:s",
            "core.assistant:after handoff",
          ]);
          assert.deepStrictEqual(requests, [["system:", "user:continue here", "user:w", "user:s"]]);
          for (const submission of [input, steer]) {
            assertMatches(yield* status(submission), {
              status: "done",
              answer: entries.at(-1)?.id,
            });
          }
          assertMatches(yield* status(write), { status: "done" });
          yield* harness.close;
        }),
      ),
  );

  it.effect(
    "holds a follow-up queued across a handoff until the continuing run's final boundary",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const setup = chatSetup();
          const gate = Deferred.makeUnsafe<void>();
          const after = gated(answer("after handoff"));
          setup.faux.setResponses([HOLD, after.step, answer("follow-up")]);
          const { harness, root } = yield* openChat(yield* controlledStorage, setup, {
            extensions: [holdTool(gate, { output: [], control: { handoff: "continue here" } })],
          });
          const input = yield* root.submit({ type: "input", content: "a" });
          yield* toolRunning(harness, root);
          const followUp = yield* root.submit({ type: "input", content: "f" });
          yield* Deferred.succeed(gate, undefined);
          yield* reached(after.reached);
          assert.strictEqual((yield* status(input)).status, "placed");
          assert.deepStrictEqual(yield* inbox(harness, root), [[followUp.id, "followUp"]]);
          yield* after.release;
          yield* settle(followUp);
          const entries = yield* allEntries(root);
          assert.deepStrictEqual(transcript(entries), [
            "core.user:a",
            "core.assistant",
            "core.tool-result",
            "core.reset:continue here",
            "core.assistant:after handoff",
            "core.user:f",
            "core.assistant:follow-up",
          ]);
          const answers = assistantIds(entries);
          assertMatches(yield* status(input), { status: "done", answer: answers[1] });
          assertMatches(yield* status(followUp), { status: "done", answer: answers[2] });
          yield* harness.close;
        }),
      ),
  );

  it.effect("ends a handed-off run at a queued reset and runs earlier follow-ups after it", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        const gate = Deferred.makeUnsafe<void>();
        const requests: Array<Array<string>> = [];
        setup.faux.setResponses([HOLD, recordRequest(requests, "fresh")]);
        const { harness, root } = yield* openChat(yield* controlledStorage, setup, {
          extensions: [holdTool(gate, { output: [], control: { handoff: "continue here" } })],
        });
        const input = yield* root.submit({ type: "input", content: "a" });
        yield* toolRunning(harness, root);
        const followUp = yield* root.submit({ type: "input", content: "f" });
        yield* root.reset(undefined);
        yield* Deferred.succeed(gate, undefined);
        yield* settle(followUp);
        assertMatches(yield* status(input), { status: "unanswered", reason: "reset" });
        assert.deepStrictEqual(transcript(yield* allEntries(root)), [
          "core.user:a",
          "core.assistant",
          "core.tool-result",
          "core.reset:continue here",
          "core.reset",
          "core.user:f",
          "core.assistant:fresh",
        ]);
        assert.deepStrictEqual(requests, [["system:", "user:f"]]);
        assert.strictEqual(setup.faux.state.callCount, 2);
        yield* harness.close;
      }),
    ),
  );

  it.effect("hands off again in a later round of the same run", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        let handoffs = 0;
        const handingOff = Extension.make({
          name: "hold",
          tools: [
            AgentTool.make("hold", {
              description: "Hands off",
              parameters: Schema.Struct({}),
              jsonSchema: EMPTY_OBJECT,
              execute: () =>
                Effect.sync(() => ({ output: [], control: { handoff: `h${++handoffs}` } })),
            }),
          ],
        });
        setup.faux.setResponses([HOLD, HOLD, answer("done")]);
        const { harness, root } = yield* openChat(yield* controlledStorage, setup, {
          extensions: [handingOff],
        });
        const settled = yield* settle(yield* root.submit({ type: "input", content: "a" }));
        const entries = yield* allEntries(root);
        assert.deepStrictEqual(transcript(entries), [
          "core.user:a",
          "core.assistant",
          "core.tool-result",
          "core.reset:h1",
          "core.assistant",
          "core.tool-result",
          "core.reset:h2",
          "core.assistant:done",
        ]);
        assertMatches(settled, { status: "done", answer: entries.at(-1)?.id });
        const messages = (yield* root.context()).messages;
        assert.deepStrictEqual(
          messages.map((message) => `${message.role}:${textOf(message) ?? ""}`),
          ["system:", "user:h2", "assistant:done"],
        );
        yield* harness.close;
      }),
    ),
  );

  it.effect("ends the run after the handoff when the round also terminates", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        const gate = Deferred.makeUnsafe<void>();
        yield* Deferred.succeed(gate, undefined);
        setup.faux.setResponses([HOLD]);
        const { harness, root } = yield* openChat(yield* controlledStorage, setup, {
          extensions: [
            holdTool(gate, { output: [], control: { handoff: "continue here", terminate: true } }),
          ],
        });
        const settled = yield* settle(yield* root.submit({ type: "input", content: "a" }));
        const entries = yield* allEntries(root);
        const calling = entries.find((entry) => entry.kind === "core.assistant");
        assertMatches(settled, { status: "done", answer: calling?.id });
        assert.deepStrictEqual(transcript(entries), [
          "core.user:a",
          "core.assistant",
          "core.tool-result",
          "core.reset:continue here",
        ]);
        assert.strictEqual(setup.faux.state.callCount, 1);
        assert.deepStrictEqual(yield* live(harness, root), {});
        yield* harness.close;
      }),
    ),
  );

  it.effect("drops an onYield continuation when the final boundary selects a follow-up", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        const first = gated(answer("first"));
        setup.faux.setResponses([first.step, answer("second")]);
        let yields = 0;
        const hooks = generationHooks({
          onYield: () => Effect.sync(() => (yields++ === 0 ? { continue: "more" } : undefined)),
        });
        const { harness, root } = yield* openChat(yield* controlledStorage, setup, {
          extensions: [hooks],
        });
        const input = yield* root.submit({ type: "input", content: "a" });
        yield* reached(first.reached);
        const followUp = yield* root.submit({ type: "input", content: "f" });
        yield* first.release;
        yield* settle(followUp);
        assertMatches(yield* status(input), { status: "done" });
        assert.deepStrictEqual(transcript(yield* allEntries(root)), [
          "core.user:a",
          "core.assistant:first",
          "core.user:f",
          "core.assistant:second",
        ]);
        yield* harness.close;
      }),
    ),
  );

  it.effect("withdraws a queued submission and removes its item", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        const first = gated(answer("first"));
        setup.faux.setResponses([first.step]);
        const { harness, root } = yield* openChat(yield* controlledStorage, setup);
        const input = yield* root.submit({ type: "input", content: "a" });
        yield* reached(first.reached);
        const kept = yield* root.submit({ type: "write", entry: queuedNote("kept") });
        const withdrawn = yield* root.submit({ type: "input", content: "f" });
        assert.deepStrictEqual(yield* withdrawn.withdraw, { _tag: "Withdrawn" });
        assertMatches(yield* settle(withdrawn), { status: "unanswered", reason: "aborted" });
        assert.deepStrictEqual(yield* inbox(harness, root), [[kept.id, "write"]]);
        yield* first.release;
        yield* settle(input);
        assertMatches(yield* status(kept), { status: "done" });
        assert.strictEqual(setup.faux.state.callCount, 1);
        yield* harness.close;
      }),
    ),
  );

  it.effect(
    "leaves queued items after a failed run until the next submission places them in order",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const setup = chatSetup();
          const failing = gated(
            fauxAssistantMessage([], { stopReason: "error", errorMessage: "invalid request" }),
          );
          setup.faux.setResponses([failing.step, answer("for f"), answer("for g")]);
          const { harness, root } = yield* openChat(yield* controlledStorage, setup);
          const input = yield* root.submit({ type: "input", content: "a" });
          yield* reached(failing.reached);
          const f = yield* root.submit({ type: "input", content: "f" });
          yield* failing.release;
          assertMatches(yield* settle(input), { status: "unanswered", reason: "model_error" });
          yield* idle(harness);
          assert.strictEqual((yield* status(f)).status, "queued");
          assert.deepStrictEqual(yield* inbox(harness, root), [[f.id, "followUp"]]);

          // Idle with a queued item: the new input queues behind it, and a final boundary places the older one
          // first.
          const g = yield* root.submit({ type: "input", content: "g", whenBusy: "reject" });
          yield* settle(g);
          assertMatches(yield* status(f), { status: "done" });
          assert.deepStrictEqual(transcript(yield* allEntries(root)).slice(-4), [
            "core.user:f",
            "core.assistant:for f",
            "core.user:g",
            "core.assistant:for g",
          ]);
          yield* harness.close;
        }),
      ),
  );

  it.effect("keeps queued submissions across reopen and settles them afterwards", () =>
    run(backend)(
      Effect.gen(function* () {
        // Reopening is a new Harness over the same storage.
        const storage = yield* controlledStorage;
        const setup = chatSetup();
        const first = gated(answer("first"));
        setup.faux.setResponses([first.step, answer("first again"), answer("f")]);
        let opened = yield* openChat(storage, setup);
        yield* opened.root.submit({ type: "input", content: "a" });
        yield* reached(first.reached);
        const f = (yield* opened.root.submit({ type: "input", content: "f" })).id;
        yield* opened.harness.close;

        opened = yield* openChat(storage, setup);
        const submission = Option.getOrThrow(yield* opened.harness.harness.submission(f));
        assertMatches(yield* settle(submission), { status: "done" });
        assert.deepStrictEqual(transcript(yield* allEntries(opened.root)).slice(-2), [
          "core.user:f",
          "core.assistant:f",
        ]);
        yield* opened.harness.close;
      }),
    ),
  );

  it.effect("commits inbox changes as positional delta operations and a base when empty", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        const first = gated(answer("first"));
        setup.faux.setResponses([first.step, answer("second")]);
        const { harness, root } = yield* openChat(yield* controlledStorage, setup);
        const ops = yield* recordInboxOps(harness);
        const input = yield* root.submit({ type: "input", content: "a" });
        yield* reached(first.reached);
        const w1 = yield* root.submit({ type: "write", entry: queuedNote("w") });
        const f1 = yield* root.submit({ type: "input", content: "f1" });
        const w2 = yield* root.submit({ type: "write", entry: queuedNote("w") });
        const f2 = yield* root.submit({ type: "input", content: "f2" });
        const w3 = yield* root.submit({ type: "write", entry: queuedNote("w") });
        assert.deepStrictEqual(ops, [
          [["p", ["items"], 0, 0, [{ id: w1.id, mode: "write", entry: queuedNote("w") }]]],
          [["p", ["items"], 1, 0, [{ id: f1.id, mode: "followUp", content: "f1" }]]],
          [["p", ["items"], 2, 0, [{ id: w2.id, mode: "write", entry: queuedNote("w") }]]],
          [["p", ["items"], 3, 0, [{ id: f2.id, mode: "followUp", content: "f2" }]]],
          [["p", ["items"], 4, 0, [{ id: w3.id, mode: "write", entry: queuedNote("w") }]]],
        ]);
        yield* first.release;
        yield* settle(input);
        // Every write and the first follow-up leave; only f2 at index 3 remains. No retained value is carried.
        const boundary = ops[5] ?? [];
        assert.isTrue(
          boundary.every((op) => op[0] === "p" && Array.isArray(op[4]) && op[4].length === 0),
        );
        assert.deepInclude(boundary, ["p", ["items"], 4, 1, []]);
        assert.notInclude(Schema.encodeSync(Schema.fromJsonString(Schema.Unknown))(boundary), "f2");
        yield* settle(f2);
        yield* harness.close;
      }),
    ),
  );

  it.effect("settles a stale write at once while idle, and a queued one behind waiting items", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        const failing = gated(
          fauxAssistantMessage([], { stopReason: "error", errorMessage: "invalid request" }),
        );
        setup.faux.setResponses([failing.step, answer("for f")]);
        const { harness, root } = yield* openChat(yield* controlledStorage, setup);
        const old = yield* root.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            return yield* tx.appendEntry(root.id, { kind: "note" });
          }),
        );
        yield* root.reset(undefined);
        const before = (yield* allEntries(root)).length;
        const idleWrite = yield* root.submit({
          type: "write",
          entry: { kind: "summary", head: old.id },
        });
        assertMatches(yield* settle(idleWrite), { status: "unanswered", reason: "stale" });
        assert.lengthOf(yield* allEntries(root), before);

        // After a failed run, a follow-up waits; a stale write queues behind it and the boundary rejects it.
        const input = yield* root.submit({ type: "input", content: "a" });
        yield* reached(failing.reached);
        const f = yield* root.submit({ type: "input", content: "f" });
        yield* failing.release;
        yield* settle(input);
        yield* idle(harness);
        const queued = yield* root.submit({
          type: "write",
          entry: { kind: "summary", head: old.id },
        });
        assertMatches(yield* settle(queued), { status: "unanswered", reason: "stale" });
        assertMatches(yield* settle(f), { status: "done" });
        yield* harness.close;
      }),
    ),
  );

  it.effect(
    "keeps an onYield continuation across a queued plain write, with the run's original input",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const setup = chatSetup();
          const first = gated(answer("first"));
          setup.faux.setResponses([first.step, answer("second")]);
          let yields = 0;
          const hooks = generationHooks({
            onYield: () => Effect.sync(() => (yields++ === 0 ? { continue: "more" } : undefined)),
          });
          const { harness, root } = yield* openChat(yield* controlledStorage, setup, {
            extensions: [hooks],
          });
          const input = yield* root.submit({ type: "input", content: "a" });
          yield* reached(first.reached);
          const write = yield* root.submit({ type: "write", entry: queuedNote("w") });
          yield* first.release;
          yield* settle(input);
          const entries = yield* allEntries(root);
          assert.deepStrictEqual(transcript(entries), [
            "core.user:a",
            "core.assistant:first",
            "note:w",
            "core.user:more",
            "core.assistant:second",
          ]);
          assertMatches(yield* status(input), { status: "done", answer: entries.at(-1)?.id });
          assertMatches(yield* status(write), { status: "done" });
          yield* harness.close;
        }),
      ),
  );

  it.effect("drops an onYield continuation for a queued reset", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        const first = gated(answer("first"));
        setup.faux.setResponses([first.step]);
        const hooks = generationHooks({
          onYield: () => Effect.succeed({ continue: "more" }),
        });
        const { harness, root } = yield* openChat(yield* controlledStorage, setup, {
          extensions: [hooks],
        });
        const input = yield* root.submit({ type: "input", content: "a" });
        yield* reached(first.reached);
        yield* root.reset(undefined);
        yield* first.release;
        yield* settle(input);
        yield* idle(harness);
        assertMatches(yield* status(input), { status: "done" });
        assert.deepStrictEqual(transcript(yield* allEntries(root)), [
          "core.user:a",
          "core.assistant:first",
          "core.reset",
        ]);
        assert.strictEqual(setup.faux.state.callCount, 1);
        yield* harness.close;
      }),
    ),
  );

  it.effect("adds every steer to the run at the postTools boundary with steeringMode all", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        const gate = Deferred.makeUnsafe<void>();
        setup.faux.setResponses([HOLD, answer("after tools"), answer("follow-up")]);
        const { harness, root } = yield* openChat(yield* controlledStorage, setup, {
          extensions: [holdTool(gate)],
        });
        setup.settings.steeringMode = "all";
        const input = yield* root.submit({ type: "input", content: "a" });
        yield* toolRunning(harness, root);
        const s1 = yield* root.submit({ type: "input", content: "s1", whenBusy: "steer" });
        const f = yield* root.submit({ type: "input", content: "f" });
        const s2 = yield* root.submit({ type: "input", content: "s2", whenBusy: "steer" });
        yield* Deferred.succeed(gate, undefined);
        yield* settle(f);
        const entries = yield* allEntries(root);
        assert.deepStrictEqual(transcript(entries), [
          "core.user:a",
          "core.assistant",
          "core.tool-result",
          "core.user:s1",
          "core.user:s2",
          "core.assistant:after tools",
          "core.user:f",
          "core.assistant:follow-up",
        ]);
        const answers = assistantIds(entries);
        for (const submission of [input, s1, s2]) {
          assertMatches(yield* status(submission), { status: "done", answer: answers[1] });
        }
        yield* harness.close;
      }),
    ),
  );

  it.effect(
    "queues an idle steer behind waiting items and places it with the first follow-up in ID order",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const setup = chatSetup();
          const failing = gated(
            fauxAssistantMessage([], { stopReason: "error", errorMessage: "invalid request" }),
          );
          setup.faux.setResponses([failing.step, answer("both")]);
          const { harness, root } = yield* openChat(yield* controlledStorage, setup);
          const input = yield* root.submit({ type: "input", content: "a" });
          yield* reached(failing.reached);
          const f = yield* root.submit({ type: "input", content: "f" });
          yield* failing.release;
          yield* settle(input);
          yield* idle(harness);
          const steer = yield* root.submit({ type: "input", content: "s", whenBusy: "steer" });
          const settled = yield* settle(steer);
          assertMatches(yield* status(f), {
            status: "done",
            answer: settled.status === "done" && "answer" in settled && settled.answer,
          });
          assert.deepStrictEqual(transcript(yield* allEntries(root)).slice(-3), [
            "core.user:f",
            "core.user:s",
            "core.assistant:both",
          ]);
          yield* harness.close;
        }),
      ),
  );

  it.effect("starts a queued follow-up after a terminating round", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        const gate = Deferred.makeUnsafe<void>();
        setup.faux.setResponses([HOLD, answer("follow-up")]);
        const { harness, root } = yield* openChat(yield* controlledStorage, setup, {
          extensions: [holdTool(gate, { output: [], control: { terminate: true } })],
        });
        const input = yield* root.submit({ type: "input", content: "a" });
        yield* toolRunning(harness, root);
        const f = yield* root.submit({ type: "input", content: "f" });
        yield* Deferred.succeed(gate, undefined);
        yield* settle(f);
        const entries = yield* allEntries(root);
        const calling = entries.find((entry) => entry.kind === "core.assistant");
        assertMatches(yield* status(input), { status: "done", answer: calling?.id });
        assert.deepStrictEqual(transcript(entries), [
          "core.user:a",
          "core.assistant",
          "core.tool-result",
          "core.user:f",
          "core.assistant:follow-up",
        ]);
        yield* harness.close;
      }),
    ),
  );

  it.effect(
    "writes only the last handoff in call order and runs queued follow-ups after the continuing run",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const setup = chatSetup();
          const firstGate = Deferred.makeUnsafe<void>();
          const secondGate = Deferred.makeUnsafe<void>();
          const later = Extension.make({
            name: "later",
            tools: [
              AgentTool.make("later", {
                description: "Finishes first",
                parameters: Schema.Struct({}),
                jsonSchema: EMPTY_OBJECT,
                execute: () =>
                  Deferred.await(secondGate).pipe(
                    Effect.as({ output: [], control: { handoff: "two" } }),
                  ),
              }),
            ],
          });
          const round = fauxAssistantMessage(
            [fauxToolCall("hold", {}, { id: "c1" }), fauxToolCall("later", {}, { id: "c2" })],
            { stopReason: "toolUse" },
          );
          setup.faux.setResponses([round, answer("after handoff"), answer("follow-up")]);
          const { harness, root } = yield* openChat(yield* controlledStorage, setup, {
            extensions: [holdTool(firstGate, { output: [], control: { handoff: "one" } }), later],
          });
          const input = yield* root.submit({ type: "input", content: "a" });
          yield* toolRunning(harness, root);
          const f = yield* root.submit({ type: "input", content: "f" });
          yield* Deferred.succeed(secondGate, undefined);
          yield* waitFor(
            live(harness, root).pipe(Effect.map((value) => value?.tools?.[1]?.status === "done")),
          );
          yield* Deferred.succeed(firstGate, undefined);
          yield* settle(f);
          const entries = yield* allEntries(root);
          assert.deepStrictEqual(transcript(entries), [
            "core.user:a",
            "core.assistant",
            "core.tool-result",
            "core.tool-result",
            "core.reset:two",
            "core.assistant:after handoff",
            "core.user:f",
            "core.assistant:follow-up",
          ]);
          const answers = assistantIds(entries);
          assertMatches(yield* status(input), { status: "done", answer: answers[1] });
          yield* harness.close;
        }),
      ),
  );

  it.effect("leaves the inbox alone when the run's task is aborted", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        const first = gated(answer("never"));
        setup.faux.setResponses([first.step]);
        const { harness, root } = yield* openChat(yield* controlledStorage, setup);
        const input = yield* root.submit({ type: "input", content: "a" });
        yield* reached(first.reached);
        const f = yield* root.submit({ type: "input", content: "f" });
        const taskId = (yield* live(harness, root))?.run?.taskId;
        if (taskId === undefined) return assert.fail("no run");
        yield* finished(harness.harness.abortTask(taskId));
        assertMatches(yield* settle(input), { status: "unanswered", reason: "aborted" });
        yield* idle(harness);
        assert.strictEqual((yield* status(f)).status, "queued");
        assert.deepStrictEqual(yield* inbox(harness, root), [[f.id, "followUp"]]);
        yield* harness.close;
      }),
    ),
  );

  it.effect("returns a queued submission for its repeated request ID without a second item", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        const first = gated(answer("first"));
        setup.faux.setResponses([first.step]);
        const { harness, root } = yield* openChat(yield* controlledStorage, setup);
        yield* root.submit({ type: "input", content: "a" });
        yield* reached(first.reached);
        const queued = yield* root.submit({ type: "input", content: "f", requestId: "r" });
        const again = yield* root.submit({ type: "input", content: "f", requestId: "r" });
        assert.strictEqual(again.id, queued.id);
        assert.deepStrictEqual(yield* inbox(harness, root), [[queued.id, "followUp"]]);
        yield* harness.close;
      }),
    ),
  );

  it.effect("withdraws a middle item with one positional removal", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        const first = gated(answer("first"));
        setup.faux.setResponses([first.step]);
        const { harness, root } = yield* openChat(yield* controlledStorage, setup);
        yield* root.submit({ type: "input", content: "a" });
        yield* reached(first.reached);
        const items: Array<Submission> = [];
        for (const text of ["x", "y", "z"]) {
          items.push(yield* root.submit({ type: "input", content: text }));
        }
        const ops = yield* recordInboxOps(harness);
        const middle = items[1];
        if (middle === undefined) return assert.fail("no middle item");
        yield* middle.withdraw;
        assert.deepStrictEqual(ops, [[["p", ["items"], 1, 1, []]]]);
        assert.deepStrictEqual(yield* inbox(harness, root), [
          [items[0]?.id, "followUp"],
          [items[2]?.id, "followUp"],
        ]);
        yield* harness.close;
      }),
    ),
  );

  it.effect("stores the inbox as a base exactly when it becomes empty", () =>
    run(backend)(
      Effect.gen(function* () {
        const written: Array<{ kind: "base" | "delta"; empty: boolean }> = [];
        let inboxId: number | undefined;
        // Record the inbox's document changes.
        const storage = yield* controlledStorage;
        storage.intercept.commit = (batches, next) => {
          for (const write of batches.flat()) {
            if (write.type !== "document.change" || write.id !== inboxId) continue;
            const content = write.content;
            const items = content.kind === "base" ? content.value["items"] : undefined;
            const empty = Array.isArray(items) && items.length === 0;
            written.push({ kind: content.kind, empty });
          }
          return next;
        };
        const setup = chatSetup();
        const first = gated(answer("first"));
        setup.faux.setResponses([first.step, answer("second")]);
        const { harness, root } = yield* openChat(storage, setup);
        yield* harness.session.observeCommits({
          advance: (publication) => {
            for (const change of documentChanges(publication)) {
              if (change.record.kind === "core.inbox") inboxId = change.record.id;
            }
          },
          end: () => {},
        });
        const input = yield* root.submit({ type: "input", content: "a" });
        yield* reached(first.reached);
        yield* root.submit({ type: "input", content: "f1" });
        const f2 = yield* root.submit({ type: "input", content: "f2" });
        yield* first.release;
        yield* settle(input);
        yield* settle(f2);
        // The inbox ID is learned from the f1 push; later writes: the f2 push, removing f1, and emptying the
        // inbox.
        assert.deepStrictEqual(written, [
          { kind: "delta", empty: false },
          { kind: "delta", empty: false },
          { kind: "base", empty: true },
        ]);
        yield* harness.close;
      }),
    ),
  );

  it("keeps a complete inbox base exactly while it is empty, and a usage base on every change", () => {
    const info = { deltasSinceBase: 1000 };
    const item: InboxItem = { id: SubmissionId.make(1), mode: "followUp", content: "x" };
    const inboxWhen = InboxDoc.definition.checkpointWhen;
    const usageWhen = UsageDoc.definition.checkpointWhen;
    assert.isTrue(inboxWhen?.({ items: [] }, [], info));
    assert.isFalse(inboxWhen?.({ items: [item] }, [], info));
    assert.isTrue(usageWhen?.({ models: {}, tools: {} }, [], info));
  });
});

describeEachBackend("usage", (backend) => {
  it.effect("totals assistant usage per model and tool usage per tool, and sums the Session", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        const spent: Usage = {
          input: 1,
          output: 2,
          cacheRead: 3,
          cacheWrite: 4,
          totalTokens: 10,
          cost: { input: 0.1, output: 0.2, cacheRead: 0.3, cacheWrite: 0.4, total: 1 },
        };
        const gate = Deferred.makeUnsafe<void>();
        yield* Deferred.succeed(gate, undefined);
        setup.faux.setResponses([HOLD, answer("done"), answer("other")]);
        const { harness, root } = yield* openChat(yield* controlledStorage, setup, {
          extensions: [holdTool(gate, { output: [], usage: spent })],
        });
        yield* settle(yield* root.submit({ type: "input", content: "a" }));
        const entries = yield* allEntries(root);
        const assistants = assistantMessages(entries);
        const expected = assistants.reduce(
          (sum, message) => ({
            input: sum.input + message.usage.input,
            output: sum.output + message.usage.output,
            totalTokens: sum.totalTokens + message.usage.totalTokens,
          }),
          { input: 0, output: 0, totalTokens: 0 },
        );
        const usage = Option.getOrThrow(yield* harness.session.snapshot(UsageDoc, root.id));
        const model = usage.models["faux/faux-1"];
        assert.deepStrictEqual(
          { input: model?.input, output: model?.output, totalTokens: model?.totalTokens },
          expected,
        );
        assert.deepStrictEqual(usage.tools, { hold: spent });
        const result = entries.find((entry) => entry.kind === "core.tool-result")?.model?.[0];
        assert.strictEqual(result?.role, "toolResult");
        assert.deepStrictEqual(result?.role === "toolResult" ? result.usage : undefined, spent);

        // A fork starts at zero; the Session total adds every conversation once.
        const last = entries.at(-1);
        if (last === undefined) return assert.fail("no entries");
        const fork = yield* root.fork(last.id, { ownership: { kind: "ownerless" } });
        assert.deepStrictEqual(
          yield* harness.session.snapshot(UsageDoc, fork.id),
          Option.some({ models: {}, tools: {} }),
        );
        yield* settle(yield* fork.submit({ type: "input", content: "b" }));
        const forkUsage = Option.getOrThrow(yield* harness.session.snapshot(UsageDoc, fork.id))
          .models["faux/faux-1"];
        const total = yield* harness.harness.usage;
        assert.strictEqual(
          total.models["faux/faux-1"]?.output,
          expected.output + (forkUsage?.output ?? Number.NaN),
        );
        assert.deepStrictEqual(total.tools, { hold: spent });
        yield* harness.close;
      }),
    ),
  );

  it.effect(
    "counts failed attempts, converted partials, and tool usage replaced by afterTool",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const setup = chatSetup({ tokensPerSecond: 200, tokenSize: { min: 1, max: 1 } });
          const spent: Usage = {
            input: 5,
            output: 0,
            cacheRead: 0,
            cacheWrite: 0,
            totalTokens: 5,
            cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
          };
          const gate = Deferred.makeUnsafe<void>();
          yield* Deferred.succeed(gate, undefined);
          const hooks = Extension.make({
            name: "hooks",
            hooks: [
              Hook.tool({
                afterTool: (_call, result) => Effect.succeed({ ...result, usage: spent }),
              }),
            ],
          });
          const retryable = fauxAssistantMessage([], {
            stopReason: "error",
            errorMessage: "503 Service Unavailable",
          });
          setup.faux.setResponses([retryable, HOLD, answer("done"), answer("x".repeat(400))]);
          const { harness, root } = yield* openChat(yield* controlledStorage, setup, {
            extensions: [holdTool(gate), hooks],
          });
          setup.settings.retry = { enabled: true, maxRetries: 1, baseDelayMs: 1 };
          yield* settle(yield* root.submit({ type: "input", content: "a" }));
          assert.deepStrictEqual(
            Option.getOrThrow(yield* harness.session.snapshot(UsageDoc, root.id)).tools,
            { hold: spent },
          );

          // A partial committed while streaming becomes an aborted entry on abort; its usage counts too.
          const input = yield* root.submit({ type: "input", content: "b" });
          yield* waitFor(
            live(harness, root).pipe(
              Effect.map((value) => value?.generation?.message !== undefined),
            ),
          );
          const taskId = (yield* live(harness, root))?.run?.taskId;
          if (taskId === undefined) return assert.fail("no run");
          yield* finished(harness.harness.abortTask(taskId));
          yield* settle(input);
          const assistants = assistantMessages(yield* allEntries(root));
          assert.deepStrictEqual(
            assistants.map((message) => message.stopReason),
            ["error", "toolUse", "stop", "aborted"],
          );
          const output = assistants.reduce((sum, message) => sum + message.usage.output, 0);
          assert.strictEqual(
            Option.getOrThrow(yield* harness.session.snapshot(UsageDoc, root.id)).models[
              "faux/faux-1"
            ]?.output,
            output,
          );
          yield* harness.close;
        }),
      ),
  );

  it.effect(
    "keeps tools named like object prototype keys in the ledger and the Session total",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const { harness, root } = yield* openChat(yield* controlledStorage, chatSetup());
          const usage: Usage = {
            input: 1,
            output: 1,
            cacheRead: 0,
            cacheWrite: 0,
            totalTokens: 2,
            cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
          };
          const names = ["constructor", "__proto__", "toString"];
          for (const name of names) {
            yield* root.commit(recordUsage(root.id, "tools", name, usage));
            yield* root.commit(recordUsage(root.id, "tools", name, usage));
          }
          const total = yield* harness.harness.usage;
          assert.deepStrictEqual(Object.keys(total.tools), names);
          for (const name of names) {
            const descriptor: { readonly value?: Usage } | undefined =
              Object.getOwnPropertyDescriptor(total.tools, name);
            assert.strictEqual(descriptor?.value?.output, 2, name);
          }
          yield* harness.close;
        }),
      ),
  );

  it.effect(
    "keeps usage totals exact across reopen, counting a partial converted after reopen once",
    () =>
      run(backend)(
        Effect.gen(function* () {
          // Reopening is a new Harness over the same storage.
          const storage = yield* controlledStorage;
          const setup = chatSetup({ tokensPerSecond: 200, tokenSize: { min: 1, max: 1 } });
          setup.faux.setResponses([answer("first"), answer("x".repeat(400)), answer("again")]);
          let opened = yield* openChat(storage, setup);
          yield* settle(yield* opened.root.submit({ type: "input", content: "a" }));
          yield* opened.root.submit({ type: "input", content: "b" });
          const first = opened;
          yield* waitFor(
            live(first.harness, first.root).pipe(
              Effect.map((value) => value?.generation?.message !== undefined),
            ),
          );
          // Closing mid-stream keeps the committed partial; the reopened request converts it into an aborted entry.
          yield* opened.harness.close;
          opened = yield* openChat(storage, setup);
          yield* opened.harness.harness.resume;
          yield* idle(opened.harness);
          const assistants = assistantMessages(yield* allEntries(opened.root));
          assert.deepStrictEqual(
            assistants.map((message) => message.stopReason),
            ["stop", "aborted", "stop"],
          );
          const total = (yield* opened.harness.harness.usage).models["faux/faux-1"];
          const sum = (field: "input" | "output" | "totalTokens") =>
            assistants.reduce((value, message) => value + message.usage[field], 0);
          assert.deepStrictEqual(
            [total?.input, total?.output, total?.totalTokens],
            [sum("input"), sum("output"), sum("totalTokens")],
          );
          yield* opened.harness.close;
        }),
      ),
  );

  it.effect("records usage as numeric sets on the ledger in the entry's commit", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        setup.faux.setResponses([answer("one"), answer("two")]);
        const { harness, root } = yield* openChat(yield* controlledStorage, setup);
        const commits: Array<{ entries: number; ops: Delta.Delta }> = [];
        yield* harness.session.observeCommits({
          advance: (publication) => {
            for (const change of documentChanges(publication)) {
              if (change.record.kind !== "core.usage") continue;
              const entries = publication.changes.filter((other) => other.type === "entry").length;
              commits.push({ entries, ops: change.ops });
            }
          },
          end: () => {},
        });
        yield* settle(yield* root.submit({ type: "input", content: "a" }));
        yield* settle(yield* root.submit({ type: "input", content: "b" }));
        assert.lengthOf(commits, 2);
        const firstOps = commits[0]?.ops ?? [];
        assert.lengthOf(firstOps, 1);
        const set = firstOps[0];
        assert.strictEqual(set?.[0], "s");
        assert.deepStrictEqual(set?.[1], ["models", "faux/faux-1"]);
        assert.isObject(set?.[2]);
        assert.isTrue(
          (commits[1]?.ops ?? []).every((op) => op[0] === "s" && op[1][1] === "faux/faux-1"),
        );
        assert.isTrue(commits.every((commit) => commit.entries >= 1));
        yield* harness.close;
      }),
    ),
  );
});

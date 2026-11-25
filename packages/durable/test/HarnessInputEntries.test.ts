// Inputs placed as application entries, and data-only writes appended at once, on every backend.
//
// `openChat` opens a Harness over a `ControlledStorage`; a faux step racing its gate against the request's abort
// signal is an Effect interrupted with the request; `now` is the `TestClock`; reopening a SQLite file is a new
// Harness over the same storage; `addTool` and `addSection` are extensions given to `openChat`. Waits on the agent
// let the `TestClock` advance in 5 ms steps (`waitFor`). An untyped caller's draft is a typed draft behind a cast.
//
// "appends a data-only write mid-run in time order, leaving the model's messages unchanged" collects the
// conversation's watch frames and checks the first frame that shows the write: the entry is last and
// `core.live.run` still holds the input.
import { assert, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import * as AgentTool from "../src/AgentTool.ts";
import * as Extension from "../src/Extension.ts";
import type { Conversation } from "../src/Harness.ts";
import type { EntryId } from "../src/Ids.ts";
import { InboxDoc } from "../src/Inbox.ts";
import { LiveDoc, type LiveState } from "../src/Live.ts";
import type { AssistantMessage } from "../src/Message.ts";
import type { EntryRecord, SubmissionRecord } from "../src/Records.ts";
import * as Section from "../src/Section.ts";
import type { Submission, SubmissionDraft } from "../src/Submission.ts";
import {
  type FauxResponseFactory,
  fauxAssistantMessage,
  fauxText,
  fauxToolCall,
} from "../src/testing/FauxModel.ts";
import type { ConversationView } from "../src/View.ts";
import { allEntries, chatSetup, openChat, textOf, waitFor } from "./ChatSupport.ts";
import { anyNumber, type TestHarness } from "./HarnessSupport.ts";
import { controlledStorage, describeEachBackend, run } from "./SessionSupport.ts";

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

/** A `hold` tool whose calls wait for `gate`. */
const holdTool = (gate: Deferred.Deferred<void>) =>
  Extension.make({
    name: "hold",
    tools: [
      AgentTool.make("hold", {
        description: "Waits for the test",
        parameters: Schema.Struct({}),
        jsonSchema: { type: "object", properties: {} },
        execute: () =>
          Deferred.await(gate).pipe(
            Effect.as({ output: [{ type: "text", text: "held" }] } as const),
          ),
      }),
    ],
  });

const HOLD = fauxAssistantMessage([fauxToolCall("hold", {}, { id: "c1" })], {
  stopReason: "toolUse",
});

/** Role and text of each request message, as the provider receives them. */
const requestLog =
  (requests: Array<Array<string>>, reply: AssistantMessage): FauxResponseFactory =>
  (request) =>
    Effect.sync(() => {
      requests.push(request.messages.map((message) => `${message.role}:${textOf(message) ?? ""}`));
      return reply;
    });

/** Kind, application data, and model text of each entry, skipping system entries. */
const transcript = (entries: ReadonlyArray<EntryRecord>): Array<Array<unknown>> =>
  entries
    .filter((entry) => entry.kind !== "core.system")
    .map((entry) => {
      const message = entry.model?.[0];
      const text =
        message === undefined || message.role === "toolResult" ? undefined : textOf(message);
      const row: Array<unknown> = [entry.kind];
      if (entry.data !== undefined && !entry.kind.startsWith("core.")) row.push(entry.data);
      if (text !== undefined) row.push(text);
      return row;
    });

/** Runs `effect` and returns its result once it settles, letting the `TestClock` advance meanwhile. */
const finished = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  Effect.gen(function* () {
    const fiber = yield* Effect.forkChild(effect, { startImmediately: true });
    yield* waitFor(Effect.sync(() => fiber.pollUnsafe() !== undefined));
    return yield* Fiber.join(fiber);
  });

/** Waits for a submission to settle. */
const settle = (submission: Submission) => finished(submission.await);

/** Waits until a deferred is done, letting the `TestClock` advance meanwhile. */
const reached = (deferred: Deferred.Deferred<void>) => waitFor(Deferred.isDone(deferred));

const idle = (harness: TestHarness) => finished(harness.harness.awaitIdle);

/** The fields of a submission record a case matches. */
interface Expected {
  readonly status?: SubmissionRecord["status"];
  readonly reason?: string | undefined;
  readonly entry?: EntryId | undefined;
  readonly answer?: EntryId | undefined;
}

/** Asserts that a submission record matches the expected fields. */
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

const inboxItems = (harness: TestHarness, root: Conversation) =>
  harness.session
    .snapshot(InboxDoc, root.id)
    .pipe(Effect.map((found) => Option.getOrThrow(found).items));

const live = (harness: TestHarness, root: Conversation) =>
  harness.session.snapshot(LiveDoc, root.id).pipe(Effect.map(Option.getOrUndefined));

/** Whether a watch frame shows a `chat.message` entry. */
const showsChat = (frame: ConversationView | null) =>
  frame?.entries.some((entry) => entry.kind === "chat.message") === true;

const toolRunning = (harness: TestHarness, root: Conversation) =>
  waitFor(live(harness, root).pipe(Effect.map((value) => value?.tools?.[0]?.status === "running")));

const mention = (text: string) =>
  ({ kind: "chat.mention", data: { author: "ann", text } }) as const;

/** What an untyped caller passes as an input draft. */
interface UntypedInput {
  readonly type: "input";
  readonly content: string;
  readonly entry: Schema.JsonObject;
}

/** An untyped caller's draft. */
const untyped = (draft: UntypedInput): SubmissionDraft =>
  // SAFETY: deliberately not a valid draft (its entry may carry any fields); the case checks what admission does
  // with it.
  draft as SubmissionDraft;

describeEachBackend("input entries", (backend) => {
  it.effect(
    "places an idle input as its entry: the given kind and data, with the content as its user message",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const setup = chatSetup();
          // The clock reads 42.
          yield* TestClock.setTime(42);
          const requests: Array<Array<string>> = [];
          setup.faux.setResponses([requestLog(requests, answer("hi ann"))]);
          const { harness, root } = yield* openChat(yield* controlledStorage, setup);
          const submission = yield* root.submit({
            type: "input",
            content: "ann: hi",
            entry: mention("hi"),
          });
          const settled = yield* settle(submission);
          const entries = yield* allEntries(root);
          assert.deepStrictEqual(transcript(entries), [
            ["chat.mention", { author: "ann", text: "hi" }, "ann: hi"],
            ["core.assistant", "hi ann"],
          ]);
          const placed = entries.find((entry) => entry.kind === "chat.mention");
          if (placed === undefined) return assert.fail("no placed entry");
          assert.deepStrictEqual(placed.model, [
            { role: "user", content: "ann: hi", timestamp: 42 },
          ]);
          assertMatches(settled, { status: "done", entry: placed.id, answer: entries.at(-1)?.id });
          assert.deepStrictEqual(requests, [["user:ann: hi"]]);
          // The context of a fork at the input is the same as for a core.user entry.
          const view = yield* root.context({ at: placed.id });
          assert.deepStrictEqual(
            view.messages.filter((message) => message.role !== "system"),
            placed.model,
          );
          yield* harness.close;
        }),
      ),
  );

  it.effect(
    "rejects an input entry whose kind is not a non-empty string or starts with core., writing nothing",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const setup = chatSetup();
          const { harness, root } = yield* openChat(yield* controlledStorage, setup);
          const before = (yield* allEntries(root)).length;
          for (const kind of [123, "", "core.user", "core.compaction"]) {
            const draft = untyped({ type: "input", content: "x", entry: { kind } });
            const error = yield* Effect.flip(root.submit(draft));
            assert.include(error.message, "An input entry's kind must be");
          }
          assert.strictEqual((yield* allEntries(root)).length, before);
          assert.deepStrictEqual(yield* live(harness, root), {});
          // Writes keep their kinds: reset() writes core.reset.
          yield* root.reset(undefined);
          assert.strictEqual((yield* allEntries(root)).at(-1)?.kind, "core.reset");
          yield* harness.close;
        }),
      ),
  );

  it.effect("takes only kind and data from an untyped caller's entry", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        const first = gated(answer("first"));
        setup.faux.setResponses([answer("idle"), first.step, answer("queued")]);
        const { harness, root } = yield* openChat(yield* controlledStorage, setup);
        const extra = {
          kind: "chat.mention",
          model: [{ role: "user", content: "forged", timestamp: 0 }],
          head: "self",
          edits: [{ target: 1, action: "omit" }],
        };
        const draft = (content: string) => untyped({ type: "input", content, entry: extra });
        yield* settle(yield* root.submit(draft("idle")));
        yield* root.submit({ type: "input", content: "a" });
        yield* reached(first.reached);
        const queued = yield* root.submit(draft("later"));
        assert.deepStrictEqual((yield* inboxItems(harness, root)).at(-1), {
          id: queued.id,
          mode: "followUp",
          content: "later",
          entry: { kind: "chat.mention" },
        });
        yield* first.release;
        yield* settle(queued);
        const mentions = (yield* allEntries(root)).filter((entry) => entry.kind === "chat.mention");
        assert.deepStrictEqual(
          mentions.map((entry) => [
            entry.head,
            entry.edits,
            textOf(entry.model?.[0]),
            entry.model?.length,
          ]),
          [
            [undefined, undefined, "idle", 1],
            [undefined, undefined, "later", 1],
          ],
        );
        yield* harness.close;
      }),
    ),
  );

  it.effect(
    "queues busy inputs with their entries in the inbox and places them as those entries at boundaries",
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
          const steer = yield* root.submit({
            type: "input",
            content: "s",
            entry: mention("s"),
            whenBusy: "steer",
            requestId: "r-s",
          });
          const followUp = yield* root.submit({
            type: "input",
            content: "f",
            entry: { kind: "p2.input" },
            whenBusy: "followUp",
          });
          assert.deepStrictEqual(yield* inboxItems(harness, root), [
            {
              id: steer.id,
              mode: "steer",
              content: "s",
              entry: { kind: "chat.mention", data: { author: "ann", text: "s" } },
            },
            { id: followUp.id, mode: "followUp", content: "f", entry: { kind: "p2.input" } },
          ]);
          // The request ID deduplicates a queued input with an entry as any other.
          const again = yield* root.submit({
            type: "input",
            content: "other",
            entry: mention("other"),
            whenBusy: "steer",
            requestId: "r-s",
          });
          assert.strictEqual(again.id, steer.id);
          const busy = yield* Effect.flip(
            root.submit({ type: "input", content: "x", entry: mention("x"), whenBusy: "reject" }),
          );
          assert.strictEqual(busy._tag, "ConversationBusy");
          assert.lengthOf(yield* inboxItems(harness, root), 2);

          yield* Deferred.succeed(gate, undefined);
          yield* settle(followUp);
          const entries = yield* allEntries(root);
          assert.deepStrictEqual(transcript(entries), [
            ["core.user", "a"],
            ["core.assistant"],
            ["core.tool-result"],
            ["chat.mention", { author: "ann", text: "s" }, "s"],
            ["core.assistant", "after tools"],
            ["p2.input", "f"],
            ["core.assistant", "follow-up"],
          ]);
          const answers = entries.filter((entry) => entry.kind === "core.assistant");
          const placed = (kind: string) => entries.find((entry) => entry.kind === kind)?.id;
          assertMatches(yield* input.status, { status: "done", answer: answers[1]?.id });
          assertMatches(yield* steer.status, {
            status: "done",
            entry: placed("chat.mention"),
            answer: answers[1]?.id,
          });
          assertMatches(yield* followUp.status, {
            status: "done",
            entry: placed("p2.input"),
            answer: answers[2]?.id,
          });
          yield* harness.close;
        }),
      ),
  );

  it.effect("withdraws queued inputs with entries, one by one and by conversation abort", () =>
    run(backend)(
      Effect.gen(function* () {
        const setup = chatSetup();
        const first = gated(answer("first"));
        setup.faux.setResponses([first.step]);
        const { harness, root } = yield* openChat(yield* controlledStorage, setup);
        const input = yield* root.submit({ type: "input", content: "a" });
        yield* reached(first.reached);
        const withdrawn = yield* root.submit({ type: "input", content: "w", entry: mention("w") });
        const aborted = yield* root.submit({ type: "input", content: "x", entry: mention("x") });
        assert.deepStrictEqual(yield* withdrawn.withdraw, { _tag: "Withdrawn" });
        assert.deepStrictEqual(
          (yield* inboxItems(harness, root)).map((item) => item.id),
          [aborted.id],
        );
        yield* finished(root.abort());
        for (const submission of [withdrawn, aborted]) {
          assertMatches(yield* settle(submission), { status: "unanswered", reason: "aborted" });
        }
        assertMatches(yield* settle(input), { status: "unanswered", reason: "aborted" });
        assert.deepStrictEqual(yield* inboxItems(harness, root), []);
        assert.isFalse((yield* allEntries(root)).some((entry) => entry.kind === "chat.mention"));
        yield* harness.close;
      }),
    ),
  );
});

describeEachBackend("data-only writes", (backend) => {
  it.effect(
    "appends a data-only write mid-run in time order, leaving the model's messages unchanged",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const setup = chatSetup();
          const gate = Deferred.makeUnsafe<void>();
          const requests: Array<Array<string>> = [];
          setup.faux.setResponses([
            requestLog(requests, HOLD),
            requestLog(requests, answer("done")),
          ]);
          const { harness, root } = yield* openChat(yield* controlledStorage, setup, {
            extensions: [holdTool(gate)],
          });
          // The view's frames, in order.
          const frames: Array<ConversationView> = [];
          const watch = yield* root.watch();
          const collecting = yield* Effect.forkChild(
            Stream.runForEach(watch.frames, (frame) =>
              Effect.sync(() => {
                frames.push(frame.value);
              }),
            ),
            { startImmediately: true },
          );
          const input = yield* root.submit({ type: "input", content: "a" });
          yield* toolRunning(harness, root);
          const chat = { kind: "chat.message", data: { author: "bob", text: "lunch?" } };
          const write = yield* root.submit({ type: "write", entry: chat, requestId: "bob:1" });
          // Settled and appended in its own commit, between the call and its still-running tool's result.
          const settled = yield* write.status;
          assert.strictEqual(settled.status, "done");
          if (settled.type !== "write" || settled.status !== "done") return assert.fail("not done");
          assert.deepStrictEqual(yield* inboxItems(harness, root), []);
          assert.deepStrictEqual(transcript(yield* allEntries(root)), [
            ["core.user", "a"],
            ["core.assistant"],
            ["chat.message", { author: "bob", text: "lunch?" }],
          ]);
          // The view shows it while the run goes on.
          yield* waitFor(Effect.sync(() => frames.some(showsChat)));
          const shown = frames.find(showsChat);
          assert.strictEqual(shown?.entries.at(-1)?.kind, "chat.message");
          assert.strictEqual(shown?.entries.at(-1)?.id, settled.entry);
          // SAFETY: a view's `core.live` value, which its schema checked when it was written.
          const shownLive = shown?.docs["core.live"] as LiveState | undefined;
          assert.deepStrictEqual(shownLive?.run?.inputs, [input.id]);
          // A repeated request ID returns the same write without a second entry.
          const repeated = yield* root.submit({ type: "write", entry: chat, requestId: "bob:1" });
          assert.strictEqual(repeated.id, write.id);

          yield* Deferred.succeed(gate, undefined);
          yield* settle(input);
          assert.deepStrictEqual(transcript(yield* allEntries(root)), [
            ["core.user", "a"],
            ["core.assistant"],
            ["chat.message", { author: "bob", text: "lunch?" }],
            ["core.tool-result"],
            ["core.assistant", "done"],
          ]);
          assert.deepStrictEqual(requests, [
            ["system:", "user:a"],
            ["system:", "user:a", "assistant:", "toolResult:held"],
          ]);
          yield* Fiber.interrupt(collecting);
          yield* harness.close;
        }),
      ),
  );

  it.effect(
    "queues a data-only write behind a queued reset, so it lands after the reset and stays visible",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const setup = chatSetup();
          const first = gated(answer("first"));
          setup.faux.setResponses([first.step]);
          const { harness, root } = yield* openChat(yield* controlledStorage, setup);
          const input = yield* root.submit({ type: "input", content: "a" });
          yield* reached(first.reached);
          yield* root.reset(undefined);
          const chat = yield* root.submit({
            type: "write",
            entry: { kind: "chat.message", data: "hi" },
          });
          assert.strictEqual((yield* chat.status).status, "queued");
          yield* first.release;
          yield* settle(input);
          const settled = yield* settle(chat);
          const entries = yield* allEntries(root);
          const reset = entries.find((entry) => entry.kind === "core.reset");
          if (reset === undefined || settled.type === "continue" || settled.status !== "done")
            return assert.fail("not placed");
          assert.isAbove(settled.entry, reset.id);
          assert.deepStrictEqual(
            (yield* root.context()).entries.map((entry) => entry.kind),
            ["core.reset", "chat.message"],
          );
          // Once the reset is placed, display-only writes go in at once again.
          const later = yield* root.submit({
            type: "write",
            entry: { kind: "chat.message", data: "again" },
          });
          assert.strictEqual((yield* later.status).status, "done");
          yield* harness.close;
        }),
      ),
  );

  it.effect(
    "leaves the request unchanged by a data-only write that lands while the generation prepares",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const setup = chatSetup();
          const rendering = Deferred.makeUnsafe<void>();
          const render = Deferred.makeUnsafe<void>();
          let renders = 0;
          const slow = Extension.make({
            name: "slow",
            sections: [
              Section.make("slow", () =>
                Effect.suspend(() =>
                  renders++ === 1
                    ? Deferred.succeed(rendering, undefined).pipe(
                        Effect.andThen(Deferred.await(render)),
                        Effect.as("SLOW"),
                      )
                    : Effect.succeed("SLOW"),
                ),
              ),
            ],
          });
          const requests: Array<Array<string>> = [];
          setup.faux.setResponses([
            requestLog(requests, answer("one")),
            requestLog(requests, answer("two")),
          ]);
          const { harness, root } = yield* openChat(yield* controlledStorage, setup, {
            extensions: [slow],
          });
          yield* settle(yield* root.submit({ type: "input", content: "a" }));
          const second = yield* root.submit({ type: "input", content: "b" });
          yield* reached(rendering);
          yield* root.submit({ type: "write", entry: { kind: "chat.message", data: "hi" } });
          yield* Deferred.succeed(render, undefined);
          yield* settle(second);
          assert.deepStrictEqual(requests[1], ["system:", "user:a", "assistant:one", "user:b"]);
          const kinds = (yield* allEntries(root)).map((entry) => entry.kind);
          // The unchanged prompt adds no second core.system entry.
          assert.deepStrictEqual(kinds, [
            "core.user",
            "core.system",
            "core.assistant",
            "core.user",
            "chat.message",
            "core.assistant",
          ]);
          yield* harness.close;
        }),
      ),
  );

  it.effect(
    "appends a data-only write while idle after a failed run without placing the queued items",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const setup = chatSetup();
          const failing = gated(
            fauxAssistantMessage([], { stopReason: "error", errorMessage: "invalid request" }),
          );
          setup.faux.setResponses([failing.step, answer("for f")]);
          const { harness, root } = yield* openChat(yield* controlledStorage, setup);
          const input = yield* root.submit({ type: "input", content: "a" });
          yield* reached(failing.reached);
          const f = yield* root.submit({ type: "input", content: "f", entry: mention("f") });
          yield* failing.release;
          assertMatches(yield* settle(input), { status: "unanswered", reason: "model_error" });
          yield* idle(harness);
          const note = yield* root.submit({ type: "write", entry: { kind: "note", data: "n" } });
          assert.strictEqual((yield* note.status).status, "done");
          yield* idle(harness);
          assert.strictEqual((yield* f.status).status, "queued");
          assert.deepStrictEqual(
            (yield* inboxItems(harness, root)).map((item) => item.id),
            [f.id],
          );
          // Idle `core.live` keeps why the run failed.
          const failed = yield* live(harness, root);
          assert.deepStrictEqual(failed, {
            runFailure: {
              taskId: anyNumber(failed?.runFailure?.taskId),
              inputs: [input.id],
              reason: "model_error",
              detail: "invalid request",
              first: anyNumber(failed?.runFailure?.first),
            },
          });
          assert.strictEqual(setup.faux.state.callCount, 1);
          // The next input places the waiting mention first.
          const g = yield* root.submit({ type: "input", content: "g" });
          yield* settle(g);
          assert.deepStrictEqual(
            transcript(yield* allEntries(root)).filter(([kind]) => kind !== "core.assistant"),
            [
              ["core.user", "a"],
              ["note", "n"],
              ["chat.mention", { author: "ann", text: "f" }, "f"],
              ["core.user", "g"],
            ],
          );
          yield* harness.close;
        }),
      ),
  );

  it.effect("keeps a queued input's entry across reopen and places it as that entry", () =>
    run(backend)(
      Effect.gen(function* () {
        // Reopening is the same storage under a new Harness.
        const storage = yield* controlledStorage;
        const setup = chatSetup();
        const first = gated(answer("first"));
        setup.faux.setResponses([first.step, answer("first again"), answer("for f")]);
        let opened = yield* openChat(storage, setup);
        yield* opened.root.submit({ type: "input", content: "a" });
        yield* reached(first.reached);
        const f = (yield* opened.root.submit({ type: "input", content: "f", entry: mention("f") }))
          .id;
        yield* opened.harness.close;

        opened = yield* openChat(storage, setup);
        const item = (yield* inboxItems(opened.harness, opened.root)).find((each) => each.id === f);
        assert.deepStrictEqual(item?.entry, mention("f"));
        const settled = yield* settle(
          Option.getOrThrow(yield* opened.harness.harness.submission(f)),
        );
        const placed = (yield* allEntries(opened.root)).find(
          (entry) => entry.kind === "chat.mention",
        );
        if (placed === undefined) return assert.fail("not placed");
        assertMatches(settled, { status: "done", entry: placed.id });
        assert.deepStrictEqual(placed.data, mention("f").data);
        const message = placed.model?.[0];
        assert.lengthOf(placed.model ?? [], 1);
        assert.strictEqual(message?.role, "user");
        assert.strictEqual(message?.role === "user" ? message.content : undefined, "f");
        yield* opened.harness.close;
      }),
    ),
  );

  it.effect(
    "appends a data-only write at once behind a non-empty inbox, and still queues writes with model or edits",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const setup = chatSetup();
          const first = gated(answer("first"));
          setup.faux.setResponses([first.step, answer("second")]);
          const { harness, root } = yield* openChat(yield* controlledStorage, setup);
          const input = yield* root.submit({ type: "input", content: "a" });
          yield* reached(first.reached);
          const followUp = yield* root.submit({ type: "input", content: "f" });
          const model = yield* root.submit({
            type: "write",
            entry: { kind: "note", model: [{ role: "user", content: "m", timestamp: 0 }] },
          });
          const target = (yield* allEntries(root)).find((entry) => entry.kind === "core.user");
          if (target === undefined) return assert.fail("no input entry");
          const edit = yield* root.submit({
            type: "write",
            entry: { kind: "redact", edits: [{ target: target.id, action: "omit" }] },
          });
          const note = yield* root.submit({ type: "write", entry: { kind: "note", data: "n" } });
          assert.strictEqual((yield* note.status).status, "done");
          assert.deepStrictEqual(
            (yield* inboxItems(harness, root)).map((item) => [item.id, item.mode]),
            [
              [followUp.id, "followUp"],
              [model.id, "write"],
              [edit.id, "write"],
            ],
          );
          yield* first.release;
          yield* settle(followUp);
          assert.deepStrictEqual(transcript(yield* allEntries(root)), [
            ["core.user", "a"],
            ["note", "n"],
            ["core.assistant", "first"],
            ["note", "m"],
            ["redact"],
            ["core.user", "f"],
            ["core.assistant", "second"],
          ]);
          assertMatches(yield* input.status, { status: "done" });
          yield* harness.close;
        }),
      ),
  );
});

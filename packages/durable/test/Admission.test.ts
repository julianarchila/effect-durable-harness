import { assert, describe, it } from "@effect/vitest";
import * as Option from "effect/Option";
import type * as Schema from "effect/Schema";
import { ConversationId, EntryId, SubmissionId } from "../src/Ids.ts";
import type { InboxItem } from "../src/Inbox.ts";
import type { SubmissionRecord } from "../src/Records.ts";
import type {
  InputSubmissionDraft,
  SubmissionDraft,
  WriteSubmissionDraft,
} from "../src/Submission.ts";
import { type AdmissionInput, decideAdmission } from "../src/internal/submissions.ts";

// Tests of admission's decision: `decideAdmission` is a pure function of what admission loaded. The Harness suites
// cover the same rules through `Conversation.submit`; these check each branch, and the reads it asks for, in order.

const conversation = ConversationId.make(1);
const input: InputSubmissionDraft = { type: "input", content: "hi" };
const write: WriteSubmissionDraft = { type: "write", entry: { kind: "note", model: [] } };
const displayOnly: WriteSubmissionDraft = { type: "write", entry: { kind: "note", data: 1 } };
const queuedItem: InboxItem = { id: SubmissionId.make(9), mode: "followUp", content: "earlier" };
const idle = { items: [], head: undefined };

/** What an untyped caller passes as an input draft. */
interface UntypedInput {
  readonly type: "input";
  readonly content: string;
  readonly requestId: string;
  readonly entry: Schema.JsonObject;
}

/** An untyped caller's draft. */
const untyped = (draft: UntypedInput): SubmissionDraft =>
  // SAFETY: deliberately not a valid draft (its kind may not be a string); the case checks what admission does
  // with it, as an untyped caller would.
  draft as SubmissionDraft;

const record = (type: "input" | "write"): SubmissionRecord => ({
  id: SubmissionId.make(4),
  conversationId: conversation,
  requestId: "r",
  type,
  status: "queued",
});

/** Every read admission makes for an idle conversation, with an empty inbox. */
const loaded = (draft: AdmissionInput["draft"], more: Partial<AdmissionInput> = {}) =>
  decideAdmission({
    draft,
    existing: Option.none(),
    queuedReset: false,
    busy: false,
    boundary: idle,
    ...more,
  });

describe("decideAdmission", () => {
  describe("reads, in order", () => {
    it("asks for the request ID first, only when the draft has one", () => {
      assert.deepStrictEqual(decideAdmission({ draft: { ...input, requestId: "r" } }), {
        _tag: "Load",
        read: "request",
        requestId: "r",
      });
      // No request ID, no lookup: `core.live` comes next.
      assert.deepStrictEqual(decideAdmission({ draft: input }), { _tag: "Load", read: "live" });
    });

    it("asks for the inbox before core.live only for a display-only write", () => {
      assert.deepStrictEqual(decideAdmission({ draft: displayOnly }), {
        _tag: "Load",
        read: "inbox",
      });
      assert.deepStrictEqual(decideAdmission({ draft: write }), { _tag: "Load", read: "live" });
    });

    it("asks for the boundary only when idle", () => {
      assert.deepStrictEqual(decideAdmission({ draft: input, busy: false }), {
        _tag: "Load",
        read: "boundary",
      });
      // A busy conversation needs no boundary.
      assert.strictEqual(decideAdmission({ draft: input, busy: true })._tag, "Queue");
    });
  });

  describe("the untyped caller's input entry", () => {
    for (const kind of ["", "core.user", 7]) {
      it(`rejects kind ${String(kind)} before any read`, () => {
        const draft = untyped({ type: "input", content: "hi", requestId: "r", entry: { kind } });
        assert.deepStrictEqual(decideAdmission({ draft }), {
          _tag: "InvalidEntry",
          kind: String(kind),
        });
      });
    }

    it("accepts an application kind", () => {
      const draft: InputSubmissionDraft = { ...input, entry: { kind: "app.prompt" } };
      assert.deepStrictEqual(loaded(draft), {
        _tag: "PlaceInput",
        content: "hi",
        entry: { kind: "app.prompt" },
      });
    });
  });

  describe("request IDs", () => {
    it("returns the existing submission of the same type without writing", () => {
      const plan = loaded({ ...input, requestId: "r" }, { existing: Option.some(record("input")) });
      assert.deepStrictEqual(plan, { _tag: "Existing", id: SubmissionId.make(4) });
    });

    it("rejects a request ID of the other type", () => {
      const plan = loaded({ ...write, requestId: "r" }, { existing: Option.some(record("input")) });
      assert.deepStrictEqual(plan, { _tag: "Mismatch", requestId: "r", existingType: "input" });
    });

    it("goes on when no submission has the request ID", () => {
      assert.strictEqual(loaded({ ...input, requestId: "r" })._tag, "PlaceInput");
    });
  });

  describe("display-only writes", () => {
    it("appends at once, busy or not, without reading core.live", () => {
      assert.deepStrictEqual(decideAdmission({ draft: displayOnly, queuedReset: false }), {
        _tag: "DisplayOnly",
        entry: displayOnly.entry,
      });
    });

    it("queues behind a queued reset", () => {
      assert.deepStrictEqual(decideAdmission({ draft: displayOnly, queuedReset: true }), {
        _tag: "Load",
        read: "live",
      });
      assert.deepStrictEqual(loaded(displayOnly, { queuedReset: true, busy: true }), {
        _tag: "Queue",
        queued: { mode: "write", entry: displayOnly.entry },
        runFinal: false,
      });
    });

    it("queues behind a queued reset of an idle conversation and runs a final boundary", () => {
      const reset: InboxItem = {
        id: SubmissionId.make(9),
        mode: "write",
        entry: { kind: "n", head: "self" },
      };
      const boundary = { items: [reset], head: undefined };
      assert.deepStrictEqual(loaded(displayOnly, { queuedReset: true, boundary }), {
        _tag: "Queue",
        queued: { mode: "write", entry: displayOnly.entry },
        runFinal: true,
      });
    });

    it("counts a present head, model or edits as visible to the model", () => {
      for (const entry of [
        { kind: "n", model: [] },
        { kind: "n", head: "self" as const },
        { kind: "n", edits: [] },
      ]) {
        assert.deepStrictEqual(decideAdmission({ draft: { type: "write", entry } }), {
          _tag: "Load",
          read: "live",
        });
      }
    });
  });

  describe("busy conversations", () => {
    it("rejects whenBusy reject input with nothing written", () => {
      assert.deepStrictEqual(loaded({ ...input, whenBusy: "reject" }, { busy: true }), {
        _tag: "Busy",
      });
    });

    it("queues input as steer only for whenBusy steer", () => {
      const queued = (whenBusy: InputSubmissionDraft["whenBusy"]) => {
        const plan = loaded({ ...input, whenBusy }, { busy: true });
        return plan._tag === "Queue" ? plan.queued : undefined;
      };
      assert.deepStrictEqual(queued("steer"), { mode: "steer", content: "hi", entry: undefined });
      assert.deepStrictEqual(queued("followUp"), {
        mode: "followUp",
        content: "hi",
        entry: undefined,
      });
      assert.deepStrictEqual(queued(undefined), {
        mode: "followUp",
        content: "hi",
        entry: undefined,
      });
    });

    it("queues a write without a boundary", () => {
      assert.deepStrictEqual(loaded(write, { busy: true }), {
        _tag: "Queue",
        queued: { mode: "write", entry: write.entry },
        runFinal: false,
      });
    });
  });

  describe("idle with queued items", () => {
    it("queues behind them and runs a final boundary", () => {
      const boundary = { items: [queuedItem], head: undefined };
      assert.deepStrictEqual(loaded(input, { boundary }), {
        _tag: "Queue",
        queued: { mode: "followUp", content: "hi", entry: undefined },
        runFinal: true,
      });
      assert.deepStrictEqual(loaded(write, { boundary }), {
        _tag: "Queue",
        queued: { mode: "write", entry: write.entry },
        runFinal: true,
      });
    });

    it("rejects whenBusy reject input only when busy", () => {
      const boundary = { items: [queuedItem], head: undefined };
      assert.strictEqual(loaded({ ...input, whenBusy: "reject" }, { boundary })._tag, "Queue");
    });
  });

  describe("idle with an empty inbox", () => {
    it("places input", () => {
      assert.deepStrictEqual(loaded(input), {
        _tag: "PlaceInput",
        content: "hi",
        entry: undefined,
      });
    });

    it("places a write", () => {
      assert.deepStrictEqual(loaded(write), { _tag: "PlaceWrite", entry: write.entry });
    });

    it("settles a head write before the active range stale", () => {
      const boundary = { items: [], head: EntryId.make(10) };
      const head = (target: EntryId | "self") =>
        loaded({ type: "write", entry: { kind: "n", head: target } }, { boundary })._tag;
      assert.strictEqual(head(EntryId.make(9)), "Stale");
      assert.strictEqual(head(EntryId.make(10)), "PlaceWrite");
      assert.strictEqual(head("self"), "PlaceWrite");
      // No head marker yet: nothing is before the active range.
      assert.strictEqual(
        loaded({ type: "write", entry: { kind: "n", head: EntryId.make(1) } })._tag,
        "PlaceWrite",
      );
    });
  });
});

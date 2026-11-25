// Admission, waits and withdrawal of the durable
// submissions of one Harness. Admission is one commit that loads what it reads first (the request ID, the
// inbox, `core.live`, the boundary), then decides and writes; waits are checked and registered in one step on
// the line and resolved by the commit listener after a yield.
import type * as Tracker from "@effect-durable-harness/delta/Tracker";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Predicate from "effect/Predicate";
import * as Result from "effect/Result";
import * as Tracer from "effect/Tracer";
import type * as Schema from "effect/Schema";
import type { EntryDraft } from "../Entry.ts";
import type { ConversationId, EntryId, SubmissionId } from "../Ids.ts";
import { InboxDoc, type InboxItem, type InputEntry } from "../Inbox.ts";
import { LiveDoc } from "../Live.ts";
import type { UserContent } from "../Message.ts";
import type { SubmissionNotify, SubmissionRecord } from "../Records.ts";
import type { StorageFailure, StorageRequestError } from "../AgentStorage.ts";
import {
  CommitRejected,
  ReadAfterWrite,
  type DocumentAccessError,
  InvalidJson,
  ReadOnlySession,
  SessionClosed,
  type SessionFailed,
} from "../SessionError.ts";
import {
  ConversationBusy,
  ConversationIdle,
  InvalidInputEntry,
  RequestIdMismatch,
  type SettledSubmission,
  type Submission,
  type AdmitError,
  type SubmissionDraft,
  SubmissionError,
  SubmissionNotFound,
  type WithdrawResult,
} from "../Submission.ts";
import { carrierOf, Spans, spanOf, type TraceCarrier } from "../Telemetry.ts";
import { Tx } from "../Tx.ts";
import { startRun } from "./generation.ts";
import {
  appendInput,
  applyBoundary,
  hasQueuedReset,
  isDisplayOnly,
  isStale,
  pickInputEntry,
  prepareBoundary,
  type QueueModes,
  removeInboxItem,
} from "./inbox.ts";
import { copyJson, type JsonInput } from "./documents.ts";
import type { TaskError } from "../TaskError.ts";
import type { ObservedCommit, SessionKernel } from "./session.ts";
import { transactionIdentity } from "./transaction.ts";
import * as telemetry from "./telemetry.ts";
import * as Waiters from "./waiters.ts";

type ClosedError = SessionClosed | SessionFailed;

/** What `Submissions` needs from the Harness. */
export interface SubmissionsOptions {
  readonly kernel: SessionKernel;
  /** Read at each admission, on the Session line; a reader that throws is a defect of that admission. */
  readonly queueModes: () => QueueModes;
  /** Enables task scheduling; submitting or waiting asks for progress. */
  readonly resume: () => void;
  /** A read-only Harness: a wait, which would run tasks, fails with `ReadOnlySession`. */
  readonly readOnly: boolean;
}

/** The submissions of one Harness. */
export interface Submissions {
  /**
   * `refuse`, for a handle bound to a task invocation, is checked as the first step of the admission's line
   * work, so a submit queued behind the invocation's end or abort mark writes nothing.
   */
  readonly submit: (
    conversationId: ConversationId,
    draft: SubmissionDraft,
    refuse?: (() => TaskError | undefined) | undefined,
  ) => Effect.Effect<Submission, SubmitError | TaskError>;
  /**
   * Admits inside the caller's commit, before its first
   * table write; `refuse` as for `submit`.
   */
  readonly admit: (
    conversationId: ConversationId,
    draft: SubmissionDraft,
    refuse?: (() => TaskError | undefined) | undefined,
  ) => Effect.Effect<SubmissionId, AdmitError | TaskError, Tx>;
  readonly get: (id: SubmissionId) => Effect.Effect<Option.Option<Submission>, ReadError>;
  readonly byRequest: (
    conversationId: ConversationId,
    requestId: string,
  ) => Effect.Effect<Option.Option<Submission>, ReadError>;
  readonly withdraw: (
    id: SubmissionId,
    conversationId: ConversationId | undefined,
  ) => Effect.Effect<WithdrawResult | { readonly _tag: "NotFound" }, ReadError | ReadOnlySession>;
}

type ReadError = StorageFailure | StorageRequestError | SessionFailed | SessionClosed;
type SubmitError =
  | ReadError
  | ReadOnlySession
  | CommitRejected
  | DocumentAccessError
  | ConversationBusy
  | ConversationIdle
  | SubmissionError;

/** Builds the submissions of one Harness; its listeners register on the kernel. */
export const make = (options: SubmissionsOptions) =>
  Effect.gen(function* () {
    const kernel = options.kernel;
    const waiters = Waiters.make<SubmissionId, SettledSubmission, ClosedError>();
    let closed = false;
    yield* kernel.followCommits((publication: ObservedCommit) => {
      for (const change of publication.changes) {
        if (change.type !== "submission" || !isSettled(change.value)) continue;
        waiters.resolve(change.value.id, change.value);
      }
    });
    yield* kernel.subscribeClose(() => {
      closed = true;
      waiters.rejectAll(closedError());
    });

    const closedError = (): ClosedError =>
      Option.getOrElse(kernel.failure, (): ClosedError => new SessionClosed());

    const status = (id: SubmissionId) =>
      kernel
        .readOnLine(kernel.storage.submission(id))
        .pipe(
          Effect.flatMap(
            (found): Effect.Effect<SubmissionRecord, SubmissionError> =>
              Option.isSome(found) ? Effect.succeed(found.value) : Effect.fail(notFound(id)),
          ),
        );

    /**
     * A submission, an in-commit admission and a wait ask for progress first, which a read-only Harness refuses
     * before any read, so an admitted `requestId` or a busy conversation answers nothing else.
     */
    const readOnlyRefusal = options.readOnly
      ? Effect.fail(new ReadOnlySession({ what: "running tasks" }))
      : undefined;
    const wait = (id: SubmissionId) =>
      readOnlyRefusal ??
      Effect.suspend(() => {
        options.resume();
        let registered: Waiters.Waiter<SettledSubmission, ClosedError> | undefined;
        return kernel
          .readOnLine(
            // Checked and registered on the line, so no settling publication falls
            // between them; a settled submission resolves even once close has begun.
            Effect.gen(function* () {
              const found = yield* kernel.storage.submission(id);
              if (Option.isNone(found)) return yield* notFound(id);
              if (isSettled(found.value)) return Option.some(found.value);
              if (closed) return yield* closedError();
              registered = waiters.add(id);
              return Option.none<SettledSubmission>();
            }),
          )
          .pipe(
            Effect.flatMap((settled) => {
              if (Option.isSome(settled)) return Effect.succeed(settled.value);
              if (registered === undefined)
                return Effect.die(new Error(`Submission ${id} has no waiter`));
              return registered.await;
            }),
            Effect.onInterrupt(() =>
              Effect.sync(() => {
                registered?.cancel();
              }),
            ),
          );
      });

    const withdraw = (id: SubmissionId, conversationId: ConversationId | undefined) =>
      kernel
        .commitWith(() =>
          Effect.gen(function* () {
            const tx = yield* Tx;
            const found = Option.getOrUndefined(yield* kernel.storage.submission(id));
            if (
              found === undefined ||
              (conversationId !== undefined && found.conversationId !== conversationId)
            ) {
              return { _tag: "NotFound" } as const;
            }
            if (found.status === "queued") {
              yield* tx.settleSubmission(id, { status: "unanswered", reason: "aborted" });
              yield* removeInboxItem(found.conversationId, id);
              return { _tag: "Withdrawn" } as const;
            }
            return found.status === "placed"
              ? ({ _tag: "AlreadyPlaced" } as const)
              : ({ _tag: "AlreadySettled" } as const);
          }),
        )
        .pipe(
          // Its only table read comes first and its writes are valid: a rejection or an inaccessible built-in
          // `core.inbox` is a bug, not a condition the caller handles.
          Effect.catchTag("CommitRejected", (error) => Effect.die(error)),
          Effect.catchTag("DocumentAccessError", (error) => Effect.die(error)),
        );

    const handle = (id: SubmissionId): Submission => ({
      id,
      status: status(id),
      await: wait(id),
      withdraw: withdraw(id, undefined).pipe(
        Effect.flatMap((result) =>
          result._tag === "NotFound" ? Effect.fail(notFound(id)) : Effect.succeed(result),
        ),
      ),
    });

    const submissions: Submissions = {
      submit: (conversationId, draft, refuse) =>
        readOnlyRefusal ??
        Effect.flatMap(Effect.serviceOption(Tracer.ParentSpan), (caller) =>
          traced(conversationId, draft, Option.getOrUndefined(caller), (span) =>
            Effect.suspend(() => {
              options.resume();
              return kernel.commitWith(() =>
                Effect.gen(function* () {
                  const refused = refuse?.();
                  if (refused !== undefined) return yield* refused;
                  return yield* admitSubmission(
                    conversationId,
                    draft,
                    kernel.now(),
                    options.queueModes(),
                    span,
                  );
                }),
              );
            }),
          ),
        ).pipe(
          // Admission reads its tables before its first table write.
          Effect.catchTag("ReadAfterWrite", (error) => Effect.die(error)),
          Effect.map((admission) => handle(admission.id)),
        ),
      admit: (conversationId, draft, refuse) =>
        Effect.gen(function* () {
          const tx = yield* Tx;
          const identity = transactionIdentity(tx);
          // A wrapped or foreign transaction is a caller's bug.
          if (identity === undefined || !kernel.ownsTransaction(tx)) {
            return yield* Effect.die(new Error("The transaction belongs to another Session"));
          }
          const refused = refuse?.();
          if (refused !== undefined) return yield* refused;
          // Enforced on every path: admission reads tables only on some.
          if (identity.hasTableWrite()) {
            return yield* new ReadAfterWrite({ method: "Conversation.admit" });
          }
          if (readOnlyRefusal !== undefined) return yield* readOnlyRefusal;
          options.resume();
          // The span covers the admission only; its result is provisional until
          // the caller's commit is stored, and that commit's events go to the caller's span.
          const admission = yield* traced(conversationId, draft, identity.caller, (span) =>
            admitSubmission(conversationId, draft, kernel.now(), options.queueModes(), span),
          );
          return admission.id;
        }),
      get: (id) =>
        kernel
          .readOnLine(kernel.storage.submission(id))
          .pipe(Effect.map(Option.map((record) => handle(record.id)))),
      byRequest: (conversationId, requestId) =>
        kernel
          .readOnLine(kernel.storage.submissionByRequest(conversationId, requestId))
          .pipe(Effect.map(Option.map((record) => handle(record.id)))),
      withdraw,
    };
    return submissions;
  });

/**
 * Runs one public admission in its `durable.submit` span. An input or a
 * continue starts a run, so it starts a root trace linked to its caller; a write is its caller's child. The caller
 * of an admission inside a commit is the commit's caller, not the kernel's `Session.commit` span.
 */
const traced = <E, R>(
  conversationId: ConversationId,
  draft: SubmissionDraft,
  caller: Tracer.AnySpan | undefined,
  admit: (span: Tracer.Span) => Effect.Effect<Admission, E, R>,
): Effect.Effect<Admission, E, R> =>
  Effect.suspend(() => {
    const startsRun = draft.type !== "write";
    const links: ReadonlyArray<Tracer.SpanLink> =
      startsRun && caller !== undefined ? [{ span: caller, attributes: {} }] : [];
    return Effect.useSpan(
      Spans.submit,
      {
        ...(startsRun ? { root: true } : caller === undefined ? {} : { parent: caller }),
        links,
      },
      (span) => {
        // Telemetry never fails an admission; no reporter is at hand here, so a throw is dropped.
        telemetry.annotate(
          span,
          { "durable.conversation.id": conversationId, "durable.submission.type": draft.type },
          telemetry.drop,
        );
        telemetry.traceIds(span, telemetry.drop);
        return admit(span).pipe(
          Effect.tap((admission) =>
            Effect.sync(() =>
              telemetry.annotate(
                span,
                {
                  "durable.submission.id": admission.id,
                  "durable.submission.result": admission.status,
                },
                telemetry.drop,
              ),
            ),
          ),
          Effect.withParentSpan(span),
        );
      },
    );
  });

const notFound = (id: SubmissionId) =>
  new SubmissionError({
    reason: new SubmissionNotFound({ message: `Submission ${id} does not exist` }),
  });

const isSettled = (record: SubmissionRecord): record is SettledSubmission =>
  record.status === "done" || record.status === "unanswered";

/**
 * Admits a submission inside a commit. A known request
 * ID returns its existing submission without writing. A display-only write is appended and settled `done` at
 * once, busy or not, except behind a queued reset. A busy conversation queues anything else in `core.inbox`, or
 * rejects `whenBusy: "reject"` input with `ConversationBusy`. An idle conversation with queued items queues it
 * behind them and runs a final boundary. Otherwise idle input places its entry and starts a run, and an idle
 * write appends its entry and settles `done`, or `stale` when its head reaches before the active range.
 *
 * Load, decide, apply: each read is one `decideAdmission` asked for, in a fixed order (the request ID, the
 * inbox for a display-only write, `core.live`, and the boundary only when idle, so the table reads precede the
 * first table write); the last decision runs in the step that starts its writes.
 */
export const admitSubmission = (
  conversationId: ConversationId,
  draft: SubmissionDraft,
  now: number,
  modes: QueueModes,
  span?: Tracer.Span | undefined,
): Effect.Effect<Admission, AdmitError, Tx> =>
  Effect.gen(function* () {
    const tx = yield* Tx;
    const origin: Origin = { requestId: draft.requestId, notify: draft.notify };
    // The admission span's carrier is stored on the submission.
    const trace = span === undefined ? undefined : carrierOf(span);
    let input: AdmissionInput = { draft };
    let plan = decideAdmission(input);
    if (plan._tag === "Load" && plan.read === "request") {
      input = { ...input, existing: yield* tx.submissionByRequest(conversationId, plan.requestId) };
      plan = decideAdmission(input);
    }
    if (plan._tag === "Load" && plan.read === "inbox") {
      const items = (yield* tx.doc(InboxDoc, conversationId)).items;
      input = { ...input, queuedReset: hasQueuedReset(items) };
      plan = decideAdmission(input);
    }
    switch (plan._tag) {
      case "InvalidEntry":
        return yield* new SubmissionError({
          reason: new InvalidInputEntry({
            message: `An input entry's kind must be a non-empty string not starting with "core.", got ${plan.kind}`,
          }),
        });
      case "Mismatch":
        return yield* new SubmissionError({
          reason: new RequestIdMismatch({
            message: `Request ${plan.requestId} already identifies a submission of type ${plan.existingType}`,
          }),
        });
      case "Existing": {
        // An unanswered submission reports `done`.
        const status = Option.match(input.existing ?? Option.none(), {
          onNone: () => "done" as const,
          onSome: (existing) => (existing.status === "unanswered" ? "done" : existing.status),
        });
        return { id: plan.id, status };
      }
      case "DisplayOnly":
        return done(yield* appendWrite(conversationId, origin, trace, plan.entry));
    }
    // Every other decision needs `core.live`.
    const live = yield* tx.doc(LiveDoc, conversationId);
    // A steer links the running trace; its work stays there. A table read, before
    // any table write.
    if (
      span !== undefined &&
      draft.type === "input" &&
      draft.whenBusy === "steer" &&
      live.run !== undefined
    ) {
      const run = Option.getOrUndefined(yield* tx.task(live.run.taskId));
      if (run?.trace !== undefined) linkSpan(span, run.trace);
    }
    input = { ...input, busy: live.run !== undefined };
    plan = decideAdmission(input);
    // A boundary reads the table, so it is prepared before the first table write; a busy one needs none.
    const boundary =
      plan._tag === "Load" && plan.read === "boundary"
        ? yield* prepareBoundary(conversationId, modes)
        : undefined;
    if (boundary !== undefined) {
      input = { ...input, boundary: { items: boundary.inbox.items, head: boundary.head } };
      plan = decideAdmission(input);
    }
    switch (plan._tag) {
      case "Busy":
        return yield* new ConversationBusy({ conversationId });
      case "Idle":
        return yield* new ConversationIdle({ conversationId });
      case "Continue": {
        // `Continue` is decided only for an idle conversation, whose boundary was loaded.
        if (boundary === undefined)
          return yield* Effect.die(new Error("A continue has no boundary"));
        const prepared = boundary;
        const created = yield* tx.createSubmission(
          withFields(origin, trace, { conversationId, type: "continue", status: "placed" }),
        );
        // Items an earlier failed run left queued (`afterFailedRun: "keep"`)
        // are placed first and join the run. Its history starts with the failed run's.
        const placed = yield* applyBoundary(prepared, "final", now);
        yield* startRun(conversationId, live, {
          users: [...placed.users, created.id],
          first: live.runFailure?.first ?? placed.first,
        });
        return { id: created.id, status: "placed" };
      }
      case "Queue": {
        const created = yield* tx.createSubmission(
          withFields(origin, trace, {
            conversationId,
            type: plan.queued.mode === "write" ? "write" : "input",
            status: "queued",
          }),
        );
        const items = (boundary?.inbox ?? (yield* tx.doc(InboxDoc, conversationId))).items;
        if (plan.queued.mode === "write") {
          items.push({
            id: created.id,
            mode: "write",
            entry: yield* jsonObject(plan.queued.entry),
          });
        } else {
          const content = yield* jsonContent(plan.queued.content);
          const entry = plan.queued.entry;
          const item: InboxItem =
            entry === undefined
              ? { id: created.id, mode: plan.queued.mode, content }
              : {
                  id: created.id,
                  mode: plan.queued.mode,
                  content,
                  entry: yield* jsonInputEntry(entry),
                };
          // SAFETY: a fresh strict-JSON item; the draft takes it as its own value.
          items.push(item as Tracker.Draft<InboxItem>);
        }
        if (!plan.runFinal || boundary === undefined) return { id: created.id, status: "queued" };
        const placed = yield* applyBoundary(boundary, "final", now);
        if (placed.users.length > 0) yield* startRun(conversationId, live, placed);
        const status =
          plan.queued.mode === "write"
            ? ("done" as const)
            : placed.users.at(-1) === created.id
              ? ("placed" as const)
              : ("queued" as const);
        return { id: created.id, status };
      }
      case "Stale":
        return done(
          (yield* tx.createSubmission(
            withFields(origin, trace, {
              conversationId,
              type: "write",
              status: "unanswered",
              reason: "stale",
            }),
          )).id,
        );
      case "PlaceWrite":
        return done(yield* appendWrite(conversationId, origin, trace, plan.entry));
      case "PlaceInput": {
        const entry = yield* appendInput(conversationId, plan.content, plan.entry, now);
        const created = yield* tx.createSubmission(
          withFields(origin, trace, {
            conversationId,
            type: "input",
            status: "placed",
            entry: entry.id,
          }),
        );
        yield* startRun(conversationId, live, { users: [created.id], first: entry.id });
        return { id: created.id, status: "placed" };
      }
    }
    return yield* Effect.die(new Error(`Admission did not decide: ${plan._tag}`));
  });

/** Appends a write's entry and records it `done`. */
const appendWrite = (
  conversationId: ConversationId,
  origin: Origin,
  trace: TraceCarrier | undefined,
  draft: EntryDraft,
) =>
  Effect.gen(function* () {
    const tx = yield* Tx;
    const entry = yield* tx.appendEntry(conversationId, draft);
    const created = yield* tx.createSubmission(
      withFields(origin, trace, {
        conversationId,
        type: "write",
        status: "done",
        entry: entry.id,
      }),
    );
    return created.id;
  });

/**
 * What an admission did: the submission and its status as `durable.submit`'s `result` reports it,
 * `done` for one already settled.
 */
export interface Admission {
  readonly id: SubmissionId;
  readonly status: "queued" | "placed" | "done";
}

const done = (id: SubmissionId): Admission => ({ id, status: "done" });

/** Links `span` to a stored trace, and lists it in `durable.trace.links` for scalar-only backends. */
const linkSpan = (span: Tracer.Span, carrier: TraceCarrier) => {
  telemetry.link(span, [{ span: spanOf(carrier), attributes: {} }], telemetry.drop);
  telemetry.traceIds(span, telemetry.drop);
};

/** What admission has loaded so far, in read order; a field is `undefined` until it is loaded. */
export interface AdmissionInput {
  readonly draft: SubmissionDraft;
  /** The submission with the draft's request ID. */
  readonly existing?: Option.Option<SubmissionRecord> | undefined;
  /** Whether `core.inbox` holds a reset; read only for a display-only write. */
  readonly queuedReset?: boolean | undefined;
  /** Whether `core.live` has a run. */
  readonly busy?: boolean | undefined;
  /** An idle conversation's boundary: its queued items and the start of its active range. */
  readonly boundary?:
    | { readonly items: ReadonlyArray<InboxItem>; readonly head: EntryId | undefined }
    | undefined;
}

/** What a queued submission becomes in `core.inbox`. */
export type QueuedDraft =
  | { readonly mode: "write"; readonly entry: EntryDraft }
  | {
      readonly mode: "steer" | "followUp";
      readonly content: UserContent;
      readonly entry: InputEntry | undefined;
    };

/** What admission does next: load one more read, or the writes it decided. */
export type AdmissionPlan =
  | { readonly _tag: "Load"; readonly read: "request"; readonly requestId: string }
  | { readonly _tag: "Load"; readonly read: "inbox" | "live" | "boundary" }
  | { readonly _tag: "InvalidEntry"; readonly kind: string }
  | { readonly _tag: "Existing"; readonly id: SubmissionId }
  | { readonly _tag: "Mismatch"; readonly requestId: string; readonly existingType: string }
  | { readonly _tag: "DisplayOnly"; readonly entry: EntryDraft }
  | { readonly _tag: "Busy" }
  | { readonly _tag: "Idle" }
  | { readonly _tag: "Continue" }
  | { readonly _tag: "Queue"; readonly queued: QueuedDraft; readonly runFinal: boolean }
  | { readonly _tag: "Stale" }
  | { readonly _tag: "PlaceWrite"; readonly entry: EntryDraft }
  | {
      readonly _tag: "PlaceInput";
      readonly content: UserContent;
      readonly entry: InputEntry | undefined;
    };

/**
 * Admission's decisions: a pure function of what admission has loaded. It asks for the next read while the
 * decision depends on one, in a fixed order.
 */
export const decideAdmission = (input: AdmissionInput): AdmissionPlan => {
  const draft = input.draft;
  if (draft.type === "input" && draft.entry !== undefined) {
    // Guards an untyped caller, whose kind may not even be a string: the type
    // does not hold at runtime there, so the admission fails instead of dying on `startsWith`.
    const kind: unknown = draft.entry.kind;
    if (!Predicate.isString(kind) || kind.length === 0 || kind.startsWith("core.")) {
      return { _tag: "InvalidEntry", kind: String(kind) };
    }
  }
  if (draft.requestId !== undefined) {
    if (input.existing === undefined) {
      return { _tag: "Load", read: "request", requestId: draft.requestId };
    }
    if (Option.isSome(input.existing)) {
      return input.existing.value.type === draft.type
        ? { _tag: "Existing", id: input.existing.value.id }
        : { _tag: "Mismatch", requestId: draft.requestId, existingType: input.existing.value.type };
    }
  }
  // A display-only write changes nothing the model sees; only behind a queued
  // reset it queues, so it lands after the reset.
  if (draft.type === "write" && isDisplayOnly(draft.entry)) {
    if (input.queuedReset === undefined) return { _tag: "Load", read: "inbox" };
    if (!input.queuedReset) return { _tag: "DisplayOnly", entry: draft.entry };
  }
  if (input.busy === undefined) return { _tag: "Load", read: "live" };
  if (input.busy && draft.type === "input" && draft.whenBusy === "reject") return { _tag: "Busy" };
  // `whenIdle: "reject"` refuses an input that would start a run, before any write.
  if (!input.busy && draft.type === "input" && draft.whenIdle === "reject") return { _tag: "Idle" };
  if (draft.type === "continue") {
    // Never queued: whether a run can continue depends on how the current one
    // ends. Rejected before any write.
    if (input.busy) return { _tag: "Busy" };
    if (input.boundary === undefined) return { _tag: "Load", read: "boundary" };
    return { _tag: "Continue" };
  }
  const queued: QueuedDraft =
    draft.type === "write"
      ? { mode: "write", entry: draft.entry }
      : {
          mode: draft.whenBusy === "steer" ? "steer" : "followUp",
          content: draft.content,
          entry: draft.entry,
        };
  if (input.busy) return { _tag: "Queue", queued, runFinal: false };
  if (input.boundary === undefined) return { _tag: "Load", read: "boundary" };
  if (input.boundary.items.length > 0) return { _tag: "Queue", queued, runFinal: true };
  if (draft.type === "write") {
    return isStale(input.boundary.head, draft.entry)
      ? { _tag: "Stale" }
      : { _tag: "PlaceWrite", entry: draft.entry };
  }
  return { _tag: "PlaceInput", content: draft.content, entry: draft.entry };
};

/**
 * Hosts may leave optional fields `undefined`; document drafts take strict JSON, so
 * a queued value is copied first, and a value that is not JSON rejects the admission.
 */
const strictJson = (value: JsonInput) =>
  Effect.fromResult(
    Result.mapError(
      copyJson(value, { omitUndefinedProperties: true }),
      (message) => new CommitRejected({ reason: new InvalidJson({ message }) }),
    ),
  );

const jsonObject = (draft: EntryDraft) =>
  // SAFETY: a strict-JSON copy of an object is an object.
  Effect.map(strictJson(draft), (copied) => copied as Schema.JsonObject);

// Copies an input entry's kind and data as strict JSON.
const jsonInputEntry = (entry: InputEntry) =>
  // SAFETY: a strict-JSON copy of an input entry's kind and data has the same shape.
  Effect.map(strictJson(pickInputEntry(entry)), (copied) => copied as InputEntry);

const jsonContent = (content: UserContent) =>
  // SAFETY: a strict-JSON copy of user content has the same shape.
  Effect.map(strictJson(content), (copied) => copied as UserContent);

/** What a submission record keeps of its draft besides its content: its request ID and whom it notifies. */
interface Origin {
  readonly requestId: string | undefined;
  readonly notify: SubmissionNotify | undefined;
}

/** `create` with a new submission's optional fields: its request ID, whom it notifies, and its admission span's carrier. */
const withFields = <A extends object>(
  origin: Origin,
  trace: TraceCarrier | undefined,
  create: A,
): A => {
  const fields: A & { requestId?: string; trace?: TraceCarrier; notify?: SubmissionNotify } = {
    ...create,
  };
  if (origin.requestId !== undefined) fields.requestId = origin.requestId;
  if (origin.notify !== undefined) {
    fields.notify = { taskId: origin.notify.taskId, key: origin.notify.key };
  }
  if (trace !== undefined) fields.trace = trace;
  return fields;
};

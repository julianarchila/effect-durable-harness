/**
 * The records storage persists, as schemas. Storage
 * indexes only a few fields and treats every payload as opaque JSON (`data`,
 * task input and checkpoints, document values). An entry's `model` is typed
 * with `Message` and checked when the entry is appended.
 *
 * @since 0.0.0
 */
import * as Delta from "@effect-durable-harness/delta/Delta";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import { ConversationId, DocumentId, EntryId, Seq, SubmissionId, TaskId } from "./Ids.ts";
import { Message } from "./Message.ts";
import { TraceCarrier } from "./Telemetry.ts";

// Optional record fields use `Schema.optionalKey` (`x?: T`), not
// `Schema.optional` (`x?: T | undefined`): records are persisted as JSON, which
// has no `undefined`, so a present-but-undefined field would read back absent.

/**
 * One delta operation, validated with `@effect-durable-harness/delta`.
 *
 * @category documents
 * @since 0.0.0
 */
export const DeltaOperation = Schema.declare(
  (input): input is Delta.Operation => Result.isSuccess(Delta.validateOperation(input)),
  { identifier: "DeltaOperation" },
);

/**
 * An ordered batch of delta operations, stored as raw JSON.
 *
 * @category documents
 * @since 0.0.0
 */
export const DeltaOperations = Schema.Array(DeltaOperation);

// -----------------------------------------------------------------------------
// conversations and entries
// -----------------------------------------------------------------------------

/**
 * Immutable identity, history ancestry and task ownership of a transcript scope.
 *
 * @category conversations
 * @since 0.0.0
 */
export const ConversationRecord = Schema.Struct({
  id: ConversationId,
  /** Fork source and inclusive parent entry through which history is inherited. */
  parent: Schema.optionalKey(Schema.Struct({ conversationId: ConversationId, at: EntryId })),
  /** Creator edge used for attribution, subtree abort and subtree idle waits. */
  owner: Schema.optionalKey(Schema.Struct({ conversationId: ConversationId, taskId: TaskId })),
});
/**
 * The type of `ConversationRecord`.
 *
 * @category conversations
 * @since 0.0.0
 */
export type ConversationRecord = typeof ConversationRecord.Type;

/**
 * An immutable override of one visible entry's contribution to model context.
 *
 * @category entries
 * @since 0.0.0
 */
export const ContextEdit = Schema.Union([
  Schema.Struct({ target: EntryId, action: Schema.Literal("omit") }),
  Schema.Struct({
    target: EntryId,
    action: Schema.Literal("replace"),
    /** Checked when the entry is appended, as `model`. */
    messages: Schema.Array(Message),
  }),
]);
/**
 * The type of `ContextEdit`.
 *
 * @category entries
 * @since 0.0.0
 */
export type ContextEdit = typeof ContextEdit.Type;

/**
 * Immutable transcript event with separate model-facing and application-facing payloads.
 *
 * @category entries
 * @since 0.0.0
 */
export const EntryRecord = Schema.Struct({
  id: EntryId,
  conversationId: ConversationId,
  /** Application-defined entry discriminator. */
  kind: Schema.String,
  /**
   * Messages contributed to model context, in the `Message` shape. Checked
   * when an entry is appended; storage trusts its own writes.
   */
  model: Schema.optionalKey(Schema.Array(Message)),
  /** JSON payload consumed by views, extensions or bookkeeping logic. */
  data: Schema.optionalKey(Schema.Json),
  /** First entry in the active context selected by this entry. */
  head: Schema.optionalKey(EntryId),
  /** Context-only overrides of earlier visible entries. */
  edits: Schema.optionalKey(Schema.Array(ContextEdit)),
  /** Task that appended this entry. */
  byTaskId: Schema.optionalKey(TaskId),
});
/**
 * The type of `EntryRecord`.
 *
 * @category entries
 * @since 0.0.0
 */
export type EntryRecord = typeof EntryRecord.Type;

// -----------------------------------------------------------------------------
// submissions
// -----------------------------------------------------------------------------

/**
 * Where a settling submission delivers `SubmissionSettled`: a task, and the key it sleeps on.
 *
 * @category submissions
 * @since 0.0.0
 */
export const SubmissionNotify = Schema.Struct({ taskId: TaskId, key: Schema.String });
/**
 * The type of `SubmissionNotify`.
 *
 * @category submissions
 * @since 0.0.0
 */
export type SubmissionNotify = typeof SubmissionNotify.Type;

const submissionBase = {
  id: SubmissionId,
  conversationId: ConversationId,
  /** Host-provided deduplication key, scoped to the conversation. */
  requestId: Schema.optionalKey(Schema.String),
  /** The admission span's carrier; absent for an internal admission. */
  trace: Schema.optionalKey(TraceCarrier),
  /** The task that receives `SubmissionSettled` under `key` when the submission settles. */
  notify: Schema.optionalKey(SubmissionNotify),
} as const;

const queued = Schema.Literal("queued");
const placed = Schema.Literal("placed");
const done = Schema.Literal("done");
const unanswered = Schema.Literal("unanswered");
const input = Schema.Literal("input");
const write = Schema.Literal("write");
const continued = Schema.Literal("continue");

const settledMembers = [
  Schema.Struct({ ...submissionBase, type: input, status: done, entry: EntryId, answer: EntryId }),
  Schema.Struct({
    ...submissionBase,
    type: input,
    status: unanswered,
    entry: Schema.optionalKey(EntryId),
    reason: Schema.String,
    detail: Schema.optionalKey(Schema.Json),
  }),
  Schema.Struct({ ...submissionBase, type: write, status: done, entry: EntryId }),
  Schema.Struct({
    ...submissionBase,
    type: write,
    status: unanswered,
    reason: Schema.String,
    detail: Schema.optionalKey(Schema.Json),
  }),
  Schema.Struct({ ...submissionBase, type: continued, status: done, answer: EntryId }),
  Schema.Struct({
    ...submissionBase,
    type: continued,
    status: unanswered,
    reason: Schema.String,
    detail: Schema.optionalKey(Schema.Json),
  }),
] as const;

/**
 * Durable lifecycle of one admitted user input, passive entry write, or continue.
 *
 * @category submissions
 * @since 0.0.0
 */
export const SubmissionRecord = Schema.Union([
  Schema.Struct({ ...submissionBase, type: input, status: queued }),
  Schema.Struct({ ...submissionBase, type: input, status: placed, entry: EntryId }),
  Schema.Struct({ ...submissionBase, type: write, status: queued }),
  // A run started without a user message: it places no entry and is never queued.
  Schema.Struct({ ...submissionBase, type: continued, status: placed }),
  ...settledMembers,
]);
/**
 * The type of `SubmissionRecord`.
 *
 * @category submissions
 * @since 0.0.0
 */
export type SubmissionRecord = typeof SubmissionRecord.Type;

/**
 * The lifecycle status of a submission, an indexed column of storage.
 *
 * @category submissions
 * @since 0.0.0
 */
export type SubmissionStatus = SubmissionRecord["status"];

/**
 * A settled submission: `done` or `unanswered`, what a wait for it resolves to.
 *
 * @category submissions
 * @since 0.0.0
 */
export const SettledSubmissionRecord = Schema.Union(settledMembers);
/**
 * The type of `SettledSubmissionRecord`.
 *
 * @category submissions
 * @since 0.0.0
 */
export type SettledSubmissionRecord = typeof SettledSubmissionRecord.Type;

// -----------------------------------------------------------------------------
// tasks
// -----------------------------------------------------------------------------

/**
 * JSON-safe error snapshot persisted instead of a runtime error.
 *
 * @category tasks
 * @since 0.0.0
 */
export const TaskOutcomeError = Schema.Struct({
  message: Schema.String,
  detail: Schema.optionalKey(Schema.Json),
});
/**
 * The type of `TaskOutcomeError`.
 *
 * @category tasks
 * @since 0.0.0
 */
export type TaskOutcomeError = typeof TaskOutcomeError.Type;

/**
 * Durable reason and optional result recorded when a task becomes terminal.
 *
 * @category tasks
 * @since 0.0.0
 */
export const TaskOutcome = Schema.Union([
  Schema.Struct({ status: Schema.Literal("completed"), result: Schema.Json }),
  Schema.Struct({
    status: Schema.Literal("failed"),
    error: TaskOutcomeError,
    result: Schema.optionalKey(Schema.Json),
  }),
  Schema.Struct({
    status: Schema.Literal("aborted"),
    reason: Schema.optionalKey(Schema.String),
    result: Schema.optionalKey(Schema.Json),
  }),
  Schema.Struct({ status: Schema.Literal("orphaned"), reason: Schema.String }),
  Schema.Struct({ status: Schema.Literal("faulted"), error: TaskOutcomeError }),
]);
/**
 * The type of `TaskOutcome`.
 *
 * @category tasks
 * @since 0.0.0
 */
export type TaskOutcome = typeof TaskOutcome.Type;

/**
 * How a waiting task joins the tasks it waits on: `allSettled` resumes once every task in `on` is terminal,
 * `failFast` too but aborts the others once one fails, `any` once at least one is terminal.
 *
 * @category tasks
 * @since 0.0.0
 */
export const JoinPolicy = Schema.Literals(["failFast", "allSettled", "any"]);
/**
 * The type of `JoinPolicy`.
 *
 * @category tasks
 * @since 0.0.0
 */
export type JoinPolicy = typeof JoinPolicy.Type;

/**
 * The key of one wait for a signal, as sleeps and parks store it: the signal's name and the key.
 *
 * @category tasks
 * @since 0.0.0
 */
export const SignalKeyRecord = Schema.Struct({ signal: Schema.String, key: Schema.String });
/**
 * The type of `SignalKeyRecord`.
 *
 * @category tasks
 * @since 0.0.0
 */
export type SignalKeyRecord = typeof SignalKeyRecord.Type;

/**
 * Why parked work runs again, as the reservation that woke it recorded: `time` (the sleep's `until`, no
 * signals listed), `expired` (`until` came before any listed signal), `signal` (the key whose value woke it;
 * read the value with `readSignal`), `tasks` (the terminal tasks of the wait's `on` at the reservation). Kept
 * until the task commits another checkpoint, parks, or ends.
 *
 * @category tasks
 * @since 0.0.0
 */
export const WakeReason = Schema.Union([
  Schema.Struct({ reason: Schema.Literal("time") }),
  Schema.Struct({ reason: Schema.Literal("expired") }),
  Schema.Struct({ reason: Schema.Literal("signal"), signal: Schema.String, key: Schema.String }),
  Schema.Struct({ reason: Schema.Literal("tasks"), ended: Schema.Array(TaskId) }),
]);
/**
 * The type of `WakeReason`.
 *
 * @category tasks
 * @since 0.0.0
 */
export type WakeReason = typeof WakeReason.Type;

/**
 * States from which a task can still run: they carry a checkpoint.
 *
 * @category tasks
 * @since 0.0.0
 */
export const LiveTaskState = Schema.Union([
  /** `wake` says why the task woke from its last wait or sleep, kept while it runs at that checkpoint. */
  Schema.Struct({
    status: Schema.Literal("pending"),
    checkpoint: Schema.Json,
    wake: Schema.optionalKey(WakeReason),
  }),
  Schema.Struct({
    status: Schema.Literal("running"),
    checkpoint: Schema.Json,
    wake: Schema.optionalKey(WakeReason),
  }),
  Schema.Struct({
    status: Schema.Literal("waiting"),
    checkpoint: Schema.Json,
    on: Schema.Array(TaskId),
    policy: JoinPolicy,
  }),
  /**
   * Parked without an invocation until the Harness clock reaches `until`, or one of `signals` holds a value no
   * wake used, whichever comes first; then resumes at `checkpoint`. Needs `until` or a non-empty `signals`.
   */
  Schema.Struct({
    status: Schema.Literal("sleeping"),
    checkpoint: Schema.Json,
    until: Schema.optionalKey(Schema.Number),
    signals: Schema.optionalKey(Schema.Array(SignalKeyRecord)),
  }),
]);

/**
 * States whose outcome is decided: they carry the outcome and run no more code.
 *
 * @category tasks
 * @since 0.0.0
 */
export const SettledTaskState = Schema.Union([
  Schema.Struct({ status: Schema.Literal("completing"), outcome: TaskOutcome }),
  Schema.Struct({ status: Schema.Literal("terminal"), outcome: TaskOutcome }),
]);

/**
 * Complete durable execution state of a task.
 *
 * @category tasks
 * @since 0.0.0
 */
export const TaskState = Schema.Union([LiveTaskState, SettledTaskState]);
/**
 * The type of `TaskState`.
 *
 * @category tasks
 * @since 0.0.0
 */
export type TaskState = typeof TaskState.Type;

/**
 * The execution status of a task, an indexed column of storage.
 *
 * @category tasks
 * @since 0.0.0
 */
export type TaskStatus = TaskState["status"];

const taskBase = {
  id: TaskId,
  conversationId: ConversationId,
  /** Registered task definition name. */
  kind: Schema.String,
  /** Definition version used to migrate live input and checkpoints. */
  version: Schema.Number,
  /** Original task input. */
  input: Schema.Json,
  /** Owning task of a child task; absent for a task its conversation owns. */
  owner: Schema.optionalKey(TaskId),
  background: Schema.Boolean,
  abortRequested: Schema.Boolean,
  abortReason: Schema.optionalKey(Schema.Literal("restart")),
  abandonOnRestart: Schema.optionalKey(Schema.Literal(true)),
  /** Wall-clock milliseconds of the first change to `running`. */
  startedAt: Schema.optionalKey(Schema.Number),
  /** Wall-clock milliseconds of the change to `terminal`. */
  endedAt: Schema.optionalKey(Schema.Number),
  /**
   * Times in a row a crash, eviction or reset cut the task off while it ran, without the task changing its
   * state since; open counts them, and a close does not.
   */
  restarts: Schema.optionalKey(Schema.Number),
  /** The carrier of the span that created the task, its phases' parent; absent for a root trace. */
  trace: Schema.optionalKey(TraceCarrier),
  /** Creation-time links, attached to the phase spans of the task's first invocation. */
  links: Schema.optionalKey(Schema.Array(TraceCarrier)),
} as const;

/**
 * A task record that can still run; it may carry memos.
 *
 * @category tasks
 * @since 0.0.0
 */
export const LiveTaskRecord = Schema.Struct({
  ...taskBase,
  state: LiveTaskState,
  /** Small first-writer-wins values retained while the task can run. */
  memos: Schema.optionalKey(Schema.Record(Schema.String, Schema.Json)),
});
/**
 * The type of `LiveTaskRecord`.
 *
 * @category tasks
 * @since 0.0.0
 */
export type LiveTaskRecord = typeof LiveTaskRecord.Type;

/**
 * A task record whose outcome is decided; memos are dropped.
 *
 * @category tasks
 * @since 0.0.0
 */
export const SettledTaskRecord = Schema.Struct({ ...taskBase, state: SettledTaskState });
/**
 * The type of `SettledTaskRecord`.
 *
 * @category tasks
 * @since 0.0.0
 */
export type SettledTaskRecord = typeof SettledTaskRecord.Type;

/**
 * Complete replacement record for one durable task state machine.
 *
 * @category tasks
 * @since 0.0.0
 */
export const TaskRecord = Schema.Union([LiveTaskRecord, SettledTaskRecord]);
/**
 * The type of `TaskRecord`.
 *
 * @category tasks
 * @since 0.0.0
 */
export type TaskRecord = typeof TaskRecord.Type;

// -----------------------------------------------------------------------------
// documents
// -----------------------------------------------------------------------------

/**
 * Ownership scope of a document.
 *
 * @category documents
 * @since 0.0.0
 */
export const DocumentScope = Schema.Union([
  Schema.Struct({ kind: Schema.Literal("session") }),
  Schema.Struct({ kind: Schema.Literal("conversation"), conversationId: ConversationId }),
  Schema.Struct({ kind: Schema.Literal("task"), taskId: TaskId }),
]);
/**
 * The type of `DocumentScope`.
 *
 * @category documents
 * @since 0.0.0
 */
export type DocumentScope = typeof DocumentScope.Type;

const documentVariants = <const Fields extends Schema.Struct.Fields>(fields: Fields) => {
  const identity = {
    /** Unique incarnation ID; never reused when the same logical document is recreated. */
    id: DocumentId,
    /** Stable document definition kind. */
    kind: Schema.String,
    /** Family member key; absent for singleton documents. */
    key: Schema.optionalKey(Schema.String),
    ...fields,
  };
  return Schema.Union([
    Schema.Struct({ ...identity, scope: Schema.Struct({ kind: Schema.Literal("session") }) }),
    Schema.Struct({
      ...identity,
      scope: Schema.Struct({
        kind: Schema.Literal("conversation"),
        conversationId: ConversationId,
      }),
      /** Retain only current state. */
      history: Schema.Literal("latest"),
      fork: Schema.Literals(["current", "initial"]),
    }),
    Schema.Struct({
      ...identity,
      scope: Schema.Struct({
        kind: Schema.Literal("conversation"),
        conversationId: ConversationId,
      }),
      /** Retain the history needed for as-of reads. */
      history: Schema.Literal("rewindable"),
      fork: Schema.Literals(["asOf", "current", "initial"]),
    }),
    Schema.Struct({
      ...identity,
      scope: Schema.Struct({ kind: Schema.Literal("task"), taskId: TaskId }),
    }),
  ]);
};

/**
 * Fields supplied when storage creates and stamps a new `DocumentRecord`.
 *
 * @category documents
 * @since 0.0.0
 */
export const DocumentCreate = documentVariants({});
/**
 * The type of `DocumentCreate`.
 *
 * @category documents
 * @since 0.0.0
 */
export type DocumentCreate = typeof DocumentCreate.Type;

/**
 * Persisted lifecycle record of one create-to-retire document incarnation.
 *
 * @category documents
 * @since 0.0.0
 */
export const DocumentRecord = documentVariants({
  /** Commit that created the incarnation, stamped by storage. */
  createdAt: Seq,
  /** Commit that retired the incarnation; absent while it is current. */
  retiredAt: Schema.optionalKey(Seq),
});
/**
 * The type of `DocumentRecord`.
 *
 * @category documents
 * @since 0.0.0
 */
export type DocumentRecord = typeof DocumentRecord.Type;

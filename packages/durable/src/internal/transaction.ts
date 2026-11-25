// The transaction of one Session commit callback.
//
// Every `Tx` operation is tracked, so settlement can reject the commit and
// drain work the callback left running. The Session calls one settlement, then
// either discards the prepared changes or adopts them once after storage
// succeeds.
import type * as Delta from "@effect-durable-harness/delta/Delta";
import * as PendingOps from "@effect-durable-harness/delta/PendingOps";
import * as Tracker from "@effect-durable-harness/delta/Tracker";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import type * as Exit from "effect/Exit";
import * as Option from "effect/Option";
import * as Predicate from "effect/Predicate";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import type * as Tracer from "effect/Tracer";
import type {
  AgentStorage,
  Cursor,
  DocumentAddress,
  DocumentContent,
  DocumentCopySource,
  HeadMarker,
  StorageFailure,
  StorageRequestError,
  StorageWrite,
} from "../AgentStorage.ts";
import type { EntryDraft, EntryKind, TypedEntry, TypedEntryDraft } from "../Entry.ts";
import type {
  ConversationDoc,
  ConversationDocFamily,
  Doc,
  SessionDoc,
  SessionDocFamily,
  TaskDoc,
  TaskDocFamily,
} from "../Doc.ts";
import {
  ConversationId,
  DocumentId,
  EntryId,
  ROOT_CONVERSATION_ID,
  type Seq,
  SubmissionId,
  TaskId,
} from "../Ids.ts";
import type {
  ConversationRecord,
  DocumentCreate,
  DocumentRecord,
  EntryRecord,
  SettledSubmissionRecord,
  SubmissionNotify,
  SubmissionRecord,
  TaskRecord,
} from "../Records.ts";
import { type Signal, SubmissionSettled } from "../Signal.ts";
import type { DocumentCommitChange } from "../Session.ts";
import {
  CommitRejected,
  type CommitRejectedReason,
  type DocumentAccessError,
  ForkConflict,
  InvalidJson,
  InvalidOwner,
  NotFound,
  ReadAfterWrite,
  type SessionFailed,
  Settled,
  SubmissionConflict,
  TaskConflict,
} from "../SessionError.ts";
import type { Task, TaskOptions } from "../Task.ts";
import { carrierOf, type TraceCarrier } from "../Telemetry.ts";
import { Message } from "../Message.ts";
import {
  type ConversationOwnership,
  type SubmissionCreate,
  type SubmissionSettlement,
  Tx,
  type TxDocError,
  type TxReadError,
  type TxWriteError,
} from "../Tx.ts";
import {
  type AnyDefinition,
  checkRecord,
  checkRecordScope,
  copyJson,
  definitionOf,
  type JsonInput,
  type DocArgument,
  documentCreate,
  materializeValue,
  type ResolvedAddress,
  resolveAddress,
} from "./documents.ts";
import { addressKey } from "./commit.ts";
import { documentKey, incarnationKey, scopeKey, tableKeys, type Touch } from "./keys.ts";
import { prepareForkDocumentCopies } from "./forks.ts";
import { doneAfterYield } from "./signal.ts";
import * as SignalMemos from "./signalMemos.ts";

const INTERNAL_SCAN_PAGE_SIZE = 256;
const EMPTY_OPERATIONS: Delta.Delta = [];

export type StorageError = StorageFailure | StorageRequestError | SessionFailed;

/**
 * Storage behind the Session's failure guard: every error of a call may also be `SessionFailed`. It has no
 * `commit`: only the Session stores, and it settles a commit's outcome itself (`session.ts`, `store`).
 */
export type GuardedStorage = {
  readonly [K in Exclude<
    keyof AgentStorage["Service"],
    "commit"
  >]: AgentStorage["Service"][K] extends (...args: infer Args) => Effect.Effect<infer A, infer E>
    ? (...args: Args) => Effect.Effect<A, E | SessionFailed>
    : AgentStorage["Service"][K] extends Effect.Effect<infer A, infer E>
      ? Effect.Effect<A, E | SessionFailed>
      : never;
};

/** One committed document incarnation owned by the Session's tracker cache. */
export interface LoadedDocument {
  readonly addressId: string;
  readonly record: DocumentRecord;
  /** Persisted definition version; older while the tracked value is migrated only in memory. */
  storedVersion: number;
  /** Definition version whose shape the tracked value has; access with another version reloads. */
  readonly valueVersion: number;
  /** Stored deltas after the newest base; advanced by adoption so the next predicate needs no read. */
  deltasSinceBase: number;
  readonly tracker: Tracker.Tracker<Schema.JsonObject>;
}

/**
 * The changes of a cached incarnation that deferred commits
 * adopted in memory and the Session has not stored yet, folded. The Session owns it; a commit that stores them
 * takes them.
 */
export interface HeldDocument {
  readonly loaded: LoadedDocument;
  readonly pending: PendingOps.PendingOps;
  /** Set when a held commit selected a base (`checkpointWhen`): their store writes the whole value. */
  base: boolean;
}

/** One incarnation a deferred commit holds (`holdable`): its operations, and whether they selected a base. */
export interface HeldChange {
  readonly loaded: LoadedDocument;
  readonly ops: Delta.Delta;
  readonly base: boolean;
}

/** What adoption publishes. */
export interface AdoptedDocuments {
  /** This commit's own changes, as memory adopted them: what the Session's own observers get. */
  readonly observed: ReadonlyArray<DocumentCommitChange>;
  /** What storage got: `observed`, unless the commit also stored held changes, which precede its own. */
  readonly stored: ReadonlyArray<DocumentCommitChange>;
}

/** Session services a transaction uses while it holds the mutation line. */
export interface TransactionHost {
  readonly storage: GuardedStorage;
  /** Wall clock for task lifecycle times. */
  readonly now: () => number;
  /** Waits while `key` has an unpublished writer (group commit); `undefined` when there is none. */
  readonly touch: Touch;
  /** The cached current incarnation, without loading. */
  readonly cached: (addressId: string) => LoadedDocument | undefined;
  /** The cached current incarnation, cold-loading and migrating it when necessary. */
  readonly load: (
    definition: AnyDefinition,
    addressId: string,
    address: DocumentAddress,
  ) => Effect.Effect<Option.Option<LoadedDocument>, DocumentAccessError | StorageError>;
  /** Installs a newly committed incarnation. */
  readonly install: (document: LoadedDocument) => void;
  /** Removes a retired incarnation if it is still the cached occupant of its address. */
  readonly evict: (addressId: string, recordId: DocumentId) => void;
  /**
   * The current documents of a task the Session created and indexed since; `undefined` for any other task, whose documents are scanned.
   */
  readonly taskDocuments: (taskId: TaskId) => Iterable<DocumentRecord> | undefined;
  /** The newest visible head marker of a conversation, from the Session's tails. */
  readonly latestHeadMarker: (
    conversationId: ConversationId,
  ) => Effect.Effect<HeadMarker | undefined, StorageError>;
  /** Stages writes that belong to every new or forked conversation, in its creating transaction. */
  readonly conversationCreated: (
    tx: Transaction,
    record: ConversationRecord,
  ) => Effect.Effect<void, TxDocError, Tx>;
}

/** Defaults a commit binds to: the `createTask` conversation and the task stamped on appended entries. */
export interface TransactionScope {
  readonly conversationId?: ConversationId | undefined;
  /** Task whose runtime commit this is; stamped as `byTaskId` on appended entries. */
  readonly taskId?: TaskId | undefined;
}

/** The transaction: the public `Tx` service plus what the Session and the Harness use. */
export interface Transaction {
  readonly service: Tx["Service"];
  /** Replaces one task record completely; tasks change their own state through their runtime. */
  readonly setTask: (record: TaskRecord) => Effect.Effect<void, CommitRejected>;
  /** Candidate records of the tasks this transaction created or replaced so far. */
  readonly stagedTasks: () => ReadonlyArray<TaskRecord>;
  /**
   * The keys a settled commit writes: its table records, and per document
   * it changes or retires, its address and incarnation, plus its scope when it creates or retires one.
   */
  readonly writeKeys: () => Array<string>;
  /** Tasks this transaction created. */
  readonly createdTaskIds: () => ReadonlyArray<TaskId>;
  /** Conversations this transaction created or forked so far. */
  readonly stagedConversations: () => ReadonlyArray<ConversationRecord>;
  /** Stages the root conversation at its reserved ID. */
  readonly createRootConversation: Effect.Effect<ConversationRecord, TxDocError>;
  /** Seals after the callback failed: aborts every change and drains pending operations. */
  readonly settleFailure: Effect.Effect<void>;
  /** Seals after the callback succeeded, prepares every change and assembles the atomic batch. */
  readonly settleSuccess: Effect.Effect<ReadonlyArray<StorageWrite>, CommitRejected | StorageError>;
  /** Aborts every prepared change after a storage failure or when nothing is written. */
  readonly discard: () => void;
  /** Adopts every prepared change by pointer swap after storage succeeded and describes the publication. */
  readonly adopt: (seq: Seq) => AdoptedDocuments;
  /**
   * What a deferred commit holds in memory instead of storing,
   * each changed incarnation with its operations; `undefined` when the commit must be stored. It may be held only
   * when every write changes an existing `latest`, `initial`-fork conversation document at its stored version.
   */
  readonly holdable: () => ReadonlyArray<HeldChange> | undefined;
  /**
   * Adopts a held commit in memory only. Trackers advance by
   * pointer swap; stored versions and delta counts stay, as nothing was stored. Returns its publication.
   */
  readonly adoptHeld: () => ReadonlyArray<DocumentCommitChange>;
  /**
   * Stores the held changes of every incarnation this commit reads
   * or writes in front of its own, so a stored document never misses an operation its later ones build on.
   * Called right before admission, after every fallible step, because it empties the holds. Returns the holds
   * it took; the batch `settleSuccess` returned now carries them.
   */
  readonly takeHeld: <H extends HeldDocument>(
    held: (id: DocumentId) => H | undefined,
  ) => ReadonlyArray<H>;
}

interface TransactionTask {
  committedRead?: Deferred.Deferred<Option.Option<TaskRecord>, StorageError> | undefined;
  write?: { readonly kind: "create" | "replace"; readonly record: TaskRecord };
  publicationConversationId?: ConversationId;
}

/** Storage and cache provenance of one staged incarnation. */
type DocumentTarget =
  | { readonly kind: "loaded"; readonly document: LoadedDocument }
  | {
      readonly kind: "created";
      readonly record: DocumentCreate;
      readonly version: number;
      readonly tracker: Tracker.Tracker<Schema.JsonObject>;
    }
  | {
      readonly kind: "fork-copy";
      readonly record: DocumentCreate;
      readonly source: DocumentCopySource;
    }
  | { readonly kind: "retire-only"; readonly record: DocumentRecord };

/** One incarnation acquired, created or retired by this transaction. */
interface DocumentEntry {
  readonly addressId: string;
  readonly address: DocumentAddress;
  /** Absent for definition-free fork copies and retirements found by a terminal-task scan. */
  definition?: AnyDefinition | undefined;
  /** Memoized acquisition; absent for a retirement without acquisition. */
  draft?: Deferred.Deferred<Tracker.Draft<Schema.JsonObject>, TxDocError> | undefined;
  target?: DocumentTarget | undefined;
  change?: Tracker.Change<Schema.JsonObject> | undefined;
  prepared?: Tracker.Prepared<Schema.JsonObject> | undefined;
  retireOnCommit: boolean;
}

/** What one staged incarnation writes and publishes, decided before storage admission. */
interface DocumentPlan {
  readonly addressId: string;
  /** A `DocumentRecord` for a committed incarnation, a `DocumentCreate` for a new one. */
  readonly record: DocumentCreate | DocumentRecord;
  retire: boolean;
  content?:
    | Extract<
        StorageWrite,
        { readonly type: "document.create" | "document.copy" | "document.change" }
      >
    | undefined;
  readonly change?:
    | {
        readonly tracker: Tracker.Tracker<Schema.JsonObject>;
        readonly prepared: Tracker.Prepared<Schema.JsonObject>;
        readonly version: number;
        /** The cached incarnation this change updates; absent when it creates one. */
        readonly loaded?: LoadedDocument | undefined;
        readonly definition?: AnyDefinition | undefined;
      }
    | undefined;
  /** Resolved before admission, so adoption performs no reads. */
  conversationId?: ConversationId | undefined;
  /**
   * Set when this commit also stores held changes of the incarnation (`takeHeld`): the operations its stored
   * change publishes, held first, and whether it has a change of its own to show the Session's observers.
   */
  held?: { readonly ops: Delta.Delta; readonly observed: boolean } | undefined;
}

const reject = (reason: CommitRejectedReason) => new CommitRejected({ reason });
const settled = () => reject(new Settled({ message: "Transaction has settled" }));
const notFound = (message: string) => reject(new NotFound({ message }));

/** Copies caller JSON into kernel ownership; `undefined` object properties are omitted. */
const isModel = Schema.is(Schema.Array(Message));

/** A staged submission change: a terminal settlement, or a placement at an entry. */
type SubmissionChange =
  | SubmissionSettlement
  | { readonly status: "placed"; readonly entry: EntryId };

/**
 * Applies one change to a submission record. A settled record stays as it is;
 * only a queued submission is placed, and only a placed input or continue is answered.
 */
const applySubmissionChange = (
  current: SubmissionRecord,
  change: SubmissionChange,
):
  | { readonly _tag: "Changed"; readonly record: SubmissionRecord }
  | { readonly _tag: "Conflict"; readonly message: string } => {
  if (current.status === "done" || current.status === "unanswered") {
    return { _tag: "Changed", record: current };
  }
  if (change.status === "placed") {
    if (current.status !== "queued") {
      return { _tag: "Conflict", message: `Submission ${current.id} is not queued` };
    }
    return {
      _tag: "Changed",
      record:
        current.type === "input"
          ? { ...current, status: "placed", entry: change.entry }
          : { ...current, status: "done", entry: change.entry },
    };
  }
  if (change.status === "done") {
    if (current.status !== "placed") {
      return {
        _tag: "Conflict",
        message: `Submission ${current.id} is not a placed input or continue`,
      };
    }
    return { _tag: "Changed", record: { ...current, status: "done", answer: change.answer } };
  }
  return {
    _tag: "Changed",
    // SAFETY: queued and placed records carry no answer, reason or detail, so the merge is the unanswered
    // record of the same type, and an input keeps its entry; `change` is a
    // strict-JSON copy, so an absent detail is absent.
    record: { ...current, ...change } as SubmissionRecord,
  };
};

const copyTableJson = (value: JsonInput) =>
  Effect.fromResult(
    Result.mapError(copyJson(value, { omitUndefinedProperties: true }), (message) =>
      reject(new InvalidJson({ message })),
    ),
  );

/** What `Conversation.admit` checks of a `Tx`: the Session it belongs to, and whether it wrote a table. */
interface TransactionIdentity {
  readonly host: TransactionHost;
  readonly hasTableWrite: () => boolean;
  /** The span of the commit's caller, which an admission inside the commit links or parents. */
  readonly caller: Tracer.AnySpan | undefined;
  /**
   * The stored carrier of a submission, staged in this commit or stored, for the
   * run its inputs start. Not a table read: it never refuses after a write.
   */
  readonly submissionTrace: (
    id: SubmissionId,
  ) => Effect.Effect<
    TraceCarrier | undefined,
    StorageFailure | StorageRequestError | SessionFailed
  >;
}

const identities = new WeakMap<Tx["Service"], TransactionIdentity>();

/**
 * The identity
 * of a `Tx` service this module built, or `undefined` for any other value, such as a wrapped or hand-built one.
 */
export const transactionIdentity = (service: Tx["Service"]): TransactionIdentity | undefined =>
  identities.get(service);

/** Builds the transaction of one commit callback. */
export const openTransaction = (
  host: TransactionHost,
  scope: TransactionScope = {},
  /** The span of the commit's caller: a task created without `trace` continues it. */
  caller?: Tracer.AnySpan | undefined,
): Transaction => {
  const storage = host.storage;
  let sealed = false;
  let hasTableWrite = false;
  let pending = 0;
  let drained: Deferred.Deferred<void> | undefined;

  /** Atomic batch: conversation and entry writes stage eagerly; task and document writes assemble later. */
  const writes: Array<StorageWrite> = [];
  const createdConversationIds = new Set<ConversationId>();
  const forkSourceConversationIds = new Set<ConversationId>();
  const forkSourceDocumentIds = new Set<DocumentId>();
  const tasksById = new Map<TaskId, TransactionTask>();
  const plans: Array<DocumentPlan> = [];
  const documents: Array<DocumentEntry> = [];
  const latestDocumentByAddress = new Map<string, DocumentEntry>();
  /** Submissions this transaction created or changed, resolved during assembly. */
  const submissions = new Map<SubmissionId, SubmissionRecord>();
  const submissionChanges: Array<{ readonly id: SubmissionId; readonly change: SubmissionChange }> =
    [];

  // ─── Tracking ─────────────────────────────────────────────────────────────

  /**
   * Registers an operation, so settlement can reject the commit and drain it.
   * Callers call it synchronously, in the same step as their `sealed` check:
   * an operation is either tracked before settlement seals the transaction,
   * or it sees the seal. "Uninterruptible" is not enough on its own, because
   * a fiber can still yield to the scheduler between two steps.
   *
   * An operation runs uninterruptibly from its first step,
   * bookkeeping included (`read`, `write`, `docOf` and `retireDocOf`),
   * so interrupting the fiber that runs it takes effect when it finishes and
   * never leaves an acquisition, a memo or a retirement half done. Only
   * `docOf`'s wait for another fiber's acquisition of the same address is
   * neither tracked nor uninterruptible: it changes nothing. An
   * operation forked without `startImmediately` that has not started when the
   * callback returns is not seen: the commit settles without it and the
   * operation fails with `Settled` when it runs.
   */
  const track = <A, E, R>(
    operation: Effect.Effect<A, E, R>,
    // Runs in the same synchronous step, after the operation stops counting as pending: whoever it
    // wakes can never observe the operation as still pending, whatever the scheduler does.
    onSettled?: (exit: Exit.Exit<A, E>) => void,
  ): Effect.Effect<A, E, R> => {
    pending++;
    return operation.pipe(
      Effect.onExit((exit) =>
        Effect.sync(() => {
          pending--;
          if (pending === 0 && drained !== undefined) doneAfterYield(drained, Effect.void);
          onSettled?.(exit);
        }),
      ),
    );
  };

  const drain = Effect.suspend(() => {
    if (pending === 0) return Effect.void;
    const wait = Deferred.makeUnsafe<void>();
    drained = wait;
    return Deferred.await(wait);
  });

  const read = <A, E>(method: string, operation: Effect.Effect<A, E>) =>
    uninterruptibleSuspend((): Effect.Effect<A, E | CommitRejected | ReadAfterWrite> => {
      if (sealed) return Effect.fail(settled());
      // Any table read after the first table write.
      if (hasTableWrite) return Effect.fail(new ReadAfterWrite({ method }));
      return track(operation);
    });

  const write = <A, E, R>(operation: Effect.Effect<A, E, R>) =>
    uninterruptibleSuspend((): Effect.Effect<A, E | CommitRejected, R> => {
      if (sealed) return Effect.fail(settled());
      hasTableWrite = true;
      return track(operation);
    });

  const requireConversation = (id: ConversationId) =>
    Effect.gen(function* () {
      if (createdConversationIds.has(id)) return;
      if (Option.isNone(yield* storage.conversation(id))) {
        return yield* notFound(`Conversation ${id} does not exist`);
      }
    });

  const taskEntry = (id: TaskId): TransactionTask => {
    const existing = tasksById.get(id);
    if (existing !== undefined) return existing;
    const created: TransactionTask = {};
    tasksById.set(id, created);
    return created;
  };

  /** Committed task, read once per transaction; always inside a tracked operation or settlement. */
  const committedTask = (id: TaskId) =>
    Effect.suspend(() => {
      const task = taskEntry(id);
      if (task.committedRead !== undefined) return Deferred.await(task.committedRead);
      const deferred = Deferred.makeUnsafe<Option.Option<TaskRecord>, StorageError>();
      task.committedRead = deferred;
      return storage.task(id).pipe(Effect.onExit((exit) => publish(deferred, exit)));
    });

  /** Latest candidate task record, falling back to committed state; not a caller table read. */
  const currentTask = (id: TaskId) =>
    Effect.suspend(() => {
      const candidate = tasksById.get(id)?.write?.record;
      return candidate !== undefined ? Effect.succeed(Option.some(candidate)) : committedTask(id);
    });

  // ─── Table writes ─────────────────────────────────────────────────────────

  const stageConversation = (
    parent: ConversationRecord["parent"],
    ownership: ConversationOwnership,
    reservedId?: ConversationId,
  ): Effect.Effect<ConversationRecord, TxDocError, Tx> =>
    Effect.gen(function* () {
      const id = reservedId ?? ConversationId.make(yield* storage.mintId);
      if (sealed) return yield* settled();
      let owner: ConversationRecord["owner"];
      if (ownership.kind === "task") {
        const task = yield* currentTask(ownership.taskId);
        if (sealed) return yield* settled();
        if (Option.isNone(task)) {
          return yield* reject(
            new InvalidOwner({
              message: `Conversation owner task ${ownership.taskId} does not exist`,
            }),
          );
        }
        owner = { conversationId: task.value.conversationId, taskId: ownership.taskId };
      }
      let record: ConversationRecord = { id };
      if (parent !== undefined) record = { ...record, parent };
      if (owner !== undefined) record = { ...record, owner };
      const copies =
        parent === undefined
          ? []
          : yield* prepareForkDocumentCopies(storage, parent.conversationId, parent.at, id);
      if (sealed) return yield* settled();
      for (const copy of copies) {
        forkSourceDocumentIds.add(copy.source.id);
        const entry: DocumentEntry = {
          addressId: addressKey(copy.record),
          address: copy.record,
          target: { kind: "fork-copy", ...copy },
          retireOnCommit: false,
        };
        documents.push(entry);
        latestDocumentByAddress.set(entry.addressId, entry);
      }
      if (parent !== undefined) forkSourceConversationIds.add(parent.conversationId);
      createdConversationIds.add(id);
      writes.push({ type: "conversation", value: record });
      yield* host.conversationCreated(transaction, record);
      if (sealed) return yield* settled();
      return record;
    });

  const appendEntry = (conversationId: ConversationId, draft: EntryDraft) =>
    write(
      Effect.gen(function* () {
        yield* requireConversation(conversationId);
        if (sealed) return yield* settled();
        const id = EntryId.make(yield* storage.mintId);
        if (sealed) return yield* settled();
        const head = draft.head === "self" ? id : draft.head;
        // Undefined `head`, `data` and `byTaskId` are omitted by the copy.
        const copied = yield* copyTableJson({
          ...draft,
          id,
          conversationId,
          head,
          byTaskId: scope.taskId,
        });
        // `model` must be strict JSON in the `Message` shape, which the column is typed with; so must a replace
        // edit's.
        if (draft.model !== undefined && !isModel(draft.model)) {
          return yield* reject(
            new InvalidJson({ message: "Entry model is not an array of model messages" }),
          );
        }
        if (draft.edits?.some((edit) => edit.action === "replace" && !isModel(edit.messages))) {
          return yield* reject(
            new InvalidJson({ message: "A replace edit's messages are not model messages" }),
          );
        }
        // SAFETY: a strict-JSON copy of an `EntryDraft` plus the fields set here, whose `model` was checked,
        // is an `EntryRecord`.
        const record = copied as EntryRecord;
        writes.push({ type: "entry", value: record });
        return record;
      }),
    );

  const createSubmission = (create: SubmissionCreate) =>
    write(
      Effect.gen(function* () {
        yield* requireConversation(create.conversationId);
        if (sealed) return yield* settled();
        const id = SubmissionId.make(yield* storage.mintId);
        if (sealed) return yield* settled();
        const copied = yield* copyTableJson({ ...create, id });
        // SAFETY: a strict-JSON copy of a `SubmissionCreate` with its minted ID is a `SubmissionRecord`.
        const record = copied as SubmissionRecord;
        submissions.set(id, record);
        return record;
      }),
    );

  /**
   * A submission change is staged, not read: it is resolved during assembly
   * against the latest candidate record, so it counts as a table write and works after one.
   */
  const stageSubmissionChange = (id: SubmissionId, change: SubmissionChange) =>
    Effect.suspend((): Effect.Effect<void, CommitRejected> => {
      if (sealed) return Effect.fail(settled());
      hasTableWrite = true;
      return Effect.map(copyTableJson(change), (copied) => {
        // SAFETY: a strict-JSON copy of a `SubmissionChange`.
        submissionChanges.push({ id, change: copied as SubmissionChange });
      });
    });

  const createTask = <I extends Schema.Json, S extends { readonly phase: string }, R, Req>(
    task: Task<I, S, R, Req>,
    input: I,
    options: TaskOptions,
  ): Effect.Effect<TaskId<R>, TxWriteError> =>
    write(
      Effect.gen(function* () {
        const ownership = options.ownership;
        let owner: TaskRecord | undefined;
        if (ownership.kind === "task") {
          // Validated again against the owner's final candidate during assembly.
          const found = yield* currentTask(ownership.taskId);
          if (sealed) return yield* settled();
          if (Option.isNone(found)) {
            return yield* reject(
              new InvalidOwner({ message: `Task owner ${ownership.taskId} does not exist` }),
            );
          }
          owner = found.value;
          if (options.background === true) {
            return yield* reject(
              new InvalidOwner({ message: "A child task cannot be background" }),
            );
          }
          if (
            options.conversationId !== undefined &&
            options.conversationId !== owner.conversationId
          ) {
            return yield* reject(
              new InvalidOwner({
                message: `A child task lives in its owner's conversation ${owner.conversationId}`,
              }),
            );
          }
        }
        const conversationId =
          owner?.conversationId ?? options.conversationId ?? scope.conversationId;
        if (conversationId === undefined) {
          return yield* reject(
            new InvalidOwner({ message: "Tx.createTask() requires options.conversationId" }),
          );
        }
        yield* requireConversation(conversationId);
        if (sealed) return yield* settled();
        const definition = task.definition;
        const checkpoint = definition.initial(input);
        const id = TaskId.make(yield* storage.mintId);
        if (sealed) return yield* settled();
        const copied = yield* copyTableJson({
          id,
          conversationId,
          kind: definition.name,
          version: definition.version,
          input,
          // Undefined properties are omitted by the copy.
          owner: owner?.id,
          background: options.background ?? false,
          abortRequested: false,
          abandonOnRestart: options.abandonOnRestart === true ? true : undefined,
          // Omitted, the creator's carrier; `"root"`, none.
          trace:
            options.trace === "root"
              ? undefined
              : (options.trace ?? (caller === undefined ? undefined : carrierOf(caller))),
          links:
            options.links === undefined || options.links.length === 0 ? undefined : options.links,
          state: { status: "pending", checkpoint },
        });
        // SAFETY: a strict-JSON copy of the pending task record built here.
        tasksById.set(id, { write: { kind: "create", record: copied as TaskRecord } });
        return id;
      }),
    );

  // A write like the others: tracked and uninterruptible from its first step, so a forked `setTask`
  // can never stage its record after settlement.
  const setTask = (value: TaskRecord) =>
    write(Effect.suspend((): Effect.Effect<void, CommitRejected> => stageTask(value)));

  /**
   * Stages a task record over the transaction's candidate, also during assembly. A live replacement keeps the
   * memos an earlier write of this transaction staged, such as a signal delivered to a task whose own commit
   * then changes its state: no write erases another.
   */
  const stageTask = (value: TaskRecord): Effect.Effect<void, CommitRejected> => {
    const task = taskEntry(value.id);
    const candidate = task.write?.record;
    if (candidate?.state.status === "terminal") {
      return Effect.fail(
        reject(new TaskConflict({ message: `Task ${value.id} already has a terminal candidate` })),
      );
    }
    if (candidate !== undefined && candidate.conversationId !== value.conversationId) {
      return Effect.fail(
        reject(new TaskConflict({ message: `Task ${value.id} cannot change conversations` })),
      );
    }
    const staged = SignalMemos.memosOf(candidate);
    const live = value.state.status !== "terminal" && value.state.status !== "completing";
    const composed = staged !== undefined && live ? SignalMemos.underMemos(value, staged) : value;
    // Checked and staged in one synchronous step: a concurrent
    // `setTask` cannot slip in between the candidate check and the write.
    const copied = copyJson(stampTimes(composed, candidate), { omitUndefinedProperties: true });
    if (Result.isFailure(copied)) {
      return Effect.fail(reject(new InvalidJson({ message: copied.failure })));
    }
    task.write = {
      kind: task.write?.kind === "create" ? "create" : "replace",
      // SAFETY: a strict-JSON copy of a task record.
      record: copied.success as TaskRecord,
    };
    return Effect.void;
  };

  /**
   * Delivers a signal to a task: a read of the task, decided against the transaction's latest candidate of
   * it, then a write of it when the key changes. A delivery started alongside another, in the same
   * transaction, sees what the other staged once its own read returned.
   */
  const signal = <T>(taskId: TaskId, token: Signal<T>, key: string, value: T) =>
    read(
      "signal",
      Effect.gen(function* () {
        const committed = Option.getOrUndefined(yield* committedTask(taskId));
        // Deliveries started together, before any of them wrote, are all admitted as reads: each decides after
        // a yield, against what the ones before it staged.
        yield* Effect.yieldNow;
        const delivered = SignalMemos.deliver(
          tasksById.get(taskId)?.write?.record ?? committed,
          token,
          key,
          value,
          host.now(),
        );
        if (delivered.record !== undefined) {
          hasTableWrite = true;
          yield* stageTask(delivered.record);
        }
        return delivered.result;
      }),
    );

  /**
   * Delivers `SubmissionSettled` for a submission this commit settles, against the transaction's latest
   * candidate of the task; a delivery the task refuses changes nothing and fails nothing.
   */
  const notify = (record: SettledSubmissionRecord, target: SubmissionNotify) =>
    Effect.gen(function* () {
      const committed = Option.getOrUndefined(yield* committedTask(target.taskId));
      const delivered = SignalMemos.deliver(
        tasksById.get(target.taskId)?.write?.record ?? committed,
        SubmissionSettled,
        target.key,
        record,
        host.now(),
      );
      if (delivered.record !== undefined) yield* stageTask(delivered.record);
    });

  /**
   * Lifecycle times: `startedAt` on the first change to `running`, `endedAt`
   * on the change to `terminal`; once set, they carry over.
   */
  const stampTimes = (value: TaskRecord, candidate: TaskRecord | undefined): TaskRecord => {
    const status = value.state.status;
    const startedAt =
      candidate?.startedAt ?? value.startedAt ?? (status === "running" ? host.now() : undefined);
    const endedAt =
      candidate?.endedAt ?? value.endedAt ?? (status === "terminal" ? host.now() : undefined);
    let stamped = value;
    if (startedAt !== undefined && startedAt !== value.startedAt)
      stamped = { ...stamped, startedAt };
    if (endedAt !== undefined && endedAt !== value.endedAt) stamped = { ...stamped, endedAt };
    return stamped;
  };

  // ─── Documents ────────────────────────────────────────────────────────────

  const assertTaskDocumentsOpen = (resolved: ResolvedAddress) => {
    const docScope = resolved.address.scope;
    return docScope.kind === "task" &&
      tasksById.get(docScope.taskId)?.write?.record.state.status === "terminal"
      ? Option.some(reject(new TaskConflict({ message: `Task ${docScope.taskId} is terminal` })))
      : Option.none();
  };

  /** Runs one acquisition and memoizes its result for every later acquisition of the address. */
  const memoize = (
    entry: DocumentEntry,
    acquisition: Effect.Effect<Tracker.Draft<Schema.JsonObject>, TxDocError>,
  ) => {
    const deferred = Deferred.makeUnsafe<Tracker.Draft<Schema.JsonObject>, TxDocError>();
    entry.draft = deferred;
    // Published when the tracked, uninterruptible operation settles, in the same step that stops counting
    // it as pending: the memo always holds its real outcome, and a waiter that sees it never finds its
    // producer still pending.
    return track(acquisition, (exit) => publishNow(deferred, exit));
  };

  const docOf = (definition: AnyDefinition, args: ReadonlyArray<DocArgument>) =>
    Effect.uninterruptibleMask((restore) =>
      Effect.suspend((): Effect.Effect<Tracker.Draft<Schema.JsonObject>, TxDocError> => {
        if (sealed) return Effect.fail(settled());
        const resolved = resolveAddress(definition, args);
        const closed = assertTaskDocumentsOpen(resolved);
        if (Option.isSome(closed)) return Effect.fail(closed.value);
        const latest = latestDocumentByAddress.get(resolved.id);
        if (latest !== undefined && !latest.retireOnCommit) {
          if (latest.draft !== undefined) return restore(Deferred.await(latest.draft));
          if (latest.target?.kind === "fork-copy") {
            return memoize(latest, acquireForkCopy(latest, definition, latest.target));
          }
        }
        let seed: Schema.Json | undefined;
        if (definition.family === true) {
          const copied = copyJson(args[resolved.nextArgument]);
          if (Result.isFailure(copied))
            return Effect.fail(reject(new InvalidJson({ message: copied.failure })));
          seed = copied.success;
        }
        const entry: DocumentEntry = {
          addressId: resolved.id,
          address: resolved.address,
          definition,
          retireOnCommit: false,
        };
        documents.push(entry);
        latestDocumentByAddress.set(entry.addressId, entry);
        // Retirement is captured before awaiting, so a pending old acquisition and its replacement stay distinct.
        return memoize(entry, acquire(entry, seed, latest?.retireOnCommit === true));
      }),
    );

  const retireDocOf = (definition: AnyDefinition, args: ReadonlyArray<DocArgument>) =>
    uninterruptibleSuspend((): Effect.Effect<void, TxDocError> => {
      if (sealed) return Effect.fail(settled());
      const resolved = resolveAddress(definition, args);
      const latest = latestDocumentByAddress.get(resolved.id);
      if (latest?.retireOnCommit === true) return Effect.void;
      if (latest?.target?.kind === "fork-copy") {
        const checked = checkRecordScope(definition, latest.target.record);
        if (Result.isFailure(checked)) return Effect.fail(checked.failure);
        latest.retireOnCommit = true;
        return Effect.void;
      }
      if (latest?.draft !== undefined) {
        // Retirement of an acquired draft persists its final content first.
        latest.retireOnCommit = true;
        return track(Deferred.await(latest.draft).pipe(Effect.asVoid));
      }
      const entry: DocumentEntry = {
        addressId: resolved.id,
        address: resolved.address,
        definition,
        retireOnCommit: true,
      };
      documents.push(entry);
      latestDocumentByAddress.set(entry.addressId, entry);
      return track(findRetirement(entry));
    });

  const acquire = (entry: DocumentEntry, seed: Schema.Json | undefined, skipLoad: boolean) =>
    Effect.gen(function* () {
      // SAFETY: public acquisitions always carry their definition.
      const definition = entry.definition!;
      const loaded = skipLoad
        ? Option.none<LoadedDocument>()
        : yield* host.load(definition, entry.addressId, entry.address);
      if (sealed) return yield* settled();
      if (Option.isSome(loaded)) {
        const document = loaded.value;
        yield* Effect.fromResult(checkRecord(definition, document.record, document.storedVersion));
        entry.target = { kind: "loaded", document };
        const change = Tracker.beginChange(document.tracker);
        entry.change = change;
        return change.state;
      }
      const docScope = entry.address.scope;
      if (docScope.kind === "conversation") yield* requireConversation(docScope.conversationId);
      if (docScope.kind === "task") {
        const task = yield* currentTask(docScope.taskId);
        if (Option.isNone(task)) return yield* notFound(`Task ${docScope.taskId} does not exist`);
        if (task.value.state.status === "terminal") {
          return yield* reject(
            new TaskConflict({ message: `Task ${docScope.taskId} is terminal` }),
          );
        }
      }
      if (sealed) return yield* settled();
      // An initializer's root is caller code: copy it, rejecting non-JSON.
      const initial = yield* Effect.fromResult(
        Result.mapError(
          copyJson(definition.family === true ? definition.initial(seed) : definition.initial()),
          (message) => reject(new InvalidJson({ message })),
        ),
      );
      const id = DocumentId.make(yield* storage.mintId);
      if (sealed) return yield* settled();
      // SAFETY: the copy of the object `initial` returned is an object.
      const tracker = Tracker.make(initial as Schema.JsonObject);
      entry.target = {
        kind: "created",
        record: documentCreate(definition, entry.address, id),
        version: definition.version,
        tracker,
      };
      const change = Tracker.beginChange(tracker);
      entry.change = change;
      return change.state;
    });

  const acquireForkCopy = (
    entry: DocumentEntry,
    definition: AnyDefinition,
    target: Extract<DocumentTarget, { readonly kind: "fork-copy" }>,
  ) =>
    Effect.gen(function* () {
      const stored = yield* storage.document(target.source.id, target.source.at);
      if (sealed) return yield* settled();
      if (Option.isNone(stored)) {
        return yield* notFound(`Fork source document ${target.source.id} cannot be read`);
      }
      const source = stored.value.record;
      if (
        !("history" in source) ||
        !("history" in target.record) ||
        source.kind !== target.record.kind ||
        source.key !== target.record.key ||
        source.history !== target.record.history ||
        source.fork !== target.record.fork
      ) {
        return yield* reject(
          new ForkConflict({
            message: `Fork source document ${target.source.id} does not match the copied record`,
          }),
        );
      }
      const value = yield* Effect.fromResult(
        materializeValue(definition, target.record, stored.value.version, stored.value.value),
      );
      const tracker = Tracker.make(value);
      entry.definition = definition;
      entry.target = {
        kind: "created",
        record: target.record,
        version: definition.version,
        tracker,
      };
      const change = Tracker.beginChange(tracker);
      entry.change = change;
      return change.state;
    });

  const findRetirement = (entry: DocumentEntry) =>
    Effect.gen(function* () {
      // What an unpublished commit wrote at the address is not in the cache yet.
      const gate = host.touch(documentKey(entry.addressId));
      if (gate !== undefined) yield* gate;
      const cached = host.cached(entry.addressId)?.record;
      const record =
        cached !== undefined
          ? Option.some(cached)
          : yield* storage.findDocument(entry.address, "current");
      if (sealed) return yield* settled();
      if (Option.isNone(record)) return;
      // SAFETY: a retirement by token always carries its definition.
      yield* Effect.fromResult(checkRecordScope(entry.definition!, record.value));
      entry.target = { kind: "retire-only", record: record.value };
    });

  // ─── Settlement ───────────────────────────────────────────────────────────

  const abortChanges = () => {
    for (const document of documents)
      if (document.change !== undefined) Tracker.abort(document.change);
  };

  const settleFailure = Effect.suspend(() => {
    sealed = true;
    abortChanges();
    return drain;
  });

  const settleSuccess = Effect.suspend(() => {
    sealed = true;
    if (pending > 0) {
      abortChanges();
      return drain.pipe(
        Effect.andThen(
          Effect.fail(
            reject(
              new Settled({
                message: "Session commit callback settled before its pending Tx operations",
              }),
            ),
          ),
        ),
      );
    }
    // Synchronously prepare every open change; this revokes every draft.
    for (const document of documents) {
      if (document.change !== undefined) document.prepared = Tracker.prepare(document.change);
    }
    return assemble.pipe(Effect.onError(() => Effect.sync(abortChanges)));
  });

  const assemble = Effect.gen(function* () {
    for (const document of documents) {
      const plan = planDocument(document);
      if (plan !== undefined) plans.push(plan);
    }
    yield* rejectForkSourceWrites();
    yield* validateOwners;
    for (const [id, task] of tasksById) {
      if (task.write?.kind !== "replace") continue;
      const committed = yield* committedTask(id);
      if (Option.isNone(committed)) return yield* notFound(`Task ${id} does not exist`);
      if (committed.value.state.status === "terminal") {
        return yield* reject(new TaskConflict({ message: `Task ${id} is already terminal` }));
      }
      if (committed.value.conversationId !== task.write.record.conversationId) {
        return yield* reject(
          new TaskConflict({ message: `Task ${id} cannot change conversations` }),
        );
      }
    }

    // Terminal settlement retires every task document, including ones created here.
    const terminalTaskIds = new Set<TaskId>();
    for (const task of tasksById.values()) {
      if (task.write?.record.state.status === "terminal") terminalTaskIds.add(task.write.record.id);
    }
    if (terminalTaskIds.size > 0) {
      const retiring = new Set<DocumentId>();
      for (const plan of plans) {
        const planScope = plan.record.scope;
        if (planScope.kind !== "task" || !terminalTaskIds.has(planScope.taskId)) continue;
        plan.retire = true;
        retiring.add(plan.record.id);
      }
      for (const taskId of terminalTaskIds) {
        if (tasksById.get(taskId)?.write?.kind === "create") continue;
        // The Session knows the documents of the tasks it created; any other task's are scanned. A document an unpublished commit created for the task
        // is in neither yet: wait for it first.
        const gate = host.touch(scopeKey({ kind: "task", taskId }));
        if (gate !== undefined) yield* gate;
        const known = host.taskDocuments(taskId);
        if (known !== undefined) {
          for (const record of known) {
            if (retiring.has(record.id)) continue;
            plans.push({ addressId: addressKey(record), record, retire: true });
            retiring.add(record.id);
          }
          continue;
        }
        let cursor: Option.Option<Cursor> = Option.none();
        do {
          const page = yield* storage.scanDocuments(
            { scope: { kind: "task", taskId }, at: "current" },
            INTERNAL_SCAN_PAGE_SIZE,
            Option.getOrUndefined(cursor),
          );
          for (const record of page.items) {
            if (retiring.has(record.id)) continue;
            plans.push({ addressId: addressKey(record), record, retire: true });
            retiring.add(record.id);
          }
          cursor = page.next;
        } while (Option.isSome(cursor));
      }
    }

    // Publication owners are resolved before admission, so adoption stays synchronous.
    for (const plan of plans) {
      if (!publishes(plan)) continue;
      const planScope = plan.record.scope;
      if (planScope.kind === "conversation") plan.conversationId = planScope.conversationId;
      if (planScope.kind !== "task") continue;
      const task = taskEntry(planScope.taskId);
      if (task.publicationConversationId === undefined) {
        const current = yield* currentTask(planScope.taskId);
        if (Option.isSome(current)) task.publicationConversationId = current.value.conversationId;
      }
      plan.conversationId = task.publicationConversationId;
    }

    // Created settled, or settled by a change: each one with `notify` signals its task.
    const settling = Array.from(submissions.values()).filter(SignalMemos.isSettled);
    for (const { id, change } of submissionChanges) {
      const current = submissions.get(id) ?? Option.getOrUndefined(yield* storage.submission(id));
      if (current === undefined) return yield* notFound(`Submission ${id} does not exist`);
      const next = applySubmissionChange(current, change);
      if (next._tag === "Conflict")
        return yield* reject(new SubmissionConflict({ message: next.message }));
      if (next.record === current) continue;
      submissions.set(id, next.record);
      if (SignalMemos.isSettled(next.record)) settling.push(next.record);
    }
    for (const record of settling) {
      if (record.notify !== undefined) yield* notify(record, record.notify);
    }
    for (const value of submissions.values()) writes.push({ type: "submission", value });
    for (const task of tasksById.values()) {
      if (task.write !== undefined) writes.push({ type: "task", value: task.write.record });
    }
    for (const plan of plans) {
      const change = plan.change;
      // Checkpoint predicates run last, after every validation.
      if (
        plan.content?.type === "document.change" &&
        plan.content.content.kind === "delta" &&
        change?.loaded !== undefined
      ) {
        const info = { deltasSinceBase: change.loaded.deltasSinceBase };
        // A throw in `checkpointWhen` is a defect of the definition; the
        // receiver is the definition.
        if (
          change.definition?.checkpointWhen(change.prepared.value, change.prepared.ops, info) ===
          true
        ) {
          plan.content = {
            ...plan.content,
            content: { version: change.version, kind: "base", value: change.prepared.value },
          };
        }
      }
      if (plan.content !== undefined) writes.push(plan.content);
      if (plan.retire) writes.push({ type: "document.retire", id: plan.record.id });
    }
    return writes;
  });

  /**
   * New owned work needs a live owner, judged on its final candidate: not
   * `completing`, terminal or abort-marked.
   */
  const validateOwners = Effect.gen(function* () {
    const owners: Array<{ readonly what: string; readonly taskId: TaskId }> = [];
    for (const staged of writes) {
      if (staged.type !== "conversation" || staged.value.owner === undefined) continue;
      owners.push({ what: "Conversation owner task", taskId: staged.value.owner.taskId });
    }
    for (const task of tasksById.values()) {
      const record = task.write?.kind === "create" ? task.write.record : undefined;
      if (record?.owner !== undefined) owners.push({ what: "Task owner", taskId: record.owner });
    }
    for (const { what, taskId } of owners) {
      const task = yield* currentTask(taskId);
      if (Option.isNone(task))
        return yield* reject(new InvalidOwner({ message: `${what} ${taskId} does not exist` }));
      const status = task.value.state.status;
      if (status === "terminal" || status === "completing") {
        return yield* reject(new InvalidOwner({ message: `${what} ${taskId} is ${status}` }));
      }
      if (task.value.abortRequested) {
        return yield* reject(new InvalidOwner({ message: `${what} ${taskId} is abort-marked` }));
      }
    }
  });

  // A fork reads one unambiguous stored source.
  const rejectForkSourceWrites = () =>
    Effect.suspend((): Effect.Effect<void, CommitRejected> => {
      for (const plan of plans) {
        if (plan.content === undefined && !plan.retire) continue;
        const record = plan.record;
        if (forkSourceDocumentIds.has(record.id)) {
          return Effect.fail(
            reject(
              new ForkConflict({
                message: `Cannot change fork source document ${record.id} in the fork transaction`,
              }),
            ),
          );
        }
        if (
          record.scope.kind === "conversation" &&
          forkSourceConversationIds.has(record.scope.conversationId) &&
          "fork" in record &&
          record.fork === "current"
        ) {
          return Effect.fail(
            reject(
              new ForkConflict({
                message: `Cannot fork conversation ${record.scope.conversationId} while changing its current-policy documents`,
              }),
            ),
          );
        }
      }
      return Effect.void;
    });

  const adopt = (seq: Seq): AdoptedDocuments => {
    const publications: Array<DocumentCommitChange> = [];
    // Only when the commit stored held changes: then what storage got differs
    // from what memory adopted.
    let stored: Array<DocumentCommitChange> | undefined;
    const push = (plan: DocumentPlan, publication: DocumentCommitChange) => {
      const held = plan.held;
      if (held === undefined) {
        publications.push(publication);
        stored?.push(publication);
        return;
      }
      stored ??= [...publications];
      if (held.observed) publications.push(publication);
      // Only a document change of a loaded incarnation carries held operations; a retirement is published alone.
      stored.push(
        publication.type === "document" && publication.value !== null
          ? { ...publication, ops: held.ops }
          : publication,
      );
    };
    for (const plan of plans) {
      const committed = "createdAt" in plan.record;
      // SAFETY: a record with `createdAt` came from storage, so it is a `DocumentRecord`.
      let record: DocumentRecord = committed
        ? (plan.record as DocumentRecord)
        : { ...plan.record, createdAt: seq };
      if (plan.retire) record = { ...record, retiredAt: seq };
      const change = plan.change;
      if (change !== undefined) {
        // A new incarnation is adopted unless it retires in the same commit; a loaded one only when it changed.
        if (change.loaded === undefined ? !plan.retire : change.prepared.ops.length > 0) {
          Tracker.adopt(change.tracker, change.prepared);
        } else {
          Tracker.abort(change.prepared);
        }
        if (change.loaded !== undefined) {
          if (change.loaded.storedVersion < change.version)
            change.loaded.storedVersion = change.version;
          if (plan.content?.type === "document.change") {
            if (plan.content.content.kind === "base") change.loaded.deltasSinceBase = 0;
            else change.loaded.deltasSinceBase++;
          }
        } else if (!plan.retire) {
          host.install({
            addressId: plan.addressId,
            record,
            storedVersion: change.version,
            valueVersion: change.version,
            deltasSinceBase: 0,
            tracker: change.tracker,
          });
        }
      }
      const conversationId = plan.conversationId;
      if (plan.retire) {
        if (committed) host.evict(plan.addressId, record.id);
        push(plan, {
          type: "document",
          record,
          conversationId,
          version: undefined,
          value: null,
          ops: EMPTY_OPERATIONS,
        });
      } else if (plan.content?.type === "document.copy") {
        // SAFETY: a fork copy is always a conversation document, so its owner was resolved.
        push(plan, {
          type: "document.copy",
          record,
          conversationId: conversationId!,
          source: plan.content.source,
        });
      } else if (change !== undefined && publishes(plan)) {
        push(plan, {
          type: "document",
          record,
          conversationId,
          version: change.version,
          value: change.prepared.value,
          ops: change.loaded === undefined ? EMPTY_OPERATIONS : change.prepared.ops,
        });
      }
    }
    return { observed: publications, stored: stored ?? publications };
  };

  const holdable = (): ReadonlyArray<HeldChange> | undefined => {
    if (writes.some((write) => write.type !== "document.change")) return undefined;
    const held: Array<HeldChange> = [];
    for (const plan of plans) {
      const content = plan.content;
      const change = plan.change;
      if (content === undefined && !plan.retire) continue;
      if (plan.retire || change?.loaded === undefined || content?.type !== "document.change") {
        return undefined;
      }
      const record = change.loaded.record;
      if (
        record.scope.kind !== "conversation" ||
        !("history" in record) ||
        record.history !== "latest" ||
        record.fork !== "initial"
      ) {
        return undefined;
      }
      // A migration stores a base at once.
      if (change.loaded.storedVersion !== change.version) return undefined;
      held.push({
        loaded: change.loaded,
        ops: change.prepared.ops,
        base: content.content.kind === "base",
      });
    }
    return held;
  };

  const adoptHeld = (): ReadonlyArray<DocumentCommitChange> => {
    const publications: Array<DocumentCommitChange> = [];
    for (const plan of plans) {
      const change = plan.change;
      // Every plan of a held commit changes or reads a loaded incarnation (`holdable`).
      if (change?.loaded === undefined) continue;
      if (change.prepared.ops.length === 0) {
        Tracker.abort(change.prepared);
        continue;
      }
      Tracker.adopt(change.tracker, change.prepared);
      publications.push({
        type: "document",
        record: change.loaded.record,
        conversationId: plan.conversationId,
        version: change.version,
        value: change.prepared.value,
        ops: change.prepared.ops,
      });
    }
    return publications;
  };

  const takeHeld = <H extends HeldDocument>(
    held: (id: DocumentId) => H | undefined,
  ): ReadonlyArray<H> => {
    const taken: Array<H> = [];
    for (const plan of plans) {
      // A new incarnation holds nothing yet.
      if (!("createdAt" in plan.record)) continue;
      const hold = held(plan.record.id);
      if (hold === undefined) continue;
      taken.push(hold);
      const own = plan.content?.type === "document.change" ? plan.content : undefined;
      const change = plan.change;
      const value = change?.prepared.value ?? hold.loaded.tracker.value;
      const stored = storeHeld(
        hold,
        change?.prepared.ops ?? EMPTY_OPERATIONS,
        value,
        hold.loaded.valueVersion,
      );
      // A base this commit selected or must write covers what was held.
      const content = own?.content.kind === "base" ? own.content : stored.content;
      const write = { type: "document.change", id: plan.record.id, content } as const;
      if (own !== undefined) writes[writes.indexOf(own)] = write;
      else insertBeforeRetirement(write);
      if (!plan.retire) plan.held = { ops: stored.ops, observed: own !== undefined };
      plan.content = write;
      // Held documents are conversation documents; one this commit only read has no owner resolved yet.
      if (plan.record.scope.kind === "conversation")
        plan.conversationId = plan.record.scope.conversationId;
    }
    return taken;
  };

  /** Stages a content write for an incarnation, before its retirement when it retires. */
  const insertBeforeRetirement = (
    write: Extract<StorageWrite, { readonly type: "document.change" }>,
  ) => {
    const at = writes.findIndex(
      (candidate) => candidate.type === "document.retire" && candidate.id === write.id,
    );
    if (at < 0) writes.push(write);
    else writes.splice(at, 0, write);
  };

  // ─── The public Tx ────────────────────────────────────────────────────────

  // The public overloads resolve their arguments through the token's scope and family shape (or
  // whether the first argument is an entry kind), which the erased implementations read at runtime.

  function doc<T>(token: SessionDoc<T>): Effect.Effect<Tracker.Draft<T>, TxDocError>;
  function doc<T>(
    token: ConversationDoc<T>,
    conversationId: ConversationId,
  ): Effect.Effect<Tracker.Draft<T>, TxDocError>;
  function doc<T>(token: TaskDoc<T>, taskId: TaskId): Effect.Effect<Tracker.Draft<T>, TxDocError>;
  function doc<T, I>(
    token: SessionDocFamily<T, I>,
    key: string,
    seed: I,
  ): Effect.Effect<Tracker.Draft<T>, TxDocError>;
  function doc<T, I>(
    token: ConversationDocFamily<T, I>,
    conversationId: ConversationId,
    key: string,
    seed: I,
  ): Effect.Effect<Tracker.Draft<T>, TxDocError>;
  function doc<T, I>(
    token: TaskDocFamily<T, I>,
    taskId: TaskId,
    key: string,
    seed: I,
  ): Effect.Effect<Tracker.Draft<T>, TxDocError>;
  function doc<T>(token: Doc<T, object>, ...args: ReadonlyArray<DocArgument>) {
    return docOf(definitionOf(token), args).pipe(
      // SAFETY: the draft edits the document the token types.
      Effect.map((draft) => draft as Tracker.Draft<T>),
    );
  }

  function retireDoc<T>(token: SessionDoc<T>): Effect.Effect<void, TxDocError>;
  function retireDoc<T>(
    token: ConversationDoc<T>,
    conversationId: ConversationId,
  ): Effect.Effect<void, TxDocError>;
  function retireDoc<T>(token: TaskDoc<T>, taskId: TaskId): Effect.Effect<void, TxDocError>;
  function retireDoc<T, I>(
    token: SessionDocFamily<T, I>,
    key: string,
  ): Effect.Effect<void, TxDocError>;
  function retireDoc<T, I>(
    token: ConversationDocFamily<T, I>,
    conversationId: ConversationId,
    key: string,
  ): Effect.Effect<void, TxDocError>;
  function retireDoc<T, I>(
    token: TaskDocFamily<T, I>,
    taskId: TaskId,
    key: string,
  ): Effect.Effect<void, TxDocError>;
  function retireDoc<T>(token: Doc<T, object>, ...args: ReadonlyArray<DocArgument>) {
    return retireDocOf(definitionOf(token), args);
  }

  function entry(id: EntryId): Effect.Effect<Option.Option<EntryRecord>, TxReadError>;
  function entry<D extends Schema.Json>(
    kind: EntryKind<D>,
    id: EntryId,
  ): Effect.Effect<Option.Option<TypedEntry<D>>, TxReadError>;
  function entry<D extends Schema.Json>(first: EntryId | EntryKind<D>, second?: EntryId) {
    const kind = Predicate.isNumber(first) ? undefined : first.kind;
    // SAFETY: the two-argument overload always passes the ID second.
    const id = Predicate.isNumber(first) ? first : second!;
    return read(
      "entry",
      storage
        .entry(id)
        .pipe(
          Effect.map(Option.map((committed) => committed.entry)),
          Effect.map(Option.filter((record) => kind === undefined || record.kind === kind)),
        ),
    );
  }

  function appendEntryTo(
    conversationId: ConversationId,
    draft: EntryDraft,
  ): Effect.Effect<EntryRecord, TxWriteError>;
  function appendEntryTo<D extends Schema.Json>(
    kind: EntryKind<D>,
    conversationId: ConversationId,
    draft: TypedEntryDraft<D>,
  ): Effect.Effect<TypedEntry<D>, TxWriteError>;
  function appendEntryTo<D extends Schema.Json>(
    first: ConversationId | EntryKind<D>,
    second: ConversationId | EntryDraft,
    third?: TypedEntryDraft<D>,
  ) {
    if (Predicate.isNumber(first)) {
      // SAFETY: the raw overload passes the draft second.
      return appendEntry(first, second as EntryDraft);
    }
    // SAFETY: the typed overload passes the conversation second and the draft third.
    return appendEntry(second as ConversationId, { ...third, kind: first.kind });
  }

  const service = Tx.of({
    conversation: (id) => read("conversation", storage.conversation(id)),
    entry,
    task: (id) => read("task", committedTask(id)),
    scanConversations: (query, limit, cursor) =>
      read("scanConversations", storage.scanConversations(query, limit, cursor)),
    scanEntries: (query, limit, cursor) =>
      read("scanEntries", storage.scanEntries(query, limit, cursor)),
    scanTasks: (query, limit, cursor) => read("scanTasks", storage.scanTasks(query, limit, cursor)),
    latestHeadMarker: (conversationId) =>
      read(
        "latestHeadMarker",
        Effect.map(host.latestHeadMarker(conversationId), Option.fromUndefinedOr),
      ),
    submission: (id) => read("submission", storage.submission(id)),
    submissionByRequest: (conversationId, requestId) =>
      read("submissionByRequest", storage.submissionByRequest(conversationId, requestId)),
    createConversation: (options) =>
      write(stageConversation(undefined, options.ownership)).pipe(
        Effect.provideService(Tx, service),
      ),
    forkConversation: (parent, at, options) =>
      write(stageConversation({ conversationId: parent, at }, options.ownership)).pipe(
        Effect.provideService(Tx, service),
      ),
    appendEntry: appendEntryTo,
    createTask: (task, input, options) => createTask(task, input, options),
    createSubmission,
    settleSubmission: (id, settlement) => stageSubmissionChange(id, settlement),
    placeSubmission: (id, entry) => stageSubmissionChange(id, { status: "placed", entry }),
    signal,
    doc,
    retireDoc,
  });

  identities.set(service, {
    host,
    hasTableWrite: () => hasTableWrite,
    caller,
    submissionTrace: (id) =>
      Effect.suspend(() => {
        const staged = submissions.get(id);
        return staged !== undefined
          ? Effect.succeed(staged.trace)
          : Effect.map(storage.submission(id), (found) =>
              Option.isSome(found) ? found.value.trace : undefined,
            );
      }),
  });

  const transaction: Transaction = {
    service,
    setTask,
    stagedTasks: () => {
      const records: Array<TaskRecord> = [];
      for (const task of tasksById.values())
        if (task.write !== undefined) records.push(task.write.record);
      return records;
    },
    writeKeys: () => {
      const keys: Array<string> = [];
      tableKeys(writes, keys);
      for (const plan of plans) {
        if (plan.content === undefined && !plan.retire) continue;
        keys.push(documentKey(plan.addressId), incarnationKey(plan.record.id));
        if (plan.retire || !("createdAt" in plan.record)) keys.push(scopeKey(plan.record.scope));
      }
      return keys;
    },
    createdTaskIds: () => {
      const ids: Array<TaskId> = [];
      for (const [id, task] of tasksById) if (task.write?.kind === "create") ids.push(id);
      return ids;
    },
    stagedConversations: () => {
      const records: Array<ConversationRecord> = [];
      for (const staged of writes) if (staged.type === "conversation") records.push(staged.value);
      return records;
    },
    createRootConversation: write(
      stageConversation(undefined, { kind: "ownerless" }, ROOT_CONVERSATION_ID),
    ).pipe(Effect.provideService(Tx, service)),
    settleFailure,
    settleSuccess,
    discard: abortChanges,
    adopt,
    holdable,
    adoptHeld,
    takeHeld,
  };
  return transaction;
};

/** What `storeHeld` gives a store of held changes: the content to write, and the ops a reader of storage applies. */
export interface StoredHeld {
  readonly content: DocumentContent;
  readonly ops: Delta.Delta;
}

/**
 * Takes a hold's changes after `ops`, a commit's own on top of
 * them, as one stored content for an incarnation now at `value`, and the operations a reader of the stored state
 * applies to reach it: the folded ones, or a root replacement after the queue overflowed, which stores a base, as
 * does a hold whose commits selected one.
 */
export const storeHeld = (
  hold: HeldDocument,
  ops: Delta.Delta,
  value: Schema.JsonObject,
  version: number,
): StoredHeld => {
  PendingOps.push(hold.pending, ops);
  const taken = PendingOps.take(hold.pending);
  if (taken._tag === "Replace")
    return { content: { version, kind: "base", value }, ops: [["r", value]] };
  if (hold.base) return { content: { version, kind: "base", value }, ops: taken.ops };
  return { content: { version, kind: "delta", ops: taken.ops }, ops: taken.ops };
};

/** `Effect.suspend` whose body runs uninterruptibly from its first step. */
const uninterruptibleSuspend = <A, E, R>(
  body: () => Effect.Effect<A, E, R>,
): Effect.Effect<A, E, R> => Effect.uninterruptible(Effect.suspend(body));

/**
 * Completes a memo; its waiters yield before they see the outcome, as a
 * promise `await` resumes in a later microtask. That yield is not an ordering
 * guarantee: a memo whose waiters must not see its producer still pending is
 * published from `track`'s `onSettled` (see `memoize`). The task-read memo's
 * waiters are tracked operations themselves, so they cannot settle a commit
 * early.
 */
const publishNow = <A, E>(deferred: Deferred.Deferred<A, E>, exit: Exit.Exit<A, E>) => {
  doneAfterYield(deferred, exit);
};

const publish = <A, E>(deferred: Deferred.Deferred<A, E>, exit: Exit.Exit<A, E>) =>
  Effect.sync(() => publishNow(deferred, exit));

/** Plan of one staged document: its record, content write and prepared change. Retirement is decided later. */
const planDocument = (document: DocumentEntry): DocumentPlan | undefined => {
  const target = document.target;
  if (target === undefined) return undefined;
  const base = { addressId: document.addressId, retire: document.retireOnCommit };
  switch (target.kind) {
    case "created": {
      // SAFETY: every created target has an open change, prepared at settlement.
      const prepared = document.prepared!;
      return {
        ...base,
        record: target.record,
        content: {
          type: "document.create",
          record: target.record,
          content: { version: target.version, kind: "base", value: prepared.value },
        },
        change: { tracker: target.tracker, prepared, version: target.version },
      };
    }
    case "fork-copy":
      return {
        ...base,
        record: target.record,
        content: { type: "document.copy", record: target.record, source: target.source },
      };
    case "retire-only":
      return { ...base, record: target.record };
    case "loaded": {
      const loaded = target.document;
      // SAFETY: a loaded target came from a typed acquisition, prepared at settlement.
      const definition = document.definition!;
      const prepared = document.prepared!;
      const version = definition.version;
      const id = loaded.record.id;
      // A version change stores a base even without operations; otherwise only a
      // change stores a delta.
      const content: DocumentPlan["content"] =
        loaded.storedVersion < version
          ? {
              type: "document.change",
              id,
              content: { version, kind: "base", value: prepared.value },
            }
          : prepared.ops.length > 0
            ? {
                type: "document.change",
                id,
                content: { version, kind: "delta", ops: prepared.ops },
              }
            : undefined;
      const plan: DocumentPlan = {
        ...base,
        record: loaded.record,
        change: { tracker: loaded.tracker, prepared, version, loaded, definition },
      };
      if (content !== undefined) plan.content = content;
      return plan;
    }
  }
};

/**
 * Whether adoption publishes the plan: every creation, copy and retirement,
 * and a loaded incarnation that writes content, which includes a
 * migration-only base so observers of the older shape receive the new value.
 */
const publishes = (plan: DocumentPlan): boolean =>
  plan.retire || plan.change?.loaded === undefined || plan.content !== undefined;

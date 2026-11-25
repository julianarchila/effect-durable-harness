/**
 * In-memory `AgentStorage`: the reference semantics.
 *
 * @since 0.0.0
 */
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Predicate from "effect/Predicate";
import * as Result from "effect/Result";
import {
  AgentStorage,
  type CommittedEntry,
  type ConversationQuery,
  type Cursor,
  type DocumentAddress,
  type DocumentContent,
  type DocumentPoint,
  type DocumentQuery,
  type EntryQuery,
  type HeadMarker,
  type ScanOrder,
  type StorageFailure,
  StorageRequestError,
  type StorageWrite,
  type StoredDocument,
  type SubmissionQuery,
  type TaskQuery,
} from "./AgentStorage.ts";
import {
  type ConversationId,
  type DocumentId,
  type EntryId,
  Seq,
  type SubmissionId,
  type TaskId,
} from "./Ids.ts";
import {
  addressKey,
  candidateNextId,
  checkDocumentActions,
  checkGlobalIds,
  copyContent,
  corrupted,
  type DocumentAction,
  isAliveAt,
  isCurrentOnly,
  mintFrom,
  prepareDocumentActions,
  rejected,
  replay,
  scopeOwner,
  type TableName,
} from "./internal/commit.ts";
import { detach } from "./internal/detach.ts";
import { openGate } from "./internal/gate.ts";
import { page, type ScanStart, scanStart } from "./internal/scan.ts";
import type {
  ConversationRecord,
  DocumentRecord,
  DocumentScope,
  EntryRecord,
  SubmissionRecord,
  SubmissionStatus,
  TaskRecord,
  TaskStatus,
} from "./Records.ts";

type Revision = DocumentContent & { readonly seq: Seq };

interface StoredDocumentState {
  record: DocumentRecord;
  revisions: Array<Revision>;
}

interface AddressIndex {
  readonly ids: Array<DocumentId>;
  currentId?: DocumentId;
}

const lowerBound = (ids: ReadonlyArray<number>, target: number): number => {
  let low = 0;
  let high = ids.length;
  while (low < high) {
    const middle = (low + high) >>> 1;
    if ((ids[middle] ?? 0) < target) low = middle + 1;
    else high = middle;
  }
  return low;
};

const upperBound = (ids: ReadonlyArray<number>, target: number): number => {
  let low = 0;
  let high = ids.length;
  while (low < high) {
    const middle = (low + high) >>> 1;
    if ((ids[middle] ?? 0) <= target) low = middle + 1;
    else high = middle;
  }
  return low;
};

const insertSorted = <I extends number>(ids: Array<I>, id: I): void => {
  const last = ids.at(-1);
  if (last === undefined || last < id) ids.push(id);
  else ids.splice(lowerBound(ids, id), 0, id);
};

const removeSorted = <I extends number>(ids: Array<I>, id: I): void => {
  const index = lowerBound(ids, id);
  if (ids[index] === id) ids.splice(index, 1);
};

const indexed = <K, I extends number>(index: Map<K, Array<I>>, key: K): Array<I> => {
  let ids = index.get(key);
  if (ids === undefined) {
    ids = [];
    index.set(key, ids);
  }
  return ids;
};

/** Indexes of sorted `ids` in scan order, after the cursor's ID when there is one. */
function* scanIndexes(ids: ReadonlyArray<number>, start: ScanStart): Generator<number> {
  if (start.order === "ascending") {
    const first = start.after === undefined ? 0 : upperBound(ids, start.after);
    for (let index = first; index < ids.length; index++) yield index;
  } else {
    const end = start.after === undefined ? ids.length : lowerBound(ids, start.after);
    for (let index = end - 1; index >= 0; index--) yield index;
  }
}

const scopeKey = (scope: DocumentScope): string => JSON.stringify([scope.kind, scopeOwner(scope)]);

const unknownConversation = (id: ConversationId) =>
  new StorageRequestError({ message: `Unknown conversation: ${id}` });

const historicalRead = (id: DocumentId) =>
  new StorageRequestError({ message: `Document ${id} does not retain historical content` });

const make = Effect.gen(function* () {
  const recordTypes = new Map<number, TableName>();
  const conversations = new Map<number, ConversationRecord>();
  const conversationIds: Array<number> = [];
  const conversationIdsByOwnerConversation = new Map<number, Array<number>>();
  const conversationIdsByOwnerTask = new Map<number, Array<number>>();
  const entries = new Map<number, EntryRecord>();
  const entryIds = new Map<number, Array<number>>();
  const headEntryIds = new Map<number, Array<number>>();
  const entryCommitSeqs = new Map<number, Seq>();
  const tasks = new Map<number, TaskRecord>();
  const taskIds: Array<number> = [];
  const taskIdsByStatus: Record<TaskStatus, Array<number>> = {
    pending: [],
    running: [],
    waiting: [],
    sleeping: [],
    completing: [],
    terminal: [],
  };
  const submissions = new Map<number, SubmissionRecord>();
  const submissionIds: Array<number> = [];
  const submissionIdsByStatus: Record<SubmissionStatus, Array<number>> = {
    queued: [],
    placed: [],
    done: [],
    unanswered: [],
  };
  const submissionIdsByRequest = new Map<number, Map<string, number>>();
  const documents = new Map<number, StoredDocumentState>();
  const documentAddresses = new Map<string, AddressIndex>();
  const documentIdsByScope = new Map<string, Array<DocumentId>>();
  let nextId = 2;
  let nextSeq = 1;

  const gate = yield* openGate("MemoryAgentStorage");
  yield* Effect.addFinalizer(() => gate.close);

  const materialize = (
    id: DocumentId,
    at: DocumentPoint,
  ): Result.Result<Option.Option<StoredDocument>, StorageRequestError | StorageFailure> => {
    const stored = documents.get(id);
    if (stored === undefined) return Result.succeed(Option.none());
    if (at !== "current" && isCurrentOnly(stored.record)) return Result.fail(historicalRead(id));
    if (!isAliveAt(stored.record, at)) return Result.succeed(Option.none());
    const revisions =
      at === "current"
        ? stored.revisions
        : stored.revisions.filter((revision) => revision.seq <= at);
    let baseIndex = revisions.length - 1;
    while (baseIndex >= 0 && revisions[baseIndex]?.kind !== "base") baseIndex--;
    const base = revisions[baseIndex];
    if (base?.kind !== "base") {
      return Result.fail(corrupted(`Document ${id} is missing a required base`, id));
    }
    const tail = revisions.slice(baseIndex + 1).map((revision) => ({
      kind: revision.kind,
      version: revision.version,
      ops: revision.kind === "delta" ? revision.ops : [],
    }));
    return Result.map(replay(id, base, tail), (value) =>
      Option.some({
        record: stored.record,
        version: base.version,
        value,
        deltasSinceBase: tail.length,
      }),
    );
  };

  function* visibleEntries(
    conversationId: ConversationId,
    minEntryId = Number.NEGATIVE_INFINITY,
    maxEntryId = Number.POSITIVE_INFINITY,
  ): Generator<EntryRecord> {
    let currentId: number = conversationId;
    let upperEntryId = maxEntryId;
    while (true) {
      const ids = entryIds.get(currentId) ?? [];
      for (let index = upperBound(ids, upperEntryId) - 1; index >= 0; index--) {
        const id = ids[index] ?? 0;
        if (id < minEntryId) break;
        const found = entries.get(id);
        if (found !== undefined) yield found;
      }
      const conversation = conversations.get(currentId);
      if (conversation?.parent === undefined) break;
      upperEntryId = Math.min(upperEntryId, conversation.parent.at);
      if (upperEntryId < minEntryId) break;
      currentId = conversation.parent.conversationId;
    }
  }

  /** Visible entries oldest first: the fork chain's segments from the root conversation forward. */
  function* visibleEntriesAscending(
    conversationId: ConversationId,
    minEntryId = Number.NEGATIVE_INFINITY,
    maxEntryId = Number.POSITIVE_INFINITY,
  ): Generator<EntryRecord> {
    const segments: Array<{ readonly conversationId: number; readonly upper: number }> = [];
    let currentId: number = conversationId;
    let upperEntryId = maxEntryId;
    while (true) {
      segments.push({ conversationId: currentId, upper: upperEntryId });
      const conversation = conversations.get(currentId);
      if (conversation?.parent === undefined) break;
      upperEntryId = Math.min(upperEntryId, conversation.parent.at);
      if (upperEntryId < minEntryId) break;
      currentId = conversation.parent.conversationId;
    }
    for (const segment of segments.toReversed()) {
      const ids = entryIds.get(segment.conversationId) ?? [];
      for (let index = lowerBound(ids, minEntryId); index < ids.length; index++) {
        const id = ids[index] ?? 0;
        if (id > segment.upper) break;
        const found = entries.get(id);
        if (found !== undefined) yield found;
      }
    }
  }

  const requireConversation = (id: ConversationId): Result.Result<void, StorageRequestError> =>
    conversations.has(id) ? Result.void : Result.fail(unknownConversation(id));

  const committedEntry = (found: EntryRecord): CommittedEntry => ({
    entry: detach(found),
    commitSeq: entryCommitSeqs.get(found.id) ?? Seq.make(0),
  });

  const applyTableWrite = (write: StorageWrite, seq: Seq): void => {
    switch (write.type) {
      case "conversation": {
        const value = write.value;
        recordTypes.set(value.id, "conversation");
        conversations.set(value.id, value);
        insertSorted(conversationIds, value.id);
        if (value.owner !== undefined) {
          insertSorted(
            indexed(conversationIdsByOwnerConversation, value.owner.conversationId),
            value.id,
          );
          insertSorted(indexed(conversationIdsByOwnerTask, value.owner.taskId), value.id);
        }
        return;
      }
      case "entry": {
        const value = write.value;
        recordTypes.set(value.id, "entry");
        entries.set(value.id, value);
        entryCommitSeqs.set(value.id, seq);
        insertSorted(indexed(entryIds, value.conversationId), value.id);
        if (value.head !== undefined)
          insertSorted(indexed(headEntryIds, value.conversationId), value.id);
        return;
      }
      case "task": {
        const value = write.value;
        recordTypes.set(value.id, "task");
        const previous = tasks.get(value.id);
        if (previous === undefined) {
          insertSorted(taskIds, value.id);
          insertSorted(taskIdsByStatus[value.state.status], value.id);
        } else if (previous.state.status !== value.state.status) {
          removeSorted(taskIdsByStatus[previous.state.status], value.id);
          insertSorted(taskIdsByStatus[value.state.status], value.id);
        }
        tasks.set(value.id, value);
        return;
      }
      case "submission": {
        const value = write.value;
        recordTypes.set(value.id, "submission");
        const previous = submissions.get(value.id);
        if (previous === undefined) {
          insertSorted(submissionIds, value.id);
          insertSorted(submissionIdsByStatus[value.status], value.id);
        } else if (previous.status !== value.status) {
          removeSorted(submissionIdsByStatus[previous.status], value.id);
          insertSorted(submissionIdsByStatus[value.status], value.id);
        }
        if (previous?.requestId !== undefined) {
          const previousRequests = submissionIdsByRequest.get(previous.conversationId);
          if (previousRequests?.get(previous.requestId) === value.id) {
            previousRequests.delete(previous.requestId);
            if (previousRequests.size === 0) submissionIdsByRequest.delete(previous.conversationId);
          }
        }
        submissions.set(value.id, value);
        if (value.requestId !== undefined) {
          let requests = submissionIdsByRequest.get(value.conversationId);
          if (requests === undefined) {
            requests = new Map();
            submissionIdsByRequest.set(value.conversationId, requests);
          }
          requests.set(value.requestId, value.id);
        }
        return;
      }
      case "document.create":
      case "document.copy":
      case "document.change":
      case "document.retire":
        return;
    }
  };

  const applyDocumentActions = (
    actions: ReadonlyMap<DocumentId, DocumentAction>,
    seq: Seq,
  ): void => {
    for (const [id, action] of actions) {
      let stored = documents.get(id);
      if (action.create !== undefined) {
        const record: DocumentRecord = action.retire
          ? { ...action.create, createdAt: seq, retiredAt: seq }
          : { ...action.create, createdAt: seq };
        stored = {
          record,
          revisions: action.content === undefined ? [] : [{ ...action.content, seq }],
        };
        recordTypes.set(id, "document");
        documents.set(id, stored);
        let address = documentAddresses.get(addressKey(record));
        if (address === undefined) {
          address = { ids: [] };
          documentAddresses.set(addressKey(record), address);
        }
        insertSorted(address.ids, id);
        insertSorted(indexed(documentIdsByScope, scopeKey(record.scope)), id);
      } else if (action.content !== undefined && stored !== undefined) {
        const revision: Revision = { ...action.content, seq };
        if (revision.kind === "base" && isCurrentOnly(stored.record)) stored.revisions = [revision];
        else stored.revisions.push(revision);
      }
      if (stored === undefined) continue;
      if (action.retire && action.create === undefined)
        stored.record = { ...stored.record, retiredAt: seq };
      if (action.retire && isCurrentOnly(stored.record)) stored.revisions = [];
      if (action.create !== undefined || action.retire) {
        const address = documentAddresses.get(addressKey(stored.record));
        if (address !== undefined) {
          if (action.retire && address.currentId === id) delete address.currentId;
          if (action.create !== undefined && !action.retire) address.currentId = id;
        }
      }
    }
  };

  const scanTable = <A extends { readonly id: number }>(
    ids: ReadonlyArray<number>,
    records: ReadonlyMap<number, A>,
    order: ScanOrder | undefined,
    cursor: Cursor | undefined,
    limit: number,
    matches: (value: A) => boolean,
  ) =>
    Result.map(scanStart(order, cursor, "ascending"), (start) => {
      const values: Array<A> = [];
      for (const index of scanIndexes(ids, start)) {
        if (values.length > limit) break;
        const value = records.get(ids[index] ?? 0);
        if (value !== undefined && matches(value)) values.push(detach(value));
      }
      return page(values, limit, start.order);
    });

  /**
   * Checks one batch against the current state and detaches it, changing
   * nothing, so a batch it refuses leaves no trace.
   */
  const prepareBatch = Effect.fnUntraced(function* (writes: ReadonlyArray<StorageWrite>) {
    // Writes are copied in, so a caller's later mutation changes nothing.
    const detached = writes.map(detach);
    yield* checkGlobalIds(detached, (id) => Effect.succeed(recordTypes.get(id)));
    const actions = yield* Effect.fromResult(prepareDocumentActions(detached));
    // A copy reads pre-batch committed state, whatever the order of the writes.
    for (const [id, action] of actions) {
      if (action.copy === undefined || action.create === undefined) continue;
      const stored = yield* Effect.fromResult(
        Result.mapError(materialize(action.copy.id, action.copy.at), (error) =>
          Predicate.isTagged(error, "StorageRequestError")
            ? rejected(`Document copy ${id} was rejected: ${error.message}`)
            : error,
        ),
      );
      action.content = detach(
        yield* Effect.fromResult(copyContent(id, action.copy, stored, action.create)),
      );
    }
    yield* checkDocumentActions(actions, {
      existing: (id) =>
        Effect.succeed(
          Option.map(Option.fromUndefinedOr(documents.get(id)), (stored) => ({
            record: stored.record,
            lastVersion: stored.revisions.at(-1)?.version,
          })),
        ),
      hasCurrent: (record) =>
        Effect.succeed(documentAddresses.get(addressKey(record))?.currentId !== undefined),
    });
    return { writes: detached, actions };
  });

  return AgentStorage.of({
    // Each batch is checked against the state the earlier ones left, changing
    // nothing, then applied; the first one refused ends the call. Memory is durable at once; the receipts
    // follow the last batch.
    commit: Effect.fn("AgentStorage.commit")(
      function* (
        batches: ReadonlyArray<ReadonlyArray<StorageWrite>>,
        receive: (index: number, seq: Seq) => void,
      ) {
        const seqs: Array<Seq> = [];
        let refused: StorageFailure | undefined;
        for (const writes of batches) {
          const prepared = yield* Effect.result(prepareBatch(writes));
          if (Result.isFailure(prepared)) {
            refused = prepared.failure;
            break;
          }
          const seq = Seq.make(nextSeq);
          const batch = prepared.success;
          const applied = Result.try({
            try: () => {
              for (const write of batch.writes) applyTableWrite(write, seq);
              applyDocumentActions(batch.actions, seq);
            },
            catch: (error) => error,
          });
          if (Result.isFailure(applied)) {
            // A batch whose apply throws: the batches applied before it are stored, and
            // their receipts stand; the call then fails.
            for (const [index, stored] of seqs.entries()) receive(index, stored);
            return yield* Effect.die(applied.failure);
          }
          nextId = candidateNextId(nextId, batch.writes);
          nextSeq = seq + 1;
          seqs.push(seq);
        }
        for (const [index, seq] of seqs.entries()) receive(index, seq);
        return refused === undefined ? { stored: seqs.length } : { stored: seqs.length, refused };
      },
      gate.admit,
      Effect.uninterruptible,
    ),

    // `mintId` is an Effect value, not a method, so it is traced with `withSpan`.
    mintId: Effect.gen(function* () {
      const id = yield* Effect.fromResult(mintFrom(nextId));
      nextId = id + 1;
      return id;
    }).pipe(gate.admit, Effect.withSpan("AgentStorage.mintId")),

    conversation: Effect.fn("AgentStorage.conversation")(function* (id: ConversationId) {
      return Option.map(Option.fromUndefinedOr(conversations.get(id)), detach);
    }, gate.admit),

    scanConversations: Effect.fn("AgentStorage.scanConversations")(function* (
      query: ConversationQuery,
      limit: number,
      cursor?: Cursor | undefined,
    ) {
      const ids =
        query.ownerTaskId !== undefined
          ? (conversationIdsByOwnerTask.get(query.ownerTaskId) ?? [])
          : query.ownerConversationId !== undefined
            ? (conversationIdsByOwnerConversation.get(query.ownerConversationId) ?? [])
            : conversationIds;
      return yield* Effect.fromResult(
        scanTable(
          ids,
          conversations,
          query.order,
          cursor,
          limit,
          (value) =>
            query.ownerConversationId === undefined ||
            value.owner?.conversationId === query.ownerConversationId,
        ),
      );
    }, gate.admit),

    entry: Effect.fn("AgentStorage.entry")(function* (id: EntryId) {
      return Option.map(Option.fromUndefinedOr(entries.get(id)), committedEntry);
    }, gate.admit),

    visibleEntry: Effect.fn("AgentStorage.visibleEntry")(function* (
      conversationId: ConversationId,
      id: EntryId,
    ) {
      yield* Effect.fromResult(requireConversation(conversationId));
      return Option.map(
        Option.fromUndefinedOr(visibleEntries(conversationId, id, id).next().value),
        committedEntry,
      );
    }, gate.admit),

    findLatestHeadMarker: Effect.fn("AgentStorage.findLatestHeadMarker")(function* (
      conversationId: ConversationId,
      atOrBefore?: EntryId | undefined,
    ) {
      yield* Effect.fromResult(requireConversation(conversationId));
      let currentId: number = conversationId;
      let upperEntryId: number = atOrBefore ?? Number.POSITIVE_INFINITY;
      while (true) {
        const ids = headEntryIds.get(currentId) ?? [];
        const index = upperBound(ids, upperEntryId) - 1;
        const marker = index >= 0 ? entries.get(ids[index] ?? 0) : undefined;
        if (marker?.head !== undefined) {
          return Option.some<HeadMarker>(detach({ ...marker, head: marker.head }));
        }
        const conversation = conversations.get(currentId);
        if (conversation?.parent === undefined) return Option.none<HeadMarker>();
        upperEntryId = Math.min(upperEntryId, conversation.parent.at);
        currentId = conversation.parent.conversationId;
      }
    }, gate.admit),

    scanEntries: Effect.fn("AgentStorage.scanEntries")(function* (
      query: EntryQuery,
      limit: number,
      cursor?: Cursor | undefined,
    ) {
      const start = yield* Effect.fromResult(scanStart(query.order, cursor, "descending"));
      yield* Effect.fromResult(requireConversation(query.conversationId));
      // The cursor narrows the bound on the side the scan moves away from.
      const after = start.after;
      let minEntryId: number | undefined = query.minEntryId;
      let maxEntryId: number | undefined = query.maxEntryId;
      if (after !== undefined && start.order === "descending") {
        maxEntryId = Math.min(maxEntryId ?? after, after - 1);
      }
      if (after !== undefined && start.order === "ascending") {
        minEntryId = Math.max(minEntryId ?? after, after + 1);
      }
      const visible: Array<EntryRecord> = [];
      const found =
        start.order === "descending"
          ? visibleEntries(query.conversationId, minEntryId, maxEntryId)
          : visibleEntriesAscending(query.conversationId, minEntryId, maxEntryId);
      for (const value of found) {
        visible.push(value);
        if (visible.length > limit) break;
      }
      return page(visible.map(detach), limit, start.order);
    }, gate.admit),

    task: Effect.fn("AgentStorage.task")(function* (id: TaskId) {
      return Option.map(Option.fromUndefinedOr(tasks.get(id)), detach);
    }, gate.admit),

    scanTasks: Effect.fn("AgentStorage.scanTasks")(function* (
      query: TaskQuery,
      limit: number,
      cursor?: Cursor | undefined,
    ) {
      return yield* Effect.fromResult(
        scanTable(
          query.status === undefined ? taskIds : taskIdsByStatus[query.status],
          tasks,
          query.order,
          cursor,
          limit,
          (value) =>
            (query.conversationId === undefined || value.conversationId === query.conversationId) &&
            (query.kind === undefined || value.kind === query.kind) &&
            (query.abortRequested === undefined || value.abortRequested === query.abortRequested) &&
            (query.background === undefined || value.background === query.background),
        ),
      );
    }, gate.admit),

    submission: Effect.fn("AgentStorage.submission")(function* (id: SubmissionId) {
      return Option.map(Option.fromUndefinedOr(submissions.get(id)), detach);
    }, gate.admit),

    scanSubmissions: Effect.fn("AgentStorage.scanSubmissions")(function* (
      query: SubmissionQuery,
      limit: number,
      cursor?: Cursor | undefined,
    ) {
      return yield* Effect.fromResult(
        scanTable(
          query.status === undefined ? submissionIds : submissionIdsByStatus[query.status],
          submissions,
          query.order,
          cursor,
          limit,
          (value) =>
            query.conversationId === undefined || value.conversationId === query.conversationId,
        ),
      );
    }, gate.admit),

    submissionByRequest: Effect.fn("AgentStorage.submissionByRequest")(function* (
      conversationId: ConversationId,
      requestId: string,
    ) {
      const id = submissionIdsByRequest.get(conversationId)?.get(requestId);
      return Option.map(
        Option.fromUndefinedOr(id === undefined ? undefined : submissions.get(id)),
        detach,
      );
    }, gate.admit),

    findDocument: Effect.fn("AgentStorage.findDocument")(function* (
      address: DocumentAddress,
      at: DocumentPoint,
    ) {
      const index = documentAddresses.get(addressKey(address));
      if (at === "current") {
        const current = index?.currentId === undefined ? undefined : documents.get(index.currentId);
        return Option.map(Option.fromUndefinedOr(current?.record), detach);
      }
      for (const id of index?.ids ?? []) {
        const record = documents.get(id)?.record;
        if (record !== undefined && isAliveAt(record, at)) return Option.some(detach(record));
      }
      return Option.none<DocumentRecord>();
    }, gate.admit),

    document: Effect.fn("AgentStorage.document")(function* (id: DocumentId, at: DocumentPoint) {
      const stored = yield* Effect.fromResult(materialize(id, at));
      // Reads are copied out.
      return Option.map(stored, (document) => ({
        record: detach(document.record),
        version: document.version,
        value: detach(document.value),
        deltasSinceBase: document.deltasSinceBase,
      }));
    }, gate.admit),

    scanDocuments: Effect.fn("AgentStorage.scanDocuments")(function* (
      query: DocumentQuery,
      limit: number,
      cursor?: Cursor | undefined,
    ) {
      const start = yield* Effect.fromResult(scanStart("ascending", cursor, "ascending"));
      const ids = documentIdsByScope.get(scopeKey(query.scope)) ?? [];
      const values: Array<DocumentRecord> = [];
      for (const index of scanIndexes(ids, start)) {
        if (values.length > limit) break;
        const record = documents.get(ids[index] ?? 0)?.record;
        if (record === undefined || (query.kind !== undefined && record.kind !== query.kind)) {
          continue;
        }
        if (isAliveAt(record, query.at)) values.push(detach(record));
      }
      return page(values, limit, "ascending");
    }, gate.admit),
  });
});

/**
 * In-memory `AgentStorage`, the reference semantics. Writes are copied in and
 * reads are copied out. Closing the layer's scope makes
 * every later operation fail with the `Closed` reason. Use it in tests and as
 * the model for other backends.
 *
 * @category layers
 * @since 0.0.0
 */
export const layer: Layer.Layer<AgentStorage> = Layer.effect(AgentStorage, make);

/**
 * Batch validation shared by the built-in backends: global ID claims,
 * document commands, copies and delta replay. Each backend supplies only its
 * lookups.
 *
 * @internal
 */
import * as Delta from "@effect-durable-harness/delta/Delta";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Result from "effect/Result";
import type * as Schema from "effect/Schema";
import {
  type BaseContent,
  Corrupted,
  type DocumentAddress,
  type DocumentContent,
  type DocumentCopySource,
  type DocumentPoint,
  IdSpaceExhausted,
  Rejected,
  StorageFailure,
  type StorageWrite,
  type StoredDocument,
} from "../AgentStorage.ts";
import type { DocumentId } from "../Ids.ts";
import type { DocumentCreate, DocumentRecord } from "../Records.ts";

/** The table that owns a global ID. */
export type TableName = "conversation" | "entry" | "task" | "submission" | "document";

// A rejected batch and unreadable state are typed `StorageFailure` reasons,
// because the Session must observe them and become `failed`.
export const rejected = (message: string, cause?: unknown): StorageFailure =>
  new StorageFailure({ reason: new Rejected({ message, cause }) });

export const corrupted = (
  message: string,
  documentId?: DocumentId | undefined,
  cause?: unknown,
): StorageFailure => new StorageFailure({ reason: new Corrupted({ message, documentId, cause }) });

// -----------------------------------------------------------------------------
// IDs
// -----------------------------------------------------------------------------

/** The ID a write creates or replaces; document changes and retirements claim none. */
export const writeId = (write: StorageWrite): number | undefined => {
  switch (write.type) {
    case "conversation":
    case "entry":
    case "task":
    case "submission":
      return write.value.id;
    case "document.create":
    case "document.copy":
      return write.record.id;
    case "document.change":
    case "document.retire":
      return undefined;
  }
};

/** The next ID once `writes` commit: above every ID they claim. */
export const candidateNextId = (nextId: number, writes: ReadonlyArray<StorageWrite>): number => {
  let candidate = nextId;
  for (const write of writes) {
    const id = writeId(write);
    if (id !== undefined) candidate = Math.max(candidate, id + 1);
  }
  return candidate;
};

/** A fresh ID, or `IdSpaceExhausted` once no safe integer is left. */
export const mintFrom = (nextId: number): Result.Result<number, StorageFailure> =>
  Number.isSafeInteger(nextId)
    ? Result.succeed(nextId)
    : Result.fail(
        new StorageFailure({ reason: new IdSpaceExhausted({ message: "ID space is exhausted" }) }),
      );

/**
 * Conversation, entry and document IDs are written only once; task and
 * submission writes replace the whole record but never change table.
 */
export const checkGlobalIds = Effect.fnUntraced(function* <E>(
  writes: ReadonlyArray<StorageWrite>,
  existingTable: (id: number) => Effect.Effect<TableName | undefined, E>,
) {
  const claimed = new Map<number, TableName>();
  for (const write of writes) {
    if (write.type === "document.change" || write.type === "document.retire") continue;
    const document = write.type === "document.create" || write.type === "document.copy";
    const table: TableName = document ? "document" : write.type;
    const id = document ? write.record.id : write.value.id;
    const existing = yield* existingTable(id);
    const earlier = claimed.get(id);
    if (table === "conversation" || table === "entry" || table === "document") {
      if (existing !== undefined) return yield* rejected(`ID ${id} already belongs to ${existing}`);
      if (earlier !== undefined) return yield* rejected(`ID ${id} is written more than once`);
    } else {
      if (existing !== undefined && existing !== table) {
        return yield* rejected(`ID ${id} already belongs to ${existing}`);
      }
      if (earlier !== undefined && earlier !== table) {
        return yield* rejected(`ID ${id} is written as two record types`);
      }
    }
    claimed.set(id, table);
  }
});

// -----------------------------------------------------------------------------
// documents
// -----------------------------------------------------------------------------

/** Lifetimes are half-open: alive from `createdAt` until `retiredAt`. */
export const isAliveAt = (record: DocumentRecord, at: DocumentPoint): boolean => {
  if (at === "current") return record.retiredAt === undefined;
  return record.createdAt <= at && (record.retiredAt === undefined || at < record.retiredAt);
};

/** Session, task and `latest` conversation documents keep only current state. */
export const isCurrentOnly = (record: DocumentRecord | DocumentCreate): boolean =>
  !("history" in record) || record.history === "latest";

/** History and fork of a conversation document; absent for other scopes. */
const retention = (record: DocumentRecord | DocumentCreate): string | undefined =>
  "history" in record ? `${record.history}/${record.fork}` : undefined;

/** The owner column of a scope: 0 for the Session, else the conversation or task ID. */
export const scopeOwner = (scope: DocumentRecord["scope"]): number => {
  switch (scope.kind) {
    case "session":
      return 0;
    case "conversation":
      return scope.conversationId;
    case "task":
      return scope.taskId;
  }
};

/** One string per exact logical address; a missing key is the singleton. */
export const addressKey = (address: DocumentAddress | DocumentCreate | DocumentRecord): string =>
  JSON.stringify([
    address.kind,
    address.scope.kind,
    scopeOwner(address.scope),
    address.key === undefined ? null : ["family", address.key],
  ]);

/** The document commands of one batch for one incarnation. */
export interface DocumentAction {
  create?: DocumentCreate | undefined;
  copy?: DocumentCopySource | undefined;
  content?: DocumentContent | undefined;
  retire: boolean;
}

/**
 * Group a batch's document commands by incarnation: at most one content
 * command (create, copy or change) plus an optional retirement. A copy whose
 * source the same batch touches is rejected.
 */
export const prepareDocumentActions = (
  writes: ReadonlyArray<StorageWrite>,
): Result.Result<Map<DocumentId, DocumentAction>, StorageFailure> => {
  const actions = new Map<DocumentId, DocumentAction>();
  for (const write of writes) {
    if (
      write.type !== "document.create" &&
      write.type !== "document.copy" &&
      write.type !== "document.change" &&
      write.type !== "document.retire"
    ) {
      continue;
    }
    const id =
      write.type === "document.create" || write.type === "document.copy"
        ? write.record.id
        : write.id;
    let action = actions.get(id);
    if (action === undefined) {
      action = { retire: false };
      actions.set(id, action);
    }
    const hasContent = action.content !== undefined || action.copy !== undefined;
    switch (write.type) {
      case "document.create":
        if (action.create !== undefined || hasContent) {
          return Result.fail(rejected(`Document ${id} has more than one content command`));
        }
        action.create = write.record;
        action.content = write.content;
        break;
      case "document.copy":
        if (action.create !== undefined || hasContent) {
          return Result.fail(rejected(`Document ${id} has more than one content command`));
        }
        action.create = write.record;
        action.copy = write.source;
        break;
      case "document.change":
        if (hasContent)
          return Result.fail(rejected(`Document ${id} has more than one content command`));
        action.content = write.content;
        break;
      case "document.retire":
        if (action.retire) return Result.fail(rejected(`Document ${id} is retired more than once`));
        action.retire = true;
        break;
    }
  }
  for (const [id, action] of actions) {
    if (action.copy !== undefined && actions.has(action.copy.id)) {
      return Result.fail(
        rejected(
          `Document copy ${id} was rejected: fork source document ${action.copy.id} is changed in the copy batch`,
        ),
      );
    }
  }
  return Result.succeed(actions);
};

/** What the action checks need to know about a stored incarnation. */
export interface ExistingDocument {
  readonly record: DocumentRecord;
  /** Version of the newest stored revision; absent once every revision was reclaimed. */
  readonly lastVersion: number | undefined;
}

/** Backend lookups against pre-batch committed state. */
export interface DocumentLookups<E> {
  readonly existing: (id: DocumentId) => Effect.Effect<Option.Option<ExistingDocument>, E>;
  readonly hasCurrent: (address: DocumentCreate | DocumentRecord) => Effect.Effect<boolean, E>;
}

/**
 * Check a batch's document actions against committed state: creates are new,
 * changes target a current incarnation, a delta keeps the stored version
 * and an address ends with at most one current incarnation.
 */
export const checkDocumentActions = Effect.fnUntraced(function* <E>(
  actions: ReadonlyMap<DocumentId, DocumentAction>,
  lookups: DocumentLookups<E>,
) {
  const liveCounts = new Map<string, number>();
  for (const [id, action] of actions) {
    const existing = Option.getOrUndefined(yield* lookups.existing(id));
    if (action.create === undefined && existing === undefined) {
      return yield* rejected(`Unknown document: ${id}`);
    }
    if (action.create !== undefined && existing !== undefined) {
      return yield* rejected(`Document ${id} already exists`);
    }
    if (existing?.record.retiredAt !== undefined)
      return yield* rejected(`Document ${id} is retired`);
    if (action.content?.kind === "delta") {
      if (existing?.lastVersion === undefined)
        return yield* rejected(`Document ${id} delta has no base`);
      if (existing.lastVersion !== action.content.version) {
        return yield* rejected(`Document ${id} version transition requires a base`);
      }
    }
    const record = action.create ?? existing?.record;
    if (record === undefined) continue;
    const key = addressKey(record);
    let live = liveCounts.get(key);
    if (live === undefined) live = (yield* lookups.hasCurrent(record)) ? 1 : 0;
    if (action.retire && existing !== undefined) live--;
    if (action.create !== undefined && !action.retire) live++;
    liveCounts.set(key, live);
  }
  for (const live of liveCounts.values()) {
    if (live > 1) return yield* rejected("Document address already has a current incarnation");
  }
});

/**
 * A copy starts from one independent base at the source's
 * stored value and version, when the source matches the child's kind, key,
 * history and fork.
 */
export const copyContent = (
  id: DocumentId,
  source: DocumentCopySource,
  stored: Option.Option<StoredDocument>,
  create: DocumentCreate,
): Result.Result<BaseContent, StorageFailure> => {
  if (Option.isNone(stored)) {
    return Result.fail(
      rejected(
        `Document copy ${id} was rejected: fork source document ${source.id} cannot be read`,
      ),
    );
  }
  const record = stored.value.record;
  if (
    record.scope.kind !== "conversation" ||
    create.scope.kind !== "conversation" ||
    record.kind !== create.kind ||
    record.key !== create.key ||
    retention(record) !== retention(create)
  ) {
    return Result.fail(
      rejected(
        `Document copy ${id} was rejected: fork source document ${source.id} does not match the copied record`,
      ),
    );
  }
  return Result.succeed({ kind: "base", version: stored.value.version, value: stored.value.value });
};

/** One stored revision after the selected base. */
export interface TailRevision {
  readonly kind: DocumentContent["kind"];
  readonly version: number;
  readonly ops: Delta.Delta;
}

/**
 * Replay a delta tail over its base without mutating either. A version change
 * inside the tail or an operation that cannot be applied is corruption.
 */
export const replay = (
  id: DocumentId,
  base: { readonly version: number; readonly value: Schema.JsonObject },
  tail: ReadonlyArray<TailRevision>,
): Result.Result<Schema.JsonObject, StorageFailure> => {
  for (const revision of tail) {
    if (revision.kind !== "delta" || revision.version !== base.version) {
      return Result.fail(
        corrupted(`Document ${id} crosses a stored version boundary without a base`, id),
      );
    }
  }
  const replayed = Delta.applyImmutableBatches(
    base.value,
    tail.map((revision) => revision.ops),
  );
  return Result.isSuccess(replayed)
    ? Result.succeed(replayed.success)
    : Result.fail(
        corrupted(
          `Document ${id} cannot be replayed: ${replayed.failure.message}`,
          id,
          replayed.failure,
        ),
      );
};

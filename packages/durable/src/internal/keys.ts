// The keys of the Session line, what a commit writes and a job reads, so a read of an unpublished write waits for
// it.
import * as Effect from "effect/Effect";
import type { StorageWrite } from "../AgentStorage.ts";
import type { ConversationId, DocumentId, EntryId, SubmissionId, TaskId } from "../Ids.ts";
import type { DocumentRecord } from "../Records.ts";
import type { SessionFailed } from "../SessionError.ts";
import { addressKey } from "./commit.ts";
import type { GuardedStorage } from "./transaction.ts";

/** A task record. */
export const taskKey = (id: TaskId): string => `task:${id}`;

/** A conversation record. */
export const conversationKey = (id: ConversationId): string => `conv:${id}`;

/** An entry record. */
export const entryKey = (id: EntryId): string => `entry:${id}`;

/** The newest entries of a conversation: appended blind, read by scans, the newest entry, and head markers. */
export const appendKey = (id: ConversationId): string => `entry-append:${id}`;

/** A submission record. */
export const submissionKey = (id: SubmissionId): string => `sub:${id}`;

/** The submission a conversation's request ID names. */
export const requestKey = (conversationId: ConversationId, requestId: string): string =>
  `req:${conversationId}:${requestId}`;

/** A document address (`addressKey`): its current incarnation, value, and held changes. */
export const documentKey = (addressId: string): string => `doc:${addressId}`;

/** A document incarnation, for reads by its ID. */
export const incarnationKey = (id: DocumentId): string => `docid:${id}`;

/** The incarnations alive in a scope: created and retired blind, read by scans and the task-document index. */
export const scopeKey = (scope: DocumentRecord["scope"]): string => {
  switch (scope.kind) {
    case "session":
      return "docscope:session";
    case "conversation":
      return `docscope:c${scope.conversationId}`;
    case "task":
      return `docscope:t${scope.taskId}`;
  }
};

/**
 * A node of the scheduler's ownership tree (a task ID, or `c<conversation ID>`): the live work listed below it.
 * Written by task creation and a task becoming terminal for every node from the task's parent up to and including
 * the nearest live task or the root conversation; read by every walk that expands the node.
 */
export const ownedKey = (node: TaskId | string): string => `owned:${node}`;

/** The ownerless conversations with live work below them, which global idle walks from. */
export const ROOTS_KEY = "roots";

/** The live tasks with cancellation intent: abort marks, and held outcomes other than `completed`. */
export const INTENT_KEY = "intent";

/** Every unpublished write: what a read that cannot be keyed, such as a scan, touches. */
export const ALL_KEYS = "*";

/** The keys of the table records in `writes`; document writes are keyed by the transaction, which knows addresses. */
export const tableKeys = (writes: ReadonlyArray<StorageWrite>, keys: Array<string>): void => {
  for (const write of writes) {
    switch (write.type) {
      case "conversation":
        keys.push(conversationKey(write.value.id));
        break;
      case "entry":
        keys.push(entryKey(write.value.id), appendKey(write.value.conversationId));
        break;
      case "task":
        keys.push(taskKey(write.value.id));
        break;
      case "submission":
        keys.push(submissionKey(write.value.id));
        if (write.value.requestId !== undefined) {
          keys.push(requestKey(write.value.conversationId, write.value.requestId));
        }
        break;
      default:
        break;
    }
  }
};

/** Waits while `key` has an unpublished writer; `undefined` when there is nothing to wait for. */
export type Touch = (key: string) => Effect.Effect<void, SessionFailed> | undefined;

/**
 * The storage a job on the line reads through. Every read first touches its
 * key, which waits while the key has an unpublished writer, so the read sees what the line admitted before it. Scans
 * that cannot be keyed touch `ALL_KEYS`.
 */
export const gated = (storage: GuardedStorage, touch: Touch): GuardedStorage => {
  const after = <A, E>(
    key: string,
    read: Effect.Effect<A, E>,
  ): Effect.Effect<A, E | SessionFailed> =>
    Effect.suspend(() => {
      const gate = touch(key);
      return gate === undefined ? read : Effect.andThen(gate, read);
    });
  /** A read of a conversation's entries waits for its creation too: storage refuses an unknown conversation. */
  const afterEntries = <A, E>(conversationId: ConversationId, read: Effect.Effect<A, E>) =>
    after(conversationKey(conversationId), after(appendKey(conversationId), read));
  return {
    mintId: storage.mintId,
    conversation: (id) => after(conversationKey(id), storage.conversation(id)),
    scanConversations: (query, limit, cursor) =>
      after(ALL_KEYS, storage.scanConversations(query, limit, cursor)),
    entry: (id) => after(entryKey(id), storage.entry(id)),
    // Visibility follows the conversation's immutable ancestry; the conversation may be unpublished itself.
    visibleEntry: (conversationId, id) =>
      after(
        conversationKey(conversationId),
        after(entryKey(id), storage.visibleEntry(conversationId, id)),
      ),
    findLatestHeadMarker: (conversationId, atOrBefore) =>
      afterEntries(conversationId, storage.findLatestHeadMarker(conversationId, atOrBefore)),
    scanEntries: (query, limit, cursor) =>
      afterEntries(query.conversationId, storage.scanEntries(query, limit, cursor)),
    task: (id) => after(taskKey(id), storage.task(id)),
    scanTasks: (query, limit, cursor) => after(ALL_KEYS, storage.scanTasks(query, limit, cursor)),
    submission: (id) => after(submissionKey(id), storage.submission(id)),
    scanSubmissions: (query, limit, cursor) =>
      after(ALL_KEYS, storage.scanSubmissions(query, limit, cursor)),
    submissionByRequest: (conversationId, requestId) =>
      after(
        requestKey(conversationId, requestId),
        storage.submissionByRequest(conversationId, requestId),
      ),
    findDocument: (address, at) =>
      after(documentKey(addressKey(address)), storage.findDocument(address, at)),
    document: (id, at) => after(incarnationKey(id), storage.document(id, at)),
    scanDocuments: (query, limit, cursor) =>
      after(scopeKey(query.scope), storage.scanDocuments(query, limit, cursor)),
  };
};

// Select every persisted conversation document a fork copies.
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import type {
  Cursor,
  DocumentCopySource,
  DocumentPoint,
  StorageFailure,
  StorageRequestError,
} from "../AgentStorage.ts";
import { type ConversationId, DocumentId, type EntryId } from "../Ids.ts";
import type { DocumentCreate, DocumentRecord } from "../Records.ts";
import { CommitRejected, ForkConflict, NotFound, type SessionFailed } from "../SessionError.ts";
import { addressKey } from "./commit.ts";
import type { GuardedStorage } from "./transaction.ts";

const SCAN_PAGE_SIZE = 256;

/** One definition-free document copy created with a forked conversation. */
export interface ForkDocumentCopy {
  readonly record: DocumentCreate;
  readonly source: DocumentCopySource;
}

type ForkError = CommitRejected | StorageFailure | StorageRequestError | SessionFailed;

/** Selects every persisted conversation document copied by one fork. */
export const prepareForkDocumentCopies = Effect.fnUntraced(function* (
  storage: GuardedStorage,
  parentConversationId: ConversationId,
  at: EntryId,
  childConversationId: ConversationId,
) {
  const entry = yield* storage.visibleEntry(parentConversationId, at);
  if (Option.isNone(entry)) {
    return yield* new CommitRejected({
      reason: new NotFound({
        message: `Entry ${at} is not visible from conversation ${parentConversationId}`,
      }),
    });
  }
  const copies: Array<ForkDocumentCopy> = [];
  const copied = new Set<string>();
  // `asOf` documents are read where the entry lives, at its commit; `current`
  // ones from the parent itself, now.
  yield* collectCopies(
    storage,
    entry.value.entry.conversationId,
    entry.value.commitSeq,
    "asOf",
    childConversationId,
    copies,
    copied,
  );
  yield* collectCopies(
    storage,
    parentConversationId,
    "current",
    "current",
    childConversationId,
    copies,
    copied,
  );
  return copies;
});

const collectCopies = (
  storage: GuardedStorage,
  conversationId: ConversationId,
  at: DocumentPoint,
  policy: "asOf" | "current",
  childConversationId: ConversationId,
  copies: Array<ForkDocumentCopy>,
  copied: Set<string>,
): Effect.Effect<void, ForkError> =>
  Effect.gen(function* () {
    let cursor: Option.Option<Cursor> = Option.none();
    do {
      const page = yield* storage.scanDocuments(
        { scope: { kind: "conversation", conversationId }, at },
        SCAN_PAGE_SIZE,
        Option.getOrUndefined(cursor),
      );
      for (const source of page.items) {
        if (!("history" in source) || source.fork !== policy) continue;
        const id = DocumentId.make(yield* storage.mintId);
        const record = copyRecord(source, id, childConversationId);
        const copyAddress = addressKey(record);
        if (copied.has(copyAddress)) {
          const member = record.key === undefined ? record.kind : `${record.kind}/${record.key}`;
          return yield* new CommitRejected({
            reason: new ForkConflict({
              message: `Fork selects multiple source documents for ${member}`,
            }),
          });
        }
        copied.add(copyAddress);
        copies.push({ record, source: { id: source.id, at } });
      }
      cursor = page.next;
    } while (Option.isSome(cursor));
  });

const copyRecord = (
  source: Extract<DocumentRecord, { readonly history: string }>,
  id: DocumentId,
  conversationId: ConversationId,
): DocumentCreate => {
  const scope = { kind: "conversation" as const, conversationId };
  const record: DocumentCreate =
    source.history === "latest"
      ? { id, kind: source.kind, scope, history: "latest", fork: source.fork }
      : { id, kind: source.kind, scope, history: "rewindable", fork: source.fork };
  return source.key === undefined ? record : { ...record, key: source.key };
};

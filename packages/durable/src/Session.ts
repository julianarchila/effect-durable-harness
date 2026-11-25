/**
 * The Session kernel.
 *
 * A Session owns one mutation line over one `AgentStorage`. Every commit runs
 * its callback against a `Tx`, prepares every touched document, writes one
 * atomic batch and only then adopts and publishes the new revisions: only
 * committed state is observable. Documents load lazily into a use-bounded
 * cache, migrate on access and are observed through `snapshot`,
 * `snapshotAsOf` and `watchDoc`. A storage failure fails the
 * whole Session (`SessionFailed`).
 *
 * @since 0.0.0
 */
import type * as Delta from "@effect-durable-harness/delta/Delta";
import * as Context from "effect/Context";
import type * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import type * as Option from "effect/Option";
import type * as Schema from "effect/Schema";
import type * as Scope from "effect/Scope";
import type {
  AgentStorage,
  DocumentCopySource,
  StorageFailure,
  StorageRequestError,
  StorageWrite,
} from "./AgentStorage.ts";
import type {
  ConversationDoc,
  ConversationDocFamily,
  DocumentWatch,
  RewindableConversationDoc,
  RewindableConversationDocFamily,
  SessionDoc,
  SessionDocFamily,
  TaskDoc,
  TaskDocFamily,
} from "./Doc.ts";
import type { ConversationId, EntryId, Seq, TaskId } from "./Ids.ts";
import * as internal from "./internal/session.ts";
import type { CommitObservation, CommitObserver, Observation, Observer } from "./Observer.ts";
import type { DocumentRecord } from "./Records.ts";
import type {
  CommitRejected,
  DocumentAccessError,
  ReadOnlySession,
  SessionClosed,
  SessionFailed,
} from "./SessionError.ts";
import type { Tx } from "./Tx.ts";

// -----------------------------------------------------------------------------
// publications
// -----------------------------------------------------------------------------

/**
 * A committed change of one document incarnation.
 *
 * @category publications
 * @since 0.0.0
 */
export type DocumentCommitChange =
  | {
      readonly type: "document";
      readonly record: DocumentRecord;
      /** Conversation owning the document; task documents derive it from their task. Absent for Session documents. */
      readonly conversationId: ConversationId | undefined;
      /** Definition version of `value`; absent when this commit retired the incarnation. */
      readonly version: number | undefined;
      /** Exact adopted immutable revision, or `null` when this commit retired the incarnation. */
      readonly value: Schema.JsonObject | null;
      /** Exact adopted operations for an ordinary update; empty for creation and retirement. */
      readonly ops: Delta.Delta;
    }
  | {
      /** A definition-free fork copy; observers hydrate it through a state or watch acquisition. */
      readonly type: "document.copy";
      readonly record: DocumentRecord;
      readonly conversationId: ConversationId;
      readonly source: DocumentCopySource;
    };

/**
 * A complete table record committed without another copy.
 *
 * @category publications
 * @since 0.0.0
 */
export type TableCommitChange = Extract<
  StorageWrite,
  { readonly type: "conversation" | "entry" | "task" | "submission" }
>;

/**
 * One change of a commit publication.
 *
 * @category publications
 * @since 0.0.0
 */
export type CommitChange = TableCommitChange | DocumentCommitChange;

/**
 * Every immutable change of one successful commit. Change order is
 * unspecified.
 *
 * @category publications
 * @since 0.0.0
 */
export interface CommitPublication {
  readonly seq: Seq;
  readonly changes: ReadonlyArray<CommitChange>;
}

/**
 * How a Session ended: closed, or failed with the first storage error.
 *
 * @category models
 * @since 0.0.0
 */
export type SessionEnd =
  | { readonly reason: "closed" }
  | { readonly reason: "failed"; readonly error: StorageFailure | Error };

// -----------------------------------------------------------------------------
// reads
// -----------------------------------------------------------------------------

/**
 * Errors of a Session document read.
 *
 * @category errors
 * @since 0.0.0
 */
export type ReadError =
  | DocumentAccessError
  | StorageRequestError
  | StorageFailure
  | SessionFailed
  | SessionClosed;

/**
 * Errors a commit adds to its callback's own errors. `ReadOnlySession` comes only from a read-only Harness's
 * commit with writes.
 *
 * @category errors
 * @since 0.0.0
 */
export type CommitError =
  | CommitRejected
  | StorageFailure
  | SessionFailed
  | SessionClosed
  | ReadOnlySession;

/**
 * How a commit is stored. `immediate`, the default, stores it before
 * it resolves. `deferred` is a permission: a commit that only changes existing `latest`, `initial`-fork
 * conversation documents at their stored version is adopted in memory, resolves and is shown to watches at once,
 * and is stored with the next commit that reads or writes the document, before the document leaves memory, at
 * close, or within `withinMs` milliseconds (default `SessionOptions.persistWithin`, or the Harness's
 * `settings.progress.persistWithinMs`; `Infinity` waits for one of the others). Any other write makes it
 * immediate. A crash loses what is held.
 *
 * **Gotchas**
 *
 * - A `withinMs` that is not a number of milliseconds, `0` or more, dies with a `TypeError`.
 * - `subscribeCommits` sees a deferred commit only once stored, with exactly what was stored.
 *
 * @category models
 * @since 0.0.0
 */
export type CommitPersistence =
  | { readonly kind: "immediate" }
  | { readonly kind: "deferred"; readonly withinMs?: number | undefined };

/**
 * Options of a commit.
 *
 * @category models
 * @since 0.0.0
 */
export interface CommitOptions {
  readonly persistence?: CommitPersistence | undefined;
}

type Snapshot<T, E> = Effect.Effect<Option.Option<Readonly<T>>, E>;

/**
 * `snapshot`: the committed value of one address, never created.
 *
 * @category models
 * @since 0.0.0
 */
export interface DocSnapshot<E = ReadError> {
  <T>(doc: SessionDoc<T>): Snapshot<T, E>;
  <T>(doc: ConversationDoc<T>, conversationId: ConversationId): Snapshot<T, E>;
  <T>(doc: TaskDoc<T>, taskId: TaskId): Snapshot<T, E>;
  <T, I>(doc: SessionDocFamily<T, I>, key: string): Snapshot<T, E>;
  <T, I>(
    doc: ConversationDocFamily<T, I>,
    conversationId: ConversationId,
    key: string,
  ): Snapshot<T, E>;
  <T, I>(doc: TaskDocFamily<T, I>, taskId: TaskId, key: string): Snapshot<T, E>;
}

/**
 * `snapshotAsOf`: a rewindable conversation document as of a visible entry.
 *
 * @category models
 * @since 0.0.0
 */
export interface DocSnapshotAsOf<E = ReadError> {
  <T>(
    doc: RewindableConversationDoc<T>,
    conversationId: ConversationId,
    at: EntryId,
  ): Snapshot<T, E>;
  <T, I>(
    doc: RewindableConversationDocFamily<T, I>,
    conversationId: ConversationId,
    key: string,
    at: EntryId,
  ): Snapshot<T, E>;
}

type WatchOf<T, E> = Effect.Effect<Option.Option<DocumentWatch<T>>, E, Scope.Scope>;

/**
 * `watchDoc`: a watch that follows the address of one existing incarnation,
 * stopped with its scope; `None` when the address is absent.
 *
 * @category models
 * @since 0.0.0
 */
export interface DocWatchAcquire<E = ReadError> {
  <T>(doc: SessionDoc<T>): WatchOf<T, E>;
  <T>(doc: ConversationDoc<T>, conversationId: ConversationId): WatchOf<T, E>;
  <T>(doc: TaskDoc<T>, taskId: TaskId): WatchOf<T, E>;
  <T, I>(doc: SessionDocFamily<T, I>, key: string): WatchOf<T, E>;
  <T, I>(
    doc: ConversationDocFamily<T, I>,
    conversationId: ConversationId,
    key: string,
  ): WatchOf<T, E>;
  <T, I>(doc: TaskDocFamily<T, I>, taskId: TaskId, key: string): WatchOf<T, E>;
}

type ObserveOf<T, E> = Effect.Effect<Option.Option<Observation<T>>, E>;

/**
 * `observeDoc`: attaches a synchronous `Observer` to the address of one
 * existing incarnation, on the Session line; `None` when the address is
 * absent. The attaching Effect governs only the attachment: the observer stays
 * attached until it detaches or ends.
 *
 * @category models
 * @since 0.0.0
 */
export interface DocObserveAttach<E = ReadError> {
  <T>(doc: SessionDoc<T>, observer: Observer<T>): ObserveOf<T, E>;
  <T>(
    doc: ConversationDoc<T>,
    conversationId: ConversationId,
    observer: Observer<T>,
  ): ObserveOf<T, E>;
  <T>(doc: TaskDoc<T>, taskId: TaskId, observer: Observer<T>): ObserveOf<T, E>;
  <T, I>(doc: SessionDocFamily<T, I>, key: string, observer: Observer<T>): ObserveOf<T, E>;
  <T, I>(
    doc: ConversationDocFamily<T, I>,
    conversationId: ConversationId,
    key: string,
    observer: Observer<T>,
  ): ObserveOf<T, E>;
  <T, I>(
    doc: TaskDocFamily<T, I>,
    taskId: TaskId,
    key: string,
    observer: Observer<T>,
  ): ObserveOf<T, E>;
}

// -----------------------------------------------------------------------------
// service
// -----------------------------------------------------------------------------

/**
 * Options of `Session.layer`.
 *
 * @category models
 * @since 0.0.0
 */
export interface SessionOptions {
  /**
   * How long a document no watch observes stays loaded after its last use; read synchronously
   * at each use. Default ten minutes; zero evicts right after use;
   * `Duration.infinity` keeps documents; a negative value, or a reader that throws, means the default.
   */
  readonly documentRetention?: (() => Duration.Duration) | undefined;
  /**
   * The default window of a deferred commit (`CommitPersistence`), read at each one. Default one second; `Duration.infinity` waits for a commit that reads or writes the
   * document, or close; a negative value, or a reader that throws, means the default. A reader of a `Duration`,
   * as `documentRetention` is: the Harness reads its settings at each commit.
   */
  readonly persistWithin?: (() => Duration.Duration) | undefined;
}

/**
 * The Session kernel over the `AgentStorage` in context.
 *
 * @category services
 * @since 0.0.0
 */
// Cancellation is interruption; lookups return `Option`; watches are released
// with their `Scope`, while observers stay attached until they detach; there is
// no `close()`, because closing is the end of the layer's scope.
export class Session extends Context.Service<
  Session,
  {
    /**
     * Runs `change` on the mutation line and commits its writes atomically. Nothing is published before
     * storage succeeds, unless `options.persistence` defers it (`CommitPersistence`); once the batch is
     * admitted, interrupting the caller does not interrupt it.
     */
    readonly commit: <A, E>(
      change: Effect.Effect<A, E, Tx>,
      options?: CommitOptions,
    ) => Effect.Effect<A, E | CommitError>;
    /** The committed value of one address; never creates and never persists a migration. */
    readonly snapshot: DocSnapshot;
    /** A rewindable conversation document as of a visible entry, through the conversation's ancestry. */
    readonly snapshotAsOf: DocSnapshotAsOf;
    /** A watch of the address of one existing incarnation, stopped with the scope. */
    readonly watchDoc: DocWatchAcquire;
    /**
     * Attaches a synchronous observer of one address: it gets each change there as
     * memory adopts it, unfolded, and ends `Ended` after its task's retirement for a task document, `Failed` when
     * the Session closes or fails.
     */
    readonly observeDoc: DocObserveAttach;
    /**
     * Attaches a synchronous observer of every stored commit, with exactly what was stored;
     * it ends `Failed` after the Session's last publication. A deferred commit reaches it only with the commit or
     * deadline that stores it.
     */
    readonly observeCommits: (
      observer: CommitObserver,
    ) => Effect.Effect<CommitObservation, SessionFailed | SessionClosed>;
    /** Settles once the Session has ended. */
    readonly closed: Effect.Effect<SessionEnd>;
  }
>()("@effect-durable-harness/core/Session") {}

/**
 * The Session over the `AgentStorage` in context. Closing the layer's scope
 * closes the Session: it seals admission, ends every watch, and
 * lets admitted commits settle. Storage is closed by its own layer.
 *
 * @category layers
 * @since 0.0.0
 */
// A layer over the `AgentStorage` in context; it does not close that storage.
export const layer = (options?: SessionOptions): Layer.Layer<Session, never, AgentStorage> =>
  Layer.effect(
    Session,
    Effect.map(internal.make(options ?? {}), (kernel) => Session.of(internal.serviceOf(kernel))),
  );

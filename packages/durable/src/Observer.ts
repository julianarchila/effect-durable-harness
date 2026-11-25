/**
 * Synchronous observers of committed state (`Observer`, `CommitObserver`):
 * the callbacks an adapter, such as a replication feed or
 * a client bridge, attaches with `Session.observeDoc`,
 * `Session.observeCommits` or `Conversation.observe`. Watches are built on the
 * same followers inside the Session.
 *
 * @since 0.0.0
 */
import type { WatchFrame } from "./Doc.ts";
import type { CommitPublication } from "./Session.ts";
import type { SessionClosed, SessionFailed } from "./SessionError.ts";

/**
 * How an observer ended. `Ended`: the owner of what it
 * observes ended, such as a task document's task. `Failed`: the Session
 * closed (`SessionClosed`, so an adapter may follow the next Session) or
 * failed (`SessionFailed`). `ListenerError`: its own `advance` threw, or the
 * Effect it returned failed.
 *
 * @category models
 * @since 0.0.0
 */
export type ObserverEnd =
  | { readonly _tag: "Ended" }
  | { readonly _tag: "Failed"; readonly error: SessionFailed | SessionClosed }
  | { readonly _tag: "ListenerError"; readonly error: Error };

/**
 * A synchronous observer of one document address or one conversation view.
 * `advance` gets every change, unfolded, as memory adopts it: a document observer one call per change at its address (two for a
 * commit that retires and recreates it), a view observer at most one per
 * commit. Its ops apply from `Observation.value`, and a call may come before
 * the attaching Effect returns.
 *
 * `advance` runs synchronously inside the commit's publication, so it must
 * only record the change: no waiting and no Session calls. A throw is
 * reported as `listener_error`, detaches the observer and ends it
 * `ListenerError`; so does a returned Effect that fails, which the Session
 * runs in the background. `end` is called once, and never after `detach`.
 *
 * @category models
 * @since 0.0.0
 */
export interface Observer<T> {
  /** One change; synchronous, at the publication of the commit that made it. */
  readonly advance: (change: WatchFrame<T>) => void;
  /** The observer's end, once. */
  readonly end: (end: ObserverEnd) => void;
}

/**
 * An attached `Observer`: the value at attachment, which the first `advance`
 * starts from, and how to detach it.
 *
 * @category models
 * @since 0.0.0
 */
// `detach` is a synchronous function, not an Effect, because observers detach from inside their own synchronous
// `advance`.
export interface Observation<T> {
  readonly value: Readonly<T>;
  /** Idempotent: no `advance` or `end` follows it, not even for the rest of a commit underway. */
  readonly detach: () => void;
}

/**
 * A synchronous observer of every stored commit:
 * `advance` gets each `CommitPublication` at its publication, after the
 * Session's own components and before the waits the commit resolves; `end`
 * gets the Session's close, after its last publication. A throw is handled as
 * for `Observer`.
 *
 * @category models
 * @since 0.0.0
 */
export interface CommitObserver {
  /** One stored commit; synchronous, at its publication. */
  readonly advance: (publication: CommitPublication) => void;
  /** The observer's end, once: `Failed` at the Session's close or failure, or `ListenerError`. */
  readonly end: (end: ObserverEnd) => void;
}

/**
 * An attached `CommitObserver`.
 *
 * @category models
 * @since 0.0.0
 */
export interface CommitObservation {
  /** Idempotent: no `advance` or `end` follows it. */
  readonly detach: () => void;
}

// The task graph view.
//
// At most one mount, built on the Session line by its first observer and dropped with its last. It advances
// synchronously inside the Session's stored commit publications, so it shows only committed state, and on the
// line outside any commit when a blocked reason changes without one. Watches over it reuse the document watch
// (`internal/observation.ts`), and host observers the guard, as the conversation views do (`internal/view.ts`).
//
// `make` returns its two operations and registers its listeners on the kernel. Refreshing blocked reasons is a
// `Signal` loop (a deferred pass), forked into the Harness's fiber scope.
import * as Delta from "@effect-durable-harness/delta/Delta";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Result from "effect/Result";
import * as Scope from "effect/Scope";
import type { WatchFrame } from "../Doc.ts";
import type { BlockedReason, HarnessError } from "../Harness.ts";
import type { ConversationId } from "../Ids.ts";
import type { Observation, Observer } from "../Observer.ts";
import type { SignalKeyRecord, TaskRecord } from "../Records.ts";
import { SessionClosed, type SessionFailed } from "../SessionError.ts";
import type { TaskGraph, TaskGraphNode, TaskGraphState, TaskGraphWatch } from "../TaskGraph.ts";
import { type BlockedSubject, blockedSubject, phaseOf } from "./decisions.ts";
import {
  type Follower,
  type Guarded,
  guardedFollower,
  guardObserver,
  watchObserver,
} from "./observation.ts";
import { LISTENER, type Reporter } from "./report.ts";
import { scanAll } from "./scan.ts";
import type { ObservedCommit, SessionKernel } from "./session.ts";
import * as Signal from "./signal.ts";

const SCAN_PAGE_SIZE = 256;

type Mutable<T> = { -readonly [K in keyof T]: T[K] };
const LIVE_STATUSES = ["pending", "running", "waiting", "sleeping", "completing"] as const;

/** The Harness's task graph mount. */
export interface TaskGraphs {
  /** The graph as a watch, released with the scope. */
  readonly watch: Effect.Effect<TaskGraphWatch, HarnessError, Scope.Scope>;
  /**
   * Attaches a host's synchronous observer behind `guardObserver`. The attaching fiber governs
   * only the attachment: interrupted after it attached, it detaches.
   */
  readonly observe: (
    observer: Observer<TaskGraph>,
  ) => Effect.Effect<Observation<TaskGraph>, HarnessError>;
}

/** What the graph asks the scheduler: a task's blocked reason, and when reasons may change without a commit. */
export interface TaskGraphBlocking {
  readonly blocked: (subject: BlockedSubject) => BlockedReason | undefined;
  readonly subscribeBlocked: (listener: () => void) => () => void;
}

export interface TaskGraphsOptions {
  readonly kernel: SessionKernel;
  readonly blocking: TaskGraphBlocking;
  /** Receives an observer's throw; the other observers still get the revision. */
  readonly report: Reporter;
  /** Where the blocked refresh loop runs; closed with the Harness. */
  readonly scope: Scope.Scope;
}

/** What the mount drives on each of its observers: a watch or a guarded host observer. */
type GraphObserver = Pick<Follower<TaskGraph>, "advance" | "closeSession">;

interface Mount {
  value: TaskGraph;
  readonly observers: Set<GraphObserver>;
  /** What the blocked reason reads of each live task, so a refresh can derive it again without payloads. */
  readonly records: Map<string, BlockedSubject>;
}

/**
 * Registers the mount's commit listener, the scheduler's blocked listener and the close listener on the kernel
 * Call it after the submissions registered theirs and before the views, so listeners run in that order.
 */
export const make = (
  options: TaskGraphsOptions,
): Effect.Effect<TaskGraphs, SessionFailed | SessionClosed> =>
  Effect.gen(function* () {
    const kernel = options.kernel;
    const blocking = options.blocking;
    let mount: Mount | undefined;
    let closed = false;
    const report = (error: Error) => options.report(Cause.die(error), LISTENER);

    // The services an observer's returned Effect runs with: the Harness's, as for the views.
    const services = yield* Effect.context<never>();
    const guardServices = {
      report,
      runReturned: (effect: Effect.Effect<void>) => {
        Effect.runForkWith(services)(effect);
      },
    };

    yield* kernel.followCommits((publication) => {
      // A commit held in memory changes documents only, which the graph does not show.
      if (mount === undefined || publication.kind === "memory") return;
      advance(mount, publication, blocking.blocked, report);
    });

    // On the line, derive every node's blocked reason again and
    // publish the ones that changed, outside any commit.
    const refresh = Signal.make();
    blocking.subscribeBlocked(() => {
      if (mount !== undefined && !closed) refresh.fire();
    });
    yield* Signal.forkLoop(
      refresh,
      kernel
        .readOnLine(
          Effect.sync(() => {
            const current = mount;
            if (current === undefined || closed) return;
            const ops: Array<Delta.Operation> = [];
            for (const [key, subject] of current.records) {
              const blocked = blocking.blocked(subject);
              if (blocked === current.value.tasks[key]?.blocked) continue;
              ops.push(
                blocked === undefined
                  ? ["d", ["tasks", key, "blocked"]]
                  : ["s", ["tasks", key, "blocked"], blocked],
              );
            }
            publish(current, ops, report);
          }),
        )
        .pipe(
          Effect.catchCause((cause) =>
            Cause.hasInterruptsOnly(cause)
              ? Effect.interrupt
              : Effect.sync(() => {
                  // Refused only once the Session is closing or failed, when the mount
                  // is gone. A defect (a reason that throws, ops that do not apply) is reported, as a
                  // throwing listener is, and the loop keeps refreshing.
                  if (Cause.hasDies(cause)) options.report(cause, LISTENER);
                }),
          ),
        ),
      options.scope,
    );

    // Close ends every observer at the seal; a commit that settles during close finds
    // no mount, so it publishes nothing to them.
    yield* kernel.subscribeClose(() => {
      closed = true;
      const failure = Option.getOrUndefined(kernel.failure);
      for (const observer of Array.from(mount?.observers ?? [])) {
        contained(() => observer.closeSession(failure), report);
      }
      mount = undefined;
    });

    /**
     * Registers an observer created from the current revision, atomically
     * on the Session line, and `finalize` on the scope; an acquisition interrupted after it attached finalizes at
     * once.
     */
    const acquire = <O extends GraphObserver>(
      create: (value: TaskGraph, release: () => void) => O,
      finalize: (observer: O, detach: () => void) => void,
    ): Effect.Effect<O, HarnessError, Scope.Scope> =>
      Effect.gen(function* () {
        const scope = yield* Effect.scope;
        let attached: { readonly observer: O; readonly detach: () => void } | undefined;
        return yield* kernel
          .readOnLine(
            Effect.gen(function* () {
              const target = mount ?? (yield* build(kernel, blocking.blocked));
              // Check and register in one synchronous step, with the finalizer: close may begin while the mount
              // builds, and then nothing is registered.
              return yield* Effect.uninterruptible(
                Effect.suspend(() => {
                  if (closed) {
                    return Effect.fail(
                      Option.getOrUndefined(kernel.failure) ?? new SessionClosed(),
                    );
                  }
                  let registered = true;
                  const detach = () => {
                    if (!registered) return;
                    registered = false;
                    target.observers.delete(observer);
                    if (target.observers.size === 0 && mount === target) mount = undefined;
                  };
                  const observer = create(target.value, detach);
                  mount = target;
                  target.observers.add(observer);
                  attached = { observer, detach };
                  return Scope.addFinalizer(
                    scope,
                    Effect.sync(() => finalize(observer, detach)),
                  ).pipe(Effect.as(observer));
                }),
              );
            }),
          )
          .pipe(
            Effect.onInterrupt(() =>
              Effect.sync(() => {
                if (attached !== undefined) finalize(attached.observer, attached.detach);
              }),
            ),
          );
      });

    return {
      watch: acquire(watchObserver<TaskGraph>, (observer) => observer.stop()).pipe(
        Effect.map((observer) => observer.watch),
      ),
      observe: (observer) =>
        Effect.suspend(() => {
          let value: TaskGraph | undefined;
          let guarded: Guarded<WatchFrame<TaskGraph>> | undefined;
          // The attachment's own scope only records the detach; the observer outlives it.
          return Effect.scoped(
            acquire(
              (current, release) => {
                value = current;
                guarded = guardObserver(observer, release, guardServices);
                return guardedFollower(guarded);
              },
              () => {},
            ),
          ).pipe(
            // SAFETY: both are set by the attachment that just returned.
            Effect.map(() => observationOf(value!, guarded!)),
            Effect.onInterrupt(() =>
              Effect.sync(() => {
                guarded?.detach();
              }),
            ),
          );
        }),
    } satisfies TaskGraphs;
  });

/** A module function, so a detached observation keeps only `value` and the spent guard. */
const observationOf = (
  value: TaskGraph,
  guarded: Guarded<WatchFrame<TaskGraph>>,
): Observation<TaskGraph> => ({ value, detach: () => guarded.detach() });

/**
 * The committed live tasks and the conversations each owns, one owner scan
 * per live task, read on the line.
 */
const build = (kernel: SessionKernel, blocked: TaskGraphBlocking["blocked"]) =>
  Effect.gen(function* () {
    const storage = kernel.storage;
    const records: Array<TaskRecord> = [];
    for (const status of LIVE_STATUSES) {
      records.push(
        ...(yield* scanAll((cursor) => storage.scanTasks({ status }, SCAN_PAGE_SIZE, cursor))),
      );
    }
    records.sort((a, b) => a.id - b.id);
    const tasks: Record<string, TaskGraphNode> = {};
    const live = new Map<string, BlockedSubject>();
    for (const record of records) {
      const owned = yield* scanAll((cursor) =>
        storage.scanConversations({ ownerTaskId: record.id }, SCAN_PAGE_SIZE, cursor),
      );
      const conversations = owned.map((conversation) => conversation.id).toSorted((a, b) => a - b);
      const subject = blockedSubject(record);
      tasks[String(record.id)] = nodeOf(record, conversations, blocked(subject));
      live.set(String(record.id), subject);
    }
    return { value: { tasks }, observers: new Set(), records: live } satisfies Mount;
  });

/**
 * Derives the mount's operations from one publication, applies them, and
 * hands the revision to every observer; one that throws is reported, and the others still get it.
 */
const advance = (
  mount: Mount,
  publication: ObservedCommit,
  blocked: TaskGraphBlocking["blocked"],
  report: (error: Error) => void,
): void => {
  const ops: Array<Delta.Operation> = [];
  // Nodes this publication set or deleted, over the mount's value.
  const changed = new Map<string, TaskGraphNode | undefined>();
  const node = (key: string): TaskGraphNode | undefined =>
    changed.has(key) ? changed.get(key) : mount.value.tasks[key];
  for (const change of publication.changes) {
    if (change.type !== "task") continue;
    const record = change.value;
    const key = String(record.id);
    const previous = node(key);
    if (record.state.status === "terminal") {
      mount.records.delete(key);
      if (previous === undefined) continue;
      ops.push(["d", ["tasks", key]]);
      changed.set(key, undefined);
      continue;
    }
    const subject = blockedSubject(record);
    mount.records.set(key, subject);
    const next = nodeOf(record, previous?.conversations ?? [], blocked(subject));
    if (previous !== undefined && sameNode(next, previous)) continue;
    ops.push(["s", ["tasks", key], next]);
    changed.set(key, next);
  }
  // After the tasks, so a conversation created with its owner task in one commit finds the owner's node. Change
  // order within a publication is unspecified, so each owner's list is sorted again.
  const created = new Map<string, Array<ConversationId>>();
  for (const change of publication.changes) {
    if (change.type !== "conversation" || change.value.owner === undefined) continue;
    const key = String(change.value.owner.taskId);
    if (node(key) !== undefined) created.set(key, [...(created.get(key) ?? []), change.value.id]);
  }
  for (const [key, ids] of created) {
    // SAFETY: only keys with a node were collected above.
    const conversations = [...node(key)!.conversations, ...ids].toSorted((a, b) => a - b);
    ops.push(["s", ["tasks", key, "conversations"], conversations]);
  }
  publish(mount, ops, report);
};

/** Applies `ops` to the mount and hands the revision to every observer. */
const publish = (
  mount: Mount,
  ops: ReadonlyArray<Delta.Operation>,
  report: (error: Error) => void,
): void => {
  if (ops.length === 0) return;
  // A batch that does not apply breaks the mount's invariant: the throw fails the Session.
  mount.value = Delta.applyImmutableUnsafe(mount.value, ops);
  const frame: WatchFrame<TaskGraph> = { value: mount.value, ops, retired: false };
  for (const observer of Array.from(mount.observers)) {
    contained(() => observer.advance(frame), report);
  }
};

/** Optional fields are keys only when set. */
const nodeOf = (
  record: TaskRecord,
  conversations: ReadonlyArray<ConversationId>,
  blocked: BlockedReason | undefined,
): TaskGraphNode => {
  const node: Mutable<TaskGraphNode> = {
    id: record.id,
    kind: record.kind,
    conversationId: record.conversationId,
    background: record.background,
    abortRequested: record.abortRequested,
    state: stateOf(record),
    conversations,
  };
  if (record.owner !== undefined) node.owner = record.owner;
  if (blocked !== undefined) node.blocked = blocked;
  return node;
};

/** A task record's graph state. */
/** A `sleeping` graph state under construction: `until` and `signals` are set only when present. */
interface SleepingNode {
  readonly status: "sleeping";
  readonly phase: string;
  until?: number;
  signals?: ReadonlyArray<SignalKeyRecord>;
}

const stateOf = (record: TaskRecord): TaskGraphState => {
  const state = record.state;
  switch (state.status) {
    case "pending":
    case "running":
      return { status: state.status, phase: phaseOf(state.checkpoint) ?? "" };
    case "waiting":
      return {
        status: "waiting",
        phase: phaseOf(state.checkpoint) ?? "",
        on: [...state.on],
        policy: state.policy,
      };
    case "sleeping": {
      const phase = phaseOf(state.checkpoint) ?? "";
      const signals = state.signals?.map((key) => ({ ...key }));
      const sleeping: SleepingNode = { status: "sleeping", phase };
      if (state.until !== undefined) sleeping.until = state.until;
      if (signals !== undefined) sleeping.signals = signals;
      return sleeping;
    }
    // Terminal records never reach here: they leave the graph.
    case "completing":
    case "terminal":
      return { status: "completing", outcome: state.outcome.status };
  }
};

// Nodes are not compared by `JSON.stringify`, which `preferSchemaOverJson` forbids. A node is flat apart from `state.on` and `conversations`, so it compares field by field.
const sameNode = (a: TaskGraphNode, b: TaskGraphNode): boolean =>
  a.id === b.id &&
  a.kind === b.kind &&
  a.conversationId === b.conversationId &&
  a.owner === b.owner &&
  a.background === b.background &&
  a.abortRequested === b.abortRequested &&
  a.blocked === b.blocked &&
  sameIds(a.conversations, b.conversations) &&
  sameState(a.state, b.state);

const sameState = (a: TaskGraphState, b: TaskGraphState): boolean => {
  if (a.status === "completing" || b.status === "completing") {
    return a.status === "completing" && b.status === "completing" && a.outcome === b.outcome;
  }
  if (a.status !== b.status || a.phase !== b.phase) return false;
  if (a.status === "waiting" && b.status === "waiting") {
    return a.policy === b.policy && sameIds(a.on, b.on);
  }
  if (a.status === "sleeping" && b.status === "sleeping") {
    return a.until === b.until && sameKeys(a.signals ?? [], b.signals ?? []);
  }
  return true;
};

const sameKeys = (a: ReadonlyArray<SignalKeyRecord>, b: ReadonlyArray<SignalKeyRecord>): boolean =>
  a.length === b.length &&
  a.every((key, index) => key.signal === b[index]?.signal && key.key === b[index]?.key);

const sameIds = (a: ReadonlyArray<number>, b: ReadonlyArray<number>): boolean =>
  a.length === b.length && a.every((id, index) => id === b[index]);

/** Runs an observer callback that must not fail the publication; a throw goes to `onError`. */
const contained = (callback: () => void, onError: (error: Error) => void) => {
  const result = Result.try({
    try: callback,
    catch: (error) => (error instanceof Error ? error : new Error(String(error))),
  });
  if (Result.isFailure(result)) onError(result.failure);
};

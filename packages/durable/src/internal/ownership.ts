// The scheduler's ownership index (the sets the commit listener keeps): the ownership tree around live work and the
// candidate sets derived from committed task records.
//
// Tasks and conversations form one ownership tree: a task's
// parent is its owner task, or its conversation; a conversation's parent is
// its owner task, if any. Walks up that tree decide what reaches one task: a
// cancelling owner, a scope. `below` lists the tree downward around live
// work, so what lies below one node is a walk down from it. No pass visits
// every live task; the candidate sets hold each pass's candidates.
//
// The index is bounded by live work: an owner edge and an ended task's
// ownership fields are kept only while something is listed below them, so
// only the chains above live tasks are known, whole. Work that appears later
// below a dropped node finds it unknown and loads its chain from storage again.
//
// Everything here is synchronous and runs no Effect: the scheduler calls it
// inside commit callbacks and the commit listener, where a yield would let
// another fiber see the index half updated.
//
// The sets are a module of their own rather than fields of the scheduler, so
// the logic is tested without fibers.
import * as Predicate from "effect/Predicate";
import { conversationKey, INTENT_KEY, ownedKey, ROOTS_KEY, taskKey } from "./keys.ts";
import type { ConversationId, TaskId } from "../Ids.ts";
import type { TaskRecord } from "../Records.ts";

/** The immutable ownership fields of a task. */
export type TaskNode = Pick<TaskRecord, "conversationId" | "owner" | "background">;

/** Where a walk up the ownership tree continues: an owner task, or a conversation. */
export type Up = { readonly task: TaskId } | { readonly conversation: ConversationId };

/** One step of a walk up: an owner task with its node, a conversation, or an edge not loaded yet. */
export type Step =
  | { readonly task: TaskId; readonly node: TaskNode }
  | { readonly conversation: ConversationId }
  | { readonly unknown: true };

/** Candidate records a commit staged; they override committed records in ownership walks. */
export interface Overlay {
  readonly tasks: Map<TaskId, TaskRecord>;
  readonly edges: ReadonlyMap<ConversationId, TaskId | null>;
  /** Links to the staged tasks `below` does not list, by parent node, for walks down; built at first use. */
  links?: ReadonlyMap<Node, ReadonlySet<Node>>;
}

/** A node of the downward index: a task, or a conversation as `c<id>`. */
export type Node = TaskId | `c${number}`;

/** Where ordinary ownership traversal starts: one conversation, or every ownerless conversation. */
export type TraversalScope = { readonly conversation: ConversationId } | { readonly roots: true };

export interface WalkOptions {
  readonly overlay?: Overlay | undefined;
  readonly crossBackground?: boolean | undefined;
  readonly enter?: ((id: TaskId) => boolean) | undefined;
}

/** A task a walk down reached: its node, and its record while live. */
export interface Walked {
  readonly id: TaskId;
  readonly node: TaskNode;
  readonly live: TaskRecord | undefined;
}

export interface OwnershipOptions {
  /**
   * Notes that a read depends on `key`: the scheduler records what a decision region reads,
   * and does nothing anywhere else. The index calls it for every live record it reads (`task:<id>`), every node
   * a walk down expands (`owned:<node>`), every conversation edge (`conv:<id>`), the intent set and the roots.
   */
  readonly record?: ((key: string) => void) | undefined;
  /** Called when a node is queued for `sweep`; the scheduler sweeps after the operation underway. */
  readonly onDropQueued: () => void;
}

export interface OwnershipIndex {
  /** Every committed non-terminal task record, by ID; changed only by `track`. */
  readonly live: ReadonlyMap<TaskId, TaskRecord>;
  /** Live tasks with cancellation intent, the sources of abort cascades. */
  readonly intent: ReadonlySet<TaskId>;
  /** Live tasks that are not `completing`, the candidates of a reservation pass; the scheduler removes what it handled. */
  readonly runnable: Set<TaskId>;
  /** `completing` tasks the next finalize pass checks; the scheduler clears what it checked. */
  readonly finalizeChecks: Set<TaskId>;
  /** Live waiting tasks by each task in their `on`. */
  readonly waiters: ReadonlyMap<TaskId, ReadonlySet<TaskId>>;
  /** Live sleeping tasks, due or not. */
  readonly sleeping: ReadonlySet<TaskId>;
  /** Sleeping calls parked at their gates that an open woke before their time; they leave once they stop sleeping. */
  readonly gateWakes: ReadonlySet<TaskId>;
  /** Known tasks whose chain may not be loaded; every other known task's chain is, and stays, loaded. */
  readonly unloaded: ReadonlySet<TaskId>;
  /** Brings the index in line with a task's committed record. */
  readonly track: (previous: TaskRecord | undefined, record: TaskRecord) => void;
  /** Wakes a live sleeping task before its time, so its phase runs again. */
  readonly wakeGate: (id: TaskId) => void;
  /** Records a loaded owner edge of a conversation, `null` when ownerless. */
  readonly setEdge: (conversationId: ConversationId, owner: TaskId | null) => void;
  /** Knows a terminal task a chain load read, so walks pass through it; its own chain is not loaded yet. */
  readonly settle: (id: TaskId, node: TaskNode) => void;
  /** Keeps `node` through sweeps until as many `unpin`s. */
  readonly pin: (node: Node) => void;
  /** Releases one `pin` of `node`; unneeded, it waits for the next sweep. */
  readonly unpin: (node: Node) => void;
  /** Marks the chains above these tasks loaded. */
  readonly markLoaded: (ids: ReadonlyArray<TaskId>) => void;
  /** Drops the queued nodes that are still unneeded: not live, nothing listed below. */
  readonly sweep: () => void;
  /** Frees the nearest live task above ended ordinary work: one holding its outcome, or an aborting one. */
  readonly release: (node: TaskNode, overlay?: Overlay) => TaskRecord | undefined;
  /** The nearest live task above ended ordinary work `node`, in the overlay's view. */
  readonly nearestLiveAbove: (node: TaskNode, overlay?: Overlay) => TaskRecord | undefined;
  /** Walks up from `start`: owner tasks and conversations, ending at a root or an edge not loaded yet. */
  readonly above: (start: Up, overlay?: Overlay) => Generator<Step>;
  /** Whether the chain above `node` is loaded, without walking. */
  readonly known: (node: Node) => boolean;
  /** Owner task of a conversation, `null` when ownerless, `undefined` while not loaded. */
  readonly edgeOf: (id: ConversationId, overlay?: Overlay) => TaskId | null | undefined;
  /** The ownership fields of a known task, live or settled. */
  readonly knownNode: (id: TaskId) => TaskNode | undefined;
  /** The tasks below `start` that a walk down reaches. */
  readonly walkDown: (start: Node, walk?: WalkOptions) => Generator<Walked>;
  /** The live ordinary owned work of `owner`; with `first`, only the first. */
  readonly ownedLive: (
    owner: TaskId,
    overlay?: Overlay,
    first?: boolean,
    done?: ReadonlySet<TaskId>,
  ) => Array<TaskId>;
  readonly hasOwnedLive: (owner: TaskId, overlay?: Overlay, done?: ReadonlySet<TaskId>) => boolean;
  /** The live tasks `owner` owns directly, by ID. */
  readonly liveOwnedBy: (owner: TaskId) => Array<TaskRecord>;
  /** Whether ordinary traversal from `scope` reaches `start`; `undefined` while an edge is not loaded. */
  readonly inScope: (
    start: Up,
    scope: TraversalScope,
    crossBackground?: boolean,
  ) => boolean | undefined;
  /** Whether a live owner's cancellation intent reaches `start`. */
  readonly belowCancelled: (start: Up) => boolean;
  /** No live non-background task in the scope; `undefined` is every ownerless conversation. */
  readonly idle: (conversationId: ConversationId | undefined) => boolean;
  /**
   * Whether a conversation is inactive, for the kept contexts: idle, or with all its live work parked
   * (`parked`), which holds no invocation however long it waits.
   */
  readonly inactive: (conversationId: ConversationId) => boolean;
  /** Sizes of the indexes, for tests that bound the scheduler's memory. */
  readonly sizes: () => Readonly<
    Record<
      "live" | "settled" | "edges" | "below" | "roots" | "unloaded" | "dropQueue" | "pins",
      number
    >
  >;
}

/** Builds an empty ownership index. */
export const make = (options: OwnershipOptions): OwnershipIndex => {
  const record = options.record ?? (() => {});
  const live = new RecordingMap<TaskId, TaskRecord>((id) => record(taskKey(id)));
  /** Owner task of each known conversation, `null` when ownerless. */
  const edges = new Map<ConversationId, TaskId | null>();
  /** Ownership fields of the known terminal tasks, which walks pass through. */
  const settled = new Map<TaskId, TaskNode>();
  /** Ended tasks and conversations found unneeded, dropped by `sweep()` unless needed again by then. */
  const dropQueue = new Set<Node>();
  /** The tree downward around live work: each node's child nodes that are live tasks or have nodes below. */
  const below = new Map<Node, Set<Node>>();
  const roots = new Set<ConversationId>();
  const unloaded = new Set<TaskId>();
  const intent = new Set<TaskId>();
  const runnable = new Set<TaskId>();
  const finalizeChecks = new Set<TaskId>();
  const waiters = new Map<TaskId, Set<TaskId>>();
  const sleeping = new Set<TaskId>();
  const gateWakes = new Set<TaskId>();

  const setEdge = (conversationId: ConversationId, owner: TaskId | null) => {
    edges.set(conversationId, owner);
    // The conversation now hangs from its owner, or is a root; with nothing below it, it waits for the sweep.
    relist(conversationNode(conversationId));
    dropIfUnneeded(conversationNode(conversationId));
  };

  const settle = (id: TaskId, node: TaskNode) => {
    settled.set(id, node);
    unloaded.add(id);
    relist(id);
    dropIfUnneeded(id);
  };

  /** Queues an ended task or a conversation for the sweep when nothing is listed below it. */
  const dropIfUnneeded = (node: Node) => {
    if (!isConversationNode(node) && live.has(node)) return;
    if (below.has(node)) return;
    dropQueue.add(node);
    options.onDropQueued();
  };

  /**
   * Nodes a chain load installed for a line job whose commit is not published yet, with how many such loads
   * hold each: the sweep keeps them, so the commit finds its chain at publication.
   */
  const pins = new Map<Node, number>();

  /** A flat loop. */
  const sweep = () => {
    for (const node of dropQueue) {
      if (below.has(node) || pins.has(node)) continue;
      if (isConversationNode(node)) {
        edges.delete(conversationOf(node));
        roots.delete(conversationOf(node));
      } else if (!live.has(node)) {
        settled.delete(node);
        unloaded.delete(node);
      }
    }
    dropQueue.clear();
  };

  /**
   * A task that ended may free the nearest live task above it: one holding its outcome finalizes, an
   * abort-marked one runs its abort handler; its waiters may run.
   */
  const track = (previous: TaskRecord | undefined, record: TaskRecord) => {
    const id = record.id;
    if (previous?.state.status === "waiting") {
      for (const on of previous.state.on) deleteFrom(waiters, on, id);
    }
    if (record.state.status === "terminal") {
      const node = taskNodeOf(record);
      live.delete(id);
      sleeping.delete(id);
      gateWakes.delete(id);
      intent.delete(id);
      runnable.delete(id);
      finalizeChecks.delete(id);
      settled.set(id, node);
      // Before anything is forgotten: the walk up to the task it held passes the ended tasks between them.
      release(node);
      for (const waiter of waiters.get(id) ?? []) runnable.add(waiter);
      relist(id);
      dropIfUnneeded(id);
      return;
    }
    live.set(id, record);
    if (previous === undefined) {
      if (!known(parentNode(record))) unloaded.add(id);
      relist(id);
    }
    if (cancellationIntent(record)) intent.add(id);
    else intent.delete(id);
    if (record.state.status === "completing") {
      runnable.delete(id);
      finalizeChecks.add(id);
    } else {
      runnable.add(id);
    }
    if (record.state.status === "waiting") {
      for (const on of record.state.on) addTo(waiters, on, id);
    }
    if (record.state.status === "sleeping") sleeping.add(id);
    else {
      sleeping.delete(id);
      gateWakes.delete(id);
    }
  };

  const release = (node: TaskNode, overlay?: Overlay) => {
    const above = nearestLiveAbove(node, overlay);
    if (above?.state.status === "completing") finalizeChecks.add(above.id);
    else if (above?.abortRequested === true) runnable.add(above.id);
    return above;
  };

  /**
   * Lists `start` under its parent node in `below` while it is a live task or has nodes listed below it,
   * and unlists it otherwise; then its parent, as far up as that changes. A loop, as ownership chains
   * can be deeper than the call stack.
   */
  const relist = (start: Node) => {
    let node: Node = start;
    for (;;) {
      const needed = (!isConversationNode(node) && live.has(node)) || below.has(node);
      let parent: Node | null | undefined;
      if (isConversationNode(node)) {
        parent = edges.get(conversationOf(node));
      } else {
        const knownNode = live.get(node) ?? settled.get(node);
        if (knownNode === undefined) return;
        parent = parentNode(knownNode);
      }
      if (parent === undefined) return;
      if (parent === null) {
        // SAFETY: only conversations have a `null` parent, the ownerless ones.
        const root = conversationOf(node as `c${number}`);
        if (needed) roots.add(root);
        else roots.delete(root);
        return;
      }
      if ((below.get(parent)?.has(node) === true) === needed) return;
      if (needed) {
        const first = !below.has(parent);
        addTo(below, parent, node);
        if (!first) return;
      } else {
        deleteFrom(below, parent, node);
        if (below.has(parent)) return;
        dropIfUnneeded(parent);
      }
      node = parent;
    }
  };

  /** The one that work held, if any. Work at or below a background task holds nothing above it. */
  const nearestLiveAbove = (node: TaskNode, overlay?: Overlay): TaskRecord | undefined => {
    record(INTENT_KEY);
    for (let current = node; !current.background; ) {
      const owner = current.owner ?? edgeOf(current.conversationId, overlay);
      if (owner === undefined || owner === null) return undefined;
      const record = overlay?.tasks.get(owner) ?? live.get(owner);
      if (record !== undefined && record.state.status !== "terminal") return record;
      const above = record ?? settled.get(owner);
      if (above === undefined) return undefined;
      current = above;
    }
    return undefined;
  };

  const known = (node: Node): boolean => {
    if (!isConversationNode(node)) {
      return (live.has(node) || settled.has(node)) && !unloaded.has(node);
    }
    const owner = edges.get(conversationOf(node));
    return owner === null || (owner !== undefined && known(owner));
  };

  const edgeOf = (id: ConversationId, overlay?: Overlay): TaskId | null | undefined => {
    if (overlay?.edges.has(id) === true) return overlay.edges.get(id);
    record(conversationKey(id));
    return edges.get(id);
  };

  const nodeFor = (id: TaskId, overlay?: Overlay): TaskNode | undefined =>
    overlay?.tasks.get(id) ?? live.get(id) ?? settled.get(id);

  /** Walks up from `start`: owner tasks and conversations, ending at a root or an edge not loaded yet. */
  function* above(start: Up, overlay?: Overlay): Generator<Step> {
    let at: Up | undefined = start;
    while (at !== undefined) {
      if ("task" in at) {
        const node = nodeFor(at.task, overlay);
        if (node === undefined) {
          yield { unknown: true };
          return;
        }
        yield { task: at.task, node };
        at = parentOf(node);
      } else {
        yield { conversation: at.conversation };
        const edge = edgeOf(at.conversation, overlay);
        if (edge === undefined) {
          yield { unknown: true };
          return;
        }
        at = edge === null ? undefined : { task: edge };
      }
    }
  }

  /**
   * Those `below` lists and, under an overlay, the commit's new tasks, with its candidates in place of
   * committed records. Background tasks are reached but not entered, unless `crossBackground`, and neither
   * are the tasks `enter` rejects. A loop.
   */
  function* walkDown(start: Node, walk: WalkOptions = {}): Generator<Walked> {
    const overlay = walk.overlay;
    const crossBackground = walk.crossBackground ?? false;
    const links = overlay === undefined ? undefined : linksOf(overlay);
    const pending: Array<Node> = [start];
    for (let at = pending.pop(); at !== undefined; at = pending.pop()) {
      record(ownedKey(at));
      for (const children of [below.get(at), links?.get(at)]) {
        for (const child of children ?? []) {
          if (isConversationNode(child)) {
            pending.push(child);
            continue;
          }
          const record = overlay?.tasks.get(child) ?? live.get(child);
          const node = record ?? settled.get(child);
          if (node === undefined) continue;
          const liveRecord =
            record !== undefined && record.state.status !== "terminal" ? record : undefined;
          yield { id: child, node, live: liveRecord };
          if (
            (crossBackground || !node.background) &&
            (walk.enter === undefined || walk.enter(child))
          ) {
            pending.push(child);
          }
        }
      }
    }
  }

  /**
   * The links walks down need for the commit's new tasks: each one under its parent node, then each node
   * above it that `below` does not list yet, until one it lists, a live task, or the chain's end.
   */
  const linksOf = (overlay: Overlay): ReadonlyMap<Node, ReadonlySet<Node>> => {
    if (overlay.links !== undefined) return overlay.links;
    const links = new Map<Node, Set<Node>>();
    for (const record of overlay.tasks.values()) {
      if (live.has(record.id) || settled.has(record.id)) continue;
      let child: Node = record.id;
      let parent: Node | null | undefined = parentNode(record);
      while (parent !== undefined && parent !== null && links.get(parent)?.has(child) !== true) {
        addTo(links, parent, child);
        if (!isConversationNode(parent) && (live.has(parent) || overlay.tasks.has(parent))) break;
        let next: Node | null | undefined;
        if (isConversationNode(parent)) {
          next = edgeOf(conversationOf(parent), overlay);
        } else {
          const fields = settled.get(parent);
          next = fields === undefined ? undefined : parentNode(fields);
        }
        if (next !== undefined && next !== null && below.get(next)?.has(parent) === true) break;
        child = parent;
        parent = next;
      }
    }
    overlay.links = links;
    return links;
  };

  /** The live non-background tasks below it that a walk down reaches without entering a background task. */
  const ownedLive = (
    owner: TaskId,
    overlay?: Overlay,
    first = false,
    done?: ReadonlySet<TaskId>,
  ): Array<TaskId> => {
    const found: Array<TaskId> = [];
    const walk = walkDown(owner, {
      overlay,
      enter: done === undefined ? undefined : (id: TaskId) => !done.has(id),
    });
    for (const walked of walk) {
      if (walked.live === undefined || walked.node.background) continue;
      found.push(walked.id);
      if (first) break;
    }
    return found;
  };

  const hasOwnedLive = (owner: TaskId, overlay?: Overlay, done?: ReadonlySet<TaskId>) =>
    ownedLive(owner, overlay, true, done).length > 0;

  const liveOwnedBy = (owner: TaskId): Array<TaskRecord> => {
    // Every live task is listed under its parent node, its owner task when it has one. The read
    // records the owner's ownership key and each child's record.
    record(ownedKey(owner));
    const owned: Array<TaskRecord> = [];
    for (const node of below.get(owner) ?? []) {
      const record = isConversationNode(node) ? undefined : live.get(node);
      if (record?.owner === owner) owned.push(record);
    }
    owned.sort((a, b) => a.id - b.id);
    return owned;
  };

  /**
   * Walking up reaches the scope's conversation, or an ownerless one for `roots`, without crossing a
   * background owner, unless `crossBackground`.
   */
  const inScope = (
    start: Up,
    scope: TraversalScope,
    crossBackground = false,
  ): boolean | undefined => {
    record(INTENT_KEY);
    for (const step of above(start)) {
      if ("unknown" in step) return undefined;
      if ("conversation" in step) {
        if ("conversation" in scope && step.conversation === scope.conversation) return true;
      } else if (step.node.background && !crossBackground) {
        return false;
      }
    }
    return "roots" in scope;
  };

  /** Terminal owners never cascade. */
  const belowCancelled = (start: Up): boolean => {
    record(INTENT_KEY);
    // Nothing cancels, the common case: no walk.
    if (intent.size === 0) return false;
    for (const step of above(start)) {
      if ("unknown" in step) return false;
      if (!("task" in step)) continue;
      const record = live.get(step.task);
      if (record !== undefined && cancellationIntent(record)) return true;
      if (step.node.background) return false;
    }
    return false;
  };

  const idle = (conversationId: ConversationId | undefined): boolean =>
    quiet(conversationId, () => true);

  const inactive = (conversationId: ConversationId): boolean =>
    quiet(conversationId, (record) => !parked(record));

  /** No live non-background task in the scope that `counts`; one whose owner edges are not loaded counts as inside. */
  const quiet = (
    conversationId: ConversationId | undefined,
    counts: (record: TaskRecord) => boolean,
  ): boolean => {
    const scope: TraversalScope =
      conversationId === undefined ? { roots: true } : { conversation: conversationId };
    // Global idle reads the roots it starts from.
    if (conversationId === undefined) record(ROOTS_KEY);
    for (const id of unloaded) {
      const record = live.get(id);
      if (
        record !== undefined &&
        !record.background &&
        counts(record) &&
        inScope(parentOf(record), scope) !== false
      ) {
        return false;
      }
    }
    const starts = conversationId === undefined ? roots : [conversationId];
    for (const startId of starts) {
      for (const walked of walkDown(conversationNode(startId))) {
        if (walked.live !== undefined && !walked.node.background && counts(walked.live))
          return false;
      }
    }
    return true;
  };

  return {
    live,
    intent,
    runnable,
    finalizeChecks,
    waiters,
    sleeping,
    gateWakes,
    unloaded,
    track,
    wakeGate: (id) => {
      if (!sleeping.has(id)) return;
      gateWakes.add(id);
      runnable.add(id);
    },
    setEdge,
    settle,
    pin: (node) => {
      pins.set(node, (pins.get(node) ?? 0) + 1);
    },
    unpin: (node) => {
      const count = (pins.get(node) ?? 1) - 1;
      if (count > 0) {
        pins.set(node, count);
        return;
      }
      pins.delete(node);
      dropIfUnneeded(node);
    },
    markLoaded: (ids) => {
      for (const id of ids) unloaded.delete(id);
    },
    sweep,
    release,
    nearestLiveAbove,
    above,
    known,
    edgeOf,
    knownNode: (id) => live.get(id) ?? settled.get(id),
    walkDown,
    ownedLive,
    hasOwnedLive,
    liveOwnedBy,
    inScope,
    belowCancelled,
    idle,
    inactive,
    sizes: () => {
      let listed = 0;
      for (const children of below.values()) listed += children.size;
      return {
        live: live.size,
        settled: settled.size,
        edges: edges.size,
        below: listed,
        roots: roots.size,
        unloaded: unloaded.size,
        dropQueue: dropQueue.size,
        pins: pins.size,
      };
    },
  };
};

export const conversationNode = (id: ConversationId): Node => `c${id}`;

/**
 * The live records, noting each read of one (`get`, `has`): a decision region that reads a task's record waits
 * when an unpublished commit writes it.
 */
class RecordingMap<K, V> extends Map<K, V> {
  readonly #record: (key: K) => void;

  constructor(record: (key: K) => void) {
    super();
    this.#record = record;
  }

  override get(key: K): V | undefined {
    this.#record(key);
    return super.get(key);
  }

  override has(key: K): boolean {
    this.#record(key);
    return super.has(key);
  }
}

// SAFETY: a conversation node is `c` followed by the conversation's ID.
export const conversationOf = (node: `c${number}`): ConversationId =>
  Number(node.slice(1)) as ConversationId;

export const isConversationNode = (node: Node): node is `c${number}` => Predicate.isString(node);

/** The node a task hangs from: its owner task, or its conversation. */
export const parentNode = (node: TaskNode): Node =>
  node.owner ?? conversationNode(node.conversationId);

/** Where a walk up from a task starts: its owner task, or its conversation. */
export const parentOf = (node: TaskNode): Up =>
  node.owner !== undefined ? { task: node.owner } : { conversation: node.conversationId };

/** The ownership fields of a record. */
export const taskNodeOf = (record: TaskRecord): TaskNode =>
  record.owner === undefined
    ? { conversationId: record.conversationId, background: record.background }
    : { conversationId: record.conversationId, owner: record.owner, background: record.background };

/** A live owner's durable cancellation intent: its abort mark, or a held outcome other than `completed`. */
export const cancellationIntent = (record: TaskRecord): boolean =>
  record.state.status !== "terminal" && (record.abortRequested || failedOutcome(record));

/** Whether the record holds or ends with an outcome other than `completed`. */
export const failedOutcome = (record: TaskRecord): boolean => {
  const state = record.state;
  return (
    (state.status === "completing" || state.status === "terminal") &&
    state.outcome.status !== "completed"
  );
};

/**
 * Whether replacing `previous` (absent: a new task) with `next`
 * changes the cancellation intent set or what it means: a mark set, upgraded, or cleared, or a held or ended
 * outcome other than `completed`.
 */
export const intentChanges = (previous: TaskRecord | undefined, next: TaskRecord): boolean =>
  (previous?.abortRequested ?? false) !== next.abortRequested ||
  previous?.abortReason !== next.abortReason ||
  (previous !== undefined && failedOutcome(previous)) !== failedOutcome(next) ||
  (previous !== undefined && cancellationIntent(previous)) !== cancellationIntent(next);

const addTo = <K, V>(map: Map<K, Set<V>>, key: K, value: V) => {
  const values = map.get(key);
  if (values === undefined) map.set(key, new Set([value]));
  else values.add(value);
};

const deleteFrom = <K, V>(map: Map<K, Set<V>>, key: K, value: V) => {
  const values = map.get(key);
  if (values === undefined) return;
  values.delete(value);
  if (values.size === 0) map.delete(key);
};

/** A live task that holds no invocation while it waits: sleeping, waiting, or holding its outcome. */
export const parked = (record: TaskRecord): boolean => {
  const status = record.state.status;
  return status === "sleeping" || status === "waiting" || status === "completing";
};

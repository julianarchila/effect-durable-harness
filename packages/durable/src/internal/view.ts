// The conversation view mounts.
//
// At most one mount per conversation, built on the Session line by its first observer and dropped with its
// last. The mount holds the documents, shared by every observer; each observer has its own window over the
// entries. Each mount advances synchronously inside the Session's commit publications, which are durable, so
// it shows only committed state. Watches over a mount reuse the document watch (`internal/observation.ts`).
//
// `make` returns the mounts, and the Harness keeps them.
import * as Delta from "@effect-durable-harness/delta/Delta";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Result from "effect/Result";
import type * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import { AgentDoc } from "../Agent.ts";
import type { Cursor, EntryQuery } from "../AgentStorage.ts";
import type { WatchFrame } from "../Doc.ts";
import type { HarnessError } from "../Harness.ts";
import type { ConversationId, DocumentId, EntryId } from "../Ids.ts";
import { InboxDoc } from "../Inbox.ts";
import { LiveDoc } from "../Live.ts";
import { ProviderDoc } from "../ProviderSession.ts";
import type { ConversationRecord, EntryRecord } from "../Records.ts";
import { SessionClosed, type SessionFailed } from "../SessionError.ts";
import { UsageDoc } from "../Usage.ts";
import type { ConversationView, ConversationWatch, ConversationWindow } from "../View.ts";
import { type AnyDefinition, definitionOf } from "./documents.ts";
import type { Observation, Observer } from "../Observer.ts";
import { LISTENER, type Reporter } from "./report.ts";
import {
  type Follower,
  type Guarded,
  guardedFollower,
  guardObserver,
  watchObserver,
} from "./observation.ts";
import type { GuardedStorage } from "./transaction.ts";
import type { ObservedCommit, SessionKernel } from "./session.ts";

const SCAN_PAGE_SIZE = 256;

/** Default `ConversationWindow.min`. */
const DEFAULT_WINDOW_MIN = 50;

/** Receives each next revision of one observer's window, and the Session's close. */
export type ViewObserver = Pick<Follower<ConversationView>, "advance" | "closeSession">;

/** Builds an observer from its window's first revision; `release` drops it, and the mount with its last one. */
export type CreateViewObserver<O extends ViewObserver> = (
  value: ConversationView,
  release: () => void,
) => O;

/** The Harness's conversation view mounts. */
export interface Views {
  /** The view as a watch, released with the scope. */
  readonly watch: (
    conversationId: ConversationId,
    window?: ConversationWindow | undefined,
  ) => Effect.Effect<ConversationWatch, HarnessError, Scope.Scope>;
  /**
   * Attaches a host's synchronous observer behind `guardObserver`. The attaching
   * fiber governs only the attachment: interrupted after it attached, it detaches.
   */
  readonly observe: (
    conversationId: ConversationId,
    observer: Observer<ConversationView>,
    window?: ConversationWindow | undefined,
  ) => Effect.Effect<Observation<ConversationView>, HarnessError>;
  /**
   * Registers an observer created from its window's first revision, atomically on the Session line: it sees
   * every later publication and nothing earlier. Released with the scope.
   */
  readonly attach: <O extends ViewObserver>(
    conversationId: ConversationId,
    window: ConversationWindow | undefined,
    create: CreateViewObserver<O>,
  ) => Effect.Effect<O, HarnessError, Scope.Scope>;
}

export interface ViewsOptions {
  readonly kernel: SessionKernel;
  /** Receives an observer's throw; the other observers still get the revision. */
  readonly report: Reporter;
}

/** One observer of a mount and its window: the bounds and the revision it was last given. */
interface Windowed {
  readonly min: number;
  readonly max: number;
  value: ConversationView;
  readonly observer: ViewObserver;
}

/** One conversation's mount: the documents it shows, shared by every observer, and the observers. */
interface Mount {
  readonly conversation: ConversationRecord;
  docs: Readonly<Record<string, Schema.JsonObject>>;
  /** Mounted incarnation and definition version per kind; another incarnation or version is set whole. */
  readonly incarnations: Map<string, { readonly id: DocumentId; readonly version: number }>;
  readonly observers: Set<Windowed>;
}

/** A window with its defaults applied. */
interface WindowBounds {
  readonly from: EntryId | undefined;
  readonly min: number;
  readonly max: number;
}

const MOUNTED: ReadonlyArray<AnyDefinition> = [
  definitionOf(AgentDoc),
  definitionOf(LiveDoc),
  definitionOf(InboxDoc),
  definitionOf(ProviderDoc),
  definitionOf(UsageDoc),
];
const MOUNTED_KINDS: ReadonlySet<string> = new Set(MOUNTED.map((definition) => definition.kind));

/**
 * `window` with its defaults. Bounds that are not integers, or `max` below `min`, are a `RangeError`; argument
 * checks die with it, as `maxTurns` does.
 */
export const resolveWindow = (window: ConversationWindow | undefined): WindowBounds => {
  const min = window?.min ?? DEFAULT_WINDOW_MIN;
  if (!Number.isSafeInteger(min) || min < 1) {
    throw new RangeError(`Window min must be an integer >= 1, got ${min}`);
  }
  const max = window?.max ?? 2 * min;
  if (!Number.isSafeInteger(max) || max < min) {
    throw new RangeError(`Window max must be an integer >= min (${min}), got ${max}`);
  }
  const from = window?.from;
  if (from !== undefined && !Number.isSafeInteger(from)) {
    throw new RangeError(`Window from must be an entry ID, got ${from}`);
  }
  return { from, min, max };
};

/**
 * Registers the mounts' commit observer and close listener on the kernel, in that order.
 * Call it after the scheduler and the submissions registered theirs, so listeners run in that order.
 */
export const make = (options: ViewsOptions): Effect.Effect<Views, SessionFailed | SessionClosed> =>
  Effect.gen(function* () {
    const kernel = options.kernel;
    const mounts = new Map<ConversationId, Mount>();
    let closed = false;
    const report = (error: Error) => options.report(Cause.die(error), LISTENER);

    // The services an observer's returned Effect runs with: the Harness's, as for its other host callbacks.
    const services = yield* Effect.context<never>();
    const guardServices = {
      report,
      runReturned: (effect: Effect.Effect<void>) => {
        Effect.runForkWith(services)(effect);
      },
    };

    yield* kernel.followCommits((publication) => {
      for (const [id, mount] of mounts) advance(id, mount, publication, report);
    });
    // Close ends every watch at the seal; a commit that settles during close finds no mount,
    // so it publishes nothing to them.
    yield* kernel.subscribeClose(() => {
      closed = true;
      const failure = Option.getOrUndefined(kernel.failure);
      for (const mount of mounts.values()) {
        for (const windowed of Array.from(mount.observers)) {
          contained(() => windowed.observer.closeSession(failure), report);
        }
      }
      mounts.clear();
    });

    /**
     * Attaches on the line and registers `finalize` on the scope; an acquisition interrupted after it attached
     * finalizes at once.
     */
    const acquire = <O extends ViewObserver>(
      conversationId: ConversationId,
      window: ConversationWindow | undefined,
      create: CreateViewObserver<O>,
      finalize: (observer: O, detach: () => void) => void,
    ): Effect.Effect<O, HarnessError, Scope.Scope> =>
      Effect.gen(function* () {
        // A throw in `sync` is a defect: bad bounds die with the `RangeError`.
        const bounds = yield* Effect.sync(() => resolveWindow(window));
        const scope = yield* Effect.scope;
        let attached: { readonly observer: O; readonly detach: () => void } | undefined;
        return yield* kernel
          .readOnLine(
            Effect.gen(function* () {
              const mount = mounts.get(conversationId) ?? (yield* build(kernel, conversationId));
              const loaded = yield* loadWindow(kernel.storage, conversationId, bounds);
              // Check and register in one synchronous step, with the finalizer: close may begin while the
              // mount hydrates, and then nothing is registered.
              return yield* Effect.uninterruptible(
                Effect.suspend(() => {
                  if (closed) {
                    return Effect.fail(
                      Option.getOrUndefined(kernel.failure) ?? new SessionClosed(),
                    );
                  }
                  const value = view(mount.conversation, loaded.entries, loaded.before, mount.docs);
                  let registered = true;
                  const detach = () => {
                    if (!registered) return;
                    registered = false;
                    mount.observers.delete(windowed);
                    if (mount.observers.size === 0 && mounts.get(conversationId) === mount) {
                      mounts.delete(conversationId);
                    }
                  };
                  const observer = create(value, detach);
                  const windowed: Windowed = { min: bounds.min, max: bounds.max, value, observer };
                  mounts.set(conversationId, mount);
                  mount.observers.add(windowed);
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
      observe: (conversationId, observer, window) =>
        Effect.suspend(() => {
          let value: ConversationView | undefined;
          let guarded: Guarded<WatchFrame<ConversationView>> | undefined;
          // The attachment's own scope only records the detach; the observer outlives it.
          return Effect.scoped(
            acquire(
              conversationId,
              window,
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
      watch: (conversationId, window) =>
        acquire(conversationId, window, watchObserver<ConversationView>, (observer) =>
          observer.stop(),
        ).pipe(Effect.map((observer) => observer.watch)),
      attach: (conversationId, window, create) =>
        acquire(conversationId, window, create, (_observer, detach) => detach()),
    } satisfies Views;
  });

/**
 * The `Observation` of a guarded observer attached at `value`. A module function, so a
 * detached observation keeps nothing but `value` and the spent guard alive.
 */
const observationOf = (
  value: ConversationView,
  guarded: Guarded<WatchFrame<ConversationView>>,
): Observation<ConversationView> => ({ value, detach: () => guarded.detach() });

/** The conversation and its mounted documents. Run on the line. */
const build = (kernel: SessionKernel, conversationId: ConversationId) =>
  Effect.gen(function* () {
    const storage = kernel.storage;
    const conversation = yield* storage.conversation(conversationId);
    if (Option.isNone(conversation)) {
      return yield* Effect.die(new Error(`Conversation ${conversationId} does not exist`));
    }
    // The documents come through the Session's document cache
    // (`conversationDocumentOnLine`), not storage: memory may be ahead of storage by held changes (deferred
    // commits), and the mount must start where the next publication's ops apply. A built-in document
    // that fails its definition is a defect.
    const docs: { [kind: string]: Schema.JsonObject } = {};
    const incarnations = new Map<string, { readonly id: DocumentId; readonly version: number }>();
    for (const definition of MOUNTED) {
      const loaded = yield* kernel
        .conversationDocumentOnLine(definition, conversationId)
        .pipe(Effect.catchTag("DocumentAccessError", (error) => Effect.die(error)));
      if (Option.isNone(loaded)) continue;
      docs[definition.kind] = loaded.value.value;
      incarnations.set(definition.kind, {
        id: loaded.value.record.id,
        version: loaded.value.version,
      });
    }
    return {
      conversation: conversation.value,
      docs,
      incarnations,
      observers: new Set(),
    } satisfies Mount;
  });

/** A view revision; `before` is a key only when set. */
const view = (
  conversation: ConversationRecord,
  entries: ReadonlyArray<EntryRecord>,
  before: EntryId | undefined,
  docs: Readonly<Record<string, Schema.JsonObject>>,
): ConversationView =>
  before === undefined ? { conversation, entries, docs } : { conversation, entries, before, docs };

/**
 * A new observer's window, read on the Session line with bounded scans:
 * the entries from `from` when at most `max` are at or after it, otherwise the newest `min`, and the entry
 * before them.
 */
const loadWindow = (
  storage: GuardedStorage,
  conversationId: ConversationId,
  bounds: WindowBounds,
) =>
  Effect.gen(function* () {
    const { from, min, max } = bounds;
    if (from !== undefined) {
      const since = yield* scanUpTo(
        storage,
        { conversationId, minEntryId: from, order: "ascending" },
        max + 1,
      );
      if (since.length <= max) {
        // SAFETY: entry IDs are integers; the one below `from` bounds the scan exclusively.
        const maxEntryId = (from - 1) as EntryId;
        const previous = yield* storage.scanEntries({ conversationId, maxEntryId }, 1, undefined);
        return { entries: since, before: previous.items[0]?.id };
      }
    }
    const newest = yield* scanUpTo(storage, { conversationId }, min + 1);
    const before = newest.length > min ? newest.pop()?.id : undefined;
    return { entries: newest.toReversed(), before };
  });

/** Up to `limit` entries of `query`, following cursors past short pages. */
const scanUpTo = (storage: GuardedStorage, query: EntryQuery, limit: number) =>
  Effect.gen(function* () {
    const entries: Array<EntryRecord> = [];
    let cursor: Cursor | undefined;
    do {
      const page = yield* storage.scanEntries(
        query,
        Math.min(SCAN_PAGE_SIZE, limit - entries.length),
        cursor,
      );
      entries.push(...page.items);
      cursor = Option.getOrUndefined(page.next);
    } while (cursor !== undefined && entries.length < limit);
    return entries;
  });

/**
 * Derives the mount's operations from one publication and hands each observer
 * the next revision of its window; one that throws is reported, and the others still get theirs. Documents
 * are applied once and shared; each window takes the appended entries and, past its `max`, drops its oldest
 * down to its `min` in one splice.
 */
const advance = (
  id: ConversationId,
  mount: Mount,
  publication: ObservedCommit,
  report: (error: Error) => void,
): void => {
  const docOps: Array<Delta.Operation> = [];
  // Entry writes are published in ID order.
  const appended: Array<EntryRecord> = [];
  for (const change of publication.changes) {
    if (change.type === "entry" && change.value.conversationId === id) {
      appended.push(change.value);
      continue;
    }
    if (change.type !== "document" || change.conversationId !== id) continue;
    const kind = change.record.kind;
    if (!MOUNTED_KINDS.has(kind) || change.record.key !== undefined) continue;
    const path: Delta.NonEmptyPath = ["docs", kind];
    const mounted = mount.incarnations.get(kind);
    if (change.value === null) {
      if (mounted?.id !== change.record.id) continue;
      mount.incarnations.delete(kind);
      docOps.push(["d", path]);
    } else if (mounted?.id === change.record.id && mounted.version === change.version) {
      for (const op of change.ops) docOps.push(prefixed(op, path));
    } else {
      // SAFETY: a published change with a value always has its version.
      mount.incarnations.set(kind, { id: change.record.id, version: change.version! });
      docOps.push(["s", path, change.value]);
    }
  }
  if (docOps.length === 0 && appended.length === 0) return;
  // A batch that does not apply breaks the mount's invariant: the throw fails the Session.
  if (docOps.length > 0) {
    mount.docs = Delta.applyImmutableUnsafe({ docs: mount.docs }, docOps).docs;
  }
  const inserted: Array<Schema.Json> = appended;
  for (const windowed of Array.from(mount.observers)) {
    const previous = windowed.value;
    let entries = previous.entries;
    let before = previous.before;
    let ops: Array<Delta.Operation> = docOps;
    if (appended.length > 0) {
      ops = [...docOps, ["p", ["entries"], entries.length, 0, inserted]];
      entries = entries.concat(appended);
      if (entries.length > windowed.max) {
        const drop = entries.length - windowed.min;
        // SAFETY: `drop` is at least 1 and below the length.
        before = entries[drop - 1]!.id;
        entries = entries.slice(drop);
        ops.push(["p", ["entries"], 0, drop, []], ["s", ["before"], before]);
      }
    }
    windowed.value = view(mount.conversation, entries, before, mount.docs);
    const frame: WatchFrame<ConversationView> = { value: windowed.value, ops, retired: false };
    contained(() => windowed.observer.advance(frame), report);
  }
};

/** `op` moved under `prefix`; a root replacement becomes a set of the prefix. */
const prefixed = (op: Delta.Operation, prefix: Delta.NonEmptyPath): Delta.Operation => {
  const at = (path: Delta.Path): Delta.NonEmptyPath => [...prefix, ...path];
  switch (op[0]) {
    case "r":
      return ["s", prefix, op[1]];
    case "p":
      return ["p", at(op[1]), op[2], op[3], op[4]];
    case "m":
      return ["m", at(op[1]), op[2]];
    case "s":
      return ["s", at(op[1]), op[2]];
    case "d":
      return ["d", at(op[1])];
    case "a":
      return ["a", at(op[1]), op[2]];
    case "t":
      return ["t", at(op[1]), op[2]];
  }
};

/** Runs an observer callback that must not fail the publication; a throw goes to `onError`. */
const contained = (callback: () => void, onError: (error: Error) => void) => {
  const result = Result.try({
    try: callback,
    catch: (error) => (error instanceof Error ? error : new Error(String(error))),
  });
  if (Result.isFailure(result)) onError(result.failure);
};

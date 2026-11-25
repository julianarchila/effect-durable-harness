// Shared support for the Session suites. `ControlledStorage` wraps whatever
// `AgentStorage` is in context, so each suite runs on memory and on
// `sqlite-node`.
import * as SqliteClient from "@effect/sql-sqlite-node/SqliteClient";
import { describe } from "@effect/vitest";
import * as Cause from "effect/Cause";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Exit from "effect/Exit";
import * as Option from "effect/Option";
import * as Scope from "effect/Scope";
import * as AgentStorageModule from "../src/AgentStorage.ts";
import { AgentStorage, StorageFailure, type StorageWrite } from "../src/AgentStorage.ts";
import * as internal from "../src/internal/session.ts";
import * as MemoryAgentStorage from "../src/MemoryAgentStorage.ts";
import type { ConversationId, Seq, TaskId } from "../src/Ids.ts";
import type { CommitPublication, DocumentCommitChange, SessionOptions } from "../src/Session.ts";
import * as SqlAgentStorage from "../src/SqlAgentStorage.ts";
import { Tx } from "../src/Tx.ts";

/** One storage backend a Session suite runs on. */
export interface Backend {
  readonly name: string;
  readonly layer: Layer.Layer<AgentStorage, StorageFailure>;
}

export const backends: ReadonlyArray<Backend> = [
  { name: "memory", layer: MemoryAgentStorage.layer },
  {
    name: "sqlite",
    // The client layer fails with `SqlError` when it cannot open the database (effect 4.0.1): broken setup here.
    layer: SqlAgentStorage.layer().pipe(
      Layer.provide(Layer.orDie(SqliteClient.layer({ filename: ":memory:" }))),
    ),
  },
];

/** Registers `suite` once per backend, as `describe("<name> (<backend>)")`. */
export const describeEachBackend = (name: string, suite: (backend: Backend) => void): void => {
  for (const backend of backends) describe(`${name} (${backend.name})`, () => suite(backend));
};

/** Runs a test body with a fresh storage of the backend and a scope. */
export const run =
  (backend: Backend) =>
  <A, E>(
    body: Effect.Effect<A, E, AgentStorage | Scope.Scope>,
  ): Effect.Effect<A, E | StorageFailure> =>
    body.pipe(Effect.provide(backend.layer, { local: true }), Effect.scoped);

/** A gate that holds calls until released and reports when the first held call arrives. */
export interface Gate {
  readonly entered: Effect.Effect<void>;
  readonly release: Effect.Effect<void>;
}

interface Held {
  readonly gate: Deferred.Deferred<void>;
  readonly entered: Deferred.Deferred<void>;
}

/** Storage with observable commits, held calls and injected commit failures. */
export type ControlledStorage = AgentStorage["Service"] & Controls;

/**
 * Hooks a test sets to observe or change one storage call: each gets the call's arguments and the call
 * itself as `next`, and returns what the caller sees.
 */
export interface Intercepts {
  /** One storage call, with every batch it was given. */
  commit?:
    | ((
        batches: ReadonlyArray<ReadonlyArray<StorageWrite>>,
        next: StorageCall<"commit">,
      ) => StorageCall<"commit">)
    | undefined;
  /** Each durability receipt the storage gives, with the batch it stored, before the Session takes it. */
  receive?: ((writes: ReadonlyArray<StorageWrite>, seq: Seq) => void) | undefined;
  mintId?:
    | ((next: AgentStorage["Service"]["mintId"]) => AgentStorage["Service"]["mintId"])
    | undefined;
  task?: ((id: TaskId, next: StorageCall<"task">) => StorageCall<"task">) | undefined;
  conversation?:
    | ((id: ConversationId, next: StorageCall<"conversation">) => StorageCall<"conversation">)
    | undefined;
  scanEntries?:
    | ((
        args: Parameters<AgentStorage["Service"]["scanEntries"]>,
        next: StorageCall<"scanEntries">,
      ) => StorageCall<"scanEntries">)
    | undefined;
  scanSubmissions?:
    | ((
        args: Parameters<AgentStorage["Service"]["scanSubmissions"]>,
        next: StorageCall<"scanSubmissions">,
      ) => StorageCall<"scanSubmissions">)
    | undefined;
}

type StorageCall<K extends "commit" | "task" | "conversation" | "scanEntries" | "scanSubmissions"> =
  ReturnType<AgentStorage["Service"][K]>;

interface Controls {
  /** Per-call hooks; see `Intercepts`. */
  readonly intercept: Intercepts;
  /** Simulates a crash during the held commit: it never reaches storage, and later commits proceed. */
  readonly crash: Effect.Effect<void>;
  /** Exact borrowed batches the Session admitted. */
  readonly admittedCommits: Array<ReadonlyArray<StorageWrite>>;
  /** Detached batches for value assertions. */
  readonly commits: Array<ReadonlyArray<StorageWrite>>;
  readonly mintCount: number;
  readonly documentReadCount: number;
  /** Set while the Session takes a durability receipt. */
  readonly adopting: boolean;
  /** Set from a commit's start until it settles, its `intercept.commit` included. */
  readonly committing: boolean;
  readonly holdCommits: Effect.Effect<Gate>;
  readonly holdFindDocument: Effect.Effect<Gate>;
  /** The next commit fails with a `StorageFailure` whose message is `message`, before reaching storage. */
  readonly failNextCommit: (message: string) => void;
  /** The next commit dies with a defect whose message is `message`, before reaching storage. */
  readonly dieNextCommit: (message: string) => void;
}

const wait = (held: Held | undefined) =>
  held === undefined
    ? Effect.void
    : Deferred.succeed(held.entered, undefined).pipe(Effect.andThen(Deferred.await(held.gate)));

/** Wraps the `AgentStorage` in context. */
export const controlledStorage: Effect.Effect<ControlledStorage, never, AgentStorage> = Effect.gen(
  function* () {
    const storage = yield* AgentStorage;
    const admittedCommits: Array<ReadonlyArray<StorageWrite>> = [];
    const commits: Array<ReadonlyArray<StorageWrite>> = [];
    let mintCount = 0;
    let documentReadCount = 0;
    let commitGate: Held | undefined;
    let crashed: Held | undefined;
    const intercept: Intercepts = {};
    let findGate: Held | undefined;
    let commitFailure: string | undefined;
    let commitDefect: string | undefined;
    let adopting = false;
    let committing = false;

    const hold = (set: (held: Held | undefined) => void, current: () => Held | undefined) =>
      Effect.sync((): Gate => {
        const held: Held = { gate: Deferred.makeUnsafe(), entered: Deferred.makeUnsafe() };
        set(held);
        return {
          entered: Deferred.await(held.entered),
          release: Effect.sync(() => {
            if (current() === held) set(undefined);
            Deferred.doneUnsafe(held.gate, Effect.void);
          }),
        };
      });
    return {
      ...storage,
      intercept,
      // The held commit dies without reaching storage, so
      // the crashed Session fails and closes instead of waiting for it forever.
      crash: Effect.sync(() => {
        const held = commitGate;
        if (held === undefined) return;
        crashed = held;
        commitGate = undefined;
        Deferred.doneUnsafe(held.gate, Effect.void);
      }),
      admittedCommits,
      commits,
      get mintCount() {
        return mintCount;
      },
      get documentReadCount() {
        return documentReadCount;
      },
      get adopting() {
        return adopting;
      },
      get committing() {
        return committing;
      },
      holdCommits: hold(
        (held) => {
          commitGate = held;
        },
        () => commitGate,
      ),
      holdFindDocument: hold(
        (held) => {
          findGate = held;
        },
        () => findGate,
      ),
      failNextCommit: (message) => {
        commitFailure = message;
      },
      dieNextCommit: (message) => {
        commitDefect = message;
      },
      commit: (batches, receive) =>
        Effect.gen(function* () {
          committing = true;
          // A call that fails before storage records only its first batch.
          const failing = commitDefect !== undefined || commitFailure !== undefined;
          for (const writes of failing ? batches.slice(0, 1) : batches) {
            admittedCommits.push(writes);
            commits.push(structuredClone(writes));
          }
          const held = commitGate;
          yield* wait(held);
          if (held !== undefined && crashed === held) {
            return yield* Effect.die(new Error("Storage crashed during this commit"));
          }
          const defect = commitDefect;
          const failure = commitFailure;
          if (defect !== undefined) {
            commitDefect = undefined;
            return yield* Effect.die(new Error(defect));
          }
          if (failure !== undefined) {
            commitFailure = undefined;
            return yield* new StorageFailure({
              reason: new AgentStorageModule.Backend({ message: failure, cause: failure }),
            });
          }
          const step = (index: number, seq: Seq) => {
            const writes = batches[index];
            if (writes !== undefined) intercept.receive?.(writes, seq);
            adopting = true;
            try {
              receive(index, seq);
            } finally {
              adopting = false;
            }
          };
          const hook = intercept.commit;
          return yield* hook === undefined
            ? storage.commit(batches, step)
            : hook(batches, storage.commit(batches, step));
        }).pipe(
          Effect.ensuring(
            Effect.sync(() => {
              committing = false;
            }),
          ),
        ),
      task: (id) => {
        const hook = intercept.task;
        return hook === undefined ? storage.task(id) : hook(id, storage.task(id));
      },
      conversation: (id) => {
        const hook = intercept.conversation;
        return hook === undefined ? storage.conversation(id) : hook(id, storage.conversation(id));
      },
      scanEntries: (...args) => {
        const hook = intercept.scanEntries;
        return hook === undefined
          ? storage.scanEntries(...args)
          : hook(args, storage.scanEntries(...args));
      },
      scanSubmissions: (...args) => {
        const hook = intercept.scanSubmissions;
        return hook === undefined
          ? storage.scanSubmissions(...args)
          : hook(args, storage.scanSubmissions(...args));
      },
      mintId: Effect.suspend(() => {
        mintCount++;
        const hook = intercept.mintId;
        return hook === undefined ? storage.mintId : hook(storage.mintId);
      }),
      document: (id, at) =>
        Effect.suspend(() => {
          documentReadCount++;
          return storage.document(id, at);
        }),
      findDocument: (address, at) =>
        wait(findGate).pipe(Effect.andThen(storage.findDocument(address, at))),
    };
  },
);

/** Session kernel plus its controlled storage and every committed publication. */
export interface TestSession {
  readonly storage: ControlledStorage;
  readonly session: internal.SessionKernel;
  readonly publications: Array<CommitPublication>;
}

/** Opens a Session kernel over a controlled wrapper of the storage in context. */
export const openTestSession = (
  options: SessionOptions = {},
): Effect.Effect<TestSession, never, AgentStorage | Scope.Scope> =>
  Effect.gen(function* () {
    const storage = yield* controlledStorage;
    const session = yield* openSession(storage, options);
    const publications: Array<CommitPublication> = [];
    yield* session.observeCommits({
      advance: (publication) => {
        publications.push(publication);
      },
      end: () => {},
    });
    return { storage, session, publications };
  }).pipe(Effect.orDie);

/** Opens another Session kernel over the same storage, as a reopened process would. */
export const openSession = (
  storage: AgentStorage["Service"],
  options: SessionOptions = {},
): Effect.Effect<internal.SessionKernel, never, Scope.Scope> =>
  internal.make(options).pipe(Effect.provideService(AgentStorage, storage));

export const documentChanges = (
  publication: CommitPublication,
): ReadonlyArray<Extract<DocumentCommitChange, { readonly type: "document" }>> =>
  publication.changes.filter(
    (change): change is Extract<DocumentCommitChange, { readonly type: "document" }> =>
      change.type === "document",
  );

/** Creates one conversation and returns its ID. */
export const createConversation = (session: internal.SessionKernel) =>
  session.commit(
    Effect.gen(function* () {
      const tx = yield* Tx;
      return (yield* tx.createConversation({ ownership: { kind: "ownerless" } })).id;
    }),
  );

/** Lets forked consumers run: a bounded number of scheduler turns. */
export const flush: Effect.Effect<void> = Effect.forEach(
  Array.from({ length: 50 }),
  () => Effect.yieldNow,
  {
    discard: true,
  },
);

/** The rendered cause of an effect's failure or defect, to check messages; fails the test when it succeeds. */
export const failureMessage = <A, E>(effect: Effect.Effect<A, E>): Effect.Effect<string> =>
  Effect.exit(effect).pipe(
    Effect.flatMap((exit) =>
      exit._tag === "Success"
        ? Effect.die(new Error("expected a failure"))
        : Effect.succeed(Cause.pretty(exit.cause)),
    ),
  );

/** Acquires a state or watch in its own scope; closing the scope disposes or stops it. */
export const acquire = <A, E>(open: Effect.Effect<Option.Option<A>, E, Scope.Scope>) =>
  Effect.gen(function* () {
    const scope = yield* Scope.make();
    const value = Option.getOrThrow(yield* Scope.provide(open, scope));
    return { value, release: Scope.close(scope, Exit.void) };
  });

/**
 * A Session kernel for setup over `storage` in a scope of its own, used inside `body` and then closed, before a
 * Harness opens over the same storage.
 */
export const withSession = <A, E>(
  storage: AgentStorage["Service"],
  body: (session: internal.SessionKernel) => Effect.Effect<A, E>,
) =>
  Effect.gen(function* () {
    const scope = yield* Scope.make();
    const session = yield* openSession(storage).pipe(Scope.provide(scope));
    return yield* body(session).pipe(Effect.ensuring(Scope.close(scope, Exit.void)));
  });

/**
 * Records, from now on, every storage call the controlled storage's hooks see, by name. A Session that has ended
 * makes none, and the Session does not close storage. Replaces the hooks the test had set.
 */
export const recordCalls = (storage: ControlledStorage): ReadonlyArray<string> => {
  const calls: Array<string> = [];
  storage.intercept.commit = (_batches, next) => {
    calls.push("commit");
    return next;
  };
  storage.intercept.mintId = (next) => {
    calls.push("mintId");
    return next;
  };
  storage.intercept.task = (_id, next) => {
    calls.push("task");
    return next;
  };
  storage.intercept.conversation = (_id, next) => {
    calls.push("conversation");
    return next;
  };
  storage.intercept.scanEntries = (_args, next) => {
    calls.push("scanEntries");
    return next;
  };
  storage.intercept.scanSubmissions = (_args, next) => {
    calls.push("scanSubmissions");
    return next;
  };
  return calls;
};

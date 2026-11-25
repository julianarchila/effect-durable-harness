/**
 * The backend-independent conformance suite of `AgentStorage`. Run it against
 * every backend.
 *
 * @since 0.0.0
 */
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Predicate from "effect/Predicate";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import {
  AgentStorage,
  type CommitResult,
  type CommittedEntry,
  type Cursor,
  type Page,
  type ScanOrder,
  StorageFailure,
  type StorageFailureReason,
  StorageRequestError,
  type StorageWrite,
  type SubmissionQuery,
} from "../AgentStorage.ts";
import {
  ConversationId,
  DocumentId,
  EntryId,
  ROOT_CONVERSATION_ID,
  type Seq,
  SubmissionId,
  TaskId,
} from "../Ids.ts";
import type {
  DocumentCreate,
  EntryRecord,
  LiveTaskRecord,
  SettledTaskRecord,
  SubmissionRecord,
  TaskRecord,
  TaskStatus,
} from "../Records.ts";
import { commitWrites } from "./CommitWrites.ts";

// -----------------------------------------------------------------------------
// public API
// -----------------------------------------------------------------------------

// A failed assertion is a typed failure, because then the cases need no test
// runner and a Durable Object can run them behind a route.
/**
 * An assertion of a conformance case did not hold. The message says what was
 * expected and what the storage returned.
 *
 * @category errors
 * @since 0.0.0
 */
export class ConformanceFailure extends Schema.TaggedError<ConformanceFailure>()(
  "ConformanceFailure",
  { message: Schema.String },
) {}

// A service, because a case is an Effect: `open` builds the storage in the
// caller's scope, and closing that scope closes the storage.
/**
 * How a backend supplies storage to the cases. Provide it with `layer`, or
 * with a custom `open` when a backend needs one, such as a storage rebuilt
 * after every commit.
 *
 * @category services
 * @since 0.0.0
 */
export class ConformanceStorage extends Context.Service<
  ConformanceStorage,
  {
    /** Builds one fresh, empty storage in the caller's scope; closing the scope closes it. */
    readonly open: Effect.Effect<AgentStorage["Service"], StorageFailure, Scope.Scope>;
  }
>()("@effect-durable-harness/core/testing/AgentStorageConformance/ConformanceStorage") {}

/**
 * `ConformanceStorage` that builds the given storage layer anew for every
 * case, so each case starts from an empty storage.
 *
 * @category layers
 * @since 0.0.0
 */
export const layer = (
  storage: Layer.Layer<AgentStorage, StorageFailure>,
): Layer.Layer<ConformanceStorage> =>
  Layer.succeed(ConformanceStorage, {
    open: Layer.build(storage).pipe(Effect.map(Context.get(AgentStorage))),
  });

/**
 * One named, runner-independent conformance case. In vitest, run it with
 * `it.effect(case.name, () => case.run.pipe(Effect.provide(storage)))`.
 *
 * @category models
 * @since 0.0.0
 */
export interface ConformanceCase {
  readonly name: string;
  readonly run: Effect.Effect<
    void,
    ConformanceFailure | StorageFailure | StorageRequestError,
    ConformanceStorage
  >;
}

// -----------------------------------------------------------------------------
// assertions
// -----------------------------------------------------------------------------

type Storage = AgentStorage["Service"];
type CaseError = ConformanceFailure | StorageFailure | StorageRequestError;

const show = <A>(value: A): string => {
  try {
    return JSON.stringify(value) ?? String(value);
  } catch {
    return String(value);
  }
};

/** Exact deep equality of JSON-like values, as vitest's `toEqual` for JSON. */
const deepEqual = <A>(left: A, right: A): boolean => {
  if (Object.is(left, right)) return true;
  if (Array.isArray(left) || Array.isArray(right)) {
    if (!Array.isArray(left) || !Array.isArray(right) || left.length !== right.length) return false;
    return left.every((item, index) => deepEqual(item, right[index]));
  }
  if (!Predicate.isObject(left) || !Predicate.isObject(right)) return false;
  const leftEntries = Object.entries(left);
  const rightEntries = new Map(Object.entries(right));
  if (leftEntries.length !== rightEntries.size) return false;
  return leftEntries.every(
    ([key, value]) => rightEntries.has(key) && deepEqual(value, rightEntries.get(key)),
  );
};

/** Partial deep match, as vitest's `toMatchObject`: extra object keys in `actual` are allowed. */
const deepMatch = <A, B>(actual: A, expected: B): boolean => {
  if (Array.isArray(expected)) {
    return (
      Array.isArray(actual) &&
      actual.length === expected.length &&
      expected.every((item, index) => deepMatch(actual[index], item))
    );
  }
  if (Predicate.isObject(expected)) {
    if (!Predicate.isObject(actual) || Array.isArray(actual)) return false;
    const actualEntries = new Map(Object.entries(actual));
    return Object.entries(expected).every(
      ([key, value]) => actualEntries.has(key) && deepMatch(actualEntries.get(key), value),
    );
  }
  return Object.is(actual, expected);
};

const fail = (message: string) => Effect.fail(new ConformanceFailure({ message }));

const expectTrue = (condition: boolean, message: string): Effect.Effect<void, ConformanceFailure> =>
  condition ? Effect.void : fail(message);

const expectEqual = <A>(actual: A, expected: A, label = "value") =>
  expectTrue(
    deepEqual(actual, expected),
    `Expected ${label} to equal ${show(expected)}, got ${show(actual)}`,
  );

const expectMatch = <A, B>(actual: A, expected: B, label = "value") =>
  expectTrue(
    deepMatch(actual, expected),
    `Expected ${label} to match ${show(expected)}, got ${show(actual)}`,
  );

/** The lookup's value; a failure when it is absent. */
const some = <A, E>(lookup: Effect.Effect<Option.Option<A>, E>, label = "value") =>
  Effect.flatMap(lookup, (found) =>
    Option.match(found, {
      onNone: () => fail(`Expected ${label} to be present`),
      onSome: Effect.succeed,
    }),
  );

const expectNone = <A, E>(lookup: Effect.Effect<Option.Option<A>, E>, label = "value") =>
  Effect.flatMap(lookup, (found) =>
    Option.match(found, {
      onNone: () => Effect.void,
      onSome: (value) => fail(`Expected ${label} to be absent, got ${show(value)}`),
    }),
  );

// A check names the error tag, and the `StorageFailure` reason where one is
// expected, because errors are typed.
type Expected =
  | { readonly tag: "StorageRequestError" }
  | { readonly tag: "StorageFailure"; readonly reason?: StorageFailureReason["_tag"] };

const describeExpected = (expected: Expected): string =>
  expected.tag === "StorageFailure" && expected.reason !== undefined
    ? `StorageFailure(${expected.reason})`
    : expected.tag;

/** The operation must fail with the expected error whose message includes `includes`. */
const expectError = <A, R>(
  operation: Effect.Effect<A, StorageFailure | StorageRequestError, R>,
  expected: Expected,
  includes: string,
): Effect.Effect<void, ConformanceFailure, R> =>
  Effect.flatMap(Effect.exit(operation), (exit) => {
    if (Exit.isSuccess(exit)) {
      return fail(`Expected ${describeExpected(expected)} "${includes}", got ${show(exit.value)}`);
    }
    const error = Exit.findErrorOption(exit);
    if (Option.isNone(error)) {
      return fail(
        `Expected ${describeExpected(expected)} "${includes}", got a defect or interruption`,
      );
    }
    const actual = error.value;
    const failure = Predicate.isTagged(actual, "StorageFailure")
      ? Option.some(actual)
      : Option.none();
    const actualTag = Option.isSome(failure) ? "StorageFailure" : "StorageRequestError";
    const reasonMatches =
      expected.tag === "StorageRequestError" ||
      expected.reason === undefined ||
      Option.exists(failure, (found) => found.reason._tag === expected.reason);
    return expectTrue(
      actualTag === expected.tag && reasonMatches && actual.message.includes(includes),
      `Expected ${describeExpected(expected)} "${includes}", got ${actualTag}: ${actual.message}`,
    );
  });

const rejected = { tag: "StorageFailure", reason: "Rejected" } as const;

/** The commit result names a refusal, a `Rejected` one whose message includes `includes`. */
const expectRefused = (result: CommitResult, includes: string) =>
  expectError(
    result.refused === undefined ? Effect.void : Effect.fail(result.refused),
    rejected,
    includes,
  );
const requestError = { tag: "StorageRequestError" } as const;

/** Replace or extend the container at `path` of a returned value, to prove reads are detached. */
const mutateAt = (
  value: Schema.Json | undefined,
  path: ReadonlyArray<string | number>,
  change: (container: Array<Schema.Json> | { [key: string]: Schema.Json }) => void,
): Effect.Effect<void, ConformanceFailure> => {
  let current = value;
  for (const segment of path) {
    if (Array.isArray(current) && Predicate.isNumber(segment)) current = current[segment];
    else if (
      Predicate.isObject(current) &&
      !Array.isArray(current) &&
      Predicate.isString(segment)
    ) {
      current = Object.getOwnPropertyDescriptor(current, segment)?.value;
    } else return fail(`Path ${show(path)} does not resolve in ${show(value)}`);
  }
  if (!Predicate.isObjectOrArray(current)) return fail(`Path ${show(path)} is not a container`);
  // SAFETY: storage returns plain, unfrozen JSON copies; the case mutates one to prove it is detached.
  change(current as Array<Schema.Json> | { [key: string]: Schema.Json });
  return Effect.void;
};

const push =
  (item: Schema.Json) =>
  (container: Array<Schema.Json> | { [key: string]: Schema.Json }): void => {
    if (Array.isArray(container)) container.push(item);
  };

const set =
  (key: string, item: Schema.Json) =>
  (container: Array<Schema.Json> | { [key: string]: Schema.Json }): void => {
    if (!Array.isArray(container)) {
      Object.defineProperty(container, key, {
        value: item,
        writable: true,
        enumerable: true,
        configurable: true,
      });
    }
  };

const ownValue = (value: Schema.Json | undefined, key: string): Schema.Json | undefined =>
  Predicate.isObject(value) ? Object.getOwnPropertyDescriptor(value, key)?.value : undefined;

/** `JSON.parse` keeps `__proto__` as an own key, which an object literal cannot express. */
const parseJson = (text: string): Schema.JsonObject => JSON.parse(text);

const roundTrip = (cursor: Cursor): Cursor => JSON.parse(JSON.stringify(cursor));

// -----------------------------------------------------------------------------
// fixtures
// -----------------------------------------------------------------------------

const mintEntryId = (storage: Storage) => Effect.map(storage.mintId, EntryId.make);
const mintTaskId = (storage: Storage) => Effect.map(storage.mintId, TaskId.make);
const mintConversationId = (storage: Storage) => Effect.map(storage.mintId, ConversationId.make);
const mintSubmissionId = (storage: Storage) => Effect.map(storage.mintId, SubmissionId.make);
const mintDocumentId = (storage: Storage) => Effect.map(storage.mintId, DocumentId.make);

const createRoot = (storage: Storage) =>
  commitWrites(storage, [{ type: "conversation", value: { id: ROOT_CONVERSATION_ID } }]).pipe(
    Effect.as(ROOT_CONVERSATION_ID),
  );

const pendingTask = (
  id: TaskId,
  conversationId: ConversationId,
  phase = "ready",
): LiveTaskRecord => ({
  id,
  conversationId,
  kind: "test.task",
  version: 1,
  input: { value: id },
  state: { status: "pending", checkpoint: { phase } },
  background: false,
  abortRequested: false,
});

const entry = (
  id: EntryId,
  conversationId: ConversationId,
  kind = "message",
  extra: Partial<Omit<EntryRecord, "id" | "conversationId" | "kind">> = {},
): EntryRecord => ({ id, conversationId, kind, ...extra });

/** A rewindable conversation document of the batched-copy case. */
const batchCopy = (id: DocumentId, conversationId: ConversationId): DocumentCreate => ({
  id,
  kind: "batch.copy",
  scope: { kind: "conversation", conversationId },
  history: "rewindable",
  fork: "asOf",
});

const ids = <A extends { readonly id: number }>(page: Page<A>): ReadonlyArray<number> =>
  page.items.map(({ id }) => id);

const withOrder = (order: ScanOrder | undefined) => (order === undefined ? {} : { order });

const rows = (offset: number, prefix: string) =>
  Array.from({ length: 512 }, (_, value) => ({
    value: offset + value,
    stable: `${prefix}-${value}`,
  }));

const cursorOf = <A>(page: Page<A>): Cursor | undefined => Option.getOrUndefined(page.next);

/**
 * `storage` with every commit checked against the receipt contract: one `receive` per stored batch, in
 * order, with increasing seqs, all before the call settles.
 *
 * `receive` is synchronous and cannot fail its Effect, so the checker collects what it saw and the case
 * fails at its end.
 */
const checkReceipts = (storage: Storage, violations: Array<string>): Storage => ({
  ...storage,
  commit: (batches, receive) =>
    Effect.suspend(() => {
      const seqs: Array<Seq> = [];
      let settled = false;
      return storage
        .commit(batches, (index, seq) => {
          if (settled) violations.push("Storage.commit() received a batch after it settled");
          if (index !== seqs.length) {
            violations.push(`Expected receipt index ${seqs.length}, got ${index}`);
          }
          const last = seqs.at(-1);
          if (last !== undefined && seq <= last) {
            violations.push(`Expected receipt seq ${seq} to be greater than ${last}`);
          }
          seqs.push(seq);
          receive(index, seq);
        })
        .pipe(
          Effect.tap((result) =>
            Effect.sync(() => {
              if (result.stored !== seqs.length) {
                violations.push(`Expected ${result.stored} receipts, got ${seqs.length}`);
              }
            }),
          ),
          Effect.ensuring(
            Effect.sync(() => {
              settled = true;
            }),
          ),
        );
    }),
});

const conformanceCase = (
  name: string,
  test: (storage: Storage) => Effect.Effect<void, CaseError>,
): ConformanceCase => ({
  name,
  run: Effect.scoped(
    Effect.gen(function* () {
      const conformanceStorage = yield* ConformanceStorage;
      const violations: Array<string> = [];
      yield* test(checkReceipts(yield* conformanceStorage.open, violations));
      if (violations.length > 0) return yield* fail(violations.join("; "));
    }),
  ),
});

// -----------------------------------------------------------------------------
// cases
// -----------------------------------------------------------------------------

/**
 * The 32 cases of the storage conformance suite.
 * Each case needs a fresh storage from `ConformanceStorage`, and every commit
 * is checked against the receipt contract.
 *
 * @category cases
 * @since 0.0.0
 */
export const cases: ReadonlyArray<ConformanceCase> = [
  conformanceCase("reserves ID 1 for the immutable root conversation", (storage) =>
    Effect.gen(function* () {
      yield* expectEqual(yield* storage.mintId, 2, "first minted ID");
      yield* expectEqual(yield* createRoot(storage), ROOT_CONVERSATION_ID);
      yield* expectEqual(yield* some(storage.conversation(ROOT_CONVERSATION_ID)), {
        id: ROOT_CONVERSATION_ID,
      });
      yield* expectError(
        commitWrites(storage, [{ type: "conversation", value: { id: ROOT_CONVERSATION_ID } }]),
        rejected,
        `ID ${ROOT_CONVERSATION_ID} already belongs to conversation`,
      );
    }),
  ),

  conformanceCase(
    "receives a durable commit once before resolving, and never a refused one",
    (storage) =>
      Effect.gen(function* () {
        const rootId = yield* createRoot(storage);
        const events: Array<string> = [];
        const entryId = yield* mintEntryId(storage);
        const context = yield* Effect.context<never>();
        let read: Fiber.Fiber<Option.Option<CommittedEntry>, CaseError> | undefined;
        let seq: Seq | undefined;
        const result = yield* storage.commit(
          [[{ type: "entry", value: entry(entryId, rootId) }]],
          (_index, received) => {
            seq = received;
            events.push(`receive ${received}`);
            // The batch is durable when it is received: a read started now observes it. The read is forked
            // at once, on its own fiber.
            read = Effect.runForkWith(context)(storage.entry(entryId));
          },
        );
        events.push(`resolve ${result.stored}`);
        yield* expectEqual(events, [`receive ${seq}`, "resolve 1"]);
        if (read === undefined) return yield* fail("Expected a read started at the receipt");
        yield* expectEqual(Option.getOrUndefined(yield* Fiber.join(read))?.commitSeq, seq);

        events.length = 0;
        const refused = yield* storage.commit(
          [[{ type: "conversation", value: { id: rootId } }]],
          (_index, received) => events.push(`receive ${received}`),
        );
        yield* expectEqual(refused.stored, 0);
        yield* expectRefused(refused, `ID ${rootId} already belongs to conversation`);
        yield* expectEqual(events, []);

        const next = yield* commitWrites(storage, [
          { type: "entry", value: entry(yield* mintEntryId(storage), rootId) },
        ]);
        yield* expectTrue(seq !== undefined && next > seq, `Expected ${next} to follow ${seq}`);
      }),
  ),

  conformanceCase(
    "commits mixed table writes atomically and rolls all of them back on failure",
    (storage) =>
      Effect.gen(function* () {
        const rootId = yield* createRoot(storage);
        const entryId = yield* mintEntryId(storage);
        const taskId = yield* mintTaskId(storage);
        const submissionId = yield* mintSubmissionId(storage);
        const task = pendingTask(taskId, rootId);
        const input: SubmissionRecord = {
          id: submissionId,
          conversationId: rootId,
          requestId: "request-1",
          type: "input",
          status: "placed",
          entry: entryId,
        };
        const initialSeq = yield* commitWrites(storage, [
          { type: "entry", value: entry(entryId, rootId, "user", { data: { text: "hello" } }) },
          { type: "task", value: task },
          { type: "submission", value: input },
        ]);

        yield* expectEqual(yield* some(storage.entry(entryId)), {
          entry: entry(entryId, rootId, "user", { data: { text: "hello" } }),
          commitSeq: initialSeq,
        });
        yield* expectEqual(yield* some(storage.task(taskId)), task);
        yield* expectEqual(yield* some(storage.submission(submissionId)), input);

        const transientEntryId = yield* mintEntryId(storage);
        const runningTask: TaskRecord = {
          ...task,
          state: { status: "running", checkpoint: { phase: "effect" } },
        };
        const doneInput: SubmissionRecord = { ...input, status: "done", answer: transientEntryId };
        yield* expectError(
          commitWrites(storage, [
            { type: "task", value: runningTask },
            { type: "submission", value: doneInput },
            { type: "entry", value: entry(transientEntryId, rootId, "assistant") },
            { type: "conversation", value: { id: rootId } },
          ]),
          rejected,
          `ID ${rootId} already belongs to conversation`,
        );

        yield* expectEqual(yield* some(storage.task(taskId)), task);
        yield* expectEqual(yield* some(storage.submission(submissionId)), input);
        yield* expectNone(storage.entry(transientEntryId));
        const afterRollbackSeq = yield* commitWrites(storage, [
          { type: "entry", value: entry(yield* mintEntryId(storage), rootId, "after-rollback") },
        ]);
        yield* expectTrue(
          afterRollbackSeq > initialSeq,
          `Expected ${afterRollbackSeq} to be greater than ${initialSeq}`,
        );
      }),
  ),

  conformanceCase("detaches retained writes and every returned record", (storage) =>
    Effect.gen(function* () {
      const rootId = yield* createRoot(storage);
      const entryId = yield* mintEntryId(storage);
      const taskId = yield* mintTaskId(storage);
      const submissionId = yield* mintSubmissionId(storage);
      const entryData = { nested: [1, 2] };
      const checkpoint = { phase: "ready", nested: { count: 1 } };
      const detail = { codes: ["initial"] };
      const storedTask: TaskRecord = {
        ...pendingTask(taskId, rootId),
        state: { status: "pending", checkpoint },
      };
      const storedInput: SubmissionRecord = {
        id: submissionId,
        conversationId: rootId,
        type: "input",
        status: "unanswered",
        reason: "failed",
        detail,
      };
      yield* commitWrites(storage, [
        { type: "entry", value: entry(entryId, rootId, "note", { data: entryData }) },
        { type: "task", value: storedTask },
        { type: "submission", value: storedInput },
      ]);

      entryData.nested.push(3);
      checkpoint.nested.count = 2;
      detail.codes.push("mutated");
      const expectDetached = Effect.gen(function* () {
        yield* expectEqual(
          (yield* some(storage.entry(entryId))).entry.data,
          { nested: [1, 2] },
          "entry data",
        );
        yield* expectEqual(
          (yield* some(storage.task(taskId))).state,
          { status: "pending", checkpoint: { phase: "ready", nested: { count: 1 } } },
          "task state",
        );
        const submission = yield* some(storage.submission(submissionId));
        yield* expectEqual(
          submission.status === "unanswered" ? submission.detail : undefined,
          { codes: ["initial"] },
          "submission detail",
        );
      });
      yield* expectDetached;

      yield* mutateAt((yield* some(storage.entry(entryId))).entry.data, ["nested"], push(9));
      const readTask = yield* some(storage.task(taskId));
      if (readTask.state.status === "pending") {
        yield* mutateAt(readTask.state.checkpoint, ["nested"], set("count", 9));
      }
      const readInput = yield* some(storage.submission(submissionId));
      if (readInput.status === "unanswered") {
        yield* mutateAt(readInput.detail, ["codes"], push("read mutation"));
      }
      yield* expectDetached;
    }),
  ),

  conformanceCase(
    "detaches prototype-like JSON keys without changing object prototypes",
    (storage) =>
      Effect.gen(function* () {
        const rootId = yield* createRoot(storage);
        const entryId = yield* mintEntryId(storage);
        const data = parseJson(
          '{"__proto__":{"polluted":false},"constructor":{"label":"stored"},"toString":"value"}',
        );
        yield* commitWrites(storage, [
          { type: "entry", value: entry(entryId, rootId, "note", { data }) },
        ]);

        yield* mutateAt(data, ["__proto__"], set("polluted", true));
        yield* mutateAt(data, ["constructor"], set("label", "mutated"));
        const firstRead = (yield* some(storage.entry(entryId))).entry.data;
        yield* expectTrue(
          Predicate.isObject(firstRead) && Object.getPrototypeOf(firstRead) === Object.prototype,
          "Expected the read value to have Object.prototype",
        );
        yield* expectTrue(
          Predicate.isObject(firstRead) && Object.hasOwn(firstRead, "__proto__"),
          "Expected __proto__ to stay an own key",
        );
        yield* expectEqual(ownValue(firstRead, "__proto__"), { polluted: false }, "__proto__");
        yield* expectEqual(ownValue(firstRead, "constructor"), { label: "stored" }, "constructor");
        yield* expectEqual(ownValue(firstRead, "toString"), "value", "toString");
        yield* expectTrue(
          !Object.hasOwn(Object.prototype, "polluted"),
          "Expected Object.prototype to stay unpolluted",
        );

        yield* mutateAt(firstRead, ["__proto__"], set("polluted", true));
        const secondRead = (yield* some(storage.entry(entryId))).entry.data;
        yield* expectEqual(ownValue(secondRead, "__proto__"), { polluted: false }, "__proto__");
        yield* expectEqual(ownValue(secondRead, "constructor"), { label: "stored" }, "constructor");
        yield* expectEqual(ownValue(secondRead, "toString"), "value", "toString");
      }),
  ),

  conformanceCase("indexes entries committed out of ID order", (storage) =>
    Effect.gen(function* () {
      const rootId = yield* createRoot(storage);
      yield* commitWrites(storage, [
        { type: "entry", value: entry(EntryId.make(30), rootId) },
        { type: "entry", value: entry(EntryId.make(10), rootId) },
        {
          type: "entry",
          value: entry(EntryId.make(20), rootId, "marker", { head: EntryId.make(10) }),
        },
      ]);

      yield* expectEqual(
        ids(yield* storage.scanEntries({ conversationId: rootId }, 10)),
        [30, 20, 10],
      );
      yield* expectEqual((yield* some(storage.findLatestHeadMarker(rootId))).id, 20);
    }),
  ),

  conformanceCase("continues an entry cursor below its last item after a newer commit", (storage) =>
    Effect.gen(function* () {
      const rootId = yield* createRoot(storage);
      const oldestId = yield* mintEntryId(storage);
      const middleId = yield* mintEntryId(storage);
      const newestId = yield* mintEntryId(storage);
      yield* commitWrites(storage, [
        { type: "entry", value: entry(oldestId, rootId) },
        { type: "entry", value: entry(middleId, rootId) },
        { type: "entry", value: entry(newestId, rootId) },
      ]);

      const first = yield* storage.scanEntries({ conversationId: rootId }, 2);
      yield* expectEqual(ids(first), [newestId, middleId]);
      const appendedId = yield* mintEntryId(storage);
      yield* commitWrites(storage, [{ type: "entry", value: entry(appendedId, rootId) }]);
      const second = yield* storage.scanEntries({ conversationId: rootId }, 2, cursorOf(first));
      yield* expectEqual(ids(second), [oldestId]);
      yield* expectTrue(Option.isNone(second.next), "Expected the last page to have no cursor");
    }),
  ),

  conformanceCase("paginates conversations by opaque cursor in ascending ID order", (storage) =>
    Effect.gen(function* () {
      const rootId = yield* createRoot(storage);
      const secondId = yield* mintConversationId(storage);
      const thirdId = yield* mintConversationId(storage);
      yield* commitWrites(storage, [
        { type: "conversation", value: { id: thirdId } },
        { type: "conversation", value: { id: secondId } },
      ]);

      const first = yield* storage.scanConversations({}, 2);
      yield* expectEqual(ids(first), [rootId, secondId]);
      const next = yield* some(Effect.succeed(first.next), "cursor");
      const second = yield* storage.scanConversations({}, 2, roundTrip(next));
      yield* expectEqual(ids(second), [thirdId]);
      yield* expectTrue(Option.isNone(second.next), "Expected the last page to have no cursor");
    }),
  ),

  conformanceCase("filters and pages conversations by durable owner edges", (storage) =>
    Effect.gen(function* () {
      const rootId = yield* createRoot(storage);
      const otherOwnerId = yield* mintConversationId(storage);
      const firstTaskId = yield* mintTaskId(storage);
      const secondTaskId = yield* mintTaskId(storage);
      const firstId = yield* mintConversationId(storage);
      const secondId = yield* mintConversationId(storage);
      const thirdId = yield* mintConversationId(storage);
      yield* commitWrites(storage, [
        { type: "conversation", value: { id: otherOwnerId } },
        {
          type: "conversation",
          value: { id: firstId, owner: { conversationId: rootId, taskId: firstTaskId } },
        },
        {
          type: "conversation",
          value: { id: secondId, owner: { conversationId: rootId, taskId: secondTaskId } },
        },
        {
          type: "conversation",
          value: { id: thirdId, owner: { conversationId: otherOwnerId, taskId: firstTaskId } },
        },
      ]);

      const first = yield* storage.scanConversations({ ownerConversationId: rootId }, 1);
      yield* expectEqual(ids(first), [firstId]);
      yield* expectTrue(Option.isSome(first.next), "Expected a cursor");
      const second = yield* storage.scanConversations(
        { ownerConversationId: rootId },
        1,
        cursorOf(first),
      );
      yield* expectEqual(ids(second), [secondId]);
      yield* expectTrue(Option.isNone(second.next), "Expected the last page to have no cursor");
      yield* expectEqual(ids(yield* storage.scanConversations({ ownerTaskId: firstTaskId }, 10)), [
        firstId,
        thirdId,
      ]);
      yield* expectEqual(
        ids(
          yield* storage.scanConversations(
            { ownerConversationId: rootId, ownerTaskId: firstTaskId },
            10,
          ),
        ),
        [firstId],
      );
    }),
  ),

  conformanceCase("scans deep fork history newest-first through every ancestor cap", (storage) =>
    Effect.gen(function* () {
      const rootId = yield* createRoot(storage);
      const rootFirst = yield* mintEntryId(storage);
      const rootForkPoint = yield* mintEntryId(storage);
      const rootExcludedSameCommit = yield* mintEntryId(storage);
      const rootEntriesSeq = yield* commitWrites(storage, [
        { type: "entry", value: entry(rootFirst, rootId) },
        { type: "entry", value: entry(rootForkPoint, rootId, "marker", { head: rootFirst }) },
        { type: "entry", value: entry(rootExcludedSameCommit, rootId) },
      ]);
      const childId = yield* mintConversationId(storage);
      yield* commitWrites(storage, [
        {
          type: "conversation",
          value: { id: childId, parent: { conversationId: rootId, at: rootForkPoint } },
        },
      ]);
      const childForkPoint = yield* mintEntryId(storage);
      const childExcluded = yield* mintEntryId(storage);
      yield* commitWrites(storage, [
        { type: "entry", value: entry(childForkPoint, childId, "note") },
        { type: "entry", value: entry(childExcluded, childId) },
      ]);
      const rootExcludedLater = yield* mintEntryId(storage);
      yield* commitWrites(storage, [{ type: "entry", value: entry(rootExcludedLater, rootId) }]);
      const grandchildId = yield* mintConversationId(storage);
      yield* commitWrites(storage, [
        {
          type: "conversation",
          value: { id: grandchildId, parent: { conversationId: childId, at: childForkPoint } },
        },
      ]);
      const grandchildHead = yield* mintEntryId(storage);
      const grandchildTail = yield* mintEntryId(storage);
      const grandchildEntriesSeq = yield* commitWrites(storage, [
        {
          type: "entry",
          value: entry(grandchildHead, grandchildId, "marker", { head: grandchildHead }),
        },
        { type: "entry", value: entry(grandchildTail, grandchildId) },
      ]);
      const childExcludedLater = yield* mintEntryId(storage);
      yield* commitWrites(storage, [{ type: "entry", value: entry(childExcludedLater, childId) }]);

      const first = yield* storage.scanEntries({ conversationId: grandchildId }, 2);
      yield* expectEqual(ids(first), [grandchildTail, grandchildHead]);
      const second = yield* storage.scanEntries(
        { conversationId: grandchildId },
        2,
        cursorOf(first),
      );
      yield* expectEqual(ids(second), [childForkPoint, rootForkPoint]);
      const third = yield* storage.scanEntries(
        { conversationId: grandchildId },
        2,
        cursorOf(second),
      );
      yield* expectEqual(ids(third), [rootFirst]);
      yield* expectTrue(Option.isNone(third.next), "Expected the last page to have no cursor");

      // Oldest first: the root's segment, then each fork's, each capped at the next fork point.
      const ascending = { conversationId: grandchildId, order: "ascending" } as const;
      const firstUp = yield* storage.scanEntries(ascending, 2);
      yield* expectEqual(ids(firstUp), [rootFirst, rootForkPoint]);
      const secondUp = yield* storage.scanEntries(ascending, 2, cursorOf(firstUp));
      yield* expectEqual(ids(secondUp), [childForkPoint, grandchildHead]);
      const thirdUp = yield* storage.scanEntries(
        { conversationId: grandchildId },
        2,
        cursorOf(secondUp),
      );
      yield* expectEqual(ids(thirdUp), [grandchildTail]);
      yield* expectTrue(Option.isNone(thirdUp.next), "Expected the last page to have no cursor");
      yield* expectEqual(
        ids(
          yield* storage.scanEntries(
            { ...ascending, minEntryId: rootForkPoint, maxEntryId: childForkPoint },
            10,
          ),
        ),
        [rootForkPoint, childForkPoint],
      );
      yield* expectError(
        storage.scanEntries(
          { conversationId: grandchildId, order: "descending" },
          2,
          cursorOf(firstUp),
        ),
        requestError,
        "cursor",
      );

      const currentMarker = yield* some(storage.findLatestHeadMarker(grandchildId));
      yield* expectEqual(currentMarker.id, grandchildHead);
      yield* expectEqual(currentMarker.head, grandchildHead);
      const historicalMarker = yield* some(
        storage.findLatestHeadMarker(grandchildId, childForkPoint),
      );
      yield* expectEqual(historicalMarker.id, rootForkPoint);
      yield* expectEqual(historicalMarker.head, rootFirst);
      yield* expectNone(storage.findLatestHeadMarker(grandchildId, rootFirst));

      const activeFirst = yield* storage.scanEntries(
        { conversationId: grandchildId, minEntryId: currentMarker.head },
        1,
      );
      yield* expectEqual(ids(activeFirst), [grandchildTail]);
      yield* expectTrue(Option.isSome(activeFirst.next), "Expected a cursor");
      const activeSecond = yield* storage.scanEntries(
        { conversationId: grandchildId, minEntryId: currentMarker.head },
        1,
        cursorOf(activeFirst),
      );
      yield* expectEqual(ids(activeSecond), [grandchildHead]);
      yield* expectTrue(
        Option.isNone(activeSecond.next),
        "Expected the last page to have no cursor",
      );

      yield* expectEqual(
        ids(
          yield* storage.scanEntries(
            {
              conversationId: grandchildId,
              minEntryId: historicalMarker.head,
              maxEntryId: childForkPoint,
            },
            10,
          ),
        ),
        [childForkPoint, rootForkPoint, rootFirst],
      );

      yield* expectEqual(yield* some(storage.entry(rootFirst)), {
        entry: entry(rootFirst, rootId),
        commitSeq: rootEntriesSeq,
      });
      yield* expectEqual((yield* some(storage.entry(rootForkPoint))).commitSeq, rootEntriesSeq);
      yield* expectEqual(
        (yield* some(storage.entry(grandchildHead))).commitSeq,
        grandchildEntriesSeq,
      );
      yield* expectEqual(
        (yield* some(storage.entry(grandchildTail))).commitSeq,
        grandchildEntriesSeq,
      );
      yield* expectNone(storage.entry(EntryId.make(999_999)));

      yield* expectEqual(yield* some(storage.visibleEntry(grandchildId, rootFirst)), {
        entry: entry(rootFirst, rootId),
        commitSeq: rootEntriesSeq,
      });
      yield* expectEqual(
        (yield* some(storage.visibleEntry(grandchildId, childForkPoint))).entry.conversationId,
        childId,
      );
      yield* expectEqual(
        (yield* some(storage.visibleEntry(grandchildId, grandchildTail))).commitSeq,
        grandchildEntriesSeq,
      );
      yield* expectNone(storage.visibleEntry(grandchildId, rootExcludedSameCommit));
      yield* expectNone(storage.visibleEntry(grandchildId, rootExcludedLater));
      yield* expectNone(storage.visibleEntry(grandchildId, childExcluded));
      yield* expectNone(storage.visibleEntry(grandchildId, childExcludedLater));
      yield* expectNone(storage.visibleEntry(rootId, grandchildHead));
      yield* expectNone(storage.visibleEntry(grandchildId, EntryId.make(999_999)));
      yield* expectError(
        storage.visibleEntry(ConversationId.make(999_999), rootFirst),
        requestError,
        "Unknown conversation",
      );
      yield* expectError(
        storage.scanEntries({ conversationId: ConversationId.make(999_999) }, 10),
        requestError,
        "Unknown conversation",
      );
    }),
  ),

  conformanceCase(
    "scans tables in either ID order and continues a cursor in its order",
    (storage) =>
      Effect.gen(function* () {
        const rootId = yield* createRoot(storage);
        const conversationIds: Array<number> = [rootId];
        const taskIds: Array<number> = [];
        const submissionIds: Array<number> = [];
        for (let index = 0; index < 3; index++) {
          const conversationId = yield* mintConversationId(storage);
          const taskId = yield* mintTaskId(storage);
          const submissionId = yield* mintSubmissionId(storage);
          yield* commitWrites(storage, [
            { type: "conversation", value: { id: conversationId } },
            { type: "task", value: pendingTask(taskId, rootId) },
            {
              type: "submission",
              value: { id: submissionId, conversationId: rootId, type: "input", status: "queued" },
            },
          ]);
          conversationIds.push(conversationId);
          taskIds.push(taskId);
          submissionIds.push(submissionId);
        }
        type Scan = (
          order: ScanOrder | undefined,
          cursor: Cursor | undefined,
        ) => Effect.Effect<Page<{ readonly id: number }>, StorageFailure | StorageRequestError>;
        const scans: ReadonlyArray<readonly [Scan, ReadonlyArray<number>]> = [
          [
            (order, cursor) => storage.scanConversations(withOrder(order), 2, cursor),
            conversationIds,
          ],
          [(order, cursor) => storage.scanTasks(withOrder(order), 2, cursor), taskIds],
          [(order, cursor) => storage.scanSubmissions(withOrder(order), 2, cursor), submissionIds],
        ];
        const all = (scan: Scan, order: ScanOrder | undefined) =>
          Effect.gen(function* () {
            const found: Array<number> = [];
            let page = yield* scan(order, undefined);
            found.push(...ids(page));
            while (Option.isSome(page.next)) {
              // A cursor carries its order; the query may omit it.
              page = yield* scan(undefined, roundTrip(page.next.value));
              found.push(...ids(page));
            }
            return found;
          });
        for (const [scan, expected] of scans) {
          const reversed = expected.toReversed();
          yield* expectEqual(yield* all(scan, undefined), [...expected]);
          yield* expectEqual(yield* all(scan, "ascending"), [...expected]);
          yield* expectEqual(yield* all(scan, "descending"), reversed);
          const descending = yield* scan("descending", undefined);
          yield* expectEqual(
            ids(yield* scan("descending", cursorOf(descending))),
            reversed.slice(2, 4),
          );
          yield* expectError(scan("ascending", cursorOf(descending)), requestError, "cursor");
        }
      }),
  ),

  conformanceCase("replaces complete task records and pages filtered task scans", (storage) =>
    Effect.gen(function* () {
      const rootId = yield* createRoot(storage);
      const firstId = yield* mintTaskId(storage);
      const secondId = yield* mintTaskId(storage);
      const thirdId = yield* mintTaskId(storage);
      const first: LiveTaskRecord = { ...pendingTask(firstId, rootId), memos: { winner: "first" } };
      const second: LiveTaskRecord = { ...pendingTask(secondId, rootId), background: true };
      const third: LiveTaskRecord = { ...pendingTask(thirdId, rootId), abortRequested: true };
      yield* commitWrites(storage, [
        { type: "task", value: first },
        { type: "task", value: second },
        { type: "task", value: third },
      ]);

      const running: TaskRecord = {
        ...first,
        state: { status: "running", checkpoint: { phase: "effect", attempt: 1 } },
        abortRequested: true,
      };
      yield* commitWrites(storage, [{ type: "task", value: running }]);
      yield* expectEqual(yield* some(storage.task(firstId)), running);
      const terminal: TaskRecord = {
        id: firstId,
        conversationId: rootId,
        kind: first.kind,
        version: first.version,
        input: first.input,
        state: { status: "terminal", outcome: { status: "completed", result: { entryId: 99 } } },
        background: false,
        abortRequested: true,
      };
      yield* commitWrites(storage, [{ type: "task", value: terminal }]);
      yield* expectEqual(yield* some(storage.task(firstId)), terminal);

      const pendingPage = yield* storage.scanTasks({ status: "pending" }, 1);
      yield* expectEqual(ids(pendingPage), [secondId]);
      yield* expectTrue(Option.isSome(pendingPage.next), "Expected a cursor");
      yield* expectEqual(
        ids(yield* storage.scanTasks({ status: "pending" }, 1, cursorOf(pendingPage))),
        [thirdId],
      );
      yield* expectEqual(
        (yield* storage.scanTasks({ status: "terminal", abortRequested: true }, 10)).items,
        [terminal],
      );
      yield* expectEqual(ids(yield* storage.scanTasks({ background: true }, 10)), [secondId]);
    }),
  ),

  conformanceCase("stores owners and scans waiting and completing tasks by status", (storage) =>
    Effect.gen(function* () {
      const rootId = yield* createRoot(storage);
      const ownerId = yield* mintTaskId(storage);
      const waitingId = yield* mintTaskId(storage);
      const completingId = yield* mintTaskId(storage);
      const owner = pendingTask(ownerId, rootId);
      const waiting: TaskRecord = {
        ...pendingTask(waitingId, rootId),
        owner: ownerId,
        state: {
          status: "waiting",
          checkpoint: { phase: "next" },
          on: [ownerId],
          policy: "allSettled",
        },
        memos: { kept: true },
      };
      const { state: _state, ...base } = pendingTask(completingId, rootId);
      const completing: SettledTaskRecord = {
        ...base,
        owner: ownerId,
        state: { status: "completing", outcome: { status: "failed", error: { message: "held" } } },
      };
      yield* commitWrites(
        storage,
        [owner, waiting, completing].map((value): StorageWrite => ({ type: "task", value })),
      );
      yield* expectEqual(yield* some(storage.task(waitingId)), waiting);
      yield* expectEqual(yield* some(storage.task(completingId)), completing);
      const scan = (status: TaskStatus) =>
        Effect.map(storage.scanTasks({ status }, 10), (page) => page.items);
      yield* expectEqual(yield* scan("waiting"), [waiting]);
      yield* expectEqual(yield* scan("completing"), [completing]);
      yield* expectEqual(
        (yield* scan("pending")).map(({ id }) => id),
        [ownerId],
      );
      const terminal: TaskRecord = {
        ...completing,
        state: { status: "terminal", outcome: completing.state.outcome },
      };
      yield* commitWrites(storage, [{ type: "task", value: terminal }]);
      yield* expectEqual(yield* scan("completing"), []);
      yield* expectEqual(yield* scan("terminal"), [terminal]);
    }),
  ),

  conformanceCase("stores and scans sleeping tasks by status", (storage) =>
    Effect.gen(function* () {
      const rootId = yield* createRoot(storage);
      const sleepingId = yield* mintTaskId(storage);
      const sleeping: TaskRecord = {
        ...pendingTask(sleepingId, rootId),
        state: { status: "sleeping", checkpoint: { phase: "next" }, until: 61_000 },
        memos: { kept: true },
      };
      yield* commitWrites(storage, [{ type: "task", value: sleeping }]);
      yield* expectEqual(yield* some(storage.task(sleepingId)), sleeping);
      const scan = (status: TaskStatus) =>
        Effect.map(storage.scanTasks({ status }, 10), (page) => page.items);
      yield* expectEqual(yield* scan("sleeping"), [sleeping]);
      const running: TaskRecord = {
        ...sleeping,
        state: { status: "running", checkpoint: { phase: "next" } },
      };
      yield* commitWrites(storage, [{ type: "task", value: running }]);
      yield* expectEqual(yield* scan("sleeping"), []);
      yield* expectEqual(yield* scan("running"), [running]);
    }),
  ),

  conformanceCase("round-trips sleeps on signals, any waits, and wakes", (storage) =>
    Effect.gen(function* () {
      const rootId = yield* createRoot(storage);
      const a = yield* mintTaskId(storage);
      const b = yield* mintTaskId(storage);
      const c = yield* mintTaskId(storage);
      const d = yield* mintTaskId(storage);
      const e = yield* mintTaskId(storage);
      const signals = [{ signal: "app.answer", key: "a" }];
      const memos = { "core.signal.app.answer/a": { version: 1, value: { text: "yes" } } };
      // A sleep without `until`, waiting only for a signal.
      const onSignal: TaskRecord = {
        ...pendingTask(a, rootId),
        state: { status: "sleeping", checkpoint: { phase: "next" }, signals },
      };
      const onBoth: TaskRecord = {
        ...pendingTask(b, rootId),
        state: { status: "sleeping", checkpoint: { phase: "next" }, until: 61_000, signals },
        memos,
      };
      const any: TaskRecord = {
        ...pendingTask(c, rootId),
        state: { status: "waiting", checkpoint: { phase: "next" }, on: [a, b], policy: "any" },
      };
      const pending: TaskRecord = {
        ...pendingTask(d, rootId),
        state: {
          status: "pending",
          checkpoint: { phase: "next" },
          wake: { reason: "tasks", ended: [a] },
        },
      };
      const running: TaskRecord = {
        ...pendingTask(e, rootId),
        state: {
          status: "running",
          checkpoint: { phase: "next" },
          wake: { reason: "signal", signal: "app.answer", key: "a" },
        },
      };
      const tasks = [onSignal, onBoth, any, pending, running];
      yield* commitWrites(
        storage,
        tasks.map((value) => ({ type: "task", value }) as const),
      );
      for (const task of tasks) yield* expectEqual(yield* some(storage.task(task.id)), task);
      const scan = (status: TaskStatus) =>
        Effect.map(storage.scanTasks({ status }, 10), (page) => page.items);
      yield* expectEqual(yield* scan("sleeping"), [onSignal, onBoth]);
      yield* expectEqual(yield* scan("waiting"), [any]);
    }),
  ),

  conformanceCase(
    "indexes request IDs per conversation and replaces complete submission records",
    (storage) =>
      Effect.gen(function* () {
        const rootId = yield* createRoot(storage);
        const secondConversationId = yield* mintConversationId(storage);
        yield* commitWrites(storage, [
          { type: "conversation", value: { id: secondConversationId } },
        ]);
        const firstId = yield* mintSubmissionId(storage);
        const secondId = yield* mintSubmissionId(storage);
        const otherConversationId = yield* mintSubmissionId(storage);
        const first: SubmissionRecord = {
          id: firstId,
          conversationId: rootId,
          requestId: "same",
          type: "input",
          status: "queued",
        };
        const second: SubmissionRecord = {
          id: secondId,
          conversationId: rootId,
          requestId: "other",
          type: "input",
          status: "queued",
        };
        const otherConversation: SubmissionRecord = {
          id: otherConversationId,
          conversationId: secondConversationId,
          requestId: "same",
          type: "input",
          status: "queued",
        };
        yield* commitWrites(storage, [
          { type: "submission", value: first },
          { type: "submission", value: second },
          { type: "submission", value: otherConversation },
        ]);
        yield* expectEqual(yield* some(storage.submissionByRequest(rootId, "same")), first);
        yield* expectEqual(
          yield* some(storage.submissionByRequest(secondConversationId, "same")),
          otherConversation,
        );

        const placedSecond: SubmissionRecord = {
          ...second,
          status: "placed",
          entry: yield* mintEntryId(storage),
        };
        yield* commitWrites(storage, [{ type: "submission", value: placedSecond }]);
        yield* expectEqual(yield* some(storage.submission(secondId)), placedSecond);
        yield* expectEqual(yield* some(storage.submissionByRequest(rootId, "other")), placedSecond);

        const scanIds = (query: SubmissionQuery) =>
          Effect.gen(function* () {
            const found: Array<number> = [];
            let cursor: Cursor | undefined;
            do {
              const page = yield* storage.scanSubmissions(query, 1, cursor);
              found.push(...ids(page));
              cursor = cursorOf(page);
            } while (cursor !== undefined);
            return found;
          });
        yield* expectEqual(yield* scanIds({}), [firstId, secondId, otherConversationId]);
        yield* expectEqual(yield* scanIds({ conversationId: rootId }), [firstId, secondId]);
        // A status change moves the record between status scans.
        yield* expectEqual(yield* scanIds({ status: "queued" }), [firstId, otherConversationId]);
        yield* expectEqual(yield* scanIds({ status: "placed" }), [secondId]);
        yield* expectEqual(
          yield* scanIds({ conversationId: secondConversationId, status: "queued" }),
          [otherConversationId],
        );
        yield* expectEqual(
          yield* scanIds({ conversationId: secondConversationId, status: "placed" }),
          [],
        );
        yield* expectEqual((yield* storage.scanSubmissions({ status: "placed" }, 10)).items, [
          placedSecond,
        ]);
      }),
  ),

  conformanceCase(
    "stores passive write submissions without input-only lifecycle states",
    (storage) =>
      Effect.gen(function* () {
        const rootId = yield* createRoot(storage);
        const doneId = yield* mintSubmissionId(storage);
        const failedId = yield* mintSubmissionId(storage);
        const queuedDone: SubmissionRecord = {
          id: doneId,
          conversationId: rootId,
          requestId: "passive-done",
          type: "write",
          status: "queued",
        };
        const queuedFailed: SubmissionRecord = {
          id: failedId,
          conversationId: rootId,
          requestId: "passive-failed",
          type: "write",
          status: "queued",
        };
        yield* commitWrites(storage, [
          { type: "submission", value: queuedDone },
          { type: "submission", value: queuedFailed },
        ]);

        const done: SubmissionRecord = {
          ...queuedDone,
          status: "done",
          entry: yield* mintEntryId(storage),
        };
        const unanswered: SubmissionRecord = {
          ...queuedFailed,
          status: "unanswered",
          reason: "closed",
          detail: { retryable: false },
        };
        yield* commitWrites(storage, [
          { type: "submission", value: done },
          { type: "submission", value: unanswered },
        ]);
        yield* expectEqual(yield* some(storage.submission(doneId)), done);
        yield* expectEqual(yield* some(storage.submissionByRequest(rootId, "passive-done")), done);
        yield* expectEqual(yield* some(storage.submission(failedId)), unanswered);
        yield* expectEqual(
          yield* some(storage.submissionByRequest(rootId, "passive-failed")),
          unanswered,
        );
      }),
  ),

  conformanceCase(
    "stores continue submissions, which have no entry and are never queued",
    (storage) =>
      Effect.gen(function* () {
        const rootId = yield* createRoot(storage);
        const doneId = yield* mintSubmissionId(storage);
        const failedId = yield* mintSubmissionId(storage);
        const placedDone: SubmissionRecord = {
          id: doneId,
          conversationId: rootId,
          requestId: "continue-done",
          type: "continue",
          status: "placed",
        };
        const placedFailed: SubmissionRecord = {
          id: failedId,
          conversationId: rootId,
          requestId: "continue-failed",
          type: "continue",
          status: "placed",
        };
        yield* commitWrites(storage, [
          { type: "submission", value: placedDone },
          { type: "submission", value: placedFailed },
        ]);
        yield* expectEqual(yield* some(storage.submission(doneId)), placedDone);
        yield* expectEqual((yield* storage.scanSubmissions({ status: "placed" }, 10)).items, [
          placedDone,
          placedFailed,
        ]);

        const done: SubmissionRecord = {
          ...placedDone,
          status: "done",
          answer: yield* mintEntryId(storage),
        };
        const unanswered: SubmissionRecord = {
          ...placedFailed,
          status: "unanswered",
          reason: "nothing_to_answer",
        };
        yield* commitWrites(storage, [
          { type: "submission", value: done },
          { type: "submission", value: unanswered },
        ]);
        yield* expectEqual(yield* some(storage.submission(doneId)), done);
        yield* expectEqual(yield* some(storage.submissionByRequest(rootId, "continue-done")), done);
        yield* expectEqual(yield* some(storage.submission(failedId)), unanswered);
        yield* expectEqual(
          yield* some(storage.submissionByRequest(rootId, "continue-failed")),
          unanswered,
        );
      }),
  ),

  conformanceCase(
    "reconstructs rewindable documents and preserves half-open incarnations",
    (storage) =>
      Effect.gen(function* () {
        const rootId = yield* createRoot(storage);
        const firstId = yield* mintDocumentId(storage);
        const firstRecord = {
          id: firstId,
          kind: "conversation.notes",
          scope: { kind: "conversation", conversationId: rootId },
          history: "rewindable",
          fork: "asOf",
        } satisfies DocumentCreate;
        const items = ["a"];
        const initial = { items, nested: { count: 1 } };
        const createdAt = yield* commitWrites(storage, [
          {
            type: "document.create",
            record: firstRecord,
            content: { kind: "base", version: 1, value: initial },
          },
        ]);
        const appended = ["b"];
        const changedAt = yield* commitWrites(storage, [
          {
            type: "document.change",
            id: firstId,
            content: {
              kind: "delta",
              version: 1,
              ops: [
                ["p", ["items"], 1, 0, appended],
                ["s", ["nested", "count"], 2],
              ],
            },
          },
        ]);

        items.push("caller mutation");
        appended.push("caller mutation");
        yield* expectMatch(yield* some(storage.document(firstId, createdAt)), {
          version: 1,
          value: { items: ["a"], nested: { count: 1 } },
          deltasSinceBase: 0,
        });
        const changed = yield* some(storage.document(firstId, changedAt));
        yield* expectEqual(changed.value, { items: ["a", "b"], nested: { count: 2 } });
        yield* expectEqual(changed.deltasSinceBase, 1);
        yield* mutateAt(changed.value, ["items"], push("read mutation"));
        yield* expectEqual((yield* some(storage.document(firstId, "current"))).value, {
          items: ["a", "b"],
          nested: { count: 2 },
        });

        const checkpointAt = yield* commitWrites(storage, [
          {
            type: "document.change",
            id: firstId,
            content: {
              kind: "base",
              version: 2,
              value: { items: ["checkpoint"], nested: { count: 3 } },
            },
          },
        ]);
        const replacedAt = yield* commitWrites(storage, [
          {
            type: "document.change",
            id: firstId,
            content: {
              kind: "delta",
              version: 2,
              ops: [["r", { items: ["replacement"], nested: { count: 4 } }]],
            },
          },
        ]);
        yield* expectMatch(yield* some(storage.document(firstId, changedAt)), {
          version: 1,
          value: { items: ["a", "b"], nested: { count: 2 } },
        });
        yield* expectMatch(yield* some(storage.document(firstId, checkpointAt)), {
          version: 2,
          value: { items: ["checkpoint"], nested: { count: 3 } },
          deltasSinceBase: 0,
        });
        yield* expectMatch(yield* some(storage.document(firstId, replacedAt)), {
          value: { items: ["replacement"], nested: { count: 4 } },
          deltasSinceBase: 1,
        });
        yield* expectEqual((yield* some(storage.document(firstId, "current"))).deltasSinceBase, 1);

        const secondId = yield* mintDocumentId(storage);
        const retiredAt = yield* commitWrites(storage, [
          {
            type: "document.create",
            record: { ...firstRecord, id: secondId },
            content: { kind: "base", version: 1, value: { items: ["new"] } },
          },
          { type: "document.retire", id: firstId },
          {
            type: "document.change",
            id: firstId,
            content: { kind: "delta", version: 2, ops: [["s", ["retiring"], true]] },
          },
        ]);
        const address = { kind: firstRecord.kind, scope: firstRecord.scope };
        yield* expectEqual((yield* some(storage.findDocument(address, changedAt))).id, firstId);
        yield* expectMatch(yield* some(storage.findDocument(address, retiredAt)), {
          id: secondId,
          createdAt: retiredAt,
        });
        yield* expectEqual(
          ids(yield* storage.scanDocuments({ scope: firstRecord.scope, at: changedAt }, 10)),
          [firstId],
        );
        yield* expectEqual(
          ids(yield* storage.scanDocuments({ scope: firstRecord.scope, at: retiredAt }, 10)),
          [secondId],
        );
        yield* expectNone(storage.document(firstId, retiredAt));
        yield* expectEqual((yield* some(storage.document(secondId, "current"))).value, {
          items: ["new"],
        });
      }),
  ),

  conformanceCase("streams long document tails across root replacement deltas", (storage) =>
    Effect.gen(function* () {
      const rootId = yield* createRoot(storage);
      const id = yield* mintDocumentId(storage);
      const record = {
        id,
        kind: "conversation.long-tail",
        scope: { kind: "conversation", conversationId: rootId },
        history: "rewindable",
        fork: "asOf",
      } satisfies DocumentCreate;
      const initial = { revision: 0, rows: rows(0, "row") };
      const createdAt = yield* commitWrites(storage, [
        { type: "document.create", record, content: { kind: "base", version: 1, value: initial } },
      ]);
      const beforeReplacement = structuredClone(initial);
      let beforeReplacementAt = createdAt;
      for (let revision = 1; revision <= 24; revision++) {
        const index = (revision * 17) % beforeReplacement.rows.length;
        const row = beforeReplacement.rows[index];
        if (row !== undefined) row.value = -revision;
        beforeReplacement.revision = revision;
        beforeReplacementAt = yield* commitWrites(storage, [
          {
            type: "document.change",
            id,
            content: {
              kind: "delta",
              version: 1,
              ops: [
                ["s", ["rows", index, "value"], -revision],
                ["s", ["revision"], revision],
              ],
            },
          },
        ]);
      }

      const replacement = { revision: 100, rows: rows(10_000, "new") };
      const replacementSnapshot = structuredClone(replacement);
      const replacementAt = yield* commitWrites(storage, [
        {
          type: "document.change",
          id,
          content: { kind: "delta", version: 1, ops: [["r", replacement]] },
        },
      ]);
      const firstReplacementRow = replacement.rows[0];
      if (firstReplacementRow !== undefined) firstReplacementRow.value = -999;

      const current = structuredClone(replacementSnapshot);
      for (let revision = 101; revision <= 124; revision++) {
        const index = (revision * 19) % current.rows.length;
        const row = current.rows[index];
        if (row !== undefined) row.value = -revision;
        current.revision = revision;
        yield* commitWrites(storage, [
          {
            type: "document.change",
            id,
            content: {
              kind: "delta",
              version: 1,
              ops: [
                ["s", ["rows", index, "value"], -revision],
                ["s", ["revision"], revision],
              ],
            },
          },
        ]);
      }
      yield* expectEqual((yield* some(storage.document(id, createdAt))).value, initial);
      yield* expectEqual(
        (yield* some(storage.document(id, beforeReplacementAt))).value,
        beforeReplacement,
      );
      yield* expectEqual(
        (yield* some(storage.document(id, replacementAt))).value,
        replacementSnapshot,
      );
      const read = yield* some(storage.document(id, "current"));
      yield* expectEqual(read.value, current);
      yield* mutateAt(read.value, ["rows", 0], set("value", -1_000));
      yield* expectEqual((yield* some(storage.document(id, "current"))).value, current);
    }),
  ),

  conformanceCase(
    "copies stored document bases independently and rejects ambiguous sources",
    (storage) =>
      Effect.gen(function* () {
        const rootId = yield* createRoot(storage);
        const childId = yield* mintConversationId(storage);
        const secondChildId = yield* mintConversationId(storage);
        yield* commitWrites(storage, [
          { type: "conversation", value: { id: childId } },
          { type: "conversation", value: { id: secondChildId } },
        ]);
        const sourceId = yield* mintDocumentId(storage);
        const sourceRecord = {
          id: sourceId,
          kind: "copy.source",
          scope: { kind: "conversation", conversationId: rootId },
          history: "rewindable",
          fork: "asOf",
        } satisfies DocumentCreate;
        const createdAt = yield* commitWrites(storage, [
          {
            type: "document.create",
            record: sourceRecord,
            content: { kind: "base", version: 2, value: { count: 1, rows: [{ value: "base" }] } },
          },
        ]);
        yield* commitWrites(storage, [
          {
            type: "document.change",
            id: sourceId,
            content: {
              kind: "delta",
              version: 2,
              ops: [
                ["s", ["count"], 2],
                ["p", ["rows"], 1, 0, [{ value: "current" }]],
              ],
            },
          },
        ]);
        const historicalCopyId = yield* mintDocumentId(storage);
        const currentCopyId = yield* mintDocumentId(storage);
        const retiredCopyId = yield* mintDocumentId(storage);
        const childRecord = (id: DocumentId, conversationId: ConversationId): DocumentCreate => ({
          id,
          kind: sourceRecord.kind,
          scope: { kind: "conversation", conversationId },
          history: "rewindable",
          fork: "asOf",
        });
        yield* commitWrites(storage, [
          {
            type: "document.copy",
            record: childRecord(historicalCopyId, childId),
            source: { id: sourceId, at: createdAt },
          },
          {
            type: "document.copy",
            record: childRecord(currentCopyId, secondChildId),
            source: { id: sourceId, at: "current" },
          },
          {
            type: "document.copy",
            record: childRecord(retiredCopyId, rootId),
            source: { id: sourceId, at: "current" },
          },
          { type: "document.retire", id: retiredCopyId },
        ]);
        yield* expectMatch(yield* some(storage.document(historicalCopyId, "current")), {
          version: 2,
          value: { count: 1, rows: [{ value: "base" }] },
        });
        yield* expectMatch(yield* some(storage.document(currentCopyId, "current")), {
          version: 2,
          value: { count: 2, rows: [{ value: "base" }, { value: "current" }] },
        });
        yield* expectNone(storage.document(retiredCopyId, "current"));

        yield* commitWrites(storage, [
          {
            type: "document.change",
            id: sourceId,
            content: { kind: "base", version: 2, value: { count: 99, rows: [] } },
          },
          { type: "document.retire", id: sourceId },
        ]);
        yield* expectEqual((yield* some(storage.document(currentCopyId, "current"))).value, {
          count: 2,
          rows: [{ value: "base" }, { value: "current" }],
        });

        const latestSourceId = yield* mintDocumentId(storage);
        const latestCopyId = yield* mintDocumentId(storage);
        const latestSource = {
          id: latestSourceId,
          kind: "copy.latest",
          scope: { kind: "conversation", conversationId: rootId },
          history: "latest",
          fork: "current",
        } satisfies DocumentCreate;
        yield* commitWrites(storage, [
          {
            type: "document.create",
            record: latestSource,
            content: { kind: "base", version: 4, value: { retained: "copy" } },
          },
        ]);
        yield* commitWrites(storage, [
          {
            type: "document.copy",
            record: {
              ...latestSource,
              id: latestCopyId,
              scope: { kind: "conversation", conversationId: childId },
            },
            source: { id: latestSourceId, at: "current" },
          },
        ]);
        yield* commitWrites(storage, [
          {
            type: "document.change",
            id: latestSourceId,
            content: { kind: "base", version: 4, value: { retained: "source-only" } },
          },
          { type: "document.retire", id: latestSourceId },
        ]);
        yield* expectMatch(yield* some(storage.document(latestCopyId, "current")), {
          version: 4,
          value: { retained: "copy" },
        });

        const conflictId = yield* mintDocumentId(storage);
        // Rejected without effect: the copy and the retirement in the same batch are both absent.
        yield* expectError(
          commitWrites(storage, [
            {
              type: "document.copy",
              record: childRecord(conflictId, childId),
              source: { id: currentCopyId, at: "current" },
            },
            { type: "document.retire", id: currentCopyId },
          ]),
          rejected,
          "",
        );
        yield* expectNone(storage.document(conflictId, "current"));
        yield* expectEqual((yield* some(storage.document(currentCopyId, "current"))).value, {
          count: 2,
          rows: [{ value: "base" }, { value: "current" }],
        });

        const mismatchId = yield* mintDocumentId(storage);
        yield* expectError(
          commitWrites(storage, [
            {
              type: "document.copy",
              record: { ...childRecord(mismatchId, childId), kind: "copy.mismatch" },
              source: { id: currentCopyId, at: "current" },
            },
          ]),
          rejected,
          "",
        );
        yield* expectNone(storage.document(mismatchId, "current"));
      }),
  ),

  conformanceCase(
    "uses bases for version transitions and rejects historical reads of current-only documents",
    (storage) =>
      Effect.gen(function* () {
        yield* createRoot(storage);
        const id = yield* mintDocumentId(storage);
        const record = {
          id,
          kind: "session.settings",
          scope: { kind: "session" },
        } satisfies DocumentCreate;
        yield* commitWrites(storage, [
          {
            type: "document.create",
            record,
            content: { kind: "base", version: 1, value: { count: 1 } },
          },
        ]);
        yield* commitWrites(storage, [
          {
            type: "document.change",
            id,
            content: { kind: "delta", version: 1, ops: [["s", ["count"], 2]] },
          },
        ]);
        const migratedAt = yield* commitWrites(storage, [
          {
            type: "document.change",
            id,
            content: { kind: "base", version: 2, value: { count: 3 } },
          },
        ]);
        yield* expectMatch(yield* some(storage.document(id, "current")), {
          version: 2,
          value: { count: 3 },
        });
        yield* expectError(
          storage.document(id, migratedAt),
          requestError,
          "does not retain historical content",
        );

        yield* expectError(
          commitWrites(storage, [
            {
              type: "document.change",
              id,
              content: { kind: "delta", version: 1, ops: [["s", ["count"], 4]] },
            },
          ]),
          rejected,
          "version transition requires a base",
        );
        yield* expectEqual((yield* some(storage.document(id, "current"))).value, { count: 3 });
        yield* commitWrites(storage, [{ type: "document.retire", id }]);
        yield* expectNone(storage.document(id, "current"));
      }),
  ),

  conformanceCase("indexes logical addresses and exact-scope scans independently", (storage) =>
    Effect.gen(function* () {
      const rootId = yield* createRoot(storage);
      const firstId = yield* mintDocumentId(storage);
      const secondId = yield* mintDocumentId(storage);
      const conversationId = yield* mintDocumentId(storage);
      const taskId = yield* mintTaskId(storage);
      const taskSingletonId = yield* mintDocumentId(storage);
      const taskFamilyId = yield* mintDocumentId(storage);
      const taskOtherKindId = yield* mintDocumentId(storage);
      const createdAt = yield* commitWrites(storage, [
        { type: "task", value: pendingTask(taskId, rootId) },
        {
          type: "document.create",
          record: { id: firstId, kind: "cache", scope: { kind: "session" }, key: "__proto__" },
          content: { kind: "base", version: 1, value: { owner: "first" } },
        },
        {
          type: "document.create",
          record: { id: secondId, kind: "cache", scope: { kind: "session" }, key: "constructor" },
          content: { kind: "base", version: 1, value: { owner: "second" } },
        },
        {
          type: "document.create",
          record: {
            id: conversationId,
            kind: "cache",
            scope: { kind: "conversation", conversationId: rootId },
            history: "latest",
            fork: "current",
            key: "__proto__",
          },
          content: { kind: "base", version: 1, value: { owner: "conversation" } },
        },
        {
          type: "document.create",
          record: { id: taskSingletonId, kind: "task.cache", scope: { kind: "task", taskId } },
          content: { kind: "base", version: 1, value: { owner: "singleton" } },
        },
        {
          type: "document.create",
          record: {
            id: taskFamilyId,
            kind: "task.cache",
            scope: { kind: "task", taskId },
            key: "member",
          },
          content: { kind: "base", version: 1, value: { owner: "family" } },
        },
        {
          type: "document.create",
          record: { id: taskOtherKindId, kind: "task.other", scope: { kind: "task", taskId } },
          content: { kind: "base", version: 1, value: { owner: "other" } },
        },
      ]);

      const session = { kind: "session" } as const;
      yield* expectEqual(
        (yield* some(
          storage.findDocument({ kind: "cache", scope: session, key: "__proto__" }, "current"),
        )).id,
        firstId,
      );
      yield* expectEqual(
        (yield* storage.scanDocuments({ scope: session, at: "current" }, 1)).items.length,
        1,
      );
      const first = yield* storage.scanDocuments({ scope: session, at: "current" }, 1);
      const second = yield* storage.scanDocuments(
        { scope: session, at: "current" },
        1,
        cursorOf(first),
      );
      yield* expectEqual([...ids(first), ...ids(second)], [firstId, secondId]);
      yield* expectEqual(
        ids(
          yield* storage.scanDocuments(
            { scope: { kind: "conversation", conversationId: rootId }, at: "current" },
            10,
          ),
        ),
        [conversationId],
      );
      yield* expectEqual(
        (yield* some(
          storage.findDocument({ kind: "task.cache", scope: { kind: "task", taskId } }, "current"),
        )).id,
        taskSingletonId,
      );
      yield* expectEqual(
        (yield* some(
          storage.findDocument(
            { kind: "task.cache", scope: { kind: "task", taskId }, key: "member" },
            "current",
          ),
        )).id,
        taskFamilyId,
      );
      yield* expectEqual(
        ids(
          yield* storage.scanDocuments(
            { scope: { kind: "task", taskId }, at: "current", kind: "task.cache" },
            10,
          ),
        ),
        [taskSingletonId, taskFamilyId],
      );
      yield* expectError(
        storage.document(taskSingletonId, createdAt),
        requestError,
        "does not retain historical content",
      );
    }),
  ),

  conformanceCase(
    "keeps document lifecycle failures atomic and gives create-plus-retire an empty lifetime",
    (storage) =>
      Effect.gen(function* () {
        const rootId = yield* createRoot(storage);
        const firstId = yield* mintDocumentId(storage);
        const secondId = yield* mintDocumentId(storage);
        const record = {
          id: firstId,
          kind: "singleton",
          scope: { kind: "session" },
        } satisfies DocumentCreate;
        yield* commitWrites(storage, [
          {
            type: "document.create",
            record,
            content: { kind: "base", version: 1, value: { value: 1 } },
          },
        ]);
        yield* expectError(
          commitWrites(storage, [
            {
              type: "document.create",
              record: { ...record, id: secondId },
              content: { kind: "base", version: 1, value: { value: 2 } },
            },
            {
              type: "document.change",
              id: firstId,
              content: { kind: "delta", version: 1, ops: [] },
            },
          ]),
          rejected,
          "already has a current incarnation",
        );
        yield* expectEqual((yield* some(storage.document(firstId, "current"))).value, { value: 1 });
        yield* expectNone(storage.document(secondId, "current"));

        const emptyId = yield* mintDocumentId(storage);
        const emptyAt = yield* commitWrites(storage, [
          {
            type: "document.create",
            record: {
              id: emptyId,
              kind: record.kind,
              key: "empty",
              scope: { kind: "conversation", conversationId: rootId },
              history: "rewindable",
              fork: "initial",
            },
            content: { kind: "base", version: 1, value: {} },
          },
          { type: "document.retire", id: emptyId },
        ]);
        yield* expectNone(storage.document(emptyId, "current"));
        yield* expectNone(storage.document(emptyId, emptyAt));
        yield* expectNone(
          storage.findDocument(
            {
              kind: record.kind,
              scope: { kind: "conversation", conversationId: rootId },
              key: "empty",
            },
            emptyAt,
          ),
        );
      }),
  ),

  conformanceCase(
    "refuses a creation and a change of one document in one commit, in either order",
    (storage) =>
      Effect.gen(function* () {
        yield* createRoot(storage);
        for (const order of ["create first", "change first"] as const) {
          const id = yield* mintDocumentId(storage);
          const create: StorageWrite = {
            type: "document.create",
            record: { id, kind: "twice", key: order, scope: { kind: "session" } },
            content: { kind: "base", version: 1, value: { x: 1 } },
          };
          const change: StorageWrite = {
            type: "document.change",
            id,
            content: { kind: "delta", version: 1, ops: [["s", ["x"], 2]] },
          };
          const writes = order === "create first" ? [create, change] : [change, create];
          yield* expectError(
            commitWrites(storage, writes),
            rejected,
            "more than one content command",
          );
          yield* expectNone(storage.document(id, "current"));
        }
      }),
  ),

  conformanceCase(
    "rolls back record tables and secondary indexes when a document command fails",
    (storage) =>
      Effect.gen(function* () {
        const rootId = yield* createRoot(storage);
        const taskId = yield* mintTaskId(storage);
        const submissionId = yield* mintSubmissionId(storage);
        const documentId = yield* mintDocumentId(storage);
        const task = pendingTask(taskId, rootId);
        const submission: SubmissionRecord = {
          id: submissionId,
          conversationId: rootId,
          requestId: "atomic",
          type: "input",
          status: "queued",
        };
        const record = {
          id: documentId,
          kind: "atomic",
          scope: { kind: "session" },
        } satisfies DocumentCreate;
        const baselineSeq = yield* commitWrites(storage, [
          { type: "task", value: task },
          { type: "submission", value: submission },
          {
            type: "document.create",
            record,
            content: { kind: "base", version: 1, value: { count: 1 } },
          },
        ]);

        const entryId = yield* mintEntryId(storage);
        const conflictingDocumentId = yield* mintDocumentId(storage);
        yield* expectError(
          commitWrites(storage, [
            {
              type: "task",
              value: { ...task, state: { status: "running", checkpoint: { phase: "effect" } } },
            },
            {
              type: "submission",
              value: { ...submission, status: "unanswered", reason: "failed" },
            },
            { type: "entry", value: entry(entryId, rootId, "transient") },
            {
              type: "document.create",
              record: { ...record, id: conflictingDocumentId },
              content: { kind: "base", version: 1, value: { count: 2 } },
            },
          ]),
          rejected,
          "already has a current incarnation",
        );

        yield* expectEqual(yield* some(storage.task(taskId)), task);
        yield* expectEqual((yield* storage.scanTasks({ status: "pending" }, 10)).items, [task]);
        yield* expectEqual(yield* some(storage.submissionByRequest(rootId, "atomic")), submission);
        yield* expectNone(storage.entry(entryId));
        yield* expectNone(storage.document(conflictingDocumentId, "current"));
        yield* expectEqual(
          (yield* some(storage.findDocument({ kind: record.kind, scope: record.scope }, "current")))
            .id,
          documentId,
        );
        const afterRollbackSeq = yield* commitWrites(storage, [
          {
            type: "document.change",
            id: documentId,
            content: { kind: "delta", version: 1, ops: [["s", ["count"], 3]] },
          },
        ]);
        yield* expectTrue(
          afterRollbackSeq > baselineSeq,
          `Expected ${afterRollbackSeq} to be greater than ${baselineSeq}`,
        );
      }),
  ),

  conformanceCase("keeps indexed string identities lossless", (storage) =>
    Effect.gen(function* () {
      const rootId = yield* createRoot(storage);
      const first = "\ud800";
      const second = "\ud801";
      const firstTaskId = yield* mintTaskId(storage);
      const secondTaskId = yield* mintTaskId(storage);
      const firstSubmissionId = yield* mintSubmissionId(storage);
      const secondSubmissionId = yield* mintSubmissionId(storage);
      const firstKindDocumentId = yield* mintDocumentId(storage);
      const secondKindDocumentId = yield* mintDocumentId(storage);
      const firstKeyDocumentId = yield* mintDocumentId(storage);
      const secondKeyDocumentId = yield* mintDocumentId(storage);
      const session = { kind: "session" } as const;
      yield* commitWrites(storage, [
        { type: "task", value: { ...pendingTask(firstTaskId, rootId), kind: first } },
        { type: "task", value: { ...pendingTask(secondTaskId, rootId), kind: second } },
        {
          type: "submission",
          value: {
            id: firstSubmissionId,
            conversationId: rootId,
            requestId: first,
            type: "input",
            status: "queued",
          },
        },
        {
          type: "submission",
          value: {
            id: secondSubmissionId,
            conversationId: rootId,
            requestId: second,
            type: "input",
            status: "queued",
          },
        },
        {
          type: "document.create",
          record: { id: firstKindDocumentId, kind: first, scope: session },
          content: { kind: "base", version: 1, value: { identity: "first kind" } },
        },
        {
          type: "document.create",
          record: { id: secondKindDocumentId, kind: second, scope: session },
          content: { kind: "base", version: 1, value: { identity: "second kind" } },
        },
        {
          type: "document.create",
          record: { id: firstKeyDocumentId, kind: "family", key: first, scope: session },
          content: { kind: "base", version: 1, value: { identity: "first key" } },
        },
        {
          type: "document.create",
          record: { id: secondKeyDocumentId, kind: "family", key: second, scope: session },
          content: { kind: "base", version: 1, value: { identity: "second key" } },
        },
      ]);

      yield* expectEqual(ids(yield* storage.scanTasks({ kind: first }, 10)), [firstTaskId]);
      yield* expectEqual(ids(yield* storage.scanTasks({ kind: second }, 10)), [secondTaskId]);
      yield* expectEqual((yield* some(storage.task(firstTaskId))).kind, first);
      yield* expectEqual((yield* some(storage.task(secondTaskId))).kind, second);
      yield* expectEqual(
        (yield* some(storage.submissionByRequest(rootId, first))).requestId,
        first,
      );
      yield* expectEqual(
        (yield* some(storage.submissionByRequest(rootId, first))).id,
        firstSubmissionId,
      );
      yield* expectEqual(
        (yield* some(storage.submissionByRequest(rootId, second))).id,
        secondSubmissionId,
      );
      yield* expectEqual(
        (yield* some(storage.findDocument({ kind: first, scope: session }, "current"))).id,
        firstKindDocumentId,
      );
      yield* expectEqual(
        (yield* some(storage.findDocument({ kind: second, scope: session }, "current"))).id,
        secondKindDocumentId,
      );
      yield* expectEqual(
        (yield* some(
          storage.findDocument({ kind: "family", key: first, scope: session }, "current"),
        )).id,
        firstKeyDocumentId,
      );
      yield* expectEqual(
        (yield* some(
          storage.findDocument({ kind: "family", key: second, scope: session }, "current"),
        )).id,
        secondKeyDocumentId,
      );
      yield* expectEqual(
        ids(yield* storage.scanDocuments({ scope: session, at: "current", kind: first }, 10)),
        [firstKindDocumentId],
      );
    }),
  ),

  conformanceCase(
    "keeps one global record ID namespace and rejects exhausted ID minting",
    (storage) =>
      Effect.gen(function* () {
        const rootId = yield* createRoot(storage);
        const explicitEntryId = EntryId.make(100);
        yield* commitWrites(storage, [{ type: "entry", value: entry(explicitEntryId, rootId) }]);
        yield* expectEqual(yield* storage.mintId, 101);
        yield* expectError(
          commitWrites(storage, [
            { type: "task", value: pendingTask(TaskId.make(explicitEntryId), rootId) },
          ]),
          rejected,
          `ID ${explicitEntryId} already belongs to entry`,
        );

        yield* commitWrites(storage, [
          {
            type: "entry",
            value: entry(EntryId.make(Number.MAX_SAFE_INTEGER), rootId, "last-id"),
          },
        ]);
        const exhausted = { tag: "StorageFailure", reason: "IdSpaceExhausted" } as const;
        yield* expectError(storage.mintId, exhausted, "ID space is exhausted");
        yield* expectError(storage.mintId, exhausted, "ID space is exhausted");
      }),
  ),

  conformanceCase(
    "commits batches in order, each with its seq, checked against the earlier ones",
    (storage) =>
      Effect.gen(function* () {
        const rootId = yield* createRoot(storage);
        const firstId = yield* mintEntryId(storage);
        const lastId = yield* mintEntryId(storage);
        const documentId = yield* mintDocumentId(storage);
        const receipts: Array<readonly [number, Seq]> = [];
        const context = yield* Effect.context<never>();
        let read: Fiber.Fiber<Option.Option<CommittedEntry>, CaseError> | undefined;
        const result = yield* storage.commit(
          [
            [
              { type: "entry", value: entry(firstId, rootId) },
              {
                type: "document.create",
                record: {
                  id: documentId,
                  kind: "batch.doc",
                  scope: { kind: "conversation", conversationId: rootId },
                  history: "latest",
                  fork: "initial",
                },
                content: { kind: "base", version: 1, value: { count: 1 } },
              },
            ],
            // Changes the document the batch before created.
            [
              {
                type: "document.change",
                id: documentId,
                content: { kind: "delta", version: 1, ops: [["s", ["count"], 2]] },
              },
            ],
            [{ type: "entry", value: entry(lastId, rootId) }],
          ],
          (index, seq) => {
            receipts.push([index, seq]);
            // Receipts follow the last durable batch: a read started at the first observes it.
            read ??= Effect.runForkWith(context)(storage.entry(lastId));
          },
        );
        yield* expectEqual(result, { stored: 3 });
        yield* expectEqual(
          receipts.map(([index]) => index),
          [0, 1, 2],
        );
        const [first, second, last] = receipts.map(([, seq]) => seq);
        if (
          first === undefined ||
          second === undefined ||
          last === undefined ||
          read === undefined
        ) {
          return yield* fail(`Expected three receipts, got ${show(receipts)}`);
        }
        yield* expectTrue(
          second > first && last > second,
          `Expected increasing seqs, got ${show(receipts)}`,
        );
        yield* expectEqual(Option.getOrUndefined(yield* Fiber.join(read))?.commitSeq, last);
        yield* expectEqual((yield* some(storage.entry(firstId))).commitSeq, first);
        yield* expectEqual((yield* some(storage.document(documentId, "current"))).value, {
          count: 2,
        });
        const next = yield* commitWrites(storage, [
          { type: "entry", value: entry(yield* mintEntryId(storage), rootId) },
        ]);
        yield* expectTrue(next > last, `Expected ${next} to be greater than ${last}`);
      }),
  ),

  conformanceCase("stores the batches before the first one refused, and none after it", (storage) =>
    Effect.gen(function* () {
      const rootId = yield* createRoot(storage);
      const firstId = yield* mintEntryId(storage);
      const afterId = yield* mintEntryId(storage);
      const receipts: Array<number> = [];
      const result = yield* storage.commit(
        [
          [{ type: "entry", value: entry(firstId, rootId) }],
          // Refused against the state the first batch left: the ID is taken now.
          [
            { type: "task", value: pendingTask(yield* mintTaskId(storage), rootId) },
            { type: "entry", value: entry(firstId, rootId) },
          ],
          [{ type: "entry", value: entry(afterId, rootId) }],
        ],
        (index) => receipts.push(index),
      );
      yield* expectEqual(result.stored, 1);
      yield* expectRefused(result, `ID ${firstId} already belongs to entry`);
      yield* expectEqual(receipts, [0]);
      yield* some(storage.entry(firstId));
      yield* expectNone(storage.entry(afterId));
      yield* expectEqual(ids(yield* storage.scanTasks({}, 10)), []);

      // A refused first batch stores nothing; the storage goes on.
      const refused = yield* storage.commit(
        [[{ type: "conversation", value: { id: rootId } }]],
        (index) => receipts.push(index),
      );
      yield* expectEqual(refused.stored, 0);
      yield* expectRefused(refused, `ID ${rootId} already belongs to conversation`);
      yield* expectEqual(receipts, [0]);
      yield* commitWrites(storage, [{ type: "entry", value: entry(afterId, rootId) }]);
      yield* some(storage.entry(afterId));
    }),
  ),

  conformanceCase(
    "refuses a later batch its own malformed actions fail, keeping the batches before it",
    (storage) =>
      Effect.gen(function* () {
        const rootId = yield* createRoot(storage);
        const documentId = yield* mintDocumentId(storage);
        yield* commitWrites(storage, [
          {
            type: "document.create",
            record: { id: documentId, kind: "staged", scope: { kind: "session" } },
            content: { kind: "base", version: 1, value: { n: 1 } },
          },
        ]);
        const firstId = yield* mintEntryId(storage);
        const secondId = yield* mintEntryId(storage);
        const afterId = yield* mintEntryId(storage);
        const receipts: Array<number> = [];
        const result = yield* storage.commit(
          [
            [{ type: "entry", value: entry(firstId, rootId) }],
            // Malformed in itself, whatever is stored: one document retired twice.
            [
              { type: "entry", value: entry(secondId, rootId) },
              { type: "document.retire", id: documentId },
              { type: "document.retire", id: documentId },
            ],
            [{ type: "entry", value: entry(afterId, rootId) }],
          ],
          (index) => receipts.push(index),
        );
        yield* expectEqual(result.stored, 1);
        yield* expectRefused(result, "retired more than once");
        yield* expectEqual(receipts, [0]);
        yield* some(storage.entry(firstId));
        yield* expectNone(storage.entry(secondId));
        yield* expectNone(storage.entry(afterId));
        yield* expectEqual((yield* some(storage.document(documentId, "current"))).value, { n: 1 });
      }),
  ),

  conformanceCase("resolves a batched copy against the state the earlier batches left", (storage) =>
    Effect.gen(function* () {
      const rootId = yield* createRoot(storage);
      const childId = yield* mintConversationId(storage);
      const sourceId = yield* mintDocumentId(storage);
      const copyId = yield* mintDocumentId(storage);
      const seqs: Array<Seq> = [];
      const result = yield* storage.commit(
        [
          [
            {
              type: "document.create",
              record: batchCopy(sourceId, rootId),
              content: { kind: "base", version: 1, value: { count: 1 } },
            },
          ],
          [
            {
              type: "document.change",
              id: sourceId,
              content: { kind: "delta", version: 1, ops: [["s", ["count"], 2]] },
            },
          ],
          [
            { type: "conversation", value: { id: childId } },
            {
              type: "document.copy",
              record: batchCopy(copyId, childId),
              source: { id: sourceId, at: "current" },
            },
          ],
        ],
        (_index, seq) => seqs.push(seq),
      );
      yield* expectEqual(result, { stored: 3 });
      yield* expectEqual((yield* some(storage.document(copyId, "current"))).value, { count: 2 });
      const createdAt = seqs[0];
      if (createdAt === undefined) return yield* fail("Expected a receipt for the first batch");
      yield* expectEqual((yield* some(storage.document(sourceId, createdAt))).value, {
        count: 1,
      });

      // A copy whose source the same batch changes is refused before the batch writes anything.
      const laterId = yield* mintConversationId(storage);
      const laterCopyId = yield* mintDocumentId(storage);
      const refused = yield* storage.commit(
        [
          [
            { type: "conversation", value: { id: laterId } },
            {
              type: "document.copy",
              record: batchCopy(laterCopyId, laterId),
              source: { id: sourceId, at: "current" },
            },
            {
              type: "document.change",
              id: sourceId,
              content: { kind: "delta", version: 1, ops: [["s", ["count"], 3]] },
            },
          ],
        ],
        () => {},
      );
      yield* expectEqual(refused.stored, 0);
      yield* expectRefused(refused, "is changed in the copy batch");
      yield* expectNone(storage.conversation(laterId));
      yield* expectEqual((yield* some(storage.document(sourceId, "current"))).value, {
        count: 2,
      });
    }),
  ),

  {
    name: "rejects every operation after close",
    run: Effect.gen(function* () {
      const conformanceStorage = yield* ConformanceStorage;
      const scope = yield* Scope.make();
      const storage = yield* conformanceStorage.open.pipe(
        Effect.provideService(Scope.Scope, scope),
      );
      yield* createRoot(storage);
      // Closing the storage's scope closes it: `AgentStorage` has no `close()`.
      yield* Scope.close(scope, Exit.void);
      const closed = { tag: "StorageFailure", reason: "Closed" } as const;
      yield* expectError(storage.conversation(ROOT_CONVERSATION_ID), closed, "closed");
      yield* expectError(
        commitWrites(storage, [{ type: "conversation", value: { id: ROOT_CONVERSATION_ID } }]),
        closed,
        "closed",
      );
      yield* expectError(storage.mintId, closed, "closed");
    }),
  },
];

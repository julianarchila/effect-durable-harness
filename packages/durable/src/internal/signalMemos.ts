// Signal keys stored in a task's memos, and the decisions on them: a delivery, a read, whether a sleep may
// park on its keys, and how a reservation wakes it. Pure and synchronous: they run inside commits and the
// scheduler's decisions.
import * as Option from "effect/Option";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import type {
  SettledSubmissionRecord,
  SubmissionRecord,
  TaskRecord,
  TaskState,
  WakeReason,
} from "../Records.ts";
import { InvalidSignal, type Signal, type SignalResult, type SignalState } from "../Signal.ts";
import { copyJson } from "./documents.ts";

type Json = Schema.Json;
type Memos = Readonly<Record<string, Json>>;
export type Sleeping = Extract<TaskState, { readonly status: "sleeping" }>;

/** Stored state of one key: a value with the version it was delivered at, or expired. */
type SignalMemo =
  | { readonly version: number; readonly value: Json; readonly consumed?: true }
  | { readonly expired: true };

/** The memo holding a key's state. Signal names contain no `/`, so names never collide. */
export const signalMemoName = (signal: string, key: string): string =>
  `core.signal.${signal}/${key}`;

/** Own memo entries only; a name such as `toString` must not resolve to an inherited property. */
const memoOf = (memos: Memos | undefined, name: string): SignalMemo | undefined => {
  if (memos === undefined || !Object.hasOwn(memos, name)) return undefined;
  // SAFETY: names under `core.signal.` hold signal state, which this module writes as a `SignalMemo`; a task
  // that writes one through `runtime.memo` corrupts its own signals.
  return memos[name] as SignalMemo;
};

const isLive = (record: TaskRecord): boolean =>
  record.state.status !== "terminal" && record.state.status !== "completing";

/** What a delivery decided: its result, and the record to write when it changes the task. */
export interface Delivery {
  readonly result: SignalResult;
  readonly record?: TaskRecord | undefined;
}

/**
 * Decides a delivery to `record`: the result, and the record to write when it changes, with the value stored,
 * or the key marked expired when the task sleeps on it past its deadline.
 */
export const deliver = <T>(
  record: TaskRecord | undefined,
  signal: Signal<T>,
  key: string,
  value: T,
  now: number,
): Delivery => {
  if (record === undefined) return { result: "not_found" };
  if (!isLive(record) || record.abortRequested) return { result: "closed" };
  // Values come from outside: the trust boundary of the signal's schema.
  if (key === "") return { result: "invalid" };
  const encoded = Schema.encodeUnknownResult(signal.schema)(value);
  if (Result.isFailure(encoded)) return { result: "invalid" };
  const json = copyJson(encoded.success);
  if (Result.isFailure(json)) return { result: "invalid" };
  const name = signalMemoName(signal.name, key);
  const stored = memoOf(memosOf(record), name);
  if (stored !== undefined) return { result: "expired" in stored ? "closed" : "duplicate" };
  const state = record.state;
  if (
    state.status === "sleeping" &&
    state.until !== undefined &&
    state.until <= now &&
    state.signals?.some((listed) => listed.signal === signal.name && listed.key === key) === true
  ) {
    return { result: "closed", record: withMemos(record, { [name]: { expired: true } }) };
  }
  return {
    result: "delivered",
    record: withMemos(record, { [name]: { version: signal.version, value: json.success } }),
  };
};

/**
 * A key's state in `memos`, the value migrated to the token's version and decoded; none while nothing came.
 * Fails for a stored version newer than the token's, a missing or throwing `migrate`, or a value the schema rejects.
 */
export const read = <T>(
  memos: Memos | undefined,
  signal: Signal<T>,
  key: string,
): Result.Result<Option.Option<SignalState<T>>, InvalidSignal> => {
  const stored = memoOf(memos, signalMemoName(signal.name, key));
  if (stored === undefined) return Result.succeed(Option.none());
  if ("expired" in stored) return Result.succeed(Option.some({ expired: true }));
  const invalid = (message: string) =>
    Result.fail(new InvalidSignal({ signal: signal.name, key, message }));
  if (stored.version > signal.version) {
    return invalid(
      `Signal ${signal.name} value has version ${stored.version}, newer than ${signal.version}`,
    );
  }
  if (stored.version === signal.version) {
    const decoded = Schema.decodeUnknownResult(signal.schema)(stored.value);
    return Result.isSuccess(decoded)
      ? Result.succeed(Option.some({ value: decoded.success }))
      : invalid(`Signal ${signal.name} value does not match its schema`);
  }
  if (signal.migrate === undefined) {
    return invalid(`Signal ${signal.name} has no migration from version ${stored.version}`);
  }
  const migrate = signal.migrate;
  const fromVersion = stored.version;
  const value = stored.value;
  // A throw in `migrate` makes the stored value unreadable, as a migration that returns an invalid value does.
  const migrated = Result.try(() => migrate(value, fromVersion));
  if (Result.isFailure(migrated)) {
    return invalid(`Signal ${signal.name} migration from version ${fromVersion} failed`);
  }
  // The migration's result is checked as a delivery is: it must encode to JSON.
  const encoded = Schema.encodeUnknownResult(signal.schema)(migrated.success);
  if (Result.isFailure(encoded) || Result.isFailure(copyJson(encoded.success))) {
    return invalid(
      `Signal ${signal.name} migration from version ${stored.version} returned an invalid value`,
    );
  }
  return Result.succeed(Option.some({ value: migrated.success }));
};

/** Why a sleep is refused: it waits for nothing, or one of its keys already served a wait or expired. */
export const invalidSleep = (
  state: Sleeping,
  memos: Memos | undefined,
  task: number,
): string | undefined => {
  if (state.until === undefined && (state.signals === undefined || state.signals.length === 0)) {
    return `Task ${task} cannot sleep without until or signals`;
  }
  for (const listed of state.signals ?? []) {
    const stored = memoOf(memos, signalMemoName(listed.signal, listed.key));
    if (stored === undefined) continue;
    if ("expired" in stored) {
      return `Task ${task} cannot sleep on signal ${listed.signal} key ${listed.key}: it expired`;
    }
    if (stored.consumed === true) {
      return `Task ${task} cannot sleep on signal ${listed.signal} key ${listed.key}: it already woke a wait`;
    }
  }
  return undefined;
};

/** Whether a sleep can wake now: a listed key holds a value no wake used, or its deadline came. */
export const sleepDue = (state: Sleeping, memos: Memos | undefined, now: number): boolean =>
  readyKey(state, memos) !== undefined || (state.until !== undefined && state.until <= now);

/**
 * The wake of a sleep that can wake now, and the memos the waking reservation writes: the first listed key
 * holding a value no wake used, marked consumed; otherwise the deadline, with every listed key that holds
 * nothing marked expired. None when the sleep cannot wake yet.
 */
export const sleepWake = (
  state: Sleeping,
  memos: Memos | undefined,
  now: number,
): { readonly wake: WakeReason; readonly memos: Memos } | undefined => {
  const ready = readyKey(state, memos);
  if (ready !== undefined) {
    return {
      wake: { reason: "signal", signal: ready.signal, key: ready.key },
      memos: { [ready.name]: { ...ready.stored, consumed: true } },
    };
  }
  if (state.until === undefined || state.until > now) return undefined;
  if (state.signals === undefined || state.signals.length === 0) {
    return { wake: { reason: "time" }, memos: {} };
  }
  const expired: Record<string, Json> = {};
  for (const listed of state.signals) {
    const name = signalMemoName(listed.signal, listed.key);
    if (memoOf(memos, name) === undefined) expired[name] = { expired: true };
  }
  return { wake: { reason: "expired" }, memos: expired };
};

/** A listed key holding a value no wake used, with its memo's name and stored value. */
interface ReadyKey {
  readonly signal: string;
  readonly key: string;
  readonly name: string;
  readonly stored: Extract<SignalMemo, { readonly version: number }>;
}

/** The first listed key, in `signals` order, holding a value no wake used. */
const readyKey = (state: Sleeping, memos: Memos | undefined): ReadyKey | undefined => {
  for (const listed of state.signals ?? []) {
    const name = signalMemoName(listed.signal, listed.key);
    const stored = memoOf(memos, name);
    if (stored !== undefined && !("expired" in stored) && stored.consumed !== true) {
      return { signal: listed.signal, key: listed.key, name, stored };
    }
  }
  return undefined;
};

/** A record's memos; settled records carry none. */
export const memosOf = (record: TaskRecord | undefined): Memos | undefined =>
  record !== undefined && "memos" in record ? record.memos : undefined;

/** `record` with `memos` added; signal memos are written only to live records. */
export const withMemos = (record: TaskRecord, memos: Memos): TaskRecord =>
  // SAFETY: callers pass live records, which carry memos.
  ({ ...record, memos: { ...memosOf(record), ...memos } }) as TaskRecord;

/** `record` under `memos`: its own memos win over them. Signal memos are written only to live records. */
export const underMemos = (record: TaskRecord, memos: Memos): TaskRecord =>
  // SAFETY: callers pass live records, which carry memos.
  ({ ...record, memos: { ...memos, ...memosOf(record) } }) as TaskRecord;

/** Whether a submission settled: answered (`done`) or not (`unanswered`). */
export const isSettled = (record: SubmissionRecord): record is SettledSubmissionRecord =>
  record.status === "done" || record.status === "unanswered";

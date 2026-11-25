// What a tool call and a task invocation share: an idempotency key, a step recorded on the task's `memo`, a
// conversation lookup that fails when absent, and the transitions a phase commits.
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";
import type { ConversationId, TaskId } from "../Ids.ts";
import type { JoinPolicy, SignalKeyRecord } from "../Records.ts";
import type { CommitError } from "../Session.ts";
import { ConversationNotFound } from "../SessionError.ts";
import type * as Task from "../Task.ts";
import type { TaskError } from "../TaskError.ts";
import type { Tx } from "../Tx.ts";

type Json = Schema.Json;

/** `key(name)`: stable for every run of one task, so an external system can deduplicate on it. */
export const keyOf = (taskId: TaskId, name: string): string => `${taskId}:${name}`;

/**
 * `step(name, schema, effect)` for one call or invocation: the result an earlier run of this step recorded, or
 * `effect`'s, recorded first-writer-wins with the task's `memo` under `step:<name>` (a handler should not name its own
 * memos `step:…`). Steps of the same name run one at a time, so two concurrent steps of one name run `effect`
 * once. A crash after `effect` and before the record runs it again.
 */
export const makeStep = (memo: Task.TaskMemo) => {
  const locks = new Map<string, Semaphore.Semaphore>();
  const lockOf = (name: string) => {
    const existing = locks.get(name);
    if (existing !== undefined) return existing;
    const created = Semaphore.makeUnsafe(1);
    locks.set(name, created);
    return created;
  };
  return <A, I extends Json, E, R>(
    name: string,
    schema: Schema.Codec<A, I>,
    effect: Effect.Effect<A, E, R>,
  ): Effect.Effect<A, E | TaskError | CommitError | Schema.SchemaError, R> =>
    lockOf(name).withPermits(1)(
      Effect.gen(function* () {
        const memoName = `step:${name}`;
        const decode = Schema.decodeEffect(schema);
        const recorded = yield* memo<I>(memoName);
        if (Option.isSome(recorded)) return yield* decode(recorded.value);
        const value = yield* effect;
        const encoded = yield* Schema.encodeEffect(schema)(value);
        // Another run of the same task may have recorded first: its value wins, as `memo`'s does.
        return yield* decode(yield* memo(memoName, encoded));
      }),
    );
};

/** `requireConversation(id)`: `conversation(id)`, failing with `ConversationNotFound` when it is absent. */
export const requireConversation = <H, E>(
  conversation: (id: ConversationId) => Effect.Effect<Option.Option<H>, E>,
  id: ConversationId,
): Effect.Effect<H, E | ConversationNotFound> =>
  Effect.flatMap(conversation(id), (found) =>
    Option.isSome(found)
      ? Effect.succeed(found.value)
      : Effect.fail(new ConversationNotFound({ conversationId: id })),
  );

// -----------------------------------------------------------------------------
// transitions
// -----------------------------------------------------------------------------

/** The `running` state at `checkpoint`. */
export const next = <S>(checkpoint: S) => ({ status: "running", checkpoint }) as const;

/** The `waiting` state: parked on `on`, resuming at `checkpoint`. */
export const waitFor = <S>(on: ReadonlyArray<TaskId>, checkpoint: S, policy: JoinPolicy) =>
  ({ status: "waiting", checkpoint, on, policy }) as const;

/** The `sleeping` state: parked until the Harness clock reaches `until`, resuming at `checkpoint`. */
export const sleepUntil = <S>(until: number, checkpoint: S) =>
  ({ status: "sleeping", checkpoint, until }) as const;

/** A `sleeping` state under construction: `until` and `signals` are set only when present. */
interface SleepingState<S> {
  readonly status: "sleeping";
  readonly checkpoint: S;
  until?: number;
  signals?: ReadonlyArray<SignalKeyRecord>;
}

/** The `sleeping` state: parked until `until`, or a value under one of `signals`, resuming at `checkpoint`. */
export const sleep = <S>(options: Task.SleepOptions<S>) => {
  // Only the key itself is stored, not the phantom value type.
  const signals = options.signals?.map((listed) => ({ signal: listed.signal, key: listed.key }));
  const until = options.until;
  const checkpoint = options.checkpoint;
  const state: SleepingState<S> = { status: "sleeping", checkpoint };
  if (until !== undefined) state.until = until;
  if (signals !== undefined) state.signals = signals;
  return state;
};

/** The terminal `completed` outcome. */
export const complete = <R>(result: R) =>
  ({ status: "terminal", outcome: { status: "completed", result } }) as const;

/** The terminal `failed` outcome, with the error message a thrown error would record. */
export const fail = (message: string) =>
  ({ status: "terminal", outcome: { status: "failed", error: { message } } }) as const;

/** The terminal `aborted` outcome, without a reason. */
export const aborted = { status: "terminal", outcome: { status: "aborted" } } as const;

type ErasedState = Task.NextTaskState<Json, Json>;
type Change<E> = Effect.Effect<unknown, E, Tx> | undefined;

/** The transitions of an erased runtime, each one call of its `commit`. */
export const transitions = (
  commit: <E>(
    change: Effect.Effect<ErasedState, E, Tx>,
  ) => Effect.Effect<void, E | TaskError | CommitError>,
) => {
  // `change` runs first and its writes commit with the state.
  const to = <E>(state: ErasedState, change: Change<E>) =>
    commit(change === undefined ? Effect.succeed(state) : Effect.as(change, state));
  return {
    next: <E = never>(checkpoint: Json, change?: Change<E>) => to(next(checkpoint), change),
    waitFor: <E = never>(
      on: ReadonlyArray<TaskId>,
      options: Task.WaitOptions<Json>,
      change?: Change<E>,
    ) => to(waitFor(on, options.checkpoint, options.policy), change),
    sleepUntil: <E = never>(until: number, checkpoint: Json, change?: Change<E>) =>
      to(sleepUntil(until, checkpoint), change),
    sleep: <E = never>(options: Task.SleepOptions<Json>, change?: Change<E>) =>
      to(sleep(options), change),
    complete: <E = never>(result: Json, change?: Change<E>) => to(complete(result), change),
    fail: <E = never>(message: string, change?: Change<E>) => to(fail(message), change),
    aborted: to<never>(aborted, undefined),
  };
};

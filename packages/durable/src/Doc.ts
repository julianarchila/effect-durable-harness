/**
 * Typed document definitions. A definition
 * is a token: it names a document kind, its scope, its history and fork
 * policy (conversation documents only), its version and how to build,
 * migrate and checkpoint its value. Definitions are not registered: code that
 * holds a token reads and writes the documents of its kind.
 *
 * @since 0.0.0
 */
import type * as Delta from "@effect-durable-harness/delta/Delta";
import type * as Tracker from "@effect-durable-harness/delta/Tracker";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Predicate from "effect/Predicate";
import type * as Schema from "effect/Schema";
import type * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import type { ConversationId, TaskId } from "./Ids.ts";
import { type ReadError, Session } from "./Session.ts";
import { DocumentAccessError, NotFound, type SessionFailed } from "./SessionError.ts";
import { Tx, type TxDocError } from "./Tx.ts";

// -----------------------------------------------------------------------------
// definitions
// -----------------------------------------------------------------------------

/**
 * What a checkpoint predicate knows about the stored history.
 *
 * @category models
 * @since 0.0.0
 */
export interface CheckpointInfo {
  /** Deltas already stored after the newest base, excluding the change being decided. */
  readonly deltasSinceBase: number;
}

/**
 * Fields every definition has.
 *
 * @category models
 * @since 0.0.0
 */
export interface CommonDefinition<T> {
  /** Stable kind, persisted in the document record. */
  readonly kind: string;
  /** Positive integer version, persisted with every base and delta. */
  readonly version: number;
  // The schema infers `T` and checks values that come from outside the tracker.
  // It must be a JSON codec whose type is its encoding, because drafts edit the
  // stored JSON directly.
  /** JSON schema of the value; checked when a value enters from storage, a fork copy or `migrate`. */
  readonly schema: Schema.Codec<T, T>;
  /** Converts a value stored by any older version; pure. */
  readonly migrate?: ((value: Schema.JsonObject, fromVersion: number) => NoInfer<T>) | undefined;
  /**
   * Selects a complete base instead of a delta for an ordinary change; called once per change. Required,
   * with no default: `() => false` keeps ordinary changes as deltas.
   */
  readonly checkpointWhen: (
    value: Readonly<NoInfer<T>>,
    ops: Delta.Delta,
    info: CheckpointInfo,
  ) => boolean;
}

/**
 * A singleton definition: one document per owner.
 *
 * @category models
 * @since 0.0.0
 */
export interface SingletonDefinition<T> extends CommonDefinition<T> {
  readonly family?: undefined;
  /** The value of a document created by `tx.doc`. */
  readonly initial: () => NoInfer<T>;
}

/**
 * A family definition: one document per owner and string key, created from a
 * seed.
 *
 * @category models
 * @since 0.0.0
 */
export interface FamilyDefinition<T, I> extends CommonDefinition<T> {
  readonly family: true;
  /** The value of a member created by `tx.doc`, from the first acquisition's seed. */
  readonly initial: (seed: I) => NoInfer<T>;
}

/**
 * A Session-scoped document: shared by every conversation, current-only.
 *
 * @category models
 * @since 0.0.0
 */
export interface SessionSemantics {
  readonly scope: "session";
}

/**
 * A conversation document that keeps only its current value.
 *
 * @category models
 * @since 0.0.0
 */
export interface LatestConversationSemantics {
  readonly scope: "conversation";
  readonly history: "latest";
  readonly fork: "current" | "initial";
}

/**
 * A conversation document that keeps its history for as-of reads.
 *
 * @category models
 * @since 0.0.0
 */
export interface RewindableConversationSemantics {
  readonly scope: "conversation";
  readonly history: "rewindable";
  readonly fork: "asOf" | "current" | "initial";
}

/**
 * A task document: current-only, never copied by a fork, retired with its task.
 *
 * @category models
 * @since 0.0.0
 */
export interface TaskSemantics {
  readonly scope: "task";
}

/**
 * The semantics of a conversation document.
 *
 * @category models
 * @since 0.0.0
 */
export type ConversationSemantics = LatestConversationSemantics | RewindableConversationSemantics;

declare const DocType: unique symbol;

/**
 * The address of a document of definition `D`: its owner (none for a Session document), then its key for a
 * family member, as one argument list.
 *
 * @category models
 * @since 0.0.0
 */
export type Address<D> = D extends { readonly scope: infer Owned }
  ? [...Owner<Owned>, ...(D extends { readonly family: true } ? [key: string] : [])]
  : never;

type Owner<Owned> = Owned extends "conversation"
  ? [conversationId: ConversationId]
  : Owned extends "task"
    ? [taskId: TaskId]
    : [];

/**
 * What `get` takes for a document of definition `D`: its address, then the seed of a family member, which
 * creates it when absent.
 *
 * @category models
 * @since 0.0.0
 */
export type Acquisition<D> = [
  ...Address<D>,
  ...(D extends { readonly family: true; readonly initial: (seed: infer I) => infer _Value }
    ? [seed: I]
    : []),
];

/**
 * A document token: the definition, typed by its value `T`, and the operations on the documents of its
 * kind.
 *
 * @category models
 * @since 0.0.0
 */
export interface Doc<T, D> {
  readonly definition: D;
  /**
   * Inside a commit: the draft of the document at `address`, created from `initial` when absent; the
   * commit writes what the draft holds when it ends (`tx.doc`).
   */
  readonly get: (...acquisition: Acquisition<D>) => Effect.Effect<Tracker.Draft<T>, TxDocError, Tx>;
  /** Inside a commit: retires the document at `address`, if it exists, without creating it (`tx.retireDoc`). */
  readonly retire: (...address: Address<D>) => Effect.Effect<void, TxDocError, Tx>;
  /** The committed value at `address`, read now; never creates (`session.snapshot`). */
  readonly read: (
    ...address: Address<D>
  ) => Effect.Effect<Option.Option<Readonly<T>>, ReadError, Session>;
  /**
   * The committed value at `address`, failing with a `DocumentAccessError` of reason `NotFound` when it is
   * absent: for a document that always exists, such as one written at creation.
   */
  readonly require: (...address: Address<D>) => Effect.Effect<Readonly<T>, ReadError, Session>;
  /**
   * The committed value at `address`, then its newest value after each change (a watch's folded frames,
   * `session.watchDoc`): `None` once it is retired, after which the stream ends, or at once when it does
   * not exist. A recreation is not followed here; `watchDoc` follows it.
   */
  readonly changes: (
    ...address: Address<D>
  ) => Stream.Stream<Option.Option<Readonly<T>>, ReadError, Session>;
  /** Phantom: pins `T`. */
  readonly [DocType]?: (value: T) => T;
}

/**
 * Token of a Session singleton.
 *
 * @category models
 * @since 0.0.0
 */
export type SessionDoc<T> = Doc<T, SingletonDefinition<T> & SessionSemantics>;

/**
 * Token of a conversation singleton, latest or rewindable.
 *
 * @category models
 * @since 0.0.0
 */
export type ConversationDoc<T> = Doc<T, SingletonDefinition<T> & ConversationSemantics>;

/**
 * Token of a rewindable conversation singleton.
 *
 * @category models
 * @since 0.0.0
 */
export type RewindableConversationDoc<T> = Doc<
  T,
  SingletonDefinition<T> & RewindableConversationSemantics
>;

/**
 * Token of a task singleton.
 *
 * @category models
 * @since 0.0.0
 */
export type TaskDoc<T> = Doc<T, SingletonDefinition<T> & TaskSemantics>;

/**
 * Token of a Session family.
 *
 * @category models
 * @since 0.0.0
 */
export type SessionDocFamily<T, I> = Doc<T, FamilyDefinition<T, I> & SessionSemantics>;

/**
 * Token of a conversation family, latest or rewindable.
 *
 * @category models
 * @since 0.0.0
 */
export type ConversationDocFamily<T, I> = Doc<T, FamilyDefinition<T, I> & ConversationSemantics>;

/**
 * Token of a rewindable conversation family.
 *
 * @category models
 * @since 0.0.0
 */
export type RewindableConversationDocFamily<T, I> = Doc<
  T,
  FamilyDefinition<T, I> & RewindableConversationSemantics
>;

/**
 * Token of a task family.
 *
 * @category models
 * @since 0.0.0
 */
export type TaskDocFamily<T, I> = Doc<T, FamilyDefinition<T, I> & TaskSemantics>;

/**
 * Defines a singleton document: `Doc.make(kind, options)`, or the earlier
 * object form with `kind` inside. `T` is inferred from `schema`.
 *
 * **Gotchas**
 *
 * Throws a `TypeError` when `version` is not a positive integer: a definition is a constant, so this fails when the module
 * loads.
 *
 * @category constructors
 * @since 0.0.0
 */
export function make<T extends Schema.JsonObject>(
  kind: string,
  options: WithoutKind<SingletonDefinition<T>> & SessionSemantics,
): SessionDoc<T>;
export function make<T extends Schema.JsonObject>(
  kind: string,
  options: WithoutKind<SingletonDefinition<T>> & LatestConversationSemantics,
): ConversationDoc<T>;
export function make<T extends Schema.JsonObject>(
  kind: string,
  options: WithoutKind<SingletonDefinition<T>> & RewindableConversationSemantics,
): RewindableConversationDoc<T>;
export function make<T extends Schema.JsonObject>(
  kind: string,
  options: WithoutKind<SingletonDefinition<T>> & TaskSemantics,
): TaskDoc<T>;
export function make<T extends Schema.JsonObject>(
  definition: SingletonDefinition<T> & SessionSemantics,
): SessionDoc<T>;
export function make<T extends Schema.JsonObject>(
  definition: SingletonDefinition<T> & LatestConversationSemantics,
): ConversationDoc<T>;
export function make<T extends Schema.JsonObject>(
  definition: SingletonDefinition<T> & RewindableConversationSemantics,
): RewindableConversationDoc<T>;
export function make<T extends Schema.JsonObject>(
  definition: SingletonDefinition<T> & TaskSemantics,
): TaskDoc<T>;
export function make(
  first: string | ErasedDefinition,
  options?: WithoutKind<ErasedDefinition>,
): Doc<Schema.JsonObject, ErasedDefinition> {
  return token(definitionOf(first, options));
}

/**
 * Defines a document family: `Doc.family(kind, options)`, or the earlier
 * object form with `kind` and `family: true` inside. Members are
 * addressed by a string key and created from the first acquisition's seed.
 *
 * **Gotchas**
 *
 * Throws a `TypeError` when `version` is not a positive integer.
 *
 * @category constructors
 * @since 0.0.0
 */
export function family<T extends Schema.JsonObject, I extends Schema.Json>(
  kind: string,
  options: MemberOptions<T, I> & SessionSemantics,
): SessionDocFamily<T, I>;
export function family<T extends Schema.JsonObject, I extends Schema.Json>(
  kind: string,
  options: MemberOptions<T, I> & LatestConversationSemantics,
): ConversationDocFamily<T, I>;
export function family<T extends Schema.JsonObject, I extends Schema.Json>(
  kind: string,
  options: MemberOptions<T, I> & RewindableConversationSemantics,
): RewindableConversationDocFamily<T, I>;
export function family<T extends Schema.JsonObject, I extends Schema.Json>(
  kind: string,
  options: MemberOptions<T, I> & TaskSemantics,
): TaskDocFamily<T, I>;
export function family<T extends Schema.JsonObject, I extends Schema.Json>(
  definition: FamilyDefinition<T, I> & SessionSemantics,
): SessionDocFamily<T, I>;
export function family<T extends Schema.JsonObject, I extends Schema.Json>(
  definition: FamilyDefinition<T, I> & LatestConversationSemantics,
): ConversationDocFamily<T, I>;
export function family<T extends Schema.JsonObject, I extends Schema.Json>(
  definition: FamilyDefinition<T, I> & RewindableConversationSemantics,
): RewindableConversationDocFamily<T, I>;
export function family<T extends Schema.JsonObject, I extends Schema.Json>(
  definition: FamilyDefinition<T, I> & TaskSemantics,
): TaskDocFamily<T, I>;
export function family(
  first: string | ErasedDefinition,
  options?: WithoutKind<ErasedDefinition>,
): Doc<Schema.JsonObject, ErasedDefinition> {
  // `family(kind, options)` says it is a family; the object form says it with `family: true`.
  return token(
    definitionOf(first, options === undefined ? undefined : { ...options, family: true }),
  );
}

/** A definition's fields but its kind, which `make(kind, options)` takes first. */
type WithoutKind<D> = Omit<D, "kind">;

/**
 * What `Doc.family(kind, options)` takes besides the semantics: a family definition without its kind and
 * its `family` flag.
 *
 * @category models
 * @since 0.0.0
 */
export type MemberOptions<T, I> = Omit<FamilyDefinition<T, I>, "kind" | "family">;

/** Any definition, as the constructors' implementations see it. */
type ErasedDefinition = CommonDefinition<Schema.JsonObject> & {
  readonly scope: "session" | "conversation" | "task";
  readonly family?: true | undefined;
};

/** The definition `make(kind, options)` or `make(definition)` describes. */
const definitionOf = (
  first: string | ErasedDefinition,
  options: WithoutKind<ErasedDefinition> | undefined,
): ErasedDefinition => {
  if (!Predicate.isString(first)) return first;
  // A definition is a module constant: failing here is broken setup.
  if (options === undefined) throw new TypeError(`Document ${first} has no options`);
  return { ...options, kind: first };
};

/** Erased: every operation forwards its address unchanged to the overloaded `Tx` or `Session` method. */
type ErasedOperation<A, E, R> = (
  doc: Doc<Schema.JsonObject, ErasedDefinition>,
  ...address: ReadonlyArray<string | number | Schema.Json>
) => Effect.Effect<A, E, R>;

/** The token of `definition`, with its operations. */
const token = (definition: ErasedDefinition): Doc<Schema.JsonObject, ErasedDefinition> => {
  validateVersion(definition);
  const read = (...address: ReadonlyArray<string | number>) =>
    Effect.gen(function* () {
      const session = yield* Session;
      // SAFETY: the overloads of `snapshot` are this call with the address of this token's scope.
      return yield* (
        session.snapshot as ErasedOperation<Option.Option<Schema.JsonObject>, ReadError, never>
      )(doc, ...address);
    });
  const doc: Doc<Schema.JsonObject, ErasedDefinition> = {
    definition,
    get: (...acquisition) =>
      Effect.gen(function* () {
        const tx = yield* Tx;
        // SAFETY: the overloads of `tx.doc` are this call with the address and seed of this token's scope.
        return yield* (
          tx.doc as ErasedOperation<Tracker.Draft<Schema.JsonObject>, TxDocError, never>
        )(doc, ...acquisition);
      }),
    retire: (...address) =>
      Effect.gen(function* () {
        const tx = yield* Tx;
        // SAFETY: the overloads of `tx.retireDoc` are this call with the address of this token's scope.
        return yield* (tx.retireDoc as ErasedOperation<void, TxDocError, never>)(doc, ...address);
      }),
    read,
    require: (...address) =>
      Effect.flatMap(read(...address), (value) =>
        Option.isSome(value)
          ? Effect.succeed(value.value)
          : Effect.fail(
              new DocumentAccessError({
                reason: new NotFound({ message: `Document ${definition.kind} does not exist` }),
              }),
            ),
      ),
    changes: (...address) =>
      Stream.unwrap(
        Effect.gen(function* () {
          const session = yield* Session;
          // SAFETY: the overloads of `watchDoc` are this call with the address of this token's scope.
          const watch = yield* (
            session.watchDoc as ErasedOperation<
              Option.Option<DocumentWatch<Schema.JsonObject>>,
              ReadError,
              Scope.Scope
            >
          )(doc, ...address);
          if (Option.isNone(watch)) return Stream.make(Option.none<Schema.JsonObject>());
          // A watch whose folded frames give the newest value.
          // A retirement ends it: a recreation at the address is followed by `watchDoc`, not here.
          return Stream.make(Option.some(watch.value.value)).pipe(
            Stream.concat(
              watch.value.frames.pipe(
                Stream.takeUntil((frame) => frame.retired),
                Stream.map((frame) =>
                  frame.retired ? Option.none<Schema.JsonObject>() : Option.some(frame.value),
                ),
              ),
            ),
          );
        }),
      ),
  };
  return doc;
};

// Versions are positive integers.
const validateVersion = (definition: { readonly kind: string; readonly version: number }): void => {
  if (!Number.isSafeInteger(definition.version) || definition.version < 1) {
    // A definition is a module constant: failing here is broken setup.
    throw new TypeError(`Document ${definition.kind} version must be a positive integer`);
  }
};

// -----------------------------------------------------------------------------
// observation
// -----------------------------------------------------------------------------

/**
 * One delivery of a watch: the newest committed value and the operations that
 * produced it from the previous frame's.
 *
 * @category observation
 * @since 0.0.0
 */
export interface WatchFrame<T> {
  /** The newest value; after a retirement, the last value the watch was given. */
  readonly value: Readonly<T>;
  /** The ops that turn the previous frame's value into `value`, folded across the frames pulled at once. */
  readonly ops: Delta.Delta;
  /** The document was retired since the previous frame; a later incarnation arrives as `[["r", value]]`. */
  readonly retired: boolean;
}

/**
 * A watch of one document address: it starts at the
 * incarnation found at acquisition and follows the address across retirements
 * and recreations.
 *
 * @category observation
 * @since 0.0.0
 */
// Frames are a single-consumer stream: pulling one frame at a time serializes delivery
// and folds what accumulated meanwhile into it; closing the acquiring scope (`stopped`), or a consumer that
// fails or is interrupted (`listener_error`, `aborted`), stops the watch. The stream ends when the Session
// closes (`ended`) or after a task document's terminal retirement, and fails when the Session fails
// (`failed`).
export interface DocumentWatch<T> {
  /** The acquisition revision until the first frame is pulled, then the latest pulled frame's value. */
  readonly value: Readonly<T>;
  /**
   * Committed frames, for one consumer at a time; the watch stops when a consumer fails or is interrupted,
   * while one that ends early (`Stream.take`) leaves what is pending for the next. What accumulates
   * between pulls is folded into one frame (`PendingOps`); past 10,000 pending ops the frame is one root
   * replacement of the newest value.
   */
  readonly frames: Stream.Stream<WatchFrame<T>, SessionFailed>;
}

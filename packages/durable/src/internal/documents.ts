// Address resolution, record checks and materialization of typed document access.
import * as Json from "@effect-durable-harness/delta/Json";
import type * as Delta from "@effect-durable-harness/delta/Delta";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import type { DocumentAddress, StoredDocument } from "../AgentStorage.ts";
import type { CheckpointInfo, Doc } from "../Doc.ts";
import { addressKey } from "./commit.ts";
import type { ConversationId, DocumentId, TaskId } from "../Ids.ts";
import type { DocumentCreate, DocumentRecord } from "../Records.ts";
import {
  DocumentAccessError,
  InvalidValue,
  MigrationRequired,
  NewerVersion,
  SemanticsMismatch,
} from "../SessionError.ts";

/** Erased definition the kernel works with after overload resolution. */
export interface AnyDefinition {
  readonly kind: string;
  readonly version: number;
  readonly scope: "session" | "conversation" | "task";
  readonly history?: "latest" | "rewindable" | undefined;
  readonly fork?: "asOf" | "current" | "initial" | undefined;
  readonly family?: true | undefined;
  readonly schema: Schema.Top;
  readonly initial: (seed?: Schema.Json) => Schema.JsonObject;
  readonly migrate?:
    | ((value: Schema.JsonObject, fromVersion: number) => Schema.JsonObject)
    | undefined;
  readonly checkpointWhen: (
    value: Schema.JsonObject,
    ops: Delta.Delta,
    info: CheckpointInfo,
  ) => boolean;
}

/** The erased definition of a token, for the kernel. */
export const definitionOf = <T>(doc: Doc<T, object>): AnyDefinition =>
  // SAFETY: every token comes from `Doc.make` or `Doc.family`, whose definitions have this shape
  // once `T` and the seed type are erased.
  doc.definition as AnyDefinition;

/** A materialized value, typed back as the value its token describes. */
export const asValue = <T>(value: Schema.JsonObject): Readonly<T> =>
  // SAFETY: the value was materialized through the token's definition and checked against its schema.
  value as Readonly<T>;

/** One argument after the token in a document overload: an owner ID, a family key or a seed. */
export type DocArgument = Schema.Json;

/** A logical address and its string identity for maps. */
export interface ResolvedAddress {
  readonly address: DocumentAddress;
  readonly id: string;
  /** Index of the first argument after the owner and the family key. */
  readonly nextArgument: number;
}

/** Resolves an overloaded argument list by the definition's scope and family shape. */
export const resolveAddress = (
  definition: AnyDefinition,
  args: ReadonlyArray<DocArgument>,
): ResolvedAddress => {
  let index = 0;
  // SAFETY: the overloads of every document accessor type the owner argument
  // as a `ConversationId` or `TaskId` for exactly these scopes.
  const owner = () => args[index++] as ConversationId & TaskId;
  const scope: DocumentAddress["scope"] =
    definition.scope === "session"
      ? { kind: "session" }
      : definition.scope === "conversation"
        ? { kind: "conversation", conversationId: owner() }
        : { kind: "task", taskId: owner() };
  // SAFETY: family overloads type the argument after the owner as a string key.
  const key = definition.family === true ? (args[index++] as string) : undefined;
  const address: DocumentAddress =
    key === undefined ? { kind: definition.kind, scope } : { kind: definition.kind, scope, key };
  return { address, id: addressKey(address), nextArgument: index };
};

/** Span attributes of a document access: identity only, never the value. */
export const spanAttributes = (address: DocumentAddress): SpanAttributes => {
  const scope = address.scope;
  if (scope.kind === "conversation") {
    return {
      "durable.document.kind": address.kind,
      "durable.conversation.id": scope.conversationId,
    };
  }
  if (scope.kind === "task")
    return { "durable.document.kind": address.kind, "durable.task.id": scope.taskId };
  return { "durable.document.kind": address.kind };
};

/** Identity attributes of a Session span. A type literal, so it fits `withSpan`'s attribute record. */
export type SpanAttributes = {
  readonly "durable.document.kind"?: string;
  readonly "durable.conversation.id"?: number;
  readonly "durable.task.id"?: number;
};

/** The storage create record for a new incarnation at an address. */
export const documentCreate = (
  definition: AnyDefinition,
  address: DocumentAddress,
  id: DocumentId,
): DocumentCreate => {
  const key = address.key === undefined ? {} : { key: address.key };
  const scope = address.scope;
  if (scope.kind === "session") return { id, kind: address.kind, ...key, scope };
  if (scope.kind === "task") return { id, kind: address.kind, ...key, scope };
  return definition.history === "rewindable"
    ? {
        id,
        kind: address.kind,
        ...key,
        scope,
        history: "rewindable",
        fork: definition.fork ?? "initial",
      }
    : {
        id,
        kind: address.kind,
        ...key,
        scope,
        history: "latest",
        fork: definition.fork === "current" ? "current" : "initial",
      };
};

const accessError = (reason: DocumentAccessError["reason"]) => new DocumentAccessError({ reason });

/** Rejects typed access whose token disagrees with the persisted scope, history or fork. */
export const checkRecordScope = (
  definition: AnyDefinition,
  record: DocumentCreate | DocumentRecord,
): Result.Result<void, DocumentAccessError> =>
  record.scope.kind !== definition.scope ||
  ("history" in record &&
    (record.history !== definition.history || record.fork !== definition.fork))
    ? Result.fail(
        accessError(
          new SemanticsMismatch({
            message: `Document ${record.id} (${record.kind}) does not match the supplied definition semantics`,
          }),
        ),
      )
    : Result.void;

/** Rejects typed access to a stored version the definition cannot use. */
export const checkRecordVersion = (
  definition: AnyDefinition,
  record: DocumentCreate | DocumentRecord,
  version: number,
): Result.Result<void, DocumentAccessError> => {
  if (version > definition.version) {
    return Result.fail(
      accessError(
        new NewerVersion({
          message: `Document ${record.id} (${record.kind}) has newer version ${version} than ${definition.version}`,
        }),
      ),
    );
  }
  if (version < definition.version && definition.migrate === undefined) {
    return Result.fail(
      accessError(
        new MigrationRequired({
          message: `Document ${record.id} (${record.kind}) requires migration from version ${version}`,
        }),
      ),
    );
  }
  return Result.void;
};

/** Checks both, as every typed access does. */
export const checkRecord = (
  definition: AnyDefinition,
  record: DocumentCreate | DocumentRecord,
  version: number,
): Result.Result<void, DocumentAccessError> =>
  Result.flatMap(checkRecordScope(definition, record), () =>
    checkRecordVersion(definition, record, version),
  );

/**
 * Validates and materializes one detached value for typed access: checks the
 * record, migrates an older version (copying the result into kernel
 * ownership) and checks the value against the definition's schema.
 */
export const materializeValue = (
  definition: AnyDefinition,
  record: DocumentCreate | DocumentRecord,
  version: number,
  value: Schema.JsonObject,
): Result.Result<Schema.JsonObject, DocumentAccessError> =>
  Result.flatMap(checkRecord(definition, record, version), () => {
    if (version === definition.version) return checkValue(definition, record, value);
    // A throw in `migrate` is a defect of the definition. Called as a method, so its receiver is the definition.
    // SAFETY: `checkRecordVersion` rejected an older version without `migrate`.
    const migrated = definition.migrate!(value, version);
    // A migration result is caller code: copy it, rejecting non-JSON.
    const copied = Result.mapError(copyJson(migrated), (message) =>
      accessError(new InvalidValue({ message })),
    );
    // SAFETY: a copy of the object `migrate` returned is an object; `checkValue` checks its shape.
    return Result.flatMap(copied, (value) =>
      checkValue(definition, record, value as Schema.JsonObject),
    );
  });

/** Materializes a stored document. */
export const materializeStored = (
  definition: AnyDefinition,
  stored: StoredDocument,
): Result.Result<Schema.JsonObject, DocumentAccessError> =>
  materializeValue(definition, stored.record, stored.version, stored.value);

// A materialized value must be strict JSON and is also checked against the
// definition's schema. `Schema.is` does not rebuild it, so identities and
// structural sharing hold.
const checkValue = (
  definition: AnyDefinition,
  record: DocumentCreate | DocumentRecord,
  value: Schema.JsonObject,
): Result.Result<Schema.JsonObject, DocumentAccessError> =>
  Schema.is(definition.schema)(value)
    ? Result.succeed(value)
    : Result.fail(
        accessError(
          new InvalidValue({
            message: `Document ${record.id} (${record.kind}) does not match the schema of version ${definition.version}`,
          }),
        ),
      );

/**
 * A value caller code hands to the kernel: JSON, possibly with `undefined`
 * object properties, which a copy with `omitUndefinedProperties` drops.
 */
export type JsonInput =
  | Schema.Json
  | undefined
  | { readonly [key: string]: JsonInput }
  | ReadonlyArray<JsonInput>;

/**
 * Copies caller JSON into kernel ownership, failing
 * with `Json.copy`'s message when the value is not strict JSON.
 */
export const copyJson = (
  value: JsonInput,
  options?: Json.CopyOptions,
): Result.Result<Schema.Json, string> =>
  Result.try({
    try: () => Json.copy(value, options),
    catch: (error) => (error instanceof Error ? error.message : String(error)),
  });

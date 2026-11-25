import * as Tracker from "@effect-durable-harness/delta/Tracker";
import * as Predicate from "effect/Predicate";
import type * as Schema from "effect/Schema";

/** A mutable JSON object, as a draft's. */
export type JsonRecord = { [key: string]: Schema.Json };
type JsonContainer = JsonRecord | Array<Schema.Json>;

/** How `assignJson` writes one value instead of assigning it leaf by leaf. */
export type AssignPlan =
  /** The string grew by `text` since it was last assigned: append it, when the lengths agree. */
  | { readonly kind: "append"; readonly text: string }
  /** Leave the value as it is; the caller writes it. */
  | { readonly kind: "keep" }
  /** Plans for some members or items of a container. */
  | { readonly kind: "fields"; readonly fields: ReadonlyMap<string | number, AssignPlan> };

/**
 * Assigns `value` at `target[key]` leaf by leaf. A container assignment is
 * one full set in the delta, and only a string leaf reassigned with a longer string is an append, so writing a
 * partial whole would store and publish the complete message on every flush. That append costs a compare of
 * the whole string, though; where `plan` says what a string member grew by, and the committed string plus
 * that text has the new string's length, the text is appended with `Tracker.appendText` instead.
 */
export const assignJson = (
  target: JsonContainer,
  key: string | number,
  value: Schema.Json,
  plan?: AssignPlan | undefined,
): void => {
  if (plan?.kind === "keep") return;
  // SAFETY: an object or an array, indexed by the key the caller passes for it.
  const slots = target as Record<string | number, Schema.Json>;
  // A `__proto__` member the target lacks reads through to Object.prototype; merging into
  // that would pollute every object in the process. Other inherited members are functions, never merged into.
  const current: Schema.Json | undefined =
    key === "__proto__" && !Object.hasOwn(slots, key) ? undefined : slots[key];
  const fields = plan?.kind === "fields" ? plan.fields : undefined;
  if (isRecord(current) && isRecord(value)) {
    // SAFETY: an object of the draft the caller edits.
    const editable = current as JsonRecord;
    for (const name of Object.keys(editable))
      if (!Object.hasOwn(value, name)) delete editable[name];
    for (const [name, child] of Object.entries(value))
      assignJson(editable, name, child, fields?.get(name));
    return;
  }
  if (Array.isArray(current) && Array.isArray(value) && current.length <= value.length) {
    const items: Array<Schema.Json> = current;
    for (let index = 0; index < value.length; index++) {
      const item = value[index] ?? null;
      if (index < items.length) assignJson(items, index, item, fields?.get(index));
      else items.push(item);
    }
    return;
  }
  if (
    plan?.kind === "append" &&
    Predicate.isString(current) &&
    Predicate.isString(value) &&
    !Array.isArray(target) &&
    current.length + plan.text.length === value.length
  ) {
    appendTo(target, String(key), plan.text);
    return;
  }
  if (current !== value) slots[key] = value;
};

/** Appends `text` to the string member `key` of `target`, a draft object, at O(text) cost. */
export const appendTo = (target: JsonRecord, key: string, text: string): void =>
  // SAFETY: the caller checked that `target[key]` is a string of a draft object.
  Tracker.appendText(target as Tracker.Draft<Record<string, string>>, key, text);

export const isRecord = (value: Schema.Json | undefined): value is JsonRecord =>
  Predicate.isObject(value) && !Array.isArray(value);

/** Structural JSON equality; object key order does not matter. */
export const jsonEqual = (a: Schema.Json, b: Schema.Json): boolean => {
  if (a === b) return true;
  if (!Predicate.isObject(a) || !Predicate.isObject(b)) return false;
  if (Array.isArray(a) || Array.isArray(b)) {
    return (
      Array.isArray(a) &&
      Array.isArray(b) &&
      a.length === b.length &&
      a.every((item: Schema.Json, index) => jsonEqual(item, b[index]))
    );
  }
  // SAFETY: neither is an array, so both are JSON objects.
  const left = a as Schema.JsonObject;
  // SAFETY: as `left`.
  const right = b as Schema.JsonObject;
  const keys = Object.keys(left);
  return (
    keys.length === Object.keys(right).length &&
    keys.every((key) => Object.hasOwn(right, key) && jsonEqual(left[key]!, right[key]!))
  );
};

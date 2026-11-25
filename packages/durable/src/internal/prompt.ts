// Sections in effect after replaying the transcript, the
// agent's sections rendered for one request, and the plan of `core.system` entries that make the replayed prompt
// and tools equal what the request wants. Rendering runs user code off the Session line; planning is pure.
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Cause from "effect/Cause";
import * as Schema from "effect/Schema";
import type * as AgentTool from "../AgentTool.ts";
import type { ContextView } from "../ContextView.ts";
import { SystemEntry, type TypedEntryDraft } from "../Entry.ts";
import type { Message, SystemMessage, Tool, ToolReference } from "../Message.ts";
import type { ContextEdit } from "../Records.ts";
import type * as Section from "../Section.ts";
import { type Failure, stoppedBy } from "./decisions.ts";
import { declarationsEqual, getCurrentTools } from "./transcript.ts";

type SystemDraft = TypedEntryDraft<never>;

/** A system message under construction. */
type MutableSystemMessage = { -readonly [K in keyof SystemMessage]: SystemMessage[K] };

interface ToolChanges {
  readonly toolsRemoved: ReadonlyArray<ToolReference>;
  readonly toolsAdded: ReadonlyArray<Tool>;
}

/** Sections in effect after replaying system messages in order. */
export const replaySections = (messages: ReadonlyArray<Message>): Map<string, string> => {
  const shown = new Map<string, string>();
  for (const message of messages) {
    if (message.role !== "system" || message.sections === undefined) continue;
    for (const [key, value] of Object.entries(message.sections)) {
      if (value === null) shown.delete(key);
      else shown.set(key, value);
    }
  }
  return shown;
};

/**
 * Renders the agent's sections in order. `undefined` omits a section;
 * tagged text is wrapped as `<key>\n...\n</key>`. A section that fails keeps its shown text, if any, and is
 * reported; an interruption propagates.
 */
export const renderSections = (
  sections: ReadonlyArray<Section.Any>,
  input: Section.PromptInput,
  shown: ReadonlyMap<string, string>,
  report: (cause: Cause.Cause<unknown>, section: Section.Any) => Effect.Effect<void>,
): Effect.Effect<Map<string, string>> =>
  Effect.gen(function* () {
    const desired = new Map<string, string>();
    for (const section of sections) {
      const key = section.key;
      const exit = yield* Effect.exit(erasedSection(section).render(input));
      if (Exit.isFailure(exit)) {
        // A failure once the invocation is stopped propagates instead of being reported.
        if (stoppedBy(exit.cause)) return yield* Effect.interrupt;
        // Each failing section is reported as it happens.
        yield* report(exit.cause, section);
        const kept = shown.get(key);
        if (kept !== undefined) desired.set(key, kept);
        continue;
      }
      const text = exit.value;
      if (text === undefined) continue;
      desired.set(key, section.definition.tag === false ? text : `<${key}>\n${text}\n</${key}>`);
    }
    return desired;
  });

/** A section as the generation task calls it. */
interface ErasedSection {
  readonly key: string;
  readonly tag?: boolean | undefined;
  readonly render: (input: Section.PromptInput) => Effect.Effect<string | undefined, Failure>;
}

const erasedSection = (section: Section.Any): ErasedSection =>
  // SAFETY: every section is built by `Section.make`; its requirements are the Harness's, provided to the
  // handler that renders it, and any failure is reported as a Cause.
  section.definition as ErasedSection;

/**
 * The declaration the model is offered for a tool: its name,
 * description, and the JSON Schema of its parameters, or its own `jsonSchema`.
 */
export const toolDeclaration = (tool: AgentTool.Any): Tool => ({
  name: tool.name,
  description: tool.definition.description,
  parameters: toolJsonSchema(tool),
});

/** The JSON Schema of the tool's parameters the model is offered: its own `jsonSchema`, or the generated one. */
export const toolJsonSchema = (tool: AgentTool.Any): Schema.JsonObject =>
  tool.definition.jsonSchema ?? parametersSchema(tool.definition.parameters);

/** Generated schemas by `parameters`: a tool's schema is long-lived, and each call's arguments are coerced by it. */
const generated = new WeakMap<Schema.Decoder<unknown>, Schema.JsonObject>();

const parametersSchema = (parameters: Schema.Decoder<unknown>): Schema.JsonObject => {
  const cached = generated.get(parameters);
  if (cached !== undefined) return cached;
  const schema = generateSchema(parameters);
  generated.set(parameters, schema);
  return schema;
};

const generateSchema = (parameters: Schema.Decoder<unknown>): Schema.JsonObject => {
  const document = Schema.toJsonSchemaDocument(parameters);
  // SAFETY: a generated JSON Schema document is JSON, as Effect's own tools send it (ai/Tool.ts:1748-1761).
  const schema = document.schema as Schema.JsonObject;
  if (Object.keys(document.definitions).length === 0) return schema;
  // SAFETY: as above.
  return { ...schema, $defs: document.definitions as Schema.JsonObject };
};

/**
 * The `core.system` entries that make the replayed sections and tools
 * of `view` equal `desired` and `tools` in values and order.
 *
 * - A head marker with no later `core.system` entry in context: one complete baseline that omits every retained
 *   earlier `core.system` entry, written even when it restates the replayed values.
 * - Otherwise, when a minimal section patch would leave a different order: remove every shown section, then
 *   re-add every desired section in order.
 * - Otherwise the minimal patch of changed values and `null` removals, or nothing.
 *
 * Tool changes ride on the last planned entry, or on one entry of their own.
 */
export const planSystemEntries = (
  view: ContextView,
  desired: ReadonlyMap<string, string>,
  tools: ReadonlyArray<Tool>,
  timestamp: number,
): Array<SystemDraft> => {
  const head = view.head;
  if (
    head !== undefined &&
    !view.entries.some((entry) => SystemEntry.is(entry) && entry.id > head.id)
  ) {
    const edits: Array<ContextEdit> = view.entries
      .filter((entry) => SystemEntry.is(entry))
      .map((entry) => ({ target: entry.id, action: "omit" }));
    const baseline = systemEntry(
      Object.fromEntries(desired),
      { toolsRemoved: [], toolsAdded: tools },
      timestamp,
    );
    return [edits.length === 0 ? baseline : { ...baseline, edits }];
  }
  const sections = planSections(replaySections(view.messages), desired);
  const changes = planTools(getCurrentTools(view.messages), tools);
  if (changes.toolsRemoved.length === 0 && changes.toolsAdded.length === 0) {
    return sections.map((patch) => systemEntry(patch, undefined, timestamp));
  }
  if (sections.length === 0) return [systemEntry(undefined, changes, timestamp)];
  return sections.map((patch, index) =>
    systemEntry(patch, index === sections.length - 1 ? changes : undefined, timestamp),
  );
};

/**
 * Tool changes from `offered` to `desired`. A changed declaration is
 * removed and re-added; when replay would not yield the desired order, every offered tool is removed and every
 * desired tool re-added in order.
 */
const planTools = (offered: ReadonlyArray<Tool>, desired: ReadonlyArray<Tool>): ToolChanges => {
  const wanted = new Map(desired.map((tool) => [tool.name, tool]));
  const kept = offered.filter((tool) => {
    const next = wanted.get(tool.name);
    return next !== undefined && declarationsEqual(tool, next);
  });
  const keptNames = new Set(kept.map((tool) => tool.name));
  const added = desired.filter((tool) => !keptNames.has(tool.name));
  const replayed = [...kept, ...added];
  if (replayed.some((tool, index) => tool.name !== desired[index]?.name)) {
    return {
      toolsRemoved: offered.map((tool) => ({ name: tool.name })),
      toolsAdded: desired,
    };
  }
  return {
    toolsRemoved: offered
      .filter((tool) => !keptNames.has(tool.name))
      .map((tool) => ({ name: tool.name })),
    toolsAdded: added,
  };
};

/**
 * Section changes: none, the minimal patch, or a remove-all and re-add-all pair when the
 * order would differ.
 */
const planSections = (
  shown: ReadonlyMap<string, string>,
  desired: ReadonlyMap<string, string>,
): Array<Record<string, string | null>> => {
  const patchedOrder = [
    ...[...shown.keys()].filter((key) => desired.has(key)),
    ...[...desired.keys()].filter((key) => !shown.has(key)),
  ];
  const desiredOrder = [...desired.keys()];
  if (patchedOrder.some((key, index) => key !== desiredOrder[index])) {
    return [
      Object.fromEntries([...shown.keys()].map((key) => [key, null])),
      Object.fromEntries(desired),
    ];
  }
  const patch: Record<string, string | null> = {};
  for (const [key, value] of shown) {
    const next = desired.get(key);
    if (next !== value) patch[key] = next ?? null;
  }
  for (const [key, value] of desired) if (!shown.has(key)) patch[key] = value;
  return Object.keys(patch).length === 0 ? [] : [patch];
};

/** A `core.system` entry with the given section and tool changes. */
const systemEntry = (
  sections: Record<string, string | null> | undefined,
  tools: ToolChanges | undefined,
  timestamp: number,
): SystemDraft => {
  const message: MutableSystemMessage = {
    role: "system",
    content: "",
    timestamp,
  };
  if (sections !== undefined) message.sections = sections;
  if (tools !== undefined && tools.toolsRemoved.length > 0)
    message.toolsRemoved = tools.toolsRemoved;
  if (tools !== undefined && tools.toolsAdded.length > 0) message.toolsAdded = tools.toolsAdded;
  return { model: [message] };
};

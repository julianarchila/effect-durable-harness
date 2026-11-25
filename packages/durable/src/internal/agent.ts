// Resolving a conversation's agent from `core.agent`, the static extensions
// and the settings, applying `configure` changes and a round's `addTools`, and the built-in `core.agent` part of
// the creation hook. Resolution is pure and synchronous; the draft edits run inside commit callbacks.
import * as Json from "@effect-durable-harness/delta/Json";
import type * as Tracker from "@effect-durable-harness/delta/Tracker";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import {
  type Agent,
  type AgentChange,
  AgentDoc,
  type AgentState,
  INSTRUCTIONS_KEY,
} from "../Agent.ts";
import type * as AgentTool from "../AgentTool.ts";
import type * as Extension from "../Extension.ts";
import type * as Hook from "../Hook.ts";
import type { ConversationId } from "../Ids.ts";
import type { ConversationRecord } from "../Records.ts";
import * as Section from "../Section.ts";
import type { CompactionPolicy, CompactionSettings, PartialPolicy, Settings } from "../Settings.ts";
import { Tx, type TxDocError } from "../Tx.ts";
import { wrapComposite } from "./parks.ts";

type Draft = Tracker.Draft<AgentState>;
type StoredCompactionSettings = NonNullable<AgentState["compaction"]>;
type Mutable<T> = { -readonly [K in keyof T]: T[K] };

/** One change to `core.agent`, in the caller's commit. */
export const configure = (conversationId: ConversationId, change: AgentChange) =>
  Effect.gen(function* () {
    // A caller's bug the type cannot express, like the other argument checks (`Doc.make`'s version), so a
    // defect here.
    const maxTurns = change.maxTurns;
    if (
      maxTurns !== undefined &&
      maxTurns !== null &&
      !(Number.isSafeInteger(maxTurns) && maxTurns >= 1)
    ) {
      return yield* Effect.die(
        new RangeError(`maxTurns must be an integer 1 or more, got ${String(maxTurns)}`),
      );
    }
    const compaction =
      change.compaction === undefined || change.compaction === null
        ? change.compaction
        : storedCompaction(change.compaction);
    if (compaction instanceof RangeError) return yield* Effect.die(compaction);
    const tx = yield* Tx;
    const state = yield* tx.doc(AgentDoc, conversationId);
    applyChange(state, change, compaction);
  });

/** A stored copy of a conversation's compaction settings, without undefined fields. */
const storedCompaction = (settings: CompactionSettings): StoredCompactionSettings | RangeError => {
  const policy = compactionFields(settings);
  const overrides = settings.modelOverrides;
  if (policy instanceof RangeError || overrides === undefined) return policy;
  const stored: Record<string, Mutable<CompactionFields>> = {};
  for (const [key, override] of Object.entries(overrides)) {
    if (override === undefined) continue;
    const fields = compactionFields(override);
    if (fields instanceof RangeError) return fields;
    stored[key] = fields;
  }
  return { ...policy, modelOverrides: stored };
};

type CompactionFields = Omit<StoredCompactionSettings, "modelOverrides">;

const TOKEN_FIELDS = ["reserveTokens", "keepRecentTokens", "backgroundTokens"] as const;

/** The set fields of `policy`, or a `RangeError` for a token count that is not an integer `0` or more. */
const compactionFields = (
  policy: PartialPolicy<CompactionPolicy>,
): Mutable<CompactionFields> | RangeError => {
  const fields: Mutable<CompactionFields> =
    policy.enabled === undefined ? {} : { enabled: policy.enabled };
  for (const key of TOKEN_FIELDS) {
    const value = policy[key];
    if (value === undefined) continue;
    if (!(Number.isSafeInteger(value) && value >= 0)) {
      return new RangeError(`compaction.${key} must be an integer 0 or more, got ${String(value)}`);
    }
    fields[key] = value;
  }
  return fields;
};

/**
 * A round's `addTools`, applied to `tools` and `modelTools` by name, without
 * the registry, so the next request offers the tools.
 */
export const addTools = (
  conversationId: ConversationId,
  added: ReadonlyArray<string>,
): Effect.Effect<void, TxDocError, Tx> =>
  Effect.gen(function* () {
    const tx = yield* Tx;
    const state = yield* tx.doc(AgentDoc, conversationId);
    const tools = state.tools;
    if (isList(tools)) {
      for (const name of added) if (!tools.includes(name)) tools.push(name);
    } else if (tools !== undefined && tools.remove.some((name) => added.includes(name))) {
      state.tools = { remove: tools.remove.filter((name) => !added.includes(name)) };
    }
    const modelTools = state.modelTools;
    if (isList(modelTools)) {
      for (const name of added) if (!modelTools.includes(name)) modelTools.push(name);
      return;
    }
    // Assigned whole; a document write of an equal value records no change.
    const remove = (modelTools?.remove ?? []).filter((name) => !added.includes(name));
    const add = [...(modelTools?.add ?? [])];
    for (const name of added) if (!add.includes(name)) add.push(name);
    state.modelTools = remove.length === 0 ? { add } : { remove, add };
  });

/**
 * The built-in part of every commit that creates or forks a conversation,
 * for `core.agent`. A fork keeps its `asOf` copy; a new task-owned conversation copies the stored agent of its
 * owner task's conversation; a new ownerless one starts empty.
 */
export const createAgent = (record: ConversationRecord) =>
  Effect.gen(function* () {
    if (record.parent !== undefined) return;
    const tx = yield* Tx;
    const agent = yield* tx.doc(AgentDoc, record.id);
    if (record.owner === undefined) return;
    const owner = yield* tx.doc(AgentDoc, record.owner.conversationId);
    // SAFETY: a strict-JSON copy of the owner's stored agent state, which the schema typed when it loaded.
    Object.assign(agent, Json.copy(owner) as AgentState);
  });

/**
 * Handlers of the selected extensions' hooks for a task name, in extension order, each with its extension's
 * name.
 */
export const agentHooks = (
  agent: Agent,
  taskName: string,
): ReadonlyArray<{ readonly extension: string; readonly handlers: Hook.Any["handlers"] }> => {
  const handlers: Array<{ readonly extension: string; readonly handlers: Hook.Any["handlers"] }> =
    [];
  for (const extension of agent.extensions) {
    for (const hook of extension.hooks) {
      if (hook.task === taskName)
        handlers.push({ extension: extension.name, handlers: hook.handlers });
    }
  }
  return handlers;
};

/**
 * The extension whose code a tool of `agent` runs: the last selected one that provides or wraps it.
 */
export const toolExtension = (agent: Agent, name: string): string | undefined => {
  let found: string | undefined;
  for (const extension of agent.extensions) {
    const provides = extension.tools.some((tool) => tool.name === name);
    const wraps = extension.wraps.some((wrap) => "tool" in wrap && wrap.tool === name);
    if (provides || wraps) found = extension.name;
  }
  return found;
};

/**
 * The extension whose code a section of `agent` runs: the last selected one that provides or wraps it.
 */
export const sectionExtension = (agent: Agent, key: string): string | undefined => {
  let found: string | undefined;
  for (const extension of agent.extensions) {
    const provides = extension.sections.some((section) => section.key === key);
    const wraps = extension.wraps.some((wrap) => "section" in wrap && wrap.section === key);
    if (provides || wraps) found = extension.name;
  }
  return found;
};

/**
 * An agent from its stored state (absent: every field unset), the
 * Harness's extensions and the resolved settings. A wrapper that fails or renames drops its target and is
 * reported with the wrapper's extension; a wrapper without a target does nothing.
 */
export const resolveAgent = (
  state: Readonly<AgentState> | undefined,
  installed: ReadonlyArray<Extension.Any>,
  settings: Settings,
  report: (cause: Cause.Cause<unknown>, extension: string) => void,
): Agent => {
  const extensions = selectExtensions(state?.extensions, installed, settings);

  const composed = new Map<string, AgentTool.Any>();
  for (const extension of extensions)
    for (const tool of extension.tools) composed.set(tool.name, tool);
  const sections = new Map<string, Section.Any>();
  for (const extension of extensions) {
    for (const section of extension.sections) sections.set(section.key, section);
  }
  for (const extension of extensions) {
    for (const wrap of extension.wraps) {
      if ("tool" in wrap) {
        const wrapper = wrap.wrap;
        applyWrap(
          composed,
          wrap.tool,
          (tool) => wrapComposite(tool, wrapper),
          (tool) => tool.name,
          (cause) => report(cause, extension.name),
        );
      } else {
        applyWrap(
          sections,
          wrap.section,
          wrap.wrap,
          (section) => section.key,
          (cause) => report(cause, extension.name),
        );
      }
    }
  }

  const enabled = filterTools([...composed.values()], state?.tools);
  // `callers` splits the enabled tools: the model is offered those it may call,
  // through `modelTools`, and its deferred list holds those it may call and is not offered yet; nested calls
  // resolve among those tools may call.
  const modelCallable = enabled.filter(callableBy("model"));
  const tools = offeredTools(modelCallable, state?.modelTools);
  const offered = new Set(tools);
  const deferred = modelCallable.filter(
    (tool) => tool.definition.deferred === true && !offered.has(tool),
  );
  const callable = enabled.filter(callableBy("tools"));

  const instructions = state?.instructions;
  const agentSections: Array<Section.Any> = [...sections.values()];
  if (instructions !== undefined) {
    agentSections.push(Section.make(INSTRUCTIONS_KEY, () => Effect.succeed(instructions)));
  }

  const agent: Mutable<Agent> = {
    thinkingLevel: state?.thinkingLevel ?? "off",
    extensions,
    tools,
    deferred,
    callable,
    sections: agentSections,
  };
  if (state?.model !== undefined) agent.model = state.model;
  if (instructions !== undefined) agent.instructions = instructions;
  if (state?.cwd !== undefined) agent.cwd = state.cwd;
  if (state?.maxTurns !== undefined) agent.maxTurns = state.maxTurns;
  if (state?.compaction !== undefined) agent.compaction = state.compaction;
  return agent;
};

/** Whether `caller` may call the tool; default both. */
const callableBy = (caller: "model" | "tools") => (tool: AgentTool.Any) =>
  tool.definition.callers === undefined || tool.definition.callers.includes(caller);

/** The tools offered to the model, as `AgentState.modelTools` describes. */
const offeredTools = (
  tools: ReadonlyArray<AgentTool.Any>,
  filter: AgentState["modelTools"],
): ReadonlyArray<AgentTool.Any> => {
  if (isList(filter)) return filterTools(tools, filter);
  const removed = new Set(filter?.remove ?? []);
  const kept = tools.filter((tool) => tool.definition.deferred !== true && !removed.has(tool.name));
  // Loaded deferred tools follow in load order, so loading one appends to the declared tools.
  const byName = new Map(tools.map((tool) => [tool.name, tool]));
  const loaded = [...new Set(filter?.add ?? [])].flatMap((name) => {
    const tool = byName.get(name);
    return tool?.definition.deferred === true && !removed.has(name) ? [tool] : [];
  });
  return [...kept, ...loaded];
};

/** `tools` through a stored filter: an array selects exactly its names, in its order; `{ remove }` drops names. */
const filterTools = (
  tools: ReadonlyArray<AgentTool.Any>,
  filter: AgentState["tools"] | ReadonlyArray<string>,
): ReadonlyArray<AgentTool.Any> => {
  if (filter === undefined) return tools;
  if (isList(filter)) {
    const byName = new Map(tools.map((tool) => [tool.name, tool]));
    return [...new Set(filter)].flatMap((name) => {
      const tool = byName.get(name);
      return tool === undefined ? [] : [tool];
    });
  }
  const removed = new Set(filter.remove);
  return tools.filter((tool) => !removed.has(tool.name));
};

/** Selected extensions: the stored array, or the default selection edited by `{ add, remove }`. */
const selectExtensions = (
  stored: AgentState["extensions"],
  installed: ReadonlyArray<Extension.Any>,
  settings: Settings,
): ReadonlyArray<Extension.Any> => {
  let selected: ReadonlyArray<string>;
  if (isList(stored)) selected = stored;
  else {
    const base = (settings.extensions ?? installed).map((extension) => extension.name);
    const removed = new Set(stored?.remove ?? []);
    selected = [...base, ...(stored?.add ?? [])].filter((name) => !removed.has(name));
  }
  const byName = new Map(installed.map((extension) => [extension.name, extension]));
  const extensions: Array<Extension.Any> = [];
  for (const name of new Set(selected)) {
    const extension = byName.get(name);
    if (extension !== undefined) extensions.push(extension);
  }
  return extensions;
};

const applyWrap = <T>(
  items: Map<string, T>,
  target: string,
  wrap: (item: T) => T,
  nameOf: (item: T) => string,
  report: (cause: Cause.Cause<unknown>) => void,
): void => {
  const item = items.get(target);
  if (item === undefined) return;
  try {
    const wrapped = wrap(item);
    if (nameOf(wrapped) !== target) {
      throw new Error(`Wrapper renamed ${target} to ${nameOf(wrapped)}`);
    }
    items.set(target, wrapped);
  } catch (error) {
    // A wrapper is plain user code: its throw drops the target and is reported.
    items.delete(target);
    report(Cause.die(error));
  }
};

const applyChange = (
  state: Draft,
  change: AgentChange,
  compaction: StoredCompactionSettings | null | undefined,
): void => {
  const set = <K extends keyof AgentState>(key: K, value: AgentState[K] | null | undefined) => {
    if (value === undefined) return;
    if (value === null) {
      delete state[key];
      return;
    }
    // SAFETY: a stored field takes its own JSON type; the draft edits that value.
    state[key] = value as Draft[K];
  };
  set(
    "model",
    change.model === undefined || change.model === null ? change.model : { ...change.model },
  );
  set("thinkingLevel", change.thinkingLevel);
  const extensions = change.extensions;
  set(
    "extensions",
    extensions === undefined || extensions === null
      ? extensions
      : isList(extensions)
        ? names(extensions)
        : listEdit(extensions.add, extensions.remove),
  );
  const tools = change.tools;
  set(
    "tools",
    tools === undefined || tools === null
      ? tools
      : isList(tools)
        ? names(tools)
        : { remove: names(tools.remove) },
  );
  const modelTools = change.modelTools;
  set(
    "modelTools",
    modelTools === undefined || modelTools === null
      ? modelTools
      : isList(modelTools)
        ? names(modelTools)
        : removeAdd(modelTools.remove, modelTools.add),
  );
  set("instructions", change.instructions);
  set("cwd", change.cwd);
  set("maxTurns", change.maxTurns);
  set("compaction", compaction);
};

/** A stored `{ add, remove }` edit of names. */
interface NameEdit {
  add?: Array<string>;
  remove?: Array<string>;
}

const listEdit = (
  add: ReadonlyArray<{ readonly name: string }> | undefined,
  remove: ReadonlyArray<{ readonly name: string }> | undefined,
): NameEdit => {
  const edit: NameEdit = {};
  if (add !== undefined) edit.add = names(add);
  if (remove !== undefined) edit.remove = names(remove);
  return edit;
};

const removeAdd = (
  remove: ReadonlyArray<{ readonly name: string }> | undefined,
  add: ReadonlyArray<{ readonly name: string }> | undefined,
): NameEdit => {
  const edit: NameEdit = {};
  if (remove !== undefined) edit.remove = names(remove);
  if (add !== undefined) edit.add = names(add);
  return edit;
};

const isList = <T, O>(value: ReadonlyArray<T> | O): value is ReadonlyArray<T> =>
  Array.isArray(value);

const names = (items: ReadonlyArray<{ readonly name: string }>): Array<string> =>
  items.map((item) => item.name);

/**
 * Extensions: the static registry of a Harness.
 *
 * An extension names what it contributes: tasks, tools, prompt sections,
 * hooks, and wrappers of other extensions' tools and sections. The list is
 * fixed when `Harness.layer` is built, so the requirements of every
 * handler are part of the layer's type. Tasks are resolved by name for every
 * task; tools, sections, hooks and wraps apply where a conversation's agent
 * selects the extension.
 *
 * @since 0.0.0
 */
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Predicate from "effect/Predicate";
import type * as Schema from "effect/Schema";
import type * as Scope from "effect/Scope";
import * as AgentTool from "./AgentTool.ts";
import type * as Hook from "./Hook.ts";
import type * as Section from "./Section.ts";
import type { Session } from "./Session.ts";
import * as Task from "./Task.ts";
import { capturedServices, taskHandler, toolHandler } from "./internal/handlers.ts";

/**
 * Replaces a tool or a section of the same name wherever the wrapping
 * extension is selected; pure. A wrapper that fails or renames drops its
 * target and is reported.
 *
 * @category models
 * @since 0.0.0
 */
export type Wrap =
  | { readonly tool: string; readonly wrap: (tool: AgentTool.Any) => AgentTool.Any }
  | { readonly section: string; readonly wrap: (section: Section.Any) => Section.Any };

/**
 * A named set of contributions.
 *
 * @category models
 * @since 0.0.0
 */
export interface Extension<
  Tasks extends ReadonlyArray<Task.Any> = ReadonlyArray<Task.Any>,
  Tools extends ReadonlyArray<AgentTool.Any> = ReadonlyArray<AgentTool.Any>,
  Sections extends ReadonlyArray<Section.Any> = ReadonlyArray<Section.Any>,
  Hooks extends ReadonlyArray<Hook.Any> = ReadonlyArray<Hook.Any>,
> {
  /** Unique name of the extension; conversations select extensions by name. */
  readonly name: string;
  /** Task definitions; names are unique across a Harness. */
  readonly tasks: Tasks;
  readonly tools: Tools;
  readonly sections: Sections;
  readonly hooks: Hooks;
  readonly wraps: ReadonlyArray<Wrap>;
  /**
   * Implements every tool and task this extension declares without an implementation, as one Layer (as
   * `effect/ai`'s `toolkit.toLayer`): a record of handlers by name, which the compiler checks is complete. Given
   * an Effect, it runs once when the Layer is built, and the services resolved there are captured.
   */
  toLayer<H extends Handlers<Tasks, Tools>, RB = never>(
    build: H | Effect.Effect<H, never, RB>,
  ): Layer.Layer<Provides<Tasks, Tools>, never, HandlersRequirements<H> | Exclude<RB, Scope.Scope>>;
  /** Types a record of handlers for `toLayer`, as `Toolkit.of` does: the handlers' arguments are inferred. */
  of<H extends Handlers<Tasks, Tools>>(handlers: H): H;
}

/**
 * The handler of each tool in `Tools` declared without an implementation, by name.
 *
 * @category models
 * @since 0.0.0
 */
export type ToolHandlers<Tools extends ReadonlyArray<AgentTool.Any>> = {
  readonly [T in Tools[number] as T extends AgentTool.Declared<
    infer Name,
    infer _P,
    infer _D,
    infer _S
  >
    ? Name
    : never]: T extends AgentTool.Declared<infer _N, infer Params, infer Details, infer Structured>
    ?
        | ToolExecute<Params, Details, Structured>
        | {
            readonly execute: ToolExecute<Params, Details, Structured>;
            readonly cancel?:
              | ((args: Params, call: AgentTool.Call) => Effect.Effect<void, unknown, unknown>)
              | undefined;
          }
    : never;
};

/** A declared tool's `execute`, as a record of handlers holds it. */
type ToolExecute<Params, Details extends Schema.Json, Structured extends Schema.Json> = (
  args: Params,
  call: AgentTool.Call,
) => Effect.Effect<
  AgentTool.ToolExecutionResult<Details, Structured> | AgentTool.ToolPark,
  unknown,
  unknown
>;

/**
 * The handlers of each task in `Tasks` declared without them, by task name.
 *
 * @category models
 * @since 0.0.0
 */
export type TaskHandlers<Tasks extends ReadonlyArray<Task.Any>> = {
  readonly [T in Tasks[number] as T extends Task.Declared<infer Name, infer _I, infer _S, infer _R>
    ? Name
    : never]: T extends Task.Declared<
    infer _N,
    infer I,
    infer S extends { readonly phase: string },
    infer R
  >
    ? Task.Implementation<Task.TaskPhases<I, S, R, unknown>, Task.TaskHandler<I, S, S, R, unknown>>
    : never;
};

/** A record that may be omitted when it has nothing to hold. */
type Group<Key extends string, H> = [keyof H] extends [never]
  ? { readonly [K in Key]?: H }
  : { readonly [K in Key]: H };

/**
 * What an extension's `toLayer` takes: the handlers of its declared tools and of its declared tasks.
 *
 * @category models
 * @since 0.0.0
 */
export type Handlers<
  Tasks extends ReadonlyArray<Task.Any>,
  Tools extends ReadonlyArray<AgentTool.Any>,
> = Group<"tools", ToolHandlers<Tools>> & Group<"tasks", TaskHandlers<Tasks>>;

type ToolHandlerOf<Name> = Name extends string ? AgentTool.Handler<Name> : never;
type TaskHandlerOf<Name> = Name extends string ? Task.Handler<Name> : never;

/**
 * What an extension's `toLayer` provides: one handler per declared tool and task.
 *
 * @category models
 * @since 0.0.0
 */
export type Provides<
  Tasks extends ReadonlyArray<Task.Any>,
  Tools extends ReadonlyArray<AgentTool.Any>,
> = ToolHandlerOf<keyof ToolHandlers<Tools>> | TaskHandlerOf<keyof TaskHandlers<Tasks>>;

type Services<F> = F extends (
  ...args: ReadonlyArray<never>
) => Effect.Effect<infer _A, infer _E, infer R>
  ? R
  : never;
/** The services of a tool's handlers: `execute` alone, or with `cancel`. */
type ToolServices<H> = H extends {
  readonly execute: infer Execute;
  readonly cancel?: infer Cancel;
}
  ? Services<Execute> | Services<Cancel>
  : Services<H>;
type ImplementationServices<I> = I extends {
  readonly phases: infer Phases;
  readonly abort: infer Abort;
}
  ? Services<Phases[keyof Phases]> | Services<Abort>
  : never;

/**
 * The services a record of handlers requires, besides what the Harness gives each call: the `ToolContext`
 * and the `Session`.
 *
 * @category models
 * @since 0.0.0
 */
export type HandlersRequirements<H> = Exclude<
  | (H extends { readonly tools: infer Tools }
      ? Exclude<ToolServices<Tools[keyof Tools]>, AgentTool.ToolContext>
      : never)
  | (H extends { readonly tasks: infer Tasks }
      ? ImplementationServices<Tasks[keyof Tasks]>
      : never),
  Session
>;

/**
 * Any extension.
 *
 * @category models
 * @since 0.0.0
 */
export type Any = Extension<
  ReadonlyArray<Task.Any>,
  ReadonlyArray<AgentTool.Any>,
  ReadonlyArray<Section.Any>,
  ReadonlyArray<Hook.Any>
>;

/**
 * The services the handlers of a list of extensions require.
 *
 * @category models
 * @since 0.0.0
 */
export type Requirements<X extends ReadonlyArray<Any>> = Exclude<
  | Task.Requirements<X[number]["tasks"][number]>
  | AgentTool.Requirements<X[number]["tools"][number]>
  | Section.Requirements<X[number]["sections"][number]>
  | Hook.Requirements<X[number]["hooks"][number]>,
  // The Harness provides the `Session` to every handler, section and hook per call.
  Session
>;

/**
 * Options of `Extension.make` in its object form, with the name inside.
 *
 * @category models
 * @since 0.0.0
 */
export interface ExtensionOptions<
  Tasks extends ReadonlyArray<Task.Any>,
  Tools extends ReadonlyArray<AgentTool.Any>,
  Sections extends ReadonlyArray<Section.Any>,
  Hooks extends ReadonlyArray<Hook.Any>,
> extends ExtensionContents<Tasks, Tools, Sections, Hooks> {
  readonly name: string;
}

/**
 * What `Extension.make(name, contents)` takes: everything the extension contributes.
 *
 * @category models
 * @since 0.0.0
 */
export interface ExtensionContents<
  Tasks extends ReadonlyArray<Task.Any>,
  Tools extends ReadonlyArray<AgentTool.Any>,
  Sections extends ReadonlyArray<Section.Any>,
  Hooks extends ReadonlyArray<Hook.Any>,
> {
  readonly tasks?: Tasks | undefined;
  readonly tools?: Tools | undefined;
  readonly sections?: Sections | undefined;
  readonly hooks?: Hooks | undefined;
  readonly wraps?: ReadonlyArray<Wrap> | undefined;
}

/**
 * Defines an extension: `Extension.make(name, contents)`, or the earlier object form with `name` inside.
 *
 * @category constructors
 * @since 0.0.0
 */
export function make<
  const Tasks extends ReadonlyArray<Task.Any> = readonly [],
  const Tools extends ReadonlyArray<AgentTool.Any> = readonly [],
  const Sections extends ReadonlyArray<Section.Any> = readonly [],
  const Hooks extends ReadonlyArray<Hook.Any> = readonly [],
>(
  name: string,
  contents: ExtensionContents<Tasks, Tools, Sections, Hooks>,
): Extension<Tasks, Tools, Sections, Hooks>;
export function make<
  const Tasks extends ReadonlyArray<Task.Any> = readonly [],
  const Tools extends ReadonlyArray<AgentTool.Any> = readonly [],
  const Sections extends ReadonlyArray<Section.Any> = readonly [],
  const Hooks extends ReadonlyArray<Hook.Any> = readonly [],
>(
  options: ExtensionOptions<Tasks, Tools, Sections, Hooks>,
): Extension<Tasks, Tools, Sections, Hooks>;
export function make(
  first:
    | string
    | ExtensionOptions<Lists["tasks"], Lists["tools"], Lists["sections"], Lists["hooks"]>,
  contents?: ExtensionContents<Lists["tasks"], Lists["tools"], Lists["sections"], Lists["hooks"]>,
): Any {
  // `make(name, contents)`; the object form is the earlier shape.
  return Predicate.isString(first) ? build({ ...contents, name: first }) : build(first);
}

/** The lists of any extension. */
interface Lists {
  readonly tasks: ReadonlyArray<Task.Any>;
  readonly tools: ReadonlyArray<AgentTool.Any>;
  readonly sections: ReadonlyArray<Section.Any>;
  readonly hooks: ReadonlyArray<Hook.Any>;
}

const build = (
  options: ExtensionOptions<Lists["tasks"], Lists["tools"], Lists["sections"], Lists["hooks"]>,
): Any => ({
  // SAFETY: `toLayer`'s signature types the record from the declarations; `handlersLayer` is its erasure.
  toLayer: (build) => handlersLayer(build as never) as never,
  of: (handlers) => handlers,
  name: options.name,
  tasks: options.tasks ?? [],
  tools: options.tools ?? [],
  sections: options.sections ?? [],
  hooks: options.hooks ?? [],
  wraps: options.wraps ?? [],
});

/** What `toLayer` builds, erased: the record's handlers, each under its name's key. */
interface ErasedHandlers {
  readonly tools?:
    | {
        readonly [name: string]: AgentTool.HandlerService["execute"] | AgentTool.HandlerService;
      }
    | undefined;
  readonly tasks?: { readonly [name: string]: Task.HandlerService } | undefined;
}

const handlersLayer = (build: ErasedHandlers | Effect.Effect<ErasedHandlers>): Layer.Layer<never> =>
  Layer.effectContext(
    Effect.gen(function* () {
      const handlers = Effect.isEffect(build) ? yield* build : build;
      const services = yield* capturedServices;
      let context = Context.empty();
      for (const [name, tool] of Object.entries(handlers.tools ?? {})) {
        const handler = Predicate.isFunction(tool)
          ? toolHandler({ execute: tool }, services)
          : toolHandler(tool, services);
        context = Context.add(context, AgentTool.handlerKey(name), handler);
      }
      for (const [name, implementation] of Object.entries(handlers.tasks ?? {})) {
        context = Context.add(
          context,
          Task.handlerKey(name),
          taskHandler(implementation, services),
        );
      }
      // SAFETY: the keys added are the declared names `toLayer`'s signature says it provides.
      return context as Context.Context<never>;
    }),
  );

/**
 * Wraps the tool named like `tool` wherever the wrapping extension is
 * selected.
 *
 * @category constructors
 * @since 0.0.0
 */
export const wrapTool = (
  tool: AgentTool.Any,
  wrapper: (tool: AgentTool.Any) => AgentTool.Any,
): Wrap => ({ tool: tool.name, wrap: wrapper });

/**
 * Wraps the section `key` wherever the wrapping extension is selected.
 *
 * @category constructors
 * @since 0.0.0
 */
export const wrapSection = (key: string, wrapper: (section: Section.Any) => Section.Any): Wrap => ({
  section: key,
  wrap: wrapper,
});

// How the tool task tells a base tool's parks from a wrapper's: the first wrapper of a base gets a copy whose
// execution records every park it returns in its call's `ToolContext`, so the composite's own parks are told
// apart. A wrapper never parks on its own: a park its base did not return in the same run is invalid.
import * as Effect from "effect/Effect";
import * as AgentTool from "../AgentTool.ts";

/**
 * The parks a wrapped base returned in one run of a tool, carried by that run's `ToolContext` under this key. An
 * own enumerable symbol property, so a wrapper that spreads the context passes it on to the base.
 */
export const BASE_PARKS: unique symbol = Symbol("core.tool.baseParks");

/** A run's `ToolContext` with the parks its wrapped base returned. */
export interface ParkCarrier {
  readonly [BASE_PARKS]?: Set<AgentTool.ToolPark> | undefined;
}

/** Composite tools a wrapper built; their parks must come from their base. */
const composites = new WeakSet<AgentTool.Any>();

/** Whether `tool` is a composite a wrapper built, whose parks must come from its base. */
export const isComposite = (tool: AgentTool.Any): boolean => composites.has(tool);

/**
 * Applies `wrap` to `tool` as tool wrappers apply: the first wrapper of a base gets a copy whose execution records
 * every park it returns in its call's `ToolContext`, and the result is a composite.
 */
export const wrapComposite = (
  tool: AgentTool.Any,
  wrap: (tool: AgentTool.Any) => AgentTool.Any,
): AgentTool.Any => {
  const base = composites.has(tool) ? tool : recordingParks(tool);
  const composite = wrap(base);
  composites.add(composite);
  return composite;
};

/** A copy of `tool` whose execution records the parks it returns in the `ToolContext` it runs with. */
const recordingParks = (tool: AgentTool.Any): AgentTool.Any => {
  const execute = (args: never, call: AgentTool.Call & ParkCarrier) =>
    Effect.tap(
      Effect.provideService(AgentTool.run(tool, args), AgentTool.ToolContext, call),
      (returned) =>
        Effect.sync(() => {
          if (AgentTool.isPark(returned)) call[BASE_PARKS]?.add(returned);
        }),
    );
  const cancel = (args: never, call: AgentTool.Call) =>
    Effect.provideService(AgentTool.runCancel(tool, args), AgentTool.ToolContext, call);
  // The definition `AgentTool.run` reads: its own `execute`, and a `cancel` unless the base's own definition
  // has none. A declared base's handlers are known only in its Layer, so its copy always has one.
  // SAFETY: tools are built by `make`; a definition without `execute` is a declared tool's.
  const own = tool.definition as { readonly execute?: unknown; readonly cancel?: unknown };
  const cancels = own.execute === undefined || own.cancel !== undefined;
  const definition = { ...tool.definition, execute, cancel: cancels ? cancel : undefined };
  return { ...tool, definition };
};

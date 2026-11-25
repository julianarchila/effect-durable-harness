// The handler services of declared tools and tasks, shared by `AgentTool.toLayer`, `Task.toLayer` and
// `Extension.toLayer`: each binds a handler to the services its Layer was built with.
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Scope from "effect/Scope";
import type * as AgentTool from "../AgentTool.ts";
import { Session } from "../Session.ts";
import type * as Task from "../Task.ts";

type ErasedTaskHandler = Task.HandlerService["abort"];

/**
 * What a Layer's build saw, without its build scope, since each tool call and task phase runs in a scope of
 * its own, and without a `Session`, which the Harness provides per call: a Layer built once for many
 * sessions must not pin the one it was built beside.
 */
export const capturedServices = Effect.map(
  Effect.context<never>(),
  Context.omit(Scope.Scope, Session),
);

/** A tool's handler service: `execute` and `cancel`, each run with `services`. */
export const toolHandler = (
  handlers: AgentTool.HandlerService,
  services: Context.Context<never>,
): AgentTool.HandlerService => {
  const execute = handlers.execute;
  const cancel = handlers.cancel;
  return {
    execute: (args, call) => Effect.provide(execute(args, call), services),
    cancel:
      cancel === undefined
        ? undefined
        : (args, call) => Effect.provide(cancel(args, call), services),
  };
};

/** A task's handler service: its phases and abort, each run with `services`. */
export const taskHandler = (
  handlers: Task.HandlerService,
  services: Context.Context<never>,
): Task.HandlerService => {
  const bind =
    (handler: ErasedTaskHandler): ErasedTaskHandler =>
    (task, runtime) =>
      Effect.provide(handler(task, runtime), services);
  const phases: { [phase: string]: ErasedTaskHandler | undefined } = {};
  for (const [phase, handler] of Object.entries(handlers.phases)) {
    if (handler !== undefined) phases[phase] = bind(handler);
  }
  return { phases, abort: bind(handlers.abort) };
};

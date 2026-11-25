// The static registry, as the built-in handlers reach it: the generation creates the tasks its `afterTools`,
// `onYield` and `afterRun` hooks ask for from the installed definitions. The runtime has no `registry`, so the Harness provides
// this service to the handlers, as it provides `Session`.
import * as Context from "effect/Context";
import type * as Schema from "effect/Schema";
import type * as Task from "../Task.ts";

/** An installed task: its token, and the phases its handlers serve, from its definition or its handler Layer. */
export interface InstalledTask {
  readonly task: Task.Task<Schema.Json, { readonly phase: string }, Schema.Json, never>;
  readonly phases: ReadonlySet<string>;
  /** The extension that installed it, which a broken result of the task is reported against. */
  readonly extension: string | undefined;
}

export class InstalledTasks extends Context.Service<
  InstalledTasks,
  {
    /** The installed task of kind `name`, if any. */
    readonly get: (name: string) => InstalledTask | undefined;
  }
>()("@effect-durable-harness/core/internal/installed/InstalledTasks") {}

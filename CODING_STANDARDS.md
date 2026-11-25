# Coding standards

The linter and the type checker enforce what they can (`bun run typecheck`, `bun run lint`); fix what they report. This covers the rest. When a rule doesn't cover a case, follow the nearby code: `packages/delta` and the Effect sources in `.repos/effect` are the models. When a rule seems wrong, raise it instead of deviating silently.

## Practices

- **Make the change the task asks for, and only that.** A separate improvement is a separate change.
- **No complexity for a speculative case.** Name the concrete failure and its cost before handling an unlikely race or edge case.
- **One canonical model per concept.** No parallel types, bridge aliases or compatibility shims. Delete dead code.
- **Keep logic in one function** unless a part is reused or names a real concept. Write the happy path first, helpers below it.
- **Ordinary functions for pure logic.** Services are for real edges: storage, clock, model, host.
- **Validate once, at the boundary that owns the data,** then pass typed values inward.
- **Semantic tagged results** over booleans or nullable tuples: `{ _tag: "Accepted" } | { _tag: "Duplicate"; existing }`.
- **Names state the guarantee:** `commit`, `submit`, `claim`; `ensure` for idempotent get-or-create, `load` for reads. Contracts get distinctive names (`AgentStorage`, not `Storage`).
- **Comments explain why,** not what the next line does. They state the behaviour on its own terms, without citing documents or decisions.
- **Every retry, poll and wait is bounded.** A hang is a bug: find the cause, don't re-run.
- **Report what was verified and how,** and any check that could not run.

## TypeScript

- One concept per PascalCase module (`AgentStorage.ts`). Implementation details live in `internal/`, blocked by `"./internal/*": null` in `package.json`.
- Relative imports use the `.ts` extension.
- Inputs with several fields use a named type, not an inline object literal.
- Optional properties are `readonly x?: T | undefined`. Data is `readonly` by default.
- Every type assertion has a `// SAFETY:` comment saying why it holds.

## Effect

- `Effect.gen` for sequential logic, `.pipe` for cross-cutting behaviour (`catchTag`, `retry`, `timeout`).
- No `throw`, `async` or `Promise` in library code. Wrap a platform promise once, at the edge, with `Effect.tryPromise` and a typed error.
- Fail with `return yield* new SomeError(...)`.
- Bind a service before calling it: `const storage = yield* AgentStorage`, then `yield* storage.commit(...)`.
- Decode external data with `Schema`, never `JSON.parse` and casts. Read time, randomness and logs through `Clock`, `Random` and `Effect.log`.

### Services and layers

- A service is a `Context.Service` class keyed `"@effect-durable-harness/<package>/<Module>"`. Its members are `readonly`, each with a one-line doc comment stating its guarantee.
- Build it as a plain object with `Service.of({ ... })` inside a `make` Effect, which gets its dependencies with `yield*`.
- Pass the operation's identity (Session, conversation, task, `callId`) as input; a long-lived layer never captures it.
- `layer` is the normal way to get a service; variants are `layerWith(options)`, `layerConfig`, `layerMemory`, `layerTest`. It leaves in its requirements only what the consumer must choose (`SqlClient`, credentials).
- Compose layers at the edges (host, example, tests). The kernel never provides infrastructure on its own.
- A scoped resource registers its finalizer in its layer; closing is the end of the `Scope`.

### Errors

- Errors are tagged classes: `Schema.TaggedError` when they may be persisted or cross RPC (all of `core` and `cloudflare`), `Data.TaggedError` for in-process errors.
- The failures of one boundary are one error with a `reason` union, handled with `Effect.catchReason`. Models: `StorageFailure`, `DeltaError` (`packages/delta/src/internal/errors.ts`).
- Map external failures once, at the boundary. Never match on an error message deep in the code.
- Defects only for broken setup or violated invariants. A failure the caller or the Session must react to, such as storage, stays typed.
- Lookups return `Option`. Ids are branded with `Schema.brand`. Secrets are `Redacted`.

### Telemetry

- Every public service method is an `Effect.fn("<Service>.<method>")`; internal helpers use `Effect.fnUntraced`.
- Span attributes carry identity (`durable.session.id`, `durable.tool.call_id`), never prompts, model output or tool payloads.
- The kernel never chooses an exporter.

## Tests

- Use `@effect/vitest` (`it.effect`, `assert`, `layer`). Assert expected failures with `Effect.flip`.
- Test the real implementation through its public interface. No module mocking: replace a dependency with another layer of the same service.
- Don't duplicate the logic under test; assert on observable results.
- Deterministic: `TestClock` for time, no real sleeps, every wait bounded.
- A contract shared by several backends is one suite run against each.

## Documentation

Public exports have JSDoc: one paragraph on what the export does and when to use it, then `@category` and `@since` (see `packages/delta/src/Delta.ts`). `internal/` modules, tests and examples need none.

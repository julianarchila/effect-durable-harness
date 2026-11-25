# AGENTS.md

effect-durable-harness is an Effect-native and Alchemy-native framework for durable agents. The kernel holds Sessions over single-owner storage, transcripts of immutable entries with atomic commits, durable tasks with a checkpoint per phase, tool calls whose intent is persisted before they run, idempotent submissions, tree ownership with bottom-up abort, typed documents and a live view. Hosts run it on Durable Objects.

## Principles

- **Effect-native and Alchemy-native.** No Promise↔Effect bridges in the public API. Tools are `Effect`s and their requirements bubble up to the agent's `Layer`.
- **Host-agnostic kernel.** `@effect-durable-harness/core` imports nothing from Cloudflare, Alchemy or any runtime.
- **External systems belong to the consumer.** The library decides when and how many times a tool runs, and gives it a stable `callId` for its own idempotency.
- **Mechanisms in the kernel, policy in extensions.** The kernel holds what needs its atomicity, ordering or recovery. Approval rules, memory, channels, schedules and skills are extensions, written against the same `Extension` API a consumer uses.
- **Decisions are recorded.** A change to the kernel's semantics goes in `DESIGN.md` with its reason. If a change needs a decision nobody has made, ask the user.

## Structure

- `packages/durable` (`@effect-durable-harness/core`): the kernel. Its `AGENTS.md` has the concurrency rules.
- `packages/delta`: immutable JSON revisions and op batches, used by documents.
- `packages/host`: the engine-neutral Durable Object host and the browser client. Tested over `TestEngine`.
- `packages/cloudflare`: the host on Cloudflare. Its workerd suite runs in the gates.
- `packages/rivet` (experimental): the host on Rivet. `test:rivet` runs on a local Docker swarm, outside the gates.
- `packages/opencode`: the `Models` service on `@opencode/ai`.
- `examples/*`: Cloudflare and Rivet apps. Their tests run in the gates with a fake model.
- `tools/oxlint`: lint rules.
- `.repos/`: read-only copies of upstream dependencies, for reference. Don't edit or import from them.

## Writing code

- Read `CODING_STANDARDS.md` first, then follow the nearby code.
- For an Effect API you don't know well, search `.repos/effect/LLMS.md` and `.repos/effect/ai-docs/src`, and read the source.
- Iterate with the narrowest check (`bun run --cwd packages/<name> test`, `bunx vitest run <file>`), then run the four gates. Report any check you could not run.

## Commands

- `bun install`
- Gates: `bun run typecheck`, `bun run test`, `bun run lint`, `bun run fmt:check` (`bun run fmt` fixes formatting).
- `bun run build` builds the packages; `bun run pack:local` packs them into `.packs/`.
- `bun run dev` / `deploy` / `destroy`: Alchemy in `examples/cloudflare-chat`.

Versions live in the root `package.json` `catalog`. Changes to a dependency are `bun patch`es in `patches/`, described in `patches/UPSTREAM.md`. To verify a change on Cloudflare, use the `verify-on-cloudflare` skill.

## Secrets

Secrets live in Doppler. Never ask for, paste or print one; run anything that needs a key under `doppler run -- <command>`. Live tests are skipped when their key is missing.

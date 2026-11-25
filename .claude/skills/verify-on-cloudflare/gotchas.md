# Gotchas

Each one was observed on Cloudflare. When a deployed result differs from local, check here first.

## Platform

- **A commit is durable only after the output gate flushes.** Locally a committed `storage.transaction` survives an immediate `state.abort()`. Deployed it does not: the abort drops the commit, alarm included (still atomic: both or neither). `storage.sync()` before the abort keeps it. The output gate also holds the RPC response and outgoing `fetch`, so nothing outside saw the lost commit.
- **Implicit writes are batched, not durable.** SQL or `setAlarm` outside an explicit transaction, followed by `abort()` in the same event, are dropped locally too. Write anything that must survive a restart inside a transaction.
- **Inspecting a Durable Object wakes it.** Every RPC is an incoming request: it resets idle eviction and runs the constructor if the object was gone. To measure eviction or lifetime, start the work, touch nothing, and inspect once at the end.
- **Time stands still between I/O.** Inside a Worker, `Clock`/`Date` only advance across I/O, so timestamps around pure CPU work look equal. Measure across awaits.
- **An alarm cut at 15 minutes is retried from the start** (`isRetry: true`) every 15 minutes, with `retryCount` stuck at 1. Commit progress and re-arm a heartbeat, or a long single operation loops forever.
- **Work outside an event dies about 70 s after the last incoming event if its only pending I/O is a service binding call or a timer**, even a streaming one, and even under `waitUntil`; the lifecycle docs say both prevent eviction. An outgoing `fetch` to the internet does keep the object alive. So a Worker behind a service binding is not a faithful stand-in for an external API when the claim is about eviction. An alarm invocation counts as an event: an alarm that re-arms itself every 30 s keeps detached work alive indefinitely, with no 15 min cut.
- **What keeps an object alive depends on the compatibility date.** Before `2026-10-01` (Alchemy's default is `2026-08-31`), only an outbound `fetch` to the internet, TCP sockets and outbound WebSockets hold it; from `2026-10-01`, also service bindings, `waitUntil` and timers (each up to 15 min, billed). Check the date before trusting an eviction result.
- **One alarm per object.** Alchemy durable callbacks (`Alchemy.makeCallback`, `scheduleEvent`) reconcile the native alarm from their own tables and delete it when their queue is empty. Code that owns the alarm itself must not register them. The object's own `alarm()` also runs on every Alchemy wake.

## Tooling

- **`cf` is not on PATH** in agent shells: `export PATH="$HOME/.bun/bin:$PATH"`.
- **A fresh worktree has no `node_modules`.** `bunx alchemy` then fails with `could not load the required peer dependency "@effect/platform-bun"`. Run `bun install` at the repo root first.
- **`date` on this machine is uutils coreutils**: `date +%s%3N` ignores the width and prints nanoseconds. Use `$(($(date +%s%N) / 1000000))` for epoch milliseconds.
- **`cf` picks its profile from the working directory** (`cf auth list` shows the bindings). `CLOUDFLARE_API_TOKEN` in the environment overrides every profile.
- **Fresh deploys are not routable at once.** For up to a minute, requests can get 404, `error code: 1042`, or an RPC error `The RPC receiver does not implement the method "<name>"`. None of them reached your code: retry before you call it a failure. The M0 spike's `Api` returns the RPC error as status 200 with `rpcFailed`, so `curl --retry` does not retry it: warm up until the body has no `rpcFailed`.
- **Observability can miss an invocation.** One long `jsrpc` invocation never appeared in `cf o11y` while its committed rows proved it ran. Do not read a missing event as "it did not run".
- **`alchemy/Test` under Vitest needs `sequence: { hooks: "list" }`**, and `Test.make` called in the same tick as your `afterAll(destroy(Stack))` (in the test file, or in a `setup()` the file calls). Otherwise the sidecar closes first and `destroy` hangs until the hook timeout, after the tests already passed.
- **`NODE_TLS_REJECT_UNAUTHORIZED` warnings** during local `alchemy/Test` runs come from Alchemy's dev sidecar. Harmless.
- **Observability is a few seconds behind**, and a `cf o11y` query without `parameters` returns a bare 400.
- **Spans from work that outlives its call are not exported.** Spans from a fiber forked in an RPC stop appearing once the RPC returns; spans inside `alarm()` arrive when the alarm invocation ends. Use committed rows for progress, spans for what happened inside one invocation.
- **The object listing lags.** `cf durable-objects namespaces objects list` returned nothing 33 minutes after six objects were created. Use your inspection RPC to prove an object exists.
- **No CPU profiler.** Alchemy does not expose workerd's inspector locally, and there is none deployed. Use `$workers.cpuTimeMs` per invocation and spans.
- **`bunx alchemy` at the repo root** downloads the latest npm version instead of the pinned one. Run it from a package that depends on `alchemy`.
- **Alchemy state is local.** `.alchemy/` next to `alchemy.run.ts` maps a stage to its resources. Destroy from the checkout that deployed; a different worktree sees nothing to destroy.

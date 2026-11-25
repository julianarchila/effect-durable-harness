---
name: verify-on-cloudflare
description: Verify your own change on Cloudflare with Alchemy and the cf CLI — run it on local workerd or a deployed scratch stage, read logs and traces, inspect Durable Objects, then destroy. Use when you change code that runs in a Worker or Durable Object, or need platform behaviour local tests cannot show (alarms, eviction, limits, durability).
---

# Verify on Cloudflare

You verify your own work: run it where it will run, look at what it did, and leave the account as you found it. The account is the user's **personal** Cloudflare account. It also hosts other production Workers and Alchemy's state store (`alchemy-state-store`); a **scratch stage** is the only thing you create or destroy.

Commands, query recipes and output shapes: [commands.md](commands.md). Platform surprises seen so far: [gotchas.md](gotchas.md) — read it before you trust a deployed result that differs from local.

## 1. Preflight

```sh
export PATH="$HOME/.bun/bin:$PATH"          # cf lives here; agent shells often lack it
bunx alchemy profile show --no-input        # run from a package that has alchemy
cf auth whoami                              # run from inside the repo
```

Done when `alchemy profile show` lists Cloudflare as connected and `cf auth whoami` lists exactly one account, "Personal Account", with `authSource` pointing at `config/personal.json`. If `cf` lists other accounts, you are outside the repo or `CLOUDFLARE_API_TOKEN` is set. If either is not authenticated, stop and ask the user: both logins open a browser.

## 2. Pick the target

Run on **local workerd** first: same code, same Alchemy bridge, seconds instead of minutes, nothing to clean up. Go to a **deployed** scratch stage only for what local cannot show: durability before the output gate, the 15 min alarm limit, idle eviction, the deploy grace period, real latency, Workers Logs and Traces.

Done when you can name, for each claim you want to check, whether local or deployed answers it.

## 3. Run it

- **Tests** (preferred): `alchemy/Test` deploys the stack per test file. The same suite runs locally (`ALCHEMY_TEST_DEV=1`) or deployed (`ALCHEMY_TEST_DEV=0 ALCHEMY_TEST_STAGE=<stage>`). Smallest example: `packages/cloudflare/test/workerd/` (`bun run --cwd packages/cloudflare test:workerd`); a fuller harness with polling and recorded results: `git show m0-host-spike:spikes/host/test/harness.ts` (the M0 spike, kept on that tag).
- **By hand**: `bunx alchemy deploy --stage <stage> --no-input --yes` from the package with `alchemy.run.ts`, then drive the printed URL with `curl`.

Name the stage `<milestone>-<topic>` (for example `m3-wakeups`); Alchemy names each Worker `<stack>-<resource>-<stage>-<hash>`, lowercased, so the stage name finds everything you deployed. Done when the Worker answers at its URL.

For up to a minute after a deploy, calls can fail with 404, `error code: 1042` or `The RPC receiver does not implement the method` without reaching your code, even after another call succeeded. Retrying is safe for reads. Before a sequence that is timing-sensitive or cannot be repeated (start work, then crash it), warm up the exact object and method you will use, check that each step's response came from your code, and discard the run if any step got one of those errors.

## 4. Observe

Look at evidence, not at the HTTP status alone:

- **Logs**: `bunx alchemy logs --stage <stage> --since 10m`, or `--tail` while you drive it. Request lines show status, CPU and wall time; with traces on, span and storage lines are mixed in. `--limit` keeps the oldest entries of the window, so narrow `--since` to see the latest. For which invocation did what (an `alarm` vs a `fetch`, on which object), query by event type (recipe in [commands.md](commands.md)).
- **Traces and spans**: `cf o11y telemetry query` (recipes in [commands.md](commands.md)). Effect spans show up only if the Worker provides `Cloudflare.Telemetry()`.
- **Durable Object state**: no Cloudflare API reads a DO's SQLite. Expose a read-only inspection RPC (`InspectJob` in `git show m0-host-spike:spikes/host/src/SpikeObject.ts` is the pattern) and call it. Every call wakes the object and runs its constructor if it was gone, so for claims about eviction, restarts or alarms, wait long enough and inspect **once**; polling can itself be what woke it.
- **Timing**: `$workers.wallTimeMs` / `cpuTimeMs` per invocation, or p99 by event type with a `calculations` query.

Done when every claim from step 2 has a log line, span, metric or inspected row behind it, saved where your write-up can cite it.

## 5. Destroy

```sh
bunx alchemy destroy --stage <stage> --no-input --yes
cf workers scripts search --name <stage>
cf durable-objects namespaces list | jq -r '.[].name' | grep <stage>
```

Done when both searches come back empty. Alchemy keeps stack state in `.alchemy/` next to `alchemy.run.ts`: destroy from the same checkout that deployed, or the resources become orphans you must delete by name.

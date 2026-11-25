# Commands

Verified on 2026-10-16 with `alchemy@2.0.0-beta.80` and `cf@1.0.0-beta.14`. `cf` help is long: find a command with `cf cli search "<task, no names or ids>"`, then `<command> --help`; `cf schema <command without cf>` shows the request body.

Run `bunx alchemy …` from a package that depends on `alchemy` (`packages/cloudflare`, `examples/cloudflare-chat`). At the repo root `bunx` downloads whatever version npm has.

## Alchemy

| Task | Command |
| --- | --- |
| Deploy a stage | `bunx alchemy deploy --stage <stage> --no-input --yes` (prints the stack outputs, e.g. `url`) |
| Destroy it | `bunx alchemy destroy --stage <stage> --no-input --yes` |
| Plan only | `bunx alchemy plan --stage <stage> --no-input` (reads local state; it does not prove access) |
| Recent logs | `bunx alchemy logs --stage <stage> --since 10m --limit 100 --no-input` |
| Live logs | `bunx alchemy logs --stage <stage> --tail --no-input` |
| One resource | add `--resource Api` (logical id) |

Without `--yes`, a non-interactive deploy or destroy changes nothing and exits.

## Tests on both targets

```sh
bun run --cwd packages/cloudflare test:workerd                          # local workerd
ALCHEMY_TEST_STAGE=<stage> bun run --cwd packages/cloudflare test:deployed  # deployed, destroyed after each file
NO_DESTROY=1 …                                                          # keep the deployment to inspect it
```

## Workers Observability (`cf o11y telemetry query`)

Every query needs `queryId` (any string), `timeframe` in epoch milliseconds, and a `parameters` object, even an empty one; without `parameters` the API answers a bare 400. Shell helper:

```sh
NOW=$(python3 -c 'import time; print(int(time.time() * 1000))'); FROM=$((NOW - 15 * 60 * 1000))
q() { cf o11y telemetry query --body "{\"queryId\":\"adhoc\",\"timeframe\":{\"from\":$FROM,\"to\":$NOW},$1}"; }
script='{"key":"$workers.scriptName","operation":"includes","type":"string","value":"<stage>"}'
```

| Want | Query | Read with |
| --- | --- | --- |
| Log lines | `q "\"view\":\"events\",\"limit\":50,\"parameters\":{\"filters\":[$script]}"` | `jq '.events.events[] \| {timestamp, message: .source.message, outcome: ."$workers".outcome}'` |
| Search text | add `"needle":{"value":"spike.","isRegex":false}` to `parameters` | same |
| One kind of invocation | add `{"key":"$workers.eventType","operation":"eq","type":"string","value":"alarm"}` (or `fetch`, `jsrpc`) to `filters` | `jq '.events.events[] \| {timestamp, outcome: ."$workers".outcome, wall: ."$workers".wallTimeMs, class: ."$workers".entrypoint, object: ."$workers".durableObjectId}'` |
| Traces | `"view":"traces"` | `jq '.traces[] \| {traceId, rootTransactionName, traceDurationMs, spans}'` |
| One trace's spans | filter `{"key":"$metadata.traceId","operation":"eq","type":"string","value":"<id>"}` | `jq '.events.events[] \| {name: ."$metadata".message, span: ."$metadata".spanId, parent: ."$metadata".parentSpanId}'` |
| Timing by event type | `"view":"calculations"` with `"calculations":[{"operator":"count"},{"operator":"p99","key":"$workers.wallTimeMs","keyType":"number"},{"operator":"p99","key":"$workers.cpuTimeMs","keyType":"number"}],"groupBys":[{"type":"string","value":"$workers.eventType"}]` | `jq '.calculations[] \| {calculation, aggregates: [.aggregates[] \| {groups: [.groups[].value], value}]}'` |

Effect spans (`Effect.withSpan`, `Effect.fn("name")`) land in dataset `otel` with `source.cloudflare.durable_object.id`, `source.cloudflare.entrypoint` and `source.cloudflare.script_version.id`; the span name is `source.name`. A span with `gen_ai.*` attributes (`durable.generation.request`, its outcome) is stored twice, also in dataset `otel-gen-ai`: to count spans, add `{"key":"$metadata.dataset","operation":"eq","type":"string","value":"otel"}` to `filters` or count unique `$metadata.spanId`. A `calculations` query returns only the top 10 groups unless `parameters` has `"limit"`, and its percentile operators are `median`, `p90`, `p99` and the like (no `p50`). They need `Cloudflare.Telemetry()` provided to the Worker; Alchemy's default compatibility date (`2026-08-31`) already satisfies its `2026-08-25` minimum.

## Durable Objects

| Task | Command |
| --- | --- |
| Namespaces | `cf durable-objects namespaces list \| jq -c '.[] \| {id, name, class, use_sqlite}'` (names are `<script>_<Class>`) |
| Objects in one | `cf durable-objects namespaces objects list --id <namespace-id>` (no `--limit` below the API minimum; can lag behind new objects) |
| Read its storage | No API. Call an inspection RPC on the object. |

## Workers

| Task | Command |
| --- | --- |
| Find scripts | `cf workers scripts search --name <fragment> \| jq -r '.[].script_name'` |
| Who am I | `cf auth whoami` (per-directory profile; see `cf auth list`) |

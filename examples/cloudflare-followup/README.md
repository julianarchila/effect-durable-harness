# Follow-up: deferred work and external delivery

A small assistant answers inbound messages and delivers each reply to an external webhook. When a contact goes quiet, it sends one follow-up hours later.

## What it shows

| Pattern            | Where                                                            | What effect-durable-harness provides                                                                                                                                                                                                                               |
| ------------------ | ---------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Idempotent inbound | `Contact.receive` (`src/server/Contact.ts`)                      | One commit checks `submissionByRequest`, admits the input under `inbound:<providerMessageId>`, records the contact's newest message and creates both tasks. A resent webhook gets `duplicate` and creates nothing.                                                 |
| Reporter           | `DeliverReply` (`src/server/agent/tasks/deliver-reply.ts`)       | It submits the input, awaits the answer, claims the answer (first writer wins), and sends it under `followup:<to>:answer:<entryId>`. If the webhook refuses, it sleeps a backoff with `sleepUntil` and retries with the same key.                                  |
| Durable sleep      | `FollowUp` (`src/server/agent/tasks/follow-up.ts`)               | `sleepUntil(wakeAt)` ends the phase holding no invocation. The object's alarm sits at the deadline, and the object can be evicted meanwhile. On waking, a newer inbound supersedes the follow-up. Otherwise the commit that completes it creates a `DeliverReply`. |
| External system    | `Sink` (`src/sink/Sink.ts`), `Webhook` (`src/server/Webhook.ts`) | Nothing. The webhook deduplicates by key, and effect-durable-harness only decides when and how many times to call it.                                                                                                                                              |

The send comes before the commit that records it, so a crash between the two sends again under the same key. The webhook's deduplication is what makes the delivery happen once. A webhook that does not answer within 30 s counts as a refusal.

A delivery's `kind` is the kind of the task that claimed the answer. When an inbound message is steered into the run a follow-up started, the follow-up's task may claim the shared answer, and that answer goes out as `follow-up`.

## Layout

- `src/server/Api.ts`: the inbound Worker. It routes each message to its contact's `Contact` object, one Durable Object per contact address.
  - `POST /inbound` with `{ providerMessageId, from, text }` answers `202 { status: "accepted" | "duplicate" }`.
  - `GET /contacts/:address` returns the object's transcript, live tasks, documents and alarm. The call wakes the object.
  - `POST /contacts/:address/crash` resets the object, but only with `FOLLOWUP_ALLOW_CRASH=true`.
- `src/server/Contact.ts`: the object (`Host.agent` and `agent.start`) and its methods.
- `src/server/agent/`: the extension, its two documents and its two tasks.
- `src/sink/Sink.ts`: the external webhook, a second Worker with a `Ledger` object per recipient. It is a tagged Worker (`Sink` plus a default `Sink.make(...)`), so the inbound Worker binds it without bundling its implementation.
  - `POST /deliveries` with an `Idempotency-Key` header returns `201` for a new key, `200` for a known key, or `503` for an attempt it refuses on purpose.
  - `GET /deliveries/:to` returns the recipient's attempts and deliveries.
  - `PUT /faults/:to` with `{ failFirst, delayMs }` refuses the next attempts or answers them late.

## Configuration

These are read when the Worker starts, and Alchemy binds them from the deploying shell. A binding stays on the Worker until you deploy it with another value. Leaving a variable out of a redeploy does not remove it. A stage once deployed with `FOLLOWUP_ALLOW_CRASH=true` keeps the crash route until you deploy it with `FOLLOWUP_ALLOW_CRASH=false`.

| Variable                       | Default             | Meaning                                                         |
| ------------------------------ | ------------------- | --------------------------------------------------------------- |
| `FOLLOW_UP_AFTER_MS`           | `14400000` (4 h)    | Silence before the follow-up                                    |
| `FOLLOWUP_KEEP_ALIVE_MS`       | host default (30 s) | Alarm keep-alive while there is active work                     |
| `FOLLOWUP_MODEL`               | Gemini              | `faux` for the fake model the tests use                         |
| `GOOGLE_GENERATIVE_AI_API_KEY` | none                | From Doppler. Without it, the fake model is used                |
| `FOLLOWUP_ALLOW_CRASH`         | `false`             | Enables `POST /contacts/:address/crash` (tests, scratch stages) |

## Run

From this directory, under Doppler for the Gemini key:

```sh
doppler run -- bun run dev                                    # local workerd
doppler run -- bunx alchemy deploy --stage <stage> --no-input --yes
bunx alchemy destroy --stage <stage> --no-input --yes
```

Try it:

```sh
curl -X POST "$API/inbound" -H 'content-type: application/json' \
  -d '{"providerMessageId":"m1","from":"ada","text":"Do you deliver to Bogota?"}'
curl "$SINK/deliveries/ada"
```

## Tests

`bun run test` deploys both Workers on local workerd with the fake model, an 8 s follow-up and a 1 s keep-alive. It runs six flows:

1. One inbound message produces exactly one delivery.
2. The same inbound message posted twice is one submission and one delivery.
3. The webhook refuses the first two attempts. The task retries with the same key until the webhook records one delivery.
4. The object is killed while the webhook holds the response. After the restart, the task resends with the same key, and the webhook keeps one delivery.
5. The follow-up sleeps with the alarm at its deadline, past the keep-alive, and survives an eviction. The alarm wakes the object and the follow-up is delivered.
6. A newer inbound message before the deadline supersedes the pending follow-up.

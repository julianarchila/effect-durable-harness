# Support: an agent over the shop's own infrastructure

A support agent for a small shop, one `SupportSession` Durable Object per ticket. Its tools and its task reach the shop's systems through ordinary Effect services, each built on an Alchemy binding. It is the reference for how an object is composed with `Host.agent` and `agent.start`.

## What it shows

| Pattern                        | Where                                 | How                                                                                                                                                                                                                                                |
| ------------------------------ | ------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Services on Alchemy bindings   | `src/server/services/`                | `Orders` and `Staff` on D1, `Policies` and `CustomerAuth` on KV, `Payments` on a service binding. Each binds its resource in its Layer, once per isolate, and calls it with `Host.withRuntimeContext`.                                             |
| Handlers that request services | `src/server/agent/support.live.ts`    | `RefundOrderLive` yields `Orders`, `Payments` and `Policies`; the requirements bubble up to `SupportSessionLive`, which provides them with `Layer.provide`.                                                                                        |
| An external effect, once       | `refund_order`                        | `replay: "unsafe"`, and the gateway refunds once per `call.key("refund")`.                                                                                                                                                                         |
| A durable side task            | `escalate` and `support.notify-staff` | One commit marks the ticket escalated and creates the task; the task pages the on-call person in D1 under `run.key("page")`, retried across restarts.                                                                                              |
| The object as Alchemy's class  | `src/server/SupportSession.ts`        | The class is typed by the hand-written `SupportMethods`. `Host.agent` runs in the isolate phase; in the instance phase, `yield* agent.start`, then `return host.methods<SupportMethods>({ … })` with the methods inline; `alarm` stays the host's. |
| An isolate service in a method | `handOff`                             | `Staff` is yielded beside `Host.agent`, and `handOff` closes over it.                                                                                                                                                                              |
| Tagged errors across RPC       | `StaffNotFound`, `src/server/Api.ts`  | Listed in the class's `errors`, so the API Worker catches it by tag.                                                                                                                                                                               |
| Socket authorization from KV   | `Customers` in `SupportSession.ts`    | `accept` checks `?token=` with `CustomerAuth`; prompts are attributed with `input`, and `host.socket.broadcast` tells the customer about a hand-off.                                                                                               |

## Layout

- `src/server/Api.ts`: the API Worker, which hosts the objects.
  - `POST /customers/:id/tokens` with `{ name }`: a socket token (KV).
  - `PUT /policies/refund` with `{ windowDays, maxAgentCents }`: the refund policy (KV), read on every refund.
  - `POST /tickets` with `{ ticketId, customerId, message }`: opens the ticket; idempotent by `ticketId`.
  - `GET /tickets/:id`: the ticket document, transcript, live tasks and escalations.
  - `GET /tickets/:id/socket?token=`: the session socket (`SessionProtocol`).
  - `POST /tickets/:id/handoff` with `{ email }`: a person takes the ticket; `404` for unknown staff.
  - `POST /tickets/:id/crash`: resets the object, only with `SUPPORT_ALLOW_CRASH=true`.
- `src/server/SupportSession.ts`: the object, its socket and its methods.
- `src/server/agent/`: the contract (`support.ts`) and the handlers (`support.live.ts`).
- `src/server/services/`: the shop's services.
- `src/payments/PaymentGateway.ts`: the external payment API, a second Worker with its own D1 database.
  - `POST /refunds` with an `Idempotency-Key` header: `201` for a new key, `200` with the first refund for a known one.
  - `GET /refunds/:chargeId`: the charge's refunds.
- `migrations/shop`, `migrations/payments`: the two databases, with sample orders and staff.

The fake model the tests use (`src/server/models.ts`) answers `where is order <id>`, `refund <id> <cents>` and any message with `human` by calling the matching tool.

## Configuration

| Variable                       | Default             | Meaning                                          |
| ------------------------------ | ------------------- | ------------------------------------------------ |
| `SUPPORT_MODEL`                | Gemini              | `faux` for the fake model the tests use          |
| `SUPPORT_KEEP_ALIVE_MS`        | host default (30 s) | Alarm keep-alive while there is active work      |
| `SUPPORT_ALLOW_CRASH`          | `false`             | Enables the crash route                          |
| `GOOGLE_GENERATIVE_AI_API_KEY` | none                | From Doppler. Without it, the fake model is used |

## Run

- `bun run test`: the workerd tests, with the fake model, in the gates.
- `doppler run -- bun run dev`: locally with Gemini.

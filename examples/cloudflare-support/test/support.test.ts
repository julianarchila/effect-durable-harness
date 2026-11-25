import { ROOT_CONVERSATION_ID } from "@effect-durable-harness/core/Ids";
import * as Alchemy from "alchemy";
import * as Cloudflare from "@alchemy.run/cloudflare";
import * as Test from "@alchemy.run/test/Vitest";
import * as Effect from "effect/Effect";
import { expect } from "vitest";
import type { TicketStatus } from "../src/protocol.ts";
import {
  connect,
  crash,
  eventually,
  handOff,
  issueToken,
  openTicket,
  refunds,
  setRefundPolicy,
  status,
  ticketId,
} from "./support.ts";

/**
 * The support example on local workerd with the fake model (`src/server/models.ts`): an agent whose tools and task
 * use the shop's D1 database, KV namespaces and payment gateway through services, hosted with `Host.agent` and
 * `agent.start`. Driven over HTTP and the session socket.
 */
process.env["SUPPORT_MODEL"] = "faux";
process.env["SUPPORT_ALLOW_CRASH"] = "true";
process.env["SUPPORT_KEEP_ALIVE_MS"] = "1000";
const { default: Stack } = await import("./alchemy.run.ts");

const { test, beforeAll, afterAll, deploy, destroy } = Test.make({
  providers: Cloudflare.providers(),
  state: Alchemy.localState(),
  dev: true,
});

const stack = beforeAll(deploy(Stack), { timeout: 300_000 });
afterAll.skipIf(!!process.env.NO_DESTROY)(destroy(Stack), { timeout: 300_000 });

/** The run settled: an assistant answer after the last user message, and no live task. */
const answered = (current: TicketStatus) =>
  current.transcript.at(-1)?.startsWith("assistant: ") === true && current.tasks.length === 0;

test(
  "an order question is answered from the shop's database",
  Effect.gen(function* () {
    const { api } = yield* stack;
    const id = ticketId("lookup");
    yield* openTicket(api, {
      ticketId: id,
      customerId: "c-ada",
      message: "where is order o-1001?",
    });
    const current = yield* eventually(status(api, id), answered);
    expect(current.ticket).toMatchObject({ ticketId: id, customerId: "c-ada", status: "open" });
    expect(current.transcript).toEqual([
      "user: where is order o-1001?",
      "assistant: ",
      expect.stringMatching(
        /^assistant: lookup_order: o-1001: Mechanical keyboard, shipped, total 12900 cents/u,
      ),
    ]);
  }),
);

test(
  "another customer's order is not found",
  Effect.gen(function* () {
    const { api } = yield* stack;
    const id = ticketId("other");
    yield* openTicket(api, {
      ticketId: id,
      customerId: "c-grace",
      message: "where is order o-1001",
    });
    const current = yield* eventually(status(api, id), answered);
    expect(current.transcript.at(-1)).toBe("assistant: lookup_order: no order o-1001");
  }),
);

test(
  "a refund within policy goes through the gateway once, however often the ticket is opened",
  Effect.gen(function* () {
    const { api, gateway } = yield* stack;
    const id = ticketId("refund");
    const input = { ticketId: id, customerId: "c-ada", message: "refund o-1001 2000" };
    yield* Effect.all([openTicket(api, input), openTicket(api, input)]);
    const current = yield* eventually(status(api, id), answered);
    expect(current.transcript.filter((line) => line.startsWith("user: "))).toEqual([
      "user: refund o-1001 2000",
    ]);
    expect(current.transcript.at(-1)).toMatch(
      /^assistant: refund_order: refunded 2000 cents \(re_\d+\)$/u,
    );
    const made = yield* refunds(gateway, "ch_1001");
    expect(made).toHaveLength(1);
    expect(made[0]?.amountCents).toBe(2000);
    // And the shop's database recorded it.
    const after = ticketId("refund-after");
    yield* openTicket(api, {
      ticketId: after,
      customerId: "c-ada",
      message: "where is order o-1001",
    });
    const looked = yield* eventually(status(api, after), answered);
    expect(looked.transcript.at(-1)).toContain("refunded 2000");
  }),
);

test(
  "two tickets are two refunds and two escalations, though their calls share task IDs",
  Effect.gen(function* () {
    const { api, gateway } = yield* stack;
    const before = (yield* refunds(gateway, "ch_1001")).length;
    // Fresh objects mint the same task IDs, so `call.key` and `run.key` repeat across tickets (found on a deployed
    // stage, 2026-11-08): the keys sent to the gateway and the shop's database carry the ticket ID.
    const first = ticketId("twice-a");
    const second = ticketId("twice-b");
    yield* Effect.all([
      openTicket(api, { ticketId: first, customerId: "c-ada", message: "refund o-1001 500" }),
      openTicket(api, { ticketId: second, customerId: "c-ada", message: "refund o-1001 500" }),
    ]);
    yield* eventually(status(api, first), answered);
    yield* eventually(status(api, second), answered);
    expect((yield* refunds(gateway, "ch_1001")).length).toBe(before + 2);

    const third = ticketId("escalate-a");
    const fourth = ticketId("escalate-b");
    yield* Effect.all([
      openTicket(api, { ticketId: third, customerId: "c-ada", message: "a human please" }),
      openTicket(api, { ticketId: fourth, customerId: "c-ada", message: "a human please" }),
    ]);
    const escalated = (current: TicketStatus) =>
      current.escalations.length > 0 && answered(current);
    expect((yield* eventually(status(api, third), escalated)).escalations).toHaveLength(1);
    expect((yield* eventually(status(api, fourth), escalated)).escalations).toHaveLength(1);
  }),
);

test(
  "a refund past the policy's window is refused without calling the gateway",
  Effect.gen(function* () {
    const { api, gateway } = yield* stack;
    const id = ticketId("late");
    yield* openTicket(api, { ticketId: id, customerId: "c-ada", message: "refund o-1002 1000" });
    const current = yield* eventually(status(api, id), answered);
    expect(current.transcript.at(-1)).toBe(
      "assistant: refund_order: refused: o-1002 is past the 30-day window",
    );
    expect(yield* refunds(gateway, "ch_1002")).toEqual([]);
  }),
);

test(
  "an escalation pages the on-call person, and a hand-off reaches the customer's socket",
  Effect.gen(function* () {
    const { api } = yield* stack;
    const id = ticketId("escalate");
    const token = yield* issueToken(api, "c-ada", "Ada");
    yield* openTicket(api, { ticketId: id, customerId: "c-ada", message: "hello" });
    yield* eventually(status(api, id), answered);
    const socket = yield* connect(api, id, token);
    socket.send({ type: "watch", requestId: "w1", conversationId: ROOT_CONVERSATION_ID });

    // A prompt from the socket is attributed to the customer (`input`), and escalates.
    socket.send({
      type: "submit",
      requestId: "s1",
      conversationId: ROOT_CONVERSATION_ID,
      content: "I want a human please",
      whenBusy: "followUp",
    });
    const escalated = yield* eventually(
      status(api, id),
      (current) => current.escalations.length > 0 && answered(current),
    );
    expect(escalated.ticket?.status).toBe("escalated");
    expect(escalated.transcript).toContain("user: [Ada] I want a human please");
    expect(escalated.escalations).toEqual([
      { reason: "I want a human please", paged: "lin@shop.example" },
    ]);

    // Unknown staff: the method's tagged error is revived in the API Worker.
    expect(yield* handOff(api, id, "nobody@shop.example")).toEqual({
      status: 404,
      body: { error: "No staff nobody@shop.example" },
    });
    expect(yield* handOff(api, id, "lin@shop.example")).toEqual({
      status: 200,
      body: { assignee: "Lin" },
    });
    yield* eventually(
      Effect.sync(() => socket.events),
      (events) =>
        events.some(
          (event) =>
            event.type === "event" &&
            JSON.stringify(event.data) === JSON.stringify({ type: "handoff", name: "Lin" }),
        ),
    );
    const current = yield* status(api, id);
    expect(current.ticket).toMatchObject({ status: "human", assignee: "Lin" });
    socket.close();
  }),
);

test(
  "the socket refuses a token it does not know",
  Effect.gen(function* () {
    const { api } = yield* stack;
    const id = ticketId("refused");
    yield* openTicket(api, { ticketId: id, customerId: "c-ada", message: "hello" });
    const socket = yield* connect(api, id, "tok_unknown");
    const code = yield* eventually(Effect.sync(socket.closed), (closed) => closed !== undefined);
    expect(code).toBe(4403);
  }),
);

test(
  "the refund policy in KV applies to the next refund",
  Effect.gen(function* () {
    const { api, gateway } = yield* stack;
    yield* setRefundPolicy(api, { windowDays: 30, maxAgentCents: 1000 });
    const id = ticketId("policy");
    yield* openTicket(api, { ticketId: id, customerId: "c-grace", message: "refund o-2001 1500" });
    const current = yield* eventually(status(api, id), answered);
    expect(current.transcript.at(-1)).toBe(
      "assistant: refund_order: refused: over the 1000-cent limit",
    );
    expect(yield* refunds(gateway, "ch_2001")).toEqual([]);
  }),
);

test(
  "a ticket survives a reset of its object",
  Effect.gen(function* () {
    const { api } = yield* stack;
    const id = ticketId("crash");
    yield* openTicket(api, { ticketId: id, customerId: "c-ada", message: "hello there" });
    const before = yield* eventually(status(api, id), answered);
    yield* crash(api, id);
    const after = yield* eventually(status(api, id), answered);
    expect(after.transcript).toEqual(before.transcript);
    expect(after.ticket).toEqual(before.ticket);
  }),
);

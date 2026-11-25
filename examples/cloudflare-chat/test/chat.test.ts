import * as Alchemy from "alchemy";
import * as Cloudflare from "@alchemy.run/cloudflare";
import * as Test from "@alchemy.run/test/Vitest";
import * as Effect from "effect/Effect";
import { expect } from "vitest";
import { ROOT_CONVERSATION_ID } from "../src/protocol.ts";
import {
  connect,
  conversationOf,
  crash,
  createSession,
  eventually,
  requestId,
  resultOf,
  resultsOf,
  transcript,
  userEntryId,
  watch,
} from "./support.ts";

/**
 * The chat example on local workerd, with the fake model (`src/server/models.ts`): the flows of the
 * multiplayer chat, driven over the session socket (`Host.socket`) as the web app's
 * `SessionClient` does.
 */
process.env["CHAT_MODEL"] = "faux";
process.env["CHAT_KEEP_ALIVE_MS"] = "1000";
const { default: Stack } = await import("./alchemy.run.ts");

const { test, beforeAll, afterAll, deploy, destroy } = Test.make({
  providers: Cloudflare.providers(),
  state: Alchemy.localState(),
  dev: true,
});

const stack = beforeAll(deploy(Stack), { timeout: 300_000 });
afterAll.skipIf(!!process.env.NO_DESTROY)(destroy(Stack), { timeout: 300_000 });

const prompt = (conversationId: number, content: string) => ({
  type: "submit" as const,
  requestId: requestId("prompt"),
  conversationId,
  content,
});

test(
  "a prompt is answered, attributed to its sender, and streamed into the conversation's view",
  Effect.gen(function* () {
    const { url } = yield* stack;
    const session = yield* createSession(url);
    const ada = yield* connect(url, session, "Ada");
    yield* watch(ada, ROOT_CONVERSATION_ID);
    const command = prompt(ROOT_CONVERSATION_ID, "hello");
    ada.send(command);
    const result = yield* resultOf(ada, command.requestId);
    expect(result).toMatchObject({ ok: true, conversationId: ROOT_CONVERSATION_ID });
    const messages = yield* eventually(() => {
      const current = transcript(ada.view(ROOT_CONVERSATION_ID));
      return current.length === 2 ? current : undefined;
    });
    expect(messages[0]).toBe("user: [Ada]\nhello");
    expect(messages[1]?.startsWith("assistant: echo: hello")).toBe(true);
    ada.close();
  }),
);

test(
  "two clients share the session: queued messages are answered in order, and typing is shown to the other",
  Effect.gen(function* () {
    const { url } = yield* stack;
    const session = yield* createSession(url);
    const ada = yield* connect(url, session, "Ada");
    const bob = yield* connect(url, session, "Bob");
    yield* watch(ada, ROOT_CONVERSATION_ID);
    yield* watch(bob, ROOT_CONVERSATION_ID);
    ada.send({
      type: "presence",
      conversationId: ROOT_CONVERSATION_ID,
      presence: { typing: true },
    });
    yield* eventually(() =>
      bob.events.some(
        (event) =>
          event.type === "presence" &&
          event.peers.length === 1 &&
          JSON.stringify(event.peers[0]?.connection) === JSON.stringify({ username: "Ada" }) &&
          JSON.stringify(event.peers[0]?.presence) === JSON.stringify({ typing: true }),
      ),
    );
    ada.send(prompt(ROOT_CONVERSATION_ID, "first"));
    yield* eventually(() => transcript(bob.view(ROOT_CONVERSATION_ID)).length === 1);
    // Ada's turn is still streaming: Bob's message waits in the conversation's durable inbox.
    bob.send(prompt(ROOT_CONVERSATION_ID, "second"));
    const messages = yield* eventually(() => {
      const current = transcript(ada.view(ROOT_CONVERSATION_ID));
      return current.length === 4 ? current : undefined;
    });
    expect(messages[0]).toBe("user: [Ada]\nfirst");
    expect(messages[1]?.startsWith("assistant: echo: first")).toBe(true);
    expect(messages[2]).toBe("user: [Bob]\nsecond");
    expect(messages[3]?.startsWith("assistant: echo: second")).toBe(true);
    ada.close();
    bob.close();
  }),
);

test(
  "a steer sent while a tool runs joins that run: the model reads it before its next answer",
  Effect.gen(function* () {
    const { url } = yield* stack;
    const session = yield* createSession(url);
    const ada = yield* connect(url, session, "Ada");
    yield* watch(ada, ROOT_CONVERSATION_ID);
    ada.send(prompt(ROOT_CONVERSATION_ID, "dice"));
    // `roll_dice` runs for three seconds: its slot in `core.live` says so.
    yield* eventually(() => {
      const live = ada.view(ROOT_CONVERSATION_ID)?.docs["core.live"];
      return JSON.stringify(live ?? {}).includes('"status":"running"');
    });
    ada.send({ ...prompt(ROOT_CONVERSATION_ID, "hurry"), whenBusy: "steer" });
    const messages = yield* eventually(() => {
      const current = transcript(ada.view(ROOT_CONVERSATION_ID));
      return current.at(-1)?.startsWith("assistant: echo: hurry") === true ? current : undefined;
    });
    // The steer is placed after the tool round, in the same run: no answer to the dice in between.
    expect(messages.slice(2)).toHaveLength(3);
    expect(messages[2]?.startsWith("toolResult: Rolled")).toBe(true);
    expect(messages[3]).toBe("user: [Ada]\nhurry");
    ada.close();
  }),
);

test(
  "a fork starts from the chosen entry, and a resent command creates nothing twice",
  Effect.gen(function* () {
    const { url } = yield* stack;
    const session = yield* createSession(url);
    const ada = yield* connect(url, session, "Ada");
    yield* watch(ada, ROOT_CONVERSATION_ID);
    ada.send(prompt(ROOT_CONVERSATION_ID, "one"));
    yield* eventually(() => transcript(ada.view(ROOT_CONVERSATION_ID)).length === 2);
    const submit = prompt(ROOT_CONVERSATION_ID, "two");
    ada.send(submit);
    yield* eventually(() => transcript(ada.view(ROOT_CONVERSATION_ID)).length === 4);
    // The same request again, as a client resends it after a reconnect: the same submission, no new turn.
    ada.send(submit);
    yield* eventually(() => resultsOf(ada, submit.requestId).length === 2);
    const [first, again] = resultsOf(ada, submit.requestId);
    expect(again).toMatchObject({ ok: true });
    expect(again).toEqual(first);
    const entryId = userEntryId(ada.view(ROOT_CONVERSATION_ID), "two");
    expect(entryId).toBeDefined();
    const fork = {
      type: "fork" as const,
      requestId: requestId("fork"),
      conversationId: ROOT_CONVERSATION_ID,
      entryId: entryId ?? 0,
    };
    ada.send(fork);
    const forkId = conversationOf(yield* resultOf(ada, fork.requestId));
    expect(forkId).toBeGreaterThan(ROOT_CONVERSATION_ID);
    ada.send(fork);
    const refork = yield* eventually(() => resultsOf(ada, fork.requestId)[1]);
    expect(refork).toMatchObject({ ok: true, conversationId: forkId });
    // The fork has the history through the chosen entry.
    yield* watch(ada, forkId);
    const inherited = yield* eventually(() => {
      const current = transcript(ada.view(forkId));
      return current.length === 3 ? current : undefined;
    });
    expect(inherited[2]).toBe("user: [Ada]\ntwo");
    expect(
      ada.conversations().filter((conversation) => conversation.parent !== undefined),
    ).toHaveLength(1);
    expect(transcript(ada.view(ROOT_CONVERSATION_ID))).toHaveLength(4);
    ada.close();
  }),
);

test(
  "a scheduled instruction is delivered to the conversation after its deadline",
  Effect.gen(function* () {
    const { url } = yield* stack;
    const session = yield* createSession(url);
    const ada = yield* connect(url, session, "Ada");
    yield* watch(ada, ROOT_CONVERSATION_ID);
    ada.send(prompt(ROOT_CONVERSATION_ID, "schedule 3 check the weather in Lima"));
    const messages = yield* eventually(() => {
      const current = transcript(ada.view(ROOT_CONVERSATION_ID));
      return current.some((line) => line === "assistant: done: check the weather in Lima")
        ? current
        : undefined;
    });
    expect(messages.some((line) => line.startsWith("assistant: schedule: Scheduled."))).toBe(true);
    expect(messages).toContain("user: Scheduled instruction due now: check the weather in Lima");
    ada.close();
  }),
);

test(
  "search_history answers from other conversations through a subagent its call owns",
  Effect.gen(function* () {
    const { url } = yield* stack;
    const session = yield* createSession(url);
    const ada = yield* connect(url, session, "Ada");
    yield* watch(ada, ROOT_CONVERSATION_ID);
    ada.send(prompt(ROOT_CONVERSATION_ID, "postgres rocks"));
    yield* eventually(() => transcript(ada.view(ROOT_CONVERSATION_ID)).length === 2);
    const create = { type: "create" as const, requestId: requestId("create") };
    ada.send(create);
    const other = conversationOf(yield* resultOf(ada, create.requestId));
    yield* watch(ada, other);
    ada.send(prompt(other, "search postgres"));
    const messages = yield* eventually(() => {
      const current = transcript(ada.view(other));
      return current.some((line) => line.startsWith("assistant: search_history:"))
        ? current
        : undefined;
    });
    const answer = messages.find((line) => line.startsWith("assistant: search_history:")) ?? "";
    expect(answer).toContain("postgres rocks");
    // The subagent's conversation was announced, owned by the searching conversation.
    expect(
      ada.conversations().some((conversation) => conversation.owner?.conversationId === other),
    ).toBe(true);
    ada.close();
  }),
);

test(
  "a turn in flight finishes after the object is killed, with no client connected",
  Effect.gen(function* () {
    const { url } = yield* stack;
    const session = yield* createSession(url);
    const ada = yield* connect(url, session, "Ada");
    yield* watch(ada, ROOT_CONVERSATION_ID);
    ada.send(prompt(ROOT_CONVERSATION_ID, "a long answer please"));
    // Streaming has started: the partial is in `core.live`.
    yield* eventually(() => {
      const live = ada.view(ROOT_CONVERSATION_ID)?.docs["core.live"];
      return live !== undefined && "generation" in live;
    });
    ada.close();
    yield* crash(url, session);
    yield* Effect.sleep("8 seconds");
    // A reconnecting client gets the session, and its first snapshot of the conversation already holds the full
    // answer: the alarm finished the turn.
    const back = yield* connect(url, session, "Ada");
    const listed = yield* eventually(() => back.events.find((event) => event.type === "session"));
    expect(listed).toMatchObject({ conversations: [{ id: ROOT_CONVERSATION_ID }] });
    yield* watch(back, ROOT_CONVERSATION_ID);
    const first = transcript(back.view(ROOT_CONVERSATION_ID));
    expect(first.at(-1)?.startsWith("assistant: echo: a long answer please")).toBe(true);
    back.close();
  }),
);

import * as Alchemy from "alchemy";
import * as Cloudflare from "@alchemy.run/cloudflare";
import * as Test from "@alchemy.run/test/Vitest";
import * as Effect from "effect/Effect";
import { expect } from "vitest";
import Stack from "./alchemy.run.ts";
import { MAX_WATCHED_CONVERSATIONS } from "@effect-durable-harness/host/SessionProtocol";
import { call, eventually as poll, objectName } from "./support.ts";
import {
  callSession,
  connect,
  eventually,
  refused,
  requestId,
  resultOf,
  transcript,
  watch,
} from "./sessionSupport.ts";

/**
 * The session socket (`Host.socket`) and the agent's RPC (`Host.agentRpc`) on local workerd, with the fake
 * model. Modeled on the chat example's `test/chat.test.ts`, whose socket the host now owns.
 */
const { test, beforeAll, afterAll, deploy, destroy } = Test.make({
  providers: Cloudflare.providers(),
  state: Alchemy.localState(),
  dev: true,
});

const stack = beforeAll(deploy(Stack), { timeout: 300_000 });
afterAll.skipIf(!!process.env.NO_DESTROY)(destroy(Stack), { timeout: 300_000 });

const ROOT = 1;

test(
  "a connection gets hello, the session and activity; a prompt is attributed, answered and followed in the view",
  Effect.gen(function* () {
    const { url } = yield* stack;
    const name = objectName("socket-submit");
    const ada = yield* connect(url, "session", name, "Ada");
    const hello = yield* eventually(() => ada.events.find((event) => event.type === "hello"));
    expect(hello).toMatchObject({ connection: { username: "Ada" } });
    const session = yield* eventually(() => ada.events.find((event) => event.type === "session"));
    expect(session).toMatchObject({ conversations: [{ id: ROOT }] });
    yield* eventually(() => ada.events.some((event) => event.type === "activity"));
    yield* watch(ada, ROOT);
    const id = requestId("submit");
    ada.send({ type: "submit", requestId: id, conversationId: ROOT, content: "hello" });
    const result = yield* resultOf(ada, id);
    expect(result).toMatchObject({ ok: true, conversationId: ROOT });
    const messages = yield* eventually(() => {
      const current = transcript(ada.view(ROOT));
      return current.length === 2 ? current : undefined;
    });
    expect(messages[0]).toBe("user: [Ada]\nhello");
    expect(messages[1]?.startsWith("assistant: echo: [Ada]\nhello")).toBe(true);
    ada.close();
  }),
);

test(
  "pages history with `entries`, and a watch from `from` starts right after what the client has",
  Effect.gen(function* () {
    const { url } = yield* stack;
    const name = objectName("socket-window");
    const ada = yield* connect(url, "session", name, "Ada");
    yield* watch(ada, ROOT);
    for (const content of ["one", "two"]) {
      const id = requestId("submit");
      ada.send({ type: "submit", requestId: id, conversationId: ROOT, content });
      yield* resultOf(ada, id);
    }
    const entries = yield* eventually(() => {
      const view = ada.view(ROOT);
      return transcript(view).length === 4 ? view?.entries : undefined;
    });
    const newest = entries.at(-1)!.id;

    const page = requestId("entries");
    ada.send({
      type: "entries",
      requestId: page,
      conversationId: ROOT,
      maxEntryId: newest,
      order: "descending",
      limit: 2,
    });
    const answered = yield* resultOf(ada, page);
    expect(answered).toMatchObject({ ok: true, more: true });
    expect(
      answered.type === "result" && answered.ok
        ? answered.entries?.map((entry) => entry["id"])
        : undefined,
    ).toEqual(
      entries
        .slice(-2)
        .map((entry) => entry.id)
        .toReversed(),
    );

    // A reconnect resumes right after the newest entry: an empty window whose `before` is that entry.
    const bob = yield* connect(url, "session", name, "Bob");
    const resumed = requestId("watch");
    bob.send({
      type: "watch",
      requestId: resumed,
      conversationId: ROOT,
      window: { from: newest + 1 },
    });
    yield* resultOf(bob, resumed);
    const view = yield* eventually(() => bob.view(ROOT) ?? undefined);
    expect(view.entries).toEqual([]);
    expect(view.before).toBe(newest);
    ada.close();
    bob.close();
  }),
);

test(
  "a resent submit answers the first submission; create and fork are idempotent by request ID",
  Effect.gen(function* () {
    const { url } = yield* stack;
    const name = objectName("socket-idempotent");
    const ada = yield* connect(url, "session", name, "Ada");
    yield* watch(ada, ROOT);
    const submit = {
      type: "submit" as const,
      requestId: requestId("submit"),
      conversationId: ROOT,
      content: "once",
    };
    ada.send(submit);
    ada.send(submit);
    yield* eventually(
      () =>
        ada.events.filter(
          (event) => event.type === "result" && event.requestId === submit.requestId,
        ).length === 2,
    );
    const answers = ada.events.filter(
      (event) => event.type === "result" && event.requestId === submit.requestId,
    );
    expect(answers[0]).toEqual(answers[1]);
    yield* eventually(() => transcript(ada.view(ROOT)).length === 2);

    const create = { type: "create" as const, requestId: requestId("create") };
    ada.send(create);
    ada.send(create);
    yield* eventually(
      () =>
        ada.events.filter(
          (event) => event.type === "result" && event.requestId === create.requestId,
        ).length === 2,
    );
    const created = ada.events.filter(
      (event) => event.type === "result" && event.requestId === create.requestId,
    );
    expect(created[0]).toMatchObject({ ok: true });
    const createdId =
      created[0]?.type === "result" && created[0].ok ? created[0].conversationId : 0;
    expect(created[1]).toEqual(created[0]);
    // Every connection hears of a new conversation.
    yield* eventually(() =>
      ada.events.some(
        (event) => event.type === "conversation" && event.conversation.id === createdId,
      ),
    );

    const entryId = ada.view(ROOT)?.entries.find((entry) => entry.kind === "core.user")?.id ?? 0;
    const fork = {
      type: "fork" as const,
      requestId: requestId("fork"),
      conversationId: ROOT,
      entryId,
    };
    ada.send(fork);
    const forked = yield* resultOf(ada, fork.requestId);
    expect(forked).toMatchObject({ ok: true });
    const forkedId = forked.type === "result" && forked.ok ? forked.conversationId : 0;
    ada.send(fork);
    yield* eventually(
      () =>
        ada.events.filter((event) => event.type === "result" && event.requestId === fork.requestId)
          .length === 2,
    );
    // The request ID of the fork, reused for a create, is refused.
    ada.send({ type: "create", requestId: fork.requestId });
    yield* eventually(
      () =>
        ada.events.filter((event) => event.type === "result" && event.requestId === fork.requestId)
          .length === 3,
    );
    const reused = ada.events.filter(
      (event) => event.type === "result" && event.requestId === fork.requestId,
    )[2];
    expect(reused).toMatchObject({ ok: false, error: { code: "conflict" } });
    const conversations = yield* callSession<ReadonlyArray<{ readonly id: number }>>(
      url,
      "session",
      name,
      "conversations",
    );
    expect(conversations.map((conversation) => conversation.id)).toEqual([1, createdId, forkedId]);
    ada.close();
  }),
);

test(
  "presence reaches the other watchers of the conversation, and clears when its connection leaves",
  Effect.gen(function* () {
    const { url } = yield* stack;
    const name = objectName("socket-presence");
    const ada = yield* connect(url, "session", name, "Ada");
    const bob = yield* connect(url, "session", name, "Bob");
    yield* watch(ada, ROOT);
    yield* watch(bob, ROOT);
    ada.send({ type: "presence", conversationId: ROOT, presence: { typing: true } });
    yield* eventually(() =>
      bob.events.some(
        (event) =>
          event.type === "presence" &&
          event.peers.length === 1 &&
          // `peer` projects what the others see of the connection.
          JSON.stringify(event.peers[0]?.connection) === JSON.stringify({ name: "Ada" }) &&
          JSON.stringify(event.peers[0]?.presence) === JSON.stringify({ typing: true }),
      ),
    );
    // A presence the schema refuses is an error, and changes nothing.
    ada.send({ type: "presence", conversationId: ROOT, presence: { typing: "yes" } });
    yield* eventually(() => ada.events.some((event) => event.type === "error"));
    // So is one over `MAX_PRESENCE_CHARACTERS`, which the schema would take.
    ada.send({
      type: "presence",
      conversationId: ROOT,
      presence: { typing: true, note: "x".repeat(200) },
    });
    yield* eventually(() => ada.events.filter((event) => event.type === "error").length === 2);
    ada.close();
    yield* eventually(() => {
      const last = bob.events.findLast((event) => event.type === "presence");
      return last?.type === "presence" && last.peers.length === 0;
    });
    bob.close();
  }),
);

test(
  "a read-only socket follows conversations and refuses commands that write",
  Effect.gen(function* () {
    const { url } = yield* stack;
    const name = objectName("socket-readonly");
    const reader = yield* connect(url, "readonly", name, "Ada");
    yield* watch(reader, ROOT);
    const id = requestId("submit");
    reader.send({ type: "submit", requestId: id, conversationId: ROOT, content: "hello" });
    expect(yield* resultOf(reader, id)).toMatchObject({
      ok: false,
      error: { code: "read_only" },
    });
    const create = requestId("create");
    reader.send({ type: "create", requestId: create });
    expect(yield* resultOf(reader, create)).toMatchObject({
      ok: false,
      error: { code: "read_only" },
    });
    // Writes through the object's methods still reach the reader.
    yield* callSession(url, "readonly", name, "submit", ROOT, "from rpc", {
      requestId: "rpc-1",
    });
    const messages = yield* eventually(() => {
      const current = transcript(reader.view(ROOT));
      return current.length === 2 ? current : undefined;
    });
    expect(messages[0]).toBe("user: from rpc");
    reader.close();
  }),
);

test(
  "accept refuses an upgrade; host.socket closes the connections a method picks",
  Effect.gen(function* () {
    const { url } = yield* stack;
    const name = objectName("socket-close");
    expect(yield* refused(url, "session", name, "intruder")).toBe(true);
    const ada = yield* connect(url, "session", name, "Ada");
    const bob = yield* connect(url, "session", name, "Bob");
    yield* watch(ada, ROOT);
    yield* watch(bob, ROOT);
    ada.send({ type: "presence", conversationId: ROOT, presence: { typing: true } });
    yield* eventually(() => {
      const last = bob.events.findLast((event) => event.type === "presence");
      return last?.type === "presence" && last.peers.length === 1;
    });
    const connections = yield* callSession<
      ReadonlyArray<{ readonly username: string; readonly watching: ReadonlyArray<number> }>
    >(url, "session", name, "connections");
    expect(connections.map((connection) => connection.username).toSorted()).toEqual(["Ada", "Bob"]);
    expect(connections.every((connection) => connection.watching.join() === "1")).toBe(true);
    yield* callSession(url, "session", name, "notify", ROOT, "hi watchers");
    yield* eventually(() =>
      bob.events.some((event) => event.type === "event" && event.conversationId === ROOT),
    );
    expect(yield* callSession(url, "session", name, "kick", "Ada")).toBe(1);
    const closed = yield* eventually(() => ada.closed());
    expect(closed).toEqual({ code: 4403, reason: "kicked" });
    expect(bob.closed()).toBeUndefined();
    // The presence the others get leaves the closed connection out.
    yield* eventually(() => {
      const last = bob.events.findLast((event) => event.type === "presence");
      return last?.type === "presence" && last.peers.length === 0;
    });
    const left = yield* callSession<ReadonlyArray<{ readonly username: string }>>(
      url,
      "session",
      name,
      "connections",
    );
    expect(left.map((connection) => connection.username)).toEqual(["Bob"]);
    bob.close();
  }),
);

test(
  "a resent withdraw answers ok once the first withdrew the submission; a placed one is a conflict",
  Effect.gen(function* () {
    const { url } = yield* stack;
    const name = objectName("socket-withdraw");
    const ada = yield* connect(url, "session", name, "Ada");
    yield* watch(ada, ROOT);
    const running = requestId("submit");
    ada.send({ type: "submit", requestId: running, conversationId: ROOT, content: "busy" });
    const placed = yield* resultOf(ada, running);
    // The fake model streams for about three seconds: the next prompt waits in the inbox.
    const queued = requestId("submit");
    ada.send({ type: "submit", requestId: queued, conversationId: ROOT, content: "later" });
    const waiting = yield* resultOf(ada, queued);
    const submissionId = waiting.type === "result" && waiting.ok ? (waiting.submissionId ?? 0) : 0;
    const withdraw = {
      type: "withdraw" as const,
      requestId: requestId("withdraw"),
      conversationId: ROOT,
      submissionId,
    };
    ada.send(withdraw);
    ada.send(withdraw);
    yield* eventually(
      () =>
        ada.events.filter(
          (event) => event.type === "result" && event.requestId === withdraw.requestId,
        ).length === 2,
    );
    const answers = ada.events.filter(
      (event) => event.type === "result" && event.requestId === withdraw.requestId,
    );
    expect(answers[0]).toMatchObject({ ok: true });
    expect(answers[1]).toEqual(answers[0]);
    const runningId = placed.type === "result" && placed.ok ? (placed.submissionId ?? 0) : 0;
    const late = requestId("withdraw");
    ada.send({ type: "withdraw", requestId: late, conversationId: ROOT, submissionId: runningId });
    expect(yield* resultOf(ada, late)).toMatchObject({ ok: false, error: { code: "conflict" } });
    ada.close();
  }),
);

test(
  "a connection watches at most MAX_WATCHED_CONVERSATIONS conversations",
  Effect.gen(function* () {
    const { url } = yield* stack;
    const name = objectName("socket-watch-cap");
    const ada = yield* connect(url, "session", name, "Ada");
    const ids: Array<number> = [ROOT];
    for (let index = 0; index < MAX_WATCHED_CONVERSATIONS; index++) {
      const id = requestId("create");
      ada.send({ type: "create", requestId: id });
      const created = yield* resultOf(ada, id);
      ids.push(created.type === "result" && created.ok ? (created.conversationId ?? 0) : 0);
    }
    for (const conversationId of ids.slice(0, MAX_WATCHED_CONVERSATIONS)) {
      yield* watch(ada, conversationId);
    }
    const over = requestId("watch");
    ada.send({
      type: "watch",
      requestId: over,
      conversationId: ids[MAX_WATCHED_CONVERSATIONS] ?? 0,
    });
    expect(yield* resultOf(ada, over)).toMatchObject({ ok: false, error: { code: "invalid" } });
    // Watching one already watched is not over the cap.
    const again = requestId("watch");
    ada.send({ type: "watch", requestId: again, conversationId: ROOT });
    expect(yield* resultOf(ada, again)).toMatchObject({ ok: true });
    ada.close();
  }),
);

test(
  "after the object restarts, a client that reconnects watches again and gets a fresh snapshot",
  Effect.gen(function* () {
    const { url } = yield* stack;
    const name = objectName("socket-restart");
    const ada = yield* connect(url, "session", name, "Ada");
    yield* watch(ada, ROOT);
    const id = requestId("submit");
    ada.send({ type: "submit", requestId: id, conversationId: ROOT, content: "before" });
    yield* eventually(() => transcript(ada.view(ROOT)).length === 2);
    yield* callSession(url, "session", name, "crash");
    // An abort resets the object and the runtime closes its sockets, as a deploy does.
    yield* eventually(() => ada.closed() !== undefined);
    const again = yield* connect(url, "session", name, "Ada");
    yield* watch(again, ROOT);
    expect(transcript(again.view(ROOT))).toHaveLength(2);
    // The resent prompt (same request ID) answers the first submission instead of submitting again.
    again.send({ type: "submit", requestId: id, conversationId: ROOT, content: "before" });
    expect(yield* resultOf(again, id)).toMatchObject({ ok: true });
    again.send({
      type: "submit",
      requestId: requestId("submit"),
      conversationId: ROOT,
      content: "after",
    });
    const messages = yield* eventually(() => {
      const current = transcript(again.view(ROOT));
      return current.length === 4 ? current : undefined;
    });
    expect(messages[2]).toBe("user: [Ada]\nafter");
    again.close();
  }),
);

test(
  "the agent's RPC: idempotent submit and create, awaited submissions, and effect-durable-harness's errors arrive as their classes",
  Effect.gen(function* () {
    const { url } = yield* stack;
    const name = objectName("agent-rpc");
    const root = yield* callSession<{ readonly id: number }>(url, "session", name, "ensureRoot");
    expect(root).toEqual({ id: ROOT });
    const first = yield* callSession<{ readonly id: number; readonly status: string }>(
      url,
      "session",
      name,
      "submit",
      ROOT,
      "hello",
      { requestId: "rpc-hello" },
    );
    const again = yield* callSession<{ readonly id: number }>(
      url,
      "session",
      name,
      "submit",
      ROOT,
      "hello",
      { requestId: "rpc-hello" },
    );
    expect(again.id).toBe(first.id);
    const done = yield* callSession<{ readonly status: string }>(
      url,
      "session",
      name,
      "awaitSubmission",
      first.id,
    );
    expect(done.status).toBe("done");
    const created = yield* callSession<{ readonly id: number }>(
      url,
      "session",
      name,
      "createConversation",
      { requestId: "rpc-create" },
    );
    const createdAgain = yield* callSession<{ readonly id: number }>(
      url,
      "session",
      name,
      "createConversation",
      { requestId: "rpc-create" },
    );
    expect(createdAgain).toEqual(created);
    const missing = yield* callSession(url, "session", name, "submit", 99, "hello", {});
    expect(missing).toEqual({ failed: "ConversationNotFound", revived: true });
    const unknown = yield* callSession(url, "session", name, "awaitSubmission", 999);
    expect(unknown).toEqual({ failed: "SubmissionError", revived: true });
  }),
);

test(
  "a handler Layer provided to the object is built at init: its KV binding works, and two objects share it",
  Effect.gen(function* () {
    const { url } = yield* stack;
    const first = objectName("isolate");
    const second = objectName("isolate");
    const one = yield* call<string>(url, first, "isolate");
    const two = yield* call<string>(url, second, "isolate");
    expect(one).toBe(two);
    const id = yield* call<number>(url, first, "startKv", "greeting", "hello from KV");
    const outcome = yield* poll(
      call<unknown>(url, first, "outcome", id),
      (value) => value !== null,
    );
    expect(outcome).toEqual({ status: "completed", result: "hello from KV" });
  }),
);

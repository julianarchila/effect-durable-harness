// The browser client's outbox, watches and resync, against a fake socket: what it sends, and what it does
// with what the object answers. The workerd suite covers the object's side.
import { afterEach, assert, beforeEach, describe, it, vi } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as SessionClient from "../src/SessionClient.ts";
import {
  type ClientCommand,
  decodeClientCommand,
  encodeServerEvent,
  REFUSED_CLOSE_CODE,
  type ServerEvent,
} from "../src/SessionProtocol.ts";

class FakeSocket implements SessionClient.ClientSocket {
  readyState = 0;
  readonly sent: Array<ClientCommand> = [];
  closed = false;
  readonly url: string;
  readonly listeners = new Map<string, Array<(event: SessionClient.SocketEvent) => void>>();

  constructor(url: string) {
    this.url = url;
  }

  addEventListener(
    type: "open" | "close" | "message",
    listener: (event: SessionClient.SocketEvent) => void,
  ) {
    this.listeners.set(type, [...(this.listeners.get(type) ?? []), listener]);
  }

  emit(type: "open" | "close" | "message", event: SessionClient.SocketEvent = {}) {
    for (const listener of this.listeners.get(type) ?? []) listener(event);
  }

  send(data: string) {
    const decoded = decodeClientCommand(data);
    if (decoded._tag === "Success") this.sent.push(decoded.success);
  }

  close(code?: number) {
    this.closed = true;
    this.readyState = 3;
    this.emit("close", code === undefined ? {} : { code });
  }

  open() {
    this.readyState = 1;
    this.emit("open");
  }

  receive(event: ServerEvent) {
    this.emit("message", { data: encodeServerEvent(event) });
  }

  /** The request ID of the last command of `type` sent. */
  last(type: ClientCommand["type"]) {
    const command = this.sent.findLast((sent) => sent.type === type);
    return command !== undefined && "requestId" in command ? command.requestId : "";
  }
}

const harness = () => {
  const sockets: Array<FakeSocket> = [];
  const client = SessionClient.make({
    url: "https://chat.example/api/sessions/s1",
    params: [["username", "Ada"]],
    connect: (url) => {
      const socket = new FakeSocket(url);
      sockets.push(socket);
      return socket;
    },
  });
  const current = () => {
    const socket = sockets.at(-1);
    if (socket === undefined) throw new Error("no socket");
    return socket;
  };
  return { client, sockets, current };
};

const snapshot = (conversationId: number): ServerEvent => ({
  type: "view",
  conversationId,
  frame: {
    type: "snapshot",
    value: { conversation: { id: conversationId }, entries: [], docs: {} },
  },
});

beforeEach(() => {
  vi.useFakeTimers();
});
afterEach(() => {
  vi.useRealTimers();
});

const entry = (id: number) => ({ id, conversationId: 1, kind: "app.note", data: { id } });

describe("SessionClient", () => {
  it("connects over ws with the params, and sends a command made before the socket opened once it opens", () => {
    const { client, current } = harness();
    assert.strictEqual(current().url, "wss://chat.example/api/sessions/s1?username=Ada");
    const fiber = Effect.runFork(client.submit(1, "hello", { whenBusy: "steer" }));
    assert.deepStrictEqual(current().sent, []);
    current().open();
    assert.deepStrictEqual(
      current().sent.map((command) => command.type),
      ["submit"],
    );
    current().receive({
      type: "result",
      requestId: current().last("submit"),
      ok: true,
      conversationId: 1,
      submissionId: 5,
    });
    assert.deepStrictEqual(Effect.runSync(Fiber.await(fiber)), Exit.succeed(5));
    client.close();
  });

  it("resends unanswered commands under the same request ID after a reconnect, and watches again", () => {
    const { client, sockets, current } = harness();
    current().open();
    const conversation = client.conversation(1);
    const unsubscribe = conversation.subscribe(() => {});
    const watchId = current().last("watch");
    current().receive({ type: "result", requestId: watchId, ok: true, conversationId: 1 });
    current().receive(snapshot(1));
    assert.strictEqual(conversation.get().status, "ready");
    client.presence(1, { typing: true });
    const fiber = Effect.runFork(client.create());
    const createId = current().last("create");
    assert.deepStrictEqual(conversation.get().sending, [], "only prompts are listed as sending");

    current().close();
    assert.strictEqual(client.state.get().status, "reconnecting");
    vi.advanceTimersByTime(500);
    assert.strictEqual(sockets.length, 2);
    current().open();
    const resent = current().sent;
    assert.deepStrictEqual(
      resent.map((command) => command.type),
      ["watch", "presence", "create"],
    );
    assert.notStrictEqual(current().last("watch"), watchId);
    assert.strictEqual(current().last("create"), createId);
    current().receive({ type: "result", requestId: createId, ok: true, conversationId: 2 });
    assert.deepStrictEqual(Effect.runSync(Fiber.await(fiber)), Exit.succeed(2));
    unsubscribe();
    client.close();
  });

  it("keeps a transcript: loads earlier entries with `entries`, and resumes after the newest on a reconnect", async () => {
    const { client, sockets, current } = harness();
    current().open();
    const conversation = client.conversation(1);
    const unsubscribe = conversation.subscribe(() => {});
    current().receive({
      type: "view",
      conversationId: 1,
      frame: {
        type: "snapshot",
        value: { conversation: { id: 1 }, entries: [entry(4), entry(5)], before: 3, docs: {} },
      },
    });
    const kinds = (): ReadonlyArray<number | string> | undefined =>
      conversation
        .get()
        .transcript?.items.map((item) => (item.kind === "entry" ? item.entry.id : item.kind));
    assert.deepStrictEqual(kinds(), ["earlier", 4, 5]);

    const loading = Effect.runFork(client.loadEarlier(1));
    // A load starts on its own fiber after the caller's step.
    await vi.advanceTimersByTimeAsync(0);
    const request = current().sent.findLast((sent) => sent.type === "entries");
    assert.deepStrictEqual(request === undefined ? undefined : { ...request, requestId: "" }, {
      type: "entries",
      requestId: "",
      conversationId: 1,
      maxEntryId: 3,
      order: "descending",
      limit: 50,
    });
    current().receive({
      type: "result",
      requestId: current().last("entries"),
      ok: true,
      conversationId: 1,
      entries: [entry(3), entry(1)],
      more: false,
    });
    await Effect.runPromise(Fiber.join(loading));
    // A page without more reaches the start: the marker goes.
    assert.deepStrictEqual(kinds(), [1, 3, 4, 5]);

    current().close();
    vi.advanceTimersByTime(500);
    assert.strictEqual(sockets.length, 2);
    current().open();
    const watch = current().sent.find((sent) => sent.type === "watch");
    assert.deepStrictEqual(watch?.type === "watch" ? watch.window : undefined, { from: 6 });
    current().receive({
      type: "view",
      conversationId: 1,
      frame: {
        type: "snapshot",
        value: { conversation: { id: 1 }, entries: [entry(6)], before: 5, docs: {} },
      },
    });
    assert.deepStrictEqual(kinds(), [1, 3, 4, 5, 6]);
    unsubscribe();
    client.close();
  });

  it("lists a prompt as sending until it is answered, and fails the command the object refuses", () => {
    const { client, current } = harness();
    current().open();
    const conversation = client.conversation(1);
    const unsubscribe = conversation.subscribe(() => {});
    const fiber = Effect.runFork(client.submit(1, "hello"));
    assert.deepStrictEqual(
      conversation.get().sending.map((sending) => sending.content),
      ["hello"],
    );
    current().receive({
      type: "result",
      requestId: current().last("submit"),
      ok: false,
      error: { code: "busy", message: "Conversation 1 is busy" },
    });
    assert.deepStrictEqual(conversation.get().sending, []);
    const exit = Effect.runSync(Fiber.await(fiber));
    assert.isTrue(Exit.isFailure(exit));
    assert.strictEqual(client.state.get().error?.code, "busy");
    unsubscribe();
    client.close();
  });

  it("marks a conversation the object does not have as missing", () => {
    const { client, current } = harness();
    current().open();
    const conversation = client.conversation(9);
    const unsubscribe = conversation.subscribe(() => {});
    current().receive({
      type: "result",
      requestId: current().last("watch"),
      ok: false,
      error: { code: "not_found", message: "Conversation 9 does not exist" },
    });
    assert.strictEqual(conversation.get().status, "missing");
    unsubscribe();
    client.close();
  });

  it("reconnects for a fresh snapshot when a batch does not apply", () => {
    const { client, sockets, current } = harness();
    current().open();
    const unsubscribe = client.conversation(1).subscribe(() => {});
    current().receive(snapshot(1));
    current().receive({
      type: "view",
      conversationId: 1,
      frame: { type: "ops", ops: [["d", ["missing", "deeper"]]] },
    });
    assert.isTrue(sockets[0]?.closed);
    vi.advanceTimersByTime(500);
    assert.strictEqual(sockets.length, 2);
    unsubscribe();
    client.close();
  });

  it("unwatches a conversation once its last subscriber leaves", async () => {
    const { client, current } = harness();
    current().open();
    const conversation = client.conversation(1);
    const first = conversation.subscribe(() => {});
    const second = conversation.subscribe(() => {});
    first();
    await Promise.resolve();
    assert.isFalse(current().sent.some((command) => command.type === "unwatch"));
    second();
    await Promise.resolve();
    assert.isTrue(current().sent.some((command) => command.type === "unwatch"));
    client.close();
  });

  it("sends a command only when its Effect runs, and each run is a new request", () => {
    const { client, current } = harness();
    current().open();
    const create = client.create();
    assert.deepStrictEqual(current().sent, []);
    const first = SessionClient.run(create);
    const second = SessionClient.run(create);
    const requests = current().sent.flatMap((command) =>
      command.type === "create" ? [command.requestId] : [],
    );
    assert.strictEqual(requests.length, 2);
    assert.notStrictEqual(requests[0], requests[1]);
    for (const [index, id] of requests.entries()) {
      current().receive({ type: "result", requestId: id, ok: true, conversationId: index + 2 });
    }
    assert.deepStrictEqual(Effect.runSync(Fiber.await(first)), Exit.succeed(2));
    assert.deepStrictEqual(Effect.runSync(Fiber.await(second)), Exit.succeed(3));
    client.close();
  });

  it("ignores the previous watch's batches after a re-watch, until its snapshot", async () => {
    const { client, sockets, current } = harness();
    current().open();
    const conversation = client.conversation(1);
    const first = conversation.subscribe(() => {});
    current().receive(snapshot(1));
    first();
    await Promise.resolve();
    assert.isTrue(current().sent.some((command) => command.type === "unwatch"));
    const second = conversation.subscribe(() => {});
    assert.strictEqual(current().sent.filter((command) => command.type === "watch").length, 2);
    // A batch of the first watch, still in flight: it does not apply to the reset copy.
    current().receive({
      type: "view",
      conversationId: 1,
      frame: { type: "ops", ops: [["d", ["missing", "deeper"]]] },
    });
    assert.isFalse(current().closed);
    assert.strictEqual(conversation.get().status, "loading");
    current().receive(snapshot(1));
    assert.strictEqual(conversation.get().status, "ready");
    assert.strictEqual(sockets.length, 1);
    second();
    client.close();
  });

  it("keeps watching through a remount in one tick, without asking again", async () => {
    const { client, current } = harness();
    current().open();
    const conversation = client.conversation(1);
    const first = conversation.subscribe(() => {});
    current().receive(snapshot(1));
    first();
    const second = conversation.subscribe(() => {});
    await Promise.resolve();
    assert.deepStrictEqual(
      current().sent.map((command) => command.type),
      ["watch"],
    );
    assert.strictEqual(conversation.get().status, "ready");
    second();
    await Promise.resolve();
    assert.strictEqual(
      conversation.get().status,
      "loading",
      "an unwatched conversation is forgotten",
    );
    client.close();
  });

  it("stops reconnecting when the object refuses the connection", () => {
    const { client, sockets, current } = harness();
    const fiber = SessionClient.run(client.create());
    current().close(REFUSED_CLOSE_CODE);
    assert.strictEqual(client.state.get().status, "refused");
    vi.advanceTimersByTime(60_000);
    assert.strictEqual(sockets.length, 1);
    const exit = Effect.runSync(Fiber.await(fiber));
    assert.isTrue(Exit.isFailure(exit));
  });

  it("fails unanswered commands when closed", () => {
    const { client, current } = harness();
    const fiber = Effect.runFork(client.fork(1, 2));
    client.close();
    const exit = Effect.runSync(Fiber.await(fiber));
    assert.isTrue(Exit.isFailure(exit));
    assert.strictEqual(client.state.get().status, "closed");
    assert.isTrue(current().closed);
  });
});

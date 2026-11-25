import * as Net from "node:net";
import * as Alchemy from "alchemy";
import * as Cloudflare from "@alchemy.run/cloudflare";
import * as Test from "@alchemy.run/test/Vitest";
import * as Effect from "effect/Effect";
import { expect } from "vitest";
import Stack from "./alchemy.run.ts";
import { call, deployed, eventually, objectName, state } from "./support.ts";

/**
 * A TCP socket opened from a task phase, on the object's fibers after the call that created the task returned:
 * what a database driver does when a tool or a task queries Postgres.
 * The echo server runs in the test process, so this runs on local workerd only; a deployed run needs a public
 * endpoint (Hyperdrive), measured separately.
 */
const { test, beforeAll, afterAll, deploy, destroy } = Test.make({
  providers: Cloudflare.providers(),
  state: Alchemy.localState(),
  dev: true,
});

const stack = beforeAll(deploy(Stack), { timeout: 300_000 });
afterAll.skipIf(!!process.env.NO_DESTROY)(destroy(Stack), { timeout: 300_000 });

/** An echo server on a free local port, closed with the test's scope. */
const echoServer = Effect.acquireRelease(
  Effect.callback<{ readonly server: Net.Server; readonly port: number }>((resume) => {
    const server = Net.createServer((socket) => socket.on("data", (data) => socket.write(data)));
    server.listen(0, "127.0.0.1", () => {
      // SAFETY: a server listening on a TCP port reports an `AddressInfo`.
      const address = server.address() as Net.AddressInfo;
      resume(Effect.succeed({ server, port: address.port }));
    });
  }),
  ({ server }) => Effect.sync(() => server.close()),
);

test.skipIf(deployed)(
  "a task phase opens a TCP socket on the object's fibers, after the call that created it returned",
  Effect.gen(function* () {
    const { url } = yield* stack;
    const { port } = yield* echoServer;
    const name = objectName("socket");
    yield* state(url, name);
    const id = yield* call<number>(url, name, "startSocket", "127.0.0.1", port);
    const outcome = yield* eventually(
      call<unknown>(url, name, "outcome", id),
      (value) => value !== null,
    );
    expect(outcome).toEqual({ status: "completed", result: "ping" });
  }).pipe(Effect.scoped),
);

// Deployed, the object cannot reach the test process: a public echo server stands in for a database.
test.skipIf(!deployed)(
  "deployed: a task phase opens a TCP socket to the internet from the object's fibers",
  Effect.gen(function* () {
    const { url } = yield* stack;
    const name = objectName("socket-deployed");
    yield* state(url, name);
    const id = yield* call<number>(url, name, "startSocket", "tcpbin.com", 4242);
    const outcome = yield* eventually(
      call<unknown>(url, name, "outcome", id),
      (value) => value !== null,
    );
    expect(outcome).toEqual({ status: "completed", result: "ping" });
  }),
);

import { RuleTester } from "oxlint/plugins-dev";
import { describe, it } from "vitest";

import { noExternalInCommitRule } from "../rules/no-external-in-commit.ts";

RuleTester.describe = describe;
RuleTester.it = it;

const tester = new RuleTester({
  languageOptions: { sourceType: "module", parserOptions: { lang: "ts" } },
});

const imports = `import * as Effect from "effect/Effect";
import * as Clock from "effect/Clock";
import { Models } from "@effect-durable-harness/core/Models";
import * as HttpClient from "effect/unstable/http/HttpClient";
import * as Deferred from "effect/Deferred";
import { Tx } from "@effect-durable-harness/core/Tx";
`;

// Each case runs inside a generator, as a commit does in an `Effect.gen` body.
const source = (body: string, header = imports) =>
  `${header}\nexport const program = Effect.gen(function* () {\n${body}\n});\n`;

const external = (what: string) => ({ messageId: "external", data: { what } });
const nested = (what: string) => ({ messageId: "nested", data: { what } });
const service = (what: string) => ({ messageId: "service", data: { what } });

tester.run("no-external-in-commit", noExternalInCommitRule, {
  valid: [
    {
      name: "writes through Tx inside the change",
      code: source(`yield* session.commit(Effect.gen(function* () {
  const tx = yield* Tx;
  yield* tx.appendEntry(id, entry);
}));`),
    },
    {
      name: "the effect sandwich: the external work before and after the commit",
      code: source(`const fetched = yield* Effect.tryPromise(() => fetch(url));
yield* session.commit(Effect.gen(function* () { yield* write(fetched); }));
yield* Effect.sleep("1 second");`),
    },
    {
      name: "a fork that is the commit's own child",
      code: source(`yield* session.commit(Effect.gen(function* () {
  const fiber = yield* Effect.forkChild(read, { startImmediately: true });
  yield* Fiber.join(fiber);
}));`),
    },
    {
      name: "reading the clock is not a wait",
      code: source(
        `yield* session.commit(Effect.gen(function* () { const now = yield* Clock.currentTimeMillis; }));`,
      ),
    },
    {
      name: "a transition without a change",
      code: source(`yield* runtime.complete(Effect.promise(() => p));`),
    },
    {
      name: "Effect.fail and Deferred.complete are not transitions",
      code: source(`yield* Deferred.complete(deferred, Effect.promise(() => p));
yield* Effect.fail(Effect.sleep("1 second"));`),
    },
    {
      name: "a change passed as a parameter is not followed",
      code: source(`const run = (change) => session.commit(change);
const change = Effect.promise(() => p);`),
    },
    {
      name: "a `let` change is not followed",
      code: source(`let change = Effect.promise(() => p);
yield* session.commit(change);`),
    },
    {
      name: "a local fetch is not the global one",
      code: source(`const fetch = (id) => Effect.succeed(id);
yield* session.commit(fetch(1));`),
    },
    {
      name: "a commit of another shape is not a commit: a tracker's three-argument commit",
      code: source(`yield* run.commit(Effect.gen(function* () {
  changes.commit(draft, message, taken);
}));
changes.commit(Effect.promise(() => p), message, taken);`),
    },
    {
      name: "a service read and used outside the change, its result passed in",
      code: source(`const models = yield* Models;
const reply = yield* models.complete(request);
yield* session.commit(Effect.gen(function* () { yield* write(reply); }));`),
    },
    {
      name: "Tx read inside the change, and a captured tx",
      code: source(`yield* session.commit(Effect.gen(function* () {
  const tx = yield* Tx;
  yield* Effect.sync(() => tx.appendEntry(id, entry));
}));`),
    },
    {
      name: "a commit after another, not inside it",
      code: source(`yield* session.commit(write).pipe(Effect.andThen(session.commit(other)));`),
    },
    {
      name: "a transition on an import is a constructor, not a commit",
      code: source(
        `const state = Task.complete(result, Effect.promise(() => p));`,
        `${imports}import * as Task from "@effect-durable-harness/core/Task";\n`,
      ),
    },
    {
      name: "Effect.never outside a change, a local never, and a captured clock read",
      code: source(`const clock = yield* Clock.Clock;
yield* Effect.never;
yield* session.commit(Effect.gen(function* () {
  const never = Effect.void;
  yield* never;
  const now = clock.currentTimeMillisUnsafe();
  const tx = yield* Tx.Tx;
}));`),
    },
    {
      name: "the external work in a nested function outside any change",
      code: source(`const later = () => Effect.sleep(1);
yield* session.commit(write);
yield* later();`),
    },
  ],
  invalid: [
    {
      name: "Effect.promise in Session.commit",
      code: source(
        `yield* session.commit(Effect.gen(function* () { yield* Effect.promise(() => p); }));`,
      ),
      errors: [external("Effect.promise")],
    },
    {
      name: "Effect.tryPromise in Harness.commit",
      code: source(`yield* harness.commit(Effect.tryPromise(() => p));`),
      errors: [external("Effect.tryPromise")],
    },
    {
      name: "Effect.sleep in Conversation.commit",
      code: source(`yield* conversation.commit(Effect.andThen(Effect.sleep("1 second"), write));`),
      errors: [external("Effect.sleep")],
    },
    {
      name: "waits in a pipe: delay, timeout, retry, a Clock's sleep, never",
      code: source(`yield* session.commit(Effect.gen(function* () {
  yield* write.pipe(Effect.delay("1 second"), Effect.timeout("1 second"), Effect.retry(policy));
  yield* Effect.clockWith((clock) => clock.sleep("1 second"));
  yield* Effect.never;
}));`),
      errors: [
        external("Effect.delay"),
        external("Effect.timeout"),
        external("Effect.retry"),
        external("sleep"),
        external("Effect.never"),
      ],
    },
    {
      name: "fetch, HttpClient and a detached fork",
      code: source(`yield* session.commit(Effect.gen(function* () {
  yield* HttpClient.get(url);
  yield* Effect.forkDetach(work);
  const response = globalThis.fetch(url);
  const other = fetch(url);
}));`),
      errors: [
        external("HttpClient.get"),
        external("Effect.forkDetach"),
        external("fetch"),
        external("fetch"),
      ],
    },
    {
      name: "a nested run and a promise",
      code: source(`yield* session.commit(Effect.sync(() => {
  Effect.runPromise(work);
  return new Promise((resolve) => resolve(1));
}));`),
      errors: [external("Effect.runPromise"), external("new Promise")],
    },
    {
      name: "await inside the change",
      code: source(`yield* session.commit(Effect.promise(async () => { await p; }));`),
      errors: [external("Effect.promise"), external("await")],
    },
    {
      name: "commitWith with the concrete transaction",
      code: source(
        `yield* kernel.commitWith((tx) => Effect.gen(function* () { yield* Effect.sleep(10); }));`,
      ),
      errors: [external("Effect.sleep")],
    },
    {
      name: "a task runtime's commit, as a function of the reread record",
      code: source(
        `yield* runtime.commit((current) => Effect.as(Effect.promise(() => p), Task.next(current.state)));`,
      ),
      errors: [external("Effect.promise")],
    },
    {
      name: "a bare commit from a destructured runtime",
      code: source(`const { commit } = runtime;
yield* commit(Effect.promise(() => p));`),
      errors: [external("Effect.promise")],
    },
    {
      name: "the change of a transition, after its own arguments",
      code: source(`yield* runtime.next(checkpoint, Effect.promise(() => p));
yield* runtime.complete(result, Effect.sleep(1));
yield* runtime.fail("failed", Effect.sleep(1));
yield* runtime.waitFor([id], options, Effect.sleep(1));
yield* runtime.sleepUntil(until, checkpoint, Effect.sleep(1));`),
      errors: [
        external("Effect.promise"),
        external("Effect.sleep"),
        external("Effect.sleep"),
        external("Effect.sleep"),
        external("Effect.sleep"),
      ],
    },
    {
      name: "a change passed by name, defined with const in the same file",
      code: source(`const change = Effect.gen(function* () { yield* Effect.promise(() => p); });
yield* session.commit(change);`),
      errors: [external("Effect.promise")],
    },
    {
      name: "a change built by a local function",
      code: source(`function write(entry) { return Effect.tryPromise(() => send(entry)); }
const later = Effect.fn("later")(function* (entry) { yield* Effect.sleep(1); });
yield* session.commit(write(entry));
yield* session.commit(later(entry));`),
      errors: [external("Effect.tryPromise"), external("Effect.sleep")],
    },
    {
      name: "a commit inside a commit",
      code: source(`yield* session.commit(Effect.gen(function* () {
  yield* session.commit(write);
  yield* call.commit(write, { persistence: { kind: "deferred" } });
}));
yield* kernel.commitWith((tx) => commit(write));`),
      errors: [nested(".commit"), nested(".commit"), nested("commit")],
    },
    {
      name: "a service yielded inside the change",
      code: source(`yield* session.commit(Effect.gen(function* () {
  const models = yield* Models;
  const session = yield* Session;
}));`),
      errors: [service("yield* Models"), service("yield* Session")],
    },
    {
      name: "a module's service tag, yielded inside the change and captured from outside it",
      code: source(`const client = yield* HttpClient.HttpClient;
const sql = yield* SqlClient.SqlClient;
yield* session.commit(Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem;
  yield* sql.withTransaction(write);
  return yield* client.get(url);
}));`),
      errors: [
        service("yield* FileSystem.FileSystem"),
        service("sql, read from SqlClient.SqlClient"),
        service("client, read from HttpClient.HttpClient"),
      ],
    },
    {
      name: "Effect.never as a value, from the namespace and a named import",
      code: source(
        `yield* session.commit(Effect.gen(function* () { yield* never; }));
yield* session.commit(Effect.andThen(write, Effect.never));`,
        `${imports}import { never } from "effect/Effect";\n`,
      ),
      errors: [external("Effect.never"), external("Effect.never")],
    },
    {
      name: "a service read outside the change and called inside it",
      code: source(`const models = yield* Models;
const { stream } = yield* Models;
yield* conversation.commit(Effect.gen(function* () {
  const reply = yield* models.complete(request);
  yield* stream(request);
}));`),
      errors: [service("models, read from Models"), service("stream, read from Models")],
    },
    {
      name: "a service captured by a plain function change, in a nested function",
      code: source(`const crm = yield* CrmClient;
yield* run.commit((current) => Effect.sync(() => {
  const inner = () => crm.upsert(current.input);
  return inner();
}));`),
      errors: [service("crm, read from CrmClient")],
    },
    {
      name: "an external inside a nested Effect.gen and an arrow inside the change",
      code: source(`yield* session.commit(Effect.gen(function* () {
  const step = Effect.gen(function* () { yield* Effect.sleep(1); });
  const later = () => Effect.promise(() => p);
  yield* step;
  yield* later();
}));`),
      errors: [external("Effect.sleep"), external("Effect.promise")],
    },
    {
      name: "a change built with pipe and Effect.andThen",
      code: source(
        `yield* session.commit(write.pipe(Effect.andThen(() => Effect.tryPromise(() => send()))));`,
      ),
      errors: [external("Effect.tryPromise")],
    },
    {
      name: "named imports from effect/Effect and the effect barrel",
      code: source(
        `yield* session.commit(fromPromise(() => p));
yield* session.commit(Effect.sleep(1));`,
        `import { promise as fromPromise } from "effect/Effect";
import { Effect } from "effect";
`,
      ),
      errors: [external("Effect.promise"), external("Effect.sleep")],
    },
  ],
});

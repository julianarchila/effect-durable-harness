// What a report says it is about, on every backend.
//
// - A `Report` holds the info and the `cause`; the message is the cause's.
// - "name the extension whose definition ran, also after another one replaced it" is skipped: it replaces a
//   definition while its task runs, which a static registry cannot do.
// - "tell extension, listener, wake, and timer failures from a failed Session": the commit observer is the
//   kernel's and wakes are a host's `SessionWakeups`; timers are `Clock` sleeps, which cannot fail. A storage
//   failure is `failNextCommit`.
import { assert, it } from "@effect/vitest";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as AgentTool from "../src/AgentTool.ts";
import * as Extension from "../src/Extension.ts";
import * as Hook from "../src/Hook.ts";
import type { Report, ReportInfo } from "../src/Report.ts";
import * as Section from "../src/Section.ts";
import { SessionWakeups } from "../src/SessionWakeups.ts";
import * as Task from "../src/Task.ts";
import { fauxAssistantMessage } from "../src/testing/FauxModel.ts";
import { Tx } from "../src/Tx.ts";
import { chatSetup, openChat, waitFor } from "./ChatSupport.ts";
import { completed, eventually, openHarness } from "./HarnessSupport.ts";
import { controlledStorage, describeEachBackend, run } from "./SessionSupport.ts";

/** A report's message and what it is about. */
interface Described {
  readonly message: string;
  readonly info: ReportInfo;
}

const described = (report: Report): Described => {
  const { cause, ...info } = report;
  const error = Cause.squash(cause);
  return { message: error instanceof Error ? error.message : String(error), info };
};

const infoOf = (reports: ReadonlyArray<Report>, message: string) =>
  reports.map(described).find((report) => report.message === message)?.info;

/** A failure of the environment builder. */
class EnvFailed extends Schema.TaggedError<EnvFailed>()("EnvFailed", { message: Schema.String }) {}

const Step = Schema.Struct({ phase: Schema.Literal("run") });

describeEachBackend("reports", (backend) => {
  it.effect("say which task reported a failure or faulted", () =>
    run(backend)(
      Effect.gen(function* () {
        const Faulty = Task.make({
          name: "test.faulty",
          version: 1,
          input: Schema.Null,
          checkpoint: Step,
          result: Schema.Null,
          initial: () => ({ phase: "run" }),
          phases: {
            run: (_task, runtime) =>
              runtime
                .report(Cause.die(new Error("noted")))
                .pipe(Effect.andThen(Effect.die(new Error("broken")))),
          },
          abort: () => Effect.void,
        });
        const h = yield* openHarness(yield* controlledStorage, [], {
          extensions: [Extension.make({ name: "test", tasks: [Faulty] })],
        });
        const root = yield* h.harness.ensureRoot();
        const id = yield* root.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            return yield* tx.createTask(Faulty, null, { ownership: { kind: "conversation" } });
          }),
        );
        yield* h.harness.resume;
        const settled = yield* h.harness.awaitTask(id);
        assert.strictEqual(settled.state.outcome.status, "faulted");
        yield* eventually(Effect.sync(() => h.reports.length === 2));
        // The task's own failures name the extension that installed it.
        const task = {
          task: { id, kind: "test.faulty", conversationId: root.id },
          extension: "test",
        };
        assert.deepStrictEqual(h.reports.map(described), [
          { message: "noted", info: { kind: "task_error", ...task } },
          { message: "broken", info: { kind: "task_faulted", ...task, reason: "threw" } },
        ]);
        yield* h.close;
      }),
    ),
  );

  // A static registry never replaces a definition while its task runs.
  it.skip("name the extension whose definition ran, also after another one replaced it", () => {});

  it.effect(
    "name the extension of a failing section or hook, and tell them from a failing environment builder",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const setup = chatSetup();
          setup.faux.setResponses([fauxAssistantMessage("answer")]);
          const { harness, root } = yield* openChat(yield* controlledStorage, setup, {
            extensions: [
              Extension.make({
                name: "sections",
                sections: [Section.make("broken", () => Effect.die(new Error("section")))],
              }),
              Extension.make({
                name: "hooks",
                hooks: [Hook.generation({ afterResponse: () => Effect.die(new Error("hook")) })],
              }),
            ],
            env: () => Effect.fail(new EnvFailed({ message: "env" })),
          });
          const submission = yield* root.submit({ type: "input", content: "hi" });
          yield* waitFor(submission.status.pipe(Effect.map((record) => record.status === "done")));
          yield* eventually(Effect.sync(() => infoOf(setup.reports, "hook") !== undefined));
          const generation = { kind: "core.generation", conversationId: root.id };
          const env = infoOf(setup.reports, "env");
          assert.strictEqual(env?.kind, "host_error");
          assert.containSubset(env, { callback: "env", task: generation });
          const section = infoOf(setup.reports, "section");
          assert.strictEqual(section?.kind, "extension_error");
          assert.containSubset(section, { extension: "sections", task: generation });
          const hook = infoOf(setup.reports, "hook");
          assert.strictEqual(hook?.kind, "extension_error");
          assert.containSubset(hook, { extension: "hooks", task: generation });
          yield* harness.close;
        }),
      ),
  );

  it.effect("tell extension, listener, and wake failures from a failed Session", () =>
    run(backend)(
      Effect.gen(function* () {
        const Sleeper = Task.make({
          name: "test.sleeper",
          version: 1,
          input: Schema.Null,
          checkpoint: Schema.Struct({ phase: Schema.Literals(["run", "done"]) }),
          result: Schema.Null,
          initial: () => ({ phase: "run" as const }),
          phases: {
            run: (_task, runtime) =>
              Effect.gen(function* () {
                const now = yield* runtime.now;
                return yield* runtime.sleepUntil(now + 60_000, { phase: "done" });
              }),
            done: (_task, runtime) => runtime.commit(() => Effect.succeed(completed(null))),
          },
          abort: () => Effect.void,
        });
        const echo = AgentTool.make("echo", {
          description: "echo",
          parameters: Schema.Struct({}),
          execute: () => Effect.succeed({}),
        });
        const storage = yield* controlledStorage;
        const h = yield* openHarness(storage, [Sleeper], {
          extensions: [
            Extension.make({ name: "tools", tools: [echo] }),
            Extension.make({
              name: "wraps",
              wraps: [Extension.wrapTool(echo, (tool) => ({ ...tool, name: "renamed" }))],
            }),
          ],
        }).pipe(
          Effect.provideService(SessionWakeups, {
            schedule: () => Effect.die(new Error("alarm")),
          }),
        );
        yield* h.kernel.observeCommits({
          advance: () => {
            throw new Error("listener");
          },
          end: () => {},
        });
        const root = yield* h.harness.ensureRoot();
        yield* root.agent;
        yield* root.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            return yield* tx.createTask(Sleeper, null, { ownership: { kind: "conversation" } });
          }),
        );
        yield* h.harness.resume;
        yield* eventually(
          Effect.sync(() =>
            ["alarm", "listener", "Wrapper renamed echo to renamed"].every(
              (message) => infoOf(h.reports, message) !== undefined,
            ),
          ),
        );
        assert.deepStrictEqual(infoOf(h.reports, "Wrapper renamed echo to renamed"), {
          kind: "extension_error",
          conversationId: root.id,
          extension: "wraps",
        });
        assert.deepStrictEqual(infoOf(h.reports, "listener"), { kind: "listener_error" });
        assert.deepStrictEqual(infoOf(h.reports, "alarm"), {
          kind: "host_error",
          callback: "onWake",
        });

        storage.failNextCommit("disk gone");
        yield* Effect.exit(
          root.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              return yield* tx.createConversation({ ownership: { kind: "ownerless" } });
            }),
          ),
        );
        yield* h.harness.closed;
        yield* eventually(Effect.sync(() => infoOf(h.reports, "disk gone") !== undefined));
        assert.deepStrictEqual(infoOf(h.reports, "disk gone"), { kind: "storage_failed" });
      }),
    ),
  );
});

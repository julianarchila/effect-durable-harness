// Conversations: creation, ownership, entries and configuration, on every backend.
//
// - `openHarness(storage, names)` opens with a static `tools` extension holding those tools; tools added before
//   first use are more extensions of the same open.
// - A reopen is a new Harness over the same storage; a fresh storage is the backend under test.
// - `Conversation.entries` is a `Stream`; paging is checked with storage pages of two, forced through
//   `intercept.scanEntries`.
// - `snapshot`, `snapshotAsOf` and `watchDoc` are on the `Session` service the Harness provides.
// - A throwing `init` or creation hook is a defect of the creating commit.
// - `configure(id, change)` is the internal one, with `Tx` in context.
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Predicate from "effect/Predicate";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { AgentDoc } from "../src/Agent.ts";
import { AgentStorage, type ScanOrder } from "../src/AgentStorage.ts";
import * as AgentTool from "../src/AgentTool.ts";
import * as Doc from "../src/Doc.ts";
import * as Entry from "../src/Entry.ts";
import * as Extension from "../src/Extension.ts";
import type { Conversation } from "../src/Harness.ts";
import { ConversationId, ROOT_CONVERSATION_ID } from "../src/Ids.ts";
import { configure } from "../src/internal/agent.ts";
import { LiveDoc } from "../src/Live.ts";
import type { UserMessage } from "../src/Message.ts";
import { ProviderDoc } from "../src/ProviderSession.ts";
import type { EntryRecord } from "../src/Records.ts";
import * as Task from "../src/Task.ts";
import { Tx } from "../src/Tx.ts";
import { openHarness } from "./HarnessSupport.ts";
import {
  acquire,
  type ControlledStorage,
  controlledStorage,
  createConversation,
  describeEachBackend,
  failureMessage,
  run,
  withSession,
} from "./SessionSupport.ts";

const UUID_V7 = /^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/u;

const NoteDoc = Doc.make({
  checkpointWhen: () => false,
  kind: "test.note",
  version: 1,
  scope: "conversation",
  history: "rewindable",
  fork: "asOf",
  schema: Schema.Struct({ text: Schema.String }),
  initial: () => ({ text: "" }),
});

const Message = Entry.make("message");

/** A tool with empty parameters that returns no output. */
const tool = (name: string, description = `${name} tool`) =>
  AgentTool.make(name, {
    description,
    parameters: Schema.Struct({}),
    execute: () => Effect.succeed({ output: [] }),
  });

const tools = (...names: ReadonlyArray<string>) =>
  Extension.make({ name: "tools", tools: names.map((name) => tool(name)) });

const user = (text: string): UserMessage => ({ role: "user", content: text, timestamp: 1 });

const ownerless = { ownership: { kind: "ownerless" } } as const;

const append = (conversation: Conversation, text: string) =>
  conversation.commit(
    Effect.gen(function* () {
      const tx = yield* Tx;
      return yield* tx.appendEntry(conversation.id, { kind: "message", model: [user(text)] });
    }),
  );

const textOf = (entry: EntryRecord): string => {
  const message = entry.model?.[0];
  return message?.role === "user" && Predicate.isString(message.content) ? message.content : "";
};

/** Every entry's text, through the whole stream. */
const allEntries = (conversation: Conversation, order?: ScanOrder) =>
  Stream.runCollect(conversation.entries(order === undefined ? {} : { order })).pipe(
    Effect.map((entries) => Array.from(entries, textOf)),
  );

/** Storage pages of two entries. */
const pagesOfTwo = (storage: ControlledStorage) =>
  Effect.gen(function* () {
    const backend = yield* AgentStorage;
    storage.intercept.scanEntries = ([query, _limit, cursor]) =>
      backend.scanEntries(query, 2, cursor);
  });

const some = <A>(value: Option.Option<A>) => Option.getOrUndefined(value);

describeEachBackend("Harness root and conversations", (backend) => {
  it.effect("creates the root lazily with its agent change and init in one commit", () =>
    run(backend)(
      Effect.gen(function* () {
        const storage = yield* controlledStorage;
        const h = yield* openHarness(storage, [], { extensions: [tools("read", "bash")] });
        assert.lengthOf(storage.commits, 0);
        const root = yield* h.harness.ensureRoot({
          agent: { thinkingLevel: "high" },
          init: (id) =>
            Effect.gen(function* () {
              const tx = yield* Tx;
              // The agent change applied before init.
              assert.strictEqual((yield* tx.doc(AgentDoc, id)).thinkingLevel, "high");
              (yield* tx.doc(NoteDoc, id)).text = "root note";
            }),
        });
        assert.strictEqual(root.id, ROOT_CONVERSATION_ID);
        assert.lengthOf(storage.commits, 1);
        // Conversation, five built-in documents, and the init note.
        assert.deepStrictEqual(
          storage.commits[0]!.map((write) => write.type),
          [
            "conversation",
            "document.create",
            "document.create",
            "document.create",
            "document.create",
            "document.create",
            "document.create",
          ],
        );
        assert.deepStrictEqual(some(yield* h.session.snapshot(LiveDoc, root.id)), {});
        const provider = some(yield* h.session.snapshot(ProviderDoc, root.id));
        assert.deepStrictEqual(Object.keys(provider!), ["sessionId"]);
        assert.match(provider!.sessionId, UUID_V7);
        assert.deepStrictEqual(some(yield* h.session.snapshot(AgentDoc, root.id)), {
          thinkingLevel: "high",
        });
        const agent = yield* root.agent;
        assert.strictEqual(agent.thinkingLevel, "high");
        assert.deepStrictEqual(
          agent.tools.map((each) => each.name),
          ["read", "bash"],
        );
        assert.deepStrictEqual(some(yield* h.session.snapshot(NoteDoc, root.id)), {
          text: "root note",
        });

        const again = yield* h.harness.ensureRoot({
          agent: { thinkingLevel: "low" },
          init: () => Effect.die(new Error("unreachable")),
        });
        assert.strictEqual(again.id, root.id);
        assert.lengthOf(storage.commits, 1);
        yield* h.close;
      }),
    ),
  );

  it.effect("keeps root and conversation identity and state across reopen", () =>
    run(backend)(
      Effect.gen(function* () {
        const storage = yield* controlledStorage;
        const first = yield* openHarness(storage, [], { extensions: [tools("read")] });
        const root = yield* first.harness.ensureRoot();
        yield* root.configure({
          model: { provider: "anthropic", modelId: "claude" },
          cwd: "/repo",
        });
        const entry = yield* append(root, "hello");
        const child = yield* first.harness.createConversation(ownerless);
        const fork = yield* root.fork(entry.id, ownerless);
        const providerSessionIds = yield* Effect.forEach([root, child, fork], (conversation) =>
          first.session
            .snapshot(ProviderDoc, conversation.id)
            .pipe(Effect.map((state) => some(state)?.sessionId)),
        );
        // A fork must not inherit its parent's provider identity.
        for (const sessionId of providerSessionIds) assert.match(sessionId!, UUID_V7);
        assert.strictEqual(new Set(providerSessionIds).size, 3);
        yield* first.close;
        assert.isTrue((yield* Effect.exit(root.agent))._tag === "Failure");

        // The new process installs nothing: the stored choices survive, the tools do not resolve.
        const second = yield* openHarness(storage, []);
        const reopened = yield* second.harness.ensureRoot({
          init: () => Effect.die(new Error("unreachable")),
        });
        assert.strictEqual(reopened.id, ROOT_CONVERSATION_ID);
        const agent = yield* reopened.agent;
        assert.deepStrictEqual(agent.model, { provider: "anthropic", modelId: "claude" });
        assert.strictEqual(agent.cwd, "/repo");
        assert.deepStrictEqual(agent.tools, []);
        yield* pagesOfTwo(storage);
        assert.deepStrictEqual(yield* allEntries(reopened), ["hello"]);
        assert.strictEqual(some(yield* second.harness.conversation(child.id))?.id, child.id);
        assert.deepStrictEqual(
          yield* Effect.forEach([root.id, child.id, fork.id], (id) =>
            second.session
              .snapshot(ProviderDoc, id)
              .pipe(Effect.map((state) => some(state)?.sessionId)),
          ),
          providerSessionIds,
        );
        const reopenedFork = some(yield* second.harness.conversation(fork.id))!;
        assert.deepStrictEqual(yield* allEntries(reopenedFork), ["hello"]);
        assert.deepStrictEqual((yield* reopenedFork.agent).model, {
          provider: "anthropic",
          modelId: "claude",
        });
        assert.isTrue(Option.isNone(yield* second.harness.conversation(ConversationId.make(999))));
        yield* second.close;
      }),
    ),
  );

  it.effect("creates independent conversations atomically with init and rolls back failures", () =>
    run(backend)(
      Effect.gen(function* () {
        const storage = yield* controlledStorage;
        const h = yield* openHarness(storage, [], { extensions: [tools("read")] });
        const created = yield* h.harness.createConversation({
          ...ownerless,
          init: (id) =>
            Effect.gen(function* () {
              const tx = yield* Tx;
              yield* tx.appendEntry(id, { kind: "message", model: [user("seed")] });
            }),
        });
        assert.lengthOf(storage.commits, 1);
        assert.deepStrictEqual(yield* allEntries(created), ["seed"]);

        const before = storage.commits.length;
        assert.include(
          yield* failureMessage(
            h.harness.createConversation({
              ...ownerless,
              agent: { thinkingLevel: "high" },
              init: () => Effect.die(new Error("init failed")),
            }),
          ),
          "init failed",
        );
        assert.lengthOf(storage.commits, before);
        yield* h.close;
      }),
    ),
  );

  it.effect(
    "forks at a concrete entry with the as-of agent and applies agent and init overrides",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const storage = yield* controlledStorage;
          const legacy = tool("legacy");
          const h = yield* openHarness(storage, [], {
            extensions: [tools("read"), Extension.make({ name: "tool:legacy", tools: [legacy] })],
          });
          const root = yield* h.harness.ensureRoot();
          yield* root.configure({ thinkingLevel: "low" });
          const at = yield* append(root, "one");
          yield* root.configure({ thinkingLevel: "high", tools: [tool("read")] });
          yield* append(root, "two");

          const child = yield* root.fork(at.id, ownerless);
          assert.deepStrictEqual(some(yield* h.session.snapshot(AgentDoc, child.id)), {
            thinkingLevel: "low",
          });
          assert.deepStrictEqual(yield* allEntries(child), ["one"]);

          const overridden = yield* root.fork(at.id, {
            ...ownerless,
            agent: { thinkingLevel: "minimal", tools: [legacy] },
            init: (id) =>
              Effect.gen(function* () {
                const tx = yield* Tx;
                assert.strictEqual((yield* tx.doc(AgentDoc, id)).thinkingLevel, "minimal");
              }),
          });
          assert.deepStrictEqual(some(yield* h.session.snapshot(AgentDoc, overridden.id)), {
            thinkingLevel: "minimal",
            tools: ["legacy"],
          });
          assert.deepStrictEqual(some(yield* h.session.snapshot(AgentDoc, root.id)), {
            thinkingLevel: "high",
            tools: ["read"],
          });

          const unrelated = yield* h.harness.createConversation(ownerless);
          yield* root.fork(at.id, ownerless);
          assert.include(yield* failureMessage(unrelated.fork(at.id, ownerless)), "is not visible");
          yield* h.close;
        }),
      ),
  );

  it.effect(
    "paginates fork-aware history through deep ancestor caps and same-commit prefixes",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const storage = yield* controlledStorage;
          const h = yield* openHarness(storage, []);
          yield* pagesOfTwo(storage);
          const root = yield* h.harness.ensureRoot();
          yield* append(root, "r1");
          const [r2] = yield* root.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              return [
                yield* tx.appendEntry(root.id, { kind: "message", model: [user("r2")] }),
                yield* tx.appendEntry(root.id, { kind: "message", model: [user("r3")] }),
              ];
            }),
          );
          const child = yield* root.fork(r2!.id, ownerless);
          const c1 = yield* append(child, "c1");
          yield* append(child, "c2");
          const grandchild = yield* child.fork(c1.id, ownerless);
          yield* append(grandchild, "g1");

          assert.deepStrictEqual(yield* allEntries(root), ["r3", "r2", "r1"]);
          assert.deepStrictEqual(yield* allEntries(child), ["c2", "c1", "r2", "r1"]);
          assert.deepStrictEqual(yield* allEntries(grandchild), ["g1", "c1", "r2", "r1"]);
          assert.deepStrictEqual(yield* allEntries(grandchild, "ascending"), [
            "r1",
            "r2",
            "c1",
            "g1",
          ]);
          const bounded = Array.from(
            yield* Stream.runCollect(
              grandchild.entries(
                // SAFETY: a `conversationId` the query type excludes; the handle ignores it.
                { minEntryId: r2!.id, maxEntryId: c1.id, conversationId: root.id } as never,
              ),
            ),
          );
          assert.deepStrictEqual(
            bounded.map((entry) => entry.id),
            [c1.id, r2!.id],
          );
          assert.isTrue(Message.is(bounded[0]));
          assert.isFalse(Message.is(undefined));
          yield* h.close;
        }),
      ),
  );

  it.effect("binds commits and task creation to the conversation", () =>
    run(backend)(
      Effect.gen(function* () {
        const storage = yield* controlledStorage;
        const h = yield* openHarness(storage, []);
        const conversation = yield* h.harness.createConversation(ownerless);
        const work = Task.make({
          name: "test.work",
          version: 1,
          input: Schema.Struct({ n: Schema.Number }),
          checkpoint: Schema.Struct({ phase: Schema.Literal("run") }),
          result: Schema.Null,
          initial: () => ({ phase: "run" }),
          phases: { run: () => Effect.void },
          abort: () => Effect.void,
        });
        const taskId = yield* conversation.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            return yield* tx.createTask(work, { n: 1 }, { ownership: { kind: "conversation" } });
          }),
        );
        const record = yield* h.harness.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            return yield* tx.task(taskId);
          }),
        );
        assert.strictEqual(some(record)?.conversationId, conversation.id);
        assert.include(
          yield* failureMessage(
            h.harness.commit(
              Effect.gen(function* () {
                const tx = yield* Tx;
                return yield* tx.createTask(
                  work,
                  { n: 2 },
                  { ownership: { kind: "conversation" } },
                );
              }),
            ),
          ),
          "requires options.conversationId",
        );
        yield* h.close;
      }),
    ),
  );

  it.effect(
    "runs conversationCreated in every creating commit, after the built-ins and before agent and init",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const seen: Array<string> = [];
          const storage = yield* controlledStorage;
          const h = yield* openHarness(storage, [], {
            conversationCreated: (conversation) =>
              Effect.gen(function* () {
                const tx = yield* Tx;
                const agent = yield* tx.doc(AgentDoc, conversation.id);
                seen.push(
                  `${conversation.id}:${conversation.parent === undefined ? "new" : "fork"}:${agent.cwd}`,
                );
                const note = yield* tx.doc(NoteDoc, conversation.id);
                note.text ||= "created";
                if (agent.cwd === "/fail") return yield* Effect.die(new Error("no"));
              }),
          });
          const root = yield* h.harness.ensureRoot({
            agent: { cwd: "/root" },
            init: (id) =>
              Effect.gen(function* () {
                const tx = yield* Tx;
                seen.push(`init:${(yield* tx.doc(NoteDoc, id)).text}`);
              }),
          });
          // A raw creation in a commit, as in a tool, and a fork, which already has the asOf copies.
          const raw = yield* h.harness.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              return (yield* tx.createConversation(ownerless)).id;
            }),
          );
          const entry = yield* append(root, "hello");
          const fork = yield* root.fork(entry.id, ownerless);
          assert.deepStrictEqual(seen, [
            `${root.id}:new:undefined`,
            "init:created",
            `${raw}:new:undefined`,
            `${fork.id}:fork:/root`,
          ]);
          assert.deepStrictEqual(some(yield* h.session.snapshot(NoteDoc, raw)), {
            text: "created",
          });
          // A throw fails the creating commit.
          yield* root.configure({ cwd: "/fail" });
          yield* root.fork(entry.id, ownerless);
          const failing = yield* append(root, "after");
          assert.include(yield* failureMessage(root.fork(failing.id, ownerless)), "no");
          yield* h.close;
        }),
      ),
  );
});

describeEachBackend("Harness agent", (backend) => {
  it.effect("replaces whole fields, clears them with null, and leaves undefined fields alone", () =>
    run(backend)(
      Effect.gen(function* () {
        const read = tool("read");
        const bash = tool("bash");
        const edit = tool("edit");
        const storage = yield* controlledStorage;
        const h = yield* openHarness(storage, [], {
          extensions: [Extension.make({ name: "coding", tools: [read, bash, edit] })],
        });
        const root = yield* h.harness.ensureRoot();
        const stored = h.session.snapshot(AgentDoc, root.id).pipe(Effect.map(some));
        const offered = root.agent.pipe(
          Effect.map((agent) => agent.tools.map((each) => each.name)),
        );
        const initial = yield* root.agent;
        assert.strictEqual(initial.thinkingLevel, "off");
        assert.deepStrictEqual(initial.tools, [read, bash, edit]);
        assert.isUndefined(initial.model);

        yield* root.configure({
          model: { provider: "openai", modelId: "gpt" },
          thinkingLevel: "medium",
        });
        yield* root.configure({ model: undefined, instructions: "Be terse." });
        assert.deepStrictEqual(yield* stored, {
          model: { provider: "openai", modelId: "gpt" },
          thinkingLevel: "medium",
          instructions: "Be terse.",
        });
        yield* root.configure({ model: null, instructions: null });
        assert.deepStrictEqual(yield* stored, { thinkingLevel: "medium" });

        yield* root.configure({ tools: { remove: [edit] } });
        assert.deepStrictEqual(yield* offered, ["read", "bash"]);
        // A new filter replaces the old one: edit is offered again.
        yield* root.configure({ tools: { remove: [bash] } });
        assert.deepStrictEqual(yield* offered, ["read", "edit"]);
        yield* root.configure({ tools: [edit, read] });
        assert.deepStrictEqual(yield* offered, ["edit", "read"]);
        yield* root.configure({ tools: null });
        assert.deepStrictEqual(yield* offered, ["read", "bash", "edit"]);
        // Names are stored without checking the registry.
        yield* root.configure({ tools: [tool("missing"), read] });
        assert.deepStrictEqual((yield* stored)?.tools, ["missing", "read"]);
        assert.deepStrictEqual(yield* offered, ["read"]);
        yield* h.close;
      }),
    ),
  );

  it.effect(
    "gives conversations created through Tx their documents: empty, an owner copy, or the fork's as-of copy",
    () =>
      run(backend)(
        Effect.gen(function* () {
          const storage = yield* controlledStorage;
          const h = yield* openHarness(storage, [], { extensions: [tools("read")] });
          const root = yield* h.harness.ensureRoot({
            agent: {
              model: { provider: "faux", modelId: "m" },
              instructions: "Main role.",
              cwd: "/repo",
            },
          });
          const owner = Task.make({
            name: "test.owner",
            version: 1,
            input: Schema.Struct({}),
            checkpoint: Schema.Struct({ phase: Schema.Literal("never") }),
            result: Schema.Null,
            initial: () => ({ phase: "never" }),
            phases: { never: () => Effect.void },
            abort: () => Effect.void,
          });
          const ids = yield* root.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              const taskId = yield* tx.createTask(
                owner,
                {},
                { ownership: { kind: "conversation" } },
              );
              const plain = yield* tx.createConversation(ownerless);
              const owned = yield* tx.createConversation({ ownership: { kind: "task", taskId } });
              // The copy exists when createConversation() returns, so a configure() in the same callback
              // overrides it.
              assert.strictEqual((yield* tx.doc(AgentDoc, owned.id)).cwd, "/repo");
              yield* configure(owned.id, { cwd: "/worktree" });
              return { taskId, plain: plain.id, owned: owned.id };
            }),
          );
          const providerOf = (id: ConversationId) =>
            h.session.snapshot(ProviderDoc, id).pipe(Effect.map((state) => some(state)?.sessionId));
          assert.deepStrictEqual(some(yield* h.session.snapshot(AgentDoc, ids.plain)), {});
          assert.deepStrictEqual(some(yield* h.session.snapshot(LiveDoc, ids.plain)), {});
          assert.match((yield* providerOf(ids.plain))!, UUID_V7);
          assert.notStrictEqual(yield* providerOf(ids.owned), yield* providerOf(root.id));
          assert.deepStrictEqual(some(yield* h.session.snapshot(AgentDoc, ids.owned)), {
            model: { provider: "faux", modelId: "m" },
            instructions: "Main role.",
            cwd: "/worktree",
          });

          // A later owner change does not reach the child.
          yield* root.configure({ thinkingLevel: "high" });
          assert.isUndefined(some(yield* h.session.snapshot(AgentDoc, ids.owned))?.thinkingLevel);

          // A task-owned fork keeps its as-of copy of its fork parent, not its owner's agent.
          const at = yield* root.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              return (yield* tx.appendEntry(ids.plain, { kind: "note" })).id;
            }),
          );
          const fork = yield* root.commit(
            Effect.gen(function* () {
              const tx = yield* Tx;
              return (yield* tx.forkConversation(ids.plain, at, {
                ownership: { kind: "task", taskId: ids.taskId },
              })).id;
            }),
          );
          assert.deepStrictEqual(some(yield* h.session.snapshot(AgentDoc, fork)), {});
          assert.deepStrictEqual(some(yield* h.session.snapshot(LiveDoc, fork)), {});
          assert.notStrictEqual(yield* providerOf(fork), yield* providerOf(ids.plain));
          yield* h.close;
        }),
      ),
  );

  it.effect("reads an absent agent for conversations a plain Session created without writing", () =>
    run(backend)(
      Effect.gen(function* () {
        const storage = yield* controlledStorage;
        const id = yield* withSession(storage, createConversation);
        const h = yield* openHarness(storage, [], { extensions: [tools("read")] });
        const raw = some(yield* h.harness.conversation(id))!;
        assert.isTrue(Option.isNone(yield* h.session.snapshot(LiveDoc, id)));
        const commits = storage.commits.length;
        const agent = yield* raw.agent;
        assert.strictEqual(agent.thinkingLevel, "off");
        assert.deepStrictEqual(
          agent.tools.map((each) => each.name),
          ["read"],
        );
        assert.lengthOf(storage.commits, commits);
        yield* raw.configure({ thinkingLevel: "low" });
        assert.deepStrictEqual(some(yield* h.session.snapshot(AgentDoc, id)), {
          thinkingLevel: "low",
        });
        yield* h.close;
      }),
    ),
  );
});

describeEachBackend("Harness lifecycle", (backend) => {
  it.effect("returns stateless handles and rejects operations after close", () =>
    run(backend)(
      Effect.gen(function* () {
        const storage = yield* controlledStorage;
        const h = yield* openHarness(storage, []);
        const root = yield* h.harness.ensureRoot();
        const again = yield* h.harness.ensureRoot();
        assert.notStrictEqual(again, root);
        assert.strictEqual(again.id, root.id);
        yield* h.close;
        assert.include(yield* failureMessage(h.harness.ensureRoot()), "is closed");
        assert.include(yield* failureMessage(h.harness.createConversation(ownerless)), "is closed");
        assert.include(yield* failureMessage(h.harness.conversation(root.id)), "is closed");
      }),
    ),
  );

  it.effect("forwards generic Session document APIs", () =>
    run(backend)(
      Effect.gen(function* () {
        const storage = yield* controlledStorage;
        const h = yield* openHarness(storage, []);
        const root = yield* h.harness.ensureRoot();
        const entry = yield* root.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            (yield* tx.doc(NoteDoc, root.id)).text = "first";
            return yield* tx.appendEntry(root.id, { kind: "message", model: [user("m")] });
          }),
        );
        yield* h.harness.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            (yield* tx.doc(NoteDoc, root.id)).text = "second";
          }),
        );
        assert.deepStrictEqual(some(yield* h.session.snapshot(NoteDoc, root.id)), {
          text: "second",
        });
        assert.deepStrictEqual(some(yield* h.session.snapshotAsOf(NoteDoc, root.id, entry.id)), {
          text: "first",
        });
        const watch = yield* acquire(h.session.watchDoc(NoteDoc, root.id));
        assert.deepStrictEqual(watch.value.value, { text: "second" });
        yield* watch.release;
        yield* h.close;
      }),
    ),
  );
});

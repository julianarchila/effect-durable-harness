# effect-durable-harness

A framework for durable agents, written in [Effect](https://effect.website) and deployed with [Alchemy](https://alchemy.run). An agent's state lives in a Session, and every step it takes (a model request, a tool call, a background task) is committed before the next one starts.

- **Durable.** A crash, deploy or eviction resumes the agent from its last commit.
- **Effect-native.** Tools and tasks are `Effect`s, so a missing service is a compile error.
- **Alchemy-native.** The agent and its infrastructure are TypeScript in one repo.
- **Portable.** The same agent runs in a Cloudflare Durable Object or a Rivet actor (experimental), over any SQL storage.
- **Composable.** An agent is a set of extensions you can mix and write yourself.

## Inspiration

The harness core is heavily inspired by [`@earendil-works/pi-durable`](https://github.com/earendil-works/pi/tree/main/packages/durable) and [vercel-labs/fx](https://github.com/vercel-labs/fx).

## Packages

| Package                              | What it is                                                                                 |
| ------------------------------------ | ------------------------------------------------------------------------------------------ |
| `@effect-durable-harness/core`       | The kernel. Host-agnostic: it imports nothing from Cloudflare, Alchemy or any runtime.     |
| `@effect-durable-harness/delta`      | Immutable JSON revisions and op batches, used by documents.                                |
| `@effect-durable-harness/host`       | The engine-neutral Durable Object host, and the browser client (`SessionClient`, `react`). |
| `@effect-durable-harness/cloudflare` | The host bound to Cloudflare Durable Objects.                                              |
| `@effect-durable-harness/rivet`      | The host bound to Rivet actors. Experimental.                                              |
| `@effect-durable-harness/opencode`   | The `Models` service on `@opencode/ai`.                                                    |

## Example

The snippets below are trimmed from [`examples/cloudflare-chat`](./examples/cloudflare-chat).

A tool declares its parameters with `Schema`. Its handler is a `Layer`:

```ts
import * as AgentTool from "@effect-durable-harness/core/AgentTool";
import * as Effect from "effect/Effect";
import * as Random from "effect/Random";
import * as Schema from "effect/Schema";

export const RollDice = AgentTool.make("roll_dice", {
  description: "Roll dice slowly, for games.",
  parameters: Schema.Struct({ sides: Schema.optional(Schema.Int) }),
  details: Schema.Struct({ rolled: Schema.Int }),
});

export const RollDiceLive = RollDice.toLayer(({ sides }, call) =>
  Effect.gen(function* () {
    yield* call.output("Rolling…\n");
    const rolled = yield* Random.nextIntBetween(1, (sides ?? 6) + 1);
    return AgentTool.text(`Rolled ${rolled}`, { details: { rolled } });
  }),
);
```

A task's phases end in a transition. Here `wait` sleeps until a time with no invocation running, and `deliver` submits an input to the conversation when the alarm fires:

```ts
export const DeliverScheduled = Task.make("chat.deliver-scheduled", {
  version: 1,
  input: Scheduled,
  checkpoint: Schema.Union([
    Schema.Struct({ phase: Schema.Literal("wait") }),
    Schema.Struct({ phase: Schema.Literal("deliver") }),
  ]),
  result: Schema.Null,
  initial: () => ({ phase: "wait" }),
});

export const DeliverScheduledLive = DeliverScheduled.toLayer({
  phases: {
    wait: (task, run) => run.sleepUntil(task.input.runAt, { phase: "deliver" }),
    deliver: (task, run) =>
      Effect.gen(function* () {
        const conversation = yield* run.conversation(
          ConversationId.make(task.input.conversationId),
        );
        if (Option.isNone(conversation)) return yield* run.complete(null);
        // The request ID makes a rerun after a crash get the same submission back.
        const submission = yield* conversation.value.submit({
          type: "input",
          content: task.input.instruction,
          requestId: task.input.id,
          whenBusy: "followUp",
        });
        yield* submission.await;
        return yield* run.complete(null);
      }),
  },
  abort: (_task, run) => run.aborted,
});
```

Tools, tasks and system prompt sections are grouped into an extension, and the harness lists its extensions:

```ts
export const Chat = Extension.make("chat", {
  tasks: [DeliverScheduled],
  tools: [RollDice, Schedule],
  sections: [Preamble],
});

export const ChatHarness = Harness.make("chat", { extensions: [Chat] });
```

The Cloudflare host runs the harness in a Durable Object. The handlers and the models are provided as layers, and the object's SQLite is its storage:

```ts
export class Session extends Cloudflare.DurableObject<Session, SessionMethods>()("Session", {
  errors: Host.errors,
}) {}

export const SessionLive = Session.make(
  Effect.gen(function* () {
    const agent = yield* Host.agent(ChatAgent, { socket: chatSocket });
    return Effect.gen(function* () {
      const host = yield* agent.start;
      yield* host.harness.ensureRoot().pipe(Effect.orDie);
      return host.methods<SessionMethods>({
        /* … */
      });
    });
  }),
).pipe(Layer.provide(ChatLive), Layer.provide(ModelsLive));
```

## Examples

- [`examples/cloudflare-chat`](./examples/cloudflare-chat): a multiplayer chat with a React app, one `Session` object per chat. Start here.
- [`examples/cloudflare-followup`](./examples/cloudflare-followup): an assistant that replies through an external webhook and sends a follow-up after hours of silence.
- [`examples/cloudflare-support`](./examples/cloudflare-support): a support agent whose tools use D1, KV and a second Worker. The reference for composing an object with `Host.agent`.
- [`examples/rivet-chat`](./examples/rivet-chat): the chat example on Rivet.

Each example's tests run in workerd with a fake model, so they need no API key.

## Run it

Requires Bun 1.3 or later.

```sh
bun install
bun run test                  # the four gates are in AGENTS.md
CHAT_MODEL=faux bun run dev   # the chat example on a scripted model; needs an Alchemy profile, no API key
```

## Documentation

- [`AGENTS.md`](./AGENTS.md): principles, layout and commands.
- [`DESIGN.md`](./DESIGN.md): the kernel's design decisions and their reasons.
- [`CODING_STANDARDS.md`](./CODING_STANDARDS.md): how the code is written.
- [`packages/durable/AGENTS.md`](./packages/durable/AGENTS.md): the kernel's concurrency rules.

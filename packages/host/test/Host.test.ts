// The engine-neutral host over `TestEngine`, in Node: the alarm policy that every engine shares (armed before a
// commit that leaves a live task, set afresh when an instance starts, capped by the engine), the instance's lifecycle
// (`Engine.onStop`), and the session socket. The engines' own suites (`packages/cloudflare` on workerd,
// `packages/rivet` on a local Rivet) run the same behaviours on the real platforms.
import { assert, describe, it } from "@effect/vitest";
import * as Extension from "@effect-durable-harness/core/Extension";
import * as Harness from "@effect-durable-harness/core/Harness";
import * as Signal from "@effect-durable-harness/core/Signal";
import * as Task from "@effect-durable-harness/core/Task";
import { TaskId } from "@effect-durable-harness/core/Ids";
import { Tx } from "@effect-durable-harness/core/Tx";
import * as FauxModel from "@effect-durable-harness/core/testing/FauxModel";
import * as Clock from "effect/Clock";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as TestClock from "effect/testing/TestClock";
import * as HttpServerRequest from "effect/http/HttpServerRequest";
import * as Host from "../src/Host.ts";
import { decodeServerEvent } from "../src/SessionProtocol.ts";
import {
  adapter,
  makeWorld,
  runtimeContext,
  stop,
  TestObject,
  type FakeSocket,
  type World,
} from "./support/TestEngine.ts";

const KEEP_ALIVE_MS = 1_000;

/** Waits for `release` in a phase of its own, then completes: active work for as long as the test wants. */
const Hold = Task.make({
  name: "test.hold",
  version: 1,
  input: Schema.Struct({ ms: Schema.Number }),
  checkpoint: Schema.Struct({ phase: Schema.Literal("hold") }),
  result: Schema.Null,
  initial: () => ({ phase: "hold" }),
  phases: {
    hold: (task, runtime) =>
      Effect.gen(function* () {
        yield* Effect.sleep(task.input.ms);
        yield* runtime.commit(() =>
          Effect.succeed({
            status: "terminal",
            outcome: { status: "completed", result: null },
          } as const),
        );
      }),
  },
  abort: (_task, runtime) =>
    runtime.commit(() =>
      Effect.succeed({ status: "terminal", outcome: { status: "aborted" } } as const),
    ),
});

const Answer = Signal.make("test.answer", { version: 1, schema: Schema.String });

/** Sleeps until an answer arrives under `answer`, with no deadline, then completes. */
const AwaitAnswer = Task.make({
  name: "test.await",
  version: 1,
  input: Schema.Null,
  checkpoint: Schema.Struct({ phase: Schema.Literals(["sleep", "answered"]) }),
  result: Schema.Null,
  initial: () => ({ phase: "sleep" }),
  phases: {
    sleep: (_task, runtime) =>
      runtime.sleep({ checkpoint: { phase: "answered" }, signals: [Answer.key("answer")] }),
    answered: (_task, runtime) => runtime.complete(null),
  },
  abort: (_task, runtime) => runtime.aborted,
});

/** Sleeps until `at` with `sleepUntil`: no invocation while it sleeps. */
const Nap = Task.make({
  name: "test.nap",
  version: 1,
  input: Schema.Struct({ at: Schema.Number }),
  checkpoint: Schema.Struct({ phase: Schema.Literals(["sleep", "woke"]) }),
  result: Schema.Null,
  initial: () => ({ phase: "sleep" }),
  phases: {
    sleep: (task, runtime) => runtime.sleepUntil(task.input.at, { phase: "woke" }),
    woke: (_task, runtime) => runtime.complete(null),
  },
  abort: (_task, runtime) => runtime.aborted,
});

/** Creates a `test.nap` child, then waits for it in memory in its phase: no timer or I/O of its own. */
const AwaitNap = Task.make({
  name: "test.await-nap",
  version: 1,
  input: Schema.Struct({ ms: Schema.Number }),
  checkpoint: Schema.Union([
    Schema.Struct({ phase: Schema.Literal("spawn") }),
    Schema.Struct({ phase: Schema.Literal("wait"), child: Schema.Number }),
  ]),
  result: Schema.Null,
  initial: () => ({ phase: "spawn" }),
  phases: {
    spawn: (task, runtime) =>
      Effect.gen(function* () {
        const at = (yield* Clock.currentTimeMillis) + task.input.ms;
        yield* runtime.commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            const child = yield* tx.createTask(
              Nap,
              { at },
              { ownership: { kind: "task", taskId: task.id } },
            );
            return Task.next({ phase: "wait" as const, child });
          }),
        );
      }),
    wait: (task, runtime) =>
      Effect.gen(function* () {
        // SAFETY: the checkpoint stores the ID `createTask` returned.
        yield* runtime.awaitTask(TaskId.make(task.state.checkpoint.child));
        yield* runtime.complete(null);
      }),
  },
  abort: (_task, runtime) => runtime.aborted,
});

const agentLayer = Harness.layer({
  extensions: [Extension.make({ name: "test", tasks: [Hold, AwaitAnswer, Nap, AwaitNap] })],
}).pipe(Layer.provide(FauxModel.make().layer));

const agent = Host.bindAgent<TestObject, FakeSocket>(adapter);

const User = Schema.Struct({ username: Schema.String });

/** Starts one instance over `world` in `scope`; closing the scope evicts it. */
const start = <C>(world: World, scope: Scope.Scope, options: Host.AgentOptions<C, never> = {}) =>
  Effect.gen(function* () {
    const hosted = yield* agent(agentLayer, {
      storage: { keepAliveMs: KEEP_ALIVE_MS },
      ...options,
    });
    return yield* hosted.start.pipe(
      Effect.provide(Layer.mergeAll(Layer.succeed(TestObject, world), runtimeContext)),
      Scope.provide(scope),
    );
  });

const createHold = (harness: Harness.Harness["Service"], ms: number) =>
  Effect.gen(function* () {
    const root = yield* harness.ensureRoot({}).pipe(Effect.orDie);
    return yield* root
      .commit(
        Effect.gen(function* () {
          const tx = yield* Tx;
          return yield* tx.createTask(Hold, { ms }, { ownership: { kind: "conversation" } });
        }),
      )
      .pipe(Effect.orDie);
  });

/**
 * Waits until the kernel reports no active work (open's reconcile pass has run) and the host applied that wake, at
 * most `rounds` scheduler turns.
 */
const quiet = (harness: Harness.Harness["Service"], rounds = 200) =>
  Effect.gen(function* () {
    for (let round = 0; round < rounds; round++) {
      const wake = yield* harness.wake.pipe(Effect.orDie);
      yield* Effect.yieldNow;
      if (!wake.activeWork) return;
    }
    return yield* Effect.die(new Error(`the kernel was still active after ${rounds} turns`));
  });

/** Waits until the kernel reports active work (an invocation running), at most `rounds` scheduler turns. */
const running = (harness: Harness.Harness["Service"], rounds = 200) =>
  Effect.gen(function* () {
    for (let round = 0; round < rounds; round++) {
      const wake = yield* harness.wake.pipe(Effect.orDie);
      if (wake.activeWork) return;
      yield* Effect.yieldNow;
    }
    return yield* Effect.die(new Error(`the kernel was not active after ${rounds} turns`));
  });

/** Turns the scheduler until `done` holds, at most `rounds` turns; answers whether it held. */
const turnsUntil = (done: () => boolean, rounds = 200) =>
  Effect.gen(function* () {
    for (let round = 0; round < rounds && !done(); round++) yield* Effect.yieldNow;
    return done();
  });

/** Advances the clock in steps until task `id` ends, at most `steps` of 200 ms. */
const finish = <R>(harness: Harness.Harness["Service"], id: TaskId<R>, steps = 30) =>
  Effect.gen(function* () {
    const waiting = yield* Effect.forkChild(harness.awaitTask(id).pipe(Effect.orDie));
    for (let step = 0; step < steps && waiting.pollUnsafe() === undefined; step++) {
      yield* TestClock.adjust(200);
      yield* Effect.yieldNow;
    }
    return yield* Fiber.join(waiting);
  });

/** Yields until the host applied a wake that holds the instance (`awake`) or lets it go, at most `rounds` turns. */
const awake = (world: World, expected: boolean, rounds = 200) =>
  Effect.gen(function* () {
    for (let round = 0; round < rounds && world.awake !== expected; round++) yield* Effect.yieldNow;
    assert.strictEqual(world.awake, expected);
  });

describe("Host", () => {
  it.effect("a fresh instance deletes an alarm it cannot trust when nothing is due", () =>
    Effect.gen(function* () {
      const world = yield* makeWorld();
      // An alarm an earlier activation stored and the engine may never fire (Rivet).
      world.alarm = 123_456;
      const scope = yield* Scope.make();
      const instance = yield* start(world, scope);
      yield* quiet(instance.harness);
      assert.isUndefined(world.alarm);
      yield* Scope.close(scope, Exit.void);
    }),
  );

  it.effect("a commit that leaves a live task arms the alarm before the task is stored", () =>
    Effect.gen(function* () {
      const world = yield* makeWorld();
      const scope = yield* Scope.make();
      const instance = yield* start(world, scope);
      yield* quiet(instance.harness);
      const before = world.alarmWrites.length;
      const now = yield* Clock.currentTimeMillis;
      yield* createHold(instance.harness, 60_000);
      // The first write after the commit began found no task stored yet: it came before the commit.
      assert.deepStrictEqual(world.alarmWrites[before], {
        at: now + KEEP_ALIVE_MS,
        storedTasks: 0,
        liveTasks: 0,
        signalledTasks: 0,
      });
      assert.isAtMost(world.alarm ?? Infinity, now + KEEP_ALIVE_MS);
      yield* Scope.close(scope, Exit.void);
    }),
  );

  it.effect(
    "a fresh instance sets the keep-alive anew while work is active, whatever is stored",
    () =>
      Effect.gen(function* () {
        const world = yield* makeWorld();
        const first = yield* Scope.make();
        const instance = yield* start(world, first);
        yield* createHold(instance.harness, 60_000);
        assert.isDefined(world.alarm);
        yield* Scope.close(first, Exit.void);
        // The stored alarm is lost, as when Rivet stops between a commit and its schedule.
        world.alarm = undefined;
        const second = yield* Scope.make();
        yield* start(world, second);
        const now = yield* Clock.currentTimeMillis;
        assert.isDefined(world.alarm);
        assert.isAtMost(world.alarm ?? Infinity, now + KEEP_ALIVE_MS);
        yield* Scope.close(second, Exit.void);
      }),
  );

  it.effect("the keep-alive never exceeds the engine's limit", () =>
    Effect.gen(function* () {
      const world = yield* makeWorld({ maxKeepAliveMs: 500 });
      const scope = yield* Scope.make();
      const instance = yield* start(world, scope);
      const now = yield* Clock.currentTimeMillis;
      yield* createHold(instance.harness, 60_000);
      assert.isAtMost(world.alarm ?? Infinity, now + 500);
      yield* Scope.close(scope, Exit.void);
    }),
  );

  it.effect("the instance is held in memory while work is active, and let go once it is not", () =>
    Effect.gen(function* () {
      const world = yield* makeWorld();
      const scope = yield* Scope.make();
      const instance = yield* start(world, scope);
      yield* quiet(instance.harness);
      assert.isFalse(world.awake);
      const id = yield* createHold(instance.harness, 2_000);
      yield* awake(world, true);
      const waiting = yield* Effect.forkChild(instance.harness.awaitTask(id).pipe(Effect.orDie));
      for (let step = 0; step < 30 && waiting.pollUnsafe() === undefined; step++) {
        // Held for the whole wait, while the phase sleeps with no event reaching the instance.
        assert.isTrue(world.awake);
        yield* TestClock.adjust(200);
        yield* Effect.yieldNow;
      }
      yield* Fiber.join(waiting);
      yield* quiet(instance.harness);
      yield* awake(world, false);
      yield* Scope.close(scope, Exit.void);
    }),
  );

  it.effect("a phase waiting in memory for a sleeping task holds the instance until it ends", () =>
    Effect.gen(function* () {
      const world = yield* makeWorld();
      const scope = yield* Scope.make();
      const instance = yield* start(world, scope);
      const root = yield* instance.harness.ensureRoot({}).pipe(Effect.orDie);
      const id = yield* root
        .commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            return yield* tx.createTask(
              AwaitNap,
              { ms: 2_000 },
              { ownership: { kind: "conversation" } },
            );
          }),
        )
        .pipe(Effect.orDie);
      yield* awake(world, true);
      const waiting = yield* Effect.forkChild(instance.harness.awaitTask(id).pipe(Effect.orDie));
      // The child sleeps durably and the waiter holds no timer: only the engine's alarm wakes the child.
      for (let step = 0; step < 30 && waiting.pollUnsafe() === undefined; step++) {
        assert.isTrue(world.awake);
        yield* TestClock.adjust(200);
        if ((world.alarm ?? Infinity) <= (yield* Clock.currentTimeMillis)) {
          yield* instance.methods().alarm();
        }
        yield* Effect.yieldNow;
      }
      const record = yield* Fiber.join(waiting);
      assert.strictEqual(record.state.status, "terminal");
      yield* quiet(instance.harness);
      yield* awake(world, false);
      yield* Scope.close(scope, Exit.void);
    }),
  );

  it.effect("ending the instance lets it go while work is still active", () =>
    Effect.gen(function* () {
      const world = yield* makeWorld();
      const scope = yield* Scope.make();
      const instance = yield* start(world, scope);
      yield* createHold(instance.harness, 60_000);
      yield* awake(world, true);
      yield* stop(world);
      // The engine's stop ends the instance's Session and scope, and the hold with them.
      assert.isFalse(world.awake);
      yield* Scope.close(scope, Exit.void);
      assert.isFalse(world.awake);
    }),
  );

  it.effect(
    "an instance that starts with work already active is held without a call reaching it",
    () =>
      Effect.gen(function* () {
        const world = yield* makeWorld();
        const first = yield* Scope.make();
        const before = yield* start(world, first);
        yield* createHold(before.harness, 60_000);
        yield* awake(world, true);
        yield* stop(world);
        yield* Scope.close(first, Exit.void);
        assert.isFalse(world.awake);
        // The next instance resumes the task with no call reaching it: the wake reported once its invocation runs
        // holds it.
        const second = yield* Scope.make();
        const after = yield* start(world, second);
        yield* awake(world, true);
        const wake = yield* after.harness.wake.pipe(Effect.orDie);
        assert.isTrue(wake.activeWork);
        yield* Scope.close(second, Exit.void);
      }),
  );

  it.effect("the engine stopping the instance closes its Session", () =>
    Effect.gen(function* () {
      const world = yield* makeWorld();
      const scope = yield* Scope.make();
      const instance = yield* start(world, scope);
      assert.strictEqual(world.stops.length, 1);
      yield* stop(world);
      const end = yield* instance.harness.closed;
      assert.notStrictEqual(end.reason, "failed");
      yield* Scope.close(scope, Exit.void);
    }),
  );

  it.effect("work cut by a stop resumes in the next instance", () =>
    Effect.gen(function* () {
      const world = yield* makeWorld();
      const first = yield* Scope.make();
      const before = yield* start(world, first);
      const id = yield* createHold(before.harness, 2_000);
      yield* stop(world);
      yield* Scope.close(first, Exit.void);
      const second = yield* Scope.make();
      const after = yield* start(world, second);
      const waiting = yield* Effect.forkChild(after.harness.awaitTask(id).pipe(Effect.orDie));
      for (let step = 0; step < 30 && waiting.pollUnsafe() === undefined; step++) {
        yield* TestClock.adjust(200);
        yield* Effect.yieldNow;
      }
      const record = yield* Fiber.join(waiting);
      assert.strictEqual(record.state.status, "terminal");
      yield* Scope.close(second, Exit.void);
    }),
  );

  it.effect("the session socket greets a connection and reaches it from the instance", () =>
    Effect.gen(function* () {
      const world = yield* makeWorld();
      const scope = yield* Scope.make();
      const instance = yield* start(world, scope, {
        socket: Host.socket({
          connection: User,
          accept: () => Effect.succeed({ username: "ana" }),
        }),
      });
      const served = instance.methods();
      const response = yield* served.fetch.pipe(
        Effect.provideService(
          HttpServerRequest.HttpServerRequest,
          HttpServerRequest.fromWeb(
            new Request("http://object/", { headers: { upgrade: "websocket" } }),
          ),
        ),
      );
      assert.strictEqual(response.status, 101);
      const socket = world.sockets[0];
      assert.isDefined(socket);
      const types = (socket?.sent ?? []).map((frame) => {
        const decoded = decodeServerEvent(frame);
        return decoded._tag === "Success" ? decoded.success.type : "invalid";
      });
      assert.deepStrictEqual(types, ["hello", "session", "activity"]);
      const connections = yield* instance.socket.connections;
      assert.deepStrictEqual(
        connections.map((connection) => connection.connection),
        [{ username: "ana" }],
      );
      assert.strictEqual(yield* instance.socket.send(() => true, { text: "hi" }), 1);
      yield* Scope.close(scope, Exit.void);
    }),
  );

  it.effect("an instance whose start fails leaves an alarm, so the engine starts it again", () =>
    Effect.gen(function* () {
      const world = yield* makeWorld();
      const first = yield* Scope.make();
      const before = yield* start(world, first);
      yield* createHold(before.harness, 60_000);
      // Every wake reported so far is applied.
      yield* turnsUntil(() => false);
      yield* Scope.close(first, Exit.void);
      // The live task no longer decodes, so opening the Harness fails; its alarm is gone, as after it fired.
      yield* world.sql`UPDATE durable_tasks SET record = '{}'`.pipe(Effect.orDie);
      world.alarm = undefined;
      const second = yield* Scope.make();
      const now = yield* Clock.currentTimeMillis;
      const exit = yield* Effect.exit(start(world, second));
      assert.isTrue(Exit.isFailure(exit));
      assert.isDefined(world.alarm);
      assert.isAtMost(world.alarm ?? Infinity, now + KEEP_ALIVE_MS);
      yield* Scope.close(second, Exit.void);
    }),
  );

  it.effect("an alarm write that fails while the wake is reported resets the object", () =>
    Effect.gen(function* () {
      const world = yield* makeWorld();
      const scope = yield* Scope.make();
      const instance = yield* start(world, scope);
      const id = yield* createHold(instance.harness, 1_000);
      yield* running(instance.harness);
      // The alarm firing applies a wake that knows every commit so far.
      yield* instance.methods().alarm();
      // The task's completion writes no live task, so only the wake's report writes the alarm: it deletes it.
      world.failAlarmWrites = true;
      yield* finish(instance.harness, id);
      assert.isTrue(yield* turnsUntil(() => world.aborts.length > 0));
      yield* Scope.close(scope, Exit.void);
    }),
  );

  it.effect("a wake reported before a commit that leaves a live task never clears its alarm", () =>
    Effect.gen(function* () {
      const world = yield* makeWorld();
      const scope = yield* Scope.make();
      const instance = yield* start(world, scope);
      const first = yield* createHold(instance.harness, 1_000);
      // Every wake reported so far is applied.
      yield* turnsUntil(() => false);
      // The idle wake that follows the task's completion holds at its read of the alarm.
      const hold = {
        entered: yield* Deferred.make<void>(),
        release: yield* Deferred.make<void>(),
      };
      world.holdAlarmGet = hold;
      yield* finish(instance.harness, first);
      assert.isTrue(yield* turnsUntil(() => Deferred.isDoneUnsafe(hold.entered)));
      // A commit that leaves a live task runs meanwhile, then the idle wake goes on.
      const second = yield* Effect.forkChild(createHold(instance.harness, 60_000));
      yield* turnsUntil(() => second.pollUnsafe() !== undefined);
      yield* Deferred.succeed(hold.release, undefined);
      yield* Fiber.join(second);
      yield* turnsUntil(() => false, 50);
      const cleared = world.alarmWrites.filter(
        (write) => write.at === undefined && write.liveTasks > 0,
      );
      assert.deepStrictEqual(cleared, []);
      assert.isDefined(world.alarm);
      yield* Scope.close(scope, Exit.void);
    }),
  );

  it.effect("a signal that wakes a sleeping task arms the alarm before it is stored", () =>
    Effect.gen(function* () {
      const world = yield* makeWorld();
      const scope = yield* Scope.make();
      const instance = yield* start(world, scope);
      const root = yield* instance.harness.ensureRoot({}).pipe(Effect.orDie);
      const id = yield* root
        .commit(
          Effect.gen(function* () {
            const tx = yield* Tx;
            return yield* tx.createTask(AwaitAnswer, null, {
              ownership: { kind: "conversation" },
            });
          }),
        )
        .pipe(Effect.orDie);
      // Asleep on its signal only: nothing is due, so the alarm is cleared.
      yield* quiet(instance.harness);
      // The alarm firing applies a wake that knows the sleep: nothing is due, so the alarm is cleared.
      yield* instance.methods().alarm();
      assert.isUndefined(world.alarm);
      const before = world.alarmWrites.length;
      const now = yield* Clock.currentTimeMillis;
      const result = yield* instance.harness.signal(id, Answer, "answer", "yes").pipe(Effect.orDie);
      assert.strictEqual(result, "delivered");
      // The first write after the delivery began found the signal not yet stored: it came before the commit.
      assert.deepStrictEqual(world.alarmWrites[before], {
        at: now + KEEP_ALIVE_MS,
        storedTasks: 1,
        liveTasks: 1,
        signalledTasks: 0,
      });
      const record = yield* instance.harness.awaitTask(id).pipe(Effect.orDie);
      assert.strictEqual(record.state.status, "terminal");
      yield* Scope.close(scope, Exit.void);
    }),
  );
});

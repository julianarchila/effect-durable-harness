// The host's alarm policy: a pure function of the kernel's wake, the clock and
// the alarm the object has, so the workerd suite only has to check that the host applies it.
import { assert, describe, it } from "@effect/vitest";
import type { StorageWrite } from "@effect-durable-harness/core/AgentStorage";
import { ConversationId, DocumentId, TaskId } from "@effect-durable-harness/core/Ids";
import * as Option from "effect/Option";
import { earlyAlarm, plan, planUnknown, writesLiveTask } from "../src/internal/alarm.ts";

const keepAliveMs = 30_000;
const now = 1_000_000;

const wake = (nextWakeAt: number | undefined, activeWork: boolean) => ({
  nextWakeAt: Option.fromUndefinedOr(nextWakeAt),
  activeWork,
});

describe("plan", () => {
  it("arms the keep-alive while there is active work and no alarm", () => {
    assert.deepStrictEqual(
      plan({ wake: wake(undefined, true), now, current: Option.none(), keepAliveMs }),
      {
        _tag: "Set",
        at: now + keepAliveMs,
      },
    );
  });

  it("does not push a keep-alive that is already armed: commits do not move it", () => {
    const current = Option.some(now + 10_000);
    assert.deepStrictEqual(plan({ wake: wake(undefined, true), now, current, keepAliveMs }), {
      _tag: "Keep",
    });
  });

  it("keeps an alarm already due", () => {
    const current = Option.some(now - 5);
    assert.deepStrictEqual(plan({ wake: wake(undefined, true), now, current, keepAliveMs }), {
      _tag: "Keep",
    });
  });

  it("brings a later alarm forward to the keep-alive while there is active work", () => {
    const current = Option.some(now + 3_600_000);
    assert.deepStrictEqual(plan({ wake: wake(now + 3_600_000, true), now, current, keepAliveMs }), {
      _tag: "Set",
      at: now + keepAliveMs,
    });
  });

  it("arms a sleep's deadline before the keep-alive while there is active work", () => {
    assert.deepStrictEqual(
      plan({ wake: wake(now + 2_000, true), now, current: Option.some(now + 10_000), keepAliveMs }),
      { _tag: "Set", at: now + 2_000 },
    );
    assert.deepStrictEqual(
      plan({ wake: wake(now + 2_000, true), now, current: Option.some(now + 1_000), keepAliveMs }),
      { _tag: "Keep" },
    );
  });

  it("treats work due now as the keep-alive: the scheduler in memory runs it", () => {
    assert.deepStrictEqual(
      plan({ wake: wake(now, false), now, current: Option.none(), keepAliveMs }),
      {
        _tag: "Set",
        at: now + keepAliveMs,
      },
    );
    assert.deepStrictEqual(
      plan({ wake: wake(now - 1, false), now, current: Option.some(now + 1_000), keepAliveMs }),
      { _tag: "Keep" },
    );
  });

  it("sets the alarm at the next deadline when the Session only sleeps", () => {
    assert.deepStrictEqual(
      plan({
        wake: wake(now + 3_600_000, false),
        now,
        current: Option.some(now + 20_000),
        keepAliveMs,
      }),
      { _tag: "Set", at: now + 3_600_000 },
    );
    assert.deepStrictEqual(
      plan({ wake: wake(now + 3_600_000, false), now, current: Option.none(), keepAliveMs }),
      { _tag: "Set", at: now + 3_600_000 },
    );
  });

  it("keeps an alarm that is already at the deadline", () => {
    assert.deepStrictEqual(
      plan({
        wake: wake(now + 60_000, false),
        now,
        current: Option.some(now + 60_000),
        keepAliveMs,
      }),
      { _tag: "Keep" },
    );
  });

  it("deletes the alarm when nothing is due", () => {
    assert.deepStrictEqual(
      plan({ wake: wake(undefined, false), now, current: Option.some(now + 20_000), keepAliveMs }),
      { _tag: "Delete" },
    );
    assert.deepStrictEqual(
      plan({ wake: wake(undefined, false), now, current: Option.none(), keepAliveMs }),
      { _tag: "Keep" },
    );
  });
});

describe("planUnknown", () => {
  it("sets the keep-alive while there is active work, whatever the engine reports", () => {
    assert.deepStrictEqual(planUnknown({ wake: wake(undefined, true), now, keepAliveMs }), {
      _tag: "Set",
      at: now + keepAliveMs,
    });
  });

  it("sets the next deadline when the Session only sleeps", () => {
    assert.deepStrictEqual(planUnknown({ wake: wake(now + 60_000, false), now, keepAliveMs }), {
      _tag: "Set",
      at: now + 60_000,
    });
  });

  it("deletes the alarm when nothing is due, even one the engine does not report", () => {
    assert.deepStrictEqual(planUnknown({ wake: wake(undefined, false), now, keepAliveMs }), {
      _tag: "Delete",
    });
  });
});

describe("earlyAlarm", () => {
  it("arms no later than the keep-alive", () => {
    assert.deepStrictEqual(earlyAlarm({ now, current: Option.none(), keepAliveMs }), {
      _tag: "Set",
      at: now + keepAliveMs,
    });
    assert.deepStrictEqual(
      earlyAlarm({ now, current: Option.some(now + 3_600_000), keepAliveMs }),
      {
        _tag: "Set",
        at: now + keepAliveMs,
      },
    );
    assert.deepStrictEqual(earlyAlarm({ now, current: Option.some(now + 1_000), keepAliveMs }), {
      _tag: "Keep",
    });
  });
});

describe("writesLiveTask", () => {
  const base = {
    id: TaskId.make(7),
    conversationId: ConversationId.make(1),
    kind: "test",
    version: 1,
    input: null,
    background: false,
    abortRequested: false,
  } as const;
  const live = (status: "pending" | "running"): StorageWrite => ({
    type: "task",
    value: { ...base, state: { status, checkpoint: { phase: "run" } } },
  });
  const waiting: StorageWrite = {
    type: "task",
    value: {
      ...base,
      state: { status: "waiting", checkpoint: { phase: "wait" }, on: [], policy: "allSettled" },
    },
  };
  const terminal: StorageWrite = {
    type: "task",
    value: {
      ...base,
      state: { status: "terminal", outcome: { status: "completed", result: null } },
    },
  };

  it("holds for a batch that leaves a task able to run", () => {
    assert.isTrue(writesLiveTask([live("pending")]));
    assert.isTrue(writesLiveTask([live("running")]));
    assert.isTrue(writesLiveTask([waiting]));
  });

  it("does not hold for a batch without live tasks", () => {
    assert.isFalse(writesLiveTask([]));
    assert.isFalse(writesLiveTask([terminal]));
    assert.isFalse(writesLiveTask([{ type: "document.retire", id: DocumentId.make(3) }]));
  });
});

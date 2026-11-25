import { assert, describe, it } from "@effect/vitest";
import type { ToolControl } from "../src/AgentTool.ts";
import { TaskId } from "../src/Ids.ts";
import type { ToolSlot } from "../src/Live.ts";
import type { ToolCall } from "../src/Message.ts";
import type { RetryPolicy } from "../src/Settings.ts";
import {
  classifyAnswer,
  classifyError,
  classifyInterruption,
  decideFinal,
  decideRoundEnd,
  nextPollAt,
  planToolRound,
} from "../src/internal/generationPlan.ts";
import { fauxAssistantMessage, fauxToolCall } from "../src/testing/FauxModel.ts";

// Tests of the generation task's decisions: each is a pure function of what its commit loaded. The Harness
// generation and tool suites cover the same rules through the scheduler.

const call = (name: string, id = name): ToolCall => fauxToolCall(name, {}, { id });
const retry: RetryPolicy = { enabled: true, maxRetries: 2, baseDelayMs: 100 };
const error = (errorMessage: string) =>
  fauxAssistantMessage("", { stopReason: "error", errorMessage });

describe("classifyAnswer", () => {
  it("starts a tool round for a toolUse answer with calls", () => {
    const calls = [call("a"), call("b")];
    const message = fauxAssistantMessage(calls, { stopReason: "toolUse" });
    assert.deepStrictEqual(classifyAnswer(message), { _tag: "Tools", calls });
  });

  it("answers for stop, length and a toolUse without calls", () => {
    for (const stopReason of ["stop", "length", "toolUse"] as const) {
      assert.deepStrictEqual(
        classifyAnswer(fauxAssistantMessage("done", { stopReason })),
        { _tag: "Answer" },
        stopReason,
      );
    }
  });

  it("answers for tool calls that did not stop for toolUse", () => {
    const message = fauxAssistantMessage([call("a")], { stopReason: "stop" });
    assert.deepStrictEqual(classifyAnswer(message), { _tag: "Answer" });
  });

  it("leaves error and aborted responses to classifyError", () => {
    for (const stopReason of ["error", "aborted"] as const) {
      assert.deepStrictEqual(
        classifyAnswer(fauxAssistantMessage("", { stopReason })),
        { _tag: "Error" },
        stopReason,
      );
    }
  });
});

describe("classifyError", () => {
  it("retries a retryable error while attempt <= maxRetries, with the policy's delay", () => {
    const message = error("503 overloaded");
    assert.deepStrictEqual(classifyError(message, retry, 1, 1000), {
      _tag: "Retry",
      until: 1100,
      error: "503 overloaded",
    });
    // The boundary: the last allowed attempt still retries, with the doubled delay.
    assert.deepStrictEqual(classifyError(message, retry, 2, 1000), {
      _tag: "Retry",
      until: 1200,
      error: "503 overloaded",
    });
    assert.deepStrictEqual(classifyError(message, retry, 3, 1000), {
      _tag: "Fail",
      text: "503 overloaded",
    });
  });

  it("fails a retryable error when retries are disabled", () => {
    assert.deepStrictEqual(classifyError(error("overloaded"), { ...retry, enabled: false }, 1, 0), {
      _tag: "Fail",
      text: "overloaded",
    });
  });

  it("never retries a context overflow, even one that reads as retryable", () => {
    // Only a compaction can make the next request fit; `classify` tries one first (HarnessCompaction).
    const message = error("prompt is too long: server overloaded");
    assert.deepStrictEqual(classifyError(message, retry, 1, 0), {
      _tag: "Fail",
      text: "prompt is too long: server overloaded",
    });
  });

  it("fails a non-retryable error with its message", () => {
    assert.deepStrictEqual(classifyError(error("invalid api key"), retry, 1, 0), {
      _tag: "Fail",
      text: "invalid api key",
    });
  });

  it("fails without an error message with the stop reason", () => {
    const aborted = fauxAssistantMessage("", { stopReason: "aborted" });
    assert.deepStrictEqual(classifyError(aborted, retry, 1, 0), {
      _tag: "Fail",
      text: "Model response ended with stop reason aborted",
    });
  });
});

describe("classifyInterruption", () => {
  it("charges a request or a poll cut off by a crash under the retry policy, each with its text", () => {
    assert.deepStrictEqual(classifyInterruption(retry, 1, 1000, false), {
      _tag: "Retry",
      until: 1100,
      error: "The host stopped while the model was answering, on attempt 1",
    });
    assert.deepStrictEqual(classifyInterruption(retry, 3, 1000, true), {
      _tag: "Fail",
      text: "The host stopped while the deferred answer was fetched, on attempt 3",
    });
  });
});

describe("nextPollAt", () => {
  const handle = { provider: "faux", modelId: "faux-1", api: "faux", id: "h" };

  it("polls pollAfterMs from now, 5 s by default", () => {
    assert.strictEqual(nextPollAt({ ...handle, pollAfterMs: 10 }, 1000, undefined), 1010);
    assert.strictEqual(nextPollAt(handle, 1000, undefined), 6000);
  });

  it("polls a still deferred result strictly after the poll that returned it", () => {
    assert.strictEqual(nextPollAt({ ...handle, pollAfterMs: 0 }, 1000, 1000), 1001);
    // A clock behind the poll's time.
    assert.strictEqual(nextPollAt({ ...handle, pollAfterMs: 10 }, 500, 2000), 2001);
  });
});

describe("decideFinal", () => {
  it("continues for an onYield continuation only with no user item and no reset", () => {
    assert.strictEqual(decideFinal(0, false, true), "Continue");
    assert.strictEqual(decideFinal(1, false, true), "End");
    assert.strictEqual(decideFinal(0, true, true), "End");
    assert.strictEqual(decideFinal(0, false, false), "End");
  });
});

describe("planToolRound", () => {
  it("gives a call the request did not offer a flagged task", () => {
    const calls = [call("a"), call("missing"), call("b")];
    assert.deepStrictEqual(
      planToolRound(calls, new Set(["a", "b"]), false).map((planned) => [
        planned._tag,
        planned.unoffered,
      ]),
      [
        ["Task", false],
        ["Task", true],
        ["Task", false],
      ],
    );
  });

  it("starts only the first call of a sequential round, offered or not", () => {
    const calls = [call("missing"), call("a", "1"), call("a", "2"), call("missing", "3")];
    assert.deepStrictEqual(
      planToolRound(calls, new Set(["a"]), true).map((planned) => [
        planned._tag,
        planned.unoffered,
      ]),
      [
        ["Task", true],
        ["Pending", false],
        ["Pending", false],
        ["Pending", true],
      ],
    );
  });

  it("keeps each call with its plan, in call order", () => {
    const calls = [call("a", "1"), call("a", "2")];
    assert.deepStrictEqual(planToolRound(calls, new Set(["a"]), true), [
      { _tag: "Task", call: calls[0]!, unoffered: false },
      { _tag: "Pending", call: calls[1]!, unoffered: false },
    ]);
  });
});

const slot = (callId: string, taskId?: number): ToolSlot =>
  taskId === undefined
    ? { callId, name: "t", status: "done" }
    : { callId, name: "t", status: "done", taskId: TaskId.make(taskId) };
const controls = (...entries: ReadonlyArray<readonly [number, ToolControl | undefined]>) =>
  new Map(entries.map(([id, control]) => [TaskId.make(id), control] as const));

describe("decideRoundEnd", () => {
  it("terminates only when every slot has a task that asks to", () => {
    const both = controls([1, { terminate: true }], [2, { terminate: true }]);
    assert.isTrue(decideRoundEnd([slot("a", 1), slot("b", 2)], both).terminate);
    const one = controls([1, { terminate: true }], [2, {}]);
    assert.isFalse(decideRoundEnd([slot("a", 1), slot("b", 2)], one).terminate);
    // A slot without a task (faulted or orphaned before it got one) cannot ask to terminate.
    assert.isFalse(
      decideRoundEnd([slot("a", 1), slot("b")], controls([1, { terminate: true }])).terminate,
    );
    // A faulted tool has no control.
    assert.isFalse(decideRoundEnd([slot("a", 1)], controls([1, undefined])).terminate);
    assert.isFalse(decideRoundEnd([], controls()).terminate);
  });

  it("lets the last handoff in call order win", () => {
    const end = decideRoundEnd(
      [slot("a", 1), slot("b", 2), slot("c", 3)],
      controls([1, { handoff: "first" }], [2, { handoff: "second" }], [3, {}]),
    );
    assert.strictEqual(end.handoff, "second");
    assert.isUndefined(decideRoundEnd([slot("a", 1)], controls([1, {}])).handoff);
  });

  it("applies the controls in call order, the slots' order", () => {
    const end = decideRoundEnd(
      [slot("a", 1), slot("b", 2), slot("c", 3)],
      controls(
        [3, { addTools: ["z", "x"], handoff: "third" }],
        [1, { addTools: ["x", "y"], handoff: "first" }],
        [2, undefined],
      ),
    );
    assert.deepStrictEqual(end.added, ["x", "y", "z", "x"]);
    assert.strictEqual(end.handoff, "third");
  });
});

import { assert, describe, it } from "@effect/vitest";
import * as Cause from "effect/Cause";
import * as Clock from "effect/Clock";
import * as Data from "effect/Data";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as Scope from "effect/Scope";
import * as TestClock from "effect/testing/TestClock";
import {
  boundOutput,
  OutputBuffer,
  type ResolvedOutputLimits,
  sanitizeOutput,
} from "../src/internal/output.ts";
import * as Progress from "../src/internal/progress.ts";

// Output limits and the `Progress` buffer. The pure cases are plain `it`. The four `Progress` cases are `it.effect`: fake
// timers are the `TestClock` (`advance` adjusts it and lets the forked commits
// run), `Date.now()` is `Clock`, a promise waiter is a forked `markAndWait`
// fiber, and the thrown commit error is a tagged failure. `Progress` is made in a
// scope (`Effect.scoped`), which owns its fibers.

const head = (maxLines: number, maxBytes = 1000): ResolvedOutputLimits => ({
  maxBytes,
  maxLines,
  retain: "head",
});
const tail = (maxLines: number, maxBytes = 1000): ResolvedOutputLimits => ({
  maxBytes,
  maxLines,
  retain: "tail",
});

function bufferTail(content: string, maxBytes: number): string {
  const bytes = Buffer.from(content, "utf8");
  if (bytes.length <= maxBytes) return content;
  let start = bytes.length - maxBytes;
  while (start < bytes.length && ((bytes[start] ?? 0) & 0xc0) === 0x80) start++;
  return bytes.subarray(start).toString("utf8");
}

/** A single line longer than the byte limit is cut like Buffer's tail slice on a character boundary. */
function assertMatchesBufferTail(input: string, maxByteValues?: ReadonlyArray<number>): void {
  const totalBytes = Buffer.byteLength(input, "utf8");
  const values = maxByteValues ?? Array.from({ length: totalBytes + 5 }, (_, maxBytes) => maxBytes);
  for (const maxBytes of values) {
    const kept = boundOutput(input, { maxBytes, maxLines: 10, retain: "tail" }).text;
    const expected = bufferTail(input, maxBytes);
    assert.strictEqual(
      kept,
      expected,
      `tail mismatch input=${JSON.stringify(input)} maxBytes=${maxBytes}`,
    );
    assert.isAtMost(
      Buffer.byteLength(kept, "utf8"),
      maxBytes,
      `tail output exceeded ${maxBytes} bytes`,
    );
  }
}

function sampledByteLimits(input: string): Array<number> {
  const totalBytes = Buffer.byteLength(input, "utf8");
  const candidates = [
    0,
    1,
    2,
    3,
    4,
    5,
    8,
    Math.floor(totalBytes / 2),
    totalBytes - 4,
    totalBytes - 1,
    totalBytes,
    totalBytes + 1,
  ];
  return [...new Set(candidates.filter((value) => value >= 0))].toSorted((a, b) => a - b);
}

function bound(text: string, limits: ResolvedOutputLimits) {
  const slice = boundOutput(text, limits);
  return { kept: slice.text, droppedBytes: slice.droppedBytes, droppedLines: slice.droppedLines };
}

describe("tool output bounds", () => {
  it("removes control characters but keeps tabs, newlines, and other text", () => {
    assert.strictEqual(sanitizeOutput("a\0b\tc\nd\re\u0007f\ufff9g\ufffbh😀"), "ab\tc\ndefgh😀");
  });

  it("keeps output within the limits unchanged", () => {
    assert.deepStrictEqual(bound("a\nb\n", head(2)), {
      kept: "a\nb\n",
      droppedBytes: 0,
      droppedLines: 0,
    });
    assert.deepStrictEqual(bound("a\nb", tail(2)), {
      kept: "a\nb",
      droppedBytes: 0,
      droppedLines: 0,
    });
    assert.deepStrictEqual(bound("", tail(2)), { kept: "", droppedBytes: 0, droppedLines: 0 });
  });

  it("keeps nothing with a zero limit", () => {
    assert.deepStrictEqual(bound("ab\ncd\n", head(10, 0)), {
      kept: "",
      droppedBytes: 6,
      droppedLines: 2,
    });
    assert.deepStrictEqual(bound("ab\ncd\n", tail(0)), {
      kept: "",
      droppedBytes: 6,
      droppedLines: 2,
    });
  });

  it("keeps exact slices of whole lines, trailing newline included", () => {
    assert.deepStrictEqual(bound("a\nb\nc\n", head(2)), {
      kept: "a\nb\n",
      droppedBytes: 2,
      droppedLines: 1,
    });
    assert.deepStrictEqual(bound("a\nb\nc\n", tail(2)), {
      kept: "b\nc\n",
      droppedBytes: 2,
      droppedLines: 1,
    });
    assert.deepStrictEqual(bound("a\nb\nc", tail(2)), {
      kept: "b\nc",
      droppedBytes: 2,
      droppedLines: 1,
    });
    // Blank lines are lines.
    assert.deepStrictEqual(bound("a\nb\nc\n\n", tail(3)), {
      kept: "b\nc\n\n",
      droppedBytes: 2,
      droppedLines: 1,
    });
  });

  it("cuts at the byte limit on whole lines when possible", () => {
    assert.deepStrictEqual(bound("aa\nbb\ncc\n", head(10, 7)), {
      kept: "aa\nbb\n",
      droppedBytes: 3,
      droppedLines: 1,
    });
    assert.deepStrictEqual(bound("aa\nbb\ncc\n", tail(10, 7)), {
      kept: "bb\ncc\n",
      droppedBytes: 3,
      droppedLines: 1,
    });
  });

  it("cuts a single line longer than the byte limit on a character boundary", () => {
    // "é" is two bytes; five bytes hold two whole characters.
    assert.deepStrictEqual(bound("ééé\n", head(10, 5)), {
      kept: "éé",
      droppedBytes: 3,
      droppedLines: 0,
    });
    assert.deepStrictEqual(bound("x\néééé", tail(10, 5)), {
      kept: "éé",
      droppedBytes: 6,
      droppedLines: 1,
    });
  });

  it("keeps a U+FEFF at the start of a kept slice", () => {
    assert.deepStrictEqual(bound("x\ufeffa", tail(10, 4)), {
      kept: "\ufeffa",
      droppedBytes: 1,
      droppedLines: 0,
    });
  });

  it("cuts tails of surrogate edge cases exactly like Buffer", () => {
    const inputs = ["a\ud83d", "\ude42b", "a\ude42b", "\ud83d🙂", "🙂\ude42", "👩‍💻"];
    for (const input of inputs) assertMatchesBufferTail(input);
  });

  it("cuts tails exactly like Buffer across deterministic fuzz cases", () => {
    const alphabet = [
      "a",
      "\u007f",
      "\u0080",
      "é",
      "߿",
      "ࠀ",
      "中",
      "퟿",
      "\ud800",
      "\ud83d",
      "\udc00",
      "\ude42",
      "🙂",
      "",
      "￿",
    ];
    function checkExhaustive(prefix: string, depth: number): void {
      assertMatchesBufferTail(prefix, sampledByteLimits(prefix));
      if (depth === 0) return;
      for (const character of alphabet) checkExhaustive(prefix + character, depth - 1);
    }
    checkExhaustive("", 3);
    let seed = 0x12345678;
    const random = (): number => {
      seed = (seed * 1664525 + 1013904223) >>> 0;
      return seed / 0x100000000;
    };
    for (let i = 0; i < 1_000; i++) {
      let input = "";
      const length = Math.floor(random() * 80);
      for (let j = 0; j < length; j++) input += alphabet[Math.floor(random() * alphabet.length)];
      assertMatchesBufferTail(input, sampledByteLimits(input));
    }
  });
});

describe("OutputBuffer", () => {
  it("keeps exact totals across chunks and decodes UTF-8 split across byte chunks", () => {
    const buffer = new OutputBuffer(tail(2));
    const bytes = new TextEncoder().encode("😀\n");
    buffer.push("a\nb\n");
    buffer.push(bytes.subarray(0, 2));
    buffer.push(bytes.subarray(2));
    assert.deepStrictEqual(buffer.snapshot(), {
      text: "b\n😀\n",
      droppedBytes: 2,
      droppedLines: 1,
    });
  });

  it("sanitizes the retained text but counts the raw stream", () => {
    const buffer = new OutputBuffer(tail(1));
    buffer.push("a\u0007\n");
    assert.deepStrictEqual(buffer.snapshot(), { text: "a\n", droppedBytes: 0, droppedLines: 0 });
    buffer.push("b\u001b\n");
    assert.deepStrictEqual(buffer.snapshot(), { text: "b\n", droppedBytes: 3, droppedLines: 1 });
  });

  it("drops a byte-order mark only at the start of the output", () => {
    const bom = Uint8Array.of(0xef, 0xbb, 0xbf);
    const buffer = new OutputBuffer(tail(10));
    buffer.push(bom.subarray(0, 1));
    buffer.push(Uint8Array.of(0xbb, 0xbf, 0x61));
    buffer.push("b");
    // A U+FEFF after a string chunk is text.
    buffer.push(Uint8Array.of(...bom, 0x63));
    buffer.end();
    assert.strictEqual(buffer.snapshot().text, "ab\ufeffc");
  });

  it("flushes an incomplete character before a string chunk and at the end", () => {
    const buffer = new OutputBuffer(tail(10));
    const euro = new TextEncoder().encode("€");
    buffer.push(euro.subarray(0, 1));
    buffer.push("x");
    buffer.push(euro.subarray(0, 2));
    buffer.end();
    assert.strictEqual(buffer.snapshot().text, "\ufffdx\ufffd");
  });

  it("matches bounding the whole stream when several chunks arrive between snapshots", () => {
    for (const limits of [head(3, 40), tail(3, 40), head(50, 25), tail(50, 25)]) {
      const buffer = new OutputBuffer(limits);
      let stream = "";
      for (let index = 0; index < 300; index++) {
        const chunk = index % 7 === 0 ? `${"é".repeat(index % 30)}\n` : `line ${index}\n`;
        stream += chunk;
        buffer.push(chunk);
        if (index % 5 !== 4) continue;
        const expected = boundOutput(stream, limits);
        assert.deepStrictEqual(buffer.snapshot(), {
          text: expected.text,
          droppedBytes: expected.droppedBytes,
          droppedLines: expected.droppedLines,
        });
      }
    }
  });

  it("stops storing head output once the window is full", () => {
    const buffer = new OutputBuffer(head(2));
    for (let index = 0; index < 1000; index++) buffer.push(`line ${index}\n`);
    assert.isBelow(buffer.storedBytes, 20);
    assert.deepStrictEqual(buffer.snapshot(), {
      text: "line 0\nline 1\n",
      droppedBytes: 8876,
      droppedLines: 998,
    });
  });

  it("stores only the tail window after each snapshot", () => {
    const buffer = new OutputBuffer(tail(3, 100));
    let stream = "";
    for (let index = 0; index < 2000; index++) {
      const chunk = `line ${index}\n\n`;
      stream += chunk;
      buffer.push(chunk);
      const snapshot = buffer.snapshot();
      assert.isAtMost(buffer.storedBytes, 100);
      assert.strictEqual(snapshot.text, boundOutput(stream, tail(3, 100)).text);
    }
  });
});

class CommitFailed extends Data.TaggedError("CommitFailed")<{ readonly message: string }> {}

/** Advances the TestClock and lets the commits it starts run. */
const advance = (millis: number) =>
  Effect.gen(function* () {
    yield* TestClock.adjust(millis);
    for (let turn = 0; turn < 10; turn++) yield* Effect.yieldNow;
  });

describe("Progress", () => {
  it.effect(
    "commits the first change at once, then waits at least 100 ms and the written size at 100 KiB/s",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const commits: Array<number> = [];
          let size = 50 * 1024;
          const progress = yield* Progress.make({
            write: () =>
              Effect.map(Clock.currentTimeMillis, (now) => {
                commits.push(now);
                return size;
              }),
            onError: () => Effect.void,
            minIntervalMs: 100,
          });
          yield* progress.mark;
          yield* advance(0);
          assert.deepStrictEqual(commits, [0]);
          // 50 KiB buys 500 ms; changes meanwhile coalesce into one commit.
          size = 10;
          yield* progress.mark;
          yield* progress.mark;
          yield* advance(499);
          assert.deepStrictEqual(commits, [0]);
          yield* advance(1);
          assert.deepStrictEqual(commits, [0, 500]);
          // A small commit still waits the minimum 100 ms.
          yield* progress.mark;
          yield* advance(99);
          assert.deepStrictEqual(commits, [0, 500]);
          yield* advance(1);
          assert.deepStrictEqual(commits, [0, 500, 600]);
        }),
      ),
  );

  it.effect("waits the configured minimum interval between small commits", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const commits: Array<number> = [];
        const progress = yield* Progress.make({
          write: () =>
            Effect.map(Clock.currentTimeMillis, (now) => {
              commits.push(now);
              return 10;
            }),
          onError: () => Effect.void,
          minIntervalMs: 500,
        });
        yield* progress.mark;
        yield* advance(0);
        yield* progress.mark;
        yield* advance(499);
        assert.deepStrictEqual(commits, [0]);
        yield* advance(1);
        assert.deepStrictEqual(commits, [0, 500]);
      }),
    ),
  );

  it.effect("rejects the waiters of a failed commit and reports its error", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const errors: Array<CommitFailed> = [];
        const failure = new CommitFailed({ message: "commit failed" });
        const progress = yield* Progress.make({
          write: () => Effect.fail(failure),
          onError: (cause) =>
            Effect.sync(() => {
              const error = Cause.findErrorOption(cause);
              if (Option.isSome(error)) errors.push(error.value);
            }),
          minIntervalMs: 100,
        });
        assert.strictEqual(yield* Effect.flip(progress.markAndWait()), failure);
        assert.deepStrictEqual(errors, [failure]);
      }),
    ),
  );

  it.effect(
    "stops: waits for the commit in flight and hands back waiters no commit covered yet",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const inFlight = yield* Deferred.make<void>();
          const progress = yield* Progress.make({
            write: () => Effect.as(Deferred.await(inFlight), 0),
            onError: () => Effect.void,
            minIntervalMs: 100,
          });
          const first = yield* progress
            .markAndWait()
            .pipe(Effect.forkChild({ startImmediately: true }));
          const second = yield* progress
            .markAndWait()
            .pipe(Effect.forkChild({ startImmediately: true }));
          const stopped = yield* progress.stop.pipe(Effect.forkChild({ startImmediately: true }));
          yield* Deferred.succeed(inFlight, undefined);
          const pending = yield* Fiber.join(stopped);
          yield* Fiber.join(first);
          assert.strictEqual(pending.length, 1);
          pending[0]?.resolve();
          yield* Fiber.join(second);
        }),
      ),
  );
});

// Ordering cases: `Progress` decides in the caller's step (`progress.ts`), so the order
// of waits, stops and commits is the order of the calls.
describe("Progress ordering", () => {
  it.effect("a wait refused by its admit check after stop fails at once, with no waiter left", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const refusal = new CommitFailed({ message: "ended" });
        let ended = false;
        const progress = yield* Progress.make({
          write: () => Effect.succeed(0),
          onError: () => Effect.void,
          minIntervalMs: 100,
        });
        ended = true;
        assert.strictEqual((yield* progress.stop).length, 0);
        const exit = yield* Effect.exit(progress.markAndWait(() => (ended ? refusal : undefined)));
        assert.deepStrictEqual(exit, Exit.fail(refusal));
        assert.strictEqual((yield* progress.stop).length, 0);
      }),
    ),
  );

  it.effect("a wait admitted after stop is a defect, never a wait no commit settles", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const progress = yield* Progress.make({
          write: () => Effect.succeed(0),
          onError: () => Effect.void,
          minIntervalMs: 100,
        });
        yield* progress.stop;
        const exit = yield* Effect.exit(progress.markAndWait());
        assert.isTrue(Exit.hasDies(exit));
      }),
    ),
  );

  it.effect("a mark with no pause pending captures the commit's content in the caller's step", () =>
    Effect.scoped(
      Effect.gen(function* () {
        let reported = "a";
        const written: Array<string> = [];
        const progress = yield* Progress.make({
          write: () => {
            const content = reported;
            return Effect.sync(() => {
              written.push(content);
              return 0;
            });
          },
          onError: () => Effect.void,
          minIntervalMs: 100,
        });
        yield* progress.mark;
        reported = "ab";
        yield* advance(100);
        assert.deepStrictEqual(written, ["a"]);
      }),
    ),
  );

  it.effect("interrupting a wait cancels only the wait; the change is still committed", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const commits: Array<number> = [];
        const progress = yield* Progress.make({
          write: () =>
            Effect.map(Clock.currentTimeMillis, (now) => {
              commits.push(now);
              return 0;
            }),
          onError: () => Effect.void,
          minIntervalMs: 100,
        });
        yield* progress.mark;
        const waiting = yield* progress
          .markAndWait()
          .pipe(Effect.forkChild({ startImmediately: true }));
        yield* Fiber.interrupt(waiting);
        yield* advance(100);
        assert.deepStrictEqual(commits, [0, 100]);
        assert.strictEqual((yield* progress.stop).length, 0);
      }),
    ),
  );

  it.effect(
    "a change marked while the last commit is in flight is never committed after stop",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const release = yield* Deferred.make<void>();
          const commits: Array<number> = [];
          const progress = yield* Progress.make({
            write: () =>
              Deferred.await(release).pipe(
                Effect.andThen(Clock.currentTimeMillis),
                Effect.map((now) => {
                  commits.push(now);
                  return 0;
                }),
              ),
            onError: () => Effect.void,
            minIntervalMs: 100,
          });
          yield* progress.mark;
          // Dirty again while the first commit is in flight: without `stop` it would get the next commit.
          yield* progress.mark;
          const stopped = yield* progress.stop.pipe(Effect.forkChild({ startImmediately: true }));
          yield* Deferred.succeed(release, undefined);
          yield* Fiber.join(stopped);
          yield* advance(1000);
          assert.deepStrictEqual(commits, [0]);
        }),
      ),
  );

  it.effect("closing the scope with a pause pending commits nothing afterwards", () =>
    Effect.gen(function* () {
      const commits: Array<number> = [];
      const scope = yield* Scope.make();
      const progress = yield* Progress.make({
        write: () =>
          Effect.map(Clock.currentTimeMillis, (now) => {
            commits.push(now);
            return 0;
          }),
        onError: () => Effect.void,
        minIntervalMs: 100,
      }).pipe(Scope.provide(scope));
      yield* progress.mark;
      yield* advance(0);
      // The second change waits out the 100 ms pause; closing the scope cancels it.
      yield* progress.mark;
      yield* Scope.close(scope, Exit.void);
      yield* advance(1000);
      assert.deepStrictEqual(commits, [0]);
    }),
  );

  it.effect("closing the scope awaits the commit in flight and ends the waits left", () =>
    Effect.gen(function* () {
      const release = yield* Deferred.make<void>();
      const committed: Array<string> = [];
      const scope = yield* Scope.make();
      const progress = yield* Progress.make({
        write: () =>
          Deferred.await(release).pipe(
            Effect.andThen(Effect.sync(() => committed.push("commit"))),
            Effect.as(0),
          ),
        onError: () => Effect.void,
        minIntervalMs: 100,
      }).pipe(Scope.provide(scope));
      yield* progress.mark;
      const late = yield* progress.markAndWait().pipe(Effect.forkChild({ startImmediately: true }));
      const closing = yield* Scope.close(scope, Exit.void).pipe(
        Effect.andThen(Effect.sync(() => committed.push("closed"))),
        Effect.forkChild({ startImmediately: true }),
      );
      yield* Deferred.succeed(release, undefined);
      yield* Fiber.join(closing);
      assert.deepStrictEqual(committed, ["commit", "closed"]);
      assert.isTrue(Exit.hasInterrupts(yield* Fiber.await(late)));
    }),
  );
});

# @effect-durable-harness/core

The kernel: Session, `Tx`, documents, tasks and the Harness.

## Concurrency in the kernel

An Effect fiber can yield between any two steps, and a `Deferred` resumes its waiters inline, on the completer's stack. The kernel keeps its ordering with four rules; most kernel bugs so far broke one of them.

- **Check and register in one synchronous step.** A decision that depends on a flag (`closing`, `sealed`, `ended`, a live record) is made in the same synchronous step that registers what depends on it: a place in the line (`Line.run`'s `refuse`), a waiter (`Waiters.add`), a tracked operation (`track`), a listener, a handler about to start (`runHandler`). Uninterruptible is not atomic.
- **Mutate, then wake; wake after a yield.** Finish the bookkeeping a waiter must not see half done before waking it. Wake with `doneAfterYield` or a `Signal` (`internal/signal.ts`), never a bare `Deferred` completion or `Latch.openUnsafe`; `durable/no-inline-wakeup` enforces it.
- **Deferred passes are loops on a `Signal`** (`Signal.forkLoop`). A loop re-reads the state when it wakes; firing is level-triggered.
- **The Session's mutations go through the FIFO line** (`internal/line.ts`), on the caller's fiber. `Semaphore` is not FIFO.

In-memory index logic stays synchronous and Effect-free (`internal/ownership.ts`): it runs inside commit callbacks, where a yield would expose a half-updated index.

Scheduler decisions are pure functions that return a plan (`internal/decisions.ts`). A scheduler commit loads what it needs, decides and registers in one synchronous step, then applies the plan's writes in order. No `yield*` between reading the state a decision depends on and acting on it.

`test/SessionKernel.test.ts` and `test/SchedulerKernel.test.ts` sweep interruption and yield points (`Scheduler.MaxOpsBeforeYield`).

import * as Alchemy from "alchemy";
import * as Cloudflare from "@alchemy.run/cloudflare";
import * as Test from "@alchemy.run/test/Vitest";
import * as Effect from "effect/Effect";
import { expect } from "vitest";
import Stack from "./alchemy.run.ts";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import { NestResult, type HoldResult } from "./fixture/AgentProbe.ts";
import {
  callKeepAlive,
  deployed,
  idle,
  millis,
  objectName,
  type ProbeState,
  type TranscriptMessage,
} from "./support.ts";

/**
 * Invocations that wait in memory for minutes, with no I/O and no timer of their own pending and no incoming call,
 * stay in one instance: the host holds the object while the kernel reports active work. Without the hold, Cloudflare
 * hibernates the object about 10 s after its last event and the wait is lost. Two waits, on two objects, at once:
 *
 * - `probe.hold` waits in its phase for a child that sleeps with `sleepUntil` and was created with
 *   `abandonOnRestart`: a reopen would abort the child and rerun the phase in another instance, with a restart.
 * - the model's `nest` tool waits with `executeTool` for a nested `nap` call that is parked (`AgentTool.sleep`): the
 *   caller's invocation stays occupied while the nested call holds none. `nest` is not replay-safe, so a reopen
 *   would end it `interrupted`.
 *
 * Deployed only, and opt-in (`DURABLE_AWAKE=1`): local workerd does not hibernate, and it takes about five minutes.
 */
const run = deployed && process.env["DURABLE_AWAKE"] === "1";

const WAIT_MS = 240_000;

const { test, beforeAll, afterAll, deploy, destroy } = Test.make({
  providers: Cloudflare.providers(),
  state: Alchemy.localState(),
  dev: true,
});

const stack = beforeAll(deploy(Stack), { timeout: 300_000 });
afterAll.skipIf(!!process.env.NO_DESTROY)(destroy(Stack), { timeout: 300_000 });

const decodeNest = Schema.decodeUnknownOption(Schema.fromJsonString(NestResult));

test.skipIf(!run)(
  "a phase and a tool waiting in memory for minutes stay in one instance",
  Effect.gen(function* () {
    const { url } = yield* stack;
    const holder = objectName("awake-task");
    const nester = objectName("awake-tool");
    const holderBefore = yield* callKeepAlive<ProbeState>(url, holder, "state");
    const nesterBefore = yield* callKeepAlive<ProbeState>(url, nester, "state");
    const id = yield* callKeepAlive<number>(url, holder, "startHold", WAIT_MS);
    yield* callKeepAlive<number>(url, nester, "submit", `nest:${WAIT_MS}`, "awake-nest");
    yield* idle(millis(WAIT_MS + 30_000));

    const held = yield* callKeepAlive<typeof HoldResult.Type | null>(url, holder, "holdResult", id);
    const messages = yield* callKeepAlive<ReadonlyArray<TranscriptMessage>>(
      url,
      nester,
      "transcript",
    );
    const toolResult = messages.find((message) => message.role === "toolResult")?.text ?? "";
    const nested = Option.getOrUndefined(decodeNest(toolResult));
    yield* Effect.log("awake", {
      task: { before: holderBefore.instance, held },
      tool: { before: nesterBefore.instance, toolResult },
    });

    expect({
      waitedIn: held?.waitedIn,
      instance: held?.instance,
      restarts: held?.restarts,
      child: held?.child,
    }).toEqual({
      waitedIn: holderBefore.instance,
      instance: holderBefore.instance,
      restarts: 0,
      child: "completed",
    });
    expect((held?.at ?? 0) - (held?.waitedFrom ?? 0)).toBeGreaterThanOrEqual(WAIT_MS - 5_000);

    expect({
      waitedIn: nested?.waitedIn,
      instance: nested?.instance,
      nested: nested?.nested,
    }).toEqual({
      waitedIn: nesterBefore.instance,
      instance: nesterBefore.instance,
      nested: "woke",
    });
    expect((nested?.at ?? 0) - (nested?.waitedFrom ?? 0)).toBeGreaterThanOrEqual(WAIT_MS - 5_000);
  }),
  { timeout: 600_000 },
);

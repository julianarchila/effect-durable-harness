// Helpers of the host suite: calls to the `AgentProbe` object through the test Worker, and bounded polling.
import * as Data from "effect/Data";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Schedule from "effect/Schedule";
import * as HttpClient from "effect/http/HttpClient";
import * as HttpClientRequest from "effect/http/HttpClientRequest";
import * as HttpClientResponse from "effect/http/HttpClientResponse";
import type { ConversationView } from "@effect-durable-harness/core/View";
import * as View from "@effect-durable-harness/host/View";
import type { TaskView } from "./fixture/AgentProbe.ts";

/** Whether the suite runs against a deployed stage, where a commit is durable only after the output gate. */
export const deployed = process.env["ALCHEMY_TEST_DEV"] === "0";

let objects = 0;
/** A fresh object name per test, so every test starts from an empty Session. */
export const objectName = (label: string) => `${label}-${process.pid}-${objects++}`;

/** What `AgentProbe.state` answers. */
export interface ProbeState {
  readonly instance: string;
  readonly instanceStartedAt: number;
  readonly now: number;
  readonly alarm: number | null;
  readonly nextWakeAt: number | null;
  readonly activeWork: boolean;
  readonly scheduling: string;
  readonly running: ReadonlyArray<string>;
  /** Each running task as `kind:phase`, its checkpoint phase. */
  readonly phases: ReadonlyArray<string>;
}

/** One transcript message, as `AgentProbe.transcript` answers it. */
export interface TranscriptMessage {
  readonly role: string;
  readonly text: string;
}

const base = (url: string) => url.replace(/\/+$/, "");

const post = <A>(target: string, args: ReadonlyArray<unknown>) =>
  Effect.gen(function* () {
    const client = yield* HttpClient.HttpClient;
    const request = HttpClientRequest.post(target).pipe(HttpClientRequest.bodyJsonUnsafe(args));
    const response = yield* client.execute(request).pipe(
      Effect.flatMap(HttpClientResponse.filterStatusOk),
      // A fresh deployment can answer 404 / error 1042 for a few seconds.
      Effect.retry({ schedule: Schedule.spaced("1 second"), times: 30 }),
    );
    // SAFETY: the test Worker answers the method's JSON result.
    return (yield* response.json) as A;
  }).pipe(Effect.orDie);

/** Calls one `AgentProbe` method of the object `name`; a call that reset the object answers `{ reset: true }`. */
export const call = <A>(
  url: string,
  name: string,
  method: string,
  ...args: ReadonlyArray<unknown>
) => post<A>(`${base(url)}/agent/${name}/${method}`, args);

/** The same, on `KeepAliveProbe` (default keep-alive and settings). */
export const callKeepAlive = <A>(
  url: string,
  name: string,
  method:
    | "startTask"
    | "task"
    | "state"
    | "submit"
    | "transcript"
    | "partialLength"
    | "startHold"
    | "holdResult",
  ...args: ReadonlyArray<unknown>
) => post<A>(`${base(url)}/keepalive/${name}/${method}`, args);

/**
 * The object's state. A call that fails answers `{ reset: true }`, which a fresh deployment also does for a while,
 * so this retries until an instance answers.
 */
export const state = (url: string, name: string) =>
  call<ProbeState>(url, name, "state").pipe(
    Effect.repeat({
      schedule: Schedule.spaced("250 millis"),
      until: (current) => current.instance !== undefined,
      times: 120,
    }),
  );
export const task = (url: string, name: string, id: number) =>
  call<TaskView | null>(url, name, "task", id);
export const transcript = (url: string, name: string) =>
  call<ReadonlyArray<TranscriptMessage>>(url, name, "transcript");
/** Resets the object; deployed, the last commit is made durable first. */
export const crash = (url: string, name: string) => call<unknown>(url, name, "crash", deployed);

/** Runs `effect` until `done` holds, every `spacing`, at most `times` times; dies with the last value otherwise. */
export const eventually = <A, E, R>(
  effect: Effect.Effect<A, E, R>,
  done: (value: A) => boolean,
  options: { readonly spacing?: Duration.Input; readonly times?: number } = {},
) =>
  effect.pipe(
    Effect.repeat({
      schedule: Schedule.spaced(options.spacing ?? "250 millis"),
      until: done,
      times: options.times ?? 80,
    }),
    Effect.flatMap((value) =>
      done(value)
        ? Effect.succeed(value)
        : Effect.die(new Error(`Condition not reached; last value: ${JSON.stringify(value)}`)),
    ),
  );

/** Waits `duration` without touching the object. */
export const idle = (duration: Duration.Input) => Effect.sleep(duration);

/** A view socket of a conversation, with the messages it received and a reader kept current from them. */
export interface ViewClient {
  readonly messages: Array<View.ViewMessage>;
  readonly reader: View.Reader;
  readonly closed: () => boolean;
  readonly close: () => void;
}

/** A view socket that failed to open. */
class ViewSocketFailed extends Data.TaggedError("ViewSocketFailed")<{}> {}

/**
 * Opens the view socket of the object `name`'s root conversation; retried, since a fresh deployment refuses
 * connections for a while.
 */
export const openView = (url: string, name: string, route: "agent" | "keepalive" = "agent") =>
  Effect.callback<ViewClient, ViewSocketFailed>((resume) => {
    const socket = new WebSocket(`${base(url).replace(/^http/, "ws")}/${route}/${name}/view`);
    const messages: Array<View.ViewMessage> = [];
    const reader = View.makeReader();
    let closed = false;
    socket.addEventListener("message", (event) => {
      // SAFETY: the host sends `ViewMessage` JSON text.
      const message = JSON.parse(String(event.data)) as View.ViewMessage;
      messages.push(message);
      reader.apply(message);
    });
    socket.addEventListener("close", () => {
      closed = true;
    });
    socket.addEventListener("open", () =>
      resume(
        Effect.succeed({
          messages,
          reader,
          closed: () => closed,
          close: () => socket.close(),
        }),
      ),
    );
    socket.addEventListener("error", () => resume(Effect.fail(new ViewSocketFailed())));
  }).pipe(Effect.retry({ schedule: Schedule.spaced("1 second"), times: 30 }), Effect.orDie);

/** The assistant texts of a view's entries. */
export const assistantTexts = (view: ConversationView | null): ReadonlyArray<string> =>
  (view?.entries ?? []).flatMap((entry) =>
    (entry.model ?? []).flatMap((message) =>
      message.role !== "assistant"
        ? []
        : message.content.flatMap((part) => (part.type === "text" ? [part.text] : [])),
    ),
  );

/** Waits until a probe task is terminal. */
export const settled = (url: string, name: string, id: number) =>
  eventually(task(url, name, id), (view) => view?.status === "terminal");

/** Milliseconds as a `Duration`. */
export const millis = (ms: number) => Duration.millis(ms);

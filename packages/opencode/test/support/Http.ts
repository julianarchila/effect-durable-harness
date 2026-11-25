/**
 * HTTP at the `fetch` boundary. opencode sends
 * through Effect's `FetchHttpClient`, which reads the `Fetch` reference per request, so a replaying `fetch` runs
 * opencode's real provider code offline.
 */
import * as Option from "effect/Option";
import * as Predicate from "effect/Predicate";
import * as Schema from "effect/Schema";

export type HeaderMap = Record<string, string>;

/** A request `fetch` saw, without credentials. */
export interface SentRequest {
  readonly url: string;
  readonly method: string;
  readonly headers: HeaderMap;
  /** The JSON request body, or its text when it is not JSON. */
  readonly body: Schema.Json | undefined;
}

export interface Canned {
  readonly status: number;
  readonly contentType: string;
  readonly response: string;
}

const SECRET_HEADERS = new Set(["x-api-key", "x-goog-api-key", "authorization"]);

type HeadersInput = ConstructorParameters<typeof Headers>[0];

const plainHeaders = (headers: HeadersInput) =>
  Object.fromEntries(
    [...new Headers(headers)].map(([key, value]) => [
      key,
      SECRET_HEADERS.has(key.toLowerCase()) ? "<redacted>" : value,
    ]),
  );

const JsonText = Schema.fromJsonString(Schema.Json);

const parseBody = (text: string): Schema.Json =>
  Schema.decodeUnknownOption(JsonText)(text).pipe(Option.getOrElse(() => text));

const requestParts = async (
  input: Request | string | URL,
  init: RequestInit | undefined,
): Promise<SentRequest> => {
  if (input instanceof Request) {
    return {
      url: input.url,
      method: input.method,
      headers: plainHeaders(input.headers),
      body: parseBody(await input.clone().text()),
    };
  }
  const body = init?.body;
  const text = Predicate.isString(body)
    ? body
    : body instanceof Uint8Array
      ? new TextDecoder().decode(body)
      : undefined;
  // SAFETY: Bun's and the DOM's `HeadersInit` describe the same runtime values; the cast only joins the two typings.
  const headers = init?.headers as HeadersInput;
  return {
    url: String(input),
    method: init?.method ?? "GET",
    headers: plainHeaders(headers),
    body: text === undefined ? undefined : parseBody(text),
  };
};

/** A `fetch` that answers each request with the next canned response, and records the requests. */
export const replayFetch = (responses: ReadonlyArray<Canned>) => {
  const requests: Array<SentRequest> = [];
  // SAFETY: `FetchHttpClient` only calls `fetch(input, init)`; the static members of `typeof fetch` are never used.
  const fetch = (async (input: Request | string | URL, init?: RequestInit) => {
    requests.push(await requestParts(input, init));
    const canned = responses[requests.length - 1];
    if (canned === undefined)
      return new Response("replayFetch: no canned response", { status: 599 });
    return new Response(canned.response, {
      status: canned.status,
      headers: { "content-type": canned.contentType },
    });
  }) as typeof globalThis.fetch;
  return { fetch, requests };
};

/** A `fetch` that sends through `fetch` and records the requests, for live tests. */
export const recordingFetch = (fetch: typeof globalThis.fetch) => {
  const requests: Array<SentRequest> = [];
  // SAFETY: as in `replayFetch`, only `fetch(input, init)` is called.
  const recording = (async (input: Request | string | URL, init?: RequestInit) => {
    requests.push(await requestParts(input, init));
    return fetch(input, init);
  }) as typeof globalThis.fetch;
  return { fetch: recording, requests };
};

/** One Anthropic stream event: its `type` plus JSON fields. */
export type SseEvent = { readonly type: string } & { readonly [field: string]: Schema.Json };

const encodeJson = Schema.encodeSync(JsonText);

/** Server-sent events in Anthropic's framing. */
export const sse = (events: ReadonlyArray<SseEvent>): Canned => ({
  status: 200,
  contentType: "text/event-stream",
  response: events.map((event) => `event: ${event.type}\ndata: ${encodeJson(event)}\n\n`).join(""),
});

export const jsonError = (status: number, type: string, message: string): Canned => ({
  status,
  contentType: "application/json",
  response: encodeJson({ type: "error", error: { type, message } }),
});

export type Block =
  | { readonly type: "text"; readonly text: string }
  | { readonly type: "thinking"; readonly thinking: string; readonly signature: string }
  | { readonly type: "redacted_thinking"; readonly data: string }
  | {
      readonly type: "tool_use";
      readonly id: string;
      readonly name: string;
      readonly input: Schema.Json;
    };

export interface AnthropicStreamInput {
  readonly blocks: ReadonlyArray<Block>;
  readonly stopReason: string;
  readonly stopDetails?:
    | { readonly type: "refusal"; readonly category: string | null; readonly explanation: string }
    | undefined;
  readonly usage?: Schema.JsonObject | undefined;
  /** Raw SSE appended after the blocks instead of `message_delta` and `message_stop`. */
  readonly tail?: ReadonlyArray<SseEvent> | undefined;
}

/** A complete Anthropic Messages stream with the given blocks and final `message_delta`. */
export const anthropicStream = (input: AnthropicStreamInput): Canned => {
  // The usage shape of a real `claude-haiku-5-5` stream (fixtures).
  const usage = {
    input_tokens: 10,
    cache_creation_input_tokens: 0,
    cache_read_input_tokens: 0,
    cache_creation: { ephemeral_5m_input_tokens: 0, ephemeral_1h_input_tokens: 0 },
    output_tokens: 5,
    service_tier: "standard",
    ...input.usage,
  };
  const events: Array<SseEvent> = [
    {
      type: "message_start",
      message: {
        id: "msg_test",
        type: "message",
        role: "assistant",
        model: "claude-haiku-5-5",
        content: [],
        stop_reason: null,
        stop_sequence: null,
        usage: { ...usage, output_tokens: 1 },
      },
    },
  ];
  input.blocks.forEach((block, index) => {
    events.push(...blockEvents(block, index), { type: "content_block_stop", index });
  });
  if (input.tail !== undefined) {
    events.push(...input.tail);
  } else {
    const delta =
      input.stopDetails === undefined
        ? { stop_reason: input.stopReason, stop_sequence: null }
        : { stop_reason: input.stopReason, stop_sequence: null, stop_details: input.stopDetails };
    events.push({ type: "message_delta", delta, usage }, { type: "message_stop" });
  }
  return sse(events);
};

const blockEvents = (block: Block, index: number): ReadonlyArray<SseEvent> => {
  switch (block.type) {
    case "text":
      return [
        { type: "content_block_start", index, content_block: { type: "text", text: "" } },
        { type: "content_block_delta", index, delta: { type: "text_delta", text: block.text } },
      ];
    case "thinking":
      return [
        {
          type: "content_block_start",
          index,
          content_block: { type: "thinking", thinking: "", signature: "" },
        },
        {
          type: "content_block_delta",
          index,
          delta: { type: "thinking_delta", thinking: block.thinking },
        },
        {
          type: "content_block_delta",
          index,
          delta: { type: "signature_delta", signature: block.signature },
        },
      ];
    case "redacted_thinking":
      return [
        {
          type: "content_block_start",
          index,
          content_block: { type: "redacted_thinking", data: block.data },
        },
      ];
    case "tool_use":
      return [
        {
          type: "content_block_start",
          index,
          content_block: { type: "tool_use", id: block.id, name: block.name, input: {} },
        },
        {
          type: "content_block_delta",
          index,
          delta: { type: "input_json_delta", partial_json: encodeJson(block.input) },
        },
      ];
  }
};

/** A Gemini `streamGenerateContent?alt=sse` response: one `data:` line per chunk, as Google frames it. */
export const geminiStream = (chunks: ReadonlyArray<Schema.JsonObject>): Canned => ({
  status: 200,
  contentType: "text/event-stream",
  response: chunks.map((chunk) => `data: ${encodeJson(chunk)}\n\n`).join(""),
});

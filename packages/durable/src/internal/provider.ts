// The provider session a model request sends.
// `ProviderDoc` itself is the public `ProviderSession` module. The generation and the compaction task share it.
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import { ProviderDoc } from "../ProviderSession.ts";
import type * as Task from "../Task.ts";
import { Tx } from "../Tx.ts";
import { uuidv7 } from "./uuid.ts";

/**
 * The conversation's provider session, without writing in
 * the normal path; a conversation without one gets it in one commit before the request.
 */
export const ensureProviderSessionId = <I, S, R>(runtime: Task.TaskRuntime<I, S, R>) =>
  Effect.gen(function* () {
    const existing = yield* runtime.snapshot(ProviderDoc, runtime.conversationId);
    if (Option.isSome(existing) && existing.value.sessionId !== "") return existing.value.sessionId;
    const fresh = yield* uuidv7;
    let sessionId = fresh;
    yield* runtime.commit(
      Effect.gen(function* () {
        const tx = yield* Tx;
        const provider = yield* tx.doc(ProviderDoc, runtime.conversationId);
        if (provider.sessionId === "") provider.sessionId = fresh;
        sessionId = provider.sessionId;
      }),
    );
    return sessionId;
  });

/**
 * React bindings of `SessionClient`, over `useSyncExternalStore`. Imported as `@effect-durable-harness/host/react`;
 * `react` is an optional peer dependency.
 *
 * ```tsx
 * const { conversations, status } = useSession(session);
 * const { items, live, queued, running, peers } = useConversation(session, conversationId);
 * // An `earlier` item: SessionClient.run(session.loadEarlier(conversationId))
 * ```
 *
 * @since 0.0.0
 */
import { useMemo, useSyncExternalStore } from "react";
import * as ConversationState from "./ConversationState.ts";
import type { SessionClient, SessionState, WatchedConversation } from "./SessionClient.ts";

/**
 * A watched conversation as a component reads it: its UI state (`ConversationState`, from its transcript), its
 * status, raw view and transcript, peers and prompts not yet answered.
 *
 * @category models
 * @since 0.0.0
 */
export interface ConversationSnapshot
  extends ConversationState.ConversationState, WatchedConversation {}

/**
 * The session's state; re-renders on every change.
 *
 * @category hooks
 * @since 0.0.0
 */
export const useSession = (client: SessionClient): SessionState =>
  useSyncExternalStore(client.state.subscribe, client.state.get, client.state.get);

/**
 * A conversation of the session, watched while the component is mounted; re-renders on every change.
 *
 * @category hooks
 * @since 0.0.0
 */
export const useConversation = (
  client: SessionClient,
  conversationId: number,
): ConversationSnapshot => {
  const store = useMemo(() => client.conversation(conversationId), [client, conversationId]);
  const watched = useSyncExternalStore(store.subscribe, store.get, store.get);
  const derived = useMemo(() => ConversationState.make(watched.transcript), [watched.transcript]);
  return useMemo(() => ({ ...derived, ...watched }), [derived, watched]);
};

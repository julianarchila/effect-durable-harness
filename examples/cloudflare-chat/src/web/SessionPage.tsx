/**
 * One session: the conversations on the left, the watched conversation in the middle, and the session's live
 * tasks on the right. Everything shown comes from the session socket, through `SessionClient`, which reconnects,
 * resends unanswered commands and re-watches the conversation on its own; the page holds only what is typed and
 * which conversation is watched.
 */
import { useEffect, useRef, useState, type KeyboardEvent } from "react";
import type * as ConversationState from "@effect-durable-harness/host/ConversationState";
import type * as ConversationTranscript from "@effect-durable-harness/core/ConversationTranscript";
import {
  type ConversationSnapshot,
  useConversation,
  useSession,
} from "@effect-durable-harness/host/react";
import * as SessionClient from "@effect-durable-harness/host/SessionClient";
import {
  type ActivityTask,
  type ConversationSummary,
  MAX_CONTENT_CHARACTERS,
  type WhenBusy,
} from "@effect-durable-harness/host/SessionProtocol";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { ROOT_CONVERSATION_ID } from "../protocol.ts";
import { inputOf, subagentOf, typingNames } from "./chat.ts";
import { Markdown } from "./Markdown.tsx";

const TYPING_IDLE_MS = 3_000;
const showJson = Schema.encodeSync(Schema.fromJsonString(Schema.Json));

// Under `alchemy dev` on Bun, the Vite server's WebSocket proxy drops the upgrade (Bun's `node:http` client reports
// the 101 as a plain response), so in development the socket goes straight to the API Worker. Deployed, it goes
// through the front door like every other request.
const socketBase = (): string =>
  import.meta.env.DEV && import.meta.env["VITE_API_URL"] !== undefined
    ? String(import.meta.env["VITE_API_URL"])
    : window.location.href;

interface SessionPageProps {
  readonly sessionId: string;
  readonly username: string;
  readonly onRename: () => void;
}

export const SessionPage = ({ sessionId, username, onRename }: SessionPageProps) => {
  // The socket opens in an effect, so StrictMode's second mount gets a client of its own.
  const [client, setClient] = useState<SessionClient.SessionClient>();
  useEffect(() => {
    const connected = SessionClient.make({
      url: new URL(`/api/sessions/${sessionId}`, socketBase()).toString(),
      params: [["username", username]],
    });
    setClient(connected);
    return () => connected.close();
  }, [sessionId, username]);
  return client === undefined ? null : (
    <Session client={client} sessionId={sessionId} username={username} onRename={onRename} />
  );
};

interface SessionProps extends SessionPageProps {
  readonly client: SessionClient.SessionClient;
}

/** What the page does: commands, the conversation they open, and the last command's error. */
interface Actions {
  readonly open: (conversationId: number) => void;
  readonly create: () => void;
  readonly fork: (entryId: number) => void;
  readonly submit: (content: string, whenBusy: WhenBusy) => void;
  readonly typing: (on: boolean) => void;
  /** Pages older history in: the view holds only a window of the newest entries. */
  readonly loadEarlier: () => void;
  readonly fillGap: (gap: ConversationTranscript.TranscriptGap) => void;
}

const Session = ({ client, sessionId, username, onRename }: SessionProps) => {
  const session = useSession(client);
  const [watched, setWatched] = useState(ROOT_CONVERSATION_ID);
  const [error, setError] = useState<string>();
  const conversation = useConversation(client, watched);
  // Every command's Effect is run: it sends the command (resent after a reconnect) and waits for the answer.
  const run = <A,>(command: Effect.Effect<A, SessionClient.CommandFailed>, then?: (a: A) => void) =>
    Effect.runFork(
      command.pipe(
        Effect.match({
          onFailure: (failure) => setError(failure.message),
          onSuccess: (value) => {
            setError(undefined);
            then?.(value);
          },
        }),
      ),
    );
  const actions: Actions = {
    open: setWatched,
    create: () => run(client.create(), setWatched),
    fork: (entryId) => run(client.fork(watched, entryId), setWatched),
    submit: (content, whenBusy) => run(client.submit(watched, content, { whenBusy })),
    typing: (on) => client.presence(watched, on ? { typing: true } : null),
    loadEarlier: () => run(client.loadEarlier(watched)),
    fillGap: (gap) => run(client.fillGap(watched, gap)),
  };
  return (
    <div className="session">
      <header className="topbar">
        <span className="brand">durable chat</span>
        <span className="session-id" title="Share this page's link to invite others">
          {sessionId}
        </span>
        <CopyLink />
        <span className={`status status-${session.status}`}>{session.status}</span>
        <span className="spacer" />
        {conversation.status === "ready" ? <Usage conversation={conversation} /> : null}
        <button type="button" className="link" onClick={onRename}>
          {username}
        </button>
      </header>
      <Sidebar
        conversations={session.conversations}
        watched={watched}
        onOpen={actions.open}
        onCreate={actions.create}
      />
      <section className="conversation" aria-label={`Conversation ${watched}`}>
        <Transcript watched={watched} conversation={conversation} actions={actions} />
        <Composer
          key={watched}
          actions={actions}
          typing={typingNames(conversation.peers)}
          error={error}
          busy={conversation.running}
        />
      </section>
      <Activity tasks={session.activity} onOpen={actions.open} />
    </div>
  );
};

const CopyLink = () => {
  const [copied, setCopied] = useState(false);
  return (
    <button
      type="button"
      className="link"
      onClick={() => {
        void navigator.clipboard.writeText(window.location.href).then(() => {
          setCopied(true);
          setTimeout(() => setCopied(false), 1500);
        });
      }}
    >
      {copied ? "Copied" : "Copy link"}
    </button>
  );
};

const money = new Intl.NumberFormat("en", {
  style: "currency",
  currency: "USD",
  maximumFractionDigits: 4,
});
const count = new Intl.NumberFormat("en");

interface UsageProps {
  readonly conversation: ConversationSnapshot;
}

const Usage = ({ conversation }: UsageProps) => (
  <span className="usage">
    <span>{conversation.model ?? "no model"}</span>
    <span>{count.format(conversation.tokens)} tokens</span>
    <span>{money.format(conversation.cost)}</span>
  </span>
);

const labelOf = (conversation: ConversationSummary): string => {
  if (conversation.id === ROOT_CONVERSATION_ID) return "Main";
  if (conversation.owner !== undefined) return `Subagent of #${conversation.owner.conversationId}`;
  if (conversation.parent !== undefined) return `Fork of #${conversation.parent.conversationId}`;
  return "Conversation";
};

interface SidebarProps {
  readonly conversations: ReadonlyArray<ConversationSummary>;
  readonly watched: number;
  readonly onOpen: (conversationId: number) => void;
  readonly onCreate: () => void;
}

const Sidebar = ({ conversations, watched, onOpen, onCreate }: SidebarProps) => (
  <nav className="sidebar" aria-label="Conversations">
    <button type="button" className="primary" onClick={onCreate}>
      New conversation
    </button>
    <ul>
      {conversations.map((conversation) => (
        <li key={conversation.id}>
          <button
            type="button"
            className={conversation.id === watched ? "item selected" : "item"}
            aria-current={conversation.id === watched ? "page" : undefined}
            onClick={() => onOpen(conversation.id)}
          >
            <span className="item-id">#{conversation.id}</span>
            <span>{labelOf(conversation)}</span>
          </button>
        </li>
      ))}
    </ul>
  </nav>
);

const time = new Intl.DateTimeFormat("en", { hour: "2-digit", minute: "2-digit" });

interface TranscriptProps {
  readonly watched: number;
  readonly conversation: ConversationSnapshot;
  readonly actions: Actions;
}

const Transcript = ({ watched, conversation, actions }: TranscriptProps) => {
  const end = useRef<HTMLDivElement>(null);
  // Follows the newest entry, not the count: loading earlier history keeps the reader where they are.
  const newest = conversation.entries.at(-1)?.id;
  const partial = conversation.live?.text.length ?? 0;
  useEffect(() => {
    end.current?.scrollIntoView({ block: "end" });
  }, [newest, partial, watched]);
  if (conversation.status === "missing") {
    return <div className="transcript empty">Conversation #{watched} does not exist.</div>;
  }
  if (conversation.status === "loading") {
    return <div className="transcript empty">Loading conversation #{watched}…</div>;
  }
  return (
    <div className="transcript" aria-live="polite">
      {conversation.entries.length === 0 && conversation.live === undefined ? (
        <p className="empty">
          Nothing here yet. Ask about the weather, roll dice, schedule a reminder, or search past
          conversations.
        </p>
      ) : null}
      {conversation.items.map((item) => {
        if (item.kind === "earlier") {
          return (
            <button key="earlier" type="button" className="page" onClick={actions.loadEarlier}>
              Load earlier messages
            </button>
          );
        }
        if (item.kind === "gap") {
          return (
            <button
              key={`gap-${item.after}`}
              type="button"
              className="page"
              onClick={() => actions.fillGap(item)}
            >
              Load missing messages
            </button>
          );
        }
        return (
          <Entry
            key={item.entry.id}
            entry={item.entry}
            onFork={() => actions.fork(item.entry.id)}
            onOpen={actions.open}
          />
        );
      })}
      {conversation.tools.length === 0 ? null : (
        <div className="tools">
          {conversation.tools.map((tool) => (
            <ToolSlot key={tool.callId} tool={tool} onOpen={actions.open} />
          ))}
        </div>
      )}
      {conversation.live === undefined ? null : (
        <article className="message assistant streaming">
          <div className="meta">assistant</div>
          <div className="text">
            {conversation.live.text}
            <span className="cursor" aria-hidden="true" />
          </div>
          {conversation.live.retry === undefined ? null : (
            <div className="notice">Retrying: {conversation.live.retry}</div>
          )}
        </article>
      )}
      {conversation.queued.map((item) => (
        <article key={`queued-${item.id}`} className="message user queued">
          <div className="meta">
            {item.steer ? "steering: read at the next step" : "queued for the next turn"}
          </div>
          <div className="text">{inputOf(item.text).text}</div>
        </article>
      ))}
      {conversation.sending.map((item) => (
        <article key={item.requestId} className="message user queued">
          <div className="meta">sending</div>
          <div className="text">{item.content}</div>
        </article>
      ))}
      <div ref={end} />
    </div>
  );
};

interface EntryProps {
  readonly entry: ConversationState.EntryView;
  readonly onFork: () => void;
  readonly onOpen: (conversationId: number) => void;
}

const Entry = ({ entry, onFork, onOpen }: EntryProps) => {
  const at = entry.at === undefined ? undefined : time.format(entry.at);
  switch (entry.role) {
    case "user": {
      const input = inputOf(entry.text);
      return input.kind === "scheduled" ? (
        <article className="message scheduled">
          <div className="meta">
            <span>Scheduled instruction</span>
            {at === undefined ? null : <time>{at}</time>}
          </div>
          <div className="text">{input.text}</div>
        </article>
      ) : (
        <article className="message user">
          <div className="meta">
            <span>{input.username ?? "user"}</span>
            {at === undefined ? null : <time>{at}</time>}
            <button type="button" className="link fork" onClick={onFork}>
              Fork from here
            </button>
          </div>
          <div className="text">{input.text}</div>
        </article>
      );
    }
    case "assistant":
      return (
        <article className="message assistant">
          <div className="meta">
            <span>assistant</span>
            {at === undefined ? null : <time>{at}</time>}
          </div>
          {entry.text === "" ? null : <Markdown text={entry.text} />}
          {entry.toolCalls.map((call) => (
            <div key={call.callId} className="call">
              <code>{call.name}</code> <code className="args">{showJson(call.args)}</code>
            </div>
          ))}
          {entry.stopped === undefined ? null : (
            <div className="notice">Stopped: {entry.stopped}</div>
          )}
        </article>
      );
    case "tool": {
      const result = entry.toolResult;
      const child = subagentOf(result?.details);
      return (
        <article className={result?.isError === true ? "result error" : "result"}>
          <div className="meta">
            <code>{result?.name}</code>
            {result?.isError === true ? <span>failed</span> : null}
            {child === undefined ? null : (
              <button type="button" className="link" onClick={() => onOpen(child)}>
                Open subagent #{child}
              </button>
            )}
          </div>
          <pre>{entry.text}</pre>
        </article>
      );
    }
    case "data":
      return null;
  }
};

interface ToolSlotProps {
  readonly tool: ConversationState.LiveToolView;
  readonly onOpen: (conversationId: number) => void;
}

const ToolSlot = ({ tool, onOpen }: ToolSlotProps) => {
  const child = subagentOf(tool.details);
  return (
    <div className={`slot slot-${tool.status}`}>
      <code>{tool.name}</code>
      <span className="slot-status">{tool.status}</span>
      {child === undefined ? null : (
        <button type="button" className="link" onClick={() => onOpen(child)}>
          Watch subagent #{child}
        </button>
      )}
      {tool.output === undefined ? null : <pre>{tool.output}</pre>}
    </div>
  );
};

interface ComposerProps {
  readonly actions: Actions;
  readonly typing: ReadonlyArray<string>;
  readonly error: string | undefined;
  /** The agent is working: a message can steer the run or follow it up. */
  readonly busy: boolean;
}

const Composer = ({ actions, typing, error, busy }: ComposerProps) => {
  const [draft, setDraft] = useState("");
  const idle = useRef<ReturnType<typeof setTimeout>>(undefined);
  const active = useRef(false);
  // The latest actions, for the unmount cleanup: they are a new object on every render.
  const latest = useRef(actions);
  latest.current = actions;
  const setTyping = (on: boolean) => {
    clearTimeout(idle.current);
    if (on) idle.current = setTimeout(() => setTyping(false), TYPING_IDLE_MS);
    if (active.current === on) return;
    active.current = on;
    actions.typing(on);
  };
  useEffect(
    () => () => {
      clearTimeout(idle.current);
      if (active.current) latest.current.typing(false);
    },
    [],
  );
  const send = (whenBusy: WhenBusy) => {
    const content = draft.trim();
    if (content === "") return;
    actions.submit(content, whenBusy);
    setDraft("");
    setTyping(false);
  };
  const onKeyDown = (event: KeyboardEvent<HTMLTextAreaElement>) => {
    if (event.key === "Enter" && !event.shiftKey && !event.nativeEvent.isComposing) {
      event.preventDefault();
      send(event.metaKey || event.ctrlKey ? "steer" : "followUp");
    }
  };
  return (
    <form
      className="composer"
      onSubmit={(event) => {
        event.preventDefault();
        send("followUp");
      }}
    >
      <div className="presence" aria-live="polite">
        {typing.length === 0
          ? null
          : `${typing.join(", ")} ${typing.length === 1 ? "is" : "are"} typing…`}
        {error === undefined ? null : <span className="error">{error}</span>}
      </div>
      <div className="composer-row">
        <textarea
          aria-label="Message"
          rows={2}
          value={draft}
          maxLength={MAX_CONTENT_CHARACTERS}
          placeholder={
            busy
              ? "The agent is working. Enter queues for the next turn, Ctrl+Enter steers this one."
              : "Message the agent. Enter sends, Shift+Enter adds a line."
          }
          onChange={(event) => {
            setDraft(event.target.value);
            setTyping(event.target.value !== "");
          }}
          onKeyDown={onKeyDown}
        />
        {busy ? (
          <button
            type="button"
            className="secondary"
            disabled={draft.trim() === ""}
            title="Join the run in progress: the agent reads it before its next step"
            onClick={() => send("steer")}
          >
            Steer
          </button>
        ) : null}
        <button type="submit" className="primary" disabled={draft.trim() === ""}>
          {busy ? "Queue" : "Send"}
        </button>
      </div>
    </form>
  );
};

interface ActivityProps {
  readonly tasks: ReadonlyArray<ActivityTask>;
  readonly onOpen: (conversationId: number) => void;
}

const Activity = ({ tasks, onOpen }: ActivityProps) => (
  <aside className="activity" aria-label="Activity">
    <h2>Activity</h2>
    {tasks.length === 0 ? <p className="empty">No tasks running.</p> : null}
    <ul>
      {tasks.map((task) => (
        <li key={task.id} className={task.abortRequested ? "task aborting" : "task"}>
          <div className="task-head">
            <code>{task.kind}</code>
            <span className="task-status">{task.status}</span>
          </div>
          <div className="task-meta">
            <span>#{task.id}</span>
            <button type="button" className="link" onClick={() => onOpen(task.conversationId)}>
              conversation #{task.conversationId}
            </button>
            {task.phase === undefined ? null : <span>phase {task.phase}</span>}
            {task.owner === undefined ? null : <span>owned by #{task.owner}</span>}
            {task.background ? <span>background</span> : null}
            {task.waitingOn === undefined || task.waitingOn.length === 0 ? null : (
              <span>waiting on {task.waitingOn.map((id) => `#${id}`).join(", ")}</span>
            )}
          </div>
        </li>
      ))}
    </ul>
  </aside>
);

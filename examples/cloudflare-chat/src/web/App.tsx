/**
 * The app: a landing page that creates a session, and the session page at `/sessions/:id`. A session's link is
 * all another person needs to join it; each person picks a display name, kept in this browser.
 */
import { useEffect, useState } from "react";
import * as Schema from "effect/Schema";
import { normalizeUsername } from "../protocol.ts";
import { SessionPage } from "./SessionPage.tsx";

const USERNAME_KEY = "durable-chat:username";
const sessionPath = /^\/sessions\/([a-zA-Z0-9_-]+)\/?$/u;
const Created = Schema.Struct({ sessionId: Schema.String });

const usePath = () => {
  const [path, setPath] = useState(window.location.pathname);
  useEffect(() => {
    const changed = () => setPath(window.location.pathname);
    window.addEventListener("popstate", changed);
    return () => window.removeEventListener("popstate", changed);
  }, []);
  const navigate = (next: string) => {
    window.history.pushState(null, "", next);
    setPath(next);
  };
  return [path, navigate] as const;
};

export const App = () => {
  const [path, navigate] = usePath();
  const [username, setUsername] = useState(() => localStorage.getItem(USERNAME_KEY) ?? "");
  const sessionId = sessionPath.exec(path)?.[1];
  const remember = (name: string) => {
    localStorage.setItem(USERNAME_KEY, name);
    setUsername(name);
  };
  if (sessionId !== undefined && username !== "") {
    return (
      <SessionPage
        key={`${sessionId}:${username}`}
        sessionId={sessionId}
        username={username}
        onRename={() => setUsername("")}
      />
    );
  }
  return (
    <Landing
      joining={sessionId}
      initial={username}
      onReady={(name, joined) => {
        remember(name);
        if (joined !== undefined) navigate(`/sessions/${joined}`);
      }}
    />
  );
};

interface LandingProps {
  readonly joining: string | undefined;
  readonly initial: string;
  readonly onReady: (username: string, sessionId: string | undefined) => void;
}

const Landing = ({ joining, initial, onReady }: LandingProps) => {
  const [name, setName] = useState(initial);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string>();
  const username = normalizeUsername(name);
  const start = async () => {
    if (joining !== undefined) return onReady(username, joining);
    setBusy(true);
    setError(undefined);
    try {
      const response = await fetch("/api/sessions", { method: "POST" });
      if (response.status !== 201) throw new Error(`The API answered ${response.status}`);
      const created = Schema.decodeUnknownSync(Created)(await response.json());
      onReady(username, created.sessionId);
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : "Could not create a session");
      setBusy(false);
    }
  };
  return (
    <main className="landing">
      <h1>durable chat</h1>
      <p className="lede">
        A shared, durable agent session on a Durable Object. Everyone with the link sees the same
        conversations, live; a restart of the object loses nothing.
      </p>
      <form
        className="landing-form"
        onSubmit={(event) => {
          event.preventDefault();
          if (username !== "") void start();
        }}
      >
        <label htmlFor="username">Your name</label>
        <input
          id="username"
          autoFocus
          autoComplete="nickname"
          value={name}
          maxLength={60}
          onChange={(event) => setName(event.target.value)}
          placeholder="Ada"
        />
        <button type="submit" disabled={busy || username === ""}>
          {joining === undefined ? (busy ? "Creating…" : "Create a session") : "Join the session"}
        </button>
        {error === undefined ? null : <p className="error">{error}</p>}
      </form>
    </main>
  );
};

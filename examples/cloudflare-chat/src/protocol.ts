/**
 * What the object and the web app agree on beyond the host's session socket (`@effect-durable-harness/host/SessionProtocol`):
 * display names and how a prompt is attributed to its sender, and how a scheduled delivery frames its input.
 */

/** The root conversation every session starts with (`@effect-durable-harness/core/Ids`, without bundling the kernel). */
export const ROOT_CONVERSATION_ID = 1;
export const MAX_USERNAME_CHARACTERS = 40;

// ─── Display names and attribution ─────────────────────────────────────────

/** A display name: printable, no brackets (they frame attribution), collapsed whitespace, at most 40 characters. */
export const normalizeUsername = (value: string): string =>
  Array.from(
    value
      .normalize("NFKC")
      .replace(/[\p{Cc}\p{Cf}]/gu, " ")
      .replace(/[[\]]/gu, "")
      .replace(/\s+/gu, " ")
      .trim(),
  )
    .slice(0, MAX_USERNAME_CHARACTERS)
    .join("");

/** A prompt as the model reads it: the sender's name in brackets on its own line. */
export const attributedPrompt = (username: string, content: string): string =>
  `[${username}]\n${content}`;

/** The name and text of an attributed prompt, or the text alone for one that is not. */
export const parseAttribution = (
  text: string,
): { readonly username?: string; readonly text: string } => {
  const match = /^\[([^\]\n]+)\]\n/u.exec(text);
  return match === null
    ? { text }
    : { username: match[1] ?? "", text: text.slice(match[0].length) };
};

// ─── Scheduled instructions ────────────────────────────────────────────────

const SCHEDULED_PREFIX = "Scheduled instruction due now: ";

/** The input a scheduled delivery submits. Self-contained: no prompt section needed to understand it. */
export const scheduledInput = (instruction: string): string => `${SCHEDULED_PREFIX}${instruction}`;

/** The instruction of a scheduled delivery's input, or `undefined` for an ordinary one. */
export const parseScheduledInput = (text: string): string | undefined =>
  text.startsWith(SCHEDULED_PREFIX) ? text.slice(SCHEDULED_PREFIX.length) : undefined;

// Tool output handling: sanitizing, bounding to head or tail windows, and the
// running `OutputBuffer` of one tool call. Synchronous and Effect-free: it runs
// inside the tool's progress write, whose content is captured in the caller's
// step. The `Progress` throttle lives in `progress.ts`, because it is an Effect
// primitive.
//
// Also here: the default limits `DEFAULT_MAX_BYTES` and `DEFAULT_MAX_LINES`, and
// `utf8ByteLength`. The skip and window types are the public `OutputSkip` and
// `OutputWindow` of `AgentTool.ts`.
import type { OutputLimits, OutputSkip } from "../AgentTool.ts";

/** Default line limit of a tool's output. */
export const DEFAULT_MAX_LINES = 2000;
/** Default byte limit of a tool's output (50 KiB). */
export const DEFAULT_MAX_BYTES = 50 * 1024;

// oxlint-disable-next-line eslint/no-control-regex -- the range starts at U+0000 on purpose: every ASCII character
const nonAsciiPattern = /[^\x00-\x7f]/;

/**
 * UTF-8 byte length of `content`; a lone surrogate counts as the three bytes of U+FFFD.
 *
 * A plain loop rather than Node's `Buffer.byteLength`: reaching for `Buffer` needs a cast on `globalThis`, and the
 * loop gives the same counts on every runtime.
 */
export function utf8ByteLength(content: string): number {
  const firstNonAscii = content.search(nonAsciiPattern);
  if (firstNonAscii === -1) return content.length;
  let bytes = firstNonAscii;
  for (let i = firstNonAscii; i < content.length; i++) {
    const code = content.charCodeAt(i);
    if (code <= 0x7f) {
      bytes += 1;
    } else if (code <= 0x7ff) {
      bytes += 2;
    } else if (code >= 0xd800 && code <= 0xdbff && i + 1 < content.length) {
      const next = content.charCodeAt(i + 1);
      if (next >= 0xdc00 && next <= 0xdfff) {
        bytes += 4;
        i++;
      } else {
        bytes += 3;
      }
    } else {
      bytes += 3;
    }
  }
  return bytes;
}

/** A tool's output limits with the defaults applied (`toolPlan.resolveLimits`). */
export type ResolvedOutputLimits = {
  readonly [K in keyof OutputLimits]-?: Exclude<OutputLimits[K], undefined>;
};

/** Retained output and what the limits dropped. */
export interface BoundedOutput {
  readonly text: string;
  readonly droppedBytes: number;
  readonly droppedLines: number;
}

/** An exact slice of the input within the limits, and what it left out. */
export interface OutputSlice {
  readonly text: string;
  readonly bytes: number;
  readonly droppedBytes: number;
  readonly droppedLines: number;
}

const NEWLINE = 0x0a;
// Control characters except tab and newline, and the interlinear annotation characters.
// oxlint-disable-next-line eslint/no-control-regex -- matching control characters is the point
const INVALID_OUTPUT = /[\x00-\x08\x0b-\x1f\ufff9-\ufffb]/g;
const encoder = new TextEncoder();
/** Slices decode exactly: a U+FEFF at a slice's start is text, not a byte-order mark. */
const decoder = new TextDecoder("utf-8", { ignoreBOM: true });

/** Remove control characters that break display and transcripts; tabs and newlines stay. */
export function sanitizeOutput(text: string): string {
  return text.replace(INVALID_OUTPUT, "");
}

/**
 * Bound `text` to whole lines within the limits: the first lines for `head`, the last lines for `tail`. The result is an
 * exact slice, trailing newline included. A single line longer than `maxBytes` is cut at the byte limit on a character
 * boundary.
 */
export function boundOutput(text: string, limits: ResolvedOutputLimits): OutputSlice {
  const bytes = encoder.encode(text);
  const [from, to] = limits.retain === "head" ? headRange(bytes, limits) : tailRange(bytes, limits);
  const kept = bytes.subarray(from, to);
  return {
    text: kept.length === bytes.length ? text : decoder.decode(kept),
    bytes: kept.length,
    droppedBytes: bytes.length - kept.length,
    droppedLines: lineCount(bytes) - lineCount(kept),
  };
}

function headRange(bytes: Uint8Array, limits: ResolvedOutputLimits): readonly [number, number] {
  if (limits.maxLines === 0 || limits.maxBytes === 0) return [0, 0];
  let end = bytes.length;
  let lines = 0;
  for (
    let index = bytes.indexOf(NEWLINE);
    index !== -1;
    index = bytes.indexOf(NEWLINE, index + 1)
  ) {
    if (++lines === limits.maxLines) {
      end = index + 1;
      break;
    }
  }
  if (end > limits.maxBytes) {
    const newline = bytes.lastIndexOf(NEWLINE, limits.maxBytes - 1);
    end = newline === -1 ? characterEnd(bytes, limits.maxBytes) : newline + 1;
  }
  return [0, end];
}

function tailRange(bytes: Uint8Array, limits: ResolvedOutputLimits): readonly [number, number] {
  if (limits.maxLines === 0 || limits.maxBytes === 0) return [bytes.length, bytes.length];
  // A trailing newline ends the last line rather than starting another.
  const last = bytes[bytes.length - 1] === NEWLINE ? bytes.length - 2 : bytes.length - 1;
  let start = 0;
  let lines = 1;
  for (let index = last < 0 ? -1 : bytes.lastIndexOf(NEWLINE, last); index !== -1; ) {
    if (lines === limits.maxLines) {
      start = index + 1;
      break;
    }
    lines++;
    index = index === 0 ? -1 : bytes.lastIndexOf(NEWLINE, index - 1);
  }
  if (bytes.length - start > limits.maxBytes) {
    const from = bytes.length - limits.maxBytes;
    const newline = bytes.indexOf(NEWLINE, from - 1);
    // The first line starting inside the byte window, or a cut of the last line when it
    // alone is too long.
    start =
      newline !== -1 && newline + 1 < bytes.length ? newline + 1 : characterStart(bytes, from);
  }
  return [start, bytes.length];
}

/** The last character boundary at or before `index`. */
export function characterEnd(bytes: Uint8Array, index: number): number {
  let end = index;
  while (end > 0 && ((bytes[end] ?? 0) & 0xc0) === 0x80) end--;
  return end;
}

/** The first character boundary at or after `index`. */
function characterStart(bytes: Uint8Array, index: number): number {
  let start = index;
  while (start < bytes.length && ((bytes[start] ?? 0) & 0xc0) === 0x80) start++;
  return start;
}

function lineCount(bytes: Uint8Array): number {
  if (bytes.length === 0) return 0;
  let newlines = 0;
  for (
    let index = bytes.indexOf(NEWLINE);
    index !== -1;
    index = bytes.indexOf(NEWLINE, index + 1)
  ) {
    newlines++;
  }
  return newlines + (bytes[bytes.length - 1] === NEWLINE ? 0 : 1);
}

interface StoredChunk {
  readonly text: string;
  readonly bytes: number;
  readonly newlines: number;
}

/**
 * Bounded running output of one tool call. Accepting a chunk costs time proportional to the chunk: head retention stops
 * storing once the window is full, and tail retention drops stored text the window no longer needs when it snapshots.
 * Counts of the whole stream are kept so the dropped totals stay exact.
 */
export class OutputBuffer {
  readonly #limits: ResolvedOutputLimits;
  /**
   * Decoder of byte chunks. A string chunk or a skip ends an incomplete character of earlier bytes (it becomes U+FFFD);
   * the next byte chunk then starts a new character. Only a byte-order mark at the very start of the output is
   * dropped, never a U+FEFF later in it.
   */
  readonly #decoder = new TextDecoder("utf-8", { ignoreBOM: true });
  #started = false;
  /** Stored chunks: for head the start of the stream, for tail a suffix that still contains the next window. */
  #chunks: Array<StoredChunk> = [];
  #storedBytes = 0;
  #storedNewlines = 0;
  #full = false;
  #totalBytes = 0;
  #totalNewlines = 0;
  #endsWithNewline = true;

  constructor(limits: ResolvedOutputLimits) {
    this.#limits = limits;
  }

  /** Bytes currently held; bounded by the limits plus one chunk. */
  get storedBytes(): number {
    return this.#storedBytes;
  }

  /**
   * Accept a chunk; returns whether anything was accepted. `skipped` is output omitted right before the chunk, which
   * must be more than the tail window by at least one byte or line; only tail retention accepts it.
   */
  push(chunk: string | Uint8Array, skipped?: OutputSkip | undefined): boolean {
    // `instanceof Uint8Array` tells a string chunk from bytes: `no-runtime-typeof` forbids `typeof` here.
    const bytes = chunk instanceof Uint8Array ? chunk : undefined;
    // Bytes of an incomplete character from an earlier byte chunk come first.
    const pending = bytes === undefined || skipped !== undefined ? this.#decoder.decode() : "";
    const decoded =
      chunk instanceof Uint8Array ? this.#decoder.decode(chunk, { stream: true }) : chunk;
    const first = !this.#started && pending === "" && skipped === undefined;
    if (pending !== "" || decoded !== "" || skipped !== undefined) this.#started = true;
    const text =
      first && bytes !== undefined && decoded.startsWith("\ufeff") ? decoded.slice(1) : decoded;
    if (skipped === undefined) return this.#accept(pending + text);
    // A skip with head retention is a broken caller, not a condition anyone handles: a violated invariant, which
    // inside the tool's Effect surfaces as a defect.
    if (this.#limits.retain !== "tail") throw new Error("Skipped output requires tail retention");
    this.#accept(pending);
    this.#skip(skipped);
    this.#accept(text);
    return true;
  }

  /** Count omitted output; nothing stored before it can be in the window once the text after it arrives. */
  #skip(skipped: OutputSkip): void {
    if (skipped.bytes === 0) return;
    this.#totalBytes += skipped.bytes;
    this.#totalNewlines += skipped.newlines;
    this.#endsWithNewline = skipped.endsWithNewline;
    this.#chunks = [];
    this.#storedBytes = 0;
    this.#storedNewlines = 0;
  }

  /** Flush an incomplete trailing character as a replacement character; call when the stream ends. */
  end(): void {
    this.#accept(this.#decoder.decode());
  }

  #accept(text: string): boolean {
    if (text.length === 0) return false;
    const bytes = utf8ByteLength(text);
    const newlines = countNewlines(text);
    this.#totalBytes += bytes;
    this.#totalNewlines += newlines;
    this.#endsWithNewline = text.endsWith("\n");
    if (this.#full) return true;
    this.#chunks.push({ text, bytes, newlines });
    this.#storedBytes += bytes;
    this.#storedNewlines += newlines;
    if (this.#limits.retain === "head") {
      // Nothing past a full window is ever needed.
      this.#full =
        this.#storedBytes > this.#limits.maxBytes || this.#storedNewlines >= this.#limits.maxLines;
      return true;
    }
    // Drop leading chunks while the rest still holds more than a window: more than
    // `maxBytes` bytes or `maxLines` newlines, plus one, so the window's line start can still be found. Each chunk
    // is dropped once.
    for (
      let first = this.#chunks[0];
      first !== undefined && this.#chunks.length > 1;
      first = this.#chunks[0]
    ) {
      const bytesAfter = this.#storedBytes - first.bytes;
      const newlinesAfter = this.#storedNewlines - first.newlines;
      if (bytesAfter <= this.#limits.maxBytes + 1 && newlinesAfter <= this.#limits.maxLines + 1)
        break;
      this.#chunks.shift();
      this.#storedBytes = bytesAfter;
      this.#storedNewlines = newlinesAfter;
    }
    return true;
  }

  /** Retained, sanitized output and what the limits dropped from the whole stream. */
  snapshot(): BoundedOutput {
    const only = this.#chunks.length === 1 ? this.#chunks[0] : undefined;
    const stored =
      only !== undefined ? only.text : this.#chunks.map((chunk) => chunk.text).join("");
    const kept = boundOutput(stored, this.#limits);
    const storedLines = lines(this.#storedNewlines, stored === "" || stored.endsWith("\n"));
    const keptLines = storedLines - kept.droppedLines;
    // Tail windows never reach back before this one, but finding a later window's first
    // line needs what precedes it: keep the shortest suffix longer than the window by a byte or a line, as `#accept`
    // does.
    if (this.#limits.retain === "tail" || this.#chunks.length > 1) {
      const text = this.#limits.retain === "tail" ? tailMargin(stored, this.#limits) : stored;
      const bytes = this.#limits.retain === "tail" ? utf8ByteLength(text) : this.#storedBytes;
      this.#chunks = text === "" ? [] : [{ text, bytes, newlines: countNewlines(text) }];
      this.#storedBytes = bytes;
      this.#storedNewlines = this.#chunks[0]?.newlines ?? 0;
    }
    return {
      text: sanitizeOutput(kept.text),
      droppedBytes: this.#totalBytes - kept.bytes,
      droppedLines: lines(this.#totalNewlines, this.#endsWithNewline) - keptLines,
    };
  }
}

/**
 * The shortest suffix of `text` with more than `maxBytes` bytes or more than `maxLines` newlines, or all of it. The
 * tail window of any text that ends with this suffix, followed by anything, is the same as of `text` followed by it.
 */
function tailMargin(text: string, limits: ResolvedOutputLimits): string {
  const bytes = encoder.encode(text);
  const byteStart =
    bytes.length > limits.maxBytes ? characterEnd(bytes, bytes.length - limits.maxBytes - 1) : 0;
  let lineStart = 0;
  let newlines = 0;
  for (
    let index = bytes.lastIndexOf(NEWLINE);
    index !== -1;
    index = bytes.lastIndexOf(NEWLINE, index - 1)
  ) {
    if (++newlines > limits.maxLines) {
      lineStart = index;
      break;
    }
    if (index === 0) break;
  }
  const start = Math.max(byteStart, lineStart);
  return start === 0 ? text : decoder.decode(bytes.subarray(start));
}

/** Lines of text with `newlines` newlines; a final unterminated line counts. */
function lines(newlines: number, terminated: boolean): number {
  return newlines + (terminated ? 0 : 1);
}

function countNewlines(text: string): number {
  let count = 0;
  for (let index = text.indexOf("\n"); index !== -1; index = text.indexOf("\n", index + 1)) count++;
  return count;
}

/** Each progress commit also buys a pause proportional to what it wrote. */
export const PROGRESS_BYTES_PER_SECOND = 100 * 1024;

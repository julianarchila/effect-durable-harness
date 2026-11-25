/**
 * The little markdown a model answers with: headings, paragraphs, `*` / `-` / numbered lists, `**bold**`, `*italic*` and
 * `` `code` ``. Built as React elements, never as HTML, so a reply cannot inject markup.
 */
import type { ReactNode } from "react";

const inline = /(\*\*[^*\n]+\*\*|`[^`\n]+`|\*[^*\s][^*\n]*\*)/u;

const renderInline = (text: string): ReactNode[] =>
  text.split(inline).map((part, index) => {
    if (part.startsWith("**") && part.endsWith("**") && part.length > 4) {
      return <strong key={index}>{part.slice(2, -2)}</strong>;
    }
    if (part.startsWith("`") && part.endsWith("`") && part.length > 2) {
      return <code key={index}>{part.slice(1, -1)}</code>;
    }
    if (part.startsWith("*") && part.endsWith("*") && part.length > 2) {
      return <em key={index}>{part.slice(1, -1)}</em>;
    }
    return part;
  });

const heading = /^\s*#{1,6}\s+(.*)$/u;
const bullet = /^\s*([*-]|\d+\.)\s+(.*)$/u;

interface MarkdownProps {
  readonly text: string;
}

export const Markdown = ({ text }: MarkdownProps) => {
  const blocks: ReactNode[] = [];
  let items: string[] = [];
  let ordered = false;
  let lines: string[] = [];
  const flushList = () => {
    if (items.length === 0) return;
    const rendered = items.map((item, index) => <li key={index}>{renderInline(item)}</li>);
    blocks.push(
      ordered ? <ol key={blocks.length}>{rendered}</ol> : <ul key={blocks.length}>{rendered}</ul>,
    );
    items = [];
  };
  const flushParagraph = () => {
    if (lines.length === 0) return;
    blocks.push(<p key={blocks.length}>{renderInline(lines.join("\n"))}</p>);
    lines = [];
  };
  for (const line of text.split("\n")) {
    const title = heading.exec(line);
    const item = bullet.exec(line);
    if (title !== null) {
      flushParagraph();
      flushList();
      blocks.push(
        <p key={blocks.length} className="heading">
          {renderInline(title[1] ?? "")}
        </p>,
      );
    } else if (item !== null) {
      flushParagraph();
      if (items.length === 0) ordered = /\d/u.test(item[1] ?? "");
      items.push(item[2] ?? "");
    } else if (line.trim() === "") {
      flushParagraph();
      flushList();
    } else {
      flushList();
      lines.push(line);
    }
  }
  flushParagraph();
  flushList();
  return <div className="text markdown">{blocks}</div>;
};

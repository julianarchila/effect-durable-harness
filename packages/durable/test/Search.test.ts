// Tests of the ranking and tokenizer of `tool_search`, pure functions of `internal/search.ts`.
import { assert, describe, it } from "@effect/vitest";
import { rankDocuments, type SearchDocument, tokenize } from "../src/internal/search.ts";

function doc(name: string, text: string, namespaceText?: string): SearchDocument {
  return namespaceText === undefined ? { name, text } : { name, text, namespaceText };
}

describe("tokenize", () => {
  it("splits camelCase and snake_case, drops stop words, and folds plurals", () => {
    assert.deepStrictEqual(tokenize("listIssues for the GitHub_repo"), [
      "list",
      "issue",
      "git",
      "hub",
      "repo",
    ]);
    assert.deepStrictEqual(tokenize("searches queries HTTPServer"), [
      "search",
      "query",
      "http",
      "server",
    ]);
    assert.deepStrictEqual(tokenize("repo.issues/search:queries"), [
      "repo",
      "issue",
      "search",
      "query",
    ]);
  });

  it.each([
    ["搜索仓库问题", ["搜索", "仓库", "问题"]],
    ["文書を検索", ["文書", "を", "検索"]],
    ["문서 검색", ["문서", "검색"]],
    ["ค้นหาเอกสาร", ["ค้นหา", "เอกสาร"]],
    ["Поиск документов", ["поиск", "документов"]],
    ["البحث عن الملفات", ["البحث", "عن", "الملفات"]],
    ["फ़ाइल खोजें", ["फ़ाइल", "खोजें"]],
    ["Éléments cafés", ["éléments", "cafés"]],
  ])("segments Unicode words in %s", (text, expected) => {
    assert.deepStrictEqual(tokenize(text), expected);
  });

  it("preserves non-ASCII words and splits mixed-language identifiers", () => {
    assert.deepStrictEqual(tokenize("搜索GitHub_repoIssues résumé cafés"), [
      "搜索",
      "git",
      "hub",
      "repo",
      "issue",
      "résumé",
      "cafés",
    ]);
  });

  it("normalizes canonically equivalent Unicode text", () => {
    assert.deepStrictEqual(tokenize("CAFÉ cafe\u0301"), ["café", "café"]);
  });

  it("ignores whitespace, punctuation, and emoji", () => {
    assert.deepStrictEqual(tokenize(" \t，。!? 😀 "), []);
  });
});

describe("rankDocuments", () => {
  const documents = [
    doc("mcp__github__list_issues", "List issues in a repository. state open or closed"),
    doc("mcp__github__create_pull_request", "Open a pull request."),
    doc("mcp__linear__search_issues", "Search Linear issues by text."),
    doc("mcp__docs__search", "Search the documentation."),
  ];

  it("ranks by term relevance and respects the limit", () => {
    assert.deepStrictEqual(rankDocuments("issue", documents, 8), [
      "mcp__linear__search_issues",
      "mcp__github__list_issues",
    ]);
    assert.strictEqual(
      rankDocuments("pull requests", documents, 8)[0],
      "mcp__github__create_pull_request",
    );
    assert.lengthOf(rankDocuments("search", documents, 1), 1);
    assert.deepStrictEqual(rankDocuments("closed", documents, 8), ["mcp__github__list_issues"]);
  });

  it.each([
    ["搜索仓库问题", "问题"],
    ["文書を検索", "検索"],
    ["문서 검색", "검색"],
    ["ค้นหาเอกสาร", "เอกสาร"],
    ["Поиск документов", "документов"],
    ["البحث عن الملفات", "الملفات"],
    ["फ़ाइल खोजें", "खोजें"],
    ["Rechercher un café", "cafe\u0301"],
  ])("matches Unicode queries against %s", (text, query) => {
    assert.deepStrictEqual(rankDocuments(query, [...documents, doc("localized", text)], 8), [
      "localized",
    ]);
  });

  it("returns nothing for unknown or empty queries", () => {
    assert.deepStrictEqual(rankDocuments("kubernetes", documents, 8), []);
    assert.deepStrictEqual(rankDocuments("the", documents, 8), []);
    assert.deepStrictEqual(rankDocuments("未知", documents, 8), []);
  });

  it("searches the namespace text", () => {
    const namespaced = [doc("mcp__x__run", "Run it.", "mcp__x Kubernetes cluster tools")];
    assert.deepStrictEqual(rankDocuments("kubernetes", namespaced, 8), ["mcp__x__run"]);
  });

  it("ranks a tool first when the query is its name", () => {
    const named = [
      doc("mcp__app__queue_read", "Read a queued message of this thread."),
      doc(
        "mcp__dev-app__thread_read",
        "Read durable state and a paginated timeline from any thread. The default view returns user messages, assistant messages and plans. Continue with afterPosition until hasMore is false.",
      ),
    ];
    for (const query of [
      "thread_read",
      "dev_app__thread_read",
      "mcp__dev-app__thread_read",
      "Thread_Read",
    ]) {
      assert.deepStrictEqual(rankDocuments(query, named, 1), ["mcp__dev-app__thread_read"]);
    }
    // Partial names still rank by BM25.
    assert.deepStrictEqual(rankDocuments("read", named, 1), ["mcp__app__queue_read"]);
  });

  it("ranks a name match above mentions in descriptions", () => {
    const ci = [
      doc("mcp__ci__status", "Deploy status: last deploy, deploy log, deploy errors."),
      doc(
        "mcp__ci__deploy_release",
        "Start a release of the current branch to an environment, run the migrations, switch traffic over, and wait until the new version is live and healthy.",
      ),
    ];
    assert.deepStrictEqual(rankDocuments("deploy", ci, 1), ["mcp__ci__deploy_release"]);
  });

  it("ranks a tool first when the query contains its whole name", () => {
    const peekaboo = "mcp__peekaboo Peekaboo controls macOS apps by app name.";
    const named = [
      doc(
        "mcp__peekaboo__see",
        "Capture the app window of the peekaboo app and describe the app.",
        peekaboo,
      ),
      doc("mcp__peekaboo__menu", "Click a menu item of an app.", peekaboo),
      doc(
        "mcp__peekaboo__app",
        "Launch, quit, relaunch, focus, hide, or switch applications. Targets an application by name, bundle identifier, or process ID, and can wait until it is ready.",
        peekaboo,
      ),
      doc("mcp__app__queue_read", "Read a queued message of this thread."),
      doc(
        "mcp__dev-app__thread_read",
        "Read durable state and a paginated timeline from any thread. The default view returns user messages, assistant messages and plans.",
      ),
    ];
    assert.deepStrictEqual(rankDocuments("peekaboo app", named, 1), ["mcp__peekaboo__app"]);
    assert.deepStrictEqual(rankDocuments("open the peekaboo app tool", named, 1), [
      "mcp__peekaboo__app",
    ]);
    assert.deepStrictEqual(rankDocuments("read thread_read state", named, 1), [
      "mcp__dev-app__thread_read",
    ]);
    // Without the whole name, mentions in descriptions decide.
    assert.deepStrictEqual(rankDocuments("describe window", named, 1), ["mcp__peekaboo__see"]);
  });

  it("ranks a tool matching more query terms first, whatever the scores", () => {
    const peekaboo = [
      doc(
        "mcp__peekaboo__app",
        "Launch, quit, focus, or switch applications by name or bundle identifier.",
      ),
      doc("mcp__peekaboo__see", "Capture the app window and describe the UI elements of the app."),
      doc("mcp__peekaboo__window", "Move, resize, or close a window of an app."),
      doc("mcp__peekaboo__image", "Take a screenshot of the screen, an app, or a window."),
    ];
    // `window` and `app` are whole names, but each covers one of the two terms.
    assert.deepStrictEqual(rankDocuments("screenshot window", peekaboo, 1), [
      "mcp__peekaboo__image",
    ]);
    assert.deepStrictEqual(rankDocuments("describe app", peekaboo, 1), ["mcp__peekaboo__see"]);
    // An exact name still ranks first.
    assert.deepStrictEqual(rankDocuments("window", peekaboo, 1), ["mcp__peekaboo__window"]);
  });
});

/**
 * The `tool_search` tool and its extension: the model searches the deferred tools it is not offered yet (`Agent.deferred`), and
 * the matches are loaded with `control.addTools`, so the next request offers them. Loading writes the
 * conversation's `modelTools` (its `add`), so loaded tools stay offered on that branch, also after a reset.
 *
 * It is opt-in: give `ToolSearch.extension` to `Harness.layer` and select it where the model
 * should find deferred tools.
 *
 * @since 0.0.0
 */
import * as Effect from "effect/Effect";
import * as Predicate from "effect/Predicate";
import * as Schema from "effect/Schema";
import * as AgentTool from "./AgentTool.ts";
import * as Extension from "./Extension.ts";
import { toolDeclaration } from "./internal/prompt.ts";
import { rankDocuments } from "./internal/search.ts";

/**
 * The tool's name.
 *
 * @category constants
 * @since 0.0.0
 */
export const TOOL_NAME = "tool_search";

/**
 * The most tools one search loads when the call gives no `limit`.
 *
 * @category constants
 * @since 0.0.0
 */
export const DEFAULT_LIMIT = 8;

/** Fixed, so it does not change while deferred tools are registered, for example as MCP servers connect. */
const DESCRIPTION = `# Tool discovery\n\nSearches over deferred tool metadata with BM25 and exposes matching tools for the next model call.\n\nSome of the tools, such as tools of MCP servers, may not have been provided to you upfront, and you should use this tool (\`${TOOL_NAME}\`) to search for the required tools. For MCP tool discovery, always use \`${TOOL_NAME}\`.`;

const Parameters = Schema.Struct({
  query: Schema.String,
  limit: Schema.optionalKey(Schema.Number),
});

// An open object, given as is: the JSON Schema has no `additionalProperties: false`, and decoding ignores extra
// keys.
const PARAMETERS_JSON_SCHEMA: Schema.JsonObject = {
  type: "object",
  properties: {
    query: { type: "string", description: "Search query for deferred tools." },
    limit: {
      type: "number",
      description: `Maximum number of tools to return. Defaults to ${DEFAULT_LIMIT}.`,
    },
  },
  required: ["query"],
};

/**
 * `tool_search` result details: the tools it loaded.
 *
 * @category models
 * @since 0.0.0
 */
export const ToolSearchDetails = Schema.Struct({ loaded: Schema.Array(Schema.String) });
/**
 * The type of `ToolSearchDetails`.
 *
 * @category models
 * @since 0.0.0
 */
export type ToolSearchDetails = typeof ToolSearchDetails.Type;

/**
 * A search the tool refuses: an empty query, or a `limit` that is not a positive integer. It becomes the call's
 * `tool_error` result.
 *
 * @category errors
 * @since 0.0.0
 */
export class ToolSearchError extends Schema.TaggedError<ToolSearchError>()("ToolSearchError", {
  message: Schema.String,
}) {}

/**
 * The `tool_search` tool: ranks the calling conversation's `Agent.deferred` against the
 * query and loads at most `limit` of them (default `DEFAULT_LIMIT`). Replay-safe: a search only reads.
 *
 * @category tools
 * @since 0.0.0
 */
// A value, not a factory, since it takes no options.
export const tool = AgentTool.make(TOOL_NAME, {
  description: DESCRIPTION,
  parameters: Parameters,
  // Searching changes what the model is offered; scripts have their own search.
  callers: ["model"],
  jsonSchema: PARAMETERS_JSON_SCHEMA,
  details: ToolSearchDetails,
  replay: "safe",
  execute: (args, call) =>
    Effect.gen(function* () {
      if (args.query.trim() === "") {
        return yield* new ToolSearchError({ message: "query must not be empty" });
      }
      const max = args.limit ?? DEFAULT_LIMIT;
      if (!Number.isInteger(max) || max <= 0) {
        return yield* new ToolSearchError({ message: "limit must be a positive integer" });
      }
      const candidates = (yield* call.agent).deferred;
      const documents = candidates.map((candidate) => ({
        name: candidate.name,
        text: searchText(candidate),
      }));
      const loaded = rankDocuments(args.query, documents, max);
      const text =
        loaded.length === 0
          ? "No matching tools found."
          : `Loaded ${loaded.length} tool${loaded.length === 1 ? "" : "s"}. They are available from your next call:\n${loaded
              .map((name) => {
                const description =
                  candidates.find((candidate) => candidate.name === name)?.definition.description ??
                  "";
                return `- ${name}: ${description.trim().split(/\r?\n/)[0]}`;
              })
              .join("\n")}`;
      const result: AgentTool.ToolExecutionResult<ToolSearchDetails> = {
        output: [{ type: "text", text }],
        details: { loaded },
      };
      return loaded.length === 0 ? result : { ...result, control: { addTools: loaded } };
    }),
});

/**
 * The `tool_search` extension, named `tool-search`: install it and select it where
 * the model should find deferred tools.
 *
 * @category extensions
 * @since 0.0.0
 */
export const extension = Extension.make("tool-search", { tools: [tool] });

/**
 * Search text of a tool: its description and its parameters' names and descriptions, from the JSON Schema it is
 * offered with; the ranker reads the name.
 */
const searchText = (candidate: AgentTool.Any): string => {
  const parts = [candidate.definition.description];
  const parameters = toolDeclaration(candidate).parameters;
  const definitions = parameters["$defs"];
  schemaText(parameters, parts, isJsonObject(definitions) ? definitions : {}, new Set());
  return parts.filter((part) => part.trim() !== "").join(" ");
};

/**
 * Schema descriptions and property names, recursively.
 *
 * A `$ref` to `#/$defs/<name>` is followed into the root's `$defs`. Effect's generator moves a schema with an
 * `identifier`, and any `Schema.Class`, into `$defs` and leaves a `$ref` in its place; without following it, nested names and descriptions would not be searchable. `visiting`
 * holds the definitions on the current path, so a recursive schema is read once per path.
 */
const schemaText = (
  schema: Schema.Json | undefined,
  parts: Array<string>,
  definitions: Schema.JsonObject,
  visiting: Set<string>,
): void => {
  if (!isJsonObject(schema)) return;
  const ref = schema["$ref"];
  if (Predicate.isString(ref) && ref.startsWith(DEFS_PREFIX)) {
    const name = ref.slice(DEFS_PREFIX.length).replaceAll("~1", "/").replaceAll("~0", "~");
    if (!visiting.has(name) && Object.hasOwn(definitions, name)) {
      visiting.add(name);
      schemaText(definitions[name], parts, definitions, visiting);
      visiting.delete(name);
    }
  }
  if (Predicate.isString(schema["description"])) parts.push(schema["description"]);
  const properties = schema["properties"];
  if (isJsonObject(properties)) {
    for (const [name, property] of Object.entries(properties)) {
      parts.push(name);
      schemaText(property, parts, definitions, visiting);
    }
  }
  schemaText(schema["items"], parts, definitions, visiting);
  for (const key of ["anyOf", "oneOf", "allOf"]) {
    const variants = schema[key];
    if (Array.isArray(variants)) {
      for (const variant of variants) schemaText(variant, parts, definitions, visiting);
    }
  }
};

const DEFS_PREFIX = "#/$defs/";

const isJsonObject = (value: Schema.Json | undefined): value is Schema.JsonObject =>
  Predicate.isObject(value) && !Array.isArray(value);

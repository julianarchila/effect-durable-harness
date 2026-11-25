/**
 * Transcript helpers: the replay of tool changes in system messages and the
 * comparison of tool declarations. `prompt.ts` plans `core.system` entries with
 * them and the generation checks which tools a request offered. Rendering
 * provider requests stays behind `Models`.
 */
import type { Tool } from "../Message.ts";

/** `getCurrentTools`, public in `Message.ts`; re-exported for the kernel modules that import it from here. */
export { getCurrentTools } from "../Message.ts";

/**
 * Strip executable and display-only fields from a tool before transcript
 * comparison or persistence.
 *
 * There is no JSON round-trip of `parameters`: `Tool.parameters` is already a
 * JSON object, and `Tool` has no `constrainedSampling` (`Message.ts`). The key
 * order is fixed, so `declarationsEqual` compares declarations exactly.
 */
export const toToolDeclaration = (tool: Tool): Tool => ({
  name: tool.name,
  description: tool.description,
  parameters: tool.parameters,
});

/**
 * Whether two tools declare the same interface to the model. Both sides go through `toToolDeclaration` first, so
 * extra fields of a registration do not count; comparing the serialized
 * declarations is exact, and sensitive to key order inside `parameters`.
 */
export const declarationsEqual = (left: Tool, right: Tool): boolean =>
  JSON.stringify(toToolDeclaration(left)) === JSON.stringify(toToolDeclaration(right));

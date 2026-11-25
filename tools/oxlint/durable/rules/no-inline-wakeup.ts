import { defineRule } from "@oxlint/plugins";

import type { ESTree } from "@oxlint/plugins";

// Every export of `effect/Deferred` that completes a deferred: each resumes its waiters inline, on the
// caller's stack (`effect/Deferred.ts:857-871`).
const COMPLETIONS = new Set([
  "complete",
  "completeWith",
  "die",
  "dieSync",
  "done",
  "doneUnsafe",
  "fail",
  "failCause",
  "failCauseSync",
  "failSync",
  "interrupt",
  "interruptWith",
  "into",
  "succeed",
  "sync",
]);

/**
 * In the kernel, a waiter must resume after a yield, not on the stack of whoever wakes it, so
 * the kernel's observable order does not depend on who woke whom. Completing a `Deferred` or opening a `Latch` with `openUnsafe` resumes
 * waiters inline, so wakeups go through `internal/signal.ts`. A `done` that only marks the end of an
 * invocation or of close stays inline, with a disable comment that says so.
 */
export const noInlineWakeupRule = defineRule({
  meta: {
    type: "problem",
    docs: {
      description:
        "Disallow completing a Deferred or opening a Latch inline; wake through internal/signal.ts.",
    },
    messages: {
      deferred:
        "`Deferred.{{name}}` resumes its waiters inline, on this stack. Use `doneAfterYield` or a `Signal` from `internal/signal.ts`; if this only marks the end of an invocation or of close, disable the rule here and say so.",
      latch:
        "`openUnsafe` resumes the latch's waiters inline, on this stack. Use `internal/signal.ts`, or `open`, which schedules them; if this only marks an end its joiners wait for, disable the rule here and say so.",
    },
  },
  create(context) {
    const namespaces = new Set<string>();
    const named = new Map<string, string>();

    return {
      ImportDeclaration(node) {
        if (node.source.value !== "effect/Deferred" || node.importKind === "type") return;
        for (const specifier of node.specifiers) {
          if (specifier.type === "ImportNamespaceSpecifier") namespaces.add(specifier.local.name);
          if (specifier.type !== "ImportSpecifier" || specifier.importKind === "type") continue;
          const imported =
            specifier.imported.type === "Identifier"
              ? specifier.imported.name
              : specifier.imported.value;
          if (COMPLETIONS.has(imported)) named.set(specifier.local.name, imported);
        }
      },
      CallExpression(node) {
        const callee = node.callee;
        if (callee.type === "Identifier") {
          const imported = named.get(callee.name);
          if (imported !== undefined) {
            context.report({ node, messageId: "deferred", data: { name: imported } });
          }
          return;
        }
        if (callee.type !== "MemberExpression" || callee.computed) return;
        const property = memberName(callee);
        if (property === undefined) return;
        if (property === "openUnsafe") {
          context.report({ node, messageId: "latch" });
          return;
        }
        if (
          callee.object.type === "Identifier" &&
          namespaces.has(callee.object.name) &&
          COMPLETIONS.has(property)
        ) {
          context.report({ node, messageId: "deferred", data: { name: property } });
        }
      },
    };
  },
});

function memberName(member: ESTree.MemberExpression): string | undefined {
  return member.property.type === "Identifier" ? member.property.name : undefined;
}

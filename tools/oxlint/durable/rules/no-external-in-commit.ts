import { defineRule } from "@oxlint/plugins";

import type { ESTree, Scope } from "@oxlint/plugins";

type Node = ESTree.Node;

// What counts as a commit callback (the change): the first argument of a call to `commit` or
// `commitWith`, as a method or a bare name, with no more arguments than the kernel's: `commit(change,
// options?)`, `commitWith(change, scope?, options?)`. A `commit` of another shape (a delta tracker's
// `changes.commit(draft, message, taken)`) is not one. That covers `Session.commit`, `Harness.commit`,
// `Conversation.commit`, a tool call's `call.commit`, a task runtime's `run.commit` (an Effect or a
// function of the reread record) and the kernel's `commitWith((tx) => ...)`. A task runtime's transitions
// take the change last, after their own arguments (`Task.ts` `Transition`), so they match only as a method
// on a value of this file (not an import such as `Task.next`) and with exactly that many arguments.
const COMMITS = new Map([
  ["commit", 2],
  ["commitWith", 3],
]);
const TRANSITIONS = new Map([
  ["next", 1],
  ["complete", 1],
  ["fail", 1],
  ["waitFor", 2],
  ["sleepUntil", 2],
]);

// What `Effect<A, E, Tx>` cannot see, from `effect/Effect`. Each leaves `R` untouched, so the type lets it
// into a commit, where it would hold the mutation line on something outside the Session.
const EFFECT_EXTERNAL = new Set([
  "promise",
  "tryPromise",
  "callback",
  "sleep",
  "delay",
  "timeout",
  "timeoutOption",
  "timeoutOrElse",
  "retry",
  "retryOrElse",
  "repeat",
  "repeatOrElse",
  "schedule",
  "scheduleFrom",
  // `forkChild` is not here: the child inherits `Tx` and ends with the commit's fiber, so it stays inside
  // the commit (the kernel's tests fork inside a change on purpose). `forkScoped` needs a `Scope`, which
  // the type already refuses. `forkDetach` and `forkIn` outlive the commit.
  "forkDetach",
  "forkIn",
  "runCallback",
  "runCallbackWith",
  "runFork",
  "runForkWith",
  "runPromise",
  "runPromiseExit",
  "runPromiseExitWith",
  "runPromiseWith",
  "runSync",
  "runSyncExit",
  "runSyncExitWith",
  "runSyncWith",
]);
// `Effect.never` is a value, not a function: it matches as a reference, called or not.
const EFFECT_EXTERNAL_VALUES = new Set(["never"]);
const GLOBAL_EXTERNAL = new Set([
  "fetch",
  "setTimeout",
  "setInterval",
  "setImmediate",
  "queueMicrotask",
]);

// The services a change may use: the transaction, and the clock, which a change may read
// (`clock.currentTimeMillis`); a clock's `sleep` is a wait and matches on its own. The kernel's document,
// entry and task operations (`Doc.get`, `Entry.append`, `Task.create`, `tx.*`) are built on `Tx` and are
// plain calls, so they need no entry here.
const ALLOWED_SERVICES = new Set(["Tx", "Clock"]);

type ModuleKind = "Effect" | "HttpClient";

function moduleKind(source: string): ModuleKind | undefined {
  if (source === "effect/Effect") return "Effect";
  if (/(^|\/)HttpClient$/u.test(source)) return "HttpClient";
  return undefined;
}

interface Found {
  readonly node: Node;
  readonly what: string;
  readonly messageId: "external" | "nested" | "service";
  // For a captured service: where it was read. A read inside the same change is reported as a yield.
  readonly declaration?: Node;
}

/**
 * Nothing external inside a commit: a commit is atomic and holds the Session's mutation line. `commit` takes `Effect<A, E, Tx>`, so a
 * service outside the Session does not compile there; this rule flags what the type cannot see: a promise,
 * a timer or a wait, a detached fork or a nested run, `fetch` and `HttpClient`, a commit inside a commit,
 * and a call through a service read outside the change (`const models = yield* Models`) and captured by
 * it. It matches lexically: the change argument of a commit call, and, one level deep in the same file,
 * the definition of a change passed by name (`commit(write)`) or built by a local function
 * (`commit(write(entry))`). A closure or helper defined in another file, or bound with `let` or as a
 * parameter, is out of its sight.
 */
export const noExternalInCommitRule = defineRule({
  meta: {
    type: "problem",
    docs: {
      description:
        "Disallow promises, timers, waits, forks, HTTP calls, other services and nested commits inside the change passed to a commit.",
    },
    messages: {
      external:
        "`{{what}}` inside a commit: the change holds the Session's mutation line, and nothing external belongs there. Do the external work before or after the commit (the effect sandwich); if this is not external, disable the rule here and say why.",
      nested:
        "`{{what}}` inside a commit: a commit waits for the mutation line the outer change holds. Write through `Tx` in the same change, or commit after it.",
      service:
        "`{{what}}` inside a commit: only `Tx` belongs in a change. Use the service before or after the commit (the effect sandwich) and pass the change its result; if this service only reads memory, disable the rule here and say why.",
    },
  },
  create(context) {
    const modules = new Map<string, ModuleKind>();
    const named = new Map<string, string>();
    const namedValues = new Map<string, ESTree.Node>();
    const imported = new Set<string>();
    const changes: Array<Node> = [];
    const found: Array<Found> = [];

    const addResolved = (identifier: Node, name: string): void => {
      const definition = resolve(context.sourceCode.getScope(identifier), name);
      if (definition !== undefined) changes.push(definition);
    };

    const addChange = (change: Node): void => {
      changes.push(change);
      if (change.type === "Identifier") addResolved(change, change.name);
      if (change.type === "CallExpression" && change.callee.type === "Identifier") {
        addResolved(change.callee, change.callee.name);
      }
    };

    // A call through a value read with `yield* Service` in this file: `models.stream(...)`, `stream(...)`
    // after `const { stream } = yield* Models`.
    const captured = (
      node: ESTree.CallExpression,
      identifier: ESTree.IdentifierReference,
    ): void => {
      const service = serviceRead(context.sourceCode.getScope(identifier), identifier.name);
      if (service === undefined) return;
      found.push({
        node,
        what: `${identifier.name}, read from ${service.name}`,
        messageId: "service",
        declaration: service.declaration,
      });
    };

    return {
      ImportDeclaration(node) {
        if (node.importKind === "type") return;
        const kind = moduleKind(node.source.value);
        for (const specifier of node.specifiers) {
          imported.add(specifier.local.name);
          if (kind === undefined) {
            if (
              node.source.value === "effect" &&
              specifier.type === "ImportSpecifier" &&
              specifier.importKind !== "type" &&
              importedName(specifier) === "Effect"
            ) {
              modules.set(specifier.local.name, "Effect");
            }
            continue;
          }
          if (specifier.type === "ImportNamespaceSpecifier") {
            modules.set(specifier.local.name, kind);
          } else if (specifier.type === "ImportSpecifier" && specifier.importKind !== "type") {
            const name = importedName(specifier);
            if (kind === "Effect" && EFFECT_EXTERNAL.has(name)) {
              named.set(specifier.local.name, `Effect.${name}`);
            }
            if (kind === "Effect" && EFFECT_EXTERNAL_VALUES.has(name)) {
              namedValues.set(specifier.local.name, specifier.local);
            }
          }
        }
      },
      NewExpression(node) {
        if (node.callee.type === "Identifier" && node.callee.name === "Promise") {
          found.push({ node, what: "new Promise", messageId: "external" });
        }
      },
      AwaitExpression(node) {
        found.push({ node, what: "await", messageId: "external" });
      },
      YieldExpression(node) {
        const tag = node.delegate && node.argument ? serviceTag(node.argument) : undefined;
        if (tag !== undefined) found.push({ node, what: `yield* ${tag}`, messageId: "service" });
      },
      MemberExpression(node) {
        if (
          !node.computed &&
          node.object.type === "Identifier" &&
          modules.get(node.object.name) === "Effect" &&
          node.property.type === "Identifier" &&
          EFFECT_EXTERNAL_VALUES.has(node.property.name)
        ) {
          found.push({ node, what: `Effect.${node.property.name}`, messageId: "external" });
        }
      },
      Identifier(node) {
        const local = namedValues.get(node.name);
        // The import's own binding is not a use, nor a property name (`Effect.never`, `{ never: 1 }`).
        if (local === undefined || local.start === node.start) return;
        const parent = node.parent;
        if (parent.type === "MemberExpression" && parent.property === node && !parent.computed) {
          return;
        }
        if (parent.type === "Property" && parent.key === node && !parent.shorthand) return;
        if (resolvesToImport(context.sourceCode.getScope(node), node.name)) {
          found.push({ node, what: `Effect.${node.name}`, messageId: "external" });
        }
      },
      CallExpression(node) {
        const callee = node.callee;
        if (callee.type === "Identifier") {
          const change = node.arguments[0];
          if (change !== undefined && isCommit(callee.name, node)) {
            addChange(change);
            found.push({ node, what: callee.name, messageId: "nested" });
          }
          const name = named.get(callee.name);
          if (name !== undefined) found.push({ node, what: name, messageId: "external" });
          if (
            GLOBAL_EXTERNAL.has(callee.name) &&
            !declared(context.sourceCode.getScope(callee), callee.name)
          ) {
            found.push({ node, what: callee.name, messageId: "external" });
          }
          captured(node, callee);
          return;
        }
        if (callee.type !== "MemberExpression" || callee.computed) return;
        if (callee.property.type !== "Identifier") return;
        const property = callee.property.name;
        const object = callee.object;
        const namespace = object.type === "Identifier" ? modules.get(object.name) : undefined;

        const first = node.arguments[0];
        if (first !== undefined && isCommit(property, node)) {
          addChange(first);
          found.push({ node, what: `.${property}`, messageId: "nested" });
        }
        const arity = TRANSITIONS.get(property);
        const fromImport = object.type === "Identifier" && imported.has(object.name);
        if (arity !== undefined && !fromImport && node.arguments.length === arity + 1) {
          const change = node.arguments[arity];
          if (change !== undefined) addChange(change);
        }

        if (namespace === "Effect" && EFFECT_EXTERNAL.has(property)) {
          found.push({ node, what: `Effect.${property}`, messageId: "external" });
        } else if (namespace === "HttpClient") {
          found.push({ node, what: `HttpClient.${property}`, messageId: "external" });
        } else if (
          object.type === "Identifier" &&
          object.name === "globalThis" &&
          GLOBAL_EXTERNAL.has(property)
        ) {
          found.push({ node, what: property, messageId: "external" });
        } else if (property === "sleep" && namespace === undefined) {
          // `clock.sleep(...)` on a `Clock` read with `Effect.clockWith` or `yield* Clock.Clock`.
          found.push({ node, what: "sleep", messageId: "external" });
        }
        if (object.type === "Identifier") captured(node, object);
      },
      "Program:exit"() {
        for (const item of found) {
          const inside = changes.some(
            (change) =>
              contains(change, item.node) &&
              (item.declaration === undefined || !contains(change, item.declaration)),
          );
          if (inside) {
            context.report({
              node: item.node,
              messageId: item.messageId,
              data: { what: item.what },
            });
          }
        }
      },
    };
  },
});

function isCommit(name: string, call: ESTree.CallExpression): boolean {
  const most = COMMITS.get(name);
  return most !== undefined && call.arguments.length <= most;
}

function importedName(specifier: ESTree.ImportSpecifier): string {
  return specifier.imported.type === "Identifier"
    ? specifier.imported.name
    : specifier.imported.value;
}

// A service tag is a PascalCase name (`Models`, `Session`); a value is camelCase.
function isServiceName(name: string): boolean {
  return /^[A-Z][A-Za-z0-9]*$/u.test(name) && name !== name.toUpperCase();
}

// The name of the service tag `yield*` reads, when it is one and not allowed: a PascalCase identifier
// (`Models`), or a PascalCase property of a module (`HttpClient.HttpClient`, `SqlClient.SqlClient`).
function serviceTag(node: Node): string | undefined {
  if (node.type === "Identifier") {
    return isServiceName(node.name) && !ALLOWED_SERVICES.has(node.name) ? node.name : undefined;
  }
  if (
    node.type !== "MemberExpression" ||
    node.computed ||
    node.object.type !== "Identifier" ||
    node.property.type !== "Identifier"
  ) {
    return undefined;
  }
  const property = node.property.name;
  if (!isServiceName(property) || ALLOWED_SERVICES.has(property)) return undefined;
  return `${node.object.name}.${property}`;
}

// Whether `name` here is the module-level import, not a local that shadows it.
function resolvesToImport(scope: Scope, name: string): boolean {
  for (let current: Scope | null = scope; current !== null; current = current.upper) {
    const variable = current.set.get(name);
    if (variable === undefined) continue;
    return variable.defs[0]?.type === "ImportBinding";
  }
  return false;
}

function contains(outer: Node, inner: Node): boolean {
  return outer.start <= inner.start && inner.end <= outer.end;
}

// Whether this file declares `name` in `scope` or above: a global, configured or not, has no definition.
function declared(scope: Scope, name: string): boolean {
  for (let current: Scope | null = scope; current !== null; current = current.upper) {
    const variable = current.set.get(name);
    if (variable !== undefined && variable.defs.length > 0) return true;
  }
  return false;
}

function declaratorOf(scope: Scope, name: string): ESTree.VariableDeclarator | undefined {
  for (let current: Scope | null = scope; current !== null; current = current.upper) {
    const variable = current.set.get(name);
    if (variable === undefined) continue;
    const definition = variable.defs[0];
    if (definition === undefined || definition.type !== "Variable") return undefined;
    const declarator = definition.node;
    return declarator.type === "VariableDeclarator" ? declarator : undefined;
  }
  return undefined;
}

interface ServiceRead {
  readonly name: string;
  readonly declaration: Node;
}

// The service a name was read from, when the same file binds it with `yield* Service` or
// `yield* Module.Service` (or destructures it), the service not being allowed.
function serviceRead(scope: Scope, name: string): ServiceRead | undefined {
  const declarator = declaratorOf(scope, name);
  const init = declarator?.init;
  if (declarator === undefined || init === null || init === undefined) return undefined;
  if (init.type !== "YieldExpression" || !init.delegate) return undefined;
  const tag = init.argument ? serviceTag(init.argument) : undefined;
  return tag === undefined ? undefined : { name: tag, declaration: declarator };
}

// The definition a name in a change points to, when the same file defines it with `const` or as a
// function: the initializer, or the function itself. Parameters, imports and `let` are not followed.
function resolve(scope: Scope, name: string): Node | undefined {
  for (let current: Scope | null = scope; current !== null; current = current.upper) {
    const variable = current.set.get(name);
    if (variable === undefined) continue;
    const definition = variable.defs[0];
    if (definition === undefined) return undefined;
    if (definition.type === "FunctionName") return definition.node;
    if (definition.type !== "Variable") return undefined;
    const declarator = definition.node;
    if (declarator.type !== "VariableDeclarator") return undefined;
    const declaration = definition.parent;
    if (declaration === null || declaration.type !== "VariableDeclaration") return undefined;
    if (declaration.kind !== "const") return undefined;
    return declarator.init ?? undefined;
  }
  return undefined;
}

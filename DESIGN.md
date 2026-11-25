# Design decisions

Decisions about the kernel's semantics that a user of the library would notice, each with its reason.

## Kernel

- **Static registry.** Extensions are fixed when `Harness.layer` is built, so their requirements are part of the layer's type. A deploy replaces the code: there is no hot reload and no task handover.
- **Schemas type everything stored.** A document value, a task migration's input or checkpoint, and an entry's `model` (which must be model messages) are checked against their schemas; a value they reject fails the read or the commit, or blocks the task `migration_failed`. The reason: handlers receive decoded values, so a value with no decoded form has nothing to run with.
- **A safe replay decodes its stored arguments again.** Recovery after a tool call's intent, and its run after a park, decode the stored arguments with the current `parameters`. A deploy that narrows the schema ends the call `invalid_arguments`, and a `cancel` whose arguments no longer decode does not run.
- **Closing cannot fail.** Closing a Session is the end of its layer's scope, whose finalizers cannot fail. When the final store of held deferred commits fails, close succeeds and the failure is in `closed` (`failed`).
- **Interruption is the abort.** An abort reaches a running tool as its interruption, so a tool cannot ignore it and park anyway. Close interrupts invocations in the step that seals the Session; no handler code starts after it.
- **The Session doesn't own its storage.** The layer that provides `AgentStorage` closes it. A storage failure reaches the caller as `SessionFailed`, with the backend's error as cause.

## Outside the kernel

The execution environment, code mode and sandboxes are packages built on the kernel's guarantees: the environment resolved per call, replay policy, a stable `callId` and keyed nested calls.

## Host

The host runs a Session in a Durable Object instead of a long-lived process.

- Wakeups use the object's alarm, armed before the commit that leaves a live task. While there is active work, a 30 s keep-alive alarm runs and the instance is held in memory (`Engine.keepAwake`), and it is billed for that time. Long waits belong in parked tools (`AgentTool.wait`, `AgentTool.sleep`) or `sleepUntil`, which hold no invocation.
- A fallback alarm is armed before the Harness opens, so an instance whose start fails starts again. The object aborts itself when the Harness closes with a storage failure.
- Create and fork commands keep a receipt per request ID, in the same commit.

## Open questions

- **Document visibility across extensions.** Today a document token is the capability. Reopen when an app needs one extension's documents kept from another.
- **Mandatory extensions.** `configure` can remove any extension, policies included. Reopen when an app needs a policy that neither its code nor a subagent can remove.

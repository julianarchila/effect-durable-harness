// Open and close, the built-in creation hook, the conversation
// conveniences and handles, and the public `Harness` over the Session kernel, the scheduler, the submissions
// and the view mounts.
import * as Cause from "effect/Cause";
import * as Context from "effect/Context";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as Tracer from "effect/Tracer";
import { type AgentChange, AgentDoc, INSTRUCTIONS_KEY } from "../Agent.ts";
import * as AgentTool from "../AgentTool.ts";
import type { AgentStorage, Cursor, StorageFailure } from "../AgentStorage.ts";
import { ResetEntry } from "../Entry.ts";
import { type ExecutionEnv, ExecutionEnvError, type ExecutionEnvTarget } from "../ExecutionEnv.ts";
import type * as Extension from "../Extension.ts";
import type {
  Conversation,
  CreateConversationOptions,
  Harness,
  HarnessInspection,
  HarnessOptions,
  RootOptions,
} from "../Harness.ts";
import { type ConversationId, type EntryId, ROOT_CONVERSATION_ID, type TaskId } from "../Ids.ts";
import { InboxDoc } from "../Inbox.ts";
import { LiveDoc } from "../Live.ts";
import type { Models } from "../Models.ts";
import { ProviderDoc } from "../ProviderSession.ts";
import type { Report } from "../Report.ts";
import type { ConversationRecord } from "../Records.ts";
import { Session } from "../Session.ts";
import { ReadOnlySession, SessionClosed, type SessionFailed } from "../SessionError.ts";
import { SessionWakeups } from "../SessionWakeups.ts";
import * as Settings from "../Settings.ts";
import * as Task from "../Task.ts";
import { Tx, type TxDocError } from "../Tx.ts";
import { UsageDoc, type UsageState } from "../Usage.ts";
import { configure, createAgent, resolveAgent } from "./agent.ts";
import { erased } from "./decisions.ts";
import { readContextFrom } from "./contextRead.ts";
import { CompactionTask, createCompaction } from "./compaction.ts";
import { GenerationTask } from "./generation.ts";
import { type InstalledTask, InstalledTasks } from "./installed.ts";
import { withdrawQueuedInputs } from "./inbox.ts";
import { LIMITS_RESTARTS, settleSchedulerOutcome } from "./live.ts";
import * as scheduler from "./scheduler.ts";
import { scanAll } from "./scan.ts";
import * as session from "./session.ts";
import * as Submissions from "./submissions.ts";
import { gatedCall, keepsChildren, ToolTask, toolErrorMessage } from "./tool.ts";
import type { Reporter } from "./report.ts";
import * as telemetry from "./telemetry.ts";
import { addUsageState } from "./usage.ts";
import { uuidv7 } from "./uuid.ts";
import * as TaskGraphs from "./taskGraph.ts";
import * as Views from "./view.ts";

const SCAN_PAGE_SIZE = 256;
/** Section keys. */
const SECTION_KEY = /^[a-z][a-z0-9_-]*$/;

/** The Harness and what tests reach behind it: the Session kernel and the scheduler. */
export interface HarnessKernel {
  readonly harness: Harness["Service"];
  readonly session: Session["Service"];
  readonly kernel: session.SessionKernel;
  readonly scheduler: scheduler.Scheduler;
  readonly views: Views.Views;
}

/** Opens a Harness over the `AgentStorage` in context, in the current scope. */
export const make = <const X extends ReadonlyArray<Extension.Any>, R = never, E = never>(
  options: HarnessOptions<X, R, E>,
): Effect.Effect<
  HarnessKernel,
  StorageFailure | SessionFailed,
  AgentStorage | Models | Scope.Scope | Extension.Requirements<X> | R
> =>
  Effect.gen(function* () {
    const installed: ReadonlyArray<Extension.Any> = options.extensions;
    const tasks = registryOf(installed);
    // The handlers' services, without the scope this Harness is built in: each phase runs in a scope of its
    // own, so a resource a phase acquires is released when the phase ends, not when the Harness does.
    const services = Context.omit(Scope.Scope)(
      yield* Effect.context<Extension.Requirements<X> | Models>(),
    );
    // As `HttpApiBuilder`: a declared tool or task without a handler in context (only possible past
    // the types, with a cast) fails the build at once, naming it, instead of the first call that needs it.
    const missing = missingHandler(installed, services);
    if (missing !== undefined) return yield* Effect.die(new Error(missing));
    // The environment builder's services, the Harness's scope included; spans come from the call that builds it.
    // SAFETY: the builder's requirements are `R`, and the parent span is always the caller's.
    const envServices = Context.omit(Tracer.ParentSpan)(
      yield* Effect.context<R>(),
    ) as Context.Context<R>;
    // Resolved once, when the Harness is built.
    const configured = options.defaults?.agent;
    const defaultAgent: AgentChange | undefined = Effect.isEffect(configured)
      ? yield* configured
      : configured;
    const wakeups = yield* SessionWakeups;
    const harnessScope = yield* Effect.scope;
    // Fibers of the Harness live in a scope of their own, closed by the Harness's finalizer after the
    // invocations joined, so no scheduler fiber outlives close and none is interrupted before the seal. It
    // is a child of the Harness's scope from the start, so a build interrupted before that finalizer is
    // registered still closes it.
    const fibers = yield* Scope.fork(harnessScope);

    // Reports run in order on a fiber of their own; one that fails is dropped.
    const reports = yield* Queue.unbounded<Report>();
    const onReport =
      options.onReport ??
      ((delivered: Report) =>
        Effect.logWarning(`durable report: ${delivered.kind}`, delivered.cause).pipe(
          Effect.annotateLogs(telemetry.reportAttributes(delivered)),
        ));
    // Suspended, so an `onReport` that throws while building its Effect is dropped too.
    const deliver = (delivered: Report) =>
      Effect.suspend(() => onReport(delivered)).pipe(Effect.catchCause(() => Effect.void));
    // Delivered to `onReport`, then counted and recorded on the reporting fiber's span.
    const report: Reporter = (cause, info, on) => {
      Queue.offerUnsafe(reports, { ...info, cause });
      telemetry.recordReport(info, on);
    };
    const readOnly = options.readOnly === true;
    const extensionOf = extensionsOf(installed);
    // A delivery runs uninterruptibly, so a close that stops this fiber waits for the report in flight
    // instead of losing it; an `onReport` that never returns therefore holds close, as a handler does.
    yield* Effect.forkIn(
      Effect.forever(
        Queue.take(reports).pipe(Effect.flatMap((cause) => Effect.uninterruptible(deliver(cause)))),
      ),
      fibers,
    );

    /** The resolved settings: read anew at every use; a reader that throws throws. */
    const settings = () => Settings.resolve(options.settings?.());

    const created = options.conversationCreated;
    let tasksScheduler: scheduler.Scheduler | undefined;
    const kernel = yield* session.make({
      // Read at each use, as all settings are; the kernel falls back to the default
      // when the reader throws.
      documentRetention: () => Duration.millis(settings().documentRetentionMs),
      // The default window of a deferred commit, read at each one.
      persistWithin: () => Duration.millis(settings().progress.persistWithinMs),
      // The built-in creation hook, then the host's.
      conversationCreated: (_tx, record) =>
        Effect.gen(function* () {
          yield* createBuiltins(record);
          // Every top-level conversation gets the default agent in its creation commit, whoever creates
          // it (the Harness, a host's `Tx.createConversation`); a fork keeps its parent's, which the built-in
          // hook copied, and an owned conversation is configured by its owner.
          if (
            defaultAgent !== undefined &&
            record.parent === undefined &&
            record.owner === undefined
          ) {
            yield* configure(record.id, defaultAgent);
          }
          if (created !== undefined) yield* created(record);
        }),
      // An Effect handler can be preempted between its dispatch and its first operation, so a host close
      // signals the invocations in the same step as the seal: no handler code starts after it.
      afterSeal: () => tasksScheduler?.signalAll(),
      // Join the invocations before the line drains.
      beforeClose: Effect.suspend(() => tasksScheduler?.join ?? Effect.void),
      // The ownership memberships a commit's changes write, from the scheduler.
      admitting: (tx) =>
        Effect.suspend(() =>
          tasksScheduler === undefined ? Effect.succeed([]) : tasksScheduler.admitting(tx),
        ),
      report,
      readOnly,
    });

    /** The committed `core.agent` against the extensions and settings. */
    const agentOf = (conversationId: ConversationId) =>
      kernel.snapshot(AgentDoc, conversationId).pipe(
        // A settings reader that throws fails the resolution with that defect.
        Effect.flatMap((state) =>
          Effect.sync(() =>
            resolveAgent(Option.getOrUndefined(state), installed, settings(), (cause, extension) =>
              // A wrapper's failure names its extension and the conversation.
              report(cause, { kind: "extension_error", conversationId, extension }),
            ),
          ),
        ),
        // `core.agent` is built in: an inaccessible one is broken setup.
        Effect.catchTag("DocumentAccessError", (error) => Effect.die(error)),
      );

    /**
     * The host's environment for a conversation, from its committed `cwd`, under the services the layer was
     * built with. Any failure of the builder, a defect too, is its `ExecutionEnvError`.
     */
    const envOf = (
      target: Omit<ExecutionEnvTarget, "cwd">,
    ): Effect.Effect<ExecutionEnv | undefined, ExecutionEnvError> => {
      const build = options.env;
      if (build === undefined) return Effect.succeed(undefined);
      return Effect.gen(function* () {
        const state = yield* kernel.snapshot(AgentDoc, target.conversationId);
        const cwd = Option.isSome(state) ? state.value.cwd : undefined;
        return yield* build(cwd === undefined ? target : { ...target, cwd });
      }).pipe(
        Effect.provideContext(envServices),
        Effect.catchCause((cause) =>
          Cause.hasInterrupts(cause)
            ? Effect.interrupt
            : Effect.fail(
                new ExecutionEnvError({
                  message: toolErrorMessage(cause),
                  cause: Cause.squash(cause),
                }),
              ),
        ),
      );
    };

    let submissions: Submissions.Submissions | undefined;
    const submissionsOf = () => {
      if (submissions === undefined) throw new Error("The Harness's submissions are not open yet");
      return submissions;
    };

    const sessionService = Session.of(session.serviceOf(kernel));

    const opened = yield* scheduler.make({
      kernel,
      tasks,
      // SAFETY: the context holds every service the handlers of these extensions and the built-in tasks
      // require. The `Session` too, so every handler reaches its session per call.
      services: Context.add(
        Context.add(services, Session, sessionService),
        InstalledTasks,
        installedTasks(tasks, extensionOf, services),
      ) as Context.Context<never>,
      wakeups,
      limitsRestarts: (kind) => LIMITS_RESTARTS.has(kind),
      keepsAbandoned: keepsChildren,
      gated: gatedCall,
      report,
      extensionOf: (kind) => extensionOf.get(kind),
      readOnly,
      settleOutcome: (_tx, record, outcome) => builtIn(settleSchedulerOutcome(record, outcome)),
      withdrawInputs: (_tx, conversationId) => builtIn(withdrawQueuedInputs(conversationId)),
      scope: fibers,
      agentOf,
      settings,
      envOf,
      submit: (conversationId, draft, refuse) =>
        Effect.suspend(() => submissionsOf().submit(conversationId, draft, refuse)).pipe(
          Effect.catchTag("DocumentAccessError", (error) => Effect.die(error)),
          // A task's handle submits only while the task runs, which a read-only Harness never does.
          Effect.catchTag("ReadOnlySession", (error) => Effect.die(error)),
        ),
      admit: (conversationId, draft, refuse) =>
        Effect.suspend(() => submissionsOf().admit(conversationId, draft, refuse)),
    });
    tasksScheduler = opened;

    // Registered after the kernel's own finalizer, so it runs first: close seals, joins the invocations
    // and drains the line before the scheduler's fibers stop and pending reports are delivered.
    yield* Scope.addFinalizer(
      harnessScope,
      kernel.close.pipe(
        Effect.andThen(Scope.close(fibers, Exit.void)),
        Effect.andThen(Queue.clear(reports)),
        Effect.flatMap((pending) => Effect.forEach(pending, deliver, { discard: true })),
      ),
    );

    // An open that fails closes the Session and rethrows the open error.
    yield* opened.open.pipe(
      Effect.catchTag("SessionClosed", (error) => Effect.die(error)),
      Effect.catchTag("StorageRequestError", (error) => Effect.die(error)),
    );

    // The scheduler's commit observer first, then the submissions', then the views'.
    const openSubmissions = yield* Submissions.make({
      kernel,
      queueModes: settings,
      resume: () => opened.resume(),
      readOnly,
    }).pipe(Effect.catchTag("SessionClosed", (error) => Effect.die(error)));
    submissions = openSubmissions;
    // The task graph's listeners after the submissions', before the views'.
    const taskGraph = yield* TaskGraphs.make({
      kernel,
      blocking: { blocked: opened.blockedReason, subscribeBlocked: opened.subscribeBlocked },
      report,
      scope: fibers,
    }).pipe(Effect.catchTag("SessionClosed", (error) => Effect.die(error)));
    const views = yield* Views.make({ kernel, report }).pipe(
      Effect.catchTag("SessionClosed", (error) => Effect.die(error)),
    );

    const readOnLine = kernel.readOnLine;

    /**
     * On a read-only Harness every call that would run tasks fails with `ReadOnlySession("running tasks")`
     * before it does anything.
     */
    const runningTasks = <A, E>(
      effect: Effect.Effect<A, E>,
    ): Effect.Effect<A, E | ReadOnlySession> =>
      readOnly ? Effect.fail(new ReadOnlySession({ what: "running tasks" })) : effect;

    /** A root, independent or forked conversation with `agent` and `init`. */
    const create = (
      target:
        | { readonly kind: "root" }
        | {
            readonly kind: "independent";
            readonly ownership: CreateConversationOptions["ownership"];
          }
        | {
            readonly kind: "fork";
            readonly parent: ConversationId;
            readonly at: EntryId;
            readonly ownership: CreateConversationOptions["ownership"];
          },
      createOptions: RootOptions,
    ) =>
      kernel
        .commitWith((tx) =>
          Effect.gen(function* () {
            if (
              target.kind === "root" &&
              Option.isSome(yield* tx.service.conversation(ROOT_CONVERSATION_ID))
            ) {
              return ROOT_CONVERSATION_ID;
            }
            const record =
              target.kind === "root"
                ? yield* tx.createRootConversation
                : target.kind === "fork"
                  ? yield* tx.service.forkConversation(target.parent, target.at, {
                      ownership: target.ownership,
                    })
                  : yield* tx.service.createConversation({ ownership: target.ownership });
            // The default agent was applied in the creation hook; an explicit one is applied over it.
            if (createOptions.agent !== undefined) yield* configure(record.id, createOptions.agent);
            if (createOptions.init !== undefined) yield* createOptions.init(record.id);
            return record.id;
          }),
        )
        .pipe(
          // Its only table read comes first and its only query is valid.
          Effect.catchTag("ReadAfterWrite", (error) => Effect.die(error)),
          Effect.catchTag("StorageRequestError", (error) => Effect.die(error)),
          Effect.map(conversation),
        );

    const conversation = (id: ConversationId): Conversation => ({
      id,
      commit: (change, options) => kernel.commitWith(() => change, { conversationId: id }, options),
      submit: (draft) =>
        submissionsOf()
          .submit(id, draft)
          .pipe(
            // `core.inbox` and `core.live` are built in: an inaccessible one is broken setup.
            Effect.catchTag("DocumentAccessError", (error) => Effect.die(error)),
            // Only an invocation-bound submit refuses.
            Effect.catchTag("TaskError", (error) => Effect.die(error)),
          ),
      admit: (draft) =>
        submissionsOf()
          .admit(id, draft)
          .pipe(
            // Only an invocation-bound admission refuses.
            Effect.catchTag("TaskError", (error) => Effect.die(error)),
          ),
      compact: (instructions) =>
        runningTasks(
          Effect.suspend(() => {
            // Enables scheduling, then admits the task with its status in one commit.
            opened.resume();
            const input =
              instructions === undefined
                ? ({ reason: "manual" } as const)
                : ({ reason: "manual", instructions } as const);
            return kernel.commit(builtIn(createCompaction(id, input))).pipe(
              // Its writes query nothing the caller chose.
              Effect.catchTag("StorageRequestError", (error) => Effect.die(error)),
            );
          }),
        ).pipe(
          Effect.withSpan("Conversation.compact", {
            attributes: { "durable.conversation.id": id },
          }),
        ),
      reset: (handoff) =>
        Effect.gen(function* () {
          const model =
            handoff === undefined
              ? undefined
              : [{ role: "user", content: handoff, timestamp: kernel.now() } as const];
          const entry =
            model === undefined
              ? { kind: ResetEntry.kind, head: "self" as const }
              : { kind: ResetEntry.kind, head: "self" as const, model };
          yield* submissionsOf().submit(id, { type: "write", entry });
        }).pipe(
          Effect.catchTag("DocumentAccessError", (error) => Effect.die(error)),
          Effect.catchTag("TaskError", (error) => Effect.die(error)),
          // A write is never busy, nor refused as idle.
          Effect.catchTag("ConversationBusy", (error) => Effect.die(error)),
          Effect.catchTag("ConversationIdle", (error) => Effect.die(error)),
          Effect.withSpan("Conversation.reset", { attributes: { "durable.conversation.id": id } }),
        ),
      agent: agentOf(id).pipe(
        Effect.withSpan("Conversation.agent", { attributes: { "durable.conversation.id": id } }),
      ),
      configure: (change) =>
        kernel.commit(configure(id, change)).pipe(
          Effect.withSpan("Conversation.configure", {
            attributes: { "durable.conversation.id": id },
          }),
        ),
      context: (contextOptions) =>
        readContextFrom(kernel, id, contextOptions?.at, undefined).pipe(
          Effect.map((read) => read.view),
          Effect.withSpan("Conversation.context", {
            attributes: { "durable.conversation.id": id },
          }),
        ),
      entries: (query) =>
        Stream.paginate(Option.none<Cursor>(), (cursor) =>
          readOnLine(
            // Only the bounds and the order come from the query: a stray `conversationId` never retargets the
            // handle.
            kernel.storage.scanEntries(
              {
                conversationId: id,
                minEntryId: query?.minEntryId,
                maxEntryId: query?.maxEntryId,
                order: query?.order,
              },
              SCAN_PAGE_SIZE,
              Option.getOrUndefined(cursor),
            ),
          ).pipe(Effect.map((page) => [page.items, Option.map(page.next, Option.some)] as const)),
        ).pipe(
          Stream.withSpan("Conversation.entries", {
            attributes: { "durable.conversation.id": id },
          }),
        ),
      fork: (at, createOptions) =>
        create(
          { kind: "fork", parent: id, at, ownership: createOptions.ownership },
          createOptions,
        ).pipe(
          Effect.withSpan("Conversation.fork", { attributes: { "durable.conversation.id": id } }),
        ),
      abort: (abortOptions) =>
        runningTasks(
          Effect.suspend(() => {
            opened.resume();
            return opened.abortConversation(id, abortOptions?.background === true);
          }),
        ).pipe(
          // A background abort waits for the tasks its own commit marked, which exist.
          Effect.catchTag("TaskError", (error) => Effect.die(error)),
          Effect.withSpan("Conversation.abort", { attributes: { "durable.conversation.id": id } }),
        ),
      awaitIdle: runningTasks(
        Effect.suspend(() => {
          opened.resume();
          return opened.awaitIdle(id);
        }),
      ).pipe(
        Effect.withSpan("Conversation.awaitIdle", {
          attributes: { "durable.conversation.id": id },
        }),
      ),
      observe: (observer, window) =>
        views.observe(id, observer, window).pipe(
          Effect.withSpan("Conversation.observe", {
            attributes: { "durable.conversation.id": id },
          }),
        ),
      watch: (window) =>
        views.watch(id, window).pipe(
          Effect.withSpan("Conversation.watch", {
            attributes: { "durable.conversation.id": id },
          }),
        ),
    });

    const harness: Harness["Service"] = {
      commit: kernel.commit,
      ensureRoot: (rootOptions) =>
        create({ kind: "root" }, rootOptions ?? {}).pipe(Effect.withSpan("Harness.ensureRoot")),
      conversation: (id) =>
        readOnLine(kernel.storage.conversation(id)).pipe(
          Effect.map(Option.map((record) => conversation(record.id))),
          Effect.withSpan("Harness.conversation", {
            attributes: { "durable.conversation.id": id },
          }),
        ),
      createConversation: (createOptions) =>
        create({ kind: "independent", ownership: createOptions.ownership }, createOptions).pipe(
          Effect.withSpan("Harness.createConversation"),
        ),
      signal: (taskId, signal, key, value) =>
        runningTasks(
          Effect.suspend(() => {
            // Enables scheduling, then delivers in one commit.
            opened.resume();
            return kernel.commit(
              Effect.gen(function* () {
                const tx = yield* Tx;
                return yield* tx.signal(taskId, signal, key, value);
              }),
            );
          }),
        ).pipe(
          // The commit's only table read comes first.
          Effect.catchTag("ReadAfterWrite", (error) => Effect.die(error)),
          Effect.catchTag("StorageRequestError", (error) => Effect.die(error)),
          Effect.withSpan("Harness.signal", {
            attributes: { "durable.task.id": taskId, "durable.signal.name": signal.name },
          }),
        ),
      getTask: <R>(id: TaskId<R>) =>
        readOnLine(kernel.storage.task(id)).pipe(
          // SAFETY: the record of the task `id` names; its result type is the caller's.
          Effect.map((found) => found as Option.Option<Task.TypedTaskRecord<never, never, R>>),
          Effect.withSpan("Harness.getTask", { attributes: { "durable.task.id": id } }),
        ),
      awaitTask: <R>(id: TaskId<R>) =>
        runningTasks(
          Effect.suspend(() => {
            opened.resume();
            return opened.awaitTask(id);
          }),
        ).pipe(
          // SAFETY: a terminal record of the task `id` names; its result type is the caller's.
          Effect.map((record) => record as Task.SettledTask<R>),
          Effect.withSpan("Harness.awaitTask", { attributes: { "durable.task.id": id } }),
        ),
      abortTask: (id) =>
        opened
          .abort(id)
          .pipe(Effect.withSpan("Harness.abortTask", { attributes: { "durable.task.id": id } })),
      awaitIdle: runningTasks(
        Effect.suspend(() => {
          opened.resume();
          return opened.awaitIdle(undefined);
        }),
      ).pipe(Effect.withSpan("Harness.awaitIdle")),
      inspect: readOnLine(
        Effect.gen(function* () {
          const inspected = yield* opened.inspect;
          const queued = yield* scanSubmissions(kernel, "queued");
          const placed = yield* scanSubmissions(kernel, "placed");
          const submitted = [...queued, ...placed].toSorted((a, b) => a.id - b.id);
          return { ...inspected, submissions: submitted } satisfies HarnessInspection;
        }),
      ).pipe(Effect.withSpan("Harness.inspect")),
      resume: Effect.suspend(
        (): Effect.Effect<void, SessionClosed | SessionFailed | ReadOnlySession> => {
          if (Option.isSome(kernel.failure)) return Effect.fail(kernel.failure.value);
          if (kernel.closing) return Effect.fail(new SessionClosed());
          if (readOnly) return Effect.fail(new ReadOnlySession({ what: "running tasks" }));
          return Effect.sync(opened.resume);
        },
      ).pipe(Effect.withSpan("Harness.resume")),
      wake: readOnLine(Effect.sync(opened.wake)).pipe(Effect.withSpan("Harness.wake")),
      watchTaskGraph: taskGraph.watch.pipe(Effect.withSpan("Harness.watchTaskGraph")),
      observeTaskGraph: (observer) =>
        taskGraph.observe(observer).pipe(Effect.withSpan("Harness.observeTaskGraph")),
      closed: kernel.closed,
      submission: (id) =>
        submissionsOf()
          .get(id)
          .pipe(
            Effect.withSpan("Harness.submission", { attributes: { "durable.submission.id": id } }),
          ),
      submissionByRequest: (conversationId, requestId) =>
        submissionsOf()
          .byRequest(conversationId, requestId)
          .pipe(
            Effect.withSpan("Harness.submissionByRequest", {
              attributes: { "durable.conversation.id": conversationId },
            }),
          ),
      withdrawSubmission: (id, conversationId) =>
        submissionsOf()
          .withdraw(id, conversationId)
          .pipe(
            Effect.withSpan("Harness.withdrawSubmission", {
              attributes: { "durable.submission.id": id },
            }),
          ),
      // Every conversation's committed `core.usage`, each read at its own point.
      usage: Effect.gen(function* () {
        const records = yield* readOnLine(
          scanAll((cursor) => kernel.storage.scanConversations({}, SCAN_PAGE_SIZE, cursor)),
        );
        const total: UsageTotal = { models: {}, tools: {} };
        for (const record of records) {
          const state = yield* kernel.snapshot(UsageDoc, record.id);
          if (Option.isSome(state)) addUsageState(total, state.value);
        }
        return total satisfies UsageState;
      }).pipe(
        Effect.catchTag("DocumentAccessError", (error) => Effect.die(error)),
        Effect.withSpan("Harness.usage"),
      ),
      conversations: (query) =>
        Stream.paginate(Option.none<Cursor>(), (cursor) =>
          readOnLine(
            kernel.storage.scanConversations(
              query ?? {},
              SCAN_PAGE_SIZE,
              Option.getOrUndefined(cursor),
            ),
          ).pipe(Effect.map((page) => [page.items, Option.map(page.next, Option.some)] as const)),
        ).pipe(Stream.withSpan("Harness.conversations")),
    };

    return { harness, session: sessionService, kernel, scheduler: opened, views };
  });

/** A Session usage total under construction. */
type UsageTotal = { -readonly [K in keyof UsageState]: { [key: string]: UsageState[K][string] } };

/**
 * The built-in part of every commit that creates or forks a conversation: empty
 * `core.live`, `core.inbox` and `core.usage`, a fresh `core.provider`, and the conversation's `core.agent`.
 */
const createBuiltins = (record: ConversationRecord) =>
  Effect.gen(function* () {
    const tx = yield* Tx;
    yield* tx.doc(LiveDoc, record.id);
    yield* tx.doc(InboxDoc, record.id);
    yield* tx.doc(UsageDoc, record.id);
    const provider = yield* tx.doc(ProviderDoc, record.id);
    provider.sessionId = yield* uuidv7;
    yield* createAgent(record);
  });

/** A Harness write to a built-in document: an inaccessible one, or a rejected write, is broken setup. */
const builtIn = <A, R>(effect: Effect.Effect<A, TxDocError, R>) =>
  effect.pipe(
    Effect.catchTag("DocumentAccessError", (error) => Effect.die(error)),
    Effect.catchTag("CommitRejected", (error) => Effect.die(error)),
  );

/**
 * The static registry: the built-in
 * tasks and every extension's tasks by name. Duplicate extension or task names, duplicate tools or sections
 * within an extension, an invalid section key and the reserved `instructions` key are broken setup.
 */
const registryOf = (extensions: ReadonlyArray<Extension.Any>): ReadonlyMap<string, Task.Any> => {
  const tasks = new Map<string, Task.Any>();
  const builtins: ReadonlyArray<Task.Any> = [GenerationTask, ToolTask, CompactionTask];
  for (const task of builtins) tasks.set(task.definition.name, task);
  const names = new Set<string>();
  for (const extension of extensions) {
    if (names.has(extension.name))
      throw new Error(`Extension ${extension.name} is already installed`);
    names.add(extension.name);
    const tools = new Set<string>();
    for (const tool of extension.tools) {
      if (tools.has(tool.name))
        throw new Error(`Extension ${extension.name} has two tools named ${tool.name}`);
      tools.add(tool.name);
    }
    const sections = new Set<string>();
    for (const section of extension.sections) {
      const key = section.key;
      if (!SECTION_KEY.test(key))
        throw new TypeError(`Section key ${JSON.stringify(key)} must match ${SECTION_KEY}`);
      if (key === INSTRUCTIONS_KEY)
        throw new Error(`Section key ${key} is reserved for the agent's instructions`);
      if (sections.has(key))
        throw new Error(`Extension ${extension.name} has two sections with key ${key}`);
      sections.add(key);
    }
    for (const task of extension.tasks) {
      const name = task.definition.name;
      if (tasks.has(name))
        throw new Error(`Task ${name} of extension ${extension.name} is already installed`);
      tasks.set(name, task);
    }
  }
  return tasks;
};

/**
 * The registry as the built-in handlers read it (`InstalledTasks`): each task with the phases its handlers serve,
 * from its definition or, for a task declared without them, its handler Layer.
 */
const installedTasks = (
  tasks: ReadonlyMap<string, Task.Any>,
  extensionOf: ReadonlyMap<string, string>,
  services: Context.Context<never>,
): InstalledTasks["Service"] =>
  InstalledTasks.of({
    get: (name) => {
      const task = tasks.get(name);
      if (task === undefined) return undefined;
      // SAFETY: every registered task is a token built by `Task.make` or `Task.declare`; the registry erases it.
      const token = task as InstalledTask["task"];
      const phases =
        token.definition.phases ??
        Option.getOrUndefined(Context.getOption(services, Task.handlerKey(name)))?.phases ??
        {};
      return {
        task: token,
        phases: new Set(Object.keys(phases)),
        extension: extensionOf.get(name),
      };
    },
  });

/** The extension that installed each task kind; the built-in tasks have none. */
const extensionsOf = (extensions: ReadonlyArray<Extension.Any>): ReadonlyMap<string, string> => {
  const installers = new Map<string, string>();
  for (const extension of extensions) {
    for (const task of extension.tasks) installers.set(task.definition.name, extension.name);
  }
  return installers;
};

/** The first declared tool or task of `extensions` whose handler `services` lacks, as a readable message. */
const missingHandler = (
  extensions: ReadonlyArray<Extension.Any>,
  services: Context.Context<never>,
): string | undefined => {
  for (const extension of extensions) {
    for (const tool of extension.tools) {
      // SAFETY: tools are built by `AgentTool.make`; a declared one has no `execute`.
      const own = (tool.definition as { readonly execute?: unknown }).execute;
      if (
        own === undefined &&
        Option.isNone(Context.getOption(services, AgentTool.handlerKey(tool.name)))
      ) {
        return `Extension "${extension.name}" declares tool "${tool.name}" with no handler`;
      }
    }
    for (const task of extension.tasks) {
      const definition = erased(task);
      if (
        definition.phases === undefined &&
        Option.isNone(Context.getOption(services, Task.handlerKey(definition.name)))
      ) {
        return `Extension "${extension.name}" declares task "${definition.name}" with no handler`;
      }
    }
  }
  return undefined;
};

const scanSubmissions = (kernel: session.SessionKernel, status: "queued" | "placed") =>
  scanAll((cursor) => kernel.storage.scanSubmissions({ status }, SCAN_PAGE_SIZE, cursor));

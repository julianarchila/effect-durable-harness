import assert from "node:assert/strict";
import { setImmediate } from "node:timers/promises";
// Runs as a child process under `node --expose-gc` (TrackerRetention.test.ts), which strips the types; hence the
// relative import of the source instead of the package name, which Vitest resolves but Node does not.
import * as Tracker from "../src/Tracker.ts";

const retainedSettledObjects: object[] = [];
const retainedLargePrepared: object[] = [];
const retainedSettledChanges: object[] = [];

async function collect(ref: WeakRef<object>): Promise<void> {
  assert.ok(global.gc, "worker requires --expose-gc");
  for (let attempt = 0; attempt < 30; attempt++) {
    await setImmediate();
    global.gc();
    if (ref.deref() === undefined) return;
  }
  assert.fail("released change data is still retained");
}

function abortedPayload(
  tracker: Tracker.Tracker<{ payload: { rows: { value: number }[] } | null }>,
): WeakRef<object> {
  const change = Tracker.beginChange(tracker);
  change.state.payload = { rows: Array.from({ length: 50_000 }, (_, value) => ({ value })) };
  const reference = new WeakRef(change.state.payload.rows);
  Tracker.abort(change);
  return reference;
}

function unadoptedPrepared(
  tracker: Tracker.Tracker<{ payload: { rows: { value: number }[] } | null }>,
): WeakRef<object> {
  const change = Tracker.beginChange(tracker);
  change.state.payload = { rows: Array.from({ length: 50_000 }, (_, value) => ({ value })) };
  const prepared = Tracker.prepare(change);
  return new WeakRef(prepared.value.payload!.rows);
}

function draftProxies(
  tracker: Tracker.Tracker<{ payload: { rows: { value: number }[] } | null }>,
): WeakRef<object> {
  const change = Tracker.beginChange(tracker);
  change.state.payload = { rows: [{ value: 1 }] };
  const reference = new WeakRef(change.state.payload.rows[0]!);
  Tracker.abort(change);
  return reference;
}

async function assertRetained(
  reference: WeakRef<object>,
  message = "retained Prepared must retain its immutable revisions",
): Promise<void> {
  assert.ok(global.gc, "worker requires --expose-gc");
  for (let attempt = 0; attempt < 5; attempt++) {
    await setImmediate();
    global.gc();
  }
  assert.notEqual(reference.deref(), undefined, message);
}

function settledLifecycle(): { tracker: WeakRef<object>; future: WeakRef<object> } {
  const tracker = Tracker.make<{ payload: { rows: { value: number }[] } | null }>({
    payload: null,
  });
  const change = Tracker.beginChange(tracker);
  change.state.payload = { rows: [{ value: 1 }] };
  const retainedPrepared = Tracker.prepare(change);
  Tracker.adopt(tracker, retainedPrepared);
  retainedSettledObjects.push(change, retainedPrepared);

  const futurePrepared = Tracker.prepareReplace(tracker, {
    payload: { rows: Array.from({ length: 50_000 }, (_, value) => ({ value })) },
  });
  Tracker.adopt(tracker, futurePrepared);
  return {
    tracker: new WeakRef(tracker),
    future: new WeakRef(futurePrepared.value.payload!.rows),
  };
}

const scenario = process.argv[2];
if (scenario === "settled-lifecycle") {
  const references = settledLifecycle();
  await collect(references.tracker);
  await collect(references.future);
} else if (scenario === "retained-settled-prepared") {
  const tracker = Tracker.make({
    payload: { rows: Array.from({ length: 50_000 }, (_, value) => ({ value })) },
  });
  const retain = (): { base: WeakRef<object>; value: WeakRef<object> } => {
    const change = Tracker.beginChange(tracker);
    change.state.payload.rows[0]!.value = -1;
    const prepared = Tracker.prepare(change);
    const references = {
      base: new WeakRef(prepared.base.payload.rows),
      value: new WeakRef(prepared.value.payload.rows),
    };
    Tracker.adopt(tracker, prepared);
    retainedLargePrepared.push(prepared);
    return references;
  };
  const references = retain();
  const replace = (): void => {
    const replacement = Tracker.prepareReplace(tracker, { payload: { rows: [{ value: 1 }] } });
    Tracker.adopt(tracker, replacement);
  };
  replace();
  await assertRetained(references.base);
  await assertRetained(references.value);
  retainedLargePrepared.length = 0;
  await collect(references.base);
  await collect(references.value);
} else if (scenario === "retained-settled-change") {
  const tracker = Tracker.make({
    payload: { rows: Array.from({ length: 50_000 }, (_, value) => ({ value })) },
  });
  const settleChange = (): { base: WeakRef<object>; value: WeakRef<object> } => {
    const change = Tracker.beginChange(tracker);
    change.state.payload.rows[0]!.value = -1;
    const prepared = Tracker.prepare(change);
    const references = {
      base: new WeakRef(prepared.base.payload.rows),
      value: new WeakRef(prepared.value.payload.rows),
    };
    Tracker.adopt(tracker, prepared);
    retainedSettledChanges.push(change);
    return references;
  };
  const references = settleChange();
  const replace = (): void => {
    const replacement = Tracker.prepareReplace(tracker, { payload: { rows: [{ value: 1 }] } });
    Tracker.adopt(tracker, replacement);
  };
  replace();
  await collect(references.base);
  await collect(references.value);
  assert.equal(retainedSettledChanges.length, 1);
} else if (scenario === "retained-settled-proxy") {
  const tracker = Tracker.make({
    child: { value: 0 },
    payload: { rows: Array.from({ length: 50_000 }, (_, value) => ({ value })) },
  });
  const obsolete = new WeakRef(tracker.value.payload.rows);
  const commit = (): object => {
    const change = Tracker.beginChange(tracker);
    const held = change.state.child;
    held.value = 1;
    const prepared = Tracker.prepare(change);
    Tracker.adopt(tracker, prepared);
    return held;
  };
  retainedSettledChanges.push(commit());
  const replace = (): void => {
    const replacement = Tracker.prepareReplace(tracker, {
      child: { value: 2 },
      payload: { rows: [{ value: 1 }] },
    });
    Tracker.adopt(tracker, replacement);
  };
  replace();
  await collect(obsolete);
  assert.throws(() => (retainedSettledChanges[0] as { value: number }).value, /settled/);
} else if (scenario === "retained-large-settled-proxy") {
  const tracker = Tracker.make({
    rows: Array.from({ length: 100_000 }, (_, value) => ({ value })),
  });
  const obsolete = new WeakRef(tracker.value.rows);
  const commit = (): object => {
    const change = Tracker.beginChange(tracker);
    const held = change.state.rows[0]!;
    for (let index = 1; index < 5_000; index++)
      assert.equal(change.state.rows[index]!.value, index);
    held.value = -1;
    const prepared = Tracker.prepare(change);
    Tracker.adopt(tracker, prepared);
    return held;
  };
  retainedSettledChanges.push(commit());
  const replace = (): void => {
    const replacement = Tracker.prepareReplace(tracker, { rows: [{ value: 1 }] });
    Tracker.adopt(tracker, replacement);
  };
  replace();
  await collect(obsolete);
  assert.throws(() => (retainedSettledChanges[0] as { value: number }).value, /settled/);
} else if (scenario === "retained-large-placement-proxies") {
  type Root = {
    child: { value: number };
    visited: { value: number }[];
    first: number[] | null;
    second: number[] | null;
    rows: Array<number[] | null>;
  };
  const run = (kind: "write" | "writes" | "insert" | "override"): WeakRef<object> => {
    const tracker = Tracker.make<Root>({
      child: { value: 0 },
      visited: Array.from({ length: 5_000 }, (_, value) => ({ value })),
      first: null,
      second: null,
      rows: kind === "override" ? [null] : [],
    });
    const change = Tracker.beginChange(tracker);
    for (const value of change.state.visited) assert.equal(value.value >= 0, true);
    let held: object;
    if (kind === "write" || kind === "writes") {
      held = change.state.child;
      change.state.first = Array<number>(50_000).fill(1);
      if (kind === "writes") change.state.second = Array<number>(50_000).fill(2);
    } else {
      held = change.state.rows;
      if (kind === "insert") change.state.rows.push(Array<number>(50_000).fill(1));
      else change.state.rows[0] = Array<number>(50_000).fill(1);
    }
    const prepared = Tracker.prepare(change);
    const payload =
      kind === "write" || kind === "writes" ? prepared.value.first : prepared.value.rows[0];
    assert.ok(payload);
    const reference = new WeakRef(payload);
    Tracker.adopt(tracker, prepared);
    const replacement = Tracker.prepareReplace(tracker, {
      child: { value: 1 },
      visited: [],
      first: null,
      second: null,
      rows: [],
    });
    Tracker.adopt(tracker, replacement);
    retainedSettledChanges.push(held);
    return reference;
  };
  const references = [run("write"), run("writes"), run("insert"), run("override")];
  for (const reference of references) await collect(reference);
  for (const held of retainedSettledChanges)
    assert.throws(() => Reflect.get(held, "length"), /settled/);
} else if (scenario === "stale-unprepared-change") {
  const tracker = Tracker.make({
    payload: { rows: Array.from({ length: 50_000 }, (_, value) => ({ value })) },
  });
  const stale = Tracker.beginChange(tracker);
  stale.state.payload.rows[0]!.value = -1;
  const obsolete = new WeakRef(tracker.value.payload.rows);
  const replace = (): void => {
    const winner = Tracker.prepareReplace(tracker, { payload: { rows: [{ value: 1 }] } });
    Tracker.adopt(tracker, winner);
  };
  replace();
  assert.throws(() => Tracker.prepare(stale), /settled/);
  retainedSettledChanges.push(stale);
  await collect(obsolete);
  assert.equal(retainedSettledChanges.length, 1);
} else if (scenario === "same-job-fast-cleanup") {
  assert.ok(global.gc, "worker requires --expose-gc");
  const tracker = Tracker.make({
    rows: Array.from({ length: 100_000 }, (_, value) => ({ value })),
  });
  global.gc();
  const readyHeap = process.memoryUsage().heapUsed;
  const commit = (value: number): void => {
    const change = Tracker.beginChange(tracker);
    const draft = change.state;
    for (let index = 0; index < 4_094; index++) assert.equal(draft.rows[index]!.value, index);
    draft.rows.at(-1)!.value = value;
    const prepared = Tracker.prepare(change);
    Tracker.adopt(tracker, prepared);
  };
  for (let value = 0; value < 100; value++) commit(-value - 1);
  global.gc();
  const retainedMiB = (process.memoryUsage().heapUsed - readyHeap) / (1024 * 1024);
  assert.ok(retainedMiB < 16, `same-job cleanup retained ${retainedMiB.toFixed(2)} MiB`);
  assert.equal(tracker.value.rows.at(-1)!.value, -100);
} else if (scenario === "same-job-folded-ops-cleanup") {
  assert.ok(global.gc, "worker requires --expose-gc");
  const tracker = Tracker.make(
    Object.fromEntries(Array.from({ length: 5_000 }, (_, index) => [`field${index}`, 0])) as Record<
      string,
      number
    >,
  );
  global.gc();
  const readyHeap = process.memoryUsage().heapUsed;
  const commit = (value: number): void => {
    const change = Tracker.beginChange(tracker);
    for (let index = 0; index < 5_000; index++) change.state[`field${index}`] = value;
    const prepared = Tracker.prepare(change);
    assert.equal(prepared.ops.length, 1);
    assert.equal(prepared.ops[0]![0], "r");
    Tracker.adopt(tracker, prepared);
  };
  for (let value = 1; value <= 100; value++) commit(value);
  global.gc();
  const retainedMiB = (process.memoryUsage().heapUsed - readyHeap) / (1024 * 1024);
  assert.ok(retainedMiB < 8, `same-job folded operations retained ${retainedMiB.toFixed(2)} MiB`);
  assert.equal(tracker.value.field0, 100);
} else if (scenario === "adopted-changes-flat") {
  // Each change's context is tracked by the tracker until it is settled; adoption must drop every one, so a long run
  // of small changes keeps only the document, not a record per change.
  const gc = global.gc;
  assert.ok(gc, "worker requires --expose-gc");
  const tracker = Tracker.make({ entries: [{ id: 0, text: "" }] });
  const commit = (): void => {
    const change = Tracker.beginChange(tracker);
    change.state.entries[0]!.text += " tok";
    Tracker.adopt(tracker, Tracker.prepare(change));
  };
  const settledHeap = async (): Promise<number> => {
    for (let round = 0; round < 5; round++) {
      await setImmediate();
      gc();
    }
    return process.memoryUsage().heapUsed;
  };
  for (let index = 0; index < 1_000; index++) commit();
  const readyHeap = await settledHeap();
  for (let index = 0; index < 10_000; index++) {
    commit();
    // Let WeakRef targets go, as between the jobs of real commits.
    if (index % 100 === 0) await setImmediate();
  }
  // The text itself grows 4 bytes per change.
  const perChange = ((await settledHeap()) - readyHeap) / 10_000 - 4;
  assert.ok(perChange < 48, `each adopted change retained ${perChange.toFixed(1)} bytes`);
  assert.equal(tracker.value.entries[0]!.text.length, 4 * 11_000);
} else if (scenario === "abandoned-draft-released") {
  // The tracker holds its most recent open draft strongly: one dropped without settling stays until the tracker's next
  // change or adoption, which releases it.
  type Root = { value: number; payload: { rows: { value: number }[] } | null };
  const abandon = (tracker: Tracker.Tracker<Root>): WeakRef<object> => {
    const change = Tracker.beginChange(tracker);
    change.state.payload = { rows: Array.from({ length: 50_000 }, (_, value) => ({ value })) };
    return new WeakRef(change.state.payload.rows[0]!);
  };
  const byChange = Tracker.make<Root>({ value: 0, payload: null });
  const first = abandon(byChange);
  await assertRetained(first, "the tracker holds its latest open draft");
  const next = Tracker.beginChange(byChange);
  await collect(first);
  // A chain of abandoned drafts keeps only the latest.
  Tracker.abort(next);
  const chain = Array.from({ length: 20 }, () => abandon(byChange));
  for (const reference of chain.slice(0, -1)) await collect(reference);
  await assertRetained(chain.at(-1)!, "the tracker holds its latest open draft");
  Tracker.adopt(byChange, Tracker.prepareReplace(byChange, { value: 1, payload: null }));
  await collect(chain.at(-1)!);

  const byAdopt = Tracker.make<Root>({ value: 0, payload: null });
  const dropped = abandon(byAdopt);
  Tracker.adopt(byAdopt, Tracker.prepareReplace(byAdopt, { value: 1, payload: null }));
  await collect(dropped);
  assert.deepEqual(byAdopt.value, { value: 1, payload: null });
} else if (scenario === "tracker-with-open-draft") {
  const open = (): WeakRef<object> => {
    const tracker = Tracker.make({
      value: 0,
      rows: Array.from({ length: 50_000 }, (_, value) => ({ value })),
    });
    const change = Tracker.beginChange(tracker);
    change.state.rows[0]!.value = -1;
    retainedSettledChanges.push(change);
    const competitor = Tracker.beginChange(tracker);
    competitor.state.value = 1;
    retainedSettledChanges.push(competitor);
    return new WeakRef(tracker);
  };
  await collect(open());
  assert.equal(retainedSettledChanges.length, 2);
} else if (scenario === "lifecycle-churn") {
  const tracker = Tracker.make({ value: 0 });
  for (let index = 0; index < 100_000; index++) {
    const change = Tracker.beginChange(tracker);
    change.state.value = index;
    Tracker.abort(change);
  }
  const prepared = Tracker.prepareReplace(tracker, { value: 1 });
  Tracker.adopt(tracker, prepared);
  assert.deepEqual(tracker.value, { value: 1 });
} else if (scenario === "obsolete-revisions") {
  const tracker = Tracker.make({
    payload: { rows: Array.from({ length: 50_000 }, (_, value) => ({ value })) },
  });
  const oldRoot = new WeakRef(tracker.value.payload.rows);
  const replace = (value: number): void => {
    const prepared = Tracker.prepareReplace(tracker, { payload: { rows: [{ value }] } });
    Tracker.adopt(tracker, prepared);
  };
  for (let value = 0; value < 100; value++) replace(value);
  await collect(oldRoot);
  assert.deepEqual(tracker.value, { payload: { rows: [{ value: 99 }] } });
} else {
  const tracker = Tracker.make<{ payload: { rows: { value: number }[] } | null }>({
    payload: null,
  });
  let reference: WeakRef<object>;
  switch (scenario) {
    case "aborted-payload":
      reference = abortedPayload(tracker);
      break;
    case "unadopted-prepared":
      reference = unadoptedPrepared(tracker);
      break;
    case "draft-proxies":
      reference = draftProxies(tracker);
      break;
    default:
      throw new Error(`unknown retention scenario: ${scenario}`);
  }
  await collect(reference);
  assert.deepEqual(tracker.value, { payload: null });
}

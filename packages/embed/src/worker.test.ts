import assert from "node:assert/strict";
import { test } from "node:test";
import {
  type Deps,
  EmbedError,
  type Job,
  type JobQueue,
  Worker,
} from "./index.js";

const job: Job = {
  id: "j1",
  scope: { organizationId: "acme", collectionId: "kb" },
  prefix: "",
  state: "running",
  attempts: 1,
  lease: "L1",
  leaseMs: 60_000,
};

function fakeQueue(overrides: Partial<JobQueue> & { leaseMs?: number } = {}) {
  const calls: unknown[][] = [];
  const queue: JobQueue = {
    enqueue: async () => job,
    get: async () => job,
    claim: async () =>
      calls.length === 0
        ? { ...job, leaseMs: overrides.leaseMs ?? 60_000 }
        : undefined,
    heartbeat: async () => true,
    succeed: async (...a) => (calls.push(["succeed", ...a]), true),
    fail: async (...a) => (calls.push(["fail", ...a]), true),
    release: async (...a) => void calls.push(["release", ...a]),
    ...overrides,
  };
  return { queue, calls };
}

const depsWith = (list: Deps["source"]["list"]) =>
  ({
    source: { list },
    index: { revisions: async () => new Map() },
  }) as unknown as Deps;

const waitForAbort = (req: { signal?: AbortSignal }) =>
  new Promise<never>((_, bad) =>
    req.signal?.addEventListener("abort", () => bad(new Error("aborted"))),
  );

const worker = (queue: JobQueue, list: Deps["source"]["list"], extra = {}) =>
  new Worker(queue, {
    owner: "w1",
    onError: (err) => {
      throw err;
    },
    resolve: async () => depsWith(list),
    ...extra,
  });

test("a finished sync is recorded with its result", async () => {
  const { queue, calls } = fakeQueue();
  const w = worker(queue, async () => ({ items: [] }));
  assert.equal(await w.runOnce(new AbortController().signal), true);
  assert.deepEqual(calls, [
    [
      "succeed",
      "j1",
      "L1",
      { ingested: 0, refreshed: 0, removed: 0, failed: 0 },
    ],
  ]);
  assert.equal(await w.runOnce(new AbortController().signal), false);
});

test("bad requests are permanent failures and other errors are retried", async () => {
  for (const [err, code, retryable] of [
    [new EmbedError("too_large", "x"), "too_large", false],
    [new EmbedError("not_found", "x"), "not_found", false],
    [new EmbedError("model_error", "x"), "model_error", true],
    [new Error("boom"), "internal", true],
  ] as const) {
    const { queue, calls } = fakeQueue();
    await worker(
      queue,
      async () => {
        throw err;
      },
      { onError: () => {} },
    ).runOnce(new AbortController().signal);
    assert.deepEqual(calls, [["fail", "j1", "L1", code, retryable]]);
  }
});

test("a failure that is not a classified embed error is reported, a classified one is not", async () => {
  for (const [err, reported] of [
    [new Error("boom"), 1],
    [new EmbedError("too_large", "x"), 0],
  ] as const) {
    const errors: unknown[] = [];
    await worker(
      fakeQueue().queue,
      async () => {
        throw err;
      },
      { onError: (e: unknown) => errors.push(e) },
    ).runOnce(new AbortController().signal);
    assert.equal(errors.length, reported);
  }
});

test("shutdown stops the sync and hands the job back", async () => {
  const { queue, calls } = fakeQueue();
  const stop = new AbortController();
  const done = worker(queue, waitForAbort).runOnce(stop.signal);
  setTimeout(() => stop.abort(), 10);
  await done;
  assert.deepEqual(calls, [["release", "j1", "L1"]]);
});

test("a job claimed as shutdown begins is handed back unrun", async () => {
  const stop = new AbortController();
  const { queue, calls } = fakeQueue({
    claim: async () => (stop.abort(), job),
  });
  await worker(queue, async () => {
    throw new Error("must not run");
  }).runOnce(stop.signal);
  assert.deepEqual(calls, [["release", "j1", "L1"]]);
});

test("a worker that lost its lease stops and leaves the job alone", async () => {
  const { queue, calls } = fakeQueue({ heartbeat: async () => false });
  await worker(queue, waitForAbort, { heartbeatMs: 5 }).runOnce(
    new AbortController().signal,
  );
  assert.deepEqual(calls, []);
});

test("a stalled heartbeat cannot keep a run alive past the lease", async () => {
  const { queue, calls } = fakeQueue({
    leaseMs: 30,
    heartbeat: () => new Promise(() => {}),
  });
  await worker(queue, waitForAbort, { heartbeatMs: 5 }).runOnce(
    new AbortController().signal,
  );
  assert.deepEqual(calls, []);
});

test("a failure recording the result does not turn the sync into a failure", async () => {
  const { queue, calls } = fakeQueue({
    succeed: async () => {
      throw new Error("db down");
    },
  });
  await assert.rejects(
    worker(queue, async () => ({ items: [] })).runOnce(
      new AbortController().signal,
    ),
    /db down/,
  );
  assert.deepEqual(calls, []);
});

test("a queue outage is reported and the loop keeps going", async () => {
  const stop = new AbortController();
  const errors: unknown[] = [];
  let claims = 0;
  const { queue } = fakeQueue({
    claim: async () => {
      if (++claims === 1) throw new Error("db down");
      if (claims === 3) stop.abort();
      return undefined;
    },
  });
  await worker(queue, async () => ({ items: [] }), {
    pollMs: 1,
    onError: (e: unknown) => errors.push(e),
  }).run(stop.signal);
  assert.equal(errors.length, 1);
  assert.equal(claims, 3);
});

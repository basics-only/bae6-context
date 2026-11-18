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
};

function fakeQueue(heldLease = true) {
  const calls: unknown[][] = [];
  const queue: JobQueue = {
    enqueue: async () => job,
    get: async () => job,
    claim: async () => (calls.length === 0 ? job : undefined),
    heartbeat: async () => heldLease,
    succeed: async (...a) => (calls.push(["succeed", ...a]), true),
    fail: async (...a) => (calls.push(["fail", ...a]), true),
    release: async (...a) => void calls.push(["release", ...a]),
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

test("a finished sync is recorded with its result", async () => {
  const { queue, calls } = fakeQueue();
  const worker = new Worker(queue, {
    owner: "w1",
    resolve: async () => depsWith(async () => ({ items: [] })),
  });
  assert.equal(await worker.runOnce(new AbortController().signal), true);
  assert.deepEqual(calls, [
    [
      "succeed",
      "j1",
      "w1",
      { ingested: 0, refreshed: 0, removed: 0, failed: 0 },
    ],
  ]);
  assert.equal(await worker.runOnce(new AbortController().signal), false);
});

test("bad requests are permanent failures and other errors are retried", async () => {
  for (const [err, code, retryable] of [
    [new EmbedError("too_large", "x"), "too_large", false],
    [new EmbedError("model_error", "x"), "model_error", true],
    [new Error("boom"), "internal", true],
  ] as const) {
    const { queue, calls } = fakeQueue();
    const worker = new Worker(queue, {
      owner: "w1",
      resolve: async () =>
        depsWith(async () => {
          throw err;
        }),
    });
    await worker.runOnce(new AbortController().signal);
    assert.deepEqual(calls, [["fail", "j1", "w1", code, retryable]]);
  }
});

test("shutdown stops the sync and hands the job back", async () => {
  const { queue, calls } = fakeQueue();
  const worker = new Worker(queue, {
    owner: "w1",
    resolve: async () => depsWith(waitForAbort),
  });
  const stop = new AbortController();
  const done = worker.runOnce(stop.signal);
  setTimeout(() => stop.abort(), 10);
  await done;
  assert.deepEqual(calls, [["release", "j1", "w1"]]);
});

test("a worker that lost its lease stops and leaves the job alone", async () => {
  const { queue, calls } = fakeQueue(false);
  const worker = new Worker(queue, {
    owner: "w1",
    heartbeatMs: 5,
    resolve: async () => depsWith(waitForAbort),
  });
  await worker.runOnce(new AbortController().signal);
  assert.deepEqual(calls, []);
});

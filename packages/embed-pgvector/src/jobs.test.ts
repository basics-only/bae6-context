import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { after, before, beforeEach, test } from "node:test";
import type { Scope } from "@bae6/context-embed";
import pg from "pg";
import { type JobQueueOptions, PgJobQueue, PgVectorIndex } from "./index.js";

const url = process.env.BAE6_EMBED_DATABASE_URL;
const skip = url ? false : "BAE6_EMBED_DATABASE_URL not set";
const pool = new pg.Pool({ connectionString: url, max: 8 });
const index = new PgVectorIndex(pool);
const queue = (opts?: JobQueueOptions) => new PgJobQueue(pool, opts);
const result = { ingested: 1, refreshed: 0, removed: 0, failed: 0 };
const sleep = (ms: number) => new Promise((ok) => setTimeout(ok, ms));

before(async () => {
  if (url) await PgVectorIndex.migrate(pool);
});
beforeEach(async () => {
  if (url) await pool.query("DELETE FROM jobs");
});
after(() => pool.end());

async function collection(organizationId = `org-${randomUUID()}`) {
  const scope: Scope = { organizationId, collectionId: `kb-${randomUUID()}` };
  await index.ensureCollection(scope, "m1", 2);
  return scope;
}

test(
  "enqueueing the same sync again returns the waiting job",
  { skip },
  async () => {
    const q = queue();
    const scope = await collection();
    const a = await q.enqueue(scope, "docs/");
    assert.equal((await q.enqueue(scope, "docs/")).id, a.id);
    assert.notEqual((await q.enqueue(scope, "")).id, a.id);
    await assert.rejects(q.enqueue({ ...scope, collectionId: "nope" }, ""), {
      code: "invalid_request",
    });
    await assert.rejects(q.enqueue(scope, "x".repeat(1025)), {
      code: "invalid_request",
    });
  },
);

test("a job is invisible to another tenant", { skip }, async () => {
  const q = queue();
  const scope = await collection();
  const job = await q.enqueue(scope, "");
  assert.equal((await q.get(scope, job.id))?.state, "queued");
  assert.equal(
    await q.get({ ...scope, organizationId: "other" }, job.id),
    undefined,
  );
  assert.equal(await q.get(scope, "not-a-uuid"), undefined);
});

test("only the lease holder can finish a job", { skip }, async () => {
  const q = queue();
  const scope = await collection();
  await q.enqueue(scope, "");
  const job = await q.claim("w1");
  assert.equal(job?.state, "running");
  assert.equal(await q.claim("w2"), undefined);
  assert.equal(await q.succeed(job!.id, "w1", result), false);
  assert.equal(await q.succeed(job!.id, job!.lease!, result), true);
  assert.deepEqual((await q.get(scope, job!.id))?.result, result);
});

test("three replicas never run the same job twice", { skip }, async () => {
  const q = queue();
  const scopes = await Promise.all(
    Array.from({ length: 9 }, () => collection()),
  );
  await Promise.all(scopes.map((s) => q.enqueue(s, "")));
  const claimed: string[] = [];
  await Promise.all(
    ["w1", "w2", "w3"].map(async (w) => {
      for (let i = 0; i < 6; i++) {
        const job = await q.claim(w);
        if (job) claimed.push(job.id);
      }
    }),
  );
  for (let job = await q.claim("w1"); job; job = await q.claim("w1")) {
    claimed.push(job.id);
  }
  assert.equal(claimed.length, 9);
  assert.equal(new Set(claimed).size, 9);
});

test("a tenant runs at most its concurrency limit", { skip }, async () => {
  const q = queue({ maxRunningPerTenant: 2 });
  const org = `org-${randomUUID()}`;
  for (let i = 0; i < 3; i++) await q.enqueue(await collection(org), "");
  const first = await q.claim("w1");
  assert.ok(await q.claim("w2"));
  assert.equal(await q.claim("w3"), undefined);
  await q.succeed(first!.id, first!.lease!, result);
  assert.ok(await q.claim("w3"));
});

test("a tenant at its limit does not starve the others", { skip }, async () => {
  const q = queue({ maxRunningPerTenant: 1 });
  const busy = `org-${randomUUID()}`;
  for (let i = 0; i < 7; i++) await q.enqueue(await collection(busy), "");
  assert.ok(await q.claim("w1"));
  const other = await collection();
  const waiting = await q.enqueue(other, "");
  assert.equal((await q.claim("w2"))?.id, waiting.id);
});

test(
  "a job whose lease ran out is taken over and the old owner is fenced",
  { skip },
  async () => {
    const q = queue({ leaseMs: 50 });
    const scope = await collection();
    await q.enqueue(scope, "");
    const first = await q.claim("w1");
    await sleep(120);
    assert.equal(await q.heartbeat(first!.id, first!.lease!), false);
    const second = await q.claim("w1");
    assert.equal(second?.id, first!.id);
    assert.equal(second?.attempts, 2);
    assert.equal(await q.heartbeat(first!.id, first!.lease!), false);
    assert.equal(await q.succeed(first!.id, first!.lease!, result), false);
    assert.equal(await q.succeed(second!.id, second!.lease!, result), true);
  },
);

test("a job that keeps losing its lease ends up failed", { skip }, async () => {
  const q = queue({ leaseMs: 30, maxAttempts: 2 });
  const scope = await collection();
  const job = await q.enqueue(scope, "");
  for (const w of ["w1", "w2"]) {
    assert.ok(await q.claim(w));
    await sleep(80);
  }
  assert.equal(await q.claim("w3"), undefined);
  const done = await q.get(scope, job.id);
  assert.deepEqual([done?.state, done?.errorCode], ["failed", "lease_expired"]);
});

test(
  "a retryable failure waits out a backoff and a permanent one does not return",
  { skip },
  async () => {
    const q = queue();
    const scope = await collection();
    const job = await q.enqueue(scope, "");
    const run = await q.claim("w1");
    assert.equal(await q.fail(job.id, run!.lease!, "model_error", true), true);
    assert.equal((await q.get(scope, job.id))?.state, "queued");
    assert.equal(await q.claim("w2"), undefined);

    const other = await collection();
    const bad = await q.enqueue(other, "");
    const badRun = await q.claim("w1");
    await q.fail(bad.id, badRun!.lease!, "invalid_request", false);
    const failed = await q.get(other, bad.id);
    assert.deepEqual(
      [failed?.state, failed?.errorCode],
      ["failed", "invalid_request"],
    );
  },
);

test(
  "a released job is claimable at once and keeps its attempts",
  { skip },
  async () => {
    const q = queue();
    const scope = await collection();
    await q.enqueue(scope, "");
    const first = await q.claim("w1");
    await q.release(first!.id, first!.lease!);
    const again = await q.claim("w2");
    assert.equal(again?.id, first!.id);
    assert.equal(again?.attempts, 1);
  },
);

test("a tenant cannot queue without bound", { skip }, async () => {
  const q = queue({ maxActivePerTenant: 2 });
  const scope = await collection();
  await q.enqueue(scope, "a/");
  await q.enqueue(scope, "b/");
  await assert.rejects(q.enqueue(scope, "c/"), { code: "too_large" });
  assert.equal((await q.enqueue(scope, "a/")).prefix, "a/");
});

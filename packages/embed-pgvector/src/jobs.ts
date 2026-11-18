import { randomUUID } from "node:crypto";
import {
  EmbedError,
  type Job,
  type JobQueue,
  type JobState,
  type Scope,
  type SyncResult,
} from "@bae6/context-embed";
import type { Pool, PoolClient } from "pg";
import { inTransaction } from "./tx.js";

export const JOBS_SCHEMA = `
CREATE TABLE IF NOT EXISTS jobs (
  job_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  organization_id text NOT NULL,
  collection_id text NOT NULL,
  prefix text NOT NULL,
  state text NOT NULL CHECK (state IN ('queued', 'running', 'succeeded', 'failed')),
  attempts int NOT NULL DEFAULT 0,
  run_after timestamptz NOT NULL DEFAULT clock_timestamp(),
  lease_owner text,
  lease_expires_at timestamptz,
  result jsonb,
  error_code text,
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  FOREIGN KEY (organization_id, collection_id)
    REFERENCES collections ON DELETE CASCADE
);
CREATE UNIQUE INDEX IF NOT EXISTS jobs_one_queued
  ON jobs (organization_id, collection_id, prefix) WHERE state = 'queued';
CREATE INDEX IF NOT EXISTS jobs_claimable ON jobs (state, run_after);
CREATE INDEX IF NOT EXISTS jobs_by_collection ON jobs (organization_id, collection_id);
`;

export const MAX_PREFIX_LENGTH = 1024;
const LOCK_NAMESPACE = 6_201_105;
const CLAIM_CANDIDATES = 5;
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const FINISHED_RETENTION = "7 days";

export interface JobQueueOptions {
  leaseMs?: number;
  maxAttempts?: number;
  /** Jobs one organization may have running at once, across all replicas. */
  maxRunningPerTenant?: number;
  /** Queued plus running jobs one organization may hold. */
  maxActivePerTenant?: number;
}

const toJob = (r: Record<string, unknown>): Job => ({
  id: r.job_id as string,
  scope: {
    organizationId: r.organization_id as string,
    collectionId: r.collection_id as string,
  },
  prefix: r.prefix as string,
  state: r.state as JobState,
  attempts: r.attempts as number,
  ...(r.result ? { result: r.result as SyncResult } : {}),
  ...(r.error_code ? { errorCode: r.error_code as string } : {}),
  ...(r.state === "running" ? { lease: r.lease_owner as string } : {}),
});

const lockTenant = (client: PoolClient, organizationId: string) =>
  client.query("SELECT pg_advisory_xact_lock($1, hashtext($2))", [
    LOCK_NAMESPACE,
    organizationId,
  ]);

/** Durable jobs with leases: a crashed worker's job is picked up again once its lease runs out. */
export class PgJobQueue implements JobQueue {
  private readonly leaseMs: number;
  private readonly maxAttempts: number;
  private readonly maxRunning: number;
  private readonly maxActive: number;

  constructor(
    private readonly pool: Pool,
    opts: JobQueueOptions = {},
  ) {
    this.leaseMs = opts.leaseMs ?? 60_000;
    this.maxAttempts = opts.maxAttempts ?? 3;
    this.maxRunning = opts.maxRunningPerTenant ?? 2;
    this.maxActive = opts.maxActivePerTenant ?? 100;
  }

  async enqueue(scope: Scope, prefix: string): Promise<Job> {
    if (prefix.length > MAX_PREFIX_LENGTH) {
      throw new EmbedError(
        "invalid_request",
        `prefix is at most ${MAX_PREFIX_LENGTH} characters.`,
      );
    }
    return inTransaction(
      this.pool,
      async (client) => {
        await lockTenant(client, scope.organizationId);
        const known = await client.query(
          `SELECT 1 FROM collections WHERE organization_id = $1 AND collection_id = $2`,
          [scope.organizationId, scope.collectionId],
        );
        if (known.rowCount === 0) {
          throw new EmbedError("invalid_request", "unknown collection.");
        }
        await client.query(
          `DELETE FROM jobs WHERE organization_id = $1
           AND state IN ('succeeded', 'failed')
           AND created_at < clock_timestamp() - $2::interval`,
          [scope.organizationId, FINISHED_RETENTION],
        );
        const active = await client.query(
          `SELECT count(*)::int AS n FROM jobs
         WHERE organization_id = $1 AND state IN ('queued', 'running')`,
          [scope.organizationId],
        );
        const { rows } = await client.query(
          `INSERT INTO jobs (organization_id, collection_id, prefix, state)
         SELECT $1, $2, $3, 'queued'
         WHERE $4::int < $5 OR EXISTS (
           SELECT 1 FROM jobs WHERE organization_id = $1 AND collection_id = $2
             AND prefix = $3 AND state = 'queued')
         ON CONFLICT (organization_id, collection_id, prefix) WHERE state = 'queued'
         DO UPDATE SET prefix = jobs.prefix
         RETURNING *`,
          [
            scope.organizationId,
            scope.collectionId,
            prefix,
            active.rows[0].n,
            this.maxActive,
          ],
        );
        if (rows.length === 0) {
          throw new EmbedError("too_large", "too many jobs are waiting.");
        }
        return toJob(rows[0]);
      },
      { timeoutMs: 10_000 },
    );
  }

  async get(scope: Scope, id: string): Promise<Job | undefined> {
    if (!UUID.test(id)) return undefined;
    const { rows } = await this.pool.query(
      `SELECT * FROM jobs
       WHERE job_id = $1 AND organization_id = $2 AND collection_id = $3`,
      [id, scope.organizationId, scope.collectionId],
    );
    return rows[0] ? toJob(rows[0]) : undefined;
  }

  async claim(owner: string): Promise<Job | undefined> {
    return inTransaction(
      this.pool,
      async (client) => {
        await client.query(
          `UPDATE jobs SET state = 'failed', error_code = 'lease_expired',
                           lease_owner = NULL, lease_expires_at = NULL
           WHERE state = 'running' AND lease_expires_at < clock_timestamp()
             AND attempts >= $1`,
          [this.maxAttempts],
        );
        const candidates = await client.query(
          `SELECT job_id, organization_id FROM jobs
           WHERE ((state = 'queued' AND run_after <= clock_timestamp())
              OR (state = 'running' AND lease_expires_at < clock_timestamp()))
             AND (SELECT count(*) FROM jobs r
                  WHERE r.organization_id = jobs.organization_id
                    AND r.state = 'running'
                    AND r.lease_expires_at >= clock_timestamp()) < $1
           ORDER BY created_at
           LIMIT ${CLAIM_CANDIDATES}
           FOR UPDATE SKIP LOCKED`,
          [this.maxRunning],
        );
        for (const c of candidates.rows) {
          // A tenant whose lock is held is busy being claimed or enqueued: skip it, never wait.
          const lock = await client.query(
            "SELECT pg_try_advisory_xact_lock($1, hashtext($2)) AS ok",
            [LOCK_NAMESPACE, c.organization_id],
          );
          if (!lock.rows[0].ok) continue;
          const running = await client.query(
            `SELECT count(*)::int AS n FROM jobs
             WHERE organization_id = $1 AND state = 'running'
               AND lease_expires_at >= clock_timestamp()`,
            [c.organization_id],
          );
          if (running.rows[0].n >= this.maxRunning) continue;
          const { rows } = await client.query(
            `UPDATE jobs SET state = 'running', attempts = attempts + 1,
                             lease_owner = $2,
                             lease_expires_at = clock_timestamp() + $3 * interval '1 millisecond'
             WHERE job_id = $1
             RETURNING *`,
            [c.job_id, `${owner}:${randomUUID()}`, this.leaseMs],
          );
          return { ...toJob(rows[0]), leaseMs: this.leaseMs };
        }
        return undefined;
      },
      { timeoutMs: 10_000 },
    );
  }

  async heartbeat(id: string, owner: string) {
    const res = await this.pool.query(
      `UPDATE jobs
       SET lease_expires_at = clock_timestamp() + $3 * interval '1 millisecond'
       WHERE job_id = $1 AND lease_owner = $2 AND state = 'running'
         AND lease_expires_at >= clock_timestamp()`,
      [id, owner, this.leaseMs],
    );
    return res.rowCount === 1;
  }

  async succeed(id: string, owner: string, result: SyncResult) {
    const res = await this.pool.query(
      `UPDATE jobs SET state = 'succeeded', result = $3, error_code = NULL,
                       lease_owner = NULL, lease_expires_at = NULL
       WHERE job_id = $1 AND lease_owner = $2 AND state = 'running'
         AND lease_expires_at >= clock_timestamp()`,
      [id, owner, JSON.stringify(result)],
    );
    return res.rowCount === 1;
  }

  fail(id: string, owner: string, code: string, retryable: boolean) {
    return this.settle(id, owner, code, retryable, false);
  }

  async release(id: string, owner: string) {
    await this.settle(id, owner, null, true, true);
  }

  /** Back to the queue after a backoff while attempts remain, else failed. A shutdown refunds its attempt. */
  private settle(
    id: string,
    owner: string,
    code: string | null,
    retryable: boolean,
    refund: boolean,
  ) {
    return inTransaction(
      this.pool,
      async (client) => {
        const lock = await client.query(
          `SELECT pg_advisory_xact_lock($1, hashtext(organization_id))
         FROM jobs WHERE job_id = $2`,
          [LOCK_NAMESPACE, id],
        );
        if (lock.rowCount === 0) return false;
        const cur = await client.query(
          `SELECT attempts,
                EXISTS (SELECT 1 FROM jobs q
                        WHERE q.organization_id = j.organization_id
                          AND q.collection_id = j.collection_id
                          AND q.prefix = j.prefix AND q.state = 'queued') AS covered
         FROM jobs j
         WHERE job_id = $1 AND lease_owner = $2 AND state = 'running'`,
          [id, owner],
        );
        if (cur.rowCount === 0) return false;
        const attempts = cur.rows[0].attempts - (refund ? 1 : 0);
        // A job already waiting for the same prefix covers this one.
        const requeue =
          retryable && attempts < this.maxAttempts && !cur.rows[0].covered;
        await client.query(
          `UPDATE jobs SET state = $3, attempts = $4,
                run_after = clock_timestamp() + $5 * interval '1 second',
                error_code = $6, lease_owner = NULL, lease_expires_at = NULL
         WHERE job_id = $1 AND lease_owner = $2`,
          [
            id,
            owner,
            requeue ? "queued" : "failed",
            attempts,
            refund ? 0 : Math.min(2 ** attempts, 60),
            requeue ? code : (code ?? "superseded"),
          ],
        );
        return true;
      },
      { timeoutMs: 10_000 },
    );
  }
}

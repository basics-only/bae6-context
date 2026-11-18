import { EmbedError, type Scope } from "./index.js";
import { type Deps, type SyncResult, syncCollection } from "./pipeline.js";

export type JobState = "queued" | "running" | "succeeded" | "failed";

export interface Job {
  id: string;
  scope: Scope;
  prefix: string;
  state: JobState;
  attempts: number;
  result?: SyncResult;
  errorCode?: string;
}

/** Every transition names the owner, so a worker that lost its lease cannot change a job it no longer holds. */
export interface JobQueue {
  /** One queued job per (collection, prefix): enqueueing again returns the waiting one. */
  enqueue(scope: Scope, prefix: string): Promise<Job>;
  /** Tenant-scoped: another tenant's job is `undefined`. */
  get(scope: Scope, id: string): Promise<Job | undefined>;
  claim(owner: string): Promise<Job | undefined>;
  heartbeat(id: string, owner: string): Promise<boolean>;
  succeed(id: string, owner: string, result: SyncResult): Promise<boolean>;
  fail(
    id: string,
    owner: string,
    code: string,
    retryable: boolean,
  ): Promise<boolean>;
  release(id: string, owner: string): Promise<void>;
}

export interface WorkerOptions {
  owner: string;
  /** The collection's source, model, index and ACL for this job's tenant. */
  resolve(job: Job, signal: AbortSignal): Promise<Deps>;
  heartbeatMs?: number;
  pollMs?: number;
  maxObjects?: number;
}

const PERMANENT = new Set(["invalid_request", "too_large"]);

const pause = (ms: number, signal: AbortSignal) =>
  new Promise<void>((ok) => {
    const t = setTimeout(ok, ms);
    signal.addEventListener(
      "abort",
      () => {
        clearTimeout(t);
        ok();
      },
      { once: true },
    );
  });

export class Worker {
  constructor(
    private readonly queue: JobQueue,
    private readonly opts: WorkerOptions,
  ) {}

  /** Runs at most one job; resolves false when nothing was claimable. */
  async runOnce(signal: AbortSignal): Promise<boolean> {
    const { owner, heartbeatMs = 15_000, maxObjects } = this.opts;
    const job = await this.queue.claim(owner);
    if (!job) return false;
    const run = new AbortController();
    const stop = () => run.abort();
    signal.addEventListener("abort", stop, { once: true });
    let lostLease = false;
    const beat = setInterval(() => {
      this.queue.heartbeat(job.id, owner).then(
        (held) => {
          if (!held) {
            lostLease = true;
            run.abort();
          }
        },
        () => {
          lostLease = true;
          run.abort();
        },
      );
    }, heartbeatMs);
    try {
      const deps = await this.opts.resolve(job, run.signal);
      const result = await syncCollection(deps, job.scope, {
        prefix: job.prefix,
        signal: run.signal,
        ...(maxObjects ? { maxObjects } : {}),
      });
      await this.queue.succeed(job.id, owner, result);
    } catch (err) {
      if (lostLease) return true;
      if (signal.aborted) {
        await this.queue.release(job.id, owner);
        return true;
      }
      const code = err instanceof EmbedError ? err.code : "internal";
      await this.queue.fail(job.id, owner, code, !PERMANENT.has(code));
    } finally {
      clearInterval(beat);
      signal.removeEventListener("abort", stop);
    }
    return true;
  }

  /** Returns once `signal` aborts and the job in flight has been released or finished. */
  async run(signal: AbortSignal) {
    const { pollMs = 1000 } = this.opts;
    while (!signal.aborted) {
      if (!(await this.runOnce(signal))) await pause(pollMs, signal);
    }
  }
}

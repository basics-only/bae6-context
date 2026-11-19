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
  /** Present on a claimed job: a token unique to this claim, presented on every later call. */
  lease?: string;
  leaseMs?: number;
}

/** Every transition presents the claim's lease token, so a worker that lost its lease cannot change the job. */
export interface JobQueue {
  /** One queued job per (collection, prefix): enqueueing again returns the waiting one. */
  enqueue(scope: Scope, prefix: string): Promise<Job>;
  /** Tenant-scoped: another tenant's job is `undefined`. */
  get(scope: Scope, id: string): Promise<Job | undefined>;
  claim(owner: string): Promise<Job | undefined>;
  heartbeat(id: string, lease: string): Promise<boolean>;
  succeed(id: string, lease: string, result: SyncResult): Promise<boolean>;
  fail(
    id: string,
    lease: string,
    code: string,
    retryable: boolean,
  ): Promise<boolean>;
  release(id: string, lease: string): Promise<void>;
}

export interface WorkerOptions {
  owner: string;
  /** The collection's source, model, index and ACL for this job's tenant. */
  resolve(job: Job, signal: AbortSignal): Promise<Deps>;
  /** Queue outages land here; the loop backs off and carries on. */
  onError(err: unknown): void;
  heartbeatMs?: number;
  pollMs?: number;
  maxObjects?: number;
}

const PERMANENT = new Set(["invalid_request", "too_large", "not_found"]);

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
    const lease = job.lease;
    if (!lease || !job.leaseMs) throw new Error("claim returned no lease");
    if (signal.aborted) {
      await this.queue.release(job.id, lease);
      return true;
    }

    const run = new AbortController();
    const stop = () => run.abort();
    signal.addEventListener("abort", stop, { once: true });
    let lostLease = false;
    const lose = () => {
      lostLease = true;
      run.abort();
    };
    // The lease is only trusted until the deadline; a stalled heartbeat cannot extend it.
    let deadline = Date.now() + job.leaseMs;
    let beating = false;
    const beat = setInterval(() => {
      if (Date.now() >= deadline) return lose();
      if (beating) return;
      beating = true;
      const started = Date.now();
      this.queue
        .heartbeat(job.id, lease)
        .then((held) => {
          if (held) deadline = started + (job.leaseMs ?? 0);
          else lose();
        }, lose)
        .finally(() => {
          beating = false;
        });
    }, heartbeatMs);

    let result: SyncResult | undefined;
    let failure: unknown;
    try {
      const deps = await this.opts.resolve(job, run.signal);
      result = await syncCollection(deps, job.scope, {
        prefix: job.prefix,
        signal: run.signal,
        ...(maxObjects ? { maxObjects } : {}),
      });
    } catch (err) {
      failure = err;
    } finally {
      clearInterval(beat);
      signal.removeEventListener("abort", stop);
    }

    if (lostLease) return true;
    if (result) {
      await this.queue.succeed(job.id, lease, result);
    } else if (signal.aborted) {
      await this.queue.release(job.id, lease);
    } else {
      const code = failure instanceof EmbedError ? failure.code : "internal";
      await this.queue.fail(job.id, lease, code, !PERMANENT.has(code));
      if (!(failure instanceof EmbedError)) this.opts.onError(failure);
    }
    return true;
  }

  /** Returns once `signal` aborts and the job in flight has been released or finished. */
  async run(signal: AbortSignal) {
    const { pollMs = 1000 } = this.opts;
    while (!signal.aborted) {
      try {
        if (await this.runOnce(signal)) continue;
      } catch (err) {
        this.opts.onError(err);
      }
      await pause(pollMs, signal);
    }
  }
}

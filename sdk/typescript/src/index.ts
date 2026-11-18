export interface ContextOptions {
  baseUrl: string;
  /** Bearer token identifying this workload. */
  credentials: string;
  /** Per attempt. Defaults to 10000; raise it above the source's query timeout. */
  timeoutMs?: number;
}

export interface CallOptions {
  /** Act for this user; must belong to the workload's organization and application. */
  endUserToken?: string;
  signal?: AbortSignal;
}

export type Scalar = string | number | boolean | null;

export interface CapabilityDescriptor {
  kind: string;
  id: string;
  operations: string[];
  limits?: Record<string, number>;
  attributes?: Record<string, unknown>;
}

export interface QueryRequest {
  statement: string;
  /** In the source's own dialect: an array for Postgres, an object for Databricks. */
  parameters?: Scalar[] | Record<string, Scalar>;
  maxRows?: number;
  timeoutMs?: number;
}

export interface QueryResult {
  columns: { name: string; type: string }[];
  rows: (string | null)[][];
  truncated: boolean;
  access: "service_principal" | "delegated";
}

export interface ObjectInfo {
  key: string;
  size: number;
  etag: string;
  versionId?: string;
  lastModified: string;
}

export interface ObjectContent {
  body: Uint8Array;
  /** The whole object's size, also for a range. */
  size: number;
  etag: string;
  lastModified: string;
  versionId?: string;
}

export class ContextError extends Error {
  constructor(
    message: string,
    readonly requestId?: string,
  ) {
    super(message);
    this.name = new.target.name;
  }
}

/** The service answered with an error; `code` is null when the body was not an envelope. */
export class ContextApiError extends ContextError {
  constructor(
    readonly status: number,
    readonly code: string | null,
    message: string,
    requestId?: string,
    readonly retryAfterSeconds?: number,
  ) {
    super(message, requestId);
  }
}

/** A 2xx response this SDK cannot use. */
export class ContextInvalidResponseError extends ContextError {}

/** The service was not reached or the connection failed. */
export class ContextTransportError extends ContextError {}

export class ContextTimeoutError extends ContextTransportError {}

const MAX_RESPONSE_BYTES = 64 * 1024 * 1024;
const MAX_ATTEMPTS = 3;
const RETRYABLE = new Set([502, 503, 504]);

type Json = Record<string, unknown>;
const isObject = (v: unknown): v is Json =>
  typeof v === "object" && v !== null && !Array.isArray(v);
const str = (v: unknown) => (typeof v === "string" ? v : undefined);

function parseJson(bytes: Uint8Array): unknown {
  try {
    return JSON.parse(Buffer.from(bytes).toString("utf8"));
  } catch {
    return undefined;
  }
}

function apiError(status: number, bytes: Uint8Array): ContextApiError {
  const body = parseJson(bytes);
  const envelope = isObject(body) && typeof body.code === "string";
  const b = envelope ? body : {};
  const retry = b.retryAfterSeconds;
  return new ContextApiError(
    status,
    envelope ? (body.code as string) : null,
    str(b.message) ?? `the service returned HTTP ${status}`,
    str(b.requestId),
    typeof retry === "number" ? retry : undefined,
  );
}

const invalid = () =>
  new ContextInvalidResponseError("the service returned an unusable response");

interface Raw {
  status: number;
  headers: Headers;
  body: Uint8Array;
}

interface Send {
  body?: unknown;
  headers?: Record<string, string>;
  idempotencyKey?: string;
  signal?: AbortSignal;
}

export class Transport {
  readonly #baseUrl: string;
  readonly #credentials: string;
  readonly #timeoutMs: number;

  constructor(options: ContextOptions) {
    this.#baseUrl = options.baseUrl.replace(/\/+$/, "");
    this.#credentials = options.credentials;
    this.#timeoutMs = options.timeoutMs ?? 10_000;
  }

  /** The parsed JSON of a 2xx response. */
  async request(method: string, path: string, opts: Send = {}) {
    const res = await this.raw(method, path, opts);
    const parsed = parseJson(res.body);
    if (parsed === undefined) throw invalid();
    return parsed;
  }

  /** Any 2xx response; retries idempotent requests on 502/503/504, timeouts and dropped connections. */
  async raw(method: string, path: string, opts: Send = {}): Promise<Raw> {
    const idempotent = method === "GET" || opts.idempotencyKey !== undefined;
    for (let attempt = 1; ; attempt++) {
      const last = !idempotent || attempt === MAX_ATTEMPTS;
      let res: Raw;
      try {
        res = await this.#once(method, path, opts);
      } catch (err) {
        if (last || opts.signal?.aborted) throw err;
        await backoff(attempt);
        continue;
      }
      if (res.status >= 200 && res.status < 300) return res;
      const err = apiError(res.status, res.body);
      if (
        last ||
        !RETRYABLE.has(res.status) ||
        (err.retryAfterSeconds ?? 0) > 1
      ) {
        throw err;
      }
      await backoff(attempt);
    }
  }

  async #once(method: string, path: string, opts: Send): Promise<Raw> {
    const timeout = AbortSignal.timeout(this.#timeoutMs);
    const signal = opts.signal
      ? AbortSignal.any([opts.signal, timeout])
      : timeout;
    try {
      const res = await fetch(this.#baseUrl + path, {
        method,
        // Credentials must not follow a redirect to another host.
        redirect: "manual",
        signal,
        headers: {
          authorization: `Bearer ${this.#credentials}`,
          ...(opts.idempotencyKey
            ? { "idempotency-key": opts.idempotencyKey }
            : {}),
          ...(opts.body === undefined
            ? {}
            : { "content-type": "application/json" }),
          ...opts.headers,
        },
        ...(opts.body === undefined ? {} : { body: JSON.stringify(opts.body) }),
      });
      const chunks: Uint8Array[] = [];
      let size = 0;
      for await (const chunk of res.body ?? []) {
        size += chunk.byteLength;
        if (size > MAX_RESPONSE_BYTES) {
          throw new ContextTransportError("the service response is too large");
        }
        chunks.push(chunk);
      }
      return {
        status: res.status,
        headers: res.headers,
        body: Buffer.concat(chunks),
      };
    } catch (cause) {
      if (cause instanceof ContextError) throw cause;
      if (timeout.aborted) {
        throw new ContextTimeoutError(
          `the service did not answer within ${this.#timeoutMs} ms`,
        );
      }
      // No `cause`: fetch's error can quote a header value, i.e. a token.
      throw new ContextTransportError(
        `could not reach the service (${cause instanceof Error ? cause.name : "error"})`,
      );
    }
  }
}

const backoff = (attempt: number) =>
  new Promise((ok) => setTimeout(ok, Math.random() * 40 * attempt));

const enc = encodeURIComponent;

function headers(options: CallOptions, extra: Record<string, string> = {}) {
  return {
    ...(options.endUserToken
      ? { "bae6-end-user-token": options.endUserToken }
      : {}),
    ...extra,
  };
}

/** The bae6-context data API: tenant-scoped, read-only access to configured sources. */
export class DataClient {
  readonly #http: Transport;

  constructor(options: ContextOptions) {
    this.#http = new Transport(options);
  }

  /** `unavailable`: IDs of sources that could not be opened just now. */
  async sources(
    options: CallOptions = {},
  ): Promise<{ items: CapabilityDescriptor[]; unavailable: string[] }> {
    const body = await this.#http.request("GET", "/v1/sources", {
      headers: headers(options),
      ...signal(options),
    });
    if (
      !isObject(body) ||
      !Array.isArray(body.items) ||
      !Array.isArray(body.unavailable)
    ) {
      throw invalid();
    }
    return body as { items: CapabilityDescriptor[]; unavailable: string[] };
  }

  /** `sourceToken`: a token issued for the source itself, for delegated sources only. */
  async query(
    sourceId: string,
    request: QueryRequest,
    options: CallOptions & { sourceToken?: string } = {},
  ): Promise<QueryResult> {
    const body = await this.#http.request(
      "POST",
      `/v1/sources/${enc(sourceId)}/query`,
      {
        body: request,
        headers: headers(
          options,
          options.sourceToken
            ? { "bae6-source-token": options.sourceToken }
            : {},
        ),
        ...signal(options),
      },
    );
    if (
      !isObject(body) ||
      !Array.isArray(body.rows) ||
      !Array.isArray(body.columns)
    ) {
      throw invalid();
    }
    return body as unknown as QueryResult;
  }

  async listObjects(
    sourceId: string,
    page: { prefix?: string; cursor?: string; limit?: number } = {},
    options: CallOptions = {},
  ): Promise<{ items: ObjectInfo[]; nextCursor?: string }> {
    const q = new URLSearchParams();
    if (page.prefix !== undefined) q.set("prefix", page.prefix);
    if (page.cursor !== undefined) q.set("cursor", page.cursor);
    if (page.limit !== undefined) q.set("limit", String(page.limit));
    const body = await this.#http.request(
      "GET",
      `/v1/sources/${enc(sourceId)}/objects?${q}`,
      { headers: headers(options), ...signal(options) },
    );
    if (!isObject(body) || !Array.isArray(body.items)) throw invalid();
    return body as { items: ObjectInfo[]; nextCursor?: string };
  }

  async statObject(
    sourceId: string,
    key: string,
    version: { versionId?: string } = {},
    options: CallOptions = {},
  ): Promise<ObjectInfo> {
    const body = await this.#http.request(
      "GET",
      `/v1/sources/${enc(sourceId)}/objects/metadata?${target(key, version.versionId)}`,
      { headers: headers(options), ...signal(options) },
    );
    if (!isObject(body) || typeof body.key !== "string") throw invalid();
    return body as unknown as ObjectInfo;
  }

  /** The object's bytes, or the inclusive `range` of them. */
  async readObject(
    sourceId: string,
    key: string,
    opts: { versionId?: string; range?: { start: number; end: number } } = {},
    options: CallOptions = {},
  ): Promise<ObjectContent> {
    const res = await this.#http.raw(
      "GET",
      `/v1/sources/${enc(sourceId)}/objects/content?${target(key, opts.versionId)}`,
      {
        headers: headers(
          options,
          opts.range
            ? { range: `bytes=${opts.range.start}-${opts.range.end}` }
            : {},
        ),
        ...signal(options),
      },
    );
    const etag = res.headers.get("etag");
    const modified = res.headers.get("last-modified");
    const total = /\/(\d+)$/.exec(res.headers.get("content-range") ?? "")?.[1];
    if (!etag || !modified || (res.status === 206) !== (total !== undefined)) {
      throw invalid();
    }
    const versionId = res.headers.get("bae6-version-id");
    return {
      body: res.body,
      size: total === undefined ? res.body.byteLength : Number(total),
      etag: etag.replace(/^"|"$/g, ""),
      lastModified: new Date(modified).toISOString(),
      ...(versionId ? { versionId } : {}),
    };
  }
}

const signal = (o: CallOptions) => (o.signal ? { signal: o.signal } : {});

function target(key: string, versionId: string | undefined) {
  const q = new URLSearchParams({ key });
  if (versionId !== undefined) q.set("versionId", versionId);
  return q;
}

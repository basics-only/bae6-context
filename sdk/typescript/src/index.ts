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

const SYNC_STATES = ["queued", "running", "succeeded", "failed"];

// fetch resolves "." and ".." segments, which would send the request elsewhere.
function segment(id: string) {
  if (id === "." || id === "..") {
    throw new ContextError("not a valid identifier");
  }
  return enc(id);
}
const collection = (id: string) => `/v1/collections/${segment(id)}`;

/** The embed API acts as the workload token's subject; there is no end-user delegation. */
export type EmbedCallOptions = Pick<CallOptions, "signal">;

export interface Collection {
  id: string;
  model: string;
  dimensions: number;
}

export interface Sync {
  id: string;
  state: "queued" | "running" | "succeeded" | "failed";
  prefix: string;
  attempts: number;
  result?: {
    ingested: number;
    refreshed: number;
    removed: number;
    failed: number;
  };
  errorCode?: string;
}

export interface SearchHit {
  chunkId: string;
  key: string;
  revision: string;
  start: number;
  end: number;
  text: string;
  score: number;
}

/** The bae6-context embed API: tenant-scoped sync and search over configured collections. */
export class EmbedClient {
  readonly #http: Transport;

  constructor(options: ContextOptions) {
    this.#http = new Transport(options);
  }

  /** Provisions the configured collection's storage; a no-op when it exists. */
  async createCollection(
    collectionId: string,
    options: EmbedCallOptions = {},
  ): Promise<Collection> {
    const body = await this.#http.request(
      "PUT",
      collection(collectionId),
      signal(options),
    );
    if (
      !isObject(body) ||
      typeof body.id !== "string" ||
      typeof body.model !== "string" ||
      !Number.isInteger(body.dimensions)
    ) {
      throw invalid();
    }
    return body as unknown as Collection;
  }

  /** Not retried: each call queues a sync. `prefix` also matches keys that extend it. */
  async startSync(
    collectionId: string,
    request: { prefix?: string } = {},
    options: EmbedCallOptions = {},
  ): Promise<Sync> {
    return this.#sync(
      await this.#http.request("POST", `${collection(collectionId)}/syncs`, {
        body: request,
        ...signal(options),
      }),
    );
  }

  async getSync(
    collectionId: string,
    syncId: string,
    options: EmbedCallOptions = {},
  ): Promise<Sync> {
    return this.#sync(
      await this.#http.request(
        "GET",
        `${collection(collectionId)}/syncs/${segment(syncId)}`,
        signal(options),
      ),
    );
  }

  /** Ranked for the workload token's subject: only chunks that subject may read are returned. */
  async search(
    collectionId: string,
    request: { query: string; limit?: number },
    options: EmbedCallOptions = {},
  ): Promise<SearchHit[]> {
    const body = await this.#http.request(
      "POST",
      `${collection(collectionId)}/search`,
      { body: request, ...signal(options) },
    );
    if (!isObject(body) || !Array.isArray(body.hits)) throw invalid();
    if (!body.hits.every((h) => isObject(h) && typeof h.chunkId === "string")) {
      throw invalid();
    }
    return body.hits as SearchHit[];
  }

  /** Removes the key from the index only; the source object is untouched. */
  async deleteDocument(
    collectionId: string,
    key: string,
    options: EmbedCallOptions = {},
  ): Promise<void> {
    await this.#http.raw(
      "DELETE",
      `${collection(collectionId)}/documents?${new URLSearchParams({ key })}`,
      signal(options),
    );
  }

  #sync(body: unknown): Sync {
    if (
      !isObject(body) ||
      typeof body.id !== "string" ||
      !SYNC_STATES.includes(body.state as string)
    ) {
      throw invalid();
    }
    return body as unknown as Sync;
  }
}

export interface McpCapability {
  name: string;
  description?: string;
}

export interface McpVersionInput {
  version: string;
  endpoint: string;
  protocolVersion: string;
  transport: "streamable-http";
  authentication: {
    method: "oauth2" | "none";
    resource?: string;
    requiredScopes?: string[];
  };
  tools?: McpCapability[];
  resources?: McpCapability[];
  prompts?: McpCapability[];
  allowedNetworkZones?: string[];
}

export interface McpVersion extends McpVersionInput {
  state: "pending" | "approved" | "revoked";
  scanStatus: "unscanned" | "clean";
  publishedBy: string;
  publishedVia: string;
  publishedAt: string;
  decidedBy?: string;
  decidedAt?: string;
}

export interface McpServer {
  id: string;
  organization: string;
  team: string;
  name: string;
  description?: string;
  createdAt: string;
  versions: McpVersion[];
}

export interface McpServerHealth {
  version: string;
  status: "healthy" | "unhealthy";
  checkedAt: string;
  reason?: string;
}

const server = (id: string) => `/v1/servers/${segment(id)}`;

function mcpServer(body: unknown): McpServer {
  if (
    !isObject(body) ||
    typeof body.id !== "string" ||
    !Array.isArray(body.versions)
  ) {
    throw invalid();
  }
  return body as unknown as McpServer;
}

/**
 * The bae6-context MCP registry: a tenant-scoped catalog of MCP servers.
 * Mutations are POSTs and are never retried.
 */
export class RegistryClient {
  readonly #http: Transport;

  constructor(options: ContextOptions) {
    this.#http = new Transport(options);
  }

  /** Registers a server with its first version, which starts `pending`. Needs `mcp:publish`. */
  async createServer(
    request: {
      id: string;
      team: string;
      name: string;
      description?: string;
      version: McpVersionInput;
    },
    options: CallOptions = {},
  ): Promise<McpServer> {
    return mcpServer(
      await this.#http.request("POST", "/v1/servers", {
        body: request,
        headers: headers(options),
        ...signal(options),
      }),
    );
  }

  async listServers(
    page: { cursor?: string; limit?: number } = {},
    options: CallOptions = {},
  ): Promise<{ items: McpServer[]; nextCursor?: string }> {
    const q = new URLSearchParams();
    if (page.cursor !== undefined) q.set("cursor", page.cursor);
    if (page.limit !== undefined) q.set("limit", String(page.limit));
    const body = await this.#http.request("GET", `/v1/servers?${q}`, {
      headers: headers(options),
      ...signal(options),
    });
    if (!isObject(body) || !Array.isArray(body.items)) throw invalid();
    body.items.forEach(mcpServer);
    return body as { items: McpServer[]; nextCursor?: string };
  }

  async getServer(
    serverId: string,
    options: CallOptions = {},
  ): Promise<McpServer> {
    return mcpServer(
      await this.#http.request("GET", server(serverId), {
        headers: headers(options),
        ...signal(options),
      }),
    );
  }

  /** Publishes a new `pending` version. Needs `mcp:publish`. */
  async createVersion(
    serverId: string,
    version: McpVersionInput,
    options: CallOptions = {},
  ): Promise<McpServer> {
    return mcpServer(
      await this.#http.request("POST", `${server(serverId)}/versions`, {
        body: version,
        headers: headers(options),
        ...signal(options),
      }),
    );
  }

  /** The approver cannot be the version's publisher. Needs `mcp:approve`. */
  async approveVersion(
    serverId: string,
    request: { version: string; scanStatus?: "unscanned" | "clean" },
    options: CallOptions = {},
  ): Promise<McpServer> {
    return mcpServer(
      await this.#http.request("POST", `${server(serverId)}/approve`, {
        body: request,
        headers: headers(options),
        ...signal(options),
      }),
    );
  }

  /** One version, or every version when `version` is absent. Irreversible. Needs `mcp:approve`. */
  async revokeVersion(
    serverId: string,
    request: { version?: string } = {},
    options: CallOptions = {},
  ): Promise<McpServer> {
    return mcpServer(
      await this.#http.request("POST", `${server(serverId)}/revoke`, {
        body: request,
        headers: headers(options),
        ...signal(options),
      }),
    );
  }

  /** Probes an approved version, the latest when `version` is absent. Needs `mcp:discover`. */
  async getServerHealth(
    serverId: string,
    request: { version?: string } = {},
    options: CallOptions = {},
  ): Promise<McpServerHealth> {
    const q = new URLSearchParams();
    if (request.version !== undefined) q.set("version", request.version);
    const body = await this.#http.request(
      "GET",
      `${server(serverId)}/health?${q}`,
      { headers: headers(options), ...signal(options) },
    );
    if (
      !isObject(body) ||
      typeof body.version !== "string" ||
      (body.status !== "healthy" && body.status !== "unhealthy")
    ) {
      throw invalid();
    }
    return body as unknown as McpServerHealth;
  }
}

const signal = (o: CallOptions) => (o.signal ? { signal: o.signal } : {});

function target(key: string, versionId: string | undefined) {
  const q = new URLSearchParams({ key });
  if (versionId !== undefined) q.set("versionId", versionId);
  return q;
}

import { randomUUID } from "node:crypto";
import {
  createServer,
  type IncomingMessage,
  type ServerResponse,
} from "node:http";
import {
  checkPrincipals,
  type EmbeddingModel,
  EmbedError,
  type EmbedErrorCode,
  type Job,
  type JobQueue,
  type Scope,
  type VectorIndex,
} from "@bae6/context-embed";
import { COLLECTION_ID } from "./config.js";
import { envelope, HttpError, readJson, send } from "./http.js";
import { type Identity, identify, type Verifier } from "./identity.js";

export interface BoundCollection {
  id: string;
  organization: string;
  sourcePrefix: string;
  model: EmbeddingModel;
}

export interface EmbedApiDeps {
  verify: Verifier;
  collections: readonly BoundCollection[];
  index: VectorIndex & {
    ensureCollection(
      scope: Scope,
      modelId: string,
      dimensions: number,
    ): Promise<void>;
  };
  queue: Pick<JobQueue, "enqueue" | "get">;
  /** The caller's principal keys, from the verified identity and never the request (ADR-0005). */
  principals(who: Identity): string[];
  /** Rejects when the database cannot be reached. */
  ping(): Promise<void>;
  /** Documents whose access rules were confirmed longer ago than this are not returned. */
  maxAclAgeMs: number;
  maxInflight: number;
  /** Below maxInflight, so one organization cannot take a whole replica. */
  maxInflightPerOrganization: number;
  requestTimeoutMs: number;
  log?: (event: Record<string, unknown>) => void;
}

const MAX_BODY_BYTES = 64 * 1024;
const BODY_DEADLINE_MS = 10_000;
const WRITE_DEADLINE_MS = 30_000;
// Bounds bodies being read before admission: MAX_CONNECTIONS x MAX_BODY_BYTES.
const MAX_CONNECTIONS = 256;
const REQUEST_ID = /^[A-Za-z0-9._-]{1,64}$/;
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const ROUTE =
  /^\/v1\/collections\/([^/]{1,64})(?:\/(syncs|search|documents)(?:\/([^/]{1,64}))?)?$/;
const MAX_QUERY_CHARS = 8000;
const MAX_KEY_CHARS = 1024;
const MAX_LIMIT = 50;
const DEFAULT_LIMIT = 10;
const PING_TIMEOUT_MS = 2000;

type Operation =
  | "createCollection"
  | "startSync"
  | "getSync"
  | "search"
  | "deleteDocument";

const OPERATION: Record<string, Operation | undefined> = {
  "PUT:": "createCollection",
  "POST:syncs": "startSync",
  "GET:syncs": "getSync",
  "POST:search": "search",
  "DELETE:documents": "deleteDocument",
};
const SCOPE: Record<Operation, string> = {
  createCollection: "embed:write",
  startSync: "embed:write",
  getSync: "embed:read",
  search: "embed:read",
  deleteDocument: "embed:write",
};

const STATUS: Record<EmbedErrorCode, number> = {
  invalid_request: 400,
  too_large: 400,
  not_found: 404,
  model_error: 503,
  // Only the caller's own disconnect cancels, so nobody reads this answer.
  cancelled: 499,
};

const notFound = () => new HttpError(404, "not_found", "Not found.");

const header = (req: IncomingMessage, name: string) => {
  const v = req.headers[name];
  return typeof v === "string" ? v : undefined;
};

function toHttp(err: EmbedError) {
  return new HttpError(
    STATUS[err.code],
    err.code === "model_error" ? "unavailable" : err.code,
    err.code === "model_error"
      ? "The embedding model is unavailable."
      : err.message,
    err.code === "model_error" ? 1 : undefined,
  );
}

async function call<T>(work: () => Promise<T>): Promise<T> {
  try {
    return await work();
  } catch (err) {
    throw err instanceof EmbedError ? toHttp(err) : err;
  }
}

const toSync = (job: Job) => ({
  id: job.id,
  state: job.state,
  prefix: job.prefix,
  attempts: job.attempts,
  ...(job.result ? { result: job.result } : {}),
  ...(job.errorCode ? { errorCode: job.errorCode } : {}),
});

async function bodyOf(
  req: IncomingMessage,
  keys: string[],
  optional = false,
): Promise<Record<string, unknown>> {
  if (
    optional &&
    req.headers["transfer-encoding"] === undefined &&
    (req.headers["content-length"] ?? "0") === "0"
  ) {
    return {};
  }
  const body = await readJson(req, MAX_BODY_BYTES, BODY_DEADLINE_MS);
  if (typeof body !== "object" || body === null || Array.isArray(body)) {
    throw new HttpError(400, "invalid_request", "Body must be an object.");
  }
  const unknown = Object.keys(body).find((k) => !keys.includes(k));
  if (unknown !== undefined) {
    throw new HttpError(400, "invalid_request", `Unknown field ${unknown}.`);
  }
  return body as Record<string, unknown>;
}

export function createEmbedApi(deps: EmbedApiDeps) {
  const { log = () => {} } = deps;
  const byOrg = new Map<string, Map<string, BoundCollection>>();
  for (const c of deps.collections) {
    const org = byOrg.get(c.organization) ?? new Map<string, BoundCollection>();
    org.set(c.id, c);
    byOrg.set(c.organization, org);
  }
  const perOrg = new Map<string, number>();
  let inflight = 0;
  let draining = false;

  /** Takes a slot until the response is flushed or dropped. */
  function admit(res: ServerResponse, org: string) {
    // A response that closed during an earlier await never emits `close` again.
    if (res.closed) {
      throw new HttpError(499, "cancelled", "The client went away.");
    }
    const orgUsed = perOrg.get(org) ?? 0;
    if (
      inflight >= deps.maxInflight ||
      orgUsed >= deps.maxInflightPerOrganization
    ) {
      throw new HttpError(
        429,
        "rate_limited",
        "Too many concurrent requests.",
        1,
      );
    }
    inflight++;
    perOrg.set(org, orgUsed + 1);
    res.once("close", () => {
      inflight--;
      perOrg.set(org, (perOrg.get(org) ?? 1) - 1);
    });
  }

  async function ready() {
    let timer: NodeJS.Timeout | undefined;
    try {
      await Promise.race([
        deps.ping(),
        new Promise<never>((_, bad) => {
          timer = setTimeout(() => bad(new Error("timeout")), PING_TIMEOUT_MS);
        }),
      ]);
    } catch {
      throw new HttpError(503, "unavailable", "Not ready.");
    } finally {
      clearTimeout(timer);
    }
  }

  async function route(
    req: IncomingMessage,
    res: ServerResponse,
    url: URL,
    access: Record<string, unknown>,
  ) {
    const { pathname: path } = url;
    if (req.method === "GET" && path === "/health/live") {
      return send(res, 200, { status: "ok" });
    }
    if (req.method === "GET" && path === "/health/ready") {
      if (draining) throw new HttpError(503, "unavailable", "Not ready.");
      await ready();
      return send(res, 200, { status: "ok" });
    }
    if (!path.startsWith("/v1/")) throw notFound();

    let who: (Identity & { workloadId?: string }) | null;
    try {
      who = await identify(
        deps.verify,
        req.headers.authorization,
        header(req, "bae6-end-user-token"),
      );
    } catch (err) {
      log({ msg: "identity_unavailable", error: (err as Error).name });
      throw new HttpError(
        503,
        "unavailable",
        "Identity provider unavailable.",
        5,
      );
    }
    if (!who) {
      throw new HttpError(
        401,
        "unauthenticated",
        "Missing or invalid credentials.",
      );
    }
    Object.assign(access, {
      organizationId: who.organizationId,
      applicationId: who.applicationId,
      subjectId: who.subjectId,
      ...(who.workloadId ? { workloadId: who.workloadId } : {}),
    });

    const m = ROUTE.exec(path);
    const op = m ? OPERATION[`${req.method}:${m[2] ?? ""}`] : undefined;
    if (!m || !op || (op === "getSync") !== (m[3] !== undefined)) {
      throw notFound();
    }
    if (!who.scopes.includes(SCOPE[op])) {
      throw new HttpError(403, "forbidden", `Missing scope ${SCOPE[op]}.`);
    }
    let id: string;
    try {
      id = decodeURIComponent(m[1] as string);
    } catch {
      throw notFound();
    }
    // Another organization's collection is indistinguishable from a missing one (ADR-0002).
    const bound = COLLECTION_ID.test(id)
      ? byOrg.get(who.organizationId)?.get(id)
      : undefined;
    if (!bound) throw notFound();
    Object.assign(access, { collectionId: bound.id, operation: op });
    const scope: Scope = {
      organizationId: who.organizationId,
      collectionId: bound.id,
    };

    // A slow body must not hold a slot, so it is read before one is taken.
    let body: Record<string, unknown> = {};
    if (op === "startSync") body = await bodyOf(req, ["prefix"], true);
    if (op === "search") body = await bodyOf(req, ["query", "limit"]);
    admit(res, who.organizationId);
    const closed = new AbortController();
    // `req` closes once its body is read; only `res` closes on a client disconnect.
    res.once("close", () => {
      if (!res.writableEnded) closed.abort();
    });

    if (op === "createCollection") {
      try {
        await deps.index.ensureCollection(
          scope,
          bound.model.id,
          bound.model.dimensions,
        );
      } catch (err) {
        if (err instanceof EmbedError && err.code === "invalid_request") {
          throw new HttpError(409, "conflict", err.message);
        }
        throw err instanceof EmbedError ? toHttp(err) : err;
      }
      return send(res, 200, {
        id: bound.id,
        model: bound.model.id,
        dimensions: bound.model.dimensions,
      });
    }

    if (op === "startSync") {
      const { prefix = bound.sourcePrefix } = body;
      if (typeof prefix !== "string" || Buffer.byteLength(prefix) > 1024) {
        throw new HttpError(
          400,
          "invalid_request",
          "prefix must be a string of at most 1024 bytes.",
        );
      }
      if (!prefix.startsWith(bound.sourcePrefix)) {
        throw new HttpError(
          400,
          "invalid_request",
          "prefix is outside this collection's source.",
        );
      }
      try {
        return send(res, 202, toSync(await deps.queue.enqueue(scope, prefix)));
      } catch (err) {
        if (err instanceof EmbedError && err.code === "too_large") {
          throw new HttpError(
            429,
            "rate_limited",
            "Too many syncs are waiting.",
            30,
          );
        }
        throw err instanceof EmbedError ? toHttp(err) : err;
      }
    }

    if (op === "getSync") {
      const syncId = m[3] as string;
      const job = UUID.test(syncId)
        ? await deps.queue.get(scope, syncId)
        : undefined;
      if (!job) throw notFound();
      return send(res, 200, toSync(job));
    }

    if (op === "deleteDocument") {
      const keys = url.searchParams.getAll("key");
      const key = keys[0];
      if (
        keys.length !== 1 ||
        key === undefined ||
        key === "" ||
        key.length > MAX_KEY_CHARS
      ) {
        throw new HttpError(
          400,
          "invalid_request",
          `key must be given once, 1 to ${MAX_KEY_CHARS} characters.`,
        );
      }
      await call(() => deps.index.remove(scope, key, closed.signal));
      res.writeHead(204);
      return void res.end();
    }

    const { query, limit = DEFAULT_LIMIT } = body;
    if (
      typeof query !== "string" ||
      query === "" ||
      query.length > MAX_QUERY_CHARS ||
      !Number.isInteger(limit) ||
      (limit as number) < 1 ||
      (limit as number) > MAX_LIMIT
    ) {
      throw new HttpError(
        400,
        "invalid_request",
        `query must be 1 to ${MAX_QUERY_CHARS} characters and limit 1 to ${MAX_LIMIT}.`,
      );
    }
    let principals: string[];
    try {
      principals = checkPrincipals(deps.principals(who));
    } catch {
      principals = [];
    }
    if (principals.length === 0) {
      throw new HttpError(403, "forbidden", "No principal to search as.");
    }
    const [vector] = await call(() =>
      bound.model.embed([query], closed.signal),
    );
    if (
      !vector ||
      vector.length !== bound.model.dimensions ||
      !vector.every(Number.isFinite)
    ) {
      throw toHttp(new EmbedError("model_error", "unusable vector"));
    }
    // The caller's input was already validated, so a rejection here is a
    // mismatch between the configured model and the provisioned collection.
    const hits = await call(() =>
      deps.index
        .search(
          scope,
          {
            vector,
            limit: limit as number,
            principals,
            maxAclAgeMs: deps.maxAclAgeMs,
          },
          closed.signal,
        )
        .catch((err) => {
          throw err instanceof EmbedError && err.code === "invalid_request"
            ? new EmbedError("model_error", err.message)
            : err;
        }),
    );
    send(res, 200, { hits });
  }

  const server = createServer(async (req, res) => {
    const supplied = req.headers["x-request-id"];
    const requestId =
      typeof supplied === "string" && REQUEST_ID.test(supplied)
        ? supplied
        : `req_${randomUUID()}`;
    res.setHeader("x-request-id", requestId);
    const started = performance.now();
    const access: Record<string, unknown> = {};
    let url: URL;
    try {
      url = new URL(req.url ?? "/", "http://embed-api");
    } catch {
      url = new URL("http://embed-api/");
    }
    // `close`, not `finish`: a request the client dropped is still logged.
    res.once("close", () =>
      log({
        msg: "request",
        requestId,
        method: req.method,
        path: url.pathname,
        status: res.statusCode,
        durationMs: Math.round(performance.now() - started),
        ...access,
      }),
    );
    try {
      await route(req, res, url, access);
    } catch (err) {
      let e: HttpError;
      if (err instanceof HttpError) {
        e = err;
      } else {
        log({ msg: "internal_error", requestId, error: (err as Error).name });
        e = new HttpError(500, "internal", "Internal error.");
      }
      if (e.retryAfterSeconds !== undefined) {
        res.setHeader("retry-after", String(e.retryAfterSeconds));
      }
      if (!res.headersSent) send(res, e.status, envelope(e, requestId));
      else res.end();
    }
    if (!res.writableFinished && !res.closed) {
      const t = setTimeout(() => res.destroy(), WRITE_DEADLINE_MS).unref();
      res.once("close", () => clearTimeout(t));
    }
  });
  server.headersTimeout = 10_000;
  server.maxConnections = MAX_CONNECTIONS;
  server.requestTimeout = deps.requestTimeoutMs;

  return Object.assign(server, {
    /** Fail readiness and close idle sockets; in-flight requests finish. */
    drain() {
      draining = true;
      server.closeIdleConnections();
    },
  });
}

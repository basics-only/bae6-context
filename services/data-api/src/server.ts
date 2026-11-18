import { randomUUID } from "node:crypto";
import {
  createServer,
  type IncomingMessage,
  type ServerResponse,
} from "node:http";
import {
  DataError,
  type DataErrorCode,
  type ObjectSource,
  type QuerySource,
} from "@bae6/context-data";
import { SOURCE_ID } from "./config.js";
import { envelope, HttpError, readJson, send } from "./http.js";
import { type Identity, identify, type Verifier } from "./identity.js";

export type Source = QuerySource | ObjectSource;

export interface BoundSource {
  id: string;
  organization: string;
  maxConcurrent: number;
  open: () => Promise<Source>;
}

export interface DataApiDeps {
  verify: Verifier;
  sources: readonly BoundSource[];
  maxInflight: number;
  /** Below maxInflight, so one organization cannot take a whole replica. */
  maxInflightPerOrganization: number;
  /** Longest a client may take to send a request; source calls bound themselves. */
  requestTimeoutMs: number;
  log?: (event: Record<string, unknown>) => void;
}

const MAX_BODY_BYTES = 1024 * 1024;
const BODY_DEADLINE_MS = 10_000;
// A client that stops reading must not keep a buffered response in memory.
const WRITE_DEADLINE_MS = 30_000;
// Bounds bodies being read before admission: MAX_CONNECTIONS x MAX_BODY_BYTES.
const MAX_CONNECTIONS = 256;
const TOKEN = /^[A-Za-z0-9._~+/-]{1,8192}=*$/;
const REQUEST_ID = /^[A-Za-z0-9._-]{1,64}$/;
const RANGE = /^bytes=(\d{1,15})-(\d{1,15})$/;
const ROUTE =
  /^\/v1\/sources\/([^/]{1,64})\/(query|objects|objects\/metadata|objects\/content)$/;
const QUERY_KEYS = new Set(["statement", "parameters", "maxRows", "timeoutMs"]);

const STATUS: Record<DataErrorCode, number> = {
  invalid_request: 400,
  too_large: 400,
  forbidden: 403,
  not_found: 404,
  // Only the caller's own disconnect cancels, so nobody reads this answer.
  cancelled: 499,
  unavailable: 503,
  timeout: 504,
};

const notFound = () => new HttpError(404, "not_found", "Not found.");

function header(req: IncomingMessage, name: string): string | undefined {
  const v = req.headers[name];
  return typeof v === "string" ? v : undefined;
}

function param(url: URL, name: string, required: true): string;
function param(url: URL, name: string, required?: false): string | undefined;
function param(url: URL, name: string, required = false) {
  const all = url.searchParams.getAll(name);
  if (all.length > 1 || (required && all.length === 0)) {
    throw new HttpError(
      400,
      "invalid_request",
      `${name} must be given ${required ? "once" : "at most once"}.`,
    );
  }
  return all[0];
}

export function createDataApi(deps: DataApiDeps) {
  const { log = () => {} } = deps;
  const byOrg = new Map<string, Map<string, BoundSource>>();
  for (const s of deps.sources) {
    const org = byOrg.get(s.organization) ?? new Map<string, BoundSource>();
    org.set(s.id, s);
    byOrg.set(s.organization, org);
  }
  const busy = new Map<BoundSource, number>();
  const perOrg = new Map<string, number>();
  let inflight = 0;
  let draining = false;

  async function open(bound: BoundSource): Promise<Source> {
    try {
      return await bound.open();
    } catch (err) {
      log({
        msg: "source_unavailable",
        sourceId: bound.id,
        error: (err as Error).name,
      });
      throw new HttpError(503, "unavailable", "The source is unavailable.", 1);
    }
  }

  async function call<T>(work: () => Promise<T>): Promise<T> {
    try {
      return await work();
    } catch (err) {
      if (!(err instanceof DataError)) throw err;
      throw new HttpError(
        STATUS[err.code],
        err.code,
        err.message,
        err.code === "unavailable" ? 1 : undefined,
      );
    }
  }

  async function queryBody(req: IncomingMessage) {
    const body = await readJson(req, MAX_BODY_BYTES, BODY_DEADLINE_MS);
    if (typeof body !== "object" || body === null || Array.isArray(body)) {
      throw new HttpError(400, "invalid_request", "Body must be an object.");
    }
    const unknown = Object.keys(body).find((k) => !QUERY_KEYS.has(k));
    if (unknown !== undefined) {
      throw new HttpError(400, "invalid_request", `Unknown field ${unknown}.`);
    }
    return body as { statement: string };
  }

  async function query(
    req: IncomingMessage,
    res: ServerResponse,
    bound: BoundSource,
    body: { statement: string },
    signal: AbortSignal,
  ) {
    const sourceToken = header(req, "bae6-source-token");
    // Node joins repeated headers with ", ", which no token contains.
    if (sourceToken !== undefined && !TOKEN.test(sourceToken)) {
      throw new HttpError(
        400,
        "invalid_request",
        "Bae6-Source-Token is malformed.",
      );
    }
    const source = await open(bound);
    if (!("query" in source)) throw unsupported(bound, "query");
    const result = await call(() =>
      source.query({
        ...body,
        signal,
        ...(sourceToken === undefined ? {} : { delegatedToken: sourceToken }),
      }),
    );
    send(res, 200, result);
  }

  async function objects(
    res: ServerResponse,
    url: URL,
    bound: BoundSource,
    op: string,
    range: string | undefined,
    signal: AbortSignal,
  ) {
    const source = await open(bound);
    if (!("list" in source)) throw unsupported(bound, op);
    if (op === "objects") {
      const limit = param(url, "limit") ?? "50";
      if (!/^\d{1,3}$/.test(limit) || +limit < 1 || +limit > 100) {
        throw new HttpError(400, "invalid_request", "limit must be 1 to 100.");
      }
      const prefix = param(url, "prefix");
      const cursor = param(url, "cursor");
      return send(
        res,
        200,
        await call(() =>
          source.list({
            limit: +limit,
            signal,
            ...(prefix === undefined ? {} : { prefix }),
            ...(cursor === undefined ? {} : { cursor }),
          }),
        ),
      );
    }
    const key = param(url, "key", true);
    const versionId = param(url, "versionId");
    const target = {
      key,
      signal,
      ...(versionId === undefined ? {} : { versionId }),
    };
    if (op === "objects/metadata") {
      return send(res, 200, await call(() => source.stat(target)));
    }
    let span: { start: number; end: number } | undefined;
    if (range !== undefined) {
      const m = RANGE.exec(range);
      if (!m) {
        throw new HttpError(
          400,
          "invalid_request",
          "Range must be one bytes=<start>-<end>.",
        );
      }
      span = { start: Number(m[1]), end: Number(m[2]) };
    }
    const { info, body } = await call(() =>
      source.read({ ...target, ...(span ? { range: span } : {}) }),
    );
    res.writeHead(span ? 206 : 200, {
      "content-type": "application/octet-stream",
      "x-content-type-options": "nosniff",
      "content-length": body.byteLength,
      etag: `"${info.etag}"`,
      "last-modified": new Date(info.lastModified).toUTCString(),
      ...(info.versionId ? { "bae6-version-id": info.versionId } : {}),
      ...(span
        ? {
            "content-range": `bytes ${span.start}-${span.start + body.byteLength - 1}/${info.size}`,
          }
        : {}),
    });
    res.end(body);
  }

  const unsupported = (bound: BoundSource, op: string) =>
    new HttpError(
      400,
      "unsupported_operation",
      `Source ${bound.id} does not support ${op === "query" ? "queries" : "objects"}.`,
    );

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
    if (!who.scopes.includes("data:read")) {
      throw new HttpError(403, "forbidden", "Missing scope data:read.");
    }
    const org = byOrg.get(who.organizationId);

    if (req.method === "GET" && path === "/v1/sources") {
      admit(res, who.organizationId);
      const bounds = [...(org?.values() ?? [])];
      const opened = await Promise.allSettled(bounds.map(open));
      return send(res, 200, {
        items: opened.flatMap((o) =>
          o.status === "fulfilled" ? [o.value.capabilities()] : [],
        ),
        unavailable: bounds
          .filter((_, i) => opened[i]?.status === "rejected")
          .map((b) => b.id),
      });
    }

    const m = ROUTE.exec(path);
    const op = m?.[2];
    if (!m || !op || req.method !== (op === "query" ? "POST" : "GET")) {
      throw notFound();
    }
    let id: string;
    try {
      id = decodeURIComponent(m[1] as string);
    } catch {
      throw notFound();
    }
    // Another organization's source is indistinguishable from a missing one (ADR-0002).
    const bound = SOURCE_ID.test(id) ? org?.get(id) : undefined;
    if (!bound) throw notFound();
    Object.assign(access, { sourceId: bound.id, operation: op });

    // A slow body must not hold a slot, so it is read before one is taken.
    const body = op === "query" ? await queryBody(req) : undefined;
    admit(res, who.organizationId, bound);
    const closed = new AbortController();
    // `req` closes once its body is read; only `res` closes on a client disconnect.
    res.once("close", () => {
      if (!res.writableEnded) closed.abort();
    });
    await (body
      ? query(req, res, bound, body, closed.signal)
      : objects(res, url, bound, op, header(req, "range"), closed.signal));
  }

  /** Takes a slot until the response is flushed or dropped, so buffered bodies count too. */
  function admit(res: ServerResponse, org: string, bound?: BoundSource) {
    // A response that closed during an earlier await never emits `close` again.
    if (res.closed) {
      throw new HttpError(499, "cancelled", "The client went away.");
    }
    const used = bound ? (busy.get(bound) ?? 0) : 0;
    const orgUsed = perOrg.get(org) ?? 0;
    if (
      inflight >= deps.maxInflight ||
      orgUsed >= deps.maxInflightPerOrganization ||
      (bound && used >= bound.maxConcurrent)
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
    if (bound) busy.set(bound, used + 1);
    res.once("close", () => {
      inflight--;
      perOrg.set(org, (perOrg.get(org) ?? 1) - 1);
      if (bound) busy.set(bound, (busy.get(bound) ?? 1) - 1);
    });
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
      url = new URL(req.url ?? "/", "http://data-api");
    } catch {
      url = new URL("http://data-api/");
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

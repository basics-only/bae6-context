import { randomUUID } from "node:crypto";
import {
  createServer,
  type IncomingMessage,
  type ServerResponse,
} from "node:http";
import {
  type Caller,
  type Probe,
  type Registry,
  RegistryError,
  type RegistryErrorCode,
  SERVER_ID,
  versionName,
} from "@bae6/context-mcp";
import { envelope, HttpError, readJson, send } from "./http.js";
import { type Identity, identify, type Verifier } from "./identity.js";

export interface RegistryApiDeps {
  verify: Verifier;
  registry: Registry;
  probe: Probe;
  /** Rejects when the database cannot be reached. */
  ping(): Promise<void>;
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
const ROUTE =
  /^\/v1\/servers(?:\/([^/]{1,64})(?:\/(versions|approve|revoke|health))?)?$/;
const MAX_CURSOR_CHARS = 512;
const MAX_LIMIT = 100;
const DEFAULT_LIMIT = 50;
const PING_TIMEOUT_MS = 2000;

type Operation =
  | "createServer"
  | "listServers"
  | "getServer"
  | "createVersion"
  | "approveVersion"
  | "revokeVersion"
  | "getServerHealth";

const OPERATION: Record<string, Operation | undefined> = {
  "POST::": "createServer",
  "GET::": "listServers",
  "GET:id:": "getServer",
  "POST:id:versions": "createVersion",
  "POST:id:approve": "approveVersion",
  "POST:id:revoke": "revokeVersion",
  "GET:id:health": "getServerHealth",
};
const ANY_SCOPE = ["mcp:discover", "mcp:publish", "mcp:approve"];
const SCOPE: Record<Operation, string[]> = {
  createServer: ["mcp:publish"],
  listServers: ANY_SCOPE,
  getServer: ANY_SCOPE,
  createVersion: ["mcp:publish"],
  approveVersion: ["mcp:approve"],
  revokeVersion: ["mcp:approve"],
  getServerHealth: ["mcp:discover"],
};

const STATUS: Record<RegistryErrorCode, number> = {
  invalid_request: 400,
  not_found: 404,
  conflict: 409,
  forbidden: 403,
};

const notFound = () => new HttpError(404, "not_found", "Not found.");

const header = (req: IncomingMessage, name: string) => {
  const v = req.headers[name];
  return typeof v === "string" ? v : undefined;
};

async function call<T>(work: () => Promise<T>): Promise<T> {
  try {
    return await work();
  } catch (err) {
    throw err instanceof RegistryError
      ? new HttpError(STATUS[err.code], err.code, err.message)
      : err;
  }
}

const encodeCursor = (id: string) => Buffer.from(id).toString("base64url");

function decodeCursor(cursor: string): string {
  const id = Buffer.from(cursor, "base64url").toString("utf8");
  if (
    cursor.length > MAX_CURSOR_CHARS ||
    !SERVER_ID.test(id) ||
    encodeCursor(id) !== cursor
  ) {
    throw new HttpError(400, "invalid_request", "cursor is not valid.");
  }
  return id;
}

function limitOf(url: URL): number {
  const raw = url.searchParams.get("limit");
  if (raw === null) return DEFAULT_LIMIT;
  const n = Number(raw);
  if (!/^[0-9]{1,3}$/.test(raw) || n < 1 || n > MAX_LIMIT) {
    throw new HttpError(
      400,
      "invalid_request",
      `limit must be an integer from 1 to ${MAX_LIMIT}.`,
    );
  }
  return n;
}

async function bodyOf(req: IncomingMessage, optional = false) {
  if (
    optional &&
    req.headers["transfer-encoding"] === undefined &&
    (req.headers["content-length"] ?? "0") === "0"
  ) {
    return {};
  }
  return readJson(req, MAX_BODY_BYTES, BODY_DEADLINE_MS);
}

function callerOf(who: Identity & { workloadId?: string }, requestId: string) {
  const workload = who.workloadId ?? who.subjectId;
  const caller: Caller = {
    subject: who.subjectId,
    workload,
    actor: who.workloadId ? `${workload} for ${who.subjectId}` : workload,
    requestId,
  };
  return caller;
}

export function createRegistryApi(deps: RegistryApiDeps) {
  const { log = () => {}, registry } = deps;
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
    requestId: string,
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
    const op = m
      ? OPERATION[
          `${req.method}:${m[1] === undefined ? "" : "id"}:${m[2] ?? ""}`
        ]
      : undefined;
    if (!m || !op) throw notFound();
    const needed = SCOPE[op];
    if (!needed.some((s) => who.scopes.includes(s))) {
      throw new HttpError(
        403,
        "forbidden",
        `Missing scope ${needed.join(" or ")}.`,
      );
    }
    const org = who.organizationId;
    const manage =
      who.scopes.includes("mcp:publish") || who.scopes.includes("mcp:approve");
    const caller = callerOf(who, requestId);
    let id = "";
    if (m[1] !== undefined) {
      try {
        id = decodeURIComponent(m[1]);
      } catch {
        throw notFound();
      }
      if (!SERVER_ID.test(id)) throw notFound();
    }
    Object.assign(access, { operation: op, ...(id ? { serverId: id } : {}) });

    // A slow body must not hold a slot, so it is read before one is taken.
    let body: unknown;
    if (op === "createServer" || op === "createVersion")
      body = await bodyOf(req);
    if (op === "approveVersion") body = await bodyOf(req);
    if (op === "revokeVersion") body = await bodyOf(req, true);
    admit(res, org);

    switch (op) {
      case "createServer":
        return send(
          res,
          201,
          await call(() => registry.createServer(org, body, caller)),
        );
      case "createVersion":
        return send(
          res,
          201,
          await call(() => registry.addVersion(org, id, body, caller)),
        );
      case "approveVersion":
        return send(
          res,
          200,
          await call(() => registry.approve(org, id, body, caller)),
        );
      case "revokeVersion":
        return send(
          res,
          200,
          await call(() => registry.revoke(org, id, body, caller)),
        );
      case "getServer":
        return send(res, 200, await call(() => registry.get(org, id, manage)));
      case "listServers": {
        const cursor = url.searchParams.get("cursor");
        const page = await call(() =>
          registry.list(
            org,
            {
              limit: limitOf(url),
              ...(cursor === null ? {} : { after: decodeCursor(cursor) }),
            },
            manage,
          ),
        );
        return send(res, 200, {
          items: page.items,
          ...(page.next === undefined
            ? {}
            : { nextCursor: encodeCursor(page.next) }),
        });
      }
      case "getServerHealth": {
        const requested = url.searchParams.get("version") ?? undefined;
        return send(
          res,
          200,
          await call(() => {
            if (requested !== undefined) versionName(requested);
            return registry.health(org, id, requested, deps.probe);
          }),
        );
      }
    }
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
      url = new URL(req.url ?? "/", "http://mcp-registry");
    } catch {
      url = new URL("http://mcp-registry/");
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
      await route(req, res, url, requestId, access);
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

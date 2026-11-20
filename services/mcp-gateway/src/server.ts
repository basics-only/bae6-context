import { randomUUID } from "node:crypto";
import {
  createServer,
  type IncomingMessage,
  type ServerResponse,
} from "node:http";
import { SERVER_ID, VERSION_NAME, type Version } from "@bae6/context-mcp";
import { type Entries, RegistryUnavailable } from "./entries.js";
import { envelope, HttpError, readJson, send } from "./http.js";
import { type Identity, identify, type Verifier } from "./identity.js";
import { type ToolCall, UpstreamError } from "./upstream.js";

export interface GatewayDeps {
  verify: Verifier;
  entries: Entries;
  upstream(call: ToolCall): Promise<unknown>;
  maxInflight: number;
  /** Below maxInflight, so one organization cannot take a whole replica. */
  maxInflightPerOrganization: number;
  requestTimeoutMs: number;
  log?: (event: Record<string, unknown>) => void;
}

const MAX_BODY_BYTES = 64 * 1024;
const BODY_DEADLINE_MS = 10_000;
const WRITE_DEADLINE_MS = 30_000;
const MAX_CONNECTIONS = 256;
const REQUEST_ID = /^[A-Za-z0-9._-]{1,64}$/;
const ROUTE = /^\/v1\/servers\/([^/]{1,64})\/tools\/call$/;
const SCOPE = "mcp:invoke";

const notFound = () => new HttpError(404, "not_found", "Not found.");
const invalid = (message: string) =>
  new HttpError(400, "invalid_request", message);

const header = (req: IncomingMessage, name: string) => {
  const v = req.headers[name];
  return typeof v === "string" ? v : undefined;
};

const isRecord = (v: unknown): v is Record<string, unknown> =>
  typeof v === "object" && v !== null && !Array.isArray(v);

function parseCall(raw: unknown) {
  if (!isRecord(raw)) throw invalid("Body must be a JSON object.");
  for (const key of Object.keys(raw)) {
    if (!["tool", "arguments", "version"].includes(key)) {
      throw invalid(`Unknown field ${key}.`);
    }
  }
  const { tool, version } = raw;
  const args = raw.arguments ?? {};
  if (typeof tool !== "string" || tool.length < 1 || tool.length > 128) {
    throw invalid("tool must be a string of 1 to 128 characters.");
  }
  if (!isRecord(args)) throw invalid("arguments must be an object.");
  if (
    version !== undefined &&
    (typeof version !== "string" || !VERSION_NAME.test(version))
  ) {
    throw invalid("version is not valid.");
  }
  return { tool, args, version };
}

function usable(v: unknown): v is Version {
  const x = v as Partial<Version> | null;
  return (
    isRecord(x) &&
    typeof x.version === "string" &&
    typeof x.endpoint === "string" &&
    typeof x.protocolVersion === "string" &&
    typeof x.publishedAt === "string" &&
    isRecord(x.authentication)
  );
}

/** The pinned approved version, else the most recently published approved one. */
function select(versions: unknown[], pinned: string | undefined) {
  const approved = versions.filter(
    (v): v is Version => usable(v) && v.state === "approved",
  );
  if (pinned !== undefined) return approved.find((v) => v.version === pinned);
  return approved.reduce<Version | undefined>(
    (best, v) => (!best || v.publishedAt > best.publishedAt ? v : best),
    undefined,
  );
}

export function createGateway(deps: GatewayDeps) {
  const { log = () => {} } = deps;
  const perOrg = new Map<string, number>();
  let inflight = 0;
  let draining = false;

  function admit(res: ServerResponse, org: string) {
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
    if (!m || req.method !== "POST") throw notFound();
    if (!who.scopes.includes(SCOPE)) {
      throw new HttpError(403, "forbidden", `Missing scope ${SCOPE}.`);
    }
    let id: string;
    try {
      id = decodeURIComponent(m[1] ?? "");
    } catch {
      throw notFound();
    }
    if (!SERVER_ID.test(id)) throw notFound();
    Object.assign(access, { operation: "callTool", serverId: id });

    // A slow body must not hold a slot, so it is read before one is taken.
    const call = parseCall(
      await readJson(req, MAX_BODY_BYTES, BODY_DEADLINE_MS),
    );
    Object.assign(access, { tool: call.tool });
    const org = who.organizationId;
    admit(res, org);

    let server: Awaited<ReturnType<Entries>>;
    try {
      server = await deps.entries(org, id);
    } catch (err) {
      if (!(err instanceof RegistryUnavailable)) throw err;
      log({ msg: "registry_unavailable", requestId, reason: err.message });
      throw new HttpError(503, "unavailable", "Registry unavailable.", 5);
    }
    if (!server) throw notFound();
    const version = select(server.versions, call.version);
    if (!version) throw notFound();
    Object.assign(access, { version: version.version });
    const declared = Array.isArray(version.tools) ? version.tools : [];
    if (!declared.some((t) => t?.name === call.tool)) {
      throw new HttpError(
        404,
        "not_found",
        "The version does not declare that tool.",
      );
    }
    if (version.authentication.method !== "none") {
      throw new HttpError(
        409,
        "upstream_auth_unsupported",
        "The server needs upstream authentication, which the gateway does not support.",
      );
    }

    let result: unknown;
    try {
      result = await deps.upstream({
        endpoint: version.endpoint,
        protocolVersion: version.protocolVersion,
        tool: call.tool,
        arguments: call.args,
      });
    } catch (err) {
      if (!(err instanceof UpstreamError)) throw err;
      throw new HttpError(
        err.reason === "timeout" ? 504 : 502,
        `upstream_${err.reason}`,
        "The upstream call failed.",
      );
    }
    return send(res, 200, {
      server: id,
      version: version.version,
      tool: call.tool,
      result,
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
      url = new URL(req.url ?? "/", "http://mcp-gateway");
    } catch {
      url = new URL("http://mcp-gateway/");
    }
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
      access.outcome = e.code;
      if (e.retryAfterSeconds !== undefined) {
        res.setHeader("retry-after", String(e.retryAfterSeconds));
      }
      if (!res.headersSent) send(res, e.status, envelope(e, requestId));
      else res.end();
    }
    access.outcome ??= "ok";
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

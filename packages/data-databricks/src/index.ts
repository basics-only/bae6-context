import {
  type Access,
  abortError,
  type CapabilityDescriptor,
  checkQuery,
  DataError,
  type QueryLimits,
  type QueryRequest,
  type QueryResult,
  type QuerySource,
  rowBytes,
} from "@bae6/context-data";
import { isReadOnlyStatement } from "./guard.js";

export { isReadOnlyStatement } from "./guard.js";

/**
 * `service_principal`: the source sees this service's OAuth client, never the user.
 * `delegated`: each query carries the user's own token issued for Databricks (ADR-0003).
 */
export type DatabricksAuth =
  | { mode: "service_principal"; clientId: string; clientSecret: string }
  | { mode: "delegated" };

export interface DatabricksConfig {
  id: string;
  /** Workspace URL, e.g. https://dbc-1234.cloud.databricks.com */
  host: string;
  warehouseId: string;
  auth: DatabricksAuth;
  limits?: Partial<QueryLimits>;
  /** Tests only. */
  allowHttp?: boolean;
}

const ID = /^[a-z0-9][a-z0-9._:/-]{0,127}$/;
const WAREHOUSE = /^[A-Za-z0-9]{1,64}$/;
const PARAM = /^[A-Za-z_][A-Za-z0-9_]{0,63}$/;
const CHUNK_LINK =
  /^\/api\/2\.0\/sql\/statements\/[A-Za-z0-9-]{1,128}\/result\/chunks\/\d{1,6}(\?[A-Za-z0-9=&_.-]{0,512})?$/;
const MAX_RESPONSE_BYTES = 32 * 1024 * 1024;
const DEFAULT_LIMITS: QueryLimits = {
  maxRows: 10_000,
  maxBytes: 16 * 1024 * 1024,
  // The longest wait the Statement Execution API allows.
  maxTimeoutMs: 50_000,
  maxStatementLength: 100_000,
};

interface Statement {
  statement_id?: string;
  status?: {
    state?: string;
    error?: { error_code?: string; message?: string };
  };
  manifest?: {
    truncated?: boolean;
    schema?: {
      columns?: { name: string; type_text?: string; type_name?: string }[];
    };
  };
  result?: Chunk;
}
interface Chunk {
  data_array?: unknown[];
  next_chunk_internal_link?: string;
}

async function readJson(res: Response): Promise<unknown> {
  const reader = res.body?.getReader();
  const parts: Uint8Array[] = [];
  let size = 0;
  for (;;) {
    const r = reader ? await reader.read() : { done: true as const };
    if (r.done) break;
    size += r.value.byteLength;
    if (size > MAX_RESPONSE_BYTES) {
      await reader?.cancel();
      throw new DataError("too_large", "The source response is too large.");
    }
    parts.push(r.value);
  }
  try {
    return JSON.parse(Buffer.concat(parts).toString("utf8"));
  } catch {
    throw new DataError("unavailable", "The source sent a malformed response.");
  }
}

function failed(error: { error_code?: string; message?: string } = {}) {
  const code = error.error_code ?? "";
  if (
    code === "PERMISSION_DENIED" ||
    /INSUFFICIENT_PERMISSIONS|PERMISSION_DENIED/.test(error.message ?? "")
  ) {
    return new DataError("forbidden", "The source denied this statement.");
  }
  if (/RESOURCE_EXHAUSTED|TEMPORARILY_UNAVAILABLE|INTERNAL_ERROR/.test(code)) {
    return new DataError("unavailable", `The source failed (${code}).`);
  }
  // Messages can quote the statement or data, so only the code is passed on.
  return new DataError(
    "invalid_request",
    `The statement failed${/^[A-Z_]{1,64}$/.test(code) ? ` (${code})` : ""}.`,
  );
}

export function createDatabricksSource(cfg: DatabricksConfig): QuerySource {
  if (!ID.test(cfg.id)) throw new Error(`id must match ${ID}`);
  const host = new URL(cfg.host);
  if (
    !(
      host.protocol === "https:" ||
      (host.protocol === "http:" && cfg.allowHttp)
    ) ||
    host.username ||
    host.password ||
    host.pathname !== "/" ||
    host.search
  ) {
    throw new Error(`${cfg.id}: host must be a bare https:// workspace URL`);
  }
  if (!WAREHOUSE.test(cfg.warehouseId)) {
    throw new Error(`${cfg.id}: warehouseId must match ${WAREHOUSE}`);
  }
  const limits = { ...DEFAULT_LIMITS, ...cfg.limits };
  if (limits.maxTimeoutMs > DEFAULT_LIMITS.maxTimeoutMs) {
    throw new Error(`${cfg.id}: maxTimeoutMs is at most 50000`);
  }
  const access: Access =
    cfg.auth.mode === "delegated" ? "delegated" : "service_principal";

  async function call(
    path: string,
    bearer: string,
    signal: AbortSignal,
    callerSignal: AbortSignal | undefined,
    body?: unknown,
  ): Promise<unknown> {
    let res: Response;
    try {
      res = await fetch(new URL(path, host), {
        method: body === undefined ? "GET" : "POST",
        headers: {
          authorization: `Bearer ${bearer}`,
          ...(body === undefined ? {} : { "content-type": "application/json" }),
        },
        ...(body === undefined ? {} : { body: JSON.stringify(body) }),
        redirect: "error",
        signal,
      });
    } catch {
      throw signal.aborted
        ? abortError(callerSignal)
        : new DataError("unavailable", "The source is unreachable.");
    }
    if (res.ok) return readJson(res);
    await res.body?.cancel();
    if (res.status === 403 || (res.status === 401 && access === "delegated")) {
      throw new DataError("forbidden", "The source refused the credential.");
    }
    if (res.status === 400) {
      throw new DataError(
        "invalid_request",
        "The source rejected the request.",
      );
    }
    throw new DataError("unavailable", `The source answered ${res.status}.`);
  }

  let cached: { token: string; expires: number } | undefined;
  let pending: Promise<string> | undefined;
  async function servicePrincipalToken(
    signal: AbortSignal,
    callerSignal: AbortSignal | undefined,
  ): Promise<string> {
    if (cfg.auth.mode !== "service_principal") throw new Error("unreachable");
    if (cached && cached.expires > Date.now()) return cached.token;
    const { clientId, clientSecret } = cfg.auth;
    pending ??= (async () => {
      try {
        const res = await fetch(new URL("/oidc/v1/token", host), {
          method: "POST",
          headers: {
            authorization: `Basic ${Buffer.from(`${clientId}:${clientSecret}`).toString("base64")}`,
            "content-type": "application/x-www-form-urlencoded",
          },
          body: "grant_type=client_credentials&scope=all-apis",
          redirect: "error",
          signal: AbortSignal.timeout(10_000),
        });
        const json = (res.ok ? await readJson(res) : null) as {
          access_token?: unknown;
          expires_in?: unknown;
        } | null;
        if (
          typeof json?.access_token !== "string" ||
          typeof json.expires_in !== "number"
        ) {
          throw new Error("bad token response");
        }
        cached = {
          token: json.access_token,
          expires: Date.now() + Math.max(0, json.expires_in - 60) * 1000,
        };
        return json.access_token;
      } catch {
        throw new DataError(
          "unavailable",
          "The source credential could not be obtained.",
        );
      } finally {
        pending = undefined;
      }
    })();
    const token = pending;
    return Promise.race([
      token,
      new Promise<never>((_, reject) =>
        signal.addEventListener(
          "abort",
          () => reject(abortError(callerSignal)),
          { once: true },
        ),
      ),
    ]);
  }

  return {
    capabilities: (): CapabilityDescriptor => ({
      kind: "data_source",
      id: cfg.id,
      operations: ["query"],
      limits: { ...limits },
      attributes: {
        dialect: "databricks",
        parameterStyle: "named",
        access,
        readOnly: true,
        readOnlyEnforcement: "statement-check+source-grants",
      },
    }),

    async query(req: QueryRequest): Promise<QueryResult> {
      const { maxRows, timeoutMs, signal } = checkQuery(req, limits);
      if (access === "delegated" && !req.delegatedToken) {
        throw new DataError(
          "invalid_request",
          "This source needs the user's Databricks token.",
        );
      }
      if (access === "service_principal" && req.delegatedToken !== undefined) {
        throw new DataError(
          "invalid_request",
          "This source uses its own service principal; it does not accept delegated tokens.",
        );
      }
      const params = req.parameters ?? {};
      if (Array.isArray(params)) {
        throw new DataError(
          "invalid_request",
          "Databricks parameters are named (:name): pass an object.",
        );
      }
      if (!Object.keys(params).every((k) => PARAM.test(k))) {
        throw new DataError(
          "invalid_request",
          `Parameter names must match ${PARAM}.`,
        );
      }
      if (!isReadOnlyStatement(req.statement)) {
        throw new DataError(
          "forbidden",
          "Only single read statements (SELECT, WITH, SHOW, DESCRIBE, EXPLAIN) are allowed.",
        );
      }
      const bearer =
        cfg.auth.mode === "delegated"
          ? (req.delegatedToken as string)
          : await servicePrincipalToken(signal, req.signal);

      const s = (await call(
        "/api/2.0/sql/statements",
        bearer,
        signal,
        req.signal,
        {
          warehouse_id: cfg.warehouseId,
          statement: req.statement,
          parameters: Object.entries(params).map(([name, v]) => ({
            name,
            ...(v === null ? {} : { value: String(v) }),
            type:
              typeof v === "boolean"
                ? "BOOLEAN"
                : typeof v === "number"
                  ? Number.isInteger(v)
                    ? "BIGINT"
                    : "DOUBLE"
                  : "STRING",
          })),
          // At least 5 s; a shorter caller timeout aborts our side and the server cancels at 5 s.
          wait_timeout: `${Math.min(50, Math.max(5, Math.ceil(timeoutMs / 1000)))}s`,
          on_wait_timeout: "CANCEL",
          disposition: "INLINE",
          format: "JSON_ARRAY",
          row_limit: maxRows + 1,
          byte_limit: limits.maxBytes,
        },
      )) as Statement;

      const state = s.status?.state;
      if (state === "FAILED") throw failed(s.status?.error);
      // on_wait_timeout CANCEL: anything still running at wait_timeout was cancelled server-side.
      if (state !== "SUCCEEDED") {
        throw new DataError("timeout", "The statement timed out.");
      }
      const columns = (s.manifest?.schema?.columns ?? []).map((c) => ({
        name: String(c.name),
        type: String(c.type_text ?? c.type_name ?? "unknown"),
      }));
      const rows: (string | null)[][] = [];
      let bytes = 0;
      let truncated = s.manifest?.truncated === true;
      let chunk: Chunk = s.result ?? {};
      for (;;) {
        for (const row of chunk.data_array ?? []) {
          if (
            !Array.isArray(row) ||
            !row.every((v) => v === null || typeof v === "string")
          ) {
            throw new DataError(
              "unavailable",
              "The source sent a malformed row.",
            );
          }
          const size = rowBytes(row);
          if (rows.length === maxRows || bytes + size > limits.maxBytes) {
            truncated = true;
            break;
          }
          bytes += size;
          rows.push(row);
        }
        const link = chunk.next_chunk_internal_link;
        if (truncated || link === undefined) break;
        // Only follow links back to this statement's results, never anywhere else.
        if (typeof link !== "string" || !CHUNK_LINK.test(link)) {
          throw new DataError(
            "unavailable",
            "The source sent an unexpected link.",
          );
        }
        chunk = (await call(link, bearer, signal, req.signal)) as Chunk;
      }
      return { columns, rows, truncated, access };
    },

    close: async () => {},
  };
}

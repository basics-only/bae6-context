/** How the source sees the caller (ADR-0003): never present a service principal as the user. */
export type Access = "service_principal" | "delegated";

export type Scalar = string | number | boolean | null;

export interface CapabilityDescriptor {
  kind: "data_source";
  id: string;
  operations: ("query" | "list" | "read" | "stat")[];
  limits: Record<string, number>;
  attributes: Record<string, string | number | boolean | string[]>;
}

export interface QueryRequest {
  statement: string;
  /** In the adapter's own dialect: positional for Postgres, named for Databricks. */
  parameters?: Scalar[] | Record<string, Scalar>;
  maxRows?: number;
  timeoutMs?: number;
  signal?: AbortSignal;
  /** Required in delegated mode, refused otherwise: a token issued for the source itself. */
  delegatedToken?: string;
}

export interface QueryResult {
  columns: { name: string; type: string }[];
  /** Values in the source's text form; null is SQL NULL. */
  rows: (string | null)[][];
  /** True when rows stopped at `maxRows` or the byte limit. */
  truncated: boolean;
  access: Access;
}

export interface ObjectInfo {
  key: string;
  size: number;
  etag: string;
  versionId?: string;
  lastModified: string;
}

export interface ListRequest {
  prefix?: string;
  cursor?: string;
  limit?: number;
  signal?: AbortSignal;
}

export interface ReadRequest {
  key: string;
  versionId?: string;
  /** Inclusive byte offsets, as in HTTP Range. */
  range?: { start: number; end: number };
  signal?: AbortSignal;
}

export interface QuerySource {
  capabilities(): CapabilityDescriptor;
  query(req: QueryRequest): Promise<QueryResult>;
  close(): Promise<void>;
}

export interface ObjectSource {
  capabilities(): CapabilityDescriptor;
  list(req: ListRequest): Promise<{ items: ObjectInfo[]; nextCursor?: string }>;
  read(req: ReadRequest): Promise<{ info: ObjectInfo; body: Uint8Array }>;
  stat(key: string, signal?: AbortSignal): Promise<ObjectInfo>;
}

export type DataErrorCode =
  | "invalid_request"
  | "forbidden"
  | "not_found"
  | "timeout"
  | "unavailable"
  | "too_large";

/** The only error adapters throw. Messages never carry SQL, credentials or source data. */
export class DataError extends Error {
  constructor(
    readonly code: DataErrorCode,
    message: string,
  ) {
    super(message);
    this.name = "DataError";
  }
}

export interface QueryLimits {
  maxRows: number;
  maxBytes: number;
  maxTimeoutMs: number;
  maxStatementLength: number;
}

/** Validates a query against the adapter's limits; over-limit requests are refused, not clamped. */
export function checkQuery(req: QueryRequest, limits: QueryLimits) {
  const { maxRows = limits.maxRows, timeoutMs = limits.maxTimeoutMs } = req;
  if (
    typeof req.statement !== "string" ||
    req.statement.trim() === "" ||
    req.statement.length > limits.maxStatementLength
  ) {
    throw new DataError(
      "invalid_request",
      `statement must be 1 to ${limits.maxStatementLength} characters.`,
    );
  }
  if (!Number.isInteger(maxRows) || maxRows < 1 || maxRows > limits.maxRows) {
    throw new DataError(
      "invalid_request",
      `maxRows must be 1 to ${limits.maxRows}.`,
    );
  }
  if (
    !Number.isInteger(timeoutMs) ||
    timeoutMs < 1 ||
    timeoutMs > limits.maxTimeoutMs
  ) {
    throw new DataError(
      "invalid_request",
      `timeoutMs must be 1 to ${limits.maxTimeoutMs}.`,
    );
  }
  const params = req.parameters ?? [];
  const values = Array.isArray(params) ? params : Object.values(params);
  if (
    values.length > 256 ||
    !values.every(
      (v) =>
        v === null ||
        typeof v === "boolean" ||
        (typeof v === "number" && Number.isFinite(v)) ||
        (typeof v === "string" && v.length <= 65_536),
    )
  ) {
    throw new DataError(
      "invalid_request",
      "parameters must be at most 256 scalars.",
    );
  }
  return {
    maxRows,
    timeoutMs,
    signal: req.signal
      ? AbortSignal.any([req.signal, AbortSignal.timeout(timeoutMs)])
      : AbortSignal.timeout(timeoutMs),
  };
}

/** Bytes a row adds to the JSON response; used for the byte limit. */
export const rowBytes = (row: (string | null)[]) =>
  Buffer.byteLength(JSON.stringify(row));

import {
  type CapabilityDescriptor,
  checkQuery,
  DataError,
  type QueryLimits,
  type QueryRequest,
  type QueryResult,
  type QuerySource,
  rowBytes,
} from "@bae6/context-data";
import pg from "pg";

export interface PostgresConfig {
  id: string;
  /** From the operator's secret, never from a caller. The role should be read-only. */
  connectionString: string;
  /** Size it for the replica count: poolMax × replicas ≤ this database's connection share. */
  poolMax: number;
  limits?: Partial<QueryLimits>;
}

const ID = /^[a-z0-9][a-z0-9._:/-]{0,127}$/;
const FETCH_ROWS = 500;
const DEFAULT_LIMITS: QueryLimits = {
  maxRows: 10_000,
  maxBytes: 16 * 1024 * 1024,
  maxTimeoutMs: 60_000,
  maxStatementLength: 100_000,
};

const TYPES: Record<number, string> = {
  16: "boolean",
  17: "bytea",
  20: "bigint",
  21: "smallint",
  23: "integer",
  25: "text",
  114: "json",
  700: "real",
  701: "double precision",
  1043: "varchar",
  1082: "date",
  1114: "timestamp",
  1184: "timestamptz",
  1700: "numeric",
  2950: "uuid",
  3802: "jsonb",
};

// Every value stays in Postgres's text form: lossless, and the same shape as other sources.
const text = { getTypeParser: () => (v: string) => v };

function mapError(err: unknown): DataError {
  if (err instanceof DataError) return err;
  const code = (err as { code?: unknown }).code;
  if (typeof code !== "string" || !/^[0-9A-Z]{5}$/.test(code)) {
    return new DataError("unavailable", "The database is unreachable.");
  }
  if (code === "57014") {
    return new DataError(
      "timeout",
      "The statement was cancelled or timed out.",
    );
  }
  if (code === "25006" || code === "42501") {
    return new DataError(
      "forbidden",
      `The statement is not permitted (SQLSTATE ${code}).`,
    );
  }
  // Caller errors: syntax, undefined objects, bad data, unsupported features.
  if (/^(0A|21|22|23|42)/.test(code)) {
    return new DataError(
      "invalid_request",
      `The statement failed (SQLSTATE ${code}).`,
    );
  }
  return new DataError(
    "unavailable",
    `The database failed (SQLSTATE ${code}).`,
  );
}

/** A read-only query source. Refuses to start as a superuser, who could bypass read-only mode. */
export async function createPostgresSource(
  cfg: PostgresConfig,
): Promise<QuerySource> {
  if (!ID.test(cfg.id)) throw new Error(`id must match ${ID}`);
  if (!Number.isInteger(cfg.poolMax) || cfg.poolMax < 1 || cfg.poolMax > 100) {
    throw new Error("poolMax must be 1 to 100");
  }
  const limits = { ...DEFAULT_LIMITS, ...cfg.limits };
  const pool = new pg.Pool({
    connectionString: cfg.connectionString,
    max: cfg.poolMax,
    connectionTimeoutMillis: 2_000,
    idleTimeoutMillis: 30_000,
    application_name: "bae6-context-data",
    types: text,
  });
  pool.on("error", () => {});
  try {
    const { rows } = await pool.query<{ rolsuper: string }>(
      "SELECT rolsuper::text AS rolsuper FROM pg_roles WHERE rolname = current_user",
    );
    if (rows[0]?.rolsuper !== "false") {
      throw new Error(`${cfg.id}: refusing to connect as a superuser`);
    }
  } catch (err) {
    await pool.end();
    throw err;
  }

  let inflight = 0;
  return {
    capabilities: (): CapabilityDescriptor => ({
      kind: "data_source",
      id: cfg.id,
      operations: ["query"],
      limits: { ...limits },
      attributes: {
        dialect: "postgres",
        parameterStyle: "positional",
        access: "service_principal",
        readOnly: true,
      },
    }),

    async query(req: QueryRequest): Promise<QueryResult> {
      const { maxRows, timeoutMs, signal } = checkQuery(req, limits);
      if (req.delegatedToken !== undefined) {
        throw new DataError(
          "invalid_request",
          "This source uses its own credential; it does not accept delegated tokens.",
        );
      }
      if (req.parameters !== undefined && !Array.isArray(req.parameters)) {
        throw new DataError(
          "invalid_request",
          "Postgres parameters are positional ($1, $2, …): pass an array.",
        );
      }
      // The pool's wait queue is otherwise unbounded: poolMax running plus poolMax waiting.
      if (inflight >= 2 * cfg.poolMax) {
        throw new DataError("unavailable", "Too many queries are waiting.");
      }
      inflight++;
      let client: pg.PoolClient;
      try {
        client = await pool.connect();
      } catch {
        inflight--;
        throw new DataError("unavailable", "No database connection available.");
      }
      // Not via the pool: with every pooled connection busy, the cancel would queue behind itself.
      const cancel = async () => {
        const c = new pg.Client({
          connectionString: cfg.connectionString,
          connectionTimeoutMillis: 2_000,
        });
        c.on("error", () => {});
        try {
          await c.connect();
          await c.query("SELECT pg_cancel_backend($1)", [
            (client as unknown as { processID: number }).processID,
          ]);
        } catch {
          // statement_timeout still ends the query.
        } finally {
          await c.end().catch(() => {});
        }
      };
      signal.addEventListener("abort", cancel, { once: true });
      let broken = false;
      try {
        if (signal.aborted) throw new DataError("timeout", "Cancelled.");
        await client.query("BEGIN READ ONLY");
        await client.query(`SET LOCAL statement_timeout = ${timeoutMs}`);
        // Extended protocol: one statement only, so `…; COMMIT; INSERT …` cannot escape.
        // A cursor only accepts SELECT or VALUES.
        await client.query({
          text: `DECLARE bae6_result NO SCROLL CURSOR FOR ${req.statement}`,
          values: req.parameters ?? [],
          queryMode: "extended",
        } as pg.QueryConfig);
        const rows: (string | null)[][] = [];
        let columns: QueryResult["columns"] = [];
        let bytes = 0;
        let truncated = false;
        for (;;) {
          const want = Math.min(FETCH_ROWS, maxRows + 1 - rows.length);
          const r = await client.query<(string | null)[]>({
            text: `FETCH ${want} FROM bae6_result`,
            rowMode: "array",
          });
          if (columns.length === 0) {
            columns = r.fields.map((f) => ({
              name: f.name,
              type: TYPES[f.dataTypeID] ?? `oid:${f.dataTypeID}`,
            }));
          }
          for (const row of r.rows) {
            // shortcut: one row is parsed before its size is known; bound column sizes at the source if that matters.
            const size = rowBytes(row);
            if (rows.length === maxRows || bytes + size > limits.maxBytes) {
              truncated = true;
              break;
            }
            bytes += size;
            rows.push(row);
          }
          if (truncated || r.rows.length < want) break;
        }
        await client.query("ROLLBACK");
        return { columns, rows, truncated, access: "service_principal" };
      } catch (err) {
        await client.query("ROLLBACK").catch(() => {
          broken = true;
        });
        throw signal.aborted
          ? new DataError(
              "timeout",
              "The statement was cancelled or timed out.",
            )
          : mapError(err);
      } finally {
        signal.removeEventListener("abort", cancel);
        client.release(broken);
        inflight--;
      }
    },

    close: () => pool.end(),
  };
}

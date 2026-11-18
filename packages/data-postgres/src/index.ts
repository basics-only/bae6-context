import {
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
import pg from "pg";

export interface PostgresConfig {
  id: string;
  /**
   * From the operator's secret, never from a caller. Use a role with SELECT grants only; it
   * can still cancel its own role's other sessions, so give each source its own role.
   */
  connectionString: string;
  /** Size it for the replica count: poolMax × replicas ≤ this database's connection share. */
  poolMax: number;
  limits?: Partial<QueryLimits>;
}

const ID = /^[a-z0-9][a-z0-9._:/-]{0,127}$/;
const MAX_FETCH_ROWS = 100;
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

// A role with any of these could write, read server files or bypass row security.
const UNSAFE_ROLE = `
  SELECT (r.rolsuper OR r.rolbypassrls
    OR pg_has_role(current_user, 'pg_write_server_files', 'USAGE')
    OR pg_has_role(current_user, 'pg_read_server_files', 'USAGE')
    OR pg_has_role(current_user, 'pg_execute_server_program', 'USAGE')
    OR pg_has_role(current_user, 'pg_signal_backend', 'USAGE')
    OR EXISTS (SELECT 1 FROM pg_proc p
               WHERE p.proname IN ('dblink', 'dblink_exec')
                 AND has_function_privilege(p.oid, 'EXECUTE')))::text AS unsafe
  FROM pg_roles r WHERE r.rolname = current_user`;

function mapError(err: unknown): DataError {
  if (err instanceof DataError) return err;
  if (err instanceof TypeError || err instanceof RangeError) throw err;
  const code = (err as { code?: unknown } | null)?.code;
  if (typeof code !== "string" || !/^[0-9A-Z]{5}$/.test(code)) {
    return new DataError("unavailable", "The database is unreachable.");
  }
  if (code === "57014") {
    return new DataError("timeout", "The statement timed out.");
  }
  if (code === "25006" || code === "42501") {
    return new DataError(
      "forbidden",
      `The statement is not permitted (SQLSTATE ${code}).`,
    );
  }
  // Caller errors: syntax, undefined objects, bad data, wrong parameter count.
  if (/^(0A|21|22|23|42)/.test(code) || code === "08P01") {
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

/** A read-only query source. Refuses a role that could write or bypass read-only mode. */
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
  // An idle client that errors is already discarded by the pool; without a listener it would crash the process.
  pool.on("error", () => {});
  try {
    const { rows } = await pool.query<{ unsafe: string }>(UNSAFE_ROLE);
    if (rows[0]?.unsafe !== "false") {
      throw new Error(
        `${cfg.id}: refusing a role that is a superuser, bypasses RLS, has server-file or signal roles, or can call dblink`,
      );
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
      const deadline = Date.now() + timeoutMs;
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
      let broken = false;
      const onClientError = () => {
        broken = true;
      };
      client.on("error", onClientError);
      let cancelling: Promise<void> | undefined;
      // Not via the pool: with every pooled connection busy, the cancel would queue behind itself.
      const cancel = () => {
        cancelling = (async () => {
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
            // The per-FETCH statement_timeout still ends it.
            broken = true;
          } finally {
            await c.end().catch(() => {});
          }
        })();
      };
      signal.addEventListener("abort", cancel, { once: true });
      const remaining = () => {
        if (signal.aborted) throw abortError(req.signal);
        return Math.max(1, deadline - Date.now());
      };
      try {
        await client.query("BEGIN READ ONLY");
        await client.query(`SET LOCAL statement_timeout = ${remaining()}`);
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
        let largest = 0;
        let truncated = false;
        for (;;) {
          // Size each batch from the largest row so far against the bytes left, starting at
          // one row. shortcut: a batch whose rows are far larger than earlier ones can still
          // buffer batch × row size; cap column sizes in a view if a source has huge values.
          const budget =
            largest === 0
              ? 1
              : Math.floor((limits.maxBytes - bytes) / largest) + 1;
          const want = Math.max(
            1,
            Math.min(MAX_FETCH_ROWS, maxRows + 1 - rows.length, budget),
          );
          // Reset each time: the caller's statement cannot turn the timeout off for later FETCHes.
          await client.query(`SET LOCAL statement_timeout = ${remaining()}`);
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
            const size = rowBytes(row);
            largest = Math.max(largest, size);
            if (rows.length === 0 && size > limits.maxBytes) {
              throw new DataError(
                "too_large",
                `A single row is over ${limits.maxBytes} bytes.`,
              );
            }
            if (rows.length === maxRows || bytes + size > limits.maxBytes) {
              truncated = true;
              break;
            }
            bytes += size;
            rows.push(row);
          }
          if (truncated || r.rows.length < want) break;
        }
        remaining();
        return { columns, rows, truncated, access: "service_principal" };
      } catch (err) {
        throw signal.aborted ? abortError(req.signal) : mapError(err);
      } finally {
        signal.removeEventListener("abort", cancel);
        // A late cancel must land before the connection serves anyone else.
        await cancelling;
        // DISCARD ALL drops session state a statement could leave behind: advisory locks,
        // settings, prepared statements, temp tables.
        if (!broken) {
          try {
            await client.query("ROLLBACK");
            await client.query("DISCARD ALL");
          } catch {
            broken = true;
          }
        }
        client.off("error", onClientError);
        client.release(broken);
        inflight--;
      }
    },

    close: () => pool.end(),
  };
}

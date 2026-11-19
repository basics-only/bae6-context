import { createProbe, type EgressPolicy, Registry } from "@bae6/context-mcp";
import { PgRegistryStore } from "@bae6/context-mcp-postgres";
import pg from "pg";
import { createVerifier, type Issuer } from "./identity.js";
import { createRegistryApi } from "./server.js";

const env = process.env;
const log = (e: Record<string, unknown>) => console.log(JSON.stringify(e));

function int(name: string, fallback: number, min: number, max: number) {
  const v = Number(env[name] ?? fallback);
  if (!Number.isInteger(v) || v < min || v > max) {
    throw new Error(`${name} must be an integer from ${min} to ${max}`);
  }
  return v;
}

// Fails closed: an unset NODE_ENV is not development.
const dev = (name: string) => {
  const on = env[name] === "true";
  if (on && env.NODE_ENV !== "development") {
    throw new Error(`${name} needs NODE_ENV=development`);
  }
  if (on) log({ msg: "insecure_dev_flag", flag: name });
  return on;
};
const allowHttpJwks = dev("BAE6_ALLOW_HTTP_JWKS");
const allowInsecure = dev("BAE6_MCP_ALLOW_INSECURE_ENDPOINTS");

function required(name: string) {
  const v = env[name];
  if (!v) throw new Error(`${name} is required`);
  return v;
}

const issuers = JSON.parse(env.BAE6_MCP_ISSUERS ?? "null") as Issuer[] | null;
if (!Array.isArray(issuers)) {
  throw new Error(
    "BAE6_MCP_ISSUERS must be a JSON array of {issuer,audience,jwksUri,organization,application}",
  );
}
const suffixes = (env.BAE6_MCP_ALLOWED_HOST_SUFFIXES ?? "")
  .split(",")
  .map((s) => s.trim())
  .filter(Boolean);
if (suffixes.length === 0 && !allowInsecure) {
  throw new Error("BAE6_MCP_ALLOWED_HOST_SUFFIXES is required");
}
const policy: EgressPolicy = {
  ...(suffixes.length > 0 ? { allowedHostSuffixes: suffixes } : {}),
  ...(allowInsecure ? { allowInsecure } : {}),
};

const port = int("PORT", 8080, 1, 65535);
const maxInflight = int("BAE6_MAX_INFLIGHT", 32, 1, 10_000);
const maxInflightPerOrganization = int(
  "BAE6_MAX_INFLIGHT_PER_ORGANIZATION",
  Math.max(1, Math.floor(maxInflight / 2)),
  1,
  maxInflight,
);
const poolMax = int("BAE6_MCP_DB_POOL_MAX", 10, 1, 100);
const shutdownDelayMs = int("BAE6_SHUTDOWN_DELAY_MS", 0, 0, 15_000);
const requestTimeoutMs = int("BAE6_REQUEST_TIMEOUT_MS", 30_000, 1000, 600_000);

const pool = new pg.Pool({
  connectionString: required("BAE6_MCP_DATABASE_URL"),
  max: poolMax,
  connectionTimeoutMillis: 5000,
  statement_timeout: 15_000,
});
pool.on("error", (err) => log({ msg: "pool_error", error: err.name }));
await PgRegistryStore.migrate(pool);

const server = createRegistryApi({
  verify: createVerifier(issuers, { allowHttpJwks }),
  registry: new Registry(new PgRegistryStore(pool), policy),
  probe: createProbe(policy),
  ping: async () => void (await pool.query("SELECT 1")),
  maxInflight,
  maxInflightPerOrganization,
  requestTimeoutMs,
  log,
});
server.listen(port, "0.0.0.0", () => log({ msg: "listening", port }));

process.once("SIGTERM", () => {
  server.drain();
  setTimeout(() => {
    server.close(async () => {
      await pool.end().catch(() => {});
      process.exit(0);
    });
    server.closeIdleConnections();
    setTimeout(() => process.exit(1), requestTimeoutMs + 5000).unref();
  }, shutdownDelayMs).unref();
});

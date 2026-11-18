import { readFileSync } from "node:fs";
import { createDatabricksSource } from "@bae6/context-data-databricks";
import { createPostgresSource } from "@bae6/context-data-postgres";
import { awsCredentials, createS3Source } from "@bae6/context-data-s3";
import { lazy, parseConfig, type SourceConfig } from "./config.js";
import { createVerifier, type Issuer } from "./identity.js";
import { type BoundSource, createDataApi, type Source } from "./server.js";

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
const allowHttp = dev("BAE6_ALLOW_HTTP_SOURCES");

const issuers = JSON.parse(env.BAE6_DATA_ISSUERS ?? "null") as Issuer[] | null;
if (!Array.isArray(issuers)) {
  throw new Error(
    "BAE6_DATA_ISSUERS must be a JSON array of {issuer,audience,jwksUri,organization,application}",
  );
}
const configFile = env.BAE6_DATA_CONFIG_FILE;
const rawConfig =
  env.BAE6_DATA_CONFIG ?? (configFile && readFileSync(configFile, "utf8"));
if (!rawConfig) {
  throw new Error("BAE6_DATA_CONFIG or BAE6_DATA_CONFIG_FILE is required");
}
const config = parseConfig(JSON.parse(rawConfig), env);

const port = int("PORT", 8080, 1, 65535);
const maxInflight = int("BAE6_MAX_INFLIGHT", 16, 1, 10_000);
const maxInflightPerOrganization = int(
  "BAE6_MAX_INFLIGHT_PER_ORGANIZATION",
  Math.max(1, Math.floor(maxInflight / 2)),
  1,
  maxInflight,
);
const openTimeoutMs = int("BAE6_SOURCE_OPEN_TIMEOUT_MS", 10_000, 100, 60_000);
const shutdownDelayMs = int("BAE6_SHUTDOWN_DELAY_MS", 0, 0, 15_000);
// Above every adapter's longest timeout (Postgres's 60 s default).
const requestTimeoutMs = int("BAE6_REQUEST_TIMEOUT_MS", 90_000, 1000, 600_000);

const aws = awsCredentials(env);
const opened: Source[] = [];

function create(c: SourceConfig): Promise<Source> | Source {
  const limits = c.limits ? { limits: c.limits } : {};
  if (c.kind === "postgres") {
    return createPostgresSource({
      id: c.id,
      connectionString: c.connectionString,
      poolMax: c.poolMax,
      ...limits,
    });
  }
  if (c.kind === "databricks") {
    return createDatabricksSource({
      id: c.id,
      host: c.host,
      warehouseId: c.warehouseId,
      auth: c.auth,
      allowHttp,
      ...limits,
    });
  }
  return createS3Source({
    id: c.id,
    region: c.region,
    bucket: c.bucket,
    prefix: c.prefix,
    credentials: aws,
    allowHttp,
    ...(c.endpoint ? { endpoint: c.endpoint } : {}),
    ...limits,
  });
}

const sources: BoundSource[] = config.map((c) => ({
  id: c.id,
  organization: c.organization,
  maxConcurrent: c.maxConcurrent,
  // One organization's unreachable source must not keep the API from starting for all.
  open: lazy(() => opening(c)),
}));

async function opening(c: SourceConfig): Promise<Source> {
  const pending = Promise.resolve().then(() => create(c));
  let timer: NodeJS.Timeout | undefined;
  try {
    const s = await Promise.race([
      pending,
      new Promise<never>((_, fail) => {
        timer = setTimeout(
          () => fail(new Error(`${c.id} did not open in time`)),
          openTimeoutMs,
        );
      }),
    ]);
    opened.push(s);
    return s;
  } catch (err) {
    // One that opens after its timeout is closed rather than leaked.
    pending.then((s) => s.close()).catch(() => {});
    throw err;
  } finally {
    clearTimeout(timer);
  }
}

const server = createDataApi({
  verify: createVerifier(issuers, { allowHttpJwks }),
  sources,
  maxInflight,
  maxInflightPerOrganization,
  requestTimeoutMs,
  log,
});
server.listen(port, "0.0.0.0", () =>
  log({ msg: "listening", port, sources: sources.length }),
);

process.once("SIGTERM", () => {
  server.drain();
  setTimeout(() => {
    server.close(() => {
      Promise.allSettled(opened.map((s) => s.close())).finally(() =>
        process.exit(0),
      );
    });
    server.closeIdleConnections();
    setTimeout(() => process.exit(1), requestTimeoutMs + 5000).unref();
  }, shutdownDelayMs).unref();
});

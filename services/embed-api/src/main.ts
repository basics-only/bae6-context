import { randomUUID } from "node:crypto";
import { readFileSync } from "node:fs";
import { readFile } from "node:fs/promises";
import type { ObjectSource } from "@bae6/context-data";
import { awsCredentials, createS3Source } from "@bae6/context-data-s3";
import { prefixAcl, Worker } from "@bae6/context-embed";
import { PgJobQueue, PgVectorIndex } from "@bae6/context-embed-pgvector";
import pg from "pg";
import { type CollectionConfig, parseConfig } from "./config.js";
import { createVerifier, type Issuer, principalOf } from "./identity.js";
import { createAiEmbeddingModel } from "./model.js";
import { type BoundCollection, createEmbedApi } from "./server.js";

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

function required(name: string) {
  const v = env[name];
  if (!v) throw new Error(`${name} is required`);
  return v;
}

const issuers = JSON.parse(env.BAE6_EMBED_ISSUERS ?? "null") as Issuer[] | null;
if (!Array.isArray(issuers)) {
  throw new Error(
    "BAE6_EMBED_ISSUERS must be a JSON array of {issuer,audience,jwksUri,organization,application}",
  );
}
const configFile = env.BAE6_EMBED_CONFIG_FILE;
const rawConfig =
  env.BAE6_EMBED_CONFIG ?? (configFile && readFileSync(configFile, "utf8"));
if (!rawConfig) {
  throw new Error("BAE6_EMBED_CONFIG or BAE6_EMBED_CONFIG_FILE is required");
}
const config = parseConfig(JSON.parse(rawConfig));

const aiUrl = required("BAE6_AI_URL");
if (new URL(aiUrl).protocol !== "https:" && !allowHttp) {
  throw new Error("BAE6_AI_URL must be https");
}
const aiTokenFile = env.BAE6_AI_TOKEN_FILE;
const aiToken = env.BAE6_AI_TOKEN?.trim();
if (!aiTokenFile === !aiToken) {
  throw new Error("set exactly one of BAE6_AI_TOKEN_FILE or BAE6_AI_TOKEN");
}
const port = int("PORT", 8080, 1, 65535);
const maxInflight = int("BAE6_MAX_INFLIGHT", 16, 1, 10_000);
const maxInflightPerOrganization = int(
  "BAE6_MAX_INFLIGHT_PER_ORGANIZATION",
  Math.max(1, Math.floor(maxInflight / 2)),
  1,
  maxInflight,
);
const workerCount = int("BAE6_EMBED_WORKERS", 1, 0, 8);
const poolMax = int("BAE6_EMBED_DB_POOL_MAX", 10, 1, 100);
const maxAclAgeMs = int(
  "BAE6_MAX_ACL_AGE_MS",
  24 * 3600 * 1000,
  1,
  7 * 24 * 3600 * 1000,
);
const shutdownDelayMs = int("BAE6_SHUTDOWN_DELAY_MS", 0, 0, 15_000);
const requestTimeoutMs = int("BAE6_REQUEST_TIMEOUT_MS", 30_000, 1000, 600_000);

const pool = new pg.Pool({
  connectionString: required("BAE6_EMBED_DATABASE_URL"),
  max: poolMax,
  connectionTimeoutMillis: 5000,
  statement_timeout: 15_000,
});
pool.on("error", (err) => log({ msg: "pool_error", error: err.name }));
await PgVectorIndex.migrate(pool);
const index = new PgVectorIndex(pool);
const queue = new PgJobQueue(pool);

const token = async () =>
  aiToken ?? (await readFile(aiTokenFile as string, "utf8")).trim();
const aws = awsCredentials(env);
const models = new Map<string, BoundCollection["model"]>();
const key = (org: string, id: string) => `${org}/${id}`;
for (const c of config) {
  models.set(
    key(c.organization, c.id),
    createAiEmbeddingModel({
      url: aiUrl,
      model: c.model.id,
      dimensions: c.model.dimensions,
      token,
    }),
  );
}
const model = (c: CollectionConfig) =>
  models.get(key(c.organization, c.id)) as BoundCollection["model"];
const collections: BoundCollection[] = config.map((c) => ({
  id: c.id,
  organization: c.organization,
  model: model(c),
}));

const byKey = new Map(config.map((c) => [key(c.organization, c.id), c]));
const sources = new Map<string, Promise<ObjectSource>>();
function source(c: CollectionConfig) {
  const k = key(c.organization, c.id);
  let opened = sources.get(k);
  if (!opened) {
    opened = Promise.resolve(
      createS3Source({
        id: c.id,
        region: c.source.region,
        bucket: c.source.bucket,
        prefix: c.source.prefix,
        credentials: aws,
        allowHttp,
        ...(c.source.endpoint ? { endpoint: c.source.endpoint } : {}),
      }),
    );
    opened.catch(() => sources.delete(k));
    sources.set(k, opened);
  }
  return opened;
}

const stop = new AbortController();
const workers = Array.from({ length: workerCount }, () =>
  new Worker(queue, {
    owner: `w-${randomUUID()}`,
    onError: (err) => log({ msg: "worker_error", error: (err as Error).name }),
    resolve: async (job) => {
      const c = byKey.get(
        key(job.scope.organizationId, job.scope.collectionId),
      );
      if (!c) throw new Error("collection is no longer configured");
      return {
        source: await source(c),
        model: model(c),
        index,
        acl: prefixAcl(c.acl),
      };
    },
  }).run(stop.signal),
);

const server = createEmbedApi({
  verify: createVerifier(issuers, { allowHttpJwks }),
  collections,
  index,
  queue,
  principals: (who) => [principalOf(who)],
  ping: async () => void (await pool.query("SELECT 1")),
  maxAclAgeMs,
  maxInflight,
  maxInflightPerOrganization,
  requestTimeoutMs,
  log,
});
server.listen(port, "0.0.0.0", () =>
  log({
    msg: "listening",
    port,
    collections: collections.length,
    workers: workerCount,
  }),
);

process.once("SIGTERM", () => {
  server.drain();
  setTimeout(() => {
    stop.abort();
    server.close(async () => {
      await Promise.allSettled(workers);
      const opened = await Promise.allSettled(sources.values());
      await Promise.allSettled(
        opened.flatMap((o) =>
          o.status === "fulfilled" ? [o.value.close()] : [],
        ),
      );
      await pool.end().catch(() => {});
      process.exit(0);
    });
    server.closeIdleConnections();
    setTimeout(() => process.exit(1), requestTimeoutMs + 5000).unref();
  }, shutdownDelayMs).unref();
});

import { randomUUID } from "node:crypto";
import type { Scope } from "@bae6/context-embed";
import pg from "pg";
import { PgVectorIndex } from "../src/index.js";

const url = process.env.BAE6_EMBED_DATABASE_URL;
if (!url) throw new Error("BAE6_EMBED_DATABASE_URL is required");
const host = new URL(url).hostname;
if (!["localhost", "127.0.0.1", "::1", "[::1]"].includes(host)) {
  throw new Error(
    "the benchmark creates and drops databases: local hosts only",
  );
}
const args = process.argv.slice(2).map(Number);
const [chunks = 20_000, dims = 256, concurrency = 8, queries = 1000] = args;
for (const n of [chunks, dims, concurrency, queries]) {
  if (!Number.isInteger(n) || n < 1) {
    throw new Error(
      "usage: bench [chunks dims concurrency queries], all integers >= 1",
    );
  }
}
const perDoc = 10;
const users = 20;

let seed = 1;
const rand = () => {
  seed = (seed * 1_664_525 + 1_013_904_223) >>> 0;
  return seed / 2 ** 32 - 0.5;
};
const vector = () => Array.from({ length: dims }, rand);
const quantile = (xs: number[], q: number) =>
  [...xs].sort((a, b) => a - b)[Math.ceil(q * xs.length) - 1] as number;
const ms = (n: number) => Math.round(n * 100) / 100;

const dbName = `bench_${randomUUID().replaceAll("-", "")}`;
const benchUrl = new URL(url);
benchUrl.pathname = `/${dbName}`;
const admin = new pg.Client({ connectionString: url });
admin.on("error", () => {});
const pool = new pg.Pool({
  connectionString: benchUrl.href,
  max: concurrency + 2,
});
pool.on("error", () => {});

try {
  await admin.connect();
  await admin.query(`CREATE DATABASE ${dbName}`);
  await PgVectorIndex.migrate(pool);
  const index = new PgVectorIndex(pool, 60_000);
  const scope: Scope = {
    organizationId: `bench-${randomUUID()}`,
    collectionId: "bench",
  };
  await index.ensureCollection(scope, "bench-model", dims);

  const docs = Math.ceil(chunks / perDoc);
  const corpus = Array.from({ length: docs }, (_, d) => ({
    key: `doc-${d}`,
    revision: "r1",
    principals: [`user:u${d % users}`],
    aclSyncedAt: Date.now(),
    chunks: Array.from({ length: perDoc }, (_, i) => ({
      id: `doc-${d}#r1#${i}`,
      ordinal: i,
      text: `chunk ${i} of doc ${d} `.repeat(20),
      start: i * 400,
      end: i * 400 + 399,
      vector: vector(),
    })),
  }));
  const queryVectors = Array.from({ length: queries }, vector);

  const ingestStart = performance.now();
  for (const doc of corpus) await index.upsert(scope, doc);
  const ingestSeconds = (performance.now() - ingestStart) / 1000;
  await pool.query("ANALYZE");

  const tables = await pool.query(
    `SELECT sum(pg_total_relation_size(c.oid)) AS bytes
     FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
     WHERE n.nspname = 'public' AND c.relkind = 'r'`,
  );
  const stored = Number(tables.rows[0].bytes);
  const versions = await pool.query(
    `SELECT version() AS pg,
            (SELECT extversion FROM pg_extension WHERE extname = 'vector') AS vector`,
  );

  const search = async (i: number) => {
    const vec = queryVectors[i % queries] as number[];
    const t = performance.now();
    const hits = await index.search(scope, {
      vector: vec,
      principals: [`user:u${i % users}`],
      limit: 10,
      maxAclAgeMs: 3_600_000,
    });
    const elapsed = performance.now() - t;
    if (hits.length !== 10) throw new Error(`got ${hits.length} hits`);
    return elapsed;
  };
  await Promise.all(Array.from({ length: concurrency }, (_, i) => search(i)));
  for (let i = 0; i < 10; i++) await search(i);

  const serial: number[] = [];
  for (let i = 0; i < queries; i++) serial.push(await search(i));

  const parallel: number[] = [];
  const wallStart = performance.now();
  let next = 0;
  await Promise.all(
    Array.from({ length: concurrency }, async () => {
      while (next < queries) parallel.push(await search(next++));
    }),
  );
  const wall = (performance.now() - wallStart) / 1000;

  console.log(
    JSON.stringify({
      postgres: String(versions.rows[0].pg).split(" on ")[0],
      pgvector: versions.rows[0].vector,
      index: "exact scan, no ANN index",
      node: process.version,
      chunks: docs * perDoc,
      dims,
      queries,
      ingestChunksPerSecond: Math.round((docs * perDoc) / ingestSeconds),
      storedBytesPerChunk: Math.round(stored / (docs * perDoc)),
      rawVectorBytesPerChunk: dims * 4,
      serialMs: {
        p50: ms(quantile(serial, 0.5)),
        p95: ms(quantile(serial, 0.95)),
      },
      concurrency,
      concurrentMs: {
        p50: ms(quantile(parallel, 0.5)),
        p95: ms(quantile(parallel, 0.95)),
      },
      concurrentQps: Math.round(queries / wall),
    }),
  );
} finally {
  await pool.end().catch(() => {});
  await admin
    .query(`DROP DATABASE IF EXISTS ${dbName} WITH (FORCE)`)
    .catch(() => {});
  await admin.end().catch(() => {});
}

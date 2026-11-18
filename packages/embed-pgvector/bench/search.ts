import { randomUUID } from "node:crypto";
import type { Scope } from "@bae6/context-embed";
import pg from "pg";
import { PgVectorIndex } from "../src/index.js";

const url = process.env.BAE6_EMBED_DATABASE_URL;
if (!url) throw new Error("BAE6_EMBED_DATABASE_URL is required");
const [chunks = 20_000, dims = 256, concurrency = 8, queries = 200] =
  process.argv.slice(2).map(Number);
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
const admin = new pg.Client({ connectionString: url });
await admin.connect();
await admin.query(`CREATE DATABASE ${dbName}`);
const benchUrl = new URL(url);
benchUrl.pathname = `/${dbName}`;
const pool = new pg.Pool({
  connectionString: benchUrl.href,
  max: concurrency + 2,
});
await PgVectorIndex.migrate(pool);
const index = new PgVectorIndex(pool, 60_000);
const scope: Scope = {
  organizationId: `bench-${randomUUID()}`,
  collectionId: "bench",
};

try {
  await index.ensureCollection(scope, "bench-model", dims);
  const docs = Math.ceil(chunks / perDoc);
  const ingestStart = performance.now();
  for (let d = 0; d < docs; d++) {
    await index.upsert(scope, {
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
    });
  }
  const ingestSeconds = (performance.now() - ingestStart) / 1000;

  const sizes = await pool.query(
    `SELECT pg_total_relation_size('chunks') AS chunks,
            pg_total_relation_size('documents') AS documents`,
  );
  const stored = Number(sizes.rows[0].chunks) + Number(sizes.rows[0].documents);

  const search = async (user: number) => {
    const t = performance.now();
    const hits = await index.search(scope, {
      vector: vector(),
      principals: [`user:u${user}`],
      limit: 10,
      maxAclAgeMs: 3_600_000,
    });
    if (hits.length !== 10) throw new Error(`got ${hits.length} hits`);
    return performance.now() - t;
  };
  for (let i = 0; i < 10; i++) await search(i % users);

  const serial: number[] = [];
  for (let i = 0; i < queries; i++) serial.push(await search(i % users));

  const parallel: number[] = [];
  const wallStart = performance.now();
  let next = 0;
  await Promise.all(
    Array.from({ length: concurrency }, async () => {
      while (next < queries) parallel.push(await search(next++ % users));
    }),
  );
  const wall = (performance.now() - wallStart) / 1000;

  console.log(
    JSON.stringify({
      chunks: docs * perDoc,
      dims,
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
  pool.on("error", () => {});
  await pool.end();
  await admin.query(`DROP DATABASE ${dbName} WITH (FORCE)`);
  await admin.end();
}

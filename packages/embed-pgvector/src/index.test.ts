import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { after, before, test } from "node:test";
import type { IndexedDocument, Scope } from "@bae6/context-embed";
import pg from "pg";
import { PgVectorIndex } from "./index.js";

const url = process.env.BAE6_EMBED_DATABASE_URL;
const skip = url ? false : "BAE6_EMBED_DATABASE_URL not set";
const pool = new pg.Pool({ connectionString: url, max: 4 });
const index = new PgVectorIndex(pool);

before(async () => {
  if (url) await Promise.all([1, 2, 3].map(() => PgVectorIndex.migrate(pool)));
});
after(() => pool.end());

const doc = (
  key: string,
  principals: string[],
  vectors: number[][],
  revision = "r1",
): IndexedDocument => ({
  key,
  revision,
  principals,
  chunks: vectors.map((vector, i) => ({
    id: `${key}#${revision}#${i}`,
    ordinal: i,
    text: `text of ${key} ${i}`,
    start: i * 10,
    end: i * 10 + 9,
    vector,
  })),
});

async function fresh() {
  const scope: Scope = {
    organizationId: `org-${randomUUID()}`,
    collectionId: "kb",
  };
  await index.ensureCollection(scope, "m1", 2);
  return scope;
}

const q = (principals: string[], extra = {}) => ({
  vector: [1, 0],
  limit: 10,
  principals,
  maxAclAgeMs: 60_000,
  ...extra,
});

test("ranks by similarity and cites the span", { skip }, async () => {
  const scope = await fresh();
  await index.upsert(
    scope,
    doc(
      "a.txt",
      ["group:g"],
      [
        [0, 1],
        [1, 0],
      ],
    ),
  );
  const hits = await index.search(scope, q(["group:g"]));
  assert.deepEqual(
    hits.map((h) => [h.key, h.start, h.end, h.text]),
    [
      ["a.txt", 10, 19, "text of a.txt 1"],
      ["a.txt", 0, 9, "text of a.txt 0"],
    ],
  );
  assert.ok(hits[0]!.score > hits[1]!.score);
});

test(
  "returns nothing to other principals or other tenants",
  { skip },
  async () => {
    const scope = await fresh();
    await index.upsert(scope, doc("a.txt", ["group:g"], [[1, 0]]));
    assert.deepEqual(await index.search(scope, q(["group:other"])), []);
    assert.deepEqual(await index.search(scope, q([])), []);
    const other = await fresh();
    assert.deepEqual(await index.search(other, q(["group:g"])), []);
    await assert.rejects(
      index.search({ ...other, collectionId: "missing" }, q(["group:g"])),
      { code: "invalid_request" },
    );
  },
);

test("a revoked principal no longer finds the document", { skip }, async () => {
  const scope = await fresh();
  await index.upsert(scope, doc("a.txt", ["user:alice"], [[1, 0]]));
  assert.equal((await index.search(scope, q(["user:alice"]))).length, 1);
  await index.refreshAcl(scope, "a.txt", ["user:bob"]);
  assert.deepEqual(await index.search(scope, q(["user:alice"])), []);
  assert.equal((await index.search(scope, q(["user:bob"]))).length, 1);
});

test(
  "a document whose ACL is older than the bound is excluded",
  { skip },
  async () => {
    const scope = await fresh();
    await index.upsert(scope, doc("a.txt", ["user:alice"], [[1, 0]]));
    await new Promise((ok) => setTimeout(ok, 30));
    assert.deepEqual(
      await index.search(scope, q(["user:alice"], { maxAclAgeMs: 10 })),
      [],
    );
    await index.refreshAcl(scope, "a.txt", ["user:alice"]);
    assert.equal(
      (await index.search(scope, q(["user:alice"], { maxAclAgeMs: 10_000 })))
        .length,
      1,
    );
  },
);

test(
  "remove takes the document and its chunks out at once",
  { skip },
  async () => {
    const scope = await fresh();
    await index.upsert(scope, doc("a.txt", ["user:alice"], [[1, 0]]));
    await index.remove(scope, "a.txt");
    assert.deepEqual(await index.search(scope, q(["user:alice"])), []);
    const { rows } = await pool.query(
      "SELECT count(*)::int AS n FROM chunks WHERE organization_id = $1",
      [scope.organizationId],
    );
    assert.equal(rows[0].n, 0);
  },
);

test(
  "a new revision replaces the old chunks, and a failed one leaves them",
  { skip },
  async () => {
    const scope = await fresh();
    await index.upsert(
      scope,
      doc(
        "a.txt",
        ["user:alice"],
        [
          [1, 0],
          [0, 1],
        ],
      ),
    );
    await index.upsert(scope, doc("a.txt", ["user:alice"], [[1, 0]], "r2"));
    let hits = await index.search(scope, q(["user:alice"]));
    assert.deepEqual(
      hits.map((h) => h.revision),
      ["r2"],
    );
    await assert.rejects(
      index.upsert(scope, doc("a.txt", ["user:alice"], [[1, 0], [1]], "r3")),
      { code: "invalid_request" },
    );
    hits = await index.search(scope, q(["user:alice"]));
    assert.deepEqual(
      hits.map((h) => h.revision),
      ["r2"],
    );
  },
);

test("a collection is tied to its model and dimensions", { skip }, async () => {
  const scope = await fresh();
  await assert.rejects(index.ensureCollection(scope, "m2", 2), {
    code: "invalid_request",
  });
  await assert.rejects(index.ensureCollection(scope, "m1", 3), {
    code: "invalid_request",
  });
  await assert.rejects(index.search(scope, { ...q(["user:a"]), vector: [1] }), {
    code: "invalid_request",
  });
});

test("bounds the result size and the document listing", { skip }, async () => {
  const scope = await fresh();
  await assert.rejects(index.search(scope, q(["user:a"], { limit: 51 })), {
    code: "invalid_request",
  });
  await index.upsert(scope, doc("a.txt", ["user:a"], [[1, 0]]));
  await index.upsert(scope, doc("b.txt", ["user:a"], [[1, 0]]));
  assert.equal((await index.revisions(scope, 2)).size, 2);
  await assert.rejects(index.revisions(scope, 1), { code: "too_large" });
});

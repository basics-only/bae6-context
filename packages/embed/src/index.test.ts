import assert from "node:assert/strict";
import { test } from "node:test";
import type { ObjectInfo, ObjectSource } from "@bae6/context-data";
import {
  chunkText,
  type EmbeddingModel,
  EmbedError,
  type Hit,
  type IndexedDocument,
  ingestObject,
  prefixAcl,
  type Scope,
  syncCollection,
  type VectorIndex,
} from "./index.js";

const scope: Scope = { organizationId: "acme", collectionId: "kb" };

class MemoryIndex implements VectorIndex {
  docs = new Map<string, IndexedDocument>();
  private k = (s: Scope, key: string) =>
    `${s.organizationId}/${s.collectionId}/${key}`;
  async upsert(s: Scope, doc: IndexedDocument) {
    this.docs.set(this.k(s, doc.key), doc);
  }
  async refreshAcl(s: Scope, key: string, principals: string[]) {
    const d = this.docs.get(this.k(s, key));
    if (d) d.principals = principals;
  }
  async remove(s: Scope, key: string) {
    this.docs.delete(this.k(s, key));
  }
  async revisions(s: Scope, _limit: number) {
    const prefix = this.k(s, "");
    return new Map(
      [...this.docs]
        .filter(([k]) => k.startsWith(prefix))
        .map(([k, d]) => [k.slice(prefix.length), d.revision]),
    );
  }
  async search(
    s: Scope,
    req: { vector: number[]; limit: number; principals: string[] },
  ): Promise<Hit[]> {
    const prefix = this.k(s, "");
    const hits: Hit[] = [];
    for (const [k, d] of this.docs) {
      if (!k.startsWith(prefix)) continue;
      if (!d.principals.some((p) => req.principals.includes(p))) continue;
      for (const c of d.chunks) {
        hits.push({
          chunkId: c.id,
          key: d.key,
          revision: d.revision,
          start: c.start,
          end: c.end,
          text: c.text,
          score: c.vector.reduce((a, v, i) => a + v * req.vector[i]!, 0),
        });
      }
    }
    return hits.sort((a, b) => b.score - a.score).slice(0, req.limit);
  }
}

class FakeModel implements EmbeddingModel {
  id = "fake";
  dimensions = 2;
  calls = 0;
  bad = false;
  async embed(texts: string[]) {
    this.calls += texts.length;
    if (this.bad) return [[1]];
    return texts.map((t) => (t.includes("cat") ? [1, 0] : [0, 1]));
  }
}

function fakeSource(objects: Record<string, string | Uint8Array>) {
  const rev = new Map<string, number>();
  const source = {
    objects,
    bump: (key: string) => rev.set(key, (rev.get(key) ?? 1) + 1),
    info(key: string): ObjectInfo {
      const body = objects[key]!;
      return {
        key,
        size: body.length,
        etag: `e${rev.get(key) ?? 1}`,
        lastModified: "2026-01-01T00:00:00Z",
      };
    },
    capabilities: () => {
      throw new Error("unused");
    },
    close: async () => {},
    async list(req: { cursor?: string }) {
      const keys = Object.keys(objects).sort();
      const from = Number(req.cursor ?? 0);
      const items = keys.slice(from, from + 2).map((k) => source.info(k));
      return from + 2 < keys.length
        ? { items, nextCursor: String(from + 2) }
        : { items };
    },
    async read(req: { key: string }) {
      const body = objects[req.key];
      if (body === undefined) throw new Error("missing");
      return {
        info: source.info(req.key),
        body: typeof body === "string" ? new TextEncoder().encode(body) : body,
      };
    },
    stat: async (req: { key: string }) => source.info(req.key),
  };
  return source satisfies ObjectSource & Record<string, unknown>;
}

function setup(objects: Record<string, string | Uint8Array>) {
  const source = fakeSource(objects);
  const model = new FakeModel();
  const index = new MemoryIndex();
  const acl = prefixAcl([
    { prefix: "", principals: ["group:everyone"] },
    { prefix: "hr/", principals: ["group:hr"] },
  ]);
  return { source, model, index, deps: { source, model, index, acl } };
}

test("chunks are deterministic, bounded and cover the text", () => {
  const text = "word ".repeat(1000);
  const a = chunkText("k", text);
  assert.deepEqual(a, chunkText("k", text));
  assert.equal(a[0]!.start, 0);
  assert.equal(a.at(-1)!.end, text.length);
  for (const c of a) assert.ok(c.text.length <= 1000);
  for (let i = 1; i < a.length; i++) assert.ok(a[i]!.start < a[i - 1]!.end);
  assert.notEqual(chunkText("other", text)[0]!.id, a[0]!.id);
  assert.throws(() => chunkText("k", "x".repeat(600_000)), {
    code: "too_large",
  });
  assert.throws(() => chunkText("k", "x", { size: 10 }), {
    code: "invalid_request",
  });
});

test("search returns cited spans only to principals on the document", async () => {
  const { deps, index } = setup({
    "a.txt": "a cat sat",
    "hr/pay.txt": "cat salaries",
  });
  const result = await syncCollection(deps, scope);
  assert.deepEqual(result, {
    ingested: 2,
    refreshed: 0,
    removed: 0,
    failed: 0,
  });
  const q = { vector: [1, 0], limit: 10 };
  const everyone = await index.search(scope, {
    ...q,
    principals: ["group:everyone"],
  });
  assert.deepEqual(
    everyone.map((h) => h.key),
    ["a.txt"],
  );
  assert.deepEqual([everyone[0]!.start, everyone[0]!.end], [0, 9]);
  const hr = await index.search(scope, { ...q, principals: ["group:hr"] });
  assert.deepEqual(
    hr.map((h) => h.key),
    ["hr/pay.txt"],
  );
  assert.deepEqual(
    await index.search(
      { ...scope, organizationId: "other" },
      { ...q, principals: ["group:hr"] },
    ),
    [],
  );
});

test("sync re-embeds only changed objects and removes deleted ones", async () => {
  const { deps, model, source } = setup({
    "a.txt": "cat",
    "b.txt": "dog",
    "c.txt": "bird",
  });
  await syncCollection(deps, scope);
  assert.equal(model.calls, 3);
  source.bump("a.txt");
  delete source.objects["c.txt"];
  const result = await syncCollection(deps, scope);
  assert.deepEqual(result, {
    ingested: 1,
    refreshed: 1,
    removed: 1,
    failed: 0,
  });
  assert.equal(model.calls, 4);
});

test("an object whose ACL no longer matches anyone is removed", async () => {
  const { deps, index, source, model } = setup({ "hr/pay.txt": "cat" });
  await syncCollection(deps, scope);
  const narrowed = {
    ...deps,
    acl: prefixAcl([{ prefix: "public/", principals: ["group:everyone"] }]),
  };
  const result = await syncCollection(narrowed, scope);
  assert.equal(result.removed, 1);
  assert.equal((await index.revisions(scope, 10)).size, 0);
  assert.equal(source.objects["hr/pay.txt"], "cat");
  assert.equal(model.calls, 1);
});

test("a listing over the limit changes nothing", async () => {
  const { deps, index } = setup({
    "a.txt": "cat",
    "b.txt": "dog",
    "c.txt": "bird",
  });
  await syncCollection(deps, scope);
  await assert.rejects(syncCollection(deps, scope, { maxObjects: 2 }), {
    code: "too_large",
  });
  assert.equal((await index.revisions(scope, 10)).size, 3);
});

test("unusable vectors and non-text objects are not indexed", async () => {
  const { deps, index, model } = setup({
    "a.txt": "cat",
    "b.txt": new Uint8Array([0xff, 0xfe, 0xfd]),
    "c.bin": "cat",
  });
  model.bad = true;
  const result = await syncCollection(deps, scope);
  assert.deepEqual(result, {
    ingested: 0,
    refreshed: 0,
    removed: 0,
    failed: 2,
  });
  assert.equal((await index.revisions(scope, 10)).size, 0);
  await assert.rejects(
    ingestObject(deps, scope, deps.source.info("a.txt"), []),
    (e) => e instanceof EmbedError && e.code === "invalid_request",
  );
});

test("a changed object that fails to index is removed, not left under its old ACL", async () => {
  const { deps, index, model, source } = setup({ "a.txt": "cat" });
  await syncCollection(deps, scope);
  source.bump("a.txt");
  model.bad = true;
  const result = await syncCollection(deps, scope);
  assert.equal(result.failed, 1);
  assert.equal((await index.revisions(scope, 10)).size, 0);
});

test("a prefix sync leaves documents outside the prefix alone", async () => {
  const { deps, index } = setup({ "a.txt": "cat", "docs/b.txt": "dog" });
  await syncCollection(deps, scope);
  const result = await syncCollection(deps, scope, { prefix: "docs/" });
  assert.equal(result.removed, 0);
  assert.equal((await index.revisions(scope, 10)).size, 2);
});

test("an ACL resolver outage aborts the sync and removes nothing", async () => {
  const { deps, index } = setup({ "a.txt": "cat" });
  await syncCollection(deps, scope);
  const down = {
    ...deps,
    acl: async () => {
      throw new Error("down");
    },
  };
  await assert.rejects(syncCollection(down, scope));
  assert.equal((await index.revisions(scope, 10)).size, 1);
});

test("odd sizes still give integer spans that match the text", () => {
  const text = "ab cd ".repeat(100);
  for (const c of chunkText("k", text, { size: 101, overlap: 10 })) {
    assert.ok(Number.isInteger(c.start) && Number.isInteger(c.end));
    assert.equal(c.text, text.slice(c.start, c.end));
  }
});

test("chunk ids differ between tenants indexing the same text", async () => {
  const { deps, index } = setup({ "a.txt": "cat" });
  await syncCollection(deps, scope);
  const other = { ...scope, organizationId: "globex" };
  await syncCollection(deps, other);
  const ids = (s: Scope) =>
    [...index.docs.entries()]
      .filter(([k]) => k.startsWith(`${s.organizationId}/`))
      .flatMap(([, d]) => d.chunks.map((c) => c.id));
  assert.notDeepEqual(ids(scope), ids(other));
});

test("principals must be user: or group: keys", async () => {
  assert.throws(() => prefixAcl([{ prefix: "", principals: ["everyone"] }]), {
    code: "invalid_request",
  });
});

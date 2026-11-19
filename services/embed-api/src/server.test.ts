import assert from "node:assert/strict";
import { readdirSync, readFileSync } from "node:fs";
import type { AddressInfo } from "node:net";
import { join, resolve } from "node:path";
import { after, before, beforeEach, test } from "node:test";
import {
  type EmbeddingModel,
  EmbedError,
  type Job,
  type Scope,
  type SearchRequest,
} from "@bae6/context-embed";
import AjvModule from "ajv/dist/2020.js";
import { parse } from "yaml";
import type { Identity } from "./identity.js";
import { createEmbedApi } from "./server.js";

const JOB_ID = "0b8b3d9e-6f0e-4a3c-9a55-1f2d3c4b5a69";
const job: Job = {
  id: JOB_ID,
  scope: { organizationId: "acme", collectionId: "kb" },
  prefix: "docs/",
  state: "succeeded",
  attempts: 1,
  result: { ingested: 2, refreshed: 1, removed: 0, failed: 0 },
};

let embedded: string[] = [];
let searches: { scope: Scope; req: SearchRequest }[] = [];
let removed: { scope: Scope; key: string }[] = [];
let ensured: unknown[][] = [];
let modelFails = false;
let ensureFails = false;
let searchMismatch = false;
let pingFails = false;
const model: EmbeddingModel = {
  id: "test-model",
  dimensions: 2,
  embed: async (texts) => {
    embedded.push(...texts);
    if (modelFails)
      throw new EmbedError("model_error", "secret upstream detail");
    return texts.map(() => [1, 0]);
  },
};
const hit = {
  chunkId: "c1",
  key: "docs/a.txt",
  revision: "v1",
  start: 0,
  end: 5,
  text: "hello",
  score: 0.9,
};

const who = (org: string, scopes: string[], sub = "alice"): Identity => ({
  subjectId: sub,
  subjectType: "user",
  organizationId: org,
  applicationId: "app",
  scopes,
});
const identities: Record<string, Identity> = {
  acme: who("acme", ["embed:read", "embed:write"]),
  reader: who("acme", ["embed:read"]),
  writer: who("acme", ["embed:write"]),
  globex: who("globex", ["embed:read", "embed:write"]),
  odd: who("acme", ["embed:read"], "has space"),
};
const logs: Record<string, unknown>[] = [];
const server = createEmbedApi({
  verify: async (t) => (t ? (identities[t] ?? null) : null),
  collections: [
    { id: "kb", organization: "acme", sourcePrefix: "docs/", model },
    { id: "kb", organization: "globex", sourcePrefix: "", model },
  ],
  index: {
    upsert: async () => {},
    refreshAcl: async () => {},
    revisions: async () => new Map(),
    remove: async (scope, key) => void removed.push({ scope, key }),
    search: async (scope, req) => {
      searches.push({ scope, req });
      if (searchMismatch) {
        throw new EmbedError("invalid_request", "dimensions differ");
      }
      return [hit];
    },
    ensureCollection: async (...args) => {
      ensured.push(args);
      if (ensureFails) {
        throw new EmbedError("invalid_request", "different model");
      }
    },
  },
  queue: {
    enqueue: async (scope, prefix) => {
      if (prefix === "docs/full/") throw new EmbedError("too_large", "backlog");
      return { ...job, scope, prefix, state: "queued", attempts: 0 };
    },
    get: async (scope, id) =>
      scope.organizationId === "acme" && id === JOB_ID ? job : undefined,
  },
  principals: (who) => [`user:${who.subjectId}`],
  ping: async () => {
    if (pingFails) throw new Error("db down");
  },
  maxAclAgeMs: 3_600_000,
  maxInflight: 8,
  maxInflightPerOrganization: 2,
  requestTimeoutMs: 5000,
  log: (e) => logs.push(e),
});
let base: string;
before(async () => {
  await new Promise<void>((r) => server.listen(0, "127.0.0.1", r));
  base = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
});
after(() => server.close());
beforeEach(() => {
  embedded = [];
  searches = [];
  removed = [];
  ensured = [];
  modelFails = false;
  ensureFails = false;
  searchMismatch = false;
  pingFails = false;
});

const call = (
  method: string,
  path: string,
  token: string | undefined,
  body?: unknown,
) =>
  fetch(`${base}${path}`, {
    method,
    headers: token ? { authorization: `Bearer ${token}` } : {},
    ...(body === undefined ? {} : { body: JSON.stringify(body) }),
  });
const search = (token: string, body: unknown) =>
  call("POST", "/v1/collections/kb/search", token, body);
const codeOf = async (res: Response) =>
  [res.status, ((await res.json()) as { code: string }).code] as const;

test("refuses missing credentials and the wrong scope for each operation", async () => {
  assert.deepEqual(
    await codeOf(
      await call("POST", "/v1/collections/kb/search", undefined, {}),
    ),
    [401, "unauthenticated"],
  );
  assert.deepEqual(await codeOf(await search("writer", { query: "x" })), [
    403,
    "forbidden",
  ]);
  for (const [method, path] of [
    ["PUT", "/v1/collections/kb"],
    ["POST", "/v1/collections/kb/syncs"],
    ["DELETE", "/v1/collections/kb/documents?key=a.txt"],
  ] as const) {
    assert.deepEqual(await codeOf(await call(method, path, "reader")), [
      403,
      "forbidden",
    ]);
  }
  assert.equal(
    (await call("GET", `/v1/collections/kb/syncs/${JOB_ID}`, "writer")).status,
    403,
  );
  assert.equal(embedded.length + searches.length + removed.length, 0);
});

test("another organization's collection is the same 404 as a missing one", async () => {
  const missing = await call("POST", "/v1/collections/nope/search", "acme", {
    query: "x",
  });
  const noAccess = await call(
    "DELETE",
    "/v1/collections/nope/documents?key=a",
    "acme",
  );
  assert.deepEqual(await codeOf(missing), [404, "not_found"]);
  assert.equal(noAccess.status, 404);
  const own = await search("globex", { query: "x" });
  assert.equal(own.status, 200);
  assert.equal(searches[0]?.scope.organizationId, "globex");
});

test("search ranks only for the verified subject and never takes principals from the body", async () => {
  const res = await search("reader", { query: "hello", limit: 3 });
  assert.equal(res.status, 200);
  assert.deepEqual(await res.json(), { hits: [hit] });
  assert.deepEqual(embedded, ["hello"]);
  assert.deepEqual(searches[0]?.scope, {
    organizationId: "acme",
    collectionId: "kb",
  });
  assert.deepEqual(searches[0]?.req, {
    vector: [1, 0],
    limit: 3,
    principals: ["user:alice"],
    maxAclAgeMs: 3_600_000,
  });
  const forged = await search("reader", {
    query: "hello",
    principals: ["group:admins"],
  });
  assert.deepEqual(await codeOf(forged), [400, "invalid_request"]);
  assert.equal(searches.length, 1);
});

test("a subject that is not a valid principal searches nothing", async () => {
  assert.deepEqual(await codeOf(await search("odd", { query: "x" })), [
    403,
    "forbidden",
  ]);
  assert.equal(embedded.length + searches.length, 0);
});

test("search refuses out-of-bounds input", async () => {
  for (const body of [
    {},
    { query: "" },
    { query: "x".repeat(8001) },
    { query: "x", limit: 0 },
    { query: "x", limit: 51 },
    { query: "x", limit: 1.5 },
    [],
  ]) {
    assert.deepEqual(await codeOf(await search("reader", body)), [
      400,
      "invalid_request",
    ]);
  }
  const big = await search("reader", { query: "x".repeat(70_000) });
  assert.equal(big.status, 413);
  assert.equal(embedded.length, 0);
});

test("a collection that disagrees with the model is the operator's 503, not the caller's 400", async () => {
  searchMismatch = true;
  const res = await search("reader", { query: "x" });
  assert.equal(res.status, 503);
  assert.equal(((await res.json()) as { code: string }).code, "unavailable");
});

test("a failing model is 503 and does not leak its message", async () => {
  modelFails = true;
  const res = await search("reader", { query: "x" });
  assert.equal(res.status, 503);
  assert.equal(res.headers.get("retry-after"), "1");
  assert.doesNotMatch(JSON.stringify(await res.json()), /secret upstream/);
  assert.equal(searches.length, 0);
});

test("a sync is queued for the caller's own collection, with or without a body", async () => {
  const bare = await call("POST", "/v1/collections/kb/syncs", "writer");
  assert.equal(bare.status, 202);
  assert.equal(((await bare.json()) as Job).prefix, "docs/");
  const scoped = await call("POST", "/v1/collections/kb/syncs", "writer", {
    prefix: "docs/guides/",
  });
  assert.equal(((await scoped.json()) as Job).prefix, "docs/guides/");
  for (const body of [
    { prefix: "other/" },
    { prefix: "" },
    { prefix: 1 },
    { prefix: "x".repeat(1025) },
    { extra: 1 },
  ]) {
    assert.deepEqual(
      await codeOf(
        await call("POST", "/v1/collections/kb/syncs", "writer", body),
      ),
      [400, "invalid_request"],
    );
  }
});

test("a full backlog is a 429 with a retry hint", async () => {
  const res = await call("POST", "/v1/collections/kb/syncs", "writer", {
    prefix: "docs/full/",
  });
  assert.deepEqual([res.status, res.headers.get("retry-after")], [429, "30"]);
});

test("a sync is readable only inside its organization", async () => {
  assert.equal(
    (await call("GET", `/v1/collections/kb/syncs/${JOB_ID}`, "reader")).status,
    200,
  );
  for (const [token, id] of [
    ["globex", JOB_ID],
    ["reader", "0b8b3d9e-6f0e-4a3c-9a55-1f2d3c4b5a00"],
    ["reader", "not-a-uuid"],
  ] as const) {
    assert.equal(
      (await call("GET", `/v1/collections/kb/syncs/${id}`, token)).status,
      404,
    );
  }
});

test("deleting a document removes exactly that key from the caller's collection", async () => {
  const res = await call(
    "DELETE",
    "/v1/collections/kb/documents?key=docs%2Fa.txt",
    "writer",
  );
  assert.equal(res.status, 204);
  assert.deepEqual(removed, [
    {
      scope: { organizationId: "acme", collectionId: "kb" },
      key: "docs/a.txt",
    },
  ]);
  for (const q of ["", "?key=", "?key=a&key=b", `?key=${"x".repeat(1025)}`]) {
    assert.equal(
      (await call("DELETE", `/v1/collections/kb/documents${q}`, "writer"))
        .status,
      400,
    );
  }
  assert.equal(removed.length, 1);
});

test("creating a collection provisions the configured model and reports a mismatch as 409", async () => {
  const ok = await call("PUT", "/v1/collections/kb", "writer");
  assert.equal(ok.status, 200);
  assert.deepEqual(ensured[0], [
    { organizationId: "acme", collectionId: "kb" },
    "test-model",
    2,
  ]);
  ensureFails = true;
  assert.deepEqual(
    await codeOf(await call("PUT", "/v1/collections/kb", "writer")),
    [409, "conflict"],
  );
});

test("readiness follows the database and draining", async () => {
  assert.equal((await call("GET", "/health/ready", undefined)).status, 200);
  pingFails = true;
  assert.equal((await call("GET", "/health/ready", undefined)).status, 503);
  assert.equal((await call("GET", "/health/live", undefined)).status, 200);
});

test("one organization cannot take every slot", async () => {
  const release: (() => void)[] = [];
  const original = model.embed;
  model.embed = (texts) =>
    new Promise((ok) => release.push(() => ok(texts.map(() => [1, 0]))));
  try {
    const held = [
      search("reader", { query: "a" }),
      search("reader", { query: "b" }),
    ];
    while (release.length < 2) await new Promise((r) => setTimeout(r, 5));
    const third = await search("reader", { query: "c" });
    assert.deepEqual(
      [third.status, third.headers.get("retry-after")],
      [429, "1"],
    );
    for (const r of release) r();
    const done = await Promise.all(held);
    assert.deepEqual(
      done.map((r) => r.status),
      [200, 200],
    );
  } finally {
    model.embed = original;
  }
});

test("a request leaves no document text in the logs", async () => {
  logs.length = 0;
  await search("reader", { query: "ssn-123-45-6789" });
  await new Promise((r) => setTimeout(r, 20));
  assert.doesNotMatch(JSON.stringify(logs), /ssn-123/);
  const line = logs.find((l) => l.operation === "search");
  assert.equal(line?.organizationId, "acme");
  assert.equal(line?.collectionId, "kb");
});

const contracts = resolve(import.meta.dirname, "../../../contracts");
const spec = parse(
  readFileSync(join(contracts, "embed-api.yaml"), "utf8")
    .replaceAll(
      "./vendor/schemas/",
      "https://github.com/basics-only/bae6-contracts/schemas/",
    )
    .replaceAll(
      "./vendor/openapi/common.yaml#/components/schemas/ErrorEnvelope",
      "https://github.com/basics-only/bae6-contracts/schemas/error-envelope.json",
    ),
);
const ajv = new AjvModule.default({ strict: false, validateFormats: false });
for (const f of readdirSync(join(contracts, "vendor/schemas"))) {
  ajv.addSchema(
    JSON.parse(readFileSync(join(contracts, "vendor/schemas", f), "utf8")),
  );
}
ajv.addSchema(spec, "openapi");
function conforms(schema: string, body: unknown) {
  const validate = ajv.getSchema(`openapi#/components/schemas/${schema}`);
  assert.ok(validate, schema);
  assert.ok(
    validate(body),
    `${schema}: ${JSON.stringify(validate.errors)} ${JSON.stringify(body)}`,
  );
}

test("every documented route is served", async () => {
  const routes = Object.entries(spec.paths as Record<string, object>).flatMap(
    ([path, item]) =>
      "$ref" in item
        ? [["GET", path]]
        : Object.keys(item).map((m) => [m.toUpperCase(), path]),
  );
  assert.equal(routes.length, 7);
  for (const [method, path] of routes) {
    const res = await call(
      method as string,
      (path as string)
        .replace("{collectionId}", "kb")
        .replace("{syncId}", JOB_ID)
        .concat(method === "DELETE" ? "?key=a" : ""),
      "acme",
      method === "POST" ? { query: "x" } : undefined,
    );
    assert.notEqual(res.status, 404, `${method} ${path}`);
  }
});

test("responses conform to the contract", async () => {
  conforms(
    "Collection",
    await (await call("PUT", "/v1/collections/kb", "acme")).json(),
  );
  conforms(
    "Sync",
    await (await call("POST", "/v1/collections/kb/syncs", "acme", {})).json(),
  );
  conforms(
    "Sync",
    await (
      await call("GET", `/v1/collections/kb/syncs/${JOB_ID}`, "acme")
    ).json(),
  );
  conforms("SearchResult", await (await search("acme", { query: "x" })).json());
  conforms("SearchRequest", { query: "x", limit: 50 });
});

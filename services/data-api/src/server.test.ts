import assert from "node:assert/strict";
import { readdirSync, readFileSync } from "node:fs";
import type { AddressInfo } from "node:net";
import { join, resolve } from "node:path";
import { after, before, beforeEach, test } from "node:test";
import {
  DataError,
  type ObjectSource,
  type QueryRequest,
  type QuerySource,
} from "@bae6/context-data";
import AjvModule from "ajv/dist/2020.js";
import { parse } from "yaml";
import type { Identity } from "./identity.js";
import { type BoundSource, createDataApi } from "./server.js";

let queries: QueryRequest[] = [];
let answer: (req: QueryRequest) => Promise<unknown> = async () => result;
const result = {
  columns: [{ name: "id", type: "integer" }],
  rows: [["1"], [null]],
  truncated: false,
  access: "service_principal" as const,
};
const descriptor = (id: string, operations: string[]) => ({
  kind: "data_source" as const,
  id,
  operations: operations as ("query" | "list" | "read" | "stat")[],
  limits: { maxRows: 100 },
  attributes: {},
});
const db: QuerySource = {
  capabilities: () => descriptor("db", ["query"]),
  query: async (req) => {
    queries.push(req);
    return (await answer(req)) as typeof result;
  },
  close: async () => {},
};
const info = {
  key: "acme/a.txt",
  size: 11,
  etag: "e1",
  versionId: "v1",
  lastModified: "2026-01-01T00:00:00.000Z",
};
let slowReads = false;
const finishReads: (() => void)[] = [];
const docs: ObjectSource = {
  capabilities: () => descriptor("docs", ["list", "read", "stat"]),
  list: async (req) => ({
    items: [{ ...info, key: `${req.prefix ?? ""}a.txt` }],
    nextCursor: "c2",
  }),
  read: async (req) => {
    if (slowReads) await new Promise<void>((ok) => finishReads.push(ok));
    const all = Buffer.from("hello world");
    return {
      info,
      body: req.range ? all.subarray(req.range.start, req.range.end + 1) : all,
    };
  },
  stat: async (req) => {
    if (req.key !== info.key)
      throw new DataError("not_found", "No such object.");
    return info;
  },
  close: async () => {},
};
let brokenOpens = 0;
const sources: BoundSource[] = [
  { id: "db", organization: "acme", maxConcurrent: 1, open: async () => db },
  {
    id: "docs",
    organization: "acme",
    maxConcurrent: 4,
    open: async () => docs,
  },
  {
    id: "secret",
    organization: "globex",
    maxConcurrent: 4,
    open: async () => db,
  },
  {
    id: "broken",
    organization: "initech",
    maxConcurrent: 4,
    open: async () => {
      brokenOpens++;
      throw new Error("ECONNREFUSED postgres://user:pw@db");
    },
  },
];

const who = (org: string, scopes = ["data:read"]): Identity => ({
  subjectId: "svc",
  subjectType: "workload",
  organizationId: org,
  applicationId: "app",
  scopes,
});
const identities: Record<string, Identity> = {
  acme: who("acme"),
  globex: who("globex"),
  initech: who("initech"),
  noscope: who("acme", []),
};
const logs: Record<string, unknown>[] = [];
const server = createDataApi({
  verify: async (t) => {
    if (t === "slow") {
      await new Promise((r) => setTimeout(r, 50));
      return identities.acme ?? null;
    }
    return t ? (identities[t] ?? null) : null;
  },
  sources,
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
  queries = [];
  answer = async () => result;
});

const call = (
  method: string,
  path: string,
  token: string | undefined,
  opts: { body?: unknown; headers?: Record<string, string> } = {},
) =>
  fetch(`${base}${path}`, {
    method,
    headers: {
      ...(token ? { authorization: `Bearer ${token}` } : {}),
      ...opts.headers,
    },
    ...(opts.body === undefined ? {} : { body: JSON.stringify(opts.body) }),
  });
const query = (token: string, body: unknown, headers = {}) =>
  call("POST", "/v1/sources/db/query", token, { body, headers });
const codeOf = async (res: Response) =>
  [res.status, ((await res.json()) as { code: string }).code] as const;

test("refuses missing credentials and a missing scope", async () => {
  assert.deepEqual(await codeOf(await call("GET", "/v1/sources", undefined)), [
    401,
    "unauthenticated",
  ]);
  assert.deepEqual(await codeOf(await call("GET", "/v1/sources", "noscope")), [
    403,
    "forbidden",
  ]);
});

test("another organization's source is the same 404 as a missing one", async () => {
  const theirs = await call("POST", "/v1/sources/secret/query", "acme", {
    body: { statement: "SELECT 1" },
  });
  const missing = await call("POST", "/v1/sources/nope/query", "acme", {
    body: { statement: "SELECT 1" },
  });
  const a = (await theirs.json()) as Record<string, unknown>;
  const b = (await missing.json()) as Record<string, unknown>;
  assert.equal(theirs.status, 404);
  assert.equal(missing.status, 404);
  assert.deepEqual({ ...a, requestId: "" }, { ...b, requestId: "" });
  assert.equal(queries.length, 0);
  const listed = (await (await call("GET", "/v1/sources", "acme")).json()) as {
    items: { id: string }[];
  };
  assert.deepEqual(
    listed.items.map((s) => s.id),
    ["db", "docs"],
  );
});

test("a query passes only the documented fields and the source token", async () => {
  const res = await query(
    "acme",
    { statement: "SELECT $1", parameters: [1], maxRows: 5, timeoutMs: 100 },
    { "bae6-source-token": "dbx-user-token" },
  );
  assert.equal(res.status, 200);
  assert.deepEqual(await res.json(), result);
  const [q] = queries;
  assert.deepEqual(
    { ...q, signal: undefined },
    {
      statement: "SELECT $1",
      parameters: [1],
      maxRows: 5,
      timeoutMs: 100,
      delegatedToken: "dbx-user-token",
      signal: undefined,
    },
  );
  for (const body of [
    { statement: "SELECT 1", delegatedToken: "x" },
    { statement: "SELECT 1", signal: null },
    [],
  ]) {
    assert.deepEqual(await codeOf(await query("acme", body)), [
      400,
      "invalid_request",
    ]);
  }
  assert.equal(queries.length, 1);
  const big = await query("acme", { statement: "x".repeat(1024 * 1024) });
  assert.equal(big.status, 413);
});

test("maps source errors to the envelope without inventing detail", async () => {
  for (const [code, status] of [
    ["forbidden", 403],
    ["invalid_request", 400],
    ["too_large", 400],
    ["timeout", 504],
    ["unavailable", 503],
  ] as const) {
    answer = async () => {
      throw new DataError(code, "Safe message.");
    };
    const res = await query("acme", { statement: "SELECT 1" });
    const body = (await res.json()) as { code: string; message: string };
    assert.deepEqual(
      [res.status, body.code, body.message],
      [status, code, "Safe message."],
    );
    if (code === "unavailable")
      assert.equal(res.headers.get("retry-after"), "1");
  }
  answer = async () => {
    throw new TypeError("boom at secret_table");
  };
  const res = await query("acme", { statement: "SELECT 1" });
  assert.deepEqual(await codeOf(res), [500, "internal"]);
});

test("a source that cannot open is 503 for its calls and listed as unavailable", async () => {
  const before = brokenOpens;
  const listed = await call("GET", "/v1/sources", "initech");
  assert.equal(listed.status, 200);
  assert.deepEqual(await listed.json(), { items: [], unavailable: ["broken"] });
  const res = await call("POST", "/v1/sources/broken/query", "initech", {
    body: { statement: "SELECT 1" },
  });
  const body = await res.text();
  assert.equal(res.status, 503);
  assert.doesNotMatch(body, /postgres|pw/);
  assert.equal(brokenOpens - before, 2);
  assert.ok(!JSON.stringify(logs).includes("pw@"));
});

test("a body that never finishes does not hold the source's only slot", async () => {
  const { request } = await import("node:http");
  const stalled = request(`${base}/v1/sources/db/query`, {
    method: "POST",
    headers: { authorization: "Bearer acme", "content-length": "100" },
  });
  stalled.on("error", () => {});
  stalled.write('{"statement":');
  await new Promise((r) => setTimeout(r, 50));
  assert.equal((await query("acme", { statement: "SELECT 1" })).status, 200);
  stalled.destroy();
});

test("one organization cannot take every slot on the replica", async () => {
  slowReads = true;
  const pending = [0, 1].map(() =>
    call("GET", "/v1/sources/docs/objects/content?key=acme%2Fa.txt", "acme"),
  );
  await new Promise((r) => setTimeout(r, 50));
  assert.deepEqual(
    await codeOf(await call("GET", "/v1/sources/docs/objects", "acme")),
    [429, "rate_limited"],
  );
  assert.equal((await call("GET", "/v1/sources", "globex")).status, 200);
  slowReads = false;
  for (const f of finishReads.splice(0)) f();
  for (const r of await Promise.all(pending)) await r.arrayBuffer();
});

test("a client that leaves before admission holds no slot", async () => {
  const { request } = await import("node:http");
  for (let i = 0; i < 3; i++) {
    const dropped = request(`${base}/v1/sources`, {
      headers: { authorization: "Bearer slow" },
    });
    dropped.on("error", () => {});
    dropped.end();
    await new Promise((r) => setTimeout(r, 10));
    dropped.destroy();
  }
  await new Promise((r) => setTimeout(r, 100));
  assert.equal((await call("GET", "/v1/sources", "acme")).status, 200);
});

test("a malformed or repeated source token is refused", async () => {
  for (const token of ["a b", "a, b"]) {
    assert.deepEqual(
      await codeOf(
        await query(
          "acme",
          { statement: "SELECT 1" },
          { "bae6-source-token": token },
        ),
      ),
      [400, "invalid_request"],
    );
  }
  assert.equal(queries.length, 0);
});

test("refuses the wrong operation for a source's kind", async () => {
  assert.deepEqual(
    await codeOf(
      await call("POST", "/v1/sources/docs/query", "acme", {
        body: { statement: "SELECT 1" },
      }),
    ),
    [400, "unsupported_operation"],
  );
  assert.deepEqual(
    await codeOf(await call("GET", "/v1/sources/db/objects", "acme")),
    [400, "unsupported_operation"],
  );
});

test("a source at its concurrency limit answers 429, and a disconnect cancels the call", async () => {
  let release!: () => void;
  let seen!: AbortSignal;
  answer = (req) => {
    seen = req.signal as AbortSignal;
    return new Promise((ok) => {
      release = () => ok(result);
    });
  };
  const client = new AbortController();
  const first = query("acme", { statement: "SELECT pg_sleep(1)" });
  await new Promise((r) => setTimeout(r, 50));
  const busy = await query("acme", { statement: "SELECT 1" });
  assert.deepEqual(await codeOf(busy), [429, "rate_limited"]);
  assert.equal(busy.headers.get("retry-after"), "1");
  release();
  assert.equal((await first).status, 200);

  answer = (req) =>
    new Promise((_, fail) => {
      seen = req.signal as AbortSignal;
      seen.addEventListener("abort", () =>
        fail(new DataError("cancelled", "Cancelled.")),
      );
    });
  const dropped = fetch(`${base}/v1/sources/db/query`, {
    method: "POST",
    headers: { authorization: "Bearer acme" },
    body: JSON.stringify({ statement: "SELECT 1" }),
    signal: client.signal,
  }).catch(() => "aborted");
  await new Promise((r) => setTimeout(r, 50));
  logs.length = 0;
  client.abort();
  assert.equal(await dropped, "aborted");
  await new Promise((r) => setTimeout(r, 50));
  assert.equal(seen.aborted, true);
  assert.ok(logs.some((l) => l.msg === "request" && l.sourceId === "db"));
  answer = async () => result;
  assert.equal((await query("acme", { statement: "SELECT 1" })).status, 200);
});

test("lists, stats and reads objects with ranges", async () => {
  const list = await call(
    "GET",
    "/v1/sources/docs/objects?prefix=acme%2F&limit=10",
    "acme",
  );
  assert.deepEqual(await list.json(), {
    items: [{ ...info, key: "acme/a.txt" }],
    nextCursor: "c2",
  });
  const stat = await call(
    "GET",
    "/v1/sources/docs/objects/metadata?key=acme%2Fa.txt",
    "acme",
  );
  assert.deepEqual(await stat.json(), info);
  assert.equal(
    (await call("GET", "/v1/sources/docs/objects/metadata?key=x", "acme"))
      .status,
    404,
  );
  const whole = await call(
    "GET",
    "/v1/sources/docs/objects/content?key=acme%2Fa.txt",
    "acme",
  );
  assert.equal(whole.status, 200);
  assert.equal(await whole.text(), "hello world");
  assert.equal(whole.headers.get("etag"), '"e1"');
  assert.equal(whole.headers.get("bae6-version-id"), "v1");
  const part = await call(
    "GET",
    "/v1/sources/docs/objects/content?key=acme%2Fa.txt",
    "acme",
    { headers: { range: "bytes=6-10" } },
  );
  assert.equal(part.status, 206);
  assert.equal(await part.text(), "world");
  assert.equal(part.headers.get("content-range"), "bytes 6-10/11");
  for (const [path, headers] of [
    ["/v1/sources/docs/objects/content?key=a", { range: "bytes=6-" }],
    ["/v1/sources/docs/objects/content", {}],
    ["/v1/sources/docs/objects/metadata?key=a&key=b", {}],
    ["/v1/sources/docs/objects?limit=0", {}],
    ["/v1/sources/docs/objects?limit=101", {}],
  ] as const) {
    assert.deepEqual(
      await codeOf(await call("GET", path, "acme", { headers })),
      [400, "invalid_request"],
      path,
    );
  }
});

test("logs who accessed which source, never the statement", async () => {
  logs.length = 0;
  await query("acme", { statement: "SELECT 'ssn-123'" });
  await new Promise((r) => setTimeout(r, 10));
  const line = logs.find((l) => l.msg === "request");
  assert.equal(line?.organizationId, "acme");
  assert.equal(line?.sourceId, "db");
  assert.equal(line?.operation, "query");
  assert.doesNotMatch(JSON.stringify(logs), /ssn-123/);
});

const contracts = resolve(import.meta.dirname, "../../../contracts");
const spec = parse(
  readFileSync(join(contracts, "data-api.yaml"), "utf8")
    // The vendored schemas are the release published under these $ids.
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
  const validate = ajv.getSchema(
    schema.startsWith("https:")
      ? schema
      : `openapi#/components/schemas/${schema}`,
  );
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
      (path as string).replace("{sourceId}", method === "POST" ? "db" : "docs"),
      "acme",
    );
    assert.notEqual(res.status, 404, `${method} ${path}`);
  }
});

test("responses conform to the contract", async () => {
  conforms(
    "SourceList",
    await (await call("GET", "/v1/sources", "acme")).json(),
  );
  conforms(
    "QueryResult",
    await (await query("acme", { statement: "SELECT 1" })).json(),
  );
  conforms(
    "ObjectList",
    await (await call("GET", "/v1/sources/docs/objects", "acme")).json(),
  );
  conforms(
    "ObjectInfo",
    await (
      await call(
        "GET",
        "/v1/sources/docs/objects/metadata?key=acme%2Fa.txt",
        "acme",
      )
    ).json(),
  );
  conforms("QueryRequest", {
    statement: "SELECT :a",
    parameters: { a: 1 },
    maxRows: 10,
  });
  for (const res of [
    await call("GET", "/v1/sources", undefined),
    await call("GET", "/v1/sources", "noscope"),
    await call("GET", "/v1/sources/secret/objects", "acme"),
    await query("acme", { nope: 1 }),
  ]) {
    conforms(
      "https://github.com/basics-only/bae6-contracts/schemas/error-envelope.json",
      await res.json(),
    );
  }
});

import assert from "node:assert/strict";
import { createServer, type IncomingMessage } from "node:http";
import type { AddressInfo } from "node:net";
import { after, beforeEach, test } from "node:test";
import { DataError } from "@bae6/context-data";
import { createDatabricksSource, isReadOnlyStatement } from "./index.js";

interface Seen {
  method: string;
  path: string;
  auth: string;
  body: string;
}
let seen: Seen[] = [];
let handler: (req: Seen) => {
  status?: number;
  json?: unknown;
  delayMs?: number;
} = () => ({ json: {} });

const server = createServer(async (req: IncomingMessage, res) => {
  let body = "";
  for await (const c of req) body += c;
  const s: Seen = {
    method: req.method ?? "",
    path: req.url ?? "",
    auth: req.headers.authorization ?? "",
    body,
  };
  seen.push(s);
  if (s.path === "/oidc/v1/token") {
    res.writeHead(200, { "content-type": "application/json" });
    res.end(JSON.stringify({ access_token: "sp-token", expires_in: 3600 }));
    return;
  }
  const r = handler(s);
  if (r.delayMs) await new Promise((ok) => setTimeout(ok, r.delayMs));
  if (res.destroyed) return;
  res.writeHead(r.status ?? 200, { "content-type": "application/json" });
  res.end(JSON.stringify(r.json ?? {}));
});
await new Promise<void>((ok) => server.listen(0, "127.0.0.1", ok));
const host = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
after(() => server.close());
beforeEach(() => {
  seen = [];
});

const sp = (extra: object = {}) =>
  createDatabricksSource({
    id: "dbx-test",
    host,
    warehouseId: "abc123",
    auth: {
      mode: "service_principal",
      clientId: "cid",
      clientSecret: "secret",
    },
    allowHttp: true,
    ...extra,
  });

const ok = (rows: (string | null)[][], extra: object = {}) => ({
  json: {
    statement_id: "01ef-aa",
    status: { state: "SUCCEEDED" },
    manifest: { schema: { columns: [{ name: "id", type_text: "BIGINT" }] } },
    result: { data_array: rows, ...extra },
  },
});

const code = async (p: Promise<unknown>) => {
  try {
    await p;
  } catch (err) {
    assert.ok(err instanceof DataError, String(err));
    return err.code;
  }
  assert.fail("expected a DataError");
};

const statements = () => seen.filter((s) => s.path.startsWith("/api/"));

test("runs a statement as the service principal with named, typed parameters", async () => {
  handler = () => ok([["1"], ["2"]]);
  const s = sp();
  const r = await s.query({
    statement:
      "SELECT id FROM t WHERE id < :n AND name = :name AND f = :f AND x = :x",
    parameters: { n: 3, name: "bob", f: true, x: null },
    maxRows: 50,
  });
  assert.deepEqual(r, {
    columns: [{ name: "id", type: "BIGINT" }],
    rows: [["1"], ["2"]],
    truncated: false,
    access: "service_principal",
  });
  await s.query({ statement: "SELECT 1" });
  const tokenCalls = seen.filter((x) => x.path === "/oidc/v1/token");
  assert.equal(tokenCalls.length, 1);
  assert.equal(
    tokenCalls[0]?.auth,
    `Basic ${Buffer.from("cid:secret").toString("base64")}`,
  );
  const [first] = statements();
  assert.equal(first?.auth, "Bearer sp-token");
  const body = JSON.parse(first?.body ?? "");
  assert.deepEqual(body.parameters, [
    { name: "n", value: "3", type: "BIGINT" },
    { name: "name", value: "bob", type: "STRING" },
    { name: "f", value: "true", type: "BOOLEAN" },
    { name: "x", type: "STRING" },
  ]);
  assert.equal(body.warehouse_id, "abc123");
  assert.equal(body.row_limit, 51);
  assert.equal(body.on_wait_timeout, "CANCEL");
});

test("follows result chunks and stops at maxRows", async () => {
  handler = (req) =>
    req.method === "POST"
      ? ok([["1"], ["2"]], {
          next_chunk_internal_link:
            "/api/2.0/sql/statements/01ef-aa/result/chunks/1",
        })
      : { json: { data_array: [["3"], ["4"]] } };
  const r = await sp().query({ statement: "SELECT id FROM t", maxRows: 3 });
  assert.deepEqual(r.rows, [["1"], ["2"], ["3"]]);
  assert.equal(r.truncated, true);
});

test("refuses a chunk link that leaves the statement API", async () => {
  for (const link of [
    "https://evil.example/api/2.0/sql/statements/x/result/chunks/1",
    "//evil.example/api/2.0/sql/statements/x/result/chunks/1",
    "/api/2.0/sql/statements/../../../token",
  ]) {
    handler = () => ok([["1"]], { next_chunk_internal_link: link });
    assert.equal(
      await code(sp().query({ statement: "SELECT 1" })),
      "unavailable",
      link,
    );
  }
  assert.equal(seen.filter((s) => s.method === "GET").length, 0);
});

test("delegated mode sends the caller's token and requires one", async () => {
  handler = () => ok([["1"]]);
  const s = sp({ auth: { mode: "delegated" } });
  const r = await s.query({
    statement: "SELECT 1",
    delegatedToken: "user-token",
  });
  assert.equal(r.access, "delegated");
  assert.equal(statements()[0]?.auth, "Bearer user-token");
  assert.equal(seen.filter((x) => x.path === "/oidc/v1/token").length, 0);
  assert.equal(
    await code(s.query({ statement: "SELECT 1" })),
    "invalid_request",
  );
  assert.equal(
    await code(
      sp().query({ statement: "SELECT 1", delegatedToken: "user-token" }),
    ),
    "invalid_request",
  );
});

test("refuses writes and multiple statements without calling the source", async () => {
  for (const statement of [
    "INSERT INTO t VALUES (1)",
    "select 1; drop table t",
    "WITH x AS (SELECT 1) INSERT INTO t SELECT * FROM x",
    "SELECT 1 /* unterminated",
    "SELECT 'unterminated",
    "SET spark.sql.ansi.enabled = false",
    "OPTIMIZE t",
    "MERGE INTO t USING s ON t.id = s.id WHEN MATCHED THEN DELETE",
  ]) {
    assert.equal(await code(sp().query({ statement })), "forbidden", statement);
  }
  assert.equal(statements().length, 0);
});

test("allows reads with comments, quoted keywords and a trailing semicolon", () => {
  for (const sql of [
    "-- insert\nSELECT 1",
    "/* drop */ SELECT 'insert; drop' AS a",
    "SELECT `delete` FROM t;",
    "WITH a AS (SELECT 1) SELECT * FROM a",
    "SHOW TABLES",
    "DESCRIBE TABLE t",
    "SELECT replace(name, 'a', 'b') FROM t",
  ]) {
    assert.equal(isReadOnlyStatement(sql), true, sql);
  }
});

test("maps source failures without passing their messages on", async () => {
  const cases: [ReturnType<typeof handler>, string][] = [
    [{ status: 403 }, "forbidden"],
    [{ status: 400 }, "invalid_request"],
    [{ status: 429 }, "unavailable"],
    [{ status: 503 }, "unavailable"],
    [
      {
        json: {
          status: {
            state: "FAILED",
            error: { error_code: "PERMISSION_DENIED", message: "secret_table" },
          },
        },
      },
      "forbidden",
    ],
    [
      {
        json: {
          status: {
            state: "FAILED",
            error: { error_code: "BAD_REQUEST", message: "near 'ssn=123'" },
          },
        },
      },
      "invalid_request",
    ],
    [{ json: { status: { state: "CANCELED" } } }, "timeout"],
  ];
  for (const [r, expected] of cases) {
    handler = () => r;
    try {
      await sp().query({ statement: "SELECT 1" });
      assert.fail("expected an error");
    } catch (err) {
      assert.ok(err instanceof DataError);
      assert.equal(err.code, expected, JSON.stringify(r));
      assert.doesNotMatch(err.message, /secret_table|ssn/);
    }
  }
});

test("a slow source times out; a caller abort is cancelled", async () => {
  handler = () => ({ ...ok([["1"]]), delayMs: 1000 });
  const started = Date.now();
  assert.equal(
    await code(sp().query({ statement: "SELECT 1", timeoutMs: 100 })),
    "timeout",
  );
  const caller = new AbortController();
  setTimeout(() => caller.abort(), 100);
  assert.equal(
    await code(sp().query({ statement: "SELECT 1", signal: caller.signal })),
    "cancelled",
  );
  assert.ok(Date.now() - started < 900);
});

test("refuses positional parameters and bad parameter names", async () => {
  const s = sp();
  assert.equal(
    await code(s.query({ statement: "SELECT ?", parameters: [1] })),
    "invalid_request",
  );
  assert.equal(
    await code(
      s.query({ statement: "SELECT :a", parameters: { "a; DROP": 1 } }),
    ),
    "invalid_request",
  );
  assert.equal(statements().length, 0);
});

test("refuses a non-https or non-bare host", () => {
  for (const h of [
    "http://dbc.example.com",
    "https://user:pw@dbc.example.com",
    "https://dbc.example.com/path",
  ]) {
    assert.throws(() =>
      createDatabricksSource({
        id: "dbx",
        host: h,
        warehouseId: "abc",
        auth: { mode: "delegated" },
      }),
    );
  }
});

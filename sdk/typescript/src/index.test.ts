import assert from "node:assert/strict";
import { readdirSync, readFileSync } from "node:fs";
import {
  createServer,
  type IncomingMessage,
  type ServerResponse,
} from "node:http";
import type { AddressInfo } from "node:net";
import { join, resolve } from "node:path";
import { after, before, test } from "node:test";
import { inspect } from "node:util";
import {
  ContextApiError,
  ContextError,
  ContextInvalidResponseError,
  ContextTimeoutError,
  ContextTransportError,
  DataClient,
  EmbedClient,
  Transport,
} from "./index.js";

interface Seen {
  method: string;
  url: string;
  headers: IncomingMessage["headers"];
  body: string;
}
let seen: Seen[] = [];
let reply: (res: ServerResponse, n: number) => void = (res) => res.end();
const server = createServer((req, res) => {
  let body = "";
  req.on("data", (c) => {
    body += c;
  });
  req.on("end", () => {
    seen.push({
      method: req.method ?? "",
      url: req.url ?? "",
      headers: req.headers,
      body,
    });
    reply(res, seen.length - 1);
  });
});
let base: string;
before(async () => {
  await new Promise<void>((r) => server.listen(0, "127.0.0.1", r));
  base = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
});
after(() => {
  server.closeAllConnections();
  server.close();
});

interface Case {
  name: string;
  client?: { timeoutMs?: number };
  request: {
    method: string;
    path: string;
    body?: unknown;
    idempotencyKey?: string;
  };
  responses: {
    status?: number;
    headers?: Record<string, string>;
    body?: unknown;
    delayMs?: number;
    drop?: true;
  }[];
  expectRequestCount: number;
  expect: { result?: unknown; error?: Record<string, unknown> };
}

const cases = resolve(
  import.meta.dirname,
  "../../../contracts/vendor/fixtures/transport/cases",
);
for (const file of readdirSync(cases)) {
  const c = JSON.parse(readFileSync(join(cases, file), "utf8")) as Case;
  test(`transport: ${c.name}`, async () => {
    seen = [];
    reply = (res, n) => {
      const r = c.responses[n] ?? c.responses.at(-1);
      if (!r) return res.end();
      if (r.drop) return res.socket?.destroy();
      setTimeout(() => {
        if (res.destroyed) return;
        const raw = typeof r.body === "string";
        res.writeHead(r.status ?? 200, {
          ...(raw ? {} : { "content-type": "application/json" }),
          ...r.headers,
        });
        res.end(raw ? r.body : JSON.stringify(r.body));
      }, r.delayMs ?? 0);
    };
    const t = new Transport({
      baseUrl: base,
      credentials: "svc-token",
      ...(c.client?.timeoutMs ? { timeoutMs: c.client.timeoutMs } : {}),
    });
    const call = t.request(c.request.method, c.request.path, {
      ...(c.request.body === undefined ? {} : { body: c.request.body }),
      ...(c.request.idempotencyKey
        ? { idempotencyKey: c.request.idempotencyKey }
        : {}),
    });
    if (c.expect.error) {
      const err = await call.then(
        () => assert.fail("expected an error"),
        (e: unknown) => e,
      );
      assert.ok(err instanceof ContextError);
      const want = c.expect.error;
      const kind =
        err instanceof ContextApiError
          ? "api"
          : err instanceof ContextTimeoutError
            ? "timeout"
            : err instanceof ContextTransportError
              ? "connection"
              : err instanceof ContextInvalidResponseError
                ? "invalid_response"
                : "unknown";
      assert.equal(kind, want.kind);
      if (err instanceof ContextApiError) {
        for (const k of [
          "status",
          "code",
          "requestId",
          "retryAfterSeconds",
        ] as const) {
          if (k in want) assert.equal(err[k], want[k], k);
        }
      }
      assert.doesNotMatch(`${err} ${err.message} ${inspect(err)}`, /svc-token/);
    } else {
      assert.deepEqual(await call, c.expect.result);
    }
    await new Promise((r) => setTimeout(r, 20));
    assert.equal(seen.length, c.expectRequestCount);
    for (const s of seen) {
      assert.equal(s.headers.authorization, "Bearer svc-token");
      if (s.body) assert.equal(s.headers["content-type"], "application/json");
      if (c.request.idempotencyKey) {
        assert.equal(s.headers["idempotency-key"], c.request.idempotencyKey);
      }
    }
  });
}

const json = (status: number, body: unknown) => (res: ServerResponse) => {
  res.writeHead(status, { "content-type": "application/json" });
  res.end(JSON.stringify(body));
};
const data = () =>
  new DataClient({ baseUrl: `${base}/`, credentials: "svc-token" });

test("a query sends the source and end-user tokens and encodes the source ID", async () => {
  seen = [];
  const result = {
    columns: [],
    rows: [],
    truncated: false,
    access: "delegated",
  };
  reply = json(200, result);
  assert.deepEqual(
    await data().query(
      "a/b",
      { statement: "SELECT :x", parameters: { x: 1 } },
      { sourceToken: "dbx-token", endUserToken: "user-token" },
    ),
    result,
  );
  const [s] = seen;
  assert.equal(s?.url, "/v1/sources/a%2Fb/query");
  assert.equal(s?.headers["bae6-source-token"], "dbx-token");
  assert.equal(s?.headers["bae6-end-user-token"], "user-token");
  assert.deepEqual(JSON.parse(s?.body ?? ""), {
    statement: "SELECT :x",
    parameters: { x: 1 },
  });
  reply = json(200, { rows: "x" });
  await assert.rejects(
    data().query("db", { statement: "SELECT 1" }),
    ContextInvalidResponseError,
  );
});

test("reads a range and reports the whole object's size", async () => {
  seen = [];
  reply = (res) => {
    res.writeHead(206, {
      etag: '"e1"',
      "last-modified": "Thu, 01 Jan 2026 00:00:00 GMT",
      "content-range": "bytes 6-10/11",
      "bae6-version-id": "v1",
    });
    res.end("world");
  };
  const r = await data().readObject("docs", "a b.txt", {
    range: { start: 6, end: 10 },
  });
  assert.equal(Buffer.from(r.body).toString(), "world");
  assert.deepEqual(
    { ...r, body: undefined },
    {
      body: undefined,
      size: 11,
      etag: "e1",
      lastModified: "2026-01-01T00:00:00.000Z",
      versionId: "v1",
    },
  );
  assert.equal(seen[0]?.url, "/v1/sources/docs/objects/content?key=a+b.txt");
  assert.equal(seen[0]?.headers.range, "bytes=6-10");
  reply = (res) => {
    res.writeHead(206, {
      etag: '"e"',
      "last-modified": "Thu, 01 Jan 2026 00:00:00 GMT",
    });
    res.end("x");
  };
  await assert.rejects(
    data().readObject("docs", "a"),
    ContextInvalidResponseError,
  );
});

test("lists and stats objects", async () => {
  seen = [];
  const info = {
    key: "k",
    size: 1,
    etag: "e",
    lastModified: "2026-01-01T00:00:00Z",
  };
  reply = json(200, { items: [info], nextCursor: "c" });
  assert.deepEqual(
    await data().listObjects("docs", { prefix: "p/", limit: 5 }),
    {
      items: [info],
      nextCursor: "c",
    },
  );
  assert.equal(seen[0]?.url, "/v1/sources/docs/objects?prefix=p%2F&limit=5");
  reply = json(200, info);
  assert.deepEqual(
    await data().statObject("docs", "k", { versionId: "v" }),
    info,
  );
  assert.equal(
    seen[1]?.url,
    "/v1/sources/docs/objects/metadata?key=k&versionId=v",
  );
  reply = json(200, {
    items: [{ kind: "data_source", id: "db", operations: ["query"] }],
    unavailable: ["lake"],
  });
  const listed = await data().sources();
  assert.equal(listed.items[0]?.id, "db");
  assert.deepEqual(listed.unavailable, ["lake"]);
});

const embed = () =>
  new EmbedClient({ baseUrl: base, credentials: "svc-token" });
const sync = { id: "s1", state: "queued", prefix: "docs/", attempts: 0 };

test("embed calls hit the documented routes and encode caller-controlled parts", async () => {
  seen = [];
  reply = json(200, { id: "kb", model: "m", dimensions: 8 });
  assert.equal((await embed().createCollection("kb")).dimensions, 8);
  reply = json(202, sync);
  await embed().startSync("kb", { prefix: "docs/" });
  reply = json(200, sync);
  await embed().getSync("kb", "s/1");
  reply = (res) => {
    res.writeHead(204);
    res.end();
  };
  await embed().deleteDocument("kb", "a b&c.txt");
  assert.deepEqual(
    seen.map((s) => [s.method, s.url]),
    [
      ["PUT", "/v1/collections/kb"],
      ["POST", "/v1/collections/kb/syncs"],
      ["GET", "/v1/collections/kb/syncs/s%2F1"],
      ["DELETE", "/v1/collections/kb/documents?key=a+b%26c.txt"],
    ],
  );
  assert.deepEqual(JSON.parse(seen[1]?.body ?? ""), { prefix: "docs/" });
});

test("search returns hits and never sends an end-user token", async () => {
  seen = [];
  const hit = {
    chunkId: "c",
    key: "k",
    revision: "r",
    start: 0,
    end: 1,
    text: "t",
    score: 0.5,
  };
  reply = json(200, { hits: [hit] });
  assert.deepEqual(await embed().search("kb", { query: "q", limit: 3 }), [hit]);
  assert.deepEqual(JSON.parse(seen[0]?.body ?? ""), { query: "q", limit: 3 });
  assert.equal(seen[0]?.headers["bae6-end-user-token"], undefined);
});

test("embed errors surface the envelope, a bad body is invalid and a queued sync is not retried", async () => {
  seen = [];
  reply = json(404, {
    code: "not_found",
    message: "Not found.",
    requestId: "r1",
  });
  await assert.rejects(embed().search("nope", { query: "q" }), {
    status: 404,
    code: "not_found",
    requestId: "r1",
  });
  reply = json(200, { nothits: [] });
  await assert.rejects(
    embed().search("kb", { query: "q" }),
    ContextInvalidResponseError,
  );
  seen = [];
  reply = json(503, { code: "unavailable", message: "x" });
  await assert.rejects(embed().startSync("kb"), ContextApiError);
  assert.equal(seen.length, 1);
});

test("embed refuses dot segments and unknown sync states", async () => {
  seen = [];
  await assert.rejects(embed().getSync("..", "s1"), ContextError);
  await assert.rejects(embed().search(".", { query: "q" }), ContextError);
  assert.equal(seen.length, 0);
  reply = json(200, { ...sync, state: "bogus" });
  await assert.rejects(
    embed().getSync("kb", "s1"),
    ContextInvalidResponseError,
  );
});

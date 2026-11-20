import assert from "node:assert/strict";
import {
  createServer,
  type IncomingMessage,
  type ServerResponse,
} from "node:http";
import type { AddressInfo } from "node:net";
import { after, before, test } from "node:test";
import { createUpstream, UpstreamError } from "./upstream.js";

const VERSION = "2025-06-18";

interface Seen {
  method: string;
  rpc?: { method?: string; params?: Record<string, unknown> };
  headers: IncomingMessage["headers"];
}

const seen: Seen[] = [];
let behave: (rpc: { id?: number; method: string }, res: ServerResponse) => void;

const json = (res: ServerResponse, body: unknown, headers = {}) => {
  res.writeHead(200, { "content-type": "application/json", ...headers });
  res.end(JSON.stringify(body));
};

const happy: typeof behave = (rpc, res) => {
  if (rpc.method === "initialize") {
    return json(
      res,
      { jsonrpc: "2.0", id: rpc.id, result: { protocolVersion: VERSION } },
      { "mcp-session-id": "sess-1" },
    );
  }
  if (rpc.method === "notifications/initialized") {
    res.writeHead(202);
    return res.end();
  }
  json(res, {
    jsonrpc: "2.0",
    id: rpc.id,
    result: { content: [{ type: "text", text: "ok" }] },
  });
};

const server = createServer((req, res) => {
  let raw = "";
  req.on("data", (c) => {
    raw += c;
  });
  req.on("end", () => {
    const rpc = raw ? JSON.parse(raw) : undefined;
    seen.push({
      method: req.method ?? "",
      headers: req.headers,
      ...(rpc ? { rpc } : {}),
    });
    if (req.method === "DELETE") {
      res.writeHead(204);
      return res.end();
    }
    behave(rpc, res);
  });
});
let endpoint: string;

before(async () => {
  await new Promise<void>((r) => server.listen(0, "127.0.0.1", r));
  endpoint = `http://127.0.0.1:${(server.address() as AddressInfo).port}/mcp`;
});
after(() => {
  server.close();
});

const call = (
  overrides: Partial<Parameters<ReturnType<typeof upstream>>[0]> = {},
) =>
  upstream()({
    endpoint,
    protocolVersion: VERSION,
    tool: "echo",
    arguments: { text: "hi" },
    ...overrides,
  });

const upstream = (o: Partial<Parameters<typeof createUpstream>[0]> = {}) =>
  createUpstream({
    policy: { allowInsecure: true },
    timeoutMs: 2000,
    maxResponseBytes: 4096,
    ...o,
  });

async function failure(promise: Promise<unknown>) {
  try {
    await promise;
  } catch (err) {
    assert.ok(err instanceof UpstreamError, String(err));
    return err.reason;
  }
  assert.fail("expected a failure");
}

test("performs the handshake, calls the tool, sends no credential and closes the session", async () => {
  behave = happy;
  seen.length = 0;
  const result = await call();
  assert.deepEqual(result, { content: [{ type: "text", text: "ok" }] });
  assert.deepEqual(
    seen.map((s) => s.rpc?.method ?? s.method),
    ["initialize", "notifications/initialized", "tools/call", "DELETE"],
  );
  assert.deepEqual(seen[2]?.rpc?.params, {
    name: "echo",
    arguments: { text: "hi" },
  });
  assert.ok(seen.every((s) => s.headers.authorization === undefined));
  assert.equal(seen[2]?.headers["mcp-session-id"], "sess-1");
  assert.equal(seen[2]?.headers["mcp-protocol-version"], VERSION);
});

test("reads a result sent as an event stream after other messages", async () => {
  behave = (rpc, res) => {
    if (rpc.method !== "tools/call") return happy(rpc, res);
    res.writeHead(200, { "content-type": "text/event-stream" });
    res.end(
      `data: ${JSON.stringify({ jsonrpc: "2.0", method: "notifications/progress", params: {} })}\n\n` +
        `data: ${JSON.stringify({ jsonrpc: "2.0", id: rpc.id, result: { isError: true } })}\n\n`,
    );
  };
  assert.deepEqual(await call(), { isError: true });
});

test("refuses replies it cannot trust", async () => {
  const cases: [string, typeof behave][] = [
    [
      "bad_response",
      (rpc, res) =>
        rpc.method === "initialize"
          ? json(res, {
              jsonrpc: "2.0",
              id: rpc.id,
              result: { protocolVersion: "1999-01-01" },
            })
          : happy(rpc, res),
    ],
    [
      "bad_response",
      (rpc, res) => {
        if (rpc.method !== "tools/call") return happy(rpc, res);
        json(res, { jsonrpc: "2.0", id: 99, method: "sampling/createMessage" });
      },
    ],
    [
      "bad_response",
      (rpc, res) => {
        if (rpc.method !== "tools/call") return happy(rpc, res);
        res.writeHead(302, { location: "http://127.0.0.1:1/" });
        res.end();
      },
    ],
    [
      "bad_response",
      (rpc, res) => {
        if (rpc.method !== "tools/call") return happy(rpc, res);
        res.writeHead(200, { "content-type": "text/html" });
        res.end("<p>hi</p>");
      },
    ],
    [
      "rejected",
      (rpc, res) =>
        rpc.method === "tools/call"
          ? json(res, {
              jsonrpc: "2.0",
              id: rpc.id,
              error: { code: -32602, message: "secret detail" },
            })
          : happy(rpc, res),
    ],
    [
      "too_large",
      (rpc, res) =>
        rpc.method === "tools/call"
          ? json(res, { jsonrpc: "2.0", id: rpc.id, result: "x".repeat(5000) })
          : happy(rpc, res),
    ],
  ];
  for (const [reason, run] of cases) {
    behave = run;
    assert.equal(await failure(call()), reason);
  }
});

test("a server that never answers times out", async () => {
  behave = () => {};
  assert.equal(
    await failure(
      upstream({ timeoutMs: 150 })({
        endpoint,
        protocolVersion: VERSION,
        tool: "echo",
        arguments: {},
      }),
    ),
    "timeout",
  );
});

test("endpoints outside the egress policy are blocked before any request", async () => {
  seen.length = 0;
  assert.equal(
    await failure(
      createUpstream({
        policy: {},
        timeoutMs: 500,
        maxResponseBytes: 4096,
      })({ endpoint, protocolVersion: VERSION, tool: "t", arguments: {} }),
    ),
    "blocked",
  );
  assert.equal(seen.length, 0);
});

test("a public name that resolves to a private address is blocked", async () => {
  seen.length = 0;
  const resolve = (
    _host: string,
    done: (err: null, found: { address: string; family: number }[]) => void,
  ) => done(null, [{ address: "127.0.0.1", family: 4 }]);
  assert.equal(
    await failure(
      createUpstream({
        policy: {},
        resolve,
        timeoutMs: 500,
        maxResponseBytes: 4096,
      })({
        endpoint: "https://tools.example.com/mcp",
        protocolVersion: VERSION,
        tool: "t",
        arguments: {},
      }),
    ),
    "blocked",
  );
  assert.equal(seen.length, 0);
});

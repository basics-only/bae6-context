import assert from "node:assert/strict";
import {
  createServer,
  type IncomingMessage,
  type ServerResponse,
} from "node:http";
import type { AddressInfo } from "node:net";
import { test } from "node:test";
import { createProbe, type Resolver, type Version } from "./index.js";

type Handler = (req: IncomingMessage, res: ServerResponse) => void;

async function serve(handler: Handler) {
  const hits: string[] = [];
  const server = createServer((req, res) => {
    hits.push(`${req.method} ${req.url}`);
    handler(req, res);
  });
  await new Promise<void>((ok) => server.listen(0, "127.0.0.1", ok));
  const origin = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
  return {
    origin,
    hits,
    close: () => {
      server.closeAllConnections();
      server.close();
    },
  };
}

const dev = { allowInsecure: true };

const versionAt = (origin: string, oauth = false): Version => ({
  version: "1.0.0",
  endpoint: `${origin}/mcp`,
  protocolVersion: "2025-06-18",
  transport: "streamable-http",
  authentication: oauth
    ? { method: "oauth2", resource: `${origin}/mcp`, requiredScopes: [] }
    : { method: "none" },
  tools: [],
  resources: [],
  prompts: [],
  allowedNetworkZones: [],
  state: "approved",
  scanStatus: "unscanned",
  publishedBy: "alice",
  publishedAt: "2026-11-18T10:00:00Z",
});

const metadata =
  (resource: string): Handler =>
  (req, res) => {
    if (req.url === "/.well-known/oauth-protected-resource/mcp") {
      res.writeHead(200, { "content-type": "application/json" });
      return void res.end(JSON.stringify({ resource }));
    }
    res.writeHead(401).end();
  };

test("an endpoint that answers, even with 401, is healthy", async () => {
  const s = await serve((_req, res) => res.writeHead(401).end());
  const h = await createProbe(dev)(versionAt(s.origin));
  s.close();
  assert.equal(h.status, "healthy");
  assert.equal(h.reason, undefined);
});

test("a 5xx answer is unhealthy", async () => {
  const s = await serve((_req, res) => res.writeHead(503).end());
  const h = await createProbe(dev)(versionAt(s.origin));
  s.close();
  assert.deepEqual([h.status, h.reason], ["unhealthy", "bad_status"]);
});

test("a redirect is reported and never followed", async () => {
  const target = await serve((_req, res) => res.writeHead(200).end());
  const s = await serve((_req, res) =>
    res.writeHead(302, { location: `${target.origin}/secret` }).end(),
  );
  const h = await createProbe(dev)(versionAt(s.origin));
  s.close();
  target.close();
  assert.equal(h.reason, "redirected");
  assert.deepEqual(target.hits, []);
});

test("an oauth2 server must publish the declared resource", async () => {
  let origin = "";
  const s = await serve((req, res) => metadata(`${origin}/mcp`)(req, res));
  origin = s.origin;
  const h = await createProbe(dev)(versionAt(s.origin, true));
  s.close();
  assert.equal(h.status, "healthy");
});

test("metadata naming a different resource is a mismatch", async () => {
  const s = await serve(metadata("https://other.example.com/mcp"));
  const h = await createProbe(dev)(versionAt(s.origin, true));
  s.close();
  assert.equal(h.reason, "resource_mismatch");
});

test("missing, unparsable or oversized metadata is unhealthy", async () => {
  const cases: [Handler, string][] = [
    [(_req, res) => res.writeHead(401).end(), "bad_status"],
    [
      (req, res) =>
        req.url === "/mcp"
          ? res.writeHead(401).end()
          : res.writeHead(200).end("<html>"),
      "invalid_metadata",
    ],
    [
      (req, res) =>
        req.url === "/mcp"
          ? res.writeHead(401).end()
          : res.writeHead(200).end(JSON.stringify({ resource: 5 })),
      "invalid_metadata",
    ],
    [
      (req, res) =>
        req.url === "/mcp"
          ? res.writeHead(401).end()
          : res.writeHead(200).end("x".repeat(70 * 1024)),
      "invalid_metadata",
    ],
  ];
  for (const [handler, reason] of cases) {
    const s = await serve(handler);
    const h = await createProbe(dev)(versionAt(s.origin, true));
    s.close();
    assert.equal(h.reason, reason);
  }
});

test("a server that never answers times out", async () => {
  const s = await serve(() => {});
  const h = await createProbe(dev, { timeoutMs: 100 })(versionAt(s.origin));
  s.close();
  assert.equal(h.reason, "timeout");
});

test("a closed port is unreachable", async () => {
  const s = await serve((_req, res) => res.end());
  s.close();
  const h = await createProbe(dev)(versionAt(s.origin));
  assert.equal(h.reason, "unreachable");
});

test("an endpoint resolving to a private address is blocked before any connection", async () => {
  const toPrivate: Resolver = (_host, done) =>
    done(null, [{ address: "127.0.0.1", family: 4 }]);
  const v = versionAt("https://mcp.example.com");
  const h = await createProbe({}, { resolve: toPrivate })(v);
  assert.deepEqual([h.status, h.reason], ["unhealthy", "endpoint_blocked"]);
});

test("an endpoint the policy no longer allows is blocked", async () => {
  const h = await createProbe({ allowedHostSuffixes: ["corp.example"] })(
    versionAt("https://mcp.example.com"),
  );
  assert.equal(h.reason, "endpoint_blocked");
});

test("the probe sends no credentials", async () => {
  let seen: IncomingMessage["headers"] = {};
  const s = await serve((req, res) => {
    seen = req.headers;
    res.writeHead(200).end();
  });
  await createProbe(dev)(versionAt(s.origin));
  s.close();
  assert.equal(seen.authorization, undefined);
  assert.equal(seen.cookie, undefined);
});

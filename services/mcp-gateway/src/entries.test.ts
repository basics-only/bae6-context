import assert from "node:assert/strict";
import { createServer } from "node:http";
import type { AddressInfo } from "node:net";
import { after, before, test } from "node:test";
import { createEntries, RegistryUnavailable } from "./entries.js";

const requests: { path?: string; authorization?: string }[] = [];
let respond: (path: string) => { status: number; body: string } = () => ({
  status: 404,
  body: "{}",
});

const registry = createServer((req, res) => {
  requests.push({
    ...(req.url ? { path: req.url } : {}),
    ...(req.headers.authorization
      ? { authorization: req.headers.authorization }
      : {}),
  });
  const { status, body } = respond(req.url ?? "");
  res.writeHead(status, { "content-type": "application/json" });
  res.end(body);
});
let url: string;
before(async () => {
  await new Promise<void>((r) => registry.listen(0, "127.0.0.1", r));
  url = `http://127.0.0.1:${(registry.address() as AddressInfo).port}`;
});
after(() => {
  registry.closeAllConnections();
  registry.close();
});

const server = (over: Record<string, unknown> = {}) =>
  JSON.stringify({
    id: "tools",
    organization: "acme",
    team: "t",
    name: "Tools",
    createdAt: "2026-11-01T00:00:00.000Z",
    versions: [],
    ...over,
  });

let clock = 0;
const entries = (over: Partial<Parameters<typeof createEntries>[0]> = {}) =>
  createEntries({
    registryUrl: url,
    token: async (org) => (org === "acme" ? "acme-token" : undefined),
    ttlMs: 1000,
    timeoutMs: 1000,
    maxBytes: 4096,
    maxEntries: 2,
    now: () => clock,
    ...over,
  });

const reset = () => {
  requests.length = 0;
  clock = 0;
  respond = () => ({ status: 200, body: server() });
};

test("asks the registry with the organization's own token and caches the answer until the TTL ends", async () => {
  reset();
  const get = entries();
  assert.equal((await get("acme", "tools"))?.id, "tools");
  assert.equal((await get("acme", "tools"))?.id, "tools");
  assert.equal(requests.length, 1);
  assert.deepEqual(requests[0], {
    path: "/v1/servers/tools",
    authorization: "Bearer acme-token",
  });
  clock = 1001;
  await get("acme", "tools");
  assert.equal(requests.length, 2);
});

test("an organization without a registry token is served nothing and the registry is not asked", async () => {
  reset();
  assert.equal(await entries()("globex", "tools"), null);
  assert.equal(requests.length, 0);
});

test("a reply for another organization is refused, never returned", async () => {
  reset();
  respond = () => ({ status: 200, body: server({ organization: "globex" }) });
  await assert.rejects(entries()("acme", "tools"), RegistryUnavailable);
});

test("a reply for another server is refused", async () => {
  reset();
  respond = () => ({ status: 200, body: server({ id: "other" }) });
  await assert.rejects(entries()("acme", "tools"), RegistryUnavailable);
});

test("not found is null and is not cached", async () => {
  reset();
  respond = () => ({ status: 404, body: "{}" });
  const get = entries();
  assert.equal(await get("acme", "tools"), null);
  assert.equal(await get("acme", "tools"), null);
  assert.equal(requests.length, 2);
});

test("registry errors, junk and oversized replies fail instead of looking like not found", async () => {
  const bad: [number, string][] = [
    [500, "{}"],
    [401, "{}"],
    [200, "not json"],
    [200, server({ pad: "x".repeat(5000) })],
  ];
  for (const [status, body] of bad) {
    reset();
    respond = () => ({ status, body });
    await assert.rejects(
      entries()("acme", "tools"),
      RegistryUnavailable,
      `${status} ${body.slice(0, 10)}`,
    );
  }
});

test("a revoked entry is gone once the TTL ends", async () => {
  reset();
  const get = entries();
  await get("acme", "tools");
  respond = () => ({ status: 404, body: "{}" });
  assert.ok(await get("acme", "tools"));
  clock = 1001;
  assert.equal(await get("acme", "tools"), null);
});

test("the cache holds at most maxEntries", async () => {
  reset();
  const get = entries();
  respond = (path) => ({
    status: 200,
    body: server({ id: path.split("/").pop() }),
  });
  for (const id of ["a", "b", "c"]) await get("acme", id);
  requests.length = 0;
  await get("acme", "a");
  assert.equal(requests.length, 1);
});

import assert from "node:assert/strict";
import { test } from "node:test";
import {
  type Caller,
  type Health,
  MemoryStore,
  Registry,
  RegistryError,
} from "./index.js";

const alice: Caller = { subject: "alice", actor: "alice", requestId: "r1" };
const bob: Caller = {
  subject: "bob",
  actor: "svc for bob",
  requestId: "r2",
};

const version = (over: Record<string, unknown> = {}) => ({
  version: "1.0.0",
  endpoint: "https://mcp.example.com/mcp",
  protocolVersion: "2025-06-18",
  transport: "streamable-http",
  authentication: {
    method: "oauth2",
    resource: "https://mcp.example.com/mcp",
    requiredScopes: ["tools.read"],
  },
  tools: [{ name: "search", description: "Search" }],
  ...over,
});

const server = (over: Record<string, unknown> = {}) => ({
  id: "docs",
  team: "platform",
  name: "Docs",
  version: version(),
  ...over,
});

function setup() {
  const store = new MemoryStore();
  let now = Date.parse("2026-11-18T10:00:00Z");
  const registry = new Registry(store, {}, () => new Date(now));
  return {
    store,
    registry,
    advance: (ms: number) => {
      now += ms;
    },
  };
}

const code = (c: string) => (e: unknown) =>
  e instanceof RegistryError && e.code === c;

test("a published version is pending and hidden from discovery until approved", async () => {
  const { registry } = setup();
  const created = await registry.createServer("acme", server(), alice);
  assert.equal(created.versions[0]?.state, "pending");
  await assert.rejects(registry.get("acme", "docs", false), code("not_found"));
  assert.equal((await registry.get("acme", "docs", true)).versions.length, 1);

  const approved = await registry.approve(
    "acme",
    "docs",
    { version: "1.0.0", scanStatus: "clean" },
    bob,
  );
  assert.equal(approved.versions[0]?.state, "approved");
  assert.equal(approved.versions[0]?.decidedBy, "bob");
  const seen = await registry.get("acme", "docs", false);
  assert.equal(seen.versions[0]?.scanStatus, "clean");
});

test("a publisher cannot approve their own version", async () => {
  const { registry } = setup();
  await registry.createServer("acme", server(), alice);
  await assert.rejects(
    registry.approve("acme", "docs", { version: "1.0.0" }, alice),
    code("forbidden"),
  );
});

test("another organization sees neither the server nor its name in listings", async () => {
  const { registry } = setup();
  await registry.createServer("acme", server(), alice);
  await registry.approve("acme", "docs", { version: "1.0.0" }, bob);
  await assert.rejects(registry.get("globex", "docs", true), code("not_found"));
  assert.deepEqual(
    (await registry.list("globex", { limit: 10 }, true)).items,
    [],
  );
  await assert.rejects(
    registry.approve("globex", "docs", { version: "1.0.0" }, bob),
    code("not_found"),
  );
  await assert.rejects(
    registry.revoke("globex", "docs", {}, bob),
    code("not_found"),
  );
  await assert.rejects(
    registry.addVersion("globex", "docs", version({ version: "2" }), alice),
    code("not_found"),
  );
  assert.equal((await registry.get("acme", "docs", true)).versions.length, 1);
});

test("the same server ID can exist in two organizations", async () => {
  const { registry } = setup();
  await registry.createServer("acme", server(), alice);
  await registry.createServer("globex", server({ name: "Other" }), alice);
  assert.equal((await registry.get("globex", "docs", true)).name, "Other");
});

test("a revoked version is gone from discovery and cannot return", async () => {
  const { registry, store } = setup();
  await registry.createServer("acme", server(), alice);
  await registry.approve("acme", "docs", { version: "1.0.0" }, bob);
  await registry.revoke("acme", "docs", { version: "1.0.0" }, bob);
  await assert.rejects(registry.get("acme", "docs", false), code("not_found"));
  assert.deepEqual(
    (await registry.list("acme", { limit: 10 }, false)).items,
    [],
  );
  await assert.rejects(
    registry.approve("acme", "docs", { version: "1.0.0" }, bob),
    code("conflict"),
  );
  const audits = store.audit.length;
  await registry.revoke("acme", "docs", { version: "1.0.0" }, bob);
  assert.equal(store.audit.length, audits);
  assert.equal(
    (await registry.get("acme", "docs", true)).versions[0]?.state,
    "revoked",
  );
});

test("revoking without a version revokes every version", async () => {
  const { registry } = setup();
  await registry.createServer("acme", server(), alice);
  await registry.addVersion(
    "acme",
    "docs",
    version({ version: "2.0.0" }),
    alice,
  );
  await registry.approve("acme", "docs", { version: "2.0.0" }, bob);
  const out = await registry.revoke("acme", "docs", {}, bob);
  assert.deepEqual(
    out.versions.map((v) => v.state),
    ["revoked", "revoked"],
  );
});

test("an unrevoked older version stays discoverable when a newer one is revoked", async () => {
  const { registry } = setup();
  await registry.createServer("acme", server(), alice);
  await registry.addVersion(
    "acme",
    "docs",
    version({ version: "2.0.0" }),
    alice,
  );
  await registry.approve("acme", "docs", { version: "1.0.0" }, bob);
  await registry.approve("acme", "docs", { version: "2.0.0" }, bob);
  await registry.revoke("acme", "docs", { version: "2.0.0" }, bob);
  const seen = await registry.get("acme", "docs", false);
  assert.deepEqual(
    seen.versions.map((v) => v.version),
    ["1.0.0"],
  );
});

test("approving an approved version succeeds without a second audit event", async () => {
  const { registry, store } = setup();
  await registry.createServer("acme", server(), alice);
  await registry.approve("acme", "docs", { version: "1.0.0" }, bob);
  const audits = store.audit.length;
  await registry.approve("acme", "docs", { version: "1.0.0" }, bob);
  assert.equal(store.audit.length, audits);
});

test("duplicate servers and versions are conflicts, and versions are capped", async () => {
  const { registry } = setup();
  await registry.createServer("acme", server(), alice);
  await assert.rejects(
    registry.createServer("acme", server(), alice),
    code("conflict"),
  );
  await assert.rejects(
    registry.addVersion("acme", "docs", version(), alice),
    code("conflict"),
  );
  for (let i = 2; i <= 50; i++) {
    await registry.addVersion(
      "acme",
      "docs",
      version({ version: `${i}` }),
      alice,
    );
  }
  await assert.rejects(
    registry.addVersion("acme", "docs", version({ version: "51" }), alice),
    code("conflict"),
  );
});

test("every change is audited with the actor and request", async () => {
  const { registry, store } = setup();
  await registry.createServer("acme", server(), alice);
  await registry.approve("acme", "docs", { version: "1.0.0" }, bob);
  await registry.revoke("acme", "docs", {}, bob);
  assert.deepEqual(
    store.audit.map((e) => [e.action, e.actor, e.requestId, e.organization]),
    [
      ["servers.create", "alice", "r1", "acme"],
      ["servers.approve", "svc for bob", "r2", "acme"],
      ["servers.revoke", "svc for bob", "r2", "acme"],
    ],
  );
  assert.ok(store.audit.every((e) => /^[a-z_]+(\.[a-z_]+)+$/.test(e.action)));
});

test("a rejected change stores nothing", async () => {
  const { registry, store } = setup();
  await assert.rejects(
    registry.createServer("acme", server({ team: "Bad Team" }), alice),
    code("invalid_request"),
  );
  assert.equal(store.audit.length, 0);
  await assert.rejects(registry.get("acme", "docs", true), code("not_found"));
});

test("listing pages by ID and reports where the next page starts", async () => {
  const { registry } = setup();
  for (const id of ["c", "a", "b"]) {
    await registry.createServer("acme", server({ id }), alice);
  }
  const first = await registry.list("acme", { limit: 2 }, true);
  assert.deepEqual(
    first.items.map((s) => s.id),
    ["a", "b"],
  );
  assert.equal(first.next, "b");
  const second = await registry.list(
    "acme",
    { limit: 2, after: first.next as string },
    true,
  );
  assert.deepEqual(
    second.items.map((s) => s.id),
    ["c"],
  );
  assert.equal(second.next, undefined);
});

test("discover-only listing skips servers with nothing approved", async () => {
  const { registry } = setup();
  await registry.createServer("acme", server({ id: "a" }), alice);
  await registry.createServer("acme", server({ id: "b" }), alice);
  await registry.approve("acme", "b", { version: "1.0.0" }, bob);
  const out = await registry.list("acme", { limit: 10 }, false);
  assert.deepEqual(
    out.items.map((s) => s.id),
    ["b"],
  );
});

test("health is probed for approved versions only and reused for 30 seconds", async () => {
  const { registry, advance } = setup();
  await registry.createServer("acme", server(), alice);
  let probes = 0;
  const probe = async (v: { version: string }): Promise<Health> => {
    probes++;
    return {
      version: v.version,
      status: "healthy",
      checkedAt: new Date(Date.parse("2026-11-18T10:00:00Z")).toISOString(),
    };
  };
  await assert.rejects(
    registry.health("acme", "docs", undefined, probe),
    code("not_found"),
  );
  await registry.approve("acme", "docs", { version: "1.0.0" }, bob);
  await registry.health("acme", "docs", undefined, probe);
  await registry.health("acme", "docs", "1.0.0", probe);
  assert.equal(probes, 1);
  advance(31_000);
  await registry.health("acme", "docs", undefined, probe);
  assert.equal(probes, 2);
  await assert.rejects(
    registry.health("acme", "docs", "9.9.9", probe),
    code("not_found"),
  );
  await assert.rejects(
    registry.health("globex", "docs", undefined, probe),
    code("not_found"),
  );
});

const rejected: [string, Record<string, unknown>][] = [
  ["http endpoint", { endpoint: "http://mcp.example.com/mcp" }],
  ["credentials in the URL", { endpoint: "https://u:p@mcp.example.com/" }],
  ["query string", { endpoint: "https://mcp.example.com/mcp?key=abc" }],
  ["fragment", { endpoint: "https://mcp.example.com/mcp#x" }],
  ["IPv4 literal", { endpoint: "https://10.0.0.5/mcp" }],
  ["decimal IPv4 literal", { endpoint: "https://2130706433/mcp" }],
  ["IPv6 literal", { endpoint: "https://[::1]/mcp" }],
  ["localhost", { endpoint: "https://localhost/mcp" }],
  ["single-label host", { endpoint: "https://intranet/mcp" }],
  ["non-default port", { endpoint: "https://mcp.example.com:8443/mcp" }],
  ["unknown field", { extra: true }],
  ["bad protocol version", { protocolVersion: "latest" }],
  ["stdio transport", { transport: "stdio" }],
  ["duplicate tool names", { tools: [{ name: "a" }, { name: "a" }] }],
  [
    "too many tools",
    { tools: Array.from({ length: 201 }, (_, i) => ({ name: `t${i}` })) },
  ],
  ["control character in name", { tools: [{ name: "a\u0000b" }] }],
  [
    "resource on another origin",
    {
      authentication: {
        method: "oauth2",
        resource: "https://other.example.com/mcp",
      },
    },
  ],
  ["oauth2 without a resource", { authentication: { method: "oauth2" } }],
  [
    "none with a resource",
    {
      authentication: { method: "none", resource: "https://mcp.example.com/" },
    },
  ],
  ["a token field", { authentication: { method: "none", token: "x" } }],
  [
    "scope with whitespace",
    {
      authentication: {
        method: "oauth2",
        resource: "https://mcp.example.com/mcp",
        requiredScopes: ["a b"],
      },
    },
  ],
  ["bad zone", { allowedNetworkZones: ["Prod Zone"] }],
];

for (const [name, over] of rejected) {
  test(`rejects a version with ${name}`, async () => {
    const { registry } = setup();
    await assert.rejects(
      registry.createServer("acme", server({ version: version(over) }), alice),
      code("invalid_request"),
    );
  });
}

test("rejects a body that carries a __proto__ key", async () => {
  const { registry } = setup();
  await assert.rejects(
    registry.createServer(
      "acme",
      JSON.parse('{"id":"docs","team":"t","name":"n","__proto__":{"x":1}}'),
      alice,
    ),
    code("invalid_request"),
  );
});

test("the egress policy limits endpoints to allowed host suffixes", async () => {
  const registry = new Registry(new MemoryStore(), {
    allowedHostSuffixes: ["example.com"],
  });
  await registry.createServer("acme", server(), alice);
  await assert.rejects(
    registry.createServer(
      "acme",
      server({
        id: "evil",
        version: version({
          endpoint: "https://notexample.com/mcp",
          authentication: { method: "none" },
        }),
      }),
      alice,
    ),
    code("invalid_request"),
  );
});

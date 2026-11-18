import assert from "node:assert/strict";
import { after, before, test } from "node:test";
import { type Caller, Registry, RegistryError } from "@bae6/context-mcp";
import pg from "pg";
import { PgRegistryStore } from "./index.js";

const url = process.env.BAE6_MCP_DATABASE_URL;
const skip = url ? false : "BAE6_MCP_DATABASE_URL not set";

const alice: Caller = { subject: "alice", actor: "alice", requestId: "r1" };
const bob: Caller = { subject: "bob", actor: "bob", requestId: "r2" };

const body = (id: string, version = "1.0.0") => ({
  id,
  team: "platform",
  name: id,
  version: {
    version,
    endpoint: "https://mcp.example.com/mcp",
    protocolVersion: "2025-06-18",
    transport: "streamable-http",
    authentication: { method: "none" },
  },
});

const pool = new pg.Pool({ connectionString: url, max: 8 });
const registry = new Registry(new PgRegistryStore(pool));
const store = new PgRegistryStore(pool);
const code = (c: string) => (e: unknown) =>
  e instanceof RegistryError && e.code === c;

before(async () => {
  if (url)
    await Promise.all([1, 2, 3].map(() => PgRegistryStore.migrate(pool)));
});
after(() => pool.end());

test(
  "a version is hidden until approved and gone once revoked",
  { skip },
  async () => {
    await registry.createServer("acme", body("life"), alice);
    await assert.rejects(
      registry.get("acme", "life", false),
      code("not_found"),
    );
    await registry.approve("acme", "life", { version: "1.0.0" }, bob);
    const seen = await registry.get("acme", "life", false);
    assert.equal(seen.versions[0]?.decidedBy, "bob");
    await registry.revoke("acme", "life", {}, bob);
    await assert.rejects(
      registry.get("acme", "life", false),
      code("not_found"),
    );
    assert.equal(
      (await registry.get("acme", "life", true)).versions[0]?.state,
      "revoked",
    );
  },
);

test(
  "another organization cannot see or change the server",
  { skip },
  async () => {
    await registry.createServer("acme", body("tenant"), alice);
    await registry.approve("acme", "tenant", { version: "1.0.0" }, bob);
    await assert.rejects(
      registry.get("globex", "tenant", true),
      code("not_found"),
    );
    await assert.rejects(
      registry.revoke("globex", "tenant", {}, bob),
      code("not_found"),
    );
    assert.deepEqual(
      (await registry.list("globex", { limit: 100 }, true)).items,
      [],
    );
    await registry.createServer("globex", body("tenant"), alice);
  },
);

test(
  "two creators of one server: one wins, the other conflicts",
  { skip },
  async () => {
    const results = await Promise.allSettled(
      [alice, bob, alice, bob].map((who) =>
        registry.createServer("acme", body("race"), who),
      ),
    );
    assert.equal(results.filter((r) => r.status === "fulfilled").length, 1);
    for (const r of results) {
      if (r.status === "rejected") assert.ok(code("conflict")(r.reason));
    }
    const { rows } = await pool.query(
      "SELECT count(*)::int AS n FROM mcp_audit WHERE organization = 'acme' AND detail->>'server' = 'race'",
    );
    assert.equal(rows[0].n, 1);
  },
);

test(
  "concurrent versions of one server all land in order",
  { skip },
  async () => {
    await registry.createServer("acme", body("versions", "0"), alice);
    await Promise.all(
      [1, 2, 3, 4, 5].map((n) =>
        registry.addVersion(
          "acme",
          "versions",
          body("x", `${n}`).version,
          alice,
        ),
      ),
    );
    const server = await registry.get("acme", "versions", true);
    assert.equal(server.versions.length, 6);
    assert.deepEqual(server.versions.map((v) => v.version).sort(), [
      "0",
      "1",
      "2",
      "3",
      "4",
      "5",
    ]);
  },
);

test("a rejected change stores no audit event", { skip }, async () => {
  await registry.createServer("acme", body("atomic"), alice);
  await assert.rejects(
    registry.approve("acme", "atomic", { version: "1.0.0" }, alice),
    code("forbidden"),
  );
  const { rows } = await pool.query(
    "SELECT action FROM mcp_audit WHERE organization = 'acme' AND detail->>'server' = 'atomic'",
  );
  assert.deepEqual(
    rows.map((r) => r.action),
    ["servers.create"],
  );
});

test("a repeated approval writes no second audit event", { skip }, async () => {
  await registry.createServer("acme", body("repeat"), alice);
  await registry.approve("acme", "repeat", { version: "1.0.0" }, bob);
  await registry.approve("acme", "repeat", { version: "1.0.0" }, bob);
  const { rows } = await pool.query(
    "SELECT 1 FROM mcp_audit WHERE organization = 'acme' AND detail->>'server' = 'repeat' AND action = 'servers.approve'",
  );
  assert.equal(rows.length, 1);
});

test(
  "listing pages by ID and hides unapproved servers from discovery",
  { skip },
  async () => {
    for (const id of ["pg-c", "pg-a", "pg-b"]) {
      await registry.createServer("pages", body(id), alice);
    }
    await registry.approve("pages", "pg-a", { version: "1.0.0" }, bob);
    await registry.approve("pages", "pg-c", { version: "1.0.0" }, bob);
    const all = await registry.list("pages", { limit: 2 }, true);
    assert.deepEqual(
      all.items.map((s) => s.id),
      ["pg-a", "pg-b"],
    );
    const rest = await registry.list(
      "pages",
      { limit: 2, after: all.next as string },
      true,
    );
    assert.deepEqual(
      rest.items.map((s) => s.id),
      ["pg-c"],
    );
    const found = await registry.list("pages", { limit: 10 }, false);
    assert.deepEqual(
      found.items.map((s) => s.id),
      ["pg-a", "pg-c"],
    );
  },
);

test(
  "health is stored per version and replaced on update",
  { skip },
  async () => {
    assert.equal(await store.getHealth("acme", "life", "1.0.0"), undefined);
    const h = {
      version: "1.0.0",
      status: "healthy" as const,
      checkedAt: "2026-11-18T10:00:00.000Z",
    };
    await store.recordHealth("acme", "life", h);
    await store.recordHealth("acme", "life", {
      ...h,
      status: "unhealthy",
      reason: "timeout",
    });
    assert.equal(
      (await store.getHealth("acme", "life", "1.0.0"))?.reason,
      "timeout",
    );
    assert.equal(await store.getHealth("globex", "life", "1.0.0"), undefined);
  },
);

import assert from "node:assert/strict";
import { readdirSync, readFileSync } from "node:fs";
import type { AddressInfo } from "node:net";
import { join, resolve } from "node:path";
import { after, before, test } from "node:test";
import { type Health, MemoryStore, Registry } from "@bae6/context-mcp";
import AjvModule from "ajv/dist/2020.js";
import { parse } from "yaml";
import type { Identity } from "./identity.js";
import { createRegistryApi } from "./server.js";

const who = (org: string, scopes: string[], sub: string): Identity => ({
  subjectId: sub,
  subjectType: "user",
  organizationId: org,
  applicationId: "app",
  scopes,
});
const identities: Record<string, Identity> = {
  publisher: who("acme", ["mcp:publish", "mcp:discover"], "alice"),
  approver: who("acme", ["mcp:approve", "mcp:discover"], "bob"),
  alice_approver: who("acme", ["mcp:approve"], "alice"),
  reader: who("acme", ["mcp:discover"], "carol"),
  nobody: who("acme", [], "dave"),
  globex: who("globex", ["mcp:publish", "mcp:approve", "mcp:discover"], "erin"),
};

const health: Health = {
  version: "1.0.0",
  status: "healthy",
  checkedAt: "2026-11-18T10:00:00.000Z",
};
let pingFails = false;
let probes = 0;
const logs: Record<string, unknown>[] = [];
const server = createRegistryApi({
  verify: async (t) => (t ? (identities[t] ?? null) : null),
  registry: new Registry(new MemoryStore(), {}),
  probe: async (v) => {
    probes++;
    return { ...health, version: v.version };
  },
  ping: async () => {
    if (pingFails) throw new Error("db down");
  },
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

const call = (
  method: string,
  path: string,
  token: string | undefined,
  body?: unknown,
) =>
  fetch(`${base}${path}`, {
    method,
    headers: token ? { authorization: `Bearer ${token}` } : {},
    ...(body === undefined || method === "GET"
      ? {}
      : { body: JSON.stringify(body) }),
  });
const codeOf = async (res: Response) =>
  [res.status, ((await res.json()) as { code: string }).code] as const;

const contracts = resolve(import.meta.dirname, "../../../contracts");
const schemas = "https://github.com/basics-only/bae6-contracts/schemas/";
const spec = parse(
  readFileSync(join(contracts, "mcp-registry-api.yaml"), "utf8")
    .replaceAll("./vendor/schemas/", schemas)
    .replaceAll(
      "./vendor/openapi/common.yaml#/components/schemas/ErrorEnvelope",
      `${schemas}error-envelope.json`,
    )
    .replaceAll(
      "./vendor/openapi/common.yaml#/components/schemas/Page",
      `${schemas}page.json`,
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
const newServer = (id: string) => ({
  id,
  team: "platform",
  name: "Docs",
  version: version(),
});
const publish = async (id: string, token = "publisher") => {
  const res = await call("POST", "/v1/servers", token, newServer(id));
  assert.equal(res.status, 201);
  conforms("Server", await res.json());
};

test("serves every documented route with spec-conformant bodies", async () => {
  assert.equal(
    Object.keys(spec.paths as object).filter((p) => p.startsWith("/v1/"))
      .length,
    6,
  );
  await publish("conf");
  assert.equal(
    (
      await call(
        "POST",
        "/v1/servers/conf/versions",
        "publisher",
        version({ version: "1.1.0" }),
      )
    ).status,
    201,
  );
  const approved = await call("POST", "/v1/servers/conf/approve", "approver", {
    version: "1.0.0",
  });
  assert.equal(approved.status, 200);
  conforms("Server", await approved.json());
  const got = await call("GET", "/v1/servers/conf", "reader");
  assert.equal(got.status, 200);
  conforms("Server", await got.json());
  const list = await call("GET", "/v1/servers", "reader");
  conforms("ServerPage", await list.json());
  const h = await call("GET", "/v1/servers/conf/health", "reader");
  assert.equal(h.status, 200);
  conforms("ServerHealth", await h.json());
  const revoked = await call("POST", "/v1/servers/conf/revoke", "approver");
  assert.equal(revoked.status, 200);
  conforms("Server", await revoked.json());
  assert.equal((await call("GET", "/health/live", undefined)).status, 200);
  assert.equal((await call("GET", "/health/ready", undefined)).status, 200);
});

test("refuses missing credentials and the wrong scope per operation", async () => {
  assert.deepEqual(await codeOf(await call("GET", "/v1/servers", undefined)), [
    401,
    "unauthenticated",
  ]);
  await publish("scopes");
  for (const [method, path, token] of [
    ["POST", "/v1/servers", "reader"],
    ["POST", "/v1/servers/scopes/versions", "approver"],
    ["POST", "/v1/servers/scopes/approve", "publisher"],
    ["POST", "/v1/servers/scopes/revoke", "publisher"],
    ["GET", "/v1/servers", "nobody"],
    ["GET", "/v1/servers/scopes", "nobody"],
    ["GET", "/v1/servers/scopes/health", "nobody"],
  ] as const) {
    assert.deepEqual(
      await codeOf(await call(method, path, token, {})),
      [403, "forbidden"],
      `${method} ${path} as ${token}`,
    );
  }
});

test("does not show one tenant's servers to another", async () => {
  await publish("private");
  await call("POST", "/v1/servers/private/approve", "approver", {
    version: "1.0.0",
  });
  for (const [method, path, body] of [
    ["GET", "/v1/servers/private", undefined],
    ["GET", "/v1/servers/private/health", undefined],
    ["POST", "/v1/servers/private/versions", version({ version: "2.0.0" })],
    ["POST", "/v1/servers/private/approve", { version: "1.0.0" }],
    ["POST", "/v1/servers/private/revoke", { version: "1.0.0" }],
  ] as const) {
    assert.deepEqual(
      await codeOf(await call(method, path, "globex", body)),
      [404, "not_found"],
      path,
    );
  }
  const list = (await (await call("GET", "/v1/servers", "globex")).json()) as {
    items: { id: string }[];
  };
  assert.ok(!list.items.some((s) => s.id === "private"));
  assert.equal(
    (await call("POST", "/v1/servers", "globex", newServer("private"))).status,
    201,
  );
});

test("hides pending and revoked servers from discover-only callers", async () => {
  await publish("lifecycle");
  assert.equal(
    (await call("GET", "/v1/servers/lifecycle", "reader")).status,
    404,
  );
  assert.equal(
    (await call("GET", "/v1/servers/lifecycle", "publisher")).status,
    200,
  );
  await call("POST", "/v1/servers/lifecycle/approve", "approver", {
    version: "1.0.0",
  });
  assert.equal(
    (await call("GET", "/v1/servers/lifecycle", "reader")).status,
    200,
  );
  await call("POST", "/v1/servers/lifecycle/revoke", "approver");
  assert.equal(
    (await call("GET", "/v1/servers/lifecycle", "reader")).status,
    404,
  );
  const list = (await (await call("GET", "/v1/servers", "reader")).json()) as {
    items: { id: string }[];
  };
  assert.ok(!list.items.some((s) => s.id === "lifecycle"));
});

test("refuses approval by the publisher", async () => {
  await publish("self");
  assert.deepEqual(
    await codeOf(
      await call("POST", "/v1/servers/self/approve", "alice_approver", {
        version: "1.0.0",
      }),
    ),
    [403, "forbidden"],
  );
});

test("pages with an opaque cursor and refuses a forged one", async () => {
  for (const id of ["page-a", "page-b", "page-c"]) await publish(id, "globex");
  const first = (await (
    await call("GET", "/v1/servers?limit=2", "globex")
  ).json()) as { items: unknown[]; nextCursor?: string };
  assert.equal(first.items.length, 2);
  assert.ok(first.nextCursor);
  const second = await call(
    "GET",
    `/v1/servers?limit=2&cursor=${first.nextCursor}`,
    "globex",
  );
  assert.equal(second.status, 200);
  for (const q of [
    "cursor=not-base64!",
    `cursor=${"a".repeat(513)}`,
    "limit=0",
    "limit=101",
    "limit=x",
  ]) {
    assert.deepEqual(
      await codeOf(await call("GET", `/v1/servers?${q}`, "globex")),
      [400, "invalid_request"],
    );
  }
});

test("refuses malformed bodies and unknown routes", async () => {
  assert.deepEqual(
    await codeOf(await call("POST", "/v1/servers", "publisher", { id: "x" })),
    [400, "invalid_request"],
  );
  await publish("dup");
  assert.deepEqual(
    await codeOf(
      await call("POST", "/v1/servers", "publisher", newServer("dup")),
    ),
    [409, "conflict"],
  );
  assert.equal((await call("GET", "/v1/other", "publisher")).status, 404);
  assert.equal(
    (await call("DELETE", "/v1/servers/dup", "publisher")).status,
    404,
  );
  assert.equal(
    (await call("POST", "/v1/servers/Bad_Id/approve", "approver", {})).status,
    404,
  );
});

test("probes only on the health route and reports not ready when the database is down", async () => {
  await publish("probe");
  const before = probes;
  await call("GET", "/v1/servers/probe", "publisher");
  assert.equal(probes, before);
  pingFails = true;
  assert.deepEqual(
    await codeOf(await call("GET", "/health/ready", undefined)),
    [503, "unavailable"],
  );
  pingFails = false;
});

test("refuses a third concurrent request from one organization", async () => {
  let open!: () => void;
  const gate = {
    promise: new Promise<void>((r) => {
      open = r;
    }),
    resolve: () => open(),
  };
  const slow = createRegistryApi({
    verify: async (t) => (t ? (identities[t] ?? null) : null),
    registry: new Registry(new MemoryStore(), {}),
    probe: async (v) => {
      await gate.promise;
      return { ...health, version: v.version };
    },
    ping: async () => {},
    maxInflight: 8,
    maxInflightPerOrganization: 2,
    requestTimeoutMs: 5000,
  });
  await new Promise<void>((r) => slow.listen(0, "127.0.0.1", r));
  const url = `http://127.0.0.1:${(slow.address() as AddressInfo).port}`;
  const send = (method: string, path: string, body?: unknown) =>
    fetch(`${url}${path}`, {
      method,
      headers: { authorization: "Bearer publisher" },
      ...(body === undefined ? {} : { body: JSON.stringify(body) }),
    });
  try {
    assert.equal(
      (await send("POST", "/v1/servers", newServer("slow"))).status,
      201,
    );
    const approve = await fetch(`${url}/v1/servers/slow/approve`, {
      method: "POST",
      headers: { authorization: "Bearer approver" },
      body: JSON.stringify({ version: "1.0.0" }),
    });
    assert.equal(approve.status, 200);
    const held = [
      send("GET", "/v1/servers/slow/health"),
      send("GET", "/v1/servers/slow/health"),
    ];
    await new Promise((r) => setTimeout(r, 100));
    const refused = await send("GET", "/v1/servers/slow");
    assert.deepEqual(
      [refused.status, refused.headers.get("retry-after")],
      [429, "1"],
    );
    gate.resolve();
    for (const r of await Promise.all(held)) assert.equal(r.status, 200);
  } finally {
    gate.resolve();
    slow.closeAllConnections();
    slow.close();
  }
});

test("fails readiness while draining", async () => {
  const draining = createRegistryApi({
    verify: async () => null,
    registry: new Registry(new MemoryStore(), {}),
    probe: async () => health,
    ping: async () => {},
    maxInflight: 1,
    maxInflightPerOrganization: 1,
    requestTimeoutMs: 5000,
  });
  await new Promise<void>((r) => draining.listen(0, "127.0.0.1", r));
  const url = `http://127.0.0.1:${(draining.address() as AddressInfo).port}`;
  try {
    assert.equal((await fetch(`${url}/health/ready`)).status, 200);
    draining.drain();
    assert.equal((await fetch(`${url}/health/ready`)).status, 503);
  } finally {
    draining.closeAllConnections();
    draining.close();
  }
});

import assert from "node:assert/strict";
import { readdirSync, readFileSync } from "node:fs";
import type { AddressInfo } from "node:net";
import { join, resolve } from "node:path";
import { after, before, test } from "node:test";
import type { Server, Version } from "@bae6/context-mcp";
import AjvModule from "ajv/dist/2020.js";
import { parse } from "yaml";
import { RegistryUnavailable } from "./entries.js";
import { createGateway } from "./server.js";
import { type ToolCall, UpstreamError } from "./upstream.js";

const who = (org: string, scopes: string[], sub: string) => ({
  subjectId: sub,
  subjectType: "user" as const,
  organizationId: org,
  applicationId: "app",
  scopes,
});
const identities = {
  caller: who("acme", ["mcp:invoke"], "alice"),
  reader: who("acme", ["mcp:discover", "mcp:publish"], "bob"),
  globex: who("globex", ["mcp:invoke"], "erin"),
};

const version = (over: Partial<Version> = {}): Version => ({
  version: "1.0.0",
  endpoint: "http://tools.example.com/mcp",
  protocolVersion: "2025-06-18",
  transport: "streamable-http",
  authentication: { method: "none" },
  tools: [{ name: "echo" }],
  resources: [],
  prompts: [],
  allowedNetworkZones: [],
  state: "approved",
  scanStatus: "unscanned",
  publishedBy: "x",
  publishedVia: "x",
  publishedAt: "2026-11-01T00:00:00.000Z",
  ...over,
});
const entry = (versions: Version[]): Server => ({
  id: "tools",
  organization: "acme",
  team: "t",
  name: "Tools",
  createdAt: "2026-11-01T00:00:00.000Z",
  versions,
});

let servers: Record<string, Server | undefined> = {};
let registryDown = false;
let upstreamCalls: ToolCall[] = [];
let upstreamResult: () => Promise<unknown> = async () => ({ ok: true });
const logs: Record<string, unknown>[] = [];

const gateway = createGateway({
  verify: async (t) =>
    t ? (identities[t as keyof typeof identities] ?? null) : null,
  entries: async (org, id) => {
    if (registryDown) throw new RegistryUnavailable("down");
    const s = servers[`${org}/${id}`];
    return s ?? null;
  },
  upstream: async (call) => {
    upstreamCalls.push(call);
    return upstreamResult();
  },
  maxInflight: 8,
  maxInflightPerOrganization: 1,
  requestTimeoutMs: 5000,
  log: (e) => logs.push(e),
});
let base: string;
before(async () => {
  await new Promise<void>((r) => gateway.listen(0, "127.0.0.1", r));
  base = `http://127.0.0.1:${(gateway.address() as AddressInfo).port}`;
});
after(() => gateway.close());

const reset = (versions: Version[] = [version()]) => {
  servers = { "acme/tools": entry(versions) };
  registryDown = false;
  upstreamCalls = [];
  upstreamResult = async () => ({ ok: true });
  logs.length = 0;
};

const post = (
  token: string | undefined,
  body: unknown,
  path = "/v1/servers/tools/tools/call",
) =>
  fetch(`${base}${path}`, {
    method: "POST",
    headers: token ? { authorization: `Bearer ${token}` } : {},
    body: typeof body === "string" ? body : JSON.stringify(body),
  });
const codeOf = async (res: Response) =>
  [res.status, ((await res.json()) as { code: string }).code] as const;

const contracts = resolve(import.meta.dirname, "../../../contracts");
const schemas = "https://github.com/basics-only/bae6-contracts/schemas/";
const spec = parse(
  readFileSync(join(contracts, "mcp-gateway-api.yaml"), "utf8")
    .replaceAll("./vendor/schemas/", schemas)
    .replaceAll(
      "./vendor/openapi/common.yaml#/components/schemas/ErrorEnvelope",
      `${schemas}error-envelope.json`,
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
  assert.ok(validate(body), `${schema}: ${JSON.stringify(validate.errors)}`);
}

test("calls a declared tool on the latest approved version and returns the result unmodified", async () => {
  reset([
    version({ version: "1.0.0" }),
    version({
      version: "2.0.0",
      publishedAt: "2026-11-10T00:00:00.000Z",
      endpoint: "http://tools.example.com/v2",
    }),
    version({
      version: "3.0.0",
      state: "pending",
      publishedAt: "2026-11-15T00:00:00.000Z",
    }),
  ]);
  upstreamResult = async () => ({ content: [{ type: "text", text: "hi" }] });
  const res = await post("caller", { tool: "echo", arguments: { text: "hi" } });
  assert.equal(res.status, 200);
  const body = await res.json();
  conforms("CallToolResponse", body);
  assert.deepEqual(body, {
    server: "tools",
    version: "2.0.0",
    tool: "echo",
    result: { content: [{ type: "text", text: "hi" }] },
  });
  assert.deepEqual(upstreamCalls, [
    {
      endpoint: "http://tools.example.com/v2",
      protocolVersion: "2025-06-18",
      tool: "echo",
      arguments: { text: "hi" },
    },
  ]);
});

test("a pinned version is used only when it is approved", async () => {
  reset([
    version({ version: "1.0.0" }),
    version({ version: "2.0.0", state: "revoked" }),
  ]);
  assert.equal(
    (await post("caller", { tool: "echo", version: "1.0.0" })).status,
    200,
  );
  assert.deepEqual(
    await codeOf(await post("caller", { tool: "echo", version: "2.0.0" })),
    [404, "not_found"],
  );
  assert.equal(upstreamCalls.length, 1);
});

test("refuses before touching the network", async () => {
  const cases: [string, () => Promise<Response>, number, string][] = [
    [
      "no token",
      () => post(undefined, { tool: "echo" }),
      401,
      "unauthenticated",
    ],
    ["bad token", () => post("nope", { tool: "echo" }), 401, "unauthenticated"],
    [
      "discover is not invoke",
      () => post("reader", { tool: "echo" }),
      403,
      "forbidden",
    ],
    [
      "another organization",
      () => post("globex", { tool: "echo" }),
      404,
      "not_found",
    ],
    [
      "unknown server",
      () => post("caller", { tool: "echo" }, "/v1/servers/other/tools/call"),
      404,
      "not_found",
    ],
    [
      "bad server id",
      () => post("caller", { tool: "echo" }, "/v1/servers/Bad%20Id/tools/call"),
      404,
      "not_found",
    ],
    ["undeclared tool", () => post("caller", { tool: "rm" }), 404, "not_found"],
    ["not json", () => post("caller", "{"), 400, "invalid_request"],
    ["no tool", () => post("caller", {}), 400, "invalid_request"],
    [
      "extra field",
      () => post("caller", { tool: "echo", url: "x" }),
      400,
      "invalid_request",
    ],
    [
      "arguments not an object",
      () => post("caller", { tool: "echo", arguments: [1] }),
      400,
      "invalid_request",
    ],
    [
      "bad version",
      () => post("caller", { tool: "echo", version: "../x" }),
      400,
      "invalid_request",
    ],
    [
      "body over the limit",
      () =>
        post("caller", { tool: "echo", arguments: { a: "x".repeat(70_000) } }),
      413,
      "payload_too_large",
    ],
  ];
  for (const [name, run, status, code] of cases) {
    reset();
    const res = await run();
    assert.deepEqual(await codeOf(res), [status, code], name);
    assert.equal(upstreamCalls.length, 0, name);
  }
});

test("a version needing upstream auth is refused with a fixed code", async () => {
  reset([
    version({
      authentication: {
        method: "oauth2",
        resource: "https://tools.example.com",
      },
    }),
  ]);
  const res = await post("caller", { tool: "echo" });
  assert.deepEqual(await codeOf(res), [409, "upstream_auth_unsupported"]);
  assert.equal(upstreamCalls.length, 0);
});

test("upstream failures map to fixed codes without the upstream's text", async () => {
  const cases: [UpstreamError["reason"], number][] = [
    ["blocked", 502],
    ["unreachable", 502],
    ["bad_response", 502],
    ["too_large", 502],
    ["rejected", 502],
    ["timeout", 504],
  ];
  for (const [reason, status] of cases) {
    reset();
    upstreamResult = async () => {
      throw new UpstreamError(reason);
    };
    const res = await post("caller", { tool: "echo" });
    const body = (await res.json()) as { code: string; message: string };
    assert.equal(res.status, status);
    assert.equal(body.code, `upstream_${reason}`);
    assert.equal(body.message, "The upstream call failed.");
  }
});

test("an unreachable registry fails the call closed", async () => {
  reset();
  registryDown = true;
  const res = await post("caller", { tool: "echo" });
  assert.deepEqual(await codeOf(res), [503, "unavailable"]);
  assert.equal(upstreamCalls.length, 0);
});

test("an organization over its in-flight cap is told to retry, and others are not", async () => {
  reset();
  let release!: () => void;
  upstreamResult = () =>
    new Promise((r) => {
      release = () => r({});
    });
  const first = post("caller", { tool: "echo" });
  while (upstreamCalls.length === 0) await new Promise((r) => setTimeout(r, 5));
  const second = await post("caller", { tool: "echo" });
  assert.deepEqual(await codeOf(second), [429, "rate_limited"]);
  assert.equal(second.headers.get("retry-after"), "1");
  release();
  assert.equal((await first).status, 200);
});

test("one access event per call carries who, what and the outcome but not arguments or results", async () => {
  reset();
  upstreamResult = async () => ({ secret: "result-data" });
  await post("caller", { tool: "echo", arguments: { secret: "arg-data" } });
  await new Promise((r) => setTimeout(r, 20));
  const events = logs.filter((e) => e.msg === "request");
  assert.equal(events.length, 1);
  assert.deepEqual(
    {
      organizationId: events[0]?.organizationId,
      subjectId: events[0]?.subjectId,
      serverId: events[0]?.serverId,
      version: events[0]?.version,
      tool: events[0]?.tool,
      outcome: events[0]?.outcome,
      status: events[0]?.status,
    },
    {
      organizationId: "acme",
      subjectId: "alice",
      serverId: "tools",
      version: "1.0.0",
      tool: "echo",
      outcome: "ok",
      status: 200,
    },
  );
  assert.ok(!JSON.stringify(logs).includes("arg-data"));
  assert.ok(!JSON.stringify(logs).includes("result-data"));
});

test("readiness fails while draining", async () => {
  const draining = createGateway({
    verify: async () => null,
    entries: async () => null,
    upstream: async () => ({}),
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
    assert.equal((await fetch(`${url}/health/live`)).status, 200);
  } finally {
    draining.closeAllConnections();
    draining.close();
  }
});

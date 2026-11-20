import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { test } from "node:test";

const start = (env: Record<string, string>) =>
  spawnSync(
    process.execPath,
    ["--import", "tsx", "--conditions=source", "src/main.ts"],
    {
      cwd: new URL("..", import.meta.url),
      env: {
        PATH: process.env.PATH ?? "",
        BAE6_MCP_ISSUERS: "[]",
        ...env,
      },
      encoding: "utf8",
      timeout: 15_000,
    },
  );

const configured = {
  BAE6_MCP_REGISTRY_URL: "http://registry.invalid",
  BAE6_MCP_REGISTRY_CREDENTIALS: "{}",
};

test("refuses to start without an egress host allowlist", () => {
  const res = start(configured);
  assert.notEqual(res.status, 0);
  assert.match(res.stderr, /BAE6_MCP_ALLOWED_HOST_SUFFIXES is required/);
});

test("refuses to start without a registry to read approvals from", () => {
  const res = start({
    BAE6_MCP_ALLOWED_HOST_SUFFIXES: "example.com",
    BAE6_MCP_REGISTRY_CREDENTIALS: "{}",
  });
  assert.notEqual(res.status, 0);
  assert.match(res.stderr, /BAE6_MCP_REGISTRY_URL must be an http\(s\) URL/);
});

test("refuses registry credentials that are not complete or not https", () => {
  const cases = [
    '["/x"]',
    '{"acme":{"tokenUrl":"https://idp.example.com/t","clientId":"gw"}}',
    '{"acme":{"tokenUrl":"http://idp.example.com/t","clientId":"gw","clientSecret":"s"}}',
  ];
  for (const credentials of cases) {
    const res = start({
      ...configured,
      BAE6_MCP_ALLOWED_HOST_SUFFIXES: "example.com",
      BAE6_MCP_REGISTRY_CREDENTIALS: credentials,
    });
    assert.notEqual(res.status, 0, credentials);
    assert.match(res.stderr, /BAE6_MCP_REGISTRY_CREDENTIALS must be/);
  }
});

test("starts past the allowlist check when one is configured", () => {
  const res = start({
    ...configured,
    BAE6_MCP_ALLOWED_HOST_SUFFIXES: "example.com",
  });
  assert.doesNotMatch(res.stderr, /ALLOWED_HOST_SUFFIXES is required/);
});

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
        BAE6_MCP_DATABASE_URL: "postgres://127.0.0.1:1/none",
        ...env,
      },
      encoding: "utf8",
      timeout: 15_000,
    },
  );

test("refuses to start without an egress host allowlist", () => {
  const res = start({});
  assert.notEqual(res.status, 0);
  assert.match(res.stderr, /BAE6_MCP_ALLOWED_HOST_SUFFIXES is required/);
});

test("starts past the allowlist check when one is configured", () => {
  const res = start({ BAE6_MCP_ALLOWED_HOST_SUFFIXES: "example.com" });
  assert.doesNotMatch(res.stderr, /ALLOWED_HOST_SUFFIXES is required/);
});

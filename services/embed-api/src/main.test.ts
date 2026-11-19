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
        BAE6_EMBED_ISSUERS: "[]",
        BAE6_EMBED_CONFIG: '{"collections":[]}',
        BAE6_AI_TOKEN: "t",
        ...env,
      },
      encoding: "utf8",
      timeout: 15_000,
    },
  );

test("refuses a plain-http AI gateway URL outside development", () => {
  const res = start({ BAE6_AI_URL: "http://ai.internal" });
  assert.notEqual(res.status, 0);
  assert.match(res.stderr, /BAE6_AI_URL must be https/);
});

test("accepts an https AI gateway URL", () => {
  const res = start({ BAE6_AI_URL: "https://ai.internal" });
  assert.doesNotMatch(res.stderr, /BAE6_AI_URL must be https/);
});

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

const refusal = /BAE6_AI_URL must be https/;

test("refuses a plain-http AI gateway URL outside the cluster", () => {
  for (const url of [
    "http://ai.example.com",
    "http://x.svc.cluster.local.example.com",
  ]) {
    const res = start({ BAE6_AI_URL: url });
    assert.notEqual(res.status, 0, url);
    assert.match(res.stderr, refusal, url);
  }
});

test("accepts https and in-cluster http AI gateway URLs", () => {
  for (const url of [
    "https://ai.example.com",
    "http://bae6-ai.bae6.svc.cluster.local",
  ]) {
    assert.doesNotMatch(start({ BAE6_AI_URL: url }).stderr, refusal, url);
  }
});

test("refuses a config spanning organizations, which would share one AI token", () => {
  const collection = (organization: string) => ({
    id: "kb",
    organization,
    model: { id: "m", dimensions: 8 },
    source: { region: "us-east-1", bucket: "b", prefix: "" },
    acl: [],
  });
  const config = (...orgs: string[]) =>
    JSON.stringify({ collections: orgs.map(collection) });
  const env = { BAE6_AI_URL: "https://ai.example.com" };
  const refused = start({
    ...env,
    BAE6_EMBED_CONFIG: config("acme", "globex"),
  });
  assert.notEqual(refused.status, 0);
  assert.match(refused.stderr, /single organization/);
  assert.doesNotMatch(
    start({ ...env, BAE6_EMBED_CONFIG: config("acme") }).stderr,
    /single organization/,
  );
});

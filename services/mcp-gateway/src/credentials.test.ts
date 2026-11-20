import assert from "node:assert/strict";
import { createServer } from "node:http";
import type { AddressInfo } from "node:net";
import { after, before, test } from "node:test";
import { createTokenSource } from "./credentials.js";
import { RegistryUnavailable } from "./entries.js";

const requests: { body: string; type?: string }[] = [];
let respond: () => { status: number; body: string } = () => ({
  status: 200,
  body: "{}",
});
const idp = createServer((req, res) => {
  const chunks: Buffer[] = [];
  req.on("data", (c) => chunks.push(c));
  req.on("end", () => {
    requests.push({
      body: Buffer.concat(chunks).toString(),
      ...(req.headers["content-type"]
        ? { type: req.headers["content-type"] }
        : {}),
    });
    const { status, body } = respond();
    res.writeHead(status, { "content-type": "application/json" });
    res.end(body);
  });
});
let tokenUrl: string;
before(async () => {
  await new Promise<void>((r) => idp.listen(0, "127.0.0.1", r));
  tokenUrl = `http://127.0.0.1:${(idp.address() as AddressInfo).port}/token`;
});
after(() => {
  idp.closeAllConnections();
  idp.close();
});

let clock = 0;
let issued = 0;
const reply =
  (expiresIn: number, over: Record<string, unknown> = {}) =>
  () => ({
    status: 200,
    body: JSON.stringify({
      access_token: `t${++issued}`,
      token_type: "Bearer",
      expires_in: expiresIn,
      ...over,
    }),
  });
const source = (over = {}) =>
  createTokenSource({
    credentials: {
      acme: {
        tokenUrl,
        clientId: "gw",
        clientSecret: "s3cret",
        scope: "mcp:discover",
      },
    },
    timeoutMs: 1000,
    maxBytes: 4096,
    now: () => clock,
    ...over,
  });
const reset = () => {
  requests.length = 0;
  clock = 0;
  issued = 0;
  respond = reply(3600);
};

test("exchanges client credentials and reuses the token until shortly before it expires", async () => {
  reset();
  const token = source();
  assert.equal(await token("acme"), "t1");
  assert.equal(await token("acme"), "t1");
  assert.equal(requests.length, 1);
  assert.equal(requests[0]?.type, "application/x-www-form-urlencoded");
  const form = new URLSearchParams(requests[0]?.body);
  assert.deepEqual(Object.fromEntries(form), {
    grant_type: "client_credentials",
    client_id: "gw",
    client_secret: "s3cret",
    scope: "mcp:discover",
  });
  clock = (3600 - 60) * 1000 + 1;
  assert.equal(await token("acme"), "t2");
});

test("concurrent callers share one token request", async () => {
  reset();
  const token = source();
  const got = await Promise.all([token("acme"), token("acme"), token("acme")]);
  assert.deepEqual(got, ["t1", "t1", "t1"]);
  assert.equal(requests.length, 1);
});

test("an organization without credentials gets nothing and the endpoint is not called", async () => {
  reset();
  assert.equal(await source()("globex"), undefined);
  assert.equal(requests.length, 0);
});

test("endpoint errors and malformed replies fail instead of yielding a token, and are not cached", async () => {
  const bad: [number, string][] = [
    [401, "{}"],
    [200, "not json"],
    [
      200,
      JSON.stringify({
        access_token: "",
        token_type: "Bearer",
        expires_in: 60,
      }),
    ],
    [
      200,
      JSON.stringify({ access_token: "x", token_type: "mac", expires_in: 60 }),
    ],
    [
      200,
      JSON.stringify({
        access_token: "x",
        token_type: "Bearer",
        expires_in: 0,
      }),
    ],
    [
      200,
      JSON.stringify({
        access_token: "x",
        token_type: "Bearer",
        expires_in: "60",
      }),
    ],
    [
      200,
      JSON.stringify({
        access_token: "x".repeat(5000),
        token_type: "Bearer",
        expires_in: 60,
      }),
    ],
  ];
  for (const [status, body] of bad) {
    reset();
    respond = () => ({ status, body });
    const token = source();
    await assert.rejects(token("acme"), RegistryUnavailable, body.slice(0, 20));
    respond = reply(3600);
    assert.equal(await token("acme"), "t1");
  }
});

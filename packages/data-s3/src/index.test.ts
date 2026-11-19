import assert from "node:assert/strict";
import { before, test } from "node:test";
import { DataError } from "@bae6/context-data";
import { createS3Source } from "./index.js";
import { encode, sha256, signS3 } from "./sigv4.js";

const endpoint = process.env.BAE6_S3_ENDPOINT;
const skip = endpoint ? false : "BAE6_S3_ENDPOINT not set";
const credentials = {
  accessKeyId: process.env.BAE6_S3_ACCESS_KEY ?? "",
  secretAccessKey: process.env.BAE6_S3_SECRET_KEY ?? "",
};
const region = "us-east-1";
const bucket = `bae6-test-${Date.now()}`;

async function put(path: string, body: string, query = "") {
  const url = new URL(
    `/${bucket}${path ? `/${path.split("/").map(encode).join("/")}` : ""}${query}`,
    endpoint,
  );
  const res = await fetch(url, {
    method: "PUT",
    headers: signS3({
      method: "PUT",
      url,
      payloadHash: sha256(body),
      region,
      credentials,
    }),
    body,
  });
  assert.ok(res.ok, `${path}${query}: ${res.status} ${await res.text()}`);
  return res.headers.get("x-amz-version-id");
}

const source = (extra: object = {}) =>
  createS3Source({
    id: "s3-test",
    region,
    bucket,
    prefix: "allowed/",
    endpoint: endpoint as string,
    credentials: async () => credentials,
    allowHttp: true,
    ...extra,
  });

const code = async (p: Promise<unknown>) => {
  try {
    await p;
  } catch (err) {
    assert.ok(err instanceof DataError, String(err));
    return err.code;
  }
  assert.fail("expected a DataError");
};

let firstVersion: string | null = null;
before(async () => {
  if (!endpoint) return;
  await put("", "");
  await put(
    "",
    '<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Status>Enabled</Status></VersioningConfiguration>',
    "?versioning",
  );
  await put("allowed/a.txt", "hello world");
  await put("allowed/b & c.txt", "spaces and ampersand");
  await put("allowed/big.bin", "x".repeat(2000));
  await put("secret/x.txt", "do not read");
  firstVersion = await put("allowed/v.txt", "one");
  await put("allowed/v.txt", "two");
});

test("lists only inside the prefix, with pagination", { skip }, async () => {
  const s = source();
  const all = await s.list({});
  assert.deepEqual(all.items.map((i) => i.key).sort(), [
    "allowed/a.txt",
    "allowed/b & c.txt",
    "allowed/big.bin",
    "allowed/v.txt",
  ]);
  assert.equal(all.items.find((i) => i.key === "allowed/a.txt")?.size, 11);
  const page = await s.list({ limit: 1 });
  assert.equal(page.items.length, 1);
  assert.ok(page.nextCursor);
  const next = await s.list({ limit: 1, cursor: page.nextCursor });
  assert.notEqual(next.items[0]?.key, page.items[0]?.key);
  assert.equal(await code(s.list({ prefix: "secret/" })), "forbidden");
  assert.equal(await code(s.list({ prefix: "" })), "forbidden");
});

test("reads whole objects, ranges, versions and stats", { skip }, async () => {
  const s = source();
  const whole = await s.read({ key: "allowed/b & c.txt" });
  assert.equal(Buffer.from(whole.body).toString(), "spaces and ampersand");
  assert.ok(whole.info.etag.length > 0 && !whole.info.etag.includes('"'));
  const part = await s.read({
    key: "allowed/a.txt",
    range: { start: 6, end: 10 },
  });
  assert.equal(Buffer.from(part.body).toString(), "world");
  assert.equal(part.info.size, 11);
  assert.ok(firstVersion);
  const old = await s.read({ key: "allowed/v.txt", versionId: firstVersion });
  assert.equal(Buffer.from(old.body).toString(), "one");
  assert.equal(
    Buffer.from((await s.read({ key: "allowed/v.txt" })).body).toString(),
    "two",
  );
  const st = await s.stat({ key: "allowed/big.bin" });
  assert.equal(st.size, 2000);
});

test(
  "refuses keys outside the prefix or that URL parsing would rewrite",
  { skip },
  async () => {
    const s = source();
    assert.equal(await code(s.read({ key: "secret/x.txt" })), "forbidden");
    assert.equal(
      await code(s.read({ key: "allowed/../secret/x.txt" })),
      "invalid_request",
    );
    assert.equal(
      await code(s.stat({ key: "allowed/./a.txt" })),
      "invalid_request",
    );
    assert.equal(
      await code(s.read({ key: "allowed/a\u0000" })),
      "invalid_request",
    );
    assert.equal(await code(s.read({ key: "allowed/missing" })), "not_found");
  },
);

test(
  "objects and ranges over the byte limit are too_large",
  { skip },
  async () => {
    const s = source({ limits: { maxObjectBytes: 1000 } });
    assert.equal(await code(s.read({ key: "allowed/big.bin" })), "too_large");
    assert.equal(
      await code(
        s.read({ key: "allowed/big.bin", range: { start: 0, end: 1000 } }),
      ),
      "too_large",
    );
    const ok = await s.read({
      key: "allowed/big.bin",
      range: { start: 0, end: 999 },
    });
    assert.equal(ok.body.byteLength, 1000);
    assert.equal(
      await code(s.read({ key: "allowed/a.txt", range: { start: 5, end: 2 } })),
      "invalid_request",
    );
  },
);

test(
  "a wrong secret is forbidden; a caller abort is cancelled",
  { skip },
  async () => {
    const bad = source({
      credentials: async () => ({ ...credentials, secretAccessKey: "wrong" }),
    });
    assert.equal(await code(bad.read({ key: "allowed/a.txt" })), "forbidden");
    const caller = new AbortController();
    caller.abort();
    assert.equal(
      await code(
        source().read({ key: "allowed/a.txt", signal: caller.signal }),
      ),
      "cancelled",
    );
  },
);

test("refuses a non-https or non-bare endpoint", () => {
  for (const e of [
    "http://s3.example.com",
    "https://u:p@s3.example.com",
    "https://s3.example.com/x",
  ]) {
    assert.throws(() =>
      createS3Source({
        id: "s3",
        region,
        bucket: "abc",
        prefix: "",
        endpoint: e,
        credentials: async () => credentials,
      }),
    );
  }
});

test("sends credentials only to AWS hosts unless development allows otherwise", () => {
  const cfg = {
    id: "s3",
    region,
    bucket: "abc",
    prefix: "",
    credentials: async () => credentials,
  };
  assert.throws(
    () => createS3Source({ ...cfg, endpoint: "https://s3.example.com" }),
    /not an AWS host/,
  );
  assert.doesNotThrow(() =>
    createS3Source({ ...cfg, endpoint: "https://s3.eu-west-1.amazonaws.com" }),
  );
  assert.doesNotThrow(() =>
    createS3Source({
      ...cfg,
      endpoint: "https://s3.example.com",
      allowHttp: true,
    }),
  );
});

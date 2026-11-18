import assert from "node:assert/strict";
import { createServer } from "node:http";
import type { AddressInfo } from "node:net";
import { after, test } from "node:test";
import { DataError } from "@bae6/context-data";
import { createS3Source } from "./index.js";
import { signS3 } from "./sigv4.js";

test("signs the AWS published S3 ListObjects example", () => {
  const headers = signS3({
    method: "GET",
    url: new URL("https://examplebucket.s3.amazonaws.com/?max-keys=2&prefix=J"),
    payloadHash:
      "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
    region: "us-east-1",
    credentials: {
      accessKeyId: "AKIAIOSFODNN7EXAMPLE",
      secretAccessKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
    },
    now: new Date("2013-05-24T00:00:00Z"),
  });
  assert.match(
    headers.authorization ?? "",
    /Signature=34b48302e7b5fa45bde8084f4b7868a86f0a534bc59db6670ed5711ef69dc6f7$/,
  );
});

let reply: { status?: number; headers?: Record<string, string>; body: string } =
  { body: "" };
let calls = 0;
const server = createServer((_req, res) => {
  calls++;
  res.writeHead(reply.status ?? 200, reply.headers ?? {});
  res.end(reply.body);
});
await new Promise<void>((ok) => server.listen(0, "127.0.0.1", ok));
after(() => server.close());

const source = (prefix = "tenant1/") =>
  createS3Source({
    id: "s3-offline",
    region: "us-east-1",
    bucket: "bucket",
    prefix,
    endpoint: `http://127.0.0.1:${(server.address() as AddressInfo).port}`,
    credentials: async () => ({ accessKeyId: "a", secretAccessKey: "b" }),
    allowHttp: true,
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

const contents = (key: string, extra = "") =>
  `<Contents><Key>${key}</Key><Size>1</Size><ETag>"e"</ETag><LastModified>2026-01-01T00:00:00Z</LastModified>${extra}</Contents>`;

test("a prefix must end at a path boundary", () => {
  assert.throws(() => source("tenant1"), /prefix/);
  assert.throws(() => source("tenant1/../"), /prefix/);
  assert.doesNotThrow(() => source(""));
});

test("refuses list prefixes and keys that a store could resolve", async () => {
  const s = source();
  calls = 0;
  assert.equal(
    await code(s.list({ prefix: "tenant1/../tenant2/" })),
    "invalid_request",
  );
  assert.equal(
    await code(s.read({ key: "tenant1/\ud800" })),
    "invalid_request",
  );
  assert.equal(calls, 0);
});

test("a listing that strays or is malformed fails instead of widening", async () => {
  const s = source();
  for (const xml of [
    contents("tenant2/x"),
    contents("tenant1/&#0;"),
    contents("tenant1/&#99999999;"),
    contents("tenant1/a").replace("2026-01-01T00:00:00Z", "garbage"),
    contents("tenant1/a") + contents("tenant1/b"),
  ]) {
    reply = { body: `<ListBucketResult>${xml}</ListBucketResult>` };
    assert.equal(await code(s.list({ limit: 1 })), "unavailable", xml);
  }
  reply = {
    body: `<ListBucketResult>${contents("tenant1/a &amp; b")}<IsTruncated>true</IsTruncated><NextContinuationToken>t1</NextContinuationToken></ListBucketResult>`,
  };
  assert.deepEqual(await s.list({}), {
    items: [
      {
        key: "tenant1/a & b",
        size: 1,
        etag: "e",
        lastModified: "2026-01-01T00:00:00.000Z",
      },
    ],
    nextCursor: "t1",
  });
});

test("a range answered with the whole object is refused", async () => {
  reply = {
    headers: { "last-modified": "Thu, 01 Jan 2026 00:00:00 GMT", etag: '"e"' },
    body: "whole object",
  };
  assert.equal(
    await code(
      source().read({ key: "tenant1/a", range: { start: 0, end: 1 } }),
    ),
    "unavailable",
  );
});

import assert from "node:assert/strict";
import type { LookupAddress } from "node:dns";
import { test } from "node:test";
import {
  checkEndpoint,
  guardedLookup,
  isPublicAddress,
  type Resolver,
} from "./index.js";

test("classifies addresses as public or not", () => {
  const nonPublic = [
    "0.0.0.0",
    "10.1.2.3",
    "100.64.0.1",
    "127.0.0.1",
    "169.254.169.254",
    "172.16.0.1",
    "172.31.255.255",
    "192.168.1.1",
    "198.18.0.1",
    "224.0.0.1",
    "255.255.255.255",
    "::",
    "::1",
    "::ffff:10.0.0.1",
    "::ffff:8.8.8.8",
    "64:ff9b::a00:1",
    "fc00::1",
    "fd12:3456::1",
    "fe80::1",
    "2002:a00:1::",
    "2001:db8::1",
    "not an address",
    "",
  ];
  for (const a of nonPublic) assert.equal(isPublicAddress(a), false, a);
  for (const a of ["8.8.8.8", "172.32.0.1", "1.1.1.1", "2606:4700::1111"]) {
    assert.equal(isPublicAddress(a), true, a);
  }
});

const resolver =
  (...addresses: string[]): Resolver =>
  (_host, done) =>
    done(
      null,
      addresses.map(
        (address): LookupAddress => ({
          address,
          family: address.includes(":") ? 6 : 4,
        }),
      ),
    );

const lookup = (
  fn: ReturnType<typeof guardedLookup>,
  all: boolean,
): Promise<{ err: NodeJS.ErrnoException | null; address: unknown }> =>
  new Promise((done) =>
    fn("mcp.example.com", { all }, (err, address) => done({ err, address })),
  );

test("hosts that end in several dots are still judged by their name", () => {
  for (const host of ["localhost..", "x.localhost..", "intranet.."]) {
    assert.throws(() => checkEndpoint(`https://${host}/`, {}), /./, host);
  }
});

test("a name resolving to any private address is refused", async () => {
  for (const found of [
    ["10.0.0.5"],
    ["8.8.8.8", "169.254.169.254"],
    ["::ffff:127.0.0.1"],
  ]) {
    const { err } = await lookup(guardedLookup({}, resolver(...found)), true);
    assert.equal(err?.code, "EBLOCKED", found.join());
  }
});

test("a name resolving to nothing is refused", async () => {
  const { err } = await lookup(guardedLookup({}, resolver()), false);
  assert.equal(err?.code, "EBLOCKED");
});

test("a public answer is passed through in either lookup form", async () => {
  const fn = guardedLookup({}, resolver("8.8.8.8"));
  assert.equal((await lookup(fn, false)).address, "8.8.8.8");
  assert.deepEqual((await lookup(fn, true)).address, [
    { address: "8.8.8.8", family: 4 },
  ]);
});

test("a resolver failure is passed on, not swallowed", async () => {
  const failing: Resolver = (_h, done) =>
    done(Object.assign(new Error("nope"), { code: "ENOTFOUND" }), []);
  const { err } = await lookup(guardedLookup({}, failing), false);
  assert.equal(err?.code, "ENOTFOUND");
});

test("the insecure development policy allows private addresses", async () => {
  const { err } = await lookup(
    guardedLookup({ allowInsecure: true }, resolver("127.0.0.1")),
    false,
  );
  assert.equal(err, null);
});

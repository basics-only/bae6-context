import assert from "node:assert/strict";
import { createServer, type Server } from "node:http";
import type { AddressInfo } from "node:net";
import { after, before, test } from "node:test";
import { exportJWK, generateKeyPair, SignJWT } from "jose";
import { createVerifier, identify, type Verifier } from "./identity.js";

let server: Server;
let verify: Verifier;
let key: CryptoKey;

before(async () => {
  const pair = await generateKeyPair("RS256");
  key = pair.privateKey;
  const jwk = { ...(await exportJWK(pair.publicKey)), kid: "k1", alg: "RS256" };
  server = createServer((_, res) => res.end(JSON.stringify({ keys: [jwk] })));
  await new Promise<void>((r) => server.listen(0, "127.0.0.1", r));
  const jwksUri = `http://127.0.0.1:${(server.address() as AddressInfo).port}/jwks`;
  verify = createVerifier(
    [
      {
        issuer: "https://acme.idp",
        audience: "bae6-ai",
        jwksUri,
        organization: "acme",
        application: "crm",
      },
      {
        issuer: "https://globex.idp",
        audience: "bae6-ai",
        jwksUri,
        organization: "globex",
        application: "crm",
      },
    ],
    { allowHttpJwks: true },
  );
});

after(() => server.close());

const token = (
  claims: Record<string, unknown> = {},
  iss = "https://acme.idp",
  aud = "bae6-ai",
) =>
  new SignJWT({ scope: "ai:invoke", ...claims })
    .setProtectedHeader({ alg: "RS256", kid: "k1" })
    .setIssuer(iss)
    .setAudience(aud)
    .setSubject("svc")
    .setExpirationTime("5m")
    .sign(key);

test("maps a valid token to the issuer's organization and application, ignoring claims", async () => {
  const who = await verify(
    await token({ organization: "globex", client_id: "svc" }),
  );
  assert.deepEqual(who, {
    subjectId: "svc",
    subjectType: "workload",
    organizationId: "acme",
    applicationId: "crm",
    scopes: ["ai:invoke"],
  });
});

test("rejects another audience, alg none and a future iat", async () => {
  assert.equal(
    await verify(await token({}, "https://acme.idp", "other")),
    null,
  );
  const b64 = (o: object) =>
    Buffer.from(JSON.stringify(o)).toString("base64url");
  const none = `${b64({ alg: "none" })}.${b64({ iss: "https://acme.idp", aud: "bae6-ai", sub: "x", exp: 9e9 })}.`;
  assert.equal(await verify(none), null);
  assert.equal(
    await verify(await token({ iat: Math.floor(Date.now() / 1000) + 3600 })),
    null,
  );
});

test("an end-user token from another tenant's issuer is refused", async () => {
  const workload = `Bearer ${await token({ client_id: "svc" })}`;
  const sameTenantUser = await token({}, "https://acme.idp");
  const otherTenantUser = await token({}, "https://globex.idp");
  const ok = await identify(verify, workload, sameTenantUser);
  assert.equal(ok?.subjectType, "user");
  assert.equal(ok?.workloadId, "svc");
  assert.equal(await identify(verify, workload, otherTenantUser), null);
});

test("refuses issuer config without a tenant mapping", () => {
  assert.throws(
    () =>
      createVerifier([
        {
          issuer: "i",
          audience: "a",
          jwksUri: "https://x/jwks",
          organization: "Acme",
          application: "crm",
        },
      ]),
    /organization/,
  );
});

test("refuses issuer config without an audience, which would skip the check", () => {
  const ok = {
    issuer: "i",
    audience: "a",
    jwksUri: "https://x/jwks",
    organization: "acme",
    application: "crm",
  };
  for (const bad of [
    { audience: undefined },
    { audience: "" },
    { issuer: 1 },
  ]) {
    assert.throws(
      () => createVerifier([{ ...ok, ...bad } as never]),
      /non-empty (audience|issuer)/,
    );
  }
});

import assert from "node:assert/strict";
import { test } from "node:test";
import { lazy, parseConfig } from "./config.js";

const env = { PG_URL: "postgres://r:pw@db/app", DBX_SECRET: "s3cret" };
const pg = {
  id: "crm",
  organization: "acme",
  kind: "postgres",
  connectionStringEnv: "PG_URL",
  poolMax: 5,
};

test("reads secrets from named environment variables", () => {
  const [p, d] = parseConfig(
    {
      sources: [
        { ...pg, limits: { maxRows: 100 } },
        {
          id: "lake",
          organization: "acme",
          kind: "databricks",
          host: "https://dbc.example.com",
          warehouseId: "abc",
          auth: {
            mode: "service_principal",
            clientId: "cid",
            clientSecretEnv: "DBX_SECRET",
          },
        },
      ],
    },
    env,
  );
  assert.deepEqual(p, {
    id: "crm",
    organization: "acme",
    kind: "postgres",
    maxConcurrent: 16,
    limits: { maxRows: 100 },
    connectionString: env.PG_URL,
    poolMax: 5,
  });
  assert.equal(d?.kind === "databricks" && d.auth.mode, "service_principal");
});

test("refuses inline secrets, unknown keys, missing env and duplicates", () => {
  for (const [source, why] of [
    [{ ...pg, connectionString: "postgres://x" }, /unknown connectionString/],
    [{ ...pg, connectionStringEnv: "MISSING" }, /MISSING is not set/],
    [{ ...pg, connectionStringEnv: "lower" }, /environment variable/],
    [{ ...pg, limits: { maxRow: 1 } }, /unknown maxRow/],
    [{ ...pg, limits: { maxRows: 0 } }, /maxRows/],
    [{ ...pg, organization: "Acme" }, /organization/],
    [{ ...pg, id: "../x" }, /id must match/],
    [{ ...pg, kind: "mysql" }, /kind/],
    [
      {
        id: "d",
        organization: "acme",
        kind: "databricks",
        host: "https://h",
        warehouseId: "w",
        auth: { mode: "delegated", clientSecretEnv: "DBX_SECRET" },
      },
      /unknown clientSecretEnv/,
    ],
    [
      {
        id: "b",
        organization: "acme",
        kind: "s3",
        region: "us-east-1",
        bucket: "b",
      },
      /prefix/,
    ],
  ] as const) {
    assert.throws(() => parseConfig({ sources: [source] }, env), why);
  }
  assert.throws(() => parseConfig({ sources: [pg, pg] }, env), /duplicate/);
  assert.doesNotThrow(() =>
    parseConfig({ sources: [pg, { ...pg, organization: "globex" }] }, env),
  );
});

test("a lazy open is shared, and retried after a failure", async () => {
  let n = 0;
  const open = lazy(async () => {
    n++;
    if (n === 1) throw new Error("down");
    return n;
  });
  await assert.rejects(open(), /down/);
  const [a, b] = await Promise.all([open(), open()]);
  assert.deepEqual([a, b, n], [2, 2, 2]);
});

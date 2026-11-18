import assert from "node:assert/strict";
import { after, before, test } from "node:test";
import { DataError, type QuerySource } from "@bae6/context-data";
import pg from "pg";
import { createPostgresSource } from "./index.js";

const admin = process.env.BAE6_DATABASE_URL;
const skip = admin ? false : "BAE6_DATABASE_URL not set";
const DB = "context_data_test";

function urlFor(user?: string) {
  const u = new URL(admin as string);
  if (user) {
    u.username = user;
    u.password = user;
  }
  u.pathname = `/${DB}`;
  return u.toString();
}

const sources: QuerySource[] = [];
async function source(user: string, extra: object = {}) {
  const s = await createPostgresSource({
    id: "pg-test",
    connectionString: urlFor(user),
    poolMax: 4,
    ...extra,
  });
  sources.push(s);
  return s;
}

async function asAdmin<T>(fn: (c: pg.Client) => Promise<T>) {
  const c = new pg.Client({ connectionString: urlFor() });
  await c.connect();
  try {
    return await fn(c);
  } finally {
    await c.end();
  }
}

const code = async (p: Promise<unknown>) => {
  try {
    await p;
  } catch (err) {
    assert.ok(err instanceof DataError, String(err));
    return err.code;
  }
  assert.fail("expected a DataError");
};

before(async () => {
  if (!admin) return;
  const root = new pg.Client({ connectionString: admin });
  await root.connect();
  await root.query(`CREATE DATABASE ${DB}`).catch((err) => {
    if (err.code !== "42P04") throw err;
  });
  for (const role of ["bae6_data_ro", "bae6_data_rw"]) {
    await root.query(
      `DO $$ BEGIN
         IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = '${role}') THEN
           CREATE ROLE ${role} LOGIN PASSWORD '${role}';
         END IF;
       END $$`,
    );
  }
  await root.end();
  await asAdmin(async (c) => {
    await c.query(`
      DROP SCHEMA IF EXISTS fixture CASCADE;
      CREATE SCHEMA fixture;
      CREATE TABLE fixture.orders (
        id int PRIMARY KEY, customer text NOT NULL, amount numeric(10,2),
        placed timestamptz NOT NULL, note text);
      INSERT INTO fixture.orders
        SELECT i, 'customer-' || i, i * 1.5, '2026-01-01T00:00:00Z'::timestamptz + i * interval '1 hour',
               CASE WHEN i % 2 = 0 THEN NULL ELSE 'note ' || i END
        FROM generate_series(1, 1200) AS i;
      CREATE TABLE fixture.salaries (name text, salary int);
      INSERT INTO fixture.salaries VALUES ('alice', 100);
      CREATE FUNCTION fixture.sneaky_write() RETURNS int LANGUAGE sql VOLATILE AS
        $$ INSERT INTO fixture.orders VALUES (9999, 'x', 0, now(), null) RETURNING 1 $$;
      GRANT CONNECT ON DATABASE ${DB} TO bae6_data_ro, bae6_data_rw;
      GRANT USAGE ON SCHEMA fixture TO bae6_data_ro, bae6_data_rw;
      GRANT SELECT ON fixture.orders TO bae6_data_ro;
      GRANT SELECT, INSERT, DELETE ON fixture.orders TO bae6_data_rw;
    `);
  });
});

after(() => Promise.all(sources.map((s) => s.close())));

test("refuses to start as a superuser", { skip }, async () => {
  await assert.rejects(
    createPostgresSource({ id: "pg", connectionString: urlFor(), poolMax: 1 }),
    /superuser/,
  );
});

test("returns rows in text form with column types", { skip }, async () => {
  const s = await source("bae6_data_ro");
  const r = await s.query({
    statement:
      "SELECT id, customer, amount, placed, note FROM fixture.orders WHERE id <= $1 ORDER BY id",
    parameters: [2],
  });
  assert.deepEqual(
    r.columns.map((c) => c.type),
    ["integer", "text", "numeric", "timestamptz", "text"],
  );
  assert.deepEqual(r.rows[0]?.slice(0, 3), ["1", "customer-1", "1.50"]);
  assert.equal(r.rows[1]?.[4], null);
  assert.equal(r.truncated, false);
  assert.equal(r.access, "service_principal");
});

test("stops at maxRows or the byte limit and says so", { skip }, async () => {
  const s = await source("bae6_data_ro");
  const all = "SELECT id FROM fixture.orders ORDER BY id";
  const capped = await s.query({ statement: all, maxRows: 1000 });
  assert.deepEqual([capped.rows.length, capped.truncated], [1000, true]);
  const exact = await s.query({
    statement: "SELECT id FROM fixture.orders WHERE id <= 1000",
    maxRows: 1000,
  });
  assert.deepEqual([exact.rows.length, exact.truncated], [1000, false]);
  const small = await source("bae6_data_ro", { limits: { maxBytes: 100 } });
  const bytes = await small.query({ statement: all });
  assert.equal(bytes.truncated, true);
  assert.ok(bytes.rows.length > 0 && bytes.rows.length < 20);
});

test(
  "write attempts fail and change nothing, even with a writable role",
  { skip },
  async () => {
    const s = await source("bae6_data_rw");
    const attempts: [string, string][] = [
      [
        "INSERT INTO fixture.orders VALUES (5000, 'x', 0, now(), null)",
        "invalid_request",
      ],
      ["SELECT 1; COMMIT; DELETE FROM fixture.orders", "invalid_request"],
      [
        "WITH d AS (DELETE FROM fixture.orders RETURNING id) SELECT * FROM d",
        "invalid_request",
      ],
      ["SELECT fixture.sneaky_write()", "forbidden"],
    ];
    for (const [statement, expected] of attempts) {
      assert.equal(await code(s.query({ statement })), expected, statement);
    }
    const count = await asAdmin((c) =>
      c.query("SELECT count(*)::int AS n FROM fixture.orders"),
    );
    assert.equal(count.rows[0].n, 1200);
  },
);

test("a table the role cannot read is forbidden", { skip }, async () => {
  const s = await source("bae6_data_ro");
  assert.equal(
    await code(s.query({ statement: "SELECT * FROM fixture.salaries" })),
    "forbidden",
  );
});

test(
  "a slow statement times out, and a cancelled one stops on the server",
  { skip },
  async () => {
    const s = await source("bae6_data_ro");
    let started = Date.now();
    assert.equal(
      await code(s.query({ statement: "SELECT pg_sleep(10)", timeoutMs: 300 })),
      "timeout",
    );
    assert.ok(Date.now() - started < 2000);

    const caller = new AbortController();
    setTimeout(() => caller.abort(), 200);
    started = Date.now();
    assert.equal(
      await code(
        s.query({ statement: "SELECT pg_sleep(10)", signal: caller.signal }),
      ),
      "timeout",
    );
    assert.ok(Date.now() - started < 2000);
    const running = await asAdmin((c) =>
      c.query(
        `SELECT count(*)::int AS n FROM pg_stat_activity
          WHERE usename = 'bae6_data_ro' AND state = 'active' AND query LIKE '%pg_sleep%'`,
      ),
    );
    assert.equal(running.rows[0].n, 0);
  },
);

test(
  "with every connection busy, extra queries are refused instead of queued",
  { skip },
  async () => {
    const s = await source("bae6_data_ro", { poolMax: 1 });
    const slow = { statement: "SELECT pg_sleep(0.5)" };
    const first = s.query(slow);
    const second = s.query(slow);
    assert.equal(await code(s.query(slow)), "unavailable");
    await Promise.all([first, second]);
  },
);

test("refuses delegated tokens and named parameters", { skip }, async () => {
  const s = await source("bae6_data_ro");
  assert.equal(
    await code(s.query({ statement: "SELECT 1", delegatedToken: "t" })),
    "invalid_request",
  );
  assert.equal(
    await code(s.query({ statement: "SELECT $1", parameters: { a: 1 } })),
    "invalid_request",
  );
});

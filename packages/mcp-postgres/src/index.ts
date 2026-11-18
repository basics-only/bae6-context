import type {
  AuditEvent,
  Change,
  Health,
  RegistryStore,
  Server,
  Version,
} from "@bae6/context-mcp";
import type { Pool, PoolClient } from "pg";

const SCHEMA = `
CREATE TABLE IF NOT EXISTS mcp_servers (
  organization text NOT NULL,
  id text NOT NULL,
  team text NOT NULL,
  name text NOT NULL,
  description text,
  created_at timestamptz NOT NULL,
  PRIMARY KEY (organization, id)
);
CREATE TABLE IF NOT EXISTS mcp_versions (
  organization text NOT NULL,
  server_id text NOT NULL,
  version text NOT NULL,
  position integer NOT NULL,
  state text NOT NULL CHECK (state IN ('pending', 'approved', 'revoked')),
  doc jsonb NOT NULL,
  PRIMARY KEY (organization, server_id, version),
  FOREIGN KEY (organization, server_id) REFERENCES mcp_servers (organization, id)
);
CREATE INDEX IF NOT EXISTS mcp_versions_approved
  ON mcp_versions (organization, server_id) WHERE state = 'approved';
CREATE TABLE IF NOT EXISTS mcp_audit (
  id text PRIMARY KEY,
  organization text NOT NULL,
  time timestamptz NOT NULL,
  actor text NOT NULL,
  action text NOT NULL,
  outcome text NOT NULL,
  request_id text NOT NULL,
  detail jsonb NOT NULL
);
CREATE INDEX IF NOT EXISTS mcp_audit_org_time ON mcp_audit (organization, time);
CREATE TABLE IF NOT EXISTS mcp_health (
  organization text NOT NULL,
  server_id text NOT NULL,
  version text NOT NULL,
  doc jsonb NOT NULL,
  PRIMARY KEY (organization, server_id, version)
);
`;

const MIGRATION_LOCK = 0x6d637001;

interface ServerRow {
  organization: string;
  id: string;
  team: string;
  name: string;
  description: string | null;
  created_at: Date;
}

interface VersionRow {
  organization: string;
  server_id: string;
  doc: Version;
}

const toServer = (row: ServerRow, versions: Version[]): Server => ({
  id: row.id,
  organization: row.organization,
  team: row.team,
  name: row.name,
  ...(row.description === null ? {} : { description: row.description }),
  createdAt: row.created_at.toISOString(),
  versions,
});

async function inTransaction<T>(
  pool: Pool,
  fn: (client: PoolClient) => Promise<T>,
): Promise<T> {
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    const result = await fn(client);
    await client.query("COMMIT");
    client.release();
    return result;
  } catch (err) {
    const rolledBack = await client.query("ROLLBACK").then(
      () => true,
      () => false,
    );
    client.release(rolledBack ? undefined : true);
    throw err;
  }
}

export class PgRegistryStore implements RegistryStore {
  constructor(private readonly pool: Pool) {}

  /** Idempotent; concurrent replicas serialize on a transaction-scoped advisory lock. */
  static async migrate(pool: Pool) {
    await inTransaction(pool, async (client) => {
      await client.query("SELECT pg_advisory_xact_lock($1)", [MIGRATION_LOCK]);
      await client.query(SCHEMA);
    });
  }

  private async load(
    client: Pool | PoolClient,
    org: string,
    servers: ServerRow[],
    approvedOnly: boolean,
  ): Promise<Server[]> {
    if (servers.length === 0) return [];
    const { rows } = await client.query<VersionRow>(
      `SELECT organization, server_id, doc FROM mcp_versions
       WHERE organization = $1 AND server_id = ANY($2)
         AND ($3::boolean = false OR state = 'approved')
       ORDER BY server_id COLLATE "C", position`,
      [org, servers.map((s) => s.id), approvedOnly],
    );
    const byServer = new Map<string, Version[]>();
    for (const row of rows) {
      byServer.set(row.server_id, [
        ...(byServer.get(row.server_id) ?? []),
        row.doc,
      ]);
    }
    return servers.flatMap((s) => {
      const versions = byServer.get(s.id) ?? [];
      return approvedOnly && versions.length === 0
        ? []
        : [toServer(s, versions)];
    });
  }

  async mutate(
    org: string,
    id: string,
    change: (current: Server | undefined) => Change | null,
  ): Promise<Server | undefined> {
    return inTransaction(this.pool, async (client) => {
      await client.query(
        "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
        [`${org}/${id}`],
      );
      const { rows } = await client.query<ServerRow>(
        "SELECT * FROM mcp_servers WHERE organization = $1 AND id = $2",
        [org, id],
      );
      const current = (await this.load(client, org, rows, false))[0];
      const result = change(current && structuredClone(current));
      if (!result) return current;
      const { next, audit } = result;
      await client.query(
        `INSERT INTO mcp_servers (organization, id, team, name, description, created_at)
         VALUES ($1, $2, $3, $4, $5, $6)
         ON CONFLICT (organization, id) DO UPDATE
           SET team = $3, name = $4, description = $5`,
        [
          org,
          id,
          next.team,
          next.name,
          next.description ?? null,
          next.createdAt,
        ],
      );
      for (const [position, v] of next.versions.entries()) {
        await client.query(
          `INSERT INTO mcp_versions (organization, server_id, version, position, state, doc)
           VALUES ($1, $2, $3, $4, $5, $6)
           ON CONFLICT (organization, server_id, version) DO UPDATE
             SET state = $5, doc = $6`,
          [org, id, v.version, position, v.state, JSON.stringify(v)],
        );
      }
      await this.insertAudit(client, audit);
      return next;
    });
  }

  private insertAudit(client: PoolClient, e: AuditEvent) {
    return client.query(
      `INSERT INTO mcp_audit (id, organization, time, actor, action, outcome, request_id, detail)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
      [
        e.id,
        e.organization,
        e.time,
        e.actor,
        e.action,
        e.outcome,
        e.requestId,
        JSON.stringify(e.detail),
      ],
    );
  }

  async get(org: string, id: string, approvedOnly: boolean) {
    const { rows } = await this.pool.query<ServerRow>(
      "SELECT * FROM mcp_servers WHERE organization = $1 AND id = $2",
      [org, id],
    );
    return (await this.load(this.pool, org, rows, approvedOnly))[0];
  }

  async list(
    org: string,
    page: { after?: string; limit: number; approvedOnly: boolean },
  ) {
    const { rows } = await this.pool.query<ServerRow>(
      `SELECT s.* FROM mcp_servers s
       WHERE s.organization = $1
         AND ($2::text IS NULL OR s.id COLLATE "C" > $2 COLLATE "C")
         AND ($4::boolean = false OR EXISTS (
           SELECT 1 FROM mcp_versions v
           WHERE v.organization = s.organization AND v.server_id = s.id
             AND v.state = 'approved'))
       ORDER BY s.id COLLATE "C"
       LIMIT $3`,
      [org, page.after ?? null, page.limit, page.approvedOnly],
    );
    return this.load(this.pool, org, rows, page.approvedOnly);
  }

  async recordHealth(org: string, id: string, health: Health) {
    await this.pool.query(
      `INSERT INTO mcp_health (organization, server_id, version, doc)
       VALUES ($1, $2, $3, $4)
       ON CONFLICT (organization, server_id, version) DO UPDATE SET doc = $4`,
      [org, id, health.version, JSON.stringify(health)],
    );
  }

  async getHealth(org: string, id: string, version: string) {
    const { rows } = await this.pool.query<{ doc: Health }>(
      `SELECT doc FROM mcp_health
       WHERE organization = $1 AND server_id = $2 AND version = $3`,
      [org, id, version],
    );
    return rows[0]?.doc;
  }
}

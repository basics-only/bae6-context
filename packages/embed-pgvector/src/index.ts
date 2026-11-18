import {
  checkPrincipals,
  EmbedError,
  type Hit,
  type IndexedDocument,
  type Scope,
  type SearchRequest,
  type VectorIndex,
} from "@bae6/context-embed";
import type { Pool, PoolClient } from "pg";

export const MAX_SEARCH_LIMIT = 50;
const MAX_DIMENSIONS = 4096;
const MIGRATION_LOCK = 6_201_104;

const SCHEMA = `
CREATE EXTENSION IF NOT EXISTS vector;
CREATE TABLE IF NOT EXISTS collections (
  organization_id text NOT NULL,
  collection_id text NOT NULL,
  model_id text NOT NULL,
  dimensions int NOT NULL CHECK (dimensions BETWEEN 1 AND ${MAX_DIMENSIONS}),
  PRIMARY KEY (organization_id, collection_id)
);
CREATE TABLE IF NOT EXISTS documents (
  organization_id text NOT NULL,
  collection_id text NOT NULL,
  key text NOT NULL,
  revision text NOT NULL,
  principals text[] NOT NULL,
  acl_synced_at timestamptz NOT NULL,
  PRIMARY KEY (organization_id, collection_id, key),
  FOREIGN KEY (organization_id, collection_id)
    REFERENCES collections ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS chunks (
  organization_id text NOT NULL,
  collection_id text NOT NULL,
  key text NOT NULL,
  chunk_id text NOT NULL,
  span_start int NOT NULL,
  span_end int NOT NULL,
  body text NOT NULL,
  embedding vector NOT NULL,
  PRIMARY KEY (organization_id, collection_id, chunk_id),
  FOREIGN KEY (organization_id, collection_id, key)
    REFERENCES documents ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS chunks_by_document
  ON chunks (organization_id, collection_id, key);
`;

const invalid = (message: string) => new EmbedError("invalid_request", message);
const vectorLiteral = (v: number[]) => `[${v.join(",")}]`;

/**
 * Exact (sequential) nearest-neighbour search per collection. Every query is scoped to one
 * organization and collection, and search only ranks documents the caller's principals
 * overlap and whose ACL is fresh.
 */
export class PgVectorIndex implements VectorIndex {
  constructor(private readonly pool: Pool) {}

  /** Idempotent; concurrent runtimes serialize on an advisory lock (ADR-0007). */
  static async migrate(pool: Pool) {
    const client = await pool.connect();
    try {
      await client.query("SELECT pg_advisory_lock($1)", [MIGRATION_LOCK]);
      try {
        await client.query(SCHEMA);
      } finally {
        await client.query("SELECT pg_advisory_unlock($1)", [MIGRATION_LOCK]);
      }
    } finally {
      client.release();
    }
  }

  /** Creates the collection, or checks it is the same model and size: a change needs a new collection. */
  async ensureCollection(scope: Scope, modelId: string, dimensions: number) {
    if (
      !Number.isInteger(dimensions) ||
      dimensions < 1 ||
      dimensions > MAX_DIMENSIONS
    ) {
      throw invalid(`dimensions must be 1 to ${MAX_DIMENSIONS}.`);
    }
    await this.pool.query(
      `INSERT INTO collections VALUES ($1, $2, $3, $4)
       ON CONFLICT DO NOTHING`,
      [scope.organizationId, scope.collectionId, modelId, dimensions],
    );
    const { rows } = await this.pool.query(
      `SELECT model_id, dimensions FROM collections
       WHERE organization_id = $1 AND collection_id = $2`,
      [scope.organizationId, scope.collectionId],
    );
    if (rows[0].model_id !== modelId || rows[0].dimensions !== dimensions) {
      throw invalid("the collection was created with a different model.");
    }
  }

  private async dimensions(db: Pool | PoolClient, scope: Scope) {
    const { rows } = await db.query(
      `SELECT dimensions FROM collections
       WHERE organization_id = $1 AND collection_id = $2`,
      [scope.organizationId, scope.collectionId],
    );
    if (rows.length === 0) throw invalid("unknown collection.");
    return rows[0].dimensions as number;
  }

  async upsert(scope: Scope, doc: IndexedDocument, signal?: AbortSignal) {
    signal?.throwIfAborted();
    const principals = checkPrincipals(doc.principals);
    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");
      const dims = await this.dimensions(client, scope);
      if (doc.chunks.some((c) => c.vector.length !== dims)) {
        throw invalid("a vector does not match the collection's dimensions.");
      }
      const key = [scope.organizationId, scope.collectionId, doc.key];
      await client.query(
        `INSERT INTO documents VALUES ($1, $2, $3, $4, $5, now())
         ON CONFLICT (organization_id, collection_id, key)
         DO UPDATE SET revision = $4, principals = $5, acl_synced_at = now()`,
        [...key, doc.revision, principals],
      );
      await client.query(
        `DELETE FROM chunks
         WHERE organization_id = $1 AND collection_id = $2 AND key = $3`,
        key,
      );
      for (const c of doc.chunks) {
        await client.query(
          `INSERT INTO chunks VALUES ($1, $2, $3, $4, $5, $6, $7, $8::vector)`,
          [...key, c.id, c.start, c.end, c.text, vectorLiteral(c.vector)],
        );
      }
      await client.query("COMMIT");
    } catch (err) {
      await client.query("ROLLBACK").catch(() => {});
      throw err;
    } finally {
      client.release();
    }
  }

  async refreshAcl(
    scope: Scope,
    key: string,
    principals: string[],
    signal?: AbortSignal,
  ) {
    signal?.throwIfAborted();
    await this.pool.query(
      `UPDATE documents SET principals = $4, acl_synced_at = now()
       WHERE organization_id = $1 AND collection_id = $2 AND key = $3`,
      [
        scope.organizationId,
        scope.collectionId,
        key,
        checkPrincipals(principals),
      ],
    );
  }

  async remove(scope: Scope, key: string, signal?: AbortSignal) {
    signal?.throwIfAborted();
    await this.pool.query(
      `DELETE FROM documents
       WHERE organization_id = $1 AND collection_id = $2 AND key = $3`,
      [scope.organizationId, scope.collectionId, key],
    );
  }

  async revisions(scope: Scope, limit: number, signal?: AbortSignal) {
    signal?.throwIfAborted();
    const { rows } = await this.pool.query(
      `SELECT key, revision FROM documents
       WHERE organization_id = $1 AND collection_id = $2 LIMIT $3`,
      [scope.organizationId, scope.collectionId, limit + 1],
    );
    if (rows.length > limit) {
      throw new EmbedError(
        "too_large",
        `a collection has at most ${limit} documents.`,
      );
    }
    return new Map<string, string>(rows.map((r) => [r.key, r.revision]));
  }

  async search(
    scope: Scope,
    req: SearchRequest,
    signal?: AbortSignal,
  ): Promise<Hit[]> {
    signal?.throwIfAborted();
    if (
      !Number.isInteger(req.limit) ||
      req.limit < 1 ||
      req.limit > MAX_SEARCH_LIMIT ||
      !Number.isInteger(req.maxAclAgeMs) ||
      req.maxAclAgeMs < 1 ||
      !req.vector.every(Number.isFinite)
    ) {
      throw invalid(`limit must be 1 to ${MAX_SEARCH_LIMIT}.`);
    }
    const principals = checkPrincipals(req.principals);
    if (principals.length === 0) return [];
    const dims = await this.dimensions(this.pool, scope);
    if (req.vector.length !== dims) {
      throw invalid("the vector does not match the collection's dimensions.");
    }
    const { rows } = await this.pool.query(
      `SELECT c.chunk_id, c.key, d.revision, c.span_start, c.span_end, c.body,
              1 - (c.embedding <=> $3::vector) AS score
       FROM chunks c
       JOIN documents d USING (organization_id, collection_id, key)
       WHERE c.organization_id = $1 AND c.collection_id = $2
         AND d.principals && $4::text[]
         AND d.acl_synced_at > now() - ($5 * interval '1 millisecond')
       ORDER BY c.embedding <=> $3::vector, c.chunk_id
       LIMIT $6`,
      [
        scope.organizationId,
        scope.collectionId,
        vectorLiteral(req.vector),
        principals,
        req.maxAclAgeMs,
        req.limit,
      ],
    );
    return rows.map((r) => ({
      chunkId: r.chunk_id,
      key: r.key,
      revision: r.revision,
      start: r.span_start,
      end: r.span_end,
      text: r.body,
      score: Number(r.score),
    }));
  }
}

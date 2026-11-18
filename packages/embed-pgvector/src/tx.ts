import type { Pool, PoolClient } from "pg";

export async function inTransaction<T>(
  pool: Pool,
  fn: (client: PoolClient) => Promise<T>,
  { readOnly = false, timeoutMs = 0 } = {},
): Promise<T> {
  const client = await pool.connect();
  try {
    await client.query(`BEGIN${readOnly ? " READ ONLY" : ""}`);
    if (timeoutMs > 0) {
      await client.query(`SET LOCAL statement_timeout = ${timeoutMs | 0}`);
    }
    const result = await fn(client);
    await client.query("COMMIT");
    client.release();
    return result;
  } catch (err) {
    // A connection that cannot roll back is discarded, not reused.
    const rolledBack = await client.query("ROLLBACK").then(
      () => true,
      () => false,
    );
    client.release(rolledBack ? undefined : true);
    throw err;
  }
}

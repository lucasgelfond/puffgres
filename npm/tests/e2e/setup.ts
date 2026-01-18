/**
 * E2E test setup - manages test database and cleanup.
 */

import { Client } from 'pg';

export interface TestContext {
  pgClient: Client;
  namespace: string;
  slotName: string;
}

/**
 * Create a unique test namespace to avoid collisions.
 */
export function createTestNamespace(): string {
  const timestamp = Date.now();
  const random = Math.random().toString(36).substring(7);
  return `test_${timestamp}_${random}`;
}

/**
 * Set up the test database with required tables and replication slot.
 */
export async function setupTestDatabase(): Promise<TestContext> {
  const connectionString = process.env.DATABASE_URL;
  if (!connectionString) {
    throw new Error('DATABASE_URL environment variable is required for E2E tests');
  }

  const client = new Client({ connectionString });
  await client.connect();

  const namespace = createTestNamespace();
  const slotName = `puffgres_${namespace}`;

  // Create test table
  await client.query(`
    CREATE TABLE IF NOT EXISTS test_users (
      id SERIAL PRIMARY KEY,
      name TEXT NOT NULL,
      email TEXT,
      status TEXT DEFAULT 'active',
      created_at TIMESTAMPTZ DEFAULT NOW()
    )
  `);

  // Create puffgres state tables
  await client.query(`
    CREATE TABLE IF NOT EXISTS __puffgres_migrations (
      id SERIAL PRIMARY KEY,
      version INTEGER NOT NULL,
      mapping_name TEXT NOT NULL,
      content_hash TEXT NOT NULL,
      applied_at TIMESTAMPTZ DEFAULT NOW(),
      UNIQUE(version, mapping_name)
    )
  `);

  await client.query(`
    CREATE TABLE IF NOT EXISTS __puffgres_checkpoints (
      mapping_name TEXT PRIMARY KEY,
      lsn BIGINT NOT NULL,
      events_processed BIGINT DEFAULT 0,
      updated_at TIMESTAMPTZ DEFAULT NOW()
    )
  `);

  await client.query(`
    CREATE TABLE IF NOT EXISTS __puffgres_dlq (
      id SERIAL PRIMARY KEY,
      mapping_name TEXT NOT NULL,
      lsn BIGINT NOT NULL,
      event_json JSONB NOT NULL,
      error_message TEXT NOT NULL,
      error_kind TEXT NOT NULL,
      retry_count INT DEFAULT 0,
      created_at TIMESTAMPTZ DEFAULT NOW()
    )
  `);

  await client.query(`
    CREATE TABLE IF NOT EXISTS __puffgres_backfill (
      mapping_name TEXT PRIMARY KEY,
      last_id TEXT,
      total_rows BIGINT,
      processed_rows BIGINT,
      status TEXT,
      updated_at TIMESTAMPTZ DEFAULT NOW()
    )
  `);

  // Create replication slot (ignore if exists)
  try {
    await client.query(`
      SELECT * FROM pg_create_logical_replication_slot($1, 'wal2json')
    `, [slotName]);
  } catch (error: any) {
    // Ignore if slot already exists
    if (!error.message.includes('already exists')) {
      throw error;
    }
  }

  return {
    pgClient: client,
    namespace,
    slotName,
  };
}

/**
 * Clean up test resources.
 */
export async function teardownTestDatabase(ctx: TestContext): Promise<void> {
  const { pgClient, slotName, namespace } = ctx;

  try {
    // Drop replication slot
    await pgClient.query(`
      SELECT pg_drop_replication_slot($1)
    `, [slotName]);
  } catch (error: any) {
    // Ignore if slot doesn't exist
    if (!error.message.includes('does not exist')) {
      console.warn('Failed to drop replication slot:', error.message);
    }
  }

  try {
    // Clean up test data
    await pgClient.query('DELETE FROM test_users');
    await pgClient.query('DELETE FROM __puffgres_checkpoints WHERE mapping_name LIKE $1', [`%${namespace}%`]);
    await pgClient.query('DELETE FROM __puffgres_dlq WHERE mapping_name LIKE $1', [`%${namespace}%`]);
    await pgClient.query('DELETE FROM __puffgres_backfill WHERE mapping_name LIKE $1', [`%${namespace}%`]);
  } catch (error: any) {
    console.warn('Failed to clean up test data:', error.message);
  }

  await pgClient.end();
}

/**
 * Insert test data into the test_users table.
 */
export async function insertTestUser(
  client: Client,
  data: { name: string; email?: string; status?: string }
): Promise<number> {
  const result = await client.query(
    'INSERT INTO test_users (name, email, status) VALUES ($1, $2, $3) RETURNING id',
    [data.name, data.email || null, data.status || 'active']
  );
  return result.rows[0].id;
}

/**
 * Update a test user.
 */
export async function updateTestUser(
  client: Client,
  id: number,
  data: { name?: string; email?: string; status?: string }
): Promise<void> {
  const updates: string[] = [];
  const values: any[] = [];
  let paramIndex = 1;

  if (data.name !== undefined) {
    updates.push(`name = $${paramIndex++}`);
    values.push(data.name);
  }
  if (data.email !== undefined) {
    updates.push(`email = $${paramIndex++}`);
    values.push(data.email);
  }
  if (data.status !== undefined) {
    updates.push(`status = $${paramIndex++}`);
    values.push(data.status);
  }

  values.push(id);
  await client.query(
    `UPDATE test_users SET ${updates.join(', ')} WHERE id = $${paramIndex}`,
    values
  );
}

/**
 * Delete a test user.
 */
export async function deleteTestUser(client: Client, id: number): Promise<void> {
  await client.query('DELETE FROM test_users WHERE id = $1', [id]);
}

/**
 * Wait for a condition to be true.
 */
export async function waitFor(
  condition: () => Promise<boolean>,
  timeoutMs: number = 30000,
  intervalMs: number = 500
): Promise<void> {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    if (await condition()) {
      return;
    }
    await new Promise((resolve) => setTimeout(resolve, intervalMs));
  }
  throw new Error(`Timeout waiting for condition after ${timeoutMs}ms`);
}

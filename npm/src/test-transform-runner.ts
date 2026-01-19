#!/usr/bin/env node
/**
 * Test transform runner - generates fake data from Postgres schema and runs transforms.
 *
 * Reads migration files, queries Postgres for column schemas, generates fake data
 * using faker.js, and runs transforms to show the output.
 *
 * Usage:
 *   pnpm test-transform-runner [migration-name]
 *
 * Output:
 *   JSON with fakeRow, event, and result fields.
 */

import { readFileSync, existsSync, readdirSync } from 'fs';
import { resolve, join, dirname } from 'path';
import { config as dotenvConfig } from 'dotenv';

// Load .env from current directory or any parent directory
function loadEnvFromParents(): void {
  let dir = process.cwd();

  while (true) {
    const envPath = join(dir, '.env');
    if (existsSync(envPath)) {
      dotenvConfig({ path: envPath });
      return;
    }
    const parent = dirname(dir);
    if (parent === dir) {
      // Reached filesystem root
      break;
    }
    dir = parent;
  }
}

// Load .env before anything else
loadEnvFromParents();
import { faker } from '@faker-js/faker';
import { parse as parseToml } from 'toml';
import type { RowEvent, Action, TransformContext, DocumentId } from '../types/index.js';
import { createTransformContext, type ContextConfig } from './context.js';

interface ColumnSchema {
  name: string;
  data_type: string;
}

interface MigrationConfig {
  version: number;
  mapping_name: string;
  namespace: string;
  columns?: string[]; // Optional when using a custom transform
  source: {
    schema: string;
    table: string;
  };
  id: {
    column: string;
    type: string;
  };
  transform?: {
    path: string;
  };
}

interface PuffgresToml {
  postgres: {
    connection_string: string;
  };
}

/**
 * Resolve environment variable references in a string.
 */
function resolveEnvVars(str: string): string {
  return str.replace(/\$\{(\w+)\}/g, (_, name) => process.env[name] || '');
}

/**
 * Find puffgres.toml in current directory or parent.
 */
function findPuffgresToml(): string | null {
  const candidates = [
    resolve(process.cwd(), 'puffgres.toml'),
    resolve(process.cwd(), '..', 'puffgres.toml'),
  ];

  for (const candidate of candidates) {
    if (existsSync(candidate)) {
      return candidate;
    }
  }
  return null;
}

/**
 * Find migrations directory.
 */
function findMigrationsDir(): string | null {
  const candidates = [
    resolve(process.cwd(), 'puffgres', 'migrations'),
    resolve(process.cwd(), 'migrations'),
    resolve(process.cwd(), '..', 'puffgres', 'migrations'),
  ];

  for (const candidate of candidates) {
    if (existsSync(candidate)) {
      return candidate;
    }
  }
  return null;
}

/**
 * Load a migration by name or get the first one.
 * Searches by filename first, then by mapping_name inside the TOML.
 */
function loadMigration(migrationsDir: string, name?: string): { path: string; config: MigrationConfig } | null {
  const files = readdirSync(migrationsDir).filter((f) => f.endsWith('.toml'));

  if (files.length === 0) {
    return null;
  }

  let targetFile: string | undefined;

  if (name) {
    // First try matching by filename
    targetFile = files.find((f) => f.includes(name) || f === name || f === `${name}.toml`);

    // If not found, try matching by mapping_name inside the TOML
    if (!targetFile) {
      for (const file of files) {
        const fullPath = join(migrationsDir, file);
        const content = readFileSync(fullPath, 'utf-8');
        const config = parseToml(content) as MigrationConfig;
        if (config.mapping_name && (config.mapping_name === name || config.mapping_name.includes(name))) {
          targetFile = file;
          break;
        }
      }
    }
  }

  if (!targetFile) {
    targetFile = files[0];
  }

  const fullPath = join(migrationsDir, targetFile);
  const content = readFileSync(fullPath, 'utf-8');
  const config = parseToml(content) as MigrationConfig;

  return { path: fullPath, config };
}

/**
 * Query Postgres for column information.
 */
async function getColumnSchemas(connectionString: string, schema: string, table: string): Promise<ColumnSchema[]> {
  // Dynamic import to avoid requiring pg at module load time
  const pg = await import('pg');
  const client = new pg.default.Client({ connectionString });

  try {
    await client.connect();

    const result = await client.query(
      `
      SELECT column_name as name, data_type
      FROM information_schema.columns
      WHERE table_schema = $1 AND table_name = $2
      ORDER BY ordinal_position
    `,
      [schema, table]
    );

    return result.rows;
  } finally {
    await client.end();
  }
}

/**
 * Generate a fake value based on PostgreSQL data type.
 */
function generateFakeValue(columnName: string, dataType: string): unknown {
  // Try to infer from column name first
  const lowerName = columnName.toLowerCase();

  // Common column name patterns
  if (lowerName === 'id' || lowerName.endsWith('_id')) {
    return faker.number.int({ min: 1, max: 100000 });
  }
  if (lowerName === 'uuid' || lowerName.endsWith('_uuid')) {
    return faker.string.uuid();
  }
  if (lowerName === 'email' || lowerName.includes('email')) {
    return faker.internet.email();
  }
  if (lowerName === 'name' || lowerName === 'full_name' || lowerName === 'fullname') {
    return faker.person.fullName();
  }
  if (lowerName === 'first_name' || lowerName === 'firstname') {
    return faker.person.firstName();
  }
  if (lowerName === 'last_name' || lowerName === 'lastname') {
    return faker.person.lastName();
  }
  if (lowerName === 'username' || lowerName === 'user_name') {
    return faker.internet.username();
  }
  if (lowerName === 'phone' || lowerName.includes('phone')) {
    return faker.phone.number();
  }
  if (lowerName === 'address' || lowerName.includes('address')) {
    return faker.location.streetAddress();
  }
  if (lowerName === 'city') {
    return faker.location.city();
  }
  if (lowerName === 'country') {
    return faker.location.country();
  }
  if (lowerName === 'zip' || lowerName === 'zipcode' || lowerName === 'postal_code') {
    return faker.location.zipCode();
  }
  if (lowerName === 'url' || lowerName.includes('url') || lowerName === 'website') {
    return faker.internet.url();
  }
  if (lowerName === 'avatar' || lowerName === 'image' || lowerName.includes('image_url')) {
    return faker.image.avatar();
  }
  if (lowerName === 'description' || lowerName === 'bio' || lowerName === 'about') {
    return faker.lorem.paragraph();
  }
  if (lowerName === 'title' || lowerName === 'subject') {
    return faker.lorem.sentence();
  }
  if (lowerName === 'content' || lowerName === 'body' || lowerName === 'text') {
    return faker.lorem.paragraphs(2);
  }
  if (lowerName === 'status') {
    return faker.helpers.arrayElement(['active', 'inactive', 'pending', 'archived']);
  }
  if (lowerName === 'type' || lowerName === 'category') {
    return faker.helpers.arrayElement(['type_a', 'type_b', 'type_c']);
  }
  if (lowerName === 'price' || lowerName === 'amount' || lowerName === 'cost') {
    return faker.commerce.price({ min: 1, max: 1000, dec: 2 });
  }
  if (lowerName === 'quantity' || lowerName === 'count') {
    return faker.number.int({ min: 1, max: 100 });
  }
  if (
    lowerName === 'created_at' ||
    lowerName === 'updated_at' ||
    lowerName === 'deleted_at' ||
    lowerName.endsWith('_at')
  ) {
    return faker.date.recent().toISOString();
  }
  if (lowerName === 'date' || lowerName.includes('date')) {
    return faker.date.recent().toISOString().split('T')[0];
  }
  if (
    lowerName === 'is_active' ||
    lowerName === 'is_deleted' ||
    lowerName === 'active' ||
    lowerName.startsWith('is_') ||
    lowerName.startsWith('has_')
  ) {
    return faker.datatype.boolean();
  }
  if (lowerName === 'tags' || lowerName === 'labels') {
    return [faker.word.noun(), faker.word.noun(), faker.word.noun()];
  }

  // Fall back to data type
  const lowerType = dataType.toLowerCase();

  if (lowerType === 'uuid') {
    return faker.string.uuid();
  }
  if (lowerType === 'boolean' || lowerType === 'bool') {
    return faker.datatype.boolean();
  }
  if (
    lowerType === 'integer' ||
    lowerType === 'int' ||
    lowerType === 'int4' ||
    lowerType === 'int8' ||
    lowerType === 'bigint' ||
    lowerType === 'smallint' ||
    lowerType === 'int2'
  ) {
    return faker.number.int({ min: 1, max: 10000 });
  }
  if (
    lowerType === 'real' ||
    lowerType === 'float4' ||
    lowerType === 'float8' ||
    lowerType === 'double precision' ||
    lowerType === 'numeric' ||
    lowerType === 'decimal'
  ) {
    return faker.number.float({ min: 0, max: 1000, fractionDigits: 2 });
  }
  if (
    lowerType === 'text' ||
    lowerType === 'varchar' ||
    lowerType === 'character varying' ||
    lowerType === 'char' ||
    lowerType === 'character'
  ) {
    return faker.lorem.sentence();
  }
  if (lowerType === 'timestamp' || lowerType === 'timestamp with time zone' || lowerType === 'timestamptz') {
    return faker.date.recent().toISOString();
  }
  if (lowerType === 'date') {
    return faker.date.recent().toISOString().split('T')[0];
  }
  if (lowerType === 'time' || lowerType === 'time with time zone' || lowerType === 'timetz') {
    return faker.date.recent().toISOString().split('T')[1];
  }
  if (lowerType === 'json' || lowerType === 'jsonb') {
    return { key: faker.lorem.word(), value: faker.lorem.sentence() };
  }
  if (lowerType.startsWith('_') || lowerType.includes('[]') || lowerType === 'array') {
    // Array type
    return [faker.lorem.word(), faker.lorem.word()];
  }

  // Default to string
  return faker.lorem.word();
}

/**
 * Generate a fake ID based on ID type.
 */
function generateFakeId(idType: string): DocumentId {
  switch (idType) {
    case 'uuid':
      return faker.string.uuid();
    case 'int':
      return faker.number.int({ min: -100000, max: 100000 });
    case 'uint':
      return faker.number.int({ min: 1, max: 100000 });
    case 'string':
    default:
      return faker.string.alphanumeric(10);
  }
}

async function main(): Promise<void> {
  const args = process.argv.slice(2);
  const migrationName = args[0];

  // Check if first arg is a JSON config (legacy mode from Rust CLI)
  if (migrationName && migrationName.startsWith('{')) {
    // Legacy mode: parse JSON config directly
    const config = JSON.parse(migrationName);

    const fakeRow: Record<string, unknown> = {};
    for (const col of config.columns) {
      fakeRow[col.name] = generateFakeValue(col.name, col.data_type);
    }

    const id = generateFakeId(config.id_type);
    if (config.id_column && config.id_column in fakeRow) {
      fakeRow[config.id_column] = id;
    }

    const event: RowEvent = {
      op: config.op as 'insert' | 'update' | 'delete',
      schema: config.schema,
      table: config.table,
      new: config.op === 'delete' ? undefined : fakeRow,
      old: config.op === 'insert' ? undefined : fakeRow,
      lsn: 0,
    };

    const fullPath = resolve(process.cwd(), config.transform_path);
    const module = await import(fullPath);
    const transform = module.default;

    if (typeof transform !== 'function') {
      throw new Error(`Transform at ${config.transform_path} must export a default function`);
    }

    const migrationInfo = {
      name: config.mapping_name || 'legacy',
      namespace: config.namespace || 'default',
      table: `${config.schema}.${config.table}`,
    };
    const contextConfig: ContextConfig = {
      migration: migrationInfo,
      env: process.env as Record<string, string>,
    };
    const ctx = createTransformContext(contextConfig);
    const result: Action = await transform(event, id, ctx);

    console.log(JSON.stringify({ fakeRow, result }));
    return;
  }

  // New mode: find config and migrations automatically
  try {
    // Find puffgres.toml
    const configPath = findPuffgresToml();
    if (!configPath) {
      console.error(
        JSON.stringify({
          error: 'Could not find puffgres.toml. Run from a puffgres project directory.',
        })
      );
      process.exit(1);
    }

    // Parse config
    const configContent = readFileSync(configPath, 'utf-8');
    const puffgresConfig = parseToml(configContent) as PuffgresToml;
    const connectionString = resolveEnvVars(puffgresConfig.postgres.connection_string);

    if (!connectionString) {
      console.error(
        JSON.stringify({
          error: 'DATABASE_URL environment variable not set.',
        })
      );
      process.exit(1);
    }

    // Find migrations
    const migrationsDir = findMigrationsDir();
    if (!migrationsDir) {
      console.error(
        JSON.stringify({
          error: 'Could not find migrations directory.',
        })
      );
      process.exit(1);
    }

    // Load migration
    const migration = loadMigration(migrationsDir, migrationName);
    if (!migration) {
      console.error(
        JSON.stringify({
          error: 'No migrations found.',
        })
      );
      process.exit(1);
    }

    const { config } = migration;

    // Get column schemas from Postgres
    const columnSchemas = await getColumnSchemas(connectionString, config.source.schema, config.source.table);

    // Filter to only columns in migration config (if columns specified), otherwise use all
    const configColumns = config.columns;
    const relevantColumns = configColumns
      ? columnSchemas.filter((col) => configColumns.includes(col.name))
      : columnSchemas;

    // Generate fake row
    const fakeRow: Record<string, unknown> = {};
    for (const col of relevantColumns) {
      fakeRow[col.name] = generateFakeValue(col.name, col.data_type);
    }

    // Generate ID
    const id = generateFakeId(config.id.type);
    if (config.id.column in fakeRow) {
      fakeRow[config.id.column] = id;
    }

    // Create row event
    const event: RowEvent = {
      op: 'insert',
      schema: config.source.schema,
      table: config.source.table,
      new: fakeRow,
      old: undefined,
      lsn: 0,
    };

    // Run transform if one exists
    let result: Action | null = null;

    if (config.transform?.path) {
      // Resolve transform path relative to puffgres directory (parent of migrations)
      const puffgresDir = dirname(migrationsDir);
      const transformPath = resolve(puffgresDir, config.transform.path);

      if (existsSync(transformPath)) {
        const module = await import(transformPath);
        const transform = module.default;

        if (typeof transform === 'function') {
          const migrationInfo = {
            name: config.mapping_name,
            namespace: config.namespace,
            table: `${config.source.schema}.${config.source.table}`,
          };
          const contextConfig: ContextConfig = {
            migration: migrationInfo,
            env: process.env as Record<string, string>,
          };
          const ctx = createTransformContext(contextConfig);
          result = await transform(event, id, ctx);
        }
      }
    }

    // If no transform, generate default upsert
    if (!result) {
      result = {
        type: 'upsert',
        id,
        doc: fakeRow,
      };
    }

    // Output everything
    console.log(
      JSON.stringify(
        {
          migration: {
            name: config.mapping_name,
            namespace: config.namespace,
            table: `${config.source.schema}.${config.source.table}`,
          },
          columns: relevantColumns,
          fakeRow,
          event,
          result,
        },
        null,
        2
      )
    );
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    console.error(
      JSON.stringify({
        error: message,
      })
    );
    process.exit(1);
  }
}

main();

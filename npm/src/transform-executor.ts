#!/usr/bin/env tsx
/**
 * Transform executor - runs a transform with provided event data.
 *
 * Called by the Rust JsTransformer to execute TypeScript/JavaScript transforms.
 *
 * Usage:
 *   npx tsx transform-executor.ts <transform-path> [migration-json]
 *
 * Rows JSON is read from stdin to avoid "Argument list too long" errors.
 *
 * Output:
 *   Writes an array of Action results to stdout as JSON.
 */

import { resolve } from 'path';
import type { RowEvent, Action, TransformContext, DocumentId, MigrationInfo, TransformInput } from '../types/index.js';
import { createTransformContext, type ContextConfig } from './context.js';

/**
 * Read all data from stdin.
 */
async function readStdin(): Promise<string> {
  const chunks: Buffer[] = [];
  for await (const chunk of process.stdin) {
    chunks.push(chunk);
  }
  return Buffer.concat(chunks).toString('utf8');
}

async function main(): Promise<void> {
  const args = process.argv.slice(2);

  if (args.length < 1) {
    console.error('Usage: transform-executor <transform-path> [migration-json]');
    console.error('Rows JSON is read from stdin.');
    process.exit(1);
  }

  const [transformPath, migrationJson] = args;

  // Read rows JSON from stdin
  const rowsJson = await readStdin();

  try {
    // Parse inputs - rows is an array of {event, id} objects
    const rows: TransformInput[] = JSON.parse(rowsJson);
    const migration: MigrationInfo = migrationJson
      ? JSON.parse(migrationJson)
      : { name: 'unknown', namespace: 'default', table: 'unknown' };

    // Load the transform
    const fullPath = resolve(process.cwd(), transformPath);
    const module = await import(fullPath);
    const transform = module.default;

    if (typeof transform !== 'function') {
      throw new Error(`Transform at ${transformPath} must export a default function`);
    }

    // Create context from environment
    const ctx = createTransformContext({
      env: process.env as Record<string, string>,
      migration,
    });

    // Execute the transform with the batch of rows
    const results: Action[] = await transform(rows, ctx);

    // Output the results
    console.log(JSON.stringify(results));
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    console.error(`Transform error: ${message}`);
    process.exit(1);
  }
}

main();

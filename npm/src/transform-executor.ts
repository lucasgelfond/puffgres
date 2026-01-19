#!/usr/bin/env tsx
/**
 * Transform executor - runs a transform with provided event data.
 *
 * Called by the Rust JsTransformer to execute TypeScript/JavaScript transforms.
 *
 * Usage:
 *   npx tsx transform-executor.ts <transform-path> <event-json> <id-json> [migration-json]
 *
 * Output:
 *   Writes the Action result to stdout as JSON.
 */

import { resolve } from 'path';
import type { RowEvent, Action, TransformContext, DocumentId, MigrationInfo } from '../types/index.js';
import { createTransformContext, type ContextConfig } from './context.js';

async function main(): Promise<void> {
  const args = process.argv.slice(2);

  if (args.length < 3) {
    console.error('Usage: transform-executor <transform-path> <event-json> <id-json> [migration-json]');
    process.exit(1);
  }

  const [transformPath, eventJson, idJson, migrationJson] = args;

  try {
    // Parse inputs
    const event: RowEvent = JSON.parse(eventJson);
    const id: DocumentId = JSON.parse(idJson);
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

    // Execute the transform
    const result: Action = await transform(event, id, ctx);

    // Output the result
    console.log(JSON.stringify(result));
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    console.error(`Transform error: ${message}`);
    process.exit(1);
  }
}

main();

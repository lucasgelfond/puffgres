/**
 * Transform runner - loads and executes TypeScript transforms.
 *
 * Supports both .ts and .js files using tsx for TypeScript compilation.
 */

import { resolve, extname } from 'path';
import type { TransformFn, RowEvent, Action, TransformContext, DocumentId, TransformInput } from '../types/index.js';

/**
 * Loaded transform module.
 */
export interface LoadedTransform {
  /** The transform function */
  transform: TransformFn;
  /** Path to the module */
  path: string;
}

/**
 * Load a transform from a file path.
 *
 * Supports:
 * - .ts files (via tsx)
 * - .js files (native import)
 * - .mjs files (native import)
 *
 * @param transformPath Path to the transform file (relative or absolute)
 * @param basePath Base path for resolving relative paths
 */
export async function loadTransform(
  transformPath: string,
  basePath: string = process.cwd()
): Promise<LoadedTransform> {
  // Resolve the path
  const fullPath = resolve(basePath, transformPath);
  const ext = extname(fullPath);

  // For TypeScript files, we need tsx
  if (ext === '.ts' || ext === '.tsx') {
    return loadTypeScriptTransform(fullPath);
  }

  // For JavaScript, use native import
  return loadJavaScriptTransform(fullPath);
}

/**
 * Load a TypeScript transform using tsx.
 */
async function loadTypeScriptTransform(path: string): Promise<LoadedTransform> {
  // Use tsx to load TypeScript
  // tsx registers itself and handles .ts imports
  try {
    // Dynamic import with tsx loader
    const module = await import(path);
    const transform = module.default as TransformFn;

    if (typeof transform !== 'function') {
      throw new Error(`Transform at ${path} must export a default function`);
    }

    return { transform, path };
  } catch (error) {
    throw new Error(`Failed to load TypeScript transform from ${path}: ${error}`);
  }
}

/**
 * Load a JavaScript transform using native import.
 */
async function loadJavaScriptTransform(path: string): Promise<LoadedTransform> {
  try {
    const module = await import(path);
    const transform = module.default as TransformFn;

    if (typeof transform !== 'function') {
      throw new Error(`Transform at ${path} must export a default function`);
    }

    return { transform, path };
  } catch (error) {
    throw new Error(`Failed to load JavaScript transform from ${path}: ${error}`);
  }
}

/**
 * Execute a transform function with error handling.
 * Takes an array of rows and returns an array of actions.
 */
export async function executeTransform(
  transform: TransformFn,
  rows: TransformInput[],
  ctx: TransformContext
): Promise<Action[]> {
  try {
    const results = await transform(rows, ctx);

    // Validate each result
    if (!Array.isArray(results)) {
      throw new Error('Transform must return an array of Action objects');
    }

    if (results.length !== rows.length) {
      throw new Error(`Transform must return exactly ${rows.length} actions, got ${results.length}`);
    }

    const validTypes = ['upsert', 'delete', 'skip'];

    for (let i = 0; i < results.length; i++) {
      const result = results[i];

      if (!result || typeof result !== 'object') {
        throw new Error(`Transform result at index ${i} must be an Action object`);
      }

      if (!('type' in result)) {
        throw new Error(`Transform result at index ${i} must have a "type" property`);
      }

      if (!validTypes.includes(result.type)) {
        throw new Error(`Invalid action type at index ${i}: ${result.type}`);
      }

      if (result.type === 'upsert' && !('id' in result && 'doc' in result)) {
        throw new Error(`Upsert action at index ${i} must have "id" and "doc" properties`);
      }

      if (result.type === 'delete' && !('id' in result)) {
        throw new Error(`Delete action at index ${i} must have "id" property`);
      }
    }

    return results;
  } catch (error) {
    // Return skip actions for all rows on error
    return rows.map(() => ({ type: 'skip' as const }));
  }
}

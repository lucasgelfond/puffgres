/**
 * Transform runner - loads and executes TypeScript transforms.
 *
 * Supports both .ts and .js files using tsx for TypeScript compilation.
 */

import { resolve, extname } from 'path';
import type { TransformFn, RowEvent, Action, TransformContext, DocumentId } from '../types/index.js';

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
 */
export async function executeTransform(
  transform: TransformFn,
  event: RowEvent,
  id: DocumentId,
  ctx: TransformContext
): Promise<Action> {
  try {
    const result = await transform(event, id, ctx);

    // Validate the result
    if (!result || typeof result !== 'object') {
      throw new Error('Transform must return an Action object');
    }

    if (!('type' in result)) {
      throw new Error('Transform result must have a "type" property');
    }

    const validTypes = ['upsert', 'delete', 'skip'];
    if (!validTypes.includes(result.type)) {
      throw new Error(`Invalid action type: ${result.type}`);
    }

    if (result.type === 'upsert' && !('id' in result && 'doc' in result)) {
      throw new Error('Upsert action must have "id" and "doc" properties');
    }

    if (result.type === 'delete' && !('id' in result)) {
      throw new Error('Delete action must have "id" property');
    }

    return result;
  } catch (error) {
    // Return an error action that can be sent to DLQ
    return {
      type: 'skip',
    };
  }
}

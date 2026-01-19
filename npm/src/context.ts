/**
 * Transform context implementation.
 *
 * Provides fetch() and lookup() capabilities to transforms.
 */

import type { TransformContext, DocumentId, MigrationInfo } from '../types/index.js';

export interface ContextConfig {
  /** Migration info */
  migration: MigrationInfo;
  /** Environment variables */
  env?: Record<string, string>;
  /** Postgres connection string for lookup */
  postgresConnectionString?: string;
}

/**
 * Create a transform context with the given configuration.
 */
export function createTransformContext(config: ContextConfig): TransformContext {
  return {
    migration: config.migration,
    env: config.env ?? { ...process.env } as Record<string, string>,

    async fetch(url: string, options?: RequestInit): Promise<Response> {
      return globalThis.fetch(url, options);
    },

    async lookup(table: string, id: DocumentId): Promise<Record<string, unknown> | null> {
      if (!config.postgresConnectionString) {
        throw new Error('Postgres connection not configured for lookup');
      }

      // This would use pg or a similar library
      // For now, throw an error indicating it's not implemented
      throw new Error('lookup() is not yet implemented');
    },
  };
}

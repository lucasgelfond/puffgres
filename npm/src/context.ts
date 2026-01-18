/**
 * Transform context implementation.
 *
 * Provides embed(), fetch(), and lookup() capabilities to transforms.
 */

import type { TransformContext, DocumentId } from '../types/index.js';

export interface ContextConfig {
  /** Environment variables */
  env?: Record<string, string>;
  /** Embedding provider config */
  embeddings?: {
    type: 'together' | 'openai';
    model: string;
    apiKey: string;
  };
  /** Postgres connection string for lookup */
  postgresConnectionString?: string;
}

/**
 * Create a transform context with the given configuration.
 */
export function createTransformContext(config: ContextConfig): TransformContext {
  return {
    env: config.env ?? { ...process.env } as Record<string, string>,

    async embed(text: string): Promise<number[]> {
      if (!config.embeddings) {
        throw new Error('Embedding provider not configured');
      }

      const { type, model, apiKey } = config.embeddings;

      if (type === 'together') {
        return embedWithTogether(text, model, apiKey);
      } else if (type === 'openai') {
        return embedWithOpenAI(text, model, apiKey);
      } else {
        throw new Error(`Unknown embedding provider: ${type}`);
      }
    },

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

/**
 * Generate embedding using Together AI.
 */
async function embedWithTogether(
  text: string,
  model: string,
  apiKey: string
): Promise<number[]> {
  const response = await fetch('https://api.together.xyz/v1/embeddings', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Authorization': `Bearer ${apiKey}`,
    },
    body: JSON.stringify({
      model,
      input: text,
    }),
  });

  if (!response.ok) {
    const error = await response.text();
    throw new Error(`Together AI error: ${error}`);
  }

  const data = await response.json() as {
    data: Array<{ embedding: number[] }>;
  };

  return data.data[0].embedding;
}

/**
 * Generate embedding using OpenAI.
 */
async function embedWithOpenAI(
  text: string,
  model: string,
  apiKey: string
): Promise<number[]> {
  const response = await fetch('https://api.openai.com/v1/embeddings', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Authorization': `Bearer ${apiKey}`,
    },
    body: JSON.stringify({
      model,
      input: text,
    }),
  });

  if (!response.ok) {
    const error = await response.text();
    throw new Error(`OpenAI error: ${error}`);
  }

  const data = await response.json() as {
    data: Array<{ embedding: number[] }>;
  };

  return data.data[0].embedding;
}

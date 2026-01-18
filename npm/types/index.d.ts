/**
 * Puffgres - CDC pipeline mirroring Postgres to turbopuffer
 */

/**
 * Row event from CDC stream.
 */
export interface RowEvent {
  /** Operation type */
  op: 'insert' | 'update' | 'delete';
  /** Schema name */
  schema: string;
  /** Table name */
  table: string;
  /** New row data (present for insert/update) */
  new?: Record<string, unknown>;
  /** Old row data (present for update/delete with replica identity) */
  old?: Record<string, unknown>;
  /** Log sequence number */
  lsn: number;
}

/**
 * Document ID - can be string, number, or UUID.
 */
export type DocumentId = string | number;

/**
 * Action returned from transform function.
 */
export type Action =
  | { type: 'upsert'; id: DocumentId; doc: Record<string, unknown> }
  | { type: 'delete'; id: DocumentId }
  | { type: 'skip' };

/**
 * Context provided to transform functions.
 */
export interface TransformContext {
  /**
   * Environment variables from the service.
   */
  env: Record<string, string>;

  /**
   * Generate embedding for text using configured provider.
   * @param text Text to embed
   * @returns Vector embedding as array of numbers
   */
  embed(text: string): Promise<number[]>;

  /**
   * HTTP client for custom API calls.
   * @param url URL to fetch
   * @param options Fetch options
   * @returns Response
   */
  fetch(url: string, options?: RequestInit): Promise<Response>;

  /**
   * Look up a row from Postgres by ID.
   * @param table Table name
   * @param id Row ID
   * @returns Row data or null if not found
   */
  lookup(table: string, id: DocumentId): Promise<Record<string, unknown> | null>;
}

/**
 * Transform function signature.
 */
export type TransformFn = (
  event: RowEvent,
  id: DocumentId,
  ctx: TransformContext
) => Promise<Action> | Action;

/**
 * Puffgres configuration from puffgres.toml
 */
export interface PuffgresConfig {
  postgres: {
    connection_string: string;
  };
  turbopuffer: {
    api_key: string;
  };
  providers?: {
    embeddings?: {
      type: 'together' | 'openai';
      model: string;
      api_key: string;
    };
  };
}

/**
 * Mapping configuration from migration TOML files.
 */
export interface MappingConfig {
  version: number;
  mapping_name: string;
  namespace: string;
  columns: string[];
  source: {
    schema: string;
    table: string;
  };
  id: {
    column: string;
    type: 'uint' | 'int' | 'uuid' | 'string';
  };
  transform?: {
    path: string;
  };
  membership?: {
    mode: 'all' | 'dsl' | 'view';
    predicate?: string;
  };
}

/**
 * Checkpoint state for a mapping.
 */
export interface Checkpoint {
  lsn: number;
  events_processed: number;
  updated_at?: string;
}

/**
 * DLQ entry.
 */
export interface DlqEntry {
  id: number;
  mapping_name: string;
  lsn: number;
  event_json: Record<string, unknown>;
  error_message: string;
  error_kind: string;
  retry_count: number;
  created_at: string;
}

/**
 * Backfill progress.
 */
export interface BackfillProgress {
  mapping_name: string;
  last_id?: string;
  total_rows?: number;
  processed_rows: number;
  status: 'pending' | 'in_progress' | 'completed';
  updated_at: string;
}

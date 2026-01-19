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
 * Distance metric for vector similarity search.
 */
export type DistanceMetric = 'cosine_distance' | 'euclidean_squared';

/**
 * Action returned from transform function.
 */
export type Action =
  | { type: 'upsert'; id: DocumentId; doc: Record<string, unknown>; distance_metric?: DistanceMetric }
  | { type: 'delete'; id: DocumentId }
  | { type: 'skip' };

/**
 * Migration info passed to transforms.
 */
export interface MigrationInfo {
  /** Migration name (mapping_name) */
  name: string;
  /** Namespace for turbopuffer */
  namespace: string;
  /** Source table in format schema.table */
  table: string;
}

/**
 * Context provided to transform functions.
 */
export interface TransformContext {
  /**
   * Migration info for the current transform.
   */
  migration: MigrationInfo;

  /**
   * Environment variables from the service.
   */
  env: Record<string, string>;

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
 * Input row for batch transforms.
 */
export interface TransformInput {
  /** Row event from CDC stream */
  event: RowEvent;
  /** Document ID extracted from the row */
  id: DocumentId;
}

/**
 * Transform function signature.
 * Receives an array of rows and returns an array of actions (one per row).
 */
export type TransformFn = (
  rows: TransformInput[],
  ctx: TransformContext
) => Promise<Action[]> | Action[];

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

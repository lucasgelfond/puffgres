/**
 * Puffgres - CDC pipeline mirroring Postgres to turbopuffer
 *
 * This package provides:
 * - TypeScript types for transforms
 * - Transform context with embed(), fetch(), lookup()
 * - Native bindings via Neon (optional)
 */

// Re-export types
export type {
  RowEvent,
  DocumentId,
  Action,
  TransformContext,
  TransformFn,
  PuffgresConfig,
  MappingConfig,
  Checkpoint,
  DlqEntry,
  BackfillProgress,
} from '../types/index.js';

// Transform utilities
export { createTransformContext } from './context.js';
export { loadTransform } from './transform-runner.js';

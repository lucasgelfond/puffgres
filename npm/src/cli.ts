#!/usr/bin/env node
/**
 * Puffgres CLI - CDC pipeline mirroring Postgres to turbopuffer
 *
 * This CLI wraps the native Rust implementation with Node.js for
 * TypeScript transform support.
 */

import { spawnSync } from 'child_process';
import { existsSync, readFileSync } from 'fs';
import { join, dirname } from 'path';
import { fileURLToPath } from 'url';

const __dirname = dirname(fileURLToPath(import.meta.url));

// Find the Rust CLI binary
function findRustBinary(): string | null {
  const candidates = [
    // Installed via cargo install
    join(process.env.HOME || '', '.cargo', 'bin', 'puffgres'),
    // Built in the project
    join(__dirname, '..', '..', 'target', 'release', 'puffgres'),
    join(__dirname, '..', '..', 'target', 'debug', 'puffgres'),
    // Built from parent directory
    join(__dirname, '..', '..', '..', 'target', 'release', 'puffgres'),
    join(__dirname, '..', '..', '..', 'target', 'debug', 'puffgres'),
  ];

  for (const candidate of candidates) {
    if (existsSync(candidate)) {
      return candidate;
    }
  }

  return null;
}

// Run a command using the Rust CLI
function runRustCommand(args: string[]): void {
  const binary = findRustBinary();

  if (!binary) {
    console.error('Error: puffgres Rust binary not found.');
    console.error('');
    console.error('To install:');
    console.error('  cargo install --path /path/to/puffgres');
    console.error('');
    console.error('Or build locally:');
    console.error('  cd /path/to/puffgres && cargo build --release');
    process.exit(1);
  }

  const result = spawnSync(binary, args, {
    stdio: 'inherit',
    env: process.env,
  });

  if (result.error) {
    console.error('Error running puffgres:', result.error.message);
    process.exit(1);
  }

  process.exit(result.status ?? 0);
}

// Show help
function showHelp(): void {
  console.log(`puffgres - CDC pipeline mirroring Postgres to turbopuffer

USAGE:
  puffgres <command> [options]

COMMANDS:
  init <dir>       Initialize a new puffgres project
  run              Start the CDC pipeline
  migrate          Apply pending migrations
  backfill <name>  Backfill existing data for a mapping
  status           Show current checkpoint status
  dlq              Manage dead letter queue

OPTIONS:
  --help           Show this help message
  --version        Show version

EXAMPLES:
  pnpm puffgres init ./my-puffgres
  pnpm puffgres migrate --dry-run
  pnpm puffgres run
  pnpm puffgres backfill users --batch-size 1000
  pnpm puffgres dlq list
`);
}

// Show version
function showVersion(): void {
  try {
    const packageJson = JSON.parse(
      readFileSync(join(__dirname, '..', 'package.json'), 'utf-8')
    );
    console.log(`puffgres ${packageJson.version}`);
  } catch {
    console.log('puffgres 0.1.0');
  }
}

// Main entry point
async function main(): Promise<void> {
  const args = process.argv.slice(2);

  if (args.length === 0 || args.includes('--help') || args.includes('-h')) {
    showHelp();
    return;
  }

  if (args.includes('--version') || args.includes('-v')) {
    showVersion();
    return;
  }

  const command = args[0];

  switch (command) {
    case 'init':
    case 'run':
    case 'migrate':
    case 'backfill':
    case 'status':
    case 'dlq':
      // Delegate to Rust CLI
      runRustCommand(args);
      break;

    default:
      console.error(`Unknown command: ${command}`);
      console.error('Run "puffgres --help" for usage.');
      process.exit(1);
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});

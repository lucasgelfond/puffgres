#!/usr/bin/env node
/**
 * Puffgres CLI - CDC pipeline mirroring Postgres to turbopuffer
 *
 * This CLI wraps the native Rust implementation with Node.js for
 * TypeScript transform support.
 */

import { spawn, spawnSync } from 'child_process';
import { existsSync, mkdirSync, writeFileSync, readFileSync } from 'fs';
import { join, dirname, resolve } from 'path';
import { fileURLToPath } from 'url';

const __dirname = dirname(fileURLToPath(import.meta.url));

// Template files for `puffgres init`
const TEMPLATES = {
  'package.json': `{
  "name": "@myapp/puffgres-service",
  "private": true,
  "type": "module",
  "scripts": {
    "start": "puffgres run",
    "migrate": "puffgres migrate",
    "status": "puffgres status",
    "backfill": "puffgres backfill",
    "test-transform-runner": "puffgres-test-transform"
  },
  "dependencies": {
    "dotenv": "^16.0.0",
    "puffgres": "^0.1.0"
  }
}
`,
  'puffgres.toml': `# Puffgres configuration
# CDC pipeline mirroring Postgres to turbopuffer

[postgres]
connection_string = "\${DATABASE_URL}"

[turbopuffer]
api_key = "\${TURBOPUFFER_API_KEY}"
`,
  '.env.example': `# Copy this to .env and fill in your values
DATABASE_URL=postgres://user:password@localhost:5432/mydb
TURBOPUFFER_API_KEY=your-turbopuffer-api-key
`,
  '.gitignore': `.env
node_modules/
`,
  'migrations/0001_example.toml': `# Example migration - rename or replace with your actual migration
version = 1
mapping_name = "users"
namespace = "users"
columns = ["id", "name", "email", "bio"]

[source]
schema = "public"
table = "users"

[id]
column = "id"
type = "uuid"

# Optional: Filter which rows to sync
# [membership]
# mode = "dsl"
# predicate = "status = 'active'"

# Optional: Custom transform
# [transform]
# path = "./transforms/users.ts"
`,
  'transforms/example.ts': `/**
 * Example transform.
 *
 * Rename this file and update migrations/0001_example.toml to use it.
 * You can import any npm packages here - just add them to package.json.
 */
import type { RowEvent, Action, TransformContext } from 'puffgres';

export default async function transform(
  event: RowEvent,
  id: string,
  ctx: TransformContext
): Promise<Action> {
  if (event.op === 'delete') {
    return { type: 'delete', id };
  }

  const row = event.new!;

  return {
    type: 'upsert',
    id,
    doc: {
      name: row.name,
      email: row.email,
      bio: row.bio,
    },
  };
}
`,
  'railway.json': `{
  "build": {
    "builder": "NIXPACKS"
  },
  "deploy": {
    "startCommand": "pnpm puffgres run",
    "restartPolicyType": "ON_FAILURE",
    "restartPolicyMaxRetries": 10
  }
}
`,
};

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

// Initialize a new puffgres project
function initProject(targetDir: string): void {
  const absPath = resolve(targetDir);

  console.log(`Initializing puffgres project in ${absPath}`);
  console.log('');

  // Create directories
  const dirs = ['migrations', 'transforms'];
  for (const dir of dirs) {
    const fullPath = join(absPath, dir);
    if (!existsSync(fullPath)) {
      mkdirSync(fullPath, { recursive: true });
      console.log(`  Created ${dir}/`);
    }
  }

  // Write template files
  for (const [filename, content] of Object.entries(TEMPLATES)) {
    const fullPath = join(absPath, filename);
    const dir = dirname(fullPath);

    if (!existsSync(dir)) {
      mkdirSync(dir, { recursive: true });
    }

    if (existsSync(fullPath)) {
      console.log(`  Skipped ${filename} (already exists)`);
    } else {
      writeFileSync(fullPath, content);
      console.log(`  Created ${filename}`);
    }
  }

  console.log('');
  console.log('Project initialized successfully!');
  console.log('');
  console.log('Next steps:');
  console.log(`  1. cd ${targetDir}`);
  console.log('  2. Copy .env.example to .env and fill in your values');
  console.log('  3. Edit migrations/0001_example.toml for your schema');
  console.log('  4. pnpm install');
  console.log('  5. pnpm puffgres migrate');
  console.log('  6. pnpm puffgres run');
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
      const targetDir = args[1] || '.';
      initProject(targetDir);
      break;

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

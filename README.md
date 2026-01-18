# puffgres

A Change Data Capture (CDC) pipeline that continuously syncs PostgreSQL tables to [turbopuffer](https://turbopuffer.com/) vector database in real-time.

## Features

- **Real-time sync** via PostgreSQL logical replication (WAL)
- **Selective sync** with membership predicates to filter rows
- **Custom transforms** in TypeScript or Rust
- **Backfill support** for initial data loading
- **Dead-letter queue** for failure handling
- **Anti-regression safety** using LSN-based versioning

## Installation

### From source

```bash
# Clone and build
git clone https://github.com/lucasgelfond/puffgres.git
cd puffgres
cargo build --release

# Install to ~/.cargo/bin (recommended)
cargo install --path crates/puffgres-cli

# Now you can run `puffgres` from anywhere
puffgres --help
```

### Alternative: Add to PATH

If you don't want to install globally, add the binary to your PATH:

```bash
# Add to your shell profile (~/.bashrc, ~/.zshrc, etc.)
export PATH="$PATH:/path/to/puffgres/target/release"

# Or create a symlink
ln -s /path/to/puffgres/target/release/puffgres-cli /usr/local/bin/puffgres
```

### Alternative: Shell alias

```bash
# Add to your shell profile
alias puffgres='/path/to/puffgres/target/release/puffgres-cli'
```

### Via npm

```bash
npm install -g puffgres
# or
pnpm add -g puffgres
```

## Quick Start

### 1. Set up your environment

```bash
export DATABASE_URL="postgres://user:password@host:5432/dbname"
export TURBOPUFFER_API_KEY="your-turbopuffer-api-key"
```

### 2. Ensure logical replication is enabled

Your PostgreSQL database must have `wal_level = logical`. Check with:

```sql
SHOW wal_level;
```

If it's not `logical`, you'll need to update your PostgreSQL configuration:

```sql
ALTER SYSTEM SET wal_level = logical;
-- Restart PostgreSQL after this change
```

**Note:** PlanetScale Postgres, Neon, Supabase, and most managed Postgres providers support logical replication out of the box.

### 3. Initialize a project

```bash
puffgres init my-project
cd my-project
```

This creates:
```
my-project/
  puffgres.toml           # Main configuration
  puffgres/
    migrations/           # Table sync definitions
    transforms/           # Custom transform functions
```

### 4. Configure puffgres.toml

```toml
[postgres]
connection_string = "${DATABASE_URL}"

[turbopuffer]
api_key = "${TURBOPUFFER_API_KEY}"
```

### 5. Define a migration

Create `puffgres/migrations/001_users.toml`:

```toml
[migration]
name = "users"
version = 1

[source]
table = "users"
schema = "public"

[mapping]
id = "id"                    # Column to use as document ID
columns = ["id", "name", "email", "bio", "created_at"]

# Optional: Only sync active users
# membership = "status = 'active'"

[target]
namespace = "users"          # turbopuffer namespace
```

### 6. Apply migrations

```bash
puffgres migrate
```

### 7. Backfill existing data

```bash
puffgres backfill users
```

### 8. Start real-time sync

```bash
puffgres run
```

This starts the CDC loop, continuously syncing changes from PostgreSQL to turbopuffer.

## CLI Reference

### Global Options

```bash
puffgres [OPTIONS] <COMMAND>

Options:
  -c, --config <PATH>    Path to puffgres.toml (default: puffgres.toml)
  -h, --help             Print help
  -V, --version          Print version
```

### Commands

#### `puffgres init [PATH]`

Initialize a new puffgres project.

```bash
puffgres init ./my-project
```

#### `puffgres migrate`

Apply pending migrations.

```bash
puffgres migrate              # Apply migrations
puffgres migrate --dry-run    # Preview without applying
```

#### `puffgres run`

Start the CDC replication loop.

```bash
puffgres run                           # Start with defaults
puffgres run --slot my-slot            # Custom replication slot name
puffgres run --create-slot             # Auto-create slot if missing
puffgres run --poll-interval-ms 500    # Poll every 500ms
```

Options:
- `--slot <NAME>` - Replication slot name (default: `puffgres`)
- `--create-slot` - Create the slot if it doesn't exist (default: true)
- `--poll-interval-ms <MS>` - Polling interval in milliseconds (default: 1000)

#### `puffgres backfill <MAPPING>`

Backfill existing table data to turbopuffer.

```bash
puffgres backfill users                    # Backfill the 'users' mapping
puffgres backfill users --batch-size 500   # Custom batch size
puffgres backfill users --resume           # Resume from checkpoint
```

Options:
- `--batch-size <SIZE>` - Rows per batch (default: 1000)
- `--resume` - Resume from previous checkpoint

#### `puffgres status`

Show current sync status.

```bash
puffgres status
```

#### `puffgres dlq`

Manage the dead-letter queue for failed events.

```bash
puffgres dlq list                      # List failed events
puffgres dlq list --mapping users      # Filter by mapping
puffgres dlq list --limit 10           # Limit results

puffgres dlq show <ID>                 # Show details of a failed event

puffgres dlq retry --id <ID>           # Retry specific event
puffgres dlq retry --mapping users     # Retry all for a mapping

puffgres dlq clear --id <ID>           # Clear specific event
puffgres dlq clear --mapping users     # Clear all for a mapping
puffgres dlq clear --all               # Clear entire DLQ
```

## Configuration

### puffgres.toml

```toml
[postgres]
connection_string = "${DATABASE_URL}"

[turbopuffer]
api_key = "${TURBOPUFFER_API_KEY}"

# Optional: Embedding provider for vector transforms
[providers.embeddings]
type = "together"
model = "BAAI/bge-base-en-v1.5"
api_key = "${TOGETHER_API_KEY}"
```

### Migration Files

Migration files define how tables map to turbopuffer namespaces.

```toml
[migration]
name = "products"
version = 1

[source]
table = "products"
schema = "public"

[mapping]
id = "id"
columns = ["id", "name", "description", "price", "category"]

# Filter rows to sync
membership = "active = true AND deleted_at IS NULL"

# Custom transform (optional)
transform = "transforms/product_transform.ts"

[target]
namespace = "products"

# Schema hints for turbopuffer
[target.schema]
price = "float"
category = "string"
```

### Membership Predicates

Filter which rows to sync using SQL-like predicates:

```toml
# Simple equality
membership = "status = 'active'"

# NULL checks
membership = "deleted_at IS NULL"

# Compound conditions
membership = "status = 'published' AND visibility = 'public'"

# Negation
membership = "NOT archived"
```

## Custom Transforms

Create transforms to modify data before syncing to turbopuffer.

### TypeScript Transform

Create `puffgres/transforms/my_transform.ts`:

```typescript
import { RowEvent, Action } from 'puffgres';

export default function transform(event: RowEvent): Action {
  if (event.type === 'delete') {
    return { type: 'delete', id: event.id };
  }

  const { id, name, description } = event.new;

  return {
    type: 'upsert',
    id,
    attributes: {
      name,
      description,
      // Combine fields for better search
      searchText: `${name} ${description}`,
    },
  };
}
```

Reference it in your migration:

```toml
[mapping]
transform = "transforms/my_transform.ts"
```

## Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `DATABASE_URL` | Yes | PostgreSQL connection string |
| `TURBOPUFFER_API_KEY` | Yes | turbopuffer API key |
| `TOGETHER_API_KEY` | No | Together AI API key (for embeddings) |
| `RUST_LOG` | No | Log level (default: `puffgres=info`) |

## Using with PlanetScale Postgres

PlanetScale Postgres supports logical replication. To use puffgres:

1. Create a PlanetScale Postgres database
2. Get your connection credentials from the dashboard
3. Set the `DATABASE_URL`:

```bash
export DATABASE_URL="postgres://user:password@host.psdb.cloud:5432/postgres?sslmode=require"
```

4. Run puffgres as normal:

```bash
puffgres run
```

## Deployment

### Docker

```dockerfile
FROM rust:1.75 as builder
WORKDIR /app
COPY . .
RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/puffgres-cli /usr/local/bin/puffgres
CMD ["puffgres", "run"]
```

### As a systemd service

```ini
[Unit]
Description=puffgres CDC pipeline
After=network.target

[Service]
Type=simple
User=puffgres
WorkingDirectory=/opt/puffgres
ExecStart=/usr/local/bin/puffgres run
Restart=always
RestartSec=10
Environment=DATABASE_URL=postgres://...
Environment=TURBOPUFFER_API_KEY=...

[Install]
WantedBy=multi-user.target
```

## Architecture

puffgres uses PostgreSQL's logical decoding to capture changes:

1. **Replication Slot** - Maintains a position in the WAL
2. **wal2json** - Decodes WAL entries to JSON
3. **Router** - Matches changes to configured mappings
4. **Transform** - Applies custom transformations
5. **Batcher** - Groups changes for efficient writes
6. **turbopuffer Client** - Writes to turbopuffer namespaces

State is stored in PostgreSQL itself (`__puffgres_*` tables), ensuring consistency and easy recovery.

## License

MIT

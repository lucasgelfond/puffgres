# Deployment Guide

This guide covers deploying puffgres as a separate service in your monorepo.

## Monorepo Structure

Puffgres is designed to run as a standalone service within your monorepo:

```
my-monorepo/
  packages/
    frontend/                   # Vercel/Netlify
    backend/                    # Railway/Fly
  puffgres/                     # Railway (separate service)
    package.json
    puffgres.toml
    migrations/
      0001_users.toml
      0002_documents.toml
    transforms/
      users.ts
      documents.ts
```

## Railway Deployment

Railway is the recommended deployment platform for puffgres.

### Setup

1. **Initialize your puffgres folder:**
   ```bash
   npx puffgres init ./puffgres
   ```

2. **Configure environment variables in Railway:**
   - `DATABASE_URL` - Your Postgres connection string (must support logical replication)
   - `TURBOPUFFER_API_KEY` - Your turbopuffer API key
   - `TOGETHER_API_KEY` - (Optional) For embedding transforms

3. **Deploy to Railway:**
   - In your Railway project, add a new service
   - Set the root directory to `puffgres`
   - Railway will auto-detect the Node.js project

### railway.json

The `puffgres init` command generates a `railway.json` file:

```json
{
  "build": {
    "builder": "NIXPACKS"
  },
  "deploy": {
    "startCommand": "pnpm puffgres run",
    "restartPolicyType": "ON_FAILURE",
    "restartPolicyMaxRetries": 10
  }
}
```

### CI/CD Flow

1. Push changes to your monorepo
2. Railway detects changes in the `puffgres/` folder
3. Rebuilds and restarts the puffgres service
4. The service validates migrations on startup

## Postgres Requirements

Puffgres requires a Postgres database with logical replication enabled.

### PlanetScale Postgres

PlanetScale Postgres supports logical replication. Enable it in your database settings.

### Neon

Neon supports logical replication on paid plans. Enable it in your project settings.

### Supabase

Supabase supports logical replication. Run these commands in the SQL editor:

```sql
-- Enable logical replication
ALTER SYSTEM SET wal_level = logical;

-- Create a replication slot
SELECT * FROM pg_create_logical_replication_slot('puffgres_slot', 'wal2json');
```

### Self-hosted Postgres

Ensure your `postgresql.conf` has:

```
wal_level = logical
max_replication_slots = 4
max_wal_senders = 4
```

## Fly.io Deployment

Puffgres also works on Fly.io.

### fly.toml

```toml
app = "my-puffgres"
primary_region = "sjc"

[build]
  builder = "heroku/buildpacks:20"

[env]
  NODE_ENV = "production"

[[services]]
  internal_port = 8080
  protocol = "tcp"

  [services.concurrency]
    hard_limit = 1
    soft_limit = 1
```

### Deployment

```bash
cd puffgres
fly launch --no-deploy
fly secrets set DATABASE_URL="postgres://..."
fly secrets set TURBOPUFFER_API_KEY="..."
fly deploy
```

## Docker Deployment

You can also deploy puffgres using Docker.

### Dockerfile

```dockerfile
FROM node:20-slim
WORKDIR /app
COPY package*.json ./
RUN npm ci --production
COPY . .
CMD ["npx", "puffgres", "run"]
```

### Build and Run

```bash
docker build -t puffgres .
docker run -e DATABASE_URL="..." -e TURBOPUFFER_API_KEY="..." puffgres
```

## Health Checks

Puffgres doesn't expose an HTTP endpoint by default. For health checks:

1. **Railway:** Use the default process-based health check
2. **Fly.io:** Use `fly checks` with the TCP internal port
3. **Docker:** Use `docker inspect` to check container status

## Scaling Considerations

Puffgres is designed to run as a single instance per replication slot. Running multiple instances will cause conflicts.

For high availability:
- Use a process supervisor (Railway/Fly handle this automatically)
- Configure restart policies for crash recovery
- Monitor the DLQ for failed events

## Troubleshooting

### "Replication slot does not exist"

Create the replication slot manually:
```sql
SELECT * FROM pg_create_logical_replication_slot('puffgres_slot', 'wal2json');
```

### "Permission denied for replication"

Ensure your database user has replication privileges:
```sql
ALTER USER myuser WITH REPLICATION;
```

### Events not appearing in turbopuffer

Check the checkpoint status:
```bash
pnpm puffgres status
```

Check the DLQ for failed events:
```bash
pnpm puffgres dlq list
```

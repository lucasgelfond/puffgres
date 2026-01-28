use std::fs;
use std::io::Write;
use std::path::Path;

use anyhow::Result;
use colored::Colorize;
use tracing::info;

pub async fn cmd_init() -> Result<()> {
    println!("Initializing puffgres project...\n");

    // Create puffgres/ directory with migrations and transforms subdirectories
    fs::create_dir_all("puffgres/migrations")?;
    fs::create_dir_all("puffgres/transforms")?;
    info!("Created puffgres/migrations/ and puffgres/transforms/");

    // Create or update package.json with required dependencies
    ensure_package_json()?;

    // Create .env.example with puffgres-specific variables
    let env_example_content = r#"# Puffgres environment variables
# Copy this file to .env and fill in your values

# Postgres connection string (Supabase, Neon, etc.)
DATABASE_URL=

# Turbopuffer API key
TURBOPUFFER_API_KEY=

# Optional: Namespace prefix for environment separation (e.g., production, development)
# PUFFGRES_BASE_NAMESPACE=

# Optional: Batch sizes for transform and upload operations
# PUFFGRES_TRANSFORM_BATCH_SIZE=100
# PUFFGRES_UPLOAD_BATCH_SIZE=500

# Optional: Maximum retries for failed turbopuffer uploads (default: 5)
# Uses exponential backoff: 100ms, 200ms, 400ms, 800ms, 1600ms
# PUFFGRES_MAX_RETRIES=5
"#;

    let env_example_path = Path::new("puffgres/.env.example");
    if !env_example_path.exists() {
        fs::write(env_example_path, env_example_content)?;
        println!("Created puffgres/.env.example");
    } else {
        println!("puffgres/.env.example already exists, skipping");
    }

    // Create .gitignore in puffgres/ directory
    let gitignore_path = Path::new("puffgres/.gitignore");
    if !gitignore_path.exists() {
        fs::write(gitignore_path, "# Puffgres\n.env\nnode_modules/\n")?;
        println!("Created puffgres/.gitignore");
    }

    // Also add puffgres/.env to root .gitignore if it exists
    let root_gitignore = Path::new(".gitignore");
    if root_gitignore.exists() {
        let content = fs::read_to_string(root_gitignore)?;
        if !content.contains("puffgres/.env") {
            let mut file = fs::OpenOptions::new().append(true).open(root_gitignore)?;
            writeln!(file, "\n# Puffgres secrets\npuffgres/.env")?;
            println!("Added puffgres/.env to .gitignore");
        }
    }

    // Create Dockerfile for containerized deployments
    let dockerfile_content = r#"# Dockerfile for puffgres
# Builds puffgres from source

# Builder stage - compile Rust binary
FROM rust:latest AS builder

RUN apt-get update && apt-get install -y git pkg-config libssl-dev nodejs npm && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Clone and build puffgres from source
RUN git clone https://github.com/lucasgelfond/puffgres.git . && \
    cargo build --release --package puffgres-cli && \
    cp target/release/puffgres /usr/local/bin/puffgres

# Build the npm package for transform executor
RUN npm --prefix npm install && npm --prefix npm run build

# Runtime stage - use Debian Trixie to match GLIBC version from rust:latest builder
FROM debian:trixie-slim

# Install Node.js 22 and OpenSSL runtime library
RUN apt-get update && apt-get install -y ca-certificates libssl3 curl && \
    curl -fsSL https://deb.nodesource.com/setup_22.x | bash - && \
    apt-get install -y nodejs && \
    rm -rf /var/lib/apt/lists/* && \
    corepack enable

# Copy the built binary from builder
COPY --from=builder /usr/local/bin/puffgres /usr/local/bin/puffgres

# Copy the npm package from builder
COPY --from=builder /build/npm /opt/puffgres-npm

WORKDIR /app

# Copy puffgres project files (migrations, transforms, package.json)
COPY migrations ./migrations
COPY transforms ./transforms
COPY package.json ./package.json

# Update package.json to use local npm package instead of registry
RUN sed -i 's|"puffgres": "link:[^"]*"|"puffgres": "file:/opt/puffgres-npm"|g' package.json && \
    sed -i 's|"puffgres": "\^[0-9.]*"|"puffgres": "file:/opt/puffgres-npm"|g' package.json

# Install dependencies (includes puffgres npm package for transform executor)
RUN pnpm install --frozen-lockfile || pnpm install

# Create .env file from environment variables at runtime, then run puffgres
CMD ["sh", "-c", "printf 'DATABASE_URL=%s\\nTURBOPUFFER_API_KEY=%s\\nPUFFGRES_BASE_NAMESPACE=%s\\nPUFFGRES_TRANSFORM_BATCH_SIZE=%s\\nPUFFGRES_UPLOAD_BATCH_SIZE=%s\\nPUFFGRES_MAX_RETRIES=%s\\n' \"$DATABASE_URL\" \"$TURBOPUFFER_API_KEY\" \"$PUFFGRES_BASE_NAMESPACE\" \"$PUFFGRES_TRANSFORM_BATCH_SIZE\" \"$PUFFGRES_UPLOAD_BATCH_SIZE\" \"$PUFFGRES_MAX_RETRIES\" > .env && puffgres run"]
"#;

    let dockerfile_path = Path::new("puffgres/Dockerfile");
    if !dockerfile_path.exists() {
        fs::write(dockerfile_path, dockerfile_content)?;
        println!("Created puffgres/Dockerfile");
    } else {
        println!("puffgres/Dockerfile already exists, skipping");
    }

    println!("\n{}", "Puffgres initialized!".green().bold());
    println!("\nNext steps:");
    println!("  1. cd puffgres");
    println!("  2. Copy .env.example to .env and fill in your credentials");
    println!("  3. Run: pnpm install");
    println!("  4. Run: puffgres setup");
    println!("  5. Run: puffgres new <table_name>");
    println!("  6. Run: puffgres migrate");
    println!("  7. Run: puffgres backfill <mapping_name>");
    println!("  8. Run: puffgres run");
    println!("\nNote: All puffgres commands should be run from inside the puffgres/ directory.\n");

    Ok(())
}

/// Ensure package.json exists with required dependencies for transforms.
fn ensure_package_json() -> Result<()> {
    let package_json_path = Path::new("puffgres/package.json");

    if package_json_path.exists() {
        // Read existing package.json and check for tsx
        let content = fs::read_to_string(package_json_path)?;
        let mut pkg: serde_json::Value = serde_json::from_str(&content)?;

        let mut modified = false;

        // Ensure devDependencies exists
        if pkg.get("devDependencies").is_none() {
            pkg["devDependencies"] = serde_json::json!({});
        }

        // Add tsx if not present
        if pkg["devDependencies"].get("tsx").is_none() {
            pkg["devDependencies"]["tsx"] = serde_json::json!("^4.7.0");
            modified = true;
        }

        // Ensure dependencies exists
        if pkg.get("dependencies").is_none() {
            pkg["dependencies"] = serde_json::json!({});
        }

        // Add puffgres if not present
        if pkg["dependencies"].get("puffgres").is_none() {
            pkg["dependencies"]["puffgres"] = serde_json::json!("^0.1.0");
            modified = true;
        }

        if modified {
            let formatted = serde_json::to_string_pretty(&pkg)?;
            fs::write(package_json_path, formatted)?;
            println!("Updated package.json with puffgres dependencies");
        } else {
            println!("package.json already has required dependencies");
        }
    } else {
        // Create new package.json
        let pkg = serde_json::json!({
            "name": "puffgres-transforms",
            "private": true,
            "type": "module",
            "scripts": {
                "start": "puffgres run",
                "migrate": "puffgres migrate",
                "status": "puffgres status",
                "backfill": "puffgres backfill"
            },
            "dependencies": {
                "puffgres": "^0.1.0"
            },
            "devDependencies": {
                "tsx": "^4.7.0"
            }
        });

        let formatted = serde_json::to_string_pretty(&pkg)?;
        fs::write(package_json_path, formatted)?;
        println!("Created package.json with puffgres dependencies");
    }

    Ok(())
}

use std::fs;
use std::io::Write;
use std::path::Path;

use anyhow::Result;
use colored::Colorize;
use tracing::info;

pub async fn cmd_init() -> Result<()> {
    println!("Initializing puffgres in current directory...\n");

    // Create directories
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
"#;

    let env_example_path = Path::new(".env.example");
    if !env_example_path.exists() {
        fs::write(env_example_path, env_example_content)?;
        println!("Created .env.example");
    } else {
        println!(".env.example already exists, skipping");
    }

    // Create puffgres.toml config
    let config = r#"# Puffgres configuration
# Secrets are loaded from .env file
# State is stored in __puffgres_* tables in your Postgres database

[postgres]
connection_string = "${DATABASE_URL}"

[turbopuffer]
api_key = "${TURBOPUFFER_API_KEY}"

# Optional: Namespace prefix for environment separation
# base_namespace = "${PUFFGRES_BASE_NAMESPACE}"

# Optional: Configure embedding providers for transforms
# [providers.embeddings]
# type = "together"
# model = "BAAI/bge-base-en-v1.5"
# api_key = "${TOGETHER_API_KEY}"
"#;

    let config_path = Path::new("puffgres.toml");
    if !config_path.exists() {
        fs::write(config_path, config)?;
        println!("Created puffgres.toml");
    } else {
        println!("puffgres.toml already exists, skipping");
    }

    // Create .gitignore in puffgres directory
    let gitignore = "# Local transform builds\nnode_modules/\n";
    let gitignore_path = Path::new("puffgres/.gitignore");
    if !gitignore_path.exists() {
        fs::write(gitignore_path, gitignore)?;
    }

    // Also add .env to root .gitignore if it exists
    let root_gitignore = Path::new(".gitignore");
    if root_gitignore.exists() {
        let content = fs::read_to_string(root_gitignore)?;
        if !content.contains(".env") {
            let mut file = fs::OpenOptions::new().append(true).open(root_gitignore)?;
            writeln!(file, "\n# Puffgres secrets\n.env")?;
            println!("Added .env to .gitignore");
        }
    } else {
        fs::write(root_gitignore, "# Puffgres secrets\n.env\n")?;
        println!("Created .gitignore with .env");
    }

    // Create Dockerfile for containerized deployments
    let dockerfile_content = r#"# Dockerfile for puffgres
# Downloads pre-built binary from GitHub releases

FROM node:22-slim

# Install curl for downloading binary
RUN apt-get update && apt-get install -y curl ca-certificates && rm -rf /var/lib/apt/lists/*

# Set puffgres version (update this or pass as build arg)
ARG PUFFGRES_VERSION=latest

# Detect architecture and download appropriate binary
RUN ARCH=$(uname -m) && \
    if [ "$ARCH" = "x86_64" ]; then \
        RUST_TARGET="x86_64-unknown-linux-gnu"; \
    elif [ "$ARCH" = "aarch64" ]; then \
        RUST_TARGET="aarch64-unknown-linux-gnu"; \
    else \
        echo "Unsupported architecture: $ARCH" && exit 1; \
    fi && \
    if [ "$PUFFGRES_VERSION" = "latest" ]; then \
        DOWNLOAD_URL="https://github.com/lucasgelfond/puffgres/releases/latest/download/puffgres-${RUST_TARGET}.tar.gz"; \
    else \
        DOWNLOAD_URL="https://github.com/lucasgelfond/puffgres/releases/download/v${PUFFGRES_VERSION}/puffgres-${RUST_TARGET}.tar.gz"; \
    fi && \
    echo "Downloading puffgres from: $DOWNLOAD_URL" && \
    curl -fsSL "$DOWNLOAD_URL" | tar xz -C /usr/local/bin && \
    chmod +x /usr/local/bin/puffgres && \
    puffgres --version

# Enable corepack for pnpm
RUN corepack enable

WORKDIR /app

# Copy package files first for better caching
COPY package.json pnpm-lock.yaml* ./

# Install dependencies
RUN pnpm install --frozen-lockfile || pnpm install

# Copy the rest of the application
COPY . .

# Default command
CMD ["pnpm", "run", "start"]
"#;

    let dockerfile_path = Path::new("Dockerfile");
    if !dockerfile_path.exists() {
        fs::write(dockerfile_path, dockerfile_content)?;
        println!("Created Dockerfile");
    } else {
        println!("Dockerfile already exists, skipping");
    }

    println!("\n{}", "Puffgres initialized!".green().bold());
    println!("\nNext steps:");
    println!("  1. Copy .env.example to .env and fill in your credentials");
    println!("  2. Run: pnpm install");
    println!("  3. Run: puffgres setup");
    println!("  4. Run: puffgres new <table_name>");
    println!("  5. Run: puffgres migrate");
    println!("  6. Run: puffgres backfill <mapping_name>");
    println!("  7. Run: puffgres run\n");

    Ok(())
}

/// Ensure package.json exists with required dependencies for transforms.
fn ensure_package_json() -> Result<()> {
    let package_json_path = Path::new("package.json");

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
            "name": "my-puffgres-project",
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

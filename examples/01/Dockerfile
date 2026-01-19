# Dockerfile for puffgres
# Builds puffgres from source

# Builder stage - compile Rust binary
FROM rust:latest AS builder

RUN apt-get update && apt-get install -y git pkg-config libssl-dev && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Clone and build puffgres from source
RUN git clone https://github.com/lucasgelfond/puffgres.git . && \
    cargo build --release --package puffgres-cli && \
    cp target/release/puffgres /usr/local/bin/puffgres

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

WORKDIR /app

# Copy package files first for better caching
COPY package.json pnpm-lock.yaml* ./

# Install dependencies (includes puffgres npm package for transform executor)
RUN pnpm install --frozen-lockfile || pnpm install

# Copy the rest of the application
COPY . .

# Create .env file from environment variables at runtime, then run puffgres
CMD sh -c 'cat > .env << EOF\n\
DATABASE_URL=${DATABASE_URL}\n\
TURBOPUFFER_API_KEY=${TURBOPUFFER_API_KEY}\n\
PUFFGRES_BASE_NAMESPACE=${PUFFGRES_BASE_NAMESPACE}\n\
PUFFGRES_TRANSFORM_BATCH_SIZE=${PUFFGRES_TRANSFORM_BATCH_SIZE}\n\
PUFFGRES_UPLOAD_BATCH_SIZE=${PUFFGRES_UPLOAD_BATCH_SIZE}\n\
PUFFGRES_MAX_RETRIES=${PUFFGRES_MAX_RETRIES}\n\
EOF\n\
puffgres run'

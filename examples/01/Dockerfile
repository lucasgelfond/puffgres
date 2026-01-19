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

# Run the Rust binary directly
CMD ["puffgres", "run"]

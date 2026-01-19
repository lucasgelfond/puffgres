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

# Runtime stage
FROM node:22-slim

# Install OpenSSL runtime library
RUN apt-get update && apt-get install -y ca-certificates libssl3 && rm -rf /var/lib/apt/lists/*

# Copy the built binary from builder
COPY --from=builder /usr/local/bin/puffgres /usr/local/bin/puffgres

# Enable corepack for pnpm
RUN corepack enable

WORKDIR /app

# Copy package files first for better caching
COPY package.json pnpm-lock.yaml* ./

# Install dependencies (includes puffgres npm package for transform executor)
RUN pnpm install --frozen-lockfile || pnpm install

# Copy the rest of the application
COPY . .

# Run the Rust binary directly
CMD ["puffgres", "run"]

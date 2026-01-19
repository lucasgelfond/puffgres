# Dockerfile for puffgres
# Downloads pre-built Rust binary from GitHub releases

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

# Install dependencies (includes puffgres npm package for transform executor)
RUN pnpm install --frozen-lockfile || pnpm install

# Copy the rest of the application
COPY . .

# Run the Rust binary directly
CMD ["puffgres", "run"]

# syntax=docker/dockerfile:1

ARG RUST_VERSION=1.85.0
ARG APP_NAME=program

################################################################################
# Stage 1: Build the application with dependencies from the trading platform
FROM rust:${RUST_VERSION}-slim-bullseye AS build
ARG APP_NAME

# Install necessary build dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Set performance-optimized environment variables
ENV RUSTFLAGS="-C target-cpu=native -C opt-level=3 -C codegen-units=1 -C panic=abort"
ENV RUST_BACKTRACE=0

# Set the working directory inside the container
WORKDIR /app

# Copy the entire TradingPlatform workspace (context is at TradingPlatform root)
COPY . ./

# Change to SignalEngine directory and build
WORKDIR /app/SignalEngine

# Debug: verify workspace structure
RUN echo "=== Checking SignalEngine workspace ===" && \
    cat Cargo.toml && \
    echo "=== Checking executionhandler dependencies ===" && \
    cat crates/executionhandler/Cargo.toml | grep -A5 "Local dependencies" && \
    echo "=== Checking smartorderrouter exists ===" && \
    ls -la crates/smartorderrouter/ && \
    cat crates/smartorderrouter/Cargo.toml && \
    echo "=== Checking databaseschema ===" && \
    ls -la /app/databaseschema/ && \
    cat /app/databaseschema/Cargo.toml && \
    echo "=== Testing cargo check on smartorderrouter ===" && \
    cargo check -p smartorderrouter 2>&1 || echo "smartorderrouter check failed" && \
    echo "=== End of debug ==="

# Build the SignalEngine program
RUN cargo build --locked --release --bin program && \
    cp target/release/program /bin/signal-engine

################################################################################
# Stage 2: Create a smaller runtime image
FROM debian:bullseye-slim AS runtime

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    libc6 \
    net-tools \
    procps \
    libssl-dev \
    ca-certificates \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Create a non-privileged user to run the app
ARG UID=10001
RUN adduser --disabled-password --gecos "" --home "/nonexistent" --shell "/sbin/nologin" --no-create-home --uid "${UID}" appuser

# Copy the built application from the build stage
COPY --from=build /bin/signal-engine /usr/local/bin/signal-engine

# Create config directory (config is managed via K8s ConfigMaps in production)
RUN mkdir -p /etc/signal-engine/config

# Ensure the binary is executable
RUN chmod +x /usr/local/bin/signal-engine

# Switch to non-privileged user
USER appuser

# Expose ports for metrics (no HTTP API - SignalEngine is a background trading engine)
EXPOSE 9090

# Health check using pgrep (SignalEngine doesn't have HTTP endpoints)
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
  CMD pgrep signal-engine || exit 1

# Set the command to run the application
ENTRYPOINT ["/usr/local/bin/signal-engine"]

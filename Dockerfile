# syntax=docker/dockerfile:1

ARG RUST_VERSION=1.90.0
ARG APP_NAME=program

################################################################################
# Stage 1: Build the application with dependencies from the trading platform
FROM rust:${RUST_VERSION}-slim-bookworm AS build
ARG APP_NAME

# Install necessary build dependencies.
# python3-dev is required by pyo3 (crates/pythonbridge-worker, pulled in
# transitively via hostbuilder -> strategyhandler): pyo3's build script links
# against libpython at build time even though the actual Python execution
# happens in a separate spawned pythonbridge-worker process, not in `program`
# itself.
#
# bookworm (not bullseye): the embedded Python SDK
# (crates/pythonbridge-worker/python_api/, copied from BacktestingEngine's
# strategy/python_api/) uses PEP 604 `X | Y` union syntax at runtime, not
# just in annotations -- this requires Python 3.10+. bullseye's default
# python3 is 3.9 and fails with `TypeError: unsupported operand type(s) for
# |: 'type' and 'type'` (confirmed by testing). bookworm's default is 3.11,
# matching BacktestingEngine's own Dockerfile (which already runs this same
# SDK successfully on bookworm) -- this isn't a new/independent version
# requirement, just matching what the code already needed.
RUN apt-get update && apt-get install -y \
    build-essential \
    pkg-config \
    libssl-dev \
    libpq-dev \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# Set performance-optimized environment variables
ENV RUSTFLAGS="-C target-cpu=x86-64-v3 -C opt-level=3 -C codegen-units=1 -C panic=abort"
ENV RUST_BACKTRACE=0

# Set the working directory inside the container
WORKDIR /app

# Copy the entire TradingPlatform workspace (context is at TradingPlatform root)
COPY . ./

# Change to SignalEngine directory and build
WORKDIR /app/SignalEngine

# Build the SignalEngine program with postgres support enabled.
# Reconciliation and DB-backed deployment recovery are cfg-gated on this feature.
#
# Also build pythonbridge-worker: PythonBridgeStrategy (strategyhandler)
# spawns this as a child process per live/paper deployment running real
# Python strategy code -- it's a separate binary, not linked into `program`,
# so it must be built and shipped alongside it explicitly.
RUN cargo build --release --bin program --features postgres && \
    cargo build --release --bin pythonbridge-worker && \
    cp target/release/program /bin/signal-engine && \
    cp target/release/pythonbridge-worker /bin/pythonbridge-worker

################################################################################
# Stage 2: Create a smaller runtime image
FROM debian:bookworm-slim AS runtime

# Install runtime dependencies.
# pythonbridge-worker embeds CPython via pyo3's auto-initialize, which
# dynamically links libpython3.11.so.1.0 at process startup -- NOT provided
# by the `python3`/`python3-minimal`/`libpython3.11-stdlib` packages (those
# only give you the interpreter binary and .py standard library files);
# `libpython3.11` specifically is the package containing the actual shared
# object (confirmed by testing: `python3 --version` ran fine without it,
# but `signal-engine` immediately failed with `error while loading shared
# libraries: libpython3.11.so.1.0: cannot open shared object file`).
# numpy/pandas need to actually be installed here too (imported by user
# strategies at runtime), not just linkable at build time.
RUN apt-get update && apt-get install -y \
    libc6 \
    net-tools \
    procps \
    libssl-dev \
    libpq5 \
    ca-certificates \
    curl \
    python3 \
    libpython3.11 \
    python3-numpy \
    python3-pandas \
    && rm -rf /var/lib/apt/lists/*

# Create a non-privileged user to run the app
ARG UID=10001
RUN adduser --disabled-password --gecos "" --home "/nonexistent" --shell "/sbin/nologin" --no-create-home --uid "${UID}" appuser

# Copy the built application from the build stage.
# pythonbridge-worker is copied into the SAME directory as signal-engine so
# PythonBridgeStrategy's default_binary_path() (sibling-of-current-exe
# lookup) finds it with no extra env var configuration needed.
COPY --from=build /bin/signal-engine /usr/local/bin/signal-engine
COPY --from=build /bin/pythonbridge-worker /usr/local/bin/pythonbridge-worker

# Create config directory (config is managed via K8s ConfigMaps in production)
RUN mkdir -p /etc/signal-engine/config

# Ensure the binaries are executable
RUN chmod +x /usr/local/bin/signal-engine /usr/local/bin/pythonbridge-worker

# Switch to non-privileged user
USER appuser

# Expose ports for metrics (no HTTP API - SignalEngine is a background trading engine)
EXPOSE 9090

# Health check using pgrep (SignalEngine doesn't have HTTP endpoints)
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
  CMD pgrep signal-engine || exit 1

# Set the command to run the application
ENTRYPOINT ["/usr/local/bin/signal-engine"]

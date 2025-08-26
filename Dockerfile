# Multi-stage build for SignalEngine
ARG RUST_VERSION=1.75

# Build stage
FROM rust:${RUST_VERSION}-slim-bullseye as builder

WORKDIR /app

# Install build dependencies
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Copy manifests
COPY Cargo.toml Cargo.lock ./
COPY config/Cargo.toml ./config/
COPY datahandler/Cargo.toml ./datahandler/
COPY exchangemetricaggregator/Cargo.toml ./exchangemetricaggregator/
COPY executionhandler/Cargo.toml ./executionhandler/
COPY hostbuilder/Cargo.toml ./hostbuilder/
COPY orderbook/Cargo.toml ./orderbook/
COPY portfolio/Cargo.toml ./portfolio/
COPY portfoliohandler/Cargo.toml ./portfoliohandler/
COPY program/Cargo.toml ./program/
COPY signal/Cargo.toml ./signal/
COPY signaldispatcher/Cargo.toml ./signaldispatcher/
COPY signalgenerator/Cargo.toml ./signalgenerator/
COPY smartorderrouter/Cargo.toml ./smartorderrouter/
COPY strategyhandler/Cargo.toml ./strategyhandler/

# Build dependencies (cache layer)
RUN cargo build --release --workspace && rm -rf src/ target/release/deps/signal_engine*

# Copy source code
COPY . .

# Build application
RUN cargo build --release --workspace

# Runtime stage - distroless for security
FROM gcr.io/distroless/cc-debian11:nonroot

WORKDIR /app

# Copy built application
COPY --from=builder /app/target/release/signal-engine /usr/local/bin/signal-engine

# Copy configuration templates
COPY --from=builder /app/config/ /etc/signal-engine/config/

# Health check script
COPY --from=builder /app/target/release/health-check /usr/local/bin/health-check

# Metadata
LABEL maintainer="Nwagbara-Group-LLC"
LABEL description="SignalEngine - Trading Signal Processing Microservice"
LABEL version="1.0.0"

ARG BUILD_DATE
ARG VCS_REF
LABEL org.label-schema.build-date=$BUILD_DATE
LABEL org.label-schema.vcs-ref=$VCS_REF
LABEL org.label-schema.schema-version="1.0"

# Security: Use non-root user
USER nonroot:nonroot

# Expose ports
EXPOSE 8080 9090

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
  CMD ["/usr/local/bin/health-check"]

# Default command
ENTRYPOINT ["/usr/local/bin/signal-engine"]
CMD ["--config", "/etc/signal-engine/config/production.yaml"]

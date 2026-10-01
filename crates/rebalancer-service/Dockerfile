# syntax=docker/dockerfile:1
#
# S-10 of product-mandate/PAPER_PILOT_DRAGONSTONE_PLAN.md: packages the rebalancer's driver-loop
# binary (`rebalancer-service`, crate doc: "the rebalancer's multi-account driver-loop binary
# (WP4.8)") as its own image, deliberately isolated from the main SignalEngine binary (`program`)
# it is built alongside.
#
# Build context MUST be the TradingPlatform monorepo root (the parent of this SignalEngine
# checkout), exactly like the sibling `SignalEngine/Dockerfile` (see that file's own comment and
# `SignalEngine/.dockerignore`'s header) -- NOT the SignalEngine directory alone. This is not a
# stylistic match: `crates/rebalancer-service` has no path dependency on any sibling repo (see its
# own Cargo.toml), but it is still a member of SignalEngine's single Cargo workspace, and Cargo
# resolves every workspace member's manifest -- including `program`'s own chain (hostbuilder ->
# databaseschema, executionhandler -> simulation-engine, datahandler -> ultra-logger/protocol) --
# before it will build ANY one member. Verified directly: `cargo build -p rebalancer-service` with
# only the SignalEngine directory as build context fails at the manifest-loading stage with
# `failed to read /databaseschema/Cargo.toml: No such file or directory`, before a single line of
# rebalancer-service's own code is even considered. With the sibling repos present it builds clean.
#
# Despite needing the same checkout set as `program`, this image contains ONLY the
# rebalancer-service binary -- no `program`, no `pythonbridge-worker`, no Python runtime. The
# isolation this slice cares about is the deployment surface (this Dockerfile, the manual-trigger
# workflow, and `k8s/rebalancer-helm` all exist apart from the main image/chart/workflow), not the
# build graph, which the workspace structure does not allow to be split without restructuring
# Cargo.toml itself -- out of scope for a packaging-only slice.

ARG RUST_VERSION=1.90.0

################################################################################
# Stage 1: build rebalancer-service only (not `program`, not pythonbridge-worker)
FROM rust:${RUST_VERSION}-slim-bookworm AS build

# libpq-dev: rebalancer-store links against libpq at build time (diesel's `postgres` feature,
# pq-sys). No python3-dev here -- unlike `program`, nothing in rebalancer-service's dependency
# tree (broker-adapters, market-data, rebalancer-core/run/store, mandate-core) touches pyo3.
RUN apt-get update && apt-get install -y \
    build-essential \
    pkg-config \
    libssl-dev \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

ENV RUSTFLAGS="-C target-cpu=x86-64-v3 -C opt-level=3 -C codegen-units=1"
ENV RUST_BACKTRACE=0

WORKDIR /app

# Copy the entire TradingPlatform workspace (context is at TradingPlatform root; see this file's
# header comment for why the sibling repos are needed even though this binary does not use them).
COPY . ./

WORKDIR /app/SignalEngine

# `-p rebalancer-service`: builds this one binary crate and its real dependency tree
# (broker-adapters with reqwest-transport, market-data, rebalancer-core/run/store, chrono, uuid),
# not the rest of the workspace. `reference-rules` (a public Mendl-Labs/BacktestingCore git
# dependency, pinned by rev in the workspace Cargo.toml) is fetched over the network during this
# step; no credentials are needed since that repo is public.
RUN cargo build --release -p rebalancer-service && \
    cp target/release/rebalancer-service /bin/rebalancer-service

################################################################################
# Stage 2: minimal runtime image
FROM debian:bookworm-slim AS runtime

# libpq5: the diesel-async/pq-sys runtime counterpart to build-time libpq-dev.
# ca-certificates: this binary makes real outbound HTTPS calls (Alpaca paper API, Massive REST
# API, Postgres over TLS) -- unlike `program`'s image, no python3/numpy/pandas are needed here.
# procps: provides `pgrep`, used by both this image's own HEALTHCHECK below and
# k8s/rebalancer-helm's startup/liveness/readiness probes (all three exec the same check). Easy
# to miss since the build stage never needs it -- matches `program`'s own runtime stage, which
# installs it for the identical reason (its HEALTHCHECK also shells out to pgrep).
RUN apt-get update && apt-get install -y \
    libc6 \
    procps \
    libssl-dev \
    libpq5 \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

ARG UID=10001
RUN adduser --disabled-password --gecos "" --home "/nonexistent" --shell "/sbin/nologin" --no-create-home --uid "${UID}" appuser

COPY --from=build /bin/rebalancer-service /usr/local/bin/rebalancer-service
RUN chmod +x /usr/local/bin/rebalancer-service

USER appuser

# No EXPOSE: rebalancer-service has no HTTP/gRPC listener and no metrics port -- it is a plain
# tick-and-sleep driver loop that only makes outbound calls (Postgres, Alpaca, Massive) and logs
# to stdout (see crates/rebalancer-service/src/main.rs's own module doc).
#
# `-f` is required, not optional: Linux's process `comm` field is truncated to 15 characters, and
# "rebalancer-service" is 19 -- `pgrep rebalancer-service` (no `-f`) NEVER matches, provably (it
# refuses to even try, printing "pattern...longer than 15 characters will result in zero matches"
# and exiting 1) -- confirmed against this exact built image and the exact real running binary,
# not assumed from `program`'s own Dockerfile pattern (which this deliberately diverges from: its
# process name, "signal-engine", is exactly 13 characters and works unmodified with plain pgrep).
# `-f` matches the full command line instead, which does contain the whole binary name/path.
HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
  CMD pgrep -f rebalancer-service || exit 1

# No argument = the real service loop (run_service in main.rs). The image's other two
# subcommands (`print-fingerprint`, `pilot-template`) are read-only/print-only owner tools, run by
# overriding the container's command by hand -- never the default entrypoint behavior.
ENTRYPOINT ["/usr/local/bin/rebalancer-service"]

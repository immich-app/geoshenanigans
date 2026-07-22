# Stage 1: Build C++ indexer
FROM debian:bookworm-slim AS builder-cpp

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential cmake \
    libosmium2-dev libprotozero-dev \
    libs2-dev \
    zlib1g-dev libbz2-dev libexpat1-dev liblz4-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY builder/ builder/
RUN mkdir build && cd build && cmake ../builder && make -j$(nproc)

# Stage 2: Build Rust server
FROM rust:bookworm AS builder-rust

RUN apt-get update && apt-get install -y --no-install-recommends \
    cmake \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY server/ server/
COPY test-portal/ test-portal/
RUN cargo build --release --manifest-path server/Cargo.toml

# Stage 3: Runtime
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    libs2-0 \
    zlib1g libbz2-1.0 libexpat1 liblz4-1 \
    curl ca-certificates zstd \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder-cpp /src/build/build-index /usr/local/bin/
COPY --from=builder-rust /src/server/target/release/query-server /usr/local/bin/
COPY entrypoint.sh /usr/local/bin/

RUN chmod +x /usr/local/bin/entrypoint.sh

# The server binds :3000 and exposes an unauthenticated /health that
# returns 200 once the index is loaded and serving.
HEALTHCHECK --interval=30s --timeout=3s --start-period=15s --retries=3 \
    CMD curl -fsS http://localhost:3000/health || exit 1

ENTRYPOINT ["entrypoint.sh"]
CMD ["auto"]

# Build Rust server (includes embedded test portal HTML)
FROM rust:bookworm@sha256:ade39a710ef85f3506cffcfec3e8a24ba090badef1dff2fb798f71d878833fea AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    cmake \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY server/ server/
COPY test-portal/index.html test-portal/index.html
COPY builder/data/ builder/data/
RUN cargo build --release --manifest-path server/Cargo.toml

# Runtime
FROM debian:bookworm-slim@sha256:734a72b3d6142a71993b4bc580ca01d8ca687f77a2c422d96a961d3376b179bc

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl ca-certificates zstd \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /src/server/target/release/query-server /usr/local/bin/
COPY test-portal/entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

EXPOSE 3000
VOLUME /data

ENTRYPOINT ["entrypoint.sh"]

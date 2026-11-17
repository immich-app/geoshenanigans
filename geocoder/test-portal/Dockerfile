# Build Rust server (includes embedded test portal HTML)
FROM rust:bookworm AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    cmake \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY server/ server/
COPY search/ search/
COPY test-portal/index.html test-portal/index.html
COPY builder/data/ builder/data/
RUN cargo build --release --manifest-path server/Cargo.toml
# geocoder-transport, which decodes the transport units the download fetches:
# a bin of the place search crate.
RUN cargo build --release --locked --manifest-path search/Cargo.toml --bin geocoder-transport

# Runtime
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl ca-certificates zstd \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /src/server/target/release/query-server /usr/local/bin/
COPY --from=builder /src/search/target/release/geocoder-transport /usr/local/bin/
COPY test-portal/entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

EXPOSE 3000
VOLUME /data

ENTRYPOINT ["entrypoint.sh"]

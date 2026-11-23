# Build Rust server (includes embedded test portal HTML)
FROM rust:trixie@sha256:223cc32ddca8f3ee90c436dbc94b6b5f052ea7f9653c5201e9612f08ad7d21b8 AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    cmake \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY server/ server/
COPY search/ search/
COPY test-portal/index.html test-portal/index.html
COPY builder/data/ builder/data/
RUN cargo build --release --locked --manifest-path server/Cargo.toml
# geocoder-transport, which decodes the transport units the download fetches:
# a bin of the place search crate.
RUN cargo build --release --locked --manifest-path search/Cargo.toml --bin geocoder-transport

# Runtime
FROM debian:trixie-slim@sha256:a72b4433de1cf7d6bae0c4cee74d0547f58601a643b3375f070c51600d63b6b2

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

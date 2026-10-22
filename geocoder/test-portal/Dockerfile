# Build Rust server (includes embedded test portal HTML)
FROM rust:bookworm@sha256:48b7f435b8fc6378ce43b44f15e3a47fd8919b2825f835c39bd4bd3da50af5b8 AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    cmake \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY server/ server/
COPY test-portal/index.html test-portal/index.html
COPY builder/data/ builder/data/
RUN cargo build --release --manifest-path server/Cargo.toml

# Runtime
FROM debian:bookworm-slim@sha256:7ff6ba31527b9e336162fa061746c366c4b2032f4d2cff2078bea43f78efe3f8

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl ca-certificates zstd \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /src/server/target/release/query-server /usr/local/bin/
COPY test-portal/entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

EXPOSE 3000
VOLUME /data

ENTRYPOINT ["entrypoint.sh"]

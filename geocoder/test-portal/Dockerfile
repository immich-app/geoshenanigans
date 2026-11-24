# Build the test portal's server, query-portal (includes the test page's
# HTML), which fetches and decodes the build itself
FROM rust:trixie@sha256:223cc32ddca8f3ee90c436dbc94b6b5f052ea7f9653c5201e9612f08ad7d21b8 AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    cmake \
    && rm -rf /var/lib/apt/lists/*

# query-portal is a bin of place-search, its sources query-server's
WORKDIR /src
COPY server/ server/
COPY search/ search/
COPY test-portal/index.html test-portal/index.html
COPY builder/data/ builder/data/
RUN cargo build --release --locked --manifest-path search/Cargo.toml --features portal --bin query-portal

# The Immich column's geodata: that of the Immich release image as it is
# when this image is built (its /build/geodata: GeoNames' cities500.txt,
# admin1CodesASCII.txt, admin2Codes.txt and countryInfo.txt, Natural Earth's
# countries and geodata-date.txt), and which release that is
FROM ghcr.io/immich-app/immich-server:release AS immich
RUN printf 'version=%s\ncommit=%s\n' "$IMMICH_SOURCE_REF" "$IMMICH_SOURCE_COMMIT" > /immich-release.txt

# Runtime
FROM debian:trixie-slim@sha256:8aa5a7bf5a1955bddbe7d74fcdd0b91ab30e74362a8b73af672b0481b35b44a0

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /src/search/target/release/query-portal /usr/local/bin/
COPY --from=immich /build/geodata/ /usr/local/share/immich-geodata/
COPY --from=immich /immich-release.txt /usr/local/share/immich-geodata/
COPY test-portal/entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

EXPOSE 3000
VOLUME /data

ENTRYPOINT ["entrypoint.sh"]

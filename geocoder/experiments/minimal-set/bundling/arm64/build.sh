#!/bin/bash
# Cross-build the Node native module (geocoder/node) for linux/arm64 in rust:bookworm, to check it builds.
set -euo pipefail
W=/ssd/shrink
# build from a copy (the repo stays read-only; geocoder/node has no Cargo.lock, server/src include_str!s builder/data)
rm -rf "$W/bundling/arm64/src" && mkdir -p "$W/bundling/arm64/src/builder"
cp -r "$W/repo/geocoder/node" "$W/repo/geocoder/server" "$W/bundling/arm64/src/"
cp -r "$W/repo/geocoder/builder/data" "$W/bundling/arm64/src/builder/"
cp "$W/bundling/arm64/src/server/Cargo.lock" "$W/bundling/arm64/src/node/Cargo.lock"
docker run --rm --cpus=16 -v "$W/bundling/arm64/src:/repo" -v "$W/cargo:/usr/local/cargo/registry" \
  -v "$W/bundling/arm64:/out" -e CARGO_TARGET_DIR=/out/target -w /repo/node rust:bookworm bash -c "
    set -e
    apt-get update -qq >/dev/null && apt-get install -y -qq gcc-aarch64-linux-gnu >/dev/null
    rustup target add aarch64-unknown-linux-gnu >/dev/null 2>&1
    export CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_LINKER=aarch64-linux-gnu-gcc CC_aarch64_unknown_linux_gnu=aarch64-linux-gnu-gcc
    time cargo build -q -j16 --release --target aarch64-unknown-linux-gnu
    time cargo build -q -j16 --release
    ls -la /out/target/aarch64-unknown-linux-gnu/release/*.so /out/target/release/*.so
  "
for f in $W/bundling/arm64/target/aarch64-unknown-linux-gnu/release/*.so $W/bundling/arm64/target/release/*.so; do /ssd/shrink/venv/bin/python -c "import struct,sys; b=open(sys.argv[1],\"rb\").read(20); print(sys.argv[1], {183:\"aarch64\",62:\"x86-64\"}[struct.unpack(\"<H\",b[18:20])[0]])" $f; done

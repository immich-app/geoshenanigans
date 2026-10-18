#!/usr/bin/env bash
# cargo test for the place-search crate and the geocoder server lib at the branch head.
set -uo pipefail
export PATH=$HOME/.cargo/bin:$PATH CARGO_HOME=/ssd/search-prod/cargo
cd /ssd/search-prod/repo/geocoder/search
echo "== search @ $(git rev-parse --short HEAD)"
CARGO_TARGET_DIR=/ssd/search-prod/target cargo test --release 2>&1 | tail -40
echo "search exit ${PIPESTATUS[0]}"
cd ../server
echo "== server"
CARGO_TARGET_DIR=/ssd/search-prod/target-server cargo test --release --lib 2>&1 | tail -30
echo "server exit ${PIPESTATUS[0]}"
cd ../node
echo "== node (build check)"
CARGO_TARGET_DIR=/ssd/search-prod/target-node cargo check --release 2>&1 | tail -5
echo "node exit ${PIPESTATUS[0]}"

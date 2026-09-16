#!/usr/bin/env bash
# Docker-based throughput bench under memory constraints.
#
# Each implementation runs in its own container with --memory=<limit>.
# Reports req/s for REPEAT/NEARBY/FAR scenarios and observes OOM /
# swap-thrash behaviour at low memory.
#
# WASM + TS run the throughput driver inside the container.
# Rust runs the server inside a container (memory-constrained) and
# the bench client on the host (no constraint).

set -uo pipefail

REPO=/home/zack/Source/immich/geoshenanigans
DATA=/home/zack/geocoder-data-v14
NETWORK=geocoder-bench-net

# Memory limits to test. Each implementation is run at each limit.
LIMITS=("512m" "1g" "2g" "4g" "8g")

mkdir -p "$REPO/build/docker-bench"
LOG="$REPO/build/docker-bench/results.log"
: > "$LOG"

log() {
  echo "$@" | tee -a "$LOG"
}

# Ensure shared docker network exists for the rust server + client to talk
docker network inspect "$NETWORK" >/dev/null 2>&1 || docker network create "$NETWORK" >/dev/null

# ---- WASM container test ----
run_wasm() {
  local mem=$1
  log ""
  log "=== WASM (memory=$mem) ==="
  docker run --rm --memory="$mem" --memory-swap="$mem" \
    -v "$REPO:/work:ro" \
    -v "$DATA:/data:ro" \
    -e GEOCODER_DATA=/data \
    -e TARGET=wasm \
    -e N=2000 \
    -w /work/geocoder/wasm-port \
    oven/bun:1 \
    bun test/throughput.ts 2>&1 | tee -a "$LOG" | tail -10
  echo "exit=$?" | tee -a "$LOG"
}

# ---- TS container test ----
run_ts() {
  local mem=$1
  log ""
  log "=== TS (memory=$mem) ==="
  docker run --rm --memory="$mem" --memory-swap="$mem" \
    -v "$REPO:/work:ro" \
    -v "$DATA:/data:ro" \
    -e GEOCODER_DATA=/data \
    -e TARGET=ts \
    -e N=2000 \
    -w /work/geocoder/wasm-port \
    oven/bun:1 \
    bun test/throughput.ts 2>&1 | tee -a "$LOG" | tail -10
  echo "exit=$?" | tee -a "$LOG"
}

# ---- Rust server container + host client ----
run_rust() {
  local mem=$1
  local cname="bench-rust-$$"
  log ""
  log "=== RustHTTP (server memory=$mem) ==="

  docker run -d --name "$cname" --memory="$mem" --memory-swap="$mem" \
    --network "$NETWORK" \
    -v "$REPO/geocoder/server/target/release:/srv:ro" \
    -v "$DATA:/data:ro" \
    -p 13556:3556 \
    debian:bookworm-slim \
    /srv/query-server /data 0.0.0.0:3556 >/dev/null

  # Wait for server ready
  local tries=0
  while [ $tries -lt 60 ]; do
    if curl -sf "http://localhost:13556/reverse?lat=51.5074&lon=-0.1278&key=test" >/dev/null 2>&1; then
      break
    fi
    sleep 1
    tries=$((tries + 1))
  done

  if [ $tries -ge 60 ]; then
    log "Server failed to start within 60s. Last 20 lines of container log:"
    docker logs "$cname" 2>&1 | tail -20 | tee -a "$LOG"
    docker rm -f "$cname" >/dev/null 2>&1
    return
  fi

  # Run bench from host against the container
  TARGET=rust RUST_URL=http://localhost:13556 N=2000 \
    bun "$REPO/geocoder/wasm-port/test/throughput.ts" 2>&1 | tee -a "$LOG" | tail -10

  # Capture container memory peak before killing
  if docker inspect "$cname" >/dev/null 2>&1; then
    local stats=$(docker stats --no-stream --format "{{.MemUsage}}" "$cname" 2>/dev/null || echo "n/a")
    log "Container mem at end: $stats"
  fi

  docker rm -f "$cname" >/dev/null 2>&1
}

# Verify server binary exists (built for linux x86_64 — should match Docker's arch)
if [ ! -x "$REPO/geocoder/server/target/release/query-server" ]; then
  log "Missing $REPO/geocoder/server/target/release/query-server — build it first"
  exit 1
fi

# Make sure the file_offset is reasonable (can't run in too-small mem)
log "Data dir: $DATA ($(du -sh "$DATA" | cut -f1))"
log "Memory limits to test: ${LIMITS[*]}"
log ""

for mem in "${LIMITS[@]}"; do
  run_rust "$mem"
done

for mem in "${LIMITS[@]}"; do
  run_wasm "$mem"
done

for mem in "${LIMITS[@]}"; do
  run_ts "$mem"
done

log ""
log "Done. Full log at $LOG"

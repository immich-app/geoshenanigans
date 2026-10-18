#!/usr/bin/env bash
# Builds cover_bench against the prebuilt S2 + abseil static libs from the s2 review track.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
s2=${S2_PREFIX:?set S2_PREFIX to an S2 + abseil install prefix (builder/tools/build_s2.sh)}
mkdir -p "$here/work"
g++ -O2 -std=c++17 -I"$s2/include" "$here/place_cell_bench.cpp" -o "$here/work/place_cell_bench" \
  -Wl,--start-group "$s2/lib/libs2.a" "$s2"/lib/libabsl_*.a -Wl,--end-group -lcrypto -lpthread

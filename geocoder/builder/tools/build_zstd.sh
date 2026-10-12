#!/usr/bin/env bash
# Usage: build_zstd.sh <prefix>
#
# Builds the zstd 1.5.7 command-line tool from its release tarball into
# <prefix>/bin/zstd; <prefix>/SHA256SUMS lists what it installed. The
# nightly compresses the index with it (compress_index.py runs the zstd first
# on PATH): at the same level, -18, it compressed planet samples smaller and
# in less CPU than Ubuntu 24.04's 1.5.5 (geo_cells 14% smaller in 21% less
# CPU, addr_points 0.7% smaller in 11% less). The format is the same, so any
# zstd decompresses its output.
set -euo pipefail

version=1.5.7
sha256=eb33e51f49a15e023950cd7825ca74a4a2b43db8354825ac24fc1b7ee09e6fa3

prefix=$(realpath -m "$1")
work=$(mktemp -d "${TMPDIR:-/tmp}/build-zstd.XXXXXX")
trap 'rm -rf "$work"' EXIT

curl -sSL --fail --connect-timeout 30 --max-time 300 --retry 5 --retry-delay 10 --retry-all-errors \
  -o "$work/zstd.tar.gz" "https://github.com/facebook/zstd/releases/download/v$version/zstd-$version.tar.gz"
echo "$sha256  $work/zstd.tar.gz" | sha256sum -c --quiet
tar xzf "$work/zstd.tar.gz" -C "$work"
make -C "$work/zstd-$version" -j"$(nproc)" zstd > "$work/build.log" 2>&1 || { tail -20 "$work/build.log"; exit 1; }
install -D -m 755 "$work/zstd-$version/programs/zstd" "$prefix/bin/zstd"
(cd "$prefix" && find . -type f ! -name SHA256SUMS -print0 | sort -z | xargs -0 sha256sum > "$work/SHA256SUMS")
mv "$work/SHA256SUMS" "$prefix/SHA256SUMS"
"$prefix/bin/zstd" --version

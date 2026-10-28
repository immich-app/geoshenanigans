#!/usr/bin/env bash
# Usage: ci_build_tools.sh <build dir> <cache dir> <s2 prefix>
#
# The nightly's tools for this source: the binaries in <cache dir> when its
# SHA256SUMS still holds (the workflow restores it keyed on the builder's
# source), else a fresh build that it saves there. Then the unit tests, which
# gate everything. Tests that fail on cached binaries (say the runner image's
# libraries moved) get one fresh build before they count.
set -euo pipefail

build=$(realpath -m "$1")
cache=$(realpath -m "$2")
s2_prefix=$3
tools=(build-index geocoder-diff geocoder-patch geocoder-gc pbf-apply heap-peak-shim heap-peak-probe geocoder-tests)
source_dir=$(cd "$(dirname "$0")/.." && pwd)

compile() {
  cmake -S "$source_dir" -B "$build" -DCMAKE_BUILD_TYPE=Release -DGEOCODER_S2_PREFIX="$s2_prefix"
  make -C "$build" -j"$(nproc)" "${tools[@]}"
  rm -rf "$cache"
  mkdir -p "$cache/bin"
  (cd "$build" && cp "${tools[@]}" "$cache/bin/")
  (cd "$cache" && sha256sum bin/* > SHA256SUMS)
}

mkdir -p "$build"
if (cd "$cache" 2> /dev/null && sha256sum -c --quiet SHA256SUMS); then
  echo "Tools from the cache"
  cp "$cache"/bin/* "$build/"
  cd "$build"
  if ./geocoder-tests; then exit 0; fi
  echo "Unit tests failed on the cached tools; building them afresh"
fi
compile
cd "$build"
./geocoder-tests

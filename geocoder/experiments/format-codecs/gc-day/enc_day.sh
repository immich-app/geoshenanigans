#!/bin/bash
# enc_day.sh SRC_DAY_DIR OUT_DAY_DIR [gc-encode args...]: encode every variant dir of a day in the
# published layout (planet/full, planet/quality/*, planet/poi/*); day-level files hardlinked.
set -euo pipefail
SRC=$1; OUT=$2; shift 2
GC=${GC:-/ssd/shrink/sortpatch/codecs/bin/gc-encode}
mkdir -p $OUT
for f in $SRC/*; do [ -f "$f" ] && ln -f "$f" $OUT/; done
for v in $(cd $SRC && find planet -mindepth 1 -maxdepth 2 -type d | sort); do
  ls $SRC/$v/*.bin > /dev/null 2>&1 || continue
  mkdir -p $OUT/$v
  echo "== $v"
  $GC $SRC/$v $OUT/$v "$@"
done

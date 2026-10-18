#!/bin/bash
# dec_day.sh ENC_DAY_DIR OUT_DAY_DIR [gc-decode args...]: decode every encoded variant dir of a day.
set -euo pipefail
SRC=$1; OUT=$2; shift 2
GC=${GC:-/ssd/shrink/sortpatch/codecs/bin/gc-decode}
mkdir -p $OUT
for f in $SRC/*; do [ -f "$f" ] && ln -f "$f" $OUT/; done
for v in $(cd $SRC && find planet -mindepth 1 -maxdepth 2 -type d | sort); do
  [ -f $SRC/$v/gc.manifest ] || continue
  mkdir -p $OUT/$v
  echo "== $v"
  $GC $SRC/$v $OUT/$v "$@"
done

#!/bin/bash
# clippy6.sh : cargo clippy of the search crate at 74a4a88 and d54f85d (verify6 archives); lists findings.
set -u
V=/ssd/verify6
export HOME=/root PATH=/root/.cargo/bin:$PATH TMPDIR=$V/tmp CARGO_HOME=$V/cargo
for r in 74a4a88 d54f85d; do
  (cd $V/src/p-$r/geocoder/search && CARGO_TARGET_DIR=$V/target-search-$r nice cargo clippy --release -j8) > $V/logs/clippy_$r.log 2>&1
  echo "CLIPPY $r rc=$? $(grep -cE "^(warning|error)" $V/logs/clippy_$r.log) lines"
  grep -E -A3 "^(warning|error)" $V/logs/clippy_$r.log | grep -E "^(warning|error)|-->" | paste - - | grep "src/" | sed -E "s/ +/ /g"
done

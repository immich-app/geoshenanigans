#!/usr/bin/env bash
# build_count.sh <name>:<rev> ... : rg_count on instrument.py copies of each reader revision.
# Binaries land in /ssd/reader/bin/rg_count.<name>.
set -euo pipefail
export PATH=$HOME/.cargo/bin:$PATH CARGO_HOME=/ssd/reader/cargo
R=/ssd/reader
for v in "$@"; do
  IFS=: read -r name rev <<< "$v"
  d=$R/src/$name-instr
  rm -rf "$d"
  mkdir -p "$d"
  git -C $R/repo archive "$rev" geocoder/server geocoder/builder/data | tar -x -C "$d"
  s=$d/geocoder/server
  python3 $R/scripts/instrument.py $s/src/lib.rs
  cp $R/scripts/rg_count.rs $s/examples/
  (cd "$s" && CARGO_TARGET_DIR=$R/target-$name-instr cargo build --release --example rg_count)
  cp $R/target-$name-instr/release/examples/rg_count $R/bin/rg_count.$name
done

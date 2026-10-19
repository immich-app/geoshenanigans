#!/usr/bin/env bash
# build.sh <name>:<rev> ... : grid_query, admin_dump and rg_bench for each reader revision of
# /ssd/reader/repo, from a git archive snapshot of geocoder/server (one target dir per name: git
# archive stamps sources with the commit time, so a shared target dir could reuse another build).
# Binaries land in /ssd/reader/bin/<example>.<name>.
set -euo pipefail
export PATH=$HOME/.cargo/bin:$PATH CARGO_HOME=/ssd/reader/cargo
R=/ssd/reader
for v in "$@"; do
  IFS=: read -r name rev <<< "$v"
  d=$R/src/$name
  rm -rf "$d"
  mkdir -p "$d"
  git -C $R/repo archive "$rev" geocoder/server geocoder/builder/data | tar -x -C "$d"
  s=$d/geocoder/server
  cp $R/scripts/rg_bench.rs $R/scripts/admin_dump.rs $s/examples/
  (cd "$s" && CARGO_TARGET_DIR=$R/target-$name cargo build --release --example grid_query --example admin_dump --example rg_bench)
  for e in grid_query admin_dump rg_bench; do cp $R/target-$name/release/examples/$e $R/bin/$e.$name; done
  echo "$name $rev $(git -C $R/repo rev-parse --short "$rev")" >> $R/bin/REVS
done
md5sum $R/bin/*.* | sort -k2

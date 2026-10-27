#!/usr/bin/env bash
# Build grid_query + rg_bench for main (f48ddc4) and the branch head (66a5ad0), and rg_count on
# instrumented copies of both, from git archive snapshots of geocoder/server. One target dir
# per variant: git archive stamps sources with the commit time, so a shared target dir sees
# the older revision as fresh and silently reuses the other build.
set -euo pipefail
export PATH=$HOME/.cargo/bin:$PATH CARGO_HOME=/ssd/search-prod/cargo
R=/ssd/search-prod/rg
REPO=/ssd/search-prod/repo
mkdir -p $R/src $R/bin
for v in main:f48ddc4 head:66a5ad0 main-instr:f48ddc4 head-instr:66a5ad0; do
  IFS=: read -r name rev <<< "$v"
  d=$R/src/$name
  rm -rf "$d"
  mkdir -p "$d"
  git -C $REPO archive $rev geocoder/server geocoder/builder/data | tar -x -C "$d"
  s=$d/geocoder/server
  if [[ $name == *instr ]]; then
    python3 $R/instrument.py $s/src/lib.rs
    cp $R/rg_count.rs $s/examples/
    exs=(rg_count)
  else
    cp $R/rg_bench.rs $s/examples/
    exs=(grid_query rg_bench)
  fi
  args=()
  for e in "${exs[@]}"; do args+=(--example "$e"); done
  (cd "$s" && CARGO_TARGET_DIR=$R/target-$name cargo build --release "${args[@]}")
  for e in "${exs[@]}"; do cp $R/target-$name/release/examples/$e $R/bin/$e.$name; done
done
ls -la $R/bin
md5sum $R/bin/*

#!/bin/bash
# build_readers.sh : release grid_query for each reader worktree under /ssd/tiers3/wt-<name>.
#   main  = origin/main f48ddc4 (main's reader)
#   psp   = feat/place-search-prod 74a4a88 (reader perf commits 20e4f4f f2f6d52 8dbb58d 66a5ad0)
# Binaries -> /ssd/tiers3/bin/grid_query.<name>. Own CARGO_HOME, nice 10, 12 jobs.
set -u
T=/ssd/tiers3
export CARGO_HOME=$T/cargo TMPDIR=$T/tmp PATH=$HOME/.cargo/bin:$PATH
cd $T/repo
for spec in "main main-f48" "psp psp"; do
  set -- $spec
  name=$1 ref=$2
  [ -d $T/wt-$name ] || git worktree add -q --detach $T/wt-$name $ref
  (cd $T/wt-$name/geocoder/server && CARGO_TARGET_DIR=$T/target-$name nice -n 10 cargo build -j 12 --release --example grid_query > $T/logs/cargo_$name.log 2>&1)
  echo "CARGO $name rc=$?"
  cp $T/target-$name/release/examples/grid_query $T/bin/grid_query.$name
done
echo READERS_DONE

#!/bin/bash
# build_count.sh : counting builds (count_patch3.py) of the main and psp readers, in their own
# worktrees /ssd/tiers3/wtc-<name> -> /ssd/tiers3/bin/grid_query.c<name>; plus the
# fix/admin-nesting reader (tiers3-study, plain) -> bin/grid_query.nest for the newer planet.
set -u
T=/ssd/tiers3
export CARGO_HOME=$T/cargo TMPDIR=$T/tmp PATH=$HOME/.cargo/bin:$PATH
cd $T/repo
for spec in "main main-f48" "psp psp"; do
  set -- $spec
  name=$1 ref=$2
  [ -d $T/wtc-$name ] || git worktree add -q --detach $T/wtc-$name $ref
  python3 -I $T/count_patch3.py $T/wtc-$name/geocoder/server/src/lib.rs $T/wtc-$name/geocoder/server/examples/grid_query.rs
  (cd $T/wtc-$name/geocoder/server && CARGO_TARGET_DIR=$T/target-c$name nice -n 10 cargo build -j 12 --release --example grid_query > $T/logs/cargo_c$name.log 2>&1)
  echo "CARGO c$name rc=$?"
  cp $T/target-c$name/release/examples/grid_query $T/bin/grid_query.c$name
done
[ -d $T/wt-nest ] || git worktree add -q --detach $T/wt-nest tiers3-study
(cd $T/wt-nest/geocoder/server && CARGO_TARGET_DIR=$T/target-nest nice -n 10 cargo build -j 12 --release --example grid_query > $T/logs/cargo_nest.log 2>&1)
echo "CARGO nest rc=$?"
cp $T/target-nest/release/examples/grid_query $T/bin/grid_query.nest
echo COUNT_DONE

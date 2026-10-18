#!/usr/bin/env bash
# The reader at 8dbb58d (20e4f4f + f2f6d52 + 8dbb58d, without 66a5ad0's query-cell-only scan) on the
# same pools as eq_run.sh, and its admin ring work (rg_count on an instrumented copy).
set -uo pipefail
export PATH=$HOME/.cargo/bin:$PATH CARGO_HOME=/ssd/search-prod/cargo
R=/ssd/search-prod/rg
T=/ssd/tiers2
P=$R/pts
d=$R/src/pip2-instr
rm -rf $d
mkdir -p $d
git -C /ssd/search-prod/repo archive 8dbb58d geocoder/server geocoder/builder/data | tar -x -C $d
python3 $R/instrument.py $d/geocoder/server/src/lib.rs
cp $R/rg_count.rs $d/geocoder/server/examples/
(cd $d/geocoder/server && CARGO_TARGET_DIR=$R/target-pip2-instr cargo build --release --example rg_count)
cp $R/target-pip2-instr/release/examples/rg_count $R/bin/rg_count.pip2-instr
declare -A DIR=(
  [orig-minimal]=$R/sets-orig/minimal [orig-recommended]=$R/sets-orig/recommended [orig-max]=$R/sets-orig/max
  [t2-q25-smallest]=$T/sets/q25/smallest [t2-q25-recommended]=$T/sets/q25/recommended
  [t2-uni75t8rc10-smallest]=$T/sets/uni75t8rc10/smallest [t2-uni75t8rc10-recommended]=$T/sets/uni75t8rc10/recommended
)
for set in "${!DIR[@]}"; do
  $R/bin/grid_query.pip2 ${DIR[$set]} < $P/pool.txt > $R/out/$set.pip2.jsonl 2> $R/out/$set.pip2.err &
  cat $P/edge2k_1000000_200000.txt $P/long15k_1000000_200000.txt | $R/bin/admin_dump.pip2 ${DIR[$set]} > $R/out/$set.pip2.admin 2> $R/out/$set.pip2.admin.err &
done
wait
for set in "${!DIR[@]}"; do
  for pool in p50k L2; do
    $R/bin/rg_count.pip2-instr ${DIR[$set]} < $P/$pool.txt > $R/count/$set.pip2.$pool.json 2> $R/count/$set.pip2.$pool.err &
  done
done
wait
echo done > $R/out/DONE_pip2

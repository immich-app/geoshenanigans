#!/bin/bash
# deg6.sh : experiment, not a commit. The tip server (64183ef) with the place-node distance of
# find_places (lib.rs:2095) in plain degrees, as getNearFeatures ST_Distance on SRID 4326 geometry
# (partition-functions.sql:32), instead of cos(lat)-scaled; answers on par30k vs the tip.
set -u
V=/ssd/verify6
export HOME=/root PATH=/root/.cargo/bin:$PATH TMPDIR=$V/tmp CARGO_HOME=$V/cargo
s=$V/src/s-deg
rm -rf $s; cp -a $V/src/s-parity-64183ef $s
sed -i "2095s/let dist_sq = dlat \* dlat + dlng \* dlng \* cos_lat \* cos_lat;/let dist_sq = dlat * dlat + dlng * dlng;/" $s/geocoder/server/src/lib.rs
sed -n 2095p $s/geocoder/server/src/lib.rs
(cd $s/geocoder/server && CARGO_TARGET_DIR=$V/target-deg nice cargo build --release -j16 --example grid_query) > $V/logs/build_deg.log 2>&1 || { echo BUILD_FAIL; exit 1; }
cp $V/target-deg/release/examples/grid_query $V/bin/grid_query.deg
for p in smallest recommended max; do
  mkdir -p $V/q/DEG_$p
  split -n l/16 -d $V/pts/par30k.txt $V/tmp/degpts.
  ls $V/tmp/degpts.* | xargs -P 16 -I{} sh -c "nice $V/bin/grid_query.deg $V/sets/tip/$p < {} > {}.out"
  cat $(ls $V/tmp/degpts.*.out | sort) > $V/q/DEG_$p/par30k.txt; rm -f $V/tmp/degpts.*
  echo "DEG $p lines $(wc -l < $V/q/DEG_$p/par30k.txt) differ_from_T $(diff <(cut -d" " -f3- $V/q/T_$p/par30k.txt) <(cut -d" " -f3- $V/q/DEG_$p/par30k.txt) | grep -c "^>")"
done
echo DEG_DONE

#!/bin/bash
# tests6.sh : cargo test of the server crate from a git archive of the whole geocoder dir (the tests
# include_str! files outside geocoder/server) at parity bc2e758, parity 64183ef (= f7096b0's server),
# reader main f48ddc4 and reader tip 9b16065; and admin_probe (parity probe source) at 64183ef.
set -u
V=/ssd/verify6
export TMPDIR=$V/tmp CARGO_HOME=$V/cargo PATH=$HOME/.cargo/bin:$PATH
t() {  # repo rev
  local s=$V/src/t-$1-$2
  rm -rf $s; mkdir -p $s
  git -C $V/repo-$1 archive $2 geocoder | tar -x -C $s
  (cd $s/geocoder/server && CARGO_TARGET_DIR=$V/target-t-$1-$2 nice -n 19 cargo test --release -j8) > $V/logs/test6_$1-$2.log 2>&1
  echo "TEST $1 $2 rc=$? $(grep -E '^test result' $V/logs/test6_$1-$2.log | sed -E 's/; finished.*//' | tr '\n' ' ')"
}
t parity bc2e758 & t parity 64183ef & t reader f48ddc4 & t reader 9b16065 &
(
  s=$V/src/s-parity-64183ef
  cp $V/scripts/admin_probe.rs $s/geocoder/server/examples/
  (cd $s/geocoder/server && CARGO_TARGET_DIR=$V/target-parity-64183ef nice -n 19 cargo build --release -j8 --example admin_probe) > $V/logs/build_admin_probe.log 2>&1
  cp $V/target-parity-64183ef/release/examples/admin_probe $V/bin/admin_probe.64183ef && echo "ADMIN_PROBE built"
) &
wait
echo TESTS6_DONE

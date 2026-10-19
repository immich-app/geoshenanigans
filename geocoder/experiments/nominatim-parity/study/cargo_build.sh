#!/bin/bash
# cargo_build.sh <label> [src repo dir] : release build + tests of the server at the working
# tree of <src repo dir> (default /ssd/parity/repo) into /ssd/parity/target-<label>; copies
# grid_query and parity_probe to /ssd/parity/bin/<name>.<label>. Niced, 8 jobs.
set -u
label=$1
src=${2:-/ssd/parity/repo}
P=/ssd/parity
export CARGO_HOME=$P/cargo CARGO_TARGET_DIR=$P/target-$label TMPDIR=$P/tmp PATH=$HOME/.cargo/bin:$PATH CARGO_BUILD_JOBS=8
cp $P/probe/parity_probe.rs $P/probe/admin_probe.rs $src/geocoder/server/examples/
cd $src/geocoder/server
nice -n 19 cargo build --release --examples > $P/logs/cargo_build.$label.log 2>&1 || { rm -f $src/geocoder/server/examples/parity_probe.rs $src/geocoder/server/examples/admin_probe.rs; echo BUILD_FAIL; tail -40 $P/logs/cargo_build.$label.log; exit 1; }
mkdir -p $P/bin
cp $CARGO_TARGET_DIR/release/examples/grid_query $P/bin/grid_query.$label
cp $CARGO_TARGET_DIR/release/examples/parity_probe $P/bin/parity_probe.$label
cp $CARGO_TARGET_DIR/release/examples/admin_probe $P/bin/admin_probe.$label
if [ "${NOTEST:-0}" != 1 ]; then
  nice -n 19 cargo test --release > $P/logs/cargo_test.$label.log 2>&1; echo "TEST rc=$?"
  grep -E "test result|FAILED|panicked" $P/logs/cargo_test.$label.log | head
fi
rm -f $src/geocoder/server/examples/parity_probe.rs $src/geocoder/server/examples/admin_probe.rs
echo CARGO_DONE $label

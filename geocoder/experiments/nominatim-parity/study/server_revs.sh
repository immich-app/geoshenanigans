#!/bin/bash
# server_revs.sh : fresh git-archive exports of the server at each server commit of the branch,
# each with its own target dir: cargo test --release and grid_query; md5 against the binaries
# the measurements used (bin/grid_query.<label>).
set -u
P=/ssd/parity
export CARGO_HOME=$P/cargo TMPDIR=$P/tmp PATH=$HOME/.cargo/bin:$PATH
for pair in s2a:984e668 s2b:7e8e9bb s3a:c99516b s3b:9171a8e s6:509845b; do
  label=${pair%%:*}; rev=${pair#*:}
  d=$P/srcs-$rev; rm -rf $d; mkdir -p $d
  git -C $P/repo archive $rev geocoder/server geocoder/builder/data geocoder/builder/configurations.template.json geocoder/test-portal | tar -x -C $d
  cd $d/geocoder/server
  CARGO_TARGET_DIR=$P/target-arch-$rev nice -n 19 cargo test --release -j12 > $P/logs/arch_test.$rev.log 2>&1
  echo "CARGO_TEST $label $rev rc=$? $(grep -E 'test result' $P/logs/arch_test.$rev.log | tr '\n' ' ')"
  CARGO_TARGET_DIR=$P/target-arch-$rev nice -n 19 cargo build --release -j12 --example grid_query > $P/logs/arch_build.$rev.log 2>&1
  echo "CARGO_BUILD $label $rev rc=$? $(md5sum < $P/target-arch-$rev/release/examples/grid_query | cut -c1-32) vs $(md5sum < $P/bin/grid_query.$label | cut -c1-32)"
done
echo SERVER_REVS_DONE

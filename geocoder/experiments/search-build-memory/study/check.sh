#!/bin/bash
# check.sh <tag> <rev>: archive <rev> of the searchbuild repo into src/chk-<tag>, then cargo test and
# cargo clippy (search crate) there; prints test results and clippy findings (file:line + lint).
set -u
S=/ssd/searchbuild
tag=$1; rev=$2
export TMPDIR=$S/tmp CARGO_HOME=$S/cargo PATH=$HOME/.cargo/bin:$PATH
d=$S/src/chk-$tag; rm -rf $d; mkdir -p $d
(cd $S/repo && git archive $rev geocoder) | tar -x -C $d
cd $d/geocoder/search
CARGO_TARGET_DIR=$S/target-chk-$tag nice cargo test --release -j12 2>&1 | grep -E "^test result|^error|FAILED|panicked" | sed "s/^/$tag test: /"
CARGO_TARGET_DIR=$S/target-chk-$tag nice cargo clippy --release -j12 2>&1 | grep -E -A3 "^(warning|error)" | grep -E "^(warning|error)|-->" | paste - - 2>/dev/null | grep "search/src" | sed -E "s/ +/ /g; s/^/$tag clippy: /"
echo "$tag done"

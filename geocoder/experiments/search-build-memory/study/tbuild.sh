#!/bin/bash
# tbuild.sh <tag> [rev]: the repo working tree (or an archive of rev) plus memtrace marks -> /ssd/searchbuild/bin/place-search.<tag>
# (experiment binary: prints live heap / RssAnon per build phase; not committed).
set -eu
S=/ssd/searchbuild
tag=$1; rev=${2:-}
d=$S/src/$tag
rm -rf $d; mkdir -p $d
if [ -n "$rev" ]; then git -C $S/repo archive $rev geocoder | tar -x -C $d; else cp -a $S/repo/geocoder $d/; fi
python3 -I $S/mk_trace.py $d/geocoder/search
export TMPDIR=$S/tmp CARGO_HOME=$S/cargo PATH=$HOME/.cargo/bin:$PATH
cd $d/geocoder/search && CARGO_TARGET_DIR=$S/target-trace nice cargo build --release -j12 2>&1 | grep -E "^(error|warning)|Finished" || true
cp $S/target-trace/release/place-search $S/bin/place-search.$tag && echo "BUILT $tag"

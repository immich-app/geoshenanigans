#!/bin/bash
# cbuild.sh <tag> [rev]: build place-search from the searchbuild repo (working tree, or an archive
# of <rev>) into /ssd/searchbuild/bin/place-search.<tag>.
set -u
S=/ssd/searchbuild
tag=$1; rev=${2:-}
export TMPDIR=$S/tmp CARGO_HOME=$S/cargo PATH=$HOME/.cargo/bin:$PATH
if [ -n "$rev" ]; then
  d=$S/src/$tag; rm -rf $d; mkdir -p $d
  git -C $S/repo archive $rev geocoder | tar -x -C $d
  src=$d/geocoder/search
else
  src=$S/repo/geocoder/search
fi
cd $src && CARGO_TARGET_DIR=$S/target-$tag nice cargo build --release -j12 2>&1 | tail -3
cp $S/target-$tag/release/place-search $S/bin/place-search.$tag && echo "BUILT $tag $(md5sum < $S/bin/place-search.$tag)"

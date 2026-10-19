#!/bin/bash
# build_revs.sh : fresh git-archive exports of the builder at base (bc2e758), r1 (fff988c) and
# the tip (0094397), each built and unit-tested in its own dir -> /ssd/parity/bin/build-index.<label>.
set -u
P=/ssd/parity
for pair in base:bc2e758 r1:fff988c r4:0094397; do
  label=${pair%%:*}; rev=${pair#*:}
  d=$P/src-$rev
  rm -rf $d; mkdir -p $d
  git -C $P/repo archive $rev geocoder/builder | tar -x -C $d
  mkdir -p $d/geocoder/server  # builder_build.sh needs only geocoder/builder
  rm -rf $P/bbuild-$label
  $P/builder_build.sh $label $d
done
md5sum $P/bin/build-index.*
echo BUILD_REVS_DONE

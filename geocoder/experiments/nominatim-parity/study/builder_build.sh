#!/bin/bash
# builder_build.sh <label> [src repo dir] : cmake + make build-index and geocoder-tests of the
# working tree (default /ssd/parity/repo) in /ssd/parity/bbuild-<label>, run the tests, copy
# build-index to /ssd/parity/bin/build-index.<label>. Niced, 16 jobs.
set -u
label=$1
src=${2:-/ssd/parity/repo}
P=/ssd/parity
b=$P/bbuild-$label
export TMPDIR=$P/tmp
mkdir -p $b $P/bin
cd $b
cmake -S $src/geocoder/builder -B . -DCMAKE_BUILD_TYPE=Release -DGEOCODER_S2_PREFIX=/ssd/precision/s2-prefix > cmake.log 2>&1 || { echo CMAKE_FAIL; tail -20 cmake.log; exit 1; }
nice -n 19 make -j16 build-index geocoder-tests > make.log 2>&1 || { echo MAKE_FAIL; grep -E "error|Error" make.log | head -20; exit 1; }
./geocoder-tests > tests.log 2>&1; echo "TESTS rc=$? $(tail -1 tests.log)"
cp build-index $P/bin/build-index.$label
echo BUILDER_DONE $label

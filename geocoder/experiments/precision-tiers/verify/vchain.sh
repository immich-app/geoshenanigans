#!/bin/bash
# vchain.sh: two verification builds under the lock.
#  1. cold, cap off (same as build step's nocap-nd) -> compare bytes with build/out-nocap
#  2. cap off + --prev-output build/out-base (strategy 2 active, like the nightly)
set -u
P=/ssd/precision
V=$P/verify
until (set -o noclobber; echo "verify" > $P/BUILD.lock) 2> /dev/null; do sleep 15; done
trap 'rm -f $P/BUILD.lock' EXIT
B=$P/build/build-index.instr
$V/vrun.sh nocap-cold $B --building-outline-cap 0
# byte compare against the build step's out-nocap
( cd $V/out-nocap-cold && find . -type f | sort ) > $V/nocap-cold/list.txt
( cd $P/build/out-nocap && find . -type f | sort ) > $V/nocap-cold/list_ref.txt
diff $V/nocap-cold/list.txt $V/nocap-cold/list_ref.txt > $V/nocap-cold/list_diff.txt
n=0
while read f; do
  cmp -s "$V/out-nocap-cold/$f" "$P/build/out-nocap/$f" || { echo "DIFF $f"; n=$((n+1)); }
done < $V/nocap-cold/list.txt > $V/nocap-cold/cmp.txt
echo "files_differing=$(grep -c DIFF $V/nocap-cold/cmp.txt) list_diff_lines=$(wc -l < $V/nocap-cold/list_diff.txt)" >> $V/nocap-cold/run.txt
rm -rf $V/out-nocap-cold
$V/vrun.sh nocap-prev $B --building-outline-cap 0 --prev-output $P/build/out-base
rm -rf $V/out-nocap-prev
echo CHAIN_DONE

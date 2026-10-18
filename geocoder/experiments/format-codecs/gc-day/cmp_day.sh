#!/bin/bash
# cmp_day.sh A_DIR B_DIR: same file names everywhere and every file byte-identical (whole-file cmp;
# files that are the same inode count as identical without reading).
set -uo pipefail
A=$1; B=$2
bad=0; same_inode=0; cmped=0; bytes=0
la=$(cd $A && find . -type f | sort); lb=$(cd $B && find . -type f | sort)
if [ "$la" != "$lb" ]; then echo "FILE LISTS DIFFER"; diff <(echo "$la") <(echo "$lb") | head -20; bad=1; fi
for f in $la; do
  [ -f "$B/$f" ] || continue
  if [ "$(stat -c %i $A/$f)" = "$(stat -c %i $B/$f)" ]; then same_inode=$((same_inode+1)); continue; fi
  if cmp -s $A/$f $B/$f; then cmped=$((cmped+1)); bytes=$((bytes + $(stat -c %s $A/$f))); else echo "DIFFERS: $f"; bad=1; fi
done
echo "cmp: $cmped files byte-identical ($bytes bytes compared), $same_inode hardlinked (same inode), result $([ $bad = 0 ] && echo PASS || echo FAIL)"
exit $bad

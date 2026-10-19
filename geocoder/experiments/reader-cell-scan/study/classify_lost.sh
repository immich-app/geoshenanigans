#!/usr/bin/env bash
# classify_lost.sh <lost file> <set dir> : for every ring in a scan_diag --lost file, run
# lune_diag on the ring as that set stores it and the points it was lost at. Prints per ring:
# id level name points, how many the S2 covering of the stored ring misses (in_cover=0), how many
# S2 says are outside the ring (s2_contains=0), how many sit in a lune, and the lune edge lengths.
set -uo pipefail
R=/ssd/reader
lost=$1 dir=$2
tmp=$R/diag/cls.$$
mkdir -p $tmp
for id in $(cut -d" " -f3 $lost | sort -u); do
  /ssd/shrink/venv/bin/python -I $R/scripts/dump_ring.py $dir $id > $tmp/ring.txt 2> $tmp/ring.err
  awk -v id=$id '$3 == id {print $1, $2}' $lost > $tmp/pts.txt
  $R/bin/lune_diag $tmp/ring.txt < $tmp/pts.txt > $tmp/out.txt 2> /dev/null
  name=$(awk -v id=$id '$3 == id {for (i = 8; i <= NF; i++) printf "%s ", $i; exit}' $lost)
  level=$(awk -v id=$id '$3 == id {print $4; exit}' $lost)
  n=$(wc -l < $tmp/pts.txt)
  miss=$(grep -c "in_cover=0" $tmp/out.txt)
  out=$(grep -c "s2_contains=0 planar_pip=1" $tmp/out.txt)
  lune=$(awk '/^[0-9-]/ {p = 0} /^[0-9-]/ && /planar_pip=1/ && /s2_contains=0/ {p = 1; k++} /lune edge/ && p {l++; p = 0} END {print l + 0}' $tmp/out.txt)
  edges=$(grep -o "length_km=[0-9.]*" $tmp/out.txt | sort -u | cut -d= -f2 | tr "\n" "," | sed "s/,$//")
  depth=$(grep -o "lune_depth_km=[0-9.]*" $tmp/out.txt | cut -d= -f2 | sort -g | tail -1)
  echo "$id L$level ${name}| points $n | cover misses $miss | s2 outside, planar inside $out | in a lune $lune | lune edge km $edges | max lune depth km $depth | $(cat $tmp/ring.err)"
done
rm -rf $tmp

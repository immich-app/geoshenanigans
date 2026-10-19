#!/bin/bash
# eqcmp.sh <set> : answer lines that differ between reader pairs on every D1 pool of a set
# (q/eq_<reader>_<set>_<st> and q/D1_<set>_<st> for nest), plus up to 3 examples per pair.
V=/ssd/verify5
s=$1
cd $V/q
for st in smallest recommended; do
  for pair in main:psp psp:pspr main:pspr nest:nest3 main:nest; do
    a=${pair%%:*}; b=${pair#*:}
    da=eq_${a}_${s}_$st; [ $a = nest ] && da=D1_${s}_$st
    db=eq_${b}_${s}_$st; [ $b = nest ] && db=D1_${s}_$st
    tot=0; diff=0; ex=""
    for p in fb10k fp10k bb140k bp100k ba50k; do
      n=$(wc -l < $da/$p.txt)
      d=$(paste -d'\t' $da/$p.txt $db/$p.txt | awk -F'\t' '$1!=$2' | wc -l)
      [ $d -gt 0 ] && ex="$ex $p:$d"
      tot=$((tot + n)); diff=$((diff + d))
    done
    echo "$s $st $a vs $b: $diff of $tot differ ($ex )"
    if [ $diff -gt 0 ] && [ $diff -lt 20 ]; then
      for p in fb10k fp10k bb140k bp100k ba50k; do
        paste -d'\t' $da/$p.txt $db/$p.txt | awk -F'\t' '$1!=$2' | head -3 | while IFS=$'\t' read -r x y; do
          echo "   $a: $(echo $x | cut -c1-230)"; echo "   $b: $(echo $y | cut -c1-230)"; done
      done
    fi
  done
done

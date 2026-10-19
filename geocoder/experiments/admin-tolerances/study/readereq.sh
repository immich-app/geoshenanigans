#!/bin/bash
# readereq.sh <scheme>... : main's reader (f48ddc4) and feat/place-search-prod's (74a4a88) on the same
# planet-261006 sets (sets6/<scheme>/{smallest,recommended}) over the timing pools (tpts: p50k =
# first 50k of tiers2 p200k, L2 = tiers2 L2 20k), answers compared line for line; plus the counting
# builds (decoded admin vertices per query) -> cmp/readereq.tsv and cmp/work.tsv.
set -u
T=/ssd/tiers3
cd $T
mkdir -p tpts
[ -s tpts/p50k.txt ] || head -50000 /ssd/tiers2/pts/p200k.txt > tpts/p50k.txt
[ -s tpts/L2.txt ] || cp /ssd/tiers2/pts/L2.txt tpts/L2.txt
for s in "$@"; do
  for st in smallest recommended; do
    for r in main psp; do PTS=$T/tpts JOBS=8 ./runq.sh eq_${r}_${s}_$st sets6/$s/$st $T/bin/grid_query.$r > /dev/null; done
    for p in p50k L2; do
      n=$(wc -l < tpts/$p.txt)
      d=$(cmp -l q/eq_main_${s}_$st/$p.txt q/eq_psp_${s}_$st/$p.txt 2>/dev/null | wc -l)
      dl=$(paste -d'\n' q/eq_main_${s}_$st/$p.txt q/eq_psp_${s}_$st/$p.txt | paste - - | awk -F'\t' '$1 != $2' | wc -l)
      printf "%s\t%s\t%s\t%s\tdiff_lines=%s\n" $s $st $p $n $dl >> cmp/readereq.tsv
    done
    for r in main psp; do
      for p in p50k L2; do
        n=$(wc -l < tpts/$p.txt)
        c=$(nice -n 5 $T/bin/grid_query.c$r sets6/$s/$st < tpts/$p.txt 2>&1 > /dev/null | grep admin_decodes)
        printf "%s\t%s\t%s\t%s\t%s\t%s\n" $r $s $st $p $n "$c" >> cmp/work.tsv
      done
    done
  done
done
echo READEREQ_DONE

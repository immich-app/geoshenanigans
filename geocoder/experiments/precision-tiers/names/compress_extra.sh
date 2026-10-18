#!/bin/bash
# compress_extra.sh: sizes of the optional extra files extra.py wrote.
# Output: out/compress_extra.tsv  split scope layout lines raw_bytes zstd18_bytes
#   xfm   extra feature-major file (one line per base-file feature, empty if none)
#   xdict extra strings of the scope not already in the base dictionary, sorted
set -eu
F=/ssd/precision/names/files
S=/ssd/precision/names/scopes
O=/ssd/precision/names/out
Z=/ssd/shrink/zstd157/bin/zstd
export TMPDIR=/ssd/precision/tmp LC_ALL=C
declare -A LV=([places]="L0" [places+major]="L0 L1" [places+notable]="L0 L1 L2")
printf 'split\tscope\tlayout\tlines\traw_bytes\tzstd18_bytes\n' > $O/compress_extra.tsv
run() {
  split=$1; scope=$2; big=${split%%_over_*}; small=${split##*_over_}
  fm=""; st=""; bd=""
  for lv in ${LV[$scope]}; do fm="$fm $S/xfm_${split}_${lv}.txt"; st="$st $S/xstr_${split}_${lv}.txt"; bd="$bd $F/dict_${small}_${lv}.txt"; done
  f=$S/run_${split}_${scope}
  cat $fm > $f.fm
  cat $bd | sort -u -S 2G > $f.base
  cat $st | sort -u -S 2G | comm -23 - $f.base > $f.xd
  for k in fm xd; do
    lay=$([ $k = fm ] && echo xfm || echo xdict)
    printf '%s\t%s\t%s\t%s\t%s\t%s\n' "$split" "$scope" "$lay" "$(wc -l < $f.$k)" "$(stat -c %s $f.$k)" "$($Z -18 -T1 -q -c $f.$k | wc -c)"
  done
  rm -f $f.fm $f.base $f.xd
}
for split in top10_over_base all_over_base all_over_top10; do
  for scope in places places+major places+notable; do
    run $split $scope &
  done
done > $S/extra_rows.tsv
wait
sort $S/extra_rows.tsv >> $O/compress_extra.tsv
rm -f $S/extra_rows.tsv
echo EXTRA_DONE

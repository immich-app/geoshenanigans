#!/bin/bash
Z=/ssd/shrink/zstd157/bin/zstd; F=/ssd/precision/names/files; N=/ssd/precision/build/names; export TMPDIR=/ssd/precision/tmp
for f in $F/fm_base_L0.txt $F/fm_all_L0.txt; do printf "%s\t%s\t%s\n" $f $(stat -c %s $f) $($Z -18 -T1 -q -c $f | wc -c); done
# fm base for places + major POIs = L0 + L1 concatenated
printf "fm_base_L0+L1\t%s\t%s\n" $(cat $F/fm_base_L0.txt $F/fm_base_L1.txt | wc -c) $(cat $F/fm_base_L0.txt $F/fm_base_L1.txt | $Z -18 -T1 -q -c | wc -c)
printf "fm_all_L0+L1\t%s\t%s\n" $(cat $F/fm_all_L0.txt $F/fm_all_L1.txt | wc -c) $(cat $F/fm_all_L0.txt $F/fm_all_L1.txt | $Z -18 -T1 -q -c | wc -c)
printf "dict_base_L0 sorted-unique\t%s\t%s\n" $(LC_ALL=C sort -u -S 4G $F/dict_base_L0.txt | wc -c) $(LC_ALL=C sort -u -S 4G $F/dict_base_L0.txt | $Z -18 -T1 -q -c | wc -c)
printf "dict_base_L0 lines\t%s\n" $(LC_ALL=C sort -u -S 4G $F/dict_base_L0.txt | wc -l)
for f in $N/strings_base_place_shipped.txt $N/strings_all_languages_place_shipped.txt $N/strings_base_poi_shipped.txt; do printf "%s\t%s\t%s\t%s\n" $f $(wc -l < $f) $(stat -c %s $f) $($Z -18 -T1 -q -c $f | wc -c); done
echo NAMES_ZS_DONE

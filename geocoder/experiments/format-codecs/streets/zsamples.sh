#!/bin/bash
# zstd 1.5.7 -18 -T1 every *.sample in a dir (skips ones already measured); prints name raw zst
D=$1
Z=/ssd/shrink/zstd157/bin/zstd
touch $D/zsizes.tsv
ls $D/*.sample | while read f; do grep -q "^$(basename $f)	" $D/zsizes.tsv || echo $f; done | xargs -P ${JOBS:-16} -I{} bash -c "printf \"%s\t%s\t%s\n\" \$(basename {}) \$(stat -c %s {}) \$($Z -18 -T1 -q -c {} | wc -c)" >> $D/zsizes.tsv
sort -u $D/zsizes.tsv -o $D/zsizes.tsv
cat $D/zsizes.tsv

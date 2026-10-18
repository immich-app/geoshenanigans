#!/bin/bash
# sizes.sh : raw bytes and zstd 1.5.7 -18 -T1 bytes of every file in every verify set (resolving symlinks)
# -> cmp/sizes.tsv (set<TAB>file<TAB>raw<TAB>zst<TAB>realpath); identical real paths compressed once.
V=/ssd/tiers2-verify
Z=/ssd/shrink/zstd157/bin/zstd
$Z --version > $V/cmp/zstd_version.txt
find -L $V/sets -type f | sort > $V/tmp/setfiles.txt
for f in $(cat $V/tmp/setfiles.txt); do readlink -f $f; done | sort -u > $V/tmp/realfiles.txt
cat $V/tmp/realfiles.txt | xargs -P 16 -I{} sh -c "printf \"%s\t%s\t%s\n\" {} \$(stat -c%s {}) \$($Z -18 -T1 -q -c {} | wc -c)" > $V/cmp/zst_real.tsv
echo SIZES_DONE

#!/bin/bash
# independent size recompute: per-file zstd 1.5.7 -18 -T1 (by path), raw, and tar|gzip -6 layer
set -euo pipefail
export TMPDIR=/ssd/shrink/tmp
Z=/ssd/shrink/zstd157/bin/zstd
cd /ssd/shrink/stack/verify
: > zst.tsv
list=()
for d in /ssd/shrink/idx/smallest /ssd/shrink/stack/final /ssd/shrink/stack/todays-format; do
  for f in $d/*; do [ "$(basename $f)" = SHA256SUMS ] && continue; list+=("$f"); done
done
printf "%s\n" "${list[@]}" | xargs -P 12 -I{} bash -c "echo -e \"{}\t\$(stat -c %s {})\t\$($Z -18 -T1 -q -c {} | wc -c)\"" >> zst.tsv
layer() { # $1 dir, rest: files
  d=$1; shift
  tar -c --sort=name --owner=0 --group=0 --numeric-owner --mtime=@0 -C $d -f - "$@" | gzip -6 -n | wc -c
}
echo -e "baseline_layer\t$(layer /ssd/shrink/idx/smallest $(cd /ssd/shrink/idx/smallest && ls))" > layer.tsv
echo -e "final_layer\t$(layer /ssd/shrink/stack/final $(cd /ssd/shrink/stack/final && ls | grep -v SHA256SUMS))" >> layer.tsv
echo -e "todays_layer\t$(layer /ssd/shrink/stack/todays-format $(cd /ssd/shrink/stack/todays-format && ls))" >> layer.tsv
echo done > sizes.done

#!/bin/bash
# wholeset.sh <set name>... : raw and zstd 1.5.7 -18 -T1 bytes of every file a flat set ships (followed
# through symlinks), without admin_polygons.osm_ids, arc_meta.json and the 16-byte dummies of the arc sets
# -> one WHOLE line per set and part. zstd sizes cached by path+size in cmp/zcache.tsv.
V=/ssd/verify5
Z=/ssd/shrink/zstd157/bin/zstd
C=$V/cmp/zcache.tsv
touch $C
zs() { local p=$1 k="$1:$(stat -c%s $1)"; local h=$(awk -F'\t' -v k="$k" '$1==k {print $2}' $C | tail -1)
  if [ -z "$h" ]; then h=$(set -o pipefail; $Z -18 -T1 -q -c $p | wc -c) || return 1; printf "%s\t%s\n" "$k" $h >> $C; fi; echo $h; }  # cache only a finished zstd
export -f zs; export Z C
for name in "$@"; do
  for st in smallest recommended; do
    files=$(for f in $V/sets/$name/$st/*; do b=$(basename $f); p=$(readlink -f $f)
      case $b in admin_polygons.osm_ids|arc_meta.json) continue ;; esac
      [ "$(stat -c%s $p)" = 16 ] && [ "$p" = $V/tmp/dummy16.bin ] && continue
      echo $p; done)
    printf '%s\n' $files | xargs -P ${PAR:-16} -I{} bash -c 'zs {} > /dev/null'
    raw=0; z=0
    for p in $files; do raw=$((raw + $(stat -c%s $p))); z=$((z + $(zs $p))); done
    echo "WHOLE $name $st raw $raw zstd $z files $(echo $files | wc -w)"
  done
done

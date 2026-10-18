#!/bin/bash
# Build lossy variant dirs (today's format) next to symlinks of the
# untouched minimal-set files, then zstd -18 each new admin_vertices.bin.
set -euo pipefail
cd "$(dirname "$0")"
SRC=/ssd/shrink/idx/smallest
mk() {
  name=$1; shift
  d=var/$name; mkdir -p "$d"
  for f in "$SRC"/*; do
    b=$(basename "$f")
    case $b in admin_polygons.bin|admin_vertices.bin) ;; *) ln -sf "$f" "$d/$b" ;; esac
  done
  ./variant "$SRC" "$d" "$@"
  zstd -18 -T1 -q -f -c "$d/admin_vertices.bin" | wc -c | xargs printf "$name zstd18 %s\n"
}
export -f mk; export SRC
printf '%s\n' "identity" "floor 75" "floor 150" "scale 2" "snap 1e-6" "snap 1e-5" "snap 1e-4" \
  | while read -r a b; do echo "${a}${b:+_$b} $a $b"; done \
  | xargs -P 7 -L 1 bash -c 'mk "$@"' _

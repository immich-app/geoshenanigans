#!/usr/bin/env bash
# Patching impact of a no-postcode variant (planet admin/ + quality/q2.5/,
# 2026-10-12 -> 2026-10-13), all on cores 0-15:
#  A. published patch applied to the complete 10-12 dirs (baseline, sha check)
#  B. published patch applied to 10-12 dirs that skipped the postcode files
#  C. geocoder-diff re-run on complete dirs (reproduces the published size?)
#     and on dirs without the postcode files (the no-postcode variant's own
#     patch), then that patch applied + compared byte for byte.
set -uo pipefail
W=/ssd/shrink/no-postcodes
R=$W/patch
BIN=$W/bin
OLD=$R/2026-10-12/planet
NEW=$R/2026-10-13/planet
S=$R/work
export TMPDIR=/ssd/shrink/tmp
TS="taskset -c 0-15"
rm -rf "$S"; mkdir -p "$S"
PC_ADMIN="postcode_centroids.bin postcode_centroid_cells.bin postcode_centroid_entries.bin strings_postcode.bin"
PC_ADMIN_OPT="postcode_centroids.bin postcode_centroid_cells.bin postcode_centroid_entries.bin"
PC_Q="postal_polygons.bin postal_vertices.bin"

copy_without() { # src dst names-to-skip...
  local src=$1 dst=$2; shift 2
  mkdir -p "$dst"
  for f in "$src"/*; do
    b=$(basename "$f")
    case $b in patch.gcpatch|*.osm_ids) continue;; esac
    case " $* " in *" $b "*) continue;; esac
    ln "$f" "$dst/$b"
  done
}
check_against_manifest() { # dir variant-prefix
  /ssd/shrink/venv/bin/python - "$1" "$2" <<'PY'
import hashlib, json, os, sys
d, pre = sys.argv[1], sys.argv[2]
F = json.load(open("/ssd/shrink/no-postcodes/patch/2026-10-13/configurations.json"))["files"]
bad = []
for n in sorted(os.listdir(d)):
    h = hashlib.sha256(open(f"{d}/{n}", "rb").read()).hexdigest()
    if F.get(f"{pre}/{n}", {}).get("sha256") != h:
        bad.append(n)
print(f"    {len(os.listdir(d))} files, sha256 vs 2026-10-13 manifest: {'ALL MATCH' if not bad else 'MISMATCH ' + ' '.join(bad)}")
PY
}
apply() { # label old patch out
  mkdir -p "$4"
  $TS "$BIN/geocoder-patch" "$2" "$3" -o "$4" > "$S/$1.log" 2>&1
  local rc=$?
  echo "  $1: rc=$rc; last line: $(tail -1 "$S/$1.log")"
  return $rc
}

echo "== A/B: published 2026-10-13 patches"
for v in admin quality/q2.5; do
  tag=${v//\//_}
  [ "$v" = admin ] && skips=("$PC_ADMIN" "$PC_ADMIN_OPT") || skips=("$PC_Q")
  copy_without "$OLD/$v" "$S/old_full_$tag"
  apply "A_$tag" "$S/old_full_$tag" "$NEW/$v/patch.gcpatch" "$S/A_out_$tag" && check_against_manifest "$S/A_out_$tag" "planet/$v"
  i=0
  for skip in "${skips[@]}"; do
    i=$((i + 1))
    copy_without "$OLD/$v" "$S/old_skip${i}_$tag" $skip
    echo "  B_${tag}_$i skips: $skip"
    apply "B_${tag}_$i" "$S/old_skip${i}_$tag" "$NEW/$v/patch.gcpatch" "$S/B_out${i}_$tag"
  done
done

echo "== C: geocoder-diff, complete vs no-postcode dirs"
for kind in full nopc; do
  for date in 2026-10-12 2026-10-13; do
    T=$S/C_$kind/$date/planet
    mkdir -p "$T/full"
    ln "$R/$date/planet/full/"* "$T/full/"
    if [ $kind = full ]; then
      copy_without "$R/$date/planet/admin" "$T/admin"
      copy_without "$R/$date/planet/quality/q2.5" "$T/quality/q2.5"
    else
      copy_without "$R/$date/planet/admin" "$T/admin" $PC_ADMIN
      copy_without "$R/$date/planet/quality/q2.5" "$T/quality/q2.5" $PC_Q
    fi
  done
  for v in admin quality/q2.5; do
    tag=${v//\//_}
    O=$S/C_$kind/2026-10-12/planet/$v
    N=$S/C_$kind/2026-10-13/planet/$v
    /usr/bin/time -f "%e s %M KiB" -o "$S/C_${kind}_$tag.time" $TS "$BIN/geocoder-diff" "$O" "$N" -o "$S/C_${kind}_$tag.gcpatch" > "$S/C_${kind}_$tag.diff.log" 2>&1
    echo "  $kind $v: diff rc=$? patch $(stat -c %s "$S/C_${kind}_$tag.gcpatch" 2>/dev/null) bytes (published $(stat -c %s "$NEW/$v/patch.gcpatch")), $(cat "$S/C_${kind}_$tag.time" | tail -1)"
    apply "C_apply_${kind}_$tag" "$O" "$S/C_${kind}_$tag.gcpatch" "$S/C_out_${kind}_$tag"
    mism=""
    for f in "$N"/*; do cmp -s "$f" "$S/C_out_${kind}_$tag/$(basename "$f")" || mism="$mism $(basename "$f")"; done
    extra=$(comm -13 <(ls "$N") <(ls "$S/C_out_${kind}_$tag") | tr '\n' ' ')
    echo "    byte compare vs new dir: ${mism:-all identical}; extra files in output: ${extra:-none}"
  done
done

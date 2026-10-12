#!/usr/bin/env bash
# Usage: verify_patch.sh <new-dir> <patched-dir>
# Passes only when the patched dir holds exactly the new build's client file
# set (every file except *.osm_ids, *.gcpatch, *.zst and dotfiles) with
# identical bytes. Prints one line per problem and the PASS/FAIL verdict.
set -uo pipefail

new_dir=$1
got_dir=$2

client_files() {
  find "$1" -maxdepth 1 -type f ! -name '.*' ! -name '*.osm_ids' ! -name '*.gcpatch' ! -name '*.zst' -printf '%f\n' \
    | LC_ALL=C sort
}

problems=0
while IFS= read -r name; do
  echo "missing: $name"
  problems=$((problems + 1))
done < <(LC_ALL=C comm -23 <(client_files "$new_dir") <(client_files "$got_dir"))
while IFS= read -r name; do
  echo "extra: $name"
  problems=$((problems + 1))
done < <(LC_ALL=C comm -13 <(client_files "$new_dir") <(client_files "$got_dir"))

checked=0
while IFS= read -r name; do
  [ -f "$got_dir/$name" ] || continue
  checked=$((checked + 1))
  if ! cmp -s "$new_dir/$name" "$got_dir/$name"; then
    first=$(cmp "$new_dir/$name" "$got_dir/$name" 2>/dev/null | awk '{print $5}' | tr -d ,)
    echo "differs: $name new=$(stat -c%s "$new_dir/$name") got=$(stat -c%s "$got_dir/$name") first_diff=${first:-end}"
    problems=$((problems + 1))
  fi
done < <(client_files "$new_dir")

if [ "$problems" -eq 0 ]; then
  echo "PASS $checked files"
  exit 0
fi
echo "FAIL $problems problems, $checked files compared"
exit 1

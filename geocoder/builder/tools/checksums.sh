#!/usr/bin/env bash
# Usage: checksums.sh zst <output dir> > <zst checksums>
#        checksums.sh all <output dir> <zst checksums> > <output dir>/checksums.sha256
#
# The published checksums.sha256: a `sha256sum` line ("<hash>  ./<path>") for
# every .zst, .gcpatch and .json file under the output dir, the hashes of the
# bytes on the wire (PATCHING.md), sorted by path (sort -k2). It is made in
# two parts so that most of the hashing leaves Finalize:
# - zst: the .zst files', final once compression ends;
# - all: the .gcpatch and .json files' (configurations.json once Finalize
#   has spliced the build's dates into it), merged with the zst lines.
# all refuses, printing nothing, when the zst lines don't name exactly the
# .zst files there, or when the line it would publish for configurations.json
# is not that file's hash.
set -euo pipefail

mode=$1
out=$2
cd "$out"

# The sha256sum lines of the files find selects (its tests in "$@"), hashed on
# every core.
hash_files() {
  find . -type f \( "$@" \) -print0 | xargs -0 -r -P "$(nproc)" -n 8 sha256sum
}

case $mode in
  zst)
    hash_files -name "*.zst"
    ;;
  all)
    zst=$(cat "$3")
    if ! diff <(sed 's/^[0-9a-f]\{64\}  //' <<< "$zst" | sed '/^$/d' | sort) <(find . -type f -name "*.zst" | sort) >&2; then
      echo "checksums.sh: the .zst checksums of $3 do not name the .zst files of $out (above: < theirs, > the files)" >&2
      exit 1
    fi
    lines=$({ [ -z "$zst" ] || echo "$zst"; hash_files -name "*.gcpatch" -o -name "*.json"; } | sort -k2)
    if [ -f configurations.json ] && ! grep -qxF "$(sha256sum ./configurations.json)" <<< "$lines"; then
      echo "checksums.sh: the line for configurations.json is not its sha256 now" >&2
      exit 1
    fi
    echo "$lines"
    ;;
  *)
    echo "Usage: checksums.sh zst|all <output dir> [<zst checksums>]" >&2
    exit 2
    ;;
esac

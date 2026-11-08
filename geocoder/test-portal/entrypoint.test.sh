#!/usr/bin/env bash
# Tests entrypoint.sh's index cache: a server starts on the dataset it finds
# only when that is a finished download of the selection it serves, and
# otherwise downloads the whole selection afresh. curl and query-server are
# stubbed out; a directory stands in for the bucket.
#
#   geocoder/test-portal/entrypoint.test.sh
set -euo pipefail

script_dir=$(cd "$(dirname "$0")" && pwd)
entrypoint="$script_dir/entrypoint.sh"
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
failures=0
build=2026-11-01

# curl serves <bucket>/<path after builds/> and logs each URL; the bucket holds
# its files plain, which the entrypoint fetches when a .zst is missing.
mkdir -p "$work/bin"
cat > "$work/bin/curl" <<'STUB'
#!/usr/bin/env bash
out= url=
while [ $# -gt 0 ]; do
  case "$1" in
    -o) out=$2; shift ;;
    http*) url=$1 ;;
  esac
  shift
done
echo "$url" >> "$CURL_LOG"
src="$BUCKET/${url#*/builds/}"
[ -f "$src" ] || exit 22
if [ -n "$out" ]; then cp "$src" "$out"; else cat "$src"; fi
STUB
printf '#!/bin/sh\necho "query-server $*"\n' > "$work/bin/query-server"
chmod +x "$work/bin/"*
export PATH="$work/bin:$PATH" CURL_LOG="$work/curl.log"

# list [VAR=value...]: the "<dir> <file>" lines of the selection.
list() {
  env "$@" DATA_DIR="$work/unused" sh "$entrypoint" --list-files
}

# The bucket: every file of each selection the cases run, 1 KiB, its first line
# naming its build and path.
bucket="$work/bucket"
mkdir -p "$bucket"
printf '{\n  "latest": "%s"\n}\n' "$build" > "$bucket/latest.json"
{ list; list QUALITY=exact; list MINIMAL=1; } | sort -u | while read -r dir f; do
  mkdir -p "$bucket/$build/planet/$dir"
  { echo "$build $dir/$f"; head -c 1024 /dev/zero; } > "$bucket/$build/planet/$dir/$f"
done

# bucket_without <file under planet/>: a copy of the bucket lacking it.
bucket_without() {
  local copy="$work/bucket-$RANDOM"
  cp -r "$bucket" "$copy"
  rm "$copy/$build/planet/$1"
  echo "$copy"
}

# start <data dir> [VAR=value...]: runs the entrypoint as the container does,
# on the bucket in $BUCKET; its output is in $work/out, its exit status in
# $status and the URLs it fetched in $CURL_LOG.
start() {
  local data=$1
  shift
  : > "$CURL_LOG"
  set +e
  env DATA_DIR="$data" "$@" sh "$entrypoint" > "$work/out" 2>&1
  status=$?
  set -e
}

# holds <data dir> [VAR=value...]: whether <data dir>/index holds the build's
# copy of each file of the selection the bucket has, and nothing more but the
# accounts and the download's marker.
holds() {
  local index=$1/index dir f
  shift
  while read -r dir f; do
    if [ -f "$BUCKET/$build/planet/$dir/$f" ]; then
      cmp -s "$BUCKET/$build/planet/$dir/$f" "$index/$f" || { echo "$dir/$f is not the build's"; return 1; }
    elif [ -e "$index/$f" ]; then
      echo "$f is not in the build"; return 1
    fi
  done < <(list "$@")
  local extra
  extra=$(cd "$index" && ls -A | grep -vxF -f <(list "$@" | cut -d' ' -f2; echo geocoder.json; echo .selection) || true)
  [ -z "$extra" ] || { echo "stale: $extra"; return 1; }
}

served() { [ "$status" = 0 ] && [ "$(tail -n1 "$work/out")" = "query-server $1/index 0.0.0.0:3000" ]; }
fetched() { grep -q '/latest.json$' "$CURL_LOG"; }

# check <name> <condition...>: reports the case; on failure, the entrypoint's output.
check() {
  local name=$1
  shift
  local why
  if why=$("$@" 2>&1); then
    echo "ok   $name"
  else
    echo "FAIL $name: $* ${why}"
    sed 's/^/       /' "$work/out"
    failures=$((failures + 1))
  fi
}

# finished <data dir> [VAR=value...]: a finished download of the selection.
finished() {
  start "$@"
  [ "$status" = 0 ] || { echo "setup download failed"; cat "$work/out"; exit 1; }
}

export BUCKET="$bucket"

# A fresh volume downloads the selection and serves it.
d="$work/fresh"
start "$d"
check "a fresh volume downloads the selection" holds "$d"
check "a fresh volume serves it" served "$d"

# A finished download of the selection is served as it is.
d="$work/finished"
finished "$d"
start "$d"
check "a finished download is not fetched again" eval '! fetched'
check "a finished download is served" served "$d"

# A dataset from before the arc geometry (admin_vertices.bin, no rings or arcs)
# is replaced by the whole selection, not completed: its cells and strings are
# another build's. The accounts stay.
d="$work/legacy"
mkdir -p "$d/index"
list | cut -d' ' -f2 | grep -vxE 'admin_rings.bin|arc_index.bin|arc_vertices.bin' | while read -r f; do
  { echo "old $f"; head -c 2048 /dev/zero; } > "$d/index/$f"
done
echo "old admin_vertices.bin" > "$d/index/admin_vertices.bin"
echo '{"users":{"kept":{}}}' > "$d/index/geocoder.json"
start "$d"
check "a legacy dataset is downloaded afresh" holds "$d"
check "a legacy dataset's accounts stay" grep -q kept "$d/index/geocoder.json"
check "a legacy dataset: the server starts" served "$d"

# So is the legacy dataset of the minimal server.
d="$work/legacy-minimal"
mkdir -p "$d/index"
for f in admin_cells.bin admin_entries.bin admin_polygons.bin admin_vertices.bin place_nodes.bin strings_core.bin strings_layout.json; do
  { echo "old $f"; head -c 2048 /dev/zero; } > "$d/index/$f"
done
start "$d" MINIMAL=1
check "a legacy minimal dataset is downloaded afresh" holds "$d" MINIMAL=1
check "a legacy minimal dataset: the server starts" served "$d"

# A download that stopped before it finished left no marker: every file is
# fetched again, the truncated ones among them.
d="$work/interrupted"
finished "$d"
rm -f "$d/index/.selection"
truncate -s 10 "$d/index/geo_cells.bin" "$d/index/strings_core.bin"
start "$d"
check "an interrupted download is downloaded afresh" holds "$d"
check "an interrupted download: the server starts" served "$d"

# A dataset of another selection (here the other boundary tier, whose files
# have the same names) is replaced by this one's.
d="$work/other-selection"
finished "$d"
start "$d" QUALITY=exact
check "another selection's dataset is downloaded afresh" holds "$d" QUALITY=exact
check "another selection's dataset: the server starts" served "$d"

# A finished download that has since lost a file it needs is fetched again.
d="$work/lost-file"
finished "$d"
rm "$d/index/arc_index.bin"
start "$d"
check "a dataset missing a file is downloaded afresh" holds "$d"

# A file the build may lack is no error, and the dataset stays.
d="$work/optional"
BUCKET=$(bucket_without full/interp_postcodes.bin)
start "$d"
check "a missing optional file still serves" served "$d"
check "a missing optional file leaves the rest of the selection" holds "$d"
start "$d"
check "a dataset without an optional file is not fetched again" eval '! fetched'
BUCKET=$bucket

# A file the server needs that the build lacks fails the download, naming it,
# and the next start tries again.
d="$work/required"
BUCKET=$(bucket_without quality/simplified/arc_vertices.bin)
start "$d"
check "a missing required file fails the start" test "$status" != 0
check "a missing required file is named" grep -qx 'quality/simplified/arc_vertices.bin' "$work/out"
check "a failed download leaves no marker" test ! -e "$d/index/.selection"
BUCKET=$bucket
start "$d"
check "a failed download is downloaded afresh next time" holds "$d"

if [ "$failures" -gt 0 ]; then
  echo "$failures failure(s)"
  exit 1
fi
echo "all passed"
